package receiver

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"weak"

	"github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// executing runs execute for record in the background under ctx and returns where its acknowledgement arrives.
func (f fixture) executing(t *testing.T, ctx context.Context, record *kgo.Record, attempt int64) <-chan kgo.AckStatus {
	t.Helper()
	admitted := f.admitted(t, record)
	done := make(chan kgo.AckStatus, 1)
	go func() { done <- f.receiver.execute(ctx, f.service.Operator, record, admitted, attempt) }()
	return done
}

// dispatched returns the dispatches, the value the last delivered at the agent's credential's target, and its attempt.
func (f fixture) dispatched(t *testing.T) (int, []byte, int64) {
	t.Helper()
	f.plugin.mu.Lock()
	defer f.plugin.mu.Unlock()
	var value []byte
	if index := slices.IndexFunc(f.plugin.lastRun.Credentials, func(secret pluginsdk.Secret) bool { return secret.Env == credentialEnv }); index >= 0 {
		value = f.plugin.lastRun.Credentials[index].Value
	}
	return f.plugin.dispatches, value, f.plugin.lastRun.Attempt
}

// request waits for the receiver's nth open request on the pool's events.
func (f fixture) request(t *testing.T, n int) wire.OpenRequest {
	t.Helper()
	for {
		if requests := f.openRequests(t); len(requests) >= n {
			return requests[n-1]
		}
		if f.ctx.Err() != nil {
			t.Fatalf("no open request %d; logs:\n%s", n, f.logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// requestRecord waits for the receiver's nth open request and returns its record.
func (f fixture) requestRecord(t *testing.T, n int) *kgo.Record {
	t.Helper()
	f.request(t, n)
	var found []*kgo.Record
	for _, record := range f.records(t, operator.PoolEventsTopic(testPool)) {
		if recordType(record.Value) == wire.OpenRequestType {
			found = append(found, record)
		}
	}
	return found[n-1]
}

// wrapFor writes, signed by writer under key, an attempt wrap of the run's content key to request's key bound to digest.
func (f fixture) wrapFor(t *testing.T, task *kgo.Record, request wire.OpenRequest, digest, writer string, key ed25519.PrivateKey) {
	t.Helper()
	contentKey := f.contentKey(t, task)
	public, err := request.Key.Public()
	if err != nil {
		t.Fatal(err)
	}
	jwe := must(t)(signed.WrapContentKey(contentKey, public))
	wrap := &wire.AttemptWrap{Custodian: writer, TaskHash: request.TaskHash, Request: digest, Receiver: request.Receiver, Attempt: request.Attempt, JWE: string(jwe)}
	f.produce(t, operator.PoolEventsTopic(testPool), request.Run, must(t)(wire.SignRecord(wrap, key)))
}

// contentKey recovers task's content key with the pool keys the fixture's receiver holds.
func (f fixture) contentKey(t *testing.T, task *kgo.Record) []byte {
	t.Helper()
	keys, err := loadPoolKeys(f.receiver.poolKeysDir())
	if err != nil {
		t.Fatal(err)
	}
	contentKey := f.receiver.contentKey(f.parse(t, task), keys, nil)
	if contentKey == nil {
		t.Fatal("no pool key opens the task")
	}
	return contentKey
}

func TestOpenRequestAsksForTheAttemptsKey(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.submit(t, f.host, nil)
	if status := f.execute(t, record, 1); status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	requests := f.openRequests(t)
	if len(requests) != 1 {
		t.Fatalf("open requests %d, want 1", len(requests))
	}
	request := requests[0]
	if _, err := request.Key.Public(); err != nil || request.Run != testRun("run-a") || request.Attempt != 1 || request.Receiver != testReceiver || request.TaskHash != wire.TaskHash(record.Value) {
		t.Fatalf("open request %#v", request)
	}
	if dispatches, credential, _ := f.dispatched(t); dispatches != 1 || string(credential) != testToken {
		t.Fatalf("dispatches %d with credential %q, want the envelope's value once", dispatches, credential)
	}
	// The request is its own transaction on the run's key, after the claimed status.
	events := f.records(t, operator.PoolEventsTopic(testPool))
	claimed := slices.IndexFunc(events, func(record *kgo.Record) bool { return recordType(record.Value) == wire.StatusType })
	if string(events[claimed+1].Key) != testRun("run-a") || recordType(events[claimed+1].Value) != wire.OpenRequestType || oneTransaction(events[claimed:claimed+2]) {
		t.Fatal("the open request is not a transaction of its own after the claimed status")
	}
	for _, topic := range []string{operator.TenantTopic(testTenant), operator.HostTopic(testTenant, testHost)} {
		for _, record := range f.records(t, topic) {
			if strings.Contains(recordType(record.Value), "seal") {
				t.Fatalf("a %s record on %s", recordType(record.Value), topic)
			}
		}
	}
}

// An attempt wrap that is not the current custodian's, or that answers another request, does not open: the attempt
// waits out its open and gives the task back.
func TestAnAttemptWrapOnlyOpensFromTheCustodianForItsRequest(t *testing.T) {
	for _, test := range []struct {
		name  string
		write func(f fixture, record *kgo.Record, request wire.OpenRequest, digest string, peer ed25519.PrivateKey)
	}{
		{"another receiver", func(f fixture, record *kgo.Record, request wire.OpenRequest, digest string, peer ed25519.PrivateKey) {
			f.wrapFor(t, record, request, digest, "receiver-b", peer)
		}},
		{"another request", func(f fixture, record *kgo.Record, request wire.OpenRequest, digest string, _ ed25519.PrivateKey) {
			f.wrapFor(t, record, request, wire.Digest([]byte("another request")), testReceiver, f.receiver.signing)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.quiet()
			peer := f.addPeer(t, "receiver-b", testPool)
			record := f.submit(t, f.host, nil)
			done := f.executing(t, f.ctx, record, 1)
			request := f.request(t, 1)
			test.write(f, record, request, wire.Digest(f.requestRecord(t, 1).Value), peer)
			if status := <-done; status != kgo.AckRelease {
				t.Fatalf("status %v, want the task released; logs:\n%s", status, f.logs.String())
			}
			if dispatches, _, _ := f.dispatched(t); dispatches != 0 {
				t.Fatalf("dispatched %d jobs with a wrap that is not the custodian's answer to the request", dispatches)
			}
		})
	}
}

// A redelivered turn requests again under a new key; a wrap answering the first attempt's request does not open it.
func TestALaterAttemptRequestsAgain(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	record := f.submit(t, f.host, nil)
	crashed, crash := context.WithCancel(f.ctx)
	done := f.executing(t, crashed, record, 1)
	first := f.request(t, 1)
	firstDigest := wire.Digest(f.requestRecord(t, 1).Value)
	crash()
	if status := <-done; status != kgo.AckRelease {
		t.Fatalf("attempt 1 ended %v, want release", status)
	}
	done = f.executing(t, f.ctx, record, 2)
	second := f.request(t, 2)
	if second.Attempt != 2 || second.Key == first.Key {
		t.Fatalf("attempt 2 request %#v, want attempt 2 under a new key", second)
	}
	f.wrapFor(t, record, first, firstDigest, testReceiver, f.receiver.signing)
	f.serving.Store(true)
	if status := <-done; status != kgo.AckAccept {
		t.Fatalf("attempt 2 ended %v; logs:\n%s", status, f.logs.String())
	}
	if dispatches, credential, attempt := f.dispatched(t); dispatches != 1 || string(credential) != testToken || attempt != 2 {
		t.Fatalf("dispatches %d with %q at attempt %d, want attempt 2 opened alone", dispatches, credential, attempt)
	}
}

func TestAttemptKeyIsGoneAfterTheAttempt(t *testing.T) {
	f := newFixture(t, resultStream)
	var key weak.Pointer[ecdsa.PrivateKey]
	previous := newAttemptKey
	newAttemptKey = func() (*ecdsa.PrivateKey, error) {
		generated, err := previous()
		key = weak.Make(generated)
		return generated, err
	}
	t.Cleanup(func() { newAttemptKey = previous })
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	if dispatches, credential, _ := f.dispatched(t); dispatches != 1 || string(credential) != testToken {
		t.Fatal("the run did not open its envelope")
	}
	for range 10 {
		runtime.GC()
		if key.Value() == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the attempt key is still reachable after the attempt ended")
}

func TestCancelDuringTheOpenWaitEndsTheTurn(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	done := f.executing(t, f.ctx, f.submit(t, f.host, nil), 1)
	f.request(t, 1)
	f.publishCancel(t, testRun("run-a"), 1, f.host, nil)
	if status := <-done; status != kgo.AckAccept || f.lastTask(t).Status.State != wire.TaskStateCanceled {
		t.Fatalf("ended %v with %s, want CANCELED", status, f.lastTask(t).Status.State)
	}
	if dispatches, _, _ := f.dispatched(t); dispatches != 0 {
		t.Fatal("dispatched a cancelled turn")
	}
}

// A wrap whose key does not open the envelope is ignored and counted, and nothing of it is logged.
func TestTamperedWrapIsIgnoredAndNeverLogged(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	record := f.submit(t, f.host, nil)
	done := f.executing(t, f.ctx, record, 1)
	request := f.request(t, 1)
	public, _ := request.Key.Public()
	other := make([]byte, 32)
	jwe := must(t)(signed.WrapContentKey(other, public))
	wrap := &wire.AttemptWrap{Custodian: testReceiver, TaskHash: request.TaskHash, Request: wire.Digest(f.requestRecord(t, 1).Value), Receiver: testReceiver, Attempt: 1, JWE: string(jwe)}
	f.produce(t, operator.PoolEventsTopic(testPool), testRun("run-a"), must(t)(wire.SignRecord(wrap, f.receiver.signing)))
	if status := <-done; status != kgo.AckRelease {
		t.Fatalf("a wrap of another key: ended %v, want the task released", status)
	}
	if logs := f.logs.String(); strings.Contains(logs, testToken) || !strings.Contains(logs, "run "+testRun("run-a")+": ignored an attempt wrap whose key does not open the envelope") {
		t.Fatalf("unexpected logs:\n%s", logs)
	}
	scraped(t, f.receiver, `wherry_ignored_wraps_total{pool="`+testPool+`"} 1`)
}

var redacted = signed.RedactionMarker(testRedactionKey, credentialName)

func TestDeliveredCredentialIsRedacted(t *testing.T) {
	f := newFixture(t, []string{
		`{"type":"assistant","message":"token ` + testToken + `"}`,
		completedWith("used " + testToken),
	})
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	t.Run("transcript", func(t *testing.T) {
		chunks := f.chunks(t)
		if len(chunks) == 0 {
			t.Fatal("no transcript")
		}
		for _, chunk := range chunks {
			if text := chunk.ArtifactUpdate.Artifact.Parts[0].Text; strings.Contains(text, testToken) {
				t.Fatalf("transcript line %q carries the credential", text)
			}
		}
		if text := chunks[0].ArtifactUpdate.Artifact.Parts[0].Text; !strings.Contains(text, redacted) {
			t.Fatalf("transcript line %q, want the credential redacted", text)
		}
	})
	result := f.lastTask(t).Artifacts[0]
	t.Run("result", func(t *testing.T) {
		if text := result.Parts[0].Text; text != "used "+redacted {
			t.Fatalf("result %q, want the credential redacted", text)
		}
	})
	t.Run("status message", func(t *testing.T) {
		if text := f.lastTask(t).Status.Message.Parts[0].Text; text != "used "+redacted {
			t.Fatalf("status message %q, want the credential redacted", text)
		}
	})
}

func TestStatusMessageIsRedacted(t *testing.T) {
	f := newFixture(t, resultStream)
	run := testActiveRun(t, f, "run-s", 1)
	run.deliver(map[string][]byte{credentialName: []byte(testToken), "empty": {}})
	var status wire.RunStatus
	if err := wire.ParseRecord(run.statusRecords("terminal", taskStatus(wire.TaskStateFailed, "failed with "+testToken), nil, nil)[0].Value, f.keys(), &status); err != nil {
		t.Fatal(err)
	}
	if text := status.StatusUpdate.Status.Message.Parts[0].Text; text != "failed with "+redacted {
		t.Fatalf("status message %q, want the credential redacted and nothing else changed", text)
	}
}

// A wrap and a cancel seen in the same poll: the cancel wins and nothing is dispatched.
func TestCancelSeenWithTheWrapEndsTheTurn(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	previous := stateCheckInterval
	stateCheckInterval = 10 * time.Second
	t.Cleanup(func() { stateCheckInterval = previous })
	record := f.submit(t, f.host, nil)
	done := f.executing(t, f.ctx, record, 1)
	request := f.request(t, 1)
	// The receiver is inside a 10-second poll; the cancel is held before the wrap ends it.
	f.publishCancel(t, testRun("run-a"), 1, f.host, nil)
	f.wrapFor(t, record, request, wire.Digest(f.requestRecord(t, 1).Value), testReceiver, f.receiver.signing)
	if status := <-done; status != kgo.AckAccept || f.lastTask(t).Status.State != wire.TaskStateCanceled {
		t.Fatalf("ended %v with %s, want CANCELED", status, f.lastTask(t).Status.State)
	}
	if dispatches, _, _ := f.dispatched(t); dispatches != 0 {
		t.Fatal("dispatched a turn cancelled when its wrap arrived")
	}
}

// A value shorter than minRedact matches ordinary output, so it is logged and left in place.
func TestShortCredentialIsLoggedNotRedacted(t *testing.T) {
	f := newFixture(t, []string{`{"type":"assistant","message":"checked: false"}`, doneOutcome})
	f.envelope = sealing(map[string][]byte{credentialName: []byte("false")})
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckAccept || f.lastTask(t).Status.State != wire.TaskStateCompleted {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	if !strings.Contains(f.logs.String(), "run "+testRun("run-a")+": credential "+credentialName+" too short to redact") {
		t.Fatalf("logs:\n%s", f.logs.String())
	}
	if first := f.chunks(t)[0].ArtifactUpdate.Artifact.Parts[0].Text; !strings.Contains(first, "checked: false") {
		t.Fatalf("transcript line %q, want the short value left in place", first)
	}
}
func TestRedactionMarkerDoesNotNameTheCredential(t *testing.T) {
	f := newFixture(t, resultStream)
	run := testActiveRun(t, f, "run-n", 1)
	run.deliver(map[string][]byte{"credential-name": []byte("12345678")})
	if got := run.redactText("a 12345678 b"); got != "a "+signed.RedactionMarker(testRedactionKey, "credential-name")+" b" || strings.Contains(got, "credential-name") {
		t.Fatalf("redacted %q, want the HMAC marker without the credential name", got)
	}
}

// A value that occurs within its own marker gets the fallback marker.
func TestMarkerFallsBackWhenAValueIsWithinIt(t *testing.T) {
	f := newFixture(t, resultStream)
	run := testActiveRun(t, f, "run-m", 1)
	value := signed.RedactionMarker(testRedactionKey, "x")[10:18]
	run.deliver(map[string][]byte{"x": []byte(value), "y": []byte("12345678")})
	if got := run.redactText(value + " 12345678"); got != "[*] "+signed.RedactionMarker(testRedactionKey, "y") {
		t.Fatalf("redacted %q, want fallback only for x", got)
	}
}

// A value that holds another credential's value is replaced whole, whatever order the values arrive in.
func TestLongerValueIsRedactedFirst(t *testing.T) {
	f := newFixture(t, resultStream)
	for range 20 {
		run := testActiveRun(t, f, "run-l", 1)
		run.deliver(map[string][]byte{"outer": []byte("prefix-inner-secret"), "inner": []byte("inner-secret")})
		if got := run.redactText("x prefix-inner-secret"); got != "x "+signed.RedactionMarker(testRedactionKey, "outer") {
			t.Fatalf("redacted %q, want the longer value replaced whole", got)
		}
	}
}

// A marker holding a delivered value would publish it, so that marker falls back: "[redacted:" is within every marker.
func TestRedactionCannotPublishAValueWithinAMarker(t *testing.T) {
	f := newFixture(t, resultStream)
	run := testActiveRun(t, f, "run-v", 1)
	run.deliver(map[string][]byte{"x": []byte("12345678"), "y": []byte("[redacted:")})
	if got := run.redactText("a 12345678 b [redacted: c"); got != "a [*] b [*] c" {
		t.Fatalf("redacted %q, want both values replaced by the fallback", got)
	}
}

// adopting returns a fixture whose Lookup finds a running job printing the credential, and the record of its turn.
func adopting(t *testing.T) (fixture, *kgo.Record) {
	t.Helper()
	f := newFixture(t, []string{
		`{"type":"assistant","message":"token ` + testToken + `"}`,
		completedWith("used " + testToken),
	})
	f.plugin.job = pluginsdk.JobRunning
	return f, f.submit(t, f.host, nil)
}

// held waits until the adopted job is followed, and a while longer for its lines to be read.
func (f fixture) held(t *testing.T) {
	t.Helper()
	for {
		if _, _, follows, _ := f.plugin.counts(); follows > 0 {
			break
		}
		if f.ctx.Err() != nil {
			t.Fatal("the adopted job was never followed")
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
}

// An adopter commits adopting, requests its own open and publishes nothing of the job until it opens; the markers it
// then writes are the run's, the claimant's, and name no credential.
func TestAnAdopterPublishesNothingBeforeItOpens(t *testing.T) {
	f, record := adopting(t)
	f.quiet()
	done := f.executing(t, f.ctx, record, 2)
	if request := f.request(t, 1); request.Attempt != 2 {
		t.Fatalf("adopter's open request %#v", request)
	}
	f.held(t)
	if chunks, kinds := f.chunks(t), f.kinds(t); len(chunks) != 0 || !slices.Equal(kinds, []string{"claimed", "adopting"}) {
		t.Fatalf("before it opens the adopter published %d chunks and statuses %v", len(chunks), kinds)
	}
	f.serving.Store(true)
	if status := <-done; status != kgo.AckAccept || f.lastTask(t).Status.State != wire.TaskStateCompleted {
		t.Fatalf("adopter ended %v; logs:\n%s", status, f.logs.String())
	}
	chunks := f.chunks(t)
	if len(chunks) != 2 || chunks[0].ArtifactUpdate.Artifact.Parts[0].Text != `{"type":"assistant","message":"token `+redacted+`"}` {
		t.Fatalf("adopter's transcript %+v, want both lines with the run's marker", chunks)
	}
	for _, record := range append(f.records(t, operator.TenantTopic(testTenant)), f.records(t, operator.TenantTranscriptTopic(testTenant))...) {
		if payload := decodedPayload(record.Value); bytes.Contains(payload, []byte(testToken)) || bytes.Contains(payload, []byte(credentialName)) {
			t.Fatalf("a feed record holds the credential or its name: %s", payload)
		}
	}
	if result := f.lastTask(t).Artifacts[0].Parts[0].Text; result != "used "+redacted {
		t.Fatalf("result %q, want the credential redacted", result)
	}
}

// An adopter no custodian answers gives its claim back with the job running, for the next claimant to adopt: silence
// says nothing of the key.
func TestAnAdopterWithoutAnAnswerReleasesTheJob(t *testing.T) {
	f, record := adopting(t)
	f.quiet()
	if status := f.execute(t, record, 2); status != kgo.AckRelease {
		t.Fatalf("adopter ended %v; logs:\n%s", status, f.logs.String())
	}
	f.plugin.mu.Lock()
	stops := f.plugin.stops
	f.plugin.mu.Unlock()
	if chunks := f.chunks(t); stops != 0 || len(chunks) != 0 {
		t.Fatalf("stops=%d chunks=%+v, want the job left running and no line published", stops, chunks)
	}
	if kinds := f.kinds(t); !slices.Equal(kinds, []string{"claimed", "adopting", "released"}) {
		t.Fatalf("statuses %v, want claimed, adopting, then released", kinds)
	}
}

// The held transcript is bounded: the oldest lines go, the log records the gap, and the transcript resumes past it.
func TestAnAdopterDropsTheOldestHeldLines(t *testing.T) {
	line := strings.Repeat("x", 1<<20)
	f := newFixture(t, append(slices.Repeat([]string{line}, 6), doneOutcome))
	f.quiet()
	f.plugin.job = pluginsdk.JobRunning
	done := f.executing(t, f.ctx, f.submit(t, f.host, nil), 2)
	f.request(t, 1)
	f.held(t)
	f.serving.Store(true)
	if status := <-done; status != kgo.AckAccept {
		t.Fatalf("adopter ended %v; logs:\n%s", status, f.logs.String())
	}
	chunks := f.chunks(t)
	first := int64(3 * (len(line) + 1))
	if len(chunks) != 4 || chunks[0].Wherry.Seq != first || !strings.Contains(f.logs.String(), fmt.Sprintf("dropped 3 transcript lines held before the open; the transcript resumes at offset %d", first)) {
		t.Fatalf("%d chunks from seq %d; logs:\n%s", len(chunks), chunks[0].Wherry.Seq, f.logs.String())
	}
}

// The listing of the pool events' end before an open request lists again while its partition fails retriably, fails at
// once on any other partition error, and fails with its context's error when the retriable error outlasts it.
// OFFSET_NOT_AVAILABLE is a retriable error kgo never retries itself.
func TestOpenWaitListsAgainOnlyOnARetriablePartitionError(t *testing.T) {
	for _, test := range []struct {
		name     string
		failures int32
		code     int16
		want     kgo.AckStatus
		logged   string
		// cancels ends the run's context once it has listed again.
		cancels  bool
		listings func(int32) bool
	}{
		{"retriable then listed", 3, kerr.OffsetNotAvailable.Code, kgo.AckAccept, "", false, func(n int32) bool { return n == 4 }},
		{"non-retriable", -1, kerr.UnknownServerError.Code, kgo.AckRelease, "read pool events: " + kerr.UnknownServerError.Error(), false, func(n int32) bool { return n == 1 }},
		{"retriable past the deadline", -1, kerr.OffsetNotAvailable.Code, kgo.AckRelease, "read pool events: " + context.Canceled.Error(), true, func(n int32) bool { return n > 2 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			record := f.submit(t, f.host, nil)
			topic := operator.PoolEventsTopic(testPool)
			seen := &atomic.Int32{}
			f.cluster.ControlKey(int16(kmsg.ListOffsets), func(request kmsg.Request) (kmsg.Response, error, bool) {
				f.cluster.KeepControl()
				listed := request.(*kmsg.ListOffsetsRequest)
				if listed.IsolationLevel != 0 || len(listed.Topics) != 1 || listed.Topics[0].Topic != topic || listed.Topics[0].Partitions[0].Timestamp != -1 {
					return nil, nil, false
				}
				if n := seen.Add(1); test.failures >= 0 && n > test.failures {
					return nil, nil, false
				}
				response := listed.ResponseKind().(*kmsg.ListOffsetsResponse)
				failed := kmsg.ListOffsetsResponseTopic{Topic: topic}
				for _, partition := range listed.Topics[0].Partitions {
					failed.Partitions = append(failed.Partitions, kmsg.ListOffsetsResponseTopicPartition{Partition: partition.Partition, ErrorCode: test.code})
				}
				response.Topics = append(response.Topics, failed)
				return response, nil, true
			})
			ctx, cancel := context.WithCancel(f.ctx)
			defer cancel()
			done := f.executing(t, ctx, record, 1)
			if test.cancels {
				for seen.Load() <= 2 && f.ctx.Err() == nil {
					time.Sleep(10 * time.Millisecond)
				}
				cancel()
			}
			if status := <-done; status != test.want || !strings.Contains(f.logs.String(), test.logged) || !test.listings(seen.Load()) {
				t.Fatalf("status %v after %d listings, want %v; logs:\n%s", status, seen.Load(), test.want, f.logs.String())
			}
		})
	}
}

// openAs writes, signed by key, receiver's open request for attempt of the task record and returns its digest.
func (f fixture) openAs(t *testing.T, record *kgo.Record, receiver string, key ed25519.PrivateKey, attempt int64) string {
	t.Helper()
	return f.requestOf(t, string(record.Key), wire.TaskHash(record.Value), receiver, key, attempt)
}

// requestOf writes, signed by key, receiver's open request for attempt of run's task hash and returns its digest.
func (f fixture) requestOf(t *testing.T, run, hash, receiver string, key ed25519.PrivateKey, attempt int64) string {
	t.Helper()
	attemptKey, err := newAttemptKey()
	if err != nil {
		t.Fatal(err)
	}
	value := must(t)(wire.SignRecord(&wire.OpenRequest{Run: run, Attempt: attempt, Receiver: receiver, TaskHash: hash, Key: wire.NewAttemptKey(&attemptKey.PublicKey)}, key))
	f.produce(t, operator.PoolEventsTopic(testPool), run, value)
	return wire.Digest(value)
}

// claimOf writes, signed by key, receiver's claimed status for attempt of run's task hash at offset on the claude task
// topic.
func (f fixture) claimOf(t *testing.T, run, hash string, offset int64, receiver string, key ed25519.PrivateKey, attempt int64) {
	t.Helper()
	status := f.receiver.status(&wire.Submission{Wherry: wire.SubmissionBinding{Tenant: testTenant, Run: run}}, hash, attempt, "claimed", taskStatus(wire.TaskStateWorking, ""), nil)
	status.Wherry.Receiver, status.Wherry.TaskTopic, status.Wherry.TaskOffset = receiver, f.tasksTopic(), &offset
	f.produce(t, operator.PoolEventsTopic(testPool), run, must(t)(wire.SignRecord(status, key)))
}

// statusAs writes, signed by key, receiver's status of kind for attempt of the task record and returns its value.
func (f fixture) statusAs(t *testing.T, record *kgo.Record, receiver string, key ed25519.PrivateKey, kind string, attempt int64) []byte {
	t.Helper()
	status := f.receiver.status(f.parse(t, record), wire.TaskHash(record.Value), attempt, kind, taskStatus(wire.TaskStateWorking, ""), nil)
	status.Wherry.Receiver = receiver
	if kind == "claimed" {
		status.Wherry.TaskTopic, status.Wherry.TaskOffset = record.Topic, &record.Offset
	}
	value := must(t)(wire.SignRecord(status, key))
	f.produce(t, operator.PoolEventsTopic(testPool), string(record.Key), value)
	return value
}

// servedAs writes receiver's claim for attempt of the task record and has the custodian serve its open request.
func (f fixture) servedAs(t *testing.T, record *kgo.Record, receiver string, key ed25519.PrivateKey, attempt int64) {
	t.Helper()
	f.statusAs(t, record, receiver, key, "claimed", attempt)
	if answer := f.answered(t, f.openAs(t, record, receiver, key, attempt)); answer != "wrap" {
		t.Fatalf("%s's claim answered %q, want served", receiver, answer)
	}
}

// advertiseAs writes receiver's advertisement, signed by key, appended at at.
func (f fixture) advertiseAs(t *testing.T, receiver string, key ed25519.PrivateKey, at time.Time) {
	t.Helper()
	f.advertiseIn(t, testPool, receiver, key, at)
}

// advertiseIn writes receiver's advertisement naming pool, signed by key, appended at at, on the custodian's own pool's
// receivers topic.
func (f fixture) advertiseIn(t *testing.T, pool, receiver string, key ed25519.PrivateKey, at time.Time) {
	t.Helper()
	value := must(t)(wire.SignRecord(&wire.Advertisement{Receiver: receiver, Pool: pool, Offers: []wire.Offer{}, Capacity: 1, AtMS: at.UnixMilli()}, key))
	if err := f.service.Operator.ProduceSync(f.ctx, &kgo.Record{Topic: operator.PoolReceiversTopic(testPool), Key: []byte(receiver), Value: value, Timestamp: at}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// answered serves the pool's open requests once and returns the answer to the request digest names: "wrap" for an
// attempt wrap, a refusal's reason, or empty for none.
func (f fixture) answered(t *testing.T, digest string) string {
	t.Helper()
	return f.answeredBy(t, f.receiver, digest)
}

// answeredBy is answered with custodian serving the requests.
func (f fixture) answeredBy(t *testing.T, custodian *Receiver, digest string) string {
	t.Helper()
	if err := custodian.serveOpens(f.ctx); err != nil {
		t.Fatal(err)
	}
	for _, record := range f.records(t, operator.PoolEventsTopic(testPool)) {
		var wrap wire.AttemptWrap
		var refusal wire.OpenRefusal
		switch {
		case wire.ParseStrict(record.Value, f.keys(), record.Timestamp, &wrap) == nil && wrap.Request == digest:
			return "wrap"
		case wire.ParseStrict(record.Value, f.keys(), record.Timestamp, &refusal) == nil && refusal.Request == digest:
			return refusal.Reason
		}
	}
	return ""
}

// decisions returns the custodian's claim decisions on the pool's events in log order.
func (f fixture) decisions(t *testing.T) []wire.ClaimDecision {
	t.Helper()
	var decisions []wire.ClaimDecision
	for _, record := range f.records(t, operator.PoolEventsTopic(testPool)) {
		var decision wire.ClaimDecision
		if wire.ParseStrict(record.Value, f.keys(), record.Timestamp, &decision) == nil {
			decisions = append(decisions, decision)
		}
	}
	return decisions
}

// Rule 6: a claimant taking over from a served one is served once the previous one released or its lease lapsed, never
// while it advertised in the pool within the lease of the claim. A claim never served holds nothing.
func TestClaimCheckServesANewClaimantOnlyOnceThePreviousIsGone(t *testing.T) {
	for _, test := range []struct {
		name     string
		served   bool
		previous func(f fixture, record *kgo.Record, key ed25519.PrivateKey)
		answer   string
		reason   string
	}{
		{"live and unreleased", true, func(f fixture, _ *kgo.Record, key ed25519.PrivateKey) {
			f.advertiseAs(t, "receiver-b", key, time.Now())
		}, wire.RefusedTwoClaimants, wire.ClaimLive},
		{"released", true, func(f fixture, record *kgo.Record, key ed25519.PrivateKey) {
			f.advertiseAs(t, "receiver-b", key, time.Now())
			f.statusAs(t, record, "receiver-b", key, "released", 1)
		}, "wrap", wire.ClaimReleased},
		{"silent past its lease", true, func(f fixture, _ *kgo.Record, key ed25519.PrivateKey) {
			f.advertiseAs(t, "receiver-b", key, time.Now().Add(-claimantLease-time.Minute))
		}, "wrap", wire.ClaimLease},
		{"advertised in another pool", true, func(f fixture, _ *kgo.Record, key ed25519.PrivateKey) {
			f.advertiseIn(t, "pool-other", "receiver-b", key, time.Now())
		}, "wrap", wire.ClaimLease},
		{"live and never served", false, func(f fixture, _ *kgo.Record, key ed25519.PrivateKey) {
			f.advertiseAs(t, "receiver-b", key, time.Now())
		}, "wrap", wire.ClaimReleased},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.quiet()
			keyB, keyC := f.addPeer(t, "receiver-b", testPool), f.addPeer(t, "receiver-c", testPool)
			record := f.submit(t, f.host, nil)
			if test.served {
				f.servedAs(t, record, "receiver-b", keyB, 1)
			} else {
				f.statusAs(t, record, "receiver-b", keyB, "claimed", 1)
			}
			test.previous(f, record, keyB)
			claim := f.statusAs(t, record, "receiver-c", keyC, "claimed", 2)
			if answer := f.answered(t, f.openAs(t, record, "receiver-c", keyC, 2)); answer != test.answer {
				t.Fatalf("answer %q, want %q; logs:\n%s", answer, test.answer, f.logs.String())
			}
			decisions := f.decisions(t)
			if last := decisions[len(decisions)-1]; last.Claim != wire.Digest(claim) || last.Previous != "receiver-b" || last.Reason != test.reason {
				t.Fatalf("decisions %+v, want the last on the claim from receiver-b, %s", decisions, test.reason)
			}
		})
	}
}

// Rule 6: the previous holder is the latest served claimant not followed by its own release; another claimant's
// refusal, with or without its release, leaves its lease standing and blocks no one once it released.
func TestClaimCheckHoldsTheLeaseUntilThePreviousHolderIsGone(t *testing.T) {
	type keys struct{ b, c, d ed25519.PrivateKey }
	refused := func(t *testing.T, f fixture, record *kgo.Record, k keys) {
		f.advertiseAs(t, "receiver-b", k.b, time.Now())
		f.statusAs(t, record, "receiver-c", k.c, "claimed", 2)
		if answer := f.answered(t, f.openAs(t, record, "receiver-c", k.c, 2)); answer != wire.RefusedTwoClaimants {
			t.Fatalf("first request answered %q, want %q", answer, wire.RefusedTwoClaimants)
		}
	}
	refusedThenReleased := func(t *testing.T, f fixture, record *kgo.Record, k keys) {
		refused(t, f, record, k)
		f.statusAs(t, record, "receiver-c", k.c, "released", 2)
	}
	for _, test := range []struct {
		name     string
		claim    func(t *testing.T, f fixture, record *kgo.Record, k keys) string
		answer   string
		reason   string
		previous string
	}{
		{"the refused claimant claims again", func(t *testing.T, f fixture, record *kgo.Record, k keys) string {
			refusedThenReleased(t, f, record, k)
			f.statusAs(t, record, "receiver-c", k.c, "claimed", 3)
			return f.openAs(t, record, "receiver-c", k.c, 3)
		}, wire.RefusedTwoClaimants, wire.ClaimLive, "receiver-b"},
		{"another claimant after the refused one released", func(t *testing.T, f fixture, record *kgo.Record, k keys) string {
			refusedThenReleased(t, f, record, k)
			f.statusAs(t, record, "receiver-d", k.d, "claimed", 3)
			return f.openAs(t, record, "receiver-d", k.d, 3)
		}, wire.RefusedTwoClaimants, wire.ClaimLive, "receiver-b"},
		{"the holder released", func(t *testing.T, f fixture, record *kgo.Record, k keys) string {
			f.advertiseAs(t, "receiver-b", k.b, time.Now())
			f.statusAs(t, record, "receiver-b", k.b, "released", 1)
			f.statusAs(t, record, "receiver-c", k.c, "claimed", 2)
			return f.openAs(t, record, "receiver-c", k.c, 2)
		}, "wrap", wire.ClaimReleased, "receiver-b"},
		{"the holder silent past its lease after a refusal", func(t *testing.T, f fixture, record *kgo.Record, k keys) string {
			refusedThenReleased(t, f, record, k)
			f.advertiseAs(t, "receiver-b", k.b, time.Now().Add(-claimantLease-time.Minute))
			f.statusAs(t, record, "receiver-c", k.c, "claimed", 3)
			return f.openAs(t, record, "receiver-c", k.c, 3)
		}, "wrap", wire.ClaimLease, "receiver-b"},
		{"the refused claimant claims again without releasing", func(t *testing.T, f fixture, record *kgo.Record, k keys) string {
			refused(t, f, record, k)
			f.statusAs(t, record, "receiver-c", k.c, "claimed", 3)
			return f.openAs(t, record, "receiver-c", k.c, 3)
		}, wire.RefusedTwoClaimants, wire.ClaimLive, "receiver-b"},
		{"another claimant after an unreleased refusal gone silent", func(t *testing.T, f fixture, record *kgo.Record, k keys) string {
			f.advertiseAs(t, "receiver-c", k.c, time.Now().Add(-claimantLease-time.Minute))
			refused(t, f, record, k)
			f.statusAs(t, record, "receiver-d", k.d, "claimed", 3)
			return f.openAs(t, record, "receiver-d", k.d, 3)
		}, wire.RefusedTwoClaimants, wire.ClaimLive, "receiver-b"},
		{"the holder released after an unreleased refusal", func(t *testing.T, f fixture, record *kgo.Record, k keys) string {
			f.advertiseAs(t, "receiver-c", k.c, time.Now())
			refused(t, f, record, k)
			f.statusAs(t, record, "receiver-b", k.b, "released", 1)
			f.statusAs(t, record, "receiver-d", k.d, "claimed", 3)
			return f.openAs(t, record, "receiver-d", k.d, 3)
		}, "wrap", wire.ClaimReleased, "receiver-b"},
		{"the holder restarted", func(t *testing.T, f fixture, record *kgo.Record, k keys) string {
			f.advertiseAs(t, "receiver-b", k.b, time.Now())
			f.statusAs(t, record, "receiver-b", k.b, "claimed", 2)
			return f.openAs(t, record, "receiver-b", k.b, 2)
		}, "wrap", wire.ClaimUnchanged, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.quiet()
			k := keys{f.addPeer(t, "receiver-b", testPool), f.addPeer(t, "receiver-c", testPool), f.addPeer(t, "receiver-d", testPool)}
			record := f.submit(t, f.host, nil)
			f.servedAs(t, record, "receiver-b", k.b, 1)
			if answer := f.answered(t, test.claim(t, f, record, k)); answer != test.answer {
				t.Fatalf("answer %q, want %q; logs:\n%s", answer, test.answer, f.logs.String())
			}
			decisions := f.decisions(t)
			if last := decisions[len(decisions)-1]; last.Previous != test.previous || last.Reason != test.reason {
				t.Fatalf("decisions %+v, want the last from %q, %s", decisions, test.previous, test.reason)
			}
		})
	}
}

// Rule 6: a claim after the previous holder's membership ended is served without its advertisement, also when the
// advertisements do not read. The events are read while the holder is live, since a full read no longer counts an ended
// receiver's claim.
func TestClaimCheckServesAClaimAfterThePreviousHolderEnded(t *testing.T) {
	for _, unread := range []bool{false, true} {
		t.Run(fmt.Sprintf("advertisements unread %t", unread), func(t *testing.T) {
			servesAClaimAfterThePreviousHolderEnded(t, unread)
		})
	}
}

func servesAClaimAfterThePreviousHolderEnded(t *testing.T, unread bool) {
	f := newFixture(t, resultStream)
	f.quiet()
	keyB, keyC := f.addPeer(t, "receiver-b", testPool), f.addPeer(t, "receiver-c", testPool)
	record := f.submit(t, f.host, nil)
	f.servedAs(t, record, "receiver-b", keyB, 1)
	f.advertiseAs(t, "receiver-b", keyB, time.Now())
	f.statusAs(t, record, "receiver-c", keyC, "claimed", 2)
	digest := f.openAs(t, record, "receiver-c", keyC, 2)
	events, err := f.receiver.readEvents(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	f.revoke(t, "receiver-b")
	keys, err := loadPoolKeys(f.receiver.poolKeysDir())
	if err != nil {
		t.Fatal(err)
	}
	check := &claimCheck{r: f.receiver, events: events, keys: keys, now: time.Now(), terms: testTerms()}
	request := slices.IndexFunc(events.requests, func(request openRequest) bool { return request.digest == digest })
	if unread {
		f.failReadOnce(t, operator.PoolReceiversTopic(testPool))
	}
	answer := check.decide(f.ctx, events.requests[request])
	var decision wire.ClaimDecision
	var wrap wire.AttemptWrap
	if len(answer) != 2 || wire.ParseStrict(answer[0].Value, f.keys(), time.Now(), &decision) != nil || wire.ParseStrict(answer[1].Value, f.keys(), time.Now(), &wrap) != nil {
		t.Fatalf("answer of %d records, want a decision and a wrap; logs:\n%s", len(answer), f.logs.String())
	}
	if decision.Previous != "receiver-b" || decision.Reason != wire.ClaimLease || !decision.Served {
		t.Fatalf("decision %+v, want served from receiver-b, %s", decision, wire.ClaimLease)
	}
}

// Rule 3: only the latest ownership event's claimant for its attempt is served; a request appended after its own
// claimant released is refused.
func TestClaimCheckRefusesARequestAfterItsRelease(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	keyB := f.addPeer(t, "receiver-b", testPool)
	record := f.submit(t, f.host, nil)
	f.statusAs(t, record, "receiver-b", keyB, "claimed", 1)
	f.statusAs(t, record, "receiver-b", keyB, "released", 1)
	if answer := f.answered(t, f.openAs(t, record, "receiver-b", keyB, 1)); answer != wire.RefusedNotClaimant {
		t.Fatalf("answer %q, want %q", answer, wire.RefusedNotClaimant)
	}
}

// Rule 1: a request is served only when signed by the live pool member it names.
func TestClaimCheckRefusesARequestNotFromItsLiveClaimant(t *testing.T) {
	t.Run("signed by another member", func(t *testing.T) {
		f := newFixture(t, resultStream)
		f.quiet()
		keyB, keyC := f.addPeer(t, "receiver-b", testPool), f.addPeer(t, "receiver-c", testPool)
		record := f.submit(t, f.host, nil)
		f.statusAs(t, record, "receiver-b", keyB, "claimed", 1)
		attemptKey, _ := newAttemptKey()
		forged := must(t)(signed.Sign(wire.OpenRequestType, "receiver-c", &wire.OpenRequest{V: 1, Run: testRun("run-a"), Attempt: 1, Receiver: "receiver-b",
			TaskHash: wire.TaskHash(record.Value), Key: wire.NewAttemptKey(&attemptKey.PublicKey)}, keyC))
		f.produce(t, operator.PoolEventsTopic(testPool), testRun("run-a"), forged)
		if answer := f.answered(t, wire.Digest(forged)); answer != wire.RefusedNotClaimant {
			t.Fatalf("answer %q, want %q", answer, wire.RefusedNotClaimant)
		}
	})
	t.Run("revoked", func(t *testing.T) {
		f := newFixture(t, resultStream)
		f.quiet()
		keyB := f.addPeer(t, "receiver-b", testPool)
		record := f.submit(t, f.host, nil)
		f.statusAs(t, record, "receiver-b", keyB, "claimed", 1)
		digest := f.openAs(t, record, "receiver-b", keyB, 1)
		// The events are read while receiver-b is live, so its claim and request stand and rules 2 to 4 serve it.
		events, err := f.receiver.readEvents(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		f.revoke(t, "receiver-b")
		keys, err := loadPoolKeys(f.receiver.poolKeysDir())
		if err != nil {
			t.Fatal(err)
		}
		check := &claimCheck{r: f.receiver, events: events, keys: keys, now: time.Now(), terms: testTerms()}
		request := slices.IndexFunc(events.requests, func(request openRequest) bool { return request.digest == digest })
		answer := check.decide(f.ctx, events.requests[request])
		var refusal wire.OpenRefusal
		if len(answer) != 1 || wire.ParseStrict(answer[0].Value, f.keys(), time.Now(), &refusal) != nil || refusal.Reason != wire.RefusedNotClaimant {
			t.Fatalf("answer of %d records, refusal %+v; want %q alone", len(answer), refusal, wire.RefusedNotClaimant)
		}
	})
}

// A claim's decision is durable: a later request for the same claim is decided by it, whatever the advertisements now
// hold, since compaction removes the history it was taken from.
func TestClaimDecisionIsReusedForItsClaim(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	keyB, keyC := f.addPeer(t, "receiver-b", testPool), f.addPeer(t, "receiver-c", testPool)
	record := f.submit(t, f.host, nil)
	f.servedAs(t, record, "receiver-b", keyB, 1)
	f.advertiseAs(t, "receiver-b", keyB, time.Now().Add(-claimantLease-time.Minute))
	f.statusAs(t, record, "receiver-c", keyC, "claimed", 2)
	claimed := time.Now()
	if answer := f.answered(t, f.openAs(t, record, "receiver-c", keyC, 2)); answer != "wrap" {
		t.Fatalf("first request answered %q, want a wrap", answer)
	}
	// The history the decision was taken from is gone: receiver-b now advertised within the lease of the claim.
	f.advertiseAs(t, "receiver-b", keyB, claimed.Add(-time.Second))
	if answer := f.answered(t, f.openAs(t, record, "receiver-c", keyC, 2)); answer != "wrap" {
		t.Fatalf("second request answered %q, want a wrap under the claim's decision", answer)
	}
	if decisions := f.decisions(t); len(decisions) != 2 || decisions[1].Reason != wire.ClaimLease {
		t.Fatalf("decisions %+v, want receiver-b's and one lease decision", decisions)
	}
}

// A claim decision outlives its custodian: verified against the key current when it was appended, it decides a later
// request for the same claim after its custodian is revoked and the advertisement it used is gone.
func TestClaimDecisionOutlivesItsCustodian(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	keyB, keyC, keyD := f.addPeer(t, "receiver-b", testPool), f.addPeer(t, "receiver-c", testPool), f.addPeer(t, "receiver-d", testPool)
	record := f.submit(t, f.host, nil)
	f.statusAs(t, record, "receiver-b", keyB, "claimed", 1)
	lapsed := time.Now().Add(-claimantLease - time.Minute)
	f.advertiseAs(t, "receiver-b", keyB, lapsed)
	claim := f.statusAs(t, record, "receiver-c", keyC, "claimed", 2)
	claimed := time.Now()
	decision := &wire.ClaimDecision{Custodian: "receiver-d", Claim: wire.Digest(claim), Previous: "receiver-b", Reason: wire.ClaimLease, Advertisement: &wire.AdvertisedRef{AtMS: lapsed.UnixMilli()}, Served: true}
	f.produce(t, operator.PoolEventsTopic(testPool), testRun("run-a"), must(t)(wire.SignRecord(decision, keyD)))
	f.revoke(t, "receiver-d")
	// The advertisement the decision used is gone: receiver-b now advertised within the lease of the claim.
	f.advertiseAs(t, "receiver-b", keyB, claimed.Add(-time.Second))
	if answer := f.answered(t, f.openAs(t, record, "receiver-c", keyC, 2)); answer != "wrap" {
		t.Fatalf("request answered %q, want a wrap under receiver-d's decision; logs:\n%s", answer, f.logs.String())
	}
	if decisions := f.decisions(t); len(decisions) != 0 {
		t.Fatalf("decisions %+v, want receiver-d's reused", decisions)
	}
}

// A claim decision outlives its custodian across a restart: a custodian started after the deciding custodian's
// revocation, holding nothing in memory, verifies the decision under the ended custodian's key and reuses it. The broker
// refuses a revoked receiver's later writes (revocation removes its ACLs first), which kfake does not enforce.
func TestClaimDecisionOutlivesItsCustodianAcrossARestart(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	keyB, keyC, keyD := f.addPeer(t, "receiver-b", testPool), f.addPeer(t, "receiver-c", testPool), f.addPeer(t, "receiver-d", testPool)
	record := f.submit(t, f.host, nil)
	f.statusAs(t, record, "receiver-b", keyB, "claimed", 1)
	live := time.Now()
	f.advertiseAs(t, "receiver-b", keyB, live)
	claim := f.statusAs(t, record, "receiver-c", keyC, "claimed", 2)
	decision := must(t)(wire.SignRecord(&wire.ClaimDecision{Custodian: "receiver-d", Claim: wire.Digest(claim), Previous: "receiver-b", Reason: wire.ClaimLive, Advertisement: &wire.AdvertisedRef{AtMS: live.UnixMilli()}}, keyD))
	f.produce(t, operator.PoolEventsTopic(testPool), testRun("run-a"), decision)
	f.revoke(t, "receiver-d")
	// The advertisement the decision used is gone: receiver-b's latest advertisement before the claim lapsed.
	f.advertiseAs(t, "receiver-b", keyB, live.Add(-claimantLease-time.Minute))
	custodian := restarted(t, f.receiver)
	if err := custodian.configure(f.ctx); err != nil {
		t.Fatal(err)
	}
	custodian.assign(true)
	if err := custodian.tend(f.ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if answer := f.answeredBy(t, custodian, f.openAs(t, record, "receiver-c", keyC, 2)); answer != wire.RefusedTwoClaimants {
		t.Fatalf("request answered %q, want %q; logs:\n%s", answer, wire.RefusedTwoClaimants, f.logs.String())
	}
	if decisions := f.decisions(t); len(decisions) != 0 {
		t.Fatalf("decisions %+v of the restarted custodian, want the ended custodian's reused", decisions)
	}
}

// revoke ends receiver's membership and refreshes the fixture's receiver.
func (f fixture) revoke(t *testing.T, receiver string) {
	t.Helper()
	denied := func(context.Context, string, string) error { return operator.ErrAuthorizationDenied }
	revocationBounds(t, f.service, "1")
	if result := f.service.RevokeReceiver(f.ctx, receiver, time.Millisecond, denied); !result.Complete {
		t.Fatalf("revoke receiver: %#v", result.Steps)
	}
	revocationBounds(t, f.service, "")
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
}

// The custodian takes the open bound from the events: task_ended_by for a claim whose job started or whose claimant
// committed adopting, task_started_by otherwise, whatever the request's attempt.
func TestClaimCheckBoundComesFromTheEvents(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	keyB := f.addPeer(t, "receiver-b", testPool)
	startBy := time.Now().Add(time.Second).Unix()
	bounded := func(name string) func(*wire.Submission) {
		return func(task *wire.Submission) {
			named(task, name)
			task.Wherry.TaskStartedBy, task.Wherry.TaskEndedBy = startBy, startBy+60
		}
	}
	first, adopted := f.submit(t, f.host, bounded("run-first")), f.submit(t, f.host, bounded("run-adopted"))
	for _, record := range []*kgo.Record{first, adopted} {
		f.statusAs(t, record, "receiver-b", keyB, "claimed", 2)
	}
	f.statusAs(t, adopted, "receiver-b", keyB, "adopting", 2)
	time.Sleep(time.Until(wire.PastBound(startBy)))
	if answer := f.answered(t, f.openAs(t, first, "receiver-b", keyB, 2)); answer != wire.RefusedPastBound {
		t.Fatalf("a first start past task_started_by answered %q, want %q", answer, wire.RefusedPastBound)
	}
	if answer := f.answered(t, f.openAs(t, adopted, "receiver-b", keyB, 2)); answer != "wrap" {
		t.Fatalf("an adoption before task_ended_by answered %q, want a wrap", answer)
	}
}

// An envelope copied into a task it is not bound to, or failing the section 5 check, never releases its content key:
// the custodian refuses envelope invalid before any attempt wrap, and the run fails with that reason and no key-lost, so
// its host does not resubmit it.
func TestInvalidEnvelopeIsRefusedBeforeAnyWrap(t *testing.T) {
	for name, edit := range map[string]func(*signed.EnvelopeValues){
		"run":             func(values *signed.EnvelopeValues) { values.Binding.Run = testRun("run-other") },
		"tenant":          func(values *signed.EnvelopeValues) { values.Binding.Tenant = "tenant-b" },
		"pool":            func(values *signed.EnvelopeValues) { values.Binding.Pool = "pool-b" },
		"task_started_by": func(values *signed.EnvelopeValues) { values.Binding.TaskStartedBy++ },
		"task_ended_by":   func(values *signed.EnvelopeValues) { values.Binding.TaskEndedBy++ },
		"section 5":       func(values *signed.EnvelopeValues) { delete(values.Credentials, credentialName) },
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.envelope = func(_ *wire.Submission, values *signed.EnvelopeValues) { edit(values) }
			if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckReject {
				t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
			}
			for _, record := range f.records(t, operator.PoolEventsTopic(testPool)) {
				if recordType(record.Value) == wire.AttemptWrapType {
					t.Fatal("the custodian wrapped the content key of an invalid envelope")
				}
			}
			if f.answeredWith(t, wire.RefusedEnvelopeInvalid) != 1 {
				t.Fatalf("no envelope invalid refusal; logs:\n%s", f.logs.String())
			}
			statuses := f.eventStatuses(t)
			terminal := statuses[len(statuses)-1]
			if terminal.Wherry.Kind != "terminal" || terminal.StatusUpdate.Status.State != wire.TaskStateFailed || terminal.Wherry.Event != "" ||
				terminal.StatusUpdate.Status.Message.Parts[0].Text != wire.RefusedEnvelopeInvalid {
				t.Fatalf("terminal %+v, want the run failed envelope invalid without key-lost", terminal)
			}
			if dispatches, lookups, _, _ := f.plugin.counts(); dispatches != 0 || lookups != 0 {
				t.Fatalf("dispatches %d, lookups %d; want neither", dispatches, lookups)
			}
			if got := scrapedValue(t, f.receiver, `wherry_runs_key_lost_total{pool="pool-a"}`); got != 0 {
				t.Fatalf("runs key-lost = %v, want 0", got)
			}
		})
	}
}

// The custodian checks an envelope's credentials against the terms its pass read, not the receiver's snapshot, which can
// lag a terms change: an envelope sealed under new terms that drop an optional credential is served, and one that still
// carries the dropped credential is refused.
func TestCustodianChecksEnvelopesUnderThePassTerms(t *testing.T) {
	withOptional := func() operator.Terms {
		terms := testTerms()
		terms.Agents[0].Credentials = append(terms.Agents[0].Credentials,
			operator.AgentCredential{TaskCredential: wire.TaskCredential{Name: "github-token", Env: "GH_TOKEN"}, Required: new(bool)})
		return terms
	}
	for name, test := range map[string]struct {
		sealedUnder operator.Terms
		answer      string
	}{
		"sealed under the new terms":      {testTerms(), "wrap"},
		"carrying the dropped credential": {withOptional(), wire.RefusedEnvelopeInvalid},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixtureWith(t, resultStream, withOptional(), testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream}})
			f.quiet()
			keyB := f.addPeer(t, "receiver-b", testPool)
			if err := f.service.PoolApply(f.ctx, testPool, testTerms(), false); err != nil {
				t.Fatal(err)
			}
			record := f.submit(t, f.host, func(submission *wire.Submission) {
				sealTask(t, submission, test.sealedUnder, f.poolKey(t), nil)
			})
			f.statusAs(t, record, "receiver-b", keyB, "claimed", 1)
			if answer := f.answered(t, f.openAs(t, record, "receiver-b", keyB, 1)); answer != test.answer {
				t.Fatalf("answer %q, want %q; logs:\n%s", answer, test.answer, f.logs.String())
			}
			if state, _, _ := f.receiver.snapshot(); len(state.Pools[testPool].Agents[0].Credentials) != 2 {
				t.Fatal("the receiver's snapshot read the new terms; the check no longer covers a stale snapshot")
			}
		})
	}
}

// peer is another configured member of the pool, its key known to the fixture's receiver.
func (f fixture) peer(t *testing.T, id string) *Receiver {
	t.Helper()
	r := f.member(t, id)
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	return r
}

// holding gives r a pool key in its state directory, as a former custodian holds one, and returns its public half.
func holding(t *testing.T, r *Receiver) *ecdsa.PublicKey {
	t.Helper()
	private, err := newAttemptKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(r.poolKeysDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	key := &poolKey{Key: jose.JSONWebKey{Key: private}, CreatedMS: time.Now().UnixMilli(), PublishedMS: time.Now().UnixMilli(), DeadlineMS: time.Now().Add(time.Hour).UnixMilli()}
	if err := key.save(r.poolKeysDir()); err != nil {
		t.Fatal(err)
	}
	return &private.PublicKey
}

// sealedTo is a submit edit that seals the task's envelope to key rather than the pool's current key.
func sealedTo(t *testing.T, key *ecdsa.PublicKey) func(*wire.Submission) {
	return func(task *wire.Submission) { sealTask(t, task, testTerms(), key, nil) }
}

// A task under the key of a custodian two changes back opens on the current one: the former custodian, which holds its
// key, re-keys it straight to the current key.
func TestTaskUnderAFormerCustodiansKeyOpensThroughItsRekey(t *testing.T) {
	f := newFixture(t, resultStream)
	a, b := f.peer(t, "receiver-x"), f.peer(t, "receiver-y")
	keyA := holding(t, a)
	holding(t, b)
	record := f.submit(t, f.host, sealedTo(t, keyA))
	done := f.executing(t, f.ctx, record, 1)
	f.request(t, 1)
	for _, former := range []*Receiver{a, b} {
		if err := former.serveOpens(f.ctx); err != nil {
			t.Fatal(err)
		}
	}
	if status := <-done; status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	var rekeys []wire.RekeyWrap
	for _, record := range f.records(t, operator.PoolEventsTopic(testPool)) {
		var rekey wire.RekeyWrap
		if wire.ParseStrict(record.Value, f.receiver.peers, record.Timestamp, &rekey) == nil {
			rekeys = append(rekeys, rekey)
		}
	}
	if len(rekeys) != 1 || rekeys[0].Holder != "receiver-x" || rekeys[0].RecipientKID != signed.KeyThumbprint(f.poolKey(t)) {
		t.Fatalf("re-key wraps %+v, want receiver-x's to the current key alone", rekeys)
	}
	if dispatches, credential, _ := f.dispatched(t); dispatches != 1 || string(credential) != testToken {
		t.Fatalf("dispatches %d with %q", dispatches, credential)
	}
}

// A holder that is live but does not re-key does not hold the open: the custodian refuses key unavailable when the
// request's wait passes, and a first delivery ends key-lost without a Lookup.
func TestStalledHolderLeavesTheKeyUnavailable(t *testing.T) {
	f := newFixture(t, resultStream)
	a := f.peer(t, "receiver-x")
	keyA := holding(t, a)
	if err := a.advertise(f.ctx); err != nil {
		t.Fatal(err)
	}
	if status := f.execute(t, f.submit(t, f.host, sealedTo(t, keyA)), 1); status != kgo.AckReject {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	terminal := f.eventStatuses(t)[len(f.eventStatuses(t))-1]
	if terminal.Wherry.Event != wire.EventKeyLost || terminal.StatusUpdate.Status.Message.Parts[0].Text != keyLostReason {
		t.Fatalf("terminal %+v, want key-lost", terminal.Wherry)
	}
	if dispatches, lookups, _, _ := f.plugin.counts(); dispatches != 0 || lookups != 0 {
		t.Fatalf("dispatches %d, lookups %d; want neither on a first delivery", dispatches, lookups)
	}
	if got := scrapedValue(t, f.receiver, `wherry_runs_key_lost_total{pool="pool-a"}`); got != 1 {
		t.Fatalf("runs key-lost = %v, want 1", got)
	}
}

// Silence is never key-lost: an open request no custodian answers ends at its wait and the claimant releases its claim
// and the task, long before the open bound.
func TestUnansweredOpenReleasesTheTask(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	began := time.Now()
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckRelease {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	if waited := time.Since(began); waited > 20*time.Second {
		t.Fatalf("released after %s, want at the request's wait", waited)
	}
	if kinds := f.kinds(t); !slices.Equal(kinds, []string{"claimed", "released"}) {
		t.Fatalf("statuses %v, want claimed then released", kinds)
	}
	scraped(t, f.receiver, `wherry_open_waits_total{outcome="unanswered",pool="pool-a",tenant="tenant-a"} 1`)
}

// A job a dispatch created whose receiver died before its started event is found by Lookup when the key is unavailable:
// it is stopped and the run fails without key-lost, since its job may have run.
func TestJobFoundAfterKeyUnavailableIsNotKeyLost(t *testing.T) {
	f := newFixture(t, resultStream)
	f.plugin.job = pluginsdk.JobRunning
	lost, _ := newAttemptKey()
	if status := f.execute(t, f.submit(t, f.host, sealedTo(t, &lost.PublicKey)), 2); status != kgo.AckReject {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	statuses := f.eventStatuses(t)
	terminal := statuses[len(statuses)-1]
	if terminal.Wherry.Kind != "terminal" || terminal.Wherry.Event != "" || terminal.StatusUpdate.Status.Message.Parts[0].Text != keyLostReason {
		t.Fatalf("terminal %+v, want the run failed without key-lost", terminal)
	}
	f.plugin.mu.Lock()
	defer f.plugin.mu.Unlock()
	if f.plugin.lookups == 0 || f.plugin.stops != 1 || f.plugin.dispatches != 0 {
		t.Fatalf("lookups %d, stops %d, dispatches %d; want the job found and stopped", f.plugin.lookups, f.plugin.stops, f.plugin.dispatches)
	}
}

// keyUnavailableOnRedelivery returns a fixture and a task whose key no holder has, claimed before by another member,
// with Lookup failing once the open request is written.
func keyUnavailableOnRedelivery(t *testing.T, edit func(*wire.Submission)) (fixture, *kgo.Record) {
	t.Helper()
	f := newFixture(t, resultStream)
	keyB := f.addPeer(t, "receiver-b", testPool)
	lost, _ := newAttemptKey()
	record := f.submit(t, f.host, func(task *wire.Submission) {
		if edit != nil {
			edit(task)
		}
		sealedTo(t, &lost.PublicKey)(task)
	})
	f.statusAs(t, record, "receiver-b", keyB, "claimed", 1)
	f.statusAs(t, record, "receiver-b", keyB, "released", 1)
	go func() {
		for f.ctx.Err() == nil && len(f.openRequests(t)) == 0 {
			time.Sleep(20 * time.Millisecond)
		}
		f.plugin.mu.Lock()
		f.plugin.lookupErr = errors.New("platform unavailable")
		f.plugin.mu.Unlock()
	}()
	return f, record
}

// Refused key unavailable with Lookup unresolved, the claimant writes a reconciliation status and asks again while it
// holds the task; when it dies, the next claimant continues Lookup without an open request or a dispatch, and ends
// key-lost only on a conclusive absent job.
func TestReconciliationHoldsTheTaskAndTheNextClaimantContinuesIt(t *testing.T) {
	f, record := keyUnavailableOnRedelivery(t, nil)
	died, die := context.WithCancel(f.ctx)
	done := f.executing(t, died, record, 2)
	for !slices.Contains(f.kinds(t), "reconciling") {
		select {
		case status := <-done:
			t.Fatalf("ended %v before reconciling; logs:\n%s", status, f.logs.String())
		case <-time.After(50 * time.Millisecond):
		}
	}
	select {
	case status := <-done:
		t.Fatalf("ended %v while Lookup is unresolved, want the task held", status)
	case <-time.After(time.Second):
	}
	die()
	<-done
	f.plugin.mu.Lock()
	f.plugin.lookupErr = nil
	f.plugin.mu.Unlock()
	requests := len(f.openRequests(t))
	if status := f.execute(t, record, 3); status != kgo.AckReject {
		t.Fatalf("next claimant ended %v; logs:\n%s", status, f.logs.String())
	}
	statuses := f.eventStatuses(t)
	if terminal := statuses[len(statuses)-1]; terminal.Wherry.Attempt != 3 || terminal.Wherry.Event != wire.EventKeyLost {
		t.Fatalf("terminal %+v, want attempt 3 key-lost", terminal.Wherry)
	}
	if dispatches, _, _ := f.dispatched(t); dispatches != 0 || len(f.openRequests(t)) != requests {
		t.Fatalf("dispatches %d, open requests %d then %d; want the next claimant to request and dispatch nothing", dispatches, requests, len(f.openRequests(t)))
	}
}

// A run whose Lookup stays unresolved past task_ended_by ends with the end bound, never key-lost.
func TestUnresolvedPastTaskEndedByIsAnEndBoundFailure(t *testing.T) {
	endBy := time.Now().Add(3 * time.Second).Unix()
	f, record := keyUnavailableOnRedelivery(t, func(task *wire.Submission) { task.Wherry.TaskStartedBy, task.Wherry.TaskEndedBy = endBy, endBy })
	if status := f.execute(t, record, 2); status != kgo.AckReject {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	statuses := f.eventStatuses(t)
	terminal := statuses[len(statuses)-1]
	if terminal.Wherry.Event != "" || terminal.StatusUpdate.Status.Message.Parts[0].Text != wire.NotEndedBy(endBy) {
		t.Fatalf("terminal %+v, want the end-bound failure without key-lost", terminal)
	}
}

// An adopter refused key unavailable requests once more; a holder re-keying meanwhile serves the second request and the
// job runs on.
func TestAnAdopterRefusedOnceRequestsAgain(t *testing.T) {
	f, record := adopting(t)
	a := f.peer(t, "receiver-x")
	keyA := holding(t, a)
	record = f.submit(t, f.host, func(task *wire.Submission) {
		named(task, "run-b")
		sealedTo(t, keyA)(task)
	})
	f.quiet()
	done := f.executing(t, f.ctx, record, 2)
	refusal := &wire.OpenRefusal{Custodian: testReceiver, Request: wire.Digest(f.requestRecord(t, 1).Value), Reason: wire.RefusedKeyUnavailable}
	f.produce(t, operator.PoolEventsTopic(testPool), testRun("run-b"), must(t)(wire.SignRecord(refusal, f.receiver.signing)))
	f.requestRecord(t, 2)
	if err := a.serveOpens(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.serving.Store(true)
	if status := <-done; status != kgo.AckAccept || f.lastTask(t).Status.State != wire.TaskStateCompleted {
		t.Fatalf("adopter ended %v; logs:\n%s", status, f.logs.String())
	}
	if requests := f.openRequests(t); len(requests) != 2 {
		t.Fatalf("open requests %d, want the refused one and one more", len(requests))
	}
	f.plugin.mu.Lock()
	defer f.plugin.mu.Unlock()
	if f.plugin.stops != 1 {
		t.Fatalf("stops %d, want the job stopped once, when it ended", f.plugin.stops)
	}
}

// answeredWith counts the refusals of reason on the pool's events.
func (f fixture) answeredWith(t *testing.T, reason string) int {
	t.Helper()
	n := 0
	for _, record := range f.records(t, operator.PoolEventsTopic(testPool)) {
		var refusal wire.OpenRefusal
		if wire.ParseStrict(record.Value, f.keys(), record.Timestamp, &refusal) == nil && refusal.Reason == reason {
			n++
		}
	}
	return n
}

// Each refusal takes the action of its row in the table.
func TestEachRefusalTakesItsTableAction(t *testing.T) {
	for _, test := range []struct {
		reason string
		ack    kgo.AckStatus
		kinds  []string
		text   string
	}{
		{wire.RefusedTwoClaimants, kgo.AckRelease, []string{"claimed", "released"}, ""},
		{wire.RefusedNotClaimant, kgo.AckRelease, []string{"claimed", "released"}, ""},
		{wire.RefusedPoolSuspended, kgo.AckRelease, []string{"claimed", "released"}, ""},
		{wire.RefusedTerminal, kgo.AckAccept, []string{"claimed"}, ""},
		{wire.RefusedPastBound, kgo.AckReject, []string{"claimed", "terminal"}, "not started by task_started_by"},
		{wire.RefusedKeyUnavailable, kgo.AckReject, []string{"claimed", "terminal"}, keyLostReason},
		{wire.RefusedEnvelopeInvalid, kgo.AckReject, []string{"claimed", "terminal"}, wire.RefusedEnvelopeInvalid},
	} {
		t.Run(test.reason, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.quiet()
			done := f.executing(t, f.ctx, f.submit(t, f.host, nil), 1)
			refusal := &wire.OpenRefusal{Custodian: testReceiver, Request: wire.Digest(f.requestRecord(t, 1).Value), Reason: test.reason}
			f.produce(t, operator.PoolEventsTopic(testPool), testRun("run-a"), must(t)(wire.SignRecord(refusal, f.receiver.signing)))
			if status := <-done; status != test.ack {
				t.Fatalf("status %v, want %v; logs:\n%s", status, test.ack, f.logs.String())
			}
			if kinds := f.kinds(t); !slices.Equal(kinds, test.kinds) {
				t.Fatalf("statuses %v, want %v", kinds, test.kinds)
			}
			if test.text != "" && !strings.HasPrefix(f.lastTask(t).Status.Message.Parts[0].Text, test.text) {
				t.Fatalf("terminal %+v", f.lastTask(t).Status)
			}
			if dispatches, _, _ := f.dispatched(t); dispatches != 0 {
				t.Fatal("dispatched a refused run")
			}
		})
	}
}

// A claim given back to the pool is released on the events before the share record goes back, so the next claimant is
// served at once.
func TestReleasedIsCommittedBeforeTheTaskIsReleased(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	record := f.submit(t, f.host, nil)
	if status := f.execute(t, record, 1); status != kgo.AckRelease {
		t.Fatalf("status %v", status)
	}
	if kinds := f.kinds(t); !slices.Equal(kinds, []string{"claimed", "released"}) {
		t.Fatalf("statuses when the task is released %v, want its claim released", kinds)
	}
	keyB := f.addPeer(t, "receiver-b", testPool)
	f.advertiseAs(t, testReceiver, f.receiver.signing, time.Now())
	f.statusAs(t, record, "receiver-b", keyB, "claimed", 2)
	if answer := f.answered(t, f.openAs(t, record, "receiver-b", keyB, 2)); answer != "wrap" {
		t.Fatalf("the next claimant was answered %q, want a wrap", answer)
	}
}

// A released status whose transaction does not commit leaves the share record unacknowledged, for its acquisition lock
// to return it.
func TestAFailedReleasedCommitLeavesTheTaskUnacknowledged(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	record := f.submit(t, f.host, nil)
	// The run's commits are its claimed status, its open request, then its released status.
	commits := 0
	f.cluster.ControlKey(kmsg.EndTxn.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		req := request.(*kmsg.EndTxnRequest)
		if req.TransactionalID != operator.TransactionalPrefix(testPool)+testRun("run-a") || !req.Commit {
			f.cluster.KeepControl()
			return nil, nil, false
		}
		if commits++; commits < 3 {
			f.cluster.KeepControl()
			return nil, nil, false
		}
		response := req.ResponseKind().(*kmsg.EndTxnResponse)
		response.SetVersion(req.Version)
		response.ErrorCode = kerr.InvalidTxnState.Code
		return response, nil, true
	})
	if status := f.execute(t, record, 1); status != 0 {
		t.Fatalf("status %v, want no acknowledgement; logs:\n%s", status, f.logs.String())
	}
	if kinds := f.kinds(t); !slices.Equal(kinds, []string{"claimed"}) {
		t.Fatalf("statuses %v, want the claim alone", kinds)
	}
}

// Records under a run's key whose run ID their signer does not derive, from another tenant and from another host of the
// run's tenant, never take the run: the task after them is admitted, opened by the custodian and re-keyed on drain.
func TestMismatchingEarlierRecordsNeverWin(t *testing.T) {
	submitted := func(t *testing.T) (fixture, *kgo.Record) {
		f := newFixture(t, resultStream)
		f.submit(t, f.tenantB(t), asTenantB)
		f.submit(t, f.hostOf(t, testTenant, "host-b"), func(task *wire.Submission) { task.Wherry.Host = "host-b" })
		record := f.submit(t, f.host, nil)
		record.Offset = f.records(t, f.tasksTopic())[2].Offset
		return f, record
	}
	t.Run("opened", func(t *testing.T) {
		f, record := submitted(t)
		if status := f.execute(t, record, 1); status != kgo.AckAccept {
			t.Fatalf("the task ended %v; logs:\n%s", status, f.logs.String())
		}
	})
	t.Run("re-keyed", func(t *testing.T) {
		f, record := submitted(t)
		successor, err := newAttemptKey()
		if err != nil {
			t.Fatal(err)
		}
		if err := f.receiver.handOver(f.ctx, wire.PoolKey{KeyID: signed.KeyThumbprint(&successor.PublicKey), Key: jose.JSONWebKey{Key: &successor.PublicKey}}); err != nil {
			t.Fatal(err)
		}
		if wraps := f.rekeys(t)[string(record.Key)]; len(wraps) != 1 || wraps[0].TaskHash != wire.TaskHash(record.Value) {
			t.Fatalf("re-key wraps %+v, want one for the task", wraps)
		}
	})
}

// A run's earlier record rejected at admission never becomes the run's task: rejected statuses name none, and the
// valid record after it runs.
func TestARejectedHashIsNeverTheRunsTask(t *testing.T) {
	now := time.Now()
	runCap := int64(operator.MaxRunDurationMS / 1000)
	for name, test := range map[string]struct {
		edit   func(*wire.Submission)
		reason string
		// appended moves the record's append time.
		appended time.Duration
	}{
		"past task_started_by at its append": {func(task *wire.Submission) {
			task.Wherry.TaskStartedBy, task.Wherry.TaskEndedBy = now.Add(-time.Minute).Unix(), now.Add(time.Minute).Unix()
		}, "not started by task_started_by", 0},
		"appended after task_started_by": {func(task *wire.Submission) {
			task.Wherry.TaskStartedBy, task.Wherry.TaskEndedBy = now.Add(time.Minute).Unix(), now.Add(2*time.Minute).Unix()
		}, "task_started_by is before its append time", 2 * time.Minute},
		"task_ended_by before task_started_by": {func(task *wire.Submission) {
			task.Wherry.TaskStartedBy, task.Wherry.TaskEndedBy = now.Add(time.Minute).Unix(), now.Add(30*time.Second).Unix()
		}, "task_ended_by is before task_started_by", 0},
		"past the run cap": {func(task *wire.Submission) {
			task.Wherry.TaskStartedBy = now.Add(time.Minute).Unix()
			task.Wherry.TaskEndedBy = task.Wherry.TaskStartedBy + runCap + 1
		}, "task_ended_by is past task_started_by plus the 6h run cap", 0},
		"past the start_by_limit": {func(task *wire.Submission) {
			task.Wherry.TaskStartedBy = now.Add(time.Duration(operator.DefaultStartByLimitMS)*time.Millisecond + time.Hour).Unix()
			task.Wherry.TaskEndedBy = task.Wherry.TaskStartedBy
		}, "task_started_by is past the pool's start_by_limit", 0},
		"no envelope": {nil, "the task carries no envelope", 0},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			invalid := testSubmission("run-a")
			if test.edit != nil {
				test.edit(invalid)
				f.seal(t, invalid)
			}
			record := f.produce(t, f.tasksTopic(), testRun("run-a"), must(t)(wire.SignRecord(invalid, f.host)))
			record.Timestamp = record.Timestamp.Add(test.appended)
			f.rejected(t, record, test.reason)
			if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckAccept {
				t.Fatalf("the valid task ended %v; logs:\n%s", status, f.logs.String())
			}
		})
	}
}

// A host submits and goes offline; a pool member that is not the key custodian claims the task, the custodian serves
// its open, and the run completes with the host's credential delivered and redacted from the feed.
func TestHostOfflineAndANonCustodianRunsTheTask(t *testing.T) {
	stream := []string{`{"type":"assistant","message":"token ` + testToken + `"}`, completedWith("used " + testToken)}
	f := newFixture(t, stream)
	f.quiet()
	// The custodian keeps its place in the custodian group and serves opens, and claims nothing.
	custody, stop := context.WithCancel(f.ctx)
	var workers sync.WaitGroup
	t.Cleanup(func() {
		stop()
		done := make(chan struct{})
		go func() {
			workers.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(20 * time.Second):
			t.Fatal("custody workers did not return")
		}
	})
	workers.Go(func() { f.receiver.keepCustody(custody, custody) })
	workers.Go(func() { f.receiver.every(custody, openInterval, f.receiver.serveOpens) })
	for {
		f.receiver.custody.mu.Lock()
		assigned := f.receiver.custody.assigned && f.receiver.custody.producer != nil
		f.receiver.custody.mu.Unlock()
		if assigned {
			break
		}
		if f.ctx.Err() != nil {
			t.Fatal("the custodian was never assigned")
		}
		time.Sleep(20 * time.Millisecond)
	}
	other, otherLogs := f.newReceiver(t, testConfig("receiver-b"))
	runUntilStopped(t, custody, other)
	for !strings.Contains(otherLogs.String(), "configured:") {
		time.Sleep(20 * time.Millisecond)
	}
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WHERRY_TEST_TOKEN", testToken)
	stateDir := filepath.Join(t.TempDir(), "host")
	connect := func() *client.Host {
		kafka, err := kgo.NewClient(append(f.service.Operator.Opts(), kgo.FetchIsolationLevel(kgo.ReadCommitted()))...)
		if err != nil {
			t.Fatal(err)
		}
		host, err := client.NewHost(kafka, f.host, operator.TenantPrincipal(testTenant, testHost), stateDir, map[string]credential.Source{credentialName: {Env: "WHERRY_TEST_TOKEN"}}, nil, log.New(io.Discard, "", 0))
		if err != nil {
			t.Fatal(err)
		}
		return host
	}
	host := connect()
	var submitted client.SubmitResult
	var err error
	for {
		if submitted, err = host.Submit(f.ctx, client.SubmitInput{Agent: "claude", Prompt: "hello", StartBy: time.Now().Add(time.Minute), EndBy: time.Now().Add(time.Minute)}); err == nil {
			break
		}
		if f.ctx.Err() != nil {
			t.Fatalf("submit: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	host.Close()
	back := connect()
	defer back.Close()
	awaited, err := back.Await(f.ctx, submitted.RunID, 30*time.Second)
	if err != nil || awaited.Status.State != wire.TaskStateCompleted {
		t.Fatalf("await %+v, %v; logs:\n%s", awaited, err, otherLogs.String())
	}
	if text := awaited.Task.Artifacts[0].Parts[0].Text; !strings.HasPrefix(text, "used [redacted:") || strings.Contains(text, testToken) {
		t.Fatalf("result %q, want the credential redacted", text)
	}
	if dispatches, credential, _ := f.dispatched(t); dispatches != 1 || string(credential) != testToken {
		t.Fatalf("dispatches %d with %q, want the host's credential once", dispatches, credential)
	}
	if !strings.Contains(otherLogs.String(), "run "+submitted.RunID+": attempt 1 in profile nomad") || strings.Contains(f.logs.String(), "run "+submitted.RunID+": attempt") {
		t.Fatalf("the run did not run on the non-custodian; custodian logs:\n%s\nother logs:\n%s", f.logs.String(), otherLogs.String())
	}
}

// Adoption is evidenced by a job, never by claim history: a redelivery after another member released the run, with no
// job, is a first start, which commits no adopting status and dispatches.
func TestRedeliveryAfterAReleaseWithNoJobIsAFirstStart(t *testing.T) {
	f := newFixture(t, resultStream)
	keyB := f.addPeer(t, "receiver-b", testPool)
	record := f.submit(t, f.host, nil)
	f.statusAs(t, record, "receiver-b", keyB, "claimed", 1)
	f.statusAs(t, record, "receiver-b", keyB, "released", 1)
	if status := f.execute(t, record, 2); status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	if kinds := f.kinds(t); slices.Contains(kinds, "adopting") {
		t.Fatalf("statuses %v, want no adoption", kinds)
	}
	// The second Lookup is the one that finds the dispatched job started.
	if dispatches, lookups, _, _ := f.plugin.counts(); dispatches != 1 || lookups != 2 {
		t.Fatalf("dispatches %d, lookups %d; want the job looked up, then dispatched and looked up once more", dispatches, lookups)
	}
}

// An answer whose commit is unconfirmed leaves the custodian's producer in its transaction; the next pass aborts it and
// answers the request.
func TestNextPassAnswersAfterAnUnconfirmedAnswerCommit(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	keyB := f.addPeer(t, "receiver-b", testPool)
	record := f.submit(t, f.host, nil)
	f.statusAs(t, record, "receiver-b", keyB, "claimed", 1)
	digest := f.openAs(t, record, "receiver-b", keyB, 1)
	previous := commitTransaction
	t.Cleanup(func() { commitTransaction = previous })
	commitTransaction = func(context.Context, *kgo.Client) error { return errors.New("commit outcome unknown") }
	if err := f.receiver.serveOpens(f.ctx); err == nil {
		t.Fatal("the pass with an unconfirmed commit returned no error")
	}
	commitTransaction = previous
	if answer := f.answered(t, digest); answer != "wrap" {
		t.Fatalf("request answered %q on the next pass, want a wrap; logs:\n%s", answer, f.logs.String())
	}
}

// twoHashes submits two tasks under one run, different in their message ID.
func (f fixture) twoHashes(t *testing.T) (*kgo.Record, *kgo.Record) {
	t.Helper()
	return f.submit(t, f.host, nil), f.submit(t, f.host, func(task *wire.Submission) { task.Message.MessageID = "msg-b" })
}

// Only the run's first task opens: of two hashes claimed under one run, by different receivers or by one receiver's
// different attempts, or the second claimed after the first opened, the second is refused at open and its redelivery
// rejected.
func TestOnlyTheRunsFirstTaskOpens(t *testing.T) {
	for _, test := range []struct {
		name          string
		second        string
		attempt       int64
		openFirstOnce bool
	}{
		{"different receivers", "receiver-c", 1, false},
		{"one receiver's different attempts", "receiver-b", 2, false},
		{"second claimed after the first opened", "receiver-c", 1, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.quiet()
			keys := map[string]ed25519.PrivateKey{"receiver-b": f.addPeer(t, "receiver-b", testPool), "receiver-c": f.addPeer(t, "receiver-c", testPool)}
			first, second := f.twoHashes(t)
			f.statusAs(t, first, "receiver-b", keys["receiver-b"], "claimed", 1)
			if test.openFirstOnce {
				if answer := f.answered(t, f.openAs(t, first, "receiver-b", keys["receiver-b"], 1)); answer != "wrap" {
					t.Fatalf("the first hash answered %q, want served", answer)
				}
			}
			f.statusAs(t, second, test.second, keys[test.second], "claimed", test.attempt)
			if answer := f.answered(t, f.openAs(t, second, test.second, keys[test.second], test.attempt)); answer != wire.RefusedNotClaimant {
				t.Fatalf("the second hash answered %q, want %q", answer, wire.RefusedNotClaimant)
			}
			if !test.openFirstOnce {
				if answer := f.answered(t, f.openAs(t, first, "receiver-b", keys["receiver-b"], 1)); answer != "wrap" {
					t.Fatalf("the first hash answered %q, want served", answer)
				}
			}
			f.rejected(t, second, "duplicate submission")
		})
	}
}

// A hash that is the run's first task stays its task after a later rejected status of it.
func TestTheRunsFirstTaskStaysFirstOnceRejected(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	keyB, keyC := f.addPeer(t, "receiver-b", testPool), f.addPeer(t, "receiver-c", testPool)
	first, second := f.twoHashes(t)
	f.statusAs(t, first, "receiver-b", keyB, "claimed", 1)
	f.publishStatus(t, first, testReceiver, f.receiver.signing, "rejected", wire.TaskStateRejected, nil)
	f.statusAs(t, second, "receiver-c", keyC, "claimed", 1)
	if answer := f.answered(t, f.openAs(t, second, "receiver-c", keyC, 1)); answer != wire.RefusedNotClaimant {
		t.Fatalf("the second hash answered %q, want %q", answer, wire.RefusedNotClaimant)
	}
}

// Pins the accepted residual: the fold holds live members' statuses alone, so revoking the member whose claim was the
// run's first task makes the next hash its task, and that hash opens.
func TestRevokingTheFirstClaimantMakesTheNextHashTheRunsTask(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	keyB, keyC := f.addPeer(t, "receiver-b", testPool), f.addPeer(t, "receiver-c", testPool)
	first, second := f.twoHashes(t)
	f.statusAs(t, first, "receiver-b", keyB, "claimed", 1)
	f.statusAs(t, second, "receiver-c", keyC, "claimed", 1)
	if answer := f.answered(t, f.openAs(t, second, "receiver-c", keyC, 1)); answer != wire.RefusedNotClaimant {
		t.Fatalf("the second hash answered %q before the revocation, want %q", answer, wire.RefusedNotClaimant)
	}
	f.revoke(t, "receiver-b")
	if answer := f.answered(t, f.openAs(t, second, "receiver-c", keyC, 1)); answer != "wrap" {
		t.Fatalf("the second hash answered %q after the revocation, want served", answer)
	}
}

// The custodian serves a claim only when the record at its task offset is the run's task: present, under the run's key,
// an authorised host's for the run under its derived ID, of the claim's hash, its bounds holding at its append time.
func TestClaimCheckRefusesAClaimWhoseOffsetHoldsNoTaskOfTheRun(t *testing.T) {
	startByLimit := time.Duration(operator.DefaultStartByLimitMS) * time.Millisecond
	for name, test := range map[string]struct {
		place  func(t *testing.T, f fixture) (run, hash string, offset int64)
		answer string
	}{
		"control": {func(t *testing.T, f fixture) (string, string, int64) {
			record := f.submit(t, f.host, nil)
			return string(record.Key), wire.TaskHash(record.Value), record.Offset
		}, "wrap"},
		"no record at the offset": {func(t *testing.T, f fixture) (string, string, int64) {
			record := f.submit(t, f.host, nil)
			return string(record.Key), wire.TaskHash(record.Value), record.Offset + 5
		}, wire.RefusedNotClaimant},
		"a record under another key": {func(t *testing.T, f fixture) (string, string, int64) {
			task := testSubmission("run-a")
			f.seal(t, task)
			record := f.produce(t, f.tasksTopic(), testRun("run-b"), must(t)(wire.SignRecord(task, f.host)))
			return testRun("run-a"), wire.TaskHash(record.Value), record.Offset
		}, wire.RefusedNotClaimant},
		"a binding run unequal to the key": {func(t *testing.T, f fixture) (string, string, int64) {
			task := testSubmission("run-b")
			f.seal(t, task)
			record := f.produce(t, f.tasksTopic(), testRun("run-a"), must(t)(wire.SignRecord(task, f.host)))
			return testRun("run-a"), wire.TaskHash(record.Value), record.Offset
		}, wire.RefusedNotClaimant},
		"another hash": {func(t *testing.T, f fixture) (string, string, int64) {
			record, other := f.twoHashes(t)
			return string(record.Key), wire.TaskHash(other.Value), record.Offset
		}, wire.RefusedNotClaimant},
		"an unauthorised record": {func(t *testing.T, f fixture) (string, string, int64) {
			_, unknown, _ := ed25519.GenerateKey(nil)
			record := f.submit(t, unknown, nil)
			return string(record.Key), wire.TaskHash(record.Value), record.Offset
		}, wire.RefusedNotClaimant},
		"a mismatching derived ID": {func(t *testing.T, f fixture) (string, string, int64) {
			record := f.submit(t, f.host, func(task *wire.Submission) {
				run := testRun("run-other")
				task.Wherry.Run, task.Message.TaskID, task.Message.ContextID = run, run, run
			})
			return string(record.Key), wire.TaskHash(record.Value), record.Offset
		}, wire.RefusedNotClaimant},
		"bounds failing at its append time": {func(t *testing.T, f fixture) (string, string, int64) {
			record := f.submit(t, f.host, func(task *wire.Submission) {
				task.Wherry.TaskStartedBy = time.Now().Add(startByLimit + time.Hour).Unix()
				task.Wherry.TaskEndedBy = task.Wherry.TaskStartedBy
			})
			return string(record.Key), wire.TaskHash(record.Value), record.Offset
		}, wire.RefusedNotClaimant},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.quiet()
			keyB := f.addPeer(t, "receiver-b", testPool)
			run, hash, offset := test.place(t, f)
			f.claimOf(t, run, hash, offset, "receiver-b", keyB, 1)
			if answer := f.answered(t, f.requestOf(t, run, hash, "receiver-b", keyB, 1)); answer != test.answer {
				t.Fatalf("answer %q, want %q; logs:\n%s", answer, test.answer, f.logs.String())
			}
		})
	}
}

// A request from a receiver that is not a live pool member is refused before the task topic is read.
func TestANonMembersRequestReadsNoTask(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	keyB := f.addPeer(t, "receiver-b", testPool)
	record := f.submit(t, f.host, nil)
	f.statusAs(t, record, "receiver-b", keyB, "claimed", 1)
	digest := f.openAs(t, record, "receiver-b", keyB, 1)
	events, err := f.receiver.readEvents(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	f.revoke(t, "receiver-b")
	fetches, _ := f.taskFetches(t, 0)
	request := slices.IndexFunc(events.requests, func(request openRequest) bool { return request.digest == digest })
	check := &claimCheck{r: f.receiver, events: events, now: time.Now(), terms: testTerms()}
	var refusal wire.OpenRefusal
	if answer := check.decide(f.ctx, events.requests[request]); len(answer) != 1 || wire.ParseStrict(answer[0].Value, f.keys(), time.Now(), &refusal) != nil ||
		refusal.Reason != wire.RefusedNotClaimant || fetches.Load() != 0 {
		t.Fatalf("refusal %+v after %d task fetches, want %q and none", refusal, fetches.Load(), wire.RefusedNotClaimant)
	}
}

// A claim forged with the offset of a rejected task is refused terminal.
func TestAClaimOfARejectedTaskIsRefusedTerminal(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	keyB := f.addPeer(t, "receiver-b", testPool)
	record := f.submit(t, f.host, nil)
	f.publishStatus(t, record, testReceiver, f.receiver.signing, "rejected", wire.TaskStateRejected, nil)
	f.statusAs(t, record, "receiver-b", keyB, "claimed", 1)
	if answer := f.answered(t, f.openAs(t, record, "receiver-b", keyB, 1)); answer != wire.RefusedTerminal {
		t.Fatalf("answer %q, want %q", answer, wire.RefusedTerminal)
	}
}

// Documents the accepted residual at ADR-0033's threat boundary: a live pool member that forges a claim naming a real
// task's offset and hash, never delivered to it, is served; an ended member or another pool's is refused.
func TestAForgedClaimIsServedToALivePoolMemberAlone(t *testing.T) {
	for _, test := range []struct {
		name   string
		answer string
	}{{"live member", "wrap"}, {"ended member", wire.RefusedNotClaimant}, {"another pool's member", wire.RefusedNotClaimant}} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.quiet()
			pool := testPool
			if test.name == "another pool's member" {
				if err := f.service.PoolApply(f.ctx, "pool-b", testTerms(), false); err != nil {
					t.Fatal(err)
				}
				pool = "pool-b"
			}
			keyB := f.addPeer(t, "receiver-b", pool)
			record := f.submit(t, f.host, nil)
			f.claimOf(t, string(record.Key), wire.TaskHash(record.Value), record.Offset, "receiver-b", keyB, 1)
			digest := f.openAs(t, record, "receiver-b", keyB, 1)
			if test.name == "ended member" {
				f.revoke(t, "receiver-b")
			}
			if answer := f.answered(t, digest); answer != test.answer {
				t.Fatalf("answer %q, want %q", answer, test.answer)
			}
		})
	}
}

// A former key holder re-keys a task from its live claim's request though an older claim's request, which fails rule
// 3, comes first in the same pass.
func TestAnOlderClaimsRequestDoesNotBlockTheRekey(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	a := f.peer(t, "receiver-x")
	keyA := holding(t, a)
	keyB := f.addPeer(t, "receiver-b", testPool)
	if err := a.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	record := f.submit(t, f.host, sealedTo(t, keyA))
	f.statusAs(t, record, "receiver-b", keyB, "claimed", 1)
	f.openAs(t, record, "receiver-b", keyB, 1)
	f.statusAs(t, record, "receiver-b", keyB, "claimed", 2)
	f.openAs(t, record, "receiver-b", keyB, 2)
	if err := a.serveOpens(f.ctx); err != nil {
		t.Fatal(err)
	}
	if rekeys := f.rekeys(t)[string(record.Key)]; len(rekeys) != 1 || rekeys[0].Holder != "receiver-x" {
		t.Fatalf("re-key wraps %+v, want receiver-x's", rekeys)
	}
}

// The claim check reads the run's task from the claim's offset with a bounded number of fetches, however long the log
// that follows it: a claimed record early in the task topic, followed by many records, is served in one fetch.
func TestTheClaimCheckReadsABoundedAmountOfALongTrailingLog(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	keyB := f.addPeer(t, "receiver-b", testPool)
	record := f.submit(t, f.host, nil)
	const trailing = 20
	for range trailing {
		f.produce(t, f.tasksTopic(), testRun("run-filler"), []byte("filler"))
	}
	// Every fetch of the task topic returns one record batch, and each record above was produced alone, so reading the
	// log from the claim's offset to its end takes a fetch per trailing record.
	topic := f.tasksTopic()
	details, err := kadm.NewClient(f.service.Operator).ListTopics(f.ctx, topic)
	if err != nil || details[topic].Err != nil {
		t.Fatalf("list %s: %v %v", topic, err, details[topic].Err)
	}
	f.cluster.ControlKey(kmsg.Fetch.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		fetch := request.(*kmsg.FetchRequest)
		for i, fetched := range fetch.Topics {
			if fetched.Topic != topic && fetched.TopicID != details[topic].ID {
				continue
			}
			fetch.MaxBytes = 1
			for j := range fetched.Partitions {
				fetch.Topics[i].Partitions[j].PartitionMaxBytes = 1
			}
		}
		return nil, nil, false
	})
	f.statusAs(t, record, "receiver-b", keyB, "claimed", 1)
	fetches, below := f.taskFetches(t, record.Offset)
	if answer := f.answered(t, f.openAs(t, record, "receiver-b", keyB, 1)); answer != "wrap" || fetches.Load() != 1 || below.Load() != 0 {
		t.Fatalf("answer %q after %d task fetches, %d from below the claim's offset; want served in one fetch of a %d-record trailing log",
			answer, fetches.Load(), below.Load(), trailing)
	}
}

// The run's task is the hash of its first status other than a rejection, whatever its kind: a run whose first status
// is a non-claimed status of one hash refuses a later hash's claim at open and rejects its delivery as a duplicate.
func TestTheRunsFirstNonRejectedStatusNamesItsTask(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	keyB, keyC := f.addPeer(t, "receiver-b", testPool), f.addPeer(t, "receiver-c", testPool)
	first, second := f.twoHashes(t)
	f.statusAs(t, first, "receiver-b", keyB, "released", 1)
	f.statusAs(t, second, "receiver-c", keyC, "claimed", 1)
	if answer := f.answered(t, f.openAs(t, second, "receiver-c", keyC, 1)); answer != wire.RefusedNotClaimant {
		t.Fatalf("the second hash answered %q, want %q; logs:\n%s", answer, wire.RefusedNotClaimant, f.logs.String())
	}
	f.rejected(t, second, "duplicate submission")
}

func TestWaitingRunReleasesWhenEntitlementEnds(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	halted := make(chan error, 4)
	f.receiver.halt = func(err error) { halted <- err }
	record := f.submit(t, f.host, nil)
	done := f.executing(t, f.ctx, record, 1)
	f.request(t, 1)
	_, _, cachedHosts := f.receiver.snapshot()
	if err := f.service.TenantPoolRemove(f.ctx, testTenant, testPool); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.receiver.mu.Lock()
	f.receiver.hosts = cachedHosts
	f.receiver.mu.Unlock()
	f.receiver.serveOpens(f.ctx)
	if ack := <-done; ack != kgo.AckRelease {
		t.Fatalf("ack=%v; logs=%s", ack, f.logs.String())
	}
	if n, _, _ := f.dispatched(t); n != 0 {
		t.Fatalf("dispatched %d runs", n)
	}
	before := len(f.records(t, operator.PoolEventsTopic(testPool)))
	if admitted, ack := f.receiver.admit(f.ctx, record, f.receiver.profiles[0]); admitted != nil || ack != kgo.AckReject {
		t.Fatalf("redelivery admitted=%v ack=%v", admitted != nil, ack)
	}
	if len(f.records(t, operator.PoolEventsTopic(testPool))) != before {
		t.Fatal("redelivery wrote records")
	}
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatalf("receiver stopped: %v", err)
	}
	select {
	case err := <-halted:
		t.Fatalf("receiver exited on refusal: %v", err)
	default:
	}
}
