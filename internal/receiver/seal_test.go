package receiver

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/base64"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
	"weak"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
	"github.com/twmb/franz-go/pkg/kgo"
)

// executing runs execute for record in the background under ctx and returns where its acknowledgement arrives.
func (f fixture) executing(t *testing.T, ctx context.Context, record *kgo.Record, attempt int64) <-chan kgo.AckStatus {
	t.Helper()
	admitted := f.admitted(t, record)
	done := make(chan kgo.AckStatus, 1)
	go func() { done <- f.receiver.execute(ctx, f.service.Admin, record, admitted, attempt) }()
	return done
}

// request waits for the receiver's nth seal request on the feed.
func (f fixture) request(t *testing.T, n int) wire.SealRequest {
	t.Helper()
	for {
		if requests := f.sealRequests(t); len(requests) >= n {
			return requests[n-1]
		}
		if f.ctx.Err() != nil {
			t.Fatalf("no seal request %d; logs:\n%s", n, f.logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
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

func TestSealRequestAsksForTheAttemptsCredential(t *testing.T) {
	f := newFixture(t, resultStream)
	f.answer(t, credentialName)
	record := f.submit(t, f.host, nil)
	before := time.Now()
	if status := f.execute(t, record, 1); status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	requests := f.sealRequests(t)
	if len(requests) != 1 {
		t.Fatalf("seal requests %d, want 1", len(requests))
	}
	request := requests[0]
	key, ok := request.Key.Key.(*ecdsa.PublicKey)
	if request.ReceiverBinding != f.receiver.binding(f.parse(t, record), 1) || !slices.Equal(request.Credentials, []string{credentialName}) ||
		!ok || request.KeyID != signed.SealKeyID(key) {
		t.Fatalf("seal request %#v", request)
	}
	if expiry := time.UnixMilli(request.ExpiryMS); expiry.Before(before.Add(sealWait)) || expiry.After(time.Now().Add(sealWait)) {
		t.Fatalf("seal request expires at %v, want the seal wait from the request", expiry)
	}
	if dispatches, credential, _ := f.dispatched(t); dispatches != 1 || string(credential) != testToken {
		t.Fatalf("dispatches %d with credential %q, want the sealed value once", dispatches, credential)
	}
	// The request is its own transaction on the run's key, after the claimed status.
	feed := f.records(t, admin.TenantTopic(testTenant))
	if string(feed[2].Key) != "run-a" || oneTransaction(feed[1:3]) || oneTransaction(feed[2:4]) {
		t.Fatal("the seal request is not a transaction of its own after the claimed status")
	}
}

// A seal bound to run B but encrypted to the key run A's request named does not open for run B.
func TestSealToAnotherRunsKeyDoesNotOpen(t *testing.T) {
	f := newFixture(t, resultStream)
	first := f.executing(t, f.ctx, f.turn(t, "run-a", 1), 1)
	runA := f.request(t, 1)
	second := f.executing(t, f.ctx, f.turn(t, "run-b", 1), 1)
	runB := f.request(t, 2)
	sealed, err := signed.Seal(signed.SealBinding{
		Tenant: runB.Tenant, Pool: runB.Pool, Receiver: runB.Receiver, KeyID: runB.KeyID, Run: runB.Run,
		Attempt: runB.Attempt, Credentials: runB.Credentials, ExpiryMS: runB.ExpiryMS,
	}, runA.Key.Key.(*ecdsa.PublicKey), signed.SealValues{Values: map[string][]byte{credentialName: []byte(testToken)}, RedactionKey: testRedactionKey})
	if err != nil {
		t.Fatal(err)
	}
	f.produce(t, admin.HostTopic(testTenant, testHost), "run-b", sealed)
	<-first
	if status := <-second; status != kgo.AckReject || f.lastTask(t).Status.State != wire.TaskStateFailed {
		t.Fatalf("run-b ended %v with %s, want FAILED", status, f.lastTask(t).Status.State)
	}
	if dispatches, _, _ := f.dispatched(t); dispatches != 0 {
		t.Fatalf("dispatched %d jobs with a seal to another run's key", dispatches)
	}
}

// A redelivered turn asks again under a new key; a seal answering the first attempt's request does not open.
func TestALaterAttemptRequestsAgain(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.submit(t, f.host, nil)
	crashed, crash := context.WithCancel(f.ctx)
	done := f.executing(t, crashed, record, 1)
	first := f.request(t, 1)
	crash()
	if status := <-done; status != kgo.AckRelease {
		t.Fatalf("attempt 1 ended %v, want release", status)
	}
	done = f.executing(t, f.ctx, record, 2)
	second := f.request(t, 2)
	if second.Attempt != 2 || second.KeyID == first.KeyID {
		t.Fatalf("attempt 2 request %#v, want attempt 2 under a new key", second)
	}
	f.sealFor(t, first, map[string][]byte{credentialName: []byte("first-attempt-token")})
	f.sealFor(t, second, map[string][]byte{credentialName: []byte(testToken)})
	if status := <-done; status != kgo.AckAccept {
		t.Fatalf("attempt 2 ended %v; logs:\n%s", status, f.logs.String())
	}
	if dispatches, credential, attempt := f.dispatched(t); dispatches != 1 || string(credential) != testToken || attempt != 2 {
		t.Fatalf("dispatches %d with %q at attempt %d, want attempt 2's seal alone", dispatches, credential, attempt)
	}
	if !strings.Contains(f.logs.String(), "run run-a: a seal for attempt 2 does not open") {
		t.Fatalf("the first attempt's seal was not refused; logs:\n%s", f.logs.String())
	}
}

func TestSealKeyIsGoneAfterTheAttempt(t *testing.T) {
	f := newFixture(t, resultStream)
	f.answer(t, credentialName)
	var key weak.Pointer[ecdsa.PrivateKey]
	previous := newSealKey
	newSealKey = func() (*ecdsa.PrivateKey, error) {
		generated, err := previous()
		key = weak.Make(generated)
		return generated, err
	}
	t.Cleanup(func() { newSealKey = previous })
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	if dispatches, credential, _ := f.dispatched(t); dispatches != 1 || string(credential) != testToken {
		t.Fatal("the run did not open its seal")
	}
	for range 10 {
		runtime.GC()
		if key.Value() == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the attempt's seal key is still reachable after the attempt ended")
}

func TestCancelDuringTheSealWaitEndsTheTurn(t *testing.T) {
	f := newFixture(t, resultStream)
	done := f.executing(t, f.ctx, f.submit(t, f.host, nil), 1)
	f.request(t, 1)
	f.publishCancel(t, "run-a", 1, f.host, nil)
	if status := <-done; status != kgo.AckAccept || f.lastTask(t).Status.State != wire.TaskStateCanceled {
		t.Fatalf("ended %v with %s, want CANCELED", status, f.lastTask(t).Status.State)
	}
	if dispatches, _, _ := f.dispatched(t); dispatches != 0 {
		t.Fatal("dispatched a cancelled turn")
	}
}

func TestSecretNeverLogged(t *testing.T) {
	f := newFixture(t, resultStream)
	done := f.executing(t, f.ctx, f.turn(t, "run-a", 1), 1)
	f.sealFor(t, f.request(t, 1), map[string][]byte{credentialName: []byte(testToken)})
	if status := <-done; status != kgo.AckAccept || f.lastTask(t).Status.State != wire.TaskStateCompleted {
		t.Fatalf("run-a ended %v, want completed", status)
	}
	done = f.executing(t, f.ctx, f.turn(t, "run-b", 1), 1)
	request := f.request(t, 2)
	sealed, err := signed.Seal(signed.SealBinding{
		Tenant: request.Tenant, Pool: request.Pool, Receiver: request.Receiver, KeyID: request.KeyID, Run: request.Run,
		Attempt: request.Attempt, Credentials: request.Credentials, ExpiryMS: request.ExpiryMS,
	}, request.Key.Key.(*ecdsa.PublicKey), signed.SealValues{Values: map[string][]byte{credentialName: []byte(testToken)}, RedactionKey: testRedactionKey})
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(string(sealed), ".")
	tag, _ := base64.RawURLEncoding.DecodeString(parts[4])
	tag[0] ^= 1
	parts[4] = base64.RawURLEncoding.EncodeToString(tag)
	f.produce(t, admin.HostTopic(testTenant, testHost), "run-b", []byte(strings.Join(parts, ".")))
	if status := <-done; status != kgo.AckReject || f.lastTask(t).Status.State != wire.TaskStateFailed {
		t.Fatalf("a tampered seal: ended %v, want FAILED", status)
	}
	if logs := f.logs.String(); strings.Contains(logs, testToken) || !strings.Contains(logs, "run run-b: a seal for attempt 1 does not open") {
		t.Fatalf("unexpected logs:\n%s", logs)
	}
}

var redacted = signed.RedactionMarker(testRedactionKey, credentialName)

func TestDeliveredCredentialIsRedacted(t *testing.T) {
	f := newFixture(t, []string{
		`{"type":"assistant","message":"token ` + testToken + `"}`,
		completedWith("used " + testToken),
	})
	f.answer(t, credentialName)
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

// A seal and a cancel seen in the same poll: the cancel wins and nothing is dispatched.
func TestCancelSeenWithTheSealEndsTheTurn(t *testing.T) {
	f := newFixture(t, resultStream)
	previous := stateCheckInterval
	stateCheckInterval = 10 * time.Second
	t.Cleanup(func() { stateCheckInterval = previous })
	done := f.executing(t, f.ctx, f.submit(t, f.host, nil), 1)
	request := f.request(t, 1)
	// The receiver is inside a 10-second poll; the cancel is held before the seal ends it.
	f.publishCancel(t, "run-a", 1, f.host, nil)
	f.sealFor(t, request, map[string][]byte{credentialName: []byte(testToken)})
	if status := <-done; status != kgo.AckAccept || f.lastTask(t).Status.State != wire.TaskStateCanceled {
		t.Fatalf("ended %v with %s, want CANCELED", status, f.lastTask(t).Status.State)
	}
	if dispatches, _, _ := f.dispatched(t); dispatches != 0 {
		t.Fatal("dispatched a turn cancelled when its seal arrived")
	}
}

// A value shorter than minRedact matches ordinary output, so it is logged and left in place.
func TestShortCredentialIsLoggedNotRedacted(t *testing.T) {
	f := newFixture(t, []string{`{"type":"assistant","message":"checked: false"}`, doneOutcome})
	answerSeals(t, f.ctx, f.service.Admin, f.keys(), testTenant, testHost, credentialName, "false")
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckAccept || f.lastTask(t).Status.State != wire.TaskStateCompleted {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	if !strings.Contains(f.logs.String(), "run run-a: credential "+credentialName+" too short to redact") {
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

// An adopter asks for its own seal and publishes nothing of the job until it opens; the markers it then writes are the
// run's, the claimant's, and name no credential.
func TestAnAdopterPublishesNothingBeforeItsSeal(t *testing.T) {
	f, record := adopting(t)
	done := f.executing(t, f.ctx, record, 2)
	request := f.request(t, 1)
	if request.Attempt != 2 || !slices.Equal(request.Credentials, []string{credentialName}) || len(request.S3Inputs) != 0 {
		t.Fatalf("adopter's seal request %#v", request)
	}
	f.held(t)
	if chunks, kinds := f.chunks(t), f.kinds(t); len(chunks) != 0 || !slices.Equal(kinds, []string{"claimed"}) {
		t.Fatalf("before its seal the adopter published %d chunks and statuses %v", len(chunks), kinds)
	}
	f.sealFor(t, request, map[string][]byte{credentialName: []byte(testToken)})
	if status := <-done; status != kgo.AckAccept || f.lastTask(t).Status.State != wire.TaskStateCompleted {
		t.Fatalf("adopter ended %v; logs:\n%s", status, f.logs.String())
	}
	chunks := f.chunks(t)
	if len(chunks) != 2 || chunks[0].ArtifactUpdate.Artifact.Parts[0].Text != `{"type":"assistant","message":"token `+redacted+`"}` {
		t.Fatalf("adopter's transcript %+v, want both lines with the run's marker", chunks)
	}
	for _, record := range append(f.records(t, admin.TenantTopic(testTenant)), f.records(t, admin.TenantTranscriptTopic(testTenant))...) {
		var request wire.SealRequest
		if wire.ParseRecord(record.Value, f.keys(), &request) == nil {
			continue
		}
		if payload := decodedPayload(record.Value); bytes.Contains(payload, []byte(testToken)) || bytes.Contains(payload, []byte(credentialName)) {
			t.Fatalf("a feed record holds the credential or its name: %s", payload)
		}
	}
	if result := f.lastTask(t).Artifacts[0].Parts[0].Text; result != "used "+redacted {
		t.Fatalf("result %q, want the credential redacted", result)
	}
}

// An adopter whose seal does not come within the request's lifetime stops the job and fails the run.
func TestAnAdopterWithoutASealFails(t *testing.T) {
	f, record := adopting(t)
	if status := f.execute(t, record, 2); status != kgo.AckReject || f.lastTask(t).Status.State != wire.TaskStateFailed {
		t.Fatalf("adopter ended %v; logs:\n%s", status, f.logs.String())
	}
	f.plugin.mu.Lock()
	stops, releases := f.plugin.stops, f.plugin.releases
	f.plugin.mu.Unlock()
	if stops != 1 || releases != 1 || len(f.chunks(t)) != 0 {
		t.Fatalf("stops=%d releases=%d chunks=%d, want the job stopped and released with nothing published", stops, releases, len(f.chunks(t)))
	}
	if kinds := f.kinds(t); !slices.Equal(kinds, []string{"claimed", "terminal"}) {
		t.Fatalf("statuses %v, want claimed then terminal", kinds)
	}
}

// The held transcript is bounded: the oldest lines go, the log records the gap, and the transcript resumes past it.
func TestAnAdopterDropsTheOldestHeldLines(t *testing.T) {
	line := strings.Repeat("x", 1<<20)
	f := newFixture(t, append(slices.Repeat([]string{line}, 6), doneOutcome))
	f.plugin.job = pluginsdk.JobRunning
	done := f.executing(t, f.ctx, f.submit(t, f.host, nil), 2)
	request := f.request(t, 1)
	f.held(t)
	f.sealFor(t, request, map[string][]byte{credentialName: []byte(testToken)})
	if status := <-done; status != kgo.AckAccept {
		t.Fatalf("adopter ended %v; logs:\n%s", status, f.logs.String())
	}
	chunks := f.chunks(t)
	first := int64(3 * (len(line) + 1))
	if len(chunks) != 4 || chunks[0].Wherry.Seq != first || !strings.Contains(f.logs.String(), fmt.Sprintf("dropped 3 transcript lines held before the seal; the transcript resumes at offset %d", first)) {
		t.Fatalf("%d chunks from seq %d; logs:\n%s", len(chunks), chunks[0].Wherry.Seq, f.logs.String())
	}
}
