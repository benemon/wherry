package receiver

import (
	"context"
	"crypto/ecdsa"
	"encoding/base64"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
	"weak"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
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

func (f fixture) dispatched(t *testing.T) (int, []byte, int64) {
	t.Helper()
	f.adapter.mu.Lock()
	defer f.adapter.mu.Unlock()
	return f.adapter.dispatches, f.adapter.lastRun.Credential, f.adapter.lastRun.Attempt
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
		Turn: runB.Turn, Attempt: runB.Attempt, Credentials: runB.Credentials, ExpiryMS: runB.ExpiryMS,
	}, runA.Key.Key.(*ecdsa.PublicKey), map[string][]byte{credentialName: []byte(testToken)})
	if err != nil {
		t.Fatal(err)
	}
	f.produce(t, admin.HostTopic(testTenant, testHost), "run-b", sealed)
	<-first
	if status := <-second; status != kgo.AckAccept || f.lastTask(t).Status.State != wire.TaskStateAuthRequired {
		t.Fatalf("run-b ended %v with %s, want AUTH_REQUIRED", status, f.lastTask(t).Status.State)
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
	if !strings.Contains(f.logs.String(), "run run-a: a seal for turn 1 attempt 2 does not open") {
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
		Turn: request.Turn, Attempt: request.Attempt, Credentials: request.Credentials, ExpiryMS: request.ExpiryMS,
	}, request.Key.Key.(*ecdsa.PublicKey), map[string][]byte{credentialName: []byte(testToken)})
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(string(sealed), ".")
	tag, _ := base64.RawURLEncoding.DecodeString(parts[4])
	tag[0] ^= 1
	parts[4] = base64.RawURLEncoding.EncodeToString(tag)
	f.produce(t, admin.HostTopic(testTenant, testHost), "run-b", []byte(strings.Join(parts, ".")))
	if status := <-done; status != kgo.AckAccept || f.lastTask(t).Status.State != wire.TaskStateAuthRequired {
		t.Fatalf("a tampered seal: ended %v, want AUTH_REQUIRED", status)
	}
	if logs := f.logs.String(); strings.Contains(logs, testToken) || !strings.Contains(logs, "run run-b: a seal for turn 1 attempt 1 does not open") {
		t.Fatalf("unexpected logs:\n%s", logs)
	}
}

const redacted = "[redacted:" + credentialName + "]"

func TestDeliveredCredentialIsRedacted(t *testing.T) {
	f := newFixture(t, []string{
		`{"type":"assistant","message":"token ` + testToken + `"}`,
		`{"type":"result","result":"used ` + testToken + `","is_error":false,"note":"` + testToken + `"}`,
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
	t.Run("metadata", func(t *testing.T) {
		if metadata := string(result.Metadata); strings.Contains(metadata, testToken) || !strings.Contains(metadata, redacted) {
			t.Fatalf("result metadata %s, want the credential redacted", metadata)
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
	f := newFixture(t, []string{`{"type":"assistant","message":"checked"}`, `{"type":"result","result":"done","is_error":false}`})
	answerSeals(t, f.ctx, f.service.Admin, f.keys(), testTenant, testHost, credentialName, "false")
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckAccept || f.lastTask(t).Status.State != wire.TaskStateCompleted {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	if !strings.Contains(f.logs.String(), "run run-a: credential "+credentialName+" too short to redact") {
		t.Fatalf("logs:\n%s", f.logs.String())
	}
	chunks := f.chunks(t)
	if last := chunks[len(chunks)-1].ArtifactUpdate.Artifact.Parts[0].Text; !strings.Contains(last, `"is_error":false`) {
		t.Fatalf("transcript line %q, want the short value left in place", last)
	}
}

// Redaction inside result metadata keeps it valid JSON, so signing it never fails.
func TestRedactedMetadataStaysValid(t *testing.T) {
	f := newFixture(t, resultStream)
	run := testActiveRun(t, f, "run-m", 1)
	run.deliver(map[string][]byte{credentialName: []byte("1234567890")})
	agent, ok := resultLine([]byte(`{"type":"result","result":"done","is_error":false,"count":1234567890,"note":"id 1234567890"}`))
	if !ok {
		t.Fatal("fixture is not a result line")
	}
	var result wire.Result
	if err := wire.ParseRecord(run.result(taskStatus(wire.TaskStateCompleted, ""), agent.Result, agent.metadata).Value, f.keys(), &result); err != nil {
		t.Fatal(err)
	}
	if metadata := string(result.Task.Artifacts[0].Metadata); !strings.Contains(metadata, `"count":1234567890`) || !strings.Contains(metadata, `"note":"id `+redacted+`"`) {
		t.Fatalf("metadata %s, want the string value redacted and the number kept", metadata)
	}
}

func TestReplacementNeverCarriesTheValue(t *testing.T) {
	f := newFixture(t, resultStream)
	run := testActiveRun(t, f, "run-n", 1)
	run.deliver(map[string][]byte{credentialName: []byte("oauth-token")})
	if got := string(run.redact([]byte("key oauth-token"))); got != "key [redacted]" {
		t.Fatalf("redacted %q, want the bare marker since the name holds the value", got)
	}
}

// A value that holds another credential's value is replaced whole, whatever order the values arrive in.
func TestLongerValueIsRedactedFirst(t *testing.T) {
	f := newFixture(t, resultStream)
	for range 20 {
		run := testActiveRun(t, f, "run-l", 1)
		run.deliver(map[string][]byte{"outer": []byte("prefix-inner-secret"), "inner": []byte("inner-secret")})
		if got := string(run.redact([]byte("x prefix-inner-secret"))); got != "x [redacted:outer]" {
			t.Fatalf("redacted %q, want the longer value replaced whole", got)
		}
	}
}
