package receiver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// testActiveRun is a run of a submitted first turn with the receiver's transactional producer.
func testActiveRun(t *testing.T, f fixture, run string, attempt int64) *activeRun {
	t.Helper()
	producer, err := f.receiver.newProducer(run)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)
	record := f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Run, task.Message.TaskID, task.Message.ContextID = run, run, run
	})
	task := f.parse(t, record)
	return &activeRun{receiver: f.receiver, producer: producer, task: task, taskHash: taskHash(record.Value), history: []wire.Message{task.Message}, attempt: attempt, adapter: f.receiver.adapters[0]}
}

func TestRunEndingWithoutResultFails(t *testing.T) {
	f := newFixture(t, noResultStream)
	task := runToTask(t, f)
	if task.Status.State != wire.TaskStateFailed || task.Artifacts[0].Parts[0].Text != "agent ended without a result" {
		t.Fatalf("unexpected task: %#v", task)
	}
	f.adapter.mu.Lock()
	defer f.adapter.mu.Unlock()
	if f.adapter.stops != 1 {
		t.Fatalf("run ended without a result but Stop was called %d times, want 1", f.adapter.stops)
	}
}

func TestStatusesAreDualWritten(t *testing.T) {
	f := newFixture(t, resultStream)
	f.sealOwn(t, credentialName, time.Hour)
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	kinds := func(statuses []wire.RunStatus) []string {
		var kinds []string
		for _, status := range statuses {
			kinds = append(kinds, status.Wherry.Kind+"/"+status.Wherry.EventID)
		}
		return kinds
	}
	events, feed := kinds(f.eventStatuses(t)), kinds(f.feedStatuses(t))
	if len(events) != 3 || !slices.Equal(events, feed) {
		t.Fatalf("pool events %v, feed %v; want claimed, started and terminal on both", events, feed)
	}
}

func TestEchoSharesTheClaimedTransaction(t *testing.T) {
	f := newFixture(t, resultStream)
	f.sealOwn(t, credentialName, time.Hour)
	record := f.submit(t, f.host, nil)
	f.execute(t, record, 1)
	records := f.records(t, admin.TenantTopic(testTenant))
	var echo wire.Echo
	if len(records) < 2 || wire.ParseRecord(records[0].Value, f.keys(), &echo) != nil {
		t.Fatal("the feed does not open with the echo")
	}
	var claimed wire.RunStatus
	if wire.ParseRecord(records[1].Value, f.keys(), &claimed) != nil || claimed.Wherry.Kind != "claimed" {
		t.Fatal("the claimed status does not follow the echo")
	}
	if echo.TaskHash != taskHash(record.Value) || echo.Pool != testPool || echo.Tenant != testTenant || !reflect.DeepEqual(echo.Placement, f.parse(t, record).Wherry.Placement) {
		t.Fatalf("echo %#v", echo)
	}
	// Both records come from one committed transaction: a reader stopping at the claimed status sees the echo.
	if records[0].ProducerID != records[1].ProducerID || records[0].ProducerEpoch != records[1].ProducerEpoch {
		t.Fatal("echo and claimed status come from different producers")
	}
}

func TestNoSealAnywhereInterrupts(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.submit(t, f.host, nil)
	before := time.Now().Add(-time.Second)
	if status := f.execute(t, record, 1); status != kgo.AckAccept {
		t.Fatalf("status %v, want accept", status)
	}
	f.adapter.mu.Lock()
	dispatches, releases := f.adapter.dispatches, f.adapter.releases
	f.adapter.mu.Unlock()
	if dispatches != 0 || releases != 1 {
		t.Fatalf("dispatches=%d releases=%d, want 0 and 1", dispatches, releases)
	}
	statuses := f.feedStatuses(t)
	if len(statuses) != 2 || len(f.eventStatuses(t)) != 2 {
		t.Fatalf("feed statuses=%d, want claimed and interrupted on the feed and the pool's events", len(statuses))
	}
	interrupted := statuses[1]
	got := interrupted.StatusUpdate.Status
	if interrupted.Wherry.Kind != "interrupted" || got.State != wire.TaskStateAuthRequired {
		t.Fatalf("interrupted status %#v, want AUTH_REQUIRED", interrupted)
	}
	if got.Message == nil || got.Message.Role != wire.RoleAgent || len(got.Message.Parts) != 1 || got.Message.Parts[0].Text != "credential "+credentialName+" required" {
		t.Fatalf("status message %#v, want one agent text part with the reason", got.Message)
	}
	if at, err := time.Parse(time.RFC3339, got.Timestamp); err != nil || at.Before(before) || at.After(time.Now()) {
		t.Fatalf("status timestamp %q is not the time of the status: %v", got.Timestamp, err)
	}
	if got, want := string(interrupted.StatusUpdate.Metadata), `{"credential":"`+credentialName+`"}`; got != want {
		t.Fatalf("status metadata %s, want %s", got, want)
	}
	if task := f.lastTask(t); task.Status.State != wire.TaskStateAuthRequired {
		t.Fatalf("result %#v, want AUTH_REQUIRED beside the interrupted status", task)
	}
}

func TestPeerSealIsStartFailure(t *testing.T) {
	f := newFixture(t, resultStream)
	_, peerSealing := f.addPeer(t, "receiver-b", testPool)
	f.seal(t, "receiver-b", peerSealing.PublicKey(), credentialName, testToken, time.Now(), time.Hour)
	record := f.submit(t, f.host, nil)
	if status := f.execute(t, record, 1); status != kgo.AckRelease {
		t.Fatalf("status %v, want release", status)
	}
	if dispatches, _, _, _ := f.adapter.counts(); dispatches != 0 {
		t.Fatalf("dispatched %d jobs without a seal", dispatches)
	}
	var failures []wire.StartFailure
	for _, record := range f.records(t, admin.PoolEventsTopic(testPool)) {
		var failure wire.StartFailure
		if wire.ParseRecord(record.Value, f.keys(), &failure) == nil {
			failures = append(failures, failure)
		}
	}
	if len(failures) != 1 || failures[0].Class != "seal-unavailable" || failures[0].Adapter != "nomad" || failures[0].Agent != "claude" || failures[0].Runtime != "exec" {
		t.Fatalf("start failures %#v", failures)
	}
	for _, status := range f.feedStatuses(t) {
		if status.Wherry.Kind == "interrupted" {
			t.Fatal("a missing seal with a peer holding one interrupted the run")
		}
	}
	// An expired peer seal is not held: the run waits for the tenant.
	f2 := newFixture(t, resultStream)
	_, expiredSealing := f2.addPeer(t, "receiver-b", testPool)
	f2.seal(t, "receiver-b", expiredSealing.PublicKey(), credentialName, testToken, time.Now().Add(-2*time.Hour), time.Hour)
	if status := f2.execute(t, f2.submit(t, f2.host, nil), 1); status != kgo.AckAccept {
		t.Fatalf("status %v with only an expired peer seal, want AUTH_REQUIRED", status)
	}
}

func TestLatestSealByIssuedTimeIsUsed(t *testing.T) {
	f := newFixture(t, resultStream)
	public := f.receiver.sealing.PublicKey()
	now := time.Now()
	// Written newest first: log order must not decide.
	f.seal(t, testReceiver, public, credentialName, "newer", now, time.Hour)
	f.seal(t, testReceiver, public, credentialName, "older", now.Add(-time.Minute), time.Hour)
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	if got := string(f.adapter.lastRun.Credential); got != "newer" {
		t.Fatalf("dispatched credential %q, want the seal with the latest issued time", got)
	}
}

func TestExpiredLatestSealIsNotUsed(t *testing.T) {
	f := newFixture(t, resultStream)
	public := f.receiver.sealing.PublicKey()
	f.seal(t, testReceiver, public, credentialName, "older", time.Now().Add(-time.Minute), time.Hour)
	f.seal(t, testReceiver, public, credentialName, "expired", time.Now().Add(-time.Second), 500*time.Millisecond)
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	if dispatches, _, _, _ := f.adapter.counts(); dispatches != 0 {
		t.Fatal("dispatched with a credential although the latest seal has expired")
	}
}

func TestSealsReadFailureReleases(t *testing.T) {
	f := newFixture(t, resultStream)
	f.sealOwn(t, credentialName, time.Hour)
	record := f.submit(t, f.host, nil)
	admitted := f.admitted(t, record)
	if _, err := kadm.NewClient(f.service.Admin).DeleteTopics(f.ctx, admin.HostTopic(testTenant, testHost)); err != nil {
		t.Fatal(err)
	}
	if status := f.receiver.execute(f.ctx, f.service.Admin, record, admitted, 1); status != kgo.AckRelease {
		t.Fatalf("status %v, want release", status)
	}
	if dispatches, _, _, _ := f.adapter.counts(); dispatches != 0 {
		t.Fatal("dispatched without reading the seals")
	}
}

func TestSecretNeverLogged(t *testing.T) {
	f := newFixture(t, resultStream)
	runToTask(t, f)
	// A newer seal that does not open is refused without echoing anything.
	seal, err := signed.SealCredential(testTenant, testReceiver, testPool, credentialName, time.Now(), time.Now().Add(time.Hour), f.receiver.sealing.PublicKey(), []byte(testToken))
	if err != nil {
		t.Fatal(err)
	}
	seal.Ciphertext[0] ^= 1
	f.produce(t, admin.HostTopic(testTenant, testHost), "", must(t)(signed.EncodeSeal(seal)))
	if _, outcome, err := f.receiver.credential(f.ctx, testTenant, "claude", credentialName); err != nil || outcome != credentialMissing {
		t.Fatalf("tampered seal: outcome %v err %v, want missing", outcome, err)
	}
	if logs := f.logs.String(); strings.Contains(logs, testToken) || !strings.Contains(logs, "latest seal does not open") {
		t.Fatalf("unexpected logs:\n%s", logs)
	}
}

func TestSealOpensOnlyForTheAgentsCredentials(t *testing.T) {
	f := newFixture(t, resultStream)
	f.sealOwn(t, credentialName, time.Hour)
	if _, outcome, _ := f.receiver.credential(f.ctx, testTenant, "codex", credentialName); outcome == credentialOpened {
		t.Fatal("opened a credential outside the agent's terms")
	}
	if value, outcome, _ := f.receiver.credential(f.ctx, testTenant, "claude", credentialName); outcome != credentialOpened || string(value) != testToken {
		t.Fatal("did not open the agent's own credential")
	}
}

func TestTaskMetadataRecordsPlacement(t *testing.T) {
	t.Run("dispatched", func(t *testing.T) {
		f := newFixture(t, resultStream)
		f.sealOwn(t, credentialName, time.Hour)
		if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckAccept {
			t.Fatalf("status %v", status)
		}
		want := map[string]any{
			"receiver_id": testReceiver, "pool": testPool, "adapter": "nomad",
			"offer": map[string]any{"agent": "claude", "runtime": "exec", "nomad.node_pool": "OCPv", "zone": "dc1"},
			"job":   "agent-run-a-t1",
		}
		if got := decodeObject(t, f.lastTask(t).Metadata); !reflect.DeepEqual(got, want) {
			t.Fatalf("task metadata %v, want %v", got, want)
		}
	})
	// The adopted job came from an earlier attempt, whose offer this attempt does not know.
	t.Run("adopted", func(t *testing.T) {
		m := newMultiFixture(t)
		m.cloud.existing = true
		if status := m.execute(t, "run-moved", "exec", 2); status != kgo.AckAccept {
			t.Fatalf("status %v", status)
		}
		want := map[string]any{"receiver_id": testReceiver, "pool": testPool, "adapter": "cloud", "job": "agent-run-moved-t1"}
		if got := decodeObject(t, m.lastTask(t).Metadata); !reflect.DeepEqual(got, want) {
			t.Fatalf("task metadata %v, want %v", got, want)
		}
	})
}

func TestTranscriptArtifactChunks(t *testing.T) {
	oversized := `{"type":"assistant","message":"` + strings.Repeat("x", maxLine) + `"}`
	for name, test := range map[string]struct {
		stream    []string
		heartbeat bool
		pointer   int
		raw       []int
	}{
		"result after an oversized line": {[]string{`{"type":"system"}`, oversized, `{"type":"assistant"}`, resultStream[1]}, false, 1, nil},
		// The stream outlives a heartbeat, which commits all but the newest line.
		"no result after a heartbeat": {[]string{`{"type":"system"}`, `{"type":"assistant"}`}, true, -1, nil},
		"empty and non-UTF-8 lines":   {[]string{`{"type":"system"}`, "", "\xff\xfe", resultStream[1]}, false, -1, []int{1, 2}},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.sealOwn(t, credentialName, time.Hour)
			f.adapter.stream = test.stream
			if test.heartbeat {
				f.adapter.endDelay = 3 * renewInterval
			}
			f.execute(t, f.submit(t, f.host, nil), 1)

			chunks := f.chunks(t)
			if len(chunks) != len(test.stream) {
				t.Fatalf("chunks=%d, want one per line (%d)", len(chunks), len(test.stream))
			}
			var offset int64
			for i, chunk := range chunks {
				update, part := chunk.ArtifactUpdate, chunk.ArtifactUpdate.Artifact.Parts[0]
				if update.TaskID != "run-a" || update.ContextID != "run-a" || update.Artifact.ArtifactID != "transcript" || chunk.Wherry.Seq != offset || part.MediaType != "application/x-ndjson" ||
					chunk.Wherry.Tenant != testTenant || chunk.Wherry.Pool != testPool {
					t.Fatalf("chunk %d: %#v", i, chunk)
				}
				if update.Append != (i > 0) || update.LastChunk != (i == len(chunks)-1) {
					t.Fatalf("chunk %d: append=%t lastChunk=%t", i, update.Append, update.LastChunk)
				}
				line := test.stream[i]
				if i == test.pointer {
					var pointer wire.PointerMetadata
					if err := json.Unmarshal(part.Metadata, &pointer); err != nil {
						t.Fatal(err)
					}
					if part.URL != "unstored:oversize" || part.Text != "" || pointer.SHA256 != taskHash([]byte(line)) || pointer.Size != int64(len(line)) {
						t.Fatalf("oversized line became %#v with %#v", part, pointer)
					}
				} else if slices.Contains(test.raw, i) {
					if part.Raw == nil || string(part.Raw) != line || part.Text != "" {
						t.Fatalf("chunk %d became %#v, want raw %q", i, part, line)
					}
				} else if part.Text != line {
					t.Fatalf("chunk %d text %q, want %q", i, part.Text, line)
				}
				offset += int64(len(line) + 1)
			}
			if len(f.records(t, admin.PoolEventsTopic(testPool))) == 0 {
				t.Fatal("nothing on the pool's events")
			}
			if !test.heartbeat {
				return
			}
			var offsets []int64
			for _, status := range f.eventStatuses(t) {
				if status.Wherry.Kind == "heartbeat" {
					offsets = append(offsets, *status.Wherry.LogOffset)
				}
			}
			// Once both lines have arrived, a heartbeat commits the first and records where the held second starts.
			if want := int64(len(test.stream[0]) + 1); len(offsets) == 0 || offsets[len(offsets)-1] != want {
				t.Fatalf("heartbeat offsets %v, want the last at %d", offsets, want)
			}
		})
	}
}

func TestHeartbeatsStayOffTheFeed(t *testing.T) {
	f := newFixture(t, resultStream)
	f.sealOwn(t, credentialName, time.Hour)
	f.adapter.stream = noResultStream
	f.adapter.endDelay = 4 * renewInterval
	f.execute(t, f.submit(t, f.host, nil), 1)
	heartbeats := 0
	for _, status := range f.eventStatuses(t) {
		if status.Wherry.Kind == "heartbeat" {
			heartbeats++
		}
	}
	if heartbeats == 0 {
		t.Fatal("no heartbeat on the pool's events")
	}
	for _, status := range f.feedStatuses(t) {
		if status.Wherry.Kind == "heartbeat" {
			t.Fatal("a heartbeat reached the tenant's feed")
		}
	}
}

func TestTaskResultDataPartWhenJSON(t *testing.T) {
	for _, result := range []string{`{"answer":42,"items":[1,2]}`, `[true,null]`, `42`} {
		t.Run(result, func(t *testing.T) {
			f := newFixture(t, resultStream)
			run := testActiveRun(t, f, "run-json", 1)
			if status := run.finish(f.ctx, Handle{Alloc: "alloc"}, nil, agentResult{Result: result}); status != kgo.AckAccept {
				t.Fatalf("status %v", status)
			}
			part := f.lastTask(t).Artifacts[0].Parts[0]
			if part.Text != "" || part.MediaType != "application/json" || string(part.Data) != result {
				t.Fatalf("result part %#v, want data %s", part, result)
			}
		})
	}
}

func TestTaskResultTextPartOtherwise(t *testing.T) {
	for _, result := range []string{"done", `{"answer":`, `{"k":1,"k":2}`, "42 is the answer"} {
		t.Run(result, func(t *testing.T) {
			f := newFixture(t, resultStream)
			run := testActiveRun(t, f, "run-text", 1)
			if status := run.finish(f.ctx, Handle{Alloc: "alloc"}, nil, agentResult{Result: result}); status != kgo.AckAccept {
				t.Fatalf("status %v", status)
			}
			task := f.lastTask(t)
			part := task.Artifacts[0].Parts[0]
			if part.Text != result || part.Data != nil || part.MediaType != "" {
				t.Fatalf("result part %#v, want text %q", part, result)
			}
			if task.ID != "run-text" || task.ContextID != "run-text" || len(task.History) != 1 || task.History[0].MessageID != "msg-a" {
				t.Fatalf("task ids or history %#v", task)
			}
		})
	}
}

func TestResultSharesTheTerminalTransaction(t *testing.T) {
	f := newFixture(t, resultStream)
	run := testActiveRun(t, f, "run-terminal", 1)
	run.finish(f.ctx, Handle{Alloc: "alloc"}, []Line{{Offset: 0, Next: 5, Data: []byte("line")}}, agentResult{Result: "done"})
	records := f.records(t, admin.TenantTopic(testTenant))
	if len(records) != 3 {
		t.Fatalf("feed records %d, want the last chunk, the result and the terminal", len(records))
	}
	var result wire.Result
	var terminal wire.RunStatus
	if wire.ParseRecord(records[1].Value, f.keys(), &result) != nil || wire.ParseRecord(records[2].Value, f.keys(), &terminal) != nil || terminal.Wherry.Kind != "terminal" {
		t.Fatal("the result is not followed by the terminal")
	}
	for _, record := range records[1:] {
		if record.ProducerID != records[0].ProducerID || record.ProducerEpoch != records[0].ProducerEpoch {
			t.Fatal("feed records of the turn's end come from different producers")
		}
	}
	if result.Wherry.TaskHash != run.taskHash || result.Wherry.Tenant != testTenant {
		t.Fatalf("result binding %#v", result.Wherry)
	}
}

func TestReceiverReleasesAfterTerminal(t *testing.T) {
	t.Run("stop", func(t *testing.T) {
		f := newFixture(t, resultStream)
		run := testActiveRun(t, f, "run-release-stop", 1)
		if status := run.finish(f.ctx, Handle{Alloc: "alloc"}, nil, agentResult{Result: "done"}); status != kgo.AckAccept {
			t.Fatalf("status %v", status)
		}
		f.adapter.mu.Lock()
		defer f.adapter.mu.Unlock()
		if f.adapter.stops != 1 || f.adapter.releases != 1 || f.adapter.releaseIDs[0] != "run-release-stop" {
			t.Fatalf("stops=%d releases=%v", f.adapter.stops, f.adapter.releaseIDs)
		}
	})
	t.Run("dispatch error", func(t *testing.T) {
		f := newFixture(t, resultStream)
		f.sealOwn(t, credentialName, time.Hour)
		f.adapter.dispatchErr = errors.New("dispatch failed")
		if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckReject {
			t.Fatalf("status %v", status)
		}
		f.adapter.mu.Lock()
		defer f.adapter.mu.Unlock()
		if f.adapter.dispatches != 1 || f.adapter.releases != 1 {
			t.Fatalf("dispatches=%d releases=%d", f.adapter.dispatches, f.adapter.releases)
		}
	})
}

func TestResultLineStopsAllocationBeforeTerminal(t *testing.T) {
	f := newFixture(t, resultStream)
	run := testActiveRun(t, f, "run-result-stop", 1)
	f.adapter.stopCheck = func() error {
		if n := len(f.records(t, admin.TenantTopic(testTenant))); n != 0 {
			return fmt.Errorf("feed records visible before Stop: %d", n)
		}
		return nil
	}
	if status := run.finish(f.ctx, Handle{Alloc: "alloc"}, nil, agentResult{Result: "done"}); status != kgo.AckAccept {
		t.Fatalf("finish status %v, want accept", status)
	}
	f.adapter.mu.Lock()
	stops := f.adapter.stops
	f.adapter.mu.Unlock()
	if stops != 1 || len(f.results(t)) != 1 || len(f.eventStatuses(t)) != 1 {
		t.Fatalf("stops=%d results=%d events=%d", stops, len(f.results(t)), len(f.eventStatuses(t)))
	}
}

func TestStopFailureReleasesWithoutTerminal(t *testing.T) {
	for _, test := range []struct {
		name string
		call func(*activeRun, context.Context) kgo.AckStatus
	}{
		{"result", func(run *activeRun, ctx context.Context) kgo.AckStatus {
			return run.finish(ctx, Handle{Alloc: "alloc"}, nil, agentResult{Result: "done"})
		}},
		{"failure", func(run *activeRun, ctx context.Context) kgo.AckStatus {
			return run.stopAndFail(ctx, Handle{Alloc: "alloc"}, "timeout", nil)
		}},
		{"cancel", func(run *activeRun, ctx context.Context) kgo.AckStatus {
			return run.stopAndCancel(ctx, Handle{Alloc: "alloc"}, nil)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.adapter.stopErr = errors.New("stop failed")
			run := testActiveRun(t, f, "run-stop-failure-"+test.name, 1)
			if status := test.call(run, f.ctx); status != kgo.AckRelease {
				t.Fatalf("status %v, want release", status)
			}
			if feed, events := len(f.records(t, admin.TenantTopic(testTenant))), len(f.records(t, admin.PoolEventsTopic(testPool))); feed != 0 || events != 0 {
				t.Fatalf("Stop failure published feed=%d events=%d", feed, events)
			}
		})
	}
}

// heartbeat signs a heartbeat for task at offset as receiver with key.
func (f fixture) heartbeat(t *testing.T, task *wire.Submission, hash, receiver string, key []byte, offset int64, mutate func(*wire.RunStatus)) []byte {
	t.Helper()
	status := f.receiver.status(task, hash, 1, "heartbeat", taskStatus(wire.TaskStateWorking, ""), &offset)
	status.Wherry.Receiver = receiver
	if mutate != nil {
		mutate(status)
	}
	return must(t)(wire.SignRecord(status, key))
}

func TestAdoptionResumesFromLastOffset(t *testing.T) {
	f := newFixture(t, resultStream)
	task := testSubmission("run-adopt")
	hash := strings.Repeat("a", 64)
	for _, offset := range []int64{10, 42} {
		f.produce(t, admin.PoolEventsTopic(testPool), "run-adopt", f.heartbeat(t, task, hash, testReceiver, f.receiver.signing, offset, nil))
	}
	// A fenced holder's aborted heartbeat must not move the resume point.
	aborted, err := f.receiver.newProducer("run-adopt")
	if err != nil {
		t.Fatal(err)
	}
	defer aborted.Close()
	if err := aborted.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := aborted.ProduceSync(f.ctx, &kgo.Record{Topic: admin.PoolEventsTopic(testPool), Key: []byte("run-adopt"), Value: f.heartbeat(t, task, hash, testReceiver, f.receiver.signing, 99, nil)}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := aborted.EndTransaction(f.ctx, kgo.TryAbort); err != nil {
		t.Fatal(err)
	}
	if got, err := f.receiver.lastOffset(f.ctx, task, hash); err != nil || got != 42 {
		t.Fatalf("resume offset %d err %v, want 42", got, err)
	}
}

func TestLastOffsetIgnoresMismatchedStatus(t *testing.T) {
	f := newFixture(t, resultStream)
	if err := f.service.TenantAdd(f.ctx, "tenant-b", testPool); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	task := testSubmission("run-offset-binding")
	hash := strings.Repeat("b", 64)
	for _, value := range [][]byte{
		f.heartbeat(t, task, hash, testReceiver, f.receiver.signing, 42, nil),
		f.heartbeat(t, task, strings.Repeat("c", 64), testReceiver, f.receiver.signing, 99, nil),
		f.heartbeat(t, task, hash, testReceiver, f.receiver.signing, 99, func(status *wire.RunStatus) { status.Wherry.Turn++ }),
		f.heartbeat(t, task, hash, testReceiver, f.receiver.signing, 99, func(status *wire.RunStatus) { status.Wherry.Tenant = "tenant-b" }),
	} {
		f.produce(t, admin.PoolEventsTopic(testPool), "run-offset-binding", value)
	}
	if got, err := f.receiver.lastOffset(f.ctx, task, hash); err != nil || got != 42 {
		t.Fatalf("resume offset %d err %v, want 42", got, err)
	}
}

func TestAdoptionReadFailureReleases(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.submit(t, f.host, nil)
	admitted := f.admitted(t, record)
	f.cluster.ControlKey(kmsg.ListOffsets.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		req := request.(*kmsg.ListOffsetsRequest)
		response := kmsg.NewPtrListOffsetsResponse()
		response.SetVersion(req.Version)
		for _, topic := range req.Topics {
			out := kmsg.ListOffsetsResponseTopic{Topic: topic.Topic}
			for _, partition := range topic.Partitions {
				out.Partitions = append(out.Partitions, kmsg.ListOffsetsResponseTopicPartition{Partition: partition.Partition, ErrorCode: kerr.UnknownServerError.Code})
			}
			response.Topics = append(response.Topics, out)
		}
		return response, nil, true
	})
	if status := f.receiver.execute(f.ctx, f.service.Admin, record, admitted, 2); status != kgo.AckRelease {
		t.Fatalf("status %v, want release", status)
	}
	if dispatches, _, _, _ := f.adapter.counts(); dispatches != 0 {
		t.Fatalf("adoption read failure dispatched %d jobs", dispatches)
	}
}

func TestAdoptionTrustsPeerHeartbeat(t *testing.T) {
	f := newFixture(t, resultStream)
	peerSigning, _ := f.addPeer(t, "receiver-peer", testPool)
	if err := f.service.PoolCreate(f.ctx, "pool-b", testTerms()); err != nil {
		t.Fatal(err)
	}
	otherSigning, _ := f.addPeer(t, "receiver-other-pool", "pool-b")
	task := testSubmission("run-peer")
	hash := strings.Repeat("a", 64)
	f.produce(t, admin.PoolEventsTopic(testPool), "run-peer", f.heartbeat(t, task, hash, "receiver-peer", peerSigning, 42, nil))
	f.produce(t, admin.PoolEventsTopic(testPool), "run-peer", f.heartbeat(t, task, hash, "receiver-other-pool", otherSigning, 99, nil))
	if got, err := f.receiver.lastOffset(f.ctx, task, hash); err != nil || got != 42 {
		t.Fatalf("peer heartbeat offset %d err %v, want 42", got, err)
	}
}

func TestFencedProducerStopsRunWrites(t *testing.T) {
	f := newFixture(t, resultStream)
	zombie := testActiveRun(t, f, "run-fence", 1)
	if err := zombie.commit(f.ctx, zombie.statusRecords("claimed", taskStatus(wire.TaskStateWorking, ""), nil, nil)...); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(admin.TransactionalPrefix(testPool)+"run-fence", "p."+testPool+".") {
		t.Fatal("transactional ID is outside the pool's prefix")
	}
	adopter := testActiveRun(t, f, "run-fence", 2)
	if err := adopter.commit(f.ctx, adopter.statusRecords("claimed", taskStatus(wire.TaskStateWorking, ""), nil, nil)...); err != nil {
		t.Fatal(err)
	}
	err := zombie.commit(f.ctx, zombie.events(zombie.status("heartbeat", taskStatus(wire.TaskStateWorking, ""), nil, nil)))
	if err == nil {
		t.Fatal("zombie write succeeded after adoption")
	}
	if status := zombie.abandon(err); status != 0 {
		t.Fatalf("fenced run returned ack %v, want 0 (no acknowledgement)", status)
	}
	for _, status := range f.eventStatuses(t) {
		if status.Wherry.Kind == "heartbeat" {
			t.Fatal("zombie heartbeat is readable after fencing")
		}
	}
}

func TestFollowErrorReasonIsFixed(t *testing.T) {
	f := newFixture(t, nil)
	f.adapter.followErr = errors.New("secret upstream detail")
	task := runToTask(t, f)
	if got := task.Artifacts[0].Parts[0].Text; got != "log stream failed" {
		t.Fatalf("result reason %q, want fixed reason", got)
	}
	logs := f.logs.String()
	if strings.Contains(logs, "secret upstream detail") || !strings.Contains(logs, "*errors.errorString") {
		t.Fatalf("unexpected follow error log: %q", logs)
	}
}

func TestDispatchErrorReasonIsFixed(t *testing.T) {
	const secret = "token=s3cr3t-dispatch-detail"
	f := newFixture(t, resultStream)
	f.sealOwn(t, credentialName, time.Hour)
	f.adapter.dispatchErr = errors.New(secret)
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckReject {
		t.Fatalf("status %v", status)
	}
	if got := f.lastTask(t).Artifacts[0].Parts[0].Text; got != "dispatch failed" {
		t.Fatalf("result reason %q, want fixed reason", got)
	}
	for _, topic := range []string{admin.PoolEventsTopic(testPool), admin.TenantTopic(testTenant)} {
		for _, record := range f.records(t, topic) {
			if strings.Contains(string(record.Value), secret) {
				t.Fatalf("dispatch error detail published on %s", topic)
			}
		}
	}
	if logs := f.logs.String(); strings.Contains(logs, secret) || !strings.Contains(logs, "*errors.errorString") {
		t.Fatalf("unexpected dispatch error log: %q", logs)
	}
}

func TestAdapterErrorLogsCarryOnlyType(t *testing.T) {
	const secret = "token=s3cr3t-adapter-detail"
	for _, test := range []struct {
		name string
		call func(*testing.T, fixture)
	}{
		{"lookup", func(t *testing.T, f fixture) {
			f.adapter.lookupErr = errors.New(secret)
			f.execute(t, f.submit(t, f.host, nil), 2)
		}},
		{"stop on result", func(t *testing.T, f fixture) {
			f.adapter.stopErr = errors.New(secret)
			testActiveRun(t, f, "run-log-result", 1).finish(f.ctx, Handle{Alloc: "alloc"}, nil, agentResult{Result: "done"})
		}},
		{"stop on failure", func(t *testing.T, f fixture) {
			f.adapter.stopErr = errors.New(secret)
			testActiveRun(t, f, "run-log-failure", 1).stopAndFail(f.ctx, Handle{Alloc: "alloc"}, "timeout", nil)
		}},
		{"stop on denial", func(t *testing.T, f fixture) {
			f.adapter.stopErr = errors.New(secret)
			testActiveRun(t, f, "run-log-denial", 1).deny(f.ctx, Handle{Alloc: "alloc"}, nil)
		}},
		{"release", func(t *testing.T, f fixture) {
			f.adapter.releaseErr = errors.New(secret)
			testActiveRun(t, f, "run-log-release", 1).release(f.ctx)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			test.call(t, f)
			if logs := f.logs.String(); strings.Contains(logs, secret) || !strings.Contains(logs, "*errors.errorString") {
				t.Fatalf("unexpected adapter error log: %q", logs)
			}
		})
	}
}

func TestGitCloneFailureFixedReason(t *testing.T) {
	const secret = "fatal: could not read from https://user:s3cr3t-git-token@git.example.test/org/repo.git"
	f := newFixture(t, resultStream)
	f.receiver.adapters[0].Git = true
	f.sealOwn(t, credentialName, time.Hour)
	f.adapter.dispatchErr = fmt.Errorf("%w: %s", ErrGitContext, secret)
	if status := f.execute(t, f.submit(t, f.host, withGitPart), 1); status != kgo.AckReject {
		t.Fatalf("status %v, want reject", status)
	}
	task := f.lastTask(t)
	if task.Status.State != wire.TaskStateFailed || task.Status.Message.Parts[0].Text != "git context unavailable" || task.Artifacts[0].Parts[0].Text != "git context unavailable" {
		t.Fatalf("task status %#v result %q, want FAILED with the fixed reason", task.Status, task.Artifacts[0].Parts[0].Text)
	}
	for _, topic := range []string{admin.PoolEventsTopic(testPool), admin.TenantTopic(testTenant)} {
		for _, record := range f.records(t, topic) {
			if strings.Contains(string(record.Value), "s3cr3t-git-token") {
				t.Fatalf("git error text published on %s", topic)
			}
		}
	}
	if logs := f.logs.String(); strings.Contains(logs, "s3cr3t-git-token") || !strings.Contains(logs, "git context unavailable: *fmt.wrapError") {
		t.Fatalf("unexpected git failure log: %q", logs)
	}
}

func TestStartedStatusRecordsGitCommit(t *testing.T) {
	const commit = "0123456789abcdef0123456789abcdef01234567"
	for name, test := range map[string]struct {
		mutate func(*wire.Submission)
		want   string
	}{
		"git":     {withGitPart, `{"git_commit":"` + commit + `"}`},
		"without": {nil, ""},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.receiver.adapters[0].Git = true
			f.sealOwn(t, credentialName, time.Hour)
			f.adapter.commit = commit
			if status := f.execute(t, f.submit(t, f.host, test.mutate), 1); status != kgo.AckAccept {
				t.Fatalf("status %v", status)
			}
			var started []wire.RunStatus
			for _, status := range f.feedStatuses(t) {
				if status.Wherry.Kind == "started" {
					started = append(started, status)
				}
			}
			if len(started) != 1 || string(started[0].StatusUpdate.Metadata) != test.want {
				t.Fatalf("started statuses %#v, want one with metadata %q", started, test.want)
			}
		})
	}
}

func TestResultLine(t *testing.T) {
	if result, ok := resultLine([]byte(`{"type":"result","result":"ok","is_error":true}`)); !ok || result.Result != "ok" || !result.IsError {
		t.Fatalf("got %#v %v", result, ok)
	}
	for _, line := range []string{`{"type":"assistant"}`, `not json`, ``} {
		if _, ok := resultLine([]byte(line)); ok {
			t.Fatalf("%q treated as a result", line)
		}
	}
}

// podmanResultLine is the result record captured in ../adapter/nomad/testdata/podman-cri-stdout.log,
// without its CRI framing.
func podmanResultLine(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("../adapter/nomad/testdata/podman-cri-stdout.log")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	framed := strings.SplitN(lines[len(lines)-1], " ", 4)
	if len(framed) != 4 || framed[1] != "stdout" || framed[2] != "F" {
		t.Fatalf("unexpected CRI framing %.80q", lines[len(lines)-1])
	}
	return framed[3]
}

func TestResultArtifactCarriesAgentResult(t *testing.T) {
	line := podmanResultLine(t)
	f := newFixture(t, resultStream)
	f.sealOwn(t, credentialName, time.Hour)
	f.adapter.stream = []string{line}
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	artifact := f.lastTask(t).Artifacts[0]
	want := decodeObject(t, []byte(line))
	if artifact.Parts[0].Text != want["result"] {
		t.Fatalf("result part %#v, want the record's result", artifact.Parts[0])
	}
	delete(want, "result")
	got := decodeObject(t, artifact.Metadata)
	if len(got) != 1 || !reflect.DeepEqual(got["agent_result"], want) {
		t.Fatalf("artifact metadata %s, want agent_result with every member but result", artifact.Metadata)
	}
}

func TestAgentResultOmittedWhenOversized(t *testing.T) {
	// The remaining object is {"pad":"<n bytes>","type":"result"}: n plus 26 bytes.
	line := func(pad int, result string) []byte {
		return []byte(`{"type":"result","result":"` + result + `","pad":"` + strings.Repeat("x", pad) + `"}`)
	}
	if result, _ := resultLine(line(maxAgentResult-26, strings.Repeat("r", 2*maxAgentResult))); result.metadata == nil {
		t.Fatal("agent_result at the bound, with a larger result member, was omitted")
	}
	result, ok := resultLine(line(maxAgentResult-25, "done"))
	if !ok || result.metadata != nil {
		t.Fatalf("agent_result one byte over the bound kept: ok=%t metadata %d bytes", ok, len(result.metadata))
	}
	f := newFixture(t, resultStream)
	run := testActiveRun(t, f, "run-oversized", 1)
	if status := run.finish(f.ctx, Handle{Alloc: "alloc"}, nil, result); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	if artifact := f.lastTask(t).Artifacts[0]; artifact.Metadata != nil || artifact.Parts[0].Text != "done" {
		t.Fatalf("result artifact %#v, want text done and no metadata", artifact)
	}
}

func TestAgentResultOmittedWhenNotObject(t *testing.T) {
	for _, data := range []string{`["result"]`, `"result"`, `null`, `42`, `{"type":"result","a":1,"a":2}`, "{\"type\":\"result\",\"a\":\"\xff\"}"} {
		if metadata := agentResultMetadata([]byte(data)); metadata != nil {
			t.Fatalf("%q produced metadata %s", data, metadata)
		}
	}
	// encoding/json accepts the duplicate member, so the line is a result whose record cannot travel.
	result, ok := resultLine([]byte(`{"type":"result","result":"done","n":1,"n":2}`))
	if !ok || result.metadata != nil {
		t.Fatalf("ok=%t metadata %s", ok, result.metadata)
	}
	f := newFixture(t, resultStream)
	run := testActiveRun(t, f, "run-duplicate", 1)
	if status := run.finish(f.ctx, Handle{Alloc: "alloc"}, nil, result); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	if artifact := f.lastTask(t).Artifacts[0]; artifact.Metadata != nil {
		t.Fatalf("result artifact metadata %s, want none", artifact.Metadata)
	}
}

func TestReceiverRoutesToMatchedAdapter(t *testing.T) {
	m := newMultiFixture(t)
	m.sealOwn(t, "cred-cloud", time.Hour)
	if status := m.execute(t, "run-lambda", "lambda", 1); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	labDispatches, _, _, _ := m.lab.counts()
	cloudDispatches, _, cloudFollows, cloudReleases := m.cloud.counts()
	if labDispatches != 0 || cloudDispatches != 1 || cloudFollows != 1 || cloudReleases != 1 || m.cloud.lastRun.Offer["runtime"] != "lambda" {
		t.Fatalf("lab dispatches=%d; cloud dispatches=%d follows=%d releases=%d run=%#v", labDispatches, cloudDispatches, cloudFollows, cloudReleases, m.cloud.lastRun)
	}
	if status := m.execute(t, "run-exec", "exec", 1); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	labDispatches, _, _, _ = m.lab.counts()
	cloudDispatches, _, _, _ = m.cloud.counts()
	if labDispatches != 1 || cloudDispatches != 1 {
		t.Fatalf("exec, offered by both, went to lab=%d cloud=%d; want the first configured adapter", labDispatches, cloudDispatches-1)
	}
}

func TestReceiverRedeliveryAdoptsViaLookup(t *testing.T) {
	m := newMultiFixture(t)
	m.cloud.existing = true
	if status := m.execute(t, "run-moved", "exec", 2); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	labDispatches, labLookups, labFollows, labReleases := m.lab.counts()
	cloudDispatches, cloudLookups, cloudFollows, cloudReleases := m.cloud.counts()
	if labLookups != 1 || cloudLookups != 1 {
		t.Fatalf("lookups lab=%d cloud=%d, want one each in order", labLookups, cloudLookups)
	}
	if labDispatches != 0 || labFollows != 0 || labReleases != 0 {
		t.Fatalf("matched adapter used: dispatches=%d follows=%d releases=%d", labDispatches, labFollows, labReleases)
	}
	if cloudDispatches != 0 || cloudFollows != 1 || cloudReleases != 1 {
		t.Fatalf("adopting adapter: dispatches=%d follows=%d releases=%d, want 0, 1, 1", cloudDispatches, cloudFollows, cloudReleases)
	}
	if _, edgeLookups, _, _ := m.edge.counts(); edgeLookups != 0 {
		t.Fatalf("lookup continued past the first hit: edge lookups=%d", edgeLookups)
	}
}

func TestReceiverFirstAttemptSkipsLookup(t *testing.T) {
	m := newMultiFixture(t)
	m.cloud.existing = true
	if status := m.execute(t, "run-first", "exec", 1); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	labDispatches, labLookups, _, _ := m.lab.counts()
	_, cloudLookups, _, _ := m.cloud.counts()
	if labLookups != 0 || cloudLookups != 0 || labDispatches != 1 {
		t.Fatalf("lookups lab=%d cloud=%d, lab dispatches=%d", labLookups, cloudLookups, labDispatches)
	}
}

func TestReceiverCredentialPerAdapter(t *testing.T) {
	m := newMultiFixture(t)
	if status := m.execute(t, "run-lab", "exec", 1); status != kgo.AckAccept {
		t.Fatalf("lab status %v", status)
	}
	if got := string(m.lab.lastRun.Credential); got != testToken {
		t.Fatal("lab adapter did not receive its own credential")
	}
	if status := m.execute(t, "run-edge", "podman", 1); status != kgo.AckAccept {
		t.Fatalf("edge status %v", status)
	}
	if m.edge.lastRun.Credential != nil {
		t.Fatal("adapter without a configured credential received one")
	}
	if status := m.execute(t, "run-cloud", "lambda", 1); status != kgo.AckAccept {
		t.Fatalf("cloud status %v, want accept", status)
	}
	if dispatches, _, _, releases := m.cloud.counts(); dispatches != 0 || releases != 1 {
		t.Fatalf("cloud dispatches=%d releases=%d, want 0 and 1", dispatches, releases)
	}
	statuses := m.feedStatuses(t)
	if got := statuses[len(statuses)-1].StatusUpdate.Status.Message.Parts[0].Text; got != "credential cred-cloud required" {
		t.Fatalf("reason %q", got)
	}
	if strings.Contains(m.logs.String(), testToken) {
		t.Fatal("credential value appeared in receiver logs")
	}
}

func TestShutdownCommitsHeldTranscriptLine(t *testing.T) {
	f := newFixture(t, noResultStream)
	f.adapter.endDelay = 30 * time.Second
	ctx, shutdown := context.WithCancel(f.ctx)
	done := startInFlight(t, f, ctx, "run-shutdown")
	shutdown()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("claimOne after shutdown: %v", err)
	}
	for _, chunk := range f.chunks(t) {
		if strings.Contains(chunk.ArtifactUpdate.Artifact.Parts[0].Text, "working") {
			return
		}
	}
	t.Fatalf("the transcript line held for the next commit was lost at shutdown; logs:\n%s", f.logs.String())
}

func TestContinuationResumesInterruptedTask(t *testing.T) {
	f := newFixture(t, resultStream)
	ctx, stop := context.WithCancel(f.ctx)
	defer stop()
	f.run(t, ctx)
	f.turn(t, "run-a", 1)
	interrupted := f.await(t, "run-a", anyTask)
	if interrupted.Status.State != wire.TaskStateAuthRequired {
		t.Fatalf("first turn %#v, want AUTH_REQUIRED", interrupted)
	}
	f.sealOwn(t, credentialName, time.Hour)
	f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Turn = 2
		task.Message.MessageID, task.Message.Parts = "msg-2", []wire.Part{{Text: "continue"}}
	})
	completed := f.await(t, "run-a", func(task wire.Task) bool { return wire.Terminal(task.Status.State) })
	if completed.Status.State != wire.TaskStateCompleted || len(completed.History) != 2 || completed.History[0].Parts[0].Text != "hi" || completed.History[1].Parts[0].Text != "continue" {
		t.Fatalf("completed continuation %#v", completed)
	}
	f.adapter.mu.Lock()
	run := f.adapter.lastRun
	f.adapter.mu.Unlock()
	if want := resumedHeader + "\n\nhi\n\ncontinue"; string(run.Prompt) != want || run.Turn != 2 {
		t.Fatalf("turn %d prompt %q, want turn 2 prompt %q", run.Turn, run.Prompt, want)
	}
}

// deleteACLs removes every ACL of the receiver's principal, as revocation's second step does.
func (f fixture) deleteACLs(t *testing.T) {
	t.Helper()
	builder := kadm.NewACLs().AnyResource().Allow(admin.ReceiverPrincipal(testReceiver)).AllowHosts("*").Operations(kadm.OpAny).ResourcePatternType(kadm.ACLPatternAny)
	builder.PrefixUser()
	results, err := kadm.NewClient(f.service.Admin).DeleteACLs(f.ctx, builder)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range results {
		if result.Err != nil {
			t.Fatal(result.Err)
		}
	}
}

func TestDenialStopsClaimingAndAccessResumesIt(t *testing.T) {
	f := newFixture(t, resultStream)
	ctx, stop := context.WithCancel(f.ctx)
	defer stop()
	f.sealOwn(t, credentialName, time.Hour)
	f.run(t, ctx)
	f.submit(t, f.host, nil)
	f.await(t, "run-a", anyTask)
	f.deleteACLs(t)
	for !f.receiver.denied() {
		if f.ctx.Err() != nil {
			t.Fatalf("receiver never noticed the denial; logs:\n%s", f.logs.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Run, task.Message.TaskID, task.Message.ContextID = "run-b", "run-b", "run-b"
	})
	time.Sleep(2 * time.Second)
	if dispatches, _, _, _ := f.adapter.counts(); dispatches != 1 {
		t.Fatalf("dispatches %d while denied, want only the task before it", dispatches)
	}
	// Access returning resumes claiming.
	if err := f.service.TenantAdd(f.ctx, testTenant, testPool); err != nil {
		t.Fatal(err)
	}
	f.await(t, "run-b", anyTask)
	if !strings.Contains(f.logs.String(), "access restored") {
		t.Fatalf("logs:\n%s", f.logs.String())
	}
}

func TestDeniedReceiverDoesNotClaim(t *testing.T) {
	f := newFixture(t, resultStream)
	f.sealOwn(t, credentialName, time.Hour)
	f.receiver.deny(kerr.GroupAuthorizationFailed)
	f.submit(t, f.host, nil)
	ctx, stop := context.WithTimeout(f.ctx, 2*time.Second)
	defer stop()
	// The broker still grants access: only the held denial keeps the receiver from claiming.
	f.receiver.claimLoop(ctx)
	if dispatches, _, _, _ := f.adapter.counts(); dispatches != 0 || len(f.records(t, admin.PoolEventsTopic(testPool))) != 0 {
		t.Fatalf("a denied receiver claimed: dispatches %d", dispatches)
	}
	if record, delivered := f.pollPool(t, "run-a", 5*time.Second); !delivered || record.DeliveryCount() != 1 {
		t.Fatal("the task was acquired while the receiver was denied")
	}
}

func TestDeniedReceiverStopsInFlightRunsAfterOneLockInterval(t *testing.T) {
	f := newFixture(t, noResultStream)
	f.adapter.endDelay = 30 * time.Second
	// kfake refuses a share lock below its minimum; the receiver reads the interval from the terms it folded.
	f.receiver.mu.Lock()
	terms := f.receiver.state.Pools[testPool]
	terms.LockDurationMS = 2000
	f.receiver.state.Pools[testPool] = terms
	f.receiver.mu.Unlock()
	done := startInFlight(t, f, f.ctx, "run-denied")
	f.receiver.deny(kerr.TopicAuthorizationFailed)
	deniedAt := time.Now()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("in-flight run kept going after a lasting denial")
	}
	if waited := time.Since(deniedAt); waited < 2*time.Second {
		t.Fatalf("run stopped %s after the denial, before one lock interval", waited)
	}
	f.adapter.mu.Lock()
	stops, releaseIDs := f.adapter.stops, f.adapter.releaseIDs
	f.adapter.mu.Unlock()
	if stops != 1 || !slices.Equal(releaseIDs, []string{"run-denied"}) {
		t.Fatalf("stops=%d releases=%v", stops, releaseIDs)
	}
	// Released, not acknowledged: the submission is delivered again to the pool.
	if record, delivered := f.pollPool(t, "run-denied", 10*time.Second); !delivered || record.DeliveryCount() < 2 {
		t.Fatal("denied run's submission was not released to the pool")
	}
}

func TestResultAfterLastingDenialReleasesTask(t *testing.T) {
	f := newFixture(t, resultStream)
	run := testActiveRun(t, f, "run-denied-result", 1)
	f.receiver.mu.Lock()
	f.receiver.deniedSince = time.Now().Add(-time.Hour)
	f.receiver.mu.Unlock()
	if status := run.finish(f.ctx, Handle{Alloc: "alloc"}, nil, agentResult{Result: "done"}); status != kgo.AckRelease {
		t.Fatalf("status %v, want release: an untrusted result must not end the task", status)
	}
	if state, reason := terminalReason(t, f, "run-denied-result"); state != wire.TaskStateFailed || reason != "receiver denied" {
		t.Fatalf("terminal %s %q, want FAILED receiver denied", state, reason)
	}
}
