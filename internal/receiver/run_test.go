package receiver

import (
	"cmp"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/adapter"
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
	return &activeRun{receiver: f.receiver, producer: producer, task: task, taskHash: taskHash(record.Value), history: []wire.Message{task.Message}, attempt: attempt, adapter: f.receiver.adapters[0], redactionKey: testRedactionKey}
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
	if logs := f.logs.String(); !strings.Contains(logs, `agent ended with exit code 1 signal "" on node node-1`) {
		t.Fatalf("logs lack the terminal state:\n%s", logs)
	}
}

func TestStatusesAreDualWritten(t *testing.T) {
	f := newFixture(t, resultStream)
	f.answer(t, credentialName)
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
	f.answer(t, credentialName)
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
	if !oneTransaction(records[:2]) {
		t.Fatal("echo and claimed status come from different transactions")
	}
}

// oneTransaction reports records of one partition written by one transaction: a commit marker takes an
// offset after every transaction, so records of two transactions are never adjacent.
func oneTransaction(records []*kgo.Record) bool {
	for i := 1; i < len(records); i++ {
		if records[i].Offset != records[i-1].Offset+1 || records[i].ProducerID != records[0].ProducerID || records[i].ProducerEpoch != records[0].ProducerEpoch {
			return false
		}
	}
	return true
}

func TestInterruptedStatusSharesItsResultsTransaction(t *testing.T) {
	f := newFixture(t, resultStream)
	f.execute(t, f.submit(t, f.host, nil), 1)
	records := f.records(t, admin.TenantTopic(testTenant))
	// echo and claimed, the seal request, then the AUTH_REQUIRED result and the interrupted status.
	if len(records) != 5 {
		t.Fatalf("feed records %d, want 5", len(records))
	}
	var result wire.Result
	var interrupted wire.RunStatus
	if wire.ParseRecord(records[3].Value, f.keys(), &result) != nil || wire.ParseRecord(records[4].Value, f.keys(), &interrupted) != nil || interrupted.Wherry.Kind != "interrupted" {
		t.Fatal("the result is not followed by the interrupted status")
	}
	if !oneTransaction(records[3:]) || oneTransaction(records[2:4]) {
		t.Fatal("the AUTH_REQUIRED result and its interrupted status are not one transaction of their own")
	}
}

func TestNoSealWithinTheWaitInterrupts(t *testing.T) {
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

// lab and cloud both launch claude; a task routed to cloud names cloud in its start failure.
func TestStartFailureNamesTheRoutedAdapter(t *testing.T) {
	m := newMultiFixture(t)
	m.answer(t, "cred-cloud")
	m.cloud.outcome = adapter.OutcomeNothingCreated
	if status := m.execute(t, "run-a", accelerated, 1); status != kgo.AckRelease {
		t.Fatalf("status %v, want release", status)
	}
	if failures := m.startFailures(t); len(failures) != 1 || failures[0].Class != "dispatch" || failures[0].Adapter != "cloud" {
		t.Fatalf("start failures %#v, want one dispatch naming cloud", failures)
	}
}

func TestTaskMetadataRecordsPlacement(t *testing.T) {
	t.Run("dispatched", func(t *testing.T) {
		f := newFixture(t, resultStream)
		f.answer(t, credentialName)
		if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckAccept {
			t.Fatalf("status %v", status)
		}
		want := map[string]any{
			"receiver_id": testReceiver, "pool": testPool, "adapter": "nomad",
			"labels": map[string]any{"zone": "dc1", "example.com/gpu": "none"},
			"job":    "agent-run-a-t1",
		}
		if got := decodeObject(t, f.lastTask(t).Metadata); !reflect.DeepEqual(got, want) {
			t.Fatalf("task metadata %v, want %v", got, want)
		}
	})
	// The adopted job came from an earlier attempt, whose offer this attempt does not know.
	t.Run("adopted", func(t *testing.T) {
		m := newMultiFixture(t)
		m.cloud.job = adapter.JobRunning
		if status := m.execute(t, "run-moved", cpu, 2); status != kgo.AckAccept {
			t.Fatalf("status %v", status)
		}
		want := map[string]any{"receiver_id": testReceiver, "pool": testPool, "adapter": "cloud", "job": "agent-run-moved-t1"}
		if got := decodeObject(t, m.lastTask(t).Metadata); !reflect.DeepEqual(got, want) {
			t.Fatalf("task metadata %v, want %v", got, want)
		}
	})
}

func TestTranscriptArtifactChunks(t *testing.T) {
	oversized := `{"type":"assistant","message":"` + strings.Repeat("x", wire.MaxLine) + `"}`
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
			f.answer(t, credentialName)
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

func TestOversizedAgentOutputIsBoundedAndResultMetadataIsAllowListed(t *testing.T) {
	f := newFixture(t, resultStream)
	f.answer(t, credentialName)
	huge := strings.Repeat("x", 3<<19)
	f.adapter.stream = []string{
		`{"type":"assistant","message":"` + huge + `"}`,
		`{"type":"result","result":"` + huge + `","is_error":false,"duration_ms":1234,"duration_api_ms":1200,"num_turns":2,"total_cost_usd":0.25,"usage":{"input_tokens":10},"credential":"sk-live-must-not-travel"}`,
	}
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckAccept {
		t.Fatalf("status %v, want accept", status)
	}
	task := f.lastTask(t)
	if task.Status.State != wire.TaskStateCompleted || task.Artifacts[0].Parts[0].Text != "[redacted text over 65536 bytes]" {
		t.Fatalf("task ended %s with result %#v", task.Status.State, task.Artifacts[0].Parts[0])
	}
	chunks := f.chunks(t)
	if len(chunks) != 2 {
		t.Fatalf("chunks %d, want the transcript and result lines", len(chunks))
	}
	for i, chunk := range chunks {
		part := chunk.ArtifactUpdate.Artifact.Parts[0]
		if part.URL != "unstored:oversize" || part.Text != "" {
			t.Fatalf("chunk %d part %#v, want an oversized pointer", i, part)
		}
	}
	metadata := decodeObject(t, task.Artifacts[0].Metadata)
	want := map[string]any{"agent_result": map[string]any{
		"duration_ms": 1234.0, "duration_api_ms": 1200.0, "num_turns": 2.0,
		"total_cost_usd": 0.25, "usage": map[string]any{"input_tokens": 10.0},
	}}
	if !reflect.DeepEqual(metadata, want) {
		t.Fatalf("result metadata %v, want only usage, cost and duration fields", metadata)
	}
}

func TestHeartbeatsStayOffTheFeed(t *testing.T) {
	f := newFixture(t, resultStream)
	f.answer(t, credentialName)
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
			if status := run.finish(f.ctx, adapter.Handle{}, nil, agentResult{Result: result}); status != kgo.AckAccept {
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
			if status := run.finish(f.ctx, adapter.Handle{}, nil, agentResult{Result: result}); status != kgo.AckAccept {
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
	run.finish(f.ctx, adapter.Handle{}, []adapter.Line{{Offset: 0, Next: 5, Data: []byte("line")}}, agentResult{Result: "done"})
	records := f.records(t, admin.TenantTopic(testTenant))
	if len(records) != 3 {
		t.Fatalf("feed records %d, want the last chunk, the result and the terminal", len(records))
	}
	var result wire.Result
	var terminal wire.RunStatus
	if wire.ParseRecord(records[1].Value, f.keys(), &result) != nil || wire.ParseRecord(records[2].Value, f.keys(), &terminal) != nil || terminal.Wherry.Kind != "terminal" {
		t.Fatal("the result is not followed by the terminal")
	}
	if !oneTransaction(records) {
		t.Fatal("feed records of the turn's end come from different transactions")
	}
	if result.Wherry.TaskHash != run.taskHash || result.Wherry.Tenant != testTenant {
		t.Fatalf("result binding %#v", result.Wherry)
	}
}

func TestReceiverReleasesAfterTerminal(t *testing.T) {
	t.Run("stop", func(t *testing.T) {
		f := newFixture(t, resultStream)
		run := testActiveRun(t, f, "run-release-stop", 1)
		if status := run.finish(f.ctx, adapter.Handle{}, nil, agentResult{Result: "done"}); status != kgo.AckAccept {
			t.Fatalf("status %v", status)
		}
		f.adapter.mu.Lock()
		defer f.adapter.mu.Unlock()
		if f.adapter.stops != 1 || f.adapter.releases != 1 || f.adapter.releaseIDs[0] != "run-release-stop" {
			t.Fatalf("stops=%d releases=%v", f.adapter.stops, f.adapter.releaseIDs)
		}
	})
	t.Run("dispatch error with no job", func(t *testing.T) {
		f := newFixture(t, resultStream)
		f.answer(t, credentialName)
		f.adapter.dispatchErr = errors.New("dispatch failed")
		if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckRelease {
			t.Fatalf("status %v", status)
		}
		f.adapter.mu.Lock()
		defer f.adapter.mu.Unlock()
		if f.adapter.dispatches != 1 || f.adapter.lookups != 1 || f.adapter.releases != 1 {
			t.Fatalf("dispatches=%d lookups=%d releases=%d", f.adapter.dispatches, f.adapter.lookups, f.adapter.releases)
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
	if status := run.finish(f.ctx, adapter.Handle{}, nil, agentResult{Result: "done"}); status != kgo.AckAccept {
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
	pending := []adapter.Line{{Offset: 0, Next: 8, Data: []byte("working")}}
	for _, test := range []struct {
		name string
		call func(*activeRun, context.Context, []adapter.Line) kgo.AckStatus
	}{
		{"result", func(run *activeRun, ctx context.Context, pending []adapter.Line) kgo.AckStatus {
			return run.finish(ctx, adapter.Handle{}, pending, agentResult{Result: "done"})
		}},
		{"failure", func(run *activeRun, ctx context.Context, pending []adapter.Line) kgo.AckStatus {
			return run.stopAndFail(ctx, adapter.Handle{}, "timeout", pending)
		}},
		{"cancel", func(run *activeRun, ctx context.Context, pending []adapter.Line) kgo.AckStatus {
			return run.stopAndCancel(ctx, adapter.Handle{}, pending)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.adapter.stopErr = errors.New("stop failed")
			run := testActiveRun(t, f, "run-stop-failure-"+test.name, 1)
			if status := test.call(run, f.ctx, pending); status != kgo.AckRelease {
				t.Fatalf("status %v, want release", status)
			}
			if feed, events := len(f.records(t, admin.TenantTopic(testTenant))), len(f.records(t, admin.PoolEventsTopic(testPool))); feed != 0 || events != 0 {
				t.Fatalf("Stop failure published feed=%d events=%d", feed, events)
			}
		})
		t.Run(test.name+" at the budget", func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.adapter.stopErr = errors.New("stop failed")
			run := testActiveRun(t, f, "run-stop-budget-"+test.name, int64(testTerms().DeliveryLimit)-1)
			status := test.call(run, f.ctx, pending)
			if chunks := f.chunks(t); len(chunks) != 1 || chunks[0].ArtifactUpdate.Artifact.Parts[0].Text != "working" || !chunks[0].ArtifactUpdate.LastChunk {
				t.Fatalf("chunks %#v, want the pending line as the last chunk", chunks)
			}
			if test.name == "result" {
				// The agent's result stands; the job that would not stop is left to the sweep.
				if task := f.lastTask(t); status != kgo.AckAccept || task.Status.State != wire.TaskStateCompleted || task.Artifacts[0].Parts[0].Text != "done" {
					t.Fatalf("status %v task %#v, want the completed result accepted", status, task.Status)
				}
				return
			}
			if status != kgo.AckReject {
				t.Fatalf("status %v, want reject", status)
			}
			if state, reason := terminalReason(t, f, "run-stop-budget-"+test.name); state != wire.TaskStateFailed || reason != "delivery budget exhausted" {
				t.Fatalf("terminal %s %q", state, reason)
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
	peerSigning := f.addPeer(t, "receiver-peer", testPool)
	if err := f.service.PoolCreate(f.ctx, "pool-b", testTerms()); err != nil {
		t.Fatal(err)
	}
	otherSigning := f.addPeer(t, "receiver-other-pool", "pool-b")
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

// shortLock makes the receiver read a lock duration below kfake's minimum from the terms it folded.
func shortLock(f fixture, lock time.Duration) {
	f.receiver.mu.Lock()
	defer f.receiver.mu.Unlock()
	terms := f.receiver.state.Pools[testPool]
	terms.LockDurationMS = lock.Milliseconds()
	f.receiver.state.Pools[testPool] = terms
}

func TestFollowLinkLossResumesFromOffset(t *testing.T) {
	f := newFixture(t, resultStream)
	f.adapter.linkLost = 1
	f.answer(t, credentialName)
	f.submit(t, f.host, nil)
	if err := f.receiver.claimOne(f.ctx); err != nil {
		t.Fatal(err)
	}
	if task := f.lastTask(t); task.Status.State != wire.TaskStateCompleted || task.Artifacts[0].Parts[0].Text != "done" {
		t.Fatalf("task %#v after a lost plugin link; want the run completed; logs:\n%s", task.Status, f.logs.String())
	}
	f.adapter.mu.Lock()
	froms := f.adapter.froms
	f.adapter.mu.Unlock()
	first := int64(len(resultStream[0]) + 1)
	if !slices.Equal(froms, []int64{0, first}) {
		t.Fatalf("follows from %v; want 0, then the offset after the line taken before the loss", froms)
	}
	working := 0
	for _, chunk := range f.chunks(t) {
		working += strings.Count(chunk.ArtifactUpdate.Artifact.Parts[0].Text, `"working"`)
	}
	if working != 1 {
		t.Fatalf("the line before the loss is in the transcript %d times", working)
	}
}

func TestFollowLinkLossFailsAfterOneLockDuration(t *testing.T) {
	f := newFixture(t, noResultStream)
	f.adapter.linkLost = 1 << 20
	shortLock(f, 2*time.Second)
	f.answer(t, credentialName)
	f.submit(t, f.host, nil)
	done := make(chan error, 1)
	started := time.Now()
	go func() { done <- f.receiver.claimOne(f.ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("a run whose plugin link stays lost kept following; logs:\n%s", f.logs.String())
	}
	if waited := time.Since(started); waited < 2*time.Second {
		t.Fatalf("run failed %s after the loss, before one lock duration", waited)
	}
	if state, reason := terminalReason(t, f, "run-a"); state != wire.TaskStateFailed || reason != "log stream failed" {
		t.Fatalf("terminal %s %q; want FAILED log stream failed", state, reason)
	}
	if _, _, follows, _ := f.adapter.counts(); follows < 2 {
		t.Fatalf("followed %d times; want retries before failing", follows)
	}
}

func TestOversizedDispatchIsLoggedByName(t *testing.T) {
	f := newFixture(t, resultStream)
	f.answer(t, credentialName)
	f.adapter.dispatchErr = adapter.ErrMessageTooLarge
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckRelease {
		t.Fatalf("status %v", status)
	}
	if logs := f.logs.String(); !strings.Contains(logs, "dispatch rejected: message too large") {
		t.Fatalf("oversized dispatch not logged by name: %q", logs)
	}
}

func TestDispatchErrorDetailIsNotPublished(t *testing.T) {
	const secret = "token=s3cr3t-dispatch-detail"
	f := newFixture(t, resultStream)
	f.answer(t, credentialName)
	f.adapter.dispatchErr = errors.New(secret)
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckRelease {
		t.Fatalf("status %v", status)
	}
	if failures := f.startFailures(t); len(failures) != 1 || failures[0].Class != "dispatch" {
		t.Fatalf("start failures %#v, want one dispatch start failure", failures)
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
			testActiveRun(t, f, "run-log-result", 1).finish(f.ctx, adapter.Handle{}, nil, agentResult{Result: "done"})
		}},
		{"stop on failure", func(t *testing.T, f fixture) {
			f.adapter.stopErr = errors.New(secret)
			testActiveRun(t, f, "run-log-failure", 1).stopAndFail(f.ctx, adapter.Handle{}, "timeout", nil)
		}},
		{"stop on denial", func(t *testing.T, f fixture) {
			f.adapter.stopErr = errors.New(secret)
			testActiveRun(t, f, "run-log-denial", 1).deny(f.ctx, adapter.Handle{}, nil)
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

// testdata/codex holds `codex exec --json` output captured from codex-cli 0.147.0 on 2026-10-20 (codex-fixtures-2026-10-20).
func codexFixture(t *testing.T, name string) []string {
	t.Helper()
	data, err := os.ReadFile("testdata/codex/" + name + ".jsonl")
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
}

func recogniseFixture(lines []string, output string) (agentResult, bool) {
	recognise, _ := resultRecognizer(output)
	for _, line := range lines {
		if result, ok := recognise(adapter.Line{Data: []byte(line)}); ok {
			return result, true
		}
	}
	return agentResult{}, false
}

func TestCodexResultFixtures(t *testing.T) {
	for _, test := range []struct {
		name    string
		result  string
		isError bool
		ok      bool
	}{
		{name: "success", result: "STEADY", ok: true},
		{name: "tool", result: "fixture input", ok: true},
		{name: "failure", result: "unexpected status 404 Not Found: The model `no-such-model-wherry` does not exist or you do not have access to it., url: https://api.openai.com/v1/responses, cf-ray: a4d97196785c1ef2-LHR, request id: req_a413a8f759d2447e898432c4ce0596d8", isError: true, ok: true},
		{name: "interrupted"},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, ok := recogniseFixture(codexFixture(t, test.name), "codex")
			if ok != test.ok || result.Result != test.result || result.IsError != test.isError {
				t.Fatalf("result = %#v, %t", result, ok)
			}
			if test.name == "success" {
				metadata := decodeObject(t, result.metadata)
				agent := metadata["agent_result"].(map[string]any)
				if len(agent) != 1 || agent["usage"] == nil {
					t.Fatalf("metadata = %v, want only usage", metadata)
				}
			}
		})
	}
}

func TestResultRecognizersRejectOtherFormat(t *testing.T) {
	if result, ok := recogniseFixture(codexFixture(t, "success"), "claude"); ok {
		t.Fatalf("Claude recogniser accepted Codex result %#v", result)
	}
	if result, ok := recogniseFixture(resultStream, "codex"); ok {
		t.Fatalf("Codex recogniser accepted Claude result %#v", result)
	}
}

func TestCodexRecognizerIgnoresMismatchedMembers(t *testing.T) {
	for _, test := range []struct {
		name    string
		lines   []string
		result  string
		isError bool
	}{
		{"string error", []string{`{"type":"turn.failed","error":"quota exceeded"}`}, "quota exceeded", true},
		{"array item", []string{`{"type":"item.completed","item":["x"]}`, `{"type":"item.completed","item":{"type":"agent_message","text":"ok"}}`, `{"type":"item.completed","item":"x"}`, `{"type":"turn.completed","item":"x","error":7}`}, "ok", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if result, ok := recogniseFixture(test.lines, "codex"); !ok || result.Result != test.result || result.IsError != test.isError {
				t.Fatalf("result = %#v, %t", result, ok)
			}
		})
	}
}

func TestResultRecognizerKeepsTheAgentMessage(t *testing.T) {
	recognise, keep := resultRecognizer("codex")
	var offset, message int64 = 0, -1
	for _, line := range codexFixture(t, "tool") {
		recognise(adapter.Line{Offset: offset, Data: []byte(line)})
		if strings.Contains(line, `"agent_message"`) {
			message = offset
		}
		if kept, ok := keep(); ok != (message >= 0) || ok && kept != message {
			t.Fatalf("at %d keep = %d, %t; want the last agent message at %d", offset, kept, ok, message)
		}
		offset += int64(len(line) + 1)
	}
	_, keep = resultRecognizer("claude")
	if _, ok := keep(); ok {
		t.Fatal("Claude recogniser keeps a line")
	}
}

func TestRunUsesCodexResultRecognizer(t *testing.T) {
	config := testConfig(testReceiver)
	config.Adapters[0].Output = "codex"
	stream := codexFixture(t, "success")
	f := newFixtureWith(t, stream, testTerms(), config, map[string]*fakeAdapter{"nomad": {stream: stream}})
	task := runToTask(t, f)
	if task.Status.State != wire.TaskStateCompleted || task.Artifacts[0].Parts[0].Text != "STEADY" {
		t.Fatalf("unexpected task: %#v", task)
	}
}

func TestCodexAdoptionPastTheAgentMessage(t *testing.T) {
	config := testConfig(testReceiver)
	config.Adapters[0].Output = "codex"
	success := codexFixture(t, "success")
	// A warning item after the final agent message, so a heartbeat can pass that message before turn.completed.
	before := append(slices.Clone(success[:4]), success[1])
	f := newFixtureWith(t, before, testTerms(), config, map[string]*fakeAdapter{"nomad": {stream: before, endDelay: 30 * time.Second}})
	f.answer(t, credentialName)
	record := f.submit(t, f.host, nil)
	var message int64
	for _, line := range success[:3] {
		message += int64(len(line) + 1)
	}
	ctx, stop := context.WithCancel(f.ctx)
	defer stop()
	first := f.executing(t, ctx, record, 1)
	for heartbeat := (*wire.RunStatus)(nil); heartbeat == nil; time.Sleep(20 * time.Millisecond) {
		for _, status := range f.eventStatuses(t) {
			if status.Wherry.Kind == "heartbeat" && *status.Wherry.LogOffset > 0 {
				heartbeat = &status
			}
		}
		if heartbeat != nil && *heartbeat.Wherry.LogOffset != message {
			t.Fatalf("heartbeat at %d, want the agent message at %d", *heartbeat.Wherry.LogOffset, message)
		}
		if f.ctx.Err() != nil {
			t.Fatalf("no heartbeat past offset 0; logs:\n%s", f.logs.String())
		}
	}
	stop()
	if status := <-first; status != kgo.AckRelease {
		t.Fatalf("attempt 1 status %v, want release", status)
	}
	f.adapter.mu.Lock()
	f.adapter.stream, f.adapter.job = append(before, success[4]), adapter.JobRunning
	f.adapter.mu.Unlock()
	if status := f.execute(t, record, 2); status != kgo.AckAccept {
		t.Fatalf("status %v, want the adopted run accepted", status)
	}
	if task := f.lastTask(t); task.Status.State != wire.TaskStateCompleted || task.Artifacts[0].Parts[0].Text != "STEADY" {
		t.Fatalf("adopted task %v result %q, want the agent message as the result", task.Status.State, task.Artifacts[0].Parts[0].Text)
	}
	f.adapter.mu.Lock()
	from := f.adapter.froms[len(f.adapter.froms)-1]
	f.adapter.mu.Unlock()
	if from != message {
		t.Fatalf("adopter followed from %d, want the agent message at %d", from, message)
	}
}

func TestCodexHeartbeatsPublishPastTheAgentMessage(t *testing.T) {
	config := testConfig(testReceiver)
	config.Adapters[0].Output = "codex"
	success := codexFixture(t, "success")
	stream := slices.Clone(success[:4])
	for range 20 {
		stream = append(stream, success[1])
	}
	f := newFixtureWith(t, stream, testTerms(), config, map[string]*fakeAdapter{"nomad": {stream: stream, endDelay: 30 * time.Second}})
	f.answer(t, credentialName)
	ctx, stop := context.WithCancel(f.ctx)
	defer stop()
	done := f.executing(t, ctx, f.submit(t, f.host, nil), 1)
	for deadline := time.Now().Add(10 * renewInterval); len(f.chunks(t)) < len(stream)-1; {
		if time.Now().After(deadline) {
			t.Fatalf("heartbeats published %d chunks, want all %d but the newest line", len(f.chunks(t)), len(stream)-1)
		}
		time.Sleep(20 * time.Millisecond)
	}
	stop()
	<-done
}

func replyOutput(text string) adapter.Output {
	return adapter.Output{Name: "reply.md", Size: int64(len(text)), Body: io.NopCloser(strings.NewReader(text))}
}

func TestFileOutputOutcomes(t *testing.T) {
	over := wire.MaxText + 1
	for _, test := range []struct {
		name     string
		stream   []string
		terminal adapter.Terminal
		outputs  []adapter.Output
		collect  error
		state    wire.TaskState
		result   string
	}{
		{name: "reply", terminal: adapter.Terminal{Started: true}, outputs: []adapter.Output{replyOutput("done " + testToken)}, state: wire.TaskStateCompleted, result: "done " + signed.RedactionMarker(testRedactionKey, credentialName)},
		{name: "reply as data", terminal: adapter.Terminal{Started: true}, outputs: []adapter.Output{{Name: "reply.md", Size: 4, Data: []byte("done")}}, state: wire.TaskStateCompleted, result: "done"},
		{name: "no reply", terminal: adapter.Terminal{Started: true}, state: wire.TaskStateFailed, result: "agent wrote no reply file"},
		{name: "not collected", terminal: adapter.Terminal{Started: true}, collect: errors.New("collect failed"), state: wire.TaskStateFailed, result: "outputs not collected"},
		{name: "exit with reply", terminal: adapter.Terminal{Started: true, ExitCode: 3}, outputs: []adapter.Output{replyOutput("partial")}, state: wire.TaskStateFailed, result: "partial"},
		{name: "exit", terminal: adapter.Terminal{Started: true, ExitCode: 3}, state: wire.TaskStateFailed, result: "agent exited 3"},
		{name: "signal", terminal: adapter.Terminal{Started: true, ExitCode: 137, Signal: "SIGKILL"}, state: wire.TaskStateFailed, result: "agent ended by SIGKILL"},
		{name: "over", terminal: adapter.Terminal{Started: true}, outputs: []adapter.Output{replyOutput(strings.Repeat("x", over))}, state: wire.TaskStateCompleted, result: "[redacted text over 65536 bytes]"},
		{name: "stdout terminals", stream: append(slices.Clone(resultStream), codexFixture(t, "success")...), terminal: adapter.Terminal{Started: true}, outputs: []adapter.Output{replyOutput("from file")}, state: wire.TaskStateCompleted, result: "from file"},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := testConfig(testReceiver)
			config.Adapters[0].Output = "file"
			stream := test.stream
			if stream == nil {
				stream = noResultStream
			}
			f := newFixtureWith(t, stream, testTerms(), config, map[string]*fakeAdapter{"nomad": {stream: stream, ended: &test.terminal, outputs: test.outputs, collectErr: test.collect}})
			f.answer(t, credentialName)
			f.execute(t, f.submit(t, f.host, nil), 1)
			task := f.lastTask(t)
			if part := task.Artifacts[0].Parts[0]; task.Status.State != test.state || part.Text != test.result {
				t.Fatalf("task %s with %q, want %s with %q", task.Status.State, part.Text, test.state, test.result)
			}
			if task.Artifacts[0].Metadata != nil {
				t.Fatalf("result metadata %s, want none", task.Artifacts[0].Metadata)
			}
		})
	}
}

func TestFileReplyIsNotAnOutput(t *testing.T) {
	config := testConfig(testReceiver)
	config.Adapters[0].Output = "file"
	terms := testTerms()
	terms.OutputLimit = 16
	f := newFixtureWith(t, noResultStream, terms, config, map[string]*fakeAdapter{"nomad": {stream: noResultStream, ended: &adapter.Terminal{Started: true}, outputs: []adapter.Output{
		{Name: "report.txt", Size: 16, Data: []byte(strings.Repeat("r", 16))}, replyOutput(strings.Repeat("a", 32)),
	}}})
	f.answer(t, credentialName)
	f.execute(t, f.submit(t, f.host, nil), 1)
	task := f.lastTask(t)
	if task.Status.State != wire.TaskStateCompleted || task.Artifacts[0].Parts[0].Text != strings.Repeat("a", 32) {
		t.Fatalf("task %s with %q, want the reply as the result", task.Status.State, task.Artifacts[0].Parts[0].Text)
	}
	listed := manifest(t, task)
	if len(listed.Outputs) != 1 || listed.Outputs[0].Name != "report.txt" || len(listed.NotReturned) != 0 {
		t.Fatalf("outputs %+v not returned %+v, want report.txt alone within the limit", listed.Outputs, listed.NotReturned)
	}
	for _, output := range f.outputChunks(t) {
		if output.ArtifactUpdate.Artifact.ArtifactID != "report.txt" {
			t.Fatalf("output %s published", output.ArtifactUpdate.Artifact.ArtifactID)
		}
	}
	f.adapter.mu.Lock()
	defer f.adapter.mu.Unlock()
	if !slices.Contains(f.adapter.limits.Stream, "reply.md") {
		t.Fatalf("collect limits %+v, want the reply streamed outside the output limits", f.adapter.limits)
	}
}

func TestFileReplyNameIsNotAnExpectedOutput(t *testing.T) {
	for _, storage := range []string{"", wire.StorageS3} {
		t.Run(cmp.Or(storage, "local"), func(t *testing.T) {
			config := testConfig(testReceiver)
			config.Adapters[0].Output = "file"
			s := newStore(t)
			f := newFixtureWith(t, noResultStream, s3Terms(s.endpoint()), config, map[string]*fakeAdapter{"nomad": {stream: noResultStream, ended: &adapter.Terminal{Started: true}, outputs: []adapter.Output{replyOutput("done")}}})
			done := f.executing(t, f.ctx, f.submit(t, f.host, func(task *wire.Submission) {
				task.Wherry.Outputs = []wire.ExpectedOutput{{Name: "reply.md", Required: true, Storage: storage}}
			}), 1)
			f.sealS3(t, validURLs(s.endpoint()))
			<-done
			task := f.lastTask(t)
			if task.Status.State != wire.TaskStateFailed || task.Status.Message.Parts[0].Text != "required output reply.md not returned: is the adapter's reply file" {
				t.Fatalf("task %s with %v, want the required output failed as the reply file", task.Status.State, task.Status.Message)
			}
			listed := manifest(t, task)
			if len(listed.NotReturned) != 1 || listed.NotReturned[0].Name != "reply.md" || listed.NotReturned[0].Reason != "is the adapter's reply file" {
				t.Fatalf("not returned %+v, want reply.md as the adapter's reply file", listed.NotReturned)
			}
			f.adapter.mu.Lock()
			defer f.adapter.mu.Unlock()
			if !slices.Equal(f.adapter.limits.Stream, []string{"reply.md"}) {
				t.Fatalf("collect streams %v, want reply.md once", f.adapter.limits.Stream)
			}
		})
	}
}

// podmanResultLine is the result record in testdata/podman-cri-stdout.log, a Nomad Podman task log capture,
// without its CRI framing.
func podmanResultLine(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("testdata/podman-cri-stdout.log")
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
	f.answer(t, credentialName)
	f.adapter.stream = []string{line}
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	artifact := f.lastTask(t).Artifacts[0]
	decoded := decodeObject(t, []byte(line))
	if artifact.Parts[0].Text != decoded["result"] {
		t.Fatalf("result part %#v, want the record's result", artifact.Parts[0])
	}
	want := map[string]any{}
	for _, name := range []string{"duration_ms", "duration_api_ms", "num_turns", "total_cost_usd", "usage", "modelUsage"} {
		want[name] = decoded[name]
	}
	got := decodeObject(t, artifact.Metadata)
	if len(got) != 1 || !reflect.DeepEqual(got["agent_result"], want) {
		t.Fatalf("artifact metadata %s, want only the result's resource-use fields", artifact.Metadata)
	}
}

func TestAgentResultOmittedWhenOversized(t *testing.T) {
	// The retained object is {"usage":{"pad":"<n bytes>"}}: n plus 20 bytes.
	line := func(pad int, result string) []byte {
		return []byte(`{"type":"result","result":"` + result + `","usage":{"pad":"` + strings.Repeat("x", pad) + `"}}`)
	}
	if result, _ := resultLine(line(wire.MaxAgentResult-20, strings.Repeat("r", 2*wire.MaxAgentResult))); result.metadata == nil {
		t.Fatal("agent_result at the bound, with a larger result member, was omitted")
	}
	result, ok := resultLine(line(wire.MaxAgentResult-19, "done"))
	if !ok || result.metadata != nil {
		t.Fatalf("agent_result one byte over the bound kept: ok=%t metadata %d bytes", ok, len(result.metadata))
	}
	f := newFixture(t, resultStream)
	run := testActiveRun(t, f, "run-oversized", 1)
	if status := run.finish(f.ctx, adapter.Handle{}, nil, result); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	if artifact := f.lastTask(t).Artifacts[0]; artifact.Metadata != nil || artifact.Parts[0].Text != "done" {
		t.Fatalf("result artifact %#v, want text done and no metadata", artifact)
	}
}

func TestAgentResultOmittedWhenNotObject(t *testing.T) {
	for _, data := range []string{`["result"]`, `"result"`, `null`, `42`, `{"type":"result","a":1,"a":2}`, "{\"type\":\"result\",\"a\":\"\xff\"}"} {
		if metadata := agentResultMetadata([]byte(data), "a"); metadata != nil {
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
	if status := run.finish(f.ctx, adapter.Handle{}, nil, result); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	if artifact := f.lastTask(t).Artifacts[0]; artifact.Metadata != nil {
		t.Fatalf("result artifact metadata %s, want none", artifact.Metadata)
	}
}

func TestReceiverRoutesToMatchedAdapter(t *testing.T) {
	m := newMultiFixture(t)
	m.answer(t, "cred-cloud")
	if status := m.execute(t, "run-lambda", accelerated, 1); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	labDispatches, _, _, _ := m.lab.counts()
	cloudDispatches, _, cloudFollows, cloudReleases := m.cloud.counts()
	if labDispatches != 0 || cloudDispatches != 1 || cloudFollows != 1 || cloudReleases != 1 {
		t.Fatalf("lab dispatches=%d; cloud dispatches=%d follows=%d releases=%d run=%#v", labDispatches, cloudDispatches, cloudFollows, cloudReleases, m.cloud.lastRun)
	}
	if status := m.execute(t, "run-exec", cpu, 1); status != kgo.AckAccept {
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
	m.answer(t, "cred-cloud")
	m.cloud.job = adapter.JobRunning
	if status := m.execute(t, "run-moved", cpu, 2); status != kgo.AckAccept {
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
	m.cloud.job = adapter.JobRunning
	if status := m.execute(t, "run-first", cpu, 1); status != kgo.AckAccept {
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
	if status := m.execute(t, "run-lab", cpu, 1); status != kgo.AckAccept {
		t.Fatalf("lab status %v", status)
	}
	if got := string(m.lab.lastRun.Credential); got != testToken {
		t.Fatal("lab adapter did not receive its own credential")
	}
	if status := m.execute(t, "run-edge", arm, 1); status != kgo.AckAccept {
		t.Fatalf("edge status %v", status)
	}
	if m.edge.lastRun.Credential != nil {
		t.Fatal("adapter without a configured credential received one")
	}
	if status := m.execute(t, "run-cloud", accelerated, 1); status != kgo.AckAccept {
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
	f.answer(t, credentialName)
	f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Turn = 2
		task.Message.MessageID, task.Message.Parts = "msg-2", []wire.Part{{Text: "continue"}}
	})
	completed := f.await(t, "run-a", func(task wire.Task) bool { return wire.Terminal(task.Status.State) })
	// The result's history is the turn's own; earlier turns are in their own results.
	if completed.Status.State != wire.TaskStateCompleted || len(completed.History) != 1 || completed.History[0].Parts[0].Text != "continue" {
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
	f.deleteReceiverACLs(t, kadm.NewACLs().AnyResource().Operations(kadm.OpAny))
}

// deleteReceiverACLs removes the receiver principal's ACLs that builder's resources and operations select.
func (f fixture) deleteReceiverACLs(t *testing.T, builder *kadm.ACLBuilder) {
	t.Helper()
	builder = builder.Allow(admin.ReceiverPrincipal(testReceiver)).AllowHosts("*").ResourcePatternType(kadm.ACLPatternAny)
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
	f.answer(t, credentialName)
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
	f.answer(t, credentialName)
	// A transactional ID denial must classify as denial: claiming with it would fail every run's first write.
	f.receiver.deny(kerr.TransactionalIDAuthorizationFailed, capWrite)
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
	f.receiver.deny(kerr.TopicAuthorizationFailed, capRead)
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
	if status := run.finish(f.ctx, adapter.Handle{}, nil, agentResult{Result: "done"}); status != kgo.AckRelease {
		t.Fatalf("status %v, want release: an untrusted result must not end the task", status)
	}
	if state, reason := terminalReason(t, f, "run-denied-result"); state != wire.TaskStateFailed || reason != "receiver denied" {
		t.Fatalf("terminal %s %q, want FAILED receiver denied", state, reason)
	}
}

func TestShareGroupDenialRetestedByClaim(t *testing.T) {
	f := newFixture(t, resultStream)
	f.answer(t, credentialName)
	f.receiver.deny(kerr.GroupAuthorizationFailed, capRead)
	f.submit(t, f.host, nil)
	ctx, stop := context.WithTimeout(f.ctx, 5*time.Second)
	defer stop()
	go f.receiver.claimLoop(ctx)
	f.await(t, "run-a", anyTask)
	if f.receiver.denied() {
		t.Fatal("a claim that worked left the share group denial in place")
	}
}

func TestAdmissionWritesDoNotFenceTheRunsHolder(t *testing.T) {
	f := newFixture(t, resultStream)
	holder := testActiveRun(t, f, "run-a", 1)
	if err := holder.commit(f.ctx, holder.statusRecords("claimed", taskStatus(wire.TaskStateWorking, ""), nil, nil)...); err != nil {
		t.Fatal(err)
	}
	// A duplicate of the held turn is rejected while the holder runs.
	duplicate := f.submit(t, f.host, func(task *wire.Submission) { task.Message.MessageID = "msg-duplicate" })
	f.rejected(t, duplicate, "duplicate submission")
	if err := holder.commit(f.ctx, holder.events(holder.status("heartbeat", taskStatus(wire.TaskStateWorking, ""), nil, nil))); err != nil {
		t.Fatalf("the rejection fenced the run's holder: %v", err)
	}
}

func TestTransactionalDenialClearsOnlyWhenTransactionsWork(t *testing.T) {
	f := newFixture(t, resultStream)
	f.deleteReceiverACLs(t, kadm.NewACLs().TransactionalIDs(admin.TransactionalPrefix(testPool)).Operations(kadm.OpAny))
	record := f.submit(t, f.host, func(task *wire.Submission) { task.Wherry.QueueDeadlineMS = time.Now().Add(-time.Second).UnixMilli() })
	if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckRelease {
		t.Fatalf("admitted=%v ack=%v, want release when the rejection cannot be written", admitted != nil, ack)
	}
	if !f.receiver.holds(capTransactional) {
		t.Fatalf("the rejection's transactional ID error did not enter denial; logs:\n%s", f.logs.String())
	}
	// Reads still work, but they say nothing about the transactional ID.
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	if !f.receiver.denied() {
		t.Fatal("a read-only refresh cleared a transactional ID denial")
	}
	if err := f.service.TenantAdd(f.ctx, testTenant, testPool); err != nil { // reconciles the ACLs
		t.Fatal(err)
	}
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	if f.receiver.denied() {
		t.Fatalf("denial held after the transactional ID worked again; logs:\n%s", f.logs.String())
	}
}

func TestAmbiguousCommitReadBack(t *testing.T) {
	for name, test := range map[string]struct {
		// applied is whether the broker applies the commit whose answer is lost.
		applied   bool
		ack       kgo.AckStatus
		releases  int
		terminals int
	}{
		"committed":     {true, kgo.AckAccept, 1, 1},
		"not committed": {false, kgo.AckRelease, 0, 0},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			run := testActiveRun(t, f, "run-ambiguous", 1)
			previous := commitTransaction
			t.Cleanup(func() { commitTransaction = previous })
			commitTransaction = func(ctx context.Context, producer *kgo.Client) error {
				try := kgo.TryAbort
				if test.applied {
					try = kgo.TryCommit
				}
				if err := producer.EndTransaction(ctx, try); err != nil {
					return err
				}
				return kerr.RequestTimedOut
			}
			if status := run.finish(f.ctx, adapter.Handle{}, nil, agentResult{Result: "done"}); status != test.ack {
				t.Fatalf("status %v, want %v; logs:\n%s", status, test.ack, f.logs.String())
			}
			if _, _, _, releases := f.adapter.counts(); releases != test.releases {
				t.Fatalf("releases %d, want %d", releases, test.releases)
			}
			terminals := 0
			for _, status := range f.eventStatuses(t) {
				if status.Wherry.Kind == "terminal" {
					terminals++
				}
			}
			if terminals != test.terminals {
				t.Fatalf("terminals on the pool's events %d, want %d", terminals, test.terminals)
			}
		})
	}
}

func TestAdoptionTrustsRotatedPeerKey(t *testing.T) {
	f := newFixture(t, resultStream)
	peerSigning := f.addPeer(t, "receiver-peer", testPool)
	// The peer rotates after the receiver's last refresh.
	_, next, _ := ed25519.GenerateKey(rand.Reader)
	f.produce(t, admin.ReceiverTopic("receiver-peer"), "receiver-peer", must(t)(signed.RotateKey("receiver-peer", peerSigning, next)))
	task := testSubmission("run-peer")
	hash := strings.Repeat("a", 64)
	f.produce(t, admin.PoolEventsTopic(testPool), "run-peer", f.heartbeat(t, task, hash, "receiver-peer", next, 42, nil))
	if got, err := f.receiver.lastOffset(f.ctx, task, hash); err != nil || got != 42 {
		t.Fatalf("heartbeat under the peer's rotated key: offset %d err %v, want 42", got, err)
	}
}

func TestContinuationAfterHostKeyRotation(t *testing.T) {
	f := newFixture(t, resultStream)
	ctx, stop := context.WithCancel(f.ctx)
	defer stop()
	f.run(t, ctx)
	f.turn(t, "run-a", 1)
	if interrupted := f.await(t, "run-a", anyTask); interrupted.Status.State != wire.TaskStateAuthRequired {
		t.Fatalf("first turn %#v, want AUTH_REQUIRED", interrupted)
	}
	_, rotated, _ := ed25519.GenerateKey(rand.Reader)
	owner := signed.HostOwner(testTenant, testHost)
	f.produce(t, admin.HostTopic(testTenant, testHost), "", must(t)(signed.RotateKey(owner, f.host, rotated)))
	f.answer(t, credentialName)
	f.submit(t, rotated, func(task *wire.Submission) {
		task.Wherry.Turn = 2
		task.Message.MessageID, task.Message.Parts = "msg-2", []wire.Part{{Text: "continue"}}
	})
	completed := f.await(t, "run-a", func(task wire.Task) bool { return wire.Terminal(task.Status.State) })
	f.adapter.mu.Lock()
	prompt := string(f.adapter.lastRun.Prompt)
	f.adapter.mu.Unlock()
	if completed.Status.State != wire.TaskStateCompleted || prompt != resumedHeader+"\n\nhi\n\ncontinue" {
		t.Fatalf("continuation after rotation %#v, prompt %q; want turn 1, signed before the rotation, in the prompt", completed, prompt)
	}
}

func TestBindTurnsPrefersTheTurnAMemberRan(t *testing.T) {
	ran, forged := testSubmission("run-a"), testSubmission("run-a")
	forged.Message.Parts = []wire.Part{{Text: "forged"}}
	all := []runSubmission{{record: forged, hash: strings.Repeat("b", 64), current: true}, {record: ran, hash: strings.Repeat("a", 64), current: true}}
	events := poolEvents{statuses: map[string][]wire.RunStatus{"run-a": {{Wherry: wire.StatusBinding{ReceiverBinding: wire.ReceiverBinding{Run: "run-a", Turn: 1}, TaskHash: strings.Repeat("a", 64)}}}}}
	if bound := bindTurns(all, ran, events); bound[1].hash != strings.Repeat("a", 64) {
		t.Fatalf("turn 1 bound to %s; want the submission a pool member's status names", bound[1].hash)
	}
	if bound := bindTurns(all, ran, poolEvents{}); bound[1].hash != strings.Repeat("b", 64) {
		t.Fatal("without statuses the first submission of a turn should stand")
	}
	// Signed only by a retired key and run by no member: it does not count, even first.
	all[0].current = false
	if bound := bindTurns(all, ran, poolEvents{}); bound[1].hash != strings.Repeat("a", 64) {
		t.Fatalf("turn 1 bound to %s; want the current-key submission over a retired-key one no member ran", bound[1].hash)
	}
}

func TestRetiredKeyForgeryDoesNotBlockTheRealTurn(t *testing.T) {
	f := newFixture(t, resultStream)
	first := f.turn(t, "run-a", 1)
	f.publishStatus(t, first, testReceiver, f.receiver.signing, "interrupted", wire.TaskStateAuthRequired, nil)
	retired := f.host
	_, current, _ := ed25519.GenerateKey(rand.Reader)
	f.produce(t, admin.HostTopic(testTenant, testHost), "", must(t)(signed.RotateKey(signed.HostOwner(testTenant, testHost), retired, current)))
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	// A leaked retired key forges turn 2 before the host writes it.
	forged := f.submit(t, retired, func(task *wire.Submission) {
		task.Wherry.Turn = 2
		task.Message.MessageID, task.Message.Parts = "msg-forged", []wire.Part{{Text: "forged"}}
	})
	genuine := f.submit(t, current, func(task *wire.Submission) {
		task.Wherry.Turn = 2
		task.Message.MessageID, task.Message.Parts = "msg-2", []wire.Part{{Text: "continue"}}
	})
	admitted := f.admitted(t, genuine)
	if len(admitted.history) != 2 || admitted.history[1].Parts[0].Text != "continue" {
		t.Fatalf("history %#v, want the real turn 2", admitted.history)
	}
	if admitted, ack := f.receiver.admit(f.ctx, forged); admitted != nil || ack != kgo.AckReject {
		t.Fatalf("the retired-key forgery admitted=%v ack=%v, want a silent rejection", admitted != nil, ack)
	}
}

func (f fixture) startFailures(t *testing.T) []wire.StartFailure {
	t.Helper()
	var failures []wire.StartFailure
	for _, record := range f.records(t, admin.PoolEventsTopic(testPool)) {
		var failure wire.StartFailure
		if wire.ParseRecord(record.Value, f.keys(), &failure) == nil {
			failures = append(failures, failure)
		}
	}
	return failures
}

func (f fixture) kinds(t *testing.T) []string {
	t.Helper()
	var kinds []string
	for _, status := range f.eventStatuses(t) {
		kinds = append(kinds, status.Wherry.Kind)
	}
	return kinds
}

func TestAmbiguousDispatchAdoptsTheExistingJob(t *testing.T) {
	t.Run("lookup finds it", func(t *testing.T) {
		f := newFixture(t, resultStream)
		f.answer(t, credentialName)
		f.adapter.dispatchErr = errors.New("register response lost")
		f.adapter.job = adapter.JobRunning
		if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckAccept {
			t.Fatalf("status %v, want the existing job followed to its result", status)
		}
		f.adapter.mu.Lock()
		defer f.adapter.mu.Unlock()
		if f.adapter.dispatches != 1 || f.adapter.follows != 1 || f.adapter.purges != 0 {
			t.Fatalf("dispatches=%d follows=%d purges=%d", f.adapter.dispatches, f.adapter.follows, f.adapter.purges)
		}
	})
	t.Run("redelivery finds it", func(t *testing.T) {
		f := newFixture(t, resultStream)
		f.answer(t, credentialName)
		f.adapter.dispatchErr = errors.New("register response lost")
		f.adapter.lookupErr = errors.New("lookup failed")
		record := f.submit(t, f.host, nil)
		if status := f.execute(t, record, 1); status != kgo.AckRelease {
			t.Fatalf("unresolved dispatch status %v, want release", status)
		}
		f.adapter.mu.Lock()
		lookups, purges, releases := f.adapter.lookups, f.adapter.purges, f.adapter.releases
		f.adapter.lookupErr, f.adapter.job = nil, adapter.JobRunning
		f.adapter.mu.Unlock()
		if lookups != 3 || purges != 0 || releases != 0 || len(f.startFailures(t)) != 0 {
			t.Fatalf("lookups=%d purges=%d releases=%d start failures=%d; want three lookups and nothing removed", lookups, purges, releases, len(f.startFailures(t)))
		}
		if status := f.execute(t, record, 2); status != kgo.AckAccept {
			t.Fatalf("redelivery status %v, want the job adopted", status)
		}
		if dispatches, _, follows, _ := f.adapter.counts(); dispatches != 1 || follows != 1 {
			t.Fatalf("dispatches=%d follows=%d, want the one job adopted", dispatches, follows)
		}
	})
}

func TestReturnTaskAtTheBudgetFailsWithoutRemoving(t *testing.T) {
	f := newFixture(t, resultStream)
	run := testActiveRun(t, f, "run-a", int64(testTerms().DeliveryLimit)-1)
	if status := run.returnTask(f.ctx, nil); status != kgo.AckReject {
		t.Fatalf("status %v, want reject", status)
	}
	if state, reason := terminalReason(t, f, "run-a"); state != wire.TaskStateFailed || reason != "delivery budget exhausted" {
		t.Fatalf("terminal %s %q", state, reason)
	}
	f.adapter.mu.Lock()
	calls := f.adapter.calls
	f.adapter.mu.Unlock()
	if len(calls) != 0 {
		t.Fatalf("calls %v, want the job and material left in place", calls)
	}
	run.attempt--
	if status := run.returnTask(f.ctx, nil); status != kgo.AckRelease {
		t.Fatalf("status %v below the budget, want release", status)
	}
}

func TestMismatchedJobIsPurgedNotAdopted(t *testing.T) {
	for name, mismatch := range map[string]func(*fakeAdapter){
		"adapter": func(adapter *fakeAdapter) { adapter.jobAdapter = "renamed" },
		"agent":   func(adapter *fakeAdapter) { adapter.jobAgent = "codex" },
	} {
		t.Run(name, func(t *testing.T) {
			m := newMultiFixture(t)
			m.cloud.job = adapter.JobRunning
			mismatch(m.cloud)
			if status := m.execute(t, "run-mismatch", cpu, 2); status != kgo.AckAccept {
				t.Fatalf("status %v", status)
			}
			m.cloud.mu.Lock()
			stops, purges, follows, adapter, agent := m.cloud.stops, m.cloud.purges, m.cloud.follows, cmp.Or(m.cloud.jobAdapter, "cloud"), cmp.Or(m.cloud.jobAgent, "claude")
			m.cloud.mu.Unlock()
			if stops != 1 || purges != 1 || follows != 0 {
				t.Fatalf("mismatched job: stops=%d purges=%d follows=%d, want stopped and purged, not followed", stops, purges, follows)
			}
			if dispatches, _, follows, _ := m.lab.counts(); dispatches != 1 || follows != 1 {
				t.Fatalf("matched adapter dispatches=%d follows=%d, want a fresh dispatch", dispatches, follows)
			}
			if want := fmt.Sprintf("run run-mismatch: purged a job from adapter %q agent %q", adapter, agent); !strings.Contains(m.logs.String(), want) {
				t.Fatalf("logs lack %q:\n%s", want, m.logs.String())
			}
		})
	}
}

func TestNeverStartedJobIsAPlatformStartFailure(t *testing.T) {
	f := newFixture(t, resultStream)
	f.answer(t, credentialName)
	f.adapter.job = adapter.JobNeverStarted
	if status := f.execute(t, f.submit(t, f.host, nil), 2); status != kgo.AckRelease {
		t.Fatalf("status %v, want release", status)
	}
	if failures := f.startFailures(t); len(failures) != 1 || failures[0].Class != "platform" || failures[0].Adapter != "nomad" {
		t.Fatalf("start failures %#v, want one platform start failure", failures)
	}
	if kinds := f.kinds(t); slices.Contains(kinds, "started") {
		t.Fatalf("statuses %v, want no started status for a job that never started", kinds)
	}
	if dispatches, _, follows, _ := f.adapter.counts(); dispatches != 0 || follows != 0 {
		t.Fatalf("dispatches=%d follows=%d, want neither", dispatches, follows)
	}
}

func TestEveryStartFailurePurgesBeforeRelease(t *testing.T) {
	for _, test := range []struct {
		name    string
		attempt int64
		class   string
		setup   func(*testing.T, fixture)
	}{
		{"nothing created", 1, "dispatch", func(t *testing.T, f fixture) { f.adapter.outcome = adapter.OutcomeNothingCreated }},
		{"unknown and absent", 1, "dispatch", func(t *testing.T, f fixture) { f.adapter.dispatchErr = errors.New("register failed") }},
		{"unknown and never started", 1, "platform", func(t *testing.T, f fixture) {
			f.adapter.dispatchErr, f.adapter.job = errors.New("wait failed"), adapter.JobNeverStarted
		}},
		{"adopted never started", 2, "platform", func(t *testing.T, f fixture) { f.adapter.job = adapter.JobNeverStarted }},
		{"followed never started", 1, "platform", func(t *testing.T, f fixture) { f.adapter.stream, f.adapter.neverStarted = nil, true }},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			test.setup(t, f)
			f.answer(t, credentialName)
			if status := f.execute(t, f.submit(t, f.host, nil), test.attempt); status != kgo.AckRelease {
				t.Fatalf("status %v, want release; logs:\n%s", status, f.logs.String())
			}
			if failures := f.startFailures(t); len(failures) != 1 || failures[0].Class != test.class {
				t.Fatalf("start failures %#v, want one %s", failures, test.class)
			}
			f.adapter.mu.Lock()
			calls := f.adapter.calls
			f.adapter.mu.Unlock()
			if !slices.Equal(calls, []string{"purge", "release"}) {
				t.Fatalf("calls %v, want purge then release", calls)
			}
		})
	}
}

func TestPurgeRetriedUntilItSucceeds(t *testing.T) {
	f := newFixture(t, resultStream)
	f.answer(t, credentialName)
	f.adapter.outcome, f.adapter.purgeFailures = adapter.OutcomeNothingCreated, 2
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckRelease {
		t.Fatalf("status %v, want release", status)
	}
	f.adapter.mu.Lock()
	defer f.adapter.mu.Unlock()
	if f.adapter.purges != 3 || f.adapter.releases != 1 {
		t.Fatalf("purges=%d releases=%d, want 3 and 1", f.adapter.purges, f.adapter.releases)
	}
}

func TestPurgeFailingPastTheDeadlineFails(t *testing.T) {
	previous := purgeDeadline
	purgeDeadline = time.Second
	t.Cleanup(func() { purgeDeadline = previous })
	f := newFixture(t, resultStream)
	f.answer(t, credentialName)
	f.adapter.outcome, f.adapter.purgeErr = adapter.OutcomeNothingCreated, errors.New("deregister denied")
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckReject {
		t.Fatalf("status %v, want reject", status)
	}
	if state, reason := terminalReason(t, f, "run-a"); state != wire.TaskStateFailed || reason != "start failed and the job could not be removed" {
		t.Fatalf("terminal %s %q", state, reason)
	}
	f.adapter.mu.Lock()
	defer f.adapter.mu.Unlock()
	if f.adapter.purges < 3 {
		t.Fatalf("purges %d, want retries every renew interval", f.adapter.purges)
	}
}

func TestStartFailureAtTheDeliveryBudgetFails(t *testing.T) {
	limit := int64(testTerms().DeliveryLimit)
	for _, test := range []struct {
		attempt int64
		ack     kgo.AckStatus
	}{
		{limit - 2, kgo.AckRelease},
		{limit - 1, kgo.AckReject},
	} {
		t.Run(fmt.Sprint(test.attempt), func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.answer(t, credentialName)
			f.adapter.outcome = adapter.OutcomeNothingCreated
			if status := f.execute(t, f.submit(t, f.host, nil), test.attempt); status != test.ack {
				t.Fatalf("status %v, want %v", status, test.ack)
			}
			if len(f.startFailures(t)) != 1 {
				t.Fatal("no start failure")
			}
			if test.ack != kgo.AckReject {
				return
			}
			if state, reason := terminalReason(t, f, "run-a"); state != wire.TaskStateFailed || reason != "delivery budget exhausted" {
				t.Fatalf("terminal %s %q", state, reason)
			}
		})
	}
}

func TestOversizedHandleIsRejected(t *testing.T) {
	for _, test := range []struct {
		size int
		ack  kgo.AckStatus
	}{
		{maxHandle, kgo.AckAccept},
		{maxHandle + 1, kgo.AckRelease},
	} {
		t.Run(fmt.Sprint(test.size), func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.answer(t, credentialName)
			f.adapter.handleSize = test.size
			if status := f.execute(t, f.submit(t, f.host, nil), 1); status != test.ack {
				t.Fatalf("status %v, want %v", status, test.ack)
			}
			if test.ack == kgo.AckAccept {
				return
			}
			if _, _, follows, releases := f.adapter.counts(); follows != 0 || releases != 0 || slices.Contains(f.kinds(t), "started") {
				t.Fatalf("follows=%d releases=%d statuses %v, want the handle refused before started", follows, releases, f.kinds(t))
			}
			f.adapter.mu.Lock()
			defer f.adapter.mu.Unlock()
			if f.adapter.stops != 1 {
				t.Fatalf("stops %d, want the job behind the refused handle stopped", f.adapter.stops)
			}
		})
	}
}

func TestReleasesAtTheBudgetFail(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*testing.T, fixture)
		// readable is false when the fault also blocks reading the feed back.
		readable bool
	}{
		{"read seals", func(t *testing.T, f fixture) {
			if _, err := kadm.NewClient(f.service.Admin).DeleteTopics(f.ctx, admin.HostTopic(testTenant, testHost)); err != nil {
				t.Fatal(err)
			}
		}, true},
		{"read events", func(t *testing.T, f fixture) {
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
		}, false},
		{"adoption lookup", func(t *testing.T, f fixture) { f.adapter.lookupErr = errors.New("lookup failed") }, true},
		{"mismatch stop", func(t *testing.T, f fixture) {
			f.adapter.job, f.adapter.jobAgent, f.adapter.stopErr = adapter.JobRunning, "codex", errors.New("stop failed")
		}, true},
		{"mismatch purge", func(t *testing.T, f fixture) {
			f.adapter.job, f.adapter.jobAgent, f.adapter.purgeErr = adapter.JobRunning, "codex", errors.New("purge failed")
		}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.answer(t, credentialName)
			record := f.submit(t, f.host, nil)
			admitted := f.admitted(t, record)
			test.setup(t, f)
			if status := f.receiver.execute(f.ctx, f.service.Admin, record, admitted, int64(testTerms().DeliveryLimit)-1); status != kgo.AckReject {
				t.Fatalf("status %v, want reject; logs:\n%s", status, f.logs.String())
			}
			if !test.readable {
				return
			}
			if state, reason := terminalReason(t, f, "run-a"); state != wire.TaskStateFailed || reason != "delivery budget exhausted" {
				t.Fatalf("terminal %s %q", state, reason)
			}
		})
	}
}

func TestShutdownAtTheBudgetStillReleases(t *testing.T) {
	f := newFixture(t, noResultStream)
	f.answer(t, credentialName)
	f.adapter.endDelay = 30 * time.Second
	record := f.submit(t, f.host, nil)
	admitted := f.admitted(t, record)
	ctx, shutdown := context.WithCancel(f.ctx)
	defer shutdown()
	done := make(chan kgo.AckStatus, 1)
	go func() {
		done <- f.receiver.execute(ctx, f.service.Admin, record, admitted, int64(testTerms().DeliveryLimit)-1)
	}()
	for {
		if _, _, follows, _ := f.adapter.counts(); follows > 0 {
			break
		}
		if f.ctx.Err() != nil {
			t.Fatal("the run never followed its job")
		}
		time.Sleep(20 * time.Millisecond)
	}
	shutdown()
	if status := <-done; status != kgo.AckRelease {
		t.Fatalf("status %v, want release: an adopter continues the run", status)
	}
	if kinds := f.kinds(t); slices.Contains(kinds, "terminal") {
		t.Fatalf("statuses %v, want no terminal at shutdown", kinds)
	}
}

func TestJobOfAnotherAdapterIsLeftToIt(t *testing.T) {
	m := newMultiFixture(t)
	// lab and cloud share one backend: both find the job, labelled with cloud.
	m.lab.job, m.lab.jobAdapter = adapter.JobRunning, "cloud"
	m.cloud.job = adapter.JobRunning
	if status := m.execute(t, "run-shared", cpu, 2); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	m.lab.mu.Lock()
	stops, purges, labFollows := m.lab.stops, m.lab.purges, m.lab.follows
	m.lab.mu.Unlock()
	if stops != 0 || purges != 0 || labFollows != 0 {
		t.Fatalf("lab stops=%d purges=%d follows=%d, want the job left to cloud", stops, purges, labFollows)
	}
	if dispatches, _, follows, _ := m.cloud.counts(); dispatches != 0 || follows != 1 {
		t.Fatalf("cloud dispatches=%d follows=%d, want the job adopted by cloud", dispatches, follows)
	}
}

func TestNothingCreatedSkipsReconcile(t *testing.T) {
	f := newFixture(t, resultStream)
	f.answer(t, credentialName)
	f.adapter.outcome = adapter.OutcomeNothingCreated
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckRelease {
		t.Fatalf("status %v, want release", status)
	}
	if failures := f.startFailures(t); len(failures) != 1 || failures[0].Class != "dispatch" {
		t.Fatalf("start failures %#v, want one dispatch", failures)
	}
	if _, lookups, _, _ := f.adapter.counts(); lookups != 0 {
		t.Fatalf("lookups %d, want none for an outcome the adapter knows", lookups)
	}
}
