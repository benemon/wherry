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
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
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
	return &activeRun{receiver: f.receiver, producer: producer, task: task, taskHash: wire.TaskHash(record.Value), history: []wire.Message{task.Message}, attempt: attempt, profile: f.receiver.profiles[0], redactionKey: testRedactionKey}
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
	if echo.TaskHash != wire.TaskHash(record.Value) || echo.Pool != testPool || echo.Tenant != testTenant || !reflect.DeepEqual(echo.Placement, f.parse(t, record).Wherry.Placement) {
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

func TestFailedStatusSharesItsResultsTransaction(t *testing.T) {
	f := newFixture(t, resultStream)
	f.execute(t, f.submit(t, f.host, nil), 1)
	records := f.records(t, admin.TenantTopic(testTenant))
	// Echo and claimed, the seal request, then the FAILED result and terminal status.
	if len(records) != 5 {
		t.Fatalf("feed records %d, want 5", len(records))
	}
	var result wire.Result
	var terminal wire.RunStatus
	if wire.ParseRecord(records[3].Value, f.keys(), &result) != nil || wire.ParseRecord(records[4].Value, f.keys(), &terminal) != nil || terminal.Wherry.Kind != "terminal" {
		t.Fatal("the result is not followed by the terminal status")
	}
	if !oneTransaction(records[3:]) || oneTransaction(records[2:4]) {
		t.Fatal("the FAILED result and terminal status are not one transaction of their own")
	}
}

func TestSealWaitExpiryFailsWithoutCredentialValue(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.submit(t, f.host, nil)
	before := time.Now().Add(-time.Second)
	if status := f.execute(t, record, 1); status != kgo.AckReject {
		t.Fatalf("status %v, want reject", status)
	}
	f.plugin.mu.Lock()
	dispatches, releases := f.plugin.dispatches, f.plugin.releases
	f.plugin.mu.Unlock()
	if dispatches != 0 || releases != 1 {
		t.Fatalf("dispatches=%d releases=%d, want 0 and 1", dispatches, releases)
	}
	statuses := f.feedStatuses(t)
	if len(statuses) != 2 || len(f.eventStatuses(t)) != 2 {
		t.Fatalf("feed statuses=%d, want claimed and terminal on the feed and the pool's events", len(statuses))
	}
	terminal := statuses[1]
	got := terminal.StatusUpdate.Status
	if terminal.Wherry.Kind != "terminal" || got.State != wire.TaskStateFailed {
		t.Fatalf("terminal status %#v, want FAILED", terminal)
	}
	want := "credential " + credentialName + " was not answered within 5m; add it to credentials in the host's wherry configuration, keep wherry watch or wherry mcp running, and submit again"
	if got.Message == nil || got.Message.Role != wire.RoleAgent || len(got.Message.Parts) != 1 || got.Message.Parts[0].Text != want || strings.Contains(got.Message.Parts[0].Text, testToken) {
		t.Fatalf("status message %#v, want credential name and no value", got.Message)
	}
	if at, err := time.Parse(time.RFC3339, got.Timestamp); err != nil || at.Before(before) || at.After(time.Now()) {
		t.Fatalf("status timestamp %q is not the time of the status: %v", got.Timestamp, err)
	}
	if task := f.lastTask(t); task.Status.State != wire.TaskStateFailed || task.Status.Message == nil || task.Status.Message.Parts[0].Text != want {
		t.Fatalf("result %#v, want FAILED beside the terminal status", task)
	}
}

// lab and cloud both launch claude; a task routed to cloud names cloud in its start failure.
func TestStartFailureNamesTheRoutedProfile(t *testing.T) {
	m := newMultiFixture(t)
	m.cloud.outcome = pluginsdk.OutcomeNothingCreated
	if status := m.execute(t, "run-a", accelerated, 1); status != kgo.AckRelease {
		t.Fatalf("status %v, want release", status)
	}
	if failures := m.startFailures(t); len(failures) != 1 || failures[0].Class != "dispatch" || failures[0].Profile != "cloud" {
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
			"receiver_id": testReceiver, "pool": testPool, "profile": "nomad",
			"labels": map[string]any{"zone": "dc1", "example.com/gpu": "none"},
			"job":    "agent-run-a",
		}
		if got := decodeObject(t, f.lastTask(t).Metadata); !reflect.DeepEqual(got, want) {
			t.Fatalf("task metadata %v, want %v", got, want)
		}
	})
	// The adopted job came from an earlier attempt, whose offer this attempt does not know.
	t.Run("adopted", func(t *testing.T) {
		m := newMultiFixture(t)
		m.cloud.job = pluginsdk.JobRunning
		if status := m.execute(t, "run-moved", cpu, 2); status != kgo.AckAccept {
			t.Fatalf("status %v", status)
		}
		want := map[string]any{"receiver_id": testReceiver, "pool": testPool, "profile": "cloud", "job": "agent-run-moved"}
		if got := decodeObject(t, m.lastTask(t).Metadata); !reflect.DeepEqual(got, want) {
			t.Fatalf("task metadata %v, want %v", got, want)
		}
	})
}

func TestTranscriptArtifactChunks(t *testing.T) {
	oversized := `{"type":"assistant","message":"` + strings.Repeat("x", wire.MaxLine) + `"}`
	for name, test := range map[string]struct {
		stream   []string
		progress bool
		pointer  int
		raw      []int
	}{
		"result after an oversized line": {[]string{`{"type":"system"}`, oversized, `{"type":"assistant"}`, resultStream[1]}, false, 1, nil},
		// The stream outlives a progress tick, which commits all but the newest line.
		"no result after a progress tick": {[]string{`{"type":"system"}`, `{"type":"assistant"}`}, true, -1, nil},
		"empty and non-UTF-8 lines":       {[]string{`{"type":"system"}`, "", "\xff\xfe", resultStream[1]}, false, -1, []int{1, 2}},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.answer(t, credentialName)
			f.plugin.stream = test.stream
			if test.progress {
				f.plugin.endDelay = 3 * renewInterval
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
					if part.URL != "unstored:oversize" || part.Text != "" || pointer.SHA256 != wire.TaskHash([]byte(line)) || pointer.Size != int64(len(line)) {
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
			if !test.progress {
				return
			}
			offsets := f.progress(t, testReceiver, "run-a")
			// Once both lines have arrived, a tick commits the first and records where the held second starts.
			if want := int64(len(test.stream[0]) + 1); len(offsets) < 2 || offsets[len(offsets)-2] != want {
				t.Fatalf("progress offsets %v, want the last at %d before the tombstone", offsets, want)
			}
		})
	}
}

func TestOversizedAgentOutputIsBounded(t *testing.T) {
	f := newFixture(t, resultStream)
	f.answer(t, credentialName)
	huge := strings.Repeat("x", 3<<19)
	f.plugin.stream = []string{`{"type":"assistant","message":"` + huge + `"}`, completedWith(strings.Repeat("x", 3<<17))}
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckAccept {
		t.Fatalf("status %v, want accept", status)
	}
	task := f.lastTask(t)
	if task.Status.State != wire.TaskStateCompleted || task.Artifacts[0].Parts[0].Text != "[redacted text over 65536 bytes]" {
		t.Fatalf("task ended %s with result %#v", task.Status.State, task.Artifacts[0].Parts[0])
	}
	if part := f.chunks(t)[0].ArtifactUpdate.Artifact.Parts[0]; part.URL != "unstored:oversize" || part.Text != "" {
		t.Fatalf("chunk part %#v, want an oversized pointer", part)
	}
}

// completedWith is the outcome file of a completed run that answers text.
func completedWith(text string) string {
	encoded, _ := json.Marshal(text) // a string always encodes
	return `{"state":"TASK_STATE_COMPLETED","message":{"role":"ROLE_AGENT","parts":[{"text":` + string(encoded) + `}]}}`
}

// Progress goes to the receiver's progress topic alone: after runs with progress, the pool's events an admission reads
// hold each run's claimed, started and terminal statuses and nothing else.
func TestProgressStaysOffTheStateTopics(t *testing.T) {
	f := newFixture(t, resultStream)
	f.answer(t, credentialName)
	// A second line moves the transcript, so a progress tick commits.
	f.plugin.stream = append([]string{`{"type":"system"}`}, noResultStream...)
	f.plugin.endDelay = 4 * renewInterval
	runs := []string{"run-a", "run-b", "run-c"}
	for _, run := range runs {
		f.execute(t, f.submit(t, f.host, func(task *wire.Submission) {
			task.Wherry.Run, task.Message.TaskID, task.Message.ContextID = run, run, run
		}), 1)
	}
	progress := f.progressByRun(t, testReceiver)
	for _, run := range runs {
		if offsets := progress[run]; len(offsets) < 2 || offsets[len(offsets)-1] != -1 || slices.Contains(offsets[:len(offsets)-1], -1) {
			t.Fatalf("%s progress offsets %v, want progress then a tombstone", run, offsets)
		}
	}
	events, err := f.receiver.readEvents(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"claimed", "started", "terminal"}
	for _, run := range runs {
		var kinds []string
		for _, status := range events.statuses[run] {
			kinds = append(kinds, status.Wherry.Kind)
		}
		if !slices.Equal(kinds, want) || len(events.others[run]) != 0 {
			t.Fatalf("%s pool events hold statuses %v and %d other records, want only %v", run, kinds, len(events.others[run]), want)
		}
	}
	if records := f.records(t, admin.PoolEventsTopic(testPool)); len(records) != len(runs)*len(want) {
		t.Fatalf("pool events hold %d records, want %d", len(records), len(runs)*len(want))
	}
}

// A progress record commits in the transaction of the chunks it covers: when the progress topic refuses it, those
// chunks are not committed either.
func TestProgressCommitsWithItsChunks(t *testing.T) {
	f := newFixture(t, resultStream)
	f.answer(t, credentialName)
	f.plugin.stream = append([]string{`{"type":"system"}`}, noResultStream...)
	f.plugin.endDelay = 4 * renewInterval
	f.deleteReceiverACLs(t, kadm.NewACLs().Topics(admin.ReceiverProgressTopic(testReceiver)).Operations(kadm.OpAny))
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckRelease {
		t.Fatalf("status %v, want release when progress cannot be written; logs:\n%s", status, f.logs.String())
	}
	if chunks := f.chunks(t); len(chunks) != 0 {
		t.Fatalf("%d chunks committed without their progress record", len(chunks))
	}
}

// A progress record commits only when the transcript has moved, so a silent job writes none, and each one still points
// at the start of a committed line an adopter resumes from. The terminal transaction writes the run's tombstone.
func TestProgressOnlyWhenTheTranscriptMoves(t *testing.T) {
	stream := []string{`{"type":"assistant","message":"one"}`, `{"type":"assistant","message":"two"}`, `{"type":"assistant","message":"three"}`, completedWith("answer")}
	f := newFixture(t, stream)
	f.answer(t, credentialName)
	f.plugin.pauses, f.plugin.endDelay = map[int]time.Duration{2: 6 * renewInterval}, 3*renewInterval
	record := f.submit(t, f.host, nil)
	if status := f.execute(t, record, 1); status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	starts := map[int64]bool{}
	for _, chunk := range f.chunks(t) {
		starts[chunk.Wherry.Seq] = true
	}
	offsets := f.progress(t, testReceiver, "run-a")
	if len(offsets) == 0 || offsets[len(offsets)-1] != -1 {
		t.Fatalf("progress offsets %v, want a tombstone last", offsets)
	}
	offsets = offsets[:len(offsets)-1]
	// Two intervals move the transcript, the one before the silence and the one after it; a third can split the
	// lines that end the silence. The silent intervals and those after the last line write none.
	first, last := int64(len(stream[0])+1), int64(len(stream[0])+len(stream[1])+len(stream[2])+3)
	if len(offsets) < 2 || len(offsets) > 3 || offsets[0] != first || offsets[len(offsets)-1] != last {
		t.Fatalf("progress offsets %v, want %d first, %d last, and one per interval with progress", offsets, first, last)
	}
	for i, offset := range offsets {
		if !starts[offset] || i > 0 && offset <= offsets[i-1] {
			t.Fatalf("progress offsets %v: %d is a repeat or no committed line's start", offsets, offset)
		}
	}
	if got, err := f.receiver.lastOffset(f.ctx, f.parse(t, record), wire.TaskHash(record.Value)); err != nil || got != last {
		t.Fatalf("resume offset %d err %v, want %d", got, err, last)
	}
	// The terminal counts the chunks the progress ticks committed with those it commits.
	statuses := f.feedStatuses(t)
	if terminal := statuses[len(statuses)-1]; terminal.Wherry.Kind != "terminal" || terminal.Wherry.TranscriptChunks != int64(len(f.chunks(t))) {
		t.Fatalf("last status %s counts %d chunks, want a terminal counting %d", terminal.Wherry.Kind, terminal.Wherry.TranscriptChunks, len(f.chunks(t)))
	}
}

func TestTaskResultDataPartWhenJSON(t *testing.T) {
	for _, result := range []string{`{"answer":42,"items":[1,2]}`, `[true,null]`, `42`} {
		t.Run(result, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.plugin.stream = []string{completedWith(result)}
			run := testActiveRun(t, f, "run-json", 1)
			if status := run.conclude(f.ctx, pluginsdk.Handle{}, nil, pluginsdk.Terminal{Started: true}); status != kgo.AckAccept {
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
			f.plugin.stream = []string{completedWith(result)}
			run := testActiveRun(t, f, "run-text", 1)
			if status := run.conclude(f.ctx, pluginsdk.Handle{}, nil, pluginsdk.Terminal{Started: true}); status != kgo.AckAccept {
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
	run.chunked = 2
	commits := 0
	commit := commitTransaction
	commitTransaction = func(ctx context.Context, producer *kgo.Client) error {
		commits++
		return commit(ctx, producer)
	}
	t.Cleanup(func() { commitTransaction = commit })
	run.conclude(f.ctx, pluginsdk.Handle{}, []pluginsdk.Line{{Offset: 0, Next: 5, Data: []byte("line")}}, pluginsdk.Terminal{Started: true})
	records := f.records(t, admin.TenantTopic(testTenant))
	if len(records) != 2 || len(f.chunks(t)) != 1 {
		t.Fatalf("feed records %d and chunks %d, want the result and the terminal, and the last chunk", len(records), len(f.chunks(t)))
	}
	var result wire.Result
	var terminal wire.RunStatus
	if wire.ParseRecord(records[0].Value, f.keys(), &result) != nil || wire.ParseRecord(records[1].Value, f.keys(), &terminal) != nil || terminal.Wherry.Kind != "terminal" {
		t.Fatal("the result is not followed by the terminal")
	}
	if commits != 1 {
		t.Fatalf("the turn's end took %d transactions, want 1", commits)
	}
	if terminal.Wherry.TranscriptChunks != 3 {
		t.Fatalf("terminal counts %d chunks, want the 2 committed and the last", terminal.Wherry.TranscriptChunks)
	}
	if result.Wherry.TaskHash != run.taskHash || result.Wherry.Tenant != testTenant {
		t.Fatalf("result binding %#v", result.Wherry)
	}
}

func TestReceiverReleasesAfterTerminal(t *testing.T) {
	t.Run("stop", func(t *testing.T) {
		f := newFixture(t, resultStream)
		run := testActiveRun(t, f, "run-release-stop", 1)
		if status := run.conclude(f.ctx, pluginsdk.Handle{}, nil, pluginsdk.Terminal{Started: true}); status != kgo.AckAccept {
			t.Fatalf("status %v", status)
		}
		f.plugin.mu.Lock()
		defer f.plugin.mu.Unlock()
		if f.plugin.stops != 1 || f.plugin.releases != 1 || f.plugin.releaseIDs[0] != "run-release-stop" {
			t.Fatalf("stops=%d releases=%v", f.plugin.stops, f.plugin.releaseIDs)
		}
	})
	t.Run("dispatch error with no job", func(t *testing.T) {
		f := newFixture(t, resultStream)
		f.answer(t, credentialName)
		f.plugin.dispatchErr = errors.New("dispatch failed")
		if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckRelease {
			t.Fatalf("status %v", status)
		}
		f.plugin.mu.Lock()
		defer f.plugin.mu.Unlock()
		if f.plugin.dispatches != 1 || f.plugin.lookups != 1 || f.plugin.releases != 1 {
			t.Fatalf("dispatches=%d lookups=%d releases=%d", f.plugin.dispatches, f.plugin.lookups, f.plugin.releases)
		}
	})
}

func TestEndOfJobStopsAllocationBeforeTerminal(t *testing.T) {
	f := newFixture(t, resultStream)
	run := testActiveRun(t, f, "run-result-stop", 1)
	f.plugin.stopCheck = func() error {
		if n := len(f.records(t, admin.TenantTopic(testTenant))) + len(f.records(t, admin.TenantTranscriptTopic(testTenant))); n != 0 {
			return fmt.Errorf("feed records visible before Stop: %d", n)
		}
		return nil
	}
	if status := run.conclude(f.ctx, pluginsdk.Handle{}, nil, pluginsdk.Terminal{Started: true}); status != kgo.AckAccept {
		t.Fatalf("conclude status %v, want accept", status)
	}
	f.plugin.mu.Lock()
	stops := f.plugin.stops
	f.plugin.mu.Unlock()
	if stops != 1 || len(f.results(t)) != 1 || len(f.eventStatuses(t)) != 1 {
		t.Fatalf("stops=%d results=%d events=%d", stops, len(f.results(t)), len(f.eventStatuses(t)))
	}
}

func TestStopFailureReleasesWithoutTerminal(t *testing.T) {
	pending := []pluginsdk.Line{{Offset: 0, Next: 8, Data: []byte("working")}}
	for _, test := range []struct {
		name string
		call func(*activeRun, context.Context, []pluginsdk.Line) kgo.AckStatus
	}{
		{"result", func(run *activeRun, ctx context.Context, pending []pluginsdk.Line) kgo.AckStatus {
			return run.conclude(ctx, pluginsdk.Handle{}, pending, pluginsdk.Terminal{Started: true})
		}},
		{"failure", func(run *activeRun, ctx context.Context, pending []pluginsdk.Line) kgo.AckStatus {
			return run.stopAndFail(ctx, pluginsdk.Handle{}, "timeout", pending)
		}},
		{"cancel", func(run *activeRun, ctx context.Context, pending []pluginsdk.Line) kgo.AckStatus {
			return run.stopAndCancel(ctx, pluginsdk.Handle{}, pending)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.plugin.stopErr = errors.New("stop failed")
			run := testActiveRun(t, f, "run-stop-failure-"+test.name, 1)
			if status := test.call(run, f.ctx, pending); status != kgo.AckRelease {
				t.Fatalf("status %v, want release", status)
			}
			if feed, events := len(f.records(t, admin.TenantTopic(testTenant)))+len(f.records(t, admin.TenantTranscriptTopic(testTenant))), len(f.records(t, admin.PoolEventsTopic(testPool))); feed != 0 || events != 0 {
				t.Fatalf("Stop failure published feed=%d events=%d", feed, events)
			}
		})
		t.Run(test.name+" at the budget", func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.plugin.stopErr = errors.New("stop failed")
			run := testActiveRun(t, f, "run-stop-budget-"+test.name, int64(testTerms().DeliveryLimit)-1)
			status := test.call(run, f.ctx, pending)
			if chunks := f.chunks(t); len(chunks) != 1 || chunks[0].ArtifactUpdate.Artifact.Parts[0].Text != "working" || !chunks[0].ArtifactUpdate.LastChunk {
				t.Fatalf("chunks %#v, want the pending line as the last chunk", chunks)
			}
			if test.name == "result" {
				// The job that would not stop is left to the sweep, its outcome file read alone.
				if task := f.lastTask(t); status != kgo.AckAccept || task.Status.State != wire.TaskStateCompleted || task.Artifacts[0].Parts[0].Text != "done" {
					t.Fatalf("status %v task %#v, want the run completed with its outcome", status, task.Status)
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

// holding commits task's attempt 1 status of kind as receiver with key, at offset when it is not nil.
func (f fixture) holding(t *testing.T, task *wire.Submission, hash, receiver string, key []byte, kind string, offset *int64) {
	t.Helper()
	status := f.receiver.status(task, hash, 1, kind, taskStatus(wire.TaskStateWorking, ""), offset)
	status.Wherry.Receiver = receiver
	f.produce(t, admin.PoolEventsTopic(testPool), task.Wherry.Run, must(t)(wire.SignRecord(status, key)))
}

// progressRecord encodes attempt 1's progress of a testTenant run at offset; mutate edits it first.
func progressRecord(t *testing.T, hash string, offset int64, mutate func(*wire.Progress)) []byte {
	t.Helper()
	progress := wire.Progress{Tenant: testTenant, TaskHash: hash, Attempt: 1, LogOffset: offset}
	if mutate != nil {
		mutate(&progress)
	}
	return must(t)(wire.EncodeProgress(progress))
}

// progressByRun is the offsets on receiver's progress topic by run, -1 for a tombstone.
func (f fixture) progressByRun(t *testing.T, receiver string) map[string][]int64 {
	t.Helper()
	offsets := map[string][]int64{}
	for _, record := range f.records(t, admin.ReceiverProgressTopic(receiver)) {
		run := string(record.Key)
		if record.Value == nil {
			offsets[run] = append(offsets[run], -1)
			continue
		}
		progress, err := wire.DecodeProgress(record.Value)
		if err != nil {
			t.Fatal(err)
		}
		offsets[run] = append(offsets[run], progress.LogOffset)
	}
	return offsets
}

// progress is the offsets on receiver's progress topic, which holds records of run alone.
func (f fixture) progress(t *testing.T, receiver, run string) []int64 {
	t.Helper()
	offsets := f.progressByRun(t, receiver)
	for key := range offsets {
		if key != run {
			t.Fatalf("progress record keyed %q, want %q", key, run)
		}
	}
	return offsets[run]
}

func TestAdoptionResumesFromLastOffset(t *testing.T) {
	f := newFixture(t, resultStream)
	task := testSubmission("run-adopt")
	hash := strings.Repeat("a", 64)
	started := int64(5)
	f.holding(t, task, hash, testReceiver, f.receiver.signing, "started", &started)
	for _, offset := range []int64{10, 42} {
		f.produce(t, admin.ReceiverProgressTopic(testReceiver), "run-adopt", progressRecord(t, hash, offset, nil))
	}
	// A fenced holder's aborted progress must not move the resume point.
	aborted, err := f.receiver.newProducer("run-adopt")
	if err != nil {
		t.Fatal(err)
	}
	defer aborted.Close()
	if err := aborted.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := aborted.ProduceSync(f.ctx, &kgo.Record{Topic: admin.ReceiverProgressTopic(testReceiver), Key: []byte("run-adopt"), Value: progressRecord(t, hash, 99, nil)}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := aborted.EndTransaction(f.ctx, kgo.TryAbort); err != nil {
		t.Fatal(err)
	}
	if got, err := f.receiver.lastOffset(f.ctx, task, hash); err != nil || got != 42 {
		t.Fatalf("resume offset %d err %v, want 42", got, err)
	}
}

// An adopter follows from the holder's last progress, publishes each line from there once, through its own progress,
// and ends the run from the outcome file.
func TestAdoptedRunEndsFromTheOutcomeFile(t *testing.T) {
	stream := []string{`{"type":"assistant","message":"one"}`, `{"type":"assistant","message":"two"}`, `{"type":"assistant","message":"three"}`, completedWith("adopted answer")}
	f := newFixture(t, stream)
	f.answer(t, credentialName)
	f.plugin.job, f.plugin.endDelay = pluginsdk.JobRunning, 4*renewInterval
	record := f.submit(t, f.host, nil)
	resume := int64(len(stream[0]) + 1)
	f.holding(t, f.parse(t, record), wire.TaskHash(record.Value), testReceiver, f.receiver.signing, "claimed", nil)
	f.produce(t, admin.ReceiverProgressTopic(testReceiver), "run-a", progressRecord(t, wire.TaskHash(record.Value), resume, nil))
	if status := f.execute(t, record, 2); status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	if task := f.lastTask(t); task.Status.State != wire.TaskStateCompleted || task.Status.Message.Parts[0].Text != "adopted answer" {
		t.Fatalf("task %s %+v, want the outcome file's", task.Status.State, task.Status.Message)
	}
	var seqs []int64
	for _, chunk := range f.chunks(t) {
		seqs = append(seqs, chunk.Wherry.Seq)
	}
	want := []int64{resume, resume + int64(len(stream[1])+1), resume + int64(len(stream[1])+len(stream[2])+2)}
	f.plugin.mu.Lock()
	froms := slices.Clone(f.plugin.froms)
	f.plugin.mu.Unlock()
	if !slices.Equal(seqs, want) || !slices.Equal(froms, []int64{resume}) {
		t.Fatalf("chunks at %v followed from %v, want each line from %d once: %v", seqs, froms, resume, want)
	}
	adopterProgress := 0
	for _, record := range f.records(t, admin.ReceiverProgressTopic(testReceiver)) {
		if progress, err := wire.DecodeProgress(record.Value); err == nil && progress.Attempt == 2 {
			adopterProgress++
		}
	}
	if adopterProgress == 0 {
		t.Fatal("the adopter wrote no progress")
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
	f.holding(t, task, hash, testReceiver, f.receiver.signing, "claimed", nil)
	for _, value := range [][]byte{
		progressRecord(t, hash, 42, nil),
		progressRecord(t, strings.Repeat("c", 64), 99, nil),
		progressRecord(t, hash, 99, func(progress *wire.Progress) { progress.Tenant = "tenant-b" }),
		[]byte(`{"v":1,"tenant":"tenant-a","task_hash":"` + hash + `","attempt":1,"log_offset":99,"extra":1}`),
	} {
		f.produce(t, admin.ReceiverProgressTopic(testReceiver), "run-offset-binding", value)
	}
	f.produce(t, admin.ReceiverProgressTopic(testReceiver), "run-other", progressRecord(t, hash, 99, nil))
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
	if dispatches, _, _, _ := f.plugin.counts(); dispatches != 0 {
		t.Fatalf("adoption read failure dispatched %d jobs", dispatches)
	}
}

func TestAdoptionTrustsPeerProgress(t *testing.T) {
	f := newFixture(t, resultStream)
	peerSigning := f.addPeer(t, "receiver-peer", testPool)
	if err := f.service.PoolCreate(f.ctx, "pool-b", testTerms()); err != nil {
		t.Fatal(err)
	}
	otherSigning := f.addPeer(t, "receiver-other-pool", "pool-b")
	task := testSubmission("run-peer")
	hash := strings.Repeat("a", 64)
	f.holding(t, task, hash, "receiver-peer", peerSigning, "claimed", nil)
	f.holding(t, task, hash, "receiver-other-pool", otherSigning, "claimed", nil)
	f.produce(t, admin.ReceiverProgressTopic("receiver-peer"), "run-peer", progressRecord(t, hash, 42, nil))
	f.produce(t, admin.ReceiverProgressTopic("receiver-other-pool"), "run-peer", progressRecord(t, hash, 99, nil))
	if got, err := f.receiver.lastOffset(f.ctx, task, hash); err != nil || got != 42 {
		t.Fatalf("peer progress offset %d err %v, want 42", got, err)
	}
}

// A holder whose progress topic the adopter cannot read contributes nothing: the adopter resumes from the run's
// started offset.
func TestAdoptionFallsBackToStartedWithoutTheHoldersProgress(t *testing.T) {
	for _, cause := range []string{"ended", "no acl"} {
		t.Run(cause, func(t *testing.T) {
			f := newFixture(t, resultStream)
			peerSigning := f.addPeer(t, "receiver-peer", testPool)
			task := testSubmission("run-peer")
			hash := strings.Repeat("a", 64)
			started := int64(7)
			f.holding(t, task, hash, testReceiver, f.receiver.signing, "started", &started)
			f.holding(t, task, hash, "receiver-peer", peerSigning, "claimed", nil)
			f.produce(t, admin.ReceiverProgressTopic("receiver-peer"), "run-peer", progressRecord(t, hash, 42, nil))
			if got, err := f.receiver.lastOffset(f.ctx, task, hash); err != nil || got != 42 {
				t.Fatalf("live peer's progress offset %d err %v, want 42", got, err)
			}
			switch cause {
			case "ended":
				result := f.service.RevokeReceiver(f.ctx, "receiver-peer", time.Millisecond, func(context.Context, string, string) error { return admin.ErrAuthorizationDenied })
				if !result.Complete {
					t.Fatalf("incomplete revocation: %+v", result)
				}
				if err := f.receiver.refresh(f.ctx); err != nil {
					t.Fatal(err)
				}
			case "no acl":
				f.deleteReceiverACLs(t, kadm.NewACLs().Topics(admin.ReceiverProgressTopic("receiver-peer")).Operations(kadm.OpAny))
			}
			if got, err := f.receiver.lastOffset(f.ctx, task, hash); err != nil || got != started {
				t.Fatalf("offset %d err %v without the peer's progress, want the started offset %d", got, err, started)
			}
		})
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
	err := zombie.commit(f.ctx, zombie.progress(42))
	if err == nil {
		t.Fatal("zombie write succeeded after adoption")
	}
	if status := zombie.abandon(err); status != 0 {
		t.Fatalf("fenced run returned ack %v, want 0 (no acknowledgement)", status)
	}
	if offsets := f.progress(t, testReceiver, "run-fence"); len(offsets) != 0 {
		t.Fatalf("zombie progress %v is readable after fencing", offsets)
	}
}

// A fenced holder's start failure must not purge the job its adopter now runs.
func TestFencedStartFailureLeavesTheJob(t *testing.T) {
	f := newFixture(t, resultStream)
	zombie := testActiveRun(t, f, "run-fence", 1)
	if err := zombie.commit(f.ctx, zombie.statusRecords("claimed", taskStatus(wire.TaskStateWorking, ""), nil, nil)...); err != nil {
		t.Fatal(err)
	}
	adopter := testActiveRun(t, f, "run-fence", 2)
	if err := adopter.commit(f.ctx, adopter.statusRecords("claimed", taskStatus(wire.TaskStateWorking, ""), nil, nil)...); err != nil {
		t.Fatal(err)
	}
	if status := zombie.startFailure(f.ctx, "dispatch", "profile nomad created no job"); status != 0 {
		t.Fatalf("fenced start failure returned ack %v, want 0 (no acknowledgement)", status)
	}
	f.plugin.mu.Lock()
	defer f.plugin.mu.Unlock()
	if f.plugin.purges != 0 {
		t.Fatalf("fenced holder purged %d times, want 0", f.plugin.purges)
	}
}

func TestStartFailureWriteDenialEntersDenial(t *testing.T) {
	f := newFixture(t, resultStream)
	run := testActiveRun(t, f, "run-a", 1)
	f.deleteReceiverACLs(t, kadm.NewACLs().Topics(admin.PoolEventsTopic(testPool)).Operations(kadm.OpWrite))
	if status := run.startFailure(f.ctx, "dispatch", "profile nomad created no job"); status != kgo.AckRelease {
		t.Fatalf("status %v, want release", status)
	}
	if !f.receiver.holds(capWrite) || !strings.Contains(f.logs.String(), "run run-a: write failed") {
		t.Fatalf("a start failure that could not be written was not denied and logged; logs:\n%s", f.logs.String())
	}
	f.plugin.mu.Lock()
	defer f.plugin.mu.Unlock()
	if f.plugin.purges != 0 {
		t.Fatalf("purged %d times without a committed start failure, want 0", f.plugin.purges)
	}
}

func TestFollowErrorReasonIsFixed(t *testing.T) {
	f := newFixture(t, nil)
	f.plugin.followErr = errors.New("upstream detail")
	task := runToTask(t, f)
	if got := task.Artifacts[0].Parts[0].Text; got != "log stream failed" {
		t.Fatalf("result reason %q, want fixed reason", got)
	}
	if logs := f.logs.String(); !strings.Contains(logs, "follow failed: upstream detail\n") {
		t.Fatalf("follow error not logged: %q", logs)
	}
}

// shortLock makes the receiver read a lock duration below kfake's minimum from the terms it folded.
func shortLock(f fixture, lock time.Duration) {
	f.receiver.mu.Lock()
	defer f.receiver.mu.Unlock()
	terms := f.receiver.state.Pools[testPool]
	terms.LockDurationMS = admin.Millis(lock.Milliseconds())
	f.receiver.state.Pools[testPool] = terms
}

func TestFollowLinkLossResumesFromOffset(t *testing.T) {
	f := newFixture(t, resultStream)
	f.plugin.linkLost = 1
	f.answer(t, credentialName)
	f.submit(t, f.host, nil)
	if err := f.receiver.claimOne(f.ctx, f.ctx, func() {}); err != nil {
		t.Fatal(err)
	}
	if task := f.lastTask(t); task.Status.State != wire.TaskStateCompleted || task.Artifacts[0].Parts[0].Text != "done" {
		t.Fatalf("task %#v after a lost plugin link; want the run completed; logs:\n%s", task.Status, f.logs.String())
	}
	f.plugin.mu.Lock()
	froms := f.plugin.froms
	f.plugin.mu.Unlock()
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
	f.plugin.linkLost = 1 << 20
	shortLock(f, 2*time.Second)
	f.answer(t, credentialName)
	f.submit(t, f.host, nil)
	done := make(chan error, 1)
	started := time.Now()
	go func() { done <- f.receiver.claimOne(f.ctx, f.ctx, func() {}) }()
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
	if _, _, follows, _ := f.plugin.counts(); follows < 2 {
		t.Fatalf("followed %d times; want retries before failing", follows)
	}
}

func TestFollowErrorResumesFromOffsetByJobState(t *testing.T) {
	for _, test := range []struct {
		name  string
		state pluginsdk.JobState
	}{
		{"job running", pluginsdk.JobRunning},
		{"job ended", pluginsdk.JobEnded},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.plugin.linkLost, f.plugin.lostErr, f.plugin.job = 1, errors.New("stream reset"), test.state
			f.answer(t, credentialName)
			f.submit(t, f.host, nil)
			if err := f.receiver.claimOne(f.ctx, f.ctx, func() {}); err != nil {
				t.Fatal(err)
			}
			if task := f.lastTask(t); task.Status.State != wire.TaskStateCompleted || task.Artifacts[0].Parts[0].Text != "done" {
				t.Fatalf("task %#v after a failed follow; want the run completed; logs:\n%s", task.Status, f.logs.String())
			}
			f.plugin.mu.Lock()
			froms, lookups := f.plugin.froms, f.plugin.lookups
			f.plugin.mu.Unlock()
			first := int64(len(resultStream[0]) + 1)
			if !slices.Equal(froms, []int64{0, first}) || lookups != 1 {
				t.Fatalf("follows from %v after %d lookups; want 0, then the offset after the line taken before the failure, after one", froms, lookups)
			}
			var transcript strings.Builder
			for _, chunk := range f.chunks(t) {
				transcript.WriteString(chunk.ArtifactUpdate.Artifact.Parts[0].Text)
			}
			if want := strings.Join(resultStream, ""); transcript.String() != want {
				t.Fatalf("transcript %q, want %q", transcript.String(), want)
			}
			if want := fmt.Sprintf("run run-a: follow failed (%s); following again from offset %d\n", test.name, first); strings.Count(f.logs.String(), want) != 1 {
				t.Fatalf("logs lack one %q:\n%s", want, f.logs.String())
			}
		})
	}
}

func TestFollowErrorOnAnEndedJobFailsOnTheSecondFailure(t *testing.T) {
	f := newFixture(t, resultStream)
	f.plugin.linkLost, f.plugin.lostErr, f.plugin.job = 2, errors.New("stream reset"), pluginsdk.JobEnded
	task := runToTask(t, f)
	if task.Status.State != wire.TaskStateFailed || task.Artifacts[0].Parts[0].Text != "log stream failed" {
		t.Fatalf("task %#v; want FAILED log stream failed", task.Status)
	}
	if _, _, follows, _ := f.plugin.counts(); follows != 2 {
		t.Fatalf("followed an ended job %d times; want once more after the failure", follows)
	}
}

func TestFollowErrorFailsAfterOneLockDuration(t *testing.T) {
	for _, test := range []struct {
		name      string
		lookupErr error
	}{
		{"job running", nil},
		{"lookup failed", errors.New("lookup down")},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, noResultStream)
			f.plugin.linkLost, f.plugin.lostErr, f.plugin.job, f.plugin.lookupErr = 1<<20, errors.New("stream reset"), pluginsdk.JobRunning, test.lookupErr
			shortLock(f, 2*time.Second)
			f.answer(t, credentialName)
			f.submit(t, f.host, nil)
			done := make(chan error, 1)
			started := time.Now()
			go func() { done <- f.receiver.claimOne(f.ctx, f.ctx, func() {}) }()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(15 * time.Second):
				t.Fatalf("a run whose follow keeps failing kept recovering; logs:\n%s", f.logs.String())
			}
			if waited := time.Since(started); waited < 2*time.Second {
				t.Fatalf("run failed %s after the failure, before one lock duration", waited)
			}
			if state, reason := terminalReason(t, f, "run-a"); state != wire.TaskStateFailed || reason != "log stream failed" {
				t.Fatalf("terminal %s %q; want FAILED log stream failed", state, reason)
			}
			if _, lookups, _, _ := f.plugin.counts(); lookups < 2 {
				t.Fatalf("looked up %d times; want retries before failing", lookups)
			}
		})
	}
}

func TestFollowErrorOnANeverStartedJobIsAStartFailure(t *testing.T) {
	f := newFixture(t, resultStream)
	f.plugin.linkLost, f.plugin.lostErr, f.plugin.job = 1, errors.New("stream reset"), pluginsdk.JobNeverStarted
	f.answer(t, credentialName)
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckRelease {
		t.Fatalf("status %v, want release; logs:\n%s", status, f.logs.String())
	}
	if failures := f.startFailures(t); len(failures) != 1 || failures[0].Class != "platform" {
		t.Fatalf("start failures %#v, want one platform start failure", failures)
	}
	if _, _, follows, _ := f.plugin.counts(); follows != 1 {
		t.Fatalf("followed %d times; want no follow after the job never started", follows)
	}
}

func TestOversizedDispatchIsLoggedByName(t *testing.T) {
	f := newFixture(t, resultStream)
	f.answer(t, credentialName)
	f.plugin.dispatchErr = pluginsdk.ErrMessageTooLarge
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
	f.plugin.dispatchErr = errors.New(secret)
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckRelease {
		t.Fatalf("status %v", status)
	}
	if failures := f.startFailures(t); len(failures) != 1 || failures[0].Class != "dispatch" {
		t.Fatalf("start failures %#v, want one dispatch start failure", failures)
	}
	for _, topic := range []string{admin.PoolEventsTopic(testPool), admin.TenantTopic(testTenant), admin.TenantTranscriptTopic(testTenant)} {
		for _, record := range f.records(t, topic) {
			if strings.Contains(string(record.Value), secret) {
				t.Fatalf("dispatch error detail published on %s", topic)
			}
		}
	}
}

func TestDispatchErrorIsLoggedRedacted(t *testing.T) {
	f := newFixture(t, resultStream)
	f.answer(t, credentialName)
	f.plugin.dispatchErr = &pluginsdk.Error{Op: "write variable nomad/jobs/agent-run-a-t1", Class: "denied", Err: errors.New("status 403: echoed " + testToken)}
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckRelease {
		t.Fatalf("status %v", status)
	}
	want := "dispatch failed: op=write variable nomad/jobs/agent-run-a-t1 class=denied: status 403: echoed " + signed.RedactionMarker(testRedactionKey, credentialName) + "\n"
	if logs := f.logs.String(); !strings.Contains(logs, want) || strings.Contains(logs, testToken) {
		t.Fatalf("dispatch error log lacks %q or holds the credential: %q", want, logs)
	}
}

func TestPluginErrorLogsAreRedacted(t *testing.T) {
	failure := &pluginsdk.Error{Op: "deregister job", Class: "conflict", Err: errors.New("echoed " + testToken)}
	for _, test := range []struct {
		name string
		call func(*testing.T, fixture, *activeRun)
	}{
		{"stop on result", func(t *testing.T, f fixture, run *activeRun) {
			f.plugin.stopErr = failure
			run.conclude(f.ctx, pluginsdk.Handle{}, nil, pluginsdk.Terminal{Started: true})
		}},
		{"stop on failure", func(t *testing.T, f fixture, run *activeRun) {
			f.plugin.stopErr = failure
			run.stopAndFail(f.ctx, pluginsdk.Handle{}, "timeout", nil)
		}},
		{"stop on denial", func(t *testing.T, f fixture, run *activeRun) {
			f.plugin.stopErr = failure
			run.deny(f.ctx, pluginsdk.Handle{}, nil)
		}},
		{"release", func(t *testing.T, f fixture, run *activeRun) {
			f.plugin.releaseErr = failure
			run.release(f.ctx)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			run := testActiveRun(t, f, "run-log", 1)
			run.deliver(map[string][]byte{credentialName: []byte(testToken)})
			test.call(t, f, run)
			want := "op=deregister job class=conflict: echoed " + signed.RedactionMarker(testRedactionKey, credentialName) + "\n"
			if logs := f.logs.String(); !strings.Contains(logs, want) || strings.Contains(logs, testToken) {
				t.Fatalf("plugin error log lacks %q or holds the credential: %q", want, logs)
			}
		})
	}
}

func TestPluginErrorWithoutClassLogsNoClass(t *testing.T) {
	if got := logText((&Receiver{}).redactLive, &pluginsdk.Error{Op: "read allocation", Err: errors.New("not found")}); got != "op=read allocation: not found" {
		t.Fatalf("logText = %q", got)
	}
}

// An adopter looks the job up before any seal gives it values to redact.
func TestLookupErrorIsLogged(t *testing.T) {
	f := newFixture(t, resultStream)
	f.plugin.lookupErr = &pluginsdk.Error{Op: "read job", Class: "unavailable", Err: errors.New("connection refused")}
	f.execute(t, f.submit(t, f.host, nil), 2)
	if logs := f.logs.String(); !strings.Contains(logs, "lookup in profile "+f.receiver.profiles[0].Name+": op=read job class=unavailable: connection refused\n") {
		t.Fatalf("lookup error not logged: %q", logs)
	}
}

// outcomeOutput is an outcome file the job leaves, streamed as a plugin streams it.
func outcomeOutput(data string) pluginsdk.Output {
	return pluginsdk.Output{Name: wire.OutcomeFile, Size: int64(len(data)), Body: io.NopCloser(strings.NewReader(data))}
}

func TestOutcomeFileEndsTheRun(t *testing.T) {
	outcome := func(state, role, parts string) string {
		return `{"state":"` + state + `","message":{"role":"` + role + `","parts":[` + parts + `]}}`
	}
	exited, killed := &pluginsdk.Terminal{Started: true, ExitCode: 3}, &pluginsdk.Terminal{Started: true, ExitCode: 137, Signal: "SIGKILL"}
	for _, test := range []struct {
		name     string
		terminal *pluginsdk.Terminal
		outcome  string
		state    wire.TaskState
		message  string
		ack      kgo.AckStatus
	}{
		{name: "completed", outcome: outcome("TASK_STATE_COMPLETED", "ROLE_AGENT", `{"text":"the answer"}`), state: wire.TaskStateCompleted, message: "the answer", ack: kgo.AckAccept},
		{name: "completed after an exit", terminal: exited, outcome: outcome("TASK_STATE_COMPLETED", "ROLE_AGENT", `{"text":"the answer"}`), state: wire.TaskStateCompleted, message: "the answer", ack: kgo.AckAccept},
		{name: "failed", outcome: outcome("TASK_STATE_FAILED", "ROLE_AGENT", `{"text":"the tests fail"}`), state: wire.TaskStateFailed, message: "the tests fail", ack: kgo.AckReject},
		{name: "auth required", terminal: exited, outcome: outcome("TASK_STATE_AUTH_REQUIRED", "ROLE_AGENT", `{"text":"the token expired"},{"data":{"credential":"github-token"}}`),
			state: wire.TaskStateAuthRequired, message: "the token expired\n{\"credential\":\"github-token\"}", ack: kgo.AckReject},
		{name: "missing after an exit", terminal: exited, state: wire.TaskStateFailed, message: "agent exited 3", ack: kgo.AckReject},
		{name: "missing after a signal", terminal: killed, state: wire.TaskStateFailed, message: "agent ended by SIGKILL", ack: kgo.AckReject},
		{name: "missing", state: wire.TaskStateFailed, message: "agent reported no outcome", ack: kgo.AckReject},
		{name: "bad JSON", outcome: `{"state":`, state: wire.TaskStateFailed, message: "agent reported no outcome", ack: kgo.AckReject},
		{name: "unknown state", terminal: exited, outcome: outcome("TASK_STATE_WORKING", "ROLE_AGENT", `{"text":"still going"}`), state: wire.TaskStateFailed, message: "agent exited 3", ack: kgo.AckReject},
		{name: "wrong role", outcome: outcome("TASK_STATE_COMPLETED", "ROLE_USER", `{"text":"the answer"}`), state: wire.TaskStateFailed, message: "agent reported no outcome", ack: kgo.AckReject},
		{name: "auth required without its credential", outcome: outcome("TASK_STATE_AUTH_REQUIRED", "ROLE_AGENT", `{"text":"the token expired"}`), state: wire.TaskStateFailed, message: "agent reported no outcome", ack: kgo.AckReject},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := &fakePlugin{stream: noResultStream, ended: test.terminal}
			if test.outcome != "" {
				fake.outputs = []pluginsdk.Output{outcomeOutput(test.outcome)}
			}
			f := newFixtureWith(t, noResultStream, testTerms(), testConfig(testReceiver), map[string]*fakePlugin{"nomad": fake})
			f.answer(t, credentialName)
			if ack := f.execute(t, f.submit(t, f.host, nil), 1); ack != test.ack {
				t.Fatalf("ack %v, want %v", ack, test.ack)
			}
			task := f.lastTask(t)
			terminal := f.feedStatuses(t)[len(f.feedStatuses(t))-1].StatusUpdate.Status
			for _, status := range []wire.TaskStatus{task.Status, terminal} {
				var texts []string
				for _, part := range status.Message.Parts {
					texts = append(texts, cmp.Or(part.Text, string(part.Data)))
				}
				if status.State != test.state || strings.Join(texts, "\n") != test.message {
					t.Fatalf("status %s with %q, want %s with %q", status.State, texts, test.state, test.message)
				}
			}
			if result := task.Artifacts[0].Parts[0].Text; result != strings.Split(test.message, "\n")[0] {
				t.Fatalf("result %q, want the message's text", result)
			}
			f.plugin.mu.Lock()
			defer f.plugin.mu.Unlock()
			if f.plugin.collects != 1 || f.plugin.stops != 1 {
				t.Fatalf("collects %d stops %d, want the job stopped and its outputs collected once", f.plugin.collects, f.plugin.stops)
			}
		})
	}
}

// An agent that ended abnormally without an outcome file is named with the platform's redacted reason, and keeps that
// cause in front of a missing required output.
func TestEndedAgentNamesItsReason(t *testing.T) {
	oom := &pluginsdk.Terminal{Started: true, ExitCode: 137, Reason: "OOMKilled"}
	outcome := outcomeOutput(`{"state":"TASK_STATE_COMPLETED","message":{"role":"ROLE_AGENT","parts":[{"text":"the answer"}]}}`)
	for _, test := range []struct {
		name     string
		terminal *pluginsdk.Terminal
		outputs  []pluginsdk.Output
		message  string
	}{
		{"exit with a missing output", oom, nil, "agent exited 137 (OOMKilled); required output report.md not returned: missing"},
		{"exit with its outputs", oom, []pluginsdk.Output{file("report.md", "r")}, "agent exited 137 (OOMKilled)"},
		{"signal", &pluginsdk.Terminal{Started: true, ExitCode: 137, Signal: "9", Reason: "OOMKilled"}, []pluginsdk.Output{file("report.md", "r")}, "agent ended by 9 (OOMKilled)"},
		{"exit without a reason", &pluginsdk.Terminal{Started: true, ExitCode: 137}, nil, "agent exited 137; required output report.md not returned: missing"},
		{"exit 0", &pluginsdk.Terminal{Started: true, Reason: "Completed"}, nil, "required output report.md not returned: missing"},
		{"outcome file", oom, []pluginsdk.Output{outcome}, "required output report.md not returned: missing"},
		{"credential", &pluginsdk.Terminal{Started: true, ExitCode: 1, Reason: "Error: " + testToken}, []pluginsdk.Output{file("report.md", "r")},
			"agent exited 1 (Error: " + signed.RedactionMarker(testRedactionKey, credentialName) + ")"},
		{"url and newline", &pluginsdk.Terminal{Started: true, ExitCode: 1, Reason: "Error: GET https://registry.example/x?token=abc\nfailed"}, []pluginsdk.Output{file("report.md", "r")},
			"agent exited 1 (Error: GET [redacted-url] failed)"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := &fakePlugin{stream: noResultStream, ended: test.terminal, outputs: test.outputs}
			f := newFixtureWith(t, noResultStream, testTerms(), testConfig(testReceiver), map[string]*fakePlugin{"nomad": fake})
			f.answer(t, credentialName)
			if ack := f.execute(t, f.submit(t, f.host, func(task *wire.Submission) {
				task.Wherry.Outputs = []wire.ExpectedOutput{{Name: "report.md", Required: true}}
			}), 1); ack != kgo.AckReject {
				t.Fatalf("ack %v, want reject", ack)
			}
			status := f.lastTask(t).Status
			if status.State != wire.TaskStateFailed || status.Message.Parts[0].Text != test.message {
				t.Fatalf("status %s with %q, want FAILED with %q", status.State, status.Message.Parts[0].Text, test.message)
			}
		})
	}
}

func TestOutcomeFileIsNotAnOutput(t *testing.T) {
	f := newFixtureWith(t, resultStream, testTerms(), testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream, outputs: []pluginsdk.Output{
		{Name: wire.OutcomeDir + "/notes.md", Size: 5, Data: []byte("notes")}, {Name: "report.md", Size: 6, Data: []byte("report")},
	}}})
	f.answer(t, credentialName)
	f.execute(t, f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Outputs = []wire.ExpectedOutput{{Name: "report.md", Required: true}}
	}), 1)
	task := f.lastTask(t)
	listed := manifest(t, task)
	if task.Status.State != wire.TaskStateCompleted || len(listed.Outputs) != 1 || listed.Outputs[0].Name != "report.md" || len(listed.NotReturned) != 0 {
		t.Fatalf("task %s outputs %+v not returned %+v, want report.md alone", task.Status.State, listed.Outputs, listed.NotReturned)
	}
	for _, output := range f.outputChunks(t) {
		if output.ArtifactUpdate.Artifact.ArtifactID != "report.md" {
			t.Fatalf("output %s published", output.ArtifactUpdate.Artifact.ArtifactID)
		}
	}
	f.plugin.mu.Lock()
	defer f.plugin.mu.Unlock()
	if !slices.Contains(f.plugin.limits.Stream, wire.OutcomeFile) {
		t.Fatalf("collect limits %+v, want the outcome file streamed outside the output limits", f.plugin.limits)
	}
}

func TestOutcomeMessageIsRedactedAndBounded(t *testing.T) {
	marker := signed.RedactionMarker(testRedactionKey, credentialName)
	for _, test := range []struct {
		name, text, want string
	}{
		{name: "credential", text: "used " + testToken, want: "used " + marker},
		{name: "over", text: strings.Repeat("x", wire.MaxText+1), want: overText},
	} {
		t.Run(test.name, func(t *testing.T) {
			outcome := `{"state":"TASK_STATE_AUTH_REQUIRED","message":{"role":"ROLE_AGENT","parts":[{"text":"` + test.text + `"},{"data":{"credential":"cred-lab"}}]}}`
			f := newFixtureWith(t, noResultStream, testTerms(), testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: noResultStream, outputs: []pluginsdk.Output{outcomeOutput(outcome)}}})
			f.answer(t, credentialName)
			f.execute(t, f.submit(t, f.host, nil), 1)
			task := f.lastTask(t)
			terminal := f.feedStatuses(t)[len(f.feedStatuses(t))-1].StatusUpdate.Status
			for _, status := range []wire.TaskStatus{task.Status, terminal} {
				if status.State != wire.TaskStateAuthRequired || status.Message.Parts[0].Text != test.want {
					t.Fatalf("status %s with %.80q, want %s with %.80q", status.State, status.Message.Parts[0].Text, wire.TaskStateAuthRequired, test.want)
				}
			}
			if result := task.Artifacts[0].Parts[0].Text; result != test.want {
				t.Fatalf("result %.80q, want %.80q", result, test.want)
			}
			for _, record := range append(f.records(t, admin.TenantTopic(testTenant)), f.records(t, admin.TenantTranscriptTopic(testTenant))...) {
				if strings.Contains(string(decodedPayload(record.Value)), testToken) {
					t.Fatal("the credential value reached the feed")
				}
			}
		})
	}
}

// An agent's text that escaping would lengthen past its base64 form travels raw in the status message, as in the result.
func TestOutcomeMessageTravelsRawWhenEscapingGrowsIt(t *testing.T) {
	text := strings.Repeat(`\u0001`, 64)
	f := newFixtureWith(t, noResultStream, testTerms(), testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: noResultStream, outputs: []pluginsdk.Output{
		outcomeOutput(`{"state":"TASK_STATE_FAILED","message":{"role":"ROLE_AGENT","parts":[{"text":"` + text + `"}]}}`),
	}}})
	f.answer(t, credentialName)
	f.execute(t, f.submit(t, f.host, nil), 1)
	part := f.lastTask(t).Status.Message.Parts[0]
	if part.Text != "" || string(part.Raw) != strings.Repeat("\x01", 64) || part.MediaType != "text/plain" {
		t.Fatalf("status message part %+v, want the text raw", part)
	}
}

func TestReceiverRoutesToMatchedProfile(t *testing.T) {
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
		t.Fatalf("exec, offered by both, went to lab=%d cloud=%d; want the first configured profile", labDispatches, cloudDispatches-1)
	}
}

func TestReceiverRedeliveryAdoptsViaLookup(t *testing.T) {
	m := newMultiFixture(t)
	m.answer(t, "cred-cloud")
	m.cloud.job = pluginsdk.JobRunning
	if status := m.execute(t, "run-moved", cpu, 2); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	labDispatches, labLookups, labFollows, labReleases := m.lab.counts()
	cloudDispatches, cloudLookups, cloudFollows, cloudReleases := m.cloud.counts()
	if labLookups != 1 || cloudLookups != 1 {
		t.Fatalf("lookups lab=%d cloud=%d, want one each in order", labLookups, cloudLookups)
	}
	if labDispatches != 0 || labFollows != 0 || labReleases != 0 {
		t.Fatalf("matched profile used: dispatches=%d follows=%d releases=%d", labDispatches, labFollows, labReleases)
	}
	if cloudDispatches != 0 || cloudFollows != 1 || cloudReleases != 1 {
		t.Fatalf("adopting profile: dispatches=%d follows=%d releases=%d, want 0, 1, 1", cloudDispatches, cloudFollows, cloudReleases)
	}
	if _, edgeLookups, _, _ := m.edge.counts(); edgeLookups != 0 {
		t.Fatalf("lookup continued past the first hit: edge lookups=%d", edgeLookups)
	}
}

func TestReceiverFirstAttemptSkipsLookup(t *testing.T) {
	m := newMultiFixture(t)
	m.cloud.job = pluginsdk.JobRunning
	if status := m.execute(t, "run-first", cpu, 1); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	labDispatches, labLookups, _, _ := m.lab.counts()
	_, cloudLookups, _, _ := m.cloud.counts()
	if labLookups != 0 || cloudLookups != 0 || labDispatches != 1 {
		t.Fatalf("lookups lab=%d cloud=%d, lab dispatches=%d", labLookups, cloudLookups, labDispatches)
	}
}

// Every plugin profile of an agent delivers the agent's credentials, each at the target the pool's terms give it.
func TestReceiverDeliversTheAgentsCredentialsInEveryProfile(t *testing.T) {
	m := newMultiFixture(t)
	want := []pluginsdk.Secret{{Name: credentialName, Env: credentialEnv, Value: []byte(testToken)}}
	for _, routed := range []struct {
		run      string
		selector []wire.Requirement
		plugin   *fakePlugin
	}{{"run-lab", cpu, m.lab}, {"run-edge", arm, m.edge}} {
		if status := m.execute(t, routed.run, routed.selector, 1); status != kgo.AckAccept {
			t.Fatalf("%s status %v", routed.run, status)
		}
		if got := routed.plugin.lastRun.Credentials; !reflect.DeepEqual(got, want) {
			t.Fatalf("%s dispatched %d credentials, want the agent's at its environment variable", routed.run, len(got))
		}
	}
	if strings.Contains(m.logs.String(), testToken) {
		t.Fatal("credential value appeared in receiver logs")
	}
}

func TestShutdownCommitsHeldTranscriptLine(t *testing.T) {
	f := newFixture(t, noResultStream)
	f.plugin.endDelay = 30 * time.Second
	ctx, shutdown := context.WithCancel(f.ctx)
	done := startInFlight(t, f, ctx, "run-shutdown")
	for f.plugin.deliveredCount() == 0 {
		if f.ctx.Err() != nil {
			t.Fatalf("the plugin never delivered the line; logs:\n%s", f.logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
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

// shutdownAt cancels the receiver as its run's status of kind is produced, once the plugin has delivered a line, so
// the shutdown lands while that status's commit is in flight.
type shutdownAt struct {
	f        fixture
	kind     string
	shutdown context.CancelFunc
}

func (h shutdownAt) OnProduceRecordBuffered(record *kgo.Record) {
	var status wire.RunStatus
	progress := h.kind == "progress" && record.Topic == admin.ReceiverProgressTopic(testReceiver) && record.Value != nil
	if !progress && (wire.ParseRecord(record.Value, h.f.keys(), &status) != nil || status.Wherry.Kind != h.kind) {
		return
	}
	for h.f.plugin.deliveredCount() == 0 && h.f.ctx.Err() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	h.shutdown()
}

func TestShutdownDuringACommitCommitsTheDeliveredLine(t *testing.T) {
	for _, kind := range []string{"started", "progress"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t, noResultStream)
			f.plugin.endDelay = 30 * time.Second
			ctx, shutdown := context.WithCancel(f.ctx)
			base, err := kgo.NewClient(append(f.receiver.base.Opts(), kgo.WithHooks(shutdownAt{f, kind, shutdown}))...)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(base.Close)
			f.receiver.base = base
			f.answer(t, credentialName)
			f.submit(t, f.host, nil)
			if err := f.receiver.claimOne(ctx, ctx, func() {}); err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("claimOne after shutdown: %v", err)
			}
			for _, chunk := range f.chunks(t) {
				if strings.Contains(chunk.ArtifactUpdate.Artifact.Parts[0].Text, "working") {
					return
				}
			}
			t.Fatalf("the delivered transcript line was lost to a shutdown during the %s commit; logs:\n%s", kind, f.logs.String())
		})
	}
}

func TestShutdownDuringTheStartedCommitCommitsAnAdoptersHeldLines(t *testing.T) {
	f, record := adopting(t)
	ctx, shutdown := context.WithCancel(f.ctx)
	base, err := kgo.NewClient(append(f.receiver.base.Opts(), kgo.WithHooks(shutdownAt{f, "started", shutdown}))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(base.Close)
	f.receiver.base = base
	done := f.executing(t, ctx, record, 2)
	request := f.request(t, 1)
	f.held(t)
	f.sealFor(t, request, map[string][]byte{credentialName: []byte(testToken)})
	if status := <-done; status != kgo.AckRelease {
		t.Fatalf("adopter ended %v, want the task released; logs:\n%s", status, f.logs.String())
	}
	if chunks := f.chunks(t); len(chunks) != 2 || chunks[0].ArtifactUpdate.Artifact.Parts[0].Text != `{"type":"assistant","message":"token `+redacted+`"}` {
		t.Fatalf("adopter's transcript %+v, want both held lines with the run's marker; logs:\n%s", chunks, f.logs.String())
	}
}

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
	if dispatches, _, _, _ := f.plugin.counts(); dispatches != 1 {
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
	f.receiver.claimLoop(ctx, ctx, func() {})
	if dispatches, _, _, _ := f.plugin.counts(); dispatches != 0 || len(f.records(t, admin.PoolEventsTopic(testPool))) != 0 {
		t.Fatalf("a denied receiver claimed: dispatches %d", dispatches)
	}
	if record, delivered := f.pollPool(t, "run-a", 5*time.Second); !delivered || record.DeliveryCount() != 1 {
		t.Fatal("the task was acquired while the receiver was denied")
	}
}

func TestDeniedReceiverStopsInFlightRunsAfterOneLockInterval(t *testing.T) {
	f := newFixture(t, noResultStream)
	f.plugin.endDelay = 30 * time.Second
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
	f.plugin.mu.Lock()
	stops, releaseIDs := f.plugin.stops, f.plugin.releaseIDs
	f.plugin.mu.Unlock()
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
	if status := run.conclude(f.ctx, pluginsdk.Handle{}, nil, pluginsdk.Terminal{Started: true}); status != kgo.AckRelease {
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
	go f.receiver.claimLoop(ctx, ctx, func() {})
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
	if err := holder.commit(f.ctx, holder.progress(0)); err != nil {
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
			if status := run.conclude(f.ctx, pluginsdk.Handle{}, nil, pluginsdk.Terminal{Started: true}); status != test.ack {
				t.Fatalf("status %v, want %v; logs:\n%s", status, test.ack, f.logs.String())
			}
			if _, _, _, releases := f.plugin.counts(); releases != test.releases {
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
	f.holding(t, task, hash, "receiver-peer", next, "claimed", nil)
	f.produce(t, admin.ReceiverProgressTopic("receiver-peer"), "run-peer", progressRecord(t, hash, 42, nil))
	if got, err := f.receiver.lastOffset(f.ctx, task, hash); err != nil || got != 42 {
		t.Fatalf("claim under the peer's rotated key: offset %d err %v, want 42", got, err)
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
		f.plugin.dispatchErr = errors.New("register response lost")
		f.plugin.job = pluginsdk.JobRunning
		if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckAccept {
			t.Fatalf("status %v, want the existing job followed to its result", status)
		}
		f.plugin.mu.Lock()
		defer f.plugin.mu.Unlock()
		if f.plugin.dispatches != 1 || f.plugin.follows != 1 || f.plugin.purges != 0 {
			t.Fatalf("dispatches=%d follows=%d purges=%d", f.plugin.dispatches, f.plugin.follows, f.plugin.purges)
		}
	})
	t.Run("redelivery finds it", func(t *testing.T) {
		f := newFixture(t, resultStream)
		f.answer(t, credentialName)
		f.plugin.dispatchErr = errors.New("register response lost")
		f.plugin.lookupErr = errors.New("lookup failed")
		record := f.submit(t, f.host, nil)
		if status := f.execute(t, record, 1); status != kgo.AckRelease {
			t.Fatalf("unresolved dispatch status %v, want release", status)
		}
		f.plugin.mu.Lock()
		lookups, purges, releases := f.plugin.lookups, f.plugin.purges, f.plugin.releases
		f.plugin.lookupErr, f.plugin.job = nil, pluginsdk.JobRunning
		f.plugin.mu.Unlock()
		if lookups != 3 || purges != 0 || releases != 0 || len(f.startFailures(t)) != 0 {
			t.Fatalf("lookups=%d purges=%d releases=%d start failures=%d; want three lookups and nothing removed", lookups, purges, releases, len(f.startFailures(t)))
		}
		if status := f.execute(t, record, 2); status != kgo.AckAccept {
			t.Fatalf("redelivery status %v, want the job adopted", status)
		}
		if dispatches, _, follows, _ := f.plugin.counts(); dispatches != 1 || follows != 1 {
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
	f.plugin.mu.Lock()
	calls := f.plugin.calls
	f.plugin.mu.Unlock()
	if len(calls) != 0 {
		t.Fatalf("calls %v, want the job and material left in place", calls)
	}
	run.attempt--
	if status := run.returnTask(f.ctx, nil); status != kgo.AckRelease {
		t.Fatalf("status %v below the budget, want release", status)
	}
}

func TestMismatchedJobIsPurgedNotAdopted(t *testing.T) {
	for name, mismatch := range map[string]func(*fakePlugin){
		"profile": func(plugin *fakePlugin) { plugin.jobProfile = "renamed" },
		"agent":   func(plugin *fakePlugin) { plugin.jobAgent = "codex" },
	} {
		t.Run(name, func(t *testing.T) {
			m := newMultiFixture(t)
			m.cloud.job = pluginsdk.JobRunning
			mismatch(m.cloud)
			if status := m.execute(t, "run-mismatch", cpu, 2); status != kgo.AckAccept {
				t.Fatalf("status %v", status)
			}
			m.cloud.mu.Lock()
			stops, purges, follows, profile, agent := m.cloud.stops, m.cloud.purges, m.cloud.follows, cmp.Or(m.cloud.jobProfile, "cloud"), cmp.Or(m.cloud.jobAgent, "claude")
			m.cloud.mu.Unlock()
			if stops != 1 || purges != 1 || follows != 0 {
				t.Fatalf("mismatched job: stops=%d purges=%d follows=%d, want stopped and purged, not followed", stops, purges, follows)
			}
			if dispatches, _, follows, _ := m.lab.counts(); dispatches != 1 || follows != 1 {
				t.Fatalf("matched profile dispatches=%d follows=%d, want a fresh dispatch", dispatches, follows)
			}
			if want := fmt.Sprintf("run run-mismatch: purged a job from profile %q agent %q", profile, agent); !strings.Contains(m.logs.String(), want) {
				t.Fatalf("logs lack %q:\n%s", want, m.logs.String())
			}
		})
	}
}

func TestNeverStartedJobIsAPlatformStartFailure(t *testing.T) {
	f := newFixture(t, resultStream)
	f.answer(t, credentialName)
	f.plugin.job = pluginsdk.JobNeverStarted
	if status := f.execute(t, f.submit(t, f.host, nil), 2); status != kgo.AckRelease {
		t.Fatalf("status %v, want release", status)
	}
	if failures := f.startFailures(t); len(failures) != 1 || failures[0].Class != "platform" || failures[0].Profile != "nomad" {
		t.Fatalf("start failures %#v, want one platform start failure", failures)
	}
	if kinds := f.kinds(t); slices.Contains(kinds, "started") {
		t.Fatalf("statuses %v, want no started status for a job that never started", kinds)
	}
	if dispatches, _, follows, _ := f.plugin.counts(); dispatches != 0 || follows != 0 {
		t.Fatalf("dispatches=%d follows=%d, want neither", dispatches, follows)
	}
}

// A plugin's never-started class, whether a plugin reports it today or not, records a platform start failure; the
// plugin's class reaches the log alone.
func TestNeverStartedPluginClassIsAPlatformStartFailure(t *testing.T) {
	for _, class := range []string{"image", "exec", "unheard-of"} {
		t.Run(class, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.plugin.stream, f.plugin.neverStarted, f.plugin.neverClass = nil, true, class
			f.answer(t, credentialName)
			if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckRelease {
				t.Fatalf("status %v, want release; logs:\n%s", status, f.logs.String())
			}
			if failures := f.startFailures(t); len(failures) != 1 || failures[0].Class != "platform" {
				t.Fatalf("start failures %#v, want one platform start failure", failures)
			}
			if want := "job never started (" + class + ")"; !strings.Contains(f.logs.String(), want) {
				t.Fatalf("logs lack %q:\n%s", want, f.logs.String())
			}
		})
	}
}

// The platform's reason for a job that never started reaches the receiver's log with the run's delivered credential
// redacted; without a reason the line is as it was.
func TestNeverStartedReasonIsLoggedRedacted(t *testing.T) {
	for _, test := range []struct{ name, reason, want string }{
		{"reason", "template rendered " + testToken, "run run-a: job never started (driver): template rendered " + signed.RedactionMarker(testRedactionKey, credentialName) + "\n"},
		{"no reason", "", "run run-a: job never started (driver)\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.plugin.stream, f.plugin.neverStarted, f.plugin.neverReason = nil, true, test.reason
			f.answer(t, credentialName)
			if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckRelease {
				t.Fatalf("status %v, want release; logs:\n%s", status, f.logs.String())
			}
			logs := f.logs.String()
			if !strings.Contains(logs, test.want) || strings.Contains(logs, testToken) {
				t.Fatalf("logs lack %q or hold the credential:\n%s", test.want, logs)
			}
		})
	}
}

func TestStartReasonIsRedactedOnOneBoundedLine(t *testing.T) {
	f := newFixture(t, resultStream)
	run := testActiveRun(t, f, "run-a", 1)
	run.deliver(map[string][]byte{credentialName: []byte(testToken)})
	marker := signed.RedactionMarker(testRedactionKey, credentialName)
	for _, test := range []struct {
		name     string
		terminal pluginsdk.Terminal
		want     string
	}{
		{"credential", pluginsdk.Terminal{Class: "validation", Reason: "rendered " + testToken}, ": rendered " + marker},
		{"url", pluginsdk.Terminal{Class: "driver", Reason: `Head "https://registry.example/v2/x?token=abc": no such host`}, `: Head "[redacted-url] no such host`},
		{"newlines", pluginsdk.Terminal{Class: "validation", Reason: "1 error occurred:\r\n* bad"}, ": 1 error occurred:  * bad"},
		{"fetch", pluginsdk.Terminal{Class: pluginsdk.ClassFetch, Reason: `error downloading "s3::bucket/object?X-Amz-Signature=abc"`}, ": [omitted]"},
		{"cap", pluginsdk.Terminal{Class: "driver", Reason: strings.Repeat("x", 2048)}, ": " + strings.Repeat("x", maxLogText)},
		{"cap after redaction", pluginsdk.Terminal{Class: "driver", Reason: strings.Repeat("x", maxLogText-10) + testToken}, ": " + (strings.Repeat("x", maxLogText-10) + marker)[:maxLogText]},
		{"empty", pluginsdk.Terminal{Class: "driver"}, ""},
	} {
		if got := run.startReason(test.terminal); got != test.want {
			t.Errorf("%s: reason %q, want %q", test.name, got, test.want)
		}
	}
}

// A plugin's log lines and the stderr go-plugin forwards reach the receiver's log with every live run's delivered
// credentials redacted.
func TestPluginOutputIsRedactedAgainstLiveRuns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wherry-plugin-fake")
	if output, err := exec.Command("go", "build", "-o", path, "github.com/benemon/wherry/pluginsdk/internal/testplugin").CombinedOutput(); err != nil {
		t.Fatalf("build test plugin: %v\n%s", err, output)
	}
	f := newFixture(t, resultStream)
	testActiveRun(t, f, "run-a", 1).deliver(map[string][]byte{credentialName: []byte(testToken)})
	logs := &syncBuffer{}
	plugin, err := pluginsdk.Launch(func() (string, error) { return path, nil }, f.receiver.PluginLogs(logs))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(plugin.Close)
	if _, err := plugin.Configure(pluginsdk.Settings{Name: "lab", Pool: testPool, Agent: "claude", Config: []byte(`{"log":"` + testToken + `"}`)}); err != nil {
		t.Fatal(err)
	}
	marker := signed.RedactionMarker(testRedactionKey, credentialName)
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		output := logs.String()
		if strings.Contains(output, testToken) {
			t.Fatalf("plugin output holds the credential:\n%s", output)
		}
		if strings.Contains(output, "configured: log="+marker) && strings.Contains(output, "[ERROR] raw "+marker) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("plugin output lacks the redacted log line and stderr line:\n%s", output)
		}
	}
}

// A run whose plugin fails to start on every attempt ends naming the last failure, with the credentials of every live
// run redacted, on one bounded line.
func TestBudgetExhaustedByStartFailuresNamesTheLastCause(t *testing.T) {
	const otherToken = "ghp-other-run-credential-never-logged"
	prefix := "delivery budget exhausted after 4 attempts; last: "
	for _, test := range []struct{ name, reason, want string }{
		{"own credential", "failed to create image " + testToken,
			prefix + "job never started (driver): failed to create image " + signed.RedactionMarker(testRedactionKey, credentialName)},
		{"another run's credential", "failed to create image " + otherToken,
			prefix + "job never started (driver): failed to create image " + signed.RedactionMarker(testRedactionKey, "github-token")},
		{"bounded", strings.Repeat("x", 2*maxLogText),
			prefix + ("job never started (driver): " + strings.Repeat("x", maxLogText))[:maxLogText]},
	} {
		t.Run(test.name, func(t *testing.T) {
			terms := testTerms()
			terms.DeliveryLimit = 5
			f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakePlugin{"nomad": {}})
			testActiveRun(t, f, "run-b", 1).deliver(map[string][]byte{"github-token": []byte(otherToken)})
			f.plugin.neverStarted, f.plugin.neverReason = true, test.reason
			f.answer(t, credentialName)
			if status := f.execute(t, f.submit(t, f.host, nil), 4); status != kgo.AckReject {
				t.Fatalf("status %v, want reject; logs:\n%s", status, f.logs.String())
			}
			if state, reason := terminalReason(t, f, "run-a"); state != wire.TaskStateFailed || reason != test.want {
				t.Fatalf("terminal %s %q, want %q", state, reason, test.want)
			}
		})
	}
}

func TestEveryStartFailurePurgesBeforeRelease(t *testing.T) {
	for _, test := range []struct {
		name    string
		attempt int64
		class   string
		setup   func(*testing.T, fixture)
	}{
		{"nothing created", 1, "dispatch", func(t *testing.T, f fixture) { f.plugin.outcome = pluginsdk.OutcomeNothingCreated }},
		{"unknown and absent", 1, "dispatch", func(t *testing.T, f fixture) { f.plugin.dispatchErr = errors.New("register failed") }},
		{"unknown and never started", 1, "platform", func(t *testing.T, f fixture) {
			f.plugin.dispatchErr, f.plugin.job = errors.New("wait failed"), pluginsdk.JobNeverStarted
		}},
		{"adopted never started", 2, "platform", func(t *testing.T, f fixture) { f.plugin.job = pluginsdk.JobNeverStarted }},
		{"followed never started", 1, "platform", func(t *testing.T, f fixture) { f.plugin.stream, f.plugin.neverStarted = nil, true }},
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
			f.plugin.mu.Lock()
			calls := f.plugin.calls
			f.plugin.mu.Unlock()
			if !slices.Equal(calls, []string{"purge", "release"}) {
				t.Fatalf("calls %v, want purge then release", calls)
			}
		})
	}
}

func TestPurgeRetriedUntilItSucceeds(t *testing.T) {
	f := newFixture(t, resultStream)
	f.answer(t, credentialName)
	f.plugin.outcome, f.plugin.purgeFailures = pluginsdk.OutcomeNothingCreated, 2
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckRelease {
		t.Fatalf("status %v, want release", status)
	}
	f.plugin.mu.Lock()
	defer f.plugin.mu.Unlock()
	if f.plugin.purges != 3 || f.plugin.releases != 1 {
		t.Fatalf("purges=%d releases=%d, want 3 and 1", f.plugin.purges, f.plugin.releases)
	}
}

func TestPurgeFailingPastTheDeadlineFails(t *testing.T) {
	previous := purgeDeadline
	purgeDeadline = time.Second
	t.Cleanup(func() { purgeDeadline = previous })
	f := newFixture(t, resultStream)
	f.answer(t, credentialName)
	f.plugin.outcome, f.plugin.purgeErr = pluginsdk.OutcomeNothingCreated, errors.New("deregister denied")
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckReject {
		t.Fatalf("status %v, want reject", status)
	}
	if state, reason := terminalReason(t, f, "run-a"); state != wire.TaskStateFailed || reason != "start failed and the job could not be removed" {
		t.Fatalf("terminal %s %q", state, reason)
	}
	f.plugin.mu.Lock()
	defer f.plugin.mu.Unlock()
	if f.plugin.purges < 3 {
		t.Fatalf("purges %d, want retries every renew interval", f.plugin.purges)
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
			f.plugin.outcome = pluginsdk.OutcomeNothingCreated
			if status := f.execute(t, f.submit(t, f.host, nil), test.attempt); status != test.ack {
				t.Fatalf("status %v, want %v", status, test.ack)
			}
			if len(f.startFailures(t)) != 1 {
				t.Fatal("no start failure")
			}
			if test.ack != kgo.AckReject {
				return
			}
			if state, reason := terminalReason(t, f, "run-a"); state != wire.TaskStateFailed || reason != "delivery budget exhausted after 9 attempts; last: profile nomad created no job" {
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
			f.plugin.handleSize = test.size
			if status := f.execute(t, f.submit(t, f.host, nil), 1); status != test.ack {
				t.Fatalf("status %v, want %v", status, test.ack)
			}
			if test.ack == kgo.AckAccept {
				return
			}
			if _, _, follows, releases := f.plugin.counts(); follows != 0 || releases != 0 || slices.Contains(f.kinds(t), "started") {
				t.Fatalf("follows=%d releases=%d statuses %v, want the handle refused before started", follows, releases, f.kinds(t))
			}
			f.plugin.mu.Lock()
			defer f.plugin.mu.Unlock()
			if f.plugin.stops != 1 {
				t.Fatalf("stops %d, want the job behind the refused handle stopped", f.plugin.stops)
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
		{"adoption lookup", func(t *testing.T, f fixture) { f.plugin.lookupErr = errors.New("lookup failed") }, true},
		{"mismatch stop", func(t *testing.T, f fixture) {
			f.plugin.job, f.plugin.jobAgent, f.plugin.stopErr = pluginsdk.JobRunning, "codex", errors.New("stop failed")
		}, true},
		{"mismatch purge", func(t *testing.T, f fixture) {
			f.plugin.job, f.plugin.jobAgent, f.plugin.purgeErr = pluginsdk.JobRunning, "codex", errors.New("purge failed")
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
	f.plugin.endDelay = 30 * time.Second
	record := f.submit(t, f.host, nil)
	admitted := f.admitted(t, record)
	ctx, shutdown := context.WithCancel(f.ctx)
	defer shutdown()
	done := make(chan kgo.AckStatus, 1)
	go func() {
		done <- f.receiver.execute(ctx, f.service.Admin, record, admitted, int64(testTerms().DeliveryLimit)-1)
	}()
	for {
		if _, _, follows, _ := f.plugin.counts(); follows > 0 {
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

// A job is adopted by the profile whose name its platform metadata records: the Run.Profile it was dispatched with.
func TestAdoptionMatchesTheProfileNameInJobMetadata(t *testing.T) {
	m := newMultiFixture(t)
	if status := m.execute(t, "run-first", accelerated, 1); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	m.cloud.mu.Lock()
	dispatched := m.cloud.lastRun.Profile
	m.cloud.mu.Unlock()
	if dispatched != "cloud" {
		t.Fatalf("dispatched with profile %q, want cloud", dispatched)
	}
	m = newMultiFixture(t)
	m.cloud.job, m.cloud.jobProfile = pluginsdk.JobRunning, dispatched
	if status := m.execute(t, "run-adopted", accelerated, 2); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	if dispatches, _, follows, _ := m.cloud.counts(); dispatches != 0 || follows != 1 {
		t.Fatalf("cloud dispatches=%d follows=%d, want the job its metadata names cloud adopted", dispatches, follows)
	}
	if want := "run run-adopted: found attempt 1's job in profile cloud"; !strings.Contains(m.logs.String(), want) {
		t.Fatalf("logs lack %q:\n%s", want, m.logs.String())
	}
}

func TestJobOfAnotherProfileIsLeftToIt(t *testing.T) {
	m := newMultiFixture(t)
	// lab and cloud share one backend: both find the job, labelled with cloud.
	m.lab.job, m.lab.jobProfile = pluginsdk.JobRunning, "cloud"
	m.cloud.job = pluginsdk.JobRunning
	m.answer(t, "cred-cloud")
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
	f.plugin.outcome = pluginsdk.OutcomeNothingCreated
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckRelease {
		t.Fatalf("status %v, want release", status)
	}
	if failures := f.startFailures(t); len(failures) != 1 || failures[0].Class != "dispatch" {
		t.Fatalf("start failures %#v, want one dispatch", failures)
	}
	if _, lookups, _, _ := f.plugin.counts(); lookups != 0 {
		t.Fatalf("lookups %d, want none for an outcome the plugin knows", lookups)
	}
}
