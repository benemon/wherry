package client

import (
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// endedRun submits a run through receiver-a and writes its echo and claimed status, then lines transcript chunks over
// two transactions, its result and a terminal status counting them.
func (f *fixture) endedRun(t *testing.T, lines int) claimed {
	t.Helper()
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	var chunks [][]byte
	for i := range lines {
		chunks = append(chunks, receiver.chunk(t, task, 1, int64(i*5)))
	}
	receiver.feed(t, f.ctx, runID, chunks[:lines/2]...)
	receiver.feed(t, f.ctx, runID, append(chunks[lines/2:], receiver.result(t, task, 1, wire.TaskStateCompleted, nil), receiver.terminal(t, task, 1, wire.TaskStateCompleted, int64(lines)))...)
	return task
}

// end lists topic's end offset.
func (f *fixture) endOffset(t *testing.T, topic string) int64 {
	t.Helper()
	ends, err := kadm.NewClient(f.service.Admin).ListEndOffsets(f.ctx, topic)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		t.Fatal(err)
	}
	var end int64
	ends.Each(func(listed kadm.ListedOffset) { end = listed.Offset })
	return end
}

// countedReader is a host of tenant-a sharing f.host's local records, connected through a client counted by counted.
func (f *fixture) countedReader(t *testing.T, counted *traffic) *Host {
	t.Helper()
	bundle := filepath.Join(t.TempDir(), "bundle.yaml")
	if err := f.service.HostAdd(f.ctx, "tenant-a", fmt.Sprintf("host-%d", time.Now().UnixNano()%1e6), bundle); err != nil {
		t.Fatal(err)
	}
	return f.countedHost(t, bundle, f.host.stateDir, counted)
}

// Status and events of an ended run read only the state records on the feed from the run's submission, whatever the
// later runs wrote to the transcript topic.
func TestStatusAndEventsOfAnEndedRunFetchNoTranscriptRecords(t *testing.T) {
	f := newFixture(t)
	task := f.endedRun(t, 3)
	runID := task.submission.Wherry.Run
	for range 5 {
		f.endedRun(t, 20)
	}
	feed, transcript := admin.TenantTopic("tenant-a"), admin.TenantTranscriptTopic("tenant-a")
	for _, record := range mustRead(t, f, feed) {
		if typ, _ := header(record.Value); typ == wire.ChunkType || typ == wire.OutputType {
			t.Fatalf("a %s record on the feed", typ)
		}
	}
	local, err := f.host.localRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	var counted traffic
	h := f.countedReader(t, &counted)
	status, err := h.Status(f.ctx, runID)
	if err != nil || status.Status.State != wire.TaskStateCompleted {
		t.Fatalf("status %s, %v; want COMPLETED", status.Status.State, err)
	}
	t.Logf("status: %d feed and %d transcript records fetched", counted.records(feed), counted.records(transcript))
	if n, want := counted.records(feed), f.endOffset(t, feed)-local.FeedOffset; counted.records(transcript) != 0 || n != want {
		t.Fatalf("status fetched %d feed and %d transcript records; want the %d feed offsets from the run's submission and none", n, counted.records(transcript), want)
	}
	counted = traffic{}
	events, err := h.Events(f.ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("events: %d feed and %d transcript records fetched", counted.records(feed), counted.records(transcript))
	if n := counted.records(transcript); n != 0 || len(events.Chunks) != 1 || events.Chunks[1] != 3 {
		t.Fatalf("events fetched %d transcript records and counted chunks %v; want none and the terminal's 3", n, events.Chunks)
	}
}

// A transcript read of an older run fetches the transcript topic from the run's first chunk, appended after its echo,
// and none of the earlier runs' records.
func TestTranscriptOfAnOlderRunFetchesFromItsEarliestEcho(t *testing.T) {
	// kfake runs one clock, so the skew allowance would only reach back to the earlier runs.
	skew := transcriptSkew
	transcriptSkew = 0
	t.Cleanup(func() { transcriptSkew = skew })
	f := newFixture(t)
	for range 3 {
		f.endedRun(t, 20)
	}
	// Log append times are milliseconds; the run's echo is appended in a later one than the earlier runs' records.
	time.Sleep(10 * time.Millisecond)
	task := f.endedRun(t, 3)
	runID := task.submission.Wherry.Run
	for range 5 {
		f.endedRun(t, 20)
	}
	topic := admin.TenantTranscriptTopic("tenant-a")
	records := mustRead(t, f, topic)
	first := slices.IndexFunc(records, func(record *kgo.Record) bool { return string(record.Key) == runID })
	end := f.endOffset(t, topic)
	var counted traffic
	transcript, err := f.countedReader(t, &counted).Transcript(f.ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("transcript: %d of %d transcript records fetched", counted.records(topic), end)
	if n, want := counted.records(topic), end-records[first].Offset; len(transcript.Lines) != 3 || n != want {
		t.Fatalf("transcript of %d lines fetched %d transcript records; want 3 lines and the %d records from the run's first chunk", len(transcript.Lines), n, want)
	}
}

// A terminal status visible on the feed before its attempt's last chunk is on the transcript topic does not end the
// transcript: the read waits for the transcript topic's last stable offset to pass it.
func TestTranscriptWaitsForChunksCommittedApartFromTheTerminal(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	receiver.feed(t, f.ctx, runID, receiver.chunk(t, task, 1, 0))
	producer, err := kgo.NewClient(append(receiver.kafka.Opts(), kgo.TransactionalID(admin.TransactionalPrefix(receiver.pool)+"window"))...)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if err := producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := producer.ProduceSync(f.ctx, &kgo.Record{Topic: admin.TenantTranscriptTopic("tenant-a"), Key: []byte(runID), Value: receiver.chunk(t, task, 1, 5)}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	receiver.feed(t, f.ctx, runID, receiver.result(t, task, 1, wire.TaskStateCompleted, nil), receiver.terminal(t, task, 1, wire.TaskStateCompleted, 2))
	type read struct {
		transcript TranscriptResult
		err        error
	}
	done := make(chan read, 1)
	go func() {
		transcript, err := f.host.Transcript(f.ctx, runID)
		done <- read{transcript, err}
	}()
	select {
	case got := <-done:
		t.Fatalf("transcript of %d lines, %v, returned before the last chunk committed", len(got.transcript.Lines), got.err)
	case <-time.After(500 * time.Millisecond):
	}
	if err := producer.EndTransaction(f.ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}
	got := <-done
	if got.err != nil || len(got.transcript.Lines) != 2 || got.transcript.Lines[1].Seq != 5 {
		t.Fatalf("transcript %+v, %v; want both lines", got.transcript.Lines, got.err)
	}
}

func mustRead(t *testing.T, f *fixture, topic string) []*kgo.Record {
	t.Helper()
	records, err := readTopic(f.ctx, f.service.Admin, topic)
	if err != nil {
		t.Fatal(err)
	}
	return records
}
