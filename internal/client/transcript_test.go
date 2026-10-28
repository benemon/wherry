package client

import (
	"context"
	"fmt"
	"io"
	"log"
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

// A stream of ticks over a live run fetches each of its transcript records once, where a Transcript each tick fetches
// the transcript topic from the run's echo again, and each tick returns only the lines committed since the last.
func TestTranscriptStreamFetchesEachChunkOnce(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	topic := admin.TenantTranscriptTopic("tenant-a")
	var counted traffic
	stream := f.countedReader(t, &counted).StreamTranscript(runID)
	const ticks = 8
	for tick := range int64(ticks) {
		receiver.feed(t, f.ctx, runID, receiver.chunk(t, task, 1, tick*5))
		got, err := stream.Next(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Lines) != 1 || got.Lines[0].Seq != tick*5 {
			t.Fatalf("tick %d returned %+v; want the line at %d alone", tick, got.Lines, tick*5)
		}
	}
	if got, err := stream.Next(f.ctx); err != nil || len(got.Lines) != 0 {
		t.Fatalf("a tick past the last chunk returned %+v, %v; want nothing", got.Lines, err)
	}
	// Each transaction appends its chunk and a commit marker.
	end := f.endOffset(t, topic)
	t.Logf("stream: %d ticks over %d chunks fetched %d of %d transcript records in %d fetch requests and %d listings", ticks+1, ticks, counted.records(topic), end, counted.counts().fetches, counted.counts().listings)
	if n := counted.records(topic); n != end {
		t.Fatalf("%d ticks fetched %d transcript records; want each of the %d once", ticks+1, n, end)
	}
}

// A stream over an adopted run returns the second attempt's lines as they commit. A line the first attempt wrote at a
// seq the adopter writes again stays returned, while Transcript's view, as transcript.jsonl holds it, takes the
// adopter's.
func TestTranscriptStreamOfAnAdoptedRun(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	stream := f.host.StreamTranscript(runID)
	type line struct{ attempt, seq int64 }
	next := func() []line {
		t.Helper()
		got, err := stream.Next(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		var lines []line
		for _, l := range got.Lines {
			lines = append(lines, line{l.Attempt, l.Seq})
		}
		return lines
	}
	receiver.feed(t, f.ctx, runID, receiver.chunk(t, task, 1, 0), receiver.chunk(t, task, 1, 5), receiver.chunk(t, task, 1, 10))
	if got, want := next(), []line{{1, 0}, {1, 5}, {1, 10}}; !slices.Equal(got, want) {
		t.Fatalf("first attempt streamed %v, want %v", got, want)
	}
	receiver.feed(t, f.ctx, runID, receiver.status(t, task, 2, "claimed-2", "claimed", wire.TaskStateWorking, nil), receiver.chunk(t, task, 2, 10), receiver.chunk(t, task, 2, 15))
	if got, want := next(), []line{{2, 10}, {2, 15}}; !slices.Equal(got, want) {
		t.Fatalf("second attempt streamed %v, want %v", got, want)
	}
	receiver.feed(t, f.ctx, runID, receiver.chunk(t, task, 2, 20), receiver.chunk(t, task, 1, 25))
	if got, want := next(), []line{{2, 20}}; !slices.Equal(got, want) {
		t.Fatalf("after adoption streamed %v, want %v without the first attempt's later line", got, want)
	}
	transcript, err := f.host.Transcript(f.ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	var view []line
	for _, l := range transcript.Lines {
		view = append(view, line{l.Attempt, l.Seq})
	}
	if want := []line{{1, 0}, {1, 5}, {2, 10}, {2, 15}, {2, 20}}; !slices.Equal(view, want) {
		t.Fatalf("transcript %v, want %v", view, want)
	}
}

// A stream followed as watch follows it, Next after each Wait, from before the run's echo, fetches each transcript
// record once and returns each line within one fetch wait of its commit.
func TestTranscriptStreamTailsTheTopic(t *testing.T) {
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	runID := f.submit(t, defaultSelector()).RunID
	task := receiver.claim(t, f.ctx, runID)[0]
	topic := admin.TenantTranscriptTopic("tenant-a")
	bundle := filepath.Join(t.TempDir(), "bundle.yaml")
	if err := f.service.HostAdd(f.ctx, "tenant-a", "host-tail", bundle); err != nil {
		t.Fatal(err)
	}
	// Long enough that a line returned before it ends came from a fetch the commit woke.
	const fetchWait = 2 * time.Second
	var counted traffic
	kafka, principal := f.connect(t, bundle, kgo.WithHooks(&counted), kgo.FetchMaxWait(fetchWait))
	host, err := NewHost(kafka, f.host.key, principal, f.host.stateDir, nil, nil, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(host.Close)
	ctx, cancel := context.WithCancel(f.ctx)
	stream := host.StreamTranscript(runID)
	if got, err := stream.Next(f.ctx); err != nil || len(got.Lines) != 0 {
		t.Fatalf("before the echo the stream returned %+v, %v; want nothing", got.Lines, err)
	}
	lines := make(chan TranscriptLine, 16)
	followed := make(chan struct{})
	go func() {
		defer close(followed)
		for ctx.Err() == nil {
			if got, err := stream.Next(ctx); err == nil {
				for _, line := range got.Lines {
					lines <- line
				}
			}
			stream.Wait(ctx)
		}
	}()
	receiver.feed(t, f.ctx, runID, receiver.echo(t, task), receiver.status(t, task, 1, "claimed-1", "claimed", wire.TaskStateWorking, nil))
	const chunks = 8
	var slowest time.Duration
	for i := range int64(chunks) {
		written := time.Now()
		receiver.feed(t, f.ctx, runID, receiver.chunk(t, task, 1, i*5))
		select {
		case line := <-lines:
			waited := time.Since(written)
			slowest = max(slowest, waited)
			if line.Seq != i*5 || waited >= fetchWait {
				t.Fatalf("line %d at %d after %v; want the line at %d within %v", i, line.Seq, waited, i*5, fetchWait)
			}
		case <-time.After(5 * fetchWait):
			t.Fatalf("line %d not returned", i)
		}
	}
	cancel()
	<-followed
	end := f.endOffset(t, topic)
	t.Logf("tail: %d chunks fetched %d of %d transcript records in %d fetch requests and %d listings; slowest line %v", chunks, counted.records(topic), end, counted.counts().fetches, counted.counts().listings, slowest)
	if n := counted.records(topic); n != end {
		t.Fatalf("%d chunks fetched %d transcript records; want each of the %d once", chunks, n, end)
	}
	// A Next lists the transcript topic's end once for each wake, a chunk and its commit marker at most two.
	if n := counted.counts().listings; n > 4*chunks {
		t.Fatalf("%d chunks took %d listings; want at most %d", chunks, n, 4*chunks)
	}
}
