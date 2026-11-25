package view

import (
	"context"
	"net/http"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// page is a transcript request against v, failing on error.
func page(t *testing.T, v *View, run string, p Page) Transcript {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	transcript, err := v.transcripts.Transcript(ctx, run, p)
	if err != nil {
		t.Fatalf("transcript %+v: %v", p, err)
	}
	return transcript
}

func seqs(lines []Line) []int64 {
	var out []int64
	for _, line := range lines {
		out = append(out, line.Seq)
	}
	return out
}

func ptr(n int64) *int64 { return &n }

// cursor is p with the cursor of the last response.
func cursor(p Page, last Transcript) Page {
	p.Attempt, p.First = last.Attempt, last.First
	return p
}

// workingRun echoes and claims a run of tenant-a on receiver-a at t0 and returns its ID.
func (f *fixture) workingRun(t0 time.Time, name string) string {
	f.t.Helper()
	run := f.echo(t0, receiverA, tenantA, hostA, poolA, name)
	f.status(t0.Add(time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "claimed", state: wire.TaskStateWorking})
	return run
}

// Test 2 and 10: a 10,000-line transcript pages by tail, before and from without gaps or repeats; the index is built
// once, so a second request's scan reads no record; the first open's time and memory are the baseline.
func TestTranscriptPaging(t *testing.T) {
	f := newFixture(t)
	t0 := f.now.Add(-20 * time.Minute)
	run := f.workingRun(t0, "paged")
	const n = 10000
	f.lines(t0.Add(2*time.Second), receiverA, tenantA, poolA, run, 1, n)
	f.chunk(t0.Add(time.Minute), ch{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, seq: n * 10, last: true})
	f.status(t0.Add(time.Minute), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "terminal", state: wire.TaskStateCompleted, chunks: n + 1})
	v := f.view(time.Hour, transcriptCaps())
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	started := time.Now()
	tail := page(t, v, run, Page{Tail: 1000})
	opened := time.Since(started)
	runtime.ReadMemStats(&after)
	t.Logf("baseline: first open of %d lines (tail=1000) took %s, scan read %d records and %d bytes, seek read %d records and %d bytes, heap grew %d KiB, index %d entries of %d bytes",
		n, opened.Truncate(time.Millisecond), tail.Read.Records, tail.Read.Bytes, tail.Seek.Records, tail.Seek.Bytes, (int64(after.HeapAlloc)-int64(before.HeapAlloc))/1024, tail.Indexed, tail.Indexed*24)
	if len(tail.Lines) != 1000 || tail.Lines[0].Seq != (n-1000)*10 || tail.Lines[999].Seq != (n-1)*10 || tail.Prev == nil || *tail.Prev != (n-1000)*10 || tail.Next != nil || !tail.Complete || tail.Attempt != 1 || tail.First == nil || *tail.First != 0 || tail.Indexed != n || tail.PredatesLog {
		t.Fatalf("tail: %d lines %v..%v prev %v next %v complete %v attempt %d first %v indexed %d", len(tail.Lines), seqs(tail.Lines[:min(2, len(tail.Lines))]), seqs(tail.Lines[max(len(tail.Lines)-2, 0):]), tail.Prev, tail.Next, tail.Complete, tail.Attempt, tail.First, tail.Indexed)
	}
	if text := tail.Lines[0].Text; text == nil || *text != "line "+strconv.FormatInt((n-1000)*10, 10) || tail.Lines[0].Receiver != operator.ReceiverPrincipal(receiverA) || tail.Lines[0].Attempt != 1 {
		t.Errorf("first line of the tail: %+v", tail.Lines[0])
	}
	// Older pages by before, down to the start.
	collected := seqs(tail.Lines)
	last := tail
	for last.Prev != nil {
		older := page(t, v, run, cursor(Page{Before: last.Prev, Limit: 1000}, last))
		if older.Read.Records != 0 || older.Reset {
			t.Fatalf("a later request scanned %d records, reset %v", older.Read.Records, older.Reset)
		}
		if len(older.Lines) == 0 {
			t.Fatalf("before %d: no lines", *last.Prev)
		}
		collected = append(seqs(older.Lines), collected...)
		last = older
	}
	var want []int64
	for i := range n {
		want = append(want, int64(i)*10)
	}
	if !slices.Equal(collected, want) {
		t.Fatalf("before pages: %d lines, first %v last %v; want %d without gaps or repeats", len(collected), collected[:min(3, len(collected))], collected[max(len(collected)-3, 0):], n)
	}
	// Forward by from, from the start, to the end.
	var forward []int64
	next := ptr(0)
	last = tail
	for next != nil {
		p := page(t, v, run, cursor(Page{From: next, Limit: 1000}, last))
		if p.Read.Records != 0 || p.Reset {
			t.Fatalf("from %d scanned %d records, reset %v", *next, p.Read.Records, p.Reset)
		}
		forward = append(forward, seqs(p.Lines)...)
		next, last = p.Next, p
	}
	if !slices.Equal(forward, want) {
		t.Fatalf("from pages: %d lines; want %d without gaps or repeats", len(forward), n)
	}
	d, _ := v.RunDetail(run)
	if len(d.Attempts) != 1 || d.Attempts[0].TranscriptChunks != n+1 {
		t.Errorf("transcript_chunks: %+v", d.Attempts)
	}
}

// Test 3: a chunk signed by another key than its receiver's, or naming another pool, task hash or tenant, is dropped; an
// output chunk on the topic is dropped by type before it is verified and never shown; a key's retirement, read after
// its chunks were indexed, drops the index, which the next request rebuilds without them.
func TestTranscriptAuthorisation(t *testing.T) {
	f := newFixture(t)
	if err := f.service.PoolApply(f.ctx, poolB, testTerms(bucketB), false); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantPoolAdd(f.ctx, tenantA, poolB); err != nil {
		t.Fatal(err)
	}
	f.addReceiver(receiverC, poolB)
	t0 := f.now.Add(-20 * time.Minute)
	run := f.workingRun(t0, "guarded")
	at := t0.Add(2 * time.Second)
	f.chunk(at, ch{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, seq: 0})
	// Signed by receiver-b, naming receiver-a; by receiver-c of pool-b; another task hash; another tenant.
	f.chunk(at.Add(time.Second), ch{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, seq: 10, key: f.keys[receiverB], part: &wire.Part{Text: "s3cr3t-other-signer"}})
	f.chunk(at.Add(2*time.Second), ch{receiver: receiverC, tenant: tenantA, pool: poolB, run: run, seq: 20, part: &wire.Part{Text: "s3cr3t-other-pool"}})
	f.chunk(at.Add(3*time.Second), ch{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, seq: 30, hash: hashOf("other"), part: &wire.Part{Text: "s3cr3t-other-hash"}})
	f.produce(operator.TenantTranscriptTopic(tenantA), run, f.chunkValue(ch{receiver: receiverA, tenant: tenantB, pool: poolA, run: run, seq: 40, part: &wire.Part{Text: "s3cr3t-other-tenant"}}), at.Add(4*time.Second))
	f.output(at.Add(5*time.Second), receiverA, tenantA, poolA, run, "notes.txt", []byte("s3cr3t-output-bytes"))
	f.output(at.Add(5*time.Second), receiverA, tenantA, poolA, run, "more.txt", []byte("s3cr3t-output-bytes"))
	f.chunk(at.Add(6*time.Second), ch{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, seq: 50})
	v := f.view(time.Hour, transcriptCaps())
	tail := page(t, v, run, Page{Tail: 100})
	if got := seqs(tail.Lines); !slices.Equal(got, []int64{0, 50}) {
		t.Fatalf("lines %v; want the two chunks receiver-a signed for the run's pool, task and tenant", got)
	}
	encoded := strings.Join(func() []string {
		var texts []string
		for _, line := range tail.Lines {
			texts = append(texts, *line.Text)
		}
		return texts
	}(), "\n")
	if strings.Contains(encoded, "s3cr3t") {
		t.Errorf("a dropped chunk's text is shown: %s", encoded)
	}
	counts := v.Snapshot(0).Counts
	if counts["transcript_skipped"] != 2 || counts["transcript_refused"] != 4 {
		t.Errorf("counts %v; want the two outputs skipped by type before verifying and four chunks refused", counts)
	}
	// A rotation appended before a chunk signed with the key it retired, read only after the chunk was indexed.
	rotatedAt := f.now.Add(time.Minute)
	f.chunk(rotatedAt.Add(time.Second), ch{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, seq: 60, part: &wire.Part{Text: "s3cr3t-retired-key"}})
	if got := seqs(page(t, v, run, Page{Tail: 100}).Lines); !slices.Equal(got, []int64{0, 50, 60}) {
		t.Fatalf("before the rotation is read: %v", got)
	}
	f.rotate(receiverA, operator.ReceiverTopic(receiverA), rotatedAt)
	f.tick(v)
	after := page(t, v, run, Page{Tail: 100})
	if got := seqs(after.Lines); !slices.Equal(got, []int64{0, 50}) || after.Read.Records == 0 {
		t.Errorf("after the rotation is read: %v, scan read %d records; want the retired key's chunk retracted by a rebuild", got, after.Read.Records)
	}
	if counts := v.Snapshot(0).Counts; counts["transcript_retracted"] != 1 {
		t.Errorf("counts %v; want one index retracted", counts)
	}
}

// Test 1 for the view and test 4: the lines shown are the highest attempt's and, below its first seq, the earlier
// attempt's; a new attempt mid-follow answers the cursor with a reset, and the tail reloaded shows the adopter's lines
// in place of the earlier attempt's from its first seq.
func TestTranscriptAdoptionResets(t *testing.T) {
	f := newFixture(t)
	t0 := f.now.Add(-20 * time.Minute)
	run := f.workingRun(t0, "adopted")
	for _, seq := range []int64{0, 10, 20, 30} {
		f.chunk(t0.Add(2*time.Second), ch{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, seq: seq})
	}
	v := f.view(time.Hour, transcriptCaps())
	tail := page(t, v, run, Page{Tail: 100})
	if got := seqs(tail.Lines); !slices.Equal(got, []int64{0, 10, 20, 30}) || tail.Attempt != 1 || tail.Complete || tail.Next == nil || *tail.Next != 31 || tail.PredatesLog {
		t.Fatalf("attempt 1: %v attempt %d complete %v next %v", got, tail.Attempt, tail.Complete, tail.Next)
	}
	// Following: nothing new.
	follow := page(t, v, run, cursor(Page{From: tail.Next, Limit: 100}, tail))
	if len(follow.Lines) != 0 || follow.Reset || follow.Next == nil || *follow.Next != 31 {
		t.Fatalf("follow with nothing new: %+v", follow)
	}
	// Receiver-b adopts at attempt 2, resuming from seq 20.
	f.status(f.now, st{receiver: receiverB, tenant: tenantA, pool: poolA, run: run, attempt: 2, kind: "started", state: wire.TaskStateWorking})
	f.tick(v)
	for _, seq := range []int64{20, 40} {
		f.chunk(f.now.Add(time.Second), ch{receiver: receiverB, tenant: tenantA, pool: poolA, run: run, attempt: 2, seq: seq})
	}
	reset := page(t, v, run, cursor(Page{From: follow.Next, Limit: 100}, follow))
	if !reset.Reset || len(reset.Lines) != 0 || reset.Attempt != 2 || reset.First == nil || *reset.First != 20 {
		t.Fatalf("mid-follow adoption: %+v", reset)
	}
	reloaded := page(t, v, run, Page{Tail: 100})
	want := []struct{ seq, attempt int64 }{{0, 1}, {10, 1}, {20, 2}, {40, 2}}
	if len(reloaded.Lines) != len(want) {
		t.Fatalf("reloaded tail: %v", seqs(reloaded.Lines))
	}
	for i, line := range reloaded.Lines {
		if line.Seq != want[i].seq || line.Attempt != want[i].attempt {
			t.Errorf("line %d: seq %d attempt %d, want %+v", i, line.Seq, line.Attempt, want[i])
		}
	}
	if reloaded.Lines[2].Receiver != operator.ReceiverPrincipal(receiverB) || reloaded.Lines[0].Receiver != operator.ReceiverPrincipal(receiverA) {
		t.Errorf("signers: %s %s", reloaded.Lines[0].Receiver, reloaded.Lines[2].Receiver)
	}
	// The adopter's last chunk completes the transcript.
	f.chunk(f.now.Add(2*time.Second), ch{receiver: receiverB, tenant: tenantA, pool: poolA, run: run, attempt: 2, seq: 50, last: true})
	if done := page(t, v, run, cursor(Page{From: reloaded.Next, Limit: 100}, reloaded)); !done.Complete || len(done.Lines) != 0 || done.Reset || done.Next != nil {
		t.Errorf("after the last chunk: %+v", done)
	}
}

// Test 5: a 512 KiB line is cut to 16 KiB and marked with its size; a tail of large lines stops under the response
// bound, naming it, with next at the cut; the line, byte and record caps each stop a scan, each named.
func TestTranscriptBounds(t *testing.T) {
	f := newFixture(t)
	t0 := f.now.Add(-20 * time.Minute)
	run := f.workingRun(t0, "bounded")
	huge := strings.Repeat("h", wire.MaxLine)
	f.chunk(t0.Add(2*time.Second), ch{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, seq: 0, part: &wire.Part{Text: huge}})
	large := strings.Repeat("l", 20<<10)
	var records []*kgo.Record
	for i := 1; i <= 1000; i++ {
		records = append(records, &kgo.Record{Topic: operator.TenantTranscriptTopic(tenantA), Key: []byte(run), Value: f.chunkValue(ch{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, seq: int64(i) * 100000, part: &wire.Part{Text: large}}), Timestamp: t0.Add(3 * time.Second)})
	}
	f.produceMany(records)
	caps := transcriptCaps()
	caps.TranscriptBytes = 256 << 20
	v := f.view(time.Hour, caps)
	first := page(t, v, run, Page{From: ptr(0), Limit: 1, Attempt: 1, First: ptr(0)})
	if len(first.Lines) != 1 || !first.Lines[0].Truncated || first.Lines[0].Size != wire.MaxLine || first.Lines[0].Text == nil || len(*first.Lines[0].Text) != MaxLineText {
		t.Fatalf("the 512 KiB line: %d lines, %+v", len(first.Lines), first.Lines)
	}
	tail := page(t, v, run, Page{Tail: 1000})
	if !slices.Contains(tail.Truncated, capResponse) || len(tail.Lines) == 0 || len(tail.Lines) >= 1000 || tail.Next == nil || *tail.Next != tail.Lines[len(tail.Lines)-1].Seq+100000 {
		t.Errorf("tail of large lines: %d lines, truncated %v, next %v", len(tail.Lines), tail.Truncated, tail.Next)
	}
	if tail.Lines[0].Seq != 100000 {
		t.Errorf("the tail's window starts at %d; want the 1,000 large lines", tail.Lines[0].Seq)
	}
	// Each scan cap, on a fresh view.
	for _, c := range []struct {
		name string
		set  func(*Caps)
		ok   func(Transcript) bool
	}{
		{capTranscriptLines, func(c *Caps) { c.TranscriptLines = 100 }, func(p Transcript) bool { return p.Indexed == 100 }},
		// The first record, the 512 KiB line, is over the budget alone and is still read, as the first of a batch.
		{capTranscriptBytes, func(c *Caps) { c.TranscriptBytes = 200 << 10 }, func(p Transcript) bool { return p.Read.Records < 20 && p.Indexed < 20 }},
		{capTranscriptRecords, func(c *Caps) { c.TranscriptRecords = 50 }, func(p Transcript) bool { return p.Read.Records == 50 }},
	} {
		small := transcriptCaps()
		c.set(&small)
		w := New(f.client, Options{Window: time.Hour, Caps: small, Now: f.clock})
		t.Cleanup(w.Close)
		if err := w.Rebuild(f.ctx); err != nil {
			t.Fatal(err)
		}
		p := page(t, w, run, Page{Tail: 10})
		if !slices.Contains(p.Truncated, c.name) || !c.ok(p) || p.Complete {
			t.Errorf("%s: truncated %v, indexed %d, read %+v, complete %v", c.name, p.Truncated, p.Indexed, p.Read, p.Complete)
		}
		if hits := w.Snapshot(0).Counts[c.name]; hits == 0 {
			t.Errorf("%s did not report", c.name)
		}
	}
}

// Test 6: message bytes count in the run's retained size, so 2,000 working statuses of 512 B pass run_bytes and drop
// the oldest events.
func TestMessagesCountInRunBytes(t *testing.T) {
	f := newFixture(t)
	t0 := f.now.Add(-30 * time.Minute)
	run := f.workingRun(t0, "chatty")
	var records []*kgo.Record
	line := strings.Repeat("w", wire.MaxWorkingText-len(" step"))
	for i := range 2000 {
		at := t0.Add(2*time.Second + time.Duration(i)*time.Millisecond)
		value := f.statusRecord(at, st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "working", state: wire.TaskStateWorking, text: line})
		records = append(records, &kgo.Record{Topic: operator.PoolEventsTopic(poolA), Key: []byte(run), Value: value, Timestamp: at}, &kgo.Record{Topic: operator.TenantTopic(tenantA), Key: []byte(run), Value: value, Timestamp: at})
	}
	f.produceMany(records)
	caps := transcriptCaps()
	caps.RunEvents = 5000
	v := f.view(time.Hour, caps)
	s := v.Snapshot(0)
	r := summaryOf(t, s, run)
	d, _ := v.RunDetail(run)
	events := len(d.Attempts[0].Events)
	if !slices.Contains(r.Truncated, capRunBytes) || s.Counts[capRunBytes] == 0 || events >= 2001 || events < 100 {
		t.Fatalf("run truncated %v, %d events kept, run_bytes hits %d; want the oldest dropped under run_bytes", r.Truncated, events, s.Counts[capRunBytes])
	}
	last := d.Attempts[0].Events[events-1]
	if last.Kind != "working" || last.Message != line+" step" || last.Truncated {
		t.Errorf("the newest event: %+v", last)
	}
	t.Logf("%d of 2,001 events kept under run_bytes", events)
}

// Test 7: a raw part that is valid UTF-8 and an empty line are text; invalid UTF-8 is raw with its size; an oversize
// reference is marked with its size.
func TestTranscriptLineKinds(t *testing.T) {
	f := newFixture(t)
	t0 := f.now.Add(-20 * time.Minute)
	run := f.workingRun(t0, "kinds")
	at := t0.Add(2 * time.Second)
	f.chunk(at, ch{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, seq: 0, part: &wire.Part{Raw: wire.StdBytes("\x1b[31mred\x1b[0m"), MediaType: "application/x-ndjson"}})
	f.chunk(at, ch{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, seq: 10, part: &wire.Part{Raw: wire.StdBytes{}, MediaType: "application/x-ndjson"}})
	f.chunk(at, ch{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, seq: 20, part: &wire.Part{Raw: wire.StdBytes{0xff, 0xfe, 0x00}, MediaType: "application/x-ndjson"}})
	f.chunk(at, ch{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, seq: 30, part: &wire.Part{Data: []byte(`{"sha256":"` + hashOf("big") + `","size":700000,"unstored":"oversize"}`), MediaType: wire.ReferenceMediaType}})
	f.chunk(at, ch{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, seq: 40, part: &wire.Part{Text: "plain"}})
	v := f.view(time.Hour, transcriptCaps())
	p := page(t, v, run, Page{Tail: 10})
	if len(p.Lines) != 5 {
		t.Fatalf("lines: %v", seqs(p.Lines))
	}
	lines := p.Lines
	if lines[0].Text == nil || *lines[0].Text != "\x1b[31mred\x1b[0m" || lines[0].Raw || lines[1].Text == nil || *lines[1].Text != "" || lines[1].Raw {
		t.Errorf("valid UTF-8 raw parts: %+v %+v", lines[0], lines[1])
	}
	if !lines[2].Raw || lines[2].Size != 3 || lines[2].Text != nil {
		t.Errorf("invalid UTF-8: %+v", lines[2])
	}
	if !lines[3].Oversize || lines[3].Size != 700000 || lines[3].Text != nil {
		t.Errorf("oversize: %+v", lines[3])
	}
	if lines[4].Text == nil || *lines[4].Text != "plain" {
		t.Errorf("text: %+v", lines[4])
	}
}

// Test 8: when retention removed the span's start, the response says so.
func TestTranscriptPredatesLog(t *testing.T) {
	f := newFixture(t)
	t0 := f.now.Add(-20 * time.Minute)
	run := f.workingRun(t0, "expired")
	topic := operator.TenantTranscriptTopic(tenantA)
	var kept *kgo.Record
	for i, seq := range []int64{0, 10, 20, 30} {
		record := f.chunk(t0.Add(2*time.Second+time.Duration(i)*time.Second), ch{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, seq: seq})
		if seq == 20 {
			kept = record
		}
	}
	if err := f.cluster.DeleteRecords(topic, 0, kept.Offset); err != nil {
		t.Fatal(err)
	}
	v := f.view(time.Hour, transcriptCaps())
	p := page(t, v, run, Page{Tail: 10})
	if !p.PredatesLog || !slices.Equal(seqs(p.Lines), []int64{20, 30}) {
		t.Errorf("after retention: predates_log %v lines %v", p.PredatesLog, seqs(p.Lines))
	}
	// A run whose span is intact does not say so.
	intact := f.workingRun(f.now.Add(-time.Minute), "intact")
	f.chunk(f.now, ch{receiver: receiverA, tenant: tenantA, pool: poolA, run: intact, seq: 0})
	f.tick(v)
	if p := page(t, v, intact, Page{Tail: 10}); p.PredatesLog || len(p.Lines) != 1 {
		t.Errorf("intact span: %+v", p)
	}
}

// Test 9: concurrent requests for the same run, under the race detector when run with -race.
func TestTranscriptConcurrentRequests(t *testing.T) {
	f := newFixture(t)
	t0 := f.now.Add(-20 * time.Minute)
	run := f.workingRun(t0, "shared")
	f.lines(t0.Add(2*time.Second), receiverA, tenantA, poolA, run, 1, 500)
	caps := transcriptCaps()
	caps.TranscriptScans = 8
	v := f.view(time.Hour, caps)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 5 {
				p := Page{Tail: 50}
				if (i+j)%2 == 1 {
					p = Page{From: ptr(int64(j) * 1000), Limit: 50, Attempt: 1, First: ptr(0)}
				}
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				transcript, err := v.transcripts.Transcript(ctx, run, p)
				cancel()
				if err != nil {
					t.Errorf("request %+v: %v", p, err)
					return
				}
				if transcript.Indexed != 500 || len(transcript.Lines) == 0 {
					t.Errorf("request %+v: indexed %d, %d lines", p, transcript.Indexed, len(transcript.Lines))
				}
			}
		}()
	}
	wg.Wait()
	// A rotation read concurrently with requests.
	wg.Add(2)
	go func() {
		defer wg.Done()
		f.rotate(receiverA, operator.ReceiverTopic(receiverA), f.now)
		f.tick(v)
	}()
	go func() {
		defer wg.Done()
		for range 5 {
			page(t, v, run, Page{Tail: 50})
		}
	}()
	wg.Wait()
}

// Item 1: the model keeps a working status's line and a failed, canceled or rejected terminal's reason, cut at 1 KiB
// and marked, never a completed terminal's text, and the run detail carries the failure reason.
func TestStatusMessagesInRunDetail(t *testing.T) {
	f := newFixture(t)
	t0 := f.now.Add(-20 * time.Minute)
	failed := f.workingRun(t0, "failed")
	f.status(t0.Add(2*time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: failed, kind: "working", state: wire.TaskStateWorking, text: "compiling module"})
	long := strings.Repeat("é", 1000)
	f.status(t0.Add(3*time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: failed, kind: "terminal", state: wire.TaskStateFailed, text: long, chunks: 3})
	rejected := f.echo(t0.Add(4*time.Second), receiverA, tenantA, hostA, poolA, "rejected")
	f.status(t0.Add(5*time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: rejected, kind: "rejected", state: wire.TaskStateRejected, text: "no profile offers the agent"})
	completed := f.workingRun(t0.Add(6*time.Second), "completed")
	f.status(t0.Add(8*time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: completed, kind: "terminal", state: wire.TaskStateCompleted, text: "s3cr3t-reply-text"})
	raw := f.workingRun(t0.Add(9*time.Second), "raw")
	status, key := f.statusValue(t0.Add(12*time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: raw, kind: "working", state: wire.TaskStateWorking})
	status.StatusUpdate.Status.Message.Parts = []wire.Part{{Raw: wire.StdBytes("\x1b[1mbold step\x1b[0m"), MediaType: "text/plain"}}
	value := f.sign(status, key)
	f.produce(operator.PoolEventsTopic(poolA), raw, value, t0.Add(12*time.Second))
	v := f.view(time.Hour, transcriptCaps())
	d, _ := v.RunDetail(failed)
	events := d.Attempts[0].Events
	if len(events) != 3 || events[1].Message != "compiling module step" || events[1].Truncated || events[0].Message != "" {
		t.Errorf("working and claimed messages: %+v", events)
	}
	if terminal := events[2]; len(terminal.Message) != 1024 || !terminal.Truncated || !strings.HasPrefix(long, terminal.Message) || d.Reason != terminal.Message || d.Attempts[0].TranscriptChunks != 3 {
		t.Errorf("terminal message: %d bytes truncated %v reason %d bytes chunks %d", len(terminal.Message), terminal.Truncated, len(d.Reason), d.Attempts[0].TranscriptChunks)
	}
	if d, _ := v.RunDetail(rejected); d.Reason != "no profile offers the agent" || d.Attempts[0].Events[0].Message != d.Reason {
		t.Errorf("rejected: %+v", d.Attempts[0].Events)
	}
	if d, _ := v.RunDetail(completed); d.Reason != "" || d.Attempts[0].Events[1].Message != "" {
		t.Errorf("completed terminal's text kept: %+v", d.Attempts[0].Events)
	}
	if d, _ := v.RunDetail(raw); d.Attempts[0].Events[1].Message != "\x1b[1mbold step\x1b[0m" {
		t.Errorf("raw working line: %+v", d.Attempts[0].Events)
	}
}

// The endpoint: the query names exactly one of tail, before and from, within the page bound, with the cursor on the
// last two; the route is logged under its own name and needs the cookie.
func TestTranscriptEndpoint(t *testing.T) {
	s := newServed(t, ServeOptions{})
	s.f.chunk(s.f.now.Add(-9*time.Minute), ch{receiver: receiverA, tenant: tenantA, pool: poolA, run: s.run, seq: 0, part: &wire.Part{Text: "shown-line"}})
	cookie := s.session(t)
	path := "/api/runs/" + s.run + "/transcript"
	if response, _ := s.request(t, http.MethodGet, path+"?tail=10", nil, nil, nil); response.StatusCode != http.StatusForbidden {
		t.Errorf("without a cookie: %d", response.StatusCode)
	}
	for _, query := range []string{"", "?tail=0", "?tail=1001", "?tail=10&from=0&limit=10&attempt=1", "?from=0", "?from=0&limit=10", "?before=5&limit=10", "?tail=10&attempt=1", "?from=-1&limit=10&attempt=1", "?tail=x", "?from=0&limit=10&attempt=1&first=y"} {
		if response, _ := s.request(t, http.MethodGet, path+query, nil, cookie, nil); response.StatusCode != http.StatusBadRequest {
			t.Errorf("%q: %d, want 400", query, response.StatusCode)
		}
	}
	response, body := s.request(t, http.MethodGet, path+"?tail=10", nil, cookie, nil)
	if response.StatusCode != http.StatusOK || !strings.Contains(body, `"text":"shown-line"`) || !strings.Contains(body, `"attempt":1`) || !strings.Contains(body, `"first":0`) || response.Header.Get("Content-Security-Policy") != csp {
		t.Errorf("tail: %d %s", response.StatusCode, body)
	}
	if response, body := s.request(t, http.MethodGet, path+"?from=1&limit=10&attempt=1&first=0", nil, cookie, nil); response.StatusCode != http.StatusOK || strings.Contains(body, "shown-line") || strings.Contains(body, `"reset"`) {
		t.Errorf("from: %d %s", response.StatusCode, body)
	}
	if response, body := s.request(t, http.MethodGet, path+"?from=1&limit=10&attempt=2", nil, cookie, nil); response.StatusCode != http.StatusOK || !strings.Contains(body, `"reset":true`) {
		t.Errorf("stale cursor: %d %s", response.StatusCode, body)
	}
	if response, _ := s.request(t, http.MethodGet, "/api/runs/run-"+strings.Repeat("0", 32)+"/transcript?tail=10", nil, cookie, nil); response.StatusCode != http.StatusNotFound {
		t.Errorf("unknown run: %d", response.StatusCode)
	}
	if !strings.Contains(s.logs.String(), "view: GET transcript 200") || strings.Contains(s.logs.String(), "tail=") {
		t.Errorf("log: %s", s.logs.String())
	}
	// The run detail carries the working line and the rejection's reason as text, and nothing of the reply.
	_, detail := s.request(t, http.MethodGet, "/api/runs/"+s.run, nil, cookie, nil)
	if !strings.Contains(detail, `"message":"s3cr3t-working-line step"`) || strings.Contains(detail, "s3cr3t-reply-text") {
		t.Errorf("run detail: %.600s", detail)
	}
}
