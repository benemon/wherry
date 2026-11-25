package view

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/wire"
)

// A receiver of another pool, entitled to the tenant and so able to write its transcript topic, cannot forge a run's
// transcript by claiming the run's pool: a chunk signed by receiver-c of pool-b naming pool-a at attempt 99, and a
// forged last chunk, are refused, and the run's real lines stand at their attempt.
func TestTranscriptForgedPoolRefused(t *testing.T) {
	f := newFixture(t)
	if err := f.service.PoolApply(f.ctx, poolB, testTerms(bucketB), false); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantPoolAdd(f.ctx, tenantA, poolB); err != nil {
		t.Fatal(err)
	}
	f.addReceiver(receiverC, poolB)
	t0 := f.now.Add(-20 * time.Minute)
	run := f.workingRun(t0, "forged")
	at := t0.Add(2 * time.Second)
	for _, seq := range []int64{0, 10, 20} {
		f.chunk(at, ch{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, seq: seq})
	}
	f.chunk(at.Add(time.Second), ch{receiver: receiverC, tenant: tenantA, pool: poolA, run: run, attempt: 99, seq: 0, part: &wire.Part{Text: "FORGED by receiver-c of pool-b"}})
	f.chunk(at.Add(2*time.Second), ch{receiver: receiverC, tenant: tenantA, pool: poolA, run: run, attempt: 99, seq: 30, last: true})
	v := f.view(time.Hour, transcriptCaps())
	tail := page(t, v, run, Page{Tail: 100})
	if got := seqs(tail.Lines); !slices.Equal(got, []int64{0, 10, 20}) || tail.Attempt != 1 || tail.Complete {
		t.Fatalf("lines %v attempt %d complete %v; want receiver-a's three lines at attempt 1, not complete", got, tail.Attempt, tail.Complete)
	}
	for _, line := range tail.Lines {
		if line.Attempt != 1 || line.Receiver != operator.ReceiverPrincipal(receiverA) || strings.Contains(*line.Text, "FORGED") {
			t.Errorf("line %+v", line)
		}
	}
	if counts := v.Snapshot(0).Counts; counts[countTranscriptRefused] != 2 {
		t.Errorf("counts %v; want the two forged chunks refused", counts)
	}
}

// steppingClock is a clock that moves step forward on each read, so a scan's deadline passes between its reads.
type steppingClock struct {
	mu   sync.Mutex
	now  time.Time
	step time.Duration
}

func (c *steppingClock) read() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(c.step)
	return c.now
}

func (c *steppingClock) setStep(step time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.step = step
}

// The time cap: a scan whose reads outlast the request's time is cut, named, and a tail so cut is answered with no
// lines and retry, since the lines indexed so far are not the tail; the next request resumes the scan and answers.
func TestTranscriptScanTimeCap(t *testing.T) {
	f := newFixture(t)
	t0 := f.now.Add(-20 * time.Minute)
	run := f.workingRun(t0, "slow")
	f.lines(t0.Add(2*time.Second), receiverA, tenantA, poolA, run, 1, 300)
	caps := transcriptCaps()
	caps.BatchRecords, caps.TranscriptScan = 100, 5*time.Second
	clock := &steppingClock{now: f.now}
	v := New(f.client, Options{Window: time.Hour, Caps: caps, Now: clock.read})
	t.Cleanup(v.Close)
	if err := v.Rebuild(f.ctx); err != nil {
		t.Fatal(err)
	}
	// Three seconds pass at each read of the clock: the scan's share of 5 seconds is spent after its first batch.
	clock.setStep(3 * time.Second)
	cut := page(t, v, run, Page{Tail: 10})
	if !slices.Contains(cut.Truncated, capTranscriptScan) || !cut.Retry || len(cut.Lines) != 0 || cut.Next != nil || cut.Complete || cut.Indexed == 0 || cut.Indexed >= 300 {
		t.Fatalf("cut by time: truncated %v retry %v, %d lines, next %v, complete %v, indexed %d", cut.Truncated, cut.Retry, len(cut.Lines), cut.Next, cut.Complete, cut.Indexed)
	}
	if hits := v.Snapshot(0).Counts[capTranscriptScan]; hits != 1 {
		t.Errorf("%s hits %d, want 1", capTranscriptScan, hits)
	}
	clock.setStep(0)
	tail := page(t, v, run, Page{Tail: 10})
	if tail.Retry || len(tail.Lines) != 10 || tail.Indexed != 300 || tail.Lines[0].Seq != 2900 || tail.Next == nil || len(tail.Truncated) != 0 {
		t.Fatalf("resumed: retry %v, %d lines from %v, indexed %d, next %v, truncated %v", tail.Retry, len(tail.Lines), seqs(tail.Lines[:min(1, len(tail.Lines))]), tail.Indexed, tail.Next, tail.Truncated)
	}
}

// The seek has its own share of the request budget: a from request whose scan spends the rest still reads its lines.
func TestTranscriptSeekShare(t *testing.T) {
	f := newFixture(t)
	t0 := f.now.Add(-20 * time.Minute)
	run := f.workingRun(t0, "shared-budget")
	f.lines(t0.Add(2*time.Second), receiverA, tenantA, poolA, run, 1, 300)
	caps := transcriptCaps()
	caps.BatchRecords, caps.TranscriptRecords = 10, 100
	v := f.view(time.Hour, caps)
	tail := page(t, v, run, Page{Tail: 10})
	if !tail.Retry || len(tail.Lines) != 0 || tail.Next != nil || tail.Read.Records != 100-100/seekShare {
		t.Fatalf("tail under a scan cut by records: retry %v, %d lines, next %v, read %+v", tail.Retry, len(tail.Lines), tail.Next, tail.Read)
	}
	from := page(t, v, run, Page{From: ptr(0), Limit: 10, Attempt: 1, First: ptr(0)})
	if !slices.Contains(from.Truncated, capTranscriptRecords) || len(from.Lines) != 10 || from.Lines[0].Seq != 0 || from.Next == nil || *from.Next != 91 || from.Seek.Records != 10 {
		t.Fatalf("from under a scan cut by records: truncated %v, %d lines, next %v, seek %+v; want the ten lines asked for from the seek's share", from.Truncated, len(from.Lines), from.Next, from.Seek)
	}
}

// The total-lines cap over every run evicts the least recently used indexes, other than the one being built, before it
// refuses, so a few runs filling the total do not leave the others empty; the eviction is counted and reported.
func TestTranscriptTotalLinesEvicts(t *testing.T) {
	f := newFixture(t)
	t0 := f.now.Add(-20 * time.Minute)
	first := f.workingRun(t0, "first")
	second := f.workingRun(t0.Add(time.Second), "second")
	f.lines(t0.Add(2*time.Second), receiverA, tenantA, poolA, first, 1, 100)
	f.lines(t0.Add(3*time.Second), receiverA, tenantA, poolA, second, 1, 100)
	caps := transcriptCaps()
	caps.TranscriptLines, caps.TranscriptTotalLines = 100, 150
	v := f.view(time.Hour, caps)
	if p := page(t, v, first, Page{Tail: 10}); p.Indexed != 100 || p.Evicted != 0 || len(p.Truncated) != 0 {
		t.Fatalf("first run: indexed %d evicted %d truncated %v", p.Indexed, p.Evicted, p.Truncated)
	}
	p := page(t, v, second, Page{Tail: 10})
	if p.Indexed != 100 || p.Evicted != 1 || len(p.Truncated) != 0 || len(p.Lines) != 10 || p.Lines[0].Seq != 900 {
		t.Fatalf("second run at the total: indexed %d evicted %d truncated %v, %d lines from %v; want the first run's index evicted and the whole run indexed", p.Indexed, p.Evicted, p.Truncated, len(p.Lines), seqs(p.Lines[:min(1, len(p.Lines))]))
	}
	again := page(t, v, first, Page{Tail: 10})
	if again.Read.Records == 0 || again.Indexed != 100 || again.Evicted != 1 || len(again.Lines) != 10 {
		t.Fatalf("first run again: read %+v indexed %d evicted %d, %d lines; want it rebuilt with the second run's index evicted", again.Read, again.Indexed, again.Evicted, len(again.Lines))
	}
	if counts := v.Snapshot(0).Counts; counts[countTranscriptEvicted] != 2 || counts[capTranscriptLines] != 0 {
		t.Errorf("counts %v; want two evictions and no lines cap hit", counts)
	}
}

// A line whose record no longer verifies under the keys, left in the index by a rotation read between the keys
// snapshot and the chunk's indexing, does not stall paging: it leaves the index, and the pages pass it.
func TestTranscriptRefusedLineDoesNotStall(t *testing.T) {
	f := newFixture(t)
	t0 := f.now.Add(-20 * time.Minute)
	run := f.workingRun(t0, "stalled")
	at := t0.Add(2 * time.Second)
	f.chunk(at, ch{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, seq: 0})
	f.chunk(at.Add(time.Second), ch{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, seq: 50})
	rotatedAt := f.now.Add(time.Minute)
	f.chunk(rotatedAt.Add(time.Second), ch{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, seq: 60, part: &wire.Part{Text: "s3cr3t-retired-key"}})
	v := f.view(time.Hour, transcriptCaps())
	if got := seqs(page(t, v, run, Page{Tail: 100}).Lines); !slices.Equal(got, []int64{0, 50, 60}) {
		t.Fatalf("before the rotation: %v", got)
	}
	// The race: the rotation is read after the keys were snapshotted and before the index recorded the chunk's owner,
	// so the retirement drops no index and the chunk stays indexed.
	v.transcripts.mu.Lock()
	v.transcripts.indexes[run].owners = map[string]bool{}
	v.transcripts.mu.Unlock()
	f.rotate(receiverA, operator.ReceiverTopic(receiverA), rotatedAt)
	f.tick(v)
	tail := page(t, v, run, Page{Tail: 100})
	if got := seqs(tail.Lines); !slices.Equal(got, []int64{0, 50}) || tail.Read.Records != 0 || tail.Indexed != 2 || tail.Next == nil || *tail.Next != 51 {
		t.Fatalf("after the rotation: lines %v, scan read %d, indexed %d, next %v; want the retired key's line forgotten and next past the last line", got, tail.Read.Records, tail.Indexed, tail.Next)
	}
	follow := page(t, v, run, cursor(Page{From: tail.Next, Limit: 100}, tail))
	if len(follow.Lines) != 0 || follow.Next == nil || *follow.Next != 51 || follow.Reset {
		t.Errorf("follow: %+v", follow)
	}
	for _, line := range tail.Lines {
		if strings.Contains(*line.Text, "s3cr3t") {
			t.Errorf("the retired key's line is shown: %+v", line)
		}
	}
}

// An index bound to a task hash the model no longer records for the run is rebuilt on the next request: a run whose
// hash came from its echo, then from its claimed status, shows the chunks naming the status's hash.
func TestTranscriptReboundToModel(t *testing.T) {
	f := newFixture(t)
	t0 := f.now.Add(-20 * time.Minute)
	run := f.echoWith(t0, receiverA, tenantA, hostA, poolA, "rebound", hashOf("other"))
	f.chunk(t0.Add(2*time.Second), ch{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, seq: 0})
	v := f.view(time.Hour, transcriptCaps())
	if p := page(t, v, run, Page{Tail: 10}); len(p.Lines) != 0 {
		t.Fatalf("under the echo's hash: %v", seqs(p.Lines))
	}
	f.status(f.now, st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "claimed", state: wire.TaskStateWorking})
	f.tick(v)
	p := page(t, v, run, Page{Tail: 10})
	if got := seqs(p.Lines); !slices.Equal(got, []int64{0}) || p.Read.Records == 0 {
		t.Fatalf("under the status's hash: lines %v, scan read %d; want the index rebuilt with the chunk", got, p.Read.Records)
	}
	if counts := v.Snapshot(0).Counts; counts[countTranscriptRebound] != 1 {
		t.Errorf("counts %v; want one index rebound", counts)
	}
}

// Requests for one run take turns on the run's lock before taking a scan slot, so with one slot they all answer: a
// page's older-lines request and its follow cannot hold both slots and answer a second operator 503.
func TestTranscriptSameRunWaitsWithoutSlot(t *testing.T) {
	f := newFixture(t)
	t0 := f.now.Add(-20 * time.Minute)
	run := f.workingRun(t0, "queued")
	f.lines(t0.Add(2*time.Second), receiverA, tenantA, poolA, run, 1, 200)
	caps := transcriptCaps()
	caps.TranscriptScans = 1
	v := f.view(time.Hour, caps)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 3 {
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				p, err := v.transcripts.Transcript(ctx, run, Page{Tail: 50})
				cancel()
				if err != nil {
					t.Errorf("request: %v", err)
					return
				}
				if len(p.Lines) != 50 {
					t.Errorf("%d lines", len(p.Lines))
				}
			}
		}()
	}
	wg.Wait()
	if busy := v.Snapshot(0).Counts[capTranscriptScans]; busy != 0 {
		t.Errorf("%s hit %d times; want none for one run", capTranscriptScans, busy)
	}
}

// The page keeps a line's attempt and seq in their own elements and the line in a bdi, and applies a response only to
// the run and request generation it was asked for.
func TestPageIsolatesLines(t *testing.T) {
	script, err := static.ReadFile("static/app.js")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"createElement('bdi')", "transcript.run === run && transcript.generation === generation", "response.retry", "followNow"} {
		if !strings.Contains(string(script), want) {
			t.Errorf("app.js lacks %s", want)
		}
	}
}
