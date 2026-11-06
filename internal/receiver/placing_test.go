package receiver

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// placingFixture is a fixture whose plugin reports every dispatched job pending with reason.
func placingFixture(t *testing.T, reason string) fixture {
	t.Helper()
	f := newFixture(t, resultStream)
	f.plugin.placing, f.plugin.placingReason = true, reason
	return f
}

// setPlugin changes the fake plugin under its lock.
func (f fixture) setPlugin(change func(*fakePlugin)) {
	f.plugin.mu.Lock()
	defer f.plugin.mu.Unlock()
	change(f.plugin)
}

// pendingRuns is the size of the receiver's pending set.
func (f fixture) pendingRuns() int {
	f.receiver.mu.Lock()
	defer f.receiver.mu.Unlock()
	return len(f.receiver.pending)
}

// awaitKind waits until the pool's events hold a status of kind.
func (f fixture) awaitKind(t *testing.T, kind string) {
	t.Helper()
	for !slices.Contains(f.kinds(t), kind) {
		if f.ctx.Err() != nil {
			t.Fatalf("no %s status; logs:\n%s", kind, f.logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// placingTexts are the message texts of the placing statuses among statuses.
func placingTexts(statuses []wire.RunStatus) []string {
	var texts []string
	for _, status := range statuses {
		if status.Wherry.Kind != "placing" {
			continue
		}
		if message := status.StatusUpdate.Status.Message; message != nil && len(message.Parts) == 1 {
			texts = append(texts, message.Parts[0].Text)
		} else {
			texts = append(texts, "")
		}
	}
	return texts
}

// placement is the count, sum and profile labels of the receiver's placement histogram.
func (f fixture) placement(t *testing.T) (uint64, float64, []string) {
	t.Helper()
	registry := prometheus.NewRegistry()
	registry.MustRegister(f.receiver.metrics.placement)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var count uint64
	var sum float64
	var profiles []string
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			count += metric.GetHistogram().GetSampleCount()
			sum += metric.GetHistogram().GetSampleSum()
			for _, label := range metric.GetLabel() {
				profiles = append(profiles, label.GetName()+"="+label.GetValue())
			}
		}
	}
	return count, sum, profiles
}

// placementBuckets are the upper bounds of the receiver's placement histogram and the cumulative count at each.
func (f fixture) placementBuckets(t *testing.T) ([]float64, []uint64) {
	t.Helper()
	registry := prometheus.NewRegistry()
	registry.MustRegister(f.receiver.metrics.placement)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var bounds []float64
	var counts []uint64
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			for _, bucket := range metric.GetHistogram().GetBucket() {
				bounds = append(bounds, bucket.GetUpperBound())
				counts = append(counts, bucket.GetCumulativeCount())
			}
		}
	}
	return bounds, counts
}

// controlClock replaces the receiver's clock with one that returns now and then advances it an hour, and returns the
// setter of now.
func controlClock(t *testing.T, now time.Time) func(time.Time) {
	t.Helper()
	var mu sync.Mutex
	previous := clock
	clock = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		read := now
		now = now.Add(time.Hour)
		return read
	}
	t.Cleanup(func() { clock = previous })
	return func(at time.Time) {
		mu.Lock()
		defer mu.Unlock()
		now = at
	}
}

// holdReceiver takes the receiver's lock as a running Lookup returns, sets the clock to completed, and after hold sets
// it to consumed and gives the lock back: the result's completion and its consumption are hold apart.
func (f fixture) holdReceiver(set func(time.Time), completed, consumed time.Time, hold time.Duration) {
	var once sync.Once
	f.plugin.lookupHook = func(state pluginsdk.JobState) {
		if state != pluginsdk.JobRunning {
			return
		}
		once.Do(func() {
			f.receiver.mu.Lock()
			set(completed)
			go func() {
				time.Sleep(hold)
				set(consumed)
				f.receiver.mu.Unlock()
			}()
		})
	}
}

// decide takes the five steps in order.
func TestDecideOrder(t *testing.T) {
	bound := time.Unix(1_000_000, 0)
	before, at := bound.Add(-time.Nanosecond), bound
	result := func(state pluginsdk.JobState, completed time.Time, err error) *lookupResult {
		return &lookupResult{job: pluginsdk.Job{State: state}, completedAt: completed, err: err}
	}
	failed := errors.New("unavailable")
	for _, test := range []struct {
		name                    string
		ctxDone, cancelled, due bool
		result                  *lookupResult
		want                    action
	}{
		{"ctx over everything", true, true, true, result(pluginsdk.JobRunning, before, nil), leaveAction},
		{"ctx with nothing", true, false, false, nil, leaveAction},
		{"cancel over a result", false, true, false, result(pluginsdk.JobRunning, before, nil), cancelAction},
		{"cancel over a bound", false, true, true, nil, cancelAction},
		{"running just before the bound", false, false, true, result(pluginsdk.JobRunning, before, nil), resultAction},
		{"ended just before the bound", false, false, true, result(pluginsdk.JobEnded, before, nil), resultAction},
		{"running at the bound", false, false, true, result(pluginsdk.JobRunning, at, nil), expireAction},
		{"ended at the bound", false, false, true, result(pluginsdk.JobEnded, at, nil), expireAction},
		{"pending at the bound", false, false, true, result(pluginsdk.JobPending, before, nil), expireAction},
		{"error at the bound", false, false, true, result(pluginsdk.JobAbsent, before, failed), expireAction},
		{"running with an error at the bound", false, false, true, result(pluginsdk.JobRunning, before, failed), expireAction},
		{"absent at the bound", false, false, true, result(pluginsdk.JobAbsent, before, nil), expireAction},
		{"never started at the bound", false, false, true, result(pluginsdk.JobNeverStarted, before, nil), expireAction},
		{"bound alone", false, false, true, nil, expireAction},
		{"pending before the bound", false, false, false, result(pluginsdk.JobPending, before, nil), resultAction},
		{"error before the bound", false, false, false, result(pluginsdk.JobAbsent, before, failed), resultAction},
		{"absent before the bound", false, false, false, result(pluginsdk.JobAbsent, before, nil), resultAction},
		{"never started before the bound", false, false, false, result(pluginsdk.JobNeverStarted, before, nil), resultAction},
		{"running late, no bound fired", false, false, false, result(pluginsdk.JobRunning, at, nil), resultAction},
		{"nothing", false, false, false, nil, awaitAction},
	} {
		if got := decide(test.ctxDone, test.cancelled, test.result, test.due, bound); got != test.want {
			t.Errorf("%s: action %d, want %d", test.name, got, test.want)
		}
	}
	if !accepted(pluginsdk.JobRunning, before, bound) || accepted(pluginsdk.JobRunning, at, bound) || accepted(pluginsdk.JobPending, before, bound) {
		t.Error("accepted is not running or ended completed before the bound")
	}
}

// A dispatched job the platform has not placed is neither started nor followed; once a Lookup finds it running the run
// commits started and follows the job with that Lookup's handle.
func TestPendingRunStartsOnlyOnceRunning(t *testing.T) {
	f := placingFixture(t, "0 nodes evaluated")
	done := f.executing(t, f.ctx, f.submit(t, f.host, nil), 1)
	f.awaitKind(t, "placing")
	time.Sleep(600 * time.Millisecond)
	if _, lookups, follows, _ := f.plugin.counts(); follows != 0 || lookups < 2 || slices.Contains(f.kinds(t), "started") {
		t.Fatalf("lookups=%d follows=%d statuses %v while pending, want Lookups only", lookups, follows, f.kinds(t))
	}
	if n := f.pendingRuns(); n != 1 {
		t.Fatalf("%d runs in the pending set, want 1", n)
	}
	f.setPlugin(func(p *fakePlugin) { p.placing = false })
	if status := <-done; status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	if kinds := f.kinds(t); !slices.Equal(kinds, []string{"claimed", "placing", "started", "terminal"}) {
		t.Fatalf("statuses %v, want claimed, placing, started, terminal", kinds)
	}
	f.plugin.mu.Lock()
	wrong, follows := f.plugin.wrongHandles, f.plugin.follows
	f.plugin.mu.Unlock()
	if wrong != 0 || follows == 0 {
		t.Fatalf("follows=%d, %d with Dispatch's handle; want every Follow and Collect with Lookup's", follows, wrong)
	}
	if n := f.pendingRuns(); n != 0 {
		t.Fatalf("%d runs in the pending set after the start, want none", n)
	}
	if count, _, _ := f.placement(t); count != 1 {
		t.Fatalf("placement count %d, want 1", count)
	}
}

// Placement runs from Dispatch's return to the completion of the Lookup that found the job running, by the receiver's
// clock, not to the result's consumption, carries the profile, and falls in PlacementBuckets.
func TestPlacementIsTimedFromDispatchToTheRunningLookup(t *testing.T) {
	f := placingFixture(t, "queued")
	returned := time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC)
	set := controlClock(t, returned)
	completed := returned.Add(7 * time.Second)
	f.holdReceiver(set, completed, completed.Add(5*time.Hour), 300*time.Millisecond)
	done := f.executing(t, f.ctx, f.submit(t, f.host, nil), 1)
	f.awaitKind(t, "placing")
	f.setPlugin(func(p *fakePlugin) { p.placing = false })
	if status := <-done; status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	count, sum, labels := f.placement(t)
	if count != 1 || sum != 7 || !slices.Equal(labels, []string{"profile=nomad"}) {
		t.Fatalf("placement count %d sum %v labels %v, want one sample of 7 s for profile nomad", count, sum, labels)
	}
	// 7 s falls in every bucket from 10 s up.
	var want []uint64
	for _, bound := range pluginsdk.PlacementBuckets {
		want = append(want, map[bool]uint64{false: 0, true: 1}[bound >= 7])
	}
	if bounds, counts := f.placementBuckets(t); !slices.Equal(bounds, pluginsdk.PlacementBuckets) || !slices.Equal(counts, want) {
		t.Fatalf("placement buckets %v counts %v, want %v counts %v", bounds, counts, pluginsdk.PlacementBuckets, want)
	}
}

// A Dispatch whose outcome is unknown and whose reconciling Lookup finds the job pending enters the pending set, writes
// placing, and observes no placement once the job runs.
func TestReconciledPendingDispatchPlacesWithoutASample(t *testing.T) {
	f := newFixture(t, resultStream)
	f.plugin.dispatchErr, f.plugin.job, f.plugin.placingReason = errors.New("register response lost"), pluginsdk.JobPending, "queued"
	done := f.executing(t, f.ctx, f.submit(t, f.host, nil), 1)
	f.awaitKind(t, "placing")
	if n := f.pendingRuns(); n != 1 {
		t.Fatalf("%d runs in the pending set, want 1", n)
	}
	f.setPlugin(func(p *fakePlugin) { p.job = pluginsdk.JobRunning })
	if status := <-done; status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	if texts := placingTexts(f.feedStatuses(t)); !slices.Equal(texts, []string{"waiting for placement (queued)"}) {
		t.Fatalf("placing %q", texts)
	}
	if count, _, _ := f.placement(t); count != 0 {
		t.Fatalf("placement count %d, want none for a reconciled Dispatch", count)
	}
}

// A job Lookup reports never started takes its class, whichever way the attempt reached it: a fresh Dispatch, adoption
// on redelivery (findJob) or the reconciliation of an unknown Dispatch. A fetch failure fails the run with the S3 text
// and no start failure; any other class is a start failure naming the class and reason.
func TestNeverStartedLookupTakesItsClass(t *testing.T) {
	for _, entry := range []struct {
		name    string
		attempt int64
		setup   func(p *fakePlugin, class, reason string)
	}{
		{"dispatch", 1, func(p *fakePlugin, class, reason string) {
			p.neverStarted, p.neverClass, p.neverReason = true, class, reason
		}},
		{"adoption", 2, func(p *fakePlugin, class, reason string) {
			p.job, p.neverClass, p.neverReason = pluginsdk.JobNeverStarted, class, reason
		}},
		{"reconciliation", 1, func(p *fakePlugin, class, reason string) {
			p.dispatchErr, p.job, p.neverClass, p.neverReason = errors.New("register response lost"), pluginsdk.JobNeverStarted, class, reason
		}},
	} {
		t.Run(entry.name+"/fetch", func(t *testing.T) {
			f := newFixture(t, resultStream)
			entry.setup(f.plugin, pluginsdk.ClassFetch, "403 for s3::bucket?X-Amz-Signature=abc")
			if status := f.execute(t, f.submit(t, f.host, nil), entry.attempt); status != kgo.AckReject {
				t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
			}
			if state, reason := terminalReason(t, f, testRun("run-a")); state != wire.TaskStateFailed || reason != "an S3 input was not fetched, or did not match its size and SHA-256" {
				t.Fatalf("terminal %s %q", state, reason)
			}
			if failures := len(f.startFailures(t)); failures != 0 {
				t.Fatalf("%d start failures, want none", failures)
			}
		})
		t.Run(entry.name+"/driver", func(t *testing.T) {
			terms := testTerms()
			terms.DeliveryLimit = int32(entry.attempt) + 1
			f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream}})
			entry.setup(f.plugin, "driver", "image pull failed")
			if status := f.execute(t, f.submit(t, f.host, nil), entry.attempt); status != kgo.AckReject {
				t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
			}
			want := fmt.Sprintf("delivery budget exhausted after %d attempts; last: job never started (driver): image pull failed", entry.attempt)
			if state, reason := terminalReason(t, f, testRun("run-a")); state != wire.TaskStateFailed || reason != want {
				t.Fatalf("terminal %s %q, want %q", state, reason, want)
			}
			if failures := len(f.startFailures(t)); failures != 1 {
				t.Fatalf("%d start failures, want 1", failures)
			}
		})
	}
}

// A job removed before it started is a start failure; a Lookup that fails is asked again, the run staying in the
// pending set with its last reason, and nothing is published for the failure.
func TestAbsentAndFailedLookupsBeforeTheBound(t *testing.T) {
	t.Run("absent", func(t *testing.T) {
		f := newFixture(t, resultStream)
		f.plugin.vanish = true
		if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckRelease {
			t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
		}
		if failures, kinds := len(f.startFailures(t)), f.kinds(t); failures != 1 || !slices.Equal(kinds, []string{"claimed", "released"}) {
			t.Fatalf("start failures %d, statuses %v; want one start failure and the claim released", failures, kinds)
		}
		if f.plugin.purges != 1 || !strings.Contains(f.logs.String(), "job removed before it started") {
			t.Fatalf("purges=%d; logs:\n%s", f.plugin.purges, f.logs.String())
		}
	})
	t.Run("errors while placing", func(t *testing.T) {
		f := placingFixture(t, "queued")
		var once sync.Once
		f.plugin.lookupHook = func(state pluginsdk.JobState) {
			if state == pluginsdk.JobPending {
				once.Do(func() {
					f.plugin.lookupErr = &pluginsdk.Error{Op: "read evaluations", Class: "unavailable", Err: errors.New("503")}
				})
			}
		}
		done := f.executing(t, f.ctx, f.submit(t, f.host, nil), 1)
		f.awaitKind(t, "placing")
		time.Sleep(time.Second)
		if n := f.pendingRuns(); n != 1 {
			t.Fatalf("%d runs in the pending set while Lookups fail, want 1", n)
		}
		if logged := strings.Count(f.logs.String(), "op=read evaluations class=unavailable"); logged != 1 {
			t.Fatalf("lookup failure logged %d times, want once; logs:\n%s", logged, f.logs.String())
		}
		f.setPlugin(func(p *fakePlugin) { p.lookupErr, p.placing = nil, false })
		if status := <-done; status != kgo.AckAccept {
			t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
		}
		if kinds := f.kinds(t); !slices.Equal(kinds, []string{"claimed", "placing", "started", "terminal"}) {
			t.Fatalf("statuses %v", kinds)
		}
	})
}

// placing is written on the first pending result and on each change of its normalised reason only, never after
// started, to both feeds, redacted and bounded.
func TestPlacingPublication(t *testing.T) {
	t.Run("changes only", func(t *testing.T) {
		f := newFixture(t, resultStream)
		f.plugin.reasons = []string{"A", "A", "B", "C"}
		if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckAccept {
			t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
		}
		want := []string{"waiting for placement (A)", "waiting for placement (B)", "waiting for placement (C)"}
		for name, statuses := range map[string][]wire.RunStatus{"events": f.eventStatuses(t), "feed": f.feedStatuses(t)} {
			if texts := placingTexts(statuses); !slices.Equal(texts, want) {
				t.Fatalf("%s placing %q, want %q", name, texts, want)
			}
		}
		if kinds := f.kinds(t); !slices.Equal(kinds, []string{"claimed", "placing", "placing", "placing", "started", "terminal"}) {
			t.Fatalf("statuses %v", kinds)
		}
	})
	t.Run("redacted and bounded", func(t *testing.T) {
		for name, test := range map[string]struct{ reason, want string }{
			"credential": {"node pool has " + testToken + " exhausted", "waiting for placement (node pool has " + redacted + " exhausted)"},
			"10 KiB":     {strings.Repeat("x", 10<<10), "waiting for placement (" + strings.Repeat("x", maxLogText) + ")"},
			"empty":      {"", "waiting for placement"},
		} {
			t.Run(name, func(t *testing.T) {
				f := placingFixture(t, test.reason)
				done := f.executing(t, f.ctx, f.submit(t, f.host, nil), 1)
				f.awaitKind(t, "placing")
				f.setPlugin(func(p *fakePlugin) { p.placing = false })
				if status := <-done; status != kgo.AckAccept {
					t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
				}
				for feed, statuses := range map[string][]wire.RunStatus{"events": f.eventStatuses(t), "feed": f.feedStatuses(t)} {
					if texts := placingTexts(statuses); !slices.Equal(texts, []string{test.want}) {
						t.Fatalf("%s placing %q, want %q", feed, texts, test.want)
					}
				}
			})
		}
	})
}

// A cancel while the job is pending, between Lookups or inside one the plugin blocks, cancels the Lookup, stops the job
// without waiting for the Lookup, and drops the Lookup's late result; a failed Stop returns the task.
func TestCancelWhilePending(t *testing.T) {
	for _, window := range []string{"between lookups", "inside a lookup"} {
		t.Run(window, func(t *testing.T) {
			f := placingFixture(t, "queued")
			hold := make(chan struct{})
			if window == "inside a lookup" {
				f.plugin.placing, f.plugin.lookupHold = false, hold
			}
			done := f.executing(t, f.ctx, f.submit(t, f.host, nil), 1)
			if window == "between lookups" {
				f.awaitKind(t, "placing")
			} else {
				for _, lookups, _, _ := f.plugin.counts(); lookups == 0; _, lookups, _, _ = f.plugin.counts() {
					time.Sleep(20 * time.Millisecond)
				}
			}
			f.publishCancel(t, testRun("run-a"), 1, f.host, nil)
			select {
			case status := <-done:
				if status != kgo.AckAccept {
					t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("the cancel waited on the Lookup; logs:\n%s", f.logs.String())
			}
			f.plugin.mu.Lock()
			stops, ctxs := f.plugin.stops, slices.Clone(f.plugin.lookupCtxs)
			f.plugin.mu.Unlock()
			if state, reason := terminalReason(t, f, testRun("run-a")); stops != 1 || state != wire.TaskStateCanceled || reason != cancelReason {
				t.Fatalf("stops=%d terminal %s %q, want the job stopped and the run CANCELED", stops, state, reason)
			}
			for _, ctx := range ctxs {
				if ctx.Err() == nil {
					t.Fatal("the held Lookup's context was not cancelled")
				}
			}
			close(hold)
			time.Sleep(300 * time.Millisecond)
			if _, _, follows, _ := f.plugin.counts(); follows != 0 || slices.Contains(f.kinds(t), "started") {
				t.Fatalf("follows=%d statuses %v, want the late result dropped", follows, f.kinds(t))
			}
		})
	}
	t.Run("stop fails", func(t *testing.T) {
		f := placingFixture(t, "queued")
		f.plugin.stopErr = errors.New("stop failed")
		done := f.executing(t, f.ctx, f.submit(t, f.host, nil), 1)
		f.awaitKind(t, "placing")
		f.publishCancel(t, testRun("run-a"), 1, f.host, nil)
		if status := <-done; status != kgo.AckRelease || slices.Contains(f.kinds(t), "terminal") {
			t.Fatalf("status %v statuses %v, want the task returned; logs:\n%s", status, f.kinds(t), f.logs.String())
		}
	})
}

// A result held until a bound has fired is acted on only when its Lookup completed before the bound and found the job
// running; a pending result then expires the run without placing.
func TestResultAtTheBound(t *testing.T) {
	for name, test := range map[string]struct {
		state   pluginsdk.JobState
		offset  time.Duration
		started bool
	}{
		"running completed just before":  {pluginsdk.JobRunning, -time.Nanosecond, true},
		"running completed at the bound": {pluginsdk.JobRunning, 0, false},
		"pending completed before":       {pluginsdk.JobPending, -time.Second, false},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			startBy := time.Now().Add(2 * time.Second).Unix()
			bound := wire.PastBound(startBy)
			record := f.submit(t, f.host, func(task *wire.Submission) {
				task.Wherry.TaskStartedBy, task.Wherry.TaskEndedBy = startBy, time.Now().Add(time.Minute).Unix()
			})
			set := controlClock(t, time.Now())
			// The first Lookup's result reaches the phase only after the bound has fired.
			var once sync.Once
			f.plugin.lookupHook = func(state pluginsdk.JobState) {
				once.Do(func() {
					f.receiver.mu.Lock()
					set(bound.Add(test.offset))
					go func() {
						time.Sleep(time.Until(bound) + 200*time.Millisecond)
						f.receiver.mu.Unlock()
					}()
				})
			}
			if test.state == pluginsdk.JobPending {
				f.plugin.placing = true
			}
			status := f.execute(t, record, 1)
			kinds := f.kinds(t)
			if test.started {
				if status != kgo.AckAccept || !slices.Contains(kinds, "started") {
					t.Fatalf("status %v statuses %v, want the run started and completed; logs:\n%s", status, kinds, f.logs.String())
				}
				return
			}
			if status != kgo.AckReject || slices.Contains(kinds, "started") || slices.Contains(kinds, "placing") {
				t.Fatalf("status %v statuses %v, want expired without started or placing; logs:\n%s", status, kinds, f.logs.String())
			}
			if state, reason := terminalReason(t, f, testRun("run-a")); state != wire.TaskStateFailed || reason != wire.NotStartedBy(startBy) {
				t.Fatalf("terminal %s %q", state, reason)
			}
		})
	}
}

// A pending run past both bounds expires with the end-bound text.
func TestPendingPastBothBoundsNamesTheEndBound(t *testing.T) {
	f := placingFixture(t, "queued")
	bound := time.Now().Add(time.Second).Unix()
	record := f.submit(t, f.host, func(task *wire.Submission) { task.Wherry.TaskStartedBy, task.Wherry.TaskEndedBy = bound, bound })
	if status := f.execute(t, record, 1); status != kgo.AckReject {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	if state, reason := terminalReason(t, f, testRun("run-a")); state != wire.TaskStateFailed || reason != wire.NotEndedBy(bound) {
		t.Fatalf("terminal %s %q, want FAILED %q", state, reason, wire.NotEndedBy(bound))
	}
}

// A running result completed before the bound that is buffered only once the phase has woken on the bound is taken by
// the drain, not lost to the expiry: the receiver's lock holds the phase at its cancel check while the result arrives.
func TestResultBufferedAfterTheBoundWakesThePhase(t *testing.T) {
	f := newFixture(t, resultStream)
	startBy := time.Now().Add(2 * time.Second).Unix()
	bound := wire.PastBound(startBy)
	record := f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.TaskStartedBy, task.Wherry.TaskEndedBy = startBy, time.Now().Add(time.Minute).Unix()
	})
	set := controlClock(t, time.Now())
	var once sync.Once
	f.plugin.lookupHook = func(state pluginsdk.JobState) {
		once.Do(func() {
			// The phase wakes with nothing buffered and waits on the lock; the result completes before the bound by the
			// receiver's clock and is buffered before the lock is given back.
			f.receiver.mu.Lock()
			time.Sleep(time.Until(bound) + 200*time.Millisecond)
			set(bound.Add(-time.Second))
			go func() {
				time.Sleep(200 * time.Millisecond)
				f.receiver.mu.Unlock()
			}()
		})
	}
	if status, kinds := f.execute(t, record, 1), f.kinds(t); status != kgo.AckAccept || !slices.Contains(kinds, "started") {
		t.Fatalf("status %v statuses %v, want the buffered result started; logs:\n%s", status, kinds, f.logs.String())
	}
}

// A Dispatch that returns created exactly at the bound issues no Lookup under the ended context: the run expires.
func TestDispatchReturningAtTheBoundIssuesNoLookup(t *testing.T) {
	f := newFixture(t, resultStream)
	f.plugin.dispatchBlocks = true
	startBy := time.Now().Add(time.Second).Unix()
	record := f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.TaskStartedBy, task.Wherry.TaskEndedBy = startBy, time.Now().Add(time.Minute).Unix()
	})
	status := f.execute(t, record, 1)
	// A Lookup issued as the phase began has counted by now.
	time.Sleep(300 * time.Millisecond)
	if dispatches, lookups, _, _ := f.plugin.counts(); status != kgo.AckReject || dispatches != 1 || lookups != 0 {
		t.Fatalf("status %v dispatches=%d lookups=%d, want one Dispatch, no Lookup and the run expired; logs:\n%s", status, dispatches, lookups, f.logs.String())
	}
	if state, reason := terminalReason(t, f, testRun("run-a")); state != wire.TaskStateFailed || reason != wire.NotStartedBy(startBy) || f.plugin.stops != 1 {
		t.Fatalf("terminal %s %q stops=%d, want stopped and FAILED %q", state, reason, f.plugin.stops, wire.NotStartedBy(startBy))
	}
}

// A shutdown or a halt during the expiry's Stop writes nothing more: a shutdown releases the claim, a halt leaves it.
func TestContextEndingDuringTheExpiryStop(t *testing.T) {
	for name, halt := range map[string]bool{"shutdown": false, "halt": true} {
		t.Run(name, func(t *testing.T) {
			f := placingFixture(t, "queued")
			startBy := time.Now().Add(time.Second).Unix()
			record := f.submit(t, f.host, func(task *wire.Submission) {
				task.Wherry.TaskStartedBy, task.Wherry.TaskEndedBy = startBy, time.Now().Add(time.Minute).Unix()
			})
			ctx, cancel := context.WithCancelCause(f.ctx)
			defer cancel(nil)
			f.plugin.stopCheck = func() error {
				if halt {
					cancel(errors.New("plugin nomad exited"))
				} else {
					cancel(nil)
				}
				return nil
			}
			admitted := f.admitted(t, record)
			status := f.receiver.execute(ctx, f.service.Operator, record, admitted, 1)
			kinds := f.kinds(t)
			if strings.Contains(f.logs.String(), "write failed") {
				t.Fatalf("a write was attempted after the context ended; logs:\n%s", f.logs.String())
			}
			switch {
			case halt && (status != 0 || !slices.Equal(kinds, []string{"claimed", "placing"})):
				t.Fatalf("status %v statuses %v, want 0 and nothing written; logs:\n%s", status, kinds, f.logs.String())
			case !halt && (status != kgo.AckRelease || !slices.Equal(kinds, []string{"claimed", "placing", "released"})):
				t.Fatalf("status %v statuses %v, want released and nothing more; logs:\n%s", status, kinds, f.logs.String())
			}
		})
	}
}

// A shutdown while pending releases the claim and leaves the job, which the next holder finds pending and starts; a halt
// writes nothing and acknowledges nothing.
func TestShutdownWhilePending(t *testing.T) {
	t.Run("shutdown", func(t *testing.T) {
		f := placingFixture(t, "queued")
		record := f.submit(t, f.host, nil)
		ctx, cancel := context.WithCancel(f.ctx)
		done := f.executing(t, ctx, record, 1)
		f.awaitKind(t, "placing")
		cancel()
		if status := <-done; status != kgo.AckRelease {
			t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
		}
		if f.plugin.stops != 0 || f.plugin.purges != 0 || !slices.Equal(f.kinds(t), []string{"claimed", "placing", "released"}) {
			t.Fatalf("stops=%d purges=%d statuses %v, want the job left and the claim released", f.plugin.stops, f.plugin.purges, f.kinds(t))
		}
		adopted := f.executing(t, f.ctx, record, 2)
		for len(placingTexts(f.eventStatuses(t))) < 2 {
			if f.ctx.Err() != nil {
				t.Fatalf("the adopter never wrote placing; logs:\n%s", f.logs.String())
			}
			time.Sleep(20 * time.Millisecond)
		}
		f.setPlugin(func(p *fakePlugin) { p.placing = false })
		if status := <-adopted; status != kgo.AckAccept {
			t.Fatalf("adopter status %v; logs:\n%s", status, f.logs.String())
		}
		if dispatches, _, _, _ := f.plugin.counts(); dispatches != 1 {
			t.Fatalf("dispatches=%d, want the adopter to continue the job", dispatches)
		}
	})
	t.Run("halt", func(t *testing.T) {
		f := placingFixture(t, "queued")
		ctx, cancel := context.WithCancelCause(f.ctx)
		done := f.executing(t, ctx, f.submit(t, f.host, nil), 1)
		f.awaitKind(t, "placing")
		cancel(errors.New("plugin nomad exited"))
		if status := <-done; status != 0 {
			t.Fatalf("status %v, want 0", status)
		}
		if f.plugin.stops != 0 || !slices.Equal(f.kinds(t), []string{"claimed", "placing"}) {
			t.Fatalf("stops=%d statuses %v, want nothing written", f.plugin.stops, f.kinds(t))
		}
	})
}

// adoptingPending returns a fixture whose Lookup finds an earlier attempt's job pending, and the record of its task.
func adoptingPending(t *testing.T) (fixture, *kgo.Record) {
	t.Helper()
	f := newFixture(t, resultStream)
	f.plugin.job, f.plugin.placingReason = pluginsdk.JobPending, "queued"
	return f, f.submit(t, f.host, nil)
}

// An adopter of a pending job enters the pending set at once and publishes nothing while its open waits; its first
// placing comes from its first Lookup after the open, and the job starts without a second Dispatch or a placement
// sample.
func TestAnAdopterOfAPendingJob(t *testing.T) {
	t.Run("pending after the open", func(t *testing.T) {
		f, record := adoptingPending(t)
		f.quiet()
		done := f.executing(t, f.ctx, record, 2)
		f.request(t, 1)
		time.Sleep(300 * time.Millisecond)
		if n, kinds := f.pendingRuns(), f.kinds(t); n != 1 || !slices.Equal(kinds, []string{"claimed", "adopting"}) {
			t.Fatalf("pending set %d, statuses %v while the open waits; want the run entered and no placing", n, kinds)
		}
		f.setPlugin(func(p *fakePlugin) { p.placingReason = "after the open" })
		f.serving.Store(true)
		f.awaitKind(t, "placing")
		if _, _, follows, _ := f.plugin.counts(); follows != 0 {
			t.Fatalf("followed %d times while the job was pending, want none", follows)
		}
		f.setPlugin(func(p *fakePlugin) { p.job = pluginsdk.JobRunning })
		if status := <-done; status != kgo.AckAccept {
			t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
		}
		if texts := placingTexts(f.feedStatuses(t)); !slices.Equal(texts, []string{"waiting for placement (after the open)"}) {
			t.Fatalf("placing %q, want the post-open reason alone", texts)
		}
		dispatches, _, follows, _ := f.plugin.counts()
		if count, _, _ := f.placement(t); dispatches != 0 || follows == 0 || count != 0 || f.plugin.wrongHandles != 0 {
			t.Fatalf("dispatches=%d follows=%d placement=%d, want the job adopted, followed, and no sample", dispatches, follows, count)
		}
	})
	for name, change := range map[string]func(*fakePlugin){
		"running after the open": func(p *fakePlugin) { p.job = pluginsdk.JobRunning },
		"errs after the open":    func(p *fakePlugin) { p.lookupErr = errors.New("lookup down") },
		"blocks after the open":  func(p *fakePlugin) { p.lookupBlocks = true },
	} {
		t.Run(name, func(t *testing.T) {
			f, record := adoptingPending(t)
			f.quiet()
			done := f.executing(t, f.ctx, record, 2)
			f.request(t, 1)
			f.setPlugin(change)
			f.serving.Store(true)
			if name == "running after the open" {
				if status := <-done; status != kgo.AckAccept || slices.Contains(f.kinds(t), "placing") {
					t.Fatalf("status %v statuses %v, want started without placing; logs:\n%s", status, f.kinds(t), f.logs.String())
				}
				return
			}
			time.Sleep(time.Second)
			if kinds := f.kinds(t); slices.Contains(kinds, "placing") || slices.Contains(kinds, "started") {
				t.Fatalf("statuses %v, want no placing and no start", kinds)
			}
			f.publishCancel(t, testRun("run-a"), 1, f.host, nil)
			if status := <-done; status != kgo.AckAccept {
				t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
			}
			if state, _ := terminalReason(t, f, testRun("run-a")); state != wire.TaskStateCanceled || f.plugin.stops != 1 {
				t.Fatalf("terminal %s stops=%d, want the cancel to end it", state, f.plugin.stops)
			}
		})
	}
	t.Run("cancelled before adopting", func(t *testing.T) {
		f, record := adoptingPending(t)
		f.publishCancel(t, testRun("run-a"), 1, f.host, nil)
		if status := f.execute(t, record, 2); status != kgo.AckAccept || f.plugin.stops != 1 || slices.Contains(f.kinds(t), "adopting") {
			t.Fatalf("status %v stops=%d statuses %v, want the job stopped and the run cancelled", status, f.plugin.stops, f.kinds(t))
		}
	})
	t.Run("past start-by before the open", func(t *testing.T) {
		f := newFixture(t, resultStream)
		f.plugin.job = pluginsdk.JobPending
		startBy := time.Now().Add(time.Second).Unix()
		record := f.submit(t, f.host, func(task *wire.Submission) { task.Wherry.TaskStartedBy = startBy })
		f.publishStatus(t, record, testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
		passed(startBy)
		if status := f.execute(t, record, 2); status != kgo.AckReject {
			t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
		}
		if state, reason := terminalReason(t, f, testRun("run-a")); state != wire.TaskStateFailed || reason != wire.NotStartedBy(startBy) || f.plugin.stops != 1 || f.plugin.purges != 0 {
			t.Fatalf("terminal %s %q stops=%d purges=%d, want stopped and FAILED with the start bound", state, reason, f.plugin.stops, f.plugin.purges)
		}
		if f.plugin.releases != 1 || len(f.openRequests(t)) != 0 {
			t.Fatalf("releases=%d open requests %d, want released without an open", f.plugin.releases, len(f.openRequests(t)))
		}
	})
	t.Run("past both bounds before the open", func(t *testing.T) {
		f := newFixture(t, resultStream)
		f.plugin.job = pluginsdk.JobPending
		bound := time.Now().Add(time.Second).Unix()
		record := f.submit(t, f.host, func(task *wire.Submission) { task.Wherry.TaskStartedBy, task.Wherry.TaskEndedBy = bound, bound })
		f.publishStatus(t, record, testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
		admitted := f.admitted(t, record)
		passed(bound)
		if status := f.receiver.execute(f.ctx, f.service.Operator, record, admitted, 2); status != kgo.AckReject {
			t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
		}
		if state, reason := terminalReason(t, f, testRun("run-a")); state != wire.TaskStateFailed || reason != wire.NotEndedBy(bound) || f.plugin.stops != 1 || f.plugin.purges != 0 {
			t.Fatalf("terminal %s %q stops=%d purges=%d, want stopped and FAILED %q", state, reason, f.plugin.stops, f.plugin.purges, wire.NotEndedBy(bound))
		}
	})
}

// An adopter's refusals with a pending job: past the bound it expires without a purge; a key no holder has takes the
// never-started route, a purge then key-lost, from the adopter and from reconciling.
func TestAnAdopterOfAPendingJobRefused(t *testing.T) {
	refuse := func(t *testing.T, f fixture, n int, reason string) {
		t.Helper()
		refusal := &wire.OpenRefusal{Custodian: testReceiver, Request: wire.Digest(f.requestRecord(t, n).Value), Reason: reason}
		f.produce(t, operator.PoolEventsTopic(testPool), testRun("run-a"), must(t)(wire.SignRecord(refusal, f.receiver.signing)))
	}
	t.Run("past bound", func(t *testing.T) {
		f, record := adoptingPending(t)
		f.quiet()
		done := f.executing(t, f.ctx, record, 2)
		refuse(t, f, 1, wire.RefusedPastBound)
		if status := <-done; status != kgo.AckReject || f.plugin.stops != 1 || f.plugin.purges != 0 {
			t.Fatalf("status %v stops=%d purges=%d, want expired with a Stop and no Purge", status, f.plugin.stops, f.plugin.purges)
		}
	})
	t.Run("key unavailable", func(t *testing.T) {
		f, record := adoptingPending(t)
		f.quiet()
		done := f.executing(t, f.ctx, record, 2)
		refuse(t, f, 1, wire.RefusedKeyUnavailable)
		refuse(t, f, 2, wire.RefusedKeyUnavailable)
		if status := <-done; status != kgo.AckReject {
			t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
		}
		if task := f.lastTask(t); f.plugin.purges != 1 || f.plugin.stops != 0 || task.Status.Message.Parts[0].Text != keyLostReason || f.lastEvent(t) != wire.EventKeyLost {
			t.Fatalf("purges=%d stops=%d terminal %+v, want purged and key-lost", f.plugin.purges, f.plugin.stops, task.Status)
		}
	})
	t.Run("reconciling", func(t *testing.T) {
		f, record := adoptingPending(t)
		f.publishStatus(t, record, testReceiver, f.receiver.signing, "reconciling", wire.TaskStateWorking, nil)
		if status := f.execute(t, record, 2); status != kgo.AckReject {
			t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
		}
		if f.plugin.purges != 1 || f.plugin.stops != 0 || f.lastEvent(t) != wire.EventKeyLost {
			t.Fatalf("purges=%d stops=%d event %q, want purged and key-lost", f.plugin.purges, f.plugin.stops, f.lastEvent(t))
		}
	})
}

// lastEvent is the wherry event of the last terminal status on the pool's events.
func (f fixture) lastEvent(t *testing.T) string {
	t.Helper()
	event := ""
	for _, status := range f.eventStatuses(t) {
		if status.Wherry.Kind == "terminal" {
			event = status.Wherry.Event
		}
	}
	return event
}

// gated reports the receiver's claim gate as a slot reads it.
func gated(f fixture) bool {
	f.receiver.mu.Lock()
	gated := f.receiver.claimGated()
	f.receiver.mu.Unlock()
	return gated
}

// Each execution holds its own entry in the pending set, so an old attempt clears only its own, in either order of
// entry, and a late pending result of the old attempt does not reopen the gate the new one holds.
func TestPendingSetIsKeyedByExecution(t *testing.T) {
	f := newFixture(t, resultStream)
	f.receiver.mu.Lock()
	f.receiver.offers = []wire.Offer{{Profile: "nomad"}}
	f.receiver.mu.Unlock()
	if gated(f) {
		t.Fatal("gated before any pending run")
	}
	for name, order := range map[string][]int{"old first": {0, 1}, "new first": {1, 0}} {
		t.Run(name, func(t *testing.T) {
			attempts := []*activeRun{testActiveRun(t, f, "run-a", 1), testActiveRun(t, f, "run-a", 2)}
			for _, i := range order {
				attempts[i].pending()
			}
			attempts[0].placed()
			if !gated(f) {
				t.Fatal("the old attempt's exit reopened the gate the new attempt holds")
			}
			attempts[0].pending()
			attempts[0].placed()
			if !gated(f) {
				t.Fatal("the old attempt's late pending result and exit reopened the gate")
			}
			attempts[1].placed()
			if gated(f) {
				t.Fatal("still gated once every entry is gone")
			}
		})
	}
}

// Two executions of one run on one receiver: the newer holder adopts the pending job and enters the pending set; the
// older one's Lookup, held until then, finds the job pending, and its placing write is fenced. Its exit leaves the gate
// closed, so no poll opens until the newer holder's job starts, and then a fetch follows.
func TestFencedHolderLeavesTheNewerHoldersGate(t *testing.T) {
	f := pendingGateFixture(t)
	hold := make(chan struct{})
	f.plugin.lookupHold = hold
	record := f.submit(t, f.host, nil)
	older := f.executing(t, f.ctx, record, 1)
	for f.lookupsHeld() == 0 {
		if f.ctx.Err() != nil {
			t.Fatalf("the older holder never looked up; logs:\n%s", f.logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	f.setPlugin(func(p *fakePlugin) { p.lookupHold = nil })
	newer := f.executing(t, f.ctx, record, 2)
	f.awaitKind(t, "placing")
	if n := f.pendingRuns(); n != 1 {
		t.Fatalf("%d entries in the pending set, want the newer holder's", n)
	}
	joins := f.shareJoins(t)
	f.run(t, f.ctx)
	close(hold)
	select {
	case status := <-older:
		if status != 0 || !strings.Contains(f.logs.String(), "run "+testRun("run-a")+": fenced by a newer holder") {
			t.Fatalf("older holder ended %v, want fenced with 0; logs:\n%s", status, f.logs.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("the older holder did not exit; logs:\n%s", f.logs.String())
	}
	if texts := placingTexts(f.eventStatuses(t)); len(texts) != 1 {
		t.Fatalf("placing %q, want the newer holder's alone", texts)
	}
	if most := f.mostMembers(2 * time.Second); most != 0 || joins() != 0 {
		t.Fatalf("%d share group members and %d joins after the fenced holder left, the newer one pending; want none", most, joins())
	}
	f.setPlugin(func(p *fakePlugin) { p.placedRuns[testRun("run-a")] = true })
	if status := <-newer; status != kgo.AckAccept {
		t.Fatalf("newer holder ended %v; logs:\n%s", status, f.logs.String())
	}
	if !f.awaitMembers(time.Second+500*time.Millisecond, func(n int) bool { return n > 0 }) {
		t.Fatalf("no share group member once the newer holder started; logs:\n%s", f.logs.String())
	}
}

// lookupsHeld counts the Lookups lookupHold has held.
func (f fixture) lookupsHeld() int {
	f.plugin.mu.Lock()
	defer f.plugin.mu.Unlock()
	return len(f.plugin.lookupCtxs)
}

// A suspended pool serves an adopter's open for a pending job, as for a running one, and the job starts.
func TestSuspendedPoolServesAPendingAdoption(t *testing.T) {
	f, record := adoptingPending(t)
	f.suspend(t)
	done := f.executing(t, f.ctx, record, 2)
	f.awaitKind(t, "placing")
	f.setPlugin(func(p *fakePlugin) { p.job = pluginsdk.JobRunning })
	if status := <-done; status != kgo.AckAccept || f.lastTask(t).Status.State != wire.TaskStateCompleted {
		t.Fatalf("adopter ended %v; logs:\n%s", status, f.logs.String())
	}
	if f.answeredWith(t, wire.RefusedPoolSuspended) != 0 {
		t.Fatal("a pending adoption was refused pool suspended")
	}
}

// pendingGateFixture is a fixture of capacity 6 whose plugin keeps each dispatched job pending until placedRuns or
// placing says otherwise.
func pendingGateFixture(t *testing.T) fixture {
	t.Helper()
	config := testConfig(testReceiver)
	config.Profiles[0].Replicas = 6
	return newFixtureWith(t, resultStream, testTerms(), config, map[string]*fakePlugin{
		"nomad": {stream: resultStream, placing: true, placingReason: "queued", placedRuns: map[string]bool{}},
	})
}

// A validated pending observation closes the claim gate: the idle polls end within the watcher's 100 ms plus their 1 s
// FetchMaxWait, no share group member joins while any held run is pending, records already fetched are admitted, and the
// gate reopens only once no run is pending.
func TestPendingRunClosesTheGate(t *testing.T) {
	f := pendingGateFixture(t)
	f.submit(t, f.host, nil)
	f.submit(t, f.host, func(task *wire.Submission) { named(task, "run-b") })
	f.run(t, f.ctx)
	// Both tasks are fetched by slots that polled before any pending observation, so both are admitted.
	for placing := 0; placing < 2; placing = len(placingTexts(f.eventStatuses(t))) {
		if f.ctx.Err() != nil {
			t.Fatalf("both runs never placing; logs:\n%s", f.logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if n := f.pendingRuns(); n != 2 {
		t.Fatalf("%d runs in the pending set, want 2", n)
	}
	if !f.awaitMembers(100*time.Millisecond+time.Second+500*time.Millisecond, func(n int) bool { return n == 0 }) {
		t.Fatalf("%d share group members still open 1.6 s after the gate closed, want none", f.polling())
	}
	joins := f.shareJoins(t)
	if most := f.mostMembers(3 * time.Second); most != 0 || joins() != 0 {
		t.Fatalf("%d share group members and %d joins while runs are pending, want none", most, joins())
	}
	f.setPlugin(func(p *fakePlugin) { p.placedRuns[testRun("run-a")] = true })
	f.await(t, testRun("run-a"), func(task wire.Task) bool { return task.Status.State == wire.TaskStateCompleted })
	if most := f.mostMembers(2 * time.Second); most != 0 || joins() != 0 {
		t.Fatalf("%d share group members and %d joins with one run still pending, want none", most, joins())
	}
	f.setPlugin(func(p *fakePlugin) { p.placing = false })
	f.await(t, testRun("run-b"), func(task wire.Task) bool { return task.Status.State == wire.TaskStateCompleted })
	// A slot asleep on the gate checks again within its 1 s sleep.
	if !f.awaitMembers(time.Second+500*time.Millisecond, func(n int) bool { return n > 0 }) {
		t.Fatalf("no share group member once no run is pending; logs:\n%s", f.logs.String())
	}
}

// The gate holds at a slot's entry: with a pending run held before the slots start, none opens a session.
func TestPendingRunGatesTheSlotsEntry(t *testing.T) {
	f := pendingGateFixture(t)
	held := testActiveRun(t, f, "run-z", 1)
	held.pending()
	joins := f.shareJoins(t)
	f.run(t, f.ctx)
	if most := f.mostMembers(2 * time.Second); most != 0 || joins() != 0 {
		t.Fatalf("%d share group members and %d joins with a pending run held, want none", most, joins())
	}
	held.placed()
	if !f.awaitMembers(time.Second+500*time.Millisecond, func(n int) bool { return n > 0 }) {
		t.Fatalf("no share group member once the run left the pending set; logs:\n%s", f.logs.String())
	}
}

// The poll's watcher ends a poll open when a run turns pending, and its empty result ends the slot's session rather
// than polling again.
func TestPendingRunEndsAnOpenPoll(t *testing.T) {
	f := pendingGateFixture(t)
	f.run(t, f.ctx)
	if !f.awaitMembers(10*time.Second, func(n int) bool { return n == 6 }) {
		t.Fatalf("the slots never polled; logs:\n%s", f.logs.String())
	}
	held := testActiveRun(t, f, "run-z", 1)
	held.pending()
	if !f.awaitMembers(100*time.Millisecond+time.Second+500*time.Millisecond, func(n int) bool { return n == 0 }) {
		t.Fatalf("%d share group members still open 1.6 s after a run turned pending, want none", f.polling())
	}
	if most := f.mostMembers(2 * time.Second); most != 0 {
		t.Fatalf("%d share group members while a run is pending, want none", most)
	}
	held.placed()
}

// Pending holds the gate beside the other reasons, each in its own storage: clearing one leaves the gate closed while
// another holds, and clearing all reopens it.
func TestPendingHoldsTheGateBesideTheOtherReasons(t *testing.T) {
	f := gateFixture(t, resultStream)
	held := testActiveRun(t, f, "run-z", 1)
	suspend := func(suspended bool) {
		if err := f.service.PoolSuspend(f.ctx, testPool, suspended); err != nil {
			t.Fatal(err)
		}
		if err := f.receiver.refresh(f.ctx); err != nil {
			t.Fatal(err)
		}
	}
	closed := func(what string) {
		t.Helper()
		if most := f.mostMembers(1500 * time.Millisecond); most != 0 {
			t.Fatalf("%s: %d share group members, want none", what, most)
		}
	}
	held.pending()
	f.run(t, f.ctx)
	closed("pending")
	suspend(true)
	held.placed()
	closed("pending cleared while suspended")
	held.pending()
	suspend(false)
	f.setReady(t, false)
	closed("offers empty while pending")
	f.setReady(t, true)
	closed("offers returned, pending held")
	held.placed()
	if !f.awaitMembers(time.Second+500*time.Millisecond, func(n int) bool { return n > 0 }) {
		t.Fatalf("every reason cleared: no share group member joined; logs:\n%s", f.logs.String())
	}
}

// A run held pending past its claim's lock duration keeps its claim by renewal: one delivery, no release and no purge.
func TestPendingRunHeldPastTheLockDuration(t *testing.T) {
	f := placingFixture(t, "queued")
	renewals := f.renewals(t)
	f.submit(t, f.host, nil)
	f.run(t, f.ctx)
	f.awaitKind(t, "placing")
	began := time.Now()
	lock := time.Duration(testTerms().LockDurationMS) * time.Millisecond
	time.Sleep(lock + 2*time.Second)
	var late int
	for _, at := range renewals() {
		if at.Sub(began) > lock {
			late++
		}
	}
	if late == 0 {
		t.Fatalf("no renewal past the %s lock duration", lock)
	}
	if kinds := f.kinds(t); !slices.Equal(kinds, []string{"claimed", "placing"}) || f.plugin.purges != 0 {
		t.Fatalf("statuses %v purges=%d, want one claim held, no release and no purge", kinds, f.plugin.purges)
	}
	f.setPlugin(func(p *fakePlugin) { p.placing = false })
	f.await(t, testRun("run-a"), func(task wire.Task) bool { return task.Status.State == wire.TaskStateCompleted })
	var attempts []int64
	for _, status := range f.eventStatuses(t) {
		if status.Wherry.Kind == "claimed" {
			attempts = append(attempts, status.Wherry.Attempt)
		}
	}
	if !slices.Equal(attempts, []int64{1}) {
		t.Fatalf("claimed at attempts %v, want once at the first delivery", attempts)
	}
}
