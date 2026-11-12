package receiver

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
	"github.com/twmb/franz-go/pkg/kgo"
)

// attemptAt is a record of the run's attempt with its append time.
type attemptAt struct {
	attempt int64
	at      time.Time
}

// retryEvents are the receiver's claimed statuses and start failures on the pool's events, in log order.
func (f fixture) retryEvents(t *testing.T) (claims, failures []attemptAt) {
	t.Helper()
	for _, record := range f.records(t, operator.PoolEventsTopic(testPool)) {
		var status wire.RunStatus
		var failure wire.StartFailure
		switch {
		case recordType(record.Value) == wire.StartFailureType && wire.ParseRecord(record.Value, f.keys(), &failure) == nil:
			failures = append(failures, attemptAt{failure.Wherry.Attempt, record.Timestamp})
		case wire.ParseRecord(record.Value, f.keys(), &status) == nil && status.Wherry.Kind == "claimed":
			claims = append(claims, attemptAt{status.Wherry.Attempt, record.Timestamp})
		}
	}
	return claims, failures
}

// setDuration sets interval to d for the test.
func setDuration(t *testing.T, interval *time.Duration, d time.Duration) {
	previous := *interval
	*interval = d
	t.Cleanup(func() { *interval = previous })
}

// longWait makes the first wait after a start failure last d, inside a window of an hour.
func longWait(t *testing.T, d time.Duration) {
	setDuration(t, &startRetryWait, d)
	setDuration(t, &startRetryWindow, time.Hour)
}

// awaitStartFailures waits for n start failures on the pool's events.
func (f fixture) awaitStartFailures(t *testing.T, n int) {
	t.Helper()
	waitFor(t, f, func() bool { return len(f.startFailures(t)) >= n })
}

// A plugin whose jobs fail to start three times, each converging on the failed job until Purge removes it: one
// delivery retries in place after growing waits, purging each failed job and releasing the run's material once, and
// completes.
func TestStartRetriesInOneDelivery(t *testing.T) {
	f := newFixture(t, resultStream)
	setDuration(t, &openInterval, 20*time.Millisecond)
	failing := 3
	f.plugin.failStart = func(string) bool { failing--; return failing >= 0 }
	f.run(t, f.ctx)
	f.submit(t, f.host, nil)
	if task := f.await(t, testRun("run-a"), anyTask); task.Status.State != wire.TaskStateCompleted {
		t.Fatalf("ended %s; logs:\n%s", task.Status.State, f.logs.String())
	}
	claims, failures := f.retryEvents(t)
	if len(claims) != 4 || len(failures) != 3 || slices.ContainsFunc(append(claims, failures...), func(a attemptAt) bool { return a.attempt != 1 }) {
		t.Fatalf("claims %v and start failures %v, want 4 and 3 at attempt 1", claims, failures)
	}
	if requests := f.openRequests(t); len(requests) != 4 || slices.ContainsFunc(requests, func(request wire.OpenRequest) bool { return request.Attempt != 1 }) {
		t.Fatalf("open requests %d, want 4 at attempt 1", len(requests))
	}
	f.plugin.mu.Lock()
	dispatches, times, calls := f.plugin.dispatches, slices.Clone(f.plugin.dispatchTimes), slices.Clone(f.plugin.calls)
	f.plugin.mu.Unlock()
	if dispatches != 4 || !slices.Equal(calls, []string{"purge", "purge", "purge", "release"}) {
		t.Fatalf("dispatches %d and calls %v, want 4 and a purge per failure then one release", dispatches, calls)
	}
	wait := startRetryWait
	for i := 1; i < len(times); i++ {
		if gap := times[i].Sub(times[i-1]); gap < wait {
			t.Fatalf("dispatch %d followed %d by %s, want at least %s", i+1, i, gap, wait)
		}
		wait = min(2*wait, startRetryMaxWait)
	}
	scraped(t, f.receiver, `wherry_start_failures_total{class="platform",pool="pool-a",profile="nomad"} 3`)
}

// A delivery's start failures past the window release the task to one more delivery; the final attempt starts at the
// window's end, measured from the first failure. On the last delivery, an outage that ends before that attempt is
// survived by it.
func TestStartRetryWindowAndBoundaryRecovery(t *testing.T) {
	t.Run("window", func(t *testing.T) {
		f := newFixture(t, resultStream)
		setDuration(t, &openInterval, 20*time.Millisecond)
		f.plugin.failStart = func(string) bool { return true }
		f.run(t, f.ctx)
		f.submit(t, f.host, nil)
		// The second delivery counts once however often it retries.
		waitFor(t, f, func() bool {
			_, failures := f.retryEvents(t)
			return len(slices.DeleteFunc(failures, func(a attemptAt) bool { return a.attempt != 2 })) >= 2
		})
		scraped(t, f.receiver, `wherry_redeliveries_total{pool="pool-a",profile="nomad",tenant="tenant-a"} 1`)
		claims, failures := f.retryEvents(t)
		var first []attemptAt
		for _, failure := range failures {
			if failure.attempt == 1 {
				first = append(first, failure)
			}
		}
		end := first[0].at.Add(startRetryWindow)
		const slack = 50 * time.Millisecond
		for _, failure := range first[:len(first)-1] {
			if !failure.at.Before(end.Add(slack)) {
				t.Fatalf("a failure retried at %s, past the window's end %s", failure.at, end)
			}
		}
		if last := first[len(first)-1].at; last.Before(end.Add(-slack)) {
			t.Fatalf("released at %s, before the window's end %s", last, end)
		}
		var final time.Time
		for _, claim := range claims {
			if claim.attempt == 1 {
				final = claim.at
			}
		}
		if final.Before(end.Add(-slack)) || final.After(end.Add(3*slack)) {
			t.Fatalf("final attempt claimed at %s, want the window's end %s", final, end)
		}
	})
	t.Run("last delivery", func(t *testing.T) {
		terms := testTerms()
		terms.DeliveryLimit = 2
		f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream}})
		setDuration(t, &openInterval, 20*time.Millisecond)
		var outage time.Time
		f.plugin.failStart = func(string) bool {
			if outage.IsZero() {
				outage = time.Now().Add(startRetryWindow - startRetryWait/2)
			}
			return time.Now().Before(outage)
		}
		f.run(t, f.ctx)
		f.submit(t, f.host, nil)
		if task := f.await(t, testRun("run-a"), anyTask); task.Status.State != wire.TaskStateCompleted {
			t.Fatalf("ended %s, want the final attempt to complete; logs:\n%s", task.Status.State, f.logs.String())
		}
		claims, failures := f.retryEvents(t)
		if slices.ContainsFunc(claims, func(a attemptAt) bool { return a.attempt != 1 }) || len(failures) < 3 {
			t.Fatalf("claims %v and failures %v, want retries within the one delivery", claims, failures)
		}
		if final := claims[len(claims)-1].at; final.Before(failures[0].at.Add(startRetryWindow - 50*time.Millisecond)) {
			t.Fatalf("completed by an attempt at %s, before the window's end", final)
		}
	})
}

// A wait that reaches task_started_by ends the run FAILED with the bound and the last start failure's cause, which was
// redacted when it was stored.
func TestStartRetryBoundFailsWithTheLastCause(t *testing.T) {
	f := newFixture(t, resultStream)
	longWait(t, time.Minute)
	f.plugin.failStart = func(string) bool { return true }
	f.plugin.neverReason = "pull " + testToken + " from https://registry.example/v2/agent?token=" + testToken + "\nretrying\r" + strings.Repeat("é", 400)
	bound := time.Now().Add(2 * time.Second).Unix()
	f.run(t, f.ctx)
	f.submit(t, f.host, func(task *wire.Submission) { task.Wherry.TaskStartedBy = bound })
	task := f.await(t, testRun("run-a"), anyTask)
	if ended := time.Since(wire.PastBound(bound)); ended > 2*time.Second {
		t.Fatalf("ended %s after the bound", ended)
	}
	reason := task.Status.Message.Parts[0].Text
	prefix := wire.NotStartedBy(bound) + "; last: job never started (driver): pull "
	cause, _ := strings.CutPrefix(reason, wire.NotStartedBy(bound)+"; last: ")
	switch {
	case task.Status.State != wire.TaskStateFailed || !strings.HasPrefix(reason, prefix):
		t.Fatalf("ended %s with %q, want FAILED with prefix %q", task.Status.State, reason, prefix)
	case strings.Contains(reason, testToken) || strings.Contains(reason, "https://") || strings.ContainsAny(reason, "\r\n"):
		t.Fatalf("reason %q carries a credential, a URL or a line break", reason)
	case !utf8.ValidString(reason) || len(cause) > maxLogText || !strings.Contains(reason, "[redacted-url]"):
		t.Fatalf("cause %q is not valid UTF-8 within %d bytes with its URL redacted", cause, maxLogText)
	}
	if dispatches, _, _, _ := f.plugin.counts(); dispatches != 1 {
		t.Fatalf("dispatches %d, want the one before the wait", dispatches)
	}
}

// A held cancel ends a waiting run CANCELED at once.
func TestStartRetryWaitEnds(t *testing.T) {
	f := newFixture(t, resultStream)
	longWait(t, 3*time.Second)
	setDuration(t, &stateCheckInterval, 100*time.Millisecond)
	f.plugin.failStart = func(string) bool { return true }
	f.run(t, f.ctx)
	f.submit(t, f.host, nil)
	f.awaitStartFailures(t, 1)
	cancelled := time.Now()
	f.publishCancel(t, testRun("run-a"), 1, f.host, nil)
	task := f.await(t, testRun("run-a"), anyTask)
	if took := time.Since(cancelled); task.Status.State != wire.TaskStateCanceled || took > time.Second {
		t.Fatalf("ended %s %s after the cancel, want CANCELED within 1s", task.Status.State, took)
	}
	if dispatches, _, _, releases := f.plugin.counts(); dispatches != 1 || releases != 1 {
		t.Fatalf("dispatches %d and releases %d, want 1 and 1", dispatches, releases)
	}
}

// A stop or configuration change during a wait releases the task once the idle slots have closed, with the run's
// material released: at replicas 2, the task's next delivery is its second.
func TestStartRetryStopWaitsForIdleClose(t *testing.T) {
	for _, change := range []string{"stop", "configuration"} {
		t.Run(change, func(t *testing.T) {
			config := testConfig(testReceiver)
			config.Profiles[0].Replicas = 2
			f := newFixtureWith(t, resultStream, testTerms(), config, map[string]*fakePlugin{"nomad": {stream: resultStream}})
			longWait(t, time.Minute)
			f.plugin.failStart = func(string) bool { return true }
			ctx, stop := context.WithCancel(f.ctx)
			defer stop()
			done := f.run(t, ctx)
			record := f.submit(t, f.host, nil)
			f.awaitStartFailures(t, 1)
			if change == "stop" {
				stop()
			} else {
				config.Profiles[0].Replicas = 3
				f.appendConfig(t, config)
			}
			f.stopped(t, done, 20*time.Second)
			if kinds := f.kinds(t); kinds[len(kinds)-1] != "released" {
				t.Fatalf("statuses %v, want the task released", kinds)
			}
			if dispatches, _, _, releases := f.plugin.counts(); dispatches != 1 || releases != 1 {
				t.Fatalf("dispatches %d and releases %d, want 1 and 1", dispatches, releases)
			}
			var released time.Time
			for _, record := range f.records(t, operator.PoolEventsTopic(testPool)) {
				var status wire.RunStatus
				if wire.ParseRecord(record.Value, f.keys(), &status) == nil && status.Wherry.Kind == "released" {
					released = record.Timestamp
				}
			}
			// Only the holder's own client leaves after the release.
			if members := slices.DeleteFunc(f.members.all(), func(member shareMember) bool { return member.left.Before(released) }); len(members) != 1 {
				t.Fatalf("members %+v left after the release at %s, want the holder alone", members, released)
			}
			share, err := f.receiver.newClient(kgo.ConsumeTopics(record.Topic), kgo.ShareGroup(operator.PoolGroup(testPool)), kgo.ShareMaxRecords(1), kgo.ShareMaxRecordsStrict())
			if err != nil {
				t.Fatal(err)
			}
			defer share.Close()
			fetches := share.PollRecords(f.ctx, 1)
			if records := fetches.Records(); len(records) != 1 || attemptOf(records[0]) != 2 {
				t.Fatalf("next delivery %v (%v), want the second", records, fetches.Err())
			}
		})
	}
}

// A claim lost during a wait halts the run: nothing more is dispatched, released or published.
func TestStartRetryLostClaimHalts(t *testing.T) {
	f, claim := claimFixture(t)
	longWait(t, 3*time.Second)
	f.plugin.failStart = func(string) bool { return true }
	admitted := f.admitted(t, claim.record)
	done := make(chan kgo.AckStatus, 1)
	go func() { done <- f.receiver.executeDelivery(f.ctx, claim.ctx, claim, claim.record, admitted, 1) }()
	f.awaitStartFailures(t, 1)
	failRenewal(t, f, 1, "refused")
	select {
	case ack := <-done:
		if ack != 0 || !errors.Is(context.Cause(claim.ctx), errClaimLost) {
			t.Fatalf("ack %v with cause %v, want a halt on the lost claim", ack, context.Cause(claim.ctx))
		}
	case <-time.After(startRetryWait):
		t.Fatal("the wait outlived its lost claim")
	}
	if dispatches, _, _, releases := f.plugin.counts(); dispatches != 1 || releases != 0 || slices.Contains(f.kinds(t), "released") {
		t.Fatalf("dispatches %d, releases %d, statuses %v after the claim was lost", dispatches, releases, f.kinds(t))
	}
}

// Each start failure before the run's started status is retried in place: Dispatch creating no job, a reconcile
// finding none, a job removed or never started while awaited, and a redelivery finding a job that never started.
func TestEachStartFailureSiteRetries(t *testing.T) {
	for _, site := range []struct {
		name       string
		attempt    int64
		dispatches int
		fail       func(*fakePlugin)
		recover    func(*fakePlugin)
	}{
		{"created no job", 1, 2, func(p *fakePlugin) { p.outcome = pluginsdk.OutcomeNothingCreated }, func(p *fakePlugin) { p.outcome = 0 }},
		{"reconciled absent", 1, 2, func(p *fakePlugin) { p.dispatchErr = errors.New("register response lost") }, func(p *fakePlugin) { p.dispatchErr = nil }},
		{"removed before it started", 1, 2, func(p *fakePlugin) { p.vanish = true }, func(p *fakePlugin) { p.vanish = false }},
		{"never started", 1, 2, func(p *fakePlugin) { p.neverStarted = true }, func(p *fakePlugin) { p.neverStarted = false }},
		{"adopted never started", 2, 1, func(p *fakePlugin) { p.job = pluginsdk.JobNeverStarted }, func(p *fakePlugin) { p.job = pluginsdk.JobAbsent }},
	} {
		t.Run(site.name, func(t *testing.T) {
			f, claim := claimFixture(t)
			site.fail(f.plugin)
			f.plugin.purgeHook = func() { site.recover(f.plugin) }
			ack := f.receiver.executeDelivery(f.ctx, claim.ctx, claim, claim.record, f.admitted(t, claim.record), site.attempt)
			if ack != kgo.AckAccept {
				t.Fatalf("ack %v, want the retry completed; logs:\n%s", ack, f.logs.String())
			}
			if claims, failures := f.retryEvents(t); len(failures) != 1 || len(claims) != 2 {
				t.Fatalf("start failures %v and claims %v, want 1 and 2", failures, claims)
			}
			if dispatches, _, _, releases := f.plugin.counts(); dispatches != site.dispatches || releases != 1 {
				t.Fatalf("dispatches %d and releases %d, want %d and 1", dispatches, releases, site.dispatches)
			}
		})
	}
}

// A job found never started once its run has started, in this delivery or an earlier one, is not retried in place: the
// task is released, so no replacement job follows a committed transcript.
func TestStartRetryDoesNotFollowAStartedRun(t *testing.T) {
	t.Run("this delivery", func(t *testing.T) {
		f, claim := claimFixture(t)
		never := pluginsdk.JobNeverStarted
		f.plugin.followErr, f.plugin.followedJob = errors.New("stream down"), &never
		if ack := f.receiver.executeDelivery(f.ctx, claim.ctx, claim, claim.record, f.admitted(t, claim.record), 1); ack != kgo.AckRelease {
			t.Fatalf("ack %v, want the task released; logs:\n%s", ack, f.logs.String())
		}
		if dispatches, _, _, _ := f.plugin.counts(); dispatches != 1 || len(f.startFailures(t)) != 1 {
			t.Fatalf("dispatches %d and start failures %d, want 1 and 1", dispatches, len(f.startFailures(t)))
		}
	})
	t.Run("an earlier delivery", func(t *testing.T) {
		f, claim := claimFixture(t)
		f.publishStatus(t, claim.record, testReceiver, f.receiver.signing, "started", wire.TaskStateWorking, func(status *wire.RunStatus) {
			status.Wherry.LogOffset = new(int64)
		})
		f.plugin.job = pluginsdk.JobNeverStarted
		if ack := f.receiver.executeDelivery(f.ctx, claim.ctx, claim, claim.record, f.admitted(t, claim.record), 2); ack != kgo.AckRelease {
			t.Fatalf("ack %v, want the task released; logs:\n%s", ack, f.logs.String())
		}
		if dispatches, _, _, _ := f.plugin.counts(); dispatches != 0 || len(f.startFailures(t)) != 1 {
			t.Fatalf("dispatches %d and start failures %d, want 0 and 1", dispatches, len(f.startFailures(t)))
		}
	})
}

// A run waiting to retry is not pending, so it gates no claim: another slot's task completes during the wait.
func TestStartRetryWaitDoesNotGateClaims(t *testing.T) {
	config := testConfig(testReceiver)
	config.Profiles[0].Replicas = 2
	f := newFixtureWith(t, resultStream, testTerms(), config, map[string]*fakePlugin{"nomad": {stream: resultStream, reasons: []string{"queued"}}})
	longWait(t, time.Minute)
	f.plugin.failStart = func(run string) bool { return run == testRun("run-a") }
	f.run(t, f.ctx)
	f.submit(t, f.host, nil)
	f.awaitKind(t, "placing")
	f.awaitStartFailures(t, 1)
	if pending := f.pendingRuns(); pending != 0 {
		t.Fatalf("%d runs pending during the wait", pending)
	}
	f.submit(t, f.host, func(task *wire.Submission) { named(task, "run-b") })
	f.await(t, testRun("run-b"), func(task wire.Task) bool { return task.Status.State == wire.TaskStateCompleted })
	if dispatches, _, _, _ := f.plugin.counts(); dispatches != 2 {
		t.Fatalf("dispatches %d, want run-a's one and run-b's; logs:\n%s", dispatches, f.logs.String())
	}
}
