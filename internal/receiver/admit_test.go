package receiver

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json/jsontext"
	"errors"
	"net"
	"slices"
	"strings"
	"sync/atomic"
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

func (f fixture) rejected(t *testing.T, record *kgo.Record, reason string) {
	t.Helper()
	if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckReject {
		t.Fatalf("admitted=%v ack=%v, want rejection %q", admitted != nil, ack, reason)
	}
	if !strings.Contains(f.logs.String(), reason) {
		t.Fatalf("fixed rejection reason %q missing from %q", reason, f.logs.String())
	}
}

func TestFirstTurnPromptUnchanged(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.submit(t, f.host, func(task *wire.Submission) {
		task.Message.Parts = []wire.Part{{Text: "line one"}, {Text: "--- turn 1 ---"}, {Text: "line three\n"}}
	})
	if got := prompt(f.admitted(t, record).history); got != "line one\n--- turn 1 ---\nline three\n" {
		t.Fatalf("prompt %q", got)
	}
}

func TestAdmitAcceptsValidTask(t *testing.T) {
	f := newFixture(t, resultStream)
	admitted := f.admitted(t, f.submit(t, f.host, nil))
	if admitted.profile.Name != "nomad" || admitted.profile.Agent != "claude" || admitted.profile.Labels["zone"] != "dc1" || prompt(admitted.history) != "hi" {
		t.Fatalf("got %#v", admitted)
	}
}

func TestCommittedTerminalAcknowledgedWithoutWrites(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.submit(t, f.host, nil)
	f.publishStatus(t, record, testReceiver, f.receiver.signing, "terminal", wire.TaskStateCompleted, nil)
	before := len(f.records(t, admin.PoolEventsTopic(testPool)))
	if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckAccept {
		t.Fatalf("admitted=%v ack=%v, want accept", admitted != nil, ack)
	}
	if after := len(f.records(t, admin.PoolEventsTopic(testPool))); after != before || len(f.records(t, admin.TenantTopic(testTenant))) != 0 {
		t.Fatalf("a task with a committed terminal published records: events %d -> %d", before, after)
	}
	// A terminal from outside the pool does not count.
	other := f.submit(t, f.host, func(task *wire.Submission) {
		named(task, "run-b")
	})
	_, foreign, _ := ed25519.GenerateKey(rand.Reader)
	f.publishStatus(t, other, testReceiver, foreign, "terminal", wire.TaskStateCompleted, nil)
	if admitted, _ := f.receiver.admit(f.ctx, other); admitted == nil {
		t.Fatal("an unauthorised terminal stopped the task")
	}
}

func TestUnsignedTaskRejectedSilently(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.produce(t, admin.PoolTasksTopic(testPool), testRun("run-a"), []byte("acl-probe"))
	if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckReject || len(f.records(t, admin.PoolEventsTopic(testPool))) != 0 {
		t.Fatalf("admitted=%v ack=%v", admitted != nil, ack)
	}
}

func TestForeignSignedTaskRejectedSilently(t *testing.T) {
	f := newFixture(t, resultStream)
	_, other, _ := ed25519.GenerateKey(rand.Reader)
	if admitted, ack := f.receiver.admit(f.ctx, f.submit(t, other, nil)); admitted != nil || ack != kgo.AckReject || len(f.records(t, admin.PoolEventsTopic(testPool))) != 0 {
		t.Fatalf("admitted=%v ack=%v", admitted != nil, ack)
	}
	// A record keyed by another run than it names is not admitted either.
	misfiled := f.submit(t, f.host, nil)
	misfiled.Key = []byte(testRun("run-other"))
	if admitted, ack := f.receiver.admit(f.ctx, misfiled); admitted != nil || ack != kgo.AckReject {
		t.Fatalf("misfiled admitted=%v ack=%v", admitted != nil, ack)
	}
}

func TestHostKeyReReadOnUnknownKeyID(t *testing.T) {
	f := newFixture(t, resultStream)
	_, next, _ := ed25519.GenerateKey(rand.Reader)
	owner := signed.HostOwner(testTenant, testHost)
	f.produce(t, admin.HostTopic(testTenant, testHost), "", must(t)(signed.RotateKey(owner, f.host, next)))
	// The receiver's last refresh holds the old key; the task is signed with the new one.
	if admitted, ack := f.receiver.admit(f.ctx, f.submit(t, next, nil)); admitted == nil {
		t.Fatalf("task signed by the host's rotated key was not admitted: ack %v", ack)
	}
}

func TestTaskSignedWithARetiredHostKeyIsRefused(t *testing.T) {
	f := newFixture(t, resultStream)
	_, next, _ := ed25519.GenerateKey(rand.Reader)
	f.produce(t, admin.HostTopic(testTenant, testHost), "", must(t)(signed.RotateKey(signed.HostOwner(testTenant, testHost), f.host, next)))
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	if admitted, ack := f.receiver.admit(f.ctx, f.submit(t, f.host, nil)); admitted != nil || ack != kgo.AckReject {
		t.Fatalf("task signed with the host's retired key: admitted=%v ack=%v", admitted != nil, ack)
	}
}

func TestAdmitRejections(t *testing.T) {
	past := time.Now().Add(-time.Second).Unix()
	for _, c := range []struct {
		name   string
		reason string
		mutate func(*wire.Submission)
	}{
		{"PastStartBy", "not started by task_started_by (" + time.Unix(past, 0).UTC().Format(time.RFC3339) + ")", func(task *wire.Submission) {
			task.Wherry.TaskStartedBy = past
		}},
		{"PastEndBy", "not ended by task_ended_by (" + time.Unix(past, 0).UTC().Format(time.RFC3339) + ")", func(task *wire.Submission) {
			task.Wherry.TaskEndedBy = past
		}},
		{"AgentOutsideTerms", "agent is outside the pool terms", func(task *wire.Submission) { task.Wherry.Placement.Agent = "gemini" }},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.rejected(t, f.submit(t, f.host, c.mutate), c.reason)
			events, feed := f.eventStatuses(t), f.feedStatuses(t)
			if len(events) != 1 || len(feed) != 1 || events[0].Wherry.EventID != feed[0].Wherry.EventID || feed[0].Wherry.Kind != "rejected" || feed[0].StatusUpdate.Status.State != wire.TaskStateRejected {
				t.Fatalf("want one REJECTED status on the pool's events and the feed, got %d and %d", len(events), len(feed))
			}
		})
	}
}

// A task is admitted under the pool's current terms, so one whose model has left its agent's models since submit is
// rejected.
func TestAdmitRejectsModelOutsideTheTerms(t *testing.T) {
	models := func(names ...string) admin.Terms {
		terms := testTerms()
		terms.Agents[0].Models, terms.Agents[0].DefaultModel = names, names[0]
		return terms
	}
	f := newFixtureWith(t, resultStream, models("haiku", "sonnet"), testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream}})
	record := f.submit(t, f.host, func(task *wire.Submission) { task.Wherry.Placement.Model = "sonnet" })
	f.admitted(t, record)
	terms := models("haiku")
	encoded := must(t)(admin.EncodeRecord(admin.Record{Kind: "pool", Pool: testPool, Terms: &terms}))
	if err := f.service.Operator.ProduceSync(f.ctx, &kgo.Record{Topic: admin.AdminTopic, Value: encoded}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.rejected(t, record, "model not in the pool's terms")
	if !strings.Contains(scrape(t, f.receiver), `wherry_rejections_total{pool="pool-a",reason="model not in the pool's terms",tenant="tenant-a"} 1`) {
		t.Fatal("the rejection is not counted under its fixed reason")
	}
	f.rejected(t, f.submit(t, f.host, func(task *wire.Submission) {
		named(task, "run-b")
	}), "model not in the pool's terms")
}

func TestDispatchCarriesThePlacementModel(t *testing.T) {
	terms := testTerms()
	terms.Agents[0].Models, terms.Agents[0].DefaultModel = []string{"haiku", "sonnet"}, "haiku"
	f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream}})
	if status := f.execute(t, f.submit(t, f.host, func(task *wire.Submission) { task.Wherry.Placement.Model = "sonnet" }), 1); status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	if run := f.lastRun(); run.Model != "sonnet" {
		t.Fatalf("dispatched model %q, want the placement's", run.Model)
	}
}

func TestRunTimeIsCappedSixHoursAfterTheStartBy(t *testing.T) {
	startBy, twoHours := time.Now().Add(10*time.Minute).Unix(), time.Now().Add(2*time.Hour).Unix()
	for name, test := range map[string]struct {
		endBy int64
		want  time.Time
	}{
		"two hours": {twoHours, time.Unix(twoHours+1, 0)},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			record := f.submit(t, f.host, func(task *wire.Submission) {
				task.Wherry.TaskEndedBy, task.Wherry.TaskStartedBy = test.endBy, startBy
			})
			f.publishStatus(t, record, testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
			// The fixture's context ends within a minute, sooner than either end.
			if status := f.receiver.execute(context.WithoutCancel(f.ctx), f.service.Admin, record, f.admitted(t, record), 1); status != kgo.AckAccept {
				t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
			}
			f.plugin.mu.Lock()
			defer f.plugin.mu.Unlock()
			if !f.plugin.lastDeadline.Equal(test.want) {
				t.Fatalf("run end %v, want %v", f.plugin.lastDeadline, test.want)
			}
		})
	}
}

func TestAdmitRejectsRunPastLifetime(t *testing.T) {
	terms := testTerms()
	terms.StartByLimitMS = 1
	f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream}})
	record := f.turn(t, "run-expired", 1)
	time.Sleep(10 * time.Millisecond)
	f.rejected(t, record, "task_started_by is past the pool's start_by_limit")
}

func TestAdmitRejectsStartByPastTheStartByLimit(t *testing.T) {
	terms := testTerms()
	terms.StartByLimitMS = admin.Millis(time.Hour.Milliseconds())
	f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream}})
	first := f.submit(t, f.host, func(task *wire.Submission) {
		named(task, "run-queued")
		task.Wherry.TaskStartedBy = time.Now().Add(2 * time.Hour).Unix()
	})
	f.rejected(t, first, "task_started_by is past the pool's start_by_limit")
}

func TestRedeliveredClaimedRunPastStartByLimitIsAdopted(t *testing.T) {
	terms := testTerms()
	terms.StartByLimitMS = 50
	f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream, job: pluginsdk.JobRunning}})
	record := startedTurn(t, f, "run-held", func(task *wire.Submission) {
		task.Wherry.TaskStartedBy = time.Now().Unix()
	})
	time.Sleep(60 * time.Millisecond)
	if status := f.receiver.execute(f.ctx, f.service.Admin, record, f.admitted(t, record), 2); status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	if dispatches, lookups, follows, _ := f.plugin.counts(); dispatches != 0 || lookups != 1 || follows != 1 {
		t.Fatalf("dispatches=%d lookups=%d follows=%d, want the running job adopted", dispatches, lookups, follows)
	}
}

// startedTurn submits name's first turn with mutate applied, and its started status as a dead receiver leaves it.
func startedTurn(t *testing.T, f fixture, name string, mutate func(*wire.Submission)) *kgo.Record {
	t.Helper()
	record := f.submit(t, f.host, func(task *wire.Submission) {
		named(task, name)
		mutate(task)
	})
	f.publishStatus(t, record, testReceiver, f.receiver.signing, "started", wire.TaskStateWorking, func(status *wire.RunStatus) {
		status.Wherry.LogOffset = new(int64)
	})
	return record
}

// passed waits until bound, set after its task's append time, has passed.
func passed(bound int64) { time.Sleep(time.Until(wire.PastBound(bound))) }

func noRejection(t *testing.T, f fixture) {
	t.Helper()
	if slices.ContainsFunc(f.eventStatuses(t), func(status wire.RunStatus) bool { return status.Wherry.Kind == "rejected" }) {
		t.Fatalf("a claimed turn was rejected; logs:\n%s", f.logs.String())
	}
}

func TestRedeliveredStartedTurnPastStartByIsAdopted(t *testing.T) {
	f := newFixtureWith(t, resultStream, testTerms(), testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream, job: pluginsdk.JobRunning}})
	startBy := time.Now().Add(time.Second).Unix()
	record := startedTurn(t, f, "run-queued-held", func(task *wire.Submission) { task.Wherry.TaskStartedBy = startBy })
	passed(startBy)
	// The adopter publishes nothing until its own open succeeds.
	if status := f.execute(t, record, 2); status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	if dispatches, lookups, follows, _ := f.plugin.counts(); dispatches != 0 || lookups != 1 || follows != 1 {
		t.Fatalf("dispatches=%d lookups=%d follows=%d, want the running job adopted", dispatches, lookups, follows)
	}
	noRejection(t, f)
	if !slices.ContainsFunc(f.feedStatuses(t), func(status wire.RunStatus) bool {
		return status.Wherry.Kind == "terminal" && status.StatusUpdate.Status.State == wire.TaskStateCompleted
	}) {
		t.Fatal("no COMPLETED terminal on the feed")
	}
}

func TestRedeliveredClaimedTurnPastEndByStopsItsJob(t *testing.T) {
	f := newFixtureWith(t, resultStream, testTerms(), testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream, job: pluginsdk.JobRunning}})
	startBy := time.Now().Add(time.Second).Unix()
	endBy := startBy + 1
	record := startedTurn(t, f, "run-deadline-held", func(task *wire.Submission) {
		task.Wherry.TaskStartedBy, task.Wherry.TaskEndedBy = startBy, endBy
	})
	passed(endBy)
	if status := f.execute(t, record, 2); status != kgo.AckReject {
		t.Fatalf("status %v, want reject; logs:\n%s", status, f.logs.String())
	}
	f.plugin.mu.Lock()
	dispatches, lookups, stops := f.plugin.dispatches, f.plugin.lookups, f.plugin.stops
	f.plugin.mu.Unlock()
	if dispatches != 0 || lookups != 1 || stops != 1 {
		t.Fatalf("dispatches=%d lookups=%d stops=%d, want the found job stopped", dispatches, lookups, stops)
	}
	noRejection(t, f)
	if state, reason := terminalReason(t, f, testRun("run-deadline-held")); state != wire.TaskStateFailed || reason != wire.NotEndedBy(endBy) {
		t.Fatalf("terminal %s %q, want FAILED %q", state, reason, wire.NotEndedBy(endBy))
	}
}

// A claimed task whose job the platform has not started by task_started_by ends FAILED naming the bound, with any job
// purged; the run's own end bound is later.
func TestClaimedTaskNotStartedByItsStartByFails(t *testing.T) {
	f := newFixtureWith(t, resultStream, testTerms(), testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream, placing: true}})
	startBy := time.Now().Add(time.Second).Unix()
	record := f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.TaskStartedBy, task.Wherry.TaskEndedBy = startBy, time.Now().Add(20*time.Second).Unix()
	})
	// The dispatch still placing at task_started_by has an unknown outcome: the task is returned, nothing purged.
	if status := f.execute(t, record, 1); status != kgo.AckRelease {
		t.Fatalf("status %v, want release; logs:\n%s", status, f.logs.String())
	}
	if now := time.Now(); now.Before(wire.PastBound(startBy)) || now.After(wire.PastBound(startBy).Add(5*time.Second)) {
		t.Fatalf("returned at %v, want just past task_started_by %v", now, time.Unix(startBy, 0))
	}
	f.plugin.mu.Lock()
	purged := f.plugin.purges
	f.plugin.mu.Unlock()
	if purged != 0 {
		t.Fatalf("purges=%d after an unresolved dispatch, want 0", purged)
	}
	// The platform now answers: the job was never placed. The redelivery locates it past the bound and ends the run.
	f.plugin.mu.Lock()
	f.plugin.placing, f.plugin.job = false, pluginsdk.JobNeverStarted
	f.plugin.mu.Unlock()
	if status := f.execute(t, record, 2); status != kgo.AckReject {
		t.Fatalf("redelivery status %v, want reject; logs:\n%s", status, f.logs.String())
	}
	if state, reason := terminalReason(t, f, testRun("run-a")); state != wire.TaskStateFailed || reason != wire.NotStartedBy(startBy) {
		t.Fatalf("terminal %s %q, want FAILED %q", state, reason, wire.NotStartedBy(startBy))
	}
	f.plugin.mu.Lock()
	defer f.plugin.mu.Unlock()
	if f.plugin.purges+f.plugin.stops == 0 || f.plugin.follows != 0 {
		t.Fatalf("purges=%d stops=%d follows=%d, want the job removed by its redelivery and never followed", f.plugin.purges, f.plugin.stops, f.plugin.follows)
	}
	if !slices.ContainsFunc(f.eventStatuses(t), func(status wire.RunStatus) bool { return status.Wherry.Kind == "claimed" }) {
		t.Fatal("the task was not claimed before it ended")
	}
}

// A run whose receiver dispatched its job and ended before committing started is held while its redelivery's Lookup
// stays unresolved, past task_started_by, and ends at task_ended_by with the end bound, its job and material kept.
func TestUnstartedRunLookupTimeoutKeepsItsJob(t *testing.T) {
	f := newFixtureWith(t, resultStream, testTerms(), testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream, placing: true}})
	startBy, endBy := time.Now().Add(time.Second).Unix(), time.Now().Add(3*time.Second).Unix()
	record := f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.TaskStartedBy, task.Wherry.TaskEndedBy = startBy, endBy
	})
	f.publishStatus(t, record, testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
	if status := f.execute(t, record, 2); status != kgo.AckReject || time.Now().Before(wire.PastBound(endBy)) {
		t.Fatalf("status %v, want reject at task_ended_by; logs:\n%s", status, f.logs.String())
	}
	f.plugin.mu.Lock()
	purges, releases, dispatches := f.plugin.purges, f.plugin.releases, f.plugin.dispatches
	f.plugin.mu.Unlock()
	if purges != 0 || releases != 0 || dispatches != 0 {
		t.Fatalf("purges=%d releases=%d dispatches=%d, want the unresolved job and its material kept", purges, releases, dispatches)
	}
	if state, reason := terminalReason(t, f, testRun("run-a")); state != wire.TaskStateFailed || reason != wire.NotEndedBy(endBy) {
		t.Fatalf("terminal %s %q, want FAILED %q", state, reason, wire.NotEndedBy(endBy))
	}
	if len(f.openRequests(t)) != 0 {
		t.Fatal("an open was requested before the job's state was known")
	}
}

// A job a lookup first reports after task_started_by, with no started event, is stopped and the run fails; it is never
// adopted.
func TestJobFirstReportedPastStartByIsStopped(t *testing.T) {
	for name, state := range map[string]pluginsdk.JobState{"running": pluginsdk.JobRunning, "ended": pluginsdk.JobEnded} {
		t.Run(name, func(t *testing.T) {
			f := newFixtureWith(t, resultStream, testTerms(), testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream, job: state}})
			startBy := time.Now().Add(time.Second).Unix()
			record := f.submit(t, f.host, func(task *wire.Submission) { task.Wherry.TaskStartedBy = startBy })
			f.publishStatus(t, record, testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
			passed(startBy)
			if status := f.execute(t, record, 2); status != kgo.AckReject {
				t.Fatalf("status %v, want reject; logs:\n%s", status, f.logs.String())
			}
			f.plugin.mu.Lock()
			dispatches, stops, follows := f.plugin.dispatches, f.plugin.stops, f.plugin.follows
			f.plugin.mu.Unlock()
			if dispatches != 0 || stops != 1 || follows != 0 {
				t.Fatalf("dispatches=%d stops=%d follows=%d, want the job stopped and never followed", dispatches, stops, follows)
			}
			if state, reason := terminalReason(t, f, testRun("run-a")); state != wire.TaskStateFailed || reason != wire.NotStartedBy(startBy) {
				t.Fatalf("terminal %s %q, want FAILED %q", state, reason, wire.NotStartedBy(startBy))
			}
			if slices.ContainsFunc(f.eventStatuses(t), func(status wire.RunStatus) bool { return status.Wherry.Kind == "started" }) {
				t.Fatal("a job first reported after task_started_by was published started")
			}
		})
	}
}

// Past task_started_by, a placement no profile offers is rejected when unclaimed; when claimed its job is stopped and
// the run fails, where a task within its bounds is released.
func TestPlacementOutsideOffersPastStartBy(t *testing.T) {
	// codex is inside the terms, but no plugin here launches it.
	outside := func(startBy int64) func(*wire.Submission) {
		return func(task *wire.Submission) { task.Wherry.Placement.Agent, task.Wherry.TaskStartedBy = "codex", startBy }
	}
	t.Run("unclaimed", func(t *testing.T) {
		f := newFixture(t, resultStream)
		startBy := time.Now().Add(-time.Second).Unix()
		f.rejected(t, f.submit(t, f.host, outside(startBy)), wire.NotStartedBy(startBy))
	})
	t.Run("claimed", func(t *testing.T) {
		f := newFixtureWith(t, resultStream, testTerms(), testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream, job: pluginsdk.JobRunning, jobAgent: "codex"}})
		startBy := time.Now().Add(time.Second).Unix()
		record := f.submit(t, f.host, outside(startBy))
		f.publishStatus(t, record, testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
		passed(startBy)
		if status := f.execute(t, record, 2); status != kgo.AckReject {
			t.Fatalf("status %v, want reject; logs:\n%s", status, f.logs.String())
		}
		f.plugin.mu.Lock()
		dispatches, stops := f.plugin.dispatches, f.plugin.stops
		f.plugin.mu.Unlock()
		if dispatches != 0 || stops != 1 {
			t.Fatalf("dispatches=%d stops=%d, want the job stopped", dispatches, stops)
		}
		if state, reason := terminalReason(t, f, testRun("run-a")); state != wire.TaskStateFailed || reason != wire.NotStartedBy(startBy) {
			t.Fatalf("terminal %s %q, want FAILED %q", state, reason, wire.NotStartedBy(startBy))
		}
	})
}

// A claimed run with no offer past a bound, where no profile finds its job, ends with the bound's message.
func TestClaimedRunWithNoOfferAndNoJobEnds(t *testing.T) {
	for name, started := range map[string]bool{"started": true, "unstarted": false} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			startBy := time.Now().Add(time.Second).Unix()
			endBy, past, want := time.Now().Add(time.Minute).Unix(), startBy, wire.NotStartedBy(startBy)
			if started {
				endBy = startBy + 1
				past, want = endBy, wire.NotEndedBy(endBy)
			}
			// codex is inside the terms, but no plugin here launches it.
			record := f.submit(t, f.host, func(task *wire.Submission) {
				task.Wherry.Placement.Agent, task.Wherry.TaskStartedBy, task.Wherry.TaskEndedBy = "codex", startBy, endBy
			})
			f.publishStatus(t, record, testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
			if started {
				f.publishStatus(t, record, testReceiver, f.receiver.signing, "started", wire.TaskStateWorking, func(status *wire.RunStatus) {
					status.Wherry.LogOffset = new(int64)
				})
			}
			passed(past)
			admitted := f.admitted(t, record)
			if admitted.profile.Plugin != nil {
				t.Fatalf("admitted with profile %q, want none", admitted.profile.Name)
			}
			if status := f.receiver.execute(f.ctx, f.service.Admin, record, admitted, 2); status != kgo.AckReject {
				t.Fatalf("status %v, want reject; logs:\n%s", status, f.logs.String())
			}
			if state, reason := terminalReason(t, f, testRun("run-a")); state != wire.TaskStateFailed || reason != want {
				t.Fatalf("terminal %s %q, want FAILED %q", state, reason, want)
			}
			if dispatches, lookups, _, _ := f.plugin.counts(); dispatches != 0 || lookups != 1 {
				t.Fatalf("dispatches=%d lookups=%d, want one lookup and no dispatch", dispatches, lookups)
			}
		})
	}
}

// A started run whose lookup runs out at task_ended_by is returned for a retry, with its job and material kept.
func TestStartedRunLookupTimeoutPastEndByKeepsItsJob(t *testing.T) {
	f := newFixtureWith(t, resultStream, testTerms(), testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream, placing: true}})
	endBy := time.Now().Add(2 * time.Second).Unix()
	record := startedTurn(t, f, "run-unresolved", func(task *wire.Submission) {
		task.Wherry.TaskStartedBy, task.Wherry.TaskEndedBy = endBy-1, endBy
	})
	if status := f.execute(t, record, 2); status != kgo.AckReject {
		t.Fatalf("status %v, want reject; logs:\n%s", status, f.logs.String())
	}
	f.plugin.mu.Lock()
	purges, releases := f.plugin.purges, f.plugin.releases
	f.plugin.mu.Unlock()
	if purges != 0 || releases != 0 {
		t.Fatalf("purges=%d releases=%d, want the unresolved job and its material kept", purges, releases)
	}
	statuses := f.eventStatuses(t)
	if terminal := statuses[len(statuses)-1]; terminal.Wherry.Kind != "terminal" || terminal.Wherry.Event != "" || terminal.StatusUpdate.Status.Message.Parts[0].Text != wire.NotEndedBy(endBy) {
		t.Fatalf("terminal %+v, want the end-bound failure", terminal)
	}
}

func TestAdmitRejectsEndByPastTheRunCap(t *testing.T) {
	f := newFixture(t, resultStream)
	f.rejected(t, f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.TaskStartedBy, task.Wherry.TaskEndedBy = time.Now().Add(10*time.Minute).Unix(), time.Now().Add(7*time.Hour).Unix()
	}), "task_ended_by is past task_started_by plus the 6h run cap")
}

// An over-cap submission no ready profile offers is rejected, not released.
func TestAdmitRejectsEndByPastTheRunCapWithNoOffer(t *testing.T) {
	overCap := func(task *wire.Submission) {
		task.Wherry.TaskStartedBy, task.Wherry.TaskEndedBy = time.Now().Add(10*time.Minute).Unix(), time.Now().Add(7*time.Hour).Unix()
	}
	t.Run("no offer", func(t *testing.T) {
		f := newFixture(t, resultStream)
		// codex is inside the terms, but no plugin here launches it.
		f.rejected(t, f.submit(t, f.host, func(task *wire.Submission) {
			overCap(task)
			task.Wherry.Placement.Agent = "codex"
		}), "task_ended_by is past task_started_by plus the 6h run cap")
	})
	t.Run("unready", func(t *testing.T) {
		f := newFixture(t, resultStream)
		f.plugin.mu.Lock()
		f.plugin.unready = true
		f.plugin.mu.Unlock()
		if err := f.receiver.advertise(f.ctx); err != nil {
			t.Fatal(err)
		}
		f.rejected(t, f.submit(t, f.host, overCap), "task_ended_by is past task_started_by plus the 6h run cap")
	})
}

// A job that started by task_started_by keeps running past it to its end.
func TestStartedJobRunsPastItsStartBy(t *testing.T) {
	f := newFixture(t, resultStream)
	f.plugin.endDelay = 3 * time.Second
	startBy := time.Now().Add(time.Second).Unix()
	record := f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.TaskStartedBy, task.Wherry.TaskEndedBy = startBy, time.Now().Add(30*time.Second).Unix()
	})
	if status := f.execute(t, record, 1); status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	if time.Now().Before(wire.PastBound(startBy)) {
		t.Fatal("the run ended before task_started_by passed")
	}
	if state, _ := terminalReason(t, f, testRun("run-a")); state != wire.TaskStateCompleted {
		t.Fatalf("terminal %s, want COMPLETED", state)
	}
}

func TestPlacementOutsideOffersReleasesWithoutARecord(t *testing.T) {
	f := newFixture(t, resultStream)
	for name, mutate := range map[string]func(*wire.Submission){
		// codex is inside the terms, but no plugin here launches it.
		"agent":    func(task *wire.Submission) { task.Wherry.Placement.Agent = "codex" },
		"selector": func(task *wire.Submission) { task.Wherry.Placement.Selector = []wire.Requirement{in("zone", "dc9")} },
	} {
		t.Run(name, func(t *testing.T) {
			before := len(f.records(t, admin.PoolEventsTopic(testPool)))
			record := f.submit(t, f.host, mutate)
			if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckRelease {
				t.Fatalf("admitted=%v ack=%v, want release", admitted != nil, ack)
			}
			if after := len(f.records(t, admin.PoolEventsTopic(testPool))); after != before {
				t.Fatalf("pool events %d -> %d, want none written", before, after)
			}
		})
	}
	if len(f.feedStatuses(t)) != 0 {
		t.Fatal("an unoffered placement wrote a status to the feed")
	}
}

func TestPlacementSelectorSelectsAnOffer(t *testing.T) {
	f := newFixture(t, resultStream)
	admitted := f.admitted(t, f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Placement.Selector = []wire.Requirement{in("zone", "dc1"), exists("example.com/gpu")}
	}))
	if admitted.profile.Name != "nomad" || admitted.profile.Labels["example.com/gpu"] != "none" {
		t.Fatalf("profile %#v", admitted.profile)
	}
}

func TestDuplicateSubmissionRejected(t *testing.T) {
	f := newFixture(t, resultStream)
	accepted := f.turn(t, "run-duplicate-root", 1)
	duplicate := f.submit(t, f.host, func(task *wire.Submission) {
		named(task, "run-duplicate-root")
		task.Message.MessageID = "msg-duplicate-root"
	})
	f.admitted(t, accepted)
	f.rejected(t, duplicate, "duplicate submission")
}

func TestReceiverRejectsURLParts(t *testing.T) {
	for name, part := range map[string]wire.Part{
		"url": {URL: "https://example.test/a"},
		// The git context part that the git input replaced, as wherry submit built it.
		"git context": {URL: "https://git.example.test/org/repo.git", MediaType: "application/vnd.wherry.git", Metadata: jsontext.Value(`{"ref":"main"}`)},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.submit(t, f.host, func(task *wire.Submission) { task.Message.Parts = append(task.Message.Parts, part) })
			if err := f.receiver.claimOne(f.ctx, f.ctx, func() {}); err != nil {
				t.Fatal(err)
			}
			if dispatches, _, _, _ := f.plugin.counts(); dispatches != 0 {
				t.Fatalf("dispatched %d runs", dispatches)
			}
			statuses := f.feedStatuses(t)
			if len(statuses) != 1 {
				t.Fatalf("feed statuses=%d, want one", len(statuses))
			}
			got := statuses[0].StatusUpdate.Status
			if got.State != wire.TaskStateRejected || statuses[0].Wherry.Kind != "rejected" || got.Message == nil || got.Message.Parts[0].Text != "unsupported message part" {
				t.Fatalf("status %#v", statuses[0])
			}
		})
	}
}

func TestRejectionPublishFailureReleases(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.TaskStartedBy = time.Now().Add(-time.Second).Unix()
	})
	f.cluster.ControlKey(kmsg.Produce.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		req := request.(*kmsg.ProduceRequest)
		response := req.ResponseKind().(*kmsg.ProduceResponse)
		response.SetVersion(req.Version)
		for _, topic := range req.Topics {
			out := kmsg.NewProduceResponseTopic()
			out.Topic, out.TopicID = topic.Topic, topic.TopicID
			for _, partition := range topic.Partitions {
				result := kmsg.NewProduceResponseTopicPartition()
				result.Partition = partition.Partition
				result.ErrorCode = kerr.InvalidRecord.Code
				out.Partitions = append(out.Partitions, result)
			}
			response.Topics = append(response.Topics, out)
		}
		return response, nil, true
	})
	if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckRelease {
		t.Fatalf("admitted=%v ack=%v, want nil and release", admitted != nil, ack)
	}
	if n := len(f.eventStatuses(t)); n != 0 {
		t.Fatalf("failed rejection publish left %d events", n)
	}
}

func TestCancelledSubmissionNotDispatched(t *testing.T) {
	for _, test := range []struct {
		name    string
		attempt int64
		job     pluginsdk.JobState
		stops   int
	}{
		{"queued", 1, pluginsdk.JobAbsent, 0},
		{"redelivered with its job running", 2, pluginsdk.JobRunning, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.plugin.job = test.job
			record := f.submit(t, f.host, nil)
			f.publishCancel(t, testRun("run-a"), 1, f.host, nil)
			if status := f.execute(t, record, test.attempt); status != kgo.AckAccept {
				t.Fatalf("status %v, want accept", status)
			}
			f.plugin.mu.Lock()
			dispatches, stops, releaseIDs := f.plugin.dispatches, f.plugin.stops, f.plugin.releaseIDs
			f.plugin.mu.Unlock()
			if dispatches != 0 || stops != test.stops || !slices.Equal(releaseIDs, []string{testRun("run-a")}) {
				t.Fatalf("dispatches=%d stops=%d releases=%v, want 0, %d and run-a", dispatches, stops, releaseIDs, test.stops)
			}
			if task := f.lastTask(t); task.ID != testRun("run-a") || task.Status.State != wire.TaskStateCanceled {
				t.Fatalf("task %#v, want CANCELED", task)
			}
			if state, reason := terminalReason(t, f, testRun("run-a")); state != wire.TaskStateCanceled || reason != "canceled by user" {
				t.Fatalf("terminal %s %q", state, reason)
			}
		})
	}
}

func TestCancelSinceLastReadStopsDispatch(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.submit(t, f.host, nil)
	f.produce(t, admin.PoolEventsTopic(testPool), testRun("run-a"), must(t)(wire.SignRecord(&wire.Cancel{Tenant: testTenant, Host: testHost, Pool: testPool, Run: testRun("run-a")}, f.host)))
	if admitted := f.admitted(t, record); !admitted.cancelled {
		t.Fatalf("admission %#v, want the run admitted for cancellation", admitted)
	}
}

func TestCancelMustBeHostSignedForTheRunsTenant(t *testing.T) {
	f := newFixture(t, resultStream)
	if err := f.service.TenantAdd(f.ctx, "tenant-b", testPool); err != nil {
		t.Fatal(err)
	}
	if err := f.service.HostAdd(f.ctx, "tenant-b", "host-b", t.TempDir()+"/host-b.yaml"); err != nil {
		t.Fatal(err)
	}
	_, otherHost, _ := ed25519.GenerateKey(rand.Reader)
	f.produce(t, admin.HostTopic("tenant-b", "host-b"), "", must(t)(signed.RegisterKey(signed.HostOwner("tenant-b", "host-b"), otherHost)))
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	record := f.submit(t, f.host, nil)
	f.publishStatus(t, record, testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
	_, foreign, _ := ed25519.GenerateKey(rand.Reader)
	for name, test := range map[string]struct {
		key    ed25519.PrivateKey
		mutate func(*wire.Cancel)
	}{
		"receiver key":          {f.receiver.signing, nil},
		"unregistered key":      {foreign, nil},
		"another tenant's host": {otherHost, func(cancel *wire.Cancel) { cancel.Tenant, cancel.Host = "tenant-b", "host-b" }},
	} {
		f.publishCancel(t, testRun("run-a"), 1, test.key, test.mutate)
		if _, cancelled := f.receiver.cancelled[runKey{testTenant, testRun("run-a")}]; cancelled {
			t.Fatalf("receiver honoured a cancel signed by the %s", name)
		}
	}
	f.publishCancel(t, testRun("run-a"), 1, f.host, nil)
	if _, cancelled := f.receiver.cancelled[runKey{testTenant, testRun("run-a")}]; !cancelled {
		t.Fatal("receiver ignored the host's cancel")
	}
}

func terminalReason(t *testing.T, f fixture, run string) (wire.TaskState, string) {
	t.Helper()
	for _, status := range f.feedStatuses(t) {
		if status.Wherry.Run == run && status.Wherry.Kind == "terminal" {
			return status.StatusUpdate.Status.State, status.StatusUpdate.Status.Message.Parts[0].Text
		}
	}
	t.Fatal("no terminal status on the feed")
	return "", ""
}

// startInFlight claims a submitted run with the receiver and returns once it is following its job.
func startInFlight(t *testing.T, f fixture, ctx context.Context, name string) <-chan error {
	t.Helper()
	f.submit(t, f.host, func(task *wire.Submission) {
		named(task, name)
	})
	done := make(chan error, 1)
	go func() { done <- f.receiver.claimOne(ctx, ctx, func() {}) }()
	for {
		if _, _, follows, _ := f.plugin.counts(); follows == 1 {
			return done
		}
		if f.ctx.Err() != nil {
			t.Fatalf("run never started; logs:\n%s", f.logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// pollPool joins the pool's share group as the superuser and reports whether run is delivered within wait.
func (f fixture) pollPool(t *testing.T, run string, wait time.Duration) (*kgo.Record, bool) {
	t.Helper()
	member, err := kgo.NewClient(append(f.service.Admin.Opts(), kgo.ConsumeTopics(admin.PoolTasksTopic(testPool)), kgo.ShareGroup(admin.PoolGroup(testPool)))...)
	if err != nil {
		t.Fatal(err)
	}
	defer member.Close()
	ctx, cancel := context.WithTimeout(f.ctx, wait)
	defer cancel()
	for ctx.Err() == nil {
		for _, record := range member.PollRecords(ctx, 10).Records() {
			if string(record.Key) == run {
				return record, true
			}
		}
	}
	return nil, false
}

func TestCancelStopsInFlightRun(t *testing.T) {
	f := newFixture(t, noResultStream)
	f.plugin.endDelay = 30 * time.Second
	done := startInFlight(t, f, f.ctx, "run-cancelled")
	f.publishCancel(t, testRun("run-cancelled"), 1, f.host, nil)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("in-flight run kept going after its cancel")
	}
	f.plugin.mu.Lock()
	stops, releaseIDs := f.plugin.stops, f.plugin.releaseIDs
	f.plugin.mu.Unlock()
	if stops != 1 || !slices.Equal(releaseIDs, []string{testRun("run-cancelled")}) {
		t.Fatalf("stops=%d releases=%v", stops, releaseIDs)
	}
	if task := f.lastTask(t); task.ID != testRun("run-cancelled") || task.Status.State != wire.TaskStateCanceled {
		t.Fatalf("task %#v, want CANCELED", task)
	}
	if state, reason := terminalReason(t, f, testRun("run-cancelled")); state != wire.TaskStateCanceled || reason != "canceled by user" {
		t.Fatalf("terminal %s %q", state, reason)
	}
	// Accepted, not released: the pool never sees the submission again.
	if record, delivered := f.pollPool(t, testRun("run-cancelled"), 2*time.Second); delivered {
		t.Fatalf("cancelled run's submission was redelivered (delivery %d)", record.DeliveryCount())
	}
}

func TestRunningTurnSeesCancelBetweenProgressTicks(t *testing.T) {
	f := newFixture(t, noResultStream)
	f.plugin.endDelay = 30 * time.Second
	renewInterval = time.Minute // only the state check, not a progress tick, can stop the run in time
	done := startInFlight(t, f, f.ctx, "run-cancelled")
	f.publishCancel(t, testRun("run-cancelled"), 1, f.host, nil)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * stateCheckInterval):
		t.Fatal("running turn missed a cancel the receiver held")
	}
	if state, reason := terminalReason(t, f, testRun("run-cancelled")); state != wire.TaskStateCanceled || reason != "canceled by user" {
		t.Fatalf("terminal %s %q", state, reason)
	}
}

func TestProgressTickHonoursHeldCancel(t *testing.T) {
	f := newFixture(t, noResultStream)
	f.plugin.endDelay = 30 * time.Second
	stateCheckInterval = time.Minute // only a progress tick can stop the run in time
	t.Cleanup(func() { stateCheckInterval = time.Second })
	done := startInFlight(t, f, f.ctx, "run-cancelled")
	f.publishCancel(t, testRun("run-cancelled"), 1, f.host, nil)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * renewInterval):
		t.Fatal("progress committed past a cancel the receiver held")
	}
	if state, _ := terminalReason(t, f, testRun("run-cancelled")); state != wire.TaskStateCanceled {
		t.Fatalf("terminal %s, want CANCELED", state)
	}
}

func TestRunsUnderRetention(t *testing.T) {
	terms := testTerms()
	terms.StartByLimitMS = admin.Millis(time.Hour.Milliseconds())
	f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream, job: pluginsdk.JobRunning}})
	aged := func(topic, key string, value []byte, age time.Duration) *kgo.Record {
		t.Helper()
		record := &kgo.Record{Topic: topic, Key: []byte(key), Value: value, Timestamp: time.Now().Add(-age)}
		if err := f.service.Admin.ProduceSync(f.ctx, record).FirstErr(); err != nil {
			t.Fatal(err)
		}
		return record
	}
	run := func(name string, age time.Duration, kind string, state wire.TaskState) (*kgo.Record, *wire.Submission) {
		t.Helper()
		task := testSubmission(name)
		f.seal(t, task)
		record := aged(admin.PoolTasksTopic(testPool), task.Wherry.Run, must(t)(wire.SignRecord(task, f.host)), age)
		status := f.receiver.status(task, wire.TaskHash(record.Value), 1, kind, taskStatus(state, ""), nil)
		aged(admin.PoolEventsTopic(testPool), task.Wherry.Run, must(t)(wire.SignRecord(status, f.receiver.signing)), age)
		return record, task
	}
	run("run-ancient", 32*time.Hour, "terminal", wire.TaskStateCompleted)
	completed, _ := run("run-completed", 14*time.Hour, "terminal", wire.TaskStateCompleted)
	adopted, adoptedTask := run("run-adopted", 50*time.Minute, "started", wire.TaskStateWorking)
	aged(admin.ReceiverProgressTopic(testReceiver), testRun("run-adopted"), progressRecord(t, wire.TaskHash(adopted.Value), 41, nil), 50*time.Minute)
	f.cluster.ApplyRetention()

	for _, topic := range []string{admin.PoolTasksTopic(testPool), admin.PoolEventsTopic(testPool)} {
		if slices.ContainsFunc(f.records(t, topic), func(record *kgo.Record) bool { return string(record.Key) == testRun("run-ancient") }) {
			t.Fatalf("%s kept a record past its retention", topic)
		}
	}
	before := len(f.records(t, admin.PoolEventsTopic(testPool)))
	if admitted, ack := f.receiver.admit(f.ctx, completed); admitted != nil || ack != kgo.AckAccept {
		t.Fatalf("redelivered completed run admitted=%v ack=%v, want accept; logs:\n%s", admitted != nil, ack, f.logs.String())
	}
	if after := len(f.records(t, admin.PoolEventsTopic(testPool))); after != before {
		t.Fatalf("a redelivered completed run wrote events %d -> %d", before, after)
	}
	if offset, _, err := f.receiver.lastOffset(f.ctx, adoptedTask, wire.TaskHash(adopted.Value)); err != nil || offset != 41 {
		t.Fatalf("adoption offset %d, %v; want 41", offset, err)
	}
	if status := f.receiver.execute(f.ctx, f.service.Admin, adopted, f.admitted(t, adopted), 2); status != kgo.AckAccept {
		t.Fatalf("adopted status %v; logs:\n%s", status, f.logs.String())
	}
	if dispatches, _, follows, _ := f.plugin.counts(); dispatches != 0 || follows != 1 {
		t.Fatalf("dispatches=%d follows=%d, want the running job adopted", dispatches, follows)
	}
}

// startPeer provisions and configures a second member of the pool and refreshes the fixture receiver.
func (f fixture) startPeer(t *testing.T, id string) *Receiver {
	t.Helper()
	peer, _ := f.newReceiver(t, testConfig(id))
	if err := peer.register(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := peer.configure(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	return peer
}

func TestCommittedRejectionAcknowledged(t *testing.T) {
	f := newFixture(t, resultStream)
	past := time.Now().Add(-time.Second).Unix()
	record := f.submit(t, f.host, func(task *wire.Submission) { task.Wherry.TaskStartedBy = past })
	f.rejected(t, record, wire.NotStartedBy(past))
	before := len(f.records(t, admin.PoolEventsTopic(testPool)))
	if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckAccept {
		t.Fatalf("redelivered rejection admitted=%v ack=%v, want accept", admitted != nil, ack)
	}
	if after := len(f.records(t, admin.PoolEventsTopic(testPool))); after != before {
		t.Fatalf("a redelivered rejection was written again: events %d -> %d", before, after)
	}
}

func TestCommittedTerminalUnderRotatedPeerKey(t *testing.T) {
	f := newFixture(t, resultStream)
	peerSigning := f.addPeer(t, "receiver-b", testPool)
	_, next, _ := ed25519.GenerateKey(rand.Reader)
	f.produce(t, admin.ReceiverTopic("receiver-b"), "receiver-b", must(t)(signed.RotateKey("receiver-b", peerSigning, next)))
	record := f.submit(t, f.host, nil)
	f.publishStatus(t, record, "receiver-b", next, "terminal", wire.TaskStateCompleted, nil)
	if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckAccept {
		t.Fatalf("admitted=%v ack=%v: a terminal under the peer's rotated key must be acknowledged, not run again", admitted != nil, ack)
	}
}

func TestRejectionCarriesItsEcho(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.submit(t, f.host, func(task *wire.Submission) { task.Wherry.Placement.Agent = "gemini" })
	f.rejected(t, record, "agent is outside the pool terms")
	feed := f.records(t, admin.TenantTopic(testTenant))
	var echo wire.Echo
	var rejected wire.RunStatus
	if len(feed) != 2 || wire.ParseRecord(feed[0].Value, f.keys(), &echo) != nil || wire.ParseRecord(feed[1].Value, f.keys(), &rejected) != nil {
		t.Fatalf("feed records %d, want the echo then the rejected status", len(feed))
	}
	if echo.Pool != testPool || echo.Run != testRun("run-a") || echo.TaskHash != wire.TaskHash(record.Value) || wire.CheckEchoRunID(echo) != nil || rejected.Wherry.Kind != "rejected" {
		t.Fatalf("echo %#v status %#v", echo, rejected.Wherry)
	}
	if feed[1].Offset != feed[0].Offset+1 || feed[1].ProducerID != feed[0].ProducerID {
		t.Fatal("the echo and the rejected status are not one transaction")
	}
}

func TestRejectionIsAllOrNothing(t *testing.T) {
	f := newFixture(t, resultStream)
	// Only the feed write is taken away: the pool's events must not get the rejection alone.
	f.deleteReceiverACLs(t, kadm.NewACLs().Topics(admin.TenantTopic(testTenant)).Operations(kadm.OpWrite))
	halt := f.halts()
	record := f.submit(t, f.host, func(task *wire.Submission) { task.Wherry.TaskStartedBy = time.Now().Add(-time.Second).Unix() })
	if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckRelease {
		t.Fatalf("admitted=%v ack=%v, want release", admitted != nil, ack)
	}
	if n := len(f.eventStatuses(t)); n != 0 {
		t.Fatalf("pool events hold %d statuses of a rejection the feed never got", n)
	}
	if err := halt(); err == nil || !strings.HasPrefix(err.Error(), "denied write: ") {
		t.Fatalf("an authorisation error writing the rejection stopped the receiver with %v", err)
	}
}

func TestAdmitReleasesAtTheBudgetFail(t *testing.T) {
	terms := testTerms()
	// With a limit of 2, a first attempt is already at the budget.
	terms.DeliveryLimit = 2
	for name, test := range map[string]struct {
		mutate func(*wire.Submission)
		fault  func(*testing.T, fixture)
	}{
		"offer unavailable": {func(task *wire.Submission) { task.Wherry.Placement.Agent = "codex" }, func(*testing.T, fixture) {}},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream}})
			record := f.submit(t, f.host, test.mutate)
			test.fault(t, f)
			if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckReject {
				t.Fatalf("admitted=%v ack=%v, want reject; logs:\n%s", admitted != nil, ack, f.logs.String())
			}
			if state, reason := terminalReason(t, f, testRun("run-a")); state != wire.TaskStateFailed || reason != "delivery budget exhausted" {
				t.Fatalf("terminal %s %q", state, reason)
			}
			if task := f.lastTask(t); task.Status.State != wire.TaskStateFailed {
				t.Fatalf("result %#v, want FAILED", task.Status)
			}
			var echo wire.Echo
			if records := f.records(t, admin.TenantTopic(testTenant)); len(records) == 0 || wire.ParseRecord(records[0].Value, f.keys(), &echo) != nil || echo.Run != testRun("run-a") || echo.Pool != testPool {
				t.Fatal("the feed does not open with the run's echo, which hosts need to find its pool")
			}
		})
	}
}

// tenantB entitles tenant-b to the pool with a registered host and returns that host's key.
func (f fixture) tenantB(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	if err := f.service.TenantAdd(f.ctx, "tenant-b", testPool); err != nil {
		t.Fatal(err)
	}
	return f.hostOf(t, "tenant-b", "host-b")
}

// hostOf registers host for tenant, entitled to the pool, and returns its key.
func (f fixture) hostOf(t *testing.T, tenant, host string) ed25519.PrivateKey {
	t.Helper()
	if err := f.service.HostAdd(f.ctx, tenant, host, t.TempDir()+"/"+host+".yaml"); err != nil {
		t.Fatal(err)
	}
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	f.produce(t, admin.HostTopic(tenant, host), "", must(t)(signed.RegisterKey(signed.HostOwner(tenant, host), key)))
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	return key
}

func asTenantB(task *wire.Submission) {
	task.Wherry.Tenant, task.Wherry.Host, task.Message.MessageID = "tenant-b", "host-b", "msg-b"
}

// errMismatch is the rejection of a task whose run ID its tenant, host and nonce do not derive.
const errMismatch = "run ID does not match its signer"

// A run ID that a nonce derives for tenant-a's host does not derive for tenant-b's host of the same name.
func TestRunIDOfAnotherTenantIsRejected(t *testing.T) {
	f := newFixture(t, resultStream)
	if err := f.service.TenantAdd(f.ctx, "tenant-b", testPool); err != nil {
		t.Fatal(err)
	}
	keyB := f.hostOf(t, "tenant-b", testHost)
	holder := testActiveRun(t, f, "run-a", 1)
	if err := holder.commit(f.ctx, holder.statusRecords("claimed", taskStatus(wire.TaskStateWorking, ""), nil, nil)...); err != nil {
		t.Fatal(err)
	}
	before := len(f.records(t, admin.TenantTopic(testTenant)))
	f.rejected(t, f.submit(t, keyB, func(task *wire.Submission) { task.Wherry.Tenant, task.Message.MessageID = "tenant-b", "msg-b" }), errMismatch)
	if dispatches, _, _, _ := f.plugin.counts(); dispatches != 0 {
		t.Fatalf("dispatches %d, want none", dispatches)
	}
	if err := holder.commit(f.ctx, holder.progress(0)); err != nil {
		t.Fatalf("tenant-b's turn fenced tenant-a's holder: %v", err)
	}
	if after := len(f.records(t, admin.TenantTopic(testTenant))); after != before {
		t.Fatalf("tenant-a's feed %d -> %d records, want nothing from tenant-b's turn", before, after)
	}
	var kinds []string
	for _, record := range f.records(t, admin.TenantTopic("tenant-b")) {
		var echo wire.Echo
		var status wire.RunStatus
		switch {
		case wire.ParseRecord(record.Value, f.keys(), &echo) == nil:
			kinds = append(kinds, "echo")
		case wire.ParseRecord(record.Value, f.keys(), &status) == nil:
			kinds = append(kinds, status.Wherry.Kind+" "+status.StatusUpdate.Status.Message.Parts[0].Text)
		default:
			kinds = append(kinds, "other")
		}
	}
	if !slices.Equal(kinds, []string{"echo", "rejected " + errMismatch}) {
		t.Fatalf("tenant-b's feed %v, want only its echo and rejection", kinds)
	}
}

// A run ID that a nonce derives for host-a does not derive for host-b of the same tenant.
func TestRunIDOfAnotherHostIsRejected(t *testing.T) {
	f := newFixture(t, resultStream)
	other := f.hostOf(t, testTenant, "host-b")
	f.rejected(t, f.submit(t, other, func(task *wire.Submission) { task.Wherry.Host = "host-b" }), errMismatch)
}

// A nonce that is missing or not canonical is rejected before any claim, even under the run ID it derives.
func TestNonCanonicalRunNonceIsRejected(t *testing.T) {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	valid := testNonce("run-a")
	for name, nonce := range map[string]string{
		"missing":                     "",
		"padded":                      valid + "==",
		"21 characters":               valid[:21],
		"23 characters":               valid + "A",
		"non-canonical trailing bits": valid[:21] + string(alphabet[strings.IndexByte(alphabet, valid[21])+1]),
		"invalid alphabet":            "+" + valid[1:],
		"inserted CR":                 valid[:11] + "\r" + valid[11:],
		"inserted LF":                 valid[:11] + "\n" + valid[11:],
		"CR and LF in 22 characters":  valid[:10] + "\r\n" + valid[10:20],
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.rejected(t, f.submit(t, f.host, func(task *wire.Submission) {
				run := wire.RunID(testTenant, testHost, nonce)
				task.Wherry.Run, task.Wherry.RunNonce, task.Message.TaskID, task.Message.ContextID = run, nonce, run, run
			}), errMismatch)
			if kinds := f.kinds(t); !slices.Equal(kinds, []string{"rejected"}) {
				t.Fatalf("statuses %v, want the rejection alone", kinds)
			}
		})
	}
}

// A mismatch is rejected before the scan of its run's submissions, so an unreadable scan does not release it.
func TestRunIDMismatchIsRejectedWhenTheScanIsUnreadable(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.submit(t, f.tenantB(t), asTenantB)
	f.failReadOnce(t, admin.PoolTasksTopic(testPool))
	f.rejected(t, record, errMismatch)
}

// A mismatching task's redelivery is acknowledged without a second rejection.
func TestRunIDMismatchIsRejectedOnce(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.submit(t, f.tenantB(t), asTenantB)
	f.rejected(t, record, errMismatch)
	if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckAccept {
		t.Fatalf("redelivery admitted=%v ack=%v, want accept", admitted != nil, ack)
	}
	if kinds := f.kinds(t); !slices.Equal(kinds, []string{"rejected"}) {
		t.Fatalf("statuses %v, want one rejection", kinds)
	}
}

func TestRunIDRejectionIsTheSameForEveryMember(t *testing.T) {
	f := newFixture(t, resultStream)
	keyB := f.tenantB(t)
	f.submit(t, f.host, nil)
	record := f.submit(t, keyB, asTenantB)
	peer := f.startPeer(t, "receiver-b")
	if err := peer.advertise(f.ctx); err != nil {
		t.Fatal(err)
	}
	if admitted, ack := peer.admit(f.ctx, record); admitted != nil || ack != kgo.AckReject {
		t.Fatalf("second member admitted=%v ack=%v, want reject", admitted != nil, ack)
	}
	var reasons []string
	keys := signed.Keys{"receiver-b": {Current: peer.signing.Public().(ed25519.PublicKey)}}
	for _, record := range f.records(t, admin.TenantTopic("tenant-b")) {
		var status wire.RunStatus
		if wire.ParseRecord(record.Value, keys, &status) == nil {
			reasons = append(reasons, status.Wherry.Receiver+" "+status.StatusUpdate.Status.Message.Parts[0].Text)
		}
	}
	if !slices.Equal(reasons, []string{"receiver-b " + errMismatch}) {
		t.Fatalf("tenant-b statuses %v, want the second member's run ID rejection", reasons)
	}
}

// failReadOnce fails the next ListOffsets that names topic.
func (f fixture) failReadOnce(t *testing.T, topic string) {
	t.Helper()
	failed := false
	f.cluster.ControlKey(kmsg.ListOffsets.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		req := request.(*kmsg.ListOffsetsRequest)
		if failed || !slices.ContainsFunc(req.Topics, func(listed kmsg.ListOffsetsRequestTopic) bool { return listed.Topic == topic }) {
			f.cluster.KeepControl()
			return nil, nil, false
		}
		failed = true
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
}

func budgetTerms() admin.Terms {
	terms := testTerms()
	// With a limit of 2, a first attempt is already at the budget.
	terms.DeliveryLimit = 2
	return terms
}

func (f fixture) terminals(t *testing.T, topic string) int {
	t.Helper()
	terminals := 0
	for _, status := range f.statuses(t, topic) {
		if status.Wherry.Kind == "terminal" {
			terminals++
		}
	}
	return terminals
}

func TestUnreadableEventsReleaseACompletedRunWithoutASecondTerminal(t *testing.T) {
	f := newFixtureWith(t, resultStream, budgetTerms(), testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream}})
	record := f.submit(t, f.host, nil)
	if status := f.execute(t, record, 1); status != kgo.AckAccept {
		t.Fatalf("first delivery status %v, want accept", status)
	}
	f.failReadOnce(t, admin.PoolEventsTopic(testPool))
	if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckRelease {
		t.Fatalf("redelivery admitted=%v ack=%v, want release", admitted != nil, ack)
	}
	if terminals := f.terminals(t, admin.TenantTopic(testTenant)); terminals != 1 {
		t.Fatalf("terminals %d, want only the run's own", terminals)
	}
}

func TestUnreadableTurnsReleaseAtTheBudget(t *testing.T) {
	f := newFixtureWith(t, resultStream, budgetTerms(), testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream}})
	record := f.submit(t, f.host, nil)
	if _, err := kadm.NewClient(f.service.Admin).DeleteTopics(f.ctx, admin.PoolTasksTopic(testPool)); err != nil {
		t.Fatal(err)
	}
	if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckRelease {
		t.Fatalf("admitted=%v ack=%v, want release: the run's owner is unknown", admitted != nil, ack)
	}
	if records := f.records(t, admin.TenantTopic(testTenant)); len(records) != 0 {
		t.Fatalf("feed records %d, want nothing written under an unowned run ID", len(records))
	}
}

func TestRunIDOfAnotherTenantIsCheckedBeforeAnyWrite(t *testing.T) {
	f := newFixtureWith(t, resultStream, budgetTerms(), testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream}})
	keyB := f.tenantB(t)
	f.submit(t, f.host, nil)
	// No plugin here launches codex, and attempt 1 is at the budget.
	f.rejected(t, f.submit(t, keyB, func(task *wire.Submission) {
		asTenantB(task)
		task.Wherry.Placement.Agent = "codex"
	}), errMismatch)
	if failures := f.startFailures(t); len(failures) != 0 {
		t.Fatalf("start failures %#v, want none under another tenant's run ID", failures)
	}
	if terminals := f.terminals(t, admin.PoolEventsTopic(testPool)); terminals != 0 {
		t.Fatalf("terminals %d, want none under another tenant's run ID", terminals)
	}
}

func TestRunIDOwnershipSurvivesTheOwnersHostEnding(t *testing.T) {
	f := newFixture(t, resultStream)
	keyB := f.tenantB(t)
	holder := testActiveRun(t, f, "run-a", 1)
	if err := holder.commit(f.ctx, holder.statusRecords("claimed", taskStatus(wire.TaskStateWorking, ""), nil, nil)...); err != nil {
		t.Fatal(err)
	}
	denied := func(context.Context, string, string) error { return admin.ErrAuthorizationDenied }
	revocationBounds(t, f.service, "1")
	if result := f.service.RevokeHost(f.ctx, testTenant, testHost, time.Millisecond, denied); !result.Complete {
		t.Fatalf("revoke host: %#v", result.Steps)
	}
	revocationBounds(t, f.service, "")
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.rejected(t, f.submit(t, keyB, asTenantB), errMismatch)
}

func TestCancelOfAnotherTenantDoesNotStopTheRun(t *testing.T) {
	f := newFixture(t, noResultStream)
	keyB := f.tenantB(t)
	f.plugin.endDelay = time.Minute
	record := f.submit(t, f.host, nil)
	// tenant-b's rejection is the first status under run-a.
	f.rejected(t, f.submit(t, keyB, asTenantB), errMismatch)
	admitted := f.admitted(t, record)
	ctx, stop := context.WithCancel(f.ctx)
	defer stop()
	done := make(chan kgo.AckStatus, 1)
	go func() { done <- f.receiver.execute(ctx, f.service.Admin, record, admitted, 1) }()
	for {
		if _, _, follows, _ := f.plugin.counts(); follows > 0 {
			break
		}
		if f.ctx.Err() != nil {
			t.Fatal("the run never followed its job")
		}
		time.Sleep(20 * time.Millisecond)
	}
	f.publishCancel(t, testRun("run-a"), 1, keyB, func(cancel *wire.Cancel) { cancel.Tenant, cancel.Host = "tenant-b", "host-b" })
	time.Sleep(3 * stateCheckInterval)
	f.plugin.mu.Lock()
	stops := f.plugin.stops
	f.plugin.mu.Unlock()
	select {
	case status := <-done:
		t.Fatalf("run ended with %v after tenant-b's cancel, want it still running", status)
	default:
	}
	if stops != 0 {
		t.Fatalf("stops %d, want tenant-a's run untouched by tenant-b's cancel", stops)
	}
	f.publishCancel(t, testRun("run-a"), 1, f.host, nil)
	select {
	case status := <-done:
		if status != kgo.AckAccept {
			t.Fatalf("status %v after tenant-a's cancel, want accept", status)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("tenant-a's own cancel did not stop its run")
	}
	if state, reason := terminalReason(t, f, testRun("run-a")); state != wire.TaskStateCanceled || reason != "canceled by user" {
		t.Fatalf("terminal %s %q, want CANCELED", state, reason)
	}
}

// traffic counts the connections that clients built from a client's options open and the offset listings they send.
type traffic struct{ connections, listings atomic.Int64 }

func (c *traffic) OnBrokerE2E(_ kgo.BrokerMetadata, key int16, _ kgo.BrokerE2E) {
	if key == int16(kmsg.ListOffsets) {
		c.listings.Add(1)
	}
}

func (c *traffic) OnBrokerConnect(_ kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	if err == nil {
		c.connections.Add(1)
	}
}

// Admissions read through the receiver's one reader, so a second admits over the first's connections. kfake runs three
// brokers, so one client opens at most a seed connection and a request and a fetch connection to each; a metadata
// request goes to any broker, so the second may open the request connection to one the first did not need.
func TestAdmissionsShareTheReadersConnections(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	var counted traffic
	base, err := kgo.NewClient(append(f.receiver.base.Opts(), kgo.WithHooks(&counted))...)
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()
	f.receiver.base, f.receiver.reader = base, admin.NewReader(base)
	defer f.receiver.reader.Close()
	var opened, listed []int64
	for _, run := range []string{testRun("run-a"), testRun("run-b")} {
		record := f.turn(t, run, 1)
		connections, listings := counted.connections.Load(), counted.listings.Load()
		f.admitted(t, record)
		opened, listed = append(opened, counted.connections.Load()-connections), append(listed, counted.listings.Load()-listings)
	}
	t.Logf("admissions opened %v connections and sent %v offset listings", opened, listed)
	if opened[0] > 7 || opened[1] > 1 || !slices.Equal(listed, []int64{4, 4}) {
		t.Fatalf("admissions opened %v connections and sent %v offset listings, want at most 7 and then 1, and 4 each", opened, listed)
	}
}

// A claimed run with no offer whose receiver shuts down just after every lookup reports no job never reaches a plugin:
// with the context ended, the bound's branch is skipped, and only the no-profile guard keeps it from dispatching.
func TestClaimedRunWithNoOfferDuringShutdownNeverDispatches(t *testing.T) {
	f := newFixture(t, resultStream)
	startBy := time.Now().Add(time.Second).Unix()
	record := f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Placement.Agent, task.Wherry.TaskStartedBy, task.Wherry.TaskEndedBy = "codex", startBy, time.Now().Add(time.Minute).Unix()
	})
	f.publishStatus(t, record, testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
	passed(startBy)
	admitted := f.admitted(t, record)
	ctx, stop := context.WithCancel(f.ctx)
	defer stop()
	f.plugin.mu.Lock()
	f.plugin.afterLookup = stop
	f.plugin.mu.Unlock()
	f.receiver.execute(ctx, f.service.Admin, record, admitted, 2)
	if dispatches, _, _, _ := f.plugin.counts(); dispatches != 0 {
		t.Fatalf("dispatches=%d during shutdown, want none", dispatches)
	}
}

// A dispatch with an unknown outcome whose reconciliation is still unresolved at task_started_by is returned with its
// job and material kept: the job may exist, and only a lookup that resolves it may stop or purge it.
func TestUnresolvedDispatchPastStartByKeepsItsJob(t *testing.T) {
	f := newFixtureWith(t, resultStream, testTerms(), testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream,
		dispatchErr: errors.New("connection reset"), lookupBlocks: true}})
	record := f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.TaskStartedBy, task.Wherry.TaskEndedBy = time.Now().Add(time.Second).Unix(), time.Now().Add(20*time.Second).Unix()
	})
	if status := f.execute(t, record, 1); status != kgo.AckRelease {
		t.Fatalf("status %v, want release; logs:\n%s", status, f.logs.String())
	}
	f.plugin.mu.Lock()
	purges := f.plugin.purges
	f.plugin.mu.Unlock()
	if purges != 0 {
		t.Fatalf("purges=%d after an unresolved dispatch, want 0", purges)
	}
	if slices.ContainsFunc(f.feedStatuses(t), func(status wire.RunStatus) bool { return status.Wherry.Kind == "terminal" }) {
		t.Fatal("an unresolved dispatch was ended")
	}
}
