package receiver

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json/jsontext"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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
	if admitted.adapter.Name != "nomad" || admitted.adapter.Agent != "claude" || admitted.adapter.Labels["zone"] != "dc1" || prompt(admitted.history) != "hi" {
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
		task.Wherry.Run, task.Message.TaskID, task.Message.ContextID = "run-b", "run-b", "run-b"
	})
	_, foreign, _ := ed25519.GenerateKey(rand.Reader)
	f.publishStatus(t, other, testReceiver, foreign, "terminal", wire.TaskStateCompleted, nil)
	if admitted, _ := f.receiver.admit(f.ctx, other); admitted == nil {
		t.Fatal("an unauthorised terminal stopped the task")
	}
}

func TestUnsignedTaskRejectedSilently(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.produce(t, admin.PoolTasksTopic(testPool), "run-a", []byte("acl-probe"))
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
	misfiled.Key = []byte("run-other")
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

func TestAdmitRejections(t *testing.T) {
	for _, c := range []struct {
		name   string
		reason string
		mutate func(*wire.Submission)
	}{
		{"ExpiredQueueDeadline", "queue deadline passed", func(task *wire.Submission) {
			task.Wherry.QueueDeadlineMS = time.Now().Add(-time.Second).UnixMilli()
		}},
		{"ExpiredRunDeadline", "run deadline passed", func(task *wire.Submission) {
			task.Wherry.DeadlineMS = time.Now().Add(-time.Second).UnixMilli()
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
	f := newFixtureWith(t, resultStream, models("haiku", "sonnet"), testConfig(testReceiver), map[string]*fakeAdapter{"nomad": {stream: resultStream}})
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
		task.Wherry.Run, task.Message.TaskID, task.Message.ContextID = "run-b", "run-b", "run-b"
	}), "model not in the pool's terms")
}

func TestDispatchCarriesThePlacementModel(t *testing.T) {
	terms := testTerms()
	terms.Agents[0].Models, terms.Agents[0].DefaultModel = []string{"haiku", "sonnet"}, "haiku"
	f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakeAdapter{"nomad": {stream: resultStream}})
	f.answer(t, credentialName)
	if status := f.execute(t, f.submit(t, f.host, func(task *wire.Submission) { task.Wherry.Placement.Model = "sonnet" }), 1); status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	if run := f.lastRun(); run.Model != "sonnet" {
		t.Fatalf("dispatched model %q, want the placement's", run.Model)
	}
}

func TestAdmitRejectsTurnPastRunLifetime(t *testing.T) {
	terms := testTerms()
	terms.RunLifetimeMS = 1
	f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakeAdapter{"nomad": {stream: resultStream}})
	first := f.turn(t, "run-expired", 1)
	f.publishStatus(t, first, testReceiver, f.receiver.signing, "interrupted", wire.TaskStateAuthRequired, nil)
	time.Sleep(10 * time.Millisecond)
	f.rejected(t, f.turn(t, "run-expired", 2), "run expired")
}

func TestAdmitRejectsQueueDeadlinePastRunLifetime(t *testing.T) {
	terms := testTerms()
	terms.RunLifetimeMS = time.Hour.Milliseconds()
	f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakeAdapter{"nomad": {stream: resultStream}})
	first := f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Run, task.Message.TaskID, task.Message.ContextID = "run-queued", "run-queued", "run-queued"
		task.Wherry.QueueDeadlineMS = time.Now().Add(2 * time.Hour).UnixMilli()
	})
	f.rejected(t, first, "queue deadline exceeds run lifetime")
	interrupted := f.turn(t, "run-continued", 1)
	f.publishStatus(t, interrupted, testReceiver, f.receiver.signing, "interrupted", wire.TaskStateAuthRequired, nil)
	next := f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Run, task.Wherry.Turn, task.Message.MessageID = "run-continued", 2, "msg-2"
		task.Message.TaskID, task.Message.ContextID = "run-continued", "run-continued"
		task.Wherry.QueueDeadlineMS = time.Now().Add(2 * time.Hour).UnixMilli()
	})
	f.rejected(t, next, "queue deadline exceeds run lifetime")
}

func TestRedeliveredClaimedTurnPastRunLifetimeIsAdopted(t *testing.T) {
	terms := testTerms()
	terms.RunLifetimeMS = 50
	f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakeAdapter{"nomad": {stream: resultStream, job: adapter.JobRunning}})
	record := startedTurn(t, f, "run-held", func(task *wire.Submission) {
		task.Wherry.QueueDeadlineMS = time.Now().Add(20 * time.Millisecond).UnixMilli()
	})
	time.Sleep(60 * time.Millisecond)
	if status := f.receiver.execute(f.ctx, f.service.Admin, record, f.admitted(t, record), 2); status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	if dispatches, lookups, follows, _ := f.adapter.counts(); dispatches != 0 || lookups != 1 || follows != 1 {
		t.Fatalf("dispatches=%d lookups=%d follows=%d, want the running job adopted", dispatches, lookups, follows)
	}
}

// startedTurn submits run's first turn with mutate applied, and its started status as a dead receiver leaves it.
func startedTurn(t *testing.T, f fixture, run string, mutate func(*wire.Submission)) *kgo.Record {
	t.Helper()
	record := f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Run, task.Message.TaskID, task.Message.ContextID = run, run, run
		mutate(task)
	})
	f.publishStatus(t, record, testReceiver, f.receiver.signing, "started", wire.TaskStateWorking, nil)
	return record
}

func noRejection(t *testing.T, f fixture) {
	t.Helper()
	if slices.ContainsFunc(f.eventStatuses(t), func(status wire.RunStatus) bool { return status.Wherry.Kind == "rejected" }) {
		t.Fatalf("a claimed turn was rejected; logs:\n%s", f.logs.String())
	}
}

func TestRedeliveredClaimedTurnPastQueueDeadlineIsAdopted(t *testing.T) {
	f := newFixtureWith(t, resultStream, testTerms(), testConfig(testReceiver), map[string]*fakeAdapter{"nomad": {stream: resultStream, job: adapter.JobRunning}})
	record := startedTurn(t, f, "run-queued-held", func(task *wire.Submission) {
		task.Wherry.QueueDeadlineMS = time.Now().Add(-time.Second).UnixMilli()
	})
	// The adopter publishes nothing until its own seal arrives.
	f.answer(t, credentialName)
	if status := f.execute(t, record, 2); status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	if dispatches, lookups, follows, _ := f.adapter.counts(); dispatches != 0 || lookups != 1 || follows != 1 {
		t.Fatalf("dispatches=%d lookups=%d follows=%d, want the running job adopted", dispatches, lookups, follows)
	}
	noRejection(t, f)
	if !slices.ContainsFunc(f.feedStatuses(t), func(status wire.RunStatus) bool {
		return status.Wherry.Kind == "terminal" && status.StatusUpdate.Status.State == wire.TaskStateCompleted
	}) {
		t.Fatal("no COMPLETED terminal on the feed")
	}
}

func TestRedeliveredClaimedTurnPastRunDeadlineStopsItsJob(t *testing.T) {
	f := newFixtureWith(t, resultStream, testTerms(), testConfig(testReceiver), map[string]*fakeAdapter{"nomad": {stream: resultStream, job: adapter.JobRunning}})
	record := startedTurn(t, f, "run-deadline-held", func(task *wire.Submission) {
		task.Wherry.QueueDeadlineMS = time.Now().Add(-2 * time.Second).UnixMilli()
		task.Wherry.DeadlineMS = time.Now().Add(-time.Second).UnixMilli()
	})
	if status := f.execute(t, record, 2); status != kgo.AckReject {
		t.Fatalf("status %v, want reject; logs:\n%s", status, f.logs.String())
	}
	f.adapter.mu.Lock()
	dispatches, lookups, stops := f.adapter.dispatches, f.adapter.lookups, f.adapter.stops
	f.adapter.mu.Unlock()
	if dispatches != 0 || lookups != 1 || stops != 1 {
		t.Fatalf("dispatches=%d lookups=%d stops=%d, want the found job stopped", dispatches, lookups, stops)
	}
	noRejection(t, f)
	if state, reason := terminalReason(t, f, "run-deadline-held"); state != wire.TaskStateFailed || reason != "timeout" {
		t.Fatalf("terminal %s %q, want FAILED timeout", state, reason)
	}
}

func TestUnclaimedContinuationPastQueueDeadlineIsRejected(t *testing.T) {
	f := newFixture(t, resultStream)
	first := f.turn(t, "run-continued", 1)
	f.publishStatus(t, first, testReceiver, f.receiver.signing, "interrupted", wire.TaskStateAuthRequired, nil)
	next := f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Run, task.Wherry.Turn, task.Message.MessageID = "run-continued", 2, "msg-2"
		task.Message.TaskID, task.Message.ContextID = "run-continued", "run-continued"
		task.Wherry.QueueDeadlineMS = time.Now().Add(-time.Second).UnixMilli()
	})
	f.rejected(t, next, "queue deadline passed")
}

func TestAdmitAppliesThePoolInlineLimit(t *testing.T) {
	terms := testTerms()
	terms.InlineLimit = 100
	f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakeAdapter{"nomad": {stream: resultStream}})
	withInput := func(size int) func(*wire.Submission) {
		return func(task *wire.Submission) {
			task.Message.Parts = append(task.Message.Parts, wire.Part{Raw: make(wire.StdBytes, size), Filename: "data.bin"})
		}
	}
	f.rejected(t, f.submit(t, f.host, withInput(101)), "inline inputs are 101 bytes, over the 100-byte limit")
	if admitted, ack := f.receiver.admit(f.ctx, f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Run, task.Message.TaskID, task.Message.ContextID = "run-b", "run-b", "run-b"
		withInput(100)(task)
	})); admitted == nil {
		t.Fatalf("inputs at the pool's limit: ack %v", ack)
	}
}

func TestPlacementOutsideOffersIsStartFailure(t *testing.T) {
	f := newFixture(t, resultStream)
	for name, mutate := range map[string]func(*wire.Submission){
		// codex is inside the terms, but no adapter here launches it.
		"agent":    func(task *wire.Submission) { task.Wherry.Placement.Agent = "codex" },
		"selector": func(task *wire.Submission) { task.Wherry.Placement.Selector = []wire.Requirement{in("zone", "dc9")} },
	} {
		t.Run(name, func(t *testing.T) {
			before := len(f.records(t, admin.PoolEventsTopic(testPool)))
			record := f.submit(t, f.host, mutate)
			if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckRelease {
				t.Fatalf("admitted=%v ack=%v, want release", admitted != nil, ack)
			}
			records := f.records(t, admin.PoolEventsTopic(testPool))
			if len(records) != before+1 {
				t.Fatalf("pool events %d -> %d, want one start failure", before, len(records))
			}
			var failure wire.StartFailure
			if err := wire.ParseRecord(records[len(records)-1].Value, f.keys(), &failure); err != nil {
				t.Fatal(err)
			}
			placement := f.parse(t, record).Wherry.Placement
			if failure.Class != "offer-unavailable" || failure.Adapter != "none" || failure.Agent != placement.Agent || failure.Wherry.Run != "run-a" {
				t.Fatalf("start failure %#v", failure)
			}
		})
	}
	if len(f.feedStatuses(t)) != 0 {
		t.Fatal("a start failure wrote a status to the feed")
	}
}

func TestPlacementSelectorSelectsAnOffer(t *testing.T) {
	f := newFixture(t, resultStream)
	admitted := f.admitted(t, f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Placement.Selector = []wire.Requirement{in("zone", "dc1"), exists("example.com/gpu")}
	}))
	if admitted.adapter.Name != "nomad" || admitted.adapter.Labels["example.com/gpu"] != "none" {
		t.Fatalf("adapter %#v", admitted.adapter)
	}
}

func TestContinuationRejectedWhenNotInterrupted(t *testing.T) {
	for name, state := range map[string]wire.TaskState{"running": wire.TaskStateWorking, "completed": wire.TaskStateCompleted} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			first := f.turn(t, "run-not-interrupted", 1)
			kind := "claimed"
			if wire.Terminal(state) {
				kind = "terminal"
			}
			f.publishStatus(t, first, testReceiver, f.receiver.signing, kind, state, nil)
			f.rejected(t, f.turn(t, "run-not-interrupted", 2), "task is not awaiting continuation")
		})
	}
}

func TestStaleSubmissionRejected(t *testing.T) {
	t.Run("earlier turn after later turn started", func(t *testing.T) {
		f := newFixture(t, resultStream)
		first := f.turn(t, "run-earlier-turn", 1)
		f.publishStatus(t, first, testReceiver, f.receiver.signing, "interrupted", wire.TaskStateAuthRequired, nil)
		second := f.turn(t, "run-earlier-turn", 2)
		f.publishStatus(t, second, testReceiver, f.receiver.signing, "started", wire.TaskStateWorking, nil)
		f.rejected(t, first, "stale submission")
	})
	t.Run("turn after its interruption", func(t *testing.T) {
		f := newFixture(t, resultStream)
		first := f.turn(t, "run-interrupted-replay", 1)
		f.publishStatus(t, first, testReceiver, f.receiver.signing, "interrupted", wire.TaskStateAuthRequired, nil)
		f.rejected(t, first, "stale submission")
	})
	t.Run("active turn redelivery adopts", func(t *testing.T) {
		f := newFixture(t, resultStream)
		first := f.turn(t, "run-active-redelivery", 1)
		f.publishStatus(t, first, testReceiver, f.receiver.signing, "heartbeat", wire.TaskStateWorking, nil)
		f.admitted(t, first)
	})
}

func TestDuplicateContinuationRejected(t *testing.T) {
	f := newFixture(t, resultStream)
	first := f.turn(t, "run-duplicate", 1)
	f.publishStatus(t, first, testReceiver, f.receiver.signing, "interrupted", wire.TaskStateAuthRequired, nil)
	accepted := f.turn(t, "run-duplicate", 2)
	duplicate := f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Run, task.Wherry.Turn = "run-duplicate", 2
		task.Message.MessageID, task.Message.TaskID, task.Message.ContextID = "msg-duplicate", "run-duplicate", "run-duplicate"
	})
	f.admitted(t, accepted)
	f.rejected(t, duplicate, "duplicate continuation")
}

func TestDuplicateSubmissionRejected(t *testing.T) {
	f := newFixture(t, resultStream)
	accepted := f.turn(t, "run-duplicate-root", 1)
	duplicate := f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Run = "run-duplicate-root"
		task.Message.MessageID, task.Message.TaskID, task.Message.ContextID = "msg-duplicate-root", "run-duplicate-root", "run-duplicate-root"
	})
	f.admitted(t, accepted)
	f.rejected(t, duplicate, "duplicate submission")
}

func TestRejectedContinuationKeepsTaskResumable(t *testing.T) {
	f := newFixture(t, resultStream)
	first := f.turn(t, "run-rejected-continuation", 1)
	f.publishStatus(t, first, testReceiver, f.receiver.signing, "interrupted", wire.TaskStateAuthRequired, nil)
	f.rejected(t, f.turn(t, "run-rejected-continuation", 3), "continuation turn out of order")
	f.admitted(t, f.turn(t, "run-rejected-continuation", 2))
	statuses := f.feedStatuses(t)
	if got := statuses[len(statuses)-1]; got.Wherry.Kind != "rejected" || got.StatusUpdate.Status.State != wire.TaskStateRejected {
		t.Fatalf("rejection status %#v", got)
	}
}

func TestContinuationRejectedFromOtherPoolStatus(t *testing.T) {
	f := newFixture(t, resultStream)
	if err := f.service.PoolCreate(f.ctx, "pool-b", testTerms()); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantAdd(f.ctx, testTenant, "pool-b"); err != nil {
		t.Fatal(err)
	}
	otherKey := f.addPeer(t, "receiver-other-pool", "pool-b")
	first := f.turn(t, "run-other-pool-status", 1)
	f.publishStatus(t, first, "receiver-other-pool", otherKey, "interrupted", wire.TaskStateAuthRequired, nil)
	f.rejected(t, f.turn(t, "run-other-pool-status", 2), "task is not awaiting continuation")
}

func TestContinuationAcceptsPeerInterruption(t *testing.T) {
	f := newFixture(t, resultStream)
	peerKey := f.addPeer(t, "receiver-b", testPool)
	first := f.turn(t, "run-peer-interrupted", 1)
	f.publishStatus(t, first, "receiver-b", peerKey, "interrupted", wire.TaskStateAuthRequired, nil)
	f.admitted(t, f.turn(t, "run-peer-interrupted", 2))
}

func TestContinuationStatusBindings(t *testing.T) {
	for name, mutate := range map[string]func(*wire.RunStatus){
		"task hash": func(status *wire.RunStatus) { status.Wherry.TaskHash = strings.Repeat("d", 64) },
		"kind":      func(status *wire.RunStatus) { status.Wherry.Kind = "claimed" },
		"tenant":    func(status *wire.RunStatus) { status.Wherry.Tenant = "tenant-b" },
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			if err := f.service.TenantAdd(f.ctx, "tenant-b", testPool); err != nil {
				t.Fatal(err)
			}
			if err := f.receiver.refresh(f.ctx); err != nil {
				t.Fatal(err)
			}
			first := f.turn(t, "run-status-binding", 1)
			f.publishStatus(t, first, testReceiver, f.receiver.signing, "interrupted", wire.TaskStateAuthRequired, mutate)
			if admitted, ack := f.receiver.admit(f.ctx, f.turn(t, "run-status-binding", 2)); admitted != nil || ack != kgo.AckReject {
				t.Fatalf("admitted=%v ack=%v, want rejection", admitted != nil, ack)
			}
		})
	}
}

func TestContinuationStatusOrdering(t *testing.T) {
	f := newFixture(t, resultStream)
	first := f.turn(t, "run-status-order", 1)
	f.publishStatus(t, first, testReceiver, f.receiver.signing, "interrupted", wire.TaskStateAuthRequired, func(status *wire.RunStatus) { status.Wherry.Attempt = 2 })
	f.publishStatus(t, first, testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
	f.admitted(t, f.turn(t, "run-status-order", 2))
}

func TestBindTurnsRequireSameBindings(t *testing.T) {
	for name, mutate := range map[string]func(*wire.Submission){
		"tenant":   func(task *wire.Submission) { task.Wherry.Tenant = "tenant-b" },
		"agent":    func(task *wire.Submission) { task.Wherry.Placement.Agent = "codex" },
		"selector": func(task *wire.Submission) { task.Wherry.Placement.Selector = []wire.Requirement{in("zone", "dc1")} },
		"model":    func(task *wire.Submission) { task.Wherry.Placement.Model = "sonnet" },
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.turn(t, "run-submission-binding", 1)
			all, err := f.receiver.readTurns(f.ctx, "run-submission-binding")
			if err != nil || len(all) != 1 {
				t.Fatalf("turns %d err %v", len(all), err)
			}
			expected := testSubmission("run-submission-binding")
			mutate(expected)
			if bound := bindTurns(all, expected, poolEvents{}); len(bound) != 0 {
				t.Fatalf("mismatched %s was accepted", name)
			}
		})
	}
}

func TestReceiverRejectsURLParts(t *testing.T) {
	for name, part := range map[string]wire.Part{
		"url": {URL: "https://example.test/a"},
		// The git context part that the git input replaced, as wherry submit built it.
		"git context": {URL: "https://git.example.test/org/repo.git", MediaType: "application/vnd.wherry.git", Metadata: jsontext.Value(`{"ref":"main"}`)},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.answer(t, credentialName)
			f.submit(t, f.host, func(task *wire.Submission) { task.Message.Parts = append(task.Message.Parts, part) })
			if err := f.receiver.claimOne(f.ctx); err != nil {
				t.Fatal(err)
			}
			if dispatches, _, _, _ := f.adapter.counts(); dispatches != 0 {
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
		task.Wherry.QueueDeadlineMS = time.Now().Add(-time.Second).UnixMilli()
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
		job     adapter.JobState
		stops   int
	}{
		{"queued", 1, adapter.JobAbsent, 0},
		{"redelivered with its job running", 2, adapter.JobRunning, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.answer(t, credentialName)
			f.adapter.job = test.job
			record := f.submit(t, f.host, nil)
			f.publishCancel(t, "run-a", 1, f.host, nil)
			if status := f.execute(t, record, test.attempt); status != kgo.AckAccept {
				t.Fatalf("status %v, want accept", status)
			}
			f.adapter.mu.Lock()
			dispatches, stops, releaseIDs := f.adapter.dispatches, f.adapter.stops, f.adapter.releaseIDs
			f.adapter.mu.Unlock()
			if dispatches != 0 || stops != test.stops || !slices.Equal(releaseIDs, []string{"run-a"}) {
				t.Fatalf("dispatches=%d stops=%d releases=%v, want 0, %d and run-a", dispatches, stops, releaseIDs, test.stops)
			}
			if task := f.lastTask(t); task.ID != "run-a" || task.Status.State != wire.TaskStateCanceled {
				t.Fatalf("task %#v, want CANCELED", task)
			}
			if state, reason := terminalReason(t, f, "run-a"); state != wire.TaskStateCanceled || reason != "canceled by user" {
				t.Fatalf("terminal %s %q", state, reason)
			}
		})
	}
}

func TestCancelSinceLastReadStopsDispatch(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.submit(t, f.host, nil)
	f.produce(t, admin.PoolEventsTopic(testPool), "run-a", must(t)(wire.SignRecord(&wire.Cancel{Tenant: testTenant, Host: testHost, Pool: testPool, Run: "run-a", Turn: 1}, f.host)))
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
		f.publishCancel(t, "run-a", 1, test.key, test.mutate)
		if _, cancelled := f.receiver.cancelled[runKey{testTenant, "run-a"}]; cancelled {
			t.Fatalf("receiver honoured a cancel signed by the %s", name)
		}
	}
	f.publishCancel(t, "run-a", 1, f.host, nil)
	if _, cancelled := f.receiver.cancelled[runKey{testTenant, "run-a"}]; !cancelled {
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
func startInFlight(t *testing.T, f fixture, ctx context.Context, run string) <-chan error {
	t.Helper()
	f.answer(t, credentialName)
	f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Run, task.Message.TaskID, task.Message.ContextID = run, run, run
	})
	done := make(chan error, 1)
	go func() { done <- f.receiver.claimOne(ctx) }()
	for {
		if _, _, follows, _ := f.adapter.counts(); follows == 1 {
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
	f.adapter.endDelay = 30 * time.Second
	done := startInFlight(t, f, f.ctx, "run-cancelled")
	f.publishCancel(t, "run-cancelled", 1, f.host, nil)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("in-flight run kept going after its cancel")
	}
	f.adapter.mu.Lock()
	stops, releaseIDs := f.adapter.stops, f.adapter.releaseIDs
	f.adapter.mu.Unlock()
	if stops != 1 || !slices.Equal(releaseIDs, []string{"run-cancelled"}) {
		t.Fatalf("stops=%d releases=%v", stops, releaseIDs)
	}
	if task := f.lastTask(t); task.ID != "run-cancelled" || task.Status.State != wire.TaskStateCanceled {
		t.Fatalf("task %#v, want CANCELED", task)
	}
	if state, reason := terminalReason(t, f, "run-cancelled"); state != wire.TaskStateCanceled || reason != "canceled by user" {
		t.Fatalf("terminal %s %q", state, reason)
	}
	// Accepted, not released: the pool never sees the submission again.
	if record, delivered := f.pollPool(t, "run-cancelled", 2*time.Second); delivered {
		t.Fatalf("cancelled run's submission was redelivered (delivery %d)", record.DeliveryCount())
	}
}

func TestRunningTurnSeesCancelBetweenHeartbeats(t *testing.T) {
	f := newFixture(t, noResultStream)
	f.adapter.endDelay = 30 * time.Second
	renewInterval = time.Minute // only the state check, not a heartbeat, can stop the run in time
	done := startInFlight(t, f, f.ctx, "run-cancelled")
	f.publishCancel(t, "run-cancelled", 1, f.host, nil)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * stateCheckInterval):
		t.Fatal("running turn missed a cancel the receiver held")
	}
	if state, reason := terminalReason(t, f, "run-cancelled"); state != wire.TaskStateCanceled || reason != "canceled by user" {
		t.Fatalf("terminal %s %q", state, reason)
	}
}

func TestHeartbeatHonoursHeldCancel(t *testing.T) {
	f := newFixture(t, noResultStream)
	f.adapter.endDelay = 30 * time.Second
	stateCheckInterval = time.Minute // only a heartbeat can stop the run in time
	t.Cleanup(func() { stateCheckInterval = time.Second })
	done := startInFlight(t, f, f.ctx, "run-cancelled")
	f.publishCancel(t, "run-cancelled", 1, f.host, nil)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * renewInterval):
		t.Fatal("heartbeat committed past a cancel the receiver held")
	}
	if state, _ := terminalReason(t, f, "run-cancelled"); state != wire.TaskStateCanceled {
		t.Fatalf("terminal %s, want CANCELED", state)
	}
}

// interrupt runs the first turn of run without a seal, so it waits AUTH_REQUIRED.
func (f fixture) interrupt(t *testing.T, run, text string) *kgo.Record {
	t.Helper()
	record := f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Run, task.Message.TaskID, task.Message.ContextID = run, run, run
		task.Message.Parts = []wire.Part{{Text: text}}
	})
	if status := f.execute(t, record, 1); status != kgo.AckAccept {
		t.Fatalf("status %v, want the interrupted task acknowledged", status)
	}
	if task := f.lastTask(t); task.Status.State != wire.TaskStateAuthRequired {
		t.Fatalf("task %#v, want AUTH_REQUIRED", task)
	}
	return record
}

func TestCancelInterruptedTaskEndsIt(t *testing.T) {
	f := newFixture(t, resultStream)
	peer := f.startPeer(t, "receiver-b")
	f.interrupt(t, "run-a", "first")
	// The peer, which never held the run, reads the cancel first and ends the interrupted turn.
	f.produce(t, admin.PoolEventsTopic(testPool), "run-a", must(t)(wire.SignRecord(&wire.Cancel{Tenant: testTenant, Host: testHost, Pool: testPool, Run: "run-a", Turn: 1}, f.host)))
	if err := peer.readCancels(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.readCancels(f.ctx); err != nil {
		t.Fatal(err)
	}
	keys := signed.Keys{"receiver-b": {Current: peer.signing.Public().(ed25519.PublicKey)}, testReceiver: f.keys()[testReceiver]}
	var canceled []wire.RunStatus
	var result *wire.Result
	for _, record := range f.records(t, admin.TenantTopic(testTenant)) {
		var status wire.RunStatus
		if wire.ParseRecord(record.Value, keys, &status) == nil && status.Wherry.Kind == "terminal" {
			canceled = append(canceled, status)
		}
		var candidate wire.Result
		if wire.ParseRecord(record.Value, keys, &candidate) == nil {
			result = &candidate
		}
	}
	if len(canceled) != 1 || canceled[0].Wherry.EventID != "run-a/1/canceled" || canceled[0].Wherry.Receiver != "receiver-b" || canceled[0].StatusUpdate.Status.State != wire.TaskStateCanceled {
		t.Fatalf("CANCELED terminals on the feed %#v, want the peer's alone, with the turn's event ID", canceled)
	}
	if result == nil || result.Task.Status.State != wire.TaskStateCanceled || len(result.Task.History) != 1 || result.Task.History[0].Parts[0].Text != "first" {
		t.Fatalf("result %#v, want CANCELED with the first turn", result)
	}

	// A continuation published without the host's own check is cancelled too, and never dispatched.
	f.answer(t, credentialName)
	if status := f.execute(t, f.turn(t, "run-a", 2), 1); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	if task := f.lastTask(t); task.Status.State != wire.TaskStateCanceled || len(task.History) != 1 || task.History[0].MessageID != "msg-2" {
		t.Fatalf("task %#v, want CANCELED with the second turn alone", task)
	}
	if dispatches, _, _, _ := f.adapter.counts(); dispatches != 0 {
		t.Fatalf("continuation of a cancelled task dispatched %d jobs", dispatches)
	}
}

func TestExpiredInterruptedTaskEndsCanceled(t *testing.T) {
	f := newFixture(t, resultStream)
	f.interrupt(t, "run-expired", "first")
	terms := testTerms()
	terms.RunLifetimeMS = 1
	if err := f.service.PoolCreate(f.ctx, testPool, terms); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if err := f.receiver.readCancels(f.ctx); err != nil {
		t.Fatal(err)
	}
	if state, reason := terminalReason(t, f, "run-expired"); state != wire.TaskStateCanceled || reason != "run expired" {
		t.Fatalf("terminal %s %q, want CANCELED run expired", state, reason)
	}
}

func TestExpiredInterruptedTaskEndsWithoutItsFirstTurn(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.turn(t, "run-expired", 2)
	f.publishStatus(t, record, testReceiver, f.receiver.signing, "interrupted", wire.TaskStateAuthRequired, nil)
	if err := f.receiver.readCancels(f.ctx); err != nil {
		t.Fatal(err)
	}
	if state, reason := terminalReason(t, f, "run-expired"); state != wire.TaskStateCanceled || reason != "run expired" {
		t.Fatalf("terminal %s %q, want CANCELED run expired; logs:\n%s", state, reason, f.logs.String())
	}
}

func TestInterruptedTurnOfAnEndedHostIsReadOnce(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.turn(t, "run-ended-host", 1)
	f.publishStatus(t, record, testReceiver, f.receiver.signing, "interrupted", wire.TaskStateAuthRequired, nil)
	denied := func(context.Context, string, string) error { return admin.ErrAuthorizationDenied }
	if result := f.service.RevokeHost(f.ctx, testTenant, testHost, time.Millisecond, denied); !result.Complete {
		t.Fatalf("revoke host: %#v", result.Steps)
	}
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := f.receiver.readCancels(f.ctx); err != nil {
			t.Fatal(err)
		}
	}
	if n := strings.Count(f.logs.String(), "interrupted turn not found"); n != 1 {
		t.Fatalf("interrupted turn looked up %d times, want once; logs:\n%s", n, f.logs.String())
	}
	// A miss can come from a host key not yet refreshed, so it is looked up again once unfoundRetry has passed.
	f.receiver.mu.Lock()
	for id := range f.receiver.unfound {
		f.receiver.unfound[id] = time.Now().Add(-unfoundRetry)
	}
	f.receiver.mu.Unlock()
	if err := f.receiver.readCancels(f.ctx); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(f.logs.String(), "interrupted turn not found"); n != 2 {
		t.Fatalf("interrupted turn looked up %d times after its retry interval, want twice", n)
	}
}

// TestRunsUnderRetention backdates records against the derived retention, since its turn deadline and margin keep it
// above a day.
func TestRunsUnderRetention(t *testing.T) {
	terms := testTerms()
	terms.RunLifetimeMS = time.Hour.Milliseconds()
	f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakeAdapter{"nomad": {stream: resultStream, job: adapter.JobRunning}})
	aged := func(topic, key string, value []byte, age time.Duration) *kgo.Record {
		t.Helper()
		record := &kgo.Record{Topic: topic, Key: []byte(key), Value: value, Timestamp: time.Now().Add(-age)}
		if err := f.service.Admin.ProduceSync(f.ctx, record).FirstErr(); err != nil {
			t.Fatal(err)
		}
		return record
	}
	turn := func(run string, age time.Duration, kind string, state wire.TaskState) (*kgo.Record, *wire.Submission) {
		t.Helper()
		task := testSubmission(run)
		record := aged(admin.PoolTasksTopic(testPool), run, must(t)(wire.SignRecord(task, f.host)), age)
		status := f.receiver.status(task, wire.TaskHash(record.Value), 1, kind, taskStatus(state, ""), nil)
		aged(admin.PoolEventsTopic(testPool), run, must(t)(wire.SignRecord(status, f.receiver.signing)), age)
		return record, task
	}
	continuation := func(run string) *kgo.Record {
		t.Helper()
		return f.submit(t, f.host, func(task *wire.Submission) {
			task.Wherry.Run, task.Wherry.Turn, task.Message.MessageID = run, 2, "msg-2"
			task.Message.TaskID, task.Message.ContextID = run, run
		})
	}
	turn("run-ancient", 27*time.Hour, "interrupted", wire.TaskStateAuthRequired)
	completed, _ := turn("run-completed", 14*time.Hour, "terminal", wire.TaskStateCompleted)
	turn("run-continued", 50*time.Minute, "interrupted", wire.TaskStateAuthRequired)
	adopted, adoptedTask := turn("run-adopted", 50*time.Minute, "started", wire.TaskStateWorking)
	aged(admin.PoolEventsTopic(testPool), "run-adopted", f.heartbeat(t, adoptedTask, wire.TaskHash(adopted.Value), testReceiver, f.receiver.signing, 41, nil), 50*time.Minute)
	turn("run-stale", 2*time.Hour, "interrupted", wire.TaskStateAuthRequired)
	f.cluster.ApplyRetention()

	for _, topic := range []string{admin.PoolTasksTopic(testPool), admin.PoolEventsTopic(testPool)} {
		if slices.ContainsFunc(f.records(t, topic), func(record *kgo.Record) bool { return string(record.Key) == "run-ancient" }) {
			t.Fatalf("%s kept a record past its retention", topic)
		}
	}
	before := len(f.records(t, admin.PoolEventsTopic(testPool)))
	if admitted, ack := f.receiver.admit(f.ctx, completed); admitted != nil || ack != kgo.AckAccept {
		t.Fatalf("redelivered completed turn admitted=%v ack=%v, want accept; logs:\n%s", admitted != nil, ack, f.logs.String())
	}
	if after := len(f.records(t, admin.PoolEventsTopic(testPool))); after != before {
		t.Fatalf("a redelivered completed turn wrote events %d -> %d", before, after)
	}
	if got := prompt(f.admitted(t, continuation("run-continued")).history); got != resumedHeader+"\n\nhi\n\nhi" {
		t.Fatalf("continued prompt %q", got)
	}
	if offset, err := f.receiver.lastOffset(f.ctx, adoptedTask, wire.TaskHash(adopted.Value)); err != nil || offset != 41 {
		t.Fatalf("adoption offset %d, %v; want 41", offset, err)
	}
	if status := f.receiver.execute(f.ctx, f.service.Admin, adopted, f.admitted(t, adopted), 2); status != kgo.AckAccept {
		t.Fatalf("adopted status %v; logs:\n%s", status, f.logs.String())
	}
	if dispatches, _, follows, _ := f.adapter.counts(); dispatches != 0 || follows != 1 {
		t.Fatalf("dispatches=%d follows=%d, want the running job adopted", dispatches, follows)
	}
	f.rejected(t, continuation("run-stale"), "run expired")
	if err := f.receiver.readCancels(f.ctx); err != nil {
		t.Fatal(err)
	}
	if state, reason := terminalReason(t, f, "run-stale"); state != wire.TaskStateCanceled || reason != "run expired" {
		t.Fatalf("stale run terminal %s %q, want CANCELED run expired", state, reason)
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
	record := f.submit(t, f.host, func(task *wire.Submission) { task.Wherry.QueueDeadlineMS = time.Now().Add(-time.Second).UnixMilli() })
	f.rejected(t, record, "queue deadline passed")
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
	if echo.Pool != testPool || echo.Run != "run-a" || echo.TaskHash != wire.TaskHash(record.Value) || rejected.Wherry.Kind != "rejected" {
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
	record := f.submit(t, f.host, func(task *wire.Submission) { task.Wherry.QueueDeadlineMS = time.Now().Add(-time.Second).UnixMilli() })
	if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckRelease {
		t.Fatalf("admitted=%v ack=%v, want release", admitted != nil, ack)
	}
	if n := len(f.eventStatuses(t)); n != 0 {
		t.Fatalf("pool events hold %d statuses of a rejection the feed never got", n)
	}
	if !f.receiver.denied() {
		t.Fatal("an authorisation error writing the rejection did not enter denial")
	}
}

func TestStartFailureWriteDenialEntersDenial(t *testing.T) {
	f := newFixture(t, resultStream)
	f.deleteReceiverACLs(t, kadm.NewACLs().Topics(admin.PoolEventsTopic(testPool)).Operations(kadm.OpWrite))
	record := f.submit(t, f.host, func(task *wire.Submission) { task.Wherry.Placement.Agent = "codex" })
	if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckRelease {
		t.Fatalf("admitted=%v ack=%v, want release", admitted != nil, ack)
	}
	if !f.receiver.holds(capWrite) || !strings.Contains(f.logs.String(), "publish start failure for run-a") {
		t.Fatalf("a start failure that could not be written was not denied and logged; logs:\n%s", f.logs.String())
	}
}

func TestConcurrentInterruptedCancelIsOneFact(t *testing.T) {
	f := newFixture(t, resultStream)
	peer := f.startPeer(t, "receiver-b")
	f.interrupt(t, "run-a", "first")
	f.produce(t, admin.PoolEventsTopic(testPool), "run-a", must(t)(wire.SignRecord(&wire.Cancel{Tenant: testTenant, Host: testHost, Pool: testPool, Run: "run-a", Turn: 1}, f.host)))
	// The first commit waits until the second member's commit arrives, so both members write.
	second := make(chan struct{})
	var commits atomic.Int32
	f.cluster.ControlKey(kmsg.EndTxn.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		if !strings.Contains(request.(*kmsg.EndTxnRequest).TransactionalID, ".admission.") {
			return nil, nil, false
		}
		switch commits.Add(1) {
		case 1:
			f.cluster.SleepControl(func() {
				select {
				case <-second:
				case <-time.After(10 * time.Second):
				}
			})
		case 2:
			close(second)
			f.cluster.DropControl()
		}
		return nil, nil, false
	})
	var wg sync.WaitGroup
	for _, member := range []*Receiver{f.receiver, peer} {
		wg.Go(func() {
			if err := member.readCancels(f.ctx); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if commits.Load() != 2 {
		t.Fatalf("admission commits %d, want both members to write", commits.Load())
	}
	keys := signed.Keys{"receiver-b": {Current: peer.signing.Public().(ed25519.PublicKey)}, testReceiver: f.keys()[testReceiver]}
	feedIDs := map[string]bool{}
	for _, record := range f.records(t, admin.TenantTopic(testTenant)) {
		var status wire.RunStatus
		if wire.ParseRecord(record.Value, keys, &status) == nil && status.Wherry.Kind == "terminal" {
			feedIDs[status.Wherry.EventID] = true
		}
	}
	if len(feedIDs) != 1 || !feedIDs["run-a/1/canceled"] {
		t.Fatalf("CANCELED event IDs on the feed %v, want run-a/1/canceled alone", feedIDs)
	}
	events, err := f.receiver.readEvents(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	terminals := 0
	for _, status := range events.statuses["run-a"] {
		if status.Wherry.Kind == "terminal" {
			terminals++
		}
	}
	if terminals != 1 {
		t.Fatalf("a pool events reader sees %d CANCELED terminals, want one", terminals)
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
			f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakeAdapter{"nomad": {stream: resultStream}})
			record := f.submit(t, f.host, test.mutate)
			test.fault(t, f)
			if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckReject {
				t.Fatalf("admitted=%v ack=%v, want reject; logs:\n%s", admitted != nil, ack, f.logs.String())
			}
			if state, reason := terminalReason(t, f, "run-a"); state != wire.TaskStateFailed || reason != "delivery budget exhausted" {
				t.Fatalf("terminal %s %q", state, reason)
			}
			if task := f.lastTask(t); task.Status.State != wire.TaskStateFailed {
				t.Fatalf("result %#v, want FAILED", task.Status)
			}
			var echo wire.Echo
			if records := f.records(t, admin.TenantTopic(testTenant)); len(records) == 0 || wire.ParseRecord(records[0].Value, f.keys(), &echo) != nil || echo.Run != "run-a" || echo.Pool != testPool {
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
	if err := f.service.HostAdd(f.ctx, "tenant-b", "host-b", t.TempDir()+"/host-b.yaml"); err != nil {
		t.Fatal(err)
	}
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	f.produce(t, admin.HostTopic("tenant-b", "host-b"), "", must(t)(signed.RegisterKey(signed.HostOwner("tenant-b", "host-b"), key)))
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	return key
}

func asTenantB(task *wire.Submission) {
	task.Wherry.Tenant, task.Wherry.Host, task.Message.MessageID = "tenant-b", "host-b", "msg-b"
}

func TestRunIDOfAnotherTenantIsRejected(t *testing.T) {
	f := newFixture(t, resultStream)
	keyB := f.tenantB(t)
	holder := testActiveRun(t, f, "run-a", 1)
	if err := holder.commit(f.ctx, holder.statusRecords("claimed", taskStatus(wire.TaskStateWorking, ""), nil, nil)...); err != nil {
		t.Fatal(err)
	}
	before := len(f.records(t, admin.TenantTopic(testTenant)))
	f.rejected(t, f.submit(t, keyB, asTenantB), "run ID in use")
	if dispatches, _, _, _ := f.adapter.counts(); dispatches != 0 {
		t.Fatalf("dispatches %d, want none", dispatches)
	}
	if err := holder.commit(f.ctx, holder.events(holder.status("heartbeat", taskStatus(wire.TaskStateWorking, ""), nil, nil))); err != nil {
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
	if !slices.Equal(kinds, []string{"echo", "rejected run ID in use"}) {
		t.Fatalf("tenant-b's feed %v, want only its echo and rejection", kinds)
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
	if !slices.Equal(reasons, []string{"receiver-b run ID in use"}) {
		t.Fatalf("tenant-b statuses %v, want the second member's run ID rejection", reasons)
	}
}

// failEventsReadOnce fails the next ListOffsets that names the pool's events, the first read of an admission.
func (f fixture) failEventsReadOnce(t *testing.T) {
	t.Helper()
	failed := false
	f.cluster.ControlKey(kmsg.ListOffsets.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		req := request.(*kmsg.ListOffsetsRequest)
		if failed || !slices.ContainsFunc(req.Topics, func(topic kmsg.ListOffsetsRequestTopic) bool { return topic.Topic == admin.PoolEventsTopic(testPool) }) {
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
	f := newFixtureWith(t, resultStream, budgetTerms(), testConfig(testReceiver), map[string]*fakeAdapter{"nomad": {stream: resultStream}})
	f.answer(t, credentialName)
	record := f.submit(t, f.host, nil)
	if status := f.execute(t, record, 1); status != kgo.AckAccept {
		t.Fatalf("first delivery status %v, want accept", status)
	}
	f.failEventsReadOnce(t)
	if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckRelease {
		t.Fatalf("redelivery admitted=%v ack=%v, want release", admitted != nil, ack)
	}
	if terminals := f.terminals(t, admin.TenantTopic(testTenant)); terminals != 1 {
		t.Fatalf("terminals %d, want only the run's own", terminals)
	}
}

func TestUnreadableTurnsReleaseAtTheBudget(t *testing.T) {
	f := newFixtureWith(t, resultStream, budgetTerms(), testConfig(testReceiver), map[string]*fakeAdapter{"nomad": {stream: resultStream}})
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
	f := newFixtureWith(t, resultStream, budgetTerms(), testConfig(testReceiver), map[string]*fakeAdapter{"nomad": {stream: resultStream}})
	keyB := f.tenantB(t)
	f.submit(t, f.host, nil)
	// No adapter here launches codex, and attempt 1 is at the budget.
	f.rejected(t, f.submit(t, keyB, func(task *wire.Submission) {
		asTenantB(task)
		task.Wherry.Placement.Agent = "codex"
	}), "run ID in use")
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
	if result := f.service.RevokeHost(f.ctx, testTenant, testHost, time.Millisecond, denied); !result.Complete {
		t.Fatalf("revoke host: %#v", result.Steps)
	}
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.rejected(t, f.submit(t, keyB, asTenantB), "run ID in use")
}

func TestCancelOfAnotherTenantDoesNotStopTheRun(t *testing.T) {
	f := newFixture(t, noResultStream)
	keyB := f.tenantB(t)
	f.answer(t, credentialName)
	f.adapter.endDelay = time.Minute
	record := f.submit(t, f.host, nil)
	// tenant-b's rejection is the first status under run-a.
	f.rejected(t, f.submit(t, keyB, asTenantB), "run ID in use")
	admitted := f.admitted(t, record)
	ctx, stop := context.WithCancel(f.ctx)
	defer stop()
	done := make(chan kgo.AckStatus, 1)
	go func() { done <- f.receiver.execute(ctx, f.service.Admin, record, admitted, 1) }()
	for {
		if _, _, follows, _ := f.adapter.counts(); follows > 0 {
			break
		}
		if f.ctx.Err() != nil {
			t.Fatal("the run never followed its job")
		}
		time.Sleep(20 * time.Millisecond)
	}
	f.publishCancel(t, "run-a", 1, keyB, func(cancel *wire.Cancel) { cancel.Tenant, cancel.Host = "tenant-b", "host-b" })
	time.Sleep(3 * stateCheckInterval)
	f.adapter.mu.Lock()
	stops := f.adapter.stops
	f.adapter.mu.Unlock()
	select {
	case status := <-done:
		t.Fatalf("run ended with %v after tenant-b's cancel, want it still running", status)
	default:
	}
	if stops != 0 {
		t.Fatalf("stops %d, want tenant-a's run untouched by tenant-b's cancel", stops)
	}
	f.publishCancel(t, "run-a", 1, f.host, nil)
	select {
	case status := <-done:
		if status != kgo.AckAccept {
			t.Fatalf("status %v after tenant-a's cancel, want accept", status)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("tenant-a's own cancel did not stop its run")
	}
	if state, reason := terminalReason(t, f, "run-a"); state != wire.TaskStateCanceled || reason != "canceled by user" {
		t.Fatalf("terminal %s %q, want CANCELED", state, reason)
	}
}
