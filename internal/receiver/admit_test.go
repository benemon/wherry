package receiver

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json/jsontext"
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

func TestRunTimeIsCappedSixHoursAfterTheQueueDeadline(t *testing.T) {
	queueDeadline, twoHours := time.Now().Add(10*time.Minute).UnixMilli(), time.Now().Add(2*time.Hour).UnixMilli()
	for name, test := range map[string]struct{ deadline, want int64 }{
		"two hours":   {twoHours, twoHours},
		"seven hours": {time.Now().Add(7 * time.Hour).UnixMilli(), time.UnixMilli(queueDeadline).Add(6 * time.Hour).UnixMilli()},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.answer(t, credentialName)
			record := f.submit(t, f.host, func(task *wire.Submission) {
				task.Wherry.DeadlineMS, task.Wherry.QueueDeadlineMS = test.deadline, queueDeadline
			})
			// The fixture's context ends within a minute, sooner than either deadline.
			if status := f.receiver.execute(context.WithoutCancel(f.ctx), f.service.Admin, record, f.admitted(t, record), 1); status != kgo.AckAccept {
				t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
			}
			f.adapter.mu.Lock()
			defer f.adapter.mu.Unlock()
			if got := f.adapter.lastDeadline.UnixMilli(); got != test.want {
				t.Fatalf("run deadline %v, want %v", f.adapter.lastDeadline, time.UnixMilli(test.want))
			}
		})
	}
}

func TestAdmitRejectsRunPastLifetime(t *testing.T) {
	terms := testTerms()
	terms.MaxQueueDeadlineMS = 1
	f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakeAdapter{"nomad": {stream: resultStream}})
	record := f.turn(t, "run-expired", 1)
	time.Sleep(10 * time.Millisecond)
	f.rejected(t, record, "run expired")
}

func TestAdmitRejectsQueueDeadlinePastMaxQueueDeadline(t *testing.T) {
	terms := testTerms()
	terms.MaxQueueDeadlineMS = admin.Millis(time.Hour.Milliseconds())
	f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakeAdapter{"nomad": {stream: resultStream}})
	first := f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Run, task.Message.TaskID, task.Message.ContextID = "run-queued", "run-queued", "run-queued"
		task.Wherry.QueueDeadlineMS = time.Now().Add(2 * time.Hour).UnixMilli()
	})
	f.rejected(t, first, "queue deadline exceeds the pool's max queue deadline")
}

func TestRedeliveredClaimedRunPastMaxQueueDeadlineIsAdopted(t *testing.T) {
	terms := testTerms()
	terms.MaxQueueDeadlineMS = 50
	f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakeAdapter{"nomad": {stream: resultStream, job: adapter.JobRunning}})
	record := startedTurn(t, f, "run-held", func(task *wire.Submission) {
		task.Wherry.QueueDeadlineMS = time.Now().Add(20 * time.Millisecond).UnixMilli()
	})
	f.answer(t, credentialName)
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

func TestPlacementOutsideOffersReleasesWithoutARecord(t *testing.T) {
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
	if admitted.adapter.Name != "nomad" || admitted.adapter.Labels["example.com/gpu"] != "none" {
		t.Fatalf("adapter %#v", admitted.adapter)
	}
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
	f.produce(t, admin.PoolEventsTopic(testPool), "run-a", must(t)(wire.SignRecord(&wire.Cancel{Tenant: testTenant, Host: testHost, Pool: testPool, Run: "run-a"}, f.host)))
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

func TestRunsUnderRetention(t *testing.T) {
	terms := testTerms()
	terms.MaxQueueDeadlineMS = admin.Millis(time.Hour.Milliseconds())
	f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakeAdapter{"nomad": {stream: resultStream, job: adapter.JobRunning}})
	aged := func(topic, key string, value []byte, age time.Duration) *kgo.Record {
		t.Helper()
		record := &kgo.Record{Topic: topic, Key: []byte(key), Value: value, Timestamp: time.Now().Add(-age)}
		if err := f.service.Admin.ProduceSync(f.ctx, record).FirstErr(); err != nil {
			t.Fatal(err)
		}
		return record
	}
	run := func(run string, age time.Duration, kind string, state wire.TaskState) (*kgo.Record, *wire.Submission) {
		t.Helper()
		task := testSubmission(run)
		record := aged(admin.PoolTasksTopic(testPool), run, must(t)(wire.SignRecord(task, f.host)), age)
		status := f.receiver.status(task, wire.TaskHash(record.Value), 1, kind, taskStatus(state, ""), nil)
		aged(admin.PoolEventsTopic(testPool), run, must(t)(wire.SignRecord(status, f.receiver.signing)), age)
		return record, task
	}
	run("run-ancient", 32*time.Hour, "terminal", wire.TaskStateCompleted)
	completed, _ := run("run-completed", 14*time.Hour, "terminal", wire.TaskStateCompleted)
	adopted, adoptedTask := run("run-adopted", 50*time.Minute, "started", wire.TaskStateWorking)
	aged(admin.PoolEventsTopic(testPool), "run-adopted", f.heartbeat(t, adoptedTask, wire.TaskHash(adopted.Value), testReceiver, f.receiver.signing, 41, nil), 50*time.Minute)
	f.answer(t, credentialName)
	f.cluster.ApplyRetention()

	for _, topic := range []string{admin.PoolTasksTopic(testPool), admin.PoolEventsTopic(testPool)} {
		if slices.ContainsFunc(f.records(t, topic), func(record *kgo.Record) bool { return string(record.Key) == "run-ancient" }) {
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
	if offset, err := f.receiver.lastOffset(f.ctx, adoptedTask, wire.TaskHash(adopted.Value)); err != nil || offset != 41 {
		t.Fatalf("adoption offset %d, %v; want 41", offset, err)
	}
	if status := f.receiver.execute(f.ctx, f.service.Admin, adopted, f.admitted(t, adopted), 2); status != kgo.AckAccept {
		t.Fatalf("adopted status %v; logs:\n%s", status, f.logs.String())
	}
	if dispatches, _, follows, _ := f.adapter.counts(); dispatches != 0 || follows != 1 {
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
