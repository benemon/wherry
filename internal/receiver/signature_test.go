package receiver

import (
	"crypto/ecdsa"
	"errors"
	"io"
	"log"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Budgets on kfake with a healthy broker: from a subscription change to the old member's leave, the watcher's 100 ms
// tick, the close's 1 s FetchMaxWait and 500 ms for the scheduler; from that leave to the replacement's join, 1.5 s.
const (
	leaveBudget = 100*time.Millisecond + time.Second + 500*time.Millisecond
	joinBudget  = 1500 * time.Millisecond
)

// signatureProfile is a fake profile launching agent with labels.
func signatureProfile(name, agent string, labels map[string]string) operator.ProfileSettings {
	return operator.ProfileSettings{Name: name, Plugin: "fake", Agent: agent, Labels: labels, Config: map[string]any{}}
}

// signatureFixture is a fixture of capacity whose profiles, in order, are ready fakes; f.plugin is the first profile's.
func signatureFixture(t *testing.T, capacity int64, profiles ...operator.ProfileSettings) fixture {
	t.Helper()
	config := testConfig(testReceiver)
	config.Capacity, config.Profiles = capacity, profiles
	plugins := map[string]*fakePlugin{}
	for _, profile := range profiles {
		plugins[profile.Name] = &fakePlugin{stream: resultStream}
	}
	f := newFixtureWith(t, resultStream, testTerms(), config, plugins)
	f.plugin = plugins[profiles[0].Name]
	return f
}

// twoSignatures is a signatureFixture of capacity with profiles a and b, launching claude with sig=a and sig=b.
func twoSignatures(t *testing.T, capacity int64) fixture {
	t.Helper()
	return signatureFixture(t, capacity,
		signatureProfile("a", "claude", map[string]string{"sig": "a"}), signatureProfile("b", "claude", map[string]string{"sig": "b"}))
}

// ready sets profile's readiness and advertises, so the receiver's offers follow it at once.
func (f fixture) ready(t *testing.T, profile string, ready bool) {
	t.Helper()
	plugin := f.plugins[profile]
	plugin.mu.Lock()
	plugin.unready = !ready
	plugin.mu.Unlock()
	if err := f.receiver.advertise(f.ctx); err != nil {
		t.Fatal(err)
	}
}

// profileTopic is the signature topic of the receiver's profile.
func (f fixture) profileTopic(t *testing.T, profile string) string {
	t.Helper()
	index := slices.IndexFunc(f.receiver.profiles, func(named Named) bool { return named.Name == profile })
	if index < 0 {
		t.Fatalf("no profile %s", profile)
	}
	named := f.receiver.profiles[index]
	return operator.SignatureTopic(testPool, named.Agent, named.Labels)
}

// topics sorts topics, as a subscription is.
func topics(topics ...string) []string {
	return slices.Sorted(slices.Values(topics))
}

// awaitMember waits up to d for a live member subscribed to exactly want and returns it.
func (f fixture) awaitMember(t *testing.T, d time.Duration, want []string) shareMember {
	t.Helper()
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
		for _, member := range f.members.all() {
			if member.left.IsZero() && slices.Equal(member.topics, want) {
				return member
			}
		}
	}
	t.Fatalf("no live member subscribed to %v within %s; members %+v; logs:\n%s", want, d, f.members.all(), f.logs.String())
	return shareMember{}
}

// member is the member with id as the log holds it now.
func (f fixture) shareMember(id string) shareMember {
	members := f.members.all()
	return members[slices.IndexFunc(members, func(member shareMember) bool { return member.id == id })]
}

// awaitReopen checks that old leaves within leaveBudget of changed and a member subscribed to exactly want joins within
// joinBudget of that leave, and returns the new member.
func (f fixture) awaitReopen(t *testing.T, old shareMember, changed time.Time, want []string) shareMember {
	t.Helper()
	for f.shareMember(old.id).left.IsZero() {
		if waited := time.Since(changed); waited > leaveBudget {
			t.Fatalf("the member subscribed to %v did not leave within %s of the change; logs:\n%s", old.topics, leaveBudget, f.logs.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
	left := f.shareMember(old.id).left
	for {
		for _, member := range f.joinedSince(left) {
			if !slices.Equal(member.topics, want) {
				t.Fatalf("a member joined subscribed to %v after the change, want %v", member.topics, want)
			}
			return member
		}
		if waited := time.Since(left); waited > joinBudget {
			t.Fatalf("no member subscribed to %v joined within %s of the old member's leave; logs:\n%s", want, joinBudget, f.logs.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// claimedRuns are the runs with a claimed status on the pool's events.
func (f fixture) claimedRuns(t *testing.T) []string {
	t.Helper()
	var runs []string
	for _, status := range f.eventStatuses(t) {
		if status.Wherry.Kind == "claimed" {
			runs = append(runs, status.Wherry.Run)
		}
	}
	return runs
}

// Idle refresh: an idle slot subscribed to {A} closes its poll and reopens with {A, B} when B becomes ready while A
// stays ready, then with {B} when A goes unready; a task on A's topic then reaches no member until A is ready again.
func TestIdleSlotReopensWithTheReadyProfilesTopics(t *testing.T) {
	f := twoSignatures(t, 1)
	topicA, topicB := f.profileTopic(t, "a"), f.profileTopic(t, "b")
	f.ready(t, "b", false)
	f.run(t, f.ctx)
	first := f.awaitMember(t, 10*time.Second, []string{topicA})
	changed := time.Now()
	f.ready(t, "b", true)
	both := f.awaitReopen(t, first, changed, topics(topicA, topicB))
	changed = time.Now()
	f.ready(t, "a", false)
	f.awaitReopen(t, both, changed, []string{topicB})
	// Positive control: a task on B's topic is claimed and completes.
	onB := f.submit(t, f.host, func(task *wire.Submission) {
		named(task, "run-b")
		task.Wherry.Placement.Selector = []wire.Requirement{in("sig", "b")}
	})
	if onB.Topic != topicB {
		t.Fatalf("task on %s, want B's topic", onB.Topic)
	}
	f.await(t, testRun("run-b"), func(task wire.Task) bool { return task.Status.State == wire.TaskStateCompleted })
	onA := f.submit(t, f.host, func(task *wire.Submission) { named(task, "run-a") })
	if onA.Topic != topicA {
		t.Fatalf("task on %s, want A's topic", onA.Topic)
	}
	time.Sleep(3 * time.Second)
	if slices.Contains(f.claimedRuns(t), testRun("run-a")) {
		t.Fatalf("a task on A's topic was claimed while A is unready; logs:\n%s", f.logs.String())
	}
	if dispatches, _, _, _ := f.plugins["a"].counts(); dispatches != 0 {
		t.Fatalf("profile a dispatched %d times while unready", dispatches)
	}
	f.ready(t, "a", true)
	f.await(t, testRun("run-a"), func(task wire.Task) bool { return task.Status.State == wire.TaskStateCompleted })
}

// Idle refresh, a delayed close: the replacement joins only once the old client's close, held here at its leave, has
// completed.
func TestIdleSlotReopensOnlyAfterTheOldClientCloses(t *testing.T) {
	f := twoSignatures(t, 1)
	topicA, topicB := f.profileTopic(t, "a"), f.profileTopic(t, "b")
	f.ready(t, "b", false)
	f.run(t, f.ctx)
	first := f.awaitMember(t, 10*time.Second, []string{topicA})
	var mu sync.Mutex
	var closed time.Time
	f.cluster.ControlKey(kmsg.ShareGroupHeartbeat.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		if heartbeat := request.(*kmsg.ShareGroupHeartbeatRequest); heartbeat.MemberID == first.id && heartbeat.MemberEpoch == -1 {
			f.cluster.SleepControl(func() { time.Sleep(2 * time.Second) })
			mu.Lock()
			closed = time.Now()
			mu.Unlock()
		}
		return nil, nil, false
	})
	f.ready(t, "b", true)
	next := f.awaitMember(t, 10*time.Second, topics(topicA, topicB))
	mu.Lock()
	defer mu.Unlock()
	if left := f.shareMember(first.id).left; left.IsZero() || closed.IsZero() {
		t.Fatalf("the old member's leave was not seen; logs:\n%s", f.logs.String())
	}
	if !next.joined.After(closed) {
		t.Fatalf("the replacement joined %s before the old client's close completed", closed.Sub(next.joined))
	}
	if waited := next.joined.Sub(closed); waited > joinBudget {
		t.Fatalf("the replacement joined %s after the close completed, want within %s", waited, joinBudget)
	}
}

// Compaction: two ready profiles of one signature put its topic in the subscription once; one of them going unready
// leaves the subscription, and the poll, as they are.
func TestOneSignatureOfTwoProfilesIsSubscribedOnce(t *testing.T) {
	f := signatureFixture(t, 1,
		signatureProfile("a", "claude", map[string]string{"sig": "a", "gpu": "a100"}), signatureProfile("a2", "claude", map[string]string{"sig": "a", "gpu": "a100"}))
	topic := f.profileTopic(t, "a")
	if other := f.profileTopic(t, "a2"); other != topic {
		t.Fatalf("profiles a and a2 on %s and %s, want one signature", topic, other)
	}
	f.run(t, f.ctx)
	member := f.awaitMember(t, 10*time.Second, []string{topic})
	changed := time.Now()
	f.ready(t, "a2", false)
	time.Sleep(leaveBudget + joinBudget)
	if left := f.shareMember(member.id).left; !left.IsZero() {
		t.Fatalf("the member left %s after a profile of its one signature went unready, want its poll kept", left.Sub(changed))
	}
	if joined := f.joinedSince(changed); len(joined) != 0 {
		t.Fatalf("%d members joined after a profile of the subscribed signature went unready, want none", len(joined))
	}
	// Positive control: with no profile of the signature ready, the member leaves.
	changed = time.Now()
	f.ready(t, "a", false)
	for f.shareMember(member.id).left.IsZero() {
		if time.Since(changed) > leaveBudget {
			t.Fatalf("the member did not leave within %s once no profile was ready", leaveBudget)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Renewal during refresh: a held run keeps its share client and renews through a readiness change while the idle slot's
// client closes and a new one joins with the new subscription.
func TestHeldRunRenewsWhileTheIdleSlotReopens(t *testing.T) {
	f := twoSignatures(t, 2)
	topicA, topicB := f.profileTopic(t, "a"), f.profileTopic(t, "b")
	f.plugin.endDelay = 6 * time.Second
	f.ready(t, "b", false)
	renewals := f.renewals(t)
	f.submit(t, f.host, nil)
	f.run(t, f.ctx)
	for {
		if _, _, follows, _ := f.plugin.counts(); follows == 1 {
			break
		}
		if f.ctx.Err() != nil {
			t.Fatalf("run never started; logs:\n%s", f.logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	// The held run's client and the idle slot's, both joined subscribed to {A}.
	var before []shareMember
	for len(before) != 2 {
		before = nil
		for _, member := range f.members.all() {
			if member.left.IsZero() {
				before = append(before, member)
			}
		}
		if f.ctx.Err() != nil {
			t.Fatalf("live members %+v, want the held run's and the idle slot's", before)
		}
		time.Sleep(10 * time.Millisecond)
	}
	changed := time.Now()
	f.ready(t, "b", true)
	next := f.awaitMember(t, leaveBudget+joinBudget, topics(topicA, topicB))
	var stayed, left []shareMember
	for _, member := range before {
		if member = f.shareMember(member.id); member.left.IsZero() {
			stayed = append(stayed, member)
		} else {
			left = append(left, member)
		}
	}
	if len(stayed) != 1 || len(left) != 1 || !slices.Equal(stayed[0].topics, []string{topicA}) {
		t.Fatalf("after the change %d of the two members stayed, want the held run's alone", len(stayed))
	}
	var renewed int
	for _, at := range renewals() {
		if at.After(changed) && at.Before(next.joined) {
			renewed++
		}
	}
	if renewed == 0 {
		t.Fatalf("no renewal between the readiness change and the new member's join %s later", next.joined.Sub(changed))
	}
	f.await(t, testRun("run-a"), func(task wire.Task) bool { return task.Status.State == wire.TaskStateCompleted })
	if kinds := f.kinds(t); slices.Contains(kinds, "released") {
		t.Fatalf("statuses %v, want the held run kept to its end", kinds)
	}
}

// Gate: with an idle slot free, no run pending and the task eligible again, the slot that failed to start it joins again
// within 1.5 s of the start failure; nothing waits out a lock duration.
func TestSlotRejoinsPromptlyAfterAStartFailure(t *testing.T) {
	config := testConfig(testReceiver)
	config.Capacity = 2
	f := newFixtureWith(t, resultStream, testTerms(), config, map[string]*fakePlugin{"nomad": {stream: resultStream, outcome: pluginsdk.OutcomeNothingCreated}})
	f.submit(t, f.host, nil)
	f.run(t, f.ctx)
	var failed time.Time
	for failed.IsZero() {
		for _, record := range f.records(t, operator.PoolEventsTopic(testPool)) {
			if recordType(record.Value) == wire.StartFailureType {
				failed = record.Timestamp
				break
			}
		}
		if f.ctx.Err() != nil {
			t.Fatalf("no start failure; logs:\n%s", f.logs.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	for len(f.joinedSince(failed)) == 0 {
		if waited := time.Since(failed); waited > joinBudget {
			t.Fatalf("no member joined within %s of the start failure; logs:\n%s", joinBudget, f.logs.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Admission, agent: a task on another agent's signature topic, or on no signature topic, is rejected with the fixed
// reason on the pool's events.
func TestTaskOnAnotherAgentsTopicIsRejected(t *testing.T) {
	f := newFixture(t, resultStream)
	for name, topic := range map[string]string{
		"another agent":     operator.SignatureTopic(testPool, "codex", nil),
		"no signature":      "wherry.pool." + testPool + ".tasks",
		"another pool's":    operator.SignatureTopic("pool-b", "codex", nil),
		"uppercase digest":  strings.ToUpper(f.tasksTopic()),
		"a dotted agent ok": operator.PoolTasksPrefix(testPool) + "claude.x." + strings.Repeat("a", 64),
	} {
		t.Run(name, func(t *testing.T) {
			record := f.submit(t, f.host, func(task *wire.Submission) { named(task, "run-"+strings.ReplaceAll(name, " ", "-")) })
			// Positive control: the task on its own topic would be admitted.
			if _, agent, ok := wire.ParseTaskTopic(record.Topic); !ok || agent != "claude" {
				t.Fatalf("task on %s", record.Topic)
			}
			forged := *record
			forged.Topic = topic
			f.rejected(t, &forged, "agent does not match the task topic")
			if !slices.ContainsFunc(f.eventStatuses(t), func(status wire.RunStatus) bool {
				return status.Wherry.Run == string(record.Key) && status.Wherry.Kind == "rejected" &&
					status.StatusUpdate.Status.Message.Parts[0].Text == "agent does not match the task topic"
			}) {
				t.Fatal("no rejection with the fixed reason on the pool's events")
			}
		})
	}
	// Positive control: the same task on its own topic is admitted.
	f.admitted(t, f.submit(t, f.host, func(task *wire.Submission) { named(task, "run-own") }))
}

// Admission, selector: a signed task on the signature's own topic whose selector the signature's labels do not satisfy,
// as a host that computed the wrong topic would publish it, is rejected with the fixed reason on the pool's events and
// never released, whatever the operator.
func TestTaskWhoseSelectorItsTopicsSignatureFailsIsRejected(t *testing.T) {
	f := signatureFixture(t, 1, signatureProfile("nomad", "claude", map[string]string{"platform": "nomad"}))
	topic := f.profileTopic(t, "nomad")
	// Positive control: a selector the signature satisfies is admitted on its topic.
	matching := f.submit(t, f.host, func(task *wire.Submission) {
		named(task, "run-matching")
		task.Wherry.Placement.Selector = []wire.Requirement{in("platform", "nomad")}
	})
	if matching.Topic != topic {
		t.Fatalf("the matching task on %s, want the signature's %s", matching.Topic, topic)
	}
	f.admitted(t, matching)
	for name, selector := range map[string][]wire.Requirement{
		"In":           {in("platform", "kubernetes")},
		"NotIn":        {notIn("platform", "nomad")},
		"Exists":       {exists("gpu")},
		"DoesNotExist": {doesNotExist("platform")},
	} {
		t.Run(name, func(t *testing.T) {
			run := "run-" + strings.ToLower(name)
			record := f.submit(t, f.host, func(task *wire.Submission) {
				named(task, run)
				task.Wherry.Placement.Selector = selector
			})
			wrong := *record
			wrong.Topic = topic
			f.rejected(t, &wrong, "selector does not match the task topic")
			if !slices.ContainsFunc(f.eventStatuses(t), func(status wire.RunStatus) bool {
				return status.Wherry.Run == testRun(run) && status.Wherry.Kind == "rejected" &&
					status.StatusUpdate.Status.Message.Parts[0].Text == "selector does not match the task topic"
			}) {
				t.Fatal("no rejection with the fixed reason on the pool's events")
			}
		})
	}
}

// Unready release: a task on a profile's topic fetched after the profile went unready is released, logged, with no
// record and no metric; the slot's replacement client subscribes to the ready profile's topic alone.
func TestTaskForAnUnreadyProfileIsReleased(t *testing.T) {
	f := twoSignatures(t, 1)
	topicA, topicB := f.profileTopic(t, "a"), f.profileTopic(t, "b")
	record := f.submit(t, f.host, nil)
	if record.Topic != topicA {
		t.Fatalf("task on %s, want A's topic", record.Topic)
	}
	f.ready(t, "a", false)
	before := len(f.records(t, operator.PoolEventsTopic(testPool)))
	if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckRelease {
		t.Fatalf("admitted=%v ack=%v, want release; logs:\n%s", admitted != nil, ack, f.logs.String())
	}
	if want := "run " + testRun("run-a") + ": no ready profile for topic " + topicA + "; releasing"; !strings.Contains(f.logs.String(), want) {
		t.Fatalf("logs lack %q:\n%s", want, f.logs.String())
	}
	if after := len(f.records(t, operator.PoolEventsTopic(testPool))); after != before {
		t.Fatalf("pool events %d -> %d, want nothing written", before, after)
	}
	for key, value := range zeroSamples(t, f.receiver) {
		if strings.HasPrefix(key, "wherry_start_failures_total{") && value != 0 {
			t.Fatalf("a start failure counted for the release: %s = %g", key, value)
		}
	}
	f.run(t, f.ctx)
	f.awaitMember(t, 10*time.Second, []string{topicB})
	for _, member := range f.members.all() {
		if !slices.Equal(member.topics, []string{topicB}) {
			t.Fatalf("a member subscribed to %v with profile a unready, want B's topic alone", member.topics)
		}
	}
}

// Ending without a profile: a claimed run past its bound whose profile is unready, with a job the plugin reports, is
// ended: its job stopped and its terminal the expired one, with no rejection; with its cancel held, the same but
// canceled; with its job pending, the same.
func TestClaimedRunPastItsBoundWithItsProfileUnreadyEnds(t *testing.T) {
	for name, c := range map[string]struct {
		job    pluginsdk.JobState
		cancel bool
	}{
		"running":     {pluginsdk.JobRunning, false},
		"cancel held": {pluginsdk.JobRunning, true},
		"pending job": {pluginsdk.JobPending, false},
	} {
		t.Run(name, func(t *testing.T) {
			f := signatureFixture(t, 1, signatureProfile("nomad", "claude", map[string]string{"zone": "dc1"}))
			f.plugin.job = c.job
			startBy := time.Now().Add(time.Second).Unix()
			record := f.submit(t, f.host, func(task *wire.Submission) {
				task.Wherry.TaskStartedBy = startBy
				task.Wherry.Placement.Selector = []wire.Requirement{in("zone", "dc1")}
			})
			f.publishStatus(t, record, testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
			if c.cancel {
				f.publishCancel(t, testRun("run-a"), 1, f.host, nil)
			}
			f.ready(t, "nomad", false)
			passed(startBy)
			admitted := f.admitted(t, record)
			if admitted.profile.Plugin != nil {
				t.Fatalf("admitted with profile %q, want none before the job is found", admitted.profile.Name)
			}
			status := f.receiver.execute(f.ctx, f.service.Operator, record, admitted, 2)
			f.plugin.mu.Lock()
			stops, dispatches := f.plugin.stops, f.plugin.dispatches
			f.plugin.mu.Unlock()
			if want := map[bool]kgo.AckStatus{false: kgo.AckReject, true: kgo.AckAccept}[c.cancel]; status != want || stops != 1 || dispatches != 0 {
				t.Fatalf("status %v stops %d dispatches %d, want the job stopped and the task ended; logs:\n%s", status, stops, dispatches, f.logs.String())
			}
			noRejection(t, f)
			state, reason := terminalReason(t, f, testRun("run-a"))
			if c.cancel {
				if state != wire.TaskStateCanceled {
					t.Fatalf("terminal %s %q, want CANCELED", state, reason)
				}
				return
			}
			if state != wire.TaskStateFailed || reason != wire.NotStartedBy(startBy) {
				t.Fatalf("terminal %s %q, want FAILED %q", state, reason, wire.NotStartedBy(startBy))
			}
		})
	}
}

// Copy check: a byte-for-byte copy of the claimed record at the same offset on another signature topic is acknowledged
// without a claim.
func TestCopyOnAnotherTopicAtTheSameOffsetIsAcknowledged(t *testing.T) {
	f := twoSignatures(t, 1)
	topicB := f.profileTopic(t, "b")
	record := f.submit(t, f.host, nil)
	f.statusAs(t, record, testReceiver, f.receiver.signing, "claimed", 1)
	copied := f.produce(t, topicB, string(record.Key), record.Value)
	if copied.Offset != record.Offset || copied.Topic == record.Topic {
		t.Fatalf("copy at %s/%d, original at %s/%d: want the same offset on another topic", copied.Topic, copied.Offset, record.Topic, record.Offset)
	}
	// Positive control: the claimed record itself goes on to adoption.
	f.admitted(t, record)
	claims := len(f.claimedRuns(t))
	if admitted, ack := f.receiver.admit(f.ctx, copied); admitted != nil || ack != kgo.AckAccept {
		t.Fatalf("admitted=%v ack=%v, want the copy acknowledged; logs:\n%s", admitted != nil, ack, f.logs.String())
	}
	if after := len(f.claimedRuns(t)); after != claims {
		t.Fatalf("claims %d -> %d, want none for the copy", claims, after)
	}
}

// Custodian read: once receiver apply changes the claimant's labels, the custodian reads the task at the claim's own
// topic, not one recomputed from the claimant's recorded profile, and refuses a claim naming a topic outside the pool's
// prefix.
func TestCustodianReadsTheClaimsTopic(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	claimant := f.peer(t, "receiver-b")
	record := f.submit(t, f.host, nil)
	f.statusAs(t, record, "receiver-b", claimant.signing, "claimed", 1)
	config := testConfig("receiver-b")
	config.Profiles[0].Labels = map[string]string{"zone": "dc2"}
	if _, err := f.service.ReceiverUpdate(f.ctx, config, false); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	state, _, _ := f.receiver.snapshot()
	recorded := state.Receivers["receiver-b"].Profiles[0]
	if recomputed := operator.SignatureTopic(testPool, recorded.Agent, recorded.Labels); recomputed == record.Topic {
		t.Fatalf("the claimant's recorded profile is still on %s after its labels changed", recomputed)
	}
	if answer := f.answered(t, f.openAs(t, record, "receiver-b", claimant.signing, 1)); answer != "wrap" {
		t.Fatalf("open answered %q, want served from the claim's topic; logs:\n%s", answer, f.logs.String())
	}
	other := f.submit(t, f.host, func(task *wire.Submission) { named(task, "run-elsewhere") })
	status := f.receiver.status(f.parse(t, other), wire.TaskHash(other.Value), 1, "claimed", taskStatus(wire.TaskStateWorking, ""), nil)
	elsewhere := operator.PoolTasksPrefix("pool-b") + "claude." + strings.Repeat("a", 64)
	status.Wherry.Receiver, status.Wherry.TaskTopic, status.Wherry.TaskOffset = "receiver-b", elsewhere, &other.Offset
	f.produce(t, operator.PoolEventsTopic(testPool), string(other.Key), must(t)(wire.SignRecord(status, claimant.signing)))
	if answer := f.answered(t, f.openAs(t, other, "receiver-b", claimant.signing, 1)); answer != wire.RefusedNotClaimant {
		t.Fatalf("open of a claim on %s answered %q, want %s", elsewhere, answer, wire.RefusedNotClaimant)
	}
}

// failTopicReads fails every listing of topic's offsets, so a read of it fails.
func failTopicReads(f fixture, topic string) {
	f.cluster.ControlKey(kmsg.ListOffsets.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		req := request.(*kmsg.ListOffsetsRequest)
		if !slices.ContainsFunc(req.Topics, func(listed kmsg.ListOffsetsRequestTopic) bool { return listed.Topic == topic }) {
			return nil, nil, false
		}
		response := kmsg.NewPtrListOffsetsResponse()
		response.SetVersion(req.Version)
		for _, listed := range req.Topics {
			out := kmsg.ListOffsetsResponseTopic{Topic: listed.Topic}
			for _, partition := range listed.Partitions {
				out.Partitions = append(out.Partitions, kmsg.ListOffsetsResponseTopicPartition{Partition: partition.Partition, ErrorCode: kerr.UnknownServerError.Code})
			}
			response.Topics = append(response.Topics, out)
		}
		return response, nil, true
	})
}

// failTopicListing answers each metadata request for every topic, while failing is set, with the cluster's own answer
// but topic's entry carrying an error.
func failTopicListing(t *testing.T, f fixture, topic string, failing *atomic.Bool) {
	t.Helper()
	admin, err := kgo.NewClient(f.service.Operator.Opts()...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	var inner atomic.Bool
	f.cluster.ControlKey(kmsg.Metadata.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		req := request.(*kmsg.MetadataRequest)
		if req.Topics != nil || !failing.Load() || inner.Load() {
			return nil, nil, false
		}
		var response *kmsg.MetadataResponse
		f.cluster.SleepControl(func() {
			inner.Store(true)
			defer inner.Store(false)
			response, err = kmsg.NewPtrMetadataRequest().RequestWith(f.ctx, admin)
		})
		if err != nil {
			return nil, err, true
		}
		response.SetVersion(req.Version)
		for i := range response.Topics {
			if response.Topics[i].Topic != nil && *response.Topics[i].Topic == topic {
				response.Topics[i].ErrorCode = kerr.LeaderNotAvailable.Code
			}
		}
		return response, nil, true
	})
}

// omitFromListings leaves every task topic of the pool out of each metadata request for every topic, as a listing from
// stale metadata would, until the returned function is called.
func omitFromListings(f fixture, taskTopics ...string) func() {
	var handles []*kfake.FaultHandle
	for _, topic := range taskTopics {
		handles = append(handles, f.cluster.Fault(kfake.Fault{
			Keys: []kmsg.Key{kmsg.Metadata}, Topic: topic, Err: kerr.UnknownTopicOrPartition, Count: -1,
			When: func(request kmsg.Request) bool { return request.(*kmsg.MetadataRequest).Topics == nil },
		}))
	}
	return func() {
		for _, handle := range handles {
			handle.Remove()
		}
	}
}

// Re-key scan: the scan lists the pool's task topics and reads each; with three topics it re-keys every eligible task and
// deletes the keys, and any failing read, a failed listing, keeps every key; an empty listing deletes them with nothing
// re-keyed. A re-keyed task on the second topic opens under the successor's key.
func TestRekeyScanReadsEveryTaskTopic(t *testing.T) {
	three := func(t *testing.T) (fixture, []*kgo.Record, wire.PoolKey, *ecdsa.PrivateKey) {
		f := signatureFixture(t, 1,
			signatureProfile("a", "claude", map[string]string{"sig": "a"}), signatureProfile("b", "claude", map[string]string{"sig": "b"}),
			signatureProfile("c", "claude", map[string]string{"sig": "c"}))
		f.quiet()
		var tasks []*kgo.Record
		for _, sig := range []string{"a", "b", "c"} {
			tasks = append(tasks, f.submit(t, f.host, func(task *wire.Submission) {
				named(task, "run-"+sig)
				task.Wherry.Placement.Selector = []wire.Requirement{in("sig", sig)}
			}))
		}
		if tasks[0].Topic == tasks[1].Topic || tasks[1].Topic == tasks[2].Topic {
			t.Fatal("the tasks share a topic")
		}
		successor, err := newAttemptKey()
		if err != nil {
			t.Fatal(err)
		}
		return f, tasks, wire.PoolKey{KeyID: signed.KeyThumbprint(&successor.PublicKey), Key: jose.JSONWebKey{Key: &successor.PublicKey}}, successor
	}
	kept := func(t *testing.T, f fixture, held int) {
		t.Helper()
		if keys := heldKeys(t, f.receiver); len(keys) != held || held == 0 {
			t.Fatalf("keys %s, want the %d held kept", describeKeys(keys), held)
		}
		if rekeys := f.rekeys(t); len(rekeys) != 0 {
			t.Fatalf("re-key wraps for %d runs from a scan that failed", len(rekeys))
		}
	}
	t.Run("all succeed", func(t *testing.T) {
		f, tasks, current, successor := three(t)
		if err := f.receiver.handOver(f.ctx, current); err != nil {
			t.Fatal(err)
		}
		rekeys := f.rekeys(t)
		for _, task := range tasks {
			if len(rekeys[string(task.Key)]) != 1 {
				t.Fatalf("run on %s re-keyed %d times, want once", task.Topic, len(rekeys[string(task.Key)]))
			}
		}
		if keys := heldKeys(t, f.receiver); len(keys) != 0 {
			t.Fatalf("keys %s after the scan, want them deleted", describeKeys(keys))
		}
		// The second topic's task opens under the successor's key alone.
		second := f.parse(t, tasks[1])
		if contentKey := f.receiver.contentKey(second, map[string]*poolKey{current.KeyID: {Key: jose.JSONWebKey{Key: successor}}}, rekeys[string(tasks[1].Key)]); contentKey == nil {
			t.Fatal("the second topic's re-keyed task does not open under the successor's key")
		}
	})
	t.Run("one read fails", func(t *testing.T) {
		f, tasks, current, _ := three(t)
		held := len(heldKeys(t, f.receiver))
		failTopicReads(f, tasks[1].Topic)
		if err := f.receiver.handOver(f.ctx, current); err == nil {
			t.Fatal("the scan succeeded with a read failing")
		}
		kept(t, f, held)
	})
	t.Run("listing fails", func(t *testing.T) {
		f, tasks, current, _ := three(t)
		held := len(heldKeys(t, f.receiver))
		var failing atomic.Bool
		failing.Store(true)
		failTopicListing(t, f, tasks[2].Topic, &failing)
		if err := f.receiver.handOver(f.ctx, current); err == nil {
			t.Fatal("the scan succeeded with its listing failing")
		}
		failing.Store(false)
		kept(t, f, held)
	})
	t.Run("empty listing", func(t *testing.T) {
		f, tasks, current, _ := three(t)
		restore := omitFromListings(f, tasks[0].Topic, tasks[1].Topic, tasks[2].Topic)
		defer restore()
		if err := f.receiver.handOver(f.ctx, current); err != nil {
			t.Fatal(err)
		}
		if keys := heldKeys(t, f.receiver); len(keys) != 0 {
			t.Fatalf("keys %s after an empty scan, want them deleted", describeKeys(keys))
		}
		if rekeys := f.rekeys(t); len(rekeys) != 0 {
			t.Fatalf("re-key wraps for %d runs from an empty listing", len(rekeys))
		}
		if !strings.Contains(f.logs.String(), "re-keyed 0 tasks") {
			t.Fatalf("logs:\n%s", f.logs.String())
		}
	})
}

// Label change: receiver apply changing a profile's label creates the new signature's topic before the record that
// moves the profile; the receiver ends its held run, exits and, restarted, subscribes to the new topic alone. A task its
// host queued on the old topic is never fetched there and its host reports it unclaimed at task_started_by, unless a
// second member shares the old signature, which serves it.
func TestLabelChangeMovesTheReceiversSignature(t *testing.T) {
	for name, shared := range map[string]bool{"alone": false, "a peer shares the old signature": true} {
		t.Run(name, func(t *testing.T) { labelChange(t, shared) })
	}
}

func labelChange(t *testing.T, shared bool) {
	f := signatureFixture(t, 1, signatureProfile("nomad", "claude", map[string]string{"zone": "a"}))
	f.plugin.endDelay = 4 * time.Second
	topicA := f.profileTopic(t, "nomad")
	topicB := operator.SignatureTopic(testPool, "claude", map[string]string{"zone": "b"})
	// The held run fills the receiver's one slot, so the task its host queues next waits on A's topic.
	f.submit(t, f.host, nil)
	done := f.run(t, f.ctx)
	for {
		if _, _, follows, _ := f.plugin.counts(); follows > 0 {
			break
		}
		if f.ctx.Err() != nil {
			t.Fatalf("the held run never started; logs:\n%s", f.logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Setenv("WHERRY_TEST_TOKEN", testToken)
	kafka, err := kgo.NewClient(append(f.service.Operator.Opts(), kgo.FetchIsolationLevel(kgo.ReadCommitted()))...)
	if err != nil {
		t.Fatal(err)
	}
	defer kafka.Close()
	host, err := client.NewHost(kafka, f.host, operator.TenantPrincipal(testTenant, testHost), filepath.Join(t.TempDir(), "host"),
		map[string]credential.Source{credentialName: {Env: "WHERRY_TEST_TOKEN"}}, nil, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	startBy := time.Now().Add(15 * time.Second)
	queued, err := host.Submit(f.ctx, client.SubmitInput{Agent: "claude", Prompt: "hello", StartBy: startBy, EndBy: startBy.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(f.records(t, topicA), func(record *kgo.Record) bool { return string(record.Key) == queued.RunID }) {
		t.Fatalf("the queued task is not on A's topic %s", topicA)
	}
	// At the creation of B's topic the record still holds the profile on A.
	var created, recordFirst atomic.Bool
	f.cluster.ControlKey(kmsg.CreateTopics.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		if created.Load() || !slices.ContainsFunc(request.(*kmsg.CreateTopicsRequest).Topics, func(topic kmsg.CreateTopicsRequestTopic) bool { return topic.Topic == topicB }) {
			return nil, nil, false
		}
		created.Store(true)
		f.cluster.SleepControl(func() {
			state, err := f.service.LoadState(f.ctx)
			recordFirst.Store(err != nil || state.Receivers[testReceiver].Profiles[0].Labels["zone"] != "a")
		})
		return nil, nil, false
	})
	config := testConfig(testReceiver)
	config.Profiles = []operator.ProfileSettings{signatureProfile("nomad", "claude", map[string]string{"zone": "b"})}
	if _, err := f.service.ReceiverUpdate(f.ctx, config, false); err != nil {
		t.Fatal(err)
	}
	if !created.Load() || recordFirst.Load() {
		t.Fatalf("B's topic created %t, after the record %t; want it created while the record still held A", created.Load(), recordFirst.Load())
	}
	// The receiver ends its held run and exits on the new record; it keeps no poll on A's topic.
	select {
	case err := <-done:
		if !errors.Is(err, ErrConfigChanged) {
			t.Fatalf("Run returned %v, want ErrConfigChanged", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("the receiver kept running on its old signature; members %+v; logs:\n%s", f.members.all(), f.logs.String())
	}
	restartedAt := time.Now()
	runUntilStopped(t, f.ctx, restarted(t, f.receiver))
	f.awaitMember(t, 10*time.Second, []string{topicB})
	for _, member := range f.members.all() {
		if member.joined.After(restartedAt) && !slices.Equal(member.topics, []string{topicB}) || member.left.IsZero() && slices.Contains(member.topics, topicA) {
			t.Fatalf("a member subscribed to %v after the restart, want B's topic alone; members %+v", member.topics, f.members.all())
		}
	}
	if shared {
		peerPlugin := &fakePlugin{stream: resultStream}
		f.plugins["peer"] = peerPlugin
		peerConfig := testConfig("receiver-c")
		peerConfig.Profiles = []operator.ProfileSettings{signatureProfile("peer", "claude", map[string]string{"zone": "a"})}
		peer, _ := f.newReceiver(t, peerConfig)
		peer.logger = f.receiver.logger
		runUntilStopped(t, f.ctx, peer)
		awaited, err := host.Await(f.ctx, queued.RunID, 30*time.Second)
		if err != nil || awaited.Status.State != wire.TaskStateCompleted {
			t.Fatalf("await %+v, %v; want the queued task served by the peer on the old signature; logs:\n%s", awaited, err, f.logs.String())
		}
		if dispatches, _, _, _ := peerPlugin.counts(); dispatches != 1 {
			t.Fatalf("the peer dispatched %d times, want the queued task once", dispatches)
		}
		if dispatches, _, _, _ := f.plugin.counts(); dispatches != 1 {
			t.Fatalf("the moved profile dispatched %d times, want the held run alone", dispatches)
		}
		return
	}
	passed(startBy.Unix())
	awaited, err := host.Await(f.ctx, queued.RunID, 30*time.Second)
	if err != nil || awaited.Unclaimed == "" || awaited.Status.State != wire.TaskStateSubmitted {
		t.Fatalf("await %+v, %v; want the queued task reported unclaimed at task_started_by", awaited, err)
	}
	if slices.Contains(f.claimedRuns(t), queued.RunID) {
		t.Fatalf("the task queued on the old signature was claimed; logs:\n%s", f.logs.String())
	}
	if dispatches, _, _, _ := f.plugin.counts(); dispatches != 1 {
		t.Fatalf("the moved profile dispatched %d times, want the held run alone", dispatches)
	}
}
