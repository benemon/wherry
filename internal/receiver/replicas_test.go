package receiver

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func waitFor(t *testing.T, f fixture, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !ok() {
		if time.Now().After(deadline) || f.ctx.Err() != nil {
			f.receiver.mu.Lock()
			held := f.receiver.inFlight
			f.receiver.mu.Unlock()
			t.Fatalf("condition not reached (held records=%d); logs:\n%s", held, f.logs.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type profileMembers struct {
	mu    sync.Mutex
	names map[string]string
}
type profileLogger struct {
	members *profileMembers
	profile string
}

func (profileLogger) Level() kgo.LogLevel { return kgo.LogLevelDebug }
func (l profileLogger) Log(_ kgo.LogLevel, msg string, values ...any) {
	if msg != "share group heartbeating" {
		return
	}
	for i := 0; i+1 < len(values); i += 2 {
		if values[i] == "member_id" {
			l.members.mu.Lock()
			l.members.names[values[i+1].(string)] = l.profile
			l.members.mu.Unlock()
		}
	}
}
func identifyProfiles(t *testing.T) *profileMembers {
	t.Helper()
	m := &profileMembers{names: map[string]string{}}
	old := newClaimClient
	newClaimClient = func(r *Receiver, p Named, opts ...kgo.Opt) (*kgo.Client, error) {
		return r.newClient(append(opts, kgo.WithLogger(profileLogger{m, p.Name}))...)
	}
	t.Cleanup(func() { newClaimClient = old })
	return m
}
func (m *profileMembers) name(id string) string { m.mu.Lock(); defer m.mu.Unlock(); return m.names[id] }
func liveProfile(f fixture, m *profileMembers, name string) []shareMember {
	var out []shareMember
	for _, member := range f.members.all() {
		if member.left.IsZero() && m.name(member.id) == name {
			out = append(out, member)
		}
	}
	return out
}

func profileReadiness(t *testing.T, duplicate bool, replicas int) {
	m := identifyProfiles(t)
	a := signatureProfile("a", "claude", map[string]string{"sig": "a"})
	a.Replicas = replicas
	b := signatureProfile("b", "claude", map[string]string{"sig": "b"})
	if duplicate {
		b.Labels = a.Labels
	}
	f := signatureFixture(t, a, b)
	f.run(t, f.ctx)
	waitFor(t, f, func() bool { return len(liveProfile(f, m, "a")) == replicas && len(liveProfile(f, m, "b")) == 1 })
	originalB := liveProfile(f, m, "b")[0]
	for cycle := 0; cycle < 2; cycle++ {
		previous := liveProfile(f, m, "a")
		f.ready(t, "a", false)
		waitFor(t, f, func() bool { return len(liveProfile(f, m, "a")) == 0 })
		if cycle == 1 {
			f.plugins["b"].mu.Lock()
			f.plugins["b"].endDelay = time.Minute
			f.plugins["b"].mu.Unlock()
			f.submit(t, f.host, func(task *wire.Submission) {
				named(task, "run-b")
				if !duplicate {
					task.Wherry.Placement.Selector = []wire.Requirement{in("sig", "b")}
				}
			})
			waitFor(t, f, func() bool { _, _, n, _ := f.plugins["b"].counts(); return n == 1 })
		}
		time.Sleep(leaveBudget + joinBudget)
		if !f.shareMember(originalB.id).left.IsZero() {
			t.Fatal("B left or rejoined after A readiness changed")
		}
		if cycle == 0 {
			restored := time.Now()
			f.ready(t, "a", true)
			waitFor(t, f, func() bool { return len(liveProfile(f, m, "a")) == replicas })
			joined := 0
			for _, member := range f.joinedSince(restored) {
				if m.name(member.id) == "a" {
					joined++
				}
			}
			if joined != replicas {
				t.Fatalf("A joined %d times on restoration, want %d", joined, replicas)
			}
			for _, member := range liveProfile(f, m, "a") {
				for _, old := range previous {
					if member.id == old.id {
						t.Fatal("A reused a withdrawn member")
					}
				}
			}
		}
	}
	if scrapedValue(t, f.receiver, `wherry_capacity{pool="pool-a"}`) != float64(replicas+1) {
		t.Fatal("metric capacity changed with readiness")
	}
	if f.advertisedOffers(t).Capacity != int64(replicas+1) {
		t.Fatal("capacity changed with readiness")
	}
}

func holdProfiles(t *testing.T, profiles ...operator.ProfileSettings) {
	t.Helper()
	f := signatureFixture(t, profiles...)
	release := make(chan struct{})
	defer close(release)
	for _, p := range f.plugins {
		p.followHold = release
	}
	var configured atomic.Int32
	factory := f.receiver.factory
	f.receiver.factory = func(pool string, settings operator.ProfileSettings, sum string) (Named, error) {
		configured.Add(1)
		return factory(pool, settings, sum)
	}
	total := 0
	for _, profile := range profiles {
		total += replicasOf(profile)
	}
	taskCount := 0
	for _, profile := range profiles {
		for range replicasOf(profile) {
			taskCount++
			run := fmt.Sprintf("run-%d", taskCount)
			f.submit(t, f.host, func(task *wire.Submission) {
				named(task, run)
				task.Wherry.Placement.Selector = []wire.Requirement{in("sig", profile.Labels["sig"])}
			})
		}
	}
	f.run(t, f.ctx)
	if total == len(profiles) {
		waitFor(t, f, func() bool { return f.live() == total })
		for _, member := range f.members.all() {
			if member.left.IsZero() && len(member.topics) != 1 {
				t.Fatalf("member subscribed to %v; want one signature", member.topics)
			}
		}
	}
	waitFor(t, f, func() bool {
		f.receiver.mu.Lock()
		held := f.receiver.inFlight
		f.receiver.mu.Unlock()
		if held != int64(total) {
			return false
		}
		for _, profile := range profiles {
			_, _, follows, _ := f.plugins[profile.Name].counts()
			if follows != replicasOf(profile) {
				return false
			}
		}
		return true
	})
	if configured.Load() != int32(len(profiles)) {
		t.Fatal("factory called per replica")
	}
	membersByTopic := map[string]int{}
	for _, member := range f.members.all() {
		if member.left.IsZero() && len(member.topics) == 1 {
			membersByTopic[member.topics[0]]++
		}
	}
	for _, profile := range profiles {
		topic := operator.SignatureTopic(testPool, profile.Agent, profile.Labels)
		if membersByTopic[topic] != replicasOf(profile) {
			t.Fatalf("topic %s members=%d, want %d", topic, membersByTopic[topic], replicasOf(profile))
		}
	}
	for _, p := range f.plugins {
		p.mu.Lock()
		ended := p.collects
		p.mu.Unlock()
		if ended != 0 {
			t.Fatal("run ended before overlap barrier")
		}
	}
	if len(f.advertisedOffers(t).Offers) != len(profiles) {
		t.Fatal("offers multiplied by replicas")
	}
	if f.advertisedOffers(t).Capacity != int64(total) {
		t.Fatal("advertisement capacity")
	}
	text := scrape(t, f.receiver)
	if !strings.Contains(text, fmt.Sprintf("wherry_capacity{pool=\"pool-a\"} %d", total)) {
		t.Fatal("metric capacity")
	}
	for _, member := range f.members.all() {
		if member.left.IsZero() && len(member.topics) != 1 {
			t.Fatalf("member subscribed to %v", member.topics)
		}
	}
}

func TestThreeProfilesHoldTogether(t *testing.T) {
	holdProfiles(t, signatureProfile("a", "claude", map[string]string{"sig": "a"}), signatureProfile("b", "claude", map[string]string{"sig": "b"}), signatureProfile("c", "claude", map[string]string{"sig": "c"}))
}
func TestReplicasHoldTogether(t *testing.T) {
	a := signatureProfile("a", "claude", map[string]string{"sig": "a"})
	a.Replicas = 3
	holdProfiles(t, a, signatureProfile("b", "claude", map[string]string{"sig": "b"}))
}
func TestReplicaReadinessIsLocal(t *testing.T) { profileReadiness(t, true, 3) }

func TestDuplicateSignatureUsesClaimingProfile(t *testing.T) {
	f := signatureFixture(t, signatureProfile("a", "claude", map[string]string{"sig": "same"}), signatureProfile("b", "claude", map[string]string{"sig": "same"}))
	release := make(chan struct{})
	releaseHeld := sync.OnceFunc(func() { close(release) })
	defer releaseHeld()
	f.plugins["a"].followHold = release
	f.plugins["b"].followHold = release
	f.ready(t, "b", false)
	f.submit(t, f.host, nil)
	f.run(t, f.ctx)
	waitFor(t, f, func() bool { _, _, n, _ := f.plugins["a"].counts(); return n == 1 })
	f.ready(t, "b", true)
	f.submit(t, f.host, func(task *wire.Submission) { named(task, "run-b") })
	waitFor(t, f, func() bool {
		a, _, _, _ := f.plugins["a"].counts()
		b, _, _, _ := f.plugins["b"].counts()
		return a+b == 2
	})
	aDispatches, _, _, _ := f.plugins["a"].counts()
	bDispatches, _, _, _ := f.plugins["b"].counts()
	if aDispatches != 1 || bDispatches != 1 {
		t.Fatalf("second fresh dispatch used A: A=%d B=%d; want one each", aDispatches, bDispatches)
	}
	f.plugins["b"].mu.Lock()
	run := f.plugins["b"].lastRun
	f.plugins["b"].mu.Unlock()
	if run.Profile != "b" {
		t.Fatalf("second dispatch profile %q, want B", run.Profile)
	}
	statuses := f.eventStatuses(t)
	if !slices.ContainsFunc(statuses, func(s wire.RunStatus) bool {
		return s.Wherry.Run == testRun("run-b") && s.Wherry.Kind == "claimed" && s.Wherry.Attempt == 1
	}) {
		t.Fatal("second task was not a fresh claim")
	}
	releaseHeld()
	result := f.await(t, testRun("run-b"), func(task wire.Task) bool { return task.Status.State == wire.TaskStateCompleted })
	var metadata wire.ResultMetadata
	if err := json.Unmarshal(result.Metadata, &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata.Profile != "b" || metadata.Labels["sig"] != "same" {
		t.Fatalf("B result metadata %+v", metadata)
	}

}
func TestDuplicateSignatureUnreadyClaimantReleases(t *testing.T) {
	f := signatureFixture(t, signatureProfile("a", "claude", nil), signatureProfile("b", "claude", nil))
	f.ready(t, "b", false)
	record := f.submit(t, f.host, nil)
	admitted, ack := f.receiver.admit(f.ctx, record, f.receiver.profiles[1])
	if admitted != nil || ack != kgo.AckRelease {
		t.Fatalf("admitted=%v ack=%v; want nil and AckRelease for B", admitted, ack)
	}
}

func TestRedeliveryAdoptsOtherProfile(t *testing.T) {
	f := signatureFixture(t, signatureProfile("a", "claude", nil), signatureProfile("b", "claude", nil))
	a, b := f.plugins["a"], f.plugins["b"]
	record := f.submit(t, f.host, nil)
	f.publishStatus(t, record, testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
	share, err := f.receiver.newClient(kgo.ConsumeTopics(record.Topic), kgo.ShareGroup(operator.PoolGroup(testPool)), kgo.ShareMaxRecords(1), kgo.ShareMaxRecordsStrict())
	if err != nil {
		t.Fatal(err)
	}
	defer share.Close()
	first := share.PollRecords(f.ctx, 1)
	if first.Err() != nil || len(first.Records()) != 1 {
		t.Fatalf("first delivery: %v", first.Err())
	}
	first.Records()[0].Ack(kgo.AckRelease)
	if err := share.FlushAcks(f.ctx); err != nil {
		t.Fatal(err)
	}
	second := share.PollRecords(f.ctx, 1)
	if second.Err() != nil || len(second.Records()) != 1 {
		t.Fatalf("redelivery: %v", second.Err())
	}
	record = second.Records()[0]
	if attemptOf(record) != 2 {
		t.Fatalf("redelivery attempt %d, want 2", attemptOf(record))
	}

	a.mu.Lock()
	a.created = map[string]bool{testRun("run-a"): true}
	a.mu.Unlock()
	admitted, ack := f.receiver.admit(f.ctx, record, f.receiver.profiles[1])
	if admitted == nil || ack != 0 || admitted.profile.Name != "b" {
		t.Fatalf("B admission: %v %v", admitted, ack)
	}
	a.followHold = make(chan struct{})
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	done := make(chan kgo.AckStatus, 1)
	go func() { done <- f.receiver.execute(ctx, share, record, admitted, attemptOf(record)) }()
	waitFor(t, f, func() bool { _, _, follows, _ := a.counts(); return follows == 1 })
	a.mu.Lock()
	stops, purges := a.stops, a.purges
	a.mu.Unlock()
	if stops != 0 || purges != 0 {
		t.Fatalf("A stops=%d purges=%d during adoption", stops, purges)
	}
	if dispatches, _, _, _ := b.counts(); dispatches != 0 {
		t.Fatal("fresh dispatch through B")
	}
	cancel()
	select {
	case <-done:
	case <-f.ctx.Done():
		t.Fatal("adopted follow did not end")
	}
}

func TestThreeLoopsCloseBeforeRelease(t *testing.T) {
	f := signatureFixture(t, signatureProfile("a", "claude", map[string]string{"sig": "a"}), signatureProfile("b", "claude", map[string]string{"sig": "b"}), signatureProfile("c", "claude", map[string]string{"sig": "c"}))
	f.plugins["a"].endDelay = time.Minute
	f.ready(t, "c", false)
	ctx, stop := context.WithCancel(f.ctx)
	defer stop()
	topicB, topicC := f.profileTopic(t, "b"), f.profileTopic(t, "c")
	f.submit(t, f.host, nil)
	done := f.run(t, ctx)
	idle := f.awaitMember(t, 10*time.Second, []string{topicB})
	for _, member := range f.members.all() {
		if slices.Contains(member.topics, topicC) {
			t.Fatal("unready C opened a client")
		}
	}
	waitFor(t, f, func() bool { _, _, n, _ := f.plugins["a"].counts(); return n == 1 })
	closing, release := make(chan struct{}), make(chan struct{})
	releaseClose := sync.OnceFunc(func() { close(release) })
	defer releaseClose()
	var once sync.Once
	f.cluster.ControlKey(kmsg.ShareGroupHeartbeat.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		if h := request.(*kmsg.ShareGroupHeartbeatRequest); h.MemberID == idle.id && h.MemberEpoch == -1 {
			once.Do(func() { close(closing) })
			f.cluster.SleepControl(func() { <-release })
		}
		return nil, nil, false
	})
	stop()
	select {
	case <-closing:
	case <-f.ctx.Done():
		t.Fatal("idle client never closed")
	}
	time.Sleep(300 * time.Millisecond)
	f.receiver.mu.Lock()
	held := f.receiver.inFlight
	f.receiver.mu.Unlock()
	if held != 1 {
		t.Error("held record released before idle close completed")
	}
	if slices.Contains(f.kinds(t), "released") {
		t.Error("release event preceded idle close")
	}
	releaseClose()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("release did not complete")
	}
	if !slices.Contains(f.kinds(t), "released") {
		t.Fatal("no release after idle close")
	}
}

func TestEffectiveReplicaDefaults(t *testing.T) {
	for _, n := range []int{0, 1} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			p := signatureProfile("a", "claude", map[string]string{"sig": "a"})
			p.Replicas = n
			holdProfiles(t, p)
		})
	}
}

func TestBusyProfileLeavesOtherSignatureAvailable(t *testing.T) {
	f := twoSignatures(t)
	release := make(chan struct{})
	defer close(release)
	f.plugins["a"].followHold = release
	f.plugins["b"].followHold = release
	f.submit(t, f.host, nil)
	f.run(t, f.ctx)
	waitFor(t, f, func() bool { _, _, n, _ := f.plugins["a"].counts(); return n == 1 })
	f.submit(t, f.host, func(task *wire.Submission) { named(task, "run-a2") })
	f.submit(t, f.host, func(task *wire.Submission) {
		named(task, "run-b")
		task.Wherry.Placement.Selector = []wire.Requirement{in("sig", "b")}
	})
	waitFor(t, f, func() bool { _, _, n, _ := f.plugins["b"].counts(); return n == 1 })
	time.Sleep(300 * time.Millisecond)
	if n, _, _, _ := f.plugins["a"].counts(); n != 1 {
		t.Fatalf("busy A dispatched %d times", n)
	}
	if slices.Contains(f.claimedRuns(t), testRun("run-a2")) {
		t.Fatal("second fresh A task did not wait")
	}
}

func TestAdmissionRequiresClaimingProfilesTopic(t *testing.T) {
	f := twoSignatures(t)
	record := f.submit(t, f.host, nil)
	admitted, ack := f.receiver.admit(f.ctx, record, f.receiver.profiles[1])
	if admitted != nil || ack != kgo.AckReject {
		t.Fatalf("topic mismatch admitted=%v ack=%v", admitted, ack)
	}
}

func TestEndingUnreadyAdmissionStillChecksSelector(t *testing.T) {
	f := twoSignatures(t)
	bound := time.Now().Add(time.Second).Unix()
	record := f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Placement.Selector = []wire.Requirement{in("sig", "b")}
		task.Wherry.TaskStartedBy = bound
	})
	record.Topic = f.profileTopic(t, "a")
	f.publishStatus(t, record, testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
	f.ready(t, "a", false)
	passed(bound)
	admitted, ack := f.receiver.admit(f.ctx, record, f.receiver.profiles[0])
	if admitted != nil || ack != kgo.AckReject {
		t.Fatalf("selector mismatch admitted=%v ack=%v", admitted, ack)
	}
	if !slices.ContainsFunc(f.eventStatuses(t), func(status wire.RunStatus) bool {
		return status.Wherry.Kind == "rejected" && status.StatusUpdate.Status.Message.Parts[0].Text == rejectionReasonSelector
	}) {
		t.Fatal("no selector rejection")
	}
}

func TestIdleReplicasKeepClientsAcrossEmptyPolls(t *testing.T) {
	f := twoSignatures(t)
	topicA, topicB := f.profileTopic(t, "a"), f.profileTopic(t, "b")
	f.run(t, f.ctx)
	a := f.awaitMember(t, 10*time.Second, []string{topicA})
	b := f.awaitMember(t, 10*time.Second, []string{topicB})
	time.Sleep(31 * time.Second)
	if !f.shareMember(a.id).left.IsZero() || !f.shareMember(b.id).left.IsZero() || len(f.members.all()) != 2 {
		t.Fatal("empty polls replaced idle members")
	}
}
