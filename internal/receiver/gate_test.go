package receiver

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"log"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// gateFixture is a fixture whose receiver has two slots, so one slot's release could reach the other's poll.
func gateFixture(t *testing.T, stream []string) fixture {
	t.Helper()
	config := testConfig(testReceiver)
	config.Capacity = 2
	return newFixtureWith(t, stream, testTerms(), config, map[string]*fakePlugin{"nomad": {stream: stream}})
}

// setReady sets the fake plugin's readiness and advertises, so the receiver's offers follow it at once.
func (f fixture) setReady(t *testing.T, ready bool) {
	t.Helper()
	f.plugin.mu.Lock()
	f.plugin.unready = !ready
	f.plugin.mu.Unlock()
	if err := f.receiver.advertise(f.ctx); err != nil {
		t.Fatal(err)
	}
}

// shareJoins counts the share group heartbeats that join the pool's share group from now on.
func (f fixture) shareJoins(t *testing.T) func() int64 {
	t.Helper()
	var joins atomic.Int64
	f.cluster.ControlKey(kmsg.ShareGroupHeartbeat.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		if heartbeat := request.(*kmsg.ShareGroupHeartbeatRequest); heartbeat.GroupID == operator.PoolGroup(testPool) && heartbeat.MemberEpoch == 0 {
			joins.Add(1)
		}
		return nil, nil, false
	})
	return joins.Load
}

// renewals records the time of every renewal the cluster receives from now on; a held run renews in share
// acknowledgements, never in a share fetch, since it no longer polls.
func (f fixture) renewals(t *testing.T) func() []time.Time {
	t.Helper()
	var mu sync.Mutex
	var times []time.Time
	renews := func(types []int8) {
		if slices.Contains(types, int8(kgo.AckRenew)) {
			mu.Lock()
			times = append(times, time.Now())
			mu.Unlock()
		}
	}
	f.cluster.ControlKey(kmsg.ShareAcknowledge.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		for _, topic := range request.(*kmsg.ShareAcknowledgeRequest).Topics {
			for _, partition := range topic.Partitions {
				for _, batch := range partition.AcknowledgementBatches {
					renews(batch.AcknowledgeTypes)
				}
			}
		}
		return nil, nil, false
	})
	return func() []time.Time {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(times)
	}
}

// mostSessions samples r.sessions() every 5 ms for d and returns the highest count seen.
func (f fixture) mostSessions(d time.Duration) int {
	most := 0
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
		most = max(most, f.receiver.sessions())
	}
	return most
}

// awaitSessions waits up to d for r.sessions() to satisfy ok and reports whether it did.
func (f fixture) awaitSessions(d time.Duration, ok func(int) bool) bool {
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
		if ok(f.receiver.sessions()) {
			return true
		}
	}
	return false
}

// awaitFollow waits for the fake plugin's first Follow, the run's start.
func (f fixture) awaitFollow(t *testing.T) {
	t.Helper()
	for {
		if _, _, follows, _ := f.plugin.counts(); follows == 1 {
			return
		}
		if f.ctx.Err() != nil {
			t.Fatalf("run never started; logs:\n%s", f.logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// (a) A receiver whose offers are empty from the start opens no share client: no session, no join, no share fetch.
func TestNoReadyOfferOpensNoPoll(t *testing.T) {
	f := gateFixture(t, resultStream)
	f.plugin.mu.Lock()
	f.plugin.unready = true
	f.plugin.mu.Unlock()
	fetches, joins := f.shareFetches(t), f.shareJoins(t)
	f.run(t, f.ctx)
	// The slots start only after Run's first advertisement; without a session no task is ever acquired.
	if most := f.mostSessions(3 * time.Second); most != 0 {
		t.Errorf("%d share sessions open while no offer is ready, want none", most)
	}
	if times, _ := fetches(); len(times) != 0 {
		t.Errorf("%d share fetches while no offer is ready, want none", len(times))
	}
	if joined := joins(); joined != 0 {
		t.Errorf("%d share group joins while no offer is ready, want none", joined)
	}
}

// (b) A slot blocked inside a poll when the offers empty ends the poll and closes its client, and no share fetch follows.
func TestOffersEmptyingEndsABlockedPoll(t *testing.T) {
	f := gateFixture(t, resultStream)
	fetches := f.shareFetches(t)
	f.run(t, f.ctx)
	if !f.awaitSessions(10*time.Second, func(n int) bool { return n == 2 }) {
		t.Fatalf("both slots never polled; logs:\n%s", f.logs.String())
	}
	for times, _ := fetches(); len(times) == 0; times, _ = fetches() {
		if f.ctx.Err() != nil {
			t.Fatal("no share fetch while both slots poll")
		}
		time.Sleep(10 * time.Millisecond)
	}
	f.setReady(t, false)
	// The 100 ms watcher, then the client's close, which waits out a pending share fetch of up to its 1 s FetchMaxWait.
	if !f.awaitSessions(3*time.Second, func(n int) bool { return n == 0 }) {
		t.Errorf("poll cancelled: %d share sessions still open 3 s after the offers emptied, want 0", f.receiver.sessions())
	}
	closed := time.Now()
	time.Sleep(3 * time.Second)
	times, epochs := fetches()
	for i, at := range times {
		// A session close carries epoch -1.
		if at.After(closed) && epochs[i] >= 0 {
			t.Errorf("no share fetch follows: a share fetch %s after the sessions closed", at.Sub(closed))
			break
		}
	}
}

// (d) At the delivery budget, a task fetched with no ready offer ends in the budget terminal and is rejected.
func TestNoOfferAtTheBudgetEnds(t *testing.T) {
	terms := testTerms()
	terms.DeliveryLimit = 2
	f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream}})
	f.setReady(t, false)
	if admitted, ack := f.receiver.admit(f.ctx, f.submit(t, f.host, nil)); admitted != nil || ack != kgo.AckReject {
		t.Fatalf("admitted=%v ack=%v, want reject; logs:\n%s", admitted != nil, ack, f.logs.String())
	}
	if state, reason := terminalReason(t, f, testRun("run-a")); state != wire.TaskStateFailed || reason != budgetReason {
		t.Fatalf("terminal %s %q, want FAILED %q", state, reason, budgetReason)
	}
	if dispatches, _, _, _ := f.plugin.counts(); dispatches != 0 {
		t.Fatalf("dispatches %d", dispatches)
	}
}

// (f) A held run is not gated: while the offers are empty it keeps renewing and completes, and its cancel is honoured.
func TestHeldRunIsNotGatedByNoOffer(t *testing.T) {
	t.Run("renews and completes", func(t *testing.T) {
		f := newFixture(t, resultStream)
		f.plugin.endDelay = 3 * time.Second
		renewals := f.renewals(t)
		f.submit(t, f.host, nil)
		f.run(t, f.ctx)
		f.awaitFollow(t)
		f.setReady(t, false)
		gated := time.Now()
		f.await(t, testRun("run-a"), func(task wire.Task) bool { return task.Status.State == wire.TaskStateCompleted })
		completed := time.Now()
		// One renewal every 200 ms renew interval through the run's last 3 s; two leave room for a slow scheduler.
		var renewed int
		for _, at := range renewals() {
			if at.After(gated) && at.Before(completed) {
				renewed++
			}
		}
		if renewed < 2 {
			t.Fatalf("%d renewals while the offers were empty, want the claim renewed; logs:\n%s", renewed, f.logs.String())
		}
		if _, delivered := f.pollPool(t, testRun("run-a"), 3*time.Second); delivered {
			t.Fatal("the completed run's task is delivered again, want it acknowledged")
		}
	})
	t.Run("cancel", func(t *testing.T) {
		f := newFixture(t, noResultStream)
		f.plugin.endDelay = 30 * time.Second
		f.submit(t, f.host, nil)
		f.run(t, f.ctx)
		f.awaitFollow(t)
		f.setReady(t, false)
		f.publishCancel(t, testRun("run-a"), 1, f.host, nil)
		// A 200 ms cancel read, then the run's 1 s state check, then its stop.
		ctx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
		defer cancel()
		for !slices.ContainsFunc(f.feedStatuses(t), func(status wire.RunStatus) bool { return status.Wherry.Kind == "terminal" }) {
			if ctx.Err() != nil {
				t.Fatalf("the held run kept going 10 s after its cancel; logs:\n%s", f.logs.String())
			}
			time.Sleep(50 * time.Millisecond)
		}
		if state, reason := terminalReason(t, f, testRun("run-a")); state != wire.TaskStateCanceled || reason != "canceled by user" {
			t.Fatalf("terminal %s %q, want CANCELED", state, reason)
		}
		f.plugin.mu.Lock()
		stops := f.plugin.stops
		f.plugin.mu.Unlock()
		if stops != 1 {
			t.Fatalf("stops %d, want the job stopped once", stops)
		}
	})
}

// (g) No offer and a start failure's back-off each hold the gate alone: clearing one leaves it closed, clearing both
// reopens it.
func TestGateReasonsHoldIndependently(t *testing.T) {
	f := gateFixture(t, resultStream)
	backoff := func(d time.Duration) time.Time {
		until := time.Now().Add(d)
		f.receiver.mu.Lock()
		f.receiver.backoffUntil = until
		f.receiver.mu.Unlock()
		return until
	}
	f.setReady(t, false)
	until := backoff(2 * time.Second)
	f.run(t, f.ctx)
	if most := f.mostSessions(1500 * time.Millisecond); most != 0 {
		t.Fatalf("no offer and back-off: %d share sessions, want none", most)
	}
	time.Sleep(time.Until(until))
	if most := f.mostSessions(1500 * time.Millisecond); most != 0 {
		t.Fatalf("back-off cleared, no offer held: %d share sessions, want none", most)
	}
	until = backoff(3 * time.Second)
	f.setReady(t, true)
	if most := f.mostSessions(1500 * time.Millisecond); most != 0 {
		t.Fatalf("offer ready, back-off held: %d share sessions, want none", most)
	}
	time.Sleep(time.Until(until))
	// A slot asleep on the gate re-checks within its 1 s sleep.
	if !f.awaitSessions(time.Second+500*time.Millisecond, func(n int) bool { return n > 0 }) {
		t.Fatalf("both reasons cleared: no share session opened; logs:\n%s", f.logs.String())
	}
}

// (h) A Ready that returns false without an error is logged on every advertisement.
func TestFalseReadyIsLogged(t *testing.T) {
	f := newFixture(t, resultStream)
	f.plugin.mu.Lock()
	f.plugin.unready = true
	f.plugin.mu.Unlock()
	if err := f.receiver.advertise(f.ctx); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(strings.Split(f.logs.String(), "\n"), "profile nomad readiness: not ready") {
		t.Fatalf("logs:\n%s", f.logs.String())
	}
}

// (e) TestNoReadyOfferOpensNoPollOnRealBroker runs a receiver with no ready offer in a broker pool; kfake does not model
// share-group acquisition. While gated the receiver holds no partition of its pool's share group, and so acquires no
// task; once ready it reopens and claims the task queued meanwhile at its first delivery.
func TestNoReadyOfferOpensNoPollOnRealBroker(t *testing.T) {
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" {
		t.Skip("WHERRY_KAFKA_TEST_BROKER not set")
	}
	shortIntervals(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	b := newBrokerPool(t, ctx, address)
	b.configure(t, ctx, 2)
	_, hostKey, _ := ed25519.GenerateKey(rand.Reader)
	registration, err := signed.RegisterKey(signed.HostOwner(b.tenant, b.host), hostKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.kafka.ProduceSync(ctx, &kgo.Record{Topic: operator.HostTopic(b.tenant, b.host), Value: registration}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	base, err := kgo.NewClient(b.opts...)
	if err != nil {
		t.Fatal(err)
	}
	_, signing, _ := ed25519.GenerateKey(rand.Reader)
	keys := signed.Keys{b.receiver: signed.KeyState{Current: signing.Public().(ed25519.PublicKey)}}
	logs := &syncBuffer{}
	plugin := &fakePlugin{stream: resultStream, unready: true}
	r := &Receiver{id: b.receiver, base: base, reader: operator.NewReader(base), signing: signing, stateDir: t.TempDir(), logger: log.New(logs, "", 0), metrics: newMetrics(prometheus.NewRegistry()),
		factory: func(_ string, settings operator.ProfileSettings, _ string) (Named, error) {
			return Named{Name: settings.Name, Agent: settings.Agent, Plugin: plugin}, nil
		}}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- r.Run(runCtx) }()
	defer func() { stop(); <-done }()
	for !strings.Contains(logs.String(), "readiness: not ready") {
		if ctx.Err() != nil {
			t.Fatalf("the receiver never advertised; logs:\n%s", logs.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	// The group's share.auto.offset.reset is earliest, so a task queued before any fetch is still delivered.
	submission := testSubmission("run-gated")
	submission.Wherry.Tenant, submission.Wherry.Host, submission.Wherry.Pool = b.tenant, b.host, b.pool
	run := wire.RunID(b.tenant, b.host, submission.Wherry.RunNonce)
	submission.Wherry.Run, submission.Message.TaskID, submission.Message.ContextID = run, run, run
	b.seal(t, ctx, submission)
	value, err := wire.SignRecord(submission, hostKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.kafka.ProduceSync(ctx, &kgo.Record{Topic: operator.PoolTasksTopic(b.pool), Key: []byte(run), Value: value}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	admin := kadm.NewClient(b.kafka)
	for end := time.Now().Add(3 * time.Second); time.Now().Before(end); time.Sleep(200 * time.Millisecond) {
		groups, err := admin.DescribeShareGroups(ctx, operator.PoolGroup(b.pool))
		if group := groups[operator.PoolGroup(b.pool)]; err == nil && len(group.AssignedPartitions()) > 0 {
			t.Fatalf("the pool's share group has assigned partitions while no offer is ready; logs:\n%s", logs.String())
		}
		if r.sessions() != 0 {
			t.Fatalf("a share session opened while no offer is ready; logs:\n%s", logs.String())
		}
	}
	plugin.mu.Lock()
	plugin.unready = false
	plugin.mu.Unlock()
	// One 200 ms advertise interval to see the offer, then a slot's 1 s gate sleep; 500 ms more for the scheduler.
	ready := time.Now()
	for r.sessions() == 0 {
		if waited := time.Since(ready); waited > time.Second+advertiseInterval+500*time.Millisecond {
			t.Fatalf("no share session %s after the offer became ready; logs:\n%s", waited, logs.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	for {
		if _, _, follows, _ := plugin.counts(); follows == 1 {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("run never started; logs:\n%s", logs.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	records, err := readTopic(ctx, b.kafka, operator.PoolEventsTopic(b.pool))
	if err != nil {
		t.Fatal(err)
	}
	var claims []int64
	for _, record := range records {
		var status wire.RunStatus
		if wire.ParseRecord(record.Value, keys, &status) == nil && status.Wherry.Run == run && status.Wherry.Kind == "claimed" {
			claims = append(claims, status.Wherry.Attempt)
		}
	}
	if !slices.Equal(claims, []int64{1}) {
		t.Fatalf("claimed at attempts %v, want once at attempt 1: the gated receiver acquired the task nothing; logs:\n%s", claims, logs.String())
	}
}
