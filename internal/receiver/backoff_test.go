package receiver

import (
	"context"
	"crypto/ed25519"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// startFailed puts receiver's start failure, signed with key, for the task in record at attempt 1 on the pool's events.
func (f fixture) startFailed(t *testing.T, record *kgo.Record, receiver string, key ed25519.PrivateKey) {
	t.Helper()
	task := f.parse(t, record)
	failure := &wire.StartFailure{Wherry: f.receiver.binding(task, 1), Profile: "nomad", Agent: "claude", Class: "dispatch", TaskHash: wire.TaskHash(record.Value)}
	failure.Wherry.Receiver = receiver
	f.produce(t, admin.PoolEventsTopic(testPool), string(record.Key), must(t)(wire.SignRecord(failure, key)))
}

func TestRedeliveryAfterOwnStartFailureReleasesWithoutDispatch(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.submit(t, f.host, nil)
	f.startFailed(t, record, testReceiver, f.receiver.signing)
	before := time.Now()
	if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckRelease {
		t.Fatalf("admitted=%v ack=%v, want release; logs:\n%s", admitted != nil, ack, f.logs.String())
	}
	if dispatches, _, _, _ := f.plugin.counts(); dispatches != 0 || len(f.eventStatuses(t)) != 0 {
		t.Fatalf("dispatches %d, statuses %v: want neither", dispatches, f.kinds(t))
	}
	if !strings.Contains(f.logs.String(), "run run-a: already failed here; releasing for another member") {
		t.Fatalf("logs:\n%s", f.logs.String())
	}
	f.receiver.mu.Lock()
	until := f.receiver.backoffUntil
	f.receiver.mu.Unlock()
	if lock := testTerms().LockDuration(); until.Before(before.Add(lock)) || until.After(time.Now().Add(lock)) {
		t.Fatalf("back-off until %s, want one lock duration after the release", until)
	}
}

func TestPeerStartFailureDoesNotHoldTheTaskBack(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.submit(t, f.host, nil)
	f.startFailed(t, record, "receiver-b", f.addPeer(t, "receiver-b", testPool))
	f.admitted(t, record)
	if strings.Contains(f.logs.String(), "already failed here") {
		t.Fatalf("logs:\n%s", f.logs.String())
	}
}

func TestRedeliveryAtTheBudgetAfterOwnStartFailureEnds(t *testing.T) {
	terms := testTerms()
	terms.DeliveryLimit = 2
	f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream}})
	record := f.submit(t, f.host, nil)
	f.startFailed(t, record, testReceiver, f.receiver.signing)
	if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckReject {
		t.Fatalf("admitted=%v ack=%v, want reject; logs:\n%s", admitted != nil, ack, f.logs.String())
	}
	if state, reason := terminalReason(t, f, "run-a"); state != wire.TaskStateFailed || reason != "delivery budget exhausted" {
		t.Fatalf("terminal %s %q", state, reason)
	}
	if dispatches, _, _, _ := f.plugin.counts(); dispatches != 0 {
		t.Fatalf("dispatches %d", dispatches)
	}
}

// shareFetches records the time and session epoch of every share fetch the cluster receives from now on.
func (f fixture) shareFetches(t *testing.T) func() ([]time.Time, []int32) {
	t.Helper()
	var mu sync.Mutex
	var times []time.Time
	var epochs []int32
	f.cluster.ControlKey(kmsg.ShareFetch.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		mu.Lock()
		defer mu.Unlock()
		times, epochs = append(times, time.Now()), append(epochs, request.(*kmsg.ShareFetchRequest).ShareSessionEpoch)
		return nil, nil, false
	})
	return func() ([]time.Time, []int32) {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(times), slices.Clone(epochs)
	}
}

// releasedForAnother claims with slots claim loops, which the test's end stops, under a lock duration of lock until the
// receiver releases a task it failed to start for another member, and returns when it did.
func (f fixture) releasedForAnother(t *testing.T, slots int, lock time.Duration) time.Time {
	t.Helper()
	shortLock(f, lock)
	record := f.submit(t, f.host, nil)
	f.startFailed(t, record, testReceiver, f.receiver.signing)
	ctx, stop := context.WithCancel(f.ctx)
	var wg sync.WaitGroup
	for range slots {
		wg.Go(func() { f.receiver.claimLoop(ctx, ctx, func() {}) })
	}
	t.Cleanup(func() { stop(); wg.Wait() })
	for !strings.Contains(f.logs.String(), "already failed here") {
		if f.ctx.Err() != nil {
			t.Fatalf("never released; logs:\n%s", f.logs.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
	return time.Now()
}

func TestReleaseForAnotherMemberWaitsOneLockDuration(t *testing.T) {
	f := newFixture(t, resultStream)
	lock := 500 * time.Millisecond
	fetches := f.shareFetches(t)
	released := f.releasedForAnother(t, 1, lock)
	for {
		times, epochs := fetches()
		for i, at := range times {
			if at.After(released) && epochs[i] == 0 {
				if waited := at.Sub(released); waited < lock-50*time.Millisecond {
					t.Fatalf("next share session opened %s after the release, want one lock duration", waited)
				}
				return
			}
		}
		if f.ctx.Err() != nil {
			t.Fatal("no share session opened after the back-off")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestReleaseForAnotherMemberHoldsEverySlot(t *testing.T) {
	f := newFixture(t, resultStream)
	lock := 2 * time.Second
	fetches := f.shareFetches(t)
	released := f.releasedForAnother(t, 2, lock)
	time.Sleep(lock - 100*time.Millisecond)
	// A slot that fetched again would have been handed the released task, and released it again.
	if releases := strings.Count(f.logs.String(), "already failed here"); releases != 1 {
		t.Fatalf("released %d times inside the back-off, want once; logs:\n%s", releases, f.logs.String())
	}
	times, epochs := fetches()
	for i, at := range times {
		// The idle slot's poll ends within its 100 ms check; a session close carries epoch -1.
		if at.After(released.Add(300*time.Millisecond)) && at.Before(released.Add(lock-100*time.Millisecond)) && epochs[i] >= 0 {
			t.Fatalf("a slot fetched %s after the release, inside the back-off", at.Sub(released))
		}
	}
}

// A start failure for another tenant's run under the same run ID is authorised on the pool's events, but binds to that
// run by its task hash and never holds this run back.
func TestStartFailureOfAnotherTenantsRunDoesNotHoldTheTaskBack(t *testing.T) {
	f := newFixture(t, resultStream)
	keyB := f.tenantB(t)
	record := f.submit(t, f.host, nil)
	other := f.submit(t, keyB, asTenantB)
	task := testSubmission("run-a")
	asTenantB(task)
	failure := &wire.StartFailure{Wherry: f.receiver.binding(task, 1), Profile: "nomad", Agent: "claude", Class: "dispatch", TaskHash: wire.TaskHash(other.Value)}
	f.produce(t, admin.PoolEventsTopic(testPool), "run-a", must(t)(wire.SignRecord(failure, f.receiver.signing)))
	events, err := f.receiver.readEvents(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if failures := events.startFailures["run-a"]; len(failures) != 1 || failures[0].Wherry.Tenant != "tenant-b" {
		t.Fatalf("start failures %+v, want tenant-b's", failures)
	}
	f.admitted(t, record)
	if strings.Contains(f.logs.String(), "already failed here") {
		t.Fatalf("logs:\n%s", f.logs.String())
	}
}

// A status or start failure on the pool's events counts only from a live member of the pool, naming the pool, for a
// tenant entitled to it; a refused record is not kept for the cancel check either.
func TestPoolEventsRefuseUnboundStatusesAndStartFailures(t *testing.T) {
	for name, mutate := range map[string]func(*wire.ReceiverBinding){
		"another pool":       func(binding *wire.ReceiverBinding) { binding.Pool = "pool-b" },
		"unentitled tenant":  func(binding *wire.ReceiverBinding) { binding.Tenant = "tenant-b" },
		"control (accepted)": nil,
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			record := f.submit(t, f.host, nil)
			task := f.parse(t, record)
			status := f.receiver.status(task, wire.TaskHash(record.Value), 1, "claimed", taskStatus(wire.TaskStateWorking, ""), nil)
			failure := &wire.StartFailure{Wherry: f.receiver.binding(task, 1), Profile: "nomad", Agent: "claude", Class: "dispatch", TaskHash: wire.TaskHash(record.Value)}
			if mutate != nil {
				mutate(&status.Wherry.ReceiverBinding)
				mutate(&failure.Wherry)
			}
			for _, value := range []wire.Record{status, failure} {
				f.produce(t, admin.PoolEventsTopic(testPool), "run-a", must(t)(wire.SignRecord(value, f.receiver.signing)))
			}
			events, err := f.receiver.readEvents(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			counted := len(events.statuses["run-a"]) + len(events.startFailures["run-a"])
			if want := map[bool]int{true: 2, false: 0}[mutate == nil]; counted != want || len(events.others["run-a"]) != 0 {
				t.Fatalf("%d records counted and %d kept as others, want %d counted", counted, len(events.others["run-a"]), want)
			}
		})
	}
}

// A status from a member whose membership ended counts for nothing on the pool's events.
func TestPoolEventsRefuseAnEndedMember(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.submit(t, f.host, nil)
	f.publishStatus(t, record, testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
	f.startFailed(t, record, testReceiver, f.receiver.signing)
	f.receiver.mu.Lock()
	f.receiver.state.Ended[admin.ReceiverPrincipal(testReceiver)] = true
	f.receiver.mu.Unlock()
	events, err := f.receiver.readEvents(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(events.statuses["run-a"]) != 0 || len(events.startFailures["run-a"]) != 0 {
		t.Fatalf("an ended member's records counted: %+v", events)
	}
}

// Each status and start failure on the pool's events is verified once, and any other record not at all.
func TestPoolEventsVerifyEachRecordOnce(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.submit(t, f.host, nil)
	f.publishStatus(t, record, testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
	f.startFailed(t, record, testReceiver, f.receiver.signing)
	f.publishCancel(t, "run-a", 1, f.host, nil)
	verifications := 0
	parseHistory = func(appended time.Time) func([]byte, signed.Keys, wire.Record) error {
		parse := wire.ParseHistory(appended)
		return func(encoded []byte, keys signed.Keys, value wire.Record) error {
			verifications++
			return parse(encoded, keys, value)
		}
	}
	t.Cleanup(func() { parseHistory = wire.ParseHistory })
	events, err := f.receiver.readEvents(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(events.statuses["run-a"]) != 1 || len(events.startFailures["run-a"]) != 1 || len(events.others["run-a"]) != 1 {
		t.Fatalf("events %+v, want one status, one start failure and the cancel", events)
	}
	if verifications != 2 {
		t.Fatalf("%d verifications, want one per status and start failure", verifications)
	}
}
