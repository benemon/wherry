package receiver

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// advertiseAs puts receiver's advertisement, signed with key, offering agent and dated age ago on wherry.receivers.
func (f fixture) advertiseAs(t *testing.T, receiver string, key ed25519.PrivateKey, agent string, age time.Duration) {
	t.Helper()
	ad := &wire.Advertisement{Receiver: receiver, Pool: testPool, Offers: []wire.Offer{{Profile: "nomad", Agent: agent}}, Capacity: 1, AtMS: time.Now().Add(-age).UnixMilli()}
	f.produce(t, admin.ReceiversTopic, receiver, must(t)(wire.SignRecord(ad, key)))
}

// livePeer provisions another member of the pool that advertises an offer of claude now, so a start failure here
// returns the task to the pool rather than exhausting it.
func (f fixture) livePeer(t *testing.T, id string) ed25519.PrivateKey {
	t.Helper()
	key := f.addPeer(t, id, testPool)
	f.advertiseAs(t, id, key, "claude", 0)
	return key
}

// startFailed puts receiver's start failure, signed with key, for the task in record at attempt 1 on the pool's events;
// mutate edits it first.
func (f fixture) startFailed(t *testing.T, record *kgo.Record, receiver string, key ed25519.PrivateKey, mutate func(*wire.StartFailure)) {
	t.Helper()
	task := f.parse(t, record)
	failure := &wire.StartFailure{Wherry: f.receiver.binding(task, 1), Profile: "nomad", Agent: "claude", Class: "dispatch", TaskHash: wire.TaskHash(record.Value)}
	failure.Wherry.Receiver = receiver
	if mutate != nil {
		mutate(failure)
	}
	f.produce(t, admin.PoolEventsTopic(testPool), string(record.Key), must(t)(wire.SignRecord(failure, key)))
}

const exhaustedPrefix = "no receiver in pool pool-a could start agent claude"

func TestPoolExhaustedWhenEveryEligibleMemberFailed(t *testing.T) {
	f := newFixture(t, resultStream)
	peer := f.livePeer(t, "receiver-b")
	record := f.submit(t, f.host, nil)
	f.startFailed(t, record, "receiver-b", peer, nil)
	f.answer(t, credentialName)
	f.plugin.outcome = pluginsdk.OutcomeNothingCreated
	if status := f.execute(t, record, 1); status != kgo.AckReject {
		t.Fatalf("status %v, want reject; logs:\n%s", status, f.logs.String())
	}
	if state, reason := terminalReason(t, f, "run-a"); state != wire.TaskStateFailed || !strings.HasPrefix(reason, exhaustedPrefix+" after 1 attempts; last: ") {
		t.Fatalf("terminal %s %q", state, reason)
	}
	if failures := f.startFailures(t); len(failures) != 1 {
		t.Fatalf("own start failures %d, want 1", len(failures))
	}
}

func TestExhaustedRunIsPurgedBeforeItsTerminal(t *testing.T) {
	f := newFixture(t, resultStream)
	peer := f.livePeer(t, "receiver-b")
	record := f.submit(t, f.host, nil)
	f.startFailed(t, record, "receiver-b", peer, nil)
	f.answer(t, credentialName)
	var mu sync.Mutex
	var atPurge []int
	f.plugin.outcome = pluginsdk.OutcomeNothingCreated
	f.plugin.purgeCheck = func() {
		mu.Lock()
		defer mu.Unlock()
		atPurge = append(atPurge, f.terminals(t, admin.PoolEventsTopic(testPool)))
	}
	if status := f.execute(t, record, 1); status != kgo.AckReject {
		t.Fatalf("status %v, want reject; logs:\n%s", status, f.logs.String())
	}
	f.plugin.mu.Lock()
	calls := f.plugin.calls
	f.plugin.mu.Unlock()
	if !slices.Equal(calls, []string{"purge", "release"}) || !slices.Equal(atPurge, []int{0}) || f.terminals(t, admin.PoolEventsTopic(testPool)) != 1 {
		t.Fatalf("calls %v, terminals at purge %v: want one purge before the one terminal", calls, atPurge)
	}
}

func TestExhaustionReasonNamesAttemptsAndCause(t *testing.T) {
	f := newFixture(t, resultStream)
	peer := f.livePeer(t, "receiver-b")
	record := f.submit(t, f.host, nil)
	f.startFailed(t, record, "receiver-b", peer, nil)
	f.answer(t, credentialName)
	f.plugin.outcome = pluginsdk.OutcomeNothingCreated
	if status := f.execute(t, record, 2); status != kgo.AckReject {
		t.Fatalf("status %v, want reject; logs:\n%s", status, f.logs.String())
	}
	want := "no receiver in pool pool-a could start agent claude after 2 attempts; last: profile nomad created no job"
	if state, reason := terminalReason(t, f, "run-a"); state != wire.TaskStateFailed || reason != want {
		t.Fatalf("terminal %s %q, want %q", state, reason, want)
	}
}

// hideOwnFailureFromNextRead makes the next listing of the pool events' end offset stop before this receiver's start
// failure, as a read-committed read just after its commit can.
func (f fixture) hideOwnFailureFromNextRead(t *testing.T) {
	t.Helper()
	topic := admin.PoolEventsTopic(testPool)
	end := int64(-1)
	for _, record := range f.records(t, topic) {
		var failure wire.StartFailure
		if wire.ParseRecord(record.Value, f.keys(), &failure) == nil && failure.Wherry.Receiver == testReceiver {
			end = record.Offset
		}
	}
	if end < 0 {
		t.Fatal("no own start failure committed before the purge")
	}
	f.cluster.ControlKey(kmsg.ListOffsets.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		list := request.(*kmsg.ListOffsetsRequest)
		if len(list.Topics) != 1 || list.Topics[0].Topic != topic || slices.ContainsFunc(list.Topics[0].Partitions, func(p kmsg.ListOffsetsRequestTopicPartition) bool { return p.Timestamp != -1 }) {
			f.cluster.KeepControl()
			return nil, nil, false
		}
		response := list.ResponseKind().(*kmsg.ListOffsetsResponse)
		listed := kmsg.NewListOffsetsResponseTopic()
		listed.Topic = topic
		for _, partition := range list.Topics[0].Partitions {
			offset := kmsg.NewListOffsetsResponseTopicPartition()
			offset.Partition, offset.Offset = partition.Partition, end
			listed.Partitions = append(listed.Partitions, offset)
		}
		response.Topics = append(response.Topics, listed)
		return response, nil, true
	})
}

func TestOwnStartFailureCountsWhenTheReadTrailsItsCommit(t *testing.T) {
	f := newFixture(t, resultStream)
	peer := f.livePeer(t, "receiver-b")
	record := f.submit(t, f.host, nil)
	f.startFailed(t, record, "receiver-b", peer, nil)
	f.answer(t, credentialName)
	f.plugin.outcome = pluginsdk.OutcomeNothingCreated
	f.plugin.purgeCheck = func() { f.hideOwnFailureFromNextRead(t) }
	if status := f.execute(t, record, 2); status != kgo.AckReject {
		t.Fatalf("status %v, want reject; logs:\n%s", status, f.logs.String())
	}
	want := "no receiver in pool pool-a could start agent claude after 2 attempts; last: profile nomad created no job"
	if state, reason := terminalReason(t, f, "run-a"); state != wire.TaskStateFailed || reason != want {
		t.Fatalf("terminal %s %q, want %q", state, reason, want)
	}
}

func TestPoolExhaustionCountsLiveMembersOfferingTheAgent(t *testing.T) {
	for _, test := range []struct {
		name   string
		age    time.Duration
		agent  string
		failed func(*wire.StartFailure)
		ack    kgo.AckStatus
	}{
		{"peer advertisement past liveness", 121 * time.Second, "claude", nil, kgo.AckReject},
		{"peer advertisement within liveness", 119 * time.Second, "claude", nil, kgo.AckRelease},
		{"peer offers another agent", 0, "codex", nil, kgo.AckReject},
		{"peer failed another task", 0, "claude", func(failure *wire.StartFailure) { failure.TaskHash = strings.Repeat("b", 64) }, kgo.AckRelease},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			peer := f.addPeer(t, "receiver-b", testPool)
			f.advertiseAs(t, "receiver-b", peer, test.agent, test.age)
			record := f.submit(t, f.host, nil)
			if test.failed != nil {
				f.startFailed(t, record, "receiver-b", peer, test.failed)
			}
			f.answer(t, credentialName)
			f.plugin.outcome = pluginsdk.OutcomeNothingCreated
			if status := f.execute(t, record, 1); status != test.ack {
				t.Fatalf("status %v, want %v; logs:\n%s", status, test.ack, f.logs.String())
			}
			if test.ack == kgo.AckReject {
				if state, reason := terminalReason(t, f, "run-a"); state != wire.TaskStateFailed || !strings.HasPrefix(reason, exhaustedPrefix) {
					t.Fatalf("terminal %s %q", state, reason)
				}
			}
		})
	}
}

func TestStartFailuresCountFromLiveMembersOfThePoolOnly(t *testing.T) {
	_, stranger, _ := ed25519.GenerateKey(rand.Reader)
	for _, test := range []struct {
		name    string
		key     func(peer ed25519.PrivateKey) ed25519.PrivateKey
		mutate  func(*wire.StartFailure)
		end     bool
		counted int
	}{
		{"member", func(peer ed25519.PrivateKey) ed25519.PrivateKey { return peer }, nil, false, 1},
		{"another pool", func(peer ed25519.PrivateKey) ed25519.PrivateKey { return peer }, func(failure *wire.StartFailure) { failure.Wherry.Pool = "pool-b" }, false, 0},
		{"key of no member", func(ed25519.PrivateKey) ed25519.PrivateKey { return stranger }, nil, false, 0},
		{"ended member", func(peer ed25519.PrivateKey) ed25519.PrivateKey { return peer }, nil, true, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			peer := f.addPeer(t, "receiver-b", testPool)
			record := f.submit(t, f.host, nil)
			f.startFailed(t, record, "receiver-b", test.key(peer), test.mutate)
			if test.end {
				// The peer's key stays folded, so only the membership check can refuse its record.
				f.receiver.mu.Lock()
				ended := maps.Clone(f.receiver.state.Ended)
				if ended == nil {
					ended = map[string]bool{}
				}
				ended[admin.ReceiverPrincipal("receiver-b")] = true
				f.receiver.state.Ended = ended
				f.receiver.mu.Unlock()
			}
			events, err := f.receiver.readEvents(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			if got := len(events.startFailures["run-a"]); got != test.counted {
				t.Fatalf("start failures counted %d, want %d", got, test.counted)
			}
		})
	}
}

func TestStartFailureUnderRotatedPeerKeyCounts(t *testing.T) {
	for _, test := range []struct {
		name string
		// retired signs with the key the peer rotated away from, after the receiver refolded the rotation; otherwise
		// the peer signs with its new key before the receiver refolds.
		retired bool
	}{{"retired key", true}, {"key rotated since the last refresh", false}} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			previous := f.addPeer(t, "receiver-b", testPool)
			record := f.submit(t, f.host, nil)
			_, next, _ := ed25519.GenerateKey(rand.Reader)
			if test.retired {
				f.startFailed(t, record, "receiver-b", previous, nil)
			}
			f.produce(t, admin.ReceiverTopic("receiver-b"), "receiver-b", must(t)(signed.RotateKey("receiver-b", previous, next)))
			if test.retired {
				if err := f.receiver.refresh(f.ctx); err != nil {
					t.Fatal(err)
				}
			} else {
				f.startFailed(t, record, "receiver-b", next, nil)
			}
			events, err := f.receiver.readEvents(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			if got := len(events.startFailures["run-a"]); got != 1 {
				t.Fatalf("start failures counted %d, want 1; logs:\n%s", got, f.logs.String())
			}
		})
	}
}

func TestRedeliveryAfterOwnStartFailureReleasesWithoutDispatch(t *testing.T) {
	f := newFixture(t, resultStream)
	f.livePeer(t, "receiver-b")
	record := f.submit(t, f.host, nil)
	f.startFailed(t, record, testReceiver, f.receiver.signing, nil)
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

func TestOwnStartFailureEndsTheRunAtExhaustionOrTheBudget(t *testing.T) {
	for _, test := range []struct {
		name   string
		limit  int32
		peer   bool
		reason string
	}{
		{"exhausted below the budget", 10, false, exhaustedPrefix},
		{"exhausted at the budget", 2, false, exhaustedPrefix},
		{"not exhausted at the budget", 2, true, "delivery budget exhausted"},
	} {
		t.Run(test.name, func(t *testing.T) {
			terms := testTerms()
			terms.DeliveryLimit = test.limit
			f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream}})
			if test.peer {
				f.livePeer(t, "receiver-b")
			}
			record := f.submit(t, f.host, nil)
			f.startFailed(t, record, testReceiver, f.receiver.signing, nil)
			if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckReject {
				t.Fatalf("admitted=%v ack=%v, want reject; logs:\n%s", admitted != nil, ack, f.logs.String())
			}
			if state, reason := terminalReason(t, f, "run-a"); state != wire.TaskStateFailed || reason != test.reason {
				t.Fatalf("terminal %s %q, want %q", state, reason, test.reason)
			}
			if dispatches, _, _, _ := f.plugin.counts(); dispatches != 0 {
				t.Fatalf("dispatches %d", dispatches)
			}
		})
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
	f.livePeer(t, "receiver-b")
	shortLock(f, lock)
	record := f.submit(t, f.host, nil)
	f.startFailed(t, record, testReceiver, f.receiver.signing, nil)
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

func TestOwnStartFailureCountsThisReceiverWhateverItAdvertises(t *testing.T) {
	// With no ready profile the receiver advertises no offers, so only its start failure makes it eligible.
	f := newFixtureWith(t, resultStream, testTerms(), testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream, unready: true}})
	record := f.submit(t, f.host, nil)
	f.startFailed(t, record, testReceiver, f.receiver.signing, nil)
	if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckReject {
		t.Fatalf("admitted=%v ack=%v, want reject; logs:\n%s", admitted != nil, ack, f.logs.String())
	}
	if state, reason := terminalReason(t, f, "run-a"); state != wire.TaskStateFailed || reason != exhaustedPrefix {
		t.Fatalf("terminal %s %q", state, reason)
	}
}

func TestNoEligibleMemberIsNotExhaustion(t *testing.T) {
	if exhausted(map[string]bool{}, nil, strings.Repeat("a", 64)) {
		t.Fatal("an empty eligible set counted as exhausted")
	}
}
