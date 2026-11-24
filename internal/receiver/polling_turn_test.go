package receiver

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type claimClientEvents struct {
	mu           sync.Mutex
	next         atomic.Int32
	events       []string
	polledKeys   []string
	closeStarted chan int32
	closeRelease chan struct{}
	closeBlocked atomic.Bool
	// blockClient, when set, is the only client whose close closeStarted holds.
	blockClient atomic.Int32
}

type claimClientHook struct {
	id     int32
	events *claimClientEvents
}

func (h *claimClientHook) OnBrokerWrite(_ kgo.BrokerMetadata, key int16, _ int, _, _ time.Duration, err error) {
	if key == kmsg.ShareFetch.Int16() && err == nil {
		h.events.add(fmt.Sprintf("write:%d", h.id))
	}
}

// OnPollStart records polls; Close polls with a nil context to drain, which is not a poll by the slot.
func (h *claimClientHook) OnPollStart(ctx context.Context) {
	if ctx != nil {
		h.events.add(fmt.Sprintf("poll:%d", h.id))
	}
}

func (h *claimClientHook) OnClientClosed(*kgo.Client) {
	if block := h.events.blockClient.Load(); h.events.closeStarted != nil && (block == 0 || block == h.id) && h.events.closeBlocked.CompareAndSwap(false, true) {
		h.events.closeStarted <- h.id
		<-h.events.closeRelease
	}
	h.events.add(fmt.Sprintf("close:%d", h.id))
}

func (h *claimClientHook) OnFetchRecordUnbuffered(record *kgo.Record, polled bool) {
	if polled {
		h.events.mu.Lock()
		h.events.polledKeys = append(h.events.polledKeys, string(record.Key))
		h.events.mu.Unlock()
	}
}

func (e *claimClientEvents) add(event string) {
	e.mu.Lock()
	e.events = append(e.events, event)
	e.mu.Unlock()
}

func (e *claimClientEvents) snapshot() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.events)
}

func installClaimClientEvents(t *testing.T, events *claimClientEvents) {
	t.Helper()
	old := newClaimClient
	newClaimClient = func(r *Receiver, _ Named, opts ...kgo.Opt) (*kgo.Client, error) {
		hook := &claimClientHook{id: events.next.Add(1), events: events}
		opts = append(opts, kgo.WithHooks(hook))
		return r.newClient(opts...)
	}
	t.Cleanup(func() { newClaimClient = old })
}

func awaitClaimEvent(t *testing.T, f fixture, events *claimClientEvents, ok func([]string) bool) []string {
	t.Helper()
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
		got := events.snapshot()
		if ok(got) {
			return got
		}
	}
	t.Fatalf("claim client event not observed; events=%v; logs:\n%s", events.snapshot(), f.logs.String())
	return nil
}

func firstShareFetchWriter(events []string) string {
	for _, event := range events {
		if len(event) > len("write:") && event[:len("write:")] == "write:" {
			return event[len("write:"):]
		}
	}
	return ""
}

func TestGatedHolderClosesBeforePassingTheProfileTurn(t *testing.T) {
	f := gateFixture(t, resultStream)
	events := &claimClientEvents{closeStarted: make(chan int32, 1), closeRelease: make(chan struct{})}
	installClaimClientEvents(t, events)
	held, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	f.cluster.ControlKey(kmsg.ShareFetch.Int16(), func(kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		once.Do(func() {
			f.cluster.SleepControl(func() {
				close(held)
				<-release
			})
		})
		return nil, nil, false
	})
	f.run(t, f.ctx)
	<-held
	first := firstShareFetchWriter(events.snapshot())
	holder, err := strconv.Atoi(first)
	if err != nil {
		t.Fatalf("no ShareFetch writer before the hold; events=%v", events.snapshot())
	}
	events.blockClient.Store(int32(holder))
	f.setReady(t, false)
	close(release)
	var closing int32
	select {
	case closing = <-events.closeStarted:
	case <-time.After(10 * time.Second):
		t.Fatalf("gated holder did not begin closing; events=%v", events.snapshot())
	}
	if fmt.Sprint(closing) != first {
		close(events.closeRelease)
		t.Fatalf("client %d closed first, want the holder %s", closing, first)
	}
	// With the holder's close held and readiness back, a waiter that had the turn would join and fetch within this.
	f.setReady(t, true)
	time.Sleep(2 * time.Second)
	for _, event := range events.snapshot() {
		if strings.HasPrefix(event, "write:") && event != "write:"+first {
			close(events.closeRelease)
			t.Fatalf("client %s fetched while the gated holder %s was still closing; events=%v", event[6:], first, events.snapshot())
		}
	}
	close(events.closeRelease)
	got := awaitClaimEvent(t, f, events, func(got []string) bool {
		closed := slices.Index(got, "close:"+first)
		return closed >= 0 && slices.ContainsFunc(got[closed+1:], func(event string) bool { return strings.HasPrefix(event, "write:") && event != "write:"+first })
	})
	closed := slices.Index(got, "close:"+first)
	if next := slices.IndexFunc(got, func(event string) bool { return strings.HasPrefix(event, "write:") && event != "write:"+first }); next < closed {
		t.Fatalf("next holder wrote before the gated holder closed; events=%v", got)
	}
}

func TestPollErrorClosesBeforePassingTheProfileTurn(t *testing.T) {
	f := gateFixture(t, resultStream)
	events := &claimClientEvents{closeStarted: make(chan int32, 1), closeRelease: make(chan struct{})}
	installClaimClientEvents(t, events)
	var injected atomic.Bool
	var topicID [16]byte
	var partition int32
	f.cluster.ControlKey(kmsg.ShareFetch.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		req := request.(*kmsg.ShareFetchRequest)
		if len(req.Topics) > 0 && len(req.Topics[0].Partitions) > 0 {
			topicID, partition = req.Topics[0].TopicID, req.Topics[0].Partitions[0].Partition
		}
		if injected.Load() || req.ShareSessionEpoch == 0 || topicID == [16]byte{} {
			return nil, nil, false
		}
		injected.Store(true)
		response := req.ResponseKind().(*kmsg.ShareFetchResponse)
		response.SetVersion(req.Version)
		failed := kmsg.NewShareFetchResponseTopic()
		failed.TopicID = topicID
		failed.Partitions = append(failed.Partitions, kmsg.ShareFetchResponseTopicPartition{
			Partition: partition, ErrorCode: kerr.InvalidRequest.Code,
			CurrentLeader: kmsg.ShareFetchResponseTopicPartitionCurrentLeader{LeaderID: -1, LeaderEpoch: -1},
		})
		response.Topics = append(response.Topics, failed)
		return response, nil, true
	})
	f.run(t, f.ctx)
	var failed int32
	select {
	case failed = <-events.closeStarted:
	case <-time.After(5 * time.Second):
		t.Fatalf("failed holder did not begin closing; events=%v", events.snapshot())
	}
	for _, event := range events.snapshot() {
		if len(event) > 6 && event[:6] == "write:" && event != fmt.Sprintf("write:%d", failed) {
			close(events.closeRelease)
			t.Fatalf("client %s wrote while failed client %d was still closing; events=%v", event[6:], failed, events.snapshot())
		}
	}
	close(events.closeRelease)
	got := awaitClaimEvent(t, f, events, func(got []string) bool {
		closed := slices.Index(got, fmt.Sprintf("close:%d", failed))
		return closed >= 0 && slices.ContainsFunc(got[closed+1:], func(event string) bool {
			return len(event) > 6 && event[:6] == "write:" && event != fmt.Sprintf("write:%d", failed)
		})
	})
	closed := slices.Index(got, fmt.Sprintf("close:%d", failed))
	if next := slices.IndexFunc(got, func(event string) bool {
		return len(event) > 6 && event[:6] == "write:" && event != fmt.Sprintf("write:%d", failed)
	}); next < closed {
		t.Fatalf("next holder wrote before the failed holder closed; events=%v", got)
	}
}

func TestBufferedKeptClientClosesBeforePolling(t *testing.T) {
	f := newFixture(t, resultStream)
	events := &claimClientEvents{}
	installClaimClientEvents(t, events)
	held, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	f.cluster.ControlKey(kmsg.ShareFetch.Int16(), func(kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		once.Do(func() {
			f.cluster.SleepControl(func() {
				close(held)
				<-release
			})
		})
		return nil, nil, false
	})
	f.submit(t, f.host, nil)
	share, err := f.receiver.newSlotClient(f.receiver.profiles[0])
	if err != nil {
		t.Fatal(err)
	}
	pollCtx, cancel := context.WithTimeout(f.ctx, 100*time.Millisecond)
	if records := share.PollRecords(pollCtx, 1).Records(); len(records) != 0 {
		t.Fatalf("cancelled poll returned %d records, want none", len(records))
	}
	cancel()
	<-held
	close(release)
	for end := time.Now().Add(5 * time.Second); share.BufferedFetchRecords() == 0 && time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
	}
	if buffered := share.BufferedFetchRecords(); buffered != 1 {
		t.Fatalf("cancelled poll buffered %d records after its response, want 1", buffered)
	}
	before := len(events.snapshot())
	kept, err := f.receiver.claimNext(f.ctx, f.ctx, f.receiver.profiles[0], newClaimTurn(), func() {}, share)
	if err != nil || kept != nil {
		t.Fatalf("buffered client returned kept=%t err=%v, want closed", kept != nil, err)
	}
	after := events.snapshot()[before:]
	if !slices.Contains(after, "close:1") || slices.Contains(after, "write:1") {
		t.Fatalf("buffered client did not close before another poll: events=%v", after)
	}
	if err := f.receiver.claimOne(f.ctx, f.ctx, f.receiver.profiles[0], newClaimTurn(), func() {}); err != nil {
		t.Fatal(err)
	}
	if attempts := f.claimedAttempts(t, testRun("run-a")); !slices.Equal(attempts, []int64{2}) {
		t.Fatalf("released buffered record claimed at attempts %v, want delivery 2", attempts)
	}
}

func TestAdmissionDoesNotHoldTheProfileTurn(t *testing.T) {
	previous := cancelReadInterval
	cancelReadInterval = time.Hour
	t.Cleanup(func() { cancelReadInterval = previous })
	f := gateFixture(t, resultStream)
	events := &claimClientEvents{}
	installClaimClientEvents(t, events)
	f.run(t, f.ctx)
	if !f.awaitMembers(10*time.Second, func(n int) bool { return n == 2 }) {
		t.Fatalf("claim clients did not join; logs:\n%s", f.logs.String())
	}
	polled := func(name string) bool {
		events.mu.Lock()
		defer events.mu.Unlock()
		return slices.Contains(events.polledKeys, testRun(name))
	}
	await := func(name string) bool {
		// A loaded runner can take seconds to fetch; the bound only matters when the turn is wrongly held.
		for end := time.Now().Add(30 * time.Second); time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
			if polled(name) {
				return true
			}
		}
		return false
	}
	// admit reads the pool's events under this lock first, so holding it keeps run-a's slot inside admit.
	f.receiver.events.mu.Lock()
	locked := true
	defer func() {
		if locked {
			f.receiver.events.mu.Unlock()
		}
	}()
	f.submit(t, f.host, nil)
	if !await("run-a") {
		t.Fatalf("run-a was not returned from a poll; events=%v", events.snapshot())
	}
	f.submit(t, f.host, func(task *wire.Submission) { named(task, "run-b") })
	if !await("run-b") {
		t.Fatalf("run-b was not returned from a poll while run-a's admission was held; events=%v", events.snapshot())
	}
	f.receiver.events.mu.Unlock()
	locked = false
}

func TestReadinessFlapPassesTheProfileTurn(t *testing.T) {
	config := testConfig(testReceiver)
	config.Profiles[0].Replicas = 3
	f := newFixtureWith(t, resultStream, testTerms(), config, map[string]*fakePlugin{"nomad": {stream: resultStream}})
	f.run(t, f.ctx)
	if !f.awaitMembers(10*time.Second, func(n int) bool { return n == 3 }) {
		t.Fatalf("claim clients did not join; logs:\n%s", f.logs.String())
	}
	f.setReady(t, false)
	if !f.awaitMembers(3*time.Second, func(n int) bool { return n == 0 }) {
		t.Fatalf("%d idle clients after readiness was withdrawn, want 0", f.polling())
	}
	// The holder passes the turn after its client closes; readiness stays withdrawn until that exit has finished.
	time.Sleep(time.Second)
	f.setReady(t, true)
	if !f.awaitMembers(5*time.Second, func(n int) bool { return n == 3 }) {
		t.Fatalf("claim clients did not rejoin; logs:\n%s", f.logs.String())
	}
	f.submit(t, f.host, nil)
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
		if _, _, follows, _ := f.plugin.counts(); follows > 0 {
			break
		}
	}
	if _, _, follows, _ := f.plugin.counts(); follows == 0 {
		t.Fatalf("later task was not claimed after the readiness flap; logs:\n%s", f.logs.String())
	}
	f.await(t, testRun("run-a"), completed)
	// The run publishes its terminal before settling its record and releasing the hold.
	waitFor(t, f, func() bool { return f.receiver.held() == 0 })
}

func TestWaiterRechecksReadinessAfterTakingTheProfileTurn(t *testing.T) {
	f := newFixture(t, resultStream)
	events := &claimClientEvents{}
	installClaimClientEvents(t, events)
	taken, resume := make(chan struct{}), make(chan struct{})
	old := claimTurnTaken
	claimTurnTaken = func() {
		close(taken)
		<-resume
	}
	t.Cleanup(func() { claimTurnTaken = old })
	turn := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- f.receiver.claimOne(ctx, ctx, f.receiver.profiles[0], turn, func() {}) }()
	for f.live() != 1 {
		if f.ctx.Err() != nil {
			t.Fatal("waiting claim client did not join")
		}
		time.Sleep(10 * time.Millisecond)
	}
	turn <- struct{}{}
	<-taken
	f.setReady(t, false)
	close(resume)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waiter returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiter did not return")
	}
	if acted := slices.DeleteFunc(events.snapshot(), func(event string) bool {
		return !strings.HasPrefix(event, "write:") && !strings.HasPrefix(event, "poll:")
	}); len(acted) != 0 {
		t.Fatalf("waiter polled after readiness was withdrawn: %v", acted)
	}
}
