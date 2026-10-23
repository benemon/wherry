package client

import (
	"context"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// feedHook counts a host's open clients that consume the tenant feed and the feed records its clients fetch, and holds
// each fetched feed record back while slow is set.
type feedHook struct {
	consumers atomic.Int64
	fetched   atomic.Int64
	slow      atomic.Bool
}

func consumesFeed(client *kgo.Client) bool {
	topic := admin.TenantTopic("tenant-a")
	partitions, _ := client.OptValue(kgo.ConsumePartitions).(map[string]map[int32]kgo.Offset)
	_, consumed := client.OptValue(kgo.ConsumeTopics).(map[string]*regexp.Regexp)[topic]
	return partitions[topic] != nil || consumed
}

func (h *feedHook) OnNewClient(client *kgo.Client) {
	if consumesFeed(client) {
		h.consumers.Add(1)
	}
}

func (h *feedHook) OnClientClosed(client *kgo.Client) {
	if consumesFeed(client) {
		h.consumers.Add(-1)
	}
}

func (h *feedHook) OnFetchRecordBuffered(record *kgo.Record) {
	if record.Topic == admin.TenantTopic("tenant-a") {
		h.fetched.Add(1)
		if h.slow.Load() {
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// hooked returns the fixture's host on a Kafka client that runs hook.
func (f *fixture) hooked(t *testing.T, hook *feedHook) *Host {
	t.Helper()
	kafka, err := kgo.NewClient(append(f.host.kafka.Opts(), kgo.WithHooks(hook))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kafka.Close)
	host, err := NewHost(kafka, f.host.key, admin.TenantPrincipal("tenant-a", "host-a"), f.stateDir, f.host.credentials, f.host.logger)
	if err != nil {
		t.Fatal(err)
	}
	return host
}

// following starts host's follower, which runs until the test ends.
func following(t *testing.T, f *fixture, host *Host) *follower {
	t.Helper()
	ctx, stop := context.WithCancel(f.ctx)
	t.Cleanup(stop)
	follower, err := host.follow(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return follower
}

// With a follower running, a view of a run fetches only the records committed since the last view.
func TestAViewFetchesOnlyTheRecordsSinceTheLast(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	hook := &feedHook{}
	host := f.hooked(t, hook)
	following(t, f, host)
	if _, err := host.history(f.ctx, runID); err != nil {
		t.Fatal(err)
	}
	before := hook.fetched.Load()
	for range 5 {
		if err := receiver.kafka.ProduceSync(f.ctx, &kgo.Record{Topic: admin.TenantTopic("tenant-a"), Key: []byte("run-other"), Value: []byte("unreadable")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	r, err := host.history(f.ctx, runID)
	if err != nil || r.status().Status.State != wire.TaskStateWorking {
		t.Fatalf("history %v; want WORKING", err)
	}
	if fetched := hook.fetched.Load() - before; fetched != 5 {
		t.Fatalf("the second view fetched %d feed records; want the 5 committed since the first", fetched)
	}
}

// A claim committed before a view of its run is in that view, however far the follower lags.
func TestAViewHoldsAClaimCommittedBeforeIt(t *testing.T) {
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	submitted := f.submit(t, defaultSelector())
	task := receiver.claim(t, f.ctx, submitted.RunID)[0]
	receiver.feed(t, f.ctx, submitted.RunID, receiver.echo(t, task))
	hook := &feedHook{}
	host := f.hooked(t, hook)
	following(t, f, host)
	hook.slow.Store(true)
	receiver.feed(t, f.ctx, submitted.RunID, receiver.status(t, task, 1, "claimed-1", "claimed", wire.TaskStateWorking, nil))
	r, err := host.history(f.ctx, submitted.RunID)
	if err != nil || r.status().Status.State != wire.TaskStateWorking {
		t.Fatalf("history %v; want WORKING from the claim committed before it", err)
	}
}

// Await and the Answer it runs read the tenant feed through one follower.
func TestAwaitAndItsAnswerShareOneFollower(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	hook := &feedHook{}
	host := f.hooked(t, hook)
	done := make(chan AwaitResult, 1)
	go func() {
		awaited, err := host.Await(f.ctx, runID, 20*time.Second)
		if err != nil {
			t.Errorf("await: %v", err)
		}
		done <- awaited
	}()
	asked := newRequest(t, receiver.binding(task, 1), "model")
	receiver.feed(t, f.ctx, runID, receiver.sign(t, &asked.record))
	f.waitSeals(t, "host-a", 1)
	consumers := hook.consumers.Load()
	receiver.feed(t, f.ctx, runID, receiver.result(t, task, 1, wire.TaskStateCompleted, nil), receiver.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateCompleted, nil))
	if awaited := <-done; awaited.Status.State != wire.TaskStateCompleted {
		t.Fatalf("await %#v; want COMPLETED", awaited)
	}
	if consumers != 1 {
		t.Fatalf("%d clients consumed the tenant feed while await waited; want its one follower", consumers)
	}
}

// A run whose feed offset lies below the follower's start, as one that ended before the follower started, is read once
// from its offset.
func TestARunBelowTheFollowersStartIsReadOnce(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	receiver.feed(t, f.ctx, runID, receiver.result(t, task, 1, wire.TaskStateCompleted, nil), receiver.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateCompleted, nil))
	follower := following(t, f, f.host)
	local, err := f.host.localRun(runID)
	if err != nil || follower.start <= local.FeedOffset {
		t.Fatalf("follower starts at %d, the ended run's offset is %d, %v; want the follower past the run", follower.start, local.FeedOffset, err)
	}
	r, err := f.host.history(f.ctx, runID)
	if err != nil || r.status().Status.State != wire.TaskStateCompleted || r.skipped != 0 {
		t.Fatalf("history %v; want COMPLETED with nothing skipped", err)
	}
	if awaited, err := f.host.Await(f.ctx, runID, 5*time.Second); err != nil || awaited.Status.State != wire.TaskStateCompleted {
		t.Fatalf("await %#v, %v; want COMPLETED", awaited, err)
	}
}
