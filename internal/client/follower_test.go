package client

import (
	"context"
	"errors"
	"regexp"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
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

// pastDeadline moves the run deadline of host's local record of runID into the past and returns the record.
func pastDeadline(t *testing.T, host *Host, runID string) *runRecord {
	t.Helper()
	local, err := host.localRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	local.DeadlineMS = time.Now().Add(-time.Minute).UnixMilli()
	if err := host.writeRun(*local); err != nil {
		t.Fatal(err)
	}
	return local
}

// committedEnd returns the tenant feed's committed end.
func committedEnd(t *testing.T, f *fixture) int64 {
	t.Helper()
	ends, err := kadm.NewClient(f.service.Admin).ListCommittedOffsets(f.ctx, admin.TenantTopic("tenant-a"))
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		t.Fatal(err)
	}
	var end int64
	ends.Each(func(listed kadm.ListedOffset) { end = listed.Offset })
	return end
}

// A follower starts from the host's local run records and fetches no feed record to choose its start.
func TestTheFollowerStartsWithoutReadingTheFeed(t *testing.T) {
	f := newFixture(t)
	_, task := f.admitted(t)
	pastDeadline(t, f.host, task.submission.Wherry.Run)
	hook := &feedHook{}
	host := f.hooked(t, hook)
	follower := following(t, f, host)
	if fetched := hook.fetched.Load(); fetched != 0 {
		t.Fatalf("the follower's start fetched %d feed records; want none", fetched)
	}
	if end := committedEnd(t, f); follower.start != end {
		t.Fatalf("follower starts at %d; want the committed end %d, as no run's deadline is ahead", follower.start, end)
	}
}

// A follower started while the host reads would otherwise build its reader in the same spare capacity of the host's
// options.
func TestTheFollowerLeavesTheHostsOptionsUnwritten(t *testing.T) {
	f := newFixture(t)
	opts := append(make([]kgo.Opt, 0, len(f.host.kafka.Opts())+3), f.host.kafka.Opts()...)
	kafka, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kafka.Close)
	host, err := NewHost(kafka, f.host.key, admin.TenantPrincipal("tenant-a", "host-a"), f.stateDir, f.host.credentials, f.host.logger)
	if err != nil {
		t.Fatal(err)
	}
	following(t, f, host)
	if slices.ContainsFunc(opts[len(opts):cap(opts)], func(opt kgo.Opt) bool { return opt != nil }) {
		t.Fatal("the follower wrote its reader's options into the host's")
	}
}

// A follower whose runs begin below the feed's log start starts at the log start, so a view returns without a new
// record.
func TestAFollowerBelowTheLogStartStartsAtIt(t *testing.T) {
	f := newFixture(t)
	_, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	end := committedEnd(t, f)
	var deleted kadm.Offsets
	deleted.AddOffset(admin.TenantTopic("tenant-a"), 0, end, -1)
	responses, err := kadm.NewClient(f.service.Admin).DeleteRecords(f.ctx, deleted)
	if err == nil {
		err = responses.Error()
	}
	if err != nil {
		t.Fatal(err)
	}
	local, err := f.host.localRun(runID)
	if err != nil || local.FeedOffset >= end {
		t.Fatalf("the run's offset is %d, %v; want it below the log start %d", local.FeedOffset, err, end)
	}
	follower := following(t, f, f.host)
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	if _, _, err := f.host.readFeed(ctx, follower, end); err != nil {
		t.Fatalf("read the feed from the log start: %v", err)
	}
	if follower.start != end {
		t.Fatalf("follower starts at %d; want the log start %d", follower.start, end)
	}
}

// A view while the follower cannot fetch reads the feed once and returns that read's error.
func TestAViewWhileTheFollowerCannotFetchReadsOnce(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	following(t, f, f.host)
	denied := kadm.NewACLs().Deny("User:" + admin.TenantPrincipal("tenant-a", "host-a")).Topics(admin.TenantTopic("tenant-a")).Operations(kadm.OpRead).ResourcePatternType(kadm.ACLPatternLiteral)
	created, err := kadm.NewClient(f.service.Admin).CreateACLs(f.ctx, denied)
	if err == nil {
		for _, result := range created {
			err = errors.Join(err, result.Err)
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	receiver.feed(t, f.ctx, runID, receiver.status(t, task, 1, "claimed-1", "claimed", wire.TaskStateWorking, nil))
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	if _, err := f.host.history(ctx, runID); !errors.Is(err, kerr.TopicAuthorizationFailed) {
		t.Fatalf("history %v; want the one-shot read's authorization error", err)
	}
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
	local := pastDeadline(t, f.host, runID)
	follower := following(t, f, f.host)
	if follower.start <= local.FeedOffset {
		t.Fatalf("follower starts at %d, the ended run's offset is %d; want the follower past the run", follower.start, local.FeedOffset)
	}
	r, err := f.host.history(f.ctx, runID)
	if err != nil || r.status().Status.State != wire.TaskStateCompleted || r.skipped != 0 {
		t.Fatalf("history %v; want COMPLETED with nothing skipped", err)
	}
	if awaited, err := f.host.Await(f.ctx, runID, 5*time.Second); err != nil || awaited.Status.State != wire.TaskStateCompleted {
		t.Fatalf("await %#v, %v; want COMPLETED", awaited, err)
	}
}

// Await and the Answer it runs fetch the records after the run's feed offset, however long the feed's history.
func TestAnsweringFetchesOnlyTheRecordsAfterTheRunsOffset(t *testing.T) {
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	backlog := make([]*kgo.Record, 2000)
	for i := range backlog {
		backlog[i] = &kgo.Record{Topic: admin.TenantTopic("tenant-a"), Key: []byte("run-other"), Value: []byte("unreadable")}
	}
	if err := receiver.kafka.ProduceSync(f.ctx, backlog...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	_, task := f.admitted(t)
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
	fetched := hook.fetched.Load()
	receiver.feed(t, f.ctx, runID, receiver.result(t, task, 1, wire.TaskStateCompleted, nil), receiver.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateCompleted, nil))
	if awaited := <-done; awaited.Status.State != wire.TaskStateCompleted {
		t.Fatalf("await %#v; want COMPLETED", awaited)
	}
	if fetched >= 50 {
		t.Fatalf("answering fetched %d feed records; want the few after the run's offset, not the backlog of 2000", fetched)
	}
}
