package receiver

import (
	"context"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// shareAck is one record's acknowledgement as the cluster received it, in a share fetch or a share acknowledgement.
type shareAck struct {
	member string
	offset int64
	kind   kgo.AckStatus
}

// shareAcks records, from now on, every acknowledgement of a record of the pool's share group.
func (f fixture) shareAcks(t *testing.T) func() []shareAck {
	t.Helper()
	var mu sync.Mutex
	var acks []shareAck
	add := func(group, member *string, first, last int64, kinds []int8) {
		if group == nil || *group != operator.PoolGroup(testPool) || member == nil {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		for offset := first; offset <= last; offset++ {
			kind := kinds[0]
			if len(kinds) > 1 {
				kind = kinds[offset-first]
			}
			acks = append(acks, shareAck{*member, offset, kgo.AckStatus(kind)})
		}
	}
	f.cluster.ControlKey(kmsg.ShareFetch.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		fetch := request.(*kmsg.ShareFetchRequest)
		for _, topic := range fetch.Topics {
			for _, partition := range topic.Partitions {
				for _, batch := range partition.AcknowledgementBatches {
					add(fetch.GroupID, fetch.MemberID, batch.FirstOffset, batch.LastOffset, batch.AcknowledgeTypes)
				}
			}
		}
		return nil, nil, false
	})
	f.cluster.ControlKey(kmsg.ShareAcknowledge.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		ack := request.(*kmsg.ShareAcknowledgeRequest)
		for _, topic := range ack.Topics {
			for _, partition := range topic.Partitions {
				for _, batch := range partition.AcknowledgementBatches {
					add(ack.GroupID, ack.MemberID, batch.FirstOffset, batch.LastOffset, batch.AcknowledgeTypes)
				}
			}
		}
		return nil, nil, false
	})
	return func() []shareAck {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(acks)
	}
}

// ackedBy is the members that acknowledged offset with kind, once for each acknowledgement.
func ackedBy(acks []shareAck, offset int64, kind kgo.AckStatus) []string {
	var members []string
	for _, ack := range acks {
		if ack.offset == offset && ack.kind == kind {
			members = append(members, ack.member)
		}
	}
	return members
}

// claimedAttempts is the attempts at which run was claimed, in log order.
func (f fixture) claimedAttempts(t *testing.T, run string) []int64 {
	t.Helper()
	var attempts []int64
	for _, status := range f.eventStatuses(t) {
		if status.Wherry.Run == run && status.Wherry.Kind == "claimed" {
			attempts = append(attempts, status.Wherry.Attempt)
		}
	}
	return attempts
}

// liveMember is the one member of the pool's share group that has joined and not left.
func (f fixture) liveMember(t *testing.T) shareMember {
	t.Helper()
	var live []shareMember
	for _, member := range f.members.all() {
		if member.left.IsZero() {
			live = append(live, member)
		}
	}
	if len(live) != 1 {
		t.Fatalf("%d live members, want one; logs:\n%s", len(live), f.logs.String())
	}
	return live[0]
}

// awaitLeft waits up to d for member to leave and reports whether it did.
func (f fixture) awaitLeft(member shareMember, d time.Duration) bool {
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
		if !f.shareMember(member.id).left.IsZero() {
			return true
		}
	}
	return false
}

func completed(task wire.Task) bool { return task.Status.State == wire.TaskStateCompleted }

// A slot that accepts a claim takes the next record on the same member, without joining again, well inside one
// heartbeat interval of the record's arrival.
func TestKeptClientTakesTheNextRecord(t *testing.T) {
	f := newFixture(t, resultStream)
	acks := f.shareAcks(t)
	first := f.submit(t, f.host, nil)
	f.run(t, f.ctx)
	waitFor(t, f, func() bool { return len(ackedBy(acks(), first.Offset, kgo.AckAccept)) == 1 })
	joins := f.shareJoins(t)
	submitted := time.Now()
	second := f.submit(t, f.host, func(task *wire.Submission) { named(task, "run-b") })
	waitFor(t, f, func() bool { n, _, _, _ := f.plugin.counts(); return n == 2 })
	received := time.Since(submitted)
	waitFor(t, f, func() bool { return len(ackedBy(acks(), second.Offset, kgo.AckAccept)) == 1 })
	a, b := ackedBy(acks(), first.Offset, kgo.AckAccept), ackedBy(acks(), second.Offset, kgo.AckAccept)
	if a[0] != b[0] || joins() != 0 {
		t.Fatalf("accepted by %s then %s with %d joins between, want one member and no join; logs:\n%s", a[0], b[0], joins(), f.logs.String())
	}
	// kfake's share heartbeat interval is 5 s, as a broker's default.
	if received > 2*time.Second {
		t.Fatalf("the second record was dispatched %s after it was produced, want well under one heartbeat", received)
	}
}

// failReads makes the cluster fail every offset listing of topic from now on, so every read of it fails.
func (f fixture) failReads(t *testing.T, topic string) {
	t.Helper()
	f.cluster.ControlKey(kmsg.ListOffsets.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		req := request.(*kmsg.ListOffsetsRequest)
		if !slices.ContainsFunc(req.Topics, func(listed kmsg.ListOffsetsRequestTopic) bool { return listed.Topic == topic }) {
			return nil, nil, false
		}
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

// A claim that releases its record closes its client: each delivery is released by a member of its own, so the
// delivery count grows by at most one per join.
func TestReleasedClaimClosesItsClient(t *testing.T) {
	f := newFixture(t, resultStream)
	acks := f.shareAcks(t)
	f.run(t, f.ctx)
	f.awaitMember(t, 10*time.Second, []string{f.tasksTopic()})
	f.failReads(t, operator.PoolEventsTopic(testPool))
	joins := f.shareJoins(t)
	record := f.submit(t, f.host, nil)
	waitFor(t, f, func() bool { return len(ackedBy(acks(), record.Offset, kgo.AckRelease)) >= 2 })
	time.Sleep(2 * time.Second)
	released := ackedBy(acks(), record.Offset, kgo.AckRelease)
	for _, member := range released {
		if n := strings.Count(strings.Join(released, "\n")+"\n", member+"\n"); n > 1 {
			t.Fatalf("member %s released the record %d times, want once before it closes; logs:\n%s", member, n, f.logs.String())
		}
	}
	// The member open at submission takes the first delivery.
	if joined := joins(); int64(len(released)) > joined+1 {
		t.Fatalf("%d releases over %d joins, want at most one more release than joins", len(released), joined)
	}
}

// A claim lost under Run leaves its record unacknowledged and closes its client, which releases it: the record is
// delivered again rather than accepted by the next poll.
func TestLostClaimClosesItsClient(t *testing.T) {
	f := newFixture(t, resultStream)
	acks := f.shareAcks(t)
	failRenewal(t, f, 1, "invalid")
	record := f.submit(t, f.host, nil)
	f.run(t, f.ctx)
	waitFor(t, f, func() bool { return strings.Contains(f.logs.String(), ": claim lost:") })
	waitFor(t, f, func() bool { return slices.Contains(f.claimedAttempts(t, testRun("run-a")), 2) })
	accepted := ackedBy(acks(), record.Offset, kgo.AckAccept)
	lost := f.members.all()[0].id
	if slices.Contains(accepted, lost) {
		t.Fatalf("the member that lost the claim accepted the record; logs:\n%s", f.logs.String())
	}
}

// A claim that ends with no acknowledgement while claiming goes on, here fenced at its claimed commit, closes its client:
// the record is delivered again, and claimed at the second delivery.
func TestUnacknowledgedClaimClosesItsClient(t *testing.T) {
	f := newFixture(t, resultStream)
	var commits atomic.Int32
	f.cluster.ControlKey(kmsg.EndTxn.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		req := request.(*kmsg.EndTxnRequest)
		if req.TransactionalID != operator.TransactionalPrefix(testPool)+testRun("run-a") || !req.Commit || commits.Add(1) != 1 {
			return nil, nil, false
		}
		response := req.ResponseKind().(*kmsg.EndTxnResponse)
		response.SetVersion(req.Version)
		response.ErrorCode = kerr.ProducerFenced.Code
		return response, nil, true
	})
	f.submit(t, f.host, nil)
	f.run(t, f.ctx)
	waitFor(t, f, func() bool { return len(f.claimedAttempts(t, testRun("run-a"))) > 0 })
	f.await(t, testRun("run-a"), completed)
	if !strings.Contains(f.logs.String(), "run "+testRun("run-a")+": fenced by a newer holder") {
		t.Fatalf("the first claim was not fenced; logs:\n%s", f.logs.String())
	}
	if attempts := f.claimedAttempts(t, testRun("run-a")); !slices.Equal(attempts, []int64{2}) {
		t.Fatalf("claimed at attempts %v, want once, at the second delivery; logs:\n%s", attempts, f.logs.String())
	}
}

// A final acknowledgement held past the flush bound closes the client.
func TestTimedOutFinalFlushClosesItsClient(t *testing.T) {
	f := newFixture(t, resultStream)
	held, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	releaseHeld := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseHeld)
	f.cluster.ControlKey(kmsg.ShareAcknowledge.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		for _, topic := range request.(*kmsg.ShareAcknowledgeRequest).Topics {
			for _, partition := range topic.Partitions {
				for _, batch := range partition.AcknowledgementBatches {
					if slices.Contains(batch.AcknowledgeTypes, int8(kgo.AckAccept)) {
						once.Do(func() { close(held) })
						f.cluster.SleepControl(func() { <-release })
					}
				}
			}
		}
		return nil, nil, false
	})
	f.submit(t, f.host, nil)
	f.run(t, f.ctx)
	select {
	case <-held:
	case <-f.ctx.Done():
		t.Fatalf("the final acknowledgement was never sent; logs:\n%s", f.logs.String())
	}
	member := f.liveMember(t)
	time.Sleep(shutdownTimeout + 500*time.Millisecond)
	releaseHeld()
	if !f.awaitLeft(member, 3*time.Second) {
		t.Fatalf("the member whose final flush timed out is still open; logs:\n%s", f.logs.String())
	}
}

// A final acknowledgement the broker answers with an error closes the client, though FlushAcks returns nil.
func TestRefusedFinalAcknowledgementClosesItsClient(t *testing.T) {
	f := newFixture(t, resultStream)
	refused := f.refuseAck(t, kgo.AckAccept)
	f.submit(t, f.host, nil)
	f.run(t, f.ctx)
	select {
	case <-refused:
	case <-f.ctx.Done():
		t.Fatalf("the final acknowledgement was never sent; logs:\n%s", f.logs.String())
	}
	if member := f.liveMember(t); !f.awaitLeft(member, 3*time.Second) {
		t.Fatalf("the member whose final acknowledgement was refused is still open; logs:\n%s", f.logs.String())
	}
}

// strayFetch widens the window after a poll drains a buffered fetch, when the client may grant one more fetch: on the
// first record polled it produces next and waits, so that fetch acquires next while the slot runs the first.
type strayFetch struct {
	f        fixture
	next     *kgo.Record
	once     sync.Once
	buffered atomic.Bool
}

func (s *strayFetch) OnFetchRecordUnbuffered(_ *kgo.Record, polled bool) {
	if polled {
		s.once.Do(func() {
			_ = s.f.service.Operator.ProduceSync(s.f.ctx, s.next).FirstErr()
			time.Sleep(300 * time.Millisecond)
		})
	}
}

func (s *strayFetch) OnFetchRecordBuffered(record *kgo.Record) {
	if string(record.Key) == string(s.next.Key) {
		s.buffered.Store(true)
	}
}

// A record buffered while the slot is not polling closes the client after its claim: the record is released by the
// close, not claimed at that delivery.
func TestStrayBufferedRecordClosesItsClient(t *testing.T) {
	f := newFixture(t, resultStream)
	f.plugin.endDelay = time.Second
	submission := testSubmission("run-b")
	f.seal(t, submission)
	stray := &strayFetch{f: f, next: &kgo.Record{Topic: f.tasksTopic(), Key: []byte(submission.Wherry.Run), Value: must(t)(wire.SignRecord(submission, f.host))}}
	old := newClaimClient
	newClaimClient = func(r *Receiver, _ Named, opts ...kgo.Opt) (*kgo.Client, error) {
		return r.newClient(append(opts, kgo.WithHooks(stray))...)
	}
	t.Cleanup(func() { newClaimClient = old })
	f.submit(t, f.host, nil)
	f.run(t, f.ctx)
	f.await(t, testRun("run-a"), completed)
	if !stray.buffered.Load() {
		t.Fatalf("no record was buffered outside a poll; logs:\n%s", f.logs.String())
	}
	f.await(t, testRun("run-b"), completed)
	if attempts := f.claimedAttempts(t, testRun("run-b")); !slices.Equal(attempts, []int64{2}) {
		t.Fatalf("the stray record was claimed at attempts %v, want once, at the delivery after its release; logs:\n%s", attempts, f.logs.String())
	}
}

// Two tasks back to back on one slot's kept client, each held past the lock with renewals, both complete at their first
// delivery with no renewal failure: each claim's renewals are answered to it.
func TestBackToBackClaimsRenewOnTheKeptClient(t *testing.T) {
	f := newFixture(t, resultStream)
	f.plugin.endDelay = 2500 * time.Millisecond
	shortLock(f, time.Second)
	renewals := f.renewals(t)
	ctx, stop := context.WithCancel(f.ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.receiver.claimLoop(ctx, ctx, f.receiver.profiles[0], func() {})
	}()
	t.Cleanup(func() { stop(); <-done })
	f.submit(t, f.host, nil)
	f.await(t, testRun("run-a"), completed)
	joins := f.shareJoins(t)
	since := time.Now()
	f.submit(t, f.host, func(task *wire.Submission) { named(task, "run-b") })
	f.await(t, testRun("run-b"), completed)
	for _, run := range []string{testRun("run-a"), testRun("run-b")} {
		if attempts := f.claimedAttempts(t, run); !slices.Equal(attempts, []int64{1}) {
			t.Fatalf("%s claimed at attempts %v, want once at the first delivery; logs:\n%s", run, attempts, f.logs.String())
		}
	}
	if got := f.renewalFailures(t); got != 0 {
		t.Fatalf("renewal failures %v, want 0; logs:\n%s", got, f.logs.String())
	}
	if joins() != 0 {
		t.Fatalf("%d joins between the claims, want the second on the kept client", joins())
	}
	late := 0
	for _, at := range renewals() {
		if at.After(since) {
			late++
		}
	}
	if late < 3 {
		t.Fatalf("%d renewals of the second claim, want it renewed past the 1 s lock", late)
	}
}

// closeCount counts the claim clients created and closed.
type closeCount struct{ created, closed atomic.Int32 }

func (c *closeCount) OnClientClosed(*kgo.Client) { c.closed.Add(1) }

// No claim client is open once Run returns, whether claiming ended with a client kept, a pool suspended or the receiver
// halted.
func TestNoClaimClientOutlivesRun(t *testing.T) {
	for _, end := range []string{"stop", "suspension", "plugin exit"} {
		t.Run(end, func(t *testing.T) {
			f := gateFixture(t, resultStream)
			f.plugin.exited = make(chan struct{})
			count := &closeCount{}
			old := newClaimClient
			newClaimClient = func(r *Receiver, _ Named, opts ...kgo.Opt) (*kgo.Client, error) {
				count.created.Add(1)
				return r.newClient(append(opts, kgo.WithHooks(count))...)
			}
			t.Cleanup(func() { newClaimClient = old })
			ctx, stop := context.WithCancel(f.ctx)
			defer stop()
			f.submit(t, f.host, nil)
			done := f.run(t, ctx)
			f.await(t, testRun("run-a"), completed)
			switch end {
			case "stop":
				stop()
			case "suspension":
				f.suspend(t)
				f.submit(t, f.host, func(task *wire.Submission) { named(task, "run-b") })
				time.Sleep(2 * time.Second)
				stop()
			case "plugin exit":
				close(f.plugin.exited)
			}
			select {
			case <-done:
			case <-time.After(20 * time.Second):
				t.Fatalf("Run did not return; logs:\n%s", f.logs.String())
			}
			if created, closed := count.created.Load(), count.closed.Load(); created == 0 || closed != created {
				t.Fatalf("%d claim clients created, %d closed, want all closed; logs:\n%s", created, closed, f.logs.String())
			}
		})
	}
}
