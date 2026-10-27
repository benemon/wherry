package receiver

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"sync"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// topicHook counts the records the receiver's reader fetches, by topic.
type topicHook struct {
	mu      sync.Mutex
	fetched map[string]int
}

func (h *topicHook) OnFetchRecordBuffered(record *kgo.Record) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.fetched[record.Topic]++
}

// take returns the counts since the last take.
func (h *topicHook) take() map[string]int {
	h.mu.Lock()
	defer h.mu.Unlock()
	fetched := h.fetched
	h.fetched = map[string]int{}
	return fetched
}

// counted has the receiver read through a client that runs a topicHook.
func (f fixture) counted(t *testing.T) *topicHook {
	t.Helper()
	hook := &topicHook{fetched: map[string]int{}}
	base, err := kgo.NewClient(append(f.receiver.base.Opts(), kgo.WithHooks(hook))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(base.Close)
	f.receiver.reader = admin.NewReader(base)
	t.Cleanup(f.receiver.reader.Close)
	return hook
}

func (f fixture) endMember(t *testing.T, receiver string) {
	t.Helper()
	encoded, err := admin.EncodeRecord(admin.Record{Kind: "end", Receiver: receiver})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.service.Operator.ProduceSync(f.ctx, &kgo.Record{Topic: admin.AdminTopic, Value: encoded}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// A second admission fetches only the records each pool topic gained since the first.
func TestASecondAdmissionFetchesOnlyTheNewRecords(t *testing.T) {
	f := newFixture(t, resultStream)
	hook := f.counted(t)
	for _, run := range []string{"run-v", "run-w"} {
		f.publishStatus(t, f.turn(t, run, 1), testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
	}
	f.admitted(t, f.turn(t, "run-a", 1))
	hook.take()
	for _, run := range []string{"run-x", "run-y", "run-z"} {
		f.publishStatus(t, f.turn(t, run, 1), testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
	}
	f.admitted(t, f.turn(t, "run-b", 1))
	if fetched := hook.take(); fetched[admin.PoolEventsTopic(testPool)] != 3 || fetched[admin.PoolTasksTopic(testPool)] != 4 {
		t.Fatalf("the second admission fetched %v; want the 3 events and 4 tasks appended since the first", fetched)
	}
}

// A peer's terminal stops counting once a refresh shows the peer ended.
func TestAnEndedPeersTerminalStopsCounting(t *testing.T) {
	f := newFixture(t, resultStream)
	peer := f.addPeer(t, "receiver-b", testPool)
	record := f.submit(t, f.host, nil)
	f.publishStatus(t, record, "receiver-b", peer, "terminal", wire.TaskStateCompleted, nil)
	if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckAccept {
		t.Fatalf("admitted=%v ack=%v; want the peer's terminal acknowledged", admitted != nil, ack)
	}
	f.endMember(t, "receiver-b")
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	if admitted, ack := f.receiver.admit(f.ctx, record); admitted == nil {
		t.Fatalf("ack %v; want the run admitted once its terminal's writer ended; logs:\n%s", ack, f.logs.String())
	}
}

// After the host rotates its key, a run's first submission is its first under the current key.
func TestTheFirstSubmissionFollowsTheHostsCurrentKey(t *testing.T) {
	f := newFixture(t, resultStream)
	old := f.turn(t, "run-a", 1)
	f.admitted(t, f.turn(t, "run-b", 1))
	// A retired key counts only strictly before its rotation's broker time, which has millisecond resolution.
	time.Sleep(2 * time.Millisecond)
	_, next, _ := ed25519.GenerateKey(rand.Reader)
	f.produce(t, admin.HostTopic(testTenant, testHost), "", must(t)(signed.RotateKey(signed.HostOwner(testTenant, testHost), f.host, next)))
	f.host = next
	current := f.turn(t, "run-a", 2)
	if admitted, ack := f.receiver.admit(f.ctx, current); admitted == nil {
		t.Fatalf("current-key submission ack %v; want admitted as the run's first; logs:\n%s", ack, f.logs.String())
	}
	if admitted, ack := f.receiver.admit(f.ctx, old); admitted != nil || ack != kgo.AckReject {
		t.Fatalf("old-key submission admitted=%v ack=%v; want rejected", admitted != nil, ack)
	}
}

// The periodic read of cancels drops from the folds the runs retention has deleted.
func TestPruningDropsRunsPastRetention(t *testing.T) {
	terms := testTerms()
	terms.MaxQueueDeadlineMS = admin.Millis(time.Hour.Milliseconds())
	f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakeAdapter{"nomad": {stream: resultStream}})
	// In log order: retention deletes the log up to its last expired record.
	for _, aged := range []struct {
		run string
		age time.Duration
	}{{"run-ancient", 32 * time.Hour}, {"run-recent", time.Hour}} {
		run, age := aged.run, aged.age
		task := testSubmission(run)
		record := &kgo.Record{Topic: admin.PoolTasksTopic(testPool), Key: []byte(run), Value: must(t)(wire.SignRecord(task, f.host)), Timestamp: time.Now().Add(-age)}
		status := f.receiver.status(task, wire.TaskHash(record.Value), 1, "terminal", taskStatus(wire.TaskStateCompleted, ""), nil)
		event := &kgo.Record{Topic: admin.PoolEventsTopic(testPool), Key: []byte(run), Value: must(t)(wire.SignRecord(status, f.receiver.signing)), Timestamp: record.Timestamp}
		if err := f.service.Admin.ProduceSync(f.ctx, record, event).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.receiver.readSubmission(f.ctx, "run-recent"); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.readCancels(f.ctx); err != nil {
		t.Fatal(err)
	}
	if f.receiver.events.runs["run-ancient"] == nil {
		t.Fatal("the events fold lacks the ancient run before retention")
	}
	f.cluster.ApplyRetention()
	if err := f.receiver.readCancels(f.ctx); err != nil {
		t.Fatal(err)
	}
	_, events := f.receiver.events.runs["run-ancient"]
	_, tasks := f.receiver.tasks.runs["run-ancient"]
	if events || tasks || f.receiver.events.runs["run-recent"] == nil {
		t.Fatalf("after retention the folds hold the ancient run: events %v, tasks %v; want only the recent run", events, tasks)
	}
}

// An ambiguous commit reads back the pool's events from the offset folded before the transaction began.
func TestAnAmbiguousCommitReadsBackFromTheFold(t *testing.T) {
	f := newFixture(t, resultStream)
	for _, run := range []string{"run-x", "run-y", "run-z"} {
		f.publishStatus(t, f.turn(t, run, 1), testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
	}
	record := f.turn(t, "run-a", 1)
	hook := f.counted(t)
	if _, err := f.receiver.readEvents(f.ctx); err != nil {
		t.Fatal(err)
	}
	hook.take()
	previous := commitTransaction
	t.Cleanup(func() { commitTransaction = previous })
	commitTransaction = func(ctx context.Context, producer *kgo.Client) error {
		if err := producer.EndTransaction(ctx, kgo.TryCommit); err != nil {
			return err
		}
		return kerr.RequestTimedOut
	}
	status := f.receiver.status(f.parse(t, record), wire.TaskHash(record.Value), 1, "claimed", taskStatus(wire.TaskStateWorking, ""), nil)
	if err := f.receiver.admissionCommit(f.ctx, &kgo.Record{Topic: admin.PoolEventsTopic(testPool), Key: []byte("run-a"), Value: f.receiver.sign(status)}); err != nil {
		t.Fatalf("commit %v; want it read back as committed", err)
	}
	// The transaction's record and its commit marker.
	if fetched := hook.take()[admin.PoolEventsTopic(testPool)]; fetched != 2 {
		t.Fatalf("the read-back fetched %d event records; want the transaction's 2", fetched)
	}
}

// A receiver with folds built over many passes decides as a fresh receiver that folds the topics once.
func TestAFreshReceiverDecidesAsAWarmOne(t *testing.T) {
	f := newFixture(t, resultStream)
	warm := f.receiver
	fresh := func() *Receiver {
		t.Helper()
		r := &Receiver{id: warm.id, base: warm.base, reader: admin.NewReader(warm.base), signing: warm.signing, factory: warm.factory, logger: warm.logger, metrics: warm.metrics, config: warm.config, profiles: warm.profiles, offers: warm.offers}
		t.Cleanup(r.reader.Close)
		if err := r.refresh(f.ctx); err != nil {
			t.Fatal(err)
		}
		if err := r.readCancels(f.ctx); err != nil {
			t.Fatal(err)
		}
		return r
	}
	decide := func(r *Receiver, record *kgo.Record) [3]any {
		t.Helper()
		admitted, ack := r.admit(f.ctx, record)
		return [3]any{admitted != nil, admitted != nil && admitted.cancelled, ack}
	}
	records := map[string]*kgo.Record{}
	compare := func(step string) {
		t.Helper()
		other := fresh()
		for run, record := range records {
			if got, want := decide(warm, record), decide(other, record); got != want {
				t.Fatalf("%s, %s: warm receiver decided %v; a fresh one %v", step, run, got, want)
			}
			gotOffset, _ := warm.lastOffset(f.ctx, f.parse(t, record), wire.TaskHash(record.Value))
			wantOffset, _ := other.lastOffset(f.ctx, f.parse(t, record), wire.TaskHash(record.Value))
			if gotOffset != wantOffset {
				t.Fatalf("%s, %s: warm receiver's adoption offset %d; a fresh one's %d", step, run, gotOffset, wantOffset)
			}
		}
	}
	records["run-done"] = f.turn(t, "run-done", 1)
	f.publishStatus(t, records["run-done"], testReceiver, warm.signing, "terminal", wire.TaskStateCompleted, nil)
	records["run-new"] = f.turn(t, "run-new", 1)
	compare("terminal and new")
	records["run-held"] = f.turn(t, "run-held", 1)
	held := f.parse(t, records["run-held"])
	f.publishStatus(t, records["run-held"], testReceiver, warm.signing, "started", wire.TaskStateWorking, nil)
	f.produce(t, admin.PoolEventsTopic(testPool), "run-held", f.heartbeat(t, held, wire.TaskHash(records["run-held"].Value), testReceiver, warm.signing, 41, nil))
	records["run-cancelled"] = f.turn(t, "run-cancelled", 1)
	f.publishStatus(t, records["run-cancelled"], testReceiver, warm.signing, "claimed", wire.TaskStateWorking, nil)
	f.publishCancel(t, "run-cancelled", 1, f.host, nil)
	compare("held and cancelled")
	f.produce(t, admin.PoolEventsTopic(testPool), "run-held", f.heartbeat(t, held, wire.TaskHash(records["run-held"].Value), testReceiver, warm.signing, 57, nil))
	compare("later heartbeat")
}
