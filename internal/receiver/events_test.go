package receiver

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// freshReader is a receiver with the ID of f's receiver and its own fold of the pool's events.
func freshReader(t *testing.T, f fixture) *Receiver {
	t.Helper()
	fresh := restarted(t, f.receiver)
	if err := fresh.configure(f.ctx); err != nil {
		t.Fatal(err)
	}
	return fresh
}

// sameAsFullRead reads the pool's events through f's receiver's fold and fails unless the view equals fresh's first read
// of the whole topic.
func sameAsFullRead(t *testing.T, f fixture, fresh *Receiver) poolEvents {
	t.Helper()
	incremental, err := f.receiver.readEvents(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := fresh.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	fresh.events.mu.Lock()
	fresh.events.reset()
	fresh.events.mu.Unlock()
	full, err := fresh.readEvents(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(incremental, full) {
		t.Fatalf("incremental view %s, want the full read's %s", describeEvents(incremental), describeEvents(full))
	}
	return incremental
}

func describeEvents(events poolEvents) string {
	count := func(lengths ...int) (n int) {
		for _, length := range lengths {
			n += length
		}
		return n
	}
	var statuses, failures, others, rekeys []int
	for _, run := range events.statuses {
		statuses = append(statuses, len(run))
	}
	for _, run := range events.startFailures {
		failures = append(failures, len(run))
	}
	for _, run := range events.others {
		others = append(others, len(run))
	}
	for _, hash := range events.rekeys {
		rekeys = append(rekeys, len(hash))
	}
	return fmt.Sprintf("{statuses %d, start failures %d, others %d, requests %d, answered %d, re-key wraps %d, decisions %d}",
		count(statuses...), count(failures...), count(others...), len(events.requests), len(events.answered), count(rekeys...), len(events.decisions))
}

// The view each read of the fold returns equals a first read of the whole topic, after appends of every record kind,
// one or several between reads.
func TestPoolEventsIncrementalViewEqualsAFullRead(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	keyB := f.addPeer(t, "receiver-b", testPool)
	fresh := freshReader(t, f)
	sameAsFullRead(t, f, fresh)
	record := f.submit(t, f.host, nil)
	other := f.turn(t, "run-b", 1)
	events := operator.PoolEventsTopic(testPool)
	var eventID string
	steps := [][]func(){
		{func() {
			f.publishStatus(t, record, testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, func(status *wire.RunStatus) { eventID = status.Wherry.EventID })
		}},
		{
			func() { f.startFailed(t, record, testReceiver, f.receiver.signing) },
			func() {
				f.produce(t, events, testRun("run-a"), must(t)(wire.SignRecord(&wire.Cancel{Tenant: testTenant, Host: testHost, Pool: testPool, Run: testRun("run-a")}, f.host)))
			},
		},
		{func() { f.answered(t, f.openAs(t, record, testReceiver, f.receiver.signing, 1)) }},
		{
			func() {
				f.publishStatus(t, record, testReceiver, f.receiver.signing, "started", wire.TaskStateWorking, func(status *wire.RunStatus) { status.Wherry.EventID = eventID })
			},
			func() { f.statusAs(t, other, "receiver-b", keyB, "claimed", 1) },
			func() { f.openAs(t, other, "receiver-b", keyB, 1) },
			func() { f.produce(t, events, testRun("run-b"), []byte("not a record")) },
		},
		{func() { f.statusAs(t, other, "receiver-b", keyB, "started", 1) }},
	}
	for _, step := range steps {
		for _, appendRecord := range step {
			appendRecord()
		}
		sameAsFullRead(t, f, fresh)
	}
	view := sameAsFullRead(t, f, fresh)
	if len(view.statuses[testRun("run-a")]) != 1 || len(view.statuses[testRun("run-b")]) != 2 || len(view.requests) != 2 || len(view.answered) != 1 {
		t.Fatalf("view %s, want every append counted", describeEvents(view))
	}
}

// Retention moving the log start drops what only the deleted records contributed: a cancel goes, and a status whose
// event ID a later record repeats is replaced by that record, as a full read sees it.
func TestPoolEventsDropWhatRetentionDeleted(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	fresh := freshReader(t, f)
	record := f.submit(t, f.host, nil)
	topic := operator.PoolEventsTopic(testPool)
	f.produce(t, topic, testRun("run-a"), must(t)(wire.SignRecord(&wire.Cancel{Tenant: testTenant, Host: testHost, Pool: testPool, Run: testRun("run-a")}, f.host)))
	var eventID string
	f.publishStatus(t, record, testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, func(status *wire.RunStatus) { eventID = status.Wherry.EventID })
	before := sameAsFullRead(t, f, fresh)
	f.publishStatus(t, record, testReceiver, f.receiver.signing, "started", wire.TaskStateWorking, func(status *wire.RunStatus) { status.Wherry.EventID = eventID })
	sameAsFullRead(t, f, fresh)
	records := f.records(t, topic)
	repeated := records[len(records)-1].Offset
	var offsets kadm.Offsets
	offsets.Add(kadm.Offset{Topic: topic, Partition: 0, At: repeated})
	deleted, err := kadm.NewClient(f.service.SuperUser).DeleteRecords(f.ctx, offsets)
	if err == nil {
		err = deleted.Error()
	}
	if err != nil {
		t.Fatal(err)
	}
	after := sameAsFullRead(t, f, fresh)
	if len(before.others[testRun("run-a")]) != 1 || len(after.others[testRun("run-a")]) != 0 {
		t.Fatalf("cancels %d before and %d after the deletion, want 1 and 0", len(before.others[testRun("run-a")]), len(after.others[testRun("run-a")]))
	}
	if kept := after.records[testRun("run-a")]; len(kept) != 1 || kept[0].Offset != repeated {
		t.Fatalf("status records %d, want the one repeating the deleted status's event ID", len(kept))
	}
}

// Retention moving the log start between the listing before a read and its fetch: the fold drops by the start in
// effect after the fetch, as a full read sees it.
func TestPoolEventsDropByTheStartInEffectAfterTheFetch(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	fresh := freshReader(t, f)
	record := f.submit(t, f.host, nil)
	topic := operator.PoolEventsTopic(testPool)
	f.produce(t, topic, testRun("run-a"), must(t)(wire.SignRecord(&wire.Cancel{Tenant: testTenant, Host: testHost, Pool: testPool, Run: testRun("run-a")}, f.host)))
	before := sameAsFullRead(t, f, fresh)
	f.publishStatus(t, record, testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
	records := f.records(t, topic)
	last := records[len(records)-1].Offset
	var once sync.Once
	f.cluster.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Fetch}, Topic: topic, Observe: true, When: func(kmsg.Request) bool {
		once.Do(func() {
			if err := f.cluster.DeleteRecords(topic, 0, last); err != nil {
				t.Error(err)
			}
		})
		return true
	}})
	after := sameAsFullRead(t, f, fresh)
	if len(before.others[testRun("run-a")]) != 1 || len(after.others[testRun("run-a")]) != 0 || len(after.statuses[testRun("run-a")]) != 1 {
		t.Fatalf("cancels %d before and %d after, statuses %d after, want 1, 0 and the retained status", len(before.others[testRun("run-a")]), len(after.others[testRun("run-a")]), len(after.statuses[testRun("run-a")]))
	}
}

// A revocation changes how earlier records verify: the fold starts again and drops the revoked member's statuses.
func TestPoolEventsRebuildWhenMembershipChanges(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	keyB := f.addPeer(t, "receiver-b", testPool)
	fresh := freshReader(t, f)
	record := f.submit(t, f.host, nil)
	f.statusAs(t, record, "receiver-b", keyB, "claimed", 1)
	f.statusAs(t, record, testReceiver, f.receiver.signing, "claimed", 2)
	if before := sameAsFullRead(t, f, fresh); len(before.statuses[testRun("run-a")]) != 2 {
		t.Fatalf("statuses %d before the revocation, want 2", len(before.statuses[testRun("run-a")]))
	}
	f.revoke(t, "receiver-b")
	if after := sameAsFullRead(t, f, fresh); len(after.statuses[testRun("run-a")]) != 1 {
		t.Fatalf("statuses %d after the revocation, want this receiver's alone", len(after.statuses[testRun("run-a")]))
	}
}

// Each status, start failure and open request on the pool's events is verified once across every read of the process,
// refreshes between them included, and any other record not at all.
func TestPoolEventsVerifyEachRecordOnce(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	var verifications atomic.Int64
	parseHistory = func(appended time.Time) func([]byte, signed.Keys, wire.Record) error {
		parse := wire.ParseHistory(appended)
		return func(encoded []byte, keys signed.Keys, value wire.Record) error {
			verifications.Add(1)
			return parse(encoded, keys, value)
		}
	}
	parseStrict = func(encoded []byte, keys signed.Keys, appended time.Time, value wire.Record) error {
		verifications.Add(1)
		return wire.ParseStrict(encoded, keys, appended, value)
	}
	t.Cleanup(func() { parseHistory, parseStrict = wire.ParseHistory, wire.ParseStrict })
	record := f.submit(t, f.host, nil)
	f.publishStatus(t, record, testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
	f.startFailed(t, record, testReceiver, f.receiver.signing)
	f.publishCancel(t, testRun("run-a"), 1, f.host, nil)
	f.openAs(t, record, testReceiver, f.receiver.signing, 1)
	read := func() poolEvents {
		t.Helper()
		events, err := f.receiver.readEvents(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.receiver.refresh(f.ctx); err != nil {
			t.Fatal(err)
		}
		return events
	}
	for range 5 {
		if events := read(); len(events.statuses[testRun("run-a")]) != 1 || len(events.startFailures[testRun("run-a")]) != 1 || len(events.others[testRun("run-a")]) != 1 || len(events.requests) != 1 {
			t.Fatalf("events %s, want one status, one start failure, the cancel and the open request", describeEvents(events))
		}
	}
	if got := verifications.Load(); got != 3 {
		t.Fatalf("%d verifications over 5 reads, want one per status, start failure and open request", got)
	}
	f.publishStatus(t, record, testReceiver, f.receiver.signing, "started", wire.TaskStateWorking, nil)
	for range 5 {
		read()
	}
	if got := verifications.Load(); got != 4 {
		t.Fatalf("%d verifications after one more status, want 4", got)
	}
}
