package receiver

import (
	"context"
	"crypto/ed25519"
	"io"
	"log"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// A rotate-key request has the key custodian rotate on its next pass, once, the command returns once it reads the
// rotated snapshot, and after resume a host wraps to the new key.
func TestRotateKeyRecordRotatesAtOnceAndHostsEncryptToTheNewKey(t *testing.T) {
	f := newBareFixture(t)
	custodian(t, f.ctx, f.receiver, time.Now())
	old, _ := onlyKey(t, f.receiver)
	if err := f.service.PoolSuspend(f.ctx, testPool, true); err != nil {
		t.Fatal(err)
	}
	type rotated struct {
		report operator.RotateReport
		err    error
	}
	done := make(chan rotated, 1)
	go func() {
		report, err := f.service.PoolRotateKey(f.ctx, testPool, nil, io.Discard)
		done <- rotated{report, err}
	}()
	var result rotated
	for waiting := true; waiting; {
		select {
		case result = <-done:
			waiting = false
		case <-time.After(100 * time.Millisecond):
			if err := f.receiver.refresh(f.ctx); err != nil {
				t.Fatal(err)
			}
			if err := f.receiver.tend(f.ctx, time.Now()); err != nil {
				t.Fatal(err)
			}
		}
	}
	if result.err != nil || !reflect.DeepEqual(result.report.SuspectKeys, []string{old}) {
		t.Fatalf("rotate-key %+v, %v; want the current key %s suspect", result.report, result.err, old)
	}
	for range 2 {
		if err := f.receiver.tend(f.ctx, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	records := f.snapshotRecords(t)
	latest := parseSnapshot(t, records[len(records)-1], testReceiver, f.receiver.signing)
	if len(records) != 2 || latest.Reason != "rotation" || latest.Current.KeyID == old {
		t.Fatalf("%d snapshots, latest %s with key %s; want one rotation away from %s; logs:\n%s", len(records), latest.Reason, latest.Current.KeyID, old, f.logs.String())
	}
	if err := f.service.PoolSuspend(f.ctx, testPool, false); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WHERRY_TEST_TOKEN", testToken)
	kafka, err := kgo.NewClient(append(f.service.Operator.Opts(), kgo.FetchIsolationLevel(kgo.ReadCommitted()))...)
	if err != nil {
		t.Fatal(err)
	}
	defer kafka.Close()
	host, err := client.NewHost(kafka, f.host, operator.TenantPrincipal(testTenant, testHost), filepath.Join(t.TempDir(), "host"),
		map[string]credential.Source{credentialName: {Env: "WHERRY_TEST_TOKEN"}}, nil, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	submitted, err := host.Submit(f.ctx, client.SubmitInput{Agent: "claude", Prompt: "hello", StartBy: time.Now().Add(time.Minute), EndBy: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range f.records(t, f.tasksTopic()) {
		if string(record.Key) != submitted.RunID {
			continue
		}
		var task wire.Submission
		if err := wire.ParseRecord(record.Value, signed.Keys{signed.HostOwner(testTenant, testHost): {Current: f.host.Public().(ed25519.PublicKey)}}, &task); err != nil {
			t.Fatal(err)
		}
		if kid, err := signed.WrapKeyID([]byte(task.Wherry.Wrap)); err != nil || kid != latest.Current.KeyID {
			t.Fatalf("submit wrap to %q, %v; want the rotated key %s", kid, err, latest.Current.KeyID)
		}
		return
	}
	t.Fatal("the submitted task is not on the task topic")
}

// Rotate-key requests are ordered by their wherry.operator offsets, never by a clock: a request above the offset the
// current snapshot carries rotates the key once, whatever its timestamp, and one at or below it never does.
func TestRotateKeyRequestsAreOrderedByOffset(t *testing.T) {
	f := newBareFixture(t)
	custodian(t, f.ctx, f.receiver, time.Now())
	request := func(stamped time.Time) int64 {
		t.Helper()
		record := &kgo.Record{Topic: operator.OperatorTopic, Key: []byte("rotate-key." + testPool), Value: must(t)(operator.EncodeRecord(operator.Record{Kind: "rotate-key", Pool: testPool})), Timestamp: stamped}
		if err := f.service.Operator.ProduceSync(f.ctx, record).FirstErr(); err != nil {
			t.Fatal(err)
		}
		if err := f.receiver.refresh(f.ctx); err != nil {
			t.Fatal(err)
		}
		return record.Offset
	}
	passes := func(want int, rotatedFor int64) {
		t.Helper()
		for range 3 {
			if err := f.receiver.tend(f.ctx, time.Now()); err != nil {
				t.Fatal(err)
			}
		}
		records := f.snapshotRecords(t)
		if latest := parseSnapshot(t, records[len(records)-1], testReceiver, f.receiver.signing); len(records) != want || latest.RotatedFor != rotatedFor {
			t.Fatalf("%d snapshots, the latest rotated for %d; want %d, rotated for %d; logs:\n%s", len(records), latest.RotatedFor, want, rotatedFor, f.logs.String())
		}
	}
	// Stamped after every snapshot, so a clock would rotate on it at every pass.
	future := request(time.Now().Add(time.Hour))
	passes(2, future)
	// Stamped before every snapshot, so a clock would never rotate on it.
	past := request(time.Now().Add(-time.Hour))
	passes(3, past)
	// A restarted custodian reads which requests are done from the snapshot.
	again := restarted(t, f.receiver)
	if err := again.configure(f.ctx); err != nil {
		t.Fatal(err)
	}
	custodian(t, f.ctx, again, time.Now())
	f.receiver = again
	passes(3, past)
}

// suspend suspends the pool and has the receiver read it.
func (f fixture) suspend(t *testing.T) {
	t.Helper()
	if err := f.service.PoolSuspend(f.ctx, testPool, true); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
}

// A receiver of a suspended pool claims nothing and does not stop for a denial; after resume it claims and runs the
// task.
func TestSuspendedPoolIsNotClaimedFromUntilResumed(t *testing.T) {
	f := newFixture(t, resultStream)
	f.suspend(t)
	f.submit(t, f.host, nil)
	f.run(t, f.ctx)
	time.Sleep(2 * time.Second)
	if kinds := f.kinds(t); len(kinds) != 0 || strings.Contains(f.logs.String(), "stopping: ") {
		t.Fatalf("statuses %v while suspended; logs:\n%s", kinds, f.logs.String())
	}
	if err := f.service.PoolSuspend(f.ctx, testPool, false); err != nil {
		t.Fatal(err)
	}
	if task := f.await(t, testRun("run-a"), func(task wire.Task) bool { return task.Status.State == wire.TaskStateCompleted }); task.ID != testRun("run-a") {
		t.Fatalf("task %+v after resume", task)
	}
}

// A run in progress when its pool is suspended renews its claim, completes and acknowledges its task.
func TestARunInProgressAtSuspensionCompletes(t *testing.T) {
	f := newFixture(t, resultStream)
	f.plugin.endDelay = 3 * time.Second
	record := f.submit(t, f.host, nil)
	done := make(chan error, 1)
	go func() { done <- f.receiver.claimOne(f.ctx, f.ctx, f.receiver.profiles[0], func() {}) }()
	for {
		if _, _, follows, _ := f.plugin.counts(); follows == 1 {
			break
		}
		if f.ctx.Err() != nil {
			t.Fatalf("run never started; logs:\n%s", f.logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	f.suspend(t)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if task := f.lastTask(t); task.Status.State != wire.TaskStateCompleted {
		t.Fatalf("task %+v, want COMPLETED; logs:\n%s", task.Status, f.logs.String())
	}
	if _, delivered := f.pollPool(t, string(record.Key), 3*time.Second); delivered {
		t.Fatal("the completed run's task is delivered again, want it acknowledged")
	}
}

// While its pool is suspended the custodian refuses a first start's open request pool suspended and the claimant
// releases the task, unstarted, to the queue; an adoption's open request is served and the adopted run completes.
func TestSuspendedPoolRefusesFirstStartsAndServesAdoptions(t *testing.T) {
	t.Run("first start", func(t *testing.T) {
		f := newFixture(t, resultStream)
		f.suspend(t)
		if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckRelease {
			t.Fatalf("status %v, want the task released; logs:\n%s", status, f.logs.String())
		}
		if kinds := f.kinds(t); !slices.Equal(kinds, []string{"claimed", "released"}) || f.answeredWith(t, wire.RefusedPoolSuspended) != 1 {
			t.Fatalf("statuses %v and %d pool suspended refusals, want claimed, released and one", kinds, f.answeredWith(t, wire.RefusedPoolSuspended))
		}
		if dispatches, _, _ := f.dispatched(t); dispatches != 0 {
			t.Fatalf("dispatches %d, want none", dispatches)
		}
	})
	t.Run("adoption", func(t *testing.T) {
		f, record := adopting(t)
		f.suspend(t)
		if status := f.execute(t, record, 2); status != kgo.AckAccept || f.lastTask(t).Status.State != wire.TaskStateCompleted {
			t.Fatalf("adopter ended %v; logs:\n%s", status, f.logs.String())
		}
		if f.answeredWith(t, wire.RefusedPoolSuspended) != 0 {
			t.Fatal("an adoption was refused pool suspended")
		}
	})
}

// While its pool is suspended a run with an earlier started status whose job Lookup reports absent is a first start:
// the custodian refuses its open request pool suspended and nothing is dispatched.
func TestSuspendedPoolRefusesAStartedRunWhoseJobIsAbsent(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.submit(t, f.host, nil)
	f.statusAs(t, record, testReceiver, f.receiver.signing, "claimed", 1)
	f.statusAs(t, record, testReceiver, f.receiver.signing, "started", 1)
	f.suspend(t)
	if status := f.execute(t, record, 2); status != kgo.AckRelease {
		t.Fatalf("status %v, want the task released; logs:\n%s", status, f.logs.String())
	}
	if refused := f.answeredWith(t, wire.RefusedPoolSuspended); refused != 1 {
		t.Fatalf("%d pool suspended refusals, want one; logs:\n%s", refused, f.logs.String())
	}
	if dispatches, _, _ := f.dispatched(t); dispatches != 0 {
		t.Fatalf("dispatches %d, want none", dispatches)
	}
}

// A receiver that has not read its pool's suspension still claims, but the custodian reads wherry.operator before it serves
// a first start: the open is refused pool suspended and no attempt wrap is written or job dispatched.
func TestCustodianReadsTheSuspensionFresh(t *testing.T) {
	f := newFixture(t, resultStream)
	if err := f.service.PoolSuspend(f.ctx, testPool, true); err != nil {
		t.Fatal(err)
	}
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckRelease {
		t.Fatalf("status %v, want the task released; logs:\n%s", status, f.logs.String())
	}
	if refused := f.answeredWith(t, wire.RefusedPoolSuspended); refused != 1 {
		t.Fatalf("%d pool suspended refusals, want one; logs:\n%s", refused, f.logs.String())
	}
	for _, record := range f.records(t, operator.PoolEventsTopic(testPool)) {
		if recordType(record.Value) == wire.AttemptWrapType {
			t.Fatal("an attempt wrap was written for a suspended pool")
		}
	}
	if dispatches, _, _ := f.dispatched(t); dispatches != 0 {
		t.Fatalf("dispatches %d, want none", dispatches)
	}
}

// A suspension landing after a pass's read of wherry.operator does not stop that pass's first start, and pool suspend
// waits for the next pass's snapshot acknowledging the record; no first start decided after it opens.
func TestSuspendReturnsOnceTheKeyCustodianAcknowledges(t *testing.T) {
	// Restored once the fixture stops serving.
	t.Cleanup(func() { loadState = (*operator.Reader).Load })
	f := newFixture(t, resultStream)
	f.quiet()
	first := f.executing(t, f.ctx, f.submit(t, f.host, nil), 1)
	f.request(t, 1)
	type outcome struct {
		text string
		err  error
	}
	returned := make(chan outcome, 1)
	var suspendedAt int64
	landed := false
	loadState = func(reader *operator.Reader, ctx context.Context) (operator.State, error) {
		state, err := reader.Load(ctx)
		if landed {
			return state, err
		}
		landed = true
		go func() {
			text, err := f.service.PoolSuspendAcknowledged(f.ctx, testPool, true, io.Discard)
			returned <- outcome{text, err}
		}()
		for {
			if current, err := f.service.LoadState(f.ctx); err == nil && current.Suspended[testPool] {
				suspendedAt = current.SuspendChanged[testPool]
				return state, err
			}
			if f.ctx.Err() != nil {
				t.Error("the suspend record was never written")
				return state, err
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	if err := f.receiver.serveOpens(f.ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-returned:
		t.Fatalf("pool suspend returned %+v before the key custodian acknowledged it", result)
	case <-time.After(time.Second):
	}
	f.serving.Store(true)
	result := <-returned
	records := f.snapshotRecords(t)
	acknowledged := parseSnapshot(t, records[len(records)-1], testReceiver, f.receiver.signing)
	if result.err != nil || !strings.Contains(result.text, "no new start opens") || acknowledged.SuspendedFor != suspendedAt {
		t.Fatalf("pool suspend returned %+v with the snapshot suspended for %d, want suspension in effect for %d", result, acknowledged.SuspendedFor, suspendedAt)
	}
	if status := <-first; status != kgo.AckAccept {
		t.Fatalf("the first start decided before the suspension ended %v; logs:\n%s", status, f.logs.String())
	}
	second := f.submit(t, f.host, func(task *wire.Submission) {
		named(task, "run-b")
	})
	if status := f.execute(t, second, 1); status != kgo.AckRelease || f.answeredWith(t, wire.RefusedPoolSuspended) != 1 {
		t.Fatalf("a first start after the acknowledgement ended %v with %d pool suspended refusals, want released and one", status, f.answeredWith(t, wire.RefusedPoolSuspended))
	}
	for _, record := range f.records(t, operator.PoolEventsTopic(testPool)) {
		if recordType(record.Value) == wire.AttemptWrapType && (string(record.Key) != testRun("run-a") || record.Timestamp.After(records[len(records)-1].Timestamp)) {
			t.Fatalf("an attempt wrap for %s appended at %s, after the acknowledgement at %s", record.Key, record.Timestamp, records[len(records)-1].Timestamp)
		}
	}
}

// A pass reads wherry.operator once for all its pending first starts.
func TestOneSuspensionReadPerPass(t *testing.T) {
	// Restored once the fixture stops serving.
	t.Cleanup(func() { loadState = (*operator.Reader).Load })
	f := newFixture(t, resultStream)
	f.quiet()
	if err := f.service.PoolSuspend(f.ctx, testPool, true); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"run-a", "run-b", "run-c"} {
		record := f.submit(t, f.host, func(task *wire.Submission) {
			named(task, name)
		})
		f.publishStatus(t, record, testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
		f.openAs(t, record, testReceiver, f.receiver.signing, 1)
	}
	reads := 0
	loadState = func(reader *operator.Reader, ctx context.Context) (operator.State, error) {
		reads++
		return reader.Load(ctx)
	}
	if err := f.receiver.serveOpens(f.ctx); err != nil {
		t.Fatal(err)
	}
	if refused := f.answeredWith(t, wire.RefusedPoolSuspended); refused != 3 || reads != 1 {
		t.Fatalf("%d pool suspended refusals over %d reads of wherry.operator, want 3 over one; logs:\n%s", refused, reads, f.logs.String())
	}
}

// With no snapshot pool suspend returns at once, saying so.
func TestSuspendWithNoCustodianReturns(t *testing.T) {
	f := newBareFixture(t)
	text, err := f.service.PoolSuspendAcknowledged(f.ctx, testPool, true, io.Discard)
	if err != nil || !strings.Contains(text, "no key custodian yet") {
		t.Fatalf("pool suspend returned %q, %v; want no key custodian yet", text, err)
	}
}

// A drain while the pool is suspended scans nothing, does not stop for a denial and keeps every key it holds.
func TestDrainWhileSuspendedKeepsTheKeys(t *testing.T) {
	f, successor, drain := drainFixture(t, 15*time.Second)
	f.submit(t, f.host, nil)
	held := heldKeys(t, f.receiver)
	if err := f.service.PoolSuspend(f.ctx, testPool, true); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	drain()
	records := f.snapshotRecords(t)
	if parseSnapshot(t, records[len(records)-1], "receiver-b", successor.signing).Reason != "takeover" {
		t.Fatalf("no takeover; logs:\n%s", f.logs.String())
	}
	if keys := heldKeys(t, f.receiver); len(keys) != len(held) || len(f.rekeys(t)) != 0 {
		t.Fatalf("keys %s and %d re-keyed runs after a drain while suspended, want %s kept and none", describeKeys(keys), len(f.rekeys(t)), describeKeys(held))
	}
	if logs := f.logs.String(); !strings.Contains(logs, "is suspended; its re-key scan waits for resume") || strings.Contains(logs, "stopping: ") {
		t.Fatalf("want the suspended scan logged without a denial; logs:\n%s", logs)
	}
}
