package receiver

import (
	"crypto/ed25519"
	"io"
	"log"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/credential"
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
		report admin.RotateReport
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
	kafka, err := kgo.NewClient(append(f.service.Admin.Opts(), kgo.FetchIsolationLevel(kgo.ReadCommitted()))...)
	if err != nil {
		t.Fatal(err)
	}
	defer kafka.Close()
	host, err := client.NewHost(kafka, f.host, admin.TenantPrincipal(testTenant, testHost), filepath.Join(t.TempDir(), "host"),
		map[string]credential.Source{credentialName: {Env: "WHERRY_TEST_TOKEN"}}, nil, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	submitted, err := host.Submit(f.ctx, client.SubmitInput{Agent: "claude", Prompt: "hello", StartBy: time.Now().Add(time.Minute), EndBy: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range f.records(t, admin.PoolTasksTopic(testPool)) {
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

// Rotate-key requests are ordered by their wherry.admin offsets, never by a clock: a request above the offset the
// current snapshot carries rotates the key once, whatever its timestamp, and one at or below it never does.
func TestRotateKeyRequestsAreOrderedByOffset(t *testing.T) {
	f := newBareFixture(t)
	custodian(t, f.ctx, f.receiver, time.Now())
	request := func(stamped time.Time) int64 {
		t.Helper()
		record := &kgo.Record{Topic: admin.AdminTopic, Key: []byte("rotate-key." + testPool), Value: must(t)(admin.EncodeRecord(admin.Record{Kind: "rotate-key", Pool: testPool})), Timestamp: stamped}
		if err := f.service.Admin.ProduceSync(f.ctx, record).FirstErr(); err != nil {
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

// A receiver of a suspended pool claims nothing and records no denial; after resume it claims and runs the task.
func TestSuspendedPoolIsNotClaimedFromUntilResumed(t *testing.T) {
	f := newFixture(t, resultStream)
	f.suspend(t)
	f.submit(t, f.host, nil)
	f.run(t, f.ctx)
	time.Sleep(2 * time.Second)
	if kinds := f.kinds(t); len(kinds) != 0 || strings.Contains(f.logs.String(), "denied (") {
		t.Fatalf("statuses %v while suspended; logs:\n%s", kinds, f.logs.String())
	}
	if err := f.service.PoolSuspend(f.ctx, testPool, false); err != nil {
		t.Fatal(err)
	}
	if task := f.await(t, "run-a", func(task wire.Task) bool { return task.Status.State == wire.TaskStateCompleted }); task.ID != "run-a" {
		t.Fatalf("task %+v after resume", task)
	}
}

// A run in progress when its pool is suspended renews its claim, completes and acknowledges its task.
func TestARunInProgressAtSuspensionCompletes(t *testing.T) {
	f := newFixture(t, resultStream)
	f.plugin.endDelay = 3 * time.Second
	record := f.submit(t, f.host, nil)
	done := make(chan error, 1)
	go func() { done <- f.receiver.claimOne(f.ctx, f.ctx, func() {}) }()
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

// A drain while the pool is suspended scans nothing, records no denial and keeps every key it holds.
func TestDrainWhileSuspendedKeepsTheKeys(t *testing.T) {
	f, successor, drain := handoverFixture(t, 15*time.Second)
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
	if parseSnapshot(t, records[len(records)-1], "receiver-b", successor.signing).Reason != "handover" {
		t.Fatalf("no handover; logs:\n%s", f.logs.String())
	}
	if keys := heldKeys(t, f.receiver); len(keys) != len(held) || len(f.rekeys(t)) != 0 {
		t.Fatalf("keys %s and %d re-keyed runs after a drain while suspended, want %s kept and none", describeKeys(keys), len(f.rekeys(t)), describeKeys(held))
	}
	if logs := f.logs.String(); !strings.Contains(logs, "is suspended; its re-key scan waits for resume") || strings.Contains(logs, "denied (") {
		t.Fatalf("want the suspended scan logged without a denial; logs:\n%s", logs)
	}
}
