package receiver

import (
	"context"
	"crypto/ed25519"
	"io"
	"log"
	"path/filepath"
	"reflect"
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

// A receiver of a suspended pool claims nothing, answers no open request and records no denial; after resume it claims
// and runs the task.
func TestSuspendedPoolIsNotClaimedFromUntilResumed(t *testing.T) {
	f := newFixture(t, resultStream)
	if err := f.service.PoolSuspend(f.ctx, testPool, true); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	record := f.submit(t, f.host, nil)
	f.openAs(t, record, testReceiver, f.receiver.signing, 1)
	f.run(t, f.ctx)
	time.Sleep(2 * time.Second)
	if dispatches, _, _ := f.dispatched(t); dispatches != 0 || len(f.records(t, admin.PoolEventsTopic(testPool))) != 1 || strings.Contains(f.logs.String(), "denied (") {
		t.Fatalf("dispatches %d while suspended; logs:\n%s", dispatches, f.logs.String())
	}
	if err := f.service.PoolSuspend(f.ctx, testPool, false); err != nil {
		t.Fatal(err)
	}
	if task := f.await(t, "run-a", func(task wire.Task) bool { return task.Status.State == wire.TaskStateCompleted }); task.ID != "run-a" {
		t.Fatalf("task %+v after resume", task)
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
	if logs := f.logs.String(); !strings.Contains(logs, "is suspended, so its task topic cannot be scanned") || strings.Contains(logs, "denied (") {
		t.Fatalf("want the suspended scan logged without a denial; logs:\n%s", logs)
	}
}

// endedSuspended checks that run ended FAILED pool suspended, without key-lost, after stops Stops, and that its task
// delivered again after resume is acknowledged without a run.
func endedSuspended(t *testing.T, f fixture, record *kgo.Record, stops int) {
	t.Helper()
	var terminals []wire.RunStatus
	for _, status := range f.eventStatuses(t) {
		if status.Wherry.Run == string(record.Key) && status.Wherry.Kind == "terminal" {
			terminals = append(terminals, status)
		}
	}
	if len(terminals) != 1 || terminals[0].StatusUpdate.Status.State != wire.TaskStateFailed || terminals[0].StatusUpdate.Status.Message.Parts[0].Text != suspendedReason ||
		terminals[0].Wherry.Event != "" {
		t.Fatalf("terminals %+v, want one FAILED pool suspended without key-lost; logs:\n%s", terminals, f.logs.String())
	}
	f.plugin.mu.Lock()
	stopped := f.plugin.stops
	f.plugin.mu.Unlock()
	if stopped != stops {
		t.Fatalf("stops %d, want %d", stopped, stops)
	}
	if err := f.service.PoolSuspend(f.ctx, testPool, false); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckAccept {
		t.Fatalf("after resume the task is admitted %v with %v, want acknowledged without a run", admitted != nil, ack)
	}
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

// A run waiting for its open when its pool is suspended ends FAILED pool suspended and never dispatches.
func TestSuspendEndsARunBeforeItsOpen(t *testing.T) {
	f := newFixture(t, resultStream)
	previous := openWait
	openWait = 30 * time.Second
	t.Cleanup(func() { openWait = previous })
	f.quiet()
	record := f.submit(t, f.host, nil)
	done := f.executing(t, f.ctx, record, 1)
	f.request(t, 1)
	f.suspend(t)
	if status := <-done; status != kgo.AckReject {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	if dispatches, _, _ := f.dispatched(t); dispatches != 0 {
		t.Fatalf("dispatches %d, want none", dispatches)
	}
	endedSuspended(t, f, record, 0)
}

// A run following its job when its pool is suspended has its job stopped and ends FAILED pool suspended, on the renewal
// the withdrawn READ refuses, before the receiver reads wherry.admin again; after resume its task is not run again.
func TestSuspendStopsARunningRunOnItsRefusedRenewal(t *testing.T) {
	f := newFixture(t, noResultStream)
	f.plugin.endDelay = 30 * time.Second
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
	if err := f.service.PoolSuspend(f.ctx, testPool, true); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatalf("the run kept going on refused renewals; logs:\n%s", f.logs.String())
	}
	endedSuspended(t, f, record, 1)
	// The claim the suspension left unacknowledged comes back after resume; the receiver acknowledges it unrun.
	ctx, stop := context.WithCancel(f.ctx)
	done = make(chan error, 1)
	go func() { done <- f.receiver.Run(ctx) }()
	time.Sleep(3 * time.Second)
	stop()
	<-done
	if dispatches, _, _ := f.dispatched(t); dispatches != 1 {
		t.Fatalf("dispatches %d after resume, want 1; logs:\n%s", dispatches, f.logs.String())
	}
	if record, delivered := f.pollPool(t, string(record.Key), 3*time.Second); delivered {
		t.Fatalf("the task is still delivered (delivery %d) after resume", record.DeliveryCount())
	}
}

// A run whose pool is suspended while its outputs are collected for its terminal commit ends FAILED pool suspended
// with its job stopped, whatever its outcome.
func TestSuspendEndsARunInItsTerminalCommit(t *testing.T) {
	f := newFixture(t, resultStream)
	f.plugin.collecting = func() { f.suspend(t) }
	record := f.submit(t, f.host, nil)
	if status := <-f.executing(t, f.ctx, record, 1); status != kgo.AckReject {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	f.plugin.mu.Lock()
	f.plugin.collecting = nil
	f.plugin.mu.Unlock()
	endedSuspended(t, f, record, 2)
}
