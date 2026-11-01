package receiver

import (
	"context"
	"crypto/ed25519"
	"errors"
	"io"
	"log"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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

// suspendDuringDispatch starts a run under ctx whose Dispatch hangs, with Lookup reporting its job running, and
// suspends the pool once it dispatches; it returns where the run's acknowledgement arrives.
func suspendDuringDispatch(t *testing.T, f fixture, ctx context.Context, record *kgo.Record) <-chan kgo.AckStatus {
	t.Helper()
	f.plugin.mu.Lock()
	f.plugin.dispatchBlocks, f.plugin.job = true, pluginsdk.JobRunning
	f.plugin.mu.Unlock()
	done := f.executing(t, ctx, record, 1)
	for {
		if dispatches, _, _ := f.dispatched(t); dispatches == 1 {
			break
		}
		if f.ctx.Err() != nil {
			t.Fatalf("never dispatched; logs:\n%s", f.logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	f.suspend(t)
	return done
}

// acknowledged returns the acknowledgement from done, failing the test without it within timeout.
func acknowledged(t *testing.T, f fixture, done <-chan kgo.AckStatus, timeout time.Duration) kgo.AckStatus {
	t.Helper()
	select {
	case status := <-done:
		return status
	case <-time.After(timeout):
		t.Fatalf("the run outlived the suspension; logs:\n%s", f.logs.String())
		return 0
	}
}

// A suspension cancels a hung Dispatch; the job it may have created is looked up and stopped, and the run ends FAILED
// pool suspended.
func TestSuspendCancelsAHungDispatchAndStopsItsJob(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.submit(t, f.host, nil)
	if status := acknowledged(t, f, suspendDuringDispatch(t, f, f.ctx, record), 15*time.Second); status != kgo.AckReject {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	if _, lookups, _, _ := f.plugin.counts(); lookups == 0 {
		t.Fatal("the job was not looked up after the cancelled dispatch")
	}
	endedSuspended(t, f, record, 1)
}

// A suspended run whose job's Stop fails until the bound ends FAILED job stop unconfirmed with its task and material
// kept: no released status, no Release, and the stop counted unconfirmed.
func TestSuspendWithAnUnconfirmedStopKeepsTheRunHeld(t *testing.T) {
	f := newFixture(t, resultStream)
	previous := suspendStopBound
	suspendStopBound = 2 * time.Second
	t.Cleanup(func() { suspendStopBound = previous })
	f.plugin.stopErr = errors.New("platform unavailable")
	record := f.submit(t, f.host, nil)
	if status := acknowledged(t, f, suspendDuringDispatch(t, f, f.ctx, record), 15*time.Second); status != kgo.AckReject {
		t.Fatalf("status %v, want the run ended, not released; logs:\n%s", status, f.logs.String())
	}
	var terminals []wire.RunStatus
	for _, status := range f.eventStatuses(t) {
		if status.Wherry.Kind == "released" {
			t.Fatalf("the run published released; logs:\n%s", f.logs.String())
		}
		if status.Wherry.Kind == "terminal" {
			terminals = append(terminals, status)
		}
	}
	if len(terminals) != 1 || terminals[0].StatusUpdate.Status.State != wire.TaskStateFailed || terminals[0].StatusUpdate.Status.Message.Parts[0].Text != suspendStopUnconfirmed {
		t.Fatalf("terminals %+v, want one FAILED %s; logs:\n%s", terminals, suspendStopUnconfirmed, f.logs.String())
	}
	f.plugin.mu.Lock()
	stops, releases := f.plugin.stops, f.plugin.releases
	f.plugin.mu.Unlock()
	if stops < 2 || releases != 0 {
		t.Fatalf("stops %d and releases %d, want Stop retried and nothing released", stops, releases)
	}
	if !strings.Contains(f.logs.String(), "the job's stop is unconfirmed") {
		t.Fatalf("the unconfirmed stop is not logged; logs:\n%s", f.logs.String())
	}
	scraped(t, f.receiver, `wherry_suspend_stop_unconfirmed_total{pool="`+testPool+`"} 1`)
}

// endedUnconfirmed checks that the run ended with one FAILED job stop unconfirmed terminal, published no released
// status and released no material.
func endedUnconfirmed(t *testing.T, f fixture) {
	t.Helper()
	var terminals []wire.RunStatus
	for _, status := range f.eventStatuses(t) {
		if status.Wherry.Kind == "released" {
			t.Fatalf("the run published released; logs:\n%s", f.logs.String())
		}
		if status.Wherry.Kind == "terminal" {
			terminals = append(terminals, status)
		}
	}
	if len(terminals) != 1 || terminals[0].StatusUpdate.Status.State != wire.TaskStateFailed || terminals[0].StatusUpdate.Status.Message.Parts[0].Text != suspendStopUnconfirmed {
		t.Fatalf("terminals %+v, want one FAILED %s; logs:\n%s", terminals, suspendStopUnconfirmed, f.logs.String())
	}
	f.plugin.mu.Lock()
	releases := f.plugin.releases
	f.plugin.mu.Unlock()
	if releases != 0 {
		t.Fatalf("releases %d, want the material kept", releases)
	}
}

// A shutdown while a suspended run's Stop fails ends the run unconfirmed: its terminal committed past the shutdown and
// its task rejected, never released.
func TestShutdownDuringASuspendedStopEndsTheRunUnconfirmed(t *testing.T) {
	f := newFixture(t, resultStream)
	f.plugin.stopErr = errors.New("platform unavailable")
	record := f.submit(t, f.host, nil)
	ctx, shutdown := context.WithCancel(f.ctx)
	defer shutdown()
	done := suspendDuringDispatch(t, f, ctx, record)
	for {
		f.plugin.mu.Lock()
		stops := f.plugin.stops
		f.plugin.mu.Unlock()
		if stops > 0 {
			break
		}
		if f.ctx.Err() != nil {
			t.Fatalf("never stopped; logs:\n%s", f.logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	shutdown()
	if status := acknowledged(t, f, done, shutdownTimeout+5*time.Second); status != kgo.AckReject {
		t.Fatalf("status %v, want the task rejected; logs:\n%s", status, f.logs.String())
	}
	endedUnconfirmed(t, f)
}

// A Stop that hangs is bounded by the suspended stop's bound: the run ends unconfirmed within it.
func TestSuspendedStopThatHangsEndsUnconfirmedWithinTheBound(t *testing.T) {
	f := newFixture(t, resultStream)
	previous := suspendStopBound
	suspendStopBound = 2 * time.Second
	t.Cleanup(func() { suspendStopBound = previous })
	f.plugin.stopBlocks = true
	record := f.submit(t, f.host, nil)
	if status := acknowledged(t, f, suspendDuringDispatch(t, f, f.ctx, record), suspendStopBound+10*time.Second); status != kgo.AckReject {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	endedUnconfirmed(t, f)
}

// A suspension cuts an adopted run's open short where it blocks, in the listing of the pool events' end or in the open
// request's commit: the run ends FAILED pool suspended with its job stopped.
func TestSuspendCutsAnAdoptedRunsOpenShort(t *testing.T) {
	for _, test := range []struct {
		name string
		// block blocks the open until its context ends and sets blocked once it does.
		block func(t *testing.T, f fixture, blocked *atomic.Bool)
	}{
		{"offset listing", func(t *testing.T, f fixture, blocked *atomic.Bool) {
			topic := admin.PoolEventsTopic(testPool)
			f.cluster.ControlKey(int16(kmsg.ListOffsets), func(request kmsg.Request) (kmsg.Response, error, bool) {
				f.cluster.KeepControl()
				listed := request.(*kmsg.ListOffsetsRequest)
				if listed.IsolationLevel != 0 || len(listed.Topics) != 1 || listed.Topics[0].Topic != topic || listed.Topics[0].Partitions[0].Timestamp != -1 {
					return nil, nil, false
				}
				blocked.Store(true)
				response := listed.ResponseKind().(*kmsg.ListOffsetsResponse)
				failed := kmsg.ListOffsetsResponseTopic{Topic: topic}
				for _, partition := range listed.Topics[0].Partitions {
					failed.Partitions = append(failed.Partitions, kmsg.ListOffsetsResponseTopicPartition{Partition: partition.Partition, ErrorCode: kerr.OffsetNotAvailable.Code})
				}
				response.Topics = append(response.Topics, failed)
				return response, nil, true
			})
		}},
		{"open request commit", func(t *testing.T, f fixture, blocked *atomic.Bool) {
			previous := commitTransaction
			t.Cleanup(func() { commitTransaction = previous })
			// The adopter's commits are its claimed status, its adopting status, then its open request.
			commits := 0
			commitTransaction = func(ctx context.Context, producer *kgo.Client) error {
				if producer.OptValue(kgo.TransactionalID) == admin.TransactionalPrefix(testPool)+"run-a" {
					if commits++; commits == 3 {
						blocked.Store(true)
						<-ctx.Done()
						return ctx.Err()
					}
				}
				return previous(ctx, producer)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, record := adopting(t)
			f.quiet()
			blocked := &atomic.Bool{}
			test.block(t, f, blocked)
			done := f.executing(t, f.ctx, record, 2)
			for !blocked.Load() {
				if f.ctx.Err() != nil {
					t.Fatalf("the open never blocked; logs:\n%s", f.logs.String())
				}
				time.Sleep(20 * time.Millisecond)
			}
			f.suspend(t)
			if status := acknowledged(t, f, done, 15*time.Second); status != kgo.AckReject {
				t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
			}
			if len(f.openRequests(t)) != 0 {
				t.Fatal("the cut-short open request was committed")
			}
			endedSuspended(t, f, record, 1)
		})
	}
}

// A suspended open of an adopted run whose cut-short commit's abort blocks or fails leaves its task unacknowledged:
// no released status, no terminal and its job not stopped, for its lock to return the task after resume.
func TestSuspendedOpenWhoseAbortIsUnconfirmedIsNotAcknowledged(t *testing.T) {
	for _, test := range []struct {
		name  string
		abort func(f fixture, request *kmsg.EndTxnRequest) (kmsg.Response, error, bool)
	}{
		{"abort blocks", func(f fixture, request *kmsg.EndTxnRequest) (kmsg.Response, error, bool) {
			release := make(chan struct{})
			time.AfterFunc(shutdownTimeout+2*time.Second, func() { close(release) })
			f.cluster.SleepControl(func() { <-release })
			return nil, nil, false
		}},
		{"abort fails", func(f fixture, request *kmsg.EndTxnRequest) (kmsg.Response, error, bool) {
			response := request.ResponseKind().(*kmsg.EndTxnResponse)
			response.ErrorCode = kerr.UnknownServerError.Code
			return response, nil, true
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, record := adopting(t)
			f.quiet()
			transactionalID := admin.TransactionalPrefix(testPool) + "run-a"
			previous := commitTransaction
			t.Cleanup(func() { commitTransaction = previous })
			// The adopter's commits are its claimed status, its adopting status, then its open request.
			blocked, commits := &atomic.Bool{}, 0
			commitTransaction = func(ctx context.Context, producer *kgo.Client) error {
				if producer.OptValue(kgo.TransactionalID) == transactionalID {
					if commits++; commits == 3 {
						blocked.Store(true)
						<-ctx.Done()
						return ctx.Err()
					}
				}
				return previous(ctx, producer)
			}
			f.cluster.ControlKey(int16(kmsg.EndTxn), func(request kmsg.Request) (kmsg.Response, error, bool) {
				f.cluster.KeepControl()
				ended := request.(*kmsg.EndTxnRequest)
				if ended.TransactionalID != transactionalID || ended.Commit || !blocked.Load() {
					return nil, nil, false
				}
				return test.abort(f, ended)
			})
			done := f.executing(t, f.ctx, record, 2)
			for !blocked.Load() {
				if f.ctx.Err() != nil {
					t.Fatalf("the open never blocked; logs:\n%s", f.logs.String())
				}
				time.Sleep(20 * time.Millisecond)
			}
			f.suspend(t)
			if status := acknowledged(t, f, done, shutdownTimeout+10*time.Second); status != 0 {
				t.Fatalf("status %v, want no acknowledgement; logs:\n%s", status, f.logs.String())
			}
			for _, status := range f.eventStatuses(t) {
				if status.Wherry.Kind == "released" || status.Wherry.Kind == "terminal" {
					t.Fatalf("the run published %s; logs:\n%s", status.Wherry.Kind, f.logs.String())
				}
			}
			f.plugin.mu.Lock()
			stops := f.plugin.stops
			f.plugin.mu.Unlock()
			if stops != 0 {
				t.Fatalf("stops %d, want the job left to the redelivery's Lookup", stops)
			}
		})
	}
}

// A suspended run whose terminal commit fails or times out at a shutdown leaves its task unacknowledged: no
// rejection and no released status.
func TestSuspendedTerminalNotCommittedAtShutdownIsNotAcknowledged(t *testing.T) {
	for _, test := range []struct {
		name   string
		commit func(ctx context.Context) error
	}{
		{"commit fails", func(ctx context.Context) error { return errors.New("broker unavailable") }},
		{"commit times out", func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.plugin.stopErr = errors.New("platform unavailable")
			previous := commitTransaction
			t.Cleanup(func() { commitTransaction = previous })
			record := f.submit(t, f.host, nil)
			failing := &atomic.Bool{}
			commitTransaction = func(ctx context.Context, producer *kgo.Client) error {
				if failing.Load() && producer.OptValue(kgo.TransactionalID) == admin.TransactionalPrefix(testPool)+string(record.Key) {
					return test.commit(ctx)
				}
				return previous(ctx, producer)
			}
			ctx, shutdown := context.WithCancel(f.ctx)
			defer shutdown()
			done := suspendDuringDispatch(t, f, ctx, record)
			for {
				f.plugin.mu.Lock()
				stops := f.plugin.stops
				f.plugin.mu.Unlock()
				if stops > 0 {
					break
				}
				if f.ctx.Err() != nil {
					t.Fatalf("never stopped; logs:\n%s", f.logs.String())
				}
				time.Sleep(20 * time.Millisecond)
			}
			failing.Store(true)
			shutdown()
			if status := acknowledged(t, f, done, shutdownTimeout+readTimeout+5*time.Second); status != 0 {
				t.Fatalf("status %v, want no acknowledgement; logs:\n%s", status, f.logs.String())
			}
			for _, status := range f.eventStatuses(t) {
				if status.Wherry.Kind == "released" || status.Wherry.Kind == "terminal" {
					t.Fatalf("the run published %s; logs:\n%s", status.Wherry.Kind, f.logs.String())
				}
			}
		})
	}
}

// latched waits until a run the receiver holds has read its pool suspended.
func latched(t *testing.T, f fixture) {
	t.Helper()
	for {
		f.receiver.liveMu.Lock()
		seen := false
		for run := range f.receiver.live {
			seen = seen || run.suspendSeen.Load()
		}
		f.receiver.liveMu.Unlock()
		if seen {
			return
		}
		if f.ctx.Err() != nil {
			t.Fatalf("the suspension was never read; logs:\n%s", f.logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// endedSuspendedAtShutdown checks that a run its shutdown found suspended was rejected, so only after its terminal
// committed, published no released status, and ended FAILED pool suspended with its job stopped.
func endedSuspendedAtShutdown(t *testing.T, f fixture, record *kgo.Record, done <-chan kgo.AckStatus) {
	t.Helper()
	if status := acknowledged(t, f, done, shutdownTimeout+10*time.Second); status != kgo.AckReject {
		t.Fatalf("status %v, want the task rejected; logs:\n%s", status, f.logs.String())
	}
	for _, status := range f.eventStatuses(t) {
		if status.Wherry.Kind == "released" {
			t.Fatalf("the run published released; logs:\n%s", f.logs.String())
		}
	}
	endedSuspended(t, f, record, 1)
}

// A shutdown of a following run whose pool is suspended, before the run's own check reads it, ends the run suspended:
// its job stopped and its task rejected once the terminal commits, never released. The suspension is visible at the
// shutdown, or was read by the run's watch while a progress commit held the run.
func TestShutdownOfAFollowingRunAfterASuspensionEndsItSuspended(t *testing.T) {
	for _, test := range []struct {
		name string
		// block blocks the run's progress commit until down ends, or does nothing.
		block   func(t *testing.T, f fixture, record *kgo.Record, down context.Context) *atomic.Bool
		latched bool
	}{
		{"visible at shutdown", func(t *testing.T, f fixture, record *kgo.Record, down context.Context) *atomic.Bool {
			for interval, long := range map[*time.Duration]time.Duration{&stateCheckInterval: time.Minute, &renewInterval: time.Minute} {
				previous := *interval
				*interval = long
				t.Cleanup(func() { *interval = previous })
			}
			return nil
		}, false},
		{"latched during a progress commit", func(t *testing.T, f fixture, record *kgo.Record, down context.Context) *atomic.Bool {
			blocking := &atomic.Bool{}
			previous := commitTransaction
			t.Cleanup(func() { commitTransaction = previous })
			commitTransaction = func(ctx context.Context, producer *kgo.Client) error {
				if producer.OptValue(kgo.TransactionalID) == admin.TransactionalPrefix(testPool)+string(record.Key) && blocking.CompareAndSwap(true, false) {
					<-down.Done()
					return down.Err()
				}
				return previous(ctx, producer)
			}
			return blocking
		}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, append(slices.Clone(noResultStream), noResultStream...))
			f.plugin.endDelay = 30 * time.Second
			record := f.submit(t, f.host, nil)
			down, shutdown := context.WithCancel(f.ctx)
			defer shutdown()
			blocking := test.block(t, f, record, down)
			done := f.executing(t, down, record, 1)
			for {
				f.plugin.mu.Lock()
				delivered := f.plugin.delivered
				f.plugin.mu.Unlock()
				if delivered == 2 && slices.Contains(f.kinds(t), "started") {
					break
				}
				if f.ctx.Err() != nil {
					t.Fatalf("the run never followed its job; logs:\n%s", f.logs.String())
				}
				time.Sleep(20 * time.Millisecond)
			}
			if blocking != nil {
				blocking.Store(true)
				for blocking.Load() {
					if f.ctx.Err() != nil {
						t.Fatalf("no progress commit; logs:\n%s", f.logs.String())
					}
					time.Sleep(20 * time.Millisecond)
				}
			}
			f.suspend(t)
			if test.latched {
				latched(t, f)
			}
			shutdown()
			endedSuspendedAtShutdown(t, f, record, done)
		})
	}
}

// A shutdown of an adopted run's open after its pool is suspended ends the run suspended, not released: the shutdown
// arrives after the run's watch read the suspension while the open request's commit held the run, or while the run
// waits for its answer with the suspension visible.
func TestShutdownOfAnOpenAfterASuspensionEndsItSuspended(t *testing.T) {
	for _, test := range []struct {
		name string
		// block holds the run in its open until down ends; it returns when the run is held there, and when, once
		// suspended, it is ready for the shutdown.
		block func(t *testing.T, f fixture, down context.Context) (held, ready func() bool)
	}{
		{"latched during the request's commit", func(t *testing.T, f fixture, down context.Context) (func() bool, func() bool) {
			previous := commitTransaction
			t.Cleanup(func() { commitTransaction = previous })
			// The adopter's commits are its claimed status, its adopting status, then its open request, whose context
			// the run's watch cuts once it reads the suspension.
			blocked, cut, commits := &atomic.Bool{}, &atomic.Bool{}, 0
			commitTransaction = func(ctx context.Context, producer *kgo.Client) error {
				if producer.OptValue(kgo.TransactionalID) == admin.TransactionalPrefix(testPool)+"run-a" {
					if commits++; commits == 3 {
						blocked.Store(true)
						<-ctx.Done()
						cut.Store(down.Err() == nil)
						<-down.Done()
						return ctx.Err()
					}
				}
				return previous(ctx, producer)
			}
			return blocked.Load, cut.Load
		}},
		{"visible awaiting the answer", func(t *testing.T, f fixture, down context.Context) (func() bool, func() bool) {
			previous := stateCheckInterval
			stateCheckInterval = time.Minute
			t.Cleanup(func() { stateCheckInterval = previous })
			return func() bool { return len(f.openRequests(t)) == 1 }, func() bool { return true }
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, record := adopting(t)
			f.quiet()
			down, shutdown := context.WithCancel(f.ctx)
			defer shutdown()
			held, ready := test.block(t, f, down)
			done := f.executing(t, down, record, 2)
			for !held() {
				if f.ctx.Err() != nil {
					t.Fatalf("the open never blocked; logs:\n%s", f.logs.String())
				}
				time.Sleep(20 * time.Millisecond)
			}
			f.suspend(t)
			for !ready() {
				if f.ctx.Err() != nil {
					t.Fatalf("the suspension was never read; logs:\n%s", f.logs.String())
				}
				time.Sleep(20 * time.Millisecond)
			}
			shutdown()
			endedSuspendedAtShutdown(t, f, record, done)
		})
	}
}

// A shutdown of a run still looking its job up after its pool is suspended ends the run unconfirmed, not released.
func TestShutdownOfARunLookingUpItsJobAfterASuspensionIsNotReleased(t *testing.T) {
	f := newFixture(t, resultStream)
	f.plugin.lookupErr = errors.New("platform unavailable")
	record := f.submit(t, f.host, nil)
	down, shutdown := context.WithCancel(f.ctx)
	defer shutdown()
	done := f.executing(t, down, record, 2)
	for {
		if _, lookups, _, _ := f.plugin.counts(); lookups > 0 {
			break
		}
		if f.ctx.Err() != nil {
			t.Fatalf("the job was never looked up; logs:\n%s", f.logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	f.suspend(t)
	shutdown()
	if status := acknowledged(t, f, done, shutdownTimeout+10*time.Second); status != kgo.AckReject {
		t.Fatalf("status %v, want the task rejected; logs:\n%s", status, f.logs.String())
	}
	endedUnconfirmed(t, f)
}
