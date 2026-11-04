package receiver

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// heldKeys reads the pool key files in r's state directory without settling anything.
func heldKeys(t *testing.T, r *Receiver) map[string]*poolKey {
	t.Helper()
	entries, err := os.ReadDir(r.poolKeysDir())
	if errors.Is(err, os.ErrNotExist) {
		return map[string]*poolKey{}
	}
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]*poolKey{}
	for _, entry := range entries {
		id, ok := strings.CutSuffix(entry.Name(), ".json")
		if !ok || strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		encoded, err := os.ReadFile(filepath.Join(r.poolKeysDir(), entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		key := &poolKey{}
		if err := json.Unmarshal(encoded, key); err != nil {
			t.Fatal(err)
		}
		keys[id] = key
	}
	return keys
}

// snapshotRecords returns the snapshot topic's committed records in log order.
func (f fixture) snapshotRecords(t *testing.T) []*kgo.Record {
	t.Helper()
	return f.records(t, operator.CustodianTopic(testPool))
}

// parseSnapshot verifies record against writer's signing key.
func parseSnapshot(t *testing.T, record *kgo.Record, writer string, key ed25519.PrivateKey) wire.Snapshot {
	t.Helper()
	snapshot, err := wire.ParseSnapshot(record.Value, signed.Keys{writer: {Current: key.Public().(ed25519.PublicKey)}})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

// member provisions, registers and configures another receiver of the pool on the fixture's cluster.
func (f fixture) member(t *testing.T, id string) *Receiver {
	t.Helper()
	r, _ := f.newReceiver(t, testConfig(id))
	r.logger = f.receiver.logger
	if err := r.register(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.configure(f.ctx); err != nil {
		t.Fatal(err)
	}
	return r
}

// restarted is r started again as a new process: its own Kafka client, its signing key and state directory.
func restarted(t *testing.T, r *Receiver) *Receiver {
	t.Helper()
	base, err := kgo.NewClient(r.base.Opts()...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(base.Close)
	return &Receiver{id: r.id, base: base, reader: operator.NewReader(base), signing: r.signing, stateDir: r.stateDir, drainTimeout: r.drainTimeout, factory: r.factory, logger: r.logger, metrics: newMetrics(prometheus.NewRegistry())}
}

// runUntilStopped runs r and returns a function that stops it and waits for Run to return.
func runUntilStopped(t *testing.T, ctx context.Context, r *Receiver) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = r.Run(ctx)
	}()
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(20 * time.Second):
			t.Fatal("Run did not return")
		}
	}
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return stop
}

// awaitSnapshots waits up to 15 seconds until the snapshot topic holds n committed records.
func (f fixture) awaitSnapshots(t *testing.T, n int) []*kgo.Record {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if records := f.snapshotRecords(t); len(records) >= n {
			return records
		}
		if time.Now().After(deadline) {
			t.Fatalf("fewer than %d snapshots; logs:\n%s", n, f.logs.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// custodian makes r act as custodian, as the group's assignment does, and tends it at now.
func custodian(t *testing.T, ctx context.Context, r *Receiver, now time.Time) {
	t.Helper()
	r.assign(true)
	if err := r.tend(ctx, now); err != nil {
		t.Fatal(err)
	}
}

// describeKeys names each key by its ID and custody record, never its private material.
func describeKeys(keys map[string]*poolKey) string {
	var described []string
	for id, key := range keys {
		if key == nil {
			described = append(described, id+" absent")
			continue
		}
		described = append(described, fmt.Sprintf("%s created_ms=%d published_ms=%d deadline_ms=%d", id, key.CreatedMS, key.PublishedMS, key.DeadlineMS))
	}
	return "[" + strings.Join(described, ", ") + "]"
}

func onlyKey(t *testing.T, r *Receiver) (string, *poolKey) {
	t.Helper()
	keys := heldKeys(t, r)
	if len(keys) != 1 {
		t.Fatalf("held %d keys, want 1", len(keys))
	}
	for id, key := range keys {
		return id, key
	}
	return "", nil
}

func TestFirstReceiverBootstrapsThePool(t *testing.T) {
	f := newBareFixture(t)
	// At the commit the key is on disk and not yet marked published.
	var atCommit map[string]*poolKey
	f.cluster.ControlKey(kmsg.EndTxn.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		if request.(*kmsg.EndTxnRequest).TransactionalID == "p."+testPool+".custodian" && atCommit == nil {
			atCommit = heldKeys(t, f.receiver)
		}
		return nil, nil, false
	})
	runUntilStopped(t, f.ctx, f.receiver)
	records := f.awaitSnapshots(t, 1)
	snapshot := parseSnapshot(t, records[0], testReceiver, f.receiver.signing)
	if snapshot.Reason != "bootstrap" || snapshot.Custodian != testReceiver || snapshot.Pool != testPool {
		t.Fatalf("snapshot = %+v, want a bootstrap naming %s", snapshot, testReceiver)
	}
	if len(atCommit) != 1 || atCommit[snapshot.Current.KeyID] == nil || atCommit[snapshot.Current.KeyID].PublishedMS != 0 {
		t.Fatalf("keys at the commit = %s, want the snapshot's key %s persisted unmarked", describeKeys(atCommit), snapshot.Current.KeyID)
	}
	for {
		if id, key := onlyKey(t, f.receiver); id == snapshot.Current.KeyID && key.PublishedMS == records[0].Timestamp.UnixMilli() {
			break
		}
		if f.ctx.Err() != nil {
			t.Fatal("key never marked published")
		}
		time.Sleep(20 * time.Millisecond)
	}
	info, err := os.Stat(filepath.Join(f.receiver.poolKeysDir(), snapshot.Current.KeyID+".json"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("key file: %v %v, want mode 0600", info, err)
	}
	if got := scrapedValue(t, f.receiver, `wherry_custodian_takeovers_total{pool="pool-a",reason="takeover"}`); got != 0 {
		t.Fatalf("takeovers = %v after a bootstrap, want 0", got)
	}
}

func TestRestartInsideSessionTimeoutContinuesWithItsKey(t *testing.T) {
	f := newBareFixture(t)
	stop := runUntilStopped(t, f.ctx, f.receiver)
	f.awaitSnapshots(t, 1)
	id, _ := onlyKey(t, f.receiver)
	stop()
	again := restarted(t, f.receiver)
	runUntilStopped(t, f.ctx, again)
	for {
		again.custody.mu.Lock()
		acting := again.custody.producer != nil
		again.custody.mu.Unlock()
		if acting {
			break
		}
		if f.ctx.Err() != nil {
			t.Fatalf("restart never acted as custodian; logs:\n%s", f.logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(5 * custodyInterval)
	if records := f.snapshotRecords(t); len(records) != 1 {
		t.Fatalf("snapshots = %d after the restart, want 1", len(records))
	}
	if got, _ := onlyKey(t, again); got != id {
		t.Fatalf("key = %s after the restart, want %s", got, id)
	}
}

func TestSameReceiverWithoutItsKeysTakesOver(t *testing.T) {
	f := newBareFixture(t)
	stop := runUntilStopped(t, f.ctx, f.receiver)
	f.awaitSnapshots(t, 1)
	lost, _ := onlyKey(t, f.receiver)
	stop()
	if err := os.RemoveAll(f.receiver.poolKeysDir()); err != nil {
		t.Fatal(err)
	}
	again := restarted(t, f.receiver)
	runUntilStopped(t, f.ctx, again)
	records := f.awaitSnapshots(t, 2)
	snapshot := parseSnapshot(t, records[1], testReceiver, f.receiver.signing)
	if snapshot.Reason != "takeover" || snapshot.Current.KeyID == lost {
		t.Fatalf("snapshot = %+v, want a takeover with a new key", snapshot)
	}
	scraped(t, again, `wherry_custodian_takeovers_total{pool="pool-a",reason="takeover"} 1`)
	if !strings.Contains(f.logs.String(), "took over pool "+testPool+" (WherryCustodianTakeover)") {
		t.Fatalf("takeover not logged; logs:\n%s", f.logs.String())
	}
}

func TestSuccessorFencesTheOldCustodian(t *testing.T) {
	f := newBareFixture(t)
	now := time.Now()
	custodian(t, f.ctx, f.receiver, now)
	successor := f.member(t, "receiver-b")
	custodian(t, f.ctx, successor, now)
	records := f.snapshotRecords(t)
	if len(records) != 2 || parseSnapshot(t, records[1], "receiver-b", successor.signing).Reason != "takeover" {
		t.Fatalf("snapshots = %d, want the bootstrap and the successor's takeover", len(records))
	}
	// The old custodian still holds its producer and is still assigned; its next write fails at the broker.
	if err := f.receiver.tend(f.ctx, now); err == nil {
		t.Fatal("the old custodian's write succeeded after the successor initialised the transactional ID")
	}
	if f.receiver.custody.producer != nil {
		t.Fatal("the fenced custodian kept its producer")
	}
	if records := f.snapshotRecords(t); len(records) != 2 {
		t.Fatalf("snapshots = %d, want the fenced write absent", len(records))
	}
}

func TestRotationFiresADayAndAJitterAfterTheSnapshot(t *testing.T) {
	f := newBareFixture(t)
	custodian(t, f.ctx, f.receiver, time.Now())
	first := f.snapshotRecords(t)[0]
	id, _ := onlyKey(t, f.receiver)
	due := rotationAt(first.Timestamp, testPool, id)
	if jitter := due.Sub(first.Timestamp) - rotationInterval; jitter < 0 || jitter >= rotationJitter {
		t.Fatalf("jitter = %v, want within [0, %v)", jitter, rotationJitter)
	}
	if err := f.receiver.tend(f.ctx, due.Add(-time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	// A restart just before the rotation keeps its time.
	again := restarted(t, f.receiver)
	if err := again.configure(f.ctx); err != nil {
		t.Fatal(err)
	}
	custodian(t, f.ctx, again, due.Add(-time.Millisecond))
	if records := f.snapshotRecords(t); len(records) != 1 {
		t.Fatalf("snapshots = %d before the rotation is due, want 1", len(records))
	}
	if err := again.tend(f.ctx, due); err != nil {
		t.Fatal(err)
	}
	records := f.snapshotRecords(t)
	if len(records) != 2 || parseSnapshot(t, records[1], testReceiver, f.receiver.signing).Reason != "rotation" {
		t.Fatalf("snapshots = %d, want a rotation once due", len(records))
	}
}

// rotate tends r at the rotation time of the current snapshot and returns the new snapshot's record.
func rotate(t *testing.T, f fixture, r *Receiver) *kgo.Record {
	t.Helper()
	records := f.snapshotRecords(t)
	latest := records[len(records)-1]
	snapshot := parseSnapshot(t, latest, r.id, r.signing)
	if err := r.tend(f.ctx, rotationAt(latest.Timestamp, testPool, snapshot.Current.KeyID)); err != nil {
		t.Fatal(err)
	}
	records = f.snapshotRecords(t)
	if parseSnapshot(t, records[len(records)-1], r.id, r.signing).Reason != "rotation" {
		t.Fatal("no rotation")
	}
	return records[len(records)-1]
}

func TestSecondRotationKeepsBothEarlierKeys(t *testing.T) {
	f := newBareFixture(t)
	custodian(t, f.ctx, f.receiver, time.Now())
	first, _ := onlyKey(t, f.receiver)
	second := rotate(t, f, f.receiver)
	third := rotate(t, f, f.receiver)
	window := time.Duration(operator.DefaultStartByLimitMS)*time.Millisecond + keyGrace
	keys := heldKeys(t, f.receiver)
	secondID := parseSnapshot(t, second, testReceiver, f.receiver.signing).Current.KeyID
	thirdID := parseSnapshot(t, third, testReceiver, f.receiver.signing).Current.KeyID
	if len(keys) != 3 || keys[first] == nil || keys[secondID] == nil || keys[thirdID] == nil {
		t.Fatalf("held %d keys, want the current key and both replaced keys", len(keys))
	}
	if keys[first].DeadlineMS != second.Timestamp.Add(window).UnixMilli() || keys[secondID].DeadlineMS != third.Timestamp.Add(window).UnixMilli() || keys[thirdID].DeadlineMS != 0 {
		t.Fatalf("deadlines = %d, %d, %d, want each replacing snapshot's append time + start_by_limit + 7h, and none for the current key",
			keys[first].DeadlineMS, keys[secondID].DeadlineMS, keys[thirdID].DeadlineMS)
	}
}

func TestReplacedKeyIsDeletedOnlyAtItsDeadline(t *testing.T) {
	f := newBareFixture(t)
	custodian(t, f.ctx, f.receiver, time.Now())
	first, _ := onlyKey(t, f.receiver)
	rotation := rotate(t, f, f.receiver)
	deadline := rotation.Timestamp.Add(time.Duration(operator.DefaultStartByLimitMS)*time.Millisecond + keyGrace)
	if key := heldKeys(t, f.receiver)[first]; key == nil || key.DeadlineMS != deadline.UnixMilli() {
		t.Fatalf("replaced key = %s, want it held with the rotation's append time + start_by_limit + 7h", describeKeys(map[string]*poolKey{first: key}))
	}
	if err := f.receiver.tend(f.ctx, deadline.Add(-time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if heldKeys(t, f.receiver)[first] == nil {
		t.Fatal("replaced key deleted before its deadline")
	}
	if err := f.receiver.tend(f.ctx, deadline); err != nil {
		t.Fatal(err)
	}
	if heldKeys(t, f.receiver)[first] != nil {
		t.Fatal("replaced key kept past its deadline")
	}
}

// writeKey persists a P-256 pool key in r's state directory as a crash would have left it.
func writeKey(t *testing.T, r *Receiver, key *poolKey) string {
	t.Helper()
	private, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	key.Key = jose.JSONWebKey{Key: private}
	if err := os.MkdirAll(r.poolKeysDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := key.save(r.poolKeysDir()); err != nil {
		t.Fatal(err)
	}
	return key.id()
}

func TestUnmarkedKeyNamedByTheCurrentSnapshotIsMarkedAndKeptAsCurrent(t *testing.T) {
	f := newBareFixture(t)
	custodian(t, f.ctx, f.receiver, time.Now())
	id, key := onlyKey(t, f.receiver)
	record := f.awaitSnapshots(t, 1)[0]
	// A crash after the snapshot committed and before its key was marked, the key created a week before.
	key.CreatedMS, key.PublishedMS = time.Now().Add(-7*24*time.Hour).UnixMilli(), 0
	if err := key.save(f.receiver.poolKeysDir()); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.tend(f.ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if key := heldKeys(t, f.receiver)[id]; key == nil || key.PublishedMS != record.Timestamp.UnixMilli() || key.DeadlineMS != 0 {
		t.Fatalf("key = %s, want it marked published at the snapshot's append time with no deadline", describeKeys(map[string]*poolKey{id: key}))
	}
	if records := f.snapshotRecords(t); len(records) != 1 {
		t.Fatalf("snapshots = %d, want the key kept as current", len(records))
	}
}

func TestUnmarkedKeyNotNamedGetsItsDeadlineFromTheLaterSnapshot(t *testing.T) {
	f := newBareFixture(t)
	// A crash between persisting the key and marking it, two days before; another receiver's snapshot is current.
	crashed := writeKey(t, f.receiver, &poolKey{CreatedMS: time.Now().Add(-48 * time.Hour).UnixMilli()})
	other := f.member(t, "receiver-b")
	custodian(t, f.ctx, other, time.Now())
	record := f.awaitSnapshots(t, 1)[0]
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	deadline := record.Timestamp.Add(time.Duration(operator.DefaultStartByLimitMS)*time.Millisecond + keyGrace)
	for _, now := range []time.Time{time.Now(), deadline.Add(-time.Millisecond)} {
		if err := f.receiver.tend(f.ctx, now); err != nil {
			t.Fatal(err)
		}
		if key := heldKeys(t, f.receiver)[crashed]; key == nil || key.PublishedMS != 0 || key.DeadlineMS != deadline.UnixMilli() {
			t.Fatalf("unmarked key = %s, want it kept unmarked to the snapshot's append time + start_by_limit + 7h",
				describeKeys(map[string]*poolKey{crashed: key}))
		}
	}
	if err := f.receiver.tend(f.ctx, deadline); err != nil {
		t.Fatal(err)
	}
	if heldKeys(t, f.receiver)[crashed] != nil {
		t.Fatal("unmarked key kept past its deadline")
	}
}

func TestNoKeyIsDeletedWhileTheSnapshotTopicCannotBeRead(t *testing.T) {
	f := newBareFixture(t)
	custodian(t, f.ctx, f.receiver, time.Now())
	current, _ := onlyKey(t, f.receiver)
	long := time.Now().Add(-30 * 24 * time.Hour).UnixMilli()
	expired := writeKey(t, f.receiver, &poolKey{CreatedMS: long, PublishedMS: long, DeadlineMS: time.Now().Add(-time.Hour).UnixMilli()})
	unmarked := writeKey(t, f.receiver, &poolKey{CreatedMS: long})
	held := func(when string) {
		t.Helper()
		keys := heldKeys(t, f.receiver)
		if keys[current] == nil || keys[expired] == nil || keys[unmarked] == nil || keys[unmarked].DeadlineMS != 0 {
			t.Fatalf("%s: keys = %s, want every key kept as it was", when, describeKeys(keys))
		}
	}
	failed, cancel := context.WithCancel(f.ctx)
	cancel()
	if err := f.receiver.tend(failed, time.Now()); err == nil {
		t.Fatal("tended with the snapshot topic unread")
	}
	held("read error")
	if err := f.receiver.tend(f.ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if keys := heldKeys(t, f.receiver); keys[expired] != nil || keys[unmarked] == nil || keys[unmarked].DeadlineMS == 0 {
		t.Fatalf("keys = %s after a read, want the expired key deleted and the unmarked key given a deadline", describeKeys(keys))
	}
}

// The snapshot read returns the latest committed record while another writer's transaction is open on the snapshot
// topic, and a custody pass goes on from it.
func TestSnapshotReadReturnsTheLatestCommittedRecordWhileATransactionIsOpen(t *testing.T) {
	f := newBareFixture(t)
	custodian(t, f.ctx, f.receiver, time.Now())
	current, _ := onlyKey(t, f.receiver)
	open, err := f.receiver.newClient(kgo.TransactionalID(operator.TransactionalPrefix(testPool) + "open"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(open.Close)
	if err := open.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := open.ProduceSync(f.ctx, &kgo.Record{Topic: operator.CustodianTopic(testPool), Key: []byte(wire.CustodianKey), Value: []byte("pending")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	latest, err := f.receiver.readCurrent(f.ctx)
	if err != nil || !latest.names(f.receiver) || latest.snapshot.Current.KeyID != current {
		t.Fatalf("read %+v, %v behind an open transaction, want the committed snapshot naming key %s", latest.snapshot, err, current)
	}
	if err := f.receiver.tend(f.ctx, time.Now()); err != nil {
		t.Fatalf("tend behind an open transaction: %v", err)
	}
}

// TestCustodyDenialStopsTheReceiver takes away an access only the receiver's key custody uses: Run returns the denial.
func TestCustodyDenialStopsTheReceiver(t *testing.T) {
	for name, c := range map[string]struct {
		denied *kadm.ACLBuilder
		want   string
	}{
		"transactional ID init": {kadm.NewACLs().TransactionalIDs(operator.TransactionalPrefix(testPool)).Operations(kadm.OpAny), "denied transactional ID: "},
		"snapshot read":         {kadm.NewACLs().Topics(operator.CustodianTopic(testPool)).Operations(kadm.OpRead), "denied read: "},
		"snapshot transaction":  {kadm.NewACLs().Topics(operator.CustodianTopic(testPool)).Operations(kadm.OpWrite), "denied write: "},
		"custodian group":       {kadm.NewACLs().Groups(operator.CustodianGroup(testPool)).Operations(kadm.OpAny), "denied share group: "},
	} {
		t.Run(name, func(t *testing.T) {
			f := newBareFixture(t)
			f.deleteReceiverACLs(t, c.denied)
			if err := f.stopped(t, f.run(t, f.ctx), 20*time.Second); err == nil || !strings.HasPrefix(err.Error(), c.want) {
				t.Fatalf("Run returned %v, want %q", err, c.want)
			}
		})
	}
}

func TestMarkedNonCurrentKeyGetsADeadlineOnStart(t *testing.T) {
	f := newBareFixture(t)
	retired := writeKey(t, f.receiver, &poolKey{CreatedMS: time.Now().Add(-48 * time.Hour).UnixMilli(), PublishedMS: time.Now().Add(-48 * time.Hour).UnixMilli()})
	now := time.Now()
	if err := f.receiver.tend(f.ctx, now); err != nil {
		t.Fatal(err)
	}
	want := now.Add(time.Duration(operator.DefaultStartByLimitMS)*time.Millisecond + keyGrace).UnixMilli()
	if got := heldKeys(t, f.receiver)[retired].DeadlineMS; got != want {
		t.Fatalf("deadline = %d, want now + start_by_limit + 7h = %d", got, want)
	}
}

func TestSigningKeyRotationRepublishesTheSnapshot(t *testing.T) {
	f := newBareFixture(t)
	custodian(t, f.ctx, f.receiver, time.Now())
	id, _ := onlyKey(t, f.receiver)
	time.Sleep(5 * time.Millisecond)
	_, next, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	f.produce(t, operator.ReceiverTopic(testReceiver), testReceiver, must(t)(signed.RotateKey(testReceiver, f.receiver.signing, next)))
	f.receiver.signing = next
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.tend(f.ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	records := f.snapshotRecords(t)
	if len(records) != 2 {
		t.Fatalf("snapshots = %d, want the snapshot republished", len(records))
	}
	if snapshot := parseSnapshot(t, records[1], testReceiver, next); snapshot.Current.KeyID != id || snapshot.Reason != "bootstrap" {
		t.Fatalf("republished snapshot = %+v, want the same key and reason", snapshot)
	}
}

func TestUnassignedReceiverNeverWritesASnapshot(t *testing.T) {
	f := newBareFixture(t)
	runUntilStopped(t, f.ctx, f.receiver)
	f.awaitSnapshots(t, 1)
	other := f.member(t, "receiver-b")
	writeKey(t, other, &poolKey{CreatedMS: time.Now().UnixMilli(), PublishedMS: time.Now().Add(-48 * time.Hour).UnixMilli()})
	runUntilStopped(t, f.ctx, other)
	time.Sleep(10 * custodyInterval)
	for _, record := range f.snapshotRecords(t) {
		if _, err := wire.ParseSnapshot(record.Value, signed.Keys{testReceiver: {Current: f.receiver.signing.Public().(ed25519.PublicKey)}}); err != nil {
			t.Fatalf("a snapshot not written by the assigned receiver: %v", err)
		}
	}
	other.custody.mu.Lock()
	defer other.custody.mu.Unlock()
	if other.custody.assigned || other.custody.producer != nil {
		t.Fatal("the unassigned receiver acts as custodian")
	}
}

// rotateSigningKey rotates r's signing key on its key topic and folds it in.
func rotateSigningKey(t *testing.T, f fixture, r *Receiver) {
	t.Helper()
	time.Sleep(5 * time.Millisecond)
	_, next, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	f.produce(t, operator.ReceiverTopic(r.id), r.id, must(t)(signed.RotateKey(r.id, r.signing, next)))
	r.signing = next
	if err := r.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
}

func takeoverLines(f fixture) int {
	return strings.Count(f.logs.String(), "took over pool "+testPool+" (WherryCustodianTakeover)")
}

func TestTakeoverIsCountedOnceAcrossARepublish(t *testing.T) {
	f := newBareFixture(t)
	custodian(t, f.ctx, f.receiver, time.Now())
	successor := f.member(t, "receiver-b")
	custodian(t, f.ctx, successor, time.Now())
	rotateSigningKey(t, f, successor)
	if err := successor.tend(f.ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	records := f.snapshotRecords(t)
	if len(records) != 3 || parseSnapshot(t, records[2], "receiver-b", successor.signing).Reason != "takeover" {
		t.Fatalf("snapshots = %d, want the bootstrap, the takeover and its republish", len(records))
	}
	if got := scrapedValue(t, successor, `wherry_custodian_takeovers_total{pool="pool-a",reason="takeover"}`); got != 1 || takeoverLines(f) != 1 {
		t.Fatalf("takeovers counted %v and logged %d times, want once", got, takeoverLines(f))
	}
}

func TestSupersededReceiverLeavesTheCustodianUnfenced(t *testing.T) {
	f := newBareFixture(t)
	custodian(t, f.ctx, f.receiver, time.Now())
	successor := f.member(t, "receiver-b")
	old := f.receiver.custody.producer
	previous := commitTransaction
	t.Cleanup(func() { commitTransaction = previous })
	landed := false
	// The successor's takeover lands while the old custodian commits, and the old custodian's commit then fails.
	commitTransaction = func(ctx context.Context, producer *kgo.Client) error {
		if producer != old {
			return previous(ctx, producer)
		}
		if !landed {
			landed = true
			custodian(t, f.ctx, successor, time.Now())
		}
		return errors.New("commit failed")
	}
	id, key := onlyKey(t, f.receiver)
	if err := f.receiver.tend(f.ctx, rotationAt(time.UnixMilli(key.PublishedMS), testPool, id)); err == nil {
		t.Fatal("the failed rotation succeeded")
	}
	if !f.receiver.custody.fenced || f.receiver.custody.producer != nil {
		t.Fatalf("fenced=%v producer=%v, want the old custodian superseded", f.receiver.custody.fenced, f.receiver.custody.producer != nil)
	}
	// Still assigned in its own view, it neither initialises the transactional ID again nor writes.
	if err := f.receiver.tend(f.ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if records := f.snapshotRecords(t); len(records) != 2 {
		t.Fatalf("snapshots = %d, want the bootstrap and the successor's takeover", len(records))
	}
	rotate(t, f, successor)
}

func TestTransientCommitErrorRetriesWithoutInitialisingAgain(t *testing.T) {
	f := newBareFixture(t)
	var inits atomic.Int32
	f.cluster.ControlKey(kmsg.InitProducerID.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		if id := request.(*kmsg.InitProducerIDRequest).TransactionalID; id != nil && *id == "p."+testPool+".custodian" {
			inits.Add(1)
		}
		return nil, nil, false
	})
	previous := commitTransaction
	t.Cleanup(func() { commitTransaction = previous })
	failed := false
	commitTransaction = func(ctx context.Context, producer *kgo.Client) error {
		if !failed {
			failed = true
			return errors.New("transient commit error")
		}
		return previous(ctx, producer)
	}
	custodian(t, f.ctx, f.receiver, time.Now())
	if records := f.snapshotRecords(t); len(records) != 1 || !failed {
		t.Fatalf("snapshots = %d, want the bootstrap committed on a retry", len(records))
	}
	if inits.Load() != 1 {
		t.Fatalf("%d initialisations of the custodian transactional ID, want 1", inits.Load())
	}
}

func TestRepublishKeepsTheRotationTime(t *testing.T) {
	f := newBareFixture(t)
	custodian(t, f.ctx, f.receiver, time.Now())
	id, key := onlyKey(t, f.receiver)
	// The key was published an hour before the republish.
	key.PublishedMS = time.UnixMilli(key.PublishedMS).Add(-time.Hour).UnixMilli()
	if err := key.save(f.receiver.poolKeysDir()); err != nil {
		t.Fatal(err)
	}
	rotateSigningKey(t, f, f.receiver)
	if err := f.receiver.tend(f.ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if records := f.snapshotRecords(t); len(records) != 2 {
		t.Fatalf("snapshots = %d, want the snapshot republished", len(records))
	}
	if got := heldKeys(t, f.receiver)[id].PublishedMS; got != key.PublishedMS {
		t.Fatalf("published_ms = %d after the republish, want %d", got, key.PublishedMS)
	}
	if err := f.receiver.tend(f.ctx, rotationAt(time.UnixMilli(key.PublishedMS), testPool, id)); err != nil {
		t.Fatal(err)
	}
	records := f.snapshotRecords(t)
	if len(records) != 3 || parseSnapshot(t, records[2], testReceiver, f.receiver.signing).Reason != "rotation" {
		t.Fatalf("snapshots = %d, want a rotation at the key's publication + 24h + jitter", len(records))
	}
}

func TestRevokedMemberNeverActsAsCustodian(t *testing.T) {
	f := newBareFixture(t)
	custodian(t, f.ctx, f.receiver, time.Now())
	f.receiver.mu.Lock()
	f.receiver.state.Ended[operator.ReceiverPrincipal(testReceiver)] = true
	f.receiver.mu.Unlock()
	for range 2 {
		f.receiver.assign(true)
		if err := f.receiver.tend(f.ctx, time.Now()); err != nil {
			t.Fatal(err)
		}
		if f.receiver.custody.producer != nil {
			t.Fatal("the revoked member holds the custodian producer")
		}
	}
	if records := f.snapshotRecords(t); len(records) != 1 {
		t.Fatalf("snapshots = %d, want the revoked member to write none", len(records))
	}
}

// A custodian revoked while it holds the custodian group's assignment leaves the group at once, well within the session
// timeout a closed static member would otherwise keep the partition for.
func TestRevokedCustodianLeavesTheGroup(t *testing.T) {
	f := newBareFixture(t)
	custodyUntilDrained(t, f.ctx, f.receiver)
	awaitActing(t, f, f.receiver)
	admin := kadm.NewClient(f.service.Operator)
	members := func() []string {
		described, err := admin.DescribeGroups(f.ctx, operator.CustodianGroup(testPool))
		if err != nil {
			t.Fatal(err)
		}
		var ids []string
		for _, member := range described[operator.CustodianGroup(testPool)].Members {
			if member.InstanceID != nil {
				ids = append(ids, *member.InstanceID)
			}
		}
		return ids
	}
	if ids := members(); !slices.Contains(ids, testReceiver) {
		t.Fatalf("custodian group members %v before the revocation, want %s", ids, testReceiver)
	}
	f.receiver.mu.Lock()
	f.receiver.state.Ended[operator.ReceiverPrincipal(testReceiver)] = true
	f.receiver.mu.Unlock()
	f.receiver.custody.wake <- struct{}{}
	deadline := time.Now().Add(sessionTimeout / 3)
	for slices.Contains(members(), testReceiver) {
		if time.Now().After(deadline) {
			t.Fatalf("%s still a member of the custodian group %s after its revocation; logs:\n%s", testReceiver, sessionTimeout/3, f.logs.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestTakeoverReadBackLostIsCountedOnTheNextPass(t *testing.T) {
	f := newBareFixture(t)
	custodian(t, f.ctx, f.receiver, time.Now())
	successor := f.member(t, "receiver-b")
	ctx, cancel := context.WithCancel(f.ctx)
	previous := commitTransaction
	t.Cleanup(func() { commitTransaction = previous })
	lost := false
	// The commit lands, its answer and the read-back are lost.
	commitTransaction = func(commitCtx context.Context, producer *kgo.Client) error {
		if !lost {
			lost = true
			if err := previous(commitCtx, producer); err != nil {
				t.Fatal(err)
			}
			cancel()
		}
		return errors.New("commit answer lost")
	}
	successor.assign(true)
	if err := successor.tend(ctx, time.Now()); err == nil {
		t.Fatal("the takeover was read back")
	}
	if records := f.snapshotRecords(t); len(records) != 2 || takeoverLines(f) != 0 {
		t.Fatalf("snapshots = %d and %d takeover lines, want the takeover committed and not yet counted", len(records), takeoverLines(f))
	}
	commitTransaction = previous
	for range 2 {
		if err := successor.tend(f.ctx, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if records := f.snapshotRecords(t); len(records) != 2 {
		t.Fatalf("snapshots = %d, want the confirmed takeover kept", len(records))
	}
	if got := scrapedValue(t, successor, `wherry_custodian_takeovers_total{pool="pool-a",reason="takeover"}`); got != 1 || takeoverLines(f) != 1 {
		t.Fatalf("takeovers counted %v and logged %d times, want once", got, takeoverLines(f))
	}
	if got := scrapedValue(t, successor, `wherry_ambiguous_commits_total{pool="pool-a"}`); got != 1 {
		t.Fatalf("ambiguous commits = %v, want 1", got)
	}
}

// custodyUntilDrained tends r's custody as Run does and returns a function that drains it, as a stop signal does, and
// waits for the drain to end.
func custodyUntilDrained(t *testing.T, ctx context.Context, r *Receiver) func() {
	t.Helper()
	drainCtx, drain := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.keepCustody(drainCtx, ctx)
	}()
	stop := func() {
		drain()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("the drain did not end")
		}
	}
	t.Cleanup(func() {
		drain()
		<-done
	})
	return stop
}

// awaitActing waits until r acts as custodian.
func awaitActing(t *testing.T, f fixture, r *Receiver) {
	t.Helper()
	for {
		r.custody.mu.Lock()
		acting := r.custody.producer != nil
		r.custody.mu.Unlock()
		if acting {
			return
		}
		if f.ctx.Err() != nil {
			t.Fatalf("%s never acted as custodian; logs:\n%s", r.id, f.logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// rekeys returns the re-key wraps on the pool's events by run.
func (f fixture) rekeys(t *testing.T) map[string][]wire.RekeyWrap {
	t.Helper()
	rekeys := map[string][]wire.RekeyWrap{}
	for _, record := range f.records(t, operator.PoolEventsTopic(testPool)) {
		var rekey wire.RekeyWrap
		if wire.ParseStrict(record.Value, f.receiver.peers, record.Timestamp, &rekey) == nil {
			rekeys[string(record.Key)] = append(rekeys[string(record.Key)], rekey)
		}
	}
	return rekeys
}

// drainFixture is a fixture whose receiver is custodian with drainTimeout, and receiver-b, a live member of the
// custodian group; it returns receiver-b and the function that drains the custodian.
func drainFixture(t *testing.T, drainTimeout time.Duration) (fixture, *Receiver, func()) {
	t.Helper()
	f := newBareFixture(t)
	f.receiver.drainTimeout = drainTimeout
	drain := custodyUntilDrained(t, f.ctx, f.receiver)
	f.awaitSnapshots(t, 1)
	awaitActing(t, f, f.receiver)
	successor := f.peer(t, "receiver-b")
	if err := successor.advertise(f.ctx); err != nil {
		t.Fatal(err)
	}
	custodyUntilDrained(t, f.ctx, successor)
	return f, successor, drain
}

// A planned drain hands custody on at once through the explicit leave: the successor publishes a takeover snapshot
// within seconds and counts it, every queued or running task under the old key is re-keyed to the successor's key, and
// the old keys are then deleted. No advertisement carries stepping_down and no snapshot carries handover.
func TestPlannedDrainHandsOverToALiveSuccessor(t *testing.T) {
	f, successor, drain := drainFixture(t, 15*time.Second)
	queued := f.submit(t, f.host, nil)
	running := f.turn(t, "run-r", 1)
	f.publishStatus(t, running, testReceiver, f.receiver.signing, "started", wire.TaskStateWorking, nil)
	ended := f.turn(t, "run-t", 1)
	f.publishStatus(t, ended, testReceiver, f.receiver.signing, "terminal", wire.TaskStateCompleted, nil)
	began := time.Now()
	drain()
	records := f.snapshotRecords(t)
	snapshot := parseSnapshot(t, records[len(records)-1], "receiver-b", successor.signing)
	if len(records) != 2 || snapshot.Reason != "takeover" {
		t.Fatalf("snapshots = %d, last %+v, want receiver-b's takeover; logs:\n%s", len(records), snapshot, f.logs.String())
	}
	if handed := records[1].Timestamp.Sub(began); handed > 10*time.Second {
		t.Fatalf("takeover %s after the drain began, want within seconds", handed)
	}
	if keys := heldKeys(t, f.receiver); len(keys) != 0 {
		t.Fatalf("the drained custodian holds %s, want its keys deleted", describeKeys(keys))
	}
	rekeys := f.rekeys(t)
	if len(rekeys) != 2 || len(rekeys[testRun("run-t")]) != 0 {
		t.Fatalf("re-key wraps for %d runs, want the queued and the running task's alone", len(rekeys))
	}
	keys := heldKeys(t, successor)
	for _, record := range []*kgo.Record{queued, running} {
		wraps := rekeys[string(record.Key)]
		if len(wraps) != 1 || wraps[0].RecipientKID != snapshot.Current.KeyID || successor.contentKey(f.parse(t, record), keys, wraps) == nil {
			t.Fatalf("run %s: re-key wraps %+v, want one the successor's key opens", record.Key, wraps)
		}
	}
	if got := awaitScraped(t, successor, `wherry_custodian_takeovers_total{pool="pool-a",reason="takeover"}`, 1); got != 1 {
		t.Fatalf("takeovers = %v after a drain, want 1", got)
	}
	for _, record := range append(f.records(t, operator.ReceiversTopic), records...) {
		parsed, err := jose.ParseSignedCompact(string(record.Value), []jose.SignatureAlgorithm{jose.EdDSA})
		if err != nil {
			t.Fatal(err)
		}
		if payload := string(parsed.UnsafePayloadWithoutVerification()); strings.Contains(payload, "stepping_down") || strings.Contains(payload, `"handover"`) {
			t.Fatalf("record on %s carries the drain label: %s", record.Topic, payload)
		}
	}
}

// A drain with the default drain timeout of 0 leaves the custodian group: the successor takes over and counts it,
// nothing is re-keyed and the old custodian keeps its keys.
func TestDrainWithNoTimeoutLeavesForATakeover(t *testing.T) {
	f, successor, drain := drainFixture(t, 0)
	f.submit(t, f.host, nil)
	held := heldKeys(t, f.receiver)
	drain()
	records := f.awaitSnapshots(t, 2)
	if snapshot := parseSnapshot(t, records[1], "receiver-b", successor.signing); snapshot.Reason != "takeover" {
		t.Fatalf("reason %s, want receiver-b's takeover; logs:\n%s", snapshot.Reason, f.logs.String())
	}
	if keys := heldKeys(t, f.receiver); len(keys) != len(held) {
		t.Fatalf("keys %s after the drain, want %s kept", describeKeys(keys), describeKeys(held))
	}
	if len(f.rekeys(t)) != 0 {
		t.Fatal("re-key wraps written with no drain timeout")
	}
	if got := awaitScraped(t, successor, `wherry_custodian_takeovers_total{pool="pool-a",reason="takeover"}`, 1); got != 1 {
		t.Fatalf("takeovers = %v at the successor, want 1", got)
	}
}

// advertiseWith writes advertisement for r, signed by its key, appended at its at_ms.
func advertiseWith(t *testing.T, f fixture, r *Receiver, advertisement wire.Advertisement) {
	t.Helper()
	advertisement.Receiver, advertisement.Pool, advertisement.Offers, advertisement.Capacity = r.id, testPool, []wire.Offer{}, 1
	value := must(t)(wire.SignRecord(&advertisement, r.signing))
	if err := f.service.Operator.ProduceSync(f.ctx, &kgo.Record{Topic: operator.ReceiversTopic, Key: []byte(r.id), Value: value, Timestamp: time.UnixMilli(advertisement.AtMS)}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// Custody follows the custodian group's session alone: a custodian whose advertisement is stale stays custodian while
// its session lives, and its crash hands custody on at the session timeout, as a counted takeover.
func TestCustodyFollowsTheGroupSessionNotAdvertisements(t *testing.T) {
	f := newBareFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), sessionTimeout+time.Minute)
	defer cancel()
	custodian := f.peer(t, "receiver-b")
	advertiseWith(t, f, custodian, wire.Advertisement{AtMS: time.Now().Add(-10 * time.Minute).UnixMilli()})
	running, crash := context.WithCancel(ctx)
	crashed := make(chan struct{})
	go func() {
		defer close(crashed)
		custodian.keepCustody(ctx, running)
	}()
	awaitActing(t, f, custodian)
	custodyUntilDrained(t, ctx, f.receiver)
	time.Sleep(10 * custodyInterval)
	latest, err := f.receiver.readCurrent(ctx)
	f.receiver.custody.mu.Lock()
	acting := f.receiver.custody.producer != nil
	f.receiver.custody.mu.Unlock()
	if err != nil || !latest.names(custodian) || acting {
		t.Fatalf("snapshot %+v, %v, the member acting %v; want receiver-b custodian with a stale advertisement and a live session; logs:\n%s", latest.snapshot, err, acting, f.logs.String())
	}
	// A crash: the group client closes with no leave, so the static membership ends at the session timeout.
	crash()
	<-crashed
	crashedAt := time.Now()
	for !latest.names(f.receiver) {
		if ctx.Err() != nil {
			t.Fatalf("no takeover after the crash; logs:\n%s", f.logs.String())
		}
		time.Sleep(500 * time.Millisecond)
		latest, _ = f.receiver.readCurrent(ctx)
	}
	if took := time.Since(crashedAt); took < sessionTimeout/2 {
		t.Fatalf("custody moved %s after the crash, before the session could expire", took)
	}
	if latest.snapshot.Reason != "takeover" {
		t.Fatalf("reason %s, want takeover", latest.snapshot.Reason)
	}
	if got := scrapedValue(t, f.receiver, `wherry_custodian_takeovers_total{pool="pool-a",reason="takeover"}`); got != 1 {
		t.Fatalf("takeovers = %v after a crash, want 1", got)
	}
}

// A successor arriving near the drain's deadline while the task topic's reads block: the drain ends within the drain
// timeout and stepDownMargin, and the keys are kept.
func TestHandoverNearTheDeadlineEndsWithinTheBoundKeepingTheKeys(t *testing.T) {
	interval := custodyInterval
	custodyInterval = 200 * time.Millisecond
	t.Cleanup(func() { custodyInterval = interval })
	f := newBareFixture(t)
	f.receiver.drainTimeout = 4 * time.Second
	drain := custodyUntilDrained(t, f.ctx, f.receiver)
	f.awaitSnapshots(t, 1)
	awaitActing(t, f, f.receiver)
	f.submit(t, f.host, nil)
	held := heldKeys(t, f.receiver)
	successor := f.peer(t, "receiver-b")
	if err := successor.advertise(f.ctx); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	f.cluster.ControlKey(kmsg.ListOffsets.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		if slices.ContainsFunc(request.(*kmsg.ListOffsetsRequest).Topics, func(topic kmsg.ListOffsetsRequestTopic) bool { return topic.Topic == operator.PoolTasksTopic(testPool) }) {
			f.cluster.SleepControl(func() { <-release })
		}
		return nil, nil, false
	})
	tended := make(chan error, 1)
	time.AfterFunc(f.receiver.drainTimeout-time.Second, func() {
		successor.assign(true)
		tended <- successor.tend(f.ctx, time.Now())
	})
	bound := f.receiver.drainTimeout + stepDownMargin
	began := time.Now()
	drain()
	if took := time.Since(began); took > bound+time.Second {
		t.Fatalf("drain took %s, want within %s", took, bound)
	}
	if err := <-tended; err != nil {
		t.Fatal(err)
	}
	records := f.snapshotRecords(t)
	if parseSnapshot(t, records[len(records)-1], "receiver-b", successor.signing).Reason != "takeover" {
		t.Fatalf("no takeover; logs:\n%s", f.logs.String())
	}
	if keys := heldKeys(t, f.receiver); len(keys) != len(held) {
		t.Fatalf("keys %s after the bound ended the scan, want %s kept", describeKeys(keys), describeKeys(held))
	}
	if len(f.rekeys(t)) != 0 {
		t.Fatal("re-key wraps written from a scan the bound ended")
	}
}

// A stop arriving while a custody pass waits, with no held run, so the run drain ends with the stop: the custodian
// steps down, waits for a successor until the deadline the stop fixed, and Run returns then, within the drain timeout and
// stepDownMargin. Both ends are ready when the pass returns, so the stop runs eight times.
func TestStopDuringACustodyPassStepsDownByTheStopsDeadline(t *testing.T) {
	interval := custodyInterval
	custodyInterval = 100 * time.Millisecond
	t.Cleanup(func() { custodyInterval = interval })
	for round := range 8 {
		t.Run(fmt.Sprint(round), stopDuringACustodyPass)
	}
}

func stopDuringACustodyPass(t *testing.T) {
	f := newBareFixture(t)
	f.receiver.drainTimeout = time.Second
	ctx, stop := context.WithCancel(f.ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = f.receiver.Run(ctx)
	}()
	t.Cleanup(func() {
		stop()
		<-done
	})
	f.awaitSnapshots(t, 1)
	awaitActing(t, f, f.receiver)
	// The next pass waits for the custody lock until half a second after the stop, when the run drain has ended too.
	f.receiver.custody.mu.Lock()
	time.Sleep(3 * custodyInterval)
	stopped := time.Now()
	stop()
	time.Sleep(500 * time.Millisecond)
	f.receiver.custody.mu.Unlock()
	bound := f.receiver.drainTimeout + stepDownMargin
	select {
	case <-done:
	case <-time.After(bound + 5*time.Second):
		t.Fatalf("Run did not return; logs:\n%s", f.logs.String())
	}
	// The wait ends at the deadline the stop fixed, not half a second later when the pass let the step-down begin.
	if took := time.Since(stopped); took < f.receiver.drainTimeout || took > f.receiver.drainTimeout+400*time.Millisecond {
		t.Fatalf("shutdown took %s, want the drain timeout %s from the stop; logs:\n%s", took, f.receiver.drainTimeout, f.logs.String())
	}
	if !strings.Contains(f.logs.String(), "no successor in pool "+testPool) {
		t.Fatalf("no wait for a successor; logs:\n%s", f.logs.String())
	}
}

// failTaskListing fails every listing of the pool's task topic's offsets.
func failTaskListing(f fixture) {
	f.cluster.ControlKey(kmsg.ListOffsets.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		req := request.(*kmsg.ListOffsetsRequest)
		if !slices.ContainsFunc(req.Topics, func(topic kmsg.ListOffsetsRequestTopic) bool { return topic.Topic == operator.PoolTasksTopic(testPool) }) {
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

// A drain whose scan of the task topic fails keeps every key it holds.
func TestDrainScanErrorKeepsTheKeys(t *testing.T) {
	f, successor, drain := drainFixture(t, 15*time.Second)
	f.submit(t, f.host, nil)
	held := heldKeys(t, f.receiver)
	failTaskListing(f)
	drain()
	records := f.snapshotRecords(t)
	if parseSnapshot(t, records[len(records)-1], "receiver-b", successor.signing).Reason != "takeover" {
		t.Fatalf("no takeover; logs:\n%s", f.logs.String())
	}
	if keys := heldKeys(t, f.receiver); len(keys) != len(held) {
		t.Fatalf("keys %s after a failed scan, want %s kept", describeKeys(keys), describeKeys(held))
	}
	if len(f.rekeys(t)) != 0 {
		t.Fatal("re-key wraps written from a failed scan")
	}
	if !strings.Contains(f.logs.String(), "keeping the pool keys to their deadlines") {
		t.Fatalf("no log of the kept keys; logs:\n%s", f.logs.String())
	}
}

// A drain with no successor waits out the drain timeout and exits within the stop window keeping its keys, and its
// restart continues as custodian with the same key.
func TestDrainWithNoSuccessorKeepsTheKeys(t *testing.T) {
	f := newBareFixture(t)
	f.receiver.drainTimeout = time.Second
	stop := runUntilStopped(t, f.ctx, f.receiver)
	f.awaitSnapshots(t, 1)
	awaitActing(t, f, f.receiver)
	id, _ := onlyKey(t, f.receiver)
	began := time.Now()
	stop()
	if took := time.Since(began); took < f.receiver.drainTimeout || took > f.receiver.drainTimeout+stepDownMargin {
		t.Fatalf("stop took %s, want between the drain timeout and %s", took, f.receiver.drainTimeout+stepDownMargin)
	}
	if !strings.Contains(f.logs.String(), "no successor in pool "+testPool+"; keeping the pool keys to their deadlines") {
		t.Fatalf("no log of the drain without a successor; logs:\n%s", f.logs.String())
	}
	if got, key := onlyKey(t, f.receiver); got != id || key.DeadlineMS != 0 {
		t.Fatalf("key %s after the drain, want %s kept as current", describeKeys(map[string]*poolKey{got: key}), id)
	}
	again := restarted(t, f.receiver)
	runUntilStopped(t, f.ctx, again)
	awaitActing(t, f, again)
	time.Sleep(5 * custodyInterval)
	if records := f.snapshotRecords(t); len(records) != 1 {
		t.Fatalf("snapshots = %d after the restart, want 1", len(records))
	}
	if got, _ := onlyKey(t, again); got != id {
		t.Fatalf("key = %s after the restart, want %s", got, id)
	}
}

// A custodian draining at the same time as the last other member of the custodian group, and a whole-group stop: each
// receiver stopped together returns from Run within the stop window, drain_timeout + stepDownMargin of the stop, whether
// the other member took custody in the meantime or not. No key is lost: each receiver keeps the keys it held unless it
// re-keyed and deleted them on a handover, and the current snapshot's custodian holds the snapshot's key. That
// custodian, restarted alone, resumes with the key and publishes nothing.
// In the rebalancing case the broker holds the custodian group's JoinGroups, as it does until every member rejoins, so
// the custodian and the last other member are stopped with their rejoins after receiver-c's leave in flight.
func TestConcurrentDrainsStopInTheWindowKeepingTheKeys(t *testing.T) {
	for name, test := range map[string]struct{ lastOther, rebalancing bool }{
		"with the last other member":                    {lastOther: true},
		"with the last other member during a rebalance": {lastOther: true, rebalancing: true},
		"whole group": {},
	} {
		t.Run(name, func(t *testing.T) { concurrentDrains(t, test.lastOther, test.rebalancing) })
	}
}

// A running receiver stays up through a peer's add: the peer's record lands only once the running receiver may read
// the peer's key topic (agb-yk39). The broker holds that grant past several refreshes, so a record appended before it
// would be read in the meantime.
func TestRunningReceiverStaysUpThroughAPeersAdd(t *testing.T) {
	f := newBareFixture(t)
	member := runMember(t, f.ctx, f.receiver, f.logs)
	f.awaitSnapshots(t, 1)
	awaitActing(t, f, f.receiver)
	var held atomic.Bool
	f.cluster.ControlKey(kmsg.CreateACLs.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		for _, creation := range request.(*kmsg.CreateACLsRequest).Creations {
			if creation.ResourceName == operator.ReceiverTopic("receiver-b") && creation.Principal == "User:"+operator.ReceiverPrincipal(testReceiver) && !held.Swap(true) {
				f.cluster.SleepControl(func() { time.Sleep(5 * refreshInterval) })
			}
		}
		return nil, nil, false
	})
	f.newReceiver(t, testConfig("receiver-b"))
	if !held.Load() {
		t.Fatal("the grant on the peer's key topic was never held")
	}
	time.Sleep(5 * refreshInterval)
	select {
	case <-member.ended:
		t.Fatalf("the running receiver exited during a peer's add; logs:\n%s", f.logs.String())
	default:
	}
}

// holdJoins holds every JoinGroup of the pool's custodian group until release closes, and returns the count of those
// held.
func holdJoins(f fixture, release <-chan struct{}) *atomic.Int32 {
	held := &atomic.Int32{}
	f.cluster.ControlKey(kmsg.JoinGroup.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		if request.(*kmsg.JoinGroupRequest).Group == operator.CustodianGroup(testPool) {
			held.Add(1)
			f.cluster.SleepControl(func() { <-release })
		}
		return nil, nil, false
	})
	return held
}

// drainingMember is a receiver run until its stop, with its own logs and the time its Run returned.
type drainingMember struct {
	r     *Receiver
	logs  *syncBuffer
	stop  context.CancelFunc
	ended chan time.Time
}

func runMember(t *testing.T, ctx context.Context, r *Receiver, logs *syncBuffer) *drainingMember {
	t.Helper()
	ctx, stop := context.WithCancel(ctx)
	m := &drainingMember{r: r, logs: logs, stop: stop, ended: make(chan time.Time, 1)}
	go func() {
		_ = r.Run(ctx)
		m.ended <- time.Now()
	}()
	t.Cleanup(func() {
		stop()
		select {
		case <-m.ended:
		case <-time.After(time.Minute):
		}
	})
	return m
}

func concurrentDrains(t *testing.T, lastOther, rebalancing bool) {
	f := newBareFixture(t)
	f.receiver.drainTimeout = 2 * time.Second
	// Peers are provisioned before any Run: a running receiver that refreshes between a peer's operator record and its
	// key-topic grant is denied and exits (agb-yk39).
	type peer struct {
		id   string
		r    *Receiver
		logs *syncBuffer
	}
	var peers []peer
	for _, id := range []string{"receiver-b", "receiver-c"} {
		r, logs := f.newReceiver(t, testConfig(id))
		r.drainTimeout = f.receiver.drainTimeout
		peers = append(peers, peer{id, r, logs})
	}
	members := []*drainingMember{runMember(t, f.ctx, f.receiver, f.logs)}
	f.awaitSnapshots(t, 1)
	awaitActing(t, f, f.receiver)
	for _, p := range peers {
		id, r, logs := p.id, p.r, p.logs
		members = append(members, runMember(t, f.ctx, r, logs))
		for !strings.Contains(logs.String(), "configured:") {
			if f.ctx.Err() != nil {
				t.Fatalf("%s never configured", id)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	// Each Run refreshes its view of the pool's members every refreshInterval.
	time.Sleep(5 * custodyInterval)
	together := members
	release := make(chan struct{})
	releaseJoins := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseJoins)
	var held *atomic.Int32
	if rebalancing {
		held = holdJoins(f, release)
	}
	if lastOther {
		// receiver-c leaves first, so receiver-b is the last other member of the group when the custodian drains.
		members[2].stop()
		select {
		case at := <-members[2].ended:
			members[2].ended <- at
		case <-time.After(f.receiver.drainTimeout + stepDownMargin + time.Second):
			t.Fatalf("receiver-c did not stop; logs:\n%s", members[2].logs.String())
		}
		together = members[:2]
	}
	for rebalancing && held.Load() < int32(len(together)) {
		if f.ctx.Err() != nil {
			t.Fatalf("%d rejoins held, want %d", held.Load(), len(together))
		}
		time.Sleep(20 * time.Millisecond)
	}
	keys := map[*Receiver]map[string]*poolKey{}
	for _, m := range members {
		keys[m.r] = heldKeys(t, m.r)
	}
	stopped := time.Now()
	for _, m := range together {
		m.stop()
	}
	bound := f.receiver.drainTimeout + stepDownMargin
	for _, m := range together {
		select {
		case at := <-m.ended:
			m.ended <- at
			if took := at.Sub(stopped); took > bound {
				t.Fatalf("%s stopped %s after the stop, want within %s; logs:\n%s", m.r.id, took, bound, m.logs.String())
			}
		case <-time.After(time.Until(stopped.Add(bound + time.Second))):
			t.Fatalf("%s did not stop within %s; logs:\n%s", m.r.id, bound, m.logs.String())
		}
	}
	releaseJoins()
	for _, m := range members {
		after := heldKeys(t, m.r)
		for id := range keys[m.r] {
			if _, ok := after[id]; !ok && !strings.Contains(m.logs.String(), "custodian: re-keyed") {
				t.Fatalf("%s lost key %s with no handover: holds %s, held %s; logs:\n%s", m.r.id, id, describeKeys(after), describeKeys(keys[m.r]), m.logs.String())
			}
		}
	}
	records := f.snapshotRecords(t)
	latest := records[len(records)-1]
	var holder *drainingMember
	var current wire.Snapshot
	for _, m := range members {
		if snapshot, err := wire.ParseSnapshot(latest.Value, signed.Keys{m.r.id: {Current: m.r.signing.Public().(ed25519.PublicKey)}}); err == nil {
			holder, current = m, snapshot
		}
	}
	if holder == nil {
		t.Fatal("the current snapshot is no member's")
	}
	if _, ok := heldKeys(t, holder.r)[current.Current.KeyID]; !ok {
		t.Fatalf("custodian %s does not hold the current key %s: holds %s", holder.r.id, current.Current.KeyID, describeKeys(heldKeys(t, holder.r)))
	}
	again := restarted(t, holder.r)
	runUntilStopped(t, f.ctx, again)
	awaitActing(t, f, again)
	time.Sleep(5 * custodyInterval)
	if after := f.snapshotRecords(t); len(after) != len(records) {
		t.Fatalf("snapshots = %d after %s restarted, want %d; logs:\n%s", len(after), holder.r.id, len(records), holder.logs.String())
	}
	if _, ok := heldKeys(t, again)[current.Current.KeyID]; !ok {
		t.Fatalf("%s holds %s after the restart, want the current key %s", again.id, describeKeys(heldKeys(t, again)), current.Current.KeyID)
	}
}

// fence initialises the custodian transactional ID as another writer would, fencing its producer.
func fence(t *testing.T, f fixture) {
	t.Helper()
	fencer, err := f.receiver.newClient(kgo.TransactionalID(f.receiver.custodianTransactionalID()))
	if err != nil {
		t.Fatal(err)
	}
	defer fencer.Close()
	if _, _, err := fencer.ProducerID(f.ctx); err != nil {
		t.Fatal(err)
	}
}

// A custodian fenced while assigned re-initialises and republishes its snapshot as contested, counted; a second fence
// within the session timeout stands it down until it is assigned again.
func TestFencedWhileAssignedContestsOnce(t *testing.T) {
	f := newBareFixture(t)
	custodian(t, f.ctx, f.receiver, time.Now())
	id, key := onlyKey(t, f.receiver)
	rotation := rotationAt(time.UnixMilli(key.PublishedMS), testPool, id)
	fence(t, f)
	if err := f.receiver.tend(f.ctx, rotation); err == nil {
		t.Fatal("the fenced custodian's rotation succeeded")
	}
	scraped(t, f.receiver, `wherry_custodian_takeovers_total{pool="pool-a",reason="contested"} 1`)
	if err := f.receiver.tend(f.ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	records := f.snapshotRecords(t)
	if snapshot := parseSnapshot(t, records[len(records)-1], testReceiver, f.receiver.signing); len(records) != 2 || snapshot.Reason != "contested" || snapshot.Current.KeyID != id {
		t.Fatalf("snapshots = %d, last %+v, want the snapshot republished as contested with key %s", len(records), snapshot, id)
	}
	fence(t, f)
	if err := f.receiver.tend(f.ctx, rotation); err == nil {
		t.Fatal("the fenced custodian's rotation succeeded")
	}
	for range 2 {
		if err := f.receiver.tend(f.ctx, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if records := f.snapshotRecords(t); len(records) != 2 {
		t.Fatalf("snapshots = %d, want none after the second fence", len(records))
	}
	if !f.receiver.custody.fenced || f.receiver.custody.producer != nil {
		t.Fatalf("fenced=%v producer=%v, want the custodian stood down", f.receiver.custody.fenced, f.receiver.custody.producer != nil)
	}
	if !strings.Contains(f.logs.String(), "fenced again within the session timeout in pool "+testPool+"; standing down until assigned again") {
		t.Fatalf("no stand-down line; logs:\n%s", f.logs.String())
	}
	if got := scrapedValue(t, f.receiver, `wherry_custodian_takeovers_total{pool="pool-a",reason="contested"}`); got != 1 {
		t.Fatalf("contested counted %v, want 1", got)
	}
}

// holdProduces answers every produce to topic with a retriable error while held is set, and closes arrived at the first.
func holdProduces(t *testing.T, f fixture, topic string, held *atomic.Bool) <-chan struct{} {
	t.Helper()
	details, err := kadm.NewClient(f.service.Operator).ListTopics(f.ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	id := details[topic].ID
	arrived := make(chan struct{})
	once := sync.OnceFunc(func() { close(arrived) })
	f.cluster.ControlKey(kmsg.Produce.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		req := request.(*kmsg.ProduceRequest)
		if !held.Load() || !slices.ContainsFunc(req.Topics, func(produced kmsg.ProduceRequestTopic) bool { return produced.Topic == topic || produced.TopicID == id }) {
			return nil, nil, false
		}
		once()
		response := req.ResponseKind().(*kmsg.ProduceResponse)
		response.SetVersion(req.Version)
		for _, produced := range req.Topics {
			out := kmsg.NewProduceResponseTopic()
			out.Topic, out.TopicID = produced.Topic, produced.TopicID
			for _, partition := range produced.Partitions {
				result := kmsg.NewProduceResponseTopicPartition()
				result.Partition, result.ErrorCode = partition.Partition, kerr.NotLeaderForPartition.Code
				out.Partitions = append(out.Partitions, result)
			}
			response.Topics = append(response.Topics, out)
		}
		return response, nil, true
	})
	return arrived
}

// A task its host built under the old key and appended after the drain's scan is not re-keyed: it fails before it
// starts, key-lost, and its host resubmits it once under the successor's key, where it completes.
func TestTaskAppendedAfterTheDrainScanIsResubmittedOnce(t *testing.T) {
	f := newBareFixture(t)
	f.receiver.drainTimeout = 15 * time.Second
	drain := custodyUntilDrained(t, f.ctx, f.receiver)
	// The drained custodian serves opens while its held runs end, as Run does.
	go f.receiver.every(f.ctx, openInterval, f.receiver.serveOpens)
	f.awaitSnapshots(t, 1)
	awaitActing(t, f, f.receiver)
	successor, successorLogs := f.newReceiver(t, testConfig("receiver-b"))
	runUntilStopped(t, f.ctx, successor)
	for !strings.Contains(successorLogs.String(), "configured:") {
		time.Sleep(20 * time.Millisecond)
	}
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	var held atomic.Bool
	held.Store(true)
	arrived := holdProduces(t, f, operator.PoolTasksTopic(testPool), &held)
	t.Setenv("WHERRY_TEST_TOKEN", testToken)
	kafka, err := kgo.NewClient(append(f.service.Operator.Opts(), kgo.FetchIsolationLevel(kgo.ReadCommitted()))...)
	if err != nil {
		t.Fatal(err)
	}
	host, err := client.NewHost(kafka, f.host, operator.TenantPrincipal(testTenant, testHost), filepath.Join(t.TempDir(), "host"),
		map[string]credential.Source{credentialName: {Env: "WHERRY_TEST_TOKEN"}}, nil, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	submitted := make(chan client.SubmitResult, 1)
	go func() {
		for f.ctx.Err() == nil {
			result, err := host.Submit(f.ctx, client.SubmitInput{Agent: "claude", Prompt: "hello", StartBy: time.Now().Add(time.Minute), EndBy: time.Now().Add(time.Minute)})
			if err == nil {
				submitted <- result
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	select {
	case <-arrived:
	case <-f.ctx.Done():
		t.Fatal("the host never produced its task")
	}
	drain()
	records := f.snapshotRecords(t)
	if parseSnapshot(t, records[len(records)-1], "receiver-b", successor.signing).Reason != "takeover" || len(heldKeys(t, f.receiver)) != 0 {
		t.Fatalf("want receiver-b's takeover and the old keys deleted; logs:\n%s", f.logs.String())
	}
	held.Store(false)
	var result client.SubmitResult
	select {
	case result = <-submitted:
	case <-f.ctx.Done():
		t.Fatal("submit never returned")
	}
	awaited, err := host.Await(f.ctx, result.RunID, 30*time.Second)
	if err != nil || awaited.Status.State != wire.TaskStateCompleted || len(awaited.Runs) != 2 || awaited.Runs[0] != result.RunID {
		t.Fatalf("await %+v, %v, want the run resubmitted once and completed; logs:\n%s", awaited, err, successorLogs.String())
	}
	lost := false
	for _, record := range f.records(t, operator.PoolEventsTopic(testPool)) {
		var status wire.RunStatus
		if wire.ParseRecord(record.Value, signed.Keys{"receiver-b": {Current: successor.signing.Public().(ed25519.PublicKey)}}, &status) == nil {
			lost = lost || status.Wherry.Run == result.RunID && status.Wherry.Event == wire.EventKeyLost
		}
	}
	if dispatches, _, _ := f.dispatched(t); !lost || dispatches != 1 {
		t.Fatalf("key-lost %v, dispatches %d, want the late task failed key-lost and only its replacement dispatched", lost, dispatches)
	}
}

// A handover verifies the records of each run's task alone, each hash once for the pass: a copy of the task and a second
// hash under the run cost nothing.
func TestHandoverVerifiesEachTaskRecordOnce(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	const runs = 5
	for i := range runs {
		run := fmt.Sprintf("run-%d", i)
		record := f.turn(t, run, 1)
		f.produce(t, operator.PoolTasksTopic(testPool), string(record.Key), record.Value)
		f.publishStatus(t, record, testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
		f.turn(t, run, 2)
	}
	var verifications atomic.Int64
	authorizeSubmission = func(state operator.State, hosts signed.Keys, encoded []byte, pool string) (wire.Submission, error) {
		verifications.Add(1)
		return operator.AuthorizeSubmission(state, hosts, encoded, pool)
	}
	t.Cleanup(func() { authorizeSubmission = operator.AuthorizeSubmission })
	successor, err := newAttemptKey()
	if err != nil {
		t.Fatal(err)
	}
	current := wire.PoolKey{KeyID: signed.KeyThumbprint(&successor.PublicKey), Key: jose.JSONWebKey{Key: &successor.PublicKey}}
	if err := f.receiver.handOver(f.ctx, current); err != nil {
		t.Fatal(err)
	}
	if rekeys := f.rekeys(t); len(rekeys) != runs {
		t.Fatalf("re-key wraps for %d runs, want %d", len(rekeys), runs)
	}
	t.Logf("%d verifications for %d runs over %d task records", verifications.Load(), runs, 3*runs)
	if got := verifications.Load(); got != runs {
		t.Fatalf("%d verifications for %d runs over %d task records, want one per run", got, runs, 3*runs)
	}
}

// A drain re-keys each live hash once, a copy of it included: a run's task when it has one and each admissible hash of a
// run with none, never a terminal, expired or already re-keyed task.
func TestDrainRekeysEachLiveHashOnce(t *testing.T) {
	f := newFixture(t, resultStream)
	f.quiet()
	successor, err := newAttemptKey()
	if err != nil {
		t.Fatal(err)
	}
	current := wire.PoolKey{KeyID: signed.KeyThumbprint(&successor.PublicKey), Key: jose.JSONWebKey{Key: &successor.PublicKey}}
	expired := f.submit(t, f.host, func(task *wire.Submission) {
		named(task, "run-expired")
		task.Wherry.TaskStartedBy, task.Wherry.TaskEndedBy = time.Now().Unix()+1, time.Now().Unix()+1
	})
	live := f.turn(t, "run-live", 1)
	f.produce(t, operator.PoolTasksTopic(testPool), string(live.Key), live.Value)
	f.publishStatus(t, live, testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
	f.turn(t, "run-live", 2)
	f.turn(t, "run-open", 1)
	f.turn(t, "run-open", 2)
	f.publishStatus(t, f.turn(t, "run-done", 1), testReceiver, f.receiver.signing, "terminal", wire.TaskStateCompleted, nil)
	moved := f.turn(t, "run-moved", 1)
	rekey, err := f.receiver.rekeyRecord(string(moved.Key), wire.TaskHash(moved.Value), f.contentKey(t, moved), current)
	if err != nil {
		t.Fatal(err)
	}
	f.produce(t, rekey.Topic, string(rekey.Key), rekey.Value)
	passed(f.parse(t, expired).Wherry.TaskEndedBy)
	if err := f.receiver.handOver(f.ctx, current); err != nil {
		t.Fatal(err)
	}
	rekeys := f.rekeys(t)
	for run, want := range map[string]int{testRun("run-live"): 1, testRun("run-open"): 2, testRun("run-done"): 0, testRun("run-moved"): 1, testRun("run-expired"): 0} {
		if len(rekeys[run]) != want {
			t.Fatalf("run %s re-key wraps %+v, want %d", run, rekeys[run], want)
		}
	}
	if wraps := rekeys[testRun("run-live")]; wraps[0].TaskHash != wire.TaskHash(live.Value) {
		t.Fatalf("run-live re-keyed %s, want its task", wraps[0].TaskHash)
	}
}
