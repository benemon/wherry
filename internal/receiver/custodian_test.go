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

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/credential"
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
	return f.records(t, admin.CustodianTopic(testPool))
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
	return &Receiver{id: r.id, base: base, reader: admin.NewReader(base), signing: r.signing, stateDir: r.stateDir, started: time.Now(), drainTimeout: r.drainTimeout, factory: r.factory, logger: r.logger, metrics: newMetrics(prometheus.NewRegistry())}
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
	window := time.Duration(admin.DefaultStartByLimitMS)*time.Millisecond + keyGrace
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
	deadline := rotation.Timestamp.Add(time.Duration(admin.DefaultStartByLimitMS)*time.Millisecond + keyGrace)
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
	deadline := record.Timestamp.Add(time.Duration(admin.DefaultStartByLimitMS)*time.Millisecond + keyGrace)
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
	// Another writer's transaction open on the snapshot topic holds its last stable offset.
	open, err := f.receiver.newClient(kgo.TransactionalID(admin.TransactionalPrefix(testPool) + "open"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(open.Close)
	if err := open.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := open.ProduceSync(f.ctx, &kgo.Record{Topic: admin.CustodianTopic(testPool), Key: []byte(wire.CustodianKey), Value: []byte("pending")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.tend(f.ctx, time.Now()); err == nil {
		t.Fatal("tended behind an open transaction")
	}
	held("last stable offset blocked")
	if err := open.EndTransaction(f.ctx, kgo.TryAbort); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.tend(f.ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if keys := heldKeys(t, f.receiver); keys[expired] != nil || keys[unmarked] == nil || keys[unmarked].DeadlineMS == 0 {
		t.Fatalf("keys = %s after the abort, want the expired key deleted and the unmarked key given a deadline", describeKeys(keys))
	}
}

func TestMarkedNonCurrentKeyGetsADeadlineOnStart(t *testing.T) {
	f := newBareFixture(t)
	retired := writeKey(t, f.receiver, &poolKey{CreatedMS: time.Now().Add(-48 * time.Hour).UnixMilli(), PublishedMS: time.Now().Add(-48 * time.Hour).UnixMilli()})
	now := time.Now()
	if err := f.receiver.tend(f.ctx, now); err != nil {
		t.Fatal(err)
	}
	want := now.Add(time.Duration(admin.DefaultStartByLimitMS)*time.Millisecond + keyGrace).UnixMilli()
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
	f.produce(t, admin.ReceiverTopic(testReceiver), testReceiver, must(t)(signed.RotateKey(testReceiver, f.receiver.signing, next)))
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
	f.produce(t, admin.ReceiverTopic(r.id), r.id, must(t)(signed.RotateKey(r.id, r.signing, next)))
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
	f.receiver.state.Ended[admin.ReceiverPrincipal(testReceiver)] = true
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
	for _, record := range f.records(t, admin.PoolEventsTopic(testPool)) {
		var rekey wire.RekeyWrap
		if wire.ParseStrict(record.Value, f.receiver.peers, record.Timestamp, &rekey) == nil {
			rekeys[string(record.Key)] = append(rekeys[string(record.Key)], rekey)
		}
	}
	return rekeys
}

// handoverFixture is a fixture whose receiver is custodian with drainTimeout, and receiver-b, a live member of the
// custodian group; it returns receiver-b and the function that drains the custodian.
func handoverFixture(t *testing.T, drainTimeout time.Duration) (fixture, *Receiver, func()) {
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

// A planned drain hands custody on at once through the explicit leave: the successor publishes a handover snapshot
// within seconds, every queued or running task under the old key is re-keyed to the successor's key, and the old keys
// are then deleted.
func TestPlannedDrainHandsOverToALiveSuccessor(t *testing.T) {
	f, successor, drain := handoverFixture(t, 15*time.Second)
	queued := f.submit(t, f.host, nil)
	running := f.turn(t, "run-r", 1)
	f.publishStatus(t, running, testReceiver, f.receiver.signing, "started", wire.TaskStateWorking, nil)
	ended := f.turn(t, "run-t", 1)
	f.publishStatus(t, ended, testReceiver, f.receiver.signing, "terminal", wire.TaskStateCompleted, nil)
	began := time.Now()
	drain()
	records := f.snapshotRecords(t)
	snapshot := parseSnapshot(t, records[len(records)-1], "receiver-b", successor.signing)
	if len(records) != 2 || snapshot.Reason != "handover" {
		t.Fatalf("snapshots = %d, last %+v, want receiver-b's handover; logs:\n%s", len(records), snapshot, f.logs.String())
	}
	if handed := records[1].Timestamp.Sub(began); handed > 10*time.Second {
		t.Fatalf("handover %s after the drain began, want within seconds", handed)
	}
	if keys := heldKeys(t, f.receiver); len(keys) != 0 {
		t.Fatalf("the drained custodian holds %s, want its keys deleted", describeKeys(keys))
	}
	rekeys := f.rekeys(t)
	if len(rekeys) != 2 || len(rekeys["run-t"]) != 0 {
		t.Fatalf("re-key wraps for %d runs, want the queued and the running task's alone", len(rekeys))
	}
	keys := heldKeys(t, successor)
	for _, record := range []*kgo.Record{queued, running} {
		wraps := rekeys[string(record.Key)]
		if len(wraps) != 1 || wraps[0].RecipientKID != snapshot.Current.KeyID || successor.contentKey(f.parse(t, record), keys, wraps) == nil {
			t.Fatalf("run %s: re-key wraps %+v, want one the successor's key opens", record.Key, wraps)
		}
	}
	if got := scrapedValue(t, successor, `wherry_custodian_takeovers_total{pool="pool-a",reason="takeover"}`); got != 0 {
		t.Fatalf("takeovers = %v after a handover, want 0", got)
	}
}

// A drain with the default drain timeout of 0 leaves the custodian group without stepping_down: the successor takes over
// and counts it, nothing is re-keyed and the old custodian keeps its keys.
func TestDrainWithNoTimeoutLeavesForATakeover(t *testing.T) {
	f, successor, drain := handoverFixture(t, 0)
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
		t.Fatal("re-key wraps written without a handover")
	}
	if got := scrapedValue(t, successor, `wherry_custodian_takeovers_total{pool="pool-a",reason="takeover"}`); got != 1 {
		t.Fatalf("takeovers = %v at the successor, want 1", got)
	}
}

// A drain whose stepping_down advertisement cannot be written still leaves the custodian group, within the drain timeout
// and stepDownMargin.
func TestDrainLeavesWhenTheAdvertisementFails(t *testing.T) {
	f := newBareFixture(t)
	f.receiver.drainTimeout = time.Second
	drain := custodyUntilDrained(t, f.ctx, f.receiver)
	f.awaitSnapshots(t, 1)
	awaitActing(t, f, f.receiver)
	if err := f.peer(t, "receiver-b").advertise(f.ctx); err != nil {
		t.Fatal(err)
	}
	var held atomic.Bool
	held.Store(true)
	arrived := holdProduces(t, f, admin.ReceiversTopic, &held)
	var left atomic.Bool
	f.cluster.ControlKey(kmsg.LeaveGroup.Int16(), func(kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		left.Store(true)
		return nil, nil, false
	})
	bound := f.receiver.drainTimeout + stepDownMargin
	// Past the bound the advertisement may land, so a drain that waits for it ends and fails here.
	defer time.AfterFunc(bound+time.Second, func() { held.Store(false) }).Stop()
	began := time.Now()
	drain()
	if took := time.Since(began); took > bound+time.Second {
		t.Fatalf("drain took %s, want within %s", took, bound)
	}
	select {
	case <-arrived:
	default:
		t.Fatal("no stepping_down advertisement attempted")
	}
	if !left.Load() {
		t.Fatalf("no leave of the custodian group; logs:\n%s", f.logs.String())
	}
}

// advertiseWith writes advertisement for r, signed by its key, appended at its at_ms.
func advertiseWith(t *testing.T, f fixture, r *Receiver, advertisement wire.Advertisement) {
	t.Helper()
	advertisement.Receiver, advertisement.Pool, advertisement.Offers, advertisement.Capacity = r.id, testPool, []wire.Offer{}, 1
	value := must(t)(wire.SignRecord(&advertisement, r.signing))
	if err := f.service.Admin.ProduceSync(f.ctx, &kgo.Record{Topic: admin.ReceiversTopic, Key: []byte(r.id), Value: value, Timestamp: time.UnixMilli(advertisement.AtMS)}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// A successor counts a handover only when the previous custodian's latest advertisement is live and carries
// stepping_down set by the process advertising; anything else is a takeover.
func TestHandoverOnlyFromTheLiveProcessSteppingDown(t *testing.T) {
	// Later than the fixtures' own advertisements.
	now := time.Now().Add(time.Minute)
	started := now.Add(-time.Hour).UnixMilli()
	for name, test := range map[string]struct {
		advertisement wire.Advertisement
		reason        string
	}{
		"stepping down":                {wire.Advertisement{AtMS: now.UnixMilli(), StartedMS: started, SteppingDown: &wire.SteppingDown{AtMS: now.UnixMilli(), StartedMS: started}}, "handover"},
		"flag from an earlier process": {wire.Advertisement{AtMS: now.UnixMilli(), StartedMS: started + 1, SteppingDown: &wire.SteppingDown{AtMS: now.UnixMilli(), StartedMS: started}}, "takeover"},
		"advertisement not live":       {wire.Advertisement{AtMS: now.Add(-advertisementLive - time.Second).UnixMilli(), StartedMS: started, SteppingDown: &wire.SteppingDown{AtMS: now.UnixMilli(), StartedMS: started}}, "takeover"},
		"no flag":                      {wire.Advertisement{AtMS: now.UnixMilli(), StartedMS: started}, "takeover"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newBareFixture(t)
			custodian(t, f.ctx, f.receiver, now)
			successor := f.member(t, "receiver-b")
			advertiseWith(t, f, f.receiver, test.advertisement)
			custodian(t, f.ctx, successor, now)
			records := f.snapshotRecords(t)
			if snapshot := parseSnapshot(t, records[len(records)-1], "receiver-b", successor.signing); snapshot.Reason != test.reason {
				t.Fatalf("reason %s, want %s", snapshot.Reason, test.reason)
			}
			if counted := strings.Contains(scrape(t, successor), `wherry_custodian_takeovers_total{pool="pool-a",reason="takeover"} 1`); counted != (test.reason == "takeover") {
				t.Fatalf("takeover counted %v, want %v", counted, test.reason == "takeover")
			}
		})
	}
}

// A stepping_down flag an earlier process left is not the live custodian's: the same receiver back without its keys after
// a drain with no successor takes over, and counts it.
func TestStaleSteppingDownThenStateLossIsATakeover(t *testing.T) {
	f := newBareFixture(t)
	f.receiver.drainTimeout = time.Second
	drain := custodyUntilDrained(t, f.ctx, f.receiver)
	f.awaitSnapshots(t, 1)
	awaitActing(t, f, f.receiver)
	// A live member that never joins the custodian group: the drain steps down and waits for no successor.
	if err := f.peer(t, "receiver-b").advertise(f.ctx); err != nil {
		t.Fatal(err)
	}
	drain()
	if err := os.RemoveAll(f.receiver.poolKeysDir()); err != nil {
		t.Fatal(err)
	}
	again := restarted(t, f.receiver)
	if err := again.configure(f.ctx); err != nil {
		t.Fatal(err)
	}
	// The latest advertisement is the earlier process's, stepping down.
	custodian(t, f.ctx, again, time.Now())
	records := f.snapshotRecords(t)
	if len(records) != 2 || parseSnapshot(t, records[1], testReceiver, again.signing).Reason != "takeover" {
		t.Fatalf("snapshots = %d, want the bootstrap and the takeover", len(records))
	}
	scraped(t, again, `wherry_custodian_takeovers_total{pool="pool-a",reason="takeover"} 1`)
}

// failTaskListing fails every listing of the pool's task topic's offsets.
func failTaskListing(f fixture) {
	f.cluster.ControlKey(kmsg.ListOffsets.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		req := request.(*kmsg.ListOffsetsRequest)
		if !slices.ContainsFunc(req.Topics, func(topic kmsg.ListOffsetsRequestTopic) bool { return topic.Topic == admin.PoolTasksTopic(testPool) }) {
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
	f, successor, drain := handoverFixture(t, 15*time.Second)
	f.submit(t, f.host, nil)
	held := heldKeys(t, f.receiver)
	failTaskListing(f)
	drain()
	records := f.snapshotRecords(t)
	if parseSnapshot(t, records[len(records)-1], "receiver-b", successor.signing).Reason != "handover" {
		t.Fatalf("no handover; logs:\n%s", f.logs.String())
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

// A drain with no successor exits keeping its keys, and its restart continues as custodian with the same key.
func TestDrainWithNoSuccessorKeepsTheKeys(t *testing.T) {
	f := newBareFixture(t)
	f.receiver.drainTimeout = time.Second
	stop := runUntilStopped(t, f.ctx, f.receiver)
	f.awaitSnapshots(t, 1)
	awaitActing(t, f, f.receiver)
	id, _ := onlyKey(t, f.receiver)
	stop()
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
	details, err := kadm.NewClient(f.service.Admin).ListTopics(f.ctx, topic)
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
	arrived := holdProduces(t, f, admin.PoolTasksTopic(testPool), &held)
	t.Setenv("WHERRY_TEST_TOKEN", testToken)
	kafka, err := kgo.NewClient(append(f.service.Admin.Opts(), kgo.FetchIsolationLevel(kgo.ReadCommitted()))...)
	if err != nil {
		t.Fatal(err)
	}
	host, err := client.NewHost(kafka, f.host, admin.TenantPrincipal(testTenant, testHost), filepath.Join(t.TempDir(), "host"),
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
	if parseSnapshot(t, records[len(records)-1], "receiver-b", successor.signing).Reason != "handover" || len(heldKeys(t, f.receiver)) != 0 {
		t.Fatalf("want receiver-b's handover and the old keys deleted; logs:\n%s", f.logs.String())
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
	for _, record := range f.records(t, admin.PoolEventsTopic(testPool)) {
		var status wire.RunStatus
		if wire.ParseRecord(record.Value, signed.Keys{"receiver-b": {Current: successor.signing.Public().(ed25519.PublicKey)}}, &status) == nil {
			lost = lost || status.Wherry.Run == result.RunID && status.Wherry.Event == wire.EventKeyLost
		}
	}
	if dispatches, _, _ := f.dispatched(t); !lost || dispatches != 1 {
		t.Fatalf("key-lost %v, dispatches %d, want the late task failed key-lost and only its replacement dispatched", lost, dispatches)
	}
}
