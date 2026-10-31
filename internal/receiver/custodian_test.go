package receiver

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	json "encoding/json/v2"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/prometheus/client_golang/prometheus"
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
	return &Receiver{id: r.id, base: base, reader: admin.NewReader(base), signing: r.signing, stateDir: r.stateDir, factory: r.factory, logger: r.logger, metrics: newMetrics(prometheus.NewRegistry())}
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
	f := newFixture(t, resultStream)
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
	if len(atCommit) != 1 || atCommit[snapshot.Current.KeyID] == nil || atCommit[snapshot.Current.KeyID].Published {
		t.Fatalf("keys at the commit = %v, want the snapshot's key persisted unmarked", atCommit)
	}
	for {
		if id, key := onlyKey(t, f.receiver); id == snapshot.Current.KeyID && key.Published {
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
	if strings.Contains(scrape(t, f.receiver), "wherry_custodian_takeovers_total") {
		t.Fatal("a bootstrap counted as a takeover")
	}
}

func TestRestartInsideSessionTimeoutContinuesWithItsKey(t *testing.T) {
	f := newFixture(t, resultStream)
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
	f := newFixture(t, resultStream)
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
	f := newFixture(t, resultStream)
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
	if !f.receiver.custody.fenced || f.receiver.custody.producer != nil {
		t.Fatalf("fenced=%v producer=%v, want fenced and no producer", f.receiver.custody.fenced, f.receiver.custody.producer != nil)
	}
	if err := f.receiver.tend(f.ctx, now); err != nil {
		t.Fatal(err)
	}
	if records := f.snapshotRecords(t); len(records) != 2 {
		t.Fatalf("snapshots = %d, want the fenced custodian to write no more", len(records))
	}
}

func TestRotationFiresADayAndAJitterAfterTheSnapshot(t *testing.T) {
	f := newFixture(t, resultStream)
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
	f := newFixture(t, resultStream)
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
	f := newFixture(t, resultStream)
	custodian(t, f.ctx, f.receiver, time.Now())
	first, _ := onlyKey(t, f.receiver)
	rotation := rotate(t, f, f.receiver)
	deadline := rotation.Timestamp.Add(time.Duration(admin.DefaultStartByLimitMS)*time.Millisecond + keyGrace)
	if key := heldKeys(t, f.receiver)[first]; key == nil || key.DeadlineMS != deadline.UnixMilli() {
		t.Fatalf("replaced key = %+v, want it held with the rotation's append time + start_by_limit + 7h", key)
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

func TestUnmarkedKeyIsKeptToItsConservativeDeadline(t *testing.T) {
	f := newFixture(t, resultStream)
	created := time.Now()
	// A crash between persisting the key and publishing it; another receiver is custodian.
	crashed := writeKey(t, f.receiver, &poolKey{CreatedMS: created.UnixMilli()})
	other := f.member(t, "receiver-b")
	custodian(t, f.ctx, other, created)
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.UnixMilli(created.UnixMilli()).Add(time.Duration(admin.DefaultStartByLimitMS)*time.Millisecond + keyGrace + rotationInterval)
	for _, now := range []time.Time{created, deadline.Add(-time.Millisecond)} {
		if err := f.receiver.tend(f.ctx, now); err != nil {
			t.Fatal(err)
		}
		if key := heldKeys(t, f.receiver)[crashed]; key == nil || key.Published {
			t.Fatalf("unmarked key at %v: %+v, want kept unmarked", now.Sub(created), key)
		}
	}
	if err := f.receiver.tend(f.ctx, deadline); err != nil {
		t.Fatal(err)
	}
	if heldKeys(t, f.receiver)[crashed] != nil {
		t.Fatal("unmarked key kept past its deadline")
	}
}

func TestMarkedNonCurrentKeyGetsADeadlineOnStart(t *testing.T) {
	f := newFixture(t, resultStream)
	retired := writeKey(t, f.receiver, &poolKey{CreatedMS: time.Now().Add(-48 * time.Hour).UnixMilli(), Published: true})
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
	f := newFixture(t, resultStream)
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
	f := newFixture(t, resultStream)
	runUntilStopped(t, f.ctx, f.receiver)
	f.awaitSnapshots(t, 1)
	other := f.member(t, "receiver-b")
	writeKey(t, other, &poolKey{CreatedMS: time.Now().UnixMilli(), Published: true})
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
