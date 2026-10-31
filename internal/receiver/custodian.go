package receiver

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/trust"
	"github.com/benemon/wherry/internal/wire"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	// poolKeysDir is the state directory's subdirectory holding the pool keys this receiver holds.
	poolKeysDir = "pool-keys"
	// rotationInterval and rotationJitter place the daily rotation after the current snapshot's append time.
	rotationInterval = 24 * time.Hour
	rotationJitter   = time.Hour
	// keyGrace is added to start_by_limit for a replaced key's deadline: the 6 h run cap and 1 h of margin.
	keyGrace = 7 * time.Hour
)

// custodyInterval is how often the receiver tends its pool keys; a variable so tests can shorten it.
var custodyInterval = 10 * time.Second

// custody is the receiver's part in its pool's key custody, guarded by its mu, which each pass holds throughout.
type custody struct {
	mu sync.Mutex
	// assigned is set while the custodian group assigns this receiver the snapshot topic's partition.
	assigned bool
	// producer is the custodian transactional ID's producer while the receiver acts as custodian.
	producer *kgo.Client
	// fenced is set when a successor fenced producer, until the next assignment.
	fenced bool
	// tended is set after the first pass of the process.
	tended bool
	wake   chan struct{}
}

// poolKey is a pool private key in the receiver's state directory with its custody record.
type poolKey struct {
	Key       jose.JSONWebKey `json:"key"`
	CreatedMS int64           `json:"created_ms"`
	// Published is set once a committed snapshot naming the key current is read back.
	Published bool `json:"published,omitzero"`
	// DeadlineMS is when the key is deleted, set once a later snapshot replaces it.
	DeadlineMS int64 `json:"deadline_ms,omitzero"`
}

func (key *poolKey) id() string {
	return signed.SealKeyID(&key.Key.Key.(*ecdsa.PrivateKey).PublicKey)
}

// custodianTransactionalID is the one transactional ID every custodian of the pool writes through, so a successor's
// initialisation fences its predecessor.
func (r *Receiver) custodianTransactionalID() string {
	return admin.TransactionalPrefix(r.config.Pool) + "custodian"
}

func (r *Receiver) poolKeysDir() string { return filepath.Join(r.stateDir, poolKeysDir) }

// keepCustody joins the pool's custodian group with the receiver ID as its instance ID and tends the pool keys at each
// custody interval and on each change of assignment, until ctx ends.
func (r *Receiver) keepCustody(ctx context.Context) {
	topic := admin.CustodianTopic(r.config.Pool)
	r.custody.wake = make(chan struct{}, 1)
	gained := func(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
		if len(partitions[topic]) > 0 {
			r.assign(true)
		}
	}
	lost := func(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
		if len(partitions[topic]) > 0 {
			r.assign(false)
		}
	}
	group, err := r.newClient(kgo.ConsumerGroup(admin.CustodianGroup(r.config.Pool)), kgo.ConsumeTopics(topic), kgo.InstanceID(r.id),
		kgo.DisableAutoCommit(), kgo.OnPartitionsAssigned(gained), kgo.OnPartitionsRevoked(lost), kgo.OnPartitionsLost(lost))
	if err != nil {
		r.logger.Printf("custodian group: %v", err)
		return
	}
	defer group.Close()
	defer r.assign(false)
	ticker := time.NewTicker(custodyInterval)
	defer ticker.Stop()
	for {
		if err := r.tend(ctx, time.Now()); err != nil && ctx.Err() == nil {
			r.logger.Printf("custody: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-r.custody.wake:
		}
	}
}

// assign records whether the group assigns this receiver the snapshot topic's partition. A receiver that loses it stops
// writing as custodian before the group moves on.
func (r *Receiver) assign(assigned bool) {
	r.custody.mu.Lock()
	defer r.custody.mu.Unlock()
	if assigned && !r.custody.assigned {
		r.custody.fenced = false
	}
	if !assigned && r.custody.producer != nil {
		r.logger.Printf("custodian: lost the custodian group's assignment for pool %s", r.config.Pool)
		r.dropProducer()
	}
	r.custody.assigned = assigned
	select {
	case r.custody.wake <- struct{}{}:
	default:
	}
}

func (r *Receiver) dropProducer() {
	r.custody.producer.Close()
	r.custody.producer = nil
}

// current is a read of the pool's snapshot topic: its latest committed record, if any, and that record's snapshot when it
// is a valid snapshot of the pool. resign is set when this receiver wrote it with a signing key it has since rotated.
type current struct {
	record   *kgo.Record
	snapshot *wire.Snapshot
	resign   bool
}

func (c current) names(r *Receiver) bool { return c.snapshot != nil && c.snapshot.Custodian == r.id }

func (r *Receiver) readCurrent(ctx context.Context) (current, error) {
	records, err := r.reader.ReadFrom(ctx, admin.CustodianTopic(r.config.Pool), 0)
	if err != nil {
		return current{}, err
	}
	var latest current
	for _, record := range records {
		if string(record.Key) == wire.CustodianKey {
			latest.record = record
		}
	}
	if latest.record == nil {
		return latest, nil
	}
	state, peers, _ := r.snapshot()
	if snapshot, err := admin.AuthorizeSnapshot(state, peers, latest.record.Value, r.config.Pool); err == nil {
		latest.snapshot = &snapshot
		return latest, nil
	}
	for _, retired := range peers[r.id].Retired {
		if !latest.record.Timestamp.Before(retired.At) {
			continue
		}
		keys := maps.Clone(peers)
		keys[r.id] = signed.KeyState{Current: retired.Key}
		if snapshot, err := admin.AuthorizeSnapshot(state, keys, latest.record.Value, r.config.Pool); err == nil && snapshot.Custodian == r.id {
			latest.snapshot, latest.resign = &snapshot, true
		}
	}
	return latest, nil
}

// tend keeps the deadlines of the pool keys the receiver holds and, while it is assigned and not fenced, acts as the
// pool's key custodian: it becomes custodian, republishes a snapshot its rotated signing key wrote, and rotates the pool
// key when due. now is the receiver's clock.
func (r *Receiver) tend(ctx context.Context, now time.Time) error {
	r.custody.mu.Lock()
	defer r.custody.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	becoming := r.custody.assigned && !r.custody.fenced && r.custody.producer == nil
	if becoming {
		producer, err := r.newClient(kgo.TransactionalID(r.custodianTransactionalID()))
		if err != nil {
			return err
		}
		// Initialising the producer before the read fences the previous custodian and settles its open transaction.
		if _, _, err := producer.ProducerID(ctx); err != nil {
			producer.Close()
			return fmt.Errorf("initialise %s: %w", r.custodianTransactionalID(), err)
		}
		r.custody.producer = producer
	}
	keys, err := loadPoolKeys(r.poolKeysDir())
	if err != nil {
		return err
	}
	latest, err := r.readCurrent(ctx)
	if err != nil {
		return fmt.Errorf("read snapshot: %w", err)
	}
	if err := r.keepKeys(keys, latest, now); err != nil {
		return err
	}
	if r.custody.producer == nil {
		return nil
	}
	if !latest.names(r) || keys[latest.snapshot.Current.KeyID] == nil {
		reason := "takeover"
		if latest.record == nil {
			reason = "bootstrap"
		}
		return r.mint(ctx, keys, reason, now)
	}
	if becoming {
		r.logger.Printf("custodian: continuing as custodian of pool %s with key %s", r.config.Pool, latest.snapshot.Current.KeyID)
	}
	if latest.resign {
		return r.publish(ctx, keys, latest.snapshot.Reason, keys[latest.snapshot.Current.KeyID], now)
	}
	if !now.Before(rotationAt(latest.record.Timestamp, r.config.Pool, latest.snapshot.Current.KeyID)) {
		return r.mint(ctx, keys, "rotation", now)
	}
	return nil
}

// rotationAt is when the pool key keyID, current since appended, is rotated: a day later and a jitter derived from the
// pool and key ID, so a restart does not move it.
func rotationAt(appended time.Time, pool, keyID string) time.Time {
	sum := sha256.Sum256([]byte(pool + "/" + keyID))
	return appended.Add(rotationInterval + time.Duration(binary.BigEndian.Uint64(sum[:8])%uint64(rotationJitter)))
}

// keepKeys marks the key latest names current for this receiver, gives every other key its deadline, and deletes the
// keys whose deadline has passed. An unpublished key is kept to its creation time and the longest any snapshot could
// have named it.
func (r *Receiver) keepKeys(keys map[string]*poolKey, latest current, now time.Time) error {
	state, _, _ := r.snapshot()
	window := state.Pools[r.config.Pool].StartByLimit() + keyGrace
	// A key replaced before this process started gets its deadline from now; one replaced since, from the replacing
	// snapshot's append time.
	replaced := now
	if r.custody.tended && latest.record != nil {
		replaced = latest.record.Timestamp
	}
	r.custody.tended = true
	for id, key := range keys {
		deadline := time.UnixMilli(key.DeadlineMS)
		switch {
		case latest.names(r) && latest.snapshot.Current.KeyID == id:
			if key.Published && key.DeadlineMS == 0 {
				continue
			}
			key.Published, key.DeadlineMS = true, 0
			if err := key.save(r.poolKeysDir()); err != nil {
				return err
			}
			continue
		case !key.Published:
			deadline = time.UnixMilli(key.CreatedMS).Add(window + rotationInterval)
		case key.DeadlineMS == 0:
			key.DeadlineMS = replaced.Add(window).UnixMilli()
			if err := key.save(r.poolKeysDir()); err != nil {
				return err
			}
			continue
		}
		if !now.Before(deadline) {
			if err := os.Remove(filepath.Join(r.poolKeysDir(), id+".json")); err != nil {
				return err
			}
			delete(keys, id)
			r.logger.Printf("custodian: deleted pool key %s at its deadline", id)
		}
	}
	return nil
}

// mint generates a pool key, persists it unpublished and publishes it as current for reason.
func (r *Receiver) mint(ctx context.Context, keys map[string]*poolKey, reason string, now time.Time) error {
	private, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	key := &poolKey{Key: jose.JSONWebKey{Key: private}, CreatedMS: now.UnixMilli()}
	if err := key.save(r.poolKeysDir()); err != nil {
		return err
	}
	keys[key.id()] = key
	return r.publish(ctx, keys, reason, key, now)
}

// publish commits a snapshot naming key current for reason in one transaction, reads the committed snapshot back, which
// also settles an ambiguous commit, and then marks key published. A failed write drops the producer, so the next pass
// initialises it again; a fenced one stops the receiver writing as custodian.
func (r *Receiver) publish(ctx context.Context, keys map[string]*poolKey, reason string, key *poolKey, now time.Time) error {
	public := &key.Key.Key.(*ecdsa.PrivateKey).PublicKey
	value := r.sign(&wire.Snapshot{Pool: r.config.Pool, Custodian: r.id, Reason: reason, Current: wire.PoolKey{KeyID: key.id(), Key: jose.JSONWebKey{Key: public}}})
	producer := r.custody.producer
	err := producer.BeginTransaction()
	if err == nil {
		if err = producer.ProduceSync(ctx, &kgo.Record{Topic: admin.CustodianTopic(r.config.Pool), Key: []byte(wire.CustodianKey), Value: value}).FirstErr(); err != nil {
			_ = producer.EndTransaction(ctx, kgo.TryAbort)
		} else {
			err = producer.EndTransaction(ctx, kgo.TryCommit)
		}
	}
	latest, readErr := r.readCurrent(ctx)
	committed := readErr == nil && latest.record != nil && bytes.Equal(latest.record.Value, value)
	if err != nil || !committed {
		if errors.Is(err, kerr.ProducerFenced) || errors.Is(err, kerr.InvalidProducerEpoch) {
			r.custody.fenced = true
			r.logger.Printf("custodian: fenced by a successor in pool %s; writing no more snapshots", r.config.Pool)
		}
		r.dropProducer()
	}
	if !committed {
		return fmt.Errorf("publish %s snapshot: %w", reason, errors.Join(err, readErr, errors.New("not read back")))
	}
	if err := r.keepKeys(keys, latest, now); err != nil {
		return err
	}
	r.logger.Printf("custodian: published pool %s key %s (%s)", r.config.Pool, key.id(), reason)
	if reason == "takeover" {
		r.metrics.takeovers.WithLabelValues(reason).Inc()
		r.logger.Printf("custodian: took over pool %s (WherryCustodianTakeover)", r.config.Pool)
	}
	return nil
}

// save writes key to dir through a temporary file, synced with the directory, with mode 0600.
func (key *poolKey) save(dir string) error {
	encoded, err := json.Marshal(key)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(dir, ".pool-key-*")
	if err != nil {
		return err
	}
	_, err = file.Write(encoded)
	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = trust.ReplaceKeyFile(file.Name(), filepath.Join(dir, key.id()+".json"))
	}
	if err != nil {
		_ = os.Remove(file.Name())
	}
	return err
}

// loadPoolKeys reads the pool keys in dir by key ID, removing temporary files a crash left: each is an update of a key
// whose file is intact, or a key never published.
func loadPoolKeys(dir string) (map[string]*poolKey, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	keys := map[string]*poolKey{}
	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())
		if strings.HasPrefix(entry.Name(), ".pool-key-") {
			if err := os.Remove(path); err != nil {
				return nil, err
			}
			continue
		}
		id, ok := strings.CutSuffix(entry.Name(), ".json")
		if !ok {
			continue
		}
		encoded, err := trust.ReadOwnerOnly(path)
		if err != nil {
			return nil, err
		}
		key := &poolKey{}
		if err := json.Unmarshal(encoded, key, signed.Strict); err != nil {
			return nil, fmt.Errorf("pool key %s: %w", entry.Name(), err)
		}
		if private, ok := key.Key.Key.(*ecdsa.PrivateKey); !ok || private.Curve != elliptic.P256() || key.id() != id {
			return nil, fmt.Errorf("pool key %s is not the P-256 private key its name gives", entry.Name())
		}
		keys[id] = key
	}
	return keys, nil
}
