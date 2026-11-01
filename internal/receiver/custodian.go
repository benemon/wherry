package receiver

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/trust"
	"github.com/benemon/wherry/internal/wire"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/twmb/franz-go/pkg/kadm"
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
	// commitAttempts bounds the commits of one snapshot write; commitRetryInterval separates them.
	commitAttempts      = 3
	commitRetryInterval = 200 * time.Millisecond
	// sessionTimeout is the custodian group's session timeout, also the window in which a second fence stands a
	// contesting custodian down.
	sessionTimeout = 45 * time.Second
	// advertisementLive is how recent a receiver's advertisement is while the receiver is live.
	advertisementLive = 120 * time.Second
	// stepDownMargin is how long a drain may take beyond the drain timeout before any handover: half of it bounds the
	// read of live members and the stepping_down advertisement, the rest the leave of the custodian group.
	stepDownMargin = 5 * time.Second
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
	// fenced is set when the receiver is superseded, its producer fenced twice within the session timeout or another
	// receiver's snapshot committed after its failed write, until the next assignment.
	fenced bool
	// contested is set when its producer was fenced while assigned, until a snapshot it republishes is read back;
	// contestedAt is when.
	contested   bool
	contestedAt time.Time
	// tended is set after the first pass of the process.
	tended bool
	wake   chan struct{}
}

// poolKey is a pool private key in the receiver's state directory with its custody record.
type poolKey struct {
	Key       jose.JSONWebKey `json:"key"`
	CreatedMS int64           `json:"created_ms"`
	// PublishedMS is the append time of the committed snapshot first read back naming the key current.
	PublishedMS int64 `json:"published_ms,omitzero"`
	// DeadlineMS is when the key is deleted, set once a later snapshot replaces it.
	DeadlineMS int64 `json:"deadline_ms,omitzero"`
}

func (key *poolKey) id() string {
	return signed.KeyThumbprint(&key.Key.Key.(*ecdsa.PrivateKey).PublicKey)
}

// custodianTransactionalID is the one transactional ID every custodian of the pool writes through, so a successor's
// initialisation fences its predecessor.
func (r *Receiver) custodianTransactionalID() string {
	return admin.TransactionalPrefix(r.config.Pool) + "custodian"
}

func (r *Receiver) poolKeysDir() string { return filepath.Join(r.stateDir, poolKeysDir) }

// keepCustody joins the pool's custodian group with the receiver ID as its instance ID and tends the pool keys at each
// custody interval and on each change of assignment, until ctx ends, when a custodian steps down, or running ends.
func (r *Receiver) keepCustody(ctx, running context.Context) {
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
	// The group only assigns custody: it is never polled and commits no offsets, so the offsets it fetches on assignment
	// never decide the current snapshot; readCurrent scans the topic for it.
	group, err := r.newClient(kgo.ConsumerGroup(admin.CustodianGroup(r.config.Pool)), kgo.ConsumeTopics(topic), kgo.InstanceID(r.id),
		kgo.SessionTimeout(sessionTimeout), kgo.DisableAutoCommit(), kgo.OnPartitionsAssigned(gained), kgo.OnPartitionsRevoked(lost), kgo.OnPartitionsLost(lost))
	if err != nil {
		r.logger.Printf("custodian group: %v", err)
		return
	}
	defer func() {
		if group != nil {
			group.Close()
		}
	}()
	defer r.assign(false)
	ticker := time.NewTicker(custodyInterval)
	defer ticker.Stop()
	for {
		// A revoked member leaves the custodian group.
		if group != nil && r.revoked() {
			group.Close()
			group = nil
			r.assign(false)
		}
		if err := r.tend(running, time.Now()); err != nil && running.Err() == nil {
			r.logger.Printf("custody: %v", err)
		}
		select {
		case <-ctx.Done():
			if group != nil {
				r.stepDown(ctx, group)
				group = nil
			}
			return
		case <-running.Done():
			return
		case <-ticker.C:
		case <-r.custody.wake:
		}
	}
}

// stepDown relinquishes the custodian group at a drain, closing group and removing its static membership. A custodian
// with a drain timeout and another live member first advertises stepping_down and afterwards waits up to the drain timeout
// for a successor's snapshot: with one it hands its keys over; with none it keeps them to their deadlines, so its restart
// is an ordinary custodian restart. Any other custodian leaves without stepping_down, so its successor takes over.
func (r *Receiver) stepDown(ctx context.Context, group *kgo.Client) {
	// The steps before a handover end within the drain timeout and stepDownMargin, whatever ctx.
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.drainTimeout+stepDownMargin)
	defer cancel()
	r.custody.mu.Lock()
	acting := r.custody.producer != nil
	r.custody.mu.Unlock()
	waiting := false
	if acting && r.drainTimeout > 0 {
		adCtx, cancel := context.WithTimeout(bounded, stepDownMargin/2)
		live, err := r.peerLive(adCtx)
		if waiting = err == nil && live; waiting {
			r.mu.Lock()
			r.steppingDown = &wire.SteppingDown{AtMS: time.Now().UnixMilli(), StartedMS: r.started.UnixMilli()}
			r.mu.Unlock()
			if err := r.advertise(adCtx); err != nil {
				r.logger.Printf("custodian: advertise stepping down: %v", err)
			}
		}
		cancel()
	}
	// A closed static member keeps its assignment until the session timeout; removing it hands the partition on at once.
	group.Close()
	r.assign(false)
	left, err := kadm.NewClient(r.base).LeaveGroup(bounded, kadm.LeaveGroup(admin.CustodianGroup(r.config.Pool)).InstanceIDs(r.id))
	if err == nil {
		err = left.Error()
	}
	if err != nil {
		r.logger.Printf("custodian: leave the custodian group: %v", err)
	}
	switch {
	case !acting:
		return
	case r.drainTimeout == 0:
		r.logger.Printf("custodian: leaving pool %s with no drain timeout; keeping the pool keys to their deadlines", r.config.Pool)
		return
	case waiting:
		r.logger.Printf("custodian: stepping down in pool %s; waiting up to %s for a successor", r.config.Pool, r.drainTimeout)
		waitCtx, cancel := context.WithTimeout(bounded, r.drainTimeout)
		defer cancel()
		for waitCtx.Err() == nil {
			latest, err := r.readCurrent(waitCtx)
			if err == nil && latest.snapshot != nil && latest.snapshot.Custodian != r.id {
				handCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), readTimeout)
				defer cancel()
				if err := r.handOver(handCtx, latest.snapshot.Current); err != nil {
					r.logger.Printf("custodian: hand over to %s: %v; keeping the pool keys to their deadlines", latest.snapshot.Custodian, err)
				}
				return
			}
			// With no other live member there is no successor to wait for.
			if live, err := r.peerLive(waitCtx); err == nil && !live {
				break
			}
			sleep(waitCtx, custodyInterval)
		}
	}
	r.logger.Printf("custodian: no successor in pool %s; keeping the pool keys to their deadlines", r.config.Pool)
}

// peerLive reports whether another member of the pool has a live advertisement.
func (r *Receiver) peerLive(ctx context.Context) (bool, error) {
	state, peers, _ := r.snapshot()
	live, err := r.reader.ReadAdvertisements(ctx, state, peers, time.Now(), advertisementLive)
	delete(live, r.id)
	return len(live) > 0, err
}

// handOver writes, in one transaction, a re-key wrap to the successor's current key for every task the receiver's keys
// open that is its run's earliest accepted, not terminal and not past its task_ended_by, from one read of the task topic
// to its last stable offset, and then deletes the keys. Any error keeps them.
func (r *Receiver) handOver(ctx context.Context, current wire.PoolKey) error {
	r.custody.mu.Lock()
	defer r.custody.mu.Unlock()
	keys, err := loadPoolKeys(r.poolKeysDir())
	if err != nil {
		return err
	}
	tasks, err := r.reader.ReadFrom(ctx, admin.PoolTasksTopic(r.config.Pool), 0)
	if err != nil {
		r.deny(err, capRead)
		return err
	}
	events, err := r.readEvents(ctx)
	if err != nil {
		return err
	}
	var records []*kgo.Record
	seen := map[string]bool{}
	for _, record := range tasks {
		run := string(record.Key)
		if seen[run] {
			continue
		}
		seen[run] = true
		task := r.earliest(tasks, run)
		if task == nil || events.committedTerminal(run, task.hash) || !time.Now().Before(wire.PastBound(task.record.Wherry.TaskEndedBy)) ||
			slices.ContainsFunc(events.rekeys[task.hash], func(wrap wire.RekeyWrap) bool { return wrap.RecipientKID == current.KeyID }) {
			continue
		}
		contentKey := r.contentKey(task.record, keys, events.rekeys[task.hash])
		if contentKey == nil {
			continue
		}
		rekey, err := r.rekeyRecord(run, task.hash, contentKey, current)
		if err != nil {
			return err
		}
		records = append(records, rekey)
	}
	if len(records) > 0 {
		if err := r.admissionCommit(ctx, records...); err != nil {
			return fmt.Errorf("re-key: %w", err)
		}
	}
	for id := range keys {
		if err := os.Remove(filepath.Join(r.poolKeysDir(), id+".json")); err != nil {
			return err
		}
	}
	r.logger.Printf("custodian: re-keyed %d tasks to pool key %s and deleted %d pool keys", len(records), current.KeyID, len(keys))
	return nil
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

func (r *Receiver) revoked() bool {
	state, _, _ := r.snapshot()
	return state.Ended[admin.ReceiverPrincipal(r.id)]
}

func (r *Receiver) dropProducer() {
	r.custody.producer.Close()
	r.custody.producer = nil
}

// current is a read of the pool's snapshot topic: its latest committed record, if any, and that record's snapshot when it
// is a valid snapshot of the pool, verified by its append time. resign is set when this receiver wrote it with a signing
// key it has since rotated.
type current struct {
	record   *kgo.Record
	snapshot *wire.Snapshot
	resign   bool
}

func (c current) names(r *Receiver) bool { return c.snapshot != nil && c.snapshot.Custodian == r.id }

func (r *Receiver) readCurrent(ctx context.Context) (current, error) {
	topic := admin.CustodianTopic(r.config.Pool)
	listing, err := r.reader.Admin()
	if err != nil {
		return current{}, err
	}
	ends, err := admin.ListOffsets(ctx, listing.ListEndOffsets, topic)
	if err != nil {
		return current{}, err
	}
	records, stable, err := r.reader.ReadFromWithEnd(ctx, topic, 0)
	if err != nil {
		return current{}, err
	}
	// A transaction open below the end holds the last stable offset: the snapshot it may commit would be current.
	var end int64
	ends.Each(func(listed kadm.ListedOffset) { end = listed.Offset })
	if stable < end {
		return current{}, errors.New("an open transaction holds the snapshot topic's last stable offset")
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
	snapshot, err := admin.AuthorizeSnapshotHistory(state, peers, latest.record.Value, r.config.Pool, latest.record.Timestamp)
	if err != nil {
		return latest, nil
	}
	latest.snapshot = &snapshot
	if snapshot.Custodian == r.id {
		_, err := signed.Verify(latest.record.Value, wire.CustodianType, peers, new(jsontext.Value))
		latest.resign = err != nil
	}
	return latest, nil
}

// tend keeps the deadlines of the pool keys the receiver holds and, while it is assigned, not superseded and not
// revoked, acts as the pool's key custodian: it becomes custodian, republishes a snapshot its rotated signing key wrote,
// and rotates the pool key when due. now is the receiver's clock.
func (r *Receiver) tend(ctx context.Context, now time.Time) error {
	r.custody.mu.Lock()
	defer r.custody.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	revoked := r.revoked()
	if revoked && r.custody.producer != nil {
		r.logger.Printf("custodian: membership of pool %s ended; writing no more snapshots", r.config.Pool)
		r.dropProducer()
	}
	// The transactional ID is initialised only on an assignment: a later initialisation would fence a successor.
	becoming := r.custody.assigned && !r.custody.fenced && !revoked && r.custody.producer == nil
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
		switch {
		case latest.record == nil:
			reason = "bootstrap"
		case r.custody.contested:
			reason = "contested"
		case r.handedOver(ctx, latest, now):
			reason = "handover"
		}
		return r.mint(ctx, keys, reason, latest, now)
	}
	if becoming {
		r.logger.Printf("custodian: continuing as custodian of pool %s with key %s", r.config.Pool, latest.snapshot.Current.KeyID)
	}
	key := keys[latest.snapshot.Current.KeyID]
	if r.custody.contested {
		return r.publish(ctx, keys, "contested", key, latest, now)
	}
	if latest.resign {
		return r.publish(ctx, keys, latest.snapshot.Reason, key, latest, now)
	}
	if !now.Before(rotationAt(time.UnixMilli(key.PublishedMS), r.config.Pool, latest.snapshot.Current.KeyID)) {
		return r.mint(ctx, keys, "rotation", latest, now)
	}
	return nil
}

// handedOver reports that latest's custodian, another receiver, hands custody on: its latest advertisement is live at now
// and carries stepping_down set by the process that wrote it. The same receiver back is a later process.
func (r *Receiver) handedOver(ctx context.Context, latest current, now time.Time) bool {
	if latest.snapshot == nil || latest.snapshot.Custodian == r.id {
		return false
	}
	state, peers, _ := r.snapshot()
	live, err := r.reader.ReadAdvertisements(ctx, state, peers, now, advertisementLive)
	if err != nil {
		r.logger.Printf("custodian: read advertisements: %v", err)
		return false
	}
	advertisement, ok := live[latest.snapshot.Custodian]
	return ok && advertisement.SteppingDown != nil && advertisement.SteppingDown.StartedMS == advertisement.StartedMS
}

// contest handles the custodian producer's fence while the group still assigns this receiver the partition: the first
// fence re-initialises the producer on the next pass, which republishes the snapshot with reason contested; a second
// within the session timeout stands the receiver down until it is assigned again.
func (r *Receiver) contest() {
	r.dropProducer()
	if time.Since(r.custody.contestedAt) < sessionTimeout {
		r.custody.fenced = true
		r.logger.Printf("custodian: fenced again within the session timeout in pool %s; standing down until assigned again", r.config.Pool)
		return
	}
	r.custody.contested, r.custody.contestedAt = true, time.Now()
	r.metrics.takeovers.WithLabelValues("contested").Inc()
	r.logger.Printf("custodian: fenced while assigned in pool %s; re-initialising to republish (WherryCustodianTakeover)", r.config.Pool)
	select {
	case r.custody.wake <- struct{}{}:
	default:
	}
}

// rotationAt is when the pool key keyID, published at appended, is rotated: a day later and a jitter derived from the
// pool and key ID, so a restart does not move it.
func rotationAt(appended time.Time, pool, keyID string) time.Time {
	sum := sha256.Sum256([]byte(pool + "/" + keyID))
	return appended.Add(rotationInterval + time.Duration(binary.BigEndian.Uint64(sum[:8])%uint64(rotationJitter)))
}

// keepKeys marks the key latest names current for this receiver published at latest's append time, gives every other
// key its deadline, and deletes the keys whose deadline has passed. An unmarked key not named, which a snapshot before
// latest may have named, gets its deadline from latest's append time; with no snapshot it was never named. A takeover
// is counted when its key is marked, so once, also when its commit landed but its read-back failed.
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
			if key.PublishedMS != 0 && key.DeadlineMS == 0 {
				continue
			}
			minted := key.PublishedMS == 0
			if minted {
				key.PublishedMS = latest.record.Timestamp.UnixMilli()
			}
			key.DeadlineMS = 0
			if err := key.save(r.poolKeysDir()); err != nil {
				return err
			}
			if minted && latest.snapshot.Reason == "takeover" {
				r.metrics.takeovers.WithLabelValues("takeover").Inc()
				r.logger.Printf("custodian: took over pool %s (WherryCustodianTakeover)", r.config.Pool)
			}
			continue
		case key.DeadlineMS == 0:
			from := replaced
			if key.PublishedMS == 0 {
				if latest.record == nil {
					continue
				}
				from = latest.record.Timestamp
			}
			key.DeadlineMS = from.Add(window).UnixMilli()
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
func (r *Receiver) mint(ctx context.Context, keys map[string]*poolKey, reason string, from current, now time.Time) error {
	private, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	key := &poolKey{Key: jose.JSONWebKey{Key: private}, CreatedMS: now.UnixMilli()}
	if err := key.save(r.poolKeysDir()); err != nil {
		return err
	}
	keys[key.id()] = key
	return r.publish(ctx, keys, reason, key, from, now)
}

// publish commits a snapshot naming key current for reason in one transaction, reads the committed snapshot back, which
// also settles an ambiguous commit, and then marks key published. from is the read the pass started from. A failed write
// keeps the producer for the next pass, unless the producer was fenced or another receiver's snapshot committed after
// from: then the receiver is superseded and writes no more until it is assigned again.
func (r *Receiver) publish(ctx context.Context, keys map[string]*poolKey, reason string, key *poolKey, from current, now time.Time) error {
	public := &key.Key.Key.(*ecdsa.PrivateKey).PublicKey
	value := r.sign(&wire.Snapshot{Pool: r.config.Pool, Custodian: r.id, Reason: reason, Current: wire.PoolKey{KeyID: key.id(), Key: jose.JSONWebKey{Key: public}}})
	producer := r.custody.producer
	// A transaction a failed pass left open is aborted first.
	err := producer.EndTransaction(ctx, kgo.TryAbort)
	if err == nil {
		err = producer.BeginTransaction()
	}
	if err == nil {
		if err = producer.ProduceSync(ctx, &kgo.Record{Topic: admin.CustodianTopic(r.config.Pool), Key: []byte(wire.CustodianKey), Value: value}).FirstErr(); err == nil {
			err = commitSnapshot(ctx, producer)
			if err != nil && !fenced(err) && !authorisationError(err) {
				r.metrics.ambiguousCommits.Inc()
			}
		}
		if err != nil {
			_ = producer.EndTransaction(ctx, kgo.TryAbort)
		}
	}
	latest, readErr := r.readCurrent(ctx)
	if readErr != nil || latest.record == nil || !bytes.Equal(latest.record.Value, value) {
		superseded := readErr == nil && latest.record != nil && !latest.names(r) && (from.record == nil || latest.record.Offset > from.record.Offset)
		if fenced(err) {
			r.contest()
		} else if superseded {
			r.custody.fenced = true
			r.logger.Printf("custodian: superseded in pool %s; writing no more snapshots until assigned again", r.config.Pool)
			r.dropProducer()
		}
		return fmt.Errorf("publish %s snapshot: %w", reason, errors.Join(err, readErr, errors.New("not read back")))
	}
	r.custody.contested = false
	if err := r.keepKeys(keys, latest, now); err != nil {
		return err
	}
	r.logger.Printf("custodian: published pool %s key %s (%s)", r.config.Pool, key.id(), reason)
	return nil
}

// commitSnapshot commits producer's transaction, retrying a transient error up to commitAttempts in all.
func commitSnapshot(ctx context.Context, producer *kgo.Client) error {
	err := commitTransaction(ctx, producer)
	for attempt := 1; attempt < commitAttempts && err != nil && !fenced(err) && !authorisationError(err); attempt++ {
		sleep(ctx, commitRetryInterval)
		err = commitTransaction(ctx, producer)
	}
	return err
}

func fenced(err error) bool {
	return errors.Is(err, kerr.ProducerFenced) || errors.Is(err, kerr.InvalidProducerEpoch)
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
