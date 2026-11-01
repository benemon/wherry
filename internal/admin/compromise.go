package admin

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"time"

	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/twmb/franz-go/pkg/kadm"
)

// PoolSuspend records pool suspended, or resumed when suspend is false, and reconciles the ACLs to match.
func (service *Service) PoolSuspend(ctx context.Context, pool string, suspend bool) error {
	_, err := service.poolSuspend(ctx, pool, suspend)
	return err
}

// PoolSuspendAcknowledged is PoolSuspend that then waits for pool's key custodian to acknowledge the record, reporting
// the wait to progress, and returns the outcome. With no snapshot it returns at once: a custodian's first pass reads
// the record before it opens anything. A stale advertisement does not end custody, so a snapshot is always awaited.
func (service *Service) PoolSuspendAcknowledged(ctx context.Context, pool string, suspend bool, progress io.Writer) (string, error) {
	recorded, err := service.poolSuspend(ctx, pool, suspend)
	if err != nil {
		return "", err
	}
	kind, effect := "resume", "resumed: its key custodian acknowledged the resume record at wherry.admin offset %d; first starts open again"
	if suspend {
		kind, effect = "suspend", "suspended: its key custodian acknowledged the suspend record at wherry.admin offset %d; no new start opens"
	}
	reader := NewReader(service.Admin)
	defer reader.Close()
	state, err := service.LoadState(ctx)
	if err != nil {
		return "", err
	}
	if _, err := currentSnapshot(ctx, reader, state, pool); errors.Is(err, errNoSnapshot) {
		return fmt.Sprintf("pool %s's %s record is written at wherry.admin offset %d; the pool has no key custodian yet, so nothing opens until one reads it", pool, kind, recorded), nil
	} else if err != nil {
		return "", err
	}
	err = awaitSnapshot(ctx, reader, state, pool, progress, func(current wire.Snapshot) bool {
		return suspend && current.SuspendedFor >= recorded || !suspend && current.ResumedFor >= recorded
	}, fmt.Sprintf("waiting for pool %s's key custodian to acknowledge the %s record at wherry.admin offset %d", pool, kind, recorded),
		fmt.Errorf("pool %s's %s record is written at wherry.admin offset %d, but its key custodian did not acknowledge it within %s; it is not yet in effect", pool, kind, recorded, snapshotWait))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("pool %s is "+effect, pool, recorded), nil
}

// poolSuspend appends pool's suspend or resume record, reconciles the ACLs and returns the record's offset.
func (service *Service) poolSuspend(ctx context.Context, pool string, suspend bool) (int64, error) {
	kind := "resume"
	if suspend {
		kind = "suspend"
	}
	state, recorded, err := service.appendRecord(ctx, []byte(kind+"."+pool), Record{Kind: kind, Pool: pool})
	if err != nil {
		return 0, err
	}
	return recorded, service.reconcileACLs(ctx, state)
}

// SuspectTask is a retained task record whose submit wrap is to a suspect key: names only, never a value.
type SuspectTask struct {
	Run, Tenant, KeyID string
	Credentials        []string
	AppendedMS         int64
}

// RotateReport lists the retained tasks under the suspect keys. Horizon is each task topic partition's oldest retained
// offset after the scan and OldestMS the append time of the oldest retained record, 0 with none: a task deleted by
// retention before them is not listed. DeletedFrom is, for each partition whose log start retention moved during the
// scan, its start before the scan: tasks from there to the horizon were deleted during it.
type RotateReport struct {
	SuspectKeys []string
	Tasks       []SuspectTask
	Horizon     map[int32]int64
	DeletedFrom map[int32]int64
	OldestMS    int64
}

// snapshotWait bounds an admin command's wait for the snapshot acknowledging its record, and snapshotProgress separates
// the lines reporting it; variables so tests can shorten them. advertisementLive is how recent a live receiver's
// advertisement is.
var snapshotWait, snapshotProgress = 5 * time.Minute, 10 * time.Second

const advertisementLive = 120 * time.Second

// awaitSnapshot waits up to snapshotWait for a current snapshot of pool that done accepts, writing waiting and the time
// left to progress every snapshotProgress, and fails with timeout without one.
func awaitSnapshot(ctx context.Context, reader *Reader, state State, pool string, progress io.Writer, done func(wire.Snapshot) bool, waiting string, timeout error) error {
	deadline, reported := time.Now().Add(snapshotWait), time.Time{}
	for {
		current, err := currentSnapshot(ctx, reader, state, pool)
		if err == nil && done(current) {
			return nil
		}
		if time.Now().After(deadline) {
			return timeout
		}
		if time.Since(reported) >= snapshotProgress {
			reported = time.Now()
			fmt.Fprintf(progress, "%s, up to %s\n", waiting, time.Until(deadline).Truncate(time.Second))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// PoolRotateKey records a rotate-key request for a suspended pool, waits for its key custodian's snapshot rotated for it,
// reporting the wait to progress, and then reports every task retained through the task topic's end at that point whose
// submit wrap is to one of suspects, by default the current key when the request is made. A task record is listed as
// written, verified or not, since its envelope is exposed either way. Its credential names are its own and those every
// terms record of the pool on wherry.admin gives its agent: a superset, since an incident inventory must not miss one.
func (service *Service) PoolRotateKey(ctx context.Context, pool string, suspects []string, progress io.Writer) (RotateReport, error) {
	operator := NewReader(service.Operator)
	defer operator.Close()
	raw, err := readRaw(ctx, operator)
	if err != nil {
		return RotateReport{}, err
	}
	state, err := load(raw)
	if err != nil {
		return RotateReport{}, err
	}
	if _, ok := state.Pools[pool]; !ok {
		return RotateReport{}, errors.New("no pool by that name")
	}
	// A pool taking tasks would add to the inventory after it is taken.
	if !state.Suspended[pool] {
		return RotateReport{}, fmt.Errorf("pool %s is not suspended; run wherry admin pool suspend %s first", pool, pool)
	}
	reader := NewReader(service.Admin)
	defer reader.Close()
	if len(suspects) == 0 {
		current, err := currentSnapshot(ctx, reader, state, pool)
		if err != nil {
			return RotateReport{}, fmt.Errorf("%w; name the suspect keys with --suspect-key", err)
		}
		suspects = []string{current.Current.KeyID}
	}
	_, requested, err := service.appendRecord(ctx, []byte("rotate-key."+pool), Record{Kind: "rotate-key", Pool: pool})
	if err != nil {
		return RotateReport{}, err
	}
	err = awaitSnapshot(ctx, reader, state, pool, progress, func(current wire.Snapshot) bool { return current.RotatedFor >= requested },
		fmt.Sprintf("waiting for pool %s's key custodian to rotate for the request at wherry.admin offset %d", pool, requested),
		fmt.Errorf("pool %s's key custodian published no snapshot rotated for the rotate-key request at wherry.admin offset %d within %s; the request stands", pool, requested, snapshotWait))
	if err != nil {
		return RotateReport{}, err
	}
	listing, err := reader.Admin()
	if err != nil {
		return RotateReport{}, err
	}
	// Every task appended before the rotated snapshot was read lies below these ends.
	ends, err := ListOffsets(ctx, listing.ListEndOffsets, PoolTasksTopic(pool))
	if err != nil {
		return RotateReport{}, err
	}
	starts, err := ListOffsets(ctx, listing.ListStartOffsets, PoolTasksTopic(pool))
	if err != nil {
		return RotateReport{}, err
	}
	report := RotateReport{SuspectKeys: suspects, Horizon: map[int32]int64{}, DeletedFrom: map[int32]int64{}}
	starts.Each(func(listed kadm.ListedOffset) { report.Horizon[listed.Partition] = listed.Offset })
	history := termsHistory(raw, pool)
	records, err := reader.ReadFrom(ctx, PoolTasksTopic(pool), 0)
	if err != nil {
		return RotateReport{}, err
	}
	// A fetch below the log start begins at the start then in effect, so the horizon is the one listed after the scan.
	scanned, err := ListOffsets(ctx, listing.ListStartOffsets, PoolTasksTopic(pool))
	if err != nil {
		return RotateReport{}, err
	}
	scanned.Each(func(listed kadm.ListedOffset) {
		if before := report.Horizon[listed.Partition]; listed.Offset > before {
			report.DeletedFrom[listed.Partition], report.Horizon[listed.Partition] = before, listed.Offset
		}
	})
	for _, record := range records {
		if end, ok := ends.Lookup(record.Topic, record.Partition); !ok || record.Offset >= end.Offset {
			continue
		}
		if appended := record.Timestamp.UnixMilli(); report.OldestMS == 0 || appended < report.OldestMS {
			report.OldestMS = appended
		}
		jws, err := jose.ParseSignedCompact(string(record.Value), []jose.SignatureAlgorithm{jose.EdDSA})
		if err != nil {
			continue
		}
		var task wire.Submission
		if json.Unmarshal(jws.UnsafePayloadWithoutVerification(), &task, signed.Tolerant) != nil {
			continue
		}
		kid, err := signed.WrapKeyID([]byte(task.Wherry.Wrap))
		if err != nil || !slices.Contains(suspects, kid) {
			continue
		}
		names := map[string]bool{}
		for _, credential := range task.Wherry.Credentials {
			names[credential.Name] = true
		}
		for _, terms := range history {
			if agent, err := envelopeCredentials(terms, task.Wherry); err == nil {
				maps.Copy(names, agent)
			}
		}
		report.Tasks = append(report.Tasks, SuspectTask{task.Wherry.Run, task.Wherry.Tenant, kid, slices.Sorted(maps.Keys(names)), record.Timestamp.UnixMilli()})
	}
	return report, nil
}

// termsHistory is every terms record of pool among raw, wherry.admin's records, which keep them all, in offset order.
func termsHistory(raw []rawRecord, pool string) []Terms {
	var history []Terms
	for _, entry := range raw {
		record, err := DecodeRecord(entry.Value)
		if err == nil && record.Kind == "pool" && record.Pool == pool {
			history = append(history, *record.Terms)
		}
	}
	return history
}

// currentSnapshot is the latest committed snapshot of pool, verified by its writer's key, live or ended.
func currentSnapshot(ctx context.Context, reader *Reader, state State, pool string) (wire.Snapshot, error) {
	records, err := reader.ReadFrom(ctx, CustodianTopic(pool), 0)
	if err != nil {
		return wire.Snapshot{}, err
	}
	topics := PoolKeyTopics(state, pool)
	maps.Copy(topics, EndedPoolKeyTopics(state, pool))
	keys, err := reader.ReadKeys(ctx, topics)
	if err != nil {
		return wire.Snapshot{}, err
	}
	for _, record := range slices.Backward(records) {
		if string(record.Key) != wire.CustodianKey {
			continue
		}
		snapshot, err := wire.ParseSnapshotHistory(record.Value, keys, record.Timestamp)
		if err != nil || snapshot.Pool != pool {
			return wire.Snapshot{}, fmt.Errorf("pool %s's current snapshot does not verify", pool)
		}
		return snapshot, nil
	}
	return wire.Snapshot{}, fmt.Errorf("pool %s has %w", pool, errNoSnapshot)
}

var errNoSnapshot = errors.New("no custodian snapshot")
