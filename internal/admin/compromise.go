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
	kind := "resume"
	if suspend {
		kind = "suspend"
	}
	state, err := service.appendChecked(ctx, []byte(kind+"."+pool), Record{Kind: kind, Pool: pool})
	if err != nil {
		return err
	}
	return service.reconcileACLs(ctx, state)
}

// SuspectTask is a retained task record whose submit wrap is to a suspect key: names only, never a value.
type SuspectTask struct {
	Run, Tenant, KeyID string
	Credentials        []string
	AppendedMS         int64
}

// RotateReport lists the retained tasks under the suspect keys. Horizon is each task topic partition's oldest retained
// offset and OldestMS the append time of the oldest retained record, 0 with none: a task deleted by retention before
// them is not listed.
type RotateReport struct {
	SuspectKeys []string
	Tasks       []SuspectTask
	Horizon     map[int32]int64
	OldestMS    int64
}

// rotateWait bounds rotate-key's wait for the snapshot rotated for its request, and rotateProgress separates the lines
// reporting it; variables so tests can shorten them.
var rotateWait, rotateProgress = 5 * time.Minute, 10 * time.Second

// PoolRotateKey records a rotate-key request for a suspended pool, waits for its key custodian's snapshot rotated for it,
// reporting the wait to progress, and then reports every task retained through the task topic's end at that point whose
// submit wrap is to one of suspects, by default the current key when the request is made. A task record is listed as
// written, verified or not, since its envelope is exposed either way. Its credential names are those of the pool's
// terms in force when it was appended and its own, never the current terms'.
func (service *Service) PoolRotateKey(ctx context.Context, pool string, suspects []string, progress io.Writer) (RotateReport, error) {
	state, err := service.LoadState(ctx)
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
	deadline, reported := time.Now().Add(rotateWait), time.Time{}
	for {
		current, err := currentSnapshot(ctx, reader, state, pool)
		if err == nil && current.RotatedFor >= requested {
			break
		}
		if time.Now().After(deadline) {
			return RotateReport{}, fmt.Errorf("pool %s's key custodian published no snapshot rotated for the rotate-key request at wherry.admin offset %d within %s; the request stands", pool, requested, rotateWait)
		}
		if time.Since(reported) >= rotateProgress {
			reported = time.Now()
			fmt.Fprintf(progress, "waiting for pool %s's key custodian to rotate for the request at wherry.admin offset %d, up to %s\n", pool, requested, time.Until(deadline).Truncate(time.Second))
		}
		select {
		case <-ctx.Done():
			return RotateReport{}, ctx.Err()
		case <-time.After(time.Second):
		}
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
	report := RotateReport{SuspectKeys: suspects, Horizon: map[int32]int64{}}
	starts.Each(func(listed kadm.ListedOffset) { report.Horizon[listed.Partition] = listed.Offset })
	history, err := termsHistory(ctx, reader, pool)
	if err != nil {
		return RotateReport{}, err
	}
	records, err := reader.ReadFrom(ctx, PoolTasksTopic(pool), 0)
	if err != nil {
		return RotateReport{}, err
	}
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
		// The terms in force at the task's append are the last terms record appended no later than it.
		var inForce *Terms
		for _, dated := range history {
			if dated.appendedMS <= record.Timestamp.UnixMilli() {
				inForce = &dated.terms
			}
		}
		if inForce != nil {
			if agent, err := envelopeCredentials(*inForce, task.Wherry); err == nil {
				maps.Copy(names, agent)
			}
		}
		report.Tasks = append(report.Tasks, SuspectTask{task.Wherry.Run, task.Wherry.Tenant, kid, slices.Sorted(maps.Keys(names)), record.Timestamp.UnixMilli()})
	}
	return report, nil
}

// datedTerms are a pool's terms as a wherry.admin pool record set them, with the record's append time.
type datedTerms struct {
	terms      Terms
	appendedMS int64
}

// termsHistory is every terms record of pool on wherry.admin, which keeps them all, in offset order.
func termsHistory(ctx context.Context, reader *Reader, pool string) ([]datedTerms, error) {
	raw, err := readRaw(ctx, reader)
	if err != nil {
		return nil, err
	}
	var history []datedTerms
	for _, entry := range raw {
		record, err := DecodeRecord(entry.Value)
		if err == nil && record.Kind == "pool" && record.Pool == pool {
			history = append(history, datedTerms{*record.Terms, entry.AppendedMS})
		}
	}
	return history, nil
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
	return wire.Snapshot{}, fmt.Errorf("pool %s has no custodian snapshot", pool)
}
