package admin

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"slices"

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

// PoolRotateKey records a rotate-key request, on which the pool's custodian rotates its key, and reports every retained
// task whose submit wrap is to one of suspects, by default the current key when the request is made. A task record is
// listed as written, verified or not, since its envelope is exposed either way.
func (service *Service) PoolRotateKey(ctx context.Context, pool string, suspects []string) (RotateReport, error) {
	state, err := service.LoadState(ctx)
	if err != nil {
		return RotateReport{}, err
	}
	if _, ok := state.Pools[pool]; !ok {
		return RotateReport{}, errors.New("no pool by that name")
	}
	reader := NewReader(service.Admin)
	defer reader.Close()
	if len(suspects) == 0 {
		current, err := currentKeyID(ctx, reader, state, pool)
		if err != nil {
			return RotateReport{}, err
		}
		suspects = []string{current}
	}
	if _, err := service.appendChecked(ctx, []byte("rotate-key."+pool), Record{Kind: "rotate-key", Pool: pool}); err != nil {
		return RotateReport{}, err
	}
	listing, err := reader.Admin()
	if err != nil {
		return RotateReport{}, err
	}
	starts, err := ListOffsets(ctx, listing.ListStartOffsets, PoolTasksTopic(pool))
	if err != nil {
		return RotateReport{}, err
	}
	report := RotateReport{SuspectKeys: suspects, Horizon: map[int32]int64{}}
	starts.Each(func(listed kadm.ListedOffset) { report.Horizon[listed.Partition] = listed.Offset })
	records, err := reader.ReadFrom(ctx, PoolTasksTopic(pool), 0)
	if err != nil {
		return RotateReport{}, err
	}
	for _, record := range records {
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
		names, err := envelopeCredentials(state.Pools[pool], task.Wherry)
		if err != nil {
			names = map[string]bool{}
			for _, credential := range task.Wherry.Credentials {
				names[credential.Name] = true
			}
		}
		report.Tasks = append(report.Tasks, SuspectTask{task.Wherry.Run, task.Wherry.Tenant, kid, slices.Sorted(maps.Keys(names)), record.Timestamp.UnixMilli()})
	}
	return report, nil
}

// currentKeyID is the key ID the latest committed snapshot of pool names, verified by its writer's key, live or ended.
func currentKeyID(ctx context.Context, reader *Reader, state State, pool string) (string, error) {
	records, err := reader.ReadFrom(ctx, CustodianTopic(pool), 0)
	if err != nil {
		return "", err
	}
	topics := PoolKeyTopics(state, pool)
	maps.Copy(topics, EndedPoolKeyTopics(state, pool))
	keys, err := reader.ReadKeys(ctx, topics)
	if err != nil {
		return "", err
	}
	for _, record := range slices.Backward(records) {
		if string(record.Key) != wire.CustodianKey {
			continue
		}
		snapshot, err := wire.ParseSnapshotHistory(record.Value, keys, record.Timestamp)
		if err != nil || snapshot.Pool != pool {
			return "", fmt.Errorf("pool %s's current snapshot does not verify; name the suspect keys with --suspect-key", pool)
		}
		return snapshot.Current.KeyID, nil
	}
	return "", fmt.Errorf("pool %s has no custodian snapshot; name the suspect keys with --suspect-key", pool)
}
