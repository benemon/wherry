package admin

import (
	"context"
	"time"

	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ReadTopic reads a topic from its start to its last stable offset with a
// dedicated read_committed reader built from client's options. client must be
// a plain client: a transactional ID in its options would fence its producer.
func ReadTopic(ctx context.Context, client *kgo.Client, topic string) ([]*kgo.Record, error) {
	admin := kadm.NewClient(client)
	starts, err := admin.ListStartOffsets(ctx, topic)
	if err == nil {
		err = starts.Error()
	}
	var ends kadm.ListedOffsets
	if err == nil {
		ends, err = admin.ListCommittedOffsets(ctx, topic)
	}
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		return nil, err
	}
	remaining := make(map[int32]int64)
	partitions := make(map[int32]kgo.Offset)
	ends.Each(func(end kadm.ListedOffset) {
		if start, ok := starts.Lookup(topic, end.Partition); ok && start.Offset < end.Offset {
			remaining[end.Partition] = end.Offset
			partitions[end.Partition] = kgo.NewOffset().At(start.Offset)
		}
	})
	if len(remaining) == 0 {
		return nil, nil
	}
	reader, err := kgo.NewClient(append(client.Opts(),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		// A trailing abort marker is the last offset below the bound; it must be seen to finish.
		kgo.KeepControlRecords(),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: partitions}))...)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	var records []*kgo.Record
	for len(remaining) > 0 {
		fetches := reader.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := fetches.Err(); err != nil {
			return nil, err
		}
		fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
			end, wanted := remaining[partition.Partition]
			if !wanted {
				return
			}
			for _, record := range partition.Records {
				if record.Offset < end && !record.Attrs.IsControl() {
					records = append(records, record)
				}
				if record.Offset >= end-1 {
					delete(remaining, partition.Partition)
				}
			}
			if len(partition.Records) == 0 && partition.LastStableOffset >= end {
				delete(remaining, partition.Partition)
			}
		})
	}
	return records, nil
}

// Load folds wherry.admin as client's principal; it fails closed.
func Load(ctx context.Context, client *kgo.Client) (State, error) {
	raw, err := readRaw(ctx, client)
	if err != nil {
		return State{}, err
	}
	return load(raw)
}

func readRaw(ctx context.Context, client *kgo.Client) ([]rawRecord, error) {
	records, err := ReadTopic(ctx, client, AdminTopic)
	if err != nil {
		return nil, err
	}
	raw := make([]rawRecord, len(records))
	for i, record := range records {
		raw[i] = rawRecord{record.Offset, record.Value}
	}
	return raw, nil
}

// ReadKeys folds each owner's key topic. A record that breaks the
// registration rules is ignored and the fold continues from the prior state.
func ReadKeys(ctx context.Context, client *kgo.Client, topics map[string]string) (signed.Keys, error) {
	keys := signed.Keys{}
	for owner, topic := range topics {
		records, err := ReadTopic(ctx, client, topic)
		if err != nil {
			return nil, err
		}
		state := signed.KeyState{}
		for _, record := range records {
			// ApplyKeyRecord returns the prior state with its error.
			state, _ = signed.ApplyKeyRecord(state, owner, record.Value)
		}
		keys[owner] = state
	}
	return keys, nil
}

// PoolKeyTopics names the key topics of a pool's receivers.
func PoolKeyTopics(state State, pool string) map[string]string {
	topics := map[string]string{}
	for receiver, config := range state.Receivers {
		if config.Pool == pool && !state.Ended[ReceiverPrincipal(receiver)] {
			topics[receiver] = ReceiverTopic(receiver)
		}
	}
	return topics
}

// HostKeyTopics names the key topics of the live hosts of tenants entitled
// to pool.
func HostKeyTopics(state State, pool string) map[string]string {
	topics := map[string]string{}
	for principal, host := range state.Hosts {
		if state.Entitlements[host.Tenant][pool] && !state.Ended[principal] {
			topics[signed.HostOwner(host.Tenant, host.Name)] = HostTopic(host.Tenant, host.Name)
		}
	}
	return topics
}

// ReadAdvertisements returns each receiver's latest authorised advertisement
// that is no older than liveness at now.
func ReadAdvertisements(ctx context.Context, client *kgo.Client, state State, keys signed.Keys, now time.Time, liveness time.Duration) (map[string]wire.Advertisement, error) {
	records, err := ReadTopic(ctx, client, ReceiversTopic)
	if err != nil {
		return nil, err
	}
	latest := map[string]wire.Advertisement{}
	for _, record := range records {
		advertisement, err := AuthorizeAdvertisement(state, keys, record.Value)
		if err != nil || now.Sub(time.UnixMilli(advertisement.AtMS)) > liveness {
			continue
		}
		if previous, ok := latest[advertisement.Receiver]; !ok || advertisement.AtMS >= previous.AtMS {
			latest[advertisement.Receiver] = advertisement
		}
	}
	return latest, nil
}
