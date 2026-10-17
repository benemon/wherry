package client

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func (client *Client) produce(ctx context.Context, records ...*kgo.Record) error {
	if err := client.kafka.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("produce: %w", err)
	}
	return nil
}

func (client *Client) readBounded(ctx context.Context, topic string) ([]*kgo.Record, error) {
	admin := kadm.NewClient(client.kafka)
	starts, err := admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	if err := starts.Error(); err != nil {
		return nil, err
	}
	ends, err := admin.ListCommittedOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	if err := ends.Error(); err != nil {
		return nil, err
	}
	startPartitions, ok := starts[topic]
	if !ok {
		return nil, fmt.Errorf("topic %s has no partitions", topic)
	}
	assignments := map[string]map[int32]kgo.Offset{topic: {}}
	remove := map[string][]int32{topic: {}}
	endOffsets := make(map[int32]int64)
	for partition, start := range startPartitions {
		end, ok := ends.Lookup(topic, partition)
		if partition < 0 || !ok || start.Err != nil || end.Err != nil {
			continue
		}
		if start.Offset < end.Offset {
			assignments[topic][partition] = kgo.NewOffset().At(start.Offset)
			remove[topic] = append(remove[topic], partition)
			endOffsets[partition] = end.Offset
		}
	}
	if len(endOffsets) == 0 {
		return nil, nil
	}
	client.kafka.AddConsumePartitions(assignments)
	defer client.kafka.RemoveConsumePartitions(remove)

	var records []*kgo.Record
	for len(endOffsets) > 0 {
		fetches := client.kafka.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := fetches.Err(); err != nil {
			return nil, err
		}
		fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
			end, wanted := endOffsets[partition.Partition]
			if partition.Topic != topic || !wanted {
				return
			}
			for _, record := range partition.Records {
				if record.Offset < end && !record.Attrs.IsControl() {
					records = append(records, record)
				}
				if record.Offset >= end-1 {
					delete(endOffsets, partition.Partition)
				}
			}
			// Compaction or aborted records can leave nothing to return below the bound.
			if len(partition.Records) == 0 && partition.LastStableOffset >= end {
				delete(endOffsets, partition.Partition)
			}
		})
	}
	return records, nil
}

func (client *Client) followFromStart(ctx context.Context, topics ...string) (func(), error) {
	starts, err := kadm.NewClient(client.kafka).ListStartOffsets(ctx, topics...)
	if err != nil {
		return nil, err
	}
	if err := starts.Error(); err != nil {
		return nil, err
	}
	assignments := make(map[string]map[int32]kgo.Offset, len(topics))
	remove := make(map[string][]int32, len(topics))
	for _, topic := range topics {
		partitions, ok := starts[topic]
		if !ok {
			return nil, fmt.Errorf("topic %s has no partitions", topic)
		}
		assignments[topic] = map[int32]kgo.Offset{}
		remove[topic] = []int32{}
		for partition, offset := range partitions {
			if partition < 0 || offset.Err != nil {
				continue
			}
			assignments[topic][partition] = kgo.NewOffset().At(offset.Offset)
			remove[topic] = append(remove[topic], partition)
		}
		if len(assignments[topic]) == 0 {
			return nil, fmt.Errorf("topic %s has no readable partitions", topic)
		}
	}
	client.kafka.AddConsumePartitions(assignments)
	return func() { client.kafka.RemoveConsumePartitions(remove) }, nil
}
