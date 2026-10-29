// Package kafkatest waits for new topics on every real-broker test broker, and moves partition leaders on the
// multi-broker cluster test/kafka/up-cluster.sh starts, for the tests that only make sense with several brokers.
package kafkatest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Brokers skips t unless WHERRY_KAFKA_TEST_BROKERS names a cluster of at least two brokers, and fails it unless the
// cluster has that many. It returns the cluster's broker IDs in order.
func Brokers(t testing.TB, ctx context.Context, adm *kadm.Client) []int32 {
	t.Helper()
	want, _ := strconv.Atoi(os.Getenv("WHERRY_KAFKA_TEST_BROKERS"))
	if want < 2 {
		t.Skip("WHERRY_KAFKA_TEST_BROKERS does not name a multi-broker cluster")
	}
	brokers, err := adm.ListBrokers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ids := brokers.NodeIDs()
	slices.Sort(ids)
	if len(ids) != want {
		t.Fatalf("cluster has brokers %v, want %d", ids, want)
	}
	return ids
}

// Led waits up to 30 seconds until every broker's own metadata gives each partition of topics a leader: a broker the
// new topic's metadata has not reached answers UNKNOWN_TOPIC_OR_PARTITION, and not every read path retries it.
func Led(ctx context.Context, kafka *kgo.Client, topics ...string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		err := led(ctx, kafka, topics)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("topics %v never led on every broker: %w (%w)", topics, err, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func led(ctx context.Context, kafka *kgo.Client, topics []string) error {
	metadata, err := kadm.NewClient(kafka).BrokerMetadata(ctx)
	if err != nil {
		return err
	}
	request := kmsg.NewPtrMetadataRequest()
	for _, topic := range topics {
		requested := kmsg.NewMetadataRequestTopic()
		requested.Topic = kmsg.StringPtr(topic)
		request.Topics = append(request.Topics, requested)
	}
	for _, broker := range metadata.Brokers {
		response, err := request.RequestWith(ctx, kafka.Broker(int(broker.NodeID)))
		if err != nil {
			return fmt.Errorf("broker %d: %w", broker.NodeID, err)
		}
		for _, topic := range response.Topics {
			if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
				return fmt.Errorf("broker %d: %s: %w", broker.NodeID, *topic.Topic, err)
			}
			if len(topic.Partitions) == 0 {
				return fmt.Errorf("broker %d: %s has no partitions", broker.NodeID, *topic.Topic)
			}
			for _, partition := range topic.Partitions {
				if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
					return fmt.Errorf("broker %d: %s/%d: %w", broker.NodeID, *topic.Topic, partition.Partition, err)
				}
				if partition.Leader < 0 {
					return fmt.Errorf("broker %d: %s/%d has no leader", broker.NodeID, *topic.Topic, partition.Partition)
				}
			}
		}
	}
	return nil
}

// Partition is topic's partition's current leader and replicas.
func Partition(ctx context.Context, adm *kadm.Client, topic string, partition int32) (kadm.PartitionDetail, error) {
	metadata, err := adm.Metadata(ctx, topic)
	if err != nil {
		return kadm.PartitionDetail{}, err
	}
	detail, ok := metadata.Topics[topic].Partitions[partition]
	if !ok {
		return kadm.PartitionDetail{}, fmt.Errorf("%s/%d has no metadata", topic, partition)
	}
	if detail.Err != nil {
		return kadm.PartitionDetail{}, fmt.Errorf("%s/%d: %w", topic, partition, detail.Err)
	}
	return detail, nil
}

// MoveLeader makes broker, one of the partition's replicas, its leader: it reorders the replicas so broker is the
// preferred one, elects the preferred replica, and waits until the metadata names broker. A preferred replica out of
// the ISR (catching up after an earlier move) is retried until ctx ends.
func MoveLeader(ctx context.Context, adm *kadm.Client, topic string, partition, broker int32) error {
	for {
		err := moveLeader(ctx, adm, topic, partition, broker)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("move %s/%d's leader to %d: %w (%w)", topic, partition, broker, err, ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func moveLeader(ctx context.Context, adm *kadm.Client, topic string, partition, broker int32) error {
	detail, err := Partition(ctx, adm, topic, partition)
	if err != nil {
		return err
	}
	if detail.Leader == broker {
		return nil
	}
	index := slices.Index(detail.Replicas, broker)
	if index < 0 {
		return fmt.Errorf("broker %d is not a replica of %s/%d (%v)", broker, topic, partition, detail.Replicas)
	}
	if index != 0 {
		replicas := append([]int32{broker}, slices.Delete(slices.Clone(detail.Replicas), index, index+1)...)
		var request kadm.AlterPartitionAssignmentsReq
		request.Assign(topic, partition, replicas)
		responses, err := adm.AlterPartitionAssignments(ctx, request)
		if err != nil {
			return err
		}
		if err := responses[topic][partition].Err; err != nil {
			return err
		}
	}
	elections, err := adm.ElectLeaders(ctx, kadm.ElectPreferredReplica, kadm.TopicsSet{topic: {partition: {}}})
	if err != nil {
		return err
	}
	if err := elections[topic][partition].Err; err != nil && !errors.Is(err, kerr.ElectionNotNeeded) {
		return err
	}
	detail, err = Partition(ctx, adm, topic, partition)
	if err != nil {
		return err
	}
	if detail.Leader != broker {
		return fmt.Errorf("leader is %d", detail.Leader)
	}
	return nil
}

// Rotate moves topic's partition's leader to the next of its replicas in ascending broker order and returns the
// new leader.
func Rotate(ctx context.Context, adm *kadm.Client, topic string, partition int32) (int32, error) {
	detail, err := Partition(ctx, adm, topic, partition)
	if err != nil {
		return -1, err
	}
	replicas := slices.Sorted(slices.Values(detail.Replicas))
	next := replicas[(slices.Index(replicas, detail.Leader)+1)%len(replicas)]
	return next, MoveLeader(ctx, adm, topic, partition, next)
}

// Spread leads partition 0 of topics[i] by brokers[i%len(brokers)], so consecutive topics have different leaders.
func Spread(ctx context.Context, adm *kadm.Client, brokers []int32, topics ...string) error {
	for i, topic := range topics {
		if err := MoveLeader(ctx, adm, topic, 0, brokers[i%len(brokers)]); err != nil {
			return err
		}
	}
	return nil
}

// Churn rotates the leader of partition 0 of each of topics every interval until ctx ends, and counts the moves. A
// move that fails is retried on the next tick; the count says whether leaders moved at all.
type Churn struct {
	moves chan int
}

func StartChurn(ctx context.Context, adm *kadm.Client, interval time.Duration, topics ...string) *Churn {
	c := &Churn{moves: make(chan int, 1)}
	go func() {
		moved := 0
		defer func() { c.moves <- moved }()
		for {
			for _, topic := range topics {
				moveCtx, cancel := context.WithTimeout(ctx, interval)
				if _, err := Rotate(moveCtx, adm, topic, 0); err == nil {
					moved++
				}
				cancel()
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(interval):
			}
		}
	}()
	return c
}

// Moves waits for the churn's context to end and returns how many leader moves it made.
func (c *Churn) Moves() int { return <-c.moves }
