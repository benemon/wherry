package admin

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/kafkatest"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestCompactionAcrossReplicasOnCluster runs on the multi-broker cluster test/kafka/up-cluster.sh starts. It writes
// several values under each of a few keys to wherry.receivers and to a progress topic, both created with wherry's own
// configs, the progress values in transactions as a receiver writes them and with a terminal tombstone. With segments
// closing quickly it then makes each replica the leader in turn and reads the topic from it: every replica must have
// compacted its own copy to each key's latest record, and that record must survive each leader change.
func TestCompactionAcrossReplicasOnCluster(t *testing.T) {
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" {
		t.Skip("WHERRY_KAFKA_TEST_BROKER not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	kafka, err := kgo.NewClient(kgo.SeedBrokers(address), kgo.FetchMaxWait(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer kafka.Close()
	adm := kadm.NewClient(kafka)
	brokers := kafkatest.Brokers(t, ctx, adm)
	service := &Service{Admin: kafka, Operator: kafka}
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	progress := ReceiverProgressTopic("compaction-" + suffix)
	if err := service.createTopicsWith(ctx, receiversConfigs, ReceiversTopic); err != nil {
		t.Fatal(err)
	}
	if err := service.createTopicsWith(ctx, progressConfigs(retentionBounds{tasks: 7 * 24 * 60 * 60 * 1000}), progress); err != nil {
		t.Fatal(err)
	}
	// Segments close within seconds rather than the hour wherry sets, and the cleaner takes any dirty segment;
	// wherry.receivers, which the other tests share, gets its configs back afterwards.
	fast := []kadm.AlterConfig{
		{Op: kadm.SetConfig, Name: "segment.ms", Value: kadm.StringPtr("5000")},
		{Op: kadm.SetConfig, Name: "max.compaction.lag.ms", Value: kadm.StringPtr("5000")},
		{Op: kadm.SetConfig, Name: "min.cleanable.dirty.ratio", Value: kadm.StringPtr("0.01")},
	}
	// The cleanup restores wherry.receivers on a client of its own: the test's client is closed by then, its deferred
	// Close running before any cleanup, and a closed client fails every request as a dead broker connection. Each
	// restoring request is retried briefly on a retriable error or a lost connection, which a leader election the
	// test made may still cause.
	t.Cleanup(func() {
		restoring, err := kgo.NewClient(kgo.SeedBrokers(address))
		if err != nil {
			t.Errorf("restore wherry.receivers: %v", err)
			return
		}
		defer restoring.Close()
		restoreCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		restoreAdm, restoreService := kadm.NewClient(restoring), &Service{Admin: restoring, Operator: restoring}
		if err := retried(restoreCtx, func() error {
			return alterTopicConfigs(restoreCtx, restoreAdm, []kadm.AlterConfig{{Op: kadm.DeleteConfig, Name: "min.cleanable.dirty.ratio"}}, ReceiversTopic)
		}); err != nil {
			t.Errorf("restore wherry.receivers: %v", err)
		}
		if err := retried(restoreCtx, func() error {
			return restoreService.configureTopics(restoreCtx, receiversConfigs, ReceiversTopic)
		}); err != nil {
			t.Errorf("restore wherry.receivers: %v", err)
		}
	})
	if err := alterTopicConfigs(ctx, adm, fast, ReceiversTopic, progress); err != nil {
		t.Fatal(err)
	}
	for _, topic := range []string{ReceiversTopic, progress} {
		if _, err := kafkatest.Partition(ctx, adm, topic, 0); err != nil {
			t.Fatal(err)
		}
	}

	keys := []string{"compaction-" + suffix + "-a", "compaction-" + suffix + "-b", "compaction-" + suffix + "-c"}
	const rounds = 5
	value := func(key string, round int) []byte { return fmt.Appendf(nil, "%s-%d", key, round) }
	// want is each key's latest value; nil is a tombstone.
	want := map[string]map[string][]byte{ReceiversTopic: {}, progress: {}}
	for round := range rounds {
		for _, key := range keys {
			record := &kgo.Record{Topic: ReceiversTopic, Key: []byte(key), Value: value(key, round)}
			if err := kafka.ProduceSync(ctx, record).FirstErr(); err != nil {
				t.Fatal(err)
			}
			want[ReceiversTopic][key] = record.Value
		}
	}
	producer, err := kgo.NewClient(kgo.SeedBrokers(address), kgo.TransactionalID("p.compaction-"+suffix+".run"))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	for round := range rounds {
		if err := producer.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		for i, key := range keys {
			record := &kgo.Record{Topic: progress, Key: []byte(key), Value: value(key, round)}
			if i == len(keys)-1 && round == rounds-1 {
				record.Value = nil // the run's terminal tombstone
			}
			if err := producer.ProduceSync(ctx, record).FirstErr(); err != nil {
				t.Fatal(err)
			}
			want[progress][key] = record.Value
		}
		if err := producer.EndTransaction(ctx, kgo.TryCommit); err != nil {
			t.Fatal(err)
		}
	}
	// A record past segment.ms closes the segment holding the test's records, which only then becomes cleanable.
	time.Sleep(6 * time.Second)
	for _, topic := range []string{ReceiversTopic, progress} {
		if err := kafka.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte("compaction-" + suffix + "-roll"), Value: []byte("roll")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	for _, topic := range []string{ReceiversTopic, progress} {
		for _, broker := range brokers {
			if err := kafkatest.MoveLeader(ctx, adm, topic, 0, broker); err != nil {
				t.Fatal(err)
			}
			var got map[string][][]byte
			deadline := time.Now().Add(3 * time.Minute)
			for {
				got = keyed(t, ctx, kafka, topic, keys)
				if compacted(got, want[topic]) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("%s led by broker %d: records by key %s, want only each key's latest %s", topic, broker, show(got), show(single(want[topic])))
				}
				time.Sleep(2 * time.Second)
			}
		}
	}
}

// alterTopicConfigs applies configs to topics, failing on the request's error or any topic's.
func alterTopicConfigs(ctx context.Context, adm *kadm.Client, configs []kadm.AlterConfig, topics ...string) error {
	responses, err := adm.AlterTopicConfigs(ctx, configs, topics...)
	if err != nil {
		return err
	}
	for _, response := range responses {
		if response.Err != nil {
			return fmt.Errorf("configure topic %s: %w (%s)", response.Name, response.Err, response.ErrMessage)
		}
	}
	return nil
}

// retried calls fn until it succeeds, fails with an error that is neither a retriable Kafka error nor a lost or
// failed broker connection, has failed five times, or ctx ends. It waits 250ms after the first failure, doubling
// each time.
func retried(ctx context.Context, fn func() error) error {
	backoff := 250 * time.Millisecond
	for attempt := 1; ; attempt++ {
		err := fn()
		if err == nil || attempt == 5 || !(kerr.IsRetriable(err) || kgo.IsRetryableBrokerErr(err)) {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w (%w)", err, ctx.Err())
		case <-time.After(backoff):
		}
		backoff *= 2
	}
}

// keyed reads topic to its last stable offset from its leader and returns the values of keys' records in order.
func keyed(t *testing.T, ctx context.Context, kafka *kgo.Client, topic string, keys []string) map[string][][]byte {
	t.Helper()
	reader := NewReader(kafka)
	defer reader.Close()
	records, err := reader.ReadFrom(ctx, topic, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string][][]byte{}
	for _, record := range records {
		if record.Attrs.IsControl() {
			continue
		}
		for _, key := range keys {
			if string(record.Key) == key {
				got[key] = append(got[key], record.Value)
			}
		}
	}
	return got
}

// compacted reports whether got holds exactly one record per key of want, with want's value. A tombstone may also
// have been removed already.
func compacted(got map[string][][]byte, want map[string][]byte) bool {
	for key, value := range want {
		values := got[key]
		if value == nil && len(values) == 0 {
			continue
		}
		if len(values) != 1 || !bytes.Equal(values[0], value) || (value == nil) != (values[0] == nil) {
			return false
		}
	}
	return true
}

func single(want map[string][]byte) map[string][][]byte {
	got := map[string][][]byte{}
	for key, value := range want {
		got[key] = [][]byte{value}
	}
	return got
}

func show(values map[string][][]byte) string {
	var parts []string
	for key, list := range values {
		var shown []string
		for _, value := range list {
			if value == nil {
				shown = append(shown, "<tombstone>")
			} else {
				shown = append(shown, string(value))
			}
		}
		parts = append(parts, key+"=["+strings.Join(shown, " ")+"]")
	}
	return strings.Join(parts, " ")
}
