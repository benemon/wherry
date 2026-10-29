package admin

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"maps"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func readerCluster(t *testing.T, topics ...string) *kgo.Client {
	t.Helper()
	_, client := readerClusterOf(t, nil, topics...)
	return client
}

// readerClusterOf is readerCluster with extra cluster options.
func readerClusterOf(t *testing.T, opts []kfake.Opt, topics ...string) (*kfake.Cluster, *kgo.Client) {
	t.Helper()
	cluster := kfake.MustCluster(append([]kfake.Opt{kfake.EnableSASL(), kfake.Superuser("SCRAM-SHA-512", "admin", "password")}, opts...)...)
	t.Cleanup(cluster.Close)
	for _, topic := range topics {
		if err := cluster.CreateTopic(topic, 1, nil); err != nil {
			t.Fatal(err)
		}
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.SASL(scram.Auth{User: "admin", Pass: "password"}.AsSha512Mechanism()), kgo.FetchMaxWait(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return cluster, client
}

// readTopic reads topic from its start through a reader of its own.
func readTopic(ctx context.Context, client *kgo.Client, topic string) ([]*kgo.Record, error) {
	reader := NewReader(client)
	defer reader.Close()
	return reader.ReadFrom(ctx, topic, 0)
}

func TestReadFromStopsAfterTrailingAbort(t *testing.T) {
	client := readerCluster(t, "topic-a")
	ctx := context.Background()
	if err := client.ProduceSync(ctx, &kgo.Record{Topic: "topic-a", Value: []byte("kept")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	producer, err := kgo.NewClient(append(client.Opts(), kgo.TransactionalID("p.pool-a.run-a"))...)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if err := producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := producer.ProduceSync(ctx, &kgo.Record{Topic: "topic-a", Value: []byte("aborted")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := producer.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatal(err)
	}
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	records, err := readTopic(readCtx, client, "topic-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || string(records[0].Value) != "kept" {
		t.Fatalf("read %d records", len(records))
	}
}

func TestReadKeysFoldsEachOwnersTopic(t *testing.T) {
	client := readerCluster(t, "keys-host", "keys-receiver")
	ctx := context.Background()
	_, host, _ := ed25519.GenerateKey(rand.Reader)
	_, receiver, _ := ed25519.GenerateKey(rand.Reader)
	_, intruder, _ := ed25519.GenerateKey(rand.Reader)
	hostOwner := signed.HostOwner("tenant-a", "host-a")
	must := func(value []byte, err error) []byte {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	produce := func(topic string, value []byte) {
		t.Helper()
		if err := client.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: value}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	produce("keys-host", must(signed.RegisterKey(hostOwner, host)))
	produce("keys-host", must(signed.RegisterKey(hostOwner, intruder)))
	produce("keys-receiver", must(signed.RegisterKey("receiver-a", receiver)))
	_, next, _ := ed25519.GenerateKey(rand.Reader)
	produce("keys-receiver", must(signed.RotateKey("receiver-a", receiver, next)))
	keys, err := ReadKeys(ctx, client, map[string]string{hostOwner: "keys-host", "receiver-a": "keys-receiver"})
	if err != nil {
		t.Fatal(err)
	}
	if !keys[hostOwner].Current.Equal(host.Public()) {
		t.Fatal("a second bare registration replaced the host key")
	}
	records, err := readTopic(ctx, client, "keys-receiver")
	if err != nil {
		t.Fatal(err)
	}
	state := keys["receiver-a"]
	if !state.Current.Equal(next.Public()) || len(state.Retired) != 1 || !state.Retired[0].Key.Equal(receiver.Public()) || !state.Retired[0].At.Equal(records[1].Timestamp) {
		t.Fatalf("rotation folded to %#v; want the old key retired at the rotation's broker time", state)
	}
}

func TestReadAdvertisementsKeepsLatestLiveAuthorised(t *testing.T) {
	client := readerCluster(t, ReceiversTopic)
	ctx := context.Background()
	state := modelState(t)
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	keys := signed.Keys{"receiver-a": {Current: key.Public().(ed25519.PublicKey)}, "receiver-b": {Current: key.Public().(ed25519.PublicKey)}}
	now := time.Now()
	advertise := func(receiver, pool string, at time.Time, capacity int64) {
		t.Helper()
		encoded := sign(t, &wire.Advertisement{Receiver: receiver, Pool: pool, Offers: []wire.Offer{{Profile: "nomad", Agent: "agent-a"}}, Capacity: capacity, AtMS: at.UnixMilli()}, key)
		if err := client.ProduceSync(ctx, &kgo.Record{Topic: ReceiversTopic, Value: encoded}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	advertise("receiver-a", "pool-a", now.Add(-10*time.Second), 1)
	advertise("receiver-a", "pool-a", now.Add(-5*time.Second), 2)
	advertise("receiver-b", "pool-b", now.Add(-time.Hour), 3)
	advertise("receiver-b", "pool-a", now, 4)
	advertise("receiver-a", "pool-a", now.Add(time.Hour), 5)
	reader := NewReader(client)
	defer reader.Close()
	latest, err := reader.ReadAdvertisements(ctx, state, keys, now, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(latest) != 1 || latest["receiver-a"].Capacity != 2 {
		t.Fatalf("advertisements: %+v", latest)
	}
}

func TestReadFromStartsAtTheOffset(t *testing.T) {
	client := readerCluster(t, "topic-a")
	ctx := context.Background()
	for _, value := range []string{"before", "after"} {
		if err := client.ProduceSync(ctx, &kgo.Record{Topic: "topic-a", Value: []byte(value)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	reader := NewReader(client)
	defer reader.Close()
	from, err := reader.ReadFrom(readCtx, "topic-a", 1)
	if err != nil || len(from) != 1 || string(from[0].Value) != "after" {
		t.Fatalf("from offset 1: %v, %v; want the later record alone", from, err)
	}
	all, err := reader.ReadFrom(readCtx, "topic-a", 0)
	if err != nil || len(all) != 2 {
		t.Fatalf("from offset 0: %d records, %v; want both", len(all), err)
	}
}

// Concurrent reads on one client would otherwise build each reader in the same spare capacity of its options.
func TestReaderLeavesTheClientsOptionsUnwritten(t *testing.T) {
	base := readerCluster(t, "topic-a")
	ctx := context.Background()
	if err := base.ProduceSync(ctx, &kgo.Record{Topic: "topic-a", Value: []byte("kept")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	opts := append(make([]kgo.Opt, 0, len(base.Opts())+3), base.Opts()...)
	client, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := readTopic(readCtx, client, "topic-a"); err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(opts[len(opts):cap(opts)], func(opt kgo.Opt) bool { return opt != nil }) {
		t.Fatal("the reader wrote its options into the client's")
	}
}

// ReadAdvertisements verifies each receiver's records newest first and stops at the first that authorises, which is
// the receiver's latest authorised record by time, the later of equal times, as verifying every record finds.
func TestReadAdvertisementsVerifiesNewestFirst(t *testing.T) {
	state := modelState(t)
	ended := state
	ended.Ended = map[string]bool{ReceiverPrincipal("receiver-b"): true}
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	keys := signed.Keys{"receiver-a": {Current: key.Public().(ed25519.PublicKey)}, "receiver-b": {Current: key.Public().(ed25519.PublicKey)}}
	now := time.Now()
	liveness := 2 * time.Minute
	advertisement := func(receiver, pool string, age time.Duration, capacity int64) []byte {
		return sign(t, &wire.Advertisement{Receiver: receiver, Pool: pool, Offers: []wire.Offer{{Profile: "nomad", Agent: "agent-a"}}, Capacity: capacity, AtMS: now.Add(-age).UnixMilli()}, key)
	}
	// forged is record's header and payload under other's signature, so its kid names the receiver's current key.
	forged := func(record, other []byte) []byte {
		parts := bytes.Split(record, []byte("."))
		parts[2] = bytes.Split(other, []byte("."))[2]
		return bytes.Join(parts, []byte("."))
	}
	for _, c := range []struct {
		name    string
		state   State
		records [][]byte
		want    map[string]int64
	}{
		{"newest valid", state, [][]byte{advertisement("receiver-a", "pool-a", 10*time.Second, 1), advertisement("receiver-a", "pool-a", 5*time.Second, 2)}, map[string]int64{"receiver-a": 2}},
		{"newest forged", state, [][]byte{advertisement("receiver-a", "pool-a", 10*time.Second, 1), forged(advertisement("receiver-a", "pool-a", 5*time.Second, 9), advertisement("receiver-a", "pool-a", 10*time.Second, 1))}, map[string]int64{"receiver-a": 1}},
		{"newest from an ended receiver", ended, [][]byte{advertisement("receiver-a", "pool-a", 10*time.Second, 1), advertisement("receiver-b", "pool-b", 10*time.Second, 2), advertisement("receiver-b", "pool-b", 5*time.Second, 3)}, map[string]int64{"receiver-a": 1}},
		{"equal times", state, [][]byte{advertisement("receiver-a", "pool-a", 5*time.Second, 1), advertisement("receiver-a", "pool-a", 5*time.Second, 2)}, map[string]int64{"receiver-a": 2}},
		{"none verify", state, [][]byte{forged(advertisement("receiver-a", "pool-a", 10*time.Second, 1), advertisement("receiver-b", "pool-b", 5*time.Second, 2)), forged(advertisement("receiver-b", "pool-b", 5*time.Second, 3), advertisement("receiver-a", "pool-a", 10*time.Second, 4))}, map[string]int64{}},
	} {
		t.Run(c.name, func(t *testing.T) {
			client := readerCluster(t, ReceiversTopic)
			ctx := context.Background()
			// verified is the latest authorised record of each receiver found by verifying every record.
			verified := map[string]wire.Advertisement{}
			for _, value := range c.records {
				if err := client.ProduceSync(ctx, &kgo.Record{Topic: ReceiversTopic, Value: value}).FirstErr(); err != nil {
					t.Fatal(err)
				}
				advertisement, err := AuthorizeAdvertisement(c.state, keys, value)
				if age := now.Sub(time.UnixMilli(advertisement.AtMS)); err != nil || age > liveness || age < -liveness {
					continue
				}
				if previous, ok := verified[advertisement.Receiver]; !ok || advertisement.AtMS >= previous.AtMS {
					verified[advertisement.Receiver] = advertisement
				}
			}
			reader := NewReader(client)
			defer reader.Close()
			latest, err := reader.ReadAdvertisements(ctx, c.state, keys, now, liveness)
			if err != nil {
				t.Fatal(err)
			}
			capacities := map[string]int64{}
			for receiver, advertisement := range latest {
				capacities[receiver] = advertisement.Capacity
			}
			if !reflect.DeepEqual(latest, verified) || !reflect.DeepEqual(capacities, c.want) {
				t.Fatalf("advertisements %+v; want %+v with capacities %v", latest, verified, c.want)
			}
		})
	}
}

// A failed read of one key topic fails ReadKeys whole, with no keys from the reads that succeeded.
func TestReadKeysFailsWholeOnOneFailedRead(t *testing.T) {
	client := readerCluster(t, "keys-a")
	ctx := context.Background()
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	registration, err := signed.RegisterKey("receiver-a", key)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.ProduceSync(ctx, &kgo.Record{Topic: "keys-a", Value: registration}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	keys, err := ReadKeys(ctx, client, map[string]string{"receiver-a": "keys-a", "receiver-b": "keys-missing"})
	if err == nil || keys != nil {
		t.Fatalf("keys %v, %v; want an error and no keys", keys, err)
	}
}

// requests counts the requests of each key that clients built from a client's options send.
type requests struct {
	mu   sync.Mutex
	keys map[int16]int
}

func (c *requests) OnBrokerE2E(_ kgo.BrokerMetadata, key int16, _ kgo.BrokerE2E) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.keys[key]++
}

func (c *requests) count(key kmsg.Key) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.keys[int16(key)]
}

// Several key topics are read in one listing of each kind and one fetch; kfake's one broker leads them all, as kadm
// sends a listing to each leader. kgo sends its next fetch once a poll takes the last, which can be before the reader
// stops consuming partitions read to their end, so only fetches from below an end are counted.
func TestReadKeysListsAndFetchesTheTopicsTogether(t *testing.T) {
	topics := map[string]string{"receiver-a": "keys-a", "receiver-b": "keys-b", "receiver-c": "keys-c"}
	cluster, client := readerClusterOf(t, []kfake.Opt{kfake.NumBrokers(1)}, slices.Collect(maps.Values(topics))...)
	ctx := context.Background()
	for owner, topic := range topics {
		_, key, _ := ed25519.GenerateKey(rand.Reader)
		record, err := signed.RegisterKey(owner, key)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: record}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	// reads holds the number of partitions each fetch from below an end asks for; each topic ends at offset 1.
	var mu sync.Mutex
	var reads []int
	cluster.ControlKey(int16(kmsg.Fetch), func(request kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		read := 0
		for _, topic := range request.(*kmsg.FetchRequest).Topics {
			for _, partition := range topic.Partitions {
				if partition.FetchOffset < 1 {
					read++
				}
			}
		}
		if read > 0 {
			mu.Lock()
			reads = append(reads, read)
			mu.Unlock()
		}
		return nil, nil, false
	})
	counted := &requests{keys: map[int16]int{}}
	hooked, err := kgo.NewClient(append(client.Opts(), kgo.WithHooks(counted))...)
	if err != nil {
		t.Fatal(err)
	}
	defer hooked.Close()
	reader := NewReader(hooked)
	defer reader.Close()
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	keys, err := reader.ReadKeys(readCtx, topics)
	if err != nil {
		t.Fatal(err)
	}
	for owner := range topics {
		if keys[owner].Current == nil {
			t.Fatalf("keys %v; want each owner's key", keys)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	listed := counted.count(kmsg.ListOffsets)
	t.Logf("%d list offsets and %d fetch requests, of which %v partitions read", listed, counted.count(kmsg.Fetch), reads)
	if listed != 2 || !slices.Equal(reads, []int{len(topics)}) {
		t.Fatalf("sent %d list offsets and fetches reading %v partitions, want 2 and one reading %d", listed, reads, len(topics))
	}
}

// The reader groups advertisements by the receiver its signed payload names, never by the Kafka key, so a record
// another receiver writes under a receiver's key is never that receiver's advertisement.
func TestReadAdvertisementsIgnoresTheKafkaKey(t *testing.T) {
	client := readerCluster(t, ReceiversTopic)
	ctx := context.Background()
	state := modelState(t)
	_, keyA, _ := ed25519.GenerateKey(rand.Reader)
	_, keyB, _ := ed25519.GenerateKey(rand.Reader)
	keys := signed.Keys{"receiver-a": {Current: keyA.Public().(ed25519.PublicKey)}, "receiver-b": {Current: keyB.Public().(ed25519.PublicKey)}}
	now := time.Now()
	advertise := func(receiver, pool string, capacity int64, key ed25519.PrivateKey) {
		t.Helper()
		encoded := sign(t, &wire.Advertisement{Receiver: receiver, Pool: pool, Offers: []wire.Offer{{Profile: "nomad", Agent: "agent-a"}}, Capacity: capacity, AtMS: now.UnixMilli()}, key)
		if err := client.ProduceSync(ctx, &kgo.Record{Topic: ReceiversTopic, Key: []byte("receiver-a"), Value: encoded}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	advertise("receiver-b", "pool-b", 1, keyB)
	advertise("receiver-a", "pool-a", 2, keyB)
	reader := NewReader(client)
	defer reader.Close()
	latest, err := reader.ReadAdvertisements(ctx, state, keys, now, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := latest["receiver-a"]; ok || len(latest) != 1 || latest["receiver-b"].Capacity != 1 {
		t.Fatalf("advertisements %+v; want receiver-b's alone", latest)
	}
}

// Compaction leaves gaps in a topic's offsets but never removes its last record, so a read still ends.
func TestReadFromReadsOverCompactedGaps(t *testing.T) {
	cluster := kfake.MustCluster()
	t.Cleanup(cluster.Close)
	if err := cluster.CreateTopic("topic-a", 1, map[string]string{"cleanup.policy": "compact,delete"}); err != nil {
		t.Fatal(err)
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.FetchMaxWait(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	ctx := context.Background()
	for _, key := range []string{"a", "b", "b", "c"} {
		if err := client.ProduceSync(ctx, &kgo.Record{Topic: "topic-a", Key: []byte(key), Value: []byte(key)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	cluster.Compact()
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	records, err := readTopic(readCtx, client, "topic-a")
	if err != nil {
		t.Fatal(err)
	}
	var offsets []int64
	for _, record := range records {
		offsets = append(offsets, record.Offset)
	}
	if !slices.Equal(offsets, []int64{0, 2, 3}) {
		t.Fatalf("read offsets %v, want 0, 2 and 3 around the compacted 1", offsets)
	}
}

// failListings has cluster answer the first failures ListOffsets requests, or every one if failures is negative, with
// code for each partition, and returns the number of requests it has seen. client is rebuilt without kgo's own retries,
// which it takes only for some listings.
func failListings(t *testing.T, cluster *kfake.Cluster, client *kgo.Client, failures int32, code int16) (*kgo.Client, *atomic.Int32) {
	t.Helper()
	seen := &atomic.Int32{}
	cluster.ControlKey(int16(kmsg.ListOffsets), func(request kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		if n := seen.Add(1); failures >= 0 && n > failures {
			return nil, nil, false
		}
		listing := request.(*kmsg.ListOffsetsRequest)
		response := listing.ResponseKind().(*kmsg.ListOffsetsResponse)
		for _, topic := range listing.Topics {
			listed := kmsg.NewListOffsetsResponseTopic()
			listed.Topic = topic.Topic
			for _, partition := range topic.Partitions {
				failed := kmsg.NewListOffsetsResponseTopicPartition()
				failed.Partition = partition.Partition
				failed.ErrorCode = code
				listed.Partitions = append(listed.Partitions, failed)
			}
			response.Topics = append(response.Topics, listed)
		}
		return response, nil, true
	})
	unretried, err := kgo.NewClient(append(client.Opts(), kgo.RequestRetries(0))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(unretried.Close)
	return unretried, seen
}

// A partition whose leader is moving fails its listing retriably until it has one; ReadKeys lists again until then.
func TestReadKeysListsAgainOnARetriablePartitionError(t *testing.T) {
	cluster, client := readerClusterOf(t, []kfake.Opt{kfake.NumBrokers(1)}, "keys-a")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	registration, err := signed.RegisterKey("receiver-a", key)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.ProduceSync(ctx, &kgo.Record{Topic: "keys-a", Value: registration}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	unretried, _ := failListings(t, cluster, client, 3, kerr.NotLeaderForPartition.Code)
	keys, err := ReadKeys(ctx, unretried, map[string]string{"receiver-a": "keys-a"})
	if err != nil || keys["receiver-a"].Current == nil {
		t.Fatalf("keys %v, %v; want receiver-a's key", keys, err)
	}
}

// A non-retriable partition error fails the read with the first listings of start and end offsets; kadm fails the
// listing itself on an authorization error, which can cancel the concurrent listing before it reaches the broker.
func TestReadKeysFailsAtOnceOnANonRetriablePartitionError(t *testing.T) {
	for _, failure := range []*kerr.Error{kerr.TopicAuthorizationFailed, kerr.UnknownServerError} {
		t.Run(failure.Message, func(t *testing.T) {
			cluster, client := readerClusterOf(t, []kfake.Opt{kfake.NumBrokers(1)}, "keys-a")
			unretried, seen := failListings(t, cluster, client, -1, failure.Code)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			keys, err := ReadKeys(ctx, unretried, map[string]string{"receiver-a": "keys-a"})
			if !errors.Is(err, failure) || keys != nil || seen.Load() > 2 {
				t.Fatalf("keys %v, %v after %d listings; want %v after at most 2", keys, err, seen.Load(), failure)
			}
		})
	}
}

// A partition that never recovers fails the read when its context ends.
func TestReadKeysFailsWhenARetriablePartitionErrorOutlastsTheContext(t *testing.T) {
	cluster, client := readerClusterOf(t, []kfake.Opt{kfake.NumBrokers(1)}, "keys-a")
	unretried, seen := failListings(t, cluster, client, -1, kerr.NotLeaderForPartition.Code)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	keys, err := ReadKeys(ctx, unretried, map[string]string{"receiver-a": "keys-a"})
	if !errors.Is(err, context.DeadlineExceeded) || keys != nil || seen.Load() <= 2 {
		t.Fatalf("keys %v, %v after %d listings; want the context's error after listing again", keys, err, seen.Load())
	}
}

// ListOffsets lists again while a partition fails retriably, fails at once on any other partition error, and fails
// with its context's error when the retriable error outlasts it.
func TestListOffsetsListsAgainOnlyOnARetriablePartitionError(t *testing.T) {
	for _, test := range []struct {
		name     string
		failures int32
		code     int16
		timeout  time.Duration
		want     error
		listings func(int32) bool
	}{
		{"retriable then listed", 3, kerr.NotLeaderForPartition.Code, 10 * time.Second, nil, func(n int32) bool { return n == 4 }},
		{"non-retriable", -1, kerr.UnknownServerError.Code, 5 * time.Second, kerr.UnknownServerError, func(n int32) bool { return n == 1 }},
		{"retriable past the deadline", -1, kerr.NotLeaderForPartition.Code, 500 * time.Millisecond, context.DeadlineExceeded, func(n int32) bool { return n > 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			cluster, client := readerClusterOf(t, []kfake.Opt{kfake.NumBrokers(1)}, "keys-a")
			unretried, seen := failListings(t, cluster, client, test.failures, test.code)
			ctx, cancel := context.WithTimeout(context.Background(), test.timeout)
			defer cancel()
			listed, err := ListOffsets(ctx, kadm.NewClient(unretried).ListEndOffsets, "keys-a")
			if !errors.Is(err, test.want) || !test.listings(seen.Load()) || err == nil && len(listed["keys-a"]) != 1 {
				t.Fatalf("listed %v, %v after %d listings; want %v", listed, err, seen.Load(), test.want)
			}
		})
	}
}

// A topic missing from the first listings' metadata, as a created topic is from a broker its metadata has not reached,
// is listed again until it is there.
func TestReadFromListsAgainWhileTheTopicIsMissing(t *testing.T) {
	cluster, client := readerClusterOf(t, nil)
	listed := make(chan struct{})
	var once sync.Once
	cluster.ControlKey(int16(kmsg.Metadata), func(request kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		for _, topic := range request.(*kmsg.MetadataRequest).Topics {
			if topic.Topic != nil && *topic.Topic == "keys-a" {
				once.Do(func() { close(listed) })
			}
		}
		return nil, nil, false
	})
	created := make(chan error, 1)
	go func() {
		<-listed
		created <- cluster.CreateTopic("keys-a", 1, nil)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), topicWait)
	defer cancel()
	records, err := readTopic(ctx, client, "keys-a")
	if err != nil || len(records) != 0 {
		t.Fatalf("records %v, %v; want the created topic read empty", records, err)
	}
	if err := <-created; err != nil {
		t.Fatal(err)
	}
}

// A topic still missing after topicWait is reported missing, as it was before the wait.
func TestATopicMissingPastTheWaitIsReportedMissing(t *testing.T) {
	for _, test := range []struct {
		name string
		read func(context.Context, *kgo.Client) error
	}{
		{"ReadFrom", func(ctx context.Context, client *kgo.Client) error {
			_, err := readTopic(ctx, client, "keys-a")
			return err
		}},
		{"ListOffsets", func(ctx context.Context, client *kgo.Client) error {
			_, err := ListOffsets(ctx, kadm.NewClient(client).ListEndOffsets, "keys-a")
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			client := readerCluster(t)
			ctx, cancel := context.WithTimeout(context.Background(), 2*topicWait)
			defer cancel()
			start := time.Now()
			if err := test.read(ctx, client); err != kerr.UnknownTopicOrPartition || time.Since(start) < topicWait {
				t.Fatalf("%v after %v; want %v after %v", err, time.Since(start), kerr.UnknownTopicOrPartition, topicWait)
			}
		})
	}
}
