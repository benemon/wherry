package admin

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"maps"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func readerCluster(t *testing.T, topics ...string) *kgo.Client {
	t.Helper()
	return readerClusterOf(t, nil, topics...)
}

// readerClusterOf is readerCluster with extra cluster options.
func readerClusterOf(t *testing.T, opts []kfake.Opt, topics ...string) *kgo.Client {
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
	return client
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
// sends a listing to each leader.
func TestReadKeysListsAndFetchesTheTopicsTogether(t *testing.T) {
	topics := map[string]string{"receiver-a": "keys-a", "receiver-b": "keys-b", "receiver-c": "keys-c"}
	client := readerClusterOf(t, []kfake.Opt{kfake.NumBrokers(1)}, slices.Collect(maps.Values(topics))...)
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
	listed, fetched := counted.count(kmsg.ListOffsets), counted.count(kmsg.Fetch)
	t.Logf("%d list offsets and %d fetch requests", listed, fetched)
	if listed != 2 || fetched != 1 {
		t.Fatalf("sent %d list offsets and %d fetch requests, want 2 and 1", listed, fetched)
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
