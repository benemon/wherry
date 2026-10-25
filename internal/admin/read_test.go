package admin

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func readerCluster(t *testing.T, topics ...string) *kgo.Client {
	t.Helper()
	cluster := kfake.MustCluster(kfake.EnableSASL(), kfake.Superuser("SCRAM-SHA-512", "admin", "password"))
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

func TestReadTopicStopsAfterTrailingAbort(t *testing.T) {
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
	records, err := ReadTopic(readCtx, client, "topic-a")
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
	records, err := ReadTopic(ctx, client, "keys-receiver")
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
	now := time.UnixMilli(1_800_000_000_000)
	advertise := func(receiver, pool string, at time.Time, capacity int64) {
		t.Helper()
		encoded := sign(t, &wire.Advertisement{Receiver: receiver, Pool: pool, Offers: []wire.Offer{{Adapter: "nomad", Agent: "agent-a"}}, Capacity: capacity, AtMS: at.UnixMilli()}, key)
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

func TestReadTopicFromStartsAtTheOffset(t *testing.T) {
	client := readerCluster(t, "topic-a")
	ctx := context.Background()
	for _, value := range []string{"before", "after"} {
		if err := client.ProduceSync(ctx, &kgo.Record{Topic: "topic-a", Value: []byte(value)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	from, err := ReadTopicFrom(readCtx, client, "topic-a", 1)
	if err != nil || len(from) != 1 || string(from[0].Value) != "after" {
		t.Fatalf("from offset 1: %v, %v; want the later record alone", from, err)
	}
	all, err := ReadTopicFrom(readCtx, client, "topic-a", 0)
	if err != nil || len(all) != 2 {
		t.Fatalf("from offset 0: %d records, %v; want both", len(all), err)
	}
}

// Concurrent reads on one client would otherwise build each reader in the same spare capacity of its options.
func TestReadTopicLeavesTheClientsOptionsUnwritten(t *testing.T) {
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
	if _, err := ReadTopic(readCtx, client, "topic-a"); err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(opts[len(opts):cap(opts)], func(opt kgo.Opt) bool { return opt != nil }) {
		t.Fatal("ReadTopic wrote its reader's options into the client's")
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
	now := time.UnixMilli(1_800_000_000_000)
	liveness := 2 * time.Minute
	advertisement := func(receiver, pool string, age time.Duration, capacity int64) []byte {
		return sign(t, &wire.Advertisement{Receiver: receiver, Pool: pool, Offers: []wire.Offer{{Adapter: "nomad", Agent: "agent-a"}}, Capacity: capacity, AtMS: now.Add(-age).UnixMilli()}, key)
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
