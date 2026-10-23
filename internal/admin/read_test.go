package admin

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
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
	latest, err := ReadAdvertisements(ctx, client, state, keys, now, 2*time.Minute)
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
