package receiver

import (
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestShutdownReleasesClaimOnRealBroker runs a receiver in a pool of its own on the plaintext broker
// named by WHERRY_KAFKA_TEST_BROKER; kfake does not model share-group acquisition. That broker has no
// authoriser, so the admin records are written with admin.EncodeRecord rather than admin.Service.
func TestShutdownReleasesClaimOnRealBroker(t *testing.T) {
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" {
		t.Skip("WHERRY_KAFKA_TEST_BROKER not set")
	}
	shortIntervals(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	opts := []kgo.Opt{kgo.SeedBrokers(address), kgo.FetchMaxWait(10 * time.Millisecond)}
	kafka, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer kafka.Close()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	receiverID, pool, tenant, host := "receiver-"+suffix, "pool-"+suffix, "tenant-"+suffix, "host-"+suffix
	topics := []string{admin.AdminTopic, admin.ReceiversTopic, admin.PoolTasksTopic(pool), admin.PoolEventsTopic(pool), admin.TenantTopic(tenant), admin.HostTopic(tenant, host), admin.ReceiverTopic(receiverID)}
	if _, err := kadm.NewClient(kafka).CreateTopics(ctx, 1, -1, nil, topics...); err != nil {
		t.Fatal(err)
	}
	terms := testTerms()
	config := testConfig(receiverID)
	config.Pool = pool
	for _, record := range []admin.Record{
		{Kind: "pool", Pool: pool, Terms: &terms},
		{Kind: "host", Tenant: tenant, Host: host},
		{Kind: "entitlement", Tenant: tenant, Pool: pool},
		{Kind: "receiver", Pool: pool, Receiver: receiverID, Config: &config},
	} {
		encoded, err := admin.EncodeRecord(record)
		if err != nil {
			t.Fatal(err)
		}
		if err := kafka.ProduceSync(ctx, &kgo.Record{Topic: admin.AdminTopic, Value: encoded}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	_, hostKey, _ := ed25519.GenerateKey(rand.Reader)
	_, signing, _ := ed25519.GenerateKey(rand.Reader)
	sealing, _ := ecdh.X25519().GenerateKey(rand.Reader)
	registration, err := signed.RegisterKey(signed.HostOwner(tenant, host), hostKey, nil)
	if err != nil {
		t.Fatal(err)
	}
	seal, err := signed.SealCredential(tenant, receiverID, pool, credentialName, time.Now(), time.Now().Add(time.Hour), sealing.PublicKey(), []byte(testToken))
	if err != nil {
		t.Fatal(err)
	}
	encodedSeal, err := signed.EncodeSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if err := kafka.ProduceSync(ctx, &kgo.Record{Topic: admin.HostTopic(tenant, host), Value: registration}, &kgo.Record{Topic: admin.HostTopic(tenant, host), Value: encodedSeal}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	base, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	adapter := &fakeAdapter{stream: noResultStream, endDelay: time.Minute}
	r := &Receiver{id: receiverID, kafkaOpt: opts, base: base, signing: signing, sealing: sealing, logger: log.New(logs, "", 0),
		factory: func(settings admin.AdapterSettings) (Named, error) {
			return Named{Name: settings.Name, Agent: settings.Agent, Credential: settings.Credential, Adapter: adapter}, nil
		}}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- r.Run(runCtx) }()
	defer func() { stop(); <-done }()
	// The pool's share group starts at the log end, so the task is published only once the receiver holds the partition.
	for {
		groups, err := kadm.NewClient(kafka).DescribeShareGroups(ctx, admin.PoolGroup(pool))
		if group := groups[admin.PoolGroup(pool)]; err == nil && len(group.AssignedPartitions()) > 0 {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("receiver never joined its pool's share group; logs:\n%s", logs.String())
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(time.Second) // the first share fetch sets the group's start offset
	submission := testSubmission("run-broker")
	submission.Wherry.Tenant, submission.Wherry.Host, submission.Wherry.Pool = tenant, host, pool
	value, err := wire.SignRecord(submission, hostKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := kafka.ProduceSync(ctx, &kgo.Record{Topic: admin.PoolTasksTopic(pool), Key: []byte("run-broker"), Value: value}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	for {
		if _, _, follows, _ := adapter.counts(); follows == 1 {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("run never started; logs:\n%s", logs.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	stop()
	<-done
	done <- nil
	stopped := time.Now()
	member, err := kgo.NewClient(append(append([]kgo.Opt(nil), opts...), kgo.ConsumeTopics(admin.PoolTasksTopic(pool)), kgo.ShareGroup(admin.PoolGroup(pool)), kgo.ShareMaxRecords(1))...)
	if err != nil {
		t.Fatal(err)
	}
	defer member.Close()
	for redelivered := false; !redelivered; {
		pollCtx, cancelPoll := context.WithTimeout(ctx, 2*time.Second)
		for _, record := range member.PollRecords(pollCtx, 1).Records() {
			redelivered = redelivered || string(record.Key) == "run-broker"
		}
		cancelPoll()
		if ctx.Err() != nil {
			t.Fatal("run-broker never delivered to another member")
		}
	}
	if waited := time.Since(stopped); waited > 10*time.Second {
		t.Fatalf("task returned to the pool %s after shutdown, want at once (the acquisition lock expired instead)", waited)
	}
}
