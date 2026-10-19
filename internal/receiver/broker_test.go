package receiver

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// brokerPool is a pool of its own on the plaintext broker named by WHERRY_KAFKA_TEST_BROKER, with one receiver, one
// host and its tenant. That broker has no authoriser, so the admin records are written with admin.EncodeRecord rather
// than admin.Service.
type brokerPool struct {
	opts                         []kgo.Opt
	kafka                        *kgo.Client
	receiver, pool, tenant, host string
}

func newBrokerPool(t *testing.T, ctx context.Context, address string) brokerPool {
	t.Helper()
	opts := []kgo.Opt{kgo.SeedBrokers(address), kgo.FetchMaxWait(10 * time.Millisecond)}
	kafka, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kafka.Close)
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	b := brokerPool{opts: opts, kafka: kafka, receiver: "receiver-" + suffix, pool: "pool-" + suffix, tenant: "tenant-" + suffix, host: "host-" + suffix}
	topics := []string{admin.AdminTopic, admin.ReceiversTopic, admin.PoolTasksTopic(b.pool), admin.PoolEventsTopic(b.pool), admin.TenantTopic(b.tenant), admin.HostTopic(b.tenant, b.host), admin.ReceiverTopic(b.receiver)}
	if _, err := kadm.NewClient(kafka).CreateTopics(ctx, 1, -1, nil, topics...); err != nil {
		t.Fatal(err)
	}
	terms := testTerms()
	config := testConfig(b.receiver)
	config.Pool = b.pool
	for _, record := range []admin.Record{
		{Kind: "pool", Pool: b.pool, Terms: &terms},
		{Kind: "host", Tenant: b.tenant, Host: b.host},
		{Kind: "entitlement", Tenant: b.tenant, Pool: b.pool},
		{Kind: "receiver", Pool: b.pool, Receiver: b.receiver, Config: &config},
	} {
		encoded, err := admin.EncodeRecord(record)
		if err != nil {
			t.Fatal(err)
		}
		if err := kafka.ProduceSync(ctx, &kgo.Record{Topic: admin.AdminTopic, Value: encoded}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	return b
}

// joined waits until the receiver holds a partition of its pool's share group, and for the first share fetch that
// sets the group's start offset; the group starts at the log end, so a task published earlier is never delivered.
func (b brokerPool) joined(t *testing.T, ctx context.Context, logs *syncBuffer) {
	t.Helper()
	for {
		groups, err := kadm.NewClient(b.kafka).DescribeShareGroups(ctx, admin.PoolGroup(b.pool))
		if group := groups[admin.PoolGroup(b.pool)]; err == nil && len(group.AssignedPartitions()) > 0 {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("receiver never joined its pool's share group; logs:\n%s", logs.String())
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(time.Second)
}

// TestShutdownReleasesClaimOnRealBroker runs a receiver in a broker pool; kfake does not model share-group
// acquisition.
func TestShutdownReleasesClaimOnRealBroker(t *testing.T) {
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" {
		t.Skip("WHERRY_KAFKA_TEST_BROKER not set")
	}
	shortIntervals(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	b := newBrokerPool(t, ctx, address)
	opts, kafka, receiverID, pool, tenant, host := b.opts, b.kafka, b.receiver, b.pool, b.tenant, b.host
	_, hostKey, _ := ed25519.GenerateKey(rand.Reader)
	_, signing, _ := ed25519.GenerateKey(rand.Reader)
	registration, err := signed.RegisterKey(signed.HostOwner(tenant, host), hostKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := kafka.ProduceSync(ctx, &kgo.Record{Topic: admin.HostTopic(tenant, host), Value: registration}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	answerSeals(t, ctx, kafka, signed.Keys{receiverID: {Current: signing.Public().(ed25519.PublicKey)}}, tenant, host, credentialName, testToken)
	base, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	adapter := &fakeAdapter{stream: noResultStream, endDelay: time.Minute}
	r := &Receiver{id: receiverID, kafkaOpt: opts, base: base, signing: signing, logger: log.New(logs, "", 0),
		factory: func(_ string, settings admin.AdapterSettings) (Named, error) {
			return Named{Name: settings.Name, Agent: settings.Agent, Credential: settings.Credential, Adapter: adapter}, nil
		}}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- r.Run(runCtx) }()
	defer func() { stop(); <-done }()
	b.joined(t, ctx, logs)
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

// TestCredentialedRunOnRealBroker runs a credentialed task from a host's Submit to its Await on the real broker: the
// receiver's seal request is a transaction on the tenant feed, Await answers it, and the receiver opens the seal.
func TestCredentialedRunOnRealBroker(t *testing.T) {
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" {
		t.Skip("WHERRY_KAFKA_TEST_BROKER not set")
	}
	shortIntervals(t)
	sealWait = 30 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	b := newBrokerPool(t, ctx, address)
	credential := filepath.Join(t.TempDir(), credentialName)
	if err := os.WriteFile(credential, []byte(testToken), 0o600); err != nil {
		t.Fatal(err)
	}
	hostLogs := &syncBuffer{}
	_, hostKey, _ := ed25519.GenerateKey(rand.Reader)
	hostKafka, err := kgo.NewClient(append(slices.Clone(b.opts), kgo.FetchIsolationLevel(kgo.ReadCommitted()))...)
	if err != nil {
		t.Fatal(err)
	}
	host, err := client.NewHost(hostKafka, hostKey, admin.TenantPrincipal(b.tenant, b.host), t.TempDir(), map[string]string{credentialName: credential}, log.New(hostLogs, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if _, err := host.Register(ctx); err != nil {
		t.Fatal(err)
	}
	base, err := kgo.NewClient(b.opts...)
	if err != nil {
		t.Fatal(err)
	}
	_, signing, _ := ed25519.GenerateKey(rand.Reader)
	logs := &syncBuffer{}
	adapter := &fakeAdapter{stream: resultStream}
	r := &Receiver{id: b.receiver, kafkaOpt: b.opts, base: base, signing: signing, logger: log.New(logs, "", 0),
		factory: func(_ string, settings admin.AdapterSettings) (Named, error) {
			return Named{Name: settings.Name, Agent: settings.Agent, Credential: settings.Credential, Adapter: adapter}, nil
		}}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- r.Run(runCtx) }()
	defer func() { stop(); <-done }()
	b.joined(t, ctx, logs)
	var submitted client.SubmitResult
	for ctx.Err() == nil {
		if submitted, err = host.Submit(ctx, client.SubmitInput{Prompt: "hello", Deadline: time.Minute, QueueDeadline: time.Minute}); err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("submit never placed: %v", err)
	}
	awaited, err := host.Await(ctx, submitted.RunID, 60*time.Second)
	if err != nil || awaited.Status.State != wire.TaskStateCompleted {
		t.Fatalf("await %+v, %v; receiver logs:\n%s\nhost logs:\n%s", awaited, err, logs.String(), hostLogs.String())
	}
	if dispatches, _, _, _ := adapter.counts(); dispatches != 1 || string(adapter.lastRun.Credential) != testToken {
		t.Fatalf("dispatches %d with credential %q, want the host's credential once", dispatches, adapter.lastRun.Credential)
	}
	if strings.Contains(logs.String()+hostLogs.String(), testToken) {
		t.Fatalf("logs carry the credential:\n%s\n%s", logs.String(), hostLogs.String())
	}
}
