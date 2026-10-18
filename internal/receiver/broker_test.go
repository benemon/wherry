package receiver

import (
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// brokerReceiver is a running, enrolled receiver with a credential on the real broker named by
// WHERRY_KAFKA_TEST_BROKER, in a pool of its own. kfake does not model share-group acquisition.
type brokerReceiver struct {
	ctx     context.Context
	opts    []kgo.Opt
	kafka   *kgo.Client
	client  *busclient.Client
	r       *Receiver
	adapter *fakeAdapter
	logs    *syncBuffer
	pool    string
	stop    context.CancelFunc
	done    chan error
}

func startBrokerReceiver(t *testing.T) brokerReceiver {
	t.Helper()
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" {
		t.Skip("WHERRY_KAFKA_TEST_BROKER not set")
	}
	for _, interval := range []*time.Duration{&renewInterval, &capabilityInterval, &refreshInterval, &enrolPollInterval} {
		previous := *interval
		*interval = 200 * time.Millisecond
		t.Cleanup(func() { *interval = previous })
	}
	opts := []kgo.Opt{kgo.SeedBrokers(address), kgo.FetchMaxWait(10 * time.Millisecond)}
	kafka, err := kgo.NewClient(append(opts, kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.KeepControlRecords())...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kafka.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	receiverID, pool, replies := "receiver-broker-"+suffix, "broker-"+suffix, "replies.broker-"+suffix
	admin := kadm.NewClient(kafka)
	if err := wire.ApplyTopics(ctx, admin, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.CreateTopics(ctx, 1, 1, nil, replies); err != nil {
		t.Fatal(err)
	}
	_, userKey, _ := ed25519.GenerateKey(rand.Reader)
	_, signing, _ := ed25519.GenerateKey(rand.Reader)
	sealing, _ := ecdh.X25519().GenerateKey(rand.Reader)
	base, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	adapter := &fakeAdapter{stream: noResultStream, endDelay: time.Minute, offers: []map[string]string{{"runtime": "exec", "nomad.node_pool": "OCPv", "zone": "dc1"}}}
	r := &Receiver{
		config:   Config{ReceiverID: receiverID, Capacity: 1, Labels: map[string]string{}},
		adapters: []Named{{Name: "nomad", Credential: credentialName, Adapter: adapter}},
		kafkaOpt: opts, base: base,
		userPub: userKey.Public().(ed25519.PublicKey), signing: signing, sealing: sealing,
		logger: log.New(logs, "", 0), credentials: make(map[string]heldCredential),
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- r.Run(runCtx) }()
	t.Cleanup(func() { stop(); <-done })

	client := busclient.New(kafka, userKey, replies)
	var requestHash string
	for requestHash == "" {
		enrolments, _, err := client.Enrolments(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, enrolment := range enrolments {
			if enrolment.ReceiverID == receiverID && len(enrolment.Adapters) == 1 && len(enrolment.Adapters[0].Offers) == 1 {
				requestHash = enrolment.RequestHash
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if _, err := client.Approve(ctx, requestHash, pool, map[string]string{}, nomadGrants(), "auto", time.Hour); err != nil {
		t.Fatal(err)
	}
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(testToken), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := client.PutSecret(ctx, credentialName, receiverID, tokenFile, time.Hour); err != nil {
		t.Fatal(err)
	}
	for heldValue(r) != testToken {
		if ctx.Err() != nil {
			t.Fatal("credential never delivered")
		}
		time.Sleep(100 * time.Millisecond)
	}
	// The pool's share group starts at the log end, so tasks are published only once the receiver holds the partition.
	for {
		groups, err := admin.DescribeShareGroups(ctx, "p-"+pool)
		group := groups["p-"+pool]
		if err == nil && len(group.AssignedPartitions()) > 0 {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("receiver never joined its pool's share group")
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(time.Second) // the first share fetch sets the group's start offset
	return brokerReceiver{ctx: ctx, opts: opts, kafka: kafka, client: client, r: r, adapter: adapter, logs: logs, pool: pool, stop: stop, done: done}
}

func (b brokerReceiver) submit(t *testing.T) string {
	t.Helper()
	for {
		submitted, err := b.client.Submit(b.ctx, busclient.SubmitInput{
			Selector: []wire.Requirement{{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}}}, Prompt: "hello",
			Deadline: time.Minute, QueueDeadline: time.Minute,
		})
		if err == nil {
			return submitted.RunID
		}
		if !strings.Contains(err.Error(), "no matching") || b.ctx.Err() != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// member joins the pool's share group as a plain consumer.
func (b brokerReceiver) member(t *testing.T) *kgo.Client {
	t.Helper()
	member, err := kgo.NewClient(append(append([]kgo.Opt(nil), b.opts...), kgo.ConsumeTopics(wire.TopicTask), kgo.ShareGroup("p-"+b.pool), kgo.ShareMaxRecords(1))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(member.Close)
	return member
}

// nextDelivery polls the pool as member until runID arrives and returns it.
func (b brokerReceiver) nextDelivery(t *testing.T, member *kgo.Client, runID string) *kgo.Record {
	t.Helper()
	for {
		pollCtx, cancelPoll := context.WithTimeout(b.ctx, 2*time.Second)
		fetches := member.PollRecords(pollCtx, 1)
		cancelPoll()
		for _, record := range fetches.Records() {
			if string(record.Key) == runID {
				return record
			}
			record.Ack(kgo.AckRelease)
		}
		if b.ctx.Err() != nil {
			t.Fatalf("%s never delivered to another member", runID)
		}
	}
}

func TestShutdownReleasesClaimOnRealBroker(t *testing.T) {
	b := startBrokerReceiver(t)
	runID := b.submit(t)
	for {
		if _, _, follows, _ := b.adapter.counts(); follows == 1 {
			break
		}
		if b.ctx.Err() != nil {
			t.Fatalf("run never started; logs:\n%s", b.logs.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	b.stop()
	if err := <-b.done; err != nil && b.ctx.Err() != nil {
		t.Fatal(err)
	}
	b.done <- nil
	stopped := time.Now()
	b.nextDelivery(t, b.member(t), runID)
	if waited := time.Since(stopped); waited > 10*time.Second {
		t.Fatalf("task returned to the pool %s after shutdown, want at once (the acquisition lock expired instead)", waited)
	}
}
