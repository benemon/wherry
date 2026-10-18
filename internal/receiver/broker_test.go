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
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestShutdownReleasesClaimOnRealBroker needs a real broker: kfake does not model share-group acquisition locks.
func TestShutdownReleasesClaimOnRealBroker(t *testing.T) {
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
	receiverID, pool, replies := "receiver-gjl-"+suffix, "gjl-"+suffix, "replies.gjl-"+suffix
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
	// A new share group starts at the log end; earliest lets the receiver see a task published before it joined.
	alter := kmsg.NewPtrIncrementalAlterConfigsRequest()
	resource := kmsg.NewIncrementalAlterConfigsRequestResource()
	resource.ResourceType, resource.ResourceName = kmsg.ConfigResourceTypeGroupConfig, "p-"+pool
	config := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	config.Name, config.Value = "share.auto.offset.reset", kmsg.StringPtr("earliest")
	resource.Configs = append(resource.Configs, config)
	alter.Resources = append(alter.Resources, resource)
	altered, err := alter.RequestWith(ctx, kafka)
	if err != nil {
		t.Fatal(err)
	}
	if code := altered.Resources[0].ErrorCode; code != 0 {
		t.Fatalf("share.auto.offset.reset: error code %d", code)
	}
	var submitted busclient.SubmitResult
	for {
		submitted, err = client.Submit(ctx, busclient.SubmitInput{
			Selector: []wire.Requirement{{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}}}, Prompt: "hello",
			Deadline: time.Minute, QueueDeadline: time.Minute,
		})
		if err == nil {
			break
		}
		if !strings.Contains(err.Error(), "no matching") || ctx.Err() != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
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

	member, err := kgo.NewClient(append(opts, kgo.ConsumeTopics(wire.TopicTask), kgo.ShareGroup("p-"+pool), kgo.ShareMaxRecords(1))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(member.Close)
	stop()
	if err := <-done; err != nil && ctx.Err() != nil {
		t.Fatal(err)
	}
	done <- nil
	stopped := time.Now()
	for {
		pollCtx, cancelPoll := context.WithTimeout(ctx, 2*time.Second)
		fetches := member.PollRecords(pollCtx, 1)
		cancelPoll()
		for _, record := range fetches.Records() {
			if string(record.Key) == submitted.RunID {
				if waited := time.Since(stopped); waited > 10*time.Second {
					t.Fatalf("task returned to the pool %s after shutdown, want at once (the acquisition lock expired instead)", waited)
				}
				return
			}
			record.Ack(kgo.AckRelease)
		}
		if ctx.Err() != nil {
			t.Fatal("task never returned to the pool after shutdown")
		}
	}
}
