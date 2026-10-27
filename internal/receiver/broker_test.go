package receiver

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/adapter"
	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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
// acquisition. The receiver has more slots than the pool's delivery limit, so a release its idle slots' closing
// clients hand back and forth would archive the task before another member could take it.
func TestShutdownReleasesClaimOnRealBroker(t *testing.T) {
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" {
		t.Skip("WHERRY_KAFKA_TEST_BROKER not set")
	}
	for name, stopping := range map[string]struct {
		stop func(t *testing.T, ctx context.Context, b brokerPool, cancel context.CancelFunc)
		want error
	}{
		"cancelled":             {func(_ *testing.T, _ context.Context, _ brokerPool, cancel context.CancelFunc) { cancel() }, context.Canceled},
		"configuration changed": {func(t *testing.T, ctx context.Context, b brokerPool, _ context.CancelFunc) { b.configure(t, ctx, 7) }, ErrConfigChanged},
	} {
		t.Run(name, func(t *testing.T) {
			shortIntervals(t)
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()
			b := newBrokerPool(t, ctx, address)
			b.configure(t, ctx, 6)
			b.deliveryLimit(t, ctx, 5)
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
			r := &Receiver{id: receiverID, base: base, reader: admin.NewReader(base), signing: signing, logger: log.New(logs, "", 0), metrics: newMetrics(prometheus.NewRegistry()),
				factory: func(_ string, settings admin.ProfileSettings) (Named, error) {
					return Named{Name: settings.Name, Agent: settings.Agent, Adapter: adapter}, nil
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
			stopping.stop(t, ctx, b, stop)
			if err := <-done; !errors.Is(err, stopping.want) {
				t.Fatalf("receiver stopped with %v, want %v; logs:\n%s", err, stopping.want, logs.String())
			}
			done <- nil
			stopped := time.Now()
			member, err := kgo.NewClient(append(append([]kgo.Opt(nil), opts...), kgo.ConsumeTopics(admin.PoolTasksTopic(pool)), kgo.ShareGroup(admin.PoolGroup(pool)), kgo.ShareMaxRecords(1))...)
			if err != nil {
				t.Fatal(err)
			}
			defer member.Close()
			var redelivered *kgo.Record
			for redelivered == nil {
				pollCtx, cancelPoll := context.WithTimeout(ctx, 2*time.Second)
				for _, record := range member.PollRecords(pollCtx, 1).Records() {
					if string(record.Key) == "run-broker" {
						redelivered = record
					}
				}
				cancelPoll()
				if ctx.Err() != nil {
					t.Fatalf("run-broker never delivered to another member; logs:\n%s", logs.String())
				}
			}
			if waited := time.Since(stopped); waited > 10*time.Second {
				t.Fatalf("task returned to the pool %s after shutdown, want at once (the acquisition lock expired instead)", waited)
			}
			if count := redelivered.DeliveryCount(); count != 2 {
				t.Fatalf("redelivered with delivery count %d, want 2: the receiver's claim and this one", count)
			}
			offsets, err := kadm.NewClient(kafka).DescribeShareGroupOffsets(ctx, admin.PoolGroup(pool))
			if err != nil {
				t.Fatal(err)
			}
			if offset, ok := offsets[admin.PoolGroup(pool)].Offsets.Lookup(admin.PoolTasksTopic(pool), redelivered.Partition); !ok || offset.Err != nil || offset.StartOffset > redelivered.Offset {
				t.Fatalf("share group start offset %+v past the task at %d: the task was archived", offset, redelivered.Offset)
			}
		})
	}
}

// configure writes the receiver's configuration record with capacity.
func (b brokerPool) configure(t *testing.T, ctx context.Context, capacity int64) {
	t.Helper()
	config := testConfig(b.receiver)
	config.Pool, config.Capacity = b.pool, capacity
	encoded, err := admin.EncodeRecord(admin.Record{Kind: "receiver", Pool: b.pool, Receiver: b.receiver, Config: &config})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.kafka.ProduceSync(ctx, &kgo.Record{Topic: admin.AdminTopic, Value: encoded}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// deliveryLimit sets the pool's delivery limit in its terms and on its share group, as pool create does.
func (b brokerPool) deliveryLimit(t *testing.T, ctx context.Context, limit int32) {
	t.Helper()
	terms := testTerms()
	terms.DeliveryLimit = limit
	encoded, err := admin.EncodeRecord(admin.Record{Kind: "pool", Pool: b.pool, Terms: &terms})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.kafka.ProduceSync(ctx, &kgo.Record{Topic: admin.AdminTopic, Value: encoded}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	value := fmt.Sprint(limit)
	config := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	config.Name, config.Value, config.Op = "share.delivery.count.limit", &value, kmsg.IncrementalAlterConfigOpSet
	resource := kmsg.NewIncrementalAlterConfigsRequestResource()
	resource.ResourceType, resource.ResourceName = kmsg.ConfigResourceTypeGroupConfig, admin.PoolGroup(b.pool)
	resource.Configs = []kmsg.IncrementalAlterConfigsRequestResourceConfig{config}
	request := kmsg.NewPtrIncrementalAlterConfigsRequest()
	request.Resources = []kmsg.IncrementalAlterConfigsRequestResource{resource}
	response, err := request.RequestWith(ctx, b.kafka)
	if err == nil {
		err = kerr.ErrorForCode(response.Resources[0].ErrorCode)
	}
	if err != nil {
		t.Fatalf("set the share group's delivery limit: %v", err)
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
	credentialFile := filepath.Join(t.TempDir(), credentialName)
	if err := os.WriteFile(credentialFile, []byte(testToken), 0o600); err != nil {
		t.Fatal(err)
	}
	hostLogs := &syncBuffer{}
	_, hostKey, _ := ed25519.GenerateKey(rand.Reader)
	hostKafka, err := kgo.NewClient(append(slices.Clone(b.opts), kgo.FetchIsolationLevel(kgo.ReadCommitted()))...)
	if err != nil {
		t.Fatal(err)
	}
	host, err := client.NewHost(hostKafka, hostKey, admin.TenantPrincipal(b.tenant, b.host), t.TempDir(), map[string]credential.Source{credentialName: {File: credentialFile}}, nil, log.New(hostLogs, "", 0))
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
	r := &Receiver{id: b.receiver, base: base, reader: admin.NewReader(base), signing: signing, logger: log.New(logs, "", 0), metrics: newMetrics(prometheus.NewRegistry()),
		factory: func(_ string, settings admin.ProfileSettings) (Named, error) {
			return Named{Name: settings.Name, Agent: settings.Agent, Adapter: adapter}, nil
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
	if dispatches, _, _, _ := adapter.counts(); dispatches != 1 || len(adapter.lastRun.Credentials) != 1 ||
		adapter.lastRun.Credentials[0].Env != credentialEnv || string(adapter.lastRun.Credentials[0].Value) != testToken {
		t.Fatalf("dispatches %d with %d credentials, want the host's credential once", dispatches, len(adapter.lastRun.Credentials))
	}
	if strings.Contains(logs.String()+hostLogs.String(), testToken) {
		t.Fatalf("logs carry the credential:\n%s\n%s", logs.String(), hostLogs.String())
	}
}

// TestUnansweredCredentialFailsOnRealBroker verifies that an expired seal wait is terminal and acknowledged.
func TestUnansweredCredentialFailsOnRealBroker(t *testing.T) {
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" {
		t.Skip("WHERRY_KAFKA_TEST_BROKER not set")
	}
	shortIntervals(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	b := newBrokerPool(t, ctx, address)
	_, hostKey, _ := ed25519.GenerateKey(rand.Reader)
	hostKafka, err := kgo.NewClient(append(slices.Clone(b.opts), kgo.FetchIsolationLevel(kgo.ReadCommitted()))...)
	if err != nil {
		t.Fatal(err)
	}
	// The host configures the credential, so submit takes the run, but its source never resolves.
	unresolved := map[string]credential.Source{credentialName: {Env: "WHERRY_TEST_UNSET_" + b.host}}
	host, err := client.NewHost(hostKafka, hostKey, admin.TenantPrincipal(b.tenant, b.host), t.TempDir(), unresolved, nil, log.New(&syncBuffer{}, "", 0))
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
	fake := &fakeAdapter{stream: resultStream}
	r := &Receiver{id: b.receiver, base: base, reader: admin.NewReader(base), signing: signing, logger: log.New(logs, "", 0), metrics: newMetrics(prometheus.NewRegistry()),
		factory: func(_ string, settings admin.ProfileSettings) (Named, error) {
			return Named{Name: settings.Name, Agent: settings.Agent, Adapter: fake}, nil
		}}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- r.Run(runCtx) }()
	defer func() { stop(); <-done }()
	b.joined(t, ctx, logs)
	submitted, err := host.Submit(ctx, client.SubmitInput{Prompt: "hello", Deadline: time.Minute, QueueDeadline: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	want := "credential " + credentialName + " was not answered within 5m; add it to credentials in the host's wherry configuration, keep wherry await or wherry mcp running, and submit again"
	for {
		status, statusErr := host.Status(ctx, submitted.RunID)
		if statusErr == nil && status.Status.State == wire.TaskStateFailed {
			if status.Status.Message == nil || len(status.Status.Message.Parts) != 1 || status.Status.Message.Parts[0].Text != want || strings.Contains(status.Status.Message.Parts[0].Text, testToken) {
				t.Fatalf("failed status %+v, want the credential name and no value", status.Status)
			}
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("run did not fail after its seal wait; status error %v; logs:\n%s", statusErr, logs.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
	member, err := kgo.NewClient(append(slices.Clone(b.opts), kgo.ConsumeTopics(admin.PoolTasksTopic(b.pool)), kgo.ShareGroup(admin.PoolGroup(b.pool)), kgo.ShareMaxRecords(1))...)
	if err != nil {
		t.Fatal(err)
	}
	defer member.Close()
	pollCtx, cancelPoll := context.WithTimeout(ctx, 3*time.Second)
	defer cancelPoll()
	for _, record := range member.PollRecords(pollCtx, 1).Records() {
		if string(record.Key) == submitted.RunID {
			t.Fatal("failed task was redelivered")
		}
	}
	if dispatches, _, _, _ := fake.counts(); dispatches != 0 {
		t.Fatalf("unsealed run dispatched %d jobs", dispatches)
	}
}

// TestFilesOnRealBroker runs a task with an input, a task credential and outputs filling the output limit from a
// host's Submit to its Await on the real broker: the outputs' chunks commit in the result's transaction, within the
// broker's record limit, and the host verifies them. A 480 KiB input travels inline, and 250 long-named outputs over the
// limit lengthen the result, so the result commits only because its history does not carry the input.
func TestFilesOnRealBroker(t *testing.T) {
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" {
		t.Skip("WHERRY_KAFKA_TEST_BROKER not set")
	}
	shortIntervals(t)
	sealWait = 30 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	b := newBrokerPool(t, ctx, address)
	terms := testTerms()
	encoded, err := admin.EncodeRecord(admin.Record{Kind: "pool", Pool: b.pool, Terms: &terms})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.kafka.ProduceSync(ctx, &kgo.Record{Topic: admin.AdminTopic, Value: encoded}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	input := make([]byte, 480<<10)
	rand.Read(input)
	dir := t.TempDir()
	credentialFile, deployKey := filepath.Join(dir, credentialName), filepath.Join(dir, "deploy-key")
	if err := os.WriteFile(credentialFile, []byte(testToken), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(deployKey, []byte(deployValue), 0o600); err != nil {
		t.Fatal(err)
	}
	hostLogs := &syncBuffer{}
	_, hostKey, _ := ed25519.GenerateKey(rand.Reader)
	hostKafka, err := kgo.NewClient(append(slices.Clone(b.opts), kgo.FetchIsolationLevel(kgo.ReadCommitted()))...)
	if err != nil {
		t.Fatal(err)
	}
	host, err := client.NewHost(hostKafka, hostKey, admin.TenantPrincipal(b.tenant, b.host), t.TempDir(),
		map[string]credential.Source{credentialName: {File: credentialFile}, "deploy-key": {File: deployKey}}, nil, log.New(hostLogs, "", 0))
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
	report := []byte("key " + deployValue + "\n")
	big := make([]byte, admin.DefaultOutputLimit-len(report))
	for i := range big {
		big[i] = byte(i * 7)
	}
	outputs := []adapter.Output{{Name: "big.bin", Size: int64(len(big)), Data: big}, {Name: "report.md", Size: int64(len(report)), Data: report}}
	// Outputs past the limit, listed in the result by their long names, leave no room for the input in its history.
	for i := range 250 {
		outputs = append(outputs, adapter.Output{Name: strings.Repeat(fmt.Sprintf("x%0249d/", i), 2) + "over", Size: 1})
	}
	fake := &fakeAdapter{stream: resultStream, outputs: outputs}
	r := &Receiver{id: b.receiver, base: base, reader: admin.NewReader(base), signing: signing, logger: log.New(logs, "", 0), metrics: newMetrics(prometheus.NewRegistry()),
		factory: func(_ string, settings admin.ProfileSettings) (Named, error) {
			return Named{Name: settings.Name, Agent: settings.Agent, Adapter: fake}, nil
		}}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- r.Run(runCtx) }()
	defer func() { stop(); <-done }()
	b.joined(t, ctx, logs)
	var submitted client.SubmitResult
	for ctx.Err() == nil {
		if submitted, err = host.Submit(ctx, client.SubmitInput{Prompt: "hello", Parts: []wire.Part{{Raw: input, Filename: "data.bin"}},
			Credentials: []wire.TaskCredential{{Name: "deploy-key", File: "id_ed25519"}}, Outputs: []wire.ExpectedOutput{{Name: "report.md", Required: true}},
			Deadline: time.Minute, QueueDeadline: time.Minute}); err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("submit never placed: %v", err)
	}
	awaited, err := host.Await(ctx, submitted.RunID, 60*time.Second)
	if err != nil || awaited.Status.State != wire.TaskStateCompleted {
		t.Fatalf("await %+v, %v; receiver logs:\n%s\nhost logs:\n%s", awaited.Status, err, logs.String(), hostLogs.String())
	}
	if len(awaited.Outputs) != 2 || !bytes.Equal(awaited.Outputs[0].Data, big) || !regexp.MustCompile(`^key \[redacted:[A-Za-z0-9_-]{16}\]\n$`).Match(awaited.Outputs[1].Data) || len(awaited.Rejected) != 0 || len(awaited.NotReturned) != 250 {
		t.Fatalf("outputs %d, rejected %+v, not returned %d; want big.bin intact, report.md redacted and 250 over the limit", len(awaited.Outputs), awaited.Rejected, len(awaited.NotReturned))
	}
	run := fake.lastRun
	if len(run.Inputs) != 1 || !bytes.Equal(run.Inputs[0].Data, input) || len(run.Credentials) != 2 || string(run.Credentials[1].Value) != deployValue {
		t.Fatalf("dispatched inputs %+v and credentials %d", run.Inputs, len(run.Credentials))
	}
	if strings.Contains(logs.String()+hostLogs.String(), deployValue) {
		t.Fatalf("logs carry the task credential:\n%s\n%s", logs.String(), hostLogs.String())
	}
}

// TestGitInputOnRealBroker runs a git task from a host's Submit to its Await on the real broker: the client writes an
// scp-like remote as an ssh URL, takes the credential and host key of the git entry the remote falls under, refuses a
// remote outside the pool's VCS hosts before publishing, and declares the patch output; the receiver delivers the host key as known_hosts and the clone commands with the key's path, never its value.
func TestGitInputOnRealBroker(t *testing.T) {
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" {
		t.Skip("WHERRY_KAFKA_TEST_BROKER not set")
	}
	shortIntervals(t)
	sealWait = 30 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	b := newBrokerPool(t, ctx, address)
	terms := vcsTerms("git.example.test")
	encoded, err := admin.EncodeRecord(admin.Record{Kind: "pool", Pool: b.pool, Terms: &terms})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.kafka.ProduceSync(ctx, &kgo.Record{Topic: admin.AdminTopic, Value: encoded}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	credentialFile, deployKey := filepath.Join(dir, credentialName), filepath.Join(dir, "deploy-key")
	if err := os.WriteFile(credentialFile, []byte(testToken), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(deployKey, []byte(deployValue), 0o600); err != nil {
		t.Fatal(err)
	}
	hostKeyFile := filepath.Join(dir, "known_hosts")
	if err := os.WriteFile(hostKeyFile, []byte(testHostKey), 0o600); err != nil {
		t.Fatal(err)
	}
	hostLogs := &syncBuffer{}
	_, hostKey, _ := ed25519.GenerateKey(rand.Reader)
	hostKafka, err := kgo.NewClient(append(slices.Clone(b.opts), kgo.FetchIsolationLevel(kgo.ReadCommitted()))...)
	if err != nil {
		t.Fatal(err)
	}
	host, err := client.NewHost(hostKafka, hostKey, admin.TenantPrincipal(b.tenant, b.host), t.TempDir(),
		map[string]credential.Source{credentialName: {File: credentialFile}, "deploy-key": {File: deployKey}},
		[]client.GitCredential{{URL: "ssh://git@git.example.test/", Credential: "deploy-key", HostKey: hostKeyFile}}, log.New(hostLogs, "", 0))
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
	patch := []byte("From 0000000000000000000000000000000000000000 Mon Sep 17 00:00:00 2001\n")
	fake := &fakeAdapter{stream: resultStream, outputs: []adapter.Output{{Name: wire.GitPatch, Size: int64(len(patch)), Data: patch}}}
	r := &Receiver{id: b.receiver, base: base, reader: admin.NewReader(base), signing: signing, logger: log.New(logs, "", 0), metrics: newMetrics(prometheus.NewRegistry()),
		factory: func(_ string, settings admin.ProfileSettings) (Named, error) {
			return Named{Name: settings.Name, Agent: settings.Agent, Adapter: fake}, nil
		}}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- r.Run(runCtx) }()
	defer func() { stop(); <-done }()
	b.joined(t, ctx, logs)
	input := client.SubmitInput{
		Prompt: "change the README", Git: &wire.GitInput{Remote: "git@git.example.test:org/repo.git", Ref: "main"},
		GitChanges: "patch", Deadline: time.Minute, QueueDeadline: time.Minute,
	}
	var submitted client.SubmitResult
	for ctx.Err() == nil {
		if submitted, err = host.Submit(ctx, input); err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("submit never placed: %v", err)
	}
	tasks, err := readTopic(ctx, b.kafka, admin.PoolTasksTopic(b.pool))
	if err != nil {
		t.Fatal(err)
	}
	outside := input
	outside.Git = &wire.GitInput{Remote: "https://github.com/org/repo.git", Ref: "main"}
	if _, err := host.Submit(ctx, outside); !errors.Is(err, client.ErrInvalidInput) || !strings.Contains(err.Error(), "git remote host github.com is not in pool "+b.pool+"'s vcs hosts") {
		t.Fatalf("submit outside the vcs hosts = %v; want a refusal naming the host and the pool", err)
	}
	if after, err := readTopic(ctx, b.kafka, admin.PoolTasksTopic(b.pool)); err != nil || len(after) != len(tasks) {
		t.Fatalf("task topic holds %d records after the refusal, %v; want %d", len(after), err, len(tasks))
	}
	awaited, err := host.Await(ctx, submitted.RunID, 60*time.Second)
	if err != nil || awaited.Status.State != wire.TaskStateCompleted {
		t.Fatalf("await %+v, %v; receiver logs:\n%s\nhost logs:\n%s", awaited.Status, err, logs.String(), hostLogs.String())
	}
	if len(awaited.Outputs) != 1 || awaited.Outputs[0].Name != wire.GitPatch || !bytes.Equal(awaited.Outputs[0].Data, patch) {
		t.Fatalf("outputs %+v, want the patch", awaited.Outputs)
	}
	run := fake.lastRun
	if len(run.Inputs) != 1 || run.Inputs[0].Name != wire.KnownHostsFile || string(run.Inputs[0].Data) != testHostKey ||
		len(run.Credentials) != 2 || run.Credentials[1].File != wire.GitCredentialFile || string(run.Credentials[1].Value) != deployValue {
		t.Fatalf("dispatched inputs %+v and credentials %d", run.Inputs, len(run.Credentials))
	}
	prompt := string(run.Prompt)
	for _, want := range []string{
		"-- 'ssh://git@git.example.test/org/repo.git' 'repo'", `-i "/task/secrets/credentials/git-credential"`, `UserKnownHostsFile="/task/inputs/known_hosts"`,
		`format-patch --output="/task/outputs/changes.patch" refs/wherry/start..HEAD`, "/task/outputs/changes.patch.",
	} {
		if !strings.Contains(prompt, want) {
			t.Errorf("prompt lacks %q:\n%s", want, prompt)
		}
	}
	if strings.Contains(prompt+logs.String()+hostLogs.String(), deployValue) {
		t.Fatalf("prompt or logs carry the deploy key")
	}
}
