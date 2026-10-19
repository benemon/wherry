package client

import (
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"gopkg.in/yaml.v3"
)

// fixture is a kfake cluster provisioned through admin.Service, with a host connected as its own principal.
type fixture struct {
	ctx       context.Context
	service   *admin.Service
	host      *Host
	stateDir  string
	secret    string
	receivers map[string]*testReceiver
}

type testReceiver struct {
	id      string
	pool    string
	key     ed25519.PrivateKey
	sealing *ecdh.PrivateKey
	kafka   *kgo.Client
}

func testTerms() admin.Terms {
	return admin.Terms{
		Agents:         []admin.AgentTerms{{Name: "claude", Credentials: []string{"model"}}, {Name: "codex", Credentials: []string{"openai"}}},
		DefaultAgent:   "claude",
		SealLifetimeMS: int64(time.Hour / time.Millisecond), DeliveryLimit: 3, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50,
	}
}

func newFixtureNoHost(t *testing.T) *fixture {
	t.Helper()
	cluster := kfake.MustCluster(kfake.EnableSASL(), kfake.EnableACLs(), kfake.Superuser("SCRAM-SHA-512", "admin", "password"), kfake.Superuser("PLAIN", "wherry.operator", "operator-password"))
	t.Cleanup(cluster.Close)
	if err := cluster.CreateTopic(admin.AdminTopic, 1, nil); err != nil {
		t.Fatal(err)
	}
	root, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.SASL(scram.Auth{User: "admin", Pass: "password"}.AsSha512Mechanism()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(root.Close)
	operator, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.SASL(plain.Auth{User: "wherry.operator", Pass: "operator-password"}.AsMechanism()), kgo.FetchMaxWait(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	f := &fixture{ctx: ctx, service: &admin.Service{Admin: root, Operator: operator, Broker: cluster.ListenAddrs()[0], DeliveryCeiling: 10}, receivers: map[string]*testReceiver{}}
	f.pool(t, "pool-a")
	return f
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := newFixtureNoHost(t)
	f.stateDir = t.TempDir()
	f.secret = filepath.Join(t.TempDir(), "model")
	if err := os.WriteFile(f.secret, []byte("model-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	f.host = f.newHost(t, "host-a", f.stateDir)
	return f
}

func (f *fixture) pool(t *testing.T, pool string) {
	t.Helper()
	if err := f.service.PoolCreate(f.ctx, pool, testTerms()); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantAdd(f.ctx, "tenant-a", pool); err != nil {
		t.Fatal(err)
	}
}

// connect opens a plain client as the principal of the bundle at path.
func (f *fixture) connect(t *testing.T, path string) (*kgo.Client, string) {
	t.Helper()
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var bundle admin.Bundle
	if err := yaml.Unmarshal(encoded, &bundle); err != nil {
		t.Fatal(err)
	}
	// kfake's SCRAM server always offers 4096 iterations, so the 8192 that admin provisions never authenticates.
	if _, err := kadm.NewClient(f.service.Admin).AlterUserSCRAMs(f.ctx, nil, []kadm.UpsertSCRAM{{User: bundle.Principal, Mechanism: kadm.ScramSha512, Iterations: 4096, Password: bundle.Password}}); err != nil {
		t.Fatal(err)
	}
	kafka, err := kgo.NewClient(kgo.SeedBrokers(f.service.Broker), kgo.SASL(scram.Auth{User: bundle.Principal, Pass: bundle.Password}.AsSha512Mechanism()),
		kgo.FetchMaxWait(10*time.Millisecond), kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kafka.Close)
	return kafka, bundle.Principal
}

// newHost provisions host in tenant-a and registers its key, as wherry init does.
func (f *fixture) newHost(t *testing.T, host, stateDir string) *Host {
	t.Helper()
	bundle := filepath.Join(t.TempDir(), "bundle.yaml")
	if err := f.service.HostAdd(f.ctx, "tenant-a", host, bundle); err != nil {
		t.Fatal(err)
	}
	kafka, principal := f.connect(t, bundle)
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	h, err := NewHost(kafka, key, principal, stateDir, map[string]string{"model": f.secret, "openai": f.secret})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Register(f.ctx); err != nil {
		t.Fatal(err)
	}
	return h
}

// addReceiver provisions a receiver whose lab adapter launches claude and cloud adapter codex, and registers its signing
// and sealing keys.
func (f *fixture) addReceiver(t *testing.T, id, pool string) *testReceiver {
	t.Helper()
	config := admin.ReceiverConfig{Receiver: id, Pool: pool, Capacity: 2, Adapters: []admin.AdapterSettings{
		{Name: "lab", Type: "nomad", Agent: "claude", Credential: "model", Labels: map[string]string{"runtime": "podman", "region": "eu"}},
		{Name: "cloud", Type: "aws", Agent: "codex", Credential: "openai", Labels: map[string]string{"runtime": "exec"}},
	}}
	bundle := filepath.Join(t.TempDir(), "bundle.yaml")
	if err := f.service.ReceiverAdd(f.ctx, config, nil, bundle); err != nil {
		t.Fatal(err)
	}
	kafka, _ := f.connect(t, bundle)
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sealing, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	receiver := &testReceiver{id: id, pool: pool, key: key, sealing: sealing, kafka: kafka}
	registration, err := signed.RegisterKey(id, key, sealing.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	receiver.produce(t, f.ctx, admin.ReceiverTopic(id), id, registration)
	f.receivers[id] = receiver
	return receiver
}

func (r *testReceiver) produce(t *testing.T, ctx context.Context, topic, key string, value []byte) {
	t.Helper()
	if err := r.kafka.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(key), Value: value}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func (r *testReceiver) sign(t *testing.T, record wire.Record) []byte {
	t.Helper()
	encoded, err := wire.SignRecord(record, r.key)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func claudeOffer() wire.Offer {
	return wire.Offer{Adapter: "lab", Agent: "claude", Labels: map[string]string{"runtime": "podman", "region": "eu"}}
}

// bothOffers makes both of addReceiver's adapters ready.
func bothOffers() []wire.Offer {
	return []wire.Offer{claudeOffer(), {Adapter: "cloud", Agent: "codex", Labels: map[string]string{"runtime": "exec"}}}
}

// advertise publishes a live advertisement; mutate may change it before it is signed.
func (r *testReceiver) advertise(t *testing.T, ctx context.Context, mutate func(*wire.Advertisement)) {
	t.Helper()
	ad := &wire.Advertisement{Receiver: r.id, Pool: r.pool, Offers: []wire.Offer{claudeOffer()}, Capacity: 2, SealingKey: signed.SealingKeyID(r.sealing.PublicKey()), AtMS: time.Now().UnixMilli()}
	if mutate != nil {
		mutate(ad)
	}
	r.produce(t, ctx, admin.ReceiversTopic, r.id, r.sign(t, ad))
}

func defaultSelector() []wire.Requirement {
	return []wire.Requirement{{Key: "runtime", Operator: wire.OpIn, Values: []string{"podman"}}}
}

func (f *fixture) submit(t *testing.T, selector []wire.Requirement) SubmitResult {
	t.Helper()
	result, err := f.host.Submit(f.ctx, SubmitInput{Selector: selector, Prompt: "do the work", Deadline: 30 * time.Minute, QueueDeadline: 10 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

// claimed is a task as a receiver reads and authorises it from its pool's task topic.
type claimed struct {
	submission wire.Submission
	hash       string
}

// claim reads the tasks for runID from the pool's task topic as receiver r and authorises them against the host keys
// of the pool's entitled tenants, as admission does.
func (r *testReceiver) claim(t *testing.T, ctx context.Context, runID string) []claimed {
	t.Helper()
	state, err := admin.Load(ctx, r.kafka)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := admin.ReadKeys(ctx, r.kafka, admin.HostKeyTopics(state, r.pool))
	if err != nil {
		t.Fatal(err)
	}
	records, err := admin.ReadTopic(ctx, r.kafka, admin.PoolTasksTopic(r.pool))
	if err != nil {
		t.Fatal(err)
	}
	var tasks []claimed
	for _, record := range records {
		submission, err := admin.AuthorizeSubmission(state, keys, record.Value, r.pool)
		if err != nil {
			t.Fatalf("task does not authorise: %v", err)
		}
		if submission.Wherry.Run == runID {
			sum := sha256.Sum256(record.Value)
			tasks = append(tasks, claimed{submission, hex.EncodeToString(sum[:])})
		}
	}
	return tasks
}

func (r *testReceiver) binding(task claimed, attempt int64) wire.ReceiverBinding {
	return wire.ReceiverBinding{Receiver: r.id, Tenant: task.submission.Wherry.Tenant, Pool: r.pool, Run: task.submission.Wherry.Run, Turn: task.submission.Wherry.Turn, Attempt: attempt}
}

func (r *testReceiver) echo(t *testing.T, task claimed) []byte {
	binding := task.submission.Wherry
	return r.sign(t, &wire.Echo{Receiver: r.id, Tenant: binding.Tenant, Pool: r.pool, Run: binding.Run, Turn: binding.Turn, Placement: binding.Placement,
		TaskHash: task.hash, MessageID: task.submission.Message.MessageID, DeadlineMS: binding.DeadlineMS, QueueDeadlineMS: binding.QueueDeadlineMS})
}

func (r *testReceiver) status(t *testing.T, task claimed, attempt int64, eventID, kind string, state wire.TaskState, mutate func(*wire.RunStatus)) []byte {
	t.Helper()
	run := task.submission.Wherry.Run
	status := &wire.RunStatus{
		StatusUpdate: wire.TaskStatusUpdateEvent{TaskID: run, ContextID: run, Status: wire.TaskStatus{State: state, Timestamp: time.Now().UTC().Format(time.RFC3339)}},
		Wherry:       wire.StatusBinding{ReceiverBinding: r.binding(task, attempt), EventID: eventID, Kind: kind, TaskHash: task.hash},
	}
	if mutate != nil {
		mutate(status)
	}
	return r.sign(t, status)
}

func (r *testReceiver) chunk(t *testing.T, task claimed, attempt, seq int64) []byte {
	run := task.submission.Wherry.Run
	return r.sign(t, &wire.Chunk{
		ArtifactUpdate: wire.TaskArtifactUpdateEvent{TaskID: run, ContextID: run, Artifact: wire.Artifact{ArtifactID: "transcript", Parts: []wire.Part{{Text: "line"}}, Extensions: []string{wire.ExtensionURI}}},
		Wherry:         wire.ChunkBinding{ReceiverBinding: r.binding(task, attempt), TaskHash: task.hash, Seq: seq},
	})
}

func (r *testReceiver) result(t *testing.T, task claimed, attempt int64, state wire.TaskState, mutate func(*wire.Result)) []byte {
	t.Helper()
	run := task.submission.Wherry.Run
	result := &wire.Result{
		Task: wire.Task{ID: run, ContextID: run, Status: wire.TaskStatus{State: state, Timestamp: time.Now().UTC().Format(time.RFC3339)},
			Artifacts: []wire.Artifact{{ArtifactID: "answer", Parts: []wire.Part{{Text: "finished"}}, Extensions: []string{wire.ExtensionURI}}},
			History:   []wire.Message{task.submission.Message}},
		Wherry: wire.ResultBinding{ReceiverBinding: r.binding(task, attempt), TaskHash: task.hash},
	}
	if mutate != nil {
		mutate(result)
	}
	return r.sign(t, result)
}

// feed writes records to the tenant feed under the run's key in one transaction, as a receiver commits them.
func (r *testReceiver) feed(t *testing.T, ctx context.Context, runID string, values ...[]byte) {
	t.Helper()
	producer, err := kgo.NewClient(append(r.kafka.Opts(), kgo.TransactionalID(admin.TransactionalPrefix(r.pool)+runID))...)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if err := producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	for _, value := range values {
		if err := producer.ProduceSync(ctx, &kgo.Record{Topic: admin.TenantTopic("tenant-a"), Key: []byte(runID), Value: value}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	if err := producer.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}
}

// admitted submits a task through receiver-a and writes its echo and claimed status, as admission does.
func (f *fixture) admitted(t *testing.T) (*testReceiver, claimed) {
	t.Helper()
	receiver := f.receivers["receiver-a"]
	if receiver == nil {
		receiver = f.addReceiver(t, "receiver-a", "pool-a")
		receiver.advertise(t, f.ctx, nil)
	}
	submitted := f.submit(t, defaultSelector())
	task := receiver.claim(t, f.ctx, submitted.RunID)[0]
	receiver.feed(t, f.ctx, submitted.RunID, receiver.echo(t, task), receiver.status(t, task, 1, "claimed-1", "claimed", wire.TaskStateWorking, nil))
	return receiver, task
}

func (f *fixture) end(t *testing.T, receiver string) {
	t.Helper()
	encoded, err := admin.EncodeRecord(admin.Record{Kind: "end", Receiver: receiver})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.service.Operator.ProduceSync(f.ctx, &kgo.Record{Topic: admin.AdminTopic, Value: encoded}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func TestHostEndToEnd(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	if binding := task.submission.Wherry; binding.Tenant != "tenant-a" || binding.Host != "host-a" || binding.Pool != "pool-a" || binding.Turn != 1 ||
		binding.Placement.Agent != "claude" || !reflect.DeepEqual(binding.Placement.Selector, defaultSelector()) {
		t.Fatalf("submitted binding %#v", binding)
	}
	if message := task.submission.Message; message.TaskID != runID || message.ContextID != runID || message.Parts[0].Text != "do the work" {
		t.Fatalf("submitted message %#v", message)
	}
	local, err := f.host.localRun(runID)
	if err != nil || local == nil || local.Run != runID || local.Pool != "pool-a" || local.Turn != 1 || local.TaskHash != task.hash || local.Placement.Agent != "claude" || !reflect.DeepEqual(local.Placement.Selector, defaultSelector()) {
		t.Fatalf("local record %#v, %v", local, err)
	}
	receiver.feed(t, f.ctx, runID, receiver.chunk(t, task, 1, 0), receiver.chunk(t, task, 1, 1))
	receiver.feed(t, f.ctx, runID, receiver.chunk(t, task, 2, 0), receiver.result(t, task, 2, wire.TaskStateCompleted, nil),
		receiver.status(t, task, 2, "terminal-2", "terminal", wire.TaskStateCompleted, nil))
	awaited, err := f.host.Await(f.ctx, runID, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if awaited.Task == nil || awaited.Status.State != wire.TaskStateCompleted || awaited.Task.Artifacts[0].Parts[0].Text != "finished" {
		t.Fatalf("awaited %#v", awaited)
	}
	events, err := f.host.Events(f.ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events.Statuses) != 2 || events.Statuses[1].Wherry.Kind != "terminal" || events.Cancelled ||
		events.Chunks[TurnAttempt{1, 1}] != 2 || events.Chunks[TurnAttempt{1, 2}] != 1 || len(events.Chunks) != 2 {
		t.Fatalf("events %#v", events)
	}
	status, err := f.host.Status(f.ctx, runID)
	if err != nil || status.Status.State != wire.TaskStateCompleted || status.Turn != 1 {
		t.Fatalf("status %#v, %v", status, err)
	}

	seals, err := admin.ReadTopic(f.ctx, receiver.kafka, admin.HostTopic("tenant-a", "host-a"))
	if err != nil {
		t.Fatal(err)
	}
	var opened []string
	for _, record := range seals {
		seal, err := signed.DecodeSeal(record.Value)
		if err != nil {
			continue
		}
		plaintext, err := signed.OpenCredential(seal, "tenant-a", "receiver-a", "pool-a", "model", time.Now(), []string{"model"}, receiver.sealing)
		if err != nil {
			t.Fatal(err)
		}
		opened = append(opened, string(plaintext))
	}
	if len(opened) != 1 || opened[0] != "model-token" {
		t.Fatalf("opened seals %q, want the model credential once", opened)
	}
}

func TestRegisterIsIdempotentAndRefusesAnotherKey(t *testing.T) {
	f := newFixture(t)
	id, err := f.host.Register(f.ctx)
	if err != nil || id != signed.KeyID(f.host.key.Public().(ed25519.PublicKey)) {
		t.Fatalf("second register %q, %v", id, err)
	}
	records, err := admin.ReadTopic(f.ctx, f.host.kafka, admin.HostTopic("tenant-a", "host-a"))
	if err != nil || len(records) != 1 {
		t.Fatalf("host topic holds %d records, %v; want one registration", len(records), err)
	}
	_, other, _ := ed25519.GenerateKey(rand.Reader)
	impostor := *f.host
	impostor.key = other
	if _, err := impostor.Register(f.ctx); err == nil {
		t.Fatal("registered a second key on the host topic")
	}
}

func TestNewHostParsesPrincipal(t *testing.T) {
	for _, principal := range []string{"wherry.rcv.receiver-a", "wherry.tnt.tenant-a", "wherry.tnt.tenant-a.host.a", "wherry.tnt.Tenant.host-a", "tnt.tenant-a.host-a"} {
		if _, err := NewHost(nil, nil, principal, "", nil); err == nil {
			t.Errorf("accepted principal %q", principal)
		}
	}
	host, err := NewHost(nil, nil, "wherry.tnt.tenant-a.host-a", "", nil)
	if err != nil || host.tenant != "tenant-a" || host.host != "host-a" {
		t.Fatalf("host %#v, %v", host, err)
	}
}

func (f *fixture) tasksEnd(t *testing.T, pool string) int {
	t.Helper()
	records, err := admin.ReadTopic(f.ctx, f.service.Admin, admin.PoolTasksTopic(pool))
	if err != nil {
		t.Fatal(err)
	}
	return len(records)
}

func TestSubmitNoMatchingReceiverProducesNothing(t *testing.T) {
	f := newFixture(t)
	// cloud would launch codex with runtime=exec, but it is not ready: the advertisement offers only lab.
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
	_, err := f.host.Submit(f.ctx, SubmitInput{Selector: []wire.Requirement{{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}}}, Prompt: "work", Deadline: time.Minute, QueueDeadline: time.Minute})
	if !errors.Is(err, ErrNoMatchingReceiver) {
		t.Fatalf("submit = %v, want no matching receiver", err)
	}
	if n := f.tasksEnd(t, "pool-a"); n != 0 {
		t.Fatalf("pool tasks hold %d records", n)
	}
}

func TestSubmitMatchesOneAdaptersLabels(t *testing.T) {
	f := newFixture(t)
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.Offers = bothOffers() })
	_, err := f.host.Submit(f.ctx, SubmitInput{Selector: []wire.Requirement{
		{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}}, {Key: "region", Operator: wire.OpIn, Values: []string{"eu"}},
	}, Prompt: "work", Deadline: time.Minute, QueueDeadline: time.Minute})
	if !errors.Is(err, ErrNoMatchingReceiver) {
		t.Fatalf("submit matched labels of two different adapters: %v", err)
	}
}

func TestSubmitMatchesAcrossAdapters(t *testing.T) {
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.Offers = bothOffers() })
	selector := []wire.Requirement{{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}}}
	result, err := f.host.Submit(f.ctx, SubmitInput{Selector: selector, Prompt: "work", Deadline: time.Minute, QueueDeadline: time.Minute})
	if err != nil {
		t.Fatalf("second adapter did not match: %v", err)
	}
	if placement := receiver.claim(t, f.ctx, result.RunID)[0].submission.Wherry.Placement; placement.Agent != "codex" || !reflect.DeepEqual(placement.Selector, selector) {
		t.Fatalf("placement %#v, want codex under the submitted selector", placement)
	}
	for name, input := range map[string]SubmitInput{
		"explicit agent the matching adapter does not launch": {Agent: "claude", Selector: selector},
		"agent outside the pool's terms":                      {Agent: "gemini"},
	} {
		input.Prompt, input.Deadline, input.QueueDeadline = "work", time.Minute, time.Minute
		if _, err := f.host.Submit(f.ctx, input); !errors.Is(err, ErrNoMatchingReceiver) {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

func TestSubmitIgnoresStaleOrUnauthorisedAdvertisements(t *testing.T) {
	for name, setup := range map[string]func(*testing.T, *fixture){
		"stale": func(t *testing.T, f *fixture) {
			f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.AtMS = time.Now().Add(-121 * time.Second).UnixMilli() })
		},
		"from the future": func(t *testing.T, f *fixture) {
			f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.AtMS = time.Now().Add(time.Hour).UnixMilli() })
		},
		"unregistered key": func(t *testing.T, f *fixture) {
			receiver := f.addReceiver(t, "receiver-a", "pool-a")
			_, receiver.key, _ = ed25519.GenerateKey(rand.Reader)
			receiver.advertise(t, f.ctx, nil)
		},
		"unregistered sealing key": func(t *testing.T, f *fixture) {
			other, _ := ecdh.X25519().GenerateKey(rand.Reader)
			f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.SealingKey = signed.SealingKeyID(other.PublicKey()) })
		},
		"ended member": func(t *testing.T, f *fixture) {
			f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
			f.end(t, "receiver-a")
		},
		"pool not entitled": func(t *testing.T, f *fixture) {
			if err := f.service.PoolCreate(f.ctx, "pool-b", testTerms()); err != nil {
				t.Fatal(err)
			}
			f.addReceiver(t, "receiver-b", "pool-b").advertise(t, f.ctx, nil)
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			setup(t, f)
			if _, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Deadline: time.Minute, QueueDeadline: time.Minute}); !errors.Is(err, ErrNoMatchingReceiver) {
				t.Fatalf("submit = %v, want no matching receiver", err)
			}
		})
	}
}

func TestSubmitChoosesPoolOfMostSpareMember(t *testing.T) {
	f := newFixture(t)
	f.pool(t, "pool-b")
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.Capacity, ad.InFlight = 4, 3 })
	f.addReceiver(t, "receiver-b", "pool-b").advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.Capacity, ad.InFlight = 4, 1 })
	f.addReceiver(t, "receiver-c", "pool-a").advertise(t, f.ctx, func(ad *wire.Advertisement) {
		ad.Capacity, ad.InFlight = 9, 0
		ad.Offers = bothOffers()[1:]
	})
	if result := f.submit(t, defaultSelector()); result.Pool != "pool-b" {
		t.Fatalf("pool %q, want pool-b through its member with most spare capacity", result.Pool)
	}
	f.receivers["receiver-a"].advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.Capacity, ad.InFlight = 4, 1 })
	if result := f.submit(t, defaultSelector()); result.Pool != "pool-a" {
		t.Fatalf("pool %q, want pool-a through the lower receiver ID on equal spare capacity", result.Pool)
	}
}

func TestResolvePlacement(t *testing.T) {
	terms := testTerms()
	terms.Agents = append(terms.Agents, admin.AgentTerms{Name: "gemini"})
	terms.DefaultAgent = "codex"
	// The declared agent order is claude, codex, gemini; codex is the default.
	config := admin.ReceiverConfig{Receiver: "receiver-a", Pool: "pool-a", Adapters: []admin.AdapterSettings{
		{Name: "lab", Type: "nomad", Agent: "claude", Labels: map[string]string{"runtime": "podman", "region": "eu", "gpu": "a100"}},
		{Name: "batch", Type: "nomad", Agent: "gemini", Labels: map[string]string{"runtime": "exec", "region": "us"}},
		{Name: "cloud", Type: "aws", Agent: "codex", Labels: map[string]string{"runtime": "exec"}},
		{Name: "spare", Type: "nomad", Agent: "codex", Labels: map[string]string{"runtime": "wasm"}},
		{Name: "rogue", Type: "nomad", Agent: "other"},
	}}
	state := admin.NewState()
	state.Pools["pool-a"] = terms
	state.Receivers["receiver-a"] = config
	state.Entitlements["tenant-a"] = map[string]bool{"pool-a": true}
	// Advertised labels deliberately disagree with the config record; only the record counts.
	offer := func(adapter, agent string) wire.Offer {
		return wire.Offer{Adapter: adapter, Agent: agent, Labels: map[string]string{"runtime": "wasm", "region": "mars"}}
	}
	ready := []wire.Offer{offer("lab", "claude"), offer("batch", "gemini"), offer("cloud", "codex"), offer("rogue", "other")}
	requirement := func(key, operator string, values ...string) wire.Requirement {
		return wire.Requirement{Key: key, Operator: operator, Values: values}
	}
	for name, test := range map[string]struct {
		agent    string
		selector []wire.Requirement
		offers   []wire.Offer
		want     string
	}{
		"default agent first":                     {"", nil, ready, "codex"},
		"declared order after the default":        {"", []wire.Requirement{requirement("region", wire.OpExists)}, ready, "claude"},
		"explicit agent":                          {"gemini", nil, ready, "gemini"},
		"explicit agent outside the terms":        {"other", nil, ready, ""},
		"explicit agent no ready adapter matches": {"claude", []wire.Requirement{requirement("region", wire.OpIn, "us")}, ready, ""},
		"In":                                  {"", []wire.Requirement{requirement("region", wire.OpIn, "us", "ap")}, ready, "gemini"},
		"NotIn":                               {"", []wire.Requirement{requirement("runtime", wire.OpNotIn, "exec")}, ready, "claude"},
		"NotIn passes an absent key":          {"", []wire.Requirement{requirement("gpu", wire.OpNotIn, "a100")}, ready, "codex"},
		"Exists":                              {"", []wire.Requirement{requirement("gpu", wire.OpExists)}, ready, "claude"},
		"DoesNotExist":                        {"", []wire.Requirement{requirement("region", wire.OpDoesNotExist)}, ready, "codex"},
		"all requirements on one adapter":     {"", []wire.Requirement{requirement("runtime", wire.OpIn, "exec"), requirement("gpu", wire.OpExists)}, ready, ""},
		"labels from the record, not the ad":  {"", []wire.Requirement{requirement("region", wire.OpIn, "mars")}, ready, ""},
		"not-ready adapter never chosen":      {"", []wire.Requirement{requirement("runtime", wire.OpIn, "wasm")}, ready, ""},
		"not-ready default falls to declared": {"", nil, ready[:2], "claude"},
		"advertised agent ignored":            {"claude", nil, []wire.Offer{offer("cloud", "claude")}, ""},
		"adapter outside the record":          {"", nil, []wire.Offer{offer("edge", "codex")}, ""},
	} {
		t.Run(name, func(t *testing.T) {
			ads := map[string]wire.Advertisement{"receiver-a": {Receiver: "receiver-a", Pool: "pool-a", Offers: test.offers, Capacity: 1}}
			pool, placement, ok := resolvePlacement(state, "tenant-a", test.agent, test.selector, ads)
			if ok && (pool != "pool-a" || !reflect.DeepEqual(placement.Selector, test.selector)) {
				t.Fatalf("pool %q placement %#v, want pool-a under the submitted selector", pool, placement)
			}
			if placement.Agent != test.want {
				t.Fatalf("placed %q, want %q", placement.Agent, test.want)
			}
		})
	}
	if _, _, ok := resolvePlacement(state, "tenant-b", "", nil, map[string]wire.Advertisement{"receiver-a": {Receiver: "receiver-a", Pool: "pool-a", Offers: ready}}); ok {
		t.Fatal("placed a task for a tenant not entitled to the pool")
	}
}

func TestResolvePlacementChoosesMostSpareThenLowestID(t *testing.T) {
	state := admin.NewState()
	state.Pools["pool-a"], state.Pools["pool-b"], state.Pools["pool-c"] = testTerms(), testTerms(), testTerms()
	state.Entitlements["tenant-a"] = map[string]bool{"pool-a": true, "pool-b": true}
	ads := map[string]wire.Advertisement{}
	member := func(id, pool string, capacity, inFlight int64, adapter string) {
		state.Receivers[id] = admin.ReceiverConfig{Receiver: id, Pool: pool, Adapters: []admin.AdapterSettings{
			{Name: "lab", Type: "nomad", Agent: "claude"}, {Name: "cloud", Type: "aws", Agent: "codex"},
		}}
		ads[id] = wire.Advertisement{Receiver: id, Pool: pool, Capacity: capacity, InFlight: inFlight, Offers: []wire.Offer{{Adapter: adapter, Agent: "x", Labels: map[string]string{}}}}
	}
	member("receiver-d", "pool-b", 5, 2, "lab")   // spare 3
	member("receiver-c", "pool-a", 5, 2, "lab")   // spare 3, lower ID
	member("receiver-b", "pool-a", 9, 0, "cloud") // most spare, but its ready adapter launches codex
	member("receiver-a", "pool-c", 9, 0, "lab")   // most spare, pool not entitled
	member("receiver-e", "pool-a", 4, 2, "lab")   // spare 2
	pool, placement, ok := resolvePlacement(state, "tenant-a", "claude", nil, ads)
	if !ok || pool != "pool-a" || placement.Agent != "claude" {
		t.Fatalf("placed %q %#v %v, want pool-a through receiver-c", pool, placement, ok)
	}
	ads["receiver-d"] = wire.Advertisement{Receiver: "receiver-d", Pool: "pool-b", Capacity: 5, InFlight: 1, Offers: ads["receiver-d"].Offers}
	if pool, _, _ := resolvePlacement(state, "tenant-a", "claude", nil, ads); pool != "pool-b" {
		t.Fatalf("pool %q, want pool-b through receiver-d's larger spare capacity", pool)
	}
}

// sealsTo returns the host's seals, newest last, addressed to receiver.
func (f *fixture) sealsTo(t *testing.T, receiver string) []signed.Seal {
	t.Helper()
	records, err := admin.ReadTopic(f.ctx, f.service.Admin, admin.HostTopic("tenant-a", "host-a"))
	if err != nil {
		t.Fatal(err)
	}
	var seals []signed.Seal
	for _, record := range records {
		if seal, err := signed.DecodeSeal(record.Value); err == nil && seal.Receiver == receiver {
			seals = append(seals, seal)
		}
	}
	return seals
}

func TestSubmitSealsToEveryLiveMemberLackingASeal(t *testing.T) {
	f := newFixture(t)
	f.pool(t, "pool-b")
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
	f.addReceiver(t, "receiver-b", "pool-a").advertise(t, f.ctx, nil)
	f.addReceiver(t, "receiver-c", "pool-a").advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.AtMS = time.Now().Add(-time.Hour).UnixMilli() })
	f.addReceiver(t, "receiver-d", "pool-b").advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.Offers = bothOffers()[1:] })
	f.submit(t, defaultSelector())
	for receiver, want := range map[string]int{"receiver-a": 1, "receiver-b": 1, "receiver-c": 0, "receiver-d": 0} {
		if got := len(f.sealsTo(t, receiver)); got != want {
			t.Fatalf("%s holds %d seals after the first submit, want %d", receiver, got, want)
		}
	}
	f.submit(t, defaultSelector())
	if got := len(f.sealsTo(t, "receiver-a")) + len(f.sealsTo(t, "receiver-b")); got != 2 {
		t.Fatalf("resealed to members holding a current seal: %d seals", got)
	}
	f.addReceiver(t, "receiver-e", "pool-a").advertise(t, f.ctx, nil)
	f.submit(t, defaultSelector())
	if got := len(f.sealsTo(t, "receiver-e")); got != 1 {
		t.Fatalf("new member holds %d seals, want 1", got)
	}
	// A current seal must outlast the queue deadline: a newer seal ending in 30 minutes is replaced for a 40-minute one.
	now := time.Now()
	short, err := signed.SealCredential("tenant-a", "receiver-a", "pool-a", "model", now, now.Add(30*time.Minute), f.receivers["receiver-a"].sealing.PublicKey(), []byte("model-token"))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := signed.EncodeSeal(short)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.host.produce(f.ctx, &kgo.Record{Topic: admin.HostTopic("tenant-a", "host-a"), Value: encoded}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Deadline: time.Hour, QueueDeadline: 40 * time.Minute}); err != nil {
		t.Fatal(err)
	}
	if got := len(f.sealsTo(t, "receiver-a")); got != 3 {
		t.Fatalf("seal lasting less than the queue deadline counted as current: %d seals", got)
	}
}

func TestSubmitPinsSealingKey(t *testing.T) {
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	f.submit(t, defaultSelector())
	pins, err := f.host.readPins()
	if err != nil || pins["receiver-a"] != signed.SealingKeyID(receiver.sealing.PublicKey()) {
		t.Fatalf("pins %v, %v", pins, err)
	}
	next, _ := ecdh.X25519().GenerateKey(rand.Reader)
	_, nextKey, _ := ed25519.GenerateKey(rand.Reader)
	rotation, err := signed.RotateKey("receiver-a", receiver.key, nextKey, next.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	receiver.produce(t, f.ctx, admin.ReceiverTopic("receiver-a"), "receiver-a", rotation)
	receiver.key, receiver.sealing = nextKey, next
	receiver.advertise(t, f.ctx, nil)
	before := f.tasksEnd(t, "pool-a")
	if _, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Deadline: time.Minute, QueueDeadline: time.Minute}); err == nil {
		t.Fatal("sealed to a sealing key other than the pinned one")
	}
	if len(f.sealsTo(t, "receiver-a")) != 1 || f.tasksEnd(t, "pool-a") != before {
		t.Fatal("a refused seal still wrote a seal or a task")
	}
}

func TestStatusBeforeAndAfterEcho(t *testing.T) {
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	submitted := f.submit(t, defaultSelector())
	status, err := f.host.Status(f.ctx, submitted.RunID)
	if err != nil || status.Status.State != wire.TaskStateSubmitted || status.Turn != 1 {
		t.Fatalf("status before the echo %#v, %v", status, err)
	}
	other := f.newHost(t, "host-b", t.TempDir())
	if _, err := other.Status(f.ctx, submitted.RunID); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("another host's status before the echo: %v, want run not found", err)
	}
	task := receiver.claim(t, f.ctx, submitted.RunID)[0]
	receiver.feed(t, f.ctx, submitted.RunID, receiver.echo(t, task), receiver.status(t, task, 1, "claimed-1", "claimed", wire.TaskStateWorking, nil))
	for _, host := range []*Host{f.host, other} {
		if status, err := host.Status(f.ctx, submitted.RunID); err != nil || status.Status.State != wire.TaskStateWorking {
			t.Fatalf("status after the claim %#v, %v", status, err)
		}
	}
	if _, err := f.host.Status(f.ctx, "run-missing"); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("status of an unknown run: %v", err)
	}
}

func TestAwaitAcceptsOnlyBoundRecords(t *testing.T) {
	for name, forge := range map[string]func(*testing.T, *fixture, *testReceiver, claimed) []byte{
		"unregistered key": func(t *testing.T, _ *fixture, receiver *testReceiver, task claimed) []byte {
			impostor := *receiver
			_, impostor.key, _ = ed25519.GenerateKey(rand.Reader)
			return impostor.result(t, task, 1, wire.TaskStateCompleted, nil)
		},
		"task hash": func(t *testing.T, _ *fixture, receiver *testReceiver, task claimed) []byte {
			task.hash = hex.EncodeToString(make([]byte, 32))
			return receiver.result(t, task, 1, wire.TaskStateCompleted, nil)
		},
		"history": func(t *testing.T, _ *fixture, receiver *testReceiver, task claimed) []byte {
			return receiver.result(t, task, 1, wire.TaskStateCompleted, func(result *wire.Result) { result.Task.History = []wire.Message{} })
		},
		"message": func(t *testing.T, _ *fixture, receiver *testReceiver, task claimed) []byte {
			return receiver.result(t, task, 1, wire.TaskStateCompleted, func(result *wire.Result) { result.Task.History[0].MessageID = "msg-other" })
		},
		"turn": func(t *testing.T, _ *fixture, receiver *testReceiver, task claimed) []byte {
			task.submission.Wherry.Turn = 2
			task.submission.Message.MessageID = "msg-turn"
			return receiver.result(t, task, 1, wire.TaskStateCompleted, func(result *wire.Result) {
				result.Task.History = append([]wire.Message{task.submission.Message}, task.submission.Message)
			})
		},
		"other pool": func(t *testing.T, f *fixture, _ *testReceiver, task claimed) []byte {
			f.pool(t, "pool-b")
			other := f.addReceiver(t, "receiver-b", "pool-b")
			return other.result(t, task, 1, wire.TaskStateCompleted, nil)
		},
		"working": func(t *testing.T, _ *fixture, receiver *testReceiver, task claimed) []byte {
			return receiver.result(t, task, 1, wire.TaskStateWorking, nil)
		},
		"terminal status alone": func(t *testing.T, _ *fixture, receiver *testReceiver, task claimed) []byte {
			return receiver.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateCompleted, nil)
		},
		"status hash": func(t *testing.T, _ *fixture, receiver *testReceiver, task claimed) []byte {
			task.hash = hex.EncodeToString(make([]byte, 32))
			return receiver.status(t, task, 1, "interrupted-1", "interrupted", wire.TaskStateAuthRequired, nil)
		},
		"status from other pool": func(t *testing.T, f *fixture, _ *testReceiver, task claimed) []byte {
			f.pool(t, "pool-b")
			other := f.addReceiver(t, "receiver-b", "pool-b")
			return other.status(t, task, 1, "interrupted-1", "interrupted", wire.TaskStateAuthRequired, nil)
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			receiver, task := f.admitted(t)
			forged := forge(t, f, receiver, task)
			receiver.feed(t, f.ctx, task.submission.Wherry.Run, forged)
			if awaited, err := f.host.Await(f.ctx, task.submission.Wherry.Run, 300*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("await = %#v, %v; want the %s record ignored", awaited, err, name)
			}
		})
	}
}

func TestAwaitHighAttemptDoesNotMaskResult(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	receiver.feed(t, f.ctx, runID, receiver.result(t, task, 2, wire.TaskStateWorking, nil))
	receiver.feed(t, f.ctx, runID, receiver.result(t, task, 1, wire.TaskStateCompleted, nil))
	awaited, err := f.host.Await(f.ctx, runID, 5*time.Second)
	if err != nil || awaited.Task == nil || awaited.Status.State != wire.TaskStateCompleted {
		t.Fatalf("await %#v, %v", awaited, err)
	}
}

func TestAwaitFailsClosedWithoutRun(t *testing.T) {
	f := newFixture(t)
	if _, err := f.host.Await(f.ctx, "run-missing", time.Second); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("await = %v, want run not found", err)
	}
}

func TestAwaitReturnsOnInterruptedOrRejected(t *testing.T) {
	t.Run("interrupted", func(t *testing.T) {
		f := newFixture(t)
		receiver, task := f.admitted(t)
		receiver.feed(t, f.ctx, task.submission.Wherry.Run, receiver.status(t, task, 1, "interrupted-1", "interrupted", wire.TaskStateAuthRequired, nil))
		awaited, err := f.host.Await(f.ctx, task.submission.Wherry.Run, 5*time.Second)
		if err != nil || awaited.Task != nil || awaited.Status.State != wire.TaskStateAuthRequired {
			t.Fatalf("await %#v, %v; want AUTH_REQUIRED without a task", awaited, err)
		}
	})
}

func TestAwaitKeepsArtifactsWhenResultFollows(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	go func() {
		time.Sleep(500 * time.Millisecond)
		receiver.feed(t, f.ctx, runID, receiver.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateCompleted, nil), receiver.result(t, task, 1, wire.TaskStateCompleted, nil))
	}()
	awaited, err := f.host.Await(f.ctx, runID, 10*time.Second)
	if err != nil || awaited.Task == nil {
		t.Fatalf("await %#v, %v; want the result", awaited, err)
	}
}

func TestAwaitTakesLaterCancelOfInterruptedRun(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	receiver.feed(t, f.ctx, runID, receiver.status(t, task, 3, "interrupted-1", "interrupted", wire.TaskStateAuthRequired, nil))
	peer := f.addReceiver(t, "receiver-b", "pool-a")
	receiver.feed(t, f.ctx, runID, peer.status(t, task, 1, runID+"/1/canceled", "terminal", wire.TaskStateCanceled, nil))
	for _, read := range []func() (wire.TaskState, error){
		func() (wire.TaskState, error) {
			awaited, err := f.host.Await(f.ctx, runID, 5*time.Second)
			return awaited.Status.State, err
		},
		func() (wire.TaskState, error) {
			status, err := f.host.Status(f.ctx, runID)
			return status.Status.State, err
		},
	} {
		if state, err := read(); err != nil || state != wire.TaskStateCanceled {
			t.Fatalf("state %s, %v; want the CANCELED terminal written after the interrupt", state, err)
		}
	}
	if _, err := f.host.Continue(f.ctx, runID, ContinueInput{Prompt: "again", Deadline: time.Minute, QueueDeadline: time.Minute}); !errors.Is(err, ErrRunCanceled) {
		t.Fatalf("continue after cancel: %v", err)
	}
}

func TestEchoFromAnotherPoolDoesNotMoveTheRun(t *testing.T) {
	f := newFixture(t)
	_, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	f.pool(t, "pool-b")
	forger := f.addReceiver(t, "receiver-b", "pool-b")
	forger.feed(t, f.ctx, runID, forger.echo(t, task), forger.result(t, task, 1, wire.TaskStateCompleted, nil))
	other := f.newHost(t, "host-b", t.TempDir())
	for name, host := range map[string]*Host{"submitting host": f.host, "other host": other} {
		if awaited, err := host.Await(f.ctx, runID, 300*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("%s accepted a result through another pool's echo: %#v, %v", name, awaited, err)
		}
	}
}

func TestRecordsAfterMembershipEndAreRejected(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	receiver.feed(t, f.ctx, runID, receiver.result(t, task, 1, wire.TaskStateCompleted, nil))
	if _, err := f.host.Await(f.ctx, runID, 5*time.Second); err != nil {
		t.Fatalf("before the membership end: %v", err)
	}
	f.end(t, "receiver-a")
	if awaited, err := f.host.Await(f.ctx, runID, 300*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("await after the membership end %#v, %v", awaited, err)
	}
	if status, err := f.host.Status(f.ctx, runID); err != nil || status.Status.State != wire.TaskStateSubmitted {
		t.Fatalf("status after the membership end %#v, %v; want only the local record", status, err)
	}
}

func TestContinueThroughTheEcho(t *testing.T) {
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.Offers = bothOffers() })
	selector := []wire.Requirement{{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}}}
	submitted := f.submit(t, selector)
	task := receiver.claim(t, f.ctx, submitted.RunID)[0]
	runID := task.submission.Wherry.Run
	receiver.feed(t, f.ctx, runID, receiver.echo(t, task), receiver.status(t, task, 1, "interrupted-1", "interrupted", wire.TaskStateAuthRequired, nil))
	// A pool with more spare capacity appears: a continuation placed afresh would go there, with the default agent.
	f.pool(t, "pool-b")
	f.addReceiver(t, "receiver-b", "pool-b").advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.Capacity = 9 })
	// Another host of the tenant has no local record: the pool and placement come from the echo alone.
	other := f.newHost(t, "host-b", t.TempDir())
	if _, err := f.host.Continue(f.ctx, "run-missing", ContinueInput{Prompt: "again", Deadline: time.Minute, QueueDeadline: time.Minute}); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("continue of an unknown run: %v", err)
	}
	result, err := other.Continue(f.ctx, runID, ContinueInput{Prompt: "again", Deadline: time.Minute, QueueDeadline: time.Minute})
	if err != nil || result.Turn != 2 {
		t.Fatalf("continue %#v, %v", result, err)
	}
	tasks := receiver.claim(t, f.ctx, runID)
	if len(tasks) != 2 {
		t.Fatalf("pool-a tasks hold %d turns", len(tasks))
	}
	next := tasks[1].submission
	if next.Wherry.Turn != 2 || next.Wherry.Host != "host-b" || next.Wherry.Pool != "pool-a" || next.Wherry.Placement.Agent != "codex" ||
		!reflect.DeepEqual(next.Wherry.Placement.Selector, selector) || next.Message.Parts[0].Text != "again" || next.Message.ContextID != runID {
		t.Fatalf("continuation %#v", next)
	}
	sealed := map[string]bool{}
	records, err := admin.ReadTopic(f.ctx, f.service.Admin, admin.HostTopic("tenant-a", "host-b"))
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if seal, err := signed.DecodeSeal(record.Value); err == nil {
			sealed[seal.Receiver+"/"+seal.Pool+"/"+seal.Credential] = true
		}
	}
	if len(sealed) != 1 || !sealed["receiver-a/pool-a/openai"] {
		t.Fatalf("continuing host sealed %v, want codex's credential resealed to pool-a", sealed)
	}
	if _, err := other.Continue(f.ctx, runID, ContinueInput{Prompt: "again", Deadline: time.Minute, QueueDeadline: time.Minute}); !errors.Is(err, ErrTurnSubmitted) {
		t.Fatalf("second continue: %v", err)
	}
}

func TestContinueRequiresInterruptedTurn(t *testing.T) {
	f := newFixture(t)
	_, task := f.admitted(t)
	if _, err := f.host.Continue(f.ctx, task.submission.Wherry.Run, ContinueInput{Prompt: "again", Deadline: time.Minute, QueueDeadline: time.Minute}); !errors.Is(err, ErrNotAwaitingContinuation) {
		t.Fatalf("continued a working run: %v", err)
	}
}

func TestContinueIgnoresRejectedStatus(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	receiver.feed(t, f.ctx, runID, receiver.status(t, task, 1, "interrupted-1", "interrupted", wire.TaskStateAuthRequired, nil),
		receiver.status(t, task, 2, "rejected-1", "rejected", wire.TaskStateRejected, nil))
	if result, err := f.host.Continue(f.ctx, runID, ContinueInput{Prompt: "again", Deadline: time.Minute, QueueDeadline: time.Minute}); err != nil || result.Turn != 2 {
		t.Fatalf("continue %#v, %v", result, err)
	}
}

func TestCancelWritesToTheEchosPool(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	other := f.newHost(t, "host-b", t.TempDir())
	if _, err := other.Cancel(f.ctx, runID); err != nil {
		t.Fatal(err)
	}
	if _, err := other.Cancel(f.ctx, "run-missing"); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("cancel of an unknown run: %v", err)
	}
	state, err := admin.Load(f.ctx, receiver.kafka)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := admin.ReadKeys(f.ctx, receiver.kafka, admin.HostKeyTopics(state, "pool-a"))
	if err != nil {
		t.Fatal(err)
	}
	records, err := admin.ReadTopic(f.ctx, receiver.kafka, admin.PoolEventsTopic("pool-a"))
	if err != nil || len(records) != 1 {
		t.Fatalf("pool events hold %d records, %v", len(records), err)
	}
	cancel, err := admin.AuthorizeCancel(state, keys, records[0].Value, "pool-a", "tenant-a")
	if err != nil || cancel.Run != runID || cancel.Turn != 1 || cancel.Host != "host-b" {
		t.Fatalf("cancel %#v, %v", cancel, err)
	}
}

func TestReceiversListsLiveStaleSilentAndEnded(t *testing.T) {
	f := newFixture(t)
	f.pool(t, "pool-b")
	// The advertisement's labels disagree with the config record; the listing shows the record's.
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, func(ad *wire.Advertisement) {
		ad.Capacity, ad.InFlight = 3, 1
		ad.Offers[0].Labels = map[string]string{"runtime": "wasm"}
	})
	f.addReceiver(t, "receiver-b", "pool-b").advertise(t, f.ctx, func(ad *wire.Advertisement) {
		ad.Capacity, ad.InFlight, ad.AtMS = 4, 2, time.Now().Add(-121*time.Second).UnixMilli()
	})
	f.addReceiver(t, "receiver-c", "pool-a")
	f.addReceiver(t, "receiver-d", "pool-a").advertise(t, f.ctx, nil)
	f.submit(t, defaultSelector())
	f.end(t, "receiver-d")
	if err := f.service.PoolCreate(f.ctx, "pool-c", testTerms()); err != nil {
		t.Fatal(err)
	}
	f.addReceiver(t, "receiver-e", "pool-c").advertise(t, f.ctx, nil)
	receivers, err := f.host.Receivers(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]Receiver{}
	var order []string
	for _, receiver := range receivers {
		got[receiver.ReceiverID] = receiver
		order = append(order, receiver.ReceiverID)
	}
	if want := []string{"receiver-a", "receiver-c", "receiver-d", "receiver-b"}; len(order) != len(want) || order[0] != want[0] || order[1] != want[1] || order[2] != want[2] || order[3] != want[3] {
		t.Fatalf("receivers %v, want %v", order, want)
	}
	if a := got["receiver-a"]; a.Status != "live" || a.Capacity != 3 || a.InFlight != 1 || len(a.Seals) != 1 || a.Seals[0].Name != "model" || a.Seals[0].Pool != "pool-a" ||
		!reflect.DeepEqual(a.Adapters, []Adapter{
			{Name: "lab", Agent: "claude", Labels: map[string]string{"runtime": "podman", "region": "eu"}, Ready: true},
			{Name: "cloud", Agent: "codex", Labels: map[string]string{"runtime": "exec"}, Ready: false},
		}) {
		t.Fatalf("live receiver %#v", a)
	}
	if b := got["receiver-b"]; b.Status != "stale" || b.Capacity != 4 || b.InFlight != 2 || len(b.Adapters) != 2 || b.Adapters[0].Ready || b.Adapters[1].Ready {
		t.Fatalf("stale receiver %#v; want its adapters listed, none ready", b)
	}
	if c := got["receiver-c"]; c.Status != "silent" || c.HeartbeatAge != "-" {
		t.Fatalf("silent receiver %#v", c)
	}
	if d := got["receiver-d"]; d.Status != "ended" || !d.Ended {
		t.Fatalf("ended receiver %#v", d)
	}
}

func TestEventsBindsRecordsToTheRun(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	f.pool(t, "pool-b")
	other := f.addReceiver(t, "receiver-b", "pool-b")
	_, otherTask := f.admitted(t)
	wrongHash := task
	wrongHash.hash = hex.EncodeToString(make([]byte, 32))
	wrongTurn := task
	wrongTurn.submission.Wherry.Turn = 2
	receiver.feed(t, f.ctx, runID,
		receiver.status(t, task, 1, "claimed-1", "claimed", wire.TaskStateWorking, nil),
		receiver.status(t, wrongHash, 1, "started-1", "started", wire.TaskStateWorking, nil),
		receiver.status(t, wrongTurn, 1, "started-2", "started", wire.TaskStateWorking, nil),
		other.status(t, task, 1, "started-3", "started", wire.TaskStateWorking, nil),
		receiver.chunk(t, task, 1, 0),
		receiver.chunk(t, wrongTurn, 1, 0),
		receiver.chunk(t, wrongHash, 1, 2),
		other.chunk(t, task, 1, 1),
		[]byte("not a record"),
	)
	receiver.feed(t, f.ctx, otherTask.submission.Wherry.Run, receiver.status(t, otherTask, 1, "started-4", "started", wire.TaskStateWorking, nil), []byte("not a record"))
	events, err := f.host.Events(f.ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events.Statuses) != 1 || events.Statuses[0].Wherry.EventID != "claimed-1" || events.Chunks[TurnAttempt{1, 1}] != 1 || len(events.Chunks) != 1 || events.Skipped != 7 {
		t.Fatalf("statuses %d chunks %v skipped %d, want the one claimed status, one chunk and seven skipped", len(events.Statuses), events.Chunks, events.Skipped)
	}
	if _, err := f.host.Events(f.ctx, "run-missing"); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("events of an unknown run: %v", err)
	}
}

func TestConfigRejectsUnknownField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"bundle":"bundle.yaml","unknown":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("config with unknown field was accepted")
	}
}

func TestConnectErrorsOmitPassword(t *testing.T) {
	const password = "kafka-password-secret"
	dir := t.TempDir()
	config := Config{Bundle: filepath.Join(dir, "bundle.yaml"), HostKey: filepath.Join(dir, "missing.key"), StateDir: dir}
	for name, bundle := range map[string]string{
		"missing key": "broker: localhost:9092\nca: x\nprincipal: wherry.tnt.tenant-a.host-a\npassword: " + password + "\n",
		"bad yaml":    "broker: [localhost\npassword: " + password + "\n",
		"wrong type":  "broker: localhost:9092\nprincipal: wherry.tnt.tenant-a.host-a\npassword: [" + password + "]\n",
		"unknown":     "broker: localhost:9092\npassword: " + password + "\nextra: " + password + "\n",
	} {
		if err := os.WriteFile(config.Bundle, []byte(bundle), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Connect(config); err == nil || strings.Contains(err.Error(), password) {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

// awaitInBackground starts Await and returns a channel with its result, once the follower has had time to start.
func awaitInBackground(f *fixture, host *Host, runID string, timeout time.Duration) <-chan error {
	done := make(chan error, 1)
	go func() {
		awaited, err := host.Await(f.ctx, runID, timeout)
		if err == nil {
			err = fmt.Errorf("accepted %s, task %t", awaited.Status.State, awaited.Task != nil)
		}
		done <- err
	}()
	time.Sleep(time.Second)
	return done
}

func TestAwaitRejectsRecordsFromMemberEndedDuringAwait(t *testing.T) {
	for name, outcome := range map[string]func(*testing.T, *testReceiver, claimed) []byte{
		"result": func(t *testing.T, receiver *testReceiver, task claimed) []byte {
			return receiver.result(t, task, 1, wire.TaskStateCompleted, nil)
		},
		"interrupted status": func(t *testing.T, receiver *testReceiver, task claimed) []byte {
			return receiver.status(t, task, 1, "interrupted-1", "interrupted", wire.TaskStateAuthRequired, nil)
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			receiver, task := f.admitted(t)
			runID := task.submission.Wherry.Run
			done := awaitInBackground(f, f.host, runID, 4*time.Second)
			f.end(t, "receiver-a")
			receiver.feed(t, f.ctx, runID, outcome(t, receiver, task))
			if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("await = %v; want the ended member's %s refused", err, name)
			}
		})
	}
}

func TestAwaitRejectsOutcomeWhoseEchoingMemberEnded(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	peer := f.addReceiver(t, "receiver-b", "pool-a")
	other := f.newHost(t, "host-b", t.TempDir())
	done := awaitInBackground(f, other, runID, 4*time.Second)
	f.end(t, "receiver-a")
	receiver.feed(t, f.ctx, runID, peer.result(t, task, 2, wire.TaskStateCompleted, nil))
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("await = %v; want a run bound only by an ended member's echo refused", err)
	}
}

func TestAwaitAcceptsMemberAddedDuringAwait(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	done := make(chan error, 1)
	go func() {
		awaited, err := f.host.Await(f.ctx, runID, 10*time.Second)
		if err == nil && awaited.Status.State != wire.TaskStateCompleted {
			err = fmt.Errorf("state %s", awaited.Status.State)
		}
		done <- err
	}()
	time.Sleep(time.Second)
	adopter := f.addReceiver(t, "receiver-b", "pool-a")
	receiver.feed(t, f.ctx, runID, adopter.result(t, task, 2, wire.TaskStateCompleted, nil))
	if err := <-done; err != nil {
		t.Fatalf("await of a result from a member added since it started: %v", err)
	}
}

func TestAwaitRereadsRotatedKey(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	done := make(chan error, 1)
	go func() {
		awaited, err := f.host.Await(f.ctx, runID, 10*time.Second)
		if err == nil && awaited.Task == nil {
			err = fmt.Errorf("state %s without the result", awaited.Status.State)
		}
		done <- err
	}()
	time.Sleep(time.Second)
	_, next, _ := ed25519.GenerateKey(rand.Reader)
	rotation, err := signed.RotateKey("receiver-a", receiver.key, next, receiver.sealing.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	receiver.produce(t, f.ctx, admin.ReceiverTopic("receiver-a"), "receiver-a", rotation)
	receiver.key = next
	receiver.feed(t, f.ctx, runID, receiver.result(t, task, 1, wire.TaskStateCompleted, nil))
	if err := <-done; err != nil {
		t.Fatalf("await of a result signed with the rotated key: %v", err)
	}
}

func TestAwaitEndsOnCanceledTerminal(t *testing.T) {
	for name, eventID := range map[string]string{"holder's own event ID": "terminal-1", "interrupted run's event ID": ""} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			receiver, task := f.admitted(t)
			runID := task.submission.Wherry.Run
			if eventID == "" {
				eventID = runID + "/1/canceled"
			}
			receiver.feed(t, f.ctx, runID, receiver.status(t, task, 1, eventID, "terminal", wire.TaskStateCanceled, nil))
			awaited, err := f.host.Await(f.ctx, runID, 5*time.Second)
			if err != nil || awaited.Status.State != wire.TaskStateCanceled {
				t.Fatalf("await %#v, %v; want CANCELED", awaited, err)
			}
		})
	}
}

func TestRejectionIsFoundThroughItsEcho(t *testing.T) {
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	submitted := f.submit(t, defaultSelector())
	task := receiver.claim(t, f.ctx, submitted.RunID)[0]
	receiver.feed(t, f.ctx, submitted.RunID, receiver.echo(t, task), receiver.status(t, task, 1, "rejected-1", "rejected", wire.TaskStateRejected, nil))
	for name, host := range map[string]*Host{"submitting host": f.host, "another host of the tenant": f.newHost(t, "host-b", t.TempDir())} {
		awaited, err := host.Await(f.ctx, submitted.RunID, 5*time.Second)
		if err != nil || awaited.Task != nil || awaited.Status.State != wire.TaskStateRejected {
			t.Fatalf("%s: await %#v, %v; want REJECTED", name, awaited, err)
		}
	}
}

func TestRegisterConcurrently(t *testing.T) {
	f := newFixture(t)
	bundle := filepath.Join(t.TempDir(), "bundle.yaml")
	if err := f.service.HostAdd(f.ctx, "tenant-a", "host-c", bundle); err != nil {
		t.Fatal(err)
	}
	hosts := make([]*Host, 8)
	for i := range hosts {
		kafka, principal := f.connect(t, bundle)
		_, key, _ := ed25519.GenerateKey(rand.Reader)
		host, err := NewHost(kafka, key, principal, t.TempDir(), nil)
		if err != nil {
			t.Fatal(err)
		}
		hosts[i] = host
	}
	errs := make([]error, len(hosts))
	var wg sync.WaitGroup
	for i, host := range hosts {
		wg.Go(func() { _, errs[i] = host.Register(f.ctx) })
	}
	wg.Wait()
	owner := signed.HostOwner("tenant-a", "host-c")
	keys, err := admin.ReadKeys(f.ctx, f.service.Admin, map[string]string{owner: admin.HostTopic("tenant-a", "host-c")})
	if err != nil {
		t.Fatal(err)
	}
	var won []int
	for i, err := range errs {
		if err == nil {
			won = append(won, i)
		}
	}
	if len(won) != 1 || !keys[owner].Current.Equal(hosts[won[0]].key.Public()) {
		t.Fatalf("registrations %v succeeded; want exactly the host whose key is current", won)
	}
}

func TestPinsSurviveConcurrentWriters(t *testing.T) {
	dir := t.TempDir()
	host, other := &Host{stateDir: dir}, &Host{stateDir: dir}
	lock, err := os.OpenFile(filepath.Join(dir, "pins.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	a, _ := ecdh.X25519().GenerateKey(rand.Reader)
	b, _ := ecdh.X25519().GenerateKey(rand.Reader)
	done := make(chan error, 1)
	go func() { done <- host.pin(map[string]*ecdh.PublicKey{"receiver-a": a.PublicKey()}) }()
	select {
	case err := <-done:
		t.Fatalf("pin finished while another writer held the lock: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	// The lock holder, as another process would, pins receiver-b.
	if err := other.writePins(map[string]string{"receiver-b": signed.SealingKeyID(b.PublicKey())}); err != nil {
		t.Fatal(err)
	}
	lock.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	pins, err := host.readPins()
	if err != nil || pins["receiver-a"] != signed.SealingKeyID(a.PublicKey()) || pins["receiver-b"] != signed.SealingKeyID(b.PublicKey()) {
		t.Fatalf("pins %v, %v; want both writers' pins", pins, err)
	}
}

func TestSealOutlivesTheQueueDeadline(t *testing.T) {
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	submitted, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Deadline: time.Hour, QueueDeadline: 50 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	queueDeadline := receiver.claim(t, f.ctx, submitted.RunID)[0].submission.Wherry.QueueDeadlineMS
	if seals := f.sealsTo(t, "receiver-a"); len(seals) != 1 || seals[0].ExpiryMS <= queueDeadline {
		t.Fatalf("seals %#v; want one expiring after the queue deadline %d", seals, queueDeadline)
	}
	// The pool seals for an hour: a two-hour queue deadline cannot be covered.
	if _, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Deadline: 3 * time.Hour, QueueDeadline: 2 * time.Hour}); !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "queue deadline") {
		t.Fatalf("submit with a queue deadline past the seal lifetime: %v", err)
	}
	if len(f.sealsTo(t, "receiver-a")) != 1 || f.tasksEnd(t, "pool-a") != 1 {
		t.Fatal("a refused submit still wrote a seal or a task")
	}
}

func TestSupersededCancelDoesNotMaskALaterAttempt(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	receiver.feed(t, f.ctx, runID, receiver.status(t, task, 2, "started-2", "started", wire.TaskStateWorking, nil),
		receiver.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateCanceled, nil))
	if status, err := f.host.Status(f.ctx, runID); err != nil || status.Status.State != wire.TaskStateWorking {
		t.Fatalf("status %#v, %v; want attempt 2's WORKING over attempt 1's CANCELED", status, err)
	}
}

func TestAwaitIgnoresWhatAnEndedMemberWritesAfterItsEnd(t *testing.T) {
	for name, stale := range map[string]func(*testing.T, *testReceiver, claimed) []byte{
		"interrupted status with a high attempt": func(t *testing.T, ended *testReceiver, task claimed) []byte {
			return ended.status(t, task, 9, "interrupted-9", "interrupted", wire.TaskStateAuthRequired, nil)
		},
		"echo for a later turn": func(t *testing.T, ended *testReceiver, task claimed) []byte {
			task.submission.Wherry.Turn = 2
			return ended.echo(t, task)
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			ended, task := f.admitted(t)
			runID := task.submission.Wherry.Run
			peer := f.addReceiver(t, "receiver-b", "pool-a")
			type outcome struct {
				result AwaitResult
				err    error
			}
			done := make(chan outcome, 1)
			go func() {
				result, err := f.host.Await(f.ctx, runID, 10*time.Second)
				done <- outcome{result, err}
			}()
			time.Sleep(time.Second)
			f.end(t, "receiver-a")
			ended.feed(t, f.ctx, runID, stale(t, ended, task))
			time.Sleep(500 * time.Millisecond)
			peer.feed(t, f.ctx, runID, peer.echo(t, task), peer.result(t, task, 2, wire.TaskStateCompleted, nil))
			got := <-done
			if got.err != nil || got.result.Task == nil || got.result.Status.State != wire.TaskStateCompleted {
				t.Fatalf("await %#v, %v; want the live member's result", got.result, got.err)
			}
		})
	}
}

func TestReceiversDoesNotShowAFutureAdvertisementAsLive(t *testing.T) {
	f := newFixture(t)
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.AtMS = time.Now().Add(time.Hour).UnixMilli() })
	receivers, err := f.host.Receivers(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(receivers) != 1 || receivers[0].Status == "live" || strings.HasPrefix(receivers[0].HeartbeatAge, "-") || receivers[0].Adapters[0].Ready {
		t.Fatalf("receivers %#v; want a future advertisement neither live, ready nor of negative age", receivers)
	}
}
