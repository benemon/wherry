package client

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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
	// logs holds what every host of the fixture logs.
	logs *syncBuffer
}

type testReceiver struct {
	id    string
	pool  string
	key   ed25519.PrivateKey
	kafka *kgo.Client
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func testTerms() admin.Terms {
	optional := false
	return admin.Terms{
		Agents: []admin.AgentTerms{
			{Name: "claude", Credentials: []admin.AgentCredential{{TaskCredential: wire.TaskCredential{Name: "model", Env: "MODEL_API_KEY"}}}},
			{Name: "codex", Credentials: []admin.AgentCredential{
				{TaskCredential: wire.TaskCredential{Name: "openai", Env: "OPENAI_API_KEY"}},
				{TaskCredential: wire.TaskCredential{Name: "github-token", Env: "GH_TOKEN"}, Required: &optional},
			}},
		},
		DefaultAgent:  "claude",
		DeliveryLimit: 3, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50,
	}
}

func newFixtureNoHost(t *testing.T, opts ...kfake.Opt) *fixture {
	t.Helper()
	cluster := kfake.MustCluster(append([]kfake.Opt{kfake.EnableSASL(), kfake.EnableACLs(), kfake.Superuser("SCRAM-SHA-512", "admin", "password"), kfake.Superuser("PLAIN", "wherry.operator", "operator-password")}, opts...)...)
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
	f := &fixture{ctx: ctx, service: &admin.Service{Admin: root, Operator: operator, Broker: cluster.ListenAddrs()[0], DeliveryCeiling: 10}, receivers: map[string]*testReceiver{}, logs: &syncBuffer{}}
	f.pool(t, "pool-a")
	return f
}

func newFixture(t *testing.T, opts ...kfake.Opt) *fixture {
	t.Helper()
	f := newFixtureNoHost(t, opts...)
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
func (f *fixture) connect(t *testing.T, path string, extra ...kgo.Opt) (*kgo.Client, string) {
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
	kafka, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(f.service.Broker), kgo.SASL(scram.Auth{User: bundle.Principal, Pass: bundle.Password}.AsSha512Mechanism()),
		kgo.FetchMaxWait(10 * time.Millisecond), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.DisableClientMetrics()}, extra...)...)
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
	h, err := NewHost(kafka, key, principal, stateDir, map[string]credential.Source{"model": {File: f.secret}, "openai": {File: f.secret}}, nil, log.New(f.logs, host+": ", 0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Register(f.ctx); err != nil {
		t.Fatal(err)
	}
	return h
}

// addReceiver provisions a receiver whose lab profile launches claude and cloud profile codex, and registers its signing
// key.
func (f *fixture) addReceiver(t *testing.T, id, pool string) *testReceiver {
	t.Helper()
	config := admin.ReceiverConfig{Receiver: id, Pool: pool, Capacity: 2, Profiles: []admin.ProfileSettings{
		{Name: "lab", Plugin: "nomad", Agent: "claude", Labels: map[string]string{"runtime": "podman", "region": "eu"}},
		{Name: "cloud", Plugin: "aws", Agent: "codex", Labels: map[string]string{"runtime": "exec"}},
	}}
	return f.addReceiverConfig(t, config)
}

func (f *fixture) addReceiverConfig(t *testing.T, config admin.ReceiverConfig) *testReceiver {
	t.Helper()
	bundle := filepath.Join(t.TempDir(), "bundle.yaml")
	if err := f.service.ReceiverAdd(f.ctx, config, bundle); err != nil {
		t.Fatal(err)
	}
	kafka, _ := f.connect(t, bundle)
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	receiver := &testReceiver{id: config.Receiver, pool: config.Pool, key: key, kafka: kafka}
	registration, err := signed.RegisterKey(config.Receiver, key)
	if err != nil {
		t.Fatal(err)
	}
	receiver.produce(t, f.ctx, admin.ReceiverTopic(config.Receiver), config.Receiver, registration)
	f.receivers[config.Receiver] = receiver
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
	return wire.Offer{Profile: "lab", Agent: "claude"}
}

// bothOffers makes both of addReceiver's profiles ready.
func bothOffers() []wire.Offer {
	return []wire.Offer{claudeOffer(), {Profile: "cloud", Agent: "codex"}}
}

// advertise publishes a live advertisement; mutate may change it before it is signed.
func (r *testReceiver) advertise(t *testing.T, ctx context.Context, mutate func(*wire.Advertisement)) {
	t.Helper()
	ad := &wire.Advertisement{Receiver: r.id, Pool: r.pool, Offers: []wire.Offer{claudeOffer()}, Capacity: 2, AtMS: time.Now().UnixMilli()}
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
	records, err := readTopic(ctx, r.kafka, admin.PoolTasksTopic(r.pool))
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
	return wire.ReceiverBinding{Receiver: r.id, Tenant: task.submission.Wherry.Tenant, Pool: r.pool, Run: task.submission.Wherry.Run, Attempt: attempt}
}

func (r *testReceiver) echo(t *testing.T, task claimed) []byte {
	binding := task.submission.Wherry
	return r.sign(t, &wire.Echo{Receiver: r.id, Tenant: binding.Tenant, Pool: r.pool, Run: binding.Run, Placement: binding.Placement,
		TaskHash: task.hash, MessageID: task.submission.Message.MessageID})
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

// terminal is a terminal status counting its attempt's transcript chunks.
func (r *testReceiver) terminal(t *testing.T, task claimed, attempt int64, state wire.TaskState, chunks int64) []byte {
	t.Helper()
	return r.status(t, task, attempt, fmt.Sprintf("terminal-%d", attempt), "terminal", state, func(status *wire.RunStatus) { status.Wherry.TranscriptChunks = chunks })
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

// feed writes records under the run's key in one transaction, as a receiver commits them: chunks and outputs to the
// tenant's transcript topic, the others to its feed.
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
		topic := admin.TenantTopic("tenant-a")
		if typ, _ := header(value); typ == wire.ChunkType || typ == wire.OutputType {
			topic = admin.TenantTranscriptTopic("tenant-a")
		}
		if err := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(runID), Value: value}).FirstErr(); err != nil {
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
	if binding := task.submission.Wherry; binding.Tenant != "tenant-a" || binding.Host != "host-a" || binding.Pool != "pool-a" ||
		binding.Placement.Agent != "claude" || !reflect.DeepEqual(binding.Placement.Selector, defaultSelector()) {
		t.Fatalf("submitted binding %#v", binding)
	}
	if message := task.submission.Message; message.TaskID != runID || message.ContextID != runID || message.Parts[0].Text != "do the work" {
		t.Fatalf("submitted message %#v", message)
	}
	local, err := f.host.localRun(runID)
	if err != nil || local == nil || local.Run != runID || local.Pool != "pool-a" || local.TaskHash != task.hash || local.Placement.Agent != "claude" || !reflect.DeepEqual(local.Placement.Selector, defaultSelector()) {
		t.Fatalf("local record %#v, %v", local, err)
	}
	receiver.feed(t, f.ctx, runID, receiver.chunk(t, task, 1, 0), receiver.chunk(t, task, 1, 1))
	receiver.feed(t, f.ctx, runID, receiver.chunk(t, task, 2, 0), receiver.result(t, task, 2, wire.TaskStateCompleted, nil),
		receiver.terminal(t, task, 2, wire.TaskStateCompleted, 1))
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
	if len(events.Statuses) != 2 || events.Statuses[1].Wherry.Kind != "terminal" ||
		events.Chunks[1] != 2 || events.Chunks[2] != 1 || len(events.Chunks) != 2 {
		t.Fatalf("events %#v", events)
	}
	status, err := f.host.Status(f.ctx, runID)
	if err != nil || status.Status.State != wire.TaskStateCompleted {
		t.Fatalf("status %#v, %v", status, err)
	}
	// Submit seals nothing: the host topic holds the host's key alone.
	if records, err := readTopic(f.ctx, receiver.kafka, admin.HostTopic("tenant-a", "host-a")); err != nil || len(records) != 1 {
		t.Fatalf("host topic holds %d records, %v; want the key registration alone", len(records), err)
	}
}

func TestRegisterIsIdempotentAndRefusesAnotherKey(t *testing.T) {
	f := newFixture(t)
	id, err := f.host.Register(f.ctx)
	if err != nil || id != signed.KeyID(f.host.key.Public().(ed25519.PublicKey)) {
		t.Fatalf("second register %q, %v", id, err)
	}
	records, err := readTopic(f.ctx, f.host.kafka, admin.HostTopic("tenant-a", "host-a"))
	if err != nil || len(records) != 1 {
		t.Fatalf("host topic holds %d records, %v; want one registration", len(records), err)
	}
	_, other, _ := ed25519.GenerateKey(rand.Reader)
	impostor, err := NewHost(f.host.kafka, other, admin.TenantPrincipal("tenant-a", "host-a"), f.stateDir, f.host.credentials, f.host.git, f.host.logger)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := impostor.Register(f.ctx); err == nil {
		t.Fatal("registered a second key on the host topic")
	}
}

func TestNewHostParsesPrincipal(t *testing.T) {
	for _, principal := range []string{"wherry.rcv.receiver-a", "wherry.tnt.tenant-a", "wherry.tnt.tenant-a.host.a", "wherry.tnt.Tenant.host-a", "tnt.tenant-a.host-a"} {
		if _, err := NewHost(nil, nil, principal, "", nil, nil, nil); err == nil {
			t.Errorf("accepted principal %q", principal)
		}
	}
	host, err := NewHost(nil, nil, "wherry.tnt.tenant-a.host-a", "", nil, nil, nil)
	if err != nil || host.tenant != "tenant-a" || host.host != "host-a" {
		t.Fatalf("host %#v, %v", host, err)
	}
}

func (f *fixture) tasksEnd(t *testing.T, pool string) int {
	t.Helper()
	records, err := readTopic(f.ctx, f.service.Admin, admin.PoolTasksTopic(pool))
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

func TestSubmitMatchesOneProfilesLabels(t *testing.T) {
	f := newFixture(t)
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.Offers = bothOffers() })
	_, err := f.host.Submit(f.ctx, SubmitInput{Selector: []wire.Requirement{
		{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}}, {Key: "region", Operator: wire.OpIn, Values: []string{"eu"}},
	}, Prompt: "work", Deadline: time.Minute, QueueDeadline: time.Minute})
	if !errors.Is(err, ErrNoMatchingReceiver) {
		t.Fatalf("submit matched labels of two different profiles: %v", err)
	}
}

func TestSubmitMatchesAcrossProfiles(t *testing.T) {
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.Offers = bothOffers() })
	selector := []wire.Requirement{{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}}}
	result, err := f.host.Submit(f.ctx, SubmitInput{Selector: selector, Prompt: "work", Deadline: time.Minute, QueueDeadline: time.Minute})
	if err != nil {
		t.Fatalf("second profile did not match: %v", err)
	}
	if placement := receiver.claim(t, f.ctx, result.RunID)[0].submission.Wherry.Placement; placement.Agent != "codex" || !reflect.DeepEqual(placement.Selector, selector) {
		t.Fatalf("placement %#v, want codex under the submitted selector", placement)
	}
	for name, input := range map[string]SubmitInput{
		"explicit agent the matching profile does not launch": {Agent: "claude", Selector: selector},
		"agent outside the pool's terms":                      {Agent: "gemini"},
	} {
		input.Prompt, input.Deadline, input.QueueDeadline = "work", time.Minute, time.Minute
		if _, err := f.host.Submit(f.ctx, input); !errors.Is(err, ErrNoMatchingReceiver) {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

func TestSubmitRefusesInputsOverSmallestProfileInlineLimit(t *testing.T) {
	f := newFixture(t)
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.Offers[0].InlineLimit = 1000 })
	f.addReceiver(t, "receiver-b", "pool-a").advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.Offers[0].InlineLimit = 5000 })
	_, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Parts: []wire.Part{{Raw: make([]byte, 2000), Filename: "data.bin"}}, Deadline: time.Minute, QueueDeadline: time.Minute})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "profile lab's 1000-byte limit") || !strings.Contains(err.Error(), "pool with storage") {
		t.Fatalf("submit = %v, want invalid input naming profile lab's 1000-byte limit and storage", err)
	}
	if n := f.tasksEnd(t, "pool-a"); n != 0 {
		t.Fatalf("pool tasks hold %d records after the refused input", n)
	}
}

func TestSubmitKeepsInputsWithinProfileInlineLimit(t *testing.T) {
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.Offers[0].InlineLimit = 1000 })
	f.addReceiver(t, "receiver-b", "pool-a").advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.Offers[0].InlineLimit = 5000 })
	result, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Parts: []wire.Part{{Raw: make([]byte, 900), Filename: "data.bin"}}, Deadline: time.Minute, QueueDeadline: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	task := receiver.claim(t, f.ctx, result.RunID)[0].submission
	if len(task.Message.Parts) != 2 || len(task.Message.Parts[1].Raw) != 900 || len(task.Wherry.S3Inputs) != 0 {
		t.Fatalf("submission parts %+v and S3 inputs %+v; want the 900-byte input inline", task.Message.Parts, task.Wherry.S3Inputs)
	}
}

func TestSubmitIgnoresInlineLimitOfOfferOutsideSelector(t *testing.T) {
	f := newFixture(t)
	config := admin.ReceiverConfig{Receiver: "receiver-a", Pool: "pool-a", Capacity: 2, Profiles: []admin.ProfileSettings{
		{Name: "lab", Plugin: "nomad", Agent: "claude", Labels: map[string]string{"runtime": "podman"}},
		{Name: "other", Plugin: "nomad", Agent: "claude", Labels: map[string]string{"runtime": "exec"}},
	}}
	receiver := f.addReceiverConfig(t, config)
	receiver.advertise(t, f.ctx, func(ad *wire.Advertisement) {
		ad.Offers = []wire.Offer{{Profile: "lab", Agent: "claude", InlineLimit: 5000}, {Profile: "other", Agent: "claude", InlineLimit: 100}}
	})
	result, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Parts: []wire.Part{{Raw: make([]byte, 900), Filename: "data.bin"}}, Deadline: time.Minute, QueueDeadline: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	task := receiver.claim(t, f.ctx, result.RunID)[0].submission
	if len(task.Message.Parts) != 2 || len(task.Message.Parts[1].Raw) != 900 {
		t.Fatalf("submission parts %+v; want the 900-byte input inline", task.Message.Parts)
	}
}

// modelPool creates pool-b, whose claude runs haiku or sonnet and defaultModel by default, with an advertising member.
func (f *fixture) modelPool(t *testing.T, defaultModel string) *testReceiver {
	t.Helper()
	terms := testTerms()
	terms.Agents[0].Models, terms.Agents[0].DefaultModel = []string{"haiku", "sonnet"}, defaultModel
	if err := f.service.PoolCreate(f.ctx, "pool-b", terms); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantAdd(f.ctx, "tenant-a", "pool-b"); err != nil {
		t.Fatal(err)
	}
	receiver := f.addReceiver(t, "receiver-b", "pool-b")
	receiver.advertise(t, f.ctx, nil)
	return receiver
}

func TestSubmitModel(t *testing.T) {
	f := newFixture(t)
	receiver := f.modelPool(t, "haiku")
	receivers, err := f.host.Receivers(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if lab := receivers[len(receivers)-1].Profiles[0]; lab.Agent != "claude" || !slices.Equal(lab.Models, []string{"haiku", "sonnet"}) || lab.DefaultModel != "haiku" {
		t.Fatalf("receivers list %+v, want claude's models and default model from the pool terms", lab)
	}
	for model, want := range map[string]string{"": "haiku", "sonnet": "sonnet"} {
		result, err := f.host.Submit(f.ctx, SubmitInput{Model: model, Prompt: "work", Deadline: time.Minute, QueueDeadline: time.Minute})
		if err != nil {
			t.Fatalf("model %q: %v", model, err)
		}
		if placed := receiver.claim(t, f.ctx, result.RunID)[0].submission.Wherry.Placement; placed.Agent != "claude" || placed.Model != want {
			t.Fatalf("model %q placed %#v, want claude running %s", model, placed, want)
		}
		if status, err := f.host.Status(f.ctx, result.RunID); err != nil || status.Model != want {
			t.Fatalf("model %q status %+v, %v; want model %s", model, status, err, want)
		}
	}
	published := f.tasksEnd(t, "pool-b")
	_, err = f.host.Submit(f.ctx, SubmitInput{Model: "opus", Prompt: "work", Deadline: time.Minute, QueueDeadline: time.Minute})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "model opus") || !strings.Contains(err.Error(), "pool-b") {
		t.Fatalf("submit of a model outside the terms = %v, want invalid input naming the model and pool", err)
	}
	if _, err := f.host.Submit(f.ctx, SubmitInput{Agent: "claude", Model: "opus", Prompt: "work", Deadline: time.Minute, QueueDeadline: time.Minute}); !errors.Is(err, ErrInvalidInput) ||
		!strings.Contains(err.Error(), "model opus is not in agent claude's models in pool pool-b") {
		t.Fatalf("submit of a model outside the agent's models = %v, want invalid input naming the model, agent and pool", err)
	}
	if n := f.tasksEnd(t, "pool-b"); n != published {
		t.Fatalf("pool tasks hold %d records after refused models, want %d", n, published)
	}
}

func TestSubmitWithoutAModelForAnAgentRequiringOne(t *testing.T) {
	f := newFixture(t)
	f.modelPool(t, "")
	_, err := f.host.Submit(f.ctx, SubmitInput{Agent: "claude", Prompt: "work", Deadline: time.Minute, QueueDeadline: time.Minute})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "agent claude in pool pool-b requires a model") {
		t.Fatalf("submit without a model = %v, want invalid input saying the agent requires one", err)
	}
	if n := f.tasksEnd(t, "pool-b"); n != 0 {
		t.Fatalf("pool tasks hold %d records after a refused submit", n)
	}
}

func TestResolvePlacementModel(t *testing.T) {
	terms := testTerms()
	terms.DefaultAgent = "codex"
	terms.Agents[0].Models, terms.Agents[0].DefaultModel = []string{"haiku", "sonnet"}, "haiku"
	terms.Agents[1].Models, terms.Agents[1].DefaultModel = []string{"gpt-5-mini"}, "gpt-5-mini"
	state := admin.NewState()
	state.Pools["pool-a"] = terms
	state.Receivers["receiver-a"] = admin.ReceiverConfig{Receiver: "receiver-a", Pool: "pool-a", Profiles: []admin.ProfileSettings{
		{Name: "lab", Plugin: "nomad", Agent: "claude"}, {Name: "cloud", Plugin: "aws", Agent: "codex"},
	}}
	state.Entitlements["tenant-a"] = map[string]bool{"pool-a": true}
	ads := map[string]wire.Advertisement{"receiver-a": {Receiver: "receiver-a", Pool: "pool-a", Capacity: 1, Offers: []wire.Offer{{Profile: "lab"}, {Profile: "cloud"}}}}
	for name, test := range map[string]struct{ agent, model, wantAgent, wantModel string }{
		"default agent's default model":            {"", "", "codex", "gpt-5-mini"},
		"fallback skips the default lacking model": {"", "sonnet", "claude", "sonnet"},
		"explicit agent's default model":           {"claude", "", "claude", "haiku"},
	} {
		_, placement, err := resolvePlacement(state, "tenant-a", test.agent, test.model, nil, ads)
		if err != nil || placement.Agent != test.wantAgent || placement.Model != test.wantModel {
			t.Fatalf("%s: placed %#v, %v; want %s running %s", name, placement, err, test.wantAgent, test.wantModel)
		}
	}
	if _, _, err := resolvePlacement(state, "tenant-a", "", "opus", nil, ads); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("a model no agent holds placed: %v", err)
	}
	// claude, now the default agent, has models and no default model, so a task naming none falls to codex.
	terms.DefaultAgent, terms.Agents[0].DefaultModel = "claude", ""
	state.Pools["pool-a"] = terms
	if _, placement, err := resolvePlacement(state, "tenant-a", "", "", nil, ads); err != nil || placement.Agent != "codex" || placement.Model != "gpt-5-mini" {
		t.Fatalf("fallback placed %#v, %v; want codex past the agent requiring a model", placement, err)
	}
	if _, _, err := resolvePlacement(state, "tenant-a", "claude", "", nil, ads); !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "agent claude in pool pool-a requires a model") {
		t.Fatalf("explicit agent requiring a model without one: %v", err)
	}
	if _, placement, err := resolvePlacement(state, "tenant-a", "claude", "sonnet", nil, ads); err != nil || placement.Model != "sonnet" {
		t.Fatalf("explicit agent requiring a model, given one: %#v, %v", placement, err)
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
	config := admin.ReceiverConfig{Receiver: "receiver-a", Pool: "pool-a", Profiles: []admin.ProfileSettings{
		{Name: "lab", Plugin: "nomad", Agent: "claude", Labels: map[string]string{"runtime": "podman", "region": "eu", "gpu": "a100"}},
		{Name: "batch", Plugin: "nomad", Agent: "gemini", Labels: map[string]string{"runtime": "exec", "region": "us"}},
		{Name: "cloud", Plugin: "aws", Agent: "codex", Labels: map[string]string{"runtime": "exec"}},
		{Name: "spare", Plugin: "nomad", Agent: "codex", Labels: map[string]string{"runtime": "wasm"}},
		{Name: "rogue", Plugin: "nomad", Agent: "other"},
	}}
	state := admin.NewState()
	state.Pools["pool-a"] = terms
	state.Receivers["receiver-a"] = config
	state.Entitlements["tenant-a"] = map[string]bool{"pool-a": true}
	offer := func(profile, agent string) wire.Offer {
		return wire.Offer{Profile: profile, Agent: agent}
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
		"explicit agent no ready profile matches": {"claude", []wire.Requirement{requirement("region", wire.OpIn, "us")}, ready, ""},
		"In":                                  {"", []wire.Requirement{requirement("region", wire.OpIn, "us", "ap")}, ready, "gemini"},
		"NotIn":                               {"", []wire.Requirement{requirement("runtime", wire.OpNotIn, "exec")}, ready, "claude"},
		"NotIn passes an absent key":          {"", []wire.Requirement{requirement("gpu", wire.OpNotIn, "a100")}, ready, "codex"},
		"Exists":                              {"", []wire.Requirement{requirement("gpu", wire.OpExists)}, ready, "claude"},
		"DoesNotExist":                        {"", []wire.Requirement{requirement("region", wire.OpDoesNotExist)}, ready, "codex"},
		"all requirements on one profile":     {"", []wire.Requirement{requirement("runtime", wire.OpIn, "exec"), requirement("gpu", wire.OpExists)}, ready, ""},
		"not-ready profile never chosen":      {"", []wire.Requirement{requirement("runtime", wire.OpIn, "wasm")}, ready, ""},
		"not-ready default falls to declared": {"", nil, ready[:2], "claude"},
		"advertised agent ignored":            {"claude", nil, []wire.Offer{offer("cloud", "claude")}, ""},
		"profile outside the record":          {"", nil, []wire.Offer{offer("edge", "codex")}, ""},
	} {
		t.Run(name, func(t *testing.T) {
			ads := map[string]wire.Advertisement{"receiver-a": {Receiver: "receiver-a", Pool: "pool-a", Offers: test.offers, Capacity: 1}}
			pool, placement, err := resolvePlacement(state, "tenant-a", test.agent, "", test.selector, ads)
			if err == nil && (pool != "pool-a" || !reflect.DeepEqual(placement.Selector, test.selector)) {
				t.Fatalf("pool %q placement %#v, want pool-a under the submitted selector", pool, placement)
			}
			if placement.Agent != test.want {
				t.Fatalf("placed %q, want %q", placement.Agent, test.want)
			}
		})
	}
	if _, _, err := resolvePlacement(state, "tenant-b", "", "", nil, map[string]wire.Advertisement{"receiver-a": {Receiver: "receiver-a", Pool: "pool-a", Offers: ready}}); err == nil {
		t.Fatal("placed a task for a tenant not entitled to the pool")
	}
}

func TestResolvePlacementChoosesMostSpareThenLowestID(t *testing.T) {
	state := admin.NewState()
	state.Pools["pool-a"], state.Pools["pool-b"], state.Pools["pool-c"] = testTerms(), testTerms(), testTerms()
	state.Entitlements["tenant-a"] = map[string]bool{"pool-a": true, "pool-b": true}
	ads := map[string]wire.Advertisement{}
	member := func(id, pool string, capacity, inFlight int64, profile string) {
		state.Receivers[id] = admin.ReceiverConfig{Receiver: id, Pool: pool, Profiles: []admin.ProfileSettings{
			{Name: "lab", Plugin: "nomad", Agent: "claude"}, {Name: "cloud", Plugin: "aws", Agent: "codex"},
		}}
		ads[id] = wire.Advertisement{Receiver: id, Pool: pool, Capacity: capacity, InFlight: inFlight, Offers: []wire.Offer{{Profile: profile, Agent: "x"}}}
	}
	member("receiver-d", "pool-b", 5, 2, "lab")   // spare 3
	member("receiver-c", "pool-a", 5, 2, "lab")   // spare 3, lower ID
	member("receiver-b", "pool-a", 9, 0, "cloud") // most spare, but its ready profile launches codex
	member("receiver-a", "pool-c", 9, 0, "lab")   // most spare, pool not entitled
	member("receiver-e", "pool-a", 4, 2, "lab")   // spare 2
	pool, placement, err := resolvePlacement(state, "tenant-a", "claude", "", nil, ads)
	if err != nil || pool != "pool-a" || placement.Agent != "claude" {
		t.Fatalf("placed %q %#v %v, want pool-a through receiver-c", pool, placement, err)
	}
	ads["receiver-d"] = wire.Advertisement{Receiver: "receiver-d", Pool: "pool-b", Capacity: 5, InFlight: 1, Offers: ads["receiver-d"].Offers}
	if pool, _, _ := resolvePlacement(state, "tenant-a", "claude", "", nil, ads); pool != "pool-b" {
		t.Fatalf("pool %q, want pool-b through receiver-d's larger spare capacity", pool)
	}
}

func TestResolvePlacementPrefersTheDefaultAgentOverSpareCapacity(t *testing.T) {
	state := admin.NewState()
	state.Pools["pool-a"] = testTerms()
	state.Entitlements["tenant-a"] = map[string]bool{"pool-a": true}
	ads := map[string]wire.Advertisement{}
	for _, member := range []struct {
		id, agent string
		spare     int64
	}{{"receiver-a", "codex", 9}, {"receiver-b", "claude", 1}} {
		state.Receivers[member.id] = admin.ReceiverConfig{Receiver: member.id, Pool: "pool-a", Profiles: []admin.ProfileSettings{{Name: member.agent, Plugin: "nomad", Agent: member.agent}}}
		ads[member.id] = wire.Advertisement{Receiver: member.id, Pool: "pool-a", Capacity: member.spare, Offers: []wire.Offer{{Profile: member.agent, Agent: member.agent}}}
	}
	if _, placement, err := resolvePlacement(state, "tenant-a", "", "", nil, ads); err != nil || placement.Agent != "claude" {
		t.Fatalf("placed %#v %v, want the default agent claude through receiver-b despite receiver-a's spare capacity", placement, err)
	}
	if _, placement, err := resolvePlacement(state, "tenant-a", "codex", "", nil, ads); err != nil || placement.Agent != "codex" {
		t.Fatalf("placed %#v %v, want the named agent codex", placement, err)
	}
}

func TestStatusBeforeAndAfterEcho(t *testing.T) {
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	submitted := f.submit(t, defaultSelector())
	status, err := f.host.Status(f.ctx, submitted.RunID)
	if err != nil || status.Status.State != wire.TaskStateSubmitted || status.RunID != submitted.RunID || status.Pool != "pool-a" || status.Updated.IsZero() {
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
			return receiver.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateFailed, nil)
		},
		"status from other pool": func(t *testing.T, f *fixture, _ *testReceiver, task claimed) []byte {
			f.pool(t, "pool-b")
			other := f.addReceiver(t, "receiver-b", "pool-b")
			return other.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateFailed, nil)
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

func TestAwaitReturnsOnFailed(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	receiver.feed(t, f.ctx, task.submission.Wherry.Run, receiver.result(t, task, 1, wire.TaskStateFailed, nil),
		receiver.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateFailed, nil))
	awaited, err := f.host.Await(f.ctx, task.submission.Wherry.Run, 5*time.Second)
	if err != nil || awaited.Task == nil || awaited.Status.State != wire.TaskStateFailed {
		t.Fatalf("await %#v, %v; want FAILED", awaited, err)
	}
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

func TestSubmitRefusesQueueDeadlinePastMaximumBeforePublishing(t *testing.T) {
	f := newFixture(t)
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
	input := SubmitInput{Selector: defaultSelector(), Prompt: "work", Deadline: time.Minute, QueueDeadline: 15 * 24 * time.Hour}
	if _, err := f.host.Submit(f.ctx, input); !errors.Is(err, ErrQueueDeadlinePastMaximum) {
		t.Fatalf("submit = %v, want queue deadline past the maximum", err)
	}
	if n := f.tasksEnd(t, "pool-a"); n != 0 {
		t.Fatalf("pool tasks hold %d records", n)
	}
}

func TestSubmitRefusesAKeyThatIsNotCurrentBeforePublishing(t *testing.T) {
	f := newFixture(t)
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
	_, next, _ := ed25519.GenerateKey(rand.Reader)
	rotation, err := signed.RotateKey(signed.HostOwner("tenant-a", "host-a"), f.host.key, next)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.host.produce(f.ctx, &kgo.Record{Topic: admin.HostTopic("tenant-a", "host-a"), Key: []byte("tenant-a.host-a"), Value: rotation}); err != nil {
		t.Fatal(err)
	}
	_, err = f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Deadline: time.Minute, QueueDeadline: time.Minute})
	if err == nil || !strings.Contains(err.Error(), "host key is not the current key") || !strings.Contains(err.Error(), "wherry init") {
		t.Fatalf("submit = %v, want the host key refused as not current", err)
	}
	if n := f.tasksEnd(t, "pool-a"); n != 0 {
		t.Fatalf("pool tasks hold %d records", n)
	}
	if runs, err := os.ReadDir(filepath.Join(f.stateDir, "runs")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("state holds runs %v, %v", runs, err)
	}
}

// submitQueued submits a run with queueDeadline through receiver-a.
func (f *fixture) submitQueued(t *testing.T, queueDeadline time.Duration) string {
	t.Helper()
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
	result, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Deadline: time.Minute, QueueDeadline: queueDeadline})
	if err != nil {
		t.Fatal(err)
	}
	return result.RunID
}

func TestAwaitAndStatusReportAnUnclaimedRunPastItsQueueDeadline(t *testing.T) {
	f := newFixture(t)
	runID := f.submitQueued(t, 2*time.Second)
	start := time.Now()
	awaited, err := f.host.Await(f.ctx, runID, 20*time.Second)
	elapsed := time.Since(start)
	if err != nil || awaited.Status.State != wire.TaskStateSubmitted || !strings.HasPrefix(awaited.Unclaimed, "queue deadline passed at ") ||
		!strings.HasSuffix(awaited.Unclaimed, " and no receiver claimed the run") {
		t.Fatalf("await %#v, %v; want the passed queue deadline", awaited, err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("await returned after %s, want soon after the 2s queue deadline", elapsed)
	}
	status, err := f.host.Status(f.ctx, runID)
	if err != nil || status.Status.State != wire.TaskStateSubmitted || status.Unclaimed != awaited.Unclaimed {
		t.Fatalf("status %#v, %v; want %q", status, err, awaited.Unclaimed)
	}
}

func TestAwaitKeepsWaitingBeforeTheQueueDeadline(t *testing.T) {
	f := newFixture(t)
	runID := f.submitQueued(t, 10*time.Minute)
	if awaited, err := f.host.Await(f.ctx, runID, 2*time.Second); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("await = %#v, %v; want it still waiting", awaited, err)
	}
	if status, err := f.host.Status(f.ctx, runID); err != nil || status.Unclaimed != "" {
		t.Fatalf("status %#v, %v; want no passed queue deadline", status, err)
	}
}

func TestClaimedRunNeverReportsItsQueueDeadline(t *testing.T) {
	f := newFixture(t)
	runID := f.submitQueued(t, 5*time.Second)
	receiver := f.receivers["receiver-a"]
	task := receiver.claim(t, f.ctx, runID)[0]
	receiver.feed(t, f.ctx, runID, receiver.echo(t, task), receiver.status(t, task, 1, "claimed-1", "claimed", wire.TaskStateWorking, nil))
	if time.Now().UnixMilli() > task.submission.Wherry.QueueDeadlineMS {
		t.Fatal("the claim landed after the queue deadline")
	}
	time.Sleep(time.Until(time.UnixMilli(task.submission.Wherry.QueueDeadlineMS + 1)))
	if awaited, err := f.host.Await(f.ctx, runID, 2*time.Second); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("await = %#v, %v; want it still waiting on the claimed run", awaited, err)
	}
	if status, err := f.host.Status(f.ctx, runID); err != nil || status.Status.State != wire.TaskStateWorking || status.Unclaimed != "" {
		t.Fatalf("status %#v, %v; want WORKING with no passed queue deadline", status, err)
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
	records, err := readTopic(f.ctx, receiver.kafka, admin.PoolEventsTopic("pool-a"))
	if err != nil || len(records) != 1 {
		t.Fatalf("pool events hold %d records, %v", len(records), err)
	}
	cancel, err := admin.AuthorizeCancel(state, keys, records[0].Value, "pool-a", "tenant-a")
	if err != nil || cancel.Run != runID || cancel.Host != "host-b" {
		t.Fatalf("cancel %#v, %v", cancel, err)
	}
}

func TestReceiversListsLiveStaleSilentAndEnded(t *testing.T) {
	f := newFixture(t)
	f.pool(t, "pool-b")
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, func(ad *wire.Advertisement) {
		ad.Capacity, ad.InFlight = 3, 1
		ad.Offers[0].InlineLimit = 49152
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
	if a := got["receiver-a"]; a.Status != "live" || a.Capacity != 3 || a.InFlight != 1 ||
		!reflect.DeepEqual(a.Profiles, []Profile{
			{Name: "lab", Plugin: "nomad", Agent: "claude", InlineLimit: 49152, Credentials: []Credential{{Name: "model", Configured: true}}, Labels: map[string]string{"runtime": "podman", "region": "eu"}, Ready: true},
			{Name: "cloud", Plugin: "aws", Agent: "codex", Credentials: []Credential{{Name: "openai", Configured: true}, {Name: "github-token", Optional: true}}, Labels: map[string]string{"runtime": "exec"}, Ready: false},
		}) {
		t.Fatalf("live receiver %#v", a)
	}
	if b := got["receiver-b"]; b.Status != "stale" || b.Capacity != 4 || b.InFlight != 2 || len(b.Profiles) != 2 || b.Profiles[0].Ready || b.Profiles[1].Ready {
		t.Fatalf("stale receiver %#v; want its profiles listed, none ready", b)
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
	receiver.feed(t, f.ctx, runID,
		receiver.status(t, task, 1, "claimed-1", "claimed", wire.TaskStateWorking, nil),
		receiver.status(t, wrongHash, 1, "started-1", "started", wire.TaskStateWorking, nil),
		other.status(t, task, 1, "started-3", "started", wire.TaskStateWorking, nil),
		receiver.chunk(t, task, 1, 0),
		receiver.chunk(t, wrongHash, 1, 2),
		other.chunk(t, task, 1, 1),
		[]byte("not a record"),
	)
	receiver.feed(t, f.ctx, otherTask.submission.Wherry.Run, receiver.status(t, otherTask, 1, "started-4", "started", wire.TaskStateWorking, nil), []byte("not a record"))
	events, err := f.host.Events(f.ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events.Statuses) != 1 || events.Statuses[0].Wherry.EventID != "claimed-1" || events.Chunks[1] != 1 || len(events.Chunks) != 1 || events.Skipped != 5 {
		t.Fatalf("statuses %d chunks %v skipped %d, want the one claimed status, one chunk and five skipped", len(events.Statuses), events.Chunks, events.Skipped)
	}
	if _, err := f.host.Events(f.ctx, "run-missing"); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("events of an unknown run: %v", err)
	}
}

func TestConfigRejectsUnknownField(t *testing.T) {
	const secret = "sk-live-config-secret"
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("bundle: bundle.yaml\nstray_field: "+secret+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "line 2") || strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "stray_field") {
		t.Fatalf("config with an unknown field: %v, want it refused at its line, quoting nothing from the file", err)
	}
}

func TestDefaultConfigPathIsYAML(t *testing.T) {
	if path, err := DefaultConfigPath(); err != nil || !strings.HasSuffix(path, filepath.Join(".config", "wherry", "config.yaml")) {
		t.Fatalf("default config path %s, %v", path, err)
	}
}

func TestConfigInputRoots(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	for name, test := range map[string]struct{ root, want string }{
		"missing":  {filepath.Join(dir, "absent"), "does not exist or is not a directory"},
		"a file":   {path, "does not exist or is not a directory"},
		"relative": {".", "must be absolute"},
	} {
		if err := os.WriteFile(path, []byte("bundle: bundle.yaml\ninput_roots: ['"+test.root+"']\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Errorf("config with an input root that is %s: %v, want %q", name, err, test.want)
		}
	}
	if err := os.WriteFile(path, []byte("bundle: bundle.yaml\ninput_roots: ['~']\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	if config, err := LoadConfig(path); err != nil || !slices.Equal(config.InputRoots, []string{home}) {
		t.Fatalf("input roots %v, %v", config.InputRoots, err)
	}
}

// TestConfigExampleLoads loads the reference's complete host configuration, which sets every member.
func TestConfigExampleLoads(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	for _, root := range []string{"src", "notes", ".config/wherry/known_hosts"} {
		if err := os.MkdirAll(filepath.Join(home, root), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(home, ".config/wherry/known_hosts/gitlab"), []byte("gitlab.example ssh-ed25519 AAAA\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	example, err := os.ReadFile(filepath.Join("testdata", "example-config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	config, err := LoadConfig(filepath.Join("testdata", "example-config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	members := reflect.ValueOf(config)
	for i := range members.NumField() {
		if members.Field(i).IsZero() {
			t.Errorf("example config leaves %s unset", members.Type().Field(i).Name)
		}
	}
	// host_key and state_dir differ from their defaults, so a member the example drops shows here.
	if config.HostKey != filepath.Join(home, ".config", "wherry", "laptop.key") || config.StateDir != filepath.Join(home, ".local", "state", "wherry") ||
		!slices.Equal(config.InputRoots, []string{filepath.Join(home, "src"), filepath.Join(home, "notes")}) {
		t.Fatalf("host key %s, state dir %s, input roots %v, want each under %s", config.HostKey, config.StateDir, config.InputRoots, home)
	}
	reference, err := os.ReadFile(filepath.Join("..", "..", "docs", "configuration.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(reference), "```yaml\n"+string(example)+"```\n") {
		t.Fatal("docs/configuration.md has no code block holding testdata/example-config.yaml")
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
		if _, err := Connect(config, log.New(io.Discard, "", 0)); err == nil || strings.Contains(err.Error(), password) {
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
		"failed status": func(t *testing.T, receiver *testReceiver, task claimed) []byte {
			return receiver.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateFailed, nil)
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
	rotation, err := signed.RotateKey("receiver-a", receiver.key, next)
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

func TestStatusReadsWhatAReceiverSignedBeforeItRotated(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	retired := receiver.key
	time.Sleep(5 * time.Millisecond)
	_, next, _ := ed25519.GenerateKey(rand.Reader)
	rotation, err := signed.RotateKey("receiver-a", retired, next)
	if err != nil {
		t.Fatal(err)
	}
	receiver.produce(t, f.ctx, admin.ReceiverTopic("receiver-a"), "receiver-a", rotation)
	time.Sleep(5 * time.Millisecond)
	receiver.feed(t, f.ctx, runID, receiver.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateCanceled, nil))
	status, err := f.host.Status(f.ctx, runID)
	if err != nil || status.Status.State != wire.TaskStateWorking || status.Skipped != 1 {
		t.Fatalf("status %#v, %v; want the WORKING signed before the rotation, and the CANCELED signed with the retired key after it skipped", status, err)
	}
}

func TestAwaitEndsOnCanceledTerminal(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	receiver.feed(t, f.ctx, runID, receiver.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateCanceled, nil))
	awaited, err := f.host.Await(f.ctx, runID, 5*time.Second)
	if err != nil || awaited.Status.State != wire.TaskStateCanceled {
		t.Fatalf("await %#v, %v; want CANCELED", awaited, err)
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
		host, err := NewHost(kafka, key, principal, t.TempDir(), nil, nil, log.New(io.Discard, "", 0))
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
		"failed status with a high attempt": func(t *testing.T, ended *testReceiver, task claimed) []byte {
			return ended.status(t, task, 9, "terminal-9", "terminal", wire.TaskStateFailed, nil)
		},
		"echo for another submission": func(t *testing.T, ended *testReceiver, task claimed) []byte {
			task.hash = strings.Repeat("b", 64)
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
	if len(receivers) != 1 || receivers[0].Status == "live" || strings.HasPrefix(receivers[0].HeartbeatAge, "-") || receivers[0].Profiles[0].Ready {
		t.Fatalf("receivers %#v; want a future advertisement neither live, ready nor of negative age", receivers)
	}
}

func TestReceiversListsALiveReceiverWithNoOffers(t *testing.T) {
	f := newFixture(t)
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.Offers = []wire.Offer{} })
	receivers, err := f.host.Receivers(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(receivers) != 1 || receivers[0].Status != "live" || !reflect.DeepEqual(receivers[0].Profiles, []Profile{
		{Name: "lab", Plugin: "nomad", Agent: "claude", Credentials: []Credential{{Name: "model", Configured: true}}, Labels: map[string]string{"runtime": "podman", "region": "eu"}, Ready: false},
		{Name: "cloud", Plugin: "aws", Agent: "codex", Credentials: []Credential{{Name: "openai", Configured: true}, {Name: "github-token", Optional: true}}, Labels: map[string]string{"runtime": "exec"}, Ready: false},
	}) {
		t.Fatalf("receivers %#v; want the live receiver with its configured profiles, none ready", receivers)
	}
}

// claimedAt writes task's echo and a claimed status stamped at, as admission does.
func (r *testReceiver) claimedAt(t *testing.T, ctx context.Context, task claimed, at time.Time) {
	t.Helper()
	r.feed(t, ctx, task.submission.Wherry.Run, r.echo(t, task), r.status(t, task, 1, "claimed-1", "claimed", wire.TaskStateWorking, func(status *wire.RunStatus) {
		status.StatusUpdate.Status.Timestamp = at.UTC().Format(time.RFC3339)
	}))
}

func runIDs(runs []StatusResult) []string {
	ids := make([]string, 0, len(runs))
	for _, run := range runs {
		ids = append(ids, run.RunID)
	}
	return ids
}

func TestRunsListsLocalAndEchoedRuns(t *testing.T) {
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	before := time.Now()
	local := f.submit(t, defaultSelector())
	other := f.newHost(t, "host-b", t.TempDir())
	elsewhere, err := other.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Deadline: time.Minute, QueueDeadline: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	claimedAt := time.Now().Add(-time.Hour).Truncate(time.Second)
	receiver.claimedAt(t, f.ctx, receiver.claim(t, f.ctx, elsewhere.RunID)[0], claimedAt)
	runs, err := f.host.Runs(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := runIDs(runs.Runs), []string{local.RunID, elsewhere.RunID}; !reflect.DeepEqual(got, want) {
		t.Fatalf("runs %v, want this host's unadmitted run then the other host's echoed run: %v", got, want)
	}
	if run := runs.Runs[0]; run.Status.State != wire.TaskStateSubmitted || run.Pool != "pool-a" || run.Updated.Before(before.Truncate(time.Millisecond)) {
		t.Fatalf("unadmitted run %#v, want SUBMITTED at its local submit time", run)
	}
	if run := runs.Runs[1]; run.Status.State != wire.TaskStateWorking || run.Pool != "pool-a" || !run.Updated.Equal(claimedAt) {
		t.Fatalf("echoed run %#v, want WORKING at its status time %v", run, claimedAt)
	}
}

func TestRunsNewestFirst(t *testing.T) {
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	now := time.Now().Truncate(time.Second)
	first, second := f.submit(t, defaultSelector()), f.submit(t, defaultSelector())
	receiver.claimedAt(t, f.ctx, receiver.claim(t, f.ctx, second.RunID)[0], now.Add(-2*time.Hour))
	receiver.claimedAt(t, f.ctx, receiver.claim(t, f.ctx, first.RunID)[0], now.Add(-time.Hour))
	unadmitted := f.submit(t, defaultSelector())
	runs, err := f.host.Runs(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := runIDs(runs.Runs), []string{unadmitted.RunID, first.RunID, second.RunID}; !reflect.DeepEqual(got, want) {
		t.Fatalf("runs %v, want latest update first: %v", got, want)
	}
}

func TestRunsEmpty(t *testing.T) {
	f := newFixture(t)
	runs, err := f.host.Runs(f.ctx)
	if err != nil || len(runs.Runs) != 0 || runs.Skipped != 0 {
		t.Fatalf("runs %#v, %v; want none", runs, err)
	}
}

func TestRunsSkipsUndecodableLocalRecords(t *testing.T) {
	f := newFixture(t)
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
	good := f.submit(t, defaultSelector())
	encoded, err := os.ReadFile(filepath.Join(f.stateDir, "runs", good.RunID+".json"))
	if err != nil {
		t.Fatal(err)
	}
	newer := strings.Replace(string(encoded), "{", `{"from_a_newer_wherry":true,`, 1)
	for name, contents := range map[string]string{"run-corrupt": "{", "run-newer": newer} {
		if err := os.WriteFile(filepath.Join(f.stateDir, "runs", name+".json"), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	runs, err := f.host.Runs(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := runIDs(runs.Runs); !reflect.DeepEqual(got, []string{good.RunID}) || runs.Skipped != 2 {
		t.Fatalf("runs %v skipped %d, want the good run and both undecodable records skipped", got, runs.Skipped)
	}
	if _, err := f.host.Status(f.ctx, "run-corrupt"); err == nil || errors.Is(err, ErrRunNotFound) {
		t.Fatalf("status of an undecodable local record: %v, want the decode error", err)
	}
}

func TestRunsDropsUnauthorisedRuns(t *testing.T) {
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	other := f.newHost(t, "host-b", t.TempDir())
	submit := func() string {
		t.Helper()
		submitted, err := other.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Deadline: time.Minute, QueueDeadline: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		return submitted.RunID
	}
	impostor := *receiver
	_, impostor.key, _ = ed25519.GenerateKey(rand.Reader)
	forged := submit()
	task := receiver.claim(t, f.ctx, forged)[0]
	impostor.feed(t, f.ctx, forged, impostor.echo(t, task), impostor.status(t, task, 1, "claimed-1", "claimed", wire.TaskStateWorking, nil))
	if runs, err := f.host.Runs(f.ctx); err != nil || len(runs.Runs) != 0 {
		t.Fatalf("runs %#v, %v; want none from an impostor's echo", runs, err)
	}
	ended := submit()
	receiver.feed(t, f.ctx, ended, receiver.echo(t, receiver.claim(t, f.ctx, ended)[0]))
	if runs, err := f.host.Runs(f.ctx); err != nil || !reflect.DeepEqual(runIDs(runs.Runs), []string{ended}) {
		t.Fatalf("runs %#v, %v; want the echoed run while its receiver is a member", runs, err)
	}
	f.end(t, "receiver-a")
	if runs, err := f.host.Runs(f.ctx); err != nil || len(runs.Runs) != 0 {
		t.Fatalf("runs %#v, %v; want none once the echoing receiver has ended", runs, err)
	}
}

func TestRunsUpdatedUsesSubmissionAndEchoTimes(t *testing.T) {
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	local := f.submit(t, defaultSelector())
	path := filepath.Join(f.stateDir, "runs", local.RunID+".json")
	var record map[string]any
	if err := readState(path, &record); err != nil {
		t.Fatal(err)
	}
	modified := time.Now().Add(-3 * time.Hour).Truncate(time.Millisecond)
	record["submitted_at_ms"] = modified.UnixMilli()
	if err := writeState(path, record); err != nil {
		t.Fatal(err)
	}
	other := f.newHost(t, "host-b", t.TempDir())
	echoed, err := other.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Deadline: time.Minute, QueueDeadline: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	before := time.Now().Add(-time.Second)
	receiver.feed(t, f.ctx, echoed.RunID, receiver.echo(t, receiver.claim(t, f.ctx, echoed.RunID)[0]))
	runs, err := f.host.Runs(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := runIDs(runs.Runs); !reflect.DeepEqual(got, []string{echoed.RunID, local.RunID}) {
		t.Fatalf("runs %v, want the echoed run then the older local one", got)
	}
	if run := runs.Runs[0]; run.Updated.IsZero() || run.Updated.Before(before) {
		t.Fatalf("echo-only run updated %v, want its echo's timestamp", run.Updated)
	}
	if run := runs.Runs[1]; !run.Updated.Equal(modified) {
		t.Fatalf("local run updated %v, want its submission time %v", run.Updated, modified)
	}
}

func TestSSHRemote(t *testing.T) {
	for remote, want := range map[string]string{
		"git@github.com:org/repo.git":             "ssh://git@github.com/org/repo.git",
		"github.com:org/repo.git":                 "ssh://github.com/org/repo.git",
		"git@git.example.test:/srv/repo.git":      "ssh://git@git.example.test/srv/repo.git",
		"https://github.com/org/repo.git":         "https://github.com/org/repo.git",
		"ssh://git@github.com:22/org/repo.git":    "ssh://git@github.com:22/org/repo.git",
		"https://user:s3cr3t@github.com/repo.git": "https://user:s3cr3t@github.com/repo.git",
		"./repo": "./repo",
	} {
		if got := sshRemote(remote); got != want {
			t.Errorf("sshRemote(%q) = %q, want %q", remote, got, want)
		}
	}
}

func TestSubmitRefusesADeadlinePastTheRunCap(t *testing.T) {
	f := newFixture(t)
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
	_, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "do the work", Deadline: 6*time.Hour + 11*time.Minute, QueueDeadline: 10 * time.Minute})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "6h run cap") {
		t.Fatalf("submit past the cap: %v; want it refused naming the cap", err)
	}
	if _, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "do the work", Deadline: 6*time.Hour + 10*time.Minute, QueueDeadline: 10 * time.Minute}); err != nil {
		t.Fatalf("submit at the cap: %v", err)
	}
}

// Every record about a run is written after the run's submission, so a read for the run starts at the tenant feed's
// end as the host published the submission; a record below that offset is not read.
func TestRunHistoryStartsAtTheFeedOffsetOfItsSubmission(t *testing.T) {
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	if err := receiver.kafka.ProduceSync(f.ctx, &kgo.Record{Topic: admin.TenantTopic("tenant-a"), Key: []byte("run-earlier"), Value: []byte("unreadable")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	submitted := f.submit(t, defaultSelector())
	local, err := f.host.localRun(submitted.RunID)
	if err != nil || local.FeedOffset != 1 {
		t.Fatalf("local record %#v, %v; want the feed offset after the earlier record", local, err)
	}
	// A record keyed by the run below the recorded offset, which a whole-feed read would count as unreadable.
	if err := receiver.kafka.ProduceSync(f.ctx, &kgo.Record{Topic: admin.TenantTopic("tenant-a"), Key: []byte(submitted.RunID), Value: []byte("unreadable")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	local.FeedOffset = 2
	if err := f.host.writeRun(*local); err != nil {
		t.Fatal(err)
	}
	task := receiver.claim(t, f.ctx, submitted.RunID)[0]
	receiver.feed(t, f.ctx, submitted.RunID, receiver.echo(t, task), receiver.status(t, task, 1, "claimed-1", "claimed", wire.TaskStateWorking, nil))
	r, err := f.host.history(f.ctx, submitted.RunID)
	if err != nil || r.status().Status.State != wire.TaskStateWorking || r.skipped != 0 {
		t.Fatalf("history %v; want WORKING with nothing skipped, the earlier record unread", err)
	}
}

// A host opens a few connections for wherry status and wherry submit with three receivers on one broker, where a client
// per topic read opened 17 and 21.
func TestStatusAndSubmitShareConnections(t *testing.T) {
	f := newFixture(t, kfake.NumBrokers(1))
	for _, id := range []string{"receiver-a", "receiver-b", "receiver-c"} {
		f.addReceiver(t, id, "pool-a").advertise(t, f.ctx, nil)
	}
	_, task := f.admitted(t)
	for _, c := range []struct {
		name      string
		limit     int64
		operation func(*Host) error
	}{
		{"status", 6, func(h *Host) error {
			_, err := h.Status(f.ctx, task.submission.Wherry.Run)
			return err
		}},
		{"submit", 7, func(h *Host) error {
			_, err := h.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "do the work", Deadline: 30 * time.Minute, QueueDeadline: 10 * time.Minute})
			return err
		}},
	} {
		var dials atomic.Int64
		var dialer net.Dialer
		kafka, err := kgo.NewClient(append(f.host.kafka.Opts(), kgo.Dialer(func(ctx context.Context, network, host string) (net.Conn, error) {
			dials.Add(1)
			return dialer.DialContext(ctx, network, host)
		}))...)
		if err != nil {
			t.Fatal(err)
		}
		h, err := NewHost(kafka, f.host.key, admin.TenantPrincipal("tenant-a", "host-a"), f.stateDir, f.host.credentials, f.host.git, f.host.logger)
		if err != nil {
			t.Fatal(err)
		}
		err = c.operation(h)
		h.Close()
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if n := dials.Load(); n > c.limit {
			t.Errorf("%s opened %d connections; want at most %d", c.name, n, c.limit)
		}
	}
}

// A run's history is unchanged by records keyed by other runs.
func TestHistorySkipsOtherRunsRecords(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	run := task.submission.Wherry.Run
	receiver.feed(t, f.ctx, run, receiver.chunk(t, task, 1, 0), receiver.chunk(t, task, 1, 5))
	read := func() (StatusResult, TranscriptResult) {
		t.Helper()
		status, err := f.host.Status(f.ctx, run)
		if err != nil {
			t.Fatal(err)
		}
		transcript, err := f.host.Transcript(f.ctx, run)
		if err != nil {
			t.Fatal(err)
		}
		return status, transcript
	}
	status, transcript := read()
	if status.Status.State != wire.TaskStateWorking || len(transcript.Lines) != 2 {
		t.Fatalf("status %s, %d transcript lines", status.Status.State, len(transcript.Lines))
	}
	for i := range 10 {
		other := task
		other.submission.Wherry.Run = fmt.Sprintf("run-other-%d", i)
		values := make([][]byte, 100)
		for j := range values {
			values[j] = receiver.status(t, other, 1, fmt.Sprintf("claimed-%d", j), "claimed", wire.TaskStateWorking, nil)
		}
		receiver.feed(t, f.ctx, other.submission.Wherry.Run, values...)
	}
	afterStatus, afterTranscript := read()
	if !reflect.DeepEqual(afterStatus, status) || !reflect.DeepEqual(afterTranscript, transcript) {
		t.Fatalf("after 1,000 records of other runs: status %+v, transcript %+v; want %+v, %+v", afterStatus, afterTranscript, status, transcript)
	}
}

// A failed read of one receiver's key topic fails the whole load, so nothing is placed on the keys the other reads
// returned.
func TestFailedKeyReadFailsTheLoad(t *testing.T) {
	f := newFixture(t)
	for _, id := range []string{"receiver-a", "receiver-b", "receiver-c"} {
		f.addReceiver(t, id, "pool-a").advertise(t, f.ctx, nil)
	}
	deleted, err := kadm.NewClient(f.service.Admin).DeleteTopics(f.ctx, admin.ReceiverTopic("receiver-b"))
	if err == nil {
		err = deleted.Error()
	}
	if err != nil {
		t.Fatal(err)
	}
	tasks := f.tasksEnd(t, "pool-a")
	if _, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "do the work", Deadline: 30 * time.Minute, QueueDeadline: 10 * time.Minute}); err == nil {
		t.Fatal("submitted over a key load missing a receiver's keys")
	}
	if end := f.tasksEnd(t, "pool-a"); end != tasks {
		t.Fatalf("task topic end %d; want %d", end, tasks)
	}
	if receivers, err := f.host.Receivers(f.ctx); err == nil {
		t.Fatalf("listed %d receivers over a key load missing a receiver's keys", len(receivers))
	}
}

// traffic counts the connections that clients built from a client's options open, the requests of each key they
// send and the records, transaction markers included, they fetch from each topic.
type traffic struct {
	connections atomic.Int64
	mu          sync.Mutex
	keys        map[int16]int64
	fetched     map[string]int64
}

func (c *traffic) OnFetchBatchRead(_ kgo.BrokerMetadata, topic string, _ int32, metrics kgo.FetchBatchMetrics) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fetched == nil {
		c.fetched = map[string]int64{}
	}
	c.fetched[topic] += int64(metrics.NumRecords)
}

// records returns the records fetched from topic.
func (c *traffic) records(topic string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fetched[topic]
}

func (c *traffic) OnBrokerConnect(_ kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	if err == nil {
		c.connections.Add(1)
	}
}

func (c *traffic) OnBrokerE2E(_ kgo.BrokerMetadata, key int16, _ kgo.BrokerE2E) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.keys == nil {
		c.keys = map[int16]int64{}
	}
	c.keys[key]++
}

// counts are a scenario's connections, requests, offset listings and fetches, and its requests by key.
type counts struct {
	connections, requests, listings, fetches int64
	keys                                     map[int16]int64
}

func (c *traffic) counts() counts {
	c.mu.Lock()
	defer c.mu.Unlock()
	var requests int64
	for _, n := range c.keys {
		requests += n
	}
	return counts{c.connections.Load(), requests, c.keys[int16(kmsg.ListOffsets)], c.keys[int16(kmsg.Fetch)], maps.Clone(c.keys)}
}

// countedHost is a host connected as the principal of the bundle at path through a client counted by counted.
func (f *fixture) countedHost(t *testing.T, bundle, stateDir string, counted *traffic) *Host {
	t.Helper()
	kafka, principal := f.connect(t, bundle, kgo.WithHooks(counted))
	host, err := NewHost(kafka, f.host.key, principal, stateDir, nil, nil, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(host.Close)
	return host
}

// bound fails t when got lists offsets other than want's number of times or exceeds want in another count; a zero
// requests or fetches bound is none, for counts the follower's polling sets.
func bound(t *testing.T, name string, got, want counts) {
	t.Helper()
	t.Logf("%s: %+v", name, got)
	if got.listings != want.listings || got.connections > want.connections || want.requests > 0 && got.requests > want.requests || want.fetches > 0 && got.fetches > want.fetches {
		t.Fatalf("%s: %+v, want %d listings and at most %+v", name, got, want.listings, want)
	}
}

// A host that only reads dials one client: a seed connection, then a request and a fetch connection to the one broker.
// Each read lists start and committed offsets once; the tenant feed's committed end is listed apart only to wait for
// a follower.
func TestHostReadsBoundTheirConnectionsAndRequests(t *testing.T) {
	f := newFixture(t, kfake.NumBrokers(1))
	_, task := f.admitted(t)
	bundle := filepath.Join(t.TempDir(), "bundle.yaml")
	if err := f.service.HostAdd(f.ctx, "tenant-a", "host-c", bundle); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name string
		want counts
		read func(*Host) error
	}{
		{"receivers", counts{3, 31, 6, 4, nil}, func(h *Host) error { _, err := h.Receivers(f.ctx); return err }},
		{"status", counts{3, 31, 6, 4, nil}, func(h *Host) error { _, err := h.Status(f.ctx, task.submission.Wherry.Run); return err }},
	} {
		t.Run(c.name, func(t *testing.T) {
			var counted traffic
			if err := c.read(f.countedHost(t, bundle, t.TempDir(), &counted)); err != nil {
				t.Fatal(err)
			}
			bound(t, c.name, counted.counts(), c.want)
		})
	}
}

// Await of a finished run reads it through the reader and the follower alone; the host's own client dials nothing. The
// follower's consuming client opens a seed and a fetch connection, and one of either client's may reopen. Reading the
// outputs lists the empty transcript topic's end, its offset after the echo's time twice, and its start and committed
// offsets.
func TestAwaitOfAFinishedRunBoundsItsConnectionsAndRequests(t *testing.T) {
	f := newFixture(t, kfake.NumBrokers(1))
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	receiver.feed(t, f.ctx, runID, receiver.result(t, task, 1, wire.TaskStateCompleted, nil), receiver.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateCompleted, nil))
	bundle := filepath.Join(t.TempDir(), "bundle.yaml")
	if err := f.service.HostAdd(f.ctx, "tenant-a", "host-c", bundle); err != nil {
		t.Fatal(err)
	}
	var counted traffic
	awaited, err := f.countedHost(t, bundle, f.host.stateDir, &counted).Await(f.ctx, runID, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if awaited.Status.State != wire.TaskStateCompleted {
		t.Fatalf("awaited %s, want COMPLETED", awaited.Status.State)
	}
	bound(t, "await", counted.counts(), counts{6, 0, 16, 0, nil})
}

// readTopic reads topic from its start through a reader of its own.
func readTopic(ctx context.Context, client *kgo.Client, topic string) ([]*kgo.Record, error) {
	reader := admin.NewReader(client)
	defer reader.Close()
	return reader.ReadFrom(ctx, topic, 0)
}
