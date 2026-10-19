package receiver

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
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

const (
	testToken      = "sk-ant-oat01-test-credential-never-logged"
	credentialName = "claude-oauth-token"
	testPool       = "pool-a"
	testTenant     = "tenant-a"
	testHost       = "host-a"
	testReceiver   = "receiver-a"
)

var (
	resultStream   = []string{`{"type":"assistant","message":"working"}`, `{"type":"result","result":"done","is_error":false}`}
	noResultStream = []string{`{"type":"assistant","message":"working"}`}
)

type fakeAdapter struct {
	mu          sync.Mutex
	offers      []map[string]string
	offersErr   error
	stream      []string
	stops       int
	stopErr     error
	stopCheck   func() error
	followErr   error
	follows     int
	dispatches  int
	dispatchErr error
	lastRun     Run
	lookups     int
	lookupErr   error
	existing    bool
	commit      string
	releases    int
	releaseErr  error
	releaseIDs  []string
	sweeps      int
	sweepErr    error
	// endDelay holds a stream without a result line open before it ends.
	endDelay time.Duration
}

func (f *fakeAdapter) Platform() string { return "nomad" }

func (f *fakeAdapter) Offers(context.Context) ([]map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]map[string]string(nil), f.offers...), f.offersErr
}

func (f *fakeAdapter) Dispatch(_ context.Context, run Run) (Handle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dispatches++
	f.lastRun = run
	if f.dispatchErr != nil {
		return Handle{}, f.dispatchErr
	}
	handle := Handle{Job: fmt.Sprintf("agent-%s-t%d", run.ID, run.Turn), Alloc: run.ID}
	if run.Git != nil {
		handle.Commit = f.commit
	}
	return handle, nil
}

func (f *fakeAdapter) Lookup(_ context.Context, runID string, turn int64) (Handle, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lookups++
	if f.lookupErr != nil {
		return Handle{}, false, f.lookupErr
	}
	if !f.existing {
		return Handle{}, false, nil
	}
	return Handle{Job: fmt.Sprintf("agent-%s-t%d", runID, turn), Alloc: runID}, true, nil
}

// Follow emits the stream; a stream with a result keeps running like a live task, one without ends.
func (f *fakeAdapter) Follow(ctx context.Context, _ Handle, _ int64, lines chan<- Line) error {
	f.mu.Lock()
	f.follows++
	stream, followErr, endDelay := append([]string(nil), f.stream...), f.followErr, f.endDelay
	f.mu.Unlock()
	var offset int64
	for _, data := range stream {
		select {
		case lines <- Line{Offset: offset, Next: offset + int64(len(data)+1), Data: []byte(data)}:
		case <-ctx.Done():
			return ctx.Err()
		}
		offset += int64(len(data) + 1)
	}
	if followErr != nil {
		return followErr
	}
	if !slices.ContainsFunc(stream, func(data string) bool { _, ok := resultLine([]byte(data)); return ok }) {
		select {
		case <-time.After(endDelay):
		case <-ctx.Done():
		}
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}

func (f *fakeAdapter) Stop(context.Context, Handle) error {
	f.mu.Lock()
	f.stops++
	err, check := f.stopErr, f.stopCheck
	f.mu.Unlock()
	if check != nil {
		return check()
	}
	return err
}

func (f *fakeAdapter) Release(_ context.Context, runID string, _ int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releases++
	f.releaseIDs = append(f.releaseIDs, runID)
	return f.releaseErr
}

func (f *fakeAdapter) Sweep(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sweeps++
	return f.sweepErr
}

func (f *fakeAdapter) sweepCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sweeps
}

func (f *fakeAdapter) counts() (dispatches, lookups, follows, releases int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dispatches, f.lookups, f.follows, f.releases
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

// fixture is a receiver provisioned through admin.Service on kfake with SASL and ACLs, its keys
// registered and its configuration read, and a host of the pool's one entitled tenant.
type fixture struct {
	ctx      context.Context
	cluster  *kfake.Cluster
	service  *admin.Service
	host     ed25519.PrivateKey
	receiver *Receiver
	adapter  *fakeAdapter
	logs     *syncBuffer
	// adapters are the fakes the receiver's factory builds, by adapter name.
	adapters map[string]*fakeAdapter
}

func testTerms() admin.Terms {
	return admin.Terms{
		Agents:   []admin.AgentTerms{{Name: "claude", Credentials: []string{credentialName, "cred-lab", "cred-cloud"}}, {Name: "codex", Credentials: []string{}}},
		Runtimes: []string{"exec", "podman", "lambda"}, DefaultAgent: "claude", DefaultRuntime: "exec",
		SealLifetimeMS: time.Hour.Milliseconds(), DeliveryLimit: 10, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50,
	}
}

func testConfig(receiver string) admin.ReceiverConfig {
	return admin.ReceiverConfig{
		Receiver: receiver, Pool: testPool, Capacity: 1, Labels: map[string]string{}, Runtimes: []string{"exec", "podman"},
		Adapters: []admin.AdapterSettings{{Name: "nomad", Type: "fake", Agent: "claude", Credential: credentialName, Config: map[string]any{}}},
	}
}

func shortIntervals(t *testing.T) {
	for _, interval := range []*time.Duration{&renewInterval, &advertiseInterval, &refreshInterval, &cancelReadInterval} {
		previous := *interval
		*interval = 200 * time.Millisecond
		t.Cleanup(func() { *interval = previous })
	}
}

func newService(t *testing.T, ctx context.Context, terms admin.Terms) (*kfake.Cluster, *admin.Service) {
	t.Helper()
	cluster := kfake.MustCluster(kfake.EnableSASL(), kfake.EnableACLs(), kfake.Superuser("SCRAM-SHA-512", "admin", "password"), kfake.Superuser("PLAIN", "wherry.operator", "operator-password"))
	t.Cleanup(cluster.Close)
	root, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.SASL(scram.Auth{User: "admin", Pass: "password"}.AsSha512Mechanism()), kgo.FetchMaxWait(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(root.Close)
	operator, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.SASL(plain.Auth{User: "wherry.operator", Pass: "operator-password"}.AsMechanism()), kgo.FetchMaxWait(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	service := &admin.Service{Admin: root, Operator: operator, Broker: cluster.ListenAddrs()[0], DeliveryCeiling: 10}
	if err := service.PoolCreate(ctx, testPool, terms); err != nil {
		t.Fatal(err)
	}
	if err := service.TenantAdd(ctx, testTenant, testPool); err != nil {
		t.Fatal(err)
	}
	if err := service.HostAdd(ctx, testTenant, testHost, filepath.Join(t.TempDir(), "host.yaml")); err != nil {
		t.Fatal(err)
	}
	return cluster, service
}

func newFixture(t *testing.T, stream []string) fixture {
	return newFixtureWith(t, stream, testTerms(), testConfig(testReceiver), map[string]*fakeAdapter{
		"nomad": {stream: stream, offers: []map[string]string{
			{"runtime": "exec", "nomad.node_pool": "OCPv", "zone": "dc1"},
			{"runtime": "podman", "nomad.node_pool": "default", "zone": "dc1"},
		}},
	})
}

func newFixtureWith(t *testing.T, stream []string, terms admin.Terms, config admin.ReceiverConfig, adapters map[string]*fakeAdapter) fixture {
	t.Helper()
	shortIntervals(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	cluster, service := newService(t, ctx, terms)
	_, host, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	f := fixture{ctx: ctx, cluster: cluster, service: service, host: host, adapters: adapters, adapter: adapters["nomad"]}
	f.produce(t, admin.HostTopic(testTenant, testHost), "", must(t)(signed.RegisterKey(signed.HostOwner(testTenant, testHost), host, nil)))
	f.receiver, f.logs = f.newReceiver(t, config)
	r := f.receiver
	if err := r.register(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.configure(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.advertise(ctx); err != nil {
		t.Fatal(err)
	}
	return f
}

func must(t *testing.T) func([]byte, error) []byte {
	return func(value []byte, err error) []byte {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
}

// newReceiver provisions config with admin.ReceiverAdd and builds its receiver from the bundle's
// principal and password over kfake's plaintext listener.
func (f fixture) newReceiver(t *testing.T, config admin.ReceiverConfig) (*Receiver, *syncBuffer) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bundle.yaml")
	if err := f.service.ReceiverAdd(f.ctx, config, nil, path); err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var bundle admin.Bundle
	if err := yaml.Unmarshal(encoded, &bundle); err != nil {
		t.Fatal(err)
	}
	// kfake's server-first message always names 4096 iterations, whatever the credential was stored with.
	upsert := kadm.UpsertSCRAM{User: bundle.Principal, Mechanism: kadm.ScramSha512, Iterations: 4096, Password: bundle.Password}
	if altered, err := kadm.NewClient(f.service.Admin).AlterUserSCRAMs(f.ctx, nil, []kadm.UpsertSCRAM{upsert}); err != nil || altered.Error() != nil {
		t.Fatalf("re-salt SCRAM credential: %v %v", err, altered.Error())
	}
	opts := []kgo.Opt{kgo.SeedBrokers(f.cluster.ListenAddrs()...), kgo.SASL(scram.Auth{User: bundle.Principal, Pass: bundle.Password}.AsSha512Mechanism()), kgo.FetchMaxWait(10 * time.Millisecond)}
	base, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(base.Close)
	_, signing, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sealing, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	factory := func(settings admin.AdapterSettings) (Named, error) {
		adapter, ok := f.adapters[settings.Name]
		if !ok || settings.Type != "fake" {
			return Named{}, errors.New("no fake adapter")
		}
		return Named{Name: settings.Name, Agent: settings.Agent, Credential: settings.Credential, Adapter: adapter}, nil
	}
	return &Receiver{id: config.Receiver, kafkaOpt: opts, base: base, signing: signing, sealing: sealing, factory: factory, logger: log.New(logs, "", 0)}, logs
}

func (f fixture) produce(t *testing.T, topic, key string, value []byte) *kgo.Record {
	t.Helper()
	record := &kgo.Record{Topic: topic, Key: []byte(key), Value: value}
	if err := f.service.Admin.ProduceSync(f.ctx, record).FirstErr(); err != nil {
		t.Fatal(err)
	}
	return record
}

// testSubmission is a first turn for run as a host builds it.
func testSubmission(run string) *wire.Submission {
	return &wire.Submission{
		Message: wire.Message{MessageID: "msg-a", ContextID: run, TaskID: run, Role: wire.RoleUser, Parts: []wire.Part{{Text: "hi"}}, Extensions: []string{wire.ExtensionURI}},
		Wherry: wire.SubmissionBinding{
			Tenant: testTenant, Host: testHost, Pool: testPool, Run: run, Turn: 1,
			Placement:  wire.Placement{Agent: "claude", Runtime: "exec", Labels: map[string]string{}},
			DeadlineMS: time.Now().Add(time.Minute).UnixMilli(), QueueDeadlineMS: time.Now().Add(time.Minute).UnixMilli(),
		},
	}
}

// submit signs a submission with key and puts it on the pool's task topic, where every delivered record is.
func (f fixture) submit(t *testing.T, key ed25519.PrivateKey, mutate func(*wire.Submission)) *kgo.Record {
	t.Helper()
	submission := testSubmission("run-a")
	if mutate != nil {
		mutate(submission)
	}
	return f.produce(t, admin.PoolTasksTopic(testPool), submission.Wherry.Run, must(t)(wire.SignRecord(submission, key)))
}

func (f fixture) turn(t *testing.T, run string, turn int64) *kgo.Record {
	t.Helper()
	return f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Run, task.Wherry.Turn = run, turn
		task.Message.MessageID, task.Message.TaskID, task.Message.ContextID = fmt.Sprintf("msg-%d", turn), run, run
	})
}

func (f fixture) parse(t *testing.T, record *kgo.Record) *wire.Submission {
	t.Helper()
	var submission wire.Submission
	if err := wire.ParseRecord(record.Value, signed.Keys{signed.HostOwner(testTenant, testHost): {Current: f.host.Public().(ed25519.PublicKey)}}, &submission); err != nil {
		t.Fatal(err)
	}
	return &submission
}

// publishStatus signs a status for the submission in record with key, as the receiver builds it.
func (f fixture) publishStatus(t *testing.T, record *kgo.Record, receiver string, key ed25519.PrivateKey, kind string, state wire.TaskState, mutate func(*wire.RunStatus)) {
	t.Helper()
	status := f.receiver.status(f.parse(t, record), taskHash(record.Value), 1, kind, taskStatus(state, ""), nil)
	status.Wherry.Receiver = receiver
	if mutate != nil {
		mutate(status)
	}
	f.produce(t, admin.PoolEventsTopic(testPool), string(record.Key), must(t)(wire.SignRecord(status, key)))
}

// publishCancel puts a cancel for run signed by key on the pool's events and refreshes the receiver's cancelled set.
func (f fixture) publishCancel(t *testing.T, run string, turn int64, key ed25519.PrivateKey, mutate func(*wire.Cancel)) {
	t.Helper()
	cancel := &wire.Cancel{Tenant: testTenant, Host: testHost, Pool: testPool, Run: run, Turn: turn}
	if mutate != nil {
		mutate(cancel)
	}
	f.produce(t, admin.PoolEventsTopic(testPool), run, must(t)(wire.SignRecord(cancel, key)))
	if err := f.receiver.readCancels(f.ctx); err != nil {
		t.Fatal(err)
	}
}

// seal puts a credential sealed to receiver's sealing key on the host's topic.
func (f fixture) seal(t *testing.T, receiver string, key *ecdh.PublicKey, credential, value string, issued time.Time, lifetime time.Duration) {
	t.Helper()
	seal, err := signed.SealCredential(testTenant, receiver, testPool, credential, issued, issued.Add(lifetime), key, []byte(value))
	if err != nil {
		t.Fatal(err)
	}
	f.produce(t, admin.HostTopic(testTenant, testHost), "", must(t)(signed.EncodeSeal(seal)))
}

func (f fixture) sealOwn(t *testing.T, credential string, lifetime time.Duration) {
	t.Helper()
	f.seal(t, testReceiver, f.receiver.sealing.PublicKey(), credential, testToken, time.Now(), lifetime)
}

// addPeer provisions another member of the pool and registers its keys.
func (f fixture) addPeer(t *testing.T, id, pool string) (ed25519.PrivateKey, *ecdh.PrivateKey) {
	t.Helper()
	config := testConfig(id)
	config.Pool = pool
	if err := f.service.ReceiverAdd(f.ctx, config, nil, filepath.Join(t.TempDir(), "bundle.yaml")); err != nil {
		t.Fatal(err)
	}
	_, signing, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sealing, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	f.produce(t, admin.ReceiverTopic(id), id, must(t)(signed.RegisterKey(id, signing, sealing.PublicKey())))
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	return signing, sealing
}

func (f fixture) records(t *testing.T, topic string) []*kgo.Record {
	t.Helper()
	records, err := admin.ReadTopic(f.ctx, f.service.Admin, topic)
	if err != nil {
		t.Fatal(err)
	}
	return records
}

func (f fixture) keys() signed.Keys {
	return signed.Keys{testReceiver: {Current: f.receiver.signing.Public().(ed25519.PublicKey)}}
}

// statuses returns this receiver's statuses on topic in log order.
func (f fixture) statuses(t *testing.T, topic string) []wire.RunStatus {
	t.Helper()
	var statuses []wire.RunStatus
	for _, record := range f.records(t, topic) {
		var status wire.RunStatus
		if wire.ParseRecord(record.Value, f.keys(), &status) == nil {
			statuses = append(statuses, status)
		}
	}
	return statuses
}

func (f fixture) feedStatuses(t *testing.T) []wire.RunStatus {
	return f.statuses(t, admin.TenantTopic(testTenant))
}

func (f fixture) eventStatuses(t *testing.T) []wire.RunStatus {
	return f.statuses(t, admin.PoolEventsTopic(testPool))
}

func (f fixture) results(t *testing.T) []wire.Result {
	t.Helper()
	var results []wire.Result
	for _, record := range f.records(t, admin.TenantTopic(testTenant)) {
		var result wire.Result
		if wire.ParseRecord(record.Value, f.keys(), &result) == nil {
			results = append(results, result)
		}
	}
	return results
}

func (f fixture) chunks(t *testing.T) []wire.Chunk {
	t.Helper()
	var chunks []wire.Chunk
	for _, record := range f.records(t, admin.TenantTopic(testTenant)) {
		var chunk wire.Chunk
		if wire.ParseRecord(record.Value, f.keys(), &chunk) == nil {
			chunks = append(chunks, chunk)
		}
	}
	return chunks
}

func (f fixture) lastTask(t *testing.T) *wire.Task {
	t.Helper()
	results := f.results(t)
	if len(results) == 0 {
		t.Fatal("no results on the feed")
	}
	return &results[len(results)-1].Task
}

func decodeObject(t *testing.T, value []byte) map[string]any {
	t.Helper()
	var object map[string]any
	if err := json.Unmarshal(value, &object); err != nil {
		t.Fatalf("decode %s: %v", value, err)
	}
	return object
}

// admitted admits record, failing the test when it is not admitted.
func (f fixture) admitted(t *testing.T, record *kgo.Record) *admission {
	t.Helper()
	admitted, ack := f.receiver.admit(f.ctx, record)
	if admitted == nil {
		t.Fatalf("not admitted (ack %v); logs:\n%s", ack, f.logs.String())
	}
	return admitted
}

func (f fixture) execute(t *testing.T, record *kgo.Record, attempt int64) kgo.AckStatus {
	t.Helper()
	return f.receiver.execute(f.ctx, f.service.Admin, record, f.admitted(t, record), attempt)
}

// run starts the receiver's Run and returns the channel its error arrives on.
func (f fixture) run(t *testing.T, ctx context.Context) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- f.receiver.Run(ctx) }()
	t.Cleanup(func() {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
		}
	})
	return done
}

// await returns the run's latest result on the feed once one arrives.
func (f fixture) await(t *testing.T, run string, done func(wire.Task) bool) wire.Task {
	t.Helper()
	for {
		for _, result := range slices.Backward(f.results(t)) {
			if result.Task.ID == run && done(result.Task) {
				return result.Task
			}
		}
		if f.ctx.Err() != nil {
			t.Fatalf("no result for %s; logs:\n%s", run, f.logs.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func anyTask(wire.Task) bool { return true }

// runToTask runs the receiver, seals its credential, submits one task and returns its result.
func runToTask(t *testing.T, f fixture) wire.Task {
	t.Helper()
	ctx, stop := context.WithCancel(f.ctx)
	t.Cleanup(stop)
	f.sealOwn(t, credentialName, time.Hour)
	f.run(t, ctx)
	f.submit(t, f.host, nil)
	return f.await(t, "run-a", anyTask)
}

func TestEndToEndWithFakeAdapter(t *testing.T) {
	f := newFixture(t, resultStream)
	task := runToTask(t, f)
	if task.Status.State != wire.TaskStateCompleted || task.Artifacts[0].Parts[0].Text != "done" {
		t.Fatalf("unexpected task: %#v", task)
	}
	echoes := 0
	for _, record := range f.records(t, admin.TenantTopic(testTenant)) {
		var echo wire.Echo
		if wire.ParseRecord(record.Value, f.keys(), &echo) == nil {
			echoes++
			if echo.Pool != testPool || echo.Run != "run-a" || echo.Turn != 1 || echo.MessageID != "msg-a" || echo.Placement.Agent != "claude" {
				t.Fatalf("echo %#v", echo)
			}
		}
	}
	if echoes != 1 {
		t.Fatalf("echoes on the feed = %d, want 1", echoes)
	}
}

func TestRegisterPutsBothKeysOnAnEmptyTopic(t *testing.T) {
	f := newFixture(t, resultStream)
	records := f.records(t, admin.ReceiverTopic(testReceiver))
	if len(records) != 1 {
		t.Fatalf("key topic records = %d, want one registration", len(records))
	}
	keys, err := admin.ReadKeys(f.ctx, f.service.Admin, map[string]string{testReceiver: admin.ReceiverTopic(testReceiver)})
	if err != nil {
		t.Fatal(err)
	}
	state := keys[testReceiver]
	if !state.Current.Equal(f.receiver.signing.Public()) || state.Sealing == nil || !state.Sealing.Equal(f.receiver.sealing.PublicKey()) {
		t.Fatalf("registered keys %#v do not match the state directory's", state)
	}
	// A restart with the same keys registers nothing more.
	if err := f.receiver.register(f.ctx); err != nil {
		t.Fatal(err)
	}
	if records := f.records(t, admin.ReceiverTopic(testReceiver)); len(records) != 1 {
		t.Fatalf("key topic records after restart = %d, want 1", len(records))
	}
}

func TestRegisterRefusesAnotherKey(t *testing.T) {
	f := newFixture(t, resultStream)
	for name, change := range map[string]func(*Receiver){
		"signing": func(r *Receiver) { _, r.signing, _ = ed25519.GenerateKey(rand.Reader) },
		"sealing": func(r *Receiver) { r.sealing, _ = ecdh.X25519().GenerateKey(rand.Reader) },
	} {
		t.Run(name, func(t *testing.T) {
			other := &Receiver{id: testReceiver, base: f.receiver.base, signing: f.receiver.signing, sealing: f.receiver.sealing, logger: f.receiver.logger}
			change(other)
			if err := other.register(f.ctx); err == nil {
				t.Fatalf("started over a key topic holding another %s key", name)
			}
		})
	}
	if records := f.records(t, admin.ReceiverTopic(testReceiver)); len(records) != 1 {
		t.Fatalf("key topic records = %d, want the first registration alone", len(records))
	}
}

func TestConfigureWaitsForItsRecord(t *testing.T) {
	f := newFixture(t, resultStream)
	// The superuser reads wherry.admin; no record names receiver-z.
	waiting := &Receiver{id: "receiver-z", base: f.service.Admin, logger: f.receiver.logger, factory: f.receiver.factory}
	ctx, cancel := context.WithTimeout(f.ctx, time.Second)
	defer cancel()
	if err := waiting.configure(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("configure without a record: %v, want to wait until the deadline", err)
	}
	if !strings.Contains(f.logs.String(), "waiting for configuration") {
		t.Fatalf("logs:\n%s", f.logs.String())
	}
}

// appendConfig writes a receiver record as the operator does, bypassing the service's validation.
func (f fixture) appendConfig(t *testing.T, config admin.ReceiverConfig) {
	t.Helper()
	encoded, err := admin.EncodeRecord(admin.Record{Kind: "receiver", Pool: config.Pool, Receiver: config.Receiver, Config: &config})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.service.Operator.ProduceSync(f.ctx, &kgo.Record{Topic: admin.AdminTopic, Key: []byte(config.Receiver), Value: encoded}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func TestConfigureRefusesConfigOutsideTerms(t *testing.T) {
	f := newFixture(t, resultStream)
	config := testConfig(testReceiver)
	config.Runtimes = []string{"exec", "wasm"}
	f.appendConfig(t, config)
	restarted := &Receiver{id: testReceiver, base: f.receiver.base, logger: f.receiver.logger, factory: f.receiver.factory}
	if err := restarted.configure(f.ctx); err == nil || !strings.Contains(err.Error(), "outside pool terms") {
		t.Fatalf("configure: %v, want a refusal naming the terms", err)
	}
}

func TestRunExitsOnConfigChange(t *testing.T) {
	f := newFixture(t, resultStream)
	done := f.run(t, f.ctx)
	time.Sleep(3 * refreshInterval)
	select {
	case err := <-done:
		t.Fatalf("Run returned %v before any change", err)
	default:
	}
	config := testConfig(testReceiver)
	config.Capacity = 2
	f.appendConfig(t, config)
	select {
	case err := <-done:
		if !errors.Is(err, ErrConfigChanged) {
			t.Fatalf("Run returned %v, want ErrConfigChanged", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run kept going under a changed configuration")
	}
}

func TestRunJoinsPeriodicWork(t *testing.T) {
	f := newFixture(t, resultStream)
	ctx, cancel := context.WithCancel(f.ctx)
	done := f.run(t, ctx)
	for f.adapter.sweepCount() < 3 {
		if f.ctx.Err() != nil {
			t.Fatal("receiver did not start its periodic work")
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want context canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after periodic work completed")
	}
}

func testCA(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"}, NotBefore: time.Now(), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestNewTakesItsIDFromTheBundle(t *testing.T) {
	ca := testCA(t)
	dir := t.TempDir()
	for name, bundle := range map[string]admin.Bundle{
		"host principal": {Broker: "127.0.0.1:1", CA: string(ca), Principal: admin.TenantPrincipal("tenant-a", "host-a"), Password: "p"},
		"no CA":          {Broker: "127.0.0.1:1", Principal: admin.ReceiverPrincipal("receiver-a"), Password: "p"},
	} {
		if _, err := New(bundle, filepath.Join(dir, name), nil, log.New(io.Discard, "", 0)); err == nil {
			t.Fatalf("%s: New accepted the bundle", name)
		}
	}
	r, err := New(admin.Bundle{Broker: "127.0.0.1:1", CA: string(ca), Principal: admin.ReceiverPrincipal("receiver-a"), Password: "p"}, filepath.Join(dir, "state"), nil, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	defer r.base.Close()
	if r.id != "receiver-a" {
		t.Fatalf("receiver ID %q", r.id)
	}
	again, err := New(admin.Bundle{Broker: "127.0.0.1:1", CA: string(ca), Principal: admin.ReceiverPrincipal("receiver-a"), Password: "p"}, filepath.Join(dir, "state"), nil, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	defer again.base.Close()
	if !again.signing.Equal(r.signing) || !again.sealing.Equal(r.sealing) {
		t.Fatal("keys were not kept in the state directory")
	}
}

func TestAdvertisement(t *testing.T) {
	config := testConfig(testReceiver)
	config.Labels = map[string]string{"region": "eu"}
	f := newFixtureWith(t, resultStream, testTerms(), config, map[string]*fakeAdapter{
		"nomad": {stream: resultStream, offers: []map[string]string{{"runtime": "exec", "zone": "dc1"}, {"runtime": "lambda", "zone": "dc1"}}},
	})
	state, err := f.service.LoadState(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	latest, err := admin.ReadAdvertisements(f.ctx, f.service.Admin, state, f.keys(), time.Now(), 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	advertisement, ok := latest[testReceiver]
	if !ok {
		t.Fatal("no live advertisement")
	}
	want := []wire.Offer{{Adapter: "nomad", Labels: map[string]string{"agent": "claude", "runtime": "exec", "zone": "dc1", "region": "eu"}}}
	if !reflect.DeepEqual(advertisement.Offers, want) || advertisement.Pool != testPool || advertisement.Capacity != 1 ||
		advertisement.SealingKey != signed.SealingKeyID(f.receiver.sealing.PublicKey()) {
		t.Fatalf("advertisement %#v, want offers %#v", advertisement, want)
	}
}

func TestAdapterOffersErrorIsolated(t *testing.T) {
	m := newMultiFixture(t)
	m.lab.mu.Lock()
	m.lab.offersErr = errors.New("nomad unreachable")
	m.lab.mu.Unlock()
	if err := m.receiver.advertise(m.ctx); err != nil {
		t.Fatal(err)
	}
	m.receiver.mu.Lock()
	offers := m.receiver.offers
	m.receiver.mu.Unlock()
	var got []string
	for _, offer := range offers {
		got = append(got, offer.Adapter+":"+wire.TermsText(offer.Labels))
	}
	want := []string{"cloud:agent=claude,runtime=lambda", "cloud:agent=claude,runtime=exec", "edge:agent=claude,runtime=podman"}
	if !slices.Equal(got, want) {
		t.Fatalf("published offers %v, want %v", got, want)
	}
	if !strings.Contains(m.logs.String(), "adapter lab offers: nomad unreachable") {
		t.Fatalf("offers error not logged: %q", m.logs.String())
	}
}

func TestSweepErrorIsolated(t *testing.T) {
	m := newMultiFixture(t)
	const detail = "variable item test-credential"
	m.lab.mu.Lock()
	m.lab.sweepErr = errors.New(detail)
	m.lab.mu.Unlock()
	before := []int{m.lab.sweepCount(), m.cloud.sweepCount(), m.edge.sweepCount()}
	if err := m.receiver.advertise(m.ctx); err != nil {
		t.Fatal(err)
	}
	after := []int{m.lab.sweepCount(), m.cloud.sweepCount(), m.edge.sweepCount()}
	for i := range after {
		if after[i] != before[i]+1 {
			t.Fatalf("sweeps lab, cloud, edge went %v -> %v, want one more each", before, after)
		}
	}
	m.receiver.mu.Lock()
	offers := len(m.receiver.offers)
	m.receiver.mu.Unlock()
	if offers != 4 {
		t.Fatalf("published %d offers after a sweep error, want 4", offers)
	}
	logs := m.logs.String()
	if !strings.Contains(logs, "adapter lab sweep: *errors.errorString") || strings.Contains(logs, detail) {
		t.Fatalf("unexpected sweep error log: %q", logs)
	}
}

func TestReceiverSweepsOnTick(t *testing.T) {
	f := newFixture(t, resultStream)
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	f.run(t, ctx)
	for f.adapter.sweepCount() < 3 {
		if f.ctx.Err() != nil {
			t.Fatalf("sweeps = %d, want repeated sweeps on the advertisement tick", f.adapter.sweepCount())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

type multiFixture struct {
	fixture
	lab, cloud, edge *fakeAdapter
}

// newMultiFixture configures adapters lab, cloud and edge for one agent; only lab's credential is sealed.
func newMultiFixture(t *testing.T) multiFixture {
	t.Helper()
	m := multiFixture{
		lab:   &fakeAdapter{stream: resultStream, offers: []map[string]string{{"runtime": "exec"}}},
		cloud: &fakeAdapter{stream: resultStream, offers: []map[string]string{{"runtime": "lambda"}, {"runtime": "exec"}}},
		edge:  &fakeAdapter{stream: resultStream, offers: []map[string]string{{"runtime": "podman"}}},
	}
	config := testConfig(testReceiver)
	config.Runtimes = []string{"exec", "podman", "lambda"}
	config.Adapters = []admin.AdapterSettings{
		{Name: "lab", Type: "fake", Agent: "claude", Credential: "cred-lab", Config: map[string]any{}},
		{Name: "cloud", Type: "fake", Agent: "claude", Credential: "cred-cloud", Config: map[string]any{}},
		{Name: "edge", Type: "fake", Agent: "claude", Config: map[string]any{}},
	}
	m.fixture = newFixtureWith(t, resultStream, testTerms(), config, map[string]*fakeAdapter{"lab": m.lab, "cloud": m.cloud, "edge": m.edge})
	m.sealOwn(t, "cred-lab", time.Hour)
	return m
}

func (m multiFixture) execute(t *testing.T, run, runtime string, attempt int64) kgo.AckStatus {
	t.Helper()
	record := m.submit(t, m.host, func(task *wire.Submission) {
		task.Wherry.Run, task.Message.TaskID, task.Message.ContextID = run, run, run
		task.Wherry.Placement.Runtime = runtime
	})
	return m.fixture.execute(t, record, attempt)
}
