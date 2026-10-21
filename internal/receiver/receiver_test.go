package receiver

import (
	"bytes"
	"cmp"
	"context"
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

	"github.com/benemon/wherry/adapter"
	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/prometheus/client_golang/prometheus"
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

var testRedactionKey = []byte("0123456789abcdef0123456789abcdef")

var (
	resultStream   = []string{`{"type":"assistant","message":"working"}`, `{"type":"result","result":"done","is_error":false}`}
	noResultStream = []string{`{"type":"assistant","message":"working"}`}
)

type fakeAdapter struct {
	mu sync.Mutex
	// pool, name and agent are the ones the fixture's factory builds the adapter with.
	pool, name, agent string
	// unready and readyErr make Ready report the adapter unable to start a job.
	unready     bool
	readyErr    error
	stream      []string
	stops       int
	stopErr     error
	stopCheck   func() error
	followErr   error
	follows     int
	dispatches  int
	dispatchErr error
	// outcome is what Dispatch reports, OutcomeCreated when zero, or beside dispatchErr OutcomeUnknown when zero.
	outcome adapter.Outcome
	// handleSize is the length of the handle data Dispatch and Lookup return.
	handleSize int
	lastRun    adapter.Run
	lookups    int
	lookupErr  error
	// job is the state Lookup reports; jobAdapter and jobAgent override the metadata it reports.
	job                  adapter.JobState
	jobAdapter, jobAgent string
	// neverStarted makes Follow report a job whose agent never started, of class neverClass or else driver, for
	// neverReason.
	neverStarted bool
	neverClass   string
	neverReason  string
	purges       int
	// purgeFailures fail that many Purges; purgeErr fails every one.
	purgeFailures int
	purgeErr      error
	releases      int
	releaseErr    error
	releaseIDs    []string
	// calls records Purge and Release in order.
	calls    []string
	sweeps   int
	sweepErr error
	// endDelay holds a stream without a result line open before it ends.
	endDelay time.Duration
	// ended makes Follow end with that terminal after the stream, result line or not.
	ended *adapter.Terminal
	// linkLost makes that many Follows end with adapter.ErrLinkLost after at most one line; froms
	// records the offset of every Follow.
	linkLost int
	froms    []int64
	// hang makes Sweep and Ready block until their context ends.
	hang bool
	// outputs are what Collect streams, unless collectErr fails it; collects counts the calls, stopsBefore the
	// Stops each one followed and limits the last one's limits.
	outputs    []adapter.Output
	collectErr error
	// collectHangs makes Collect block until its context ends.
	collectHangs bool
	collects     int
	stopsBefore  []int
	limits       adapter.Limits
	// samples are what Metrics reports, unless metricsHang makes it block until its context ends.
	samples     []adapter.Sample
	metricsHang bool
}

func (f *fakeAdapter) Directories() adapter.Directories {
	return adapter.Directories{Input: "/task/inputs", Output: "/task/outputs", Credentials: "/task/secrets/credentials"}
}

func (f *fakeAdapter) Ready(ctx context.Context) (bool, error) {
	if f.hung(ctx) {
		return false, ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.unready && f.readyErr == nil, f.readyErr
}

func (f *fakeAdapter) handle(runID string, turn int64) adapter.Handle {
	return adapter.Handle{Data: []byte(runID + strings.Repeat("h", max(0, f.handleSize-len(runID)))), Name: fmt.Sprintf("agent-%s-t%d", runID, turn)}
}

func (f *fakeAdapter) Dispatch(_ context.Context, run adapter.Run) (adapter.Outcome, adapter.Handle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dispatches++
	f.lastRun = run
	if f.dispatchErr != nil {
		return f.outcome, adapter.Handle{}, f.dispatchErr
	}
	if f.outcome == adapter.OutcomeNothingCreated {
		return f.outcome, adapter.Handle{}, nil
	}
	return cmp.Or(f.outcome, adapter.OutcomeCreated), f.handle(run.ID, run.Turn), nil
}

func (f *fakeAdapter) Lookup(ctx context.Context, runID string, turn int64) (adapter.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lookups++
	if f.lookupErr != nil {
		return adapter.Job{}, f.lookupErr
	}
	if err := ctx.Err(); err != nil {
		return adapter.Job{}, err
	}
	if f.job == adapter.JobAbsent {
		return adapter.Job{}, nil
	}
	return adapter.Job{State: f.job, Handle: f.handle(runID, turn), Attempt: 1, Adapter: cmp.Or(f.jobAdapter, f.name), Agent: cmp.Or(f.jobAgent, f.agent)}, nil
}

// Follow emits the stream; a stream with a result keeps running like a live task, one without ends.
func (f *fakeAdapter) Follow(ctx context.Context, _ adapter.Handle, from int64, lines chan<- adapter.Line) (adapter.Terminal, error) {
	f.mu.Lock()
	f.follows++
	f.froms = append(f.froms, from)
	lost := f.follows <= f.linkLost
	stream, followErr, endDelay, neverStarted, neverClass, neverReason, ended := append([]string(nil), f.stream...), f.followErr, f.endDelay, f.neverStarted, f.neverClass, f.neverReason, f.ended
	f.mu.Unlock()
	var offset int64
	for _, data := range stream {
		next := offset + int64(len(data)+1)
		if offset >= from {
			select {
			case lines <- adapter.Line{Offset: offset, Next: next, Data: []byte(data)}:
			case <-ctx.Done():
				return adapter.Terminal{}, ctx.Err()
			}
			if lost {
				return adapter.Terminal{}, adapter.ErrLinkLost
			}
		}
		offset = next
	}
	if lost {
		return adapter.Terminal{}, adapter.ErrLinkLost
	}
	if followErr != nil {
		return adapter.Terminal{}, followErr
	}
	if ended != nil || !slices.ContainsFunc(stream, func(data string) bool { _, ok := resultLine([]byte(data)); return ok }) {
		select {
		case <-time.After(endDelay):
		case <-ctx.Done():
		}
		switch {
		case neverStarted:
			return adapter.Terminal{Class: cmp.Or(neverClass, "driver"), Node: "node-1", Reason: neverReason}, nil
		case ended != nil:
			return *ended, nil
		}
		return adapter.Terminal{Started: true, ExitCode: 1, Node: "node-1"}, nil
	}
	<-ctx.Done()
	return adapter.Terminal{}, ctx.Err()
}

func (f *fakeAdapter) Stop(context.Context, adapter.Handle) error {
	f.mu.Lock()
	f.stops++
	err, check := f.stopErr, f.stopCheck
	f.mu.Unlock()
	if check != nil {
		return check()
	}
	return err
}

func (f *fakeAdapter) Collect(ctx context.Context, _ adapter.Handle, limits adapter.Limits, out chan<- adapter.Output) error {
	f.mu.Lock()
	f.collects++
	f.limits = limits
	f.stopsBefore = append(f.stopsBefore, f.stops)
	outputs, err, hangs := slices.Clone(f.outputs), f.collectErr, f.collectHangs
	f.mu.Unlock()
	if hangs {
		<-ctx.Done()
		return ctx.Err()
	}
	if err != nil {
		return err
	}
	for _, output := range outputs {
		select {
		case out <- output:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (f *fakeAdapter) Purge(context.Context, string, int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.purges++
	f.calls = append(f.calls, "purge")
	if f.purges <= f.purgeFailures {
		return errors.New("purge failed")
	}
	return f.purgeErr
}

func (f *fakeAdapter) Release(_ context.Context, runID string, _ int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releases++
	f.releaseIDs = append(f.releaseIDs, runID)
	f.calls = append(f.calls, "release")
	return f.releaseErr
}

// hung blocks until ctx ends when the adapter hangs, and reports whether it did.
func (f *fakeAdapter) hung(ctx context.Context) bool {
	f.mu.Lock()
	hang := f.hang
	f.mu.Unlock()
	if hang {
		<-ctx.Done()
	}
	return hang
}

func (f *fakeAdapter) Sweep(ctx context.Context) error {
	if f.hung(ctx) {
		return ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sweeps++
	return f.sweepErr
}

func (f *fakeAdapter) Metrics(ctx context.Context) ([]adapter.Sample, error) {
	f.mu.Lock()
	hang, samples := f.metricsHang, f.samples
	f.mu.Unlock()
	if hang {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return samples, nil
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
		Agents:       []admin.AgentTerms{{Name: "claude", Credentials: []string{credentialName, "cred-lab", "cred-cloud"}}, {Name: "codex", Credentials: []string{}}},
		DefaultAgent: "claude", SealLifetimeMS: time.Hour.Milliseconds(), DeliveryLimit: 10, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50,
	}
}

func testConfig(receiver string) admin.ReceiverConfig {
	return admin.ReceiverConfig{
		Receiver: receiver, Pool: testPool, Capacity: 1,
		Adapters: []admin.AdapterSettings{{Name: "nomad", Type: "fake", Agent: "claude", Credential: credentialName, Labels: map[string]string{"zone": "dc1", "example.com/gpu": "none"}, Config: map[string]any{}}},
	}
}

func shortIntervals(t *testing.T) {
	for _, interval := range []*time.Duration{&renewInterval, &advertiseInterval, &refreshInterval, &cancelReadInterval, &reconcileInterval} {
		previous := *interval
		*interval = 200 * time.Millisecond
		t.Cleanup(func() { *interval = previous })
	}
	previous := sealWait
	sealWait = 1500 * time.Millisecond
	t.Cleanup(func() { sealWait = previous })
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
		"nomad": {stream: stream},
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
	f.produce(t, admin.HostTopic(testTenant, testHost), "", must(t)(signed.RegisterKey(signed.HostOwner(testTenant, testHost), host)))
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
	logs := &syncBuffer{}
	factory := func(pool string, settings admin.AdapterSettings) (Named, error) {
		adapter, ok := f.adapters[settings.Name]
		if !ok || settings.Type != "fake" {
			return Named{}, errors.New("no fake adapter")
		}
		adapter.mu.Lock()
		adapter.pool, adapter.name, adapter.agent = pool, settings.Name, settings.Agent
		adapter.mu.Unlock()
		return Named{Name: settings.Name, Agent: settings.Agent, Credential: settings.Credential, Labels: settings.Labels, Adapter: adapter}, nil
	}
	return &Receiver{id: config.Receiver, kafkaOpt: opts, base: base, signing: signing, factory: factory, logger: log.New(logs, "", 0), metrics: newMetrics(prometheus.NewRegistry())}, logs
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
			Placement:  wire.Placement{Agent: "claude", Selector: []wire.Requirement{}},
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

// sealRequests returns this receiver's seal requests on the tenant feed in log order.
func (f fixture) sealRequests(t *testing.T) []wire.SealRequest {
	t.Helper()
	var requests []wire.SealRequest
	for _, record := range f.records(t, admin.TenantTopic(testTenant)) {
		var request wire.SealRequest
		if wire.ParseRecord(record.Value, f.keys(), &request) == nil {
			requests = append(requests, request)
		}
	}
	return requests
}

// sealFor seals values to request's key with signed.Seal, as the host's answer does, and puts the seal on the host's
// topic under the request's run.
func (f fixture) sealFor(t *testing.T, request wire.SealRequest, values map[string][]byte) {
	t.Helper()
	sealed, err := signed.Seal(signed.SealBinding{
		Tenant: request.Tenant, Pool: request.Pool, Receiver: request.Receiver, KeyID: request.KeyID, Run: request.Run,
		Turn: request.Turn, Attempt: request.Attempt, Credentials: request.Credentials, TaskCredentials: request.TaskCredentials,
		S3Inputs: request.S3Inputs, S3Outputs: request.S3Outputs, ExpiryMS: request.ExpiryMS,
	}, request.Key.Key.(*ecdsa.PublicKey), signed.SealValues{Values: values, RedactionKey: testRedactionKey})
	if err != nil {
		t.Fatal(err)
	}
	f.produce(t, admin.HostTopic(testTenant, testHost), request.Run, sealed)
}

// answer answers, until the test ends, each of this receiver's seal requests that names credential alone with
// testToken, as a host holding that credential does.
func (f fixture) answer(t *testing.T, credential string) {
	t.Helper()
	answerSeals(t, f.ctx, f.service.Admin, f.keys(), testTenant, testHost, credential, testToken)
}

// answerSeals answers, until the test ends, each seal request on tenant's feed signed under keys that names credential
// alone: it seals value with signed.Seal, as the host's answer does, onto host's topic under the request's run.
func answerSeals(t *testing.T, ctx context.Context, kafka *kgo.Client, keys signed.Keys, tenant, host, credential, value string) {
	t.Helper()
	ctx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	t.Cleanup(func() { stop(); <-done })
	go func() {
		defer close(done)
		answered := map[string]bool{}
		for ctx.Err() == nil {
			records, err := admin.ReadTopic(ctx, kafka, admin.TenantTopic(tenant))
			for _, record := range records {
				var request wire.SealRequest
				if err != nil || wire.ParseRecord(record.Value, keys, &request) != nil || answered[request.KeyID] || !slices.Equal(request.Credentials, []string{credential}) {
					continue
				}
				answered[request.KeyID] = true
				sealed, err := signed.Seal(signed.SealBinding{
					Tenant: request.Tenant, Pool: request.Pool, Receiver: request.Receiver, KeyID: request.KeyID, Run: request.Run,
					Turn: request.Turn, Attempt: request.Attempt, Credentials: request.Credentials, ExpiryMS: request.ExpiryMS,
				}, request.Key.Key.(*ecdsa.PublicKey), signed.SealValues{Values: map[string][]byte{credential: []byte(value)}, RedactionKey: testRedactionKey})
				if err == nil {
					err = kafka.ProduceSync(ctx, &kgo.Record{Topic: admin.HostTopic(tenant, host), Key: []byte(request.Run), Value: sealed}).FirstErr()
				}
				if err != nil && ctx.Err() == nil {
					panic(err)
				}
			}
			sleep(ctx, 50*time.Millisecond)
		}
	}()
}

// addPeer provisions another member of the pool and registers its key.
func (f fixture) addPeer(t *testing.T, id, pool string) ed25519.PrivateKey {
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
	f.produce(t, admin.ReceiverTopic(id), id, must(t)(signed.RegisterKey(id, signing)))
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	return signing
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

// runToTask runs the receiver, answers its seal request, submits one task and returns its result.
func runToTask(t *testing.T, f fixture) wire.Task {
	t.Helper()
	ctx, stop := context.WithCancel(f.ctx)
	t.Cleanup(stop)
	f.answer(t, credentialName)
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

func TestRegisterPutsItsKeyOnAnEmptyTopic(t *testing.T) {
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
	if !state.Current.Equal(f.receiver.signing.Public()) {
		t.Fatalf("registered key %#v does not match the state directory's", state)
	}
	// A restart with the same keys registers nothing more.
	if err := f.receiver.register(f.ctx); err != nil {
		t.Fatal(err)
	}
	if records := f.records(t, admin.ReceiverTopic(testReceiver)); len(records) != 1 {
		t.Fatalf("key topic records after restart = %d, want 1", len(records))
	}
}

func TestRegisterRefusesAKeyTopicThatIsNotEmpty(t *testing.T) {
	f := newFixture(t, resultStream)
	fresh, _ := f.newReceiver(t, testConfig("receiver-c"))
	f.produce(t, admin.ReceiverTopic("receiver-c"), "receiver-c", []byte("not a key record"))
	if err := fresh.register(f.ctx); err == nil {
		t.Fatal("registered over a key topic that already holds a record")
	}
	if records := f.records(t, admin.ReceiverTopic("receiver-c")); len(records) != 1 {
		t.Fatalf("key topic records = %d, want the invalid record alone", len(records))
	}
}

func TestRegisterAcceptsARotationToItsKeys(t *testing.T) {
	f := newFixture(t, resultStream)
	_, next, _ := ed25519.GenerateKey(rand.Reader)
	f.produce(t, admin.ReceiverTopic(testReceiver), testReceiver, must(t)(signed.RotateKey(testReceiver, f.receiver.signing, next)))
	rotated := &Receiver{id: testReceiver, base: f.receiver.base, signing: next, logger: f.receiver.logger}
	if err := rotated.register(f.ctx); err != nil {
		t.Fatalf("a receiver holding the keys its topic rotated to did not start: %v", err)
	}
	if err := f.receiver.register(f.ctx); err == nil {
		t.Fatal("a receiver holding the retired keys started")
	}
	f.produce(t, admin.ReceiverTopic(testReceiver), testReceiver, []byte("stray"))
	if err := rotated.register(f.ctx); err == nil {
		t.Fatal("started over a key topic with a stray record after its rotation")
	}
	if records := f.records(t, admin.ReceiverTopic(testReceiver)); len(records) != 3 {
		t.Fatalf("key topic records = %d, want nothing appended", len(records))
	}
}

func TestRegisterRefusesAnotherKey(t *testing.T) {
	f := newFixture(t, resultStream)
	other := &Receiver{id: testReceiver, base: f.receiver.base, logger: f.receiver.logger}
	_, other.signing, _ = ed25519.GenerateKey(rand.Reader)
	if err := other.register(f.ctx); err == nil {
		t.Fatal("started over a key topic holding another signing key")
	}
	if records := f.records(t, admin.ReceiverTopic(testReceiver)); len(records) != 1 {
		t.Fatalf("key topic records = %d, want the first registration alone", len(records))
	}
}

func TestConfigureWaitsForItsRecord(t *testing.T) {
	f := newFixture(t, resultStream)
	// The superuser reads wherry.admin; no record names receiver-z.
	waiting := &Receiver{id: "receiver-z", base: f.service.Admin, logger: f.receiver.logger, factory: f.receiver.factory, metrics: newMetrics(prometheus.NewRegistry())}
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
	config.Adapters[0].Agent = "gemini"
	f.appendConfig(t, config)
	restarted := &Receiver{id: testReceiver, base: f.receiver.base, logger: f.receiver.logger, factory: f.receiver.factory, metrics: newMetrics(prometheus.NewRegistry())}
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
		if _, err := New(bundle, filepath.Join(dir, name), nil, log.New(io.Discard, "", 0), prometheus.NewRegistry()); err == nil {
			t.Fatalf("%s: New accepted the bundle", name)
		}
	}
	r, err := New(admin.Bundle{Broker: "127.0.0.1:1", CA: string(ca), Principal: admin.ReceiverPrincipal("receiver-a"), Password: "p"}, filepath.Join(dir, "state"), nil, log.New(io.Discard, "", 0), prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer r.base.Close()
	if r.id != "receiver-a" {
		t.Fatalf("receiver ID %q", r.id)
	}
	again, err := New(admin.Bundle{Broker: "127.0.0.1:1", CA: string(ca), Principal: admin.ReceiverPrincipal("receiver-a"), Password: "p"}, filepath.Join(dir, "state"), nil, log.New(io.Discard, "", 0), prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer again.base.Close()
	if !again.signing.Equal(r.signing) {
		t.Fatal("the signing key was not kept in the state directory")
	}
}

func TestNewDeletesTheRetiredSealingKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sealing.key")
	if err := os.WriteFile(path, []byte("x25519-private-key"), 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := New(admin.Bundle{Broker: "127.0.0.1:1", CA: string(testCA(t)), Principal: admin.ReceiverPrincipal("receiver-a"), Password: "p"}, dir, nil, log.New(io.Discard, "", 0), prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer r.base.Close()
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("sealing key left in the state directory: %v", err)
	}
}

// advertisedOffers reads this receiver's latest live advertisement as a host does.
func (f fixture) advertisedOffers(t *testing.T) wire.Advertisement {
	t.Helper()
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
	return advertisement
}

func TestAdvertisement(t *testing.T) {
	f := newFixture(t, resultStream)
	advertisement := f.advertisedOffers(t)
	want := []wire.Offer{{Adapter: "nomad", Agent: "claude"}}
	if !reflect.DeepEqual(advertisement.Offers, want) || advertisement.Pool != testPool || advertisement.Capacity != 1 {
		t.Fatalf("advertisement %#v, want offers %#v", advertisement, want)
	}
}

func TestAdvertisedOffersAreTheConfiguredAdapters(t *testing.T) {
	m := newMultiFixture(t)
	state, err := m.service.LoadState(m.ctx)
	if err != nil {
		t.Fatal(err)
	}
	var want []wire.Offer
	for _, settings := range state.Receivers[testReceiver].Adapters {
		want = append(want, wire.Offer{Adapter: settings.Name, Agent: settings.Agent})
	}
	if offers := m.advertisedOffers(t).Offers; !reflect.DeepEqual(offers, want) {
		t.Fatalf("offers %#v, want each adapter wherry.admin configures, in order: %#v", offers, want)
	}
}

func TestReceiverWithNoReadyAdapterAdvertisesNoOffers(t *testing.T) {
	f := newFixture(t, resultStream)
	f.adapter.mu.Lock()
	f.adapter.unready = true
	f.adapter.mu.Unlock()
	if err := f.receiver.advertise(f.ctx); err != nil {
		t.Fatal(err)
	}
	if advertisement := f.advertisedOffers(t); len(advertisement.Offers) != 0 || advertisement.Capacity != 1 {
		t.Fatalf("advertisement %#v, want a live advertisement with no offers", advertisement)
	}
	if admitted, ack := f.receiver.admit(f.ctx, f.submit(t, f.host, nil)); admitted != nil || ack != kgo.AckRelease {
		t.Fatalf("admitted=%v ack=%v, want a start failure and release", admitted != nil, ack)
	}
}

func TestReadinessErrorLogsAndLeavesAdapterOut(t *testing.T) {
	m := newMultiFixture(t)
	m.lab.mu.Lock()
	m.lab.readyErr = errors.New("nomad unreachable")
	m.lab.mu.Unlock()
	if err := m.receiver.advertise(m.ctx); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, offer := range m.advertisedOffers(t).Offers {
		got = append(got, offer.Adapter)
	}
	if want := []string{"cloud", "edge"}; !slices.Equal(got, want) {
		t.Fatalf("advertised adapters %v, want %v", got, want)
	}
	if logs := m.logs.String(); !strings.Contains(logs, "adapter lab readiness: *errors.errorString") || strings.Contains(logs, "nomad unreachable") {
		t.Fatalf("readiness error not logged: %q", m.logs.String())
	}
}

func TestHungAdapterDoesNotStopAdvertising(t *testing.T) {
	m := newMultiFixture(t)
	m.lab.mu.Lock()
	m.lab.hang = true
	m.lab.mu.Unlock()
	done := make(chan error, 1)
	go func() { done <- m.receiver.advertise(m.ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("advertise waited on a hung adapter")
	}
	var got []string
	for _, offer := range m.advertisedOffers(t).Offers {
		got = append(got, offer.Adapter)
	}
	if want := []string{"cloud", "edge"}; !slices.Equal(got, want) {
		t.Fatalf("advertised adapters %v, want %v", got, want)
	}
	logs := m.logs.String()
	if !strings.Contains(logs, "adapter lab sweep: timed out") || !strings.Contains(logs, "adapter lab readiness: timed out") {
		t.Fatalf("hung adapter's calls not logged: %q", logs)
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
	if offers != 3 {
		t.Fatalf("published %d offers after a sweep error, want 3", offers)
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

// newMultiFixture configures adapters lab, cloud and edge, in that order, for one agent with the labels
// their operator declares: lab and cloud both carry class=cpu, only cloud an accelerator, edge class=arm.
// Only lab's credential is answered.
func newMultiFixture(t *testing.T) multiFixture {
	t.Helper()
	m := multiFixture{lab: &fakeAdapter{stream: resultStream}, cloud: &fakeAdapter{stream: resultStream}, edge: &fakeAdapter{stream: resultStream}}
	config := testConfig(testReceiver)
	config.Adapters = []admin.AdapterSettings{
		{Name: "lab", Type: "fake", Agent: "claude", Credential: "cred-lab", Labels: map[string]string{"site": "lab", "class": "cpu"}, Config: map[string]any{}},
		{Name: "cloud", Type: "fake", Agent: "claude", Credential: "cred-cloud", Labels: map[string]string{"site": "cloud", "class": "cpu", "example.com/accelerator": "gpu"}, Config: map[string]any{}},
		{Name: "edge", Type: "fake", Agent: "claude", Labels: map[string]string{"site": "edge", "class": "arm"}, Config: map[string]any{}},
	}
	m.fixture = newFixtureWith(t, resultStream, testTerms(), config, map[string]*fakeAdapter{"lab": m.lab, "cloud": m.cloud, "edge": m.edge})
	m.answer(t, "cred-lab")
	return m
}

func in(key string, values ...string) wire.Requirement {
	return wire.Requirement{Key: key, Operator: wire.OpIn, Values: values}
}

func notIn(key string, values ...string) wire.Requirement {
	return wire.Requirement{Key: key, Operator: wire.OpNotIn, Values: values}
}

func exists(key string) wire.Requirement {
	return wire.Requirement{Key: key, Operator: wire.OpExists}
}

func doesNotExist(key string) wire.Requirement {
	return wire.Requirement{Key: key, Operator: wire.OpDoesNotExist}
}

// Selectors for the multi fixture: cpu matches lab then cloud, accelerated cloud alone, arm edge alone.
var (
	cpu         = []wire.Requirement{in("class", "cpu")}
	accelerated = []wire.Requirement{exists("example.com/accelerator")}
	arm         = []wire.Requirement{in("class", "arm")}
)

func (m multiFixture) execute(t *testing.T, run string, selector []wire.Requirement, attempt int64) kgo.AckStatus {
	t.Helper()
	record := m.submit(t, m.host, func(task *wire.Submission) {
		task.Wherry.Run, task.Message.TaskID, task.Message.ContextID = run, run, run
		task.Wherry.Placement.Selector = selector
	})
	return m.fixture.execute(t, record, attempt)
}

// routed admits a task with selector and returns the adapter it was routed to.
func (m multiFixture) routed(t *testing.T, selector ...wire.Requirement) string {
	t.Helper()
	record := m.submit(t, m.host, func(task *wire.Submission) {
		task.Wherry.Placement.Selector = append([]wire.Requirement{}, selector...)
	})
	return m.admitted(t, record).adapter.Name
}

func TestEachSelectorOperatorRoutes(t *testing.T) {
	m := newMultiFixture(t)
	for _, test := range []struct {
		name     string
		selector []wire.Requirement
		want     string
	}{
		{"none, first in declared order", nil, "lab"},
		{"In", []wire.Requirement{in("class", "arm")}, "edge"},
		{"In, several values", []wire.Requirement{in("site", "edge", "cloud")}, "cloud"},
		{"NotIn", []wire.Requirement{notIn("site", "lab")}, "cloud"},
		{"NotIn, key absent", []wire.Requirement{notIn("example.com/accelerator", "gpu")}, "lab"},
		{"Exists", []wire.Requirement{exists("example.com/accelerator")}, "cloud"},
		{"DoesNotExist", []wire.Requirement{doesNotExist("example.com/accelerator"), in("class", "cpu")}, "lab"},
		{"conjunction", []wire.Requirement{in("class", "cpu"), notIn("site", "lab"), exists("example.com/accelerator")}, "cloud"},
	} {
		if got := m.routed(t, test.selector...); got != test.want {
			t.Errorf("%s: routed to %s, want %s", test.name, got, test.want)
		}
	}
}

func TestUnreadyAdapterIsNeverChosen(t *testing.T) {
	m := newMultiFixture(t)
	m.lab.mu.Lock()
	m.lab.unready = true
	m.lab.mu.Unlock()
	if err := m.receiver.advertise(m.ctx); err != nil {
		t.Fatal(err)
	}
	for _, offer := range m.advertisedOffers(t).Offers {
		if offer.Adapter == "lab" {
			t.Fatal("an adapter that is not ready was advertised")
		}
	}
	if got := m.routed(t, in("class", "cpu")); got != "cloud" {
		t.Fatalf("routed to %s, want cloud: lab is not ready", got)
	}
	// Only the unready adapter satisfies this selector: a start failure, not a dispatch.
	record := m.submit(t, m.host, func(task *wire.Submission) { task.Wherry.Placement.Selector = []wire.Requirement{in("site", "lab")} })
	if admitted, ack := m.receiver.admit(m.ctx, record); admitted != nil || ack != kgo.AckRelease {
		t.Fatalf("admitted=%v ack=%v, want a start failure and release", admitted != nil, ack)
	}
}

func TestAdaptersAreBuiltForTheReceiversPool(t *testing.T) {
	f := newFixture(t, resultStream)
	f.adapter.mu.Lock()
	defer f.adapter.mu.Unlock()
	if f.adapter.pool != testPool {
		t.Fatalf("adapter built for pool %q, want %q", f.adapter.pool, testPool)
	}
}
