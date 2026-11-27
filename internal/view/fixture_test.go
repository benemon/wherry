package view

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	json "encoding/json/v2"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

const (
	poolA, poolB         = "pool-a", "pool-b"
	tenantA, tenantB     = "tenant-a", "tenant-b"
	hostA, hostB         = "host-a", "host-b"
	receiverA, receiverB = "receiver-a", "receiver-b"
	receiverC            = "receiver-c"
	agentA               = "agent-a"
	endpointA, bucketA   = "https://s3.example.com", "bucket-a"
	bucketB              = "bucket-b"
)

// fixture is a kfake cluster provisioned as an operator does, with the keys of its receivers and hosts, and a clock
// the view reads.
type fixture struct {
	t       *testing.T
	ctx     context.Context
	cluster *kfake.Cluster
	service *operator.Service
	client  *kgo.Client
	root    *kgo.Client
	keys    map[string]ed25519.PrivateKey
	mu      sync.Mutex
	now     time.Time
}

func testTerms(bucket string) operator.Terms {
	return operator.Terms{
		Agents:       []operator.AgentTerms{{Name: agentA, Models: []string{"model-x"}, Credentials: []operator.AgentCredential{{TaskCredential: wire.TaskCredential{Name: "model", Env: "MODEL_API_KEY"}}}}},
		DefaultAgent: agentA, DeliveryLimit: 3, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50,
		Storage: &operator.Storage{Endpoint: endpointA, Bucket: bucket},
	}
}

func receiverConfig(receiver, pool string) operator.ReceiverConfig {
	return operator.ReceiverConfig{Receiver: receiver, Pool: pool, Profiles: []operator.ProfileSettings{{Name: "profile-a", Plugin: "fake", Agent: agentA, Labels: map[string]string{"zone": "dc1"}}},
		PluginSHA256: map[string]string{"fake": strings.Repeat("ab", 32)}}
}

// newFixture provisions pool-a with receivers a and b, tenant-a entitled to it with host-a, and registers their keys.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	cluster := kfake.MustCluster(kfake.EnableSASL(), kfake.EnableACLs(), kfake.Superuser("SCRAM-SHA-512", "admin", "password"), kfake.Superuser("PLAIN", operator.OperatorPrincipal, "operator-password"))
	t.Cleanup(cluster.Close)
	root, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.SASL(scram.Auth{User: "admin", Pass: "password"}.AsSha512Mechanism()), kgo.FetchMaxWait(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(root.Close)
	client, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.SASL(plain.Auth{User: operator.OperatorPrincipal, Pass: "operator-password"}.AsMechanism()), kgo.FetchMaxWait(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	ctx := context.Background()
	// The fixture's clock runs ahead of the wall clock, so every record a test writes at a time of its own comes after
	// the operator records the service writes now.
	f := &fixture{t: t, ctx: ctx, cluster: cluster, client: client, root: root, keys: map[string]ed25519.PrivateKey{}, now: time.Now().Add(3 * time.Hour).Truncate(time.Millisecond)}
	f.service = &operator.Service{Operator: client, Broker: cluster.ListenAddrs()[0], DeliveryCeiling: 10, LockCeilingMS: 60000}
	if err := f.service.PoolApply(ctx, poolA, testTerms(bucketA), false); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantAdd(ctx, tenantA); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantPoolAdd(ctx, tenantA, poolA); err != nil {
		t.Fatal(err)
	}
	f.addHost(tenantA, hostA)
	f.addReceiver(receiverA, poolA)
	f.addReceiver(receiverB, poolA)
	return f
}

func (f *fixture) addHost(tenant, host string) ed25519.PrivateKey {
	f.t.Helper()
	if err := f.service.HostAdd(f.ctx, tenant, host, filepath.Join(f.t.TempDir(), "host.bundle")); err != nil {
		f.t.Fatal(err)
	}
	owner := signed.HostOwner(tenant, host)
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	f.keys[owner] = key
	f.produce(operator.HostTopic(tenant, host), owner, must(f.t)(signed.RegisterKey(owner, key)), f.now.Add(-48*time.Hour))
	return key
}

func (f *fixture) addReceiver(receiver, pool string) ed25519.PrivateKey {
	f.t.Helper()
	if err := f.service.ReceiverAdd(f.ctx, receiverConfig(receiver, pool), f.service.Broker, filepath.Join(f.t.TempDir(), receiver+".bundle")); err != nil {
		f.t.Fatal(err)
	}
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	f.keys[receiver] = key
	f.produce(operator.ReceiverTopic(receiver), receiver, must(f.t)(signed.RegisterKey(receiver, key)), f.now.Add(-48*time.Hour))
	return key
}

// rotate replaces owner's key at at on its key topic and returns the old key.
func (f *fixture) rotate(owner, topic string, at time.Time) ed25519.PrivateKey {
	f.t.Helper()
	old := f.keys[owner]
	_, next, _ := ed25519.GenerateKey(rand.Reader)
	f.produce(topic, owner, must(f.t)(signed.RotateKey(owner, old, next)), at)
	f.keys[owner] = next
	return old
}

// operator appends record to wherry.operator stamped at.
func (f *fixture) operator(at time.Time, record operator.Record) {
	f.t.Helper()
	record.Version, record.AtMS = 1, at.UnixMilli()
	f.produce(operator.OperatorTopic, "", must(f.t)(operator.EncodeRecord(record)), at)
}

// must is the bytes a signing or encoding returned, failing t on its error.
func must(t *testing.T) func([]byte, error) []byte {
	return func(value []byte, err error) []byte {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
}

func (f *fixture) produce(topic, key string, value []byte, at time.Time) *kgo.Record {
	f.t.Helper()
	record := &kgo.Record{Topic: topic, Key: []byte(key), Value: value, Timestamp: at}
	if err := f.client.ProduceSync(f.ctx, record).FirstErr(); err != nil {
		f.t.Fatalf("produce to %s: %v", topic, err)
	}
	return record
}

func (f *fixture) sign(record wire.Record, key ed25519.PrivateKey) []byte {
	f.t.Helper()
	return must(f.t)(wire.SignRecord(record, key))
}

// nonce is a fixed run nonce for name, and runID the run tenant's host derives from it.
func nonce(name string) string {
	sum := sha256.Sum256([]byte(name))
	return base64.RawURLEncoding.EncodeToString(sum[:16])
}

func runID(tenant, host, name string) string { return wire.RunID(tenant, host, nonce(name)) }

// hashOf is a task hash for the run named name.
func hashOf(name string) string {
	sum := sha256.Sum256([]byte("task-" + name))
	return hex.EncodeToString(sum[:])
}

// st describes a status to sign.
type st struct {
	receiver, tenant, pool, run string
	attempt                     int64
	kind                        string
	state                       wire.TaskState
	hash, event, text, eventID  string
	key                         ed25519.PrivateKey
	taskOffset                  *int64
	chunks                      int64
}

func (f *fixture) statusRecord(at time.Time, s st) []byte {
	f.t.Helper()
	status, key := f.statusValue(at, s)
	return f.sign(status, key)
}

// statusValue is the status s describes, unsigned, with the key s names or its receiver's.
func (f *fixture) statusValue(at time.Time, s st) (*wire.RunStatus, ed25519.PrivateKey) {
	f.t.Helper()
	if s.attempt == 0 {
		s.attempt = 1
	}
	if s.hash == "" {
		s.hash = hashOf(s.run)
	}
	if s.key == nil {
		s.key = f.keys[s.receiver]
	}
	if s.eventID == "" {
		s.eventID = wire.RandomID("event-")
	}
	status := &wire.RunStatus{
		StatusUpdate: wire.TaskStatusUpdateEvent{TaskID: s.run, ContextID: s.run, Status: wire.TaskStatus{State: s.state, Timestamp: at.UTC().Format(time.RFC3339Nano)}},
		Wherry:       wire.StatusBinding{ReceiverBinding: wire.ReceiverBinding{Receiver: s.receiver, Tenant: s.tenant, Pool: s.pool, Run: s.run, Attempt: s.attempt}, EventID: s.eventID, Kind: s.kind, TaskHash: s.hash, Event: s.event, TranscriptChunks: s.chunks},
	}
	if s.kind == "placing" || s.kind == "working" {
		status.StatusUpdate.Status.Message = &wire.Message{MessageID: wire.RandomID("msg-"), Role: wire.RoleAgent, Parts: []wire.Part{{Text: strings.TrimSpace(s.text + " step")}}, Extensions: []string{wire.ExtensionURI}}
	} else if s.text != "" {
		status.StatusUpdate.Status.Message = &wire.Message{MessageID: wire.RandomID("msg-"), Role: wire.RoleAgent, Parts: []wire.Part{{Text: s.text}}, Extensions: []string{wire.ExtensionURI}}
	}
	if s.kind == "claimed" {
		offset := int64(7)
		if s.taskOffset != nil {
			offset = *s.taskOffset
		}
		status.Wherry.TaskTopic, status.Wherry.TaskOffset = operator.SignatureTopic(s.pool, agentA, map[string]string{"zone": "dc1"}), &offset
	}
	return status, s.key
}

// status dual-writes a status to the pool's events and the tenant's feed, as a receiver does, and returns its digest.
func (f *fixture) status(at time.Time, s st) string {
	f.t.Helper()
	value := f.statusRecord(at, s)
	f.produce(operator.PoolEventsTopic(s.pool), s.run, value, at)
	f.produce(operator.TenantTopic(s.tenant), s.run, value, at)
	return wire.Digest(value)
}

// poolOnly writes a status to the pool's events alone, as released, adopting and reconciling are.
func (f *fixture) poolOnly(at time.Time, s st) string {
	f.t.Helper()
	value := f.statusRecord(at, s)
	f.produce(operator.PoolEventsTopic(s.pool), s.run, value, at)
	return wire.Digest(value)
}

// echo writes receiver's echo of name's run, submitted by tenant's host, to the tenant's feed.
func (f *fixture) echo(at time.Time, receiver, tenant, host, pool, name string) string {
	f.t.Helper()
	return f.echoWith(at, receiver, tenant, host, pool, name, hashOf(runID(tenant, host, name)))
}

func (f *fixture) echoWith(at time.Time, receiver, tenant, host, pool, name, hash string) string {
	f.t.Helper()
	run := runID(tenant, host, name)
	echo := &wire.Echo{Receiver: receiver, Tenant: tenant, Host: host, Pool: pool, Run: run, RunNonce: nonce(name), Placement: wire.Placement{Agent: agentA, Model: "model-x"}, TaskHash: hash, MessageID: "message-" + name}
	f.produce(operator.TenantTopic(tenant), run, f.sign(echo, f.keys[receiver]), at)
	return run
}

// result writes a result with metadata and reply to the tenant's feed.
func (f *fixture) result(at time.Time, receiver, tenant, pool, run string, attempt int64, state wire.TaskState, metadata wire.ResultMetadata, reply string) {
	f.t.Helper()
	f.resultWith(at, receiver, tenant, pool, run, attempt, state, metadata, reply, hashOf(run))
}

func (f *fixture) resultWith(at time.Time, receiver, tenant, pool, run string, attempt int64, state wire.TaskState, metadata wire.ResultMetadata, reply, hash string) {
	f.t.Helper()
	metadata.ReceiverID, metadata.Pool = receiver, pool
	encoded := must(f.t)(json.Marshal(metadata))
	result := &wire.Result{
		Task: wire.Task{ID: run, ContextID: run, Status: wire.TaskStatus{State: state, Timestamp: at.UTC().Format(time.RFC3339Nano), Message: &wire.Message{MessageID: "reply-" + run, Role: wire.RoleAgent, Parts: []wire.Part{{Text: reply}}, Extensions: []string{wire.ExtensionURI}}},
			Artifacts: []wire.Artifact{{ArtifactID: wire.ResultArtifact, Parts: []wire.Part{{Text: reply}}, Extensions: []string{wire.ExtensionURI}}}, History: []wire.Message{}, Metadata: encoded},
		Wherry: wire.ResultBinding{ReceiverBinding: wire.ReceiverBinding{Receiver: receiver, Tenant: tenant, Pool: pool, Run: run, Attempt: attempt}, TaskHash: hash},
	}
	f.produce(operator.TenantTopic(tenant), run, f.sign(result, f.keys[receiver]), at)
}

func (f *fixture) startFailure(at time.Time, receiver, tenant, pool, run string, attempt int64, class string) {
	f.t.Helper()
	f.startFailureWith(at, receiver, tenant, pool, run, attempt, class, "profile-a", agentA, hashOf(run))
}

func (f *fixture) startFailureWith(at time.Time, receiver, tenant, pool, run string, attempt int64, class, profile, agent, hash string) {
	f.t.Helper()
	failure := &wire.StartFailure{Wherry: wire.ReceiverBinding{Receiver: receiver, Tenant: tenant, Pool: pool, Run: run, Attempt: attempt}, Profile: profile, Agent: agent, Class: class, TaskHash: hash}
	f.produce(operator.PoolEventsTopic(pool), run, f.sign(failure, f.keys[receiver]), at)
}

// progress writes holder's progress record for run; a negative offset writes the tombstone.
func (f *fixture) progress(at time.Time, holder, run, tenant string, attempt, offset int64) {
	f.t.Helper()
	if offset < 0 {
		f.produce(operator.ReceiverProgressTopic(holder), run, nil, at)
		return
	}
	f.produce(operator.ReceiverProgressTopic(holder), run, must(f.t)(wire.EncodeProgress(wire.Progress{Tenant: tenant, TaskHash: hashOf(run), Attempt: attempt, LogOffset: offset})), at)
}

func (f *fixture) advertise(at time.Time, receiver, pool string, capacity, inFlight int64) {
	f.t.Helper()
	ad := &wire.Advertisement{Receiver: receiver, Pool: pool, Offers: []wire.Offer{{Profile: "profile-a", Agent: agentA, Replicas: capacity, InFlight: inFlight}}, Capacity: capacity, InFlight: inFlight, AtMS: at.UnixMilli()}
	f.produce(operator.PoolReceiversTopic(pool), receiver, f.sign(ad, f.keys[receiver]), at)
}

func (f *fixture) cancel(at time.Time, tenant, host, pool, run string, key ed25519.PrivateKey) {
	f.t.Helper()
	if key == nil {
		key = f.keys[signed.HostOwner(tenant, host)]
	}
	f.produce(operator.PoolEventsTopic(pool), run, f.sign(&wire.Cancel{Tenant: tenant, Host: host, Pool: pool, Run: run}, key), at)
}

// snapshot writes custodian's snapshot of pool with a fresh P-256 key and returns the key ID.
func (f *fixture) snapshot(at time.Time, pool, custodian string) string {
	f.t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		f.t.Fatal(err)
	}
	id := signed.KeyThumbprint(&key.PublicKey)
	snapshot := &wire.Snapshot{Pool: pool, Custodian: custodian, Reason: "bootstrap", Current: wire.PoolKey{KeyID: id, Key: jose.JSONWebKey{Key: &key.PublicKey}}}
	f.produce(operator.CustodianTopic(pool), wire.CustodianKey, f.sign(snapshot, f.keys[custodian]), at)
	return id
}

func (f *fixture) openRequest(at time.Time, receiver, pool, run string, attempt int64) string {
	f.t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	value := f.sign(&wire.OpenRequest{Run: run, Attempt: attempt, Receiver: receiver, TaskHash: hashOf(run), Key: wire.NewAttemptKey(&key.PublicKey)}, f.keys[receiver])
	f.produce(operator.PoolEventsTopic(pool), run, value, at)
	return wire.Digest(value)
}

func (f *fixture) wrap(at time.Time, custodian, pool, run, request, receiver string, attempt int64) {
	f.t.Helper()
	value := f.sign(&wire.AttemptWrap{Custodian: custodian, TaskHash: hashOf(run), Request: request, Receiver: receiver, Attempt: attempt, JWE: "jwe"}, f.keys[custodian])
	f.produce(operator.PoolEventsTopic(pool), run, value, at)
}

func (f *fixture) refusal(at time.Time, custodian, pool, run, request, reason string) {
	f.t.Helper()
	value := f.sign(&wire.OpenRefusal{Custodian: custodian, Request: request, Reason: reason}, f.keys[custodian])
	f.produce(operator.PoolEventsTopic(pool), run, value, at)
}

func (f *fixture) decision(at time.Time, custodian, pool, run, claim, previous, reason string, served bool) {
	f.t.Helper()
	value := f.sign(&wire.ClaimDecision{Custodian: custodian, Claim: claim, Previous: previous, Reason: reason, Served: served}, f.keys[custodian])
	f.produce(operator.PoolEventsTopic(pool), run, value, at)
}

// clock is the view's clock: the fixture's now, which advance moves.
func (f *fixture) clock() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fixture) advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

// testCaps are DefaultCaps with small batches, so the bounded reads take several calls on a small log.
func testCaps() Caps {
	caps := DefaultCaps
	caps.BatchRecords, caps.BatchBytes = 7, 1<<20
	return caps
}

// view builds a view over the fixture's cluster with window, rebuilt.
func (f *fixture) view(window time.Duration, caps Caps) *View {
	f.t.Helper()
	v := New(f.client, Options{Window: window, Caps: caps, Now: f.clock, Tick: 50 * time.Millisecond, Refresh: 50 * time.Millisecond})
	f.t.Cleanup(v.Close)
	ctx, cancel := context.WithTimeout(f.ctx, 60*time.Second)
	defer cancel()
	if err := v.Rebuild(ctx); err != nil {
		f.t.Fatalf("rebuild: %v", err)
	}
	return v
}

func (f *fixture) tick(v *View) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 30*time.Second)
	defer cancel()
	if err := v.tick(ctx, false); err != nil {
		f.t.Fatalf("tick: %v", err)
	}
}

// summaryOf is run id's summary in s, failing when absent.
func summaryOf(t *testing.T, s Snapshot, id string) RunSummary {
	t.Helper()
	for _, r := range s.Runs {
		if r.ID == id {
			return r
		}
	}
	t.Fatalf("run %s is not in the snapshot of %d runs", id, len(s.Runs))
	return RunSummary{}
}

func hasRun(s Snapshot, id string) bool {
	for _, r := range s.Runs {
		if r.ID == id {
			return true
		}
	}
	return false
}

func receiverOf(t *testing.T, s Snapshot, name string) Receiver {
	t.Helper()
	for _, r := range s.Receivers {
		if r.Name == name {
			return r
		}
	}
	t.Fatalf("receiver %s is not in the snapshot", name)
	return Receiver{}
}

func poolOf(t *testing.T, s Snapshot, name string) Pool {
	t.Helper()
	for _, p := range s.Pools {
		if p.Name == name {
			return p
		}
	}
	t.Fatalf("pool %s is not in the snapshot", name)
	return Pool{}
}

// ch describes a transcript chunk to sign: the line at seq of attempt, text "line <seq>" unless part is given, the
// empty final chunk when last is set with no part.
type ch struct {
	receiver, tenant, pool, run string
	attempt, seq                int64
	part                        *wire.Part
	last                        bool
	hash                        string
	key                         ed25519.PrivateKey
}

// chunkValue is the chunk c describes, signed with the key c names or its receiver's.
func (f *fixture) chunkValue(c ch) []byte {
	f.t.Helper()
	if c.attempt == 0 {
		c.attempt = 1
	}
	if c.hash == "" {
		c.hash = hashOf(c.run)
	}
	if c.key == nil {
		c.key = f.keys[c.receiver]
	}
	parts := []wire.Part{}
	switch {
	case c.part != nil:
		parts = append(parts, *c.part)
	case !c.last:
		parts = append(parts, wire.Part{Text: "line " + strconv.FormatInt(c.seq, 10), MediaType: "application/x-ndjson"})
	}
	chunk := &wire.Chunk{
		ArtifactUpdate: wire.TaskArtifactUpdateEvent{TaskID: c.run, ContextID: c.run, Artifact: wire.Artifact{ArtifactID: wire.TranscriptArtifact, Parts: parts, Extensions: []string{wire.ExtensionURI}}, Append: c.seq > 0, LastChunk: c.last},
		Wherry:         wire.ChunkBinding{ReceiverBinding: wire.ReceiverBinding{Receiver: c.receiver, Tenant: c.tenant, Pool: c.pool, Run: c.run, Attempt: c.attempt}, TaskHash: c.hash, Seq: c.seq},
	}
	return f.sign(chunk, c.key)
}

// chunk writes the chunk c describes to tenant's transcript topic, keyed by its run, stamped at; topicTenant names
// another tenant's topic when set.
func (f *fixture) chunk(at time.Time, c ch) *kgo.Record {
	f.t.Helper()
	return f.produce(operator.TenantTranscriptTopic(c.tenant), c.run, f.chunkValue(c), at)
}

// output writes receiver's output chunk of name holding data for run to tenant's transcript topic.
func (f *fixture) output(at time.Time, receiver, tenant, pool, run, name string, data []byte) {
	f.t.Helper()
	metadata := must(f.t)(json.Marshal(wire.OutputMetadata{Index: 0}))
	output := &wire.Output{
		ArtifactUpdate: wire.TaskArtifactUpdateEvent{TaskID: run, ContextID: run, LastChunk: true, Metadata: metadata,
			Artifact: wire.Artifact{ArtifactID: name, Name: name, Parts: []wire.Part{{Raw: wire.StdBytes(data), Filename: name, MediaType: wire.OutputMediaType}}, Extensions: []string{wire.ExtensionURI}}},
		Wherry: wire.ResultBinding{ReceiverBinding: wire.ReceiverBinding{Receiver: receiver, Tenant: tenant, Pool: pool, Run: run, Attempt: 1}, TaskHash: hashOf(run)},
	}
	f.produce(operator.TenantTranscriptTopic(tenant), run, f.sign(output, f.keys[receiver]), at)
}

// produceMany writes records together, in order, and waits for them all.
func (f *fixture) produceMany(records []*kgo.Record) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 2*time.Minute)
	defer cancel()
	var failed error
	var mu sync.Mutex
	for _, record := range records {
		f.client.Produce(ctx, record, func(_ *kgo.Record, err error) {
			if err != nil {
				mu.Lock()
				failed = err
				mu.Unlock()
			}
		})
	}
	if err := f.client.Flush(ctx); err != nil {
		f.t.Fatal(err)
	}
	if failed != nil {
		f.t.Fatalf("produce: %v", failed)
	}
}

// lines writes n text chunks of attempt for run, seq 0, 10, 20 and so on, each stamped a millisecond apart from at.
func (f *fixture) lines(at time.Time, receiver, tenant, pool, run string, attempt int64, n int) {
	f.t.Helper()
	var records []*kgo.Record
	for i := range n {
		records = append(records, &kgo.Record{Topic: operator.TenantTranscriptTopic(tenant), Key: []byte(run), Value: f.chunkValue(ch{receiver: receiver, tenant: tenant, pool: pool, run: run, attempt: attempt, seq: int64(i) * 10}), Timestamp: at.Add(time.Duration(i) * time.Millisecond)})
	}
	f.produceMany(records)
}

// transcriptCaps are DefaultCaps: the bounded reads of a transcript scan take whole batches.
func transcriptCaps() Caps { return DefaultCaps }
