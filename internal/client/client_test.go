package client

import (
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/trust"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

type clientFixture struct {
	ctx           context.Context
	kafka         *kgo.Client
	client        *Client
	userPublicKey ed25519.PublicKey
	userKey       ed25519.PrivateKey
	receiverKey   ed25519.PrivateKey
	sealingKey    *ecdh.PrivateKey
	request       []byte
}

func newClientFixture(t *testing.T, approve bool) clientFixture {
	t.Helper()
	cluster := kfake.MustCluster(kfake.NumBrokers(1))
	t.Cleanup(cluster.Close)
	kafka, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.FetchMaxWait(10*time.Millisecond),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.KeepControlRecords(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kafka.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	if err := wire.ApplyTopics(ctx, kadm.NewClient(kafka), 1); err != nil {
		t.Fatal(err)
	}
	created, err := kadm.NewClient(kafka).CreateTopics(ctx, 1, 1, nil, "replies.test")
	if err != nil {
		t.Fatal(err)
	}
	if created["replies.test"].Err != nil {
		t.Fatal(created["replies.test"].Err)
	}
	keyPath := filepath.Join(t.TempDir(), "user.key")
	fingerprint, err := InitUserKey(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	userKey, err := trust.LoadSigningKeyFile(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	userPublicKey := userKey.Public().(ed25519.PublicKey)
	if fingerprint != wire.Fingerprint(userPublicKey) {
		t.Fatalf("init printed fingerprint %q, want %q", fingerprint, wire.Fingerprint(userPublicKey))
	}
	_, receiverKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sealingKey, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	request, err := trust.NewEnrolRequest(
		"receiver-a", receiverKey, sealingKey, map[string]string{}, testDetected(), time.Now(),
	)
	if err != nil {
		t.Fatal(err)
	}
	fixture := clientFixture{
		ctx: ctx, kafka: kafka, client: New(kafka, userKey, "replies.test"),
		userPublicKey: userPublicKey, userKey: userKey,
		receiverKey: receiverKey, sealingKey: sealingKey, request: request,
	}
	fixture.produce(t, wire.TopicState, "receiver-a", request)
	if approve {
		result, err := fixture.client.Approve(
			ctx, trust.EnvelopeHash(request), "default", map[string]string{}, testGrants(), testSelector(), "auto", 30,
		)
		if err != nil {
			t.Fatal(err)
		}
		if result.ReceiverID != "receiver-a" || result.Generation != 1 {
			t.Fatalf("unexpected approval result: %#v", result)
		}
	}
	return fixture
}

func (fixture clientFixture) produce(t *testing.T, topic, key string, value []byte) {
	t.Helper()
	if err := fixture.kafka.ProduceSync(fixture.ctx, &kgo.Record{
		Topic: topic, Key: []byte(key), Value: value,
	}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func (fixture clientFixture) capability(t *testing.T, mutate func(*wire.Capability)) {
	t.Helper()
	capability := &wire.Capability{
		ReceiverID: "receiver-a", Pool: "default", Labels: map[string]string{},
		Offers:   []wire.Offer{{Adapter: "lab", Labels: map[string]string{"runtime": "exec", "nomad.node_pool": "OCPv"}}},
		Selector: testSelector(), Capacity: 3, InFlight: 1, Generation: 1, AtMS: time.Now().UnixMilli(),
	}
	if mutate != nil {
		mutate(capability)
	}
	fixture.produceCapability(t, capability.ReceiverID, capability, fixture.receiverKey)
}

func testSelector() []wire.Requirement {
	return []wire.Requirement{{Key: "region", Operator: wire.OpIn, Values: []string{"eu"}}}
}

func testDetected() []wire.DetectedAdapter {
	return []wire.DetectedAdapter{
		{Name: "lab", Platform: "nomad", Offers: []wire.Offer{{Adapter: "lab", Labels: map[string]string{"runtime": "exec", "nomad.node_pool": "OCPv"}}}},
		{Name: "cloud", Platform: "aws", Offers: []wire.Offer{{Adapter: "cloud", Labels: map[string]string{"runtime": "lambda"}}}},
	}
}

func testGrants() []wire.AdapterGrant {
	return []wire.AdapterGrant{
		{Name: "lab", Platform: "nomad", Allowed: map[string][]string{"runtime": {"exec", "podman"}, "nomad.node_pool": {"OCPv", "default"}}, Defaults: map[string]string{}},
		{Name: "cloud", Platform: "aws", Allowed: map[string][]string{"runtime": {"lambda", "exec"}}, Defaults: map[string]string{}},
	}
}

func singleGrant() []wire.AdapterGrant {
	return []wire.AdapterGrant{{Name: "lab", Platform: "nomad", Allowed: map[string][]string{"runtime": {"exec"}}, Defaults: map[string]string{}}}
}

func (fixture clientFixture) addReceiver(t *testing.T, receiverID, pool string, labels map[string]string, selector []wire.Requirement) ed25519.PrivateKey {
	t.Helper()
	_, signingKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sealingKey, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	detected := []wire.DetectedAdapter{{Name: "lab", Platform: "nomad", Offers: []wire.Offer{{Adapter: "lab", Labels: map[string]string{"runtime": "exec"}}}}}
	request, err := trust.NewEnrolRequest(receiverID, signingKey, sealingKey, labels, detected, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	fixture.produce(t, wire.TopicState, receiverID, request)
	if _, err := fixture.client.Approve(fixture.ctx, trust.EnvelopeHash(request), pool, labels, singleGrant(), selector, "auto", 30); err != nil {
		t.Fatal(err)
	}
	return signingKey
}

func TestApproveGrantMustNameDetectedAdapter(t *testing.T) {
	for name, grant := range map[string]wire.AdapterGrant{
		"unknown name":   {Name: "other", Platform: "nomad", Allowed: map[string][]string{}, Defaults: map[string]string{}},
		"wrong platform": {Name: "lab", Platform: "aws", Allowed: map[string][]string{}, Defaults: map[string]string{}},
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newClientFixture(t, false)
			before := fixture.topicEnd(t, wire.TopicState)
			_, err := fixture.client.Approve(fixture.ctx, trust.EnvelopeHash(fixture.request), "", map[string]string{}, []wire.AdapterGrant{grant}, []wire.Requirement{}, "auto", 30)
			if err == nil || !strings.Contains(err.Error(), "detected adapter") {
				t.Fatalf("got %v, want grant rejected", err)
			}
			if after := fixture.topicEnd(t, wire.TopicState); after != before {
				t.Fatalf("receivers.state changed from offset %d to %d", before, after)
			}
		})
	}
}

func TestApproveDefaultsPoolToReceiverID(t *testing.T) {
	fixture := newClientFixture(t, false)
	if _, err := fixture.client.Approve(fixture.ctx, trust.EnvelopeHash(fixture.request), "", map[string]string{}, testGrants(), []wire.Requirement{}, "auto", 30); err != nil {
		t.Fatal(err)
	}
	records, err := fixture.client.readBounded(fixture.ctx, wire.TopicState)
	if err != nil {
		t.Fatal(err)
	}
	state := trust.NewState(fixture.userPublicKey)
	for _, record := range records {
		if _, err := state.Apply(record.Value); err != nil {
			t.Fatal(err)
		}
	}
	enrolled, err := state.Receiver("receiver-a", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if enrolled.Pool != "receiver-a" {
		t.Fatalf("pool %q, want the receiver ID", enrolled.Pool)
	}
}

func (fixture clientFixture) produceCapability(t *testing.T, key string, capability *wire.Capability, signingKey ed25519.PrivateKey) {
	t.Helper()
	fixture.produce(t, wire.TopicCapability, key, signed(t, wire.TypeCapability, capability, signingKey))
}

func (fixture clientFixture) submit(t *testing.T) SubmitResult {
	t.Helper()
	result, err := fixture.client.Submit(fixture.ctx, SubmitInput{
		Selector: []wire.Requirement{{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}}},
		Labels:   map[string]string{"region": "eu"},
		Prompt:   "do the work", Deadline: 30 * time.Minute, QueueDeadline: 10 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestClientEndToEnd(t *testing.T) {
	fixture := newClientFixture(t, true)
	enrolments, skipped, err := fixture.client.Enrolments(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if skipped != 0 || len(enrolments) != 1 || !enrolments[0].Approved || enrolments[0].Generation != 1 {
		t.Fatalf("unexpected enrolments: %#v, skipped %d", enrolments, skipped)
	}
	fixture.capability(t, nil)
	submission := fixture.submit(t)
	if !strings.HasPrefix(submission.RunID, "run-") || submission.Pool != "default" {
		t.Fatalf("unexpected submission: %#v", submission)
	}
	tasks, err := fixture.client.readBounded(fixture.ctx, wire.TopicTask)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || string(tasks[0].Key) != submission.RunID {
		t.Fatalf("got %d tasks for %q", len(tasks), submission.RunID)
	}
	task, err := trust.VerifyRootTask(tasks[0].Value, fixture.userPublicKey)
	if err != nil {
		t.Fatal(err)
	}
	reply := &wire.Reply{
		RunID: task.RunID, Attempt: 1, TaskHash: trust.EnvelopeHash(tasks[0].Value),
		CorrelationID: task.CorrelationID, State: wire.TaskStateCompleted,
		Artifacts: []wire.Artifact{{ArtifactID: "answer", Parts: []wire.Part{{Text: "finished"}}}},
		AtMS:      time.Now().UnixMilli(),
	}
	fixture.produce(t, "replies.test", task.RunID, signed(t, wire.TypeReply, reply, fixture.receiverKey))
	awaited, err := fixture.client.Await(fixture.ctx, task.RunID, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if awaited.Reply.State != wire.TaskStateCompleted || awaited.Reply.Artifacts[0].Parts[0].Text != "finished" {
		t.Fatalf("unexpected reply: %#v", awaited.Reply)
	}

	event := &wire.Event{
		RunID: task.RunID, Attempt: 1, TaskHash: trust.EnvelopeHash(tasks[0].Value),
		ReceiverID: "receiver-a", AtMS: time.Now().UnixMilli(),
		State: wire.TaskStateCompleted, Kind: "terminal",
	}
	fixture.produce(t, wire.TopicEvent, task.RunID, signed(t, wire.TypeEvent, event, fixture.receiverKey))
	events, err := fixture.client.Events(fixture.ctx, task.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events.Events) != 1 || events.Events[0].Kind != "terminal" {
		t.Fatalf("unexpected events: %#v", events)
	}

	secretPath := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(secretPath, []byte("super secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	secret, err := fixture.client.PutSecret(fixture.ctx, "token", "receiver-a", secretPath)
	if err != nil {
		t.Fatal(err)
	}
	if secret.Version != 1 {
		t.Fatalf("got secret version %d, want 1", secret.Version)
	}
	stateRecords, err := fixture.client.readBounded(fixture.ctx, wire.TopicState)
	if err != nil {
		t.Fatal(err)
	}
	state := trust.NewState(fixture.userPublicKey)
	for _, record := range stateRecords {
		if _, err := state.Apply(record.Value); err != nil {
			t.Fatal(err)
		}
	}
	sealedRecords, err := fixture.client.readBounded(fixture.ctx, wire.TopicSecret)
	if err != nil {
		t.Fatal(err)
	}
	if len(sealedRecords) != 1 || string(sealedRecords[0].Key) != "receiver-a/token" {
		t.Fatalf("unexpected sealed-secret records: %#v", sealedRecords)
	}
	plaintext, _, err := trust.Open(
		sealedRecords[0].Value, state, "receiver-a", fixture.sealingKey,
		fixture.userPublicKey, time.Now(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if string(plaintext) != "super secret" {
		t.Fatalf("got plaintext %q", plaintext)
	}
}

func TestSubmitNoMatchingReceiverProducesNothing(t *testing.T) {
	fixture := newClientFixture(t, true)
	fixture.capability(t, nil)
	before := fixture.topicEnd(t, wire.TopicTask)
	_, err := fixture.client.Submit(fixture.ctx, SubmitInput{
		Selector: []wire.Requirement{{Key: "runtime", Operator: wire.OpIn, Values: []string{"podman"}}}, Labels: map[string]string{"region": "eu"},
		Prompt: "work", Deadline: time.Minute, QueueDeadline: time.Minute,
	})
	if err == nil {
		t.Fatal("submit without a matching receiver succeeded")
	}
	if after := fixture.topicEnd(t, wire.TopicTask); after != before {
		t.Fatalf("tasks.queue changed from offset %d to %d", before, after)
	}
}

func TestSubmitMatchesOffersNotFlattenedSets(t *testing.T) {
	fixture := newClientFixture(t, true)
	fixture.capability(t, func(capability *wire.Capability) {
		capability.Offers = []wire.Offer{
			{Adapter: "lab", Labels: map[string]string{"runtime": "exec", "nomad.node_pool": "OCPv"}},
			{Adapter: "lab", Labels: map[string]string{"runtime": "podman", "nomad.node_pool": "default"}},
		}
	})
	_, err := fixture.client.Submit(fixture.ctx, SubmitInput{
		Selector: []wire.Requirement{
			{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}},
			{Key: "nomad.node_pool", Operator: wire.OpIn, Values: []string{"default"}},
		},
		Labels: map[string]string{"region": "eu"}, Prompt: "work", Deadline: time.Minute, QueueDeadline: time.Minute,
	})
	if err == nil {
		t.Fatal("submit matched values from different offers")
	}
}

func TestSubmitMatchesAcrossAdapters(t *testing.T) {
	fixture := newClientFixture(t, true)
	fixture.capability(t, func(capability *wire.Capability) {
		capability.Offers = []wire.Offer{
			{Adapter: "lab", Labels: map[string]string{"runtime": "exec", "nomad.node_pool": "OCPv"}},
			{Adapter: "cloud", Labels: map[string]string{"runtime": "lambda"}},
		}
	})
	submit := func(selector []wire.Requirement) error {
		_, err := fixture.client.Submit(fixture.ctx, SubmitInput{
			Selector: selector, Labels: map[string]string{"region": "eu"},
			Prompt: "work", Deadline: time.Minute, QueueDeadline: time.Minute,
		})
		return err
	}
	if err := submit([]wire.Requirement{{Key: "platform", Operator: wire.OpIn, Values: []string{"aws"}}, {Key: "runtime", Operator: wire.OpIn, Values: []string{"lambda"}}}); err != nil {
		t.Fatalf("second adapter's offer did not match: %v", err)
	}
	if err := submit([]wire.Requirement{{Key: "platform", Operator: wire.OpIn, Values: []string{"aws"}}, {Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}}}); err == nil {
		t.Fatal("matched exec on aws, which only the nomad adapter offers")
	}
}

func TestSubmitIgnoresOneWayMatch(t *testing.T) {
	fixture := newClientFixture(t, true)
	fixture.capability(t, nil)
	before := fixture.topicEnd(t, wire.TopicTask)
	_, err := fixture.client.Submit(fixture.ctx, SubmitInput{
		Selector: []wire.Requirement{{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}}}, Labels: map[string]string{"region": "us"},
		Prompt: "work", Deadline: time.Minute, QueueDeadline: time.Minute,
	})
	if err == nil {
		t.Fatal("one-way match was accepted")
	}
	if after := fixture.topicEnd(t, wire.TopicTask); after != before {
		t.Fatalf("tasks.queue changed from offset %d to %d", before, after)
	}
}

func TestSubmitIgnoresStaleCapability(t *testing.T) {
	fixture := newClientFixture(t, true)
	fixture.capability(t, func(capability *wire.Capability) {
		capability.AtMS = time.Now().Add(-121 * time.Second).UnixMilli()
	})
	if _, err := fixture.client.Submit(fixture.ctx, SubmitInput{
		Selector: []wire.Requirement{{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}}}, Labels: map[string]string{"region": "eu"},
		Prompt: "work", Deadline: time.Minute, QueueDeadline: time.Minute,
	}); err == nil {
		t.Fatal("stale capability was accepted")
	}
}

func TestSubmitIgnoresCapabilityBeyondEnrolment(t *testing.T) {
	fixture := newClientFixture(t, true)
	fixture.capability(t, func(capability *wire.Capability) {
		capability.Labels["lab/arch"] = "arm64"
	})
	if _, err := fixture.client.Submit(fixture.ctx, SubmitInput{
		Selector: []wire.Requirement{{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}}}, Labels: map[string]string{"region": "eu"},
		Prompt: "work", Deadline: time.Minute, QueueDeadline: time.Minute,
	}); err == nil {
		t.Fatal("capability beyond enrolment was accepted")
	}
}

func TestReceiversListsLiveStaleAndSilent(t *testing.T) {
	fixture := newClientFixture(t, true)
	fixture.capability(t, nil)
	batchKey := fixture.addReceiver(t, "receiver-b", "batch", map[string]string{}, testSelector())
	fixture.addReceiver(t, "receiver-c", "quiet", map[string]string{}, testSelector())
	fixture.produceCapability(t, "receiver-b", &wire.Capability{
		ReceiverID: "receiver-b", Pool: "batch",
		Labels: map[string]string{}, Offers: []wire.Offer{{Adapter: "lab", Labels: map[string]string{"runtime": "exec"}}}, Selector: testSelector(),
		Capacity: 4, InFlight: 2, Generation: 1, AtMS: time.Now().Add(-121 * time.Second).UnixMilli(),
	}, batchKey)

	receivers, skipped, err := fixture.client.Receivers(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if skipped != 0 || len(receivers) != 3 {
		t.Fatalf("got %d receivers and %d skipped, want 3 and 0", len(receivers), skipped)
	}
	if receivers[0].ReceiverID != "receiver-b" || receivers[0].Status != "stale" || receivers[0].Capacity != 4 || receivers[0].InFlight != 2 {
		t.Fatalf("unexpected stale receiver: %#v", receivers[0])
	}
	if receivers[1].ReceiverID != "receiver-a" || receivers[1].Status != "live" {
		t.Fatalf("unexpected live receiver: %#v", receivers[1])
	}
	if receivers[2].ReceiverID != "receiver-c" || receivers[2].Status != "silent" || receivers[2].HeartbeatAge != "-" {
		t.Fatalf("unexpected silent receiver: %#v", receivers[2])
	}
}

func TestReceiversSkipsUnverifiableCapability(t *testing.T) {
	fixture := newClientFixture(t, true)
	_, unenrolledKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	fixture.produceCapability(t, "unenrolled", &wire.Capability{
		ReceiverID: "unenrolled", Pool: "default", Labels: map[string]string{}, Offers: []wire.Offer{}, Selector: []wire.Requirement{},
		Capacity: 1, Generation: 1, AtMS: time.Now().UnixMilli(),
	}, unenrolledKey)
	fixture.capability(t, func(capability *wire.Capability) {
		capability.Labels["lab/arch"] = "arm64"
	})

	receivers, skipped, err := fixture.client.Receivers(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if skipped != 2 {
		t.Fatalf("got skipped count %d, want 2", skipped)
	}
	if len(receivers) != 1 || receivers[0].Status != "silent" {
		t.Fatalf("unexpected receivers: %#v", receivers)
	}
}

func TestReceiversShowsOffers(t *testing.T) {
	fixture := newClientFixture(t, false)
	labels := map[string]string{"lab/site": "home"}
	key := fixture.addReceiver(t, "receiver-nomad", "nomad", labels, testSelector())
	offers := []wire.Offer{{Adapter: "lab", Labels: map[string]string{"runtime": "exec"}}}
	fixture.produceCapability(t, "receiver-nomad", &wire.Capability{
		ReceiverID: "receiver-nomad", Pool: "nomad", Labels: labels, Offers: offers, Selector: testSelector(),
		Capacity: 1, Generation: 1, AtMS: time.Now().UnixMilli(),
	}, key)

	receivers, _, err := fixture.client.Receivers(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(receivers) != 1 || receivers[0].Pool != "nomad" || !reflect.DeepEqual(receivers[0].Offers, offers) || !reflect.DeepEqual(receivers[0].Adapters, singleGrant()) {
		t.Fatalf("unexpected receivers: %#v", receivers)
	}
}

func TestSubmitChoosesLeastLoadedAmongMatches(t *testing.T) {
	fixture := newClientFixture(t, true)
	fixture.capability(t, func(capability *wire.Capability) {
		capability.Capacity = 10
		capability.InFlight = 0
	})
	namedKey := fixture.addReceiver(t, "receiver-b", "named", map[string]string{}, testSelector())
	fixture.produceCapability(t, "receiver-b", &wire.Capability{
		ReceiverID: "receiver-b", Pool: "named",
		Labels: map[string]string{}, Offers: []wire.Offer{{Adapter: "lab", Labels: map[string]string{"runtime": "exec"}}}, Selector: testSelector(),
		Capacity: 2, InFlight: 1, Generation: 1, AtMS: time.Now().UnixMilli(),
	}, namedKey)

	result, err := fixture.client.Submit(fixture.ctx, SubmitInput{
		Selector: []wire.Requirement{{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}}}, Labels: map[string]string{"region": "eu"},
		Prompt: "work", Deadline: time.Minute, QueueDeadline: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Pool != "default" {
		t.Fatalf("got pool %q, want least-loaded default", result.Pool)
	}
}

func TestAwaitIgnoresReplyFromUnenrolledKey(t *testing.T) {
	fixture := newClientFixture(t, true)
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	reply := testReply("run-unenrolled", 1, wire.TaskStateCompleted)
	fixture.produce(t, "replies.test", reply.RunID, signed(t, wire.TypeReply, reply, key))
	if _, err := fixture.client.Await(fixture.ctx, reply.RunID, 100*time.Millisecond); err == nil {
		t.Fatal("reply from unenrolled key was accepted")
	}
}

func TestAwaitIgnoresLowerAttemptReply(t *testing.T) {
	fixture := newClientFixture(t, true)
	runID := "run-attempts"
	fixture.produce(t, "replies.test", runID, signed(t, wire.TypeReply, testReply(runID, 2, wire.TaskStateWorking), fixture.receiverKey))
	fixture.produce(t, "replies.test", runID, signed(t, wire.TypeReply, testReply(runID, 1, wire.TaskStateCompleted), fixture.receiverKey))
	if _, err := fixture.client.Await(fixture.ctx, runID, 100*time.Millisecond); err == nil {
		t.Fatal("lower-attempt terminal reply was accepted")
	}
}

func TestStateLoadSkipsUnsignedRecords(t *testing.T) {
	fixture := newClientFixture(t, false)
	fixture.produce(t, wire.TopicState, "unsigned", []byte(`{"not":"an envelope"}`))
	enrolments, skipped, err := fixture.client.Enrolments(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(enrolments) != 1 {
		t.Fatalf("got %d verified enrolments, want 1", len(enrolments))
	}
	if skipped != 1 {
		t.Fatalf("got skipped count %d, want 1", skipped)
	}
}

func TestConfigRejectsUnknownField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	encoded := `{"broker":"localhost:9092","ca_file":"ca.pem","kafka_user":"user","kafka_password_file":"password","unknown":true}`
	if err := os.WriteFile(path, []byte(encoded), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("config with unknown field was accepted")
	}
}

func TestConnectErrorsOmitPassword(t *testing.T) {
	const password = "kafka-password-secret"
	dir := t.TempDir()
	server := httptest.NewTLSServer(nil)
	server.Close()
	config := Config{
		Broker: "localhost:9092", KafkaUser: "user",
		CAFile:            filepath.Join(dir, "ca.pem"),
		KafkaPasswordFile: filepath.Join(dir, "password"),
		UserKey:           filepath.Join(dir, "missing.key"),
	}
	if err := os.WriteFile(config.CAFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(config.KafkaPasswordFile, []byte(password+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Connect(config); err == nil || strings.Contains(err.Error(), password) {
		t.Fatalf("missing user key: %v", err)
	}
	config.UserKey = filepath.Join(dir, "user.key")
	if _, err := InitUserKey(config.UserKey); err != nil {
		t.Fatal(err)
	}
	config.Broker = "localhost:not-a-port"
	if _, err := Connect(config); err == nil || strings.Contains(err.Error(), password) {
		t.Fatalf("bad broker: %v", err)
	}
}

func (fixture clientFixture) topicEnd(t *testing.T, topic string) int64 {
	t.Helper()
	offsets, err := kadm.NewClient(fixture.kafka).ListEndOffsets(fixture.ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if err := offsets.Error(); err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, offset := range offsets[topic] {
		if offset.Partition >= 0 {
			total += offset.Offset
		}
	}
	return total
}

func signed(t *testing.T, recordType wire.RecordType, value any, key ed25519.PrivateKey) []byte {
	t.Helper()
	payload, err := wire.MarshalPayload(recordType, value)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := wire.MarshalEnvelope(wire.Sign(recordType, payload, key))
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

func testReply(runID string, attempt int64, state wire.TaskState) *wire.Reply {
	return &wire.Reply{
		RunID: runID, Attempt: attempt, TaskHash: strings.Repeat("a", 64),
		CorrelationID: "correlation", State: state, Artifacts: []wire.Artifact{},
		AtMS: time.Now().UnixMilli(),
	}
}
