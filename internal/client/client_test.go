package client

import (
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json/jsontext"
	"encoding/pem"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
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
			ctx, trust.EnvelopeHash(request), "default", map[string]string{}, testGrants(), "auto", 30*24*time.Hour,
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
		Capacity: 3, InFlight: 1, Generation: 1, AtMS: time.Now().UnixMilli(),
	}
	if mutate != nil {
		mutate(capability)
	}
	fixture.produceCapability(t, capability.ReceiverID, capability, fixture.receiverKey)
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

func (fixture clientFixture) addReceiver(t *testing.T, receiverID, pool string, labels map[string]string) ed25519.PrivateKey {
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
	if _, err := fixture.client.Approve(fixture.ctx, trust.EnvelopeHash(request), pool, labels, singleGrant(), "auto", 30*24*time.Hour); err != nil {
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
			_, err := fixture.client.Approve(fixture.ctx, trust.EnvelopeHash(fixture.request), "", map[string]string{}, []wire.AdapterGrant{grant}, "auto", 30*24*time.Hour)
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
	if _, err := fixture.client.Approve(fixture.ctx, trust.EnvelopeHash(fixture.request), "", map[string]string{}, testGrants(), "auto", 30*24*time.Hour); err != nil {
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
	verified, err := trust.VerifyRootSubmission(tasks[0].Value, fixture.userPublicKey)
	if err != nil {
		t.Fatal(err)
	}
	runID := verified.Wherry.RunID
	if message := verified.Message; message.TaskID != runID || message.ContextID != runID || !slices.Equal(message.Extensions, []string{wire.ExtensionURI}) {
		t.Fatalf("submitted message %#v does not carry the run ID, root context and extension", message)
	}
	task := testTask(verified, trust.EnvelopeHash(tasks[0].Value), 1, wire.TaskStateCompleted)
	task.Task.Artifacts = []wire.Artifact{{ArtifactID: "answer", Parts: []wire.Part{{Text: "finished"}}, Extensions: []string{wire.ExtensionURI}}}
	fixture.produce(t, "replies.test", runID, signed(t, wire.TypeTask, task, fixture.receiverKey))
	awaited, err := fixture.client.Await(fixture.ctx, runID, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if awaited.Task.Status.State != wire.TaskStateCompleted || awaited.Task.Artifacts[0].Parts[0].Text != "finished" {
		t.Fatalf("unexpected task: %#v", awaited.Task)
	}

	status := &wire.StatusRecord{
		StatusUpdate: wire.TaskStatusUpdateEvent{TaskID: runID, ContextID: runID, Status: wire.TaskStatus{
			State: wire.TaskStateCompleted, Timestamp: time.Now().UTC().Format(time.RFC3339),
		}},
		Wherry: wire.StatusExtension{Turn: 1, Attempt: 1, TaskHash: trust.EnvelopeHash(tasks[0].Value), ReceiverID: "receiver-a", Kind: "terminal"},
	}
	fixture.produce(t, wire.TopicEvent, runID, signed(t, wire.TypeStatus, status, fixture.receiverKey))
	for _, attempt := range []int64{1, 1, 2} {
		chunk := &wire.ArtifactUpdateRecord{
			ArtifactUpdate: wire.TaskArtifactUpdateEvent{TaskID: runID, ContextID: runID, Artifact: wire.Artifact{
				ArtifactID: "transcript", Parts: []wire.Part{{Text: "line"}}, Extensions: []string{wire.ExtensionURI},
			}},
			Wherry: wire.ArtifactUpdateExtension{Turn: 1, Attempt: attempt, Seq: 0},
		}
		fixture.produce(t, wire.TopicTranscript, runID, signed(t, wire.TypeArtifactUpdate, chunk, fixture.receiverKey))
	}
	events, err := fixture.client.Events(fixture.ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events.Statuses) != 1 || events.Statuses[0].Wherry.Kind != "terminal" || !reflect.DeepEqual(events.Chunks, map[TurnAttempt]int{{Turn: 1, Attempt: 1}: 2, {Turn: 1, Attempt: 2}: 1}) {
		t.Fatalf("unexpected events: %#v", events)
	}

	secretPath := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(secretPath, []byte("super secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	secret, err := fixture.client.PutSecret(fixture.ctx, "token", "receiver-a", secretPath, 30*24*time.Hour)
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
	plaintext, _, _, err := trust.Open(
		sealedRecords[0].Value, state, "receiver-a", "token", fixture.sealingKey,
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
		Selector: []wire.Requirement{{Key: "runtime", Operator: wire.OpIn, Values: []string{"podman"}}},
		Prompt:   "work", Deadline: time.Minute, QueueDeadline: time.Minute,
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
		Prompt: "work", Deadline: time.Minute, QueueDeadline: time.Minute,
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
			Selector: selector,
			Prompt:   "work", Deadline: time.Minute, QueueDeadline: time.Minute,
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

func TestSubmitIgnoresStaleCapability(t *testing.T) {
	fixture := newClientFixture(t, true)
	fixture.capability(t, func(capability *wire.Capability) {
		capability.AtMS = time.Now().Add(-121 * time.Second).UnixMilli()
	})
	if _, err := fixture.client.Submit(fixture.ctx, SubmitInput{
		Selector: []wire.Requirement{{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}}},
		Prompt:   "work", Deadline: time.Minute, QueueDeadline: time.Minute,
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
		Selector: []wire.Requirement{{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}}},
		Prompt:   "work", Deadline: time.Minute, QueueDeadline: time.Minute,
	}); err == nil {
		t.Fatal("capability beyond enrolment was accepted")
	}
}

func TestReceiversListsLiveStaleAndSilent(t *testing.T) {
	fixture := newClientFixture(t, true)
	fixture.capability(t, nil)
	batchKey := fixture.addReceiver(t, "receiver-b", "batch", map[string]string{})
	fixture.addReceiver(t, "receiver-c", "quiet", map[string]string{})
	fixture.produceCapability(t, "receiver-b", &wire.Capability{
		ReceiverID: "receiver-b", Pool: "batch",
		Labels: map[string]string{}, Offers: []wire.Offer{{Adapter: "lab", Labels: map[string]string{"runtime": "exec"}}},
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
		ReceiverID: "unenrolled", Pool: "default", Labels: map[string]string{}, Offers: []wire.Offer{},
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
	key := fixture.addReceiver(t, "receiver-nomad", "nomad", labels)
	offers := []wire.Offer{{Adapter: "lab", Labels: map[string]string{"runtime": "exec"}}}
	fixture.produceCapability(t, "receiver-nomad", &wire.Capability{
		ReceiverID: "receiver-nomad", Pool: "nomad", Labels: labels, Offers: offers,
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
	namedKey := fixture.addReceiver(t, "receiver-b", "named", map[string]string{})
	fixture.produceCapability(t, "receiver-b", &wire.Capability{
		ReceiverID: "receiver-b", Pool: "named",
		Labels: map[string]string{}, Offers: []wire.Offer{{Adapter: "lab", Labels: map[string]string{"runtime": "exec"}}},
		Capacity: 2, InFlight: 1, Generation: 1, AtMS: time.Now().UnixMilli(),
	}, namedKey)

	result, err := fixture.client.Submit(fixture.ctx, SubmitInput{
		Selector: []wire.Requirement{{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}}},
		Prompt:   "work", Deadline: time.Minute, QueueDeadline: time.Minute,
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
	submission, hash := fixture.rootSubmission(t, "run-unenrolled", "default")
	task := testTask(submission, hash, 1, wire.TaskStateCompleted)
	fixture.produce(t, "replies.test", task.Task.ID, signed(t, wire.TypeTask, task, key))
	if _, err := fixture.client.Await(fixture.ctx, task.Task.ID, 100*time.Millisecond); err == nil {
		t.Fatal("task from unenrolled key was accepted")
	}
}

func TestAwaitRejectsForgedTaskFromOtherPool(t *testing.T) {
	fixture := newClientFixture(t, true)
	otherPoolKey := fixture.addReceiver(t, "receiver-other", "other", map[string]string{})
	submission, hash := fixture.rootSubmission(t, "run-other-pool", "default")
	task := testTask(submission, hash, 1, wire.TaskStateCompleted)
	fixture.produce(t, "replies.test", task.Task.ID, signed(t, wire.TypeTask, task, otherPoolKey))
	if _, err := fixture.client.Await(fixture.ctx, task.Task.ID, 100*time.Millisecond); err == nil {
		t.Fatal("task forged by a receiver in another pool was accepted")
	}
}

func TestAwaitRejectsTaskHashMismatch(t *testing.T) {
	fixture := newClientFixture(t, true)
	submission, _ := fixture.rootSubmission(t, "run-hash-mismatch", "default")
	task := testTask(submission, strings.Repeat("a", 64), 1, wire.TaskStateCompleted)
	fixture.produce(t, "replies.test", task.Task.ID, signed(t, wire.TypeTask, task, fixture.receiverKey))
	if _, err := fixture.client.Await(fixture.ctx, task.Task.ID, 100*time.Millisecond); err == nil {
		t.Fatal("task with a mismatched submission hash was accepted")
	}
}

func TestAwaitHighAttemptDoesNotMaskResult(t *testing.T) {
	fixture := newClientFixture(t, true)
	submission, hash := fixture.rootSubmission(t, "run-attempts", "default")
	fixture.produce(t, "replies.test", submission.Wherry.RunID, signed(t, wire.TypeTask, testTask(submission, hash, 2, wire.TaskStateWorking), fixture.receiverKey))
	fixture.produce(t, "replies.test", submission.Wherry.RunID, signed(t, wire.TypeTask, testTask(submission, hash, 1, wire.TaskStateCompleted), fixture.receiverKey))
	awaited, err := fixture.client.Await(fixture.ctx, submission.Wherry.RunID, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if awaited.Task.Status.State != wire.TaskStateCompleted {
		t.Fatalf("state %s, want COMPLETED", awaited.Task.Status.State)
	}
}

func TestAwaitFailsClosedWithoutSubmission(t *testing.T) {
	fixture := newClientFixture(t, true)
	if _, err := fixture.client.Await(fixture.ctx, "run-missing", time.Second); err == nil || !strings.Contains(err.Error(), "verified root submission not found") {
		t.Fatalf("got %v, want missing verified submission error", err)
	}
}

func TestAwaitRejectsTaskLineageMismatch(t *testing.T) {
	for name, mutate := range map[string]func(*wire.TaskRecord){
		"context": func(task *wire.TaskRecord) { task.Task.ContextID = "run-other" },
		"history": func(task *wire.TaskRecord) { task.Task.History = []wire.Message{} },
		"message": func(task *wire.TaskRecord) { task.Task.History[0].MessageID = "msg-other" },
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newClientFixture(t, true)
			submission, hash := fixture.rootSubmission(t, "run-lineage-"+name, "default")
			task := testTask(submission, hash, 1, wire.TaskStateCompleted)
			mutate(task)
			fixture.produce(t, "replies.test", task.Task.ID, signed(t, wire.TypeTask, task, fixture.receiverKey))
			if _, err := fixture.client.Await(fixture.ctx, task.Task.ID, 100*time.Millisecond); err == nil {
				t.Fatal("task with mismatched lineage was accepted")
			}
		})
	}
}

func TestAwaitReturnsOnInterrupted(t *testing.T) {
	fixture := newClientFixture(t, true)
	submission, hash := fixture.rootSubmission(t, "run-auth", "default")
	status := &wire.StatusRecord{
		StatusUpdate: wire.TaskStatusUpdateEvent{
			TaskID: submission.Wherry.RunID, ContextID: submission.Wherry.Lineage.RootRunID,
			Status: wire.TaskStatus{
				State: wire.TaskStateAuthRequired, Timestamp: time.Now().UTC().Format(time.RFC3339),
				Message: &wire.Message{MessageID: "msg-status", Role: wire.RoleAgent, Parts: []wire.Part{{Text: "credential token required"}}, Extensions: []string{wire.ExtensionURI}},
			},
		},
		Wherry: wire.StatusExtension{Turn: 1, Attempt: 1, TaskHash: hash, ReceiverID: "receiver-a", Kind: "interrupted"},
	}
	fixture.produce(t, wire.TopicEvent, submission.Wherry.RunID, signed(t, wire.TypeStatus, status, fixture.receiverKey))
	awaited, err := fixture.client.Await(fixture.ctx, submission.Wherry.RunID, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if awaited.Task != nil || awaited.Status.State != wire.TaskStateAuthRequired {
		t.Fatalf("result %#v, want AUTH_REQUIRED without a task", awaited)
	}
}

func TestAwaitReturnsOnRejection(t *testing.T) {
	fixture := newClientFixture(t, true)
	submission, hash := fixture.rootSubmission(t, "run-rejected", "default")
	status := testStatus(submission, hash, "receiver-a", "rejected", wire.TaskStateRejected)
	fixture.produce(t, wire.TopicEvent, submission.Wherry.RunID, signed(t, wire.TypeStatus, status, fixture.receiverKey))
	awaited, err := fixture.client.Await(fixture.ctx, submission.Wherry.RunID, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if awaited.Task != nil || awaited.Status.State != wire.TaskStateRejected {
		t.Fatalf("result %#v, want REJECTED without a task", awaited)
	}
}

func TestAwaitReturnsOnTerminalStatus(t *testing.T) {
	fixture := newClientFixture(t, true)
	submission, hash := fixture.rootSubmission(t, "run-terminal-status", "default")
	status := testStatus(submission, hash, "receiver-a", "terminal", wire.TaskStateCompleted)
	fixture.produce(t, wire.TopicEvent, submission.Wherry.RunID, signed(t, wire.TypeStatus, status, fixture.receiverKey))
	awaited, err := fixture.client.Await(fixture.ctx, submission.Wherry.RunID, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if awaited.Task != nil || awaited.Status.State != wire.TaskStateCompleted {
		t.Fatalf("result %#v, want terminal status", awaited)
	}
}

func TestAwaitKeepsArtifactsWhenTaskFollowsStatus(t *testing.T) {
	fixture := newClientFixture(t, true)
	submission, hash := fixture.rootSubmission(t, "run-split-fetch", "default")
	status := testStatus(submission, hash, "receiver-a", "terminal", wire.TaskStateCompleted)
	fixture.produce(t, wire.TopicEvent, submission.Wherry.RunID, signed(t, wire.TypeStatus, status, fixture.receiverKey))
	task := testTask(submission, hash, 1, wire.TaskStateCompleted)
	go func() {
		time.Sleep(500 * time.Millisecond)
		fixture.produce(t, "replies.test", task.Task.ID, signed(t, wire.TypeTask, task, fixture.receiverKey))
	}()
	awaited, err := fixture.client.Await(fixture.ctx, submission.Wherry.RunID, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if awaited.Task == nil {
		t.Fatalf("result %#v, want the task record that followed the terminal status", awaited)
	}
}

func TestAwaitStatusBindings(t *testing.T) {
	for name, setup := range map[string]func(*testing.T, clientFixture, *wire.SubmissionRecord, string) (ed25519.PrivateKey, *wire.StatusRecord){
		"task hash": func(_ *testing.T, f clientFixture, submission *wire.SubmissionRecord, _ string) (ed25519.PrivateKey, *wire.StatusRecord) {
			return f.receiverKey, testStatus(submission, strings.Repeat("a", 64), "receiver-a", "interrupted", wire.TaskStateAuthRequired)
		},
		"receiver id": func(_ *testing.T, f clientFixture, submission *wire.SubmissionRecord, hash string) (ed25519.PrivateKey, *wire.StatusRecord) {
			return f.receiverKey, testStatus(submission, hash, "receiver-other", "interrupted", wire.TaskStateAuthRequired)
		},
		"turn": func(_ *testing.T, f clientFixture, submission *wire.SubmissionRecord, hash string) (ed25519.PrivateKey, *wire.StatusRecord) {
			status := testStatus(submission, hash, "receiver-a", "interrupted", wire.TaskStateAuthRequired)
			status.Wherry.Turn++
			return f.receiverKey, status
		},
		"pool": func(t *testing.T, f clientFixture, submission *wire.SubmissionRecord, hash string) (ed25519.PrivateKey, *wire.StatusRecord) {
			key := f.addReceiver(t, "receiver-other", "other", map[string]string{})
			return key, testStatus(submission, hash, "receiver-other", "interrupted", wire.TaskStateAuthRequired)
		},
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newClientFixture(t, true)
			submission, hash := fixture.rootSubmission(t, "run-await-status-binding", "default")
			key, status := setup(t, fixture, submission, hash)
			fixture.produce(t, wire.TopicEvent, submission.Wherry.RunID, signed(t, wire.TypeStatus, status, key))
			if _, err := fixture.client.Await(fixture.ctx, submission.Wherry.RunID, 100*time.Millisecond); err == nil {
				t.Fatalf("status with mismatched %s was accepted", name)
			}
		})
	}
}

func TestAwaitTaskRequiresLatestTurn(t *testing.T) {
	fixture := newClientFixture(t, true)
	submission, hash := fixture.rootSubmission(t, "run-task-turn", "default")
	task := testTask(submission, hash, 1, wire.TaskStateCompleted)
	task.Wherry.Turn++
	fixture.produce(t, "replies.test", submission.Wherry.RunID, signed(t, wire.TypeTask, task, fixture.receiverKey))
	if _, err := fixture.client.Await(fixture.ctx, submission.Wherry.RunID, 100*time.Millisecond); err == nil {
		t.Fatal("task from a non-latest turn was accepted")
	}
}

func TestAwaitEnforcesRecordTopics(t *testing.T) {
	t.Run("task only from reply topic", func(t *testing.T) {
		fixture := newClientFixture(t, true)
		submission, hash := fixture.rootSubmission(t, "run-task-topic", "default")
		fixture.produce(t, wire.TopicEvent, submission.Wherry.RunID, signed(t, wire.TypeTask, testTask(submission, hash, 1, wire.TaskStateCompleted), fixture.receiverKey))
		if _, err := fixture.client.Await(fixture.ctx, submission.Wherry.RunID, 100*time.Millisecond); err == nil {
			t.Fatal("task record from tasks.events was accepted")
		}
	})
	t.Run("status only from event topic", func(t *testing.T) {
		fixture := newClientFixture(t, true)
		submission, hash := fixture.rootSubmission(t, "run-status-topic", "default")
		status := testStatus(submission, hash, "receiver-a", "interrupted", wire.TaskStateAuthRequired)
		fixture.produce(t, "replies.test", submission.Wherry.RunID, signed(t, wire.TypeStatus, status, fixture.receiverKey))
		if _, err := fixture.client.Await(fixture.ctx, submission.Wherry.RunID, 100*time.Millisecond); err == nil {
			t.Fatal("status record from the reply topic was accepted")
		}
	})
}

func TestClientContinueRefusesDuplicateTurn(t *testing.T) {
	fixture := newClientFixture(t, true)
	first, hash := fixture.turnSubmission(t, "run-duplicate-turn", 1, "msg-1")
	status := testStatus(first, hash, "receiver-a", "interrupted", wire.TaskStateAuthRequired)
	fixture.produce(t, wire.TopicEvent, first.Wherry.RunID, signed(t, wire.TypeStatus, status, fixture.receiverKey))
	fixture.turnSubmission(t, first.Wherry.RunID, 2, "msg-2")
	if _, err := fixture.client.Continue(fixture.ctx, first.Wherry.RunID, ContinueInput{Prompt: "again", Deadline: time.Minute, QueueDeadline: time.Minute}); err == nil || !strings.Contains(err.Error(), "already submitted") {
		t.Fatalf("Continue = %v, want duplicate-turn refusal", err)
	}
}

func TestClientContinueRequiresInterruptedTask(t *testing.T) {
	fixture := newClientFixture(t, true)
	first, hash := fixture.turnSubmission(t, "run-working", 1, "msg-1")
	status := testStatus(first, hash, "receiver-a", "claimed", wire.TaskStateWorking)
	fixture.produce(t, wire.TopicEvent, first.Wherry.RunID, signed(t, wire.TypeStatus, status, fixture.receiverKey))
	if _, err := fixture.client.Continue(fixture.ctx, first.Wherry.RunID, ContinueInput{Prompt: "again", Deadline: time.Minute, QueueDeadline: time.Minute}); err == nil {
		t.Fatal("continued a task that was not interrupted")
	}
}

func TestLatestStatusesRequireBindings(t *testing.T) {
	for name, setup := range map[string]func(*testing.T, clientFixture, *wire.SubmissionRecord, string) (ed25519.PrivateKey, *wire.StatusRecord){
		"task hash": func(_ *testing.T, f clientFixture, submission *wire.SubmissionRecord, _ string) (ed25519.PrivateKey, *wire.StatusRecord) {
			return f.receiverKey, testStatus(submission, strings.Repeat("a", 64), "receiver-a", "interrupted", wire.TaskStateAuthRequired)
		},
		"receiver id": func(_ *testing.T, f clientFixture, submission *wire.SubmissionRecord, hash string) (ed25519.PrivateKey, *wire.StatusRecord) {
			return f.receiverKey, testStatus(submission, hash, "receiver-other", "interrupted", wire.TaskStateAuthRequired)
		},
		"pool": func(t *testing.T, f clientFixture, submission *wire.SubmissionRecord, hash string) (ed25519.PrivateKey, *wire.StatusRecord) {
			key := f.addReceiver(t, "receiver-other", "other", map[string]string{})
			return key, testStatus(submission, hash, "receiver-other", "interrupted", wire.TaskStateAuthRequired)
		},
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newClientFixture(t, true)
			submission, hash := fixture.rootSubmission(t, "run-latest-binding", "default")
			key, status := setup(t, fixture, submission, hash)
			fixture.produce(t, wire.TopicEvent, submission.Wherry.RunID, signed(t, wire.TypeStatus, status, key))
			submissions, _, err := fixture.client.readSubmissions(fixture.ctx)
			if err != nil {
				t.Fatal(err)
			}
			loaded, err := fixture.client.loadState(fixture.ctx)
			if err != nil {
				t.Fatal(err)
			}
			statuses, _, err := fixture.client.latestStatuses(fixture.ctx, submissions, loaded.receiverSigners(time.Now()))
			if err != nil {
				t.Fatal(err)
			}
			if statuses[submission.Wherry.RunID] != nil {
				t.Fatalf("status with mismatched %s was accepted", name)
			}
		})
	}
}

func TestLatestStatusesIgnoresRejected(t *testing.T) {
	fixture := newClientFixture(t, true)
	first, hash := fixture.turnSubmission(t, "run-ignore-rejected", 1, "msg-1")
	interrupted := testStatus(first, hash, "receiver-a", "interrupted", wire.TaskStateAuthRequired)
	fixture.produce(t, wire.TopicEvent, first.Wherry.RunID, signed(t, wire.TypeStatus, interrupted, fixture.receiverKey))
	third, thirdHash := fixture.turnSubmission(t, first.Wherry.RunID, 3, "msg-3")
	rejected := testStatus(third, thirdHash, "receiver-a", "rejected", wire.TaskStateRejected)
	fixture.produce(t, wire.TopicEvent, first.Wherry.RunID, signed(t, wire.TypeStatus, rejected, fixture.receiverKey))
	result, err := fixture.client.Continue(fixture.ctx, first.Wherry.RunID, ContinueInput{Prompt: "second", Deadline: time.Minute, QueueDeadline: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if result.Turn != 2 {
		t.Fatalf("continued at turn %d, want 2", result.Turn)
	}
}

// Each status's message text names the other credential; only the metadata decides.
func TestWaitingTasksUsesStructuredCredential(t *testing.T) {
	fixture := newClientFixture(t, true)
	interrupt := func(runID, text, metadata string) {
		submission, hash := fixture.rootSubmission(t, runID, "default")
		status := testStatus(submission, hash, "receiver-a", "interrupted", wire.TaskStateAuthRequired)
		status.StatusUpdate.Status.Message = &wire.Message{MessageID: "msg-status", Role: wire.RoleAgent, Parts: []wire.Part{{Text: text}}, Extensions: []string{wire.ExtensionURI}}
		status.StatusUpdate.Metadata = jsontext.Value(metadata)
		fixture.produce(t, wire.TopicEvent, runID, signed(t, wire.TypeStatus, status, fixture.receiverKey))
	}
	interrupt("run-token", "credential other required", `{"credential":"token"}`)
	interrupt("run-other", "credential token required", `{"credential":"other"}`)
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	result, err := fixture.client.PutSecret(fixture.ctx, "token", "receiver-a", path, 30*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Waiting) != 1 || result.Waiting[0].RunID != "run-token" || result.Waiting[0].Turn != 1 {
		t.Fatalf("waiting tasks %#v, want run-token alone", result.Waiting)
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

func TestEventsSkipsOnlyThisRun(t *testing.T) {
	fixture := newClientFixture(t, true)
	submission, hash := fixture.rootSubmission(t, "run-events", "default")
	other, otherHash := fixture.rootSubmission(t, "run-other", "default")
	fixture.produce(t, wire.TopicEvent, "run-events", signed(t, wire.TypeStatus, testStatus(submission, hash, "receiver-a", "started", wire.TaskStateWorking), fixture.receiverKey))
	fixture.produce(t, wire.TopicEvent, "run-other", signed(t, wire.TypeStatus, testStatus(other, otherHash, "receiver-a", "started", wire.TaskStateWorking), fixture.receiverKey))
	for _, topic := range []string{wire.TopicEvent, wire.TopicTranscript} {
		fixture.produce(t, topic, "run-events", []byte("not an envelope"))
		fixture.produce(t, topic, "run-other", []byte("not an envelope"))
	}
	events, err := fixture.client.Events(fixture.ctx, "run-events")
	if err != nil {
		t.Fatal(err)
	}
	if len(events.Statuses) != 1 || events.Statuses[0].StatusUpdate.TaskID != "run-events" || events.Skipped != 2 {
		t.Fatalf("statuses %#v skipped %d, want this run's status and its two bad records", events.Statuses, events.Skipped)
	}
}

func TestEventsBindsRecordsToTheSubmission(t *testing.T) {
	fixture := newClientFixture(t, true)
	otherPoolKey := fixture.addReceiver(t, "receiver-other", "other", map[string]string{}, testSelector())
	submission, hash := fixture.rootSubmission(t, "run-bound", "default")
	status := func(mutate func(*wire.StatusRecord)) []byte {
		value := testStatus(submission, hash, "receiver-a", "started", wire.TaskStateWorking)
		mutate(value)
		return signed(t, wire.TypeStatus, value, fixture.receiverKey)
	}
	chunk := func(contextID string, key ed25519.PrivateKey) []byte {
		return signed(t, wire.TypeArtifactUpdate, &wire.ArtifactUpdateRecord{
			ArtifactUpdate: wire.TaskArtifactUpdateEvent{TaskID: "run-bound", ContextID: contextID, Artifact: wire.Artifact{
				ArtifactID: "transcript", Parts: []wire.Part{{Text: "line", MediaType: "application/x-ndjson"}}, Extensions: []string{wire.ExtensionURI},
			}},
			Wherry: wire.ArtifactUpdateExtension{Turn: 1, Attempt: 1},
		}, key)
	}
	fixture.produce(t, wire.TopicEvent, "run-bound", status(func(*wire.StatusRecord) {}))
	for _, mutate := range []func(*wire.StatusRecord){
		func(value *wire.StatusRecord) { value.Wherry.TaskHash = strings.Repeat("a", 64) },
		func(value *wire.StatusRecord) { value.StatusUpdate.ContextID = "run-elsewhere" },
		func(value *wire.StatusRecord) { value.Wherry.Turn = 2 },
	} {
		fixture.produce(t, wire.TopicEvent, "run-bound", status(mutate))
	}
	fixture.produce(t, wire.TopicTranscript, "run-bound", chunk("run-bound", fixture.receiverKey))
	fixture.produce(t, wire.TopicTranscript, "run-bound", chunk("run-elsewhere", fixture.receiverKey))
	fixture.produce(t, wire.TopicTranscript, "run-bound", chunk("run-bound", otherPoolKey))
	events, err := fixture.client.Events(fixture.ctx, "run-bound")
	if err != nil {
		t.Fatal(err)
	}
	if len(events.Statuses) != 1 || events.Chunks[TurnAttempt{Turn: 1, Attempt: 1}] != 1 || events.Skipped != 5 {
		t.Fatalf("statuses %d chunks %v skipped %d, want 1, 1 and 5", len(events.Statuses), events.Chunks, events.Skipped)
	}
	if _, err := fixture.client.Events(fixture.ctx, "run-never-submitted"); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("events without a submission: %v, want ErrRunNotFound", err)
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

func (fixture clientFixture) rootSubmission(t *testing.T, runID, pool string) (*wire.SubmissionRecord, string) {
	t.Helper()
	submission := &wire.SubmissionRecord{
		Message: wire.Message{
			MessageID: "msg-" + runID, ContextID: runID, TaskID: runID, Role: wire.RoleUser,
			Parts: []wire.Part{{Text: "work"}}, Extensions: []string{wire.ExtensionURI},
		},
		Wherry: wire.SubmissionExtension{
			RunID: runID, Turn: 1, Attempt: 1, Pool: pool, Selector: []wire.Requirement{},
			DeadlineMS: time.Now().Add(time.Minute).UnixMilli(), QueueDeadlineMS: time.Now().Add(time.Minute).UnixMilli(),
			ReplyTopic: "replies.test",
			Lineage:    wire.Lineage{RootRunID: runID, Ancestors: []string{}, Chain: []wire.Bytes{}},
		},
	}
	envelope, err := trust.SignRootSubmission(*submission, fixture.userKey)
	if err != nil {
		t.Fatal(err)
	}
	fixture.produce(t, wire.TopicTask, runID, envelope)
	return submission, trust.EnvelopeHash(envelope)
}

func (fixture clientFixture) turnSubmission(t *testing.T, runID string, turn int64, messageID string) (*wire.SubmissionRecord, string) {
	t.Helper()
	submission := &wire.SubmissionRecord{
		Message: wire.Message{
			MessageID: messageID, ContextID: runID, TaskID: runID, Role: wire.RoleUser,
			Parts: []wire.Part{{Text: "work"}}, Extensions: []string{wire.ExtensionURI},
		},
		Wherry: wire.SubmissionExtension{
			RunID: runID, Turn: turn, Attempt: 1, Pool: "default", Selector: []wire.Requirement{},
			DeadlineMS: time.Now().Add(time.Minute).UnixMilli(), QueueDeadlineMS: time.Now().Add(time.Minute).UnixMilli(),
			ReplyTopic: "replies.test",
			Lineage:    wire.Lineage{RootRunID: runID, Ancestors: []string{}, Chain: []wire.Bytes{}},
		},
	}
	envelope, err := trust.SignRootSubmission(*submission, fixture.userKey)
	if err != nil {
		t.Fatal(err)
	}
	fixture.produce(t, wire.TopicTask, runID, envelope)
	return submission, trust.EnvelopeHash(envelope)
}

func testStatus(submission *wire.SubmissionRecord, hash, receiverID, kind string, state wire.TaskState) *wire.StatusRecord {
	return &wire.StatusRecord{
		StatusUpdate: wire.TaskStatusUpdateEvent{
			TaskID: submission.Wherry.RunID, ContextID: submission.Wherry.Lineage.RootRunID,
			Status: wire.TaskStatus{State: state, Timestamp: time.Now().UTC().Format(time.RFC3339)},
		},
		Wherry: wire.StatusExtension{
			Turn: submission.Wherry.Turn, Attempt: 1, TaskHash: hash, ReceiverID: receiverID, Kind: kind,
		},
	}
}

func testTask(submission *wire.SubmissionRecord, taskHash string, attempt int64, state wire.TaskState) *wire.TaskRecord {
	return &wire.TaskRecord{
		Task: wire.Task{
			ID: submission.Wherry.RunID, ContextID: submission.Wherry.Lineage.RootRunID,
			Artifacts: []wire.Artifact{}, History: []wire.Message{submission.Message},
			Status: wire.TaskStatus{State: state, Timestamp: time.Now().UTC().Format(time.RFC3339)},
		},
		Wherry: wire.TaskExtension{Turn: submission.Wherry.Turn, Attempt: attempt, TaskHash: taskHash},
	}
}

func TestReEnrolAboveFloor(t *testing.T) {
	fixture := newClientFixture(t, true)
	revoked, err := fixture.client.Revoke(fixture.ctx, "receiver-a")
	if err != nil {
		t.Fatal(err)
	}
	if revoked.Floor != 1 {
		t.Fatalf("floor %d, want 1", revoked.Floor)
	}
	approve := func(want int64) {
		t.Helper()
		result, err := fixture.client.Approve(fixture.ctx, trust.EnvelopeHash(fixture.request), "default", map[string]string{}, testGrants(), "auto", 30*24*time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if result.Generation != want {
			t.Fatalf("approved generation %d, want %d", result.Generation, want)
		}
		loaded, err := fixture.client.loadState(fixture.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if enrolled, err := loaded.state.Receiver("receiver-a", time.Now()); err != nil || enrolled.Generation != want {
			t.Fatalf("enrolled generation %d, err %v; want %d", enrolled.Generation, err, want)
		}
	}
	approve(2)
	// A floor above the current generation: the next approval must clear it, not follow the current generation.
	revocation, err := trust.NewRevocation("receiver-a", 5, fixture.userKey, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	fixture.produce(t, wire.TopicState, "receiver-a", revocation)
	approve(6)
}

func TestClientDistrustsRevokedReceiverRecords(t *testing.T) {
	fixture := newClientFixture(t, true)
	submission, hash := fixture.rootSubmission(t, "run-revoked", "default")
	fixture.produce(t, "replies.test", submission.Wherry.RunID, signed(t, wire.TypeTask, testTask(submission, hash, 1, wire.TaskStateCompleted), fixture.receiverKey))
	if _, err := fixture.client.Await(fixture.ctx, submission.Wherry.RunID, time.Second); err != nil {
		t.Fatalf("before revocation: %v", err)
	}
	if _, err := fixture.client.Revoke(fixture.ctx, "receiver-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.client.Await(fixture.ctx, submission.Wherry.RunID, 200*time.Millisecond); err == nil {
		t.Fatal("task signed by a revoked receiver was accepted")
	}
}

func (fixture clientFixture) requestRenewal(t *testing.T, generation int64) {
	t.Helper()
	request := &wire.RenewRequest{ReceiverID: "receiver-a", Generation: generation, AtMS: time.Now().UnixMilli()}
	fixture.produce(t, wire.TopicState, "receiver-a", signed(t, wire.TypeRenewRequest, request, fixture.receiverKey))
}

func TestAutoRenewalByClient(t *testing.T) {
	fixture := newClientFixture(t, true)
	before, err := fixture.client.loadState(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	founding, err := before.state.Receiver("receiver-a", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	fixture.requestRenewal(t, 1)
	end := fixture.topicEnd(t, wire.TopicState)
	if _, _, err := fixture.client.Receivers(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	if after := fixture.topicEnd(t, wire.TopicState); after != end+1 {
		t.Fatalf("state records %d after answering one renew request, want %d", after, end+1)
	}
	loaded, err := fixture.client.loadState(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if fixture.topicEnd(t, wire.TopicState) != end+1 {
		t.Fatal("an answered renew request was renewed again")
	}
	renewed, err := loaded.state.Receiver("receiver-a", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	current, _, _ := loaded.state.Approval("receiver-a")
	if renewed.Generation != 1 || renewed.ApprovalHash != founding.ApprovalHash || current.PrevStateHash != founding.ApprovalHash {
		t.Fatalf("renewal generation %d hash %s prev %s", renewed.Generation, renewed.ApprovalHash, current.PrevStateHash)
	}
	if loaded.skipped != 0 {
		t.Fatalf("skipped %d records", loaded.skipped)
	}

	t.Run("revoked", func(t *testing.T) {
		if _, err := fixture.client.Revoke(fixture.ctx, "receiver-a"); err != nil {
			t.Fatal(err)
		}
		fixture.requestRenewal(t, 1)
		end := fixture.topicEnd(t, wire.TopicState)
		if _, _, err := fixture.client.Enrolments(fixture.ctx); err != nil {
			t.Fatal(err)
		}
		if after := fixture.topicEnd(t, wire.TopicState); after != end {
			t.Fatal("client renewed a revoked receiver")
		}
	})
}

func TestManualRenewalNotAutomatic(t *testing.T) {
	fixture := newClientFixture(t, false)
	if _, err := fixture.client.Approve(fixture.ctx, trust.EnvelopeHash(fixture.request), "default", map[string]string{}, testGrants(), "manual", 30*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	fixture.requestRenewal(t, 1)
	end := fixture.topicEnd(t, wire.TopicState)
	enrolments, _, err := fixture.client.Enrolments(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if fixture.topicEnd(t, wire.TopicState) != end {
		t.Fatal("client answered a manual renew request")
	}
	if len(enrolments) != 1 || !enrolments[0].RenewalRequested {
		t.Fatalf("enrolments %#v, want the renew request listed", enrolments)
	}
}

func TestAutoRenewalLeavesLongerApproval(t *testing.T) {
	fixture := newClientFixture(t, false)
	if _, err := fixture.client.Approve(fixture.ctx, trust.EnvelopeHash(fixture.request), "default", map[string]string{}, testGrants(), "auto", 120*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	fixture.requestRenewal(t, 1)
	end := fixture.topicEnd(t, wire.TopicState)
	if _, _, err := fixture.client.Receivers(fixture.ctx); err != nil {
		t.Fatalf("a renew request that cannot extend the approval broke the client: %v", err)
	}
	if fixture.topicEnd(t, wire.TopicState) != end {
		t.Fatal("client published a renewal that does not extend the approval")
	}
}
