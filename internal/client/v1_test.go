package client

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	json "encoding/json/v2"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/trust"
	"github.com/benemon/wherry/internal/wire"
)

// v1Fixture is a client of the user whose version 1 records, signed at commit fc3b69d, are in the trust testdata.
type v1Fixture struct {
	clientFixture
	receiverKey ed25519.PrivateKey
	records     map[string][]byte
}

func newV1Fixture(t *testing.T) v1Fixture {
	t.Helper()
	encoded, err := os.ReadFile("../trust/testdata/v1-records.json")
	if err != nil {
		t.Fatal(err)
	}
	var golden map[string]string
	if err := json.Unmarshal(encoded, &golden); err != nil {
		t.Fatal(err)
	}
	seed := func(name string) ed25519.PrivateKey {
		decoded, err := hex.DecodeString(golden[name])
		if err != nil {
			t.Fatal(err)
		}
		return ed25519.NewKeyFromSeed(decoded)
	}
	records := make(map[string][]byte)
	for _, name := range []string{"enrol_request", "enrol_approval", "selector_approval", "submission", "chain_request", "chain_founding", "chain_renewal_1", "chain_renewal_2", "chain_next"} {
		if records[name], err = base64.RawURLEncoding.DecodeString(golden[name]); err != nil {
			t.Fatal(err)
		}
	}
	fixture := v1Fixture{clientFixture: newClientFixture(t, false), receiverKey: seed("receiver_seed_hex"), records: records}
	fixture.userKey = seed("user_seed_hex")
	fixture.userPublicKey = fixture.userKey.Public().(ed25519.PublicKey)
	fixture.client = New(fixture.kafka, fixture.userKey, "replies.test")
	fixture.produce(t, wire.TopicState, "receiver-v1", records["enrol_request"])
	fixture.produce(t, wire.TopicState, "receiver-v1", records["enrol_approval"])
	return fixture
}

func TestV1SubmissionReadsAsHistory(t *testing.T) {
	fixture := newV1Fixture(t)
	submission := fixture.records["submission"]
	fixture.produce(t, wire.TopicTask, "run-v1", submission)
	verified, err := trust.VerifyRootSubmission(submission, fixture.userPublicKey)
	if err != nil {
		t.Fatal(err)
	}
	taskHash := trust.EnvelopeHash(submission)
	status := &wire.StatusRecord{
		StatusUpdate: wire.TaskStatusUpdateEvent{TaskID: "run-v1", ContextID: "run-v1", Status: wire.TaskStatus{
			State: wire.TaskStateWorking, Timestamp: time.Now().UTC().Format(time.RFC3339),
		}},
		Wherry: wire.StatusExtension{Turn: 1, Attempt: 1, TaskHash: taskHash, ReceiverID: "receiver-v1", Kind: "started"},
	}
	fixture.produce(t, wire.TopicEvent, "run-v1", signed(t, wire.TypeStatus, status, fixture.receiverKey))
	fixture.produce(t, "replies.test", "run-v1", signed(t, wire.TypeTask, testTask(verified, taskHash, 1, wire.TaskStateCompleted), fixture.receiverKey))

	awaited, err := fixture.client.Await(fixture.ctx, "run-v1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if awaited.Task == nil || awaited.Task.Status.State != wire.TaskStateCompleted {
		t.Fatalf("unexpected await result: %#v", awaited)
	}
	events, err := fixture.client.Events(fixture.ctx, "run-v1")
	if err != nil {
		t.Fatal(err)
	}
	if len(events.Statuses) != 1 || events.Statuses[0].Wherry.TaskHash != taskHash {
		t.Fatalf("unexpected events: %#v", events)
	}
}

func TestV1SelectorApprovalCountsTowardGeneration(t *testing.T) {
	fixture := newV1Fixture(t)
	fixture.produce(t, wire.TopicState, "receiver-v1", fixture.records["selector_approval"])
	receivers, _, err := fixture.client.Receivers(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, receiver := range receivers {
		if receiver.ReceiverID == "receiver-v1" {
			t.Fatalf("selector approval enrolled the receiver: %#v", receiver)
		}
	}
	before := fixture.topicEnd(t, wire.TopicState)
	if _, err := fixture.client.Renew(fixture.ctx, "receiver-v1"); !errors.Is(err, trust.ErrSelectorApproval) {
		t.Fatalf("renew: got %v, want ErrSelectorApproval", err)
	}
	if after := fixture.topicEnd(t, wire.TopicState); after != before {
		t.Fatalf("receivers.state changed from offset %d to %d", before, after)
	}
	result, err := fixture.client.Approve(fixture.ctx, trust.EnvelopeHash(fixture.records["enrol_request"]), "default", map[string]string{}, singleGrant(), "auto", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if result.Generation != 3 {
		t.Fatalf("got generation %d, want 3", result.Generation)
	}
	receivers, skipped, err := fixture.client.Receivers(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if skipped != 0 || len(receivers) != 1 || receivers[0].ReceiverID != "receiver-v1" || receivers[0].Generation != 3 {
		t.Fatalf("unexpected receivers: %#v, skipped %d", receivers, skipped)
	}
}

func TestV1SelectorRenewalsKeepGenerationNumbering(t *testing.T) {
	fixture := newV1Fixture(t)
	for _, name := range []string{"chain_request", "chain_founding", "chain_renewal_1", "chain_renewal_2", "chain_next"} {
		fixture.produce(t, wire.TopicState, "receiver-chain", fixture.records[name])
	}
	result, err := fixture.client.Approve(fixture.ctx, trust.EnvelopeHash(fixture.records["chain_request"]), "default", map[string]string{}, singleGrant(), "auto", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if result.Skipped != 0 || result.Generation != 3 {
		t.Fatalf("got generation %d with %d skipped, want generation 3 after the v1 history's generation 2", result.Generation, result.Skipped)
	}
}
