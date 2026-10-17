package trust

import (
	"bytes"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/benemon/agentbus/internal/wire"
)

var testNow = time.UnixMilli(1_800_000_000_000)

type trustFixture struct {
	userPublicKey  ed25519.PublicKey
	userPrivateKey ed25519.PrivateKey
	receiverKey    ed25519.PrivateKey
	sealingKey     *ecdh.PrivateKey
	request        []byte
	enrolRequest   *wire.EnrolRequest
	approval       []byte
	state          *State
	enrolled       Enrolled
}

func newTrustFixture(t *testing.T) trustFixture {
	t.Helper()
	userPublicKey, userPrivateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, receiverKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sealingKey, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	request, err := NewEnrolRequest("receiver-a", receiverKey, sealingKey, testLabels(), testDetected(), testNow)
	if err != nil {
		t.Fatal(err)
	}
	state := NewState(userPublicKey)
	decoded, err := state.Apply(request)
	if err != nil {
		t.Fatal(err)
	}
	enrolRequest := decoded.(*wire.EnrolRequest)
	approval, err := Approve(request, enrolRequest, userPrivateKey, testLabels(), "default", testGrants(), testSelector(), "auto", 1, "", testNow.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Apply(approval); err != nil {
		t.Fatal(err)
	}
	enrolled, err := state.Receiver("receiver-a", testNow)
	if err != nil {
		t.Fatal(err)
	}
	return trustFixture{
		userPublicKey: userPublicKey, userPrivateKey: userPrivateKey,
		receiverKey: receiverKey, sealingKey: sealingKey,
		request: request, enrolRequest: enrolRequest, approval: approval, state: state, enrolled: enrolled,
	}
}

func TestKeyFileRoundTrip(t *testing.T) {
	directory := t.TempDir()
	signingPath := filepath.Join(directory, "signing.key")
	signingKey, err := GenerateSigningKeyFile(signingPath)
	if err != nil {
		t.Fatal(err)
	}
	loadedSigningKey, err := LoadSigningKeyFile(signingPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(loadedSigningKey, signingKey) {
		t.Fatal("loaded signing key differs")
	}
	if _, err := GenerateSigningKeyFile(signingPath); err == nil {
		t.Fatal("existing signing key was overwritten")
	}

	sealingPath := filepath.Join(directory, "sealing.key")
	sealingKey, err := GenerateSealingKeyFile(sealingPath)
	if err != nil {
		t.Fatal(err)
	}
	loadedSealingKey, err := LoadSealingKeyFile(sealingPath)
	if err != nil {
		t.Fatal(err)
	}
	if !loadedSealingKey.Equal(sealingKey) {
		t.Fatal("loaded sealing key differs")
	}
	if _, err := GenerateSealingKeyFile(sealingPath); err == nil {
		t.Fatal("existing sealing key was overwritten")
	}
	for _, path := range []string{signingPath, sealingPath} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("%s mode is %o, want 600", path, info.Mode().Perm())
		}
	}
}

func TestEnrolRequestApproveState(t *testing.T) {
	fixture := newTrustFixture(t)
	if !bytes.Equal(fixture.enrolled.SigningPub, fixture.receiverKey.Public().(ed25519.PublicKey)) {
		t.Fatal("state returned wrong signing key")
	}
	if !fixture.enrolled.SealingPub.Equal(fixture.sealingKey.PublicKey()) {
		t.Fatal("state returned wrong sealing key")
	}
	if !reflect.DeepEqual(fixture.enrolled.Labels, testLabels()) ||
		!reflect.DeepEqual(fixture.enrolled.Adapters, testGrants()) ||
		!reflect.DeepEqual(fixture.enrolled.Selector, testSelector()) ||
		fixture.enrolled.Pool != "default" || fixture.enrolled.Generation != 1 {
		t.Fatalf("unexpected enrolment: %#v", fixture.enrolled)
	}
	if _, err := fixture.state.Receiver("receiver-a", testNow.Add(2*time.Hour)); err == nil {
		t.Fatal("expired enrolment was returned")
	}
}

func TestRootTaskSignVerify(t *testing.T) {
	fixture := newTrustFixture(t)
	task := rootTask()
	envelope, err := SignRootTask(task, fixture.userPrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := VerifyRootTask(envelope, fixture.userPublicKey)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(verified, &task) {
		t.Fatalf("verified task differs:\n got: %#v\nwant: %#v", verified, &task)
	}
}

func TestSealOpenRoundTrip(t *testing.T) {
	fixture := newTrustFixture(t)
	versionEnvelope, sealedEnvelope, err := Seal(fixture.enrolled, "receiver-a", "token", 1, []byte("secret value"), testNow.Add(time.Minute), fixture.userPrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.state.Apply(versionEnvelope); err != nil {
		t.Fatal(err)
	}
	plaintext, _, err := Open(sealedEnvelope, fixture.state, "receiver-a", fixture.sealingKey, fixture.userPublicKey, testNow)
	if err != nil {
		t.Fatal(err)
	}
	if string(plaintext) != "secret value" {
		t.Fatalf("got %q, want secret value", plaintext)
	}
}

func testLabels() map[string]string { return map[string]string{"lab/site": "home"} }

func testSelector() []wire.Requirement {
	return []wire.Requirement{{Key: "region", Operator: wire.OpIn, Values: []string{"eu"}}}
}

func testDetected() []wire.DetectedAdapter {
	return []wire.DetectedAdapter{
		{Name: "lab", Platform: "nomad", Offers: []wire.Offer{{Adapter: "lab", Labels: map[string]string{"runtime": "exec"}}}},
		{Name: "cloud", Platform: "aws", Offers: []wire.Offer{{Adapter: "cloud", Labels: map[string]string{"runtime": "lambda"}}}},
	}
}

func testGrants() []wire.AdapterGrant {
	return []wire.AdapterGrant{
		{Name: "lab", Platform: "nomad", Allowed: map[string][]string{"runtime": {"exec"}}, Defaults: map[string]string{}},
		{Name: "cloud", Platform: "aws", Allowed: map[string][]string{"runtime": {"lambda"}}, Defaults: map[string]string{}},
	}
}

func testCapability(offers ...wire.Offer) *wire.Capability {
	return &wire.Capability{
		ReceiverID: "receiver-a", Pool: "default", Labels: testLabels(), Offers: offers, Selector: testSelector(),
		Capacity: 2, InFlight: 1, Generation: 1, AtMS: testNow.UnixMilli(),
	}
}

func TestCapabilityWithinBounds(t *testing.T) {
	fixture := newTrustFixture(t)
	capability := testCapability(
		wire.Offer{Adapter: "lab", Labels: map[string]string{"runtime": "exec"}},
		wire.Offer{Adapter: "cloud", Labels: map[string]string{"runtime": "lambda"}},
	)
	envelope := mustSigned(t, wire.TypeCapability, capability, fixture.receiverKey)
	if _, err := CapabilityWithin(envelope, fixture.enrolled); err != nil {
		t.Fatal(err)
	}
}

func TestCapabilityOfferOutsideAllowedRejected(t *testing.T) {
	fixture := newTrustFixture(t)
	capability := testCapability(wire.Offer{Adapter: "lab", Labels: map[string]string{"runtime": "podman"}})
	if _, err := CapabilityWithin(mustSigned(t, wire.TypeCapability, capability, fixture.receiverKey), fixture.enrolled); err == nil {
		t.Fatal("offer outside allowed was accepted")
	}
}

func TestCapabilityOfferUnknownAdapterRejected(t *testing.T) {
	fixture := newTrustFixture(t)
	capability := testCapability(wire.Offer{Adapter: "other", Labels: map[string]string{"runtime": "exec"}})
	if _, err := CapabilityWithin(mustSigned(t, wire.TypeCapability, capability, fixture.receiverKey), fixture.enrolled); err == nil || !strings.Contains(err.Error(), "ungranted") {
		t.Fatalf("got %v, want offer from ungranted adapter rejected", err)
	}
}

func TestCapabilityOfferOutsideItsGrantRejected(t *testing.T) {
	fixture := newTrustFixture(t)
	capability := testCapability(wire.Offer{Adapter: "lab", Labels: map[string]string{"runtime": "lambda"}})
	if _, err := CapabilityWithin(mustSigned(t, wire.TypeCapability, capability, fixture.receiverKey), fixture.enrolled); err == nil || !strings.Contains(err.Error(), "exceeds its grant") {
		t.Fatalf("got %v, want value allowed only in the other grant rejected", err)
	}
}

func TestApproveRejectsSwappedSealingKey(t *testing.T) {
	fixture := newTrustFixture(t)
	envelope, err := wire.DecodeEnvelope(fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := wire.DecodePayload(wire.TypeEnrolRequest, envelope.Payload)
	if err != nil {
		t.Fatal(err)
	}
	request := decoded.(*wire.EnrolRequest)
	request.SealingPub = bytes.Repeat([]byte{9}, 32)
	envelope.Payload, err = wire.MarshalPayload(wire.TypeEnrolRequest, request)
	if err != nil {
		t.Fatal(err)
	}
	tampered, err := wire.MarshalEnvelope(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Approve(tampered, request, fixture.userPrivateKey, testLabels(), "default", testGrants(), testSelector(), "auto", 1, "", testNow.Add(time.Hour)); err == nil {
		t.Fatal("request with swapped sealing key was approved")
	}
}

func TestStateIgnoresOlderApprovalAppendedLater(t *testing.T) {
	fixture := newTrustFixture(t)
	approval2, err := Approve(fixture.request, fixture.enrolRequest, fixture.userPrivateKey, testLabels(), "default", testGrants(), testSelector(), "auto", 2, EnvelopeHash(fixture.approval), testNow.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.state.Apply(approval2); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.state.Apply(fixture.approval); err != nil {
		t.Fatal(err)
	}
	enrolled, err := fixture.state.Receiver("receiver-a", testNow)
	if err != nil {
		t.Fatal(err)
	}
	if enrolled.Generation != 2 {
		t.Fatalf("got generation %d, want 2", enrolled.Generation)
	}
}

func TestStateRejectsApprovalWithWrongPrevHash(t *testing.T) {
	fixture := newTrustFixture(t)
	wrongHash := strings.Repeat("a", 64)
	approval2, err := Approve(fixture.request, fixture.enrolRequest, fixture.userPrivateKey, testLabels(), "default", testGrants(), testSelector(), "auto", 2, wrongHash, testNow.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.state.Apply(approval2); err == nil {
		t.Fatal("approval with wrong previous hash was accepted")
	}
	enrolled, err := fixture.state.Receiver("receiver-a", testNow)
	if err != nil {
		t.Fatal(err)
	}
	if enrolled.Generation != 1 {
		t.Fatal("failed approval changed state")
	}
}

func TestStateRejectsApprovalForUnknownRequest(t *testing.T) {
	fixture := newTrustFixture(t)
	state := NewState(fixture.userPublicKey)
	if _, err := state.Apply(fixture.approval); err == nil {
		t.Fatal("approval for unknown request was accepted")
	}
	if _, err := state.Receiver("receiver-a", testNow); err == nil {
		t.Fatal("failed approval changed state")
	}
}

func TestStateRejectsReceiverSignedApproval(t *testing.T) {
	fixture := newTrustFixture(t)
	state := NewState(fixture.userPublicKey)
	if _, err := state.Apply(fixture.request); err != nil {
		t.Fatal(err)
	}
	approval := &wire.EnrolApproval{
		ReceiverID: "receiver-a", RequestHash: EnvelopeHash(fixture.request),
		Labels: testLabels(), Pool: "default", Adapters: testGrants(), Selector: testSelector(),
		Renewal: "auto", Generation: 1, PrevStateHash: "", ExpMS: testNow.Add(time.Hour).UnixMilli(),
	}
	if _, err := state.Apply(mustSigned(t, wire.TypeEnrolApproval, approval, fixture.receiverKey)); err == nil {
		t.Fatal("receiver-signed approval was accepted")
	}
}

func TestCapabilityBroaderThanApprovalRejected(t *testing.T) {
	fixture := newTrustFixture(t)
	tests := map[string]func(*wire.Capability){
		"extra label":        func(capability *wire.Capability) { capability.Labels["lab/arch"] = "arm64" },
		"different selector": func(capability *wire.Capability) { capability.Selector[0].Values = []string{"us"} },
		"different pool":     func(capability *wire.Capability) { capability.Pool = "other" },
		"stale generation":   func(capability *wire.Capability) { capability.Generation = 0 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			capability := testCapability(wire.Offer{Adapter: "lab", Labels: map[string]string{"runtime": "exec"}})
			mutate(capability)
			envelope := mustSigned(t, wire.TypeCapability, capability, fixture.receiverKey)
			if _, err := CapabilityWithin(envelope, fixture.enrolled); err == nil {
				t.Fatal("out-of-bounds capability was accepted")
			}
		})
	}
}

func TestOpenRejectsRolledBackVersion(t *testing.T) {
	fixture := newTrustFixture(t)
	version1, sealed1, err := Seal(fixture.enrolled, "receiver-a", "token", 1, []byte("old"), testNow.Add(time.Minute), fixture.userPrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	version2, _, err := Seal(fixture.enrolled, "receiver-a", "token", 2, []byte("new"), testNow.Add(time.Minute), fixture.userPrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.state.Apply(version1); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.state.Apply(version2); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Open(sealed1, fixture.state, "receiver-a", fixture.sealingKey, fixture.userPublicKey, testNow); err == nil {
		t.Fatal("rolled-back secret version was opened")
	}
}

func TestOpenRejectsAADMismatch(t *testing.T) {
	fixture := newTrustFixture(t)
	versionEnvelope, sealedEnvelope, err := Seal(fixture.enrolled, "receiver-a", "token", 1, []byte("secret"), testNow.Add(time.Minute), fixture.userPrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.state.Apply(versionEnvelope); err != nil {
		t.Fatal(err)
	}
	t.Run("receiver", func(t *testing.T) {
		if _, _, err := Open(sealedEnvelope, fixture.state, "receiver-b", fixture.sealingKey, fixture.userPublicKey, testNow); err == nil {
			t.Fatal("secret sealed for receiver A opened as receiver B")
		}
	})
	t.Run("name", func(t *testing.T) {
		version := decodeSecretVersion(t, versionEnvelope)
		version.Name = "other"
		if _, err := fixture.state.Apply(mustSigned(t, wire.TypeSecretVersion, version, fixture.userPrivateKey)); err != nil {
			t.Fatal(err)
		}
		sealed := decodeSealedSecret(t, sealedEnvelope)
		sealed.Name = "other"
		if _, _, err := Open(mustSigned(t, wire.TypeSealedSecret, sealed, fixture.userPrivateKey), fixture.state, "receiver-a", fixture.sealingKey, fixture.userPublicKey, testNow); err == nil {
			t.Fatal("secret opened under another name")
		}
	})
}

func TestOpenRejectsCiphertextHashMismatch(t *testing.T) {
	fixture := newTrustFixture(t)
	versionEnvelope, sealedEnvelope, err := Seal(fixture.enrolled, "receiver-a", "token", 1, []byte("secret"), testNow.Add(time.Minute), fixture.userPrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.state.Apply(versionEnvelope); err != nil {
		t.Fatal(err)
	}
	sealed := decodeSealedSecret(t, sealedEnvelope)
	sealed.CT[0] ^= 1
	if _, _, err := Open(mustSigned(t, wire.TypeSealedSecret, sealed, fixture.userPrivateKey), fixture.state, "receiver-a", fixture.sealingKey, fixture.userPublicKey, testNow); err == nil {
		t.Fatal("secret with mismatched ciphertext hash was opened")
	}
}

func TestSecretErrorsOmitPlaintext(t *testing.T) {
	const plaintext = "plaintext-secret-value"
	fixture := newTrustFixture(t)
	unsealable := fixture.enrolled
	unsealable.SealingPub = nil
	if _, _, err := Seal(unsealable, "receiver-a", "token", 1, []byte(plaintext), testNow.Add(time.Minute), fixture.userPrivateKey); err == nil || strings.Contains(err.Error(), plaintext) {
		t.Fatalf("seal error %v", err)
	}
	versionEnvelope, sealedEnvelope, err := Seal(fixture.enrolled, "receiver-a", "token", 1, []byte(plaintext), testNow.Add(time.Minute), fixture.userPrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.state.Apply(versionEnvelope); err != nil {
		t.Fatal(err)
	}
	sealed := decodeSealedSecret(t, sealedEnvelope)
	sealed.CT[0] ^= 1
	tampered := mustSigned(t, wire.TypeSealedSecret, sealed, fixture.userPrivateKey)
	if _, _, err := Open(tampered, fixture.state, "receiver-a", fixture.sealingKey, fixture.userPublicKey, testNow); err == nil || strings.Contains(err.Error(), plaintext) {
		t.Fatalf("hash mismatch error %v", err)
	}
	version := decodeSecretVersion(t, versionEnvelope)
	version.CiphertextHash = ciphertextHash(sealed.Enc, sealed.CT)
	if _, err := fixture.state.Apply(mustSigned(t, wire.TypeSecretVersion, version, fixture.userPrivateKey)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Open(tampered, fixture.state, "receiver-a", fixture.sealingKey, fixture.userPublicKey, testNow); err == nil || strings.Contains(err.Error(), plaintext) {
		t.Fatalf("decrypt error %v", err)
	}
}

func TestVerifyRootTaskRejectsChild(t *testing.T) {
	fixture := newTrustFixture(t)
	child := rootTask()
	child.RunID = "child"
	child.Depth = 1
	child.Ancestors = []string{"root"}
	child.ParentHash = strings.Repeat("a", 64)
	child.Chain = []wire.Bytes{wire.Bytes("parent envelope")}
	envelope := mustSigned(t, wire.TypeTask, &child, fixture.userPrivateKey)
	if _, err := VerifyRootTask(envelope, fixture.userPublicKey); err == nil {
		t.Fatal("child task was accepted as a root")
	}
	if _, err := SignRootTask(child, fixture.userPrivateKey); err == nil {
		t.Fatal("child task was signed as a root")
	}
}

func TestLoadKeyFileRejectsGroupReadable(t *testing.T) {
	directory := t.TempDir()
	t.Run("signing", func(t *testing.T) {
		path := filepath.Join(directory, "signing.key")
		if _, err := GenerateSigningKeyFile(path); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0o640); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadSigningKeyFile(path); err == nil {
			t.Fatal("group-readable signing key was loaded")
		}
	})
	t.Run("sealing", func(t *testing.T) {
		path := filepath.Join(directory, "sealing.key")
		if _, err := GenerateSealingKeyFile(path); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0o604); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadSealingKeyFile(path); err == nil {
			t.Fatal("other-readable sealing key was loaded")
		}
	})
}

func TestEnvelopeHashUsesExactBytes(t *testing.T) {
	input := []byte("exact envelope bytes")
	digest := sha256.Sum256(input)
	if got, want := EnvelopeHash(input), hex.EncodeToString(digest[:]); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestSealedSecretAADBytes(t *testing.T) {
	header := secretAAD{
		Topic: sealedSecretTopic, ReceiverID: "receiver-a", Name: "token",
		SealingKeyFP: "sha256:" + strings.Repeat("a", 64), Generation: 5,
		Version: 6, ExpMS: 7,
	}
	encoded, err := marshalSecretAAD(header)
	if err != nil {
		t.Fatal(err)
	}
	want := "agentbus/v1/sealed_secret_aad\x00" +
		`{"topic":"receivers.secrets","receiver_id":"receiver-a","name":"token","sealing_key_fp":"sha256:` + strings.Repeat("a", 64) + `","generation":5,"version":6,"exp_ms":7}`
	if string(encoded) != want {
		t.Fatalf("AAD differs:\n got: %q\nwant: %q", encoded, want)
	}
}

func rootTask() wire.Task {
	return wire.Task{
		RunID: "root", Attempt: 1, RootRunID: "root", Depth: 0, Ancestors: []string{}, ParentHash: "", Chain: []wire.Bytes{},
		Pool: "default", Labels: map[string]string{"lab/os": "linux"}, Selector: testSelector(),
		DeadlineMS: testNow.Add(time.Hour).UnixMilli(), QueueDeadlineMS: testNow.Add(time.Minute).UnixMilli(),
		ReplyTopic: "replies.client", CorrelationID: "correlation",
		Message: wire.Message{MessageID: "message", Role: wire.RoleUser, Parts: []wire.Part{{Text: "hello"}}},
	}
}

func mustSigned(t *testing.T, recordType wire.RecordType, value any, key ed25519.PrivateKey) []byte {
	t.Helper()
	envelope, err := marshalSigned(recordType, value, key)
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

func decodeSecretVersion(t *testing.T, encoded []byte) *wire.SecretVersion {
	t.Helper()
	envelope, err := wire.DecodeEnvelope(encoded)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := wire.DecodePayload(wire.TypeSecretVersion, envelope.Payload)
	if err != nil {
		t.Fatal(err)
	}
	return decoded.(*wire.SecretVersion)
}

func decodeSealedSecret(t *testing.T, encoded []byte) *wire.SealedSecret {
	t.Helper()
	envelope, err := wire.DecodeEnvelope(encoded)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := wire.DecodePayload(wire.TypeSealedSecret, envelope.Payload)
	if err != nil {
		t.Fatal(err)
	}
	return decoded.(*wire.SealedSecret)
}

func TestEmptySelectorSurvivesState(t *testing.T) {
	fixture := newTrustFixture(t)
	approval, err := Approve(fixture.request, fixture.enrolRequest, fixture.userPrivateKey, testLabels(), "default", testGrants(), []wire.Requirement{}, "auto", 2, EnvelopeHash(fixture.approval), testNow.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.state.Apply(approval); err != nil {
		t.Fatal(err)
	}
	enrolled, err := fixture.state.Receiver("receiver-a", testNow)
	if err != nil {
		t.Fatal(err)
	}
	capability := testCapability()
	capability.Offers, capability.Selector, capability.Generation = []wire.Offer{}, enrolled.Selector, 2
	if _, err := marshalSigned(wire.TypeCapability, capability, fixture.receiverKey); err != nil {
		t.Fatalf("capability built from an empty approved selector: %v", err)
	}
}
