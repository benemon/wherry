package trust

import (
	"bytes"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/wire"
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
		fixture.enrolled.Pool != "default" || fixture.enrolled.Generation != 1 || fixture.enrolled.ApprovalHash != EnvelopeHash(fixture.approval) {
		t.Fatalf("unexpected enrolment: %#v", fixture.enrolled)
	}
	if _, err := fixture.state.Receiver("receiver-a", testNow.Add(2*time.Hour)); err == nil {
		t.Fatal("expired enrolment was returned")
	}
}

func TestRootSubmissionSignVerify(t *testing.T) {
	fixture := newTrustFixture(t)
	submission := rootSubmission()
	envelope, err := SignRootSubmission(submission, fixture.userPrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := VerifyRootSubmission(envelope, fixture.userPublicKey)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(verified, &submission) {
		t.Fatalf("verified submission differs:\n got: %#v\nwant: %#v", verified, &submission)
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
	plaintext, _, err := Open(sealedEnvelope, fixture.state, "receiver-a", "token", fixture.sealingKey, fixture.userPublicKey, testNow)
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
	if _, _, err := Open(sealed1, fixture.state, "receiver-a", "token", fixture.sealingKey, fixture.userPublicKey, testNow); err == nil {
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
		if _, _, err := Open(sealedEnvelope, fixture.state, "receiver-b", "token", fixture.sealingKey, fixture.userPublicKey, testNow); err == nil {
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
		if _, _, err := Open(mustSigned(t, wire.TypeSealedSecret, sealed, fixture.userPrivateKey), fixture.state, "receiver-a", "token", fixture.sealingKey, fixture.userPublicKey, testNow); err == nil {
			t.Fatal("secret opened under another name")
		}
	})
}

func TestOpenRejectsReceiverCrossWiring(t *testing.T) {
	fixture := newTrustFixture(t)
	version, sealed, err := Seal(fixture.enrolled, "receiver-b", "token", 1, []byte("secret"), testNow.Add(time.Minute), fixture.userPrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.state.Apply(version); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Open(sealed, fixture.state, "receiver-a", "token", fixture.sealingKey, fixture.userPublicKey, testNow); err == nil {
		t.Fatal("secret for receiver B opened by receiver A")
	}
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
	if _, _, err := Open(mustSigned(t, wire.TypeSealedSecret, sealed, fixture.userPrivateKey), fixture.state, "receiver-a", "token", fixture.sealingKey, fixture.userPublicKey, testNow); err == nil {
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
	if _, _, err := Open(tampered, fixture.state, "receiver-a", "token", fixture.sealingKey, fixture.userPublicKey, testNow); err == nil || strings.Contains(err.Error(), plaintext) {
		t.Fatalf("hash mismatch error %v", err)
	}
	version := decodeSecretVersion(t, versionEnvelope)
	version.CiphertextHash = ciphertextHash(sealed.Enc, sealed.CT)
	if _, err := fixture.state.Apply(mustSigned(t, wire.TypeSecretVersion, version, fixture.userPrivateKey)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Open(tampered, fixture.state, "receiver-a", "token", fixture.sealingKey, fixture.userPublicKey, testNow); err == nil || strings.Contains(err.Error(), plaintext) {
		t.Fatalf("decrypt error %v", err)
	}
}

func TestVerifyRootSubmissionRejectsChild(t *testing.T) {
	fixture := newTrustFixture(t)
	child := rootSubmission()
	child.Wherry.RunID, child.Message.TaskID = "child", "child"
	child.Wherry.Lineage = wire.Lineage{
		RootRunID: "root", Depth: 1, Ancestors: []string{"root"},
		ParentHash: strings.Repeat("a", 64), Chain: []wire.Bytes{wire.Bytes("parent envelope")},
	}
	envelope := mustSigned(t, wire.TypeSubmission, &child, fixture.userPrivateKey)
	if _, err := VerifyRootSubmission(envelope, fixture.userPublicKey); err == nil {
		t.Fatal("child submission was accepted as a root")
	}
	if _, err := SignRootSubmission(child, fixture.userPrivateKey); err == nil {
		t.Fatal("child submission was signed as a root")
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
		ApprovalHash: strings.Repeat("b", 64), Version: 6, ExpMS: 7,
	}
	encoded, err := marshalSecretAAD(header)
	if err != nil {
		t.Fatal(err)
	}
	want := "wherry/v1/sealed_secret_aad\x00" +
		`{"topic":"receivers.secrets","receiver_id":"receiver-a","name":"token","sealing_key_fp":"sha256:` + strings.Repeat("a", 64) + `","generation":5,"approval_hash":"` + strings.Repeat("b", 64) + `","version":6,"exp_ms":7}`
	if string(encoded) != want {
		t.Fatalf("AAD differs:\n got: %q\nwant: %q", encoded, want)
	}
}

func rootSubmission() wire.SubmissionRecord {
	return wire.SubmissionRecord{
		Message: wire.Message{
			MessageID: "message", ContextID: "root", TaskID: "root", Role: wire.RoleUser,
			Parts: []wire.Part{{Text: "hello"}}, Extensions: []string{wire.ExtensionURI},
		},
		Wherry: wire.SubmissionExtension{
			RunID: "root", Turn: 1, Attempt: 1, Pool: "default", Selector: testSelector(), Labels: map[string]string{"lab/os": "linux"},
			DeadlineMS: testNow.Add(time.Hour).UnixMilli(), QueueDeadlineMS: testNow.Add(time.Minute).UnixMilli(),
			ReplyTopic: "replies.client",
			Lineage:    wire.Lineage{RootRunID: "root", Depth: 0, Ancestors: []string{}, ParentHash: "", Chain: []wire.Bytes{}},
		},
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

// When history restarts, generation numbering restarts at 1; a secret sealed under the
// superseded generation-1 approval must not open under the new one.
func TestSealRejectedUnderDifferentApproval(t *testing.T) {
	fixture := newTrustFixture(t)
	versionEnvelope, sealedEnvelope, err := Seal(fixture.enrolled, "receiver-a", "token", 1, []byte("secret"), testNow.Add(time.Minute), fixture.userPrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	reapprove := func(state *State, generation int64, previous string) Enrolled {
		t.Helper()
		approval, err := Approve(fixture.request, fixture.enrolRequest, fixture.userPrivateKey, testLabels(), "default", testGrants(), testSelector(), "auto", generation, previous, testNow.Add(2*time.Hour))
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
		if enrolled.ApprovalHash == fixture.enrolled.ApprovalHash || enrolled.Generation != generation {
			t.Fatalf("re-approval did not produce a distinct generation-%d approval", generation)
		}
		return enrolled
	}
	open := func(state *State, header []byte) error {
		t.Helper()
		if _, err := state.Apply(header); err != nil {
			t.Fatal(err)
		}
		_, _, err := Open(sealedEnvelope, state, "receiver-a", "token", fixture.sealingKey, fixture.userPublicKey, testNow)
		return err
	}
	restarted := func() (*State, Enrolled) {
		t.Helper()
		state := NewState(fixture.userPublicKey)
		if _, err := state.Apply(fixture.request); err != nil {
			t.Fatal(err)
		}
		return state, reapprove(state, 1, "")
	}

	t.Run("next generation", func(t *testing.T) {
		reapprove(fixture.state, 2, EnvelopeHash(fixture.approval))
		if err := open(fixture.state, versionEnvelope); err == nil {
			t.Fatal("secret sealed under approval A opened under A'")
		}
	})
	t.Run("matching generation", func(t *testing.T) {
		state, _ := restarted()
		if err := open(state, versionEnvelope); err == nil {
			t.Fatal("secret sealed under generation-1 approval A opened under generation-1 approval A'")
		}
	})
	t.Run("header relabelled to the current approval", func(t *testing.T) {
		state, current := restarted()
		header := decodeSecretVersion(t, versionEnvelope)
		header.ApprovalHash = current.ApprovalHash
		if err := open(state, mustSigned(t, wire.TypeSecretVersion, header, fixture.userPrivateKey)); err == nil {
			t.Fatal("ciphertext sealed under approval A opened through a header naming A'")
		}
	})
}

func (fixture trustFixture) revoke(t *testing.T, floor int64) {
	t.Helper()
	revocation, err := NewRevocation("receiver-a", floor, fixture.userPrivateKey, testNow)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.state.Apply(revocation); err != nil {
		t.Fatal(err)
	}
}

func (fixture trustFixture) approve(t *testing.T, generation int64, previous string) []byte {
	t.Helper()
	approval, err := Approve(fixture.request, fixture.enrolRequest, fixture.userPrivateKey, testLabels(), "default", testGrants(), testSelector(), "auto", generation, previous, testNow.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return approval
}

func TestRevocationFloorIsMaximum(t *testing.T) {
	fixture := newTrustFixture(t)
	approval2 := fixture.approve(t, 2, EnvelopeHash(fixture.approval))
	if _, err := fixture.state.Apply(approval2); err != nil {
		t.Fatal(err)
	}
	fixture.revoke(t, 2)
	fixture.revoke(t, 1)
	if floor := fixture.state.Floor("receiver-a"); floor != 2 {
		t.Fatalf("floor %d after a lower floor was appended, want 2", floor)
	}
	if _, err := fixture.state.Receiver("receiver-a", testNow); !errors.Is(err, ErrRevoked) {
		t.Fatalf("got %v, want ErrRevoked", err)
	}
}

func TestApprovalAtOrBelowFloorRejected(t *testing.T) {
	t.Run("approved before the revocation", func(t *testing.T) {
		fixture := newTrustFixture(t)
		fixture.revoke(t, 1)
		if _, err := fixture.state.Receiver("receiver-a", testNow); !errors.Is(err, ErrRevoked) {
			t.Fatalf("got %v, want ErrRevoked", err)
		}
		if _, err := fixture.state.Apply(fixture.approval); err == nil {
			t.Fatal("replayed approval at the floor was accepted")
		}
		if _, err := fixture.state.Receiver("receiver-a", testNow); !errors.Is(err, ErrRevoked) {
			t.Fatalf("after replay got %v, want ErrRevoked", err)
		}
	})
	for name, floor := range map[string]int64{"at the floor": 2, "below the floor": 3} {
		t.Run(name+" appended after the revocation", func(t *testing.T) {
			fixture := newTrustFixture(t)
			fixture.revoke(t, floor)
			if _, err := fixture.state.Apply(fixture.approve(t, 2, EnvelopeHash(fixture.approval))); err == nil {
				t.Fatal("approval at or below the floor was accepted")
			}
			if _, err := fixture.state.Receiver("receiver-a", testNow); !errors.Is(err, ErrRevoked) {
				t.Fatalf("got %v, want ErrRevoked", err)
			}
			// The rejected approval must not become the previous state of the next one.
			if _, err := fixture.state.Apply(fixture.approve(t, floor+1, EnvelopeHash(fixture.approval))); err != nil {
				t.Fatal(err)
			}
			if enrolled, err := fixture.state.Receiver("receiver-a", testNow); err != nil || enrolled.Generation != floor+1 {
				t.Fatalf("got generation %d, err %v; want %d", enrolled.Generation, err, floor+1)
			}
		})
	}
}

func TestOpenFailsForRevokedReceiver(t *testing.T) {
	fixture := newTrustFixture(t)
	versionEnvelope, sealedEnvelope, err := Seal(fixture.enrolled, "receiver-a", "token", 1, []byte("secret value"), testNow.Add(time.Minute), fixture.userPrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.state.Apply(versionEnvelope); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Open(sealedEnvelope, fixture.state, "receiver-a", "token", fixture.sealingKey, fixture.userPublicKey, testNow); err != nil {
		t.Fatal(err)
	}
	fixture.revoke(t, 1)
	if _, _, err := Open(sealedEnvelope, fixture.state, "receiver-a", "token", fixture.sealingKey, fixture.userPublicKey, testNow); !errors.Is(err, ErrRevoked) {
		t.Fatalf("got %v, want ErrRevoked", err)
	}
}

func TestStateRejectsRevocationNotSignedByUser(t *testing.T) {
	fixture := newTrustFixture(t)
	_, otherKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	revocation := &wire.Revocation{ReceiverID: "receiver-a", FloorGeneration: 1, AtMS: testNow.UnixMilli()}
	for name, key := range map[string]ed25519.PrivateKey{"receiver": fixture.receiverKey, "unknown": otherKey} {
		if _, err := fixture.state.Apply(mustSigned(t, wire.TypeRevocation, revocation, key)); err == nil {
			t.Fatalf("%s-signed revocation was accepted", name)
		}
	}
	if fixture.state.Floor("receiver-a") != 0 {
		t.Fatal("unverified revocation set a floor")
	}
	if _, err := fixture.state.Receiver("receiver-a", testNow); err != nil {
		t.Fatalf("unverified revocation revoked the receiver: %v", err)
	}
}
