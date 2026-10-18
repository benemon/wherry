package trust

import (
	"crypto/ecdh"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	json "encoding/json/v2"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/wire"
)

// v1Records holds records signed by the version 1 signers at commit fc3b69d; the generator is outside the repo.
type v1Records struct {
	userKey          ed25519.PrivateKey
	receiverKey      ed25519.PrivateKey
	sealingKey       *ecdh.PrivateKey
	request          []byte
	approval         []byte
	selectorApproval []byte
	secretVersion    []byte
	sealedSecret     []byte
	submission       []byte
	capability       []byte
	// named holds every record in the file by its name.
	named map[string][]byte
}

func loadV1Records(t *testing.T) v1Records {
	t.Helper()
	encoded, err := os.ReadFile("testdata/v1-records.json")
	if err != nil {
		t.Fatal(err)
	}
	var golden map[string]string
	if err := json.Unmarshal(encoded, &golden); err != nil {
		t.Fatal(err)
	}
	decode := func(name string, decoder func(string) ([]byte, error)) []byte {
		value, err := decoder(golden[name])
		if err != nil || len(value) == 0 {
			t.Fatalf("golden %s: %v", name, err)
		}
		return value
	}
	b64 := func(name string) []byte { return decode(name, base64.RawURLEncoding.DecodeString) }
	named := make(map[string][]byte)
	for name := range golden {
		if !strings.HasSuffix(name, "_hex") && name != "producer" {
			named[name] = b64(name)
		}
	}
	sealingKey, err := ecdh.X25519().NewPrivateKey(decode("sealing_key_hex", hex.DecodeString))
	if err != nil {
		t.Fatal(err)
	}
	return v1Records{
		userKey:     ed25519.NewKeyFromSeed(decode("user_seed_hex", hex.DecodeString)),
		receiverKey: ed25519.NewKeyFromSeed(decode("receiver_seed_hex", hex.DecodeString)),
		sealingKey:  sealingKey,
		request:     b64("enrol_request"), approval: b64("enrol_approval"), selectorApproval: b64("selector_approval"),
		secretVersion: b64("secret_version"), sealedSecret: b64("sealed_secret"),
		submission: b64("submission"), capability: b64("capability"), named: named,
	}
}

func (records v1Records) state(t *testing.T, envelopes ...[]byte) *State {
	t.Helper()
	state := NewState(records.userKey.Public().(ed25519.PublicKey))
	for _, envelope := range envelopes {
		if _, err := state.Apply(envelope); err != nil {
			t.Fatal(err)
		}
	}
	return state
}

func envelopeVersion(t *testing.T, encoded []byte) int64 {
	t.Helper()
	envelope, err := wire.DecodeEnvelope(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return envelope.V
}

func TestV1ApprovalWithoutSelectorEnrols(t *testing.T) {
	records := loadV1Records(t)
	if envelopeVersion(t, records.approval) != 1 {
		t.Fatal("golden approval is not version 1")
	}
	state := records.state(t, records.request, records.approval)
	enrolled, err := state.Receiver("receiver-v1", testNow)
	if err != nil {
		t.Fatal(err)
	}
	if enrolled.Generation != 1 || enrolled.Pool != "default" || enrolled.ApprovalHash != EnvelopeHash(records.approval) {
		t.Fatalf("unexpected enrolment: %#v", enrolled)
	}
	capability, err := CapabilityWithin(records.capability, enrolled)
	if err != nil {
		t.Fatalf("version 1 capability: %v", err)
	}
	if capability.Generation != 1 || len(capability.Offers) != 1 {
		t.Fatalf("unexpected capability: %#v", capability)
	}
}

func TestV1SubmissionWithLabelsVerifies(t *testing.T) {
	records := loadV1Records(t)
	if envelopeVersion(t, records.submission) != 1 {
		t.Fatal("golden submission is not version 1")
	}
	submission, err := VerifyRootSubmission(records.submission, records.userKey.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	if submission.Wherry.RunID != "run-v1" || len(submission.Wherry.Selector) != 1 || submission.Wherry.Selector[0].Key != "runtime" {
		t.Fatalf("unexpected submission: %#v", submission.Wherry)
	}
}

func TestV1SelectorApprovalCountsButGrantsNothing(t *testing.T) {
	records := loadV1Records(t)
	state := records.state(t, records.request, records.approval, records.selectorApproval)
	current, hash, ok := state.Approval("receiver-v1")
	if !ok || current.Generation != 2 || hash != EnvelopeHash(records.selectorApproval) {
		t.Fatalf("selector approval is not the current generation: %#v", current)
	}
	if _, err := state.Receiver("receiver-v1", testNow); !errors.Is(err, ErrSelectorApproval) {
		t.Fatalf("got %v, want ErrSelectorApproval", err)
	}
	renewal, err := Renew(current, hash, records.userKey, time.UnixMilli(current.ExpMS).Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Apply(renewal); !errors.Is(err, ErrSelectorApproval) {
		t.Fatalf("renewal of a selector approval: got %v, want ErrSelectorApproval", err)
	}
	renewRequest := mustSigned(t, wire.TypeRenewRequest, &wire.RenewRequest{ReceiverID: "receiver-v1", Generation: 2, AtMS: testNow.UnixMilli()}, records.receiverKey)
	if _, err := state.Apply(renewRequest); err != nil {
		t.Fatal(err)
	}
	if _, pending := state.RenewRequest("receiver-v1"); pending {
		t.Fatal("renew request for a selector approval is pending")
	}

	decoded, err := wire.DecodePayload(wire.TypeEnrolRequest, mustPayload(t, records.request))
	if err != nil {
		t.Fatal(err)
	}
	reused, err := Approve(records.request, decoded.(*wire.EnrolRequest), records.userKey, map[string]string{}, "default", current.Adapters, "auto", 2, hash, testNow.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Apply(reused); err == nil {
		t.Fatal("generation 2 was reused")
	}
	next, err := Approve(records.request, decoded.(*wire.EnrolRequest), records.userKey, map[string]string{}, "default", current.Adapters, "auto", 3, hash, testNow.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Apply(next); err != nil {
		t.Fatal(err)
	}
	if enrolled, err := state.Receiver("receiver-v1", testNow); err != nil || enrolled.Generation != 3 {
		t.Fatalf("generation 3: %#v, %v", enrolled, err)
	}
}

func TestV1FoundedGenerationRenewsAsV2(t *testing.T) {
	records := loadV1Records(t)
	state := records.state(t, records.request, records.approval, records.secretVersion)
	current, hash, _ := state.Approval("receiver-v1")
	renewal, err := Renew(current, hash, records.userKey, time.UnixMilli(current.ExpMS).Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if envelopeVersion(t, renewal) != 2 {
		t.Fatal("renewal is not version 2")
	}
	if _, err := state.Apply(renewal); err != nil {
		t.Fatal(err)
	}
	if _, head, _ := state.Approval("receiver-v1"); head != EnvelopeHash(renewal) {
		t.Fatal("renewal is not the head")
	}
	enrolled, err := state.Receiver("receiver-v1", time.UnixMilli(current.ExpMS).Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if enrolled.Generation != 1 || enrolled.ApprovalHash != EnvelopeHash(records.approval) {
		t.Fatalf("renewal moved the founding approval: %#v", enrolled)
	}
	plaintext, _, _, err := Open(records.sealedSecret, state, "receiver-v1", "token", records.sealingKey, records.userKey.Public().(ed25519.PublicKey), testNow)
	if err != nil {
		t.Fatal(err)
	}
	if string(plaintext) != "v1 secret" {
		t.Fatalf("got %q", plaintext)
	}
}

func mustPayload(t *testing.T, encoded []byte) []byte {
	t.Helper()
	envelope, err := wire.DecodeEnvelope(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return envelope.Payload
}

func TestV1SelectorRenewalsChainToNextGeneration(t *testing.T) {
	records := loadV1Records(t)
	state := records.state(t, records.named["chain_request"], records.named["chain_founding"],
		records.named["chain_renewal_1"], records.named["chain_renewal_2"])
	current, hash, _ := state.Approval("receiver-chain")
	if current.Generation != 1 || hash != EnvelopeHash(records.named["chain_renewal_2"]) {
		t.Fatalf("v1 renewals of a selector approval did not advance the head: generation %d", current.Generation)
	}
	if _, err := state.Receiver("receiver-chain", time.UnixMilli(current.ExpMS).Add(-time.Minute)); !errors.Is(err, ErrSelectorApproval) {
		t.Fatalf("renewed selector approval: got %v, want ErrSelectorApproval", err)
	}
	if _, err := state.Apply(records.named["chain_next"]); err != nil {
		t.Fatalf("generation 2 chained to the last v1 renewal: %v", err)
	}
	enrolled, err := state.Receiver("receiver-chain", testNow)
	if err != nil || enrolled.Generation != 2 || enrolled.ApprovalHash != EnvelopeHash(records.named["chain_next"]) {
		t.Fatalf("generation 2: %#v, %v", enrolled, err)
	}
}

func TestV1SelectorRenewalRejectsChangedSelector(t *testing.T) {
	records := loadV1Records(t)
	state := records.state(t, records.named["chain_request"], records.named["chain_founding"])
	_, hash, _ := state.Approval("receiver-chain")
	if _, err := state.Apply(records.named["chain_widened"]); err == nil {
		t.Fatal("v1 renewal with a different selector was accepted")
	}
	if _, head, _ := state.Approval("receiver-chain"); head != hash {
		t.Fatal("rejected renewal became the head")
	}
}

func TestV1RenewedGenerationRenewsAsV2(t *testing.T) {
	records := loadV1Records(t)
	state := records.state(t, records.named["renewed_request"], records.named["renewed_founding"],
		records.named["renewed_renewal"], records.named["renewed_version"])
	current, hash, _ := state.Approval("receiver-renewed")
	if hash != EnvelopeHash(records.named["renewed_renewal"]) {
		t.Fatal("head is not the v1 renewal")
	}
	renewal, err := Renew(current, hash, records.userKey, time.UnixMilli(current.ExpMS).Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if envelopeVersion(t, renewal) != 2 {
		t.Fatal("renewal is not version 2")
	}
	if _, err := state.Apply(renewal); err != nil {
		t.Fatalf("v2 renewal of a v1 renewal: %v", err)
	}
	enrolled, err := state.Receiver("receiver-renewed", testNow)
	if err != nil || enrolled.ApprovalHash != EnvelopeHash(records.named["renewed_founding"]) {
		t.Fatalf("founding hash moved: %#v, %v", enrolled, err)
	}
	plaintext, _, _, err := Open(records.named["renewed_sealed"], state, "receiver-renewed", "token", records.sealingKey, records.userKey.Public().(ed25519.PublicKey), testNow)
	if err != nil || string(plaintext) != "renewed secret" {
		t.Fatalf("seal: %q, %v", plaintext, err)
	}
}
