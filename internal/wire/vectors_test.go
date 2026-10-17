package wire

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var updateVectors = flag.Bool("update-vectors", false, "rewrite published wire test vectors")

var vectorRecordTypes = []RecordType{
	TypeTask,
	TypeCancel,
	TypeEvent,
	TypeReply,
	TypeTranscript,
	TypeCapability,
	TypeEnrolRequest,
	TypeEnrolApproval,
	TypeRenewRequest,
	TypeRevocation,
	TypeSecretVersion,
	TypeSealedSecret,
}

var vectorFilenames = []string{
	"task-ok.json",
	"cancel-ok.json",
	"event-ok.json",
	"reply-ok.json",
	"transcript-ok.json",
	"capability-ok.json",
	"enrol_request-ok.json",
	"enrol_approval-ok.json",
	"renew_request-ok.json",
	"revocation-ok.json",
	"secret_version-ok.json",
	"sealed_secret-ok.json",
	"duplicate-member.json",
	"unknown-member.json",
	"trailing-value.json",
	"cross-type-signature.json",
	"cancel-altered-payload.json",
	"float-numeric.json",
	"transcript-both.json",
	"enrol-request-not-self-signed.json",
	"unentitled-signer.json",
	"empty-label-value.json",
	"attempt-zero.json",
}

type publishedVector struct {
	Name          string     `json:"name"`
	Type          RecordType `json:"type"`
	SignerSeedHex string     `json:"signer_seed_hex"`
	SignerKind    SignerKind `json:"signer_kind"`
	PayloadB64    string     `json:"payload_b64"`
	EnvelopeB64   string     `json:"envelope_b64"`
	Expect        string     `json:"expect"`
}

func TestPublishedVectors(t *testing.T) {
	if *updateVectors {
		writePublishedVectors(t)
	}
	paths, err := filepath.Glob("testdata/vectors/*.json")
	if err != nil {
		t.Fatal(err)
	}
	wanted := make(map[string]bool, len(vectorFilenames))
	for _, filename := range vectorFilenames {
		wanted[filename] = true
	}
	if len(paths) != len(wanted) {
		t.Fatalf("got %d vector files, want %d", len(paths), len(wanted))
	}
	for _, path := range paths {
		filename := filepath.Base(path)
		if !wanted[filename] {
			t.Errorf("unexpected vector file %s", filename)
			continue
		}
		t.Run(filename, func(t *testing.T) {
			verifyPublishedVector(t, path)
		})
	}
}

func verifyPublishedVector(t *testing.T, path string) {
	t.Helper()
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var vector publishedVector
	if err := json.Unmarshal(encoded, &vector, strictDecodeOptions); err != nil {
		t.Fatal(err)
	}
	seed, err := hex.DecodeString(vector.SignerSeedHex)
	if err != nil {
		t.Fatal(err)
	}
	privateKey := ed25519.NewKeyFromSeed(seed)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	envelopeBytes, err := base64.RawURLEncoding.Strict().DecodeString(vector.EnvelopeB64)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := DecodeEnvelope(envelopeBytes)
	if err != nil {
		t.Fatal(err)
	}
	if envelope.Type != vector.Type || base64.RawURLEncoding.EncodeToString(envelope.Payload) != vector.PayloadB64 {
		t.Fatal("vector metadata does not match envelope")
	}
	_, err = Verify(envelope, publicKey, vector.SignerKind)
	switch vector.Expect {
	case "ok":
		if err != nil {
			t.Fatal(err)
		}
	case "invalid_signature":
		if !errors.Is(err, errInvalidSignature) {
			t.Fatalf("got %v, want invalid_signature", err)
		}
	case "invalid_payload":
		if !errors.Is(err, errInvalidPayload) {
			t.Fatalf("got %v, want invalid_payload", err)
		}
	case "not_self_signed":
		if !errors.Is(err, errNotSelfSigned) {
			t.Fatalf("got %v, want not_self_signed", err)
		}
	case "unentitled_signer":
		if !errors.Is(err, errUnentitledSigner) {
			t.Fatalf("got %v, want unentitled_signer", err)
		}
	default:
		t.Fatalf("unknown expectation %q", vector.Expect)
	}
}

func writePublishedVectors(t *testing.T) {
	t.Helper()
	const seedHex = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	_, privateKey := fixedKey(t)
	payloads := validPayloads(t)
	vectors := make(map[string]publishedVector, len(vectorFilenames))

	add := func(filename, name string, envelope Envelope, kind SignerKind, expect string) {
		envelopeBytes, err := MarshalEnvelope(envelope)
		if err != nil {
			t.Fatal(err)
		}
		vectors[filename] = publishedVector{
			Name: name, Type: envelope.Type, SignerSeedHex: seedHex, SignerKind: kind,
			PayloadB64:  base64.RawURLEncoding.EncodeToString(envelope.Payload),
			EnvelopeB64: base64.RawURLEncoding.EncodeToString(envelopeBytes), Expect: expect,
		}
	}
	mustPayload := func(recordType RecordType, payload any) []byte {
		encoded, err := MarshalPayload(recordType, payload)
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}

	for _, recordType := range vectorRecordTypes {
		payload := mustPayload(recordType, payloads[recordType])
		add(string(recordType)+"-ok.json", string(recordType)+" ok", Sign(recordType, payload, privateKey), verificationKind(recordType), "ok")
	}

	add("duplicate-member.json", "duplicate payload member", Sign(TypeCancel, []byte(`{"run_id":"run-1","run_id":"run-2","at_ms":1}`), privateKey), SignerUser, "invalid_payload")
	add("unknown-member.json", "unknown payload member", Sign(TypeCancel, []byte(`{"run_id":"run-1","at_ms":1,"unexpected":true}`), privateKey), SignerUser, "invalid_payload")
	add("trailing-value.json", "trailing payload value", Sign(TypeCancel, []byte(`{"run_id":"run-1","at_ms":1} {}`), privateKey), SignerUser, "invalid_payload")

	eventPayload := mustPayload(TypeEvent, payloads[TypeEvent])
	crossType := Sign(TypeEvent, eventPayload, privateKey)
	crossType.Type = TypeReply
	add("cross-type-signature.json", "event signature presented as reply", crossType, SignerReceiver, "invalid_signature")

	cancelPayload := mustPayload(TypeCancel, payloads[TypeCancel])
	altered := Sign(TypeCancel, cancelPayload, privateKey)
	altered.Payload = Bytes(strings.Replace(string(altered.Payload), `"at_ms":1000`, `"at_ms":1001`, 1))
	add("cancel-altered-payload.json", "altered payload byte", altered, SignerUser, "invalid_signature")

	add("float-numeric.json", "float numeric field", Sign(TypeCancel, []byte(`{"run_id":"run-1","at_ms":1.5}`), privateKey), SignerUser, "invalid_payload")
	hash := strings.Repeat("a", 64)
	both := `{"run_id":"run-1","attempt":1,"seq":1,"line":"bGluZQ","pointer":{"url":"https://example.test/line","sha256":"` + hash + `","size":4}}`
	add("transcript-both.json", "transcript line and pointer", Sign(TypeTranscript, []byte(both), privateKey), SignerReceiver, "invalid_payload")
	zeroAttempt := bytes.Replace(eventPayload, []byte(`"attempt":1`), []byte(`"attempt":0`), 1)
	add("attempt-zero.json", "zero attempt", Sign(TypeEvent, zeroAttempt, privateKey), SignerReceiver, "invalid_payload")

	request := payloads[TypeEnrolRequest].(*EnrolRequest)
	request.SigningPub = bytes.Repeat([]byte{9}, ed25519.PublicKeySize)
	add("enrol-request-not-self-signed.json", "enrolment signing key differs", Sign(TypeEnrolRequest, mustPayload(TypeEnrolRequest, request), privateKey), SignerReceiver, "not_self_signed")
	add("unentitled-signer.json", "receiver verifies cancel", Sign(TypeCancel, cancelPayload, privateKey), SignerReceiver, "unentitled_signer")

	taskPayload := mustPayload(TypeTask, payloads[TypeTask])
	emptyLabel := strings.Replace(string(taskPayload), `"labels":{"lab/os":"linux"}`, `"labels":{"lab/os":""}`, 1)
	add("empty-label-value.json", "empty label value", Sign(TypeTask, []byte(emptyLabel), privateKey), SignerUser, "invalid_payload")

	if err := os.MkdirAll("testdata/vectors", 0o755); err != nil {
		t.Fatal(err)
	}
	for _, filename := range vectorFilenames {
		vector, ok := vectors[filename]
		if !ok {
			t.Fatalf("vector %s was not generated", filename)
		}
		encoded, err := json.Marshal(&vector, json.Deterministic(true), jsontext.Multiline(true))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join("testdata/vectors", filename), append(encoded, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
