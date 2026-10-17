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
	TypeSubmission,
	TypeCancel,
	TypeStatus,
	TypeTask,
	TypeArtifactUpdate,
	TypeCapability,
	TypeEnrolRequest,
	TypeEnrolApproval,
	TypeRenewRequest,
	TypeRevocation,
	TypeSecretVersion,
	TypeSealedSecret,
}

var vectorFilenames = []string{
	"submission-ok.json",
	"cancel-ok.json",
	"status-ok.json",
	"task-ok.json",
	"artifact_update-ok.json",
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
	"artifact-update-two-parts.json",
	"enrol-request-not-self-signed.json",
	"unentitled-signer.json",
	"empty-label-value.json",
	"attempt-zero.json",
	"turn-zero.json",
	"continuation-ok.json",
	"submission-parts-ok.json",
	"part-two-contents.json",
	"part-no-content.json",
	"metadata-not-object.json",
	"raw-invalid-base64.json",
	"submission-task-id-mismatch.json",
	"message-extension-missing.json",
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

	add("duplicate-member.json", "duplicate payload member", Sign(TypeRevocation, []byte(`{"receiver_id":"receiver-1","receiver_id":"receiver-2","floor_generation":1,"at_ms":1}`), privateKey), SignerUser, "invalid_payload")
	add("unknown-member.json", "unknown payload member", Sign(TypeRevocation, []byte(`{"receiver_id":"receiver-1","floor_generation":1,"at_ms":1,"unexpected":true}`), privateKey), SignerUser, "invalid_payload")
	add("trailing-value.json", "trailing payload value", Sign(TypeRevocation, []byte(`{"receiver_id":"receiver-1","floor_generation":1,"at_ms":1} {}`), privateKey), SignerUser, "invalid_payload")

	statusPayload := mustPayload(TypeStatus, payloads[TypeStatus])
	crossType := Sign(TypeStatus, statusPayload, privateKey)
	crossType.Type = TypeTask
	add("cross-type-signature.json", "status signature presented as task", crossType, SignerReceiver, "invalid_signature")

	cancelPayload := mustPayload(TypeCancel, payloads[TypeCancel])
	altered := Sign(TypeCancel, cancelPayload, privateKey)
	altered.Payload = Bytes(strings.Replace(string(altered.Payload), `"id":"run-1"`, `"id":"run-2"`, 1))
	add("cancel-altered-payload.json", "altered payload byte", altered, SignerUser, "invalid_signature")

	add("float-numeric.json", "float numeric field", Sign(TypeRevocation, []byte(`{"receiver_id":"receiver-1","floor_generation":1,"at_ms":1.5}`), privateKey), SignerUser, "invalid_payload")
	hash := strings.Repeat("a", 64)
	both := replaceOnce(t, mustPayload(TypeArtifactUpdate, payloads[TypeArtifactUpdate]), `"mediaType":"application/x-ndjson"}]`,
		`"mediaType":"application/x-ndjson"},{"url":"unstored:oversize","metadata":{"sha256":"`+hash+`","size":4}}]`)
	add("artifact-update-two-parts.json", "transcript chunk with a line and a pointer", Sign(TypeArtifactUpdate, both, privateKey), SignerReceiver, "invalid_payload")
	zeroAttempt := replaceOnce(t, statusPayload, `"attempt":1`, `"attempt":0`)
	add("attempt-zero.json", "zero attempt", Sign(TypeStatus, zeroAttempt, privateKey), SignerReceiver, "invalid_payload")
	zeroTurn := replaceOnce(t, statusPayload, `"turn":1`, `"turn":0`)
	add("turn-zero.json", "zero turn", Sign(TypeStatus, zeroTurn, privateKey), SignerReceiver, "invalid_payload")
	continuation := *payloads[TypeSubmission].(*SubmissionRecord)
	continuation.Message.MessageID = "message-2"
	continuation.Wherry.Turn = 2
	add("continuation-ok.json", "continuation submission", Sign(TypeSubmission, mustPayload(TypeSubmission, &continuation), privateKey), SignerUser, "ok")

	request := payloads[TypeEnrolRequest].(*EnrolRequest)
	request.SigningPub = bytes.Repeat([]byte{9}, ed25519.PublicKeySize)
	add("enrol-request-not-self-signed.json", "enrolment signing key differs", Sign(TypeEnrolRequest, mustPayload(TypeEnrolRequest, request), privateKey), SignerReceiver, "not_self_signed")
	add("unentitled-signer.json", "receiver verifies submission", Sign(TypeSubmission, mustPayload(TypeSubmission, payloads[TypeSubmission]), privateKey), SignerReceiver, "unentitled_signer")

	submissionPayload := mustPayload(TypeSubmission, payloads[TypeSubmission])
	emptyLabel := replaceOnce(t, submissionPayload, `"labels":{"lab/os":"linux"}`, `"labels":{"lab/os":""}`)
	add("empty-label-value.json", "empty label value", Sign(TypeSubmission, emptyLabel, privateKey), SignerUser, "invalid_payload")
	mismatch := replaceOnce(t, submissionPayload, `"taskId":"run-1"`, `"taskId":"run-2"`)
	add("submission-task-id-mismatch.json", "message taskId differs from run_id", Sign(TypeSubmission, mismatch, privateKey), SignerUser, "invalid_payload")
	undeclared := replaceOnce(t, submissionPayload, `,"extensions":["`+ExtensionURI+`"]`, ``)
	add("message-extension-missing.json", "message without the wherry extension", Sign(TypeSubmission, undeclared, privateKey), SignerUser, "invalid_payload")

	parts := payloads[TypeSubmission].(*SubmissionRecord)
	parts.Message.Metadata = jsontext.Value(`{"origin":"vector","tags":["a","b"]}`)
	parts.Message.Parts = []Part{
		{Text: "hello"},
		{Raw: StdBytes{0xfb, 0xff}, MediaType: "application/octet-stream", Filename: "blob.bin"},
		{URL: "https://example.test/report.pdf", MediaType: "application/pdf"},
		{Data: jsontext.Value(`{"rows":[1,2.5,null]}`), MediaType: "application/json", Metadata: jsontext.Value(`{"schema":"v1"}`)},
	}
	add("submission-parts-ok.json", "submission with raw, url and data parts and metadata", Sign(TypeSubmission, mustPayload(TypeSubmission, parts), privateKey), SignerUser, "ok")
	add("part-two-contents.json", "part with text and url", Sign(TypeSubmission, submissionJSON(t, `"parts":[{"text":"hello","url":"https://example.test/a"}]`), privateKey), SignerUser, "invalid_payload")
	add("part-no-content.json", "part with no content", Sign(TypeSubmission, submissionJSON(t, `"parts":[{"mediaType":"text/plain"}]`), privateKey), SignerUser, "invalid_payload")
	add("metadata-not-object.json", "message metadata array", Sign(TypeSubmission, submissionJSON(t, `"parts":[{"text":"hello"}],"metadata":["a"]`), privateKey), SignerUser, "invalid_payload")
	add("raw-invalid-base64.json", "raw not base64", Sign(TypeSubmission, submissionJSON(t, `"parts":[{"raw":"aGk*"}]`), privateKey), SignerUser, "invalid_payload")

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

func replaceOnce(t *testing.T, payload []byte, old, replacement string) []byte {
	t.Helper()
	if bytes.Count(payload, []byte(old)) != 1 {
		t.Fatalf("payload does not hold %s once: %s", old, payload)
	}
	return bytes.Replace(payload, []byte(old), []byte(replacement), 1)
}
