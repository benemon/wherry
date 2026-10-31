package wire

import (
	"crypto/ed25519"
	"crypto/rand"
	"strings"
	"testing"

	"github.com/benemon/wherry/internal/signed"
	jose "github.com/go-jose/go-jose/v4"
)

func testSnapshot() *Snapshot {
	return &Snapshot{Pool: "pool-a", Custodian: "receiver-a", Reason: "bootstrap", Current: PoolKey{KeyID: signed.SealKeyID(&testSealKey.PublicKey), Key: jose.JSONWebKey{Key: &testSealKey.PublicKey}}}
}

// A snapshot decodes strictly: an unknown or duplicate member, here or in its key, refuses it; and its writer must be
// the custodian it names.
func TestSnapshotDecodesStrictlyAndBindsItsWriter(t *testing.T) {
	_, keyA, _ := ed25519.GenerateKey(rand.Reader)
	_, keyB, _ := ed25519.GenerateKey(rand.Reader)
	keys := signed.Keys{"receiver-a": {Current: keyA.Public().(ed25519.PublicKey)}, "receiver-b": {Current: keyB.Public().(ed25519.PublicKey)}}
	encoded, err := SignRecord(testSnapshot(), keyA)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := ParseSnapshot(encoded, keys)
	if err != nil || snapshot.Custodian != "receiver-a" || snapshot.Current.KeyID != signed.SealKeyID(&testSealKey.PublicKey) {
		t.Fatalf("parse %+v, %v", snapshot, err)
	}
	payload, err := signed.Encode(testSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	valid := strings.Replace(string(payload), `{`, `{"v":1,`, 1)
	valid = strings.Replace(valid, `"v":0,`, ``, 1)
	for name, test := range map[string]struct {
		owner   string
		key     ed25519.PrivateKey
		payload string
		want    string
	}{
		"valid":                     {"receiver-a", keyA, valid, ""},
		"unknown member":            {"receiver-a", keyA, strings.Replace(valid, `"pool":`, `"later":1,"pool":`, 1), "decode snapshot"},
		"duplicate member":          {"receiver-a", keyA, strings.Replace(valid, `"pool":"pool-a"`, `"pool":"pool-a","pool":"pool-a"`, 1), "duplicate"},
		"unknown reason":            {"receiver-a", keyA, strings.Replace(valid, `"bootstrap"`, `"promotion"`, 1), "invalid custodian snapshot"},
		"written by another":        {"receiver-b", keyB, valid, "written by another receiver than the custodian it names"},
		"key ID not the thumbprint": {"receiver-a", keyA, strings.Replace(valid, `"key_id":"`, `"key_id":"x`, 1), "invalid custodian snapshot"},
	} {
		t.Run(name, func(t *testing.T) {
			encoded := signRaw(t, test.owner, []byte(test.payload), test.key)
			_, err = ParseSnapshot(encoded, keys)
			if test.want == "" && err != nil || test.want != "" && (err == nil || !strings.Contains(err.Error(), test.want)) {
				t.Fatalf("parse = %v, want %q", err, test.want)
			}
		})
	}
}

// signRaw signs payload as it is, as a snapshot from owner; signed.Sign would refuse a duplicate member.
func signRaw(t *testing.T, owner string, payload []byte, key ed25519.PrivateKey) []byte {
	t.Helper()
	options := new(jose.SignerOptions).WithType(CustodianType).WithHeader("kid", owner+"/"+signed.KeyID(key.Public().(ed25519.PublicKey)))
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.EdDSA, Key: key}, options)
	if err != nil {
		t.Fatal(err)
	}
	jws, err := signer.Sign(payload)
	if err != nil {
		t.Fatal(err)
	}
	compact, err := jws.CompactSerialize()
	if err != nil {
		t.Fatal(err)
	}
	return []byte(compact)
}

// key-lost is the one wherry event, on a failed terminal status alone.
func TestStatusEventIsKeyLostOnAFailedTerminal(t *testing.T) {
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	for name, test := range map[string]struct {
		event, kind string
		state       TaskState
		valid       bool
	}{
		"key-lost on a failed terminal": {EventKeyLost, "terminal", TaskStateFailed, true},
		"key-lost on a completed one":   {EventKeyLost, "terminal", TaskStateCompleted, false},
		"key-lost on a claim":           {EventKeyLost, "claimed", TaskStateWorking, false},
		"another event":                 {"released", "terminal", TaskStateFailed, false},
	} {
		status := &RunStatus{StatusUpdate: TaskStatusUpdateEvent{TaskID: "run-a", ContextID: "run-a", Status: TaskStatus{State: test.state, Timestamp: testTime}},
			Wherry: StatusBinding{ReceiverBinding: testBinding(), EventID: "event-a", Kind: test.kind, TaskHash: testHash, Event: test.event}}
		if _, err := SignRecord(status, key); (err == nil) != test.valid {
			t.Errorf("%s: sign = %v, want valid %v", name, err, test.valid)
		}
	}
}

// A resubmission names both the run it replaces and its S3 input run; an envelope travels with its wrap.
func TestSubmissionResubmissionAndEnvelopeMembers(t *testing.T) {
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	for name, test := range map[string]struct {
		mutate func(*SubmissionBinding)
		valid  bool
	}{
		"resubmission":            {func(b *SubmissionBinding) { b.Replaces, b.S3InputRun = "run-root", "run-root" }, true},
		"replaces alone":          {func(b *SubmissionBinding) { b.Replaces = "run-root" }, false},
		"s3_input_run alone":      {func(b *SubmissionBinding) { b.S3InputRun = "run-root" }, false},
		"replaces itself":         {func(b *SubmissionBinding) { b.Replaces, b.S3InputRun = "run-a", "run-a" }, false},
		"replaces a non-run":      {func(b *SubmissionBinding) { b.Replaces, b.S3InputRun = "Run A", "run-root" }, false},
		"envelope and wrap":       {func(b *SubmissionBinding) { b.Envelope, b.Wrap = "e", "w" }, true},
		"envelope without wrap":   {func(b *SubmissionBinding) { b.Envelope = "e" }, false},
		"envelope over its bound": {func(b *SubmissionBinding) { b.Envelope, b.Wrap = strings.Repeat("e", signed.MaxEnvelope+1), "w" }, false},
	} {
		submission := testRecords()["submission"].(*Submission)
		test.mutate(&submission.Wherry)
		if _, err := SignRecord(submission, key); (err == nil) != test.valid {
			t.Errorf("%s: sign = %v, want valid %v", name, err, test.valid)
		}
	}
}

// The task core a submission's envelope is bound to is the hash of the payload signed.Sign produces with the envelope
// span removed, also when the message holds raw JSON that a plain encoding would rewrite.
func TestTaskCoreOfTheSignedPayloadIsTheCoresHash(t *testing.T) {
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	submission := testRecords()["submission"].(*Submission)
	submission.Message.Parts = append(submission.Message.Parts, Part{Data: []byte(`{"k":"\u00e9\/"}`), Filename: "data", MediaType: "application/json"})
	var core string
	encoded, err := SignEnveloped(submission, key, func(taskCore string) ([]byte, []byte, error) {
		core = taskCore
		return []byte("envelope"), []byte("wrap"), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	jws, err := jose.ParseSignedCompact(string(encoded), []jose.SignatureAlgorithm{jose.EdDSA})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := jws.Verify(key.Public())
	if err != nil {
		t.Fatal(err)
	}
	if got, err := signed.TaskCore(payload); err != nil || got != core {
		t.Fatalf("task core of the signed payload %q, %v; the envelope was built for %q", got, err, core)
	}
	var decoded Submission
	if err := ParseRecord(encoded, signed.Keys{"tenant-a.host-a": {Current: key.Public().(ed25519.PublicKey)}}, &decoded); err != nil || decoded.Wherry.Envelope != "envelope" || decoded.Wherry.Wrap != "wrap" {
		t.Fatalf("decoded %+v, %v", decoded.Wherry, err)
	}
}
