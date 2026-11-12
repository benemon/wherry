package wire

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"strings"
	"testing"

	"github.com/benemon/wherry/internal/signed"
	jose "github.com/go-jose/go-jose/v4"
)

func testSnapshot() *Snapshot {
	return &Snapshot{Pool: "pool-a", Custodian: "receiver-a", Reason: "bootstrap", Current: PoolKey{KeyID: signed.KeyThumbprint(&testPoolKey.PublicKey), Key: jose.JSONWebKey{Key: &testPoolKey.PublicKey}}}
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
	if err != nil || snapshot.Custodian != "receiver-a" || snapshot.Current.KeyID != signed.KeyThumbprint(&testPoolKey.PublicKey) {
		t.Fatalf("parse %+v, %v", snapshot, err)
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.Split(string(encoded), ".")[1])
	if err != nil {
		t.Fatal(err)
	}
	valid := string(payload)
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
		"handover reason":           {"receiver-a", keyA, strings.Replace(valid, `"bootstrap"`, `"handover"`, 1), "invalid custodian snapshot"},
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
		"supplied credentials":    {func(b *SubmissionBinding) { b.Supplied = []string{"model", "github-token"} }, true},
		"a duplicate supplied":    {func(b *SubmissionBinding) { b.Supplied = []string{"model", "model"} }, false},
		"an invalid supplied":     {func(b *SubmissionBinding) { b.Supplied = []string{"Model Key"} }, false},
	} {
		submission := testRecords()["submission"].(*Submission)
		test.mutate(&submission.Wherry)
		if _, err := SignRecord(submission, key); (err == nil) != test.valid {
			t.Errorf("%s: sign = %v, want valid %v", name, err, test.valid)
		}
	}
}

// A submission naming the agent credentials it does not supply, as one before supplied_credentials did, decodes as
// supplying none.
func TestUnsuppliedCredentialsDecodeAsSupplyingNone(t *testing.T) {
	keys, host, _ := testKeys(t)
	submission := testRecords()["submission"].(*Submission)
	submission.V = 1
	encoded, err := json.Marshal(submission)
	if err != nil {
		t.Fatal(err)
	}
	payload := jsontext.Value(strings.Replace(string(encoded), `"task_started_by"`, `"unsupplied_credentials":["model"],"task_started_by"`, 1))
	if !strings.Contains(string(payload), "unsupplied_credentials") {
		t.Fatalf("payload %s carries no unsupplied_credentials", payload)
	}
	signedTask, err := signed.Sign(submission.jwsType(), signed.HostOwner("tenant-a", "host-a"), payload, host)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Submission
	if err := ParseRecord(signedTask, keys, &decoded); err != nil || decoded.Wherry.Supplied != nil {
		t.Fatalf("parse = %v, supplied %v; want the task supplying none", err, decoded.Wherry.Supplied)
	}
}

// An envelope opened against a task that differs from its binding in any of the five members is refused.
func TestCheckBindingRefusesEachOtherTask(t *testing.T) {
	task := validSubmission()
	contentKey := make([]byte, 32)
	envelope, err := signed.BuildEnvelope(signed.EnvelopeValues{Binding: task.Wherry.EnvelopeBinding(), Credentials: map[string][]byte{},
		S3Inputs: map[string]string{}, S3Outputs: map[string]string{}, RedactionKey: make([]byte, 32)}, contentKey)
	if err != nil {
		t.Fatal(err)
	}
	values, err := signed.OpenEnvelope(envelope, contentKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckBinding(task, values.Binding); err != nil {
		t.Fatalf("check against its own task: %v", err)
	}
	for name, mutate := range map[string]func(*SubmissionBinding){
		"run":             func(b *SubmissionBinding) { b.Run = "run-b" },
		"tenant":          func(b *SubmissionBinding) { b.Tenant = "tenant-b" },
		"pool":            func(b *SubmissionBinding) { b.Pool = "pool-b" },
		"task_started_by": func(b *SubmissionBinding) { b.TaskStartedBy++ },
		"task_ended_by":   func(b *SubmissionBinding) { b.TaskEndedBy++ },
	} {
		other := *task
		mutate(&other.Wherry)
		if err := CheckBinding(&other, values.Binding); err == nil {
			t.Errorf("an envelope opened against a task with another %s passed", name)
		}
	}
}
