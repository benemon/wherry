package wire

import (
	"crypto/ed25519"
	"crypto/rand"
	"strings"
	"testing"

	"github.com/benemon/wherry/internal/signed"
)

const testTime = "2026-10-19T10:00:00Z"

var testHash = strings.Repeat("a", 64)

func testMessage(run string) Message {
	return Message{MessageID: "message-a", ContextID: run, TaskID: run, Role: RoleUser, Parts: []Part{{Text: "hello"}}, Extensions: []string{ExtensionURI}}
}

func testBinding() ReceiverBinding {
	return ReceiverBinding{Receiver: "receiver-a", Tenant: "tenant-a", Pool: "pool-a", Run: "run-a", Turn: 1, Attempt: 1}
}

func testRecords() map[string]Record {
	placement := Placement{Agent: "claude", Runtime: "podman", Labels: map[string]string{}}
	status := TaskStatus{State: "TASK_STATE_WORKING", Timestamp: testTime}
	return map[string]Record{
		"submission": &Submission{Message: testMessage("run-a"), Wherry: SubmissionBinding{Tenant: "tenant-a", Host: "host-a", Pool: "pool-a", Run: "run-a", Turn: 1, Placement: placement}},
		"cancel":     &Cancel{Tenant: "tenant-a", Host: "host-a", Pool: "pool-a", Run: "run-a", Turn: 1},
		"status": &RunStatus{StatusUpdate: TaskStatusUpdateEvent{TaskID: "run-a", ContextID: "run-a", Status: status},
			Wherry: StatusBinding{ReceiverBinding: testBinding(), EventID: "event-a", Kind: "started", TaskHash: testHash}},
		"chunk": &Chunk{ArtifactUpdate: TaskArtifactUpdateEvent{TaskID: "run-a", ContextID: "run-a", Artifact: Artifact{ArtifactID: "transcript", Parts: []Part{{Text: "line"}}, Extensions: []string{ExtensionURI}}},
			Wherry: ChunkBinding{ReceiverBinding: testBinding(), TaskHash: testHash, Seq: 0}},
		"result": &Result{Task: Task{ID: "run-a", ContextID: "run-a", Status: TaskStatus{State: "TASK_STATE_COMPLETED", Timestamp: testTime}, Artifacts: []Artifact{}, History: []Message{testMessage("run-a")}},
			Wherry: ResultBinding{ReceiverBinding: testBinding(), TaskHash: testHash}},
		"echo":          &Echo{Receiver: "receiver-a", Tenant: "tenant-a", Pool: "pool-a", Run: "run-a", Turn: 1, Placement: placement, TaskHash: testHash, MessageID: "message-a"},
		"advertisement": &Advertisement{Receiver: "receiver-a", Pool: "pool-a", Offers: []Offer{{Adapter: "nomad", Labels: map[string]string{"agent": "claude", "runtime": "podman"}}}, Capacity: 2, SealingKey: "sha256:x", AtMS: 1},
		"start failure": &StartFailure{Wherry: testBinding(), Adapter: "nomad", Agent: "claude", Runtime: "podman", Class: "launch"},
	}
}

func testKeys(t *testing.T) (signed.Keys, ed25519.PrivateKey, ed25519.PrivateKey) {
	_, host, _ := ed25519.GenerateKey(rand.Reader)
	_, receiver, _ := ed25519.GenerateKey(rand.Reader)
	return signed.Keys{
		signed.HostOwner("tenant-a", "host-a"): {Current: host.Public().(ed25519.PublicKey)},
		"receiver-a":                           {Current: receiver.Public().(ed25519.PublicKey)},
	}, host, receiver
}

func TestRecordsRoundTrip(t *testing.T) {
	keys, host, receiver := testKeys(t)
	for name, record := range testRecords() {
		key := receiver
		if name == "submission" || name == "cancel" {
			key = host
		}
		encoded, err := SignRecord(record, key)
		if err != nil {
			t.Fatalf("%s: sign: %v", name, err)
		}
		decoded := testRecords()[name]
		if err := ParseRecord(encoded, keys, decoded); err != nil {
			t.Fatalf("%s: parse: %v", name, err)
		}
	}
}

func TestRecordSignedByAnotherOwnerIsRejected(t *testing.T) {
	keys, host, receiver := testKeys(t)
	for name, record := range testRecords() {
		key := host
		if name == "submission" || name == "cancel" {
			key = receiver
		}
		encoded, err := SignRecord(record, key)
		if err != nil {
			t.Fatalf("%s: sign: %v", name, err)
		}
		if err := ParseRecord(encoded, keys, testRecords()[name]); err == nil {
			t.Fatalf("%s: accepted a record signed by another owner's key", name)
		}
	}
}

func TestRecordTypesAreNotInterchangeable(t *testing.T) {
	keys, _, receiver := testKeys(t)
	encoded, err := SignRecord(testRecords()["status"], receiver)
	if err != nil {
		t.Fatal(err)
	}
	if err := ParseRecord(encoded, keys, &Chunk{}); err == nil {
		t.Fatal("accepted a status as a chunk")
	}
}

func TestRecordValidation(t *testing.T) {
	_, host, receiver := testKeys(t)
	cases := map[string]func(map[string]Record) Record{
		"submission turn 0": func(r map[string]Record) Record {
			value := r["submission"].(*Submission)
			value.Wherry.Turn = 0
			return value
		},
		"submission message for another run": func(r map[string]Record) Record {
			value := r["submission"].(*Submission)
			value.Message.TaskID = "run-b"
			return value
		},
		"submission without agent": func(r map[string]Record) Record {
			value := r["submission"].(*Submission)
			value.Wherry.Placement.Agent = ""
			return value
		},
		"status of unknown kind": func(r map[string]Record) Record {
			value := r["status"].(*RunStatus)
			value.Wherry.Kind = "other"
			return value
		},
		"status for another run": func(r map[string]Record) Record {
			value := r["status"].(*RunStatus)
			value.StatusUpdate.TaskID = "run-b"
			return value
		},
		"status attempt 0": func(r map[string]Record) Record {
			value := r["status"].(*RunStatus)
			value.Wherry.Attempt = 0
			return value
		},
		"chunk with two parts": func(r map[string]Record) Record {
			value := r["chunk"].(*Chunk)
			value.ArtifactUpdate.Artifact.Parts = append(value.ArtifactUpdate.Artifact.Parts, Part{Text: "more"})
			return value
		},
		"chunk without task hash": func(r map[string]Record) Record {
			value := r["chunk"].(*Chunk)
			value.Wherry.TaskHash = ""
			return value
		},
		"result for another run": func(r map[string]Record) Record {
			value := r["result"].(*Result)
			value.Task.ID = "run-b"
			return value
		},
		"echo without hash": func(r map[string]Record) Record {
			value := r["echo"].(*Echo)
			value.TaskHash = ""
			return value
		},
		"advertisement without offers": func(r map[string]Record) Record {
			value := r["advertisement"].(*Advertisement)
			value.Offers = nil
			return value
		},
		"advertisement without sealing key": func(r map[string]Record) Record {
			value := r["advertisement"].(*Advertisement)
			value.SealingKey = ""
			return value
		},
		"start failure without class": func(r map[string]Record) Record {
			value := r["start failure"].(*StartFailure)
			value.Class = ""
			return value
		},
	}
	for name, mutate := range cases {
		key := receiver
		if strings.HasPrefix(name, "submission") {
			key = host
		}
		if _, err := SignRecord(mutate(testRecords()), key); err == nil {
			t.Fatalf("signed an invalid record: %s", name)
		}
	}
}

func TestRecordMustNameItsSigner(t *testing.T) {
	keys, _, receiver := testKeys(t)
	submission := testRecords()["submission"].(*Submission)
	submission.V = 1
	borrowed, err := signed.Sign(submission.jwsType(), "receiver-a", submission, receiver)
	if err != nil {
		t.Fatal(err)
	}
	if err := ParseRecord(borrowed, keys, &Submission{}); err == nil {
		t.Fatal("accepted a submission naming a host but signed under a receiver's kid")
	}
	status := testRecords()["status"].(*RunStatus)
	status.V = 2
	future, err := signed.Sign(status.jwsType(), "receiver-a", status, receiver)
	if err != nil {
		t.Fatal(err)
	}
	if err := ParseRecord(future, keys, &RunStatus{}); err == nil {
		t.Fatal("accepted a version-2 payload under a version-1 type")
	}
}

func TestParseValidatesWhatItAccepts(t *testing.T) {
	keys, _, receiver := testKeys(t)
	status := testRecords()["status"].(*RunStatus)
	status.V, status.Wherry.Kind = 1, "other"
	encoded, err := signed.Sign(status.jwsType(), "receiver-a", status, receiver)
	if err != nil {
		t.Fatal(err)
	}
	if err := ParseRecord(encoded, keys, &RunStatus{}); err == nil {
		t.Fatal("accepted an invalid status signed outside SignRecord")
	}
}
