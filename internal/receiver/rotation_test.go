package receiver

import (
	"crypto/ed25519"
	"crypto/rand"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// rotateSelf rotates f.receiver's own key as `wherry receiver run --rotate-key` does at start, then refreshes.
func rotateSelf(t *testing.T, f *fixture) {
	t.Helper()
	// A retired key counts only strictly before its rotation's broker time, which has millisecond resolution.
	time.Sleep(2 * time.Millisecond)
	_, next, _ := ed25519.GenerateKey(rand.Reader)
	f.produce(t, admin.ReceiverTopic(testReceiver), testReceiver, must(t)(signed.RotateKey(testReceiver, f.receiver.signing, next)))
	f.receiver.signing = next
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
}

func rotatePeer(t *testing.T, f fixture, id string, old ed25519.PrivateKey) {
	t.Helper()
	time.Sleep(2 * time.Millisecond)
	_, next, _ := ed25519.GenerateKey(rand.Reader)
	f.produce(t, admin.ReceiverTopic(id), id, must(t)(signed.RotateKey(id, old, next)))
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
}

// A completed run whose ack was lost is acknowledged on redelivery after its receiver restarts with --rotate-key.
func TestCompletedRunRedeliveredAfterSelfRotation(t *testing.T) {
	for _, rotate := range []bool{false, true} {
		t.Run(map[bool]string{false: "control", true: "rotated"}[rotate], func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.answer(t, credentialName)
			record := f.submit(t, f.host, nil)
			if status := f.execute(t, record, 1); status != kgo.AckAccept {
				t.Fatalf("first delivery %v", status)
			}
			if rotate {
				rotateSelf(t, &f)
				f.answer(t, credentialName)
			}
			admitted, ack := f.receiver.admit(f.ctx, record)
			if admitted != nil || ack != kgo.AckAccept {
				t.Errorf("a committed terminal was not acknowledged on redelivery")
			}
		})
	}
}

// A completed run redelivered past its queue deadline after a rotation is acknowledged on redelivery.
func TestCompletedRunPastQueueDeadlineAfterSelfRotation(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.submit(t, f.host, func(task *wire.Submission) { task.Wherry.QueueDeadlineMS = time.Now().Add(-time.Second).UnixMilli() })
	f.publishStatus(t, record, testReceiver, f.receiver.signing, "terminal", wire.TaskStateCompleted, nil)
	rotateSelf(t, &f)
	admitted, ack := f.receiver.admit(f.ctx, record)
	if admitted != nil || ack != kgo.AckAccept {
		t.Errorf("a committed terminal was not acknowledged on redelivery")
	}
}

// A held run past its queue deadline is admitted for adoption after its holder rotates.
func TestHeldRunPastQueueDeadline(t *testing.T) {
	for _, test := range []struct {
		name   string
		rotate func(*testing.T, *fixture, ed25519.PrivateKey)
		holder string
	}{
		{"control", func(*testing.T, *fixture, ed25519.PrivateKey) {}, testReceiver},
		{"self-rotated", func(t *testing.T, f *fixture, _ ed25519.PrivateKey) { rotateSelf(t, f) }, testReceiver},
		{"peer-rotated", func(t *testing.T, f *fixture, old ed25519.PrivateKey) { rotatePeer(t, *f, "receiver-b", old) }, "receiver-b"},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			key := f.receiver.signing
			if test.holder != testReceiver {
				key = f.addPeer(t, test.holder, testPool)
			}
			record := f.submit(t, f.host, func(task *wire.Submission) { task.Wherry.QueueDeadlineMS = time.Now().Add(-time.Second).UnixMilli() })
			f.publishStatus(t, record, test.holder, key, "claimed", wire.TaskStateWorking, nil)
			f.publishStatus(t, record, test.holder, key, "started", wire.TaskStateWorking, nil)
			test.rotate(t, &f, key)
			admitted, _ := f.receiver.admit(f.ctx, record)
			if admitted == nil {
				t.Errorf("a held run was not admitted for adoption")
			}
		})
	}
}

// An adopter resumes from the offset of a heartbeat signed before the rotation.
func TestLastOffsetAfterSelfRotation(t *testing.T) {
	f := newFixture(t, resultStream)
	task := testSubmission("run-adopt")
	hash := strings.Repeat("a", 64)
	f.produce(t, admin.PoolEventsTopic(testPool), "run-adopt", f.heartbeat(t, task, hash, testReceiver, f.receiver.signing, 42, nil))
	rotateSelf(t, &f)
	got, err := f.receiver.lastOffset(f.ctx, task, hash)
	if err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Errorf("pre-rotation heartbeat offset lost")
	}
}

// A cancel of a run the rotated receiver held still reaches the redelivered run.
func TestCancelOfHeldRunAfterSelfRotation(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.submit(t, f.host, nil)
	f.publishStatus(t, record, testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
	rotateSelf(t, &f)
	f.publishCancel(t, "run-a", 1, f.host, nil)
	admitted, _ := f.receiver.admit(f.ctx, record)
	if admitted == nil || !admitted.cancelled {
		t.Errorf("the redelivered run does not carry its cancel")
	}
	// The adopter's first commit is its own claimed status, under the new key.
	run := testActiveRun(t, f, "run-a", 2)
	if err := run.commit(f.ctx, run.statusRecords("claimed", taskStatus(wire.TaskStateWorking, ""), nil, nil)...); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.readCancels(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.receiver.mu.Lock()
	held := f.receiver.cancelled[runKey{testTenant, "run-a"}]
	f.receiver.mu.Unlock()
	if !held {
		t.Errorf("a held run's cancel is not seen")
	}
}

// A held run ID stays bound to its tenant after the holder rotates, when the owner's submission is unreadable.
func TestRunIDOfAnotherTenantAfterHolderRotation(t *testing.T) {
	for _, rotate := range []bool{false, true} {
		t.Run(map[bool]string{false: "control", true: "rotated"}[rotate], func(t *testing.T) {
			f := newFixture(t, resultStream)
			keyB := f.tenantB(t)
			producer, err := f.receiver.newProducer("run-x")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(producer.Close)
			task := testSubmission("run-x")
			holder := &activeRun{receiver: f.receiver, producer: producer, task: task, taskHash: wire.TaskHash(must(t)(wire.SignRecord(task, f.host))), history: []wire.Message{task.Message}, attempt: 1, profile: f.receiver.profiles[0]}
			if err := holder.commit(f.ctx, holder.statusRecords("claimed", taskStatus(wire.TaskStateWorking, ""), nil, nil)...); err != nil {
				t.Fatal(err)
			}
			if rotate {
				rotateSelf(t, &f)
			}
			record := f.submit(t, keyB, func(task *wire.Submission) {
				asTenantB(task)
				task.Wherry.Run, task.Message.TaskID, task.Message.ContextID = "run-x", "run-x", "run-x"
			})
			admitted, _ := f.receiver.admit(f.ctx, record)
			if admitted != nil {
				t.Errorf("tenant-b was admitted under tenant-a's held run ID")
			}
		})
	}
}

// A status under the retired key appended after the rotation still counts for nothing.
func TestRetiredKeyAfterRotationRefused(t *testing.T) {
	f := newFixture(t, resultStream)
	old := f.receiver.signing
	record := f.submit(t, f.host, func(task *wire.Submission) { task.Wherry.QueueDeadlineMS = time.Now().Add(-time.Second).UnixMilli() })
	rotateSelf(t, &f)
	time.Sleep(10 * time.Millisecond)
	f.publishStatus(t, record, testReceiver, old, "claimed", wire.TaskStateWorking, nil)
	f.publishStatus(t, record, testReceiver, old, "terminal", wire.TaskStateCompleted, nil)
	events, err := f.receiver.readEvents(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	admitted, ack := f.receiver.admit(f.ctx, record)
	if len(events.statuses["run-a"]) != 0 || admitted != nil || ack != kgo.AckReject {
		t.Errorf("a status signed by the retired key after the rotation counted")
	}
}
