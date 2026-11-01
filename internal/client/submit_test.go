package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
)

// anotherRun submits a second run and admits it through receiver.
func (f *fixture) anotherRun(t *testing.T, receiver *testReceiver) claimed {
	t.Helper()
	submitted := f.submit(t, defaultSelector())
	task := receiver.claim(t, f.ctx, submitted.RunID)[0]
	receiver.feed(t, f.ctx, submitted.RunID, receiver.echo(t, task), receiver.status(t, task, 1, "claimed-1", "claimed", wire.TaskStateWorking, nil))
	return task
}

// The local record precedes the task, so a submission whose record cannot be written publishes nothing.
func TestSubmitRecordsTheRunBeforeProducing(t *testing.T) {
	f := newFixture(t)
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
	// The runs directory reads, so the run has no earlier record, but takes no new one.
	if err := os.MkdirAll(filepath.Join(f.stateDir, "runs"), 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(filepath.Join(f.stateDir, "runs"), 0o700) })
	before := f.tasksEnd(t, "pool-a")
	if _, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)}); err == nil {
		t.Fatal("submitted without a local record")
	}
	if f.tasksEnd(t, "pool-a") != before {
		t.Fatal("published a task whose local record was not written")
	}
}

// A task write that fails puts back the record it replaced, so the turn can be submitted again.
func TestAFailedTaskWriteLeavesNoRecord(t *testing.T) {
	f := newFixture(t)
	_, task := f.admitted(t)
	cancelled, cancel := context.WithCancel(f.ctx)
	cancel()
	fresh, err := f.host.submission("run-fresh", "pool-a", task.submission.Wherry.Placement, []wire.Part{{Text: "new"}}, time.Now().Add(time.Minute), time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := wire.SignRecord(fresh, f.host.key)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.host.publish(cancelled, fresh, encoded, time.Now(), make([]byte, 32)); err == nil {
		t.Fatal("publish succeeded on a cancelled context")
	}
	if local, err := f.host.localRun("run-fresh"); err != nil || local != nil {
		t.Fatalf("local record %#v, %v; want none for a run never published", local, err)
	}
}

func TestSweepLeavesAHeldLock(t *testing.T) {
	f := newFixture(t)
	lock := filepath.Join(f.stateDir, "resubmit", "run-a.json.lock")
	unlock, err := f.host.lockFile(f.ctx, lock)
	if err != nil {
		t.Fatal(err)
	}
	f.host.sweep()
	if _, err := os.Stat(lock); err != nil {
		t.Fatalf("sweep removed a held lock: %v", err)
	}
	unlock()
	f.host.sweep()
	if _, err := os.Stat(lock); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("sweep kept an idle lock: %v", err)
	}
}

func TestALockWaitEndsWithItsContext(t *testing.T) {
	f := newFixture(t)
	lock := filepath.Join(f.stateDir, "resubmit", "run-a.json.lock")
	unlock, err := f.host.lockFile(f.ctx, lock)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	ctx, cancel := context.WithTimeout(f.ctx, 200*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		release, err := f.host.lockFile(ctx, lock)
		if err == nil {
			release()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("took a lock another holder holds")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the lock wait outlived its context")
	}
}

// admittedWith submits a task declaring credentials through receiver-a, and writes its echo and claimed status.
func (f *fixture) admittedWith(t *testing.T, credentials []wire.TaskCredential) (*testReceiver, claimed) {
	t.Helper()
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	submitted, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "do the work", Credentials: credentials, EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	task := receiver.claim(t, f.ctx, submitted.RunID)[0]
	receiver.feed(t, f.ctx, submitted.RunID, receiver.echo(t, task), receiver.status(t, task, 1, "claimed-1", "claimed", wire.TaskStateWorking, nil))
	return receiver, task
}

// Redact replaces each credential value the host resolved, trimmed and JSON-escaped, with the marker of its name
// under no key, and leaves a value under 8 bytes.
func TestRedactReplacesResolvedCredentials(t *testing.T) {
	f := newFixture(t)
	dir := t.TempDir()
	for name, value := range map[string]string{"quoted-key": "say \"held\" key\n", "short-key": "abc1234"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
		f.host.credentials[name] = credential.Source{File: filepath.Join(dir, name)}
	}
	f.admittedWith(t, []wire.TaskCredential{{Name: "quoted-key", File: "quoted"}, {Name: "short-key", File: "short"}})
	marker := signed.RedactionMarker(nil, "quoted-key")
	if got, want := Redact(`raw say "held" key; json "say \"held\" key"; short abc1234`), "raw "+marker+"; json \""+marker+"\"; short abc1234"; got != want {
		t.Fatalf("Redact = %q, want %q", got, want)
	}
}

func TestSubmitRefusesAnUnconfiguredCredential(t *testing.T) {
	f := newFixture(t)
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
	_, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "p", Credentials: []wire.TaskCredential{{Name: "absent", File: "token"}}, EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "auth required: credential absent is not in this host's configuration") {
		t.Fatalf("submit = %v; want the unconfigured credential named", err)
	}
	if records, _ := readTopic(f.ctx, f.service.Admin, admin.PoolTasksTopic("pool-a")); len(records) != 0 {
		t.Fatal("published a task naming an unconfigured credential")
	}
}

// A record whose key is not 32 bytes leaves its markers unnamed.
func TestACorruptRedactionKeyNamesNoMarker(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	local, err := f.host.localRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	local.RedactionKey = local.RedactionKey[:31]
	if err := f.host.writeRun(*local); err != nil {
		t.Fatal(err)
	}
	if after, err := f.host.localRun(runID); err != nil || !bytes.Equal(after.RedactionKey, local.RedactionKey) {
		t.Fatalf("local record %+v, %v; want the corrupt key kept", after, err)
	}
	marker := signed.RedactionMarker(local.RedactionKey, "model")
	receiver.feed(t, f.ctx, runID,
		receiver.result(t, task, 1, wire.TaskStateCompleted, func(result *wire.Result) {
			result.Task.Artifacts[0].Parts = []wire.Part{{Text: "printed " + marker}}
		}),
		receiver.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateCompleted, func(status *wire.RunStatus) {
			status.StatusUpdate.Status.Message = &wire.Message{MessageID: "msg-s", Role: wire.RoleAgent, Parts: []wire.Part{{Text: "done with " + marker}}, Extensions: []string{wire.ExtensionURI}}
		}))
	awaited, err := f.host.Await(f.ctx, runID, 10*time.Second)
	if err != nil || awaited.Task == nil || awaited.Task.Artifacts[0].Parts[0].Text != "printed "+marker || len(awaited.Redacted) != 0 {
		t.Fatalf("await %#v, %v; want the marker unmapped", awaited, err)
	}
	status, err := f.host.Status(f.ctx, runID)
	if err != nil || status.Status.Message.Parts[0].Text != "done with "+marker || len(status.Redacted) != 0 {
		t.Fatalf("status %#v, %v; want the marker unmapped", status, err)
	}
}

// A local record turning unreadable while await waits does not turn the delivered outcome into an error.
func TestAwaitDeliversTheOutcomeOfARecordTurnedUnreadable(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	local, err := f.host.localRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	marker := signed.RedactionMarker(local.RedactionKey, "model")
	done := make(chan error, 1)
	var awaited AwaitResult
	go func() {
		var err error
		awaited, err = f.host.Await(f.ctx, runID, 20*time.Second)
		done <- err
	}()
	time.Sleep(time.Second)
	path, _ := f.host.runPath(runID)
	if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	receiver.feed(t, f.ctx, runID,
		receiver.result(t, task, 1, wire.TaskStateCompleted, func(result *wire.Result) {
			result.Task.Artifacts[0].Parts = []wire.Part{{Text: "printed " + marker}}
		}),
		receiver.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateCompleted, nil))
	if err := <-done; err != nil || awaited.Status.State != wire.TaskStateCompleted || awaited.Task.Artifacts[0].Parts[0].Text != "printed [redacted:model]" {
		t.Fatalf("await %#v, %v; want the completed outcome", awaited, err)
	}
}

// Await and status name each marker the run's key gives a credential; the feed carries only the marker.
func TestAwaitAndStatusNameTheRunsMarkers(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	_, values, err := open(t, f, runID, f.poolKeys["pool-a"])
	if err != nil {
		t.Fatal(err)
	}
	marker, other := signed.RedactionMarker(values.RedactionKey, "model"), signed.RedactionMarker(bytes.Repeat([]byte{1}, 32), "model")
	receiver.feed(t, f.ctx, runID,
		receiver.result(t, task, 1, wire.TaskStateCompleted, func(result *wire.Result) {
			result.Task.Artifacts[0].Parts = []wire.Part{{Text: "printed " + marker + " and " + marker + " and " + other}}
		}),
		receiver.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateCompleted, func(status *wire.RunStatus) {
			status.StatusUpdate.Status.Message = &wire.Message{MessageID: "msg-s", Role: wire.RoleAgent, Parts: []wire.Part{{Text: "done with " + marker}}, Extensions: []string{wire.ExtensionURI}}
		}))
	awaited, err := f.host.Await(f.ctx, runID, 10*time.Second)
	if err != nil || awaited.Task == nil {
		t.Fatalf("await %#v, %v", awaited, err)
	}
	if text := awaited.Task.Artifacts[0].Parts[0].Text; text != "printed [redacted:model] and [redacted:model] and "+other || !maps.Equal(awaited.Redacted, map[string]int{"model": 2}) {
		t.Fatalf("await shows %q counting %v, want model's two markers named and another run's left", text, awaited.Redacted)
	}
	status, err := f.host.Status(f.ctx, runID)
	if err != nil || status.Status.Message.Parts[0].Text != "done with [redacted:model]" || !maps.Equal(status.Redacted, map[string]int{"model": 1}) {
		t.Fatalf("status %#v, %v; want the marker named", status, err)
	}
}

func TestSubmitRefusesALongCredentialName(t *testing.T) {
	f := newFixture(t)
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
	name := strings.Repeat("c", 33)
	f.host.credentials[name] = credential.Source{File: f.secret}
	_, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "p", Credentials: []wire.TaskCredential{{Name: name, File: "token"}}, EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "credential names are at most 32") {
		t.Fatalf("submit = %v; want the name refused", err)
	}
	if records, _ := readTopic(f.ctx, f.service.Admin, admin.PoolTasksTopic("pool-a")); len(records) != 0 {
		t.Fatal("published a credential name over 32 characters")
	}
}

func TestSubmitRefusesOver256ExpectedOutputs(t *testing.T) {
	f := newFixture(t)
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
	var outputs []wire.ExpectedOutput
	for i := range wire.MaxOutputs + 1 {
		outputs = append(outputs, wire.ExpectedOutput{Name: "o" + strings.Repeat("x", i%10) + string(rune('a'+i%26)) + strings.Repeat("y", i/26)})
	}
	_, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "p", Outputs: outputs, EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "a submission expects at most 256 outputs") {
		t.Fatalf("submit = %v; want the outputs refused", err)
	}
	if records, _ := readTopic(f.ctx, f.service.Admin, admin.PoolTasksTopic("pool-a")); len(records) != 0 {
		t.Fatal("published over 256 expected outputs")
	}
}

func TestSubmitNamesARecordTheBrokerRefuses(t *testing.T) {
	f := newFixture(t)
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
	limit := "2000"
	if responses, err := kadm.NewClient(f.service.Admin).AlterTopicConfigs(f.ctx, []kadm.AlterConfig{{Op: kadm.SetConfig, Name: "max.message.bytes", Value: &limit}}, admin.PoolTasksTopic("pool-a")); err != nil || responses[0].Err != nil {
		t.Fatalf("alter topic: %v %v", err, responses)
	}
	random := make([]byte, 10000)
	rand.Read(random)
	_, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: hex.EncodeToString(random), EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "-byte record, over the broker's record limit; shorten the prompt or the input files") {
		t.Fatalf("submit = %v; want the record size and a remedy", err)
	}
}
