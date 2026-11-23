package view

import (
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/wire"
)

func TestReviewRetiredKeyLive(t *testing.T) {
	f := newFixture(t)
	v := f.view(time.Hour, testCaps())
	at := f.now.Add(time.Minute)
	old := f.rotate(receiverA, operator.ReceiverTopic(receiverA), at)
	run := runID(tenantA, hostA, "retired-live")
	f.status(at.Add(time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "terminal", state: wire.TaskStateCompleted, key: old})
	f.tick(v)
	if hasRun(v.Snapshot(0), run) {
		t.Fatal("post-rotation status signed by retired key accepted live")
	}
}

// A rotation read after records signed with the key it retired were folded retracts them: the records were appended
// after the rotation, so they no longer verify.
func TestRetiredKeyRetractsFoldedFacts(t *testing.T) {
	f := newFixture(t)
	v := f.view(time.Hour, testCaps())
	at := f.now.Add(time.Minute)
	run := f.echo(at, receiverA, tenantA, hostA, poolA, "retract")
	f.status(at.Add(2*time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "terminal", state: wire.TaskStateCompleted})
	f.result(at.Add(2*time.Second), receiverA, tenantA, poolA, run, 1, wire.TaskStateCompleted, wire.ResultMetadata{Profile: "profile-a"}, "reply")
	f.tick(v)
	if r := summaryOf(t, v.Snapshot(0), run); !r.Terminal || r.Outputs != 0 {
		t.Fatalf("before the rotation is read: %+v", r)
	}
	// The rotation was appended between the echo and the status, and is read only now.
	f.rotate(receiverA, operator.ReceiverTopic(receiverA), at.Add(time.Second))
	f.tick(v)
	s := v.Snapshot(0)
	r := summaryOf(t, s, run)
	d, _ := v.RunDetail(run)
	if r.Terminal || r.Stage != "echoed" || d.Result != nil || len(d.Echoes) != 1 {
		t.Errorf("facts under a key retired before their append time kept: %+v result %+v echoes %+v", r, d.Result, d.Echoes)
	}
	if s.Counts["retracted"] != 2 {
		t.Errorf("counts %v; want the status and the result retracted", s.Counts)
	}
	// Records under the new key are accepted from the next tick.
	f.status(at.Add(3*time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "terminal", state: wire.TaskStateFailed})
	f.tick(v)
	if r := summaryOf(t, v.Snapshot(0), run); !r.Terminal || r.Class != ClassFailed {
		t.Errorf("status under the new key: %+v", r)
	}
}
