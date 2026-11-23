package view

import (
	json "encoding/json/v2"
	"strings"
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

// A run admitted before its tenant's entitlement ended takes only records joined to that echo: the same run, tenant,
// pool and task hash under the run's Kafka key. A different hash is refused, never adopted.
func TestReviewWrongHashAfterEnd(t *testing.T) {
	f := newFixture(t)
	at := f.now.Add(-time.Hour)
	run := f.echo(at, receiverA, tenantA, hostA, poolA, "wrong-after-end")
	f.operator(at.Add(time.Minute), operator.Record{Kind: "end", Tenant: tenantA, Pool: poolA})
	f.status(at.Add(2*time.Minute), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "terminal", state: wire.TaskStateCompleted, hash: hashOf("different")})
	f.resultWith(at.Add(2*time.Minute), receiverA, tenantA, poolA, run, 1, wire.TaskStateCompleted, wire.ResultMetadata{}, "reply", hashOf("different"))
	f.startFailureWith(at.Add(2*time.Minute), receiverA, tenantA, poolA, run, 2, "dispatch", "profile-a", agentA, hashOf("different"))
	// The right hash under another run's Kafka key.
	other := runID(tenantA, hostA, "other")
	f.produce(operator.PoolEventsTopic(poolA), other, f.statusRecord(at.Add(3*time.Minute), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "working", state: wire.TaskStateWorking}), at.Add(3*time.Minute))
	v := f.view(2*time.Hour, testCaps())
	f.tick(v)
	s := v.Snapshot(0)
	r := summaryOf(t, s, run)
	d, _ := v.RunDetail(run)
	if r.Terminal || r.Stage != "echoed" || d.Hash != hashOf(run) || d.Result != nil || len(d.Attempts) != 0 {
		t.Fatalf("post-entitlement records with a different task hash accepted: %+v hash %s result %+v attempts %+v", r, d.Hash, d.Result, d.Attempts)
	}
	if hasRun(s, other) {
		t.Error("a status under another run's key made a run")
	}
	if s.Counts["refused"] != 4 || s.Counts["pending"] != 0 {
		t.Errorf("counts %v; want the four refused and nothing pending", s.Counts)
	}
	// The joined record is taken.
	f.status(at.Add(4*time.Minute), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "terminal", state: wire.TaskStateCompleted})
	f.tick(v)
	if r := summaryOf(t, v.Snapshot(0), run); !r.Terminal {
		t.Errorf("the joined terminal was not taken: %+v", r)
	}
}

// Nothing a result or start failure says of labels, a profile or an agent is returned: a profile or agent is shown
// only as the configuration names it, labels never.
func TestReviewLabelsProjection(t *testing.T) {
	f := newFixture(t)
	at := f.now.Add(-time.Minute)
	run := f.echo(at, receiverA, tenantA, hostA, poolA, "labels")
	f.result(at.Add(time.Second), receiverA, tenantA, poolA, run, 1, wire.TaskStateCompleted, wire.ResultMetadata{Profile: "s3cr3t-profile", Labels: map[string]string{"secret": "s3cr3t-label-value"}}, "reply")
	f.startFailureWith(at.Add(time.Second), receiverA, tenantA, poolA, run, 2, "dispatch", "s3cr3t-profile", "s3cr3t-agent", hashOf(run))
	known := f.echo(at, receiverA, tenantA, hostA, poolA, "known")
	f.result(at.Add(time.Second), receiverA, tenantA, poolA, known, 1, wire.TaskStateCompleted, wire.ResultMetadata{Profile: "profile-a", Labels: map[string]string{"zone": "dc1"}}, "reply")
	v := f.view(time.Hour, testCaps())
	d, _ := v.RunDetail(run)
	encoded, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	for _, sentinel := range []string{"s3cr3t", "secret", "label"} {
		if strings.Contains(string(encoded), sentinel) {
			t.Errorf("result metadata projected into the API detail: %s", encoded)
		}
	}
	if d.Result == nil || d.Result.Profile != "" || len(d.Attempts) != 1 || d.Attempts[0].StartFailure == nil || d.Attempts[0].StartFailure.Profile != "" || d.Attempts[0].StartFailure.Agent != "" {
		t.Errorf("an unconfigured profile or agent shown: %+v %+v", d.Result, d.Attempts)
	}
	if d, _ := v.RunDetail(known); d.Result == nil || d.Result.Profile != "profile-a" {
		t.Errorf("the configured profile is not shown: %+v", d.Result)
	}
}
