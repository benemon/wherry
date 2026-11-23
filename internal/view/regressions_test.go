package view

import (
	json "encoding/json/v2"
	"slices"
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

// A span name is rebuilt from validated components: a profile the configuration names, an operator command of the
// fixed set; any other text becomes the placeholder.
func TestReviewSpanNameAllowlist(t *testing.T) {
	a := allowlist{topics: map[string]bool{"known-topic": true}, profiles: map[string]bool{"profile-a": true}}
	for _, name := range []string{"process known-topic s3cr3t-prompt", "operator reveal secret prompt", "operator pool apply ", "operator pool-apply", "process other-topic profile-a", "send other-topic", "revocation step 12", "s3cr3t"} {
		if got := a.name(name); got != nameOther {
			t.Errorf("untrusted span name %q passed as %q", name, got)
		}
	}
	for _, name := range []string{"process known-topic profile-a", "process known-topic", "send known-topic", "operator pool apply", "operator check", "revocation step 3", "submit"} {
		if got := a.name(name); got != name {
			t.Errorf("valid span name %q became %q", name, got)
		}
	}
}

// One run's retained events are bounded while folding whatever the runs and digests caps allow: the oldest are
// dropped, the run marked and the cap reported.
func TestReviewPerRunBound(t *testing.T) {
	f := newFixture(t)
	at := f.now.Add(-time.Minute)
	run := f.echo(at, receiverA, tenantA, hostA, poolA, "many-events")
	f.status(at.Add(time.Millisecond), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "claimed", state: wire.TaskStateWorking})
	for i := 1; i < 100; i++ {
		f.status(at.Add(time.Duration(i)*time.Millisecond), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "started", state: wire.TaskStateWorking})
	}
	caps := testCaps()
	caps.Runs, caps.Digests, caps.RunEvents = 1, 4, 4
	v := f.view(time.Hour, caps)
	v.mu.RLock()
	r := v.m.runs[run]
	seen, events, kept := len(r.seen), len(r.Attempts[1].Events), r.events
	v.mu.RUnlock()
	if seen > caps.RunEvents || events > caps.RunEvents || kept != events {
		t.Fatalf("one run retains %d event IDs and %d events (measured %d) despite runs=1/digests=4/run_events=4", seen, events, kept)
	}
	s := v.Snapshot(0)
	if r := summaryOf(t, s, run); !slices.Equal(r.Truncated, []string{capRunEvents}) || r.Attempt != 1 || r.State != string(wire.TaskStateWorking) {
		t.Errorf("truncated run: %+v", r)
	}
	// Under a digest cap of 4 the feed copies are not de-duplicated and re-enter as the oldest, to be dropped again.
	if s.Counts[capRunEvents] < 96 {
		t.Errorf("counts %v; want run_events reported at least 96 times", s.Counts)
	}
}

// The model's events and bytes caps evict the oldest runs, the per-run attempts and opens caps drop the lowest and
// oldest, the progress cap drops the oldest record, and a detail over the response bound is refused before encoding.
func TestRetainedStateBounds(t *testing.T) {
	f := newFixture(t)
	t0 := f.now.Add(-30 * time.Minute)
	// Three runs of five statuses each on the pool's events alone, so an evicted run is made again by no later record.
	var runs []string
	for i := range 3 {
		run := runID(tenantA, hostA, "run-"+string(rune('a'+i)))
		for j := range 5 {
			f.poolOnly(t0.Add(time.Duration(i)*time.Minute+time.Duration(j+1)*time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "started", state: wire.TaskStateWorking})
		}
		runs = append(runs, run)
	}
	// Six attempts of one run; four open requests; progress for three runs.
	attempts := f.echo(t0.Add(10*time.Minute), receiverA, tenantA, hostA, poolA, "attempts")
	for attempt := int64(1); attempt <= 6; attempt++ {
		f.status(t0.Add(10*time.Minute+time.Duration(attempt)*time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: attempts, attempt: attempt, kind: "claimed", state: wire.TaskStateWorking})
	}
	for i := range 4 {
		f.openRequest(t0.Add(11*time.Minute+time.Duration(i)*time.Second), receiverA, poolA, attempts, 6)
	}
	for i, run := range runs {
		f.progress(t0.Add(12*time.Minute+time.Duration(i)*time.Second), receiverA, run, tenantA, 1, 1)
	}
	caps := testCaps()
	caps.Events, caps.RunAttempts, caps.RunOpens, caps.Progress = 12, 3, 2, 2
	v := f.view(time.Hour, caps)
	s := v.Snapshot(0)
	if s.RunsTotal != 2 || hasRun(s, runs[0]) || hasRun(s, runs[1]) || !hasRun(s, runs[2]) || !hasRun(s, attempts) {
		t.Errorf("runs kept under the events cap: %d %v", s.RunsTotal, s.Runs)
	}
	d, _ := v.RunDetail(attempts)
	if r := summaryOf(t, s, attempts); len(d.Attempts) != 3 || d.Attempts[0].Number != 4 || r.Attempt != 6 || len(d.Opens) != 2 || !slices.Equal(r.Truncated, []string{capRunAttempts, capRunOpens}) {
		t.Errorf("attempts and opens: %+v %+v %+v", r, d.Attempts, d.Opens)
	}
	v.mu.RLock()
	progressed, held := v.m.progressed, len(v.m.progress[receiverA])
	_, oldest := v.m.progress[receiverA][runs[0]]
	v.mu.RUnlock()
	if progressed != 2 || held != 2 || oldest {
		t.Errorf("progress kept: %d %d, oldest kept %v", progressed, held, oldest)
	}
	for _, cap := range []string{capEvents, capRunAttempts, capRunOpens, capProgress} {
		if s.Counts[cap] == 0 {
			t.Errorf("%s not reported: %v", cap, s.Counts)
		}
	}
	// A run whose events alone cannot encode under the bound is refused before encoding; a smaller bound still fits a
	// run of one event.
	if least := d.atLeast(); least < 3*eventJSONAtLeast || least > 2000 {
		t.Errorf("detail lower bound %d", least)
	}
	encoded, fits, err := marshalBounded(d, 100)
	if fits || err != nil || encoded != nil {
		t.Errorf("an encoding over its bound was built: %d bytes, fits %v, %v", len(encoded), fits, err)
	}
	if encoded, fits, err := marshalBounded(d, 1<<20); !fits || err != nil || !strings.Contains(string(encoded), `"truncated":["run_attempts","run_opens"]`) {
		t.Errorf("an encoding under its bound: fits %v, %v, %.200s", fits, err, encoded)
	}
}
