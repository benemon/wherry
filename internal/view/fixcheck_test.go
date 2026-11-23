package view

// The reviewer's probes of the first fix round, kept as they were written: each failed on 71de86f and passes at the
// head. TestFixReviewTaskSpan builds its allowlist from a catalog, as the allowlist now reads one; the last test's
// assertion names the per-run events cap, since the digests cap never bounded event IDs.

import (
	json "encoding/json/v2"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/wire"
)

func TestFixReviewDescendingAttempts(t *testing.T) {
	f := newFixture(t)
	at := f.now.Add(-time.Minute)
	run := runID(tenantA, hostA, "descending")
	for i := int64(100); i > 0; i-- {
		f.startFailure(at.Add(time.Duration(100-i)*time.Millisecond), receiverA, tenantA, poolA, run, i, "dispatch")
	}
	caps := testCaps()
	caps.RunAttempts = 3
	v := f.view(time.Hour, caps)
	d, _ := v.RunDetail(run)
	if len(d.Attempts) > 3 {
		t.Fatalf("retained %d attempts under cap 3", len(d.Attempts))
	}
}

func TestFixReviewNonEventBytes(t *testing.T) {
	f := newFixture(t)
	at := f.now.Add(-time.Minute)
	var run string
	for i := 0; i < 100; i++ {
		run = f.echoWith(at.Add(time.Duration(i)*time.Millisecond), receiverA, tenantA, hostA, poolA, "echo-bytes", hashOf(fmt.Sprint(i)))
	}
	caps := testCaps()
	caps.RunBytes = 1024
	caps.Bytes = 2048
	v := f.view(time.Hour, caps)
	v.mu.RLock()
	defer v.mu.RUnlock()
	if r := v.m.runs[run]; r != nil && (r.me.bytes > caps.RunBytes || v.m.total.bytes > caps.Bytes) {
		t.Fatalf("retained %d echoes, run bytes %d model bytes %d, caps 1024/2048, counts %v", len(r.Echoes), r.me.bytes, v.m.total.bytes, v.m.counts)
	}
}

func TestFixReviewAdmissionRetraction(t *testing.T) {
	f := newFixture(t)
	at := f.now.Add(-time.Minute)
	run := f.echo(at, receiverA, tenantA, hostA, poolA, "join-retraction")
	f.operator(at.Add(time.Second), operator.Record{Kind: "end", Tenant: tenantA, Pool: poolA})
	f.status(at.Add(2*time.Second), st{receiver: receiverB, tenant: tenantA, pool: poolA, run: run, kind: "terminal", state: wire.TaskStateCompleted})
	v := f.view(time.Hour, testCaps())
	f.tick(v)
	if r := summaryOf(t, v.Snapshot(0), run); !r.Terminal {
		t.Fatal("setup: terminal not admitted")
	}
	f.rotate(receiverA, operator.ReceiverTopic(receiverA), at.Add(-time.Second))
	f.tick(v)
	d, ok := v.RunDetail(run)
	if ok && len(d.Echoes) == 0 && len(d.Attempts) > 0 {
		t.Fatalf("terminal survives loss of its historic admission: %+v", d)
	}
}

func TestFixReviewMethodLog(t *testing.T) {
	s := newServed(t, ServeOptions{})
	s.request(t, "s3cr3t-method", "/", nil, nil, nil)
	if strings.Contains(s.logs.String(), "s3cr3t-method") {
		t.Fatalf("request free text logged: %s", s.logs.String())
	}
}

func TestFixReviewEchoProjection(t *testing.T) {
	f := newFixture(t)
	at := f.now.Add(-time.Minute)
	name := "echo-secret"
	run := runID(tenantA, hostA, name)
	echo := &wire.Echo{Receiver: receiverA, Tenant: tenantA, Host: hostA, Pool: poolA, Run: run, RunNonce: nonce(name), Placement: wire.Placement{Agent: agentA, Model: "s3cr3t prompt with spaces"}, TaskHash: hashOf(run), MessageID: "message-secret"}
	f.produce(operator.TenantTopic(tenantA), run, f.sign(echo, f.keys[receiverA]), at)
	v := f.view(time.Hour, testCaps())
	d, _ := v.RunDetail(run)
	encoded, _ := json.Marshal(d)
	if strings.Contains(string(encoded), "s3cr3t") {
		t.Fatalf("echo free text projected: %s", encoded)
	}
}

func TestFixReviewTaskSpan(t *testing.T) {
	a := allowlist{names: newCatalog(operator.History{State: operator.State{Pools: map[string]operator.Terms{poolA: {}}}})}
	name := "process " + operator.SignatureTopic(poolA, "s3cr3t-prompt", nil)
	if got := a.name(name); strings.Contains(got, "s3cr3t") {
		t.Fatalf("unconfigured task agent projected: %s", got)
	}
}

// Reconstruct the original bound probe's reported cap settings: the event IDs kept are one per event kept, and the
// events are bounded by the per-run events cap, whatever the runs and digests caps allow.
func TestFixReviewOriginalBoundSettings(t *testing.T) {
	f := newFixture(t)
	at := f.now.Add(-time.Minute)
	run := f.echo(at, receiverA, tenantA, hostA, poolA, "original-bound")
	for i := 0; i < 100; i++ {
		f.poolOnly(at.Add(time.Duration(i+1)*time.Millisecond), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "started", state: wire.TaskStateWorking})
	}
	caps := testCaps()
	caps.Runs = 1
	caps.Digests = 4
	caps.RunEvents = 10
	v := f.view(time.Hour, caps)
	v.mu.RLock()
	defer v.mu.RUnlock()
	r := v.m.runs[run]
	if len(r.seen) != r.me.events || r.me.events > caps.RunEvents {
		t.Fatalf("original settings retain %d event IDs and %d events under RunEvents=%d", len(r.seen), r.me.events, caps.RunEvents)
	}
}
