package view

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	json "encoding/json/v2"
	"errors"
	"maps"
	"os"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Test 6: hostile records are absent and history is present, each checked by its signature against the key history
// and against the topic it was read from, never against the membership or entitlements of its time.
func TestRebuildAuthorisation(t *testing.T) {
	f := newFixture(t)
	t0 := f.now.Add(-2 * time.Hour)
	hostAOld := f.rotate(signed.HostOwner(tenantA, hostA), operator.HostTopic(tenantA, hostA), t0.Add(20*time.Minute))
	receiverBOld := f.rotate(receiverB, operator.ReceiverTopic(receiverB), t0.Add(10*time.Minute))
	// tenant-b, entitled to pool-a with hosts b and c; its entitlement ends at +40 min and host-b at +50 min.
	if err := f.service.TenantAdd(f.ctx, tenantB); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantPoolAdd(f.ctx, tenantB, poolA); err != nil {
		t.Fatal(err)
	}
	f.addHost(tenantB, hostB)
	f.addHost(tenantB, "host-c")
	f.operator(t0.Add(30*time.Minute), operator.Record{Kind: "end", Receiver: receiverB})
	f.operator(t0.Add(40*time.Minute), operator.Record{Kind: "end", Tenant: tenantB, Pool: poolA})
	f.operator(t0.Add(50*time.Minute), operator.Record{Kind: "end", Tenant: tenantB, Host: hostB})

	// Hostile: a host-signed status, a status under a retired key after its rotation, a status naming another pool than
	// its topic, a forged signature and an unknown key.
	h1 := runID(tenantA, hostA, "h1")
	f.status(t0.Add(time.Minute), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: h1, kind: "terminal", state: wire.TaskStateCompleted, key: f.keys[signed.HostOwner(tenantA, hostA)]})
	h2 := runID(tenantA, hostA, "h2")
	f.status(t0.Add(10*time.Minute+5*time.Millisecond), st{receiver: receiverB, tenant: tenantA, pool: poolA, run: h2, kind: "terminal", state: wire.TaskStateCompleted, key: receiverBOld})
	h3 := runID(tenantA, hostA, "h3")
	f.produce(operator.PoolEventsTopic(poolA), h3, f.statusRecord(t0.Add(time.Minute), st{receiver: receiverA, tenant: tenantA, pool: poolB, run: h3, kind: "terminal", state: wire.TaskStateCompleted}), t0.Add(time.Minute))
	h4 := runID(tenantA, hostA, "h4")
	f.echoWith(t0.Add(time.Minute), receiverA, tenantA, hostA, poolA, "h4", hashOf("another task"))
	f.status(t0.Add(2*time.Minute), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: h4, kind: "claimed", state: wire.TaskStateWorking})
	f.status(t0.Add(3*time.Minute), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: h4, kind: "terminal", state: wire.TaskStateCompleted})
	h6 := runID(tenantA, hostA, "h6")
	f.produce(operator.PoolEventsTopic(poolA), h6, forge(f.statusRecord(t0.Add(time.Minute), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: h6, kind: "terminal", state: wire.TaskStateCompleted})), t0.Add(time.Minute))
	h7 := runID(tenantA, hostA, "h7")
	_, unknown, _ := ed25519.GenerateKey(rand.Reader)
	f.status(t0.Add(time.Minute), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: h7, kind: "terminal", state: wire.TaskStateCompleted, key: unknown})

	// History: a run completed by a receiver since removed under its earlier key, one it completed after its end, runs of
	// an entitlement since ended, cancels by a since-ended host, before a host key rotation and after an entitlement's
	// end.
	l1 := runID(tenantA, hostA, "l1")
	f.echo(t0.Add(4*time.Minute), receiverB, tenantA, hostA, poolA, "l1")
	f.status(t0.Add(5*time.Minute), st{receiver: receiverB, tenant: tenantA, pool: poolA, run: l1, kind: "claimed", state: wire.TaskStateWorking, key: receiverBOld})
	f.status(t0.Add(6*time.Minute), st{receiver: receiverB, tenant: tenantA, pool: poolA, run: l1, kind: "terminal", state: wire.TaskStateCompleted, key: receiverBOld})
	l6 := runID(tenantA, hostA, "l6")
	f.status(t0.Add(35*time.Minute), st{receiver: receiverB, tenant: tenantA, pool: poolA, run: l6, kind: "terminal", state: wire.TaskStateCompleted})
	l2 := f.echo(t0.Add(35*time.Minute), receiverA, tenantB, hostB, poolA, "l2")
	f.status(t0.Add(36*time.Minute), st{receiver: receiverA, tenant: tenantB, pool: poolA, run: l2, kind: "claimed", state: wire.TaskStateWorking})
	f.status(t0.Add(45*time.Minute), st{receiver: receiverA, tenant: tenantB, pool: poolA, run: l2, kind: "terminal", state: wire.TaskStateCompleted})
	l3 := f.echo(t0.Add(35*time.Minute), receiverA, tenantB, hostB, poolA, "l3")
	f.status(t0.Add(36*time.Minute), st{receiver: receiverA, tenant: tenantB, pool: poolA, run: l3, kind: "claimed", state: wire.TaskStateWorking})
	f.cancel(t0.Add(45*time.Minute), tenantB, hostB, poolA, l3, nil)
	l4 := f.echo(t0.Add(14*time.Minute), receiverA, tenantA, hostA, poolA, "l4")
	f.status(t0.Add(14*time.Minute), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: l4, kind: "claimed", state: wire.TaskStateWorking})
	f.cancel(t0.Add(15*time.Minute), tenantA, hostA, poolA, l4, hostAOld)
	l5 := f.echo(t0.Add(35*time.Minute), receiverA, tenantB, "host-c", poolA, "l5")
	f.status(t0.Add(36*time.Minute), st{receiver: receiverA, tenant: tenantB, pool: poolA, run: l5, kind: "claimed", state: wire.TaskStateWorking})
	f.cancel(t0.Add(55*time.Minute), tenantB, "host-c", poolA, l5, nil)

	v := f.view(3*time.Hour, testCaps())
	// The status under an unknown key waits for a key read; the next tick reads the keys again and refuses it.
	f.tick(v)
	s := v.Snapshot(0)
	for name, id := range map[string]string{"host-signed status": h1, "status under a retired key after its rotation": h2, "wrong-pool status": h3, "forged signature": h6, "status under an unknown key": h7} {
		if hasRun(s, id) {
			t.Errorf("%s is present", name)
		}
	}
	if r := summaryOf(t, s, h4); r.Host != "" || r.State != string(wire.TaskStateCompleted) {
		t.Errorf("wrong-hash echo joined: host %q state %s", r.Host, r.State)
	}
	if d, _ := v.RunDetail(h4); len(d.Echoes) != 0 {
		t.Errorf("wrong-hash echo listed: %+v", d.Echoes)
	}
	if r := summaryOf(t, s, l1); r.State != string(wire.TaskStateCompleted) || r.Receiver != receiverB || r.Host != hostA {
		t.Errorf("run completed by a since-removed receiver under its earlier key: %+v", r)
	}
	if d, _ := v.RunDetail(l1); d.Echoes[0].Signer != operator.ReceiverPrincipal(receiverB) || d.Attempts[0].Events[1].Signer != operator.ReceiverPrincipal(receiverB) {
		t.Errorf("signers of a since-removed receiver's run: %+v %+v", d.Echoes, d.Attempts)
	}
	if r := summaryOf(t, s, l6); !r.Terminal || r.Receiver != receiverB {
		t.Errorf("status an ended receiver signed after its end, the accepted residual: %+v", r)
	}
	if d, _ := v.RunDetail(l6); d.Attempts[0].Events[0].Signer != operator.ReceiverPrincipal(receiverB) {
		t.Errorf("the residual is not labelled with its signer: %+v", d.Attempts)
	}
	if r := summaryOf(t, s, l2); r.State != string(wire.TaskStateCompleted) || !r.Terminal {
		t.Errorf("held run completing after its entitlement ended: %+v", r)
	}
	for name, id := range map[string]string{"cancel by a since-ended host": l3, "cancel signed before a host key rotation": l4, "cancel from a live host of an ended entitlement": l5} {
		if r := summaryOf(t, s, id); !r.Cancelled {
			t.Errorf("%s is absent: %+v", name, r)
		}
	}
	if d, _ := v.RunDetail(l4); d.Cancels[0].Signer != operator.TenantPrincipal(tenantA, hostA) {
		t.Errorf("cancel signer: %+v", d.Cancels)
	}
	if s.Counts["refused"] != 5 || s.Counts["pending"] != 0 {
		t.Errorf("counts %v; want the hostile five refused and nothing pending", s.Counts)
	}
}

// forge returns the JWS value with its last signature byte changed, so it no longer verifies under the key its kid names.
func forge(value []byte) []byte {
	forged := slices.Clone(value)
	last := &forged[len(forged)-1]
	if *last == 'A' {
		*last = 'B'
	} else {
		*last = 'A'
	}
	return forged
}

// Test 7: exact counts over a recorded workload, the live path across a committed and an aborted transaction, a
// reconnect, members changing and a terms change.
func TestRebuildAccuracy(t *testing.T) {
	f := newFixture(t)
	t0 := f.now.Add(-time.Hour)
	f.advertise(f.now.Add(-10*time.Second), receiverA, poolA, 2, 1)
	f.advertise(f.now.Add(-10*time.Second), receiverB, poolA, 2, 2)
	// A terms change at +30 min: runs admitted before keep bucket-a.
	changed := testTerms(bucketB)
	f.operator(t0.Add(30*time.Minute), operator.Record{Kind: "pool", Pool: poolA, Terms: &changed})
	complete := func(at time.Time, receiver, name string, state wire.TaskState) string {
		run := f.echo(at, receiver, tenantA, hostA, poolA, name)
		f.status(at.Add(time.Second), st{receiver: receiver, tenant: tenantA, pool: poolA, run: run, kind: "claimed", state: wire.TaskStateWorking})
		f.status(at.Add(2*time.Second), st{receiver: receiver, tenant: tenantA, pool: poolA, run: run, kind: "started", state: wire.TaskStateWorking})
		f.status(at.Add(3*time.Second), st{receiver: receiver, tenant: tenantA, pool: poolA, run: run, kind: "working", state: wire.TaskStateWorking, text: "s3cr3t-working-line"})
		f.status(at.Add(10*time.Second), st{receiver: receiver, tenant: tenantA, pool: poolA, run: run, kind: "terminal", state: state})
		f.result(at.Add(10*time.Second), receiver, tenantA, poolA, run, 1, state, wire.ResultMetadata{Profile: "profile-a", S3Outputs: []wire.ReturnedOutput{{Name: "out.bin", SHA256: hashOf("out"), Size: 1234, Key: "k"}}}, "s3cr3t-reply-text")
		return run
	}
	s1 := complete(t0.Add(time.Minute), receiverA, "s1", wire.TaskStateCompleted)
	s2 := complete(t0.Add(40*time.Minute), receiverA, "s2", wire.TaskStateCompleted)
	fail1 := complete(t0.Add(5*time.Minute), receiverB, "fail1", wire.TaskStateFailed)
	// A redelivery skipping attempt 2.
	redeliver := f.echo(t0.Add(6*time.Minute), receiverA, tenantA, hostA, poolA, "redeliver")
	f.status(t0.Add(6*time.Minute+time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: redeliver, kind: "claimed", state: wire.TaskStateWorking})
	f.poolOnly(t0.Add(6*time.Minute+2*time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: redeliver, kind: "released", state: wire.TaskStateWorking})
	f.status(t0.Add(7*time.Minute), st{receiver: receiverB, tenant: tenantA, pool: poolA, run: redeliver, attempt: 3, kind: "claimed", state: wire.TaskStateWorking})
	f.status(t0.Add(8*time.Minute), st{receiver: receiverB, tenant: tenantA, pool: poolA, run: redeliver, attempt: 3, kind: "terminal", state: wire.TaskStateCompleted})
	// A late lower-attempt record after attempt 2 was claimed.
	late := f.echo(t0.Add(9*time.Minute), receiverA, tenantA, hostA, poolA, "late")
	f.status(t0.Add(9*time.Minute+time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: late, kind: "claimed", state: wire.TaskStateWorking})
	f.status(t0.Add(10*time.Minute), st{receiver: receiverB, tenant: tenantA, pool: poolA, run: late, attempt: 2, kind: "claimed", state: wire.TaskStateWorking})
	f.status(t0.Add(11*time.Minute), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: late, kind: "working", state: wire.TaskStateWorking})
	// A rejection.
	reject := f.echo(t0.Add(12*time.Minute), receiverA, tenantA, hostA, poolA, "reject")
	f.status(t0.Add(12*time.Minute+time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: reject, kind: "rejected", state: wire.TaskStateRejected, text: "s3cr3t-rejection-reason"})
	// An in-place retry: two placings before the start.
	retry := f.echo(t0.Add(13*time.Minute), receiverA, tenantA, hostA, poolA, "retry")
	for i, kind := range []string{"claimed", "placing", "placing", "started", "terminal"} {
		state := wire.TaskStateWorking
		if kind == "terminal" {
			state = wire.TaskStateCompleted
		}
		f.status(t0.Add(13*time.Minute+time.Duration(i+1)*time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: retry, kind: kind, state: state})
	}
	// A silent run started before the window, known from progress alone, and one more in flight not identified.
	old := runID(tenantA, hostA, "old")
	f.progress(t0.Add(-3*time.Hour), receiverB, old, tenantA, 1, 5)
	// An adopted run: receiver-a's progress is stale once receiver-b holds attempt 2.
	adopt := f.echo(t0.Add(14*time.Minute), receiverA, tenantA, hostA, poolA, "adopt")
	f.status(t0.Add(14*time.Minute+time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: adopt, kind: "claimed", state: wire.TaskStateWorking})
	f.progress(t0.Add(14*time.Minute+2*time.Second), receiverA, adopt, tenantA, 1, 3)
	f.poolOnly(t0.Add(15*time.Minute), st{receiver: receiverB, tenant: tenantA, pool: poolA, run: adopt, attempt: 2, kind: "adopting", state: wire.TaskStateWorking})
	f.status(t0.Add(15*time.Minute+time.Second), st{receiver: receiverB, tenant: tenantA, pool: poolA, run: adopt, attempt: 2, kind: "claimed", state: wire.TaskStateWorking})
	f.progress(t0.Add(15*time.Minute+2*time.Second), receiverB, adopt, tenantA, 2, 3)
	// A progress record newer than s1's terminal: discarded.
	f.progress(t0.Add(50*time.Minute), receiverA, s1, tenantA, 1, 9)

	// An open transaction holds statuses of run txn below the last stable offset through the rebuild.
	producer, err := kgo.NewClient(append(f.client.Opts(), kgo.TransactionalID("p.pool-a.run-txn"))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)
	txn := f.echo(t0.Add(50*time.Minute), receiverA, tenantA, hostA, poolA, "txn")
	if err := producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	value := f.statusRecord(t0.Add(51*time.Minute), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: txn, kind: "claimed", state: wire.TaskStateWorking})
	if err := producer.ProduceSync(f.ctx, &kgo.Record{Topic: operator.PoolEventsTopic(poolA), Key: []byte(txn), Value: value, Timestamp: t0.Add(51 * time.Minute)}, &kgo.Record{Topic: operator.TenantTopic(tenantA), Key: []byte(txn), Value: value, Timestamp: t0.Add(51 * time.Minute)}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	// A record after the open transaction on the events topic, so the high watermark is past the stable offset.
	f.status(t0.Add(52*time.Minute), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: s2, kind: "working", state: wire.TaskStateWorking})

	v := f.view(2*time.Hour, testCaps())
	s := v.Snapshot(0)
	if r := summaryOf(t, s, txn); r.Stage != "echoed" {
		t.Fatalf("a record of an open transaction was read: %+v", r)
	}
	if r := summaryOf(t, s, s1); r.State != string(wire.TaskStateCompleted) || !r.Terminal || r.Outputs != 1 || r.Storage == nil || r.Storage.Bucket != bucketA || r.Host != hostA || r.Receiver != receiverA {
		t.Errorf("s1: %+v", r)
	}
	if r := summaryOf(t, s, s2); r.Storage == nil || r.Storage.Bucket != bucketB {
		t.Errorf("s2 admitted after the terms change: %+v", r.Storage)
	}
	if r := summaryOf(t, s, fail1); r.State != string(wire.TaskStateFailed) || r.Class != ClassFailed {
		t.Errorf("fail1: %+v", r)
	}
	if r := summaryOf(t, s, redeliver); r.Attempt != 3 || r.State != string(wire.TaskStateCompleted) || r.Receiver != receiverB {
		t.Errorf("redelivery: %+v", r)
	}
	if r := summaryOf(t, s, late); r.Attempt != 2 || r.Stage != "claimed" || r.Receiver != receiverB {
		t.Errorf("late lower-attempt record overrode: %+v", r)
	}
	if r := summaryOf(t, s, reject); r.Class != ClassRejected || !r.Terminal || r.State != string(wire.TaskStateRejected) {
		t.Errorf("rejection: %+v", r)
	}
	if r := summaryOf(t, s, retry); r.Attempt != 1 || r.State != string(wire.TaskStateCompleted) {
		t.Errorf("in-place retry: %+v", r)
	}
	if d, _ := v.RunDetail(retry); len(d.Attempts) != 1 || len(d.Attempts[0].Events) != 5 {
		t.Errorf("in-place retry attempts: %+v", d.Attempts)
	}
	if r := summaryOf(t, s, adopt); r.Attempt != 2 || r.Receiver != receiverB || r.Stage != "claimed" {
		t.Errorf("adopted: %+v", r)
	}
	a, b := receiverOf(t, s, receiverA), receiverOf(t, s, receiverB)
	if len(b.Held) != 2 || b.Held[0].Run != adopt || b.Held[1].Run != old || b.Held[1].Stage != StageInFlight || b.Unidentified != 0 {
		t.Errorf("receiver-b holds %+v, unidentified %d; want adopt then the pre-window run", b.Held, b.Unidentified)
	}
	if len(a.Stale) != 1 || a.Stale[0].Run != adopt || !a.Stale[0].Stale || len(a.Held) != 0 || a.Unidentified != 1 {
		t.Errorf("receiver-a holds %+v stale %+v unidentified %d; want adopt stale and one not identified", a.Held, a.Stale, a.Unidentified)
	}
	for _, h := range append(a.Held, a.Stale...) {
		if h.Run == s1 {
			t.Error("a progress record newer than the run's terminal counted")
		}
	}
	if !hasRun(s, old) == false && summaryOf(t, s, old).Stage != StageInFlight {
		t.Error("the pre-window run is listed with records")
	}
	// Exact counts: 9 runs in the window, the echoed txn among them, 6 terminal.
	p := poolOf(t, s, poolA)
	if s.RunsTotal != 9 || p.Runs != 9 || p.ByState[string(wire.TaskStateCompleted)] != 4 || p.ByState[string(wire.TaskStateFailed)] != 1 || p.ByState[string(wire.TaskStateRejected)] != 1 || p.InProgress != 3 {
		t.Errorf("pool counts: total %d %+v", s.RunsTotal, p)
	}
	if a.Runs != 4 || b.Runs != 4 {
		t.Errorf("runs per receiver: a %d b %d; want 4 and 4 by current holder", a.Runs, b.Runs)
	}
	if h := s.Hosts; len(h) != 1 || h[0].Runs != 9 {
		t.Errorf("hosts: %+v", h)
	}

	// The transaction commits between the rebuild's end and the first live read.
	if err := producer.EndTransaction(f.ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}
	f.tick(v)
	s = v.Snapshot(0)
	if r := summaryOf(t, s, txn); r.Stage != "claimed" {
		t.Errorf("committed transaction: %+v", r)
	}
	if d, _ := v.RunDetail(txn); len(d.Attempts) != 1 || len(d.Attempts[0].Events) != 1 {
		t.Errorf("committed transaction's events: %+v", d.Attempts)
	}
	// An aborted one leaves nothing.
	abort := f.echo(t0.Add(53*time.Minute), receiverA, tenantA, hostA, poolA, "abort")
	if err := producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	value = f.statusRecord(t0.Add(54*time.Minute), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: abort, kind: "claimed", state: wire.TaskStateWorking})
	if err := producer.ProduceSync(f.ctx, &kgo.Record{Topic: operator.PoolEventsTopic(poolA), Key: []byte(abort), Value: value}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := producer.EndTransaction(f.ctx, kgo.TryAbort); err != nil {
		t.Fatal(err)
	}
	f.tick(v)
	if r := summaryOf(t, v.Snapshot(0), abort); r.Stage != "echoed" {
		t.Errorf("aborted transaction's status counted: %+v", r)
	}
	// A dropped connection: the broker closes the next fetch's connection; the read resumes from the same cursor.
	f.cluster.ControlKey(int16(kmsg.Fetch), func(kmsg.Request) (kmsg.Response, error, bool) { return nil, errors.New("dropped"), true })
	recon := f.echo(t0.Add(55*time.Minute), receiverA, tenantA, hostA, poolA, "recon")
	f.status(t0.Add(55*time.Minute+time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: recon, kind: "claimed", state: wire.TaskStateWorking})
	for range 3 {
		ctx, cancel := context.WithTimeout(f.ctx, 30*time.Second)
		err = v.tick(ctx, false)
		cancel()
		if err == nil {
			break
		}
	}
	if d, ok := v.RunDetail(recon); !ok || len(d.Attempts) != 1 || len(d.Attempts[0].Events) != 1 {
		t.Errorf("after the reconnect: %+v %v", d.Attempts, ok)
	}
	// A receiver added while the view runs verifies at once; one removed shows ended.
	f.addReceiver(receiverC, poolA)
	f.advertise(f.now, receiverC, poolA, 1, 0)
	c1 := f.echo(t0.Add(56*time.Minute), receiverC, tenantA, hostA, poolA, "c1")
	f.status(t0.Add(56*time.Minute+time.Second), st{receiver: receiverC, tenant: tenantA, pool: poolA, run: c1, kind: "claimed", state: wire.TaskStateWorking})
	f.operator(f.now, operator.Record{Kind: "end", Receiver: receiverB})
	f.advance(time.Second)
	f.tick(v)
	f.tick(v)
	s = v.Snapshot(0)
	if r := receiverOf(t, s, receiverC); r.Status != "live" || r.Runs != 1 {
		t.Errorf("receiver added live: %+v", r)
	}
	if r := summaryOf(t, s, c1); r.Receiver != receiverC || r.Stage != "claimed" {
		t.Errorf("new receiver's status: %+v", r)
	}
	if r := receiverOf(t, s, receiverB); !r.Ended || r.Status != "ended" {
		t.Errorf("receiver removed: %+v", r)
	}
	// A stage change reaches a subscriber within 5 s on the live path.
	runCtx, stop := context.WithCancel(f.ctx)
	defer stop()
	go v.Run(runCtx)
	versions, unsubscribe := v.Subscribe()
	defer unsubscribe()
	before := v.Version()
	f.status(t0.Add(57*time.Minute), st{receiver: receiverC, tenant: tenantA, pool: poolA, run: c1, kind: "started", state: wire.TaskStateWorking})
	deadline := time.After(5 * time.Second)
	for {
		select {
		case version := <-versions:
			if version > before {
				if r, _ := v.RunDetail(c1); r.Stage == "started" {
					return
				}
			}
		case <-deadline:
			t.Fatalf("the stage change did not reach the stream within 5 s; version %d, before %d", v.Version(), before)
		}
	}
}

// Test 10: the model carries each acceptance question's fields with actual values, every node kind included, idle
// nodes among them.
func TestModelCompleteness(t *testing.T) {
	f := newFixture(t)
	t0 := f.now.Add(-30 * time.Minute)
	if err := f.service.PoolApply(f.ctx, poolB, testTerms(bucketB), false); err != nil {
		t.Fatal(err)
	}
	f.addHost(tenantA, "host-idle")
	f.addReceiver(receiverC, poolA)
	f.advertise(f.now.Add(-5*time.Second), receiverA, poolA, 2, 0)
	f.advertise(f.now.Add(-5*time.Second), receiverB, poolA, 1, 0)
	keyID := f.snapshot(t0, poolA, receiverA)
	run := f.echo(t0.Add(time.Minute), receiverA, tenantA, hostA, poolA, "run")
	claim := f.status(t0.Add(time.Minute+time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "claimed", state: wire.TaskStateWorking})
	f.decision(t0.Add(time.Minute+2*time.Second), receiverA, poolA, run, claim, "", wire.ClaimUnchanged, true)
	request := f.openRequest(t0.Add(time.Minute+3*time.Second), receiverA, poolA, run, 1)
	f.wrap(t0.Add(time.Minute+4*time.Second), receiverA, poolA, run, request, receiverA, 1)
	f.status(t0.Add(time.Minute+5*time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "started", state: wire.TaskStateWorking})
	f.status(t0.Add(2*time.Minute), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "terminal", state: wire.TaskStateCompleted})
	f.result(t0.Add(2*time.Minute), receiverA, tenantA, poolA, run, 1, wire.TaskStateCompleted, wire.ResultMetadata{Profile: "profile-a", Labels: map[string]string{"zone": "dc1"},
		Outputs: []wire.ReturnedOutput{{Name: "notes.md", SHA256: hashOf("notes"), Size: 42}}, S3Outputs: []wire.ReturnedOutput{{Name: "big.tar", SHA256: hashOf("big"), Size: 5000, Key: "k"}},
		NotReturned: []wire.UnreturnedOutput{{Name: "missing.txt", Reason: "missing"}}, Reply: &wire.ReturnedReply{SHA256: hashOf("reply"), Size: 70000}}, "the reply")
	keyLost := f.echo(t0.Add(3*time.Minute), receiverB, tenantA, hostA, poolA, "key-lost")
	f.status(t0.Add(3*time.Minute+time.Second), st{receiver: receiverB, tenant: tenantA, pool: poolA, run: keyLost, kind: "terminal", state: wire.TaskStateFailed, event: wire.EventKeyLost, text: "s3cr3t-key-lost-message"})
	failed := f.echo(t0.Add(4*time.Minute), receiverB, tenantA, hostA, poolA, "failed")
	f.status(t0.Add(4*time.Minute+time.Second), st{receiver: receiverB, tenant: tenantA, pool: poolA, run: failed, kind: "terminal", state: wire.TaskStateFailed, text: "s3cr3t-failure-message"})
	cancelled := f.echo(t0.Add(5*time.Minute), receiverB, tenantA, hostA, poolA, "cancelled")
	f.cancel(t0.Add(5*time.Minute+time.Second), tenantA, hostA, poolA, cancelled, nil)
	f.status(t0.Add(5*time.Minute+2*time.Second), st{receiver: receiverB, tenant: tenantA, pool: poolA, run: cancelled, kind: "terminal", state: wire.TaskStateCanceled})
	unstarted := f.echo(t0.Add(6*time.Minute), receiverB, tenantA, hostA, poolA, "unstarted")
	f.status(t0.Add(6*time.Minute+time.Second), st{receiver: receiverB, tenant: tenantA, pool: poolA, run: unstarted, kind: "claimed", state: wire.TaskStateWorking})
	f.startFailure(t0.Add(6*time.Minute+2*time.Second), receiverB, tenantA, poolA, unstarted, 1, "dispatch")
	// A released run that exhausted the delivery limit of 3.
	exhausted := f.echo(t0.Add(7*time.Minute), receiverB, tenantA, hostA, poolA, "exhausted")
	for attempt := int64(1); attempt <= 3; attempt++ {
		f.status(t0.Add(7*time.Minute+time.Duration(attempt)*2*time.Second), st{receiver: receiverB, tenant: tenantA, pool: poolA, run: exhausted, attempt: attempt, kind: "claimed", state: wire.TaskStateWorking})
		f.poolOnly(t0.Add(7*time.Minute+time.Duration(attempt)*2*time.Second+time.Second), st{receiver: receiverB, tenant: tenantA, pool: poolA, run: exhausted, attempt: attempt, kind: "released", state: wire.TaskStateWorking})
	}
	// A share-group member on pool-a's task topic, so the group describes and the lag lists.
	if err := f.client.ProduceSync(f.ctx, &kgo.Record{Topic: operator.SignatureTopic(poolA, agentA, map[string]string{"zone": "dc1"}), Value: []byte("task")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	member, err := kgo.NewClient(append(f.client.Opts(), kgo.ConsumeTopics(operator.SignatureTopic(poolA, agentA, map[string]string{"zone": "dc1"})), kgo.ShareGroup(operator.PoolGroup(poolA)), kgo.ShareMaxRecords(1))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(member.Close)
	pollCtx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
	member.PollRecords(pollCtx, 1)
	cancel()

	v := f.view(time.Hour, testCaps())
	s := v.Snapshot(0)
	// Question 1: a run's path end to end, with key exchange and return.
	d, ok := v.RunDetail(run)
	if !ok {
		t.Fatal("run absent")
	}
	var steps []string
	for _, step := range d.Path {
		steps = append(steps, step.Step)
	}
	if want := []string{"admitted", "claimed", "claim decided", "open requested", "open wrap", "started", "terminal", "result"}; !slices.Equal(steps, want) {
		t.Errorf("path %v, want %v", steps, want)
	}
	if d.Host != hostA || d.Receiver != receiverA || d.Agent != agentA || d.Model != "model-x" || len(d.Opens) != 1 || d.Opens[0].Answer != "wrap" || d.Attempts[0].Decision == nil || d.Attempts[0].Decision.Reason != wire.ClaimUnchanged {
		t.Errorf("path facts: %+v", d)
	}
	// Question 2: where its results went in S3.
	if d.Storage == nil || d.Storage.Endpoint != endpointA || d.Storage.Bucket != bucketA || d.Result == nil || len(d.Result.S3Outputs) != 1 || d.Result.S3Outputs[0].SHA256 != hashOf("big") || d.Result.S3Outputs[0].Size != 5000 ||
		len(d.Result.Outputs) != 1 || d.Result.Outputs[0].SHA256 != hashOf("notes") || len(d.Result.NotReturned) != 1 || d.Result.NotReturned[0].Reason != "missing" || d.Result.ReplySize != 70000 || d.Result.Profile != "profile-a" {
		t.Errorf("S3 and result references: storage %+v result %+v", d.Storage, d.Result)
	}
	if len(s.Storage) != 2 || s.Storage[0].Bucket != bucketA || s.Storage[0].Outputs != 1 || s.Storage[0].Bytes != 5000 || s.Storage[0].Runs != 6 || s.Storage[0].FailedRuns != 3 || s.Storage[1].Bucket != bucketB || s.Storage[1].Runs != 0 {
		t.Errorf("storage nodes: %+v", s.Storage)
	}
	// Question 3: per-component content.
	p := poolOf(t, s, poolA)
	if p.Lag.Status != LagOK && p.Lag.Status != LagZero || p.Group.Members != 1 || p.Custodian != receiverA || p.DeliveryLimit != 3 || !slices.Equal(p.Receivers, []string{receiverA, receiverB, receiverC}) || !slices.Equal(p.Tenants, []string{tenantA}) || !slices.Equal(p.Agents, []string{agentA}) {
		t.Errorf("pool: %+v", p)
	}
	if len(s.Custodians) != 1 || s.Custodians[0].KeyID != keyID || s.Custodians[0].Receiver != receiverA || s.Custodians[0].Status != "live" {
		t.Errorf("custodians: %+v", s.Custodians)
	}
	if len(s.Brokers) == 0 || s.Brokers[0].Host == "" || s.Brokers[0].Port == 0 || !slices.ContainsFunc(s.Brokers, func(b Broker) bool { return b.Controller }) {
		t.Errorf("brokers: %+v", s.Brokers)
	}
	a := receiverOf(t, s, receiverA)
	if a.Status != "live" || a.Capacity != 2 || len(a.Offers) != 1 || a.Offers[0].Profile != "profile-a" || !slices.Equal(a.Profiles, []string{"profile-a"}) {
		t.Errorf("receiver: %+v", a)
	}
	// Question 4: why a run failed, from the table.
	for id, class := range map[string]string{keyLost: ClassKeyLost, failed: ClassFailed, cancelled: ClassCanceled, unstarted: StartFailureClass("dispatch")} {
		if r := summaryOf(t, s, id); r.Class != class {
			t.Errorf("run %s class %q, want %q", id, r.Class, class)
		}
	}
	if r := summaryOf(t, s, exhausted); r.Stage != StageNoTerminal || r.Attempt != 3 {
		t.Errorf("exhausted: %+v", r)
	}
	if p.NoTerminal != 1 || p.ByClass[ClassFailed] != 1 || p.ByClass[ClassKeyLost] != 1 {
		t.Errorf("pool failure tallies: %+v", p)
	}
	// Question 6: every node kind, idle or not.
	if len(s.Pools) != 2 || poolOf(t, s, poolB).Runs != 0 || len(s.Receivers) != 3 || receiverOf(t, s, receiverC).Status != "silent" || receiverOf(t, s, receiverC).Runs != 0 {
		t.Errorf("idle pool and receiver: %+v %+v", s.Pools, s.Receivers)
	}
	if len(s.Hosts) != 2 || !slices.ContainsFunc(s.Hosts, func(h Host) bool { return h.Name == "host-idle" && h.Runs == 0 }) || len(s.Tenants) != 1 || s.Tenants[0].Runs != 6 || !slices.Equal(s.Tenants[0].Hosts, []string{hostA, "host-idle"}) {
		t.Errorf("hosts and tenants: %+v %+v", s.Hosts, s.Tenants)
	}
	// Question 7: runs per receiver over the window.
	if a.Runs != 1 || receiverOf(t, s, receiverB).Runs != 5 || a.ByState[string(wire.TaskStateCompleted)] != 1 {
		t.Errorf("runs per receiver: a %+v b %+v", a, receiverOf(t, s, receiverB))
	}
	// Nothing of the message texts reaches the JSON.
	encoded, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	detail, _ := json.Marshal(d)
	for _, sentinel := range []string{"s3cr3t", "the reply"} {
		if strings.Contains(string(encoded), sentinel) || strings.Contains(string(detail), sentinel) {
			t.Errorf("the model carries %q", sentinel)
		}
	}
}

// Test 11: the caps apply while reading, each reported when hit.
func TestBoundsApplyWhileReadingAndReport(t *testing.T) {
	f := newFixture(t)
	t0 := f.now.Add(-30 * time.Minute)
	var runs []string
	for i := range 4 {
		run := f.echo(t0.Add(time.Duration(i)*time.Minute), receiverA, tenantA, hostA, poolA, "run-"+string(rune('a'+i)))
		f.status(t0.Add(time.Duration(i)*time.Minute+time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "claimed", state: wire.TaskStateWorking})
		f.result(t0.Add(time.Duration(i)*time.Minute+2*time.Second), receiverA, tenantA, poolA, run, 1, wire.TaskStateCompleted, wire.ResultMetadata{Reply: &wire.ReturnedReply{SHA256: hashOf("r"), Size: 1}}, strings.Repeat("reply ", 400))
		runs = append(runs, run)
	}
	// Cancels of runs the view holds no record of wait for a join that never comes.
	for i := range 3 {
		f.cancel(t0.Add(10*time.Minute+time.Duration(i)*time.Second), tenantA, hostA, poolA, runID(tenantA, hostA, "pending-"+string(rune('a'+i))), nil)
	}
	// Each bounded batch lists the data topics' offsets again; listings counts the listings naming the events topic.
	var listings atomic.Int32
	f.cluster.ControlKey(int16(kmsg.ListOffsets), func(request kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		for _, topic := range request.(*kmsg.ListOffsetsRequest).Topics {
			if topic.Topic == operator.PoolEventsTopic(poolA) {
				listings.Add(1)
			}
		}
		return nil, nil, false
	})
	caps := testCaps()
	caps.Runs, caps.RecordBytes, caps.Digests, caps.Pending, caps.BatchRecords = 2, 2000, 3, 1, 3
	v := f.view(time.Hour, caps)
	s := v.Snapshot(0)
	if s.RunsTotal != 2 || !hasRun(s, runs[3]) || !hasRun(s, runs[2]) {
		t.Errorf("runs kept: %d %v; want the newest two", s.RunsTotal, s.Runs)
	}
	for _, r := range s.Runs {
		if r.Outputs != 0 {
			t.Errorf("a result over the record bound was decoded: %+v", r)
		}
	}
	hits := map[string]int64{}
	for _, hit := range s.Bounds {
		hits[hit.Name] = hit.Hits
	}
	if hits[capRuns] == 0 || hits[capRecordBytes] < 4 || hits[capDigests] == 0 || hits[capPending] < 2 {
		t.Errorf("bounds reported %v; want runs, record_bytes, digests and pending hit", hits)
	}
	if n := listings.Load(); n < 10 {
		t.Errorf("%d listings of the events topic during the rebuild; want one pair per bounded batch of 3 over 19 records", n)
	}
}

// A 24 h model at the assessment's volume: about 58,000 events records of about 1 KB with their feed copies, 1,440
// runs, read on kfake within 10 s and within the heap bound. Run with WHERRY_VIEW_SCALE=1; it produces about 150 MB.
func TestScaleWindowOnKfake(t *testing.T) {
	if os.Getenv("WHERRY_VIEW_SCALE") == "" {
		t.Skip("WHERRY_VIEW_SCALE is not set")
	}
	f := newFixture(t)
	runs, working := 1440, 33
	start := f.now.Add(-24 * time.Hour)
	step := 24 * time.Hour / time.Duration(runs)
	text := strings.Repeat("w", 480)
	producer, err := kgo.NewClient(append(f.client.Opts(), kgo.ProducerLinger(5*time.Millisecond), kgo.MaxBufferedRecords(100000))...)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	produce := func(topic, key string, value []byte, at time.Time) {
		producer.Produce(f.ctx, &kgo.Record{Topic: topic, Key: []byte(key), Value: value, Timestamp: at}, func(_ *kgo.Record, err error) {
			if err != nil {
				t.Error(err)
			}
		})
	}
	var records, bytes int64
	produced := time.Now()
	for i := range runs {
		at := start.Add(time.Duration(i) * step)
		name := "run-" + string(rune('a'+i%26)) + "-" + time.Duration(i).String()
		run := runID(tenantA, hostA, name)
		echo := &wire.Echo{Receiver: receiverA, Tenant: tenantA, Host: hostA, Pool: poolA, Run: run, RunNonce: nonce(name), Placement: wire.Placement{Agent: agentA}, TaskHash: hashOf(run), MessageID: "message-" + name}
		produce(operator.TenantTopic(tenantA), run, f.sign(echo, f.keys[receiverA]), at)
		kinds := []string{"claimed", "started"}
		for range working {
			kinds = append(kinds, "working")
		}
		kinds = append(kinds, "terminal")
		for j, kind := range kinds {
			state := wire.TaskStateWorking
			if kind == "terminal" {
				state = wire.TaskStateCompleted
			}
			value := f.statusRecord(at.Add(time.Duration(j)*10*time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: kind, state: state, text: text})
			produce(operator.PoolEventsTopic(poolA), run, value, at.Add(time.Duration(j)*10*time.Second))
			produce(operator.TenantTopic(tenantA), run, value, at.Add(time.Duration(j)*10*time.Second))
			records += 2
			bytes += 2 * int64(len(value))
		}
	}
	if err := producer.Flush(f.ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("produced %d records, %d MB, in %v", records, bytes>>20, time.Since(produced))
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	var peak atomic.Uint64
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
			}
			var now runtime.MemStats
			runtime.ReadMemStats(&now)
			if now.HeapInuse > peak.Load() {
				peak.Store(now.HeapInuse)
			}
		}
	}()
	started := time.Now()
	v := f.view(24*time.Hour, DefaultCaps)
	elapsed := time.Since(started)
	close(stop)
	s := v.Snapshot(0)
	t.Logf("rebuilt %d runs in %v; heap in use before %d MB, peak during %d MB, delta %d MB", s.RunsTotal, elapsed, before.HeapInuse>>20, peak.Load()>>20, (peak.Load()-min(peak.Load(), before.HeapInuse))>>20)
	if s.RunsTotal != runs || s.Counts["refused"] != 0 {
		t.Errorf("runs %d refused %d", s.RunsTotal, s.Counts["refused"])
	}
	if elapsed > 10*time.Second {
		t.Errorf("rebuild took %v, over 10 s", elapsed)
	}
	if delta := peak.Load() - min(peak.Load(), before.HeapInuse); delta > 256<<20 {
		t.Errorf("the rebuild added %d MB of heap, over the 256 MB bound", delta>>20)
	}
}

// The failure class table never reads a message.
func TestFailureClassTable(t *testing.T) {
	for _, c := range []struct {
		kind  string
		state wire.TaskState
		event string
		want  string
	}{
		{"terminal", wire.TaskStateFailed, wire.EventKeyLost, ClassKeyLost},
		{"terminal", wire.TaskStateFailed, "", ClassFailed},
		{"terminal", wire.TaskStateCanceled, "", ClassCanceled},
		{"rejected", wire.TaskStateRejected, "", ClassRejected},
		{"terminal", wire.TaskStateCompleted, "", ""},
		{"terminal", wire.TaskStateWorking, "", ClassUnclassified},
		{"working", wire.TaskStateWorking, "", ""},
	} {
		if got := FailureClass(c.kind, c.state, c.event); got != c.want {
			t.Errorf("FailureClass(%s, %s, %q) = %q, want %q", c.kind, c.state, c.event, got, c.want)
		}
	}
	if got := StartFailureClass("platform"); got != "start-failure:platform" {
		t.Errorf("StartFailureClass = %q", got)
	}
}

// The window predates the log when the earliest retained record is newer than the window's start, and the topics
// the state calls for that do not exist are reported missing.
func TestTopicsReportRetentionAndAbsence(t *testing.T) {
	f := newFixture(t)
	run := f.echo(f.now.Add(-time.Minute), receiverA, tenantA, hostA, poolA, "run")
	f.status(f.now.Add(-time.Minute), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "claimed", state: wire.TaskStateWorking})
	if err := f.cluster.DeleteTopic(operator.ReceiverProgressTopic(receiverB)); err != nil {
		t.Fatal(err)
	}
	v := f.view(48*time.Hour, testCaps())
	topics := map[string]Topic{}
	for _, topic := range v.Snapshot(0).Topics {
		topics[topic.Name] = topic
	}
	if !topics[operator.PoolEventsTopic(poolA)].PredatesLog || !topics[operator.TenantTopic(tenantA)].PredatesLog {
		t.Errorf("a window older than the log is not reported: %+v", topics)
	}
	if !topics[operator.ReceiverProgressTopic(receiverB)].Missing || topics[operator.ReceiverProgressTopic(receiverA)].Missing {
		t.Errorf("missing topics: %+v", topics)
	}
	if got := slices.Sorted(maps.Keys(topics)); !reflect.DeepEqual(got, slices.Sorted(slices.Values([]string{operator.OperatorTopic, operator.PoolEventsTopic(poolA), operator.CustodianTopic(poolA), operator.TenantTopic(tenantA), operator.ReceiverProgressTopic(receiverA), operator.ReceiverProgressTopic(receiverB)}))) {
		t.Errorf("topics followed: %v", got)
	}
}
