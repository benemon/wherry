package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/telemetry"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Brief tests 1 and 2: after every check passes, operator check lists the topics of an ended tenant and its host, of an
// ended host of a live tenant and of an ended receiver, then a pool's signature topic that a relabel left, each with the
// offsets it retains after records were deleted from its start, marks the receiver key topic kept, prints a summary and
// exits 0. Live and revoking actors' topics are not listed, a clean install prints nothing, a leftover topic an admin
// deleted is not listed, and a check that fails prints no leftovers.
func TestCheckReportsLeftovers(t *testing.T) {
	noTerminal(t)
	b, _ := kfakeInitBroker(t)
	dir := t.TempDir()
	if err := b.init(t, dir); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	check := func() (string, error) {
		t.Helper()
		var stdout bytes.Buffer
		err := runOperator([]string{"check", "--dir", dir}, &stdout, io.Discard)
		// A host on its enrolment password is reported by check, and is no leftover.
		var lines []string
		for _, line := range strings.SplitAfter(stdout.String(), "\n") {
			if !strings.HasSuffix(line, " is on its enrolment password\n") {
				lines = append(lines, line)
			}
		}
		return strings.Join(lines, ""), err
	}
	if out, err := check(); err != nil || out != "" {
		t.Fatalf("check on a clean install printed %q, %v; want nothing", out, err)
	}
	wherry := func(args ...string) {
		t.Helper()
		if err := runOperator(append(args, "--dir", dir), io.Discard, io.Discard); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
	}
	pool := filepath.Join(writeTree(t, map[string]string{"pool.yaml": "agents:\n  - name: claude\nproducer_byte_rate: 1048576\nrequest_percentage: 10\n"}), "pool.yaml")
	for _, args := range [][]string{
		{"apply", "--pool", "pool-a", "--file", pool},
		{"add", "--tenant", "ta"}, {"allow", "--tenant", "ta", "--pool", "pool-a"}, {"add", "--host", "ha", "--tenant", "ta"},
		{"add", "--tenant", "tb"}, {"allow", "--tenant", "tb", "--pool", "pool-a"}, {"add", "--host", "hb-live", "--tenant", "tb"},
		{"add", "--host", "hb-ended", "--tenant", "tb"}, {"add", "--host", "hb-revoking", "--tenant", "tb"},
	} {
		wherry(args...)
	}
	// ra's first profile is relabelled, which leaves its signature topic; rb shares ra's second signature.
	profile := func(zone string) string {
		return "pool: pool-a\nplugin_sha256: {nomad: " + fakeSHA256 + "}\n" + basePlugin + item("name: claude\nagent: claude\nlabels: {zone: "+zone+"}\n", "      ")
	}
	for _, receiver := range []string{"ra", "rb"} {
		localFixture(t, dir, receiver)
	}
	wherry("apply", "--receiver", "ra", "--file", filepath.Join(writeTree(t, map[string]string{"receiver.yaml": profile("a")}), "receiver.yaml"))
	wherry("apply", "--receiver", "rb", "--file", filepath.Join(writeTree(t, map[string]string{"receiver.yaml": profile("b")}), "receiver.yaml"))
	revoking, err := operator.EncodeRecord(operator.Record{Kind: "revoking", Tenant: "tb", Host: "hb-revoking"})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.root.ProduceSync(ctx, &kgo.Record{Topic: operator.OperatorTopic, Key: []byte("revoking"), Value: revoking}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if out, err := check(); err != nil || out != "" {
		t.Fatalf("check with every actor live or revoking printed %q, %v; want nothing", out, err)
	}
	wherry("apply", "--receiver", "ra", "--file", filepath.Join(writeTree(t, map[string]string{"receiver.yaml": profile("b")}), "receiver.yaml"))
	stale := operator.SignatureTopic("pool-a", "claude", map[string]string{"platform": "nomad", "tier": "base", "zone": "a"})
	revocationBounds(t, &operator.Service{Operator: b.root}, "1")
	stubProbe(t, deniedProbe)
	for _, args := range [][]string{{"remove", "--host", "ha", "--tenant", "ta"}, {"remove", "--tenant", "ta"}, {"remove", "--host", "hb-ended", "--tenant", "tb"}, {"remove", "--receiver", "rb"}} {
		wherry(args...)
	}
	// Five records on ta's feed, the first two deleted, and two on rb's key topic: the offsets are what is retained.
	produce := func(topic string, count int) {
		t.Helper()
		for i := range count {
			if err := b.root.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(strconv.Itoa(i))}).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
	}
	produce(operator.TenantTopic("ta"), 5)
	produce(operator.ReceiverTopic("rb"), 2)
	admin := kadm.NewClient(b.root)
	deleted, err := admin.DeleteRecords(ctx, kadm.Offsets{operator.TenantTopic("ta"): {0: {Topic: operator.TenantTopic("ta"), Partition: 0, At: 2}}})
	if err == nil {
		err = deleted.Error()
	}
	if err != nil {
		t.Fatal(err)
	}
	lines := []string{
		"leftover wherry.tenant.ta tenant=ta offsets=3",
		"leftover wherry.tenant.ta.transcript tenant=ta offsets=0",
		"leftover " + telemetry.TraceTopic("ta") + " tenant=ta offsets=0",
		"leftover wherry.host.ta.ha tenant=ta offsets=0",
		"leftover wherry.host.tb.hb-ended tenant=tb host=hb-ended offsets=0",
		"leftover wherry.receiver.rb receiver=rb offsets=2 kept: holds keys",
		"leftover wherry.receiver.rb.progress receiver=rb offsets=0",
		"leftover " + stale + " pool=pool-a unattributed offsets=0",
		"leftovers: 8 topics, 5 offsets",
	}
	want := ""
	for _, line := range lines {
		want += line + "\n"
	}
	if out, err := check(); err != nil || out != want {
		t.Fatalf("check printed\n%s\nwith %v; want\n%s", out, err, want)
	}
	// Brief test 2: an admin deleted the progress topic.
	responses, err := admin.DeleteTopics(ctx, operator.ReceiverProgressTopic("rb"))
	if err == nil {
		err = responses.Error()
	}
	if err != nil {
		t.Fatal(err)
	}
	want = ""
	for _, line := range append(lines[:6:6], lines[7], "leftovers: 7 topics, 5 offsets") {
		want += line + "\n"
	}
	if out, err := check(); err != nil || out != want {
		t.Fatalf("check after the progress topic's deletion printed\n%s\nwith %v; want\n%s", out, err, want)
	}
	// A check that fails before the leftovers step prints none.
	responses, err = admin.DeleteTopics(ctx, telemetry.TraceTopic(""))
	if err == nil {
		err = responses.Error()
	}
	if err != nil {
		t.Fatal(err)
	}
	if out, err := check(); err == nil || out != "" {
		t.Fatalf("check with a missing trace topic printed %q, %v; want nothing and the failure", out, err)
	}
}

func TestLeftoversWriteErrorIsReturned(t *testing.T) {
	if err := writeLeftovers(errorWriter{}, []operator.Leftover{{Topic: "wherry.host.t.h", Tenant: "t", Host: "h"}}); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("write to a closed pipe = %v, want it returned", err)
	}
}
