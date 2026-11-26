package main

import (
	"context"
	"io"
	"strconv"
	"testing"

	"github.com/benemon/wherry/internal/telemetry"
	"github.com/twmb/franz-go/pkg/kadm"
)

// traceTopicConfigs are the configs operator init and tenant add give the trace topics, as docs/design.md lists them.
var traceTopicConfigs = map[string]string{
	"cleanup.policy":         "delete",
	"message.timestamp.type": "LogAppendTime",
	"retention.ms":           "604800000",
	"max.message.bytes":      strconv.Itoa(telemetry.TraceBatchMaxBytes),
}

// expectTraceTopics fails t unless each of topics exists on root's broker with traceTopicConfigs.
func expectTraceTopics(t *testing.T, root *kadm.Client, topics ...string) {
	t.Helper()
	described, err := root.DescribeTopicConfigs(context.Background(), topics...)
	if err != nil {
		t.Fatal(err)
	}
	if len(described) != len(topics) {
		t.Fatalf("%d topics described, want %d", len(described), len(topics))
	}
	for _, resource := range described {
		if resource.Err != nil {
			t.Fatalf("%s: %v", resource.Name, resource.Err)
		}
		got := map[string]string{}
		for _, config := range resource.Configs {
			if config.Value != nil {
				got[config.Key] = *config.Value
			}
		}
		for name, want := range traceTopicConfigs {
			if got[name] != want {
				t.Fatalf("%s %s = %q, want %q", resource.Name, name, got[name], want)
			}
		}
	}
}

// Brief test 1: operator init creates wherry.traces; tenant add creates the tenant's trace topic; on an install whose
// tenants predate the topics, running init again creates every live tenant's without tenant add, repairs one that
// drifted, and leaves the rest as they are, so check passes.
func TestOperatorInitCreatesTheTraceTopics(t *testing.T) {
	noTerminal(t)
	b, _ := kfakeInitBroker(t)
	dir := t.TempDir()
	if err := b.init(t, dir); err != nil {
		t.Fatal(err)
	}
	root := kadm.NewClient(b.root)
	expectTraceTopics(t, root, telemetry.TraceTopic(""))
	for _, tenant := range []string{"ta", "tb", "tc"} {
		if err := runOperator([]string{"add", "--tenant", tenant, "--dir", dir}, io.Discard, io.Discard); err != nil {
			t.Fatal(err)
		}
	}
	expectTraceTopics(t, root, telemetry.TraceTopic("ta"), telemetry.TraceTopic("tb"), telemetry.TraceTopic("tc"))
	ctx := context.Background()
	deleted, err := root.DeleteTopics(ctx, telemetry.TraceTopic(""), telemetry.TraceTopic("ta"), telemetry.TraceTopic("tb"))
	if err == nil {
		err = deleted.Error()
	}
	if err != nil {
		t.Fatal(err)
	}
	// tb's topic as an operator made it by hand before this release, with no size and a feed's retention.
	responses, err := root.CreateTopics(ctx, 1, -1, map[string]*string{"retention.ms": kadm.StringPtr("2592000000")}, telemetry.TraceTopic("tb"))
	if err == nil {
		err = responses.Error()
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := runOperator([]string{"check", "--dir", dir}, io.Discard, io.Discard); err == nil {
		t.Fatal("check passed on an install that predates the trace topics")
	}
	if err := b.init(t, dir); err != nil {
		t.Fatal(err)
	}
	expectTraceTopics(t, root, telemetry.TraceTopic(""), telemetry.TraceTopic("ta"), telemetry.TraceTopic("tb"), telemetry.TraceTopic("tc"))
	if err := runOperator([]string{"check", "--dir", dir}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
}
