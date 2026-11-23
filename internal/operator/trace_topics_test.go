package operator

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/telemetry"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// traceRows are the grants of acls on trace topics, one line each.
func traceRows(acls []ACL) []string {
	var rows []string
	for _, acl := range acls {
		if strings.HasPrefix(acl.Resource, telemetry.TraceTopic("")) {
			resource := acl.Resource
			if acl.Prefix {
				resource += "*"
			}
			rows = append(rows, acl.Principal+" "+resource+" "+strings.Join(acl.Operations, ","))
		}
	}
	slices.Sort(rows)
	return rows
}

// Brief test 2: the trace rows of ACLs(state). A live host holds READ, WRITE and DESCRIBE on its tenant's trace topic
// whether or not the tenant has a live entitlement; a live receiver holds WRITE and DESCRIBE on wherry.traces and on
// the trace topic of every tenant entitled to its pool now or before; an ended or revoking host or receiver holds
// none.
func TestTraceACLRows(t *testing.T) {
	state := NewState()
	state.Tenants["t"] = false
	state.Tenants["u"] = false
	state.Hosts[TenantPrincipal("t", "h")] = Host{"t", "h"}
	state.Hosts[TenantPrincipal("u", "h")] = Host{"u", "h"}
	state.Hosts[TenantPrincipal("t", "ended")] = Host{"t", "ended"}
	state.Hosts[TenantPrincipal("t", "revoking")] = Host{"t", "revoking"}
	state.Ended[TenantPrincipal("t", "ended")] = true
	state.Revoking[TenantPrincipal("t", "revoking")] = true
	for _, r := range []string{"r", "ended", "revoking"} {
		state.Receivers[r] = ReceiverConfig{Receiver: r, Pool: "p"}
	}
	state.Ended[ReceiverPrincipal("ended")] = true
	state.Revoking[ReceiverPrincipal("revoking")] = true
	// t's entitlement to p has ended; u never had one.
	state.EndedEntitlements["t"] = map[string]bool{"p": true}
	want := []string{
		"wherry.rcv.r wherry.traces WRITE,DESCRIBE",
		"wherry.rcv.r wherry.traces.t WRITE,DESCRIBE",
		"wherry.tnt.t.h wherry.traces.t READ,WRITE,DESCRIBE",
		"wherry.tnt.u.h wherry.traces.u READ,WRITE,DESCRIBE",
	}
	if got := traceRows(ACLs(state)); !slices.Equal(got, want) {
		t.Fatalf("trace rows:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	// The entitlement live again: the same rows.
	state.Entitlements["t"], state.EndedEntitlements = map[string]bool{"p": true}, map[string]map[string]bool{}
	if got := traceRows(ACLs(state)); !slices.Equal(got, want) {
		t.Fatalf("trace rows under a live entitlement:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// topicConfig is topic's value of name on the broker, or the describe's error.
func topicConfig(ctx context.Context, service *Service, topic, name string) (string, error) {
	values, err := service.topicConfigs(ctx, name, topic)
	if err != nil {
		return "", err
	}
	return values[topic], nil
}

// expectTraceConfigs fails t unless each of topics exists with traceConfigs and one partition.
func expectTraceConfigs(t *testing.T, service *Service, topics ...string) {
	t.Helper()
	ctx := context.Background()
	if err := service.onePartition(ctx, false, topics...); err != nil {
		t.Fatal(err)
	}
	for _, topic := range topics {
		for name, want := range traceConfigs {
			if got, err := topicConfig(ctx, service, topic, name); err != nil || got != want {
				t.Fatalf("%s %s = %q, %v; want %q", topic, name, got, err, want)
			}
		}
	}
}

// Brief test 1: tenant add creates the tenant's trace topic with traceConfigs, one partition, and leaves it out of the
// feeds.
func TestTenantAddCreatesTheTraceTopic(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := t.Context()
	if err := service.TenantAdd(ctx, "t"); err != nil {
		t.Fatal(err)
	}
	expectTraceConfigs(t, service, telemetry.TraceTopic("t"))
	state, err := service.LoadState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if feeds := tenantFeeds(state); slices.Contains(feeds, telemetry.TraceTopic("t")) {
		t.Fatalf("trace topic among the feeds: %v", feeds)
	}
	if got := traceTopics(state); !slices.Equal(got, []string{telemetry.TraceTopic(""), telemetry.TraceTopic("t")}) {
		t.Fatal(got)
	}
}

// Brief test 1: on an install that predates the trace topics, EnsureTraceTopics, operator init's step, creates
// wherry.traces and the trace topic of every live tenant without tenant add, none for an ended tenant, repairs the
// configs of one that exists with other configs, and changes nothing when repeated.
func TestEnsureTraceTopicsOnAPredatingInstall(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := t.Context()
	for _, tenant := range []string{"a", "b", "gone"} {
		if err := service.TenantAdd(ctx, tenant); err != nil {
			t.Fatal(err)
		}
	}
	if err := removeTenantFixture(service, ctx, "gone"); err != nil {
		t.Fatal(err)
	}
	deleteTopics(t, service, telemetry.TraceTopic(""), telemetry.TraceTopic("a"), telemetry.TraceTopic("b"), telemetry.TraceTopic("gone"))
	// b's topic made by hand, as an operator might have before this slice, with a feed's retention and no size.
	if err := service.createTopics(ctx, strconv.Itoa(tenantRetentionMS), telemetry.TraceTopic("b")); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := service.EnsureTraceTopics(ctx); err != nil {
			t.Fatal(err)
		}
		expectTraceConfigs(t, service, telemetry.TraceTopic(""), telemetry.TraceTopic("a"), telemetry.TraceTopic("b"))
		if err := service.onePartition(ctx, false, telemetry.TraceTopic("gone")); !errors.Is(err, kerr.UnknownTopicOrPartition) {
			t.Fatalf("ended tenant's trace topic: %v; want it absent", err)
		}
		if err := service.Check(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

// Brief test 7: operator check reports a missing trace topic and drifted trace configs, wherry.traces and a tenant's
// alike, naming operator init; once init's step has run it passes.
func TestCheckReportsTraceTopics(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := t.Context()
	if err := service.TenantAdd(ctx, "t"); err != nil {
		t.Fatal(err)
	}
	if err := service.Check(ctx); err != nil {
		t.Fatal(err)
	}
	for _, topic := range []string{telemetry.TraceTopic(""), telemetry.TraceTopic("t")} {
		deleteTopics(t, service, topic)
		if err := service.Check(ctx); err == nil || err.Error() != "trace topic "+topic+" is missing; operator init creates it" {
			t.Fatalf("check without %s = %v", topic, err)
		}
		if err := service.createTopics(ctx, "1", topic); err != nil {
			t.Fatal(err)
		}
		if err := service.Check(ctx); err == nil || !strings.HasPrefix(err.Error(), "topic "+topic+" max.message.bytes is ") || !strings.HasSuffix(err.Error(), "; operator init sets it") {
			t.Fatalf("check with %s made by hand = %v", topic, err)
		}
		if err := service.EnsureTraceTopics(ctx); err != nil {
			t.Fatal(err)
		}
		if err := service.Check(ctx); err != nil {
			t.Fatal(err)
		}
		setTopicRetention(t, service, topic, 1)
		if err := service.Check(ctx); err == nil || err.Error() != "topic "+topic+" retention.ms is 1, want "+strconv.Itoa(traceRetentionMS)+"; operator init sets it" {
			t.Fatalf("check with %s drifted = %v", topic, err)
		}
		if err := service.EnsureTraceTopics(ctx); err != nil {
			t.Fatal(err)
		}
		if err := service.Check(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

// traceFixture is a tenant t with host h, entitled to pool p with receiver r, converged, and the trace rows the state
// calls for.
func traceFixture(t *testing.T, service *Service) (host, receiver string, rows []ACL) {
	t.Helper()
	ctx := context.Background()
	if err := service.PoolApply(ctx, "p", modelTerms(), false); err != nil {
		t.Fatal(err)
	}
	if err := service.ReceiverAdd(ctx, modelConfig("r", "p"), service.Broker, filepath.Join(t.TempDir(), "r.yaml")); err != nil {
		t.Fatal(err)
	}
	if err := hostFixture(t, service, ctx, "t", "h", filepath.Join(t.TempDir(), "h.yaml"), false); err != nil {
		t.Fatal(err)
	}
	if err := entitleFixture(t, service, ctx, "t", "p"); err != nil {
		t.Fatal(err)
	}
	state, err := service.LoadState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, acl := range ACLs(state) {
		if strings.HasPrefix(acl.Resource, telemetry.TraceTopic("")) {
			rows = append(rows, acl)
		}
	}
	if len(rows) != 3 {
		t.Fatalf("%d trace rows, want the host's and the receiver's two", len(rows))
	}
	return TenantPrincipal("t", "h"), ReceiverPrincipal("r"), rows
}

// Brief test 3: convergence restores each trace grant deleted behind it, operation by operation, and withdraws a
// prefix grant on wherry.traces. seeded for the host, which no row calls for.
func TestConvergeRestoresTraceGrantsAndWithdrawsAPrefix(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := t.Context()
	host, _, rows := traceFixture(t, service)
	for _, acl := range rows {
		for _, operation := range acl.Operations {
			dropped := acl
			dropped.Operations = []string{operation}
			builder, err := aclBuilder(dropped)
			if err != nil {
				t.Fatal(err)
			}
			if err := service.deleteACLs(ctx, builder); err != nil {
				t.Fatal(err)
			}
			if ok, err := granted(ctx, service, dropped); err != nil || ok {
				t.Fatalf("%s %s on %s after deletion: %t, %v", acl.Principal, operation, acl.Resource, ok, err)
			}
			if err := service.converge(ctx); err != nil {
				t.Fatal(err)
			}
			if ok, err := granted(ctx, service, dropped); err != nil || !ok {
				t.Fatalf("%s %s on %s after convergence: %t, %v; want it restored", acl.Principal, operation, acl.Resource, ok, err)
			}
		}
	}
	widened := ACL{host, "topic", telemetry.TraceTopic(""), true, []string{"READ", "WRITE", "DESCRIBE"}}
	widened.Resource += "."
	builder, err := aclBuilder(widened)
	if err != nil {
		t.Fatal(err)
	}
	results, err := kadm.NewClient(service.Operator).CreateACLs(ctx, builder)
	if err == nil {
		for _, result := range results {
			err = errors.Join(err, result.Err)
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := service.converge(ctx); err != nil {
		t.Fatal(err)
	}
	if ok, err := granted(ctx, service, widened); err != nil || ok {
		t.Fatalf("prefix grant on %s after convergence: %t, %v; want it withdrawn", widened.Resource, ok, err)
	}
	if ok, err := granted(ctx, service, ACL{host, "topic", telemetry.TraceTopic("t"), false, []string{"READ", "WRITE", "DESCRIBE"}}); err != nil || !ok {
		t.Fatalf("literal host grant after convergence: %t, %v", ok, err)
	}
}

// describedACLs are the operations the broker grants principal on topic, literal, sorted.
func describedACLs(ctx context.Context, service *Service, principal, topic string) ([]string, error) {
	builder := kadm.NewACLs().Allow(principal).AllowHosts("*").Topics(topic).ResourcePatternType(kadm.ACLPatternLiteral).Operations(kadm.OpAny)
	builder.PrefixUser()
	results, err := kadm.NewClient(service.Operator).DescribeACLs(ctx, builder)
	if err != nil {
		return nil, err
	}
	var operations []string
	for _, result := range results {
		if result.Err != nil {
			return nil, result.Err
		}
		for _, described := range result.Described {
			operations = append(operations, described.Operation.String())
		}
	}
	slices.Sort(operations)
	return operations, nil
}

// Brief test 5: after tenant remove the broker holds no trace row of the tenant's host, keeps the receiver's rows on
// the tenant's trace topic for its ended entitlement, and the topic remains.
func TestTenantRemoveRevokesTheHostTraceRows(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := t.Context()
	host, receiver, _ := traceFixture(t, service)
	topic := telemetry.TraceTopic("t")
	for principal, want := range map[string][]string{host: {"DESCRIBE", "READ", "WRITE"}, receiver: {"DESCRIBE", "WRITE"}} {
		if got, err := describedACLs(ctx, service, principal, topic); err != nil || !slices.Equal(got, want) {
			t.Fatalf("%s on %s before removal: %v, %v; want %v", principal, topic, got, err, want)
		}
	}
	revocationBounds(t, service, "1", "1", "1")
	if err := removeTenantFixture(service, ctx, "t"); err != nil {
		t.Fatal(err)
	}
	if got, err := describedACLs(ctx, service, host, topic); err != nil || len(got) != 0 {
		t.Fatalf("%s on %s after removal: %v, %v; want none", host, topic, got, err)
	}
	if got, err := describedACLs(ctx, service, receiver, topic); err != nil || !slices.Equal(got, []string{"DESCRIBE", "WRITE"}) {
		t.Fatalf("%s on %s after removal: %v, %v; want the rows kept for the ended entitlement", receiver, topic, got, err)
	}
	if got, err := describedACLs(ctx, service, receiver, telemetry.TraceTopic("")); err != nil || !slices.Equal(got, []string{"DESCRIBE", "WRITE"}) {
		t.Fatalf("%s on wherry.traces after removal: %v, %v", receiver, got, err)
	}
	expectTraceConfigs(t, service, topic)
}

// expectFetchDenied fails t unless consumer's first poll is refused with TOPIC_AUTHORIZATION_FAILED.
func expectFetchDenied(t *testing.T, consumer *kgo.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		fetches := consumer.PollFetches(ctx)
		for _, fetchErr := range fetches.Errors() {
			if errors.Is(fetchErr.Err, kerr.TopicAuthorizationFailed) {
				return
			}
			if !errors.Is(fetchErr.Err, context.DeadlineExceeded) {
				t.Fatalf("want TOPIC_AUTHORIZATION_FAILED, got %v", fetchErr.Err)
			}
		}
		if fetches.NumRecords() > 0 {
			t.Fatalf("read %d records from %s, want TOPIC_AUTHORIZATION_FAILED", fetches.NumRecords(), fetches.Records()[0].Topic)
		}
	}
	t.Fatal("no fetch was refused within the wait")
}

// principalClient connects to the kfake cluster behind service as the principal whose bundle is at path.
func principalClient(t *testing.T, service *Service, path string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	bundle, err := ReadBundle(path)
	if err != nil {
		t.Fatal(err)
	}
	client, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(service.Broker), kgo.SASL(scram.Auth{User: bundle.Principal, Pass: bundle.Password}.AsSha512Mechanism()), kgo.RequestRetries(1), kgo.FetchMaxWait(10 * time.Millisecond)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

// Brief test 4 on kfake, which enforces the same grants: a host writes and reads its tenant's trace topic and neither
// reads nor writes another tenant's, whose own host does (the positive control); a receiver writes the trace topics of
// its pool's tenants and wherry.traces, not a non-entitled tenant's.
func TestTraceTopicGrantsOnKfake(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := t.Context()
	dir := t.TempDir()
	if err := service.PoolApply(ctx, "p", modelTerms(), false); err != nil {
		t.Fatal(err)
	}
	if err := service.ReceiverAdd(ctx, modelConfig("r", "p"), service.Broker, filepath.Join(dir, "r.yaml")); err != nil {
		t.Fatal(err)
	}
	for _, tenant := range []string{"a", "b"} {
		if err := hostFixture(t, service, ctx, tenant, "h", filepath.Join(dir, tenant+".yaml"), false); err != nil {
			t.Fatal(err)
		}
	}
	if err := entitleFixture(t, service, ctx, "a", "p"); err != nil {
		t.Fatal(err)
	}
	hostA, hostB := principalClient(t, service, filepath.Join(dir, "a.yaml")), principalClient(t, service, filepath.Join(dir, "b.yaml"))
	receiver := principalClient(t, service, filepath.Join(dir, "r.yaml"))
	write := func(client *kgo.Client, topic, value string) error {
		return client.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte("trace"), Value: []byte(value)}).FirstErr()
	}
	for _, allowed := range []struct {
		client *kgo.Client
		topic  string
		value  string
	}{{hostA, telemetry.TraceTopic("a"), "host-a"}, {hostB, telemetry.TraceTopic("b"), "host-b"}, {receiver, telemetry.TraceTopic("a"), "receiver-a"}, {receiver, telemetry.TraceTopic(""), "receiver"}} {
		if err := write(allowed.client, allowed.topic, allowed.value); err != nil {
			t.Fatalf("write %s to %s: %v", allowed.value, allowed.topic, err)
		}
	}
	expectDenied(t, write(hostA, telemetry.TraceTopic("b"), "forged"))
	expectDenied(t, write(hostA, telemetry.TraceTopic(""), "untenanted"))
	expectDenied(t, write(receiver, telemetry.TraceTopic("b"), "non-entitled"))
	expectDenied(t, listEnd(hostA, telemetry.TraceTopic("b")))
	// The receiver describes the topics it writes and reads none of them.
	expectFetchDenied(t, principalClient(t, service, filepath.Join(dir, "r.yaml"), kgo.ConsumeTopics(telemetry.TraceTopic("a")), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart())))
	reader := principalClient(t, service, filepath.Join(dir, "a.yaml"), kgo.ConsumeTopics(telemetry.TraceTopic("a")), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	var values []string
	for deadline := time.Now().Add(10 * time.Second); len(values) < 2 && time.Now().Before(deadline); {
		fetches := reader.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			t.Fatal(err)
		}
		fetches.EachRecord(func(record *kgo.Record) { values = append(values, string(record.Value)) })
	}
	slices.Sort(values)
	if !slices.Equal(values, []string{"host-a", "receiver-a"}) {
		t.Fatalf("host a read %v from its trace topic; want its own span and the receiver's", values)
	}
	if got, err := describedACLs(ctx, service, TenantPrincipal("a", "h"), telemetry.TraceTopic("")); err != nil || len(got) != 0 {
		t.Fatalf("host on wherry.traces: %v, %v; want no grant", got, err)
	}
}
