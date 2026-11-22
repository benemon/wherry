package telemetry_test

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/telemetry"
	"github.com/benemon/wherry/internal/telemetry/teletest"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

const (
	tenantA, tenantB = "tenant-a", "tenant-b"
	writer, password = "wherry.rcv.writer", "writer-password"
)

// cluster is kfake with SASL and ACLs, the trace topics created, and a principal granted on the topics the test names:
// what slice 2's operator commands will create and grant.
type cluster struct {
	*kfake.Cluster
	ctx  context.Context
	root *kgo.Client
}

func newCluster(t *testing.T) *cluster {
	t.Helper()
	fake := kfake.MustCluster(kfake.EnableSASL(), kfake.EnableACLs(), kfake.Superuser("SCRAM-SHA-512", "admin", "password"))
	t.Cleanup(fake.Close)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	root, err := kgo.NewClient(kgo.SeedBrokers(fake.ListenAddrs()...), kgo.SASL(scram.Auth{User: "admin", Pass: "password"}.AsSha512Mechanism()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(root.Close)
	for _, tenant := range []string{"", tenantA, tenantB} {
		if err := fake.CreateTopic(telemetry.TraceTopic(tenant), 1, nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := kadm.NewClient(root).AlterUserSCRAMs(ctx, nil, []kadm.UpsertSCRAM{{User: writer, Mechanism: kadm.ScramSha512, Iterations: 4096, Password: password}}); err != nil {
		t.Fatal(err)
	}
	return &cluster{Cluster: fake, ctx: ctx, root: root}
}

// grant gives writer operations on tenant's trace topic.
func (c *cluster) grant(t *testing.T, tenant string, operations ...kadm.ACLOperation) {
	t.Helper()
	builder := kadm.NewACLs().Allow(writer).AllowHosts("*").Topics(telemetry.TraceTopic(tenant)).ResourcePatternType(kadm.ACLPatternLiteral).Operations(operations...)
	builder.PrefixUser()
	results, err := kadm.NewClient(c.root).CreateACLs(c.ctx, builder)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range results {
		if result.Err != nil {
			t.Fatal(result.Err)
		}
	}
}

// client is writer's client, the process's own, as a receiver's base client is built.
func (c *cluster) client(t *testing.T) *kgo.Client {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.SASL(scram.Auth{User: writer, Pass: password}.AsSha512Mechanism()), kgo.DisableClientMetrics())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

func (c *cluster) read(t *testing.T, tenant string) []*kgo.Record {
	t.Helper()
	return teletest.Read(t, c.ctx, c.root.Opts(), telemetry.TraceTopic(tenant))
}

// provider is the receiver role's tracing over client, logging to the returned builder.
func provider(t *testing.T, client *kgo.Client) (*telemetry.Tracing, *strings.Builder) {
	t.Helper()
	logs := &strings.Builder{}
	tracing, err := telemetry.New(context.Background(), telemetry.RoleReceiver, telemetry.Names{Pool: "pool-a", Receiver: "writer"}, client, log.New(logs, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	return tracing, logs
}

// Brief test 1, transport routing: the spans of two tenants and the spans of none land on their topics, one record per
// trace keyed by the trace ID, a child in its root's record.
func TestTransportRoutesSpansByTenantAndTrace(t *testing.T) {
	c := newCluster(t)
	for _, tenant := range []string{"", tenantA, tenantB} {
		c.grant(t, tenant, kadm.OpWrite, kadm.OpDescribe)
	}
	tracing, logs := provider(t, c.client(t))
	roots := map[string][]trace.SpanContext{}
	for _, tenant := range []string{tenantA, tenantA, tenantB, ""} {
		ctx, root := tracing.Root(telemetry.WithTenant(context.Background(), tenant), "process wherry.pool.pool-a.tasks", trace.SpanKindConsumer, telemetry.AttrRun.String("run-"+tenant))
		_, child := telemetry.Child(ctx, "run")
		child.End()
		root.End()
		roots[tenant] = append(roots[tenant], root.SpanContext())
	}
	if err := tracing.Close(); err != nil {
		t.Fatal(err)
	}
	for tenant, want := range map[string]int{tenantA: 2, tenantB: 1, "": 1} {
		records := c.read(t, tenant)
		if len(records) != want {
			t.Fatalf("%s: %d records, want %d", telemetry.TraceTopic(tenant), len(records), want)
		}
		keyed := map[string]bool{}
		for _, record := range records {
			keyed[string(record.Key)] = true
			spans := teletest.Spans(t, []*kgo.Record{record})
			if len(spans) != 2 || spans[0].SpanContext.TraceID().String() != string(record.Key) || spans[1].SpanContext.TraceID().String() != string(record.Key) {
				t.Fatalf("record %d on %s holds %d spans keyed %s", record.Offset, record.Topic, len(spans), record.Key)
			}
			for _, s := range spans {
				if teletest.Attr(s, telemetry.AttrTenant) != tenant || teletest.Resource(s, semconv.ServiceNameKey) != "wherry-receiver.pool-a.writer" || teletest.Resource(s, semconv.ServiceNamespaceKey) != telemetry.RoleReceiver {
					t.Errorf("span %s on %s: tenant %q, resource %v", s.Name, record.Topic, teletest.Attr(s, telemetry.AttrTenant), s.Resource.Attributes())
				}
			}
			teletest.Check(t, spans)
		}
		for _, root := range roots[tenant] {
			if !keyed[root.TraceID().String()] {
				t.Errorf("%s: no record keyed by trace %s", telemetry.TraceTopic(tenant), root.TraceID())
			}
		}
	}
	if logs.Len() != 0 {
		t.Fatalf("logged:\n%s", logs)
	}
}

// Brief test 3, transport behaviour: one refused topic in an export still delivers the others and returns the error;
// the failure is logged once per interval with the topic and the error class, and nothing of the spans.
func TestTransportDeliversPastARefusedTopic(t *testing.T) {
	c := newCluster(t)
	c.grant(t, "", kadm.OpWrite, kadm.OpDescribe)
	c.grant(t, tenantA, kadm.OpWrite, kadm.OpDescribe)
	c.grant(t, tenantB, kadm.OpDescribe)
	client := c.client(t)
	const sentinel = "s3cr3t-span-name"
	// The handler is the process's: the second provider's identical failure, within the interval, is not logged again.
	for i, want := range []string{"tracing: export to wherry.traces.tenant-b failed: TOPIC_AUTHORIZATION_FAILED (expected for a topic not yet created or an ended entitlement)\n", ""} {
		tracing, logs := provider(t, client)
		for _, tenant := range []string{tenantB, tenantA, ""} {
			_, span := tracing.Root(telemetry.WithTenant(context.Background(), tenant), sentinel+" "+tenant, trace.SpanKindInternal, telemetry.AttrRun.String(sentinel))
			span.End()
		}
		if err := tracing.Close(); err != nil {
			t.Fatalf("close = %v, want the failure reported through the handler", err)
		}
		if logs.String() != want {
			t.Fatalf("provider %d logged:\n%s\nwant:\n%s", i, logs, want)
		}
	}
	for tenant, want := range map[string]int{tenantA: 2, "": 2, tenantB: 0} {
		if records := c.read(t, tenant); len(records) != want {
			t.Fatalf("%s: %d records, want %d", telemetry.TraceTopic(tenant), len(records), want)
		}
	}
	// The error itself, as UploadTraces returns it to a flush: the refused topic's, the others delivered.
	tracing, _ := provider(t, client)
	defer func() { _ = tracing.Close() }()
	for _, tenant := range []string{tenantB, tenantA} {
		_, span := tracing.Root(telemetry.WithTenant(context.Background(), tenant), "process", trace.SpanKindInternal)
		span.End()
	}
	var export *telemetry.ExportError
	if err := tracing.Flush(c.ctx); !errors.As(err, &export) || export.Topic != telemetry.TraceTopic(tenantB) || export.Class != "TOPIC_AUTHORIZATION_FAILED" || !export.Expected || strings.Contains(err.Error(), sentinel) {
		t.Fatalf("flush = %v, want the refused topic's export error", err)
	}
	if records := c.read(t, tenantA); len(records) != 3 {
		t.Fatalf("%s: %d records, want 3", telemetry.TraceTopic(tenantA), len(records))
	}
}

// Brief test 3, transport behaviour: delivery is acknowledged within the export bound, under ShutdownBound, and an
// export the broker never acknowledges is dropped with a timeout; Stop during an upload waits for it and refuses the
// next; Close stays within ShutdownBound; the shared client is left open and produces after Stop.
func TestTransportBoundsDeliveryAndStopWaits(t *testing.T) {
	c := newCluster(t)
	c.grant(t, tenantA, kadm.OpWrite, kadm.OpDescribe)
	client := c.client(t)
	stalled := c.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Err: kerr.NotLeaderForPartition, Count: -1})
	tracing, logs := provider(t, client)
	_, span := tracing.Root(telemetry.WithTenant(context.Background(), tenantA), "submit", trace.SpanKindInternal)
	span.End()
	began := time.Now()
	err := tracing.Flush(c.ctx)
	var export *telemetry.ExportError
	if !errors.As(err, &export) || export.Topic != telemetry.TraceTopic(tenantA) || export.Class != telemetry.ClassTimeout {
		t.Fatalf("flush = %v, want a timeout for %s", err, telemetry.TraceTopic(tenantA))
	}
	if took := time.Since(began); took > telemetry.ExportBound+500*time.Millisecond {
		t.Fatalf("flush took %s, over the %s export bound", took, telemetry.ExportBound)
	}
	_, span = tracing.Root(telemetry.WithTenant(context.Background(), tenantA), "submit", trace.SpanKindInternal)
	span.End()
	began = time.Now()
	if err := tracing.Close(); err != nil {
		t.Fatalf("close = %v", err)
	}
	if took := time.Since(began); took > telemetry.ShutdownBound+500*time.Millisecond {
		t.Fatalf("close took %s, over the %s bound", took, telemetry.ShutdownBound)
	}
	if !strings.Contains(logs.String(), "tracing: export to wherry.traces.tenant-a failed: timeout") {
		t.Fatalf("logged:\n%s", logs)
	}

	// Stop during an upload: the transport alone, with an upload bounded as the processor bounds it.
	transport := telemetry.NewTransport(client)
	records, err := telemetry.Records([]*tracepb.ResourceSpans{{ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{{TraceId: make([]byte, 16), SpanId: make([]byte, 8), Name: "submit",
		Attributes: []*commonpb.KeyValue{{Key: string(telemetry.AttrTenant), Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: tenantA}}}}}}}}}})
	if err != nil || len(records) != 1 {
		t.Fatal(records, err)
	}
	uploadCtx, cancel := context.WithTimeout(c.ctx, 500*time.Millisecond)
	defer cancel()
	uploaded := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Go(func() {
		uploaded <- transport.UploadTraces(uploadCtx, []*tracepb.ResourceSpans{{ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{{TraceId: make([]byte, 16), SpanId: make([]byte, 8), Name: "submit",
			Attributes: []*commonpb.KeyValue{{Key: string(telemetry.AttrTenant), Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: tenantA}}}}}}}}}})
	})
	time.Sleep(100 * time.Millisecond)
	began = time.Now()
	if err := transport.Stop(c.ctx); err != nil {
		t.Fatalf("stop = %v", err)
	}
	stopTook := time.Since(began)
	wg.Wait()
	if err := <-uploaded; !errors.As(err, &export) || export.Class != telemetry.ClassTimeout {
		t.Fatalf("upload = %v, want the context's deadline as a timeout", err)
	}
	if stopTook < 300*time.Millisecond || stopTook > time.Second {
		t.Fatalf("stop took %s, want the upload's remaining %s", stopTook, 400*time.Millisecond)
	}
	if err := transport.UploadTraces(c.ctx, nil); err == nil {
		t.Fatal("an upload after stop was accepted")
	}
	// The client is the process's: open after Stop and Close, producing once the broker answers again. The records
	// the timed-out exports left in the partition's buffer fail with their expired context, and a record behind them
	// in the same batch with them; the next produce goes through.
	stalled.Remove()
	for {
		err := client.ProduceSync(c.ctx, &kgo.Record{Topic: telemetry.TraceTopic(tenantA), Value: []byte("after")}).FirstErr()
		if err == nil {
			break
		}
		if errors.Is(err, kgo.ErrClientClosed) || c.ctx.Err() != nil {
			t.Fatalf("the shared client after stop: %v", err)
		}
	}
}

// Records group spans by tenant, from the span's attribute else its resource's, and trace, keyed by the trace ID.
func TestRecordsGroupByTenantAndTrace(t *testing.T) {
	traceOne, traceTwo := []byte("0123456789abcdef"), []byte("fedcba9876543210")
	tenanted := func(tenant string) []*commonpb.KeyValue {
		return []*commonpb.KeyValue{{Key: string(telemetry.AttrTenant), Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: tenant}}}}
	}
	span := func(name string, traceID []byte, attrs []*commonpb.KeyValue) *tracepb.Span {
		return &tracepb.Span{TraceId: traceID, SpanId: []byte("spanspan"), Name: name, Attributes: attrs}
	}
	records, err := telemetry.Records([]*tracepb.ResourceSpans{
		{Resource: &resourcepb.Resource{Attributes: tenanted("tenant-r")}, ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{
			span("a1", traceOne, tenanted(tenantA)), span("r1", traceOne, nil), span("a2", traceTwo, tenanted(tenantA)), span("a1b", traceOne, tenanted(tenantA)),
		}}}},
		{ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{span("n", traceTwo, nil)}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	type want struct{ topic, key, names string }
	wants := []want{
		{telemetry.TraceTopic(tenantA), "30313233343536373839616263646566", "a1 a1b"},
		{telemetry.TraceTopic("tenant-r"), "30313233343536373839616263646566", "r1"},
		{telemetry.TraceTopic(tenantA), "66656463626139383736353433323130", "a2"},
		{telemetry.TraceTopic(""), "66656463626139383736353433323130", "n"},
	}
	if len(records) != len(wants) {
		t.Fatalf("%d records, want %d", len(records), len(wants))
	}
	for i, record := range records {
		var names []string
		for _, s := range teletest.Spans(t, []*kgo.Record{record}) {
			names = append(names, s.Name)
		}
		got := want{record.Topic, string(record.Key), strings.Join(names, " ")}
		if got != wants[i] {
			t.Errorf("record %d: %+v, want %+v", i, got, wants[i])
		}
	}
	if attrs := teletest.Spans(t, records[1:2])[0].Resource.Attributes(); len(attrs) != 1 || attrs[0] != attribute.String(string(telemetry.AttrTenant), "tenant-r") {
		t.Errorf("record 1 resource %v", attrs)
	}
}
