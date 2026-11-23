package telemetry_test

import (
	"context"
	"errors"
	"log"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

const (
	tenantA, tenantB, tenantC = "tenant-a", "tenant-b", "tenant-c"
	writer, password          = "wherry.rcv.writer", "writer-password"
	// workTopic stands for a topic of the process's own records, a feed or a task topic.
	workTopic = "wherry.pool.pool-a.events"
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
	for _, topic := range []string{telemetry.TraceTopic(""), telemetry.TraceTopic(tenantA), telemetry.TraceTopic(tenantB), telemetry.TraceTopic(tenantC), workTopic} {
		if err := fake.CreateTopic(topic, 1, nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := kadm.NewClient(root).AlterUserSCRAMs(ctx, nil, []kadm.UpsertSCRAM{{User: writer, Mechanism: kadm.ScramSha512, Iterations: 4096, Password: password}}); err != nil {
		t.Fatal(err)
	}
	return &cluster{Cluster: fake, ctx: ctx, root: root}
}

// grant gives writer operations on topic.
func (c *cluster) grant(t *testing.T, topic string, operations ...kadm.ACLOperation) {
	t.Helper()
	builder := kadm.NewACLs().Allow(writer).AllowHosts("*").Topics(topic).ResourcePatternType(kadm.ACLPatternLiteral).Operations(operations...)
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

// grantTraces gives writer WRITE and DESCRIBE on tenants' trace topics.
func (c *cluster) grantTraces(t *testing.T, tenants ...string) {
	t.Helper()
	for _, tenant := range tenants {
		c.grant(t, telemetry.TraceTopic(tenant), kadm.OpWrite, kadm.OpDescribe)
	}
}

// opts are writer's Kafka options, as a receiver's base client is built from its bundle.
func (c *cluster) opts() []kgo.Opt {
	return []kgo.Opt{kgo.SeedBrokers(c.ListenAddrs()...), kgo.SASL(scram.Auth{User: writer, Pass: password}.AsSha512Mechanism()), kgo.DisableClientMetrics()}
}

// client is writer's client, the process's own.
func (c *cluster) client(t *testing.T) *kgo.Client {
	t.Helper()
	client, err := kgo.NewClient(c.opts()...)
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

// stall makes every produce to tenant's trace topic fail with NOT_LEADER_OR_FOLLOWER, which the client retries, until
// the fault is removed: a broker that answers but never acknowledges.
func (c *cluster) stall(tenant string) *kfake.FaultHandle {
	return c.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: telemetry.TraceTopic(tenant), Err: kerr.NotLeaderForPartition, Count: -1})
}

// provider is the receiver role's tracing over a trace client built from opts, logging to the returned builder.
func provider(t *testing.T, opts []kgo.Opt) (*telemetry.Tracing, *strings.Builder) {
	t.Helper()
	logs := &strings.Builder{}
	tracing, err := telemetry.New(context.Background(), telemetry.RoleReceiver, telemetry.Names{Pool: "pool-a", Receiver: "writer"}, opts, log.New(logs, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	return tracing, logs
}

// span ends one span of tenant under tracing.
func span(tracing *telemetry.Tracing, tenant, name string) {
	_, s := tracing.Root(telemetry.WithTenant(context.Background(), tenant), name, trace.SpanKindInternal)
	s.End()
}

// resourceSpans is one span of tenant, as the exporter hands it to the transport.
func resourceSpans(tenant string) []*tracepb.ResourceSpans {
	return []*tracepb.ResourceSpans{{ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{{TraceId: make([]byte, 16), SpanId: make([]byte, 8), Name: "submit",
		Attributes: []*commonpb.KeyValue{{Key: string(telemetry.AttrTenant), Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: tenant}}}}}}}}}}
}

// Brief test 1, transport routing: the spans of two tenants and the spans of none land on their topics, one record per
// trace keyed by the trace ID, a child in its root's record.
func TestTransportRoutesSpansByTenantAndTrace(t *testing.T) {
	c := newCluster(t)
	c.grantTraces(t, "", tenantA, tenantB)
	tracing, logs := provider(t, c.opts())
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

// Brief test 3, transport behaviour: refused topics in an export still deliver the others and return the errors; each
// failure is logged once per interval with the topic and the error class, and nothing of the spans. Two topics are
// refused in one export, so the handler's walk through the exporter's wrapping of the joined per-topic errors is the
// production path.
func TestTransportDeliversPastRefusedTopics(t *testing.T) {
	c := newCluster(t)
	c.grantTraces(t, "", tenantA)
	c.grant(t, telemetry.TraceTopic(tenantB), kadm.OpDescribe)
	const sentinel = "s3cr3t-span-name"
	refusedB := "tracing: export to wherry.traces.tenant-b failed: TOPIC_AUTHORIZATION_FAILED (expected for a topic not yet created or an ended entitlement)\n"
	refusedC := "tracing: export to wherry.traces.tenant-c failed: TOPIC_AUTHORIZATION_FAILED (expected for a topic not yet created or an ended entitlement)\n"
	// The handler is the process's: the second provider's identical failures, within the interval, are not logged again.
	for i, want := range []string{refusedB + refusedC, ""} {
		tracing, logs := provider(t, c.opts())
		for _, tenant := range []string{tenantB, tenantA, tenantC, ""} {
			_, span := tracing.Root(telemetry.WithTenant(context.Background(), tenant), sentinel+" "+tenant, trace.SpanKindInternal, telemetry.AttrRun.String(sentinel))
			span.End()
		}
		if err := tracing.Close(); err != nil {
			t.Fatalf("close = %v, want the failures reported through the handler", err)
		}
		if logs.String() != want {
			t.Fatalf("provider %d logged:\n%s\nwant:\n%s", i, logs, want)
		}
	}
	for tenant, want := range map[string]int{tenantA: 2, "": 2, tenantB: 0, tenantC: 0} {
		if records := c.read(t, tenant); len(records) != want {
			t.Fatalf("%s: %d records, want %d", telemetry.TraceTopic(tenant), len(records), want)
		}
	}
	// The error itself, as UploadTraces returns it to a flush: one per refused topic, the others delivered.
	tracing, _ := provider(t, c.opts())
	defer func() { _ = tracing.Close() }()
	for _, tenant := range []string{tenantB, tenantA, tenantC} {
		span(tracing, tenant, "process")
	}
	err := tracing.Flush(c.ctx)
	var export *telemetry.ExportError
	if !errors.As(err, &export) || export.Topic != telemetry.TraceTopic(tenantB) || export.Class != "TOPIC_AUTHORIZATION_FAILED" || !export.Expected || strings.Contains(err.Error(), sentinel) {
		t.Fatalf("flush = %v, want the refused topics' export errors", err)
	}
	// The provider and the exporter each wrap the transport's joined per-topic errors in one more.
	if exports := exportErrors(err); len(exports) != 2 || exports[1].Topic != telemetry.TraceTopic(tenantC) {
		t.Fatalf("flush = %v, want two per-topic errors under the wrappers", err)
	}
	if records := c.read(t, tenantA); len(records) != 3 {
		t.Fatalf("%s: %d records, want 3", telemetry.TraceTopic(tenantA), len(records))
	}
}

// Brief test 3, transport behaviour: an export the broker never acknowledges is dropped with a timeout under the
// export bound and its records leave the buffer, so the next export is not failed by the earlier deadline; Close
// stays within ShutdownBound; Stop during an upload waits for it, refuses the next and closes the trace client.
func TestTransportBoundsDeliveryAndStopWaits(t *testing.T) {
	c := newCluster(t)
	c.grantTraces(t, tenantA)
	stalled := c.stall(tenantA)
	tracing, logs := provider(t, c.opts())
	span(tracing, tenantA, "submit")
	began := time.Now()
	err := tracing.Flush(c.ctx)
	var export *telemetry.ExportError
	if !errors.As(err, &export) || export.Topic != telemetry.TraceTopic(tenantA) || export.Class != telemetry.ClassTimeout {
		t.Fatalf("flush = %v, want a timeout for %s", err, telemetry.TraceTopic(tenantA))
	}
	if took := time.Since(began); took > telemetry.ExportBound+500*time.Millisecond {
		t.Fatalf("flush took %s, over the %s export bound", took, telemetry.ExportBound)
	}
	// The broker answers again: the next export is delivered at once, nothing of the timed-out one before it.
	stalled.Remove()
	span(tracing, tenantA, "submit")
	if err := tracing.Flush(c.ctx); err != nil {
		t.Fatalf("flush after the stall = %v, want delivery", err)
	}
	if records := c.read(t, tenantA); len(records) != 1 {
		t.Fatalf("%s: %d records, want the one export after the stall", telemetry.TraceTopic(tenantA), len(records))
	}
	stalled = c.stall(tenantA)
	span(tracing, tenantA, "submit")
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
	transport := telemetry.NewTransport(c.opts())
	if err := transport.Start(c.ctx); err != nil {
		t.Fatal(err)
	}
	uploadCtx, cancel := context.WithTimeout(c.ctx, 500*time.Millisecond)
	defer cancel()
	uploaded := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Go(func() { uploaded <- transport.UploadTraces(uploadCtx, resourceSpans(tenantA)) })
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
	stalled.Remove()
}

// exportErrors are the ExportErrors at the leaves of err, in order.
func exportErrors(err error) []*telemetry.ExportError {
	switch wrapped := err.(type) {
	case nil:
		return nil
	case *telemetry.ExportError:
		return []*telemetry.ExportError{wrapped}
	case interface{ Unwrap() []error }:
		var exports []*telemetry.ExportError
		for _, err := range wrapped.Unwrap() {
			exports = append(exports, exportErrors(err)...)
		}
		return exports
	case interface{ Unwrap() error }:
		return exportErrors(wrapped.Unwrap())
	}
	return nil
}

// idempotent reports whether a produce request carries a producer ID in any of its batches.
func idempotent(request kmsg.Request) bool {
	for _, topic := range request.(*kmsg.ProduceRequest).Topics {
		for _, partition := range topic.Partitions {
			var batch kmsg.RecordBatch
			if batch.ReadFrom(partition.Records) == nil && batch.ProducerID >= 0 {
				return true
			}
		}
	}
	return false
}

// The transport's client is its own, built from the process's options and not idempotent: the process's client
// closes and the export still lands, and a broker that refuses idempotent produces on the trace topic, as one refuses
// a principal without the cluster's IDEMPOTENT_WRITE, refuses nothing of the trace client's.
func TestTransportOwnsANonIdempotentClient(t *testing.T) {
	c := newCluster(t)
	c.grantTraces(t, tenantA)
	c.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: telemetry.TraceTopic(tenantA), Err: kerr.ClusterAuthorizationFailed, Count: -1, When: idempotent})
	own := c.client(t)
	tracing, logs := provider(t, own.Opts())
	own.Close()
	span(tracing, tenantA, "submit")
	if err := tracing.Flush(c.ctx); err != nil {
		t.Fatalf("flush = %v, want delivery through the transport's own client", err)
	}
	if err := tracing.Close(); err != nil {
		t.Fatal(err)
	}
	if records := c.read(t, tenantA); len(records) != 1 {
		t.Fatalf("%s: %d records, want 1", telemetry.TraceTopic(tenantA), len(records))
	}
	if logs.Len() != 0 {
		t.Fatalf("logged:\n%s", logs)
	}
}

// During a sustained outage of the trace topic the process's own produces complete with unchanged latency: the trace
// client's stalled records are its own, the process's client has none.
func TestTransportOutageLeavesTheProcessClientAlone(t *testing.T) {
	c := newCluster(t)
	c.grantTraces(t, tenantA)
	c.grant(t, workTopic, kadm.OpWrite, kadm.OpDescribe)
	own := c.client(t)
	tracing, logs := provider(t, own.Opts())
	defer func() { _ = tracing.Close() }()
	produce := func() time.Duration {
		began := time.Now()
		if err := own.ProduceSync(c.ctx, &kgo.Record{Topic: workTopic, Value: []byte("status")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
		return time.Since(began)
	}
	median := func(n int) time.Duration {
		produce()
		var durations []time.Duration
		for range n {
			durations = append(durations, produce())
		}
		slices.Sort(durations)
		return durations[len(durations)/2]
	}
	before := median(20)
	stalled := c.stall(tenantA)
	defer stalled.Remove()
	// Exports into the stall, one after another, as the processor would issue them; each times out and is dropped.
	exporting, stopExporting := context.WithCancel(c.ctx)
	var exports sync.WaitGroup
	var timedOut atomic.Int32
	exports.Go(func() {
		for exporting.Err() == nil {
			span(tracing, tenantA, "submit")
			var export *telemetry.ExportError
			if err := tracing.Flush(exporting); errors.As(err, &export) && export.Class == telemetry.ClassTimeout {
				timedOut.Add(1)
			}
		}
	})
	time.Sleep(200 * time.Millisecond)
	during := median(20)
	// The outage is sustained: the first export has timed out and been dropped, and the exports go on.
	for timedOut.Load() == 0 {
		if c.ctx.Err() != nil {
			t.Fatal("no export timed out")
		}
		time.Sleep(50 * time.Millisecond)
	}
	later := median(20)
	stopExporting()
	exports.Wait()
	t.Logf("the process's own produce, median of 20: %s before the trace outage, %s and %s during it; %d exports timed out", before, during, later, timedOut.Load())
	for _, got := range []time.Duration{during, later} {
		if got > 2*before+100*time.Millisecond {
			t.Fatalf("the process's own produce took %s during the trace outage, %s before it", got, before)
		}
	}
	if own.BufferedProduceRecords() != 0 {
		t.Fatalf("%d records buffered on the process's client", own.BufferedProduceRecords())
	}
	if logs.Len() != 0 {
		t.Fatalf("logged:\n%s", logs)
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
	records, oversize, err := telemetry.Records([]*tracepb.ResourceSpans{
		{Resource: &resourcepb.Resource{Attributes: tenanted("tenant-r")}, ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{
			span("a1", traceOne, tenanted(tenantA)), span("r1", traceOne, nil), span("a2", traceTwo, tenanted(tenantA)), span("a1b", traceOne, tenanted(tenantA)),
		}}}},
		{ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{span("n", traceTwo, nil)}}}},
	})
	if err != nil || len(oversize) != 0 {
		t.Fatal(err, oversize)
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

// sized is one span of tenant whose ExportTraceServiceRequest encodes to exactly n bytes, its name the padding.
func sized(t *testing.T, tenant string, n int) []*tracepb.ResourceSpans {
	t.Helper()
	spans := resourceSpans(tenant)
	span := spans[0].ScopeSpans[0].Spans[0]
	for range 8 {
		size := proto.Size(&coltracepb.ExportTraceServiceRequest{ResourceSpans: spans})
		if size == n {
			return spans
		}
		span.Name = strings.Repeat("x", max(len(span.Name)+n-size, 0))
	}
	t.Fatalf("no span of %d bytes", n)
	return nil
}

// Brief test 6, record limit: the trace client built from the host's options, compression off, batches at most
// TraceBatchMaxBytes whatever the process's client batches; an export of exactly TraceRecordLimit bytes is one record
// the topic takes; a trace's group over the limit splits by spans into records under its key, in order; a span over the
// limit alone is dropped, the rest of its export delivered, and reported as oversize for its topic, which the handler
// logs.
func TestTransportBoundsRecordSize(t *testing.T) {
	c := newCluster(t)
	c.grantTraces(t, tenantA, tenantB)
	// A receiver's client batches the feed's 2 MiB; the trace client built from it batches 1 MiB.
	pinned, err := kgo.NewClient(telemetry.ClientOpts(append(c.opts(), kgo.ProducerBatchMaxBytes(2<<20)))...)
	if err != nil {
		t.Fatal(err)
	}
	pinned.Close()
	if got, _ := pinned.OptValue(kgo.ProducerBatchMaxBytes).(int32); got != telemetry.TraceBatchMaxBytes {
		t.Fatalf("trace client batches %d bytes, want %d", got, telemetry.TraceBatchMaxBytes)
	}
	host := append(c.opts(), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.ProducerBatchCompression(kgo.NoCompression()))
	transport := telemetry.NewTransport(host)
	if err := transport.Start(c.ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = transport.Stop(c.ctx) }()
	if err := transport.UploadTraces(c.ctx, sized(t, tenantA, telemetry.TraceRecordLimit)); err != nil {
		t.Fatalf("upload of %d bytes = %v, want delivery", telemetry.TraceRecordLimit, err)
	}
	records := c.read(t, tenantA)
	if len(records) != 1 || len(records[0].Value) != telemetry.TraceRecordLimit {
		t.Fatalf("%d records on %s, want one of %d bytes", len(records), telemetry.TraceTopic(tenantA), telemetry.TraceRecordLimit)
	}
	var export *telemetry.ExportError
	err = transport.UploadTraces(c.ctx, sized(t, tenantA, telemetry.TraceRecordLimit+1))
	if !errors.As(err, &export) || export.Topic != telemetry.TraceTopic(tenantA) || export.Class != telemetry.ClassOversize || export.Expected {
		t.Fatalf("upload of %d bytes = %v, want oversize for %s", telemetry.TraceRecordLimit+1, err, telemetry.TraceTopic(tenantA))
	}
	if records := c.read(t, tenantA); len(records) != 1 {
		t.Fatalf("%d records on %s after the oversize span, want the one before it", len(records), telemetry.TraceTopic(tenantA))
	}

	// One trace of five spans, each three tenths of the limit, splits into two records of three and two; among them a
	// span over the limit is dropped; another tenant's span in the export is delivered.
	traceID, spanID := []byte("0123456789abcdef"), []byte("spanspan")
	tenanted := func(tenant string) []*commonpb.KeyValue {
		return []*commonpb.KeyValue{{Key: string(telemetry.AttrTenant), Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: tenant}}}}
	}
	var group []*tracepb.Span
	for _, name := range []string{"one", "two", "huge", "three", "four", "five"} {
		size := telemetry.TraceRecordLimit * 3 / 10
		if name == "huge" {
			size = telemetry.TraceRecordLimit + 1
		}
		group = append(group, &tracepb.Span{TraceId: traceID, SpanId: spanID, Name: name + strings.Repeat(".", size), Attributes: tenanted(tenantA)})
	}
	export = nil
	err = transport.UploadTraces(c.ctx, []*tracepb.ResourceSpans{
		{ScopeSpans: []*tracepb.ScopeSpans{{Spans: group}}},
		{ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{{TraceId: traceID, SpanId: spanID, Name: "other", Attributes: tenanted(tenantB)}}}}},
	})
	if !errors.As(err, &export) || export.Topic != telemetry.TraceTopic(tenantA) || export.Class != telemetry.ClassOversize || len(exportErrors(err)) != 1 {
		t.Fatalf("upload = %v, want one oversize for %s", err, telemetry.TraceTopic(tenantA))
	}
	records = c.read(t, tenantA)
	if len(records) != 3 {
		t.Fatalf("%d records on %s, want the earlier one and the split group's two", len(records), telemetry.TraceTopic(tenantA))
	}
	var names []string
	for _, record := range records[1:] {
		if string(record.Key) != "30313233343536373839616263646566" || len(record.Value) > telemetry.TraceRecordLimit {
			t.Fatalf("split record keyed %s of %d bytes", record.Key, len(record.Value))
		}
		for _, s := range teletest.Spans(t, []*kgo.Record{record}) {
			names = append(names, strings.TrimRight(s.Name, "."))
		}
	}
	if !slices.Equal(names, []string{"one", "two", "three", "four", "five"}) {
		t.Fatalf("split records hold %v", names)
	}
	if records := c.read(t, tenantB); len(records) != 1 {
		t.Fatalf("%d records on %s, want the other tenant's span", len(records), telemetry.TraceTopic(tenantB))
	}

	// Through the provider: the handler logs the class and the topic, nothing of the span.
	tracing, logs := provider(t, host)
	span(tracing, tenantA, "s3cr3t-span-name"+strings.Repeat(".", telemetry.TraceRecordLimit))
	if err := tracing.Close(); err != nil {
		t.Fatal(err)
	}
	if logs.String() != "tracing: export to wherry.traces.tenant-a failed: oversize\n" {
		t.Fatalf("logged:\n%s", logs)
	}
}
