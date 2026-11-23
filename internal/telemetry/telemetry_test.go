package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// A nil Tracing is off: nothing is exported, no record carries a header and no context is read. On, the traceparent
// header is exactly its W3C size: 55 bytes, under an 11-byte key.
func TestOffAndHeaderSize(t *testing.T) {
	var off *Tracing
	ctx, span := off.Root(context.Background(), "submit", trace.SpanKindInternal)
	span.End()
	record := &kgo.Record{Headers: []kgo.RecordHeader{{Key: traceparentHeader, Value: []byte("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")}}}
	off.Inject(ctx, record)
	if off.Enabled() || len(record.Headers) != 1 || off.TraceParent(ctx) != "" || trace.SpanContextFromContext(off.Extract(ctx, record)).IsValid() {
		t.Fatalf("a nil tracing is on: headers %v", record.Headers)
	}
	exporter := tracetest.NewInMemoryExporter()
	previous := NewExporter
	NewExporter = func(context.Context, *Transport) (sdktrace.SpanExporter, error) { return exporter, nil }
	t.Cleanup(func() { NewExporter = previous })
	tracing, err := New(context.Background(), RoleHost, Names{Tenant: "t", Host: "h"}, nil, log.New(&strings.Builder{}, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tracing.Close() }()
	ctx, span = tracing.Root(context.Background(), "submit", trace.SpanKindInternal)
	defer span.End()
	record = &kgo.Record{}
	tracing.Inject(ctx, record)
	if len(record.Headers) != 1 || record.Headers[0].Key != traceparentHeader || len(record.Headers[0].Value) != maxTraceparent {
		t.Fatalf("headers %v, want one traceparent of %d bytes", record.Headers, maxTraceparent)
	}
	t.Logf("header bytes on the record: key %d, value %d, with length prefixes about %d", len(record.Headers[0].Key), len(record.Headers[0].Value), 3+len(record.Headers[0].Key)+len(record.Headers[0].Value))
}

// Tracing off does nothing: no helper allocates, Close builds no context, and every helper hands the caller's context
// back as it was.
func TestOffAllocatesNothing(t *testing.T) {
	var off *Tracing
	ctx := context.Background()
	record := &kgo.Record{Topic: "wherry.pool.pool-a.events", Headers: []kgo.RecordHeader{{Key: traceparentHeader, Value: []byte("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")}}}
	remote := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled, Remote: true})
	var sink context.Context
	var sinkSpan trace.Span
	calls := map[string]func(){
		"Close": func() { _ = off.Close() },
		"Root": func() {
			sink, sinkSpan = off.Root(ctx, "submit", trace.SpanKindInternal, AttrRun.String("run-a"), AttrPool.String("pool-a"))
		},
		"Start": func() {
			sink, sinkSpan = off.Start(ctx, "run", trace.SpanKindInternal, AttrRun.String("run-a"), AttrAttempt.Int64(1))
		},
		"Linked": func() {
			sink, sinkSpan = off.Linked(ctx, "submit", trace.SpanKindInternal, remote, AttrRun.String("run-a"))
		},
		"Producer":       func() { sink, sinkSpan = off.Producer(ctx, record.Topic, AttrRun.String("run-a"), AttrSize.Int(10)) },
		"ProducerUnder":  func() { sink, sinkSpan = off.ProducerUnder(ctx, record.Topic, remote, AttrRun.String("run-a")) },
		"ProducerLinked": func() { sink, sinkSpan = off.ProducerLinked(ctx, record.Topic, remote, AttrRun.String("run-a")) },
		"Consumer":       func() { sink, sinkSpan = off.Consumer(ctx, record, AttrRun.String("run-a")) },
		"ConsumerNamed":  func() { sink, sinkSpan = off.ConsumerNamed(ctx, record, "process", AttrRun.String("run-a")) },
		"Child":          func() { sink, sinkSpan = Child(ctx, "admit", AttrAttempt.Int64(1), AttrProfile.String("lab")) },
		"ChildOf":        func() { sink, sinkSpan = ChildOf(ctx, "read state", trace.SpanKindClient, AttrPool.String("pool-a")) },
		"Event":          func() { Event(ctx, "transcript commit", AttrChunks.Int(3)) },
		"Fail":           func() { Fail(sinkSpan, ClassError); FailUnset(sinkSpan, ClassError); FailCtx(ctx, ClassError) },
		"WithTenant":     func() { sink = off.WithTenant(ctx, "tenant-a") },
		"Inject":         func() { off.Inject(ctx, record) },
		"Extract":        func() { sink = off.Extract(ctx, record) },
		"TraceParent":    func() { _ = off.TraceParent(ctx) },
	}
	sink, sinkSpan = off.Root(ctx, "submit", trace.SpanKindInternal)
	for name, call := range calls {
		if allocs := testing.AllocsPerRun(100, call); allocs != 0 {
			t.Errorf("%s allocates %v times per call with tracing off", name, allocs)
		}
	}
	for _, name := range []string{"Root", "Start", "Linked", "Producer", "ProducerUnder", "ProducerLinked", "Consumer", "ConsumerNamed", "Child", "ChildOf", "WithTenant", "Extract"} {
		sink = nil
		calls[name]()
		if sink != ctx {
			t.Errorf("%s replaced the context with tracing off", name)
		}
	}
	if sinkSpan.IsRecording() || sinkSpan.SpanContext().IsValid() {
		t.Errorf("span %v with tracing off, want the no-op one", sinkSpan)
	}
}

// The reporter logs a failure once per interval for each topic and class, an expected one once per hour, and never
// anything but the topic and the class; it finds every failure under the exporter's wrapping of the joined per-topic
// errors, and a failure that is no export's gets a fixed text.
func TestReporterIntervals(t *testing.T) {
	logs := &strings.Builder{}
	now := time.Now()
	r := &reporter{logger: log.New(logs, "", 0), last: map[string]time.Time{}, now: func() time.Time { return now }}
	refused := exportError(TraceTopic("tenant-b"), kerr.TopicAuthorizationFailed)
	timedOut := exportError(TraceTopic("tenant-a"), context.DeadlineExceeded)
	for range 3 {
		// As the OTLP exporter hands an export's failures to the SDK: the join under one more wrapper.
		r.Handle(fmt.Errorf("traces export: %w", errors.Join(refused, timedOut)))
	}
	now = now.Add(logInterval)
	r.Handle(timedOut)
	r.Handle(fmt.Errorf("traces export: %w", refused))
	r.Handle(errors.New("s3cr3t detail"))
	r.Handle(fmt.Errorf("wrapped: %w", errors.New("s3cr3t detail")))
	now = now.Add(expectedInterval)
	r.Handle(refused)
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	want := []string{
		"tracing: export to wherry.traces.tenant-b failed: TOPIC_AUTHORIZATION_FAILED (expected for a topic not yet created or an ended entitlement)",
		"tracing: export to wherry.traces.tenant-a failed: timeout",
		"tracing: export to wherry.traces.tenant-a failed: timeout",
		"tracing: export failed: error",
		"tracing: export to wherry.traces.tenant-b failed: TOPIC_AUTHORIZATION_FAILED (expected for a topic not yet created or an ended entitlement)",
	}
	if strings.Join(lines, "\n") != strings.Join(want, "\n") {
		t.Fatalf("logged:\n%s\nwant:\n%s", strings.Join(lines, "\n"), strings.Join(want, "\n"))
	}
	for err, class := range map[error]string{kerr.UnknownTopicOrPartition: "UNKNOWN_TOPIC_OR_PARTITION", kerr.MessageTooLarge: "MESSAGE_TOO_LARGE", kgo.ErrClientClosed: "client closed", context.Canceled: ClassCanceled, kgo.ErrRecordTimeout: ClassTimeout, errors.New("s3cr3t detail"): ClassError} {
		if got := exportError("wherry.traces", err); got.Class != class || strings.Contains(got.Error(), "s3cr3t") {
			t.Errorf("class of %v: %q, want %q", err, got.Class, class)
		}
	}
}

// The trace client's buffer cap holds: an export of more records than the cap into a stalled topic never buffers more
// than the cap, and every record ends failed with a timeout within the export's context.
func TestTransportBufferCapHolds(t *testing.T) {
	fake := kfake.MustCluster()
	defer fake.Close()
	topic := TraceTopic("tenant-a")
	if err := fake.CreateTopic(topic, 1, nil); err != nil {
		t.Fatal(err)
	}
	stalled := fake.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: topic, Err: kerr.NotLeaderForPartition, Count: -1})
	defer stalled.Remove()
	transport := NewTransport([]kgo.Opt{kgo.SeedBrokers(fake.ListenAddrs()...)})
	if err := transport.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = transport.Stop(context.Background()) }()
	var spans []*tracepb.Span
	for i := range 3 * MaxBufferedRecords {
		traceID := make([]byte, 16)
		traceID[0], traceID[1] = byte(i>>8), byte(i)
		spans = append(spans, &tracepb.Span{TraceId: traceID, SpanId: make([]byte, 8), Name: "submit",
			Attributes: []*commonpb.KeyValue{{Key: string(AttrTenant), Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "tenant-a"}}}}})
	}
	ctx, cancel := context.WithTimeout(context.Background(), ExportBound)
	defer cancel()
	polling, stopPolling := context.WithCancel(context.Background())
	var most int64
	polled := make(chan struct{})
	go func() {
		defer close(polled)
		for polling.Err() == nil {
			most = max(most, transport.kafka.BufferedProduceRecords())
			time.Sleep(time.Millisecond)
		}
	}()
	began := time.Now()
	err := transport.UploadTraces(ctx, []*tracepb.ResourceSpans{{ScopeSpans: []*tracepb.ScopeSpans{{Spans: spans}}}})
	took := time.Since(began)
	stopPolling()
	<-polled
	var export *ExportError
	if !errors.As(err, &export) || export.Topic != topic || export.Class != ClassTimeout {
		t.Fatalf("upload = %v, want a timeout for %s", err, topic)
	}
	// The count includes the one record waiting for space in Produce.
	if most > MaxBufferedRecords+1 || most == 0 {
		t.Fatalf("%d records buffered at most, want up to the cap of %d", most, MaxBufferedRecords)
	}
	if took > ExportBound+500*time.Millisecond {
		t.Fatalf("upload of %d records into a stall took %s", len(spans), took)
	}
	if buffered := transport.kafka.BufferedProduceRecords(); buffered != 0 {
		t.Fatalf("%d records still buffered after the export returned", buffered)
	}
	t.Logf("%d records into a stalled topic: %d buffered at most, upload returned in %s", len(spans), most, took)
}
