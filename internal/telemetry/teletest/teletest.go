// Package teletest holds what the tracing tests share: an in-memory exporter installed for every provider a test
// builds, the allowlist check, the sentinel search over exported spans and the decoding of trace-topic records.
package teletest

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/telemetry"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// Exporter keeps every exported span in memory, across the shutdown of the providers that send to it.
type Exporter struct {
	*tracetest.InMemoryExporter
}

// Shutdown keeps the spans: the SDK's in-memory exporter would drop them.
func (e *Exporter) Shutdown(context.Context) error { return nil }

// Spans are the spans exported so far.
func (e *Exporter) Spans() []tracetest.SpanStub { return e.GetSpans() }

var (
	mu sync.Mutex
	on bool
)

// Install hooks an in-memory exporter behind every provider built while the test runs and asks the test fixtures,
// through On, to turn each role's tracing on as its configuration would.
func Install(t testing.TB) *Exporter {
	t.Helper()
	exporter := Hook(t)
	on = true
	return exporter
}

// On reports whether Install asked the fixtures to turn tracing on. Nothing else turns it on.
func On() bool { return on }

// Hook installs the in-memory exporter without turning tracing on, so a test of the default can see any span exported.
func Hook(t testing.TB) *Exporter {
	t.Helper()
	mu.Lock()
	exporter := &Exporter{InMemoryExporter: tracetest.NewInMemoryExporter()}
	previous := telemetry.NewExporter
	telemetry.NewExporter = func(context.Context, *telemetry.Transport) (sdktrace.SpanExporter, error) { return exporter, nil }
	t.Cleanup(func() {
		telemetry.NewExporter = previous
		on = false
		mu.Unlock()
	})
	return exporter
}

// Read is topic's records from its start, through a client of opts.
func Read(t testing.TB, ctx context.Context, opts []kgo.Opt, topic string) []*kgo.Record {
	t.Helper()
	client, err := kgo.NewClient(append(slices.Clone(opts), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))...)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var records []*kgo.Record
	for {
		pollCtx, cancel := context.WithTimeout(ctx, time.Second)
		fetches := client.PollFetches(pollCtx)
		cancel()
		if err := fetches.Err0(); err != nil && err != context.DeadlineExceeded {
			t.Fatalf("read %s: %v", topic, err)
		}
		fetches.EachRecord(func(record *kgo.Record) { records = append(records, record) })
		if fetches.NumRecords() == 0 {
			return records
		}
	}
}

// Spans are the spans of records, each an ExportTraceServiceRequest, as a backend would store them.
func Spans(t testing.TB, records []*kgo.Record) []tracetest.SpanStub {
	t.Helper()
	var spans []tracetest.SpanStub
	for _, record := range records {
		var request coltracepb.ExportTraceServiceRequest
		if err := proto.Unmarshal(record.Value, &request); err != nil {
			t.Fatalf("record %d on %s is not an ExportTraceServiceRequest: %v", record.Offset, record.Topic, err)
		}
		for _, rs := range request.ResourceSpans {
			res := resource.NewSchemaless(attributes(rs.Resource.GetAttributes())...)
			for _, ss := range rs.ScopeSpans {
				scope := instrumentation.Scope{Name: ss.Scope.GetName(), Version: ss.Scope.GetVersion(), SchemaURL: ss.SchemaUrl}
				for _, span := range ss.Spans {
					stub := tracetest.SpanStub{
						Name:                 span.Name,
						SpanContext:          spanContext(span.TraceId, span.SpanId, span.Flags, span.TraceState),
						SpanKind:             kinds[span.Kind],
						StartTime:            time.Unix(0, int64(span.StartTimeUnixNano)),
						EndTime:              time.Unix(0, int64(span.EndTimeUnixNano)),
						Attributes:           attributes(span.Attributes),
						Resource:             res,
						InstrumentationScope: scope,
					}
					if len(span.ParentSpanId) != 0 {
						stub.Parent = spanContext(span.TraceId, span.ParentSpanId, span.Flags, "")
					}
					for _, event := range span.Events {
						stub.Events = append(stub.Events, sdktrace.Event{Name: event.Name, Attributes: attributes(event.Attributes), Time: time.Unix(0, int64(event.TimeUnixNano))})
					}
					for _, link := range span.Links {
						stub.Links = append(stub.Links, sdktrace.Link{SpanContext: spanContext(link.TraceId, link.SpanId, link.Flags, link.TraceState), Attributes: attributes(link.Attributes)})
					}
					if status := span.Status; status != nil {
						stub.Status = sdktrace.Status{Code: statuses[status.Code], Description: status.Message}
					}
					spans = append(spans, stub)
				}
			}
		}
	}
	return spans
}

var kinds = map[tracepb.Span_SpanKind]trace.SpanKind{
	tracepb.Span_SPAN_KIND_INTERNAL: trace.SpanKindInternal, tracepb.Span_SPAN_KIND_SERVER: trace.SpanKindServer, tracepb.Span_SPAN_KIND_CLIENT: trace.SpanKindClient,
	tracepb.Span_SPAN_KIND_PRODUCER: trace.SpanKindProducer, tracepb.Span_SPAN_KIND_CONSUMER: trace.SpanKindConsumer,
}

var statuses = map[tracepb.Status_StatusCode]codes.Code{tracepb.Status_STATUS_CODE_OK: codes.Ok, tracepb.Status_STATUS_CODE_ERROR: codes.Error}

func spanContext(traceID, spanID []byte, flags uint32, state string) trace.SpanContext {
	config := trace.SpanContextConfig{TraceFlags: trace.TraceFlags(flags), Remote: true}
	copy(config.TraceID[:], traceID)
	copy(config.SpanID[:], spanID)
	config.TraceState, _ = trace.ParseTraceState(state)
	return trace.NewSpanContext(config)
}

func attributes(kvs []*commonpb.KeyValue) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, len(kvs))
	for _, kv := range kvs {
		attrs = append(attrs, attribute.KeyValue{Key: attribute.Key(kv.Key), Value: value(kv.Value)})
	}
	return attrs
}

func value(v *commonpb.AnyValue) attribute.Value {
	switch v := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return attribute.StringValue(v.StringValue)
	case *commonpb.AnyValue_BoolValue:
		return attribute.BoolValue(v.BoolValue)
	case *commonpb.AnyValue_IntValue:
		return attribute.Int64Value(v.IntValue)
	case *commonpb.AnyValue_DoubleValue:
		return attribute.Float64Value(v.DoubleValue)
	case *commonpb.AnyValue_ArrayValue:
		var strs []string
		for _, element := range v.ArrayValue.Values {
			strs = append(strs, value(element).Emit())
		}
		return attribute.StringSliceValue(strs)
	}
	return attribute.StringValue(v.String())
}

// Bytes fails the test for any sentinel in the key or value of any record: what the broker stores.
func Bytes(t testing.TB, records []*kgo.Record, sentinels ...string) {
	t.Helper()
	for _, record := range records {
		for _, sentinel := range sentinels {
			if sentinel != "" && (strings.Contains(string(record.Key), sentinel) || strings.Contains(string(record.Value), sentinel)) {
				t.Errorf("record %d on %s carries the sentinel %q", record.Offset, record.Topic, sentinel)
			}
		}
	}
}

// Check fails the test for a span, event or link attribute key outside the allowlist, a resource key outside the
// resource allowlist, an exception event, or a status description outside telemetry's classes and classes.
func Check(t testing.TB, spans []tracetest.SpanStub, classes ...string) {
	t.Helper()
	allowed := append(slices.Clone(telemetry.Classes), classes...)
	for _, s := range spans {
		for _, kv := range s.Attributes {
			if !telemetry.Allowed(kv.Key) {
				t.Errorf("span %s carries attribute %s, outside the allowlist", s.Name, kv.Key)
			}
		}
		for _, kv := range s.Resource.Attributes() {
			if !telemetry.ResourceAllowed(kv.Key) {
				t.Errorf("span %s's resource carries attribute %s, outside the resource allowlist", s.Name, kv.Key)
			}
		}
		for _, link := range s.Links {
			for _, kv := range link.Attributes {
				if !telemetry.Allowed(kv.Key) {
					t.Errorf("span %s's link carries attribute %s, outside the allowlist", s.Name, kv.Key)
				}
			}
		}
		for _, event := range s.Events {
			if event.Name == "exception" {
				t.Errorf("span %s records an exception event", s.Name)
			}
			for _, kv := range event.Attributes {
				if !telemetry.Allowed(kv.Key) {
					t.Errorf("span %s event %s carries attribute %s, outside the allowlist", s.Name, event.Name, kv.Key)
				}
			}
		}
		if s.Status.Code == codes.Error && !slices.Contains(allowed, s.Status.Description) {
			t.Errorf("span %s has status description %q, not a fixed class", s.Name, s.Status.Description)
		}
		if s.Status.Code != codes.Error && s.Status.Description != "" {
			t.Errorf("span %s has a description %q without an error status", s.Name, s.Status.Description)
		}
	}
}

// Sentinels fails the test for any sentinel in any span's name, attributes, events, status, links, trace state or
// resource.
func Sentinels(t testing.TB, spans []tracetest.SpanStub, sentinels ...string) {
	t.Helper()
	for _, s := range spans {
		text := Text(s)
		for _, sentinel := range sentinels {
			if sentinel != "" && strings.Contains(text, sentinel) {
				t.Errorf("span %s carries a sentinel value:\n%s", s.Name, text)
			}
		}
	}
}

// Text is everything of a span a backend would store, as one string.
func Text(s tracetest.SpanStub) string {
	var b strings.Builder
	b.WriteString(s.Name)
	b.WriteString("\n")
	write := func(kvs []attribute.KeyValue) {
		for _, kv := range kvs {
			b.WriteString(string(kv.Key) + "=" + kv.Value.String() + "\n")
		}
	}
	write(s.Attributes)
	write(s.Resource.Attributes())
	for _, event := range s.Events {
		b.WriteString(event.Name + "\n")
		write(event.Attributes)
	}
	for _, link := range s.Links {
		b.WriteString(link.SpanContext.TraceState().String() + "\n")
		write(link.Attributes)
	}
	b.WriteString(s.Status.Description + "\n")
	b.WriteString(s.SpanContext.TraceState().String() + "\n")
	b.WriteString(s.Parent.TraceState().String() + "\n")
	return b.String()
}

// Attr is the string value of key on s, or "" when absent.
func Attr(s tracetest.SpanStub, key attribute.Key) string {
	for _, kv := range s.Attributes {
		if kv.Key == key {
			return kv.Value.String()
		}
	}
	return ""
}

// Resource is the string value of key on s's resource, or "" when absent.
func Resource(s tracetest.SpanStub, key attribute.Key) string {
	for _, kv := range s.Resource.Attributes() {
		if kv.Key == key {
			return kv.Value.String()
		}
	}
	return ""
}

// ByID maps spans by span ID.
func ByID(spans []tracetest.SpanStub) map[trace.SpanID]tracetest.SpanStub {
	m := make(map[trace.SpanID]tracetest.SpanStub, len(spans))
	for _, s := range spans {
		m[s.SpanContext.SpanID()] = s
	}
	return m
}

// Named returns the spans whose name has prefix, in export order.
func Named(spans []tracetest.SpanStub, prefix string) []tracetest.SpanStub {
	var named []tracetest.SpanStub
	for _, s := range spans {
		if strings.HasPrefix(s.Name, prefix) {
			named = append(named, s)
		}
	}
	return named
}

// Kinds counts the spans of each kind.
func Kinds(spans []tracetest.SpanStub) map[trace.SpanKind]int {
	counts := map[trace.SpanKind]int{}
	for _, s := range spans {
		counts[s.SpanKind]++
	}
	return counts
}
