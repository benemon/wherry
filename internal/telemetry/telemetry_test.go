package telemetry

import (
	"context"
	"errors"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
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

// The reporter logs a failure once per interval for each topic and class, an expected one once per hour, and never
// anything but the topic and the class.
func TestReporterIntervals(t *testing.T) {
	logs := &strings.Builder{}
	now := time.Now()
	r := &reporter{logger: log.New(logs, "", 0), last: map[string]time.Time{}, now: func() time.Time { return now }}
	refused := exportError(TraceTopic("tenant-b"), kerr.TopicAuthorizationFailed)
	timedOut := exportError(TraceTopic("tenant-a"), context.DeadlineExceeded)
	for range 3 {
		r.Handle(errors.Join(refused, timedOut))
	}
	now = now.Add(logInterval)
	r.Handle(timedOut)
	r.Handle(refused)
	now = now.Add(expectedInterval)
	r.Handle(refused)
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	want := []string{
		"tracing: export to wherry.traces.tenant-b failed: TOPIC_AUTHORIZATION_FAILED (expected for a topic not yet created or an ended entitlement)",
		"tracing: export to wherry.traces.tenant-a failed: timeout",
		"tracing: export to wherry.traces.tenant-a failed: timeout",
		"tracing: export to wherry.traces.tenant-b failed: TOPIC_AUTHORIZATION_FAILED (expected for a topic not yet created or an ended entitlement)",
	}
	if strings.Join(lines, "\n") != strings.Join(want, "\n") {
		t.Fatalf("logged:\n%s\nwant:\n%s", strings.Join(lines, "\n"), strings.Join(want, "\n"))
	}
	for err, class := range map[error]string{kerr.UnknownTopicOrPartition: "UNKNOWN_TOPIC_OR_PARTITION", kerr.MessageTooLarge: "MESSAGE_TOO_LARGE", kgo.ErrClientClosed: "client closed", context.Canceled: ClassCanceled, errors.New("s3cr3t detail"): ClassError} {
		if got := exportError("wherry.traces", err); got.Class != class || strings.Contains(got.Error(), "s3cr3t") {
			t.Errorf("class of %v: %q, want %q", err, got.Class, class)
		}
	}
}
