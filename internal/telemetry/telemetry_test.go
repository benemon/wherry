package telemetry

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace"
)

// A short process's shutdown is bounded: an unreachable endpoint adds no more than ShutdownBound, with a margin for
// the SDK's own bookkeeping.
func TestCloseIsBoundedWithAnUnreachableEndpoint(t *testing.T) {
	t.Setenv(EndpointEnv, "http://10.255.255.1:4318")
	tracing, err := New(context.Background(), RoleHost, Names{Tenant: "t", Host: "h"})
	if err != nil {
		t.Fatal(err)
	}
	_, span := tracing.Root(context.Background(), "submit", trace.SpanKindInternal)
	span.End()
	began := time.Now()
	_ = tracing.Close()
	if took := time.Since(began); took > ShutdownBound+500*time.Millisecond {
		t.Fatalf("close took %s, over the %s bound", took, ShutdownBound)
	}
}

// The traceparent header is exactly its W3C size: 55 bytes, under an 11-byte key.
func TestInjectedHeaderSize(t *testing.T) {
	t.Setenv(EndpointEnv, "http://127.0.0.1:1")
	tracing, err := New(context.Background(), RoleHost, Names{Tenant: "t", Host: "h"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tracing.Close() }()
	ctx, span := tracing.Root(context.Background(), "submit", trace.SpanKindInternal)
	defer span.End()
	record := &kgo.Record{}
	tracing.Inject(ctx, record)
	if len(record.Headers) != 1 || record.Headers[0].Key != traceparentHeader || len(record.Headers[0].Value) != maxTraceparent {
		t.Fatalf("headers %v, want one traceparent of %d bytes", record.Headers, maxTraceparent)
	}
	t.Logf("header bytes on the record: key %d, value %d, with length prefixes about %d", len(record.Headers[0].Key), len(record.Headers[0].Value), 3+len(record.Headers[0].Key)+len(record.Headers[0].Value))
}
