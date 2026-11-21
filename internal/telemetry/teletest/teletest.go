// Package teletest holds what the spike's tracing tests share: an in-memory exporter installed for every provider a
// test builds, the allowlist check and the sentinel search over exported spans.
package teletest

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/benemon/wherry/internal/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// Exporter keeps every exported span in memory, across the shutdown of the providers that send to it.
type Exporter struct {
	*tracetest.InMemoryExporter
}

// Shutdown keeps the spans: the SDK's in-memory exporter would drop them.
func (e *Exporter) Shutdown(context.Context) error { return nil }

// Spans are the spans exported so far.
func (e *Exporter) Spans() []tracetest.SpanStub { return e.GetSpans() }

var mu sync.Mutex

// Install turns tracing on for the test, with an in-memory exporter behind every provider built while it runs.
func Install(t testing.TB) *Exporter {
	t.Helper()
	t.Setenv(telemetry.EndpointEnv, "http://127.0.0.1:1")
	return Hook(t)
}

// Hook installs the in-memory exporter without turning tracing on, so a test of the default can see any span exported.
func Hook(t testing.TB) *Exporter {
	t.Helper()
	mu.Lock()
	exporter := &Exporter{InMemoryExporter: tracetest.NewInMemoryExporter()}
	previous := telemetry.NewExporter
	telemetry.NewExporter = func(context.Context) (sdktrace.SpanExporter, error) { return exporter, nil }
	t.Cleanup(func() {
		telemetry.NewExporter = previous
		mu.Unlock()
	})
	return exporter
}

// Check fails the test for an attribute key outside the allowlist, an exception event, or a status description outside
// telemetry's classes and classes.
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
			if !telemetry.Allowed(kv.Key) {
				t.Errorf("span %s's resource carries attribute %s, outside the allowlist", s.Name, kv.Key)
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
