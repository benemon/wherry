package view

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/telemetry"
	"github.com/benemon/wherry/internal/wire"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func id(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func str(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: key, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}}}
}

// span is a span of trace tid under parent, named name, with the run attribute and attrs.
func span(tid, sid, parent []byte, name, run string, at time.Time, attrs ...*commonpb.KeyValue) *tracepb.Span {
	return &tracepb.Span{TraceId: tid, SpanId: sid, ParentSpanId: parent, Name: name, Kind: tracepb.Span_SPAN_KIND_INTERNAL,
		StartTimeUnixNano: uint64(at.UnixNano()), EndTimeUnixNano: uint64(at.Add(time.Second).UnixNano()), Attributes: append([]*commonpb.KeyValue{str(string(telemetry.AttrRun), run), str(string(telemetry.AttrTenant), tenantA)}, attrs...)}
}

// traceRecord writes one OTLP record of spans under service to tenant-a's trace topic, keyed by the first span's
// trace ID, stamped at.
func (f *fixture) traceRecord(at time.Time, service string, spans ...*tracepb.Span) {
	f.t.Helper()
	request := &coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource:   &resourcepb.Resource{Attributes: []*commonpb.KeyValue{str("service.name", service)}},
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: spans}},
	}}}
	value, err := proto.Marshal(request)
	if err != nil {
		f.t.Fatal(err)
	}
	f.produce(telemetry.TraceTopic(tenantA), hex.EncodeToString(spans[0].TraceId), value, at)
}

func names(nodes []SpanNode, into *[]string) {
	for _, node := range nodes {
		*into = append(*into, node.Name)
		names(node.Children, into)
	}
}

// Test 8: the drill-down keeps the run's spans across its trace IDs, refuses another run's span sharing a trace ID,
// cuts malformed, oversized and cyclic records and cap exhaustion with markers, and resumes a refresh at the last end.
func TestDrillDown(t *testing.T) {
	f := newFixture(t)
	t0 := f.now.Add(-20 * time.Minute)
	run := f.echo(t0, receiverA, tenantA, hostA, poolA, "run")
	f.status(t0.Add(time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "claimed", state: wire.TaskStateWorking})
	other := f.echo(t0, receiverA, tenantA, hostA, poolA, "other")
	taskTopic := operator.SignatureTopic(poolA, agentA, map[string]string{"zone": "dc1"})
	t1, t2 := id(16), id(16)
	submit, send := id(8), id(8)
	hostService := "wherry-host." + tenantA + "." + hostA
	receiverService := "wherry-receiver." + poolA + "." + receiverA
	f.traceRecord(t0.Add(-30*time.Second), hostService,
		span(t1, submit, nil, "submit", run, t0.Add(-time.Minute), str(string(telemetry.AttrHost), hostA), str(string(telemetry.AttrOutput), "s3cr3t-span-attr")),
		span(t1, send, submit, "send "+taskTopic, run, t0.Add(-50*time.Second), str("messaging.destination.name", taskTopic)))
	// Another run's span carrying the selected run's trace ID, in a record that also holds the run's own span, so the
	// record passes the byte test and the span check alone excludes it; and a span with a hostile name and key.
	f.traceRecord(t0.Add(2*time.Second), hostService, span(t1, id(8), submit, "submit", other, t0.Add(time.Second)), span(t1, id(8), submit, "read state", run, t0.Add(time.Second)))
	f.traceRecord(t0.Add(3*time.Second), receiverService,
		span(t2, id(8), nil, "process "+taskTopic+" profile-a", run, t0.Add(2*time.Second), str(string(telemetry.AttrReceiver), receiverA)),
		span(t2, id(8), nil, "s3cr3t-span-name", run, t0.Add(2*time.Second), str("s3cr3t.key", "s3cr3t-span-value")))
	// A record written by any host: a resource naming no known service, which shows as other.
	t4 := id(16)
	f.traceRecord(t0.Add(4*time.Second), "s3cr3t-service", span(t4, id(8), nil, "admit", run, t0.Add(3*time.Second)))
	// Malformed bytes holding the run ID; an oversized record holding it; a cycle.
	large := raiseTraceLimit(t, f)
	f.produce(telemetry.TraceTopic(tenantA), "malformed", []byte("not a protobuf "+run), t0.Add(5*time.Second))
	produceLarge(t, f, large, "oversized", append([]byte(run), make([]byte, telemetry.TraceRecordLimit)...), t0.Add(6*time.Second))
	t3 := id(16)
	a, b := id(8), id(8)
	f.traceRecord(t0.Add(7*time.Second), receiverService, span(t3, a, b, "run", run, t0.Add(6*time.Second)), span(t3, b, a, "admit", run, t0.Add(6*time.Second)))

	caps := testCaps()
	caps.TraceBytes, caps.BatchRecords = 64<<20, 100
	v := f.view(time.Hour, caps)
	forest, err := v.traces.Trace(f.ctx, run)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, trace := range forest.Traces {
		ids = append(ids, trace.ID)
	}
	if want := []string{hex.EncodeToString(t1), hex.EncodeToString(t2), hex.EncodeToString(t3), hex.EncodeToString(t4)}; !slices.Equal(slices.Sorted(slices.Values(ids)), slices.Sorted(slices.Values(want))) {
		t.Errorf("traces %v, want the run's four", ids)
	}
	if forest.Spans != 8 {
		t.Errorf("%d spans, want the run's 8", forest.Spans)
	}
	var got []string
	for _, trace := range forest.Traces {
		names(trace.Roots, &got)
	}
	if slices.Contains(got, "s3cr3t-span-name") || !slices.Contains(got, nameOther) || !slices.Contains(got, "submit") || !slices.Contains(got, "send "+taskTopic) || !slices.Contains(got, "process "+taskTopic+" profile-a") {
		t.Errorf("span names %v", got)
	}
	for _, trace := range forest.Traces {
		if trace.ID == hex.EncodeToString(t4) && (len(trace.Roots) != 1 || trace.Roots[0].Service != nameOther) {
			t.Errorf("unknown service: %+v", trace.Roots)
		}
	}
	for _, marker := range []string{MarkerMalformed, MarkerOversized, MarkerCycle, MarkerFiltered} {
		if !slices.Contains(forest.Markers, marker) {
			t.Errorf("markers %v lack %q", forest.Markers, marker)
		}
	}
	for _, trace := range forest.Traces {
		if trace.ID == hex.EncodeToString(t1) {
			if len(trace.Roots) != 1 || trace.Roots[0].Name != "submit" || len(trace.Roots[0].Children) != 2 || trace.Roots[0].Attributes[string(telemetry.AttrHost)] != hostA || trace.Roots[0].Service != hostService {
				t.Errorf("trace 1: %+v", trace.Roots)
			}
			if _, ok := trace.Roots[0].Attributes[string(telemetry.AttrOutput)]; ok {
				t.Error("an output name the model does not know passed")
			}
		}
		if trace.ID == hex.EncodeToString(t3) {
			var markers []string
			var walk func([]SpanNode)
			walk = func(nodes []SpanNode) {
				for _, node := range nodes {
					markers = append(markers, node.Marker)
					walk(node.Children)
				}
			}
			walk(trace.Roots)
			if !slices.Contains(markers, MarkerCycle) {
				t.Errorf("cycle not marked: %v", markers)
			}
		}
	}
	// The other run's span never appears, under its own run either.
	if other, err := v.traces.Trace(f.ctx, other); err != nil || other.Spans != 1 || other.Traces[0].ID != hex.EncodeToString(t1) || other.Traces[0].Roots[0].Name != "submit" {
		t.Errorf("other run: %+v %v", other, err)
	}
	// Delayed spans appear on refresh, reading the new records alone.
	first := forest.Read
	late := id(8)
	f.traceRecord(f.now, receiverService, span(t2, late, nil, "return task", run, f.now.Add(-time.Second)))
	forest, err = v.traces.Trace(f.ctx, run)
	if err != nil {
		t.Fatal(err)
	}
	if forest.Spans != 9 || forest.Read.Records != 1 || forest.Read.Bytes >= first.Bytes {
		t.Errorf("refresh: %d spans, read %+v after %+v; want one more span from one record", forest.Spans, forest.Read, first)
	}
	// Cap exhaustion is cut and marked.
	small := caps
	small.TraceSpans = 2
	w := New(f.client, Options{Window: time.Hour, Caps: small, Now: f.clock})
	t.Cleanup(w.Close)
	if err := w.Rebuild(f.ctx); err != nil {
		t.Fatal(err)
	}
	forest, err = w.traces.Trace(f.ctx, run)
	if err != nil || forest.Spans != 2 || !slices.Contains(forest.Markers, markerTruncated+capTraceSpans) {
		t.Errorf("span cap: %d spans %v %v", forest.Spans, forest.Markers, err)
	}
	if hits := w.Snapshot(0).Counts[capTraceSpans]; hits == 0 {
		t.Error("the span cap did not report")
	}
	if _, err := v.traces.Trace(f.ctx, "run-unknown"); err != errNotFound {
		t.Errorf("unknown run: %v", err)
	}
	encoded := forest.String()
	if strings.Contains(encoded, "s3cr3t") {
		t.Error("the forest's text carries a sentinel")
	}
}

// A hostile oversized record is skipped before decoding: a record over the limit holding the run ID and invalid bytes
// is marked oversized, never malformed, and costs no decode.
func TestOversizedTraceRecordSkippedBeforeDecoding(t *testing.T) {
	f := newFixture(t)
	run := f.echo(f.now.Add(-time.Minute), receiverA, tenantA, hostA, poolA, "run")
	produceLarge(t, f, raiseTraceLimit(t, f), "oversized", append([]byte("garbage "+run), make([]byte, telemetry.TraceRecordLimit)...), f.now)
	v := f.view(time.Hour, DefaultCaps)
	ctx, cancel := context.WithTimeout(f.ctx, 30*time.Second)
	defer cancel()
	forest, err := v.traces.Trace(ctx, run)
	if err != nil || !slices.Contains(forest.Markers, MarkerOversized) || slices.Contains(forest.Markers, MarkerMalformed) || forest.Spans != 0 {
		t.Errorf("forest %+v, %v", forest, err)
	}
}
