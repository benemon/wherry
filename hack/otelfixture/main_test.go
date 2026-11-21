package main

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// recordingExporter keeps every span in memory and records when each was exported.
type recordingExporter struct {
	*tracetest.InMemoryExporter
	mu       sync.Mutex
	exported map[trace.SpanID]time.Time
}

func newRecordingExporter() *recordingExporter {
	return &recordingExporter{InMemoryExporter: tracetest.NewInMemoryExporter(), exported: map[trace.SpanID]time.Time{}}
}

func (e *recordingExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	now := time.Now()
	e.mu.Lock()
	for _, s := range spans {
		e.exported[s.SpanContext().SpanID()] = now
	}
	e.mu.Unlock()
	return e.InMemoryExporter.ExportSpans(ctx, spans)
}

func (e *recordingExporter) exportedAt(id trace.SpanID) time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.exported[id]
}

// emitted runs the fixture against an in-memory exporter and returns its spans and summary.
func emitted(t *testing.T, cfg config) ([]tracetest.SpanStub, summary, *recordingExporter) {
	t.Helper()
	exporter := newRecordingExporter()
	f, err := newFixture(cfg, exporter)
	if err != nil {
		t.Fatal(err)
	}
	s, err := f.run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Read before shutdown: the in-memory exporter resets on Shutdown.
	spans := exporter.GetSpans()
	if err := f.shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(spans) != s.spans {
		t.Fatalf("exported %d spans, summary counts %d", len(spans), s.spans)
	}
	return spans, s, exporter
}

func attr(kvs []attribute.KeyValue, key attribute.Key) (attribute.Value, bool) {
	for _, kv := range kvs {
		if kv.Key == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

func stringAttr(t *testing.T, s tracetest.SpanStub, key attribute.Key) string {
	t.Helper()
	v, ok := attr(s.Attributes, key)
	if !ok {
		t.Fatalf("span %s has no %s", s.Name, key)
	}
	return v.AsString()
}

func byID(spans []tracetest.SpanStub) map[trace.SpanID]tracetest.SpanStub {
	m := make(map[trace.SpanID]tracetest.SpanStub, len(spans))
	for _, s := range spans {
		m[s.SpanContext.SpanID()] = s
	}
	return m
}

func serviceName(s tracetest.SpanStub) string {
	v, _ := attr(s.Resource.Attributes(), semconv.ServiceNameKey)
	return v.AsString()
}

func variants(t *testing.T, test func(t *testing.T, variant string)) {
	for _, v := range []string{"a", "b"} {
		t.Run("variant "+v, func(t *testing.T) { test(t, v) })
	}
}

// Every CONSUMER span is a child of the PRODUCER span that sent its record, in the same trace, to the same topic.
func TestConsumerParentIsProducer(t *testing.T) {
	variants(t, func(t *testing.T, variant string) {
		spans, _, _ := emitted(t, config{variant: variant, runs: 10})
		ids := byID(spans)
		consumers, producers := 0, 0
		for _, s := range spans {
			switch s.SpanKind {
			case trace.SpanKindProducer:
				producers++
			case trace.SpanKindConsumer:
				consumers++
			default:
				continue
			}
			if s.SpanKind != trace.SpanKindConsumer {
				continue
			}
			if !s.Parent.IsValid() {
				t.Errorf("consumer %s has no parent", s.Name)
				continue
			}
			parent, ok := ids[s.Parent.SpanID()]
			if !ok {
				t.Errorf("consumer %s: parent %s was not exported", s.Name, s.Parent.SpanID())
				continue
			}
			if parent.SpanKind != trace.SpanKindProducer {
				t.Errorf("consumer %s: parent %s is %s, not PRODUCER", s.Name, parent.Name, parent.SpanKind)
			}
			if parent.SpanContext.TraceID() != s.SpanContext.TraceID() {
				t.Errorf("consumer %s: parent in trace %s, consumer in %s", s.Name, parent.SpanContext.TraceID(), s.SpanContext.TraceID())
			}
			if got, want := stringAttr(t, s, semconv.MessagingDestinationNameKey), stringAttr(t, parent, semconv.MessagingDestinationNameKey); got != want {
				t.Errorf("consumer %s reads %s, its producer sent to %s", s.Name, got, want)
			}
			if serviceName(parent) == serviceName(s) {
				t.Errorf("consumer %s and its producer share service %s: the viewers drop same-service edges", s.Name, serviceName(s))
			}
			if len(s.Links) != 0 {
				t.Errorf("consumer %s carries %d links", s.Name, len(s.Links))
			}
		}
		// Four producers per run (send, open request, wrap, terminal), four consumers, plus one released attempt per pool.
		runs := 2 * 3 * 10
		if producers != runs*4 {
			t.Errorf("%d producers, want %d", producers, runs*4)
		}
		if consumers != runs*4+2 {
			t.Errorf("%d consumers, want %d", consumers, runs*4+2)
		}
	})
}

// Each node is its own service, named by the variant, with the role as namespace.
func TestServicesPerVariant(t *testing.T) {
	want := map[string]map[string]string{
		"a": {
			"wherry-host.ben.mini":     roleHost,
			"wherry-host.alice.laptop": roleHost,
			"wherry-receiver.p1.r1":    roleReceiver,
			"wherry-receiver.p1.r2":    roleReceiver,
			"wherry-receiver.p2.r3":    roleReceiver,
			"wherry-custodian.p1":      roleCustodian,
			"wherry-custodian.p2":      roleCustodian,
		},
		"b": {
			"wherry-host.ben.mini":     roleHost,
			"wherry-host.alice.laptop": roleHost,
			"wherry-pool.p1":           roleReceiver,
			"wherry-pool.p2":           roleReceiver,
			"wherry-custodian.p1":      roleCustodian,
			"wherry-custodian.p2":      roleCustodian,
		},
	}
	variants(t, func(t *testing.T, variant string) {
		spans, _, _ := emitted(t, config{variant: variant, runs: 5})
		got := map[string]string{}
		instances := map[string][]string{}
		for _, s := range spans {
			name := serviceName(s)
			ns, _ := attr(s.Resource.Attributes(), semconv.ServiceNamespaceKey)
			got[name] = ns.AsString()
			instance, hasInstance := attr(s.Resource.Attributes(), semconv.ServiceInstanceIDKey)
			switch {
			case variant == "a" && hasInstance:
				t.Errorf("variant a: %s carries service.instance.id %s", name, instance.AsString())
			case variant == "b" && want["b"][name] == roleReceiver && !hasInstance:
				t.Errorf("variant b: %s carries no service.instance.id", name)
			case hasInstance && !slices.Contains(instances[name], instance.AsString()):
				instances[name] = append(instances[name], instance.AsString())
			}
		}
		for name, role := range want[variant] {
			if got[name] != role {
				t.Errorf("service %s has namespace %q, want %q", name, got[name], role)
			}
		}
		for name := range got {
			if _, ok := want[variant][name]; !ok {
				t.Errorf("unexpected service %s", name)
			}
		}
		if variant == "b" {
			slices.Sort(instances["wherry-pool.p1"])
			if !slices.Equal(instances["wherry-pool.p1"], []string{"r1", "r2"}) {
				t.Errorf("wherry-pool.p1 instances %v, want [r1 r2]", instances["wherry-pool.p1"])
			}
			if !slices.Equal(instances["wherry-pool.p2"], []string{"r3"}) {
				t.Errorf("wherry-pool.p2 instances %v, want [r3]", instances["wherry-pool.p2"])
			}
		}
	})
}

// The redelivered run has two CONSUMER attempts under one send span: the first released, the second processed.
func TestRedeliveredRunHasTwoAttempts(t *testing.T) {
	spans, s, _ := emitted(t, config{variant: "a", runs: 10})
	ids := byID(spans)
	children := map[trace.SpanID][]tracetest.SpanStub{}
	for _, sp := range spans {
		children[sp.Parent.SpanID()] = append(children[sp.Parent.SpanID()], sp)
	}
	redelivered := 0
	for _, sp := range spans {
		if sp.SpanKind != trace.SpanKindConsumer {
			continue
		}
		a, ok := attr(sp.Attributes, attrAttempt)
		if !ok || a.AsInt64() != 2 {
			continue
		}
		redelivered++
		send := ids[sp.Parent.SpanID()]
		siblings := children[send.SpanContext.SpanID()]
		if len(siblings) != 2 {
			t.Errorf("send %s has %d children, want two attempts", send.SpanContext.SpanID(), len(siblings))
			continue
		}
		var attempts []int64
		for _, sib := range siblings {
			if sib.SpanKind != trace.SpanKindConsumer {
				t.Errorf("attempt %s is %s, not CONSUMER", sib.Name, sib.SpanKind)
			}
			a, _ := attr(sib.Attributes, attrAttempt)
			attempts = append(attempts, a.AsInt64())
			switch a.AsInt64() {
			case 1:
				if sib.Status.Code != codes.Error || sib.Status.Description != classReleased {
					t.Errorf("first attempt status %v, want Error %s", sib.Status, classReleased)
				}
				if len(children[sib.SpanContext.SpanID()]) != 0 {
					t.Errorf("released attempt has children")
				}
			case 2:
				if sib.Status.Code != codes.Unset {
					t.Errorf("second attempt status %v, want Unset", sib.Status)
				}
				if len(children[sib.SpanContext.SpanID()]) != 1 {
					t.Errorf("second attempt has %d children, want the run", len(children[sib.SpanContext.SpanID()]))
				}
			}
		}
		slices.Sort(attempts)
		if !slices.Equal(attempts, []int64{1, 2}) {
			t.Errorf("attempts %v, want [1 2]", attempts)
		}
		if sp.SpanContext.TraceID() == s.redeliveredTrace && stringAttr(t, sp, attrPool) != "p1" {
			t.Errorf("summary names the redelivered trace of pool %s, want the first pool", stringAttr(t, sp, attrPool))
		}
	}
	if redelivered != 2 {
		t.Errorf("%d redelivered runs, want one per pool", redelivered)
	}
	if !s.redeliveredTrace.IsValid() {
		t.Error("summary has no redelivered trace id")
	}
}

// No span records an exception or a status description outside the fixed classes, and one run in five fails.
func TestStatusesAreFixedClasses(t *testing.T) {
	spans, s, _ := emitted(t, config{variant: "b", runs: 10})
	failedRuns, failedTerminals := 0, 0
	failedTraceSeen := false
	for _, sp := range spans {
		for _, ev := range sp.Events {
			if ev.Name == semconv.ExceptionEventName {
				t.Errorf("span %s records an exception", sp.Name)
			}
			t.Errorf("span %s has event %s; the fixture emits none", sp.Name, ev.Name)
		}
		switch sp.Status.Code {
		case codes.Unset, codes.Ok:
			if sp.Status.Description != "" {
				t.Errorf("span %s has a description %q without an error", sp.Name, sp.Status.Description)
			}
		case codes.Error:
			switch sp.Status.Description {
			case classTimeout:
				switch {
				case sp.Name == "run":
					failedRuns++
					if sp.SpanContext.TraceID() == s.failedTrace {
						failedTraceSeen = true
					}
				case sp.SpanKind == trace.SpanKindProducer && strings.HasPrefix(stringAttr(t, sp, semconv.MessagingDestinationNameKey), "wherry.host."):
					failedTerminals++
				default:
					t.Errorf("span %s (%s) carries %s", sp.Name, sp.SpanKind, classTimeout)
				}
			case classReleased:
				if sp.SpanKind != trace.SpanKindConsumer {
					t.Errorf("span %s (%s) carries %s", sp.Name, sp.SpanKind, classReleased)
				}
			default:
				t.Errorf("span %s has status description %q outside the fixed classes", sp.Name, sp.Status.Description)
			}
		}
	}
	if want := 2 * 3 * 10 / 5; failedRuns != want || failedTerminals != want {
		t.Errorf("%d failed runs and %d failed terminals, want %d each", failedRuns, failedTerminals, want)
	}
	if !failedTraceSeen {
		t.Errorf("summary's failed trace %s has no failed run span", s.failedTrace)
	}
}

// Every span carries the messaging attributes and the span name the brief lists, and only allowlisted keys.
func TestMessagingAttributes(t *testing.T) {
	allowed := map[attribute.Key]bool{
		attrSystemName: true, semconv.MessagingSystemKey: true, semconv.MessagingOperationTypeKey: true,
		semconv.MessagingOperationNameKey: true, semconv.MessagingDestinationNameKey: true,
		semconv.MessagingKafkaOffsetKey: true, semconv.MessagingConsumerGroupNameKey: true,
		attrRun: true, attrAttempt: true, attrTenant: true, attrHost: true, attrPool: true, attrReceiver: true,
		attrProfile: true, attrAgent: true,
	}
	spans, _, _ := emitted(t, config{variant: "a", runs: 5})
	for _, sp := range spans {
		for _, kv := range sp.Attributes {
			if !allowed[kv.Key] {
				t.Errorf("span %s carries %s, outside the allowlist", sp.Name, kv.Key)
			}
		}
		if sp.SpanKind != trace.SpanKindProducer && sp.SpanKind != trace.SpanKindConsumer {
			if _, ok := attr(sp.Attributes, semconv.MessagingDestinationNameKey); ok {
				t.Errorf("%s span %s carries messaging attributes", sp.SpanKind, sp.Name)
			}
			continue
		}
		if got := stringAttr(t, sp, attrSystemName); got != "kafka" {
			t.Errorf("span %s messaging.system.name %q", sp.Name, got)
		}
		op := stringAttr(t, sp, semconv.MessagingOperationNameKey)
		typ := stringAttr(t, sp, semconv.MessagingOperationTypeKey)
		dest := stringAttr(t, sp, semconv.MessagingDestinationNameKey)
		if sp.Name != op+" "+dest {
			t.Errorf("span name %q, want %q", sp.Name, op+" "+dest)
		}
		if !strings.HasPrefix(dest, "wherry.") {
			t.Errorf("span %s destination %q is not a wherry topic", sp.Name, dest)
		}
		want := map[trace.SpanKind][2]string{trace.SpanKindProducer: {"send", "send"}, trace.SpanKindConsumer: {"process", "process"}}[sp.SpanKind]
		if op != want[0] || typ != want[1] {
			t.Errorf("%s span %s has operation %s/%s", sp.SpanKind, sp.Name, op, typ)
		}
	}
}

// Consumer-side spans are exported in a later batch than their producers, at least the delay apart.
func TestConsumersExportedAfterDelay(t *testing.T) {
	const delay = 300 * time.Millisecond
	spans, _, exporter := emitted(t, config{variant: "a", runs: 2, delay: delay})
	ids := byID(spans)
	var lastProducer, firstConsumer time.Time
	for _, sp := range spans {
		at := exporter.exportedAt(sp.SpanContext.SpanID())
		if at.IsZero() {
			t.Fatalf("span %s has no export time", sp.Name)
		}
		consumerSide := sp.SpanKind == trace.SpanKindConsumer || sp.Name == "run"
		if consumerSide {
			if firstConsumer.IsZero() || at.Before(firstConsumer) {
				firstConsumer = at
			}
		} else if at.After(lastProducer) {
			lastProducer = at
		}
		if sp.SpanKind != trace.SpanKindConsumer {
			continue
		}
		producerAt := exporter.exportedAt(ids[sp.Parent.SpanID()].SpanContext.SpanID())
		if gap := at.Sub(producerAt); gap < delay {
			t.Errorf("consumer %s exported %s after its producer, want at least %s", sp.Name, gap, delay)
		}
	}
	if gap := firstConsumer.Sub(lastProducer); gap < delay {
		t.Errorf("first consumer batch %s after the last producer batch, want at least %s", gap, delay)
	}
}

func TestParseFlags(t *testing.T) {
	cfg, err := parseFlags(nil, &strings.Builder{})
	if err != nil || cfg.variant != "a" || cfg.runs != 10 || cfg.delay != 60*time.Second {
		t.Errorf("defaults: %+v, %v", cfg, err)
	}
	cfg, err = parseFlags([]string{"-variant", "b", "-runs", "3", "-delay", "0"}, &strings.Builder{})
	if err != nil || cfg.variant != "b" || cfg.runs != 3 || cfg.delay != 0 {
		t.Errorf("explicit: %+v, %v", cfg, err)
	}
	for _, args := range [][]string{{"-variant", "c"}, {"-runs", "0"}, {"-delay", "-1s"}, {"extra"}} {
		if _, err := parseFlags(args, &strings.Builder{}); err == nil {
			t.Errorf("%v: accepted", args)
		}
	}
}
