package receiver

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
	"github.com/benemon/wherry/internal/telemetry/teletest"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// hostSend stands for the host's send span: a span context and its traceparent header, as the task record carries it.
func hostSend(t *testing.T) (trace.SpanContext, kgo.RecordHeader) {
	t.Helper()
	var traceID trace.TraceID
	var spanID trace.SpanID
	rand.Read(traceID[:])
	rand.Read(spanID[:])
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled, Remote: true})
	return sc, kgo.RecordHeader{Key: "traceparent", Value: []byte("00-" + traceID.String() + "-" + spanID.String() + "-01")}
}

// submitTraced is submit with headers on the task record.
func (f fixture) submitTraced(t *testing.T, headers []kgo.RecordHeader, mutate func(*wire.Submission)) *kgo.Record {
	t.Helper()
	submission := testSubmission("run-a")
	if mutate != nil {
		mutate(submission)
	}
	if submission.Wherry.Envelope == "" {
		f.seal(t, submission)
	}
	record := &kgo.Record{Topic: f.taskTopic(t, submission.Wherry.Placement), Key: []byte(submission.Wherry.Run), Value: must(t)(wire.SignRecord(submission, f.host)), Headers: headers}
	if err := f.service.Operator.ProduceSync(f.ctx, record).FirstErr(); err != nil {
		t.Fatal(err)
	}
	return record
}

// spans flushes the receiver's providers and returns every span exported so far.
func (f fixture) spans(t *testing.T, exporter *teletest.Exporter) []tracetest.SpanStub {
	t.Helper()
	for _, tracing := range []*telemetry.Tracing{f.receiver.tracing, f.receiver.custodian} {
		if err := tracing.Flush(f.ctx); err != nil {
			t.Fatal(err)
		}
	}
	return exporter.Spans()
}

// spansWhen is spans once done holds of them: a record on the feed lands before the span that produced it ends.
func (f fixture) spansWhen(t *testing.T, exporter *teletest.Exporter, done func([]tracetest.SpanStub) bool) []tracetest.SpanStub {
	t.Helper()
	for {
		spans := f.spans(t, exporter)
		if done(spans) {
			return spans
		}
		if f.ctx.Err() != nil {
			t.Fatalf("spans never settled; %d exported; logs:\n%s", len(spans), f.logs.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// ended reports that every span named name has ended n times over.
func ended(name string, n int) func([]tracetest.SpanStub) bool {
	return func(spans []tracetest.SpanStub) bool { return len(teletest.Named(spans, name)) >= n }
}

// awaitRejected waits for the run's rejected status on the feed.
func (f fixture) awaitRejected(t *testing.T, run string) wire.RunStatus {
	t.Helper()
	for {
		for _, status := range f.feedStatuses(t) {
			if status.Wherry.Run == run && status.Wherry.Kind == "rejected" {
				return status
			}
		}
		if f.ctx.Err() != nil {
			t.Fatalf("no rejection of %s; logs:\n%s", run, f.logs.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func parentOf(t *testing.T, ids map[trace.SpanID]tracetest.SpanStub, s tracetest.SpanStub) tracetest.SpanStub {
	t.Helper()
	parent, ok := ids[s.Parent.SpanID()]
	if !ok {
		t.Fatalf("span %s (%s): parent %s was not exported", s.Name, s.SpanKind, s.Parent.SpanID())
	}
	return parent
}

func namespace(s tracetest.SpanStub) string { return teletest.Resource(s, semconv.ServiceNamespaceKey) }

// Brief test 1, receiver side: every attempt's CONSUMER span is a child of the host's send span, a redelivery gives two
// sibling attempts under it, the run is the attempt's INTERNAL child through the production executeDelivery path, and
// the open exchange is traced in both directions. Brief test 3, receiver side: the delivered credential, the envelope,
// the redaction key, the prompt, a presigned URL and the incoming tracestate reach no span.
func TestTracedAttemptsAreSiblingsUnderTheHostSend(t *testing.T) {
	exporter := teletest.Install(t)
	s3 := newStore(t)
	f := newFixtureWith(t, resultStream, s3Terms(s3.endpoint()), testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream}})
	// The first attempt's job never starts and its start retries run out, so the task goes back and is redelivered.
	f.plugin.failStart = func(string) bool { return f.plugin.lastRun.Attempt == 1 }
	ctx, stop := context.WithCancel(f.ctx)
	defer stop()
	f.run(t, ctx)
	send, traceparent := hostSend(t)
	const prompt, tracestate = "s3cr3t-prompt-text", "vendor=s3cr3t-tracestate"
	record := f.submitTraced(t, []kgo.RecordHeader{traceparent, {Key: "tracestate", Value: []byte(tracestate)}}, func(task *wire.Submission) {
		task.Message.Parts = []wire.Part{{Text: prompt}}
		task.Wherry.S3Inputs = []wire.S3Input{{Name: "dataset.bin", Size: 13, SHA256: strings.Repeat("ab", 32)}}
	})
	task := f.await(t, testRun("run-a"), anyTask)
	if task.Status.State != wire.TaskStateCompleted {
		t.Fatalf("task %+v; logs:\n%s", task.Status, f.logs.String())
	}
	spans := f.spansWhen(t, exporter, ended("run", 2))
	ids := teletest.ByID(spans)

	processes := teletest.Named(spans, "process "+record.Topic+" nomad")
	if len(processes) != 2 {
		t.Fatalf("process spans %d, want two attempts", len(processes))
	}
	slices.SortFunc(processes, func(a, b tracetest.SpanStub) int {
		return strings.Compare(teletest.Attr(a, telemetry.AttrAttempt), teletest.Attr(b, telemetry.AttrAttempt))
	})
	for i, process := range processes {
		if process.SpanKind != trace.SpanKindConsumer || process.Parent.SpanID() != send.SpanID() || process.SpanContext.TraceID() != send.TraceID() || len(process.Links) != 0 {
			t.Fatalf("attempt %d: kind %s, parent %s in trace %s, %d links; want a CONSUMER child of the send span %s in trace %s", i+1, process.SpanKind, process.Parent.SpanID(), process.SpanContext.TraceID(), len(process.Links), send.SpanID(), send.TraceID())
		}
		if got := teletest.Attr(process, telemetry.AttrAttempt); got != string(rune('0'+i+1)) {
			t.Fatalf("attempt %d has wherry.attempt %q", i+1, got)
		}
		if process.SpanContext.TraceState().Len() != 0 {
			t.Fatalf("attempt %d carries the incoming tracestate %q", i+1, process.SpanContext.TraceState().String())
		}
		if teletest.Attr(process, telemetry.AttrRun) != testRun("run-a") || teletest.Attr(process, telemetry.AttrProfile) != "nomad" || teletest.Attr(process, semconv.MessagingConsumerGroupNameKey) != operator.PoolGroup(testPool) {
			t.Fatalf("attempt %d attributes %v", i+1, process.Attributes)
		}
		if namespace(process) != telemetry.RoleReceiver || teletest.Resource(process, semconv.ServiceNameKey) != "wherry-receiver."+testPool+"."+testReceiver {
			t.Fatalf("attempt %d resource %v", i+1, process.Resource.Attributes())
		}
	}
	runs := teletest.Named(spans, "run")
	if len(runs) != 2 {
		t.Fatalf("run spans %d, want one per attempt", len(runs))
	}
	slices.SortFunc(runs, func(a, b tracetest.SpanStub) int {
		return strings.Compare(teletest.Attr(a, telemetry.AttrAttempt), teletest.Attr(b, telemetry.AttrAttempt))
	})
	for i, run := range runs {
		if run.SpanKind != trace.SpanKindInternal || run.Parent.SpanID() != processes[i].SpanContext.SpanID() {
			t.Fatalf("run of attempt %d: kind %s, parent %s; want an INTERNAL child of its attempt", i+1, run.SpanKind, run.Parent.SpanID())
		}
	}
	if runs[0].Status.Code != codes.Error || runs[0].Status.Description != telemetry.ClassReleased {
		t.Fatalf("first attempt's run status %+v, want released", runs[0].Status)
	}
	if runs[1].Status.Code != codes.Unset {
		t.Fatalf("second attempt's run status %+v, want none", runs[1].Status)
	}
	if retries := teletest.Attr(runs[0], telemetry.AttrRetry); retries == "" || retries == "0" {
		t.Fatalf("first attempt's run has wherry.retry %q, want its in-place retries counted", retries)
	}

	// The open exchange: request PRODUCER under the run, custodian CONSUMER under it, wrap PRODUCER under that, and the
	// receiver's CONSUMER of the wrap under the wrap. Each in-place start retry of the first attempt opens again, so
	// the exchange repeats under the first attempt's run; the second attempt opens once.
	events := operator.PoolEventsTopic(testPool)
	var requests, wraps []tracetest.SpanStub
	for _, s := range teletest.Named(spans, "send "+events) {
		if namespace(s) == telemetry.RoleReceiver {
			requests = append(requests, s)
		} else {
			wraps = append(wraps, s)
		}
	}
	if len(requests) < 2 || len(wraps) != len(requests) {
		t.Fatalf("open requests %d and wraps %d, want one wrap per request and at least one request per attempt", len(requests), len(wraps))
	}
	second := 0
	for _, request := range requests {
		if request.SpanKind != trace.SpanKindProducer || parentOf(t, ids, request).Name != "run" {
			t.Fatalf("open request %s %s has parent %s", request.Name, request.SpanKind, parentOf(t, ids, request).Name)
		}
		if request.Parent.SpanID() == runs[1].SpanContext.SpanID() {
			second++
		}
	}
	if second != 1 {
		t.Fatalf("open requests under the second attempt %d, want one", second)
	}
	custodian, receiver := 0, 0
	for _, s := range teletest.Named(spans, "process "+events) {
		parent := parentOf(t, ids, s)
		switch namespace(s) {
		case telemetry.RoleCustodian:
			custodian++
			if s.SpanKind != trace.SpanKindConsumer || namespace(parent) != telemetry.RoleReceiver || parent.SpanKind != trace.SpanKindProducer || teletest.Resource(s, semconv.ServiceNameKey) != "wherry-custodian."+testPool {
				t.Fatalf("custodian process span: kind %s, parent %s of %s", s.SpanKind, parent.Name, namespace(parent))
			}
		case telemetry.RoleReceiver:
			receiver++
			if s.SpanKind != trace.SpanKindConsumer || namespace(parent) != telemetry.RoleCustodian || parent.SpanKind != trace.SpanKindProducer || parentOf(t, ids, parent).SpanKind != trace.SpanKindConsumer {
				t.Fatalf("receiver's wrap span: kind %s, parent %s of %s", s.SpanKind, parent.Name, namespace(parent))
			}
		}
	}
	if custodian != len(requests) || receiver != len(requests) {
		t.Fatalf("custodian consumers %d, receiver wrap consumers %d; want %d each", custodian, receiver, len(requests))
	}
	terminals := teletest.Named(spans, "send "+operator.TenantTopic(testTenant))
	if len(terminals) != 1 || terminals[0].SpanKind != trace.SpanKindProducer || parentOf(t, ids, terminals[0]).SpanContext.SpanID() != runs[1].SpanContext.SpanID() {
		t.Fatalf("terminal sends %d, want the second attempt's alone under its run", len(terminals))
	}
	for _, name := range []string{"plugin Dispatch", "plugin Lookup", "plugin Follow", "plugin Stop", "deliver credentials", "await start", "find job", "return task", "admit"} {
		if len(teletest.Named(spans, name)) == 0 {
			t.Errorf("no %s span", name)
		}
	}
	for _, record := range f.records(t, operator.TenantTopic(testTenant)) {
		if recordType(record.Value) == wire.ResultType && len(record.Headers) != 1 {
			t.Fatalf("result record headers %v, want the terminal send's traceparent alone", record.Headers)
		}
	}
	teletest.Check(t, spans, rejectionLabels...)
	parsed := f.parse(t, record)
	teletest.Sentinels(t, spans, testToken, string(testRedactionKey), parsed.Wherry.Envelope, parsed.Wherry.Wrap, prompt, tracestate, "s3cr3t", s3Signature, "X-Amz-Signature", s3.endpoint())
}

// Brief test 2, receiver side: a rejection's spans carry the rejectAs label, never its reason, which quotes the task's
// bound.
func TestTracedRejectionCarriesTheLabel(t *testing.T) {
	exporter := teletest.Install(t)
	f := newFixture(t, resultStream)
	ctx, stop := context.WithCancel(f.ctx)
	defer stop()
	f.run(t, ctx)
	send, traceparent := hostSend(t)
	record := f.submitTraced(t, []kgo.RecordHeader{traceparent}, func(task *wire.Submission) {
		task.Wherry.TaskStartedBy = time.Now().Add(-time.Minute).Unix()
	})
	status := f.awaitRejected(t, testRun("run-a"))
	spans := f.spansWhen(t, exporter, ended("process "+record.Topic, 1))
	processes := teletest.Named(spans, "process "+record.Topic)
	if len(processes) != 1 || processes[0].Parent.SpanID() != send.SpanID() {
		t.Fatalf("process spans %d, want one under the host's send", len(processes))
	}
	if processes[0].Status.Code != codes.Error || processes[0].Status.Description != rejectionReasonStartBound {
		t.Fatalf("process status %+v, want the label %q", processes[0].Status, rejectionReasonStartBound)
	}
	if admits := teletest.Named(spans, "admit"); len(admits) != 1 || admits[0].Status.Description != rejectionReasonStartBound {
		t.Fatalf("admit spans %v", admits)
	}
	if sends := teletest.Named(spans, "send "+operator.TenantTopic(testTenant)); len(sends) != 1 || sends[0].Status.Description != telemetry.ClassRejected {
		t.Fatalf("rejection sends %v", sends)
	}
	if len(teletest.Named(spans, "run")) != 0 {
		t.Fatal("a rejected task has a run span")
	}
	teletest.Check(t, spans, rejectionLabels...)
	teletest.Sentinels(t, spans, status.StatusUpdate.Status.Message.Parts[0].Text, wire.RFC3339(time.Now().Add(-time.Minute).Unix())[:13])
}

// Variant B names the pool as the receiver's service, with the receiver as its instance; the custodian keeps its own.
func TestTracedVariantBNamesThePool(t *testing.T) {
	exporter := teletest.Install(t)
	t.Setenv(telemetry.NamingEnv, "b")
	f := newBareFixture(t)
	t.Cleanup(f.receiver.shutdownTracing)
	_, receiver := f.receiver.tracing.Root(f.ctx, "receiver start", trace.SpanKindInternal)
	receiver.End()
	_, custodian := f.receiver.custodian.Root(f.ctx, "custodian rotation", trace.SpanKindInternal)
	custodian.End()
	spans := f.spans(t, exporter)
	for _, s := range spans {
		switch s.Name {
		case "receiver start":
			if teletest.Resource(s, semconv.ServiceNameKey) != "wherry-pool."+testPool || teletest.Resource(s, semconv.ServiceInstanceIDKey) != testReceiver || namespace(s) != telemetry.RoleReceiver {
				t.Fatalf("receiver resource %v", s.Resource.Attributes())
			}
		case "custodian rotation":
			if teletest.Resource(s, semconv.ServiceNameKey) != "wherry-custodian."+testPool || namespace(s) != telemetry.RoleCustodian {
				t.Fatalf("custodian resource %v", s.Resource.Attributes())
			}
		}
	}
	if len(spans) != 2 {
		t.Fatalf("spans %d, want two", len(spans))
	}
	teletest.Check(t, spans)
}

// Brief test 4, receiver side: with no endpoint set the global provider is the no-op one, nothing is exported, and no
// record the receiver produces carries a header, whatever the incoming record carries.
func TestTracingIsOffByDefault(t *testing.T) {
	exporter := teletest.Hook(t)
	t.Setenv(telemetry.EndpointEnv, "")
	t.Setenv(telemetry.TracesEndpointEnv, "")
	f := newFixture(t, resultStream)
	ctx, stop := context.WithCancel(f.ctx)
	defer stop()
	f.run(t, ctx)
	_, traceparent := hostSend(t)
	record := f.submitTraced(t, []kgo.RecordHeader{traceparent}, nil)
	if task := f.await(t, testRun("run-a"), anyTask); task.Status.State != wire.TaskStateCompleted {
		t.Fatalf("task %+v", task.Status)
	}
	if f.receiver.tracing.Enabled() || f.receiver.custodian.Enabled() {
		t.Fatal("tracing is on without an endpoint")
	}
	if _, sdk := otel.GetTracerProvider().(*sdktrace.TracerProvider); sdk {
		t.Fatal("the global tracer provider is the SDK's without an endpoint")
	}
	if spans := exporter.Spans(); len(spans) != 0 {
		t.Fatalf("%d spans exported without an endpoint", len(spans))
	}
	for _, topic := range []string{operator.PoolEventsTopic(testPool), operator.TenantTopic(testTenant), operator.TenantTranscriptTopic(testTenant), record.Topic} {
		for _, produced := range f.records(t, topic) {
			if produced.Topic == record.Topic && produced.Offset == record.Offset {
				continue
			}
			if len(produced.Headers) != 0 {
				t.Fatalf("record %d on %s carries headers %v", produced.Offset, topic, produced.Headers)
			}
		}
	}
}

// Brief test 5: a malformed or oversized traceparent is ignored, the attempt's span starts a trace of its own, and
// the run proceeds.
func TestTracedHeaderBound(t *testing.T) {
	exporter := teletest.Install(t)
	f := newFixture(t, resultStream)
	ctx, stop := context.WithCancel(f.ctx)
	defer stop()
	f.run(t, ctx)
	id := make([]byte, 24)
	rand.Read(id)
	// Version 01 with trailing data is valid trace context to the W3C propagator, whatever its length.
	oversized := "01-" + hex.EncodeToString(id[:16]) + "-" + hex.EncodeToString(id[16:]) + "-01-" + strings.Repeat("x", 200)
	headers := map[string]string{"run-a": "not-a-traceparent", "run-b": oversized}
	var topic string
	for _, name := range []string{"run-a", "run-b"} {
		record := f.submitTraced(t, []kgo.RecordHeader{{Key: "traceparent", Value: []byte(headers[name])}}, func(task *wire.Submission) { named(task, name) })
		topic = record.Topic
		if task := f.await(t, testRun(name), anyTask); task.Status.State != wire.TaskStateCompleted {
			t.Fatalf("%s: task %+v; logs:\n%s", name, task.Status, f.logs.String())
		}
	}
	spans := f.spansWhen(t, exporter, ended("process "+topic, 2))
	processes := teletest.Named(spans, "process "+topic)
	if len(processes) != 2 {
		t.Fatalf("process spans %d, want one per run", len(processes))
	}
	for _, process := range processes {
		if process.Parent.IsValid() {
			t.Fatalf("attempt of %s has parent %s from header %q", teletest.Attr(process, telemetry.AttrRun), process.Parent.SpanID(), headers)
		}
		if process.SpanContext.TraceID().String() == hex.EncodeToString(id[:16]) {
			t.Fatal("the oversized header's trace id was taken")
		}
	}
	teletest.Check(t, spans, rejectionLabels...)
}
