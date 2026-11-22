package receiver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/telemetry"
	"github.com/benemon/wherry/internal/telemetry/teletest"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
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

// tenanted fails the test for a span in the run's trace, traceID, without wherry.tenant set to tenant, whichever role
// started it, and for a span outside the run carrying one; receiver start and custodian rotation must be among the
// latter.
func tenanted(t *testing.T, spans []tracetest.SpanStub, traceID trace.TraceID, tenant string) {
	t.Helper()
	outside := map[string]bool{}
	for _, s := range spans {
		got := teletest.Attr(s, telemetry.AttrTenant)
		switch {
		case s.SpanContext.TraceID() == traceID && got != tenant:
			t.Errorf("span %s (%s %s) in the run's trace carries wherry.tenant %q, want %q", s.Name, namespace(s), s.SpanKind, got, tenant)
		case s.SpanContext.TraceID() != traceID && got != "":
			t.Errorf("span %s (%s %s) outside the run carries wherry.tenant %q", s.Name, namespace(s), s.SpanKind, got)
		case s.SpanContext.TraceID() != traceID:
			outside[s.Name] = true
		}
	}
	for _, name := range []string{"receiver start", "custodian rotation"} {
		if !outside[name] {
			t.Errorf("no %s span outside the run's trace", name)
		}
	}
}

// headerSpan is the span ID of record's one traceparent header.
func headerSpan(t *testing.T, record *kgo.Record) trace.SpanID {
	t.Helper()
	if len(record.Headers) != 1 || record.Headers[0].Key != "traceparent" {
		t.Fatalf("record %d on %s carries headers %v, want a traceparent alone", record.Offset, record.Topic, record.Headers)
	}
	return telemetry.ParseTraceParent(string(record.Headers[0].Value)).SpanID()
}

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
	// The terminal records: the result and the feed's terminal status each go out under a PRODUCER span of their own,
	// both under the second attempt's run, and each record carries its own span's context and no other.
	terminals := teletest.Named(spans, "send "+operator.TenantTopic(testTenant))
	if len(terminals) != 2 {
		t.Fatalf("terminal sends %d, want one for the result and one for the terminal status", len(terminals))
	}
	for _, terminal := range terminals {
		if terminal.SpanKind != trace.SpanKindProducer || parentOf(t, ids, terminal).SpanContext.SpanID() != runs[1].SpanContext.SpanID() {
			t.Fatalf("terminal send %s has parent %s, want the second attempt's run", terminal.SpanContext.SpanID(), terminal.Parent.SpanID())
		}
	}
	carried := map[string]trace.SpanID{}
	for _, record := range f.records(t, operator.TenantTopic(testTenant)) {
		var status wire.RunStatus
		switch typ := recordType(record.Value); {
		case typ == wire.ResultType:
			carried["result"] = headerSpan(t, record)
		case typ == wire.StatusType && wire.ParseRecord(record.Value, f.keys(), &status) == nil && status.Wherry.Kind == "terminal":
			carried["terminal"] = headerSpan(t, record)
		case len(record.Headers) != 0:
			t.Fatalf("record %d of type %s carries headers %v", record.Offset, typ, record.Headers)
		}
	}
	paired := carried["result"] == terminals[0].SpanContext.SpanID() && carried["terminal"] == terminals[1].SpanContext.SpanID() ||
		carried["result"] == terminals[1].SpanContext.SpanID() && carried["terminal"] == terminals[0].SpanContext.SpanID()
	if !paired {
		t.Fatalf("result carries %s and terminal status %s; want one terminal send each", carried["result"], carried["terminal"])
	}
	for _, name := range []string{"plugin Dispatch", "plugin Lookup", "plugin Follow", "plugin Stop", "deliver credentials", "await start", "find job", "return task", "admit", "released"} {
		if len(teletest.Named(spans, name)) == 0 {
			t.Errorf("no %s span", name)
		}
	}
	// Every span of the run, the receiver's and the custodian's, carries the run's tenant, for the collector to route
	// the whole trace to the tenant's topic; the spans outside a run carry none.
	tenanted(t, spans, send.TraceID(), testTenant)
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
	// A rejection's spans, the attempt's, the admit and the rejection's PRODUCER, carry the tenant the task binds.
	tenanted(t, spans, send.TraceID(), testTenant)
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
		if tenant := teletest.Attr(s, telemetry.AttrTenant); tenant != "" {
			t.Fatalf("%s, outside a run, carries wherry.tenant %q", s.Name, tenant)
		}
	}
	if len(spans) != 2 {
		t.Fatalf("spans %d, want two", len(spans))
	}
	teletest.Check(t, spans)
}

// Brief test 4, receiver side: with no endpoint set the global provider is the no-op one, nothing is exported, and no
// record the receiver produces carries a header, whatever the incoming record carries. The traces-only endpoint
// variable does not turn tracing on (rule 3).
func TestTracingIsOffByDefault(t *testing.T) {
	exporter := teletest.Hook(t)
	t.Setenv(telemetry.EndpointEnv, "")
	t.Setenv(telemetry.TracesEndpointEnv, "http://127.0.0.1:1")
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

// A receiver whose second provider fails to build shuts the first down before configure returns: the global provider
// is the no-op one again, within shutdownTimeout, and nothing stays installed with no owner.
func TestTracingShutsTheFirstProviderDownWhenTheSecondFails(t *testing.T) {
	exporter := teletest.Install(t)
	calls := 0
	telemetry.NewExporter = func(context.Context) (sdktrace.SpanExporter, error) {
		if calls++; calls == 2 {
			return nil, errors.New("exporter refused")
		}
		return exporter, nil
	}
	logs := &strings.Builder{}
	r := &Receiver{id: testReceiver, logger: log.New(logs, "", 0)}
	began := time.Now()
	err := r.trace(context.Background(), testPool)
	if err == nil || !strings.Contains(err.Error(), "exporter refused") {
		t.Fatalf("trace = %v, want the second constructor's error", err)
	}
	if took := time.Since(began); took > shutdownTimeout {
		t.Fatalf("configure took %s, over the %s bound", took, shutdownTimeout)
	}
	if r.tracing != nil || r.custodian != nil {
		t.Fatalf("providers kept: %v, %v", r.tracing, r.custodian)
	}
	if _, sdk := otel.GetTracerProvider().(*sdktrace.TracerProvider); sdk {
		t.Fatal("the global tracer provider is still the SDK's after the first provider's shutdown")
	}
	_, span := otel.Tracer("wherry").Start(context.Background(), "orphan")
	span.End()
	if spans := exporter.Spans(); len(spans) != 0 {
		t.Fatalf("%d spans exported through the orphaned provider", len(spans))
	}
}

// Brief test 3, plugin RPCs: a plugin failure whose Op, Class and Message carry sentinels, through the real Dispatch,
// Follow and Stop failure paths, gives each CLIENT span the fixed plugin class and nothing of the failure. Dispatch
// fails delivering the credential, naming its value, as a plugin that cannot write it would.
func TestTracedPluginFailuresCarryAClass(t *testing.T) {
	exporter := teletest.Install(t)
	setDuration(t, &startRetryWait, 200*time.Millisecond)
	f := newFixture(t, resultStream)
	const op, class, message = "s3cr3t-op", "s3cr3t-class", "s3cr3t-message"
	failure := func(detail string) error {
		return &pluginsdk.Error{Op: op + " " + detail, Class: class + " " + detail, Err: errors.New(message + " " + detail + ": " + testToken)}
	}
	f.plugin.dispatchErr = failure("deliver credential")
	f.plugin.purgeHook = func() { f.plugin.dispatchErr = nil }
	f.plugin.followErr, f.plugin.followedJob = failure("follow"), new(pluginsdk.JobAbsent)
	f.plugin.stopErr = failure("stop")
	ctx, stop := context.WithCancel(f.ctx)
	defer stop()
	f.run(t, ctx)
	f.submitTraced(t, nil, nil)
	task := f.await(t, testRun("run-a"), anyTask)
	if task.Status.State != wire.TaskStateFailed {
		t.Fatalf("task %+v; logs:\n%s", task.Status, f.logs.String())
	}
	spans := f.spansWhen(t, exporter, ended("run", 1))
	for _, name := range []string{"plugin Dispatch", "plugin Follow", "plugin Stop"} {
		failed := 0
		for _, s := range teletest.Named(spans, name) {
			if s.SpanKind != trace.SpanKindClient {
				t.Fatalf("%s span kind %s", name, s.SpanKind)
			}
			switch s.Status.Code {
			case codes.Error:
				if s.Status.Description != telemetry.ClassPlugin {
					t.Fatalf("%s status %+v, want the fixed class %q", name, s.Status, telemetry.ClassPlugin)
				}
				failed++
			case codes.Unset:
			default:
				t.Fatalf("%s status %+v", name, s.Status)
			}
		}
		if failed == 0 {
			t.Fatalf("no failed %s span among %d", name, len(teletest.Named(spans, name)))
		}
	}
	teletest.Check(t, spans, rejectionLabels...)
	teletest.Sentinels(t, spans, op, class, message, testToken, "s3cr3t")
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
