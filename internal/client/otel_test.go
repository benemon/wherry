package client

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/telemetry"
	"github.com/benemon/wherry/internal/telemetry/teletest"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// receiverSend stands for a receiver's send span: a span context and its traceparent header.
func receiverSend() (trace.SpanContext, kgo.RecordHeader) {
	var traceID trace.TraceID
	var spanID trace.SpanID
	rand.Read(traceID[:])
	rand.Read(spanID[:])
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled, Remote: true})
	return sc, kgo.RecordHeader{Key: "traceparent", Value: []byte("00-" + traceID.String() + "-" + spanID.String() + "-01")}
}

// feedTraced is feed with header on the result record, as a receiver's terminal send puts it there.
func (r *testReceiver) feedTraced(t *testing.T, ctx context.Context, runID string, traced kgo.RecordHeader, values ...[]byte) {
	t.Helper()
	producer, err := kgo.NewClient(append(r.kafka.Opts(), kgo.TransactionalID(operator.TransactionalPrefix(r.pool)+runID))...)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if err := producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	for _, value := range values {
		record := &kgo.Record{Topic: operator.TenantTopic("tenant-a"), Key: []byte(runID), Value: value}
		if typ, _ := header(value); typ == wire.ResultType {
			record.Headers = []kgo.RecordHeader{traced}
		}
		if err := producer.ProduceSync(ctx, record).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	if err := producer.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}
}

func (h *Host) spans(t *testing.T, exporter *teletest.Exporter) []tracetest.SpanStub {
	t.Helper()
	if err := h.tracing.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	return exporter.Spans()
}

func taskRecord(t *testing.T, f *fixture, kafka *kgo.Client, pool, runID string) *kgo.Record {
	t.Helper()
	records, err := readTasks(f.ctx, kafka, pool)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if string(record.Key) == runID {
			return record
		}
	}
	t.Fatalf("no task record for %s", runID)
	return nil
}

// Brief test 1, host side: submit is the root, its send PRODUCER puts the trace context on the task record alone, the
// result's CONSUMER is a child of the record's own header and emitted once per record, and a cancel is its own trace
// linked to the submit. Brief test 3, host side: credential values, source paths and argv, the prompt, the envelope and
// the redaction key reach no span.
func TestTracedSubmitSendsTheHeaderAndReadsTheResult(t *testing.T) {
	exporter := teletest.Install(t)
	f := newFixture(t)
	t.Cleanup(func() { f.host.tracing.Close() })
	f.host.credentials["deploy-key"] = credential.Source{Command: []string{"cat", f.secret}}
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	const prompt = "s3cr3t-prompt-text"
	submitted, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: prompt, Credentials: []wire.TaskCredential{{Name: "deploy-key", File: "id_ed25519"}},
		EndBy: time.Now().Add(30 * time.Minute), StartBy: time.Now().Add(10 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	runID := submitted.RunID
	record := taskRecord(t, f, receiver.kafka, "pool-a", runID)
	spans := f.host.spans(t, exporter)
	ids := teletest.ByID(spans)
	submits := teletest.Named(spans, "submit")
	if len(submits) != 1 || submits[0].Parent.IsValid() || submits[0].SpanKind != trace.SpanKindInternal {
		t.Fatalf("submit spans %v, want one root", submits)
	}
	submit := submits[0]
	if teletest.Attr(submit, telemetry.AttrRun) != runID || teletest.Attr(submit, telemetry.AttrAgent) != "claude" || teletest.Attr(submit, telemetry.AttrPool) != "pool-a" ||
		teletest.Attr(submit, telemetry.AttrTenant) != "tenant-a" || teletest.Attr(submit, telemetry.AttrHost) != "host-a" {
		t.Fatalf("submit attributes %v", submit.Attributes)
	}
	if teletest.Resource(submit, semconv.ServiceNameKey) != "wherry-host.tenant-a.host-a" || teletest.Resource(submit, semconv.ServiceNamespaceKey) != telemetry.RoleHost {
		t.Fatalf("submit resource %v", submit.Resource.Attributes())
	}
	sends := teletest.Named(spans, "send "+record.Topic)
	if len(sends) != 1 || sends[0].SpanKind != trace.SpanKindProducer || sends[0].Parent.SpanID() != submit.SpanContext.SpanID() {
		t.Fatalf("send spans %v, want one PRODUCER under submit", sends)
	}
	if len(record.Headers) != 1 || record.Headers[0].Key != "traceparent" {
		t.Fatalf("task record headers %v, want traceparent alone", record.Headers)
	}
	if carried := telemetry.ParseTraceParent(string(record.Headers[0].Value)); carried.TraceID() != sends[0].SpanContext.TraceID() || carried.SpanID() != sends[0].SpanContext.SpanID() {
		t.Fatalf("task record carries %s, want the send span %s", record.Headers[0].Value, sends[0].SpanContext.SpanID())
	}
	for _, name := range []string{"read host key", "read state", "read advertisements", "read pool key"} {
		reads := teletest.Named(spans, name)
		if len(reads) != 1 || reads[0].SpanKind != trace.SpanKindClient || ids[reads[0].Parent.SpanID()].Name != "submit" {
			t.Errorf("%s spans %v, want one CLIENT under submit", name, reads)
		}
	}
	kinds := map[string]string{}
	for _, resolve := range teletest.Named(spans, "resolve credential") {
		kinds[teletest.Attr(resolve, telemetry.AttrCredential)] = teletest.Attr(resolve, telemetry.AttrCredentialSource)
	}
	if kinds["model"] != SourceFile || kinds["deploy-key"] != SourceCommand || len(kinds) != 2 {
		t.Errorf("resolve credential spans %v, want model from a file and deploy-key from a command", kinds)
	}
	if len(teletest.Named(spans, "seal envelope")) != 1 {
		t.Fatal("no seal envelope span")
	}
	local, err := f.host.localRun(runID)
	if err != nil || local == nil || telemetry.ParseTraceParent(local.TraceParent).TraceID() != submit.SpanContext.TraceID() {
		t.Fatalf("local record trace parent %q, %v; want the submit's", local.TraceParent, err)
	}

	task := receiver.claim(t, f.ctx, runID)[0]
	send, header := receiverSend()
	receiver.feedTraced(t, f.ctx, runID, header, receiver.echo(t, task), receiver.status(t, task, 1, "claimed-1", "claimed", wire.TaskStateWorking, nil),
		receiver.result(t, task, 1, wire.TaskStateCompleted, nil), receiver.terminal(t, task, 1, wire.TaskStateCompleted, 1))
	if awaited, err := f.host.Await(f.ctx, runID, 5*time.Second); err != nil || awaited.Status.State != wire.TaskStateCompleted {
		t.Fatalf("await %+v, %v", awaited, err)
	}
	if _, err := f.host.Status(f.ctx, runID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.host.Events(f.ctx, runID); err != nil {
		t.Fatal(err)
	}
	spans = f.host.spans(t, exporter)
	results := teletest.Named(spans, "process "+operator.TenantTopic("tenant-a"))
	if len(results) != 1 || results[0].SpanKind != trace.SpanKindConsumer || results[0].Parent.SpanID() != send.SpanID() || results[0].SpanContext.TraceID() != send.TraceID() || results[0].Status.Code != codes.Unset {
		t.Fatalf("result spans %v, want one CONSUMER under the receiver's send %s, read once", results, send.SpanID())
	}

	if _, err := f.host.Cancel(f.ctx, runID); err != nil {
		t.Fatal(err)
	}
	spans = f.host.spans(t, exporter)
	cancels := teletest.Named(spans, "send "+operator.PoolEventsTopic("pool-a"))
	if len(cancels) != 1 || cancels[0].SpanKind != trace.SpanKindProducer || cancels[0].Parent.IsValid() || cancels[0].SpanContext.TraceID() == submit.SpanContext.TraceID() {
		t.Fatalf("cancel spans %v, want one PRODUCER root of its own trace", cancels)
	}
	if links := cancels[0].Links; len(links) != 1 || links[0].SpanContext.SpanID() != submit.SpanContext.SpanID() {
		t.Fatalf("cancel links %v, want the submit span", links)
	}
	events, err := readTopic(f.ctx, receiver.kafka, operator.PoolEventsTopic("pool-a"))
	if err != nil {
		t.Fatal(err)
	}
	cancelled := 0
	for _, event := range events {
		if string(event.Key) == runID && len(event.Headers) == 1 && telemetry.ParseTraceParent(string(event.Headers[0].Value)).SpanID() == cancels[0].SpanContext.SpanID() {
			cancelled++
		}
	}
	if cancelled != 1 {
		t.Fatalf("cancel records carrying the cancel span %d, want one", cancelled)
	}
	teletest.Check(t, spans)
	teletest.Sentinels(t, spans, "model-token", f.secret, "cat", prompt, task.submission.Wherry.Envelope, task.submission.Wherry.Wrap,
		string(local.RedactionKey), base64.StdEncoding.EncodeToString(local.RedactionKey), "s3cr3t")
}

// Brief test 3, host side: a storage refusal naming the access key, raised before store.wrap, leaves the keys out of
// the upload span, which carries a fixed class alone.
func TestTracedUploadFailureKeepsTheStorageKeys(t *testing.T) {
	exporter := teletest.Install(t)
	server := refusingStore(t)
	f := newFixture(t)
	t.Cleanup(func() { f.host.tracing.Close() })
	const access, secret = "AKIAS3CR3TACCESSKEY", "s3cr3t-secret-key-value"
	accessFile, secretFile := filepath.Join(t.TempDir(), "access"), filepath.Join(t.TempDir(), "secret")
	for path, value := range map[string]string{accessFile: access, secretFile: secret} {
		if err := os.WriteFile(path, []byte(value+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	terms := testTerms()
	terms.Storage = &operator.Storage{Endpoint: server.URL, Bucket: testBucket}
	if err := f.service.PoolApply(f.ctx, "pool-s3", terms, false); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantPoolAdd(f.ctx, "tenant-a", "pool-s3"); err != nil {
		t.Fatal(err)
	}
	f.host.storage = map[string]StorageConfig{"pool-s3": {AccessKey: "s3-access", SecretKey: "s3-secret"}}
	f.host.credentials["s3-access"], f.host.credentials["s3-secret"] = credential.Source{File: accessFile}, credential.Source{File: secretFile}
	receiver := f.addReceiver(t, "receiver-s3", "pool-s3")
	receiver.advertise(t, f.ctx, nil)
	file, _ := randomFile(t, "dataset.bin", 16)
	_, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "upload", S3Inputs: []S3File{{Name: "dataset.bin", Body: file}},
		EndBy: time.Now().Add(30 * time.Minute), StartBy: time.Now().Add(10 * time.Minute)})
	if err == nil || !strings.Contains(err.Error(), "no access key") || strings.Contains(err.Error(), access) {
		t.Fatalf("submit = %v; want the store's refusal with the key redacted", err)
	}
	spans := f.host.spans(t, exporter)
	uploads := teletest.Named(spans, "upload inputs")
	if len(uploads) != 1 || uploads[0].SpanKind != trace.SpanKindClient || uploads[0].Status.Code != codes.Error || uploads[0].Status.Description != telemetry.ClassFailed || len(uploads[0].Events) != 0 {
		t.Fatalf("upload spans %v, want one failed CLIENT span without events", uploads)
	}
	if submits := teletest.Named(spans, "submit"); len(submits) != 1 || submits[0].Status.Description != telemetry.ClassError {
		t.Fatalf("submit spans %v, want one failed with the generic class", submits)
	}
	teletest.Check(t, spans)
	teletest.Sentinels(t, spans, access, secret, server.URL, "s3cr3t")
}

// Brief test 4, host side: with no endpoint set the task record carries no header and nothing is exported.
func TestTracingIsOffByDefaultAtTheHost(t *testing.T) {
	exporter := teletest.Hook(t)
	t.Setenv(telemetry.EndpointEnv, "")
	t.Setenv(telemetry.TracesEndpointEnv, "")
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	submitted := f.submit(t, defaultSelector())
	if f.host.tracing.Enabled() {
		t.Fatal("tracing is on without an endpoint")
	}
	if record := taskRecord(t, f, receiver.kafka, "pool-a", submitted.RunID); len(record.Headers) != 0 {
		t.Fatalf("task record carries headers %v", record.Headers)
	}
	if local, err := f.host.localRun(submitted.RunID); err != nil || local.TraceParent != "" {
		t.Fatalf("local record trace parent %q, %v; want none", local.TraceParent, err)
	}
	if _, err := f.host.Cancel(f.ctx, submitted.RunID); err != nil {
		t.Fatal(err)
	}
	if spans := exporter.Spans(); len(spans) != 0 {
		t.Fatalf("%d spans exported without an endpoint", len(spans))
	}
}

// A resubmission is a new trace, linked to the original run's submit span.
func TestTracedResubmissionLinksTheOriginalRun(t *testing.T) {
	exporter := teletest.Install(t)
	f := newFixture(t)
	t.Cleanup(func() { f.host.tracing.Close() })
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	receiver.keyLost(t, f.ctx, task)
	status, err := f.host.Status(f.ctx, runID)
	if err != nil || status.Replacement == "" {
		t.Fatalf("status %+v, %v; want a replacement", status, err)
	}
	spans := f.host.spans(t, exporter)
	var original, replacement tracetest.SpanStub
	for _, s := range teletest.Named(spans, "submit") {
		switch teletest.Attr(s, telemetry.AttrRun) {
		case runID:
			original = s
		case status.Replacement:
			replacement = s
		}
	}
	if !original.SpanContext.IsValid() || !replacement.SpanContext.IsValid() {
		t.Fatalf("submit spans %v, want the original's and the replacement's", teletest.Named(spans, "submit"))
	}
	if replacement.Parent.IsValid() || replacement.SpanContext.TraceID() == original.SpanContext.TraceID() {
		t.Fatalf("replacement submit has parent %s in trace %s; want a new trace", replacement.Parent.SpanID(), replacement.SpanContext.TraceID())
	}
	if links := replacement.Links; len(links) != 1 || links[0].SpanContext.SpanID() != original.SpanContext.SpanID() {
		t.Fatalf("replacement links %v, want the original submit", links)
	}
	record := taskRecord(t, f, receiver.kafka, "pool-a", status.Replacement)
	if len(record.Headers) != 1 || telemetry.ParseTraceParent(string(record.Headers[0].Value)).TraceID() != replacement.SpanContext.TraceID() {
		t.Fatalf("replacement record headers %v, want the replacement's trace", record.Headers)
	}
	teletest.Check(t, spans)
}
