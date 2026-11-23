package client

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"log"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/telemetry"
	"github.com/benemon/wherry/internal/telemetry/teletest"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
	"gopkg.in/yaml.v3"
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

// feedTraced is feed with resultHeader on the result record and terminalHeader on each terminal status, as a
// receiver's terminal sends put them there.
func (r *testReceiver) feedTraced(t *testing.T, ctx context.Context, runID string, resultHeader, terminalHeader kgo.RecordHeader, values ...[]byte) {
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
		var status wire.RunStatus
		switch typ, _ := header(value); {
		case typ == wire.ResultType:
			record.Headers = []kgo.RecordHeader{resultHeader}
		case typ == wire.StatusType && wire.ParseRecord(value, signed.Keys{r.id: {Current: r.key.Public().(ed25519.PublicKey)}}, &status) == nil && status.Wherry.Kind == "terminal":
			record.Headers = []kgo.RecordHeader{terminalHeader}
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
// result's and the terminal status's CONSUMER spans are each a child of their own record's header and emitted once
// per record, for the records derive accepts into the outcome alone, and a cancel is in the run's trace under the
// submit context the local record saved. Brief test 3, host side: credential values, source paths and argv, the
// prompt, the envelope and the redaction key reach no span.
func TestTracedSubmitSendsTheHeaderAndReadsTheResult(t *testing.T) {
	exporter := teletest.Install(t)
	f := newFixture(t)
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

	// The feed: the second attempt's result and terminal status end the run; a late terminal of the first attempt,
	// authorised and kept but not admitted by derive, gets no span.
	task := receiver.claim(t, f.ctx, runID)[0]
	resultSend, resultHeader := receiverSend()
	terminalSend, terminalHeader := receiverSend()
	receiver.feedTraced(t, f.ctx, runID, resultHeader, terminalHeader, receiver.echo(t, task), receiver.status(t, task, 1, "claimed-1", "claimed", wire.TaskStateWorking, nil),
		receiver.result(t, task, 2, wire.TaskStateCompleted, nil), receiver.terminal(t, task, 2, wire.TaskStateCompleted, 1), receiver.terminal(t, task, 1, wire.TaskStateCanceled, 1))
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
	consumers := teletest.Named(spans, "process "+operator.TenantTopic("tenant-a"))
	if len(consumers) != 2 {
		t.Fatalf("feed CONSUMER spans %d, want the result's and the terminal status's, each read once", len(consumers))
	}
	for name, send := range map[string]trace.SpanContext{"result": resultSend, "terminal status": terminalSend} {
		under := 0
		for _, consumer := range consumers {
			if consumer.Parent.SpanID() != send.SpanID() {
				continue
			}
			under++
			if consumer.SpanKind != trace.SpanKindConsumer || consumer.SpanContext.TraceID() != send.TraceID() || consumer.Status.Code != codes.Unset {
				t.Fatalf("%s span %v, want a CONSUMER in the trace of the receiver's send %s", name, consumer, send.SpanID())
			}
		}
		if under != 1 {
			t.Fatalf("%s CONSUMER spans under the receiver's send %s: %d, want one", name, send.SpanID(), under)
		}
	}
	if len(f.host.awaited) != 0 {
		t.Fatalf("awaited runs after the wait: %d, want the dedup state dropped", len(f.host.awaited))
	}

	if _, err := f.host.Cancel(f.ctx, runID); err != nil {
		t.Fatal(err)
	}
	spans = f.host.spans(t, exporter)
	cancels := teletest.Named(spans, "send "+operator.PoolEventsTopic("pool-a"))
	if len(cancels) != 1 || cancels[0].SpanKind != trace.SpanKindProducer || cancels[0].Parent.SpanID() != submit.SpanContext.SpanID() || cancels[0].SpanContext.TraceID() != submit.SpanContext.TraceID() || len(cancels[0].Links) != 0 {
		t.Fatalf("cancel spans %v, want one PRODUCER under the submit span %s in the run's trace", cancels, submit.SpanContext.SpanID())
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

// Brief test 4, host side: the input upload span carries the pool terms' endpoint and bucket, and nothing of the
// presigned URLs the envelope carries. Brief test 3: the keys that signed the uploads reach no span.
func TestTracedUploadInputsCarryTheTerms(t *testing.T) {
	exporter := teletest.Install(t)
	o := newUploadOracle(t)
	f := newFixture(t)
	terms := testTerms()
	terms.Storage = &operator.Storage{Endpoint: o.endpoint, Bucket: testBucket}
	if err := f.service.PoolApply(f.ctx, "pool-s3", terms, false); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantPoolAdd(f.ctx, "tenant-a", "pool-s3"); err != nil {
		t.Fatal(err)
	}
	f.host.storage = map[string]StorageConfig{"pool-s3": *o.config}
	maps.Copy(f.host.credentials, o.sources)
	receiver := f.addReceiver(t, "receiver-s3", "pool-s3")
	receiver.advertise(t, f.ctx, nil)
	file, _ := randomFile(t, "dataset.bin", 1024)
	submitted, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "upload", S3Inputs: []S3File{{Name: "dataset.bin", Body: file}},
		Outputs: []wire.ExpectedOutput{{Name: "report.pdf", Required: true, Storage: wire.StorageS3}}, EndBy: time.Now().Add(30 * time.Minute), StartBy: time.Now().Add(10 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	spans := f.host.spans(t, exporter)
	uploads := teletest.Named(spans, "upload inputs")
	if len(uploads) != 1 || uploads[0].SpanKind != trace.SpanKindClient || uploads[0].Status.Code != codes.Unset {
		t.Fatalf("upload spans %v, want one CLIENT span", uploads)
	}
	if endpoint, bucket := teletest.Attr(uploads[0], telemetry.AttrStorageEndpoint), teletest.Attr(uploads[0], telemetry.AttrStorageBucket); endpoint != o.endpoint || bucket != testBucket {
		t.Fatalf("upload span storage %q %q, want the terms' %q %q", endpoint, bucket, o.endpoint, testBucket)
	}
	if count, size := teletest.Attr(uploads[0], telemetry.AttrCount), teletest.Attr(uploads[0], telemetry.AttrSize); count != "1" || size != "1024" {
		t.Fatalf("upload span count %s size %s", count, size)
	}
	task := taskRecord(t, f, receiver.kafka, "pool-s3", submitted.RunID)
	payload := string(payloadOf(t, task.Value))
	_, envelope, _ := strings.Cut(payload, `"envelope":"`)
	envelope, _, _ = strings.Cut(envelope, `"`)
	if len(envelope) < 40 {
		t.Fatalf("task record carries no envelope: %s", payload)
	}
	teletest.Check(t, spans)
	teletest.Sentinels(t, spans, envelope, "X-Amz-Signature", "X-Amz", wire.S3InputKey("tenant-a", submitted.RunID, "dataset.bin"), wire.S3OutputKey("tenant-a", submitted.RunID, "report.pdf"),
		o.config.AccessKey, o.config.SecretKey, "s3cr3t")
}

// Brief test 3, host side: a storage refusal naming the access key, raised before store.wrap, leaves the keys out of
// the upload span, which carries a fixed class and the terms' endpoint and bucket alone.
func TestTracedUploadFailureKeepsTheStorageKeys(t *testing.T) {
	exporter := teletest.Install(t)
	server := refusingStore(t)
	f := newFixture(t)
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
	if teletest.Attr(uploads[0], telemetry.AttrStorageEndpoint) != server.URL || teletest.Attr(uploads[0], telemetry.AttrStorageBucket) != testBucket {
		t.Fatalf("upload span attributes %v, want the terms' endpoint and bucket", uploads[0].Attributes)
	}
	if submits := teletest.Named(spans, "submit"); len(submits) != 1 || submits[0].Status.Description != telemetry.ClassError {
		t.Fatalf("submit spans %v, want one failed with the generic class", submits)
	}
	teletest.Check(t, spans)
	teletest.Sentinels(t, spans, access, secret, "s3cr3t")
}

// A cancel of a run whose local record saved no trace context, submitted while tracing was off, is a root of its own
// with no parent and nothing to link.
func TestTracedCancelWithoutASavedContextIsARoot(t *testing.T) {
	exporter := teletest.Hook(t)
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	submitted := f.submit(t, defaultSelector())
	if local, err := f.host.localRun(submitted.RunID); err != nil || local.TraceParent != "" {
		t.Fatalf("local record trace parent %q, %v; want none", local.TraceParent, err)
	}
	restarted, err := NewHost(f.host.kafka, f.host.key, operator.TenantPrincipal("tenant-a", "host-a"), f.stateDir, f.host.credentials, nil, f.host.logger)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Trace(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.tracing.Close() })
	if _, err := restarted.Cancel(f.ctx, submitted.RunID); err != nil {
		t.Fatal(err)
	}
	spans := restarted.spans(t, exporter)
	cancels := teletest.Named(spans, "send "+operator.PoolEventsTopic("pool-a"))
	if len(cancels) != 1 || cancels[0].SpanKind != trace.SpanKindProducer || cancels[0].Parent.IsValid() || len(cancels[0].Links) != 0 {
		t.Fatalf("cancel spans %v, want one PRODUCER root without a link", cancels)
	}
	teletest.Check(t, spans)
}

// Tracing is off by default: without the configuration's tracing field the task record carries no header and nothing
// is exported, whatever the OTLP exporter variables say.
func TestTracingIsOffByDefaultAtTheHost(t *testing.T) {
	exporter := teletest.Hook(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	t.Setenv("OTEL_TRACES_EXPORTER", "otlp")
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	submitted := f.submit(t, defaultSelector())
	if f.host.tracing.Enabled() {
		t.Fatal("tracing is on without the configuration's tracing field")
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
		t.Fatalf("%d spans exported without the configuration's tracing field", len(spans))
	}
	var config Config
	if err := yaml.Unmarshal([]byte("bundle: b.yaml\ntracing: true\n"), &config); err != nil || !config.Tracing {
		t.Fatalf("config %+v, %v; want tracing on", config, err)
	}
}

// A resubmission is a new trace, linked to the original run's submit span.
func TestTracedResubmissionLinksTheOriginalRun(t *testing.T) {
	exporter := teletest.Install(t)
	f := newFixture(t)
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
	if replacement.Parent.IsValid() || replacement.SpanContext.TraceID() == original.SpanContext.TraceID() || replacement.Status.Code != codes.Unset {
		t.Fatalf("replacement submit has parent %s in trace %s, status %+v; want a new trace", replacement.Parent.SpanID(), replacement.SpanContext.TraceID(), replacement.Status)
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

// A resubmission that fails, here against a suspended pool, fails its span with a fixed class and nothing of the error.
func TestTracedResubmissionFailureFailsTheSpan(t *testing.T) {
	exporter := teletest.Install(t)
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	receiver.keyLost(t, f.ctx, task)
	if err := f.service.PoolSuspend(f.ctx, "pool-a", true); err != nil {
		t.Fatal(err)
	}
	status, err := f.host.Status(f.ctx, runID)
	if err != nil || status.Replacement != "" {
		t.Fatalf("status %+v, %v; want no replacement produced", status, err)
	}
	spans := f.host.spans(t, exporter)
	var failed []tracetest.SpanStub
	for _, s := range teletest.Named(spans, "submit") {
		if teletest.Attr(s, telemetry.AttrRun) != runID {
			failed = append(failed, s)
		}
	}
	if len(failed) != 1 || failed[0].Status.Code != codes.Error || failed[0].Status.Description != telemetry.ClassError || len(failed[0].Links) != 1 {
		t.Fatalf("resubmission spans %v, want one failed with the generic class, linked to the original", failed)
	}
	teletest.Check(t, spans)
	teletest.Sentinels(t, spans, "suspended")
}

// tracedHost provisions name in tenant-a with a Kafka client of its own, which Close closes, grants it WRITE and
// DESCRIBE on its tenant's trace topic as slice 2's operator will, and turns its tracing on, through the Kafka transport
// and not the test exporter. The host logs to the returned buffer; its bundle is returned for its password.
func (f *fixture) tracedHost(t *testing.T, name string) (*Host, *syncBuffer, operator.Bundle) {
	t.Helper()
	bundlePath := filepath.Join(t.TempDir(), "bundle.yaml")
	if err := f.service.HostAdd(f.ctx, "tenant-a", name, bundlePath, false); err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(bundlePath)
	if err != nil {
		t.Fatal(err)
	}
	var bundle operator.Bundle
	if err := yaml.Unmarshal(encoded, &bundle); err != nil {
		t.Fatal(err)
	}
	root, err := kgo.NewClient(kgo.SeedBrokers(f.cluster.ListenAddrs()...), kgo.SASL(scram.Auth{User: "admin", Pass: "password"}.AsSha512Mechanism()))
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	builder := kadm.NewACLs().Allow(bundle.Principal).AllowHosts("*").Topics(telemetry.TraceTopic("tenant-a")).ResourcePatternType(kadm.ACLPatternLiteral).Operations(kadm.OpWrite, kadm.OpDescribe)
	builder.PrefixUser()
	if _, err := kadm.NewClient(root).CreateACLs(f.ctx, builder); err != nil {
		t.Fatal(err)
	}
	kafka, err := kgo.NewClient(kgo.SeedBrokers(f.service.Broker), kgo.SASL(scram.Auth{User: bundle.Principal, Pass: bundle.Password}.AsSha512Mechanism()),
		kgo.FetchMaxWait(10*time.Millisecond), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.DisableClientMetrics())
	if err != nil {
		t.Fatal(err)
	}
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	host, err := NewHost(kafka, key, bundle.Principal, t.TempDir(), map[string]credential.Source{"model": {File: f.secret}}, nil, log.New(logs, name+": ", 0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.Register(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := host.Trace(); err != nil {
		t.Fatal(err)
	}
	return host, logs, bundle
}

// Brief test 3, the host clause: Close flushes the host's spans through the trace client and closes it before closing
// the host's own client, so the submit's trace is on the tenant's trace topic, keyed by its trace ID, and its records
// carry no sentinel.
func TestTracedHostFlushesBeforeClosingItsClient(t *testing.T) {
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	topic := telemetry.TraceTopic("tenant-a")
	if err := f.cluster.CreateTopic(topic, 1, nil); err != nil {
		t.Fatal(err)
	}
	host, logs, bundle := f.tracedHost(t, "host-t")
	const prompt = "s3cr3t-prompt-text"
	submitted, err := host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: prompt, EndBy: time.Now().Add(30 * time.Minute), StartBy: time.Now().Add(10 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	local, err := host.localRun(submitted.RunID)
	if err != nil {
		t.Fatal(err)
	}
	host.Close()
	records, err := readTopic(f.ctx, f.service.Operator, topic)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) == 0 {
		t.Fatalf("nothing on %s after close; logs:\n%s", topic, logs.String())
	}
	traceID := telemetry.ParseTraceParent(local.TraceParent).TraceID().String()
	spans := teletest.Spans(t, records)
	for _, record := range records {
		if string(record.Key) != traceID {
			t.Fatalf("record %d keyed %s, want the submit's trace %s", record.Offset, record.Key, traceID)
		}
	}
	if submits := teletest.Named(spans, "submit"); len(submits) != 1 || teletest.Attr(submits[0], telemetry.AttrRun) != submitted.RunID || teletest.Attr(submits[0], telemetry.AttrTenant) != "tenant-a" {
		t.Fatalf("submit spans on %s: %v", topic, submits)
	}
	for _, name := range []string{"read state", "seal envelope", "resolve credential", "send "} {
		if len(teletest.Named(spans, name)) == 0 {
			t.Errorf("no %s span on %s", name, topic)
		}
	}
	teletest.Check(t, spans)
	teletest.Sentinels(t, spans, prompt, "model-token", f.secret, bundle.Password, string(local.RedactionKey), "s3cr3t")
	teletest.Bytes(t, records, prompt, "model-token", bundle.Password, base64.StdEncoding.EncodeToString(local.RedactionKey), "s3cr3t")
	if strings.Contains(logs.String(), "tracing:") {
		t.Fatalf("logs:\n%s", logs.String())
	}
}

// During a sustained outage of its tenant's trace topic, a host's submits complete with unchanged latency: the stalled
// records are the trace client's and the host's own client has none; every export into the stall is dropped with a
// timeout, logged once, and the submits' records are on the task topic.
func TestTracedSubmitUnaffectedByATraceOutage(t *testing.T) {
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	topic := telemetry.TraceTopic("tenant-a")
	if err := f.cluster.CreateTopic(topic, 1, nil); err != nil {
		t.Fatal(err)
	}
	traced, logs, _ := f.tracedHost(t, "host-t")
	stalled := f.cluster.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: topic, Err: kerr.NotLeaderForPartition, Count: -1})
	defer stalled.Remove()
	input := SubmitInput{Selector: defaultSelector(), Prompt: "do the work", EndBy: time.Now().Add(30 * time.Minute), StartBy: time.Now().Add(10 * time.Minute)}
	median := func(h *Host) time.Duration {
		if _, err := h.Submit(f.ctx, input); err != nil {
			t.Fatal(err)
		}
		var durations []time.Duration
		for range 10 {
			began := time.Now()
			if _, err := h.Submit(f.ctx, input); err != nil {
				t.Fatal(err)
			}
			durations = append(durations, time.Since(began))
		}
		slices.Sort(durations)
		return durations[len(durations)/2]
	}
	off := median(f.host)
	// Exports into the stall, one after another, as the processor issues them, each timing out and dropped.
	exporting, stopExporting := context.WithCancel(f.ctx)
	var exports sync.WaitGroup
	var timedOut atomic.Int32
	exports.Go(func() {
		for exporting.Err() == nil {
			var export *telemetry.ExportError
			if err := traced.tracing.Flush(exporting); errors.As(err, &export) && export.Class == telemetry.ClassTimeout {
				timedOut.Add(1)
			}
		}
	})
	on := median(traced)
	for timedOut.Load() == 0 {
		if f.ctx.Err() != nil {
			t.Fatal("no export timed out")
		}
		time.Sleep(50 * time.Millisecond)
	}
	later := median(traced)
	stopExporting()
	exports.Wait()
	t.Logf("submit wall time, median of 10: tracing off %s; on, during a trace outage, %s and %s; %d exports timed out", off, on, later, timedOut.Load())
	for _, got := range []time.Duration{on, later} {
		if got > 2*off+100*time.Millisecond {
			t.Fatalf("submit took %s during the trace outage, %s with tracing off", got, off)
		}
	}
	if buffered := traced.kafka.BufferedProduceRecords(); buffered != 0 {
		t.Fatalf("%d records buffered on the host's own client", buffered)
	}
	traced.Close()
	if text := logs.String(); strings.Count(text, "tracing: export to "+topic+" failed: timeout") != 1 || strings.Count(text, "tracing:") != 1 {
		t.Fatalf("logs:\n%s", text)
	}
	if records, err := readTopic(f.ctx, f.service.Operator, topic); err != nil || len(records) != 0 {
		t.Fatalf("%d records on %s during the stall, %v", len(records), topic, err)
	}
}

// Tracing off does nothing per wait: the dedup state is neither allocated nor locked, and the terminal spans' walk
// returns before touching it.
func TestTracingOffAllocatesNothingPerWait(t *testing.T) {
	f := newFixture(t)
	if f.host.tracing.Enabled() {
		t.Fatal("tracing is on")
	}
	r := &runView{h: f.host, run: "run-a", outcome: &AwaitResult{}, accepted: []entry{{record: &kgo.Record{Topic: "wherry.tenant.tenant-a"}}}}
	for name, call := range map[string]func(){
		"awaiting":       func() { f.host.awaiting("run-a")() },
		"traceTerminals": func() { f.host.traceTerminals(f.ctx, r) },
	} {
		if allocs := testing.AllocsPerRun(100, call); allocs != 0 {
			t.Errorf("%s allocates %v times per call with tracing off", name, allocs)
		}
	}
	if len(f.host.awaited) != 0 {
		t.Fatalf("awaited runs %v with tracing off", f.host.awaited)
	}
}
