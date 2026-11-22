package main

import (
	"bytes"
	"context"
	"encoding/base64"
	json "encoding/json/v2"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/receiver"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/telemetry"
	"github.com/benemon/wherry/internal/telemetry/teletest"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
	"gopkg.in/yaml.v3"
)

// Sentinel values the round trip's spans must never carry.
const (
	tracedPrompt   = "s3cr3t-prompt-from-the-host"
	tracedAccess   = "AKIAS3CR3TACCESSKEY"
	tracedSecret   = "s3cr3t-secret-key-value"
	tracedSetting  = "s3cr3t-plugin-setting-token"
	tracedPathPart = "s3cr3t-source-directory"
)

// tracedS3 is a TLS S3 stand-in: it keeps PUT objects and serves them, and refuses a PUT of a key holding refused with
// an InvalidAccessKeyId answer that names the access key, as a real store does.
func tracedS3(t *testing.T) *s3Store {
	t.Helper()
	var mu sync.Mutex
	objects := map[string][]byte{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Has("location") {
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprint(w, "<LocationConstraint></LocationConstraint>")
			return
		}
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodPut && strings.Contains(r.URL.Path, "refused"):
			_, signer, _ := strings.Cut(r.Header.Get("Authorization"), "Credential=")
			access, _, _ := strings.Cut(signer, "/")
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprintf(w, "<Error><Code>InvalidAccessKeyId</Code><Message>no access key %s</Message></Error>", access)
		case r.Method == http.MethodPut:
			objects[r.URL.Path], _ = io.ReadAll(r.Body)
			w.Header().Set("ETag", `"test"`)
		case r.Method == http.MethodGet:
			data, ok := objects[r.URL.Path]
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Write(data)
		default:
			http.Error(w, "method", http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)
	previous := http.DefaultTransport
	http.DefaultTransport = server.Client().Transport
	t.Cleanup(func() { http.DefaultTransport = previous })
	ca := filepath.Join(t.TempDir(), "s3-ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SSL_CERT_FILE", ca)
	return &s3Store{endpoint: server.URL, keys: operator.StorageKeys{AccessKey: tracedAccess, SecretKey: tracedSecret}, client: server.Client()}
}

func readBundle(t *testing.T, path string) operator.Bundle {
	t.Helper()
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var bundle operator.Bundle
	if err := yaml.Unmarshal(encoded, &bundle); err != nil {
		t.Fatal(err)
	}
	return bundle
}

// grantTraces grants principal WRITE and DESCRIBE on the trace topics of tenants, as slice 2's operator will, after the
// last operator command: converge withdraws rows the operator does not manage.
func grantTraces(t *testing.T, ctx context.Context, root *kgo.Client, principal string, tenants ...string) {
	t.Helper()
	for _, tenant := range tenants {
		builder := kadm.NewACLs().Allow(principal).AllowHosts("*").Topics(telemetry.TraceTopic(tenant)).ResourcePatternType(kadm.ACLPatternLiteral).Operations(kadm.OpWrite, kadm.OpDescribe)
		builder.PrefixUser()
		results, err := kadm.NewClient(root).CreateACLs(ctx, builder)
		if err != nil {
			t.Fatal(err)
		}
		for _, result := range results {
			if result.Err != nil {
				t.Fatal(result.Err)
			}
		}
	}
}

// median is the middle of durations.
func median(durations []time.Duration) time.Duration {
	sorted := slices.Clone(durations)
	slices.Sort(sorted)
	return sorted[len(sorted)/2]
}

// mapMinimum reports whether a span is one the brief's map minimum names: the edges' PRODUCER and CONSUMER spans, the
// submit root and the run.
func mapMinimum(s tracetest.SpanStub) bool {
	return s.SpanKind == trace.SpanKindProducer || s.SpanKind == trace.SpanKindConsumer || s.Name == "submit" || s.Name == "run"
}

// TestTracedRoundTrip runs a task from a host's Submit through a receiver, its custodian and the test plugin process
// to the host's Await with tracing on in every role's configuration, each role writing its spans to the trace topics
// through its own Kafka client, and checks the run is one trace on the tenant's topic: brief tests 1, 3 and 4 across
// the roles, the spans per run, the trace bytes per run and the submit wall time with tracing on and off.
func TestTracedRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cluster, ca := tlsCluster(t)
	address := cluster.ListenAddrs()[0]
	for _, tenant := range []string{"", "tenant-a"} {
		if err := cluster.CreateTopic(telemetry.TraceTopic(tenant), 1, nil); err != nil {
			t.Fatal(err)
		}
	}
	root := connectTLS(t, address, ca, "admin", "password")
	service := &operator.Service{Operator: connectTLS(t, address, ca, "wherry.operator", "operator-password"),
		Broker: address, CA: []byte(ca), DeliveryCeiling: 10, LockCeilingMS: 60000}
	operatorTracing, err := telemetry.New(ctx, telemetry.RoleOperator, telemetry.Names{}, service.Operator, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	service.Tracing = operatorTracing
	dir := t.TempDir()
	optional := false
	terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude", Credentials: []operator.AgentCredential{
		{TaskCredential: wire.TaskCredential{Name: "anthropic-api-key", Env: "ANTHROPIC_API_KEY"}},
		{TaskCredential: wire.TaskCredential{Name: "github-token", Env: "GH_TOKEN"}, Required: &optional},
	}}}, DefaultAgent: "claude", DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
	bucket := tracedS3(t)
	terms.Storage = &operator.Storage{Endpoint: bucket.endpoint, Bucket: s3Bucket}
	service.Storage = func(string) (operator.StorageKeys, error) { return bucket.keys, nil }
	hostBundle, receiverBundle := filepath.Join(dir, "host.yaml"), filepath.Join(dir, "receiver.yaml")
	labels := map[string]string{"gpu": "yes"}
	config := operator.ReceiverConfig{Receiver: "receiver-a", Pool: "pool-a",
		Profiles: []operator.ProfileSettings{{Name: "fake", Plugin: "fake", Agent: "claude", Labels: labels,
			Config: map[string]any{"outputs": true, "finish": true, "inline_limit": 1000, "credential": "fake-token", "note": tracedSetting}}},
		PluginSHA256: map[string]string{"fake": fakeSHA256}}
	for _, step := range []func() error{
		func() error { return service.PoolApply(ctx, "pool-a", terms, false) },
		func() error { return service.TenantAdd(ctx, "tenant-a") },
		func() error { return service.TenantPoolAdd(ctx, "tenant-a", "pool-a") },
		func() error { return service.HostAdd(ctx, "tenant-a", "host-a", hostBundle, false) },
		func() error { return service.ReceiverAdd(ctx, config, service.Broker, receiverBundle) },
	} {
		if err := step(); err != nil {
			t.Fatal(err)
		}
	}
	if err := operatorTracing.Close(); err != nil {
		t.Fatal(err)
	}
	hostBundled, receiverBundled := readBundle(t, hostBundle), readBundle(t, receiverBundle)
	grantTraces(t, ctx, root, hostBundled.Principal, "tenant-a")
	grantTraces(t, ctx, root, receiverBundled.Principal, "tenant-a", "")

	read := recordPlugin(t)
	t.Setenv("WHERRY_TRACED_PLATFORM", platformText+"\n")
	plugins := &plugins{dir: pluginDir, credentials: map[string]credential.Source{"fake-token": {Env: "WHERRY_TRACED_PLATFORM"}}, logger: log.New(io.Discard, "", 0), running: map[string]*pluginsdk.Process{}}
	defer plugins.close()
	logs := &syncBuffer{}
	r, err := receiver.New(receiverBundled, []byte(ca), filepath.Join(dir, "receiver-state"), false, 0, plugins.profile, log.New(logs, "receiver: ", 0), prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	plugins.logs, plugins.hold = r.PluginLogs(logs), r.HoldPlatformCredential
	r.Trace()
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	ran := make(chan error, 1)
	go func() { ran <- r.Run(runCtx) }()

	sourceDir := filepath.Join(dir, tracedPathPart)
	if err := os.MkdirAll(sourceDir, 0o700); err != nil {
		t.Fatal(err)
	}
	deployPath := filepath.Join(sourceDir, "deploy-key")
	if err := os.WriteFile(deployPath, []byte(deployText), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ANTHROPIC_API_KEY", credentialText)
	caFile := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caFile, []byte(ca), 0o600); err != nil {
		t.Fatal(err)
	}
	hostConfig := busclient.Config{Bundle: hostBundle, CAFile: caFile, HostKey: filepath.Join(dir, "host.key"), StateDir: filepath.Join(dir, "host-state"),
		Credentials: map[string]credential.Source{"deploy-key": {Command: []string{"cat", deployPath}}}, Tracing: true}
	if _, _, err := busclient.Init(ctx, hostConfig); err != nil {
		t.Fatal(err)
	}
	hostLogs := &syncBuffer{}
	host, err := busclient.Connect(hostConfig, log.New(hostLogs, "host: ", 0))
	if err != nil {
		t.Fatal(err)
	}
	input := busclient.SubmitInput{Agent: "claude", Selector: []wire.Requirement{{Key: "gpu", Operator: wire.OpExists}},
		Prompt: tracedPrompt, Parts: []wire.Part{{Raw: wire.StdBytes(bytes.Repeat([]byte("dataset "), 250)), Filename: "dataset.bin"}},
		Credentials: []wire.TaskCredential{{Name: "deploy-key", File: "id_ed25519"}}, Outputs: []wire.ExpectedOutput{{Name: "big.bin", Required: true, Storage: wire.StorageS3}},
		EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)}
	var submitted busclient.SubmitResult
	for ctx.Err() == nil {
		if submitted, err = host.Submit(ctx, input); err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("submit never placed: %v", err)
	}
	awaited, err := host.Await(ctx, submitted.RunID, 30*time.Second)
	if err != nil || awaited.Status.State != wire.TaskStateCompleted || len(awaited.Stored) != 1 {
		t.Fatalf("await = %+v, %v; want the completed result with its S3 output; logs:\n%s", awaited, err, logs.String())
	}
	// A refused upload: the store's answer names the access key, which store.wrap redacts and no span records.
	input.S3Inputs, input.Parts = []busclient.S3File{{Name: "refused.bin", Body: bytes.NewReader([]byte("refused"))}}, nil
	if _, err := host.Submit(ctx, input); err == nil || !strings.Contains(err.Error(), "no access key") || strings.Contains(err.Error(), tracedAccess) {
		t.Fatalf("refused upload = %v; want the store's refusal with the key redacted", err)
	}

	// Submit wall time, median of 10 after a warm-up, with tracing on and off in the host's configuration.
	time10 := func(h *busclient.Host) time.Duration {
		if _, err := h.Submit(ctx, input); err != nil {
			t.Fatal(err)
		}
		var durations []time.Duration
		for range 10 {
			began := time.Now()
			if _, err := h.Submit(ctx, input); err != nil {
				t.Fatal(err)
			}
			durations = append(durations, time.Since(began))
		}
		return median(durations)
	}
	input.S3Inputs, input.Parts = nil, nil
	traced := time10(host)
	hostConfig.Tracing = false
	offHost, err := busclient.Connect(hostConfig, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	untraced := time10(offHost)
	t.Logf("submit wall time, median of 10: tracing on %s, off %s", traced, untraced)

	began := time.Now()
	host.Close()
	closeTook := time.Since(began)
	offHost.Close()
	stop()
	select {
	case err := <-ran:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("receiver run = %v", err)
		}
	case <-time.After(time.Minute):
		t.Fatalf("receiver kept running; logs:\n%s", logs.String())
	}
	for _, text := range []string{logs.String(), hostLogs.String()} {
		if strings.Contains(text, "tracing:") {
			t.Fatalf("an export failed:\n%s", text)
		}
	}
	tenantRecords, err := readTopic(ctx, service.Operator, telemetry.TraceTopic("tenant-a"))
	if err != nil {
		t.Fatal(err)
	}
	untenantedRecords, err := readTopic(ctx, service.Operator, telemetry.TraceTopic(""))
	if err != nil {
		t.Fatal(err)
	}
	spans := teletest.Spans(t, append(slices.Clone(tenantRecords), untenantedRecords...))
	ids := teletest.ByID(spans)
	var run []tracetest.SpanStub
	var submit tracetest.SpanStub
	for _, s := range spans {
		if s.Name == "submit" && teletest.Attr(s, telemetry.AttrRun) == submitted.RunID {
			submit = s
		}
	}
	if !submit.SpanContext.IsValid() || submit.Parent.IsValid() {
		t.Fatalf("submit span %v, want a root for %s", submit, submitted.RunID)
	}
	for _, s := range spans {
		if s.SpanContext.TraceID() == submit.SpanContext.TraceID() {
			run = append(run, s)
		}
	}
	byName := func(name string, kind trace.SpanKind, role string) []tracetest.SpanStub {
		var found []tracetest.SpanStub
		for _, s := range run {
			if strings.HasPrefix(s.Name, name) && s.SpanKind == kind && teletest.Resource(s, semconv.ServiceNamespaceKey) == role {
				found = append(found, s)
			}
		}
		return found
	}
	one := func(name string, kind trace.SpanKind, role string) tracetest.SpanStub {
		t.Helper()
		found := byName(name, kind, role)
		if len(found) != 1 {
			t.Fatalf("%s %s spans of the %s in the run's trace: %d, want one", name, kind, role, len(found))
		}
		return found[0]
	}
	taskTopic := operator.SignatureTopic("pool-a", "claude", labels)
	events, feed := operator.PoolEventsTopic("pool-a"), operator.TenantTopic("tenant-a")
	send := one("send "+taskTopic, trace.SpanKindProducer, telemetry.RoleHost)
	process := one("process "+taskTopic, trace.SpanKindConsumer, telemetry.RoleReceiver)
	running := one("run", trace.SpanKindInternal, telemetry.RoleReceiver)
	request := one("send "+events, trace.SpanKindProducer, telemetry.RoleReceiver)
	custodian := one("process "+events, trace.SpanKindConsumer, telemetry.RoleCustodian)
	wrap := one("send "+events, trace.SpanKindProducer, telemetry.RoleCustodian)
	opened := one("process "+events, trace.SpanKindConsumer, telemetry.RoleReceiver)
	for child, parent := range map[*tracetest.SpanStub]tracetest.SpanStub{
		&send: submit, &process: send, &running: process, &request: running, &custodian: request, &wrap: custodian, &opened: wrap,
	} {
		if child.Parent.SpanID() != parent.SpanContext.SpanID() {
			t.Errorf("%s (%s) has parent %s, want %s (%s)", child.Name, teletest.Resource(*child, semconv.ServiceNamespaceKey), ids[child.Parent.SpanID()].Name, parent.Name, teletest.Resource(parent, semconv.ServiceNamespaceKey))
		}
	}
	if teletest.Attr(custodian, telemetry.AttrRun) != submitted.RunID || teletest.Attr(wrap, telemetry.AttrRun) != submitted.RunID {
		t.Errorf("custodian spans name run %q and %q, want the verified task's %s", teletest.Attr(custodian, telemetry.AttrRun), teletest.Attr(wrap, telemetry.AttrRun), submitted.RunID)
	}
	// The result and the terminal status: a PRODUCER each under the run, and a host CONSUMER each under its own.
	terminals, results := byName("send "+feed, trace.SpanKindProducer, telemetry.RoleReceiver), byName("process "+feed, trace.SpanKindConsumer, telemetry.RoleHost)
	if len(terminals) != 2 || len(results) != 2 {
		t.Fatalf("terminal sends %d and feed consumers %d, want two each", len(terminals), len(results))
	}
	for _, terminal := range terminals {
		if terminal.Parent.SpanID() != running.SpanContext.SpanID() {
			t.Errorf("terminal send %s has parent %s, want the run", terminal.SpanContext.SpanID(), ids[terminal.Parent.SpanID()].Name)
		}
		under := 0
		for _, result := range results {
			if result.Parent.SpanID() == terminal.SpanContext.SpanID() {
				under++
			}
		}
		if under != 1 {
			t.Errorf("feed consumers under terminal send %s: %d, want one", terminal.SpanContext.SpanID(), under)
		}
	}
	if process.Name != "process "+taskTopic+" fake" || teletest.Attr(process, telemetry.AttrProfile) != "fake" || teletest.Attr(process, telemetry.AttrAgent) != "claude" || teletest.Attr(process, telemetry.AttrAttempt) != "1" {
		t.Errorf("process span %s %v", process.Name, process.Attributes)
	}
	for _, s := range run {
		if s.Status.Code == codes.Error {
			t.Errorf("span %s of the %s failed: %s", s.Name, teletest.Resource(s, semconv.ServiceNamespaceKey), s.Status.Description)
		}
	}
	services := map[string]bool{}
	for _, s := range run {
		services[teletest.Resource(s, semconv.ServiceNameKey)] = true
	}
	if !services["wherry-host.tenant-a.host-a"] || !services["wherry-receiver.pool-a.receiver-a"] || !services["wherry-custodian.pool-a"] || len(services) != 3 {
		t.Errorf("services in the run's trace %v", services)
	}
	for _, name := range []string{"plugin Dispatch", "plugin Lookup", "plugin Follow", "plugin Stop", "deliver credentials", "await start", "upload output", "resolve credential", "seal envelope", "upload inputs", "admit"} {
		if len(teletest.Named(run, name)) == 0 {
			t.Errorf("no %s span in the run's trace", name)
		}
	}
	// Brief test 4: the uploads carry the terms' endpoint and bucket, and nothing of the presigned URLs.
	for _, name := range []string{"upload inputs", "upload output"} {
		for _, upload := range teletest.Named(run, name) {
			if teletest.Attr(upload, telemetry.AttrStorageEndpoint) != bucket.endpoint || teletest.Attr(upload, telemetry.AttrStorageBucket) != s3Bucket {
				t.Errorf("%s span storage %q %q, want the terms' %q %q", name, teletest.Attr(upload, telemetry.AttrStorageEndpoint), teletest.Attr(upload, telemetry.AttrStorageBucket), bucket.endpoint, s3Bucket)
			}
		}
	}
	minimum := 0
	for _, s := range run {
		if mapMinimum(s) {
			minimum++
		}
	}
	runBytes, runRecords := 0, 0
	for _, record := range tenantRecords {
		if string(record.Key) == submit.SpanContext.TraceID().String() {
			runBytes += len(record.Key) + len(record.Value)
			runRecords++
		}
	}
	t.Logf("spans per run: map minimum %d, total %d; trace bytes per run %d in %d records; host close %s", minimum, len(run), runBytes, runRecords, closeTook)
	for _, s := range run {
		t.Logf("  %-9s %-9s %s", teletest.Resource(s, semconv.ServiceNamespaceKey), s.SpanKind, s.Name)
	}
	for _, name := range []string{"receiver start", "custodian rotation", "operator pool apply", "operator tenant add", "operator tenant pool add", "operator tenant host add", "operator receiver provision"} {
		roots := teletest.Named(spans, name)
		if len(roots) == 0 || roots[0].Parent.IsValid() {
			t.Errorf("%s spans %v, want a root", name, roots)
		}
	}
	// Routing: every span of the run, the host's, the receiver's and the custodian's, carries the run's tenant and is
	// on the tenant's topic, keyed by the run's trace. The spans outside a run, receiver start, custodian rotation and
	// the operator markers of no tenant, carry none and are on wherry.traces; the tenant markers are on the tenant's.
	for _, s := range run {
		if tenant := teletest.Attr(s, telemetry.AttrTenant); tenant != "tenant-a" {
			t.Errorf("span %s of the %s (%s) in the run's trace carries wherry.tenant %q, want tenant-a", s.Name, teletest.Resource(s, semconv.ServiceNamespaceKey), s.SpanKind, tenant)
		}
	}
	onTopic := map[string]string{}
	for topic, records := range map[string][]*kgo.Record{telemetry.TraceTopic("tenant-a"): tenantRecords, telemetry.TraceTopic(""): untenantedRecords} {
		for _, record := range records {
			for _, s := range teletest.Spans(t, []*kgo.Record{record}) {
				if s.SpanContext.TraceID().String() != string(record.Key) {
					t.Errorf("record %d on %s keyed %s holds span %s of trace %s", record.Offset, topic, record.Key, s.Name, s.SpanContext.TraceID())
				}
				onTopic[s.Name+" "+s.SpanContext.SpanID().String()] = topic
			}
		}
	}
	for _, s := range run {
		if onTopic[s.Name+" "+s.SpanContext.SpanID().String()] != telemetry.TraceTopic("tenant-a") {
			t.Errorf("span %s of the run is on %s", s.Name, onTopic[s.Name+" "+s.SpanContext.SpanID().String()])
		}
	}
	for _, name := range []string{"receiver start", "custodian rotation", "operator pool apply", "operator receiver provision"} {
		for _, s := range teletest.Named(spans, name) {
			if tenant := teletest.Attr(s, telemetry.AttrTenant); tenant != "" {
				t.Errorf("%s, outside a run, carries wherry.tenant %q", name, tenant)
			}
			if topic := onTopic[s.Name+" "+s.SpanContext.SpanID().String()]; topic != telemetry.TraceTopic("") {
				t.Errorf("%s is on %s, want %s", name, topic, telemetry.TraceTopic(""))
			}
		}
	}
	for _, name := range []string{"operator tenant add", "operator tenant pool add", "operator tenant host add"} {
		for _, s := range teletest.Named(spans, name) {
			if tenant := teletest.Attr(s, telemetry.AttrTenant); tenant != "tenant-a" {
				t.Errorf("%s carries wherry.tenant %q, want tenant-a", name, tenant)
			}
			if topic := onTopic[s.Name+" "+s.SpanContext.SpanID().String()]; topic != telemetry.TraceTopic("tenant-a") {
				t.Errorf("%s is on %s, want %s", name, topic, telemetry.TraceTopic("tenant-a"))
			}
		}
	}
	if markers := teletest.Named(spans, "operator "); len(markers) == 0 || teletest.Resource(markers[0], semconv.ServiceNameKey) != "wherry-operator" {
		t.Errorf("operator markers %v", markers)
	}

	teletest.Check(t, spans, "dns", "tls", "connection refused", "connection closed", "other")
	tasks, err := readTasks(ctx, service.Operator, "pool-a")
	if err != nil {
		t.Fatal(err)
	}
	var sentinels []string
	for _, record := range tasks {
		if string(record.Key) != submitted.RunID {
			continue
		}
		payload, err := base64.RawURLEncoding.DecodeString(strings.Split(string(record.Value), ".")[1])
		var task wire.Submission
		if err != nil || json.Unmarshal(payload, &task, signed.Tolerant) != nil || task.Wherry.Envelope == "" {
			t.Fatalf("task record for %s does not decode: %v", submitted.RunID, err)
		}
		sentinels = append(sentinels, task.Wherry.Envelope, task.Wherry.Wrap)
	}
	local, err := os.ReadFile(filepath.Join(hostConfig.StateDir, "runs", submitted.RunID+".json"))
	if err != nil {
		t.Fatal(err)
	}
	_, after, _ := strings.Cut(string(local), `"redaction_key":"`)
	redactionKey, _, _ := strings.Cut(after, `"`)
	if len(redactionKey) < 40 {
		t.Fatalf("local record has no redaction key: %s", local)
	}
	sentinels = append(sentinels, credentialText, platformText, deployText, deployPath, tracedPathPart, tracedPrompt, tracedAccess, tracedSecret, tracedSetting,
		hostBundled.Password, receiverBundled.Password, strings.TrimSpace(ca), "X-Amz-Signature", "X-Amz", wire.S3OutputKey("tenant-a", submitted.RunID, "big.bin"),
		wire.S3InputKey("tenant-a", submitted.RunID, "dataset.bin"), redactionKey, "s3cr3t")
	teletest.Sentinels(t, spans, sentinels...)
	teletest.Bytes(t, append(slices.Clone(tenantRecords), untenantedRecords...), sentinels...)
	var runs int
	for _, record := range read() {
		if record.Call == "dispatch" {
			runs++
		}
	}
	if runs == 0 {
		t.Fatal("the plugin dispatched nothing")
	}
}
