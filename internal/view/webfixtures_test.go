package view

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/telemetry"
	"github.com/benemon/wherry/internal/wire"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// updateFixtures rewrites the page's fixtures from the view's JSON types instead of checking them.
var updateFixtures = flag.Bool("update", false, "rewrite internal/view/web/fixtures from the view's JSON types")

// webFixturesDir holds the JSON the page's unit tests and browser smoke run against, from this package's directory.
var webFixturesDir = filepath.Join("web", "fixtures")

// The fixtures' identifiers are derived from labels, so they follow the grammars the view checks and name nothing
// real: a run ID, an event ID, a key ID, an output hash.
func fixtureHex(label string, n int) string {
	sum := sha256.Sum256([]byte("wherry view fixture " + label))
	return hex.EncodeToString(sum[:])[:n]
}

func fixtureRun(label string) string   { return "run-" + fixtureHex(label, 32) }
func fixtureEvent(label string) string { return "event-" + fixtureHex(label, 32) }
func fixtureKey(label string) string {
	sum := sha256.Sum256([]byte("wherry view fixture key " + label))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

var fixtureWorking, fixtureFailed = fixtureRun("working"), fixtureRun("failed")

// spanStatus is a span status as the server encodes it, through the same path: unset or ok, or an error with its class.
func spanStatus(code tracepb.Status_StatusCode, class string) string {
	return allowlist{}.status(&tracepb.Status{Code: code, Message: class})
}

// webFixtures is every contract state the page handles, built from the types the API encodes so the page's tests
// read what the server writes: a gated receiver, stale and silent ones, a suspended custodian, lag error and
// unavailable, bounds that drop runs and ones that do not, a truncated runs list, model errors, topics missing,
// predating the log and in error, a run with a reason and its truncated flag, cancels, a start failure, a refused
// key request, outputs not returned, unknown storage, spans with an absent, ok and error status, and each transcript
// state. The names are invented; the receiver and pool names are the ones the docs use.
func webFixtures() map[string]any {
	now := time.Date(2026, 11, 24, 17, 51, 0, 0, time.UTC)
	at := func(minutesAgo float64) time.Time { return now.Add(-time.Duration(minutesAgo * float64(time.Minute))) }
	s3 := &Storage{Endpoint: "https://s3.example.com", Bucket: "runs-example"}
	const (
		tenantA, tenantB      = "acme", "ops"
		hostA, hostB, hostC   = "laptop-1", "workstation-2", "ci-1"
		receiverSigner        = "wherry.rcv.nomad-1"
		hostSigner            = "wherry.tnt." + tenantA + "." + hostA
		brokerHost, brokerDNS = "10.0.0.17", "kafka-1.example.com"
	)
	runs := []RunSummary{
		{ID: fixtureWorking, Tenant: tenantA, Host: hostA, Pool: "nomad", Receiver: "nomad-1", Agent: "claude", Model: "claude-fable-5-1", Attempt: 1, Stage: "working", State: string(wire.TaskStateWorking), Storage: s3, Admitted: at(37), First: at(37), Last: at(1)},
		{ID: fixtureFailed, Tenant: tenantA, Host: hostA, Pool: "nomad", Receiver: "nomad-1", Agent: "claude", Model: "claude-sonnet-5-5", Attempt: 2, Stage: "terminal", State: string(wire.TaskStateFailed), Class: "failed", Terminal: true, StorageUnknown: true, Cancelled: true, Admitted: at(12), First: at(12), Last: at(3), Outputs: 1, Truncated: []string{"run_events"}},
		{ID: fixtureRun("start failure"), Tenant: tenantA, Host: hostB, Pool: "openshift", Receiver: "openshift-1", Agent: "codex", Attempt: 1, Stage: "terminal", State: string(wire.TaskStateFailed), Class: "start-failure:dispatch", Terminal: true, Storage: s3, Admitted: at(130), First: at(130), Last: at(125)},
		{ID: fixtureRun("completed"), Tenant: tenantA, Host: hostA, Pool: "systemd", Receiver: "systemd-1", Agent: "claude", Model: "claude-fable-5-1", Attempt: 1, Stage: "terminal", State: string(wire.TaskStateCompleted), Terminal: true, Storage: s3, Admitted: at(50), First: at(50), Last: at(40), Outputs: 2},
		{ID: fixtureRun("canceled"), Tenant: tenantB, Host: hostC, Pool: "nomad", Receiver: "nomad-1", Agent: "claude", Attempt: 1, Stage: "terminal", State: string(wire.TaskStateCanceled), Class: "canceled", Terminal: true, Storage: s3, Cancelled: true, Admitted: at(300), First: at(300), Last: at(290)},
		{ID: fixtureRun("rejected"), Tenant: tenantB, Pool: "nomad", Receiver: "nomad-1", Attempt: 1, Stage: "rejected", State: string(wire.TaskStateRejected), Class: "rejected", Terminal: true, Storage: s3, Admitted: at(400), First: at(400), Last: at(400)},
		{ID: fixtureRun("released"), Tenant: tenantA, Host: hostA, Pool: "nomad", Receiver: "nomad-1", Agent: "claude", Attempt: 1, Stage: "released", State: string(wire.TaskStateWorking), Storage: s3, Admitted: at(700), First: at(700), Last: at(690)},
	}
	model := Snapshot{
		Version: 7, Now: now, WindowStart: now.Add(-24 * time.Hour), Rebuilt: true, RebuiltAt: now.Add(-3 * time.Hour),
		Brokers: []Broker{{ID: 0, Host: brokerHost, Port: 9094, Controller: true, Listeners: []string{"INTERNAL-9092://" + brokerDNS + ":9092", "TLS-9093://" + brokerDNS + ":9093"}}},
		Pools: []Pool{
			{Name: "nomad", Receivers: []string{"nomad-1", "nomad-2"}, Tenants: []string{tenantA, tenantB}, Custodian: "nomad-1", Storage: s3, DeliveryLimit: 5, Group: Group{State: "Stable", Members: 2}, Lag: Lag{Status: "ok", Records: 7, At: at(0.5)}, Runs: 5, InProgress: 1, ByState: map[string]int{string(wire.TaskStateWorking): 2, string(wire.TaskStateFailed): 1, string(wire.TaskStateCanceled): 1, string(wire.TaskStateRejected): 1}, ByClass: map[string]int{"failed": 1, "canceled": 1, "rejected": 1}, NoTerminal: 1, Agents: []string{"claude", "codex"}},
			{Name: "openshift", Receivers: []string{"openshift-1"}, Tenants: []string{tenantA}, Storage: s3, DeliveryLimit: 3, Group: Group{State: "Empty"}, Lag: Lag{Status: "error", Error: "timeout", At: at(1)}, Runs: 1, ByState: map[string]int{string(wire.TaskStateFailed): 1}, ByClass: map[string]int{"start-failure:dispatch": 1}, Agents: []string{"codex"}},
			{Name: "systemd", Suspended: true, Receivers: []string{"systemd-1"}, Tenants: []string{tenantA}, Custodian: "systemd-1", Storage: s3, DeliveryLimit: 5, Group: Group{State: "Stable", Members: 1}, Lag: Lag{Status: "unavailable", At: at(1)}, Runs: 1, ByState: map[string]int{string(wire.TaskStateCompleted): 1}, ByClass: map[string]int{}, Agents: []string{"claude"}},
		},
		Receivers: []Receiver{
			{Name: "nomad-1", Pool: "nomad", Status: "live", Capacity: 3, InFlight: 2, Gated: wire.GatedDraining, Offers: []Offer{{Profile: "claude", Agent: "claude", Replicas: 2, InFlight: 1}, {Profile: "codex", Agent: "codex", Replicas: 1}}, LastAdvertised: at(0.3), Profiles: []string{"claude", "codex"}, Runs: 5, ByState: map[string]int{string(wire.TaskStateWorking): 2, string(wire.TaskStateFailed): 1, string(wire.TaskStateCanceled): 1, string(wire.TaskStateRejected): 1}, ByClass: map[string]int{"failed": 1, "canceled": 1, "rejected": 1},
				Held: []HeldRun{{Run: fixtureWorking, Attempt: 1, Stage: "working", Source: "wherry.receiver.nomad-1.progress"}}, Stale: []HeldRun{{Run: fixtureRun("released"), Attempt: 1, Stage: "working", Stale: true, Source: "wherry.receiver.nomad-1.progress"}}, Unidentified: 1},
			{Name: "nomad-2", Pool: "nomad", Status: "ended", Ended: true, Offers: []Offer{}, Profiles: []string{}, ByState: map[string]int{}, ByClass: map[string]int{}, Held: []HeldRun{}, Stale: []HeldRun{}},
			{Name: "openshift-1", Pool: "openshift", Status: "stale", Capacity: 2, Offers: []Offer{{Profile: "codex", Agent: "codex", Replicas: 2}}, LastAdvertised: at(9), Profiles: []string{"codex"}, Runs: 1, ByState: map[string]int{string(wire.TaskStateFailed): 1}, ByClass: map[string]int{"start-failure:dispatch": 1}, Held: []HeldRun{}, Stale: []HeldRun{}},
			{Name: "systemd-1", Pool: "systemd", Status: "silent", Offers: []Offer{}, Profiles: []string{"claude"}, Runs: 1, ByState: map[string]int{string(wire.TaskStateCompleted): 1}, ByClass: map[string]int{}, Held: []HeldRun{}, Stale: []HeldRun{}, Unidentified: 2},
		},
		Custodians: []Custodian{
			{Pool: "nomad", Receiver: "nomad-1", Status: "live", KeyID: fixtureKey("nomad"), Reason: "rotation", At: at(180), RotatedFor: 3, Suspended: true, Signer: receiverSigner},
			{Pool: "systemd", Receiver: "systemd-1", Status: "live", KeyID: fixtureKey("systemd"), Reason: "rotation", At: at(600), RotatedFor: 1, Signer: "wherry.rcv.systemd-1"},
		},
		Hosts: []Host{
			{Tenant: tenantA, Name: hostA, Runs: 4, ByState: map[string]int{string(wire.TaskStateWorking): 2, string(wire.TaskStateFailed): 1, string(wire.TaskStateCompleted): 1}},
			{Tenant: tenantA, Name: hostB, Ended: true, Runs: 1, ByState: map[string]int{string(wire.TaskStateFailed): 1}},
			{Tenant: tenantB, Name: hostC, Runs: 1, ByState: map[string]int{string(wire.TaskStateCanceled): 1}},
		},
		Tenants: []Tenant{{Name: tenantA, Pools: []string{"nomad", "openshift", "systemd"}, Hosts: []string{hostA, hostB}, Runs: 5}, {Name: tenantB, Pools: []string{"nomad"}, Hosts: []string{hostC}, Runs: 2}},
		Storage: []StorageNode{{Endpoint: s3.Endpoint, Bucket: s3.Bucket, Pools: []string{"nomad", "openshift", "systemd"}, Runs: 7, Outputs: 3, Bytes: 3_560_000_000, Unreturned: 1, FailedRuns: 2}},
		Runs:    runs, RunsTotal: len(runs), Truncated: true,
		Topics: []Topic{
			{Name: "wherry.operator", Kind: "operator", Next: 9120, Start: 0, End: 9120},
			{Name: "wherry.pool.nomad.events", Kind: "events", Next: 410000, Start: 120000, End: 410000, PredatesLog: true},
			{Name: "wherry.tenant." + tenantB, Kind: "feed", Missing: true},
			{Name: "wherry.receiver.systemd-1.progress", Kind: "progress", Error: "timeout"},
		},
		Counts: map[string]int64{"duplicates": 159, "pending": 0, "retracted": 2},
		Bounds: []BoundHit{{Name: "runs", Hits: 3}, {Name: "transcript_scan", Hits: 1}},
		Errors: []string{"timeout"},
	}
	failed := RunDetail{
		RunSummary: runs[1], Hash: fixtureHex("failed task", 64), Reason: "agent exited with status 1: integration tests could not reach [redacted-url]",
		Echoes: []Echo{{Receiver: "nomad-1", Host: hostA, Pool: "nomad", Agent: "claude", Model: "claude-sonnet-5-5", At: at(12), Joined: true, Signer: receiverSigner}},
		Attempts: []Attempt{
			{Number: 1, Receiver: "nomad-1", TaskTopic: "wherry.pool.nomad.tasks.claude", Events: []Event{{Kind: "claimed", State: string(wire.TaskStateWorking), EventID: fixtureEvent("failed 1 claimed"), Receiver: "nomad-1", At: at(11.9), Signer: receiverSigner}},
				Decision: &Decision{Custodian: "nomad-1", Reason: wire.ClaimUnchanged, Served: true, At: at(11.8), Signer: receiverSigner}, StartFailure: &StartFailure{Profile: "claude", Agent: "claude", Class: "dispatch", At: at(11.5), Signer: receiverSigner}},
			{Number: 2, Receiver: "nomad-1", TaskTopic: "wherry.pool.nomad.tasks.claude", TranscriptChunks: 11, Events: []Event{
				{Kind: "claimed", State: string(wire.TaskStateWorking), EventID: fixtureEvent("failed 2 claimed"), Receiver: "nomad-1", At: at(10), Signer: receiverSigner},
				{Kind: "started", State: string(wire.TaskStateWorking), EventID: fixtureEvent("failed 2 started"), Receiver: "nomad-1", At: at(9.9), Signer: receiverSigner},
				{Kind: "working", State: string(wire.TaskStateWorking), EventID: fixtureEvent("failed 2 working"), Receiver: "nomad-1", At: at(8), Message: "running make integration against [redacted-url]", Signer: receiverSigner},
				{Kind: "terminal", State: string(wire.TaskStateFailed), EventID: fixtureEvent("failed 2 terminal"), Receiver: "nomad-1", At: at(3), Class: "failed", Message: "agent exited with status 1: integration tests could not reach [redacted-url]", Truncated: true, Signer: receiverSigner}}},
		},
		Opens: []Open{
			{Attempt: 1, Receiver: "nomad-1", At: at(11.85), Signer: receiverSigner, Reason: wire.RefusedPoolSuspended},
			{Attempt: 2, Receiver: "nomad-1", At: at(9.95), Signer: receiverSigner, Answer: "wrap", Custodian: "nomad-1", AnsweredAt: at(9.94), AnswerSigner: receiverSigner},
		},
		Cancels: []Cancel{{Host: hostA, At: at(4), Signer: hostSigner}},
		Result: &Result{Receiver: "nomad-1", Attempt: 2, State: string(wire.TaskStateFailed), At: at(3), Profile: "claude",
			Outputs: []Output{{Name: "test-output.txt", Size: 18342, SHA256: fixtureHex("test-output.txt", 64)}}, S3Outputs: []Output{{Name: "reports/integration/log.txt", Size: 9_000_000, SHA256: fixtureHex("log.txt", 64)}},
			NotReturned: []Unreturned{{Name: "reports/integration/report.md", Size: 2048, Reason: "missing"}, {Name: "(invalid name)", Reason: "invalid name"}}, ReplySize: 0, Artifacts: 2, Signer: receiverSigner},
		Path: []PathStep{{Step: "admitted", Actor: hostA + " > nomad-1", At: at(12)}, {Step: "terminal", Actor: "nomad-1", Attempt: 2, At: at(3), Detail: "failed failed"}},
	}
	working := RunDetail{
		RunSummary: runs[0], Hash: fixtureHex("working task", 64),
		Echoes: []Echo{{Receiver: "nomad-1", Host: hostA, Pool: "nomad", Agent: "claude", Model: "claude-fable-5-1", At: at(37), Joined: true, Signer: receiverSigner}},
		Attempts: []Attempt{{Number: 1, Receiver: "nomad-1", Events: []Event{
			{Kind: "claimed", State: string(wire.TaskStateWorking), EventID: fixtureEvent("working claimed"), Receiver: "nomad-1", At: at(36.9), Signer: receiverSigner},
			{Kind: "started", State: string(wire.TaskStateWorking), EventID: fixtureEvent("working started"), Receiver: "nomad-1", At: at(36.8), Signer: receiverSigner},
			{Kind: "working", State: string(wire.TaskStateWorking), EventID: fixtureEvent("working working"), Receiver: "nomad-1", At: at(30), Message: "go test ./... passing so far", Signer: receiverSigner}},
			Decision: &Decision{Custodian: "nomad-1", Reason: wire.ClaimUnchanged, Served: true, At: at(36.85), Signer: receiverSigner}}},
		Opens:   []Open{{Attempt: 1, Receiver: "nomad-1", At: at(36.88), Signer: receiverSigner, Answer: "wrap", Custodian: "nomad-1", AnsweredAt: at(36.87), AnswerSigner: receiverSigner}},
		Cancels: []Cancel{},
		Path:    []PathStep{{Step: "admitted", Actor: hostA + " > nomad-1", At: at(37)}},
	}
	unset := spanStatus(tracepb.Status_STATUS_CODE_UNSET, "")
	ok := spanStatus(tracepb.Status_STATUS_CODE_OK, "")
	span := func(id, parent, name, service, kind, status string, startMinutesAgo, ms float64, attrs map[string]string, children ...SpanNode) SpanNode {
		start := at(startMinutesAgo)
		if attrs == nil {
			attrs = map[string]string{}
		}
		if children == nil {
			children = []SpanNode{}
		}
		return SpanNode{ID: fixtureHex("span "+id, 16), Parent: parent, Name: name, Service: service, Kind: kind, Start: start, End: start.Add(time.Duration(ms * float64(time.Millisecond))), DurationMS: ms, Status: status, Attributes: attrs, Children: children}
	}
	claim := fixtureHex("span claim", 16)
	run := fixtureHex("span run", 16)
	trace := Forest{Run: fixtureFailed, Tenant: tenantA, Topic: telemetry.TraceTopic(tenantA), Traces: []Trace{{ID: fixtureHex("trace", 32), Roots: []SpanNode{
		span("submit", "", "task.submit", "host", "producer", ok, 12, 12, map[string]string{"host": hostA}),
		span("claim", "", "task.claim", "receiver", "consumer", unset, 11.9, 61, nil,
			span("open", claim, "key.open", "receiver", "client", unset, 11.89, 350, nil),
			span("run", claim, "task.run", "receiver", "internal", spanStatus(tracepb.Status_STATUS_CODE_ERROR, telemetry.ClassFailed), 11.88, 440000, map[string]string{"attempt": "2"},
				span("dispatch", run, "[other]", "plugin", "client", ok, 11.87, 1370, nil),
				span("exec", run, "agent.exec", "plugin", "internal", spanStatus(tracepb.Status_STATUS_CODE_ERROR, "something else"), 11.8, 437000, map[string]string{"exit.code": "1"}))),
	}}}, Spans: 6, Markers: []string{MarkerCycle, markerTruncated + capTraceSpans}, Read: Read{Bytes: 6210, Records: 10}, Next: 10, Complete: false}
	trace.Traces[0].Roots[1].Links = []string{trace.Traces[0].Roots[0].ID}
	trace.Traces[0].Roots[1].Children[1].Events = 3
	trace.Traces[0].Roots[1].Children[1].Children[0].Marker = MarkerFiltered
	empty := Forest{Run: fixtureWorking, Tenant: tenantA, Topic: telemetry.TraceTopic(tenantA), Traces: []Trace{}, Markers: []string{}, Complete: true}
	text := func(s string) *string { return &s }
	seq := func(n int64) *int64 { return &n }
	line := func(minutesAgo float64, attempt, seq int64, l Line) Line {
		l.At, l.Attempt, l.Receiver, l.Seq = at(minutesAgo), attempt, receiverSigner, seq
		return l
	}
	tail := Transcript{Run: fixtureFailed, Attempt: 2, First: seq(120), Lines: []Line{
		line(9.9, 1, 0, Line{Text: text(`{"type":"system","subtype":"init","model":"claude-sonnet-5-5"}`)}),
		line(9.8, 1, 64, Line{Text: text("Running the integration tests first.")}),
		line(9.0, 2, 120, Line{Text: text("Starting again: running the integration tests.")}),
		line(8.5, 2, 168, Line{Raw: true, Size: 4096}),
		line(8.4, 2, 4265, Line{Oversize: true, Size: 734003}),
		line(8.0, 2, 4266, Line{Text: text("origin\thttps://x-access-token:[redacted:Zm9vYmFyYmF6cXV4]@git.example.com/acme/app.git (fetch)")}),
		line(7.0, 2, 4400, Line{Text: text("")}),
		line(6.0, 2, 4401, Line{Text: text("dial tcp " + brokerDNS + ":9093: i/o timeout"), Truncated: true, Size: 20000}),
		line(3.1, 2, 24402, Line{Text: text("The tests cannot reach the broker from this allocation.")}),
	}, Prev: seq(0), Complete: true, Indexed: 11, Read: Read{Bytes: 4096, Records: 20}, Seek: Read{Bytes: 2048, Records: 9}}
	return map[string]any{
		"model.json":               model,
		"run-failed.json":          failed,
		"run-working.json":         working,
		"trace.json":               trace,
		"trace-empty.json":         empty,
		"transcript-tail.json":     tail,
		"transcript-reset.json":    Transcript{Run: fixtureWorking, Attempt: 2, First: seq(4096), Reset: true, Lines: []Line{}, Indexed: 40},
		"transcript-retry.json":    Transcript{Run: fixtureWorking, Attempt: 1, First: nil, Lines: []Line{}, Retry: true, Indexed: 0, Truncated: []string{capTranscriptScan}},
		"transcript-predates.json": Transcript{Run: fixtureWorking, Attempt: 1, First: seq(900), Lines: []Line{line(30, 1, 900, Line{Text: text("go test ./...")})}, PredatesLog: true, Next: seq(915), Indexed: 1},
		"transcript-missing.json":  Transcript{Run: fixtureWorking, Attempt: 0, First: nil, Lines: []Line{}, Missing: true, Complete: true},
		"transcript-caps.json":     Transcript{Run: fixtureWorking, Attempt: 1, First: seq(0), Lines: []Line{line(36, 1, 0, Line{Text: text("Starting with the failing build on this branch.")})}, Next: seq(48), Indexed: 300, Truncated: []string{capTranscriptBytes, capResponse}, Evicted: 1},
		// The server's policy and the page's, with the placeholder the nonce replaces, for the smoke's fixture server.
		"csp.json": map[string]string{"csp": csp, "page": pageCSP(noncePlaceholder), "placeholder": noncePlaceholder},
	}
}

// The page's fixtures are generated from the view's own JSON types, so the front-end tests read what the server
// encodes, absent fields included; a stale file fails here and -update rewrites it.
func TestWebFixtures(t *testing.T) {
	for name, value := range webFixtures() {
		encoded, err := json.Marshal(value, json.Deterministic(true), jsontext.WithIndent("  "))
		if err != nil {
			t.Fatal(err)
		}
		encoded = append(encoded, '\n')
		path := filepath.Join(webFixturesDir, name)
		if *updateFixtures {
			if err := os.WriteFile(path, encoded, 0o644); err != nil {
				t.Fatal(err)
			}
			continue
		}
		current, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(current, encoded) {
			t.Errorf("%s is stale: go test ./internal/view -run TestWebFixtures -update", path)
		}
	}
}
