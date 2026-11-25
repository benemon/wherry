package view

import (
	"bytes"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/wire"
)

// updateFixtures rewrites the page's fixtures from the view's JSON types instead of checking them.
var updateFixtures = flag.Bool("update", false, "rewrite internal/view/web/fixtures from the view's JSON types")

// webFixturesDir holds the JSON the page's unit tests and browser smoke run against, from this package's directory.
var webFixturesDir = filepath.Join("web", "fixtures")

const (
	fixtureWorking = "run-b04ce912bceb7864b872adaaa9c74f9c"
	fixtureFailed  = "run-49f960713214457a3e21d20ece496b85"
)

// webFixtures is every contract state the page handles, built from the types the API encodes so the page's tests
// read what the server writes: a gated receiver, stale and silent ones, a suspended custodian, lag error and
// unavailable, bounds that drop runs and ones that do not, a truncated runs list, model errors, topics missing,
// predating the log and in error, a run with a reason and its truncated flag, cancels, a start failure, a refused
// key request, outputs not returned, unknown storage, spans with an absent status, and each transcript state.
func webFixtures() map[string]any {
	now := time.Date(2026, 11, 24, 17, 51, 0, 0, time.UTC)
	at := func(minutesAgo float64) time.Time { return now.Add(-time.Duration(minutesAgo * float64(time.Minute))) }
	s3 := &Storage{Endpoint: "https://s3.example.com", Bucket: "wherry-runs"}
	key := "k3JvQmFzZTY0VXJsS2V5SWRfNDNfY2hhcnNfbG9uZ18"
	runs := []RunSummary{
		{ID: fixtureWorking, Tenant: "ben", Host: "mbp", Pool: "nomad", Receiver: "nomad-1", Agent: "claude", Model: "claude-fable-5-1", Attempt: 1, Stage: "working", State: string(wire.TaskStateWorking), Storage: s3, Admitted: at(37), First: at(37), Last: at(1)},
		{ID: fixtureFailed, Tenant: "ben", Host: "mbp", Pool: "nomad", Receiver: "nomad-1", Agent: "claude", Model: "claude-sonnet-5-5", Attempt: 2, Stage: "terminal", State: string(wire.TaskStateFailed), Class: "failed", Terminal: true, StorageUnknown: true, Cancelled: true, Admitted: at(12), First: at(12), Last: at(3), Outputs: 1, Truncated: []string{"run_events"}},
		{ID: "run-1111111111111111111111111111111a", Tenant: "ben", Host: "mini", Pool: "openshift", Receiver: "openshift-1", Agent: "codex", Attempt: 1, Stage: "terminal", State: string(wire.TaskStateFailed), Class: "start-failure:dispatch", Terminal: true, Storage: s3, Admitted: at(130), First: at(130), Last: at(125)},
		{ID: "run-2222222222222222222222222222222b", Tenant: "ben", Host: "mbp", Pool: "systemd", Receiver: "systemd-1", Agent: "claude", Model: "claude-fable-5-1", Attempt: 1, Stage: "terminal", State: string(wire.TaskStateCompleted), Terminal: true, Storage: s3, Admitted: at(50), First: at(50), Last: at(40), Outputs: 2},
		{ID: "run-3333333333333333333333333333333c", Tenant: "ops", Host: "ci", Pool: "nomad", Receiver: "nomad-1", Agent: "claude", Attempt: 1, Stage: "terminal", State: string(wire.TaskStateCanceled), Class: "canceled", Terminal: true, Storage: s3, Cancelled: true, Admitted: at(300), First: at(300), Last: at(290)},
		{ID: "run-4444444444444444444444444444444d", Tenant: "ops", Pool: "nomad", Receiver: "nomad-1", Attempt: 1, Stage: "rejected", State: string(wire.TaskStateRejected), Class: "rejected", Terminal: true, Storage: s3, Admitted: at(400), First: at(400), Last: at(400)},
		{ID: "run-5555555555555555555555555555555e", Tenant: "ben", Host: "mbp", Pool: "nomad", Receiver: "nomad-1", Agent: "claude", Attempt: 1, Stage: "released", State: string(wire.TaskStateWorking), Storage: s3, Admitted: at(700), First: at(700), Last: at(690)},
	}
	model := Snapshot{
		Version: 7, Now: now, WindowStart: now.Add(-24 * time.Hour), Rebuilt: true, RebuiltAt: now.Add(-3 * time.Hour),
		Brokers: []Broker{{ID: 0, Host: "10.0.0.17", Port: 9094, Controller: true, Listeners: []string{"LAN-9094://10.0.0.17:9094", "TLS-9093://wherry-kafka-0.svc:9093"}}},
		Pools: []Pool{
			{Name: "nomad", Receivers: []string{"nomad-1", "nomad-2"}, Tenants: []string{"ben", "ops"}, Custodian: "nomad-1", Storage: s3, DeliveryLimit: 5, Group: Group{State: "Stable", Members: 2}, Lag: Lag{Status: "ok", Records: 7, At: at(0.5)}, Runs: 5, InProgress: 1, ByState: map[string]int{string(wire.TaskStateWorking): 2, string(wire.TaskStateFailed): 1, string(wire.TaskStateCanceled): 1, string(wire.TaskStateRejected): 1}, ByClass: map[string]int{"failed": 1, "canceled": 1, "rejected": 1}, NoTerminal: 1, Agents: []string{"claude", "codex"}},
			{Name: "openshift", Receivers: []string{"openshift-1"}, Tenants: []string{"ben"}, Storage: s3, DeliveryLimit: 3, Group: Group{State: "Empty"}, Lag: Lag{Status: "error", Error: "timeout", At: at(1)}, Runs: 1, ByState: map[string]int{string(wire.TaskStateFailed): 1}, ByClass: map[string]int{"start-failure:dispatch": 1}, Agents: []string{"codex"}},
			{Name: "systemd", Suspended: true, Receivers: []string{"systemd-1"}, Tenants: []string{"ben"}, Custodian: "systemd-1", Storage: s3, DeliveryLimit: 5, Group: Group{State: "Stable", Members: 1}, Lag: Lag{Status: "unavailable", At: at(1)}, Runs: 1, ByState: map[string]int{string(wire.TaskStateCompleted): 1}, ByClass: map[string]int{}, Agents: []string{"claude"}},
		},
		Receivers: []Receiver{
			{Name: "nomad-1", Pool: "nomad", Status: "live", Capacity: 3, InFlight: 2, Gated: wire.GatedDraining, Offers: []Offer{{Profile: "claude", Agent: "claude", Replicas: 2, InFlight: 1}, {Profile: "codex", Agent: "codex", Replicas: 1}}, LastAdvertised: at(0.3), Profiles: []string{"claude", "codex"}, Runs: 5, ByState: map[string]int{string(wire.TaskStateWorking): 2, string(wire.TaskStateFailed): 1, string(wire.TaskStateCanceled): 1, string(wire.TaskStateRejected): 1}, ByClass: map[string]int{"failed": 1, "canceled": 1, "rejected": 1},
				Held: []HeldRun{{Run: fixtureWorking, Attempt: 1, Stage: "working", Source: "wherry.receiver.nomad-1.progress"}}, Stale: []HeldRun{{Run: "run-5555555555555555555555555555555e", Attempt: 1, Stage: "working", Stale: true, Source: "wherry.receiver.nomad-1.progress"}}, Unidentified: 1},
			{Name: "nomad-2", Pool: "nomad", Status: "ended", Ended: true, Offers: []Offer{}, Profiles: []string{}, ByState: map[string]int{}, ByClass: map[string]int{}, Held: []HeldRun{}, Stale: []HeldRun{}},
			{Name: "openshift-1", Pool: "openshift", Status: "stale", Capacity: 2, Offers: []Offer{{Profile: "codex", Agent: "codex", Replicas: 2}}, LastAdvertised: at(9), Profiles: []string{"codex"}, Runs: 1, ByState: map[string]int{string(wire.TaskStateFailed): 1}, ByClass: map[string]int{"start-failure:dispatch": 1}, Held: []HeldRun{}, Stale: []HeldRun{}},
			{Name: "systemd-1", Pool: "systemd", Status: "silent", Offers: []Offer{}, Profiles: []string{"claude"}, Runs: 1, ByState: map[string]int{string(wire.TaskStateCompleted): 1}, ByClass: map[string]int{}, Held: []HeldRun{}, Stale: []HeldRun{}, Unidentified: 2},
		},
		Custodians: []Custodian{
			{Pool: "nomad", Receiver: "nomad-1", Status: "live", KeyID: key, Reason: "rotation", At: at(180), RotatedFor: 3, Suspended: true, Signer: "wherry.rcv.nomad-1"},
			{Pool: "systemd", Receiver: "systemd-1", Status: "live", KeyID: key, Reason: "rotation", At: at(600), RotatedFor: 1, Signer: "wherry.rcv.systemd-1"},
		},
		Hosts: []Host{
			{Tenant: "ben", Name: "mbp", Runs: 4, ByState: map[string]int{string(wire.TaskStateWorking): 2, string(wire.TaskStateFailed): 1, string(wire.TaskStateCompleted): 1}},
			{Tenant: "ben", Name: "mini", Ended: true, Runs: 1, ByState: map[string]int{string(wire.TaskStateFailed): 1}},
			{Tenant: "ops", Name: "ci", Runs: 1, ByState: map[string]int{string(wire.TaskStateCanceled): 1}},
		},
		Tenants: []Tenant{{Name: "ben", Pools: []string{"nomad", "openshift", "systemd"}, Hosts: []string{"mbp", "mini"}, Runs: 5}, {Name: "ops", Pools: []string{"nomad"}, Hosts: []string{"ci"}, Runs: 2}},
		Storage: []StorageNode{{Endpoint: s3.Endpoint, Bucket: s3.Bucket, Pools: []string{"nomad", "openshift", "systemd"}, Runs: 7, Outputs: 3, Bytes: 3_560_000_000, Unreturned: 1, FailedRuns: 2}},
		Runs:    runs, RunsTotal: len(runs), Truncated: true,
		Topics: []Topic{
			{Name: "wherry.operator", Kind: "operator", Next: 9120, Start: 0, End: 9120},
			{Name: "wherry.pool.nomad.events", Kind: "events", Next: 410000, Start: 120000, End: 410000, PredatesLog: true},
			{Name: "wherry.tenant.ops", Kind: "feed", Missing: true},
			{Name: "wherry.receiver.systemd-1.progress", Kind: "progress", Error: "timeout"},
		},
		Counts: map[string]int64{"duplicates": 159, "pending": 0, "retracted": 2},
		Bounds: []BoundHit{{Name: "runs", Hits: 3}, {Name: "transcript_scan", Hits: 1}},
		Errors: []string{"timeout"},
	}
	failed := RunDetail{
		RunSummary: runs[1], Hash: "sha256:3f1c", Reason: "agent exited with status 1: integration tests could not reach [redacted-url]",
		Echoes: []Echo{{Receiver: "nomad-1", Host: "mbp", Pool: "nomad", Agent: "claude", Model: "claude-sonnet-5-5", At: at(12), Joined: true, Signer: "wherry.rcv.nomad-1"}},
		Attempts: []Attempt{
			{Number: 1, Receiver: "nomad-1", TaskTopic: "wherry.pool.nomad.tasks.claude", Events: []Event{{Kind: "claimed", State: string(wire.TaskStateWorking), EventID: "event-e5cd77d6aa2a0e825638ae87bb3b49b1", Receiver: "nomad-1", At: at(11.9), Signer: "wherry.rcv.nomad-1"}},
				Decision: &Decision{Custodian: "nomad-1", Reason: wire.ClaimUnchanged, Served: true, At: at(11.8), Signer: "wherry.rcv.nomad-1"}, StartFailure: &StartFailure{Profile: "claude", Agent: "claude", Class: "dispatch", At: at(11.5), Signer: "wherry.rcv.nomad-1"}},
			{Number: 2, Receiver: "nomad-1", TaskTopic: "wherry.pool.nomad.tasks.claude", TranscriptChunks: 11, Events: []Event{
				{Kind: "claimed", State: string(wire.TaskStateWorking), EventID: "event-0f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c", Receiver: "nomad-1", At: at(10), Signer: "wherry.rcv.nomad-1"},
				{Kind: "started", State: string(wire.TaskStateWorking), EventID: "event-1f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c", Receiver: "nomad-1", At: at(9.9), Signer: "wherry.rcv.nomad-1"},
				{Kind: "working", State: string(wire.TaskStateWorking), EventID: "event-2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c", Receiver: "nomad-1", At: at(8), Message: "running make integration against [redacted-url]", Signer: "wherry.rcv.nomad-1"},
				{Kind: "terminal", State: string(wire.TaskStateFailed), EventID: "event-3f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c", Receiver: "nomad-1", At: at(3), Class: "failed", Message: "agent exited with status 1: integration tests could not reach [redacted-url]", Truncated: true, Signer: "wherry.rcv.nomad-1"}}},
		},
		Opens: []Open{
			{Attempt: 1, Receiver: "nomad-1", At: at(11.85), Signer: "wherry.rcv.nomad-1", Reason: wire.RefusedPoolSuspended},
			{Attempt: 2, Receiver: "nomad-1", At: at(9.95), Signer: "wherry.rcv.nomad-1", Answer: "wrap", Custodian: "nomad-1", AnsweredAt: at(9.94), AnswerSigner: "wherry.rcv.nomad-1"},
		},
		Cancels: []Cancel{{Host: "mbp", At: at(4), Signer: "wherry.tnt.ben.mbp"}},
		Result: &Result{Receiver: "nomad-1", Attempt: 2, State: string(wire.TaskStateFailed), At: at(3), Profile: "claude",
			Outputs: []Output{{Name: "test-output.txt", Size: 18342, SHA256: "7a1d0c55e9b24f3a8c6e2d1b0f9a8e7d6c5b4a39281706f5e4d3c2b1a0f9e8d7"}}, S3Outputs: []Output{{Name: "reports/integration/log.txt", Size: 9_000_000, SHA256: "0a1d0c55e9b24f3a8c6e2d1b0f9a8e7d6c5b4a39281706f5e4d3c2b1a0f9e8d7"}},
			NotReturned: []Unreturned{{Name: "reports/integration/report.md", Size: 2048, Reason: "missing"}, {Name: "(invalid name)", Reason: "invalid name"}}, ReplySize: 0, Artifacts: 2, Signer: "wherry.rcv.nomad-1"},
		Path: []PathStep{{Step: "admitted", Actor: "mbp > nomad-1", At: at(12)}, {Step: "terminal", Actor: "nomad-1", Attempt: 2, At: at(3), Detail: "failed failed"}},
	}
	working := RunDetail{
		RunSummary: runs[0], Hash: "sha256:9b2e",
		Echoes: []Echo{{Receiver: "nomad-1", Host: "mbp", Pool: "nomad", Agent: "claude", Model: "claude-fable-5-1", At: at(37), Joined: true, Signer: "wherry.rcv.nomad-1"}},
		Attempts: []Attempt{{Number: 1, Receiver: "nomad-1", Events: []Event{
			{Kind: "claimed", State: string(wire.TaskStateWorking), EventID: "event-4f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c", Receiver: "nomad-1", At: at(36.9), Signer: "wherry.rcv.nomad-1"},
			{Kind: "started", State: string(wire.TaskStateWorking), EventID: "event-5f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c", Receiver: "nomad-1", At: at(36.8), Signer: "wherry.rcv.nomad-1"},
			{Kind: "working", State: string(wire.TaskStateWorking), EventID: "event-6f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c", Receiver: "nomad-1", At: at(30), Message: "go test ./... passing so far", Signer: "wherry.rcv.nomad-1"}},
			Decision: &Decision{Custodian: "nomad-1", Reason: wire.ClaimUnchanged, Served: true, At: at(36.85), Signer: "wherry.rcv.nomad-1"}}},
		Opens:   []Open{{Attempt: 1, Receiver: "nomad-1", At: at(36.88), Signer: "wherry.rcv.nomad-1", Answer: "wrap", Custodian: "nomad-1", AnsweredAt: at(36.87), AnswerSigner: "wherry.rcv.nomad-1"}},
		Cancels: []Cancel{},
		Path:    []PathStep{{Step: "admitted", Actor: "mbp > nomad-1", At: at(37)}},
	}
	span := func(id, parent, name, service, kind, status string, startMinutesAgo, ms float64, attrs map[string]string, children ...SpanNode) SpanNode {
		start := at(startMinutesAgo)
		if attrs == nil {
			attrs = map[string]string{}
		}
		if children == nil {
			children = []SpanNode{}
		}
		return SpanNode{ID: id, Parent: parent, Name: name, Service: service, Kind: kind, Start: start, End: start.Add(time.Duration(ms * float64(time.Millisecond))), DurationMS: ms, Status: status, Attributes: attrs, Children: children}
	}
	trace := Forest{Run: fixtureFailed, Tenant: "ben", Topic: "wherry.traces.ben", Traces: []Trace{{ID: "4bf92f3577b34da6a3ce929d0e0e4736", Roots: []SpanNode{
		span("0000000000000001", "", "task.submit", "host", "producer", "ok", 12, 12, map[string]string{"host": "mbp"}),
		span("0000000000000002", "", "task.claim", "receiver", "consumer", "", 11.9, 61, nil,
			span("0000000000000003", "0000000000000002", "key.open", "receiver", "client", "", 11.89, 350, nil),
			span("0000000000000004", "0000000000000002", "task.run", "receiver", "internal", "error", 11.88, 440000, map[string]string{"attempt": "2"},
				span("0000000000000005", "0000000000000004", "[other]", "plugin", "client", "ok", 11.87, 1370, nil),
				span("0000000000000006", "0000000000000004", "agent.exec", "plugin", "internal", "error", 11.8, 437000, map[string]string{"exit.code": "1"}))),
	}}}, Spans: 6, Markers: []string{MarkerCycle, markerTruncated + capTraceSpans}, Read: Read{Bytes: 6210, Records: 10}, Next: 10, Complete: false}
	trace.Traces[0].Roots[1].Links = []string{"0000000000000001"}
	trace.Traces[0].Roots[1].Children[1].Events = 3
	trace.Traces[0].Roots[1].Children[1].Children[0].Marker = MarkerFiltered
	empty := Forest{Run: fixtureWorking, Tenant: "ben", Topic: "wherry.traces.ben", Traces: []Trace{}, Markers: []string{}, Complete: true}
	text := func(s string) *string { return &s }
	seq := func(n int64) *int64 { return &n }
	line := func(minutesAgo float64, attempt, seq int64, l Line) Line {
		l.At, l.Attempt, l.Receiver, l.Seq = at(minutesAgo), attempt, "wherry.rcv.nomad-1", seq
		return l
	}
	tail := Transcript{Run: fixtureFailed, Attempt: 2, First: seq(120), Lines: []Line{
		line(9.9, 1, 0, Line{Text: text(`{"type":"system","subtype":"init","model":"claude-sonnet-5-5"}`)}),
		line(9.8, 1, 64, Line{Text: text("Running the integration tests first.")}),
		line(9.0, 2, 120, Line{Text: text("Starting again: running the integration tests.")}),
		line(8.5, 2, 168, Line{Raw: true, Size: 4096}),
		line(8.4, 2, 4265, Line{Oversize: true, Size: 734003}),
		line(8.0, 2, 4266, Line{Text: text("origin\thttps://x-access-token:[redacted:Zm9vYmFyYmF6cXV4]@git.example.com/ben/wherry.git (fetch)")}),
		line(7.0, 2, 4400, Line{Text: text("")}),
		line(6.0, 2, 4401, Line{Text: text("dial tcp 10.0.0.17:9094: i/o timeout"), Truncated: true, Size: 20000}),
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
