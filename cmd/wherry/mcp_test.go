package main

import (
	"bytes"
	"cmp"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// mcpSession connects an MCP client to newMCPServer over in-memory transports; progress counts notifications received.
func mcpSession(t *testing.T, connect func() (*busclient.Host, error), progress *atomic.Int64) *mcp.ClientSession {
	t.Helper()
	return mcpSessionReading(t, connect, progress, inputOpener(t.TempDir(), busclient.Config{}))
}

// testLog writes the MCP server's log to the test's, so a failure the tool reports by class alone shows its cause.
type testLog struct{ t *testing.T }

func (l testLog) Write(p []byte) (int, error) {
	l.t.Log(strings.TrimSuffix(string(p), "\n"))
	return len(p), nil
}

// mcpSessionReading is mcpSession with submit opening its inputs through openInput.
func mcpSessionReading(t *testing.T, connect func() (*busclient.Host, error), progress *atomic.Int64, openInput func(string) (*os.File, error)) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := newMCPServer(connect, inputReader(openInput), openInput, func(id string) string { return "wherry wait " + id }, log.New(testLog{t}, "mcp server: ", 0)).Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { serverSession.Close() })
	options := &mcp.ClientOptions{}
	if progress != nil {
		options.ProgressNotificationHandler = func(context.Context, *mcp.ProgressNotificationClientRequest) { progress.Add(1) }
	}
	session, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "v0"}, options).Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Close() })
	return session
}

// callTool calls a tool and decodes its structured result into out; it fails the test on a tool error.
func callTool(t *testing.T, ctx context.Context, session *mcp.ClientSession, name string, arguments any, out any) {
	t.Helper()
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: arguments})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("%s: tool error %s", name, toolText(result))
	}
	encoded, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, out); err != nil {
		t.Fatalf("%s: %v in %s", name, err, encoded)
	}
}

// callToolError calls a tool that must fail and returns its structured error.
func callToolError(t *testing.T, ctx context.Context, session *mcp.ClientSession, name string, arguments any) toolError {
	t.Helper()
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: arguments})
	if err != nil {
		t.Fatal(err)
	}
	if !result.IsError {
		t.Fatalf("%s: succeeded, want a tool error", name)
	}
	var failure toolError
	if err := json.Unmarshal([]byte(toolText(result)), &failure, json.RejectUnknownMembers(true)); err != nil || failure.Code == "" || failure.Message == "" {
		t.Fatalf("%s: error %q is not a structured error: %v", name, toolText(result), err)
	}
	return failure
}

func toolText(result *mcp.CallToolResult) string {
	var texts []string
	for _, content := range result.Content {
		if text, ok := content.(*mcp.TextContent); ok {
			texts = append(texts, text.Text)
		}
	}
	return strings.Join(texts, "\n")
}

var submitArguments = map[string]any{"prompt": "fix the bug", "agent": "claude", "selector": []any{
	map[string]any{"key": "runtime", "operator": "In", "values": []string{"exec"}},
	map[string]any{"key": "region", "operator": "NotIn", "values": []string{"us"}},
	map[string]any{"key": "gpu", "operator": "DoesNotExist"},
}}

func TestMCPToolsList(t *testing.T) {
	b := newBus(t)
	session := mcpSession(t, b.connect(t), nil)
	if version := session.InitializeResult().ProtocolVersion; version != "2026-07-28" {
		t.Fatalf("negotiated protocol %q, want 2026-07-28", version)
	}
	tools, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	schemas := map[string]*mcp.Tool{}
	for _, tool := range tools.Tools {
		schemas[tool.Name] = tool
	}
	names := slices.Sorted(func(yield func(string) bool) {
		for name := range schemas {
			if !yield(name) {
				return
			}
		}
	})
	if want := []string{"cancel", "cleanup", "events", "logs", "receivers", "status", "submit", "wait"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("tools %v, want %v", names, want)
	}
	required := map[string][]string{
		"wait": {"id"}, "cancel": {"id"}, "cleanup": {"id"}, "events": {"id"}, "receivers": nil, "status": nil,
		"submit": nil, "logs": {"id"},
	}
	for name, tool := range schemas {
		if tool.Description == "" {
			t.Errorf("%s has no description", name)
		}
		for _, stale := range []string{"enrol", "after the user has sealed", "resealing", "sealed to it"} {
			if strings.Contains(tool.Description, stale) {
				t.Errorf("%s description still says %q", name, stale)
			}
		}
		for kind, schema := range map[string]any{"input": tool.InputSchema, "output": tool.OutputSchema} {
			encoded, err := json.Marshal(schema)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(encoded), "revoked") {
				t.Errorf("%s %s schema keeps a revocation field: %s", name, kind, encoded)
			}
			var decoded struct {
				Type       string         `json:"type"`
				Properties map[string]any `json:"properties"`
				Required   []string       `json:"required"`
			}
			if err := json.Unmarshal(encoded, &decoded); err != nil || decoded.Type != "object" {
				t.Fatalf("%s %s schema %s", name, kind, encoded)
			}
			if kind == "input" {
				got := slices.Sorted(slices.Values(decoded.Required))
				if len(got) == 0 {
					got = nil
				}
				if !reflect.DeepEqual(got, required[name]) {
					t.Errorf("%s input requires %v, want %v", name, got, required[name])
				}
			} else if len(decoded.Properties) == 0 {
				t.Errorf("%s output schema has no properties", name)
			}
		}
	}
	var wait struct {
		Properties map[string]any `json:"properties"`
	}
	encoded, _ := json.Marshal(schemas["wait"].OutputSchema)
	if err := json.Unmarshal(encoded, &wait); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"id", "state", "retry_after", "message", "artifacts"} {
		if _, ok := wait.Properties[field]; !ok {
			t.Errorf("wait output schema lacks %s", field)
		}
	}
}

func TestBoundsTakeATimeOrADurationAndDefault(t *testing.T) {
	b := newBus(t)
	session := mcpSession(t, b.connect(t), nil)
	tools, err := session.ListTools(b.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range tools.Tools {
		if tool.Name != "submit" {
			continue
		}
		encoded, _ := json.Marshal(tool.InputSchema)
		for field, want := range map[string]time.Duration{"task_started_by": busclient.DefaultStartBy, "task_ended_by": busclient.DefaultEndBy} {
			_, rest, _ := strings.Cut(string(encoded), `"`+field+", ")
			_, rest, _ = strings.Cut(rest, "; default ")
			stated, _, _ := strings.Cut(rest, `"`)
			if got, err := time.ParseDuration(stated); err != nil || got != want {
				t.Errorf("%s input schema states %s default %q, want %s: %s", tool.Name, field, stated, want, encoded)
			}
		}
	}
	for _, bad := range []string{"7200", "-1h", "soon", time.Now().Add(-time.Minute).Format(time.RFC3339)} {
		for _, field := range []string{"start_by", "end_by"} {
			if failure := callToolError(t, b.ctx, session, "submit", map[string]any{"prompt": "work", field: bad}); failure.Code != "INVALID_ARGUMENT" {
				t.Errorf("%s %q: %+v, want INVALID_ARGUMENT", field, bad, failure)
			}
		}
	}
	// Each bound takes either form: a duration from now, or an RFC 3339 time that arrives as whole seconds.
	at := time.Now().Add(time.Hour).Truncate(time.Second)
	near := func(lane, field string, got int64, want time.Time) {
		if got < want.Add(-5*time.Second).Unix() || got > want.Unix() {
			t.Errorf("%s %s is %s, want %s", lane, field, time.Unix(got, 0), want)
		}
	}
	for _, forms := range []struct{ startBy, endBy string }{{"5m", at.Format(time.RFC3339)}, {at.Format(time.RFC3339), "90m"}} {
		var custom submitOutput
		callTool(t, b.ctx, session, "submit", map[string]any{"prompt": "work", "start_by": forms.startBy, "end_by": forms.endBy}, &custom)
		var stdout, stderr bytes.Buffer
		if err := runSubmit(b.ctx, b.client, []string{"--prompt", "work", "--start-by", forms.startBy, "--end-by", forms.endBy}, &stdout, &stderr); err != nil {
			t.Fatal(err)
		}
		cli, _, _ := strings.Cut(strings.TrimPrefix(stdout.String(), "run_id="), " ")
		for lane, runID := range map[string]string{"MCP": custom.ID, "CLI": cli} {
			task := b.tasks(t, runID)[0].submission.Wherry
			if forms.startBy == "5m" {
				near(lane, "task_started_by", task.TaskStartedBy, time.Now().Add(5*time.Minute))
				if task.TaskEndedBy != at.Unix() {
					t.Errorf("%s end_by %s gave task_ended_by %d, want %d", lane, forms.endBy, task.TaskEndedBy, at.Unix())
				}
			} else {
				near(lane, "task_ended_by", task.TaskEndedBy, time.Now().Add(90*time.Minute))
				if task.TaskStartedBy != at.Unix() {
					t.Errorf("%s start_by %s gave task_started_by %d, want %d", lane, forms.startBy, task.TaskStartedBy, at.Unix())
				}
			}
		}
	}
	for _, flag := range []string{"--start-by", "--end-by"} {
		var stdout, stderr bytes.Buffer
		if err := runSubmit(b.ctx, b.client, []string{"--prompt", "work", flag, "soon"}, &stdout, &stderr); err == nil || !strings.HasPrefix(err.Error(), flag+": ") {
			t.Errorf("submit %s soon: %v, want it refused naming the flag", flag, err)
		}
	}
	before := time.Now()
	var submitted submitOutput
	callTool(t, b.ctx, session, "submit", map[string]any{"prompt": "work"}, &submitted)
	var stdout, stderr bytes.Buffer
	if err := runSubmit(b.ctx, b.client, []string{"--prompt", "work"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	after := time.Now()
	cli, _, _ := strings.Cut(strings.TrimPrefix(stdout.String(), "run_id="), " ")
	for lane, runID := range map[string]string{"MCP": submitted.ID, "CLI": cli} {
		task := b.tasks(t, runID)[0].submission.Wherry
		if task.TaskEndedBy < before.Add(2*time.Hour).Unix() || task.TaskEndedBy > after.Add(2*time.Hour).Unix() {
			t.Errorf("%s submit task_ended_by %v, want two hours out", lane, time.Unix(task.TaskEndedBy, 0))
		}
		if task.TaskStartedBy < before.Add(10*time.Minute).Unix() || task.TaskStartedBy > after.Add(10*time.Minute).Unix() {
			t.Errorf("%s submit task_started_by %v, want ten minutes out", lane, time.Unix(task.TaskStartedBy, 0))
		}
	}
}

func TestMCPSubmitWaitRoundTrip(t *testing.T) {
	b := newBus(t)
	ctx := b.ctx
	session := mcpSession(t, b.connect(t), nil)

	var receivers receiversOutput
	callTool(t, ctx, session, "receivers", map[string]any{}, &receivers)
	if len(receivers.Receivers) != 1 || receivers.Receivers[0].ReceiverID != "receiver-a" || receivers.Receivers[0].Pool != "pool-a" || receivers.Receivers[0].Status != "live" ||
		!reflect.DeepEqual(receivers.Receivers[0].Profiles, []busclient.Profile{{Name: "lab", Plugin: "nomad", Agent: "claude", InlineLimit: 49152,
			Credentials: []busclient.Credential{{Name: "model", Configured: true}, {Name: "github-token", Optional: true}}, Labels: map[string]string{"runtime": "exec"}, Ready: true}}) {
		t.Fatalf("receivers %#v", receivers)
	}
	var submitted submitOutput
	callTool(t, ctx, session, "submit", submitArguments, &submitted)
	if !strings.HasPrefix(submitted.ID, "run-") || submitted.Pool != "pool-a" {
		t.Fatalf("submit %#v", submitted)
	}
	var status statusOutput
	callTool(t, ctx, session, "status", map[string]any{"id": submitted.ID}, &status)
	if len(status.Runs) != 1 || status.Runs[0].ID != submitted.ID || status.Runs[0].Pool != "pool-a" || status.Runs[0].State != string(wire.TaskStateSubmitted) {
		t.Fatalf("status before a receiver reports %#v", status)
	}
	if _, err := time.Parse(time.RFC3339, status.Runs[0].Updated); err != nil {
		t.Fatalf("updated %q: %v", status.Runs[0].Updated, err)
	}

	waited := make(chan waitReply, 1)
	go func() {
		var reply waitReply
		result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "wait", Arguments: map[string]any{"id": submitted.ID}})
		if err == nil && !result.IsError {
			encoded, _ := json.Marshal(result.StructuredContent)
			json.Unmarshal(encoded, &reply)
		}
		waited <- reply
	}()

	// The receiver's side: claim the authorised task, then commit what admission and completion commit.
	admitted := b.tasks(t, submitted.ID)[0]
	if placement := admitted.submission.Wherry.Placement; placement.Agent != "claude" || !reflect.DeepEqual(placement.Selector, []wire.Requirement{
		{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}}, {Key: "region", Operator: wire.OpNotIn, Values: []string{"us"}}, {Key: "gpu", Operator: wire.OpDoesNotExist},
	}) {
		t.Fatalf("placement %#v", placement)
	}
	b.feed(t, submitted.ID, b.echo(t, admitted), b.status(t, admitted, "claimed-1", "claimed", now(wire.TaskStateWorking)))
	callTool(t, ctx, session, "status", map[string]any{"id": submitted.ID}, &status)
	if len(status.Runs) != 1 || status.Runs[0].State != string(wire.TaskStateWorking) {
		t.Fatalf("status after claim %#v", status)
	}
	b.feed(t, submitted.ID, b.result(t, admitted, []wire.Part{{Data: jsontext.Value(`{"answer":42}`), MediaType: "application/json"}}),
		b.status(t, admitted, "terminal-1", "terminal", now(wire.TaskStateCompleted)))

	reply := <-waited
	if reply.ID != submitted.ID || reply.State != string(wire.TaskStateCompleted) || reply.RetryAfter != 0 || len(reply.Artifacts) != 1 ||
		reply.Artifacts[0].ArtifactID != "result" || len(reply.Artifacts[0].Parts) != 1 ||
		!reflect.DeepEqual(reply.Artifacts[0].Parts[0].Data, map[string]any{"answer": float64(42)}) {
		t.Fatalf("wait %#v", reply)
	}
	var events eventsOutput
	callTool(t, ctx, session, "events", map[string]any{"id": submitted.ID}, &events)
	if len(events.Events) != 2 || events.Events[0].Kind != "claimed" || events.Events[1].State != string(wire.TaskStateCompleted) ||
		events.Events[1].Receiver != "receiver-a" {
		t.Fatalf("events %#v", events)
	}
}

func TestMCPToolCallsShareOneHost(t *testing.T) {
	b := newBus(t)
	ctx := b.ctx
	open := b.connect(t)
	var attempts, hosts atomic.Int64
	connect, closeHost := sharedHost(func() (*busclient.Host, error) {
		if attempts.Add(1) == 1 {
			return nil, errors.New("broker unreachable")
		}
		hosts.Add(1)
		return open()
	})
	defer closeHost()
	session := mcpSession(t, connect, nil)
	if failure := callToolError(t, ctx, session, "receivers", map[string]any{}); failure.Code != "UNAVAILABLE" {
		t.Fatalf("receivers before the host connects %#v, want UNAVAILABLE", failure)
	}
	answerCtx, stop := context.WithCancel(ctx)
	answered := make(chan struct{})
	go func() { defer close(answered); answer(answerCtx, connect, log.New(io.Discard, "", 0)) }()
	defer func() { stop(); <-answered }()

	var receivers receiversOutput
	callTool(t, ctx, session, "receivers", map[string]any{}, &receivers)
	var submitted submitOutput
	callTool(t, ctx, session, "submit", submitArguments, &submitted)
	waited := make(chan string, 1)
	go func() {
		result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "wait", Arguments: map[string]any{"id": submitted.ID}})
		if err != nil || result.IsError {
			waited <- fmt.Sprint(err, result)
			return
		}
		var reply waitReply
		encoded, _ := json.Marshal(result.StructuredContent)
		json.Unmarshal(encoded, &reply)
		waited <- reply.State
	}()
	admitted := b.tasks(t, submitted.ID)[0]
	b.feed(t, submitted.ID, b.echo(t, admitted), b.status(t, admitted, "claimed-1", "claimed", now(wire.TaskStateWorking)))
	var status statusOutput
	callTool(t, ctx, session, "status", map[string]any{"id": submitted.ID}, &status)
	callTool(t, ctx, session, "status", map[string]any{}, &status)
	b.feed(t, submitted.ID, b.result(t, admitted, []wire.Part{{Text: "done"}}), b.status(t, admitted, "terminal-1", "terminal", now(wire.TaskStateCompleted)))
	if state := <-waited; state != string(wire.TaskStateCompleted) {
		t.Fatalf("wait %s", state)
	}
	var events eventsOutput
	callTool(t, ctx, session, "events", map[string]any{"id": submitted.ID}, &events)
	if hosts.Load() != 1 {
		t.Fatalf("%d hosts opened for the tool calls and the answer loop, want 1", hosts.Load())
	}
}

func TestMCPStatusWithoutIDListsRuns(t *testing.T) {
	b := newBus(t)
	ctx := b.ctx
	session := mcpSession(t, b.connect(t), nil)
	var status statusOutput
	callTool(t, ctx, session, "status", map[string]any{}, &status)
	if status.Runs == nil || len(status.Runs) != 0 {
		t.Fatalf("status of no runs %#v, want an empty list", status)
	}
	working := b.admitted(t).submission.Wherry.Run
	var submitted submitOutput
	callTool(t, ctx, session, "submit", submitArguments, &submitted)
	callTool(t, ctx, session, "status", map[string]any{}, &status)
	if len(status.Runs) != 2 || status.Runs[0].ID != submitted.ID || status.Runs[0].State != string(wire.TaskStateSubmitted) ||
		status.Runs[1].ID != working || status.Runs[1].State != string(wire.TaskStateWorking) || status.Runs[1].Pool != "pool-a" {
		t.Fatalf("status %#v, want the submitted run then the working one", status)
	}
	for _, run := range status.Runs {
		if _, err := time.Parse(time.RFC3339, run.Updated); err != nil {
			t.Fatalf("run %s updated %q: %v", run.ID, run.Updated, err)
		}
	}
}

func TestMCPWaitBoundedRetry(t *testing.T) {
	t.Parallel()
	b := newBus(t)
	ctx := b.ctx
	var progress atomic.Int64
	session := mcpSession(t, b.connect(t), &progress)
	var submitted submitOutput
	callTool(t, ctx, session, "submit", submitArguments, &submitted)

	waitCtx, cancel := context.WithTimeout(context.Background(), waitBound+15*time.Second)
	defer cancel()
	start := time.Now()
	params := &mcp.CallToolParams{Name: "wait", Arguments: map[string]any{"id": submitted.ID}}
	params.SetProgressToken("wait-1")
	result, err := session.CallTool(waitCtx, params)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("wait after %s: %v", elapsed, err)
	}
	if result.IsError {
		t.Fatalf("wait error %s", toolText(result))
	}
	var reply map[string]any
	encoded, _ := json.Marshal(result.StructuredContent)
	if err := json.Unmarshal(encoded, &reply); err != nil {
		t.Fatal(err)
	}
	if elapsed < waitBound || elapsed > waitBound+10*time.Second {
		t.Fatalf("wait returned after %s, want the %s bound", elapsed, waitBound)
	}
	if !reflect.DeepEqual(reply, map[string]any{"id": submitted.ID, "state": string(wire.TaskStateSubmitted), "retry_after": float64(retryAfter)}) {
		t.Fatalf("wait %s", encoded)
	}
	if progress.Load() == 0 {
		t.Fatal("no progress notification during wait")
	}
}

func TestMCPWaitAndStatusReportAnUnclaimedRunPastItsStartBy(t *testing.T) {
	b := newBus(t)
	ctx := b.ctx
	session := mcpSession(t, b.connect(t), nil)
	var submitted submitOutput
	callTool(t, ctx, session, "submit", map[string]any{"prompt": "fix the bug", "start_by": "2s"}, &submitted)
	start := time.Now()
	var reply map[string]any
	callTool(t, ctx, session, "wait", map[string]any{"id": submitted.ID}, &reply)
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("wait returned after %s, want soon after the 2s task_started_by", elapsed)
	}
	message, _ := reply["message"].(string)
	if !strings.HasPrefix(message, "not started by task_started_by (") || !strings.HasSuffix(message, "): no receiver claimed the task") ||
		!reflect.DeepEqual(reply, map[string]any{"id": submitted.ID, "state": string(wire.TaskStateSubmitted), "message": message}) {
		t.Fatalf("wait %#v, want the passed task_started_by without retry_after", reply)
	}
	var status statusOutput
	callTool(t, ctx, session, "status", map[string]any{"id": submitted.ID}, &status)
	if len(status.Runs) != 1 || status.Runs[0].State != string(wire.TaskStateSubmitted) || status.Runs[0].Message != message {
		t.Fatalf("status %#v, want %q", status, message)
	}
}

func TestMCPCancel(t *testing.T) {
	t.Run("cancel", func(t *testing.T) {
		b := newBus(t)
		admitted := b.admitted(t)
		ctx, runID := b.ctx, admitted.submission.Wherry.Run
		session := mcpSession(t, b.connect(t), nil)
		var cancelled cancelOutput
		callTool(t, ctx, session, "cancel", map[string]any{"id": runID}, &cancelled)
		if cancelled.ID != runID {
			t.Fatalf("cancel %#v", cancelled)
		}
		b.cancelRun(t, admitted)
		var reply waitReply
		callTool(t, ctx, session, "wait", map[string]any{"id": runID}, &reply)
		if reply.State != string(wire.TaskStateCanceled) {
			t.Fatalf("wait after cancel %#v", reply)
		}
		var status statusOutput
		callTool(t, ctx, session, "status", map[string]any{"id": runID}, &status)
		if len(status.Runs) != 1 || status.Runs[0].State != string(wire.TaskStateCanceled) {
			t.Fatalf("status after cancel %#v", status)
		}
		var events eventsOutput
		callTool(t, ctx, session, "events", map[string]any{"id": runID}, &events)
		if len(events.Events) != 2 || events.Events[1].State != string(wire.TaskStateCanceled) {
			t.Fatalf("events %#v", events)
		}
		if failure := callToolError(t, ctx, session, "cancel", map[string]any{"id": "run-missing"}); failure.Code != "RUN_NOT_FOUND" {
			t.Fatalf("cancel of an unknown run %#v", failure)
		}
	})
}

func TestMCPSubmitNamesTheInvalidRule(t *testing.T) {
	b := newBus(t)
	ctx := b.ctx
	session := mcpSession(t, b.connect(t), nil)
	for rule, requirement := range map[string]map[string]any{
		"invalid requirement key":                             {"key": "wherry.io/owner", "operator": "In", "values": []any{"x"}},
		"In and NotIn requirements need values":               {"key": "runtime", "operator": "NotIn", "values": []any{}},
		"requirement values must be valid and unique":         {"key": "runtime", "operator": "In", "values": []any{""}},
		"Exists and DoesNotExist requirements take no values": {"key": "gpu", "operator": "Exists", "values": []any{"a100"}},
		"unknown operator":                                    {"key": "gpu", "operator": "Gt", "values": []any{"1"}},
	} {
		failure := callToolError(t, ctx, session, "submit", map[string]any{"prompt": "x", "selector": []any{requirement}})
		if failure.Code != "INVALID_ARGUMENT" || !strings.Contains(failure.Message, rule) {
			t.Fatalf("error %#v, want INVALID_ARGUMENT naming %q", failure, rule)
		}
	}
	failure := callToolError(t, ctx, session, "submit", map[string]any{"prompt": "x", "agent": "claude", "model": "sonnet"})
	if failure.Code != "INVALID_ARGUMENT" || !strings.Contains(failure.Message, "model sonnet is not in agent claude's models in pool pool-a") {
		t.Fatalf("error %#v, want INVALID_ARGUMENT naming the model, agent and pool", failure)
	}
}

func TestMCPErrorsCarryNoSecrets(t *testing.T) {
	const canary = "s3cr3t-canary"
	b := newBus(t)
	ctx := b.ctx
	// A missing bundle under a canary directory: Connect's error names the path, as it would a real one.
	broken := busclient.Config{Bundle: filepath.Join(t.TempDir(), canary, "bundle.yaml"), HostKey: filepath.Join(t.TempDir(), "host.key"), StateDir: t.TempDir()}
	if _, err := busclient.Connect(broken, log.New(io.Discard, "", 0)); err == nil || !strings.Contains(err.Error(), canary) {
		t.Fatalf("precondition: the connect error %v must carry the canary", err)
	}
	working := b.connect(t)
	secrets := []string{canary, "s3cr3t-password", string(b.hostKey), credentialText}
	for name, call := range map[string]struct {
		connect   func() (*busclient.Host, error)
		tool      string
		arguments map[string]any
		code      string
	}{
		"connect failure": {func() (*busclient.Host, error) { return busclient.Connect(broken, log.New(io.Discard, "", 0)) }, "status", map[string]any{"id": "run-a"}, "UNAVAILABLE"},
		"git userinfo": {working, "submit", map[string]any{
			"prompt": "x", "selector": []any{},
			"git": map[string]any{"remote": "https://user:s3cr3t-password@git.example.test/org/repo.git", "ref": "main"},
		}, "INVALID_ARGUMENT"},
		"unknown run": {working, "status", map[string]any{"id": "run-missing"}, "RUN_NOT_FOUND"},
		"invalid selector key": {working, "submit", map[string]any{
			"prompt": "x", "selector": []any{map[string]any{"key": "s3cr3t-password!", "operator": "Exists"}},
		}, "INVALID_ARGUMENT"},
		"no matching receiver": {working, "submit", map[string]any{
			"prompt": "x", "selector": []any{map[string]any{"key": "runtime", "operator": "In", "values": []any{"podman"}}},
		}, "NO_MATCHING_RECEIVER"},
	} {
		t.Run(name, func(t *testing.T) {
			session := mcpSession(t, call.connect, nil)
			result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: call.tool, Arguments: call.arguments})
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			for _, secret := range secrets {
				if strings.Contains(string(encoded), secret) {
					t.Fatalf("%s result carries %q: %s", call.tool, secret, encoded)
				}
			}
			if failure := callToolError(t, ctx, session, call.tool, call.arguments); failure.Code != call.code {
				t.Fatalf("%s error %#v, want %s", call.tool, failure, call.code)
			}
		})
	}
}

func TestMCPServerAnswersSealRequests(t *testing.T) {
	if values := answeredSeal(t, newBus(t)); string(values.Values["model"]) != credentialText {
		t.Fatalf("sealed %q, want the configured credential", values.Values["model"])
	}
}

// answeredSeal is the seal the MCP server's answering writes for a request for the model credential of an admitted run.
func answeredSeal(t *testing.T, b *bus) signed.SealValues {
	t.Helper()
	admitted := b.admitted(t)
	runID := admitted.submission.Wherry.Run
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	request := wire.SealRequest{ReceiverBinding: b.binding(admitted), Key: jose.JSONWebKey{Key: &key.PublicKey}, KeyID: signed.SealKeyID(&key.PublicKey),
		Credentials: []string{"model"}, ExpiryMS: time.Now().Add(time.Minute).UnixMilli()}
	b.feed(t, runID, b.sign(t, &request))
	ctx, stop := context.WithCancel(b.ctx)
	done := make(chan struct{})
	go func() { defer close(done); answer(ctx, b.connect(t), log.New(io.Discard, "", 0)) }()
	defer func() { stop(); <-done }()
	binding := signed.SealBinding{Tenant: request.Tenant, Pool: request.Pool, Receiver: request.Receiver, KeyID: request.KeyID, Run: request.Run,
		Attempt: request.Attempt, Credentials: request.Credentials, ExpiryMS: request.ExpiryMS}
	for {
		records, err := readTopic(b.ctx, b.receiver, admin.HostTopic("tenant-a", "host-a"))
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range records {
			if values, err := signed.OpenSeal(record.Value, binding, key, time.Now()); err == nil {
				return values
			}
		}
		if b.ctx.Err() != nil {
			t.Fatalf("the server never answered; logs:\n%s", b.logs.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// redactionMarker is the marker receiver-a writes for the run's value name, from the key in this host's record of the run.
func (b *bus) redactionMarker(t *testing.T, runID, name string) string {
	t.Helper()
	encoded, err := os.ReadFile(filepath.Join(b.stateDir, "runs", runID+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var record struct {
		RedactionKey []byte `json:"redaction_key"`
	}
	if err := json.Unmarshal(encoded, &record); err != nil {
		t.Fatal(err)
	}
	return signed.RedactionMarker(record.RedactionKey, name)
}

// attemptBinding is receiver-a's binding for an admitted run attempt.
func (b *bus) attemptBinding(admitted task, attempt int64) wire.ReceiverBinding {
	binding := b.binding(admitted)
	binding.Attempt = attempt
	return binding
}

// chunk signs a transcript line of attempt at seq as receiver-a does.
func (b *bus) chunk(t *testing.T, admitted task, attempt, seq int64, part wire.Part) []byte {
	t.Helper()
	runID := admitted.submission.Wherry.Run
	part.MediaType = cmp.Or(part.MediaType, "application/x-ndjson")
	return b.sign(t, &wire.Chunk{
		ArtifactUpdate: wire.TaskArtifactUpdateEvent{TaskID: runID, ContextID: runID, Append: seq > 0, Artifact: wire.Artifact{
			ArtifactID: "transcript", Parts: []wire.Part{part}, Extensions: []string{wire.ExtensionURI},
		}},
		Wherry: wire.ChunkBinding{ReceiverBinding: b.attemptBinding(admitted, attempt), TaskHash: admitted.hash, Seq: seq},
	})
}

// transcriptBig is a line over the transcript page bound.
var transcriptBig = strings.Repeat("b", 300<<10)

// transcriptRun admits a run with a stale first attempt's line and a second attempt's lines, fed out
// of order and one twice, a CANCELED terminal under a third attempt, and a working status whose message carries a
// redaction marker.
func transcriptRun(t *testing.T) (*bus, string) {
	t.Helper()
	b := newBus(t)
	admitted := b.admitted(t)
	runID := admitted.submission.Wherry.Run
	marker := b.redactionMarker(t, runID, "model")
	working := now(wire.TaskStateWorking)
	working.Message = &wire.Message{MessageID: "msg-working", Role: wire.RoleAgent, Parts: []wire.Part{{Text: "using " + marker}}, Extensions: []string{wire.ExtensionURI}}
	canceled := now(wire.TaskStateCanceled)
	b.feed(t, runID,
		b.status(t, admitted, "working-1", "started", working),
		b.chunk(t, admitted, 1, 0, wire.Part{Text: "stale"}),
		b.chunk(t, admitted, 1, 70, wire.Part{Text: "stale past the second attempt's first line"}),
		b.chunk(t, admitted, 2, 10, wire.Part{Text: "second " + marker}),
		b.chunk(t, admitted, 2, 0, wire.Part{Text: `{"type":"start"}`}),
		b.chunk(t, admitted, 2, 20, wire.Part{Raw: wire.StdBytes{0xff, 0xfe}}),
		b.chunk(t, admitted, 2, 30, wire.Part{Raw: wire.StdBytes(`"q"`)}),
		b.chunk(t, admitted, 2, 35, wire.Part{Raw: wire.StdBytes{}}),
		b.chunk(t, admitted, 2, 10, wire.Part{Text: "second " + marker}),
		b.chunk(t, admitted, 2, 40, wire.Part{Data: jsontext.Value(`{"sha256":"` + hashOf("x") + `","size":600000,"unstored":"oversize"}`), MediaType: wire.ReferenceMediaType}),
		b.chunk(t, admitted, 2, 50, wire.Part{Text: transcriptBig}),
		b.chunk(t, admitted, 2, 60, wire.Part{Text: "tail"}),
		b.sign(t, &wire.RunStatus{StatusUpdate: wire.TaskStatusUpdateEvent{TaskID: runID, ContextID: runID, Status: canceled},
			Wherry: wire.StatusBinding{ReceiverBinding: b.attemptBinding(admitted, 3), EventID: runID + "/1/canceled", Kind: "terminal", TaskHash: admitted.hash}}),
	)
	return b, runID
}

// wait prints none of the transcript and writes no transcript.jsonl, nor does MCP wait, whose reply names none.
func TestWaitWritesNoTranscript(t *testing.T) {
	b, runID := transcriptRun(t)
	dir := filepath.Join(b.stateDir, "runs", runID, "outputs")
	var stdout, stderr bytes.Buffer
	err := runWait(b.ctx, b.client, []string{runID, "--timeout", "5s"}, &stdout, &stderr)
	if err == nil || err.Error() != "run ended in TASK_STATE_CANCELED" || stdout.Len() != 0 {
		t.Fatalf("wait = %v, stdout %q; want the state alone", err, stdout.String())
	}
	if _, err := os.Stat(filepath.Join(dir, "transcript.jsonl")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("wait wrote a transcript: %v", err)
	}
	result, err := mcpSession(t, b.connect(t), nil).CallTool(b.ctx, &mcp.CallToolParams{Name: "wait", Arguments: map[string]any{"id": runID}})
	if err != nil || result.IsError {
		t.Fatalf("MCP wait = %v, %v", result, err)
	}
	encoded, _ := json.Marshal(result.StructuredContent)
	if strings.Contains(string(encoded), "transcript") {
		t.Fatalf("MCP wait %s names a transcript", encoded)
	}
	if _, err := os.Stat(filepath.Join(dir, "transcript.jsonl")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("MCP wait wrote a transcript: %v", err)
	}
}

func TestMCPLogs(t *testing.T) {
	b, runID := transcriptRun(t)
	ctx := b.ctx
	session := mcpSession(t, b.connect(t), nil)
	var reply logsReply
	callTool(t, ctx, session, "logs", map[string]any{"id": runID}, &reply)
	want := logsReply{ID: runID, Attempt: 3, Total: 9, NextOffset: 6, Redacted: map[string]int{"model": 1}, Lines: []logsLine{
		{Seq: 0, Attempt: 2, Text: `{"type":"start"}`}, {Seq: 10, Attempt: 2, Text: "second [redacted:model]"}, {Seq: 20, Attempt: 2, RawBytes: 2},
		{Seq: 30, Attempt: 2, Text: `"q"`}, {Seq: 35, Attempt: 2}, {Seq: 40, Attempt: 2, Unstored: &unstoredLine{SHA256: hashOf("x"), Size: 600000}},
	}}
	if !reflect.DeepEqual(reply, want) {
		t.Fatalf("transcript %+v, want %+v", reply, want)
	}
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "logs", Arguments: map[string]any{"id": runID, "offset": 4, "limit": 1}})
	if err != nil {
		t.Fatal(err)
	}
	if text := toolText(result); !strings.Contains(text, `{"attempt":2,"seq":35,"text":""}`) {
		t.Fatalf("an empty line %s, want its empty text", text)
	}
	var over logsReply
	callTool(t, ctx, session, "logs", map[string]any{"id": runID, "offset": 6}, &over)
	if over.NextOffset != 7 || len(over.Lines) != 1 || !over.Lines[0].Truncated || over.Lines[0].Bytes != len(transcriptBig) ||
		over.Lines[0].Text == "" || !strings.HasPrefix(transcriptBig, over.Lines[0].Text) {
		t.Fatalf("a line over the page bound: next %d, %d lines", over.NextOffset, len(over.Lines))
	}
	overResult, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "logs", Arguments: map[string]any{"id": runID, "offset": 6}})
	if err != nil {
		t.Fatal(err)
	}
	if encoded, _ := json.Marshal(overResult.StructuredContent); len(encoded) > logsBytes {
		t.Fatalf("a truncated page of %d bytes, over the %d bound", len(encoded), logsBytes)
	}
	var last logsReply
	callTool(t, ctx, session, "logs", map[string]any{"id": runID, "offset": 7}, &last)
	if last.NextOffset != 0 || !reflect.DeepEqual(last.Lines, []logsLine{{Seq: 60, Attempt: 2, Text: "tail"}, {Seq: 70, Attempt: 1, Text: "stale past the second attempt's first line"}}) {
		t.Fatalf("the last page: %+v", last)
	}
	var page logsReply
	callTool(t, ctx, session, "logs", map[string]any{"id": runID, "offset": 1, "limit": 2}, &page)
	if page.NextOffset != 3 || !reflect.DeepEqual(page.Lines, want.Lines[1:3]) {
		t.Fatalf("offset 1 limit 2: %+v", page)
	}
	for name, call := range map[string]struct {
		arguments map[string]any
		code      string
	}{
		"a limit over the most": {map[string]any{"id": runID, "limit": logsMaxLimit + 1}, "INVALID_ARGUMENT"},
		"an unknown run":        {map[string]any{"id": "run-missing"}, "RUN_NOT_FOUND"},
	} {
		if failure := callToolError(t, ctx, session, "logs", call.arguments); failure.Code != call.code {
			t.Errorf("%s: %+v, want %s", name, failure, call.code)
		}
	}
	var events eventsOutput
	callTool(t, ctx, session, "events", map[string]any{"id": runID}, &events)
	if events.Events[1].Message != "using [redacted:model]" || !reflect.DeepEqual(events.Redacted, map[string]int{"model": 1}) {
		t.Fatalf("events %+v", events)
	}
	if encoded, _ := json.Marshal(events); len(events.Chunks) == 0 || !strings.Contains(string(encoded), `"chunks":[{"attempt":`) {
		t.Fatalf("events %s, want chunks per attempt", encoded)
	}
}

func TestMCPLogsFillsFromEarlierAttempts(t *testing.T) {
	b := newBus(t)
	admitted := b.admitted(t)
	runID := admitted.submission.Wherry.Run
	session := mcpSession(t, b.connect(t), nil)
	var records [][]byte
	for seq := range int64(5) {
		records = append(records, b.chunk(t, admitted, 1, seq, wire.Part{Text: fmt.Sprintf("a1-%d", seq)}))
	}
	for seq := int64(3); seq < 7; seq++ {
		records = append(records, b.chunk(t, admitted, 2, seq, wire.Part{Text: fmt.Sprintf("a2-%d", seq)}))
	}
	b.feed(t, runID, records...)
	adopted := []logsLine{{Seq: 0, Attempt: 1, Text: "a1-0"}, {Seq: 1, Attempt: 1, Text: "a1-1"}, {Seq: 2, Attempt: 1, Text: "a1-2"},
		{Seq: 3, Attempt: 2, Text: "a2-3"}, {Seq: 4, Attempt: 2, Text: "a2-4"}, {Seq: 5, Attempt: 2, Text: "a2-5"}, {Seq: 6, Attempt: 2, Text: "a2-6"}}
	var reply logsReply
	callTool(t, b.ctx, session, "logs", map[string]any{"id": runID}, &reply)
	if reply.Attempt != 2 || !reflect.DeepEqual(reply.Lines, adopted) {
		t.Fatalf("an adopted turn: attempt %d, lines %+v", reply.Attempt, reply.Lines)
	}
	// A third attempt that has published no line yet: every line comes from the highest earlier attempt that has it.
	b.feed(t, runID, b.sign(t, &wire.RunStatus{StatusUpdate: wire.TaskStatusUpdateEvent{TaskID: runID, ContextID: runID, Status: now(wire.TaskStateWorking)},
		Wherry: wire.StatusBinding{ReceiverBinding: b.attemptBinding(admitted, 3), EventID: "started-3", Kind: "started", TaskHash: admitted.hash}}))
	var later logsReply
	callTool(t, b.ctx, session, "logs", map[string]any{"id": runID}, &later)
	if later.Attempt != 3 || !reflect.DeepEqual(later.Lines, adopted) {
		t.Fatalf("a turn whose last attempt has no lines: attempt %d, lines %+v", later.Attempt, later.Lines)
	}
}

// A Claude stream-json turn as a dogfood run's transcript shows it: each assistant event carries a thinking block
// whose signature is about 10 KiB of base64, between system thinking_tokens progress events.
func TestMCPLogsDropsThinkingSignatures(t *testing.T) {
	b := newBus(t)
	admitted := b.admitted(t)
	runID := admitted.submission.Wherry.Run
	signature := base64.StdEncoding.EncodeToString(make([]byte, 7680))
	var records [][]byte
	for i := range int64(6) {
		progress := fmt.Sprintf(`{"type":"system","subtype":"thinking_tokens","tokens":%d,"session_id":"s"}`, 100*i)
		assistant := fmt.Sprintf(`{"type":"assistant","message":{"id":"msg-%d","role":"assistant","content":[{"type":"thinking","thinking":"step %d","signature":%q},{"type":"text","text":"answer %d"}]},"session_id":"s"}`, i, i, signature, i)
		records = append(records, b.chunk(t, admitted, 1, 2*i, wire.Part{Text: progress}), b.chunk(t, admitted, 1, 2*i+1, wire.Part{Text: assistant}))
	}
	b.feed(t, runID, records...)
	session := mcpSession(t, b.connect(t), nil)
	var reply logsReply
	callTool(t, b.ctx, session, "logs", map[string]any{"id": runID}, &reply)
	if reply.Total != 6 || len(reply.Lines) < 3 {
		t.Fatalf("a page holds %d of %d lines, want several assistant lines", len(reply.Lines), reply.Total)
	}
	for i, line := range reply.Lines {
		var event struct {
			Type    string `json:"type"`
			Message struct {
				Content []map[string]any `json:"content"`
			} `json:"message"`
		}
		if err := json.Unmarshal([]byte(line.Text), &event); err != nil || event.Type != "assistant" || len(event.Message.Content) != 2 ||
			event.Message.Content[0]["thinking"] != fmt.Sprintf("step %d", i) || event.Message.Content[0]["signature"] != nil || event.Message.Content[1]["text"] != fmt.Sprintf("answer %d", i) {
			t.Fatalf("line %d %.200s, want the assistant event without its signature", i, line.Text)
		}
	}
	var stdout, stderr bytes.Buffer
	if err := runLogs(b.ctx, b.client, []string{runID}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if text := stdout.String(); strings.Count(text, "\n") != 6 || strings.Contains(text, signature) || strings.Contains(text, "thinking_tokens") {
		t.Fatalf("wherry logs printed %d lines with a signature or thinking_tokens line", strings.Count(text, "\n"))
	}
	feed, err := b.client.Transcript(b.ctx, runID)
	if err != nil || len(feed.Lines) != 12 || !strings.Contains(feed.Lines[1].Part.Text, signature) {
		t.Fatalf("the feed's transcript lost lines or signatures: %d lines, %v", len(feed.Lines), err)
	}
}

func TestMCPPromptFile(t *testing.T) {
	work := t.TempDir()
	if err := os.WriteFile(filepath.Join(work, "brief.md"), []byte("the brief"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "empty.md"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Run("submit", func(t *testing.T) {
		b := newBus(t)
		session := mcpSessionReading(t, b.connect(t), nil, inputOpener(work, busclient.Config{}))
		var submitted submitOutput
		callTool(t, b.ctx, session, "submit", map[string]any{"prompt_file": "brief.md"}, &submitted)
		if text := b.tasks(t, submitted.ID)[0].submission.Message.Parts[0].Text; !strings.Contains(text, "the brief") {
			t.Fatalf("prompt %q, want the file's", text)
		}
		if submitted.Wait != "wherry wait "+submitted.ID {
			t.Errorf("submit wait %q, want the command that waits for %s", submitted.Wait, submitted.ID)
		}
		for name, arguments := range map[string]map[string]any{
			"both":    {"prompt": "x", "prompt_file": "brief.md"},
			"neither": {},
			"outside": {"prompt_file": filepath.Join(t.TempDir(), "brief.md")},
			"empty":   {"prompt_file": "empty.md"},
		} {
			if failure := callToolError(t, b.ctx, session, "submit", arguments); failure.Code != "INVALID_ARGUMENT" {
				t.Errorf("%s: %+v", name, failure)
			}
		}
		if failure := callToolError(t, b.ctx, session, "submit", map[string]any{"prompt_file": "empty.md"}); failure.Message != "prompt_file is empty" {
			t.Errorf("empty prompt file: %+v", failure)
		}
	})
}

func TestMCPCleanup(t *testing.T) {
	b := newBus(t)
	admitted := b.admitted(t)
	runID := admitted.submission.Wherry.Run
	session := mcpSession(t, b.connect(t), nil)
	if failure := callToolError(t, b.ctx, session, "cleanup", map[string]any{"id": runID}); failure.Code != "INVALID_ARGUMENT" || !strings.Contains(failure.Message, "has not ended") {
		t.Fatalf("cleanup of a working run: %+v", failure)
	}
	b.feed(t, runID, b.status(t, admitted, "rejected-1", "rejected", now(wire.TaskStateRejected)))
	failure := callToolError(t, b.ctx, session, "cleanup", map[string]any{"id": runID})
	if failure.Code != "INVALID_ARGUMENT" || !strings.Contains(failure.Message, "no storage credential") {
		t.Fatalf("cleanup on a host without storage: %+v", failure)
	}
	if failure := callToolError(t, b.ctx, session, "cleanup", map[string]any{"id": "run-missing"}); failure.Code != "RUN_NOT_FOUND" {
		t.Fatalf("cleanup of an unknown run: %+v", failure)
	}
}

func TestWaitCommand(t *testing.T) {
	plain, err := waitCommand("")
	if err != nil || plain("run-1") != "wherry wait run-1" {
		t.Fatalf("no config file: %v", err)
	}
	dir := t.TempDir()
	t.Chdir(dir)
	named, err := waitCommand("it's.yaml")
	want := "wherry wait run-1 --config-file '" + strings.ReplaceAll(filepath.Join(dir, "it's.yaml"), "'", `'\''`) + "'"
	if err != nil || named("run-1") != want {
		t.Fatalf("config file: %q, %v; want %q", named("run-1"), err, want)
	}
	out, err := exec.Command("sh", "-c", "printf %s "+strings.TrimPrefix(named("run-1"), "wherry wait run-1 --config-file ")).Output()
	if err != nil || string(out) != filepath.Join(dir, "it's.yaml") {
		t.Fatalf("the shell reads the path as %q, %v", out, err)
	}
}

func TestToolFailureCodes(t *testing.T) {
	for err, want := range map[error]string{
		fmt.Errorf("load: %w", admin.ErrNewerRecord): "VERSION_SKEW",
		newerRecord(t): "VERSION_SKEW",
		fmt.Errorf("submit: %w", busclient.ErrStartByPastLimit): "INVALID_ARGUMENT",
		busclient.ErrNoMatchingReceiver:                         "NO_MATCHING_RECEIVER",
	} {
		if failure := toolFailure(err).(*toolError); failure.Code != want {
			t.Errorf("%v: code %s, want %s", err, failure.Code, want)
		}
	}
}

// newerRecord is DecodeRecord's error for a record one version above this release's.
func newerRecord(t *testing.T) error {
	encoded, err := admin.EncodeRecord(admin.Record{Kind: "host", Tenant: "tenant-a", Host: "host-a"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = admin.DecodeRecord(bytes.Replace(encoded, []byte(`"v":1`), []byte(fmt.Sprintf(`"v":%d`, admin.RecordVersion+1)), 1))
	return fmt.Errorf("load: %w", err)
}

// A failed Collect reaches the model as its step alone.
func TestCollectFailureNamesTheStep(t *testing.T) {
	for _, step := range []string{"write outputs", "S3 outputs"} {
		err := fmt.Errorf("wait: %w", &busclient.CollectError{Step: step, Err: errors.New("open /home/user/state: is a directory")})
		want := toolError{Code: "OUTPUTS_NOT_WRITTEN", Message: step + ": the run's results could not be collected; the MCP server's log holds the cause"}
		if failure := collectFailure(err).(*toolError); *failure != want {
			t.Errorf("%s: %+v, want %+v", step, failure, want)
		}
	}
}

// An UNAVAILABLE reaches the model by class alone; the server's log keeps the cause.
func TestMCPUnavailableLogsItsCause(t *testing.T) {
	ctx := context.Background()
	var logged bytes.Buffer
	connect := func() (*busclient.Host, error) {
		return nil, errors.New("dial broker.example:9093: connection refused")
	}
	open := inputOpener(t.TempDir(), busclient.Config{})
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := newMCPServer(connect, inputReader(open), open, func(id string) string { return "wherry wait " + id }, log.New(&logged, "", 0)).Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { serverSession.Close() })
	session, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "v0"}, nil).Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Close() })
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "receivers", Arguments: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	if text := toolText(result); !result.IsError || !strings.Contains(text, "UNAVAILABLE") || strings.Contains(text, "broker.example") {
		t.Fatalf("tool result %q; want UNAVAILABLE without its cause", text)
	}
	if !strings.Contains(logged.String(), "tool failed: dial broker.example:9093: connection refused") {
		t.Fatalf("server log %q; want the cause", logged.String())
	}
}

// The server's log keeps an UNAVAILABLE's cause with each credential value the host resolved replaced by its marker.
func TestMCPUnavailableLogRedactsResolvedCredentials(t *testing.T) {
	answeredSeal(t, newBus(t))
	ctx := context.Background()
	var logged bytes.Buffer
	connect := func() (*busclient.Host, error) {
		return nil, errors.New("dial broker.example:9093: rejected " + credentialText)
	}
	open := inputOpener(t.TempDir(), busclient.Config{})
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := newMCPServer(connect, inputReader(open), open, func(id string) string { return "wherry wait " + id }, log.New(&logged, "", 0)).Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { serverSession.Close() })
	session, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "v0"}, nil).Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Close() })
	if failure := callToolError(t, ctx, session, "receivers", map[string]any{}); failure != (toolError{Code: "UNAVAILABLE", Message: "the wherry bus could not complete the request; the wherry CLI reports the cause"}) {
		t.Fatalf("tool error %#v; want UNAVAILABLE alone", failure)
	}
	if want := "tool failed: dial broker.example:9093: rejected " + signed.RedactionMarker(nil, "model") + "\n"; logged.String() != want {
		t.Fatalf("server log %q, want %q", logged.String(), want)
	}
}
