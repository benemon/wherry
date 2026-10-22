package main

import (
	"bytes"
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

// mcpSessionReading is mcpSession with submit opening its inputs through openInput.
func mcpSessionReading(t *testing.T, connect func() (*busclient.Host, error), progress *atomic.Int64, openInput func(string) (*os.File, error)) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := newMCPServer(connect, inputReader(openInput), openInput, func(id string) string { return "wherry await " + id }).Connect(ctx, serverTransport, nil)
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
	if want := []string{"await", "cancel", "cleanup", "continue", "events", "receivers", "status", "submit", "transcript"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("tools %v, want %v", names, want)
	}
	required := map[string][]string{
		"await": {"id"}, "cancel": {"id"}, "cleanup": {"id"}, "continue": {"id"}, "events": {"id"}, "receivers": nil, "status": nil,
		"submit": nil, "transcript": {"id"},
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
	var await struct {
		Properties map[string]any `json:"properties"`
	}
	encoded, _ := json.Marshal(schemas["await"].OutputSchema)
	if err := json.Unmarshal(encoded, &await); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"id", "state", "retry_after", "message", "artifacts"} {
		if _, ok := await.Properties[field]; !ok {
			t.Errorf("await output schema lacks %s", field)
		}
	}
}

func TestDeadlineDefaultsAreTwoHours(t *testing.T) {
	b := newBus(t)
	session := mcpSession(t, b.connect(t), nil)
	tools, err := session.ListTools(b.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range tools.Tools {
		if tool.Name != "submit" && tool.Name != "continue" {
			continue
		}
		encoded, _ := json.Marshal(tool.InputSchema)
		for _, want := range []string{
			fmt.Sprintf("task deadline in seconds; default %d", int64(busclient.DefaultDeadline/time.Second)),
			fmt.Sprintf("queue deadline in seconds; default %d", int64(busclient.DefaultQueueDeadline/time.Second)),
		} {
			if !strings.Contains(string(encoded), want) {
				t.Errorf("%s input schema lacks %q: %s", tool.Name, want, encoded)
			}
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
		if task.DeadlineMS < before.Add(2*time.Hour).UnixMilli() || task.DeadlineMS > after.Add(2*time.Hour).UnixMilli() {
			t.Errorf("%s submit deadline %v, want two hours out", lane, time.UnixMilli(task.DeadlineMS))
		}
	}
}

func TestMCPSubmitAwaitRoundTrip(t *testing.T) {
	b := newBus(t)
	ctx := b.ctx
	session := mcpSession(t, b.connect(t), nil)

	var receivers receiversOutput
	callTool(t, ctx, session, "receivers", map[string]any{}, &receivers)
	if len(receivers.Receivers) != 1 || receivers.Receivers[0].ReceiverID != "receiver-a" || receivers.Receivers[0].Pool != "pool-a" || receivers.Receivers[0].Status != "live" ||
		!reflect.DeepEqual(receivers.Receivers[0].Adapters, []busclient.Adapter{{Name: "lab", Agent: "claude", Labels: map[string]string{"runtime": "exec"}, Ready: true}}) {
		t.Fatalf("receivers %#v", receivers)
	}
	var submitted submitOutput
	callTool(t, ctx, session, "submit", submitArguments, &submitted)
	if !strings.HasPrefix(submitted.ID, "run-") || submitted.Pool != "pool-a" {
		t.Fatalf("submit %#v", submitted)
	}
	var status statusOutput
	callTool(t, ctx, session, "status", map[string]any{"id": submitted.ID}, &status)
	if len(status.Runs) != 1 || status.Runs[0].ID != submitted.ID || status.Runs[0].Pool != "pool-a" || status.Runs[0].State != string(wire.TaskStateSubmitted) || status.Runs[0].Turn != 1 {
		t.Fatalf("status before a receiver reports %#v", status)
	}
	if _, err := time.Parse(time.RFC3339, status.Runs[0].Updated); err != nil {
		t.Fatalf("updated %q: %v", status.Runs[0].Updated, err)
	}

	awaited := make(chan awaitReply, 1)
	go func() {
		var reply awaitReply
		result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "await", Arguments: map[string]any{"id": submitted.ID}})
		if err == nil && !result.IsError {
			encoded, _ := json.Marshal(result.StructuredContent)
			json.Unmarshal(encoded, &reply)
		}
		awaited <- reply
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

	reply := <-awaited
	if reply.ID != submitted.ID || reply.State != string(wire.TaskStateCompleted) || reply.RetryAfter != 0 || len(reply.Artifacts) != 1 ||
		reply.Artifacts[0].ArtifactID != "result" || len(reply.Artifacts[0].Parts) != 1 ||
		!reflect.DeepEqual(reply.Artifacts[0].Parts[0].Data, map[string]any{"answer": float64(42)}) {
		t.Fatalf("await %#v", reply)
	}
	var events eventsOutput
	callTool(t, ctx, session, "events", map[string]any{"id": submitted.ID}, &events)
	if len(events.Events) != 2 || events.Events[0].Kind != "claimed" || events.Events[1].State != string(wire.TaskStateCompleted) ||
		events.Events[1].Receiver != "receiver-a" {
		t.Fatalf("events %#v", events)
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

func TestMCPAwaitBoundedRetry(t *testing.T) {
	t.Parallel()
	b := newBus(t)
	ctx := b.ctx
	var progress atomic.Int64
	session := mcpSession(t, b.connect(t), &progress)
	var submitted submitOutput
	callTool(t, ctx, session, "submit", submitArguments, &submitted)

	waitCtx, cancel := context.WithTimeout(context.Background(), awaitBound+15*time.Second)
	defer cancel()
	start := time.Now()
	params := &mcp.CallToolParams{Name: "await", Arguments: map[string]any{"id": submitted.ID}}
	params.SetProgressToken("await-1")
	result, err := session.CallTool(waitCtx, params)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("await after %s: %v", elapsed, err)
	}
	if result.IsError {
		t.Fatalf("await error %s", toolText(result))
	}
	var reply map[string]any
	encoded, _ := json.Marshal(result.StructuredContent)
	if err := json.Unmarshal(encoded, &reply); err != nil {
		t.Fatal(err)
	}
	if elapsed < awaitBound || elapsed > awaitBound+10*time.Second {
		t.Fatalf("await returned after %s, want the %s bound", elapsed, awaitBound)
	}
	if !reflect.DeepEqual(reply, map[string]any{"id": submitted.ID, "state": string(wire.TaskStateSubmitted), "retry_after": float64(retryAfter)}) {
		t.Fatalf("await %s", encoded)
	}
	if progress.Load() == 0 {
		t.Fatal("no progress notification during await")
	}
}

func TestMCPContinueAndCancel(t *testing.T) {
	t.Run("continue", func(t *testing.T) {
		b, admitted := publishInterruptedRun(t)
		ctx, runID := b.ctx, admitted.submission.Wherry.Run
		session := mcpSession(t, b.connect(t), nil)
		var reply awaitReply
		callTool(t, ctx, session, "await", map[string]any{"id": runID}, &reply)
		if reply.State != string(wire.TaskStateAuthRequired) || reply.Message != "credential token required" || reply.RetryAfter != 0 {
			t.Fatalf("await %#v", reply)
		}
		var status statusOutput
		callTool(t, ctx, session, "status", map[string]any{"id": runID}, &status)
		if len(status.Runs) != 1 || status.Runs[0].State != string(wire.TaskStateAuthRequired) || status.Runs[0].Turn != 1 || status.Runs[0].Message != "credential token required" {
			t.Fatalf("status %#v", status)
		}
		if failure := callToolError(t, ctx, session, "continue", map[string]any{"id": runID, "queue_deadline_seconds": 15 * 24 * 60 * 60}); failure.Code != "RUN_EXPIRED" {
			t.Fatalf("continue with a queue deadline past the run lifetime %#v", failure)
		}
		if failure := toolFailure(busclient.ErrRunExpired).(*toolError); failure.Code != "RUN_EXPIRED" {
			t.Fatalf("expired run %#v", failure)
		}
		var continued continueOutput
		callTool(t, ctx, session, "continue", map[string]any{"id": runID}, &continued)
		if continued.ID != runID || continued.Turn != 2 {
			t.Fatalf("continue %#v", continued)
		}
		if failure := callToolError(t, ctx, session, "continue", map[string]any{"id": runID}); failure.Code != "TURN_ALREADY_SUBMITTED" {
			t.Fatalf("second continue %#v", failure)
		}
	})
	t.Run("cancel", func(t *testing.T) {
		b, admitted := publishInterruptedRun(t)
		ctx, runID := b.ctx, admitted.submission.Wherry.Run
		session := mcpSession(t, b.connect(t), nil)
		var cancelled cancelOutput
		callTool(t, ctx, session, "cancel", map[string]any{"id": runID}, &cancelled)
		if cancelled.ID != runID {
			t.Fatalf("cancel %#v", cancelled)
		}
		b.cancelInterrupted(t, admitted)
		var reply awaitReply
		callTool(t, ctx, session, "await", map[string]any{"id": runID}, &reply)
		if reply.State != string(wire.TaskStateCanceled) {
			t.Fatalf("await after cancel %#v", reply)
		}
		var status statusOutput
		callTool(t, ctx, session, "status", map[string]any{"id": runID}, &status)
		if len(status.Runs) != 1 || status.Runs[0].State != string(wire.TaskStateCanceled) {
			t.Fatalf("status after cancel %#v", status)
		}
		var events eventsOutput
		callTool(t, ctx, session, "events", map[string]any{"id": runID}, &events)
		if len(events.Events) != 3 || events.Events[1].Kind != "interrupted" || events.Events[2].State != string(wire.TaskStateCanceled) {
			t.Fatalf("events %#v", events)
		}
		if failure := callToolError(t, ctx, session, "continue", map[string]any{"id": runID}); failure.Code != "RUN_CANCELED" {
			t.Fatalf("continue after cancel %#v", failure)
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
	b := newBus(t)
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
		Turn: request.Turn, Attempt: request.Attempt, Credentials: request.Credentials, ExpiryMS: request.ExpiryMS}
	for {
		records, err := admin.ReadTopic(b.ctx, b.receiver, admin.HostTopic("tenant-a", "host-a"))
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range records {
			if values, err := signed.OpenSeal(record.Value, binding, key, time.Now()); err == nil {
				if string(values.Values["model"]) != credentialText {
					t.Fatalf("sealed %q, want the configured credential", values.Values["model"])
				}
				return
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

// attemptBinding is receiver-a's binding for attempt of the admitted task's turn.
func (b *bus) attemptBinding(admitted task, attempt int64) wire.ReceiverBinding {
	binding := b.binding(admitted)
	binding.Attempt = attempt
	return binding
}

// chunk signs a transcript line of attempt at seq as receiver-a does.
func (b *bus) chunk(t *testing.T, admitted task, attempt, seq int64, part wire.Part) []byte {
	t.Helper()
	runID := admitted.submission.Wherry.Run
	part.MediaType = "application/x-ndjson"
	return b.sign(t, &wire.Chunk{
		ArtifactUpdate: wire.TaskArtifactUpdateEvent{TaskID: runID, ContextID: runID, Append: seq > 0, Artifact: wire.Artifact{
			ArtifactID: "transcript", Parts: []wire.Part{part}, Extensions: []string{wire.ExtensionURI},
		}},
		Wherry: wire.ChunkBinding{ReceiverBinding: b.attemptBinding(admitted, attempt), TaskHash: admitted.hash, Seq: seq},
	})
}

// transcriptBig is a line over the transcript page bound.
var transcriptBig = strings.Repeat("b", 300<<10)

// transcriptRun admits a run whose first turn has a stale first attempt's line and a second attempt's lines, fed out
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
		b.chunk(t, admitted, 2, 40, wire.Part{URL: "unstored:oversize", Metadata: jsontext.Value(`{"sha256":"` + hashOf("x") + `","size":600000}`)}),
		b.chunk(t, admitted, 2, 50, wire.Part{Text: transcriptBig}),
		b.chunk(t, admitted, 2, 60, wire.Part{Text: "tail"}),
		b.sign(t, &wire.RunStatus{StatusUpdate: wire.TaskStatusUpdateEvent{TaskID: runID, ContextID: runID, Status: canceled},
			Wherry: wire.StatusBinding{ReceiverBinding: b.attemptBinding(admitted, 3), EventID: runID + "/1/canceled", Kind: "terminal", TaskHash: admitted.hash}}),
	)
	return b, runID
}

func TestMCPTranscript(t *testing.T) {
	b, runID := transcriptRun(t)
	ctx := b.ctx
	session := mcpSession(t, b.connect(t), nil)
	var reply transcriptReply
	callTool(t, ctx, session, "transcript", map[string]any{"id": runID}, &reply)
	want := transcriptReply{ID: runID, Turn: 1, Attempt: 2, Total: 8, NextOffset: 6, Redacted: map[string]int{"model": 1}, Lines: []transcriptLine{
		{Seq: 0, Attempt: 2, Text: `{"type":"start"}`}, {Seq: 10, Attempt: 2, Text: "second [redacted:model]"}, {Seq: 20, Attempt: 2, RawBytes: 2},
		{Seq: 30, Attempt: 2, Text: `"q"`}, {Seq: 35, Attempt: 2}, {Seq: 40, Attempt: 2, Unstored: &wire.PointerMetadata{SHA256: hashOf("x"), Size: 600000}},
	}}
	if !reflect.DeepEqual(reply, want) {
		t.Fatalf("transcript %+v, want %+v", reply, want)
	}
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "transcript", Arguments: map[string]any{"id": runID, "offset": 4, "limit": 1}})
	if err != nil {
		t.Fatal(err)
	}
	if text := toolText(result); !strings.Contains(text, `{"attempt":2,"seq":35,"text":""}`) {
		t.Fatalf("an empty line %s, want its empty text", text)
	}
	var over transcriptReply
	callTool(t, ctx, session, "transcript", map[string]any{"id": runID, "offset": 6}, &over)
	if over.NextOffset != 7 || len(over.Lines) != 1 || !over.Lines[0].Truncated || over.Lines[0].Bytes != len(transcriptBig) ||
		over.Lines[0].Text == "" || !strings.HasPrefix(transcriptBig, over.Lines[0].Text) {
		t.Fatalf("a line over the page bound: next %d, %d lines", over.NextOffset, len(over.Lines))
	}
	overResult, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "transcript", Arguments: map[string]any{"id": runID, "offset": 6}})
	if err != nil {
		t.Fatal(err)
	}
	if encoded, _ := json.Marshal(overResult.StructuredContent); len(encoded) > transcriptBytes {
		t.Fatalf("a truncated page of %d bytes, over the %d bound", len(encoded), transcriptBytes)
	}
	var last transcriptReply
	callTool(t, ctx, session, "transcript", map[string]any{"id": runID, "offset": 7}, &last)
	if last.NextOffset != 0 || !reflect.DeepEqual(last.Lines, []transcriptLine{{Seq: 60, Attempt: 2, Text: "tail"}}) {
		t.Fatalf("the last page: %+v", last)
	}
	var page transcriptReply
	callTool(t, ctx, session, "transcript", map[string]any{"id": runID, "turn": 1, "offset": 1, "limit": 2}, &page)
	if page.NextOffset != 3 || !reflect.DeepEqual(page.Lines, want.Lines[1:3]) {
		t.Fatalf("offset 1 limit 2: %+v", page)
	}
	for name, call := range map[string]struct {
		arguments map[string]any
		code      string
	}{
		"a turn past the latest": {map[string]any{"id": runID, "turn": 2}, "INVALID_ARGUMENT"},
		"a limit over the most":  {map[string]any{"id": runID, "limit": transcriptMaxLimit + 1}, "INVALID_ARGUMENT"},
		"an unknown run":         {map[string]any{"id": "run-missing"}, "RUN_NOT_FOUND"},
	} {
		if failure := callToolError(t, ctx, session, "transcript", call.arguments); failure.Code != call.code {
			t.Errorf("%s: %+v, want %s", name, failure, call.code)
		}
	}
	var events eventsOutput
	callTool(t, ctx, session, "events", map[string]any{"id": runID}, &events)
	if events.Events[1].Message != "using [redacted:model]" || !reflect.DeepEqual(events.Redacted, map[string]int{"model": 1}) {
		t.Fatalf("events %+v", events)
	}
}

func TestMCPTranscriptFillsFromEarlierAttempts(t *testing.T) {
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
	adopted := []transcriptLine{{Seq: 0, Attempt: 1, Text: "a1-0"}, {Seq: 1, Attempt: 1, Text: "a1-1"}, {Seq: 2, Attempt: 1, Text: "a1-2"},
		{Seq: 3, Attempt: 2, Text: "a2-3"}, {Seq: 4, Attempt: 2, Text: "a2-4"}, {Seq: 5, Attempt: 2, Text: "a2-5"}, {Seq: 6, Attempt: 2, Text: "a2-6"}}
	var reply transcriptReply
	callTool(t, b.ctx, session, "transcript", map[string]any{"id": runID}, &reply)
	if reply.Attempt != 2 || !reflect.DeepEqual(reply.Lines, adopted) {
		t.Fatalf("an adopted turn: attempt %d, lines %+v", reply.Attempt, reply.Lines)
	}
	// A third attempt that has published no line yet: every line comes from the highest earlier attempt that has it.
	b.feed(t, runID, b.sign(t, &wire.RunStatus{StatusUpdate: wire.TaskStatusUpdateEvent{TaskID: runID, ContextID: runID, Status: now(wire.TaskStateWorking)},
		Wherry: wire.StatusBinding{ReceiverBinding: b.attemptBinding(admitted, 3), EventID: "started-3", Kind: "started", TaskHash: admitted.hash}}))
	var later transcriptReply
	callTool(t, b.ctx, session, "transcript", map[string]any{"id": runID}, &later)
	if later.Attempt != 3 || !reflect.DeepEqual(later.Lines, adopted) {
		t.Fatalf("a turn whose last attempt has no lines: attempt %d, lines %+v", later.Attempt, later.Lines)
	}
}

// A Claude stream-json turn as a dogfood run's transcript shows it: each assistant event carries a thinking block
// whose signature is about 10 KiB of base64, between system thinking_tokens progress events.
func TestMCPTranscriptDropsThinkingSignatures(t *testing.T) {
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
	var reply transcriptReply
	callTool(t, b.ctx, session, "transcript", map[string]any{"id": runID}, &reply)
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
	if err := runTranscript(b.ctx, b.client, []string{runID}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if text := stdout.String(); strings.Count(text, "\n") != 6 || strings.Contains(text, signature) || strings.Contains(text, "thinking_tokens") {
		t.Fatalf("wherry transcript printed %d lines with a signature or thinking_tokens line", strings.Count(text, "\n"))
	}
	feed, err := b.client.Transcript(b.ctx, runID, 0)
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
		if submitted.Await != "wherry await "+submitted.ID {
			t.Errorf("submit await %q, want the command that waits for %s", submitted.Await, submitted.ID)
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
	t.Run("continue", func(t *testing.T) {
		b, admitted := publishInterruptedRun(t)
		runID := admitted.submission.Wherry.Run
		session := mcpSessionReading(t, b.connect(t), nil, inputOpener(work, busclient.Config{}))
		// The arguments are checked before the server connects, so a bus that is down does not hide a bad call.
		down := mcpSessionReading(t, func() (*busclient.Host, error) { return nil, errors.New("down") }, nil, inputOpener(work, busclient.Config{}))
		for name, arguments := range map[string]map[string]any{
			"both":  {"id": runID, "prompt": "x", "prompt_file": "brief.md"},
			"empty": {"id": runID, "prompt_file": "empty.md"},
		} {
			if failure := callToolError(t, b.ctx, down, "continue", arguments); failure.Code != "INVALID_ARGUMENT" {
				t.Errorf("%s: %+v", name, failure)
			}
		}
		var continued continueOutput
		callTool(t, b.ctx, session, "continue", map[string]any{"id": runID, "prompt_file": "brief.md"}, &continued)
		if text := b.tasks(t, runID)[1].submission.Message.Parts[0].Text; !strings.Contains(text, "the brief") {
			t.Fatalf("continuation %q, want the file's", text)
		}
		if continued.Await != "wherry await "+runID {
			t.Errorf("continue await %q, want the command that waits for %s", continued.Await, runID)
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

func TestAwaitCommand(t *testing.T) {
	plain, err := awaitCommand("")
	if err != nil || plain("run-1") != "wherry await run-1" {
		t.Fatalf("no config file: %v", err)
	}
	dir := t.TempDir()
	t.Chdir(dir)
	named, err := awaitCommand("it's.yaml")
	want := "wherry await run-1 --config-file '" + strings.ReplaceAll(filepath.Join(dir, "it's.yaml"), "'", `'\''`) + "'"
	if err != nil || named("run-1") != want {
		t.Fatalf("config file: %q, %v; want %q", named("run-1"), err, want)
	}
	out, err := exec.Command("sh", "-c", "printf %s "+strings.TrimPrefix(named("run-1"), "wherry await run-1 --config-file ")).Output()
	if err != nil || string(out) != filepath.Join(dir, "it's.yaml") {
		t.Fatalf("the shell reads the path as %q, %v", out, err)
	}
}
