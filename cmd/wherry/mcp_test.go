package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"io"
	"log"
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
	ctx := context.Background()
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := newMCPServer(connect).Connect(ctx, serverTransport, nil)
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
	if want := []string{"await", "cancel", "continue", "events", "receivers", "status", "submit"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("tools %v, want %v", names, want)
	}
	required := map[string][]string{
		"await": {"id"}, "cancel": {"id"}, "continue": {"id"}, "events": {"id"}, "receivers": nil, "status": nil,
		"submit": {"prompt"},
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
			"git": map[string]any{"url": "https://user:s3cr3t-password@git.example.test/org/repo.git", "ref": "main"},
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
				if string(values["model"]) != credentialText {
					t.Fatalf("sealed %q, want the configured credential", values["model"])
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
