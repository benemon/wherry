package main

import (
	"context"
	"crypto/ed25519"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/trust"
	"github.com/benemon/wherry/internal/wire"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// connectTo opens a client per call on the cluster kafka is connected to, as Connect does for the configured broker.
func connectTo(kafka *kgo.Client, userKey ed25519.PrivateKey, replyTopic string) func() (*busclient.Client, error) {
	return func() (*busclient.Client, error) {
		connection, err := kgo.NewClient(
			kgo.SeedBrokers(kafka.OptValue(kgo.SeedBrokers).([]string)...),
			kgo.FetchMaxWait(10*time.Millisecond),
			kgo.FetchIsolationLevel(kgo.ReadCommitted()),
			kgo.KeepControlRecords(),
		)
		if err != nil {
			return nil, err
		}
		return busclient.New(connection, userKey, replyTopic), nil
	}
}

// mcpSession connects an MCP client to newMCPServer over in-memory transports; progress counts notifications received.
func mcpSession(t *testing.T, connect func() (*busclient.Client, error), progress *atomic.Int64) *mcp.ClientSession {
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

// publishCapability publishes a live capability for receiver-a matching enrolledReceiver's approval.
func publishCapability(t *testing.T, ctx context.Context, kafka *kgo.Client, receiverKey ed25519.PrivateKey) {
	t.Helper()
	encoded, err := wire.MarshalPayload(wire.TypeCapability, &wire.Capability{
		ReceiverID: "receiver-a", Labels: map[string]string{}, Pool: "default",
		Offers:   []wire.Offer{{Adapter: "lab", Labels: map[string]string{"runtime": "exec"}}},
		Capacity: 1, Generation: 1, AtMS: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	capability, err := wire.MarshalEnvelope(wire.Sign(wire.TypeCapability, encoded, receiverKey))
	if err != nil {
		t.Fatal(err)
	}
	if err := kafka.ProduceSync(ctx, &kgo.Record{Topic: wire.TopicCapability, Key: []byte("receiver-a"), Value: capability}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

var submitArguments = map[string]any{"prompt": "fix the bug", "selector": map[string]any{"runtime": []string{"exec"}}}

func TestMCPToolsList(t *testing.T) {
	_, kafka, _, userKey, _ := enrolledReceiver(t)
	session := mcpSession(t, connectTo(kafka, userKey, "replies.test"), nil)
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
		"await": {"id"}, "cancel": {"id"}, "continue": {"id"}, "events": {"id"}, "receivers": nil, "status": {"id"},
		"submit": {"prompt", "selector"},
	}
	for name, tool := range schemas {
		if tool.Description == "" {
			t.Errorf("%s has no description", name)
		}
		for kind, schema := range map[string]any{"input": tool.InputSchema, "output": tool.OutputSchema} {
			encoded, err := json.Marshal(schema)
			if err != nil {
				t.Fatal(err)
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
	ctx, kafka, _, userKey, receiverKey := enrolledReceiver(t)
	if _, err := kadm.NewClient(kafka).CreateTopics(ctx, 1, 1, nil, "replies.test"); err != nil {
		t.Fatal(err)
	}
	publishCapability(t, ctx, kafka, receiverKey)
	session := mcpSession(t, connectTo(kafka, userKey, "replies.test"), nil)

	var receivers receiversOutput
	callTool(t, ctx, session, "receivers", map[string]any{}, &receivers)
	if len(receivers.Receivers) != 1 || receivers.Receivers[0].ReceiverID != "receiver-a" || receivers.Receivers[0].Pool != "default" {
		t.Fatalf("receivers %#v", receivers)
	}
	var submitted submitOutput
	callTool(t, ctx, session, "submit", submitArguments, &submitted)
	if !strings.HasPrefix(submitted.ID, "run-") || submitted.Pool != "default" {
		t.Fatalf("submit %#v", submitted)
	}
	var status statusOutput
	callTool(t, ctx, session, "status", map[string]any{"id": submitted.ID}, &status)
	if status.State != string(wire.TaskStateSubmitted) || status.Turn != 1 {
		t.Fatalf("status before a receiver reports %#v", status)
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

	// The receiver's side: claim the verified submission, then publish the records run.go commits on completion.
	reader, err := kgo.NewClient(kgo.SeedBrokers(kafka.OptValue(kgo.SeedBrokers).([]string)...), kgo.FetchMaxWait(10*time.Millisecond),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{wire.TopicTask: {0: kgo.NewOffset().AtStart()}}))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var submission *wire.SubmissionRecord
	var submissionHash string
	for submission == nil {
		fetches := reader.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			t.Fatal(err)
		}
		for _, record := range fetches.Records() {
			if verified, err := trust.VerifyRootSubmission(record.Value, userKey.Public().(ed25519.PublicKey)); err == nil && verified.Wherry.RunID == submitted.ID {
				submission, submissionHash = verified, trust.EnvelopeHash(record.Value)
			}
		}
	}
	signed := func(topic string, recordType wire.RecordType, payload any) *kgo.Record {
		encoded, err := wire.MarshalPayload(recordType, payload)
		if err != nil {
			t.Fatal(err)
		}
		envelope, err := wire.MarshalEnvelope(wire.Sign(recordType, encoded, receiverKey))
		if err != nil {
			t.Fatal(err)
		}
		return &kgo.Record{Topic: topic, Key: []byte(submitted.ID), Value: envelope}
	}
	statusRecord := func(kind string, state wire.TaskState) *kgo.Record {
		return signed(wire.TopicEvent, wire.TypeStatus, &wire.StatusRecord{
			StatusUpdate: wire.TaskStatusUpdateEvent{TaskID: submitted.ID, ContextID: submission.Wherry.Lineage.RootRunID, Status: wire.TaskStatus{
				State: state, Timestamp: time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
			}},
			Wherry: wire.StatusExtension{Turn: 1, Attempt: 1, TaskHash: submissionHash, ReceiverID: "receiver-a", Kind: kind},
		})
	}
	if err := kafka.ProduceSync(ctx, statusRecord("claimed", wire.TaskStateWorking)).FirstErr(); err != nil {
		t.Fatal(err)
	}
	callTool(t, ctx, session, "status", map[string]any{"id": submitted.ID}, &status)
	if status.State != string(wire.TaskStateWorking) {
		t.Fatalf("status after claim %#v", status)
	}
	completed := wire.TaskStatus{State: wire.TaskStateCompleted, Timestamp: time.Now().UTC().Format("2006-01-02T15:04:05.000Z")}
	task := signed("replies.test", wire.TypeTask, &wire.TaskRecord{
		Task: wire.Task{
			ID: submitted.ID, ContextID: submission.Wherry.Lineage.RootRunID, Status: completed, History: []wire.Message{submission.Message},
			Artifacts: []wire.Artifact{{ArtifactID: "result", Parts: []wire.Part{{Data: jsontext.Value(`{"answer":42}`), MediaType: "application/json"}}, Extensions: []string{wire.ExtensionURI}}},
		},
		Wherry: wire.TaskExtension{Turn: 1, Attempt: 1, TaskHash: submissionHash},
	})
	// run.go commits the final task and its terminal status in one transaction.
	producer, err := kgo.NewClient(kgo.SeedBrokers(kafka.OptValue(kgo.SeedBrokers).([]string)...), kgo.TransactionalID(submitted.ID))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if err := producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := producer.ProduceSync(ctx, task, statusRecord("terminal", wire.TaskStateCompleted)).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := producer.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}

	reply := <-awaited
	if reply.ID != submitted.ID || reply.State != string(wire.TaskStateCompleted) || reply.RetryAfter != 0 || len(reply.Artifacts) != 1 ||
		reply.Artifacts[0].ArtifactID != "result" || len(reply.Artifacts[0].Parts) != 1 ||
		!reflect.DeepEqual(reply.Artifacts[0].Parts[0].Data, map[string]any{"answer": float64(42)}) {
		t.Fatalf("await %#v", reply)
	}
	var events eventsOutput
	callTool(t, ctx, session, "events", map[string]any{"id": submitted.ID}, &events)
	if len(events.Events) != 2 || events.Events[0].Kind != "claimed" || events.Events[1].State != string(wire.TaskStateCompleted) || events.Cancelled {
		t.Fatalf("events %#v", events)
	}
}

func TestMCPAwaitBoundedRetry(t *testing.T) {
	t.Parallel()
	ctx, kafka, _, userKey, receiverKey := enrolledReceiver(t)
	if _, err := kadm.NewClient(kafka).CreateTopics(ctx, 1, 1, nil, "replies.test"); err != nil {
		t.Fatal(err)
	}
	publishCapability(t, ctx, kafka, receiverKey)
	var progress atomic.Int64
	session := mcpSession(t, connectTo(kafka, userKey, "replies.test"), &progress)
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
		ctx, kafka, _, userKey := publishInterruptedRun(t)
		session := mcpSession(t, connectTo(kafka, userKey, "replies.test"), nil)
		var reply awaitReply
		callTool(t, ctx, session, "await", map[string]any{"id": "run-auth"}, &reply)
		if reply.State != string(wire.TaskStateAuthRequired) || reply.Message != "credential token required" || reply.RetryAfter != 0 {
			t.Fatalf("await %#v", reply)
		}
		var status statusOutput
		callTool(t, ctx, session, "status", map[string]any{"id": "run-auth"}, &status)
		if status.State != string(wire.TaskStateAuthRequired) || status.Turn != 1 || status.Message != "credential token required" {
			t.Fatalf("status %#v", status)
		}
		var continued continueOutput
		callTool(t, ctx, session, "continue", map[string]any{"id": "run-auth"}, &continued)
		if continued.ID != "run-auth" || continued.Turn != 2 {
			t.Fatalf("continue %#v", continued)
		}
		if failure := callToolError(t, ctx, session, "continue", map[string]any{"id": "run-auth"}); failure.Code != "TURN_ALREADY_SUBMITTED" {
			t.Fatalf("second continue %#v", failure)
		}
	})
	t.Run("cancel", func(t *testing.T) {
		ctx, kafka, _, userKey := publishInterruptedRun(t)
		session := mcpSession(t, connectTo(kafka, userKey, "replies.test"), nil)
		var cancelled cancelOutput
		callTool(t, ctx, session, "cancel", map[string]any{"id": "run-auth"}, &cancelled)
		if cancelled.ID != "run-auth" {
			t.Fatalf("cancel %#v", cancelled)
		}
		var reply awaitReply
		callTool(t, ctx, session, "await", map[string]any{"id": "run-auth"}, &reply)
		if reply.State != string(wire.TaskStateCanceled) {
			t.Fatalf("await after cancel %#v", reply)
		}
		var status statusOutput
		callTool(t, ctx, session, "status", map[string]any{"id": "run-auth"}, &status)
		if status.State != string(wire.TaskStateCanceled) {
			t.Fatalf("status after cancel %#v", status)
		}
		var events eventsOutput
		callTool(t, ctx, session, "events", map[string]any{"id": "run-auth"}, &events)
		if !events.Cancelled || len(events.Events) != 1 || events.Events[0].Kind != "interrupted" {
			t.Fatalf("events %#v", events)
		}
		if failure := callToolError(t, ctx, session, "continue", map[string]any{"id": "run-auth"}); failure.Code != "RUN_CANCELED" {
			t.Fatalf("continue after cancel %#v", failure)
		}
		if failure := callToolError(t, ctx, session, "cancel", map[string]any{"id": "run-missing"}); failure.Code != "RUN_NOT_FOUND" {
			t.Fatalf("cancel of an unknown run %#v", failure)
		}
	})
}

func TestMCPSubmitNamesTheInvalidRule(t *testing.T) {
	ctx, kafka, _, userKey, _ := enrolledReceiver(t)
	session := mcpSession(t, connectTo(kafka, userKey, "replies.test"), nil)
	for rule, selector := range map[string]map[string]any{
		"invalid requirement key":                         {"task": []any{"essay"}},
		"requirement values are required":                 {"runtime": []any{}},
		"requirement values must be non-empty and unique": {"runtime": []any{""}},
	} {
		failure := callToolError(t, ctx, session, "submit", map[string]any{"prompt": "x", "selector": selector})
		if failure.Code != "INVALID_ARGUMENT" || !strings.Contains(failure.Message, rule) {
			t.Fatalf("error %#v, want INVALID_ARGUMENT naming %q", failure, rule)
		}
	}
}

func TestMCPErrorsCarryNoSecrets(t *testing.T) {
	const canary = "s3cr3t-canary"
	ctx, kafka, _, userKey, _ := enrolledReceiver(t)
	// A missing CA file under a canary directory: Connect's error names the path, as it would a real one.
	broken := busclient.Config{
		Broker: strings.Join(kafka.OptValue(kgo.SeedBrokers).([]string), ","), CAFile: filepath.Join(t.TempDir(), canary, "ca.pem"),
		KafkaUser: "user", KafkaPasswordFile: filepath.Join(t.TempDir(), "password"), UserKey: filepath.Join(t.TempDir(), "user.key"),
	}
	if _, err := busclient.Connect(broken); err == nil || !strings.Contains(err.Error(), canary) {
		t.Fatalf("precondition: the connect error %v must carry the canary", err)
	}
	working := connectTo(kafka, userKey, "replies.test")
	secrets := []string{canary, "s3cr3t-password", string(userKey)}
	for name, call := range map[string]struct {
		connect   func() (*busclient.Client, error)
		tool      string
		arguments map[string]any
		code      string
	}{
		"connect failure": {func() (*busclient.Client, error) { return busclient.Connect(broken) }, "status", map[string]any{"id": "run-a"}, "UNAVAILABLE"},
		"git userinfo": {working, "submit", map[string]any{
			"prompt": "x", "selector": map[string]any{},
			"git": map[string]any{"url": "https://user:s3cr3t-password@git.example.test/org/repo.git", "ref": "main"},
		}, "INVALID_ARGUMENT"},
		"unknown run": {working, "status", map[string]any{"id": "run-missing"}, "RUN_NOT_FOUND"},
		"invalid selector key": {working, "submit", map[string]any{
			"prompt": "x", "selector": map[string]any{"s3cr3t-password": []any{"x"}},
		}, "INVALID_ARGUMENT"},
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
