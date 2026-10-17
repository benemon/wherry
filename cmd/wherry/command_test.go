package main

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/receiver"
	"github.com/benemon/wherry/internal/trust"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestParseTerms(t *testing.T) {
	got, err := parseTerms("region=eu,arch=arm64")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"region": "eu", "arch": "arm64"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
	if _, err := parseTerms("region=eu,region=us"); err == nil {
		t.Fatal("duplicate key accepted")
	}
}

func TestParseSelectorAlternatives(t *testing.T) {
	got, err := parseRequirements("runtime=exec|podman,nomad.node_pool=default")
	if err != nil {
		t.Fatal(err)
	}
	want := []wire.Requirement{
		{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec", "podman"}},
		{Key: "nomad.node_pool", Operator: wire.OpIn, Values: []string{"default"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestParseSelectorRejectsDuplicateKey(t *testing.T) {
	if _, err := parseRequirements("runtime=exec,runtime=podman"); err == nil {
		t.Fatal("duplicate key accepted")
	}
}

func TestApproveRequiresGrants(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runEnrol(context.Background(), nil, []string{"approve", "request", "--labels", "", "--selector", "region=eu"}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "--grants") {
		t.Fatalf("got %v, want required grants error", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("produced output: %q", stdout.String())
	}
}

func TestNewAdaptersRejectsUnknownType(t *testing.T) {
	_, err := newAdapters([]receiver.AdapterConfig{{Name: "k8s", Type: "kubernetes", Config: jsontext.Value(`{}`)}}, "")
	if err == nil || !strings.Contains(err.Error(), `unknown type "kubernetes"`) {
		t.Fatalf("got %v, want unknown type error", err)
	}
}

func TestNewAdaptersRequiresCredentialEnv(t *testing.T) {
	config := jsontext.Value(`{"address":"http://127.0.0.1:1","namespace":"wherry","runtimes":{}}`)
	_, err := newAdapters([]receiver.AdapterConfig{{Name: "lab", Type: "nomad", Credential: "token", Config: config}}, "")
	if err == nil || !strings.Contains(err.Error(), "credential_env") {
		t.Fatalf("got %v, want credential_env required", err)
	}
	adapters, err := newAdapters([]receiver.AdapterConfig{{Name: "lab", Type: "nomad", Config: config}}, "")
	if err != nil || len(adapters) != 1 || adapters[0].Name != "lab" {
		t.Fatalf("adapters %#v, err %v", adapters, err)
	}
}

// enrolledReceiver returns a client whose bus holds an approved enrolment for receiver-a, and that receiver's signing key.
func enrolledReceiver(t *testing.T) (context.Context, *kgo.Client, *busclient.Client, ed25519.PrivateKey, ed25519.PrivateKey) {
	t.Helper()
	cluster := kfake.MustCluster(kfake.NumBrokers(1))
	t.Cleanup(cluster.Close)
	kafka, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.FetchMaxWait(10*time.Millisecond),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.KeepControlRecords(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kafka.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	if err := wire.ApplyTopics(ctx, kadm.NewClient(kafka), 1); err != nil {
		t.Fatal(err)
	}
	_, userKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, receiverKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sealingKey, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	detected := []wire.DetectedAdapter{{Name: "lab", Platform: "nomad", Offers: []wire.Offer{{Adapter: "lab", Labels: map[string]string{"runtime": "exec"}}}}}
	request, err := trust.NewEnrolRequest("receiver-a", receiverKey, sealingKey, map[string]string{}, detected, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	grants := []wire.AdapterGrant{{Name: "lab", Platform: "nomad", Allowed: map[string][]string{"runtime": {"exec"}}, Defaults: map[string]string{}}}
	enrolRequest, err := trust.NewState(userKey.Public().(ed25519.PublicKey)).Apply(request)
	if err != nil {
		t.Fatal(err)
	}
	approval, err := trust.Approve(request, enrolRequest.(*wire.EnrolRequest), userKey, map[string]string{}, "default", grants, []wire.Requirement{{Key: "region", Operator: wire.OpIn, Values: []string{"eu"}}}, "auto", 1, "", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := kafka.ProduceSync(ctx,
		&kgo.Record{Topic: "receivers.state", Key: []byte("receiver-a"), Value: request},
		&kgo.Record{Topic: "receivers.state", Key: []byte("receiver-a"), Value: approval},
	).FirstErr(); err != nil {
		t.Fatal(err)
	}
	return ctx, kafka, busclient.New(kafka, userKey, "replies.test"), userKey, receiverKey
}

func TestReceiversJSONOutputShape(t *testing.T) {
	ctx, _, client, _, _ := enrolledReceiver(t)
	var stdout, stderr bytes.Buffer
	if err := runReceivers(ctx, client, []string{"--json"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}
	var rows []map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	keys := make([]string, 0, len(rows[0]))
	for key := range rows[0] {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	wantKeys := []string{"adapters", "capacity", "floor_generation", "generation", "heartbeat_age", "in_flight", "labels", "offers", "pool", "receiver_id", "revoked", "secrets", "selector", "status"}
	if !reflect.DeepEqual(keys, wantKeys) {
		t.Fatalf("got JSON fields %v, want %v", keys, wantKeys)
	}
	if rows[0]["receiver_id"] != "receiver-a" || rows[0]["status"] != "silent" || rows[0]["pool"] != "default" {
		t.Fatalf("unexpected JSON row: %#v", rows[0])
	}
}

// awaitOutput publishes a completed task with parts for run-a and returns what await prints.
func awaitOutput(t *testing.T, parts []wire.Part) string {
	t.Helper()
	ctx, kafka, client, userKey, receiverKey := enrolledReceiver(t)
	if _, err := kadm.NewClient(kafka).CreateTopics(ctx, 1, 1, nil, "replies.test"); err != nil {
		t.Fatal(err)
	}
	submission := wire.SubmissionRecord{
		Message: wire.Message{
			MessageID: "msg-run-a", ContextID: "run-a", TaskID: "run-a", Role: wire.RoleUser,
			Parts: []wire.Part{{Text: "work"}}, Extensions: []string{wire.ExtensionURI},
		},
		Wherry: wire.SubmissionExtension{
			RunID: "run-a", Turn: 1, Attempt: 1, Pool: "default", Selector: []wire.Requirement{}, Labels: map[string]string{},
			DeadlineMS: time.Now().Add(time.Minute).UnixMilli(), QueueDeadlineMS: time.Now().Add(time.Minute).UnixMilli(),
			ReplyTopic: "replies.test", Lineage: wire.Lineage{RootRunID: "run-a", Ancestors: []string{}, Chain: []wire.Bytes{}},
		},
	}
	submissionEnvelope, err := trust.SignRootSubmission(submission, userKey)
	if err != nil {
		t.Fatal(err)
	}
	task := &wire.TaskRecord{
		Task: wire.Task{
			ID: "run-a", ContextID: "run-a", History: []wire.Message{submission.Message},
			Status:    wire.TaskStatus{State: wire.TaskStateCompleted, Timestamp: time.Now().UTC().Format(time.RFC3339)},
			Artifacts: []wire.Artifact{{ArtifactID: "result", Parts: parts, Extensions: []string{wire.ExtensionURI}}},
		},
		Wherry: wire.TaskExtension{Turn: 1, Attempt: 1, TaskHash: trust.EnvelopeHash(submissionEnvelope)},
	}
	payload, err := wire.MarshalPayload(wire.TypeTask, task)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := wire.MarshalEnvelope(wire.Sign(wire.TypeTask, payload, receiverKey))
	if err != nil {
		t.Fatal(err)
	}
	if err := kafka.ProduceSync(ctx,
		&kgo.Record{Topic: wire.TopicTask, Key: []byte("run-a"), Value: submissionEnvelope},
		&kgo.Record{Topic: "replies.test", Key: []byte("run-a"), Value: envelope},
	).FirstErr(); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := runAwait(ctx, client, []string{"run-a", "--timeout", "5s"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	return stdout.String()
}

func TestAwaitPrintsNonTextPartPlaceholders(t *testing.T) {
	got := awaitOutput(t, []wire.Part{
		{Text: "done"},
		{Raw: wire.StdBytes("hello"), MediaType: "text/plain"},
		{URL: "https://example.test/report.pdf"},
	})
	if want := "TASK_STATE_COMPLETED\ndone\n[raw text/plain 5 bytes]\n[url https://example.test/report.pdf]\n"; got != want {
		t.Fatalf("stdout %q, want %q", got, want)
	}
}

func TestAwaitPrintsDataPart(t *testing.T) {
	got := awaitOutput(t, []wire.Part{{Data: jsontext.Value(`{"answer":42,"items":["a","b"]}`), MediaType: "application/json"}})
	if want := "TASK_STATE_COMPLETED\n{\"answer\":42,\"items\":[\"a\",\"b\"]}\n"; got != want {
		t.Fatalf("stdout %q, want %q", got, want)
	}
}

func publishInterruptedRun(t *testing.T) (context.Context, *kgo.Client, *busclient.Client) {
	t.Helper()
	ctx, kafka, client, userKey, receiverKey := enrolledReceiver(t)
	if _, err := kadm.NewClient(kafka).CreateTopics(ctx, 1, 1, nil, "replies.test"); err != nil {
		t.Fatal(err)
	}
	submission := wire.SubmissionRecord{
		Message: wire.Message{
			MessageID: "msg-run-auth", ContextID: "run-auth", TaskID: "run-auth", Role: wire.RoleUser,
			Parts: []wire.Part{{Text: "work"}}, Extensions: []string{wire.ExtensionURI},
		},
		Wherry: wire.SubmissionExtension{
			RunID: "run-auth", Turn: 1, Attempt: 1, Pool: "default", Selector: []wire.Requirement{}, Labels: map[string]string{},
			DeadlineMS: time.Now().Add(time.Minute).UnixMilli(), QueueDeadlineMS: time.Now().Add(time.Minute).UnixMilli(),
			ReplyTopic: "replies.test", Lineage: wire.Lineage{RootRunID: "run-auth", Ancestors: []string{}, Chain: []wire.Bytes{}},
		},
	}
	payload, err := trust.SignRootSubmission(submission, userKey)
	if err != nil {
		t.Fatal(err)
	}
	status := &wire.StatusRecord{
		StatusUpdate: wire.TaskStatusUpdateEvent{
			TaskID: "run-auth", ContextID: "run-auth",
			Status: wire.TaskStatus{
				State: wire.TaskStateAuthRequired, Timestamp: time.Now().UTC().Format(time.RFC3339),
				Message: &wire.Message{MessageID: "msg-status", Role: wire.RoleAgent, Parts: []wire.Part{{Text: "credential token required"}}, Extensions: []string{wire.ExtensionURI}},
			},
		},
		Wherry: wire.StatusExtension{Turn: 1, Attempt: 1, TaskHash: trust.EnvelopeHash(payload), ReceiverID: "receiver-a", Kind: "interrupted"},
	}
	statusPayload, err := wire.MarshalPayload(wire.TypeStatus, status)
	if err != nil {
		t.Fatal(err)
	}
	statusEnvelope, err := wire.MarshalEnvelope(wire.Sign(wire.TypeStatus, statusPayload, receiverKey))
	if err != nil {
		t.Fatal(err)
	}
	if err := kafka.ProduceSync(ctx,
		&kgo.Record{Topic: wire.TopicTask, Key: []byte("run-auth"), Value: payload},
		&kgo.Record{Topic: wire.TopicEvent, Key: []byte("run-auth"), Value: statusEnvelope},
	).FirstErr(); err != nil {
		t.Fatal(err)
	}
	return ctx, kafka, client
}

func TestAwaitPrintsAuthRequiredAndContinuation(t *testing.T) {
	ctx, _, client := publishInterruptedRun(t)
	var stdout, stderr bytes.Buffer
	err := runAwait(ctx, client, []string{"run-auth", "--timeout", "5s"}, &stdout, &stderr)
	if err == nil || err.Error() != "run ended in TASK_STATE_AUTH_REQUIRED" {
		t.Fatalf("error %v", err)
	}
	want := "TASK_STATE_AUTH_REQUIRED\ncredential token required\n"
	if stdout.String() != want || stderr.String() != "continue with: wherry continue run-auth\n" {
		t.Fatalf("stdout %q stderr %q, want %q and the continue hint", stdout.String(), stderr.String(), want)
	}
}

func TestContinuePrintsRunAndTurn(t *testing.T) {
	ctx, kafka, client := publishInterruptedRun(t)
	var stdout, stderr bytes.Buffer
	before := time.Now()
	if err := runContinue(ctx, client, []string{"run-auth", "--deadline", "45s", "--queue-deadline", "15s"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	after := time.Now()
	if got, want := stdout.String(), "run_id=run-auth turn=2\n"; got != want {
		t.Fatalf("stdout %q, want %q", got, want)
	}
	records, err := kadm.NewClient(kafka).ListEndOffsets(ctx, wire.TopicTask)
	if err != nil || records.Error() != nil {
		t.Fatal(err)
	}
	var count int64
	partitions := map[int32]kgo.Offset{}
	for _, offset := range records[wire.TopicTask] {
		if offset.Partition >= 0 {
			count += offset.Offset
			partitions[offset.Partition] = kgo.NewOffset().AtStart()
		}
	}
	if count != 2 {
		t.Fatalf("task queue has %d records, want submission and continuation", count)
	}
	kafka.AddConsumePartitions(map[string]map[int32]kgo.Offset{wire.TopicTask: partitions})
	var continuation *wire.SubmissionRecord
	for continuation == nil {
		fetches := kafka.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			t.Fatal(err)
		}
		for _, record := range fetches.Records() {
			envelope, err := wire.DecodeEnvelope(record.Value)
			if err != nil {
				t.Fatal(err)
			}
			payload, err := wire.DecodePayload(wire.TypeSubmission, envelope.Payload)
			if err != nil {
				t.Fatal(err)
			}
			if submission := payload.(*wire.SubmissionRecord); submission.Wherry.Turn == 2 {
				continuation = submission
			}
		}
	}
	inRange := func(ms int64, d time.Duration) bool {
		return ms >= before.Add(d).UnixMilli() && ms <= after.Add(d).UnixMilli()
	}
	if !inRange(continuation.Wherry.DeadlineMS, 45*time.Second) || !inRange(continuation.Wherry.QueueDeadlineMS, 15*time.Second) {
		t.Fatalf("continuation deadlines %d/%d do not follow the flags", continuation.Wherry.DeadlineMS, continuation.Wherry.QueueDeadlineMS)
	}
}

func TestEventsPrintsStatusesAndChunkCounts(t *testing.T) {
	ctx, kafka, client, _, receiverKey := enrolledReceiver(t)
	publish := func(topic string, recordType wire.RecordType, payload any) {
		t.Helper()
		encoded, err := wire.MarshalPayload(recordType, payload)
		if err != nil {
			t.Fatal(err)
		}
		envelope, err := wire.MarshalEnvelope(wire.Sign(recordType, encoded, receiverKey))
		if err != nil {
			t.Fatal(err)
		}
		if err := kafka.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte("run-a"), Value: envelope}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	extensions := []string{wire.ExtensionURI}
	publish(wire.TopicEvent, wire.TypeStatus, &wire.StatusRecord{
		StatusUpdate: wire.TaskStatusUpdateEvent{TaskID: "run-a", ContextID: "run-a", Status: wire.TaskStatus{
			State: wire.TaskStateRejected, Timestamp: "2026-10-17T10:00:00.000Z",
			Message: &wire.Message{MessageID: "msg-1", Role: wire.RoleAgent, Parts: []wire.Part{{Text: "queue deadline passed"}}, Extensions: extensions},
		}},
		Wherry: wire.StatusExtension{Turn: 1, Attempt: 1, TaskHash: strings.Repeat("a", 64), ReceiverID: "receiver-a", Kind: "terminal"},
	})
	for seq := range int64(2) {
		publish(wire.TopicTranscript, wire.TypeArtifactUpdate, &wire.ArtifactUpdateRecord{
			ArtifactUpdate: wire.TaskArtifactUpdateEvent{TaskID: "run-a", ContextID: "run-a", Append: seq > 0, Artifact: wire.Artifact{
				ArtifactID: "transcript", Parts: []wire.Part{{Text: "line", MediaType: "application/x-ndjson"}}, Extensions: extensions,
			}},
			Wherry: wire.ArtifactUpdateExtension{Turn: 1, Attempt: 1, Seq: seq},
		})
	}
	var stdout, stderr bytes.Buffer
	if err := runEvents(ctx, client, []string{"run-a"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	want := "turn=1 attempt=1 kind=terminal state=TASK_STATE_REJECTED receiver=receiver-a time=2026-10-17T10:00:00.000Z message=\"queue deadline passed\"\n" +
		"turn=1 attempt=1 transcript_chunks=2\n"
	if stdout.String() != want {
		t.Fatalf("stdout %q, want %q", stdout.String(), want)
	}
}

func TestRevokeCLI(t *testing.T) {
	ctx, _, client, _, _ := enrolledReceiver(t)
	var stdout, stderr bytes.Buffer
	if err := runEnrol(ctx, client, []string{"revoke", "receiver-a"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	want := "receiver=receiver-a floor_generation=1\n"
	if stdout.String() != want || stderr.String() != "credentials sealed to this receiver are not rotated; rotate them at their source\n" {
		t.Fatalf("stdout %q stderr %q, want %q and the rotation notice", stdout.String(), stderr.String(), want)
	}
	stdout.Reset()
	err := runEnrol(ctx, client, []string{"revoke", "receiver-unknown"}, &stdout, &stderr)
	if err == nil || err.Error() != "receiver receiver-unknown has never been approved" {
		t.Fatalf("got %v, want never approved error", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("produced output for an unknown receiver: %q", stdout.String())
	}
}

func TestReceiversShowsRevoked(t *testing.T) {
	ctx, _, client, _, _ := enrolledReceiver(t)
	var discard bytes.Buffer
	if err := runEnrol(ctx, client, []string{"revoke", "receiver-a"}, &discard, &discard); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := runReceivers(ctx, client, nil, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if want := "receiver=receiver-a pool=default generation=1 floor_generation=1 status=revoked\n"; stdout.String() != want {
		t.Fatalf("got %q, want %q", stdout.String(), want)
	}
	stdout.Reset()
	if err := runReceivers(ctx, client, []string{"--json"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	var rows []map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0]["revoked"] != true || rows[0]["floor_generation"] != float64(1) || rows[0]["status"] != "revoked" {
		t.Fatalf("unexpected JSON rows: %#v", rows)
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}
}

func TestManualRenewalCLI(t *testing.T) {
	ctx, kafka, client, _, receiverKey := enrolledReceiver(t)
	enrolments, _, err := client.Enrolments(ctx)
	if err != nil || len(enrolments) != 1 {
		t.Fatalf("enrolments %#v, err %v", enrolments, err)
	}
	grantsPath := filepath.Join(t.TempDir(), "grants.json")
	if err := os.WriteFile(grantsPath, []byte(`[{"name":"lab","platform":"nomad","allowed":{"runtime":["exec"]},"defaults":{}}]`), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := runEnrol(ctx, client, []string{"approve", enrolments[0].RequestHash, "--pool", "default", "--labels", "", "--grants", grantsPath, "--selector", "region=eu", "--renewal", "manual"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	payload, err := wire.MarshalPayload(wire.TypeRenewRequest, &wire.RenewRequest{ReceiverID: "receiver-a", Generation: 2, AtMS: time.Now().UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	request, err := wire.MarshalEnvelope(wire.Sign(wire.TypeRenewRequest, payload, receiverKey))
	if err != nil {
		t.Fatal(err)
	}
	if err := kafka.ProduceSync(ctx, &kgo.Record{Topic: wire.TopicState, Key: []byte("receiver-a"), Value: request}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	list := func() string {
		t.Helper()
		var stdout, stderr bytes.Buffer
		if err := runEnrol(ctx, client, []string{"list"}, &stdout, &stderr); err != nil {
			t.Fatal(err)
		}
		if stderr.Len() != 0 {
			t.Fatalf("unexpected stderr: %s", stderr.String())
		}
		return strings.SplitN(stdout.String(), "\n", 2)[0]
	}
	want := "receiver=receiver-a request=" + enrolments[0].RequestHash + " labels= approved=true generation=2"
	if got := list(); got != want+" renewal=requested" {
		t.Fatalf("got %q, want the renew request listed", got)
	}
	stdout.Reset()
	if err := runEnrol(ctx, client, []string{"renew", "receiver-a"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "receiver=receiver-a generation=2 renewed=true\n" || stderr.Len() != 0 {
		t.Fatalf("stdout %q stderr %q", stdout.String(), stderr.String())
	}
	if got := list(); got != want {
		t.Fatalf("got %q after renewal, want %q", got, want)
	}
	if err := runEnrol(ctx, client, []string{"revoke", "receiver-a"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if err := runEnrol(ctx, client, []string{"renew", "receiver-a"}, &stdout, &stderr); err == nil || err.Error() != "receiver is revoked" {
		t.Fatalf("renew revoked receiver: got %v", err)
	}
}

func TestApproveExpires(t *testing.T) {
	for _, test := range []struct {
		flags    []string
		lifetime time.Duration
	}{
		{[]string{"--expires", "48h"}, 48 * time.Hour},
		{nil, 30 * 24 * time.Hour},
	} {
		ctx, kafka, client, userKey, _ := enrolledReceiver(t)
		enrolments, _, err := client.Enrolments(ctx)
		if err != nil || len(enrolments) != 1 {
			t.Fatalf("enrolments %#v, err %v", enrolments, err)
		}
		grantsPath := filepath.Join(t.TempDir(), "grants.json")
		if err := os.WriteFile(grantsPath, []byte(`[{"name":"lab","platform":"nomad","allowed":{"runtime":["exec"]},"defaults":{}}]`), 0o600); err != nil {
			t.Fatal(err)
		}
		var stdout, stderr bytes.Buffer
		before := time.Now()
		args := append([]string{"approve", enrolments[0].RequestHash, "--labels", "", "--grants", grantsPath, "--selector", "region=eu"}, test.flags...)
		if err := runEnrol(ctx, client, args, &stdout, &stderr); err != nil {
			t.Fatal(err)
		}
		after := time.Now()
		offsets, err := kadm.NewClient(kafka).ListEndOffsets(ctx, wire.TopicState)
		if err != nil || offsets.Error() != nil {
			t.Fatal(err)
		}
		var remaining int64
		partitions := map[int32]kgo.Offset{}
		for _, offset := range offsets[wire.TopicState] {
			remaining += offset.Offset
			partitions[offset.Partition] = kgo.NewOffset().AtStart()
		}
		kafka.AddConsumePartitions(map[string]map[int32]kgo.Offset{wire.TopicState: partitions})
		state := trust.NewState(userKey.Public().(ed25519.PublicKey))
		for remaining > 0 {
			fetches := kafka.PollFetches(ctx)
			if err := fetches.Err(); err != nil {
				t.Fatal(err)
			}
			for _, record := range fetches.Records() {
				remaining--
				_, _ = state.Apply(record.Value)
			}
		}
		approval, _, ok := state.Approval("receiver-a")
		if !ok || approval.Generation != 2 || approval.ExpMS < before.Add(test.lifetime).UnixMilli() || approval.ExpMS > after.Add(test.lifetime).UnixMilli() {
			t.Fatalf("approval %#v, want generation 2 expiring %s after approval", approval, test.lifetime)
		}
	}
	var stdout, stderr bytes.Buffer
	if err := runEnrol(context.Background(), nil, []string{"approve", "request", "--labels", "", "--grants", "unused", "--selector", "region=eu", "--expires", "0s"}, &stdout, &stderr); err == nil || err.Error() != "expires must be positive" {
		t.Fatalf("zero expiry: got %v", err)
	}
}

func TestUsageErrors(t *testing.T) {
	for _, test := range []struct {
		name string
		call func(*bytes.Buffer) error
		want string
	}{
		{"submit argument", func(stderr *bytes.Buffer) error {
			return runSubmit(context.Background(), nil, []string{"--selector", "a=b", "--labels", "", "extra"}, stderr, stderr)
		}, "usage: wherry submit"},
		{"continue argument", func(stderr *bytes.Buffer) error {
			return runContinue(context.Background(), nil, []string{"run-a", "extra"}, stderr, stderr)
		}, "usage: wherry continue"},
		{"await argument", func(stderr *bytes.Buffer) error {
			return runAwait(context.Background(), nil, []string{"run-a", "extra"}, stderr, stderr)
		}, "usage: wherry await"},
		{"secret put argument", func(stderr *bytes.Buffer) error {
			return runSecret(context.Background(), nil, []string{"put", "token", "--to", "r", "--from-file", "f", "extra"}, stderr, stderr)
		}, "usage: wherry secret put"},
		{"approve argument", func(stderr *bytes.Buffer) error {
			return runEnrol(context.Background(), nil, []string{"approve", "request", "--labels", "", "--grants", "g", "--selector", "a=b", "extra"}, stderr, stderr)
		}, "usage: wherry enrol"},
		{"submit deadline", func(stderr *bytes.Buffer) error {
			return runSubmit(context.Background(), nil, []string{"--selector", "a=b", "--labels", "", "--prompt", "p", "--deadline", "0s"}, stderr, stderr)
		}, "deadlines must be positive"},
		{"continue deadline", func(stderr *bytes.Buffer) error {
			return runContinue(context.Background(), nil, []string{"run-a", "--deadline", "0s"}, stderr, stderr)
		}, "deadlines must be positive"},
		{"await timeout", func(stderr *bytes.Buffer) error {
			return runAwait(context.Background(), nil, []string{"run-a", "--timeout", "0s"}, stderr, stderr)
		}, "timeout must be positive"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			if err := test.call(&output); err == nil || !strings.HasPrefix(err.Error(), test.want) {
				t.Fatalf("got %v, want %q", err, test.want)
			}
		})
	}
}

func TestKeyChecksArguments(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "config.json")
	keyPath := filepath.Join(dir, "user.key")
	if err := os.WriteFile(config, []byte(`{"broker":"127.0.0.1:1","ca_file":"ca","kafka_user":"u","kafka_password_file":"p","user_key":"`+keyPath+`"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WHERRY_CONFIG", config)
	var stdout, stderr bytes.Buffer
	if err := run([]string{"init"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"key", "extra"}, &stdout, &stderr); err == nil || err.Error() != "usage: wherry key" {
		t.Fatalf("got %v, want key usage error", err)
	}
	stdout.Reset()
	if err := run([]string{"key"}, &stdout, &stderr); err != nil || !strings.HasPrefix(stdout.String(), "public_key=") {
		t.Fatalf("key: %v, stdout %q", err, stdout.String())
	}
}

// putSecret seals a fixed value to receiver-a with the given extra secret put flags.
func putSecret(t *testing.T, ctx context.Context, client *busclient.Client, name string, flags ...string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte("value"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := runSecret(ctx, client, append([]string{"put", name, "--to", "receiver-a", "--from-file", path}, flags...), &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
}

func TestSecretPutExpires(t *testing.T) {
	ctx, _, client, _, _ := enrolledReceiver(t)
	before := time.Now()
	putSecret(t, ctx, client, "short", "--expires", "48h")
	putSecret(t, ctx, client, "long")
	after := time.Now()
	receivers, _, err := client.Receivers(ctx)
	if err != nil || len(receivers) != 1 || len(receivers[0].Secrets) != 2 {
		t.Fatalf("receivers %#v, err %v", receivers, err)
	}
	for _, seal := range receivers[0].Secrets {
		lifetime := map[string]time.Duration{"short": 48 * time.Hour, "long": 30 * 24 * time.Hour}[seal.Name]
		if seal.ExpMS < before.Add(lifetime).UnixMilli() || seal.ExpMS > after.Add(lifetime).UnixMilli() {
			t.Fatalf("secret %s expires at %s, want %s after sealing", seal.Name, time.UnixMilli(seal.ExpMS), lifetime)
		}
	}
	var stdout, stderr bytes.Buffer
	if err := runSecret(ctx, client, []string{"put", "zero", "--to", "receiver-a", "--from-file", "unused", "--expires", "0s"}, &stdout, &stderr); err == nil {
		t.Fatal("zero expiry accepted")
	}
}

func TestSecretPutNeverPrintsPlaintext(t *testing.T) {
	const plaintext = "s3cr3t-sealed-plaintext"
	ctx, _, client, _, _ := enrolledReceiver(t)
	path := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(path, []byte(plaintext), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"token", ""} {
		var stdout, stderr bytes.Buffer
		err := runSecret(ctx, client, []string{"put", name, "--to", "receiver-a", "--from-file", path}, &stdout, &stderr)
		if (err != nil) != (name == "") {
			t.Fatalf("secret put %q: %v", name, err)
		}
		if strings.Contains(stdout.String()+stderr.String(), plaintext) || err != nil && strings.Contains(err.Error(), plaintext) {
			t.Fatalf("secret put %q printed the plaintext: stdout %q stderr %q err %v", name, stdout.String(), stderr.String(), err)
		}
	}
}

func TestReceiversWarnsSealExpiry(t *testing.T) {
	ctx, _, client, _, _ := enrolledReceiver(t)
	putSecret(t, ctx, client, "short", "--expires", "72h")
	putSecret(t, ctx, client, "long")
	var stdout, stderr bytes.Buffer
	if err := runReceivers(ctx, client, nil, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"long", "short"} {
		if !strings.Contains(stdout.String(), "\n  secret="+name+" version=1 expires=") {
			t.Fatalf("secret %s not listed:\n%s", name, stdout.String())
		}
	}
	if want := "secret short on receiver receiver-a expires in 3d\n"; stderr.String() != want {
		t.Fatalf("stderr %q, want %q", stderr.String(), want)
	}
	stdout.Reset()
	stderr.Reset()
	if err := runReceivers(ctx, client, []string{"--json"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	var rows []busclient.Receiver
	if err := json.Unmarshal(stdout.Bytes(), &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || len(rows[0].Secrets) != 2 || rows[0].Secrets[1].Name != "short" || rows[0].Secrets[1].ExpMS <= time.Now().UnixMilli() {
		t.Fatalf("JSON secrets %#v", rows)
	}
}
