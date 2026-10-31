package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"flag"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/trust"
	"github.com/benemon/wherry/internal/wire"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"gopkg.in/yaml.v3"
)

func TestParseSelectorForms(t *testing.T) {
	got, err := parseRequirements("runtime=exec|podman,example.com/segment!=batch|test,gpu,!spot")
	if err != nil {
		t.Fatal(err)
	}
	want := []wire.Requirement{
		{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec", "podman"}},
		{Key: "example.com/segment", Operator: wire.OpNotIn, Values: []string{"batch", "test"}},
		{Key: "gpu", Operator: wire.OpExists},
		{Key: "spot", Operator: wire.OpDoesNotExist},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
	if got, err := parseRequirements(""); err != nil || got == nil || len(got) != 0 {
		t.Fatalf("empty selector %#v, %v; want an empty selector", got, err)
	}
}

func TestParseSelectorRejectsInvalidInput(t *testing.T) {
	for _, value := range []string{
		"runtime=exec,runtime=podman", "runtime=exec,runtime!=podman", "gpu,!gpu",
		"runtime=", "runtime!=", "=exec", "!=exec", "!", "runtime=exec||podman", "runtime=exec|exec", "runtime==exec",
		"!gpu=a100", "wherry.io/owner=x", "a,,b", "runtime=exec,",
	} {
		if got, err := parseRequirements(value); err == nil {
			t.Errorf("accepted %q as %#v", value, got)
		}
	}
}

const credentialText = "s3cr3t-model-credential"

// bus is a kfake cluster provisioned through admin.Service: pool-a with receiver-a live in it, and host-a of
// tenant-a entitled to it with its key registered.
type bus struct {
	ctx         context.Context
	service     *admin.Service
	client      *busclient.Host
	hostBundle  string
	hostKey     ed25519.PrivateKey
	stateDir    string
	credentials map[string]credential.Source
	git         []busclient.GitCredential
	receiverKey ed25519.PrivateKey
	receiver    *kgo.Client
	// logs holds what the bus's hosts log.
	logs *syncBuffer
}

func newBus(t *testing.T) *bus {
	t.Helper()
	cluster := kfake.MustCluster(kfake.EnableSASL(), kfake.EnableACLs(), kfake.Superuser("SCRAM-SHA-512", "admin", "password"), kfake.Superuser("PLAIN", "wherry.operator", "operator-password"))
	t.Cleanup(cluster.Close)
	if err := cluster.CreateTopic(admin.AdminTopic, 1, nil); err != nil {
		t.Fatal(err)
	}
	root, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.SASL(scram.Auth{User: "admin", Pass: "password"}.AsSha512Mechanism()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(root.Close)
	operator, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.SASL(plain.Auth{User: "wherry.operator", Pass: "operator-password"}.AsMechanism()), kgo.FetchMaxWait(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	b := &bus{ctx: ctx, service: &admin.Service{Admin: root, Operator: operator, Broker: cluster.ListenAddrs()[0], DeliveryCeiling: 10}, stateDir: t.TempDir(), logs: &syncBuffer{}}
	optional := false
	terms := admin.Terms{Agents: []admin.AgentTerms{{Name: "claude", Credentials: []admin.AgentCredential{
		{TaskCredential: wire.TaskCredential{Name: "model", Env: "MODEL_API_KEY"}},
		{TaskCredential: wire.TaskCredential{Name: "github-token", Env: "GH_TOKEN"}, Required: &optional},
	}}}, DefaultAgent: "claude", DeliveryLimit: 3, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50,
		VCS: []string{"git.example.test"}}
	if err := b.service.PoolCreate(ctx, "pool-a", terms); err != nil {
		t.Fatal(err)
	}
	if err := b.service.TenantAdd(ctx, "tenant-a", "pool-a"); err != nil {
		t.Fatal(err)
	}
	b.hostBundle = filepath.Join(t.TempDir(), "host.yaml")
	if err := b.service.HostAdd(ctx, "tenant-a", "host-a", b.hostBundle); err != nil {
		t.Fatal(err)
	}
	receiverBundle := filepath.Join(t.TempDir(), "receiver.yaml")
	config := admin.ReceiverConfig{Receiver: "receiver-a", Pool: "pool-a", Capacity: 1, Profiles: []admin.ProfileSettings{{Name: "lab", Plugin: "nomad", Agent: "claude", Labels: map[string]string{"runtime": "exec"}}},
		PluginSHA256: map[string]string{"nomad": fakeSHA256}}
	if err := b.service.ReceiverAdd(ctx, config, receiverBundle); err != nil {
		t.Fatal(err)
	}
	model := filepath.Join(t.TempDir(), "model")
	if err := os.WriteFile(model, []byte(credentialText), 0o600); err != nil {
		t.Fatal(err)
	}
	b.credentials = map[string]credential.Source{"model": {File: model}}
	if _, b.hostKey, err = ed25519.GenerateKey(rand.Reader); err != nil {
		t.Fatal(err)
	}
	if b.client, err = b.connect(t)(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.client.Close)
	if _, err := b.client.Register(ctx); err != nil {
		t.Fatal(err)
	}
	b.receiver, _ = b.kafka(t, receiverBundle)
	t.Cleanup(b.receiver.Close)
	if _, b.receiverKey, err = ed25519.GenerateKey(rand.Reader); err != nil {
		t.Fatal(err)
	}
	registration, err := signed.RegisterKey("receiver-a", b.receiverKey)
	if err != nil {
		t.Fatal(err)
	}
	b.produce(t, admin.ReceiverTopic("receiver-a"), "receiver-a", registration)
	b.produce(t, admin.ReceiversTopic, "receiver-a", b.sign(t, &wire.Advertisement{Receiver: "receiver-a", Pool: "pool-a",
		Offers: []wire.Offer{{Profile: "lab", Agent: "claude", InlineLimit: 49152}}, Capacity: 1, AtMS: time.Now().UnixMilli()}))
	return b
}

// kafka opens a plain client as the principal of the bundle at path.
func (b *bus) kafka(t *testing.T, path string) (*kgo.Client, string) {
	t.Helper()
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var bundle admin.Bundle
	if err := yaml.Unmarshal(encoded, &bundle); err != nil {
		t.Fatal(err)
	}
	// kfake's SCRAM server always offers 4096 iterations, so the 8192 that admin provisions never authenticates.
	if _, err := kadm.NewClient(b.service.Admin).AlterUserSCRAMs(b.ctx, nil, []kadm.UpsertSCRAM{{User: bundle.Principal, Mechanism: kadm.ScramSha512, Iterations: 4096, Password: bundle.Password}}); err != nil {
		t.Fatal(err)
	}
	kafka, err := kgo.NewClient(kgo.SeedBrokers(b.service.Broker), kgo.SASL(scram.Auth{User: bundle.Principal, Pass: bundle.Password}.AsSha512Mechanism()),
		kgo.FetchMaxWait(10*time.Millisecond), kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	if err != nil {
		t.Fatal(err)
	}
	return kafka, bundle.Principal
}

// connect opens a host per call as Connect does, over the plaintext kfake listener.
func (b *bus) connect(t *testing.T) func() (*busclient.Host, error) {
	kafka, principal := b.kafka(t, b.hostBundle)
	kafka.Close()
	return func() (*busclient.Host, error) {
		encoded, _ := os.ReadFile(b.hostBundle)
		var bundle admin.Bundle
		if err := yaml.Unmarshal(encoded, &bundle); err != nil {
			return nil, err
		}
		kafka, err := kgo.NewClient(kgo.SeedBrokers(b.service.Broker), kgo.SASL(scram.Auth{User: bundle.Principal, Pass: bundle.Password}.AsSha512Mechanism()),
			kgo.FetchMaxWait(10*time.Millisecond), kgo.FetchIsolationLevel(kgo.ReadCommitted()))
		if err != nil {
			return nil, err
		}
		return busclient.NewHost(kafka, b.hostKey, principal, b.stateDir, b.credentials, b.git, log.New(b.logs, "", 0))
	}
}

func (b *bus) produce(t *testing.T, topic, key string, value []byte) {
	t.Helper()
	if err := b.receiver.ProduceSync(b.ctx, &kgo.Record{Topic: topic, Key: []byte(key), Value: value}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func (b *bus) sign(t *testing.T, record wire.Record) []byte {
	t.Helper()
	encoded, err := wire.SignRecord(record, b.receiverKey)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

type task struct {
	submission wire.Submission
	hash       string
}

// tasks reads and authorises runID's turns from pool-a's task topic as receiver-a does.
func (b *bus) tasks(t *testing.T, runID string) []task {
	t.Helper()
	state, err := admin.Load(b.ctx, b.receiver)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := admin.ReadKeys(b.ctx, b.receiver, admin.HostKeyTopics(state, "pool-a"))
	if err != nil {
		t.Fatal(err)
	}
	records, err := readTopic(b.ctx, b.receiver, admin.PoolTasksTopic("pool-a"))
	if err != nil {
		t.Fatal(err)
	}
	var tasks []task
	for _, record := range records {
		submission, err := admin.AuthorizeSubmission(state, keys, record.Value, "pool-a")
		if err != nil {
			t.Fatal(err)
		}
		if submission.Wherry.Run == runID {
			sum := sha256.Sum256(record.Value)
			tasks = append(tasks, task{submission, hex.EncodeToString(sum[:])})
		}
	}
	return tasks
}

func (b *bus) binding(task task) wire.ReceiverBinding {
	return wire.ReceiverBinding{Receiver: "receiver-a", Tenant: "tenant-a", Pool: "pool-a", Run: task.submission.Wherry.Run, Attempt: 1}
}

func (b *bus) echo(t *testing.T, task task) []byte {
	binding := task.submission.Wherry
	return b.sign(t, &wire.Echo{Receiver: "receiver-a", Tenant: "tenant-a", Pool: "pool-a", Run: binding.Run, Placement: binding.Placement,
		TaskHash: task.hash, MessageID: task.submission.Message.MessageID})
}

func (b *bus) status(t *testing.T, task task, eventID, kind string, status wire.TaskStatus) []byte {
	run := task.submission.Wherry.Run
	return b.sign(t, &wire.RunStatus{StatusUpdate: wire.TaskStatusUpdateEvent{TaskID: run, ContextID: run, Status: status},
		Wherry: wire.StatusBinding{ReceiverBinding: b.binding(task), EventID: eventID, Kind: kind, TaskHash: task.hash}})
}

func (b *bus) result(t *testing.T, task task, parts []wire.Part) []byte {
	run := task.submission.Wherry.Run
	status := now(wire.TaskStateCompleted)
	status.Message = &wire.Message{MessageID: "msg-status", Role: wire.RoleAgent, Parts: parts, Extensions: []string{wire.ExtensionURI}}
	return b.sign(t, &wire.Result{Task: wire.Task{ID: run, ContextID: run, History: []wire.Message{task.submission.Message}, Status: status,
		Artifacts: []wire.Artifact{{ArtifactID: "result", Parts: parts, Extensions: []string{wire.ExtensionURI}}}},
		Wherry: wire.ResultBinding{ReceiverBinding: b.binding(task), TaskHash: task.hash}})
}

func now(state wire.TaskState) wire.TaskStatus {
	return wire.TaskStatus{State: state, Timestamp: time.Now().UTC().Format("2006-01-02T15:04:05.000Z")}
}

// feed commits records in one transaction, as receiver-a does: chunks and outputs to tenant-a's transcript topic, the
// others to its feed.
func (b *bus) feed(t *testing.T, runID string, values ...[]byte) {
	t.Helper()
	producer, err := kgo.NewClient(append(b.receiver.Opts(), kgo.TransactionalID(admin.TransactionalPrefix("pool-a")+runID))...)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if err := producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	for _, value := range values {
		topic := admin.TenantTopic("tenant-a")
		if jws, err := jose.ParseSignedCompact(string(value), []jose.SignatureAlgorithm{jose.EdDSA}); err == nil {
			if typ := jws.Signatures[0].Protected.ExtraHeaders[jose.HeaderType]; typ == wire.ChunkType || typ == wire.OutputType {
				topic = admin.TenantTranscriptTopic("tenant-a")
			}
		}
		if err := producer.ProduceSync(b.ctx, &kgo.Record{Topic: topic, Key: []byte(runID), Value: value}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	if err := producer.EndTransaction(b.ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}
}

// admitted submits a task and writes its echo and claimed status, as receiver-a's admission does.
func (b *bus) admitted(t *testing.T) task {
	t.Helper()
	submitted, err := b.client.Submit(b.ctx, busclient.SubmitInput{Selector: []wire.Requirement{}, Prompt: "work", EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	admitted := b.tasks(t, submitted.RunID)[0]
	b.feed(t, submitted.RunID, b.echo(t, admitted), b.status(t, admitted, "claimed-1", "claimed", now(wire.TaskStateWorking)))
	return admitted
}

func TestReceiversJSONOutputShape(t *testing.T) {
	b := newBus(t)
	silent := admin.ReceiverConfig{Receiver: "receiver-b", Pool: "pool-a", Capacity: 1, Profiles: []admin.ProfileSettings{{Name: "lab", Plugin: "nomad", Agent: "claude"}}, PluginSHA256: map[string]string{"nomad": fakeSHA256}}
	if err := b.service.ReceiverAdd(b.ctx, silent, filepath.Join(t.TempDir(), "receiver.yaml")); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := runReceivers(b.ctx, b.client, []string{"--json"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}
	var rows []map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	keys := make([]string, 0, len(rows[0]))
	for key := range rows[0] {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	wantKeys := []string{"capacity", "in_flight", "last_advertised", "pool", "profiles", "receiver_id", "status"}
	if !reflect.DeepEqual(keys, wantKeys) {
		t.Fatalf("got JSON fields %v, want %v", keys, wantKeys)
	}
	if advertised, err := time.Parse(time.RFC3339, rows[0]["last_advertised"].(string)); err != nil || time.Since(advertised) > time.Minute {
		t.Fatalf("last_advertised %v, %v; want the RFC 3339 time of the live receiver's advertisement", rows[0]["last_advertised"], err)
	}
	if _, ok := rows[1]["last_advertised"]; ok || rows[1]["receiver_id"] != "receiver-b" || rows[1]["status"] != "silent" {
		t.Fatalf("row %#v; want the silent receiver without last_advertised", rows[1])
	}
	if rows[0]["receiver_id"] != "receiver-a" || rows[0]["status"] != "live" || rows[0]["pool"] != "pool-a" {
		t.Fatalf("unexpected JSON row: %#v", rows[0])
	}
	stdout.Reset()
	if err := runReceivers(b.ctx, b.client, nil, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "  profile=lab plugin=nomad agent=claude ready=true labels=runtime=exec inline_limit=49152") {
		t.Fatalf("receivers output %q does not carry the profile, its plugin and its inline limit", stdout.String())
	}
	// The agent's credentials by name, with whether this host has a source for each; their targets stay out.
	if output := stdout.String(); !strings.Contains(output, " credentials=model,github-token(missing,optional)\n") || strings.Contains(output, "GH_TOKEN") {
		t.Fatalf("receivers output %q, want the agent's credentials marked configured or missing", output)
	}
}

func TestReceiversShowsEnded(t *testing.T) {
	b := newBus(t)
	end, err := admin.EncodeRecord(admin.Record{Kind: "end", Receiver: "receiver-a"})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.service.Operator.ProduceSync(b.ctx, &kgo.Record{Topic: admin.AdminTopic, Value: end}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := runReceivers(b.ctx, b.client, nil, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if want := "receiver=receiver-a pool=pool-a capacity=0 in_flight=0 heartbeat_age=- status=ended\n"; stdout.String() != want || stderr.Len() != 0 {
		t.Fatalf("stdout %q stderr %q, want %q", stdout.String(), stderr.String(), want)
	}
	stdout.Reset()
	if err := runReceivers(b.ctx, b.client, []string{"--json"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if want := `[{"receiver_id":"receiver-a","pool":"pool-a","profiles":[],"capacity":0,"in_flight":0,"status":"ended"}]` + "\n"; stdout.String() != want {
		t.Fatalf("stdout %q, want %q", stdout.String(), want)
	}
}

func TestSubmitNeverPrintsCredential(t *testing.T) {
	b := newBus(t)
	var stdout, stderr bytes.Buffer
	if err := runSubmit(b.ctx, b.client, []string{"--prompt", "work"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(stdout.String()+stderr.String(), credentialText) {
		t.Fatalf("submit printed the credential: stdout %q stderr %q", stdout.String(), stderr.String())
	}
	// Submit seals nothing; the receiver that claims the task asks for the credential.
	if records, err := readTopic(b.ctx, b.receiver, admin.HostTopic("tenant-a", "host-a")); err != nil || len(records) != 1 {
		t.Fatalf("host topic holds %d records, %v; want the key registration alone", len(records), err)
	}
}

func TestSubmitModelOutsideTheTerms(t *testing.T) {
	b := newBus(t)
	var stdout, stderr bytes.Buffer
	err := runSubmit(b.ctx, b.client, []string{"--prompt", "work", "--agent", "claude", "--model", "sonnet"}, &stdout, &stderr)
	if !errors.Is(err, busclient.ErrInvalidInput) || !strings.Contains(err.Error(), "model sonnet is not in agent claude's models in pool pool-a") || stdout.Len() != 0 {
		t.Fatalf("submit = %v, stdout %q; want the model, agent and pool named", err, stdout.String())
	}
}

// waitOutput publishes a completed result with parts for an admitted run and returns what wait prints.
func waitOutput(t *testing.T, parts []wire.Part) string {
	t.Helper()
	b := newBus(t)
	admitted := b.admitted(t)
	runID := admitted.submission.Wherry.Run
	b.feed(t, runID, b.result(t, admitted, parts), b.status(t, admitted, "terminal-1", "terminal", now(wire.TaskStateCompleted)))
	var stdout, stderr bytes.Buffer
	if err := runWait(b.ctx, b.client, []string{runID, "--timeout", "5s"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	return stdout.String()
}

func TestWaitPrintsNonTextPartPlaceholders(t *testing.T) {
	got := waitOutput(t, []wire.Part{
		{Text: "done"},
		{Raw: wire.StdBytes("hello"), MediaType: "text/plain"},
		{URL: "https://example.test/report.pdf"},
	})
	if want := "done\n[raw text/plain 5 bytes]\n[url https://example.test/report.pdf]\n"; got != want {
		t.Fatalf("stdout %q, want %q", got, want)
	}
}

func TestWaitPrintsDataPart(t *testing.T) {
	got := waitOutput(t, []wire.Part{{Data: jsontext.Value(`{"answer":42,"items":["a","b"]}`), MediaType: "application/json"}})
	if want := "{\"answer\":42,\"items\":[\"a\",\"b\"]}\n"; got != want {
		t.Fatalf("stdout %q, want %q", got, want)
	}
}

// publishFailedRun admits a run and fails it after its credential is not answered.
func publishFailedRun(t *testing.T) (*bus, task) {
	t.Helper()
	b := newBus(t)
	admitted := b.admitted(t)
	status := now(wire.TaskStateFailed)
	status.Message = &wire.Message{MessageID: "msg-status", Role: wire.RoleAgent, Parts: []wire.Part{{Text: "credential token was not answered within 5m"}}, Extensions: []string{wire.ExtensionURI}}
	runID := admitted.submission.Wherry.Run
	result := &wire.Result{Task: wire.Task{ID: runID, ContextID: runID, History: []wire.Message{admitted.submission.Message}, Status: status, Artifacts: []wire.Artifact{}},
		Wherry: wire.ResultBinding{ReceiverBinding: b.binding(admitted), TaskHash: admitted.hash}}
	b.feed(t, runID, b.sign(t, result), b.status(t, admitted, "failed", "terminal", status))
	return b, admitted
}

// cancelRun reads the host's cancel from pool-a's events and writes its CANCELED terminal.
func (b *bus) cancelRun(t *testing.T, admitted task) {
	t.Helper()
	state, err := admin.Load(b.ctx, b.receiver)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := admin.ReadKeys(b.ctx, b.receiver, admin.HostKeyTopics(state, "pool-a"))
	if err != nil {
		t.Fatal(err)
	}
	records, err := readTopic(b.ctx, b.receiver, admin.PoolEventsTopic("pool-a"))
	if err != nil || len(records) != 1 {
		t.Fatalf("pool events hold %d records, %v", len(records), err)
	}
	runID := admitted.submission.Wherry.Run
	if cancel, err := admin.AuthorizeCancel(state, keys, records[0].Value, "pool-a", "tenant-a"); err != nil || cancel.Run != runID {
		t.Fatalf("cancel %#v, %v", cancel, err)
	}
	b.feed(t, runID, b.status(t, admitted, runID+"/1/canceled", "terminal", now(wire.TaskStateCanceled)))
}

func TestWaitPrintsFailed(t *testing.T) {
	b, admitted := publishFailedRun(t)
	runID := admitted.submission.Wherry.Run
	var stdout, stderr bytes.Buffer
	err := runWait(b.ctx, b.client, []string{runID, "--timeout", "5s"}, &stdout, &stderr)
	if err == nil || err.Error() != "run ended in TASK_STATE_FAILED" {
		t.Fatalf("error %v", err)
	}
	want := "credential token was not answered within 5m\n"
	if got := stdout.String(); got != want || stderr.Len() != 0 {
		t.Fatalf("stdout %q stderr %q, want %q", stdout.String(), stderr.String(), want)
	}
}

func TestCancelCLI(t *testing.T) {
	b := newBus(t)
	admitted := b.admitted(t)
	runID := admitted.submission.Wherry.Run
	var stdout, stderr bytes.Buffer
	if err := runCancel(b.ctx, b.client, []string{runID}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "run_id="+runID+"\n" || stderr.Len() != 0 {
		t.Fatalf("cancel stdout %q stderr %q", stdout.String(), stderr.String())
	}
	b.cancelRun(t, admitted)
	stdout.Reset()
	err := runWait(b.ctx, b.client, []string{runID, "--timeout", "5s"}, &stdout, &stderr)
	if err == nil || err.Error() != "run ended in TASK_STATE_CANCELED" {
		t.Fatalf("wait error %v", err)
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("wait stdout %q stderr %q", stdout.String(), stderr.String())
	}
	stdout.Reset()
	if err := runEvents(b.ctx, b.client, []string{runID}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if lines := strings.Split(strings.TrimSuffix(stdout.String(), "\n"), "\n"); len(lines) != 2 ||
		!strings.Contains(lines[1], "kind=terminal state=TASK_STATE_CANCELED receiver=receiver-a") {
		t.Fatalf("events stdout %q", stdout.String())
	}
	if err := runCancel(b.ctx, b.client, []string{"run-missing"}, &stdout, &stderr); err == nil || err.Error() != "cancel run-missing: run not found" {
		t.Fatalf("cancel of an unknown run: got %v", err)
	}
}

func TestStatusOfAWorkingRunExitsZero(t *testing.T) {
	b := newBus(t)
	runID := b.admitted(t).submission.Wherry.Run
	var stdout, stderr bytes.Buffer
	if err := runStatus(b.ctx, b.client, []string{runID}, &stdout, &stderr); err != nil {
		t.Fatalf("status of a WORKING run: %v", err)
	}
	if want := "TASK_STATE_WORKING\n"; stdout.String() != want || stderr.Len() != 0 {
		t.Fatalf("stdout %q stderr %q, want %q", stdout.String(), stderr.String(), want)
	}
}

func TestStatusPrintsFailed(t *testing.T) {
	b, admitted := publishFailedRun(t)
	runID := admitted.submission.Wherry.Run
	var stdout, stderr bytes.Buffer
	if err := runStatus(b.ctx, b.client, []string{runID}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	want := "TASK_STATE_FAILED\ncredential token was not answered within 5m\n"
	if stdout.String() != want || stderr.Len() != 0 {
		t.Fatalf("stdout %q stderr %q, want %q", stdout.String(), stderr.String(), want)
	}
}

func TestWaitAndStatusReportAnUnclaimedRunPastItsStartBy(t *testing.T) {
	b := newBus(t)
	submitted, err := b.client.Submit(b.ctx, busclient.SubmitInput{Selector: []wire.Requirement{}, Prompt: "work", EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	start := time.Now()
	err = runWait(b.ctx, b.client, []string{submitted.RunID, "--timeout", "20s"}, &stdout, &stderr)
	if elapsed := time.Since(start); elapsed > 6*time.Second {
		t.Fatalf("wait returned after %s, want soon after the 2s task_started_by", elapsed)
	}
	if err == nil || !strings.HasPrefix(err.Error(), "not started by task_started_by (") || !strings.HasSuffix(err.Error(), "): no receiver claimed the task") {
		t.Fatalf("wait error %v", err)
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("wait stdout %q stderr %q, want nothing", stdout.String(), stderr.String())
	}
	unclaimed := err.Error()
	stdout.Reset()
	if err := runStatus(b.ctx, b.client, []string{submitted.RunID}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if want := "TASK_STATE_SUBMITTED\n" + unclaimed + "\n"; stdout.String() != want || stderr.Len() != 0 {
		t.Fatalf("status stdout %q stderr %q, want %q", stdout.String(), stderr.String(), want)
	}
}

func TestStatusOfAnUnknownRunFails(t *testing.T) {
	b := newBus(t)
	var stdout, stderr bytes.Buffer
	if err := runStatus(b.ctx, b.client, []string{"run-missing"}, &stdout, &stderr); !errors.Is(err, busclient.ErrRunNotFound) || stdout.Len() != 0 {
		t.Fatalf("status of an unknown run: %v, stdout %q", err, stdout.String())
	}
}

func TestStatusListsRuns(t *testing.T) {
	b := newBus(t)
	var stdout, stderr bytes.Buffer
	if err := runStatus(b.ctx, b.client, nil, &stdout, &stderr); err != nil || stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("empty list: %v, stdout %q stderr %q", err, stdout.String(), stderr.String())
	}
	working := b.admitted(t).submission.Wherry.Run
	submitted, err := b.client.Submit(b.ctx, busclient.SubmitInput{Selector: []wire.Requirement{}, Prompt: "work", EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if err := runStatus(b.ctx, b.client, nil, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	want := submitted.RunID + " pool=pool-a TASK_STATE_SUBMITTED\n" + working + " pool=pool-a TASK_STATE_WORKING\n"
	if stdout.String() != want || stderr.Len() != 0 {
		t.Fatalf("stdout %q stderr %q, want %q", stdout.String(), stderr.String(), want)
	}
}

func TestEventsPrintsStatusesAndChunkCounts(t *testing.T) {
	b := newBus(t)
	submitted, err := b.client.Submit(b.ctx, busclient.SubmitInput{Selector: []wire.Requirement{}, Prompt: "work", EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	admitted := b.tasks(t, submitted.RunID)[0]
	extensions := []string{wire.ExtensionURI}
	// The terminal counts its attempt's chunks.
	rejected := b.sign(t, &wire.RunStatus{
		StatusUpdate: wire.TaskStatusUpdateEvent{TaskID: submitted.RunID, ContextID: submitted.RunID, Status: wire.TaskStatus{State: wire.TaskStateRejected, Timestamp: "2026-10-17T10:00:00.000Z",
			Message: &wire.Message{MessageID: "msg-1", Role: wire.RoleAgent, Parts: []wire.Part{{Text: "not started by task_started_by (2026-10-17T10:00:00Z)"}}, Extensions: extensions}}},
		Wherry: wire.StatusBinding{ReceiverBinding: b.binding(admitted), EventID: "terminal-1", Kind: "terminal", TaskHash: admitted.hash, TranscriptChunks: 2},
	})
	records := [][]byte{b.echo(t, admitted), rejected}
	for seq := range int64(2) {
		records = append(records, b.sign(t, &wire.Chunk{
			ArtifactUpdate: wire.TaskArtifactUpdateEvent{TaskID: submitted.RunID, ContextID: submitted.RunID, Append: seq > 0, Artifact: wire.Artifact{
				ArtifactID: "transcript", Parts: []wire.Part{{Text: "line", MediaType: "application/x-ndjson"}}, Extensions: extensions,
			}},
			Wherry: wire.ChunkBinding{ReceiverBinding: b.binding(admitted), TaskHash: admitted.hash, Seq: seq},
		}))
	}
	b.feed(t, submitted.RunID, records...)
	var stdout, stderr bytes.Buffer
	if err := runEvents(b.ctx, b.client, []string{submitted.RunID}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	want := "attempt=1 kind=terminal state=TASK_STATE_REJECTED receiver=receiver-a time=2026-10-17T10:00:00.000Z message=\"not started by task_started_by (2026-10-17T10:00:00Z)\"\n" +
		"attempt=1 transcript_chunks=2\n"
	if stdout.String() != want {
		t.Fatalf("stdout %q, want %q", stdout.String(), want)
	}
}

func TestLogsPrintsTheLatestAttemptsLines(t *testing.T) {
	b, runID := transcriptRun(t)
	var stdout, stderr bytes.Buffer
	if err := runLogs(b.ctx, b.client, []string{runID}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	wantOut := "{\"type\":\"start\"}\nsecond [redacted:model]\n\xff\xfe\n\"q\"\n\n" + transcriptBig + "\ntail\nstale past the second attempt's first line\n"
	wantErr := "line at offset 40 not stored: sha256=" + hashOf("x") + " size=600000\nmodel redacted 1 times\n"
	if stdout.String() != wantOut || stderr.String() != wantErr {
		t.Fatalf("stdout %q stderr %q, want %q and %q", stdout.String(), stderr.String(), wantOut, wantErr)
	}
	stdout.Reset()
	stderr.Reset()
	if err := runEvents(b.ctx, b.client, []string{runID}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), `message="using [redacted:model]"`) || stderr.String() != "model redacted 1 times\n" {
		t.Fatalf("events stdout %q stderr %q", stdout.String(), stderr.String())
	}
}

func TestEmptyPromptFileIsRefused(t *testing.T) {
	empty := filepath.Join(t.TempDir(), "empty.md")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	if err := runSubmit(context.Background(), nil, []string{"--prompt-file", empty}, &stderr, &stderr); err == nil || err.Error() != "--prompt-file is empty" {
		t.Fatalf("submit: %v", err)
	}
}

// --prompt-file - reads the prompt from standard input, though the directory holds a file named -.
func TestPromptFileReadsStandardInput(t *testing.T) {
	b := newBus(t)
	dir := t.TempDir()
	t.Chdir(dir)
	if err := os.WriteFile(filepath.Join(dir, "-"), []byte("from the file"), 0o600); err != nil {
		t.Fatal(err)
	}
	defer func(previous io.Reader) { stdin = previous }(stdin)
	stdin = strings.NewReader("from standard input")
	var stdout, stderr bytes.Buffer
	if err := runSubmit(b.ctx, b.client, []string{"--prompt-file", "-"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	runID, _, _ := strings.Cut(strings.TrimPrefix(stdout.String(), "run_id="), " ")
	if text := b.tasks(t, runID)[0].submission.Message.Parts[0].Text; !strings.Contains(text, "from standard input") || strings.Contains(text, "from the file") {
		t.Fatalf("prompt %q, want standard input's", text)
	}
	stdin = strings.NewReader("")
	if err := runSubmit(b.ctx, b.client, []string{"--prompt-file", "-"}, &stdout, &stderr); err == nil || err.Error() != "--prompt-file is empty" {
		t.Fatalf("submit with empty standard input: %v", err)
	}
}

func TestCleanupRefusesAnUnfinishedRun(t *testing.T) {
	b := newBus(t)
	runID := b.admitted(t).submission.Wherry.Run
	var stdout, stderr bytes.Buffer
	if err := runCleanup(b.ctx, b.client, []string{runID}, &stdout, &stderr); !errors.Is(err, busclient.ErrInvalidInput) || !strings.Contains(err.Error(), "has not ended") {
		t.Fatalf("cleanup of a working run: %v", err)
	}
}

func TestCleanupRecognizesAFailedRunAsEnded(t *testing.T) {
	b, admitted := publishFailedRun(t)
	var stdout, stderr bytes.Buffer
	if err := runCleanup(b.ctx, b.client, []string{admitted.submission.Wherry.Run}, &stdout, &stderr); err == nil || !strings.Contains(err.Error(), "no storage credential") || strings.Contains(err.Error(), "has not ended") {
		t.Fatalf("cleanup of a failed run: %v", err)
	}
}

func TestUsageErrors(t *testing.T) {
	for _, test := range []struct {
		name string
		call func(*bytes.Buffer) error
		want string
	}{
		{"submit argument", func(stderr *bytes.Buffer) error {
			return runSubmit(context.Background(), nil, []string{"--selector", "a=b", "extra"}, stderr, stderr)
		}, "usage: wherry submit"},
		{"submit labels", func(stderr *bytes.Buffer) error {
			return runSubmit(context.Background(), nil, []string{"--selector", "a=b", "--labels", "region=eu", "--prompt", "p"}, stderr, stderr)
		}, "flag provided but not defined: -labels"},
		{"cancel argument", func(stderr *bytes.Buffer) error {
			return runCancel(context.Background(), nil, []string{"run-a", "extra"}, stderr, stderr)
		}, "usage: wherry cancel"},
		{"wait argument", func(stderr *bytes.Buffer) error {
			return runWait(context.Background(), nil, []string{"run-a", "extra"}, stderr, stderr)
		}, "usage: wherry wait"},
		{"wait quiet", func(stderr *bytes.Buffer) error {
			return runWait(context.Background(), nil, []string{"run-a", "-q"}, stderr, stderr)
		}, "flag provided but not defined: -q"},
		{"wait long quiet", func(stderr *bytes.Buffer) error {
			return runWait(context.Background(), nil, []string{"run-a", "--quiet"}, stderr, stderr)
		}, "flag provided but not defined: -quiet"},
		{"wait result only", func(stderr *bytes.Buffer) error {
			return runWait(context.Background(), nil, []string{"run-a", "--result-only"}, stderr, stderr)
		}, "flag provided but not defined: -result-only"},
		{"logs argument", func(stderr *bytes.Buffer) error {
			return runLogs(context.Background(), nil, []string{"run-a", "extra"}, stderr, stderr)
		}, "usage: wherry logs"},
		{"logs offset", func(stderr *bytes.Buffer) error {
			return runLogs(context.Background(), nil, []string{"run-a", "--offset", "1"}, stderr, stderr)
		}, "flag provided but not defined: -offset"},
		{"logs limit", func(stderr *bytes.Buffer) error {
			return runLogs(context.Background(), nil, []string{"run-a", "--limit", "1"}, stderr, stderr)
		}, "flag provided but not defined: -limit"},
		{"removed watch", func(stderr *bytes.Buffer) error {
			return run([]string{"watch", "run-a"}, stderr, stderr)
		}, "unknown command \"watch\""},
		{"removed transcript", func(stderr *bytes.Buffer) error {
			return run([]string{"transcript", "run-a"}, stderr, stderr)
		}, "unknown command \"transcript\""},
		{"status argument", func(stderr *bytes.Buffer) error {
			return runStatus(context.Background(), nil, []string{"run-a", "extra"}, stderr, stderr)
		}, "usage: wherry status"},
		{"submit selector", func(stderr *bytes.Buffer) error {
			return runSubmit(context.Background(), nil, []string{"--selector", "runtime==exec", "--prompt", "p"}, stderr, stderr)
		}, "selector: requirement values must be valid and unique"},
		{"submit end-by", func(stderr *bytes.Buffer) error {
			return runSubmit(context.Background(), nil, []string{"--selector", "a=b", "--prompt", "p", "--end-by", "0s"}, stderr, stderr)
		}, "--end-by: not after now"},
		{"wait timeout", func(stderr *bytes.Buffer) error {
			return runWait(context.Background(), nil, []string{"run-a", "--timeout", "0s"}, stderr, stderr)
		}, "timeout must be positive"},
		{"receiver without run", func(stderr *bytes.Buffer) error {
			return run([]string{"receiver", "--bundle-file", "x"}, stderr, stderr)
		}, "usage: wherry receiver run"},
		{"receiver with another verb", func(stderr *bytes.Buffer) error {
			return run([]string{"receiver", "start", "--bundle-file", "x"}, stderr, stderr)
		}, "usage: wherry receiver run"},
		{"removed enrol", func(stderr *bytes.Buffer) error {
			return run([]string{"enrol", "list"}, stderr, stderr)
		}, "unknown command \"enrol\""},
		{"removed secret", func(stderr *bytes.Buffer) error {
			return run([]string{"secret", "put", "token"}, stderr, stderr)
		}, "unknown command \"secret\""},
		{"removed await", func(stderr *bytes.Buffer) error {
			return run(strings.Fields("await run-a"), stderr, stderr)
		}, "unknown command \"await\""},
		{"removed resume", func(stderr *bytes.Buffer) error {
			return run([]string{"resume", "run-a"}, stderr, stderr)
		}, "unknown command \"resume\""},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			if err := test.call(&output); err == nil || !strings.HasPrefix(err.Error(), test.want) {
				t.Fatalf("got %v, want %q", err, test.want)
			}
		})
	}
}

func TestKeyAndInit(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "config.yaml")
	keyPath := filepath.Join(dir, "keys", "host.key")
	if err := os.WriteFile(config, []byte("bundle: "+filepath.Join(dir, "missing.yaml")+"\nhost_key: "+keyPath+"\nstate_dir: "+dir+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := run([]string{"init", "--config-file", config}, &stdout, &stderr); err == nil || !strings.HasPrefix(err.Error(), "read bundle") {
		t.Fatalf("init without a bundle: %v", err)
	}
	key, err := trust.LoadSigningKeyFile(keyPath)
	if err != nil {
		t.Fatalf("init did not create the host key: %v", err)
	}
	if err := run([]string{"init", "--config-file", config}, &stdout, &stderr); err == nil || !strings.HasPrefix(err.Error(), "read bundle") {
		t.Fatalf("second init: %v", err)
	}
	if again, err := trust.LoadSigningKeyFile(keyPath); err != nil || !again.Equal(key) {
		t.Fatal("second init replaced the host key")
	}
	if err := run([]string{"key", "--config-file", config, "extra"}, &stdout, &stderr); err == nil || err.Error() != "usage: wherry key [rotate] [--config-file FILE]" {
		t.Fatalf("got %v, want key usage error", err)
	}
	if err := run([]string{"key", "--config-file", config}, &stdout, &stderr); err != nil || stdout.String() != signed.KeyID(key.Public().(ed25519.PublicKey))+"\n" {
		t.Fatalf("key: %v, stdout %q", err, stdout.String())
	}
}

func TestSubmitBadGitInputWithoutReceiver(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runSubmit(context.Background(), &busclient.Host{}, []string{"--selector", "runtime=exec", "--prompt", "x", "--git", "https://user:s3cr3t@git.example.test/r.git@main"}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "https git remote must not carry user information") || strings.Contains(err.Error(), "s3cr3t") {
		t.Fatalf("submit error %v, want the git rule before receiver matching", err)
	}
}

func TestSubmitGitFlags(t *testing.T) {
	b := newBus(t)
	hostKey := "git.example.test ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl\n"
	dir := t.TempDir()
	for name, contents := range map[string]string{"known_hosts": hostKey, "other_known_hosts": "# the shorter prefix's key\n" + hostKey, "deploy-key": "deploy-key-value"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	b.credentials["deploy-key"] = credential.Source{File: filepath.Join(dir, "deploy-key")}
	b.git = []busclient.GitCredential{
		{URL: "ssh://git@git.example.test/", Credential: "model", HostKey: filepath.Join(dir, "other_known_hosts")},
		{URL: "ssh://git@git.example.test/org/", Credential: "deploy-key", HostKey: filepath.Join(dir, "known_hosts")},
	}
	client, err := b.connect(t)()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	base := []string{"--agent", "claude", "--selector", "runtime=exec,!gpu", "--prompt", "fix the bug"}
	for name, test := range map[string]struct {
		flags []string
		want  string
	}{
		"no ref":            {[]string{"--git", "https://git.example.test/org/repo.git"}, "--git takes REMOTE@REF"},
		"path without git":  {[]string{"--git-path", "repo"}, "--git-path and --git-changes need --git"},
		"changes alone":     {[]string{"--git-changes", "patch"}, "--git-path and --git-changes need --git"},
		"userinfo":          {[]string{"--git", "https://user:s3cr3t@git.example.test/org/repo.git@main"}, "https git remote must not carry user information"},
		"traversal":         {[]string{"--git", "https://git.example.test/org/repo.git@main", "--git-path", "../x"}, "git path must be a relative path"},
		"outside vcs hosts": {[]string{"--git", "https://github.com/org/repo.git@main"}, "git remote host github.com is not in pool pool-a's vcs hosts"},
		"vcs host port":     {[]string{"--git", "https://git.example.test:8443/org/repo.git@main"}, "git remote host git.example.test:8443 is not in pool pool-a's vcs hosts"},
		"ssh without key":   {[]string{"--git", "ssh://deploy@git.example.test/org/repo.git@main"}, "ssh git remote requires host_key"},
		"changes":           {[]string{"--git", "https://git.example.test/org/repo.git@main", "--git-changes", "diff"}, "git changes must be patch or bundle"},
		"credential twice":  {[]string{"--git", "git@git.example.test:org/repo.git@main", "--credential", "deploy-key:env=DEPLOY_KEY"}, "credential deploy-key is listed twice"},
	} {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := runSubmit(b.ctx, client, append(slices.Clone(base), test.flags...), &stdout, &stderr)
			if err == nil || !strings.Contains(err.Error(), test.want) || strings.Contains(err.Error(), "s3cr3t") {
				t.Fatalf("submit error %v, want %q", err, test.want)
			}
		})
	}
	if records, err := readTopic(b.ctx, b.receiver, admin.PoolTasksTopic("pool-a")); err != nil || len(records) != 0 {
		t.Fatalf("refused submissions published %d tasks, %v", len(records), err)
	}
	submit := func(flags ...string) wire.Submission {
		t.Helper()
		var stdout, stderr bytes.Buffer
		if err := runSubmit(b.ctx, client, append(slices.Clone(base), flags...), &stdout, &stderr); err != nil {
			t.Fatal(err)
		}
		runID, _, _ := strings.Cut(strings.TrimPrefix(stdout.String(), "run_id="), " ")
		return b.tasks(t, runID)[0].submission
	}
	// The longest URL prefix of the remote gives the clone its credential and host key.
	submission := submit("--git", "git@git.example.test:org/repo.git@v1.2.0", "--git-path", "src/repo", "--git-changes", "patch")
	if placement := submission.Wherry.Placement; placement.Agent != "claude" || !reflect.DeepEqual(placement.Selector, []wire.Requirement{
		{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}}, {Key: "gpu", Operator: wire.OpDoesNotExist},
	}) {
		t.Fatalf("placement %#v, want the --agent and --selector flags", placement)
	}
	want := wire.GitInput{Remote: "ssh://git@git.example.test/org/repo.git", Ref: "v1.2.0", Path: "src/repo", Credential: "deploy-key", HostKey: hostKey}
	if git := submission.Wherry.Git; git == nil || *git != want || len(submission.Message.Parts) != 1 {
		t.Fatalf("submitted git %+v and parts %+v, want %+v and the prompt alone", git, submission.Message.Parts, want)
	}
	if credentials := submission.Wherry.Credentials; !slices.Equal(credentials, []wire.TaskCredential{{Name: "deploy-key", File: wire.GitCredentialFile}}) {
		t.Fatalf("submitted credentials %+v, want the git entry's at the git credential file", credentials)
	}
	if !slices.Equal(submission.Wherry.Outputs, []wire.ExpectedOutput{{Name: wire.GitPatch}}) {
		t.Fatalf("submitted outputs %+v, want the patch", submission.Wherry.Outputs)
	}
	// A remote under no entry clones anonymously.
	submission = submit("--git", "https://git.example.test/org/repo.git@main")
	if git := submission.Wherry.Git; git == nil || git.Credential != "" || git.HostKey != "" || len(submission.Wherry.Credentials) != 0 {
		t.Fatalf("submitted git %+v and credentials %+v, want an anonymous clone", git, submission.Wherry.Credentials)
	}
}

func TestParseAnywhere(t *testing.T) {
	for _, args := range [][]string{
		{"run-1", "--config-file", "f", "--timeout", "5m"},
		{"--config-file", "f", "run-1", "--timeout", "5m"},
		{"--config-file", "f", "--timeout", "5m", "run-1"},
	} {
		flags := flag.NewFlagSet("test", flag.ContinueOnError)
		config := flags.String("config-file", "", "")
		timeout := flags.Duration("timeout", 0, "")
		positional, err := parseAnywhere(flags, args)
		if err != nil || !reflect.DeepEqual(positional, []string{"run-1"}) || *config != "f" || *timeout != 5*time.Minute {
			t.Fatalf("%q: positional %q, config %q, timeout %v, %v", args, positional, *config, *timeout, err)
		}
	}
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	if _, err := parseAnywhere(flags, []string{"run-1", "--unknown"}); err == nil {
		t.Fatal("an unknown flag after the argument parsed")
	}
}

func TestOutputSuffixesInEitherOrder(t *testing.T) {
	for _, value := range []string{"big.bin:s3:required", "big.bin:required:s3"} {
		flags := flag.NewFlagSet("submit", flag.ContinueOnError)
		var outputs []wire.ExpectedOutput
		flags.Func("output", "", outputFlag(&outputs))
		if err := flags.Parse([]string{"--output", value}); err != nil || len(outputs) != 1 || outputs[0] != (wire.ExpectedOutput{Name: "big.bin", Required: true, Storage: wire.StorageS3}) {
			t.Fatalf("%s: outputs %+v, %v", value, outputs, err)
		}
	}
}

func TestVersionNeedsNoConfigOrBroker(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	var stdout, stderr bytes.Buffer
	if err := run([]string{"version"}, &stdout, &stderr); err != nil {
		t.Fatalf("version: %v (stderr %q)", err, stderr.String())
	}
	lines := strings.Split(strings.TrimSuffix(stdout.String(), "\n"), "\n")
	if len(lines) < 2 || !strings.HasPrefix(lines[0], "version=") || !strings.HasPrefix(lines[1], "revision=") {
		t.Fatalf("version printed %q, want version= and revision= lines", stdout.String())
	}
}

// readTopic reads topic from its start through a reader of its own.
func readTopic(ctx context.Context, client *kgo.Client, topic string) ([]*kgo.Record, error) {
	reader := admin.NewReader(client)
	defer reader.Close()
	return reader.ReadFrom(ctx, topic, 0)
}
