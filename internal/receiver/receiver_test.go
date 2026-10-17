package receiver

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/trust"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const (
	testToken      = "sk-ant-oat01-test-credential-never-logged"
	credentialName = "claude-oauth-token"
)

var (
	resultStream   = []string{`{"type":"assistant","message":"working"}`, `{"type":"result","result":"done","is_error":false}`}
	noResultStream = []string{`{"type":"assistant","message":"working"}`}
)

type fakeAdapter struct {
	mu          sync.Mutex
	platform    string
	offers      []map[string]string
	offersErr   error
	stream      []string
	stops       int
	stopErr     error
	stopCheck   func() error
	followErr   error
	follows     int
	dispatches  int
	dispatchErr error
	lastRun     Run
	lookups     int
	existing    bool
	releases    int
	releaseIDs  []string
	sweeps      int
	sweepErr    error
	// endDelay holds a stream without a result line open before it ends.
	endDelay time.Duration
}

func (f *fakeAdapter) Platform() string {
	if f.platform == "" {
		return "nomad"
	}
	return f.platform
}

func (f *fakeAdapter) Offers(context.Context) ([]map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]map[string]string(nil), f.offers...), f.offersErr
}

func (f *fakeAdapter) Dispatch(_ context.Context, run Run) (Handle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dispatches++
	f.lastRun = run
	if f.dispatchErr != nil {
		return Handle{}, f.dispatchErr
	}
	return Handle{Job: fmt.Sprintf("agent-%s-t%d", run.ID, run.Turn), Alloc: run.ID}, nil
}

func (f *fakeAdapter) Lookup(_ context.Context, runID string, turn int64) (Handle, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lookups++
	if !f.existing {
		return Handle{}, false, nil
	}
	return Handle{Job: fmt.Sprintf("agent-%s-t%d", runID, turn), Alloc: runID}, true, nil
}

// Follow emits the stream; a stream with a result keeps running like a live task, one without ends.
func (f *fakeAdapter) Follow(ctx context.Context, _ Handle, _ int64, lines chan<- Line) error {
	f.mu.Lock()
	f.follows++
	stream, followErr, endDelay := append([]string(nil), f.stream...), f.followErr, f.endDelay
	f.mu.Unlock()
	var offset int64
	for _, data := range stream {
		select {
		case lines <- Line{Offset: offset, Next: offset + int64(len(data)+1), Data: []byte(data)}:
		case <-ctx.Done():
			return ctx.Err()
		}
		offset += int64(len(data) + 1)
	}
	if followErr != nil {
		return followErr
	}
	if !slices.ContainsFunc(stream, func(data string) bool { _, ok := resultLine([]byte(data)); return ok }) {
		time.Sleep(endDelay)
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}

func (f *fakeAdapter) Stop(context.Context, Handle) error {
	f.mu.Lock()
	f.stops++
	err, check := f.stopErr, f.stopCheck
	f.mu.Unlock()
	if check != nil {
		return check()
	}
	return err
}

func (f *fakeAdapter) Release(_ context.Context, runID string, _ int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releases++
	f.releaseIDs = append(f.releaseIDs, runID)
	return nil
}

func (f *fakeAdapter) Sweep(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sweeps++
	return f.sweepErr
}

func (f *fakeAdapter) sweepCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sweeps
}

func (f *fakeAdapter) counts() (dispatches, lookups, follows, releases int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dispatches, f.lookups, f.follows, f.releases
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type fixture struct {
	ctx      context.Context
	kafka    *kgo.Client
	userKey  ed25519.PrivateKey
	receiver *Receiver
	adapter  *fakeAdapter
	logs     *syncBuffer
	cluster  *kfake.Cluster
}

func newFixture(t *testing.T, stream []string) fixture {
	t.Helper()
	for _, interval := range []*time.Duration{&renewInterval, &capabilityInterval, &refreshInterval, &enrolPollInterval} {
		previous := *interval
		*interval = 200 * time.Millisecond
		t.Cleanup(func() { *interval = previous })
	}
	cluster := kfake.MustCluster(kfake.NumBrokers(1))
	t.Cleanup(cluster.Close)
	opts := []kgo.Opt{kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.FetchMaxWait(10 * time.Millisecond)}
	kafka, err := kgo.NewClient(append(opts, kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.KeepControlRecords())...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kafka.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	if err := wire.ApplyTopics(ctx, kadm.NewClient(kafka), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := kadm.NewClient(kafka).CreateTopics(ctx, 1, 1, nil, "replies.test"); err != nil {
		t.Fatal(err)
	}
	_, userKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, signing, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sealing, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	base, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(base.Close)
	logs := &syncBuffer{}
	adapter := &fakeAdapter{stream: stream, offers: []map[string]string{{"runtime": "exec", "nomad.node_pool": "OCPv", "zone": "dc1"}}}
	r := &Receiver{
		config:   Config{ReceiverID: "receiver-nomad", Capacity: 1, Labels: map[string]string{}},
		adapters: []Named{{Name: "nomad", Credential: credentialName, Adapter: adapter}},
		kafkaOpt: opts, base: base,
		userPub: userKey.Public().(ed25519.PublicKey), signing: signing, sealing: sealing,
		logger: log.New(logs, "", 0), credentials: make(map[string]heldCredential),
	}
	return fixture{ctx: ctx, kafka: kafka, userKey: userKey, receiver: r, adapter: adapter, logs: logs, cluster: cluster}
}

// runToTask enrols the receiver, delivers a credential, submits one task and returns its final task.
func runToTask(t *testing.T, f fixture) *wire.Task {
	t.Helper()
	runCtx, stop := context.WithCancel(f.ctx)
	done := make(chan error, 1)
	go func() { done <- f.receiver.Run(runCtx) }()
	t.Cleanup(func() { stop(); <-done })

	client := busclient.New(f.kafka, f.userKey, "replies.test")
	var requestHash string
	for requestHash == "" {
		enrolments, _, err := client.Enrolments(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, enrolment := range enrolments {
			if enrolment.ReceiverID == "receiver-nomad" && len(enrolment.Adapters) == 1 && len(enrolment.Adapters[0].Offers) == 1 {
				requestHash = enrolment.RequestHash
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := client.Approve(f.ctx, requestHash, "", map[string]string{}, nomadGrants(), []wire.Requirement{}, "auto", 30); err != nil {
		t.Fatal(err)
	}
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(testToken), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := client.PutSecret(f.ctx, credentialName, "receiver-nomad", tokenFile, 30*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	for {
		f.receiver.mu.Lock()
		delivered := string(f.receiver.credentials["nomad"].value)
		f.receiver.mu.Unlock()
		if delivered == testToken {
			break
		}
		if f.ctx.Err() != nil {
			t.Fatal("credential never delivered")
		}
		time.Sleep(50 * time.Millisecond)
	}
	var submitted busclient.SubmitResult
	for {
		var err error
		submitted, err = client.Submit(f.ctx, busclient.SubmitInput{
			Selector: []wire.Requirement{{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}}}, Labels: map[string]string{}, Prompt: "hello",
			Deadline: time.Minute, QueueDeadline: time.Minute,
		})
		if err == nil {
			break
		}
		if !strings.Contains(err.Error(), "no matching") || f.ctx.Err() != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	result, err := client.Await(f.ctx, submitted.RunID, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return result.Task
}

func TestEndToEndWithFakeAdapter(t *testing.T) {
	f := newFixture(t, resultStream)
	task := runToTask(t, f)
	if task.Status.State != wire.TaskStateCompleted || task.Artifacts[0].Parts[0].Text != "done" {
		t.Fatalf("unexpected task: %#v", task)
	}
}

// interruptThenContinue submits first with no credential, calls between if set, delivers the credential,
// continues with second and returns the completed continuation.
func interruptThenContinue(t *testing.T, f fixture, first, second string, between func(*busclient.Client)) busclient.AwaitResult {
	t.Helper()
	runCtx, stop := context.WithCancel(f.ctx)
	done := make(chan error, 1)
	go func() { done <- f.receiver.Run(runCtx) }()
	t.Cleanup(func() { stop(); <-done })

	client := busclient.New(f.kafka, f.userKey, "replies.test")
	var requestHash string
	for requestHash == "" {
		enrolments, _, err := client.Enrolments(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, enrolment := range enrolments {
			if enrolment.ReceiverID == f.receiver.config.ReceiverID {
				requestHash = enrolment.RequestHash
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := client.Approve(f.ctx, requestHash, "", map[string]string{}, nomadGrants(), []wire.Requirement{}, "auto", 30); err != nil {
		t.Fatal(err)
	}
	var submitted busclient.SubmitResult
	for {
		var err error
		submitted, err = client.Submit(f.ctx, busclient.SubmitInput{
			Selector: []wire.Requirement{{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}}},
			Labels:   map[string]string{}, Prompt: first, Deadline: time.Minute, QueueDeadline: time.Minute,
		})
		if err == nil {
			break
		}
		if !strings.Contains(err.Error(), "no matching") || f.ctx.Err() != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	interrupted, err := client.Await(f.ctx, submitted.RunID, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if interrupted.Status.State != wire.TaskStateAuthRequired || interrupted.Task != nil {
		t.Fatalf("unexpected interruption: %#v", interrupted)
	}
	if tasks := topicCount(t, f, "replies.test"); tasks != 0 {
		t.Fatalf("task records after interruption=%d, want none", tasks)
	}
	if between != nil {
		between(client)
	}

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(testToken), 0o600); err != nil {
		t.Fatal(err)
	}
	secret, err := client.PutSecret(f.ctx, credentialName, f.receiver.config.ReceiverID, tokenFile, 30*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(secret.Waiting) != 1 || secret.Waiting[0].RunID != submitted.RunID || secret.Waiting[0].Turn != 1 {
		t.Fatalf("waiting tasks %#v", secret.Waiting)
	}
	for {
		f.receiver.mu.Lock()
		delivered := string(f.receiver.credentials["nomad"].value)
		f.receiver.mu.Unlock()
		if delivered == testToken {
			break
		}
		if f.ctx.Err() != nil {
			t.Fatal("credential never delivered")
		}
		time.Sleep(50 * time.Millisecond)
	}
	continued, err := client.Continue(f.ctx, submitted.RunID, busclient.ContinueInput{
		Prompt: second, Deadline: time.Minute, QueueDeadline: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if continued.RunID != submitted.RunID || continued.Turn != 2 {
		t.Fatalf("continuation %#v", continued)
	}
	completed, err := client.Await(f.ctx, submitted.RunID, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return completed
}

func TestContinuationResumesInterruptedTask(t *testing.T) {
	completed := interruptThenContinue(t, newFixture(t, resultStream), "first", "second", nil)
	if completed.Status.State != wire.TaskStateCompleted || completed.Task == nil ||
		len(completed.Task.History) != 2 || completed.Task.History[0].Parts[0].Text != "first" || completed.Task.History[1].Parts[0].Text != "second" {
		t.Fatalf("completed continuation %#v", completed)
	}
}

func TestContinuationPromptIncludesAllTurns(t *testing.T) {
	f := newFixture(t, resultStream)
	completed := interruptThenContinue(t, f, "write the report", "continue", nil)
	if completed.Status.State != wire.TaskStateCompleted || completed.Task == nil || len(completed.Task.History) != 2 {
		t.Fatalf("completed continuation %#v", completed)
	}
	f.adapter.mu.Lock()
	run := f.adapter.lastRun
	f.adapter.mu.Unlock()
	if want := "--- turn 1 ---\nwrite the report\n--- turn 2 ---\ncontinue"; string(run.Prompt) != want || run.Turn != 2 {
		t.Fatalf("turn %d prompt %q, want turn 2 prompt %q", run.Turn, run.Prompt, want)
	}
}

func TestFirstTurnPromptUnchanged(t *testing.T) {
	f, enrolled := admitFixture(t)
	record := signedRoot(t, f.userKey, func(task *wire.SubmissionRecord) {
		task.Message.Parts = []wire.Part{{Text: "line one"}, {Text: "--- turn 1 ---"}, {Text: "line three\n"}}
	})
	admitted, _ := f.receiver.admit(f.ctx, record, enrolled)
	if admitted == nil || admitted.prompt != "line one\n--- turn 1 ---\nline three\n" {
		t.Fatalf("got %#v", admitted)
	}
}

func TestRunEndingWithoutResultFails(t *testing.T) {
	f := newFixture(t, noResultStream)
	task := runToTask(t, f)
	if task.Status.State != wire.TaskStateFailed || task.Artifacts[0].Parts[0].Text != "agent ended without a result" {
		t.Fatalf("unexpected task: %#v", task)
	}
	f.adapter.mu.Lock()
	defer f.adapter.mu.Unlock()
	if f.adapter.stops != 1 {
		t.Fatalf("run ended without a result but Stop was called %d times, want 1", f.adapter.stops)
	}
}

func TestSecretNeverLogged(t *testing.T) {
	f := newFixture(t, resultStream)
	runToTask(t, f)
	// A forged, unsigned record as the latest sealed secret must be refused without echoing anything.
	if err := f.kafka.ProduceSync(f.ctx, &kgo.Record{Topic: wire.TopicSecret, Key: []byte("receiver-nomad/" + credentialName), Value: []byte(testToken)}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.refresh(f.ctx); err == nil || err.Error() != "credential rejected" {
		t.Fatalf("forged secret: got %v, want \"credential rejected\"", err)
	}
	f.receiver.mu.Lock()
	delivered := string(f.receiver.credentials["nomad"].value)
	f.receiver.mu.Unlock()
	if delivered != testToken {
		t.Fatal("forged record replaced the delivered credential")
	}
	if strings.Contains(f.logs.String(), testToken) {
		t.Fatal("credential value appeared in receiver logs")
	}
}

func nomadGrants() []wire.AdapterGrant {
	return []wire.AdapterGrant{{
		Name: "nomad", Platform: "nomad",
		Allowed:  map[string][]string{"runtime": {"exec", "podman"}, "nomad.node_pool": {"OCPv", "default"}, "zone": {"dc1"}},
		Defaults: map[string]string{},
	}}
}

func TestReceiverGrantPlatformMismatchNotEnrolled(t *testing.T) {
	for name, test := range map[string]struct {
		grant  wire.AdapterGrant
		reason string
	}{
		"platform": {wire.AdapterGrant{Name: "nomad", Platform: "aws", Allowed: map[string][]string{}, Defaults: map[string]string{}}, `grant "nomad" platform "aws" does not match adapter platform "nomad"`},
		"name":     {wire.AdapterGrant{Name: "other", Platform: "nomad", Allowed: map[string][]string{}, Defaults: map[string]string{}}, `grant "other" names no configured adapter`},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			request, err := trust.NewEnrolRequest(
				f.receiver.config.ReceiverID, f.receiver.signing, f.receiver.sealing,
				map[string]string{}, []wire.DetectedAdapter{{Name: "nomad", Platform: "nomad", Offers: []wire.Offer{}}}, time.Now(),
			)
			if err != nil {
				t.Fatal(err)
			}
			enrolRequest, err := trust.NewState(f.userKey.Public().(ed25519.PublicKey)).Apply(request)
			if err != nil {
				t.Fatal(err)
			}
			approval, err := trust.Approve(request, enrolRequest.(*wire.EnrolRequest), f.userKey, map[string]string{}, "nomad", []wire.AdapterGrant{test.grant}, []wire.Requirement{}, "auto", 1, "", time.Now().Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if err := f.kafka.ProduceSync(f.ctx,
				&kgo.Record{Topic: wire.TopicState, Key: []byte(f.receiver.config.ReceiverID), Value: request},
				&kgo.Record{Topic: wire.TopicState, Key: []byte(f.receiver.config.ReceiverID), Value: approval},
			).FirstErr(); err != nil {
				t.Fatal(err)
			}
			if err := f.receiver.refresh(f.ctx); err != nil {
				t.Fatal(err)
			}
			if f.receiver.current() != nil {
				t.Fatal("receiver treated mismatched grant as enrolled")
			}
			if logs := f.logs.String(); !strings.Contains(logs, test.reason) {
				t.Fatalf("logs %q do not explain %q", logs, test.reason)
			}
		})
	}
}

// testSubmission is a root submission for runID as client.Submit builds it.
func testSubmission(runID string) *wire.SubmissionRecord {
	return &wire.SubmissionRecord{
		Message: wire.Message{
			MessageID: "msg-a", ContextID: runID, TaskID: runID, Role: wire.RoleUser,
			Parts: []wire.Part{{Text: "hi"}}, Extensions: []string{wire.ExtensionURI},
		},
		Wherry: wire.SubmissionExtension{
			RunID: runID, Turn: 1, Attempt: 1, Pool: "nomad", Labels: map[string]string{"lab/trust": "lab"},
			Selector:   []wire.Requirement{{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}}},
			DeadlineMS: time.Now().Add(time.Minute).UnixMilli(), QueueDeadlineMS: time.Now().Add(time.Minute).UnixMilli(),
			ReplyTopic: "replies.test",
			Lineage:    wire.Lineage{RootRunID: runID, Ancestors: []string{}, Chain: []wire.Bytes{}},
		},
	}
}

func signedRoot(t *testing.T, key ed25519.PrivateKey, mutate func(*wire.SubmissionRecord)) *kgo.Record {
	t.Helper()
	submission := testSubmission("run-a")
	if mutate != nil {
		mutate(submission)
	}
	value, err := trust.SignRootSubmission(*submission, key)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Key: []byte(submission.Wherry.RunID), Value: value}
}

func admitFixture(t *testing.T) (fixture, *trust.Enrolled) {
	f := newFixture(t, resultStream)
	f.receiver.offers = []wire.Offer{
		{Adapter: "nomad", Labels: map[string]string{"runtime": "exec", "nomad.node_pool": "OCPv", "zone": "dc1"}},
		{Adapter: "nomad", Labels: map[string]string{"runtime": "podman", "nomad.node_pool": "default", "zone": "dc1"}},
	}
	enrolled := &trust.Enrolled{
		Pool: "nomad", Labels: map[string]string{}, Adapters: nomadGrants(),
		Selector: []wire.Requirement{{Key: "lab/trust", Operator: wire.OpIn, Values: []string{"lab"}}},
	}
	return f, enrolled
}

func topicCount(t *testing.T, f fixture, topic string) int {
	t.Helper()
	records, err := readBounded(f.ctx, f.receiver.kafkaOpt, topic)
	if err != nil {
		t.Fatal(err)
	}
	return len(records)
}

func testActiveRun(t *testing.T, f fixture, runID string, attempt int64) *activeRun {
	t.Helper()
	producer, err := kgo.NewClient(append(append([]kgo.Opt(nil), f.receiver.kafkaOpt...), kgo.TransactionalID(runID))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)
	task := testSubmission(runID)
	envelope, err := trust.SignRootSubmission(*task, f.userKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.kafka.ProduceSync(f.ctx, &kgo.Record{Topic: wire.TopicTask, Key: []byte(runID), Value: envelope}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	return &activeRun{
		receiver: f.receiver,
		producer: producer,
		task:     task,
		taskHash: trust.EnvelopeHash(envelope),
		attempt:  attempt,
		adapter:  f.receiver.adapters[0],
	}
}

func executeSubmission(t *testing.T, f fixture, record *kgo.Record, admitted *admission, attempt int64) kgo.AckStatus {
	t.Helper()
	if err := f.kafka.ProduceSync(f.ctx, &kgo.Record{Topic: wire.TopicTask, Key: record.Key, Value: record.Value}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	return f.receiver.execute(f.ctx, f.kafka, record, admitted, attempt)
}

func signedTurn(t *testing.T, f fixture, runID string, turn int64) *kgo.Record {
	t.Helper()
	record := signedRoot(t, f.userKey, func(task *wire.SubmissionRecord) {
		task.Wherry.RunID = runID
		task.Wherry.Lineage.RootRunID = runID
		task.Wherry.Turn = turn
		task.Message.MessageID = fmt.Sprintf("msg-%d", turn)
		task.Message.TaskID = runID
		task.Message.ContextID = runID
	})
	if err := f.kafka.ProduceSync(f.ctx, &kgo.Record{Topic: wire.TopicTask, Key: record.Key, Value: record.Value}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	return record
}

func publishRunStatus(t *testing.T, f fixture, record *kgo.Record, key ed25519.PrivateKey, kind string, state wire.TaskState) {
	t.Helper()
	task, err := trust.VerifyRootSubmission(record.Value, f.receiver.userPub)
	if err != nil {
		t.Fatal(err)
	}
	status := f.receiver.status(task, trust.EnvelopeHash(record.Value), 1, kind, taskStatus(state, ""), nil)
	value, err := sign(wire.TypeStatus, status, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.kafka.ProduceSync(f.ctx, &kgo.Record{Topic: wire.TopicEvent, Key: record.Key, Value: value}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func enrolReceiver(t *testing.T, f fixture, id, pool string, signing ed25519.PrivateKey, sealing *ecdh.PrivateKey) {
	t.Helper()
	request, err := trust.NewEnrolRequest(id, signing, sealing, map[string]string{}, []wire.DetectedAdapter{{Name: "nomad", Platform: "nomad", Offers: []wire.Offer{}}}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	verified, err := trust.NewState(f.userKey.Public().(ed25519.PublicKey)).Apply(request)
	if err != nil {
		t.Fatal(err)
	}
	approval, err := trust.Approve(request, verified.(*wire.EnrolRequest), f.userKey, map[string]string{}, pool, nomadGrants(), []wire.Requirement{}, "auto", 1, "", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.kafka.ProduceSync(f.ctx,
		&kgo.Record{Topic: wire.TopicState, Key: []byte(id), Value: request},
		&kgo.Record{Topic: wire.TopicState, Key: []byte(id), Value: approval},
	).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func TestSecretKeyCrossWiringRejected(t *testing.T) {
	f, _ := admitFixture(t)
	enrolReceiver(t, f, f.receiver.config.ReceiverID, "nomad", f.receiver.signing, f.receiver.sealing)
	client := busclient.New(f.kafka, f.userKey, "replies.test")
	path := filepath.Join(t.TempDir(), "other")
	if err := os.WriteFile(path, []byte("other-secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := client.PutSecret(f.ctx, "other", f.receiver.config.ReceiverID, path, 30*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	records, err := readBounded(f.ctx, f.receiver.kafkaOpt, wire.TopicSecret)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if string(record.Key) == f.receiver.config.ReceiverID+"/other" {
			if err := f.kafka.ProduceSync(f.ctx, &kgo.Record{
				Topic: wire.TopicSecret, Key: []byte(f.receiver.config.ReceiverID + "/" + credentialName), Value: record.Value,
			}).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := f.receiver.refresh(f.ctx); err == nil || err.Error() != "credential rejected" {
		t.Fatalf("refresh = %v, want credential rejected", err)
	}
	if _, ok := f.receiver.credentials["nomad"]; ok {
		t.Fatal("secret for another name was delivered as the adapter credential")
	}
}

func TestContinuationRejectedWhenNotInterrupted(t *testing.T) {
	for name, state := range map[string]wire.TaskState{
		"running":   wire.TaskStateWorking,
		"completed": wire.TaskStateCompleted,
	} {
		t.Run(name, func(t *testing.T) {
			f, enrolled := admitFixture(t)
			first := signedTurn(t, f, "run-not-interrupted", 1)
			kind := "claimed"
			if wire.Terminal(state) {
				kind = "terminal"
			}
			publishRunStatus(t, f, first, f.receiver.signing, kind, state)
			continuation := signedTurn(t, f, "run-not-interrupted", 2)
			if admitted, ack := f.receiver.admit(f.ctx, continuation, enrolled); admitted != nil || ack != kgo.AckReject {
				t.Fatalf("admitted=%#v ack=%v, want rejection", admitted, ack)
			}
			if !strings.Contains(f.logs.String(), "task is not awaiting continuation") {
				t.Fatalf("fixed rejection reason missing from %q", f.logs.String())
			}
		})
	}
}

func TestStaleSubmissionRejected(t *testing.T) {
	t.Run("root replay after terminal", func(t *testing.T) {
		f, enrolled := admitFixture(t)
		first := signedTurn(t, f, "run-terminal-replay", 1)
		publishRunStatus(t, f, first, f.receiver.signing, "terminal", wire.TaskStateCompleted)
		if admitted, ack := f.receiver.admit(f.ctx, first, enrolled); admitted != nil || ack != kgo.AckReject {
			t.Fatalf("admitted=%#v ack=%v, want stale rejection", admitted, ack)
		}
		if !strings.Contains(f.logs.String(), "stale submission") {
			t.Fatalf("fixed rejection reason missing from %q", f.logs.String())
		}
	})
	t.Run("earlier turn after later turn started", func(t *testing.T) {
		f, enrolled := admitFixture(t)
		first := signedTurn(t, f, "run-earlier-turn", 1)
		publishRunStatus(t, f, first, f.receiver.signing, "interrupted", wire.TaskStateAuthRequired)
		second := signedTurn(t, f, "run-earlier-turn", 2)
		publishRunStatus(t, f, second, f.receiver.signing, "started", wire.TaskStateWorking)
		if admitted, ack := f.receiver.admit(f.ctx, first, enrolled); admitted != nil || ack != kgo.AckReject {
			t.Fatalf("admitted=%#v ack=%v, want stale rejection", admitted, ack)
		}
		if !strings.Contains(f.logs.String(), "stale submission") {
			t.Fatalf("fixed rejection reason missing from %q", f.logs.String())
		}
	})
	t.Run("active turn redelivery adopts", func(t *testing.T) {
		f, enrolled := admitFixture(t)
		first := signedTurn(t, f, "run-active-redelivery", 1)
		publishRunStatus(t, f, first, f.receiver.signing, "heartbeat", wire.TaskStateWorking)
		if admitted, _ := f.receiver.admit(f.ctx, first, enrolled); admitted == nil {
			t.Fatal("redelivery of the active turn was rejected")
		}
	})
}

func TestDuplicateContinuationRejected(t *testing.T) {
	f, enrolled := admitFixture(t)
	first := signedTurn(t, f, "run-duplicate", 1)
	publishRunStatus(t, f, first, f.receiver.signing, "interrupted", wire.TaskStateAuthRequired)
	accepted := signedTurn(t, f, "run-duplicate", 2)
	duplicate := signedRoot(t, f.userKey, func(task *wire.SubmissionRecord) {
		task.Wherry.RunID = "run-duplicate"
		task.Wherry.Lineage.RootRunID = "run-duplicate"
		task.Wherry.Turn = 2
		task.Message.MessageID = "msg-duplicate"
		task.Message.TaskID = "run-duplicate"
		task.Message.ContextID = "run-duplicate"
	})
	if err := f.kafka.ProduceSync(f.ctx, &kgo.Record{Topic: wire.TopicTask, Key: duplicate.Key, Value: duplicate.Value}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if admitted, _ := f.receiver.admit(f.ctx, accepted, enrolled); admitted == nil {
		t.Fatal("first continuation was not admitted")
	}
	if admitted, ack := f.receiver.admit(f.ctx, duplicate, enrolled); admitted != nil || ack != kgo.AckReject {
		t.Fatalf("duplicate admitted=%#v ack=%v", admitted, ack)
	}
	if !strings.Contains(f.logs.String(), "duplicate continuation") {
		t.Fatalf("fixed rejection reason missing from %q", f.logs.String())
	}
}

func TestRejectedContinuationKeepsTaskResumable(t *testing.T) {
	f, enrolled := admitFixture(t)
	first := signedTurn(t, f, "run-rejected-continuation", 1)
	publishRunStatus(t, f, first, f.receiver.signing, "interrupted", wire.TaskStateAuthRequired)
	outOfOrder := signedTurn(t, f, "run-rejected-continuation", 3)
	if admitted, ack := f.receiver.admit(f.ctx, outOfOrder, enrolled); admitted != nil || ack != kgo.AckReject {
		t.Fatalf("out-of-order admitted=%#v ack=%v", admitted, ack)
	}
	valid := signedTurn(t, f, "run-rejected-continuation", 2)
	if admitted, _ := f.receiver.admit(f.ctx, valid, enrolled); admitted == nil {
		t.Fatal("rejected continuation changed the latest task state")
	}
	statuses := published(t, f, wire.TopicEvent)
	if got := statuses[len(statuses)-1].(*wire.StatusRecord); got.Wherry.Kind != "rejected" || got.StatusUpdate.Status.State != wire.TaskStateRejected {
		t.Fatalf("rejection status %#v", got)
	}
}

func TestContinuationRejectedOutOfOrder(t *testing.T) {
	f, enrolled := admitFixture(t)
	first := signedTurn(t, f, "run-out-of-order", 1)
	publishRunStatus(t, f, first, f.receiver.signing, "interrupted", wire.TaskStateAuthRequired)
	continuation := signedTurn(t, f, "run-out-of-order", 3)
	if admitted, ack := f.receiver.admit(f.ctx, continuation, enrolled); admitted != nil || ack != kgo.AckReject {
		t.Fatalf("admitted=%#v ack=%v, want rejection", admitted, ack)
	}
	if !strings.Contains(f.logs.String(), "continuation turn out of order") {
		t.Fatalf("fixed rejection reason missing from %q", f.logs.String())
	}
}

func TestContinuationRejectedFromOtherPoolStatus(t *testing.T) {
	f, _ := admitFixture(t)
	enrolReceiver(t, f, f.receiver.config.ReceiverID, "nomad", f.receiver.signing, f.receiver.sealing)
	first := signedTurn(t, f, "run-other-pool-status", 1)
	_, otherKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	otherSealing, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	enrolReceiver(t, f, "receiver-other-pool", "other", otherKey, otherSealing)
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	enrolled := f.receiver.current()
	enrolled.Selector = []wire.Requirement{{Key: "lab/trust", Operator: wire.OpIn, Values: []string{"lab"}}}
	f.receiver.offers = []wire.Offer{{Adapter: "nomad", Labels: map[string]string{"runtime": "exec", "nomad.node_pool": "OCPv", "zone": "dc1"}}}
	publishRunStatus(t, f, first, otherKey, "interrupted", wire.TaskStateAuthRequired)
	continuation := signedTurn(t, f, "run-other-pool-status", 2)
	if admitted, ack := f.receiver.admit(f.ctx, continuation, enrolled); admitted != nil || ack != kgo.AckReject {
		t.Fatalf("admitted=%#v ack=%v, want rejection", admitted, ack)
	}
	if !strings.Contains(f.logs.String(), "task is not awaiting continuation") {
		t.Fatalf("fixed rejection reason missing from %q", f.logs.String())
	}
}

func TestContinuationStatusBindings(t *testing.T) {
	for name, mutate := range map[string]func(*wire.StatusRecord){
		"task hash": func(status *wire.StatusRecord) { status.Wherry.TaskHash = strings.Repeat("d", 64) },
		"context":   func(status *wire.StatusRecord) { status.StatusUpdate.ContextID = "run-other" },
		"kind":      func(status *wire.StatusRecord) { status.Wherry.Kind = "claimed" },
	} {
		t.Run(name, func(t *testing.T) {
			f, enrolled := admitFixture(t)
			first := signedTurn(t, f, "run-status-binding", 1)
			task, err := trust.VerifyRootSubmission(first.Value, f.receiver.userPub)
			if err != nil {
				t.Fatal(err)
			}
			status := f.receiver.status(task, trust.EnvelopeHash(first.Value), 1, "interrupted", taskStatus(wire.TaskStateAuthRequired, ""), nil)
			mutate(status)
			value, err := sign(wire.TypeStatus, status, f.receiver.signing)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.kafka.ProduceSync(f.ctx, &kgo.Record{Topic: wire.TopicEvent, Key: first.Key, Value: value}).FirstErr(); err != nil {
				t.Fatal(err)
			}
			continuation := signedTurn(t, f, "run-status-binding", 2)
			if admitted, ack := f.receiver.admit(f.ctx, continuation, enrolled); admitted != nil || ack != kgo.AckReject {
				t.Fatalf("admitted=%#v ack=%v, want rejection", admitted, ack)
			}
		})
	}
}

func TestContinuationStatusOrdering(t *testing.T) {
	f, enrolled := admitFixture(t)
	first := signedTurn(t, f, "run-status-order", 1)
	task, err := trust.VerifyRootSubmission(first.Value, f.receiver.userPub)
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range []*wire.StatusRecord{
		f.receiver.status(task, trust.EnvelopeHash(first.Value), 2, "interrupted", taskStatus(wire.TaskStateAuthRequired, ""), nil),
		f.receiver.status(task, trust.EnvelopeHash(first.Value), 1, "claimed", taskStatus(wire.TaskStateWorking, ""), nil),
	} {
		value, err := sign(wire.TypeStatus, status, f.receiver.signing)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.kafka.ProduceSync(f.ctx, &kgo.Record{Topic: wire.TopicEvent, Key: first.Key, Value: value}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	continuation := signedTurn(t, f, "run-status-order", 2)
	if admitted, _ := f.receiver.admit(f.ctx, continuation, enrolled); admitted == nil {
		t.Fatal("lower attempt appended later masked the interruption")
	}
}

func TestSubmissionsRequireSameBindings(t *testing.T) {
	for name, mutate := range map[string]func(*wire.SubmissionRecord){
		"pool": func(task *wire.SubmissionRecord) { task.Wherry.Pool = "other" },
		"lineage": func(task *wire.SubmissionRecord) {
			task.Wherry.Lineage.Depth = 1
			task.Wherry.Lineage.Ancestors = []string{"parent"}
		},
		"labels":          func(task *wire.SubmissionRecord) { task.Wherry.Labels = map[string]string{"lab/trust": "other"} },
		"selector":        func(task *wire.SubmissionRecord) { task.Wherry.Selector[0].Values = []string{"other"} },
		"reply topic":     func(task *wire.SubmissionRecord) { task.Wherry.ReplyTopic = "replies.other" },
		"message context": func(task *wire.SubmissionRecord) { task.Message.ContextID = "run-other" },
	} {
		t.Run(name, func(t *testing.T) {
			f, _ := admitFixture(t)
			signedTurn(t, f, "run-submission-binding", 1)
			expected := testSubmission("run-submission-binding")
			mutate(expected)
			submissions, err := f.receiver.submissions(f.ctx, expected)
			if err != nil {
				t.Fatal(err)
			}
			if len(submissions) != 0 {
				t.Fatalf("mismatched %s was accepted", name)
			}
		})
	}
}

func TestHistoryRequiresFinalTurnHash(t *testing.T) {
	f, _ := admitFixture(t)
	signedTurn(t, f, "run-history-hash", 1)
	signedTurn(t, f, "run-history-hash", 2)
	duplicate := signedRoot(t, f.userKey, func(task *wire.SubmissionRecord) {
		task.Wherry.RunID = "run-history-hash"
		task.Wherry.Lineage.RootRunID = "run-history-hash"
		task.Wherry.Turn = 2
		task.Message.MessageID = "msg-history-duplicate"
		task.Message.TaskID = "run-history-hash"
		task.Message.ContextID = "run-history-hash"
	})
	task, err := trust.VerifyRootSubmission(duplicate.Value, f.receiver.userPub)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.receiver.history(f.ctx, task, trust.EnvelopeHash(duplicate.Value)); err == nil {
		t.Fatal("history accepted a final turn other than the claimed submission")
	}
}

func TestAdmitAcceptsValidTask(t *testing.T) {
	f, enrolled := admitFixture(t)
	admitted, _ := f.receiver.admit(f.ctx, signedRoot(t, f.userKey, nil), enrolled)
	if admitted == nil || admitted.offer.Adapter != "nomad" || admitted.offer.Labels["runtime"] != "exec" || admitted.prompt != "hi" {
		t.Fatalf("got %#v", admitted)
	}
}

func TestReceiverDropsOffersOutsideAllowed(t *testing.T) {
	f := newFixture(t, resultStream)
	enrolled := &trust.Enrolled{
		Pool: "nomad", Labels: map[string]string{}, Selector: []wire.Requirement{}, Generation: 1,
		Adapters: []wire.AdapterGrant{{Name: "nomad", Platform: "nomad", Allowed: map[string][]string{"runtime": {"exec"}, "nomad.node_pool": {"OCPv"}, "zone": {"dc1"}}, Defaults: map[string]string{}}},
	}
	f.receiver.mu.Lock()
	f.receiver.enrolled = enrolled
	f.receiver.mu.Unlock()
	f.adapter.mu.Lock()
	f.adapter.offers = append(f.adapter.offers, map[string]string{"runtime": "podman", "nomad.node_pool": "OCPv", "zone": "dc1"})
	f.adapter.mu.Unlock()
	if err := f.receiver.publishCapability(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.receiver.mu.Lock()
	offers := f.receiver.offers
	f.receiver.mu.Unlock()
	if len(offers) != 1 || offers[0].Labels["runtime"] != "exec" {
		t.Fatalf("published offers %#v", offers)
	}
	if !strings.Contains(f.logs.String(), "key=runtime") {
		t.Fatalf("drop log does not name offending key: %q", f.logs.String())
	}
}

func TestNoCredentialInterruptsAndReleases(t *testing.T) {
	f, enrolled := admitFixture(t)
	record := signedRoot(t, f.userKey, nil)
	admitted, _ := f.receiver.admit(f.ctx, record, enrolled)
	before := time.Now().Add(-time.Second)
	if status := executeSubmission(t, f, record, admitted, 1); status != kgo.AckAccept {
		t.Fatalf("status %v, want accept", status)
	}
	f.adapter.mu.Lock()
	dispatches, releases := f.adapter.dispatches, f.adapter.releases
	f.adapter.mu.Unlock()
	if dispatches != 0 || releases != 1 {
		t.Fatalf("dispatches=%d releases=%d, want 0 and 1", dispatches, releases)
	}
	statuses := published(t, f, wire.TopicEvent)
	if len(statuses) != 2 {
		t.Fatalf("status records=%d, want claimed and terminal", len(statuses))
	}
	interrupted := statuses[1].(*wire.StatusRecord)
	got := interrupted.StatusUpdate.Status
	if interrupted.Wherry.Kind != "interrupted" || got.State != wire.TaskStateAuthRequired {
		t.Fatalf("interrupted status %#v, want AUTH_REQUIRED", interrupted)
	}
	if got.Message == nil || got.Message.Role != wire.RoleAgent || len(got.Message.Parts) != 1 || got.Message.Parts[0].Text != "credential "+credentialName+" required" {
		t.Fatalf("status message %#v, want one agent text part with the reason", got.Message)
	}
	if at, err := time.Parse(time.RFC3339, got.Timestamp); err != nil || at.Before(before) || at.After(time.Now()) {
		t.Fatalf("status timestamp %q is not the time of the status: %v", got.Timestamp, err)
	}
	if tasks := topicCount(t, f, "replies.test"); tasks != 0 {
		t.Fatalf("task records=%d, want none", tasks)
	}
}

func TestTranscriptArtifactChunks(t *testing.T) {
	oversized := `{"type":"assistant","message":"` + strings.Repeat("x", maxLine) + `"}`
	for name, test := range map[string]struct {
		stream    []string
		heartbeat bool
		pointer   int
		raw       []int
	}{
		"result after an oversized line": {[]string{`{"type":"system"}`, oversized, `{"type":"assistant"}`, resultStream[1]}, false, 1, nil},
		// The stream outlives a heartbeat, which commits all but the newest line.
		"no result after a heartbeat": {[]string{`{"type":"system"}`, `{"type":"assistant"}`}, true, -1, nil},
		"empty and non-UTF-8 lines":   {[]string{`{"type":"system"}`, "", "\xff\xfe", resultStream[1]}, false, -1, []int{1, 2}},
	} {
		t.Run(name, func(t *testing.T) {
			f, enrolled := admitFixture(t)
			f.receiver.credentials["nomad"] = heldCredential{version: 1, value: []byte(testToken)}
			f.adapter.stream = test.stream
			if test.heartbeat {
				f.adapter.endDelay = 3 * renewInterval
			}
			record := signedRoot(t, f.userKey, nil)
			admitted, _ := f.receiver.admit(f.ctx, record, enrolled)
			executeSubmission(t, f, record, admitted, 1)

			chunks := published(t, f, wire.TopicTranscript)
			if len(chunks) != len(test.stream) {
				t.Fatalf("chunks=%d, want one per line (%d)", len(chunks), len(test.stream))
			}
			var offset int64
			for i, payload := range chunks {
				chunk := payload.(*wire.ArtifactUpdateRecord)
				update, part := chunk.ArtifactUpdate, chunk.ArtifactUpdate.Artifact.Parts[0]
				if update.TaskID != "run-a" || update.ContextID != "run-a" || update.Artifact.ArtifactID != "transcript" || chunk.Wherry.Seq != offset || part.MediaType != "application/x-ndjson" {
					t.Fatalf("chunk %d: %#v", i, chunk)
				}
				if update.Append != (i > 0) || update.LastChunk != (i == len(chunks)-1) {
					t.Fatalf("chunk %d: append=%t lastChunk=%t", i, update.Append, update.LastChunk)
				}
				line := test.stream[i]
				if i == test.pointer {
					var pointer wire.PointerMetadata
					if err := json.Unmarshal(part.Metadata, &pointer); err != nil {
						t.Fatal(err)
					}
					if part.URL != "unstored:oversize" || part.Text != "" || pointer.SHA256 != trust.EnvelopeHash([]byte(line)) || pointer.Size != int64(len(line)) {
						t.Fatalf("oversized line became %#v with %#v", part, pointer)
					}
				} else if slices.Contains(test.raw, i) {
					if part.Raw == nil || string(part.Raw) != line || part.Text != "" {
						t.Fatalf("chunk %d became %#v, want raw %q", i, part, line)
					}
				} else if part.Text != line {
					t.Fatalf("chunk %d text %q, want %q", i, part.Text, line)
				}
				offset += int64(len(line) + 1)
			}
			if !test.heartbeat {
				return
			}
			var offsets []int64
			for _, payload := range published(t, f, wire.TopicEvent) {
				if status := payload.(*wire.StatusRecord); status.Wherry.Kind == "heartbeat" {
					offsets = append(offsets, *status.Wherry.LogOffset)
				}
			}
			// Once both lines have arrived, a heartbeat commits the first and records where the held second starts.
			if want := int64(len(test.stream[0]) + 1); len(offsets) == 0 || offsets[len(offsets)-1] != want {
				t.Fatalf("heartbeat offsets %v, want the last at %d", offsets, want)
			}
		})
	}
}

func TestTaskResultDataPartWhenJSON(t *testing.T) {
	for _, result := range []string{`{"answer":42,"items":[1,2]}`, `[true,null]`, `42`} {
		t.Run(result, func(t *testing.T) {
			f := newFixture(t, resultStream)
			run := testActiveRun(t, f, "run-json", 1)
			if status := run.finish(f.ctx, Handle{Alloc: "alloc"}, nil, agentResult{Result: result}); status != kgo.AckAccept {
				t.Fatalf("status %v", status)
			}
			part := lastTask(t, f).Artifacts[0].Parts[0]
			if part.Text != "" || part.MediaType != "application/json" || string(part.Data) != result {
				t.Fatalf("result part %#v, want data %s", part, result)
			}
		})
	}
}

func TestTaskResultTextPartOtherwise(t *testing.T) {
	for _, result := range []string{"done", `{"answer":`, `{"k":1,"k":2}`, "42 is the answer"} {
		t.Run(result, func(t *testing.T) {
			f := newFixture(t, resultStream)
			run := testActiveRun(t, f, "run-text", 1)
			if status := run.finish(f.ctx, Handle{Alloc: "alloc"}, nil, agentResult{Result: result}); status != kgo.AckAccept {
				t.Fatalf("status %v", status)
			}
			task := lastTask(t, f)
			part := task.Artifacts[0].Parts[0]
			if part.Text != result || part.Data != nil || part.MediaType != "" {
				t.Fatalf("result part %#v, want text %q", part, result)
			}
			if task.ID != "run-text" || task.ContextID != "run-text" || len(task.History) != 1 || task.History[0].MessageID != "msg-a" {
				t.Fatalf("task ids or history %#v", task)
			}
		})
	}
}

func TestReceiverReleasesAfterTerminal(t *testing.T) {
	t.Run("stop", func(t *testing.T) {
		f := newFixture(t, resultStream)
		run := testActiveRun(t, f, "run-release-stop", 1)
		if status := run.finish(f.ctx, Handle{Alloc: "alloc"}, nil, agentResult{Result: "done"}); status != kgo.AckAccept {
			t.Fatalf("status %v", status)
		}
		f.adapter.mu.Lock()
		defer f.adapter.mu.Unlock()
		if f.adapter.stops != 1 || f.adapter.releases != 1 || f.adapter.releaseIDs[0] != "run-release-stop" {
			t.Fatalf("stops=%d releases=%v", f.adapter.stops, f.adapter.releaseIDs)
		}
	})
	t.Run("dispatch error", func(t *testing.T) {
		f, enrolled := admitFixture(t)
		f.receiver.credentials["nomad"] = heldCredential{version: 1, value: []byte(testToken)}
		f.adapter.dispatchErr = errors.New("dispatch failed")
		record := signedRoot(t, f.userKey, nil)
		admitted, _ := f.receiver.admit(f.ctx, record, enrolled)
		if status := executeSubmission(t, f, record, admitted, 1); status != kgo.AckReject {
			t.Fatalf("status %v", status)
		}
		f.adapter.mu.Lock()
		defer f.adapter.mu.Unlock()
		if f.adapter.dispatches != 1 || f.adapter.releases != 1 {
			t.Fatalf("dispatches=%d releases=%d", f.adapter.dispatches, f.adapter.releases)
		}
	})
}

func TestUnsignedTaskRejectedSilently(t *testing.T) {
	f, enrolled := admitFixture(t)
	admitted, ack := f.receiver.admit(f.ctx, &kgo.Record{Value: []byte("acl-probe")}, enrolled)
	if admitted != nil || ack != kgo.AckReject || topicCount(t, f, wire.TopicEvent) != 0 {
		t.Fatalf("admitted=%v ack=%v events=%d", admitted != nil, ack, topicCount(t, f, wire.TopicEvent))
	}
}

func TestForeignSignedTaskRejectedSilently(t *testing.T) {
	f, enrolled := admitFixture(t)
	_, other, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	admitted, ack := f.receiver.admit(f.ctx, signedRoot(t, other, nil), enrolled)
	if admitted != nil || ack != kgo.AckReject || topicCount(t, f, wire.TopicEvent) != 0 {
		t.Fatalf("admitted=%v ack=%v", admitted != nil, ack)
	}
}

func TestTaskForOtherPoolSkippedSilently(t *testing.T) {
	f, enrolled := admitFixture(t)
	record := signedRoot(t, f.userKey, func(task *wire.SubmissionRecord) { task.Wherry.Pool = "other" })
	admitted, ack := f.receiver.admit(f.ctx, record, enrolled)
	if admitted != nil || ack != kgo.AckAccept || topicCount(t, f, wire.TopicEvent) != 0 {
		t.Fatalf("admitted=%v ack=%v events=%d", admitted != nil, ack, topicCount(t, f, wire.TopicEvent))
	}
}

func TestAdmitRejections(t *testing.T) {
	cases := []struct {
		name   string
		reason string
		mutate func(*wire.SubmissionRecord)
	}{
		{"ExpiredQueueDeadlineRejected", "queue deadline passed", func(task *wire.SubmissionRecord) {
			task.Wherry.QueueDeadlineMS = time.Now().Add(-time.Second).UnixMilli()
		}},
		{"ExpiredRunDeadlineRejected", "run deadline passed", func(task *wire.SubmissionRecord) {
			task.Wherry.DeadlineMS = time.Now().Add(-time.Second).UnixMilli()
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f, enrolled := admitFixture(t)
			admitted, ack := f.receiver.admit(f.ctx, signedRoot(t, f.userKey, c.mutate), enrolled)
			if admitted != nil || ack != kgo.AckReject {
				t.Fatalf("admitted=%v ack=%v", admitted != nil, ack)
			}
			if !strings.Contains(f.logs.String(), c.reason) {
				t.Fatalf("logs %q lack reason %q", f.logs.String(), c.reason)
			}
			if n := topicCount(t, f, wire.TopicEvent); n != 1 {
				t.Fatalf("want one signed REJECTED event, got %d", n)
			}
		})
	}
}

func TestReceiverRejectsNonTextParts(t *testing.T) {
	for name, part := range map[string]wire.Part{
		"raw":  {Raw: wire.StdBytes("hi"), MediaType: "text/plain"},
		"url":  {URL: "https://example.test/a"},
		"data": {Data: jsontext.Value(`{"k":1}`)},
	} {
		t.Run(name, func(t *testing.T) {
			f, enrolled := admitFixture(t)
			f.receiver.credentials["nomad"] = heldCredential{version: 1, value: []byte(testToken)}
			f.receiver.enrolled = enrolled
			f.cluster.SetGroupConfigs("p-"+enrolled.Pool, map[string]string{"share.auto.offset.reset": "earliest"})
			record := signedRoot(t, f.userKey, func(task *wire.SubmissionRecord) {
				task.Message.Parts = append(task.Message.Parts, part)
			})
			record.Topic = wire.TopicTask
			if err := f.kafka.ProduceSync(f.ctx, record).FirstErr(); err != nil {
				t.Fatal(err)
			}
			if err := f.receiver.claimOne(f.ctx, enrolled); err != nil {
				t.Fatal(err)
			}
			if dispatches, _, _, _ := f.adapter.counts(); dispatches != 0 {
				t.Fatalf("dispatched %d runs", dispatches)
			}
			records, err := readBounded(f.ctx, f.receiver.kafkaOpt, wire.TopicEvent)
			if err != nil || len(records) != 1 {
				t.Fatalf("event records=%d err=%v, want one", len(records), err)
			}
			envelope, err := wire.DecodeEnvelope(records[0].Value)
			if err != nil {
				t.Fatal(err)
			}
			verified, err := wire.Verify(envelope, f.receiver.signing.Public().(ed25519.PublicKey), wire.SignerReceiver)
			if err != nil {
				t.Fatal(err)
			}
			status := verified.(*wire.StatusRecord)
			got := status.StatusUpdate.Status
			if got.State != wire.TaskStateRejected || status.Wherry.Kind != "rejected" || got.Message == nil || got.Message.Parts[0].Text != "unsupported message part" {
				t.Fatalf("status %#v", status)
			}
		})
	}
}

func TestAdmissionMismatchSkippedSilently(t *testing.T) {
	f, enrolled := admitFixture(t)
	record := signedRoot(t, f.userKey, func(task *wire.SubmissionRecord) {
		task.Wherry.Selector[0].Values = []string{"docker"}
	})
	admitted, ack := f.receiver.admit(f.ctx, record, enrolled)
	if admitted != nil || ack != kgo.AckAccept || topicCount(t, f, wire.TopicEvent) != 0 {
		t.Fatalf("admitted=%v ack=%v events=%d", admitted != nil, ack, topicCount(t, f, wire.TopicEvent))
	}
}

func TestRejectionPublishFailureReleases(t *testing.T) {
	f, enrolled := admitFixture(t)
	f.receiver.base.Close()
	record := signedRoot(t, f.userKey, func(task *wire.SubmissionRecord) {
		task.Wherry.QueueDeadlineMS = time.Now().Add(-time.Second).UnixMilli()
	})
	admitted, ack := f.receiver.admit(f.ctx, record, enrolled)
	if admitted != nil || ack != kgo.AckRelease {
		t.Fatalf("admitted=%v ack=%v, want nil and release", admitted != nil, ack)
	}
	if n := topicCount(t, f, wire.TopicEvent); n != 0 {
		t.Fatalf("failed rejection publish left %d events", n)
	}
}

func TestResultLineStopsAllocationBeforeTerminal(t *testing.T) {
	f, _ := admitFixture(t)
	run := testActiveRun(t, f, "run-result-stop", 1)
	f.adapter.stopCheck = func() error {
		if n := topicCount(t, f, "replies.test"); n != 0 {
			return fmt.Errorf("reply visible before Stop: %d records", n)
		}
		if n := topicCount(t, f, wire.TopicEvent); n != 0 {
			return fmt.Errorf("terminal event visible before Stop: %d records", n)
		}
		return nil
	}
	status := run.finish(f.ctx, Handle{Alloc: "alloc"}, nil, agentResult{Result: "done"})
	if status != kgo.AckAccept {
		t.Fatalf("finish status %v, want accept", status)
	}
	f.adapter.mu.Lock()
	stops := f.adapter.stops
	f.adapter.mu.Unlock()
	if stops != 1 || topicCount(t, f, "replies.test") != 1 || topicCount(t, f, wire.TopicEvent) != 1 {
		t.Fatalf("stops=%d replies=%d events=%d", stops, topicCount(t, f, "replies.test"), topicCount(t, f, wire.TopicEvent))
	}
}

func TestStopFailureReleasesWithoutTerminal(t *testing.T) {
	for _, test := range []struct {
		name string
		call func(*activeRun, context.Context) kgo.AckStatus
	}{
		{"result", func(run *activeRun, ctx context.Context) kgo.AckStatus {
			return run.finish(ctx, Handle{Alloc: "alloc"}, nil, agentResult{Result: "done"})
		}},
		{"failure", func(run *activeRun, ctx context.Context) kgo.AckStatus {
			return run.stopAndFail(ctx, Handle{Alloc: "alloc"}, "timeout", nil)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, _ := admitFixture(t)
			f.adapter.stopErr = errors.New("stop failed")
			run := testActiveRun(t, f, "run-stop-failure-"+test.name, 1)
			if status := test.call(run, f.ctx); status != kgo.AckRelease {
				t.Fatalf("status %v, want release", status)
			}
			if replies, events := topicCount(t, f, "replies.test"), topicCount(t, f, wire.TopicEvent); replies != 0 || events != 0 {
				t.Fatalf("Stop failure published replies=%d events=%d", replies, events)
			}
		})
	}
}

func TestAdoptionResumesFromLastOffset(t *testing.T) {
	f, _ := admitFixture(t)
	task := testSubmission("run-adopt")
	taskHash := strings.Repeat("a", 64)
	heartbeat := func(offset int64) []byte {
		value, err := sign(wire.TypeStatus, f.receiver.status(task, taskHash, 1, "heartbeat", taskStatus(wire.TaskStateWorking, ""), &offset), f.receiver.signing)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	for _, offset := range []int64{10, 42} {
		if err := f.kafka.ProduceSync(f.ctx, &kgo.Record{Topic: wire.TopicEvent, Key: []byte(task.Wherry.RunID), Value: heartbeat(offset)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	// A fenced holder's aborted heartbeat must not move the resume point.
	aborted, err := kgo.NewClient(append(append([]kgo.Opt(nil), f.receiver.kafkaOpt...), kgo.TransactionalID("run-adopt"))...)
	if err != nil {
		t.Fatal(err)
	}
	defer aborted.Close()
	if err := aborted.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := aborted.ProduceSync(f.ctx, &kgo.Record{Topic: wire.TopicEvent, Key: []byte(task.Wherry.RunID), Value: heartbeat(99)}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := aborted.EndTransaction(f.ctx, kgo.TryAbort); err != nil {
		t.Fatal(err)
	}
	got, err := f.receiver.lastOffset(f.ctx, task.Wherry.RunID, taskHash, task.Wherry.Lineage.RootRunID, task.Wherry.Turn)
	if err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Fatalf("resume offset %d, want 42", got)
	}
}

func TestLastOffsetIgnoresMismatchedStatus(t *testing.T) {
	f, _ := admitFixture(t)
	task := testSubmission("run-offset-binding")
	taskHash := strings.Repeat("b", 64)
	validOffset, mismatchedOffset := int64(42), int64(99)
	wrongTurn := f.receiver.status(task, taskHash, 1, "heartbeat", taskStatus(wire.TaskStateWorking, ""), &mismatchedOffset)
	wrongTurn.Wherry.Turn++
	for _, status := range []*wire.StatusRecord{
		f.receiver.status(task, taskHash, 1, "heartbeat", taskStatus(wire.TaskStateWorking, ""), &validOffset),
		f.receiver.status(task, strings.Repeat("c", 64), 1, "heartbeat", taskStatus(wire.TaskStateWorking, ""), &mismatchedOffset),
		wrongTurn,
	} {
		value, err := sign(wire.TypeStatus, status, f.receiver.signing)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.kafka.ProduceSync(f.ctx, &kgo.Record{Topic: wire.TopicEvent, Key: []byte(task.Wherry.RunID), Value: value}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	got, err := f.receiver.lastOffset(f.ctx, task.Wherry.RunID, taskHash, task.Wherry.Lineage.RootRunID, task.Wherry.Turn)
	if err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Fatalf("resume offset %d, want 42", got)
	}
}

func TestAdoptionReadFailureReleases(t *testing.T) {
	f, _ := admitFixture(t)
	f.cluster.ControlKey(kmsg.ListOffsets.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		req := request.(*kmsg.ListOffsetsRequest)
		response := kmsg.NewPtrListOffsetsResponse()
		response.SetVersion(req.Version)
		for _, topic := range req.Topics {
			out := kmsg.ListOffsetsResponseTopic{Topic: topic.Topic}
			for _, partition := range topic.Partitions {
				out.Partitions = append(out.Partitions, kmsg.ListOffsetsResponseTopicPartition{
					Partition: partition.Partition,
					ErrorCode: kerr.ClusterAuthorizationFailed.Code,
				})
			}
			response.Topics = append(response.Topics, out)
		}
		return response, nil, true
	})
	record := signedRoot(t, f.userKey, nil)
	task, err := trust.VerifyRootSubmission(record.Value, f.receiver.userPub)
	if err != nil {
		t.Fatal(err)
	}
	status := executeSubmission(t, f, record, &admission{task: task, offer: wire.Offer{Adapter: "nomad", Labels: map[string]string{"runtime": "exec"}}, prompt: "hi"}, 2)
	if status != kgo.AckRelease {
		t.Fatalf("status %v, want release", status)
	}
	f.adapter.mu.Lock()
	dispatches := f.adapter.dispatches
	f.adapter.mu.Unlock()
	if dispatches != 0 {
		t.Fatalf("adoption read failure dispatched %d jobs", dispatches)
	}
}

func TestAdoptionTrustsPeerHeartbeat(t *testing.T) {
	f, _ := admitFixture(t)
	enrol := func(id, pool string, signing ed25519.PrivateKey, sealing *ecdh.PrivateKey) {
		t.Helper()
		request, err := trust.NewEnrolRequest(id, signing, sealing, map[string]string{}, []wire.DetectedAdapter{{Name: "nomad", Platform: "nomad", Offers: []wire.Offer{}}}, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		enrolRequest, err := trust.NewState(f.userKey.Public().(ed25519.PublicKey)).Apply(request)
		if err != nil {
			t.Fatal(err)
		}
		approval, err := trust.Approve(request, enrolRequest.(*wire.EnrolRequest), f.userKey, map[string]string{}, pool, nomadGrants(), []wire.Requirement{}, "auto", 1, "", time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		for _, value := range [][]byte{request, approval} {
			if err := f.kafka.ProduceSync(f.ctx, &kgo.Record{Topic: wire.TopicState, Key: []byte(id), Value: value}).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
	}
	enrol(f.receiver.config.ReceiverID, "nomad", f.receiver.signing, f.receiver.sealing)
	_, peerSigning, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	peerSealing, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	enrol("receiver-peer", "nomad", peerSigning, peerSealing)
	_, otherPoolSigning, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	otherPoolSealing, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	enrol("receiver-other-pool", "other", otherPoolSigning, otherPoolSealing)
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	task := testSubmission("run-peer")
	offset := int64(42)
	taskHash := strings.Repeat("a", 64)
	value, err := sign(wire.TypeStatus, f.receiver.status(task, taskHash, 1, "heartbeat", taskStatus(wire.TaskStateWorking, ""), &offset), peerSigning)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.kafka.ProduceSync(f.ctx, &kgo.Record{Topic: wire.TopicEvent, Key: []byte(task.Wherry.RunID), Value: value}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	otherPoolOffset := int64(99)
	value, err = sign(wire.TypeStatus, f.receiver.status(task, taskHash, 1, "heartbeat", taskStatus(wire.TaskStateWorking, ""), &otherPoolOffset), otherPoolSigning)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.kafka.ProduceSync(f.ctx, &kgo.Record{Topic: wire.TopicEvent, Key: []byte(task.Wherry.RunID), Value: value}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	got, err := f.receiver.lastOffset(f.ctx, task.Wherry.RunID, taskHash, task.Wherry.Lineage.RootRunID, task.Wherry.Turn)
	if err != nil {
		t.Fatal(err)
	}
	if got != offset {
		t.Fatalf("peer heartbeat offset %d, want %d", got, offset)
	}
}

func TestFollowErrorReasonIsFixed(t *testing.T) {
	f := newFixture(t, nil)
	f.adapter.followErr = errors.New("secret upstream detail")
	task := runToTask(t, f)
	if got := task.Artifacts[0].Parts[0].Text; got != "log stream failed" {
		t.Fatalf("reply reason %q, want fixed reason", got)
	}
	logs := f.logs.String()
	if strings.Contains(logs, "secret upstream detail") || !strings.Contains(logs, "*errors.errorString") {
		t.Fatalf("unexpected follow error log: %q", logs)
	}
}

func TestDispatchErrorReasonIsFixed(t *testing.T) {
	const secret = "token=s3cr3t-dispatch-detail"
	f, enrolled := admitFixture(t)
	f.receiver.credentials["nomad"] = heldCredential{version: 1, value: []byte(testToken)}
	f.adapter.dispatchErr = errors.New(secret)
	record := signedRoot(t, f.userKey, nil)
	admitted, _ := f.receiver.admit(f.ctx, record, enrolled)
	if status := executeSubmission(t, f, record, admitted, 1); status != kgo.AckReject {
		t.Fatalf("status %v", status)
	}
	if got := lastTask(t, f).Artifacts[0].Parts[0].Text; got != "dispatch failed" {
		t.Fatalf("reply reason %q, want fixed reason", got)
	}
	for _, topic := range []string{wire.TopicEvent, "replies.test"} {
		records, err := readBounded(f.ctx, f.receiver.kafkaOpt, topic)
		if err != nil || len(records) == 0 {
			t.Fatalf("%s records=%d err=%v", topic, len(records), err)
		}
		for _, record := range records {
			envelope, err := wire.DecodeEnvelope(record.Value)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(envelope.Payload), secret) {
				t.Fatalf("dispatch error detail published on %s: %s", topic, envelope.Payload)
			}
		}
	}
	logs := f.logs.String()
	if strings.Contains(logs, secret) || !strings.Contains(logs, "*errors.errorString") {
		t.Fatalf("unexpected dispatch error log: %q", logs)
	}
}

func TestRunJoinsPeriodicWork(t *testing.T) {
	f := newFixture(t, resultStream)
	runCtx, cancel := context.WithCancel(f.ctx)
	done := make(chan error, 1)
	go func() { done <- f.receiver.Run(runCtx) }()
	t.Cleanup(cancel)

	client := busclient.New(f.kafka, f.userKey, "replies.test")
	var requestHash string
	for requestHash == "" {
		enrolments, _, err := client.Enrolments(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, enrolment := range enrolments {
			if enrolment.ReceiverID == f.receiver.config.ReceiverID {
				requestHash = enrolment.RequestHash
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := client.Approve(f.ctx, requestHash, "", map[string]string{}, nomadGrants(), []wire.Requirement{}, "auto", 30); err != nil {
		t.Fatal(err)
	}
	for !strings.Contains(f.logs.String(), "enrolled:") {
		if f.ctx.Err() != nil {
			t.Fatal("receiver did not finish enrolment")
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want context canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after periodic work completed")
	}
}

func TestFencedProducerStopsRunWrites(t *testing.T) {
	f, _ := admitFixture(t)
	zombie := testActiveRun(t, f, "run-fence", 1)
	if err := zombie.commit(f.ctx, zombie.statusRecord("claimed", taskStatus(wire.TaskStateWorking, ""), nil)); err != nil {
		t.Fatal(err)
	}
	adopter := testActiveRun(t, f, "run-fence", 2)
	if err := adopter.commit(f.ctx, adopter.statusRecord("claimed", taskStatus(wire.TaskStateWorking, ""), nil)); err != nil {
		t.Fatal(err)
	}
	err := zombie.commit(f.ctx, zombie.statusRecord("heartbeat", taskStatus(wire.TaskStateWorking, ""), nil))
	if err == nil {
		t.Fatal("zombie write succeeded after adoption")
	}
	if status := zombie.abandon(err); status != 0 {
		t.Fatalf("fenced run returned ack %v, want 0 (no acknowledgement)", status)
	}
	records, err := readBounded(f.ctx, f.receiver.kafkaOpt, wire.TopicEvent)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		envelope, _ := wire.DecodeEnvelope(record.Value)
		decoded, _ := wire.DecodePayload(wire.TypeStatus, envelope.Payload)
		if status := decoded.(*wire.StatusRecord); status.Wherry.Kind == "heartbeat" {
			t.Fatal("zombie heartbeat is readable after fencing")
		}
	}
}

func TestResultLine(t *testing.T) {
	if result, ok := resultLine([]byte(`{"type":"result","result":"ok","is_error":true}`)); !ok || result.Result != "ok" || !result.IsError {
		t.Fatalf("got %#v %v", result, ok)
	}
	for _, line := range []string{`{"type":"assistant"}`, `not json`, ``} {
		if _, ok := resultLine([]byte(line)); ok {
			t.Fatalf("%q treated as a result", line)
		}
	}
}

type multiFixture struct {
	fixture
	lab, cloud, edge *fakeAdapter
}

// newMultiFixture enrols a receiver hosting nomad adapters lab and edge and an aws adapter cloud;
// only lab's credential is delivered.
func newMultiFixture(t *testing.T) multiFixture {
	t.Helper()
	f := newFixture(t, resultStream)
	m := multiFixture{
		fixture: f,
		lab:     &fakeAdapter{stream: resultStream, offers: []map[string]string{{"runtime": "exec"}}},
		cloud:   &fakeAdapter{platform: "aws", stream: resultStream, offers: []map[string]string{{"runtime": "lambda"}, {"runtime": "exec"}}},
		edge:    &fakeAdapter{stream: resultStream, offers: []map[string]string{{"runtime": "podman"}}},
	}
	f.receiver.adapters = []Named{
		{Name: "lab", Credential: "cred-lab", Adapter: m.lab},
		{Name: "cloud", Credential: "cred-cloud", Adapter: m.cloud},
		{Name: "edge", Adapter: m.edge},
	}
	request, err := trust.NewEnrolRequest(f.receiver.config.ReceiverID, f.receiver.signing, f.receiver.sealing, map[string]string{}, []wire.DetectedAdapter{
		{Name: "lab", Platform: "nomad", Offers: []wire.Offer{}},
		{Name: "cloud", Platform: "aws", Offers: []wire.Offer{}},
		{Name: "edge", Platform: "nomad", Offers: []wire.Offer{}},
	}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := f.kafka.ProduceSync(f.ctx, &kgo.Record{Topic: wire.TopicState, Key: []byte(f.receiver.config.ReceiverID), Value: request}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	grants := []wire.AdapterGrant{
		{Name: "lab", Platform: "nomad", Allowed: map[string][]string{"runtime": {"exec"}}, Defaults: map[string]string{}},
		{Name: "cloud", Platform: "aws", Allowed: map[string][]string{"runtime": {"lambda", "exec"}}, Defaults: map[string]string{}},
		{Name: "edge", Platform: "nomad", Allowed: map[string][]string{"runtime": {"podman"}}, Defaults: map[string]string{}},
	}
	client := busclient.New(f.kafka, f.userKey, "replies.test")
	if _, err := client.Approve(f.ctx, trust.EnvelopeHash(request), "nomad", map[string]string{}, grants, []wire.Requirement{}, "auto", 30); err != nil {
		t.Fatal(err)
	}
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(testToken), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := client.PutSecret(f.ctx, "cred-lab", f.receiver.config.ReceiverID, tokenFile, 30*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	if f.receiver.current() == nil {
		t.Fatalf("receiver not enrolled: %s", f.logs.String())
	}
	if err := f.receiver.publishCapability(f.ctx); err != nil {
		t.Fatal(err)
	}
	return m
}

func (m multiFixture) execute(t *testing.T, runID string, selector []wire.Requirement, attempt int64) kgo.AckStatus {
	t.Helper()
	record := signedRoot(t, m.userKey, func(task *wire.SubmissionRecord) {
		task.Wherry.RunID, task.Wherry.Lineage.RootRunID, task.Wherry.Selector = runID, runID, selector
		task.Message.TaskID, task.Message.ContextID = runID, runID
	})
	admitted, ack := m.receiver.admit(m.ctx, record, m.receiver.current())
	if admitted == nil {
		t.Fatalf("task %s not admitted (ack %v)", runID, ack)
	}
	return executeSubmission(t, m.fixture, record, admitted, attempt)
}

func runtimeIs(value string) []wire.Requirement {
	return []wire.Requirement{{Key: "runtime", Operator: wire.OpIn, Values: []string{value}}}
}

// published returns the receiver-verified payloads on topic in log order.
func published(t *testing.T, f fixture, topic string) []any {
	t.Helper()
	records, err := readBounded(f.ctx, f.receiver.kafkaOpt, topic)
	if err != nil {
		t.Fatal(err)
	}
	payloads := make([]any, 0, len(records))
	for _, record := range records {
		envelope, err := wire.DecodeEnvelope(record.Value)
		if err != nil {
			t.Fatal(err)
		}
		verified, err := wire.Verify(envelope, f.receiver.signing.Public().(ed25519.PublicKey), wire.SignerReceiver)
		if err != nil {
			t.Fatal(err)
		}
		payloads = append(payloads, verified)
	}
	return payloads
}

func lastTask(t *testing.T, f fixture) *wire.Task {
	t.Helper()
	tasks := published(t, f, "replies.test")
	if len(tasks) == 0 {
		t.Fatal("no task records")
	}
	return &tasks[len(tasks)-1].(*wire.TaskRecord).Task
}

func TestReceiverRoutesToMatchedAdapter(t *testing.T) {
	m := newMultiFixture(t)
	m.receiver.credentials["cloud"] = heldCredential{version: 1, value: []byte("cloud-token")}
	if status := m.execute(t, "run-lambda", runtimeIs("lambda"), 1); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	labDispatches, _, _, _ := m.lab.counts()
	cloudDispatches, _, cloudFollows, cloudReleases := m.cloud.counts()
	if labDispatches != 0 || cloudDispatches != 1 || cloudFollows != 1 || cloudReleases != 1 || m.cloud.lastRun.Offer["runtime"] != "lambda" {
		t.Fatalf("lab dispatches=%d; cloud dispatches=%d follows=%d releases=%d run=%#v", labDispatches, cloudDispatches, cloudFollows, cloudReleases, m.cloud.lastRun)
	}
	if status := m.execute(t, "run-exec", runtimeIs("exec"), 1); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	labDispatches, _, _, _ = m.lab.counts()
	cloudDispatches, _, _, _ = m.cloud.counts()
	if labDispatches != 1 || cloudDispatches != 1 {
		t.Fatalf("exec, offered by both, went to lab=%d cloud=%d; want the first grant", labDispatches, cloudDispatches-1)
	}
}

func TestReceiverRedeliveryAdoptsViaLookup(t *testing.T) {
	m := newMultiFixture(t)
	m.cloud.existing = true
	if status := m.execute(t, "run-moved", runtimeIs("exec"), 2); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	labDispatches, labLookups, labFollows, labReleases := m.lab.counts()
	cloudDispatches, cloudLookups, cloudFollows, cloudReleases := m.cloud.counts()
	if labLookups != 1 || cloudLookups != 1 {
		t.Fatalf("lookups lab=%d cloud=%d, want one each in order", labLookups, cloudLookups)
	}
	if labDispatches != 0 || labFollows != 0 || labReleases != 0 {
		t.Fatalf("matched adapter used: dispatches=%d follows=%d releases=%d", labDispatches, labFollows, labReleases)
	}
	if cloudDispatches != 0 || cloudFollows != 1 || cloudReleases != 1 {
		t.Fatalf("adopting adapter: dispatches=%d follows=%d releases=%d, want 0, 1, 1", cloudDispatches, cloudFollows, cloudReleases)
	}
	if _, edgeLookups, _, _ := m.edge.counts(); edgeLookups != 0 {
		t.Fatalf("lookup continued past the first hit: edge lookups=%d", edgeLookups)
	}
}

func TestReceiverFirstAttemptSkipsLookup(t *testing.T) {
	m := newMultiFixture(t)
	m.cloud.existing = true
	if status := m.execute(t, "run-first", runtimeIs("exec"), 1); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	labDispatches, labLookups, _, _ := m.lab.counts()
	_, cloudLookups, _, _ := m.cloud.counts()
	if labLookups != 0 || cloudLookups != 0 || labDispatches != 1 {
		t.Fatalf("lookups lab=%d cloud=%d, lab dispatches=%d", labLookups, cloudLookups, labDispatches)
	}
}

func TestReceiverAdapterOffersErrorIsolated(t *testing.T) {
	m := newMultiFixture(t)
	m.lab.mu.Lock()
	m.lab.offersErr = errors.New("nomad unreachable")
	m.lab.mu.Unlock()
	if err := m.receiver.publishCapability(m.ctx); err != nil {
		t.Fatal(err)
	}
	m.receiver.mu.Lock()
	offers := m.receiver.offers
	m.receiver.mu.Unlock()
	var got []string
	for _, offer := range offers {
		got = append(got, offer.Adapter+":"+wire.TermsText(offer.Labels))
	}
	want := []string{"cloud:runtime=exec", "cloud:runtime=lambda", "edge:runtime=podman"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("published offers %v, want %v", got, want)
	}
	if !strings.Contains(m.logs.String(), "adapter lab offers: nomad unreachable") {
		t.Fatalf("offers error not logged: %q", m.logs.String())
	}
}

func TestReceiverSweepsOnTick(t *testing.T) {
	f := newFixture(t, resultStream)
	runCtx, cancel := context.WithCancel(f.ctx)
	done := make(chan error, 1)
	go func() { done <- f.receiver.Run(runCtx) }()
	t.Cleanup(func() { cancel(); <-done })

	client := busclient.New(f.kafka, f.userKey, "replies.test")
	var requestHash string
	for requestHash == "" {
		enrolments, _, err := client.Enrolments(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, enrolment := range enrolments {
			if enrolment.ReceiverID == f.receiver.config.ReceiverID {
				requestHash = enrolment.RequestHash
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := client.Approve(f.ctx, requestHash, "", map[string]string{}, nomadGrants(), []wire.Requirement{}, "auto", 30); err != nil {
		t.Fatal(err)
	}
	for f.adapter.sweepCount() < 3 {
		if f.ctx.Err() != nil {
			t.Fatalf("sweeps = %d, want repeated sweeps on the capability tick", f.adapter.sweepCount())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestReceiverSweepErrorIsolated(t *testing.T) {
	m := newMultiFixture(t)
	const detail = "variable item test-credential"
	m.lab.mu.Lock()
	m.lab.sweepErr = errors.New(detail)
	m.lab.mu.Unlock()
	before := []int{m.lab.sweepCount(), m.cloud.sweepCount(), m.edge.sweepCount()}
	if err := m.receiver.publishCapability(m.ctx); err != nil {
		t.Fatal(err)
	}
	after := []int{m.lab.sweepCount(), m.cloud.sweepCount(), m.edge.sweepCount()}
	for i := range after {
		if after[i] != before[i]+1 {
			t.Fatalf("sweeps lab, cloud, edge went %v -> %v, want one more each", before, after)
		}
	}
	m.receiver.mu.Lock()
	offers := len(m.receiver.offers)
	m.receiver.mu.Unlock()
	if offers != 4 {
		t.Fatalf("published %d offers after a sweep error, want 4", offers)
	}
	logs := m.logs.String()
	if !strings.Contains(logs, "adapter lab sweep: *errors.errorString") || strings.Contains(logs, detail) {
		t.Fatalf("unexpected sweep error log: %q", logs)
	}
}

func TestReceiverCredentialPerAdapter(t *testing.T) {
	m := newMultiFixture(t)
	if status := m.execute(t, "run-lab", runtimeIs("exec"), 1); status != kgo.AckAccept {
		t.Fatalf("lab status %v", status)
	}
	if got := string(m.lab.lastRun.Credential); got != testToken {
		t.Fatal("lab adapter did not receive its own credential")
	}
	if status := m.execute(t, "run-edge", runtimeIs("podman"), 1); status != kgo.AckAccept {
		t.Fatalf("edge status %v", status)
	}
	if m.edge.lastRun.Credential != nil {
		t.Fatal("adapter without a configured credential received one")
	}
	if status := m.execute(t, "run-cloud", runtimeIs("lambda"), 1); status != kgo.AckAccept {
		t.Fatalf("cloud status %v, want accept", status)
	}
	if dispatches, _, _, releases := m.cloud.counts(); dispatches != 0 || releases != 1 {
		t.Fatalf("cloud dispatches=%d releases=%d, want 0 and 1", dispatches, releases)
	}
	statuses := published(t, m.fixture, wire.TopicEvent)
	last := statuses[len(statuses)-1].(*wire.StatusRecord)
	if got := last.StatusUpdate.Status.Message.Parts[0].Text; got != "credential cred-cloud required" {
		t.Fatalf("reason %q", got)
	}
	if strings.Contains(m.logs.String(), testToken) {
		t.Fatal("credential value appeared in receiver logs")
	}
}

func TestReceiverStopsClaimingWhenRevoked(t *testing.T) {
	f := newFixture(t, resultStream)
	if task := runToTask(t, f); task.Status.State != wire.TaskStateCompleted {
		t.Fatalf("unexpected task before revocation: %#v", task)
	}
	client := busclient.New(f.kafka, f.userKey, "replies.test")
	if _, err := client.Revoke(f.ctx, f.receiver.config.ReceiverID); err != nil {
		t.Fatal(err)
	}
	for f.receiver.current() != nil || !strings.Contains(f.logs.String(), "revoked at generation 1") {
		if f.ctx.Err() != nil {
			t.Fatalf("receiver still enrolled after revocation; logs:\n%s", f.logs.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	f.receiver.mu.Lock()
	held := len(f.receiver.credentials)
	f.receiver.mu.Unlock()
	if held != 0 {
		t.Fatalf("revoked receiver still holds %d credentials", held)
	}

	capabilities := topicCount(t, f, wire.TopicCapability)
	submission := testSubmission("run-after-revocation")
	submission.Wherry.Pool, submission.Wherry.Labels = f.receiver.config.ReceiverID, map[string]string{}
	value, err := trust.SignRootSubmission(*submission, f.userKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.kafka.ProduceSync(f.ctx, &kgo.Record{Topic: wire.TopicTask, Key: []byte(submission.Wherry.RunID), Value: value}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	if after := topicCount(t, f, wire.TopicCapability); after != capabilities {
		t.Fatalf("revoked receiver published %d capability records", after-capabilities)
	}
	if dispatches, _, _, _ := f.adapter.counts(); dispatches != 1 {
		t.Fatalf("dispatches %d after revocation, want only the task before it", dispatches)
	}
	for _, payload := range published(t, f, wire.TopicEvent) {
		if status, ok := payload.(*wire.StatusRecord); ok && status.StatusUpdate.TaskID == submission.Wherry.RunID {
			t.Fatalf("revoked receiver claimed %s: %#v", submission.Wherry.RunID, status)
		}
	}
	// The task stays available to the pool: released, not acknowledged away.
	peer, err := kgo.NewClient(append(append([]kgo.Opt(nil), f.receiver.kafkaOpt...),
		kgo.ConsumeTopics(wire.TopicTask), kgo.ShareGroup("p-"+f.receiver.config.ReceiverID))...)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	pollCtx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
	defer cancel()
	for available := false; !available; {
		fetches := peer.PollRecords(pollCtx, 10)
		if pollCtx.Err() != nil {
			t.Fatal("task submitted after revocation is no longer available to the pool")
		}
		for _, record := range fetches.Records() {
			available = available || string(record.Key) == submission.Wherry.RunID
		}
	}
	_, err = client.Submit(f.ctx, busclient.SubmitInput{
		Selector: []wire.Requirement{{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}}}, Labels: map[string]string{}, Prompt: "hello",
		Deadline: time.Minute, QueueDeadline: time.Minute,
	})
	if err == nil || !strings.Contains(err.Error(), "no matching live receiver") {
		t.Fatalf("submit after revocation: got %v, want no matching live receiver", err)
	}
}

// startInFlight enrols the fixture receiver until exp and returns once a claimed run is following its job.
func startInFlight(t *testing.T, f fixture, runID string, exp time.Time) <-chan error {
	t.Helper()
	r := f.receiver
	request, err := trust.NewEnrolRequest(r.config.ReceiverID, r.signing, r.sealing, map[string]string{}, []wire.DetectedAdapter{{Name: "nomad", Platform: "nomad", Offers: []wire.Offer{}}}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	verified, err := trust.NewState(r.userPub).Apply(request)
	if err != nil {
		t.Fatal(err)
	}
	approval, err := trust.Approve(request, verified.(*wire.EnrolRequest), f.userKey, map[string]string{}, "nomad", nomadGrants(), []wire.Requirement{}, "auto", 1, "", exp)
	if err != nil {
		t.Fatal(err)
	}
	submission := testSubmission(runID)
	value, err := trust.SignRootSubmission(*submission, f.userKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.kafka.ProduceSync(f.ctx,
		&kgo.Record{Topic: wire.TopicState, Key: []byte(r.config.ReceiverID), Value: request},
		&kgo.Record{Topic: wire.TopicState, Key: []byte(r.config.ReceiverID), Value: approval},
		&kgo.Record{Topic: wire.TopicTask, Key: []byte(runID), Value: value},
	).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := r.refresh(f.ctx); err != nil || r.current() == nil {
		t.Fatalf("enrol: %v", err)
	}
	r.mu.Lock()
	r.offers = []wire.Offer{{Adapter: "nomad", Labels: map[string]string{"runtime": "exec", "nomad.node_pool": "OCPv", "zone": "dc1"}}}
	r.credentials["nomad"] = heldCredential{version: 1, value: []byte(testToken)}
	r.mu.Unlock()
	f.cluster.SetGroupConfigs("p-nomad", map[string]string{"share.auto.offset.reset": "earliest"})
	done := make(chan error, 1)
	go func() { done <- r.claimOne(f.ctx, r.current()) }()
	for {
		if _, _, follows, _ := f.adapter.counts(); follows == 1 {
			return done
		}
		if f.ctx.Err() != nil {
			t.Fatalf("run never started; logs:\n%s", f.logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func terminalReason(t *testing.T, f fixture, runID string) (wire.TaskState, string) {
	t.Helper()
	for _, payload := range published(t, f, wire.TopicEvent) {
		if status, ok := payload.(*wire.StatusRecord); ok && status.StatusUpdate.TaskID == runID && status.Wherry.Kind == "terminal" {
			return status.StatusUpdate.Status.State, status.StatusUpdate.Status.Message.Parts[0].Text
		}
	}
	t.Fatal("no terminal status")
	return "", ""
}

func TestRevokedReceiverStopsInFlightRuns(t *testing.T) {
	f := newFixture(t, noResultStream)
	f.adapter.endDelay = 30 * time.Second
	done := startInFlight(t, f, "run-revoked", time.Now().Add(time.Hour))
	revocation, err := trust.NewRevocation(f.receiver.config.ReceiverID, 1, f.userKey, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := f.kafka.ProduceSync(f.ctx, &kgo.Record{Topic: wire.TopicState, Key: []byte(f.receiver.config.ReceiverID), Value: revocation}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.refresh(f.ctx); err != nil || f.receiver.current() != nil {
		t.Fatalf("refresh after revocation: err=%v enrolled=%v", err, f.receiver.current())
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("in-flight run kept going after revocation")
	}
	f.adapter.mu.Lock()
	stops, releaseIDs := f.adapter.stops, f.adapter.releaseIDs
	f.adapter.mu.Unlock()
	if stops != 1 || !slices.Equal(releaseIDs, []string{"run-revoked"}) {
		t.Fatalf("stops=%d releases=%v", stops, releaseIDs)
	}
	if state, reason := terminalReason(t, f, "run-revoked"); state != wire.TaskStateFailed || reason != "receiver revoked" {
		t.Fatalf("terminal %s %q", state, reason)
	}
	// Released, not acknowledged: the submission is delivered again to the pool.
	peer, err := kgo.NewClient(append(append([]kgo.Opt(nil), f.receiver.kafkaOpt...),
		kgo.ConsumeTopics(wire.TopicTask), kgo.ShareGroup("p-nomad"))...)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	pollCtx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
	defer cancel()
	for redelivered := false; !redelivered; {
		fetches := peer.PollRecords(pollCtx, 10)
		if pollCtx.Err() != nil {
			t.Fatal("revoked run's submission was not released to the pool")
		}
		for _, record := range fetches.Records() {
			redelivered = redelivered || string(record.Key) == "run-revoked" && record.DeliveryCount() > 1
		}
	}
}

func TestExpiryDoesNotStopInFlightRuns(t *testing.T) {
	f := newFixture(t, noResultStream)
	f.adapter.endDelay = 4 * time.Second
	exp := time.Now().Add(time.Second)
	done := startInFlight(t, f, "run-expired", exp)
	time.Sleep(time.Until(exp))
	if err := f.receiver.refresh(f.ctx); err != nil || f.receiver.current() != nil {
		t.Fatalf("refresh after expiry: err=%v enrolled=%v", err, f.receiver.current())
	}
	time.Sleep(5 * renewInterval)
	f.adapter.mu.Lock()
	stops := f.adapter.stops
	f.adapter.mu.Unlock()
	if stops != 0 {
		t.Fatalf("expiry stopped the in-flight run: stops=%d", stops)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if state, reason := terminalReason(t, f, "run-expired"); state != wire.TaskStateFailed || reason != "agent ended without a result" {
		t.Fatalf("terminal %s %q", state, reason)
	}
}

// renewRequests returns the receiver-signed renew requests on the state topic.
func renewRequests(t *testing.T, f fixture) []*wire.RenewRequest {
	t.Helper()
	records, err := readBounded(f.ctx, f.receiver.kafkaOpt, wire.TopicState)
	if err != nil {
		t.Fatal(err)
	}
	var requests []*wire.RenewRequest
	for _, record := range records {
		envelope, err := wire.DecodeEnvelope(record.Value)
		if err != nil || envelope.Type != wire.TypeRenewRequest {
			continue
		}
		verified, err := wire.Verify(envelope, f.receiver.signing.Public().(ed25519.PublicKey), wire.SignerReceiver)
		if err != nil {
			t.Fatal(err)
		}
		requests = append(requests, verified.(*wire.RenewRequest))
	}
	return requests
}

func TestReceiverRequestsRenewalBeforeExpiry(t *testing.T) {
	f := newFixture(t, resultStream)
	r := f.receiver
	request, err := trust.NewEnrolRequest(r.config.ReceiverID, r.signing, r.sealing, map[string]string{}, []wire.DetectedAdapter{{Name: "nomad", Platform: "nomad", Offers: []wire.Offer{}}}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	verified, err := trust.NewState(r.userPub).Apply(request)
	if err != nil {
		t.Fatal(err)
	}
	approval, err := trust.Approve(request, verified.(*wire.EnrolRequest), f.userKey, map[string]string{}, "nomad", nomadGrants(), []wire.Requirement{}, "auto", 1, "", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.kafka.ProduceSync(f.ctx,
		&kgo.Record{Topic: wire.TopicState, Key: []byte(r.config.ReceiverID), Value: request},
		// Approved 2h ago for 3h: a third of the lifetime is left.
		&kgo.Record{Topic: wire.TopicState, Key: []byte(r.config.ReceiverID), Value: approval, Timestamp: time.Now().Add(-2*time.Hour - time.Minute)},
	).FirstErr(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := r.refresh(f.ctx); err != nil || r.current() == nil {
			t.Fatalf("refresh: %v", err)
		}
	}
	requests := renewRequests(t, f)
	if len(requests) != 1 || requests[0].ReceiverID != r.config.ReceiverID || requests[0].Generation != 1 {
		t.Fatalf("renew requests %#v, want one for generation 1 within the hour", requests)
	}
	if !strings.Contains(f.logs.String(), "renewal requested: generation=1") {
		t.Fatalf("logs:\n%s", f.logs.String())
	}
	if _, _, err := busclient.New(f.kafka, f.userKey, "replies.test").Receivers(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	if enrolled := r.current(); enrolled == nil || enrolled.Generation != 1 || enrolled.ApprovalHash != trust.EnvelopeHash(approval) {
		t.Fatalf("renewed enrolment %#v", enrolled)
	}
	if requests := renewRequests(t, f); len(requests) != 1 {
		t.Fatalf("%d renew requests after renewal, want no new one", len(requests))
	}
}

func TestContinuationAcrossRenewal(t *testing.T) {
	f := newFixture(t, resultStream)
	completed := interruptThenContinue(t, f, "first", "second", func(client *busclient.Client) {
		r := f.receiver
		request, err := sign(wire.TypeRenewRequest, &wire.RenewRequest{ReceiverID: r.config.ReceiverID, Generation: 1, AtMS: time.Now().UnixMilli()}, r.signing)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.kafka.ProduceSync(f.ctx, &kgo.Record{Topic: wire.TopicState, Key: []byte(r.config.ReceiverID), Value: request}).FirstErr(); err != nil {
			t.Fatal(err)
		}
		before := topicCount(t, f, wire.TopicState)
		if _, _, err := client.Receivers(f.ctx); err != nil {
			t.Fatal(err)
		}
		if topicCount(t, f, wire.TopicState) != before+1 {
			t.Fatal("renew request was not answered")
		}
		if err := r.refresh(f.ctx); err != nil || r.current() == nil || r.current().Generation != 1 {
			t.Fatalf("refresh after renewal: %v", err)
		}
	})
	if completed.Status.State != wire.TaskStateCompleted || completed.Task == nil || len(completed.Task.History) != 2 {
		t.Fatalf("continuation across renewal %#v", completed)
	}
}
