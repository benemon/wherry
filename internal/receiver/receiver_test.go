package receiver

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	busclient "github.com/benemon/agentbus/internal/client"
	"github.com/benemon/agentbus/internal/trust"
	"github.com/benemon/agentbus/internal/wire"
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
	return Handle{Job: "agent-" + run.ID, Alloc: run.ID}, nil
}

func (f *fakeAdapter) Lookup(_ context.Context, runID string) (Handle, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lookups++
	if !f.existing {
		return Handle{}, false, nil
	}
	return Handle{Job: "agent-" + runID, Alloc: runID}, true, nil
}

// Follow emits the stream; a stream with a result keeps running like a live task, one without ends.
func (f *fakeAdapter) Follow(ctx context.Context, _ Handle, _ int64, lines chan<- Line) error {
	f.mu.Lock()
	f.follows++
	stream, followErr := append([]string(nil), f.stream...), f.followErr
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
	if len(stream) == len(noResultStream) {
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

func (f *fakeAdapter) Release(_ context.Context, runID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releases++
	f.releaseIDs = append(f.releaseIDs, runID)
	return nil
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

// runToReply enrols the receiver, delivers a credential, submits one task and returns its reply.
func runToReply(t *testing.T, f fixture) *wire.Reply {
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
	if _, err := client.PutSecret(f.ctx, credentialName, "receiver-nomad", tokenFile); err != nil {
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
	return result.Reply
}

func TestEndToEndWithFakeAdapter(t *testing.T) {
	f := newFixture(t, resultStream)
	reply := runToReply(t, f)
	if reply.State != wire.TaskStateCompleted || reply.Artifacts[0].Parts[0].Text != "done" {
		t.Fatalf("unexpected reply: %#v", reply)
	}
}

func TestRunEndingWithoutResultFails(t *testing.T) {
	f := newFixture(t, noResultStream)
	reply := runToReply(t, f)
	if reply.State != wire.TaskStateFailed || reply.Artifacts[0].Parts[0].Text != "agent ended without a result" {
		t.Fatalf("unexpected reply: %#v", reply)
	}
	f.adapter.mu.Lock()
	defer f.adapter.mu.Unlock()
	if f.adapter.stops != 1 {
		t.Fatalf("run ended without a result but Stop was called %d times, want 1", f.adapter.stops)
	}
}

func TestSecretNeverLogged(t *testing.T) {
	f := newFixture(t, resultStream)
	runToReply(t, f)
	// A forged, unsigned record as the latest sealed secret must be refused without echoing anything.
	if err := f.kafka.ProduceSync(f.ctx, &kgo.Record{Topic: secretTopic, Key: []byte("receiver-nomad/" + credentialName), Value: []byte(testToken)}).FirstErr(); err != nil {
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
			approval, err := trust.Approve(request, f.userKey, map[string]string{}, "nomad", []wire.AdapterGrant{test.grant}, []wire.Requirement{}, "auto", 1, "", time.Now().Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if err := f.kafka.ProduceSync(f.ctx,
				&kgo.Record{Topic: stateTopic, Key: []byte(f.receiver.config.ReceiverID), Value: request},
				&kgo.Record{Topic: stateTopic, Key: []byte(f.receiver.config.ReceiverID), Value: approval},
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

func signedRoot(t *testing.T, key ed25519.PrivateKey, mutate func(*wire.Task)) *kgo.Record {
	t.Helper()
	task := wire.Task{
		RunID: "run-a", RootRunID: "run-a", Attempt: 1, Ancestors: []string{}, Chain: []wire.Bytes{},
		Pool: "nomad", Labels: map[string]string{"lab/trust": "lab"},
		Selector:   []wire.Requirement{{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}}},
		DeadlineMS: time.Now().Add(time.Minute).UnixMilli(), QueueDeadlineMS: time.Now().Add(time.Minute).UnixMilli(),
		ReplyTopic: "replies.test", CorrelationID: "corr",
		Message: wire.Message{MessageID: "msg-a", Role: wire.RoleUser, Parts: []wire.Part{{Text: "hi"}}},
	}
	if mutate != nil {
		mutate(&task)
	}
	value, err := trust.SignRootTask(task, key)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Key: []byte(task.RunID), Value: value}
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

func eventCount(t *testing.T, f fixture) int {
	t.Helper()
	records, err := readBounded(f.ctx, f.receiver.kafkaOpt, eventTopic)
	if err != nil {
		t.Fatal(err)
	}
	return len(records)
}

func topicCount(t *testing.T, f fixture, topic string) int {
	t.Helper()
	records, err := readBounded(f.ctx, f.receiver.kafkaOpt, topic)
	if err != nil {
		t.Fatal(err)
	}
	return len(records)
}

func testActiveRun(t *testing.T, f fixture, runID string) *activeRun {
	t.Helper()
	producer, err := kgo.NewClient(append(append([]kgo.Opt(nil), f.receiver.kafkaOpt...), kgo.TransactionalID(runID))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)
	return &activeRun{
		receiver: f.receiver,
		producer: producer,
		task:     &wire.Task{RunID: runID, ReplyTopic: "replies.test", CorrelationID: "corr"},
		taskHash: strings.Repeat("a", 64),
		attempt:  1,
		adapter:  "nomad",
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

func TestReceiverNoCredentialFailsRun(t *testing.T) {
	f, enrolled := admitFixture(t)
	record := signedRoot(t, f.userKey, nil)
	admitted, _ := f.receiver.admit(f.ctx, record, enrolled)
	status := f.receiver.execute(f.ctx, f.kafka, record, admitted, 1)
	if status != kgo.AckReject {
		t.Fatalf("status %v, want reject", status)
	}
	f.adapter.mu.Lock()
	dispatches, releases := f.adapter.dispatches, f.adapter.releases
	f.adapter.mu.Unlock()
	if dispatches != 0 || releases != 1 {
		t.Fatalf("dispatches=%d releases=%d, want 0 and 1", dispatches, releases)
	}
	records, err := readBounded(f.ctx, f.receiver.kafkaOpt, "replies.test")
	if err != nil || len(records) != 1 {
		t.Fatalf("reply records=%d err=%v", len(records), err)
	}
	envelope, err := wire.DecodeEnvelope(records[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := wire.Verify(envelope, f.receiver.signing.Public().(ed25519.PublicKey), wire.SignerReceiver)
	if err != nil {
		t.Fatal(err)
	}
	if got := verified.(*wire.Reply).Artifacts[0].Parts[0].Text; got != "credential unavailable" {
		t.Fatalf("reason %q", got)
	}
}

func TestReceiverReleasesAfterTerminal(t *testing.T) {
	t.Run("stop", func(t *testing.T) {
		f := newFixture(t, resultStream)
		run := testActiveRun(t, f, "run-release-stop")
		if status := run.finish(f.ctx, Handle{Adapter: "nomad", Alloc: "alloc"}, nil, agentResult{Result: "done"}); status != kgo.AckAccept {
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
		if status := f.receiver.execute(f.ctx, f.kafka, record, admitted, 1); status != kgo.AckReject {
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
	if admitted != nil || ack != kgo.AckReject || eventCount(t, f) != 0 {
		t.Fatalf("admitted=%v ack=%v events=%d", admitted != nil, ack, eventCount(t, f))
	}
}

func TestForeignSignedTaskRejectedSilently(t *testing.T) {
	f, enrolled := admitFixture(t)
	_, other, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	admitted, ack := f.receiver.admit(f.ctx, signedRoot(t, other, nil), enrolled)
	if admitted != nil || ack != kgo.AckReject || eventCount(t, f) != 0 {
		t.Fatalf("admitted=%v ack=%v", admitted != nil, ack)
	}
}

func TestTaskForOtherPoolSkippedSilently(t *testing.T) {
	f, enrolled := admitFixture(t)
	record := signedRoot(t, f.userKey, func(task *wire.Task) { task.Pool = "other" })
	admitted, ack := f.receiver.admit(f.ctx, record, enrolled)
	if admitted != nil || ack != kgo.AckAccept || eventCount(t, f) != 0 {
		t.Fatalf("admitted=%v ack=%v events=%d", admitted != nil, ack, eventCount(t, f))
	}
}

func TestAdmitRejections(t *testing.T) {
	cases := []struct {
		name   string
		reason string
		mutate func(*wire.Task)
	}{
		{"ExpiredQueueDeadlineRejected", "queue deadline passed", func(task *wire.Task) {
			task.QueueDeadlineMS = time.Now().Add(-time.Second).UnixMilli()
		}},
		{"ExpiredRunDeadlineRejected", "run deadline passed", func(task *wire.Task) {
			task.DeadlineMS = time.Now().Add(-time.Second).UnixMilli()
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
			if n := eventCount(t, f); n != 1 {
				t.Fatalf("want one signed REJECTED event, got %d", n)
			}
		})
	}
}

func TestAdmissionMismatchSkippedSilently(t *testing.T) {
	f, enrolled := admitFixture(t)
	record := signedRoot(t, f.userKey, func(task *wire.Task) {
		task.Selector[0].Values = []string{"docker"}
	})
	admitted, ack := f.receiver.admit(f.ctx, record, enrolled)
	if admitted != nil || ack != kgo.AckAccept || eventCount(t, f) != 0 {
		t.Fatalf("admitted=%v ack=%v events=%d", admitted != nil, ack, eventCount(t, f))
	}
}

func TestRejectionPublishFailureReleases(t *testing.T) {
	f, enrolled := admitFixture(t)
	f.receiver.base.Close()
	record := signedRoot(t, f.userKey, func(task *wire.Task) {
		task.QueueDeadlineMS = time.Now().Add(-time.Second).UnixMilli()
	})
	admitted, ack := f.receiver.admit(f.ctx, record, enrolled)
	if admitted != nil || ack != kgo.AckRelease {
		t.Fatalf("admitted=%v ack=%v, want nil and release", admitted != nil, ack)
	}
	if n := eventCount(t, f); n != 0 {
		t.Fatalf("failed rejection publish left %d events", n)
	}
}

func TestResultLineStopsAllocationBeforeTerminal(t *testing.T) {
	f, _ := admitFixture(t)
	run := testActiveRun(t, f, "run-result-stop")
	f.adapter.stopCheck = func() error {
		if n := topicCount(t, f, "replies.test"); n != 0 {
			return fmt.Errorf("reply visible before Stop: %d records", n)
		}
		if n := eventCount(t, f); n != 0 {
			return fmt.Errorf("terminal event visible before Stop: %d records", n)
		}
		return nil
	}
	status := run.finish(f.ctx, Handle{Adapter: "nomad", Alloc: "alloc"}, nil, agentResult{Result: "done"})
	if status != kgo.AckAccept {
		t.Fatalf("finish status %v, want accept", status)
	}
	f.adapter.mu.Lock()
	stops := f.adapter.stops
	f.adapter.mu.Unlock()
	if stops != 1 || topicCount(t, f, "replies.test") != 1 || eventCount(t, f) != 1 {
		t.Fatalf("stops=%d replies=%d events=%d", stops, topicCount(t, f, "replies.test"), eventCount(t, f))
	}
}

func TestStopFailureReleasesWithoutTerminal(t *testing.T) {
	for _, test := range []struct {
		name string
		call func(*activeRun, context.Context) kgo.AckStatus
	}{
		{"result", func(run *activeRun, ctx context.Context) kgo.AckStatus {
			return run.finish(ctx, Handle{Adapter: "nomad", Alloc: "alloc"}, nil, agentResult{Result: "done"})
		}},
		{"failure", func(run *activeRun, ctx context.Context) kgo.AckStatus {
			return run.stopAndFail(ctx, Handle{Adapter: "nomad", Alloc: "alloc"}, "timeout")
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, _ := admitFixture(t)
			f.adapter.stopErr = errors.New("stop failed")
			run := testActiveRun(t, f, "run-stop-failure-"+test.name)
			if status := test.call(run, f.ctx); status != kgo.AckRelease {
				t.Fatalf("status %v, want release", status)
			}
			if replies, events := topicCount(t, f, "replies.test"), eventCount(t, f); replies != 0 || events != 0 {
				t.Fatalf("Stop failure published replies=%d events=%d", replies, events)
			}
		})
	}
}

func TestAdoptionResumesFromLastOffset(t *testing.T) {
	f, _ := admitFixture(t)
	task := &wire.Task{RunID: "run-adopt", ReplyTopic: "replies.test", CorrelationID: "c"}
	heartbeat := func(offset int64) []byte {
		value, err := sign(wire.TypeEvent, f.receiver.event(task, strings.Repeat("a", 64), 1, "heartbeat", wire.TaskStateWorking, &offset, nil), f.receiver.signing)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	for _, offset := range []int64{10, 42} {
		if err := f.kafka.ProduceSync(f.ctx, &kgo.Record{Topic: eventTopic, Key: []byte(task.RunID), Value: heartbeat(offset)}).FirstErr(); err != nil {
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
	if err := aborted.ProduceSync(f.ctx, &kgo.Record{Topic: eventTopic, Key: []byte(task.RunID), Value: heartbeat(99)}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := aborted.EndTransaction(f.ctx, kgo.TryAbort); err != nil {
		t.Fatal(err)
	}
	got, err := f.receiver.lastOffset(f.ctx, task.RunID)
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
	task, err := trust.VerifyRootTask(record.Value, f.receiver.userPub)
	if err != nil {
		t.Fatal(err)
	}
	status := f.receiver.execute(f.ctx, f.kafka, record, &admission{task: task, offer: wire.Offer{Adapter: "nomad", Labels: map[string]string{"runtime": "exec"}}, prompt: "hi"}, 2)
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
	enrol := func(id string, signing ed25519.PrivateKey, sealing *ecdh.PrivateKey) {
		t.Helper()
		request, err := trust.NewEnrolRequest(id, signing, sealing, map[string]string{}, []wire.DetectedAdapter{{Name: "nomad", Platform: "nomad", Offers: []wire.Offer{}}}, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		approval, err := trust.Approve(request, f.userKey, map[string]string{}, "nomad", nomadGrants(), []wire.Requirement{}, "auto", 1, "", time.Now().Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		for _, value := range [][]byte{request, approval} {
			if err := f.kafka.ProduceSync(f.ctx, &kgo.Record{Topic: stateTopic, Key: []byte(id), Value: value}).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
	}
	enrol(f.receiver.config.ReceiverID, f.receiver.signing, f.receiver.sealing)
	_, peerSigning, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	peerSealing, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	enrol("receiver-peer", peerSigning, peerSealing)
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	task := &wire.Task{RunID: "run-peer", ReplyTopic: "replies.test", CorrelationID: "c"}
	offset := int64(42)
	value, err := sign(wire.TypeEvent, f.receiver.event(task, strings.Repeat("a", 64), 1, "heartbeat", wire.TaskStateWorking, &offset, nil), peerSigning)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.kafka.ProduceSync(f.ctx, &kgo.Record{Topic: eventTopic, Key: []byte(task.RunID), Value: value}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	got, err := f.receiver.lastOffset(f.ctx, task.RunID)
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
	reply := runToReply(t, f)
	if got := reply.Artifacts[0].Parts[0].Text; got != "log stream failed" {
		t.Fatalf("reply reason %q, want fixed reason", got)
	}
	logs := f.logs.String()
	if strings.Contains(logs, "secret upstream detail") || !strings.Contains(logs, "*errors.errorString") {
		t.Fatalf("unexpected follow error log: %q", logs)
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
	newRun := func(attempt int64) *activeRun {
		producer, err := kgo.NewClient(append(append([]kgo.Opt(nil), f.receiver.kafkaOpt...), kgo.TransactionalID("run-fence"))...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(producer.Close)
		task := &wire.Task{RunID: "run-fence", ReplyTopic: "replies.test", CorrelationID: "c"}
		return &activeRun{receiver: f.receiver, producer: producer, task: task, taskHash: strings.Repeat("a", 64), attempt: attempt}
	}
	zombie := newRun(1)
	if err := zombie.commit(f.ctx, zombie.eventRecord("claimed", wire.TaskStateWorking, nil, nil)); err != nil {
		t.Fatal(err)
	}
	adopter := newRun(2)
	if err := adopter.commit(f.ctx, adopter.eventRecord("claimed", wire.TaskStateWorking, nil, nil)); err != nil {
		t.Fatal(err)
	}
	err := zombie.commit(f.ctx, zombie.eventRecord("heartbeat", wire.TaskStateWorking, nil, nil))
	if err == nil {
		t.Fatal("zombie write succeeded after adoption")
	}
	if status := zombie.abandon(err); status != 0 {
		t.Fatalf("fenced run returned ack %v, want 0 (no acknowledgement)", status)
	}
	records, err := readBounded(f.ctx, f.receiver.kafkaOpt, eventTopic)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		envelope, _ := wire.DecodeEnvelope(record.Value)
		decoded, _ := wire.DecodePayload(wire.TypeEvent, envelope.Payload)
		if event := decoded.(*wire.Event); event.Kind == "heartbeat" {
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

func TestOversizeTranscriptLineBecomesPointer(t *testing.T) {
	f, _ := admitFixture(t)
	run := &activeRun{receiver: f.receiver, task: &wire.Task{RunID: "run-big"}, attempt: 1}
	record := run.transcriptRecord(Line{Offset: 7, Data: bytes.Repeat([]byte("x"), maxLine+1)})
	envelope, err := wire.DecodeEnvelope(record.Value)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := wire.DecodePayload(wire.TypeTranscript, envelope.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if transcript := decoded.(*wire.Transcript); transcript.Pointer == nil || transcript.Line != nil || transcript.Seq != 7 {
		t.Fatalf("got %#v", transcript)
	}
}

type multiFixture struct {
	fixture
	lab, cloud, edge *fakeAdapter
}

// newMultiFixture enrols a receiver hosting a nomad adapter and an aws adapter, each with its own
// credential, and a second nomad adapter with none.
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
	if err := f.kafka.ProduceSync(f.ctx, &kgo.Record{Topic: stateTopic, Key: []byte(f.receiver.config.ReceiverID), Value: request}).FirstErr(); err != nil {
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
	if _, err := client.PutSecret(f.ctx, "cred-lab", f.receiver.config.ReceiverID, tokenFile); err != nil {
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
	record := signedRoot(t, m.userKey, func(task *wire.Task) {
		task.RunID, task.RootRunID, task.Selector = runID, runID, selector
	})
	admitted, ack := m.receiver.admit(m.ctx, record, m.receiver.current())
	if admitted == nil {
		t.Fatalf("task %s not admitted (ack %v)", runID, ack)
	}
	return m.receiver.execute(m.ctx, m.kafka, record, admitted, attempt)
}

func runtimeIs(value string) []wire.Requirement {
	return []wire.Requirement{{Key: "runtime", Operator: wire.OpIn, Values: []string{value}}}
}

func lastReplyText(t *testing.T, f fixture) string {
	t.Helper()
	records, err := readBounded(f.ctx, f.receiver.kafkaOpt, "replies.test")
	if err != nil || len(records) == 0 {
		t.Fatalf("reply records=%d err=%v", len(records), err)
	}
	envelope, err := wire.DecodeEnvelope(records[len(records)-1].Value)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := wire.Verify(envelope, f.receiver.signing.Public().(ed25519.PublicKey), wire.SignerReceiver)
	if err != nil {
		t.Fatal(err)
	}
	return verified.(*wire.Reply).Artifacts[0].Parts[0].Text
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
	if status := m.execute(t, "run-cloud", runtimeIs("lambda"), 1); status != kgo.AckReject {
		t.Fatalf("cloud status %v, want reject", status)
	}
	if dispatches, _, _, releases := m.cloud.counts(); dispatches != 0 || releases != 1 {
		t.Fatalf("cloud dispatches=%d releases=%d, want 0 and 1", dispatches, releases)
	}
	if got := lastReplyText(t, m.fixture); got != "credential unavailable" {
		t.Fatalf("reason %q", got)
	}
	if strings.Contains(m.logs.String(), testToken) {
		t.Fatal("credential value appeared in receiver logs")
	}
}
