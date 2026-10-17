package client

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/trust"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRevocationTime(t *testing.T) {
	first, second := time.UnixMilli(1000), time.UnixMilli(2000)
	for name, test := range map[string]struct {
		revocations []revocationRecord
		generation  int64
		want        time.Time
		ok          bool
	}{
		"none":                  {nil, 1, time.Time{}, false},
		"floor below":           {[]revocationRecord{{floor: 1, timestampType: logAppendTime, timestamp: first}}, 2, time.Time{}, false},
		"first covering":        {[]revocationRecord{{floor: 1, timestampType: logAppendTime, timestamp: first}, {floor: 3, timestampType: logAppendTime, timestamp: second}}, 1, first, true},
		"skips lower floor":     {[]revocationRecord{{floor: 1, timestampType: logAppendTime, timestamp: first}, {floor: 3, timestampType: logAppendTime, timestamp: second}}, 2, second, true},
		"first covering is ct":  {[]revocationRecord{{floor: 2, timestampType: 0, timestamp: first}, {floor: 2, timestampType: logAppendTime, timestamp: second}}, 2, first, false},
		"no timestamp (v0 log)": {[]revocationRecord{{floor: 2, timestampType: -1, timestamp: first}}, 2, first, false},
	} {
		t.Run(name, func(t *testing.T) {
			got, ok := revocationTime(test.revocations, test.generation)
			if ok != test.ok || (ok && !got.Equal(test.want)) {
				t.Fatalf("revocationTime = %v, %v; want %v, %v", got, ok, test.want, test.ok)
			}
		})
	}
}

func TestAppendedBefore(t *testing.T) {
	revokedAt := time.UnixMilli(2000)
	for name, test := range map[string]struct {
		timestampType int8
		timestamp     time.Time
		want          bool
	}{
		"log append time before": {logAppendTime, time.UnixMilli(1999), true},
		"log append time equal":  {logAppendTime, time.UnixMilli(2000), false},
		"log append time after":  {logAppendTime, time.UnixMilli(2001), false},
		"create time before":     {0, time.UnixMilli(1999), false},
		"no timestamp":           {-1, time.UnixMilli(1999), false},
	} {
		t.Run(name, func(t *testing.T) {
			if got := appendedBefore(test.timestampType, test.timestamp, revokedAt); got != test.want {
				t.Fatalf("appendedBefore = %v, want %v", got, test.want)
			}
		})
	}
}

// brokerFixture is a client on the real broker named by WHERRY_KAFKA_TEST_BROKER. Each test signs with a fresh user
// key and uses its own receiver, run and reply topic names, so records left on the shared topics fail to verify or
// are keyed elsewhere.
type brokerFixture struct {
	clientFixture
	admin  *kadm.Client
	suffix string
}

func newBrokerFixture(t *testing.T) brokerFixture {
	t.Helper()
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" {
		t.Skip("WHERRY_KAFKA_TEST_BROKER not set")
	}
	kafka, err := kgo.NewClient(
		kgo.SeedBrokers(address),
		kgo.FetchMaxWait(10*time.Millisecond),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.KeepControlRecords(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kafka.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	admin := kadm.NewClient(kafka)
	if err := wire.ApplyTopics(ctx, admin, 1); err != nil {
		t.Fatal(err)
	}
	_, userKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	fixture := brokerFixture{
		clientFixture: clientFixture{
			ctx: ctx, kafka: kafka, client: New(kafka, userKey, ""),
			userPublicKey: userKey.Public().(ed25519.PublicKey), userKey: userKey,
		},
		admin: admin, suffix: suffix,
	}
	return fixture
}

// replyTopic creates a reply topic for this test, with LogAppendTime timestamps when logAppend is set.
func (fixture brokerFixture) replyTopic(t *testing.T, name string, logAppend bool) string {
	t.Helper()
	topic := "replies.test-" + name + "-" + fixture.suffix
	configs := map[string]*string{}
	if logAppend {
		value := "LogAppendTime"
		configs["message.timestamp.type"] = &value
	}
	created, err := fixture.admin.CreateTopic(fixture.ctx, 1, 1, configs, topic)
	if err != nil {
		t.Fatal(err)
	}
	if created.Err != nil {
		t.Fatal(created.Err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = fixture.admin.DeleteTopic(ctx, topic)
	})
	return topic
}

// clientFor returns a client of this fixture's user that awaits replies on replyTopic.
func (fixture brokerFixture) clientFor(replyTopic string) *Client {
	return New(fixture.kafka, fixture.userKey, replyTopic)
}

// produceAt produces records with a producer timestamp long before the test, so only a broker append time is recent.
func (fixture brokerFixture) produceAt(t *testing.T, records ...*kgo.Record) {
	t.Helper()
	for _, record := range records {
		record.Timestamp = time.UnixMilli(1000)
	}
	if err := fixture.kafka.ProduceSync(fixture.ctx, records...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// readBack returns a produced record as a consumer reads it, with the broker's timestamp type and timestamp.
func (fixture brokerFixture) readBack(t *testing.T, produced *kgo.Record) *kgo.Record {
	t.Helper()
	records, err := fixture.client.readBounded(fixture.ctx, produced.Topic)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if record.Partition == produced.Partition && record.Offset == produced.Offset {
			return record
		}
	}
	t.Fatalf("record %s/%d@%d not found", produced.Topic, produced.Partition, produced.Offset)
	return nil
}

// revocation returns the receiver's revocation record on receivers.state as a consumer reads it.
func (fixture brokerFixture) revocation(t *testing.T, receiverID string) *kgo.Record {
	t.Helper()
	records, err := fixture.client.readBounded(fixture.ctx, wire.TopicState)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		envelope, err := wire.DecodeEnvelope(record.Value)
		if err != nil || envelope.Type != wire.TypeRevocation || string(record.Key) != receiverID {
			continue
		}
		if _, err := wire.Verify(envelope, fixture.userPublicKey, wire.SignerUser); err == nil {
			return record
		}
	}
	t.Fatalf("no revocation for %s", receiverID)
	return nil
}

func receiverRecord(t *testing.T, topic, key string, recordType wire.RecordType, value any, signingKey ed25519.PrivateKey) *kgo.Record {
	t.Helper()
	return &kgo.Record{Topic: topic, Key: []byte(key), Value: signed(t, recordType, value, signingKey)}
}

func testChunk(submission *wire.SubmissionRecord) *wire.ArtifactUpdateRecord {
	return &wire.ArtifactUpdateRecord{
		ArtifactUpdate: wire.TaskArtifactUpdateEvent{TaskID: submission.Wherry.RunID, ContextID: submission.Wherry.Lineage.RootRunID, Artifact: wire.Artifact{
			ArtifactID: "transcript", Parts: []wire.Part{{Text: "line", MediaType: "application/x-ndjson"}}, Extensions: []string{wire.ExtensionURI},
		}},
		Wherry: wire.ArtifactUpdateExtension{Turn: submission.Wherry.Turn, Attempt: 1, Seq: 0},
	}
}

func TestHistoryVerifiesRevokedReceiverRecordsBeforeRevocation(t *testing.T) {
	fixture := newBrokerFixture(t)
	replies := fixture.replyTopic(t, "history", true)
	client := fixture.clientFor(replies)
	receiverID, pool, runID := "receiver-history-"+fixture.suffix, "pool-"+fixture.suffix, "run-history-"+fixture.suffix
	key := fixture.addReceiver(t, receiverID, pool, map[string]string{}, testSelector())
	submission, hash := fixture.rootSubmission(t, runID, pool)
	task := receiverRecord(t, replies, runID, wire.TypeTask, testTask(submission, hash, 1, wire.TaskStateCompleted), key)
	status := receiverRecord(t, wire.TopicEvent, runID, wire.TypeStatus, testStatus(submission, hash, receiverID, "terminal", wire.TaskStateCompleted), key)
	chunk := receiverRecord(t, wire.TopicTranscript, runID, wire.TypeArtifactUpdate, testChunk(submission), key)
	fixture.produceAt(t, task, status, chunk)
	if _, err := client.Revoke(fixture.ctx, receiverID); err != nil {
		t.Fatal(err)
	}
	revocation := fixture.revocation(t, receiverID)
	if revocation.Attrs.TimestampType() != logAppendTime {
		t.Fatalf("revocation timestamp type %d, want LogAppendTime", revocation.Attrs.TimestampType())
	}
	for _, produced := range []*kgo.Record{task, status, chunk} {
		read := fixture.readBack(t, produced)
		if read.Attrs.TimestampType() != logAppendTime || !read.Timestamp.Before(revocation.Timestamp) || read.Timestamp.Equal(time.UnixMilli(1000)) {
			t.Fatalf("%s record: timestamp type %d at %v, revocation at %v; want a broker append time before it",
				produced.Topic, read.Attrs.TimestampType(), read.Timestamp, revocation.Timestamp)
		}
	}

	awaited, err := client.Await(fixture.ctx, runID, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if awaited.Task == nil || awaited.Task.Status.State != wire.TaskStateCompleted || !awaited.RevokedAt.Equal(revocation.Timestamp) {
		t.Fatalf("await %#v, want the completed task marked revoked at %v", awaited, revocation.Timestamp)
	}
	events, err := client.Events(fixture.ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	position := TurnAttempt{Turn: 1, Attempt: 1}
	if len(events.Statuses) != 1 || events.Statuses[0].Wherry.Kind != "terminal" || !events.Statuses[0].RevokedAt.Equal(revocation.Timestamp) ||
		events.Chunks[position] != 1 || !events.ChunksRevokedAt[position].Equal(revocation.Timestamp) {
		t.Fatalf("events %#v, want one terminal status and one chunk marked revoked at %v", events, revocation.Timestamp)
	}
}

func TestHistoryRejectsRecordsAfterRevocation(t *testing.T) {
	t.Run("appended after", func(t *testing.T) {
		fixture := newBrokerFixture(t)
		replies := fixture.replyTopic(t, "after", true)
		client := fixture.clientFor(replies)
		receiverID, pool, runID := "receiver-after-"+fixture.suffix, "pool-"+fixture.suffix, "run-after-"+fixture.suffix
		key := fixture.addReceiver(t, receiverID, pool, map[string]string{}, testSelector())
		submission, hash := fixture.rootSubmission(t, runID, pool)
		if _, err := client.Revoke(fixture.ctx, receiverID); err != nil {
			t.Fatal(err)
		}
		task := receiverRecord(t, replies, runID, wire.TypeTask, testTask(submission, hash, 1, wire.TaskStateCompleted), key)
		status := receiverRecord(t, wire.TopicEvent, runID, wire.TypeStatus, testStatus(submission, hash, receiverID, "terminal", wire.TaskStateCompleted), key)
		chunk := receiverRecord(t, wire.TopicTranscript, runID, wire.TypeArtifactUpdate, testChunk(submission), key)
		fixture.produceAt(t, task, status, chunk)
		revocation := fixture.revocation(t, receiverID)
		if read := fixture.readBack(t, task); read.Attrs.TimestampType() != logAppendTime || read.Timestamp.Before(revocation.Timestamp) {
			t.Fatalf("task timestamp type %d at %v, revocation at %v; want a broker append time after it", read.Attrs.TimestampType(), read.Timestamp, revocation.Timestamp)
		}
		if awaited, err := client.Await(fixture.ctx, runID, time.Second); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("await = %#v, %v; want no accepted outcome", awaited, err)
		}
		events, err := client.Events(fixture.ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if len(events.Statuses) != 0 || len(events.Chunks) != 0 {
			t.Fatalf("events %#v, want no records from after the revocation", events)
		}
	})
	t.Run("appended at the revocation time", func(t *testing.T) {
		fixture := newBrokerFixture(t)
		replies := fixture.replyTopic(t, "equal", true)
		client := fixture.clientFor(replies)
		pool := "pool-" + fixture.suffix
		for try := range 50 {
			receiverID, runID := fmt.Sprintf("receiver-equal-%s-%d", fixture.suffix, try), fmt.Sprintf("run-equal-%s-%d", fixture.suffix, try)
			key := fixture.addReceiver(t, receiverID, pool, map[string]string{}, testSelector())
			submission, hash := fixture.rootSubmission(t, runID, pool)
			envelope, err := trust.NewRevocation(receiverID, 1, fixture.userKey, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			task := receiverRecord(t, replies, runID, wire.TypeTask, testTask(submission, hash, 1, wire.TaskStateCompleted), key)
			revocation := &kgo.Record{Topic: wire.TopicState, Key: []byte(receiverID), Value: envelope}
			fixture.produceAt(t, task, revocation)
			taskAt, revokedAt := fixture.readBack(t, task).Timestamp, fixture.readBack(t, revocation).Timestamp
			if !taskAt.Equal(revokedAt) {
				continue
			}
			t.Logf("try %d: task and revocation both appended at %v", try, revokedAt)
			if awaited, err := client.Await(fixture.ctx, runID, time.Second); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("try %d: await = %#v, %v; want a record appended at the revocation time %v rejected", try, awaited, err, revokedAt)
			}
			return
		}
		t.Fatal("no try appended the task and the revocation in the same millisecond")
	})
}

func TestHistoryRequiresLogAppendTime(t *testing.T) {
	fixture := newBrokerFixture(t)
	createTime, logAppend := fixture.replyTopic(t, "create-time", false), fixture.replyTopic(t, "log-append", true)
	receiverID, pool, runID := "receiver-create-time-"+fixture.suffix, "pool-"+fixture.suffix, "run-create-time-"+fixture.suffix
	key := fixture.addReceiver(t, receiverID, pool, map[string]string{}, testSelector())
	submission, hash := fixture.rootSubmission(t, runID, pool)
	value := signed(t, wire.TypeTask, testTask(submission, hash, 1, wire.TaskStateCompleted), key)
	onCreateTime := &kgo.Record{Topic: createTime, Key: []byte(runID), Value: value}
	onLogAppend := &kgo.Record{Topic: logAppend, Key: []byte(runID), Value: value}
	// Producer timestamps are now, so a CreateTime record also precedes the revocation.
	if err := fixture.kafka.ProduceSync(fixture.ctx, onCreateTime, onLogAppend).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.client.Revoke(fixture.ctx, receiverID); err != nil {
		t.Fatal(err)
	}
	revocation := fixture.revocation(t, receiverID)
	if read := fixture.readBack(t, onCreateTime); read.Attrs.TimestampType() != 0 || !read.Timestamp.Before(revocation.Timestamp) {
		t.Fatalf("default topic record: timestamp type %d at %v, revocation at %v; want CreateTime before it", read.Attrs.TimestampType(), read.Timestamp, revocation.Timestamp)
	}

	if awaited, err := fixture.clientFor(logAppend).Await(fixture.ctx, runID, 10*time.Second); err != nil || awaited.Task == nil || awaited.RevokedAt.IsZero() {
		t.Fatalf("LogAppendTime reply: await = %#v, %v; want the task accepted as history", awaited, err)
	}
	if awaited, err := fixture.clientFor(createTime).Await(fixture.ctx, runID, time.Second); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CreateTime reply: await = %#v, %v; want no accepted outcome", awaited, err)
	}
}

func TestNewWorkNeverTrustsRevokedReceiver(t *testing.T) {
	fixture := newBrokerFixture(t)
	replies := fixture.replyTopic(t, "new-work", true)
	client := fixture.clientFor(replies)
	fixture.client = client
	revokedID, liveID := "receiver-revoked-"+fixture.suffix, "receiver-live-"+fixture.suffix
	pool, runID := "pool-"+fixture.suffix, "run-new-work-"+fixture.suffix
	revokedKey := fixture.addReceiver(t, revokedID, pool, map[string]string{}, testSelector())
	fixture.addReceiver(t, liveID, pool, map[string]string{}, testSelector())
	submission, hash := fixture.rootSubmission(t, runID, pool)
	interrupted := testStatus(submission, hash, revokedID, "interrupted", wire.TaskStateAuthRequired)
	interrupted.StatusUpdate.Metadata = jsontext.Value(`{"credential":"token"}`)
	fixture.produceAt(t, receiverRecord(t, wire.TopicEvent, runID, wire.TypeStatus, interrupted, revokedKey))
	if _, err := client.Revoke(fixture.ctx, revokedID); err != nil {
		t.Fatal(err)
	}
	capability := &wire.Capability{
		ReceiverID: revokedID, Pool: pool, Labels: map[string]string{},
		Offers:   []wire.Offer{{Adapter: "lab", Labels: map[string]string{"runtime": "exec"}}},
		Selector: testSelector(), Capacity: 3, Generation: 1, AtMS: time.Now().UnixMilli(),
	}
	fixture.produceCapability(t, revokedID, capability, revokedKey)

	awaited, err := client.Await(fixture.ctx, runID, 10*time.Second)
	if err != nil || awaited.Status.State != wire.TaskStateAuthRequired || awaited.RevokedAt.IsZero() {
		t.Fatalf("await = %#v, %v; want the interrupted status accepted as history", awaited, err)
	}
	if _, err := client.Continue(fixture.ctx, runID, ContinueInput{Prompt: "again", Deadline: time.Minute, QueueDeadline: time.Minute}); err == nil || !strings.Contains(err.Error(), "not awaiting continuation") {
		t.Fatalf("Continue = %v, want refusal on a revoked receiver's interrupt", err)
	}
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	secret, err := client.PutSecret(fixture.ctx, "token", liveID, path, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(secret.Waiting) != 0 {
		t.Fatalf("waiting %#v, want none from a revoked receiver's interrupt", secret.Waiting)
	}
	if _, err := client.Submit(fixture.ctx, SubmitInput{
		Selector: []wire.Requirement{{Key: "runtime", Operator: wire.OpIn, Values: []string{"exec"}}},
		Labels:   map[string]string{"region": "eu"}, Prompt: "work", Deadline: time.Minute, QueueDeadline: time.Minute,
	}); err == nil || !strings.Contains(err.Error(), "no matching live receiver") {
		t.Fatalf("Submit = %v, want no route to the revoked receiver's capability", err)
	}
}
