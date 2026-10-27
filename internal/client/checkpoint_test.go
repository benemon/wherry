package client

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// hostAt returns a host of the fixture's host principal on stateDir, on a Kafka client that runs hook.
func (f *fixture) hostAt(t *testing.T, stateDir string, hook *feedHook) *Host {
	t.Helper()
	kafka, err := kgo.NewClient(append(f.host.kafka.Opts(), kgo.WithHooks(hook))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kafka.Close)
	host, err := NewHost(kafka, f.host.key, admin.TenantPrincipal("tenant-a", "host-a"), stateDir, f.host.credentials, f.host.git, f.host.logger)
	if err != nil {
		t.Fatal(err)
	}
	return host
}

// fullRead is a host on a copy of a host's local record of a run, whose checkpoint of the run is deleted before each use,
// so each answer is a read of the feed from the run's offset.
type fullRead struct {
	host *Host
	path string
}

func (f *fixture) fullRead(t *testing.T, from *Host, runID string) *fullRead {
	t.Helper()
	dir := t.TempDir()
	if local, err := from.localRun(runID); err != nil {
		t.Fatal(err)
	} else if local != nil {
		if err := writeState(filepath.Join(dir, "runs", runID+".json"), *local); err != nil {
			t.Fatal(err)
		}
	}
	host := f.hostAt(t, dir, &feedHook{})
	host.host, host.key = from.host, from.key
	path, _ := host.checkpointPath(runID)
	return &fullRead{host, path}
}

func (r *fullRead) fresh(t *testing.T) *Host {
	t.Helper()
	if err := os.Remove(r.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	return r.host
}

// answers is what a host answers about a run.
type answers struct {
	Status     StatusResult
	Events     EventsResult
	Transcript TranscriptResult
	Await      AwaitResult
}

func answered(t *testing.T, ctx context.Context, host *Host, runID string, await bool) answers {
	t.Helper()
	var got answers
	var err error
	// First, so await reads the checkpoint the last calls left.
	if await {
		if got.Await, err = host.Await(ctx, runID, 5*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if got.Status, err = host.Status(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if got.Events, err = host.Events(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if got.Transcript, err = host.Transcript(ctx, runID); err != nil {
		t.Fatal(err)
	}
	return got
}

func (f *fixture) checkpoint(t *testing.T, host *Host, runID string) feedCheckpoint {
	t.Helper()
	path, _ := host.checkpointPath(runID)
	var cp feedCheckpoint
	if err := readState(path, &cp); err != nil {
		t.Fatal(err)
	}
	return cp
}

func (f *fixture) unrelated(t *testing.T, receiver *testReceiver, n int) {
	t.Helper()
	records := make([]*kgo.Record, n)
	for i := range records {
		records[i] = &kgo.Record{Topic: admin.TenantTopic("tenant-a"), Key: []byte("run-other"), Value: []byte("unreadable")}
	}
	if err := receiver.kafka.ProduceSync(f.ctx, records...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// A host's checkpoint of a run has a fresh host on its state directory fetch only the records appended since.
func TestACheckpointedRunFetchesOnlyTheRecordsSince(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	if _, err := f.host.Status(f.ctx, runID); err != nil {
		t.Fatal(err)
	}
	f.unrelated(t, receiver, 7)
	hook := &feedHook{}
	if status, err := f.hostAt(t, f.stateDir, hook).Status(f.ctx, runID); err != nil || status.Status.State != wire.TaskStateWorking {
		t.Fatalf("status %#v, %v; want WORKING", status, err)
	}
	if fetched := hook.fetched.Load(); fetched != 7 {
		t.Fatalf("a fresh host fetched %d feed records; want the 7 appended since the checkpoint", fetched)
	}
}

// A checkpoint ends after the last record its read took, which a follower may have read past the committed end listed
// before it.
func TestACheckpointEndsAfterTheLastRecordRead(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	host := f.hostAt(t, f.stateDir, &feedHook{})
	follower := following(t, f, host)
	if _, err := host.Events(f.ctx, runID); err != nil {
		t.Fatal(err)
	}
	receiver.feed(t, f.ctx, runID, receiver.chunk(t, task, 1, 0))
	listed := committedEnd(t, f)
	receiver.feed(t, f.ctx, runID, receiver.chunk(t, task, 1, 1), receiver.chunk(t, task, 1, 2))
	for end := committedEnd(t, f); ; time.Sleep(10 * time.Millisecond) {
		if _, read, _, _ := follower.from(0); read >= end {
			break
		}
	}
	// The next listing of the feed's committed end answers the end between the two transactions.
	stale := false
	f.cluster.ControlKey(kmsg.ListOffsets.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		req := request.(*kmsg.ListOffsetsRequest)
		if stale || !slices.ContainsFunc(req.Topics, func(topic kmsg.ListOffsetsRequestTopic) bool { return topic.Topic == admin.TenantTopic("tenant-a") }) {
			f.cluster.KeepControl()
			return nil, nil, false
		}
		stale = true
		response := kmsg.NewPtrListOffsetsResponse()
		response.SetVersion(req.Version)
		for _, topic := range req.Topics {
			out := kmsg.ListOffsetsResponseTopic{Topic: topic.Topic}
			for _, partition := range topic.Partitions {
				out.Partitions = append(out.Partitions, kmsg.ListOffsetsResponseTopicPartition{Partition: partition.Partition, Offset: listed, Timestamp: -1, LeaderEpoch: -1})
			}
			response.Topics = append(response.Topics, out)
		}
		return response, nil, true
	})
	for range 2 {
		if events, err := host.Events(f.ctx, runID); err != nil || events.Chunks[1] != 3 {
			t.Fatalf("events %#v, %v; want the 3 chunks once", events, err)
		}
	}
}

// A checkpointed run's answers equal a full read's over every record kind, a foreign pool's echo, an unreadable record
// and a member ended between calls, for a host with the run's local record and one without.
func TestACheckpointAnswersAsAFullRead(t *testing.T) {
	f := newFixture(t)
	f.pool(t, "pool-b")
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	foreign := f.addReceiver(t, "receiver-b", "pool-b")
	peer := f.addReceiver(t, "receiver-c", "pool-a")
	other := task
	other.hash = strings.Repeat("0", 64)
	hosts := []*Host{f.hostAt(t, f.stateDir, &feedHook{}), f.newHost(t, "host-b", t.TempDir())}
	var full []*fullRead
	for _, host := range hosts {
		full = append(full, f.fullRead(t, host, runID))
	}
	compare := func(step string, await bool) {
		t.Helper()
		for i, host := range hosts {
			// The second call restores the checkpoint the first wrote.
			for range 2 {
				got, want := answered(t, f.ctx, host, runID, await), answered(t, f.ctx, full[i].fresh(t), runID, await)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("%s, host %s: checkpointed\n%#v\nwant the full read's\n%#v", step, host.host, got, want)
				}
			}
		}
	}
	receiver.feed(t, f.ctx, runID, receiver.chunk(t, task, 1, 0), receiver.chunk(t, task, 1, 1), receiver.status(t, task, 1, "started-1", "started", wire.TaskStateWorking, nil))
	peer.feed(t, f.ctx, runID, peer.chunk(t, task, 1, 2), peer.status(t, task, 1, "heartbeat-c", "heartbeat", wire.TaskStateWorking, nil))
	compare("first attempt", false)
	foreign.feed(t, f.ctx, runID, foreign.echo(t, task))
	receiver.produce(t, f.ctx, admin.TenantTopic("tenant-a"), runID, []byte("unreadable"))
	receiver.feed(t, f.ctx, runID, receiver.chunk(t, other, 1, 3), receiver.status(t, other, 1, "other-1", "started", wire.TaskStateWorking, nil))
	compare("foreign, unreadable and other-hash records", false)
	f.end(t, "receiver-c")
	receiver.feed(t, f.ctx, runID, receiver.status(t, task, 2, "claimed-2", "claimed", wire.TaskStateWorking, nil), receiver.chunk(t, task, 2, 0),
		receiver.chunk(t, task, 2, 1), receiver.outputChunk(t, task, 2, "report.md", 0, true, "# Report\n"))
	compare("member ended, second attempt", false)
	receiver.feed(t, f.ctx, runID, receiver.manifestResult(t, task, 2, []wire.ReturnedOutput{listed("report.md", "# Report\n")}, nil),
		receiver.status(t, task, 2, "terminal-2", "terminal", wire.TaskStateCompleted, nil))
	compare("ended", true)
	f.unrelated(t, receiver, 5)
	compare("ended, with later records", true)
}

// A run with a final checkpoint fetches no feed record for its status or events, however much follows it.
func TestAnEndedRunFetchesNoFeedRecord(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	receiver.feed(t, f.ctx, runID, receiver.result(t, task, 1, wire.TaskStateCompleted, nil), receiver.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateCompleted, nil))
	if _, err := f.host.Status(f.ctx, runID); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		later, laterTask := f.admitted(t)
		later.feed(t, f.ctx, laterTask.submission.Wherry.Run, later.chunk(t, laterTask, 1, 0), later.chunk(t, laterTask, 1, 1))
	}
	f.unrelated(t, receiver, 50)
	hook := &feedHook{}
	host := f.hostAt(t, f.stateDir, hook)
	if status, err := host.Status(f.ctx, runID); err != nil || status.Status.State != wire.TaskStateCompleted {
		t.Fatalf("status %#v, %v; want COMPLETED", status, err)
	}
	if events, err := host.Events(f.ctx, runID); err != nil || len(events.Statuses) != 2 {
		t.Fatalf("events %#v, %v; want the claim and the terminal", events, err)
	}
	if fetched := hook.fetched.Load(); fetched != 0 {
		t.Fatalf("an ended run's status and events fetched %d feed records; want none", fetched)
	}
}

// A recreated feed topic discards the checkpoint, whose offsets name records of the old topic.
func TestARecreatedFeedDiscardsTheCheckpoint(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	if _, err := f.host.Status(f.ctx, runID); err != nil {
		t.Fatal(err)
	}
	old := f.checkpoint(t, f.host, runID)
	topic := admin.TenantTopic("tenant-a")
	admins := kadm.NewClient(f.service.Admin)
	if deleted, err := admins.DeleteTopics(f.ctx, topic); err != nil || deleted.Error() != nil {
		t.Fatalf("delete the feed: %v, %v", err, deleted.Error())
	}
	// The entitlement creates the feed again, with its ACLs.
	if err := f.service.TenantAdd(f.ctx, "tenant-a", "pool-a"); err != nil {
		t.Fatal(err)
	}
	receiver.feed(t, f.ctx, runID, receiver.echo(t, task), receiver.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateCanceled, nil))
	if local, _ := f.host.localRun(runID); local.FeedOffset != 0 || old.Next <= 2 {
		t.Fatalf("feed offset %d, checkpoint next %d; want the new records below the old checkpoint's next", local.FeedOffset, old.Next)
	}
	// A fresh host, as each command builds: a client that read the old topic fails on the new one.
	if status, err := f.hostAt(t, f.stateDir, &feedHook{}).Status(f.ctx, runID); err != nil || status.Status.State != wire.TaskStateCanceled {
		t.Fatalf("status %#v, %v; want CANCELED from the recreated feed", status, err)
	}
	if cp := f.checkpoint(t, f.host, runID); cp.TopicID == old.TopicID || !cp.Final {
		t.Fatalf("checkpoint topic ID %s, final %v; want the new topic's, final", cp.TopicID, cp.Final)
	}
}

// lateReceiver is a receiver of pool-a that wherry.admin does not yet record; provision records it with its key.
func lateReceiver(t *testing.T, f *fixture, id string) (*testReceiver, func()) {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	receiver := &testReceiver{id: id, pool: "pool-a", key: key, kafka: f.service.Admin}
	return receiver, func() {
		t.Helper()
		config := admin.ReceiverConfig{Receiver: id, Pool: "pool-a", Capacity: 1, Profiles: []admin.ProfileSettings{{Name: "lab", Plugin: "nomad", Agent: "claude"}}}
		if err := f.service.ReceiverAdd(f.ctx, config, filepath.Join(t.TempDir(), "bundle.yaml")); err != nil {
			t.Fatal(err)
		}
		registration, err := signed.RegisterKey(id, key)
		if err != nil {
			t.Fatal(err)
		}
		receiver.produce(t, f.ctx, admin.ReceiverTopic(id), id, registration)
	}
}

// A change of the view a checkpoint was folded under discards it: a record unreadable then may read now.
func TestAViewChangeDiscardsTheCheckpoint(t *testing.T) {
	f := newFixture(t)
	_, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	late, provision := lateReceiver(t, f, "receiver-c")
	late.produce(t, f.ctx, admin.TenantTopic("tenant-a"), runID, late.status(t, task, 1, "started-c", "started", wire.TaskStateWorking, nil))
	full := f.fullRead(t, f.host, runID)
	if events, err := f.host.Events(f.ctx, runID); err != nil || events.Skipped != 1 {
		t.Fatalf("events %#v, %v; want the unprovisioned member's status unread", events, err)
	}
	provision()
	got, err := f.host.Events(f.ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if want, err := full.fresh(t).Events(f.ctx, runID); err != nil || !reflect.DeepEqual(got, want) || len(got.Statuses) != 2 {
		t.Fatalf("events after the view changed %#v; want the full read's %#v, %v, with both statuses", got, want, err)
	}
}

// A checkpoint ends at or below an open transaction, so the records after it are read once it ends.
func TestACheckpointEndsBelowAnOpenTransaction(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	producer, err := kgo.NewClient(append(receiver.kafka.Opts(), kgo.TransactionalID(admin.TransactionalPrefix("pool-a")+runID))...)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	open := committedEnd(t, f)
	if err := producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := producer.ProduceSync(f.ctx, &kgo.Record{Topic: admin.TenantTopic("tenant-a"), Key: []byte(runID), Value: receiver.chunk(t, task, 1, 0)}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	receiver.produce(t, f.ctx, admin.TenantTopic("tenant-a"), runID, receiver.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateCanceled, nil))
	if status, err := f.host.Status(f.ctx, runID); err != nil || status.Status.State != wire.TaskStateWorking {
		t.Fatalf("status %#v, %v; want WORKING below the open transaction", status, err)
	}
	if cp := f.checkpoint(t, f.host, runID); cp.Next > open {
		t.Fatalf("checkpoint next %d; want at most the open transaction's first offset %d", cp.Next, open)
	}
	if err := producer.EndTransaction(f.ctx, kgo.TryAbort); err != nil {
		t.Fatal(err)
	}
	if status, err := f.host.Status(f.ctx, runID); err != nil || status.Status.State != wire.TaskStateCanceled {
		t.Fatalf("status %#v, %v; want CANCELED from the record after the aborted transaction", status, err)
	}
}

// Divergence 5.2, accepted for the spike: a checkpoint keeps answering from records retention has deleted, where a full
// read no longer sees them.
func TestDivergenceTruncationKeepsTheDeletedRecordsAnswer(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	receiver.feed(t, f.ctx, runID, receiver.status(t, task, 1, "started-1", "started", wire.TaskStateWorking, nil))
	before, err := f.host.Status(f.ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	full := f.fullRead(t, f.host, runID)
	var deleted kadm.Offsets
	deleted.AddOffset(admin.TenantTopic("tenant-a"), 0, f.checkpoint(t, f.host, runID).Next, -1)
	if responses, err := kadm.NewClient(f.service.Admin).DeleteRecords(f.ctx, deleted); err != nil || responses.Error() != nil {
		t.Fatalf("delete records: %v, %v", err, responses.Error())
	}
	if after, err := f.host.Status(f.ctx, runID); err != nil || !reflect.DeepEqual(after, before) {
		t.Fatalf("status after truncation %#v, %v; want the one before %#v", after, err, before)
	}
	if truncated, err := full.fresh(t).Status(f.ctx, runID); err != nil || truncated.Status.State != wire.TaskStateSubmitted {
		t.Fatalf("full read after truncation %#v, %v; want SUBMITTED from no record", truncated, err)
	}
}

// Divergence 5.1, accepted for the spike: a final checkpoint stops reading its run, so a later record from a misbehaving
// receiver never counts, where a full read takes its higher attempt.
func TestDivergenceFinalStopsReadingAnEndedRun(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	receiver.feed(t, f.ctx, runID, receiver.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateCanceled, nil))
	if status, err := f.host.Status(f.ctx, runID); err != nil || status.Status.State != wire.TaskStateCanceled || !f.checkpoint(t, f.host, runID).Final {
		t.Fatalf("status %#v, %v; want CANCELED with a final checkpoint", status, err)
	}
	full := f.fullRead(t, f.host, runID)
	receiver.feed(t, f.ctx, runID, receiver.status(t, task, 2, "terminal-2", "terminal", wire.TaskStateCompleted, nil))
	if status, err := f.host.Status(f.ctx, runID); err != nil || status.Status.State != wire.TaskStateCanceled {
		t.Fatalf("status %#v, %v; want the checkpoint's CANCELED", status, err)
	}
	if status, err := full.fresh(t).Status(f.ctx, runID); err != nil || status.Status.State != wire.TaskStateCompleted {
		t.Fatalf("full read %#v, %v; want the later attempt's COMPLETED", status, err)
	}
}

// A checkpoint of another version, with an unknown field or that does not decode is rebuilt without an error.
func TestAnUnreadableCheckpointIsRebuilt(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	receiver.feed(t, f.ctx, runID, receiver.chunk(t, task, 1, 0), receiver.status(t, task, 1, "started-1", "started", wire.TaskStateWorking, nil))
	full := f.fullRead(t, f.host, runID)
	want, err := full.fresh(t).Events(f.ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.host.Events(f.ctx, runID); err != nil {
		t.Fatal(err)
	}
	path, _ := f.host.checkpointPath(runID)
	valid, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Read as a checkpoint, a next of 0 would count every record twice.
	rewound := regexp.MustCompile(`"next":[0-9]+`).ReplaceAllString(string(valid), `"next":0`)
	for name, content := range map[string]string{
		"next version":  strings.Replace(rewound, `"v":1`, `"v":2`, 1),
		"unknown field": strings.Replace(rewound, `{`, `{"extra":1,`, 1),
		"garbage":       "{not json",
	} {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if got, err := f.host.Events(f.ctx, runID); err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: events %#v, %v; want the full read's %#v", name, got, err, want)
		}
		if cp := f.checkpoint(t, f.host, runID); cp.V != checkpointVersion {
			t.Fatalf("%s: rebuilt checkpoint version %d", name, cp.V)
		}
	}
}

// Two hosts reading one run on one state directory as it runs both answer correctly, and the checkpoint always decodes.
func TestHostsSharingACheckpointAnswerCorrectly(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	path, _ := f.host.checkpointPath(runID)
	var wg sync.WaitGroup
	done := make(chan struct{})
	for range 2 {
		host := f.hostAt(t, f.stateDir, &feedHook{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				if _, err := host.Events(f.ctx, runID); err != nil {
					t.Errorf("events: %v", err)
					return
				}
				var cp feedCheckpoint
				if err := readState(path, &cp); err != nil && !errors.Is(err, os.ErrNotExist) {
					t.Errorf("checkpoint: %v", err)
					return
				}
			}
		}()
	}
	for seq := range int64(20) {
		receiver.feed(t, f.ctx, runID, receiver.chunk(t, task, 1, seq))
	}
	close(done)
	wg.Wait()
	full := f.fullRead(t, f.host, runID)
	want, err := full.fresh(t).Events(f.ctx, runID)
	if err != nil || want.Chunks[1] != 20 {
		t.Fatalf("full read %#v, %v; want 20 chunks", want, err)
	}
	for range 2 {
		if got, err := f.hostAt(t, f.stateDir, &feedHook{}).Events(f.ctx, runID); err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("events %#v, %v; want the full read's %#v", got, err, want)
		}
	}
}
