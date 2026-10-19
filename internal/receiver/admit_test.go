package receiver

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json/jsontext"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func (f fixture) rejected(t *testing.T, record *kgo.Record, reason string) {
	t.Helper()
	if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckReject {
		t.Fatalf("admitted=%v ack=%v, want rejection %q", admitted != nil, ack, reason)
	}
	if !strings.Contains(f.logs.String(), reason) {
		t.Fatalf("fixed rejection reason %q missing from %q", reason, f.logs.String())
	}
}

func TestFirstTurnPromptUnchanged(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.submit(t, f.host, func(task *wire.Submission) {
		task.Message.Parts = []wire.Part{{Text: "line one"}, {Text: "--- turn 1 ---"}, {Text: "line three\n"}}
	})
	if got := prompt(f.admitted(t, record).history); got != "line one\n--- turn 1 ---\nline three\n" {
		t.Fatalf("prompt %q", got)
	}
}

func TestAdmitAcceptsValidTask(t *testing.T) {
	f := newFixture(t, resultStream)
	admitted := f.admitted(t, f.submit(t, f.host, nil))
	if admitted.offer.Adapter != "nomad" || admitted.offer.Labels["runtime"] != "exec" || admitted.offer.Labels["agent"] != "claude" || prompt(admitted.history) != "hi" {
		t.Fatalf("got %#v", admitted)
	}
}

func TestCommittedTerminalAcknowledgedWithoutWrites(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.submit(t, f.host, nil)
	f.publishStatus(t, record, testReceiver, f.receiver.signing, "terminal", wire.TaskStateCompleted, nil)
	before := len(f.records(t, admin.PoolEventsTopic(testPool)))
	if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckAccept {
		t.Fatalf("admitted=%v ack=%v, want accept", admitted != nil, ack)
	}
	if after := len(f.records(t, admin.PoolEventsTopic(testPool))); after != before || len(f.records(t, admin.TenantTopic(testTenant))) != 0 {
		t.Fatalf("a task with a committed terminal published records: events %d -> %d", before, after)
	}
	// A terminal from outside the pool does not count.
	other := f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Run, task.Message.TaskID, task.Message.ContextID = "run-b", "run-b", "run-b"
	})
	_, foreign, _ := ed25519.GenerateKey(rand.Reader)
	f.publishStatus(t, other, testReceiver, foreign, "terminal", wire.TaskStateCompleted, nil)
	if admitted, _ := f.receiver.admit(f.ctx, other); admitted == nil {
		t.Fatal("an unauthorised terminal stopped the task")
	}
}

func TestUnsignedTaskRejectedSilently(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.produce(t, admin.PoolTasksTopic(testPool), "run-a", []byte("acl-probe"))
	if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckReject || len(f.records(t, admin.PoolEventsTopic(testPool))) != 0 {
		t.Fatalf("admitted=%v ack=%v", admitted != nil, ack)
	}
}

func TestForeignSignedTaskRejectedSilently(t *testing.T) {
	f := newFixture(t, resultStream)
	_, other, _ := ed25519.GenerateKey(rand.Reader)
	if admitted, ack := f.receiver.admit(f.ctx, f.submit(t, other, nil)); admitted != nil || ack != kgo.AckReject || len(f.records(t, admin.PoolEventsTopic(testPool))) != 0 {
		t.Fatalf("admitted=%v ack=%v", admitted != nil, ack)
	}
	// A record keyed by another run than it names is not admitted either.
	misfiled := f.submit(t, f.host, nil)
	misfiled.Key = []byte("run-other")
	if admitted, ack := f.receiver.admit(f.ctx, misfiled); admitted != nil || ack != kgo.AckReject {
		t.Fatalf("misfiled admitted=%v ack=%v", admitted != nil, ack)
	}
}

func TestHostKeyReReadOnUnknownKeyID(t *testing.T) {
	f := newFixture(t, resultStream)
	_, next, _ := ed25519.GenerateKey(rand.Reader)
	owner := signed.HostOwner(testTenant, testHost)
	f.produce(t, admin.HostTopic(testTenant, testHost), "", must(t)(signed.RotateKey(owner, f.host, next, nil)))
	// The receiver's last refresh holds the old key; the task is signed with the new one.
	if admitted, ack := f.receiver.admit(f.ctx, f.submit(t, next, nil)); admitted == nil {
		t.Fatalf("task signed by the host's rotated key was not admitted: ack %v", ack)
	}
}

func TestAdmitRejections(t *testing.T) {
	for _, c := range []struct {
		name   string
		reason string
		mutate func(*wire.Submission)
	}{
		{"ExpiredQueueDeadline", "queue deadline passed", func(task *wire.Submission) {
			task.Wherry.QueueDeadlineMS = time.Now().Add(-time.Second).UnixMilli()
		}},
		{"ExpiredRunDeadline", "run deadline passed", func(task *wire.Submission) {
			task.Wherry.DeadlineMS = time.Now().Add(-time.Second).UnixMilli()
		}},
		{"AgentOutsideTerms", "placement is outside the pool terms", func(task *wire.Submission) { task.Wherry.Placement.Agent = "gemini" }},
		{"RuntimeOutsideTerms", "placement is outside the pool terms", func(task *wire.Submission) { task.Wherry.Placement.Runtime = "wasm" }},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.rejected(t, f.submit(t, f.host, c.mutate), c.reason)
			events, feed := f.eventStatuses(t), f.feedStatuses(t)
			if len(events) != 1 || len(feed) != 1 || events[0].Wherry.EventID != feed[0].Wherry.EventID || feed[0].Wherry.Kind != "rejected" || feed[0].StatusUpdate.Status.State != wire.TaskStateRejected {
				t.Fatalf("want one REJECTED status on the pool's events and the feed, got %d and %d", len(events), len(feed))
			}
		})
	}
}

func TestPlacementOutsideOffersIsStartFailure(t *testing.T) {
	f := newFixture(t, resultStream)
	for name, mutate := range map[string]func(*wire.Submission){
		"agent":  func(task *wire.Submission) { task.Wherry.Placement.Agent = "codex" },
		"labels": func(task *wire.Submission) { task.Wherry.Placement.Labels = map[string]string{"zone": "dc9"} },
	} {
		t.Run(name, func(t *testing.T) {
			before := len(f.records(t, admin.PoolEventsTopic(testPool)))
			record := f.submit(t, f.host, mutate)
			if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckRelease {
				t.Fatalf("admitted=%v ack=%v, want release", admitted != nil, ack)
			}
			records := f.records(t, admin.PoolEventsTopic(testPool))
			if len(records) != before+1 {
				t.Fatalf("pool events %d -> %d, want one start failure", before, len(records))
			}
			var failure wire.StartFailure
			if err := wire.ParseRecord(records[len(records)-1].Value, f.keys(), &failure); err != nil {
				t.Fatal(err)
			}
			placement := f.parse(t, record).Wherry.Placement
			if failure.Class != "offer-unavailable" || failure.Agent != placement.Agent || failure.Runtime != placement.Runtime || failure.Wherry.Run != "run-a" {
				t.Fatalf("start failure %#v", failure)
			}
		})
	}
	if len(f.feedStatuses(t)) != 0 {
		t.Fatal("a start failure wrote a status to the feed")
	}
}

func TestPlacementLabelsSelectAnOffer(t *testing.T) {
	f := newFixture(t, resultStream)
	admitted := f.admitted(t, f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Placement.Runtime = "podman"
		task.Wherry.Placement.Labels = map[string]string{"nomad.node_pool": "default"}
	}))
	if admitted.offer.Labels["runtime"] != "podman" || admitted.offer.Labels["nomad.node_pool"] != "default" {
		t.Fatalf("offer %#v", admitted.offer)
	}
}

func TestContinuationRejectedWhenNotInterrupted(t *testing.T) {
	for name, state := range map[string]wire.TaskState{"running": wire.TaskStateWorking, "completed": wire.TaskStateCompleted} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			first := f.turn(t, "run-not-interrupted", 1)
			kind := "claimed"
			if wire.Terminal(state) {
				kind = "terminal"
			}
			f.publishStatus(t, first, testReceiver, f.receiver.signing, kind, state, nil)
			f.rejected(t, f.turn(t, "run-not-interrupted", 2), "task is not awaiting continuation")
		})
	}
}

func TestStaleSubmissionRejected(t *testing.T) {
	t.Run("earlier turn after later turn started", func(t *testing.T) {
		f := newFixture(t, resultStream)
		first := f.turn(t, "run-earlier-turn", 1)
		f.publishStatus(t, first, testReceiver, f.receiver.signing, "interrupted", wire.TaskStateAuthRequired, nil)
		second := f.turn(t, "run-earlier-turn", 2)
		f.publishStatus(t, second, testReceiver, f.receiver.signing, "started", wire.TaskStateWorking, nil)
		f.rejected(t, first, "stale submission")
	})
	t.Run("turn after its interruption", func(t *testing.T) {
		f := newFixture(t, resultStream)
		first := f.turn(t, "run-interrupted-replay", 1)
		f.publishStatus(t, first, testReceiver, f.receiver.signing, "interrupted", wire.TaskStateAuthRequired, nil)
		f.rejected(t, first, "stale submission")
	})
	t.Run("active turn redelivery adopts", func(t *testing.T) {
		f := newFixture(t, resultStream)
		first := f.turn(t, "run-active-redelivery", 1)
		f.publishStatus(t, first, testReceiver, f.receiver.signing, "heartbeat", wire.TaskStateWorking, nil)
		f.admitted(t, first)
	})
}

func TestDuplicateContinuationRejected(t *testing.T) {
	f := newFixture(t, resultStream)
	first := f.turn(t, "run-duplicate", 1)
	f.publishStatus(t, first, testReceiver, f.receiver.signing, "interrupted", wire.TaskStateAuthRequired, nil)
	accepted := f.turn(t, "run-duplicate", 2)
	duplicate := f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Run, task.Wherry.Turn = "run-duplicate", 2
		task.Message.MessageID, task.Message.TaskID, task.Message.ContextID = "msg-duplicate", "run-duplicate", "run-duplicate"
	})
	f.admitted(t, accepted)
	f.rejected(t, duplicate, "duplicate continuation")
}

func TestDuplicateSubmissionRejected(t *testing.T) {
	f := newFixture(t, resultStream)
	accepted := f.turn(t, "run-duplicate-root", 1)
	duplicate := f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Run = "run-duplicate-root"
		task.Message.MessageID, task.Message.TaskID, task.Message.ContextID = "msg-duplicate-root", "run-duplicate-root", "run-duplicate-root"
	})
	f.admitted(t, accepted)
	f.rejected(t, duplicate, "duplicate submission")
}

func TestRejectedContinuationKeepsTaskResumable(t *testing.T) {
	f := newFixture(t, resultStream)
	first := f.turn(t, "run-rejected-continuation", 1)
	f.publishStatus(t, first, testReceiver, f.receiver.signing, "interrupted", wire.TaskStateAuthRequired, nil)
	f.rejected(t, f.turn(t, "run-rejected-continuation", 3), "continuation turn out of order")
	f.admitted(t, f.turn(t, "run-rejected-continuation", 2))
	statuses := f.feedStatuses(t)
	if got := statuses[len(statuses)-1]; got.Wherry.Kind != "rejected" || got.StatusUpdate.Status.State != wire.TaskStateRejected {
		t.Fatalf("rejection status %#v", got)
	}
}

func TestContinuationRejectedFromOtherPoolStatus(t *testing.T) {
	f := newFixture(t, resultStream)
	if err := f.service.PoolCreate(f.ctx, "pool-b", testTerms()); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantAdd(f.ctx, testTenant, "pool-b"); err != nil {
		t.Fatal(err)
	}
	otherKey, _ := f.addPeer(t, "receiver-other-pool", "pool-b")
	first := f.turn(t, "run-other-pool-status", 1)
	f.publishStatus(t, first, "receiver-other-pool", otherKey, "interrupted", wire.TaskStateAuthRequired, nil)
	f.rejected(t, f.turn(t, "run-other-pool-status", 2), "task is not awaiting continuation")
}

func TestContinuationAcceptsPeerInterruption(t *testing.T) {
	f := newFixture(t, resultStream)
	peerKey, _ := f.addPeer(t, "receiver-b", testPool)
	first := f.turn(t, "run-peer-interrupted", 1)
	f.publishStatus(t, first, "receiver-b", peerKey, "interrupted", wire.TaskStateAuthRequired, nil)
	f.admitted(t, f.turn(t, "run-peer-interrupted", 2))
}

func TestContinuationStatusBindings(t *testing.T) {
	for name, mutate := range map[string]func(*wire.RunStatus){
		"task hash": func(status *wire.RunStatus) { status.Wherry.TaskHash = strings.Repeat("d", 64) },
		"kind":      func(status *wire.RunStatus) { status.Wherry.Kind = "claimed" },
		"tenant":    func(status *wire.RunStatus) { status.Wherry.Tenant = "tenant-b" },
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			if err := f.service.TenantAdd(f.ctx, "tenant-b", testPool); err != nil {
				t.Fatal(err)
			}
			if err := f.receiver.refresh(f.ctx); err != nil {
				t.Fatal(err)
			}
			first := f.turn(t, "run-status-binding", 1)
			f.publishStatus(t, first, testReceiver, f.receiver.signing, "interrupted", wire.TaskStateAuthRequired, mutate)
			if admitted, ack := f.receiver.admit(f.ctx, f.turn(t, "run-status-binding", 2)); admitted != nil || ack != kgo.AckReject {
				t.Fatalf("admitted=%v ack=%v, want rejection", admitted != nil, ack)
			}
		})
	}
}

func TestContinuationStatusOrdering(t *testing.T) {
	f := newFixture(t, resultStream)
	first := f.turn(t, "run-status-order", 1)
	f.publishStatus(t, first, testReceiver, f.receiver.signing, "interrupted", wire.TaskStateAuthRequired, func(status *wire.RunStatus) { status.Wherry.Attempt = 2 })
	f.publishStatus(t, first, testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
	f.admitted(t, f.turn(t, "run-status-order", 2))
}

func TestBindTurnsRequireSameBindings(t *testing.T) {
	for name, mutate := range map[string]func(*wire.Submission){
		"tenant":  func(task *wire.Submission) { task.Wherry.Tenant = "tenant-b" },
		"agent":   func(task *wire.Submission) { task.Wherry.Placement.Agent = "codex" },
		"runtime": func(task *wire.Submission) { task.Wherry.Placement.Runtime = "podman" },
		"labels":  func(task *wire.Submission) { task.Wherry.Placement.Labels = map[string]string{"zone": "dc1"} },
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.turn(t, "run-submission-binding", 1)
			all, err := f.receiver.readTurns(f.ctx, "run-submission-binding")
			if err != nil || len(all) != 1 {
				t.Fatalf("turns %d err %v", len(all), err)
			}
			expected := testSubmission("run-submission-binding")
			mutate(expected)
			if bound := bindTurns(all, expected); len(bound) != 0 {
				t.Fatalf("mismatched %s was accepted", name)
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
			f := newFixture(t, resultStream)
			f.sealOwn(t, credentialName, time.Hour)
			f.submit(t, f.host, func(task *wire.Submission) { task.Message.Parts = append(task.Message.Parts, part) })
			if err := f.receiver.claimOne(f.ctx); err != nil {
				t.Fatal(err)
			}
			if dispatches, _, _, _ := f.adapter.counts(); dispatches != 0 {
				t.Fatalf("dispatched %d runs", dispatches)
			}
			statuses := f.feedStatuses(t)
			if len(statuses) != 1 {
				t.Fatalf("feed statuses=%d, want one", len(statuses))
			}
			got := statuses[0].StatusUpdate.Status
			if got.State != wire.TaskStateRejected || statuses[0].Wherry.Kind != "rejected" || got.Message == nil || got.Message.Parts[0].Text != "unsupported message part" {
				t.Fatalf("status %#v", statuses[0])
			}
		})
	}
}

func TestRejectionPublishFailureReleases(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.QueueDeadlineMS = time.Now().Add(-time.Second).UnixMilli()
	})
	f.cluster.ControlKey(kmsg.Produce.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		req := request.(*kmsg.ProduceRequest)
		response := req.ResponseKind().(*kmsg.ProduceResponse)
		response.SetVersion(req.Version)
		for _, topic := range req.Topics {
			out := kmsg.NewProduceResponseTopic()
			out.Topic, out.TopicID = topic.Topic, topic.TopicID
			for _, partition := range topic.Partitions {
				result := kmsg.NewProduceResponseTopicPartition()
				result.Partition = partition.Partition
				result.ErrorCode = kerr.InvalidRecord.Code
				out.Partitions = append(out.Partitions, result)
			}
			response.Topics = append(response.Topics, out)
		}
		return response, nil, true
	})
	if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckRelease {
		t.Fatalf("admitted=%v ack=%v, want nil and release", admitted != nil, ack)
	}
	if n := len(f.eventStatuses(t)); n != 0 {
		t.Fatalf("failed rejection publish left %d events", n)
	}
}

// gitPart is a git context part as wherry submit builds it.
var gitPart = wire.Part{URL: "https://git.example.test/org/repo.git", MediaType: wire.GitMediaType, Metadata: jsontext.Value(`{"ref":"main"}`)}

func withGitPart(task *wire.Submission) {
	task.Message.Parts = append(task.Message.Parts, gitPart)
}

func TestGitPartAdmittedOnlyWithGitAdapter(t *testing.T) {
	for name, test := range map[string]struct {
		git    bool
		mutate func(*wire.Submission)
		admit  bool
	}{
		"git adapter":         {true, withGitPart, true},
		"adapter without git": {false, withGitPart, false},
		"non-git url": {true, func(task *wire.Submission) {
			task.Message.Parts = append(task.Message.Parts, wire.Part{URL: "https://example.test/a"})
		}, false},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.receiver.adapters[0].Git = test.git
			f.sealOwn(t, credentialName, time.Hour)
			record := f.submit(t, f.host, test.mutate)
			if !test.admit {
				f.rejected(t, record, "unsupported message part")
				return
			}
			if status := f.execute(t, record, 1); status != kgo.AckAccept {
				t.Fatalf("status %v", status)
			}
			want := wire.GitContext{URL: gitPart.URL, Ref: "main", Path: "repo"}
			if run := f.adapter.lastRun; run.Git == nil || *run.Git != want || string(run.Prompt) != "hi" {
				t.Fatalf("dispatched git %#v prompt %q, want %#v and the text alone", run.Git, run.Prompt, want)
			}
		})
	}
}

func TestCancelledSubmissionNotDispatched(t *testing.T) {
	for _, test := range []struct {
		name    string
		attempt int64
		running bool
		stops   int
	}{
		{"queued", 1, false, 0},
		{"redelivered with its job running", 2, true, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.sealOwn(t, credentialName, time.Hour)
			f.adapter.existing = test.running
			record := f.submit(t, f.host, nil)
			f.publishCancel(t, "run-a", 1, f.host, nil)
			if status := f.execute(t, record, test.attempt); status != kgo.AckAccept {
				t.Fatalf("status %v, want accept", status)
			}
			f.adapter.mu.Lock()
			dispatches, stops, releaseIDs := f.adapter.dispatches, f.adapter.stops, f.adapter.releaseIDs
			f.adapter.mu.Unlock()
			if dispatches != 0 || stops != test.stops || !slices.Equal(releaseIDs, []string{"run-a"}) {
				t.Fatalf("dispatches=%d stops=%d releases=%v, want 0, %d and run-a", dispatches, stops, releaseIDs, test.stops)
			}
			if task := f.lastTask(t); task.ID != "run-a" || task.Status.State != wire.TaskStateCanceled {
				t.Fatalf("task %#v, want CANCELED", task)
			}
			if state, reason := terminalReason(t, f, "run-a"); state != wire.TaskStateCanceled || reason != "canceled by user" {
				t.Fatalf("terminal %s %q", state, reason)
			}
		})
	}
}

func TestCancelSinceLastReadStopsDispatch(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.submit(t, f.host, nil)
	f.produce(t, admin.PoolEventsTopic(testPool), "run-a", must(t)(wire.SignRecord(&wire.Cancel{Tenant: testTenant, Host: testHost, Pool: testPool, Run: "run-a", Turn: 1}, f.host)))
	if admitted := f.admitted(t, record); !admitted.cancelled {
		t.Fatalf("admission %#v, want the run admitted for cancellation", admitted)
	}
}

func TestCancelMustBeHostSignedForTheRunsTenant(t *testing.T) {
	f := newFixture(t, resultStream)
	if err := f.service.TenantAdd(f.ctx, "tenant-b", testPool); err != nil {
		t.Fatal(err)
	}
	if err := f.service.HostAdd(f.ctx, "tenant-b", "host-b", t.TempDir()+"/host-b.yaml"); err != nil {
		t.Fatal(err)
	}
	_, otherHost, _ := ed25519.GenerateKey(rand.Reader)
	f.produce(t, admin.HostTopic("tenant-b", "host-b"), "", must(t)(signed.RegisterKey(signed.HostOwner("tenant-b", "host-b"), otherHost, nil)))
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	record := f.submit(t, f.host, nil)
	f.publishStatus(t, record, testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
	_, foreign, _ := ed25519.GenerateKey(rand.Reader)
	for name, test := range map[string]struct {
		key    ed25519.PrivateKey
		mutate func(*wire.Cancel)
	}{
		"receiver key":          {f.receiver.signing, nil},
		"unregistered key":      {foreign, nil},
		"another tenant's host": {otherHost, func(cancel *wire.Cancel) { cancel.Tenant, cancel.Host = "tenant-b", "host-b" }},
	} {
		f.publishCancel(t, "run-a", 1, test.key, test.mutate)
		if _, cancelled := f.receiver.cancelled["run-a"]; cancelled {
			t.Fatalf("receiver honoured a cancel signed by the %s", name)
		}
	}
	f.publishCancel(t, "run-a", 1, f.host, nil)
	if _, cancelled := f.receiver.cancelled["run-a"]; !cancelled {
		t.Fatal("receiver ignored the host's cancel")
	}
}

func terminalReason(t *testing.T, f fixture, run string) (wire.TaskState, string) {
	t.Helper()
	for _, status := range f.feedStatuses(t) {
		if status.Wherry.Run == run && status.Wherry.Kind == "terminal" {
			return status.StatusUpdate.Status.State, status.StatusUpdate.Status.Message.Parts[0].Text
		}
	}
	t.Fatal("no terminal status on the feed")
	return "", ""
}

// startInFlight claims a submitted run with the receiver and returns once it is following its job.
func startInFlight(t *testing.T, f fixture, ctx context.Context, run string) <-chan error {
	t.Helper()
	f.sealOwn(t, credentialName, time.Hour)
	f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Run, task.Message.TaskID, task.Message.ContextID = run, run, run
	})
	done := make(chan error, 1)
	go func() { done <- f.receiver.claimOne(ctx) }()
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

// pollPool joins the pool's share group as the superuser and reports whether run is delivered within wait.
func (f fixture) pollPool(t *testing.T, run string, wait time.Duration) (*kgo.Record, bool) {
	t.Helper()
	member, err := kgo.NewClient(append(f.service.Admin.Opts(), kgo.ConsumeTopics(admin.PoolTasksTopic(testPool)), kgo.ShareGroup(admin.PoolGroup(testPool)))...)
	if err != nil {
		t.Fatal(err)
	}
	defer member.Close()
	ctx, cancel := context.WithTimeout(f.ctx, wait)
	defer cancel()
	for ctx.Err() == nil {
		for _, record := range member.PollRecords(ctx, 10).Records() {
			if string(record.Key) == run {
				return record, true
			}
		}
	}
	return nil, false
}

func TestCancelStopsInFlightRun(t *testing.T) {
	f := newFixture(t, noResultStream)
	f.adapter.endDelay = 30 * time.Second
	done := startInFlight(t, f, f.ctx, "run-cancelled")
	f.publishCancel(t, "run-cancelled", 1, f.host, nil)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("in-flight run kept going after its cancel")
	}
	f.adapter.mu.Lock()
	stops, releaseIDs := f.adapter.stops, f.adapter.releaseIDs
	f.adapter.mu.Unlock()
	if stops != 1 || !slices.Equal(releaseIDs, []string{"run-cancelled"}) {
		t.Fatalf("stops=%d releases=%v", stops, releaseIDs)
	}
	if task := f.lastTask(t); task.ID != "run-cancelled" || task.Status.State != wire.TaskStateCanceled {
		t.Fatalf("task %#v, want CANCELED", task)
	}
	if state, reason := terminalReason(t, f, "run-cancelled"); state != wire.TaskStateCanceled || reason != "canceled by user" {
		t.Fatalf("terminal %s %q", state, reason)
	}
	// Accepted, not released: the pool never sees the submission again.
	if record, delivered := f.pollPool(t, "run-cancelled", 2*time.Second); delivered {
		t.Fatalf("cancelled run's submission was redelivered (delivery %d)", record.DeliveryCount())
	}
}

func TestRunningTurnSeesCancelBetweenHeartbeats(t *testing.T) {
	f := newFixture(t, noResultStream)
	f.adapter.endDelay = 30 * time.Second
	renewInterval = time.Minute // only the state check, not a heartbeat, can stop the run in time
	done := startInFlight(t, f, f.ctx, "run-cancelled")
	f.publishCancel(t, "run-cancelled", 1, f.host, nil)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * stateCheckInterval):
		t.Fatal("running turn missed a cancel the receiver held")
	}
	if state, reason := terminalReason(t, f, "run-cancelled"); state != wire.TaskStateCanceled || reason != "canceled by user" {
		t.Fatalf("terminal %s %q", state, reason)
	}
}

func TestHeartbeatHonoursHeldCancel(t *testing.T) {
	f := newFixture(t, noResultStream)
	f.adapter.endDelay = 30 * time.Second
	stateCheckInterval = time.Minute // only a heartbeat can stop the run in time
	t.Cleanup(func() { stateCheckInterval = time.Second })
	done := startInFlight(t, f, f.ctx, "run-cancelled")
	f.publishCancel(t, "run-cancelled", 1, f.host, nil)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * renewInterval):
		t.Fatal("heartbeat committed past a cancel the receiver held")
	}
	if state, _ := terminalReason(t, f, "run-cancelled"); state != wire.TaskStateCanceled {
		t.Fatalf("terminal %s, want CANCELED", state)
	}
}

// interrupt runs the first turn of run without a seal, so it waits AUTH_REQUIRED.
func (f fixture) interrupt(t *testing.T, run, text string) *kgo.Record {
	t.Helper()
	record := f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Run, task.Message.TaskID, task.Message.ContextID = run, run, run
		task.Message.Parts = []wire.Part{{Text: text}}
	})
	if status := f.execute(t, record, 1); status != kgo.AckAccept {
		t.Fatalf("status %v, want the interrupted task acknowledged", status)
	}
	if task := f.lastTask(t); task.Status.State != wire.TaskStateAuthRequired {
		t.Fatalf("task %#v, want AUTH_REQUIRED", task)
	}
	return record
}

func TestCancelInterruptedTaskEndsIt(t *testing.T) {
	f := newFixture(t, resultStream)
	peer := f.startPeer(t, "receiver-b")
	f.interrupt(t, "run-a", "first")
	// The peer, which never held the run, reads the cancel first and ends the interrupted turn.
	f.produce(t, admin.PoolEventsTopic(testPool), "run-a", must(t)(wire.SignRecord(&wire.Cancel{Tenant: testTenant, Host: testHost, Pool: testPool, Run: "run-a", Turn: 1}, f.host)))
	if err := peer.readCancels(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.readCancels(f.ctx); err != nil {
		t.Fatal(err)
	}
	keys := signed.Keys{"receiver-b": {Current: peer.signing.Public().(ed25519.PublicKey)}, testReceiver: f.keys()[testReceiver]}
	var canceled []wire.RunStatus
	var result *wire.Result
	for _, record := range f.records(t, admin.TenantTopic(testTenant)) {
		var status wire.RunStatus
		if wire.ParseRecord(record.Value, keys, &status) == nil && status.Wherry.Kind == "terminal" {
			canceled = append(canceled, status)
		}
		var candidate wire.Result
		if wire.ParseRecord(record.Value, keys, &candidate) == nil {
			result = &candidate
		}
	}
	if len(canceled) != 1 || canceled[0].Wherry.EventID != "run-a/1/canceled" || canceled[0].Wherry.Receiver != "receiver-b" || canceled[0].StatusUpdate.Status.State != wire.TaskStateCanceled {
		t.Fatalf("CANCELED terminals on the feed %#v, want the peer's alone, with the turn's event ID", canceled)
	}
	if result == nil || result.Task.Status.State != wire.TaskStateCanceled || len(result.Task.History) != 1 || result.Task.History[0].Parts[0].Text != "first" {
		t.Fatalf("result %#v, want CANCELED with the first turn", result)
	}

	// A continuation published without the host's own check is cancelled too, and never dispatched.
	f.sealOwn(t, credentialName, time.Hour)
	if status := f.execute(t, f.turn(t, "run-a", 2), 1); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	if task := f.lastTask(t); task.Status.State != wire.TaskStateCanceled || len(task.History) != 2 {
		t.Fatalf("task %#v, want CANCELED with both turns", task)
	}
	if dispatches, _, _, _ := f.adapter.counts(); dispatches != 0 {
		t.Fatalf("continuation of a cancelled task dispatched %d jobs", dispatches)
	}
}

// startPeer provisions and configures a second member of the pool and refreshes the fixture receiver.
func (f fixture) startPeer(t *testing.T, id string) *Receiver {
	t.Helper()
	peer, _ := f.newReceiver(t, testConfig(id))
	if err := peer.register(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := peer.configure(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	return peer
}
