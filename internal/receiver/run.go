package receiver

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/benemon/wherry/internal/trust"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	maxLine        = 512 << 10
	maxAgentResult = 16 << 10
	maxRunDuration = time.Hour // receiver cap on any task's run deadline (ADR-0013)
	readTimeout    = 30 * time.Second
)

// Intervals are variables so tests can shorten them; renew stays below the 30 s default lock.
var (
	renewInterval      = 10 * time.Second
	capabilityInterval = 30 * time.Second
	refreshInterval    = 15 * time.Second
	// shutdownTimeout bounds the final transcript commit and ack flush after the receiver is told to stop.
	shutdownTimeout   = 5 * time.Second
	enrolPollInterval = 10 * time.Second
)

// renewRequestInterval is the least time between renew requests for one generation.
const renewRequestInterval = time.Hour

// claimLoop holds one slot: a dedicated share client per claimed record,
// because renews do not survive a poll and held records are tied to the connection.
func (r *Receiver) claimLoop(ctx context.Context) {
	for ctx.Err() == nil {
		enrolled := r.current()
		if enrolled == nil {
			sleep(ctx, 5*time.Second)
			continue
		}
		if err := r.claimOne(ctx, enrolled); err != nil && ctx.Err() == nil {
			r.logger.Printf("claim: %v", err)
			sleep(ctx, 2*time.Second)
		}
	}
}

func (r *Receiver) claimOne(ctx context.Context, enrolled *trust.Enrolled) error {
	share, err := kgo.NewClient(append(append([]kgo.Opt(nil), r.kafkaOpt...),
		kgo.ConsumeTopics(wire.TopicTask),
		kgo.ShareGroup("p-"+enrolled.Pool),
		kgo.ShareMaxRecords(1),
		kgo.ShareMaxRecordsStrict(),
	)...)
	if err != nil {
		return err
	}
	defer share.Close()
	// Acks are flushed on shutdown too, so they must outlive ctx.
	flush := func() error {
		flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
		defer cancel()
		return share.FlushAcks(flushCtx)
	}
	var record *kgo.Record
	for record == nil {
		pollCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		// A receiver whose enrolment ends must not acquire a task: each release raises its delivery count.
		go func() {
			for pollCtx.Err() == nil && r.current() != nil {
				sleep(pollCtx, 100*time.Millisecond)
			}
			cancel()
		}()
		fetches := share.PollRecords(pollCtx, 1)
		cancel()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if r.current() == nil && len(fetches.Records()) == 0 {
			return nil
		}
		if err := fetches.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("poll: %w", err)
		}
		if records := fetches.Records(); len(records) > 0 {
			record = records[0]
		}
	}
	if r.current() == nil {
		// Enrolment ended while this poll was open.
		record.Ack(kgo.AckRelease)
		return flush()
	}
	admitted, ack := r.admit(ctx, record, enrolled)
	if admitted == nil {
		record.Ack(ack)
		return flush()
	}
	r.adjustInFlight(1)
	defer r.adjustInFlight(-1)
	if status := r.execute(ctx, share, record, admitted, attemptOf(record)); status != 0 {
		record.Ack(status)
		return flush()
	}
	return nil
}

type admission struct {
	task     *wire.SubmissionRecord
	taskHash string
	offer    wire.Offer
	history  []wire.Message
	git      *wire.GitContext
	// cancelled is set when the user cancelled the run; it ends CANCELED without a dispatch.
	cancelled bool
}

// admit applies ADR-0014 root verification, the run's turn order, ADR-0011/0017
// matching, ADR-0013 cancellation, deadlines and part checks, and builds the
// prompt from the run's history. A nil admission carries the acknowledgement.
func (r *Receiver) admit(ctx context.Context, record *kgo.Record, enrolled *trust.Enrolled) (*admission, kgo.AckStatus) {
	task, err := trust.VerifyRootSubmission(record.Value, r.userPub)
	if err != nil {
		return nil, kgo.AckReject
	}
	if task.Wherry.Pool != enrolled.Pool {
		return nil, kgo.AckAccept
	}
	r.mu.Lock()
	offers := r.offers
	r.mu.Unlock()
	reject := func(reason string) (*admission, kgo.AckStatus) {
		r.logger.Printf("rejected %s: %s", task.Wherry.RunID, reason)
		if err := r.publishRejection(ctx, record, task, reason); err != nil {
			r.logger.Printf("publish rejection for %s: %v", task.Wherry.RunID, err)
			return nil, kgo.AckRelease
		}
		return nil, kgo.AckReject
	}
	taskHash := trust.EnvelopeHash(record.Value)
	submissions, reason, err := r.continuationRejection(ctx, task, taskHash)
	if err != nil {
		r.logger.Printf("read continuation status for %s: %v", task.Wherry.RunID, err)
		return nil, kgo.AckRelease
	}
	if reason != "" {
		return reject(reason)
	}
	offer, matched := wire.MatchOffer(task.Wherry.Selector, enrolled.Labels, enrolled.Adapters, offers)
	if !matched {
		// Pool members are meant to be uniform; a mismatch is misconfiguration and must not drop the task unseen.
		return reject("no offer matches the task")
	}
	history := []wire.Message{task.Message}
	if task.Wherry.Turn > 1 {
		if history, err = turns(submissions, task); err != nil {
			r.logger.Printf("run %s: continuation history: %v", task.Wherry.RunID, err)
			return reject("continuation history unavailable")
		}
	}
	// A cancel published since the last refresh must still stop a task before it is dispatched.
	if err := r.refreshCancelled(ctx); err != nil {
		r.logger.Printf("read cancellations for %s: %v", task.Wherry.RunID, err)
	}
	r.mu.Lock()
	cancelled := r.cancelled[task.Wherry.RunID]
	r.mu.Unlock()
	admitted := &admission{task: task, taskHash: taskHash, offer: offer, history: history, cancelled: cancelled}
	if cancelled {
		return admitted, 0
	}
	now := time.Now()
	if now.UnixMilli() > task.Wherry.QueueDeadlineMS {
		return reject("queue deadline passed")
	}
	if now.UnixMilli() > task.Wherry.DeadlineMS {
		return reject("run deadline passed")
	}
	named, _ := r.named(offer.Adapter)
	git, supported := gitContext(history, named.Git)
	if !supported {
		return reject("unsupported message part")
	}
	admitted.git = git
	return admitted, 0
}

// gitContext returns the run's git context; false means the turns carry a part other than text
// and at most one git part, or a git part the adapter does not deliver.
func gitContext(history []wire.Message, delivered bool) (*wire.GitContext, bool) {
	var found *wire.GitContext
	for _, message := range history {
		for _, part := range message.Parts {
			if part.Text != "" {
				continue
			}
			if !delivered || part.MediaType != wire.GitMediaType || found != nil {
				return nil, false
			}
			git, err := wire.ParseGitPart(part)
			if err != nil {
				return nil, false
			}
			found = &git
		}
	}
	return found, true
}

func (r *Receiver) publishRejection(ctx context.Context, record *kgo.Record, task *wire.SubmissionRecord, reason string) error {
	status := r.status(task, trust.EnvelopeHash(record.Value), attemptOf(record), "rejected", taskStatus(wire.TaskStateRejected, reason), nil)
	value, err := sign(wire.TypeStatus, status, r.signing)
	if err != nil {
		return err
	}
	return r.base.ProduceSync(ctx, &kgo.Record{Topic: wire.TopicEvent, Key: []byte(task.Wherry.RunID), Value: value}).FirstErr()
}

type runSubmission struct {
	record *wire.SubmissionRecord
	hash   string
}

func (r *Receiver) submissions(ctx context.Context, expected *wire.SubmissionRecord) (map[int64]runSubmission, error) {
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	records, err := readBounded(ctx, r.kafkaOpt, wire.TopicTask)
	if err != nil {
		return nil, err
	}
	result := make(map[int64]runSubmission)
	for _, record := range records {
		if string(record.Key) != expected.Wherry.RunID {
			continue
		}
		submission, err := trust.VerifyRootSubmission(record.Value, r.userPub)
		if err != nil || submission.Wherry.RunID != expected.Wherry.RunID ||
			submission.Message.ContextID != expected.Message.ContextID ||
			submission.Wherry.Pool != expected.Wherry.Pool ||
			submission.Wherry.ReplyTopic != expected.Wherry.ReplyTopic ||
			!reflect.DeepEqual(submission.Wherry.Selector, expected.Wherry.Selector) ||
			!reflect.DeepEqual(submission.Wherry.Lineage, expected.Wherry.Lineage) {
			continue
		}
		if _, duplicate := result[submission.Wherry.Turn]; duplicate {
			continue
		}
		result[submission.Wherry.Turn] = runSubmission{record: submission, hash: trust.EnvelopeHash(record.Value)}
	}
	return result, nil
}

func (r *Receiver) continuationRejection(ctx context.Context, task *wire.SubmissionRecord, taskHash string) (map[int64]runSubmission, string, error) {
	submissions, err := r.submissions(ctx, task)
	if err != nil {
		return nil, "", err
	}
	if submissions[task.Wherry.Turn].hash != taskHash {
		if task.Wherry.Turn == 1 {
			return nil, "duplicate submission", nil
		}
		return nil, "duplicate continuation", nil
	}
	r.mu.Lock()
	signers := append([]ed25519.PublicKey(nil), r.signers...)
	r.mu.Unlock()
	if len(signers) == 0 {
		signers = append(signers, r.signing.Public().(ed25519.PublicKey))
	}
	records, err := readBounded(ctx, r.kafkaOpt, wire.TopicEvent)
	if err != nil {
		return nil, "", err
	}
	var latest *wire.StatusRecord
	var order wire.AttemptFilter
	for _, record := range records {
		if string(record.Key) != task.Wherry.RunID {
			continue
		}
		envelope, err := wire.DecodeEnvelope(record.Value)
		if err != nil {
			continue
		}
		var verified any
		for _, signer := range signers {
			verified, err = wire.Verify(envelope, signer, wire.SignerReceiver)
			if err == nil {
				break
			}
		}
		status, ok := verified.(*wire.StatusRecord)
		if err != nil || !ok || status.Wherry.Kind == "rejected" {
			continue
		}
		submission, found := submissions[status.Wherry.Turn]
		if !found || status.StatusUpdate.TaskID != task.Wherry.RunID ||
			status.StatusUpdate.ContextID != task.Wherry.Lineage.RootRunID || status.Wherry.TaskHash != submission.hash ||
			!order.Admit(task.Wherry.RunID, status.Wherry.Turn, status.Wherry.Attempt) {
			continue
		}
		latest = status
	}
	if latest != nil && (latest.Wherry.Turn > task.Wherry.Turn ||
		latest.Wherry.Turn == task.Wherry.Turn && (latest.Wherry.Kind == "interrupted" || latest.Wherry.Kind == "terminal")) {
		return nil, "stale submission", nil
	}
	if task.Wherry.Turn == 1 || latest != nil && latest.Wherry.Turn == task.Wherry.Turn {
		return submissions, "", nil
	}
	if latest == nil || latest.Wherry.Kind != "interrupted" || !wire.Interrupted(latest.StatusUpdate.Status.State) {
		return nil, "task is not awaiting continuation", nil
	}
	if task.Wherry.Turn != latest.Wherry.Turn+1 {
		return nil, "continuation turn out of order", nil
	}
	return submissions, "", nil
}

// renewer keeps the share lock alive from claim until the final acknowledgement,
// including while the platform places the job.
func renewer(ctx context.Context, share *kgo.Client, record *kgo.Record, logf func(string, ...any)) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(renewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				record.Ack(kgo.AckRenew)
				if err := share.FlushAcks(ctx); err != nil && ctx.Err() == nil {
					logf("renew: %v", err)
				}
			}
		}
	}()
	return func() { cancel(); wg.Wait() }
}

// execute runs one task; it returns the final share acknowledgement, or 0 when fenced.
func (r *Receiver) execute(ctx context.Context, share *kgo.Client, record *kgo.Record, admitted *admission, attempt int64) kgo.AckStatus {
	task := admitted.task
	runID := task.Wherry.RunID
	stopRenew := renewer(ctx, share, record, func(format string, args ...any) {
		r.logger.Printf("run %s: "+format, append([]any{runID}, args...)...)
	})
	defer stopRenew()

	// The cap is anchored to the signed queue deadline so redelivery cannot extend it.
	deadline := time.UnixMilli(task.Wherry.DeadlineMS)
	if limit := time.UnixMilli(task.Wherry.QueueDeadlineMS).Add(maxRunDuration); deadline.After(limit) {
		deadline = limit
	}
	runCtx, cancelRun := context.WithDeadline(ctx, deadline)
	defer cancelRun()

	// One transactional producer per run: an adopter initialising the same ID fences this holder (ADR-0010).
	producer, err := kgo.NewClient(append(append([]kgo.Opt(nil), r.kafkaOpt...), kgo.TransactionalID(runID))...)
	if err != nil {
		r.logger.Printf("run %s: producer: %v", runID, err)
		return kgo.AckRelease
	}
	defer producer.Close()
	matched, ok := r.named(admitted.offer.Adapter)
	if !ok {
		r.logger.Printf("run %s: matched adapter %s is not configured", runID, admitted.offer.Adapter)
		return kgo.AckRelease
	}
	run := &activeRun{receiver: r, producer: producer, task: task, taskHash: admitted.taskHash, history: admitted.history, attempt: attempt, adapter: matched}

	if err := run.commit(ctx, run.statusRecord("claimed", taskStatus(wire.TaskStateWorking, ""), nil)); err != nil {
		return run.abandon(err)
	}
	offset := int64(0)
	if attempt > 1 {
		offset, err = r.lastOffset(runCtx, runID, run.taskHash, task.Wherry.Lineage.RootRunID, task.Wherry.Turn)
		if err != nil {
			r.logger.Printf("run %s: read events: %v", runID, err)
			return kgo.AckRelease
		}
	}
	var handle Handle
	adopted := false
	if attempt > 1 {
		// Offers can change between attempts, so the run may live in an adapter other than the matched one.
		for _, named := range r.adapters {
			found, ok, err := named.Adapter.Lookup(runCtx, runID, task.Wherry.Turn)
			if err != nil {
				if runCtx.Err() != nil && ctx.Err() == nil {
					return run.fail(ctx, "timeout", nil)
				}
				if errors.Is(err, ErrGitContext) {
					r.logger.Printf("run %s: git context unavailable: %T", runID, err)
					run.adapter = named
					return run.fail(ctx, ErrGitContext.Error(), nil)
				}
				r.logger.Printf("run %s: lookup in adapter %s: %T", runID, named.Name, err)
				return kgo.AckRelease
			}
			if ok {
				handle, adopted, run.adapter = found, true, named
				break
			}
		}
	}
	if admitted.cancelled {
		if adopted {
			run.job = handle.Job
			return run.stopAndCancel(ctx, handle, nil)
		}
		return run.cancel(ctx, nil)
	}
	if !adopted {
		named := run.adapter
		credential, ok := r.held(named.Credential)
		if !ok {
			return run.interrupt(ctx, named.Credential)
		}
		handle, err = named.Adapter.Dispatch(runCtx, Run{
			ID: runID, Turn: task.Wherry.Turn, Offer: admitted.offer.Labels, Prompt: []byte(prompt(admitted.history)), Credential: credential,
			Git: admitted.git,
		})
		if err != nil {
			if runCtx.Err() != nil && ctx.Err() == nil {
				return run.fail(ctx, "timeout", nil)
			}
			if errors.Is(err, ErrGitContext) {
				// Git error text can echo URLs, so only the type is logged.
				r.logger.Printf("run %s: git context unavailable: %T", runID, err)
				return run.fail(ctx, ErrGitContext.Error(), nil)
			}
			r.logger.Printf("run %s: dispatch failed: %T", runID, err)
			return run.fail(ctx, "dispatch failed", nil)
		}
		run.offer = admitted.offer.Labels
	}
	run.job = handle.Job
	r.logger.Printf("run %s: attempt %d in adapter %s with %s (%s)", runID, attempt, run.adapter.Name, wire.TermsText(admitted.offer.Labels), handle.Job)
	started := r.status(task, run.taskHash, attempt, "started", taskStatus(wire.TaskStateWorking, ""), &offset)
	if handle.Commit != "" {
		started.StatusUpdate.Metadata, _ = json.Marshal(wire.CommitMetadata{GitCommit: handle.Commit}) // a string always encodes
	}
	if err := run.commit(ctx, run.signed(wire.TopicEvent, wire.TypeStatus, started)); err != nil {
		return run.abandon(err)
	}

	followCtx, stopFollow := context.WithCancel(runCtx)
	defer stopFollow()
	lines := make(chan Line, 64)
	followed := make(chan error, 1)
	go func() { followed <- run.adapter.Adapter.Follow(followCtx, handle, offset, lines) }()

	heartbeat := time.NewTicker(renewInterval)
	defer heartbeat.Stop()
	var pending []Line
	position := offset
	take := func(line Line) (agentResult, bool) {
		pending = append(pending, line)
		position = line.Next
		return resultLine(line.Data)
	}
	// shutdown commits what is pending under its own deadline, since ctx is already done, and returns the task.
	shutdown := func() kgo.AckStatus {
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
		defer cancel()
		if err := run.commit(shutdownCtx, run.chunks(pending, false)...); err != nil {
			return run.abandon(err)
		}
		return kgo.AckRelease
	}
	for {
		select {
		case line := <-lines:
			if result, ok := take(line); ok {
				stopFollow()
				return run.finish(ctx, handle, pending, result)
			}
		case err := <-followed:
			for drained := false; !drained; {
				select {
				case line := <-lines:
					if result, ok := take(line); ok {
						return run.finish(ctx, handle, pending, result)
					}
				default:
					drained = true
				}
			}
			switch {
			case runCtx.Err() != nil && ctx.Err() == nil:
				return run.stopAndFail(ctx, handle, "timeout", pending)
			case ctx.Err() != nil:
				return shutdown()
			case err != nil:
				// A dropped stream does not mean the task ended; stop it rather than leave it running.
				r.logger.Printf("run %s: follow failed: %T", runID, err)
				return run.stopAndFail(ctx, handle, "log stream failed", pending)
			default:
				return run.stopAndFail(ctx, handle, "agent ended without a result", pending)
			}
		case <-ctx.Done():
			stopFollow()
			return shutdown()
		case <-heartbeat.C:
			if ctx.Err() != nil {
				stopFollow()
				return shutdown()
			}
			// Read at each heartbeat, so a cancel stops a run within one heartbeat, not after the next refresh too.
			if err := r.refreshCancelled(ctx); err != nil {
				r.logger.Printf("run %s: read cancellations: %v", runID, err)
			}
			r.mu.Lock()
			revoked, cancelled := r.revoked, r.cancelled[runID]
			r.mu.Unlock()
			if revoked {
				stopFollow()
				return run.revoke(ctx, handle, pending)
			}
			if cancelled {
				stopFollow()
				return run.stopAndCancel(ctx, handle, pending)
			}
			// The newest line stays pending so the terminal transaction always holds the last chunk;
			// the heartbeat offset is where that line starts, so an adopter reads it again.
			at, held := position, 0
			if len(pending) > 0 {
				at, held = pending[len(pending)-1].Offset, 1
			}
			records := append(run.chunks(pending[:len(pending)-held], false), run.statusRecord("heartbeat", taskStatus(wire.TaskStateWorking, ""), &at))
			if err := run.commit(ctx, records...); err != nil {
				return run.abandon(err)
			}
			pending = pending[len(pending)-held:]
		}
	}
}

type activeRun struct {
	receiver *Receiver
	producer *kgo.Client
	task     *wire.SubmissionRecord
	taskHash string
	history  []wire.Message
	attempt  int64
	// adapter holds the run's platform material, released at the end.
	adapter Named
	// offer is set when this attempt dispatched the job; an adopted job's offer is unknown.
	offer map[string]string
	job   string
}

func (run *activeRun) commit(ctx context.Context, records ...*kgo.Record) error {
	if len(records) == 0 {
		return nil
	}
	if err := run.producer.BeginTransaction(); err != nil {
		return err
	}
	if err := run.producer.ProduceSync(ctx, records...).FirstErr(); err != nil {
		_ = run.producer.EndTransaction(ctx, kgo.TryAbort)
		return err
	}
	return run.producer.EndTransaction(ctx, kgo.TryCommit)
}

// abandon stops all activity for a run whose writes failed; a fenced holder leaves the run to its adopter.
func (run *activeRun) abandon(err error) kgo.AckStatus {
	if errors.Is(err, kerr.InvalidProducerEpoch) || errors.Is(err, kerr.ProducerFenced) {
		run.receiver.logger.Printf("run %s: fenced by a newer holder", run.task.Wherry.RunID)
		return 0
	}
	run.receiver.logger.Printf("run %s: write failed: %v", run.task.Wherry.RunID, err)
	return kgo.AckRelease
}

func (run *activeRun) stopAndFail(ctx context.Context, handle Handle, reason string, pending []Line) kgo.AckStatus {
	if err := run.adapter.Adapter.Stop(ctx, handle); err != nil {
		run.receiver.logger.Printf("run %s: stop: %T", run.task.Wherry.RunID, err)
		return kgo.AckRelease
	}
	return run.fail(ctx, reason, pending)
}

func (run *activeRun) stopAndCancel(ctx context.Context, handle Handle, pending []Line) kgo.AckStatus {
	if err := run.adapter.Adapter.Stop(ctx, handle); err != nil {
		run.receiver.logger.Printf("run %s: stop: %T", run.task.Wherry.RunID, err)
		return kgo.AckRelease
	}
	return run.cancel(ctx, pending)
}

// revoke stops a run on a revoked receiver and releases its task to the pool; clients no longer
// trust this receiver's records, so the terminal status is best effort.
func (run *activeRun) revoke(ctx context.Context, handle Handle, pending []Line) kgo.AckStatus {
	if err := run.adapter.Adapter.Stop(ctx, handle); err != nil {
		run.receiver.logger.Printf("run %s: stop: %T", run.task.Wherry.RunID, err)
		return kgo.AckRelease
	}
	const reason = "receiver revoked"
	status := taskStatus(wire.TaskStateFailed, reason)
	if err := run.commit(ctx, append(run.chunks(pending, true), run.taskRecord(status, reason, nil), run.statusRecord("terminal", status, nil))...); err != nil {
		run.receiver.logger.Printf("run %s: publish revoked status: %v", run.task.Wherry.RunID, err)
	}
	run.release(ctx)
	return kgo.AckRelease
}

func (run *activeRun) fail(ctx context.Context, reason string, pending []Line) kgo.AckStatus {
	return run.end(ctx, taskStatus(wire.TaskStateFailed, reason), reason, nil, pending, kgo.AckReject)
}

func (run *activeRun) cancel(ctx context.Context, pending []Line) kgo.AckStatus {
	const reason = "canceled by user"
	return run.end(ctx, taskStatus(wire.TaskStateCanceled, reason), reason, nil, pending, kgo.AckAccept)
}

func (run *activeRun) interrupt(ctx context.Context, credential string) kgo.AckStatus {
	record := run.receiver.status(run.task, run.taskHash, run.attempt, "interrupted", taskStatus(wire.TaskStateAuthRequired, "credential "+credential+" required"), nil)
	record.StatusUpdate.Metadata, _ = json.Marshal(wire.CredentialMetadata{Credential: credential}) // a string always encodes
	if err := run.commit(ctx, run.signed(wire.TopicEvent, wire.TypeStatus, record)); err != nil {
		return run.abandon(err)
	}
	run.release(ctx)
	return kgo.AckAccept
}

func (run *activeRun) finish(ctx context.Context, handle Handle, pending []Line, result agentResult) kgo.AckStatus {
	if err := run.adapter.Adapter.Stop(ctx, handle); err != nil {
		run.receiver.logger.Printf("run %s: stop: %T", run.task.Wherry.RunID, err)
		return kgo.AckRelease
	}
	state, ack := wire.TaskStateCompleted, kgo.AckAccept
	if result.IsError {
		state, ack = wire.TaskStateFailed, kgo.AckReject
	}
	return run.end(ctx, taskStatus(state, ""), result.Result, result.metadata, pending, ack)
}

// end commits the remaining transcript, the final task and the terminal status in one transaction.
func (run *activeRun) end(ctx context.Context, status wire.TaskStatus, result string, resultMetadata jsontext.Value, pending []Line, ack kgo.AckStatus) kgo.AckStatus {
	records := append(run.chunks(pending, true), run.taskRecord(status, result, resultMetadata), run.statusRecord("terminal", status, nil))
	if err := run.commit(ctx, records...); err != nil {
		return run.abandon(err)
	}
	run.release(ctx)
	return ack
}

func (run *activeRun) release(ctx context.Context) {
	if err := run.adapter.Adapter.Release(ctx, run.task.Wherry.RunID, run.task.Wherry.Turn); err != nil {
		run.receiver.logger.Printf("run %s: release: %T", run.task.Wherry.RunID, err)
	}
}

func (run *activeRun) statusRecord(kind string, status wire.TaskStatus, offset *int64) *kgo.Record {
	return run.signed(wire.TopicEvent, wire.TypeStatus, run.receiver.status(run.task, run.taskHash, run.attempt, kind, status, offset))
}

func (r *Receiver) status(task *wire.SubmissionRecord, taskHash string, attempt int64, kind string, status wire.TaskStatus, offset *int64) *wire.StatusRecord {
	return &wire.StatusRecord{
		StatusUpdate: wire.TaskStatusUpdateEvent{TaskID: task.Wherry.RunID, ContextID: task.Wherry.Lineage.RootRunID, Status: status},
		Wherry:       wire.StatusExtension{Turn: task.Wherry.Turn, Attempt: attempt, TaskHash: taskHash, ReceiverID: r.config.ReceiverID, Kind: kind, LogOffset: offset},
	}
}

// taskStatus stamps state with the current time; a reason becomes the status message.
func taskStatus(state wire.TaskState, reason string) wire.TaskStatus {
	status := wire.TaskStatus{State: state, Timestamp: time.Now().UTC().Format("2006-01-02T15:04:05.000Z")}
	if reason != "" {
		id := make([]byte, 16)
		rand.Read(id)
		status.Message = &wire.Message{
			MessageID: "msg-" + hex.EncodeToString(id), Role: wire.RoleAgent,
			Parts: []wire.Part{{Text: reason}}, Extensions: []string{wire.ExtensionURI},
		}
	}
	return status
}

func (run *activeRun) chunks(lines []Line, last bool) []*kgo.Record {
	records := make([]*kgo.Record, 0, len(lines))
	for i, line := range lines {
		part := wire.Part{Text: string(line.Data), MediaType: "application/x-ndjson"}
		switch {
		case len(line.Data) > maxLine:
			metadata, _ := json.Marshal(wire.PointerMetadata{SHA256: trust.EnvelopeHash(line.Data), Size: int64(len(line.Data))}) // a string and an integer always encode
			part = wire.Part{URL: "unstored:oversize", MediaType: part.MediaType, Metadata: metadata}
		case len(line.Data) == 0 || !utf8.Valid(line.Data):
			// A text part holds only non-empty UTF-8; other lines travel as exact bytes.
			part = wire.Part{Raw: append(wire.StdBytes{}, line.Data...), MediaType: part.MediaType}
		}
		update := &wire.ArtifactUpdateRecord{
			ArtifactUpdate: wire.TaskArtifactUpdateEvent{
				TaskID: run.task.Wherry.RunID, ContextID: run.task.Wherry.Lineage.RootRunID,
				Artifact: wire.Artifact{ArtifactID: "transcript", Parts: []wire.Part{part}, Extensions: []string{wire.ExtensionURI}},
				// The stream's first chunk is the line at log offset zero; an adopter resumes past it.
				Append:    line.Offset > 0,
				LastChunk: last && i == len(lines)-1,
			},
			Wherry: wire.ArtifactUpdateExtension{Turn: run.task.Wherry.Turn, Attempt: run.attempt, Seq: line.Offset},
		}
		records = append(records, run.signed(wire.TopicTranscript, wire.TypeArtifactUpdate, update))
	}
	return records
}

// placement is the final task's metadata: where its last turn ran.
type placement struct {
	ReceiverID string            `json:"receiver_id"`
	Pool       string            `json:"pool"`
	Adapter    string            `json:"adapter"`
	Offer      map[string]string `json:"offer,omitempty"`
	Job        string            `json:"job,omitempty"`
}

// taskRecord builds the final task; a result that is JSON travels as a data part.
func (run *activeRun) taskRecord(status wire.TaskStatus, result string, resultMetadata jsontext.Value) *kgo.Record {
	part := wire.Part{Text: result}
	switch {
	case result == "":
		part.Text = string(status.State)
	case jsontext.Value(result).IsValid():
		part = wire.Part{Data: jsontext.Value(result), MediaType: "application/json"}
	}
	metadata, _ := json.Marshal(placement{ // strings and a string map always encode
		ReceiverID: run.receiver.config.ReceiverID, Pool: run.task.Wherry.Pool, Adapter: run.adapter.Name, Offer: run.offer, Job: run.job,
	})
	task := &wire.TaskRecord{
		Task: wire.Task{
			ID: run.task.Wherry.RunID, ContextID: run.task.Wherry.Lineage.RootRunID, Status: status,
			History:   run.history,
			Artifacts: []wire.Artifact{{ArtifactID: "result", Parts: []wire.Part{part}, Metadata: resultMetadata, Extensions: []string{wire.ExtensionURI}}},
			Metadata:  metadata,
		},
		Wherry: wire.TaskExtension{Turn: run.task.Wherry.Turn, Attempt: run.attempt, TaskHash: run.taskHash},
	}
	return run.signed(run.task.Wherry.ReplyTopic, wire.TypeTask, task)
}

func turns(submissions map[int64]runSubmission, task *wire.SubmissionRecord) ([]wire.Message, error) {
	history := make([]wire.Message, 0, task.Wherry.Turn)
	for turn := int64(1); turn <= task.Wherry.Turn; turn++ {
		submission, ok := submissions[turn]
		if !ok {
			return nil, fmt.Errorf("submission turn %d not found", turn)
		}
		history = append(history, submission.record.Message)
	}
	return history, nil
}

func (run *activeRun) signed(topic string, recordType wire.RecordType, payload any) *kgo.Record {
	value, err := sign(recordType, payload, run.receiver.signing)
	if err != nil {
		panic(fmt.Sprintf("sign %s: %v", recordType, err)) // payloads are built here; invalid ones are programming errors
	}
	return &kgo.Record{Topic: topic, Key: []byte(run.task.Wherry.RunID), Value: value}
}

// lastOffset returns the highest started or heartbeat log offset this run reached under an earlier attempt.
func (r *Receiver) lastOffset(ctx context.Context, runID, taskHash, rootRunID string, turn int64) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	records, err := readBounded(ctx, r.kafkaOpt, wire.TopicEvent)
	if err != nil {
		return 0, err
	}
	r.mu.Lock()
	signers := append([]ed25519.PublicKey(nil), r.signers...)
	r.mu.Unlock()
	if len(signers) == 0 {
		signers = append(signers, r.signing.Public().(ed25519.PublicKey))
	}
	var highest int64
	for _, record := range records {
		if string(record.Key) != runID {
			continue
		}
		envelope, err := wire.DecodeEnvelope(record.Value)
		if err != nil {
			continue
		}
		var verified any
		for _, signer := range signers {
			verified, err = wire.Verify(envelope, signer, wire.SignerReceiver)
			if err == nil {
				break
			}
		}
		status, ok := verified.(*wire.StatusRecord)
		if err != nil || !ok || status.StatusUpdate.TaskID != runID || status.StatusUpdate.ContextID != rootRunID ||
			status.Wherry.Turn != turn || status.Wherry.TaskHash != taskHash || (status.Wherry.Kind != "started" && status.Wherry.Kind != "heartbeat") ||
			status.Wherry.LogOffset == nil {
			continue
		}
		if *status.Wherry.LogOffset > highest {
			highest = *status.Wherry.LogOffset
		}
	}
	return highest, nil
}

// attemptOf is the share-group delivery count, which franz-go reports as at least 1
// for share deliveries; anything else is treated as a first attempt.
func attemptOf(record *kgo.Record) int64 {
	return max(1, int64(record.DeliveryCount()))
}

type agentResult struct {
	Type    string `json:"type"`
	Result  string `json:"result"`
	IsError bool   `json:"is_error"`
	// metadata is the result artifact's metadata, nil when the record cannot travel.
	metadata jsontext.Value
}

// resultLine recognises the final stream-json record; stdout is data, never control.
func resultLine(data []byte) (agentResult, bool) {
	var result agentResult
	if json.Unmarshal(data, &result) != nil || result.Type != "result" {
		return agentResult{}, false
	}
	result.metadata = agentResultMetadata(data)
	return result, true
}

// agentResultMetadata passes the agent's result record through, less the result member the
// artifact already holds; a record that is not an I-JSON object or exceeds maxAgentResult is dropped.
func agentResultMetadata(data []byte) jsontext.Value {
	record := jsontext.Value(data)
	var members map[string]jsontext.Value
	if !record.IsValid() || record.Kind() != '{' || json.Unmarshal(record, &members) != nil {
		return nil
	}
	delete(members, "result")
	rest, err := json.Marshal(members)
	if err != nil || len(rest) > maxAgentResult {
		return nil
	}
	metadata, _ := json.Marshal(map[string]jsontext.Value{"agent_result": rest}) // a valid object always encodes
	return metadata
}

// resumedHeader opens a continuation's prompt. Every interrupt today happens before dispatch, so no
// earlier turn reached an agent; a per-turn transcript would read as work already done.
const resumedHeader = "This request was interrupted before any of it was carried out. Carry out all of it now. Its parts follow in the order they were sent."

// prompt is the first turn's text alone, or every turn's text as one request under resumedHeader.
func prompt(history []wire.Message) string {
	if len(history) == 1 {
		return text(history[0])
	}
	sections := []string{resumedHeader}
	for _, message := range history {
		sections = append(sections, text(message))
	}
	return strings.Join(sections, "\n\n")
}

func text(message wire.Message) string {
	var parts []string
	for _, part := range message.Parts {
		if part.Text != "" {
			parts = append(parts, part.Text)
		}
	}
	return strings.Join(parts, "\n")
}

// held returns a configured sealed secret's value; false means it is configured but not held.
func (r *Receiver) held(name string) ([]byte, bool) {
	if name == "" {
		return nil, true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	credential, ok := r.credentials[name]
	if !ok || !time.Now().Before(credential.exp) {
		return nil, false
	}
	return credential.value, true
}

func (r *Receiver) adjustInFlight(delta int64) {
	r.mu.Lock()
	r.inFlight += delta
	r.mu.Unlock()
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
