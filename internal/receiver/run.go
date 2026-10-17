package receiver

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/benemon/agentbus/internal/trust"
	"github.com/benemon/agentbus/internal/wire"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	maxLine        = 512 << 10
	maxRunDuration = time.Hour // receiver cap on any task's run deadline (ADR-0013)
	readTimeout    = 30 * time.Second
)

// Intervals are variables so tests can shorten them; renew stays below the 30 s default lock.
var (
	renewInterval      = 10 * time.Second
	capabilityInterval = 30 * time.Second
	refreshInterval    = 15 * time.Second
	enrolPollInterval  = 10 * time.Second
)

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
	var record *kgo.Record
	for record == nil {
		pollCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		fetches := share.PollRecords(pollCtx, 1)
		cancel()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := fetches.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("poll: %w", err)
		}
		if records := fetches.Records(); len(records) > 0 {
			record = records[0]
		}
	}
	admitted, ack := r.admit(ctx, record, enrolled)
	if admitted == nil {
		record.Ack(ack)
		return share.FlushAcks(ctx)
	}
	r.adjustInFlight(1)
	defer r.adjustInFlight(-1)
	if status := r.execute(ctx, share, record, admitted, attemptOf(record)); status != 0 {
		record.Ack(status)
		return share.FlushAcks(ctx)
	}
	return nil
}

type admission struct {
	task   *wire.Task
	offer  wire.Offer
	prompt string
}

// admit applies ADR-0014 root verification and ADR-0011/0017 matching. A nil
// admission carries the acknowledgement.
func (r *Receiver) admit(ctx context.Context, record *kgo.Record, enrolled *trust.Enrolled) (*admission, kgo.AckStatus) {
	task, err := trust.VerifyRootTask(record.Value, r.userPub)
	if err != nil {
		return nil, kgo.AckReject
	}
	if task.Pool != enrolled.Pool {
		return nil, kgo.AckAccept
	}
	r.mu.Lock()
	offers := r.offers
	r.mu.Unlock()
	reject := func(reason string) (*admission, kgo.AckStatus) {
		r.logger.Printf("rejected %s: %s", task.RunID, reason)
		if err := r.publishRejection(ctx, record, task, reason); err != nil {
			r.logger.Printf("publish rejection for %s: %v", task.RunID, err)
			return nil, kgo.AckRelease
		}
		return nil, kgo.AckReject
	}
	offer, matched := wire.MatchOffer(task.Selector, enrolled.Labels, enrolled.Adapters, offers)
	if !matched || !wire.Satisfies(enrolled.Selector, task.Labels) {
		return nil, kgo.AckAccept
	}
	now := time.Now()
	if now.UnixMilli() > task.QueueDeadlineMS {
		return reject("queue deadline passed")
	}
	if now.UnixMilli() > task.DeadlineMS {
		return reject("run deadline passed")
	}
	if slices.ContainsFunc(task.Message.Parts, func(part wire.Part) bool { return part.Text == "" }) {
		return reject("unsupported message part")
	}
	text := prompt(task)
	return &admission{task: task, offer: offer, prompt: text}, 0
}

func (r *Receiver) publishRejection(ctx context.Context, record *kgo.Record, task *wire.Task, reason string) error {
	event := r.event(task, trust.EnvelopeHash(record.Value), attemptOf(record), "terminal", wire.TaskStateRejected, nil, &reason)
	value, err := sign(wire.TypeEvent, event, r.signing)
	if err != nil {
		return err
	}
	return r.base.ProduceSync(ctx, &kgo.Record{Topic: wire.TopicEvent, Key: []byte(task.RunID), Value: value}).FirstErr()
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
	stopRenew := renewer(ctx, share, record, func(format string, args ...any) {
		r.logger.Printf("run %s: "+format, append([]any{task.RunID}, args...)...)
	})
	defer stopRenew()

	// The cap is anchored to the signed queue deadline so redelivery cannot extend it.
	deadline := time.UnixMilli(task.DeadlineMS)
	if limit := time.UnixMilli(task.QueueDeadlineMS).Add(maxRunDuration); deadline.After(limit) {
		deadline = limit
	}
	runCtx, cancelRun := context.WithDeadline(ctx, deadline)
	defer cancelRun()

	// One transactional producer per run: an adopter initialising the same ID fences this holder (ADR-0010).
	producer, err := kgo.NewClient(append(append([]kgo.Opt(nil), r.kafkaOpt...), kgo.TransactionalID(task.RunID))...)
	if err != nil {
		r.logger.Printf("run %s: producer: %v", task.RunID, err)
		return kgo.AckRelease
	}
	defer producer.Close()
	matched, ok := r.named(admitted.offer.Adapter)
	if !ok {
		r.logger.Printf("run %s: matched adapter %s is not configured", task.RunID, admitted.offer.Adapter)
		return kgo.AckRelease
	}
	run := &activeRun{receiver: r, producer: producer, task: task, taskHash: trust.EnvelopeHash(record.Value), attempt: attempt, adapter: matched}

	if err := run.commit(ctx, run.eventRecord("claimed", wire.TaskStateWorking, nil, nil)); err != nil {
		return run.abandon(err)
	}
	offset := int64(0)
	if attempt > 1 {
		offset, err = r.lastOffset(runCtx, task.RunID)
		if err != nil {
			r.logger.Printf("run %s: read events: %v", task.RunID, err)
			return kgo.AckRelease
		}
	}
	var handle Handle
	adopted := false
	if attempt > 1 {
		// Offers can change between attempts, so the run may live in an adapter other than the matched one.
		for _, named := range r.adapters {
			found, ok, err := named.Adapter.Lookup(runCtx, task.RunID)
			if err != nil {
				if runCtx.Err() != nil && ctx.Err() == nil {
					return run.fail(ctx, "timeout")
				}
				r.logger.Printf("run %s: lookup in adapter %s: %v", task.RunID, named.Name, err)
				return kgo.AckRelease
			}
			if ok {
				handle, adopted, run.adapter = found, true, named
				break
			}
		}
	}
	if !adopted {
		named := run.adapter
		var credential []byte
		if named.Credential != "" {
			r.mu.Lock()
			held, ok := r.credentials[named.Name]
			r.mu.Unlock()
			if !ok {
				return run.fail(ctx, "credential unavailable")
			}
			credential = held.value
		}
		handle, err = named.Adapter.Dispatch(runCtx, Run{ID: task.RunID, Offer: admitted.offer.Labels, Prompt: []byte(admitted.prompt), Credential: credential})
		if err != nil {
			if runCtx.Err() != nil && ctx.Err() == nil {
				return run.fail(ctx, "timeout")
			}
			r.logger.Printf("run %s: dispatch failed: %T", task.RunID, err)
			return run.fail(ctx, "dispatch failed")
		}
	}
	r.logger.Printf("run %s: attempt %d in adapter %s with %s (%s)", task.RunID, attempt, run.adapter.Name, wire.TermsText(admitted.offer.Labels), handle.Job)
	if err := run.commit(ctx, run.eventRecord("started", wire.TaskStateWorking, &offset, nil)); err != nil {
		return run.abandon(err)
	}

	followCtx, stopFollow := context.WithCancel(runCtx)
	defer stopFollow()
	lines := make(chan Line, 64)
	followed := make(chan error, 1)
	go func() { followed <- run.adapter.Adapter.Follow(followCtx, handle, offset, lines) }()

	heartbeat := time.NewTicker(renewInterval)
	defer heartbeat.Stop()
	var pending []*kgo.Record
	position := offset
	take := func(line Line) (agentResult, bool) {
		pending = append(pending, run.transcriptRecord(line))
		position = line.Next
		return resultLine(line.Data)
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
			if commitErr := run.commit(ctx, pending...); commitErr != nil {
				return run.abandon(commitErr)
			}
			switch {
			case runCtx.Err() != nil && ctx.Err() == nil:
				return run.stopAndFail(ctx, handle, "timeout")
			case ctx.Err() != nil:
				return kgo.AckRelease
			case err != nil:
				// A dropped stream does not mean the task ended; stop it rather than leave it running.
				r.logger.Printf("run %s: follow failed: %T", task.RunID, err)
				return run.stopAndFail(ctx, handle, "log stream failed")
			default:
				return run.stopAndFail(ctx, handle, "agent ended without a result")
			}
		case <-heartbeat.C:
			at := position
			pending = append(pending, run.eventRecord("heartbeat", wire.TaskStateWorking, &at, nil))
			if err := run.commit(ctx, pending...); err != nil {
				return run.abandon(err)
			}
			pending = nil
		}
	}
}

type activeRun struct {
	receiver *Receiver
	producer *kgo.Client
	task     *wire.Task
	taskHash string
	attempt  int64
	// adapter holds the run's platform material, released at the end.
	adapter Named
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
		run.receiver.logger.Printf("run %s: fenced by a newer holder", run.task.RunID)
		return 0
	}
	run.receiver.logger.Printf("run %s: write failed: %v", run.task.RunID, err)
	return kgo.AckRelease
}

func (run *activeRun) stopAndFail(ctx context.Context, handle Handle, reason string) kgo.AckStatus {
	if err := run.adapter.Adapter.Stop(ctx, handle); err != nil {
		run.receiver.logger.Printf("run %s: stop: %v", run.task.RunID, err)
		return kgo.AckRelease
	}
	return run.fail(ctx, reason)
}

func (run *activeRun) fail(ctx context.Context, reason string) kgo.AckStatus {
	if err := run.commit(ctx, run.reply(wire.TaskStateFailed, reason), run.eventRecord("terminal", wire.TaskStateFailed, nil, &reason)); err != nil {
		return run.abandon(err)
	}
	run.release(ctx)
	return kgo.AckReject
}

func (run *activeRun) finish(ctx context.Context, handle Handle, pending []*kgo.Record, result agentResult) kgo.AckStatus {
	if err := run.adapter.Adapter.Stop(ctx, handle); err != nil {
		run.receiver.logger.Printf("run %s: stop: %v", run.task.RunID, err)
		return kgo.AckRelease
	}
	state, status := wire.TaskStateCompleted, kgo.AckAccept
	if result.IsError {
		state, status = wire.TaskStateFailed, kgo.AckReject
	}
	pending = append(pending, run.reply(state, result.Result), run.eventRecord("terminal", state, nil, nil))
	if err := run.commit(ctx, pending...); err != nil {
		return run.abandon(err)
	}
	run.release(ctx)
	return status
}

func (run *activeRun) release(ctx context.Context) {
	if err := run.adapter.Adapter.Release(ctx, run.task.RunID); err != nil {
		run.receiver.logger.Printf("run %s: release: %v", run.task.RunID, err)
	}
}

func (run *activeRun) eventRecord(kind string, state wire.TaskState, offset *int64, reason *string) *kgo.Record {
	return run.signed(wire.TopicEvent, wire.TypeEvent, run.receiver.event(run.task, run.taskHash, run.attempt, kind, state, offset, reason))
}

func (r *Receiver) event(task *wire.Task, taskHash string, attempt int64, kind string, state wire.TaskState, offset *int64, reason *string) *wire.Event {
	return &wire.Event{
		RunID: task.RunID, Attempt: attempt, TaskHash: taskHash, ReceiverID: r.config.ReceiverID,
		AtMS: time.Now().UnixMilli(), State: state, Kind: kind, LogOffset: offset, Reason: reason,
	}
}

func (run *activeRun) transcriptRecord(line Line) *kgo.Record {
	transcript := &wire.Transcript{RunID: run.task.RunID, Attempt: run.attempt, Seq: line.Offset}
	if len(line.Data) > maxLine {
		transcript.Pointer = &wire.TranscriptPointer{URL: "unstored:oversize", SHA256: trust.EnvelopeHash(line.Data), Size: int64(len(line.Data))}
	} else {
		data := wire.Bytes(line.Data)
		transcript.Line = &data
	}
	return run.signed(wire.TopicTranscript, wire.TypeTranscript, transcript)
}

func (run *activeRun) reply(state wire.TaskState, text string) *kgo.Record {
	if text == "" {
		text = string(state)
	}
	reply := &wire.Reply{
		RunID: run.task.RunID, Attempt: run.attempt, TaskHash: run.taskHash, CorrelationID: run.task.CorrelationID,
		State: state, AtMS: time.Now().UnixMilli(),
		Artifacts: []wire.Artifact{{ArtifactID: "result", Parts: []wire.Part{{Text: text}}}},
	}
	return run.signed(run.task.ReplyTopic, wire.TypeReply, reply)
}

func (run *activeRun) signed(topic string, recordType wire.RecordType, payload any) *kgo.Record {
	value, err := sign(recordType, payload, run.receiver.signing)
	if err != nil {
		panic(fmt.Sprintf("sign %s: %v", recordType, err)) // payloads are built here; invalid ones are programming errors
	}
	return &kgo.Record{Topic: topic, Key: []byte(run.task.RunID), Value: value}
}

// lastOffset returns the highest heartbeat log offset this run reached under an earlier attempt.
func (r *Receiver) lastOffset(ctx context.Context, runID string) (int64, error) {
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
		if event, ok := verified.(*wire.Event); err == nil && ok && event.LogOffset != nil && *event.LogOffset > highest {
			highest = *event.LogOffset
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
}

// resultLine recognises the final stream-json record; stdout is data, never control.
func resultLine(data []byte) (agentResult, bool) {
	var result agentResult
	if json.Unmarshal(data, &result) != nil || result.Type != "result" {
		return agentResult{}, false
	}
	return result, true
}

func prompt(task *wire.Task) string {
	var parts []string
	for _, part := range task.Message.Parts {
		parts = append(parts, part.Text)
	}
	return strings.Join(parts, "\n")
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
