package receiver

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	maxLine        = 512 << 10
	maxAgentResult = 16 << 10
	maxRunDuration = time.Hour // receiver cap on any task's run deadline (ADR-0013)
	readTimeout    = 30 * time.Second
	maxHandle      = 4 << 10
	budgetReason   = "delivery budget exhausted"
)

// Intervals are variables so tests can shorten them; renew stays below the 30 s default lock.
var (
	renewInterval     = 10 * time.Second
	advertiseInterval = 30 * time.Second
	refreshInterval   = 15 * time.Second
	// cancelReadInterval is how often the receiver reads the pool's events, once for every run it holds.
	cancelReadInterval = 10 * time.Second
	// stateCheckInterval is how often a running turn checks the cancels and denial the receiver holds.
	stateCheckInterval = time.Second
	// shutdownTimeout bounds the final transcript commit and ack flush after the receiver is told to stop.
	shutdownTimeout = 5 * time.Second
	// reconcileInterval separates the Lookups that resolve an ambiguous Dispatch.
	reconcileInterval = time.Second
	// purgeDeadline bounds the Purge retries of a start failure.
	purgeDeadline = 5 * time.Minute
)

// claimLoop holds one slot: a dedicated share client per claimed record,
// because renews do not survive a poll and held records are tied to the connection.
func (r *Receiver) claimLoop(ctx context.Context) {
	for ctx.Err() == nil {
		if r.blocked() {
			sleep(ctx, time.Second)
			continue
		}
		if err := r.claimOne(ctx); err != nil && ctx.Err() == nil {
			r.deny(err, capClaim)
			r.logger.Printf("claim: %v", err)
			sleep(ctx, 2*time.Second)
		}
	}
}

func (r *Receiver) claimOne(ctx context.Context) error {
	share, err := kgo.NewClient(append(append([]kgo.Opt(nil), r.kafkaOpt...),
		kgo.ConsumeTopics(admin.PoolTasksTopic(r.config.Pool)),
		kgo.ShareGroup(admin.PoolGroup(r.config.Pool)),
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
		// A denied receiver must not acquire a task: each release raises its delivery count.
		go func() {
			for pollCtx.Err() == nil && !r.blocked() {
				sleep(pollCtx, 100*time.Millisecond)
			}
			cancel()
		}()
		fetches := share.PollRecords(pollCtx, 1)
		cancel()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if r.blocked() && len(fetches.Records()) == 0 {
			return nil
		}
		if err := fetches.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("poll: %w", err)
		}
		// A poll without an error is the claim attempt that re-tests a share group denial.
		r.restore(capClaim)
		if records := fetches.Records(); len(records) > 0 {
			record = records[0]
		}
	}
	if r.blocked() {
		record.Ack(kgo.AckRelease)
		return flush()
	}
	admitted, ack := r.admit(ctx, record)
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
	task     *wire.Submission
	taskHash string
	adapter  Named
	history  []wire.Message
	git      *wire.GitContext
	// cancelled is set when a host of the run's tenant cancelled the run; it ends CANCELED without a dispatch.
	cancelled bool
}

func taskHash(encoded []byte) string {
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

// admit applies the committed-terminal check, host authorisation, the placement against the pool's
// terms and this receiver's offers, cancels, the run's turn order, deadlines and part checks, and
// builds the prompt from the run's history. A nil admission carries the acknowledgement.
func (r *Receiver) admit(ctx context.Context, record *kgo.Record) (*admission, kgo.AckStatus) {
	hash := taskHash(record.Value)
	// Released even at the delivery budget: without the events, a terminal could repeat a committed one.
	events, err := r.readEvents(ctx)
	if err != nil {
		r.logger.Printf("read pool events: %v", err)
		return nil, kgo.AckRelease
	}
	if events.committedTerminal(string(record.Key), hash) {
		return nil, kgo.AckAccept
	}
	var task wire.Submission
	if err := r.authorizeHost(ctx, record.Value, func(state admin.State, hosts signed.Keys) error {
		task, err = admin.AuthorizeSubmission(state, hosts, record.Value, r.config.Pool)
		return err
	}); err != nil || task.Wherry.Run != string(record.Key) {
		return nil, kgo.AckReject
	}
	run, attempt := task.Wherry.Run, attemptOf(record)
	reject := func(reason string) (*admission, kgo.AckStatus) {
		r.logger.Printf("rejected %s: %s", run, reason)
		if err := r.publishRejection(ctx, &task, hash, attempt, reason); err != nil {
			r.logger.Printf("publish rejection for %s: %v", run, err)
			return nil, kgo.AckRelease
		}
		return nil, kgo.AckReject
	}
	// giveBack is returnTask for a task no run holds yet: the terminal goes out with the echo under the
	// admission transactional ID.
	giveBack := func() (*admission, kgo.AckStatus) {
		if !r.budgetSpent(attempt) {
			return nil, kgo.AckRelease
		}
		held := &activeRun{receiver: r, task: &task, taskHash: hash, history: []wire.Message{task.Message}, attempt: attempt}
		echo := &kgo.Record{Topic: admin.TenantTopic(task.Wherry.Tenant), Key: []byte(run), Value: r.sign(r.echo(&task, hash))}
		if err := r.admissionCommit(ctx, append([]*kgo.Record{echo}, held.budgetRecords(nil)...)...); err != nil {
			r.logger.Printf("publish terminal for %s: %v", run, err)
			return nil, kgo.AckRelease
		}
		return nil, kgo.AckReject
	}
	// Nothing is written under the run key before its owner is known; unreadable turns leave it unknown.
	all, err := r.readTurns(ctx, run)
	if err != nil {
		r.logger.Printf("read run %s turns: %v", run, err)
		return nil, kgo.AckRelease
	}
	if foreignRun(all, events.statuses[run], task.Wherry.Tenant) {
		return reject("run ID in use")
	}
	state, _, _ := r.snapshot()
	terms := state.Pools[r.config.Pool]
	placement := task.Wherry.Placement
	if !slices.ContainsFunc(terms.Agents, func(agent admin.AgentTerms) bool { return agent.Name == placement.Agent }) {
		return reject("agent is outside the pool terms")
	}
	adapter, offered := r.offer(placement)
	if !offered {
		r.logger.Printf("run %s: no current offer for agent %s satisfying its selector", run, placement.Agent)
		if err := r.admissionCommit(ctx, r.startFailure(&task, attempt, "none", "offer-unavailable")); err != nil {
			r.logger.Printf("publish start failure for %s: %v", run, err)
		}
		return giveBack()
	}
	submissions := bindTurns(all, &task, events)
	cancelledTurn, cancelled := r.cancelledTurn(ctx, events, run, task.Wherry.Tenant)
	cancelled = cancelled && cancelledTurn <= task.Wherry.Turn
	if !cancelled {
		if reason := continuationRejection(events, &task, hash, submissions); reason != "" {
			return reject(reason)
		}
	}
	history := []wire.Message{task.Message}
	if task.Wherry.Turn > 1 {
		if history, err = turns(submissions, &task); err != nil {
			r.logger.Printf("run %s: continuation history: %v", run, err)
			return reject("continuation history unavailable")
		}
	}
	admitted := &admission{task: &task, taskHash: hash, adapter: adapter, history: history, cancelled: cancelled}
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
	git, supported := gitContext(history, adapter.Git)
	if !supported {
		return reject("unsupported message part")
	}
	admitted.git = git
	return admitted, 0
}

// foreignRun reports that a run ID belongs to a tenant other than tenant: the tenant of its earliest
// submission a live host's key verifies, or of its earliest status a holder wrote. Receivers cannot read
// an ended host's key topic, so for a run whose host has ended the status is the only source.
// Every non-rejected status counts as a claim, so admission writes nothing else under the run key before this check.
func foreignRun(all []runSubmission, statuses []wire.RunStatus, tenant string) bool {
	if len(all) > 0 && all[0].record.Wherry.Tenant != tenant {
		return true
	}
	held := slices.IndexFunc(statuses, func(status wire.RunStatus) bool { return status.Wherry.Kind != "rejected" })
	return held >= 0 && statuses[held].Wherry.Tenant != tenant
}

// gitContext returns the run's git context; false means a non-text part other than one git part the adapter delivers.
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

// publishRejection writes the echo, so every host of the tenant can find the run's pool, and the rejected
// status on the pool's events and the feed, in one admission transaction.
func (r *Receiver) publishRejection(ctx context.Context, task *wire.Submission, hash string, attempt int64, reason string) error {
	status := r.status(task, hash, attempt, "rejected", taskStatus(wire.TaskStateRejected, reason), nil)
	echo := &kgo.Record{Topic: admin.TenantTopic(task.Wherry.Tenant), Key: []byte(task.Wherry.Run), Value: r.sign(r.echo(task, hash))}
	return r.admissionCommit(ctx, append([]*kgo.Record{echo}, r.statusRecords(task, status)...)...)
}

func (r *Receiver) echo(task *wire.Submission, hash string) *wire.Echo {
	return &wire.Echo{
		Receiver: r.id, Tenant: task.Wherry.Tenant, Pool: r.config.Pool, Run: task.Wherry.Run, Turn: task.Wherry.Turn, Placement: task.Wherry.Placement,
		TaskHash: hash, MessageID: task.Message.MessageID, DeadlineMS: task.Wherry.DeadlineMS, QueueDeadlineMS: task.Wherry.QueueDeadlineMS,
	}
}

// admissionTransactionalID is this receiver's own transactional ID inside its pool's prefix, for the writes
// it makes while holding no run; using a run's ID would fence that run's holder.
func (r *Receiver) admissionTransactionalID() string {
	return admin.TransactionalPrefix(r.config.Pool) + "admission." + r.id
}

// admissionCommit writes records in one transaction under the admission transactional ID; an
// authorisation error enters denial.
func (r *Receiver) admissionCommit(ctx context.Context, records ...*kgo.Record) error {
	r.admissionMu.Lock()
	defer r.admissionMu.Unlock()
	producer, err := kgo.NewClient(append(append([]kgo.Opt(nil), r.kafkaOpt...), kgo.TransactionalID(r.admissionTransactionalID()))...)
	if err != nil {
		return err
	}
	defer producer.Close()
	if err := r.transact(ctx, producer, records...); err != nil {
		r.deny(err, capWrite)
		return err
	}
	return nil
}

// probeWrite proves the write and transactional ID ACLs with an aborted admission transaction on the
// pool's events, which no read_committed reader sees.
func (r *Receiver) probeWrite(ctx context.Context) error {
	r.admissionMu.Lock()
	defer r.admissionMu.Unlock()
	producer, err := kgo.NewClient(append(append([]kgo.Opt(nil), r.kafkaOpt...), kgo.TransactionalID(r.admissionTransactionalID()))...)
	if err != nil {
		return err
	}
	defer producer.Close()
	if err := producer.BeginTransaction(); err != nil {
		return err
	}
	if err := producer.ProduceSync(ctx, &kgo.Record{Topic: admin.PoolEventsTopic(r.config.Pool), Key: []byte("probe")}).FirstErr(); err != nil {
		_ = producer.EndTransaction(ctx, kgo.TryAbort)
		return err
	}
	return producer.EndTransaction(ctx, kgo.TryAbort)
}

// commitTransaction is a variable so tests can lose the answer to a commit the broker applied, which
// kfake cannot do: its EndTxn faults all fail before the commit.
var commitTransaction = func(ctx context.Context, producer *kgo.Client) error {
	return producer.EndTransaction(ctx, kgo.TryCommit)
}

// transact commits records in one transaction on producer. An EndTransaction error leaves the outcome
// ambiguous, so the transaction's record on the pool's events is read back: present means committed.
func (r *Receiver) transact(ctx context.Context, producer *kgo.Client, records ...*kgo.Record) error {
	if len(records) == 0 {
		return nil
	}
	if err := producer.BeginTransaction(); err != nil {
		return err
	}
	if err := producer.ProduceSync(ctx, records...).FirstErr(); err != nil {
		_ = producer.EndTransaction(ctx, kgo.TryAbort)
		return err
	}
	err := commitTransaction(ctx, producer)
	if err == nil || errors.Is(err, kerr.ProducerFenced) || errors.Is(err, kerr.InvalidProducerEpoch) || authorisationError(err) {
		return err
	}
	index := slices.IndexFunc(records, func(record *kgo.Record) bool { return record.Topic == admin.PoolEventsTopic(r.config.Pool) })
	if index < 0 {
		return err
	}
	readCtx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	events, readErr := admin.ReadTopic(readCtx, r.base, admin.PoolEventsTopic(r.config.Pool))
	if readErr != nil {
		r.logger.Printf("read back transaction: %v", readErr)
		return err
	}
	if slices.ContainsFunc(events, func(event *kgo.Record) bool { return bytes.Equal(event.Value, records[index].Value) }) {
		r.logger.Printf("transaction read back as committed after: %v", err)
		return nil
	}
	return err
}

// startFailure names the adapter the task was routed to, "none" when it was routed to none.
func (r *Receiver) startFailure(task *wire.Submission, attempt int64, adapter, class string) *kgo.Record {
	failure := &wire.StartFailure{Wherry: r.binding(task, attempt), Adapter: adapter, Agent: task.Wherry.Placement.Agent, Class: class}
	return &kgo.Record{Topic: admin.PoolEventsTopic(r.config.Pool), Key: []byte(task.Wherry.Run), Value: r.sign(failure)}
}

type runSubmission struct {
	record *wire.Submission
	hash   string
	// current is set when the submission verifies against its host's current key, not only a retired one.
	current bool
}

// readTurns reads the run's authorised submissions from the pool's task topic in log order.
func (r *Receiver) readTurns(ctx context.Context, run string) ([]runSubmission, error) {
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	records, err := admin.ReadTopic(ctx, r.base, admin.PoolTasksTopic(r.config.Pool))
	if err != nil {
		r.deny(err, capRead)
		return nil, err
	}
	state, _, hosts := r.snapshot()
	var result []runSubmission
	for _, record := range records {
		if string(record.Key) != run {
			continue
		}
		submission, err := admin.AuthorizeEarlierTurn(state, hosts, record.Value, r.config.Pool)
		if err == nil && submission.Wherry.Run == run {
			_, currentErr := admin.AuthorizeSubmission(state, hosts, record.Value, r.config.Pool)
			result = append(result, runSubmission{record: &submission, hash: taskHash(record.Value), current: currentErr == nil})
		}
	}
	return result, nil
}

// bindTurns keeps, for each turn, the submission carrying the tenant and placement of expected that a pool member's
// status names by hash, or else the first one; a later submission cannot displace a turn a member already ran. A
// submission that verifies only against a retired key counts only when a member's status names it: a leaked retired
// key must not pre-empt a turn no member has run.
func bindTurns(all []runSubmission, expected *wire.Submission, events poolEvents) map[int64]runSubmission {
	ran := map[string]bool{}
	for _, status := range events.statuses[expected.Wherry.Run] {
		ran[status.Wherry.TaskHash] = true
	}
	result := make(map[int64]runSubmission)
	for _, submission := range all {
		if submission.record.Wherry.Tenant != expected.Wherry.Tenant || !reflect.DeepEqual(submission.record.Wherry.Placement, expected.Wherry.Placement) {
			continue
		}
		if !submission.current && !ran[submission.hash] {
			continue
		}
		turn := submission.record.Wherry.Turn
		if previous, duplicate := result[turn]; !duplicate || ran[submission.hash] && !ran[previous.hash] {
			result[turn] = submission
		}
	}
	return result
}

// continuationRejection checks the task against the run's turns and its pool members' statuses;
// it returns the fixed rejection reason, or "".
func continuationRejection(events poolEvents, task *wire.Submission, hash string, submissions map[int64]runSubmission) string {
	if submissions[task.Wherry.Turn].hash != hash {
		if task.Wherry.Turn == 1 {
			return "duplicate submission"
		}
		return "duplicate continuation"
	}
	var bound []wire.RunStatus
	for _, status := range events.statuses[task.Wherry.Run] {
		if submission, found := submissions[status.Wherry.Turn]; found && status.Wherry.TaskHash == submission.hash {
			bound = append(bound, status)
		}
	}
	latest := latestStatus(bound, task.Wherry.Tenant)
	if latest != nil && (latest.Wherry.Turn > task.Wherry.Turn ||
		latest.Wherry.Turn == task.Wherry.Turn && (latest.Wherry.Kind == "interrupted" || latest.Wherry.Kind == "terminal")) {
		return "stale submission"
	}
	if task.Wherry.Turn == 1 || latest != nil && latest.Wherry.Turn == task.Wherry.Turn {
		return ""
	}
	if latest == nil || latest.Wherry.Kind != "interrupted" || !wire.Interrupted(latest.StatusUpdate.Status.State) {
		return "task is not awaiting continuation"
	}
	if task.Wherry.Turn != latest.Wherry.Turn+1 {
		return "continuation turn out of order"
	}
	return ""
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

// newProducer is the run's transactional producer: an adopter initialising the same ID fences this holder (ADR-0010).
func (r *Receiver) newProducer(run string) (*kgo.Client, error) {
	return kgo.NewClient(append(append([]kgo.Opt(nil), r.kafkaOpt...), kgo.TransactionalID(admin.TransactionalPrefix(r.config.Pool)+run))...)
}

// execute runs one task; it returns the final share acknowledgement, or 0 when fenced.
func (r *Receiver) execute(ctx context.Context, share *kgo.Client, record *kgo.Record, admitted *admission, attempt int64) kgo.AckStatus {
	task := admitted.task
	runID := task.Wherry.Run
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

	producer, err := r.newProducer(runID)
	if err != nil {
		r.logger.Printf("run %s: producer: %v", runID, err)
		return kgo.AckRelease
	}
	defer producer.Close()
	run := &activeRun{receiver: r, producer: producer, task: task, taskHash: admitted.taskHash, history: admitted.history, attempt: attempt, adapter: admitted.adapter}

	if err := run.commit(ctx, append([]*kgo.Record{run.feed(r.echo(task, admitted.taskHash))}, run.statusRecords("claimed", taskStatus(wire.TaskStateWorking, ""), nil, nil)...)...); err != nil {
		return run.abandon(err)
	}
	offset := int64(0)
	if attempt > 1 {
		offset, err = r.lastOffset(runCtx, task, run.taskHash)
		if err != nil {
			r.logger.Printf("run %s: read events: %v", runID, err)
			return run.returnTask(ctx, nil)
		}
	}
	var handle Handle
	adopted, neverStarted := false, false
	if attempt > 1 {
		// Offers can change between attempts, so the run may live in an adapter other than the matched one.
		for _, named := range r.adapters {
			job, err := named.Adapter.Lookup(runCtx, runID, task.Wherry.Turn)
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
				return run.returnTask(ctx, nil)
			}
			if job.State == JobAbsent {
				continue
			}
			// A job of another adapter of this receiver on the same backend is that adapter's Lookup to judge.
			if job.Adapter != named.Name && slices.ContainsFunc(r.adapters, func(other Named) bool { return other.Name == job.Adapter }) {
				continue
			}
			if job.Adapter != named.Name || job.Agent != task.Wherry.Placement.Agent {
				if err := named.Adapter.Stop(ctx, job.Handle); err != nil {
					r.logger.Printf("run %s: stop: %T", runID, err)
					return run.returnTask(ctx, nil)
				}
				if err := named.Adapter.Purge(ctx, runID, task.Wherry.Turn); err != nil {
					r.logger.Printf("run %s: purge: %T", runID, err)
					return run.returnTask(ctx, nil)
				}
				r.logger.Printf("run %s: purged a job from adapter %q agent %q", runID, job.Adapter, job.Agent)
				continue
			}
			r.logger.Printf("run %s: found attempt %d's job in adapter %s", runID, job.Attempt, named.Name)
			handle, run.adapter = job.Handle, named
			adopted, neverStarted = job.State != JobNeverStarted, job.State == JobNeverStarted
			break
		}
	}
	if admitted.cancelled {
		if adopted {
			run.job = handle.Name
			return run.stopAndCancel(ctx, handle, nil)
		}
		return run.cancel(ctx, nil)
	}
	if neverStarted {
		return run.startFailure(ctx, "platform")
	}
	if !adopted {
		named := run.adapter
		credential, outcome, err := r.credential(runCtx, task.Wherry.Tenant, task.Wherry.Placement.Agent, named.Credential)
		switch {
		case err != nil:
			r.logger.Printf("run %s: read seals: %v", runID, err)
			return run.returnTask(ctx, nil)
		case outcome == credentialWithPeer:
			return run.startFailure(ctx, "seal-unavailable")
		case outcome == credentialMissing:
			return run.interrupt(ctx, named.Credential)
		}
		dispatched, dispatchedHandle, err := named.Adapter.Dispatch(runCtx, Run{
			ID: runID, Adapter: named.Name, Agent: task.Wherry.Placement.Agent, Turn: task.Wherry.Turn, Attempt: attempt,
			Prompt: []byte(prompt(admitted.history)), Credential: credential, Git: admitted.git,
		})
		switch {
		case err != nil && runCtx.Err() != nil && ctx.Err() == nil:
			return run.fail(ctx, "timeout", nil)
		case errors.Is(err, ErrGitContext):
			// Git error text can echo URLs, so only the type is logged.
			r.logger.Printf("run %s: git context unavailable: %T", runID, err)
			return run.fail(ctx, ErrGitContext.Error(), nil)
		case err != nil:
			r.logger.Printf("run %s: dispatch failed: %T", runID, err)
			dispatched = OutcomeUnknown
		}
		switch dispatched {
		case OutcomeCreated:
		case OutcomeNothingCreated:
			r.logger.Printf("run %s: adapter %s created no job", runID, named.Name)
			return run.startFailure(ctx, "dispatch")
		default:
			job, resolved := run.reconcile(runCtx)
			switch {
			case !resolved:
				r.logger.Printf("run %s: dispatch unresolved", runID)
				return run.returnTask(ctx, nil)
			case job.State == JobAbsent:
				return run.startFailure(ctx, "dispatch")
			case job.State == JobNeverStarted:
				return run.startFailure(ctx, "platform")
			}
			dispatchedHandle = job.Handle
		}
		handle = dispatchedHandle
		run.labels = admitted.adapter.Labels
	}
	if len(handle.Data) > maxHandle {
		r.logger.Printf("run %s: adapter %s returned a handle over %d bytes", runID, run.adapter.Name, maxHandle)
		if err := run.adapter.Adapter.Stop(ctx, handle); err != nil {
			r.logger.Printf("run %s: stop: %T", runID, err)
		}
		return run.returnTask(ctx, nil)
	}
	run.job = handle.Name
	r.logger.Printf("run %s: attempt %d in adapter %s with %s (%s)", runID, attempt, run.adapter.Name, wire.TermsText(admitted.adapter.Labels), handle.Name)
	var startedMetadata jsontext.Value
	if handle.Commit != "" {
		startedMetadata, _ = json.Marshal(wire.CommitMetadata{GitCommit: handle.Commit}) // a string always encodes
	}
	if err := run.commit(ctx, run.statusRecords("started", taskStatus(wire.TaskStateWorking, ""), &offset, startedMetadata)...); err != nil {
		return run.abandon(err)
	}

	followCtx, stopFollow := context.WithCancel(runCtx)
	defer stopFollow()
	lines := make(chan Line, 64)
	type followed struct {
		terminal Terminal
		err      error
	}
	ended := make(chan followed, 1)
	go func() {
		terminal, err := run.adapter.Adapter.Follow(followCtx, handle, offset, lines)
		ended <- followed{terminal, err}
	}()

	heartbeat := time.NewTicker(renewInterval)
	defer heartbeat.Stop()
	checks := time.NewTicker(stateCheckInterval)
	defer checks.Stop()
	var pending []Line
	position := offset
	take := func(line Line) (agentResult, bool) {
		pending = append(pending, line)
		position = line.Next
		return resultLine(line.Data)
	}
	// shutdown commits what is pending under its own deadline, since ctx is already done, and returns the task.
	// It ignores the delivery budget: the receiver is going away and an adopter continues the run.
	shutdown := func() kgo.AckStatus {
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
		defer cancel()
		if err := run.commit(shutdownCtx, run.chunks(pending, false)...); err != nil {
			return run.abandon(err)
		}
		return kgo.AckRelease
	}
	// stopHeld ends the run when the receiver holds its cancel or a denial lasting one lock interval.
	stopHeld := func() (kgo.AckStatus, bool) {
		r.mu.Lock()
		cancelledTurn, cancelled := r.cancelled[runKey{task.Wherry.Tenant, runID}]
		r.mu.Unlock()
		switch {
		case r.stopForDenial():
			stopFollow()
			return run.deny(ctx, handle, pending), true
		case cancelled && cancelledTurn <= task.Wherry.Turn:
			stopFollow()
			return run.stopAndCancel(ctx, handle, pending), true
		}
		return 0, false
	}
	for {
		select {
		case line := <-lines:
			if result, ok := take(line); ok {
				stopFollow()
				return run.finish(ctx, handle, pending, result)
			}
		case end := <-ended:
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
			case end.err != nil:
				// A dropped stream does not mean the task ended; stop it rather than leave it running.
				r.logger.Printf("run %s: follow failed: %T", runID, end.err)
				return run.stopAndFail(ctx, handle, "log stream failed", pending)
			case !end.terminal.Started:
				r.logger.Printf("run %s: job never started (%s)", runID, end.terminal.Class)
				return run.startFailure(ctx, "platform")
			default:
				r.logger.Printf("run %s: agent ended with exit code %d signal %q on node %s", runID, end.terminal.ExitCode, end.terminal.Signal, end.terminal.Node)
				return run.stopAndFail(ctx, handle, "agent ended without a result", pending)
			}
		case <-ctx.Done():
			stopFollow()
			return shutdown()
		case <-checks.C:
			if status, stop := stopHeld(); stop {
				return status
			}
		case <-heartbeat.C:
			if ctx.Err() != nil {
				stopFollow()
				return shutdown()
			}
			// The heartbeat can win the select against a due check; a held stop still comes first.
			if status, stop := stopHeld(); stop {
				return status
			}
			// The newest line stays pending so the terminal transaction always holds the last chunk;
			// the heartbeat offset is where that line starts, so an adopter reads it again.
			at, held := position, 0
			if len(pending) > 0 {
				at, held = pending[len(pending)-1].Offset, 1
			}
			// Heartbeats go to the pool's events alone: adoption needs them, hosts follow the chunks.
			records := append(run.chunks(pending[:len(pending)-held], false), run.events(run.status("heartbeat", taskStatus(wire.TaskStateWorking, ""), &at, nil)))
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
	task     *wire.Submission
	taskHash string
	history  []wire.Message
	attempt  int64
	// adapter holds the run's platform material, released at the end.
	adapter Named
	// labels is set when this attempt dispatched the job; an adopted job's adapter labels are unknown.
	labels map[string]string
	job    string
}

func (run *activeRun) commit(ctx context.Context, records ...*kgo.Record) error {
	return run.receiver.transact(ctx, run.producer, records...)
}

// abandon stops all activity for a run whose writes failed; a fenced holder leaves the run to its adopter.
func (run *activeRun) abandon(err error) kgo.AckStatus {
	if errors.Is(err, kerr.InvalidProducerEpoch) || errors.Is(err, kerr.ProducerFenced) {
		run.receiver.logger.Printf("run %s: fenced by a newer holder", run.task.Wherry.Run)
		return 0
	}
	run.receiver.deny(err, capWrite)
	run.receiver.logger.Printf("run %s: write failed: %v", run.task.Wherry.Run, err)
	return kgo.AckRelease
}

func (run *activeRun) stopAndFail(ctx context.Context, handle Handle, reason string, pending []Line) kgo.AckStatus {
	if err := run.adapter.Adapter.Stop(ctx, handle); err != nil {
		run.receiver.logger.Printf("run %s: stop: %T", run.task.Wherry.Run, err)
		return run.returnTask(ctx, pending)
	}
	return run.fail(ctx, reason, pending)
}

func (run *activeRun) stopAndCancel(ctx context.Context, handle Handle, pending []Line) kgo.AckStatus {
	if err := run.adapter.Adapter.Stop(ctx, handle); err != nil {
		run.receiver.logger.Printf("run %s: stop: %T", run.task.Wherry.Run, err)
		return run.returnTask(ctx, pending)
	}
	return run.cancel(ctx, pending)
}

// deny stops a run on a denied receiver and releases its task to the pool; the terminal status is best effort.
func (run *activeRun) deny(ctx context.Context, handle Handle, pending []Line) kgo.AckStatus {
	if err := run.adapter.Adapter.Stop(ctx, handle); err != nil {
		run.receiver.logger.Printf("run %s: stop: %T", run.task.Wherry.Run, err)
		return kgo.AckRelease
	}
	const reason = "receiver denied"
	status := taskStatus(wire.TaskStateFailed, reason)
	if err := run.commit(ctx, append(append(run.chunks(pending, true), run.result(status, reason, nil)), run.statusRecords("terminal", status, nil, nil)...)...); err != nil {
		run.receiver.logger.Printf("run %s: publish denied status: %v", run.task.Wherry.Run, err)
	}
	run.release(ctx)
	return kgo.AckRelease
}

func (run *activeRun) fail(ctx context.Context, reason string, pending []Line) kgo.AckStatus {
	return run.end(ctx, taskStatus(wire.TaskStateFailed, reason), reason, nil, pending, kgo.AckReject)
}

func (run *activeRun) cancel(ctx context.Context, pending []Line) kgo.AckStatus {
	return run.end(ctx, taskStatus(wire.TaskStateCanceled, cancelReason), cancelReason, nil, pending, kgo.AckAccept)
}

const cancelReason = "canceled by user"

// interrupt waits the run for its tenant's credential: AUTH_REQUIRED with its result, then the task is acknowledged.
func (run *activeRun) interrupt(ctx context.Context, credential string) kgo.AckStatus {
	reason := "credential " + credential + " required"
	status := taskStatus(wire.TaskStateAuthRequired, reason)
	metadata, _ := json.Marshal(wire.CredentialMetadata{Credential: credential}) // a string always encodes
	if err := run.commit(ctx, append([]*kgo.Record{run.result(status, reason, nil)}, run.statusRecords("interrupted", status, nil, metadata)...)...); err != nil {
		return run.abandon(err)
	}
	run.release(ctx)
	return kgo.AckAccept
}

// startFailure purges the turn's job and returns the task to the pool for a member that can start it.
func (run *activeRun) startFailure(ctx context.Context, class string) kgo.AckStatus {
	if err := run.commit(ctx, run.receiver.startFailure(run.task, run.attempt, run.adapter.Name, class)); err != nil {
		return run.abandon(err)
	}
	if err := run.purge(ctx); err != nil {
		// Shutdown: the next holder's Lookup finds the job again.
		if ctx.Err() != nil {
			return kgo.AckRelease
		}
		return run.fail(ctx, "start failed and the job could not be removed", nil)
	}
	run.release(ctx)
	return run.returnTask(ctx, nil)
}

// purge retries Purge every renew interval, while the renewer holds the claim, until purgeDeadline.
func (run *activeRun) purge(ctx context.Context) error {
	deadline := time.Now().Add(purgeDeadline)
	for {
		err := run.adapter.Adapter.Purge(ctx, run.task.Wherry.Run, run.task.Wherry.Turn)
		if err == nil || ctx.Err() != nil || !time.Now().Before(deadline) {
			return err
		}
		run.receiver.logger.Printf("run %s: purge: %T", run.task.Wherry.Run, err)
		sleep(ctx, renewInterval)
	}
}

// budgetSpent reports an attempt at which a released task could reach the broker's delivery limit and be
// archived with no terminal record.
func (r *Receiver) budgetSpent(attempt int64) bool {
	state, _, _ := r.snapshot()
	return attempt >= int64(state.Pools[r.config.Pool].DeliveryLimit)-1
}

// reconcile looks up the job an ambiguous Dispatch may have created; false means it stays unresolved.
func (run *activeRun) reconcile(ctx context.Context) (Job, bool) {
	for i := 0; i < 3; i++ {
		if i > 0 {
			sleep(ctx, reconcileInterval)
		}
		job, err := run.adapter.Adapter.Lookup(ctx, run.task.Wherry.Run, run.task.Wherry.Turn)
		if err == nil {
			return job, true
		}
		run.receiver.logger.Printf("run %s: lookup in adapter %s: %T", run.task.Wherry.Run, run.adapter.Name, err)
	}
	return Job{}, false
}

// returnTask hands the task back to the pool with any job and its material in place, so the next holder's
// Lookup finds them. Once the delivery budget is spent it ends the run FAILED instead, and leaves the
// material to the adapter's sweep.
func (run *activeRun) returnTask(ctx context.Context, pending []Line) kgo.AckStatus {
	if !run.receiver.budgetSpent(run.attempt) {
		return kgo.AckRelease
	}
	if err := run.commit(ctx, run.budgetRecords(pending)...); err != nil {
		return run.abandon(err)
	}
	return kgo.AckReject
}

func (run *activeRun) budgetRecords(pending []Line) []*kgo.Record {
	status := taskStatus(wire.TaskStateFailed, budgetReason)
	return append(append(run.chunks(pending, true), run.result(status, budgetReason, nil)), run.statusRecords("terminal", status, nil, nil)...)
}

func (run *activeRun) finish(ctx context.Context, handle Handle, pending []Line, result agentResult) kgo.AckStatus {
	// A result committed after access lapsed is untrusted, so acknowledging it would lose the run.
	if run.receiver.stopForDenial() {
		return run.deny(ctx, handle, pending)
	}
	if err := run.adapter.Adapter.Stop(ctx, handle); err != nil {
		run.receiver.logger.Printf("run %s: stop: %T", run.task.Wherry.Run, err)
		// At the budget the agent's result stands, and the job is left to the adapter's sweep.
		if !run.receiver.budgetSpent(run.attempt) {
			return kgo.AckRelease
		}
	}
	state, ack := wire.TaskStateCompleted, kgo.AckAccept
	if result.IsError {
		state, ack = wire.TaskStateFailed, kgo.AckReject
	}
	return run.end(ctx, taskStatus(state, ""), result.Result, result.metadata, pending, ack)
}

// end commits the remaining transcript, the result and the terminal status in one transaction.
func (run *activeRun) end(ctx context.Context, status wire.TaskStatus, result string, resultMetadata jsontext.Value, pending []Line, ack kgo.AckStatus) kgo.AckStatus {
	records := append(append(run.chunks(pending, true), run.result(status, result, resultMetadata)), run.statusRecords("terminal", status, nil, nil)...)
	if err := run.commit(ctx, records...); err != nil {
		return run.abandon(err)
	}
	run.release(ctx)
	return ack
}

func (run *activeRun) release(ctx context.Context) {
	if err := run.adapter.Adapter.Release(ctx, run.task.Wherry.Run, run.task.Wherry.Turn); err != nil {
		run.receiver.logger.Printf("run %s: release: %T", run.task.Wherry.Run, err)
	}
}

func (run *activeRun) status(kind string, status wire.TaskStatus, offset *int64, metadata jsontext.Value) *wire.RunStatus {
	value := run.receiver.status(run.task, run.taskHash, run.attempt, kind, status, offset)
	value.StatusUpdate.Metadata = metadata
	return value
}

// statusRecords dual-writes a status to the pool's events and the tenant's feed.
func (run *activeRun) statusRecords(kind string, status wire.TaskStatus, offset *int64, metadata jsontext.Value) []*kgo.Record {
	return run.receiver.statusRecords(run.task, run.status(kind, status, offset, metadata))
}

func (r *Receiver) statusRecords(task *wire.Submission, status *wire.RunStatus) []*kgo.Record {
	value := r.sign(status)
	return []*kgo.Record{
		{Topic: admin.PoolEventsTopic(r.config.Pool), Key: []byte(task.Wherry.Run), Value: value},
		{Topic: admin.TenantTopic(task.Wherry.Tenant), Key: []byte(task.Wherry.Run), Value: value},
	}
}

func (run *activeRun) events(value wire.Record) *kgo.Record {
	return &kgo.Record{Topic: admin.PoolEventsTopic(run.receiver.config.Pool), Key: []byte(run.task.Wherry.Run), Value: run.receiver.sign(value)}
}

func (run *activeRun) feed(value wire.Record) *kgo.Record {
	return &kgo.Record{Topic: admin.TenantTopic(run.task.Wherry.Tenant), Key: []byte(run.task.Wherry.Run), Value: run.receiver.sign(value)}
}

func (r *Receiver) binding(task *wire.Submission, attempt int64) wire.ReceiverBinding {
	return wire.ReceiverBinding{Receiver: r.id, Tenant: task.Wherry.Tenant, Pool: r.config.Pool, Run: task.Wherry.Run, Turn: task.Wherry.Turn, Attempt: attempt}
}

// status builds a run status; a CANCELED terminal has the turn's fixed event ID, since every
// member that reads the cancel of an interrupted run writes it.
func (r *Receiver) status(task *wire.Submission, hash string, attempt int64, kind string, status wire.TaskStatus, offset *int64) *wire.RunStatus {
	eventID := randomID("event-")
	if kind == "terminal" && status.State == wire.TaskStateCanceled {
		eventID = fmt.Sprintf("%s/%d/canceled", task.Wherry.Run, task.Wherry.Turn)
	}
	return &wire.RunStatus{
		StatusUpdate: wire.TaskStatusUpdateEvent{TaskID: task.Wherry.Run, ContextID: task.Wherry.Run, Status: status},
		Wherry:       wire.StatusBinding{ReceiverBinding: r.binding(task, attempt), EventID: eventID, Kind: kind, TaskHash: hash, LogOffset: offset},
	}
}

func (r *Receiver) sign(value wire.Record) []byte {
	encoded, err := wire.SignRecord(value, r.signing)
	if err != nil {
		panic(fmt.Sprintf("sign %T: %v", value, err)) // records are built here; invalid ones are programming errors
	}
	return encoded
}

func randomID(prefix string) string {
	id := make([]byte, 16)
	rand.Read(id)
	return prefix + hex.EncodeToString(id)
}

// taskStatus stamps state with the current time; a reason becomes the status message.
func taskStatus(state wire.TaskState, reason string) wire.TaskStatus {
	status := wire.TaskStatus{State: state, Timestamp: time.Now().UTC().Format("2006-01-02T15:04:05.000Z")}
	if reason != "" {
		status.Message = &wire.Message{
			MessageID: randomID("msg-"), Role: wire.RoleAgent,
			Parts: []wire.Part{{Text: reason}}, Extensions: []string{wire.ExtensionURI},
		}
	}
	return status
}

// chunks are the transcript lines as artifact updates on the tenant's feed.
func (run *activeRun) chunks(lines []Line, last bool) []*kgo.Record {
	records := make([]*kgo.Record, 0, len(lines))
	for i, line := range lines {
		part := wire.Part{Text: string(line.Data), MediaType: "application/x-ndjson"}
		switch {
		case len(line.Data) > maxLine:
			metadata, _ := json.Marshal(wire.PointerMetadata{SHA256: taskHash(line.Data), Size: int64(len(line.Data))}) // a string and an integer always encode
			part = wire.Part{URL: "unstored:oversize", MediaType: part.MediaType, Metadata: metadata}
		case len(line.Data) == 0 || !utf8.Valid(line.Data):
			// A text part holds only non-empty UTF-8; other lines travel as exact bytes.
			part = wire.Part{Raw: append(wire.StdBytes{}, line.Data...), MediaType: part.MediaType}
		}
		chunk := &wire.Chunk{
			ArtifactUpdate: wire.TaskArtifactUpdateEvent{
				TaskID: run.task.Wherry.Run, ContextID: run.task.Wherry.Run,
				Artifact: wire.Artifact{ArtifactID: "transcript", Parts: []wire.Part{part}, Extensions: []string{wire.ExtensionURI}},
				// The stream's first chunk is the line at log offset zero; an adopter resumes past it.
				Append:    line.Offset > 0,
				LastChunk: last && i == len(lines)-1,
			},
			Wherry: wire.ChunkBinding{ReceiverBinding: run.receiver.binding(run.task, run.attempt), TaskHash: run.taskHash, Seq: line.Offset},
		}
		records = append(records, run.feed(chunk))
	}
	return records
}

// placement is the result's metadata: where its last turn ran.
type placement struct {
	ReceiverID string            `json:"receiver_id"`
	Pool       string            `json:"pool"`
	Adapter    string            `json:"adapter,omitempty"`
	Labels     map[string]string `json:"labels,omitempty"`
	Job        string            `json:"job,omitempty"`
}

// result builds the turn's final task for the feed; a result that is JSON travels as a data part.
func (run *activeRun) result(status wire.TaskStatus, result string, resultMetadata jsontext.Value) *kgo.Record {
	part := wire.Part{Text: result}
	switch {
	case result == "":
		part.Text = string(status.State)
	case jsontext.Value(result).IsValid():
		part = wire.Part{Data: jsontext.Value(result), MediaType: "application/json"}
	}
	metadata, _ := json.Marshal(placement{ // strings and a string map always encode
		ReceiverID: run.receiver.id, Pool: run.receiver.config.Pool, Adapter: run.adapter.Name, Labels: run.labels, Job: run.job,
	})
	return run.feed(&wire.Result{
		Task: wire.Task{
			ID: run.task.Wherry.Run, ContextID: run.task.Wherry.Run, Status: status,
			History:   run.history,
			Artifacts: []wire.Artifact{{ArtifactID: "result", Parts: []wire.Part{part}, Metadata: resultMetadata, Extensions: []string{wire.ExtensionURI}}},
			Metadata:  metadata,
		},
		Wherry: wire.ResultBinding{ReceiverBinding: run.receiver.binding(run.task, run.attempt), TaskHash: run.taskHash},
	})
}

// cancelInterrupted ends a cancelled run that waits interrupted. No member holds it, so every member
// that reads the cancel writes the CANCELED terminal under the turn's fixed event ID; a write already
// on the pool's events is not repeated.
func (r *Receiver) cancelInterrupted(ctx context.Context, interrupted wire.RunStatus) error {
	events, err := r.readEvents(ctx)
	if err != nil {
		return err
	}
	runID, turn := interrupted.Wherry.Run, interrupted.Wherry.Turn
	if slices.ContainsFunc(events.statuses[runID], func(status wire.RunStatus) bool {
		return status.Wherry.EventID == fmt.Sprintf("%s/%d/canceled", runID, turn)
	}) {
		return nil
	}
	if latest := latestStatus(events.statuses[runID], interrupted.Wherry.Tenant); latest == nil || latest.Wherry.EventID != interrupted.Wherry.EventID {
		return nil
	}
	all, err := r.readTurns(ctx, runID)
	if err != nil {
		return err
	}
	index := slices.IndexFunc(all, func(submission runSubmission) bool { return submission.hash == interrupted.Wherry.TaskHash })
	if index < 0 || all[index].record.Wherry.Turn != turn || all[index].record.Wherry.Tenant != interrupted.Wherry.Tenant {
		return errors.New("interrupted turn not found")
	}
	task := all[index].record
	history, err := turns(bindTurns(all, task, events), task)
	if err != nil {
		return err
	}
	// The run's own transactional ID would fence a member that has meanwhile claimed a continuation.
	run := &activeRun{receiver: r, task: task, taskHash: interrupted.Wherry.TaskHash, history: history, attempt: interrupted.Wherry.Attempt}
	status := taskStatus(wire.TaskStateCanceled, cancelReason)
	if err := r.admissionCommit(ctx, append([]*kgo.Record{run.result(status, cancelReason, nil)}, run.statusRecords("terminal", status, nil, nil)...)...); err != nil {
		return err
	}
	r.logger.Printf("run %s: canceled interrupted turn %d", runID, turn)
	return nil
}

func turns(submissions map[int64]runSubmission, task *wire.Submission) ([]wire.Message, error) {
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

// lastOffset returns the highest started or heartbeat log offset this turn reached under an earlier attempt.
func (r *Receiver) lastOffset(ctx context.Context, task *wire.Submission, hash string) (int64, error) {
	events, err := r.readEvents(ctx)
	if err != nil {
		return 0, err
	}
	var highest int64
	for _, status := range events.statuses[task.Wherry.Run] {
		if status.Wherry.Tenant != task.Wherry.Tenant || status.Wherry.Turn != task.Wherry.Turn || status.Wherry.TaskHash != hash ||
			(status.Wherry.Kind != "started" && status.Wherry.Kind != "heartbeat") || status.Wherry.LogOffset == nil {
			continue
		}
		highest = max(highest, *status.Wherry.LogOffset)
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
