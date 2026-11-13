package receiver

import (
	"cmp"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"maps"
	"math"
	"net"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/redact"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	readTimeout  = 30 * time.Second
	maxHandle    = 4 << 10
	budgetReason = "delivery budget exhausted"
	// heldLimit bounds the transcript an adopter holds while it waits for its attempt wrap.
	heldLimit = 4 << 20
)

// Intervals are variables so tests can shorten them; renew stays below the 15 s default lock.
var (
	renewInterval     = 10 * time.Second
	advertiseInterval = 30 * time.Second
	refreshInterval   = 15 * time.Second
	// cancelReadInterval is how often the receiver reads the pool's events, once for every run it holds.
	cancelReadInterval = 10 * time.Second
	// stateCheckInterval is how often a running run checks the cancels the receiver holds.
	stateCheckInterval = time.Second
	// shutdownTimeout bounds each of a held run's final transcript commit, released status and ack flush, which after a
	// stop also end by the shutdown deadline.
	shutdownTimeout = 5 * time.Second
	// reconcileInterval separates the Lookups that resolve an ambiguous Dispatch.
	reconcileInterval = time.Second
	// startPollInterval separates the Lookups of a job not yet started.
	startPollInterval = 2 * time.Second
	// purgeDeadline bounds the Purge retries of a start failure.
	purgeDeadline = 5 * time.Minute
	// A start failure is retried in place after startRetryWait, doubling to startRetryMaxWait, until startRetryWindow
	// from the delivery's first start failure, when a final attempt starts.
	startRetryWait    = 30 * time.Second
	startRetryMaxWait = 120 * time.Second
	startRetryWindow  = 5 * time.Minute
	// followRetryInterval separates the Follows and Lookups of a run whose log stream failed.
	followRetryInterval = time.Second
	// collectTimeout bounds a plugin's collection of a run's outputs, during which the claim is renewed.
	collectTimeout = 2 * time.Minute
	// workingFloor is the least time between a run's working statuses, and workingTimeout bounds one read of its agent's
	// working file.
	workingFloor   = 60 * time.Second
	workingTimeout = 5 * time.Second
)

// clock is the receiver's clock for placement: when Dispatch returned and when each Lookup of the await-start phase
// completed. A variable so tests can control it.
var clock = time.Now

// newClaimClient lets tests tell apart the members of profiles that share a signature topic.
var newClaimClient = func(r *Receiver, _ Named, opts ...kgo.Opt) (*kgo.Client, error) {
	return r.newClient(opts...)
}

// claimLoop retains a dedicated connection per held record because renewal cannot survive another poll, calling
// idle once the idle client closes or claiming ends with a held task.
func (r *Receiver) claimLoop(claiming, running context.Context, profile Named, idle func()) {
	defer idle()
	for claiming.Err() == nil {
		if err := r.claimOne(claiming, running, profile, idle); err != nil && claiming.Err() == nil {
			r.deny(err, capClaim)
			r.logger.Printf("claim: %v", err)
			sleep(claiming, 2*time.Second)
		}
	}
}

// claimGated requires r.mu, which must not span a poll, close, sleep, or plugin call.
func (r *Receiver) claimGated() bool {
	return r.state.Suspended[r.config.Pool] || len(r.offers) == 0 || len(r.pending) > 0
}

// gatedReason is why the receiver claims no task, as an advertisement's gated names it, and empty while it claims. It
// requires r.mu.
func (r *Receiver) gatedReason() string {
	switch {
	case r.state.Suspended[r.config.Pool]:
		return wire.GatedSuspended
	case len(r.offers) == 0:
		return wire.GatedNoReadyOffer
	case len(r.pending) > 0:
		return wire.GatedPendingRun
	case r.draining:
		return wire.GatedDraining
	}
	return ""
}

func (r *Receiver) gated(profile Named) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.claimGated() || !r.ready(profile.Name)
}

func (r *Receiver) claimOne(claiming, ctx context.Context, profile Named, idle func()) error {
	if r.gated(profile) {
		sleep(claiming, time.Second)
		return nil
	}
	// While a task runs, the client acknowledges only renewals; running holds its run ID.
	var running atomic.Pointer[string]
	ctx, cancelClaim := context.WithCancelCause(ctx)
	defer cancelClaim(nil)
	claim := &runClaim{gate: make(chan struct{}, 1), cancel: cancelClaim, ctx: ctx}
	share, err := newClaimClient(r, profile,
		kgo.ConsumeTopics(operator.SignatureTopic(r.config.Pool, profile.Agent, profile.Labels)),
		kgo.ShareGroup(operator.PoolGroup(r.config.Pool)),
		kgo.ShareMaxRecords(1),
		kgo.ShareMaxRecordsStrict(),
		// Closing waits out a pending fetch for up to its MaxWait plus 5 s; this bounds the receiver's stop.
		kgo.FetchMaxWait(time.Second),
		// FlushAcks does not return the broker's answer to an acknowledgement, a renewal included.
		kgo.ShareAckCallback(func(_ *kgo.Client, results kgo.ShareAckResults) {
			for _, result := range results {
				if running.Load() != nil {
					claim.answer(result.Err)
				}
				switch {
				case authorisationError(result.Err):
					r.deny(result.Err, capClaim)
				case result.Err != nil && running.Load() != nil:
					r.metrics.renewalFailures.Inc()
				}
			}
		}),
	)
	if err != nil {
		return err
	}
	defer share.Close()
	// Acks are flushed on shutdown too, so they must outlive ctx.
	flush := func() error {
		flushCtx, cancel := r.shutdownContext(ctx)
		defer cancel()
		return share.FlushAcks(flushCtx)
	}
	// A held client keeps renewing through readiness changes.
	stale := func() bool {
		return r.gated(profile)
	}
	var record *kgo.Record
	for record == nil {
		pollCtx, cancel := context.WithTimeout(claiming, 30*time.Second)
		go func() {
			for pollCtx.Err() == nil && !stale() {
				sleep(pollCtx, 100*time.Millisecond)
			}
			cancel()
		}()
		fetches := share.PollRecords(pollCtx, 1)
		cancel()
		// Closing the client releases a record this poll returned, before the slot counts as idle.
		if claiming.Err() != nil {
			return claiming.Err()
		}
		if len(fetches.Records()) == 0 && stale() {
			return nil
		}
		if err := fetches.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("poll: %w", err)
		}
		if records := fetches.Records(); len(records) > 0 {
			record = records[0]
			claim.anchor = time.Now()
		}
	}
	// A slot holding a task releases it only after running ends, so it does not hold up the idle slots' close.
	defer context.AfterFunc(claiming, idle)()
	if r.suspended() {
		record.Ack(kgo.AckRelease)
		return flush()
	}
	admitted, ack := r.admit(ctx, record, profile)
	if admitted == nil {
		record.Ack(ack)
		return flush()
	}
	r.adjustInFlight(profile.Name, 1)
	defer r.adjustInFlight(profile.Name, -1)
	tenant, routed := admitted.task.Wherry.Tenant, admitted.profile.Name
	r.metrics.admissions.WithLabelValues(tenant, routed).Inc()
	if attemptOf(record) == 1 {
		r.metrics.queueWait.WithLabelValues(tenant, routed).Observe(time.Since(record.Timestamp).Seconds())
	}
	r.metrics.inFlight.WithLabelValues(routed).Inc()
	defer r.metrics.inFlight.WithLabelValues(routed).Dec()
	running.Store(&admitted.task.Wherry.Run)
	claim.share, claim.record = share, record
	state, _, _ := r.snapshot()
	claim.duration = state.Pools[r.config.Pool].LockDuration()
	status := r.executeDelivery(claiming, ctx, claim, record, admitted, attemptOf(record))
	// A renewal still in flight answers after execute returns; draining it here keeps its failure counted.
	if !errors.Is(context.Cause(ctx), errClaimLost) {
		_ = flush()
	}
	running.Store(nil)
	if claim.reportLoss(r, admitted.task.Wherry.Run) {
		return nil
	}
	if status != 0 {
		record.Ack(status)
		return flush()
	}
	return nil
}

type admission struct {
	task     *wire.Submission
	taskHash string
	profile  Named
	history  []wire.Message
	inputs   []pluginsdk.File
	// agentCredentials are the credentials of the agent's terms that the run supplies, and unsupplied the optional ones it
	// does not.
	agentCredentials []wire.TaskCredential
	unsupplied       []string
	// credentials, outputs, git and s3Inputs are what the run declares.
	credentials []wire.TaskCredential
	outputs     []wire.ExpectedOutput
	git         *wire.GitInput
	s3Inputs    []wire.S3Input
	// cancelled is set when a host of the run's tenant cancelled the run; it ends CANCELED without a dispatch.
	cancelled bool
}

// admit decides whether record's task runs here; a nil admission carries the acknowledgement.
func (r *Receiver) admit(ctx context.Context, record *kgo.Record, profile Named) (*admission, kgo.AckStatus) {
	hash := wire.TaskHash(record.Value)
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
	if err := r.authorizeHost(ctx, record.Value, func(state operator.State, hosts signed.Keys) error {
		task, err = operator.AuthorizeSubmission(state, hosts, record.Value, r.config.Pool)
		return err
	}); err != nil || task.Wherry.Run != string(record.Key) {
		return nil, kgo.AckReject
	}
	r.metrics.claims.WithLabelValues(task.Wherry.Tenant).Inc()
	run, attempt := task.Wherry.Run, attemptOf(record)
	// rejectAs counts the rejection under label, a fixed text for a reason that quotes the task.
	rejectAs := func(label, reason string) (*admission, kgo.AckStatus) {
		r.logger.Printf("rejected %s: %s", run, reason)
		if err := r.publishRejection(ctx, &task, hash, attempt, reason); err != nil {
			r.logger.Printf("publish rejection for %s: %v", run, err)
			return nil, kgo.AckRelease
		}
		r.metrics.rejections.WithLabelValues(task.Wherry.Tenant, label).Inc()
		return nil, kgo.AckReject
	}
	reject := func(reason string) (*admission, kgo.AckStatus) { return rejectAs(reason, reason) }
	// giveBack is returnTask for a task no run holds yet: the terminal goes out with the echo under the
	// admission transactional ID.
	giveBack := func() (*admission, kgo.AckStatus) {
		if !r.budgetSpent(attempt) {
			return nil, kgo.AckRelease
		}
		held := &activeRun{receiver: r, task: &task, taskHash: hash, history: []wire.Message{task.Message}, attempt: attempt}
		echo := &kgo.Record{Topic: operator.TenantTopic(task.Wherry.Tenant), Key: []byte(run), Value: r.sign(r.echo(&task, hash))}
		if err := r.admissionCommit(ctx, append([]*kgo.Record{echo}, held.budgetRecords(nil)...)...); err != nil {
			r.logger.Printf("publish terminal for %s: %v", run, err)
			return nil, kgo.AckRelease
		}
		held.budgetEnded()
		return nil, kgo.AckReject
	}
	// A mismatch's rejection carries its signer's tenant and hash, which every reader of the run's statuses filters by.
	if err := wire.CheckRunID(task.Wherry); err != nil {
		return rejectAs(rejectionReasonRunID, err.Error())
	}
	if first := events.task(run); first != "" && first != hash {
		return reject(rejectionReasonDuplicate)
	}
	// A copy of the claimed record, byte for byte at another topic or offset, is acknowledged without a claim before
	// anything is written under the run's key.
	if claim := events.claim(run, hash); claim != nil && (claim.Wherry.TaskTopic != record.Topic || *claim.Wherry.TaskOffset != record.Offset) {
		return nil, kgo.AckAccept
	}
	state, _, _ := r.snapshot()
	terms := state.Pools[r.config.Pool]
	placement := task.Wherry.Placement
	agent := slices.IndexFunc(terms.Agents, func(agent operator.AgentTerms) bool { return agent.Name == placement.Agent })
	if agent < 0 {
		return reject(rejectionReasonAgent)
	}
	if !modelInTerms(terms.Agents[agent], placement.Model) {
		return reject(rejectionReasonModel)
	}
	cancelled := r.cancelledRun(ctx, events, run, task.Wherry.Tenant)
	// A run a member already claimed is past the age and bound checks; its redelivery goes on to adoption.
	claimed := slices.ContainsFunc(events.statuses[run], func(status wire.RunStatus) bool {
		return status.Wherry.TaskHash == hash && status.Wherry.Kind != "rejected"
	})
	// The receiver's clock decides both bounds, each passed only when now is past its whole second.
	now := time.Now().Unix()
	if !claimed && !cancelled && now > task.Wherry.TaskStartedBy {
		return rejectAs(rejectionReasonStartBound, wire.NotStartedBy(task.Wherry.TaskStartedBy))
	}
	if !claimed && !cancelled && now > task.Wherry.TaskEndedBy {
		return rejectAs(rejectionReasonEndBound, wire.NotEndedBy(task.Wherry.TaskEndedBy))
	}
	if reason := inadmissible(runSubmission{&task, hash, record.Timestamp}, terms); !claimed && reason != "" {
		return reject(reason)
	}
	// A claimed run past a bound its job has not met is ended here, with or without an offer, so its job is stopped.
	started := slices.ContainsFunc(events.statuses[run], func(status wire.RunStatus) bool {
		return status.Wherry.TaskHash == hash && status.Wherry.Kind == "started" && status.Wherry.LogOffset != nil
	})
	ending := claimed && (now > task.Wherry.TaskStartedBy && !started || now > task.Wherry.TaskEndedBy)
	// The record's topic routes the task: its agent is the placement's, and the profile it reached is ready and serves
	// the selector, or the host computed the wrong topic.
	if _, topicAgent, ok := wire.ParseTaskTopic(record.Topic); !ok || topicAgent != placement.Agent || record.Topic != operator.SignatureTopic(r.config.Pool, profile.Agent, profile.Labels) {
		return reject(rejectionReasonTopicAgent)
	}
	r.mu.Lock()
	offered := r.ready(profile.Name)
	r.mu.Unlock()
	if !offered && !ending {
		// A profile that went unready since this slot subscribed: another member, or this one once ready, takes it.
		r.logger.Printf("run %s: no ready profile for topic %s; releasing", run, record.Topic)
		return giveBack()
	}
	if !wire.Satisfies(placement.Selector, profile.Labels) {
		return reject(rejectionReasonSelector)
	}
	if !offered && ending {
		profile = Named{}
	}
	if !claimed && time.Since(record.Timestamp) > terms.StartByLimit() {
		return reject(rejectionReasonStartLimit)
	}
	history := []wire.Message{task.Message}
	admitted := &admission{task: &task, taskHash: hash, profile: profile, history: history, cancelled: cancelled}
	if cancelled {
		return admitted, 0
	}
	declared := task.Wherry
	// A supplied name the terms no longer declare is not delivered.
	var supplied []wire.TaskCredential
	var unsupplied []string
	for _, credential := range terms.Agents[agent].Credentials {
		switch {
		case slices.Contains(declared.Supplied, credential.Name):
			supplied = append(supplied, credential.TaskCredential)
		case !credential.Optional():
			return reject(rejectionReasonRequiredCredential)
		default:
			unsupplied = append(unsupplied, credential.Name)
		}
	}
	for _, credential := range declared.Credentials {
		if slices.ContainsFunc(terms.Agents[agent].Targets(), func(target wire.TaskCredential) bool { return target.Target() == credential.Target() }) {
			return reject(rejectionReasonCredentialTarget)
		}
	}
	if declared.Git != nil {
		if host := declared.Git.VCSHost(); !slices.Contains(terms.VCS, host) {
			return rejectAs(rejectionReasonVCS, "git remote host "+host+" is not in pool "+r.config.Pool+"'s vcs hosts")
		}
	}
	if (len(declared.S3Inputs) > 0 || len(s3Outputs(declared.Outputs)) > 0) && terms.Storage == nil {
		return rejectAs(rejectionReasonStorage, "pool "+r.config.Pool+" has no storage for S3 inputs and outputs")
	}
	inputs, rejection := deliverables(history, declared.Git)
	if rejection != "" {
		return reject(rejection)
	}
	admitted.inputs, admitted.credentials, admitted.outputs, admitted.git, admitted.s3Inputs = inputs, declared.Credentials, declared.Outputs, declared.Git, declared.S3Inputs
	admitted.agentCredentials, admitted.unsupplied = supplied, unsupplied
	return admitted, 0
}

// modelInTerms reports whether agent's terms allow model: one of its models, or none when it has none.
func modelInTerms(agent operator.AgentTerms, model string) bool {
	return slices.Contains(agent.Models, model) || len(agent.Models) == 0 && model == ""
}

// s3Outputs names the outputs that return through the pool's bucket.
func s3Outputs(outputs []wire.ExpectedOutput) []string {
	var names []string
	for _, output := range outputs {
		if output.Storage == wire.StorageS3 {
			names = append(names, output.Name)
		}
	}
	return names
}

// deliverables returns the run's input files: the git input's host key as known_hosts, then the raw and data parts.
// It returns the rejection reason for a url part.
func deliverables(history []wire.Message, git *wire.GitInput) ([]pluginsdk.File, string) {
	var inputs []pluginsdk.File
	if git != nil && git.HostKey != "" {
		inputs = append(inputs, pluginsdk.File{Name: wire.KnownHostsFile, Data: []byte(git.HostKey)})
	}
	for _, message := range history {
		for _, part := range message.Parts {
			switch {
			case part.Text != "":
			case part.Raw != nil || part.Data != nil:
				// Decoding refused repeated input names, so each is unique.
				inputs = append(inputs, pluginsdk.File{Name: wire.InputName(part), Data: append(slices.Clone([]byte(part.Raw)), part.Data...)})
			default:
				return nil, rejectionReasonPart
			}
		}
	}
	return inputs, ""
}

// publishRejection writes the echo, so every host of the tenant can find the run's pool, and the rejected
// status on the pool's events and the feed, in one admission transaction.
func (r *Receiver) publishRejection(ctx context.Context, task *wire.Submission, hash string, attempt int64, reason string) error {
	status := r.status(task, hash, attempt, "rejected", taskStatus(wire.TaskStateRejected, reason), nil)
	echo := &kgo.Record{Topic: operator.TenantTopic(task.Wherry.Tenant), Key: []byte(task.Wherry.Run), Value: r.sign(r.echo(task, hash))}
	return r.admissionCommit(ctx, append([]*kgo.Record{echo}, r.statusRecords(task, status)...)...)
}

func (r *Receiver) echo(task *wire.Submission, hash string) *wire.Echo {
	return &wire.Echo{
		Receiver: r.id, Tenant: task.Wherry.Tenant, Host: task.Wherry.Host, Pool: r.config.Pool, Run: task.Wherry.Run,
		RunNonce: task.Wherry.RunNonce, Placement: task.Wherry.Placement, TaskHash: hash, MessageID: task.Message.MessageID,
	}
}

// admissionTransactionalID is this receiver's own transactional ID inside its pool's prefix, for the writes
// it makes while holding no run; using a run's ID would fence that run's holder.
func (r *Receiver) admissionTransactionalID() string {
	return operator.TransactionalPrefix(r.config.Pool) + "admission." + r.id
}

// admissionCommit writes records in one transaction under the admission transactional ID; an
// authorisation error stops the receiver.
func (r *Receiver) admissionCommit(ctx context.Context, records ...*kgo.Record) error {
	r.admissionMu.Lock()
	defer r.admissionMu.Unlock()
	producer, err := r.newClient(kgo.TransactionalID(r.admissionTransactionalID()))
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

// commitTransaction is a variable so tests can lose the answer to a commit the broker applied, which
// kfake cannot do: its EndTxn faults all fail before the commit.
var commitTransaction = func(ctx context.Context, producer *kgo.Client) error {
	return producer.EndTransaction(ctx, kgo.TryCommit)
}

// transact commits records in one transaction on producer. An EndTransaction error leaves the outcome
// ambiguous; it is returned, so the run's task is redelivered and its next holder finds the terminal if it committed.
func (r *Receiver) transact(ctx context.Context, producer *kgo.Client, records ...*kgo.Record) error {
	if len(records) == 0 {
		return nil
	}
	if err := producer.BeginTransaction(); err != nil {
		return err
	}
	if err := producer.ProduceSync(ctx, records...).FirstErr(); err != nil {
		_ = producer.EndTransaction(ctx, kgo.TryAbort)
		r.metrics.transactionAborts.Inc()
		return err
	}
	err := commitTransaction(ctx, producer)
	if err != nil && !errors.Is(err, kerr.ProducerFenced) && !errors.Is(err, kerr.InvalidProducerEpoch) && !authorisationError(err) {
		r.metrics.ambiguousCommits.Inc()
	}
	return err
}

// startFailure names the profile the task was routed to.
func (r *Receiver) startFailure(task *wire.Submission, hash string, attempt int64, profile, class string) *kgo.Record {
	failure := &wire.StartFailure{Wherry: r.binding(task, attempt), Profile: profile, Agent: task.Wherry.Placement.Agent, Class: class, TaskHash: hash}
	return &kgo.Record{Topic: operator.PoolEventsTopic(r.config.Pool), Key: []byte(task.Wherry.Run), Value: r.sign(failure)}
}

type runSubmission struct {
	record *wire.Submission
	hash   string
	// submitted is the broker's LogAppendTime for the task record.
	submitted time.Time
}

// inadmissible is the rejection reason of a submission that carries no envelope or whose bounds fail at its append time
// under terms, and empty for one that is admissible.
func inadmissible(submission runSubmission, terms operator.Terms) string {
	binding, appended := submission.record.Wherry, submission.submitted
	switch {
	case binding.Envelope == "":
		return rejectionReasonEnvelope
	case appended.Unix() > binding.TaskStartedBy:
		return rejectionReasonAppendTime
	case binding.TaskStartedBy > appended.Add(terms.StartByLimit()).Unix():
		return rejectionReasonStartLimit
	case binding.TaskStartedBy > binding.TaskEndedBy:
		return rejectionReasonBoundOrder
	case binding.TaskEndedBy > binding.TaskStartedBy+operator.MaxRunDurationMS/1000:
		return rejectionReasonRunCap
	}
	return ""
}

func renewer(ctx context.Context, claim *runClaim) (stop func()) {
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
				if !claim.renew(ctx, false) {
					return
				}
			}
		}
	}()
	return func() { cancel(); wg.Wait() }
}

// newProducer is the run's transactional producer: an adopter initialising the same ID fences this holder.
func (r *Receiver) newProducer(run string) (*kgo.Client, error) {
	return r.newClient(kgo.TransactionalID(operator.TransactionalPrefix(r.config.Pool) + run))
}

// execute runs one task; it returns the final share acknowledgement, or 0 when fenced or when the receiver stops for a
// denial or a plugin's exit, which leaves the task to the broker's redelivery and its job running for an adopter.
// claim is nil only for direct test calls of execute, which hold no share claim.
func (r *Receiver) execute(ctx context.Context, claim *runClaim, record *kgo.Record, admitted *admission, attempt int64) kgo.AckStatus {
	if claim != nil {
		defer claim.reportLoss(r, admitted.task.Wherry.Run)
		defer renewer(ctx, claim)()
	}
	return r.executeAttempt(ctx, claim, record, admitted, attempt, nil)
}

// executeAttempt is one execution of execute under the claim's renewer. With retries, a start failure before the run
// started may return retryInPlace, leaving the run in retries.run with its producer open.
func (r *Receiver) executeAttempt(ctx context.Context, claim *runClaim, record *kgo.Record, admitted *admission, attempt int64, retries *delivery) (ack kgo.AckStatus) {
	task := admitted.task
	runID := task.Wherry.Run
	defer func() {
		if halted(ctx) {
			ack = 0
		}
	}()

	deadline := wire.PastBound(task.Wherry.TaskEndedBy)
	runCtx, cancelRun := context.WithDeadline(ctx, deadline)
	defer cancelRun()
	// startCtx bounds the run until its job starts.
	startCtx, cancelStart := context.WithDeadline(runCtx, wire.PastBound(task.Wherry.TaskStartedBy))
	defer cancelStart()

	producer, err := r.newProducer(runID)
	if err != nil {
		r.logger.Printf("run %s: producer: %v", runID, err)
		return kgo.AckRelease
	}
	run := &activeRun{
		claim: claim, receiver: r, producer: producer, task: task, taskHash: admitted.taskHash, history: admitted.history, attempt: attempt, profile: admitted.profile,
		expected: admitted.outputs, deadline: deadline, claimed: time.Now(), retries: retries,
	}
	defer func() {
		if ack == retryInPlace {
			retries.run = run
		} else {
			producer.Close()
		}
	}()
	defer func() {
		r.liveMu.Lock()
		delete(r.live, run)
		r.liveMu.Unlock()
	}()
	defer run.placed()
	pastDeadline := r.metrics.pastDeadline.WithLabelValues(admitted.profile.Name)
	// A timer firing as the run ends must not count it after its decrement.
	var overdueMu sync.Mutex
	var counted, finished bool
	overdue := time.AfterFunc(time.Until(deadline), func() {
		overdueMu.Lock()
		defer overdueMu.Unlock()
		if !finished {
			counted = true
			pastDeadline.Inc()
		}
	})
	defer func() {
		overdue.Stop()
		overdueMu.Lock()
		defer overdueMu.Unlock()
		finished = true
		if counted {
			pastDeadline.Dec()
		}
	}()

	if err := run.commit(ctx, append([]*kgo.Record{run.feed(r.echo(task, admitted.taskHash))}, run.claimRecords(record.Topic, record.Offset)...)...); err != nil {
		return run.abandon(err)
	}
	// A claim given back to the pool is released first, so the next claimant is served without waiting out this one.
	defer func() {
		if ack == kgo.AckRelease && !halted(ctx) {
			ack = run.released(ctx)
		}
	}()
	events, err := r.readEvents(ctx)
	if err != nil {
		r.logger.Printf("run %s: read events: %v", runID, err)
		return run.returnTask(ctx, nil)
	}
	run.firstDelivery = attempt == 1 && run.firstClaim(events)
	// A run whose earlier claimant was refused key unavailable and left Lookup unresolved goes on reconciling.
	if slices.ContainsFunc(events.statuses[runID], func(status wire.RunStatus) bool {
		return status.Wherry.Kind == "reconciling" && status.Wherry.TaskHash == run.taskHash
	}) && !admitted.cancelled {
		return run.reconcileKey(ctx)
	}
	offset, started := int64(0), false
	if attempt > 1 {
		if retries == nil || retries.first.IsZero() {
			r.metrics.redeliveries.WithLabelValues(task.Wherry.Tenant, admitted.profile.Name).Inc()
		}
		offset, started, err = r.lastOffset(ctx, task, run.taskHash)
		if err != nil {
			r.logger.Printf("run %s: read events: %v", runID, err)
			return run.returnTask(ctx, nil)
		}
		run.next = offset
	}
	// A replacement job never follows a transcript an earlier job started.
	if started {
		run.retries = nil
	}
	// A claimed run is redelivered even past its bounds; its job is looked up all the same, so it is stopped. The
	// lookup of a job not yet started waits no later than task_started_by.
	lookupCtx := startCtx
	if started {
		lookupCtx = runCtx
	}
	if lookupCtx.Err() != nil {
		var cancelLookup context.CancelFunc
		lookupCtx, cancelLookup = context.WithTimeout(ctx, readTimeout)
		defer cancelLookup()
	}
	var handle pluginsdk.Handle
	var job pluginsdk.Job
	adopted, neverStarted := false, false
	if attempt > 1 {
		var found bool
		job, found, err = run.findJob(ctx, lookupCtx)
		// An unresolved Lookup is asked again while the claim is held, until task_ended_by passes; no open is requested
		// before the job's state is known.
		for err != nil {
			switch {
			case ctx.Err() != nil:
				return kgo.AckRelease
			case errors.Is(err, errMisplaced):
				return run.returnTask(ctx, nil)
			case !time.Now().Before(deadline):
				return run.unresolved(ctx)
			}
			sleep(ctx, reconcileInterval)
			retryCtx, cancel := context.WithTimeout(ctx, min(readTimeout, time.Until(deadline)))
			job, found, err = run.findJob(ctx, retryCtx)
			cancel()
		}
		if found {
			handle = job.Handle
			adopted, neverStarted = job.State != pluginsdk.JobNeverStarted, job.State == pluginsdk.JobNeverStarted
			if adopted {
				r.metrics.adoptions.WithLabelValues(task.Wherry.Tenant, run.profile.Name).Inc()
			}
		}
	}
	if admitted.cancelled && !adopted {
		return run.cancel(ctx, nil)
	}
	// A run admitted with no offer and no job found has no plugin to dispatch, purge or stop through.
	if run.profile.Plugin == nil {
		return run.expired(ctx, nil, nil)
	}
	// A job no started event reported by task_started_by is located only to be stopped; a pending one past both bounds
	// names the end bound.
	if !started && adopted && !admitted.cancelled && startCtx.Err() != nil && ctx.Err() == nil {
		if job.State == pluginsdk.JobPending {
			return run.expired(ctx, &handle, nil)
		}
		return run.stopAndFail(ctx, handle, wire.NotStartedBy(task.Wherry.TaskStartedBy), nil)
	}
	if !started && neverStarted && startCtx.Err() != nil && ctx.Err() == nil {
		return run.expired(ctx, nil, nil)
	}
	if neverStarted {
		return run.neverStarted(ctx, handle, pluginsdk.Terminal{Class: job.Class, Reason: job.Reason}, nil)
	}
	// unplaced is an adopted job Lookup found pending; awaiting marks a run whose job is observed until it starts, from
	// first when a Lookup's result is already in hand.
	unplaced := adopted && job.State == pluginsdk.JobPending
	awaiting := unplaced
	var first *lookupResult
	var dispatchedAt time.Time
	if startCtx.Err() != nil && ctx.Err() == nil && !adopted {
		return run.expired(ctx, nil, nil)
	}
	inputNames := make([]string, 0, len(admitted.s3Inputs))
	for _, input := range admitted.s3Inputs {
		inputNames = append(inputNames, input.Name)
	}
	outputNames := s3Outputs(admitted.outputs)
	if !adopted {
		named := run.profile
		credentials := slices.Concat(admitted.agentCredentials, admitted.credentials)
		values, status, ok := run.awaitOpen(ctx, startCtx, nil)
		if !ok {
			return status
		}
		run.redactionKey = values.RedactionKey
		var secrets []pluginsdk.Secret
		for _, credential := range credentials {
			secrets = append(secrets, pluginsdk.Secret{Name: credential.Name, Env: credential.Env, File: credential.File, Value: values.Credentials[credential.Name]})
		}
		run.deliver(values.Credentials)
		var fetches []pluginsdk.Fetch
		fetches, run.puts = run.presigned(values, admitted.s3Inputs, outputNames)
		promptInputs := make([]string, 0, len(admitted.inputs)+len(inputNames))
		for _, input := range admitted.inputs {
			promptInputs = append(promptInputs, input.Name)
		}
		if !run.claimHeld(ctx) {
			return kgo.AckRelease
		}
		dispatched, dispatchedHandle, err := named.Plugin.Dispatch(startCtx, pluginsdk.Run{
			ID: runID, Profile: named.Name, Agent: task.Wherry.Placement.Agent, Model: task.Wherry.Placement.Model, Attempt: attempt,
			Prompt: []byte(Preamble(named.Plugin.Directories(), append(promptInputs, inputNames...), credentials, admitted.unsupplied, admitted.outputs, admitted.git, runID,
				run.terms().ReplyLimit) + prompt(admitted.history)),
			Inputs: admitted.inputs, Credentials: secrets, Fetches: fetches,
		})
		returned := clock()
		var tooLarge *pluginsdk.InputTooLargeError
		var failure *pluginsdk.Error
		switch {
		// A dispatch that errs may have created a job; it is returned unpurged, and its redelivery locates the job and
		// stops it if the bound has passed.
		case err != nil && startCtx.Err() != nil && ctx.Err() == nil:
			r.metrics.dispatchOutcomes.WithLabelValues(named.Name, dispatchOutcomeUnknown).Inc()
			return run.returnTask(ctx, nil)
		case errors.As(err, &tooLarge) && dispatched == pluginsdk.OutcomeNothingCreated:
			r.metrics.dispatchOutcomes.WithLabelValues(named.Name, dispatchOutcomeNothing).Inc()
			// The plugin's name for the input reaches the status only when it is one of the run's inputs.
			name := "an input"
			if slices.ContainsFunc(admitted.inputs, func(input pluginsdk.File) bool { return input.Name == tooLarge.Name }) {
				name = "input " + tooLarge.Name
			}
			r.logger.Printf("run %s: profile %s cannot deliver %s", runID, named.Name, name)
			if err := run.commit(ctx, r.startFailure(task, run.taskHash, attempt, named.Name, startClassInputTooLarge)); err != nil {
				return run.abandon(err)
			}
			r.metrics.startFailures.WithLabelValues(named.Name, startClassInputTooLarge).Inc()
			return run.fail(ctx, name+" takes the run's inputs over what profile "+named.Name+" delivers", nil)
		case errors.As(err, &failure) && failure.Class == pluginsdk.ClassFetch && dispatched == pluginsdk.OutcomeNothingCreated:
			r.metrics.dispatchOutcomes.WithLabelValues(named.Name, dispatchOutcomeNothing).Inc()
			r.logger.Printf("run %s: dispatch failed: an S3 input was not fetched", runID)
			return run.fail(ctx, "an S3 input was not fetched, or did not match its size and SHA-256", nil)
		case errors.Is(err, pluginsdk.ErrMessageTooLarge):
			r.logger.Printf("run %s: dispatch rejected: message too large", runID)
			dispatched = pluginsdk.OutcomeUnknown
		case err != nil:
			r.logger.Printf("run %s: dispatch failed: %s", runID, logText(run.redactText, err))
			dispatched = pluginsdk.OutcomeUnknown
		}
		switch dispatched {
		case pluginsdk.OutcomeCreated:
			r.metrics.dispatchOutcomes.WithLabelValues(named.Name, dispatchOutcomeCreated).Inc()
			awaiting, dispatchedAt = true, returned
		case pluginsdk.OutcomeNothingCreated:
			r.metrics.dispatchOutcomes.WithLabelValues(named.Name, dispatchOutcomeNothing).Inc()
			r.logger.Printf("run %s: profile %s created no job", runID, named.Name)
			return run.startFailure(ctx, startClassDispatch, "profile "+named.Name+" created no job")
		default:
			r.metrics.dispatchOutcomes.WithLabelValues(named.Name, dispatchOutcomeUnknown).Inc()
			job, resolved := run.reconcile(startCtx)
			switch {
			case !resolved:
				r.logger.Printf("run %s: dispatch unresolved", runID)
				return run.returnTask(ctx, nil)
			case job.State == pluginsdk.JobAbsent:
				return run.startFailure(ctx, startClassDispatch, "profile "+named.Name+" created no job")
			case job.State == pluginsdk.JobNeverStarted:
				return run.neverStarted(ctx, job.Handle, pluginsdk.Terminal{Class: job.Class, Reason: job.Reason}, nil)
			case job.State == pluginsdk.JobPending:
				run.pending()
				awaiting, first = true, &lookupResult{job: job}
			}
			dispatchedHandle = job.Handle
		}
		handle = dispatchedHandle
		run.labels = admitted.profile.Labels
	}
	// oversized stops a job whose handle is over the bounds and returns the task.
	oversized := func(handle pluginsdk.Handle) (kgo.AckStatus, bool) {
		if len(handle.Data) <= maxHandle && len(handle.Name) <= wire.MaxJobName {
			return 0, false
		}
		if len(handle.Data) > maxHandle {
			r.logger.Printf("run %s: profile %s returned a handle over %d bytes", runID, run.profile.Name, maxHandle)
		} else {
			r.logger.Printf("run %s: profile %s returned a job name over %d bytes", runID, run.profile.Name, wire.MaxJobName)
		}
		if !run.claimHeld(ctx) {
			return kgo.AckRelease, true
		}
		if err := run.profile.Plugin.Stop(ctx, handle); err != nil {
			r.metrics.stopFailures.WithLabelValues(run.profile.Name).Inc()
			r.logger.Printf("run %s: stop: %s", runID, logText(run.redactText, err))
		}
		return run.returnTask(ctx, nil), true
	}
	if status, over := oversized(handle); over {
		return status
	}
	run.job = handle.Name
	if adopted && admitted.cancelled {
		return run.stopAndCancel(ctx, handle, nil)
	}
	if adopted && runCtx.Err() != nil && ctx.Err() == nil {
		return run.expired(ctx, &handle, nil)
	}
	// Adoption is evidenced by the job; the event bound to this claim gives its open task_ended_by as bound.
	if adopted {
		if err := run.commit(ctx, run.poolStatus("adopting", wire.TaskStateWorking)); err != nil {
			return run.abandon(err)
		}
	}

	followCtx, stopFollow := context.WithCancel(runCtx)
	defer stopFollow()
	lines := make(chan pluginsdk.Line, 64)
	type followed struct {
		terminal pluginsdk.Terminal
		err      error
	}
	ended := make(chan followed, 1)
	follow := func(from int64) {
		go func() {
			terminal, err := run.profile.Plugin.Follow(followCtx, handle, from, lines)
			ended <- followed{terminal, err}
		}()
	}
	// An adopter holds the job's lines until its own open gives it the values to redact; the input URLs the job
	// fetched with are not delivered again. An unplaced job is followed only once it has started.
	var held []pluginsdk.Line
	if adopted {
		var release func() []pluginsdk.Line
		if !unplaced {
			follow(offset)
			release = run.hold(lines)
		}
		values, status, ok := run.awaitOpen(ctx, runCtx, &handle)
		if release != nil {
			held = release()
		}
		if !ok {
			return status
		}
		run.redactionKey = values.RedactionKey
		run.deliver(values.Credentials)
		_, run.puts = run.presigned(values, nil, outputNames)
	}
	if unplaced {
		switch {
		case ctx.Err() != nil:
			return kgo.AckRelease
		case run.cancelHeld():
			return run.stopAndCancel(ctx, handle, nil)
		case startCtx.Err() != nil:
			return run.expired(ctx, &handle, nil)
		}
	}
	if awaiting {
		observed, status, done := run.awaitStart(ctx, startCtx, handle, dispatchedAt, first)
		if done {
			return status
		}
		handle = observed.Handle
		if status, over := oversized(handle); over {
			return status
		}
		run.job = handle.Name
	}
	if !adopted || unplaced {
		follow(offset)
	}
	r.logger.Printf("run %s: attempt %d in profile %s with %s (%s)", runID, attempt, run.profile.Name, wire.TermsText(admitted.profile.Labels), handle.Name)
	var pending []pluginsdk.Line
	// shutdown commits what is pending and the lines the plugin delivered that the run has not taken, under its own
	// deadline, since ctx is already done, and returns the task. A commit the shutdown cut short can leave its
	// transaction open. It ignores the delivery budget: the receiver is going away and an adopter continues the run.
	shutdown := func() kgo.AckStatus {
		if halted(ctx) {
			return 0
		}
		shutdownCtx, cancel := r.shutdownContext(ctx)
		defer cancel()
		for drained := false; !drained; {
			select {
			case line := <-lines:
				pending = append(pending, line)
			default:
				drained = true
			}
		}
		if err := run.producer.EndTransaction(shutdownCtx, kgo.TryAbort); err != nil {
			return run.abandon(err)
		}
		if err := run.commit(shutdownCtx, run.chunks(pending, false)...); err != nil {
			return run.abandon(err)
		}
		return kgo.AckRelease
	}
	// published is the last working line this execution published, at publishedAt; line is one read since, which the
	// next tick publishes. An adopter publishes again the run's last line, with its time, after its started status.
	var published, line string
	var publishedAt time.Time
	records := run.statusRecords("started", taskStatus(wire.TaskStateWorking, ""), &offset, nil)
	if last := lastWorking(events.statuses[runID], run.taskHash); adopted && last != nil {
		status := taskStatus(wire.TaskStateWorking, workingText(*last))
		status.Timestamp = last.StatusUpdate.Status.Timestamp
		records = append(records, run.statusRecords("working", status, nil, nil)...)
		published, publishedAt = workingText(*last), time.Now()
	}
	if err := run.commit(ctx, records...); err != nil {
		if ctx.Err() != nil {
			pending = held
			return shutdown()
		}
		return run.abandon(err)
	}
	run.retries = nil
	// One read of the working file runs at a time, off the loop, and the run's end cancels it.
	readCtx, stopReads := context.WithCancel(runCtx)
	defer stopReads()
	reads, reading := make(chan workingRead, 1), false
	readFailures := map[string]bool{}
	// A failed Follow or Lookup is retried until the run passes task_ended_by.
	var retry <-chan time.Time

	progress := time.NewTicker(renewInterval)
	defer progress.Stop()
	checks := time.NewTicker(stateCheckInterval)
	defer checks.Stop()
	position, reported := offset, offset
	take := func(line pluginsdk.Line) {
		pending = append(pending, line)
		position = line.Next
	}
	for _, line := range held {
		take(line)
	}
	// lookupRetry makes the next retry a Lookup; final marks the Follow of an ended job, whose failure ends the run.
	lookupRetry, final := false, false
	recoveries := map[string]bool{}
	recovering := func(class string) {
		if !recoveries[class] {
			recoveries[class] = true
			r.logger.Printf("run %s: follow failed (%s); following again from offset %d", runID, class, position)
		}
	}
	// recoverFollow asks Lookup for the job after a Follow that failed with err, and follows it again or ends the run.
	recoverFollow := func(err error) (kgo.AckStatus, bool) {
		if final {
			r.logger.Printf("run %s: follow failed: %s", runID, logText(run.redactText, err))
			return run.stopAndFail(ctx, handle, "log stream failed", pending), true
		}
		lookupCtx, cancel := context.WithTimeout(runCtx, readTimeout)
		job, lookupErr := run.profile.Plugin.Lookup(lookupCtx, runID)
		cancel()
		switch {
		// A started job is never pending again; a Lookup that says so is an observation error.
		case lookupErr != nil || job.State == pluginsdk.JobPending:
			recovering("lookup failed")
			lookupRetry, retry = true, time.After(followRetryInterval)
			return 0, false
		case job.State == pluginsdk.JobRunning:
			recovering("job running")
			lookupRetry, retry = false, time.After(followRetryInterval)
			return 0, false
		case job.State == pluginsdk.JobEnded:
			recovering("job ended")
			final = true
			follow(position)
			return 0, false
		case job.State == pluginsdk.JobNeverStarted:
			r.logger.Printf("run %s: job never started", runID)
			return run.startFailure(ctx, startClassPlatform, "job never started"), true
		}
		// A dropped stream does not mean the task ended; stop it rather than leave it running.
		r.logger.Printf("run %s: follow failed: %s", runID, logText(run.redactText, err))
		return run.stopAndFail(ctx, handle, "log stream failed", pending), true
	}
	var followErr error
	// stopHeld ends the run when the receiver holds its cancel.
	stopHeld := func() (kgo.AckStatus, bool) {
		if run.cancelHeld() {
			stopFollow()
			return run.stopAndCancel(ctx, handle, pending), true
		}
		return 0, false
	}
	for {
		select {
		case line := <-lines:
			take(line)
		case <-retry:
			retry = nil
			switch {
			case runCtx.Err() != nil && ctx.Err() == nil:
				return run.expired(ctx, &handle, pending)
			case !lookupRetry:
				follow(position)
			default:
				if status, done := recoverFollow(followErr); done {
					return status
				}
			}
		case end := <-ended:
			for drained := false; !drained; {
				select {
				case line := <-lines:
					take(line)
				default:
					drained = true
				}
			}
			switch {
			case runCtx.Err() != nil && ctx.Err() == nil:
				return run.expired(ctx, &handle, pending)
			case ctx.Err() != nil:
				return shutdown()
			case errors.Is(end.err, pluginsdk.ErrLinkLost):
				if !recoveries["link lost"] {
					recoveries["link lost"] = true
					r.logger.Printf("run %s: plugin link lost; following again from offset %d", runID, position)
				}
				retry = time.After(followRetryInterval)
			case end.err != nil:
				followErr = end.err
				if status, done := recoverFollow(end.err); done {
					return status
				}
			case !end.terminal.Started:
				return run.neverStarted(ctx, handle, end.terminal, pending)
			default:
				return run.conclude(ctx, handle, pending, end.terminal)
			}
		case <-ctx.Done():
			stopFollow()
			return shutdown()
		case <-checks.C:
			if status, stop := stopHeld(); stop {
				return status
			}
		case read := <-reads:
			reading = false
			if read.err != nil {
				if class := failureClass(read.err); !readFailures[class] {
					readFailures[class] = true
					r.logger.Printf("run %s: read working in profile %s failed (%s)", runID, run.profile.Name, logText(run.redactText, read.err))
				}
			} else if text := run.workingLine(read.data); text != "" && text != published {
				line = text
			}
		case <-progress.C:
			if ctx.Err() != nil {
				stopFollow()
				return shutdown()
			}
			// The tick can win the select against a due check; a held stop still comes first.
			if status, stop := stopHeld(); stop {
				return status
			}
			// The newest line stays pending so the terminal transaction always holds the last chunk;
			// the progress offset is where that line starts, so an adopter reads it again.
			at, held := position, 0
			if len(pending) > 0 {
				at, held = pending[len(pending)-1].Offset, 1
			}
			// A tick with nothing to flush and the offset last reported adds nothing an adopter reads.
			flush := len(pending) > held || at != reported
			var records []*kgo.Record
			if flush {
				// The progress record commits with the chunks it covers.
				records = append(run.chunks(pending[:len(pending)-held], false), run.progress(at))
			}
			if line != "" {
				records = append(records, run.statusRecords("working", taskStatus(wire.TaskStateWorking, line), nil, nil)...)
			}
			if len(records) > 0 {
				if err := run.commit(ctx, records...); err != nil {
					if ctx.Err() != nil {
						return shutdown()
					}
					return run.abandon(err)
				}
			}
			if flush {
				run.chunked += int64(len(pending) - held)
				pending = pending[len(pending)-held:]
				reported = at
			}
			if line != "" {
				published, publishedAt, line = line, time.Now(), ""
			}
			// A read within workingFloor of the last publish could not publish.
			if !reading && time.Since(publishedAt) >= workingFloor {
				reading = true
				go func(handle pluginsdk.Handle) {
					ctx, cancel := context.WithTimeout(readCtx, workingTimeout)
					defer cancel()
					data, err := run.profile.Plugin.ReadWorking(ctx, handle)
					reads <- workingRead{data, err}
				}(handle)
			}
		}
	}
}

// lookupResult is one Lookup of a job not yet started, with the receiver's clock when it completed.
type lookupResult struct {
	job         pluginsdk.Job
	err         error
	completedAt time.Time
}

// action is what the await-start phase does when it wakes.
type action int

const (
	// awaitAction acts on nothing yet.
	awaitAction action = iota
	// leaveAction gives the run up: released on a shutdown, abandoned on a halt.
	leaveAction
	cancelAction
	resultAction
	expireAction
)

// decide is the await-start phase's action when it wakes, the first that holds of: ctx ended, the run's cancel held,
// a bound fired with a result accepted against bound, a bound fired, a result.
func decide(ctxDone, cancelled bool, result *lookupResult, boundFired bool, bound time.Time) action {
	switch {
	case ctxDone:
		return leaveAction
	case cancelled:
		return cancelAction
	case boundFired && result != nil && result.err == nil && accepted(result.job.State, result.completedAt, bound):
		return resultAction
	case boundFired:
		return expireAction
	case result != nil:
		return resultAction
	}
	return awaitAction
}

// accepted reports a result acted on once a bound has fired: the job running or ended, by a Lookup completed before
// bound.
func accepted(state pluginsdk.JobState, completedAt, bound time.Time) bool {
	return (state == pluginsdk.JobRunning || state == pluginsdk.JobEnded) && completedAt.Before(bound)
}

// awaitStart observes a job not yet started, one Lookup at a time, until it starts or ends. Ending the start phase
// cancels the Lookup in flight without joining it and drops its late result. first is a result already in hand;
// dispatched is when this attempt's Dispatch returned created, or zero otherwise.
func (run *activeRun) awaitStart(ctx, startCtx context.Context, handle pluginsdk.Handle, dispatched time.Time, first *lookupResult) (pluginsdk.Job, kgo.AckStatus, bool) {
	r, runID := run.receiver, run.task.Wherry.Run
	bound := wire.PastBound(run.task.Wherry.TaskStartedBy)
	results := make(chan lookupResult, 1)
	cancelLookup := func() {}
	defer func() { cancelLookup() }()
	lookup := func() {
		lookupCtx, cancel := context.WithCancel(startCtx)
		cancelLookup = cancel
		go func() {
			job, err := run.profile.Plugin.Lookup(lookupCtx, runID)
			results <- lookupResult{job: job, err: err, completedAt: clock()}
		}()
	}
	// No Lookup is issued under a startCtx already ended; the bound is acted on below.
	if first != nil {
		results <- *first
	} else if startCtx.Err() == nil {
		lookup()
	}
	checks := time.NewTicker(stateCheckInterval)
	defer checks.Stop()
	var next <-chan time.Time
	logged := map[string]bool{}
	for {
		var result *lookupResult
		select {
		case got := <-results:
			result = &got
		case <-next:
			next = nil
			if startCtx.Err() == nil {
				lookup()
				continue
			}
		case <-ctx.Done():
		case <-startCtx.Done():
		case <-checks.C:
		}
		// The bound is read after the cancel, so a result is judged against the bound as it stands when acted on.
		cancelled := run.cancelHeld()
		boundFired := startCtx.Err() != nil
		if boundFired && result == nil {
			select {
			case got := <-results:
				result = &got
			default:
			}
		}
		switch decide(ctx.Err() != nil, cancelled, result, boundFired, bound) {
		case awaitAction:
			continue
		case leaveAction:
			cancelLookup()
			return pluginsdk.Job{}, kgo.AckRelease, true
		case cancelAction:
			cancelLookup()
			return pluginsdk.Job{}, run.stopAndCancel(ctx, handle, nil), true
		case expireAction:
			cancelLookup()
			return pluginsdk.Job{}, run.expired(ctx, &handle, nil), true
		}
		job := result.job
		switch {
		case result.err != nil || !slices.Contains([]pluginsdk.JobState{pluginsdk.JobAbsent, pluginsdk.JobRunning, pluginsdk.JobEnded, pluginsdk.JobNeverStarted, pluginsdk.JobPending}, job.State):
			class := "state " + strconv.Itoa(int(job.State))
			if result.err != nil {
				class = failureClass(result.err)
			}
			if !logged[class] {
				logged[class] = true
				text := class
				if result.err != nil {
					text = logText(run.redactText, result.err)
				}
				r.logger.Printf("run %s: lookup in profile %s failed (%s); looking up again", runID, run.profile.Name, text)
			}
			next = time.After(startPollInterval)
		case job.State == pluginsdk.JobPending:
			run.pending()
			if status, done := run.placing(ctx, job.Reason); done {
				return pluginsdk.Job{}, status, true
			}
			next = time.After(startPollInterval)
		case job.State == pluginsdk.JobRunning || job.State == pluginsdk.JobEnded:
			run.placed()
			if !dispatched.IsZero() {
				r.metrics.placement.WithLabelValues(run.profile.Name).Observe(result.completedAt.Sub(dispatched).Seconds())
			}
			return job, 0, false
		case job.State == pluginsdk.JobNeverStarted:
			run.placed()
			if job.Handle.Data != nil {
				handle = job.Handle
			}
			return pluginsdk.Job{}, run.neverStarted(ctx, handle, pluginsdk.Terminal{Class: job.Class, Reason: job.Reason}, nil), true
		default:
			run.placed()
			r.logger.Printf("run %s: job removed before it started", runID)
			return pluginsdk.Job{}, run.startFailure(ctx, startClassPlatform, "job removed before it started"), true
		}
	}
}

// failureClass is the class of a plugin call's error under which the receiver logs it once.
func failureClass(err error) string {
	var failure *pluginsdk.Error
	switch {
	case errors.Is(err, pluginsdk.ErrLinkLost):
		return "link lost"
	case errors.As(err, &failure):
		return "class " + failure.Class
	}
	return "error"
}

// workingRead is one read of the agent's working file.
type workingRead struct {
	data []byte
	err  error
}

// workingLine is the agent's working file as its working status carries it: nothing for a file over
// pluginsdk.MaxWorking bytes, and otherwise its first line, without URLs or control characters, once the whole file is
// redacted, cut at wire.MaxWorkingText bytes. The whole file is redacted before each step of normalising that can hide
// a value (folding CRLF; replacing control characters and invalid UTF-8) and after the last, which can form one;
// redacting it whole redacts a value that spans lines or the cut.
func (run *activeRun) workingLine(data []byte) string {
	if len(data) > pluginsdk.MaxWorking {
		return ""
	}
	text := strings.Map(func(r rune) rune {
		switch {
		case r == '\r':
			return '\n'
		case r != '\n' && unicode.IsControl(r):
			return ' '
		}
		return r
	}, strings.ToValidUTF8(run.redactText(strings.ReplaceAll(run.redactText(string(data)), "\r\n", "\n")), "\uFFFD"))
	line, _, _ := strings.Cut(run.redactText(reasonURL.ReplaceAllString(text, "[redacted-url]")), "\n")
	return wire.CutUTF8(strings.TrimSpace(line), wire.MaxWorkingText)
}

// lastWorking is the last of statuses that is a working status of the run's submission hash, nil without one.
func lastWorking(statuses []wire.RunStatus, hash string) *wire.RunStatus {
	for i := len(statuses) - 1; i >= 0; i-- {
		if statuses[i].Wherry.Kind == "working" && statuses[i].Wherry.TaskHash == hash {
			return &statuses[i]
		}
	}
	return nil
}

// workingText is a working status's line, which travels raw when escaping would make it longer.
func workingText(status wire.RunStatus) string {
	if part := status.StatusUpdate.Status.Message.Parts[0]; part.Raw != nil {
		return string(part.Raw)
	}
	return status.StatusUpdate.Status.Message.Parts[0].Text
}

// placing publishes the run's placing status for reason, unless this attempt's last one has the same normalised text;
// done carries the acknowledgement of a write that failed.
func (run *activeRun) placing(ctx context.Context, reason string) (kgo.AckStatus, bool) {
	text := logText(run.redactText, errors.New(reason))
	if run.placingPublished && text == run.placingText {
		return 0, false
	}
	message := "waiting for placement"
	if text != "" {
		message += " (" + text + ")"
	}
	if err := run.commit(ctx, run.statusRecords("placing", taskStatus(wire.TaskStateWorking, message), nil, nil)...); err != nil {
		if ctx.Err() != nil {
			return kgo.AckRelease, true
		}
		return run.abandon(err), true
	}
	run.placingPublished, run.placingText = true, text
	return 0, false
}

// pending enters this execution into the receiver's pending set, which closes its claim gate; placed leaves it.
func (run *activeRun) pending() {
	r := run.receiver
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pending == nil {
		r.pending = map[*activeRun]struct{}{}
	}
	r.pending[run] = struct{}{}
}

func (run *activeRun) placed() {
	r := run.receiver
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.pending, run)
}

// unplaced reports that this execution's latest observation of its job found it pending.
func (run *activeRun) unplaced() bool {
	r := run.receiver
	r.mu.Lock()
	defer r.mu.Unlock()
	_, pending := r.pending[run]
	return pending
}

// cancelHeld reports that the receiver holds the run's cancel.
func (run *activeRun) cancelHeld() bool {
	r := run.receiver
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cancelled[runKey{run.task.Wherry.Tenant, run.task.Wherry.Run}]
}

// neverStarted ends the attempt of a run whose job ended before its agent started, as terminal's class and reason
// say: a fetch failure fails the run; any other class is a start failure.
func (run *activeRun) neverStarted(ctx context.Context, handle pluginsdk.Handle, terminal pluginsdk.Terminal, pending []pluginsdk.Line) kgo.AckStatus {
	runID := run.task.Wherry.Run
	if terminal.Class == pluginsdk.ClassFetch {
		run.receiver.logger.Printf("run %s: job never started: an S3 input was not fetched%s", runID, run.startReason(terminal))
		return run.stopAndFail(ctx, handle, "an S3 input was not fetched, or did not match its size and SHA-256", pending)
	}
	cause := "job never started"
	if terminal.Class != "" {
		cause += " (" + terminal.Class + ")"
	}
	cause += run.startReason(terminal)
	run.receiver.logger.Printf("run %s: %s", runID, cause)
	return run.startFailure(ctx, startClassPlatform, cause)
}

// hold reads lines until the returned release, which gives back the most recent of them within heldLimit bytes and
// logs the lines it dropped. Nothing held is published before the attempt can redact it.
func (run *activeRun) hold(lines <-chan pluginsdk.Line) func() []pluginsdk.Line {
	stop, done := make(chan struct{}), make(chan []pluginsdk.Line)
	go func() {
		var held []pluginsdk.Line
		size, dropped := 0, 0
		for {
			select {
			case line := <-lines:
				held = append(held, line)
				for size += len(line.Data); size > heldLimit; dropped++ {
					size -= len(held[0].Data)
					held = held[1:]
				}
			case <-stop:
				if dropped > 0 {
					next := int64(-1)
					if len(held) > 0 {
						next = held[0].Offset
					}
					run.receiver.logger.Printf("run %s: dropped %d transcript lines held before the open; the transcript resumes at offset %d", run.task.Wherry.Run, dropped, next)
				}
				done <- held
				return
			}
		}
	}()
	return func() []pluginsdk.Line {
		close(stop)
		return <-done
	}
}

// presigned returns the run's fetches and PUT URLs from its envelope, and delivers the URLs for redaction under their
// names, each with its signature, so a transcript quoting a URL in part is redacted too, with the URL's marker. The
// envelope's URLs passed the section 5 checks.
func (run *activeRun) presigned(values signed.EnvelopeValues, inputs []wire.S3Input, outputs []string) ([]pluginsdk.Fetch, map[string]string) {
	urls, signatures := map[string][]byte{}, map[string][]byte{}
	deliver := func(name, value string) {
		urls[name] = []byte(value)
		if parsed, err := url.Parse(value); err == nil {
			signatures[name] = []byte(parsed.Query().Get("X-Amz-Signature"))
		}
	}
	var fetches []pluginsdk.Fetch
	for _, input := range inputs {
		get := values.S3Inputs[input.Name]
		deliver(signed.S3InputValue(input.Name), get)
		fetches = append(fetches, pluginsdk.Fetch{Name: input.Name, URL: get, Size: input.Size, SHA256: input.SHA256})
	}
	puts := map[string]string{}
	for _, output := range outputs {
		puts[output] = values.S3Outputs[output]
		deliver(signed.S3OutputValue(output), puts[output])
	}
	run.deliver(urls)
	run.deliver(signatures)
	return fetches, puts
}

type activeRun struct {
	claim    *runClaim
	receiver *Receiver
	producer *kgo.Client
	task     *wire.Submission
	taskHash string
	history  []wire.Message
	attempt  int64
	// profile holds the run's platform material, released at the end.
	profile Named
	// labels is set when this attempt dispatched the job; an adopted job's profile labels are unknown.
	labels map[string]string
	job    string
	// deadline is when the run passes task_ended_by; puts are the presigned PUT URLs of its S3 outputs, by name, which
	// an attempt holds once opened.
	deadline time.Time
	puts     map[string]string
	// redactions are the forms of the values this attempt received, longest first, and redactionKey the run's key
	// from its envelope.
	redactions   []redact.Form
	redactionKey []byte
	// expected are the outputs the run declares; outputs are the chunks of those the run returns, and returned and
	// unreturned list them for the result.
	expected   []wire.ExpectedOutput
	outputs    []*kgo.Record
	returned   []wire.ReturnedOutput
	s3Returned []wire.ReturnedOutput
	unreturned []wire.UnreturnedOutput
	// outcome is the status the agent's outcome file reports, nil when the job left no valid one.
	outcome *wire.TaskStatus
	// replied lists the reply chunks among outputs for the result, nil when the result carries the whole reply.
	replied *wire.ReturnedReply
	// dataReply marks a reply the agent gave as a data part.
	dataReply bool
	// claimed is when this attempt began executing the run, zero for a run ended without executing it.
	claimed time.Time
	// startCause is the redacted cause of this attempt's start failure, empty without one.
	startCause string
	// retries is the delivery's in-place start retries, nil once the run started or when the delivery has none.
	retries *delivery
	// chunked counts the transcript chunks this attempt has committed.
	chunked int64
	// next is the log offset past the last line this attempt has made a chunk of, or that it resumed from.
	next int64
	// firstDelivery marks the task's first claim, at attempt 1: no job can exist for it.
	firstDelivery bool
	// placingText is the normalised reason of this attempt's last placing status, once placingPublished.
	placingText      string
	placingPublished bool
	// event is the wherry event of the run's terminal status; kept leaves the run's material to the plugin's cleanup.
	event string
	kept  bool
}

// deliver keeps the forms of each credential value delivered to the run, each with the marker of the value's name under
// the run's key, which names no credential.
func (run *activeRun) deliver(values map[string][]byte) {
	run.redactions = redact.AddForms(run.redactions, run.redactionKey, values, func(name string) {
		run.receiver.logger.Printf("run %s: credential %s too short to redact", run.task.Wherry.Run, name)
	})
	r := run.receiver
	r.liveMu.Lock()
	if r.live == nil {
		r.live = map[*activeRun][]redact.Form{}
	}
	r.live[run] = slices.Clone(run.redactions)
	r.liveMu.Unlock()
}

// HoldPlatformCredential redacts value, the platform credential name, from plugin output and errors the receiver logs.
// A platform credential has no run key, so its marker is that of its name under no key.
func (r *Receiver) HoldPlatformCredential(name string, value []byte) {
	forms := redact.AddForms(nil, nil, map[string][]byte{name: value}, func(name string) {
		r.logger.Printf("platform credential %s too short to redact", name)
	})
	r.liveMu.Lock()
	r.platform = append(r.platform, forms...)
	r.liveMu.Unlock()
}

// redact replaces each form of each delivered credential value in data with its marker, in order, as bytes.ReplaceAll
// would; the result is held only while it fits limit.
func (run *activeRun) redact(data []byte, limit int64) *redactedForm {
	out := &redactedForm{limit: limit, sum: sha256.New()}
	run.redactTo(out, data)
	return out
}

// redactTo writes data redacted to out, counting the values it replaced.
func (run *activeRun) redactTo(out io.Writer, data []byte) {
	if replaced := redact.To(out, data, run.redactions); replaced > 0 {
		run.receiver.metrics.redactions.WithLabelValues(run.task.Wherry.Tenant, cmp.Or(run.profile.Name, "none")).Add(float64(replaced))
	}
}

// redactLive is text with every form of the platform credentials and of the values delivered to a run this receiver
// holds replaced by its marker, for text no one run owns.
func (r *Receiver) redactLive(text string) string {
	r.liveMu.Lock()
	forms := slices.Concat(append(slices.Collect(maps.Values(r.live)), r.platform)...)
	r.liveMu.Unlock()
	slices.SortFunc(forms, func(a, b redact.Form) int { return len(b.Value) - len(a.Value) })
	var out strings.Builder
	redact.To(&out, []byte(text), forms)
	return out.String()
}

// PluginLogs is out with redactLive applied to each write. hclog writes each plugin log line, and each line of plugin
// stderr go-plugin forwards, in one write.
func (r *Receiver) PluginLogs(out io.Writer) io.Writer {
	return pluginLogs{r, out}
}

type pluginLogs struct {
	r   *Receiver
	out io.Writer
}

func (l pluginLogs) Write(p []byte) (int, error) {
	if _, err := io.WriteString(l.out, l.r.redactLive(string(p))); err != nil {
		return 0, err
	}
	return len(p), nil
}

// redactText is redact for receiver and agent text, which becomes a marker when its redacted form passes wire.MaxText.
func (run *activeRun) redactText(text string) string {
	form := run.redact([]byte(text), wire.MaxText)
	if form.over() {
		return overText
	}
	return string(form.data)
}

var overText = "[redacted text over " + strconv.Itoa(wire.MaxText) + " bytes]"

var reasonURL = regexp.MustCompile(`(?i)https?://\S+`)

// maxLogText bounds the plugin text one log line carries, in bytes.
const maxLogText = 512

// logText is err's text for the log: a pluginsdk.Error's op and any class, then its text, redacted by redact, without
// URLs, on one line of at most maxLogText bytes. The cap follows redaction, which could otherwise miss a value it cut.
func logText(redact func(string) string, err error) string {
	text := err.Error()
	if failure, ok := err.(*pluginsdk.Error); ok {
		text = "op=" + failure.Op
		if failure.Class != "" {
			text += " class=" + failure.Class
		}
		if failure.Err != nil {
			text += ": " + failure.Err.Error()
		}
	}
	text = strings.NewReplacer("\r", " ", "\n", " ").Replace(reasonURL.ReplaceAllString(redact(text), "[redacted-url]"))
	return wire.CutUTF8(text, maxLogText)
}

// startReason is ": " and terminal's reason as logText gives it, or empty when the plugin gave none. A fetch failure's
// reason quotes its presigned URL, so it is omitted.
func (run *activeRun) startReason(terminal pluginsdk.Terminal) string {
	switch {
	case terminal.Reason == "":
		return ""
	case terminal.Class == pluginsdk.ClassFetch:
		return ": [omitted]"
	}
	return ": " + logText(run.redactText, errors.New(terminal.Reason))
}

// redactedForm is the redacted form of some bytes: the bytes while they fit limit, and the size and SHA-256 of all of
// them.
type redactedForm struct {
	limit int64
	data  []byte
	size  int64
	sum   hash.Hash
}

func (r *redactedForm) Write(p []byte) (int, error) {
	r.size += int64(len(p))
	r.sum.Write(p)
	if r.over() {
		r.data = nil
	} else {
		r.data = append(r.data, p...)
	}
	return len(p), nil
}

func (r *redactedForm) over() bool { return r.size > r.limit }

func (r *redactedForm) hash() string { return hex.EncodeToString(r.sum.Sum(nil)) }

// terms are the run's pool's terms as the receiver holds them now.
func (run *activeRun) terms() operator.Terms {
	state, _, _ := run.receiver.snapshot()
	return state.Pools[run.receiver.config.Pool]
}

func (run *activeRun) commit(ctx context.Context, records ...*kgo.Record) error {
	if run.claim != nil && errors.Is(context.Cause(run.claim.ctx), errClaimLost) {
		return errClaimLost
	}
	return run.receiver.transact(ctx, run.producer, records...)
}

// abandon stops all activity for a run whose writes failed; a fenced holder leaves the run to its adopter.
func (run *activeRun) abandon(err error) kgo.AckStatus {
	if errors.Is(err, errClaimLost) || run.claim != nil && errors.Is(context.Cause(run.claim.ctx), errClaimLost) {
		return 0
	}
	if errors.Is(err, kerr.InvalidProducerEpoch) || errors.Is(err, kerr.ProducerFenced) {
		run.receiver.metrics.fenced.Inc()
		run.receiver.logger.Printf("run %s: fenced by a newer holder", run.task.Wherry.Run)
		return 0
	}
	run.receiver.deny(err, capWrite)
	run.receiver.logger.Printf("run %s: write failed: %v", run.task.Wherry.Run, err)
	return kgo.AckRelease
}

// stopAndFail and stopAndCancel write nothing once ctx has ended during the Stop: the task is released, or on a halt left.
func (run *activeRun) stopAndFail(ctx context.Context, handle pluginsdk.Handle, reason string, pending []pluginsdk.Line) kgo.AckStatus {
	if !run.claimHeld(ctx) {
		return kgo.AckRelease
	}
	err := run.profile.Plugin.Stop(ctx, handle)
	if ctx.Err() != nil {
		return kgo.AckRelease
	}
	if err != nil {
		run.receiver.metrics.stopFailures.WithLabelValues(run.profile.Name).Inc()
		run.receiver.logger.Printf("run %s: stop: %s", run.task.Wherry.Run, logText(run.redactText, err))
		return run.returnTask(ctx, pending)
	}
	return run.fail(ctx, reason, pending)
}

func (run *activeRun) stopAndCancel(ctx context.Context, handle pluginsdk.Handle, pending []pluginsdk.Line) kgo.AckStatus {
	if !run.claimHeld(ctx) {
		return kgo.AckRelease
	}
	err := run.profile.Plugin.Stop(ctx, handle)
	if ctx.Err() != nil {
		return kgo.AckRelease
	}
	if err != nil {
		run.receiver.metrics.stopFailures.WithLabelValues(run.profile.Name).Inc()
		run.receiver.logger.Printf("run %s: stop: %s", run.task.Wherry.Run, logText(run.redactText, err))
		return run.returnTask(ctx, pending)
	}
	return run.cancel(ctx, pending)
}

// expired ends a run past a bound: task_ended_by once its deadline passed, else task_started_by before its job
// started. With no handle, the run's job, if one exists, is purged; a run admitted with no offer has no profile to
// purge in.
func (run *activeRun) expired(ctx context.Context, job *pluginsdk.Handle, pending []pluginsdk.Line) kgo.AckStatus {
	reason := run.expiredReason()
	if job != nil {
		return run.stopAndFail(ctx, *job, reason, pending)
	}
	if run.profile.Plugin == nil {
		return run.fail(ctx, reason, pending)
	}
	if err := run.purge(ctx); err != nil {
		if ctx.Err() != nil {
			return kgo.AckRelease
		}
		return run.returnTask(ctx, pending)
	}
	return run.fail(ctx, reason, pending)
}

// expiredReason names the bound a run is past: task_ended_by once its deadline passed, else task_started_by.
func (run *activeRun) expiredReason() string {
	if time.Now().Before(run.deadline) {
		return wire.NotStartedBy(run.task.Wherry.TaskStartedBy)
	}
	return wire.NotEndedBy(run.task.Wherry.TaskEndedBy)
}

// unresolved ends a run whose job's state Lookup left unresolved past its deadline with the end bound, leaving any job
// and its material to the plugin's cleanup.
func (run *activeRun) unresolved(ctx context.Context) kgo.AckStatus {
	run.kept = true
	return run.fail(ctx, run.expiredReason(), nil)
}

func (run *activeRun) fail(ctx context.Context, reason string, pending []pluginsdk.Line) kgo.AckStatus {
	return run.end(ctx, taskStatus(wire.TaskStateFailed, reason), reason, pending, kgo.AckReject)
}

func (run *activeRun) cancel(ctx context.Context, pending []pluginsdk.Line) kgo.AckStatus {
	return run.end(ctx, taskStatus(wire.TaskStateCanceled, cancelReason), cancelReason, pending, kgo.AckAccept)
}

const cancelReason = "canceled by user"

// startFailure purges the run's job and, inside its delivery's retry window, retries in place; else it returns the task
// to the pool for a member that can start it. cause is what the run's terminal message names when it ends at the
// delivery budget or task_started_by.
func (run *activeRun) startFailure(ctx context.Context, class, cause string) kgo.AckStatus {
	failed := time.Now()
	run.startCause = logText(run.receiver.redactLive, errors.New(cause))
	if err := run.commit(ctx, run.receiver.startFailure(run.task, run.taskHash, run.attempt, run.profile.Name, class)); err != nil {
		return run.abandon(err)
	}
	run.receiver.metrics.startFailures.WithLabelValues(run.profile.Name, class).Inc()
	if err := run.purge(ctx); err != nil {
		// Shutdown: the next holder's Lookup finds the job again.
		if ctx.Err() != nil {
			return kgo.AckRelease
		}
		return run.fail(ctx, "start failed and the job could not be removed", nil)
	}
	if run.retries != nil && run.retries.retry(failed) {
		return retryInPlace
	}
	run.release(ctx)
	return run.returnTask(ctx, nil)
}

// purge retries Purge every renew interval, while the renewer holds the claim, until purgeDeadline.
func (run *activeRun) purge(ctx context.Context) error {
	deadline := time.Now().Add(purgeDeadline)
	for {
		if !run.claimHeld(ctx) {
			return cmp.Or(context.Cause(ctx), errClaimLost)
		}
		err := run.profile.Plugin.Purge(ctx, run.task.Wherry.Run)
		if err != nil {
			run.receiver.metrics.purgeFailures.WithLabelValues(run.profile.Name).Inc()
		}
		if err == nil || ctx.Err() != nil || !time.Now().Before(deadline) {
			return err
		}
		run.receiver.logger.Printf("run %s: purge: %s", run.task.Wherry.Run, logText(run.redactText, err))
		sleep(ctx, renewInterval)
	}
}

// budgetSpent reports an attempt at which a released task could reach the broker's delivery limit and be
// archived with no terminal record.
func (r *Receiver) budgetSpent(attempt int64) bool {
	state, _, _ := r.snapshot()
	return attempt >= int64(state.Pools[r.config.Pool].DeliveryLimit)-1
}

// errMisplaced is a job another profile or agent left that could not be stopped and purged.
var errMisplaced = errors.New("a misplaced job was not removed")

// findJob asks every profile's Lookup for the run's job and sets the run's profile to the one holding it; a job another
// profile or agent left is stopped and purged, or errMisplaced returned. Offers can change between attempts, so the job
// may live in a profile other than the matched one. A Lookup error leaves the job's state unresolved: a job may have
// started, with or without a started event, so nothing is purged.
func (run *activeRun) findJob(ctx, lookupCtx context.Context) (pluginsdk.Job, bool, error) {
	r, runID := run.receiver, run.task.Wherry.Run
	for _, named := range r.profiles {
		job, err := named.Plugin.Lookup(lookupCtx, runID)
		if err != nil {
			r.logger.Printf("run %s: lookup in profile %s: %s", runID, named.Name, logText(run.redactText, err))
			return pluginsdk.Job{}, false, err
		}
		if job.State == pluginsdk.JobAbsent {
			continue
		}
		// A job of another profile of this receiver on the same backend is that profile's Lookup to judge.
		if job.Profile != named.Name && slices.ContainsFunc(r.profiles, func(other Named) bool { return other.Name == job.Profile }) {
			continue
		}
		if job.Profile != named.Name || job.Agent != run.task.Wherry.Placement.Agent {
			if !run.claimHeld(ctx) {
				return pluginsdk.Job{}, false, cmp.Or(context.Cause(ctx), errClaimLost)
			}
			if err := named.Plugin.Stop(ctx, job.Handle); err != nil {
				r.metrics.stopFailures.WithLabelValues(named.Name).Inc()
				r.logger.Printf("run %s: stop: %s", runID, logText(run.redactText, err))
				return pluginsdk.Job{}, false, errMisplaced
			}
			if !run.claimHeld(ctx) {
				return pluginsdk.Job{}, false, cmp.Or(context.Cause(ctx), errClaimLost)
			}
			if err := named.Plugin.Purge(ctx, runID); err != nil {
				r.metrics.purgeFailures.WithLabelValues(named.Name).Inc()
				r.logger.Printf("run %s: purge: %s", runID, logText(run.redactText, err))
				return pluginsdk.Job{}, false, errMisplaced
			}
			r.logger.Printf("run %s: purged a job from profile %q agent %q", runID, job.Profile, job.Agent)
			continue
		}
		if job.State == pluginsdk.JobPending {
			run.pending()
		}
		r.logger.Printf("run %s: found attempt %d's job in profile %s", runID, job.Attempt, named.Name)
		run.profile = named
		return job, true, nil
	}
	return pluginsdk.Job{}, false, nil
}

// reconcile looks up the job an ambiguous Dispatch may have created; false means it stays unresolved.
func (run *activeRun) reconcile(ctx context.Context) (pluginsdk.Job, bool) {
	for i := 0; i < 3; i++ {
		if i > 0 {
			sleep(ctx, reconcileInterval)
		}
		job, err := run.profile.Plugin.Lookup(ctx, run.task.Wherry.Run)
		if err == nil {
			return job, true
		}
		run.receiver.logger.Printf("run %s: lookup in profile %s: %s", run.task.Wherry.Run, run.profile.Name, logText(run.redactText, err))
	}
	return pluginsdk.Job{}, false
}

// returnTask hands the task back to the pool with any job and its material in place, so the next holder's
// Lookup finds them. Once the delivery budget is spent it ends the run FAILED instead, and leaves the
// material to the plugin's sweep.
func (run *activeRun) returnTask(ctx context.Context, pending []pluginsdk.Line) kgo.AckStatus {
	if !run.receiver.budgetSpent(run.attempt) {
		return kgo.AckRelease
	}
	if err := run.commit(ctx, run.budgetRecords(pending)...); err != nil {
		return run.abandon(err)
	}
	run.budgetEnded()
	return kgo.AckReject
}

func (run *activeRun) budgetRecords(pending []pluginsdk.Line) []*kgo.Record {
	reason := budgetReason
	if run.startCause != "" {
		reason = fmt.Sprintf("%s after %d attempts; last: %s", budgetReason, run.attempt, run.startCause)
	}
	status := taskStatus(wire.TaskStateFailed, reason)
	return append(append(run.chunks(pending, true), run.result(status, reason)), run.terminal(status, pending)...)
}

// budgetEnded counts a run whose committed terminal ended it at the delivery budget.
func (run *activeRun) budgetEnded() {
	run.receiver.metrics.budgetExhausted.WithLabelValues(run.task.Wherry.Tenant, cmp.Or(run.profile.Name, "none")).Inc()
	run.ended(wire.TaskStateFailed)
}

// conclude ends the run whose job ended as terminal says: with the status its outcome file reports, or failed for how
// it ended when it left no valid one. The outputs are collected first, whatever the exit, so the file is always read.
func (run *activeRun) conclude(ctx context.Context, handle pluginsdk.Handle, pending []pluginsdk.Line, terminal pluginsdk.Terminal) kgo.AckStatus {
	if !run.claimHeld(ctx) {
		return kgo.AckRelease
	}
	stopped := true
	if err := run.profile.Plugin.Stop(ctx, handle); err != nil {
		run.receiver.metrics.stopFailures.WithLabelValues(run.profile.Name).Inc()
		run.receiver.logger.Printf("run %s: stop: %s", run.task.Wherry.Run, logText(run.redactText, err))
		// At the budget the run ends with only its outcome file read, and the job is left to the plugin's sweep.
		if !run.receiver.budgetSpent(run.attempt) {
			return kgo.AckRelease
		}
		stopped = false
	}
	terms := run.terms()
	run.collect(ctx, handle, stopped, terms)
	cause, reason := "", ""
	if terminal.Reason != "" {
		reason = " (" + logText(run.redactText, errors.New(terminal.Reason)) + ")"
	}
	var status wire.TaskStatus
	switch {
	case run.outcome != nil:
		status = *run.outcome
	case terminal.Signal != "":
		cause = "agent ended by " + terminal.Signal + reason
	case terminal.ExitCode != 0:
		cause = "agent exited " + strconv.Itoa(terminal.ExitCode) + reason
	default:
		status = taskStatus(wire.TaskStateFailed, "agent reported no outcome")
	}
	if cause != "" {
		status = taskStatus(wire.TaskStateFailed, cause)
	}
	result := ""
	if status.Message != nil {
		result = run.reply(text(*status.Message), terms.ReplyLimit)
		status.Message = replyMessage(*status.Message, result)
	}
	for _, output := range run.unreturned {
		if slices.ContainsFunc(run.expected, func(expected wire.ExpectedOutput) bool { return expected.Name == output.Name && expected.Required }) {
			missing := "required output " + output.Name + " not returned: " + output.Reason
			if cause != "" {
				missing = cause + "; " + missing
			}
			status = taskStatus(wire.TaskStateFailed, missing)
			break
		}
	}
	ack := kgo.AckReject
	if status.State == wire.TaskStateCompleted {
		ack = kgo.AckAccept
	}
	return run.end(ctx, status, result, pending, ack)
}

// collect redacts feed outputs and uploads S3 outputs unredacted to the host's bucket, listing every expected
// output as returned or missing.
func (run *activeRun) collect(ctx context.Context, handle pluginsdk.Handle, stopped bool, terms operator.Terms) {
	limit := terms.OutputLimit
	stored := map[string]storedOutput{}
	collected, err := run.collected(ctx, handle, stopped, limit, stored)
	if err != nil {
		reason := collectionReasonFailed
		switch {
		case !stopped:
			reason = collectionReasonNotStopped
		case errors.Is(err, errOverLimits):
			reason = collectionReasonOverLimits
		case errors.Is(err, context.DeadlineExceeded):
			reason = collectionReasonTimeout
		}
		run.receiver.metrics.collectionFailures.WithLabelValues(run.profile.Name, reason).Inc()
		run.receiver.logger.Printf("run %s: outputs not collected: %s", run.task.Wherry.Run, logText(run.redactText, err))
		for _, output := range run.expected {
			run.unreturned = append(run.unreturned, wire.UnreturnedOutput{Name: output.Name, Reason: "not collected"})
		}
		if run.outcome == nil {
			run.collectOutcome(ctx, handle, limit)
		}
		return
	}
	slices.SortFunc(collected, func(a, b pluginsdk.Output) int { return strings.Compare(a.Name, b.Name) })
	s3 := s3Outputs(run.expected)
	var total int64
	listed := map[string]bool{}
	for _, output := range collected {
		name, reason := output.Name, ""
		switch {
		case !wire.ValidOutputName(name) || listed[name]:
			// The name is the agent's and may be any bytes; it is listed only as quoted ASCII.
			name, reason = strconv.QuoteToASCII(name), "rejected: invalid name"
			name = name[:min(len(name), 256)]
		case wire.ReservedArtifact(name):
			reason = "rejected: reserved name"
		case output.Mode.Type() != 0:
			reason = "rejected: " + fileType(output.Mode)
		case slices.Contains(s3, name):
			upload, streamed := stored[name]
			switch {
			case streamed && upload.reason == "":
				listed[name] = true
				run.s3Returned = append(run.s3Returned, upload.output)
				continue
			case streamed:
				reason = upload.reason
			case output.Size > wire.MaxS3Object:
				reason = "exceeds the S3 object limit"
			default:
				reason = "not streamed by the plugin"
			}
		case output.Data == nil || total+output.Size > limit:
			reason = "exceeds output limit"
		case int64(len(output.Data)) != output.Size:
			reason = "rejected: size changed while read"
		}
		var form *redactedForm
		if reason == "" {
			// Redaction can lengthen an output, so the limit holds for its redacted form too.
			if form = run.redact(output.Data, limit-total); form.over() {
				reason = "exceeds output limit"
			}
		}
		listed[name] = true
		if reason != "" {
			run.unreturned = append(run.unreturned, wire.UnreturnedOutput{Name: name, Size: max(output.Size, 0), Reason: reason})
			continue
		}
		total += form.size
		run.outputs = append(run.outputs, run.outputChunks(name, form.data)...)
		run.returned = append(run.returned, wire.ReturnedOutput{Name: name, SHA256: form.hash(), Size: form.size})
	}
	for _, output := range run.expected {
		if !listed[output.Name] {
			run.unreturned = append(run.unreturned, wire.UnreturnedOutput{Name: output.Name, Reason: "missing"})
		}
	}
}

// storedOutput is an S3 output as the upload of its stream left it: listed for the result, or not returned for reason.
type storedOutput struct {
	output wire.ReturnedOutput
	reason string
}

// collected runs the plugin's Collect under limit and uploads each streamed S3 output into stored as it arrives.
// Collect may run until the run passes task_ended_by, when its PUT URLs expire. The count and total are
// checked again here, so a plugin that ignores the limits cannot grow the run's memory.
func (run *activeRun) collected(ctx context.Context, handle pluginsdk.Handle, stopped bool, limit int64, stored map[string]storedOutput) ([]pluginsdk.Output, error) {
	if !stopped {
		return nil, errors.New("the job was not stopped")
	}
	until := time.Now().Add(collectTimeout)
	if len(run.puts) > 0 && run.deadline.After(until) {
		until = run.deadline
	}
	ctx, cancel := context.WithDeadline(ctx, until)
	defer cancel()
	outputs := make(chan pluginsdk.Output)
	ended := make(chan error, 1)
	// Streamed, the outcome file counts against neither output limit.
	stream := append(s3Outputs(run.expected), wire.OutcomeFile)
	go func() {
		defer close(outputs)
		ended <- run.profile.Plugin.Collect(ctx, handle, pluginsdk.Limits{
			Files: wire.MaxOutputs, FileBytes: limit, TotalBytes: limit, Stream: stream, StreamBytes: wire.MaxS3Object,
		}, outputs)
	}()
	var collected []pluginsdk.Output
	var total int64
	var over error
	for output := range outputs {
		if output.Name == wire.OutcomeDir || strings.HasPrefix(output.Name, wire.OutcomeDir+"/") {
			if output.Name == wire.OutcomeFile && output.Mode.Type() == 0 {
				run.outcome = run.readOutcome(output, limit)
			}
			if output.Body != nil {
				output.Body.Close()
			}
			continue
		}
		if output.Body != nil {
			if _, done := stored[output.Name]; !done && over == nil {
				stored[output.Name] = run.store(ctx, output)
			}
			output.Body.Close()
			output.Body = nil
		}
		total += int64(len(output.Data))
		if over == nil && (len(collected) == wire.MaxOutputs || total > limit) {
			over = errOverLimits
			cancel()
		}
		if over == nil {
			collected = append(collected, output)
		}
	}
	if err := cmp.Or(over, <-ended); err != nil {
		return nil, err
	}
	return collected, nil
}

// collectOutcome runs the plugin's Collect for the outcome file alone, reading no other output, under collectTimeout.
func (run *activeRun) collectOutcome(ctx context.Context, handle pluginsdk.Handle, limit int64) {
	ctx, cancel := context.WithTimeout(ctx, collectTimeout)
	defer cancel()
	outputs := make(chan pluginsdk.Output)
	go func() {
		defer close(outputs)
		run.profile.Plugin.Collect(ctx, handle, pluginsdk.Limits{Files: wire.MaxOutputs, Stream: []string{wire.OutcomeFile}, StreamBytes: limit}, outputs)
	}()
	for output := range outputs {
		if output.Name == wire.OutcomeFile && output.Mode.Type() == 0 {
			run.outcome = run.readOutcome(output, limit)
			cancel()
		}
		if output.Body != nil {
			output.Body.Close()
		}
	}
}

var errOverLimits = errors.New("the plugin returned more than the limits")

// errRedirect is what an upload's client returns for a redirect, which it never follows.
var errRedirect = errors.New("redirected")

// storageClient uploads S3 outputs. A variable so tests can trust their TLS servers.
var storageClient = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return errRedirect }}

// transportFailure names the class of a failed request's error. The error itself is never shown: its *url.Error quotes
// the presigned URL, whose query is a credential.
func transportFailure(err error) string {
	var dns *net.DNSError
	var unknownAuthority x509.UnknownAuthorityError
	var invalidCertificate x509.CertificateInvalidError
	var hostname x509.HostnameError
	var alert tls.AlertError
	var record tls.RecordHeaderError
	var timeout interface{ Timeout() bool }
	switch {
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &timeout) && timeout.Timeout():
		return "timeout"
	case errors.As(err, &dns):
		return "dns"
	case errors.As(err, &unknownAuthority), errors.As(err, &invalidCertificate), errors.As(err, &hostname),
		errors.As(err, &alert), errors.As(err, &record):
		return "tls"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "connection refused"
	case errors.Is(err, syscall.ECONNRESET), errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "connection closed"
	}
	return "other"
}

// store uploads a streamed output to its presigned PUT URL with the Content-Length of its listing, as it reads it, and
// hashes the bytes the request sends. Its reason never quotes the URL or a response body, and is no longer than the
// longest reason a pool's limits allow for.
func (run *activeRun) store(ctx context.Context, output pluginsdk.Output) storedOutput {
	put, ok := run.puts[output.Name]
	switch {
	case !ok:
		return storedOutput{reason: "not uploaded: no upload URL"}
	case output.Mode.Type() != 0:
		return storedOutput{reason: "rejected: " + fileType(output.Mode)}
	case output.Size < 0 || output.Size > wire.MaxS3Object:
		return storedOutput{reason: "exceeds the S3 object limit"}
	}
	sum := sha256.New()
	body := io.TeeReader(output.Body, sum)
	if output.Size == 0 {
		body = http.NoBody
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, put, body)
	if err != nil {
		return storedOutput{reason: "not uploaded: invalid request"}
	}
	request.ContentLength = output.Size
	response, err := storageClient.Do(request)
	if err != nil {
		reason := "not uploaded: request failed (" + transportFailure(err) + ")"
		if errors.Is(err, errRedirect) {
			reason = "not uploaded: redirected"
		}
		run.receiver.logger.Printf("run %s: S3 output %s %s", run.task.Wherry.Run, output.Name, reason)
		return storedOutput{reason: reason}
	}
	io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		reason := "not uploaded: status " + strconv.Itoa(response.StatusCode)
		run.receiver.logger.Printf("run %s: S3 output %s %s", run.task.Wherry.Run, output.Name, reason)
		return storedOutput{reason: reason}
	}
	return storedOutput{output: wire.ReturnedOutput{
		Name: output.Name, SHA256: hex.EncodeToString(sum.Sum(nil)), Size: output.Size,
		Key: wire.S3OutputKey(run.task.Wherry.Tenant, run.task.Wherry.Run, output.Name),
	}}
}

// readOutcome reads and decodes an outcome file of at most the pool's output limit. Any outcome but a TaskStatus of a state an
// agent may report, with a ROLE_AGENT message and for TASK_STATE_AUTH_REQUIRED the credential it needs, is nil. The
// status keeps the reply, the message's text or else its first other data part, and that credential's name, redacted;
// TASK_STATE_AUTH_REQUIRED ends the run FAILED.
func (run *activeRun) readOutcome(output pluginsdk.Output, limit int64) *wire.TaskStatus {
	if output.Size > limit {
		return nil
	}
	data := output.Data
	if output.Body != nil {
		data, _ = io.ReadAll(io.LimitReader(output.Body, limit))
	}
	var reported wire.TaskStatus
	if int64(len(data)) != output.Size || json.Unmarshal(data, &reported) != nil || reported.Message == nil || reported.Message.Role != wire.RoleAgent ||
		len(reported.Message.Parts) > wire.MaxParts {
		return nil
	}
	parts := reported.Message.Parts
	credential := -1
	switch reported.State {
	case wire.TaskStateCompleted, wire.TaskStateFailed:
	case wire.TaskStateAuthRequired:
		if credential = slices.IndexFunc(parts, func(part wire.Part) bool { return neededCredential(part) != "" }); credential < 0 {
			return nil
		}
	default:
		return nil
	}
	reply, fromData := text(*reported.Message), false
	if reply == "" {
		for i, part := range parts {
			if part.Data != nil && i != credential {
				reply, fromData = string(part.Data), true
				break
			}
		}
	}
	run.dataReply = fromData
	if credential < 0 {
		status := taskStatus(reported.State, reply)
		return &status
	}
	status := withCredentials(taskStatus(wire.TaskStateFailed, reply), []string{run.redactText(neededCredential(parts[credential]))})
	return &status
}

// neededCredential is the credential a credential data part names, empty for any other part.
func neededCredential(part wire.Part) string {
	var needed struct {
		Credential string `json:"credential"`
	}
	if part.Data == nil || json.Unmarshal(part.Data, &needed) != nil || !wire.ValidName(needed.Credential) {
		return ""
	}
	return needed.Credential
}

// fileType names the type of a collected entry that is not a regular file.
func fileType(mode fs.FileMode) string {
	switch {
	case mode&fs.ModeSymlink != 0:
		return "symlink"
	case mode&fs.ModeNamedPipe != 0:
		return "fifo"
	case mode&fs.ModeDevice != 0:
		return "device"
	case mode&fs.ModeSocket != 0:
		return "socket"
	}
	return "irregular file"
}

// outputChunks splits an output into chunks of at most wire.OutputChunk bytes; an empty output is one empty chunk.
func (run *activeRun) outputChunks(name string, data []byte) []*kgo.Record {
	var records []*kgo.Record
	for index := int64(0); index == 0 || len(data) > 0; index++ {
		piece := data[:min(len(data), wire.OutputChunk)]
		data = data[len(piece):]
		metadata, _ := json.Marshal(wire.OutputMetadata{Index: index}) // an integer always encodes
		artifact := wire.Artifact{ArtifactID: name, Parts: []wire.Part{{Raw: append(wire.StdBytes{}, piece...)}}, Extensions: []string{wire.ExtensionURI}}
		if name != wire.ReplyArtifact {
			artifact.Name, artifact.Parts[0].Filename, artifact.Parts[0].MediaType = name, path.Base(name), wire.OutputMediaType
		}
		records = append(records, run.transcript(&wire.Output{
			ArtifactUpdate: wire.TaskArtifactUpdateEvent{
				TaskID: run.task.Wherry.Run, ContextID: run.task.Message.ContextID,
				Artifact: artifact,
				Append:   index > 0, LastChunk: len(data) == 0, Metadata: metadata,
			},
			Wherry: wire.ResultBinding{ReceiverBinding: run.receiver.binding(run.task, run.attempt), TaskHash: run.taskHash},
		}))
	}
	return records
}

// reply is text redacted whole as the result and terminal status carry it: all of it within wire.MaxText, else its
// first wire.MaxText bytes. A longer reply, cut to the pool's reply limit, joins the outputs as reply chunks. Both cuts
// are of the redacted bytes, so no part of a value reaches the feed.
func (run *activeRun) reply(text string, limit int64) string {
	form := run.redact([]byte(text), math.MaxInt64)
	if form.size <= wire.MaxText {
		return string(form.data)
	}
	redacted := string(form.data)
	whole := wire.CutUTF8(redacted, int(limit))
	sum := sha256.Sum256([]byte(whole))
	run.replied = &wire.ReturnedReply{SHA256: hex.EncodeToString(sum[:]), Size: int64(len(whole)), Cut: int64(len(whole)) < form.size}
	run.outputs = append(run.outputs, run.outputChunks(wire.ReplyArtifact, []byte(whole))...)
	return wire.CutUTF8(redacted, wire.MaxText)
}

// replyMessage is message with its text parts replaced by one holding text, ahead of its other parts.
func replyMessage(message wire.Message, text string) *wire.Message {
	parts := slices.DeleteFunc(slices.Clone(message.Parts), func(part wire.Part) bool { return part.Text != "" })
	if text != "" {
		parts = slices.Insert(parts, 0, wire.Part{Text: text})
	}
	message.Parts = parts
	return &message
}

// end commits the remaining transcript, the outputs and reply chunks, the result and the terminal status in one
// transaction.
func (run *activeRun) end(ctx context.Context, status wire.TaskStatus, result string, pending []pluginsdk.Line, ack kgo.AckStatus) kgo.AckStatus {
	records := append(append(append(run.chunks(pending, true), run.outputs...), run.result(status, result)), run.terminal(status, pending)...)
	if err := run.commit(ctx, records...); err != nil {
		return run.abandon(err)
	}
	run.ended(status.State)
	run.release(ctx)
	return ack
}

func (run *activeRun) release(ctx context.Context) {
	if run.profile.Plugin == nil || run.kept {
		return
	}
	if !run.claimHeld(ctx) {
		return
	}
	if err := run.profile.Plugin.Release(ctx, run.task.Wherry.Run); err != nil {
		run.receiver.logger.Printf("run %s: release: %s", run.task.Wherry.Run, logText(run.redactText, err))
	}
}

func (run *activeRun) status(kind string, status wire.TaskStatus, offset *int64, metadata jsontext.Value) *wire.RunStatus {
	status = run.redactStatus(status)
	if kind == "working" && status.Message != nil {
		// Redacting again can lengthen the agent's line, as when an adopter redacts an earlier attempt's line with other
		// values; it is cut, not signed over the wire bound.
		for i, part := range status.Message.Parts {
			status.Message.Parts[i].Text = wire.CutUTF8(part.Text, wire.MaxWorkingText)
			if part.Raw != nil {
				status.Message.Parts[i].Raw = wire.StdBytes(wire.CutUTF8(string(part.Raw), wire.MaxWorkingText))
			}
		}
	}
	value := run.receiver.status(run.task, run.taskHash, run.attempt, kind, status, offset)
	value.StatusUpdate.Metadata = metadata
	return value
}

// redactStatus redacts the text of status's message, each text part whole. The agent's text reaches it redacted and
// within wire.MaxText.
func (run *activeRun) redactStatus(status wire.TaskStatus) wire.TaskStatus {
	if status.Message != nil {
		message := *status.Message
		message.Parts = slices.Clone(message.Parts)
		for i := range message.Parts {
			text := string(run.redact([]byte(message.Parts[i].Text), math.MaxInt64).data)
			message.Parts[i].Text = text
			if escapedLength([]byte(text)) > base64.StdEncoding.EncodedLen(len(text)) {
				// As in a result, an agent's text travels raw when escaping it would make it longer than base64 does.
				message.Parts[i] = wire.Part{Raw: wire.StdBytes(text), MediaType: "text/plain"}
			}
		}
		status.Message = &message
	}
	return status
}

// statusRecords dual-writes a status to the pool's events and the tenant's feed.
func (run *activeRun) statusRecords(kind string, status wire.TaskStatus, offset *int64, metadata jsontext.Value) []*kgo.Record {
	return run.receiver.statusRecords(run.task, run.status(kind, status, offset, metadata))
}

// claimRecords are the run's claimed status records, naming the task record's topic and offset.
func (run *activeRun) claimRecords(topic string, offset int64) []*kgo.Record {
	value := run.status("claimed", taskStatus(wire.TaskStateWorking, ""), nil, nil)
	value.Wherry.TaskTopic, value.Wherry.TaskOffset = topic, &offset
	return run.receiver.statusRecords(run.task, value)
}

// terminal is the terminal status records, counting the attempt's committed chunks and pending's, or the empty last
// chunk when nothing is pending, and the tombstone of the run's progress.
func (run *activeRun) terminal(status wire.TaskStatus, pending []pluginsdk.Line) []*kgo.Record {
	value := run.status("terminal", status, nil, nil)
	value.Wherry.TranscriptChunks = run.chunked + int64(max(len(pending), 1))
	value.Wherry.Event = run.event
	tombstone := &kgo.Record{Topic: operator.ReceiverProgressTopic(run.receiver.id), Key: []byte(run.task.Wherry.Run)}
	return append(run.receiver.statusRecords(run.task, value), tombstone)
}

// progress is the run's progress record at offset on the receiver's progress topic.
func (run *activeRun) progress(offset int64) *kgo.Record {
	encoded, err := wire.EncodeProgress(wire.Progress{Tenant: run.task.Wherry.Tenant, TaskHash: run.taskHash, Attempt: run.attempt, LogOffset: offset})
	if err != nil {
		panic(fmt.Sprintf("encode progress: %v", err)) // records are built here; invalid ones are programming errors
	}
	return &kgo.Record{Topic: operator.ReceiverProgressTopic(run.receiver.id), Key: []byte(run.task.Wherry.Run), Value: encoded}
}

func (r *Receiver) statusRecords(task *wire.Submission, status *wire.RunStatus) []*kgo.Record {
	value := r.sign(status)
	return []*kgo.Record{
		{Topic: operator.PoolEventsTopic(r.config.Pool), Key: []byte(task.Wherry.Run), Value: value},
		{Topic: operator.TenantTopic(task.Wherry.Tenant), Key: []byte(task.Wherry.Run), Value: value},
	}
}

func (run *activeRun) feed(value wire.Record) *kgo.Record {
	return &kgo.Record{Topic: operator.TenantTopic(run.task.Wherry.Tenant), Key: []byte(run.task.Wherry.Run), Value: run.receiver.sign(value)}
}

func (run *activeRun) transcript(value wire.Record) *kgo.Record {
	return &kgo.Record{Topic: operator.TenantTranscriptTopic(run.task.Wherry.Tenant), Key: []byte(run.task.Wherry.Run), Value: run.receiver.sign(value)}
}

func (r *Receiver) binding(task *wire.Submission, attempt int64) wire.ReceiverBinding {
	return wire.ReceiverBinding{Receiver: r.id, Tenant: task.Wherry.Tenant, Pool: r.config.Pool, Run: task.Wherry.Run, Attempt: attempt}
}

// status builds a run status.
func (r *Receiver) status(task *wire.Submission, hash string, attempt int64, kind string, status wire.TaskStatus, offset *int64) *wire.RunStatus {
	eventID := wire.RandomID("event-")
	return &wire.RunStatus{
		StatusUpdate: wire.TaskStatusUpdateEvent{TaskID: task.Wherry.Run, ContextID: task.Message.ContextID, Status: status},
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

// withCredentials adds to status's message a credential data part for each of names.
func withCredentials(status wire.TaskStatus, names []string) wire.TaskStatus {
	if len(names) == 0 {
		return status
	}
	if status.Message == nil {
		status.Message = &wire.Message{MessageID: wire.RandomID("msg-"), Role: wire.RoleAgent, Parts: []wire.Part{}, Extensions: []string{wire.ExtensionURI}}
	}
	for _, name := range names {
		data, _ := json.Marshal(map[string]string{"credential": name}) // strings always encode
		status.Message.Parts = append(status.Message.Parts, wire.Part{Data: data, MediaType: "application/json"})
	}
	return status
}

// taskStatus stamps state with the current time; a reason becomes the status message.
func taskStatus(state wire.TaskState, reason string) wire.TaskStatus {
	status := wire.TaskStatus{State: state, Timestamp: time.Now().UTC().Format("2006-01-02T15:04:05.000Z")}
	if reason != "" {
		status.Message = &wire.Message{
			MessageID: wire.RandomID("msg-"), Role: wire.RoleAgent,
			Parts: []wire.Part{{Text: reason}}, Extensions: []string{wire.ExtensionURI},
		}
	}
	return status
}

// chunks are the transcript lines as artifact updates on the tenant's transcript topic. The last chunks end with one
// marked lastChunk, a chunk with no part when no line is left.
func (run *activeRun) chunks(lines []pluginsdk.Line, last bool) []*kgo.Record {
	records := make([]*kgo.Record, 0, len(lines)+1)
	if last && len(lines) == 0 {
		records = append(records, run.transcript(&wire.Chunk{
			ArtifactUpdate: wire.TaskArtifactUpdateEvent{
				TaskID: run.task.Wherry.Run, ContextID: run.task.Message.ContextID,
				Artifact: wire.Artifact{ArtifactID: wire.TranscriptArtifact, Parts: []wire.Part{}, Extensions: []string{wire.ExtensionURI}},
				Append:   run.next > 0, LastChunk: true,
			},
			Wherry: wire.ChunkBinding{ReceiverBinding: run.receiver.binding(run.task, run.attempt), TaskHash: run.taskHash, Seq: run.next},
		}))
	}
	for i, line := range lines {
		run.next = line.Next
		form := run.redact(line.Data, wire.MaxLine)
		line.Data = form.data
		part := wire.Part{Text: string(line.Data), MediaType: "application/x-ndjson"}
		switch {
		case form.over():
			data, _ := json.Marshal(wire.Reference{SHA256: form.hash(), Size: form.size, Unstored: wire.UnstoredOversize}) // strings and an integer always encode
			part = wire.Part{Data: data, MediaType: wire.ReferenceMediaType}
		case len(line.Data) == 0 || !utf8.Valid(line.Data) || escapedLength(line.Data) > base64.StdEncoding.EncodedLen(len(line.Data)):
			// A text part holds only non-empty UTF-8, and no more than a raw part would once JSON-escaped, so a chunk
			// stays within the record a line of wire.MaxLine raw bytes makes; other lines travel as exact bytes.
			part = wire.Part{Raw: append(wire.StdBytes{}, line.Data...), MediaType: part.MediaType}
		}
		chunk := &wire.Chunk{
			ArtifactUpdate: wire.TaskArtifactUpdateEvent{
				TaskID: run.task.Wherry.Run, ContextID: run.task.Message.ContextID,
				Artifact: wire.Artifact{ArtifactID: wire.TranscriptArtifact, Parts: []wire.Part{part}, Extensions: []string{wire.ExtensionURI}},
				// The stream's first chunk is the line at log offset zero; an adopter resumes past it.
				Append:    line.Offset > 0,
				LastChunk: last && i == len(lines)-1,
			},
			Wherry: wire.ChunkBinding{ReceiverBinding: run.receiver.binding(run.task, run.attempt), TaskHash: run.taskHash, Seq: line.Offset},
		}
		records = append(records, run.transcript(chunk))
	}
	return records
}

// escapedLength is the length of valid UTF-8 text as a JSON string's contents.
func escapedLength(text []byte) int {
	quoted, _ := jsontext.AppendQuote(nil, text)
	return len(quoted) - 2
}

// result builds the run's final task for the feed; a whole reply the agent gave as a data part travels as one while
// it stays JSON once redacted. The agent's text reaches it redacted and within wire.MaxText.
func (run *activeRun) result(status wire.TaskStatus, result string) *kgo.Record {
	result = string(run.redact([]byte(result), math.MaxInt64).data)
	part := wire.Part{Text: result}
	switch {
	case result == "":
		part.Text = string(status.State)
	case run.dataReply && run.replied == nil && jsontext.Value(result).IsValid():
		part = wire.Part{Data: jsontext.Value(result), MediaType: "application/json"}
	case !utf8.ValidString(result) || escapedLength([]byte(result)) > base64.StdEncoding.EncodedLen(len(result)):
		// As for a transcript line, text travels raw when escaping it would make it longer than base64 does.
		part = wire.Part{Raw: wire.StdBytes(result), MediaType: "text/plain"}
	}
	metadata, _ := json.Marshal(wire.ResultMetadata{ // strings, numbers and a string map always encode
		ReceiverID: run.receiver.id, Pool: run.receiver.config.Pool, Profile: run.profile.Name, Labels: run.labels, Job: run.job,
		Outputs: run.returned, S3Outputs: run.s3Returned, NotReturned: run.unreturned, Reply: run.replied,
	})
	return run.feed(&wire.Result{
		Task: wire.Task{
			ID: run.task.Wherry.Run, ContextID: run.task.Message.ContextID, Status: run.redactStatus(status),
			History:   inputPointers(run.history),
			Artifacts: append([]wire.Artifact{{ArtifactID: wire.ResultArtifact, Parts: []wire.Part{part}, Extensions: []string{wire.ExtensionURI}}}, run.references()...),
			Metadata:  metadata,
		},
		Wherry: wire.ResultBinding{ReceiverBinding: run.receiver.binding(run.task, run.attempt), TaskHash: run.taskHash},
	})
}

// references are the result's reference artifacts, one for each artifact the run streamed: each returned output and
// the reply chunks, with the SHA-256 and size of their bytes, then the transcript once it has a line, by name alone.
func (run *activeRun) references() []wire.Artifact {
	reference := func(id, name string, data []byte) wire.Artifact {
		return wire.Artifact{ArtifactID: id, Name: name, Parts: []wire.Part{{Data: data, MediaType: wire.ReferenceMediaType}}, Extensions: []string{wire.ExtensionURI}}
	}
	var artifacts []wire.Artifact
	for _, output := range run.returned {
		data, _ := json.Marshal(wire.StreamedReference{SHA256: output.SHA256, Size: output.Size}) // a string and an integer always encode
		artifacts = append(artifacts, reference(output.Name, output.Name, data))
	}
	if run.replied != nil {
		data, _ := json.Marshal(wire.StreamedReference{SHA256: run.replied.SHA256, Size: run.replied.Size}) // a string and an integer always encode
		artifacts = append(artifacts, reference(wire.ReplyArtifact, "", data))
	}
	if run.next > 0 {
		artifacts = append(artifacts, reference(wire.TranscriptArtifact, "", []byte("{}")))
	}
	return artifacts
}

// inputPointers is history with each raw and data part replaced by a reference to it with its filename, size and
// SHA-256: the inputs already travelled once, and carrying them again could put the result past the record limit.
func inputPointers(history []wire.Message) []wire.Message {
	stripped := slices.Clone(history)
	for i := range stripped {
		stripped[i].Parts = slices.Clone(stripped[i].Parts)
		for j, part := range stripped[i].Parts {
			content := []byte(part.Raw)
			if part.Data != nil {
				content = part.Data
			}
			if content == nil {
				continue
			}
			data, _ := json.Marshal(wire.Reference{SHA256: wire.TaskHash(content), Size: int64(len(content)), Unstored: wire.UnstoredInput}) // strings and an integer always encode
			stripped[i].Parts[j] = wire.Part{Data: data, Filename: part.Filename, MediaType: wire.ReferenceMediaType}
		}
	}
	return stripped
}

// lastOffset returns the highest log offset this run reached under an earlier attempt: its started offsets, and the
// progress of each receiver that held an attempt of it. An ended holder, whose progress topic revocation takes from
// this receiver, contributes nothing.
func (r *Receiver) lastOffset(ctx context.Context, task *wire.Submission, hash string) (int64, bool, error) {
	events, err := r.readEvents(ctx)
	if err != nil {
		return 0, false, err
	}
	var highest int64
	started := false
	holders := map[string]bool{}
	for _, status := range events.statuses[task.Wherry.Run] {
		if status.Wherry.Tenant != task.Wherry.Tenant || status.Wherry.TaskHash != hash {
			continue
		}
		holders[status.Wherry.Receiver] = true
		if status.Wherry.Kind == "started" && status.Wherry.LogOffset != nil {
			highest, started = max(highest, *status.Wherry.LogOffset), true
		}
	}
	readCtx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	for holder := range holders {
		records, err := r.reader.ReadFrom(readCtx, operator.ReceiverProgressTopic(holder), 0)
		if errors.Is(err, kerr.UnknownTopicOrPartition) {
			continue
		}
		// The end comes into the receiver's state only at its next refresh: a holder ended before then is a peer still.
		if errors.Is(err, kerr.TopicAuthorizationFailed) {
			// A revocation of this receiver deletes its ACLs broker by broker, so its own denial can arrive here first.
			self := operator.ReceiverPrincipal(r.id)
			if state, loadErr := r.reader.Load(readCtx); loadErr == nil && state.Ended[operator.ReceiverPrincipal(holder)] && !state.Ended[self] && !state.Revoking[self] {
				continue
			}
		}
		if err != nil {
			return 0, false, r.deny(err, capRead)
		}
		for _, record := range records {
			if string(record.Key) != task.Wherry.Run || record.Value == nil {
				continue
			}
			progress, err := wire.DecodeProgress(record.Value)
			if err == nil && progress.Tenant == task.Wherry.Tenant && progress.TaskHash == hash {
				highest = max(highest, progress.LogOffset)
			}
		}
	}
	return highest, started, nil
}

// attemptOf is the share-group delivery count, which franz-go reports as at least 1
// for share deliveries; anything else is treated as a first attempt.
func attemptOf(record *kgo.Record) int64 {
	return max(1, int64(record.DeliveryCount()))
}

// Preamble is the agent's instructions for the run. It names credentials and their targets, never their values, and
// gives full paths, so an agent without a shell finds its files. A directory the plugin does not report is named by
// its environment variable.
func Preamble(directories pluginsdk.Directories, inputs []string, credentials []wire.TaskCredential, unsupplied []string, outputs []wire.ExpectedOutput, git *wire.GitInput, run string, replyLimit int64) string {
	input, output, credential := cmp.Or(directories.Input, "$WHERRY_INPUT_DIR"), cmp.Or(directories.Output, "$WHERRY_OUTPUT_DIR"),
		cmp.Or(directories.Credentials, "$WHERRY_CREDENTIALS_DIR")
	lines := []string{"Your input directory is " + input + ", your credentials directory " + credential + " and your output directory " + output +
		"; the environment variables WHERRY_INPUT_DIR, WHERRY_CREDENTIALS_DIR and WHERRY_OUTPUT_DIR hold the same paths."}
	if len(inputs) > 0 {
		paths := make([]string, 0, len(inputs))
		for _, name := range inputs {
			paths = append(paths, input+"/"+name)
		}
		lines = append(lines, "Input files: "+strings.Join(paths, ", ")+".")
	}
	var paths, variables, names []string
	for _, delivered := range credentials {
		if delivered.Env != "" {
			variables = append(variables, delivered.Env)
		} else {
			paths = append(paths, credential+"/"+delivered.File)
		}
		if !slices.Contains(names, delivered.Name) {
			names = append(names, delivered.Name)
		}
	}
	if len(paths) > 0 {
		lines = append(lines, "Credential files: "+strings.Join(paths, ", ")+". Never print or copy their contents.")
	}
	if len(variables) > 0 {
		lines = append(lines, "Credential environment variables: "+strings.Join(variables, ", ")+". Never print or copy their values.")
	}
	for _, name := range unsupplied {
		if !slices.Contains(names, name) {
			names = append(names, name+" (not supplied)")
		}
	}
	if len(outputs) > 0 {
		paths := make([]string, 0, len(outputs))
		for _, file := range outputs {
			path := output + "/" + file.Name
			if file.Required {
				path += " (required)"
			}
			paths = append(paths, path)
		}
		lines = append(lines, "Write these output files, creating their directories if they do not exist: "+strings.Join(paths, ", ")+".")
	}
	if git != nil {
		lines = append(lines, gitPreamble(git, run, input, credential, output, credentials, outputs)...)
	}
	listed := "This run has no credentials."
	if len(names) > 0 {
		listed = "This run's credentials are " + strings.Join(names, ", ") + "."
	}
	lines = append(lines, "While you work, you may report your current step in "+output+"/"+pluginsdk.WorkingFile+", creating its directory: write one line of at most "+
		strconv.Itoa(pluginsdk.MaxWorking)+" bytes to a temporary file in that directory and rename it over this file. A longer file reports nothing.")
	lines = append(lines, "When you finish, whatever happened, write an A2A TaskStatus JSON object to "+output+"/"+wire.OutcomeFile+", creating its directory: "+
		`{"state": "TASK_STATE_COMPLETED" | "TASK_STATE_FAILED" | "TASK_STATE_AUTH_REQUIRED", "message": {"role": "ROLE_AGENT", "parts": [{"text": "<the answer, or what went wrong>"}]}}. `+
		`For TASK_STATE_AUTH_REQUIRED, add the part {"data": {"credential": "<name>"}} naming the credential you need. `+listed,
		"The answer goes in the message text; long or structured content belongs in files in "+output+". A reply over "+strconv.FormatInt(replyLimit, 10)+" bytes is cut.")
	return strings.Join(lines, "\n") + "\n\n"
}

// gitPreamble gives the commands that clone a git input and return its changes. Each starts with git and carries no
// environment assignment or redirect, so a harness rule allowing git commands allows every one. The remote, ref and
// checkout path are single-quoted, which their grammar allows; file paths are double-quoted, so a directory named by
// its environment variable expands. A credential is read from its file by a credential helper or ssh, so its value
// reaches no command line.
//
// The clone's --config settings persist for later fetches and pushes and complement pluginsdk.AgentEnvironment, which
// stops system, global and environment configuration being read. They guard a cooperative agent against ambient
// configuration, not a hostile one, which can override them.
func gitPreamble(git *wire.GitInput, run, inputDir, credentialDir, outputDir string, credentials []wire.TaskCredential, outputs []wire.ExpectedOutput) []string {
	remote, ref, checkout := "'"+git.Remote+"'", "'"+git.Ref+"'", "'"+git.Checkout()+"'"
	keyFile := ""
	if index := slices.IndexFunc(credentials, func(credential wire.TaskCredential) bool {
		return credential.Name == git.Credential && credential.File != ""
	}); index >= 0 {
		keyFile = credentialDir + "/" + credentials[index].File
	}
	scheme, _, _ := strings.Cut(git.Remote, "://")
	config := []string{"credential.helper="}
	switch {
	case scheme == "ssh":
		identity := `-o IdentityFile=/dev/null`
		if keyFile != "" {
			identity = `-i "` + keyFile + `"`
		}
		config = append(config, `core.sshCommand='ssh -F /dev/null `+identity+` -o IdentitiesOnly=yes -o IdentityAgent=none -o UserKnownHostsFile="`+inputDir+"/"+wire.KnownHostsFile+
			`" -o GlobalKnownHostsFile=/dev/null -o StrictHostKeyChecking=yes -o BatchMode=yes -o ProxyCommand=none'`)
	case keyFile != "":
		config = append(config, `credential.`+git.Origin()+`.helper='!f() { test "$1" = get || exit 0; echo username=x-access-token; echo "password=$(cat "`+keyFile+`")"; }; f'`)
	}
	config = append(config, "http.followRedirects=false", "http.sslVerify=true", "protocol.allow=never", "protocol."+scheme+".allow=always", "user.name=wherry", "user.email=wherry@invalid")
	clone := "    git clone --template="
	for _, setting := range config {
		clone += " --config " + setting
	}
	const requirement = "They need git 2.30 or later: run git version first, and if it is older, stop and report that."
	var lines []string
	if base := git.Base; base != nil {
		// refs/wherry/start stays at the base, so the run's changes need only the base to import.
		lines = []string{
			"Clone the git repository " + git.Remote + " into " + git.Checkout() + ", relative to your working directory, and continue from the previous run's changes in " +
				inputDir + "/" + base.Bundle + ", with these commands. " + requirement,
			clone + " -- " + remote + " " + checkout,
			"    git -C " + checkout + " checkout --detach '" + base.Commit + "' --",
			"    git -C " + checkout + " update-ref refs/wherry/start HEAD",
			"    git -C " + checkout + ` -c protocol.file.allow=always fetch "` + inputDir + "/" + base.Bundle + `" HEAD`,
			"    git -C " + checkout + " merge-base --is-ancestor refs/wherry/start FETCH_HEAD",
			"    git -C " + checkout + " checkout --detach FETCH_HEAD --",
			"If any of these commands fails, stop and report it.",
		}
	} else {
		lines = []string{
			"Clone the git repository " + git.Remote + " at " + git.Ref + " into " + git.Checkout() + ", relative to your working directory, with these commands. " + requirement,
			clone + " -- " + remote + " " + checkout,
			"    git -C " + checkout + " checkout " + ref + " --",
			"    git -C " + checkout + " update-ref refs/wherry/start HEAD",
		}
	}
	lines = append(lines, "The clone keeps its settings for later fetches and pushes; do not change them.")
	if keyFile != "" {
		lines = append(lines, "Never print the credential or put it on a command line.")
	}
	for _, expected := range outputs {
		switch expected.Name {
		case wire.GitPatch:
			lines = append(lines, "Commit your changes, then write them as a patch series with:", "    git -C "+checkout+` format-patch --output="`+outputDir+"/"+wire.GitPatch+`" refs/wherry/start..HEAD`)
		case wire.GitBundle:
			lines = append(lines, "Commit your changes, then write them as a bundle with:", "    git -C "+checkout+` bundle create "`+outputDir+"/"+wire.GitBundle+`" refs/wherry/start..HEAD`)
		}
	}
	return append(lines, "Push only if the request asks you to, and then only to the branch wherry/"+run+":", "    git -C "+checkout+" push origin HEAD:refs/heads/wherry/"+run)
}

// prompt is the run's text.
func prompt(history []wire.Message) string {
	return text(history[0])
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

func (r *Receiver) adjustInFlight(profile string, delta int64) {
	r.mu.Lock()
	r.inFlight += delta
	if r.profileInFlight == nil {
		r.profileInFlight = map[string]int64{}
	}
	r.profileInFlight[profile] += delta
	r.mu.Unlock()
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
