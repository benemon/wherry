package receiver

import (
	"bytes"
	"cmp"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
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
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/benemon/wherry/adapter"
	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	readTimeout  = 30 * time.Second
	maxHandle    = 4 << 10
	budgetReason = "delivery budget exhausted"
	minRedact    = 8
	redactPiece  = 64 << 10
	// heldLimit bounds the transcript an adopter holds while it waits for its seal.
	heldLimit = 4 << 20
	// maxOutcome bounds the outcome file read; message text past wire.MaxText becomes the over-size marker.
	maxOutcome = 512 << 10
)

// Intervals are variables so tests can shorten them; renew stays below the 30 s default lock.
var (
	renewInterval     = 10 * time.Second
	advertiseInterval = 30 * time.Second
	refreshInterval   = 15 * time.Second
	// cancelReadInterval is how often the receiver reads the pool's events, once for every run it holds.
	cancelReadInterval = 10 * time.Second
	// stateCheckInterval is how often a running run checks the cancels and denial the receiver holds.
	stateCheckInterval = time.Second
	// shutdownTimeout bounds the final transcript commit and ack flush after the receiver is told to stop.
	shutdownTimeout = 5 * time.Second
	// reconcileInterval separates the Lookups that resolve an ambiguous Dispatch.
	reconcileInterval = time.Second
	// purgeDeadline bounds the Purge retries of a start failure.
	purgeDeadline = 5 * time.Minute
	// followRetryInterval separates the Follows of a run whose plugin link is lost.
	followRetryInterval = time.Second
	// collectTimeout bounds an adapter's collection of a run's outputs, during which the claim is renewed.
	collectTimeout = 2 * time.Minute
	// sealWait bounds how long a claimed run waits for its host's seal, which comes only while the parent runs
	// await or the MCP server; beyond it the run ends failed.
	sealWait = wire.SealWait
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
	share, err := r.newClient(
		kgo.ConsumeTopics(admin.PoolTasksTopic(r.config.Pool)),
		kgo.ShareGroup(admin.PoolGroup(r.config.Pool)),
		kgo.ShareMaxRecords(1),
		kgo.ShareMaxRecordsStrict(),
	)
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
	tenant, routed := admitted.task.Wherry.Tenant, admitted.adapter.Name
	r.metrics.admissions.WithLabelValues(tenant, routed).Inc()
	if attemptOf(record) == 1 {
		r.metrics.queueWait.WithLabelValues(tenant, routed).Observe(time.Since(record.Timestamp).Seconds())
	}
	r.metrics.inFlight.WithLabelValues(routed).Inc()
	defer r.metrics.inFlight.WithLabelValues(routed).Dec()
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
	inputs   []adapter.File
	// agentCredentials are the agent's credentials the run's host supplies, and unsupplied the optional ones it does not.
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
func (r *Receiver) admit(ctx context.Context, record *kgo.Record) (*admission, kgo.AckStatus) {
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
	if err := r.authorizeHost(ctx, record.Value, func(state admin.State, hosts signed.Keys) error {
		task, err = admin.AuthorizeSubmission(state, hosts, record.Value, r.config.Pool)
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
		echo := &kgo.Record{Topic: admin.TenantTopic(task.Wherry.Tenant), Key: []byte(run), Value: r.sign(r.echo(&task, hash))}
		if err := r.admissionCommit(ctx, append([]*kgo.Record{echo}, held.budgetRecords(nil)...)...); err != nil {
			r.logger.Printf("publish terminal for %s: %v", run, err)
			return nil, kgo.AckRelease
		}
		held.ended(wire.TaskStateFailed)
		return nil, kgo.AckReject
	}
	// Nothing is written under the run key before its owner is known; unreadable submissions leave it unknown.
	all, err := r.readSubmissions(ctx, run)
	if err != nil {
		r.logger.Printf("read run %s submissions: %v", run, err)
		return nil, kgo.AckRelease
	}
	if foreignRun(all, events.statuses[run], task.Wherry.Tenant) {
		return reject("run ID in use")
	}
	state, _, _ := r.snapshot()
	terms := state.Pools[r.config.Pool]
	placement := task.Wherry.Placement
	agent := slices.IndexFunc(terms.Agents, func(agent admin.AgentTerms) bool { return agent.Name == placement.Agent })
	if agent < 0 {
		return reject("agent is outside the pool terms")
	}
	if !modelInTerms(terms.Agents[agent], placement.Model) {
		return reject("model not in the pool's terms")
	}
	adapter, offered := r.offer(placement)
	if !offered {
		r.logger.Printf("run %s: no current offer for agent %s satisfying its selector", run, placement.Agent)
		r.metrics.startFailures.WithLabelValues("none", "offer-unavailable").Inc()
		return giveBack()
	}
	cancelled := r.cancelledRun(ctx, events, run, task.Wherry.Tenant)
	if len(all) == 0 || all[0].hash != hash {
		return reject("duplicate submission")
	}
	// A run a member already claimed is past the age and deadline checks; its redelivery goes on to adoption.
	claimed := slices.ContainsFunc(events.statuses[run], func(status wire.RunStatus) bool {
		return status.Wherry.TaskHash == hash && status.Wherry.Kind != "rejected"
	})
	if !claimed {
		maximum := terms.MaxQueueDeadline()
		if time.Since(all[0].submitted) > maximum {
			return reject("run expired")
		}
		if task.Wherry.QueueDeadlineMS > all[0].submitted.Add(maximum).UnixMilli() {
			return reject("queue deadline exceeds the pool's max queue deadline")
		}
	}
	history := []wire.Message{task.Message}
	admitted := &admission{task: &task, taskHash: hash, adapter: adapter, history: history, cancelled: cancelled}
	if cancelled {
		return admitted, 0
	}
	now := time.Now()
	if !claimed && now.UnixMilli() > task.Wherry.QueueDeadlineMS {
		return reject("queue deadline passed")
	}
	if !claimed && now.UnixMilli() > task.Wherry.DeadlineMS {
		return reject("run deadline passed")
	}
	declared := task.Wherry
	var supplied []wire.TaskCredential
	for _, credential := range terms.Agents[agent].Credentials {
		switch {
		case !slices.Contains(declared.Unsupplied, credential.Name):
			supplied = append(supplied, credential.TaskCredential)
		case !credential.Optional():
			return reject("the host does not supply a required credential of the agent")
		}
	}
	for _, credential := range declared.Credentials {
		if slices.ContainsFunc(terms.Agents[agent].Targets(), func(target wire.TaskCredential) bool { return target.Target() == credential.Target() }) {
			return reject("a task credential takes the target of one of the agent's credentials")
		}
	}
	if declared.Git != nil {
		if host := declared.Git.VCSHost(); !slices.Contains(terms.VCS, host) {
			return rejectAs("git remote host outside the pool's vcs hosts", "git remote host "+host+" is not in pool "+r.config.Pool+"'s vcs hosts")
		}
	}
	if (len(declared.S3Inputs) > 0 || len(s3Outputs(declared.Outputs)) > 0) && terms.Storage == nil {
		return rejectAs("pool has no storage", "pool "+r.config.Pool+" has no storage for S3 inputs and outputs")
	}
	inputs, rejection := deliverables(history, declared.Git)
	if rejection != "" {
		return reject(rejection)
	}
	admitted.inputs, admitted.credentials, admitted.outputs, admitted.git, admitted.s3Inputs = inputs, declared.Credentials, declared.Outputs, declared.Git, declared.S3Inputs
	admitted.agentCredentials, admitted.unsupplied = supplied, declared.Unsupplied
	return admitted, 0
}

// modelInTerms reports whether agent's terms allow model: one of its models, or none when it has none.
func modelInTerms(agent admin.AgentTerms, model string) bool {
	return slices.Contains(agent.Models, model) || len(agent.Models) == 0 && model == ""
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
func deliverables(history []wire.Message, git *wire.GitInput) ([]adapter.File, string) {
	var inputs []adapter.File
	if git != nil && git.HostKey != "" {
		inputs = append(inputs, adapter.File{Name: wire.KnownHostsFile, Data: []byte(git.HostKey)})
	}
	for _, message := range history {
		for _, part := range message.Parts {
			switch {
			case part.Text != "":
			case part.Raw != nil || part.Data != nil:
				// Decoding refused repeated input names, so each is unique.
				inputs = append(inputs, adapter.File{Name: wire.InputName(part), Data: append(slices.Clone([]byte(part.Raw)), part.Data...)})
			default:
				return nil, "unsupported message part"
			}
		}
	}
	return inputs, ""
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
		Receiver: r.id, Tenant: task.Wherry.Tenant, Pool: r.config.Pool, Run: task.Wherry.Run, Placement: task.Wherry.Placement,
		TaskHash: hash, MessageID: task.Message.MessageID,
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

// probeWrite proves the write and transactional ID ACLs with an aborted admission transaction on the
// pool's events, which no read_committed reader sees.
func (r *Receiver) probeWrite(ctx context.Context) error {
	r.admissionMu.Lock()
	defer r.admissionMu.Unlock()
	producer, err := r.newClient(kgo.TransactionalID(r.admissionTransactionalID()))
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

// newSealKey is a variable so tests can watch the key a run's seal is opened with.
var newSealKey = func() (*ecdsa.PrivateKey, error) { return ecdsa.GenerateKey(elliptic.P256(), rand.Reader) }

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
		r.metrics.transactionAborts.Inc()
		return err
	}
	err := commitTransaction(ctx, producer)
	if err == nil || errors.Is(err, kerr.ProducerFenced) || errors.Is(err, kerr.InvalidProducerEpoch) || authorisationError(err) {
		return err
	}
	r.metrics.ambiguousCommits.Inc()
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

// startFailure names the adapter the task was routed to.
func (r *Receiver) startFailure(task *wire.Submission, attempt int64, adapter, class string) *kgo.Record {
	failure := &wire.StartFailure{Wherry: r.binding(task, attempt), Adapter: adapter, Agent: task.Wherry.Placement.Agent, Class: class}
	return &kgo.Record{Topic: admin.PoolEventsTopic(r.config.Pool), Key: []byte(task.Wherry.Run), Value: r.sign(failure)}
}

type runSubmission struct {
	record *wire.Submission
	hash   string
	// submitted is the broker's LogAppendTime for the task record.
	submitted time.Time
}

// readSubmissions reads the run's current-key submissions from the pool's task topic in log order.
func (r *Receiver) readSubmissions(ctx context.Context, run string) ([]runSubmission, error) {
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
		submission, err := admin.AuthorizeSubmission(state, hosts, record.Value, r.config.Pool)
		if err == nil && submission.Wherry.Run == run {
			result = append(result, runSubmission{record: &submission, hash: wire.TaskHash(record.Value), submitted: record.Timestamp})
		}
	}
	return result, nil
}

// renewer keeps the share lock alive from claim until the final acknowledgement,
// including while the platform places the job.
func renewer(ctx context.Context, share *kgo.Client, record *kgo.Record, failures prometheus.Counter, logf func(string, ...any)) (stop func()) {
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
					failures.Inc()
					logf("renew: %v", err)
				}
			}
		}
	}()
	return func() { cancel(); wg.Wait() }
}

// newProducer is the run's transactional producer: an adopter initialising the same ID fences this holder.
func (r *Receiver) newProducer(run string) (*kgo.Client, error) {
	return r.newClient(kgo.TransactionalID(admin.TransactionalPrefix(r.config.Pool) + run))
}

// execute runs one task; it returns the final share acknowledgement, or 0 when fenced.
func (r *Receiver) execute(ctx context.Context, share *kgo.Client, record *kgo.Record, admitted *admission, attempt int64) kgo.AckStatus {
	task := admitted.task
	runID := task.Wherry.Run
	stopRenew := renewer(ctx, share, record, r.metrics.renewalFailures, func(format string, args ...any) {
		r.logger.Printf("run %s: "+format, append([]any{runID}, args...)...)
	})
	defer stopRenew()

	// The cap is anchored to the signed queue deadline so redelivery cannot extend it.
	deadline := time.UnixMilli(task.Wherry.DeadlineMS)
	if limit := time.UnixMilli(task.Wherry.QueueDeadlineMS).Add(time.Duration(admin.MaxRunDurationMS) * time.Millisecond); deadline.After(limit) {
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
	run := &activeRun{
		receiver: r, producer: producer, task: task, taskHash: admitted.taskHash, history: admitted.history, attempt: attempt, adapter: admitted.adapter,
		expected: admitted.outputs, deadline: deadline, claimed: time.Now(),
	}
	defer func() {
		r.liveMu.Lock()
		delete(r.live, run)
		r.liveMu.Unlock()
	}()

	if err := run.commit(ctx, append([]*kgo.Record{run.feed(r.echo(task, admitted.taskHash))}, run.statusRecords("claimed", taskStatus(wire.TaskStateWorking, ""), nil, nil)...)...); err != nil {
		return run.abandon(err)
	}
	// A claimed run is redelivered even past its deadline; its job is looked up all the same, so it is stopped.
	lookupCtx := runCtx
	if runCtx.Err() != nil {
		var cancelLookup context.CancelFunc
		lookupCtx, cancelLookup = context.WithTimeout(ctx, readTimeout)
		defer cancelLookup()
	}
	offset := int64(0)
	if attempt > 1 {
		r.metrics.redeliveries.WithLabelValues(task.Wherry.Tenant, admitted.adapter.Name).Inc()
		offset, err = r.lastOffset(lookupCtx, task, run.taskHash)
		if err != nil {
			r.logger.Printf("run %s: read events: %v", runID, err)
			return run.returnTask(ctx, nil)
		}
	}
	var handle adapter.Handle
	adopted, neverStarted := false, false
	if attempt > 1 {
		// Offers can change between attempts, so the run may live in an adapter other than the matched one.
		for _, named := range r.adapters {
			job, err := named.Adapter.Lookup(lookupCtx, runID)
			if err != nil {
				if lookupCtx.Err() != nil && ctx.Err() == nil {
					return run.fail(ctx, "timeout", nil)
				}
				r.logger.Printf("run %s: lookup in adapter %s: %s", runID, named.Name, logText(run.redactText, err))
				return run.returnTask(ctx, nil)
			}
			if job.State == adapter.JobAbsent {
				continue
			}
			// A job of another adapter of this receiver on the same backend is that adapter's Lookup to judge.
			if job.Adapter != named.Name && slices.ContainsFunc(r.adapters, func(other Named) bool { return other.Name == job.Adapter }) {
				continue
			}
			if job.Adapter != named.Name || job.Agent != task.Wherry.Placement.Agent {
				if err := named.Adapter.Stop(ctx, job.Handle); err != nil {
					r.metrics.stopFailures.WithLabelValues(named.Name).Inc()
					r.logger.Printf("run %s: stop: %s", runID, logText(run.redactText, err))
					return run.returnTask(ctx, nil)
				}
				if err := named.Adapter.Purge(ctx, runID); err != nil {
					r.metrics.purgeFailures.WithLabelValues(named.Name).Inc()
					r.logger.Printf("run %s: purge: %s", runID, logText(run.redactText, err))
					return run.returnTask(ctx, nil)
				}
				r.logger.Printf("run %s: purged a job from adapter %q agent %q", runID, job.Adapter, job.Agent)
				continue
			}
			r.logger.Printf("run %s: found attempt %d's job in adapter %s", runID, job.Attempt, named.Name)
			handle, run.adapter = job.Handle, named
			adopted, neverStarted = job.State != adapter.JobNeverStarted, job.State == adapter.JobNeverStarted
			if adopted {
				r.metrics.adoptions.WithLabelValues(task.Wherry.Tenant, named.Name).Inc()
			}
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
		return run.startFailure(ctx, "platform", "job never started")
	}
	if runCtx.Err() != nil && ctx.Err() == nil {
		if adopted {
			run.job = handle.Name
			return run.stopAndFail(ctx, handle, "timeout", nil)
		}
		return run.fail(ctx, "timeout", nil)
	}
	// The agent's credentials are asked for under the pool's terms; a task credential of the same name rides with one.
	agentCredentials, taskCredentials := []string{}, []string{}
	for _, credential := range admitted.agentCredentials {
		agentCredentials = append(agentCredentials, credential.Name)
	}
	for _, credential := range admitted.credentials {
		if !slices.Contains(agentCredentials, credential.Name) && !slices.Contains(taskCredentials, credential.Name) {
			taskCredentials = append(taskCredentials, credential.Name)
		}
	}
	inputNames := make([]string, 0, len(admitted.s3Inputs))
	for _, input := range admitted.s3Inputs {
		inputNames = append(inputNames, input.Name)
	}
	outputNames := s3Outputs(admitted.outputs)
	if !adopted {
		named := run.adapter
		credentials := slices.Concat(admitted.agentCredentials, admitted.credentials)
		var secrets []adapter.Secret
		var fetches []adapter.Fetch
		if len(agentCredentials)+len(taskCredentials)+len(inputNames)+len(outputNames) > 0 {
			sealed, status, ok := run.awaitSeal(ctx, runCtx, nil, agentCredentials, taskCredentials, inputNames, outputNames)
			if !ok {
				return status
			}
			values := sealed.Values
			run.redactionKey = sealed.RedactionKey
			for _, credential := range credentials {
				secrets = append(secrets, adapter.Secret{Name: credential.Name, Env: credential.Env, File: credential.File, Value: values[credential.Name]})
			}
			run.deliver(values)
			if fetches, run.puts, err = run.presigned(values, admitted.s3Inputs, outputNames); err != nil {
				r.logger.Printf("run %s: %v", runID, err)
				return run.fail(ctx, err.Error(), nil)
			}
		}
		promptInputs := make([]string, 0, len(admitted.inputs)+len(inputNames))
		for _, input := range admitted.inputs {
			promptInputs = append(promptInputs, input.Name)
		}
		dispatched, dispatchedHandle, err := named.Adapter.Dispatch(runCtx, adapter.Run{
			ID: runID, Adapter: named.Name, Agent: task.Wherry.Placement.Agent, Model: task.Wherry.Placement.Model, Attempt: attempt,
			Prompt: []byte(preamble(named.Adapter.Directories(), append(promptInputs, inputNames...), credentials, admitted.unsupplied, admitted.outputs, admitted.git, runID) +
				prompt(admitted.history)),
			Inputs: admitted.inputs, Credentials: secrets, Fetches: fetches,
		})
		var tooLarge *adapter.InputTooLargeError
		switch {
		case err != nil && runCtx.Err() != nil && ctx.Err() == nil:
			r.metrics.dispatchOutcomes.WithLabelValues(named.Name, "unknown").Inc()
			return run.fail(ctx, "timeout", nil)
		case errors.As(err, &tooLarge) && dispatched == adapter.OutcomeNothingCreated:
			r.metrics.dispatchOutcomes.WithLabelValues(named.Name, "nothing").Inc()
			// The adapter's name for the input reaches the status only when it is one of the run's inputs.
			name := "an input"
			if slices.ContainsFunc(admitted.inputs, func(input adapter.File) bool { return input.Name == tooLarge.Name }) {
				name = "input " + tooLarge.Name
			}
			r.logger.Printf("run %s: adapter %s cannot deliver %s", runID, named.Name, name)
			if err := run.commit(ctx, r.startFailure(task, attempt, named.Name, "input-too-large")); err != nil {
				return run.abandon(err)
			}
			r.metrics.startFailures.WithLabelValues(named.Name, "input-too-large").Inc()
			return run.fail(ctx, name+" takes the run's inputs over what adapter "+named.Name+" delivers", nil)
		case errors.Is(err, adapter.ErrMessageTooLarge):
			r.logger.Printf("run %s: dispatch rejected: message too large", runID)
			dispatched = adapter.OutcomeUnknown
		case err != nil:
			r.logger.Printf("run %s: dispatch failed: %s", runID, logText(run.redactText, err))
			dispatched = adapter.OutcomeUnknown
		}
		switch dispatched {
		case adapter.OutcomeCreated:
			r.metrics.dispatchOutcomes.WithLabelValues(named.Name, "created").Inc()
		case adapter.OutcomeNothingCreated:
			r.metrics.dispatchOutcomes.WithLabelValues(named.Name, "nothing").Inc()
			r.logger.Printf("run %s: adapter %s created no job", runID, named.Name)
			return run.startFailure(ctx, "dispatch", "adapter "+named.Name+" created no job")
		default:
			r.metrics.dispatchOutcomes.WithLabelValues(named.Name, "unknown").Inc()
			job, resolved := run.reconcile(runCtx)
			switch {
			case !resolved:
				r.logger.Printf("run %s: dispatch unresolved", runID)
				return run.returnTask(ctx, nil)
			case job.State == adapter.JobAbsent:
				return run.startFailure(ctx, "dispatch", "adapter "+named.Name+" created no job")
			case job.State == adapter.JobNeverStarted:
				return run.startFailure(ctx, "platform", "job never started")
			}
			dispatchedHandle = job.Handle
		}
		handle = dispatchedHandle
		run.labels = admitted.adapter.Labels
	}
	if len(handle.Data) > maxHandle {
		r.logger.Printf("run %s: adapter %s returned a handle over %d bytes", runID, run.adapter.Name, maxHandle)
		if err := run.adapter.Adapter.Stop(ctx, handle); err != nil {
			r.metrics.stopFailures.WithLabelValues(run.adapter.Name).Inc()
			r.logger.Printf("run %s: stop: %s", runID, logText(run.redactText, err))
		}
		return run.returnTask(ctx, nil)
	}
	run.job = handle.Name

	followCtx, stopFollow := context.WithCancel(runCtx)
	defer stopFollow()
	lines := make(chan adapter.Line, 64)
	type followed struct {
		terminal adapter.Terminal
		err      error
	}
	ended := make(chan followed, 1)
	follow := func(from int64) {
		go func() {
			terminal, err := run.adapter.Adapter.Follow(followCtx, handle, from, lines)
			ended <- followed{terminal, err}
		}()
	}
	follow(offset)
	// An adopter holds the job's lines until its own seal gives it the values to redact; the input URLs the job
	// fetched with are not asked for again.
	var held []adapter.Line
	if adopted && len(agentCredentials)+len(taskCredentials)+len(outputNames) > 0 {
		release := run.hold(lines)
		sealed, status, ok := run.awaitSeal(ctx, runCtx, &handle, agentCredentials, taskCredentials, nil, outputNames)
		held = release()
		if !ok {
			return status
		}
		run.redactionKey = sealed.RedactionKey
		run.deliver(sealed.Values)
		if _, run.puts, err = run.presigned(sealed.Values, nil, outputNames); err != nil {
			r.logger.Printf("run %s: %v", runID, err)
			return run.stopAndFail(ctx, handle, err.Error(), nil)
		}
	}
	r.logger.Printf("run %s: attempt %d in adapter %s with %s (%s)", runID, attempt, run.adapter.Name, wire.TermsText(admitted.adapter.Labels), handle.Name)
	var pending []adapter.Line
	// shutdown commits what is pending and the lines the adapter delivered that the run has not taken, under its own
	// deadline, since ctx is already done, and returns the task. A commit the shutdown cut short can leave its
	// transaction open. It ignores the delivery budget: the receiver is going away and an adopter continues the run.
	shutdown := func() kgo.AckStatus {
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
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
	if err := run.commit(ctx, run.statusRecords("started", taskStatus(wire.TaskStateWorking, ""), &offset, nil)...); err != nil {
		if ctx.Err() != nil {
			pending = held
			return shutdown()
		}
		return run.abandon(err)
	}
	// retryUntil bounds following again over a lost plugin link to one lock duration from the loss, after
	// which another member may hold the task; zero while the link holds, and reset by every line.
	var retryUntil time.Time
	var retry <-chan time.Time

	heartbeat := time.NewTicker(renewInterval)
	defer heartbeat.Stop()
	checks := time.NewTicker(stateCheckInterval)
	defer checks.Stop()
	position := offset
	take := func(line adapter.Line) {
		pending = append(pending, line)
		position = line.Next
	}
	for _, line := range held {
		take(line)
	}
	// stopHeld ends the run when the receiver holds its cancel or a denial lasting one lock interval.
	stopHeld := func() (kgo.AckStatus, bool) {
		r.mu.Lock()
		cancelled := r.cancelled[runKey{task.Wherry.Tenant, runID}]
		r.mu.Unlock()
		switch {
		case r.stopForDenial():
			stopFollow()
			return run.deny(ctx, handle, pending), true
		case cancelled:
			stopFollow()
			return run.stopAndCancel(ctx, handle, pending), true
		}
		return 0, false
	}
	for {
		select {
		case line := <-lines:
			retryUntil = time.Time{}
			take(line)
		case <-retry:
			retry = nil
			follow(position)
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
				return run.stopAndFail(ctx, handle, "timeout", pending)
			case ctx.Err() != nil:
				return shutdown()
			case errors.Is(end.err, adapter.ErrLinkLost) && (retryUntil.IsZero() || time.Now().Before(retryUntil)):
				if retryUntil.IsZero() {
					state, _, _ := r.snapshot()
					retryUntil = time.Now().Add(state.Pools[r.config.Pool].LockDuration())
					r.logger.Printf("run %s: plugin link lost; following again from offset %d", runID, position)
				}
				retry = time.After(followRetryInterval)
			case end.err != nil:
				// A dropped stream does not mean the task ended; stop it rather than leave it running.
				r.logger.Printf("run %s: follow failed: %s", runID, logText(run.redactText, end.err))
				return run.stopAndFail(ctx, handle, "log stream failed", pending)
			case !end.terminal.Started && end.terminal.Class == adapter.ClassFetch:
				r.logger.Printf("run %s: job never started: an S3 input was not fetched%s", runID, run.startReason(end.terminal))
				return run.stopAndFail(ctx, handle, "an S3 input was not fetched, or did not match its size and SHA-256", pending)
			case !end.terminal.Started:
				cause := "job never started (" + end.terminal.Class + ")" + run.startReason(end.terminal)
				r.logger.Printf("run %s: %s", runID, cause)
				return run.startFailure(ctx, "platform", cause)
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
				if ctx.Err() != nil {
					return shutdown()
				}
				return run.abandon(err)
			}
			pending = pending[len(pending)-held:]
		}
	}
}

// awaitSeal asks the run's host for the agent's credentials, the task's, and presigned URLs for its S3 inputs and
// outputs under a key made for this attempt, and waits for the seal while the claim is renewed. The key never leaves
// this call. A run that ends while it waits stops job, the adopted job, first. False carries the acknowledgement.
func (run *activeRun) awaitSeal(ctx, runCtx context.Context, job *adapter.Handle, credentials, taskCredentials, s3Inputs, s3Outputs []string) (signed.SealValues, kgo.AckStatus, bool) {
	r, task := run.receiver, run.task
	runID := task.Wherry.Run
	key, err := newSealKey()
	if err != nil {
		r.logger.Printf("run %s: seal key: %v", runID, err)
		return signed.SealValues{}, run.returnTask(ctx, nil), false
	}
	request := &wire.SealRequest{
		ReceiverBinding: r.binding(task, run.attempt), Key: jose.JSONWebKey{Key: &key.PublicKey}, KeyID: signed.SealKeyID(&key.PublicKey),
		Credentials: credentials, ExpiryMS: time.Now().Add(sealWait).UnixMilli(),
	}
	if len(taskCredentials) > 0 {
		request.TaskCredentials = taskCredentials
	}
	if len(s3Inputs) > 0 {
		request.S3Inputs = s3Inputs
	}
	if len(s3Outputs) > 0 {
		request.S3Outputs = s3Outputs
	}
	expected := signed.SealBinding{
		Tenant: request.Tenant, Pool: request.Pool, Receiver: request.Receiver, KeyID: request.KeyID, Run: request.Run,
		Attempt: request.Attempt, Credentials: request.Credentials, TaskCredentials: request.TaskCredentials,
		S3Inputs: request.S3Inputs, S3Outputs: request.S3Outputs, ExpiryMS: request.ExpiryMS,
	}
	// Reading from the host topic's end before the request is committed sees every answer to it.
	topic := admin.HostTopic(task.Wherry.Tenant, task.Wherry.Host)
	ends, err := kadm.NewClient(r.base).ListEndOffsets(runCtx, topic)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		r.deny(err, capRead)
		r.logger.Printf("run %s: read host topic: %v", runID, err)
		return signed.SealValues{}, run.returnTask(ctx, nil), false
	}
	partitions := map[int32]kgo.Offset{}
	ends.Each(func(end kadm.ListedOffset) { partitions[end.Partition] = kgo.NewOffset().At(end.Offset) })
	reader, err := r.newClient(kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: partitions}))
	if err != nil {
		r.logger.Printf("run %s: host topic reader: %v", runID, err)
		return signed.SealValues{}, run.returnTask(ctx, nil), false
	}
	defer reader.Close()
	if err := run.commit(ctx, run.feed(request)); err != nil {
		return signed.SealValues{}, run.abandon(err), false
	}
	waitCtx, cancel := context.WithDeadline(runCtx, time.UnixMilli(request.ExpiryMS))
	defer cancel()
	began := time.Now()
	waited := func(outcome string) {
		r.metrics.sealWaits.WithLabelValues(task.Wherry.Tenant, outcome).Inc()
		r.metrics.sealWait.WithLabelValues(task.Wherry.Tenant).Observe(time.Since(began).Seconds())
	}
	for {
		pollCtx, cancelPoll := context.WithTimeout(waitCtx, stateCheckInterval)
		fetches := reader.PollFetches(pollCtx)
		cancelPoll()
		if err := fetches.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			r.deny(err, capRead)
		}
		var values *signed.SealValues
		fetches.EachRecord(func(record *kgo.Record) {
			if values != nil || string(record.Key) != runID {
				return
			}
			opened, err := signed.OpenSeal(record.Value, expected, key, time.Now())
			if err != nil {
				// never log err: the cause may describe secret material
				r.logger.Printf("run %s: a seal for attempt %d does not open", runID, run.attempt)
				return
			}
			values = &opened
		})
		r.mu.Lock()
		cancelled := r.cancelled[runKey{task.Wherry.Tenant, runID}]
		r.mu.Unlock()
		// A stop seen in the same iteration as a seal wins, so a cancelled or expired run never dispatches.
		switch {
		case ctx.Err() != nil:
			return signed.SealValues{}, kgo.AckRelease, false
		case cancelled && job != nil:
			return signed.SealValues{}, run.stopAndCancel(ctx, *job, nil), false
		case cancelled:
			return signed.SealValues{}, run.cancel(ctx, nil), false
		case runCtx.Err() != nil && job != nil:
			waited("expired")
			return signed.SealValues{}, run.stopAndFail(ctx, *job, "timeout", nil), false
		case runCtx.Err() != nil:
			waited("expired")
			return signed.SealValues{}, run.fail(ctx, "timeout", nil), false
		case waitCtx.Err() != nil && job != nil:
			waited("adopter_refused")
			if err := run.adapter.Adapter.Stop(ctx, *job); err != nil {
				r.metrics.stopFailures.WithLabelValues(run.adapter.Name).Inc()
				r.logger.Printf("run %s: stop: %s", runID, logText(run.redactText, err))
				return signed.SealValues{}, run.returnTask(ctx, nil), false
			}
			return signed.SealValues{}, run.fail(ctx, unansweredReason(append(slices.Clone(credentials), taskCredentials...), len(s3Inputs)+len(s3Outputs) > 0), nil), false
		case waitCtx.Err() != nil:
			waited("unanswered")
			return signed.SealValues{}, run.fail(ctx, unansweredReason(append(slices.Clone(credentials), taskCredentials...), len(s3Inputs)+len(s3Outputs) > 0), nil), false
		case values != nil:
			waited("sealed")
			return *values, 0, true
		}
	}
}

// hold reads lines until the returned release, which gives back the most recent of them within heldLimit bytes and
// logs the lines it dropped. Nothing held is published before the attempt can redact it.
func (run *activeRun) hold(lines <-chan adapter.Line) func() []adapter.Line {
	stop, done := make(chan struct{}), make(chan []adapter.Line)
	go func() {
		var held []adapter.Line
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
					run.receiver.logger.Printf("run %s: dropped %d transcript lines held before the seal; the transcript resumes at offset %d", run.task.Wherry.Run, dropped, next)
				}
				done <- held
				return
			}
		}
	}()
	return func() []adapter.Line {
		close(stop)
		return <-done
	}
}

// presigned checks the seal's URLs against the pool's storage and returns the run's fetches and PUT URLs. An
// error names its input or output, never the URL. Each URL's signature joins the redacted forms under the URL's name, so
// a transcript quoting a URL in part is redacted too, with the URL's marker.
func (run *activeRun) presigned(values map[string][]byte, inputs []wire.S3Input, outputs []string) ([]adapter.Fetch, map[string]string, error) {
	state, _, _ := run.receiver.snapshot()
	storage := state.Pools[run.receiver.config.Pool].Storage
	tenant, runID := run.task.Wherry.Tenant, run.task.Wherry.Run
	signatures := map[string][]byte{}
	check := func(name, key string) (string, bool) {
		value := values[name]
		parsed, err := url.Parse(string(value))
		if err != nil || storage == nil || parsed.Scheme != "https" || parsed.User != nil || parsed.Opaque != "" || parsed.Fragment != "" ||
			parsed.Hostname()+":"+cmp.Or(parsed.Port(), "443") != storage.Host() || parsed.EscapedPath() != "/"+storage.Bucket+"/"+key {
			return "", false
		}
		signatures[name] = []byte(parsed.Query().Get("X-Amz-Signature"))
		return string(value), true
	}
	var fetches []adapter.Fetch
	for _, input := range inputs {
		get, ok := check(signed.S3InputValue(input.Name), wire.S3InputKey(tenant, runID, input.Name))
		if !ok {
			return nil, nil, errors.New("the URL for S3 input " + input.Name + " is not one of pool " + run.receiver.config.Pool + "'s storage")
		}
		fetches = append(fetches, adapter.Fetch{Name: input.Name, URL: get, Size: input.Size, SHA256: input.SHA256})
	}
	puts := map[string]string{}
	for _, output := range outputs {
		put, ok := check(signed.S3OutputValue(output), wire.S3OutputKey(tenant, runID, output))
		if !ok {
			return nil, nil, errors.New("the URL for S3 output " + output + " is not one of pool " + run.receiver.config.Pool + "'s storage")
		}
		puts[output] = put
	}
	run.deliver(signatures)
	return fetches, puts, nil
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
	// deadline is the run's; puts are the presigned PUT URLs of its S3 outputs, by name, which an attempt holds once
	// sealed.
	deadline time.Time
	puts     map[string]string
	// redactions are the forms of the values this attempt received, longest first, and redactionKey the run's key
	// from its seal.
	redactions   []redaction
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
	// claimed is when this attempt began executing the run, zero for a run ended without executing it.
	claimed time.Time
	// startCause is the redacted cause of this attempt's start failure, empty without one.
	startCause string
}

// redaction is a form of a delivered value and the marker that replaces it.
type redaction struct {
	value, marker []byte
}

// deliver keeps the forms each credential value is published in: the value without trailing CR and LF, as credential
// files written by echo or a CLI end in a newline that transcripts and outputs drop, and that form escaped as a JSON
// string, as a transcript line carries it. A value shorter than minRedact once trimmed matches ordinary text and JSON
// literals, so it is logged and left. Both forms take the marker of the value's name under the run's key, which names
// no credential. A marker holding a form of any delivered value would publish it, so that marker is [*], which no form
// of minRedact bytes fits in.
func (run *activeRun) deliver(values map[string][]byte) {
	for name, value := range values {
		trimmed := bytes.TrimRight(value, "\r\n")
		if len(trimmed) < minRedact {
			run.receiver.logger.Printf("run %s: credential %s too short to redact", run.task.Wherry.Run, name)
			continue
		}
		escaped, _ := jsontext.AppendQuote(nil, trimmed) // invalid UTF-8 is replaced, and that form never appears in a transcript
		marker := []byte(signed.RedactionMarker(run.redactionKey, name))
		run.redactions = append(run.redactions, redaction{value: trimmed, marker: marker})
		if escaped = escaped[1 : len(escaped)-1]; !bytes.Equal(escaped, trimmed) {
			run.redactions = append(run.redactions, redaction{value: escaped, marker: marker})
		}
	}
	for i, form := range run.redactions {
		if slices.ContainsFunc(run.redactions, func(other redaction) bool { return bytes.Contains(form.marker, other.value) }) {
			run.redactions[i].marker = []byte("[*]")
		}
	}
	slices.SortFunc(run.redactions, func(a, b redaction) int { return len(b.value) - len(a.value) })
	r := run.receiver
	r.liveMu.Lock()
	if r.live == nil {
		r.live = map[*activeRun][]redaction{}
	}
	r.live[run] = slices.Clone(run.redactions)
	r.liveMu.Unlock()
}

// redact replaces each form of each delivered credential value in data with its marker, in order, as bytes.ReplaceAll
// would; the result is held only while it fits limit.
func (run *activeRun) redact(data []byte, limit int64) *redactedForm {
	out := &redactedForm{limit: limit, sum: sha256.New()}
	run.redactTo(out, data)
	return out
}

// redactTo writes data redacted to out. The data passes through in pieces of redactPiece bytes, so no write, and
// nothing a form's replacer holds, is larger than a piece and the forms.
func (run *activeRun) redactTo(out io.Writer, data []byte) {
	if replaced := redactTo(out, data, run.redactions); replaced > 0 {
		run.receiver.metrics.redactions.WithLabelValues(run.task.Wherry.Tenant, cmp.Or(run.adapter.Name, "none")).Add(float64(replaced))
	}
}

// redactTo writes data to out with each of forms, longest first, replaced by its marker, and returns how many it
// replaced.
func redactTo(out io.Writer, data []byte, forms []redaction) int {
	w, replaced := out, 0
	for i := len(forms) - 1; i >= 0; i-- {
		w = &replacer{value: forms[i].value, marker: forms[i].marker, next: w, replaced: &replaced}
	}
	for piece := range slices.Chunk(data, redactPiece) {
		w.Write(piece)
	}
	if first, ok := w.(*replacer); ok {
		first.flush()
	}
	return replaced
}

// redactLive is text with every form of the values delivered to a run this receiver holds replaced by its marker, for
// text no one run owns.
func (r *Receiver) redactLive(text string) string {
	r.liveMu.Lock()
	forms := slices.Concat(slices.Collect(maps.Values(r.live))...)
	r.liveMu.Unlock()
	slices.SortFunc(forms, func(a, b redaction) int { return len(b.value) - len(a.value) })
	var out strings.Builder
	redactTo(&out, []byte(text), forms)
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

// maxLogText bounds the adapter text one log line carries, in bytes.
const maxLogText = 512

// logText is err's text for the log: an adapter.Error's op and any class, then its text, redacted by redact, without
// URLs, on one line of at most maxLogText bytes. The cap follows redaction, which could otherwise miss a value it cut.
func logText(redact func(string) string, err error) string {
	text := err.Error()
	if failure, ok := err.(*adapter.Error); ok {
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

// startReason is ": " and terminal's reason as logText gives it, or empty when the adapter gave none. A fetch failure's
// reason quotes its presigned URL, so it is omitted.
func (run *activeRun) startReason(terminal adapter.Terminal) string {
	switch {
	case terminal.Reason == "":
		return ""
	case terminal.Class == adapter.ClassFetch:
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

// replacer writes on to next what it is written with each occurrence of value replaced by marker, holding back only
// the bytes a match could still begin in.
type replacer struct {
	value, marker []byte
	next          io.Writer
	held          []byte
	replaced      *int
}

func (w *replacer) Write(p []byte) (int, error) {
	w.held = append(w.held, p...)
	for {
		i := bytes.Index(w.held, w.value)
		if i < 0 {
			break
		}
		w.next.Write(w.held[:i])
		w.next.Write(w.marker)
		w.held = w.held[i+len(w.value):]
		*w.replaced++
	}
	cut := max(len(w.held)-len(w.value)+1, 0)
	w.next.Write(w.held[:cut])
	w.held = slices.Clone(w.held[cut:])
	return len(p), nil
}

func (w *replacer) flush() {
	w.next.Write(w.held)
	w.held = nil
	if next, ok := w.next.(*replacer); ok {
		next.flush()
	}
}

func (run *activeRun) commit(ctx context.Context, records ...*kgo.Record) error {
	return run.receiver.transact(ctx, run.producer, records...)
}

// abandon stops all activity for a run whose writes failed; a fenced holder leaves the run to its adopter.
func (run *activeRun) abandon(err error) kgo.AckStatus {
	if errors.Is(err, kerr.InvalidProducerEpoch) || errors.Is(err, kerr.ProducerFenced) {
		run.receiver.metrics.fenced.Inc()
		run.receiver.logger.Printf("run %s: fenced by a newer holder", run.task.Wherry.Run)
		return 0
	}
	run.receiver.deny(err, capWrite)
	run.receiver.logger.Printf("run %s: write failed: %v", run.task.Wherry.Run, err)
	return kgo.AckRelease
}

func (run *activeRun) stopAndFail(ctx context.Context, handle adapter.Handle, reason string, pending []adapter.Line) kgo.AckStatus {
	if err := run.adapter.Adapter.Stop(ctx, handle); err != nil {
		run.receiver.metrics.stopFailures.WithLabelValues(run.adapter.Name).Inc()
		run.receiver.logger.Printf("run %s: stop: %s", run.task.Wherry.Run, logText(run.redactText, err))
		return run.returnTask(ctx, pending)
	}
	return run.fail(ctx, reason, pending)
}

func (run *activeRun) stopAndCancel(ctx context.Context, handle adapter.Handle, pending []adapter.Line) kgo.AckStatus {
	if err := run.adapter.Adapter.Stop(ctx, handle); err != nil {
		run.receiver.metrics.stopFailures.WithLabelValues(run.adapter.Name).Inc()
		run.receiver.logger.Printf("run %s: stop: %s", run.task.Wherry.Run, logText(run.redactText, err))
		return run.returnTask(ctx, pending)
	}
	return run.cancel(ctx, pending)
}

// deny stops a run on a denied receiver and releases its task to the pool; the terminal status is best effort.
func (run *activeRun) deny(ctx context.Context, handle adapter.Handle, pending []adapter.Line) kgo.AckStatus {
	if err := run.adapter.Adapter.Stop(ctx, handle); err != nil {
		run.receiver.metrics.stopFailures.WithLabelValues(run.adapter.Name).Inc()
		run.receiver.logger.Printf("run %s: stop: %s", run.task.Wherry.Run, logText(run.redactText, err))
		return kgo.AckRelease
	}
	const reason = "receiver denied"
	status := taskStatus(wire.TaskStateFailed, reason)
	if err := run.commit(ctx, append(append(run.chunks(pending, true), run.result(status, reason)), run.statusRecords("terminal", status, nil, nil)...)...); err != nil {
		run.receiver.logger.Printf("run %s: publish denied status: %v", run.task.Wherry.Run, err)
	} else {
		run.ended(status.State)
	}
	run.release(ctx)
	return kgo.AckRelease
}

func (run *activeRun) fail(ctx context.Context, reason string, pending []adapter.Line) kgo.AckStatus {
	return run.end(ctx, taskStatus(wire.TaskStateFailed, reason), reason, pending, kgo.AckReject)
}

func (run *activeRun) cancel(ctx context.Context, pending []adapter.Line) kgo.AckStatus {
	return run.end(ctx, taskStatus(wire.TaskStateCanceled, cancelReason), cancelReason, pending, kgo.AckAccept)
}

const cancelReason = "canceled by user"

// unansweredReason names only the credential names and storage URLs the host did not answer.
func unansweredReason(credentials []string, urls bool) string {
	var reasons []string
	switch len(credentials) {
	case 0:
	case 1:
		reasons = append(reasons, "credential "+credentials[0])
	default:
		reasons = append(reasons, "credentials "+strings.Join(credentials, ", "))
	}
	if urls {
		reasons = append(reasons, "presigned URLs for the S3 inputs and outputs")
	}
	subject := strings.Join(reasons, " and ")
	verb, configure := "were", "configure them"
	if len(credentials) == 1 && !urls {
		verb, configure = "was", "add it to credentials"
	} else if len(credentials) > 0 && !urls {
		configure = "add them to credentials"
	} else if len(credentials) > 0 {
		configure = "add the credentials and storage"
	}
	return subject + " " + verb + " not answered within " + strings.TrimSuffix(wire.SealWait.String(), "0s") + "; " + configure + " in the host's wherry configuration, keep wherry await or wherry mcp running, and submit again"
}

// startFailure purges the run's job and returns the task to the pool for a member that can start it; cause is what the
// run's terminal message names when the return spends the delivery budget.
func (run *activeRun) startFailure(ctx context.Context, class, cause string) kgo.AckStatus {
	run.startCause = logText(run.receiver.redactLive, errors.New(cause))
	if err := run.commit(ctx, run.receiver.startFailure(run.task, run.attempt, run.adapter.Name, class)); err != nil {
		return run.abandon(err)
	}
	run.receiver.metrics.startFailures.WithLabelValues(run.adapter.Name, class).Inc()
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
		err := run.adapter.Adapter.Purge(ctx, run.task.Wherry.Run)
		if err != nil {
			run.receiver.metrics.purgeFailures.WithLabelValues(run.adapter.Name).Inc()
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

// reconcile looks up the job an ambiguous Dispatch may have created; false means it stays unresolved.
func (run *activeRun) reconcile(ctx context.Context) (adapter.Job, bool) {
	for i := 0; i < 3; i++ {
		if i > 0 {
			sleep(ctx, reconcileInterval)
		}
		job, err := run.adapter.Adapter.Lookup(ctx, run.task.Wherry.Run)
		if err == nil {
			return job, true
		}
		run.receiver.logger.Printf("run %s: lookup in adapter %s: %s", run.task.Wherry.Run, run.adapter.Name, logText(run.redactText, err))
	}
	return adapter.Job{}, false
}

// returnTask hands the task back to the pool with any job and its material in place, so the next holder's
// Lookup finds them. Once the delivery budget is spent it ends the run FAILED instead, and leaves the
// material to the adapter's sweep.
func (run *activeRun) returnTask(ctx context.Context, pending []adapter.Line) kgo.AckStatus {
	if !run.receiver.budgetSpent(run.attempt) {
		return kgo.AckRelease
	}
	if err := run.commit(ctx, run.budgetRecords(pending)...); err != nil {
		return run.abandon(err)
	}
	run.ended(wire.TaskStateFailed)
	return kgo.AckReject
}

func (run *activeRun) budgetRecords(pending []adapter.Line) []*kgo.Record {
	reason := budgetReason
	if run.startCause != "" {
		reason = fmt.Sprintf("%s after %d attempts; last: %s", budgetReason, run.attempt, run.startCause)
	}
	status := taskStatus(wire.TaskStateFailed, reason)
	return append(append(run.chunks(pending, true), run.result(status, reason)), run.statusRecords("terminal", status, nil, nil)...)
}

// conclude ends the run whose job ended as terminal says: with the status its outcome file reports, or failed for how
// it ended when it left no valid one. The outputs are collected first, whatever the exit, so the file is always read.
func (run *activeRun) conclude(ctx context.Context, handle adapter.Handle, pending []adapter.Line, terminal adapter.Terminal) kgo.AckStatus {
	// A result committed after access lapsed is untrusted, so acknowledging it would lose the run.
	if run.receiver.stopForDenial() {
		return run.deny(ctx, handle, pending)
	}
	stopped := true
	if err := run.adapter.Adapter.Stop(ctx, handle); err != nil {
		run.receiver.metrics.stopFailures.WithLabelValues(run.adapter.Name).Inc()
		run.receiver.logger.Printf("run %s: stop: %s", run.task.Wherry.Run, logText(run.redactText, err))
		// At the budget the run ends with no outputs or outcome file read, and the job is left to the adapter's sweep.
		if !run.receiver.budgetSpent(run.attempt) {
			return kgo.AckRelease
		}
		stopped = false
	}
	run.collect(ctx, handle, stopped)
	var status wire.TaskStatus
	switch {
	case run.outcome != nil:
		status = *run.outcome
	case terminal.Signal != "":
		status = taskStatus(wire.TaskStateFailed, "agent ended by "+terminal.Signal)
	case terminal.ExitCode != 0:
		status = taskStatus(wire.TaskStateFailed, "agent exited "+strconv.Itoa(terminal.ExitCode))
	default:
		status = taskStatus(wire.TaskStateFailed, "agent reported no outcome")
	}
	result := ""
	if status.Message != nil {
		result = text(*status.Message)
	}
	for _, output := range run.unreturned {
		if slices.ContainsFunc(run.expected, func(expected wire.ExpectedOutput) bool { return expected.Name == output.Name && expected.Required }) {
			status = taskStatus(wire.TaskStateFailed, "required output "+output.Name+" not returned: "+output.Reason)
			break
		}
	}
	ack := kgo.AckReject
	if status.State == wire.TaskStateCompleted {
		ack = kgo.AckAccept
	}
	return run.end(ctx, status, result, pending, ack)
}

// collect reads the outputs of a stopped job, redacts them and keeps their chunks and the result's lists of what the
// run returned and did not. S3 outputs are uploaded as Collect streams them, unredacted, since they go to the host's own
// bucket and never to the feed. Every expected output is listed as one or the other.
func (run *activeRun) collect(ctx context.Context, handle adapter.Handle, stopped bool) {
	state, _, _ := run.receiver.snapshot()
	limit := state.Pools[run.receiver.config.Pool].OutputLimit
	stored := map[string]storedOutput{}
	collected, err := run.collected(ctx, handle, stopped, limit, stored)
	if err != nil {
		reason := "failed"
		switch {
		case !stopped:
			reason = "not_stopped"
		case errors.Is(err, errOverLimits):
			reason = "over_limits"
		case errors.Is(err, context.DeadlineExceeded):
			reason = "timeout"
		}
		run.receiver.metrics.collectionFailures.WithLabelValues(run.adapter.Name, reason).Inc()
		run.receiver.logger.Printf("run %s: outputs not collected: %s", run.task.Wherry.Run, logText(run.redactText, err))
		for _, output := range run.expected {
			run.unreturned = append(run.unreturned, wire.UnreturnedOutput{Name: output.Name, Reason: "not collected"})
		}
		return
	}
	slices.SortFunc(collected, func(a, b adapter.Output) int { return strings.Compare(a.Name, b.Name) })
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
				reason = "not streamed by the adapter"
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

// collected runs the adapter's Collect under limit and uploads each streamed S3 output into stored as it arrives.
// Collect may run until the run's deadline, when its PUT URLs expire. The count and total are checked again here, so a
// plugin that ignores the limits cannot grow the run's memory.
func (run *activeRun) collected(ctx context.Context, handle adapter.Handle, stopped bool, limit int64, stored map[string]storedOutput) ([]adapter.Output, error) {
	if !stopped {
		return nil, errors.New("the job was not stopped")
	}
	until := time.Now().Add(collectTimeout)
	if len(run.puts) > 0 && run.deadline.After(until) {
		until = run.deadline
	}
	ctx, cancel := context.WithDeadline(ctx, until)
	defer cancel()
	outputs := make(chan adapter.Output)
	ended := make(chan error, 1)
	// Streamed, the outcome file counts against neither output limit.
	stream := append(s3Outputs(run.expected), wire.OutcomeFile)
	go func() {
		defer close(outputs)
		ended <- run.adapter.Adapter.Collect(ctx, handle, adapter.Limits{
			Files: wire.MaxOutputs, FileBytes: limit, TotalBytes: limit, Stream: stream, StreamBytes: wire.MaxS3Object,
		}, outputs)
	}()
	var collected []adapter.Output
	var total int64
	var over error
	for output := range outputs {
		if output.Name == wire.OutcomeDir || strings.HasPrefix(output.Name, wire.OutcomeDir+"/") {
			if output.Name == wire.OutcomeFile && output.Mode.Type() == 0 {
				run.outcome = run.readOutcome(output)
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

var errOverLimits = errors.New("the adapter returned more than the limits")

// errRedirect is what an upload's client returns for a redirect, which it never follows.
var errRedirect = errors.New("redirected")

// storageClient uploads S3 outputs. A variable so tests can trust their TLS servers.
var storageClient = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return errRedirect }}

// store uploads a streamed output to its presigned PUT URL with the Content-Length of its listing, as it reads it, and
// hashes the bytes the request sends. Its reason never quotes the URL or a response body, and is no longer than the
// longest reason a pool's limits allow for.
func (run *activeRun) store(ctx context.Context, output adapter.Output) storedOutput {
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
		reason := "not uploaded: request failed"
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

// readOutcome reads and decodes an outcome file of at most maxOutcome bytes. Any outcome but a TaskStatus of a state an
// agent may report, with a ROLE_AGENT message and for TASK_STATE_AUTH_REQUIRED the credential it needs, is nil. The
// status keeps the message's text and that credential's name, redacted.
func (run *activeRun) readOutcome(output adapter.Output) *wire.TaskStatus {
	if output.Size > maxOutcome {
		return nil
	}
	data := output.Data
	if output.Body != nil {
		data, _ = io.ReadAll(io.LimitReader(output.Body, maxOutcome))
	}
	var reported wire.TaskStatus
	if int64(len(data)) != output.Size || json.Unmarshal(data, &reported) != nil || reported.Message == nil || reported.Message.Role != wire.RoleAgent ||
		len(reported.Message.Parts) > wire.MaxParts {
		return nil
	}
	status := taskStatus(reported.State, text(*reported.Message))
	switch reported.State {
	case wire.TaskStateCompleted, wire.TaskStateFailed:
		return &status
	case wire.TaskStateAuthRequired:
		for _, part := range reported.Message.Parts {
			var needed struct {
				Credential string `json:"credential"`
			}
			if part.Data == nil || json.Unmarshal(part.Data, &needed) != nil || !wire.ValidName(needed.Credential) {
				continue
			}
			data, _ := json.Marshal(map[string]string{"credential": run.redactText(needed.Credential)}) // strings always encode
			if status.Message == nil {
				status.Message = &wire.Message{MessageID: wire.RandomID("msg-"), Role: wire.RoleAgent, Extensions: []string{wire.ExtensionURI}}
			}
			status.Message.Parts = append(status.Message.Parts, wire.Part{Data: data, MediaType: "application/json"})
			return &status
		}
	}
	return nil
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
		records = append(records, run.feed(&wire.Output{
			ArtifactUpdate: wire.TaskArtifactUpdateEvent{
				TaskID: run.task.Wherry.Run, ContextID: run.task.Wherry.Run,
				Artifact: wire.Artifact{ArtifactID: name, Parts: []wire.Part{{Raw: append(wire.StdBytes{}, piece...)}}, Extensions: []string{wire.ExtensionURI}},
				Append:   index > 0, LastChunk: len(data) == 0, Metadata: metadata,
			},
			Wherry: wire.ResultBinding{ReceiverBinding: run.receiver.binding(run.task, run.attempt), TaskHash: run.taskHash},
		}))
	}
	return records
}

// end commits the remaining transcript, the outputs, the result and the terminal status in one transaction.
func (run *activeRun) end(ctx context.Context, status wire.TaskStatus, result string, pending []adapter.Line, ack kgo.AckStatus) kgo.AckStatus {
	records := append(append(append(run.chunks(pending, true), run.outputs...), run.result(status, result)), run.statusRecords("terminal", status, nil, nil)...)
	if err := run.commit(ctx, records...); err != nil {
		return run.abandon(err)
	}
	run.ended(status.State)
	run.release(ctx)
	return ack
}

func (run *activeRun) release(ctx context.Context) {
	if err := run.adapter.Adapter.Release(ctx, run.task.Wherry.Run); err != nil {
		run.receiver.logger.Printf("run %s: release: %s", run.task.Wherry.Run, logText(run.redactText, err))
	}
}

func (run *activeRun) status(kind string, status wire.TaskStatus, offset *int64, metadata jsontext.Value) *wire.RunStatus {
	value := run.receiver.status(run.task, run.taskHash, run.attempt, kind, run.redactStatus(status), offset)
	value.StatusUpdate.Metadata = metadata
	return value
}

// redactStatus redacts the text of status's message, each text part within wire.MaxText.
func (run *activeRun) redactStatus(status wire.TaskStatus) wire.TaskStatus {
	if status.Message != nil {
		message := *status.Message
		message.Parts = slices.Clone(message.Parts)
		for i := range message.Parts {
			text := run.redactText(message.Parts[i].Text)
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
	return wire.ReceiverBinding{Receiver: r.id, Tenant: task.Wherry.Tenant, Pool: r.config.Pool, Run: task.Wherry.Run, Attempt: attempt}
}

// status builds a run status.
func (r *Receiver) status(task *wire.Submission, hash string, attempt int64, kind string, status wire.TaskStatus, offset *int64) *wire.RunStatus {
	eventID := wire.RandomID("event-")
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

// chunks are the transcript lines as artifact updates on the tenant's feed.
func (run *activeRun) chunks(lines []adapter.Line, last bool) []*kgo.Record {
	records := make([]*kgo.Record, 0, len(lines))
	for i, line := range lines {
		form := run.redact(line.Data, wire.MaxLine)
		line.Data = form.data
		part := wire.Part{Text: string(line.Data), MediaType: "application/x-ndjson"}
		switch {
		case form.over():
			metadata, _ := json.Marshal(wire.PointerMetadata{SHA256: form.hash(), Size: form.size}) // a string and an integer always encode
			part = wire.Part{URL: "unstored:oversize", MediaType: part.MediaType, Metadata: metadata}
		case len(line.Data) == 0 || !utf8.Valid(line.Data) || escapedLength(line.Data) > base64.StdEncoding.EncodedLen(len(line.Data)):
			// A text part holds only non-empty UTF-8, and no more than a raw part would once JSON-escaped, so a chunk
			// stays within the record a line of wire.MaxLine raw bytes makes; other lines travel as exact bytes.
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

// escapedLength is the length of valid UTF-8 text as a JSON string's contents.
func escapedLength(text []byte) int {
	quoted, _ := jsontext.AppendQuote(nil, text)
	return len(quoted) - 2
}

// result builds the run's final task for the feed; a result that is JSON travels as a data part.
func (run *activeRun) result(status wire.TaskStatus, result string) *kgo.Record {
	result = run.redactText(result)
	part := wire.Part{Text: result}
	switch {
	case result == "":
		part.Text = string(status.State)
	case jsontext.Value(result).IsValid():
		part = wire.Part{Data: jsontext.Value(result), MediaType: "application/json"}
	case !utf8.ValidString(result) || escapedLength([]byte(result)) > base64.StdEncoding.EncodedLen(len(result)):
		// As for a transcript line, text travels raw when escaping it would make it longer than base64 does.
		part = wire.Part{Raw: wire.StdBytes(result), MediaType: "text/plain"}
	}
	metadata, _ := json.Marshal(wire.ResultMetadata{ // strings, numbers and a string map always encode
		ReceiverID: run.receiver.id, Pool: run.receiver.config.Pool, Adapter: run.adapter.Name, Labels: run.labels, Job: run.job,
		Outputs: run.returned, S3Outputs: run.s3Returned, NotReturned: run.unreturned,
	})
	return run.feed(&wire.Result{
		Task: wire.Task{
			ID: run.task.Wherry.Run, ContextID: run.task.Wherry.Run, Status: run.redactStatus(status),
			History:   inputPointers(run.history),
			Artifacts: []wire.Artifact{{ArtifactID: "result", Parts: []wire.Part{part}, Extensions: []string{wire.ExtensionURI}}},
			Metadata:  metadata,
		},
		Wherry: wire.ResultBinding{ReceiverBinding: run.receiver.binding(run.task, run.attempt), TaskHash: run.taskHash},
	})
}

// inputPointers is history with each raw and data part replaced by an unstored:input part with its filename, media
// type, size and SHA-256: the inputs already travelled once, and carrying them again could put the result past the
// record limit.
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
			metadata, _ := json.Marshal(wire.PointerMetadata{SHA256: wire.TaskHash(content), Size: int64(len(content))}) // a string and an integer always encode
			stripped[i].Parts[j] = wire.Part{URL: "unstored:input", Filename: part.Filename, MediaType: part.MediaType, Metadata: metadata}
		}
	}
	return stripped
}

// lastOffset returns the highest started or heartbeat log offset this run reached under an earlier attempt.
func (r *Receiver) lastOffset(ctx context.Context, task *wire.Submission, hash string) (int64, error) {
	events, err := r.readEvents(ctx)
	if err != nil {
		return 0, err
	}
	var highest int64
	for _, status := range events.statuses[task.Wherry.Run] {
		if status.Wherry.Tenant != task.Wherry.Tenant || status.Wherry.TaskHash != hash ||
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

// preamble is the agent's instructions for the run. It names credentials and their targets, never their values, and
// gives full paths, so an agent without a shell finds its files. A directory the adapter does not report is named by
// its environment variable.
func preamble(directories adapter.Directories, inputs []string, credentials []wire.TaskCredential, unsupplied []string, outputs []wire.ExpectedOutput, git *wire.GitInput, run string) string {
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
		names = append(names, name+" (not supplied)")
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
		lines = append(lines, gitPreamble(git, run, input+"/"+wire.KnownHostsFile, credential, output, credentials, outputs)...)
	}
	listed := "This run has no credentials."
	if len(names) > 0 {
		listed = "This run's credentials are " + strings.Join(names, ", ") + "."
	}
	lines = append(lines, "When you finish, whatever happened, write an A2A TaskStatus JSON object to "+output+"/"+wire.OutcomeFile+", creating its directory: "+
		`{"state": "TASK_STATE_COMPLETED" | "TASK_STATE_FAILED" | "TASK_STATE_AUTH_REQUIRED", "message": {"role": "ROLE_AGENT", "parts": [{"text": "<the answer, or what went wrong>"}]}}. `+
		`For TASK_STATE_AUTH_REQUIRED, add the part {"data": {"credential": "<name>"}} naming the credential you need. `+listed)
	return strings.Join(lines, "\n") + "\n\n"
}

// gitPreamble gives the commands that clone a git input and return its changes. Each starts with git and carries no
// environment assignment or redirect, so a harness rule allowing git commands allows every one. The remote, ref and
// checkout path are single-quoted, which their grammar allows; file paths are double-quoted, so a directory named by
// its environment variable expands. A credential is read from its file by a credential helper or ssh, so its value
// reaches no command line.
//
// The clone's --config settings persist for later fetches and pushes and complement adapter.AgentEnvironment, which
// stops system, global and environment configuration being read. They guard a cooperative agent against ambient
// configuration, not a hostile one, which can override them.
func gitPreamble(git *wire.GitInput, run, knownHosts, credentialDir, outputDir string, credentials []wire.TaskCredential, outputs []wire.ExpectedOutput) []string {
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
		config = append(config, `core.sshCommand='ssh -F /dev/null `+identity+` -o IdentitiesOnly=yes -o IdentityAgent=none -o UserKnownHostsFile="`+knownHosts+
			`" -o GlobalKnownHostsFile=/dev/null -o StrictHostKeyChecking=yes -o BatchMode=yes -o ProxyCommand=none'`)
	case keyFile != "":
		config = append(config, `credential.`+git.Origin()+`.helper='!f() { test "$1" = get || exit 0; echo username=x-access-token; echo "password=$(cat "`+keyFile+`")"; }; f'`)
	}
	config = append(config, "http.followRedirects=false", "http.sslVerify=true", "protocol.allow=never", "protocol."+scheme+".allow=always", "user.name=wherry", "user.email=wherry@invalid")
	clone := "    git clone --template="
	for _, setting := range config {
		clone += " --config " + setting
	}
	lines := []string{
		"Clone the git repository " + git.Remote + " at " + git.Ref + " into " + git.Checkout() + ", relative to your working directory, with these commands. " +
			"They need git 2.30 or later: run git version first, and if it is older, stop and report that.",
		clone + " -- " + remote + " " + checkout,
		"    git -C " + checkout + " checkout " + ref + " --",
		"    git -C " + checkout + " update-ref refs/wherry/start HEAD",
		"The clone keeps its settings for later fetches and pushes; do not change them.",
	}
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
