package receiver

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// keyLostReason ends a run whose envelope no holder of a pool key can open.
const keyLostReason = "cannot be opened: the pool's custodian changed and its key was lost"

// claimantLease is how old the previous holder's advertisement at a claim must be when the custodian decides, for a
// new claimant to be served without its release.
const claimantLease = 120 * time.Second

// openInterval is how often a receiver holding a pool key serves open requests; openWait is how long the custodian
// waits for a key to open a task after a request is appended, and openGrace how much longer its claimant waits for an
// answer. Variables so tests can shorten them.
var (
	openInterval = 2 * time.Second
	openWait     = 5 * time.Minute
	openGrace    = 120 * time.Second
)

// newAttemptKey is a variable so tests can watch the key an attempt opens with.
var newAttemptKey = func() (*ecdsa.PrivateKey, error) { return ecdsa.GenerateKey(elliptic.P256(), rand.Reader) }

// awaitOpen requests the task's content key under a key made for this attempt and waits for the custodian's attempt
// wrap or refusal until min(the request's append time plus openWait and openGrace, bound's deadline), returning the
// envelope's values checked against the task, or false with the acknowledgement; the attempt key never leaves this call.
func (run *activeRun) awaitOpen(ctx, bound context.Context, job *pluginsdk.Handle) (signed.EnvelopeValues, kgo.AckStatus, bool) {
	r, task := run.receiver, run.task
	began := time.Now()
	waited := func(outcome string) {
		r.metrics.openWaits.WithLabelValues(task.Wherry.Tenant, outcome).Inc()
		r.metrics.openWait.WithLabelValues(task.Wherry.Tenant).Observe(time.Since(began).Seconds())
	}
	for requested := 0; ; requested++ {
		values, reason, status, done := run.openOnce(ctx, bound, job, waited)
		switch {
		case values != nil:
			return *values, 0, true
		case done:
			return signed.EnvelopeValues{}, status, false
		case reason == wire.RefusedTwoClaimants || reason == wire.RefusedNotClaimant || reason == wire.RefusedPoolSuspended:
			return signed.EnvelopeValues{}, kgo.AckRelease, false
		case reason == wire.RefusedTerminal:
			return signed.EnvelopeValues{}, kgo.AckAccept, false
		case reason == wire.RefusedPastBound:
			return signed.EnvelopeValues{}, run.expired(ctx, job, nil), false
		case reason == wire.RefusedEnvelopeInvalid && job != nil:
			return signed.EnvelopeValues{}, run.stopAndFail(ctx, *job, reason, nil), false
		case reason == wire.RefusedEnvelopeInvalid:
			return signed.EnvelopeValues{}, run.fail(ctx, reason, nil), false
		case job != nil && requested == 0:
			r.logger.Printf("run %s: refused %s; requesting once more", task.Wherry.Run, reason)
		case job != nil:
			return signed.EnvelopeValues{}, run.stopAndFail(ctx, *job, keyLostReason, nil), false
		case run.firstDelivery:
			return signed.EnvelopeValues{}, run.keyLost(ctx), false
		default:
			return signed.EnvelopeValues{}, run.reconcileKey(ctx), false
		}
	}
}

// openOnce writes one open request and waits for its answer: the values of an attempt wrap, or a refusal's reason, or
// done with the run's acknowledgement.
func (run *activeRun) openOnce(ctx, bound context.Context, job *pluginsdk.Handle, waited func(string)) (*signed.EnvelopeValues, string, kgo.AckStatus, bool) {
	r, task := run.receiver, run.task
	runID := task.Wherry.Run
	topic := admin.PoolEventsTopic(r.config.Pool)
	key, err := newAttemptKey()
	if err != nil {
		r.logger.Printf("run %s: attempt key: %v", runID, err)
		return nil, "", run.returnTask(ctx, nil), true
	}
	request := r.sign(&wire.OpenRequest{Run: runID, Attempt: run.attempt, Receiver: r.id, TaskHash: run.taskHash, Key: wire.NewAttemptKey(&key.PublicKey)})
	digest := wire.Digest(request)
	// Reading from the events' end before the request is committed sees every answer to it.
	ends, err := admin.ListOffsets(ctx, kadm.NewClient(r.base).ListEndOffsets, topic)
	if err != nil {
		r.deny(err, capRead)
		r.logger.Printf("run %s: read pool events: %v", runID, err)
		return nil, "", run.returnTask(ctx, nil), true
	}
	partitions := map[int32]kgo.Offset{}
	ends.Each(func(end kadm.ListedOffset) { partitions[end.Partition] = kgo.NewOffset().At(end.Offset) })
	reader, err := r.newClient(kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: partitions}), kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	if err != nil {
		r.logger.Printf("run %s: pool events reader: %v", runID, err)
		return nil, "", run.returnTask(ctx, nil), true
	}
	defer reader.Close()
	if err := run.commit(ctx, &kgo.Record{Topic: topic, Key: []byte(runID), Value: request}); err != nil {
		return nil, "", run.abandon(err), true
	}
	// The answer is awaited from the request's append time once it is read back.
	deadline := time.Now().Add(openWait + openGrace)
	for {
		pollCtx, cancelPoll := context.WithTimeout(bound, stateCheckInterval)
		fetches := reader.PollFetches(pollCtx)
		cancelPoll()
		if err := fetches.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			r.deny(err, capRead)
		}
		var values *signed.EnvelopeValues
		var refused, failure string
		fetches.EachRecord(func(record *kgo.Record) {
			if values != nil || refused != "" || failure != "" || string(record.Key) != runID {
				return
			}
			switch recordType(record.Value) {
			case wire.OpenRequestType:
				if bytes.Equal(record.Value, request) {
					deadline = record.Timestamp.Add(openWait + openGrace)
				}
			case wire.OpenRefusalType:
				var refusal wire.OpenRefusal
				if r.fromLiveMember(record, &refusal) && refusal.Request == digest && r.isCustodian(ctx, refusal.Custodian) {
					refused = refusal.Reason
				}
			case wire.AttemptWrapType:
				var wrap wire.AttemptWrap
				if !r.fromLiveMember(record, &wrap) || wrap.Request != digest || wrap.Receiver != r.id || wrap.Attempt != run.attempt || wrap.TaskHash != run.taskHash ||
					!r.isCustodian(ctx, wrap.Custodian) {
					return
				}
				opened, err := run.open(key, []byte(wrap.JWE))
				switch {
				case errors.Is(err, errWrapKey):
					r.metrics.ignoredWraps.Inc()
					r.logger.Printf("run %s: ignored an attempt wrap whose key does not open the envelope", runID)
				case err != nil:
					failure = err.Error()
				default:
					values = &opened
				}
			}
		})
		r.mu.Lock()
		cancelled := r.cancelled[runKey{task.Wherry.Tenant, runID}]
		r.mu.Unlock()
		// A stop seen in the same iteration as an answer wins, so a cancelled or expired run never dispatches.
		switch {
		case ctx.Err() != nil:
			return nil, "", kgo.AckRelease, true
		case cancelled && job != nil:
			return nil, "", run.stopAndCancel(ctx, *job, nil), true
		case cancelled:
			return nil, "", run.cancel(ctx, nil), true
		case bound.Err() != nil:
			waited("expired")
			return nil, "", run.expired(ctx, job, nil), true
		case failure != "":
			waited("opened")
			r.logger.Printf("run %s: %s", runID, failure)
			if job != nil {
				return nil, "", run.stopAndFail(ctx, *job, failure, nil), true
			}
			return nil, "", run.fail(ctx, failure, nil), true
		case values != nil:
			waited("opened")
			return values, "", 0, true
		case refused != "":
			waited("refused")
			return nil, refused, 0, false
		case !time.Now().Before(deadline):
			// Silence asserts nothing about the key: the task goes back to the pool.
			waited("unanswered")
			r.logger.Printf("run %s: no answer to the open request for attempt %d", runID, run.attempt)
			return nil, "", kgo.AckRelease, true
		}
	}
}

// fromLiveMember decodes record into value as a live pool member's.
func (r *Receiver) fromLiveMember(record *kgo.Record, value wire.Record) bool {
	_, peers, _ := r.snapshot()
	return wire.ParseStrict(record.Value, peers, record.Timestamp, value) == nil
}

// isCustodian reports that writer is the current snapshot's custodian, the only writer of attempt wraps and refusals.
func (r *Receiver) isCustodian(ctx context.Context, writer string) bool {
	readCtx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	latest, err := r.readCurrent(readCtx)
	return err == nil && latest.snapshot != nil && latest.snapshot.Custodian == writer
}

// errWrapKey is a wrap whose key does not decrypt the task's envelope.
var errWrapKey = errors.New("the wrap's key does not open the envelope")

// open unwraps jwe with key and opens the task's envelope, checked against the task: its binding and the section 5
// checks under the pool's terms.
func (run *activeRun) open(key *ecdsa.PrivateKey, jwe []byte) (signed.EnvelopeValues, error) {
	contentKey, err := signed.UnwrapContentKey(jwe, key)
	if err != nil {
		return signed.EnvelopeValues{}, errWrapKey
	}
	values, err := signed.OpenEnvelope([]byte(run.task.Wherry.Envelope), contentKey)
	if err != nil {
		return signed.EnvelopeValues{}, errWrapKey
	}
	return values, checkOpened(run.terms(), run.task, values)
}

// checkOpened is the check of an opened envelope against its task, by the custodian before an attempt wrap and by the
// claimant at open.
func checkOpened(terms admin.Terms, task *wire.Submission, values signed.EnvelopeValues) error {
	if err := wire.CheckBinding(task, values.Binding); err != nil {
		return err
	}
	return admin.CheckEnvelope(terms, task, values)
}

// keyLost ends a run that never started and whose envelope cannot be opened, for its host to resubmit.
func (run *activeRun) keyLost(ctx context.Context) kgo.AckStatus {
	run.event = wire.EventKeyLost
	return run.fail(ctx, keyLostReason, nil)
}

// reconcileKey ends a run refused key unavailable from its job's state: key-lost only for a job absent or never
// started, and past its bound once task_ended_by passes with Lookup unresolved.
func (run *activeRun) reconcileKey(ctx context.Context) kgo.AckStatus {
	written := false
	for {
		lookupCtx, cancel := context.WithTimeout(ctx, min(readTimeout, max(time.Until(run.deadline), 0)))
		job, found, err := run.findJob(ctx, lookupCtx)
		cancel()
		switch {
		case err == nil && !found:
			return run.keyLost(ctx)
		case err == nil && job.State == pluginsdk.JobNeverStarted:
			if err := run.purge(ctx); err != nil {
				return run.returnTask(ctx, nil)
			}
			return run.keyLost(ctx)
		case err == nil:
			return run.stopAndFail(ctx, job.Handle, keyLostReason, nil)
		case ctx.Err() != nil:
			return kgo.AckRelease
		case errors.Is(err, errMisplaced):
			return run.returnTask(ctx, nil)
		case !time.Now().Before(run.deadline):
			return run.unresolved(ctx)
		}
		if !written {
			if err := run.commit(ctx, run.poolStatus("reconciling", wire.TaskStateWorking)); err != nil {
				return run.abandon(err)
			}
			written = true
		}
		sleep(ctx, reconcileInterval)
	}
}

// poolStatus is the run's status of kind on the pool's events alone: released, adopting and reconciling are the pool's.
func (run *activeRun) poolStatus(kind string, state wire.TaskState) *kgo.Record {
	return &kgo.Record{Topic: admin.PoolEventsTopic(run.receiver.config.Pool), Key: []byte(run.task.Wherry.Run), Value: run.receiver.sign(run.status(kind, taskStatus(state, ""), nil, nil))}
}

// released commits the run's released status, giving up its claim, before its task goes back to the pool. A fenced
// holder leaves the task to its adopter; a released status not committed leaves the task to its acquisition lock.
func (run *activeRun) released(ctx context.Context) kgo.AckStatus {
	releaseCtx, cancel := run.receiver.shutdownContext(ctx)
	defer cancel()
	// A transaction a cut-short commit left open is aborted first.
	err := run.producer.EndTransaction(releaseCtx, kgo.TryAbort)
	if err == nil {
		err = run.commit(releaseCtx, run.poolStatus("released", wire.TaskStateSubmitted))
	}
	if fenced(err) {
		return run.abandon(err)
	}
	if err != nil {
		run.receiver.logger.Printf("run %s: publish released: %v; the task returns when its lock expires", run.task.Wherry.Run, err)
		return 0
	}
	return kgo.AckRelease
}

// firstClaim reports that no claim of the task other than this attempt's is on events.
func (run *activeRun) firstClaim(events poolEvents) bool {
	return !slices.ContainsFunc(events.statuses[run.task.Wherry.Run], func(status wire.RunStatus) bool {
		return status.Wherry.Kind == "claimed" && status.Wherry.TaskHash == run.taskHash && (status.Wherry.Receiver != run.receiver.id || status.Wherry.Attempt != run.attempt)
	})
}

// serveOpens answers the pool's open requests. As custodian it checks each claim and wraps the task's content key to the
// request's key or refuses; holding a pool key that is not current, it re-keys each task it can open to the current
// key.
func (r *Receiver) serveOpens(ctx context.Context) error {
	r.custody.mu.Lock()
	defer r.custody.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	keys, err := loadPoolKeys(r.poolKeysDir())
	if err != nil || len(keys) == 0 {
		return err
	}
	latest, err := r.readCurrent(ctx)
	if err != nil || latest.snapshot == nil {
		return err
	}
	current := latest.snapshot.Current
	custodian := latest.names(r) && r.custody.producer != nil && keys[current.KeyID] != nil
	if !custodian && keys[current.KeyID] != nil {
		return nil
	}
	// One read of wherry.admin decides the pass's suspension. A pass begins once every attempt wrap of the previous one
	// committed, so a suspension acknowledged here holds for every first start decided after it.
	var suspended bool
	if custodian {
		state, err := loadState(r.reader, ctx)
		if err != nil {
			r.deny(err, capRead)
			return fmt.Errorf("read wherry.admin: %w", err)
		}
		suspended = state.Suspended[r.config.Pool]
		if err := r.acknowledgeSuspension(ctx, keys, latest, state); err != nil {
			return err
		}
		// A transaction a failed pass left open is aborted before the events are read, so an answer whose commit was
		// unconfirmed but landed is seen as answered.
		if err := r.custody.producer.EndTransaction(ctx, kgo.TryAbort); err != nil {
			if fenced(err) {
				r.contest()
			} else {
				r.deny(err, capWrite)
			}
			return fmt.Errorf("abort an open transaction: %w", err)
		}
	}
	events, err := r.readEvents(ctx)
	if err != nil {
		return err
	}
	var pending []openRequest
	for _, request := range events.requests {
		if !events.answered[request.digest] {
			pending = append(pending, request)
		}
	}
	if len(pending) == 0 {
		return nil
	}
	if !custodian {
		return r.rekey(ctx, pending, events, keys, current)
	}
	check := &claimCheck{r: r, events: events, keys: keys, now: time.Now(), suspended: suspended}
	for _, request := range pending {
		records := check.decide(ctx, request)
		if len(records) == 0 {
			continue
		}
		if err := r.transact(ctx, r.custody.producer, records...); err != nil {
			if fenced(err) {
				r.contest()
			} else {
				r.deny(err, capWrite)
			}
			return fmt.Errorf("answer an open request: %w", err)
		}
		if check.changed != "" {
			r.metrics.claimantChanges.WithLabelValues(check.changed).Inc()
		}
	}
	return nil
}

// acknowledgeSuspension republishes latest, the custodian's snapshot, carrying the offset of state's latest suspend or
// resume record for the pool when latest does not acknowledge it.
func (r *Receiver) acknowledgeSuspension(ctx context.Context, keys map[string]*poolKey, latest current, state admin.State) error {
	marks, at := *latest.snapshot, state.SuspendChanged[r.config.Pool]
	switch suspended := state.Suspended[r.config.Pool]; {
	case suspended && at > marks.SuspendedFor:
		marks.SuspendedFor = at
	case !suspended && at > marks.ResumedFor:
		marks.ResumedFor = at
	default:
		return nil
	}
	return r.publish(ctx, keys, marks.Reason, keys[marks.Current.KeyID], marks, latest, time.Now())
}

// rekey writes, for each pending request whose claimed task the lookup finds and keys open, a re-key wrap of its content
// key to current, unless one is on the events.
func (r *Receiver) rekey(ctx context.Context, pending []openRequest, events poolEvents, keys map[string]*poolKey, current wire.PoolKey) error {
	done := map[string]bool{}
	for _, request := range pending {
		if request.request == nil || done[request.request.TaskHash] {
			continue
		}
		hash := request.request.TaskHash
		if slices.ContainsFunc(events.rekeys[hash], func(wrap wire.RekeyWrap) bool { return wrap.RecipientKID == current.KeyID }) {
			continue
		}
		_, task, _, err := r.lookup(ctx, events, request.request)
		if err != nil {
			return err
		}
		if task == nil {
			continue
		}
		contentKey := r.contentKey(task.record, keys, events.rekeys[hash])
		if contentKey == nil {
			continue
		}
		record, err := r.rekeyRecord(request.request.Run, hash, contentKey, current)
		if err != nil {
			return err
		}
		if err := r.admissionCommit(ctx, record); err != nil {
			return fmt.Errorf("re-key run %s: %w", request.request.Run, err)
		}
		done[hash] = true
		r.logger.Printf("run %s: re-keyed its content key to pool key %s", request.request.Run, current.KeyID)
	}
	return nil
}

// rekeyRecord is a re-key wrap of run's content key to current.
func (r *Receiver) rekeyRecord(run, hash string, contentKey []byte, current wire.PoolKey) (*kgo.Record, error) {
	jwe, err := signed.WrapContentKey(contentKey, current.Key.Key.(*ecdsa.PublicKey))
	if err != nil {
		return nil, err
	}
	value := r.sign(&wire.RekeyWrap{Holder: r.id, TaskHash: hash, RecipientKID: current.KeyID, JWE: string(jwe)})
	return &kgo.Record{Topic: admin.PoolEventsTopic(r.config.Pool), Key: []byte(run), Value: value}, nil
}

// contentKey recovers a task's content key from its submit wrap or a re-key wrap to a key in keys; a wrap whose key does
// not decrypt the envelope is ignored and counted. Nil when none opens it.
func (r *Receiver) contentKey(task *wire.Submission, keys map[string]*poolKey, rekeys []wire.RekeyWrap) []byte {
	wraps := []string{task.Wherry.Wrap}
	for _, rekey := range rekeys {
		if keys[rekey.RecipientKID] != nil {
			wraps = append(wraps, rekey.JWE)
		}
	}
	for _, wrap := range wraps {
		for _, key := range keys {
			contentKey, err := signed.UnwrapContentKey([]byte(wrap), key.Key.Key.(*ecdsa.PrivateKey))
			if err != nil {
				continue
			}
			if signed.CheckContentKey([]byte(task.Wherry.Envelope), contentKey) == nil {
				return contentKey
			}
			r.metrics.ignoredWraps.Inc()
		}
	}
	return nil
}

// claimCheck is one custodian pass's view for its claim checks; suspended is the pool's suspension read at the pass's
// start, and changed the reason of the claimant change the last decision served, empty for none.
type claimCheck struct {
	r         *Receiver
	events    poolEvents
	keys      map[string]*poolKey
	now       time.Time
	suspended bool
	ads       []*kgo.Record
	adsRead   bool
	changed   string
}

// decide checks one open request against the claim check's four rules and returns the records answering it, in one
// transaction: a new claim decision with the attempt wrap or the refusal. None while the custodian waits for a key.
func (c *claimCheck) decide(ctx context.Context, opened openRequest) []*kgo.Record {
	r := c.r
	c.changed = ""
	var decision *wire.ClaimDecision
	answer := func(value wire.Record) []*kgo.Record {
		event := func(value wire.Record) *kgo.Record {
			return &kgo.Record{Topic: admin.PoolEventsTopic(r.config.Pool), Key: opened.record.Key, Value: r.sign(value)}
		}
		if decision == nil {
			return []*kgo.Record{event(value)}
		}
		return []*kgo.Record{event(decision), event(value)}
	}
	refuse := func(reason string) []*kgo.Record {
		r.logger.Printf("custodian: refused an open request for run %s: %s", opened.record.Key, reason)
		return answer(&wire.OpenRefusal{Custodian: r.id, Request: opened.digest, Reason: reason})
	}
	request := opened.request
	owners, task, reason, err := r.lookup(ctx, c.events, request)
	switch {
	case err != nil:
		r.logger.Printf("custodian: read run %s's task: %v", opened.record.Key, err)
		return nil
	case reason != "":
		return refuse(reason)
	}
	binding, statuses := task.record.Wherry, c.events.statuses[request.Run]
	adopting := slices.ContainsFunc(statuses, func(status wire.RunStatus) bool {
		return status.Wherry.TaskHash == task.hash && status.Wherry.Kind == "adopting" && status.Wherry.Receiver == request.Receiver && status.Wherry.Attempt == request.Attempt
	})
	started := slices.ContainsFunc(statuses, func(status wire.RunStatus) bool {
		return status.Wherry.TaskHash == task.hash && status.Wherry.Kind == "started"
	})
	// The open bound comes from the events: a started job or this claim's adoption extends it to task_ended_by.
	bound := binding.TaskStartedBy
	if started || adopting {
		bound = binding.TaskEndedBy
	}
	if !c.now.Before(wire.PastBound(bound)) {
		return refuse(wire.RefusedPastBound)
	}
	// A suspended pool starts no job; only this claim's adoption, written once Lookup found the job, is served.
	if !adopting && c.suspended {
		return refuse(wire.RefusedPoolSuspended)
	}
	// Rule 6, decided once per claim: a change of claimant is served once the previous one released or its lease lapsed.
	claimDigest := wire.Digest(c.events.records[request.Run][owners[len(owners)-1]].Value)
	decided, reused := c.events.decisions[claimDigest]
	if !reused {
		decision = c.claimDecision(ctx, claimDigest, statuses, owners)
		decided = *decision
	}
	if !decided.Served {
		return refuse(wire.RefusedTwoClaimants)
	}
	contentKey := r.contentKey(task.record, c.keys, c.events.rekeys[task.hash])
	if contentKey == nil {
		if c.now.Before(opened.record.Timestamp.Add(openWait)) && c.now.Before(wire.PastBound(bound)) {
			return nil
		}
		return refuse(wire.RefusedKeyUnavailable)
	}
	// A spliced envelope never releases its content key.
	values, err := signed.OpenEnvelope([]byte(binding.Envelope), contentKey)
	if err == nil {
		state, _, _ := r.snapshot()
		err = checkOpened(state.Pools[r.config.Pool], task.record, values)
	}
	if err != nil {
		r.logger.Printf("custodian: run %s's envelope does not open for its task: %v", request.Run, err)
		return refuse(wire.RefusedEnvelopeInvalid)
	}
	attemptKey, _ := request.Key.Public() // checked when the request was read
	jwe, err := signed.WrapContentKey(contentKey, attemptKey)
	if err != nil {
		r.logger.Printf("custodian: wrap run %s's content key: %v", request.Run, err)
		return nil
	}
	if decision != nil && decision.Reason != wire.ClaimUnchanged {
		c.changed = decision.Reason
	}
	return answer(&wire.AttemptWrap{Custodian: r.id, TaskHash: task.hash, Request: opened.digest, Receiver: request.Receiver, Attempt: request.Attempt, JWE: string(jwe)})
}

// lookup applies rules 1 and 3 and the terminal or cancelled refusal to request, then rule 2: the claim's task offset
// holds the run's task. It returns the indexes of the run's ownership events of the task, the claim last, and the task,
// or the refusal; an error leaves the request unanswered.
func (r *Receiver) lookup(ctx context.Context, events poolEvents, request *wire.OpenRequest) ([]int, *runSubmission, string, error) {
	// Rule 1: the request verifies as the live pool member it names.
	if state, _, _ := r.snapshot(); request == nil || !poolMember(state, r.config.Pool, request.Receiver) {
		return nil, nil, wire.RefusedNotClaimant, nil
	}
	// Rule 3: the latest ownership event is the requester's claim for its attempt.
	statuses := events.statuses[request.Run]
	var owners []int
	for i, status := range statuses {
		if status.Wherry.TaskHash == request.TaskHash && (status.Wherry.Kind == "claimed" || status.Wherry.Kind == "released") {
			owners = append(owners, i)
		}
	}
	if len(owners) == 0 {
		return nil, nil, wire.RefusedNotClaimant, nil
	}
	claim := statuses[owners[len(owners)-1]]
	if claim.Wherry.Kind != "claimed" || claim.Wherry.Receiver != request.Receiver || claim.Wherry.Attempt != request.Attempt {
		return nil, nil, wire.RefusedNotClaimant, nil
	}
	if events.committedTerminal(request.Run, request.TaskHash) || r.cancelledRun(ctx, events, request.Run, claim.Wherry.Tenant) {
		return nil, nil, wire.RefusedTerminal, nil
	}
	// Rule 2: the record at the claim's task offset is the run's task, an authorised host's under the run's key and
	// derived ID, its bounds holding at its append time.
	if events.task(request.Run) != request.TaskHash {
		return nil, nil, wire.RefusedNotClaimant, nil
	}
	record, err := r.reader.ReadAt(ctx, admin.PoolTasksTopic(r.config.Pool), *claim.Wherry.TaskOffset)
	if err != nil {
		r.deny(err, capRead)
		return nil, nil, "", err
	}
	if record == nil || string(record.Key) != request.Run || wire.TaskHash(record.Value) != request.TaskHash {
		return nil, nil, wire.RefusedNotClaimant, nil
	}
	state, _, hosts := r.snapshot()
	submission, err := authorizeSubmission(state, hosts, record.Value, r.config.Pool)
	if err != nil || submission.Wherry.Run != request.Run || wire.CheckRunID(submission.Wherry) != nil {
		return nil, nil, wire.RefusedNotClaimant, nil
	}
	task := &runSubmission{record: &submission, hash: request.TaskHash, submitted: record.Timestamp}
	if !admissible(*task, state.Pools[r.config.Pool]) {
		return nil, nil, wire.RefusedNotClaimant, nil
	}
	return owners, task, "", nil
}

// poolMember reports that receiver is a live member of pool.
func poolMember(state admin.State, pool, receiver string) bool {
	config, member := state.Receivers[receiver]
	return member && config.Pool == pool && !state.Ended[admin.ReceiverPrincipal(receiver)]
}

// claimDecision decides the claim statuses[owners[last]] against its previous holder, the receiver of the latest earlier
// claim served by its claim decision and not followed by that receiver's own release, refusing a change of claimant
// while the previous holder is a live member whose advertisement is within claimantLease.
func (c *claimCheck) claimDecision(ctx context.Context, digest string, statuses []wire.RunStatus, owners []int) *wire.ClaimDecision {
	decision := &wire.ClaimDecision{Custodian: c.r.id, Claim: digest, Reason: wire.ClaimUnchanged, Served: true}
	claim := statuses[owners[len(owners)-1]]
	// A claim refused or never decided holds nothing; a release ends only its own receiver's claim.
	var previous *wire.RunStatus
	released := map[string]bool{}
	for _, i := range slices.Backward(owners[:len(owners)-1]) {
		if status := statuses[i]; status.Wherry.Kind == "released" {
			released[status.Wherry.Receiver] = true
		} else if !released[status.Wherry.Receiver] && c.events.decisions[wire.Digest(c.events.records[claim.Wherry.Run][i].Value)].Served {
			previous = &status
			break
		}
	}
	if previous == nil {
		if len(owners) > 1 {
			decision.Previous, decision.Reason = statuses[owners[len(owners)-2]].Wherry.Receiver, wire.ClaimReleased
		}
		return decision
	}
	if previous.Wherry.Receiver == claim.Wherry.Receiver {
		return decision
	}
	decision.Previous = previous.Wherry.Receiver
	decision.Reason = wire.ClaimLease
	state, peers, _ := c.r.snapshot()
	if !poolMember(state, c.r.config.Pool, previous.Wherry.Receiver) {
		return decision
	}
	if !c.adsRead {
		readCtx, cancel := context.WithTimeout(ctx, readTimeout)
		ads, err := c.r.reader.ReadFrom(readCtx, admin.ReceiversTopic, 0)
		cancel()
		if err != nil {
			// Without the advertisements the previous holder is taken as live.
			c.r.logger.Printf("custodian: read advertisements: %v", err)
			decision.Reason, decision.Served = wire.ClaimLive, false
			return decision
		}
		c.ads, c.adsRead = ads, true
	}
	appended := c.events.records[claim.Wherry.Run][owners[len(owners)-1]].Timestamp
	for _, ad := range slices.Backward(c.ads) {
		if string(ad.Key) != previous.Wherry.Receiver || ad.Timestamp.After(appended) {
			continue
		}
		if advertisement, err := admin.AuthorizeAdvertisement(state, peers, ad.Value); err != nil || advertisement.Receiver != previous.Wherry.Receiver {
			continue
		}
		decision.Advertisement = &wire.AdvertisedRef{Offset: ad.Offset, AtMS: ad.Timestamp.UnixMilli()}
		if c.now.Sub(ad.Timestamp) <= claimantLease {
			decision.Reason, decision.Served = wire.ClaimLive, false
		}
		break
	}
	return decision
}
