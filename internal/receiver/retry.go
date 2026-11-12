package receiver

import (
	"context"
	"time"

	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// retryInPlace is the acknowledgement of an execution whose start failure its holder retries within the delivery.
const retryInPlace kgo.AckStatus = -1

// delivery is a delivery's in-place start retries: when its first start failure was observed, and the run of the
// execution that returned retryInPlace.
type delivery struct {
	first time.Time
	run   *activeRun
}

// retry reports that a start failure observed at failed is inside the window from the delivery's first.
func (d *delivery) retry(failed time.Time) bool {
	if d.first.IsZero() {
		d.first = failed
	}
	return failed.Before(d.first.Add(startRetryWindow))
}

// executeDelivery executes the claimed task, again after a wait for each start failure retried in place, under one
// renewer for the whole delivery.
func (r *Receiver) executeDelivery(claiming, ctx context.Context, claim *runClaim, record *kgo.Record, admitted *admission, attempt int64) kgo.AckStatus {
	defer claim.reportLoss(r, admitted.task.Wherry.Run)
	defer renewer(ctx, claim)()
	retries := &delivery{}
	for wait := startRetryWait; ; wait = min(2*wait, startRetryMaxWait) {
		ack := r.executeAttempt(ctx, claim, record, admitted, attempt, retries)
		run := retries.run
		retries.run = nil
		if ack != retryInPlace {
			if run != nil {
				run.producer.Close()
			}
			return ack
		}
		ack, again := run.awaitRetry(claiming, ctx, min(wait, time.Until(retries.first.Add(startRetryWindow))))
		run.producer.Close()
		if !again {
			return ack
		}
	}
}

// awaitRetry waits wait for the run's in-place retry, or ends the run: released once the idle slots have closed
// after a stop or configuration change, CANCELED for a held cancel, FAILED past task_started_by, or left on a halt or a
// lost claim.
func (run *activeRun) awaitRetry(claiming, ctx context.Context, wait time.Duration) (kgo.AckStatus, bool) {
	r := run.receiver
	r.mu.Lock()
	idleClosed := r.idleClosed
	if idleClosed == nil {
		// Without Run, as for a test's claimOne, there are no idle slots to wait for.
		idleClosed = make(chan struct{})
		close(idleClosed)
	}
	select {
	case <-idleClosed:
	default:
		r.retrying.Add(1)
		defer r.retrying.Done()
	}
	r.mu.Unlock()
	bound := wire.PastBound(run.task.Wherry.TaskStartedBy)
	retry := time.NewTimer(wait)
	defer retry.Stop()
	started := time.NewTimer(time.Until(bound))
	defer started.Stop()
	checks := time.NewTicker(stateCheckInterval)
	defer checks.Stop()
	for waited := false; ; {
		switch {
		case ctx.Err() != nil && halted(ctx):
			return 0, false
		case claiming.Err() != nil || ctx.Err() != nil:
			select {
			case <-idleClosed:
			case <-ctx.Done():
			}
			if halted(ctx) {
				return 0, false
			}
			run.release(ctx)
			return run.released(ctx), false
		case run.cancelHeld():
			return run.cancel(ctx, nil), false
		case !time.Now().Before(bound):
			return run.fail(ctx, wire.NotStartedBy(run.task.Wherry.TaskStartedBy)+"; last: "+run.startCause, nil), false
		case waited:
			return 0, true
		}
		select {
		case <-retry.C:
			waited = true
		case <-claiming.Done():
		case <-ctx.Done():
		case <-started.C:
		case <-checks.C:
		}
	}
}
