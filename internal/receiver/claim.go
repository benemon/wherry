package receiver

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var errClaimLost = errors.New("claim lost")

type claimContextKey struct{}

// The gate permits one renewal in flight. Every check timeout ends the claim,
// so no earlier answer is pending at a later check. With one record held, all
// pending acknowledgements while executing are renewals of that record.
type runClaim struct {
	gate               chan struct{}
	mu                 sync.Mutex
	ctx                context.Context
	cancel             context.CancelCauseFunc
	share              *kgo.Client
	record             *kgo.Record
	duration           time.Duration
	sequence, answered uint64
	reported           bool
	result, failure    error
	sent, anchor       time.Time
}

func (claim *runClaim) answer(err error) {
	claim.mu.Lock()
	claim.answered, claim.result = claim.sequence, err
	if err == nil && claim.ctx.Err() == nil {
		claim.anchor = claim.sent
	}
	claim.mu.Unlock()
	if err != nil && !authorisationError(err) && claim.ctx.Err() == nil {
		claim.lose(err)
	}
}

func (claim *runClaim) renew(ctx context.Context, check bool) bool {
	deadline := time.Now().Add(2 * time.Second)
	if !check {
		claim.mu.Lock()
		deadline = claim.anchor.Add(claim.duration)
		claim.mu.Unlock()
	}
	bounded, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
acquire:
	for {
		select {
		case claim.gate <- struct{}{}:
			break acquire
		case <-bounded.Done():
			if ctx.Err() != nil || claim.ctx.Err() != nil {
				return false
			}
			if !check {
				claim.mu.Lock()
				next := claim.anchor.Add(claim.duration)
				claim.mu.Unlock()
				if next.After(deadline) {
					cancel()
					deadline = next
					bounded, cancel = context.WithDeadline(ctx, deadline)
					defer cancel()
					continue
				}
			}
			claim.lose(bounded.Err())
			return false
		}
	}
	defer func() { <-claim.gate }()
	if ctx.Err() != nil || claim.ctx.Err() != nil {
		return false
	}
	if !check {
		claim.mu.Lock()
		deadline = claim.anchor.Add(claim.duration)
		claim.mu.Unlock()
		cancel()
		bounded, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	if bounded.Err() != nil {
		claim.lose(bounded.Err())
		return false
	}
	claim.mu.Lock()
	claim.sequence++
	sequence := claim.sequence
	claim.sent = time.Now()
	claim.mu.Unlock()
	claim.record.Ack(kgo.AckRenew)
	err := claim.share.FlushAcks(bounded)
	if ctx.Err() != nil || claim.ctx.Err() != nil {
		return false
	}
	claim.mu.Lock()
	answered, result := claim.answered, claim.result
	claim.mu.Unlock()
	if err == nil && bounded.Err() == nil && answered >= sequence && result == nil {
		return true
	}
	if err != nil || bounded.Err() != nil || answered < sequence || !authorisationError(result) {
		switch {
		case err != nil:
			claim.lose(err)
		case result != nil:
			claim.lose(result)
		default:
			claim.lose(errors.New("renewal not answered"))
		}
	}
	return false
}

func (run *activeRun) claimHeld(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}
	return run.claim == nil || run.claim.renew(ctx, true)
}

func (claim *runClaim) lose(err error) {
	claim.mu.Lock()
	if claim.failure == nil {
		claim.failure = err
	}
	claim.mu.Unlock()
	claim.cancel(errClaimLost)
}

func (claim *runClaim) reportLoss(r *Receiver, runID string) bool {
	if !errors.Is(context.Cause(claim.ctx), errClaimLost) {
		return false
	}
	claim.mu.Lock()
	reported, failure := claim.reported, claim.failure
	claim.reported = true
	claim.mu.Unlock()
	if !reported {
		r.metrics.claimLost.Inc()
		r.logger.Printf("run %s: claim lost: %v", runID, failure)
	}
	return true
}
