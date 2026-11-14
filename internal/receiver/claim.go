package receiver

import (
	"cmp"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var errClaimLost = errors.New("claim lost")

// slotClient is a slot's share client, kept across the claims that end in a successful accept or reject.
type slotClient struct {
	*kgo.Client
	answers atomic.Pointer[claimAnswers]
}

// claimAnswers is the claim whose acknowledgements a slot's client answers; run is set while its task runs.
type claimAnswers struct {
	run   string
	claim *runClaim
}

// The gate permits one renewal in flight. Every check timeout ends the claim,
// so no earlier answer is pending at a later check. A kept client also carries
// answers its earlier claims left uncounted, so a claim takes an answer only
// while its renewal or its final acknowledgement is outstanding.
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
	// final is set once the final acknowledgement is sent; finalAnswered and finalErr are every answer since.
	final, finalAnswered bool
	finalErr             error
}

// receive takes an answer for the claim's outstanding renewal, when running, or its final acknowledgement, and
// reports which it was.
func (claim *runClaim) receive(running bool, err error) (renewal, final bool) {
	claim.mu.Lock()
	renewal = running && claim.answered < claim.sequence
	final = !renewal && claim.final
	if final {
		claim.finalAnswered = true
		claim.finalErr = cmp.Or(claim.finalErr, err)
	}
	claim.mu.Unlock()
	if renewal {
		claim.answer(err)
	}
	return renewal, final
}

// settle marks the claim's final acknowledgement outstanding.
func (claim *runClaim) settle() {
	claim.mu.Lock()
	claim.final = true
	claim.mu.Unlock()
}

// settled reports that the final acknowledgement was answered, only with success.
func (claim *runClaim) settled() bool {
	claim.mu.Lock()
	defer claim.mu.Unlock()
	return claim.finalAnswered && claim.finalErr == nil
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
