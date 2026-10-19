package client

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// refreshInterval bounds how long Await trusts a snapshot of wherry.admin and the receivers' keys.
const refreshInterval = 2 * time.Second

// AwaitResult is the accepted outcome of the latest turn; Task is nil when a status record ended it.
type AwaitResult struct {
	Task    *wire.Task
	Status  wire.TaskStatus
	Skipped int
}

// StatusResult is the latest verified status of a run's latest turn.
type StatusResult struct {
	Turn    int64
	Status  wire.TaskStatus
	Skipped int
}

// TurnAttempt identifies one delivery attempt within one submission turn.
type TurnAttempt struct {
	Turn    int64
	Attempt int64
}

// EventsResult contains a run's verified statuses, whether it ended canceled, and its transcript chunk count per turn
// and attempt.
type EventsResult struct {
	Statuses  []wire.RunStatus
	Cancelled bool
	Chunks    map[TurnAttempt]int
	Skipped   int
}

// turn binds a turn's records: its task hash, and the echo of its admission once a receiver has written one.
type turn struct {
	hash string
	echo *wire.Echo
}

// runView is a run as the tenant feed and this host's local record show it. A run's pool comes from the local record,
// or else from its first echo; records from any other pool are not the run's.
type runView struct {
	h        *Host
	v        view
	run      string
	pool     string
	turns    map[int64]turn
	latest   int64
	statuses []*wire.RunStatus
	chunks   []*wire.Chunk
	skipped  int
	// reread holds the kids whose owner's key topic has been re-read, which bounds re-reads to one per key.
	reread map[string]bool

	filter          wire.AttemptFilter
	outcome         *AwaitResult
	outcomeTurn     int64
	outcomeReceiver string
}

func newRunView(h *Host, v view, runID string, local *runRecord) *runView {
	r := &runView{h: h, v: v, run: runID, turns: map[int64]turn{}, reread: map[string]bool{}}
	if local != nil {
		r.pool, r.latest = local.Pool, local.Turn
		r.turns[local.Turn] = turn{hash: local.TaskHash}
	}
	return r
}

// authorize runs check against the view and, once per kid, again after re-reading what the kid may have outrun: the
// whole view for an owner the view does not know, or the key topic of a live member of an entitled pool whose kid
// names a key other than the one the view holds.
func authorize[T any](ctx context.Context, r *runView, value []byte, check func(view) (T, error)) (T, error) {
	result, err := check(r.v)
	if err == nil {
		return result, nil
	}
	jws, parseErr := jose.ParseSignedCompact(string(value), []jose.SignatureAlgorithm{jose.EdDSA})
	if parseErr != nil || len(jws.Signatures) != 1 {
		return result, err
	}
	kid := jws.Signatures[0].Protected.KeyID
	owner, keyID, _ := strings.Cut(kid, "/")
	if r.reread[kid] {
		return result, err
	}
	r.reread[kid] = true
	config, member := r.v.state.Receivers[owner]
	if !member {
		v, loadErr := r.h.load(ctx)
		if loadErr != nil {
			return result, err
		}
		r.v = v
		return check(r.v)
	}
	current := r.v.keys[owner].Current
	if r.v.state.Ended[admin.ReceiverPrincipal(owner)] || !r.v.state.Entitlements[r.h.tenant][config.Pool] || current != nil && signed.KeyID(current) == keyID {
		return result, err
	}
	keys, readErr := admin.ReadKeys(ctx, r.h.kafka, map[string]string{owner: admin.ReceiverTopic(owner)})
	if readErr != nil {
		return result, err
	}
	r.v.keys[owner] = keys[owner]
	return check(r.v)
}

// echo binds an echo of the run, reporting whether value was one.
func (r *runView) echo(ctx context.Context, value []byte) bool {
	echo, err := authorize(ctx, r, value, func(v view) (wire.Echo, error) { return admin.AuthorizeEcho(v.state, v.keys, value, r.h.tenant) })
	if err != nil || echo.Run != r.run {
		return false
	}
	if r.pool == "" {
		r.pool = echo.Pool
	}
	known, ok := r.turns[echo.Turn]
	if echo.Pool != r.pool || ok && known.hash != echo.TaskHash {
		r.skipped++
		return true
	}
	if known.echo == nil {
		r.turns[echo.Turn] = turn{hash: echo.TaskHash, echo: &echo}
	}
	r.latest = max(r.latest, echo.Turn)
	return true
}

func (r *runView) status(ctx context.Context, value []byte) (*wire.RunStatus, bool) {
	if r.pool == "" {
		return nil, false
	}
	status, err := authorize(ctx, r, value, func(v view) (wire.RunStatus, error) {
		return admin.AuthorizeStatus(v.state, v.keys, value, r.pool, r.h.tenant)
	})
	if err != nil || status.Wherry.Run != r.run || r.turns[status.Wherry.Turn].hash != status.Wherry.TaskHash {
		return nil, false
	}
	return &status, true
}

func (r *runView) chunk(ctx context.Context, value []byte) (*wire.Chunk, bool) {
	if r.pool == "" {
		return nil, false
	}
	chunk, err := authorize(ctx, r, value, func(v view) (wire.Chunk, error) {
		return admin.AuthorizeChunk(v.state, v.keys, value, r.pool, r.h.tenant)
	})
	if err != nil || chunk.Wherry.Run != r.run || r.turns[chunk.Wherry.Turn].hash != chunk.Wherry.TaskHash {
		return nil, false
	}
	return &chunk, true
}

// result accepts a result for an echoed turn whose history ends with that turn's message.
func (r *runView) result(ctx context.Context, value []byte) (*wire.Result, bool) {
	if r.pool == "" {
		return nil, false
	}
	result, err := authorize(ctx, r, value, func(v view) (wire.Result, error) {
		return admin.AuthorizeResult(v.state, v.keys, value, r.pool, r.h.tenant)
	})
	if err != nil || result.Wherry.Run != r.run {
		return nil, false
	}
	binding := r.turns[result.Wherry.Turn]
	if binding.echo == nil || binding.hash != result.Wherry.TaskHash || int64(len(result.Task.History)) < result.Wherry.Turn ||
		result.Task.History[result.Wherry.Turn-1].MessageID != binding.echo.MessageID {
		return nil, false
	}
	return &result, true
}

// read takes a bounded read of the feed: echoes first, so a status committed before its turn's echo still binds.
func (r *runView) read(ctx context.Context, records []*kgo.Record) {
	echoes := make([]bool, len(records))
	for i, record := range records {
		echoes[i] = r.echo(ctx, record.Value)
	}
	for i, record := range records {
		if !echoes[i] {
			r.record(ctx, record)
		}
	}
}

func (r *runView) record(ctx context.Context, record *kgo.Record) {
	if status, ok := r.status(ctx, record.Value); ok {
		r.statuses = append(r.statuses, status)
		if ends(status) && admit(&r.filter, status) {
			r.outcome, r.outcomeTurn, r.outcomeReceiver = &AwaitResult{Status: status.StatusUpdate.Status}, status.Wherry.Turn, status.Wherry.Receiver
		}
	} else if chunk, ok := r.chunk(ctx, record.Value); ok {
		r.chunks = append(r.chunks, chunk)
	} else if result, ok := r.result(ctx, record.Value); ok {
		state := result.Task.Status.State
		if (wire.Terminal(state) || wire.Interrupted(state)) && r.filter.Admit(r.run, result.Wherry.Turn, result.Wherry.Attempt) {
			r.outcome, r.outcomeTurn, r.outcomeReceiver = &AwaitResult{Task: &result.Task, Status: result.Task.Status}, result.Wherry.Turn, result.Wherry.Receiver
		}
	} else if string(record.Key) == r.run {
		r.skipped++
	}
}

// accepted re-checks, against the current view, that the receivers of the outcome and of its turn's echo are still
// live members of the run's pool, so nothing written after a membership end is returned. Signatures are not
// re-verified: a key rotated since the record was read would refuse a record that was valid when written.
func (r *runView) accepted() bool {
	live := func(receiver string) bool {
		config, ok := r.v.state.Receivers[receiver]
		return ok && config.Pool == r.pool && !r.v.state.Ended[admin.ReceiverPrincipal(receiver)]
	}
	echo := r.turns[r.outcomeTurn].echo
	return r.v.state.Entitlements[r.h.tenant][r.pool] && live(r.outcomeReceiver) && (echo == nil || live(echo.Receiver))
}

func (h *Host) history(ctx context.Context, runID string) (*runView, error) {
	v, err := h.load(ctx)
	if err != nil {
		return nil, err
	}
	local, err := h.localRun(runID)
	if err != nil {
		return nil, err
	}
	records, err := admin.ReadTopic(ctx, h.kafka, admin.TenantTopic(h.tenant))
	if err != nil {
		return nil, err
	}
	r := newRunView(h, v, runID, local)
	r.read(ctx, records)
	return r, nil
}

// admit applies filter to a status. Every pool member writes the CANCELED terminal of an interrupted run under one
// event ID and with its own attempt, so that terminal is always current.
func admit(filter *wire.AttemptFilter, status *wire.RunStatus) bool {
	return canceled(status) || filter.Admit(status.Wherry.Run, status.Wherry.Turn, status.Wherry.Attempt)
}

// canceled reports whether status is the CANCELED terminal of an interrupted run.
func canceled(status *wire.RunStatus) bool {
	binding := status.Wherry
	return binding.Kind == "terminal" && status.StatusUpdate.Status.State == wire.TaskStateCanceled && binding.EventID == fmt.Sprintf("%s/%d/canceled", binding.Run, binding.Turn)
}

// ends reports whether a status ends a turn without a result to wait for.
func ends(status *wire.RunStatus) bool {
	state := status.StatusUpdate.Status.State
	switch status.Wherry.Kind {
	case "interrupted":
		return wire.Interrupted(state)
	case "rejected":
		return state == wire.TaskStateRejected
	case "terminal":
		return state == wire.TaskStateCanceled
	}
	return false
}

// Status returns the latest verified status of runID's latest turn, SUBMITTED until a receiver reports one.
func (h *Host) Status(ctx context.Context, runID string) (StatusResult, error) {
	r, err := h.history(ctx, runID)
	if err != nil {
		return StatusResult{}, err
	}
	if r.pool == "" {
		return StatusResult{}, fmt.Errorf("status %s: %w", runID, ErrRunNotFound)
	}
	result := StatusResult{Turn: r.latest, Status: wire.TaskStatus{State: wire.TaskStateSubmitted}, Skipped: r.skipped}
	var filter wire.AttemptFilter
	for _, status := range r.statuses {
		if status.Wherry.Turn == r.latest && admit(&filter, status) {
			result.Status = status.StatusUpdate.Status
		}
	}
	return result, nil
}

// Events returns runID's verified statuses, one per event ID, and its transcript chunk counts.
func (h *Host) Events(ctx context.Context, runID string) (EventsResult, error) {
	r, err := h.history(ctx, runID)
	if err != nil {
		return EventsResult{}, err
	}
	if r.pool == "" {
		return EventsResult{}, fmt.Errorf("events %s: %w", runID, ErrRunNotFound)
	}
	result := EventsResult{Chunks: map[TurnAttempt]int{}, Skipped: r.skipped}
	seen := map[string]bool{}
	for _, status := range r.statuses {
		if seen[status.Wherry.EventID] {
			continue
		}
		seen[status.Wherry.EventID] = true
		result.Statuses = append(result.Statuses, *status)
		result.Cancelled = result.Cancelled || status.StatusUpdate.Status.State == wire.TaskStateCanceled
	}
	for _, chunk := range r.chunks {
		result.Chunks[TurnAttempt{Turn: chunk.Wherry.Turn, Attempt: chunk.Wherry.Attempt}]++
	}
	return result, nil
}

// Await follows the tenant feed until the run's latest turn has an outcome: its result, which the receiver commits
// with the turn's terminal or interrupted status, an interrupted or rejected status, or a CANCELED terminal. Records
// already on the feed are read first, so a later outcome supersedes an earlier one. wherry.admin and the receivers'
// keys are re-read every refreshInterval and before an outcome is returned.
func (h *Host) Await(ctx context.Context, runID string, timeout time.Duration) (AwaitResult, error) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	v, err := h.load(waitCtx)
	if err != nil {
		return AwaitResult{}, err
	}
	local, err := h.localRun(runID)
	if err != nil {
		return AwaitResult{}, err
	}
	topic := admin.TenantTopic(h.tenant)
	bounds, err := kadm.NewClient(h.kafka).ListCommittedOffsets(waitCtx, topic)
	if err == nil {
		err = bounds.Error()
	}
	if err != nil {
		return AwaitResult{}, err
	}
	records, err := admin.ReadTopic(waitCtx, h.kafka, topic)
	if err != nil {
		return AwaitResult{}, err
	}
	r := newRunView(h, v, runID, local)
	r.read(waitCtx, records)
	if r.pool == "" {
		return AwaitResult{}, fmt.Errorf("await %s: %w", runID, ErrRunNotFound)
	}
	// ReadTopic reads to its own, possibly later, bound, so following resumes after its last record.
	starts := map[int32]int64{}
	bounds.Each(func(bound kadm.ListedOffset) { starts[bound.Partition] = bound.Offset })
	for _, record := range records {
		starts[record.Partition] = max(starts[record.Partition], record.Offset+1)
	}
	partitions := map[int32]kgo.Offset{}
	for partition, offset := range starts {
		partitions[partition] = kgo.NewOffset().At(offset)
	}
	reader, err := kgo.NewClient(append(h.kafka.Opts(), kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: partitions}))...)
	if err != nil {
		return AwaitResult{}, err
	}
	defer reader.Close()
	refresh := func() error {
		v, err := h.load(waitCtx)
		if err != nil {
			return fmt.Errorf("await %s: %w", runID, err)
		}
		r.v, r.reread = v, map[string]bool{}
		return nil
	}
	for {
		if r.outcome != nil && r.outcomeTurn == r.latest {
			if err := refresh(); err != nil {
				return AwaitResult{}, err
			}
			if r.accepted() {
				r.outcome.Skipped = r.skipped
				return *r.outcome, nil
			}
			r.outcome = nil
		}
		pollCtx, cancelPoll := context.WithTimeout(waitCtx, refreshInterval)
		fetches := reader.PollFetches(pollCtx)
		cancelPoll()
		if err := waitCtx.Err(); err != nil {
			return AwaitResult{}, fmt.Errorf("await %s: %w", runID, err)
		}
		if err := fetches.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return AwaitResult{}, err
		}
		fetches.EachRecord(func(record *kgo.Record) {
			if !r.echo(waitCtx, record.Value) {
				r.record(waitCtx, record)
			}
		})
	}
}
