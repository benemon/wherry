package client

import (
	"context"
	"fmt"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

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

// turn is what binds a turn's records: its task hash, and the echo of its admission once a receiver has written one.
type turn struct {
	hash string
	echo *wire.Echo
}

// runView is a run as the tenant feed and this host's local record show it. A run's pool comes from the local record,
// or else from its first echo; records from any other pool are not the run's.
type runView struct {
	v        view
	tenant   string
	run      string
	pool     string
	turns    map[int64]turn
	latest   int64
	statuses []*wire.RunStatus
	chunks   []*wire.Chunk
	skipped  int
}

func newRunView(v view, tenant, runID string, local *runRecord) *runView {
	r := &runView{v: v, tenant: tenant, run: runID, turns: map[int64]turn{}}
	if local != nil {
		r.pool, r.latest = local.Pool, local.Turn
		r.turns[local.Turn] = turn{hash: local.TaskHash}
	}
	return r
}

// echo binds an echo of the run, reporting whether value was one.
func (r *runView) echo(value []byte) bool {
	echo, err := admin.AuthorizeEcho(r.v.state, r.v.keys, value, r.tenant)
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

func (r *runView) status(value []byte) (*wire.RunStatus, bool) {
	if r.pool == "" {
		return nil, false
	}
	status, err := admin.AuthorizeStatus(r.v.state, r.v.keys, value, r.pool, r.tenant)
	if err != nil || status.Wherry.Run != r.run || r.turns[status.Wherry.Turn].hash != status.Wherry.TaskHash {
		return nil, false
	}
	return &status, true
}

func (r *runView) chunk(value []byte) (*wire.Chunk, bool) {
	if r.pool == "" {
		return nil, false
	}
	chunk, err := admin.AuthorizeChunk(r.v.state, r.v.keys, value, r.pool, r.tenant)
	if _, known := r.turns[chunk.Wherry.Turn]; err != nil || chunk.Wherry.Run != r.run || !known {
		return nil, false
	}
	return &chunk, true
}

// result accepts a result for an echoed turn whose history ends with that turn's message.
func (r *runView) result(value []byte) (*wire.Result, bool) {
	if r.pool == "" {
		return nil, false
	}
	result, err := admin.AuthorizeResult(r.v.state, r.v.keys, value, r.pool, r.tenant)
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

// history reads the run's records from the tenant feed: echoes first, so a status read before its turn's echo binds.
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
	r := newRunView(v, h.tenant, runID, local)
	echoes := make([]bool, len(records))
	for i, record := range records {
		echoes[i] = r.echo(record.Value)
	}
	for i, record := range records {
		if echoes[i] {
			continue
		}
		if status, ok := r.status(record.Value); ok {
			r.statuses = append(r.statuses, status)
		} else if chunk, ok := r.chunk(record.Value); ok {
			r.chunks = append(r.chunks, chunk)
		} else if _, ok := r.result(record.Value); !ok && string(record.Key) == runID {
			r.skipped++
		}
	}
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
// with the turn's terminal or interrupted status, an interrupted or rejected status, or the CANCELED terminal of an
// interrupted run. Records already on the feed are read first, so a later outcome supersedes an earlier one.
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
	starts, err := kadm.NewClient(h.kafka).ListStartOffsets(waitCtx, topic)
	if err == nil {
		err = starts.Error()
	}
	var bounds kadm.ListedOffsets
	if err == nil {
		bounds, err = kadm.NewClient(h.kafka).ListCommittedOffsets(waitCtx, topic)
	}
	if err == nil {
		err = bounds.Error()
	}
	if err != nil {
		return AwaitResult{}, err
	}
	partitions := map[int32]kgo.Offset{}
	remaining := map[int32]int64{}
	starts.Each(func(start kadm.ListedOffset) {
		partitions[start.Partition] = kgo.NewOffset().At(start.Offset)
		if end, ok := bounds.Lookup(topic, start.Partition); ok && start.Offset < end.Offset {
			remaining[start.Partition] = end.Offset
		}
	})
	reader, err := kgo.NewClient(append(h.kafka.Opts(),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		// A trailing abort marker is the last offset below the bound; it must be seen to finish.
		kgo.KeepControlRecords(),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: partitions}))...)
	if err != nil {
		return AwaitResult{}, err
	}
	defer reader.Close()
	r := newRunView(v, h.tenant, runID, local)
	var filter wire.AttemptFilter
	var outcome *AwaitResult
	var outcomeTurn int64
	for {
		if len(remaining) == 0 {
			if r.pool == "" {
				return AwaitResult{}, fmt.Errorf("await %s: %w", runID, ErrRunNotFound)
			}
			if outcome != nil && outcomeTurn == r.latest {
				outcome.Skipped = r.skipped
				return *outcome, nil
			}
		}
		fetches := reader.PollFetches(waitCtx)
		if err := waitCtx.Err(); err != nil {
			return AwaitResult{}, fmt.Errorf("await %s: %w", runID, err)
		}
		if err := fetches.Err(); err != nil {
			return AwaitResult{}, err
		}
		fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
			for _, record := range partition.Records {
				if end, ok := remaining[partition.Partition]; ok && record.Offset >= end-1 {
					delete(remaining, partition.Partition)
				}
				if record.Attrs.IsControl() || r.echo(record.Value) {
					continue
				}
				if result, ok := r.result(record.Value); ok {
					state := result.Task.Status.State
					if (wire.Terminal(state) || wire.Interrupted(state)) && filter.Admit(runID, result.Wherry.Turn, result.Wherry.Attempt) {
						outcome, outcomeTurn = &AwaitResult{Task: &result.Task, Status: result.Task.Status}, result.Wherry.Turn
					}
				} else if status, ok := r.status(record.Value); ok {
					if ends(status) && admit(&filter, status) {
						outcome, outcomeTurn = &AwaitResult{Status: status.StatusUpdate.Status}, status.Wherry.Turn
					}
				} else if string(record.Key) == runID {
					r.skipped++
				}
			}
			if end, ok := remaining[partition.Partition]; ok && len(partition.Records) == 0 && partition.LastStableOffset >= end {
				delete(remaining, partition.Partition)
			}
		})
	}
}

// ends reports whether a status ends a turn without a result to wait for.
func ends(status *wire.RunStatus) bool {
	state := status.StatusUpdate.Status.State
	switch status.Wherry.Kind {
	case "interrupted":
		return wire.Interrupted(state)
	case "rejected":
		return state == wire.TaskStateRejected
	}
	return canceled(status)
}
