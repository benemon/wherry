package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// AwaitResult is the accepted outcome of the latest turn; Task is nil when a status record ended it. Outputs are the
// turn's outputs that verified against its result, NotReturned those its receiver did not return, and Rejected
// those this host could not verify.
type AwaitResult struct {
	Task        *wire.Task
	Status      wire.TaskStatus
	Turn        int64
	Outputs     []Output
	NotReturned []wire.UnreturnedOutput
	Rejected    []wire.UnreturnedOutput
	Skipped     int
}

// Output is an output of a turn, verified against the size and SHA-256 its result lists.
type Output struct {
	Name   string
	SHA256 string
	Size   int64
	Data   []byte
}

// StatusResult is the latest verified status of a run's latest turn.
type StatusResult struct {
	RunID  string
	Pool   string
	Turn   int64
	Status wire.TaskStatus
	// Updated is the time of Status; while no receiver has reported one, this host's submission of the turn, else the
	// Kafka timestamp of the turn's echo.
	Updated time.Time
	Skipped int
}

// TurnAttempt identifies one delivery attempt within one submission turn.
type TurnAttempt struct {
	Turn    int64
	Attempt int64
}

// EventsResult contains a run's verified statuses and its transcript chunk count per turn and attempt.
type EventsResult struct {
	Statuses []wire.RunStatus
	Chunks   map[TurnAttempt]int
	Skipped  int
}

// turn binds a turn's records: its task hash, and the echo of its admission and the echo's Kafka timestamp once a
// receiver has written one.
type turn struct {
	hash     string
	echo     *wire.Echo
	echoedAt time.Time
}

// entry is one authorised record of the run, kept so the run can be derived again against a newer view.
type entry struct {
	receiver string
	echo     *wire.Echo
	// at is the echo's Kafka timestamp.
	at     time.Time
	status *wire.RunStatus
	chunk  *wire.Chunk
	output *wire.Output
	result *wire.Result
}

// runView is a run as the tenant feed and this host's local record show it. A run's pool comes from the local record,
// or else from its first echo; records from any other pool are not the run's. Everything below entries is derived
// from them by derive.
type runView struct {
	h       *Host
	v       view
	run     string
	local   *runRecord
	pool    string
	entries []entry
	// unread counts records keyed by the run that authorise as none of its records.
	unread int
	// reread holds the kids whose owner's key topic has been re-read, which bounds re-reads to one per key.
	reread map[string]bool

	turns       map[int64]turn
	latest      int64
	statuses    []*wire.RunStatus
	chunks      []*wire.Chunk
	outputs     []*wire.Output
	skipped     int
	outcome     *AwaitResult
	outcomeTurn int64
	// result is the result that set the outcome, nil when a status did.
	result *wire.Result
}

func newRunView(h *Host, v view, runID string, local *runRecord) *runView {
	r := &runView{h: h, v: v, run: runID, local: local, reread: map[string]bool{}}
	if local != nil {
		r.pool = local.Pool
	}
	return r
}

// authorize runs check, and once per kid runs it again after re-reading the whole view, when the kid's owner is not
// in it, or the owner's key topic, when the kid names a key other than a live entitled member's current one.
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

// addEcho keeps an echo of the run, reporting whether record was one.
func (r *runView) addEcho(ctx context.Context, record *kgo.Record) bool {
	value := record.Value
	echo, err := authorize(ctx, r, value, func(v view) (wire.Echo, error) { return admin.AuthorizeEcho(v.state, v.keys, value, r.h.tenant) })
	if err != nil || echo.Run != r.run {
		return false
	}
	if r.pool == "" {
		r.pool = echo.Pool
	}
	if echo.Pool != r.pool {
		r.unread++
		return true
	}
	r.entries = append(r.entries, entry{receiver: echo.Receiver, echo: &echo, at: record.Timestamp})
	return true
}

// add keeps a status, chunk or result of the run; a record keyed by the run that is none of them counts as unread.
func (r *runView) add(ctx context.Context, record *kgo.Record) {
	value := record.Value
	if r.pool != "" {
		if status, err := authorize(ctx, r, value, func(v view) (wire.RunStatus, error) {
			return admin.AuthorizeStatus(v.state, v.keys, value, r.pool, r.h.tenant)
		}); err == nil && status.Wherry.Run == r.run {
			r.entries = append(r.entries, entry{receiver: status.Wherry.Receiver, status: &status})
			return
		}
		if chunk, err := authorize(ctx, r, value, func(v view) (wire.Chunk, error) {
			return admin.AuthorizeChunk(v.state, v.keys, value, r.pool, r.h.tenant)
		}); err == nil && chunk.Wherry.Run == r.run {
			r.entries = append(r.entries, entry{receiver: chunk.Wherry.Receiver, chunk: &chunk})
			return
		}
		if output, err := authorize(ctx, r, value, func(v view) (wire.Output, error) {
			return admin.AuthorizeOutput(v.state, v.keys, value, r.pool, r.h.tenant)
		}); err == nil && output.Wherry.Run == r.run {
			r.entries = append(r.entries, entry{receiver: output.Wherry.Receiver, output: &output})
			return
		}
		if result, err := authorize(ctx, r, value, func(v view) (wire.Result, error) {
			return admin.AuthorizeResult(v.state, v.keys, value, r.pool, r.h.tenant)
		}); err == nil && result.Wherry.Run == r.run {
			r.entries = append(r.entries, entry{receiver: result.Wherry.Receiver, result: &result})
			return
		}
		// Answer reads seal requests; they shape no outcome.
		if request, err := authorize(ctx, r, value, func(v view) (wire.SealRequest, error) {
			return admin.AuthorizeSealRequest(v.state, v.keys, value, r.pool, r.h.tenant)
		}); err == nil && request.Run == r.run {
			return
		}
	}
	if string(record.Key) == r.run {
		r.unread++
	}
}

// read takes a bounded read of the feed: echoes first, so a status committed before its turn's echo still binds.
func (r *runView) read(ctx context.Context, records []*kgo.Record) {
	echoes := make([]bool, len(records))
	for i, record := range records {
		echoes[i] = r.addEcho(ctx, record)
	}
	for i, record := range records {
		if !echoes[i] {
			r.add(ctx, record)
		}
	}
	r.derive()
}

// derive binds the kept records to the run against the current view: records from a receiver that is no longer a live
// member of the run's pool are dropped, echoes bind turns, then statuses, chunks and results in feed order bind to
// their turn and set the outcome.
func (r *runView) derive() {
	live := func(receiver string) bool {
		config, ok := r.v.state.Receivers[receiver]
		return ok && config.Pool == r.pool && !r.v.state.Ended[admin.ReceiverPrincipal(receiver)] && r.v.state.Entitlements[r.h.tenant][r.pool]
	}
	r.turns, r.latest, r.statuses, r.chunks, r.outputs, r.skipped, r.outcome, r.outcomeTurn, r.result = map[int64]turn{}, 0, nil, nil, nil, r.unread, nil, 0, nil
	if r.local != nil {
		r.latest = r.local.Turn
		r.turns[r.local.Turn] = turn{hash: r.local.TaskHash}
	}
	for _, e := range r.entries {
		if e.echo == nil || !live(e.receiver) {
			continue
		}
		known, ok := r.turns[e.echo.Turn]
		if ok && known.hash != e.echo.TaskHash {
			r.skipped++
			continue
		}
		if known.echo == nil {
			r.turns[e.echo.Turn] = turn{hash: e.echo.TaskHash, echo: e.echo, echoedAt: e.at}
		}
		r.latest = max(r.latest, e.echo.Turn)
	}
	var filter wire.AttemptFilter
	for _, e := range r.entries {
		if e.echo != nil {
			continue
		}
		if !live(e.receiver) {
			r.skipped++
			continue
		}
		switch {
		case e.status != nil:
			status := e.status
			if r.turns[status.Wherry.Turn].hash != status.Wherry.TaskHash {
				r.skipped++
				continue
			}
			r.statuses = append(r.statuses, status)
			if ends(status) && admit(&filter, status) {
				r.outcome, r.outcomeTurn, r.result = &AwaitResult{Status: status.StatusUpdate.Status}, status.Wherry.Turn, nil
			}
		case e.chunk != nil:
			if r.turns[e.chunk.Wherry.Turn].hash != e.chunk.Wherry.TaskHash {
				r.skipped++
				continue
			}
			r.chunks = append(r.chunks, e.chunk)
		case e.output != nil:
			if r.turns[e.output.Wherry.Turn].hash != e.output.Wherry.TaskHash {
				r.skipped++
				continue
			}
			r.outputs = append(r.outputs, e.output)
		default:
			result := e.result
			binding := r.turns[result.Wherry.Turn]
			if binding.echo == nil || binding.hash != result.Wherry.TaskHash || len(result.Task.History) != 1 ||
				result.Task.History[0].MessageID != binding.echo.MessageID {
				r.skipped++
				continue
			}
			state := result.Task.Status.State
			if (wire.Terminal(state) || wire.Interrupted(state)) && filter.Admit(r.run, result.Wherry.Turn, result.Wherry.Attempt) {
				r.outcome, r.outcomeTurn, r.result = &AwaitResult{Task: &result.Task, Status: result.Task.Status}, result.Wherry.Turn, result
			}
		}
	}
	if r.outcome != nil && r.outcomeTurn != r.latest {
		r.outcome = nil
	}
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
	return r.status(), nil
}

// RunsResult lists runs, latest update first; Skipped counts their unverifiable records and undecodable local records.
type RunsResult struct {
	Runs    []StatusResult
	Skipped int
}

// Runs returns the status of every run in this host's local records or echoed on the tenant feed. Each run is derived
// from its own records on one read of wherry.admin and the feed. Feed records are grouped by Kafka key, so a record
// keyed other than its run counts as unreadable under that key.
func (h *Host) Runs(ctx context.Context) (RunsResult, error) {
	v, err := h.load(ctx)
	if err != nil {
		return RunsResult{}, err
	}
	records, err := admin.ReadTopic(ctx, h.kafka, admin.TenantTopic(h.tenant))
	if err != nil {
		return RunsResult{}, err
	}
	byRun := map[string][]*kgo.Record{}
	for _, record := range records {
		byRun[string(record.Key)] = append(byRun[string(record.Key)], record)
	}
	files, err := os.ReadDir(filepath.Join(h.stateDir, "runs"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return RunsResult{}, err
	}
	for _, file := range files {
		if runID, ok := strings.CutSuffix(file.Name(), ".json"); ok && !file.IsDir() {
			if _, ok := byRun[runID]; !ok {
				byRun[runID] = nil
			}
		}
	}
	result := RunsResult{Runs: []StatusResult{}}
	// One reread map and a view carried forward bound the reloads to one per unknown kid across all runs.
	reread := map[string]bool{}
	for runID, records := range byRun {
		local, err := h.localRun(runID)
		if errors.Is(err, errUndecodable) {
			local = nil
			result.Skipped++
		} else if err != nil {
			return RunsResult{}, err
		}
		r := newRunView(h, v, runID, local)
		r.reread = reread
		r.read(ctx, records)
		v = r.v
		if r.pool != "" {
			status := r.status()
			result.Runs = append(result.Runs, status)
			result.Skipped += status.Skipped
		}
	}
	sort.Slice(result.Runs, func(i, j int) bool {
		if !result.Runs[i].Updated.Equal(result.Runs[j].Updated) {
			return result.Runs[i].Updated.After(result.Runs[j].Updated)
		}
		return result.Runs[i].RunID < result.Runs[j].RunID
	})
	return result, nil
}

// status is the latest verified status of the run's latest turn, SUBMITTED until a receiver reports one.
func (r *runView) status() StatusResult {
	result := StatusResult{RunID: r.run, Pool: r.pool, Turn: r.latest, Status: wire.TaskStatus{State: wire.TaskStateSubmitted}, Skipped: r.skipped}
	if r.local != nil && r.local.SubmittedMS > 0 {
		result.Updated = time.UnixMilli(r.local.SubmittedMS)
	} else {
		result.Updated = r.turns[r.latest].echoedAt
	}
	var filter wire.AttemptFilter
	for _, status := range r.statuses {
		if status.Wherry.Turn == r.latest && admit(&filter, status) {
			result.Status = status.StatusUpdate.Status
			result.Updated, _ = time.Parse(time.RFC3339, result.Status.Timestamp) // decoding validated it
		}
	}
	return result
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
	}
	for _, chunk := range r.chunks {
		result.Chunks[TurnAttempt{Turn: chunk.Wherry.Turn, Attempt: chunk.Wherry.Attempt}]++
	}
	return result, nil
}

// Await follows the tenant feed until the run's latest turn has an outcome: its result, which the receiver commits
// with the turn's terminal or interrupted status, an interrupted or rejected status, or a CANCELED terminal. Records
// already on the feed are read first, so a later outcome supersedes an earlier one. Whenever records of the run
// arrive, wherry.admin and the receivers' keys are re-read and the run is derived again from every record kept, so
// nothing from a member ended by then shapes the outcome. While it waits, the host answers seal requests.
func (h *Host) Await(ctx context.Context, runID string, timeout time.Duration) (AwaitResult, error) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	answered := make(chan struct{})
	defer func() { cancel(); <-answered }()
	go func() {
		defer close(answered)
		if err := h.Answer(waitCtx); err != nil {
			h.logger.Printf("answer seal requests: %v", err)
		}
	}()
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
	rederive := func() error {
		v, err := h.load(waitCtx)
		if err != nil {
			return fmt.Errorf("await %s: %w", runID, err)
		}
		r.v, r.reread = v, map[string]bool{}
		r.derive()
		return nil
	}
	if err := rederive(); err != nil {
		return AwaitResult{}, err
	}
	for {
		if r.outcome != nil {
			r.outcome.Skipped, r.outcome.Turn = r.skipped, r.outcomeTurn
			if r.result != nil {
				r.outcome.Outputs, r.outcome.NotReturned, r.outcome.Rejected = r.verifiedOutputs(r.result)
			}
			return *r.outcome, nil
		}
		fetches := reader.PollFetches(waitCtx)
		if err := waitCtx.Err(); err != nil {
			return AwaitResult{}, fmt.Errorf("await %s: %w", runID, err)
		}
		if err := fetches.Err(); err != nil {
			return AwaitResult{}, err
		}
		kept, unread := len(r.entries), r.unread
		fetches.EachRecord(func(record *kgo.Record) {
			if !r.addEcho(waitCtx, record) {
				r.add(waitCtx, record)
			}
		})
		if len(r.entries) == kept && r.unread == unread {
			continue
		}
		if err := rederive(); err != nil {
			return AwaitResult{}, err
		}
	}
}

// verifiedOutputs joins, for each output the result lists, the chunks its own attempt wrote, and verifies them against
// the listed size and SHA-256. An output whose chunks have a gap, an overlap or a duplicate index, or whose bytes do
// not match, is rejected. It also returns the outputs the result lists as not returned.
func (r *runView) verifiedOutputs(result *wire.Result) ([]Output, []wire.UnreturnedOutput, []wire.UnreturnedOutput) {
	var manifest struct {
		Outputs     []wire.ReturnedOutput   `json:"outputs"`
		NotReturned []wire.UnreturnedOutput `json:"not_returned"`
	}
	// The metadata also holds the placement members; a manifest that does not decode lists nothing.
	if json.Unmarshal(result.Task.Metadata, &manifest) != nil {
		return nil, nil, nil
	}
	chunks := map[string]map[int64]*wire.Output{}
	duplicated := map[string]bool{}
	for _, chunk := range r.outputs {
		if chunk.Wherry != result.Wherry {
			continue
		}
		var metadata wire.OutputMetadata
		_ = json.Unmarshal(chunk.ArtifactUpdate.Metadata, &metadata) // validated when the chunk was parsed
		name := chunk.ArtifactUpdate.Artifact.ArtifactID
		if chunks[name] == nil {
			chunks[name] = map[int64]*wire.Output{}
		}
		if _, duplicate := chunks[name][metadata.Index]; duplicate {
			duplicated[name] = true
		}
		chunks[name][metadata.Index] = chunk
	}
	var verified []Output
	var rejected []wire.UnreturnedOutput
	for _, listed := range manifest.Outputs {
		reason, data := "", []byte{}
		if duplicated[listed.Name] {
			reason = "duplicate chunk"
		}
		for index := int64(0); reason == ""; index++ {
			chunk, ok := chunks[listed.Name][index]
			if !ok {
				reason = "gap"
				break
			}
			data = append(data, chunk.ArtifactUpdate.Artifact.Parts[0].Raw...)
			if chunk.ArtifactUpdate.LastChunk {
				if int64(len(chunks[listed.Name])) != index+1 {
					reason = "overlap"
				}
				break
			}
		}
		sum := sha256.Sum256(data)
		if reason == "" && (int64(len(data)) != listed.Size || hex.EncodeToString(sum[:]) != listed.SHA256) {
			reason = "size or hash mismatch"
		}
		if reason != "" {
			rejected = append(rejected, wire.UnreturnedOutput{Name: listed.Name, Size: max(listed.Size, 0), Reason: "rejected by this host: " + reason})
			continue
		}
		verified = append(verified, Output{Name: listed.Name, SHA256: listed.SHA256, Size: listed.Size, Data: data})
	}
	return verified, manifest.NotReturned, rejected
}

// WriteOutputs writes a turn's verified outputs beneath dir, by default <state_dir>/runs/<run>/outputs/<turn>, and
// returns their paths. Every write stays inside dir, whatever links it holds.
func (h *Host) WriteOutputs(runID string, turn int64, dir string, outputs []Output) ([]string, error) {
	if dir == "" {
		if _, ok := h.runPath(runID); !ok {
			return nil, errors.New("run ID does not name a local run path")
		}
		dir = filepath.Join(h.stateDir, "runs", runID, "outputs", strconv.FormatInt(turn, 10))
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	paths := []string{}
	for _, output := range outputs {
		if !wire.ValidOutputName(output.Name) {
			return nil, errors.New("an output name is not a relative path of portable file names")
		}
		if parent := path.Dir(output.Name); parent != "." {
			if err := root.MkdirAll(parent, 0o700); err != nil {
				return nil, err
			}
		}
		if err := root.WriteFile(output.Name, output.Data, 0o600); err != nil {
			return nil, err
		}
		paths = append(paths, filepath.Join(dir, filepath.FromSlash(output.Name)))
	}
	return paths, nil
}
