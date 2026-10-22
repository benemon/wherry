package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// AwaitResult is the accepted outcome of a run; Task is nil when a status record ended it.
type AwaitResult struct {
	Task   *wire.Task
	Status wire.TaskStatus
	// Model is the model the run's placement names, and ServedModels the models the agent's result reports serving it.
	Model        string
	ServedModels []string
	// Outputs are the run's outputs that verified against its result.
	Outputs []Output
	// Stored are the S3 outputs the result lists for DownloadOutputs.
	Stored []wire.ReturnedOutput
	// NotReturned are the outputs the turn's receiver did not return.
	NotReturned []wire.UnreturnedOutput
	// Rejected are the outputs this host could not verify.
	Rejected []wire.UnreturnedOutput
	// Redacted counts, by name, the redaction markers of the run's values in Task's artifacts and Status, where each
	// marker reads [redacted:<name>] instead.
	Redacted map[string]int
	Skipped  int
}

// Output is an output of a run, verified against the size and SHA-256 its result lists: its bytes, or for an S3
// output the Path DownloadOutputs wrote it to.
type Output struct {
	Name   string
	SHA256 string
	Size   int64
	Data   []byte
	Path   string
}

// StatusResult is the latest verified status of a run.
type StatusResult struct {
	RunID  string
	Pool   string
	Status wire.TaskStatus
	// Model is the model the run's placement names.
	Model string
	// Updated is the time of Status; while no receiver has reported one, this host's submission, else the echo's time.
	Updated time.Time
	// Redacted counts, by name, the redaction markers of the run's values in Status, where each reads
	// [redacted:<name>] instead.
	Redacted map[string]int
	Skipped  int
}

// EventsResult contains a run's verified statuses and its transcript chunk count per attempt.
type EventsResult struct {
	Statuses []wire.RunStatus
	Chunks   map[int64]int
	// Redacted counts, by name, the redaction markers of the run's values in the statuses' messages, where each reads
	// [redacted:<name>] instead.
	Redacted map[string]int
	Skipped  int
}

// TranscriptLine is one line of an agent's output, as the single part of its chunk carries it.
type TranscriptLine struct {
	// Seq is the line's byte offset in the agent's output.
	Seq     int64
	Attempt int64
	Part    wire.Part
}

// TranscriptResult is a run's transcript. Attempt is its highest attempt, which supersedes the others.
type TranscriptResult struct {
	Attempt int64
	Lines   []TranscriptLine
	// Redacted counts, by name, the redaction markers of the run's values in Lines, where each reads
	// [redacted:<name>] instead.
	Redacted map[string]int
	Skipped  int
}

// binding binds a run's records: its task hash, and the echo of its admission and the echo's Kafka timestamp once a
// receiver has written one.
type binding struct {
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

	binding  binding
	statuses []*wire.RunStatus
	chunks   []*wire.Chunk
	outputs  []*wire.Output
	skipped  int
	outcome  *AwaitResult
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

// read takes a bounded read of the feed: echoes first, so a status committed before its run's echo still binds.
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

// derive binds the kept records to the run against the current view, dropping those from a receiver that is no longer a
// live member of the run's pool.
func (r *runView) derive() {
	live := func(receiver string) bool {
		config, ok := r.v.state.Receivers[receiver]
		return ok && config.Pool == r.pool && !r.v.state.Ended[admin.ReceiverPrincipal(receiver)] && r.v.state.Entitlements[r.h.tenant][r.pool]
	}
	r.binding, r.statuses, r.chunks, r.outputs, r.skipped, r.outcome, r.result = binding{}, nil, nil, nil, r.unread, nil, nil
	if r.local != nil {
		r.binding.hash = r.local.TaskHash
	}
	for _, e := range r.entries {
		if e.echo == nil || !live(e.receiver) {
			continue
		}
		if r.binding.hash != "" && r.binding.hash != e.echo.TaskHash {
			r.skipped++
			continue
		}
		if r.binding.echo == nil {
			r.binding = binding{hash: e.echo.TaskHash, echo: e.echo, echoedAt: e.at}
		}
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
			if r.binding.hash != status.Wherry.TaskHash {
				r.skipped++
				continue
			}
			r.statuses = append(r.statuses, status)
			if ends(status) && admit(&filter, status) {
				r.outcome, r.result = &AwaitResult{Status: status.StatusUpdate.Status}, nil
			}
		case e.chunk != nil:
			if r.binding.hash != e.chunk.Wherry.TaskHash {
				r.skipped++
				continue
			}
			r.chunks = append(r.chunks, e.chunk)
		case e.output != nil:
			if r.binding.hash != e.output.Wherry.TaskHash {
				r.skipped++
				continue
			}
			r.outputs = append(r.outputs, e.output)
		default:
			result := e.result
			if r.binding.echo == nil || r.binding.hash != result.Wherry.TaskHash || len(result.Task.History) != 1 ||
				result.Task.History[0].MessageID != r.binding.echo.MessageID {
				r.skipped++
				continue
			}
			state := result.Task.Status.State
			if wire.Terminal(state) && filter.Admit(r.run, result.Wherry.Attempt) {
				r.outcome, r.result = &AwaitResult{Task: &result.Task, Status: result.Task.Status}, result
			}
		}
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

// admit applies filter to a status.
func admit(filter *wire.AttemptFilter, status *wire.RunStatus) bool {
	return filter.Admit(status.Wherry.Run, status.Wherry.Attempt)
}

// ends reports whether a status ends a run without a result to wait for.
func ends(status *wire.RunStatus) bool {
	state := status.StatusUpdate.Status.State
	switch status.Wherry.Kind {
	case "rejected":
		return state == wire.TaskStateRejected
	case "terminal":
		return state == wire.TaskStateCanceled
	}
	return false
}

// Status returns the latest verified status of runID, SUBMITTED until a receiver reports one.
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

// Runs returns the status of every run in this host's local records or echoed on the tenant feed. Feed records are
// grouped by Kafka key, so a record keyed other than its run counts as unreadable under that key.
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

// status is the latest verified status of the run, SUBMITTED until a receiver reports one.
func (r *runView) status() StatusResult {
	result := StatusResult{RunID: r.run, Pool: r.pool, Status: wire.TaskStatus{State: wire.TaskStateSubmitted}, Model: r.model(), Skipped: r.skipped}
	if r.local != nil && r.local.SubmittedMS > 0 {
		result.Updated = time.UnixMilli(r.local.SubmittedMS)
	} else {
		result.Updated = r.binding.echoedAt
	}
	var filter wire.AttemptFilter
	for _, status := range r.statuses {
		if admit(&filter, status) {
			result.Status = status.StatusUpdate.Status
			result.Updated, _ = time.Parse(time.RFC3339, result.Status.Timestamp) // decoding validated it
		}
	}
	result.Redacted = map[string]int{}
	result.Status = unredactStatus(result.Status, r.h.markers(r.local), result.Redacted)
	return result
}

// model is the model of the run's placement: this host's record of it, else its echo's.
func (r *runView) model() string {
	if r.local != nil {
		return r.local.Placement.Model
	}
	if echo := r.binding.echo; echo != nil {
		return echo.Placement.Model
	}
	return ""
}

// markers maps each redaction marker the run's key gives to its name: the credentials this host configures, the
// run's task credentials, and its S3 objects' URLs. The feed carries only the markers.
func (h *Host) markers(local *runRecord) map[string]string {
	if local == nil || len(local.RedactionKey) != 32 {
		return nil
	}
	names := slices.Concat(slices.Collect(maps.Keys(h.credentials)), local.TaskCredentials)
	for _, input := range local.S3Inputs {
		names = append(names, signed.S3InputValue(input))
	}
	for _, output := range local.S3Outputs {
		names = append(names, signed.S3OutputValue(output))
	}
	markers := map[string]string{}
	for _, name := range names {
		markers[signed.RedactionMarker(local.RedactionKey, name)] = name
	}
	return markers
}

// unredact replaces each marker in text with [redacted:<name>] and counts it under its name.
func unredact(text []byte, markers map[string]string, counts map[string]int) []byte {
	for marker, name := range markers {
		if n := bytes.Count(text, []byte(marker)); n > 0 {
			counts[name] += n
			text = bytes.ReplaceAll(text, []byte(marker), []byte("[redacted:"+name+"]"))
		}
	}
	return text
}

func unredactParts(parts []wire.Part, markers map[string]string, counts map[string]int) []wire.Part {
	parts = slices.Clone(parts)
	for i, part := range parts {
		parts[i].Text = string(unredact([]byte(part.Text), markers, counts))
		if part.Raw != nil {
			parts[i].Raw = unredact(part.Raw, markers, counts)
		}
		if part.Data != nil {
			parts[i].Data = unredact(part.Data, markers, counts)
		}
	}
	return parts
}

func unredactStatus(status wire.TaskStatus, markers map[string]string, counts map[string]int) wire.TaskStatus {
	if status.Message != nil {
		message := *status.Message
		message.Parts = unredactParts(message.Parts, markers, counts)
		status.Message = &message
	}
	return status
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
	result := EventsResult{Chunks: map[int64]int{}, Redacted: map[string]int{}, Skipped: r.skipped}
	markers := h.markers(r.local)
	seen := map[string]bool{}
	for _, status := range r.statuses {
		if seen[status.Wherry.EventID] {
			continue
		}
		seen[status.Wherry.EventID] = true
		record := *status
		record.StatusUpdate.Status = unredactStatus(record.StatusUpdate.Status, markers, result.Redacted)
		result.Statuses = append(result.Statuses, record)
	}
	for _, chunk := range r.chunks {
		result.Chunks[chunk.Wherry.Attempt]++
	}
	return result, nil
}

// Transcript returns the transcript of runID in seq order: the lines of the
// highest attempt, and before the first of them, or in place of them while it has none, each line of the highest
// earlier attempt that has it. An adopter resumes the job's output past the offset of the last heartbeat.
func (h *Host) Transcript(ctx context.Context, runID string) (TranscriptResult, error) {
	r, err := h.history(ctx, runID)
	if err != nil {
		return TranscriptResult{}, err
	}
	if r.pool == "" {
		return TranscriptResult{}, fmt.Errorf("transcript %s: %w", runID, ErrRunNotFound)
	}
	result := TranscriptResult{Redacted: map[string]int{}, Skipped: r.skipped}
	for _, status := range r.statuses {
		result.Attempt = max(result.Attempt, status.Wherry.Attempt)
	}
	for _, chunk := range r.chunks {
		result.Attempt = max(result.Attempt, chunk.Wherry.Attempt)
	}
	var chunks []*wire.Chunk
	first := int64(math.MaxInt64)
	for _, chunk := range r.chunks {
		chunks = append(chunks, chunk)
		if chunk.Wherry.Attempt == result.Attempt {
			first = min(first, chunk.Wherry.Seq)
		}
	}
	sort.SliceStable(chunks, func(i, j int) bool { return chunks[i].Wherry.Attempt > chunks[j].Wherry.Attempt })
	markers := h.markers(r.local)
	seen := map[int64]bool{}
	for _, chunk := range chunks {
		if seen[chunk.Wherry.Seq] || chunk.Wherry.Attempt != result.Attempt && chunk.Wherry.Seq >= first {
			continue
		}
		seen[chunk.Wherry.Seq] = true
		parts := unredactParts(chunk.ArtifactUpdate.Artifact.Parts, markers, result.Redacted)
		result.Lines = append(result.Lines, TranscriptLine{Seq: chunk.Wherry.Seq, Attempt: chunk.Wherry.Attempt, Part: parts[0]})
	}
	sort.Slice(result.Lines, func(i, j int) bool { return result.Lines[i].Seq < result.Lines[j].Seq })
	return result, nil
}

// Await follows the tenant feed until the run has an outcome, answering seal requests while it waits.
// Each arrival re-reads wherry.admin and the receivers' keys and derives the run again, so nothing from a member ended
// by then shapes the outcome.
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
			outcome := *r.outcome
			outcome.Skipped, outcome.Model = r.skipped, r.model()
			if r.result != nil {
				outcome.Outputs, outcome.Stored, outcome.NotReturned, outcome.Rejected = r.verifiedOutputs(r.result)
			}
			markers := h.markers(local)
			outcome.Redacted = map[string]int{}
			outcome.Status = unredactStatus(outcome.Status, markers, outcome.Redacted)
			if outcome.Task != nil {
				for _, artifact := range outcome.Task.Artifacts {
					var metadata struct {
						ServedModels []string `json:"served_models"`
					}
					if artifact.ArtifactID == "result" && json.Unmarshal(artifact.Metadata, &metadata) == nil {
						outcome.ServedModels = metadata.ServedModels
					}
				}
				task := *outcome.Task
				task.Status, task.Artifacts = outcome.Status, slices.Clone(task.Artifacts)
				for i := range task.Artifacts {
					task.Artifacts[i].Parts = unredactParts(task.Artifacts[i].Parts, markers, outcome.Redacted)
				}
				outcome.Task = &task
			}
			return outcome, nil
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
// not match, is rejected. It also returns the S3 outputs the result lists, and the outputs it lists as not returned.
func (r *runView) verifiedOutputs(result *wire.Result) ([]Output, []wire.ReturnedOutput, []wire.UnreturnedOutput, []wire.UnreturnedOutput) {
	var manifest struct {
		Outputs     []wire.ReturnedOutput   `json:"outputs"`
		S3Outputs   []wire.ReturnedOutput   `json:"s3_outputs"`
		NotReturned []wire.UnreturnedOutput `json:"not_returned"`
	}
	// The metadata also holds the placement members; a manifest that does not decode lists nothing.
	if json.Unmarshal(result.Task.Metadata, &manifest) != nil {
		return nil, nil, nil, nil
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
	return verified, manifest.S3Outputs, manifest.NotReturned, rejected
}

// WriteOutputs writes a run's verified outputs beneath dir, by default <state_dir>/runs/<run>/outputs, and
// returns their paths. Every write stays inside dir, whatever links it holds.
func (h *Host) WriteOutputs(runID, dir string, outputs []Output) ([]string, error) {
	if dir == "" {
		if _, ok := h.runPath(runID); !ok {
			return nil, errors.New("run ID does not name a local run path")
		}
		dir = h.outputDir(runID)
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
