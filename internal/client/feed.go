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
	"sync"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

// AwaitResult is the accepted outcome of a run; Task is nil when a status record ended it.
type AwaitResult struct {
	Task   *wire.Task
	Status wire.TaskStatus
	// Model is the model the run's placement names.
	Model string
	// Outputs are the run's outputs that verified against its result.
	Outputs []Output
	// Stored are the S3 outputs the result lists for DownloadOutputs.
	Stored []wire.ReturnedOutput
	// NotReturned are the outputs the turn's receiver did not return.
	NotReturned []wire.UnreturnedOutput
	// Rejected are the outputs this host could not verify.
	Rejected []wire.UnreturnedOutput
	// Reply is the reply the result's reply chunks carry, nil when Task's result artifact and Status hold all of it.
	Reply *Reply
	// Redacted counts, by name, the redaction markers of the run's values in Task's artifacts and Status, where each
	// marker reads [redacted:<name>] instead.
	Redacted map[string]int
	// Unclaimed is set, with Status SUBMITTED, when the run's queue deadline passed and no receiver claimed it.
	Unclaimed string
	Skipped   int
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

// Reply is a run's reply over wire.MaxText, joined from its reply chunks and verified against the size and SHA-256
// its result lists, with each marker reading [redacted:<name>]. SHA256 and Size are of Data.
type Reply struct {
	Data   []byte
	SHA256 string
	Size   int64
	// Cut is set when the reply was longer than its pool's reply limit and the chunks carry its first bytes.
	Cut bool
	// Rejected is why this host could not verify the chunks, which leaves Data empty.
	Rejected string
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
	// Unclaimed is set when the run's queue deadline passed and no receiver claimed it.
	Unclaimed string
	Skipped   int
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
	// fed is the feed offset past the records history read.
	fed int64
	// requests are the run's seal requests, and requested the Kafka timestamp of each.
	requests  []wire.SealRequest
	requested []time.Time

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

// authorize runs check on a record signed under kid, and once per kid runs it again after re-reading the whole view,
// when the kid's owner is not in it, or the owner's key topic, when the kid names a key other than a live entitled
// member's current one.
func authorize[T any](ctx context.Context, r *runView, kid string, check func(view) (T, error)) (T, error) {
	result, err := check(r.v)
	if err == nil {
		return result, nil
	}
	if kid == "" {
		return result, err
	}
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
	keys, readErr := r.h.reader.ReadKeys(ctx, map[string]string{owner: admin.ReceiverTopic(owner)})
	if readErr != nil {
		return result, err
	}
	r.v.keys[owner] = keys[owner]
	return check(r.v)
}

// addEcho keeps an echo of the run signed under kid, reporting whether record was one.
func (r *runView) addEcho(ctx context.Context, record *kgo.Record, kid string) bool {
	value := record.Value
	echo, err := authorize(ctx, r, kid, func(v view) (wire.Echo, error) {
		return admin.AuthorizeEcho(wire.ParseHistory(record.Timestamp), v.state, v.keys, value, r.h.tenant)
	})
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

// add keeps a status, chunk, output or result of the run, authorised as the record type typ its JWS header names and
// signed under kid; a record keyed by the run that is none of them counts as unread.
func (r *runView) add(ctx context.Context, record *kgo.Record, typ, kid string) {
	value := record.Value
	history := wire.ParseHistory(record.Timestamp)
	if r.pool != "" {
		switch typ {
		case wire.StatusType:
			if status, err := authorize(ctx, r, kid, func(v view) (wire.RunStatus, error) {
				return admin.AuthorizeStatus(history, v.state, v.keys, value, r.pool, r.h.tenant)
			}); err == nil && status.Wherry.Run == r.run {
				r.entries = append(r.entries, entry{receiver: status.Wherry.Receiver, status: &status})
				return
			}
		case wire.ChunkType:
			if chunk, err := authorize(ctx, r, kid, func(v view) (wire.Chunk, error) {
				return admin.AuthorizeChunk(history, v.state, v.keys, value, r.pool, r.h.tenant)
			}); err == nil && chunk.Wherry.Run == r.run {
				r.entries = append(r.entries, entry{receiver: chunk.Wherry.Receiver, chunk: &chunk})
				return
			}
		case wire.OutputType:
			if output, err := authorize(ctx, r, kid, func(v view) (wire.Output, error) {
				return admin.AuthorizeOutput(history, v.state, v.keys, value, r.pool, r.h.tenant)
			}); err == nil && output.Wherry.Run == r.run {
				r.entries = append(r.entries, entry{receiver: output.Wherry.Receiver, output: &output})
				return
			}
		case wire.ResultType:
			if result, err := authorize(ctx, r, kid, func(v view) (wire.Result, error) {
				return admin.AuthorizeResult(history, v.state, v.keys, value, r.pool, r.h.tenant)
			}); err == nil && result.Wherry.Run == r.run {
				r.entries = append(r.entries, entry{receiver: result.Wherry.Receiver, result: &result})
				return
			}
		case wire.SealRequestType:
			// Answer reads seal requests; they shape no outcome.
			if request, err := authorize(ctx, r, kid, func(v view) (wire.SealRequest, error) {
				return admin.AuthorizeSealRequest(v.state, v.keys, value, r.pool, r.h.tenant)
			}); err == nil && request.Run == r.run {
				r.requests, r.requested = append(r.requests, request), append(r.requested, record.Timestamp)
				return
			}
		}
	}
	if string(record.Key) == r.run {
		r.unread++
	}
}

// read takes a bounded read of the feed: echoes first, so a status committed before its run's echo still binds. Each
// record goes to the one check its JWS type names.
func (r *runView) read(ctx context.Context, records []*kgo.Record) {
	types, kids := make([]string, len(records)), make([]string, len(records))
	echoes := make([]bool, len(records))
	for i, record := range records {
		types[i], kids[i] = header(record.Value)
		echoes[i] = types[i] == wire.EchoType && r.addEcho(ctx, record, kids[i])
	}
	for i, record := range records {
		if !echoes[i] {
			r.add(ctx, record, types[i], kids[i])
		}
	}
	r.derive()
}

// header returns the record type and kid of value's JWS header, empty when value is no single-signature JWS.
func header(value []byte) (string, string) {
	jws, err := jose.ParseSignedCompact(string(value), []jose.SignatureAlgorithm{jose.EdDSA})
	if err != nil || len(jws.Signatures) != 1 {
		return "", ""
	}
	typ, _ := jws.Signatures[0].Protected.ExtraHeaders[jose.HeaderType].(string)
	return typ, jws.Signatures[0].Protected.KeyID
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
	local, err := h.localRun(runID)
	if err != nil {
		return nil, err
	}
	h.followMu.Lock()
	f := h.follower
	h.followMu.Unlock()
	var v view
	var records []*kgo.Record
	var fed int64
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() (err error) {
		v, err = h.load(groupCtx)
		return err
	})
	group.Go(func() (err error) {
		records, fed, err = h.readFeed(groupCtx, f, local.feedOffset())
		return err
	})
	if err := group.Wait(); err != nil {
		return nil, err
	}
	// As Runs groups the feed, a record keyed by another run is never this run's.
	var keyed []*kgo.Record
	for _, record := range records {
		if string(record.Key) == runID {
			keyed = append(keyed, record)
		}
	}
	r := newRunView(h, v, runID, local)
	r.fed = fed
	r.read(ctx, keyed)
	return r, nil
}

// readTranscript adds the run's chunks and outputs from the tenant's transcript topic, read from the first offset
// appended at or after the run's earliest echo. A reader sees the feed's and the transcript topic's commits apart, so
// once the run has an outcome it reads to a last stable offset at or past the topic's end, listed after the outcome was
// seen.
func (r *runView) readTranscript(ctx context.Context) error {
	_, offset, echoed, err := r.transcriptStart(ctx)
	if err != nil || !echoed {
		return err
	}
	topic := admin.TenantTranscriptTopic(r.h.tenant)
	listing, err := r.h.reader.Admin()
	if err != nil {
		return err
	}
	var end int64
	if r.outcome != nil {
		ends, err := admin.ListOffsets(ctx, listing.ListEndOffsets, topic)
		if err != nil {
			return err
		}
		ends.Each(func(listed kadm.ListedOffset) { end = listed.Offset })
	}
	var records []*kgo.Record
	for {
		read, stable, err := r.h.reader.ReadFromWithEnd(ctx, topic, offset)
		if err != nil {
			return err
		}
		records = append(records, read...)
		if stable >= end {
			break
		}
		offset = max(offset, stable)
		select {
		case <-time.After(transcriptRetry):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	r.addTranscript(ctx, records)
	r.derive()
	return nil
}

// transcriptStart lists the partition and first offset of the tenant's transcript topic appended at or after the run's
// earliest echo, reporting false while the run has no echo.
func (r *runView) transcriptStart(ctx context.Context) (int32, int64, bool, error) {
	var echoed time.Time
	for _, e := range r.entries {
		if e.echo != nil && (echoed.IsZero() || e.at.Before(echoed)) {
			echoed = e.at
		}
	}
	// No receiver writes a run's chunks before its echo.
	if echoed.IsZero() {
		return 0, 0, false, nil
	}
	listing, err := r.h.reader.Admin()
	if err != nil {
		return 0, 0, false, err
	}
	starts, err := admin.ListOffsets(ctx, func(ctx context.Context, topics ...string) (kadm.ListedOffsets, error) {
		return listing.ListOffsetsAfterMilli(ctx, echoed.Add(-transcriptSkew).UnixMilli(), topics...)
	}, admin.TenantTranscriptTopic(r.h.tenant))
	if err != nil {
		return 0, 0, false, err
	}
	var partition int32
	var offset int64
	starts.Each(func(listed kadm.ListedOffset) { partition, offset = listed.Partition, listed.Offset })
	return partition, offset, true, nil
}

// addTranscript keeps the run's records among records read from the transcript topic.
func (r *runView) addTranscript(ctx context.Context, records []*kgo.Record) {
	for _, record := range records {
		if string(record.Key) == r.run {
			typ, kid := header(record.Value)
			r.add(ctx, record, typ, kid)
		}
	}
}

// transcriptRetry separates reads of the transcript topic waiting for its last stable offset to pass a run's last chunks.
const transcriptRetry = 50 * time.Millisecond

// transcriptSkew is how far the transcript topic's leader may stamp append times behind the feed's leader; the two
// topics can have different leaders, each with its own clock. A variable so tests can measure the window exactly.
var transcriptSkew = time.Minute

// follower reads a topic's one partition from start until the context it was started with ends. Its records are
// unverified: each reader verifies them against the state it loads.
type follower struct {
	ctx     context.Context
	start   int64
	mu      sync.Mutex
	records []*kgo.Record
	// read is the offset the follower has read up to, control records included.
	read int64
	// err is the error of the follower's last fetch, nil once a fetch succeeds.
	err error
	// woken is closed and replaced each time read advances or a fetch fails after one succeeded.
	woken chan struct{}
}

// follow returns the host's follower, first starting one under ctx when none runs. It starts at the lowest feed offset
// of the host's runs whose run deadline has not passed, or at the feed's committed end when no such run exists, and at
// no lower than the feed's log start.
func (h *Host) follow(ctx context.Context) (*follower, error) {
	h.followMu.Lock()
	defer h.followMu.Unlock()
	if h.follower != nil && h.follower.ctx.Err() == nil {
		return h.follower, nil
	}
	topic := admin.TenantTopic(h.tenant)
	listing, err := h.reader.Admin()
	if err != nil {
		return nil, err
	}
	ends, err := admin.ListOffsets(ctx, listing.ListCommittedOffsets, topic)
	if err != nil {
		return nil, err
	}
	var partition int32
	var start int64
	ends.Each(func(end kadm.ListedOffset) { partition, start = end.Partition, end.Offset })
	// Listed after the end: a run recorded later has every record about it after the end.
	files, err := os.ReadDir(filepath.Join(h.stateDir, "runs"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	now := time.Now().UnixMilli()
	for _, file := range files {
		if runID, ok := strings.CutSuffix(file.Name(), ".json"); ok && !file.IsDir() {
			// A run cannot still be running past its run deadline.
			if local, err := h.localRun(runID); err == nil && local != nil && local.DeadlineMS > now {
				start = min(start, local.FeedOffset)
			}
		}
	}
	starts, err := admin.ListOffsets(ctx, listing.ListStartOffsets, topic)
	if err != nil {
		return nil, err
	}
	// Below the log start the follower's read position never reaches the committed end.
	starts.Each(func(logStart kadm.ListedOffset) { start = max(start, logStart.Offset) })
	f, err := h.tail(ctx, topic, partition, start)
	if err != nil {
		return nil, err
	}
	h.follower = f
	return f, nil
}

// tail starts a follower of topic's partition from start under ctx.
func (h *Host) tail(ctx context.Context, topic string, partition int32, start int64) (*follower, error) {
	reader, err := kgo.NewClient(append(slices.Clone(h.kafka.Opts()), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.KeepControlRecords(), kgo.DisableClientMetrics(),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {partition: kgo.NewOffset().At(start)}}))...)
	if err != nil {
		return nil, err
	}
	f := &follower{ctx: ctx, start: start, read: start, woken: make(chan struct{})}
	go func() {
		defer reader.Close()
		var logged string
		for {
			fetches := reader.PollFetches(ctx)
			if ctx.Err() != nil {
				return
			}
			err := fetches.Err()
			if err != nil && err.Error() != logged {
				logged = err.Error()
				h.logger.Printf("follow %s: %v", topic, err)
			}
			f.mu.Lock()
			read := f.read
			fetches.EachRecord(func(record *kgo.Record) {
				if !record.Attrs.IsControl() {
					f.records = append(f.records, record)
				}
				f.read = max(f.read, record.Offset+1)
			})
			if f.read > read || (err != nil && f.err == nil) {
				close(f.woken)
				f.woken = make(chan struct{})
			}
			f.err = err
			f.mu.Unlock()
		}
	}()
	return f, nil
}

// from returns the records f has read at or after offset, the offset f has read up to, a channel closed when f reads
// further or its fetches start failing, and the error of f's last fetch.
func (f *follower) from(offset int64) ([]*kgo.Record, int64, <-chan struct{}, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := sort.Search(len(f.records), func(i int) bool { return f.records[i].Offset >= offset })
	return f.records[i:], f.read, f.woken, f.err
}

// readFeed returns the tenant feed's records from offset as readFollowed does.
func (h *Host) readFeed(ctx context.Context, f *follower, offset int64) ([]*kgo.Record, int64, error) {
	return h.readFollowed(ctx, f, admin.TenantTopic(h.tenant), offset)
}

// readFollowed returns topic's records from offset up to at least its committed end, listed first, and the offset that
// follows them. It takes them from f, a follower of topic, while f runs, if f started at or below offset, and reads them
// once otherwise or when f's fetches fail.
func (h *Host) readFollowed(ctx context.Context, f *follower, topic string, offset int64) ([]*kgo.Record, int64, error) {
	if f != nil && f.start <= offset && f.ctx.Err() == nil {
		listing, err := h.reader.Admin()
		if err != nil {
			return nil, 0, err
		}
		ends, err := admin.ListOffsets(ctx, listing.ListCommittedOffsets, topic)
		if err != nil {
			return nil, 0, err
		}
		var end int64
		ends.Each(func(listed kadm.ListedOffset) { end = listed.Offset })
		for f.ctx.Err() == nil {
			records, read, woken, err := f.from(offset)
			if read >= end {
				return records, read, nil
			}
			if err != nil {
				break
			}
			select {
			case <-woken:
			case <-f.ctx.Done():
			case <-ctx.Done():
				return nil, 0, ctx.Err()
			}
		}
	}
	return h.reader.ReadFromWithEnd(ctx, topic, offset)
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
	records, err := h.reader.ReadFrom(ctx, admin.TenantTopic(h.tenant), 0)
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
	if r.local != nil {
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
	result.Unclaimed = r.unclaimed(time.Now())
	return result
}

// unclaimed says that the run's queue deadline passed at now with no status from a receiver, and is empty otherwise. Only
// this host's record holds the queue deadline.
func (r *runView) unclaimed(now time.Time) string {
	if r.local == nil || r.local.QueueDeadlineMS == 0 || len(r.statuses) > 0 || now.UnixMilli() <= r.local.QueueDeadlineMS {
		return ""
	}
	deadline := time.UnixMilli(r.local.QueueDeadlineMS).UTC().Format(time.RFC3339)
	return "queue deadline passed at " + deadline + " and no receiver claimed the run"
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
	// An attempt's terminal status counts its chunks; only attempts without one are counted on the transcript topic.
	ended, counting := map[int64]bool{}, false
	for _, status := range r.statuses {
		if status.Wherry.Kind == "terminal" || status.Wherry.Kind == "rejected" {
			ended[status.Wherry.Attempt] = true
			if status.Wherry.TranscriptChunks > 0 {
				result.Chunks[status.Wherry.Attempt] = int(status.Wherry.TranscriptChunks)
			}
		}
	}
	for _, status := range r.statuses {
		counting = counting || !ended[status.Wherry.Attempt]
	}
	if counting {
		if err := r.readTranscript(ctx); err != nil {
			return EventsResult{}, err
		}
		result.Skipped = r.skipped
	}
	for _, chunk := range r.chunks {
		if !ended[chunk.Wherry.Attempt] {
			result.Chunks[chunk.Wherry.Attempt]++
		}
	}
	return result, nil
}

// Transcript returns the transcript of runID in seq order: the lines of the
// highest attempt, and before the first of them, or in place of them while it has none, each line of the highest
// earlier attempt that has it. An adopter resumes the job's output past the run's latest committed offset.
func (h *Host) Transcript(ctx context.Context, runID string) (TranscriptResult, error) {
	r, err := h.history(ctx, runID)
	if err != nil {
		return TranscriptResult{}, err
	}
	if r.pool == "" {
		return TranscriptResult{}, fmt.Errorf("transcript %s: %w", runID, ErrRunNotFound)
	}
	if err := r.readTranscript(ctx); err != nil {
		return TranscriptResult{}, err
	}
	return r.transcript(), nil
}

// transcript is the run's transcript as Transcript returns it, from the chunks derived so far.
func (r *runView) transcript() TranscriptResult {
	attempt, first := r.current()
	result := TranscriptResult{Attempt: attempt, Redacted: map[string]int{}, Skipped: r.skipped}
	var chunks []*wire.Chunk
	for _, chunk := range r.chunks {
		// The empty last chunk holds no line.
		if len(chunk.ArtifactUpdate.Artifact.Parts) != 0 {
			chunks = append(chunks, chunk)
		}
	}
	sort.SliceStable(chunks, func(i, j int) bool { return chunks[i].Wherry.Attempt > chunks[j].Wherry.Attempt })
	markers := r.h.markers(r.local)
	seen := map[int64]bool{}
	for _, chunk := range chunks {
		if seen[chunk.Wherry.Seq] || chunk.Wherry.Attempt != attempt && chunk.Wherry.Seq >= first {
			continue
		}
		seen[chunk.Wherry.Seq] = true
		result.Lines = append(result.Lines, transcriptLine(chunk, markers, result.Redacted))
	}
	sort.Slice(result.Lines, func(i, j int) bool { return result.Lines[i].Seq < result.Lines[j].Seq })
	return result
}

// current returns the run's highest attempt and the lowest seq of that attempt's lines, math.MaxInt64 while it has
// none.
func (r *runView) current() (int64, int64) {
	var attempt int64
	for _, status := range r.statuses {
		attempt = max(attempt, status.Wherry.Attempt)
	}
	for _, chunk := range r.chunks {
		attempt = max(attempt, chunk.Wherry.Attempt)
	}
	first := int64(math.MaxInt64)
	for _, chunk := range r.chunks {
		if chunk.Wherry.Attempt == attempt && len(chunk.ArtifactUpdate.Artifact.Parts) != 0 {
			first = min(first, chunk.Wherry.Seq)
		}
	}
	return attempt, first
}

// transcriptLine is chunk's line, its markers named and counted in counts.
func transcriptLine(chunk *wire.Chunk, markers map[string]string, counts map[string]int) TranscriptLine {
	parts := unredactParts(chunk.ArtifactUpdate.Artifact.Parts, markers, counts)
	return TranscriptLine{Seq: chunk.Wherry.Seq, Attempt: chunk.Wherry.Attempt, Part: parts[0]}
}

// TranscriptStream returns a run's transcript as its lines arrive.
type TranscriptStream struct {
	h   *Host
	run string
	// r is the run as derived once from its feed history and updated from the records taken since.
	r *runView
	// feed is the follower the run's feed records come from, and fed the feed offset past those taken.
	feed *follower
	fed  int64
	// tail is the consumer on the transcript topic once the run has an echo, and next the offset past the records
	// taken from it.
	tail *follower
	next int64
	// attempt and first are the highest attempt and its lowest seq when Next last returned the whole transcript,
	// returned the attempt of each line Next has returned by its seq, nil before the first, and redacted the markers
	// counted in those lines.
	attempt, first int64
	returned       map[int64]int64
	redacted       map[string]int
}

// StreamTranscript returns a stream of runID's transcript.
func (h *Host) StreamTranscript(runID string) *TranscriptStream {
	return &TranscriptStream{h: h, run: runID}
}

// Next returns, in seq order, the lines of the run's transcript at seqs no earlier Next returned, with Attempt and
// Redacted as Transcript would return them now. Once the highest attempt or its lowest seq changes, which can replace
// lines returned, it returns the whole transcript again; a caller printing lines skips the seqs it has printed. The
// stream's consumer, and the host's follower when none runs, run under the ctx of the Next that starts them.
func (s *TranscriptStream) Next(ctx context.Context) (TranscriptResult, error) {
	if s.r == nil {
		f, err := s.h.follow(ctx)
		if err != nil {
			return TranscriptResult{}, err
		}
		if s.r, err = s.h.history(ctx, s.run); err != nil {
			return TranscriptResult{}, err
		}
		s.feed, s.fed = f, s.r.fed
	} else {
		records, read, _, _ := s.feed.from(s.fed)
		if s.feed.ctx.Err() != nil {
			// The follower's owner stopped it; another follower, perhaps from a later offset, takes over.
			f, err := s.h.follow(ctx)
			if err != nil {
				return TranscriptResult{}, err
			}
			if records, read, err = s.h.readFeed(ctx, f, s.fed); err != nil {
				return TranscriptResult{}, err
			}
			s.feed = f
		}
		var keyed []*kgo.Record
		for _, record := range records {
			if string(record.Key) == s.run {
				keyed = append(keyed, record)
			}
		}
		s.fed = max(s.fed, read)
		s.r.read(ctx, keyed)
	}
	if s.r.pool == "" {
		return TranscriptResult{}, fmt.Errorf("transcript %s: %w", s.run, ErrRunNotFound)
	}
	topic := admin.TenantTranscriptTopic(s.h.tenant)
	if s.tail == nil {
		partition, offset, echoed, err := s.r.transcriptStart(ctx)
		if err != nil || !echoed {
			return TranscriptResult{Redacted: map[string]int{}, Skipped: s.r.skipped}, err
		}
		if s.tail, err = s.h.tail(ctx, topic, partition, offset); err != nil {
			return TranscriptResult{}, err
		}
		s.next = offset
	}
	records, read, err := s.h.readFollowed(ctx, s.tail, topic, s.next)
	if err != nil {
		return TranscriptResult{}, err
	}
	s.next = max(s.next, read)
	s.r.addTranscript(ctx, records)
	s.r.derive()
	return s.lines(), nil
}

// lines returns the transcript's lines at seqs Next has not returned, unredacting only theirs, or the whole transcript
// when the lines returned may no longer be its own.
func (s *TranscriptStream) lines() TranscriptResult {
	attempt, first := s.r.current()
	whole := func() TranscriptResult {
		result := s.r.transcript()
		s.attempt, s.first, s.returned, s.redacted = attempt, first, map[int64]int64{}, maps.Clone(result.Redacted)
		for _, line := range result.Lines {
			s.returned[line.Seq] = line.Attempt
		}
		return result
	}
	if s.returned == nil || attempt != s.attempt || first != s.first {
		return whole()
	}
	chunks := map[int64]*wire.Chunk{}
	for _, chunk := range s.r.chunks {
		seq := chunk.Wherry.Seq
		if len(chunk.ArtifactUpdate.Artifact.Parts) == 0 || chunk.Wherry.Attempt != attempt && seq >= first {
			continue
		}
		if returned, ok := s.returned[seq]; ok {
			if returned < chunk.Wherry.Attempt {
				return whole()
			}
			continue
		}
		if kept := chunks[seq]; kept == nil || kept.Wherry.Attempt < chunk.Wherry.Attempt {
			chunks[seq] = chunk
		}
	}
	markers := s.r.h.markers(s.r.local)
	result := TranscriptResult{Attempt: attempt, Skipped: s.r.skipped}
	for seq, chunk := range chunks {
		result.Lines = append(result.Lines, transcriptLine(chunk, markers, s.redacted))
		s.returned[seq] = chunk.Wherry.Attempt
	}
	sort.Slice(result.Lines, func(i, j int) bool { return result.Lines[i].Seq < result.Lines[j].Seq })
	result.Redacted = maps.Clone(s.redacted)
	return result
}

// Wait blocks until there are records past those Next last took, on the feed or once the run has an echo on the
// transcript topic, a consumer it reads from stops, or ctx ends. Before the first Next it returns at once.
func (s *TranscriptStream) Wait(ctx context.Context) {
	if s.feed == nil {
		return
	}
	_, fed, fedWoken, _ := s.feed.from(s.fed)
	var tailWoken, tailDone <-chan struct{}
	if s.tail != nil {
		_, read, woken, _ := s.tail.from(s.next)
		if read > s.next {
			return
		}
		tailWoken, tailDone = woken, s.tail.ctx.Done()
	}
	if fed > s.fed {
		return
	}
	select {
	case <-fedWoken:
	case <-s.feed.ctx.Done():
	case <-tailWoken:
	case <-tailDone:
	case <-ctx.Done():
	}
}

// Ended reports whether the run had an outcome when Next last read it.
func (s *TranscriptStream) Ended() bool {
	return s.r != nil && s.r.outcome != nil
}

// answerGrace is how long a seal request goes unanswered before Unanswered reports it, past the answerer's retry.
const answerGrace = 3 * retryInterval

// Unanswered reports whether, when Next last read the run, it had a seal request older than answerGrace that has not
// expired and that no answerer of this host has sealed.
func (s *TranscriptStream) Unanswered() bool {
	if s.r == nil || s.r.outcome != nil {
		return false
	}
	for i, request := range s.r.requests {
		if time.Since(s.r.requested[i]) < answerGrace || !time.Now().Before(time.UnixMilli(request.ExpiryMS)) {
			continue
		}
		if _, err := os.Stat(s.h.markerPath(s.run, request.Attempt)); errors.Is(err, os.ErrNotExist) {
			return true
		}
	}
	return false
}

// Await follows the tenant feed until the run has an outcome, answering seal requests while it waits.
// Each arrival re-reads wherry.admin and the receivers' keys and derives the run again, so nothing from a member ended
// by then shapes the outcome.
func (h *Host) Await(ctx context.Context, runID string, timeout time.Duration) (AwaitResult, error) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	f, err := h.follow(waitCtx)
	if err != nil {
		return AwaitResult{}, err
	}
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
	records, next, err := h.readFeed(waitCtx, f, local.feedOffset())
	if err != nil {
		return AwaitResult{}, err
	}
	r := newRunView(h, v, runID, local)
	r.read(waitCtx, records)
	if r.pool == "" {
		return AwaitResult{}, fmt.Errorf("wait %s: %w", runID, ErrRunNotFound)
	}
	rederive := func() error {
		v, err := h.load(waitCtx)
		if err != nil {
			return fmt.Errorf("wait %s: %w", runID, err)
		}
		r.v, r.reread = v, map[string]bool{}
		r.derive()
		return nil
	}
	// The view loaded before the feed read misses a member ended during it, whose records this drops.
	if err := rederive(); err != nil {
		return AwaitResult{}, err
	}
	for {
		if r.outcome != nil {
			outcome := *r.outcome
			outcome.Skipped, outcome.Model = r.skipped, r.model()
			if r.result != nil {
				if err := r.readTranscript(waitCtx); err != nil {
					return AwaitResult{}, fmt.Errorf("wait %s: %w", runID, err)
				}
				outcome.Skipped = r.skipped
				outcome.Outputs, outcome.Stored, outcome.NotReturned, outcome.Rejected, outcome.Reply = r.verifiedOutputs(r.result)
			}
			markers := h.markers(local)
			outcome.Redacted = map[string]int{}
			if outcome.Reply != nil && outcome.Reply.Rejected == "" {
				// A marker split across chunks is whole once they are joined.
				outcome.Reply.Data = unredact(outcome.Reply.Data, markers, outcome.Redacted)
				sum := sha256.Sum256(outcome.Reply.Data)
				outcome.Reply.SHA256, outcome.Reply.Size = hex.EncodeToString(sum[:]), int64(len(outcome.Reply.Data))
			}
			outcome.Status = unredactStatus(outcome.Status, markers, outcome.Redacted)
			if outcome.Task != nil {
				task := *outcome.Task
				task.Status, task.Artifacts = outcome.Status, slices.Clone(task.Artifacts)
				for i := range task.Artifacts {
					task.Artifacts[i].Parts = unredactParts(task.Artifacts[i].Parts, markers, outcome.Redacted)
				}
				outcome.Task = &task
			}
			return outcome, nil
		}
		if unclaimed := r.unclaimed(time.Now()); unclaimed != "" {
			return AwaitResult{Status: wire.TaskStatus{State: wire.TaskStateSubmitted}, Model: r.model(), Unclaimed: unclaimed, Skipped: r.skipped}, nil
		}
		pollCtx, stopPoll := waitCtx, context.CancelFunc(func() {})
		if r.local != nil && r.local.QueueDeadlineMS > 0 && len(r.statuses) == 0 {
			pollCtx, stopPoll = context.WithDeadline(waitCtx, time.UnixMilli(r.local.QueueDeadlineMS+1))
		}
		records, read, woken, _ := f.from(next)
		if read <= next {
			select {
			case <-woken:
			case <-pollCtx.Done():
			case <-f.ctx.Done():
				// The follower's owner stopped it; another follower, perhaps from a later offset, takes over.
				if f, err = h.follow(waitCtx); err == nil {
					records, read, err = h.readFeed(waitCtx, f, next)
				}
			}
		}
		stopPoll()
		if err := waitCtx.Err(); err != nil {
			return AwaitResult{}, fmt.Errorf("wait %s: %w", runID, err)
		}
		if err != nil {
			return AwaitResult{}, err
		}
		next = max(next, read)
		kept, unread := len(r.entries), r.unread
		for _, record := range records {
			if typ, kid := header(record.Value); typ != wire.EchoType || !r.addEcho(waitCtx, record, kid) {
				r.add(waitCtx, record, typ, kid)
			}
		}
		if len(r.entries) == kept && r.unread == unread {
			continue
		}
		if err := rederive(); err != nil {
			return AwaitResult{}, err
		}
	}
}

// verifiedOutputs joins, for each output the result lists and for its reply, the chunks its own attempt wrote, and
// verifies them against the listed size and SHA-256. An output or reply whose chunks have a gap, an overlap or a
// duplicate index, or whose bytes do not match, is rejected. It also returns the S3 outputs the result lists, and the
// outputs it lists as not returned.
func (r *runView) verifiedOutputs(result *wire.Result) ([]Output, []wire.ReturnedOutput, []wire.UnreturnedOutput, []wire.UnreturnedOutput, *Reply) {
	var manifest struct {
		Outputs     []wire.ReturnedOutput   `json:"outputs"`
		S3Outputs   []wire.ReturnedOutput   `json:"s3_outputs"`
		NotReturned []wire.UnreturnedOutput `json:"not_returned"`
		Reply       *wire.ReturnedReply     `json:"reply"`
	}
	// The metadata also holds the placement members; a manifest that does not decode lists nothing.
	if json.Unmarshal(result.Task.Metadata, &manifest) != nil {
		return nil, nil, nil, nil, nil
	}
	joined := slices.DeleteFunc(slices.Clone(manifest.Outputs), func(output wire.ReturnedOutput) bool { return output.Name == wire.ReplyArtifact })
	var reply *Reply
	if manifest.Reply != nil {
		reply = &Reply{Cut: manifest.Reply.Cut}
		joined = append(joined, wire.ReturnedOutput{Name: wire.ReplyArtifact, SHA256: manifest.Reply.SHA256, Size: manifest.Reply.Size})
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
	for _, listed := range joined {
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
		switch {
		case listed.Name == wire.ReplyArtifact && reason != "":
			reply.Rejected = reason
		case listed.Name == wire.ReplyArtifact:
			reply.Data = data
		case reason != "":
			rejected = append(rejected, wire.UnreturnedOutput{Name: listed.Name, Size: max(listed.Size, 0), Reason: "rejected by this host: " + reason})
		default:
			verified = append(verified, Output{Name: listed.Name, SHA256: listed.SHA256, Size: listed.Size, Data: data})
		}
	}
	return verified, manifest.S3Outputs, manifest.NotReturned, rejected, reply
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
