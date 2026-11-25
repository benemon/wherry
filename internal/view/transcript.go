package view

import (
	"cmp"
	"context"
	json "encoding/json/v2"
	"errors"
	"maps"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// The transcript's caps, as reported.
const (
	capTranscriptBytes   = "transcript_bytes"
	capTranscriptRecords = "transcript_records"
	capTranscriptScan    = "transcript_scan"
	capTranscriptScans   = "transcript_scans"
	capTranscriptLines   = "transcript_lines"
	capTranscriptRuns    = "transcript_runs"
)

// The transcript's counts besides its caps: records dropped by type before verifying, chunks refused, indexes
// retracted by a rotation, indexes evicted to make room under the total-lines cap, and indexes rebuilt because the
// model's pool or task hash for the run changed.
const (
	countTranscriptSkipped   = "transcript_skipped"
	countTranscriptRefused   = "transcript_refused"
	countTranscriptRetracted = "transcript_retracted"
	countTranscriptEvicted   = "transcript_evicted"
	countTranscriptRebound   = "transcript_rebound"
)

// seekShare is the seek's share of a request's budget: one part in seekShare of its bytes, records and time is kept
// out of the scan's reach, so a scan that spends the rest never starves the page's seek.
const seekShare = 4

// MaxLineText bounds a line's text in a response; a longer line is cut at a rune boundary and marked with its size.
const MaxLineText = 16 << 10

// MaxPage bounds the lines one request asks for.
const MaxPage = 1000

// lineJSONMargin is what a response holds besides its lines, reserved under the response bound.
const lineJSONMargin = 4096

// Page is one transcript request: exactly one of Tail, Before and From, the last two with Limit and the cursor's
// Attempt and First, so a change of either is answered with a reset.
type Page struct {
	Tail, Limit  int
	Before, From *int64
	Attempt      int64
	First        *int64
}

// Transcript is a page of a run's transcript: the lines selected, in seq order, under the attempt rule.
type Transcript struct {
	Run string `json:"run"`
	// Attempt is the attempt shown and First the lowest seq of its lines, null while it has none; Reset is set when
	// either differs from the cursor's, so the page reloads from the tail.
	Attempt int64  `json:"attempt"`
	First   *int64 `json:"first"`
	Reset   bool   `json:"reset,omitzero"`
	Lines   []Line `json:"lines"`
	// Prev is the seq to ask before for older lines; Next the seq to ask from for later ones, present while the run
	// may have more or the page was cut.
	Prev *int64 `json:"prev,omitzero"`
	Next *int64 `json:"next,omitzero"`
	// Complete is set once the attempt's last chunk was read, or the run is terminal and the scan reached the end
	// listed after its terminal. PredatesLog is set when retention removed the span's start. Missing when the tenant
	// has no transcript topic.
	Complete    bool `json:"complete"`
	PredatesLog bool `json:"predates_log,omitzero"`
	Missing     bool `json:"missing,omitzero"`
	// Retry is set when a request budget cut a tail request's scan before the span was indexed: the response has no
	// lines, since the lines indexed so far are not the tail, and the page asks tail again.
	Retry bool `json:"retry,omitzero"`
	// Indexed is the lines indexed for the run; Read what this request's scan read and Seek what its seek read;
	// Truncated names each cap that cut it; Evicted is the other runs' indexes dropped, least recently used first, to
	// make room for this run's lines under the total-lines cap.
	Indexed   int      `json:"indexed"`
	Read      Read     `json:"read"`
	Seek      Read     `json:"seek"`
	Truncated []string `json:"truncated,omitzero"`
	Evicted   int      `json:"evicted,omitzero"`
}

// Line is one line of the transcript: Text as the receiver redacted it, Raw with Size for one that is not valid
// UTF-8, Oversize with Size for one the receiver did not store; a line over MaxLineText is cut with Truncated set and
// its Size. Receiver is the signer's principal.
type Line struct {
	At        time.Time `json:"at"`
	Attempt   int64     `json:"attempt"`
	Receiver  string    `json:"receiver"`
	Seq       int64     `json:"seq"`
	Text      *string   `json:"text,omitzero"`
	Raw       bool      `json:"raw,omitzero"`
	Oversize  bool      `json:"oversize,omitzero"`
	Truncated bool      `json:"truncated,omitzero"`
	Size      int64     `json:"size,omitzero"`
}

// transcripts keeps each requested run's index, built and resumed on request: no tick tails the transcript topics.
type transcripts struct {
	v       *View
	mu      sync.Mutex
	indexes map[string]*transcriptIndex
	running chan struct{}
}

// transcriptIndex is one run's index: seq to offset for each of its chunks that verified and named the run's tenant,
// pool and task hash, under the attempt rule. mu serializes the requests for the run; owners is guarded by the
// transcripts' mutex, since a rotation reads it.
type transcriptIndex struct {
	run, tenant, pool, hash, topic string
	mu                             sync.Mutex
	started                        bool
	listed, next                   int64
	predatesLog                    bool
	rule                           wire.AttemptRule
	// lines is sorted by seq, one per seq, the highest attempt's of those sharing it; count is their number, read
	// without the lock for the total over every index.
	lines []lineRef
	count atomic.Int64
	// ended holds the attempts whose last chunk was read; terminalEnd is the topic's end listed after the run's
	// terminal was seen, -1 before.
	ended       map[int64]bool
	terminalEnd int64
	owners      map[string]bool
	used        time.Time
}

type lineRef struct {
	seq, attempt, offset int64
}

func newTranscripts(v *View) *transcripts {
	return &transcripts{v: v, indexes: map[string]*transcriptIndex{}, running: make(chan struct{}, max(v.opts.Caps.TranscriptScans, 1))}
}

// indexFor is the index of run id, made on first sight within the cap on runs kept, the least recently used dropped
// beyond it. An index bound to another pool or task hash than the model now records for the run, after a status
// supplied the hash or a retraction changed it, is dropped and made again, so it never refuses the run's chunks for
// a binding the model no longer holds.
func (t *transcripts) indexFor(id, tenant, pool, hash string, caps Caps) *transcriptIndex {
	t.mu.Lock()
	idx := t.indexes[id]
	dropped, rebound := 0, 0
	if idx != nil && (idx.pool != pool || idx.hash != hash) {
		delete(t.indexes, id)
		idx, rebound = nil, 1
	}
	if idx == nil {
		idx = &transcriptIndex{run: id, tenant: tenant, pool: pool, hash: hash, topic: operator.TenantTranscriptTopic(tenant), rule: wire.NewAttemptRule(0), ended: map[int64]bool{}, terminalEnd: -1, owners: map[string]bool{}}
		t.indexes[id] = idx
		for len(t.indexes) > max(caps.TranscriptRuns, 1) {
			oldest := ""
			for other, state := range t.indexes {
				if other != id && (oldest == "" || state.used.Before(t.indexes[oldest].used)) {
					oldest = other
				}
			}
			delete(t.indexes, oldest)
			dropped++
		}
	}
	idx.used = t.v.opts.Now()
	t.mu.Unlock()
	if dropped > 0 || rebound > 0 {
		t.v.mu.Lock()
		if dropped > 0 {
			t.v.m.count(capTranscriptRuns, int64(dropped))
		}
		if rebound > 0 {
			t.v.m.count(countTranscriptRebound, int64(rebound))
		}
		t.v.mu.Unlock()
	}
	return idx
}

// sum is the lines indexed over every run kept. The caller holds t.mu.
func (t *transcripts) sum() int64 {
	var n int64
	for _, idx := range t.indexes {
		n += idx.count.Load()
	}
	return n
}

// room reports whether the line caps leave room for one more of idx's lines. While the total over every run is
// reached, the least recently used indexes other than idx are evicted to make it, each counted and reported in the
// response: a few huge runs filling the total must not leave every other run empty.
func (t *transcripts) room(idx *transcriptIndex, caps Caps, response *Transcript) bool {
	if idx.count.Load() >= int64(caps.TranscriptLines) {
		return false
	}
	total := int64(caps.TranscriptTotalLines)
	t.mu.Lock()
	evicted := 0
	for t.sum() >= total {
		oldest := ""
		for id, other := range t.indexes {
			if other != idx && (oldest == "" || other.used.Before(t.indexes[oldest].used)) {
				oldest = id
			}
		}
		if oldest == "" {
			break
		}
		delete(t.indexes, oldest)
		evicted++
	}
	room := t.sum() < total
	t.mu.Unlock()
	if evicted > 0 {
		response.Evicted += evicted
		t.v.mu.Lock()
		t.v.m.count(countTranscriptEvicted, int64(evicted))
		t.v.mu.Unlock()
	}
	return room
}

// drop forgets idx as run id's index when it still is, so the next request rebuilds it; it reports whether it did.
// The caller holds v.mu.
func (t *transcripts) drop(id string, idx *transcriptIndex) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.indexes[id] != idx {
		return false
	}
	delete(t.indexes, id)
	return true
}

// retire drops the indexes holding chunks owner signed, so the next request for each run rebuilds it without the
// chunks a rotation read since retracted; it returns how many it dropped. The caller holds v.mu.
func (t *transcripts) retire(owner string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	dropped := 0
	for id, idx := range t.indexes {
		if idx.owners[owner] {
			delete(t.indexes, id)
			dropped++
		}
	}
	return dropped
}

// Transcript is page of run id's transcript. The first request scans the run's span of its tenant's transcript topic
// from one minute before its first record and indexes the run's chunks; a later request resumes the scan where the
// last stopped, then seeks the records of the lines selected. Scans and seeks run under the caps, each named when it
// cuts the response.
func (t *transcripts) Transcript(ctx context.Context, id string, page Page) (Transcript, error) {
	v := t.v
	v.mu.RLock()
	r, ok := v.m.runs[id]
	var tenant, pool, hash string
	var from time.Time
	var highest int64
	var terminal bool
	if ok {
		tenant, pool, hash, from, highest = r.Tenant, r.Pool, r.Hash, r.First, r.Highest
		if admitted := r.Admitted(); !admitted.IsZero() && admitted.Before(from) {
			from = admitted
		}
		_, terminal = r.terminal()
	}
	// The keys are cloned: the tick replaces entries under the lock while a request verifies outside it. The pools are
	// each receiver's recorded pool, snapshotted with the keys; the epoch tells a retirement read during the request.
	caps, keys, signers, pools, epoch := v.m.caps, maps.Clone(v.m.keys), v.m.signers, v.m.receiverPools(), v.m.retirements
	v.mu.RUnlock()
	if !ok || tenant == "" {
		return Transcript{}, errNotFound
	}
	idx := t.indexFor(id, tenant, pool, hash, caps)
	// The run's lock is taken before a scan slot, so a request waiting its turn for the run holds no slot: one page's
	// older-lines request and its follow cannot take both.
	idx.mu.Lock()
	defer idx.mu.Unlock()
	select {
	case t.running <- struct{}{}:
		defer func() { <-t.running }()
	default:
		v.mu.Lock()
		v.m.count(capTranscriptScans, 1)
		v.mu.Unlock()
		return Transcript{}, errBusy
	}
	// The statuses' highest attempt counts as the host's does: an attempt with no line yet shows every earlier line.
	idx.rule.Add(wire.ChunkKey{Attempt: highest, Line: false})
	admin, err := v.reader.Admin()
	if err != nil {
		return Transcript{}, err
	}
	topics, err := listTopics(ctx, admin)
	if err != nil {
		return Transcript{}, err
	}
	response := Transcript{Run: id, Lines: []Line{}}
	if !topics[idx.topic] {
		response.Missing = true
		return t.finish(idx, response), nil
	}
	if !idx.started {
		listed, err := operator.ListOffsets(ctx, func(ctx context.Context, topics ...string) (kadm.ListedOffsets, error) {
			return admin.ListOffsetsAfterMilli(ctx, from.Add(-scanSkew).UnixMilli(), topics...)
		}, idx.topic)
		if err != nil {
			return Transcript{}, err
		}
		idx.next, idx.listed = math.MaxInt64, -1
		if start, ok := listed.Lookup(idx.topic, 0); ok && start.Offset >= 0 {
			idx.next, idx.listed = start.Offset, start.Offset
		}
	}
	if terminal && idx.terminalEnd < 0 {
		ends, err := operator.ListOffsets(ctx, admin.ListEndOffsets, idx.topic)
		if err != nil {
			return Transcript{}, err
		}
		if end, ok := ends.Lookup(idx.topic, 0); ok {
			idx.terminalEnd = end.Offset
		}
	}
	budget := newScanBudget(caps, v.opts.Now())
	cut, err := t.scan(ctx, idx, budget, &response, keys, pools, caps, from.Add(-scanSkew))
	if err != nil {
		return Transcript{}, err
	}
	response.Complete = idx.ended[idx.rule.Attempt] || idx.terminalEnd >= 0 && idx.next >= idx.terminalEnd
	response.Read = budget.read
	if page.Tail > 0 && budget.retries(cut) {
		// The request's budget cut the scan before the span was indexed: the lines indexed so far are not the tail, so
		// none is returned, and the page asks tail again until the scan ends. A line cap is final, so a tail cut by one
		// is the tail of what the index will hold.
		response.Retry, response.Complete = true, false
	} else {
		budget.release()
		if err := t.seek(ctx, idx, budget, &response, page, keys, signers, caps); err != nil {
			return Transcript{}, err
		}
	}
	response.Seek = Read{Bytes: budget.read.Bytes - response.Read.Bytes, Records: budget.read.Records - response.Read.Records}
	// A retirement read during the request may have left chunks a rebuild would refuse in the index, signed with the
	// retired key after its retirement and indexed under the keys snapshotted before the rotation was read: the index
	// is dropped, and the next request rebuilds it.
	v.mu.Lock()
	if v.m.retirements != epoch && t.drop(id, idx) {
		v.m.count(countTranscriptRetracted, 1)
	}
	v.mu.Unlock()
	return t.finish(idx, response), nil
}

// scanBudget is one request's budget: the bytes and records it may read and the time it may take, over its scan and
// seek together, and what it read. reserved is the seek's share, kept out of the scan's reach until release.
type scanBudget struct {
	bytes    int64
	records  int
	deadline time.Time
	reserved struct {
		bytes   int64
		records int
		time    time.Duration
	}
	read Read
}

// newScanBudget is the budget of one request starting at now under caps, the seek's share reserved.
func newScanBudget(caps Caps, now time.Time) *scanBudget {
	b := &scanBudget{bytes: caps.TranscriptBytes, records: caps.TranscriptRecords, deadline: now.Add(caps.TranscriptScan)}
	b.reserved.bytes, b.reserved.records, b.reserved.time = caps.TranscriptBytes/seekShare, caps.TranscriptRecords/seekShare, caps.TranscriptScan/seekShare
	return b
}

// release gives the seek's share to what follows.
func (b *scanBudget) release() {
	b.reserved.bytes, b.reserved.records, b.reserved.time = 0, 0, 0
}

// spent is the cap the budget hit, or empty while it has room; now is the clock.
func (b *scanBudget) spent(now time.Time) string {
	switch {
	case b.records-b.reserved.records-int(b.read.Records) <= 0:
		return capTranscriptRecords
	case b.bytes-b.reserved.bytes-b.read.Bytes <= 0:
		return capTranscriptBytes
	case now.After(b.deadline.Add(-b.reserved.time)):
		return capTranscriptScan
	}
	return ""
}

// retries reports whether cap, the one that cut a scan, is one of the request's own: bytes, records or time, which
// another request has afresh, as a line cap does not.
func (b *scanBudget) retries(cap string) bool {
	return cap == capTranscriptBytes || cap == capTranscriptRecords || cap == capTranscriptScan
}

// bounds is the budget's room for one read within the batch caps.
func (b *scanBudget) bounds(caps Caps) operator.Bounds {
	return operator.Bounds{Records: min(b.records-b.reserved.records-int(b.read.Records), caps.BatchRecords), Bytes: min(b.bytes-b.reserved.bytes-b.read.Bytes, caps.BatchBytes)}
}

func (b *scanBudget) take(record *kgo.Record) {
	b.read.Records++
	b.read.Bytes += int64(len(record.Value))
}

// cut names cap in the response once and counts the hit.
func (t *transcripts) cut(response *Transcript, cap string) {
	if slices.Contains(response.Truncated, cap) {
		return
	}
	response.Truncated = append(response.Truncated, cap)
	t.v.mu.Lock()
	t.v.m.count(cap, 1)
	t.v.mu.Unlock()
}

// scan resumes the run's scan at idx.next under the budget and the line caps, indexing each chunk of the run that
// verifies; it stops at a cap, named in the response and returned, or at the end it listed.
func (t *transcripts) scan(ctx context.Context, idx *transcriptIndex, budget *scanBudget, response *Transcript, keys signed.Keys, pools map[string]string, caps Caps, from time.Time) (string, error) {
	v := t.v
	for {
		if cap := budget.spent(v.opts.Now()); cap != "" {
			t.cut(response, cap)
			return cap, nil
		}
		if !t.room(idx, caps, response) {
			t.cut(response, capTranscriptLines)
			return capTranscriptLines, nil
		}
		ranges, err := v.reader.ReadRanges(ctx, map[string]int64{idx.topic: idx.next}, budget.bounds(caps))
		if err != nil {
			return "", err
		}
		rng := ranges[idx.topic]
		if rng.Err != nil {
			return "", rng.Err
		}
		if !idx.started {
			idx.started = true
			// Records were deleted, the span's start is at or below the log start, and the earliest retained record is
			// newer than it: retention removed the span's start. A topic whose first record is after the start is intact.
			if rng.Start > 0 && idx.listed >= 0 && idx.listed <= rng.Start && (len(rng.Records) == 0 && rng.End > rng.Start || len(rng.Records) > 0 && rng.Records[0].Timestamp.After(from)) {
				idx.predatesLog = true
			}
		}
		for _, record := range rng.Records {
			budget.take(record)
			if full := t.index(idx, record, keys, pools, caps, response); full {
				idx.next = record.Offset
				t.cut(response, capTranscriptLines)
				return capTranscriptLines, nil
			}
		}
		if idx.next == math.MaxInt64 && rng.Next == rng.End || rng.Next <= idx.next && len(rng.Records) == 0 {
			idx.next = rng.End
			return "", nil
		}
		idx.next = rng.Next
		if rng.Next >= rng.End {
			return "", nil
		}
	}
}

// index folds one record of the topic: the run's own, by key; a chunk by its JWS type, an output chunk dropped before
// verifying; verified under the key history, which binds the receiver it names to its signer; naming the run's tenant,
// pool and task hash; and signed by a receiver recorded in the run's pool, since the tenant's topic is open to every
// receiver entitled to the tenant and the ACL does not bind the pool, as the model's inItsPool says of the feed. It
// reports true when the line caps leave no room for the record's line, which then stops the scan before it.
func (t *transcripts) index(idx *transcriptIndex, record *kgo.Record, keys signed.Keys, pools map[string]string, caps Caps, response *Transcript) bool {
	if string(record.Key) != idx.run {
		return false
	}
	v := t.v
	if len(record.Value) > caps.RecordBytes {
		v.mu.Lock()
		v.m.count(capRecordBytes, 1)
		v.mu.Unlock()
		return false
	}
	typ, owner, _, ok := header(record.Value)
	if !ok || typ != wire.ChunkType {
		v.mu.Lock()
		v.m.count(countTranscriptSkipped, 1)
		v.mu.Unlock()
		return false
	}
	// The parse refuses a chunk naming another receiver than its signer, so the receiver named is the signer.
	var chunk wire.Chunk
	err := wire.ParseHistory(record.Timestamp)(record.Value, keys, &chunk)
	if err == nil {
		if b := chunk.Wherry; b.Run != idx.run || b.Tenant != idx.tenant || b.Pool != idx.pool || b.TaskHash != idx.hash {
			err = errOtherRun
		} else if recorded, member := pools[owner]; !member || recorded != idx.pool {
			err = errOtherPool
		}
	}
	if err != nil {
		v.mu.Lock()
		v.m.count(countTranscriptRefused, 1)
		v.mu.Unlock()
		return false
	}
	key := chunk.Key()
	if chunk.ArtifactUpdate.LastChunk {
		idx.ended[key.Attempt] = true
	}
	idx.rule.Add(key)
	t.mu.Lock()
	idx.owners[owner] = true
	t.mu.Unlock()
	if !key.Line {
		return false
	}
	i, found := slices.BinarySearchFunc(idx.lines, key.Seq, func(l lineRef, seq int64) int { return cmp.Compare(l.seq, seq) })
	if found {
		if idx.lines[i].attempt < key.Attempt {
			idx.lines[i] = lineRef{seq: key.Seq, attempt: key.Attempt, offset: record.Offset}
		}
		return false
	}
	if !t.room(idx, caps, response) {
		return true
	}
	idx.lines = slices.Insert(idx.lines, i, lineRef{seq: key.Seq, attempt: key.Attempt, offset: record.Offset})
	idx.count.Store(int64(len(idx.lines)))
	return false
}

// forget drops the line at seq from the index.
func (idx *transcriptIndex) forget(seq int64) {
	if i, found := slices.BinarySearchFunc(idx.lines, seq, func(l lineRef, seq int64) int { return cmp.Compare(l.seq, seq) }); found {
		idx.lines = slices.Delete(idx.lines, i, i+1)
		idx.count.Store(int64(len(idx.lines)))
	}
}

// window is the page's window of lines under the rule: the last Tail, the Limit before Before, or the Limit from From,
// in seq order, with whether shown lines exist below the first and above the last.
func (idx *transcriptIndex) window(page Page) (window []lineRef, below, above bool) {
	rule := idx.rule
	shown := func(l lineRef) bool { return rule.Shown(wire.ChunkKey{Attempt: l.attempt, Seq: l.seq, Line: true}) }
	at := func(seq int64) int {
		i, _ := slices.BinarySearchFunc(idx.lines, seq, func(l lineRef, seq int64) int { return cmp.Compare(l.seq, seq) })
		return i
	}
	// backward collects up to n shown lines below index end, newest first, and reports whether one more exists.
	backward := func(end, n int) ([]lineRef, bool) {
		var lines []lineRef
		for i := end - 1; i >= 0; i-- {
			if !shown(idx.lines[i]) {
				continue
			}
			if len(lines) == n {
				return lines, true
			}
			lines = append(lines, idx.lines[i])
		}
		return lines, false
	}
	forward := func(start, n int) ([]lineRef, bool) {
		var lines []lineRef
		for i := start; i < len(idx.lines); i++ {
			if !shown(idx.lines[i]) {
				continue
			}
			if len(lines) == n {
				return lines, true
			}
			lines = append(lines, idx.lines[i])
		}
		return lines, false
	}
	switch {
	case page.Tail > 0:
		window, below = backward(len(idx.lines), page.Tail)
		slices.Reverse(window)
	case page.Before != nil:
		window, below = backward(at(*page.Before), page.Limit)
		slices.Reverse(window)
		_, above = forward(at(*page.Before), 0)
	case page.From != nil:
		window, above = forward(at(*page.From), page.Limit)
		_, below = backward(at(*page.From), 0)
	}
	return window, below, above
}

var (
	errOtherRun  = errors.New("chunk names another run, tenant, pool or task than the index's")
	errOtherPool = errors.New("chunk is signed by a receiver not recorded in the run's pool")
)

// seek reads the records of the page's window, the interleaved span of the topic between the lowest and the highest
// offset among them, under what is left of the budget and the response bound, and decodes each into a line; the lines
// returned are a seq-prefix of the window, so a cut leaves Next at the first line not returned. A cursor whose attempt
// or first differs from the rule's is answered with a reset and no lines.
func (t *transcripts) seek(ctx context.Context, idx *transcriptIndex, budget *scanBudget, response *Transcript, page Page, keys signed.Keys, signers map[string]string, caps Caps) error {
	rule := idx.rule
	if (page.Before != nil || page.From != nil) && (page.Attempt != rule.Attempt || page.First == nil != (rule.First == math.MaxInt64) || page.First != nil && *page.First != rule.First) {
		response.Reset = true
		return nil
	}
	window, below, above := idx.window(page)
	if len(window) == 0 {
		response.Prev, response.Next = nil, nil
		if below {
			seq := int64(0)
			if page.Before != nil {
				seq = *page.Before
			}
			response.Prev = &seq
		}
		if page.From != nil && (!response.Complete || above) {
			seq := *page.From
			response.Next = &seq
		}
		return nil
	}
	wanted := map[int64]lineRef{}
	lowest, highestOffset := int64(math.MaxInt64), int64(-1)
	for _, l := range window {
		wanted[l.offset] = l
		lowest, highestOffset = min(lowest, l.offset), max(highestOffset, l.offset)
	}
	lines := map[int64]Line{}
	var size int
	limit := caps.ResponseBytes - lineJSONMargin
	v := t.v
	next, cut := lowest, false
read:
	for next <= highestOffset {
		if cap := budget.spent(v.opts.Now()); cap != "" {
			t.cut(response, cap)
			cut = true
			break
		}
		ranges, err := v.reader.ReadRanges(ctx, map[string]int64{idx.topic: next}, budget.bounds(caps))
		if err != nil {
			return err
		}
		rng := ranges[idx.topic]
		if rng.Err != nil {
			return rng.Err
		}
		for _, record := range rng.Records {
			budget.take(record)
			l, ok := wanted[record.Offset]
			if !ok {
				continue
			}
			line, ok := decodeLine(record, l, keys, signers)
			if !ok {
				continue
			}
			encoded, err := json.Marshal(line)
			if err != nil {
				return err
			}
			if size+len(encoded)+1 > limit {
				t.cut(response, capResponse)
				cut = true
				break read
			}
			size += len(encoded) + 1
			lines[l.seq] = line
		}
		if len(rng.Records) == 0 || rng.Next <= next {
			break
		}
		next = rng.Next
	}
	// The lines returned are the window's seq-prefix of those decoded; a cut ends it at the first line not returned. A
	// line whose record was not found or no longer verifies when the read was not cut is gone: retention removed it, or
	// a rotation read since it was indexed retired its key; it leaves the index, so paging passes it rather than
	// stopping at it.
	var unread *int64
	gone := 0
	for _, l := range window {
		line, ok := lines[l.seq]
		switch {
		case ok:
			response.Lines = append(response.Lines, line)
			continue
		case cut:
			seq := l.seq
			unread = &seq
		default:
			idx.forget(l.seq)
			gone++
			continue
		}
		break
	}
	if gone > 0 {
		v.mu.Lock()
		v.m.count(countTranscriptRefused, int64(gone))
		v.mu.Unlock()
	}
	if below {
		seq := window[0].seq
		if len(response.Lines) > 0 {
			seq = response.Lines[0].Seq
		}
		response.Prev = &seq
	}
	switch {
	case unread != nil:
		response.Next = unread
	case len(response.Lines) > 0 && page.Before != nil && (above || !response.Complete):
		// An older page's later lines are the cursor's own, so next is the seq asked before: the page tells a cut page,
		// whose next is lower, from a whole one.
		seq := *page.Before
		response.Next = &seq
	case len(response.Lines) > 0 && (above || !response.Complete || cut):
		seq := response.Lines[len(response.Lines)-1].Seq + 1
		response.Next = &seq
	}
	return nil
}

// decodeLine is record, a chunk of the run indexed as l, verified again under the key history and projected: its text,
// raw bytes or oversize reference, with its signer's principal.
func decodeLine(record *kgo.Record, l lineRef, keys signed.Keys, signers map[string]string) (Line, bool) {
	var chunk wire.Chunk
	if err := wire.ParseHistory(record.Timestamp)(record.Value, keys, &chunk); err != nil || len(chunk.ArtifactUpdate.Artifact.Parts) != 1 || chunk.Wherry.Seq != l.seq || chunk.Wherry.Attempt != l.attempt {
		return Line{}, false
	}
	_, owner, _, _ := header(record.Value)
	line := Line{At: record.Timestamp, Attempt: l.attempt, Receiver: Unknown, Seq: l.seq}
	if principal, ok := signers[owner]; ok {
		line.Receiver = principal
	}
	part := chunk.ArtifactUpdate.Artifact.Parts[0]
	switch {
	case part.MediaType == wire.ReferenceMediaType:
		var reference wire.Reference
		json.Unmarshal(part.Data, &reference, signed.Tolerant) // validation decoded it
		line.Oversize, line.Size = true, reference.Size
	case part.Raw != nil && !utf8.Valid(part.Raw):
		line.Raw, line.Size = true, int64(len(part.Raw))
	default:
		text := part.Text
		if part.Raw != nil {
			text = string(part.Raw)
		}
		if len(text) > MaxLineText {
			line.Truncated, line.Size = true, int64(len(text))
			text = wire.CutUTF8(text, MaxLineText)
		}
		line.Text = &text
	}
	return line, true
}

// finish fills the response's attempt, first, index size and retention flag from the index.
func (t *transcripts) finish(idx *transcriptIndex, response Transcript) Transcript {
	response.Attempt = idx.rule.Attempt
	if idx.rule.First != math.MaxInt64 {
		first := idx.rule.First
		response.First = &first
	}
	response.Indexed, response.PredatesLog = len(idx.lines), idx.predatesLog
	return response
}
