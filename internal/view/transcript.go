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
	// Indexed is the lines indexed for the run; Read what this request's scan read and Seek what its seek read;
	// Truncated names each cap that cut it.
	Indexed   int      `json:"indexed"`
	Read      Read     `json:"read"`
	Seek      Read     `json:"seek"`
	Truncated []string `json:"truncated,omitzero"`
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
// beyond it.
func (t *transcripts) indexFor(id, tenant, pool, hash string, caps Caps) *transcriptIndex {
	t.mu.Lock()
	idx := t.indexes[id]
	dropped := 0
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
	if dropped > 0 {
		t.v.mu.Lock()
		t.v.m.count(capTranscriptRuns, int64(dropped))
		t.v.mu.Unlock()
	}
	return idx
}

// total is the lines indexed over every run kept.
func (t *transcripts) total() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	var n int64
	for _, idx := range t.indexes {
		n += idx.count.Load()
	}
	return n
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
	// The keys are cloned: the tick replaces entries under the lock while a request verifies outside it.
	caps, keys, signers := v.m.caps, maps.Clone(v.m.keys), v.m.signers
	v.mu.RUnlock()
	if !ok || tenant == "" {
		return Transcript{}, errNotFound
	}
	select {
	case t.running <- struct{}{}:
		defer func() { <-t.running }()
	default:
		v.mu.Lock()
		v.m.count(capTranscriptScans, 1)
		v.mu.Unlock()
		return Transcript{}, errBusy
	}
	idx := t.indexFor(id, tenant, pool, hash, caps)
	idx.mu.Lock()
	defer idx.mu.Unlock()
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
	budget := &scanBudget{bytes: caps.TranscriptBytes, records: caps.TranscriptRecords, deadline: v.opts.Now().Add(caps.TranscriptScan)}
	if err := t.scan(ctx, idx, budget, &response, keys, caps, from.Add(-scanSkew)); err != nil {
		return Transcript{}, err
	}
	response.Complete = idx.ended[idx.rule.Attempt] || idx.terminalEnd >= 0 && idx.next >= idx.terminalEnd
	response.Read = budget.read
	if err := t.seek(ctx, idx, budget, &response, page, keys, signers, caps); err != nil {
		return Transcript{}, err
	}
	response.Seek = Read{Bytes: budget.read.Bytes - response.Read.Bytes, Records: budget.read.Records - response.Read.Records}
	return t.finish(idx, response), nil
}

// scanBudget is one request's budget: the bytes and records it may read and the time it may take, over its scan and
// seek together, and what it read.
type scanBudget struct {
	bytes    int64
	records  int
	deadline time.Time
	read     Read
}

// spent is the cap the budget hit, or empty while it has room; now is the clock.
func (b *scanBudget) spent(now time.Time) string {
	switch {
	case b.records-int(b.read.Records) <= 0:
		return capTranscriptRecords
	case b.bytes-b.read.Bytes <= 0:
		return capTranscriptBytes
	case now.After(b.deadline):
		return capTranscriptScan
	}
	return ""
}

// bounds is the budget's room for one read within the batch caps.
func (b *scanBudget) bounds(caps Caps) operator.Bounds {
	return operator.Bounds{Records: min(b.records-int(b.read.Records), caps.BatchRecords), Bytes: min(b.bytes-b.read.Bytes, caps.BatchBytes)}
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
// verifies; it stops at a cap, named in the response, or at the end it listed.
func (t *transcripts) scan(ctx context.Context, idx *transcriptIndex, budget *scanBudget, response *Transcript, keys signed.Keys, caps Caps, from time.Time) error {
	v := t.v
	for {
		if cap := budget.spent(v.opts.Now()); cap != "" {
			t.cut(response, cap)
			return nil
		}
		if idx.count.Load() >= int64(caps.TranscriptLines) || t.total() >= int64(caps.TranscriptTotalLines) {
			t.cut(response, capTranscriptLines)
			return nil
		}
		ranges, err := v.reader.ReadRanges(ctx, map[string]int64{idx.topic: idx.next}, budget.bounds(caps))
		if err != nil {
			return err
		}
		rng := ranges[idx.topic]
		if rng.Err != nil {
			return rng.Err
		}
		if !idx.started {
			idx.started = true
			// The span's start is at or below the log start, and the earliest retained record is newer than it: retention
			// removed the span's start.
			if idx.listed >= 0 && idx.listed <= rng.Start && (len(rng.Records) == 0 && rng.End > rng.Start || len(rng.Records) > 0 && rng.Records[0].Timestamp.After(from)) {
				idx.predatesLog = true
			}
		}
		for _, record := range rng.Records {
			budget.take(record)
			if full := t.index(idx, record, keys, caps); full {
				idx.next = record.Offset
				t.cut(response, capTranscriptLines)
				return nil
			}
		}
		if idx.next == math.MaxInt64 && rng.Next == rng.End || rng.Next <= idx.next && len(rng.Records) == 0 {
			idx.next = rng.End
			return nil
		}
		idx.next = rng.Next
		if rng.Next >= rng.End {
			return nil
		}
	}
}

// index folds one record of the topic: the run's own, by key; a chunk by its JWS type, an output chunk dropped before
// verifying; verified under the key history; naming the run's tenant, pool and task hash with its signer as receiver.
// It reports true when the line caps leave no room for the record's line, which then stops the scan before it.
func (t *transcripts) index(idx *transcriptIndex, record *kgo.Record, keys signed.Keys, caps Caps) bool {
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
		v.m.count("transcript_skipped", 1)
		v.mu.Unlock()
		return false
	}
	var chunk wire.Chunk
	err := wire.ParseHistory(record.Timestamp)(record.Value, keys, &chunk)
	if err == nil {
		if b := chunk.Wherry; b.Run != idx.run || b.Tenant != idx.tenant || b.Pool != idx.pool || b.TaskHash != idx.hash || b.Receiver != owner {
			err = errOtherRun
		}
	}
	if err != nil {
		v.mu.Lock()
		v.m.count("transcript_refused", 1)
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
	if idx.count.Load() >= int64(caps.TranscriptLines) || t.total() >= int64(caps.TranscriptTotalLines) {
		return true
	}
	idx.lines = slices.Insert(idx.lines, i, lineRef{seq: key.Seq, attempt: key.Attempt, offset: record.Offset})
	idx.count.Store(int64(len(idx.lines)))
	return false
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

var errOtherRun = errors.New("chunk names another run, tenant, pool, task or receiver than its signer's")

// seek reads the records of the page's window, from the lowest offset among them, under what is left of the budget and
// the response bound, and decodes each into a line; the lines returned are a seq-prefix of the window, so a cut leaves
// Next at the first line not returned. A cursor whose attempt or first differs from the rule's is answered with a
// reset and no lines.
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
	// The lines returned are the window's seq-prefix of those decoded; a cut or a record missing at its offset ends it.
	var unread *int64
	for _, l := range window {
		line, ok := lines[l.seq]
		if !ok {
			seq := l.seq
			unread = &seq
			break
		}
		response.Lines = append(response.Lines, line)
	}
	if len(response.Lines) > 0 && below {
		seq := response.Lines[0].Seq
		response.Prev = &seq
	}
	switch {
	case unread != nil:
		response.Next = unread
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
