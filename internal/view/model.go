// Package view is the operator network view: a model of the cluster rebuilt from the records Kafka keeps, kept live
// by tailing them, served read-only over loopback HTTP as JSON, a Server-Sent Events stream and a placeholder page.
// The view never produces to Kafka and never holds agent text: every record is projected to named fields, and every
// name it holds is one the operator's configuration yields or a fixed placeholder, except a result's output names,
// kept when they follow the output name grammar.
package view

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	json "encoding/json/v2"
	"errors"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Caps are the bounds the model applies while reading and keeping; each is reported under its name when hit.
type Caps struct {
	// BatchRecords and BatchBytes bound one ReadRanges call; the model consumes each batch before the next.
	BatchRecords int
	BatchBytes   int64
	// RecordBytes is the largest record value decoded; a larger one is skipped and counted before decoding.
	RecordBytes int
	// Runs bounds the runs kept, the oldest by last activity dropped; Digests the record digests kept for
	// de-duplication; Pending the records held for a key or an echo; Cursors the topics followed.
	Runs, Digests, Pending, Cursors int
	// RunEvents, RunAttempts, RunEchoes, RunCancels and RunOpens bound one run's events, attempts, echoes, cancels and
	// open requests kept, and RunBytes its retained size: the oldest is dropped first and the run marked truncated,
	// and a run whose joined echo and result alone are over a bound is dropped. Events, Attempts, Echoes, Cancels,
	// Opens and Bytes bound the same over every run: the oldest runs are dropped beyond them, the growing run too
	// when it alone is over. Progress bounds the progress records kept over every receiver.
	RunEvents, RunAttempts, RunEchoes, RunCancels, RunOpens int
	RunBytes                                                int64
	Events, Attempts, Echoes, Cancels, Opens, Bytes         int64
	Progress                                                int
	// ResponseBytes bounds a JSON response and EventBytes one SSE event.
	ResponseBytes, EventBytes int
	// TraceBytes, TraceRecords and TraceSpans bound one drill-down scan's bytes read, records decoded and spans kept;
	// TraceForest its forest's nodes; TraceScan its time; TraceScans the scans running at once; TraceRuns the runs
	// whose scan state is kept for refresh.
	TraceBytes                                        int64
	TraceRecords, TraceSpans, TraceForest, TraceScans int
	TraceScan                                         time.Duration
	TraceRuns                                         int
}

// DefaultCaps are the caps the command runs with.
var DefaultCaps = Caps{
	BatchRecords: 2000, BatchBytes: 8 << 20, RecordBytes: operator.FeedRecordLimit,
	Runs: 50000, Digests: 400000, Pending: 10000, Cursors: 10000,
	RunEvents: 1000, RunAttempts: 64, RunEchoes: 16, RunCancels: 16, RunOpens: 64, RunBytes: 512 << 10,
	Events: 2000000, Attempts: 1000000, Echoes: 200000, Cancels: 200000, Opens: 200000, Bytes: 512 << 20, Progress: 200000,
	ResponseBytes: 8 << 20, EventBytes: 64 << 10,
	TraceBytes: 32 << 20, TraceRecords: 20000, TraceSpans: 5000, TraceForest: 5000, TraceScans: 2, TraceScan: 5 * time.Second, TraceRuns: 32,
}

// The names a cap is reported under.
const (
	capRecordBytes = "record_bytes"
	capRuns        = "runs"
	capDigests     = "digests"
	capPending     = "pending"
	capCursors     = "cursors"
	capResponse    = "response_bytes"
	capEvent       = "event_bytes"
	capSessions    = "sessions"
	capRunEvents   = "run_events"
	capRunAttempts = "run_attempts"
	capRunEchoes   = "run_echoes"
	capRunCancels  = "run_cancels"
	capRunOpens    = "run_opens"
	capRunBytes    = "run_bytes"
	capEvents      = "events"
	capAttempts    = "attempts"
	capEchoes      = "echoes"
	capCancels     = "cancels"
	capOpens       = "opens"
	capBytes       = "bytes"
	capProgress    = "progress"
)

// The retained size of each fact kept, estimated for the byte bounds: the struct, its map entry and its strings.
const (
	runOverhead          = 384
	echoOverhead         = 256
	attemptOverhead      = 160
	eventOverhead        = 192
	decisionOverhead     = 128
	startFailureOverhead = 128
	openOverhead         = 256
	cancelOverhead       = 96
	resultOverhead       = 256
	outputOverhead       = 96
)

// Failure classes, from a fixed table: the wire carries no class except a start failure's, so each is derived from a
// status's kind, state and event, or a start failure's class. The reason text in a status is never read.
const (
	ClassKeyLost      = "key-lost"
	ClassFailed       = "failed"
	ClassCanceled     = "canceled"
	ClassRejected     = "rejected"
	ClassUnclassified = "unclassified"
	classStartFailure = "start-failure:"
)

// FailureClass is the class of a status of kind in state carrying event, empty for one that is no failure.
func FailureClass(kind string, state wire.TaskState, event string) string {
	switch {
	case kind == "terminal" && state == wire.TaskStateFailed && event == wire.EventKeyLost:
		return ClassKeyLost
	case kind == "terminal" && state == wire.TaskStateFailed:
		return ClassFailed
	case kind == "terminal" && state == wire.TaskStateCanceled:
		return ClassCanceled
	case kind == "rejected":
		return ClassRejected
	case kind == "terminal" && state == wire.TaskStateCompleted:
		return ""
	case kind == "terminal":
		return ClassUnclassified
	}
	return ""
}

// StartFailureClass is the class of a start failure of class, one of the wire's fixed set.
func StartFailureClass(class string) string { return classStartFailure + class }

// The stages a run shows beyond a status kind.
const (
	StageNoTerminal    = "no terminal recorded"
	StageUnknown       = "unknown"
	StageInFlight      = "in flight"
	StageNotIdentified = "in flight, not identified"
)

// source is the signer of a folded record, the owner and ID of the key that verified it, and its append time: a
// rotation seen later that retired the key before the append time retracts the fact. The owner labels the fact with its
// principal.
type source struct {
	owner, keyID string
	at           time.Time
}

// retiredBy reports whether the source signed under owner's keyID at or after since, the time a rotation retired it.
func (s source) retiredBy(owner, keyID string, since time.Time) bool {
	return s.owner == owner && s.keyID == keyID && !s.at.Before(since)
}

// signer is the label of a fact from s: the principal of the owner whose key verified it, built from the state, never
// from the record's text. A key owner the state has since lost shows as the placeholder.
func (m *model) signer(s source) string {
	if principal, ok := m.signers[s.owner]; ok {
		return principal
	}
	return Unknown
}

// event is a status projected: no message, no metadata. Its kind, state and event are of the fixed sets.
type event struct {
	Kind       string
	State      wire.TaskState
	EventID    string
	Attempt    int64
	Receiver   string
	At         time.Time
	ReceiverAt time.Time
	LogOffset  *int64
	Event      string
	Class      string
	src        source
}

type attempt struct {
	Number   int64
	Receiver string
	Events   []event
	// TaskTopic is the claimed task topic when the configuration derives it, else the placeholder.
	TaskTopic  string
	TaskOffset *int64
	// ClaimDigest is the digest of the attempt's claimed status, which a claim decision names.
	ClaimDigest  string
	Decision     *decisionFact
	StartFailure *startFailureFact
}

type decisionFact struct {
	Custodian, Previous, Reason string
	Served                      bool
	At                          time.Time
	src                         source
}

type startFailureFact struct {
	Profile, Agent, Class string
	At                    time.Time
	src                   source
}

// echoFact is an echo projected: its names as the configuration knows them, its agent and model as the pool's terms
// name them, else the placeholder.
type echoFact struct {
	Receiver, Host, Pool string
	Agent, Model         string
	At                   time.Time
	src                  source
}

type cancelFact struct {
	Host string
	At   time.Time
	src  source
}

// openFact is an open request and its answer, by the request's digest.
type openFact struct {
	Digest   string
	Attempt  int64
	Receiver string
	At       time.Time
	// Answer is "wrap" or "refusal", with Reason a refusal's fixed reason, and AnsweredAt its append time.
	Answer, Custodian, Reason string
	AnsweredAt                time.Time
	src, answerSrc            source
}

// resultFact is the Reference facts of a result: output names, sizes and hashes, never a part's content.
type resultFact struct {
	Receiver    string
	Attempt     int64
	State       wire.TaskState
	At          time.Time
	Profile     string
	Outputs     []outputFact
	S3Outputs   []outputFact
	NotReturned []unreturnedFact
	ReplySize   int64
	ReplyCut    bool
	Artifacts   int
	src         source
}

// outputFact is a returned output. Name is the tenant's own, kept when it follows the output name grammar, else the
// placeholder: the one name shown that the configuration does not yield.
type outputFact struct {
	Name   string
	Size   int64
	SHA256 string
}

type unreturnedFact struct {
	Name   string
	Size   int64
	Reason string
}

// measure is a run's retained collections counted and its estimated retained size, and the model's totals.
type measure struct {
	events, attempts, echoes, cancels, opens int
	bytes                                    int64
}

func (a measure) plus(b measure, sign int) measure {
	return measure{events: a.events + sign*b.events, attempts: a.attempts + sign*b.attempts, echoes: a.echoes + sign*b.echoes,
		cancels: a.cancels + sign*b.cancels, opens: a.opens + sign*b.opens, bytes: a.bytes + int64(sign)*b.bytes}
}

// run is one run's records projected: its echoes by task hash, its attempts, its cancels and its result.
type run struct {
	ID, Tenant, Pool string
	// Hash is the task hash the run's statuses carry: the first non-rejected status's, else the first echo's until a
	// status arrives; hashed tells which set it.
	Hash     string
	hashed   string
	Echoes   map[string]echoFact
	Attempts map[int64]*attempt
	Highest  int64
	Cancels  []cancelFact
	Result   *resultFact
	Opens    map[string]*openFact
	// Storage is the pool's S3 destination in the terms in force when the run was admitted; StorageKnown is false
	// when the run has no echo or the terms history holds nothing by then.
	Storage      *operator.Storage
	StorageKnown bool
	First, Last  time.Time
	// seen holds the event IDs of the statuses kept, one per event.
	seen map[string]bool
	// me is the run's last measure, and truncated the caps that cut it.
	me        measure
	truncated map[string]bool
}

// measure is the run's collections counted and its estimated retained size.
func (r *run) measure() measure {
	me := measure{attempts: len(r.Attempts), echoes: len(r.Echoes), cancels: len(r.Cancels), opens: len(r.Opens)}
	me.bytes = int64(runOverhead + len(r.ID) + len(r.Tenant) + len(r.Pool) + len(r.Hash))
	for hash, e := range r.Echoes {
		me.bytes += int64(echoOverhead + len(hash) + len(e.Receiver) + len(e.Host) + len(e.Pool) + len(e.Agent) + len(e.Model))
	}
	for _, a := range r.Attempts {
		me.bytes += int64(attemptOverhead + len(a.Receiver) + len(a.TaskTopic) + len(a.ClaimDigest))
		me.events += len(a.Events)
		for _, e := range a.Events {
			me.bytes += int64(eventOverhead + len(e.EventID) + len(e.Receiver))
		}
		if a.Decision != nil {
			me.bytes += decisionOverhead
		}
		if a.StartFailure != nil {
			me.bytes += startFailureOverhead
		}
	}
	for _, o := range r.Opens {
		me.bytes += int64(openOverhead + len(o.Digest))
	}
	me.bytes += int64(len(r.Cancels)) * cancelOverhead
	if f := r.Result; f != nil {
		me.bytes += resultOverhead
		for _, o := range f.Outputs {
			me.bytes += int64(outputOverhead + len(o.Name))
		}
		for _, o := range f.S3Outputs {
			me.bytes += int64(outputOverhead + len(o.Name))
		}
		for _, o := range f.NotReturned {
			me.bytes += int64(outputOverhead + len(o.Name))
		}
	}
	return me
}

// mark records that cap cut the run.
func (r *run) mark(cap string) {
	if r.truncated == nil {
		r.truncated = map[string]bool{}
	}
	r.truncated[cap] = true
}

// forget drops what e left on its attempt: its event ID and, for a claim, the claim facts and decision.
func (r *run) forget(a *attempt, e event) {
	delete(r.seen, e.EventID)
	if e.Kind == "claimed" {
		a.TaskTopic, a.TaskOffset, a.ClaimDigest, a.Decision = "", nil, "", nil
	}
}

// prune drops an attempt left with nothing and re-derives the highest.
func (r *run) prune() {
	for number, a := range r.Attempts {
		if len(a.Events) == 0 && a.StartFailure == nil {
			delete(r.Attempts, number)
		}
	}
	r.Highest = 0
	for number := range r.Attempts {
		r.Highest = max(r.Highest, number)
	}
}

// dropOldestEvent drops the run's oldest event, from its lowest attempt, so the state derived from the highest
// stands; an attempt left with nothing goes with it. It reports false when the run has no event.
func (r *run) dropOldestEvent() bool {
	for _, number := range slices.Sorted(maps.Keys(r.Attempts)) {
		a := r.Attempts[number]
		if len(a.Events) == 0 {
			continue
		}
		e := a.Events[0]
		a.Events = slices.Delete(a.Events, 0, 1)
		r.forget(a, e)
		r.prune()
		return true
	}
	return false
}

// dropLowestAttempt drops the run's lowest attempt with what it holds, a newly made one included.
func (r *run) dropLowestAttempt() bool {
	if len(r.Attempts) == 0 {
		return false
	}
	lowest := slices.Min(slices.Collect(maps.Keys(r.Attempts)))
	for _, e := range r.Attempts[lowest].Events {
		delete(r.seen, e.EventID)
	}
	delete(r.Attempts, lowest)
	r.prune()
	return true
}

func (r *run) dropOldestCancel() bool {
	if len(r.Cancels) == 0 {
		return false
	}
	oldest := 0
	for i, c := range r.Cancels {
		if c.At.Before(r.Cancels[oldest].At) {
			oldest = i
		}
	}
	r.Cancels = slices.Delete(r.Cancels, oldest, oldest+1)
	return true
}

func (r *run) dropOldestOpen() bool {
	oldest := ""
	for digest, o := range r.Opens {
		if oldest == "" || o.At.Before(r.Opens[oldest].At) || o.At.Equal(r.Opens[oldest].At) && digest < oldest {
			oldest = digest
		}
	}
	if oldest == "" {
		return false
	}
	delete(r.Opens, oldest)
	return true
}

// oldestEcho is the hash of the run's oldest echo other than the joined one, or false when there is none.
func (r *run) oldestEcho() (string, bool) {
	oldest := ""
	for hash, e := range r.Echoes {
		if hash != r.Hash && (oldest == "" || e.At.Before(r.Echoes[oldest].At) || e.At.Equal(r.Echoes[oldest].At) && hash < oldest) {
			oldest = hash
		}
	}
	return oldest, oldest != ""
}

// empty reports whether nothing is left of the run.
func (r *run) empty() bool {
	return len(r.Echoes) == 0 && len(r.Attempts) == 0 && len(r.Opens) == 0 && len(r.Cancels) == 0 && r.Result == nil
}

// echo is the echo the run's statuses join to: the one carrying the run's task hash.
func (r *run) echo() (echoFact, bool) {
	e, ok := r.Echoes[r.Hash]
	return e, ok
}

// Host is the run's host, from its joined echo.
func (r *run) Host() string {
	e, _ := r.echo()
	return e.Host
}

// Admitted is the append time of the run's joined echo, zero without one.
func (r *run) Admitted() time.Time {
	e, _ := r.echo()
	return e.At
}

// terminal is the run's terminal event, at the highest attempt that has one; a lower attempt's never overrides.
func (r *run) terminal() (event, bool) {
	for _, number := range slices.Backward(slices.Sorted(maps.Keys(r.Attempts))) {
		for _, e := range slices.Backward(r.Attempts[number].Events) {
			if e.Kind == "terminal" || e.Kind == "rejected" {
				return e, true
			}
		}
	}
	return event{}, false
}

// latest is the latest event of the run's highest attempt, the stage a run in progress shows.
func (r *run) latest() (event, bool) {
	a := r.Attempts[r.Highest]
	if a == nil || len(a.Events) == 0 {
		return event{}, false
	}
	return a.Events[len(a.Events)-1], true
}

// holder is the receiver whose attempt is the run's current one, from its latest event.
func (r *run) holder() string {
	if a := r.Attempts[r.Highest]; a != nil {
		return a.Receiver
	}
	return ""
}

// class is the run's failure class: its terminal's, else its latest attempt's start failure's.
func (r *run) class() string {
	if t, ok := r.terminal(); ok {
		return t.Class
	}
	if a := r.Attempts[r.Highest]; a != nil && a.StartFailure != nil {
		return StartFailureClass(a.StartFailure.Class)
	}
	return ""
}

// state is the run's state: its terminal's, else its latest event's.
func (r *run) state() wire.TaskState {
	if t, ok := r.terminal(); ok {
		return t.State
	}
	if e, ok := r.latest(); ok {
		return e.State
	}
	return wire.TaskStateUnspecified
}

// stage is the run's stage: its terminal's state, else its latest event's kind, else "echoed".
func (r *run) stage() string {
	if t, ok := r.terminal(); ok {
		return stateWord(t.State)
	}
	if e, ok := r.latest(); ok {
		return e.Kind
	}
	return "echoed"
}

func (r *run) attempt(number int64) *attempt {
	a := r.Attempts[number]
	if a == nil {
		a = &attempt{Number: number}
		r.Attempts[number] = a
	}
	r.Highest = max(r.Highest, number)
	return a
}

func (r *run) touch(at time.Time) {
	if r.First.IsZero() || at.Before(r.First) {
		r.First = at
	}
	if at.After(r.Last) {
		r.Last = at
	}
}

// progressFact is one receiver's progress record for a run, the latest per run on its progress topic.
type progressFact struct {
	Holder, Run, Tenant, Hash string
	Attempt, LogOffset        int64
	At                        time.Time
}

// custodianFact is a pool's latest authorised custodian snapshot.
type custodianFact struct {
	Pool, Custodian, Reason, KeyID string
	At                             time.Time
	RotatedFor, SuspendedFor       int64
	ResumedFor                     int64
	src                            source
}

// pending is a record held for a key its signer rotated or registered since the keys were tailed, or for the record it
// joins, the run of a cancel, the claim of a decision, the request of a wrap or refusal, revalidated on each tick.
type pending struct {
	topic  string
	record *kgo.Record
	reason string
	since  time.Time
	// keysRead is the model's count of key tails when the record was held: a record still wanting a key after one
	// more is refused.
	keysRead int
}

// digests is a bounded set of record digests in two generations: when the current one fills, it becomes the older
// and the oldest is dropped, so each record is de-duplicated against at least cap records before it.
type digests struct {
	cap            int
	current, older map[[32]byte]struct{}
}

// add reports whether sum was absent, and whether adding it dropped a generation.
func (d *digests) add(sum [32]byte) (added, dropped bool) {
	if _, ok := d.current[sum]; ok {
		return false, false
	}
	if _, ok := d.older[sum]; ok {
		return false, false
	}
	if d.current == nil {
		d.current = map[[32]byte]struct{}{}
	}
	if len(d.current) >= d.cap {
		d.older, d.current = d.current, map[[32]byte]struct{}{}
		dropped = d.older != nil
	}
	d.current[sum] = struct{}{}
	return true, dropped
}

// model is the view's state behind its mutex: what the records read so far say.
type model struct {
	caps    Caps
	clock   func() time.Time
	history operator.History
	// names is the catalog of the configuration's names, rebuilt when the history extends.
	names *catalog
	keys  signed.Keys
	// signers is each key owner's principal, wherry.rcv.<receiver> or wherry.tnt.<tenant>.<host>, built from the state
	// as the key topics are tailed: the label of every fact a key of that owner verified.
	signers    map[string]string
	runs       map[string]*run
	progress   map[string]map[string]progressFact // holder -> run
	custodians map[string]custodianFact
	ads        map[string]wire.Advertisement
	digests    digests
	pending    []pending
	// counts are the model's counters: records applied, refused, skipped and dropped, and each cap's hits.
	counts map[string]int64
	// total is the model's measure over every run, and progressed the progress records kept over every receiver.
	total      measure
	progressed int
	// topics holds what the reads report of each topic followed.
	topics map[string]*topicState
	// keyNext is the offset each key topic's tail resumes at; keysRead counts the tails.
	keyNext  map[string]int64
	keysRead int
	changed  bool
}

// topicState is what the view knows of a topic it follows.
type topicState struct {
	Kind    string
	Next    int64
	Start   int64
	End     int64
	Missing bool
	// PredatesLog is set when the earliest retained record is newer than the window's start: the window predates the
	// log.
	PredatesLog bool
	Err         string
	started     bool
}

func newModel(caps Caps) *model {
	return &model{caps: caps, runs: map[string]*run{}, progress: map[string]map[string]progressFact{}, custodians: map[string]custodianFact{}, ads: map[string]wire.Advertisement{},
		digests: digests{cap: max(caps.Digests, 1)}, counts: map[string]int64{}, topics: map[string]*topicState{}, keyNext: map[string]int64{}}
}

func (m *model) count(name string, n int64) {
	m.counts[name] += n
}

// catalog is the configuration's names; it is rebuilt by the operator read, and built here only before the first.
func (m *model) catalog() *catalog {
	if m.names == nil {
		return newCatalog(m.history)
	}
	return m.names
}

// header is the JWS type and the kid's owner and key ID of value, unverified; ok is false when value is not shaped as
// a compact JWS with an EdDSA protected header. Only the header segment is decoded: the signature's parse, which
// decodes the whole record, verifies it.
func header(value []byte) (typ, owner, keyID string, ok bool) {
	end := bytes.IndexByte(value, '.')
	if end <= 0 || bytes.Count(value, []byte(".")) != 2 {
		return "", "", "", false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(string(value[:end]))
	if err != nil {
		return "", "", "", false
	}
	var protected struct {
		Alg string `json:"alg"`
		Typ string `json:"typ"`
		Kid string `json:"kid"`
	}
	if json.Unmarshal(decoded, &protected, signed.Tolerant) != nil || protected.Alg != string(jose.EdDSA) {
		return "", "", "", false
	}
	owner, keyID, _ = strings.Cut(protected.Kid, "/")
	return protected.Typ, owner, keyID, true
}

// keyMissing reports whether a record signed under owner's keyID failed to verify for want of a key the view may
// not have read yet: an owner with no key read, or a key ID that is neither the current nor a retired one.
func (m *model) keyMissing(owner, keyID string) bool {
	state, ok := m.keys[owner]
	if !ok || state.Current == nil {
		return true
	}
	if signed.KeyID(state.Current) == keyID {
		return false
	}
	return !slices.ContainsFunc(state.Retired, func(retired signed.RetiredKey) bool { return signed.KeyID(retired.Key) == keyID })
}

// The reasons a record is held pending.
const (
	pendingKey  = "key"
	pendingJoin = "join"
)

var errUnjoined = errors.New("record waits for the record it joins")

// parsed is a record's work done outside the lock and in parallel: its digest and header, then, once it is known to
// be new, its verification and decoding under the keys of the moment. The checks against the topic and the fold stay
// in order under the lock.
type parsed struct {
	sum               [32]byte
	digest            string
	typ, owner, keyID string
	ok                bool
	// skip is set by admit for a record over the size bound or seen before.
	skip  bool
	value wire.Record
	err   error
}

// prepare is the first phase: the digest and the JWS header.
func prepare(record *kgo.Record) *parsed {
	p := &parsed{sum: sha256.Sum256(record.Value)}
	p.digest = hex.EncodeToString(p.sum[:])
	p.typ, p.owner, p.keyID, p.ok = header(record.Value)
	return p
}

// src is the record's signer, as its verified header names, and its append time.
func (p *parsed) src(record *kgo.Record) source {
	return source{owner: p.owner, keyID: p.keyID, at: record.Timestamp}
}

// admit is the second phase, under the lock: a record over the size bound is skipped and counted before decoding,
// and one seen before is a duplicate.
func (m *model) admit(p *parsed, record *kgo.Record) {
	if len(record.Value) > m.caps.RecordBytes {
		m.count(capRecordBytes, 1)
		p.skip = true
		return
	}
	added, dropped := m.digests.add(p.sum)
	if dropped {
		m.count(capDigests, 1)
	}
	if !added {
		m.count("duplicates", 1)
		p.skip = true
	}
}

const cancelType = "application/vnd.wherry.cancel.v1+jws"

// parseRecord is the third phase: the record verified under keys and decoded as the type its header names, for the
// kind of topic it was read from. A type the view does not read is an error.
func parseRecord(kind string, p *parsed, record *kgo.Record, keys signed.Keys) {
	if p.skip || !p.ok {
		return
	}
	strict := false
	switch {
	case kind == "events" && p.typ == wire.StatusType, kind == "feed" && p.typ == wire.StatusType:
		p.value = &wire.RunStatus{}
	case kind == "events" && p.typ == wire.StartFailureType:
		p.value = &wire.StartFailure{}
	case kind == "events" && p.typ == cancelType:
		p.value = &wire.Cancel{}
	case kind == "events" && p.typ == wire.OpenRequestType:
		p.value, strict = &wire.OpenRequest{}, true
	case kind == "events" && p.typ == wire.AttemptWrapType:
		p.value, strict = &wire.AttemptWrap{}, true
	case kind == "events" && p.typ == wire.OpenRefusalType:
		p.value, strict = &wire.OpenRefusal{}, true
	case kind == "events" && p.typ == wire.ClaimDecisionType:
		p.value, strict = &wire.ClaimDecision{}, true
	case kind == "events" && p.typ == wire.RekeyWrapType:
		p.value, strict = &wire.RekeyWrap{}, true
	case kind == "feed" && p.typ == wire.EchoType:
		p.value = &wire.Echo{}
	case kind == "feed" && p.typ == wire.ResultType:
		p.value = &wire.Result{}
	default:
		p.err = errors.New("record is not one the view reads")
		return
	}
	if strict {
		p.err = wire.ParseStrict(record.Value, keys, record.Timestamp, p.value)
	} else {
		p.err = wire.ParseHistory(record.Timestamp)(record.Value, keys, p.value)
	}
}

// apply folds one record of a topic of kind ("events" with pool, "feed" with tenant, "progress" with the holder,
// "custodian" with pool), prepared, admitted and parsed as p, or inline when p is nil. It returns the reason the
// record is held pending, or "" when it was applied or refused. A record keyed by anything but a run ID wherry
// derives is refused.
func (m *model) apply(kind, name string, record *kgo.Record, p *parsed) string {
	if len(record.Value) > m.caps.RecordBytes {
		m.count(capRecordBytes, 1)
		return ""
	}
	switch kind {
	case "progress":
		m.applyProgress(name, record)
		return ""
	case "custodian":
		m.applyCustodian(name, record)
		return ""
	}
	if p == nil {
		p = prepare(record)
		m.admit(p, record)
		parseRecord(kind, p, record, m.keys)
	}
	if p.skip {
		return ""
	}
	if !p.ok {
		m.count("undecodable", 1)
		return ""
	}
	if !validRunID(string(record.Key)) {
		m.count("refused", 1)
		return ""
	}
	var err error
	switch kind {
	case "events":
		err = m.applyEvent(name, p, record)
	case "feed":
		err = m.applyFeed(name, p, record)
	}
	switch {
	case err == nil:
		m.changed = true
		if r := m.runs[string(record.Key)]; r != nil {
			m.bound(r)
		}
		return ""
	case errors.Is(err, errUnjoined):
		return pendingJoin
	case p.err != nil && m.keyMissing(p.owner, p.keyID):
		// The record may verify under a key read later; the digest stays, so the retry is its own record.
		return pendingKey
	}
	m.count("refused", 1)
	return ""
}

// hold keeps record pending for reason, within the pending cap, dropping and counting the oldest beyond it.
func (m *model) hold(kind, name string, record *kgo.Record, reason string, now time.Time) {
	m.pending = append(m.pending, pending{topic: kind + "\x00" + name, record: record, reason: reason, since: now, keysRead: m.keysRead})
	if excess := len(m.pending) - m.caps.Pending; excess > 0 {
		m.pending = slices.Clone(m.pending[excess:])
		m.count(capPending, int64(excess))
	}
}

// pendingFor is how long a record is held pending before it is dropped as refused.
const pendingFor = 10 * time.Minute

// revalidate applies each pending record again; one still pending is kept until it has waited pendingFor, one applied
// or refused is dropped. A retried record was de-duplicated when first seen, so its digest is forgotten first.
func (m *model) revalidate(now time.Time) {
	held := m.pending
	m.pending = nil
	for _, p := range held {
		kind, name, _ := strings.Cut(p.topic, "\x00")
		delete(m.digests.current, sha256.Sum256(p.record.Value))
		delete(m.digests.older, sha256.Sum256(p.record.Value))
		reason := m.apply(kind, name, p.record, nil)
		switch {
		case reason == "":
		case reason == pendingKey && m.keysRead > p.keysRead:
			// The key topics were tailed again and the record still verifies under none: it is signed under no key the
			// state names.
			m.count("refused", 1)
		case now.Sub(p.since) >= pendingFor:
			m.count("expired", 1)
		default:
			m.pending = append(m.pending, pending{topic: p.topic, record: p.record, reason: reason, since: p.since, keysRead: p.keysRead})
		}
	}
}

// runFor is the run of id, made on first sight.
func (m *model) runFor(id, tenant, pool string, at time.Time) *run {
	r := m.runs[id]
	if r == nil {
		r = &run{ID: id, Tenant: tenant, Pool: pool, Echoes: map[string]echoFact{}, Attempts: map[int64]*attempt{}, Opens: map[string]*openFact{}, seen: map[string]bool{}}
		m.runs[id] = r
		r.touch(at)
		m.evict(r)
	}
	r.touch(at)
	return r
}

// bound applies the retained-state bounds after a record of r was folded: the run's own caps trim its oldest facts,
// its measure is taken again, and the model's caps evict the oldest runs beyond them, r among them.
func (m *model) bound(r *run) {
	if m.trim(r) {
		return
	}
	m.remeasure(r)
	m.evict(r)
}

// trim drops r's oldest facts beyond the per-run caps, the run marked by each cap that cut it: the oldest events from
// the lowest attempt, the lowest attempt, the oldest echo other than the joined one, the oldest cancel and open
// request, and for the byte cap each of those in turn. A run whose joined echo and result alone are over a bound
// is dropped and counted; trim reports whether it was.
func (m *model) trim(r *run) bool {
	for {
		me := r.measure()
		cap, dropped := "", false
		switch {
		case m.caps.RunEvents > 0 && me.events > m.caps.RunEvents:
			cap, dropped = capRunEvents, r.dropOldestEvent()
		case m.caps.RunAttempts > 0 && me.attempts > m.caps.RunAttempts:
			cap, dropped = capRunAttempts, r.dropLowestAttempt()
		case m.caps.RunEchoes > 0 && me.echoes > m.caps.RunEchoes:
			cap, dropped = capRunEchoes, m.dropOldestEcho(r)
		case m.caps.RunCancels > 0 && me.cancels > m.caps.RunCancels:
			cap, dropped = capRunCancels, r.dropOldestCancel()
		case m.caps.RunOpens > 0 && me.opens > m.caps.RunOpens:
			cap, dropped = capRunOpens, r.dropOldestOpen()
		case m.caps.RunBytes > 0 && me.bytes > m.caps.RunBytes:
			cap, dropped = capRunBytes, r.dropOldestEvent() || m.dropOldestEcho(r) || r.dropOldestCancel() || r.dropOldestOpen() || r.dropLowestAttempt()
		}
		if cap == "" {
			return false
		}
		m.count(cap, 1)
		if !dropped {
			m.drop(r)
			return true
		}
		r.mark(cap)
	}
}

// dropOldestEcho drops r's oldest echo other than the joined one.
func (m *model) dropOldestEcho(r *run) bool {
	hash, ok := r.oldestEcho()
	if !ok {
		return false
	}
	m.removeEcho(r, hash)
	return true
}

// remeasure takes r's measure again and moves the model's totals by the difference.
func (m *model) remeasure(r *run) {
	me := r.measure()
	m.total = m.total.plus(me, 1).plus(r.me, -1)
	r.me = me
}

// drop forgets r and its measure.
func (m *model) drop(r *run) {
	m.total = m.total.plus(r.me, -1)
	delete(m.runs, r.ID)
}

// over is the model cap the runs kept exceed, if any: the runs, or their events, attempts, echoes, cancels, open
// requests or retained size over every run.
func (m *model) over() string {
	t := m.total
	switch {
	case len(m.runs) > m.caps.Runs:
		return capRuns
	case m.caps.Events > 0 && int64(t.events) > m.caps.Events:
		return capEvents
	case m.caps.Attempts > 0 && int64(t.attempts) > m.caps.Attempts:
		return capAttempts
	case m.caps.Echoes > 0 && int64(t.echoes) > m.caps.Echoes:
		return capEchoes
	case m.caps.Cancels > 0 && int64(t.cancels) > m.caps.Cancels:
		return capCancels
	case m.caps.Opens > 0 && int64(t.opens) > m.caps.Opens:
		return capOpens
	case m.caps.Bytes > 0 && t.bytes > m.caps.Bytes:
		return capBytes
	}
	return ""
}

// evict drops the oldest runs by last activity beyond the model's caps, kept last, and counts each under the cap it
// was dropped for: applied as each run is made or grows, while reading, not after. A run that alone exceeds a cap
// goes too.
func (m *model) evict(kept *run) {
	cap := m.over()
	if cap == "" {
		return
	}
	for _, r := range slices.SortedFunc(maps.Values(m.runs), func(a, b *run) int { return a.Last.Compare(b.Last) }) {
		if r == kept {
			continue
		}
		m.drop(r)
		m.count(cap, 1)
		if cap = m.over(); cap == "" {
			return
		}
	}
	if kept != nil && m.runs[kept.ID] == kept {
		m.drop(kept)
		m.count(cap, 1)
	}
}

// removeEcho drops r's echo of hash and re-derives what followed from it: the run's task hash and its storage. No other
// fact depends on an echo: each stands on its own signature.
func (m *model) removeEcho(r *run, hash string) {
	delete(r.Echoes, hash)
	r.rehash()
	m.storage(r)
}

// applyEvent folds a parsed pool events record, read from pool's events.
func (m *model) applyEvent(pool string, p *parsed, record *kgo.Record) error {
	if p.err != nil {
		return p.err
	}
	at, id, src := record.Timestamp, string(record.Key), p.src(record)
	names := m.catalog()
	switch value := p.value.(type) {
	case *wire.RunStatus:
		if value.Wherry.Run != id {
			return errors.New("status names another run than its key")
		}
		if err := operator.CheckStatus(*value, pool, ""); err != nil {
			return err
		}
		return m.addStatus(*value, p.digest, src)
	case *wire.StartFailure:
		if value.Wherry.Run != id {
			return errors.New("start failure names another run than its key")
		}
		if err := operator.CheckStartFailure(*value, pool); err != nil {
			return err
		}
		r := m.runFor(value.Wherry.Run, value.Wherry.Tenant, value.Wherry.Pool, at)
		a := r.attempt(value.Wherry.Attempt)
		a.Receiver = cmp.Or(a.Receiver, value.Wherry.Receiver)
		a.StartFailure = &startFailureFact{Profile: names.profile(value.Wherry.Receiver, value.Profile), Agent: names.agent(value.Wherry.Pool, value.Agent), Class: fixed(startClasses, value.Class), At: at, src: src}
		return nil
	case *wire.OpenRequest:
		if value.Run != id {
			return errors.New("open request names another run than its key")
		}
		r := m.runFor(value.Run, "", pool, at)
		r.Opens[p.digest] = &openFact{Digest: p.digest, Attempt: value.Attempt, Receiver: value.Receiver, At: at, src: src}
		return nil
	case *wire.AttemptWrap:
		return m.answer(id, value.Custodian, value.Request, "wrap", "", src)
	case *wire.OpenRefusal:
		return m.answer(id, value.Custodian, value.Request, "refusal", fixed(refuseReasons, value.Reason), src)
	case *wire.ClaimDecision:
		r := m.runs[id]
		if r == nil {
			return errUnjoined
		}
		for _, a := range r.Attempts {
			if a.ClaimDigest == value.Claim && a.Decision == nil {
				a.Decision = &decisionFact{Custodian: value.Custodian, Previous: names.receiver(value.Previous), Reason: fixed(claimReasons, value.Reason), Served: value.Served, At: at, src: src}
				r.touch(at)
				return nil
			}
		}
		return errUnjoined
	case *wire.RekeyWrap:
		m.count("rekeys", 1)
		return nil
	case *wire.Cancel:
		r := m.runs[id]
		if r == nil || r.Tenant == "" {
			return errUnjoined
		}
		if err := operator.CheckCancel(*value, pool, r.Tenant); err != nil {
			return err
		}
		if value.Run != id {
			return errors.New("cancel names another run than its key")
		}
		r.Cancels = append(r.Cancels, cancelFact{Host: value.Host, At: at, src: src})
		r.touch(at)
		return nil
	}
	return errors.New("record is not a pool event")
}

// answer joins a custodian's wrap or refusal to the open request it names, by digest.
func (m *model) answer(id, custodian, request, kind, reason string, src source) error {
	at := src.at
	r := m.runs[id]
	if r == nil || r.Opens[request] == nil {
		return errUnjoined
	}
	open := r.Opens[request]
	if open.Answer == "" {
		open.Answer, open.Custodian, open.Reason, open.AnsweredAt, open.answerSrc = kind, custodian, reason, at, src
	}
	r.touch(at)
	return nil
}

// applyFeed folds a parsed tenant feed record, read from tenant's feed.
func (m *model) applyFeed(tenant string, p *parsed, record *kgo.Record) error {
	if p.err != nil {
		return p.err
	}
	at, id, src := record.Timestamp, string(record.Key), p.src(record)
	names := m.catalog()
	switch value := p.value.(type) {
	case *wire.Echo:
		echo := *value
		if err := operator.CheckEcho(echo, tenant); err != nil {
			return err
		}
		if echo.Run != id || wire.CheckEchoRunID(echo) != nil {
			return errors.New("echo names a run its signer does not derive")
		}
		r := m.runFor(echo.Run, echo.Tenant, echo.Pool, at)
		if r.Tenant != echo.Tenant {
			// The echo binds the run to its tenant and host; statuses that named another tenant were not its.
			m.count("mismatched", int64(len(r.Attempts)))
			r.Tenant, r.Attempts, r.Highest, r.Hash, r.hashed, r.seen = echo.Tenant, map[int64]*attempt{}, 0, "", "", map[string]bool{}
		}
		if _, dup := r.Echoes[echo.TaskHash]; !dup {
			r.Echoes[echo.TaskHash] = echoFact{Receiver: echo.Receiver, Host: names.host(echo.Tenant, echo.Host), Pool: echo.Pool,
				Agent: names.agent(echo.Pool, echo.Placement.Agent), Model: names.model(echo.Pool, echo.Placement.Model), At: at, src: src}
		}
		if r.Hash == "" {
			r.Hash, r.hashed = echo.TaskHash, "echo"
		}
		if r.Pool == "" {
			r.Pool = echo.Pool
		}
		m.storage(r)
		return nil
	case *wire.RunStatus:
		if value.Wherry.Run != id {
			return errors.New("status names another run than its key")
		}
		if err := operator.CheckStatus(*value, "", tenant); err != nil {
			return err
		}
		return m.addStatus(*value, p.digest, src)
	case *wire.Result:
		result := *value
		if result.Wherry.Run != id {
			return errors.New("result names another run than its key")
		}
		if err := operator.CheckResult(result, tenant); err != nil {
			return err
		}
		r := m.runFor(result.Wherry.Run, result.Wherry.Tenant, result.Wherry.Pool, at)
		if r.Tenant != result.Wherry.Tenant {
			return errors.New("result names another tenant than its run")
		}
		if r.Result == nil || result.Wherry.Attempt >= r.Result.Attempt {
			r.Result = m.projectResult(result, src)
		}
		return nil
	}
	return errors.New("record is not a feed record the view reads")
}

// storage sets the run's S3 destination from its pool's terms in force when it was admitted.
func (m *model) storage(r *run) {
	e, ok := r.echo()
	if !ok {
		r.Storage, r.StorageKnown = nil, false
		return
	}
	terms, known := m.history.TermsAt(r.Pool, e.At)
	r.Storage, r.StorageKnown = nil, known
	if known && terms.Storage != nil {
		storage := *terms.Storage
		r.Storage = &storage
	}
}

// addStatus keeps a verified status under its run and attempt, once per event ID, never a lower attempt over a higher
// one's terminal: each attempt keeps its own history and the run's state is derived from the highest. Its kind, state
// and event are taken from the fixed sets, its event ID as wherry issues them or derived from the digest.
func (m *model) addStatus(status wire.RunStatus, digest string, src source) error {
	binding, at := status.Wherry, src.at
	r := m.runFor(binding.Run, binding.Tenant, binding.Pool, at)
	if r.Tenant != binding.Tenant {
		return errors.New("status names another tenant than its run")
	}
	if r.hashed != "status" && binding.Kind != "rejected" {
		// The statuses say which task the run ran; an echo of another task did not admit it.
		if r.Hash != "" && r.Hash != binding.TaskHash {
			m.count("mismatched", 1)
		}
		r.Hash, r.hashed = binding.TaskHash, "status"
		m.storage(r)
	}
	id := eventID(binding.EventID, digest)
	if r.seen[id] {
		return nil
	}
	r.seen[id] = true
	a := r.attempt(binding.Attempt)
	a.Receiver = binding.Receiver
	receiverAt, _ := time.Parse(time.RFC3339, status.StatusUpdate.Status.Timestamp)
	kind, state, named := fixed(statusKinds, binding.Kind), taskState(status.StatusUpdate.Status.State), fixed(statusEvents, binding.Event)
	e := event{Kind: kind, State: state, EventID: id, Attempt: binding.Attempt, Receiver: binding.Receiver, At: at, ReceiverAt: receiverAt, LogOffset: binding.LogOffset,
		Event: named, Class: FailureClass(kind, state, named), src: src}
	// Records of one attempt arrive in log order on each topic; the dual-written copy is the same event.
	i, _ := slices.BinarySearchFunc(a.Events, e, func(x, y event) int { return x.At.Compare(y.At) })
	a.Events = slices.Insert(a.Events, i, e)
	if binding.Kind == "claimed" {
		a.TaskTopic, a.TaskOffset, a.ClaimDigest = m.catalog().taskTopic(binding.TaskTopic), binding.TaskOffset, digest
	}
	return nil
}

// projectResult is the Reference facts of result: where it ran, output names, sizes and hashes, and the reply's size.
// The result's text, history, artifact parts and labels are never kept; the profile is kept only as the receiver's
// configuration names it.
func (m *model) projectResult(result wire.Result, src source) *resultFact {
	binding := result.Wherry
	fact := &resultFact{Receiver: binding.Receiver, Attempt: binding.Attempt, State: taskState(result.Task.Status.State), At: src.at, Artifacts: len(result.Task.Artifacts), src: src}
	var metadata wire.ResultMetadata
	if result.Task.Metadata != nil && json.Unmarshal(result.Task.Metadata, &metadata, signed.Tolerant) == nil {
		fact.Profile = m.catalog().profile(binding.Receiver, metadata.Profile)
		fact.Outputs = outputs(metadata.Outputs)
		fact.S3Outputs = outputs(metadata.S3Outputs)
		for _, output := range metadata.NotReturned {
			fact.NotReturned = append(fact.NotReturned, unreturnedFact{Name: outputName(output.Name), Size: output.Size, Reason: unreturnedReason(output.Reason)})
		}
		if metadata.Reply != nil {
			fact.ReplySize, fact.ReplyCut = metadata.Reply.Size, metadata.Reply.Cut
		}
	}
	return fact
}

// InvalidName is the placeholder for an output name outside the output name grammar.
const InvalidName = "(invalid name)"

// outputName is name when wire.ValidOutputName accepts it, segments of letters, digits, '.', '_' and '-', else the
// placeholder. Output names are the tenant's own: the one name a response carries that the configuration does not
// yield, shown because the grammar admits nothing that needs escaping on any surface.
func outputName(name string) string {
	if wire.ValidOutputName(name) {
		return name
	}
	return InvalidName
}

// outputs is each returned output whose hash is a SHA-256, with its name through outputName.
func outputs(returned []wire.ReturnedOutput) []outputFact {
	var facts []outputFact
	for _, output := range returned {
		if wire.ValidHash(output.SHA256) {
			facts = append(facts, outputFact{Name: outputName(output.Name), Size: output.Size, SHA256: output.SHA256})
		}
	}
	return facts
}

// unreturnedReasons are the fixed reasons a receiver lists an output as not returned with; any other shows as other.
var unreturnedReasons = []string{"not collected", "missing", "exceeds output limit", "exceeds the S3 object limit", "not streamed by the plugin", "rejected: size changed while read", "upload failed"}

func unreturnedReason(reason string) string {
	for _, known := range unreturnedReasons {
		if reason == known || strings.HasPrefix(reason, known+":") {
			return known
		}
	}
	return "other"
}

// applyProgress keeps the latest progress record per run of holder's progress topic; a tombstone drops the run's. A
// record keyed by anything but a run ID wherry derives is dropped, and a tenant the configuration does not name is
// kept as none, so the record matches no run's echo.
func (m *model) applyProgress(holder string, record *kgo.Record) {
	id := string(record.Key)
	if !validRunID(id) {
		m.count("undecodable", 1)
		return
	}
	if m.progress[holder] == nil {
		m.progress[holder] = map[string]progressFact{}
	}
	if record.Value == nil {
		if _, ok := m.progress[holder][id]; ok {
			m.progressed--
		}
		delete(m.progress[holder], id)
		m.changed = true
		return
	}
	progress, err := wire.DecodeProgress(record.Value)
	if err != nil {
		m.count("undecodable", 1)
		return
	}
	if _, ok := m.progress[holder][id]; !ok {
		m.progressed++
	}
	tenant := ""
	if m.catalog().tenants[progress.Tenant] {
		tenant = progress.Tenant
	}
	m.progress[holder][id] = progressFact{Holder: holder, Run: id, Tenant: tenant, Hash: progress.TaskHash, Attempt: progress.Attempt, LogOffset: progress.LogOffset, At: record.Timestamp}
	// Beyond the cap on progress records kept, the oldest over every receiver is dropped.
	for m.caps.Progress > 0 && m.progressed > m.caps.Progress {
		var oldest *progressFact
		for _, runs := range m.progress {
			for _, fact := range runs {
				if (fact.Holder != holder || fact.Run != id) && (oldest == nil || fact.At.Before(oldest.At)) {
					oldest = &fact
				}
			}
		}
		if oldest == nil {
			break
		}
		delete(m.progress[oldest.Holder], oldest.Run)
		m.progressed--
		m.count(capProgress, 1)
	}
	m.changed = true
}

// custodianReasons are the fixed reasons a snapshot gives.
var custodianReasons = []string{"bootstrap", "rotation", "takeover", "contested"}

// applyCustodian keeps pool's latest snapshot: one its custodian signed, naming the pool whose topic it was read from.
// Whether the custodian is still a member is not asked: an ended custodian's snapshot is history, labelled with its
// signer.
func (m *model) applyCustodian(pool string, record *kgo.Record) {
	if string(record.Key) != wire.CustodianKey || record.Value == nil {
		return
	}
	snapshot, err := wire.ParseSnapshotHistory(record.Value, m.keys, record.Timestamp)
	if err != nil || snapshot.Pool != pool {
		m.count("refused", 1)
		return
	}
	if !keyIDPattern.MatchString(snapshot.Current.KeyID) {
		m.count("refused", 1)
		return
	}
	_, owner, keyID, _ := header(record.Value)
	m.custodians[pool] = custodianFact{Pool: pool, Custodian: snapshot.Custodian, Reason: fixed(custodianReasons, snapshot.Reason), KeyID: snapshot.Current.KeyID, At: record.Timestamp,
		RotatedFor: snapshot.RotatedFor, SuspendedFor: snapshot.SuspendedFor, ResumedFor: snapshot.ResumedFor, src: source{owner: owner, keyID: keyID, at: record.Timestamp}}
	m.changed = true
}

// retire retracts every fact folded from a record signed under owner's keyID and appended at or after since, the time
// a rotation seen since retired it: such a record no longer verifies. Facts under other keys stand, an echo's
// dependents included. A run left with nothing is dropped.
func (m *model) retire(owner, keyID string, since time.Time) {
	gone := func(s source) bool { return s.retiredBy(owner, keyID, since) }
	var retracted int64
	for _, r := range m.runs {
		for number, a := range r.Attempts {
			a.Events = slices.DeleteFunc(a.Events, func(e event) bool {
				if !gone(e.src) {
					return false
				}
				r.forget(a, e)
				retracted++
				return true
			})
			if a.Decision != nil && gone(a.Decision.src) {
				a.Decision = nil
				retracted++
			}
			if a.StartFailure != nil && gone(a.StartFailure.src) {
				a.StartFailure = nil
				retracted++
			}
			if len(a.Events) == 0 && a.StartFailure == nil {
				delete(r.Attempts, number)
			}
		}
		for digest, o := range r.Opens {
			switch {
			case gone(o.src):
				delete(r.Opens, digest)
				retracted++
			case o.Answer != "" && gone(o.answerSrc):
				o.Answer, o.Custodian, o.Reason, o.AnsweredAt, o.answerSrc = "", "", "", time.Time{}, source{}
				retracted++
			}
		}
		r.Cancels = slices.DeleteFunc(r.Cancels, func(c cancelFact) bool {
			if gone(c.src) {
				retracted++
				return true
			}
			return false
		})
		if r.Result != nil && gone(r.Result.src) {
			r.Result = nil
			retracted++
		}
		for _, hash := range slices.Sorted(maps.Keys(r.Echoes)) {
			if gone(r.Echoes[hash].src) {
				m.removeEcho(r, hash)
				retracted++
			}
		}
		r.prune()
		r.rehash()
		m.storage(r)
		m.remeasure(r)
		if r.empty() {
			m.drop(r)
		}
	}
	for pool, c := range m.custodians {
		if gone(c.src) {
			delete(m.custodians, pool)
			retracted++
		}
	}
	if retracted > 0 {
		m.count("retracted", retracted)
		m.changed = true
	}
}

// rehash re-derives the run's task hash after a retraction: the statuses' when a non-rejected status remains, else
// the earliest echo's, else none.
func (r *run) rehash() {
	if r.hashed == "status" {
		for _, a := range r.Attempts {
			if slices.ContainsFunc(a.Events, func(e event) bool { return e.Kind != "rejected" }) {
				return
			}
		}
	} else if r.hashed == "echo" {
		if _, ok := r.Echoes[r.Hash]; ok {
			return
		}
	}
	r.Hash, r.hashed = "", ""
	for hash, e := range r.Echoes {
		if r.Hash == "" || e.At.Before(r.Echoes[r.Hash].At) {
			r.Hash, r.hashed = hash, "echo"
		}
	}
}

// inFlight is what a receiver holds: the runs its progress records identify, newest first, up to its advertised count
// when it is live, the rest stale; the count beyond them not identified. A stale or silent holder's runs are unknown.
type inFlight struct {
	Count        int64
	Identified   []heldRun
	Stale        []heldRun
	Unidentified int64
	Status       string
}

type heldRun struct {
	Run     string
	Attempt int64
	Stage   string
	Stale   bool
}

// held derives receiver's in-flight runs at now.
func (m *model) held(receiver string, now time.Time) inFlight {
	status := operator.ReceiverStatus(m.history.State, m.ads, receiver, now)
	held := inFlight{Status: status}
	var candidates []progressFact
	for _, progress := range m.progress[receiver] {
		r := m.runs[progress.Run]
		if r != nil {
			// A run whose terminal is in the model at the same or a higher attempt is over, whatever its progress says;
			// a record naming another tenant or task than the run's echo is not the run's.
			if t, ok := r.terminal(); ok && t.Attempt >= progress.Attempt {
				continue
			}
			if _, ok := r.echo(); ok && (r.Tenant != progress.Tenant || r.Hash != progress.Hash) {
				continue
			}
		}
		candidates = append(candidates, progress)
	}
	slices.SortFunc(candidates, func(a, b progressFact) int { return cmp.Or(b.At.Compare(a.At), strings.Compare(a.Run, b.Run)) })
	if status != "live" {
		for _, progress := range candidates {
			held.Stale = append(held.Stale, heldRun{Run: progress.Run, Attempt: progress.Attempt, Stage: StageUnknown, Stale: true})
		}
		return held
	}
	held.Count = m.ads[receiver].InFlight
	for i, progress := range candidates {
		stage := StageInFlight
		if r := m.runs[progress.Run]; r != nil {
			if e, ok := r.latest(); ok {
				stage = e.Kind
			}
		}
		// Progress of an attempt below the run's highest, or of a run another receiver now holds, is stale: the run was
		// adopted since.
		stale := int64(i) >= held.Count
		if r := m.runs[progress.Run]; r != nil && (r.Highest > progress.Attempt || r.holder() != "" && r.holder() != receiver) {
			stale = true
		}
		h := heldRun{Run: progress.Run, Attempt: progress.Attempt, Stage: stage, Stale: stale}
		if stale {
			held.Stale = append(held.Stale, h)
		} else {
			held.Identified = append(held.Identified, h)
		}
	}
	held.Unidentified = max(held.Count-int64(len(held.Identified)), 0)
	return held
}

// noTerminal reports whether r is a released run that exhausted its pool's delivery limit without a terminal.
func (m *model) noTerminal(r *run) bool {
	if _, ok := r.terminal(); ok {
		return false
	}
	e, ok := r.latest()
	terms, known := m.history.State.Pools[r.Pool]
	return ok && e.Kind == "released" && known && r.Highest >= int64(terms.DeliveryLimit)
}
