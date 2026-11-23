package view

import (
	"cmp"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/wire"
)

// Snapshot is the model as the API serves it: every node kind, idle or not, the runs of the window and the view's own
// state. Every field is named here; nothing is copied through from a record.
type Snapshot struct {
	Version     uint64        `json:"version"`
	Now         time.Time     `json:"now"`
	WindowStart time.Time     `json:"window_start"`
	Rebuilt     bool          `json:"rebuilt"`
	RebuiltAt   time.Time     `json:"rebuilt_at,omitzero"`
	Brokers     []Broker      `json:"brokers"`
	Pools       []Pool        `json:"pools"`
	Receivers   []Receiver    `json:"receivers"`
	Custodians  []Custodian   `json:"custodians"`
	Hosts       []Host        `json:"hosts"`
	Tenants     []Tenant      `json:"tenants"`
	Storage     []StorageNode `json:"storage"`
	Runs        []RunSummary  `json:"runs"`
	RunsTotal   int           `json:"runs_total"`
	// Truncated is set when the runs list was cut to fit the response bound.
	Truncated bool             `json:"truncated,omitzero"`
	Topics    []Topic          `json:"topics"`
	Counts    map[string]int64 `json:"counts"`
	// Bounds lists each cap hit and how often.
	Bounds []BoundHit `json:"bounds"`
	Errors []string   `json:"errors,omitzero"`
}

type Broker struct {
	ID         int32    `json:"id"`
	Host       string   `json:"host"`
	Port       int32    `json:"port"`
	Rack       string   `json:"rack,omitzero"`
	Controller bool     `json:"controller"`
	Listeners  []string `json:"listeners,omitzero"`
}

type Pool struct {
	Name          string         `json:"name"`
	Suspended     bool           `json:"suspended"`
	Receivers     []string       `json:"receivers"`
	Tenants       []string       `json:"tenants"`
	Custodian     string         `json:"custodian,omitzero"`
	Storage       *Storage       `json:"storage,omitzero"`
	DeliveryLimit int32          `json:"delivery_limit"`
	Group         Group          `json:"group"`
	Lag           Lag            `json:"lag"`
	Runs          int            `json:"runs"`
	InProgress    int            `json:"in_progress"`
	ByState       map[string]int `json:"by_state"`
	ByClass       map[string]int `json:"by_class"`
	NoTerminal    int            `json:"no_terminal"`
	Agents        []string       `json:"agents"`
}

type Group struct {
	State   string `json:"state,omitzero"`
	Members int    `json:"members"`
}

// Lag is a pool's share-group lag: records not yet acknowledged past the start offset; Status is ok, zero,
// unavailable or error.
type Lag struct {
	Status  string    `json:"status"`
	Records int64     `json:"records"`
	Error   string    `json:"error,omitzero"`
	At      time.Time `json:"at,omitzero"`
}

type Storage struct {
	Endpoint string `json:"endpoint"`
	Bucket   string `json:"bucket"`
}

type Receiver struct {
	Name           string         `json:"name"`
	Pool           string         `json:"pool"`
	Status         string         `json:"status"`
	Ended          bool           `json:"ended"`
	Capacity       int64          `json:"capacity"`
	InFlight       int64          `json:"in_flight"`
	Gated          string         `json:"gated,omitzero"`
	Offers         []Offer        `json:"offers"`
	LastAdvertised time.Time      `json:"last_advertised,omitzero"`
	Profiles       []string       `json:"profiles"`
	Runs           int            `json:"runs"`
	ByState        map[string]int `json:"by_state"`
	ByClass        map[string]int `json:"by_class"`
	Held           []HeldRun      `json:"held"`
	Stale          []HeldRun      `json:"stale"`
	Unidentified   int64          `json:"unidentified"`
}

type Offer struct {
	Profile  string `json:"profile"`
	Agent    string `json:"agent"`
	Replicas int64  `json:"replicas"`
	InFlight int64  `json:"in_flight"`
}

type HeldRun struct {
	Run     string `json:"run"`
	Attempt int64  `json:"attempt"`
	Stage   string `json:"stage"`
	Stale   bool   `json:"stale,omitzero"`
}

type Custodian struct {
	Pool         string    `json:"pool"`
	Receiver     string    `json:"receiver"`
	Status       string    `json:"status"`
	KeyID        string    `json:"key_id"`
	Reason       string    `json:"reason"`
	At           time.Time `json:"at"`
	RotatedFor   int64     `json:"rotated_for"`
	SuspendedFor int64     `json:"suspended_for"`
	ResumedFor   int64     `json:"resumed_for"`
	Suspended    bool      `json:"suspended"`
}

type Host struct {
	Tenant  string         `json:"tenant"`
	Name    string         `json:"name"`
	Ended   bool           `json:"ended"`
	Runs    int            `json:"runs"`
	ByState map[string]int `json:"by_state"`
}

type Tenant struct {
	Name  string   `json:"name"`
	Ended bool     `json:"ended"`
	Pools []string `json:"pools"`
	Hosts []string `json:"hosts"`
	Runs  int      `json:"runs"`
}

// StorageNode is one S3 destination, an endpoint and bucket from pool terms, with what the signed results sent to it.
type StorageNode struct {
	Endpoint   string   `json:"endpoint"`
	Bucket     string   `json:"bucket"`
	Pools      []string `json:"pools"`
	Runs       int      `json:"runs"`
	Outputs    int      `json:"outputs"`
	Bytes      int64    `json:"bytes"`
	Unreturned int      `json:"unreturned"`
	FailedRuns int      `json:"failed_runs"`
}

type RunSummary struct {
	ID       string `json:"id"`
	Tenant   string `json:"tenant"`
	Host     string `json:"host,omitzero"`
	Pool     string `json:"pool"`
	Receiver string `json:"receiver,omitzero"`
	Agent    string `json:"agent,omitzero"`
	Model    string `json:"model,omitzero"`
	Attempt  int64  `json:"attempt"`
	Stage    string `json:"stage"`
	State    string `json:"state"`
	Class    string `json:"class,omitzero"`
	Terminal bool   `json:"terminal"`
	// StorageUnknown is set for a run without an echo in the window or whose pool's terms history the view did not
	// read.
	Storage        *Storage  `json:"storage,omitzero"`
	StorageUnknown bool      `json:"storage_unknown,omitzero"`
	Cancelled      bool      `json:"cancelled,omitzero"`
	Admitted       time.Time `json:"admitted,omitzero"`
	First          time.Time `json:"first"`
	Last           time.Time `json:"last"`
	Outputs        int       `json:"outputs"`
}

type RunDetail struct {
	RunSummary
	Hash             string     `json:"task_hash,omitzero"`
	Echoes           []Echo     `json:"echoes"`
	MismatchedEchoes int        `json:"mismatched_echoes,omitzero"`
	Attempts         []Attempt  `json:"attempts"`
	Opens            []Open     `json:"opens"`
	Cancels          []Cancel   `json:"cancels"`
	Result           *Result    `json:"result,omitzero"`
	Path             []PathStep `json:"path"`
}

type Echo struct {
	Receiver string    `json:"receiver"`
	Host     string    `json:"host"`
	Pool     string    `json:"pool"`
	Agent    string    `json:"agent"`
	Model    string    `json:"model,omitzero"`
	At       time.Time `json:"at"`
	Joined   bool      `json:"joined"`
}

type Attempt struct {
	Number       int64         `json:"number"`
	Receiver     string        `json:"receiver"`
	TaskTopic    string        `json:"task_topic,omitzero"`
	TaskOffset   *int64        `json:"task_offset,omitzero"`
	Events       []Event       `json:"events"`
	Decision     *Decision     `json:"decision,omitzero"`
	StartFailure *StartFailure `json:"start_failure,omitzero"`
}

type Event struct {
	Kind       string    `json:"kind"`
	State      string    `json:"state"`
	EventID    string    `json:"event_id"`
	Receiver   string    `json:"receiver"`
	At         time.Time `json:"at"`
	ReceiverAt time.Time `json:"receiver_at,omitzero"`
	LogOffset  *int64    `json:"log_offset,omitzero"`
	Event      string    `json:"event,omitzero"`
	Class      string    `json:"class,omitzero"`
}

type Decision struct {
	Custodian string    `json:"custodian"`
	Previous  string    `json:"previous,omitzero"`
	Reason    string    `json:"reason"`
	Served    bool      `json:"served"`
	At        time.Time `json:"at"`
}

type StartFailure struct {
	Profile string    `json:"profile"`
	Agent   string    `json:"agent"`
	Class   string    `json:"class"`
	At      time.Time `json:"at"`
}

type Open struct {
	Attempt    int64     `json:"attempt"`
	Receiver   string    `json:"receiver"`
	At         time.Time `json:"at"`
	Answer     string    `json:"answer,omitzero"`
	Custodian  string    `json:"custodian,omitzero"`
	Reason     string    `json:"reason,omitzero"`
	AnsweredAt time.Time `json:"answered_at,omitzero"`
}

type Cancel struct {
	Host string    `json:"host"`
	At   time.Time `json:"at"`
}

type Result struct {
	Receiver    string            `json:"receiver"`
	Attempt     int64             `json:"attempt"`
	State       string            `json:"state"`
	At          time.Time         `json:"at"`
	Profile     string            `json:"profile,omitzero"`
	Labels      map[string]string `json:"labels,omitzero"`
	Outputs     []Output          `json:"outputs"`
	S3Outputs   []Output          `json:"s3_outputs"`
	NotReturned []Unreturned      `json:"not_returned"`
	ReplySize   int64             `json:"reply_size"`
	ReplyCut    bool              `json:"reply_cut,omitzero"`
	Artifacts   int               `json:"artifacts"`
}

type Output struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type Unreturned struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	Reason string `json:"reason"`
}

// PathStep is one hop of a run's path: who did what, when.
type PathStep struct {
	Step    string    `json:"step"`
	Actor   string    `json:"actor"`
	Attempt int64     `json:"attempt,omitzero"`
	At      time.Time `json:"at"`
	Detail  string    `json:"detail,omitzero"`
}

type Topic struct {
	Name        string `json:"name"`
	Kind        string `json:"kind"`
	Next        int64  `json:"next"`
	Start       int64  `json:"start"`
	End         int64  `json:"end"`
	Missing     bool   `json:"missing,omitzero"`
	PredatesLog bool   `json:"predates_log,omitzero"`
	Error       string `json:"error,omitzero"`
}

type BoundHit struct {
	Name string `json:"name"`
	Hits int64  `json:"hits"`
}

// capNames are the counters that are caps.
var capNames = []string{capRecordBytes, capRuns, capDigests, capPending, capCursors, capResponse, capEvent, capTraceBytes, capTraceRecords, capTraceSpans, capTraceForest, capTraceScan, capTraceScans, capTraceRuns}

// Snapshot is the model now, with at most maxRuns runs, newest last activity first; 0 is every run.
func (v *View) Snapshot(maxRuns int) Snapshot {
	v.mu.RLock()
	defer v.mu.RUnlock()
	m, now := v.m, v.opts.Now()
	state := m.history.State
	s := Snapshot{Version: v.version, Now: now, WindowStart: v.windowStart, Rebuilt: v.rebuilt, RebuiltAt: v.rebuiltAt, Counts: maps.Clone(m.counts), Errors: slices.Clone(v.errs)}
	s.Counts["pending"] = int64(len(m.pending))
	for _, broker := range v.brokers {
		s.Brokers = append(s.Brokers, Broker(broker))
	}
	runs := slices.SortedFunc(maps.Values(m.runs), func(a, b *run) int { return cmp.Or(b.Last.Compare(a.Last), strings.Compare(a.ID, b.ID)) })
	s.RunsTotal = len(runs)
	type tally struct {
		runs, inProgress, noTerminal int
		byState, byClass             map[string]int
	}
	newTally := func() *tally { return &tally{byState: map[string]int{}, byClass: map[string]int{}} }
	pools, receivers, hosts, tenants := map[string]*tally{}, map[string]*tally{}, map[string]*tally{}, map[string]*tally{}
	storage := map[Storage]*StorageNode{}
	tallyRun := func(tallies map[string]*tally, key string, r *run, summary RunSummary) {
		if key == "" {
			return
		}
		t := tallies[key]
		if t == nil {
			t = newTally()
			tallies[key] = t
		}
		t.runs++
		t.byState[summary.State]++
		if summary.Class != "" {
			t.byClass[summary.Class]++
		}
		if !summary.Terminal {
			t.inProgress++
		}
		if summary.Stage == StageNoTerminal {
			t.noTerminal++
		}
	}
	for i, r := range runs {
		summary := m.summary(r)
		if maxRuns <= 0 || i < maxRuns {
			s.Runs = append(s.Runs, summary)
		} else {
			s.Truncated = true
		}
		tallyRun(pools, r.Pool, r, summary)
		tallyRun(receivers, r.holder(), r, summary)
		tallyRun(hosts, operator.TenantPrincipal(r.Tenant, r.Host()), r, summary)
		tallyRun(tenants, r.Tenant, r, summary)
		if r.Storage != nil {
			key := Storage(*r.Storage)
			node := storage[key]
			if node == nil {
				node = &StorageNode{Endpoint: key.Endpoint, Bucket: key.Bucket}
				storage[key] = node
			}
			node.Runs++
			if !slices.Contains(node.Pools, r.Pool) {
				node.Pools = append(node.Pools, r.Pool)
			}
			if r.Result != nil {
				node.Outputs += len(r.Result.S3Outputs)
				for _, output := range r.Result.S3Outputs {
					node.Bytes += output.Size
				}
				node.Unreturned += len(r.Result.NotReturned)
			}
			if summary.Terminal && summary.State != string(wire.TaskStateCompleted) {
				node.FailedRuns++
			}
		}
	}
	for _, pool := range slices.Sorted(maps.Keys(state.Pools)) {
		terms := state.Pools[pool]
		t := cmp.Or(pools[pool], newTally())
		p := Pool{Name: pool, Suspended: state.Suspended[pool], Receivers: []string{}, Tenants: []string{}, DeliveryLimit: terms.DeliveryLimit,
			Group: Group(v.groups[pool]), Lag: Lag(v.lag[pool]), Runs: t.runs, InProgress: t.inProgress, ByState: t.byState, ByClass: t.byClass, NoTerminal: t.noTerminal, Agents: []string{}}
		if p.Lag.Status == "" {
			p.Lag.Status = LagUnavailable
		}
		if terms.Storage != nil {
			p.Storage = &Storage{Endpoint: terms.Storage.Endpoint, Bucket: terms.Storage.Bucket}
			key := *p.Storage
			if storage[key] == nil {
				storage[key] = &StorageNode{Endpoint: key.Endpoint, Bucket: key.Bucket}
			}
			if !slices.Contains(storage[key].Pools, pool) {
				storage[key].Pools = append(storage[key].Pools, pool)
			}
		}
		for _, agent := range terms.Agents {
			p.Agents = append(p.Agents, agent.Name)
		}
		for receiver, config := range state.Receivers {
			if config.Pool == pool {
				p.Receivers = append(p.Receivers, receiver)
			}
		}
		slices.Sort(p.Receivers)
		for tenant, entitled := range state.Entitlements {
			if entitled[pool] {
				p.Tenants = append(p.Tenants, tenant)
			}
		}
		slices.Sort(p.Tenants)
		if custodian, ok := m.custodians[pool]; ok {
			p.Custodian = custodian.Custodian
			s.Custodians = append(s.Custodians, Custodian{Pool: pool, Receiver: custodian.Custodian, Status: operator.ReceiverStatus(state, m.ads, custodian.Custodian, now), KeyID: custodian.KeyID, Reason: custodian.Reason, At: custodian.At,
				RotatedFor: custodian.RotatedFor, SuspendedFor: custodian.SuspendedFor, ResumedFor: custodian.ResumedFor, Suspended: state.Suspended[pool]})
		}
		s.Pools = append(s.Pools, p)
	}
	for _, receiver := range slices.Sorted(maps.Keys(state.Receivers)) {
		config := state.Receivers[receiver]
		t := cmp.Or(receivers[receiver], newTally())
		held := m.held(receiver, now)
		node := Receiver{Name: receiver, Pool: config.Pool, Status: held.Status, Ended: state.Ended[operator.ReceiverPrincipal(receiver)], Offers: []Offer{}, Profiles: []string{},
			Runs: t.runs, ByState: t.byState, ByClass: t.byClass, Held: []HeldRun{}, Stale: []HeldRun{}, Unidentified: held.Unidentified}
		for _, profile := range config.Profiles {
			node.Profiles = append(node.Profiles, profile.Name)
		}
		if ad, ok := m.ads[receiver]; ok && !node.Ended {
			node.Capacity, node.InFlight, node.Gated, node.LastAdvertised = ad.Capacity, ad.InFlight, ad.Gated, time.UnixMilli(ad.AtMS).UTC()
			for _, offer := range ad.Offers {
				node.Offers = append(node.Offers, Offer{Profile: offer.Profile, Agent: offer.Agent, Replicas: offer.Replicas, InFlight: offer.InFlight})
			}
		}
		for _, h := range held.Identified {
			node.Held = append(node.Held, HeldRun(h))
		}
		for _, h := range held.Stale {
			node.Stale = append(node.Stale, HeldRun(h))
		}
		s.Receivers = append(s.Receivers, node)
	}
	for _, principal := range slices.Sorted(maps.Keys(state.Hosts)) {
		host := state.Hosts[principal]
		t := cmp.Or(hosts[principal], newTally())
		s.Hosts = append(s.Hosts, Host{Tenant: host.Tenant, Name: host.Name, Ended: state.Ended[principal], Runs: t.runs, ByState: t.byState})
	}
	for _, tenant := range slices.Sorted(maps.Keys(state.Tenants)) {
		t := cmp.Or(tenants[tenant], newTally())
		node := Tenant{Name: tenant, Ended: state.Tenants[tenant], Pools: slices.Sorted(maps.Keys(state.Entitlements[tenant])), Hosts: []string{}, Runs: t.runs}
		if node.Pools == nil {
			node.Pools = []string{}
		}
		for principal, host := range state.Hosts {
			if host.Tenant == tenant && !state.Ended[principal] {
				node.Hosts = append(node.Hosts, host.Name)
			}
		}
		slices.Sort(node.Hosts)
		s.Tenants = append(s.Tenants, node)
	}
	for _, key := range slices.SortedFunc(maps.Keys(storage), func(a, b Storage) int {
		return cmp.Or(strings.Compare(a.Endpoint, b.Endpoint), strings.Compare(a.Bucket, b.Bucket))
	}) {
		node := storage[key]
		slices.Sort(node.Pools)
		s.Storage = append(s.Storage, *node)
	}
	for _, topic := range slices.Sorted(maps.Keys(m.topics)) {
		t := m.topics[topic]
		s.Topics = append(s.Topics, Topic{Name: topic, Kind: t.Kind, Next: t.Next, Start: t.Start, End: t.End, Missing: t.Missing, PredatesLog: t.PredatesLog, Error: t.Err})
	}
	for _, name := range capNames {
		if hits := m.counts[name]; hits > 0 {
			s.Bounds = append(s.Bounds, BoundHit{Name: name, Hits: hits})
		}
	}
	for _, list := range []*[]Broker{&s.Brokers} {
		if *list == nil {
			*list = []Broker{}
		}
	}
	if s.Pools == nil {
		s.Pools = []Pool{}
	}
	if s.Receivers == nil {
		s.Receivers = []Receiver{}
	}
	if s.Custodians == nil {
		s.Custodians = []Custodian{}
	}
	if s.Hosts == nil {
		s.Hosts = []Host{}
	}
	if s.Tenants == nil {
		s.Tenants = []Tenant{}
	}
	if s.Storage == nil {
		s.Storage = []StorageNode{}
	}
	if s.Runs == nil {
		s.Runs = []RunSummary{}
	}
	if s.Topics == nil {
		s.Topics = []Topic{}
	}
	if s.Bounds == nil {
		s.Bounds = []BoundHit{}
	}
	return s
}

// summary is r as the API lists it; the caller holds mu.
func (m *model) summary(r *run) RunSummary {
	summary := RunSummary{ID: r.ID, Tenant: r.Tenant, Host: r.Host(), Pool: r.Pool, Receiver: r.holder(), Attempt: r.Highest, Stage: r.stage(), State: string(r.state()), Class: r.class(),
		Admitted: r.Admitted(), First: r.First, Last: r.Last, Cancelled: len(r.Cancels) > 0, StorageUnknown: !r.StorageKnown}
	placement := r.Placement()
	summary.Agent, summary.Model = placement.Agent, placement.Model
	if _, ok := r.terminal(); ok {
		summary.Terminal = true
	}
	if m.noTerminal(r) {
		summary.Stage = StageNoTerminal
	} else if !summary.Terminal {
		if holder := r.holder(); holder != "" && operator.ReceiverStatus(m.history.State, m.ads, holder, m.now()) != "live" {
			summary.Stage = StageUnknown
		}
	}
	if r.Storage != nil {
		summary.Storage = &Storage{Endpoint: r.Storage.Endpoint, Bucket: r.Storage.Bucket}
	}
	if r.Result != nil {
		summary.Outputs = len(r.Result.Outputs) + len(r.Result.S3Outputs)
	}
	return summary
}

// now is the model's clock, set by the view.
func (m *model) now() time.Time {
	if m.clock != nil {
		return m.clock()
	}
	return time.Now()
}

// RunDetail is run id's detail: its echoes, attempts and events, open exchange, cancels, result references and path.
func (v *View) RunDetail(id string) (RunDetail, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	r, ok := v.m.runs[id]
	if !ok {
		return RunDetail{}, false
	}
	d := RunDetail{RunSummary: v.m.summary(r), Hash: r.Hash, Echoes: []Echo{}, Attempts: []Attempt{}, Opens: []Open{}, Cancels: []Cancel{}, Path: []PathStep{}}
	// An echo carrying another task hash than the run's statuses did not admit them; it is counted, never shown.
	if e, ok := r.echo(); ok {
		d.Echoes = append(d.Echoes, Echo{Receiver: e.Receiver, Host: e.Host, Pool: e.Pool, Agent: e.Placement.Agent, Model: e.Placement.Model, At: e.At, Joined: true})
	}
	d.MismatchedEchoes = len(r.Echoes) - len(d.Echoes)
	if e, ok := r.echo(); ok {
		d.Path = append(d.Path, PathStep{Step: "admitted", Actor: e.Host + " > " + e.Receiver, At: e.At})
	}
	for _, number := range slices.Sorted(maps.Keys(r.Attempts)) {
		a := r.Attempts[number]
		attempt := Attempt{Number: number, Receiver: a.Receiver, TaskTopic: a.TaskTopic, TaskOffset: a.TaskOffset, Events: []Event{}}
		for _, e := range a.Events {
			attempt.Events = append(attempt.Events, Event{Kind: e.Kind, State: string(e.State), EventID: e.EventID, Receiver: e.Receiver, At: e.At, ReceiverAt: e.ReceiverAt, LogOffset: e.LogOffset, Event: e.Event, Class: e.Class})
			step := PathStep{Step: e.Kind, Actor: e.Receiver, Attempt: number, At: e.At}
			if e.Kind == "terminal" || e.Kind == "rejected" {
				step.Detail = strings.ToLower(strings.TrimPrefix(string(e.State), "TASK_STATE_"))
				if e.Class != "" {
					step.Detail += " " + e.Class
				}
			}
			d.Path = append(d.Path, step)
		}
		if a.Decision != nil {
			attempt.Decision = &Decision{Custodian: a.Decision.Custodian, Previous: a.Decision.Previous, Reason: a.Decision.Reason, Served: a.Decision.Served, At: a.Decision.At}
			d.Path = append(d.Path, PathStep{Step: "claim decided", Actor: a.Decision.Custodian, Attempt: number, At: a.Decision.At, Detail: a.Decision.Reason})
		}
		if a.StartFailure != nil {
			attempt.StartFailure = &StartFailure{Profile: a.StartFailure.Profile, Agent: a.StartFailure.Agent, Class: a.StartFailure.Class, At: a.StartFailure.At}
			d.Path = append(d.Path, PathStep{Step: "start failed", Actor: a.Receiver, Attempt: number, At: a.StartFailure.At, Detail: StartFailureClass(a.StartFailure.Class)})
		}
		d.Attempts = append(d.Attempts, attempt)
	}
	for _, digest := range slices.Sorted(maps.Keys(r.Opens)) {
		o := r.Opens[digest]
		d.Opens = append(d.Opens, Open{Attempt: o.Attempt, Receiver: o.Receiver, At: o.At, Answer: o.Answer, Custodian: o.Custodian, Reason: o.Reason, AnsweredAt: o.AnsweredAt})
		d.Path = append(d.Path, PathStep{Step: "open requested", Actor: o.Receiver, Attempt: o.Attempt, At: o.At})
		if o.Answer != "" {
			d.Path = append(d.Path, PathStep{Step: "open " + o.Answer, Actor: o.Custodian, Attempt: o.Attempt, At: o.AnsweredAt, Detail: o.Reason})
		}
	}
	for _, c := range r.Cancels {
		d.Cancels = append(d.Cancels, Cancel{Host: c.Host, At: c.At})
		d.Path = append(d.Path, PathStep{Step: "cancelled", Actor: c.Host, At: c.At})
	}
	if f := r.Result; f != nil {
		result := &Result{Receiver: f.Receiver, Attempt: f.Attempt, State: string(f.State), At: f.At, Profile: f.Profile, Labels: f.Labels, Outputs: []Output{}, S3Outputs: []Output{}, NotReturned: []Unreturned{}, ReplySize: f.ReplySize, ReplyCut: f.ReplyCut, Artifacts: f.Artifacts}
		for _, o := range f.Outputs {
			result.Outputs = append(result.Outputs, Output(o))
		}
		for _, o := range f.S3Outputs {
			result.S3Outputs = append(result.S3Outputs, Output(o))
		}
		for _, o := range f.NotReturned {
			result.NotReturned = append(result.NotReturned, Unreturned(o))
		}
		d.Result = result
		d.Path = append(d.Path, PathStep{Step: "result", Actor: f.Receiver, Attempt: f.Attempt, At: f.At, Detail: strings.ToLower(strings.TrimPrefix(string(f.State), "TASK_STATE_"))})
	}
	slices.SortStableFunc(d.Path, func(a, b PathStep) int { return a.At.Compare(b.At) })
	return d, true
}
