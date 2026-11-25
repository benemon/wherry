package view

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"math"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Options configure a View.
type Options struct {
	// Window is how far back the rebuild reads; the default is 24 hours.
	Window time.Duration
	Caps   Caps
	// Tick separates live reads and Refresh the reads of advertisements, broker metadata and lag; the defaults are
	// 1.5 and 10 seconds.
	Tick, Refresh time.Duration
	Now           func() time.Time
	Logger        *log.Logger
}

// Default intervals.
const (
	DefaultWindow  = 24 * time.Hour
	DefaultTick    = 1500 * time.Millisecond
	DefaultRefresh = 10 * time.Second
)

// View is the operator network view over one cluster, read through client, the operator's. It never produces.
type View struct {
	reader *operator.Reader
	opts   Options

	mu          sync.RWMutex
	m           *model
	brokers     []brokerFact
	lag         map[string]lagFact
	groups      map[string]groupFact
	version     uint64
	subscribers map[chan uint64]struct{}
	windowStart time.Time
	rebuilt     bool
	rebuiltAt   time.Time
	lastRefresh time.Time
	errs        []string
	traces      *traces
	transcripts *transcripts
}

type brokerFact struct {
	ID         int32
	Host       string
	Port       int32
	Rack       string
	Controller bool
	Listeners  []string
}

// lagFact is a pool's share-group lag: records not yet acknowledged past the start offset, summed over its task
// topics' partitions. Status tells unavailable (no group or offsets described), error, zero and ok apart.
type lagFact struct {
	Status  string
	Records int64
	Error   string
	At      time.Time
}

type groupFact struct {
	State   string
	Members int
}

// Lag statuses.
const (
	LagOK          = "ok"
	LagZero        = "zero"
	LagUnavailable = "unavailable"
	LagError       = "error"
)

// New is a view reading through client with opts.
func New(client *kgo.Client, opts Options) *View {
	if opts.Window <= 0 {
		opts.Window = DefaultWindow
	}
	if opts.Tick <= 0 {
		opts.Tick = DefaultTick
	}
	if opts.Refresh <= 0 {
		opts.Refresh = DefaultRefresh
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Caps == (Caps{}) {
		opts.Caps = DefaultCaps
	}
	if opts.Logger == nil {
		opts.Logger = log.New(discard{}, "", 0)
	}
	v := &View{reader: operator.NewReader(client), opts: opts, m: newModel(opts.Caps), lag: map[string]lagFact{}, groups: map[string]groupFact{}, subscribers: map[chan uint64]struct{}{}}
	v.m.clock = opts.Now
	v.traces = newTraces(v)
	v.transcripts = newTranscripts(v)
	return v
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

// Close closes the view's reader.
func (v *View) Close() { v.reader.Close() }

// Version is the model's version, raised on every change.
func (v *View) Version() uint64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.version
}

// Subscribe returns a channel that receives each new version, and the func that ends the subscription. A subscriber
// that does not keep up misses versions, never blocks the view.
func (v *View) Subscribe() (<-chan uint64, func()) {
	ch := make(chan uint64, 1)
	v.mu.Lock()
	v.subscribers[ch] = struct{}{}
	v.mu.Unlock()
	return ch, func() {
		v.mu.Lock()
		delete(v.subscribers, ch)
		v.mu.Unlock()
	}
}

// bump raises the version and tells subscribers; the caller holds mu.
func (v *View) bump() {
	v.version++
	for ch := range v.subscribers {
		select {
		case ch <- v.version:
		default:
			// A pending version is replaced by the newer one.
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- v.version:
			default:
			}
		}
	}
}

// Rebuild builds the model from the window: operator state and keys, then every pool's events and tenant's feed from
// the window's start in bounded batches until each reaches the end it listed, with advertisements, custodian
// snapshots, progress, broker metadata and lag. Run then tails from those ends.
func (v *View) Rebuild(ctx context.Context) error {
	v.mu.Lock()
	v.windowStart = v.opts.Now().Add(-v.opts.Window)
	v.mu.Unlock()
	if err := v.tick(ctx, true); err != nil {
		return err
	}
	v.mu.Lock()
	v.rebuilt, v.rebuiltAt = true, v.opts.Now()
	v.bump()
	v.mu.Unlock()
	return nil
}

// Run tails the topics every tick until ctx ends; a failed tick is logged and the next resumes from the same cursors.
func (v *View) Run(ctx context.Context) error {
	ticker := time.NewTicker(v.opts.Tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		if err := v.tick(ctx, false); err != nil && ctx.Err() == nil {
			// The error's text can quote a record; the log and the model get its class alone.
			class := errClass(err)
			v.opts.Logger.Printf("view: tick failed: %s", class)
			v.mu.Lock()
			v.errs = append(v.errs[max(len(v.errs)-9, 0):], v.opts.Now().UTC().Format(time.RFC3339)+" "+class)
			v.mu.Unlock()
		}
	}
}

// errClass is a fixed word for err: a Kafka error's name, else the context's, else "error". Error text can quote a
// record, so none reaches the model or the log.
func errClass(err error) string {
	var kafka *kerr.Error
	switch {
	case errors.As(err, &kafka):
		if named, ok := kerr.ErrorForCode(kafka.Code).(*kerr.Error); ok && named.Message == kafka.Message {
			return kafka.Message
		}
		return "kafka error"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	}
	return "error"
}

// topicRef names a topic the view follows and what it holds.
type topicRef struct {
	kind, name string
}

// followed is each topic the state calls for, by name: the operator topic, every pool's events and custodian topics,
// every tenant's feed and every receiver's progress topic, ended ones included while their topics exist.
func followed(state operator.State) map[string]topicRef {
	topics := map[string]topicRef{}
	for pool := range state.Pools {
		topics[operator.PoolEventsTopic(pool)] = topicRef{"events", pool}
		topics[operator.CustodianTopic(pool)] = topicRef{"custodian", pool}
	}
	for tenant := range state.Tenants {
		topics[operator.TenantTopic(tenant)] = topicRef{"feed", tenant}
	}
	for receiver := range state.Receivers {
		topics[operator.ReceiverProgressTopic(receiver)] = topicRef{"progress", receiver}
	}
	return topics
}

// tick is one pass: the operator topic from its cursor, the topics listed, then before each data batch the key
// topics' new records, and the data topics in one bounded call, or with drain in as many as reach every end; then
// the pending records, and every refresh interval the advertisements, custodians, broker metadata and lag.
func (v *View) tick(ctx context.Context, drain bool) error {
	admin, err := v.reader.Admin()
	if err != nil {
		return err
	}
	if err := v.readOperator(ctx); err != nil {
		return fmt.Errorf("read %s: %w", operator.OperatorTopic, err)
	}
	existing, err := listTopics(ctx, admin)
	if err != nil {
		return fmt.Errorf("list topics: %w", err)
	}
	for {
		if err := v.readKeys(ctx, existing); err != nil {
			return fmt.Errorf("read keys: %w", err)
		}
		more, err := v.readData(ctx, admin, existing)
		if err != nil {
			return err
		}
		if !more || !drain {
			break
		}
	}
	now := v.opts.Now()
	v.mu.Lock()
	v.m.revalidate(now)
	refresh := v.lastRefresh.IsZero() || now.Sub(v.lastRefresh) >= v.opts.Refresh
	v.mu.Unlock()
	if refresh {
		if err := v.refresh(ctx, admin, existing, now); err != nil {
			return err
		}
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.m.changed {
		v.m.changed = false
		v.bump()
	}
	return nil
}

// listTopics is every topic the principal may describe, from the broker rather than kadm's cache.
func listTopics(ctx context.Context, admin *kadm.Client) (map[string]bool, error) {
	listed, err := admin.ListTopics(kadm.WithAuthorizedOps(ctx))
	if err != nil {
		return nil, err
	}
	existing := map[string]bool{}
	for topic, detail := range listed {
		if detail.Err == nil {
			existing[topic] = true
		}
	}
	return existing, nil
}

// readOperator extends the history from its cursor to the end it lists, in bounded batches, before any data topic
// is read; a member it adds has its key topic tailed from the next batch on.
func (v *View) readOperator(ctx context.Context) error {
	for {
		v.mu.RLock()
		next, caps := v.m.history.Next, v.m.caps
		v.mu.RUnlock()
		ranges, err := v.reader.ReadRanges(ctx, map[string]int64{operator.OperatorTopic: next}, operator.Bounds{Records: caps.BatchRecords, Bytes: caps.BatchBytes})
		if err != nil {
			return err
		}
		r := ranges[operator.OperatorTopic]
		if r.Err != nil {
			return r.Err
		}
		v.mu.Lock()
		if len(r.Records) == 0 && v.m.history.Next == r.Next && v.m.history.Terms != nil {
			v.mu.Unlock()
			return nil
		}
		if err := v.m.history.Extend(r.Records, r.Next); err != nil {
			v.mu.Unlock()
			return err
		}
		v.m.names = newCatalog(v.m.history)
		if len(r.Records) > 0 {
			v.m.changed = true
		}
		v.m.topics[operator.OperatorTopic] = &topicState{Kind: "operator", Next: r.Next, Start: r.Start, End: r.End, started: true}
		v.mu.Unlock()
		if r.Next >= r.End {
			return nil
		}
	}
}

// readKeys tails every receiver's and host's key topic that exists, each from the offset its last tail reached, in
// bounded batches to the end it lists, and folds the new records into the keys. A rotation seen for the first time
// retires a key from its append time: the facts folded from records signed with it after that are retracted. The
// principal each key owner labels its facts with is rebuilt from the state here, with the owners walked.
func (v *View) readKeys(ctx context.Context, existing map[string]bool) error {
	v.mu.RLock()
	state, caps := v.m.history.State, v.m.caps
	owners, offsets := map[string]string{}, map[string]int64{}
	for owner, topic := range operator.AllKeyTopics(state) {
		if existing[topic] {
			owners[topic], offsets[topic] = owner, v.m.keyNext[topic]
		}
	}
	v.mu.RUnlock()
	signers := map[string]string{}
	for receiver := range state.Receivers {
		signers[receiver] = operator.ReceiverPrincipal(receiver)
	}
	for principal, host := range state.Hosts {
		signers[signed.HostOwner(host.Tenant, host.Name)] = principal
	}
	v.mu.Lock()
	v.m.signers = signers
	v.mu.Unlock()
	for len(offsets) > 0 {
		ranges, err := v.reader.ReadRanges(ctx, offsets, operator.Bounds{Records: caps.BatchRecords, Bytes: caps.BatchBytes})
		if err != nil {
			return err
		}
		v.mu.Lock()
		if v.m.keys == nil {
			v.m.keys = signed.Keys{}
		}
		for topic, r := range ranges {
			if r.Err != nil {
				delete(offsets, topic)
				continue
			}
			owner := owners[topic]
			before := v.m.keys[owner]
			after := before
			for _, record := range r.Records {
				// ApplyKeyRecord returns the prior state with its error.
				after, _ = signed.ApplyKeyRecord(after, owner, record.Value, record.Timestamp)
			}
			for _, retired := range after.Retired[min(len(before.Retired), len(after.Retired)):] {
				v.m.retire(owner, signed.KeyID(retired.Key), retired.At)
				// An index holds offsets, not sources: the runs holding the owner's chunks are indexed again on request.
				if dropped := v.transcripts.retire(owner); dropped > 0 {
					v.m.count("transcript_retracted", int64(dropped))
				}
			}
			if !after.Current.Equal(before.Current) || len(after.Retired) != len(before.Retired) {
				v.m.changed = true
			}
			v.m.keys[owner], v.m.keyNext[topic] = after, r.Next
			if r.Next < r.End {
				offsets[topic] = r.Next
			} else {
				delete(offsets, topic)
			}
		}
		v.mu.Unlock()
	}
	v.mu.Lock()
	v.m.keysRead++
	v.mu.Unlock()
	return nil
}

// readData reads every followed topic that exists from its cursor in one bounded call and folds the records, and
// reports whether a topic stopped short of the end it listed. A topic first followed starts at the window's start
// by time, or at its log start for a compacted one.
func (v *View) readData(ctx context.Context, admin *kadm.Client, existing map[string]bool) (bool, error) {
	v.mu.RLock()
	refs := followed(v.m.history.State)
	offsets, byTime := map[string]int64{}, []string{}
	for topic, ref := range refs {
		state := v.m.topics[topic]
		if !existing[topic] {
			continue
		}
		if state == nil || !state.started {
			if ref.kind == "events" || ref.kind == "feed" {
				byTime = append(byTime, topic)
				continue
			}
			offsets[topic] = 0
			continue
		}
		offsets[topic] = state.Next
	}
	caps, windowStart := v.m.caps, v.windowStart
	v.mu.RUnlock()
	if len(offsets)+len(byTime) > caps.Cursors {
		v.mu.Lock()
		v.m.count(capCursors, int64(len(offsets)+len(byTime)-caps.Cursors))
		v.mu.Unlock()
		for len(offsets)+len(byTime) > caps.Cursors {
			if len(byTime) > 0 {
				byTime = byTime[:len(byTime)-1]
			} else {
				for topic := range offsets {
					delete(offsets, topic)
					break
				}
			}
		}
	}
	starts := map[string]int64{}
	if len(byTime) > 0 {
		listed, err := admin.ListOffsetsAfterMilli(ctx, windowStart.UnixMilli(), byTime...)
		if err != nil {
			return false, fmt.Errorf("list offsets by time: %w", err)
		}
		for _, topic := range byTime {
			start, ok := listed.Lookup(topic, 0)
			switch {
			case !ok || start.Err != nil:
				// Listed again next tick.
			case start.Offset < 0:
				// No record since the window's start: the read begins at the end it lists.
				offsets[topic], starts[topic] = math.MaxInt64, -1
			default:
				offsets[topic], starts[topic] = start.Offset, start.Offset
			}
		}
	}
	v.mu.Lock()
	for topic, ref := range refs {
		state := v.m.topics[topic]
		if state == nil {
			if len(v.m.topics) >= caps.Cursors {
				continue
			}
			state = &topicState{Kind: ref.kind}
			v.m.topics[topic] = state
		}
		state.Missing = !existing[topic]
	}
	v.mu.Unlock()
	if len(offsets) == 0 {
		return false, nil
	}
	ranges, err := v.reader.ReadRanges(ctx, offsets, operator.Bounds{Records: caps.BatchRecords, Bytes: caps.BatchBytes})
	if err != nil {
		return false, fmt.Errorf("read: %w", err)
	}
	// The batch's records, in each topic's order: digested and their headers read in parallel, admitted under the
	// lock, verified and decoded in parallel under the keys of the moment, then folded in order under the lock.
	type item struct {
		ref    topicRef
		record *kgo.Record
		p      *parsed
	}
	var items []item
	for _, topic := range slices.Sorted(maps.Keys(ranges)) {
		if r := ranges[topic]; r.Err == nil {
			for _, record := range r.Records {
				items = append(items, item{ref: refs[topic], record: record})
			}
		}
	}
	isSigned := func(i item) bool { return i.ref.kind == "events" || i.ref.kind == "feed" }
	parallel(len(items), func(i int) {
		if isSigned(items[i]) {
			items[i].p = prepare(items[i].record)
		}
	})
	v.mu.Lock()
	for _, i := range items {
		if i.p != nil {
			v.m.admit(i.p, i.record)
		}
	}
	keys := v.m.keys
	v.mu.Unlock()
	parallel(len(items), func(i int) {
		if items[i].p != nil {
			parseRecord(items[i].ref.kind, items[i].p, items[i].record, keys)
		}
	})
	now := v.opts.Now()
	more := false
	v.mu.Lock()
	defer v.mu.Unlock()
	for topic, r := range ranges {
		state := v.m.topics[topic]
		if state == nil {
			continue
		}
		if r.Err != nil {
			state.Err = errClass(r.Err)
			continue
		}
		if !state.started {
			state.started = true
			if from, listed := starts[topic]; listed && (from < 0 && r.End > 0 && r.Start >= r.End || from >= 0 && from <= r.Start) {
				// The earliest retained record is at or after the window's start: the window predates the log.
				state.PredatesLog = len(r.Records) == 0 || r.Records[0].Timestamp.After(windowStart)
			}
		}
		state.Next, state.Start, state.End, state.Err = r.Next, r.Start, r.End, ""
		if r.Next < r.End {
			more = true
		}
	}
	for _, i := range items {
		if reason := v.m.apply(i.ref.kind, i.ref.name, i.record, i.p); reason != "" {
			v.m.hold(i.ref.kind, i.ref.name, i.record, reason, now)
		}
	}
	return more, nil
}

// parallel runs fn over 0 to n-1 on as many goroutines as there are processors.
func parallel(n int, fn func(int)) {
	workers := min(runtime.GOMAXPROCS(0), n)
	if workers <= 1 {
		for i := range n {
			fn(i)
		}
		return
	}
	var wg sync.WaitGroup
	var next atomic.Int64
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= n {
					return
				}
				fn(i)
			}
		}()
	}
	wg.Wait()
}

// refresh reads the advertisements of every pool, the broker metadata and listeners and each pool's share-group
// state and lag.
func (v *View) refresh(ctx context.Context, admin *kadm.Client, existing map[string]bool, now time.Time) error {
	v.mu.RLock()
	state, keys := v.m.history.State, v.m.keys
	v.mu.RUnlock()
	var pools []string
	for pool := range state.Pools {
		if existing[operator.PoolReceiversTopic(pool)] {
			pools = append(pools, pool)
		}
	}
	slices.Sort(pools)
	ads, err := v.reader.ReadAdvertisements(ctx, state, keys, pools, now, math.MaxInt64)
	if err != nil {
		return err
	}
	brokers := v.readBrokers(ctx, admin)
	lag, groups := v.readLag(ctx, admin, slices.Sorted(maps.Keys(state.Pools)), existing, now)
	v.mu.Lock()
	defer v.mu.Unlock()
	if !reflect.DeepEqual(ads, v.m.ads) || !reflect.DeepEqual(brokers, v.brokers) || !reflect.DeepEqual(groups, v.groups) || lagChanged(lag, v.lag) {
		v.m.changed = true
	}
	v.m.ads, v.brokers, v.lag, v.groups, v.lastRefresh = ads, brokers, lag, groups, now
	return nil
}

func lagChanged(a, b map[string]lagFact) bool {
	if len(a) != len(b) {
		return true
	}
	for pool, lag := range a {
		if other, ok := b[pool]; !ok || other.Status != lag.Status || other.Records != lag.Records {
			return true
		}
	}
	return false
}

// readBrokers is the cluster's brokers with their listeners, as far as the principal may describe them; a failure
// leaves the last answer.
func (v *View) readBrokers(ctx context.Context, admin *kadm.Client) []brokerFact {
	metadata, err := admin.BrokerMetadata(ctx)
	if err != nil {
		v.mu.RLock()
		defer v.mu.RUnlock()
		return v.brokers
	}
	var brokers []brokerFact
	var ids []int32
	for _, broker := range metadata.Brokers {
		rack := ""
		if broker.Rack != nil {
			rack = *broker.Rack
		}
		brokers = append(brokers, brokerFact{ID: broker.NodeID, Host: plainText(broker.Host), Port: broker.Port, Rack: plainText(rack), Controller: broker.NodeID == metadata.Controller})
		ids = append(ids, broker.NodeID)
	}
	if configs, err := admin.DescribeBrokerConfigs(ctx, ids...); err == nil {
		for _, config := range configs {
			for i := range brokers {
				if fmt.Sprint(brokers[i].ID) != config.Name {
					continue
				}
				for _, entry := range config.Configs {
					if (entry.Key == "listeners" || entry.Key == "advertised.listeners") && entry.Value != nil && !entry.Sensitive {
						brokers[i].Listeners = append(brokers[i].Listeners, listeners(*entry.Value)...)
					}
				}
				slices.Sort(brokers[i].Listeners)
				brokers[i].Listeners = slices.Compact(brokers[i].Listeners)
			}
		}
	}
	return brokers
}

// listeners is value, a comma-separated listener list, each kept only when it is a listener's name, scheme and
// address of plain characters.
func listeners(value string) []string {
	var kept []string
	for _, listener := range strings.Split(value, ",") {
		listener = strings.TrimSpace(listener)
		if listener != "" && len(listener) <= 253 && !strings.ContainsFunc(listener, func(r rune) bool {
			return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune(".-_:/[]", r))
		}) {
			kept = append(kept, listener)
		}
	}
	return kept
}

// readLag is each pool's share-group state and lag: the group's start offset against its task topics' end offsets,
// unavailable, error and zero distinct.
func (v *View) readLag(ctx context.Context, admin *kadm.Client, pools []string, existing map[string]bool, now time.Time) (map[string]lagFact, map[string]groupFact) {
	lag, groups := map[string]lagFact{}, map[string]groupFact{}
	if len(pools) == 0 {
		return lag, groups
	}
	var ids []string
	for _, pool := range pools {
		ids = append(ids, operator.PoolGroup(pool))
	}
	described, describeErr := admin.DescribeShareGroups(ctx, ids...)
	offsets, offsetsErr := admin.DescribeShareGroupOffsets(ctx, ids...)
	for _, pool := range pools {
		group := operator.PoolGroup(pool)
		if describeErr == nil {
			if d, ok := described[group]; ok && d.Err == nil {
				groups[pool] = groupFact{State: fixed(groupStates, d.GroupState), Members: len(d.Members)}
			}
		}
		var topics []string
		for topic := range existing {
			if strings.HasPrefix(topic, operator.PoolTasksPrefix(pool)) {
				topics = append(topics, topic)
			}
		}
		slices.Sort(topics)
		fact := lagFact{Status: LagUnavailable, At: now}
		switch {
		case offsetsErr != nil:
			fact.Status, fact.Error = LagError, errClass(offsetsErr)
		case len(topics) == 0:
		default:
			d, ok := offsets[group]
			if !ok || d.Err != nil && errors.Is(d.Err, kerr.GroupIDNotFound) {
				break
			}
			if d.Err != nil {
				fact.Status, fact.Error = LagError, errClass(d.Err)
				break
			}
			ends, err := admin.ListEndOffsets(ctx, topics...)
			if err != nil {
				fact.Status, fact.Error = LagError, errClass(err)
				break
			}
			var total int64
			counted := false
			for _, topic := range topics {
				for partition, end := range ends[topic] {
					start, ok := d.Offsets.Lookup(topic, partition)
					if end.Err != nil || !ok || start.Err != nil || start.StartOffset < 0 {
						continue
					}
					total += max(end.Offset-start.StartOffset, 0)
					counted = true
				}
			}
			if counted {
				fact.Status, fact.Records = LagOK, total
				if total == 0 {
					fact.Status = LagZero
				}
			}
		}
		lag[pool] = fact
	}
	return lag, groups
}
