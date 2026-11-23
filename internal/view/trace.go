package view

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/telemetry"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"go.opentelemetry.io/otel/attribute"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// The drill-down's caps, as reported.
const (
	capTraceBytes   = "trace_bytes"
	capTraceRecords = "trace_records"
	capTraceSpans   = "trace_spans"
	capTraceForest  = "trace_forest"
	capTraceScan    = "trace_scan"
	capTraceScans   = "trace_scans"
	capTraceRuns    = "trace_runs"
)

// traceSkew is how far before a run's first record its tenant's trace topic is scanned from: a host's submit spans
// are exported after the task record, on another leader's clock.
const traceSkew = time.Minute

// Markers a forest carries for what was cut.
const (
	MarkerOversized     = "oversized record skipped"
	MarkerMalformed     = "malformed record skipped"
	MarkerCycle         = "cycle cut"
	MarkerMissingParent = "missing parent"
	MarkerNoTopic       = "no trace topic"
	MarkerFiltered      = "attribute filtered"
	markerTruncated     = "truncated: "
)

// Forest is a run's spans as trees, one per trace ID the run's spans carry, with timing alone: span facts are
// advisory, since any host of the tenant can write the topic.
type Forest struct {
	Run     string   `json:"run"`
	Tenant  string   `json:"tenant"`
	Topic   string   `json:"topic"`
	Traces  []Trace  `json:"traces"`
	Spans   int      `json:"spans"`
	Markers []string `json:"markers"`
	// Read is what this refresh read; Next the offset the next refresh resumes at; Complete whether the scan reached
	// the end it listed.
	Read     Read  `json:"read"`
	Next     int64 `json:"next"`
	Complete bool  `json:"complete"`
}

type Read struct {
	Bytes   int64 `json:"bytes"`
	Records int64 `json:"records"`
}

type Trace struct {
	ID    string     `json:"id"`
	Roots []SpanNode `json:"roots"`
}

type SpanNode struct {
	ID         string            `json:"id"`
	Parent     string            `json:"parent,omitzero"`
	Name       string            `json:"name"`
	Kind       string            `json:"kind"`
	Service    string            `json:"service,omitzero"`
	Start      time.Time         `json:"start"`
	End        time.Time         `json:"end"`
	DurationMS float64           `json:"duration_ms"`
	Status     string            `json:"status,omitzero"`
	Attributes map[string]string `json:"attributes"`
	Links      []string          `json:"links,omitzero"`
	Events     int               `json:"events,omitzero"`
	Children   []SpanNode        `json:"children"`
	Marker     string            `json:"marker,omitzero"`
}

// traces keeps each scanned run's state, so a refresh resumes at the last end rather than scanning again.
type traces struct {
	v       *View
	mu      sync.Mutex
	scans   map[string]*scan
	running chan struct{}
}

type scan struct {
	run, tenant, topic string
	next               int64
	started            bool
	spans              map[string]*spanFact
	markers            map[string]bool
	used               time.Time
}

type spanFact struct {
	trace, id, parent string
	name, kind        string
	service           string
	start, end        time.Time
	status            string
	attributes        map[string]string
	links             []string
	events            int
	filtered          bool
}

func newTraces(v *View) *traces {
	return &traces{v: v, scans: map[string]*scan{}, running: make(chan struct{}, max(v.opts.Caps.TraceScans, 1))}
}

var errBusy = errors.New("too many drill-down scans running")

// Trace scans run id's tenant's trace topic from the run's start on the first call and from the last end on the next,
// within the caps, and returns the forest of the spans whose wherry.run is the run. A record is tested for the run ID
// as bytes before it is decoded.
func (t *traces) Trace(ctx context.Context, id string) (Forest, error) {
	v := t.v
	v.mu.RLock()
	r, ok := v.m.runs[id]
	var tenant string
	var from time.Time
	if ok {
		tenant, from = r.Tenant, r.First
		if admitted := r.Admitted(); !admitted.IsZero() && admitted.Before(from) {
			from = admitted
		}
	}
	caps := v.m.caps
	v.mu.RUnlock()
	if !ok || tenant == "" {
		return Forest{}, errNotFound
	}
	select {
	case t.running <- struct{}{}:
		defer func() { <-t.running }()
	default:
		v.mu.Lock()
		v.m.count(capTraceScans, 1)
		v.mu.Unlock()
		return Forest{}, errBusy
	}
	s := t.scanFor(id, tenant, caps)
	admin, err := v.reader.Admin()
	if err != nil {
		return Forest{}, err
	}
	topics, err := listTopics(ctx, admin)
	if err != nil {
		return Forest{}, err
	}
	forest := Forest{Run: id, Tenant: tenant, Topic: s.topic, Traces: []Trace{}, Markers: []string{}}
	if !topics[s.topic] {
		forest.Markers = append(forest.Markers, MarkerNoTopic)
		return forest, nil
	}
	if !s.started {
		listed, err := operator.ListOffsets(ctx, func(ctx context.Context, topics ...string) (kadm.ListedOffsets, error) {
			return admin.ListOffsetsAfterMilli(ctx, from.Add(-traceSkew).UnixMilli(), topics...)
		}, s.topic)
		if err != nil {
			return Forest{}, err
		}
		s.next, s.started = math.MaxInt64, true
		if start, ok := listed.Lookup(s.topic, 0); ok && start.Offset >= 0 {
			s.next = start.Offset
		}
	}
	deadline := v.opts.Now().Add(caps.TraceScan)
	var read Read
	complete := false
	for {
		remaining := caps.TraceRecords - int(read.Records)
		if remaining <= 0 {
			t.cut(s, capTraceRecords)
			break
		}
		if caps.TraceBytes-read.Bytes <= 0 {
			t.cut(s, capTraceBytes)
			break
		}
		if v.opts.Now().After(deadline) {
			t.cut(s, capTraceScan)
			break
		}
		ranges, err := v.reader.ReadRanges(ctx, map[string]int64{s.topic: s.next}, operator.Bounds{Records: min(remaining, caps.BatchRecords), Bytes: min(caps.TraceBytes-read.Bytes, caps.BatchBytes)})
		if err != nil {
			return Forest{}, err
		}
		rng := ranges[s.topic]
		if rng.Err != nil {
			return Forest{}, rng.Err
		}
		for _, record := range rng.Records {
			read.Records++
			read.Bytes += int64(len(record.Value))
			t.decode(s, id, record.Value, caps)
		}
		if s.next == math.MaxInt64 && rng.Next == rng.End || rng.Next <= s.next && len(rng.Records) == 0 {
			s.next = rng.End
			complete = true
			break
		}
		s.next = rng.Next
		if rng.Next >= rng.End {
			complete = true
			break
		}
	}
	forest.Read, forest.Next, forest.Complete = read, s.next, complete
	t.mu.Lock()
	markers := slices.Sorted(maps.Keys(s.markers))
	spans := slices.Collect(maps.Values(s.spans))
	t.mu.Unlock()
	forest.Markers = append(forest.Markers, markers...)
	var nodes int
	forest.Traces, nodes = t.forest(spans, caps)
	forest.Spans = len(spans)
	placed, marked := spansIn(forest.Traces)
	for _, marker := range slices.Sorted(maps.Keys(marked)) {
		if !slices.Contains(forest.Markers, marker) {
			forest.Markers = append(forest.Markers, marker)
		}
	}
	if len(spans) > len(placed) || nodes >= caps.TraceForest {
		forest.Markers = append(forest.Markers, markerTruncated+capTraceForest)
	}
	return forest, nil
}

// spansIn is the distinct span IDs placed in traces and the markers their nodes carry.
func spansIn(traces []Trace) (map[string]bool, map[string]bool) {
	ids, markers := map[string]bool{}, map[string]bool{}
	var walk func(nodes []SpanNode)
	walk = func(nodes []SpanNode) {
		for _, node := range nodes {
			ids[node.ID] = true
			if node.Marker != "" {
				markers[node.Marker] = true
			}
			walk(node.Children)
		}
	}
	for _, trace := range traces {
		walk(trace.Roots)
	}
	return ids, markers
}

// cut marks the scan truncated by cap and counts the hit.
func (t *traces) cut(s *scan, cap string) {
	t.mu.Lock()
	s.markers[markerTruncated+cap] = true
	t.mu.Unlock()
	t.v.mu.Lock()
	t.v.m.count(cap, 1)
	t.v.mu.Unlock()
}

// scanFor is the scan state of run id, made on first sight within the cap on runs kept, the least recently used
// dropped beyond it.
func (t *traces) scanFor(id, tenant string, caps Caps) *scan {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.scans[id]
	if s == nil {
		s = &scan{run: id, tenant: tenant, topic: telemetry.TraceTopic(tenant), spans: map[string]*spanFact{}, markers: map[string]bool{}}
		t.scans[id] = s
		for len(t.scans) > max(caps.TraceRuns, 1) {
			oldest := ""
			for other, state := range t.scans {
				if other != id && (oldest == "" || state.used.Before(t.scans[oldest].used)) {
					oldest = other
				}
			}
			delete(t.scans, oldest)
			t.v.mu.Lock()
			t.v.m.count(capTraceRuns, 1)
			t.v.mu.Unlock()
		}
	}
	s.used = t.v.opts.Now()
	return s
}

// decode keeps the spans of value, one trace record, whose wherry.run is id: a record over the producer's limit is
// skipped before decoding, one not holding the run ID as bytes is skipped before decoding, a malformed one is marked.
func (t *traces) decode(s *scan, id string, value []byte, caps Caps) {
	if len(value) > telemetry.TraceRecordLimit {
		t.mark(s, MarkerOversized)
		return
	}
	if !bytes.Contains(value, []byte(id)) {
		return
	}
	var request coltracepb.ExportTraceServiceRequest
	if err := proto.Unmarshal(value, &request); err != nil {
		t.mark(s, MarkerMalformed)
		return
	}
	t.v.mu.RLock()
	allow := t.allowlist(id)
	t.v.mu.RUnlock()
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, rs := range request.ResourceSpans {
		service := allow.service(rs.Resource.GetAttributes())
		for _, ss := range rs.ScopeSpans {
			for _, span := range ss.Spans {
				if runOf(span.Attributes) != id {
					continue
				}
				if len(s.spans) >= caps.TraceSpans {
					s.markers[markerTruncated+capTraceSpans] = true
					t.v.mu.Lock()
					t.v.m.count(capTraceSpans, 1)
					t.v.mu.Unlock()
					return
				}
				fact := &spanFact{trace: hex.EncodeToString(span.TraceId), id: hex.EncodeToString(span.SpanId), parent: hex.EncodeToString(span.ParentSpanId),
					name: allow.name(span.Name), kind: kindName(span.Kind), service: service,
					start: time.Unix(0, int64(min(span.StartTimeUnixNano, math.MaxInt64))).UTC(), end: time.Unix(0, int64(min(span.EndTimeUnixNano, math.MaxInt64))).UTC(),
					status: allow.status(span.Status), events: len(span.Events)}
				fact.attributes, fact.filtered = allow.attributes(span.Attributes)
				for _, link := range span.Links {
					if linked := hex.EncodeToString(link.TraceId); len(linked) == 32 {
						fact.links = append(fact.links, linked)
					}
				}
				if fact.filtered {
					s.markers[MarkerFiltered] = true
				}
				s.spans[fact.trace+"/"+fact.id] = fact
			}
		}
	}
}

func (t *traces) mark(s *scan, marker string) {
	t.mu.Lock()
	s.markers[marker] = true
	t.mu.Unlock()
}

func runOf(attrs []*commonpb.KeyValue) string {
	for _, attr := range attrs {
		if attr.Key == string(telemetry.AttrRun) {
			return attr.GetValue().GetStringValue()
		}
	}
	return ""
}

var kindNames = map[tracepb.Span_SpanKind]string{
	tracepb.Span_SPAN_KIND_INTERNAL: "internal", tracepb.Span_SPAN_KIND_SERVER: "server", tracepb.Span_SPAN_KIND_CLIENT: "client",
	tracepb.Span_SPAN_KIND_PRODUCER: "producer", tracepb.Span_SPAN_KIND_CONSUMER: "consumer",
}

func kindName(kind tracepb.Span_SpanKind) string {
	if name, ok := kindNames[kind]; ok {
		return name
	}
	return "unspecified"
}

// forest builds the trees of spans: a span whose parent is absent roots a tree marked so, one reached twice is cut
// as a cycle, and the trees stop at the forest cap.
func (t *traces) forest(spans []*spanFact, caps Caps) ([]Trace, int) {
	byTrace := map[string][]*spanFact{}
	for _, span := range spans {
		byTrace[span.trace] = append(byTrace[span.trace], span)
	}
	var traces []Trace
	nodes := 0
	cut := false
	for _, id := range slices.Sorted(maps.Keys(byTrace)) {
		members := byTrace[id]
		slices.SortFunc(members, func(a, b *spanFact) int { return a.start.Compare(b.start) })
		byID := map[string]*spanFact{}
		children := map[string][]*spanFact{}
		for _, span := range members {
			byID[span.id] = span
		}
		var roots []*spanFact
		for _, span := range members {
			if span.parent == "" || byID[span.parent] == nil {
				roots = append(roots, span)
				continue
			}
			children[span.parent] = append(children[span.parent], span)
		}
		visited := map[string]bool{}
		var build func(span *spanFact, depth int) SpanNode
		build = func(span *spanFact, depth int) SpanNode {
			node := SpanNode{ID: span.id, Parent: span.parent, Name: span.name, Kind: span.kind, Service: span.service, Start: span.start, End: span.end,
				DurationMS: float64(span.end.Sub(span.start)) / float64(time.Millisecond), Status: span.status, Attributes: span.attributes, Links: span.links, Events: span.events, Children: []SpanNode{}}
			if span.parent != "" && byID[span.parent] == nil {
				node.Marker = MarkerMissingParent
			}
			nodes++
			if visited[span.id] || depth > 64 {
				node.Marker = MarkerCycle
				return node
			}
			visited[span.id] = true
			for _, child := range children[span.id] {
				if nodes >= caps.TraceForest {
					cut = true
					break
				}
				if visited[child.id] {
					nodes++
					node.Children = append(node.Children, SpanNode{ID: child.id, Parent: child.parent, Name: child.name, Kind: child.kind, Marker: MarkerCycle, Attributes: map[string]string{}, Children: []SpanNode{}})
					continue
				}
				node.Children = append(node.Children, build(child, depth+1))
			}
			return node
		}
		trace := Trace{ID: id, Roots: []SpanNode{}}
		for _, root := range roots {
			if nodes >= caps.TraceForest {
				cut = true
				break
			}
			if visited[root.id] {
				continue
			}
			trace.Roots = append(trace.Roots, build(root, 0))
		}
		// A cycle with no root: every member has a parent in the trace. Its earliest span starts the tree.
		for _, span := range members {
			if nodes >= caps.TraceForest {
				cut = true
				break
			}
			if !visited[span.id] {
				node := build(span, 0)
				node.Marker = MarkerCycle
				trace.Roots = append(trace.Roots, node)
			}
		}
		traces = append(traces, trace)
	}
	if cut {
		t.v.mu.Lock()
		t.v.m.count(capTraceForest, 1)
		t.v.mu.Unlock()
	}
	if traces == nil {
		traces = []Trace{}
	}
	return traces, nodes
}

// allowlist is what a span of the selected run may name: the fixed span names, the attribute keys of the tracing
// allowlist, and for each key the values the model knows. Anything else is filtered: a span from any host of the
// tenant can claim any text.
type allowlist struct {
	run                                  string
	state                                operator.State
	topics                               map[string]bool
	outputs, credentials, models, agents map[string]bool
	endpoints, buckets                   map[string]bool
}

// allowlist is the allowlist for run id; the caller holds v.mu.
func (t *traces) allowlist(id string) allowlist {
	m := t.v.m
	a := allowlist{run: id, state: m.history.State, topics: map[string]bool{}, outputs: map[string]bool{}, credentials: map[string]bool{}, models: map[string]bool{}, agents: map[string]bool{}, endpoints: map[string]bool{}, buckets: map[string]bool{}}
	for topic := range followed(m.history.State) {
		a.topics[topic] = true
	}
	a.topics[operator.OperatorTopic] = true
	for pool, terms := range m.history.State.Pools {
		a.topics[operator.PoolReceiversTopic(pool)] = true
		for _, entry := range m.history.Terms[pool] {
			if entry.Terms.Storage != nil {
				a.endpoints[entry.Terms.Storage.Endpoint], a.buckets[entry.Terms.Storage.Bucket] = true, true
			}
		}
		for _, agent := range terms.Agents {
			a.agents[agent.Name] = true
			for _, credential := range agent.Credentials {
				a.credentials[credential.Name] = true
			}
			for _, model := range agent.Models {
				a.models[model] = true
			}
		}
	}
	for tenant := range m.history.State.Tenants {
		a.topics[telemetry.TraceTopic(tenant)] = true
		a.topics[operator.TenantTranscriptTopic(tenant)] = true
	}
	a.topics[telemetry.TraceTopic("")] = true
	for _, host := range m.history.State.Hosts {
		a.topics[operator.HostTopic(host.Tenant, host.Name)] = true
	}
	for receiver := range m.history.State.Receivers {
		a.topics[operator.ReceiverTopic(receiver)] = true
	}
	if r := m.runs[id]; r != nil && r.Result != nil {
		for _, list := range [][]outputFact{r.Result.Outputs, r.Result.S3Outputs} {
			for _, output := range list {
				a.outputs[output.Name] = true
			}
		}
		for _, output := range r.Result.NotReturned {
			a.outputs[output.Name] = true
		}
	}
	return a
}

// spanNames are the fixed span names; the prefixes take a tail checked by its kind.
var spanNames = map[string]bool{
	"submit": true, "read host key": true, "read state": true, "read advertisements": true, "read pool key": true,
	"resolve credential": true, "seal envelope": true, "upload inputs": true, "admit": true, "run": true,
	"deliver credentials": true, "await start": true, "find job": true, "return task": true, "released": true,
	"upload output": true, "receiver start": true, "custodian rotation": true, "plugin Dispatch": true, "plugin Lookup": true,
	"plugin Follow": true, "plugin Stop": true,
}

const nameOther = "[other]"

func (a allowlist) name(name string) string {
	if spanNames[name] {
		return name
	}
	if topic, ok := strings.CutPrefix(name, "send "); ok && a.topic(topic) {
		return name
	}
	if rest, ok := strings.CutPrefix(name, "process "); ok {
		topic, profile, _ := strings.Cut(rest, " ")
		if a.topic(topic) && (profile == "" || wire.ValidName(profile)) {
			return name
		}
	}
	if command, ok := strings.CutPrefix(name, "operator "); ok && !strings.ContainsFunc(command, func(r rune) bool { return !(r >= 'a' && r <= 'z' || r == ' ' || r == '-') }) && len(command) <= 32 {
		return name
	}
	if step, ok := strings.CutPrefix(name, "revocation step "); ok {
		if n, err := strconv.Atoi(step); err == nil && n > 0 && n < 10 {
			return name
		}
	}
	return nameOther
}

// topic reports whether name is a topic the model knows: a followed topic, a key, receivers, transcript or trace
// topic, or a task topic of a known pool.
func (a allowlist) topic(name string) bool {
	if a.topics[name] {
		return true
	}
	pool, _, ok := wire.ParseTaskTopic(name)
	_, known := a.state.Pools[pool]
	return ok && known
}

// attributes is attrs under the allowlist: a key outside it, or a value the model does not know for its key, is
// dropped; filtered reports whether any was.
func (a allowlist) attributes(attrs []*commonpb.KeyValue) (map[string]string, bool) {
	kept := map[string]string{}
	filtered := false
	for _, attr := range attrs {
		value, ok := a.value(attribute.Key(attr.Key), attr.GetValue())
		if !ok {
			filtered = true
			continue
		}
		kept[attr.Key] = value
	}
	return kept, filtered
}

func (a allowlist) value(key attribute.Key, value *commonpb.AnyValue) (string, bool) {
	if !telemetry.Allowed(key) {
		return "", false
	}
	switch v := value.GetValue().(type) {
	case *commonpb.AnyValue_IntValue:
		switch key {
		case telemetry.AttrAttempt, telemetry.AttrRetry, telemetry.AttrCount, telemetry.AttrSize, telemetry.AttrChunks, "messaging.kafka.offset":
			return strconv.FormatInt(v.IntValue, 10), v.IntValue >= 0
		}
		return "", false
	case *commonpb.AnyValue_StringValue:
		s := v.StringValue
		var ok bool
		switch key {
		case telemetry.AttrRun:
			ok = s == a.run
		case telemetry.AttrTenant:
			_, ok = a.state.Tenants[s]
		case telemetry.AttrHost:
			ok = slices.ContainsFunc(slices.Collect(maps.Values(a.state.Hosts)), func(host operator.Host) bool { return host.Name == s })
		case telemetry.AttrPool:
			_, ok = a.state.Pools[s]
		case telemetry.AttrReceiver:
			_, ok = a.state.Receivers[s]
		case telemetry.AttrProfile:
			for _, config := range a.state.Receivers {
				ok = ok || slices.ContainsFunc(config.Profiles, func(profile operator.ProfileSettings) bool { return profile.Name == s })
			}
		case telemetry.AttrAgent:
			ok = a.agents[s]
		case telemetry.AttrModel:
			ok = a.models[s]
		case telemetry.AttrCredential:
			ok = a.credentials[s]
		case telemetry.AttrCredentialSource:
			ok = s == "file" || s == "env" || s == "command"
		case telemetry.AttrOutput:
			ok = a.outputs[s]
		case telemetry.AttrStorageEndpoint:
			ok = a.endpoints[s]
		case telemetry.AttrStorageBucket:
			ok = a.buckets[s]
		case telemetry.AttrSystemName:
			ok = s == "kafka"
		case "messaging.operation.type", "messaging.operation.name":
			ok = s == "send" || s == "process" || s == "receive" || s == "create" || s == "settle"
		case "messaging.destination.name":
			ok = a.topic(s)
		case "messaging.consumer.group.name":
			for pool := range a.state.Pools {
				ok = ok || s == operator.PoolGroup(pool) || s == operator.CustodianGroup(pool)
			}
		}
		return s, ok
	}
	return "", false
}

// service is the resource's service.name when it is one of the fixed forms naming what the model knows.
func (a allowlist) service(attrs []*commonpb.KeyValue) string {
	for _, attr := range attrs {
		if attr.Key != "service.name" {
			continue
		}
		name := attr.GetValue().GetStringValue()
		switch {
		case name == "wherry-operator":
			return name
		case strings.HasPrefix(name, "wherry-host."):
			for _, host := range a.state.Hosts {
				if name == "wherry-host."+host.Tenant+"."+host.Name {
					return name
				}
			}
		case strings.HasPrefix(name, "wherry-receiver."):
			for receiver, config := range a.state.Receivers {
				if name == "wherry-receiver."+config.Pool+"."+receiver {
					return name
				}
			}
		case strings.HasPrefix(name, "wherry-custodian."):
			if _, ok := a.state.Pools[strings.TrimPrefix(name, "wherry-custodian.")]; ok {
				return name
			}
		}
		return nameOther
	}
	return ""
}

// status is the span's status: an error's class when it is one of telemetry's or a task state's, else "[other]";
// empty when unset or ok.
func (a allowlist) status(status *tracepb.Status) string {
	switch {
	case status == nil || status.Code == tracepb.Status_STATUS_CODE_UNSET:
		return ""
	case status.Code == tracepb.Status_STATUS_CODE_OK:
		return "ok"
	}
	class := status.Message
	if slices.Contains(telemetry.Classes, class) {
		return "error: " + class
	}
	for _, state := range []wire.TaskState{wire.TaskStateCompleted, wire.TaskStateFailed, wire.TaskStateCanceled, wire.TaskStateRejected, wire.TaskStateWorking, wire.TaskStateSubmitted} {
		if class == telemetry.StateClass(string(state)) {
			return "error: " + class
		}
	}
	return "error: " + nameOther
}

var errNotFound = errors.New("not found")

// String is for logs: the forest's size, never its content.
func (f Forest) String() string {
	return fmt.Sprintf("forest of %d spans in %d traces", f.Spans, len(f.Traces))
}
