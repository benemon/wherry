package receiver

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/benemon/wherry/adapter"
	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/wire"
	"github.com/prometheus/client_golang/prometheus"
)

// durationBuckets bound the core's histograms, in seconds, from one second to four hours.
var durationBuckets = []float64{1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600, 7200, 14400}

// metricsDeadline bounds each adapter's Metrics call at scrape time; a variable so tests can shorten it.
var metricsDeadline = 2 * time.Second

// Every label value is drawn from the receiver's configuration, its pool's terms and entitlements, or a fixed list in
// this package, so no run ID, node, error, URL, image, path or credential name reaches one.
type metrics struct {
	registry   *prometheus.Registry
	collectors []prometheus.Collector
	register   sync.Once

	claims, admissions, rejections, runs, adoptions, continuations, redeliveries *prometheus.CounterVec
	turnDuration, queueWait, sealWait                                            *prometheus.HistogramVec
	inFlight                                                                     *prometheus.GaugeVec
	capacity                                                                     prometheus.Gauge
	startFailures, dispatchOutcomes, stopFailures, purgeFailures                 *prometheus.CounterVec
	sweepFailures, collectionFailures, sealWaits, redactions                     *prometheus.CounterVec
	busErrors, denials, tokens, cost, dropped                                    *prometheus.CounterVec
	renewalFailures, transactionAborts, ambiguousCommits, fenced                 prometheus.Counter

	adapterDescs map[string]*prometheus.Desc
	// lastGood holds, by adapter, the samples of its latest Metrics call that answered.
	mu       sync.Mutex
	lastGood map[string][]prometheus.Metric
}

func newMetrics(registry *prometheus.Registry) *metrics {
	m := &metrics{registry: registry, adapterDescs: map[string]*prometheus.Desc{}, lastGood: map[string][]prometheus.Metric{}}
	counters := func(name, help string, labels ...string) *prometheus.CounterVec {
		vec := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "wherry_" + name, Help: help}, labels)
		m.collectors = append(m.collectors, vec)
		return vec
	}
	counter := func(name, help string) prometheus.Counter {
		c := prometheus.NewCounter(prometheus.CounterOpts{Name: "wherry_" + name, Help: help})
		m.collectors = append(m.collectors, c)
		return c
	}
	histograms := func(name, help string, labels ...string) *prometheus.HistogramVec {
		vec := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "wherry_" + name, Help: help, Buckets: durationBuckets}, labels)
		m.collectors = append(m.collectors, vec)
		return vec
	}
	m.claims = counters("claims_total", "Tasks claimed and authorised.", "tenant")
	m.admissions = counters("admissions_total", "Tasks admitted to run.", "tenant", "adapter")
	m.rejections = counters("rejections_total", "Tasks rejected, by reason.", "tenant", "reason")
	m.runs = counters("runs_total", "Turns ended, by terminal state.", "tenant", "adapter", "agent", "model", "state")
	m.adoptions = counters("adoptions_total", "Turns that adopted an earlier attempt's job.", "tenant", "adapter")
	m.continuations = counters("continuations_total", "Continuation turns admitted.", "tenant", "adapter")
	m.redeliveries = counters("redeliveries_total", "Turns executed on a redelivery.", "tenant", "adapter")
	m.turnDuration = histograms("turn_duration_seconds", "Time from claim to a turn's terminal state.", "tenant", "adapter", "agent", "model", "state")
	m.queueWait = histograms("queue_wait_seconds", "Time from submission to first claim.", "tenant", "adapter")
	m.inFlight = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "wherry_in_flight", Help: "Tasks held, by the adapter they were routed to."}, []string{"adapter"})
	m.capacity = prometheus.NewGauge(prometheus.GaugeOpts{Name: "wherry_capacity", Help: "Tasks the receiver holds at once."})
	m.collectors = append(m.collectors, m.inFlight, m.capacity)
	m.startFailures = counters("start_failures_total", "Start failures, by class.", "adapter", "class")
	m.dispatchOutcomes = counters("dispatch_outcomes_total", "Dispatch outcomes.", "adapter", "outcome")
	m.stopFailures = counters("stop_failures_total", "Failed adapter Stop calls.", "adapter")
	m.purgeFailures = counters("purge_failures_total", "Failed adapter Purge calls.", "adapter")
	m.sweepFailures = counters("sweep_failures_total", "Failed adapter Sweep calls.", "adapter")
	m.collectionFailures = counters("collection_failures_total", "Turns whose outputs were not collected, by reason.", "adapter", "reason")
	m.sealWaits = counters("seal_waits_total", "Seal waits, by outcome.", "tenant", "outcome")
	m.sealWait = histograms("seal_wait_seconds", "Time a turn waited for its seal.", "tenant")
	m.redactions = counters("redactions_total", "Credential values replaced in published output.", "tenant", "adapter")
	m.busErrors = counters("bus_errors_total", "Kafka operation errors, by operation.", "op")
	m.denials = counters("denials_total", "Kafka authorisation errors, by the capability denied.", "op")
	m.renewalFailures = counter("renewal_failures_total", "Failed claim renewals.")
	m.transactionAborts = counter("transaction_aborts_total", "Transactions aborted after a failed produce.")
	m.ambiguousCommits = counter("ambiguous_commits_total", "Transaction commits with an unknown outcome.")
	m.fenced = counter("fenced_total", "Runs given up to a newer holder.")
	m.tokens = counters("agent_reported_tokens_total", "Tokens as agents report them, by kind.", "tenant", "agent", "model", "kind")
	m.cost = counters("agent_reported_cost_usd_total", "Cost in US dollars as agents report it.", "tenant", "agent")
	m.dropped = counters("metrics_dropped_total", "Adapter samples refused, and adapter Metrics calls that failed, by reason.", "adapter", "reason")
	for _, family := range adapterFamilies {
		labels := []string{"adapter"}
		if family.label != "" {
			labels = append(labels, family.label)
		}
		m.adapterDescs[family.name] = prometheus.NewDesc(family.name, family.help, labels, nil)
	}
	return m
}

// adapterFamily is a metric family the core declares for adapters: its one label besides adapter, if any, takes only
// values.
type adapterFamily struct {
	name, help string
	kind       adapter.Kind
	label      string
	values     []string
}

var adapterFamilies = []adapterFamily{
	{"wherry_adapter_platform_errors_total", "Platform API errors, by class.", adapter.KindCounter, "class", []string{"unavailable", "denied", "conflict", "timeout", "other"}},
	{"wherry_adapter_cleanup_pending", "Ended jobs awaiting cleanup.", adapter.KindGauge, "", nil},
	{"wherry_adapter_sweep_deletions_total", "Material a sweep deleted, by kind.", adapter.KindCounter, "kind", []string{"job", "variable"}},
	{"wherry_adapter_placement_seconds", "Time from dispatch to the agent task starting.", adapter.KindHistogram, "", nil},
}

// registerFor registers the receiver's families on its registry, each labelled with pool, once per process.
func (m *metrics) registerFor(r *Receiver) {
	m.register.Do(func() {
		registerer := prometheus.WrapRegistererWith(prometheus.Labels{"pool": r.config.Pool}, m.registry)
		registerer.MustRegister(append(m.collectors, adapterCollector{r})...)
	})
}

// adapterCollector collects the adapters' samples at scrape time.
type adapterCollector struct{ receiver *Receiver }

func (c adapterCollector) Describe(descs chan<- *prometheus.Desc) {
	for _, desc := range c.receiver.metrics.adapterDescs {
		descs <- desc
	}
}

// Collect calls every adapter's Metrics at once, each within metricsDeadline. An adapter whose call fails gives the
// samples of its last call that answered.
func (c adapterCollector) Collect(out chan<- prometheus.Metric) {
	m := c.receiver.metrics
	var wg sync.WaitGroup
	for _, named := range c.receiver.adapters {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), metricsDeadline)
			defer cancel()
			samples, err := named.Adapter.Metrics(ctx)
			m.mu.Lock()
			defer m.mu.Unlock()
			switch {
			case errors.Is(err, context.DeadlineExceeded):
				m.dropped.WithLabelValues(named.Name, "timeout").Inc()
			case err != nil:
				m.dropped.WithLabelValues(named.Name, "error").Inc()
			default:
				m.lastGood[named.Name] = m.adapterMetrics(named.Name, samples)
			}
			for _, metric := range m.lastGood[named.Name] {
				out <- metric
			}
		})
	}
	wg.Wait()
}

// adapterMetrics keeps the samples that fit the core's adapter families, with the adapter label pinned to name, and
// counts each one it drops by reason.
func (m *metrics) adapterMetrics(name string, samples []adapter.Sample) []prometheus.Metric {
	var kept []prometheus.Metric
	seen := map[string]bool{}
	for _, sample := range samples {
		labels, reason := adapterLabels(name, sample)
		if key := sample.Family + "\xff" + strings.Join(labels, "\xff"); reason == "" && seen[key] {
			reason = "duplicate"
		} else {
			seen[key] = true
		}
		var metric prometheus.Metric
		if reason == "" {
			metric, reason = m.adapterMetric(sample, labels)
		}
		if reason != "" {
			m.dropped.WithLabelValues(name, reason).Inc()
			continue
		}
		kept = append(kept, metric)
	}
	return kept
}

// adapterLabels returns the label values of sample's family, the adapter's name first, or the reason sample is
// dropped.
func adapterLabels(name string, sample adapter.Sample) ([]string, string) {
	index := slices.IndexFunc(adapterFamilies, func(family adapterFamily) bool { return family.name == sample.Family })
	if index < 0 {
		return nil, "unknown_family"
	}
	family := adapterFamilies[index]
	for key := range sample.Labels {
		if key != "adapter" && key != family.label {
			return nil, "unknown_label"
		}
	}
	if sample.Kind != family.kind {
		return nil, "kind"
	}
	if family.label == "" {
		return []string{name}, ""
	}
	value := sample.Labels[family.label]
	if !slices.Contains(family.values, value) {
		return nil, "label_value"
	}
	return []string{name, value}, ""
}

func (m *metrics) adapterMetric(sample adapter.Sample, labels []string) (prometheus.Metric, string) {
	desc := m.adapterDescs[sample.Family]
	if sample.Kind == adapter.KindHistogram {
		histogram := sample.Histogram
		if len(histogram.Buckets) != len(adapter.PlacementBuckets) || math.IsNaN(histogram.Sum) || math.IsInf(histogram.Sum, 0) {
			return nil, "value"
		}
		buckets := map[float64]uint64{}
		previous := uint64(0)
		for i, count := range histogram.Buckets {
			if count < previous || count > histogram.Count {
				return nil, "value"
			}
			buckets[adapter.PlacementBuckets[i]], previous = count, count
		}
		return prometheus.MustNewConstHistogram(desc, histogram.Count, histogram.Sum, buckets, labels...), ""
	}
	if math.IsNaN(sample.Value) || math.IsInf(sample.Value, 0) || sample.Kind == adapter.KindCounter && sample.Value < 0 {
		return nil, "value"
	}
	kind := prometheus.GaugeValue
	if sample.Kind == adapter.KindCounter {
		kind = prometheus.CounterValue
	}
	return prometheus.MustNewConstMetric(desc, kind, sample.Value, labels...), ""
}

// placementLabels are placement's agent and model as the pool's terms bound them, so a host's placement cannot add a
// label value: the agent, or other when the terms do not name it; the model, other when the agent's models do not
// hold it, or empty when the agent has none.
func (r *Receiver) placementLabels(placement wire.Placement) (string, string) {
	state, _, _ := r.snapshot()
	agents := state.Pools[r.config.Pool].Agents
	index := slices.IndexFunc(agents, func(terms admin.AgentTerms) bool { return terms.Name == placement.Agent })
	switch {
	case index < 0:
		return "other", ""
	case len(agents[index].Models) == 0:
		return placement.Agent, ""
	case slices.Contains(agents[index].Models, placement.Model):
		return placement.Agent, placement.Model
	}
	return placement.Agent, "other"
}

// ended counts a turn that reached a terminal state, with the time since its claim when this attempt claimed it, and
// the tokens and cost its agent's result reports. A state outside wire's is labelled other.
func (run *activeRun) ended(state wire.TaskState, resultMetadata []byte) {
	m, tenant := run.receiver.metrics, run.task.Wherry.Tenant
	adapter := cmp.Or(run.adapter.Name, "none")
	agent, model := run.receiver.placementLabels(run.task.Wherry.Placement)
	label := "other"
	if wire.ValidTaskState(state) {
		label = strings.ToLower(strings.TrimPrefix(string(state), "TASK_STATE_"))
	}
	m.runs.WithLabelValues(tenant, adapter, agent, model, label).Inc()
	if !run.claimed.IsZero() {
		m.turnDuration.WithLabelValues(tenant, adapter, agent, model, label).Observe(time.Since(run.claimed).Seconds())
	}
	var reported struct {
		AgentResult struct {
			Usage struct {
				Input     *float64 `json:"input_tokens"`
				Output    *float64 `json:"output_tokens"`
				CacheRead *float64 `json:"cache_read_input_tokens"`
				Cached    *float64 `json:"cached_input_tokens"`
				Reasoning *float64 `json:"reasoning_output_tokens"`
			} `json:"usage"`
			Cost *float64 `json:"total_cost_usd"`
		} `json:"agent_result"`
	}
	if resultMetadata == nil {
		return
	}
	_ = json.Unmarshal(resultMetadata, &reported) // a field of another type adds nothing
	usage := reported.AgentResult.Usage
	for kind, value := range map[string]*float64{"input": usage.Input, "output": usage.Output, "cached": cmp.Or(usage.CacheRead, usage.Cached), "reasoning": usage.Reasoning} {
		if value != nil && *value >= 0 {
			m.tokens.WithLabelValues(tenant, agent, model, kind).Add(*value)
		}
	}
	if cost := reported.AgentResult.Cost; cost != nil && *cost >= 0 {
		m.cost.WithLabelValues(tenant, agent).Add(*cost)
	}
}
