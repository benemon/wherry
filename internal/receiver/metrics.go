package receiver

import (
	"cmp"
	"context"
	"errors"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
	"github.com/prometheus/client_golang/prometheus"
)

// durationBuckets bound the core's histograms, in seconds, from one second to six hours.
var durationBuckets = []float64{1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600, 7200, 14400, 21600}

// metricsDeadline bounds each profile's Metrics call at scrape time; a variable so tests can shorten it.
var metricsDeadline = 2 * time.Second

// Every label value is drawn from the receiver's configuration, its pool's terms and entitlements, or a fixed list in
// this package, so no run ID, node, error, URL, image, path or credential name reaches one.
type metrics struct {
	registry   *prometheus.Registry
	collectors []prometheus.Collector
	register   sync.Once

	claims, admissions, rejections, runs, adoptions, redeliveries *prometheus.CounterVec
	runDuration, queueWait, sealWait                              *prometheus.HistogramVec
	inFlight                                                      *prometheus.GaugeVec
	capacity                                                      prometheus.Gauge
	startFailures, dispatchOutcomes, stopFailures, purgeFailures  *prometheus.CounterVec
	sweepFailures, collectionFailures, sealWaits, redactions      *prometheus.CounterVec
	busErrors, denials, dropped                                   *prometheus.CounterVec
	renewalFailures, transactionAborts, ambiguousCommits, fenced  prometheus.Counter

	pluginDescs map[string]*prometheus.Desc
	// series names every series the core families export.
	series []string
	// lastGood holds, by profile, the samples of its latest Metrics call that answered.
	mu       sync.Mutex
	lastGood map[string][]prometheus.Metric
}

func newMetrics(registry *prometheus.Registry) *metrics {
	m := &metrics{registry: registry, pluginDescs: map[string]*prometheus.Desc{}, lastGood: map[string][]prometheus.Metric{}}
	counters := func(name, help string, labels ...string) *prometheus.CounterVec {
		vec := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "wherry_" + name, Help: help}, labels)
		m.collectors, m.series = append(m.collectors, vec), append(m.series, "wherry_"+name)
		return vec
	}
	counter := func(name, help string) prometheus.Counter {
		c := prometheus.NewCounter(prometheus.CounterOpts{Name: "wherry_" + name, Help: help})
		m.collectors, m.series = append(m.collectors, c), append(m.series, "wherry_"+name)
		return c
	}
	histograms := func(name, help string, labels ...string) *prometheus.HistogramVec {
		vec := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "wherry_" + name, Help: help, Buckets: durationBuckets}, labels)
		m.collectors, m.series = append(m.collectors, vec), append(m.series, histogramSeries("wherry_"+name)...)
		return vec
	}
	m.claims = counters("claims_total", "Tasks claimed and authorised.", "tenant")
	m.admissions = counters("admissions_total", "Tasks admitted to run.", "tenant", "profile")
	m.rejections = counters("rejections_total", "Tasks rejected, by reason.", "tenant", "reason")
	m.runs = counters("runs_total", "Runs ended, by terminal state.", "tenant", "profile", "agent", "model", "state")
	m.adoptions = counters("adoptions_total", "Runs that adopted an earlier attempt's job.", "tenant", "profile")
	m.redeliveries = counters("redeliveries_total", "Runs executed on a redelivery.", "tenant", "profile")
	m.runDuration = histograms("run_duration_seconds", "Time from claim to a run's terminal state.", "tenant", "profile", "agent", "model", "state")
	m.queueWait = histograms("queue_wait_seconds", "Time from submission to first claim.", "tenant", "profile")
	m.inFlight = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "wherry_in_flight", Help: "Tasks held, by the profile they were routed to."}, []string{"profile"})
	m.capacity = prometheus.NewGauge(prometheus.GaugeOpts{Name: "wherry_capacity", Help: "Tasks the receiver holds at once."})
	m.collectors = append(m.collectors, m.inFlight, m.capacity)
	m.series = append(m.series, "wherry_in_flight", "wherry_capacity")
	m.startFailures = counters("start_failures_total", "Start failures, by class.", "profile", "class")
	m.dispatchOutcomes = counters("dispatch_outcomes_total", "Dispatch outcomes.", "profile", "outcome")
	m.stopFailures = counters("stop_failures_total", "Failed plugin Stop calls.", "profile")
	m.purgeFailures = counters("purge_failures_total", "Failed plugin Purge calls.", "profile")
	m.sweepFailures = counters("sweep_failures_total", "Failed plugin Sweep calls.", "profile")
	m.collectionFailures = counters("collection_failures_total", "Runs whose outputs were not collected, by reason.", "profile", "reason")
	m.sealWaits = counters("seal_waits_total", "Seal waits, by outcome.", "tenant", "outcome")
	m.sealWait = histograms("seal_wait_seconds", "Time a run waited for its seal.", "tenant")
	m.redactions = counters("redactions_total", "Credential values replaced in published output.", "tenant", "profile")
	m.busErrors = counters("bus_errors_total", "Kafka operation errors, by operation.", "op")
	m.denials = counters("denials_total", "Kafka authorisation errors, by the capability denied.", "op")
	m.renewalFailures = counter("renewal_failures_total", "Failed claim renewals.")
	m.transactionAborts = counter("transaction_aborts_total", "Transactions aborted after a failed produce.")
	m.ambiguousCommits = counter("ambiguous_commits_total", "Transaction commits with an unknown outcome.")
	m.fenced = counter("fenced_total", "Runs given up to a newer holder.")
	m.dropped = counters("metrics_dropped_total", "Plugin samples refused, and plugin Metrics calls that failed, by reason.", "profile", "reason")
	for _, family := range pluginFamilies {
		labels := []string{"profile"}
		if family.label != "" {
			labels = append(labels, family.label)
		}
		m.pluginDescs[family.name] = prometheus.NewDesc(family.name, family.help, labels, nil)
	}
	return m
}

// SeriesNames returns the name of every series a receiver's core and plugin families export, a histogram's being its
// _bucket, _sum and _count series. The shipped dashboard and alert rules name no other wherry_ series.
func SeriesNames() []string {
	names := newMetrics(nil).series
	for _, family := range pluginFamilies {
		if family.kind == pluginsdk.KindHistogram {
			names = append(names, histogramSeries(family.name)...)
		} else {
			names = append(names, family.name)
		}
	}
	return names
}

func histogramSeries(family string) []string {
	return []string{family + "_bucket", family + "_sum", family + "_count"}
}

// pluginFamily is a metric family the core declares for plugins: its one label besides profile, if any, takes only
// values.
type pluginFamily struct {
	name, help string
	kind       pluginsdk.Kind
	label      string
	values     []string
}

var pluginFamilies = []pluginFamily{
	{"wherry_plugin_platform_errors_total", "Platform API errors, by class.", pluginsdk.KindCounter, "class", []string{"unavailable", "denied", "conflict", "timeout", "other"}},
	{"wherry_plugin_cleanup_pending", "Ended jobs awaiting cleanup.", pluginsdk.KindGauge, "", nil},
	{"wherry_plugin_sweep_deletions_total", "Material a sweep deleted, by kind.", pluginsdk.KindCounter, "kind", []string{"job", "variable"}},
	{"wherry_plugin_placement_seconds", "Time from dispatch to the agent task starting.", pluginsdk.KindHistogram, "", nil},
}

// registerFor registers the receiver's families on its registry, each labelled with pool, once per process.
func (m *metrics) registerFor(r *Receiver) {
	m.register.Do(func() {
		registerer := prometheus.WrapRegistererWith(prometheus.Labels{"pool": r.config.Pool}, m.registry)
		registerer.MustRegister(append(m.collectors, pluginCollector{r})...)
	})
}

// pluginCollector collects the profiles' samples at scrape time.
type pluginCollector struct{ receiver *Receiver }

func (c pluginCollector) Describe(descs chan<- *prometheus.Desc) {
	for _, desc := range c.receiver.metrics.pluginDescs {
		descs <- desc
	}
}

// Collect calls every profile's Metrics at once, each within metricsDeadline. A profile whose call fails gives the
// samples of its last call that answered.
func (c pluginCollector) Collect(out chan<- prometheus.Metric) {
	m := c.receiver.metrics
	var wg sync.WaitGroup
	for _, named := range c.receiver.profiles {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), metricsDeadline)
			defer cancel()
			samples, err := named.Plugin.Metrics(ctx)
			m.mu.Lock()
			defer m.mu.Unlock()
			switch {
			case errors.Is(err, context.DeadlineExceeded):
				m.dropped.WithLabelValues(named.Name, "timeout").Inc()
			case err != nil:
				m.dropped.WithLabelValues(named.Name, "error").Inc()
			default:
				m.lastGood[named.Name] = m.pluginMetrics(named.Name, samples)
			}
			for _, metric := range m.lastGood[named.Name] {
				out <- metric
			}
		})
	}
	wg.Wait()
}

// pluginMetrics keeps the samples that fit the core's plugin families, with the profile label pinned to name, and
// counts each one it drops by reason.
func (m *metrics) pluginMetrics(name string, samples []pluginsdk.Sample) []prometheus.Metric {
	var kept []prometheus.Metric
	seen := map[string]bool{}
	for _, sample := range samples {
		labels, reason := pluginLabels(name, sample)
		if key := sample.Family + "\xff" + strings.Join(labels, "\xff"); reason == "" && seen[key] {
			reason = "duplicate"
		} else {
			seen[key] = true
		}
		var metric prometheus.Metric
		if reason == "" {
			metric, reason = m.pluginMetric(sample, labels)
		}
		if reason != "" {
			m.dropped.WithLabelValues(name, reason).Inc()
			continue
		}
		kept = append(kept, metric)
	}
	return kept
}

// pluginLabels returns the label values of sample's family, the profile's name first, or the reason sample is
// dropped.
func pluginLabels(name string, sample pluginsdk.Sample) ([]string, string) {
	index := slices.IndexFunc(pluginFamilies, func(family pluginFamily) bool { return family.name == sample.Family })
	if index < 0 {
		return nil, "unknown_family"
	}
	family := pluginFamilies[index]
	for key := range sample.Labels {
		if key != "profile" && key != family.label {
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

func (m *metrics) pluginMetric(sample pluginsdk.Sample, labels []string) (prometheus.Metric, string) {
	desc := m.pluginDescs[sample.Family]
	if sample.Kind == pluginsdk.KindHistogram {
		histogram := sample.Histogram
		if len(histogram.Buckets) != len(pluginsdk.PlacementBuckets) || math.IsNaN(histogram.Sum) || math.IsInf(histogram.Sum, 0) {
			return nil, "value"
		}
		buckets := map[float64]uint64{}
		previous := uint64(0)
		for i, count := range histogram.Buckets {
			if count < previous || count > histogram.Count {
				return nil, "value"
			}
			buckets[pluginsdk.PlacementBuckets[i]], previous = count, count
		}
		return prometheus.MustNewConstHistogram(desc, histogram.Count, histogram.Sum, buckets, labels...), ""
	}
	if math.IsNaN(sample.Value) || math.IsInf(sample.Value, 0) || sample.Kind == pluginsdk.KindCounter && sample.Value < 0 {
		return nil, "value"
	}
	kind := prometheus.GaugeValue
	if sample.Kind == pluginsdk.KindCounter {
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

// ended counts a run that reached a terminal state, with the time since its claim when this attempt claimed it. A
// state outside wire's is labelled other.
func (run *activeRun) ended(state wire.TaskState) {
	m, tenant := run.receiver.metrics, run.task.Wherry.Tenant
	profile := cmp.Or(run.profile.Name, "none")
	agent, model := run.receiver.placementLabels(run.task.Wherry.Placement)
	label := "other"
	if wire.ValidTaskState(state) {
		label = strings.ToLower(strings.TrimPrefix(string(state), "TASK_STATE_"))
	}
	m.runs.WithLabelValues(tenant, profile, agent, model, label).Inc()
	if !run.claimed.IsZero() {
		m.runDuration.WithLabelValues(tenant, profile, agent, model, label).Observe(time.Since(run.claimed).Seconds())
	}
}
