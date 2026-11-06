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

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	openOutcomeOpened     string = "opened"
	openOutcomeRefused    string = "refused"
	openOutcomeExpired    string = "expired"
	openOutcomeUnanswered string = "unanswered"
)

const (
	dispatchOutcomeCreated string = "created"
	dispatchOutcomeNothing string = "nothing"
	dispatchOutcomeUnknown string = "unknown"
)

const (
	startClassInputTooLarge string = "input-too-large"
	startClassDispatch      string = "dispatch"
	startClassPlatform      string = "platform"
)

const (
	collectionReasonFailed     string = "failed"
	collectionReasonNotStopped string = "not_stopped"
	collectionReasonOverLimits string = "over_limits"
	collectionReasonTimeout    string = "timeout"
)

const (
	droppedReasonTimeout       string = "timeout"
	droppedReasonError         string = "error"
	droppedReasonDuplicate     string = "duplicate"
	droppedReasonUnknownFamily string = "unknown_family"
	droppedReasonUnknownLabel  string = "unknown_label"
	droppedReasonKind          string = "kind"
	droppedReasonLabelValue    string = "label_value"
	droppedReasonValue         string = "value"
)

const (
	rejectionReasonRunID              string = "run ID does not match its signer"
	rejectionReasonDuplicate          string = "duplicate submission"
	rejectionReasonAgent              string = "agent is outside the pool terms"
	rejectionReasonModel              string = "model not in the pool's terms"
	rejectionReasonStartBound         string = "not started by task_started_by"
	rejectionReasonEndBound           string = "not ended by task_ended_by"
	rejectionReasonTopicAgent         string = "agent does not match the task topic"
	rejectionReasonSelector           string = "selector does not match the task topic"
	rejectionReasonStartLimit         string = "task_started_by is past the pool's start_by_limit"
	rejectionReasonRequiredCredential string = "the host does not supply a required credential of the agent"
	rejectionReasonCredentialTarget   string = "a task credential takes the target of one of the agent's credentials"
	rejectionReasonVCS                string = "git remote host outside the pool's vcs hosts"
	rejectionReasonStorage            string = "pool has no storage"
	rejectionReasonPart               string = "unsupported message part"
	rejectionReasonEnvelope           string = "the task carries no envelope"
	rejectionReasonAppendTime         string = "task_started_by is before its append time"
	rejectionReasonBoundOrder         string = "task_ended_by is before task_started_by"
	rejectionReasonRunCap             string = "task_ended_by is past task_started_by plus the 6h run cap"
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
	runDuration, queueWait, openWait, placement                   *prometheus.HistogramVec
	inFlight, pastDeadline                                        *prometheus.GaugeVec
	capacity, advertised                                          prometheus.Gauge
	startFailures, dispatchOutcomes, stopFailures, purgeFailures  *prometheus.CounterVec
	sweepFailures, collectionFailures, openWaits, redactions      *prometheus.CounterVec
	busErrors, denials, dropped, budgetExhausted                  *prometheus.CounterVec
	takeovers, claimantChanges                                    *prometheus.CounterVec
	renewalFailures, transactionAborts, ambiguousCommits, fenced  prometheus.Counter
	ignoredWraps, keyLost                                         prometheus.Counter

	pluginDescs map[string]*prometheus.Desc
	// profileReady and claimGated are the routing families, which routingCollector reads at scrape time.
	profileReady, claimGated *prometheus.Desc
	// lastGood holds, by profile, the samples of its latest Metrics call that answered.
	mu       sync.Mutex
	lastGood map[string][]prometheus.Metric
}

func newMetrics(registry *prometheus.Registry) *metrics {
	m := &metrics{registry: registry, pluginDescs: map[string]*prometheus.Desc{}, lastGood: map[string][]prometheus.Metric{}}
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
	m.admissions = counters("admissions_total", "Tasks admitted to run.", "tenant", "profile")
	m.rejections = counters("rejections_total", "Tasks rejected, by reason.", "tenant", "reason")
	m.runs = counters("runs_total", "Runs ended, by terminal state.", "tenant", "profile", "agent", "model", "state")
	m.budgetExhausted = counters("delivery_budget_exhausted_total", "Runs ended failed because their delivery budget ran out.", "tenant", "profile")
	m.adoptions = counters("adoptions_total", "Runs that adopted an earlier attempt's job.", "tenant", "profile")
	m.redeliveries = counters("redeliveries_total", "Runs executed on a redelivery.", "tenant", "profile")
	m.runDuration = histograms("run_duration_seconds", "Time from claim to a run's terminal state.", "tenant", "profile", "agent", "model", "state")
	m.queueWait = histograms("queue_wait_seconds", "Time from submission to first claim.", "tenant", "profile")
	m.inFlight = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "wherry_in_flight", Help: "Tasks held, by the profile they were routed to."}, []string{"profile"})
	m.capacity = prometheus.NewGauge(prometheus.GaugeOpts{Name: "wherry_capacity", Help: "Maximum records the receiver holds at once: the sum of its profiles' replicas."})
	m.pastDeadline = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "wherry_runs_past_deadline", Help: "Tasks held past their task_ended_by, by the profile they were routed to."}, []string{"profile"})
	m.advertised = prometheus.NewGauge(prometheus.GaugeOpts{Name: "wherry_advertised_timestamp_seconds", Help: "Unix time of the receiver's latest advertisement Kafka acknowledged."})
	m.collectors = append(m.collectors, m.inFlight, m.capacity, m.pastDeadline, m.advertised)
	m.startFailures = counters("start_failures_total", "Start failures, by class.", "profile", "class")
	m.dispatchOutcomes = counters("dispatch_outcomes_total", "Dispatch outcomes.", "profile", "outcome")
	m.stopFailures = counters("stop_failures_total", "Failed plugin Stop calls.", "profile")
	m.purgeFailures = counters("purge_failures_total", "Failed plugin Purge calls.", "profile")
	m.sweepFailures = counters("sweep_failures_total", "Failed plugin Sweep calls.", "profile")
	m.collectionFailures = counters("collection_failures_total", "Runs whose outputs were not collected, by reason.", "profile", "reason")
	m.openWaits = counters("open_waits_total", "Open requests waited on, by outcome.", "tenant", "outcome")
	m.openWait = histograms("open_wait_seconds", "Time a run waited for its attempt wrap or refusal.", "tenant")
	m.placement = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "wherry_plugin_placement_seconds", Help: "Time from Dispatch returning to the first observation of the agent task started, by the receiver's clock.",
		Buckets: pluginsdk.PlacementBuckets,
	}, []string{"profile"})
	m.collectors = append(m.collectors, m.placement)
	m.redactions = counters("redactions_total", "Credential values replaced in published output.", "tenant", "profile")
	m.busErrors = counters("bus_errors_total", "Kafka operation errors, by operation.", "op")
	m.denials = counters("denials_total", "Kafka authorisation errors, by the capability denied.", "op")
	m.takeovers = counters("custodian_takeovers_total", "Snapshots this receiver published taking over its pool's key custody, by reason.", "reason")
	// Both series exist from the start, so an alert sees their first increase.
	for _, reason := range []string{"takeover", "contested"} {
		m.takeovers.WithLabelValues(reason)
	}
	m.claimantChanges = counters("claimant_changes_total", "Claims served to a new claimant, by reason.", "reason")
	for _, op := range []capability{capRead, capWrite, capTransactional, capClaim} {
		m.busErrors.WithLabelValues(string(op))
	}
	for _, op := range []capability{capRead, capWrite, capTransactional, capClaim, capMembership} {
		m.denials.WithLabelValues(string(op))
	}
	for _, reason := range []string{wire.ClaimReleased, wire.ClaimLease} {
		m.claimantChanges.WithLabelValues(reason)
	}
	m.keyLost = counter("runs_key_lost_total", "Runs ended with the wherry event key-lost.")
	m.ignoredWraps = counter("ignored_wraps_total", "Wraps whose key does not decrypt the task's envelope.")
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
	m.profileReady = prometheus.NewDesc("wherry_profile_ready", "Whether the profile is in the receiver's current ready offers.", []string{"profile", "topic"}, nil)
	m.claimGated = prometheus.NewDesc("wherry_claim_gated", "Whether the claim gate prevents a new poll, by reason.", []string{"reason"}, nil)
	return m
}

// Children must exist before refresh publishes entitlements and remain across subsequent refreshes.
func (m *metrics) initialize(state operator.State, pool string, profiles []Named) {
	for _, profile := range profiles {
		name := profile.Name
		for _, value := range []string{startClassInputTooLarge, startClassDispatch, startClassPlatform} {
			m.startFailures.WithLabelValues(name, value)
		}
		for _, value := range []string{dispatchOutcomeCreated, dispatchOutcomeNothing, dispatchOutcomeUnknown} {
			m.dispatchOutcomes.WithLabelValues(name, value)
		}
		for _, value := range []string{collectionReasonFailed, collectionReasonNotStopped, collectionReasonOverLimits, collectionReasonTimeout} {
			m.collectionFailures.WithLabelValues(name, value)
		}
		for _, value := range []string{droppedReasonTimeout, droppedReasonError, droppedReasonDuplicate, droppedReasonUnknownFamily, droppedReasonUnknownLabel, droppedReasonKind, droppedReasonLabelValue, droppedReasonValue} {
			m.dropped.WithLabelValues(name, value)
		}
		m.stopFailures.WithLabelValues(name)
		m.purgeFailures.WithLabelValues(name)
		m.sweepFailures.WithLabelValues(name)
		m.placement.WithLabelValues(name)
	}
	for tenant, pools := range state.Entitlements {
		if !pools[pool] {
			continue
		}
		m.claims.WithLabelValues(tenant)
		m.openWait.WithLabelValues(tenant)
		for _, value := range []string{openOutcomeOpened, openOutcomeRefused, openOutcomeExpired, openOutcomeUnanswered} {
			m.openWaits.WithLabelValues(tenant, value)
		}
		for _, value := range []string{rejectionReasonRunID, rejectionReasonDuplicate, rejectionReasonAgent, rejectionReasonModel, rejectionReasonStartBound, rejectionReasonEndBound, rejectionReasonTopicAgent, rejectionReasonSelector, rejectionReasonStartLimit, rejectionReasonRequiredCredential, rejectionReasonCredentialTarget, rejectionReasonVCS, rejectionReasonStorage, rejectionReasonPart, rejectionReasonEnvelope, rejectionReasonAppendTime, rejectionReasonBoundOrder, rejectionReasonRunCap} {
			m.rejections.WithLabelValues(tenant, value)
		}
		admissions, budget := map[string]bool{"": true}, map[string]bool{"none": true}
		for _, profile := range profiles {
			admissions[profile.Name], budget[profile.Name] = true, true
			m.adoptions.WithLabelValues(tenant, profile.Name)
			for _, terms := range state.Pools[pool].Agents {
				if terms.Name != profile.Agent {
					continue
				}
				models := terms.Models
				if len(models) == 0 {
					models = []string{""}
				}
				for _, model := range models {
					agent, model := termsPlacementLabels(state.Pools[pool].Agents, wire.Placement{Agent: profile.Agent, Model: model})
					for _, status := range []string{"completed", "failed", "canceled"} {
						m.runs.WithLabelValues(tenant, profile.Name, agent, model, status)
						m.runDuration.WithLabelValues(tenant, profile.Name, agent, model, status)
					}
				}
				break
			}
		}
		for profile := range admissions {
			m.admissions.WithLabelValues(tenant, profile)
			m.redeliveries.WithLabelValues(tenant, profile)
			m.queueWait.WithLabelValues(tenant, profile)
		}
		for profile := range budget {
			m.budgetExhausted.WithLabelValues(tenant, profile)
			m.redactions.WithLabelValues(tenant, profile)
		}
	}
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
	{"wherry_plugin_sweep_deletions_total", "Material a sweep deleted, by kind.", pluginsdk.KindCounter, "kind", []string{"job", "variable", "directory"}},
	{"wherry_plugin_storage_free_bytes", "Free bytes of the storage the profile's runs share.", pluginsdk.KindGauge, "", nil},
}

// registerFor registers the receiver's families on its registry, each labelled with pool, once per process.
func (m *metrics) registerFor(r *Receiver) {
	m.register.Do(func() {
		registerer := prometheus.WrapRegistererWith(prometheus.Labels{"pool": r.config.Pool}, m.registry)
		registerer.MustRegister(append(m.collectors, pluginCollector{r}, routingCollector{r})...)
	})
}

// routingCollector samples routing and claim gates under the receiver lock.
type routingCollector struct{ receiver *Receiver }

func (c routingCollector) Describe(descs chan<- *prometheus.Desc) {
	descs <- c.receiver.metrics.profileReady
	descs <- c.receiver.metrics.claimGated
}

// Collect snapshots routing under r.mu and emits metrics after unlocking, without plugin calls, Kafka calls, or sleeps.
func (c routingCollector) Collect(out chan<- prometheus.Metric) {
	r, m := c.receiver, c.receiver.metrics
	type profileSample struct {
		profile, topic string
		ready          bool
	}
	r.mu.Lock()
	// The three operands of claimGated, read as it reads them.
	gated := map[string]bool{
		"suspended":      r.state.Suspended[r.config.Pool],
		"no_ready_offer": len(r.offers) == 0,
		"pending_run":    len(r.pending) > 0,
	}
	profiles := make([]profileSample, 0, len(r.profiles))
	for _, named := range r.profiles {
		profiles = append(profiles, profileSample{
			profile: named.Name,
			topic:   operator.SignatureTopic(r.config.Pool, named.Agent, named.Labels),
			ready:   r.ready(named.Name),
		})
	}
	r.mu.Unlock()
	for _, sample := range profiles {
		out <- prometheus.MustNewConstMetric(m.profileReady, prometheus.GaugeValue, gauge(sample.ready), sample.profile, sample.topic)
	}
	for _, reason := range []string{"suspended", "no_ready_offer", "pending_run"} {
		out <- prometheus.MustNewConstMetric(m.claimGated, prometheus.GaugeValue, gauge(gated[reason]), reason)
	}
}

// gauge is 1 for true and 0 for false.
func gauge(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

// pluginCollector collects the profiles' samples at scrape time.
type pluginCollector struct{ receiver *Receiver }

func (c pluginCollector) Describe(descs chan<- *prometheus.Desc) {
	for _, desc := range c.receiver.metrics.pluginDescs {
		descs <- desc
	}
}

// Collect retains the last successful samples when a profile fails its bounded scrape, running profile calls
// concurrently within metricsDeadline.
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
				m.dropped.WithLabelValues(named.Name, droppedReasonTimeout).Inc()
			case err != nil:
				m.dropped.WithLabelValues(named.Name, droppedReasonError).Inc()
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
			reason = droppedReasonDuplicate
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
		return nil, droppedReasonUnknownFamily
	}
	family := pluginFamilies[index]
	for key := range sample.Labels {
		if key != "profile" && key != family.label {
			return nil, droppedReasonUnknownLabel
		}
	}
	if sample.Kind != family.kind {
		return nil, droppedReasonKind
	}
	if family.label == "" {
		return []string{name}, ""
	}
	value := sample.Labels[family.label]
	if !slices.Contains(family.values, value) {
		return nil, droppedReasonLabelValue
	}
	return []string{name, value}, ""
}

func (m *metrics) pluginMetric(sample pluginsdk.Sample, labels []string) (prometheus.Metric, string) {
	desc := m.pluginDescs[sample.Family]
	if math.IsNaN(sample.Value) || math.IsInf(sample.Value, 0) || sample.Kind == pluginsdk.KindCounter && sample.Value < 0 {
		return nil, droppedReasonValue
	}
	kind := prometheus.GaugeValue
	if sample.Kind == pluginsdk.KindCounter {
		kind = prometheus.CounterValue
	}
	return prometheus.MustNewConstMetric(desc, kind, sample.Value, labels...), ""
}

// placementLabels bounds placement labels to the current pool terms, using other for unknown agents or models
// and empty for an agent with no models.
func (r *Receiver) placementLabels(placement wire.Placement) (string, string) {
	state, _, _ := r.snapshot()
	return termsPlacementLabels(state.Pools[r.config.Pool].Agents, placement)
}

func termsPlacementLabels(agents []operator.AgentTerms, placement wire.Placement) (string, string) {
	index := slices.IndexFunc(agents, func(terms operator.AgentTerms) bool { return terms.Name == placement.Agent })
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
	if run.event == wire.EventKeyLost {
		m.keyLost.Inc()
	}
	if !run.claimed.IsZero() {
		m.runDuration.WithLabelValues(tenant, profile, agent, model, label).Observe(time.Since(run.claimed).Seconds())
	}
}
