package receiver

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"net/http/httptest"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func scrape(t *testing.T, r *Receiver) string {
	t.Helper()
	recorder := httptest.NewRecorder()
	promhttp.HandlerFor(r.metrics.registry, promhttp.HandlerOpts{}).ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	if recorder.Code != 200 {
		t.Fatalf("scrape status %d: %s", recorder.Code, recorder.Body.String())
	}
	return recorder.Body.String()
}

// scraped waits up to ten seconds for a scrape holding every one of series.
func scraped(t *testing.T, r *Receiver, series ...string) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		text, missing := scrape(t, r), ""
		for _, line := range series {
			if !strings.Contains(text, line) {
				missing = line
				break
			}
		}
		if missing == "" {
			return text
		}
		if time.Now().After(deadline) {
			t.Fatalf("scrape lacks %s:\n%s", missing, text)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestScrapeAfterARun(t *testing.T) {
	f := newFixture(t, resultStream)
	if task := runToTask(t, f); task.Status.State != wire.TaskStateCompleted {
		t.Fatalf("task %#v", task)
	}
	scraped(t, f.receiver,
		`wherry_claims_total{pool="pool-a",tenant="tenant-a"} 1`,
		`wherry_admissions_total{pool="pool-a",profile="nomad",tenant="tenant-a"} 1`,
		`wherry_queue_wait_seconds_count{pool="pool-a",profile="nomad",tenant="tenant-a"} 1`,
		`wherry_dispatch_outcomes_total{outcome="created",pool="pool-a",profile="nomad"} 1`,
		`wherry_open_waits_total{outcome="opened",pool="pool-a",tenant="tenant-a"} 1`,
		`wherry_open_wait_seconds_count{pool="pool-a",tenant="tenant-a"} 1`,
		`wherry_runs_total{agent="claude",model="",pool="pool-a",profile="nomad",state="completed",tenant="tenant-a"} 1`,
		`wherry_run_duration_seconds_count{agent="claude",model="",pool="pool-a",profile="nomad",state="completed",tenant="tenant-a"} 1`,
		`wherry_in_flight{pool="pool-a",profile="nomad"} 0`,
		`wherry_capacity{pool="pool-a"} 1`,
	)
}

// A credentialed S3 run whose agent prints its credential and URL, then fails, on a plugin whose sweep fails with a
// URL in its error, leaves none of that material in a label or value.
func TestScrapeCarriesNoRunMaterial(t *testing.T) {
	s := newStore(t)
	url := presigned(s.endpoint(), wire.S3InputKey(testTenant, testRun("run-a"), "dataset.bin"))
	f := s3Fixture(t, s, []string{`{"type":"assistant","message":"token ` + testToken + ` url ` + url + `"}`}, nil)
	f.plugin.mu.Lock()
	f.plugin.ended = &pluginsdk.Terminal{Started: true, ExitCode: 3, Node: "node-secret-7"}
	f.plugin.sweepErr = errors.New("sweep of " + url + " failed")
	f.plugin.mu.Unlock()
	if err := f.receiver.advertise(f.ctx); err != nil {
		t.Fatal(err)
	}
	record := f.submit(t, f.host, withS3)
	if status := f.execute(t, record, 1); status != kgo.AckReject {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	text := scraped(t, f.receiver,
		`wherry_runs_total{agent="claude",model="",pool="pool-a",profile="nomad",`,
		`state="failed",tenant="tenant-a"} 1`,
		`wherry_redactions_total{pool="pool-a",profile="nomad",tenant="tenant-a"} `,
		`wherry_sweep_failures_total{pool="pool-a",profile="nomad"} `,
	)
	host := strings.TrimPrefix(s.endpoint(), "https://")
	for _, forbidden := range []string{testRun("run-a"), credentialName, testToken, host, s3Bucket, s3Signature, "X-Amz", "dataset.bin", "report.pdf", "agent reported no outcome", "node-secret-7", testHost} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("scrape holds %q:\n%s", forbidden, text)
		}
	}
	// The routing families carry the profile's hashed topic and fixed reasons, and none of its label keys; its values
	// dc1 and none are generic, so a scrape may hold them elsewhere.
	topic := f.profileTopic(t, "nomad")
	if got, want := gaugeLines(t, text, "wherry_profile_ready"), []string{`wherry_profile_ready{pool="pool-a",profile="nomad",topic="` + topic + `"} 1`}; !slices.Equal(got, want) {
		t.Fatalf("wherry_profile_ready samples %q; want %q", got, want)
	}
	if got := gaugeLines(t, text, "wherry_claim_gated"); len(got) != 3 {
		t.Fatalf("wherry_claim_gated samples %q; want 3", got)
	}
	gathered, err := f.receiver.metrics.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	labels := map[string][]string{"wherry_profile_ready": {"pool", "profile", "topic"}, "wherry_claim_gated": {"pool", "reason"}}
	for _, family := range gathered {
		want, ok := labels[family.GetName()]
		if !ok {
			continue
		}
		delete(labels, family.GetName())
		for _, metric := range family.GetMetric() {
			var names []string
			for _, pair := range metric.GetLabel() {
				names = append(names, pair.GetName())
			}
			if slices.Sort(names); !slices.Equal(names, want) {
				t.Fatalf("%s labels %v; want %v", family.GetName(), names, want)
			}
		}
	}
	if len(labels) != 0 {
		t.Fatalf("scrape lacks %v", slices.Collect(maps.Keys(labels)))
	}
	for _, forbidden := range []string{"gpu", "zone", "example.com"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("scrape holds the profile label key %q:\n%s", forbidden, text)
		}
	}
}

// pluginReceiver is a configured receiver with one fake plugin, enough to scrape its plugin families.
func pluginReceiver(fake *fakePlugin) *Receiver {
	r := &Receiver{config: operator.ReceiverConfig{Pool: testPool}, profiles: []Named{{Name: "nomad", Plugin: fake}}, metrics: newMetrics(prometheus.NewRegistry())}
	r.metrics.registerFor(r)
	return r
}

func placement(buckets ...uint64) pluginsdk.Histogram {
	return pluginsdk.Histogram{Buckets: buckets, Sum: 12.5, Count: buckets[len(buckets)-1]}
}

func TestPluginSamplesAreValidated(t *testing.T) {
	r := pluginReceiver(&fakePlugin{samples: []pluginsdk.Sample{
		{Family: "wherry_plugin_platform_errors_total", Labels: map[string]string{"class": "denied"}, Kind: pluginsdk.KindCounter, Value: 4},
		{Family: "wherry_plugin_cleanup_pending", Kind: pluginsdk.KindGauge, Value: 2},
		{Family: "wherry_plugin_sweep_deletions_total", Labels: map[string]string{"kind": "variable", "profile": "nomad"}, Kind: pluginsdk.KindCounter, Value: 7},
		{Family: "wherry_plugin_placement_seconds", Kind: pluginsdk.KindHistogram, Histogram: placement(0, 1, 2, 2, 3, 3, 3, 3, 3, 3)},
		{Family: "wherry_plugin_storage_free_bytes", Kind: pluginsdk.KindGauge, Value: 5e9},
		{Family: "wherry_plugin_sweep_deletions_total", Labels: map[string]string{"kind": "directory"}, Kind: pluginsdk.KindCounter, Value: 6},
		{Family: "wherry_plugin_jobs_total", Kind: pluginsdk.KindCounter, Value: 11},
		{Family: "wherry_plugin_cleanup_pending", Labels: map[string]string{"node": "node-1"}, Kind: pluginsdk.KindGauge, Value: 12},
		{Family: "wherry_plugin_platform_errors_total", Labels: map[string]string{"class": "run-a"}, Kind: pluginsdk.KindCounter, Value: 13},
		{Family: "wherry_plugin_sweep_deletions_total", Labels: map[string]string{"kind": "job"}, Kind: pluginsdk.KindGauge, Value: 14},
	}})
	text := scraped(t, r,
		`wherry_plugin_platform_errors_total{class="denied",pool="pool-a",profile="nomad"} 4`,
		`wherry_plugin_cleanup_pending{pool="pool-a",profile="nomad"} 2`,
		`wherry_plugin_sweep_deletions_total{kind="variable",pool="pool-a",profile="nomad"} 7`,
		`wherry_plugin_storage_free_bytes{pool="pool-a",profile="nomad"} 5e+09`,
		`wherry_plugin_sweep_deletions_total{kind="directory",pool="pool-a",profile="nomad"} 6`,
		`wherry_metrics_dropped_total{pool="pool-a",profile="nomad",reason="unknown_family"} `,
		`wherry_metrics_dropped_total{pool="pool-a",profile="nomad",reason="unknown_label"} `,
		`wherry_metrics_dropped_total{pool="pool-a",profile="nomad",reason="label_value"} `,
		`wherry_metrics_dropped_total{pool="pool-a",profile="nomad",reason="kind"} `,
	)
	// The receiver observes placement itself, so a plugin's sample of it is an unknown family's.
	for _, dropped := range []string{"wherry_plugin_placement_seconds", "wherry_plugin_jobs_total", "node-1", testRun("run-a"), "} 11\n", "} 12\n", "} 13\n", "} 14\n"} {
		if strings.Contains(text, dropped) {
			t.Fatalf("scrape holds the dropped %q:\n%s", dropped, text)
		}
	}
}

// Every family the core declares for plugins is exported under wherry_plugin_.
func TestPluginFamiliesAreExportedAsWherryPlugin(t *testing.T) {
	var samples []pluginsdk.Sample
	for _, family := range pluginFamilies {
		if !strings.HasPrefix(family.name, "wherry_plugin_") {
			t.Fatalf("family %s; want the wherry_plugin_ prefix", family.name)
		}
		sample := pluginsdk.Sample{Family: family.name, Kind: family.kind, Value: 1}
		if family.label != "" {
			sample.Labels = map[string]string{family.label: family.values[0]}
		}
		samples = append(samples, sample)
	}
	r := pluginReceiver(&fakePlugin{samples: samples})
	scraped(t, r)
	gathered, err := r.metrics.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	exported := 0
	for _, family := range gathered {
		if slices.ContainsFunc(pluginFamilies, func(declared pluginFamily) bool { return declared.name == family.GetName() }) {
			exported++
		}
	}
	if exported != len(pluginFamilies) {
		t.Fatalf("exported %d of the %d plugin families", exported, len(pluginFamilies))
	}
}

func TestPluginSampleCannotSpeakForAnotherProfile(t *testing.T) {
	r := pluginReceiver(&fakePlugin{samples: []pluginsdk.Sample{
		{Family: "wherry_plugin_cleanup_pending", Labels: map[string]string{"profile": "other"}, Kind: pluginsdk.KindGauge, Value: 5},
	}})
	text := scraped(t, r, `wherry_plugin_cleanup_pending{pool="pool-a",profile="nomad"} 5`)
	if strings.Contains(text, `profile="other"`) {
		t.Fatalf("scrape holds another profile's sample:\n%s", text)
	}
}

func TestHungPluginMetricsServeTheLastGoodSamples(t *testing.T) {
	previous := metricsDeadline
	metricsDeadline = 200 * time.Millisecond
	t.Cleanup(func() { metricsDeadline = previous })
	fake := &fakePlugin{samples: []pluginsdk.Sample{{Family: "wherry_plugin_cleanup_pending", Kind: pluginsdk.KindGauge, Value: 3}}}
	r := pluginReceiver(fake)
	scraped(t, r, `wherry_plugin_cleanup_pending{pool="pool-a",profile="nomad"} 3`)
	fake.mu.Lock()
	fake.metricsHang, fake.samples = true, nil
	fake.mu.Unlock()
	for range 2 {
		began := time.Now()
		done := make(chan string, 1)
		go func() { done <- scrape(t, r) }()
		select {
		case text := <-done:
			if !strings.Contains(text, `wherry_plugin_cleanup_pending{pool="pool-a",profile="nomad"} 3`) {
				t.Fatalf("scrape of a hung plugin lacks its last good sample:\n%s", text)
			}
			if elapsed := time.Since(began); elapsed > time.Second {
				t.Fatalf("scrape of a hung plugin took %v", elapsed)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("scrape of a hung plugin did not return")
		}
	}
	scraped(t, r, `wherry_metrics_dropped_total{pool="pool-a",profile="nomad",reason="timeout"} `)
}

// A turn whose agent the pool's terms do not name, such as an interrupted turn ended after the terms changed, counts
// under agent other.
func TestAgentOutsideTheTermsIsLabelledOther(t *testing.T) {
	f := newFixture(t, resultStream)
	run := testActiveRun(t, f, "run-x", 1)
	run.task.Wherry.Placement.Agent = "gemini"
	run.ended(wire.TaskStateCompleted)
	text := scraped(t, f.receiver,
		`wherry_runs_total{agent="other",model="",pool="pool-a",profile="nomad",state="completed",tenant="tenant-a"} 1`,
	)
	if strings.Contains(text, "gemini") {
		t.Fatalf("scrape holds the unnamed agent:\n%s", text)
	}
}

// A model the agent's terms do not hold, such as one dropped from the terms while its turn ran, counts under model
// other.
func TestModelOutsideTheTermsIsLabelledOther(t *testing.T) {
	terms := testTerms()
	terms.Agents[0].Models, terms.Agents[0].DefaultModel = []string{"haiku", "sonnet"}, "haiku"
	f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream}})
	for _, model := range []string{"sonnet", "opus"} {
		run := testActiveRun(t, f, "run-"+model, 1)
		run.task.Wherry.Placement.Model = model
		run.ended(wire.TaskStateCompleted)
	}
	text := scraped(t, f.receiver,
		`wherry_runs_total{agent="claude",model="sonnet",pool="pool-a",profile="nomad",state="completed",tenant="tenant-a"} 1`,
		`wherry_runs_total{agent="claude",model="other",pool="pool-a",profile="nomad",state="completed",tenant="tenant-a"} 1`,
	)
	if strings.Contains(text, "opus") {
		t.Fatalf("scrape holds the model outside the terms:\n%s", text)
	}
}

// A state outside wire's task states counts under state other.
func TestStateOutsideTheTaskStatesIsLabelledOther(t *testing.T) {
	f := newFixture(t, resultStream)
	testActiveRun(t, f, "run-x", 1).ended(wire.TaskState("TASK_STATE_RUN_A"))
	text := scraped(t, f.receiver, `wherry_runs_total{agent="claude",model="",pool="pool-a",profile="nomad",state="other",tenant="tenant-a"} 1`)
	if strings.Contains(text, "run_a") {
		t.Fatalf("scrape holds the unknown state:\n%s", text)
	}
}

func TestRejectionReasonsThatQuoteTheTaskAreFixed(t *testing.T) {
	f := newFixture(t, resultStream)
	f.rejected(t, f.submit(t, f.host, withGit(wire.GitInput{Remote: "https://git.example.test/org/repo.git", Ref: "main"})),
		"git remote host git.example.test is not in pool "+testPool+"'s vcs hosts")
	text := scraped(t, f.receiver,
		`wherry_rejections_total{pool="pool-a",reason="git remote host outside the pool's vcs hosts",tenant="tenant-a"} 1`,
	)
	for _, forbidden := range []string{"git.example.test", testRun("run-a")} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("scrape holds %q:\n%s", forbidden, text)
		}
	}
}

// scrapedValue is the value of series in a scrape of r.
func scrapedValue(t *testing.T, r *Receiver, series string) float64 {
	t.Helper()
	for _, line := range strings.Split(scrape(t, r), "\n") {
		if value, ok := strings.CutPrefix(line, series+" "); ok {
			parsed, err := strconv.ParseFloat(value, 64)
			if err != nil {
				t.Fatal(err)
			}
			return parsed
		}
	}
	t.Fatalf("scrape lacks %s", series)
	return 0
}

// awaitScraped waits up to 15 seconds for series in a scrape of r to equal want, and returns its last value.
func awaitScraped(t *testing.T, r *Receiver, series string, want float64) float64 {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		got := scrapedValue(t, r, series)
		if got == want || time.Now().After(deadline) {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestAcknowledgedAdvertisementSetsItsTime(t *testing.T) {
	f := newFixture(t, resultStream)
	before := float64(time.Now().UnixNano()) / 1e9
	if err := f.receiver.advertise(f.ctx); err != nil {
		t.Fatal(err)
	}
	after := float64(time.Now().UnixNano()) / 1e9
	if at := scrapedValue(t, f.receiver, `wherry_advertised_timestamp_seconds{pool="pool-a"}`); at < before || at > after {
		t.Fatalf("advertised at %f, want between %f and %f", at, before, after)
	}
}

// A run counts past its task_ended_by from that bound until it ends, here held there by a Stop that has not returned.
func TestRunPastItsEndByCountsUntilItEnds(t *testing.T) {
	const series = `wherry_runs_past_deadline{pool="pool-a",profile="nomad"}`
	f := newFixture(t, resultStream)
	release := make(chan struct{})
	f.plugin.endDelay, f.plugin.stopCheck = time.Minute, func() error { <-release; return nil }
	endBy := time.Now().Add(3 * time.Second).Unix()
	deadline := wire.PastBound(endBy)
	done := f.executing(t, f.ctx, f.submit(t, f.host, func(task *wire.Submission) { task.Wherry.TaskStartedBy, task.Wherry.TaskEndedBy = endBy, endBy }), 1)
	for {
		if _, _, follows, _ := f.plugin.counts(); follows == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the run was not followed before its task_ended_by; logs:\n%s", f.logs.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if value := scrapedValue(t, f.receiver, series); value != 0 || !time.Now().Before(deadline) {
		t.Fatalf("%s %v before task_ended_by, want 0", series, value)
	}
	scraped(t, f.receiver, series+" 1\n")
	close(release)
	if status := <-done; status != kgo.AckReject {
		t.Fatalf("status %v, want reject; logs:\n%s", status, f.logs.String())
	}
	if value := scrapedValue(t, f.receiver, series); value != 0 {
		t.Fatalf("%s %v once the run ended, want 0", series, value)
	}
}

// Each run the delivery budget ends counts once, under its tenant and the profile that held it or none.
func TestBudgetEndCountsOncePerTenantAndProfile(t *testing.T) {
	terms := testTerms()
	terms.DeliveryLimit = 2
	f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream}})
	run := testActiveRun(t, f, "run-b", 1)
	if status := run.returnTask(f.ctx, nil); status != kgo.AckReject {
		t.Fatalf("status %v, want reject", status)
	}
	unoffered := f.submit(t, f.host, func(task *wire.Submission) { task.Wherry.Placement.Agent = "codex" })
	if admitted, ack := f.receiver.admit(f.ctx, unoffered); admitted != nil || ack != kgo.AckReject {
		t.Fatalf("admitted=%v ack=%v, want reject", admitted != nil, ack)
	}
	run.attempt--
	if status := run.returnTask(f.ctx, nil); status != kgo.AckRelease {
		t.Fatalf("status %v below the budget, want release", status)
	}
	for _, profile := range []string{"nomad", "none"} {
		series := `wherry_delivery_budget_exhausted_total{pool="pool-a",profile="` + profile + `",tenant="tenant-a"}`
		if value := scrapedValue(t, f.receiver, series); value != 1 {
			t.Fatalf("%s %v, want 1", series, value)
		}
	}
}

// gaugeLines is the sample lines of name in a scrape, sorted, after checking its TYPE line is gauge's.
func gaugeLines(t *testing.T, text, name string) []string {
	t.Helper()
	if !strings.Contains(text, "\n# TYPE "+name+" gauge\n") {
		t.Fatalf("scrape lacks # TYPE %s gauge:\n%s", name, text)
	}
	var lines []string
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, name+"{") || strings.HasPrefix(line, name+" ") {
			lines = append(lines, line)
		}
	}
	slices.Sort(lines)
	return lines
}

// Each profile has one wherry_profile_ready sample naming its signature topic, 1 exactly while it is in the ready
// offers, and the topics at 1 are the subscription.
func TestProfileReadyNamesEachProfilesTopic(t *testing.T) {
	f := signatureFixture(t, 1,
		signatureProfile("a", "claude", map[string]string{"gpu": "1"}),
		signatureProfile("b", "claude", map[string]string{"gpu": "2"}),
		signatureProfile("c", "claude", map[string]string{"gpu": "1"}))
	t1, t2 := f.profileTopic(t, "a"), f.profileTopic(t, "b")
	if t1 == t2 || f.profileTopic(t, "c") != t1 {
		t.Fatalf("topics a %s, b %s, c %s; want a and c to share one and b another", t1, t2, f.profileTopic(t, "c"))
	}
	check := func(a, b, c int) {
		t.Helper()
		text := scrape(t, f.receiver)
		want := []string{
			`wherry_profile_ready{pool="pool-a",profile="a",topic="` + t1 + `"} ` + strconv.Itoa(a),
			`wherry_profile_ready{pool="pool-a",profile="b",topic="` + t2 + `"} ` + strconv.Itoa(b),
			`wherry_profile_ready{pool="pool-a",profile="c",topic="` + t1 + `"} ` + strconv.Itoa(c),
		}
		got := gaugeLines(t, text, "wherry_profile_ready")
		if !slices.Equal(got, want) {
			t.Fatalf("wherry_profile_ready samples\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
		}
		var ready []string
		for _, line := range got {
			if strings.HasSuffix(line, " 1") {
				ready = append(ready, line[strings.Index(line, `topic="`)+len(`topic="`):strings.LastIndex(line, `"}`)])
			}
		}
		slices.Sort(ready)
		f.receiver.mu.Lock()
		subscription := f.receiver.subscription()
		f.receiver.mu.Unlock()
		if !slices.Equal(slices.Compact(ready), subscription) {
			t.Fatalf("topics at 1 %v; subscription %v", ready, subscription)
		}
	}
	f.ready(t, "b", false)
	check(1, 0, 1)
	f.ready(t, "b", true)
	f.ready(t, "a", false)
	check(0, 1, 1)
}

// Each wherry_claim_gated reason is 1 exactly when its operand of claimGated holds, over every combination and through
// the real pending and placed paths.
func TestClaimGatedMatchesTheGate(t *testing.T) {
	r := pluginReceiver(&fakePlugin{})
	offers := []wire.Offer{{Profile: "nomad"}}
	bit := func(set bool) string {
		if set {
			return "1"
		}
		return "0"
	}
	check := func(suspended, noOffer, pending bool) {
		t.Helper()
		text := scrape(t, r)
		want := []string{
			`wherry_claim_gated{pool="pool-a",reason="no_ready_offer"} ` + bit(noOffer),
			`wherry_claim_gated{pool="pool-a",reason="pending_run"} ` + bit(pending),
			`wherry_claim_gated{pool="pool-a",reason="suspended"} ` + bit(suspended),
		}
		if got := gaugeLines(t, text, "wherry_claim_gated"); !slices.Equal(got, want) {
			t.Fatalf("suspended %t, no offer %t, pending %t: wherry_claim_gated samples\n%s\nwant\n%s",
				suspended, noOffer, pending, strings.Join(got, "\n"), strings.Join(want, "\n"))
		}
		r.mu.Lock()
		gated := r.claimGated()
		r.mu.Unlock()
		if want := suspended || noOffer || pending; gated != want {
			t.Fatalf("suspended %t, no offer %t, pending %t: claimGated %t; want %t", suspended, noOffer, pending, gated, want)
		}
	}
	for c := range 8 {
		suspended, noOffer, pending := c&1 != 0, c&2 != 0, c&4 != 0
		r.mu.Lock()
		r.state.Suspended = map[string]bool{testPool: suspended}
		r.offers = offers
		if noOffer {
			r.offers = nil
		}
		r.pending = nil
		if pending {
			r.pending = map[*activeRun]struct{}{{receiver: r}: {}}
		}
		r.mu.Unlock()
		check(suspended, noOffer, pending)
	}

	// Transitions on the one receiver: no background loop runs, so only these steps change its state.
	r.mu.Lock()
	r.state.Suspended, r.offers, r.pending = nil, offers, nil
	r.mu.Unlock()
	check(false, false, false)
	run := &activeRun{receiver: r}
	run.pending()
	check(false, false, true)
	run.placed()
	check(false, false, false)
	for _, present := range []bool{false, true} {
		r.mu.Lock()
		r.offers = nil
		if present {
			r.offers = offers
		}
		r.mu.Unlock()
		check(false, !present, false)
	}
	for _, suspended := range []bool{true, false} {
		r.mu.Lock()
		r.state.Suspended = map[string]bool{testPool: suspended}
		r.mu.Unlock()
		check(suspended, false, false)
	}
}

func zeroSamples(t *testing.T, r *Receiver) map[string]float64 {
	t.Helper()
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(scrape(t, r)))
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]float64{}
	for name, family := range families {
		if !slices.Contains(strings.Fields("claims_total admissions_total rejections_total runs_total adoptions_total redeliveries_total run_duration_seconds queue_wait_seconds open_wait_seconds plugin_placement_seconds start_failures_total dispatch_outcomes_total stop_failures_total purge_failures_total sweep_failures_total collection_failures_total open_waits_total redactions_total bus_errors_total denials_total metrics_dropped_total delivery_budget_exhausted_total claimant_changes_total"), strings.TrimPrefix(name, "wherry_")) {
			continue
		}
		for _, metric := range family.Metric {
			labels := []string{}
			for _, label := range metric.Label {
				labels = append(labels, label.GetName()+"="+strconv.Quote(label.GetValue()))
			}
			sort.Strings(labels)
			key := name + "{" + strings.Join(labels, ",") + "}"
			if metric.Counter != nil {
				out[key] = metric.Counter.GetValue()
			}
			if h := metric.Histogram; h != nil {
				out[key+"/count"] = float64(h.GetSampleCount())
				out[key+"/sum"] = h.GetSampleSum()
				for _, bucket := range h.Bucket {
					out[fmt.Sprintf("%s/bucket/%g", key, bucket.GetUpperBound())] = float64(bucket.GetCumulativeCount())
				}
			}
		}
	}
	return out
}

func zeroExpected(tenants []string) map[string]float64 {
	out := map[string]float64{}
	add := func(family string, labels ...string) {
		labels = append(labels, `pool="pool-a"`)
		sort.Strings(labels)
		key := "wherry_" + family + "{" + strings.Join(labels, ",") + "}"
		if strings.HasSuffix(family, "_seconds") {
			out[key+"/count"] = 0
			out[key+"/sum"] = 0
			buckets := []float64{1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600, 7200, 14400, 21600}
			if family == "plugin_placement_seconds" {
				buckets = []float64{1, 2, 5, 10, 30, 60, 120, 300, 600, 1800}
			}
			for _, b := range append(buckets, math.Inf(1)) {
				out[fmt.Sprintf("%s/bucket/%g", key, b)] = 0
			}
		} else {
			out[key] = 0
		}
	}
	for _, op := range []string{"read", "write", "transactional ID", "share group"} {
		add("bus_errors_total", "op="+strconv.Quote(op))
	}
	for _, op := range []string{"read", "write", "transactional ID", "share group", "membership"} {
		add("denials_total", "op="+strconv.Quote(op))
	}
	for _, reason := range []string{"released", "lease"} {
		add("claimant_changes_total", "reason="+strconv.Quote(reason))
	}
	for _, profile := range []string{"nomad", "second"} {
		pl := "profile=" + strconv.Quote(profile)
		for family, values := range map[string][]string{
			"start_failures_total":      {"input-too-large", "dispatch", "platform"},
			"dispatch_outcomes_total":   {"created", "nothing", "unknown"},
			"collection_failures_total": {"failed", "not_stopped", "over_limits", "timeout"},
			"metrics_dropped_total":     {"timeout", "error", "duplicate", "unknown_family", "unknown_label", "kind", "label_value", "value"},
		} {
			label := "reason"
			if family == "start_failures_total" {
				label = "class"
			}
			if family == "dispatch_outcomes_total" {
				label = "outcome"
			}
			for _, value := range values {
				add(family, pl, label+"="+strconv.Quote(value))
			}
		}
		for _, family := range []string{"stop_failures_total", "purge_failures_total", "sweep_failures_total", "plugin_placement_seconds"} {
			add(family, pl)
		}
	}
	for _, tenant := range tenants {
		tl := "tenant=" + strconv.Quote(tenant)
		add("claims_total", tl)
		add("open_wait_seconds", tl)
		for _, outcome := range []string{"opened", "refused", "expired", "unanswered"} {
			add("open_waits_total", tl, "outcome="+strconv.Quote(outcome))
		}
		for _, reason := range []string{"run ID does not match its signer", "duplicate submission", "agent is outside the pool terms", "model not in the pool's terms", "not started by task_started_by", "not ended by task_ended_by", "agent does not match the task topic", "selector does not match the task topic", "task_started_by is past the pool's start_by_limit", "the host does not supply a required credential of the agent", "a task credential takes the target of one of the agent's credentials", "git remote host outside the pool's vcs hosts", "pool has no storage", "unsupported message part", "the task carries no envelope", "task_started_by is before its append time", "task_ended_by is before task_started_by", "task_ended_by is past task_started_by plus the 6h run cap"} {
			add("rejections_total", tl, "reason="+strconv.Quote(reason))
		}
		for _, profile := range []string{"nomad", "second", ""} {
			pl := "profile=" + strconv.Quote(profile)
			for _, family := range []string{"admissions_total", "redeliveries_total", "queue_wait_seconds"} {
				add(family, tl, pl)
			}
		}
		for _, profile := range []string{"nomad", "second", "none"} {
			pl := "profile=" + strconv.Quote(profile)
			for _, family := range []string{"delivery_budget_exhausted_total", "redactions_total"} {
				add(family, tl, pl)
			}
		}
		for _, profile := range []string{"nomad", "second"} {
			add("adoptions_total", tl, "profile="+strconv.Quote(profile))
		}
		for _, tuple := range [][3]string{{"nomad", "claude", ""}, {"second", "codex", "m1"}, {"second", "codex", "m2"}} {
			for _, state := range []string{"completed", "failed", "canceled"} {
				for _, family := range []string{"runs_total", "run_duration_seconds"} {
					add(family, tl, "profile="+strconv.Quote(tuple[0]), "agent="+strconv.Quote(tuple[1]), "model="+strconv.Quote(tuple[2]), "state="+strconv.Quote(state))
				}
			}
		}
	}
	return out
}

func counterZeroFixture(t *testing.T) fixture {
	t.Helper()
	terms := testTerms()
	terms.Agents[1].Models = []string{"m1", "m2"}
	config := testConfig(testReceiver)
	config.Profiles = append(config.Profiles, operator.ProfileSettings{Name: "second", Plugin: "fake", Agent: "codex", Config: map[string]any{}})
	return newFixtureWith(t, nil, terms, config, map[string]*fakePlugin{"nomad": {}, "second": {}})
}

func TestKnownSeriesAtZero(t *testing.T) {
	f := counterZeroFixture(t)
	for _, tenant := range []string{"tenant-b", "other-pool", "unentitled", "ended"} {
		if err := f.service.TenantAdd(f.ctx, tenant); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.service.PoolApply(f.ctx, "pool-b", testTerms(), false); err != nil {
		t.Fatal(err)
	}
	for _, tuple := range [][2]string{{"tenant-b", testPool}, {"other-pool", "pool-b"}, {"ended", testPool}} {
		if err := f.service.TenantPoolAdd(f.ctx, tuple[0], tuple[1]); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.service.TenantPoolRemove(f.ctx, "ended", testPool); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantRemove(f.ctx, "ended"); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	got, want := zeroSamples(t, f.receiver), zeroExpected([]string{testTenant, "tenant-b"})
	if !reflect.DeepEqual(got, want) {
		for key, value := range want {
			if v, ok := got[key]; !ok || v != value {
				t.Errorf("missing or nonzero %s: %v (exists %v)", key, v, ok)
			}
		}
		for key := range got {
			if _, ok := want[key]; !ok {
				t.Errorf("unexpected %s", key)
			}
		}
	}
}

func TestEntitledAfterStartHasZeroSeries(t *testing.T) {
	f := counterZeroFixture(t)
	if err := f.service.TenantAdd(f.ctx, "later"); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantPoolAdd(f.ctx, "later", testPool); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	got := zeroSamples(t, f.receiver)
	for key, value := range zeroExpected([]string{testTenant, "later"}) {
		if v, ok := got[key]; !ok || v != value {
			t.Fatalf("missing or nonzero %s: %v (exists %v)", key, v, ok)
		}
	}
}

func TestRefreshPreservesMetricValues(t *testing.T) {
	f := counterZeroFixture(t)
	for _, collector := range f.receiver.metrics.collectors {
		ch := make(chan prometheus.Metric)
		go func() { collector.Collect(ch); close(ch) }()
		for metric := range ch {
			if counter, ok := metric.(prometheus.Counter); ok {
				counter.Add(2)
			}
			if histogram, ok := metric.(prometheus.Observer); ok {
				histogram.Observe(2)
			}
		}
	}
	before := zeroSamples(t, f.receiver)
	if !slices.Equal(slices.Sorted(maps.Keys(before)), slices.Sorted(maps.Keys(zeroExpected([]string{testTenant})))) {
		t.Fatal("populated tuples are not the seeded set")
	}
	for range 2 {
		if err := f.receiver.refresh(f.ctx); err != nil {
			t.Fatal(err)
		}
	}
	if got := zeroSamples(t, f.receiver); !reflect.DeepEqual(got, before) {
		t.Fatal("refresh changed existing values or tuples")
	}
	terms := testTerms()
	terms.Agents[1].Models = []string{"m1", "m2", "m3"}
	if err := f.service.PoolApply(f.ctx, testPool, terms, false); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	got := zeroSamples(t, f.receiver)
	for key, value := range before {
		if got[key] != value {
			t.Fatalf("terms change reset %s", key)
		}
	}
	expected := maps.Clone(before)
	for key := range zeroExpected([]string{testTenant}) {
		if strings.Contains(key, `model="m2"`) {
			expected[strings.Replace(key, `model="m2"`, `model="m3"`, 1)] = 0
		}
	}
	if !reflect.DeepEqual(got, expected) {
		t.Fatal("terms change did not add exactly the new zero run tuples")
	}

	scraped(t, f.receiver, `wherry_runs_total{agent="codex",model="m3",pool="pool-a",profile="second",state="completed",tenant="tenant-a"} 0`, `wherry_run_duration_seconds_count{agent="codex",model="m3",pool="pool-a",profile="second",state="completed",tenant="tenant-a"} 0`)
	if err := f.service.TenantPoolRemove(f.ctx, testTenant, testPool); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantAdd(f.ctx, "unseen-ended"); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantPoolAdd(f.ctx, "unseen-ended", testPool); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantPoolRemove(f.ctx, "unseen-ended", testPool); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantRemove(f.ctx, "unseen-ended"); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	if after := zeroSamples(t, f.receiver); !reflect.DeepEqual(after, got) {
		t.Fatal("ended entitlements changed retained series or created ended tenant series")
	}
}

func TestRunFallbackSeriesStayLazy(t *testing.T) {
	f := newFixture(t, nil)
	for key := range zeroSamples(t, f.receiver) {
		if (strings.HasPrefix(key, "wherry_runs_total{") || strings.HasPrefix(key, "wherry_run_duration_seconds{")) && (strings.Contains(key, `profile="none"`) || strings.Contains(key, `="other"`)) {
			t.Fatalf("fallback seeded: %s", key)
		}
	}
	run := testActiveRun(t, f, "budget-zero", 10)
	run.profile = Named{}
	if ack := run.returnTask(f.ctx, nil); ack != kgo.AckReject {
		t.Fatalf("budget ack %v", ack)
	}
	scraped(t, f.receiver, `wherry_runs_total{agent="claude",model="",pool="pool-a",profile="none",state="failed",tenant="tenant-a"} 1`)
	for key := range zeroSamples(t, f.receiver) {
		if strings.HasPrefix(key, "wherry_run_duration_seconds{") && strings.Contains(key, `profile="none"`) {
			t.Fatalf("unclaimed duration created: %s", key)
		}
	}
	run = testActiveRun(t, f, "claimed-budget-zero", 10)
	run.profile = Named{}
	run.claimed = time.Now()
	if ack := run.returnTask(f.ctx, nil); ack != kgo.AckReject {
		t.Fatalf("claimed budget ack %v", ack)
	}
	scraped(t, f.receiver, `wherry_run_duration_seconds_count{agent="claude",model="",pool="pool-a",profile="none",state="failed",tenant="tenant-a"} 1`)
}

func TestFixedMetricDomains(t *testing.T) {
	t.Run("rejections", func(t *testing.T) {
		for _, tc := range []struct {
			reason  string
			edit    func(*wire.Submission)
			special string
		}{
			{"run ID does not match its signer", func(task *wire.Submission) { task.Wherry.RunNonce = testNonce("different") }, ""},
			{"duplicate submission", nil, "duplicate"},
			{"agent is outside the pool terms", func(task *wire.Submission) { task.Wherry.Placement.Agent = "gemini" }, ""},
			{"model not in the pool's terms", func(task *wire.Submission) { task.Wherry.Placement.Model = "missing" }, ""},
			{"not started by task_started_by", func(task *wire.Submission) { task.Wherry.TaskStartedBy = time.Now().Add(-time.Minute).Unix() }, ""},
			{"not ended by task_ended_by", func(task *wire.Submission) { task.Wherry.TaskEndedBy = time.Now().Add(-time.Minute).Unix() }, ""},
			{"agent does not match the task topic", nil, "topic"},
			{"selector does not match the task topic", func(task *wire.Submission) {
				task.Wherry.Placement.Selector = []wire.Requirement{in("zone", "elsewhere")}
			}, "selector"},
			{"task_started_by is past the pool's start_by_limit", func(task *wire.Submission) {
				task.Wherry.TaskStartedBy = time.Now().Add(96 * time.Hour).Unix()
				task.Wherry.TaskEndedBy = task.Wherry.TaskStartedBy
			}, ""},
			{"the host does not supply a required credential of the agent", func(task *wire.Submission) { task.Wherry.Unsupplied = []string{credentialName} }, ""},
			{"a task credential takes the target of one of the agent's credentials", func(task *wire.Submission) {
				task.Wherry.Credentials = []wire.TaskCredential{{Name: "deploy", Env: credentialEnv}}
			}, ""},
			{"git remote host outside the pool's vcs hosts", withGit(wire.GitInput{Remote: "https://git.example.test/repo.git", Ref: "main"}), ""},
			{"pool has no storage", func(task *wire.Submission) {
				task.Wherry.Outputs = []wire.ExpectedOutput{{Name: "out", Storage: wire.StorageS3}}
			}, ""},
			{"unsupported message part", func(task *wire.Submission) { task.Message.Parts = []wire.Part{{URL: "https://example.test/input"}} }, ""},
			{"the task carries no envelope", nil, "envelope"},
			{"task_started_by is before its append time", func(task *wire.Submission) {
				task.Wherry.TaskStartedBy = time.Now().Add(time.Minute).Unix()
				task.Wherry.TaskEndedBy = task.Wherry.TaskStartedBy + 60
			}, "append"},
			{"task_ended_by is before task_started_by", func(task *wire.Submission) {
				task.Wherry.TaskStartedBy = time.Now().Add(time.Minute).Unix()
				task.Wherry.TaskEndedBy = task.Wherry.TaskStartedBy - 30
			}, ""},
			{"task_ended_by is past task_started_by plus the 6h run cap", func(task *wire.Submission) {
				task.Wherry.TaskStartedBy = time.Now().Add(time.Minute).Unix()
				task.Wherry.TaskEndedBy = task.Wherry.TaskStartedBy + int64(operator.MaxRunDurationMS/1000) + 1
			}, ""},
		} {
			t.Run(tc.reason, func(t *testing.T) {
				f := newFixture(t, nil)
				series := `wherry_rejections_total{pool="pool-a",reason=` + strconv.Quote(tc.reason) + `,tenant="tenant-a"}`
				scraped(t, f.receiver, series+" 0")
				var record *kgo.Record
				if tc.special == "envelope" {
					record = f.produce(t, f.tasksTopic(), testRun("run-a"), must(t)(wire.SignRecord(testSubmission("run-a"), f.host)))
				} else {
					record = f.submit(t, f.host, tc.edit)
				}
				switch tc.special {
				case "duplicate":
					accepted := f.submit(t, f.host, func(task *wire.Submission) { task.Message.MessageID = "different" })
					f.admitted(t, accepted)
					f.publishStatus(t, accepted, testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
				case "topic":
					record.Topic = operator.SignatureTopic(testPool, "codex", nil)
				case "selector":
					record.Topic = f.tasksTopic()
				case "append":
					record.Timestamp = record.Timestamp.Add(2 * time.Minute)
				}
				if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckReject {
					t.Fatalf("admitted=%v ack=%v; %s", admitted != nil, ack, f.logs.String())
				}
				scraped(t, f.receiver, series+" 1")
				if tc.reason == "run ID does not match its signer" {
					if err := wire.CheckRunID(wire.SubmissionBinding{}); err == nil || err.Error() != tc.reason {
						t.Fatalf("CheckRunID: %v", err)
					}
				}
			})
		}
	})
	t.Run("dispatch and start", func(t *testing.T) {
		for _, tc := range []struct {
			outcome, class string
			plugin         *fakePlugin
		}{
			{"created", "", &fakePlugin{stream: resultStream}},
			{"nothing", "dispatch", &fakePlugin{outcome: pluginsdk.OutcomeNothingCreated}},
			{"unknown", "dispatch", &fakePlugin{outcome: pluginsdk.OutcomeUnknown, dispatchErr: errors.New("dispatch unresolved")}},
			{"nothing", "input-too-large", &fakePlugin{outcome: pluginsdk.OutcomeNothingCreated, dispatchErr: &pluginsdk.InputTooLargeError{Name: "input"}}},
			{"created", "platform", &fakePlugin{stream: resultStream, neverStarted: true}},
		} {
			t.Run(tc.outcome+tc.class, func(t *testing.T) {
				f := newFixtureWith(t, nil, testTerms(), testConfig(testReceiver), map[string]*fakePlugin{"nomad": tc.plugin})
				series := `wherry_dispatch_outcomes_total{outcome="` + tc.outcome + `",pool="pool-a",profile="nomad"}`
				scraped(t, f.receiver, series+" 0")
				start := `wherry_start_failures_total{class="` + tc.class + `",pool="pool-a",profile="nomad"}`
				if tc.class != "" {
					scraped(t, f.receiver, start+" 0")
				}
				f.execute(t, f.submit(t, f.host, nil), 1)
				scraped(t, f.receiver, series+" 1")
				if tc.class != "" {
					scraped(t, f.receiver, start+" 1")
				}
			})
		}
	})
	t.Run("open outcomes", func(t *testing.T) {
		for _, outcome := range []string{"opened", "refused", "expired", "unanswered"} {
			t.Run(outcome, func(t *testing.T) {
				f := newFixture(t, resultStream)
				series := `wherry_open_waits_total{outcome="` + outcome + `",pool="pool-a",tenant="tenant-a"}`
				scraped(t, f.receiver, series+" 0")
				if outcome != "opened" {
					f.quiet()
				}
				if outcome == "expired" {
					previous := openWait
					openWait = 10 * time.Second
					t.Cleanup(func() { openWait = previous })
					f.execute(t, f.submit(t, f.host, func(task *wire.Submission) { task.Wherry.TaskStartedBy = time.Now().Add(time.Second).Unix() }), 1)
				} else if outcome == "refused" {
					previous := openWait
					openWait = 10 * time.Second
					t.Cleanup(func() { openWait = previous })
					done := f.executing(t, f.ctx, f.submit(t, f.host, nil), 1)
					refusal := &wire.OpenRefusal{Custodian: testReceiver, Request: wire.Digest(f.requestRecord(t, 1).Value), Reason: wire.RefusedTwoClaimants}
					f.produce(t, operator.PoolEventsTopic(testPool), testRun("run-a"), must(t)(wire.SignRecord(refusal, f.receiver.signing)))
					<-done
				} else {
					f.execute(t, f.submit(t, f.host, nil), 1)
				}
				scraped(t, f.receiver, series+" 1")
			})
		}
	})
	t.Run("claimant changes", func(t *testing.T) {
		for _, reason := range []string{"released", "lease"} {
			t.Run(reason, func(t *testing.T) {
				f := newFixture(t, nil)
				f.quiet()
				before := scrape(t, f.receiver)
				for _, excluded := range []string{"live", "unchanged"} {
					if strings.Contains(before, `wherry_claimant_changes_total{pool="pool-a",reason="`+excluded+`"`) {
						t.Fatal("seeded " + excluded)
					}
				}
				series := `wherry_claimant_changes_total{pool="pool-a",reason="` + reason + `"}`
				scraped(t, f.receiver, series+" 0")
				keyB, keyC := f.addPeer(t, "receiver-b", testPool), f.addPeer(t, "receiver-c", testPool)
				record := f.submit(t, f.host, nil)
				f.servedAs(t, record, "receiver-b", keyB, 1)
				if reason == "released" {
					f.advertiseAs(t, "receiver-b", keyB, time.Now())
					f.statusAs(t, record, "receiver-b", keyB, "released", 1)
				} else {
					f.advertiseAs(t, "receiver-b", keyB, time.Now().Add(-claimantLease-time.Minute))
				}
				f.statusAs(t, record, "receiver-c", keyC, "claimed", 2)
				if answer := f.answered(t, f.openAs(t, record, "receiver-c", keyC, 2)); answer != "wrap" {
					t.Fatalf("answer %s", answer)
				}
				scraped(t, f.receiver, series+" 1")
			})
		}
	})

	t.Run("collection", func(t *testing.T) {
		for _, reason := range []string{"failed", "not_stopped", "over_limits", "timeout"} {
			t.Run(reason, func(t *testing.T) {
				f := newFixture(t, nil)
				series := `wherry_collection_failures_total{pool="pool-a",profile="nomad",reason="` + reason + `"}`
				scraped(t, f.receiver, series+" 0")
				run := testActiveRun(t, f, "collect-domain", 1)
				f.plugin.mu.Lock()
				switch reason {
				case "failed":
					f.plugin.collectErr = errors.New("collection failed")
				case "over_limits":
					f.plugin.outputs = []pluginsdk.Output{file("large", "123")}
				case "timeout":
					f.plugin.collectErr = context.DeadlineExceeded
				}
				f.plugin.mu.Unlock()
				terms := testTerms()
				terms.OutputLimit = 1
				run.collect(f.ctx, pluginsdk.Handle{}, reason != "not_stopped", terms)
				scraped(t, f.receiver, series+" 1")
			})
		}
	})
	t.Run("bus", func(t *testing.T) {
		f := newFixture(t, nil)
		before := scrape(t, f.receiver)
		if strings.Contains(before, `wherry_bus_errors_total{op="membership"`) {
			t.Fatal("membership is a bus op")
		}
		for _, op := range []capability{"read", "write", "transactional ID", "share group", "membership"} {
			denial := `wherry_denials_total{op="` + string(op) + `",pool="pool-a"}`
			if !strings.Contains(before, denial+" 0") {
				t.Fatalf("missing zero %s", denial)
			}
			bus := `wherry_bus_errors_total{op="` + string(op) + `",pool="pool-a"}`
			if op != "membership" && !strings.Contains(before, bus+" 0") {
				t.Fatalf("missing zero %s", bus)
			}
			f.receiver.deny(kerr.ClusterAuthorizationFailed, op)
			scraped(t, f.receiver, denial+" 1")
			if op != "membership" {
				scraped(t, f.receiver, bus+" 1")
			}
		}
	})
	t.Run("dropped", func(t *testing.T) {
		f := newFixture(t, nil)
		for _, reason := range []string{"timeout", "error", "duplicate", "unknown_family", "unknown_label", "kind", "label_value", "value"} {
			series := `wherry_metrics_dropped_total{pool="pool-a",profile="nomad",reason="` + reason + `"}`
			scraped(t, f.receiver, series+" 0")
			valid := pluginsdk.Sample{Family: "wherry_plugin_cleanup_pending", Kind: pluginsdk.KindGauge, Value: 1}
			switch reason {
			case "timeout", "error":
				previous := metricsDeadline
				metricsDeadline = time.Millisecond
				if reason == "timeout" {
					f.plugin.mu.Lock()
					f.plugin.metricsHang = true
					f.plugin.mu.Unlock()
				} else {
					f.receiver.profiles[0].Plugin = metricsErrorPlugin{Plugin: f.plugin}
				}
				scrape(t, f.receiver)
				f.plugin.mu.Lock()
				f.plugin.metricsHang = false
				f.plugin.mu.Unlock()
				f.receiver.profiles[0].Plugin = f.plugin
				metricsDeadline = previous
			case "duplicate":
				f.receiver.metrics.pluginMetrics("nomad", []pluginsdk.Sample{valid, valid})
			default:
				switch reason {
				case "unknown_family":
					valid.Family = "unknown"
				case "unknown_label":
					valid.Labels = map[string]string{"unknown": "x"}
				case "kind":
					valid.Kind = pluginsdk.KindCounter
				case "label_value":
					valid.Family = "wherry_plugin_platform_errors_total"
					valid.Kind = pluginsdk.KindCounter
					valid.Labels = map[string]string{"class": "invalid"}
				case "value":
					valid.Value = math.NaN()
				}
				f.receiver.metrics.pluginMetrics("nomad", []pluginsdk.Sample{valid})
			}
			scraped(t, f.receiver, series+" 1")
		}
		if strings.Contains(scrape(t, f.receiver), `reason=""`) {
			t.Fatal("empty dropped reason")
		}
	})
}

type metricsErrorPlugin struct{ pluginsdk.Plugin }

func (p metricsErrorPlugin) Metrics(context.Context) ([]pluginsdk.Sample, error) {
	return nil, errors.New("metrics failed")
}

func TestDuplicateAgentTermsKeepFallbackLazy(t *testing.T) {
	f := counterZeroFixture(t)
	terms := testTerms()
	terms.Agents[1].Models = []string{"m1", "m2"}
	terms.Agents = append(terms.Agents, operator.AgentTerms{Name: "codex", Models: []string{"later-model"}})
	if err := f.service.PoolApply(f.ctx, testPool, terms, false); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.refresh(f.ctx); err != nil {
		t.Fatal(err)
	}
	for key := range zeroSamples(t, f.receiver) {
		if (strings.HasPrefix(key, "wherry_runs_total{") || strings.HasPrefix(key, "wherry_run_duration_seconds{")) && strings.Contains(key, `model="other"`) {
			t.Errorf("fallback seeded from duplicate agent terms: %s", key)
		}
	}
}
