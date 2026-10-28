package receiver

import (
	"errors"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
		`wherry_seal_waits_total{outcome="sealed",pool="pool-a",tenant="tenant-a"} 1`,
		`wherry_seal_wait_seconds_count{pool="pool-a",tenant="tenant-a"} 1`,
		`wherry_runs_total{agent="claude",model="",pool="pool-a",profile="nomad",state="completed",tenant="tenant-a"} 1`,
		`wherry_run_duration_seconds_count{agent="claude",model="",pool="pool-a",profile="nomad",state="completed",tenant="tenant-a"} 1`,
		`wherry_in_flight{pool="pool-a",profile="nomad"} 0`,
		`wherry_capacity{pool="pool-a"} 1`,
	)
}

// SeriesNames names every family the receiver registers, so the shipped dashboard and rules are checked against all of
// them.
func TestSeriesNamesCoverEveryFamily(t *testing.T) {
	m := newMetrics(nil)
	descs := make(chan *prometheus.Desc, 64)
	go func() {
		for _, collector := range m.collectors {
			collector.Describe(descs)
		}
		for _, desc := range m.pluginDescs {
			descs <- desc
		}
		close(descs)
	}()
	names := SeriesNames()
	fqName := regexp.MustCompile(`fqName: "([^"]+)"`)
	count := 0
	for desc := range descs {
		count++
		match := fqName.FindStringSubmatch(desc.String())
		if match == nil {
			t.Fatalf("no name in %s", desc)
		}
		if !slices.Contains(names, match[1]) && !slices.Contains(names, match[1]+"_count") {
			t.Errorf("SeriesNames lacks %s", match[1])
		}
	}
	if count < 30 {
		t.Fatalf("described %d families", count)
	}
}

// A credentialed S3 run whose agent prints its credential and URL, then fails, on a plugin whose sweep fails with a
// URL in its error, leaves none of that material in a label or value.
func TestScrapeCarriesNoRunMaterial(t *testing.T) {
	s := newStore(t)
	url := presigned(s.endpoint(), wire.S3InputKey(testTenant, "run-a", "dataset.bin"))
	f := s3Fixture(t, s, []string{`{"type":"assistant","message":"token ` + testToken + ` url ` + url + `"}`}, nil)
	f.plugin.mu.Lock()
	f.plugin.ended = &pluginsdk.Terminal{Started: true, ExitCode: 3, Node: "node-secret-7"}
	f.plugin.sweepErr = errors.New("sweep of " + url + " failed")
	f.plugin.mu.Unlock()
	if err := f.receiver.advertise(f.ctx); err != nil {
		t.Fatal(err)
	}
	record := f.submit(t, f.host, withS3)
	done := f.executing(t, f.ctx, record, 1)
	f.sealS3(t, validURLs(s.endpoint()))
	if status := <-done; status != kgo.AckReject {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	text := scraped(t, f.receiver,
		`wherry_runs_total{agent="claude",model="",pool="pool-a",profile="nomad",`,
		`state="failed",tenant="tenant-a"} 1`,
		`wherry_redactions_total{pool="pool-a",profile="nomad",tenant="tenant-a"} `,
		`wherry_sweep_failures_total{pool="pool-a",profile="nomad"} `,
	)
	host := strings.TrimPrefix(s.endpoint(), "https://")
	for _, forbidden := range []string{"run-a", credentialName, testToken, host, s3Bucket, s3Signature, "X-Amz", "dataset.bin", "report.pdf", "agent reported no outcome", "node-secret-7", testHost} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("scrape holds %q:\n%s", forbidden, text)
		}
	}
}

// pluginReceiver is a configured receiver with one fake plugin, enough to scrape its plugin families.
func pluginReceiver(fake *fakePlugin) *Receiver {
	r := &Receiver{config: admin.ReceiverConfig{Pool: testPool}, profiles: []Named{{Name: "nomad", Plugin: fake}}, metrics: newMetrics(prometheus.NewRegistry())}
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
		{Family: "wherry_plugin_jobs_total", Kind: pluginsdk.KindCounter, Value: 11},
		{Family: "wherry_plugin_cleanup_pending", Labels: map[string]string{"node": "node-1"}, Kind: pluginsdk.KindGauge, Value: 12},
		{Family: "wherry_plugin_platform_errors_total", Labels: map[string]string{"class": "run-a"}, Kind: pluginsdk.KindCounter, Value: 13},
		{Family: "wherry_plugin_sweep_deletions_total", Labels: map[string]string{"kind": "job"}, Kind: pluginsdk.KindGauge, Value: 14},
	}})
	text := scraped(t, r,
		`wherry_plugin_platform_errors_total{class="denied",pool="pool-a",profile="nomad"} 4`,
		`wherry_plugin_cleanup_pending{pool="pool-a",profile="nomad"} 2`,
		`wherry_plugin_sweep_deletions_total{kind="variable",pool="pool-a",profile="nomad"} 7`,
		`wherry_plugin_placement_seconds_bucket{pool="pool-a",profile="nomad",le="2"} 1`,
		`wherry_plugin_placement_seconds_count{pool="pool-a",profile="nomad"} 3`,
		`wherry_metrics_dropped_total{pool="pool-a",profile="nomad",reason="unknown_family"} `,
		`wherry_metrics_dropped_total{pool="pool-a",profile="nomad",reason="unknown_label"} `,
		`wherry_metrics_dropped_total{pool="pool-a",profile="nomad",reason="label_value"} `,
		`wherry_metrics_dropped_total{pool="pool-a",profile="nomad",reason="kind"} `,
	)
	for _, dropped := range []string{"wherry_plugin_jobs_total", "node-1", "run-a", "} 11\n", "} 12\n", "} 13\n", "} 14\n"} {
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
		sample := pluginsdk.Sample{Family: family.name, Kind: family.kind, Value: 1, Histogram: placement(0, 0, 0, 0, 0, 0, 0, 0, 0, 1)}
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
	for _, forbidden := range []string{"git.example.test", "run-a"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("scrape holds %q:\n%s", forbidden, text)
		}
	}
}
