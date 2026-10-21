package receiver

import (
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/adapter"
	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/wire"
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
	f := newFixture(t, []string{`{"type":"assistant","message":"working"}`,
		`{"type":"result","result":"done","is_error":false,"total_cost_usd":0.25,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":3}}`})
	if task := runToTask(t, f); task.Status.State != wire.TaskStateCompleted {
		t.Fatalf("task %#v", task)
	}
	scraped(t, f.receiver,
		`wherry_claims_total{pool="pool-a",tenant="tenant-a"} 1`,
		`wherry_admissions_total{adapter="nomad",pool="pool-a",tenant="tenant-a"} 1`,
		`wherry_queue_wait_seconds_count{adapter="nomad",pool="pool-a",tenant="tenant-a"} 1`,
		`wherry_dispatch_outcomes_total{adapter="nomad",outcome="created",pool="pool-a"} 1`,
		`wherry_seal_waits_total{outcome="sealed",pool="pool-a",tenant="tenant-a"} 1`,
		`wherry_seal_wait_seconds_count{pool="pool-a",tenant="tenant-a"} 1`,
		`wherry_runs_total{adapter="nomad",agent="claude",model="",pool="pool-a",state="completed",tenant="tenant-a"} 1`,
		`wherry_turn_duration_seconds_count{adapter="nomad",agent="claude",model="",pool="pool-a",state="completed",tenant="tenant-a"} 1`,
		`wherry_agent_reported_tokens_total{agent="claude",kind="input",model="",pool="pool-a",tenant="tenant-a"} 10`,
		`wherry_agent_reported_tokens_total{agent="claude",kind="output",model="",pool="pool-a",tenant="tenant-a"} 5`,
		`wherry_agent_reported_tokens_total{agent="claude",kind="cached",model="",pool="pool-a",tenant="tenant-a"} 3`,
		`wherry_agent_reported_cost_usd_total{agent="claude",pool="pool-a",tenant="tenant-a"} 0.25`,
		`wherry_in_flight{adapter="nomad",pool="pool-a"} 0`,
		`wherry_capacity{pool="pool-a"} 1`,
	)
}

// A credentialed S3 run whose agent prints its credential and URL, then fails, on an adapter whose sweep fails with a
// URL in its error, leaves none of that material in a label or value.
func TestScrapeCarriesNoRunMaterial(t *testing.T) {
	s := newStore(t)
	url := presigned(s.endpoint(), wire.S3InputKey(testTenant, "run-a", "dataset.bin"))
	f := s3Fixture(t, s, []string{`{"type":"assistant","message":"token ` + testToken + ` url ` + url + `"}`}, nil)
	f.adapter.mu.Lock()
	f.adapter.ended = &adapter.Terminal{Started: true, ExitCode: 3, Node: "node-secret-7"}
	f.adapter.sweepErr = errors.New("sweep of " + url + " failed")
	f.adapter.mu.Unlock()
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
		`wherry_runs_total{adapter="nomad",agent="claude",model="",pool="pool-a",`,
		`state="failed",tenant="tenant-a"} 1`,
		`wherry_redactions_total{adapter="nomad",pool="pool-a",tenant="tenant-a"} `,
		`wherry_sweep_failures_total{adapter="nomad",pool="pool-a"} `,
	)
	host := strings.TrimPrefix(s.endpoint(), "https://")
	for _, forbidden := range []string{"run-a", credentialName, testToken, host, s3Bucket, s3Signature, "X-Amz", "dataset.bin", "report.pdf", "agent ended without a result", "node-secret-7", testHost} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("scrape holds %q:\n%s", forbidden, text)
		}
	}
}

// adapterReceiver is a configured receiver with one fake adapter, enough to scrape its adapter families.
func adapterReceiver(fake *fakeAdapter) *Receiver {
	r := &Receiver{config: admin.ReceiverConfig{Pool: testPool}, adapters: []Named{{Name: "nomad", Adapter: fake}}, metrics: newMetrics(prometheus.NewRegistry())}
	r.metrics.registerFor(r)
	return r
}

func placement(buckets ...uint64) adapter.Histogram {
	return adapter.Histogram{Buckets: buckets, Sum: 12.5, Count: buckets[len(buckets)-1]}
}

func TestAdapterSamplesAreValidated(t *testing.T) {
	r := adapterReceiver(&fakeAdapter{samples: []adapter.Sample{
		{Family: "wherry_adapter_platform_errors_total", Labels: map[string]string{"class": "denied"}, Kind: adapter.KindCounter, Value: 4},
		{Family: "wherry_adapter_cleanup_pending", Kind: adapter.KindGauge, Value: 2},
		{Family: "wherry_adapter_sweep_deletions_total", Labels: map[string]string{"kind": "variable", "adapter": "nomad"}, Kind: adapter.KindCounter, Value: 7},
		{Family: "wherry_adapter_placement_seconds", Kind: adapter.KindHistogram, Histogram: placement(0, 1, 2, 2, 3, 3, 3, 3, 3, 3)},
		{Family: "wherry_adapter_jobs_total", Kind: adapter.KindCounter, Value: 11},
		{Family: "wherry_adapter_cleanup_pending", Labels: map[string]string{"node": "node-1"}, Kind: adapter.KindGauge, Value: 12},
		{Family: "wherry_adapter_platform_errors_total", Labels: map[string]string{"class": "run-a"}, Kind: adapter.KindCounter, Value: 13},
		{Family: "wherry_adapter_sweep_deletions_total", Labels: map[string]string{"kind": "job"}, Kind: adapter.KindGauge, Value: 14},
	}})
	text := scraped(t, r,
		`wherry_adapter_platform_errors_total{adapter="nomad",class="denied",pool="pool-a"} 4`,
		`wherry_adapter_cleanup_pending{adapter="nomad",pool="pool-a"} 2`,
		`wherry_adapter_sweep_deletions_total{adapter="nomad",kind="variable",pool="pool-a"} 7`,
		`wherry_adapter_placement_seconds_bucket{adapter="nomad",pool="pool-a",le="2"} 1`,
		`wherry_adapter_placement_seconds_count{adapter="nomad",pool="pool-a"} 3`,
		`wherry_metrics_dropped_total{adapter="nomad",pool="pool-a",reason="unknown_family"} `,
		`wherry_metrics_dropped_total{adapter="nomad",pool="pool-a",reason="unknown_label"} `,
		`wherry_metrics_dropped_total{adapter="nomad",pool="pool-a",reason="label_value"} `,
		`wherry_metrics_dropped_total{adapter="nomad",pool="pool-a",reason="kind"} `,
	)
	for _, dropped := range []string{"wherry_adapter_jobs_total", "node-1", "run-a", "} 11\n", "} 12\n", "} 13\n", "} 14\n"} {
		if strings.Contains(text, dropped) {
			t.Fatalf("scrape holds the dropped %q:\n%s", dropped, text)
		}
	}
}

func TestAdapterSampleCannotSpeakForAnotherAdapter(t *testing.T) {
	r := adapterReceiver(&fakeAdapter{samples: []adapter.Sample{
		{Family: "wherry_adapter_cleanup_pending", Labels: map[string]string{"adapter": "other"}, Kind: adapter.KindGauge, Value: 5},
	}})
	text := scraped(t, r, `wherry_adapter_cleanup_pending{adapter="nomad",pool="pool-a"} 5`)
	if strings.Contains(text, `adapter="other"`) {
		t.Fatalf("scrape holds another adapter's sample:\n%s", text)
	}
}

func TestHungAdapterMetricsServeTheLastGoodSamples(t *testing.T) {
	previous := metricsDeadline
	metricsDeadline = 200 * time.Millisecond
	t.Cleanup(func() { metricsDeadline = previous })
	fake := &fakeAdapter{samples: []adapter.Sample{{Family: "wherry_adapter_cleanup_pending", Kind: adapter.KindGauge, Value: 3}}}
	r := adapterReceiver(fake)
	scraped(t, r, `wherry_adapter_cleanup_pending{adapter="nomad",pool="pool-a"} 3`)
	fake.mu.Lock()
	fake.metricsHang, fake.samples = true, nil
	fake.mu.Unlock()
	for range 2 {
		began := time.Now()
		done := make(chan string, 1)
		go func() { done <- scrape(t, r) }()
		select {
		case text := <-done:
			if !strings.Contains(text, `wherry_adapter_cleanup_pending{adapter="nomad",pool="pool-a"} 3`) {
				t.Fatalf("scrape of a hung adapter lacks its last good sample:\n%s", text)
			}
			if elapsed := time.Since(began); elapsed > time.Second {
				t.Fatalf("scrape of a hung adapter took %v", elapsed)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("scrape of a hung adapter did not return")
		}
	}
	scraped(t, r, `wherry_metrics_dropped_total{adapter="nomad",pool="pool-a",reason="timeout"} `)
}

// A turn whose agent the pool's terms do not name, such as an interrupted turn ended after the terms changed, counts
// under agent other.
func TestAgentOutsideTheTermsIsLabelledOther(t *testing.T) {
	f := newFixture(t, resultStream)
	run := testActiveRun(t, f, "run-x", 1)
	run.task.Wherry.Placement.Agent = "gemini"
	run.ended(wire.TaskStateCompleted, []byte(`{"agent_result":{"usage":{"input_tokens":4}}}`))
	text := scraped(t, f.receiver,
		`wherry_runs_total{adapter="nomad",agent="other",model="",pool="pool-a",state="completed",tenant="tenant-a"} 1`,
		`wherry_agent_reported_tokens_total{agent="other",kind="input",model="",pool="pool-a",tenant="tenant-a"} 4`,
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
	f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakeAdapter{"nomad": {stream: resultStream}})
	for _, model := range []string{"sonnet", "opus"} {
		run := testActiveRun(t, f, "run-"+model, 1)
		run.task.Wherry.Placement.Model = model
		run.ended(wire.TaskStateCompleted, []byte(`{"agent_result":{"usage":{"input_tokens":4}}}`))
	}
	text := scraped(t, f.receiver,
		`wherry_runs_total{adapter="nomad",agent="claude",model="sonnet",pool="pool-a",state="completed",tenant="tenant-a"} 1`,
		`wherry_runs_total{adapter="nomad",agent="claude",model="other",pool="pool-a",state="completed",tenant="tenant-a"} 1`,
		`wherry_agent_reported_tokens_total{agent="claude",kind="input",model="other",pool="pool-a",tenant="tenant-a"} 4`,
	)
	if strings.Contains(text, "opus") {
		t.Fatalf("scrape holds the model outside the terms:\n%s", text)
	}
}

// A state outside wire's task states counts under state other.
func TestStateOutsideTheTaskStatesIsLabelledOther(t *testing.T) {
	f := newFixture(t, resultStream)
	testActiveRun(t, f, "run-x", 1).ended(wire.TaskState("TASK_STATE_RUN_A"), nil)
	text := scraped(t, f.receiver, `wherry_runs_total{adapter="nomad",agent="claude",model="",pool="pool-a",state="other",tenant="tenant-a"} 1`)
	if strings.Contains(text, "run_a") {
		t.Fatalf("scrape holds the unknown state:\n%s", text)
	}
}

func TestRejectionReasonsThatQuoteTheTaskAreFixed(t *testing.T) {
	terms := testTerms()
	terms.InlineLimit = 100
	f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakeAdapter{"nomad": {stream: resultStream}})
	f.rejected(t, f.submit(t, f.host, func(task *wire.Submission) {
		task.Message.Parts = append(task.Message.Parts, wire.Part{Raw: make(wire.StdBytes, 101), Filename: "data.bin"})
	}), "inline inputs are 101 bytes, over the 100-byte limit")
	f.rejected(t, f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Run, task.Message.TaskID, task.Message.ContextID = "run-b", "run-b", "run-b"
		withGit(wire.GitInput{Remote: "https://git.example.test/org/repo.git", Ref: "main"})(task)
	}), "git remote host git.example.test is not in pool "+testPool+"'s vcs hosts")
	text := scraped(t, f.receiver,
		`wherry_rejections_total{pool="pool-a",reason="invalid inputs",tenant="tenant-a"} 1`,
		`wherry_rejections_total{pool="pool-a",reason="git remote host outside the pool's vcs hosts",tenant="tenant-a"} 1`,
	)
	for _, forbidden := range []string{"bytes", "101", "data.bin", "git.example.test", "run-a", "run-b"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("scrape holds %q:\n%s", forbidden, text)
		}
	}
}
