package receiver

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/benemon/wherry/pluginsdk"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v3"
)

var observability = filepath.Join("..", "..", "contrib", "observability")

// hostSeries are the wherry_ series the receiver process registers beside the receiver's families;
// TestHostSeries in cmd/wherry pins them to its registries.
var hostSeries = []string{"wherry_build_info"}

var fqName = regexp.MustCompile(`fqName: "([^"]+)"`)

// describedName is the name desc describes.
func describedName(t *testing.T, desc *prometheus.Desc) string {
	t.Helper()
	match := fqName.FindStringSubmatch(desc.String())
	if match == nil {
		t.Fatalf("no name in %s", desc)
	}
	return match[1]
}

// shippedSeries is every wherry_ series a receiver process can export: the receiver's core, routing and plugin families, a
// histogram's as its _bucket, _sum and _count series, and hostSeries.
func shippedSeries(t *testing.T) map[string]bool {
	t.Helper()
	names := map[string]bool{}
	add := func(name string, histogram bool) {
		if histogram {
			names[name+"_bucket"], names[name+"_sum"], names[name+"_count"] = true, true, true
		} else {
			names[name] = true
		}
	}
	m := newMetrics(nil)
	for _, collector := range m.collectors {
		_, histogram := collector.(*prometheus.HistogramVec)
		descs := make(chan *prometheus.Desc, 1)
		go func() {
			collector.Describe(descs)
			close(descs)
		}()
		for desc := range descs {
			add(describedName(t, desc), histogram)
		}
	}
	for _, family := range pluginFamilies {
		add(describedName(t, m.pluginDescs[family.name]), family.kind == pluginsdk.KindHistogram)
	}
	for _, desc := range []*prometheus.Desc{m.profileReady, m.claimGated} {
		add(describedName(t, desc), false)
	}
	for _, name := range hostSeries {
		names[name] = true
	}
	if len(names) < 40 {
		t.Fatalf("found %d series; the describe found too few to check", len(names))
	}
	return names
}

// walkStrings calls visit with every string in a decoded JSON or YAML document.
func walkStrings(value any, visit func(string)) {
	switch value := value.(type) {
	case string:
		visit(value)
	case []any:
		for _, item := range value {
			walkStrings(item, visit)
		}
	case map[string]any:
		for key, item := range value {
			visit(key)
			walkStrings(item, visit)
		}
	}
}

var seriesName = regexp.MustCompile(`(?:^|[^A-Za-z0-9_:])(wherry_[A-Za-z0-9_:]+)`)

// referenced returns the wherry_ series names document's strings hold.
func referenced(document any) map[string]bool {
	names := map[string]bool{}
	walkStrings(document, func(text string) {
		for _, match := range seriesName.FindAllStringSubmatch(text, -1) {
			names[match[1]] = true
		}
	})
	return names
}

func readDocument(t *testing.T, name string, decode func([]byte, any) error) any {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(observability, name))
	if err != nil {
		t.Fatal(err)
	}
	var document any
	if err := decode(data, &document); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return document
}

// Every series the shipped dashboard and alert rules name is one a receiver exports, so renaming a metric fails here.
func TestObservabilityNamesShippedSeries(t *testing.T) {
	shipped := shippedSeries(t)
	for name, decode := range map[string]func([]byte, any) error{
		"wherry-dashboard.json": json.Unmarshal,
		"wherry-alerts.yaml":    yaml.Unmarshal,
	} {
		names := referenced(readDocument(t, name, decode))
		if len(names) < 5 {
			t.Errorf("%s names %d wherry_ series; the parse found too few to check", name, len(names))
		}
		for series := range names {
			if !shipped[series] {
				t.Errorf("%s names %s, which no receiver exports", name, series)
			}
		}
	}
}

// Every panel and query of the dashboard reads the data source its variable picks, so it names no installation's.
func TestDashboardUsesTheDatasourceVariable(t *testing.T) {
	document := readDocument(t, "wherry-dashboard.json", json.Unmarshal)
	var check func(any)
	check = func(value any) {
		switch value := value.(type) {
		case []any:
			for _, item := range value {
				check(item)
			}
		case map[string]any:
			for key, item := range value {
				if key == "datasource" {
					source, _ := item.(map[string]any)
					if source["uid"] != "${datasource}" {
						t.Errorf("datasource %v is not the variable", item)
					}
					continue
				}
				check(item)
			}
		}
	}
	check(document)
}

// Every alert rule names its runbook, a section of docs/runbooks.md.
func TestAlertRulesNameTheirRunbooks(t *testing.T) {
	page, err := os.ReadFile(filepath.Join("..", "..", "docs", "runbooks.md"))
	if err != nil {
		t.Fatal(err)
	}
	sections := map[string]bool{}
	for _, line := range strings.Split(string(page), "\n") {
		if heading, ok := strings.CutPrefix(line, "## "); ok {
			sections[strings.ToLower(heading)] = true
		}
	}
	var rules struct {
		Groups []struct {
			Rules []struct {
				Alert       string            `yaml:"alert"`
				Expr        string            `yaml:"expr"`
				Annotations map[string]string `yaml:"annotations"`
			} `yaml:"rules"`
		} `yaml:"groups"`
	}
	data, err := os.ReadFile(filepath.Join(observability, "wherry-alerts.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(data, &rules); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, group := range rules.Groups {
		for _, rule := range group.Rules {
			count++
			want := "https://benemon.github.io/wherry/runbooks#" + strings.ToLower(rule.Alert)
			if rule.Alert == "" || rule.Expr == "" || rule.Annotations["runbook_url"] != want {
				t.Errorf("rule %q has runbook_url %q; want %q", rule.Alert, rule.Annotations["runbook_url"], want)
			}
			if !sections[strings.ToLower(rule.Alert)] {
				t.Errorf("docs/runbooks.md has no section for %s", rule.Alert)
			}
		}
	}
	if count == 0 {
		t.Fatal("no alert rules")
	}
}

var (
	takeoverSelector = regexp.MustCompile(`increase\(wherry_custodian_takeovers_total(\{[^}]*\})?\[[^\]]+\]\)\)\s*>\s*0\s*$`)
	labelMatcher     = regexp.MustCompile(`(\w+)\s*(=~|!~|!=|=)\s*"([^"]*)"`)
)

// WherryCustodianTakeover fires on any increase of wherry_custodian_takeovers_total for either reason it counts.
func TestCustodianTakeoverAlertFiresOnEitherReason(t *testing.T) {
	var rules struct {
		Groups []struct {
			Rules []struct {
				Alert string `yaml:"alert"`
				Expr  string `yaml:"expr"`
			} `yaml:"rules"`
		} `yaml:"groups"`
	}
	data, err := os.ReadFile(filepath.Join(observability, "wherry-alerts.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(data, &rules); err != nil {
		t.Fatal(err)
	}
	var expr string
	for _, group := range rules.Groups {
		for _, rule := range group.Rules {
			if rule.Alert == "WherryCustodianTakeover" {
				expr = rule.Expr
			}
		}
	}
	selector := takeoverSelector.FindStringSubmatch(strings.TrimSpace(expr))
	if selector == nil {
		t.Fatalf("WherryCustodianTakeover expr %q, want an increase of wherry_custodian_takeovers_total above 0", expr)
	}
	for _, reason := range []string{"takeover", "contested"} {
		for _, matcher := range labelMatcher.FindAllStringSubmatch(selector[1], -1) {
			if matcher[1] != "reason" {
				continue
			}
			matches := regexp.MustCompile("^(?:" + matcher[3] + ")$").MatchString(reason)
			switch matcher[2] {
			case "=":
				matches = reason == matcher[3]
			case "!=":
				matches = reason != matcher[3]
			case "!~":
				matches = !matches
			}
			if !matches {
				t.Errorf("WherryCustodianTakeover expr %q does not fire on reason %s", expr, reason)
			}
		}
	}
}

var keyLostExpr = regexp.MustCompile(`increase\(wherry_runs_key_lost_total\[[^\]]+\]\)\)\s*>\s*0\s*$`)

// WherryRunsKeyLost fires on any increase of wherry_runs_key_lost_total.
func TestRunsKeyLostAlertFiresOnAnyIncrease(t *testing.T) {
	var rules struct {
		Groups []struct {
			Rules []struct {
				Alert string `yaml:"alert"`
				Expr  string `yaml:"expr"`
			} `yaml:"rules"`
		} `yaml:"groups"`
	}
	data, err := os.ReadFile(filepath.Join(observability, "wherry-alerts.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(data, &rules); err != nil {
		t.Fatal(err)
	}
	var expr string
	for _, group := range rules.Groups {
		for _, rule := range group.Rules {
			if rule.Alert == "WherryRunsKeyLost" {
				expr = rule.Expr
			}
		}
	}
	if !keyLostExpr.MatchString(strings.TrimSpace(expr)) {
		t.Fatalf("WherryRunsKeyLost expr %q, want any increase of wherry_runs_key_lost_total above 0", expr)
	}
}

// routingSubstitution is the one fixed substitution routing.test.yml's exprs make of the dashboard's variables.
var routingSubstitution = strings.NewReplacer(`$pool`, `pool-a`, `$instance`, `r1|r2`, `$__rate_interval`, `5m`)

// Every dashboard query of the routing families is, after routingSubstitution, the expr of a promtool test in
// routing.test.yml that expects samples, so changing one in the dashboard alone fails here; the Routing row's seven
// panels must each keep their query.
func TestDashboardRoutingQueriesAreTested(t *testing.T) {
	var dashboard struct {
		Panels []struct {
			ID      int    `json:"id"`
			Title   string `json:"title"`
			Targets []struct {
				RefID string `json:"refId"`
				Expr  string `json:"expr"`
			} `json:"targets"`
		} `json:"panels"`
	}
	data, err := os.ReadFile(filepath.Join(observability, "wherry-dashboard.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &dashboard); err != nil {
		t.Fatal(err)
	}
	var tests struct {
		RuleFiles []string `yaml:"rule_files"`
		Tests     []struct {
			PromQL []struct {
				Expr       string `yaml:"expr"`
				ExpSamples *[]any `yaml:"exp_samples"`
			} `yaml:"promql_expr_test"`
		} `yaml:"tests"`
	}
	data, err = os.ReadFile(filepath.Join(observability, "routing.test.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(data, &tests); err != nil {
		t.Fatal(err)
	}
	if len(tests.RuleFiles) != 0 {
		t.Errorf("routing.test.yml loads rule files %v; its tests evaluate the dashboard's queries alone", tests.RuleFiles)
	}
	tested := map[string]bool{}
	for _, test := range tests.Tests {
		for _, query := range test.PromQL {
			if query.ExpSamples != nil && len(*query.ExpSamples) > 0 {
				tested[query.Expr] = true
			}
		}
	}
	routing := map[[2]string]bool{}
	exprs := map[string]int{}
	for _, panel := range dashboard.Panels {
		for _, target := range panel.Targets {
			if panel.ID >= 37 && panel.ID <= 43 && target.Expr != "" {
				key := [2]string{strconv.Itoa(panel.ID), target.RefID}
				if routing[key] {
					t.Errorf("panel %d has two targets %s", panel.ID, target.RefID)
				}
				routing[key] = true
				if other, ok := exprs[target.Expr]; ok {
					t.Errorf("panels %d and %d have the same query", other, panel.ID)
				}
				exprs[target.Expr] = panel.ID
			}
			if !strings.Contains(target.Expr, "wherry_profile_ready") && !strings.Contains(target.Expr, "wherry_claim_gated") {
				continue
			}
			expr := routingSubstitution.Replace(target.Expr)
			if strings.Contains(expr, "$") {
				t.Errorf("panel %q: %s leaves a variable after the substitution", panel.Title, expr)
				continue
			}
			if !tested[expr] {
				t.Errorf("panel %q: no routing.test.yml test with exp_samples evaluates %s", panel.Title, expr)
			}
		}
	}
	for id := 37; id <= 43; id++ {
		if !routing[[2]string{strconv.Itoa(id), "A"}] {
			t.Errorf("Routing panel %d has no query A", id)
		}
	}
}
