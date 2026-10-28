package receiver

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/benemon/wherry/pluginsdk"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v3"
)

var observability = filepath.Join("..", "..", "contrib", "observability")

// hostSeries are the wherry_ series the receiver process registers beside the receiver's families;
// TestHostSeries in cmd/wherry pins them to its registries.
var hostSeries = []string{"wherry_build_info", "wherry_plugin_restarts_total"}

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

// shippedSeries is every wherry_ series a receiver process can export: the receiver's core and plugin families, a
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
		if len(names) < 10 {
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
