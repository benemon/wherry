package operator

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/benemon/wherry/internal/wire"
)

// The Strimzi ignored-users pattern docs/operations-operator.md gives matches every principal wherry creates through the Admin
// API, and no other.
func TestStrimziIgnoredUsersPattern(t *testing.T) {
	doc, err := os.ReadFile(filepath.Join("..", "..", "docs", "operations-operator.md"))
	if err != nil {
		t.Fatal(err)
	}
	_, section, ok := strings.Cut(string(doc), "\n### Strimzi\n")
	if !ok {
		t.Fatal("docs/operations-operator.md has no Strimzi section")
	}
	_, block, ok := strings.Cut(section, "```\n")
	if !ok {
		t.Fatal("the Strimzi section has no code block")
	}
	pattern, _, _ := strings.Cut(block, "\n")
	ignored, err := regexp.Compile(pattern)
	if err != nil {
		t.Fatalf("pattern %q: %v", pattern, err)
	}
	for _, principal := range []string{OperatorPrincipal, TenantPrincipal("acme", "laptop"), ReceiverPrincipal("nomad-1")} {
		if !ignored.MatchString(principal) {
			t.Errorf("pattern %q does not match %s", pattern, principal)
		}
	}
	for _, principal := range []string{"kafka-admin", "wherry.operator.x", "wherry.tnt.", "wherry.rcv.", "xwherry.operator"} {
		if ignored.MatchString(principal) {
			t.Errorf("pattern %q matches %s, which wherry does not create", pattern, principal)
		}
	}
}

// A signature names the agent and the routable labels a profile carries, whatever their order, and nothing else; its
// topic is the one pattern every reader parses. The digests are sha256sum's of the literal encodings.
func TestSignatureTopic(t *testing.T) {
	terms := Terms{Routable: []string{"platform", "isolation"}}
	reordered := Terms{Routable: []string{"isolation", "platform"}}
	labels := map[string]string{"platform": "nomad", "isolation": "container", "gpu": "a100"}
	const both = "claude\nisolation=container\nplatform=nomad\n"
	if got := Signature(terms, "claude", labels); got != both {
		t.Fatalf("signature %q, want %q", got, both)
	}
	want := "wherry.pool.pool-a.tasks.claude.79c0eb55fa00d7e5903d9fa0f9f51ec162666978ff2938be19fd6e08b169833a"
	for _, other := range []map[string]string{labels, {"isolation": "container", "platform": "nomad", "gpu": "h100"}, {"isolation": "container", "platform": "nomad"}} {
		for _, routable := range []Terms{terms, reordered} {
			if got := SignatureTopic("pool-a", routable, "claude", other); got != want {
				t.Fatalf("routable %v, labels %v: topic %s, want %s", routable.Routable, other, got, want)
			}
		}
	}
	if !slices.Equal(terms.Routable, []string{"platform", "isolation"}) {
		t.Fatalf("routable keys sorted in place: %v", terms.Routable)
	}
	for _, fixture := range []struct {
		terms  Terms
		agent  string
		labels map[string]string
		topic  string
	}{
		{Terms{}, "codex", labels, "wherry.pool.pool-a.tasks.codex.243b0dc9b847e66c440dca985e10fe0ce9e29c379b018ddd5747ba8948f84cc8"},
		{terms, "claude", map[string]string{"platform": "nomad", "gpu": "a100"}, "wherry.pool.pool-a.tasks.claude.ef900330120d94dc0ea8292aaff80030aa744d924e6723016308bd36255e19a4"},
		{Terms{Routable: []string{"platform", "isolation", "gpu"}}, "claude", labels, "wherry.pool.pool-a.tasks.claude.e5bf4f20966c9016ba8c6d304637749604d2d1536b7a4c915e9109146cdd4618"},
	} {
		if got := SignatureTopic("pool-a", fixture.terms, fixture.agent, fixture.labels); got != fixture.topic {
			t.Errorf("routable %v, agent %s: topic %s, want %s", fixture.terms.Routable, fixture.agent, got, fixture.topic)
		}
		if pool, agent, ok := wire.ParseTaskTopic(fixture.topic); !ok || pool != "pool-a" || agent != fixture.agent {
			t.Errorf("%s parses to %q, %q, %v", fixture.topic, pool, agent, ok)
		}
		if !strings.HasPrefix(fixture.topic, PoolTasksPrefix("pool-a")) {
			t.Errorf("%s lacks the pool's prefix", fixture.topic)
		}
	}
	long := strings.Repeat("p", 32)
	topic := SignatureTopic(long, terms, strings.Repeat("a", 32), labels)
	if len(topic) != 148 {
		t.Fatalf("topic at the longest names is %d characters, want 148", len(topic))
	}
	if _, _, ok := wire.ParseTaskTopic(topic); !ok {
		t.Fatalf("%s does not parse", topic)
	}
	hex := strings.Repeat("a", 64)
	for _, refused := range []string{
		"wherry.pool.pool-a.tasks.claude." + hex[:63],
		"wherry.pool.pool-a.tasks.claude." + hex + "a",
		"wherry.pool.pool-a.tasks.claude." + strings.ToUpper(hex),
		"wherry.pool.Pool-a.tasks.claude." + hex,
		"wherry.pool.pool-a.tasks.-claude." + hex,
		"wherry.pool.pool.a.tasks.claude." + hex,
		"wherry.pool.pool-a.tasks.claude.x." + hex,
		"wherry.pool.pool-a.tasks",
		"wherry.pool.pool-a.tasks.",
		".", "..",
	} {
		if pool, agent, ok := wire.ParseTaskTopic(refused); ok {
			t.Errorf("%q parses to %q, %q", refused, pool, agent)
		}
	}
}

func TestRoutableLabelsAreUniqueValidKeys(t *testing.T) {
	terms := modelTerms()
	terms.Routable = []string{"platform", "example.com/isolation"}
	if err := validateTerms(terms); err != nil {
		t.Fatalf("valid routable labels refused: %v", err)
	}
	for _, routable := range [][]string{{"platform", "platform"}, {"-platform"}, {""}, {"wherry.io/platform"}} {
		terms.Routable = routable
		if err := validateTerms(terms); err == nil || err.Error() != "routable labels must be unique valid label keys" {
			t.Errorf("routable labels %q: %v", routable, err)
		}
	}
}
