package operator

import (
	"os"
	"path/filepath"
	"regexp"
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

// A signature names the agent and every label a profile carries, whatever their order, and nothing else; its topic is
// the one pattern every reader parses. The digests are sha256sum's of the literal encodings.
func TestSignatureTopic(t *testing.T) {
	labels := map[string]string{"platform": "nomad", "isolation": "container"}
	const both = "claude\nisolation=container\nplatform=nomad\n"
	if got := Signature("claude", labels); got != both {
		t.Fatalf("signature %q, want %q", got, both)
	}
	// The same labels inserted in the other order are the same signature.
	reordered := map[string]string{}
	reordered["isolation"] = "container"
	reordered["platform"] = "nomad"
	want := "wherry.pool.pool-a.tasks.claude.79c0eb55fa00d7e5903d9fa0f9f51ec162666978ff2938be19fd6e08b169833a"
	for _, same := range []map[string]string{labels, reordered} {
		if got := SignatureTopic("pool-a", "claude", same); got != want {
			t.Fatalf("labels %v: topic %s, want %s", same, got, want)
		}
	}
	// A label added, a value changed or another agent is another signature.
	for _, other := range []struct {
		agent  string
		labels map[string]string
	}{
		{"claude", map[string]string{"platform": "nomad", "isolation": "container", "gpu": "a100"}},
		{"claude", map[string]string{"platform": "nomad", "isolation": "host"}},
		{"claude", map[string]string{"platform": "nomad"}},
		{"codex", labels},
	} {
		if got := SignatureTopic("pool-a", other.agent, other.labels); got == want {
			t.Errorf("agent %s, labels %v: topic %s, the same as agent claude, labels %v", other.agent, other.labels, got, labels)
		}
	}
	if len(labels) != 2 || labels["platform"] != "nomad" || labels["isolation"] != "container" {
		t.Fatalf("labels changed by the signature: %v", labels)
	}
	for _, fixture := range []struct {
		agent  string
		labels map[string]string
		topic  string
	}{
		{"codex", labels, "wherry.pool.pool-a.tasks.codex.b88843185bc94c1384b4cdf4b1a9968f785214fa8d3a347bbad461675acb503e"},
		{"claude", map[string]string{"platform": "nomad", "isolation": "container", "gpu": "a100"}, "wherry.pool.pool-a.tasks.claude.e5bf4f20966c9016ba8c6d304637749604d2d1536b7a4c915e9109146cdd4618"},
		{"claude", nil, "wherry.pool.pool-a.tasks.claude.98038b25280788a45a2f06c209513024c99adee81c6416b691b4b5872db7e027"},
	} {
		if got := SignatureTopic("pool-a", fixture.agent, fixture.labels); got != fixture.topic {
			t.Errorf("agent %s, labels %v: topic %s, want %s", fixture.agent, fixture.labels, got, fixture.topic)
		}
		if pool, agent, ok := wire.ParseTaskTopic(fixture.topic); !ok || pool != "pool-a" || agent != fixture.agent {
			t.Errorf("%s parses to %q, %q, %v", fixture.topic, pool, agent, ok)
		}
		if !strings.HasPrefix(fixture.topic, PoolTasksPrefix("pool-a")) {
			t.Errorf("%s lacks the pool's prefix", fixture.topic)
		}
	}
	long := strings.Repeat("p", 32)
	topic := SignatureTopic(long, strings.Repeat("a", 32), labels)
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
