package operator

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The Strimzi ignored-users pattern docs/operations.md gives matches every principal wherry creates through the Admin
// API, and no other.
func TestStrimziIgnoredUsersPattern(t *testing.T) {
	doc, err := os.ReadFile(filepath.Join("..", "..", "docs", "operations.md"))
	if err != nil {
		t.Fatal(err)
	}
	_, section, ok := strings.Cut(string(doc), "\n### Strimzi\n")
	if !ok {
		t.Fatal("docs/operations.md has no Strimzi section")
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
