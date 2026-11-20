package catalog

import (
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/benemon/wherry/internal/catalog/snapshot"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
)

func entry(id, name string, env ...string) string {
	quoted := make([]string, 0, len(env))
	for _, variable := range env {
		quoted = append(quoted, `"`+variable+`"`)
	}
	return `"` + id + `": {"id": "` + id + `", "name": "` + name + `", "env": [` + strings.Join(quoted, ", ") + `], "doc": "https://example.com/` + id + `"}`
}

func file(entries ...string) []byte { return []byte("{" + strings.Join(entries, ",\n") + "}") }

// The catalog is the union of its files; an id in two of them is an error, as is an entry whose id is not its key.
func TestParseIsTheUnionWithADuplicateIdAnError(t *testing.T) {
	catalog, err := Parse(file(entry("anthropic", "Anthropic", "ANTHROPIC_API_KEY")), file(entry("claude-code", "Claude Code", "CLAUDE_CODE_OAUTH_TOKEN")))
	if err != nil {
		t.Fatal(err)
	}
	if ids := catalog.IDs(); !slices.Equal(ids, []string{"anthropic", "claude-code"}) {
		t.Errorf("ids %v", ids)
	}
	if _, err := Parse(file(entry("codex", "Codex", "OPENAI_API_KEY")), file(entry("codex", "Codex CLI", "CODEX_API_KEY"))); err == nil || !strings.Contains(err.Error(), "codex is in two files") {
		t.Errorf("duplicate id: %v", err)
	}
	if _, err := Parse(file(`"codex": {"id": "openai", "name": "Codex", "env": ["CODEX_API_KEY"], "doc": ""}`)); err == nil || !strings.Contains(err.Error(), `entry "codex" has id "openai"`) {
		t.Errorf("mismatched id: %v", err)
	}
	if _, err := Parse([]byte(`["codex"]`)); err == nil {
		t.Error("a JSON array parsed")
	}
}

// An id is usable when it follows the name grammar and every credential it derives is one a pool's terms may carry:
// wafer.ai fails the grammar; a variable a plugin sets, one that is not a variable name, and one whose derived name is
// over 32 characters each make their entry unusable, as does an entry with no variable. An unusable id expands to
// nothing.
func TestUsableIdsFollowEveryRule(t *testing.T) {
	catalog, err := Parse(file(
		entry("wafer.ai", "Wafer", "WAFER_API_KEY"),
		entry("home", "Home", "HOME"),
		entry("model", "Model", "WHERRY_MODEL"),
		entry("digits", "Digits", "302AI_API_KEY"),
		entry("long", "Long", "AZURE_COGNITIVE_SERVICES_RESOURCE_NAME"),
		entry("case", "Case", "Foo_KEY", "FOO_KEY"),
		entry("none", "None"),
		entry("ok", "OK", "OK_API_KEY"),
		entry("pair", "Pair", "PAIR_ID", "PAIR_SECRET"),
	))
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"wafer.ai", "home", "model", "digits", "long", "case", "none", "absent"} {
		if catalog.Usable(id) {
			t.Errorf("%s is usable", id)
		}
		if credentials, ok := catalog.Expand(id); ok || credentials != nil {
			t.Errorf("%s expands to %v, %t", id, credentials, ok)
		}
	}
	for _, id := range []string{"ok", "pair"} {
		if !catalog.Usable(id) {
			t.Errorf("%s is not usable", id)
		}
	}
	if !pluginsdk.ReservedEnv("HOME") || !pluginsdk.ReservedEnv("WHERRY_MODEL") || wire.ValidName("wafer.ai") {
		t.Error("the rules the test relies on changed")
	}
}

// One variable derives one required credential; several derive one optional credential per variable. A derived
// credential is named after its variable, targets it and describes itself as the entry's name.
func TestExpandDerivesOneCredentialPerVariable(t *testing.T) {
	catalog, err := Parse(file(entry("claude-code", "Claude Code", "CLAUDE_CODE_OAUTH_TOKEN"), entry("google", "Google", "GOOGLE_API_KEY", "GOOGLE_GENERATIVE_AI_API_KEY", "GEMINI_API_KEY")))
	if err != nil {
		t.Fatal(err)
	}
	single, ok := catalog.Expand("claude-code")
	if want := []Credential{{Name: "claude-code-oauth-token", Env: "CLAUDE_CODE_OAUTH_TOKEN", Description: "Claude Code", Required: true}}; !ok || !reflect.DeepEqual(single, want) {
		t.Errorf("claude-code expands to %+v, %t; want %+v", single, ok, want)
	}
	several, ok := catalog.Expand("google")
	want := []Credential{
		{Name: "google-api-key", Env: "GOOGLE_API_KEY", Description: "Google"},
		{Name: "google-generative-ai-api-key", Env: "GOOGLE_GENERATIVE_AI_API_KEY", Description: "Google"},
		{Name: "gemini-api-key", Env: "GEMINI_API_KEY", Description: "Google"},
	}
	if !ok || !reflect.DeepEqual(several, want) {
		t.Errorf("google expands to %+v, %t; want %+v", several, ok, want)
	}
	if name := DerivedName("AWS_SECRET_ACCESS_KEY"); name != "aws-secret-access-key" {
		t.Errorf("derived name %s", name)
	}
	if !catalog.Reads("GEMINI_API_KEY") || catalog.Reads("OPENAI_API_KEY") {
		t.Error("Reads")
	}
}

// The embedded catalog loads, holds the four harness entries with their variables and models.dev's providers, and
// every id models.dev gives that breaks a rule is unusable.
func TestEmbeddedCatalog(t *testing.T) {
	catalog, err := Embedded()
	if err != nil {
		t.Fatal(err)
	}
	for id, variable := range map[string]string{"claude-code": "CLAUDE_CODE_OAUTH_TOKEN", "codex": "CODEX_API_KEY", "copilot-cli": "COPILOT_GITHUB_TOKEN", "cursor": "CURSOR_API_KEY"} {
		entry, ok := catalog.Entry(id)
		if !ok || !slices.Equal(entry.Env, []string{variable}) || entry.Name == "" || !strings.HasPrefix(entry.Doc, "https://") {
			t.Errorf("harness entry %s = %+v, %t", id, entry, ok)
		}
		if credentials, ok := catalog.Expand(id); !ok || len(credentials) != 1 || !credentials[0].Required || credentials[0].Env != variable {
			t.Errorf("%s expands to %+v, %t", id, credentials, ok)
		}
	}
	if credentials, ok := catalog.Expand("google"); !ok || len(credentials) != 3 || credentials[0].Required {
		t.Errorf("google expands to %+v, %t", credentials, ok)
	}
	if entry, ok := catalog.Entry("anthropic"); !ok || !slices.Equal(entry.Env, []string{"ANTHROPIC_API_KEY"}) {
		t.Errorf("anthropic = %+v, %t", entry, ok)
	}
	// wafer.ai fails the name grammar, 302ai's variable starts with a digit, and azure-cognitive-services derives a name
	// over 32 characters.
	for _, id := range []string{"wafer.ai", "302ai", "azure-cognitive-services"} {
		if _, ok := catalog.Entry(id); !ok {
			t.Errorf("models.dev no longer lists %s; pick another example", id)
		}
		if catalog.Usable(id) {
			t.Errorf("%s is usable", id)
		}
	}
	if len(catalog.IDs()) < 300 {
		t.Errorf("%d ids; want models.dev's providers", len(catalog.IDs()))
	}
	if !catalog.Reads("GITHUB_TOKEN") || !catalog.Reads("AWS_SECRET_ACCESS_KEY") {
		t.Error("the cloud and GitHub variables the docs name are not in the catalog")
	}
}

// Both files are in the form update writes, so a rewrite of the same data changes nothing, and SOURCE names the
// snapshot's URL, fetch time and hash.
func TestCatalogFilesAreCanonical(t *testing.T) {
	for _, name := range []string{"models.dev.json", "harness.json"} {
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		entries, err := snapshot.Strip(data)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := snapshot.Encode(entries)
		if err != nil {
			t.Fatal(err)
		}
		if string(encoded) != string(data) {
			t.Errorf("%s is not in the form update writes; run go run ./internal/catalog/update", name)
		}
		for id, entry := range entries {
			if _, ok := entry["models"]; ok {
				t.Errorf("%s: %s keeps its models", name, id)
			}
		}
	}
	source, err := os.ReadFile("SOURCE")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(source), "\n"), "\n")
	if len(lines) != 3 || lines[0] != "url: https://models.dev/api.json" || !strings.HasPrefix(lines[1], "fetched: ") || !strings.HasSuffix(lines[1], "Z") || !strings.HasPrefix(lines[2], "sha256: ") || len(lines[2]) != len("sha256: ")+64 {
		t.Errorf("SOURCE:\n%s", source)
	}
}
