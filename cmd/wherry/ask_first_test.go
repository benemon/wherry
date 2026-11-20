package main

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// askFirstLanguage is the wording of v0.1.2's rule that an optional credential reached a run only when the task
// requested it. The host now sends every optional credential it resolves, named or not, so none of it may remain in a
// tool description, a shipped instruction or a current page: an agent told to withhold a credential by leaving it out
// of a task would disclose it. Lower case; matched against lower-cased text.
var askFirstLanguage = []string{
	"only when requested",
	"only when its credentials name it",
	"unrequested",
	"only when you request",
	"unless requested",
	"unless the task requests",
	"unless the user asks for",
	"request it in a new",
	"a new run that requests it",
	"leave billing credentials",
	"reaches only the runs whose task requests",
	"reaches the agent only when",
	"not delivered unless",
	"did not request it",
	"a requested one",
	"unless `--credential",
}

// foldSpace is text in lower case with each run of white space, line breaks included, as one space, so a phrase a page
// wraps or indents still matches, and for each byte of the result the offset of its first byte in text.
func foldSpace(text string) (string, []int) {
	var folded strings.Builder
	var offsets []int
	space := false
	for i, r := range strings.ToLower(text) {
		switch {
		case r == ' ' || r == '\n' || r == '\t' || r == '\r':
			if !space {
				folded.WriteByte(' ')
				offsets = append(offsets, i)
			}
			space = true
		default:
			for range len(string(r)) {
				offsets = append(offsets, i)
			}
			folded.WriteRune(r)
			space = false
		}
	}
	return folded.String(), offsets
}

// askFirstPages are the shipped instructions and pages the check reads, beyond the MCP tool descriptions: the plugin's
// skill and snippet, which agents follow, and every page of the docs. upgrade.md's section on v0.1.2 describes the old
// rule as history, marked superseded, and is the one passage left out.
func askFirstPages(t *testing.T) map[string]string {
	t.Helper()
	paths := []string{"../../plugins/wherry/skills/wherry/SKILL.md", "../../plugins/AGENTS.snippet.md", "../../README.md"}
	docs, err := filepath.Glob("../../docs/*.md")
	if err != nil || len(docs) == 0 {
		t.Fatalf("docs pages: %v, %v", docs, err)
	}
	pages := map[string]string{}
	for _, path := range slices.Concat(paths, docs) {
		text := docRead(t, path)
		if filepath.Base(path) == "upgrade.md" {
			before, after, ok := strings.Cut(text, "\n## Optional credentials from v0.1.2\n")
			if !ok {
				t.Fatal("upgrade.md no longer carries the superseded v0.1.2 section this check leaves out")
			}
			_, rest, _ := strings.Cut(after, "\n## ")
			text = before + "\n## " + rest
		}
		pages[path] = text
	}
	return pages
}

// No shipped instruction or current page tells an agent that an optional credential is delivered on request alone, or
// to withhold one by not naming it; the ones that said so are named with the phrase.
func TestNoAskFirstLanguageRemains(t *testing.T) {
	for path, text := range askFirstPages(t) {
		folded, offsets := foldSpace(text)
		for _, stale := range askFirstLanguage {
			if i := strings.Index(folded, stale); i >= 0 {
				line := 1 + strings.Count(text[:offsets[i]], "\n")
				t.Errorf("%s:%d still says %q", path, line, stale)
			}
		}
	}
	// The check reads what it should: the plugin's instructions and the pages the review named.
	pages := askFirstPages(t)
	for _, path := range []string{"../../plugins/wherry/skills/wherry/SKILL.md", "../../plugins/AGENTS.snippet.md", "../../docs/security.md", "../../docs/a2a-extension-v1.md", "../../docs/configuration-host.md", "../../docs/upgrade.md", "../../docs/commands.md", "../../docs/mcp.md"} {
		if pages[path] == "" {
			t.Errorf("%s is not read", path)
		}
	}
}
