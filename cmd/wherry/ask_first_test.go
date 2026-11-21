package main

import (
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// askFirstLanguage is the wording of v0.1.2's rule that an optional credential reached a run only when the task
// requested it. The host now sends every optional credential it resolves, named or not, so none of it may remain in a
// tool description, a shipped instruction or a current page: an agent told to withhold a credential by leaving it out
// of a task would disclose it. Lower case; matched against lower-cased text with white space folded.
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

// askFirstExceptions are the passages that name the old rule to say it is gone, each containing one of the phrases
// above. Each is exact, lower case, and blanked before the match, so a page may say the rule no longer holds or that
// its report went, and nothing else that the phrases cover.
var askFirstExceptions = []string{
	"no longer delivered only when requested",
	"no longer only when requested",
	"`unrequested` is gone",
	"in place of `unrequested`",
	"in place of the earlier `unrequested`",
	"replaces `unrequested`",
}

// superseded marks a section of a page that describes an old rule as history, and is left out of the check: the
// section's body opens with it, and the section ends at the next heading of any level.
const superseded = "Superseded by "

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

// askFirstFinding is a stale phrase found in a text and the line it starts on.
type askFirstFinding struct {
	stale string
	line  int
}

// askFirstFindings are the stale phrases in text, outside its exceptions, each with its line.
func askFirstFindings(text string) []askFirstFinding {
	folded, offsets := foldSpace(text)
	for _, exception := range askFirstExceptions {
		folded = strings.ReplaceAll(folded, exception, strings.Repeat(" ", len(exception)))
	}
	var findings []askFirstFinding
	for _, stale := range askFirstLanguage {
		for from := 0; ; {
			i := strings.Index(folded[from:], stale)
			if i < 0 {
				break
			}
			i += from
			findings = append(findings, askFirstFinding{stale, 1 + strings.Count(text[:offsets[i]], "\n")})
			from = i + len(stale)
		}
	}
	slices.SortFunc(findings, func(a, b askFirstFinding) int { return a.line - b.line })
	return findings
}

// withoutSuperseded is text without each section whose body opens with the superseded marker: from its heading line
// to the next heading of any level, or the end of the text when none follows.
func withoutSuperseded(text string) string {
	lines := strings.Split(text, "\n")
	var kept []string
	for i := 0; i < len(lines); i++ {
		if strings.HasPrefix(lines[i], "#") && strings.HasPrefix(firstBodyLine(lines[i+1:]), superseded) {
			for i++; i < len(lines) && !strings.HasPrefix(lines[i], "#"); i++ {
			}
			i--
			continue
		}
		kept = append(kept, lines[i])
	}
	return strings.Join(kept, "\n")
}

// firstBodyLine is the first line of lines that is not blank.
func firstBodyLine(lines []string) string {
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			return line
		}
	}
	return ""
}

// markdownPages are the paths of every .md file under root, nested directories included, sorted.
func markdownPages(root string) ([]string, error) {
	var paths []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err == nil && !entry.IsDir() && strings.HasSuffix(path, ".md") {
			paths = append(paths, filepath.ToSlash(path))
		}
		return err
	})
	slices.Sort(paths)
	return paths, err
}

// askFirstPages are the shipped instructions and pages the check reads, beyond the MCP tool descriptions: the plugin's
// skill and snippet, which agents follow, the README, and every page under docs, nested directories included. A
// section marked superseded, upgrade.md's on v0.1.2, describes the old rule as history and is left out.
func askFirstPages(t *testing.T) map[string]string {
	t.Helper()
	docs, err := markdownPages("../../docs")
	if err != nil || len(docs) == 0 {
		t.Fatalf("docs pages: %v, %v", docs, err)
	}
	pages := map[string]string{}
	for _, path := range slices.Concat([]string{"../../plugins/wherry/skills/wherry/SKILL.md", "../../plugins/AGENTS.snippet.md", "../../README.md"}, docs) {
		pages[path] = withoutSuperseded(docRead(t, path))
	}
	return pages
}

// No shipped instruction or current page tells an agent that an optional credential is delivered on request alone, or
// to withhold one by not naming it; the ones that said so are named with the phrase.
func TestNoAskFirstLanguageRemains(t *testing.T) {
	pages := askFirstPages(t)
	for path, text := range pages {
		for _, finding := range askFirstFindings(text) {
			t.Errorf("%s:%d still says %q", path, finding.line, finding.stale)
		}
	}
	// The check reads what it should: the plugin's instructions and the pages the review named.
	for _, path := range []string{"../../plugins/wherry/skills/wherry/SKILL.md", "../../plugins/AGENTS.snippet.md", "../../docs/security.md", "../../docs/a2a-extension-v1.md", "../../docs/configuration-host.md", "../../docs/upgrade.md", "../../docs/commands.md", "../../docs/mcp.md"} {
		if pages[path] == "" {
			t.Errorf("%s is not read", path)
		}
	}
	// The marker, not a heading, keeps upgrade.md's history out: its section on v0.1.2 names the old report.
	if full := docRead(t, "../../docs/upgrade.md"); !strings.Contains(full, "\n"+superseded) || !strings.Contains(full, "`unrequested=") {
		t.Error("upgrade.md no longer carries the superseded v0.1.2 section this check exercises the exclusion on")
	}
	if strings.Contains(pages["../../docs/upgrade.md"], "`unrequested=") {
		t.Error("upgrade.md's superseded section was read")
	}
}

// The matcher reports the old rule wherever a page wraps it, accepts the text that says the rule is gone, reads
// nested pages, and leaves out a superseded section only as far as its next heading.
func TestAskFirstCheckMatchesTheStaleClaimsAlone(t *testing.T) {
	for _, text := range []string{
		"Optional credentials are no longer delivered only when requested.",
		"`unrequested` is gone: `submit` prints `unsupplied` in its place.",
		"The host sends every optional credential it resolves, in place of `unrequested`.",
	} {
		if findings := askFirstFindings(text); len(findings) != 0 {
			t.Errorf("%q: %v; want nothing", text, findings)
		}
	}
	for _, test := range []struct {
		text  string
		stale string
		line  int
	}{
		{"An optional credential\n  is delivered only when\n  requested.", "only when requested", 2},
		{"It is no longer delivered only when requested, and one is delivered only when requested.", "only when requested", 1},
		{"`submit` prints `unrequested=<name,...>`.", "unrequested", 1},
		{"Leave it out of the task, and request it in a new run when needed.", "request it in a new", 1},
	} {
		if findings := askFirstFindings(test.text); len(findings) != 1 || findings[0] != (askFirstFinding{test.stale, test.line}) {
			t.Errorf("%q: %v; want %q at line %d", test.text, findings, test.stale, test.line)
		}
	}
	page := "# Page\n\nCurrent text.\n\n## Old rule\n\nSuperseded by the new rule.\nDelivered only when requested.\n\n### Still old\n\nunrequested here is read.\n\n## Next\n\nCurrent again.\n"
	if findings := askFirstFindings(withoutSuperseded(page)); len(findings) != 1 || findings[0].stale != "unrequested" {
		t.Errorf("superseded exclusion: %v; want the subsection's phrase alone", findings)
	}
	if findings := askFirstFindings(withoutSuperseded("## Old rule\n\nSuperseded by the new rule.\nunrequested to the end.\n")); len(findings) != 0 {
		t.Errorf("superseded section to the end of the page: %v; want nothing", findings)
	}
	if findings := askFirstFindings(withoutSuperseded("## Old rule\n\nNot marked.\nunrequested here is read.\n")); len(findings) != 1 {
		t.Errorf("section without the marker: %v; want its phrase", findings)
	}
	docs := filepath.Join(t.TempDir(), "docs")
	for _, path := range []string{"a.md", "guide/b.md", "guide/diagrams/c.svg"} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(docs, path)), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(docs, path), []byte("fine\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if found, err := markdownPages(docs); err != nil || !slices.Equal(found, []string{filepath.ToSlash(filepath.Join(docs, "a.md")), filepath.ToSlash(filepath.Join(docs, "guide", "b.md"))}) {
		t.Errorf("nested pages %v, %v; want the two pages, the nested one included", found, err)
	}
}
