package receiver

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
)

// continuation is a real-git round of --continue on the oracle's forge: a first run, upstream changes, and the
// continuing run's preamble commands.
type continuation struct {
	*oracle
	env, plain []string
}

func newContinuation(t *testing.T) *continuation {
	t.Helper()
	o := newOracle(t)
	return &continuation{oracle: o, env: o.agentEnvironment(),
		plain: []string{"HOME=" + filepath.Join(o.root, "plain-home"), "PATH=" + os.Getenv("PATH"), "GIT_CONFIG_NOSYSTEM=1", "GIT_AUTHOR_NAME=upstream",
			"GIT_AUTHOR_EMAIL=upstream@example.test", "GIT_COMMITTER_NAME=upstream", "GIT_COMMITTER_EMAIL=upstream@example.test"}}
}

// lines are the preamble's commands for a run at ref, continuing from base.bundle over commit when commit is set, which
// asks for changes.bundle: the clone, then the checkout lines, then the bundle and push.
func (c *continuation) lines(ref, commit string) []string {
	git := &wire.GitInput{Remote: c.forge.URL + "/org/repo.git", Ref: ref, Credential: "forge-token"}
	if commit != "" {
		git.Base = &wire.GitBase{Bundle: "base.bundle", Commit: commit}
	}
	credentials := []wire.TaskCredential{{Name: "forge-token", File: "token"}}
	return commands(Preamble(c.directories, []string{"base.bundle"}, credentials, nil, []wire.ExpectedOutput{{Name: wire.GitBundle}}, git, "run-git", operator.DefaultReplyLimit))
}

// run runs lines as the agent, stopping at the first that fails and returning its output and error.
func (c *continuation) run(t *testing.T, lines []string) (string, error) {
	t.Helper()
	for _, line := range lines {
		if output, err := c.oracle.run(t, c.env, line); err != nil {
			return line + ": " + output, err
		}
	}
	return "", nil
}

// commit commits a change to the agent's checkout with subject.
func (c *continuation) commit(t *testing.T, subject string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(c.work, "repo", subject), []byte(subject+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{"git -C repo add " + subject, "git -C repo commit -q -m " + subject} {
		if output, err := c.oracle.run(t, c.env, line); err != nil {
			t.Fatalf("%s: %v\n%s", line, err, output)
		}
	}
}

// first runs a first run at ref that commits subject, and delivers its changes.bundle as the next run's base.bundle,
// returning the run's base commit and clearing its checkout.
func (c *continuation) first(t *testing.T, ref, subject string) string {
	t.Helper()
	lines := c.lines(ref, "")
	if output, err := c.run(t, lines[:3]); err != nil {
		t.Fatalf("%v\n%s", err, output)
	}
	c.commit(t, subject)
	if output, err := c.run(t, lines[3:4]); err != nil {
		t.Fatalf("%v\n%s", err, output)
	}
	base := git(t, filepath.Join(c.work, "repo"), c.plain, "rev-parse", "refs/wherry/start")
	c.deliver(t)
	return base
}

// deliver moves the agent's changes.bundle to its input directory as base.bundle and removes its checkout.
func (c *continuation) deliver(t *testing.T) {
	t.Helper()
	if err := os.Rename(filepath.Join(c.directories.Output, wire.GitBundle), filepath.Join(c.directories.Input, "base.bundle")); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(c.work, "repo")); err != nil {
		t.Fatal(err)
	}
}

// upstream runs a change in a plain clone of the forge's repository and pushes with push's arguments.
func (c *continuation) upstream(t *testing.T, change func(clone string), push ...string) {
	t.Helper()
	clone := filepath.Join(c.root, "upstream")
	os.RemoveAll(clone)
	git(t, c.root, c.plain, "clone", "-q", c.repository, clone)
	change(clone)
	git(t, clone, c.plain, append([]string{"push", "-q", "origin"}, push...)...)
}

// imported is the subjects of the commits a fresh clone of the forge's main reaches from its bundle's head.
func (c *continuation) imported(t *testing.T, bundle string) string {
	t.Helper()
	fresh := filepath.Join(c.root, "fresh")
	git(t, c.root, c.plain, "clone", "-q", c.repository, fresh)
	git(t, fresh, c.plain, "fetch", "-q", bundle, "HEAD")
	return git(t, fresh, c.plain, "log", "--format=%s", "FETCH_HEAD")
}

// The continuing run's preamble, run as an agent runs it against a real git: the base commit is checked out after
// main advanced, the fetch of the previous run's bundle is allowed for that command alone, and the run's bundle carries
// both runs' commits into a fresh clone of the advanced main.
func TestGitContinuationCarriesTheChainOverAnAdvancedRef(t *testing.T) {
	c := newContinuation(t)
	base := c.first(t, "main", "c1")
	c.upstream(t, func(clone string) {
		os.WriteFile(filepath.Join(clone, "upstream"), []byte("advanced\n"), 0o600)
		git(t, clone, c.plain, "add", "upstream")
		git(t, clone, c.plain, "commit", "-q", "-m", "advanced")
	}, "main")
	lines := c.lines("main", base)
	if len(lines) != 8 {
		t.Fatalf("commands %q, want clone, five continuation lines, bundle and push", lines)
	}
	if output, err := c.run(t, lines[:6]); err != nil {
		t.Fatalf("%v\n%s", err, output)
	}
	checkout := filepath.Join(c.work, "repo")
	if start := git(t, checkout, c.plain, "rev-parse", "refs/wherry/start"); start != base {
		t.Fatalf("refs/wherry/start %s, want the base %s", start, base)
	}
	if head := git(t, checkout, c.plain, "log", "-1", "--format=%s"); head != "c1" {
		t.Fatalf("HEAD %q, want the previous run's commit", head)
	}
	if allow := git(t, checkout, c.plain, "config", "--local", "protocol.allow"); allow != "never" {
		t.Fatalf("protocol.allow %q after the fetch, want never", allow)
	}
	if _, err := os.Stat(filepath.Join(checkout, "upstream")); err == nil {
		t.Fatal("the checkout holds upstream's later commit, not the base")
	}
	c.commit(t, "c2")
	if output, err := c.run(t, lines[6:7]); err != nil {
		t.Fatalf("%v\n%s", err, output)
	}
	if subjects := c.imported(t, filepath.Join(c.directories.Output, wire.GitBundle)); subjects != "c2\nc1\none" {
		t.Fatalf("imported %q, want both runs' commits over the base", subjects)
	}
}

// A continuation does not check out its ref, so a ref deleted upstream after its commits merged does not stop it.
func TestGitContinuationOutlivesItsDeletedRef(t *testing.T) {
	c := newContinuation(t)
	base := c.first(t, "feature", "c1")
	c.upstream(t, func(clone string) {
		git(t, clone, c.plain, "merge", "-q", "--ff-only", "origin/feature")
	}, "main", ":feature")
	lines := c.lines("feature", base)
	if output, err := c.run(t, lines[:6]); err != nil {
		t.Fatalf("%v\n%s", err, output)
	}
	if head := git(t, filepath.Join(c.work, "repo"), c.plain, "log", "-1", "--format=%s"); head != "c1" {
		t.Fatalf("HEAD %q, want the previous run's commit", head)
	}
}

// A base commit upstream no longer has stops the run at its checkout.
func TestGitContinuationStopsWithoutItsBase(t *testing.T) {
	c := newContinuation(t)
	base := c.first(t, "feature", "c1")
	c.upstream(t, func(string) {}, ":feature")
	lines := c.lines("main", base)
	if output, err := c.run(t, lines[:6]); err == nil || !strings.Contains(output, "checkout --detach '"+base+"'") {
		t.Fatalf("continuation without its base upstream = %v:\n%s", err, output)
	}
}

// A bundle whose head does not descend from the base stops the run: one of another repository at its fetch, one built
// on another commit the clone holds at its ancestry check.
func TestGitContinuationStopsOnABundleOffItsBase(t *testing.T) {
	for name, bundle := range map[string]func(t *testing.T, c *continuation) string{
		"another repository": func(t *testing.T, c *continuation) string {
			other := filepath.Join(c.root, "other")
			git(t, c.root, c.plain, "init", "-q", "-b", "main", other)
			for _, subject := range []string{"o1", "o2"} {
				git(t, other, c.plain, "commit", "-q", "--allow-empty", "-m", subject)
			}
			git(t, other, c.plain, "bundle", "create", "-q", filepath.Join(c.directories.Input, "base.bundle"), "HEAD~1..HEAD")
			return " fetch "
		},
		"another commit": func(t *testing.T, c *continuation) string {
			// main's commit is in the clone and is the base's parent, not its descendant.
			other := filepath.Join(c.root, "other")
			git(t, c.root, c.plain, "clone", "-q", "--branch", "main", c.repository, other)
			git(t, other, c.plain, "commit", "-q", "--allow-empty", "-m", "off")
			git(t, other, c.plain, "bundle", "create", "-q", filepath.Join(c.directories.Input, "base.bundle"), "HEAD~1..HEAD")
			return " merge-base "
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := newContinuation(t)
			fails := bundle(t, c)
			base := git(t, c.repository, c.plain, "rev-parse", "feature")
			output, err := c.run(t, c.lines("feature", base)[:6])
			if err == nil || !strings.Contains(output, fails) {
				t.Fatalf("continuation from a bundle off its base = %v, want a failure at%s:\n%s", err, fails, output)
			}
		})
	}
}

// A continuation's preamble checks out neither its ref nor names it: the base commit replaces both.
func TestGitContinuationPreambleReplacesTheRefCheckout(t *testing.T) {
	commit := strings.Repeat("ab", 20)
	git := &wire.GitInput{Remote: "https://git.example.test/org/repo.git", Ref: "gone", Base: &wire.GitBase{Bundle: "base.bundle", Commit: commit}}
	preamble := Preamble(pluginsdk.Directories{Input: "/task/inputs"}, []string{"base.bundle"}, nil, nil, nil, git, "run-a", operator.DefaultReplyLimit)
	if strings.Contains(preamble, "gone") {
		t.Fatalf("preamble names the ref:\n%s", preamble)
	}
	for _, line := range []string{
		"Clone the git repository https://git.example.test/org/repo.git into repo, relative to your working directory, and continue from the previous run's changes in /task/inputs/base.bundle, with these commands. " +
			"They need git 2.30 or later: run git version first, and if it is older, stop and report that.",
		"    git -C 'repo' checkout --detach '" + commit + "' --",
		"    git -C 'repo' update-ref refs/wherry/start HEAD",
		`    git -C 'repo' -c protocol.file.allow=always fetch "/task/inputs/base.bundle" HEAD`,
		"    git -C 'repo' merge-base --is-ancestor refs/wherry/start FETCH_HEAD",
		"    git -C 'repo' checkout --detach FETCH_HEAD --",
		"If any of these commands fails, stop and report it.",
	} {
		if !strings.Contains(preamble, line+"\n") {
			t.Errorf("preamble lacks %q:\n%s", line, preamble)
		}
	}
}
