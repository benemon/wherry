package client

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/wire"
)

// agentBundle is what an agent returns for r: a clone of r's remote with one commit, message, on top of r's main, as a
// bundle of that commit over base, with extra rev-list arguments, and the commit itself.
func (r gitRepos) agentBundle(t *testing.T, base, message string, extra ...string) (bundle, head string) {
	t.Helper()
	agent := gitRepos{remote: r.remote, url: r.url, work: filepath.Join(t.TempDir(), "agent")}
	realGit(t, "", "clone", "-q", r.url, agent.work)
	head = agent.commit(t, message)
	bundle = filepath.Join(t.TempDir(), wire.GitBundle)
	realGit(t, agent.work, append([]string{"bundle", "create", bundle, base + "..HEAD"}, extra...)...)
	return bundle, head
}

// refs is every ref of the repository at dir with its commit.
func refs(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for line := range strings.Lines(realGit(t, dir, "for-each-ref", "--format=%(refname) %(objectname)")) {
		name, oid, _ := strings.Cut(strings.TrimSpace(line), " ")
		out[name] = oid
	}
	return out
}

// checkout is what a fetch must leave alone: HEAD's file, the index's bytes and the working tree's status.
func checkout(t *testing.T, dir string) [3]string {
	t.Helper()
	head, err := os.ReadFile(filepath.Join(dir, ".git", "HEAD"))
	if err != nil {
		t.Fatal(err)
	}
	index, err := os.ReadFile(filepath.Join(dir, ".git", "index"))
	if err != nil {
		t.Fatal(err)
	}
	return [3]string{string(head), string(index), realGit(t, dir, "status", "--porcelain")}
}

const testRun = "run-tenant-a-host-a-fetch"

// A run's bundle is fetched into wherry/<run> at the bundle's head, with HEAD, the index and the working tree
// unchanged; a second fetch of the same bundle finds the branch there and does nothing.
func TestFetchBundleCreatesTheRunsBranch(t *testing.T) {
	r := newGitRepos(t)
	base := realGit(t, r.work, "rev-parse", "HEAD")
	bundle, head := r.agentBundle(t, base, "two")
	if err := os.WriteFile(filepath.Join(r.work, "draft"), []byte("unstaged\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	before := checkout(t, r.work)
	for range 2 {
		if ref, reason := fetchBundle(context.Background(), r.work, testRun, bundle); ref != "wherry/"+testRun || reason != "" {
			t.Fatalf("fetch = %q, %q; want wherry/%s and no reason", ref, reason, testRun)
		}
		if got := realGit(t, r.work, "rev-parse", "refs/heads/wherry/"+testRun); got != head {
			t.Fatalf("wherry/%s at %s, want the bundle's head %s", testRun, got, head)
		}
		if realGit(t, r.work, "rev-parse", "HEAD") != base || checkout(t, r.work) != before {
			t.Fatalf("the fetch changed HEAD, the index or the working tree")
		}
	}
}

// A wherry/<run> that names another commit is left as it is, whether or not it is an ancestor of the bundle's head.
func TestFetchBundleLeavesAnExistingBranch(t *testing.T) {
	r := newGitRepos(t)
	base := realGit(t, r.work, "rev-parse", "HEAD")
	bundle, _ := r.agentBundle(t, base, "two")
	tree := realGit(t, r.work, "write-tree")
	for name, at := range map[string]string{"an ancestor of the head": base, "an unrelated commit": realGit(t, r.work, "commit-tree", tree, "-m", "unrelated")} {
		t.Run(name, func(t *testing.T) {
			realGit(t, r.work, "update-ref", "refs/heads/wherry/"+testRun, at)
			before := refs(t, r.work)
			if ref, reason := fetchBundle(context.Background(), r.work, testRun, bundle); ref != "" || reason != NotFetchedExists {
				t.Fatalf("fetch = %q, %q; want not fetched, %s", ref, reason, NotFetchedExists)
			}
			if after := refs(t, r.work); !mapsEqual(before, after) {
				t.Fatalf("refs changed: %v", changed(before, after))
			}
		})
	}
}

// corruptBundle is a bundle over base whose head commit has no committer email, which fsck refuses.
func (r gitRepos) corruptBundle(t *testing.T, base string) string {
	t.Helper()
	agent := gitRepos{remote: r.remote, url: r.url, work: filepath.Join(t.TempDir(), "agent")}
	realGit(t, "", "clone", "-q", r.url, agent.work)
	tree := realGit(t, agent.work, "write-tree")
	object := "tree " + tree + "\nparent " + base + "\nauthor A <a@invalid> 1700000000 +0000\ncommitter A a@invalid 1700000000 +0000\n\nbad\n"
	if err := os.WriteFile(filepath.Join(agent.work, "object"), []byte(object), 0o600); err != nil {
		t.Fatal(err)
	}
	bad := realGit(t, agent.work, "hash-object", "-w", "-t", "commit", "--literally", "object")
	realGit(t, agent.work, "update-ref", "refs/heads/bad", bad)
	realGit(t, agent.work, "symbolic-ref", "HEAD", "refs/heads/bad")
	bundle := filepath.Join(t.TempDir(), wire.GitBundle)
	realGit(t, agent.work, "bundle", "create", bundle, base+"..HEAD")
	return bundle
}

// A bundle holding a corrupt object is refused, though the repository's own configuration turns the fetch's checks off.
func TestFetchBundleChecksObjectsWhateverTheRepositorySays(t *testing.T) {
	r := newGitRepos(t)
	realGit(t, r.work, "config", "fetch.fsckObjects", "false")
	base := realGit(t, r.work, "rev-parse", "HEAD")
	bundle := r.corruptBundle(t, base)
	before := refs(t, r.work)
	if ref, reason := fetchBundle(context.Background(), r.work, testRun, bundle); ref != "" || reason != NotFetchedRefused {
		t.Fatalf("fetch = %q, %q; want not fetched, %s", ref, reason, NotFetchedRefused)
	}
	if after := refs(t, r.work); !mapsEqual(before, after) {
		t.Fatalf("refs changed: %v", changed(before, after))
	}
}

// A bundle that carries a tag adds the branch alone.
func TestFetchBundleAddsNoTag(t *testing.T) {
	r := newGitRepos(t)
	base := realGit(t, r.work, "rev-parse", "HEAD")
	agent := gitRepos{remote: r.remote, url: r.url, work: filepath.Join(t.TempDir(), "agent")}
	realGit(t, "", "clone", "-q", r.url, agent.work)
	head := agent.commit(t, "two")
	realGit(t, agent.work, "tag", "-a", "v1", "-m", "v1")
	bundle := filepath.Join(t.TempDir(), wire.GitBundle)
	realGit(t, agent.work, "bundle", "create", bundle, base+"..HEAD", "refs/tags/v1")
	if !strings.Contains(realGit(t, agent.work, "bundle", "list-heads", bundle), "refs/tags/v1") {
		t.Fatal("the bundle carries no tag")
	}
	before := refs(t, r.work)
	if ref, reason := fetchBundle(context.Background(), r.work, testRun, bundle); ref != "wherry/"+testRun || reason != "" {
		t.Fatalf("fetch = %q, %q", ref, reason)
	}
	before["refs/heads/wherry/"+testRun] = head
	if after := refs(t, r.work); !mapsEqual(before, after) {
		t.Fatalf("refs changed beyond the branch: %v", changed(before, after))
	}
}

// Hostile variables, global and repository configurations and hooks do not reach the fetch: it lands in the recorded
// repository, not GIT_DIR's, and runs no hook.
func TestFetchBundleUsesNoHostileConfigOrHooks(t *testing.T) {
	r := newGitRepos(t)
	base := realGit(t, r.work, "rev-parse", "HEAD")
	bundle, head := r.agentBundle(t, base, "two")
	hostileGitEnv(t, r)
	if ref, reason := fetchBundle(context.Background(), r.work, testRun, bundle); ref != "wherry/"+testRun || reason != "" {
		t.Fatalf("fetch = %q, %q", ref, reason)
	}
	if got := realGit(t, r.work, "rev-parse", "refs/heads/wherry/"+testRun); got != head {
		t.Fatalf("wherry/%s at %s, want %s", testRun, got, head)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(os.Getenv("GIT_DIR")), ".git", "refs", "heads", "wherry")); err == nil {
		t.Fatal("the fetch landed in GIT_DIR's repository")
	}
}

// A recorded repository whose .git is gone, inside another repository, is no repository: nothing is written to the
// repository above it.
func TestFetchBundleWithoutDotGitWritesNoEnclosingRepository(t *testing.T) {
	r := newGitRepos(t)
	base := realGit(t, r.work, "rev-parse", "HEAD")
	bundle, _ := r.agentBundle(t, base, "two")
	enclosing := filepath.Join(t.TempDir(), "enclosing")
	realGit(t, "", "clone", "-q", r.url, enclosing)
	inner := filepath.Join(enclosing, "project")
	realGit(t, "", "clone", "-q", r.url, inner)
	if err := os.RemoveAll(filepath.Join(inner, ".git")); err != nil {
		t.Fatal(err)
	}
	before := refs(t, enclosing)
	if ref, reason := fetchBundle(context.Background(), inner, testRun, bundle); ref != "" || reason != NotFetchedNoRepository {
		t.Fatalf("fetch = %q, %q; want not fetched, %s", ref, reason, NotFetchedNoRepository)
	}
	if after := refs(t, enclosing); !mapsEqual(before, after) {
		t.Fatalf("the enclosing repository's refs changed: %v", changed(before, after))
	}
}

// Each other failure is a fixed reason: the repository removed, a bundle whose prerequisite the repository lacks, a
// file that is no bundle, and no git on PATH.
func TestFetchBundleReasons(t *testing.T) {
	r := newGitRepos(t)
	base := realGit(t, r.work, "rev-parse", "HEAD")
	bundle, _ := r.agentBundle(t, base, "two")
	gone := filepath.Join(t.TempDir(), "gone")
	if ref, reason := fetchBundle(context.Background(), gone, testRun, bundle); ref != "" || reason != NotFetchedNoRepository {
		t.Fatalf("a removed repository: %q, %q; want %s", ref, reason, NotFetchedNoRepository)
	}
	agent := gitRepos{remote: r.remote, url: r.url, work: filepath.Join(t.TempDir(), "agent")}
	realGit(t, "", "clone", "-q", r.url, agent.work)
	unknown := agent.commit(t, "two")
	agent.commit(t, "three")
	ahead := filepath.Join(t.TempDir(), wire.GitBundle)
	realGit(t, agent.work, "bundle", "create", ahead, unknown+"..HEAD")
	if ref, reason := fetchBundle(context.Background(), r.work, testRun, ahead); ref != "" || reason != NotFetchedRefused {
		t.Fatalf("a missing prerequisite: %q, %q; want %s", ref, reason, NotFetchedRefused)
	}
	notBundle := filepath.Join(t.TempDir(), wire.GitBundle)
	if err := os.WriteFile(notBundle, []byte("PK\x03\x04"), 0o600); err != nil {
		t.Fatal(err)
	}
	if ref, reason := fetchBundle(context.Background(), r.work, testRun, notBundle); ref != "" || reason != NotFetchedUnreadable {
		t.Fatalf("no bundle: %q, %q; want %s", ref, reason, NotFetchedUnreadable)
	}
	if _, ok := refs(t, r.work)["refs/heads/wherry/"+testRun]; ok {
		t.Fatal("a refused fetch made the branch")
	}
	t.Setenv("PATH", t.TempDir())
	if ref, reason := fetchBundle(context.Background(), r.work, testRun, bundle); ref != "" || reason != NotFetchedNoGit {
		t.Fatalf("no git: %q, %q; want %s", ref, reason, NotFetchedNoGit)
	}
}

// Collect fetches the bundle it collects into the repository the run's record names, and reports neither a ref nor a
// reason for a run without one; a replaced run's bundle goes into wherry/<last run>, from the repository its record
// carried forward.
func TestCollectFetchesIntoTheRecordedRepository(t *testing.T) {
	f, _, _ := continueFixture(t)
	r := newGitRepos(t)
	base := realGit(t, r.work, "rev-parse", "HEAD")
	bundle, head := r.agentBundle(t, base, "two")
	data, err := os.ReadFile(bundle)
	if err != nil {
		t.Fatal(err)
	}
	submit := func() string {
		submitted, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "first", Git: &wire.GitInput{Remote: "https://git.example.test/org/repo.git", Ref: "main"},
			EndBy: time.Now().Add(30 * time.Minute), StartBy: time.Now().Add(10 * time.Minute)})
		if err != nil {
			t.Fatal(err)
		}
		return submitted.RunID
	}
	collect := func(runs []string) Collected {
		result := AwaitResult{Runs: runs, Outputs: []Output{{Name: wire.GitBundle, Data: data, Size: int64(len(data)), SHA256: hexSum(data)}}}
		collected, err := f.host.Collect(f.ctx, runs[0], result, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		return collected
	}
	plain := submit()
	if collected := collect([]string{plain}); collected.Fetched != "" || collected.NotFetched != "" {
		t.Fatalf("a run without a recorded repository: fetched %q, not fetched %q", collected.Fetched, collected.NotFetched)
	}
	recorded := submit()
	rewrite(t, f, recorded, func(local *runRecord) { local.Repo = r.work })
	if collected := collect([]string{recorded}); collected.Fetched != "wherry/"+recorded || collected.NotFetched != "" {
		t.Fatalf("fetched %q, not fetched %q; want wherry/%s", collected.Fetched, collected.NotFetched, recorded)
	}
	if got := realGit(t, r.work, "rev-parse", "refs/heads/wherry/"+recorded); got != head {
		t.Fatalf("wherry/%s at %s, want %s", recorded, got, head)
	}
	replaced := submit()
	rewrite(t, f, replaced, func(local *runRecord) { local.Repo = r.work })
	replacement := replace(t, f, replaced)
	if collected := collect([]string{replaced, replacement}); collected.Fetched != "wherry/"+replacement {
		t.Fatalf("fetched %q, not fetched %q; want wherry/%s, the chain's last run", collected.Fetched, collected.NotFetched, replacement)
	}
	if _, ok := refs(t, r.work)["refs/heads/wherry/"+replaced]; ok {
		t.Fatal("a branch was made for the replaced run")
	}
	realGit(t, r.work, "update-ref", "refs/heads/wherry/"+recorded, base)
	if collected := collect([]string{recorded}); collected.Fetched != "" || collected.NotFetched != NotFetchedExists {
		t.Fatalf("fetched %q, not fetched %q; want %s", collected.Fetched, collected.NotFetched, NotFetchedExists)
	}
}

// Through the shim, a --git . run's bundle is fetched with one command in the recorded repository: the runner's
// settings, the repository as the safe directory, the fetch's object checks and no maintenance, no tags and no
// submodules, the bundle by its absolute path, and HEAD into the branch, never forced; the ceiling is the repository's
// parent.
func TestCollectFetchCommandLine(t *testing.T) {
	f := gitFixture(t)
	shim := installGitShim(t, "2.45.0")
	top := shimRepo(t, shim, "main")
	shim.answer(t, "heads", shimOther+"\trefs/heads/main\n"+shimCommit+"\trefs/heads/feature\n")
	submitted, err := f.host.Submit(f.ctx, gitSubmit(&wire.GitInput{Remote: "."}))
	if err != nil {
		t.Fatal(err)
	}
	submitCalls := len(shim.calls(t))
	t.Chdir(t.TempDir())
	collected, err := f.host.Collect(f.ctx, submitted.RunID, AwaitResult{Outputs: []Output{{Name: wire.GitBundle, Data: testBundle, Size: int64(len(testBundle)), SHA256: hexSum(testBundle)}}}, "out")
	if err != nil || collected.Fetched != "wherry/"+submitted.RunID || collected.NotFetched != "" {
		t.Fatalf("collect = %+v, %v", collected, err)
	}
	cwd, _ := os.Getwd()
	calls := shim.calls(t)[submitCalls:]
	var subs []string
	for _, c := range calls {
		subs = append(subs, strings.Join(c.sub(), " "))
	}
	ref := "refs/heads/wherry/" + submitted.RunID
	bundle := filepath.Join(cwd, "out", wire.GitBundle)
	want := []string{"version", "rev-parse --verify --quiet " + ref, "fetch --no-tags --no-recurse-submodules " + bundle + " HEAD:" + ref}
	if !slices.Equal(subs, want) {
		t.Fatalf("git calls %q, want %q", subs, want)
	}
	fetch := calls[2]
	for _, setting := range []string{"core.hooksPath=/dev/null", "core.fsmonitor=false", "gc.auto=0", "credential.helper=", "safe.directory=" + top, "fetch.fsckObjects=true", "maintenance.auto=false"} {
		if !slices.Contains(fetch.settings(), setting) {
			t.Errorf("fetch lacks -c %s: %q", setting, fetch.argv)
		}
	}
	for _, c := range calls[1:] {
		if c.env["GIT_CEILING_DIRECTORIES"] != filepath.Dir(top) || c.env["GIT_DIR"] != "" || c.env["GIT_CONFIG_GLOBAL"] != "/dev/null" || c.env["GIT_NO_LAZY_FETCH"] != "1" {
			t.Errorf("%q environment %v", c.sub(), c.env)
		}
	}
	// The branch there at the bundle's head is left: no fetch runs.
	shim.answer(t, "wherryref", testHead+"\n")
	before := len(shim.calls(t))
	collected, err = f.host.Collect(f.ctx, submitted.RunID, AwaitResult{Outputs: []Output{{Name: wire.GitBundle, Data: testBundle, Size: int64(len(testBundle)), SHA256: hexSum(testBundle)}}}, "again")
	if err != nil || collected.Fetched != "wherry/"+submitted.RunID {
		t.Fatalf("collect at the head = %+v, %v", collected, err)
	}
	for _, c := range shim.calls(t)[before:] {
		if c.sub()[0] == "fetch" {
			t.Fatal("a fetch ran with the branch at the bundle's head")
		}
	}
}
