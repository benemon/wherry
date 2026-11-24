package client

import (
	"context"
	"encoding/base64"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/wire"
)

// realGitPath is the installed git, found before any test puts a shim on PATH.
var realGitPath, _ = exec.LookPath("git")

// realGit runs the installed git in dir with a clean environment and fails the test on a non-zero exit.
func realGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	if realGitPath == "" {
		t.Skip("git is not installed")
	}
	command := exec.Command(realGitPath, args...)
	command.Dir = dir
	command.Env = []string{"PATH=" + os.Getenv("PATH"), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@invalid",
		"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@invalid"}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
	return strings.TrimSpace(string(output))
}

// gitRepos is a bare remote and a clone of it whose main holds one pushed commit.
type gitRepos struct {
	remote, url, work string
}

func newGitRepos(t *testing.T) gitRepos {
	t.Helper()
	r := gitRepos{remote: filepath.Join(t.TempDir(), "remote.git"), work: filepath.Join(t.TempDir(), "work")}
	r.url = "file://" + r.remote
	realGit(t, "", "init", "-q", "--bare", "-b", "main", r.remote)
	realGit(t, "", "clone", "-q", r.url, r.work)
	realGit(t, r.work, "symbolic-ref", "HEAD", "refs/heads/main")
	r.commit(t, "one")
	realGit(t, r.work, "push", "-q", "-u", "origin", "main")
	return r
}

// commit adds a file named message to the clone and commits it, returning the commit.
func (r gitRepos) commit(t *testing.T, message string) string {
	t.Helper()
	if err := os.WriteFile(filepath.Join(r.work, message), []byte(message+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	realGit(t, r.work, "add", message)
	realGit(t, r.work, "commit", "-q", "-m", message)
	return realGit(t, r.work, "rev-parse", "HEAD")
}

// dotGit is every file under the clone's .git with its contents, to show a check wrote nothing.
func (r gitRepos) dotGit(t *testing.T) map[string]string {
	t.Helper()
	files := map[string]string{}
	root := filepath.Join(r.work, ".git")
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		files[strings.TrimPrefix(path, root)] = string(data)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return files
}

func newRunner(t *testing.T) *hostGit {
	t.Helper()
	runner, err := newHostGit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.close)
	return runner
}

// --git . reads origin and HEAD from the working directory's repository, anywhere below its top level, and passes a
// commit the remote advertises as a tip or an ancestor of a tip the clone holds, and nothing else; the repository is
// byte-identical afterwards.
func TestGitCurrentRepoOnRemote(t *testing.T) {
	r := newGitRepos(t)
	if err := os.Mkdir(filepath.Join(r.work, "sub"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Chdir(filepath.Join(r.work, "sub"))
	first := realGit(t, r.work, "rev-parse", "HEAD")
	before := r.dotGit(t)
	runner := newRunner(t)
	repo, err := runner.current()
	if err != nil || repo != (currentRepo{origin: r.url, commit: first, branch: "main"}) {
		t.Fatalf("current = %+v, %v; want origin %s at %s on main", repo, err, r.url, first)
	}
	if on, err := runner.onRemote(repo.origin, repo.commit); err != nil || !on {
		t.Fatalf("the pushed tip: on = %v, %v; want true", on, err)
	}
	if after := r.dotGit(t); !mapsEqual(before, after) {
		t.Fatalf(".git changed: %v", changed(before, after))
	}
	// An ancestor of the remote's tip, which the clone holds, passes without a fetch.
	second := r.commit(t, "two")
	realGit(t, r.work, "push", "-q")
	realGit(t, r.work, "reset", "-q", "--hard", first)
	before = r.dotGit(t)
	if on, err := runner.onRemote(r.url, first); err != nil || !on {
		t.Fatalf("an ancestor of the tip: on = %v, %v; want true", on, err)
	}
	if after := r.dotGit(t); !mapsEqual(before, after) {
		t.Fatalf(".git changed: %v", changed(before, after))
	}
	// An unpushed commit fails.
	realGit(t, r.work, "reset", "-q", "--hard", second)
	third := r.commit(t, "three")
	if on, err := runner.onRemote(r.url, third); err != nil || on {
		t.Fatalf("an unpushed commit: on = %v, %v; want false", on, err)
	}
	// A branch deleted on the remote does not count, though its remote-tracking ref remains.
	realGit(t, r.work, "push", "-q", "origin", "HEAD:refs/heads/feature")
	realGit(t, r.remote, "update-ref", "-d", "refs/heads/feature")
	if tracking := realGit(t, r.work, "rev-parse", "refs/remotes/origin/feature"); tracking != third {
		t.Fatalf("remote-tracking ref %s, want %s", tracking, third)
	}
	before = r.dotGit(t)
	if on, err := runner.onRemote(r.url, third); err != nil || on {
		t.Fatalf("a deleted remote branch: on = %v, %v; want false", on, err)
	}
	if after := r.dotGit(t); !mapsEqual(before, after) {
		t.Fatalf(".git changed: %v", changed(before, after))
	}
	// Detached: no branch.
	realGit(t, r.work, "checkout", "-q", "--detach", second)
	if repo, err := runner.current(); err != nil || repo.branch != "" || repo.commit != second {
		t.Fatalf("detached current = %+v, %v; want %s and no branch", repo, err, second)
	}
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for name, value := range a {
		if other, ok := b[name]; !ok || other != value {
			return false
		}
	}
	return true
}

func changed(before, after map[string]string) []string {
	var names []string
	for name := range after {
		if before[name] != after[name] {
			names = append(names, name)
		}
	}
	for name := range before {
		if _, ok := after[name]; !ok {
			names = append(names, "-"+name)
		}
	}
	return slices.Sorted(slices.Values(names))
}

// A repository without origin, without a commit, or none at all is refused with a hint.
func TestGitCurrentRepoRefusals(t *testing.T) {
	r := newGitRepos(t)
	runner := newRunner(t)
	t.Chdir(r.work)
	realGit(t, r.work, "remote", "rename", "origin", "upstream")
	if _, err := runner.current(); err == nil || !strings.Contains(err.Error(), "no origin remote") {
		t.Fatalf("without origin: %v", err)
	}
	empty := filepath.Join(t.TempDir(), "empty")
	realGit(t, "", "init", "-q", "-b", "main", empty)
	realGit(t, empty, "remote", "add", "origin", r.url)
	t.Chdir(empty)
	if _, err := runner.current(); err == nil || !strings.Contains(err.Error(), "no commit") {
		t.Fatalf("unborn HEAD: %v", err)
	}
	t.Chdir(t.TempDir())
	if _, err := runner.current(); err == nil || !strings.Contains(err.Error(), "not in a git repository") {
		t.Fatalf("outside a repository: %v", err)
	}
}

// An unpinned remote pins the commit its HEAD names; an unborn HEAD is refused.
func TestGitDefaultHead(t *testing.T) {
	r := newGitRepos(t)
	runner := newRunner(t)
	head := realGit(t, r.work, "rev-parse", "HEAD")
	if commit, err := runner.defaultHead(r.url); err != nil || commit != head {
		t.Fatalf("default head %s, %v; want %s", commit, err, head)
	}
	empty := filepath.Join(t.TempDir(), "empty.git")
	realGit(t, "", "init", "-q", "--bare", "-b", "main", empty)
	if _, err := runner.defaultHead("file://" + empty); err == nil || !strings.Contains(err.Error(), "HEAD names no commit") {
		t.Fatalf("unborn HEAD: %v", err)
	}
	if _, err := runner.defaultHead("file://" + filepath.Join(t.TempDir(), "missing.git")); err == nil || err.Error() != "git ls-remote failed (exit 128)" {
		t.Fatalf("missing remote: %v", err)
	}
}

// hostileGitEnv sets the variables and configurations that would redirect or trace the host's git, were they read.
func hostileGitEnv(t *testing.T, r gitRepos) {
	t.Helper()
	home := t.TempDir()
	global := "[url \"file:///nonexistent/\"]\n\tinsteadOf = file://\n[credential]\n\thelper = !exit 7\n[remote \"origin\"]\n\turl = https://evil.example/global\n"
	for _, path := range []string{filepath.Join(home, ".gitconfig"), filepath.Join(home, "xdg", "git", "config")} {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(global), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	other := filepath.Join(t.TempDir(), "other")
	realGit(t, "", "init", "-q", "-b", "main", other)
	realGit(t, other, "remote", "add", "origin", "https://evil.example/other")
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "xdg"))
	t.Setenv("GIT_CONFIG_PARAMETERS", "'remote.origin.url=https://evil.example/parameters'")
	t.Setenv("GIT_CONFIG_COUNT", "1")
	t.Setenv("GIT_CONFIG_KEY_0", "remote.origin.url")
	t.Setenv("GIT_CONFIG_VALUE_0", "https://evil.example/count")
	t.Setenv("GIT_DIR", filepath.Join(other, ".git"))
	t.Setenv("GIT_SSH", "/bin/false")
	t.Setenv("GIT_SSH_COMMAND", "false")
	t.Setenv("GIT_TRACE_CURL", "1")
	t.Setenv("GIT_TRACE", "1")
	t.Setenv("SSH_AUTH_SOCK", filepath.Join(home, "agent.sock"))
	t.Setenv("SSH_ASKPASS", "/bin/false")
	hooks := filepath.Join(t.TempDir(), "hooks")
	if err := os.Mkdir(hooks, 0o700); err != nil {
		t.Fatal(err)
	}
	fired := filepath.Join(hooks, "fired")
	for _, name := range []string{"monitor", "reference-transaction", "post-index-change"} {
		if err := os.WriteFile(filepath.Join(hooks, name), []byte("#!/bin/sh\ntouch '"+fired+"'\n"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	realGit(t, r.work, "config", "core.fsmonitor", filepath.Join(hooks, "monitor"))
	realGit(t, r.work, "config", "core.hooksPath", hooks)
	realGit(t, r.work, "config", "http.extraHeader", "Authorization: Basic hostile")
	realGit(t, r.work, "config", "url.file:///nonexistent/.insteadOf", "file://")
	t.Cleanup(func() {
		if _, err := os.Stat(fired); err == nil {
			t.Error("a hook or the file system monitor ran")
		}
	})
}

// With hostile environment variables and global and repository configurations, --git . still reads the repository's
// own origin and the remote it names.
func TestGitIsolationRealGit(t *testing.T) {
	r := newGitRepos(t)
	hostileGitEnv(t, r)
	t.Chdir(r.work)
	head := realGit(t, r.work, "rev-parse", "HEAD")
	runner := newRunner(t)
	repo, err := runner.current()
	if err != nil || repo != (currentRepo{origin: r.url, commit: head, branch: "main"}) {
		t.Fatalf("current = %+v, %v; want origin %s at %s on main", repo, err, r.url, head)
	}
	if on, err := runner.onRemote(repo.origin, repo.commit); err != nil || !on {
		t.Fatalf("on = %v, %v; want true", on, err)
	}
	if commit, err := runner.defaultHead(r.url); err != nil || commit != head {
		t.Fatalf("default head %s, %v; want %s", commit, err, head)
	}
}

// gitShim is a git stand-in first on PATH: it records every call's argv and environment under dir and answers from the
// files the test writes there.
type gitShim struct {
	dir string
}

const shimToken = "forge-token-value-kept-off-command-lines"

func installGitShim(t *testing.T, version string) *gitShim {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(filepath.Join(dir, "calls"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	script := `#!/bin/sh
d='` + dir + `'
n=$(cat "$d/n" 2>/dev/null || echo 0); n=$((n+1)); echo $n > "$d/n"
printf '%s\n' "$@" > "$d/calls/$n.argv"
env > "$d/calls/$n.env"
case "$GIT_SSH_COMMAND" in *" -i '"*) k=${GIT_SSH_COMMAND#*-i \'}; k=${k%%\'*}; stat -c %a "$k" > "$d/calls/$n.keymode"; cp "$k" "$d/calls/$n.key";; esac
skip=; sub=
for a in "$@"; do
  if [ -n "$skip" ]; then skip=; continue; fi
  case "$a" in -c) skip=1; continue;; esac
  sub=$a; break
done
case "$sub" in
  version) echo "git version ` + version + `";;
  config) cat "$d/origin";;
  rev-parse) case "$*" in *abbrev-ref*) cat "$d/branch";; *) cat "$d/commit";; esac;;
  ls-remote) case "$*" in *--symref*) cat "$d/symref";; *) cat "$d/heads";; esac;;
  cat-file) exit $(cat "$d/catfile" 2>/dev/null || echo 0);;
  merge-base) exit $(cat "$d/mergebase" 2>/dev/null || echo 1);;
  *) exit 1;;
esac
`
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	return &gitShim{dir: dir}
}

func (s *gitShim) answer(t *testing.T, name, contents string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(s.dir, name), []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}

// call is one recorded git call: its argv after the shim and its environment, with what the shell adds for itself
// removed.
type call struct {
	argv []string
	env  map[string]string
	// key and keyMode are the ssh identity file's contents and mode while the call ran, when GIT_SSH_COMMAND named one.
	key, keyMode string
}

func (s *gitShim) calls(t *testing.T) []call {
	t.Helper()
	var calls []call
	for n := 1; ; n++ {
		argv, err := os.ReadFile(filepath.Join(s.dir, "calls", strconv.Itoa(n)+".argv"))
		if errors.Is(err, fs.ErrNotExist) {
			return calls
		}
		if err != nil {
			t.Fatal(err)
		}
		env, err := os.ReadFile(filepath.Join(s.dir, "calls", strconv.Itoa(n)+".env"))
		if err != nil {
			t.Fatal(err)
		}
		c := call{argv: strings.Split(strings.TrimSuffix(string(argv), "\n"), "\n"), env: map[string]string{}}
		for line := range strings.Lines(string(env)) {
			name, value, _ := strings.Cut(strings.TrimSuffix(line, "\n"), "=")
			if !slices.Contains([]string{"PWD", "OLDPWD", "SHLVL", "_"}, name) {
				c.env[name] = value
			}
		}
		key, _ := os.ReadFile(filepath.Join(s.dir, "calls", strconv.Itoa(n)+".key"))
		mode, _ := os.ReadFile(filepath.Join(s.dir, "calls", strconv.Itoa(n)+".keymode"))
		c.key, c.keyMode = string(key), strings.TrimSpace(string(mode))
		calls = append(calls, c)
	}
}

// sub is the call's subcommand and the arguments after it.
func (c call) sub() []string {
	for i := 0; i < len(c.argv); i++ {
		if c.argv[i] == "-c" {
			i++
			continue
		}
		return c.argv[i:]
	}
	return nil
}

// settings are the call's -c values.
func (c call) settings() []string {
	var settings []string
	for i := 0; i+1 < len(c.argv); i++ {
		if c.argv[i] == "-c" {
			settings = append(settings, c.argv[i+1])
			i++
		}
	}
	return settings
}

// gitFixture adds pool-g, with git.example.test as its vcs host, a live receiver in it alone, and a git entry whose
// credential is a file holding shimToken.
func gitFixture(t *testing.T) *fixture {
	t.Helper()
	f := newFixture(t)
	terms := testTerms()
	terms.VCS = []string{"git.example.test"}
	if err := f.service.PoolApply(f.ctx, "pool-g", terms, false); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantPoolAdd(f.ctx, "tenant-a", "pool-g"); err != nil {
		t.Fatal(err)
	}
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte(shimToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	f.host.credentials["forge-token"] = credential.Source{File: token}
	f.host.git = []GitCredential{{URL: "https://git.example.test/", Credential: "forge-token"}}
	f.addReceiver(t, "receiver-g", "pool-g").advertise(t, f.ctx, nil)
	return f
}

func gitSubmit(git *wire.GitInput) SubmitInput {
	return SubmitInput{Selector: defaultSelector(), Prompt: "work on it", Git: git, EndBy: time.Now().Add(30 * time.Minute), StartBy: time.Now().Add(10 * time.Minute)}
}

const shimOrigin, shimCommit, shimOther = "https://git.example.test/org/repo.git", "0123456789abcdef0123456789abcdef01234567", "89abcdef0123456789abcdef0123456789abcdef"

// shimRepo makes a directory with a .git for the shim to stand in for, and moves into it.
func shimRepo(t *testing.T, shim *gitShim, branch string) string {
	t.Helper()
	repo := t.TempDir()
	if err := os.Mkdir(filepath.Join(repo, ".git"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Chdir(repo)
	shim.answer(t, "origin", shimOrigin+"\n")
	shim.answer(t, "commit", shimCommit+"\n")
	shim.answer(t, "branch", branch+"\n")
	top, err := filepath.EvalSymlinks(repo)
	if err != nil {
		t.Fatal(err)
	}
	return top
}

// Through the shim, --git . submits origin's URL and HEAD's commit with the entry's credential, and every git call
// carries the settings and the allowlisted environment alone: the hostile variables reach none, and the token no
// command line, only the environment of ls-remote as an Authorization header.
func TestSubmitGitCurrentRepo(t *testing.T) {
	f := gitFixture(t)
	shim := installGitShim(t, "2.39.5")
	top := shimRepo(t, shim, "main")
	shim.answer(t, "heads", shimOther+"\trefs/heads/main\n"+shimCommit+"\trefs/heads/feature\n")
	hostileGitEnv(t, newGitRepos(t))
	submitted, err := f.host.Submit(f.ctx, gitSubmit(&wire.GitInput{Remote: "."}))
	if err != nil {
		t.Fatal(err)
	}
	task := f.receivers["receiver-g"].claim(t, f.ctx, submitted.RunID)[0].submission
	want := wire.GitInput{Remote: shimOrigin, Ref: shimCommit, Credential: "forge-token"}
	if git := task.Wherry.Git; git == nil || *git != want {
		t.Fatalf("git %+v, want %+v", git, want)
	}
	calls := shim.calls(t)
	var subs []string
	for _, c := range calls {
		subs = append(subs, strings.Join(c.sub(), " "))
	}
	wantSubs := []string{"config --get remote.origin.url", "rev-parse --verify --quiet HEAD^{commit}", "rev-parse --abbrev-ref HEAD", "version", "ls-remote --heads --tags " + shimOrigin}
	if !slices.Equal(subs, wantSubs) {
		t.Fatalf("git calls %q, want %q", subs, wantSubs)
	}
	allowed := slices.Concat(gitInherited, []string{"GIT_CONFIG_NOSYSTEM", "GIT_CONFIG_GLOBAL", "GIT_TERMINAL_PROMPT", "GIT_CONFIG_COUNT", "GIT_CONFIG_KEY_0", "GIT_CONFIG_VALUE_0", "GIT_CONFIG_KEY_1", "GIT_CONFIG_VALUE_1"})
	for i, c := range calls {
		if strings.Contains(strings.Join(c.argv, "\n"), shimToken) {
			t.Fatalf("call %d: the token is on the command line", i)
		}
		settings := c.settings()
		for _, setting := range gitSettings {
			if !slices.Contains(settings, setting) {
				t.Errorf("call %d %q lacks -c %s", i, c.sub(), setting)
			}
		}
		inRepo := c.sub()[0] != "ls-remote" && c.sub()[0] != "version"
		if slices.Contains(settings, "safe.directory="+top) != inRepo {
			t.Errorf("call %d %q: safe.directory %v, want it in the repository alone", i, c.sub(), slices.Contains(settings, "safe.directory="+top))
		}
		for name := range c.env {
			if !slices.Contains(allowed, name) {
				t.Errorf("call %d %q: environment carries %s", i, c.sub(), name)
			}
		}
		for _, name := range []string{"GIT_CONFIG_NOSYSTEM", "GIT_TERMINAL_PROMPT", "PATH"} {
			if c.env[name] == "" {
				t.Errorf("call %d %q: environment lacks %s", i, c.sub(), name)
			}
		}
		if c.env["GIT_CONFIG_GLOBAL"] != "/dev/null" || c.env["GIT_CONFIG_KEY_0"] != "http.extraHeader" || c.env["GIT_CONFIG_VALUE_0"] != "" {
			t.Errorf("call %d %q: environment %v lacks the global config and the header reset", i, c.sub(), c.env)
		}
		header := "Authorization: Basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:"+shimToken))
		if c.sub()[0] == "ls-remote" && (c.env["GIT_CONFIG_COUNT"] != "2" || c.env["GIT_CONFIG_KEY_1"] != "http.extraHeader" || c.env["GIT_CONFIG_VALUE_1"] != header) {
			t.Errorf("ls-remote environment %v, want the token as a second http.extraHeader", c.env)
		}
		if c.sub()[0] != "ls-remote" && c.env["GIT_CONFIG_COUNT"] != "1" {
			t.Errorf("call %d %q: environment %v carries a header", i, c.sub(), c.env)
		}
	}
}

// An unpushed commit is refused naming the branch, or the commit when HEAD is detached; the error never carries the
// token.
func TestSubmitGitCurrentRepoUnpushed(t *testing.T) {
	f := gitFixture(t)
	shim := installGitShim(t, "2.39.5")
	shimRepo(t, shim, "main")
	shim.answer(t, "heads", shimOther+"\trefs/heads/main\n")
	shim.answer(t, "mergebase", "1\n")
	_, err := f.host.Submit(f.ctx, gitSubmit(&wire.GitInput{Remote: "."}))
	want := "commit 0123456789ab is not on https://git.example.test/org/repo.git: push main first, or git fetch and retry"
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), want) || strings.Contains(err.Error(), shimToken) {
		t.Fatalf("submit error %v, want %q", err, want)
	}
	shim.answer(t, "branch", "HEAD\n")
	_, err = f.host.Submit(f.ctx, gitSubmit(&wire.GitInput{Remote: "."}))
	if err == nil || !strings.Contains(err.Error(), "push the commit to a branch first, or git fetch and retry") {
		t.Fatalf("detached submit error %v", err)
	}
	// An ancestor of a tip the repository holds passes; a tip it lacks is not checked.
	shim.answer(t, "mergebase", "0\n")
	shim.answer(t, "catfile", "1\n")
	if _, err := f.host.Submit(f.ctx, gitSubmit(&wire.GitInput{Remote: "."})); err == nil || !strings.Contains(err.Error(), "is not on") {
		t.Fatalf("a tip the repository lacks: %v", err)
	}
	shim.answer(t, "catfile", "0\n")
	if _, err := f.host.Submit(f.ctx, gitSubmit(&wire.GitInput{Remote: "."})); err != nil {
		t.Fatalf("an ancestor of a held tip: %v", err)
	}
	if _, err := f.host.Submit(f.ctx, gitSubmit(&wire.GitInput{Remote: ".", Ref: "main"})); err == nil || !strings.Contains(err.Error(), "drop the ref") {
		t.Fatalf("a ref with the current repository: %v", err)
	}
	for _, c := range shim.calls(t) {
		if c.sub()[0] == "fetch" || c.sub()[0] == "for-each-ref" {
			t.Fatalf("git %q ran", c.sub())
		}
	}
}

// An unpinned remote submits the commit of the line whose ref is exactly HEAD, and is refused when there is none.
func TestSubmitGitUnpinned(t *testing.T) {
	f := gitFixture(t)
	shim := installGitShim(t, "2.39.5")
	shim.answer(t, "symref", "ref: refs/heads/main\tHEAD\n"+shimCommit+"\tHEAD\n"+shimOther+"\trefs/remotes/origin/HEAD\n")
	submitted, err := f.host.Submit(f.ctx, gitSubmit(&wire.GitInput{Remote: shimOrigin}))
	if err != nil {
		t.Fatal(err)
	}
	task := f.receivers["receiver-g"].claim(t, f.ctx, submitted.RunID)[0].submission
	want := wire.GitInput{Remote: shimOrigin, Ref: shimCommit, Credential: "forge-token"}
	if git := task.Wherry.Git; git == nil || *git != want {
		t.Fatalf("git %+v, want %+v", git, want)
	}
	calls := shim.calls(t)
	if last := calls[len(calls)-1].sub(); !slices.Equal(last, []string{"ls-remote", "--symref", shimOrigin, "HEAD"}) {
		t.Fatalf("last git call %q, want ls-remote --symref HEAD", last)
	}
	for _, symref := range []string{"", "ref: refs/heads/main\tHEAD\n", shimOther + "\trefs/remotes/origin/HEAD\n"} {
		shim.answer(t, "symref", symref)
		if _, err := f.host.Submit(f.ctx, gitSubmit(&wire.GitInput{Remote: shimOrigin})); !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "HEAD names no commit") {
			t.Fatalf("symref %q: submit error %v", symref, err)
		}
	}
	// A pinned remote runs no git.
	before := len(shim.calls(t))
	if _, err := f.host.Submit(f.ctx, gitSubmit(&wire.GitInput{Remote: shimOrigin, Ref: "main"})); err != nil {
		t.Fatal(err)
	}
	if after := len(shim.calls(t)); after != before {
		t.Fatalf("a pinned remote ran %d git calls", after-before)
	}
}

// An ssh entry's key reaches ssh as a 0600 file that is gone after the call, through GIT_SSH_COMMAND with the entry's
// host key file; a token's version check is not run.
func TestSubmitGitSSHKey(t *testing.T) {
	f := gitFixture(t)
	dir := t.TempDir()
	hostKey := "git.example.test ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl\n"
	for name, contents := range map[string]string{"known_hosts": hostKey, "deploy-key": "-----BEGIN KEY-----\nabc\n-----END KEY-----"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	f.host.credentials["deploy-key"] = credential.Source{File: filepath.Join(dir, "deploy-key")}
	f.host.git = []GitCredential{{URL: "ssh://git@git.example.test/", Credential: "deploy-key", HostKey: filepath.Join(dir, "known_hosts")}}
	shim := installGitShim(t, "2.30.0")
	shim.answer(t, "symref", "ref: refs/heads/main\tHEAD\n"+shimCommit+"\tHEAD\n")
	submitted, err := f.host.Submit(f.ctx, gitSubmit(&wire.GitInput{Remote: "git@git.example.test:org/repo.git"}))
	if err != nil {
		t.Fatal(err)
	}
	task := f.receivers["receiver-g"].claim(t, f.ctx, submitted.RunID)[0].submission
	want := wire.GitInput{Remote: "ssh://git@git.example.test/org/repo.git", Ref: shimCommit, Credential: "deploy-key", HostKey: hostKey}
	if git := task.Wherry.Git; git == nil || *git != want {
		t.Fatalf("git %+v, want %+v", git, want)
	}
	calls := shim.calls(t)
	if len(calls) != 1 || calls[0].sub()[0] != "ls-remote" {
		t.Fatalf("git calls %d, want ls-remote alone", len(calls))
	}
	c := calls[0]
	ssh := c.env["GIT_SSH_COMMAND"]
	_, after, _ := strings.Cut(ssh, "-i '")
	keyFile, _, _ := strings.Cut(after, "'")
	if c.key != "-----BEGIN KEY-----\nabc\n-----END KEY-----\n" || c.keyMode != "600" {
		t.Fatalf("key file %q mode %q, want the key with a final newline at 0600", c.key, c.keyMode)
	}
	if _, err := os.Stat(keyFile); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("key file %s after submit: %v, want gone", keyFile, err)
	}
	for _, option := range []string{"ssh -F /dev/null -i '", "-o IdentitiesOnly=yes", "-o IdentityAgent=none", "-o ProxyCommand=none", "-o BatchMode=yes", "-o StrictHostKeyChecking=yes",
		"-o UserKnownHostsFile='" + filepath.Join(dir, "known_hosts") + "'", "-o GlobalKnownHostsFile=/dev/null"} {
		if !strings.Contains(ssh, option) {
			t.Errorf("GIT_SSH_COMMAND %q lacks %s", ssh, option)
		}
	}
	if c.env["GIT_CONFIG_COUNT"] != "1" {
		t.Errorf("ls-remote environment %v carries a header", c.env)
	}
}

// Without git on PATH both forms are refused naming git; with an https credential, a git older than 2.31 is refused
// naming its version, and an anonymous remote is not.
func TestSubmitGitNeedsGit(t *testing.T) {
	f := gitFixture(t)
	shim := installGitShim(t, "2.30.2")
	shim.answer(t, "symref", "ref: refs/heads/main\tHEAD\n"+shimCommit+"\tHEAD\n")
	_, err := f.host.Submit(f.ctx, gitSubmit(&wire.GitInput{Remote: shimOrigin}))
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "git 2.30 is older than 2.31") {
		t.Fatalf("submit error %v, want the version refused", err)
	}
	f.host.git = nil
	if _, err := f.host.Submit(f.ctx, gitSubmit(&wire.GitInput{Remote: shimOrigin})); err != nil {
		t.Fatalf("anonymous on 2.30: %v", err)
	}
	for _, c := range shim.calls(t) {
		if c.sub()[0] == "version" && c.env["GIT_CONFIG_COUNT"] != "1" {
			t.Fatalf("version environment %v", c.env)
		}
	}
	t.Setenv("PATH", t.TempDir())
	for _, git := range []*wire.GitInput{{Remote: "."}, {Remote: shimOrigin}} {
		if _, err := f.host.Submit(f.ctx, gitSubmit(git)); !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "git is not on PATH") {
			t.Fatalf("submit %+v without git: %v", git, err)
		}
	}
}

// ParseGit splits at the last @ only when what precedes it is a remote.
func TestParseGit(t *testing.T) {
	for value, want := range map[string][2]string{
		".": {".", ""},
		"https://git.example.test/org/repo.git@main": {"https://git.example.test/org/repo.git", "main"},
		"https://git.example.test/org/repo.git":      {"https://git.example.test/org/repo.git", ""},
		"https://git.example.test/a@b":               {"https://git.example.test/a", "b"},
		"ssh://git@git.example.test/org/repo.git":    {"ssh://git@git.example.test/org/repo.git", ""},
		"ssh://git@git.example.test/org/repo.git@v1": {"ssh://git@git.example.test/org/repo.git", "v1"},
		"git@git.example.test:org/repo.git":          {"git@git.example.test:org/repo.git", ""},
		"git@git.example.test:org/repo.git@main":     {"git@git.example.test:org/repo.git", "main"},
		".@main":                                     {".@main", ""},
	} {
		if remote, ref := ParseGit(value); remote != want[0] || ref != want[1] {
			t.Errorf("ParseGit(%q) = %q, %q; want %q, %q", value, remote, ref, want[0], want[1])
		}
	}
}
