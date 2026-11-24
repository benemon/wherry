package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/benemon/wherry/internal/wire"
)

// gitInherited are the variables the host's git gets from the process. Every other one is left out: a GIT_* or SSH_*
// variable, or a configuration under HOME, could redirect the remote, run a program or record a token.
var gitInherited = []string{"PATH", "TMPDIR", "LANG", "LC_ALL", "HTTPS_PROXY", "HTTP_PROXY", "NO_PROXY", "https_proxy", "http_proxy", "no_proxy"}

// gitSettings go on every command line: no hooks, no file system monitor, no gc and no credential helper, whatever the
// repository's configuration says.
var gitSettings = []string{"core.hooksPath=/dev/null", "core.fsmonitor=false", "gc.auto=0", "credential.helper="}

// ParseGit splits a --git value into its remote and ref: "." is the current repository, with no ref; otherwise the text
// before the last @ is the remote when it is one, and the rest the ref; else the whole value is an unpinned remote, so
// ssh://git@host/repo and git@host:repo need no ref.
func ParseGit(value string) (remote, ref string) {
	at := strings.LastIndex(value, "@")
	if value == "." || at < 0 || !isRemote(sshRemote(value[:at])) {
		return value, ""
	}
	return value[:at], value[at+1:]
}

// isRemote reports whether remote is an https or ssh URL naming a VCS host and a path.
func isRemote(remote string) bool {
	parsed, err := url.Parse(remote)
	return err == nil && (parsed.Scheme == "https" || parsed.Scheme == "ssh") && parsed.Path != "" && wire.ValidVCSHost(wire.GitInput{Remote: remote}.VCSHost())
}

// hostGit runs git on the host for a submission: --git . reads the working directory's repository with it, and both
// that form and an unpinned remote ask the remote for its heads. It reads no system or global configuration, inherits
// gitInherited alone and never prompts. Commands run in repo, the current repository's top level, or in scratch, a
// directory of its own that holds nothing but an ssh key. header is an http.extraHeader value sent with ls-remote, ssh
// the ssh command it runs; both are empty for an anonymous remote.
type hostGit struct {
	ctx     context.Context
	scratch string
	repo    string
	header  string
	ssh     string
}

// newHostGit checks that git is on PATH and makes the runner's scratch directory, which close removes.
func newHostGit(ctx context.Context) (*hostGit, error) {
	if _, err := exec.LookPath("git"); err != nil {
		return nil, errors.New("git is not on PATH, which --git . and a --git remote without @REF need")
	}
	scratch, err := os.MkdirTemp("", "wherry-git-")
	if err != nil {
		return nil, err
	}
	return &hostGit{ctx: ctx, scratch: scratch}, nil
}

func (g *hostGit) close() {
	os.RemoveAll(g.scratch)
}

func (g *hostGit) environment() []string {
	env := []string{"GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0"}
	for _, name := range gitInherited {
		if value, ok := os.LookupEnv(name); ok {
			env = append(env, name+"="+value)
		}
	}
	// http.extraHeader is a list an empty value resets. The reset goes here, not in a -c setting: git reads -c after
	// these entries, so it would reset the header set here too.
	values := []string{""}
	if g.header != "" {
		values = append(values, g.header)
	}
	env = append(env, "GIT_CONFIG_COUNT="+strconv.Itoa(len(values)))
	for i, value := range values {
		env = append(env, fmt.Sprintf("GIT_CONFIG_KEY_%d=http.extraHeader", i), fmt.Sprintf("GIT_CONFIG_VALUE_%d=%s", i, value))
	}
	if g.ssh != "" {
		env = append(env, "GIT_SSH_COMMAND="+g.ssh)
	}
	return env
}

// gitExit is git ending with a non-zero status at a step.
type gitExit struct {
	step string
	code int
}

func (e *gitExit) Error() string {
	return fmt.Sprintf("git %s failed (exit %d)", e.step, e.code)
}

// exited reports whether err is git exiting with code.
func exited(err error, code int) bool {
	var exit *gitExit
	return errors.As(err, &exit) && exit.code == code
}

// run runs git in dir and returns its stdout. Its error names the step, never git's stderr, which can quote the remote.
// In the repository, the repository alone is a safe directory.
func (g *hostGit) run(dir string, args ...string) (string, error) {
	argv := make([]string, 0, 2*len(gitSettings)+2+len(args))
	for _, setting := range gitSettings {
		argv = append(argv, "-c", setting)
	}
	if dir == g.repo {
		argv = append(argv, "-c", "safe.directory="+g.repo)
	}
	argv = append(argv, args...)
	command := exec.CommandContext(g.ctx, "git", argv...)
	var stdout bytes.Buffer
	command.Dir, command.Env, command.Stdout = dir, g.environment(), &stdout
	err := command.Run()
	var exit *exec.ExitError
	switch {
	case errors.As(err, &exit):
		return "", &gitExit{step: args[0], code: exit.ExitCode()}
	case err != nil:
		return "", fmt.Errorf("git %s: %w", args[0], err)
	}
	return stdout.String(), nil
}

// currentRepo is the working directory's repository as --git . reads it: origin's URL as the configuration holds it,
// before any insteadOf rewrite, and HEAD's commit and branch, the branch empty when HEAD is detached.
type currentRepo struct {
	origin, commit, branch string
}

// current finds the repository holding the working directory, by its .git, and reads it. Nothing is written to it.
func (g *hostGit) current() (currentRepo, error) {
	top, err := os.Getwd()
	if err != nil {
		return currentRepo{}, err
	}
	for {
		if _, err := os.Stat(filepath.Join(top, ".git")); err == nil {
			break
		}
		parent := filepath.Dir(top)
		if parent == top {
			return currentRepo{}, errors.New("the working directory is not in a git repository")
		}
		top = parent
	}
	if g.repo, err = filepath.EvalSymlinks(top); err != nil {
		return currentRepo{}, err
	}
	origin, err := g.run(g.repo, "config", "--get", "remote.origin.url")
	if exited(err, 1) {
		return currentRepo{}, errors.New("the repository has no origin remote: add one, or name the remote with --git URL")
	}
	if err != nil {
		return currentRepo{}, err
	}
	commit, err := g.run(g.repo, "rev-parse", "--verify", "--quiet", "HEAD^{commit}")
	if exited(err, 1) {
		return currentRepo{}, errors.New("the repository has no commit on HEAD")
	}
	if err != nil {
		return currentRepo{}, err
	}
	branch, err := g.run(g.repo, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return currentRepo{}, err
	}
	repo := currentRepo{origin: strings.TrimSpace(origin), commit: strings.TrimSpace(commit), branch: strings.TrimSpace(branch)}
	if repo.branch == "HEAD" {
		repo.branch = ""
	}
	return repo, nil
}

// onRemote reports whether commit is on the remote at remote: a branch or tag tip it advertises, or an ancestor of one
// the repository holds. There is no fetch, so a tip the repository lacks is not checked. A remote-tracking ref says
// nothing: its branch may be gone from the remote.
func (g *hostGit) onRemote(remote, commit string) (bool, error) {
	out, err := g.run(g.scratch, "ls-remote", "--heads", "--tags", remote)
	if err != nil {
		return false, err
	}
	var tips []string
	for line := range strings.Lines(out) {
		tip, _, _ := strings.Cut(strings.TrimSuffix(line, "\n"), "\t")
		if tip == commit {
			return true, nil
		}
		if wire.ValidCommit(tip) {
			tips = append(tips, tip)
		}
	}
	for _, tip := range tips {
		var exit *gitExit
		if _, err := g.run(g.repo, "cat-file", "-e", tip+"^{commit}"); errors.As(err, &exit) {
			continue
		} else if err != nil {
			return false, err
		}
		_, err := g.run(g.repo, "merge-base", "--is-ancestor", commit, tip)
		switch {
		case err == nil:
			return true, nil
		case !exited(err, 1):
			return false, err
		}
	}
	return false, nil
}

// defaultHead is the commit the remote's HEAD names: the ls-remote --symref line whose ref is exactly HEAD. A remote
// whose HEAD names no commit, as an empty repository's does, has none.
func (g *hostGit) defaultHead(remote string) (string, error) {
	out, err := g.run(g.scratch, "ls-remote", "--symref", remote, "HEAD")
	if err != nil {
		return "", err
	}
	for line := range strings.Lines(out) {
		oid, ref, _ := strings.Cut(strings.TrimSuffix(line, "\n"), "\t")
		if ref == "HEAD" && wire.ValidCommit(oid) {
			return oid, nil
		}
	}
	return "", errors.New("the remote's HEAD names no commit: name a branch, tag or commit with @REF")
}

// version is git's major and minor version.
func (g *hostGit) version() (major, minor int, err error) {
	out, err := g.run(g.scratch, "version")
	if err != nil {
		return 0, 0, err
	}
	if _, err := fmt.Sscanf(strings.TrimPrefix(out, "git version "), "%d.%d", &major, &minor); err != nil {
		return 0, 0, errors.New("git version named no version")
	}
	return major, minor, nil
}

// shellQuote quotes path for the shell GIT_SSH_COMMAND runs through.
func shellQuote(path string) string {
	return "'" + strings.ReplaceAll(path, "'", `'\''`) + "'"
}
