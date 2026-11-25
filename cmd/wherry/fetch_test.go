package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/wire"
)

// gitRun runs the installed git in dir with a clean environment and fails the test on a non-zero exit.
func gitRun(t *testing.T, dir string, args ...string) string {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	command := exec.Command("git", args...)
	command.Dir = dir
	command.Env = []string{"PATH=" + os.Getenv("PATH"), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@invalid", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@invalid"}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
	return strings.TrimSpace(string(output))
}

// fetchRepo is the user's repository, work, a clone of a bare remote with one pushed commit, and an agent's clone of
// the same remote, where each round's commit is made.
type fetchRepo struct {
	work, agent string
}

func newFetchRepo(t *testing.T) fetchRepo {
	t.Helper()
	remote := filepath.Join(t.TempDir(), "remote.git")
	r := fetchRepo{work: filepath.Join(t.TempDir(), "work"), agent: filepath.Join(t.TempDir(), "agent")}
	gitRun(t, "", "init", "-q", "--bare", "-b", "main", remote)
	gitRun(t, "", "clone", "-q", "file://"+remote, r.work)
	if err := os.WriteFile(filepath.Join(r.work, "one"), []byte("one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, r.work, "add", "one")
	gitRun(t, r.work, "commit", "-q", "-m", "one")
	gitRun(t, r.work, "push", "-q", "-u", "origin", "main")
	gitRun(t, "", "clone", "-q", "file://"+remote, r.agent)
	return r
}

// round commits message in the agent's clone and returns the bundle of that commit over the one before, as an agent
// returns its round's changes.bundle, and the commit.
func (r fetchRepo) round(t *testing.T, message string) (bundle, head string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(r.agent, message), []byte(message+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, r.agent, "add", message)
	gitRun(t, r.agent, "commit", "-q", "-m", message)
	path := filepath.Join(t.TempDir(), wire.GitBundle)
	gitRun(t, r.agent, "bundle", "create", path, "HEAD~1..HEAD")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data), gitRun(t, r.agent, "rev-parse", "HEAD")
}

// branch is the commit the repository's wherry/<run> names, or "" without one.
func (r fetchRepo) branch(t *testing.T, run string) string {
	t.Helper()
	command := exec.Command("git", "rev-parse", "--verify", "--quiet", "refs/heads/wherry/"+run)
	command.Dir = r.work
	command.Env = []string{"PATH=" + os.Getenv("PATH"), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null"}
	out, _ := command.Output()
	return strings.TrimSpace(string(out))
}

// recordRepo writes repo into the run's local record, as a --git . submission records the repository it read.
func (b *bus) recordRepo(t *testing.T, runID, repo string) {
	t.Helper()
	path := filepath.Join(b.stateDir, "runs", runID+".json")
	record, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	record = bytes.TrimSuffix(bytes.TrimSpace(record), []byte("}"))
	if err := os.WriteFile(path, append(record, []byte(`,"repo":`+strconv.Quote(repo)+"}")...), 0o600); err != nil {
		t.Fatal(err)
	}
}

// fetchedLine is what result prints for a bundle fetched into wherry/<run>.
func fetchedLine(run string) string {
	return "fetched into wherry/" + run + ": git merge wherry/" + run + "\n"
}

// result, run from another directory, fetches the bundle into the recorded repository's wherry/<run> and says how to
// merge it; with the branch already at another commit it leaves it, exits 0 and says why, with the fetch by hand.
func TestResultFetchesIntoTheRecordedRepository(t *testing.T) {
	b := newBus(t)
	r := newFetchRepo(t)
	bundle, head := r.round(t, "two")
	admitted := b.admitted(t)
	run := admitted.submission.Wherry.Run
	b.recordRepo(t, run, r.work)
	b.outputResult(t, admitted, wire.GitBundle, bundle, hashOf(bundle))
	t.Chdir(t.TempDir())
	dir := t.TempDir()
	var stdout, stderr bytes.Buffer
	if err := runWait(b.ctx, b.client, []string{run, "--outputs-dir", dir}, &stdout, &stderr); err != nil {
		t.Fatalf("result: %v; stderr %s", err, stderr.String())
	}
	if !strings.Contains(stderr.String(), fetchedLine(run)) || strings.Contains(stderr.String(), "fetch with:") {
		t.Fatalf("stderr %q, want %q", stderr.String(), fetchedLine(run))
	}
	if got := r.branch(t, run); got != head {
		t.Fatalf("wherry/%s at %q, want %s", run, got, head)
	}
	base := gitRun(t, r.work, "rev-parse", "HEAD")
	gitRun(t, r.work, "update-ref", "refs/heads/wherry/"+run, base)
	stderr.Reset()
	if err := runWait(b.ctx, b.client, []string{run, "--outputs-dir", dir}, &stdout, &stderr); err != nil {
		t.Fatalf("result with the branch elsewhere: %v; stderr %s", err, stderr.String())
	}
	if want := "not fetched (exists); fetch with: git fetch " + filepath.Join(dir, wire.GitBundle) + " HEAD\n"; !strings.Contains(stderr.String(), want) {
		t.Fatalf("stderr %q, want %q", stderr.String(), want)
	}
	if got := r.branch(t, run); got != base {
		t.Fatalf("wherry/%s moved to %s", run, got)
	}
}

// submit --wait reports the fetch as result does.
func TestSubmitWaitFetchesLikeResult(t *testing.T) {
	b := newBus(t)
	r := newFetchRepo(t)
	bundle, head := r.round(t, "two")
	t.Chdir(t.TempDir())
	var run string
	var stderr bytes.Buffer
	stdout := &submitWriter{first: func(line string) {
		run = submittedID(line)
		admitted := b.claimSubmitted(t, run)
		b.recordRepo(t, run, r.work)
		b.outputResult(t, admitted, wire.GitBundle, bundle, hashOf(bundle))
	}}
	if err := runSubmit(b.ctx, b.client, []string{"--prompt", "work", "--wait", "--outputs-dir", t.TempDir()}, stdout, &stderr); err != nil {
		t.Fatalf("submit --wait: %v; stderr %s", err, stderr.String())
	}
	if !strings.Contains(stderr.String(), fetchedLine(run)) || r.branch(t, run) != head {
		t.Fatalf("stderr %q, branch %q; want %q and the branch at %s", stderr.String(), r.branch(t, run), fetchedLine(run), head)
	}
}

// The MCP result tool and submit with wait carry fetched_ref, and not_fetched with its reason.
func TestMCPResultAndSubmitWaitFetch(t *testing.T) {
	b := newBus(t)
	r := newFetchRepo(t)
	t.Chdir(t.TempDir())
	session := mcpSession(t, b.connect(t), nil)
	var submitted submitOutput
	callTool(t, b.ctx, session, "submit", map[string]any{"prompt": "work"}, &submitted)
	bundle, head := r.round(t, "two")
	admitted := b.claimSubmitted(t, submitted.ID)
	b.recordRepo(t, submitted.ID, r.work)
	b.outputResult(t, admitted, wire.GitBundle, bundle, hashOf(bundle))
	var reply waitReply
	callTool(t, b.ctx, session, "result", map[string]any{"id": submitted.ID}, &reply)
	if reply.FetchedRef != "wherry/"+submitted.ID || reply.NotFetched != "" || r.branch(t, submitted.ID) != head {
		t.Fatalf("result %+v, branch %q; want fetched_ref wherry/%s at %s", reply, r.branch(t, submitted.ID), submitted.ID, head)
	}
	// Polled again, the branch at the bundle's head is reported as fetched; moved, as not fetched.
	callTool(t, b.ctx, session, "result", map[string]any{"id": submitted.ID}, &reply)
	if reply.FetchedRef != "wherry/"+submitted.ID {
		t.Fatalf("second result %+v", reply)
	}
	gitRun(t, r.work, "update-ref", "refs/heads/wherry/"+submitted.ID, gitRun(t, r.work, "rev-parse", "HEAD"))
	var moved waitReply
	callTool(t, b.ctx, session, "result", map[string]any{"id": submitted.ID}, &moved)
	if moved.FetchedRef != "" || moved.NotFetched != busclient.NotFetchedExists {
		t.Fatalf("result with the branch moved %+v", moved)
	}

	// submit with wait: the receiver's side runs while the tool waits.
	bundle, head = r.round(t, "three")
	done := make(chan struct{})
	go func() {
		defer close(done)
		runs := waitForRuns(t, b, 2)
		id := runs[len(runs)-1]
		next := b.claimSubmitted(t, id)
		b.recordRepo(t, id, r.work)
		b.outputResult(t, next, wire.GitBundle, bundle, hashOf(bundle))
	}()
	var raw map[string]any
	callTool(t, b.ctx, session, "submit", map[string]any{"prompt": "work", "wait": true}, &raw)
	<-done
	encoded, _ := json.Marshal(raw)
	var waited submitOutput
	if err := json.Unmarshal(encoded, &waited); err != nil {
		t.Fatal(err)
	}
	if waited.Result == nil || waited.Result.FetchedRef != "wherry/"+waited.ID || r.branch(t, waited.ID) != head {
		t.Fatalf("submit with wait %s; want fetched_ref wherry/%s", encoded, waited.ID)
	}
}

// waitForRuns returns this host's runs, latest last, once there are n.
func waitForRuns(t *testing.T, b *bus, n int) []string {
	t.Helper()
	for {
		runs, err := b.client.Runs(b.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(runs.Runs) >= n {
			ids := make([]string, 0, len(runs.Runs))
			for i := len(runs.Runs) - 1; i >= 0; i-- {
				ids = append(ids, runs.Runs[i].RunID)
			}
			return ids
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// A continuation of a run with a recorded repository is fetched into its own branch, in the same repository, from the
// head the previous round left there.
func TestSubmitWaitContinuationFetches(t *testing.T) {
	b := newBusStorage(t, fallbackS3(t))
	r := newFetchRepo(t)
	t.Chdir(t.TempDir())
	var previous string
	for round, message := range []string{"two", "three"} {
		bundle, head := r.round(t, message)
		var run string
		var stderr bytes.Buffer
		stdout := &submitWriter{first: func(line string) {
			run = submittedID(line)
			admitted := b.claimSubmitted(t, run)
			if round == 0 {
				b.recordRepo(t, run, r.work)
			}
			b.outputResult(t, admitted, wire.GitBundle, bundle, hashOf(bundle))
		}}
		args := []string{"--prompt", "work", "--wait", "--outputs-dir", t.TempDir()}
		if previous == "" {
			args = append(args, "--git", "https://git.example.test/org/repo.git@main")
		} else {
			args = append(args, "--continue", previous)
		}
		if err := runSubmit(b.ctx, b.client, args, stdout, &stderr); err != nil {
			t.Fatalf("round %d: %v %s", round, err, stderr.String())
		}
		if !strings.Contains(stderr.String(), fetchedLine(run)) || r.branch(t, run) != head {
			t.Fatalf("round %d: stderr %q, branch %q; want %q at %s", round, stderr.String(), r.branch(t, run), fetchedLine(run), head)
		}
		previous = run
	}
}

// A run resubmitted after key-lost is fetched into the branch of its replacement, the run whose bundle it is.
func TestSubmitWaitResubmissionFetchesIntoTheLastRunsBranch(t *testing.T) {
	b := newBus(t)
	r := newFetchRepo(t)
	bundle, head := r.round(t, "two")
	t.Chdir(t.TempDir())
	var stderr bytes.Buffer
	var original, replacement string
	stdout := &submitWriter{first: func(line string) {
		original = submittedID(line)
		admitted := b.claimSubmitted(t, original)
		b.recordRepo(t, original, r.work)
		lost := &wire.RunStatus{StatusUpdate: wire.TaskStatusUpdateEvent{TaskID: original, ContextID: original, Status: now(wire.TaskStateFailed)}, Wherry: wire.StatusBinding{ReceiverBinding: b.binding(admitted), EventID: "lost", Kind: "terminal", Event: wire.EventKeyLost, TaskHash: admitted.hash}}
		b.feed(t, original, b.sign(t, lost))
		status, err := b.client.Status(b.ctx, original)
		if err != nil || status.Replacement == "" {
			t.Fatalf("replacement: %+v %v", status, err)
		}
		replacement = status.Replacement
		next := b.claimSubmitted(t, replacement)
		b.outputResult(t, next, wire.GitBundle, bundle, hashOf(bundle))
	}}
	if err := runSubmit(b.ctx, b.client, []string{"--prompt", "work", "--wait", "--start-by", "5s", "--end-by", "10s", "--outputs-dir", t.TempDir()}, stdout, &stderr); err != nil {
		t.Fatalf("%v: %s", err, stderr.String())
	}
	if !strings.Contains(stderr.String(), fetchedLine(replacement)) || r.branch(t, replacement) != head || r.branch(t, original) != "" {
		t.Fatalf("stderr %q; branches %q and %q; want %q with the replacement's branch alone at %s", stderr.String(), r.branch(t, original), r.branch(t, replacement), fetchedLine(replacement), head)
	}
}
