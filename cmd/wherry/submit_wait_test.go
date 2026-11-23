package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/wire"
)

type submitWriter struct {
	bytes.Buffer
	first  func(string)
	writes int
}

func (w *submitWriter) Write(p []byte) (int, error) {
	w.writes++
	n, err := w.Buffer.Write(p)
	if w.writes == 1 && w.first != nil {
		w.first(string(p))
	}
	return n, err
}
func submittedID(line string) string {
	id, _, _ := strings.Cut(strings.TrimPrefix(line, "run_id="), " ")
	return id
}
func (b *bus) claimSubmitted(t *testing.T, id string) task {
	t.Helper()
	admitted := b.tasks(t, id)[0]
	b.feed(t, id, b.echo(t, admitted), b.status(t, admitted, "claimed-1", "claimed", now(wire.TaskStateWorking)))
	return admitted
}
func TestResultRename(t *testing.T) {
	for _, args := range [][]string{{"result", "--help"}, {"host", "result", "--help"}} {
		var out, errout bytes.Buffer
		if err := run(args, &out, &errout); err == nil || !strings.Contains(errout.String(), "wherry host result RUN_ID") {
			t.Fatalf("%v: %v %s", args, err, errout.String())
		}
	}
	var out, errout bytes.Buffer
	if err := run([]string{"wait"}, &out, &errout); err == nil || err.Error() != `unknown command "wait"` {
		t.Fatalf("old command: %v", err)
	}
	if !strings.Contains(usage, "submit|result|") {
		t.Fatal("result missing from help")
	}
	runSubmit(context.Background(), nil, []string{"--help"}, &out, &errout)
	if !strings.Contains(errout.String(), "-wait") {
		t.Fatal("submit help lacks --wait")
	}
}
func TestSubmitWaitSuccess(t *testing.T) {
	b := newBus(t)
	dir := t.TempDir()
	var stderr bytes.Buffer
	stdout := &submitWriter{}
	stdout.first = func(line string) {
		if !strings.HasSuffix(line, "\n") || strings.Count(line, "\n") != 1 {
			t.Fatalf("ID must be one complete write: %q", line)
		}
		admitted := b.claimSubmitted(t, submittedID(line))
		b.outputResult(t, admitted, "report.md", "answer", hashOf("answer"))
	}
	err := runSubmit(b.ctx, b.client, []string{"--prompt", "work", "--wait", "--outputs-dir", dir}, stdout, &stderr)
	if err != nil {
		t.Fatalf("%v: %s", err, stderr.String())
	}
	if !strings.HasPrefix(stdout.String(), "run_id=") || !strings.Contains(stdout.String(), "\noutput=report.md ") {
		t.Fatalf("output order: %s", stdout.String())
	}
	if data, err := os.ReadFile(filepath.Join(dir, "report.md")); err != nil || string(data) != "answer" {
		t.Fatalf("output: %q %v", data, err)
	}
}
func TestSubmitWaitStates(t *testing.T) {
	for state, code := range map[wire.TaskState]int{wire.TaskStateFailed: 2, wire.TaskStateCanceled: 3, wire.TaskStateRejected: 4} {
		t.Run(string(state), func(t *testing.T) {
			b := newBus(t)
			var stderr bytes.Buffer
			stdout := &submitWriter{first: func(line string) {
				id := submittedID(line)
				admitted := b.claimSubmitted(t, id)
				b.feed(t, id, b.status(t, admitted, "terminal-1", "terminal", now(state)))
			}}
			err := runSubmit(b.ctx, b.client, []string{"--prompt", "work", "--wait"}, stdout, &stderr)
			var exit *exitError
			if !errors.As(err, &exit) || exit.code != code {
				t.Fatalf("%v, want code %d", err, code)
			}
		})
	}
	t.Run("unclaimed", func(t *testing.T) {
		b := newBus(t)
		var out, stderr bytes.Buffer
		err := runSubmit(b.ctx, b.client, []string{"--prompt", "work", "--wait", "--start-by", "100ms"}, &out, &stderr)
		if err == nil || !strings.Contains(err.Error(), "no receiver claimed") {
			t.Fatalf("unclaimed: %v %s", err, stderr.String())
		}
	})
}
func TestSubmitOutputsRequireWait(t *testing.T) {
	b := newBus(t)
	var out, stderr bytes.Buffer
	err := runSubmit(b.ctx, b.client, []string{"--prompt", "work", "--outputs-dir", t.TempDir()}, &out, &stderr)
	if err == nil || !strings.Contains(err.Error(), "requires --wait") {
		t.Fatalf("%v", err)
	}
	runs, err := b.client.Runs(b.ctx)
	if err != nil || len(runs.Runs) != 0 {
		t.Fatalf("published: %+v %v", runs, err)
	}
}
func TestSubmitWaitBound(t *testing.T) {
	old := submitWaitMargin
	submitWaitMargin = 50 * time.Millisecond
	t.Cleanup(func() { submitWaitMargin = old })
	for _, absolute := range []bool{false, true} {
		t.Run(fmt.Sprint(absolute), func(t *testing.T) {
			b := newBus(t)
			var stderr bytes.Buffer
			stdout := &submitWriter{first: func(line string) { b.claimSubmitted(t, submittedID(line)); time.Sleep(1200 * time.Millisecond) }}
			end := "3s"
			if absolute {
				end = time.Now().Add(3 * time.Second).Format(time.RFC3339Nano)
			}
			started := time.Now()
			ctx, cancel := context.WithTimeout(b.ctx, 5*time.Second)
			defer cancel()
			err := runSubmit(ctx, b.client, []string{"--prompt", "work", "--wait", "--start-by", "1s", "--end-by", end}, stdout, &stderr)
			var exit *exitError
			if !errors.As(err, &exit) || exit.code != 6 || !strings.Contains(stderr.String(), "collect it later with: wherry result "+submittedID(strings.Split(stdout.String(), "\n")[0])) {
				t.Fatalf("%v %s", err, stderr.String())
			}
			if elapsed := time.Since(started); elapsed < 2*time.Second || elapsed > 4*time.Second {
				t.Fatalf("bound: %s", elapsed)
			}
		})
	}
}
func TestSubmitWaitExpiredBudget(t *testing.T) {
	b := newBus(t)
	old := submitWaitMargin
	submitWaitMargin = time.Millisecond
	t.Cleanup(func() { submitWaitMargin = old })
	var stderr bytes.Buffer
	stdout := &submitWriter{first: func(line string) { b.claimSubmitted(t, submittedID(line)); time.Sleep(1200 * time.Millisecond) }}
	err := runSubmit(b.ctx, b.client, []string{"--prompt", "work", "--wait", "--start-by", "100ms", "--end-by", "200ms"}, stdout, &stderr)
	var exit *exitError
	if !errors.As(err, &exit) || exit.code != 6 || !strings.Contains(stderr.String(), "collect it later with:") {
		t.Fatalf("expired: %v %s", err, stderr.String())
	}
}
func TestSubmitIDWriteError(t *testing.T) {
	b := newBus(t)
	err := runSubmit(b.ctx, b.client, []string{"--prompt", "work", "--wait"}, errorWriter{}, io.Discard)
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("%v", err)
	}
}

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestMCPSubmitWait(t *testing.T) {
	for _, mode := range []string{"omitted", "false", "completed", "unfinished", "collection", "broker", "delayed", "expired"} {
		t.Run(mode, func(t *testing.T) {
			b := newBus(t)
			old := waitBound
			waitBound = 500 * time.Millisecond
			t.Cleanup(func() { waitBound = old })
			calls := 0
			connect := func() (*busclient.Host, error) {
				calls++
				if calls == 1 && mode == "delayed" {
					time.Sleep(300 * time.Millisecond)
				}
				if calls == 1 && mode == "expired" {
					time.Sleep(650 * time.Millisecond)
				}
				if calls == 2 {
					if mode == "broker" {
						return nil, errors.New("private broker address and credential")
					}
					runs, err := b.client.Runs(b.ctx)
					if err != nil || len(runs.Runs) != 1 {
						t.Fatalf("runs: %v %v", runs, err)
					}
					id := runs.Runs[0].RunID
					if mode == "completed" || mode == "collection" {
						admitted := b.claimSubmitted(t, id)
						b.outputResult(t, admitted, "report.md", "answer", hashOf("answer"))
						if mode == "collection" {
							dir := filepath.Join(b.stateDir, "runs", id, "outputs")
							if err := os.MkdirAll(filepath.Dir(dir), 0700); err != nil {
								t.Fatal(err)
							}
							if err := os.WriteFile(dir, []byte("private local path"), 0600); err != nil {
								t.Fatal(err)
							}
						}
					}
				}
				return b.client, nil
			}
			session := mcpSession(t, connect, nil)
			args := map[string]any{"prompt": "work"}
			if mode != "omitted" {
				args["wait"] = mode != "false"
			}
			var reply submitOutput
			started := time.Now()
			callTool(t, b.ctx, session, "submit", args, &reply)
			elapsed := time.Since(started)
			if reply.ID == "" || reply.Pool != "pool-a" || reply.Command != "wherry result "+reply.ID {
				t.Fatalf("identity: %+v", reply)
			}
			switch mode {
			case "omitted", "false":
				if reply.Result != nil || reply.Error != nil || calls != 1 {
					t.Fatalf("unexpected wait: %+v calls=%d", reply, calls)
				}
			case "broker", "collection":
				if reply.Error == nil || reply.Result != nil {
					t.Fatalf("post publish error: %+v", reply)
				}
				want := "UNAVAILABLE"
				if mode == "collection" {
					want = "OUTPUTS_NOT_WRITTEN"
				}
				if reply.Error.Code != want || strings.Contains(reply.Error.Message, "private") {
					t.Fatalf("unsafe or wrong error: %+v", reply.Error)
				}
			case "completed":
				if reply.Error != nil || reply.Result == nil || reply.Result.ID != reply.ID || reply.Result.State != string(wire.TaskStateCompleted) || len(reply.Result.Outputs) != 1 {
					t.Fatalf("nested result: %+v", reply)
				}
			default:
				if reply.Result == nil || reply.Result.ID != reply.ID || reply.Result.RetryAfter != retryAfter {
					t.Fatalf("unfinished: %+v", reply)
				}
				if mode == "delayed" && elapsed > 700*time.Millisecond {
					t.Fatalf("publish did not reduce wait: %s", elapsed)
				}
				if mode == "expired" && elapsed > 850*time.Millisecond {
					t.Fatalf("expired budget still awaited: %s", elapsed)
				}
			}
		})
	}
}

func TestSubmitWaitResubmission(t *testing.T) {
	b := newBus(t)
	var stderr bytes.Buffer
	var original, replacement string
	stdout := &submitWriter{first: func(line string) {
		original = submittedID(line)
		admitted := b.claimSubmitted(t, original)
		lost := &wire.RunStatus{StatusUpdate: wire.TaskStatusUpdateEvent{TaskID: original, ContextID: original, Status: now(wire.TaskStateFailed)}, Wherry: wire.StatusBinding{ReceiverBinding: b.binding(admitted), EventID: "lost", Kind: "terminal", Event: wire.EventKeyLost, TaskHash: admitted.hash}}
		b.feed(t, original, b.sign(t, lost))
		status, err := b.client.Status(b.ctx, original)
		if err != nil || status.Replacement == "" {
			t.Fatalf("replacement: %+v %v", status, err)
		}
		replacement = status.Replacement
		next := b.claimSubmitted(t, replacement)
		if admitted.submission.Wherry.TaskEndedBy != next.submission.Wherry.TaskEndedBy {
			t.Fatal("replacement changed the bound")
		}
		b.outputResult(t, next, "report.md", "replacement", hashOf("replacement"))
	}}
	err := runSubmit(b.ctx, b.client, []string{"--prompt", "work", "--wait", "--outputs-dir", t.TempDir()}, stdout, &stderr)
	if err != nil {
		t.Fatalf("%v: %s", err, stderr.String())
	}
	if submittedID(strings.Split(stdout.String(), "\n")[0]) != original || !strings.Contains(stderr.String(), "resubmitted as run "+replacement) || !strings.Contains(stdout.String(), "\ndone\n") {
		t.Fatalf("stdout %s stderr %s", stdout.String(), stderr.String())
	}
}

func TestSubmitWaitContinuation(t *testing.T) {
	b := newBusStorage(t, fallbackS3(t))
	bundle := "# v2 git bundle\n-" + strings.Repeat("ab", 20) + " base commit\n" + strings.Repeat("cd", 20) + " HEAD\n\nPACK\x00\x00\x00\x02"
	var previous string
	for round := 0; round < 3; round++ {
		var stderr bytes.Buffer
		stdout := &submitWriter{first: func(line string) {
			id := submittedID(line)
			admitted := b.claimSubmitted(t, id)
			if round > 0 && admitted.submission.Wherry.Run == previous {
				t.Fatal("continuation reused run ID")
			}
			b.outputResult(t, admitted, wire.GitBundle, bundle, hashOf(bundle))
		}}
		args := []string{"--prompt", "work", "--wait", "--outputs-dir", t.TempDir()}
		if previous == "" {
			args = append(args, "--git", "https://git.example.test/org/repo.git@main", "--git-changes", "bundle")
		} else {
			args = append(args, "--continue", previous)
		}
		if err := runSubmit(b.ctx, b.client, args, stdout, &stderr); err != nil {
			t.Fatalf("round %d: %v %s", round, err, stderr.String())
		}
		previous = submittedID(strings.Split(stdout.String(), "\n")[0])
	}
}

func TestMCPSubmitWaitProgress(t *testing.T) {
	for _, token := range []bool{false, true} {
		t.Run(fmt.Sprint(token), func(t *testing.T) {
			b := newBus(t)
			old := waitBound
			waitBound = progressInterval + time.Second
			t.Cleanup(func() { waitBound = old })
			var progress atomic.Int64
			session := mcpSession(t, func() (*busclient.Host, error) { return b.client, nil }, &progress)
			params := &mcp.CallToolParams{Name: "submit", Arguments: map[string]any{"prompt": "work", "wait": true}}
			if token {
				params.SetProgressToken("submit-1")
			}
			result, err := session.CallTool(b.ctx, params)
			if err != nil || result.IsError {
				t.Fatalf("submit: %v %v", result, err)
			}
			if token && progress.Load() == 0 || !token && progress.Load() != 0 {
				t.Fatalf("progress %d token %t", progress.Load(), token)
			}
		})
	}
}
