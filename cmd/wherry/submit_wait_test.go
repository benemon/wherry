package main

import (
	"bytes"
	"context"
	json "encoding/json/v2"
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
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kmsg"
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
	if err := run([]string{"host", "wait"}, &out, &errout); err == nil || !strings.Contains(err.Error(), "wait") {
		t.Fatalf("old host command: %v", err)
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
				status := now(state)
				result := &wire.Result{Task: wire.Task{ID: id, ContextID: id, History: []wire.Message{admitted.submission.Message}, Status: status, Artifacts: []wire.Artifact{}}, Wherry: wire.ResultBinding{ReceiverBinding: b.binding(admitted), TaskHash: admitted.hash}}
				b.feed(t, id, b.sign(t, result), b.status(t, admitted, "terminal-1", "terminal", status))
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
		var exit *exitError
		if err == nil || !strings.Contains(err.Error(), "no receiver claimed") || errors.As(err, &exit) {
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
	shortResultBound(t, 50*time.Millisecond, time.Second)
	for _, absolute := range []bool{false, true} {
		t.Run(fmt.Sprint(absolute), func(t *testing.T) {
			b := newBus(t)
			delayed := b.delayPublish(1200 * time.Millisecond)
			var stderr bytes.Buffer
			var deadline time.Time
			stdout := &submitWriter{first: func(line string) {
				admitted := b.claimSubmitted(t, submittedID(line))
				deadline = time.Unix(admitted.submission.Wherry.TaskEndedBy, 0).Add(resultGrace)
				if floor := time.Now().Add(resultFloor); floor.After(deadline) {
					deadline = floor
				}
			}}
			end := "3s"
			if absolute {
				end = time.Now().Add(3 * time.Second).Format(time.RFC3339Nano)
			}
			started := time.Now()
			ctx, cancel := context.WithTimeout(b.ctx, 5*time.Second)
			defer cancel()
			err := runSubmit(ctx, b.client, []string{"--prompt", "work", "--wait", "--start-by", "1s", "--end-by", end}, stdout, &stderr)
			if !delayed.Load() {
				t.Fatal("publish delay was not applied")
			}
			var exit *exitError
			if !errors.As(err, &exit) || exit.code != 6 || !strings.Contains(stderr.String(), "collect it later with: wherry result "+submittedID(strings.Split(stdout.String(), "\n")[0])) {
				t.Fatalf("%v %s", err, stderr.String())
			}
			if time.Now().After(deadline.Add(500 * time.Millisecond)) {
				t.Fatalf("wait exceeded published bound plus slack: %s", time.Since(deadline))
			}
			if elapsed := time.Since(started); elapsed < 2*time.Second || elapsed > 4*time.Second {
				t.Fatalf("bound: %s", elapsed)
			}
		})
	}
}
func TestSubmitWaitExpiredBudget(t *testing.T) {
	b := newBus(t)
	delayed := b.delayPublish(1200 * time.Millisecond)
	shortResultBound(t, time.Millisecond, time.Second)
	var stderr bytes.Buffer
	stdout := &submitWriter{first: func(line string) { b.claimSubmitted(t, submittedID(line)) }}
	err := runSubmit(b.ctx, b.client, []string{"--prompt", "work", "--wait", "--start-by", "100ms", "--end-by", "200ms"}, stdout, &stderr)
	if !delayed.Load() {
		t.Fatal("publish delay was not applied")
	}
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
			waitBound = 2 * time.Second
			t.Cleanup(func() { waitBound = old })
			calls := 0
			var delayed *atomic.Bool
			connect := func() (*busclient.Host, error) {
				calls++
				if calls == 1 && mode == "delayed" {
					delayed = b.delayPublish(time.Second)
				}
				if calls == 1 && mode == "expired" {
					delayed = b.delayPublish(3 * time.Second)
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
			var raw map[string]any
			callTool(t, b.ctx, session, "submit", args, &raw)
			encoded, err := json.Marshal(raw)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(encoded, &reply); err != nil {
				t.Fatal(err)
			}
			if mode != "omitted" && mode != "false" && mode != "broker" && mode != "collection" {
				if raw["result"] == nil || raw["state"] != nil {
					t.Fatalf("result is not nested: %s", encoded)
				}
			}
			if mode == "omitted" || mode == "false" {
				if _, ok := raw["result"]; ok {
					t.Fatalf("unrequested result member: %s", encoded)
				}
			}
			elapsed := time.Since(started)
			if (mode == "delayed" || mode == "expired") && (delayed == nil || !delayed.Load()) {
				t.Fatal("publish delay was not applied")
			}
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
				if mode == "delayed" && elapsed > 2900*time.Millisecond {
					t.Fatalf("publish did not reduce wait: %s", elapsed)
				}
				if mode == "expired" && elapsed > 4500*time.Millisecond {
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
	err := runSubmit(b.ctx, b.client, []string{"--prompt", "work", "--wait", "--start-by", "5s", "--end-by", "10s", "--outputs-dir", t.TempDir()}, stdout, &stderr)
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

func (b *bus) delayPublish(delay time.Duration) *atomic.Bool {
	delayed := new(atomic.Bool)
	b.cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		b.cluster.SleepControl(func() { time.Sleep(delay) })
		delayed.Store(true)
		b.cluster.DropControl()
		return nil, nil, false
	})
	return delayed
}

func TestSubmitWaitReplacementBound(t *testing.T) {
	b := newBus(t)
	shortResultBound(t, 50*time.Millisecond, time.Second)
	var stderr bytes.Buffer
	var deadline time.Time
	claimed := make(chan struct{})
	stdout := &submitWriter{first: func(line string) {
		id := submittedID(line)
		admitted := b.claimSubmitted(t, id)
		deadline = time.Unix(admitted.submission.Wherry.TaskEndedBy, 0).Add(resultGrace)
		go func() {
			defer close(claimed)
			time.Sleep(900 * time.Millisecond)
			lost := &wire.RunStatus{StatusUpdate: wire.TaskStatusUpdateEvent{TaskID: id, ContextID: id, Status: now(wire.TaskStateFailed)}, Wherry: wire.StatusBinding{ReceiverBinding: b.binding(admitted), EventID: "lost", Kind: "terminal", Event: wire.EventKeyLost, TaskHash: admitted.hash}}
			b.feed(t, id, b.sign(t, lost))
			status, err := b.client.Status(b.ctx, id)
			if err != nil || status.Replacement == "" {
				t.Errorf("replacement: %+v %v", status, err)
				return
			}
			b.claimSubmitted(t, status.Replacement)
		}()
	}}
	ctx, cancel := context.WithTimeout(b.ctx, 6*time.Second)
	defer cancel()
	err := runSubmit(ctx, b.client, []string{"--prompt", "work", "--wait", "--start-by", "3s", "--end-by", "4s"}, stdout, &stderr)
	<-claimed
	var exit *exitError
	if !errors.As(err, &exit) || exit.code != 6 {
		t.Fatalf("replacement bound: %v %s", err, stderr.String())
	}
	if time.Now().After(deadline.Add(500 * time.Millisecond)) {
		t.Fatalf("replacement restarted the wait budget: %s late", time.Since(deadline))
	}
}
