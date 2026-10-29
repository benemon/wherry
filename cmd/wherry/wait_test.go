package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/wire"
)

// finishedRun is a completed run with two transcript lines, the reply done, the output report.md and notes.md missing.
func finishedRun(t *testing.T) (*bus, string) {
	t.Helper()
	b := newBus(t)
	admitted := b.admitted(t)
	runID := admitted.submission.Wherry.Run
	b.feed(t, runID, b.chunk(t, admitted, 1, 0, wire.Part{Text: `{"type":"start"}`}), b.chunk(t, admitted, 1, 17, wire.Part{Raw: wire.StdBytes("second")}))
	b.outputResult(t, admitted, "report.md", "# Report\n", hashOf("# Report\n"))
	return b, runID
}

// wait prints the reply and the outputs, none of the transcript, and writes no transcript.jsonl.
func TestWaitPrintsTheReplyAndOutputs(t *testing.T) {
	b, runID := finishedRun(t)
	dir := t.TempDir()
	var stdout, stderr bytes.Buffer
	if err := runWait(b.ctx, b.client, []string{runID, "--outputs-dir", dir, "--timeout", "5s"}, &stdout, &stderr); err != nil {
		t.Fatalf("wait: %v", err)
	}
	want := "done\noutput=report.md path=" + filepath.Join(dir, "report.md") + " sha256=" + hashOf("# Report\n") + " size=9\n"
	if stdout.String() != want || stderr.String() != "output notes.md not returned: missing\n" {
		t.Fatalf("stdout %q stderr %q, want %q", stdout.String(), stderr.String(), want)
	}
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 1 || entries[0].Name() != "report.md" {
		t.Fatalf("outputs directory %v, %v; want report.md alone", entries, err)
	}
}

// Each terminal state other than COMPLETED has its own exit code, as do a passed timeout and a run wait cannot find.
func TestWaitExitCodes(t *testing.T) {
	for state, want := range map[wire.TaskState]int{
		wire.TaskStateCompleted: 0, wire.TaskStateFailed: 2, wire.TaskStateCanceled: 3, wire.TaskStateRejected: 4, wire.TaskStateAuthRequired: 5,
	} {
		t.Run(string(state), func(t *testing.T) {
			b := newBus(t)
			admitted := b.admitted(t)
			runID := admitted.submission.Wherry.Run
			b.feed(t, runID, b.resultIn(t, admitted, state), b.status(t, admitted, "terminal-1", "terminal", now(state)))
			var stdout, stderr bytes.Buffer
			err := runWait(b.ctx, b.client, []string{runID, "--timeout", "5s"}, &stdout, &stderr)
			if code := 0; err != nil {
				if code = exitCode(err); code != want || err.Error() != "run ended in "+string(state) {
					t.Fatalf("wait = %v, exit %d; want %d", err, code, want)
				}
			} else if want != 0 {
				t.Fatalf("wait succeeded; want exit %d", want)
			}
		})
	}
	b := newBus(t)
	runID := b.admitted(t).submission.Wherry.Run
	var stdout, stderr bytes.Buffer
	if err := runWait(b.ctx, b.client, []string{runID, "--timeout", "1s"}, &stdout, &stderr); exitCode(err) != 6 {
		t.Fatalf("wait past its timeout = %v, exit %d; want 6", err, exitCode(err))
	}
	if err := runWait(b.ctx, b.client, []string{"run-missing", "--timeout", "5s"}, &stdout, &stderr); err == nil || exitCode(err) != 1 || !strings.HasSuffix(err.Error(), "run not found") {
		t.Fatalf("wait for an unknown run = %v, exit %d; want 1", err, exitCode(err))
	}
}

// resultIn signs a result of admitted in state, as receiver-a's end of a run does.
func (b *bus) resultIn(t *testing.T, admitted task, state wire.TaskState) []byte {
	t.Helper()
	runID := admitted.submission.Wherry.Run
	return b.sign(t, &wire.Result{Task: wire.Task{ID: runID, ContextID: runID, History: []wire.Message{admitted.submission.Message}, Status: now(state), Artifacts: []wire.Artifact{}},
		Wherry: wire.ResultBinding{ReceiverBinding: b.binding(admitted), TaskHash: admitted.hash}})
}

// wait prints nothing while the run is live, though the agent prints lines.
func TestWaitDoesNotStream(t *testing.T) {
	b := newBus(t)
	admitted := b.admitted(t)
	runID := admitted.submission.Wherry.Run
	stdout, stderr := &syncBuffer{}, &syncBuffer{}
	waited := make(chan error, 1)
	go func() { waited <- runWait(b.ctx, b.client, []string{runID, "--timeout", "20s"}, stdout, stderr) }()
	b.feed(t, runID, b.chunk(t, admitted, 1, 0, wire.Part{Text: "first"}), b.chunk(t, admitted, 1, 6, wire.Part{Text: "second"}))
	select {
	case err := <-waited:
		t.Fatalf("wait ended before the run: %v", err)
	case <-time.After(2 * time.Second):
	}
	if stdout.String() != "" {
		t.Fatalf("wait printed %q while the run was live", stdout.String())
	}
	b.feed(t, runID, b.result(t, admitted, []wire.Part{{Text: "done"}}), b.status(t, admitted, "terminal-1", "terminal", now(wire.TaskStateCompleted)))
	if err := <-waited; err != nil {
		t.Fatalf("wait: %v; stderr %s", err, stderr.String())
	}
	if stdout.String() != "done\n" {
		t.Fatalf("stdout %q, want the reply alone", stdout.String())
	}
}
