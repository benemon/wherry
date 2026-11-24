package main

import (
	"bytes"
	"context"
	json "encoding/json/v2"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/wire"
)

// shortResultBound shortens result's default bound for a test: grace past task_ended_by and floor from now.
func shortResultBound(t *testing.T, grace, floor time.Duration) {
	t.Helper()
	oldGrace, oldFloor := resultGrace, resultFloor
	resultGrace, resultFloor = grace, floor
	t.Cleanup(func() { resultGrace, resultFloor = oldGrace, oldFloor })
}

// shortRun submits a run that starts by startBy and ends by endBy from now and returns its ID and admitted task.
func (b *bus) shortRun(t *testing.T, startBy, endBy time.Duration) (string, task) {
	t.Helper()
	submitted, err := b.client.Submit(b.ctx, busclient.SubmitInput{Selector: []wire.Requirement{}, Prompt: "work", StartBy: time.Now().Add(startBy), EndBy: time.Now().Add(endBy)})
	if err != nil {
		t.Fatal(err)
	}
	return submitted.RunID, b.tasks(t, submitted.RunID)[0]
}

// result's default bound is the run's recorded task_ended_by plus the grace, at least the floor; a run with no
// recorded task_ended_by takes the 2h10m fallback.
func TestResultBound(t *testing.T) {
	shortResultBound(t, 10*time.Second, time.Second)
	if got := resultBound(time.Time{}); got != defaultWaitTimeout {
		t.Fatalf("bound without a recorded end-by = %s; want %s", got, defaultWaitTimeout)
	}
	if got := resultBound(time.Now().Add(-time.Hour)); got != resultFloor {
		t.Fatalf("bound of a run long past its end-by = %s; want the floor %s", got, resultFloor)
	}
	if got := resultBound(time.Now().Add(time.Minute)); got < 69*time.Second || got > 70*time.Second {
		t.Fatalf("bound of a run ending in a minute = %s; want about 70s", got)
	}
}

// result with no --timeout on a run a receiver holds exits 6 near the run's task_ended_by plus the grace.
func TestResultDefaultWaitsUntilEndByPlusGrace(t *testing.T) {
	b := newBus(t)
	shortResultBound(t, 50*time.Millisecond, time.Second)
	runID, admitted := b.shortRun(t, time.Second, 4*time.Second)
	b.feed(t, runID, b.echo(t, admitted), b.status(t, admitted, "claimed-1", "claimed", now(wire.TaskStateWorking)))
	deadline := time.Unix(admitted.submission.Wherry.TaskEndedBy, 0).Add(resultGrace)
	ctx, cancel := context.WithTimeout(b.ctx, 10*time.Second)
	defer cancel()
	started := time.Now()
	var stdout, stderr bytes.Buffer
	err := runWait(ctx, b.client, []string{runID}, &stdout, &stderr)
	var exit *exitError
	if !errors.As(err, &exit) || exit.code != timeoutCode {
		t.Fatalf("result on a held run = %v; want exit %d", err, timeoutCode)
	}
	if ended := time.Now(); ended.Before(deadline.Add(-time.Second)) || ended.After(deadline.Add(500*time.Millisecond)) {
		t.Fatalf("result ended %s after task_ended_by plus the grace; want near it", ended.Sub(deadline))
	}
	if elapsed := time.Since(started); elapsed <= resultFloor {
		t.Fatalf("result waited %s, within the floor; want the run's remaining time", elapsed)
	}
}

// result with no --timeout on a finished run read long after its task_ended_by plus the grace returns its outcome:
// the floor keeps the feed replay within the bound.
func TestResultOnAFinishedRunPastItsBound(t *testing.T) {
	b := newBus(t)
	shortResultBound(t, 50*time.Millisecond, 5*time.Second)
	runID, admitted := b.shortRun(t, time.Second, time.Second)
	b.feed(t, runID, b.echo(t, admitted), b.status(t, admitted, "claimed-1", "claimed", now(wire.TaskStateWorking)))
	b.feed(t, runID, b.result(t, admitted, []wire.Part{{Text: "done"}}), b.status(t, admitted, "terminal-1", "terminal", now(wire.TaskStateCompleted)))
	time.Sleep(time.Until(time.Unix(admitted.submission.Wherry.TaskEndedBy, 0).Add(resultGrace + 500*time.Millisecond)))
	var stdout, stderr bytes.Buffer
	if err := runWait(b.ctx, b.client, []string{runID}, &stdout, &stderr); err != nil {
		t.Fatalf("result on a finished run past its bound = %v; stderr %s", err, stderr.String())
	}
	if stdout.String() != "done\n" {
		t.Fatalf("stdout %q; want the reply", stdout.String())
	}
}

// An explicit --timeout wins over the bound, longer or shorter: result on a held run with a --timeout past the run's
// task_ended_by plus the grace exits 6 at the timeout, not at the bound.
func TestExplicitTimeoutLongerThanTheBoundWins(t *testing.T) {
	b := newBus(t)
	shortResultBound(t, 50*time.Millisecond, time.Second)
	runID, admitted := b.shortRun(t, time.Second, time.Second)
	b.feed(t, runID, b.echo(t, admitted), b.status(t, admitted, "claimed-1", "claimed", now(wire.TaskStateWorking)))
	started := time.Now()
	var stdout, stderr bytes.Buffer
	err := runWait(b.ctx, b.client, []string{runID, "--timeout", "4s"}, &stdout, &stderr)
	var exit *exitError
	if !errors.As(err, &exit) || exit.code != timeoutCode {
		t.Fatalf("result with --timeout 4s on a held run = %v; want exit %d", err, timeoutCode)
	}
	if elapsed := time.Since(started); elapsed < 3500*time.Millisecond || elapsed > 5*time.Second {
		t.Fatalf("result ended after %s; want near its 4s --timeout, past the run's bound", elapsed)
	}
	if strings.Contains(err.Error(), "task_ended_by plus the upload grace") {
		t.Fatalf("result with an explicit --timeout = %v; want no bound named", err)
	}
}

// A record without task_ended_by, as hosts wrote before it was recorded, takes the 2h10m fallback: result with no
// --timeout exits 6 at the fallback, shortened through its seam, past the bound the run's end-by would give.
func TestResultWithoutARecordedEndByTakesTheFallback(t *testing.T) {
	b := newBus(t)
	shortResultBound(t, 50*time.Millisecond, time.Second)
	oldFallback := defaultWaitTimeout
	defaultWaitTimeout = 3 * time.Second
	t.Cleanup(func() { defaultWaitTimeout = oldFallback })
	runID, admitted := b.shortRun(t, time.Second, time.Second)
	b.feed(t, runID, b.echo(t, admitted), b.status(t, admitted, "claimed-1", "claimed", now(wire.TaskStateWorking)))
	path := filepath.Join(b.stateDir, "runs", runID+".json")
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var record map[string]any
	if err := json.Unmarshal(encoded, &record); err != nil {
		t.Fatal(err)
	}
	if _, recorded := record["task_ended_by"]; !recorded {
		t.Fatalf("record %s holds no task_ended_by", path)
	}
	delete(record, "task_ended_by")
	if encoded, err = json.Marshal(record); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	if got := b.client.RecordedEndBy(runID); !got.IsZero() {
		t.Fatalf("recorded end-by %s; want none", got)
	}
	started := time.Now()
	var stdout, stderr bytes.Buffer
	err = runWait(b.ctx, b.client, []string{runID}, &stdout, &stderr)
	var exit *exitError
	if !errors.As(err, &exit) || exit.code != timeoutCode {
		t.Fatalf("result without a recorded end-by = %v; want exit %d at the fallback", err, timeoutCode)
	}
	if elapsed := time.Since(started); elapsed < 2500*time.Millisecond || elapsed > 4*time.Second {
		t.Fatalf("result ended after %s; want the %s fallback, past the run's own bound", elapsed, defaultWaitTimeout)
	}
}
