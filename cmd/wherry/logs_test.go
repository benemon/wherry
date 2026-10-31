package main

import (
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/wire"
)

// logs -f prints lines while the run is live, in order and once each, though the feed carries a line twice, and
// returns once the run is terminal.
func TestLogsFollowPrintsEachLineOnceUntilTheRunEnds(t *testing.T) {
	b := newBus(t)
	admitted := b.admitted(t)
	runID := admitted.submission.Wherry.Run
	b.feed(t, runID, b.chunk(t, admitted, 1, 0, wire.Part{Text: "first"}), b.chunk(t, admitted, 1, 6, wire.Part{Text: "second"}))
	stdout, stderr := &syncBuffer{}, &syncBuffer{}
	followed := make(chan error, 1)
	go func() { followed <- runLogs(b.ctx, b.client, []string{runID, "-f"}, stdout, stderr) }()
	for !strings.Contains(stdout.String(), "second\n") {
		select {
		case err := <-followed:
			t.Fatalf("logs -f ended before the run: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
	}
	if stdout.String() != "first\nsecond\n" {
		t.Fatalf("streamed %q", stdout.String())
	}
	b.feed(t, runID, b.chunk(t, admitted, 1, 6, wire.Part{Text: "second"}), b.chunk(t, admitted, 1, 13, wire.Part{Text: "third"}))
	for !strings.Contains(stdout.String(), "third\n") {
		select {
		case err := <-followed:
			t.Fatalf("logs -f ended before the run: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
	}
	select {
	case err := <-followed:
		t.Fatalf("logs -f ended before the run: %v", err)
	case <-time.After(2 * time.Second):
	}
	b.feed(t, runID, b.result(t, admitted, []wire.Part{{Text: "done"}}), b.status(t, admitted, "terminal-1", "terminal", now(wire.TaskStateCompleted)))
	select {
	case err := <-followed:
		if err != nil {
			t.Fatalf("logs -f: %v; stderr %s", err, stderr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("logs -f still running after the run ended")
	}
	if stdout.String() != "first\nsecond\nthird\n" || stderr.String() != "" {
		t.Fatalf("stdout %q stderr %q", stdout.String(), stderr.String())
	}
	if err := runLogs(b.ctx, b.client, []string{"run-missing", "-f"}, stdout, stderr); err == nil || err.Error() != "transcript run-missing: run not found" {
		t.Fatalf("logs -f of an unknown run: %v", err)
	}
}
