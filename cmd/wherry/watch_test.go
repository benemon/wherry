package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/wire"
)

// watchedRun is a completed run with two transcript lines, the reply done, the output report.md and notes.md missing.
func watchedRun(t *testing.T) (*bus, string) {
	t.Helper()
	b := newBus(t)
	admitted := b.admitted(t)
	runID := admitted.submission.Wherry.Run
	b.feed(t, runID, b.chunk(t, admitted, 1, 0, wire.Part{Text: `{"type":"start"}`}), b.chunk(t, admitted, 1, 17, wire.Part{Raw: wire.StdBytes("second")}))
	b.outputResult(t, admitted, "report.md", "# Report\n", hashOf("# Report\n"))
	return b, runID
}

// Each mode collects the same files; default prints the transcript, the state, the reply and the outputs, --result-only
// the reply, and -q nothing.
func TestWatchModes(t *testing.T) {
	b, runID := watchedRun(t)
	const lines = "{\"type\":\"start\"}\nsecond\n"
	written := map[string]string{}
	for _, mode := range []string{"", "--result-only", "-q", "--quiet"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			args := []string{runID, "--outputs-dir", dir, "--timeout", "5s"}
			if mode != "" {
				args = append(args, mode)
			}
			var stdout, stderr bytes.Buffer
			if err := runWatch(b.ctx, b.client, args, &stdout, &stderr); err != nil {
				t.Fatalf("watch %s: %v", mode, err)
			}
			report, transcript := filepath.Join(dir, "report.md"), filepath.Join(dir, "transcript.jsonl")
			want, diagnostics := "", "output notes.md not returned: missing\n"
			switch mode {
			case "":
				want = lines + "TASK_STATE_COMPLETED\ndone\n" +
					"output=report.md path=" + report + " sha256=" + hashOf("# Report\n") + " size=9\n" +
					"output=transcript.jsonl path=" + transcript + " sha256=" + hashOf(lines) + " size=" + strconv.Itoa(len(lines)) + "\n"
			case "--result-only":
				want = "done\n"
			default:
				diagnostics = ""
			}
			if stdout.String() != want || stderr.String() != diagnostics {
				t.Fatalf("stdout %q stderr %q, want %q and %q", stdout.String(), stderr.String(), want, diagnostics)
			}
			for _, path := range []string{report, transcript} {
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if previous, ok := written[filepath.Base(path)]; ok && previous != string(data) {
					t.Fatalf("%s holds %q, another mode wrote %q", path, data, previous)
				}
				written[filepath.Base(path)] = string(data)
			}
		})
	}
	if written["transcript.jsonl"] != lines {
		t.Fatalf("transcript.jsonl %q, want %q", written["transcript.jsonl"], lines)
	}
}

func TestWatchQuietExitsWithTheState(t *testing.T) {
	b, admitted := publishFailedRun(t)
	var stdout, stderr bytes.Buffer
	err := runWatch(b.ctx, b.client, []string{admitted.submission.Wherry.Run, "-q", "--timeout", "5s"}, &stdout, &stderr)
	if err == nil || err.Error() != "run ended in TASK_STATE_FAILED" || stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("watch -q = %v, stdout %q stderr %q; want the state as the error alone", err, stdout.String(), stderr.String())
	}
}

// Lines stream while the run is live, in order and once each, though the final transcript repeats the streamed ones
// and the feed carries a line twice.
func TestWatchStreamsTheTranscript(t *testing.T) {
	b := newBus(t)
	admitted := b.admitted(t)
	runID := admitted.submission.Wherry.Run
	b.feed(t, runID, b.chunk(t, admitted, 1, 0, wire.Part{Text: "first"}), b.chunk(t, admitted, 1, 6, wire.Part{Text: "second"}))
	stdout, stderr := &syncBuffer{}, &syncBuffer{}
	watched := make(chan error, 1)
	go func() { watched <- runWatch(b.ctx, b.client, []string{runID, "--timeout", "20s"}, stdout, stderr) }()
	for !strings.Contains(stdout.String(), "second\n") {
		select {
		case err := <-watched:
			t.Fatalf("watch ended before the run: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
	}
	if stdout.String() != "first\nsecond\n" {
		t.Fatalf("streamed %q", stdout.String())
	}
	b.feed(t, runID, b.chunk(t, admitted, 1, 6, wire.Part{Text: "second"}), b.chunk(t, admitted, 1, 13, wire.Part{Text: "third"}))
	b.feed(t, runID, b.result(t, admitted, []wire.Part{{Text: "done"}}), b.status(t, admitted, "terminal-1", "terminal", now(wire.TaskStateCompleted)))
	if err := <-watched; err != nil {
		t.Fatalf("watch: %v; stderr %s", err, stderr.String())
	}
	if printed, _, _ := strings.Cut(stdout.String(), "output="); printed != "first\nsecond\nthird\nTASK_STATE_COMPLETED\ndone\n" {
		t.Fatalf("stdout %q", stdout.String())
	}
}
