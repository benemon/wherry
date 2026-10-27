package client

import (
	"fmt"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/wire"
)

// scenario renders what events, transcript and, for an ended run, await report of a run.
func (f *fixture) scenario(t *testing.T, runID string, ended bool) string {
	t.Helper()
	events, err := f.host.Events(f.ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	transcript, err := f.host.Transcript(f.ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	var kinds []string
	for _, status := range events.Statuses {
		kinds = append(kinds, fmt.Sprintf("%d:%s", status.Wherry.Attempt, status.Wherry.Kind))
	}
	var lines []string
	for _, line := range transcript.Lines {
		lines = append(lines, fmt.Sprintf("%d@%d", line.Seq, line.Attempt))
	}
	got := fmt.Sprintf("statuses %v chunks %v skipped %d; transcript attempt %d lines %v skipped %d", kinds, events.Chunks, events.Skipped, transcript.Attempt, lines, transcript.Skipped)
	if ended {
		awaited, err := f.host.Await(f.ctx, runID, 10*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var outputs []string
		for _, output := range awaited.Outputs {
			outputs = append(outputs, fmt.Sprintf("%s=%q", output.Name, output.Data))
		}
		got += fmt.Sprintf("; await %s outputs %v rejected %v skipped %d", awaited.Status.State, outputs, awaited.Rejected, awaited.Skipped)
	}
	return got
}

// Events, transcript and await report the same of a run however its records are laid out across topics.
func TestEventsTranscriptAndAwaitScenarios(t *testing.T) {
	for _, c := range []struct {
		name  string
		ended bool
		write func(t *testing.T, f *fixture, receiver *testReceiver, task claimed, runID string)
		want  string
	}{
		{"two attempts", true, func(t *testing.T, f *fixture, receiver *testReceiver, task claimed, runID string) {
			receiver.feed(t, f.ctx, runID, receiver.status(t, task, 1, "started-1", "started", wire.TaskStateWorking, nil), receiver.chunk(t, task, 1, 0), receiver.chunk(t, task, 1, 5))
			receiver.feed(t, f.ctx, runID, receiver.chunk(t, task, 1, 10))
			receiver.feed(t, f.ctx, runID, receiver.status(t, task, 2, "claimed-2", "claimed", wire.TaskStateWorking, nil), receiver.status(t, task, 2, "started-2", "started", wire.TaskStateWorking, nil), receiver.chunk(t, task, 2, 5))
			receiver.feed(t, f.ctx, runID, receiver.chunk(t, task, 2, 10), receiver.chunk(t, task, 2, 15), receiver.result(t, task, 2, wire.TaskStateCompleted, nil), receiver.terminal(t, task, 2, wire.TaskStateCompleted, 3))
		}, "statuses [1:claimed 1:started 2:claimed 2:started 2:terminal] chunks map[1:3 2:3] skipped 0; transcript attempt 2 lines [0@1 5@2 10@2 15@2] skipped 0; await TASK_STATE_COMPLETED outputs [] rejected [] skipped 0"},
		{"outputs", true, func(t *testing.T, f *fixture, receiver *testReceiver, task claimed, runID string) {
			receiver.feed(t, f.ctx, runID, receiver.status(t, task, 1, "started-1", "started", wire.TaskStateWorking, nil), receiver.chunk(t, task, 1, 0))
			receiver.feed(t, f.ctx, runID, receiver.chunk(t, task, 1, 5),
				receiver.outputChunk(t, task, 1, "report.md", 0, false, "# Rep"), receiver.outputChunk(t, task, 1, "report.md", 1, true, "ort\n"),
				receiver.manifestResult(t, task, 1, []wire.ReturnedOutput{listed("report.md", "# Report\n")}, nil), receiver.terminal(t, task, 1, wire.TaskStateCompleted, 2))
		}, `statuses [1:claimed 1:started 1:terminal] chunks map[1:2] skipped 0; transcript attempt 1 lines [0@1 5@1] skipped 0; await TASK_STATE_COMPLETED outputs [report.md="# Report\n"] rejected [] skipped 0`},
		{"running", false, func(t *testing.T, f *fixture, receiver *testReceiver, task claimed, runID string) {
			receiver.feed(t, f.ctx, runID, receiver.status(t, task, 1, "started-1", "started", wire.TaskStateWorking, nil), receiver.chunk(t, task, 1, 0), receiver.chunk(t, task, 1, 5))
			receiver.feed(t, f.ctx, runID, receiver.chunk(t, task, 1, 10))
		}, "statuses [1:claimed 1:started] chunks map[1:3] skipped 0; transcript attempt 1 lines [0@1 5@1 10@1] skipped 0"},
		{"released without a terminal", false, func(t *testing.T, f *fixture, receiver *testReceiver, task claimed, runID string) {
			receiver.feed(t, f.ctx, runID, receiver.status(t, task, 1, "started-1", "started", wire.TaskStateWorking, nil), receiver.chunk(t, task, 1, 0), receiver.chunk(t, task, 1, 5))
			receiver.feed(t, f.ctx, runID, receiver.status(t, task, 2, "claimed-2", "claimed", wire.TaskStateWorking, nil), receiver.status(t, task, 2, "started-2", "started", wire.TaskStateWorking, nil), receiver.chunk(t, task, 2, 5))
		}, "statuses [1:claimed 1:started 2:claimed 2:started] chunks map[1:2 2:1] skipped 0; transcript attempt 2 lines [0@1 5@2] skipped 0"},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newFixture(t)
			receiver, task := f.admitted(t)
			runID := task.submission.Wherry.Run
			c.write(t, f, receiver, task, runID)
			if got := f.scenario(t, runID, c.ended); got != c.want {
				t.Fatalf("got  %s\nwant %s", got, c.want)
			}
		})
	}
}
