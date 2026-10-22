package client

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	json "encoding/json/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/wire"
)

// outputChunk is one chunk of an output as the receiver's outputChunks writes it.
func (r *testReceiver) outputChunk(t *testing.T, task claimed, attempt int64, name string, index int64, last bool, data string) []byte {
	t.Helper()
	run := task.submission.Wherry.Run
	metadata, err := json.Marshal(wire.OutputMetadata{Index: index})
	if err != nil {
		t.Fatal(err)
	}
	return r.sign(t, &wire.Output{
		ArtifactUpdate: wire.TaskArtifactUpdateEvent{
			TaskID: run, ContextID: run,
			Artifact: wire.Artifact{ArtifactID: name, Parts: []wire.Part{{Raw: wire.StdBytes(data)}}, Extensions: []string{wire.ExtensionURI}},
			Append:   index > 0, LastChunk: last, Metadata: metadata,
		},
		Wherry: wire.ResultBinding{ReceiverBinding: r.binding(task, attempt), TaskHash: task.hash},
	})
}

// manifestResult is a completed result whose metadata lists outputs as the receiver's placement metadata does.
func (r *testReceiver) manifestResult(t *testing.T, task claimed, attempt int64, outputs []wire.ReturnedOutput, notReturned []wire.UnreturnedOutput) []byte {
	t.Helper()
	metadata, err := json.Marshal(map[string]any{"receiver_id": r.id, "pool": r.pool, "outputs": outputs, "not_returned": notReturned})
	if err != nil {
		t.Fatal(err)
	}
	return r.result(t, task, attempt, wire.TaskStateCompleted, func(result *wire.Result) { result.Task.Metadata = metadata })
}

func listed(name, data string) wire.ReturnedOutput {
	sum := sha256.Sum256([]byte(data))
	return wire.ReturnedOutput{Name: name, SHA256: hex.EncodeToString(sum[:]), Size: int64(len(data))}
}

func TestAwaitJoinsAndVerifiesOutputs(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	missing := wire.UnreturnedOutput{Name: "notes.md", Reason: "missing"}
	receiver.feed(t, f.ctx, runID,
		receiver.outputChunk(t, task, 1, "report.md", 0, false, "# Rep"),
		receiver.outputChunk(t, task, 1, "report.md", 1, true, "ort\n"),
		receiver.outputChunk(t, task, 1, "sub/empty", 0, true, ""),
		receiver.manifestResult(t, task, 1, []wire.ReturnedOutput{listed("report.md", "# Report\n"), listed("sub/empty", "")}, []wire.UnreturnedOutput{missing}))
	awaited, err := f.host.Await(f.ctx, runID, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(awaited.Outputs) != 2 || string(awaited.Outputs[0].Data) != "# Report\n" || awaited.Outputs[1].Name != "sub/empty" || len(awaited.Rejected) != 0 ||
		!slices.Equal(awaited.NotReturned, []wire.UnreturnedOutput{missing}) {
		t.Fatalf("await = %+v; want both outputs verified and notes.md listed as missing", awaited)
	}
	paths, err := f.host.WriteOutputs(runID, "", awaited.Outputs)
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(f.stateDir, "runs", runID, "outputs", "report.md"); paths[0] != want {
		t.Fatalf("wrote %v, want %s first", paths, want)
	}
	if written, err := os.ReadFile(paths[0]); err != nil || string(written) != "# Report\n" {
		t.Fatalf("written %q, %v", written, err)
	}
}

func TestAwaitRejectsOutputsItCannotVerify(t *testing.T) {
	for name, test := range map[string]struct {
		chunks func(*testReceiver, claimed) [][]byte
		reason string
	}{
		"gap": {func(r *testReceiver, task claimed) [][]byte {
			return [][]byte{r.outputChunk(t, task, 2, "out.txt", 0, false, "ab"), r.outputChunk(t, task, 2, "out.txt", 2, true, "ef")}
		}, "gap"},
		"no last chunk": {func(r *testReceiver, task claimed) [][]byte {
			return [][]byte{r.outputChunk(t, task, 2, "out.txt", 0, false, "ab"), r.outputChunk(t, task, 2, "out.txt", 1, false, "cdef")}
		}, "gap"},
		"overlap": {func(r *testReceiver, task claimed) [][]byte {
			return [][]byte{r.outputChunk(t, task, 2, "out.txt", 0, true, "abcdef"), r.outputChunk(t, task, 2, "out.txt", 1, true, "gh")}
		}, "overlap"},
		"duplicate index": {func(r *testReceiver, task claimed) [][]byte {
			return [][]byte{r.outputChunk(t, task, 2, "out.txt", 0, true, "abcdef"), r.outputChunk(t, task, 2, "out.txt", 0, true, "abcdef")}
		}, "duplicate chunk"},
		"mixed attempt": {func(r *testReceiver, task claimed) [][]byte {
			return [][]byte{r.outputChunk(t, task, 1, "out.txt", 0, false, "abcd"), r.outputChunk(t, task, 2, "out.txt", 1, true, "ef")}
		}, "gap"},
		"hash mismatch": {func(r *testReceiver, task claimed) [][]byte {
			return [][]byte{r.outputChunk(t, task, 2, "out.txt", 0, true, "abcdeF")}
		}, "size or hash mismatch"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			receiver, task := f.admitted(t)
			runID := task.submission.Wherry.Run
			receiver.feed(t, f.ctx, runID, append(test.chunks(receiver, task), receiver.manifestResult(t, task, 2, []wire.ReturnedOutput{listed("out.txt", "abcdef")}, nil))...)
			awaited, err := f.host.Await(f.ctx, runID, 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			want := []wire.UnreturnedOutput{{Name: "out.txt", Size: 6, Reason: "rejected by this host: " + test.reason}}
			if len(awaited.Outputs) != 0 || !slices.Equal(awaited.Rejected, want) {
				t.Fatalf("outputs %+v rejected %+v; want out.txt rejected for %s", awaited.Outputs, awaited.Rejected, test.reason)
			}
		})
	}
}

// The winning attempt's chunks are the only ones taken, whatever an earlier attempt wrote.
func TestAwaitTakesTheWinningAttemptsOutputs(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	receiver.feed(t, f.ctx, runID, receiver.outputChunk(t, task, 1, "out.txt", 0, true, "first"))
	receiver.feed(t, f.ctx, runID, receiver.outputChunk(t, task, 2, "out.txt", 0, true, "second"),
		receiver.manifestResult(t, task, 2, []wire.ReturnedOutput{listed("out.txt", "second")}, nil))
	awaited, err := f.host.Await(f.ctx, runID, 5*time.Second)
	if err != nil || len(awaited.Outputs) != 1 || string(awaited.Outputs[0].Data) != "second" {
		t.Fatalf("await = %+v, %v; want attempt 2's output", awaited, err)
	}
}

func TestWriteOutputsStaysInsideItsDirectory(t *testing.T) {
	f := newFixture(t)
	dir := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(dir, "escape")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(outside, "planted"), filepath.Join(dir, "planted")); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"escape/x", "planted", "../x", "/etc/x", ".hidden"} {
		if _, err := f.host.WriteOutputs("run-a", dir, []Output{{Name: name, Data: []byte("x")}}); err == nil {
			t.Fatalf("wrote output %q", name)
		}
	}
	if entries, _ := os.ReadDir(outside); len(entries) != 0 {
		t.Fatalf("a write left the directory: %v", entries)
	}
	paths, err := f.host.WriteOutputs("run-a", dir, []Output{{Name: "a/b/c.txt", Data: []byte("nested")}})
	if err != nil || !strings.HasSuffix(paths[0], filepath.Join("a", "b", "c.txt")) {
		t.Fatalf("nested output: %v, %v", paths, err)
	}
	if written, _ := os.ReadFile(paths[0]); !bytes.Equal(written, []byte("nested")) {
		t.Fatalf("nested output holds %q", written)
	}
}
