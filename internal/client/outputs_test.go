package client

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	json "encoding/json/v2"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/signed"
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

// replyResult is a completed result as the receiver writes one for a reply over wire.MaxText: the preview in its
// result artifact and status, and the reply entry in its metadata.
func (r *testReceiver) replyResult(t *testing.T, task claimed, attempt int64, preview string, reply wire.ReturnedReply) []byte {
	t.Helper()
	metadata, err := json.Marshal(wire.ResultMetadata{ReceiverID: r.id, Pool: r.pool, Reply: &reply})
	if err != nil {
		t.Fatal(err)
	}
	return r.result(t, task, attempt, wire.TaskStateCompleted, func(result *wire.Result) {
		result.Task.Metadata = metadata
		result.Task.Artifacts = []wire.Artifact{{ArtifactID: "result", Parts: []wire.Part{{Text: preview}}, Extensions: []string{wire.ExtensionURI}}}
		result.Task.Status.Message = &wire.Message{MessageID: "msg-r", Role: wire.RoleAgent, Parts: []wire.Part{{Text: preview}}, Extensions: []string{wire.ExtensionURI}}
	})
}

// replyChunks are data as the receiver's outputChunks splits a reply.
func (r *testReceiver) replyChunks(t *testing.T, task claimed, attempt int64, data string) [][]byte {
	t.Helper()
	var chunks [][]byte
	for index := int64(0); index == 0 || data != ""; index++ {
		piece := data[:min(len(data), wire.OutputChunk)]
		data = data[len(piece):]
		chunks = append(chunks, r.outputChunk(t, task, attempt, wire.ReplyArtifact, index, data == "", piece))
	}
	return chunks
}

// replyText describes reply for a failure message.
func replyText(reply *Reply) string {
	if reply == nil {
		return "absent"
	}
	return fmt.Sprintf("of %d bytes ending %q, sha256 %s, size %d, cut %v, rejected %q", len(reply.Data), reply.Data[max(0, len(reply.Data)-24):], reply.SHA256, reply.Size, reply.Cut, reply.Rejected)
}

func returnedReply(data string, cut bool) wire.ReturnedReply {
	listed := listed(wire.ReplyArtifact, data)
	return wire.ReturnedReply{SHA256: listed.SHA256, Size: listed.Size, Cut: cut}
}

func TestAwaitReturnsTheVerifiedReply(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	reply := strings.Repeat("r", wire.OutputChunk) + "the end"
	preview := reply[:wire.MaxText]
	receiver.feed(t, f.ctx, runID, append(receiver.replyChunks(t, task, 1, reply), receiver.replyResult(t, task, 1, preview, returnedReply(reply, true)))...)
	awaited, err := f.host.Await(f.ctx, runID, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	want := returnedReply(reply, true)
	if awaited.Reply == nil || string(awaited.Reply.Data) != reply || awaited.Reply.SHA256 != want.SHA256 || awaited.Reply.Size != want.Size || !awaited.Reply.Cut ||
		awaited.Reply.Rejected != "" {
		t.Fatalf("reply %s; want the whole reply, verified and cut", replyText(awaited.Reply))
	}
	if awaited.Task.Artifacts[0].Parts[0].Text != preview || awaited.Status.Message.Parts[0].Text != preview || len(awaited.Outputs) != 0 || len(awaited.Rejected) != 0 {
		t.Fatal("the result does not keep the preview, or the reply is listed as an output")
	}
}

func TestAwaitRejectsAReplyItCannotVerify(t *testing.T) {
	reply := strings.Repeat("a", wire.OutputChunk) + strings.Repeat("b", wire.OutputChunk) + "c"
	for name, test := range map[string]struct {
		chunks func(*testReceiver, claimed) [][]byte
		reason string
	}{
		"gap": {func(r *testReceiver, task claimed) [][]byte {
			chunks := r.replyChunks(t, task, 1, reply)
			return [][]byte{chunks[0], chunks[2]}
		}, "gap"},
		"overlap": {func(r *testReceiver, task claimed) [][]byte {
			chunks := r.replyChunks(t, task, 1, reply)
			return append(chunks, r.outputChunk(t, task, 1, wire.ReplyArtifact, 3, true, "d"))
		}, "overlap"},
		"duplicate index": {func(r *testReceiver, task claimed) [][]byte {
			chunks := r.replyChunks(t, task, 1, reply)
			return append(chunks, chunks[1])
		}, "duplicate chunk"},
		"hash mismatch": {func(r *testReceiver, task claimed) [][]byte {
			return r.replyChunks(t, task, 1, strings.Replace(reply, "c", "C", 1))
		}, "size or hash mismatch"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			receiver, task := f.admitted(t)
			runID := task.submission.Wherry.Run
			preview := reply[:wire.MaxText]
			receiver.feed(t, f.ctx, runID, append(test.chunks(receiver, task), receiver.replyResult(t, task, 1, preview, returnedReply(reply, false)))...)
			awaited, err := f.host.Await(f.ctx, runID, 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if awaited.Reply == nil || awaited.Reply.Rejected != test.reason || awaited.Reply.Data != nil || awaited.Status.Message.Parts[0].Text != preview {
				t.Fatalf("reply %s; want it rejected for %s, keeping the preview", replyText(awaited.Reply), test.reason)
			}
		})
	}
}

// Reply chunks of an earlier attempt or of another receiver are not the result's.
func TestAwaitJoinsTheResultsOwnReplyChunks(t *testing.T) {
	reply := strings.Repeat("a", wire.MaxText) + "z"
	for name, other := range map[string]func(*fixture, *testReceiver, claimed) [][]byte{
		"earlier attempt": func(_ *fixture, r *testReceiver, task claimed) [][]byte {
			return r.replyChunks(t, task, 1, strings.Repeat("b", wire.MaxText)+"z")
		},
		"another receiver": func(f *fixture, _ *testReceiver, task claimed) [][]byte {
			return f.addReceiver(t, "receiver-b", "pool-a").replyChunks(t, task, 2, strings.Repeat("b", wire.MaxText)+"z")
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			receiver, task := f.admitted(t)
			runID := task.submission.Wherry.Run
			receiver.feed(t, f.ctx, runID, append(append(other(f, receiver, task), receiver.replyChunks(t, task, 2, reply)...),
				receiver.replyResult(t, task, 2, reply[:wire.MaxText], returnedReply(reply, false)))...)
			awaited, err := f.host.Await(f.ctx, runID, 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if awaited.Reply == nil || awaited.Reply.Rejected != "" || string(awaited.Reply.Data) != reply {
				t.Fatalf("reply %s; want attempt 2's from its receiver", replyText(awaited.Reply))
			}
		})
	}
}

// A marker the chunk seam splits is whole once the chunks are joined, and counts once.
func TestAwaitNamesAMarkerAcrossTheReplysChunkSeam(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	local, err := f.host.localRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	marker := signed.RedactionMarker(local.RedactionKey, "model")
	reply := strings.Repeat("x", wire.OutputChunk-10) + marker + "tail"
	receiver.feed(t, f.ctx, runID, append(receiver.replyChunks(t, task, 1, reply), receiver.replyResult(t, task, 1, reply[:wire.MaxText], returnedReply(reply, false)))...)
	awaited, err := f.host.Await(f.ctx, runID, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Repeat("x", wire.OutputChunk-10) + "[redacted:model]tail"
	sum := sha256.Sum256([]byte(want))
	if awaited.Reply == nil || string(awaited.Reply.Data) != want || awaited.Reply.SHA256 != hex.EncodeToString(sum[:]) || awaited.Reply.Size != int64(len(want)) ||
		!maps.Equal(awaited.Redacted, map[string]int{"model": 1}) {
		t.Fatalf("reply %s counting %v; want the marker named once", replyText(awaited.Reply), awaited.Redacted)
	}
}
