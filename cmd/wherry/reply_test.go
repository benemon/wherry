package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/benemon/wherry/internal/wire"
)

// replyRun is a completed run whose reply is over wire.MaxText, as receiver-a writes one: chunks carries the reply
// chunks' bytes and the result lists listed with cut.
func replyRun(t *testing.T, reply, chunks, listed string, cut bool) (*bus, string) {
	t.Helper()
	b := newBus(t)
	admitted := b.admitted(t)
	run := admitted.submission.Wherry.Run
	var values [][]byte
	for index := int64(0); index == 0 || chunks != ""; index++ {
		piece := chunks[:min(len(chunks), wire.OutputChunk)]
		chunks = chunks[len(piece):]
		metadata, _ := json.Marshal(wire.OutputMetadata{Index: index}) // an integer always encodes
		values = append(values, b.sign(t, &wire.Output{
			ArtifactUpdate: wire.TaskArtifactUpdateEvent{TaskID: run, ContextID: run, Append: index > 0, LastChunk: chunks == "", Metadata: metadata,
				Artifact: wire.Artifact{ArtifactID: wire.ReplyArtifact, Parts: []wire.Part{{Raw: wire.StdBytes(piece)}}, Extensions: []string{wire.ExtensionURI}}},
			Wherry: wire.ResultBinding{ReceiverBinding: b.binding(admitted), TaskHash: admitted.hash},
		}))
	}
	manifest, _ := json.Marshal(wire.ResultMetadata{ReceiverID: "receiver-a", Pool: "pool-a", Reply: &wire.ReturnedReply{SHA256: hashOf(listed), Size: int64(len(listed)), Cut: cut}})
	status := now(wire.TaskStateCompleted)
	preview := []wire.Part{{Text: reply[:wire.MaxText]}}
	status.Message = &wire.Message{MessageID: "msg-status", Role: wire.RoleAgent, Parts: preview, Extensions: []string{wire.ExtensionURI}}
	values = append(values, b.sign(t, &wire.Result{Task: wire.Task{ID: run, ContextID: run, History: []wire.Message{admitted.submission.Message}, Status: status,
		Artifacts: []wire.Artifact{{ArtifactID: "result", Parts: preview, Extensions: []string{wire.ExtensionURI}}}, Metadata: manifest},
		Wherry: wire.ResultBinding{ReceiverBinding: b.binding(admitted), TaskHash: admitted.hash}}),
		b.status(t, admitted, "terminal-1", "terminal", now(wire.TaskStateCompleted)))
	b.feed(t, run, values...)
	return b, run
}

// wait prints the whole verified reply where the status message prints, and notes a cut or a rejection on stderr.
func TestWaitPrintsTheWholeReply(t *testing.T) {
	reply := strings.Repeat("w", wire.OutputChunk) + " the end"
	for _, test := range []struct {
		name, chunks, stdout, stderr string
		cut                          bool
	}{
		{name: "whole", chunks: reply, stdout: reply},
		{name: "cut", chunks: reply, stdout: reply, cut: true, stderr: "reply cut at the pool's reply limit\n"},
		{name: "rejected", chunks: reply[:wire.OutputChunk] + " the END", stdout: reply[:wire.MaxText], stderr: "reply rejected by this host: size or hash mismatch; printed its preview\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			b, runID := replyRun(t, reply, test.chunks, reply, test.cut)
			var stdout, stderr bytes.Buffer
			if err := runWait(b.ctx, b.client, []string{runID, "--outputs-dir", t.TempDir(), "--timeout", "5s"}, &stdout, &stderr); err != nil {
				t.Fatalf("wait: %v", err)
			}
			if want := test.stdout + "\n"; stdout.String() != want || stderr.String() != test.stderr {
				t.Fatalf("wait printed %d bytes starting %.40q, stderr %q; want %d bytes and %q", stdout.Len(), stdout.String(), stderr.String(), len(want), test.stderr)
			}
		})
	}
}

// MCP wait keeps the preview inline and writes the whole reply to reply.txt among the run's outputs.
func TestMCPWaitWritesTheReply(t *testing.T) {
	reply := strings.Repeat("m", wire.OutputChunk) + " the end"
	b, runID := replyRun(t, reply, reply, reply, false)
	var waited waitReply
	callTool(t, b.ctx, mcpSession(t, b.connect(t), nil), "wait", map[string]any{"id": runID}, &waited)
	path := filepath.Join(b.stateDir, "runs", runID, "outputs", wire.ReplyFile)
	if waited.Message != reply[:wire.MaxText] || waited.Reply == nil || waited.Reply.Path != path || waited.Reply.Size != int64(len(reply)) || waited.Reply.Cut {
		t.Fatalf("wait message of %d bytes, reply %+v; want the preview and %s", len(waited.Message), waited.Reply, path)
	}
	if written, err := os.ReadFile(path); err != nil || hashOf(string(written)) != waited.Reply.SHA256 || string(written) != reply {
		t.Fatalf("reply.txt holds %d bytes, %v; want the whole reply hashing to reply.sha256", len(written), err)
	}
	b, runID = replyRun(t, reply, reply[:wire.OutputChunk], reply, false)
	waited = waitReply{}
	callTool(t, b.ctx, mcpSession(t, b.connect(t), nil), "wait", map[string]any{"id": runID}, &waited)
	if waited.Reply == nil || waited.Reply.Rejected != "size or hash mismatch" || waited.Reply.Path != "" || waited.Message != reply[:wire.MaxText] {
		t.Fatalf("wait reply %+v; want it rejected with the preview inline", waited.Reply)
	}
	b, runID = finishedRun(t)
	waited = waitReply{}
	callTool(t, b.ctx, mcpSession(t, b.connect(t), nil), "wait", map[string]any{"id": runID}, &waited)
	if waited.Message != "done" || waited.Reply != nil {
		t.Fatalf("wait of a short reply %+v, want it inline alone", waited)
	}
	if _, err := os.Stat(filepath.Join(b.stateDir, "runs", runID, "outputs", wire.ReplyFile)); err == nil {
		t.Fatal("wrote reply.txt for a reply within 64 KiB")
	}
}

func TestReplyFileIsNotAnExpectedOutput(t *testing.T) {
	b := newBus(t)
	var stdout, stderr bytes.Buffer
	err := runSubmit(b.ctx, b.client, []string{"--prompt", "work", "--output", wire.ReplyFile}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "reserved for the run's reply") {
		t.Fatalf("submit expecting reply.txt: %v; want it refused", err)
	}
}
