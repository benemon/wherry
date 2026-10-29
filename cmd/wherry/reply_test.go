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

// watch prints the whole verified reply where the status message prints, and notes a cut or a rejection on stderr.
func TestWatchPrintsTheWholeReply(t *testing.T) {
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
			for _, mode := range []string{"--result-only", ""} {
				var stdout, stderr bytes.Buffer
				args := []string{runID, "--outputs-dir", t.TempDir(), "--timeout", "5s"}
				if mode != "" {
					args = append(args, mode)
				}
				if err := runWatch(b.ctx, b.client, args, &stdout, &stderr); err != nil {
					t.Fatalf("watch %s: %v", mode, err)
				}
				want := test.stdout + "\n"
				if mode == "" {
					want = "TASK_STATE_COMPLETED\n" + want
				}
				if !strings.HasPrefix(stdout.String(), want) || mode != "" && stdout.String() != want || stderr.String() != test.stderr {
					t.Fatalf("watch %s printed %d bytes starting %.40q, stderr %q; want %d bytes and %q", mode, stdout.Len(), stdout.String(), stderr.String(), len(want), test.stderr)
				}
			}
		})
	}
}

// MCP watch keeps the preview inline and writes the whole reply to reply.txt among the run's outputs.
func TestMCPWatchWritesTheReply(t *testing.T) {
	reply := strings.Repeat("m", wire.OutputChunk) + " the end"
	b, runID := replyRun(t, reply, reply, reply, false)
	var watched watchReply
	callTool(t, b.ctx, mcpSession(t, b.connect(t), nil), "watch", map[string]any{"id": runID}, &watched)
	path := filepath.Join(b.stateDir, "runs", runID, "outputs", wire.ReplyFile)
	if watched.Message != reply[:wire.MaxText] || watched.Reply == nil || watched.Reply.Path != path || watched.Reply.Size != int64(len(reply)) || watched.Reply.Cut {
		t.Fatalf("watch message of %d bytes, reply %+v; want the preview and %s", len(watched.Message), watched.Reply, path)
	}
	if written, err := os.ReadFile(path); err != nil || hashOf(string(written)) != watched.Reply.SHA256 || string(written) != reply {
		t.Fatalf("reply.txt holds %d bytes, %v; want the whole reply hashing to reply.sha256", len(written), err)
	}
	b, runID = replyRun(t, reply, reply[:wire.OutputChunk], reply, false)
	watched = watchReply{}
	callTool(t, b.ctx, mcpSession(t, b.connect(t), nil), "watch", map[string]any{"id": runID}, &watched)
	if watched.Reply == nil || watched.Reply.Rejected != "size or hash mismatch" || watched.Reply.Path != "" || watched.Message != reply[:wire.MaxText] {
		t.Fatalf("watch reply %+v; want it rejected with the preview inline", watched.Reply)
	}
	b, runID = watchedRun(t)
	watched = watchReply{}
	callTool(t, b.ctx, mcpSession(t, b.connect(t), nil), "watch", map[string]any{"id": runID}, &watched)
	if watched.Message != "done" || watched.Reply != nil {
		t.Fatalf("watch of a short reply %+v, want it inline alone", watched)
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
