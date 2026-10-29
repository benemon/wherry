package receiver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
	"github.com/twmb/franz-go/pkg/kgo"
)

// outcomeWith is the outcome file of a run that ends in state with parts as its message.
func outcomeWith(state wire.TaskState, parts ...any) string {
	encoded, _ := json.Marshal(map[string]any{"state": state, "message": map[string]any{"role": "ROLE_AGENT", "parts": parts}}) // maps of strings always encode
	return string(encoded)
}

func textPart(text string) map[string]string { return map[string]string{"text": text} }

// replyRun ends a run whose job leaves outcome, under terms, and returns its fixture and last task.
func replyRun(t *testing.T, terms admin.Terms, outcome string) (fixture, *wire.Task) {
	t.Helper()
	f := newFixtureWith(t, noResultStream, terms, testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: noResultStream, outputs: []pluginsdk.Output{outcomeOutput(outcome)}}})
	f.answer(t, credentialName)
	f.execute(t, f.submit(t, f.host, nil), 1)
	return f, f.lastTask(t)
}

// replyChunks are the reply chunks on the tenant's transcript topic, and their bytes joined.
func (f fixture) replyChunks(t *testing.T) ([]wire.Output, string) {
	t.Helper()
	var chunks []wire.Output
	var joined []byte
	for _, chunk := range f.outputChunks(t) {
		if chunk.ArtifactUpdate.Artifact.ArtifactID == wire.ReplyArtifact {
			chunks = append(chunks, chunk)
			joined = append(joined, chunk.ArtifactUpdate.Artifact.Parts[0].Raw...)
		}
	}
	return chunks, string(joined)
}

func sha256Hex(data string) string {
	sum := sha256.Sum256([]byte(data))
	return hex.EncodeToString(sum[:])
}

// A reply over wire.MaxText ends the turn in one transaction: its chunks, indexed from 0 with the last marked, the
// result with the preview and the reply's entry, and the terminal.
func TestLongReplyEndsInOneTransaction(t *testing.T) {
	for _, test := range []struct {
		name   string
		size   int
		chunks int
	}{{"over the bound by one", wire.MaxText + 1, 1}, {"three chunks", 2*wire.OutputChunk + 10, 3}} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, noResultStream)
			reply := strings.Repeat("r", test.size)
			f.plugin.outputs = []pluginsdk.Output{outcomeOutput(outcomeWith(wire.TaskStateCompleted, textPart(reply)))}
			run := testActiveRun(t, f, "run-reply", 1)
			commits := 0
			commit := commitTransaction
			commitTransaction = func(ctx context.Context, producer *kgo.Client) error {
				commits++
				return commit(ctx, producer)
			}
			t.Cleanup(func() { commitTransaction = commit })
			if ack := run.conclude(f.ctx, pluginsdk.Handle{}, nil, pluginsdk.Terminal{Started: true}); ack != kgo.AckAccept || commits != 1 {
				t.Fatalf("ack %v in %d transactions, want accept in 1", ack, commits)
			}
			chunks, joined := f.replyChunks(t)
			if len(chunks) != test.chunks {
				t.Fatalf("%d reply chunks, want %d", len(chunks), test.chunks)
			}
			for i, chunk := range chunks {
				var metadata wire.OutputMetadata
				if json.Unmarshal(chunk.ArtifactUpdate.Metadata, &metadata) != nil || metadata.Index != int64(i) || chunk.ArtifactUpdate.LastChunk != (i == len(chunks)-1) {
					t.Fatalf("chunk %d has metadata %s and last %v", i, chunk.ArtifactUpdate.Metadata, chunk.ArtifactUpdate.LastChunk)
				}
			}
			task := f.lastTask(t)
			listed := manifest(t, task)
			if listed.Reply == nil || listed.Reply.SHA256 != sha256Hex(joined) || listed.Reply.Size != int64(test.size) || listed.Reply.Cut || joined != reply {
				t.Fatalf("reply entry %+v, want the chunks' size and hash, uncut", listed.Reply)
			}
			if task.Artifacts[0].Parts[0].Text != reply[:wire.MaxText] {
				t.Fatal("the result does not carry the preview")
			}
			statuses := f.feedStatuses(t)
			if terminal := statuses[len(statuses)-1]; terminal.Wherry.Kind != "terminal" || terminal.StatusUpdate.Status.Message.Parts[0].Text != reply[:wire.MaxText] {
				t.Fatal("the terminal does not carry the preview")
			}
		})
	}
}

// A reply over the pool's reply limit is cut to it; the turn ends with the outcome's state and the entry is of the cut.
func TestReplyOverTheLimitIsCut(t *testing.T) {
	terms := testTerms()
	terms.ReplyLimit = 100000
	reply := strings.Repeat("y", 300000)
	f, task := replyRun(t, terms, outcomeWith(wire.TaskStateCompleted, textPart(reply)))
	_, joined := f.replyChunks(t)
	listed := manifest(t, task)
	if task.Status.State != wire.TaskStateCompleted || task.Artifacts[0].Parts[0].Text != reply[:wire.MaxText] {
		t.Fatalf("task ended %s; want the outcome's state and the preview", task.Status.State)
	}
	if joined != reply[:100000] || listed.Reply == nil || !listed.Reply.Cut || listed.Reply.Size != 100000 || listed.Reply.SHA256 != sha256Hex(reply[:100000]) {
		t.Fatalf("chunks hold %d bytes with entry %+v; want the first 100000, cut", len(joined), listed.Reply)
	}
}

// The outcome file is bounded by the pool's output limit.
func TestOutcomeFileIsBoundedByTheOutputLimit(t *testing.T) {
	terms := testTerms()
	terms.OutputLimit = 700 << 10
	for _, test := range []struct {
		name  string
		size  int
		state wire.TaskState
	}{{"over 512 KiB", 600 << 10, wire.TaskStateCompleted}, {"over the output limit", 701 << 10, wire.TaskStateFailed}} {
		t.Run(test.name, func(t *testing.T) {
			_, task := replyRun(t, terms, outcomeWith(wire.TaskStateCompleted, textPart(strings.Repeat("o", test.size))))
			if task.Status.State != test.state || test.state == wire.TaskStateFailed && task.Status.Message.Parts[0].Text != "agent reported no outcome" {
				t.Fatalf("task ended %s with %.40q, want %s", task.Status.State, task.Status.Message.Parts[0].Text, test.state)
			}
		})
	}
}

// A reply of wire.MaxText bytes travels whole in the result and the terminal, as before reply chunks: no chunks, no
// entry, and a data part when the agent gave a data part.
func TestReplyAtTheBoundIsWhole(t *testing.T) {
	text := `{"a":"` + strings.Repeat("j", wire.MaxText-8) + `"}`
	f, task := replyRun(t, testTerms(), outcomeWith(wire.TaskStateCompleted, map[string]any{"data": json.RawMessage(text)}))
	chunks, _ := f.replyChunks(t)
	part := task.Artifacts[0].Parts[0]
	if len(text) != wire.MaxText || len(chunks) != 0 || manifest(t, task).Reply != nil {
		t.Fatalf("%d chunks and entry %+v for a %d-byte reply, want none", len(chunks), manifest(t, task).Reply, len(text))
	}
	if string(part.Data) != text || part.MediaType != "application/json" || task.Status.Message.Parts[0].Text != text {
		t.Fatalf("result part %.60v, want the whole reply as a data part and the status text", part)
	}
}

// Each cut ends before a rune it would split.
func TestReplyCutsKeepWholeRunes(t *testing.T) {
	terms := testTerms()
	terms.ReplyLimit = 100000
	reply := strings.Repeat("a", wire.MaxText-1) + "€" + strings.Repeat("b", 100000-wire.MaxText-3) + "€" + "tail"
	f, task := replyRun(t, terms, outcomeWith(wire.TaskStateCompleted, textPart(reply)))
	_, joined := f.replyChunks(t)
	preview := task.Artifacts[0].Parts[0].Text
	if !utf8.ValidString(preview) || preview != reply[:wire.MaxText-1] {
		t.Fatalf("preview of %d bytes, want the %d before the rune", len(preview), wire.MaxText-1)
	}
	if !utf8.ValidString(joined) || joined != reply[:100000-1] {
		t.Fatalf("chunks hold %d bytes, want the %d before the rune", len(joined), 100000-1)
	}
}

// The terminal and the result's status carry one text part, the preview, and for auth required the credential part.
func TestReplyStatusHasOneTextPart(t *testing.T) {
	var parts []any
	for range wire.MaxParts - 1 {
		parts = append(parts, textPart(strings.Repeat("p", 1000)))
	}
	parts = append(parts, map[string]any{"data": map[string]string{"credential": "github-token"}})
	f, task := replyRun(t, testTerms(), outcomeWith(wire.TaskStateAuthRequired, parts...))
	whole := strings.TrimSuffix(strings.Repeat(strings.Repeat("p", 1000)+"\n", wire.MaxParts-1), "\n")
	statuses := f.feedStatuses(t)
	for _, status := range []wire.TaskStatus{task.Status, statuses[len(statuses)-1].StatusUpdate.Status} {
		got := status.Message.Parts
		if len(got) != 2 || got[0].Text != whole[:wire.MaxText] || string(got[1].Data) != `{"credential":"github-token"}` {
			t.Fatalf("status has %d parts, the first of %d bytes; want the preview and the credential", len(got), len(got[0].Text))
		}
	}
}

// The credential part is no reply: an agent that reports AUTH_REQUIRED with it alone leaves the result no text.
func TestCredentialPartIsNoReply(t *testing.T) {
	_, task := replyRun(t, testTerms(), outcomeWith(wire.TaskStateAuthRequired, map[string]any{"data": map[string]string{"credential": "github-token"}}))
	parts := task.Status.Message.Parts
	if task.Status.State != wire.TaskStateFailed || len(parts) != 1 || string(parts[0].Data) != `{"credential":"github-token"}` || task.Artifacts[0].Parts[0].Text != string(wire.TaskStateFailed) {
		t.Fatalf("ended %s with %+v and result %+v, want FAILED with the credential part alone", task.Status.State, parts, task.Artifacts[0].Parts[0])
	}
}

// The preamble names the pool's reply limit.
func TestPreambleNamesTheReplyLimit(t *testing.T) {
	terms := testTerms()
	terms.ReplyLimit = 123456
	f, _ := replyRun(t, terms, outcomeWith(wire.TaskStateCompleted, textPart("done")))
	if prompt := string(f.lastRun().Prompt); !strings.Contains(prompt, "A reply over "+strconv.Itoa(123456)+" bytes is cut.") ||
		!slices.ContainsFunc(strings.Split(prompt, "\n"), func(line string) bool { return strings.HasPrefix(line, "The answer goes in the message text") }) {
		t.Fatalf("preamble %q does not name the reply limit", prompt)
	}
}
