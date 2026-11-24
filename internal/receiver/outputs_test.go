package receiver

import (
	"bytes"
	"context"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
	"github.com/twmb/franz-go/pkg/kgo"
)

// endedWith runs a turn declaring expected whose job leaves outcome and outputs, and returns its last task.
func endedWith(t *testing.T, expected []wire.ExpectedOutput, outcome string, outputs ...pluginsdk.Output) (*wire.Task, kgo.AckStatus) {
	t.Helper()
	outputs = append([]pluginsdk.Output{outcomeOutput(outcome)}, outputs...)
	f := newFixtureWith(t, noResultStream, testTerms(), testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: noResultStream, outputs: outputs}})
	status := f.execute(t, f.submit(t, f.host, func(task *wire.Submission) { task.Wherry.Outputs = expected }), 1)
	return f.lastTask(t), status
}

// A completed run answers: one whose outcome file leaves no reply, text once trimmed or a data part, fails. The other
// states keep their message, and a missing declared output keeps its own reason.
func TestACompletedRunWithoutAReplyFails(t *testing.T) {
	const noReply = "agent returned no reply"
	credential := map[string]any{"data": map[string]string{"credential": "github-token"}}
	for name, test := range map[string]struct {
		expected []wire.ExpectedOutput
		outcome  string
		state    wire.TaskState
		text     string
	}{
		"no part":                     {nil, outcomeWith(wire.TaskStateCompleted), wire.TaskStateFailed, noReply},
		"empty text":                  {nil, outcomeWith(wire.TaskStateCompleted, textPart("")), wire.TaskStateFailed, noReply},
		"whitespace":                  {nil, outcomeWith(wire.TaskStateCompleted, textPart(" \n\t ")), wire.TaskStateFailed, noReply},
		"text":                        {nil, outcomeWith(wire.TaskStateCompleted, textPart(" the answer ")), wire.TaskStateCompleted, " the answer "},
		"data part":                   {nil, outcomeWith(wire.TaskStateCompleted, map[string]any{"data": map[string]int{"answer": 1}}), wire.TaskStateCompleted, `{"answer":1}`},
		"credential part alone":       {nil, outcomeWith(wire.TaskStateAuthRequired, credential), wire.TaskStateFailed, ""},
		"failed without a reply":      {nil, outcomeWith(wire.TaskStateFailed), wire.TaskStateFailed, ""},
		"missing output and no reply": {[]wire.ExpectedOutput{{Name: "report.md"}}, outcomeWith(wire.TaskStateCompleted), wire.TaskStateFailed, "required output report.md not returned: missing"},
	} {
		t.Run(name, func(t *testing.T) {
			task, _ := endedWith(t, test.expected, test.outcome)
			text := ""
			if task.Status.Message != nil {
				for _, part := range task.Status.Message.Parts {
					text += part.Text
				}
			}
			if task.Status.State != test.state || text != test.text {
				t.Fatalf("ended %s with %q, want %s with %q", task.Status.State, text, test.state, test.text)
			}
			if name == "credential part alone" {
				if parts := task.Status.Message.Parts; len(parts) != 1 || string(parts[0].Data) != `{"credential":"github-token"}` {
					t.Fatalf("parts %+v, want the credential part alone", parts)
				}
			}
		})
	}
}

// An agent file named reply.txt is not returned: the host writes the reply there among the outputs.
func TestReplyFileFromTheAgentIsReserved(t *testing.T) {
	_, task, status := runWithOutputs(t, []wire.ExpectedOutput{{Name: "report.md"}}, []pluginsdk.Output{file(wire.ReplyFile, "mine"), file("report.md", "r")}, nil)
	if status != kgo.AckAccept || task.Status.State != wire.TaskStateCompleted {
		t.Fatalf("ended %v %s %+v, want COMPLETED", status, task.Status.State, task.Status.Message)
	}
	listed := manifest(t, task)
	if len(listed.Outputs) != 1 || listed.Outputs[0].Name != "report.md" || !slices.Equal(listed.NotReturned, []wire.UnreturnedOutput{{Name: wire.ReplyFile, Size: 4, Reason: "rejected: reserved name"}}) {
		t.Fatalf("returned %+v, not returned %+v; want reply.txt listed reserved", listed.Outputs, listed.NotReturned)
	}
}

func gitTask(outputs ...wire.ExpectedOutput) func(*wire.Submission) {
	return func(task *wire.Submission) {
		task.Wherry.Git = &wire.GitInput{Remote: "https://git.example.test/org/repo.git", Ref: "main"}
		task.Wherry.Outputs = outputs
	}
}

// A git run that commits nothing leaves no changes file; the run completes with the file listed missing.
func TestAGitRunCompletesWithoutItsChangesFile(t *testing.T) {
	for _, name := range []string{wire.GitBundle, wire.GitPatch} {
		t.Run(name, func(t *testing.T) {
			f := newFixtureWith(t, resultStream, vcsTerms("git.example.test"), testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream}})
			status := f.execute(t, f.submit(t, f.host, gitTask(wire.ExpectedOutput{Name: name})), 1)
			task := f.lastTask(t)
			if status != kgo.AckAccept || task.Status.State != wire.TaskStateCompleted {
				t.Fatalf("ended %v %s %+v, want COMPLETED", status, task.Status.State, task.Status.Message)
			}
			if listed := manifest(t, task); !slices.Equal(listed.NotReturned, []wire.UnreturnedOutput{{Name: name, Reason: "missing"}}) {
				t.Fatalf("not returned %+v, want %s missing", listed.NotReturned, name)
			}
		})
	}
}

// Without a git input, an output of the changes file's name is a declared output like any other.
func TestTheChangesFileIsRequiredWithoutAGitInput(t *testing.T) {
	_, task, status := runWithOutputs(t, []wire.ExpectedOutput{{Name: wire.GitBundle}}, nil, nil)
	if status != kgo.AckReject || task.Status.State != wire.TaskStateFailed || task.Status.Message.Parts[0].Text != "required output changes.bundle not returned: missing" {
		t.Fatalf("ended %v %s %+v, want FAILED naming changes.bundle", status, task.Status.State, task.Status.Message)
	}
}

// The changes file is streamed outside the plugin's total and charged against output_limit after every other output,
// so a large bundle is the output that exceeds the limit, never a declared one.
func TestAGitRunsBundleIsChargedLast(t *testing.T) {
	report, bundle := strings.Repeat("r", 60), strings.Repeat("b", 60)
	streamed := func() pluginsdk.Output {
		return pluginsdk.Output{Name: wire.GitBundle, Size: int64(len(bundle)), Body: io.NopCloser(strings.NewReader(bundle))}
	}
	for name, test := range map[string]struct {
		limit    int64
		bundle   pluginsdk.Output
		returned []string
	}{
		"over the limit, read":     {100, file(wire.GitBundle, bundle), []string{"report.md"}},
		"over the limit, streamed": {100, streamed(), []string{"report.md"}},
		"within the limit":         {200, streamed(), []string{"report.md", wire.GitBundle}},
	} {
		t.Run(name, func(t *testing.T) {
			terms := vcsTerms("git.example.test")
			terms.OutputLimit = test.limit
			f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream, outputs: []pluginsdk.Output{test.bundle, file("report.md", report)}}})
			status := f.execute(t, f.submit(t, f.host, gitTask(wire.ExpectedOutput{Name: "report.md"}, wire.ExpectedOutput{Name: wire.GitBundle})), 1)
			task := f.lastTask(t)
			if status != kgo.AckAccept || task.Status.State != wire.TaskStateCompleted {
				t.Fatalf("ended %v %s %+v, want COMPLETED", status, task.Status.State, task.Status.Message)
			}
			listed := manifest(t, task)
			var returned []string
			for _, output := range listed.Outputs {
				returned = append(returned, output.Name)
			}
			if !slices.Equal(returned, test.returned) {
				t.Fatalf("returned %+v, not returned %+v; want %v", listed.Outputs, listed.NotReturned, test.returned)
			}
			if len(test.returned) == 1 && !slices.Equal(listed.NotReturned, []wire.UnreturnedOutput{{Name: wire.GitBundle, Size: 60, Reason: "exceeds output limit"}}) {
				t.Fatalf("not returned %+v, want the bundle over the limit", listed.NotReturned)
			}
			if len(test.returned) == 2 && (listed.Outputs[1].SHA256 != sha([]byte(bundle)) || listed.Outputs[1].Size != 60) {
				t.Fatalf("bundle listed as %+v, want its bytes' hash and size", listed.Outputs[1])
			}
			f.plugin.mu.Lock()
			defer f.plugin.mu.Unlock()
			if limits := f.plugin.limits; !slices.Contains(limits.Stream, wire.GitBundle) || limits.TotalBytes != test.limit {
				t.Fatalf("collect limits %+v, want the bundle streamed outside the total", limits)
			}
		})
	}
}

func continuedTask(outputs ...wire.ExpectedOutput) func(*wire.Submission) {
	return func(task *wire.Submission) {
		gitTask(outputs...)(task)
		task.Wherry.Git.Base = &wire.GitBase{Bundle: "base.bundle", Commit: strings.Repeat("ab", 20)}
		task.Message.ContextID = testRun("run-0")
		task.Wherry.S3Inputs = []wire.S3Input{{Name: "base.bundle", Size: 1, SHA256: sha([]byte("x"))}}
	}
}

// A continuation's bundle returns through S3 whatever its size, and a round that commits nothing completes without one.
func TestAContinuationReturnsItsBundleThroughS3(t *testing.T) {
	bundle := bytes.Repeat([]byte("b"), 1000)
	for name, test := range map[string]struct {
		outputs []pluginsdk.Output
		missing bool
	}{
		"over output_limit": {[]pluginsdk.Output{{Name: wire.GitBundle, Size: int64(len(bundle)), Body: io.NopCloser(bytes.NewReader(bundle))}}, false},
		"no new commit":     {nil, true},
	} {
		t.Run(name, func(t *testing.T) {
			s := newStore(t)
			terms := s3Terms(s.endpoint())
			terms.VCS, terms.OutputLimit = []string{"git.example.test"}, 100
			f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream, outputs: test.outputs}})
			status := f.execute(t, f.submit(t, f.host, continuedTask(wire.ExpectedOutput{Name: wire.GitBundle, Storage: wire.StorageS3})), 1)
			task := f.lastTask(t)
			if status != kgo.AckAccept || task.Status.State != wire.TaskStateCompleted {
				t.Fatalf("ended %v %s %+v, want COMPLETED; logs:\n%s", status, task.Status.State, task.Status.Message, f.logs.String())
			}
			listed := manifest(t, task)
			key := wire.S3OutputKey(testTenant, testRun("run-a"), wire.GitBundle)
			if test.missing {
				if len(listed.S3Outputs) != 0 || !slices.Equal(listed.NotReturned, []wire.UnreturnedOutput{{Name: wire.GitBundle, Reason: "missing"}}) {
					t.Fatalf("S3 outputs %+v, not returned %+v; want the bundle missing", listed.S3Outputs, listed.NotReturned)
				}
				return
			}
			if data, ok := s.put("/" + s3Bucket + "/" + key); !ok || !bytes.Equal(data, bundle) {
				t.Fatalf("store holds %d bytes, %v; want the bundle", len(data), ok)
			}
			if !slices.Equal(listed.S3Outputs, []wire.ReturnedOutput{{Name: wire.GitBundle, SHA256: sha(bundle), Size: int64(len(bundle)), Key: key}}) || len(listed.NotReturned) != 0 {
				t.Fatalf("S3 outputs %+v, not returned %+v; want the bundle through S3", listed.S3Outputs, listed.NotReturned)
			}
		})
	}
}

// A run stopped at task_ended_by collects until the bound plus the upload grace, when its PUT URLs expire, so its S3
// output uploads; the store refuses an upload past the grace.
func TestATimedOutRunUploadsWithinTheGrace(t *testing.T) {
	for name, test := range map[string]struct {
		after    time.Duration
		uploaded bool
	}{"within the grace": {9 * time.Minute, true}, "past the grace": {11 * time.Minute, false}} {
		t.Run(name, func(t *testing.T) {
			s := newStore(t)
			endBy := time.Now().Add(2 * time.Second).Truncate(time.Second)
			s.clock = func() time.Time { return endBy.Add(test.after) }
			fake := &fakePlugin{stream: noResultStream, endDelay: 30 * time.Second, outputs: []pluginsdk.Output{{Name: "report.pdf", Size: 6, Body: io.NopCloser(strings.NewReader("report"))}}}
			f := newFixtureWith(t, noResultStream, s3Terms(s.endpoint()), testConfig(testReceiver), map[string]*fakePlugin{"nomad": fake})
			signedAt := time.Now()
			f.envelope = withURLs(func(input, output string) (string, string) {
				return presignedUntil(s.endpoint(), wire.S3InputKey(testTenant, testRun("run-a"), input), signedAt, 15*time.Minute),
					presignedUntil(s.endpoint(), wire.S3OutputKey(testTenant, testRun("run-a"), output), signedAt, endBy.Sub(signedAt)+operator.UploadGrace)
			})
			record := f.submit(t, f.host, func(task *wire.Submission) {
				withS3(task)
				task.Wherry.TaskStartedBy, task.Wherry.TaskEndedBy = endBy.Unix(), endBy.Unix()
			})
			// The fixture's context carries the test's own deadline; the receiver's does not bound a collection.
			status := <-f.executing(t, context.WithoutCancel(f.ctx), record, 1)
			task := f.lastTask(t)
			if status != kgo.AckReject || task.Status.State != wire.TaskStateFailed || task.Status.Message.Parts[0].Text != wire.NotEndedBy(endBy.Unix()) {
				t.Fatalf("ended %v %s %+v, want FAILED at task_ended_by; logs:\n%s", status, task.Status.State, task.Status.Message, f.logs.String())
			}
			listed := manifest(t, task)
			key := wire.S3OutputKey(testTenant, testRun("run-a"), "report.pdf")
			_, held := s.put("/" + s3Bucket + "/" + key)
			switch {
			case test.uploaded && (!held || !slices.Equal(listed.S3Outputs, []wire.ReturnedOutput{{Name: "report.pdf", SHA256: sha([]byte("report")), Size: 6, Key: key}})):
				t.Fatalf("store holds the output: %v; S3 outputs %+v, not returned %+v; want the output uploaded", held, listed.S3Outputs, listed.NotReturned)
			case !test.uploaded && (held || !slices.Equal(listed.NotReturned, []wire.UnreturnedOutput{{Name: "report.pdf", Size: 6, Reason: "not uploaded: status 403"}})):
				t.Fatalf("store holds the output: %v; not returned %+v; want the upload refused", held, listed.NotReturned)
			}
			fake.mu.Lock()
			deadline := fake.collectDeadline
			fake.mu.Unlock()
			if want := endBy.Add(operator.UploadGrace); deadline.Before(want.Add(-time.Second)) || deadline.After(want.Add(time.Second)) {
				t.Fatalf("collect deadline %v, want task_ended_by plus the grace, %v; logs:\n%s", deadline, want, f.logs.String())
			}
		})
	}
}
