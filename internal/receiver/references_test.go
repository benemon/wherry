package receiver

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
	"github.com/twmb/franz-go/pkg/kgo"
)

// referenceRun ends a run whose job prints stream and leaves outputs beside an outcome replying reply, and returns its
// fixture and last task.
func referenceRun(t *testing.T, stream []string, reply string, outputs ...pluginsdk.Output) (fixture, *wire.Task) {
	t.Helper()
	outputs = append(outputs, outcomeOutput(outcomeWith(wire.TaskStateCompleted, textPart(reply))))
	f := newFixtureWith(t, stream, testTerms(), testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: stream, outputs: outputs}})
	f.answer(t, credentialName)
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckAccept {
		t.Fatalf("run ended %v, want accepted", status)
	}
	return f, f.lastTask(t)
}

// reference decodes the data of a reference artifact's one part.
func reference(t *testing.T, artifact wire.Artifact) map[string]any {
	t.Helper()
	if len(artifact.Parts) != 1 || artifact.Parts[0].MediaType != wire.ReferenceMediaType || artifact.Extensions[0] != wire.ExtensionURI {
		t.Fatalf("artifact %s is %+v, want one reference part declaring the extension", artifact.ArtifactID, artifact)
	}
	var data map[string]any
	if err := json.Unmarshal(artifact.Parts[0].Data, &data); err != nil {
		t.Fatal(err)
	}
	return data
}

// The final task names each artifact the run streamed after the result's text: each output by its name and the reply
// with the sha256 and size of the bytes their chunks carry, as the manifest lists them, and the transcript by name.
func TestResultReferencesEachStreamedArtifact(t *testing.T) {
	reply := strings.Repeat("r", wire.MaxText+1)
	f, task := referenceRun(t, noResultStream, reply, file("report.md", "# Report\n"), file("notes/extra.txt", "extra\n"))
	listed := manifest(t, task)
	ids := []string{}
	for _, artifact := range task.Artifacts {
		ids = append(ids, artifact.ArtifactID)
	}
	if want := []string{wire.ResultArtifact, "notes/extra.txt", "report.md", wire.ReplyArtifact, wire.TranscriptArtifact}; strings.Join(ids, " ") != strings.Join(want, " ") {
		t.Fatalf("artifacts %q, want %q", ids, want)
	}
	if task.Artifacts[0].Parts[0].Text != reply[:wire.MaxText] {
		t.Fatal("the result does not keep the preview")
	}
	chunks := map[string][]byte{}
	for _, chunk := range f.outputChunks(t) {
		chunks[chunk.ArtifactUpdate.Artifact.ArtifactID] = append(chunks[chunk.ArtifactUpdate.Artifact.ArtifactID], chunk.ArtifactUpdate.Artifact.Parts[0].Raw...)
	}
	for i, output := range listed.Outputs {
		artifact := task.Artifacts[1+i]
		data := reference(t, artifact)
		if artifact.Name != output.Name || len(data) != 2 || data["sha256"] != output.SHA256 || data["size"] != float64(output.Size) ||
			output.SHA256 != sha(chunks[output.Name]) || output.Size != int64(len(chunks[output.Name])) {
			t.Fatalf("output %s referenced by %+v with %v, want its name, and the sha256 and size of its chunks %+v", output.Name, artifact, data, output)
		}
	}
	replied := task.Artifacts[3]
	if data := reference(t, replied); replied.Name != "" || len(data) != 2 || data["sha256"] != listed.Reply.SHA256 || data["size"] != float64(listed.Reply.Size) ||
		listed.Reply.SHA256 != sha(chunks[wire.ReplyArtifact]) || listed.Reply.Size != int64(len(reply)) {
		t.Fatalf("reply referenced by %+v with %v, want the sha256 and size of its chunks %+v", replied, data, listed.Reply)
	}
	transcript := task.Artifacts[4]
	if data := reference(t, transcript); transcript.Name != "" || string(transcript.Parts[0].Data) != "{}" || len(data) != 0 || len(f.chunks(t)) == 0 {
		t.Fatalf("transcript referenced by %+v, want an empty object naming the transcript's chunks", transcript)
	}
}

// A run with no output, a reply within the result and no transcript line has the result's text alone.
func TestResultReferencesNothingUnstreamed(t *testing.T) {
	_, task := referenceRun(t, []string{}, "done")
	if len(task.Artifacts) != 1 || task.Artifacts[0].ArtifactID != wire.ResultArtifact || task.Artifacts[0].Parts[0].Text != "done" {
		t.Fatalf("artifacts %+v, want the result alone", task.Artifacts)
	}
}

// An output named result or transcript would take a result artifact's ID, so neither is collected.
func TestOutputsNamedAsResultArtifactsAreNotCollected(t *testing.T) {
	f, task := referenceRun(t, noResultStream, "done", file(wire.ResultArtifact, "x"), file(wire.TranscriptArtifact, "y"), file("report.md", "z"))
	listed := manifest(t, task)
	want := []wire.UnreturnedOutput{{Name: wire.ResultArtifact, Size: 1, Reason: "rejected: reserved name"}, {Name: wire.TranscriptArtifact, Size: 1, Reason: "rejected: reserved name"}}
	if len(listed.Outputs) != 1 || listed.Outputs[0].Name != "report.md" || len(listed.NotReturned) != 2 || listed.NotReturned[0] != want[0] || listed.NotReturned[1] != want[1] {
		t.Fatalf("returned %+v, not returned %+v; want report.md alone returned", listed.Outputs, listed.NotReturned)
	}
	for _, chunk := range f.outputChunks(t) {
		if id := chunk.ArtifactUpdate.Artifact.ArtifactID; id != "report.md" {
			t.Fatalf("output chunk of %s, want report.md's alone", id)
		}
	}
	if len(task.Artifacts) != 3 || task.Artifacts[1].ArtifactID != "report.md" || task.Artifacts[2].ArtifactID != wire.TranscriptArtifact {
		t.Fatalf("artifacts %+v, want the result, report.md and the transcript", task.Artifacts)
	}
}
