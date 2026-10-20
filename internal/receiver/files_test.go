package receiver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io/fs"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/adapter"
	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

const deployValue = "deploy-key-value-never-published"

func withDeployKey(task *wire.Submission) {
	task.Wherry.Credentials = []wire.TaskCredential{{Name: "deploy-key", File: "id_ed25519"}}
}

// submitPayload signs a submission's payload after edit, bypassing the rules SignRecord applies, as a host other than
// wherry's client could.
func (f fixture) submitPayload(t *testing.T, edit func(map[string]any)) *kgo.Record {
	t.Helper()
	submission := testSubmission("run-a")
	submission.V = 1
	encoded := must(t)(wire.SignRecord(submission, f.host))
	payload, err := base64.RawURLEncoding.DecodeString(strings.Split(string(encoded), ".")[1])
	if err != nil {
		t.Fatal(err)
	}
	var named map[string]any
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&named); err != nil {
		t.Fatal(err)
	}
	edit(named)
	value := must(t)(signed.Sign("application/vnd.wherry.submission.v1+jws", signed.HostOwner(testTenant, testHost), named, f.host))
	return f.produce(t, admin.PoolTasksTopic(testPool), "run-a", value)
}

func (f fixture) lastRun() adapter.Run {
	f.adapter.mu.Lock()
	defer f.adapter.mu.Unlock()
	return f.adapter.lastRun
}

func TestInputPartsLandAsNamedFiles(t *testing.T) {
	f := newFixture(t, resultStream)
	f.answer(t, credentialName)
	record := f.submit(t, f.host, func(task *wire.Submission) {
		task.Message.Parts = append(task.Message.Parts,
			wire.Part{Raw: wire.StdBytes("a,b\n1,2\n"), Filename: "data.csv", MediaType: "text/csv"},
			wire.Part{Data: jsontext.Value(`{"k":[1,2]}`), Filename: "config", MediaType: "application/json"},
			wire.Part{Raw: wire.StdBytes{}, Filename: "empty"})
	})
	if status := f.execute(t, record, 1); status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	want := []adapter.File{{Name: "data.csv", Data: []byte("a,b\n1,2\n")}, {Name: "config.json", Data: []byte(`{"k":[1,2]}`)}, {Name: "empty", Data: []byte{}}}
	run := f.lastRun()
	if !slices.EqualFunc(run.Inputs, want, func(a, b adapter.File) bool { return a.Name == b.Name && bytes.Equal(a.Data, b.Data) }) {
		t.Fatalf("inputs %q, want %q", run.Inputs, want)
	}
	if prompt := string(run.Prompt); !strings.Contains(prompt, "Input files: /task/inputs/data.csv, /task/inputs/config.json, /task/inputs/empty.") || !strings.HasSuffix(prompt, "\n\nhi") {
		t.Fatalf("prompt %q, want the preamble naming the inputs before the text", prompt)
	}
	// The result's history names each input and carries none of its bytes.
	submitted := f.parse(t, record).Message.Parts
	history := f.lastTask(t).History[0].Parts
	if len(history) != len(submitted) || history[0].Text != submitted[0].Text {
		t.Fatalf("history parts %+v, want the submission's", history)
	}
	for i, content := range [][]byte{nil, submitted[1].Raw, submitted[2].Data, submitted[3].Raw} {
		if content == nil {
			continue
		}
		pointer, _ := json.Marshal(wire.PointerMetadata{SHA256: fmt.Sprintf("%x", sha256.Sum256(content)), Size: int64(len(content))})
		want := wire.Part{URL: "unstored:input", Filename: submitted[i].Filename, MediaType: submitted[i].MediaType}
		if got := history[i]; got.URL != want.URL || got.Filename != want.Filename || got.MediaType != want.MediaType || got.Raw != nil || got.Data != nil || string(got.Metadata) != string(pointer) {
			t.Fatalf("history part %d is %+v, want %+v with metadata %s", i, got, want, pointer)
		}
	}
}

func TestInvalidInputNamesAreRejected(t *testing.T) {
	for name, parts := range map[string][]map[string]any{
		"leading dot": {{"raw": "eA==", "filename": ".hidden"}},
		"separator":   {{"raw": "eA==", "filename": "a/b"}},
		"no filename": {{"raw": "eA=="}},
		"too long":    {{"raw": "eA==", "filename": strings.Repeat("a", 256)}},
		"duplicate":   {{"raw": "eA==", "filename": "x.json"}, {"data": 1, "filename": "x"}},
		"data name":   {{"data": 1, "filename": strings.Repeat("a", 251)}},
		"shell":       {{"raw": "eA==", "filename": "a b"}},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			record := f.submitPayload(t, func(payload map[string]any) {
				message := payload["message"].(map[string]any)
				for _, part := range parts {
					message["parts"] = append(message["parts"].([]any), part)
				}
			})
			if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckReject {
				t.Fatalf("admitted %v with ack %v, want a rejection", admitted, ack)
			}
		})
	}
}

func TestOver256ExpectedOutputsAreRejected(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.submitPayload(t, func(payload map[string]any) {
		var outputs []any
		for i := range wire.MaxOutputs + 1 {
			outputs = append(outputs, map[string]any{"name": fmt.Sprintf("o%d", i)})
		}
		payload["wherry"].(map[string]any)["outputs"] = outputs
	})
	if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckReject {
		t.Fatalf("admitted %v with ack %v, want a rejection", admitted, ack)
	}
}

func TestLongTaskCredentialNameIsRejected(t *testing.T) {
	f := newFixture(t, resultStream)
	record := f.submitPayload(t, func(payload map[string]any) {
		payload["wherry"].(map[string]any)["credentials"] = []any{map[string]any{"name": strings.Repeat("c", 33), "file": "token"}}
	})
	if admitted, ack := f.receiver.admit(f.ctx, record); admitted != nil || ack != kgo.AckReject {
		t.Fatalf("admitted %v with ack %v, want a rejection", admitted, ack)
	}
}

func TestTaskCredentialsReachTheAdapterAsFiles(t *testing.T) {
	f := newFixture(t, resultStream)
	done := f.executing(t, f.ctx, f.submit(t, f.host, withDeployKey), 1)
	request := f.request(t, 1)
	if !slices.Equal(request.Credentials, []string{credentialName}) || !slices.Equal(request.TaskCredentials, []string{"deploy-key"}) {
		t.Fatalf("seal request names %v and task credentials %v", request.Credentials, request.TaskCredentials)
	}
	f.sealFor(t, request, map[string][]byte{credentialName: []byte(testToken), "deploy-key": []byte(deployValue)})
	if status := <-done; status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	run := f.lastRun()
	if string(run.Credential) != testToken || !slices.EqualFunc(run.Credentials, []adapter.Secret{{Name: "deploy-key", File: "id_ed25519", Value: []byte(deployValue)}},
		func(a, b adapter.Secret) bool {
			return a.Name == b.Name && a.File == b.File && bytes.Equal(a.Value, b.Value)
		}) {
		t.Fatalf("dispatched credential %q and task credentials %+v", run.Credential, run.Credentials)
	}
	if len(run.Inputs) != 0 || strings.Contains(string(run.Prompt), deployValue) || !strings.Contains(string(run.Prompt), "Credential files: /task/secrets/credentials/id_ed25519.") {
		t.Fatalf("inputs %+v and prompt %q; want the credential file named, its value nowhere", run.Inputs, run.Prompt)
	}
}

func TestTaskCredentialsWithoutAnAgentCredential(t *testing.T) {
	config := testConfig(testReceiver)
	config.Adapters[0].Credential = ""
	f := newFixtureWith(t, resultStream, testTerms(), config, map[string]*fakeAdapter{"nomad": {stream: resultStream}})
	done := f.executing(t, f.ctx, f.submit(t, f.host, withDeployKey), 1)
	request := f.request(t, 1)
	if request.Credentials == nil || len(request.Credentials) != 0 || !slices.Equal(request.TaskCredentials, []string{"deploy-key"}) {
		t.Fatalf("seal request names %#v and task credentials %v", request.Credentials, request.TaskCredentials)
	}
	f.sealFor(t, request, map[string][]byte{"deploy-key": []byte(deployValue)})
	if status := <-done; status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	if run := f.lastRun(); run.Credential != nil || len(run.Credentials) != 1 || string(run.Credentials[0].Value) != deployValue {
		t.Fatalf("dispatched %q and %+v", run.Credential, run.Credentials)
	}
}

func TestTaskCredentialOfTheAgentsNameIsAskedOnce(t *testing.T) {
	f := newFixture(t, resultStream)
	f.answer(t, credentialName)
	record := f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Credentials = []wire.TaskCredential{{Name: credentialName, File: "token"}}
	})
	if status := f.execute(t, record, 1); status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	if run := f.lastRun(); string(run.Credential) != testToken || len(run.Credentials) != 1 || string(run.Credentials[0].Value) != testToken || run.Credentials[0].File != "token" {
		t.Fatalf("dispatched %q and %+v", run.Credential, run.Credentials)
	}
}

func TestMissingTaskCredentialInterruptsNamingIt(t *testing.T) {
	f := newFixture(t, resultStream)
	if status := f.execute(t, f.submit(t, f.host, withDeployKey), 1); status != kgo.AckAccept {
		t.Fatalf("status %v", status)
	}
	task := f.lastTask(t)
	if task.Status.State != wire.TaskStateAuthRequired || task.Status.Message.Parts[0].Text != "credentials "+credentialName+", deploy-key required" {
		t.Fatalf("ended %s with %q, want AUTH_REQUIRED naming both credentials", task.Status.State, task.Status.Message.Parts[0].Text)
	}
}

func (f fixture) outputChunks(t *testing.T) []wire.Output {
	t.Helper()
	var outputs []wire.Output
	for _, record := range f.records(t, admin.TenantTopic(testTenant)) {
		var output wire.Output
		if wire.ParseRecord(record.Value, f.keys(), &output) == nil {
			outputs = append(outputs, output)
		}
	}
	return outputs
}

// manifest decodes the outputs a result's task metadata lists.
func manifest(t *testing.T, task *wire.Task) wire.ResultMetadata {
	t.Helper()
	var listed wire.ResultMetadata
	if err := json.Unmarshal(task.Metadata, &listed); err != nil {
		t.Fatal(err)
	}
	return listed
}

func sha(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func TestTaskCredentialIsRedactedFromTranscriptAndOutputs(t *testing.T) {
	stream := []string{`{"type":"assistant","message":"key ` + deployValue + `"}`, `{"type":"result","result":"done","is_error":false}`}
	f := newFixture(t, stream)
	f.adapter.outputs = []adapter.Output{{Name: "report.md", Size: int64(len("key " + deployValue + "\n")), Data: []byte("key " + deployValue + "\n")}}
	done := f.executing(t, f.ctx, f.submit(t, f.host, withDeployKey), 1)
	f.sealFor(t, f.request(t, 1), map[string][]byte{credentialName: []byte(testToken), "deploy-key": []byte(deployValue)})
	if status := <-done; status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	if text := f.chunks(t)[0].ArtifactUpdate.Artifact.Parts[0].Text; text != `{"type":"assistant","message":"key [redacted]"}` {
		t.Fatalf("transcript line %q, want the task credential redacted", text)
	}
	outputs := f.outputChunks(t)
	want := []byte("key [redacted]\n")
	if len(outputs) != 1 || !bytes.Equal(outputs[0].ArtifactUpdate.Artifact.Parts[0].Raw, want) {
		t.Fatalf("output chunks %+v, want report.md redacted", outputs)
	}
	if listed := manifest(t, f.lastTask(t)).Outputs; len(listed) != 1 || listed[0].SHA256 != sha(want) || listed[0].Size != int64(len(want)) {
		t.Fatalf("manifest %+v, want the redacted bytes' hash and size", listed)
	}
}

func TestPreambleNamesFilesNeverValues(t *testing.T) {
	directories := adapter.Directories{Input: "/local/inputs", Output: "/local/outputs", Credentials: "/secrets/credentials"}
	inputs := []string{"data.csv"}
	credentials := []wire.TaskCredential{{Name: "deploy-key", File: "id_ed25519"}}
	outputs := []wire.ExpectedOutput{{Name: "report.md", Required: true}, {Name: "notes/extra.txt"}}
	got := preamble(directories, inputs, credentials, outputs, nil, "run-a")
	for _, want := range []string{"input directory is /local/inputs", "credentials directory /secrets/credentials", "output directory /local/outputs",
		"WHERRY_INPUT_DIR", "Input files: /local/inputs/data.csv.", "Credential files: /secrets/credentials/id_ed25519.",
		"/local/outputs/report.md (required), /local/outputs/notes/extra.txt."} {
		if !strings.Contains(got, want) {
			t.Errorf("preamble %q lacks %q", got, want)
		}
	}
	for _, never := range []string{"deploy-key"} {
		if strings.Contains(got, never) {
			t.Errorf("preamble %q holds %q", got, never)
		}
	}
	if unreported := preamble(adapter.Directories{}, inputs, nil, nil, nil, "run-a"); !strings.Contains(unreported, "Input files: $WHERRY_INPUT_DIR/data.csv.") {
		t.Errorf("preamble %q, want the variable for a directory the adapter does not report", unreported)
	}
	if preamble(directories, nil, nil, nil, nil, "run-a") != "" {
		t.Error("a run that declares no files has a preamble")
	}
}

// runWithOutputs runs a turn declaring expected through the fake adapter, which collects outputs or fails with
// collectErr, and returns its result.
func runWithOutputs(t *testing.T, expected []wire.ExpectedOutput, outputs []adapter.Output, collectErr error) (fixture, *wire.Task, kgo.AckStatus) {
	t.Helper()
	return runWithOutputsUnder(t, testTerms(), expected, outputs, collectErr)
}

// runWithOutputsUnder is runWithOutputs in a pool of terms.
func runWithOutputsUnder(t *testing.T, terms admin.Terms, expected []wire.ExpectedOutput, outputs []adapter.Output, collectErr error) (fixture, *wire.Task, kgo.AckStatus) {
	t.Helper()
	f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakeAdapter{"nomad": {stream: resultStream}})
	f.answer(t, credentialName)
	f.adapter.outputs, f.adapter.collectErr = outputs, collectErr
	status := f.execute(t, f.submit(t, f.host, func(task *wire.Submission) { task.Wherry.Outputs = expected }), 1)
	return f, f.lastTask(t), status
}

func file(name, data string) adapter.Output {
	return adapter.Output{Name: name, Size: int64(len(data)), Data: []byte(data)}
}

func TestCollectionRejectsWhatIsNotARegularFile(t *testing.T) {
	f, task, status := runWithOutputs(t, nil, []adapter.Output{
		{Name: "link", Size: 9, Mode: fs.ModeSymlink},
		{Name: "fifo", Mode: fs.ModeNamedPipe},
		{Name: "tty", Mode: fs.ModeDevice | fs.ModeCharDevice},
		{Name: "sock", Mode: fs.ModeSocket},
		{Name: "big.bin", Size: admin.DefaultOutputLimit + 1},
		{Name: "../escape", Size: 1, Data: []byte("x")},
		{Name: "short", Size: 5, Data: []byte("abc")},
		file("report.md", "# Report\n"),
	}, nil)
	if status != kgo.AckAccept || task.Status.State != wire.TaskStateCompleted {
		t.Fatalf("ended %v %s", status, task.Status.State)
	}
	listed := manifest(t, task)
	want := []wire.UnreturnedOutput{
		{Name: `"../escape"`, Size: 1, Reason: "rejected: invalid name"}, {Name: "big.bin", Size: admin.DefaultOutputLimit + 1, Reason: "exceeds output limit"},
		{Name: "fifo", Reason: "rejected: fifo"}, {Name: "link", Size: 9, Reason: "rejected: symlink"},
		{Name: "short", Size: 5, Reason: "rejected: size changed while read"}, {Name: "sock", Reason: "rejected: socket"}, {Name: "tty", Reason: "rejected: device"},
	}
	if !slices.Equal(listed.NotReturned, want) || len(listed.Outputs) != 1 || listed.Outputs[0].Name != "report.md" {
		t.Fatalf("returned %+v and not returned %+v, want report.md alone and %+v", listed.Outputs, listed.NotReturned, want)
	}
	if outputs := f.outputChunks(t); len(outputs) != 1 || outputs[0].ArtifactUpdate.Artifact.ArtifactID != "report.md" {
		t.Fatalf("output chunks %+v, want report.md alone", outputs)
	}
}

// A plugin that ignores the limits gets its outputs refused, not kept.
func TestCollectionRefusesAnAdapterPastTheLimits(t *testing.T) {
	many := make([]adapter.Output, wire.MaxOutputs+1)
	for i := range many {
		many[i] = file(fmt.Sprintf("f%03d", i), "x")
	}
	for name, outputs := range map[string][]adapter.Output{
		"count": many,
		"total": {file("a", strings.Repeat("a", admin.DefaultOutputLimit/2+1)), file("b", strings.Repeat("b", admin.DefaultOutputLimit/2+1))},
	} {
		t.Run(name, func(t *testing.T) {
			_, task, _ := runWithOutputs(t, []wire.ExpectedOutput{{Name: "a"}}, outputs, nil)
			if listed := manifest(t, task); len(listed.Outputs) != 0 || !slices.Equal(listed.NotReturned, []wire.UnreturnedOutput{{Name: "a", Reason: "not collected"}}) {
				t.Fatalf("returned %+v, not returned %+v; want nothing collected", listed.Outputs, listed.NotReturned)
			}
		})
	}
}

func TestCollectionHonoursThePoolOutputLimit(t *testing.T) {
	terms := testTerms()
	terms.OutputLimit = 1000
	f, task, _ := runWithOutputsUnder(t, terms, nil, []adapter.Output{{Name: "grew.bin", Size: 2000, Data: []byte("x")}, file("report.md", "r")}, nil)
	if !reflect.DeepEqual(f.adapter.limits, adapter.Limits{Files: wire.MaxOutputs, FileBytes: 1000, TotalBytes: 1000, StreamBytes: wire.MaxS3Object}) {
		t.Fatalf("collected under %+v, want the pool's output limit", f.adapter.limits)
	}
	if listed := manifest(t, task); len(listed.Outputs) != 1 || !slices.Equal(listed.NotReturned, []wire.UnreturnedOutput{{Name: "grew.bin", Size: 2000, Reason: "exceeds output limit"}}) {
		t.Fatalf("returned %+v, not returned %+v; want grew.bin over the limit", listed.Outputs, listed.NotReturned)
	}
	_, task, _ = runWithOutputsUnder(t, terms, []wire.ExpectedOutput{{Name: "a"}}, []adapter.Output{file("a", strings.Repeat("a", 600)), file("b", strings.Repeat("b", 600))}, nil)
	if listed := manifest(t, task); len(listed.Outputs) != 0 || !slices.Equal(listed.NotReturned, []wire.UnreturnedOutput{{Name: "a", Reason: "not collected"}}) {
		t.Fatalf("returned %+v, not returned %+v; want an adapter past the pool's limit refused", listed.Outputs, listed.NotReturned)
	}
}

// The streamed redaction replaces as bytes.ReplaceAll does, form by form, wherever the matches fall: the last input
// has a match that spans one form's replacement and the bytes after it.
func TestRedactionMatchesReplaceAll(t *testing.T) {
	f := newFixture(t, resultStream)
	run := testActiveRun(t, f, "run-r", 1)
	run.deliver(map[string][]byte{"aaaa": []byte("aaaaaaaa"), "quoted": []byte(`say "hi" now`), "inner-holder": []byte("xxaaaaaaaaxx"),
		"p": []byte("long-value-1234567"), "q": []byte("d]tail123")})
	for _, data := range []string{"", "aaaaaaa", strings.Repeat("a", 17), "xxaaaaaaaaxx" + strings.Repeat("a", 9) + `say \"hi\" now`, "b" + strings.Repeat("aaaaaaaab", 5), "zzzxxaaaaaaaaxxqq",
		"long-value-1234567tail123"} {
		want := []byte(data)
		for _, form := range run.redactions {
			want = bytes.ReplaceAll(want, form, run.marker)
		}
		got := run.redact([]byte(data), 1<<20)
		if !bytes.Equal(got.data, want) || got.size != int64(len(want)) || got.hash() != fmt.Sprintf("%x", sha256.Sum256(want)) {
			t.Fatalf("redact(%q) = %q, %d; want %q", data, got.data, got.size, want)
		}
	}
}

// The marker can be longer than a value, so redaction cannot take an output, the turn's outputs, a transcript line or
// a text past its limit.
func TestRedactionCannotExpandPastTheLimits(t *testing.T) {
	terms := testTerms()
	terms.OutputLimit = 4096
	f := newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakeAdapter{"nomad": {stream: resultStream}})
	name, value, marker := "c", "12345678", "[redacted]"
	collect := func(outputs ...adapter.Output) *activeRun {
		run := testActiveRun(t, f, "run-x", 1)
		run.deliver(map[string][]byte{name: []byte(value)})
		f.adapter.outputs = outputs
		run.collect(f.ctx, adapter.Handle{}, true)
		return run
	}
	run := collect(file("expanding.txt", strings.Repeat(value, 450)), file("small.txt", "key "+value))
	if want := []wire.UnreturnedOutput{{Name: "expanding.txt", Size: 3600, Reason: "exceeds output limit"}}; !slices.Equal(run.unreturned, want) ||
		len(run.returned) != 1 || run.returned[0].Size != int64(len("key "+marker)) {
		t.Fatalf("returned %+v, not returned %+v; want expanding.txt over the limit once redacted", run.returned, run.unreturned)
	}
	run = collect(file("a.txt", strings.Repeat("a", 3000)), file("b.txt", strings.Repeat(value, 120)), file("c.txt", "c"))
	if want := []wire.UnreturnedOutput{{Name: "b.txt", Size: 960, Reason: "exceeds output limit"}}; !slices.Equal(run.unreturned, want) || len(run.returned) != 2 {
		t.Fatalf("returned %+v, not returned %+v; want b.txt taking the redacted total over the limit", run.returned, run.unreturned)
	}
	line := []byte(strings.Repeat(value, 60000))
	redacted := bytes.ReplaceAll(line, []byte(value), []byte(marker))
	if form := run.redact(line, wire.MaxLine); !form.over() || form.data != nil {
		t.Fatalf("redacted form over %d bytes held %d bytes", wire.MaxLine, len(form.data))
	}
	var chunk wire.Chunk
	if err := wire.ParseRecord(run.chunks([]adapter.Line{{Data: line}}, false)[0].Value, f.keys(), &chunk); err != nil {
		t.Fatal(err)
	}
	var pointer wire.PointerMetadata
	if part := chunk.ArtifactUpdate.Artifact.Parts[0]; part.URL != "unstored:oversize" || json.Unmarshal(part.Metadata, &pointer) != nil ||
		pointer.Size != int64(len(redacted)) || pointer.SHA256 != fmt.Sprintf("%x", sha256.Sum256(redacted)) {
		t.Fatalf("transcript part %+v, want a pointer to the %d-byte redacted line", chunk.ArtifactUpdate.Artifact.Parts[0], len(redacted))
	}
	if text := run.redactText(string(line)); text != "[redacted text over 65536 bytes]" {
		t.Fatalf("text %d bytes, want the marker", len(text))
	}
}

// A line whose JSON escaping would outgrow base64 travels as raw bytes, so no line of wire.MaxLine bytes makes a
// larger chunk than its raw form.
func TestEscapeHeavyLineTravelsRaw(t *testing.T) {
	f := newFixture(t, resultStream)
	run := testActiveRun(t, f, "run-e", 1)
	escapes, plain := []byte(strings.Repeat("\x01", 300)), []byte(`{"type":"assistant","message":"<b>&"}`)
	records := run.chunks([]adapter.Line{{Data: escapes}, {Offset: 300, Data: plain}}, false)
	var parts []wire.Part
	for _, record := range records {
		var chunk wire.Chunk
		if err := wire.ParseRecord(record.Value, f.keys(), &chunk); err != nil {
			t.Fatal(err)
		}
		parts = append(parts, chunk.ArtifactUpdate.Artifact.Parts[0])
	}
	if !bytes.Equal(parts[0].Raw, escapes) || parts[0].Text != "" || parts[1].Text != string(plain) {
		t.Fatalf("parts %+v, want the escape-heavy line raw and the plain one as text", parts)
	}
}

type largestWrite int

func (w *largestWrite) Write(p []byte) (int, error) {
	*w = max(*w, largestWrite(len(p)))
	return len(p), nil
}

// However large the data, every write through the chain is bounded by a piece and the forms.
func TestRedactionWorksInBoundedPieces(t *testing.T) {
	f := newFixture(t, resultStream)
	run := testActiveRun(t, f, "run-p", 1)
	run.deliver(map[string][]byte{"x": []byte("12345678"), "y": []byte("abcdefghijkl")})
	var largest largestWrite
	run.redactTo(&largest, bytes.Repeat([]byte("z"), 4<<20))
	if bound := redactPiece + 8 + 12; largest == 0 || int(largest) > bound {
		t.Fatalf("largest write %d bytes, want at most %d", largest, bound)
	}
}

func TestOutputNameOver512BytesIsRejected(t *testing.T) {
	longest := strings.Repeat("a/", 255) + "aa"
	long := longest + "a"
	_, task, _ := runWithOutputs(t, nil, []adapter.Output{file(long, "x"), file(longest, "y")}, nil)
	listed := manifest(t, task)
	if quoted := strconv.QuoteToASCII(long)[:256]; len(listed.Outputs) != 1 || len(listed.Outputs[0].Name) != 512 ||
		!slices.Equal(listed.NotReturned, []wire.UnreturnedOutput{{Name: quoted, Size: 1, Reason: "rejected: invalid name"}}) {
		t.Fatalf("returned %+v, not returned %+v; want the 513-byte name rejected and the 512-byte one returned", listed.Outputs, listed.NotReturned)
	}
}

// The result travels within its record limit: its text is capped once redacted, and text that JSON escaping would
// lengthen past base64 travels raw.
func TestResultTextIsBounded(t *testing.T) {
	f := newFixture(t, resultStream)
	run := testActiveRun(t, f, "run-t", 1)
	part := func(text string) wire.Part {
		var result wire.Result
		if err := wire.ParseRecord(run.result(taskStatus(wire.TaskStateCompleted, ""), text, nil).Value, f.keys(), &result); err != nil {
			t.Fatal(err)
		}
		return result.Task.Artifacts[0].Parts[0]
	}
	if got := part(strings.Repeat("x", wire.MaxText+1)); got.Text != "[redacted text over 65536 bytes]" {
		t.Fatalf("result over the cap is %d bytes of text", len(got.Text))
	}
	escapes := strings.Repeat("\x01", 1000)
	if got := part(escapes); string(got.Raw) != escapes || got.MediaType != "text/plain" || got.Text != "" {
		t.Fatalf("escape-heavy result %+v, want it raw", got)
	}
	if got := part("done <b>"); got.Text != "done <b>" {
		t.Fatalf("plain result %+v, want text", got)
	}
}

// The largest result the receiver builds from bounded fields fits a default producer's batch.
func TestLargestResultFitsTheProducerBatch(t *testing.T) {
	f := newFixture(t, resultStream)
	run := testActiveRun(t, f, "run-w", 1)
	parts := []wire.Part{{Text: "p"}}
	for i := 1; i < wire.MaxParts; i++ {
		parts = append(parts, wire.Part{Raw: wire.StdBytes{byte(i)}, Filename: fmt.Sprintf("%0255d", i)})
	}
	run.history = []wire.Message{{MessageID: "msg-a", Role: wire.RoleUser, Parts: parts, Extensions: []string{wire.ExtensionURI}}}
	name := strings.Repeat("a/", 255) + "aa"
	for range wire.MaxOutputs {
		run.returned = append(run.returned, wire.ReturnedOutput{Name: name, SHA256: strings.Repeat("0", 64), Size: 1 << 62})
		run.unreturned = append(run.unreturned, wire.UnreturnedOutput{Name: name, Size: 1 << 62, Reason: "rejected: size changed while read"})
	}
	metadata := agentResultMetadata([]byte(`{"type":"result","usage":{"k":"`+strings.Repeat("x", wire.MaxAgentResult-40)+`"}}`), "usage")
	record := run.result(taskStatus(wire.TaskStateFailed, strings.Repeat("x", wire.MaxText)), strings.Repeat("\x01", wire.MaxText), metadata)
	size := len(record.Key) + len(record.Value) + 97
	t.Logf("largest result: %d-byte batch", size)
	if metadata == nil || size > 1000012 {
		t.Fatalf("largest result is a %d-byte batch, metadata %v", size, metadata != nil)
	}
}

func TestRequiredOutputMissingFailsTheTurn(t *testing.T) {
	f, task, status := runWithOutputs(t, []wire.ExpectedOutput{{Name: "report.md", Required: true}, {Name: "notes.md"}}, nil, nil)
	if status != kgo.AckReject || task.Status.State != wire.TaskStateFailed || task.Status.Message.Parts[0].Text != "required output report.md not returned: missing" {
		t.Fatalf("ended %v %s %+v, want FAILED naming report.md", status, task.Status.State, task.Status.Message)
	}
	want := []wire.UnreturnedOutput{{Name: "report.md", Reason: "missing"}, {Name: "notes.md", Reason: "missing"}}
	if listed := manifest(t, task); !slices.Equal(listed.NotReturned, want) {
		t.Fatalf("not returned %+v, want %+v", listed.NotReturned, want)
	}
	if f.adapter.collects != 1 || f.adapter.stopsBefore[0] < 1 {
		t.Fatalf("collected %d times after %v stops, want once after the job was stopped", f.adapter.collects, f.adapter.stopsBefore)
	}
}

func TestOptionalOutputMissingIsListed(t *testing.T) {
	_, task, status := runWithOutputs(t, []wire.ExpectedOutput{{Name: "report.md", Required: true}, {Name: "notes.md"}}, []adapter.Output{file("report.md", "r")}, nil)
	if status != kgo.AckAccept || task.Status.State != wire.TaskStateCompleted {
		t.Fatalf("ended %v %s, want COMPLETED", status, task.Status.State)
	}
	if listed := manifest(t, task); !slices.Equal(listed.NotReturned, []wire.UnreturnedOutput{{Name: "notes.md", Reason: "missing"}}) {
		t.Fatalf("not returned %+v, want notes.md missing", listed.NotReturned)
	}
}

func TestOutputsNotCollected(t *testing.T) {
	for name, test := range map[string]struct {
		required bool
		state    wire.TaskState
	}{"required": {true, wire.TaskStateFailed}, "optional": {false, wire.TaskStateCompleted}} {
		t.Run(name, func(t *testing.T) {
			_, task, _ := runWithOutputs(t, []wire.ExpectedOutput{{Name: "report.md", Required: test.required}}, []adapter.Output{file("report.md", "r")}, context.DeadlineExceeded)
			if task.Status.State != test.state {
				t.Fatalf("ended %s, want %s", task.Status.State, test.state)
			}
			if listed := manifest(t, task); len(listed.Outputs) != 0 || !slices.Equal(listed.NotReturned, []wire.UnreturnedOutput{{Name: "report.md", Reason: "not collected"}}) {
				t.Fatalf("returned %+v, not returned %+v", listed.Outputs, listed.NotReturned)
			}
		})
	}
}

func TestOutputChunksAreBoundedAndCommittedWithTheResult(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789abcdef"), (2*wire.OutputChunk+100)/16)
	f, task, _ := runWithOutputs(t, nil, []adapter.Output{{Name: "sub/big.bin", Size: int64(len(data)), Data: data}, file("empty", "")}, nil)
	outputs := f.outputChunks(t)
	type chunk struct {
		name         string
		index        int64
		append, last bool
		size         int
	}
	var got []chunk
	for _, output := range outputs {
		var metadata wire.OutputMetadata
		if err := json.Unmarshal(output.ArtifactUpdate.Metadata, &metadata); err != nil {
			t.Fatal(err)
		}
		update := output.ArtifactUpdate
		got = append(got, chunk{update.Artifact.ArtifactID, metadata.Index, update.Append, update.LastChunk, len(update.Artifact.Parts[0].Raw)})
	}
	want := []chunk{{"empty", 0, false, true, 0}, {"sub/big.bin", 0, false, false, wire.OutputChunk}, {"sub/big.bin", 1, true, false, wire.OutputChunk}, {"sub/big.bin", 2, true, true, len(data) - 2*wire.OutputChunk}}
	if !slices.Equal(got, want) {
		t.Fatalf("chunks %+v, want %+v", got, want)
	}
	if listed := manifest(t, task).Outputs; !slices.Equal(listed, []wire.ReturnedOutput{{Name: "empty", SHA256: sha(nil), Size: 0}, {Name: "sub/big.bin", SHA256: sha(data), Size: int64(len(data))}}) {
		t.Fatalf("manifest %+v", listed)
	}
	feed := f.records(t, admin.TenantTopic(testTenant))
	last := slices.IndexFunc(feed, func(record *kgo.Record) bool {
		var result wire.Result
		return wire.ParseRecord(record.Value, f.keys(), &result) == nil
	})
	if !oneTransaction(feed[last-len(outputs) : last+1]) {
		t.Fatal("the output chunks are not in the result's transaction")
	}
}

func TestInputTooLargeFailsTheTurnNamingIt(t *testing.T) {
	for name, test := range map[string]struct{ reported, message string }{
		"one of the run's inputs": {"data.csv", "input data.csv takes the turn's inputs over what adapter nomad delivers"},
		"a name the run lacks":    {"other\nname", "an input takes the turn's inputs over what adapter nomad delivers"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			f.answer(t, credentialName)
			f.adapter.dispatchErr, f.adapter.outcome = &adapter.InputTooLargeError{Name: test.reported}, adapter.OutcomeNothingCreated
			record := f.submit(t, f.host, func(task *wire.Submission) {
				task.Message.Parts = append(task.Message.Parts, wire.Part{Raw: wire.StdBytes("x"), Filename: "data.csv"})
			})
			if status := f.execute(t, record, 1); status != kgo.AckReject {
				t.Fatalf("status %v", status)
			}
			if task := f.lastTask(t); task.Status.State != wire.TaskStateFailed || task.Status.Message.Parts[0].Text != test.message {
				t.Fatalf("ended %s with %+v, want FAILED with %q", task.Status.State, task.Status.Message, test.message)
			}
			var failures []wire.StartFailure
			for _, record := range f.records(t, admin.PoolEventsTopic(testPool)) {
				var failure wire.StartFailure
				if wire.ParseRecord(record.Value, f.keys(), &failure) == nil {
					failures = append(failures, failure)
				}
			}
			if len(failures) != 1 || failures[0].Class != "input-too-large" || failures[0].Adapter != "nomad" {
				t.Fatalf("start failures %+v, want one input-too-large", failures)
			}
		})
	}
}

func TestAHungCollectionIsNotCollected(t *testing.T) {
	previous := collectTimeout
	collectTimeout = 200 * time.Millisecond
	t.Cleanup(func() { collectTimeout = previous })
	f := newFixture(t, resultStream)
	f.answer(t, credentialName)
	f.adapter.collectHangs = true
	status := f.execute(t, f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Outputs = []wire.ExpectedOutput{{Name: "report.md", Required: true}}
	}), 1)
	if task := f.lastTask(t); status != kgo.AckReject || task.Status.Message.Parts[0].Text != "required output report.md not returned: not collected" {
		t.Fatalf("ended %v %+v, want FAILED with the output not collected", status, task.Status.Message)
	}
}

// Credential files written by echo or a CLI end in a newline; a transcript carries the value JSON-escaped, and outputs
// and result text often without the newline.
func TestCredentialFormsAreRedacted(t *testing.T) {
	const agentValue, taskValue = "sk-token-with-newline\n", `pass"word\back-slash` + "\r\n"
	stream := []string{
		`{"type":"assistant","message":"cat gives sk-token-with-newline\n and pass\"word\\back-slash\r\n"}`,
		`{"type":"result","result":"used sk-token-with-newline\n","is_error":false,"usage":{"note":"pass\"word\\back-slash"}}`,
	}
	f := newFixture(t, stream)
	f.adapter.outputs = []adapter.Output{file("report.md", `token sk-token-with-newline, password pass"word\back-slash`)}
	done := f.executing(t, f.ctx, f.submit(t, f.host, withDeployKey), 1)
	f.sealFor(t, f.request(t, 1), map[string][]byte{credentialName: []byte(agentValue), "deploy-key": []byte(taskValue)})
	if status := <-done; status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	agent, task := "[redacted]", "[redacted]"
	if line := f.chunks(t)[0].ArtifactUpdate.Artifact.Parts[0].Text; line != `{"type":"assistant","message":"cat gives `+agent+`\n and `+task+`\r\n"}` {
		t.Fatalf("transcript line %q", line)
	}
	if data := string(f.outputChunks(t)[0].ArtifactUpdate.Artifact.Parts[0].Raw); data != "token "+agent+", password "+task {
		t.Fatalf("output %q", data)
	}
	result := f.lastTask(t).Artifacts[0]
	if text := result.Parts[0].Text; text != "used "+agent+"\n" {
		t.Fatalf("result %q", text)
	}
	if metadata := string(result.Metadata); strings.Contains(metadata, "back-slash") || !strings.Contains(metadata, task) {
		t.Fatalf("result metadata %s", metadata)
	}
}

// A job that would not stop at the delivery budget keeps the agent's result, and its outputs are never read.
func TestOutputsOfAJobThatWouldNotStopAreNotCollected(t *testing.T) {
	f := newFixture(t, resultStream)
	f.adapter.stopErr = errors.New("stop failed")
	f.adapter.outputs = []adapter.Output{file("report.md", "r")}
	run := testActiveRun(t, f, "run-unstopped", int64(testTerms().DeliveryLimit)-1)
	run.expected = []wire.ExpectedOutput{{Name: "report.md"}, {Name: "notes.md"}}
	result, _ := resultLine([]byte(resultStream[1]))
	if status := run.finish(f.ctx, f.adapter.handle("run-unstopped", 1), nil, result); status != kgo.AckAccept {
		t.Fatalf("status %v, want the result accepted", status)
	}
	want := []wire.UnreturnedOutput{{Name: "report.md", Reason: "not collected"}, {Name: "notes.md", Reason: "not collected"}}
	if listed := manifest(t, f.lastTask(t)); len(listed.Outputs) != 0 || !slices.Equal(listed.NotReturned, want) || f.adapter.collects != 0 {
		t.Fatalf("returned %+v, not returned %+v after %d collects; want every output not collected and none", listed.Outputs, listed.NotReturned, f.adapter.collects)
	}
}

func TestAContinuationRepeatingAnInputNameIsRejected(t *testing.T) {
	f := newFixture(t, resultStream)
	withInput := func(task *wire.Submission) {
		task.Message.Parts = append(task.Message.Parts, wire.Part{Raw: wire.StdBytes("x"), Filename: "data.csv"})
	}
	first := f.submit(t, f.host, withInput)
	f.publishStatus(t, first, testReceiver, f.receiver.signing, "interrupted", wire.TaskStateAuthRequired, nil)
	second := f.submit(t, f.host, func(task *wire.Submission) {
		withInput(task)
		task.Wherry.Turn, task.Message.MessageID = 2, "msg-2"
	})
	if admitted, ack := f.receiver.admit(f.ctx, second); admitted != nil || ack != kgo.AckReject {
		t.Fatalf("admitted %v with ack %v, want a rejection", admitted, ack)
	}
	if statuses := f.feedStatuses(t); statuses[len(statuses)-1].StatusUpdate.Status.Message.Parts[0].Text != "input names repeat across turns" {
		t.Fatalf("status %+v", statuses[len(statuses)-1].StatusUpdate.Status)
	}
}
