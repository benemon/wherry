package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/wire"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestSubmitFileFlags(t *testing.T) {
	b := newBus(t)
	deployKey := filepath.Join(t.TempDir(), "deploy-key")
	if err := os.WriteFile(deployKey, []byte("deploy-key-value"), 0o600); err != nil {
		t.Fatal(err)
	}
	b.credentials["deploy-key"], b.credentials["vault-token"] = credential.Source{File: deployKey}, credential.Source{File: deployKey}
	input := filepath.Join(t.TempDir(), "data.csv")
	if err := os.WriteFile(input, []byte("a,b\n1,2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	large := filepath.Join(t.TempDir(), "large.bin")
	if err := os.WriteFile(large, make([]byte, 49153), 0o600); err != nil {
		t.Fatal(err)
	}
	base := []string{"--prompt", "summarise the data"}
	for name, test := range map[string]struct {
		flags []string
		want  string
	}{
		"request outside the agent":  {[]string{"--credential", "deploy-key"}, "requested credential deploy-key is not one of agent claude's credentials"},
		"credential of another kind": {[]string{"--credential", "deploy-key:path=id"}, "want NAME, NAME:file=FILE or NAME:env=VAR"},
		"unconfigured credential":    {[]string{"--credential", "absent:file=token"}, "auth required: credential absent is not in this host's configuration"},
		"agent credential target":    {[]string{"--credential", "deploy-key:env=MODEL_API_KEY"}, "task credential deploy-key takes the environment variable MODEL_API_KEY of one of agent claude's credentials"},
		"reserved env target":        {[]string{"--credential", "deploy-key:env=HOME"}, "credential deploy-key: env must match"},
		"shared target":              {[]string{"--credential", "deploy-key:env=TOKEN", "--credential", "vault-token:env=TOKEN"}, "credentials deploy-key and vault-token share a target"},
		"inputs over the limit":      {[]string{"--input", large}, "inline inputs are over profile lab's 49152-byte limit"},
		"output escaping":            {[]string{"--output", "../report.md"}, "expected outputs must be unique relative paths of portable file names"},
		"output in the outcome dir":  {[]string{"--output", ".wherry/outcome.json"}, "expected outputs must be unique relative paths of portable file names"},
		"credential file escaping":   {[]string{"--credential", "deploy-key:file=../id"}, "credential deploy-key: file must be a portable file name"},
		"unknown output suffix":      {[]string{"--output", "report.md:needed"}, "want NAME, not"},
		"s3 output suffix":           {[]string{"--output", "report.md:s3"}, "outputs move to S3 by size; drop :s3"},
		"required output suffix":     {[]string{"--output", "report.md:required"}, "outputs are always required; drop :required"},
		"s3 input suffix":            {[]string{"--input", input + ":s3"}, "inputs move to S3 by size; drop :s3"},
	} {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := runSubmit(b.ctx, b.client, append(slices.Clone(base), test.flags...), &stdout, &stderr)
			if err == nil || !strings.Contains(err.Error()+stderr.String(), test.want) || strings.Contains(err.Error(), "deploy-key-value") {
				t.Fatalf("submit error %v, stderr %q; want %q", err, stderr.String(), test.want)
			}
		})
	}
	var stdout, stderr bytes.Buffer
	flags := append(slices.Clone(base), "--input", input, "--credential", "deploy-key:file=id_ed25519", "--credential", "vault-token:env=VAULT_TOKEN",
		"--output", "report.md", "--output", "notes/extra.txt")
	if err := runSubmit(b.ctx, b.client, flags, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	runID, _, _ := strings.Cut(strings.TrimPrefix(stdout.String(), "run_id="), " ")
	submission := b.tasks(t, runID)[0].submission
	parts := submission.Message.Parts
	if len(parts) != 2 || parts[1].Filename != "data.csv" || string(parts[1].Raw) != "a,b\n1,2\n" {
		t.Fatalf("submitted parts %#v", parts)
	}
	if !slices.Equal(submission.Wherry.Credentials, []wire.TaskCredential{{Name: "deploy-key", File: "id_ed25519"}, {Name: "vault-token", Env: "VAULT_TOKEN"}}) ||
		!slices.Equal(submission.Wherry.Outputs, []wire.ExpectedOutput{{Name: "report.md"}, {Name: "notes/extra.txt"}}) {
		t.Fatalf("submitted credentials %+v and outputs %+v", submission.Wherry.Credentials, submission.Wherry.Outputs)
	}
	if strings.Contains(stdout.String()+stderr.String(), "deploy-key-value") {
		t.Fatal("submit printed the credential")
	}
}

// --credential NAME names an optional credential of the agent the task cannot go without, beside task credentials with
// targets; the submit line names the agent's optional credentials this host could not resolve, and an optional
// credential with a source is supplied without being named.
func TestSubmitSuppliesAgentCredentials(t *testing.T) {
	b := newBus(t)
	b.credentials["deploy-key"] = b.credentials["model"]
	submit := func(flags ...string) (string, wire.SubmissionBinding) {
		t.Helper()
		var stdout, stderr bytes.Buffer
		if err := runSubmit(b.ctx, b.client, append([]string{"--prompt", "work"}, flags...), &stdout, &stderr); err != nil {
			t.Fatal(err)
		}
		runID, _, _ := strings.Cut(strings.TrimPrefix(stdout.String(), "run_id="), " ")
		return stdout.String(), b.tasks(t, runID)[0].submission.Wherry
	}
	line, binding := submit("--credential", "deploy-key:env=DEPLOY_KEY")
	if !strings.HasSuffix(line, " pool=pool-a unsupplied=github-token\n") || !slices.Equal(binding.Supplied, []string{"model"}) {
		t.Fatalf("submit printed %q with supplied %v; want github-token unsupplied and model alone supplied", line, binding.Supplied)
	}
	var stderr bytes.Buffer
	if err := runSubmit(b.ctx, b.client, []string{"--prompt", "work", "--credential", "github-token"}, &bytes.Buffer{}, &stderr); err == nil || !strings.Contains(err.Error(), "auth required: credential github-token is not in this host's configuration") {
		t.Fatalf("submit naming an unresolvable optional credential = %v; want it refused", err)
	}
	b.credentials["github-token"] = b.credentials["model"]
	line, binding = submit("--credential", "deploy-key:env=DEPLOY_KEY")
	if !strings.HasSuffix(line, " pool=pool-a\n") || !slices.Equal(binding.Supplied, []string{"model", "github-token"}) || !slices.Equal(binding.Credentials, []wire.TaskCredential{{Name: "deploy-key", Env: "DEPLOY_KEY"}}) {
		t.Fatalf("submit printed %q with supplied %v and task credentials %+v; want the optional credential supplied unnamed and the task credential alone", line, binding.Supplied, binding.Credentials)
	}
	line, binding = submit("--credential", "github-token", "--credential", "model")
	if !strings.HasSuffix(line, " pool=pool-a\n") || !slices.Equal(binding.Supplied, []string{"model", "github-token"}) {
		t.Fatalf("submit printed %q with supplied %v; want both named credentials supplied", line, binding.Supplied)
	}
}

// outputResult commits an output's one chunk and a result listing it, as the receiver's end does.
func (b *bus) outputResult(t *testing.T, admitted task, name, data, listedHash string) {
	t.Helper()
	run := admitted.submission.Wherry.Run
	metadata, _ := json.Marshal(wire.OutputMetadata{Index: 0})
	chunk := b.sign(t, &wire.Output{
		ArtifactUpdate: wire.TaskArtifactUpdateEvent{TaskID: run, ContextID: run, LastChunk: true, Metadata: metadata,
			Artifact: wire.Artifact{ArtifactID: name, Name: name, Parts: []wire.Part{{Raw: wire.StdBytes(data), Filename: path.Base(name), MediaType: wire.OutputMediaType}}, Extensions: []string{wire.ExtensionURI}}},
		Wherry: wire.ResultBinding{ReceiverBinding: b.binding(admitted), TaskHash: admitted.hash},
	})
	manifest, _ := json.Marshal(map[string]any{"receiver_id": "receiver-a", "pool": "pool-a",
		"outputs":      []wire.ReturnedOutput{{Name: name, SHA256: listedHash, Size: int64(len(data))}},
		"not_returned": []wire.UnreturnedOutput{{Name: "notes.md", Reason: "missing"}}})
	status := now(wire.TaskStateCompleted)
	status.Message = &wire.Message{MessageID: "msg-status", Role: wire.RoleAgent, Parts: []wire.Part{{Text: "done"}}, Extensions: []string{wire.ExtensionURI}}
	result := &wire.Result{Task: wire.Task{ID: run, ContextID: run, History: []wire.Message{admitted.submission.Message}, Status: status,
		Artifacts: []wire.Artifact{{ArtifactID: "result", Parts: []wire.Part{{Text: "done"}}, Extensions: []string{wire.ExtensionURI}}}, Metadata: manifest},
		Wherry: wire.ResultBinding{ReceiverBinding: b.binding(admitted), TaskHash: admitted.hash}}
	b.feed(t, run, chunk, b.sign(t, result), b.status(t, admitted, "terminal-1", "terminal", now(wire.TaskStateCompleted)))
}

func hashOf(data string) string {
	sum := sha256.Sum256([]byte(data))
	return hex.EncodeToString(sum[:])
}

func TestWaitWritesVerifiedOutputs(t *testing.T) {
	b := newBus(t)
	admitted := b.admitted(t)
	runID := admitted.submission.Wherry.Run
	b.outputResult(t, admitted, "sub/report.md", "# Report\n", hashOf("# Report\n"))
	dir := t.TempDir()
	var stdout, stderr bytes.Buffer
	if err := runWait(b.ctx, b.client, []string{runID, "--outputs-dir", dir}, &stdout, &stderr); err != nil {
		t.Fatalf("wait: %v; stderr %s", err, stderr.String())
	}
	path := filepath.Join(dir, "sub", "report.md")
	if want := "output=sub/report.md path=" + path + " sha256=" + hashOf("# Report\n") + " size=9\n"; !strings.Contains(stdout.String(), want) {
		t.Fatalf("stdout %q, want %q", stdout.String(), want)
	}
	if written, err := os.ReadFile(path); err != nil || string(written) != "# Report\n" {
		t.Fatalf("written %q, %v", written, err)
	}
	if !strings.Contains(stderr.String(), "output notes.md not returned: missing") {
		t.Fatalf("stderr %q", stderr.String())
	}
}

// An expected output may be named transcript.jsonl, which wait writes as the agent left it.
func TestOutputMayBeNamedTranscript(t *testing.T) {
	b := newBus(t)
	var stdout, stderr bytes.Buffer
	if err := runSubmit(b.ctx, b.client, []string{"--prompt", "work", "--output", "transcript.jsonl"}, &stdout, &stderr); err != nil {
		t.Fatalf("submit: %v", err)
	}
	runID, _, _ := strings.Cut(strings.TrimPrefix(stdout.String(), "run_id="), " ")
	if outputs := b.tasks(t, runID)[0].submission.Wherry.Outputs; !slices.Equal(outputs, []wire.ExpectedOutput{{Name: "transcript.jsonl"}}) {
		t.Fatalf("submitted outputs %+v", outputs)
	}
	admitted := b.admitted(t)
	b.outputResult(t, admitted, "transcript.jsonl", "the agent's own\n", hashOf("the agent's own\n"))
	dir := t.TempDir()
	stdout.Reset()
	if err := runWait(b.ctx, b.client, []string{admitted.submission.Wherry.Run, "--outputs-dir", dir}, &stdout, &stderr); err != nil {
		t.Fatalf("wait: %v; stderr %s", err, stderr.String())
	}
	if written, err := os.ReadFile(filepath.Join(dir, "transcript.jsonl")); err != nil || string(written) != "the agent's own\n" {
		t.Fatalf("transcript.jsonl %q, %v; want the agent's output", written, err)
	}
}

func TestWaitFailsOnAnOutputItCannotVerify(t *testing.T) {
	b := newBus(t)
	admitted := b.admitted(t)
	runID := admitted.submission.Wherry.Run
	b.outputResult(t, admitted, "report.md", "# Report\n", hashOf("# Forged\n"))
	var stdout, stderr bytes.Buffer
	err := runWait(b.ctx, b.client, []string{runID}, &stdout, &stderr)
	if err == nil || !strings.Contains(stderr.String(), "output report.md not returned: rejected by this host: size or hash mismatch") {
		t.Fatalf("wait = %v, stderr %q; want the output refused", err, stderr.String())
	}
	if _, statErr := os.Stat(filepath.Join(b.stateDir, "runs", runID, "outputs", "report.md")); statErr == nil {
		t.Fatal("wrote an output that failed verification")
	}
}

func TestMCPSubmitCarriesFilesAndWaitWritesOutputs(t *testing.T) {
	b := newBus(t)
	b.credentials["deploy-key"] = b.credentials["model"]
	work := t.TempDir()
	session := mcpSessionReading(t, b.connect(t), nil, inputOpener(work, busclient.Config{}))
	input := filepath.Join(work, "data.csv")
	if err := os.WriteFile(input, []byte("a,b\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	arguments := map[string]any{"prompt": "summarise", "inputs": []map[string]any{{"path": input}},
		"credentials": []map[string]any{{"name": "deploy-key", "file": "id_ed25519"}}, "outputs": []map[string]any{{"name": "report.md"}}}
	var submitted submitOutput
	callTool(t, b.ctx, session, "submit", arguments, &submitted)
	admitted := b.tasks(t, submitted.ID)[0]
	if submission := admitted.submission; len(submission.Message.Parts) != 2 || submission.Message.Parts[1].Filename != "data.csv" ||
		len(submission.Wherry.Credentials) != 1 || !slices.Equal(submission.Wherry.Outputs, []wire.ExpectedOutput{{Name: "report.md"}}) {
		t.Fatalf("submission %+v", submission)
	}
	if failure := callToolError(t, b.ctx, session, "submit", map[string]any{"prompt": "x", "inputs": []map[string]any{{"path": filepath.Join(t.TempDir(), "absent")}}}); failure.Code != "INVALID_ARGUMENT" {
		t.Fatalf("submit with an unreadable input: %+v", failure)
	}
	b.feed(t, submitted.ID, b.echo(t, admitted), b.status(t, admitted, "claimed-1", "claimed", now(wire.TaskStateWorking)))
	b.outputResult(t, admitted, "report.md", "# Report\n", hashOf("# Report\n"))
	var reply waitReply
	callTool(t, b.ctx, session, "result", map[string]any{"id": submitted.ID}, &reply)
	path := filepath.Join(b.stateDir, "runs", submitted.ID, "outputs", "report.md")
	if !slices.Equal(reply.Outputs, []outputReply{{Name: "report.md", Path: path, SHA256: hashOf("# Report\n"), Size: 9}}) ||
		!slices.Equal(reply.NotReturned, []wire.UnreturnedOutput{{Name: "notes.md", Reason: "missing"}}) {
		t.Fatalf("wait outputs %+v, not returned %+v", reply.Outputs, reply.NotReturned)
	}
	if written, err := os.ReadFile(path); err != nil || string(written) != "# Report\n" {
		t.Fatalf("written %q, %v", written, err)
	}
}

func TestMCPInputsStayInTheWorkingDirectory(t *testing.T) {
	work, outside := t.TempDir(), t.TempDir()
	write := func(path string) string {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("content"), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	link := func(target, name string) string {
		t.Helper()
		path := filepath.Join(work, name)
		if err := os.Symlink(target, path); err != nil {
			t.Fatal(err)
		}
		return path
	}
	config := busclient.Config{StateDir: filepath.Join(work, "state"), HostKey: write(filepath.Join(work, "host.key")), Bundle: write(filepath.Join(outside, "bundle.yaml")),
		Credentials: map[string]credential.Source{"model": {File: write(filepath.Join(work, "secrets", "model"))}, "github-token": {Env: "GITHUB_TOKEN"},
			"s3-access": {File: write(filepath.Join(work, "s3", "access.key"))}, "s3-secret": {File: write(filepath.Join(work, "s3", "secret.key"))}},
		Storage: map[string]busclient.StorageConfig{"pool-a": {AccessKey: "s3-access", SecretKey: "s3-secret"}}}
	write(filepath.Join(config.StateDir, "runs", "run-a.json"))
	read := inputReader(inputOpener(work, config).file)
	for name, path := range map[string]string{
		"outside":                filepath.Join(outside, "bundle.yaml"),
		"a link out":             link(write(filepath.Join(outside, "elsewhere")), "elsewhere"),
		"a link to the bundle":   link(config.Bundle, "bundle"),
		"the host key":           "host.key",
		"a link to the host key": link(config.HostKey, "key"),
		"the state":              filepath.Join(config.StateDir, "runs", "run-a.json"),
		"a credential":           config.Credentials["model"].File,
		"a storage access key":   config.Credentials["s3-access"].File,
		"a storage secret key":   config.Credentials["s3-secret"].File,
		"a directory":            filepath.Join(work, "secrets"),
		"a missing file":         filepath.Join(work, "absent"),
		"a parent path":          filepath.Join(work, "..", filepath.Base(outside), "bundle.yaml"),
	} {
		if _, err := read(path, math.MaxInt64); err == nil {
			t.Errorf("read %s (%s)", name, path)
		}
	}
	fifo := filepath.Join(work, "fifo")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	refused := make(chan error, 1)
	go func() { _, err := read(fifo, math.MaxInt64); refused <- err }()
	select {
	case err := <-refused:
		if err == nil {
			t.Error("read a FIFO")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reading a FIFO blocked")
	}
	part, err := read(link(write(filepath.Join(work, "sub", "data.csv")), "data.csv"), math.MaxInt64)
	if err != nil || part.Filename != "data.csv" || string(part.Raw) != "content" {
		t.Fatalf("read a file beneath the working directory: %+v, %v", part, err)
	}
	if part, err := read("sub/data.csv", math.MaxInt64); err != nil || string(part.Raw) != "content" {
		t.Fatalf("read a relative path: %+v, %v", part, err)
	}
	if part, err := read("sub/data.csv", 3); err != nil || string(part.Raw) != "con" {
		t.Fatalf("read with a limit: %+v, %v", part, err)
	}
}

func TestMCPInputsStayInTheInputRoots(t *testing.T) {
	dirs := make([]string, 4)
	for i := range dirs {
		dir, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		dirs[i] = dir
	}
	work, first, second, outside := dirs[0], dirs[1], dirs[2], dirs[3]
	write := func(path string) string {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("content"), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	config := busclient.Config{InputRoots: []string{first, second}, StateDir: filepath.Join(second, "state"),
		HostKey: write(filepath.Join(first, "wherry", "host.key")), Bundle: write(filepath.Join(outside, "bundle.yaml")),
		Credentials: map[string]credential.Source{"model": {File: write(filepath.Join(first, "wherry", "model"))}}}
	write(filepath.Join(config.StateDir, "runs", "run-a.json"))
	link := filepath.Join(first, "out")
	if err := os.Symlink(write(filepath.Join(outside, "elsewhere")), link); err != nil {
		t.Fatal(err)
	}
	hardLink := func(target, name string) string {
		t.Helper()
		path := filepath.Join(first, name)
		if err := os.Link(target, path); err != nil {
			t.Fatal(err)
		}
		return path
	}
	read := inputReader(inputOpener(work, config).file)
	if part, err := read(write(filepath.Join(second, "sub", "data.csv")), math.MaxInt64); err != nil || string(part.Raw) != "content" {
		t.Fatalf("read a file beneath the second root: %+v, %v", part, err)
	}
	refused := map[string]string{
		"outside every root":          filepath.Join(outside, "elsewhere"),
		"the working directory":       write(filepath.Join(work, "local")),
		"the state in a root":         filepath.Join(config.StateDir, "runs", "run-a.json"),
		"a link out of a root":        link,
		"a hard link to the host key": hardLink(config.HostKey, "key"),
		"a hard link to the bundle":   hardLink(config.Bundle, "bundle"),
	}
	if _, err := os.Stat(strings.ToUpper(config.HostKey)); err == nil {
		upper := func(root, path string) string {
			relative, err := filepath.Rel(root, path)
			if err != nil {
				t.Fatal(err)
			}
			return filepath.Join(root, strings.ToUpper(relative))
		}
		refused["the host key, upper-cased"] = upper(first, config.HostKey)
		refused["a credential, upper-cased"] = upper(first, config.Credentials["model"].File)
		refused["the state, upper-cased"] = upper(second, filepath.Join(config.StateDir, "runs", "run-a.json"))
	} else {
		t.Log("the temporary directory is case-sensitive; case-varied paths are not exercised")
	}
	for name, path := range refused {
		if _, err := read(path, math.MaxInt64); err == nil {
			t.Errorf("read %s (%s)", name, path)
		}
	}
}

// A directory swapped for a link out of the root after the input's path resolves is refused at the open.
func TestMCPInputsStayInTheRootAfterResolution(t *testing.T) {
	work, outside := t.TempDir(), t.TempDir()
	for _, dir := range []string{work, outside} {
		if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "sub", "data.csv"), []byte(dir), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	read := inputReader(inputOpener(work, busclient.Config{}).file)
	previous := resolveInput
	t.Cleanup(func() { resolveInput = previous })
	resolveInput = func(path string) (string, error) {
		resolved, err := previous(path)
		if err == nil {
			err = errors.Join(os.Rename(filepath.Join(work, "sub"), filepath.Join(work, "moved")), os.Symlink(filepath.Join(outside, "sub"), filepath.Join(work, "sub")))
		}
		return resolved, err
	}
	if part, err := read(filepath.Join(work, "sub", "data.csv"), math.MaxInt64); err == nil {
		t.Fatalf("read %q through a link swapped in after resolution", part.Raw)
	}
}

// os.OpenRoot follows a link in the root's own name: a root that is a link to a directory confines inputs to that
// directory, named through the link or the directory.
func TestMCPInputRootMayBeALink(t *testing.T) {
	target, links := t.TempDir(), t.TempDir()
	root := filepath.Join(links, "root")
	if err := os.Symlink(target, root); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "data.csv"), []byte("content"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(links, "beside"), []byte("content"), 0o600); err != nil {
		t.Fatal(err)
	}
	read := inputReader(inputOpener(t.TempDir(), busclient.Config{InputRoots: []string{root}}).file)
	for _, path := range []string{filepath.Join(root, "data.csv"), filepath.Join(target, "data.csv")} {
		if part, err := read(path, math.MaxInt64); err != nil || string(part.Raw) != "content" {
			t.Fatalf("read %s beneath a linked root: %+v, %v", path, part, err)
		}
	}
	if _, err := read(filepath.Join(root, "..", "beside"), math.MaxInt64); err == nil {
		t.Fatal("read beside the linked root")
	}
}

// writeFiles writes each file, named by its path relative to dir, with the path as its content.
func writeFiles(t *testing.T, dir string, files ...string) {
	t.Helper()
	for _, file := range files {
		path := filepath.Join(dir, filepath.FromSlash(file))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(file), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// inputPart is what the signed submission carries of one input: its name, size and SHA-256, inline or through S3.
type inputPart struct {
	name, sha256 string
	size         int64
}

// inputParts lists the inputs a submission carries, after its prompt, in order.
func inputParts(submission wire.Submission) []inputPart {
	var parts []inputPart
	for _, part := range submission.Message.Parts[1:] {
		parts = append(parts, inputPart{part.Filename, hashOf(string(part.Raw)), int64(len(part.Raw))})
	}
	for _, input := range submission.Wherry.S3Inputs {
		parts = append(parts, inputPart{input.Name, input.SHA256, input.Size})
	}
	return parts
}

// --input DIR sends every file beneath the directory as <base>/<relative path>, the base its name with a trailing slash
// removed or the working directory's for ., skipping hidden entries and saying how many; a symbolic link or a FIFO
// beneath it, the root directory, a name repeated across two flags, a name under another and a name under previous/
// are refused.
func TestSubmitInputDirectory(t *testing.T) {
	b := newBus(t)
	work := t.TempDir()
	t.Chdir(work)
	writeFiles(t, work, "docs/readme.md", "docs/guide/a.md", "docs/.hidden", "docs/.git/config", "other/docs", "other/notes.md", "previous/a.md", "linked/a.md", "piped/a.md")
	if err := os.Symlink(filepath.Join(work, "docs", "readme.md"), filepath.Join(work, "linked", "link")); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(work, "piped", "fifo"), 0o600); err != nil {
		t.Fatal(err)
	}
	submit := func(t *testing.T, flags ...string) (wire.Submission, string) {
		t.Helper()
		var stdout, stderr bytes.Buffer
		if err := runSubmit(b.ctx, b.client, append([]string{"--prompt", "read"}, flags...), &stdout, &stderr); err != nil {
			t.Fatalf("submit %v: %v", flags, err)
		}
		return b.tasks(t, submittedID(stdout.String()))[0].submission, stderr.String()
	}
	submission, stderr := submit(t, "--input", "docs/")
	want := []inputPart{{"docs/guide/a.md", hashOf("docs/guide/a.md"), 15}, {"docs/readme.md", hashOf("docs/readme.md"), 14}}
	if parts := inputParts(submission); !slices.Equal(parts, want) || stderr != "skipped 2 hidden entries under docs/\n" {
		t.Fatalf("parts %+v and stderr %q; want %+v and the hidden entries counted", parts, stderr, want)
	}
	if submission, _ := submit(t, "--input", filepath.Join(work, "docs"), "--input", "other/notes.md"); !slices.Equal(inputParts(submission), append(want, inputPart{"notes.md", hashOf("other/notes.md"), 14})) {
		t.Fatalf("parts %+v, want the directory's files beside the file", inputParts(submission))
	}
	t.Chdir(filepath.Join(work, "docs"))
	if submission, _ := submit(t, "--input", "."); !slices.Equal(inputParts(submission), want) {
		t.Fatalf("parts %+v of --input ., want the working directory's name as the base", inputParts(submission))
	}
	t.Chdir(work)
	for name, test := range map[string]struct {
		flags []string
		want  string
	}{
		"the root directory":              {[]string{"--input", "/"}, "--input /: the root directory has no name to deliver its files under"},
		"a link beneath it":               {[]string{"--input", "linked"}, "--input linked: linked/link is not a regular file"},
		"a FIFO beneath it":               {[]string{"--input", "piped"}, "--input piped: piped/fifo is not a regular file"},
		"an empty directory":              {[]string{"--input", t.TempDir()}, "no file to send beneath it"},
		"a name repeated across flags":    {[]string{"--input", "docs", "--input", "docs/"}, "input docs/guide/a.md is named twice"},
		"a name under another":            {[]string{"--input", "other/docs", "--input", "docs"}, "lies under input docs"},
		"a name under previous/":          {[]string{"--input", "previous"}, "input previous/a.md: names under previous/ are reserved for a continued run's outputs"},
		"a file and the previous/ prefix": {[]string{"--input", "previous/a.md", "--input", "previous"}, "names under previous/ are reserved"},
	} {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := runSubmit(b.ctx, b.client, append([]string{"--prompt", "read"}, test.flags...), &stdout, &stderr)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("submit %v = %v, want %q", test.flags, err, test.want)
			}
		})
	}
}

// The MCP submit tool's inputs take a directory beneath the input roots, walked as --input DIR is, with the hidden
// entries it skipped counted; a directory holding one of the host's own files refuses the submission, as do an existing
// directory outside the roots and the state directory.
func TestMCPSubmitInputDirectory(t *testing.T) {
	b := newBus(t)
	work, outside := t.TempDir(), t.TempDir()
	writeFiles(t, work, "docs/readme.md", "docs/guide/a.md", "docs/.hidden", "conf/notes.md", "state/runs/run-a.json")
	writeFiles(t, outside, "readme.md")
	bundle := filepath.Join(work, "conf", "bundle.yaml")
	if err := os.WriteFile(bundle, []byte("broker: b\nprincipal: p\npassword: s3cr3t\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	session := mcpSessionReading(t, b.connect(t), nil, inputOpener(work, busclient.Config{Bundle: bundle, StateDir: filepath.Join(work, "state")}))
	var submitted submitOutput
	callTool(t, b.ctx, session, "submit", map[string]any{"prompt": "read", "inputs": []map[string]any{{"path": "docs"}}}, &submitted)
	want := []inputPart{{"docs/guide/a.md", hashOf("docs/guide/a.md"), 15}, {"docs/readme.md", hashOf("docs/readme.md"), 14}}
	if parts := inputParts(b.tasks(t, submitted.ID)[0].submission); !slices.Equal(parts, want) || submitted.Skipped != 1 {
		t.Fatalf("parts %+v with %d hidden entries skipped; want %+v and 1", parts, submitted.Skipped, want)
	}
	const refused = "inputs must be readable regular files beneath one of the MCP server's input roots, outside the wherry configuration"
	for name, path := range map[string]string{"a directory holding the bundle": "conf", "a directory outside the roots": outside, "the state directory": filepath.Join(work, "state"), "a directory in the state": filepath.Join(work, "state", "runs")} {
		failure := callToolError(t, b.ctx, session, "submit", map[string]any{"prompt": "read", "inputs": []map[string]any{{"path": path}}})
		if failure.Code != "INVALID_ARGUMENT" || failure.Message != refused {
			t.Errorf("%s: %+v; want INVALID_ARGUMENT %q", name, failure, refused)
		}
		if strings.Contains(failure.Message, "s3cr3t") {
			t.Errorf("%s: the error holds the bundle's password", name)
		}
	}
	failure := callToolError(t, b.ctx, session, "submit", map[string]any{"prompt": "read", "inputs": []map[string]any{{"path": "docs"}, {"path": filepath.Join(work, "docs")}}})
	if failure.Code != "INVALID_ARGUMENT" || !strings.Contains(failure.Message, "input docs/guide/a.md is named twice") {
		t.Fatalf("a directory given twice: %+v", failure)
	}
}

// plainRunCollected submits a run without git, has receiver-a return the outputs named with their names as content, and
// collects them with wherry result into dir, by default the state directory.
func (b *bus) plainRunCollected(t *testing.T, dir string, names ...string) string {
	t.Helper()
	admitted := b.admitted(t)
	run := admitted.submission.Wherry.Run
	var outputs []wire.ReturnedOutput
	var chunks [][]byte
	metadata, _ := json.Marshal(wire.OutputMetadata{Index: 0})
	for _, name := range names {
		outputs = append(outputs, wire.ReturnedOutput{Name: name, SHA256: hashOf(name), Size: int64(len(name))})
		chunks = append(chunks, b.sign(t, &wire.Output{
			ArtifactUpdate: wire.TaskArtifactUpdateEvent{TaskID: run, ContextID: run, LastChunk: true, Metadata: metadata,
				Artifact: wire.Artifact{ArtifactID: name, Name: name, Parts: []wire.Part{{Raw: wire.StdBytes(name), Filename: path.Base(name), MediaType: wire.OutputMediaType}}, Extensions: []string{wire.ExtensionURI}}},
			Wherry: wire.ResultBinding{ReceiverBinding: b.binding(admitted), TaskHash: admitted.hash},
		}))
	}
	manifest, _ := json.Marshal(map[string]any{"receiver_id": "receiver-a", "pool": "pool-a", "outputs": outputs})
	status := now(wire.TaskStateCompleted)
	status.Message = &wire.Message{MessageID: "msg-status", Role: wire.RoleAgent, Parts: []wire.Part{{Text: "done"}}, Extensions: []string{wire.ExtensionURI}}
	result := &wire.Result{Task: wire.Task{ID: run, ContextID: run, History: []wire.Message{admitted.submission.Message}, Status: status,
		Artifacts: []wire.Artifact{{ArtifactID: "result", Parts: []wire.Part{{Text: "done"}}, Extensions: []string{wire.ExtensionURI}}}, Metadata: manifest},
		Wherry: wire.ResultBinding{ReceiverBinding: b.binding(admitted), TaskHash: admitted.hash}}
	b.feed(t, run, append(chunks, b.sign(t, result), b.status(t, admitted, "terminal-1", "terminal", now(wire.TaskStateCompleted)))...)
	args := []string{run}
	if dir != "" {
		args = append(args, "--outputs-dir", dir)
	}
	var stdout, stderr bytes.Buffer
	if err := runWait(b.ctx, b.client, args, &stdout, &stderr); err != nil {
		t.Fatalf("result: %v; stderr %s", err, stderr.String())
	}
	return run
}

// The CLI and the MCP tool sign the same submission for the same --input DIR and the same --continue of a run without
// git: the same input names, sizes and hashes, and the same context.
func TestCLIAndMCPSubmitTheSameInputs(t *testing.T) {
	b := newBus(t)
	work := t.TempDir()
	writeFiles(t, work, "docs/readme.md", "docs/guide/a.md", "docs/.hidden")
	first := b.plainRunCollected(t, t.TempDir(), "report.md", "notes/next.md")
	session := mcpSessionReading(t, b.connect(t), nil, inputOpener(work, busclient.Config{StateDir: b.stateDir}))
	for name, test := range map[string]struct {
		flags     []string
		arguments map[string]any
	}{
		"input directory": {[]string{"--input", filepath.Join(work, "docs")}, map[string]any{"prompt": "read", "inputs": []map[string]any{{"path": "docs"}}}},
		"continue":        {[]string{"--continue", first}, map[string]any{"prompt": "read", "continue": first}},
	} {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if err := runSubmit(b.ctx, b.client, append([]string{"--prompt", "read"}, test.flags...), &stdout, &stderr); err != nil {
				t.Fatalf("submit: %v %s", err, stderr.String())
			}
			cli := b.tasks(t, submittedID(stdout.String()))[0].submission
			var submitted submitOutput
			callTool(t, b.ctx, session, "submit", test.arguments, &submitted)
			tool := b.tasks(t, submitted.ID)[0].submission
			if parts := inputParts(cli); len(parts) < 2 || !slices.Equal(parts, inputParts(tool)) {
				t.Fatalf("the CLI signed %+v and the tool %+v", parts, inputParts(tool))
			}
			if cli.Message.ContextID != tool.Message.ContextID && cli.Message.ContextID != cli.Wherry.Run || !slices.Equal(cli.Message.ReferenceTaskIDs, tool.Message.ReferenceTaskIDs) {
				t.Fatalf("the CLI's context %s and references %v; the tool's %s and %v", cli.Message.ContextID, cli.Message.ReferenceTaskIDs, tool.Message.ContextID, tool.Message.ReferenceTaskIDs)
			}
		})
	}
}

// The MCP submit tool continues a run without git whose outputs result collected to the default directory, beneath the
// state directory the input roots refuse: the outputs are the host's own, opened as the host, and arrive as previous/<name>
// with the run's context; a run never collected is refused with the hint.
func TestMCPContinuesARunCollectedToTheStateDirectory(t *testing.T) {
	b := newBus(t)
	first := b.plainRunCollected(t, "", "report.md", "notes/next.md")
	session := mcpSessionReading(t, b.connect(t), nil, inputOpener(t.TempDir(), busclient.Config{StateDir: b.stateDir}))
	var submitted submitOutput
	callTool(t, b.ctx, session, "submit", map[string]any{"prompt": "next", "continue": first}, &submitted)
	task := b.tasks(t, submitted.ID)[0].submission
	want := []inputPart{{"previous/report.md", hashOf("report.md"), 9}, {"previous/notes/next.md", hashOf("notes/next.md"), 13}}
	if parts := inputParts(task); !slices.Equal(parts, want) || task.Message.ContextID != first || !slices.Equal(task.Message.ReferenceTaskIDs, []string{first}) || task.Wherry.Git != nil {
		t.Fatalf("parts %+v, context %s, references %v, git %+v; want the previous outputs with the first run's context", parts, task.Message.ContextID, task.Message.ReferenceTaskIDs, task.Wherry.Git)
	}
	never := b.admitted(t).submission.Wherry.Run
	if failure := callToolError(t, b.ctx, session, "submit", map[string]any{"prompt": "next", "continue": never}); failure.Code != "INVALID_ARGUMENT" || failure.Message != "invalid input: continue: run "+never+" has no collected outputs; run wherry result "+never+" first" {
		t.Fatalf("continue of a run never collected: %+v", failure)
	}
}

func TestWaitSaysHowToApplyGitChanges(t *testing.T) {
	for name, command := range map[string]string{wire.GitPatch: "apply with: git am %s\n", wire.GitBundle: "fetch with: git fetch %s HEAD\n"} {
		t.Run(name, func(t *testing.T) {
			b := newBus(t)
			admitted := b.admitted(t)
			b.outputResult(t, admitted, name, "changes\n", hashOf("changes\n"))
			dir := t.TempDir()
			var stdout, stderr bytes.Buffer
			if err := runWait(b.ctx, b.client, []string{admitted.submission.Wherry.Run, "--outputs-dir", dir}, &stdout, &stderr); err != nil {
				t.Fatalf("wait: %v; stderr %s", err, stderr.String())
			}
			if want := fmt.Sprintf(command, filepath.Join(dir, name)); !strings.Contains(stderr.String(), want) {
				t.Fatalf("stderr %q, want %q", stderr.String(), want)
			}
		})
	}
}

func TestMCPSubmitCarriesTheGitInput(t *testing.T) {
	b := newBus(t)
	const key = "git.example.test ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl\n"
	hostKey := filepath.Join(t.TempDir(), "known_hosts")
	if err := os.WriteFile(hostKey, []byte(key), 0o600); err != nil {
		t.Fatal(err)
	}
	b.git = []busclient.GitCredential{{URL: "ssh://git@git.example.test/", Credential: "model", HostKey: hostKey}}
	session := mcpSessionReading(t, b.connect(t), nil, inputOpener(t.TempDir(), busclient.Config{Credentials: b.credentials}))
	git := map[string]any{"remote": "git@git.example.test:org/repo.git", "ref": "main", "path": "src/repo", "changes": "bundle"}
	var submitted submitOutput
	callTool(t, b.ctx, session, "submit", map[string]any{"prompt": "fix it", "git": git}, &submitted)
	submission := b.tasks(t, submitted.ID)[0].submission
	want := wire.GitInput{Remote: "ssh://git@git.example.test/org/repo.git", Ref: "main", Path: "src/repo", Credential: "model", HostKey: key}
	if submission.Wherry.Git == nil || *submission.Wherry.Git != want || !slices.Equal(submission.Wherry.Outputs, []wire.ExpectedOutput{{Name: wire.GitBundle}}) ||
		!slices.Equal(submission.Wherry.Credentials, []wire.TaskCredential{{Name: "model", File: wire.GitCredentialFile}}) {
		t.Fatalf("submitted git %+v, credentials %+v and outputs %+v", submission.Wherry.Git, submission.Wherry.Credentials, submission.Wherry.Outputs)
	}
	git["remote"] = "ssh://git@github.com/org/repo.git"
	if failure := callToolError(t, b.ctx, session, "submit", map[string]any{"prompt": "x", "git": git}); failure.Code != "INVALID_ARGUMENT" ||
		failure.Message != "invalid input: ssh git remote requires host_key" {
		t.Fatalf("submit of an ssh remote under no git entry: %+v", failure)
	}
}

// A git entry's host key file is read only to one byte over its bound: submit stops reading a pipe there.
func TestGitHostKeyReadIsBounded(t *testing.T) {
	b := newBus(t)
	fifo := filepath.Join(t.TempDir(), "known_hosts")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	b.git = []busclient.GitCredential{{URL: "ssh://git@git.example.test/", HostKey: fifo}}
	client, err := b.connect(t)()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	written := make(chan error, 1)
	go func() {
		writer, err := os.OpenFile(fifo, os.O_WRONLY, 0)
		if err == nil {
			_, err = writer.Write(bytes.Repeat([]byte("x"), 1<<20))
			writer.Close()
		}
		written <- err
	}()
	const rule = "git host_key must be at most 8 KiB of OpenSSH known_hosts lines"
	var stdout, stderr bytes.Buffer
	if err := runSubmit(b.ctx, client, []string{"--prompt", "x", "--git", "git@git.example.test:org/repo.git@main"}, &stdout, &stderr); err == nil || !strings.Contains(err.Error(), rule) {
		t.Fatalf("submit = %v, want %q", err, rule)
	}
	if err := <-written; err == nil {
		t.Fatal("submit read the whole host key file")
	}
}

// The S3 lane is chosen by size for inputs and outputs alike; the suffixes earlier versions took are refused with a
// hint, on the command line and in MCP, before anything is read or published.
func TestSubmitRefusesDroppedSuffixes(t *testing.T) {
	b := newBus(t)
	work := t.TempDir()
	data := filepath.Join(work, "dataset.bin")
	if err := os.WriteFile(data, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	for _, test := range []struct {
		args []string
		want string
	}{
		{[]string{"--input", data + ":s3"}, "inputs move to S3 by size; drop :s3"},
		{[]string{"--output", "report.pdf:s3"}, "outputs move to S3 by size; drop :s3"},
		{[]string{"--output", "report.pdf:required"}, "outputs are always required; drop :required"},
		{[]string{"--output", "report.pdf:required:s3"}, "outputs are always required; drop :required"},
		{[]string{"--output", "report.pdf:s3:required"}, "outputs are always required; drop :required"},
		{[]string{"--output", "report.pdf:S3"}, "want NAME, not"},
		{[]string{"--input", data + ":S3"}, "no such file"},
	} {
		if err := runSubmit(b.ctx, b.client, append([]string{"--prompt", "x"}, test.args...), &stdout, &stderr); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("submit %v = %v; want %q", test.args, err, test.want)
		}
	}
	if err := runSubmit(b.ctx, b.client, []string{"--prompt", "x", "--input", data}, &stdout, &stderr); err != nil {
		t.Fatalf("submit of a small input = %v; want it sent in the task", err)
	}
	if err := runSubmit(b.ctx, b.client, []string{"--prompt", "x", "--s3-input", data}, &stdout, &stderr); err == nil || err.Error() != "flag provided but not defined: -s3-input" {
		t.Fatalf("submit --s3-input = %v; want an unknown flag", err)
	}
	session := mcpSessionReading(t, b.connect(t), nil, inputOpener(work, busclient.Config{}))
	for _, test := range []struct {
		arguments map[string]any
		want      string
	}{
		{map[string]any{"prompt": "x", "inputs": []map[string]any{{"path": "dataset.bin", "storage": "s3"}}}, "inputs move to S3 by size; drop :s3"},
		{map[string]any{"prompt": "x", "inputs": []map[string]any{{"path": "dataset.bin", "storage": "feed"}}}, "inputs move to S3 by size; drop :s3"},
		{map[string]any{"prompt": "x", "outputs": []map[string]any{{"name": "report.pdf", "storage": "s3"}}}, "outputs move to S3 by size; drop :s3"},
		{map[string]any{"prompt": "x", "outputs": []map[string]any{{"name": "report.pdf", "storage": "feed"}}}, "outputs move to S3 by size; drop :s3"},
		{map[string]any{"prompt": "x", "outputs": []map[string]any{{"name": "report.pdf", "required": true}}}, "outputs are always required; drop :required"},
		{map[string]any{"prompt": "x", "outputs": []map[string]any{{"name": "report.pdf", "required": false}}}, "outputs are always required; drop :required"},
	} {
		if failure := callToolError(t, b.ctx, session, "submit", test.arguments); failure.Code != "INVALID_ARGUMENT" || failure.Message != test.want {
			t.Fatalf("MCP submit %v = %+v; want %q", test.arguments, failure, test.want)
		}
	}
	var submitted submitOutput
	callTool(t, b.ctx, session, "submit", map[string]any{"prompt": "x", "inputs": []map[string]any{{"path": "dataset.bin"}}}, &submitted)
	result, err := session.CallTool(b.ctx, &mcp.CallToolParams{Name: "submit", Arguments: map[string]any{"prompt": "x", "s3_inputs": []string{"dataset.bin"}}})
	if err != nil || !result.IsError || !strings.Contains(toolText(result), `unexpected additional properties ["s3_inputs"]`) {
		t.Fatalf("MCP submit with s3_inputs = %+v, %v; want s3_inputs refused as unknown", result, err)
	}
	tools, err := session.ListTools(b.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range tools.Tools {
		if tool.Name != "submit" {
			continue
		}
		encoded, _ := json.Marshal(tool.InputSchema)
		for _, dropped := range []string{`"required":{`, `"storage":{"description":"s3 to upload`} {
			if strings.Contains(string(encoded), dropped) {
				t.Fatalf("submit's input schema keeps %s: %s", dropped, encoded)
			}
		}
	}
}
