package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	json "encoding/json/v2"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/wire"
)

func TestSubmitFileFlags(t *testing.T) {
	b := newBus(t)
	deployKey := filepath.Join(t.TempDir(), "deploy-key")
	if err := os.WriteFile(deployKey, []byte("deploy-key-value"), 0o600); err != nil {
		t.Fatal(err)
	}
	b.credentials["deploy-key"] = deployKey
	input := filepath.Join(t.TempDir(), "data.csv")
	if err := os.WriteFile(input, []byte("a,b\n1,2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	large := filepath.Join(t.TempDir(), "large.bin")
	if err := os.WriteFile(large, make([]byte, admin.DefaultInlineLimit+1), 0o600); err != nil {
		t.Fatal(err)
	}
	base := []string{"--prompt", "summarise the data"}
	for name, test := range map[string]struct {
		flags []string
		want  string
	}{
		"credential without a file": {[]string{"--credential-file", "deploy-key"}, "want NAME=FILE"},
		"unconfigured credential":   {[]string{"--credential-file", "absent=token"}, "credential absent is not in this host's configuration"},
		"inputs over the limit":     {[]string{"--input", large}, "inline inputs are 262145 bytes, over the 262144-byte limit"},
		"output escaping":           {[]string{"--output", "../report.md"}, "expected outputs must be unique relative paths of portable file names"},
		"credential file escaping":  {[]string{"--credential-file", "deploy-key=../id"}, "credential files must be unique portable file names"},
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
	flags := append(slices.Clone(base), "--input", input, "--credential-file", "deploy-key=id_ed25519", "--output", "report.md:required", "--output", "notes/extra.txt")
	if err := runSubmit(b.ctx, b.client, flags, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	runID, _, _ := strings.Cut(strings.TrimPrefix(stdout.String(), "run_id="), " ")
	submission := b.tasks(t, runID)[0].submission
	parts := submission.Message.Parts
	if len(parts) != 2 || parts[1].Filename != "data.csv" || string(parts[1].Raw) != "a,b\n1,2\n" {
		t.Fatalf("submitted parts %#v", parts)
	}
	if !slices.Equal(submission.Wherry.Credentials, []wire.TaskCredential{{Name: "deploy-key", File: "id_ed25519"}}) ||
		!slices.Equal(submission.Wherry.Outputs, []wire.ExpectedOutput{{Name: "report.md", Required: true}, {Name: "notes/extra.txt"}}) {
		t.Fatalf("submitted credentials %+v and outputs %+v", submission.Wherry.Credentials, submission.Wherry.Outputs)
	}
	if strings.Contains(stdout.String()+stderr.String(), "deploy-key-value") {
		t.Fatal("submit printed the credential")
	}
}

// outputResult commits an output's one chunk and a result listing it, as the receiver's end does.
func (b *bus) outputResult(t *testing.T, admitted task, name, data, listedHash string) {
	t.Helper()
	run := admitted.submission.Wherry.Run
	metadata, _ := json.Marshal(wire.OutputMetadata{Index: 0})
	chunk := b.sign(t, &wire.Output{
		ArtifactUpdate: wire.TaskArtifactUpdateEvent{TaskID: run, ContextID: run, LastChunk: true, Metadata: metadata,
			Artifact: wire.Artifact{ArtifactID: name, Parts: []wire.Part{{Raw: wire.StdBytes(data)}}, Extensions: []string{wire.ExtensionURI}}},
		Wherry: wire.ResultBinding{ReceiverBinding: b.binding(admitted), TaskHash: admitted.hash},
	})
	manifest, _ := json.Marshal(map[string]any{"receiver_id": "receiver-a", "pool": "pool-a",
		"outputs":      []wire.ReturnedOutput{{Name: name, SHA256: listedHash, Size: int64(len(data))}},
		"not_returned": []wire.UnreturnedOutput{{Name: "notes.md", Reason: "missing"}}})
	result := &wire.Result{Task: wire.Task{ID: run, ContextID: run, History: []wire.Message{admitted.submission.Message}, Status: now(wire.TaskStateCompleted),
		Artifacts: []wire.Artifact{{ArtifactID: "result", Parts: []wire.Part{{Text: "done"}}, Extensions: []string{wire.ExtensionURI}}}, Metadata: manifest},
		Wherry: wire.ResultBinding{ReceiverBinding: b.binding(admitted), TaskHash: admitted.hash}}
	b.feed(t, run, chunk, b.sign(t, result), b.status(t, admitted, "terminal-1", "terminal", now(wire.TaskStateCompleted)))
}

func hashOf(data string) string {
	sum := sha256.Sum256([]byte(data))
	return hex.EncodeToString(sum[:])
}

func TestAwaitWritesVerifiedOutputs(t *testing.T) {
	b := newBus(t)
	admitted := b.admitted(t)
	runID := admitted.submission.Wherry.Run
	b.outputResult(t, admitted, "sub/report.md", "# Report\n", hashOf("# Report\n"))
	dir := t.TempDir()
	var stdout, stderr bytes.Buffer
	if err := runAwait(b.ctx, b.client, []string{runID, "--outputs-dir", dir}, &stdout, &stderr); err != nil {
		t.Fatalf("await: %v; stderr %s", err, stderr.String())
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

func TestAwaitFailsOnAnOutputItCannotVerify(t *testing.T) {
	b := newBus(t)
	admitted := b.admitted(t)
	runID := admitted.submission.Wherry.Run
	b.outputResult(t, admitted, "report.md", "# Report\n", hashOf("# Forged\n"))
	var stdout, stderr bytes.Buffer
	err := runAwait(b.ctx, b.client, []string{runID}, &stdout, &stderr)
	if err == nil || !strings.Contains(stderr.String(), "output report.md not returned: rejected by this host: size or hash mismatch") {
		t.Fatalf("await = %v, stderr %q; want the output refused", err, stderr.String())
	}
	if _, statErr := os.Stat(filepath.Join(b.stateDir, "runs", runID, "outputs")); statErr == nil {
		t.Fatal("wrote an outputs directory for an output that failed verification")
	}
}

func TestMCPSubmitCarriesFilesAndAwaitWritesOutputs(t *testing.T) {
	b := newBus(t)
	b.credentials["deploy-key"] = b.credentials["model"]
	work := t.TempDir()
	session := mcpSessionReading(t, b.connect(t), nil, inputOpener(work, busclient.Config{}))
	input := filepath.Join(work, "data.csv")
	if err := os.WriteFile(input, []byte("a,b\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	arguments := map[string]any{"prompt": "summarise", "inputs": []string{input},
		"credentials": []map[string]any{{"name": "deploy-key", "file": "id_ed25519"}}, "outputs": []map[string]any{{"name": "report.md", "required": true}}}
	var submitted submitOutput
	callTool(t, b.ctx, session, "submit", arguments, &submitted)
	admitted := b.tasks(t, submitted.ID)[0]
	if submission := admitted.submission; len(submission.Message.Parts) != 2 || submission.Message.Parts[1].Filename != "data.csv" ||
		len(submission.Wherry.Credentials) != 1 || !slices.Equal(submission.Wherry.Outputs, []wire.ExpectedOutput{{Name: "report.md", Required: true}}) {
		t.Fatalf("submission %+v", submission)
	}
	if failure := callToolError(t, b.ctx, session, "submit", map[string]any{"prompt": "x", "inputs": []string{filepath.Join(t.TempDir(), "absent")}}); failure.Code != "INVALID_ARGUMENT" {
		t.Fatalf("submit with an unreadable input: %+v", failure)
	}
	b.feed(t, submitted.ID, b.echo(t, admitted), b.status(t, admitted, "claimed-1", "claimed", now(wire.TaskStateWorking)))
	b.outputResult(t, admitted, "report.md", "# Report\n", hashOf("# Report\n"))
	var reply awaitReply
	callTool(t, b.ctx, session, "await", map[string]any{"id": submitted.ID}, &reply)
	path := filepath.Join(b.stateDir, "runs", submitted.ID, "outputs", "1", "report.md")
	if !slices.Equal(reply.Outputs, []outputReply{{Name: "report.md", Path: path, SHA256: hashOf("# Report\n"), Size: 9}}) ||
		!slices.Equal(reply.NotReturned, []wire.UnreturnedOutput{{Name: "notes.md", Reason: "missing"}}) {
		t.Fatalf("await outputs %+v, not returned %+v", reply.Outputs, reply.NotReturned)
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
		Credentials: map[string]string{"model": write(filepath.Join(work, "secrets", "model"))},
		Storage:     &busclient.StorageConfig{AccessKeyFile: write(filepath.Join(work, "s3", "access.key")), SecretKeyFile: write(filepath.Join(work, "s3", "secret.key"))}}
	write(filepath.Join(config.StateDir, "runs", "run-a.json"))
	read := inputReader(inputOpener(work, config))
	for name, path := range map[string]string{
		"outside":                filepath.Join(outside, "bundle.yaml"),
		"a link out":             link(write(filepath.Join(outside, "elsewhere")), "elsewhere"),
		"a link to the bundle":   link(config.Bundle, "bundle"),
		"the host key":           "host.key",
		"a link to the host key": link(config.HostKey, "key"),
		"the state":              filepath.Join(config.StateDir, "runs", "run-a.json"),
		"a credential":           config.Credentials["model"],
		"a storage access key":   config.Storage.AccessKeyFile,
		"a storage secret key":   config.Storage.SecretKeyFile,
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
		Credentials: map[string]string{"model": write(filepath.Join(first, "wherry", "model"))}}
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
	read := inputReader(inputOpener(work, config))
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
		refused["a credential, upper-cased"] = upper(first, config.Credentials["model"])
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

func TestAwaitSaysHowToApplyGitChanges(t *testing.T) {
	for name, command := range map[string]string{wire.GitPatch: "apply with: git am %s\n", wire.GitBundle: "fetch with: git fetch %s HEAD\n"} {
		t.Run(name, func(t *testing.T) {
			b := newBus(t)
			admitted := b.admitted(t)
			b.outputResult(t, admitted, name, "changes\n", hashOf("changes\n"))
			dir := t.TempDir()
			var stdout, stderr bytes.Buffer
			if err := runAwait(b.ctx, b.client, []string{admitted.submission.Wherry.Run, "--outputs-dir", dir}, &stdout, &stderr); err != nil {
				t.Fatalf("await: %v; stderr %s", err, stderr.String())
			}
			if want := fmt.Sprintf(command, filepath.Join(dir, name)); !strings.Contains(stderr.String(), want) {
				t.Fatalf("stderr %q, want %q", stderr.String(), want)
			}
		})
	}
}

func TestMCPSubmitCarriesTheGitInput(t *testing.T) {
	b := newBus(t)
	work := t.TempDir()
	session := mcpSessionReading(t, b.connect(t), nil, inputOpener(work, busclient.Config{Credentials: b.credentials}))
	const key = "git.example.test ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl\n"
	if err := os.WriteFile(filepath.Join(work, "known_hosts"), []byte(key), 0o600); err != nil {
		t.Fatal(err)
	}
	git := map[string]any{"remote": "git@git.example.test:org/repo.git", "ref": "main", "path": "src/repo", "credential": "model", "host_key": "known_hosts", "changes": "bundle"}
	var submitted submitOutput
	callTool(t, b.ctx, session, "submit", map[string]any{"prompt": "fix it", "credentials": []map[string]any{{"name": "model", "file": "id_ed25519"}}, "git": git}, &submitted)
	submission := b.tasks(t, submitted.ID)[0].submission
	want := wire.GitInput{Remote: "ssh://git@git.example.test/org/repo.git", Ref: "main", Path: "src/repo", Credential: "model", HostKey: key}
	if submission.Wherry.Git == nil || *submission.Wherry.Git != want || !slices.Equal(submission.Wherry.Outputs, []wire.ExpectedOutput{{Name: wire.GitBundle}}) {
		t.Fatalf("submitted git %+v and outputs %+v", submission.Wherry.Git, submission.Wherry.Outputs)
	}
	// The host key is read as an input is, so it cannot name a credential of this host's configuration.
	git["host_key"] = b.credentials["model"]
	if failure := callToolError(t, b.ctx, session, "submit", map[string]any{"prompt": "x", "git": git}); failure.Code != "INVALID_ARGUMENT" || strings.Contains(failure.Message, credentialText) {
		t.Fatalf("submit with a credential as the host key: %+v", failure)
	}
	if err := os.WriteFile(filepath.Join(work, "github_known_hosts"), []byte("github.com"+key[len("git.example.test"):]), 0o600); err != nil {
		t.Fatal(err)
	}
	git["host_key"], git["remote"] = "github_known_hosts", "ssh://git@github.com/org/repo.git"
	if failure := callToolError(t, b.ctx, session, "submit", map[string]any{"prompt": "x", "credentials": []map[string]any{{"name": "model", "file": "id_ed25519"}}, "git": git}); failure.Code != "INVALID_ARGUMENT" ||
		failure.Message != "invalid input: git remote host github.com is not in pool pool-a's vcs hosts" {
		t.Fatalf("submit outside the vcs hosts: %+v", failure)
	}
}

// A host key file is read only to one byte over its bound: the CLI stops reading a pipe there, and the MCP server asks
// its reader for no more.
func TestGitHostKeyReadIsBounded(t *testing.T) {
	b := newBus(t)
	fifo := filepath.Join(t.TempDir(), "known_hosts")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
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
	if err := runSubmit(b.ctx, b.client, []string{"--prompt", "x", "--git", "git@git.example.test:org/repo.git@main", "--git-host-key", fifo}, &stdout, &stderr); err == nil || !strings.Contains(err.Error(), rule) {
		t.Fatalf("submit = %v, want %q", err, rule)
	}
	if err := <-written; err == nil {
		t.Fatal("the CLI read the whole host key file")
	}
	var limits []int64
	read := func(path string, limit int64) (wire.Part, error) {
		limits = append(limits, limit)
		return wire.Part{Raw: wire.StdBytes("k")}, nil
	}
	if _, err := submitFromInput(submitInput{Prompt: "x", Git: &gitInput{Remote: "git@git.example.test:org/repo.git", Ref: "main", HostKey: "known_hosts"}}, read, nil); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(limits, []int64{wire.MaxGitHostKey + 1}) {
		t.Fatalf("host key read with limits %v, want one byte over the bound", limits)
	}
}

func TestSubmitTakesS3InputsAndOutputs(t *testing.T) {
	b := newBus(t)
	work := t.TempDir()
	data := filepath.Join(work, "dataset.bin")
	if err := os.WriteFile(data, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	const rule = "this host's configuration has no storage credential for S3 inputs and outputs"
	for _, args := range [][]string{{"--s3-input", data}, {"--output", "report.pdf:required:s3"}} {
		var stdout, stderr bytes.Buffer
		if err := runSubmit(b.ctx, b.client, append([]string{"--prompt", "x"}, args...), &stdout, &stderr); err == nil || !strings.Contains(err.Error(), rule) {
			t.Fatalf("submit %v = %v; want the S3 lane refused for want of storage", args, err)
		}
	}
	session := mcpSessionReading(t, b.connect(t), nil, inputOpener(work, busclient.Config{}))
	for _, arguments := range []map[string]any{
		{"prompt": "x", "s3_inputs": []string{"dataset.bin"}},
		{"prompt": "x", "outputs": []map[string]any{{"name": "report.pdf", "storage": "s3"}}},
	} {
		if failure := callToolError(t, b.ctx, session, "submit", arguments); failure.Code != "INVALID_ARGUMENT" || !strings.Contains(failure.Message, rule) {
			t.Fatalf("MCP submit %v = %+v", arguments, failure)
		}
	}
	if failure := callToolError(t, b.ctx, session, "submit", map[string]any{"prompt": "x", "s3_inputs": []string{"../outside.bin"}}); failure.Code != "INVALID_ARGUMENT" ||
		!strings.Contains(failure.Message, "beneath one of the MCP server's input roots") {
		t.Fatalf("MCP submit of an S3 input outside the working directory = %+v", failure)
	}
}
