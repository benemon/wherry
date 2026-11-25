package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/wire"
)

// testBase and testHead are the commits of the fixture's bundles.
var testBase, testHead = strings.Repeat("ab", 20), strings.Repeat("cd", 20)

// bundleBytes is a git bundle's header and the start of a pack.
func bundleBytes(header string) []byte {
	return []byte(header + "\nPACK\x00\x00\x00\x02")
}

// testBundle is a v2 bundle of HEAD over the one prerequisite testBase.
var testBundle = bundleBytes("# v2 git bundle\n-" + testBase + " base commit\n" + testHead + " HEAD\n")

// continueFixture adds pool-c, with storage at a fake S3 store whose credential the host holds and git.example.test as
// its vcs host, and a live receiver in it alone.
func continueFixture(t *testing.T) (*fixture, *uploadOracle, *testReceiver) {
	t.Helper()
	o := newUploadOracle(t)
	f := newFixture(t)
	f.host.roots = o.roots
	terms := testTerms()
	terms.Storage = &operator.Storage{Endpoint: o.endpoint, Bucket: testBucket}
	terms.VCS = []string{"git.example.test"}
	if err := f.service.PoolApply(f.ctx, "pool-c", terms, false); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantPoolAdd(f.ctx, "tenant-a", "pool-c"); err != nil {
		t.Fatal(err)
	}
	f.host.storage = map[string]StorageConfig{"pool-c": *o.config}
	maps.Copy(f.host.credentials, o.sources)
	receiver := f.addReceiver(t, "receiver-c", "pool-c")
	receiver.advertise(t, f.ctx, nil)
	return f, o, receiver
}

func continueInput(run string) SubmitInput {
	return SubmitInput{Selector: defaultSelector(), Prompt: "next round", Continue: run, EndBy: time.Now().Add(30 * time.Minute), StartBy: time.Now().Add(10 * time.Minute)}
}

// firstRun submits a run cloning git.example.test's org/repo at main into src/repo and records data as the
// changes.bundle Collect verified for it.
func firstRun(t *testing.T, f *fixture, data []byte) string {
	t.Helper()
	submitted, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "first round", Git: &wire.GitInput{Remote: "https://git.example.test/org/repo.git", Ref: "main", Path: "src/repo"},
		GitChanges: "bundle", EndBy: time.Now().Add(30 * time.Minute), StartBy: time.Now().Add(10 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	collectBundle(t, f, []string{submitted.RunID}, data)
	return submitted.RunID
}

// collectBundle has Collect verify data as the changes.bundle of the last of runs, written to a new directory.
func collectBundle(t *testing.T, f *fixture, runs []string, data []byte) string {
	t.Helper()
	dir := t.TempDir()
	result := AwaitResult{Runs: runs, Outputs: []Output{{Name: wire.GitBundle, Data: data, Size: int64(len(data)), SHA256: hexSum(data)}}}
	if _, err := f.host.Collect(f.ctx, runs[0], result, dir); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, wire.GitBundle)
}

// A continuation clones the previous run's repository at its base with the current configuration's credential, carries
// the collected bundle as base.bundle through S3 and asks for its own, joins the previous run's context and records the
// repository the previous run recorded.
func TestSubmitContinuesARun(t *testing.T) {
	f, o, receiver := continueFixture(t)
	first := firstRun(t, f, testBundle)
	rewrite(t, f, first, func(local *runRecord) { local.Repo = "/home/me/src/repo" })
	// The git configuration changed after the first run: the clone takes the current one's credential.
	f.host.credentials["forge-token"] = credential.Source{File: f.secret}
	f.host.git = []GitCredential{{URL: "https://git.example.test/", Credential: "forge-token"}}
	submitted, err := f.host.Submit(f.ctx, continueInput(first))
	if err != nil {
		t.Fatal(err)
	}
	if local, err := f.host.localRun(submitted.RunID); err != nil || local == nil || local.Repo != "/home/me/src/repo" {
		t.Fatalf("the continuation's record holds repo %q, %v; want the first run's", local.Repo, err)
	}
	task := receiver.claim(t, f.ctx, submitted.RunID)[0].submission
	want := wire.GitInput{Remote: "https://git.example.test/org/repo.git", Ref: "main", Path: "src/repo", Credential: "forge-token", Base: &wire.GitBase{Bundle: "base.bundle", Commit: testBase}}
	if git := task.Wherry.Git; git == nil || git.Base == nil || *git.Base != *want.Base || git.Remote != want.Remote || git.Ref != want.Ref || git.Path != want.Path || git.Credential != want.Credential {
		t.Fatalf("git %+v, want %+v with base %+v", git, want, want.Base)
	}
	if !slices.Equal(task.Wherry.S3Inputs, []wire.S3Input{{Name: "base.bundle", Size: int64(len(testBundle)), SHA256: hexSum(testBundle)}}) {
		t.Fatalf("S3 inputs %+v, want the collected bundle", task.Wherry.S3Inputs)
	}
	if got := o.object("/" + testBucket + "/" + wire.S3InputKey("tenant-a", submitted.RunID, "base.bundle")); !bytes.Equal(got, testBundle) {
		t.Fatalf("uploaded %q, want the collected bundle", got)
	}
	if !slices.Equal(task.Wherry.Outputs, []wire.ExpectedOutput{{Name: wire.GitBundle}}) {
		t.Fatalf("outputs %+v, want changes.bundle expected, not required", task.Wherry.Outputs)
	}
	if message := task.Message; message.TaskID != submitted.RunID || message.ContextID != first || !slices.Equal(message.ReferenceTaskIDs, []string{first}) {
		t.Fatalf("message task %s, context %s, references %v; want the first run's context and a reference to it", message.TaskID, message.ContextID, message.ReferenceTaskIDs)
	}
	// Continuing the continuation keeps the first run's context and references the continuation.
	collectBundle(t, f, []string{submitted.RunID}, bundleBytes("# v2 git bundle\n-"+testBase+" \n"+strings.Repeat("ef", 20)+" HEAD\n"))
	third, err := f.host.Submit(f.ctx, continueInput(submitted.RunID))
	if err != nil {
		t.Fatal(err)
	}
	if message := receiver.claim(t, f.ctx, third.RunID)[0].submission.Message; message.ContextID != first || !slices.Equal(message.ReferenceTaskIDs, []string{submitted.RunID}) {
		t.Fatalf("third round's context %s and references %v, want the first run's context and the second run", message.ContextID, message.ReferenceTaskIDs)
	}
}

// replace records replacement as the produced resubmission of run, holding the manifest run had.
func replace(t *testing.T, f *fixture, run string) string {
	t.Helper()
	local, err := f.host.localRun(run)
	if err != nil || local == nil {
		t.Fatalf("local record %v, %v", local, err)
	}
	nonce := wire.RunNonce()
	replacement := *local
	replacement.Run = wire.RunID("tenant-a", "host-a", nonce)
	task := *local.Task
	task.Wherry.Run, task.Wherry.RunNonce, task.Wherry.Replaces, task.Wherry.S3InputRun = replacement.Run, nonce, run, run
	task.Message.TaskID = replacement.Run
	replacement.Task, replacement.Signed, replacement.Produced = &task, []byte("signed"), true
	local.Replacement, local.ReplacementNonce, local.Outputs = replacement.Run, nonce, nil
	for _, record := range []runRecord{replacement, *local} {
		if err := f.host.writeRun(record); err != nil {
			t.Fatal(err)
		}
	}
	return replacement.Run
}

// Continuing a replaced run continues its replacement: its collected bundle, a reference to it, and its context.
func TestSubmitContinuesTheLastRunOfTheChain(t *testing.T) {
	f, _, receiver := continueFixture(t)
	first := firstRun(t, f, testBundle)
	replacement := replace(t, f, first)
	submitted, err := f.host.Submit(f.ctx, continueInput(first))
	if err != nil {
		t.Fatal(err)
	}
	task := receiver.claim(t, f.ctx, submitted.RunID)[0].submission
	if task.Message.ContextID != first || !slices.Equal(task.Message.ReferenceTaskIDs, []string{replacement}) || task.Wherry.S3Inputs[0].SHA256 != hexSum(testBundle) {
		t.Fatalf("context %s, references %v, S3 inputs %+v; want the chain's context, the replacement and its bundle", task.Message.ContextID, task.Message.ReferenceTaskIDs, task.Wherry.S3Inputs)
	}
}

// refused submits input and expects its refusal naming want, before any upload or publish.
func refused(t *testing.T, f *fixture, o *uploadOracle, input SubmitInput, want string) {
	t.Helper()
	_, err := f.host.Submit(f.ctx, input)
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), want) {
		t.Fatalf("submit = %v, want an invalid input naming %q", err, want)
	}
	if strings.Contains(err.Error(), f.stateDir) || strings.Contains(err.Error(), os.TempDir()) {
		t.Fatalf("refusal %q names a path", err)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.ops) != 0 {
		t.Fatalf("S3 requests %v before the refusal", o.ops)
	}
	if end := f.tasksEnd(t, "pool-c"); end != 1 {
		t.Fatalf("%d tasks published, want the first run's alone", end)
	}
}

// foreign records a copy of run's record as a run another tenant or host derived, holding run's manifest, in the
// shared state directory.
func foreign(t *testing.T, f *fixture, run, tenant, host string) string {
	t.Helper()
	local, err := f.host.localRun(run)
	if err != nil || local == nil {
		t.Fatalf("local record %v, %v", local, err)
	}
	nonce := wire.RunNonce()
	other := *local
	task := *local.Task
	other.Run = wire.RunID(tenant, host, nonce)
	task.Wherry.Tenant, task.Wherry.Host, task.Wherry.Run, task.Wherry.RunNonce, task.Message.TaskID, task.Message.ContextID = tenant, host, other.Run, nonce, other.Run, other.Run
	other.Task = &task
	if err := f.host.writeRun(other); err != nil {
		t.Fatal(err)
	}
	return other.Run
}

// rewrite changes run's local record with change.
func rewrite(t *testing.T, f *fixture, run string, change func(*runRecord)) {
	t.Helper()
	local, err := f.host.localRun(run)
	if err != nil || local == nil {
		t.Fatalf("local record %v, %v", local, err)
	}
	change(local)
	if err := f.host.writeRun(*local); err != nil {
		t.Fatal(err)
	}
}

func TestSubmitRefusesAContinuationBeforeUploadOrPublish(t *testing.T) {
	for name, test := range map[string]struct {
		setup func(t *testing.T, f *fixture, first string) SubmitInput
		want  string
	}{
		"unknown run": {func(t *testing.T, f *fixture, first string) SubmitInput {
			return continueInput(wire.RunID("tenant-a", "host-a", wire.RunNonce()))
		}, "is not a run this host submitted under its tenant"},
		"another tenant's run": {func(t *testing.T, f *fixture, first string) SubmitInput {
			return continueInput(foreign(t, f, first, "tenant-b", "host-a"))
		}, "is not a run this host submitted under its tenant"},
		"another host's run": {func(t *testing.T, f *fixture, first string) SubmitInput {
			return continueInput(foreign(t, f, first, "tenant-a", "host-b"))
		}, "is not a run this host submitted under its tenant"},
		"a record of another run": {func(t *testing.T, f *fixture, first string) SubmitInput {
			other := wire.RunID("tenant-a", "host-a", wire.RunNonce())
			local, _ := f.host.localRun(first)
			path, _ := f.host.runPath(other)
			if err := writeState(path, *local); err != nil {
				t.Fatal(err)
			}
			return continueInput(other)
		}, "is not a run this host submitted under its tenant"},
		"a task of another run": {func(t *testing.T, f *fixture, first string) SubmitInput {
			rewrite(t, f, first, func(local *runRecord) {
				local.Task.Wherry.Run = wire.RunID("tenant-a", "host-a", local.Task.Wherry.RunNonce+"x")
			})
			return continueInput(first)
		}, "is not a run this host submitted under its tenant"},
		"a run ID its nonce does not derive": {func(t *testing.T, f *fixture, first string) SubmitInput {
			rewrite(t, f, first, func(local *runRecord) { local.Task.Wherry.RunNonce = wire.RunNonce() })
			return continueInput(first)
		}, "is not a run this host submitted under its tenant"},
		"no task": {func(t *testing.T, f *fixture, first string) SubmitInput {
			rewrite(t, f, first, func(local *runRecord) { local.Task = nil })
			return continueInput(first)
		}, "is not a run this host submitted under its tenant"},
		"no bundle collected": {func(t *testing.T, f *fixture, first string) SubmitInput {
			rewrite(t, f, first, func(local *runRecord) { local.Outputs = nil })
			return continueInput(first)
		}, "result has collected no changes.bundle of run "},
		"another output collected, no bundle": {func(t *testing.T, f *fixture, first string) SubmitInput {
			rewrite(t, f, first, func(local *runRecord) { local.Outputs[0].Name = "report.md" })
			return continueInput(first)
		}, "result has collected no changes.bundle of run "},
		"the bundle removed": {func(t *testing.T, f *fixture, first string) SubmitInput {
			os.Remove(recordedBundle(t, f, first).Path)
			return continueInput(first)
		}, "cannot be read"},
		"the bundle a directory": {func(t *testing.T, f *fixture, first string) SubmitInput {
			path := recordedBundle(t, f, first).Path
			if err := os.Remove(path); err != nil || os.Mkdir(path, 0o700) != nil {
				t.Fatal(err)
			}
			return continueInput(first)
		}, "cannot be read"},
		"the bundle a FIFO": {func(t *testing.T, f *fixture, first string) SubmitInput {
			path := recordedBundle(t, f, first).Path
			if err := os.Remove(path); err != nil || syscall.Mkfifo(path, 0o600) != nil {
				t.Fatal(err)
			}
			return continueInput(first)
		}, "cannot be read"},
		"an undecodable record": {func(t *testing.T, f *fixture, first string) SubmitInput {
			path, _ := f.host.runPath(first)
			if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
				t.Fatal(err)
			}
			return continueInput(first)
		}, "'s chain cannot be read"},
		"git": {func(t *testing.T, f *fixture, first string) SubmitInput {
			input := continueInput(first)
			input.Git = &wire.GitInput{Remote: "https://git.example.test/org/repo.git", Ref: "main"}
			return input
		}, "not git or git changes"},
		"git changes": {func(t *testing.T, f *fixture, first string) SubmitInput {
			input := continueInput(first)
			input.GitChanges = "bundle"
			return input
		}, "not git or git changes"},
		"an input named base.bundle": {func(t *testing.T, f *fixture, first string) SubmitInput {
			input := continueInput(first)
			input.Parts = []wire.Part{{Raw: []byte("x"), Filename: "base.bundle"}}
			return input
		}, "base.bundle and changes.bundle are reserved"},
		"an S3 input named changes.bundle": {func(t *testing.T, f *fixture, first string) SubmitInput {
			input := continueInput(first)
			input.S3Inputs = []S3File{{Name: wire.GitBundle, Body: bytes.NewReader([]byte("x"))}}
			return input
		}, "base.bundle and changes.bundle are reserved"},
		"an output named changes.bundle": {func(t *testing.T, f *fixture, first string) SubmitInput {
			input := continueInput(first)
			input.Outputs = []wire.ExpectedOutput{{Name: wire.GitBundle}}
			return input
		}, "base.bundle and changes.bundle are reserved"},
		"a host without a storage credential": {func(t *testing.T, f *fixture, first string) SubmitInput {
			f.host.storage = nil
			return continueInput(first)
		}, "continue needs a pool with storage and a host storage credential"},
	} {
		t.Run(name, func(t *testing.T) {
			f, o, _ := continueFixture(t)
			first := firstRun(t, f, testBundle)
			refused(t, f, o, test.setup(t, f, first), test.want)
		})
	}
}

func TestSubmitRefusesAnExplicitChangesOutputWithAGitInput(t *testing.T) {
	git := &wire.GitInput{Remote: "https://git.example.test/org/repo.git", Ref: "main"}
	for name, test := range map[string]struct {
		changes, output string
		continued       bool
	}{
		"git and changes.bundle":        {"", wire.GitBundle, false},
		"git, patch and changes.patch":  {"patch", wire.GitPatch, false},
		"git, patch and changes.bundle": {"patch", wire.GitBundle, false},
		"git, none and changes.patch":   {"none", wire.GitPatch, false},
		"continue and changes.patch":    {"", wire.GitPatch, true},
	} {
		t.Run(name, func(t *testing.T) {
			f, o, _ := continueFixture(t)
			first := firstRun(t, f, testBundle)
			input := SubmitInput{Selector: defaultSelector(), Prompt: "work", Git: git, GitChanges: test.changes, EndBy: time.Now().Add(30 * time.Minute), StartBy: time.Now().Add(10 * time.Minute)}
			if test.continued {
				input = continueInput(first)
			}
			input.Outputs = []wire.ExpectedOutput{{Name: test.output}}
			refused(t, f, o, input, "the changes file comes back by default with --git; drop --output "+test.output)
		})
	}
	t.Run("no git input", func(t *testing.T) {
		f, _, receiver := continueFixture(t)
		for _, output := range []string{wire.GitBundle, wire.GitPatch} {
			submitted, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Outputs: []wire.ExpectedOutput{{Name: output}}, EndBy: time.Now().Add(30 * time.Minute), StartBy: time.Now().Add(10 * time.Minute)})
			if err != nil {
				t.Fatal(err)
			}
			if task := receiver.claim(t, f.ctx, submitted.RunID)[0].submission; !slices.Equal(task.Wherry.Outputs, []wire.ExpectedOutput{{Name: output}}) {
				t.Fatalf("outputs %+v, want %s as an ordinary output", task.Wherry.Outputs, output)
			}
		}
	})
}

func TestSubmitRefusesABundleWithoutOneBaseAndAHead(t *testing.T) {
	other := strings.Repeat("ef", 20)
	for name, header := range map[string]string{
		"not a bundle":          "PK\x03\x04",
		"two prerequisites":     "# v2 git bundle\n-" + testBase + " one\n-" + other + " two\n" + testHead + " HEAD\n",
		"no prerequisite":       "# v2 git bundle\n" + testHead + " HEAD\n",
		"no HEAD":               "# v2 git bundle\n-" + testBase + " one\n" + testHead + " refs/heads/main\n",
		"a short prerequisite":  "# v2 git bundle\n-" + testBase[:39] + " one\n" + testHead + " HEAD\n",
		"a malformed HEAD":      "# v2 git bundle\n-" + testBase + " one\nfoo HEAD\n",
		"HEAD the prerequisite": "# v2 git bundle\n-" + testBase + " one\n" + testBase + " HEAD\n",
	} {
		t.Run(name, func(t *testing.T) {
			f, o, _ := continueFixture(t)
			refused(t, f, o, continueInput(firstRun(t, f, bundleBytes(header))), "the changes.bundle collected for run")
		})
	}
	t.Run("no header end within 64 KiB", func(t *testing.T) {
		f, o, _ := continueFixture(t)
		long := []byte("# v2 git bundle\n-" + testBase + " " + strings.Repeat("x", maxBundleHeader) + "\n" + testHead + " HEAD\n\n")
		refused(t, f, o, continueInput(firstRun(t, f, long)), "the changes.bundle collected for run")
	})
}

func TestBundleHeaderReadsV2AndV3Headers(t *testing.T) {
	sha256Base, sha256Head := strings.Repeat("ab", 32), strings.Repeat("cd", 32)
	for name, test := range map[string]struct {
		header, base, head string
	}{
		"v2":                {"# v2 git bundle\n-" + testBase + " subject\n" + testHead + " HEAD\n", testBase, testHead},
		"v2 empty subject":  {"# v2 git bundle\n-" + testBase + " \n" + testHead + " HEAD\n", testBase, testHead},
		"v2 no subject":     {"# v2 git bundle\n-" + testBase + "\n" + testHead + " HEAD\n", testBase, testHead},
		"v3 object format":  {"# v3 git bundle\n@object-format=sha256\n-" + sha256Base + " subject\n" + sha256Head + " HEAD\n", sha256Base, sha256Head},
		"v3 two capability": {"# v3 git bundle\n@object-format=sha1\n@filter=blob:none\n-" + testBase + " subject\n" + testHead + " HEAD\n", testBase, testHead},
	} {
		t.Run(name, func(t *testing.T) {
			if base, head, err := bundleHeader(bytes.NewReader(bundleBytes(test.header))); err != nil || base != test.base || head != test.head {
				t.Fatalf("bundleHeader = %q, %q, %v; want %q, %q", base, head, err, test.base, test.head)
			}
		})
	}
}

// A pool without storage cannot carry the bundles.
func TestSubmitRefusesAContinuationToAPoolWithoutStorage(t *testing.T) {
	f, o, _ := continueFixture(t)
	first := firstRun(t, f, testBundle)
	f.end(t, "receiver-c")
	terms := testTerms()
	terms.VCS = []string{"git.example.test"}
	if err := f.service.PoolApply(f.ctx, "pool-a", terms, false); err != nil {
		t.Fatal(err)
	}
	f.host.storage["pool-a"] = *o.config
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
	refused(t, f, o, continueInput(first), "continue needs a pool with storage and a host storage credential")
	if end := f.tasksEnd(t, "pool-a"); end != 0 {
		t.Fatalf("%d tasks published to pool-a", end)
	}
}

// A bundle changed after Collect verified it is refused once its upload's digest differs; the upload is deleted and
// nothing is published.
func TestSubmitRefusesABundleChangedAfterCollect(t *testing.T) {
	for name, change := range map[string]func([]byte) []byte{
		"same size": func(data []byte) []byte { return append(slices.Clone(data[:len(data)-1]), 'x') },
		"larger":    func(data []byte) []byte { return append(slices.Clone(data), 'x') },
	} {
		t.Run(name, func(t *testing.T) {
			f, o, _ := continueFixture(t)
			first := firstRun(t, f, testBundle)
			if err := os.WriteFile(recordedBundle(t, f, first).Path, change(testBundle), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := f.host.Submit(f.ctx, continueInput(first)); !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "changed after result verified it") {
				t.Fatalf("submit = %v, want the changed bundle refused", err)
			}
			o.mu.Lock()
			ops := slices.Clone(o.ops)
			o.mu.Unlock()
			if len(ops) != 2 || !strings.HasPrefix(ops[0], "PUT ") || !strings.HasPrefix(ops[1], "DELETE ") || strings.TrimPrefix(ops[0], "PUT ") != strings.TrimPrefix(ops[1], "DELETE ") {
				t.Fatalf("S3 requests %v, want the upload and its deletion", ops)
			}
			if end := f.tasksEnd(t, "pool-c"); end != 1 {
				t.Fatalf("%d tasks published, want the first run's alone", end)
			}
		})
	}
}

// recordedBundle is the changes.bundle run's local record holds among its collected outputs.
func recordedBundle(t *testing.T, f *fixture, run string) collectedOutput {
	t.Helper()
	local, err := f.host.localRun(run)
	if err != nil || local == nil {
		t.Fatalf("local record %v, %v", local, err)
	}
	i := slices.IndexFunc(local.Outputs, func(output collectedOutput) bool { return output.Name == wire.GitBundle })
	if i < 0 {
		t.Fatalf("recorded outputs %+v hold no %s", local.Outputs, wire.GitBundle)
	}
	return local.Outputs[i]
}

// Collect records the name, absolute path, size and SHA-256 of every inline and S3 output in the record of the run it
// collected under, and a later collect's outputs replace the entries of their names, the rest kept.
func TestCollectRecordsTheOutputs(t *testing.T) {
	f, o, receiver := continueFixture(t)
	first := firstRun(t, f, testBundle)
	t.Chdir(t.TempDir())
	inline := bundleBytes("# v2 git bundle\n-" + testBase + " inline\n" + testHead + " HEAD\n")
	report := []byte("report")
	outputs := []Output{{Name: wire.GitBundle, Data: inline, Size: int64(len(inline)), SHA256: hexSum(inline)}, {Name: "notes/report.md", Data: report, Size: int64(len(report)), SHA256: hexSum(report)}}
	if _, err := f.host.Collect(f.ctx, first, AwaitResult{Outputs: outputs}, "relative"); err != nil {
		t.Fatal(err)
	}
	cwd, _ := os.Getwd()
	local, _ := f.host.localRun(first)
	want := []collectedOutput{{Name: wire.GitBundle, Path: filepath.Join(cwd, "relative", wire.GitBundle), Size: int64(len(inline)), SHA256: hexSum(inline)},
		{Name: "notes/report.md", Path: filepath.Join(cwd, "relative", "notes", "report.md"), Size: int64(len(report)), SHA256: hexSum(report)}}
	if !slices.Equal(local.Outputs, want) {
		t.Fatalf("recorded %+v, want %+v", local.Outputs, want)
	}
	if _, err := f.host.Collect(f.ctx, first, AwaitResult{Outputs: outputs[1:]}, "again"); err != nil {
		t.Fatal(err)
	}
	want[1].Path = filepath.Join(cwd, "again", "notes", "report.md")
	if again, _ := f.host.localRun(first); !slices.Equal(again.Outputs, want) {
		t.Fatalf("a later collect left %+v, want %+v: its own output moved and the bundle kept", again.Outputs, want)
	}
	if _, err := f.host.Collect(f.ctx, first, AwaitResult{Outputs: outputs}, "relative"); err != nil {
		t.Fatal(err)
	}
	submitted, err := f.host.Submit(f.ctx, continueInput(first))
	if err != nil {
		t.Fatal(err)
	}
	receiver.claim(t, f.ctx, submitted.RunID)
	stored := []byte("stored bundle")
	key := wire.S3OutputKey("tenant-a", submitted.RunID, wire.GitBundle)
	o.mu.Lock()
	o.objects["/"+testBucket+"/"+key] = stored
	o.mu.Unlock()
	result := AwaitResult{Status: wire.TaskStatus{State: wire.TaskStateWorking}, Stored: []wire.ReturnedOutput{{Name: wire.GitBundle, SHA256: hexSum(stored), Size: int64(len(stored)), Key: key}}}
	if _, err := f.host.Collect(f.ctx, submitted.RunID, result, "s3"); err != nil {
		t.Fatal(err)
	}
	if local, _ := f.host.localRun(submitted.RunID); !slices.Equal(local.Outputs, []collectedOutput{{Name: wire.GitBundle, Path: filepath.Join(cwd, "s3", wire.GitBundle), Size: int64(len(stored)), SHA256: hexSum(stored)}}) {
		t.Fatalf("recorded %+v for the S3 bundle", local.Outputs)
	}
}

// A later result into another directory keeps the recorded outputs it cannot collect again: the run's S3 outputs are
// gone from the bucket once a collect verified them, so the bundle stays recorded where the first collect wrote it, the
// inline output moves to the later directory, and a continuation finds them all.
func TestCollectKeepsTheOutputsALaterCollectCannotVerify(t *testing.T) {
	f, o, receiver := continueFixture(t)
	submitted, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "first round", Git: &wire.GitInput{Remote: "https://git.example.test/org/repo.git", Ref: "main", Path: "src/repo"},
		GitChanges: "bundle", EndBy: time.Now().Add(30 * time.Minute), StartBy: time.Now().Add(10 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	receiver.claim(t, f.ctx, submitted.RunID)
	key := wire.S3OutputKey("tenant-a", submitted.RunID, wire.GitBundle)
	o.mu.Lock()
	o.objects["/"+testBucket+"/"+key] = testBundle
	o.mu.Unlock()
	report := []byte("report")
	result := AwaitResult{Status: wire.TaskStatus{State: wire.TaskStateWorking},
		Outputs: []Output{{Name: "report.md", Data: report, Size: int64(len(report)), SHA256: hexSum(report)}},
		Stored:  []wire.ReturnedOutput{{Name: wire.GitBundle, SHA256: hexSum(testBundle), Size: int64(len(testBundle)), Key: key}}}
	first, second := t.TempDir(), t.TempDir()
	if collected, err := f.host.Collect(f.ctx, submitted.RunID, result, first); err != nil || len(collected.Outputs) != 2 || len(collected.Rejected) != 0 {
		t.Fatalf("first collect %+v, %v; want both outputs", collected, err)
	}
	// What the prefix's deletion does once the run ends and every output verifies.
	o.mu.Lock()
	delete(o.objects, "/"+testBucket+"/"+key)
	o.mu.Unlock()
	collected, err := f.host.Collect(f.ctx, submitted.RunID, result, second)
	if err != nil || len(collected.Outputs) != 1 || len(collected.Rejected) != 1 || collected.Rejected[0].Name != wire.GitBundle {
		t.Fatalf("second collect %+v, %v; want the report alone, the bundle rejected", collected, err)
	}
	want := []collectedOutput{{Name: "report.md", Path: filepath.Join(second, "report.md"), Size: int64(len(report)), SHA256: hexSum(report)},
		{Name: wire.GitBundle, Path: filepath.Join(first, wire.GitBundle), Size: int64(len(testBundle)), SHA256: hexSum(testBundle)}}
	if local, _ := f.host.localRun(submitted.RunID); !slices.Equal(local.Outputs, want) {
		t.Fatalf("recorded %+v, want %+v: the report moved and the bundle kept", local.Outputs, want)
	}
	next, err := f.host.Submit(f.ctx, continueInput(submitted.RunID))
	if err != nil {
		t.Fatalf("continue after the second collect: %v", err)
	}
	task := receiver.claim(t, f.ctx, next.RunID)[0].submission
	if !slices.Equal(task.Wherry.S3Inputs, []wire.S3Input{{Name: "base.bundle", Size: int64(len(testBundle)), SHA256: hexSum(testBundle)}}) || !slices.Equal(task.Message.ReferenceTaskIDs, []string{submitted.RunID}) {
		t.Fatalf("S3 inputs %+v and references %v; want the bundle the first collect wrote", task.Wherry.S3Inputs, task.Message.ReferenceTaskIDs)
	}
}

// Collect records a replaced run's bundle in its final replacement's record, and nothing without an output.
func TestCollectRecordsTheBundleInTheFinalReplacement(t *testing.T) {
	f, _, _ := continueFixture(t)
	submitted, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "first", Git: &wire.GitInput{Remote: "https://git.example.test/org/repo.git", Ref: "main"},
		EndBy: time.Now().Add(30 * time.Minute), StartBy: time.Now().Add(10 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	replacement := replace(t, f, submitted.RunID)
	if _, err := f.host.Collect(f.ctx, submitted.RunID, AwaitResult{Runs: []string{submitted.RunID, replacement}}, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if local, _ := f.host.localRun(replacement); local.Outputs != nil {
		t.Fatalf("recorded %+v without an output", local.Outputs)
	}
	collectBundle(t, f, []string{submitted.RunID, replacement}, testBundle)
	original, _ := f.host.localRun(submitted.RunID)
	if original.Outputs != nil || recordedBundle(t, f, replacement).SHA256 != hexSum(testBundle) {
		t.Fatalf("recorded %+v on the original; want the replacement's record alone to hold the bundle", original.Outputs)
	}
}

// Collect holds the lock of the run a replacement replaces, under which a resubmission writes the replacement's record.
func TestCollectRecordsTheBundleUnderThePredecessorsLock(t *testing.T) {
	f, _, _ := continueFixture(t)
	first := firstRun(t, f, testBundle)
	replacement := replace(t, f, first)
	unlock, err := f.host.lockRun(f.ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	result := AwaitResult{Runs: []string{first, replacement}, Outputs: []Output{{Name: wire.GitBundle, Data: testBundle, Size: int64(len(testBundle)), SHA256: hexSum(testBundle)}}}
	done := make(chan error, 1)
	go func() {
		_, err := f.host.Collect(f.ctx, first, result, dir)
		done <- err
	}()
	select {
	case <-done:
		t.Fatal("Collect recorded the bundle while another process held the predecessor's lock")
	case <-time.After(500 * time.Millisecond):
	}
	unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if recorded := recordedBundle(t, f, replacement); recorded.Path != filepath.Join(dir, wire.GitBundle) {
		t.Fatalf("recorded %+v, want the bundle in %s", recorded, dir)
	}
}

// A collect that cannot record its outputs fails naming the step, since a continuation would not find them, and still
// returns the outputs it wrote, since they are on disk.
func TestCollectFailsWhenItsOutputsAreNotRecorded(t *testing.T) {
	f, _, _ := continueFixture(t)
	first := firstRun(t, f, testBundle)
	unlock, err := f.host.lockRun(f.ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	ctx, cancel := context.WithTimeout(f.ctx, 200*time.Millisecond)
	defer cancel()
	result := AwaitResult{Outputs: []Output{{Name: wire.GitBundle, Data: testBundle, Size: int64(len(testBundle)), SHA256: hexSum(testBundle)}}}
	dir := t.TempDir()
	collected, err := f.host.Collect(ctx, first, result, dir)
	var failed *CollectError
	if !errors.As(err, &failed) || failed.Step != StepRecordOutputs {
		t.Fatalf("collect = %v; want the record outputs step failed", err)
	}
	if len(collected.Outputs) != 1 || collected.Outputs[0].Path != filepath.Join(dir, wire.GitBundle) || collected.Outputs[0].SHA256 != hexSum(testBundle) {
		t.Fatalf("collected %+v with the record failed; want the bundle written at its path", collected.Outputs)
	}
}

// plainRun submits a run without a git input to pool-a, a pool without storage, and has Collect write and record outputs
// for it in a new directory, each file's content its name.
func plainRun(t *testing.T, f *fixture, receiver *testReceiver, names ...string) (string, []Output) {
	t.Helper()
	submitted, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "first round", EndBy: time.Now().Add(30 * time.Minute), StartBy: time.Now().Add(10 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	receiver.claim(t, f.ctx, submitted.RunID)
	var outputs []Output
	for _, name := range names {
		outputs = append(outputs, Output{Name: name, Data: []byte(name), Size: int64(len(name)), SHA256: hexSum([]byte(name))})
	}
	if len(outputs) > 0 {
		if _, err := f.host.Collect(f.ctx, submitted.RunID, AwaitResult{Outputs: outputs}, t.TempDir()); err != nil {
			t.Fatal(err)
		}
	}
	return submitted.RunID, outputs
}

// countingOutputs has openOutput count the outputs submit opens and the bytes it reads from them, and removes each file
// once opened, so a second open by path fails.
func countingOutputs(t *testing.T) (*atomic.Int64, *atomic.Int64) {
	t.Helper()
	previous := openOutput
	t.Cleanup(func() { openOutput = previous })
	var opens, read atomic.Int64
	openOutput = func(path string) (io.ReadSeekCloser, error) {
		file, err := previous(path)
		if err != nil {
			return nil, err
		}
		opens.Add(1)
		if err := os.Remove(path); err != nil {
			t.Error(err)
		}
		return &countingReader{ReadSeekCloser: file, read: &read}, nil
	}
	return &opens, &read
}

type countingReader struct {
	io.ReadSeekCloser
	read *atomic.Int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.ReadSeekCloser.Read(p)
	r.read.Add(int64(n))
	return n, err
}

// A continuation of a run without a git input delivers the outputs result collected as the inputs previous/<name>,
// hashed from the one read of each that sends them, with the previous run's context and a reference to it; no git input,
// bundle or storage is involved, and the chain's third round keeps the first run's context.
func TestSubmitContinuesARunWithoutGit(t *testing.T) {
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	first, outputs := plainRun(t, f, receiver, "report.md", "notes/next.md", wire.ReplyFile)
	opens, read := countingOutputs(t)
	submitted, err := f.host.Submit(f.ctx, continueInput(first))
	if err != nil {
		t.Fatal(err)
	}
	task := receiver.claim(t, f.ctx, submitted.RunID)[0].submission
	var total int64
	for i, output := range outputs {
		if part := task.Message.Parts[i+1]; part.Filename != "previous/"+output.Name || hexSum(part.Raw) != output.SHA256 || int64(len(part.Raw)) != output.Size {
			t.Fatalf("part %d is %s of %d bytes, want previous/%s with the collected bytes", i+1, part.Filename, len(part.Raw), output.Name)
		}
		total += output.Size
	}
	if len(task.Message.Parts) != len(outputs)+1 || task.Wherry.Git != nil || len(task.Wherry.S3Inputs) != 0 || len(task.Wherry.Outputs) != 0 {
		t.Fatalf("task has %d parts, git %+v, S3 inputs %+v and outputs %+v; want the prompt and the previous outputs alone", len(task.Message.Parts), task.Wherry.Git, task.Wherry.S3Inputs, task.Wherry.Outputs)
	}
	if message := task.Message; message.ContextID != first || !slices.Equal(message.ReferenceTaskIDs, []string{first}) {
		t.Fatalf("context %s and references %v, want the first run's context and a reference to it", message.ContextID, message.ReferenceTaskIDs)
	}
	if opens.Load() != int64(len(outputs)) || read.Load() != total {
		t.Fatalf("submit opened %d outputs and read %d bytes, want %d and %d: each output read once", opens.Load(), read.Load(), len(outputs), total)
	}
	next := []Output{{Name: "report.md", Data: []byte("round two"), Size: 9, SHA256: hexSum([]byte("round two"))}}
	if _, err := f.host.Collect(f.ctx, submitted.RunID, AwaitResult{Outputs: next}, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	third, err := f.host.Submit(f.ctx, continueInput(submitted.RunID))
	if err != nil {
		t.Fatal(err)
	}
	task = receiver.claim(t, f.ctx, third.RunID)[0].submission
	if parts := task.Message.Parts; len(parts) != 2 || parts[1].Filename != "previous/report.md" || string(parts[1].Raw) != "round two" || task.Message.ContextID != first || !slices.Equal(task.Message.ReferenceTaskIDs, []string{submitted.RunID}) {
		t.Fatalf("third round's parts %+v, context %s and references %v; want the second round's output, the first run's context and the second run", parts, task.Message.ContextID, task.Message.ReferenceTaskIDs)
	}
}

// A continuation without git is refused, before anything is published: for a run never collected, with the hint; for an
// output removed, replaced by a directory or a FIFO, naming it; for an output changed since result verified it; and
// with an input of the caller's under previous/.
func TestSubmitRefusesAContinuationWithoutGit(t *testing.T) {
	for name, test := range map[string]struct {
		setup func(t *testing.T, f *fixture, first string, outputs []Output) SubmitInput
		want  string
	}{
		"never collected": {func(t *testing.T, f *fixture, first string, outputs []Output) SubmitInput {
			rewrite(t, f, first, func(local *runRecord) { local.Outputs = nil })
			return continueInput(first)
		}, "run " + "%s has no collected outputs; run wherry result %s first"},
		"an output removed": {func(t *testing.T, f *fixture, first string, outputs []Output) SubmitInput {
			if err := os.Remove(outputs[1].Path); err != nil {
				t.Fatal(err)
			}
			return continueInput(first)
		}, "input previous/notes/next.md: the output notes/next.md collected for run %s cannot be read"},
		"an output a directory": {func(t *testing.T, f *fixture, first string, outputs []Output) SubmitInput {
			if err := os.Remove(outputs[0].Path); err != nil || os.Mkdir(outputs[0].Path, 0o700) != nil {
				t.Fatal(err)
			}
			return continueInput(first)
		}, "input previous/report.md: the output report.md collected for run %s cannot be read"},
		"an output a FIFO": {func(t *testing.T, f *fixture, first string, outputs []Output) SubmitInput {
			if err := os.Remove(outputs[0].Path); err != nil || syscall.Mkfifo(outputs[0].Path, 0o600) != nil {
				t.Fatal(err)
			}
			return continueInput(first)
		}, "input previous/report.md: the output report.md collected for run %s cannot be read"},
		"an output changed, same size": {func(t *testing.T, f *fixture, first string, outputs []Output) SubmitInput {
			if err := os.WriteFile(outputs[0].Path, []byte("REPORT.MD"), 0o600); err != nil {
				t.Fatal(err)
			}
			return continueInput(first)
		}, "continue: the report.md collected for run %s changed after result verified it"},
		"an output changed, longer": {func(t *testing.T, f *fixture, first string, outputs []Output) SubmitInput {
			if err := os.WriteFile(outputs[1].Path, []byte("notes/next.md and more"), 0o600); err != nil {
				t.Fatal(err)
			}
			return continueInput(first)
		}, "continue: the notes/next.md collected for run %s changed after result verified it"},
		"an input under previous/": {func(t *testing.T, f *fixture, first string, outputs []Output) SubmitInput {
			input := continueInput(first)
			input.Parts = []wire.Part{{Raw: []byte("x"), Filename: "previous/mine.md"}}
			return input
		}, "input previous/mine.md: names under previous/ are reserved for a continued run's outputs"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			receiver := f.addReceiver(t, "receiver-a", "pool-a")
			receiver.advertise(t, f.ctx, nil)
			first, _ := plainRun(t, f, receiver, "report.md", "notes/next.md")
			local, _ := f.host.localRun(first)
			var outputs []Output
			for _, output := range local.Outputs {
				outputs = append(outputs, Output{Name: output.Name, Path: output.Path})
			}
			want := strings.ReplaceAll(test.want, "%s", first)
			done := make(chan error, 1)
			go func() { _, err := f.host.Submit(f.ctx, test.setup(t, f, first, outputs)); done <- err }()
			var err error
			select {
			case err = <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("submit blocked")
			}
			if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), want) {
				t.Fatalf("submit = %v, want %q", err, want)
			}
			if strings.Contains(err.Error(), f.stateDir) || strings.Contains(err.Error(), os.TempDir()) {
				t.Fatalf("refusal %q names a path", err)
			}
			if end := f.tasksEnd(t, "pool-a"); end != 1 {
				t.Fatalf("%d tasks published, want the first run's alone", end)
			}
		})
	}
}

// A previous output over the pool's record limit goes through the pool's bucket, hashed from the bytes uploaded; one
// changed since result verified it at the same size is refused after its upload, which is deleted, and one of another
// size is refused on its size, before any upload.
func TestSubmitContinuesARunWithoutGitThroughS3(t *testing.T) {
	f, o, receiver := recordLimitFixture(t, true)
	big := make([]byte, 150000)
	rand.Read(big)
	submitted, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "first round", EndBy: time.Now().Add(30 * time.Minute), StartBy: time.Now().Add(10 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	receiver.claim(t, f.ctx, submitted.RunID)
	dir := t.TempDir()
	outputs := []Output{{Name: "big.bin", Data: big, Size: int64(len(big)), SHA256: hexSum(big)}, {Name: "small.txt", Data: []byte("small"), Size: 5, SHA256: hexSum([]byte("small"))}}
	if _, err := f.host.Collect(f.ctx, submitted.RunID, AwaitResult{Outputs: outputs}, dir); err != nil {
		t.Fatal(err)
	}
	next, err := f.host.Submit(f.ctx, continueInput(submitted.RunID))
	if err != nil {
		t.Fatal(err)
	}
	task := receiver.claim(t, f.ctx, next.RunID)[0].submission
	if len(task.Message.Parts) != 2 || task.Message.Parts[1].Filename != "previous/small.txt" || !slices.Equal(task.Wherry.S3Inputs, []wire.S3Input{{Name: "previous/big.bin", Size: int64(len(big)), SHA256: hexSum(big)}}) {
		t.Fatalf("parts %d and S3 inputs %+v; want small.txt inline and big.bin through S3", len(task.Message.Parts), task.Wherry.S3Inputs)
	}
	if got := o.object("/" + testBucket + "/" + wire.S3InputKey("tenant-a", next.RunID, "previous/big.bin")); !bytes.Equal(got, big) {
		t.Fatalf("uploaded %d bytes, want the collected output", len(got))
	}
	changed := slices.Clone(big)
	changed[0]++
	if err := os.WriteFile(filepath.Join(dir, "big.bin"), changed, 0o600); err != nil {
		t.Fatal(err)
	}
	o.mu.Lock()
	o.ops = nil
	o.mu.Unlock()
	if _, err := f.host.Submit(f.ctx, continueInput(submitted.RunID)); !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "the big.bin collected for run "+submitted.RunID+" changed after result verified it") {
		t.Fatalf("submit = %v, want the changed output refused", err)
	}
	o.mu.Lock()
	ops := slices.Clone(o.ops)
	o.mu.Unlock()
	if len(ops) != 2 || !strings.HasPrefix(ops[0], "PUT ") || !strings.HasPrefix(ops[1], "DELETE ") || strings.TrimPrefix(ops[0], "PUT ") != strings.TrimPrefix(ops[1], "DELETE ") {
		t.Fatalf("S3 requests %v, want the upload and its deletion", ops)
	}
	if err := os.WriteFile(filepath.Join(dir, "big.bin"), append(slices.Clone(big), 'x'), 0o600); err != nil {
		t.Fatal(err)
	}
	o.mu.Lock()
	o.ops = nil
	o.mu.Unlock()
	if _, err := f.host.Submit(f.ctx, continueInput(submitted.RunID)); !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "the big.bin collected for run "+submitted.RunID+" changed after result verified it") {
		t.Fatalf("submit = %v, want the longer output refused", err)
	}
	o.mu.Lock()
	ops = slices.Clone(o.ops)
	o.mu.Unlock()
	if len(ops) != 0 {
		t.Fatalf("S3 requests %v, want none for an output of another size", ops)
	}
	if end := f.tasksEnd(t, "pool-r"); end != 2 {
		t.Fatalf("%d tasks published, want the first two alone", end)
	}
}

// A key-lost continuation's replacement keeps its context, its reference, its git base and its recorded repository.
func TestAReplacementKeepsTheContinuation(t *testing.T) {
	f, _, receiver := continueFixture(t)
	first := firstRun(t, f, testBundle)
	rewrite(t, f, first, func(local *runRecord) { local.Repo = "/home/me/src/repo" })
	submitted, err := f.host.Submit(f.ctx, continueInput(first))
	if err != nil {
		t.Fatal(err)
	}
	task := receiver.claim(t, f.ctx, submitted.RunID)[0]
	receiver.keyLost(t, f.ctx, task)
	status, err := f.host.Status(f.ctx, submitted.RunID)
	if err != nil || status.Replacement == "" {
		t.Fatalf("status %+v, %v; want a replacement", status, err)
	}
	if local, err := f.host.localRun(status.Replacement); err != nil || local == nil || local.Repo != "/home/me/src/repo" {
		t.Fatalf("the replacement's record holds repo %q, %v; want the continuation's", local.Repo, err)
	}
	replacement, _, err := open(t, f, status.Replacement, f.poolKeys["pool-c"])
	if err != nil {
		t.Fatal(err)
	}
	message, git := replacement.Message, replacement.Wherry.Git
	if message.TaskID != status.Replacement || message.ContextID != first || !slices.Equal(message.ReferenceTaskIDs, []string{first}) || git == nil || git.Base == nil ||
		*git.Base != (wire.GitBase{Bundle: "base.bundle", Commit: testBase}) || replacement.Wherry.S3InputRun != submitted.RunID {
		t.Fatalf("replacement message %+v and git %+v", message, git)
	}
}
