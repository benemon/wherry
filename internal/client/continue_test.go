package client

import (
	"bytes"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
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
// the collected bundle as base.bundle through S3 and asks for its own through S3, and joins the previous run's context.
func TestSubmitContinuesARun(t *testing.T) {
	f, o, receiver := continueFixture(t)
	first := firstRun(t, f, testBundle)
	// The git configuration changed after the first run: the clone takes the current one's credential.
	f.host.credentials["forge-token"] = credential.Source{File: f.secret}
	f.host.git = []GitCredential{{URL: "https://git.example.test/", Credential: "forge-token"}}
	submitted, err := f.host.Submit(f.ctx, continueInput(first))
	if err != nil {
		t.Fatal(err)
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
	if !slices.Equal(task.Wherry.Outputs, []wire.ExpectedOutput{{Name: wire.GitBundle, Storage: wire.StorageS3}}) {
		t.Fatalf("outputs %+v, want changes.bundle through S3, not required", task.Wherry.Outputs)
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
	local.Replacement, local.ReplacementNonce, local.Bundle = replacement.Run, nonce, nil
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
		"no git": {func(t *testing.T, f *fixture, first string) SubmitInput {
			rewrite(t, f, first, func(local *runRecord) { local.Task.Wherry.Git = nil })
			return continueInput(first)
		}, "has no git input"},
		"no bundle collected": {func(t *testing.T, f *fixture, first string) SubmitInput {
			rewrite(t, f, first, func(local *runRecord) { local.Bundle = nil })
			return continueInput(first)
		}, "wait has collected no changes.bundle of run "},
		"the bundle removed": {func(t *testing.T, f *fixture, first string) SubmitInput {
			local, _ := f.host.localRun(first)
			os.Remove(local.Bundle.Path)
			return continueInput(first)
		}, "cannot be read"},
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
			input.Outputs = []wire.ExpectedOutput{{Name: wire.GitBundle, Storage: wire.StorageS3}}
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

func TestSubmitRefusesABundleWithoutOneBaseAndAHead(t *testing.T) {
	other := strings.Repeat("ef", 20)
	for name, header := range map[string]string{
		"not a bundle":         "PK\x03\x04",
		"two prerequisites":    "# v2 git bundle\n-" + testBase + " one\n-" + other + " two\n" + testHead + " HEAD\n",
		"no prerequisite":      "# v2 git bundle\n" + testHead + " HEAD\n",
		"no HEAD":              "# v2 git bundle\n-" + testBase + " one\n" + testHead + " refs/heads/main\n",
		"a short prerequisite": "# v2 git bundle\n-" + testBase[:39] + " one\n" + testHead + " HEAD\n",
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

func TestBundleBaseReadsV2AndV3Headers(t *testing.T) {
	sha256Base, sha256Head := strings.Repeat("ab", 32), strings.Repeat("cd", 32)
	for name, test := range map[string]struct {
		header, want string
	}{
		"v2":                {"# v2 git bundle\n-" + testBase + " subject\n" + testHead + " HEAD\n", testBase},
		"v2 empty subject":  {"# v2 git bundle\n-" + testBase + " \n" + testHead + " HEAD\n", testBase},
		"v2 no subject":     {"# v2 git bundle\n-" + testBase + "\n" + testHead + " HEAD\n", testBase},
		"v3 object format":  {"# v3 git bundle\n@object-format=sha256\n-" + sha256Base + " subject\n" + sha256Head + " HEAD\n", sha256Base},
		"v3 two capability": {"# v3 git bundle\n@object-format=sha1\n@filter=blob:none\n-" + testBase + " subject\n" + testHead + " HEAD\n", testBase},
	} {
		t.Run(name, func(t *testing.T) {
			if got, err := bundleBase(bytes.NewReader(bundleBytes(test.header))); err != nil || got != test.want {
				t.Fatalf("bundleBase = %q, %v; want %q", got, err, test.want)
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
			local, _ := f.host.localRun(first)
			if err := os.WriteFile(local.Bundle.Path, change(testBundle), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := f.host.Submit(f.ctx, continueInput(first)); !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "changed after wait verified it") {
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

// Collect records the absolute path, size and SHA-256 of an inline or S3 changes.bundle in the record of the run it
// collected under, and keeps it when a later collect has none.
func TestCollectRecordsTheBundle(t *testing.T) {
	f, o, receiver := continueFixture(t)
	first := firstRun(t, f, testBundle)
	t.Chdir(t.TempDir())
	inline := bundleBytes("# v2 git bundle\n-" + testBase + " inline\n" + testHead + " HEAD\n")
	if _, err := f.host.Collect(f.ctx, first, AwaitResult{Outputs: []Output{{Name: wire.GitBundle, Data: inline, Size: int64(len(inline)), SHA256: hexSum(inline)}}}, "relative"); err != nil {
		t.Fatal(err)
	}
	cwd, _ := os.Getwd()
	local, _ := f.host.localRun(first)
	if want := (collectedBundle{Path: filepath.Join(cwd, "relative", wire.GitBundle), Size: int64(len(inline)), SHA256: hexSum(inline)}); local.Bundle == nil || *local.Bundle != want {
		t.Fatalf("recorded %+v, want %+v", local.Bundle, want)
	}
	report := []byte("report")
	if _, err := f.host.Collect(f.ctx, first, AwaitResult{Outputs: []Output{{Name: "report.md", Data: report, Size: int64(len(report)), SHA256: hexSum(report)}}}, "again"); err != nil {
		t.Fatal(err)
	}
	if kept, _ := f.host.localRun(first); kept.Bundle == nil || *kept.Bundle != *local.Bundle {
		t.Fatalf("a collect without a bundle left %+v, want %+v", kept.Bundle, local.Bundle)
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
	if local, _ := f.host.localRun(submitted.RunID); local.Bundle == nil || *local.Bundle != (collectedBundle{Path: filepath.Join(cwd, "s3", wire.GitBundle), Size: int64(len(stored)), SHA256: hexSum(stored)}) {
		t.Fatalf("recorded %+v for the S3 bundle", local.Bundle)
	}
}

// Collect records a replaced run's bundle in its final replacement's record, and nothing without a bundle.
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
	if local, _ := f.host.localRun(replacement); local.Bundle != nil {
		t.Fatalf("recorded %+v without a bundle", local.Bundle)
	}
	collectBundle(t, f, []string{submitted.RunID, replacement}, testBundle)
	original, _ := f.host.localRun(submitted.RunID)
	last, _ := f.host.localRun(replacement)
	if original.Bundle != nil || last.Bundle == nil || last.Bundle.SHA256 != hexSum(testBundle) {
		t.Fatalf("recorded %+v on the original and %+v on the replacement; want the replacement's alone", original.Bundle, last.Bundle)
	}
}

// A key-lost continuation's replacement keeps its context, its reference and its git base.
func TestAReplacementKeepsTheContinuation(t *testing.T) {
	f, _, receiver := continueFixture(t)
	first := firstRun(t, f, testBundle)
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
