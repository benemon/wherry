package client

import (
	"errors"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/wire"
)

// fakeStorageFixture adds pool-s3, with storage at a fake S3 store whose credential the host holds, and a live
// receiver in it alone.
func fakeStorageFixture(t *testing.T) (*fixture, *uploadOracle, *testReceiver) {
	t.Helper()
	o := newUploadOracle(t)
	f := newFixture(t)
	terms := testTerms()
	terms.Storage = &admin.Storage{Endpoint: o.endpoint, Bucket: testBucket}
	f.host.storage = o.config
	maps.Copy(f.host.credentials, o.sources)
	if err := f.service.PoolCreate(f.ctx, "pool-s3", terms); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantAdd(f.ctx, "tenant-a", "pool-s3"); err != nil {
		t.Fatal(err)
	}
	receiver := f.addReceiver(t, "receiver-s3", "pool-s3")
	receiver.advertise(t, f.ctx, nil)
	return f, o, receiver
}

// s3Run is a completed run of pool-s3 whose result lists the S3 outputs a.bin and b.bin, both in the fake store; b.bin
// is listed with tampered's hash in place of its own when tampered is set.
func s3Run(t *testing.T, tampered string) (*fixture, *uploadOracle, string, AwaitResult) {
	t.Helper()
	f, o, receiver := fakeStorageFixture(t)
	submitted, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "do the work", Deadline: time.Minute, QueueDeadline: time.Minute,
		Outputs: []wire.ExpectedOutput{{Name: "a.bin", Storage: wire.StorageS3}, {Name: "b.bin", Storage: wire.StorageS3}}})
	if err != nil {
		t.Fatal(err)
	}
	runID := submitted.RunID
	task := receiver.claim(t, f.ctx, runID)[0]
	receiver.feed(t, f.ctx, runID, receiver.echo(t, task), receiver.status(t, task, 1, "claimed-1", "claimed", wire.TaskStateWorking, nil))
	var outputs []wire.ReturnedOutput
	for _, name := range []string{"a.bin", "b.bin"} {
		data := []byte("bytes of " + name)
		key := wire.S3OutputKey("tenant-a", runID, name)
		o.objects["/"+testBucket+"/"+key] = data
		listed := wire.ReturnedOutput{Name: name, SHA256: hexSum(data), Size: int64(len(data)), Key: key}
		if name == "b.bin" && tampered != "" {
			listed.SHA256 = hexSum([]byte(tampered))
		}
		outputs = append(outputs, listed)
	}
	receiver.feed(t, f.ctx, runID, receiver.s3Result(t, task, outputs)...)
	result, err := f.host.Await(f.ctx, runID, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return f, o, runID, result
}

func TestCollectDeletesS3ObjectsOnlyOnceEveryOutputVerified(t *testing.T) {
	f, o, runID, result := s3Run(t, "")
	dir := t.TempDir()
	collected, err := f.host.Collect(f.ctx, runID, result, dir)
	if err != nil || len(collected.Rejected) != 0 || len(collected.Outputs) != 2 || collected.Outputs[1].Path != filepath.Join(dir, "b.bin") ||
		collected.Transcript.Path != filepath.Join(dir, wire.TranscriptFile) {
		t.Fatalf("collected %+v, %v", collected, err)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	lastGet, firstDelete, deleted := -1, len(o.ops), 0
	for i, op := range o.ops {
		switch {
		case strings.HasPrefix(op, "GET "):
			lastGet = i
		case strings.HasPrefix(op, "DELETE "):
			firstDelete, deleted = min(firstDelete, i), deleted+1
		}
	}
	if lastGet < 0 || deleted != 2 || firstDelete < lastGet || len(o.objects) != 0 {
		t.Fatalf("ops %q, objects left %d; want both outputs read before any is deleted, then both deleted", o.ops, len(o.objects))
	}
}

func TestCollectKeepsS3ObjectsWhenAnOutputDoesNotVerify(t *testing.T) {
	f, o, runID, result := s3Run(t, "forged")
	dir := t.TempDir()
	collected, err := f.host.Collect(f.ctx, runID, result, dir)
	if err != nil || len(collected.Outputs) != 1 || len(collected.Rejected) != 1 || collected.Rejected[0].Name != "b.bin" {
		t.Fatalf("collected %+v, %v; want b.bin rejected", collected, err)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, op := range o.ops {
		if strings.HasPrefix(op, "DELETE ") {
			t.Fatalf("ops %q; want nothing deleted while an output does not verify", o.ops)
		}
	}
	if len(o.objects) != 2 {
		t.Fatalf("%d objects left, want both", len(o.objects))
	}
}

func TestCollectOfAnUnclaimedRunCollectsNothing(t *testing.T) {
	f := newFixture(t)
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
	submitted, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "do the work", Deadline: time.Minute, QueueDeadline: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	result, err := f.host.Await(f.ctx, submitted.RunID, 10*time.Second)
	if err != nil || result.Unclaimed == "" {
		t.Fatalf("await = %+v, %v; want the run unclaimed", result, err)
	}
	result.Outputs = []Output{{Name: "report.md", SHA256: hexSum([]byte("x")), Size: 1, Data: []byte("x")}}
	dir := filepath.Join(t.TempDir(), "outputs")
	collected, err := f.host.Collect(f.ctx, submitted.RunID, result, dir)
	if err != nil || collected.Outputs != nil || collected.Transcript.Path != "" || collected.Lines != nil || collected.Rejected != nil {
		t.Fatalf("collected %+v, %v; want nothing", collected, err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("collect of an unclaimed run made its outputs directory: %v", err)
	}
}

func TestCollectNamesTheStepThatFailed(t *testing.T) {
	t.Run("write outputs", func(t *testing.T) {
		f := newFixture(t)
		receiver, task := f.admitted(t)
		runID := task.submission.Wherry.Run
		receiver.feed(t, f.ctx, runID, receiver.outputChunk(t, task, 1, "report.md", 0, true, "report"),
			receiver.manifestResult(t, task, 1, []wire.ReturnedOutput{listed("report.md", "report")}, nil))
		result, err := f.host.Await(f.ctx, runID, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		dir := t.TempDir()
		if err := os.Mkdir(filepath.Join(dir, "report.md"), 0o700); err != nil {
			t.Fatal(err)
		}
		_, err = f.host.Collect(f.ctx, runID, result, dir)
		if failed := (*CollectError)(nil); !errors.As(err, &failed) || failed.Step != "write outputs" || err.Error() != "write outputs: "+failed.Err.Error() {
			t.Fatalf("collect = %v, want the outputs' write named", err)
		}
	})
	t.Run("S3 outputs", func(t *testing.T) {
		f, _, runID, result := s3Run(t, "")
		f.host.storage = nil
		_, err := f.host.Collect(f.ctx, runID, result, t.TempDir())
		if failed := (*CollectError)(nil); !errors.As(err, &failed) || failed.Step != "S3 outputs" || err.Error() != "S3 outputs: "+failed.Err.Error() {
			t.Fatalf("collect = %v, want the S3 outputs named", err)
		}
	})
	t.Run("write transcript", func(t *testing.T) {
		f := newFixture(t)
		task := f.endedRun(t, 2)
		runID := task.submission.Wherry.Run
		result, err := f.host.Await(f.ctx, runID, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		dir := t.TempDir()
		if err := os.Mkdir(filepath.Join(dir, wire.TranscriptFile), 0o700); err != nil {
			t.Fatal(err)
		}
		_, err = f.host.Collect(f.ctx, runID, result, dir)
		if failed := (*CollectError)(nil); !errors.As(err, &failed) || failed.Step != "write transcript" || err.Error() != "write transcript: "+failed.Err.Error() {
			t.Fatalf("collect = %v, want the transcript's write named", err)
		}
	})
}

// The transcript Collect writes is its lines as ViewLines keeps them, each as AppendLine appends it.
func TestCollectWritesTheTranscriptLines(t *testing.T) {
	f := newFixture(t)
	task := f.endedRun(t, 3)
	runID := task.submission.Wherry.Run
	result, err := f.host.Await(f.ctx, runID, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	collected, err := f.host.Collect(f.ctx, runID, result, "")
	if err != nil {
		t.Fatal(err)
	}
	written, err := os.ReadFile(collected.Transcript.Path)
	if err != nil || string(written) != "line\nline\nline\n" || string(collected.Transcript.Data) != string(written) ||
		!slices.Equal([]int64{collected.Lines[0].Seq, collected.Lines[1].Seq, collected.Lines[2].Seq}, []int64{0, 5, 10}) {
		t.Fatalf("transcript %q, %v; lines %+v", written, err, collected.Lines)
	}
}
