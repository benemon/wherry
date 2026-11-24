package client

import (
	"bytes"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/wire"
)

// The policy lasts until task_ended_by plus the upload grace: a store enforcing it takes an upload within the grace and
// refuses one past it, and result downloads what was uploaded.
func TestS3OutputsUploadUntilTaskEndedByPlusTheGrace(t *testing.T) {
	f, o, receiver := continueFixture(t)
	submitted, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "do the work", Outputs: []wire.ExpectedOutput{{Name: "report.pdf"}},
		EndBy: time.Now().Add(2 * time.Minute), StartBy: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	runID := submitted.RunID
	task := receiver.claim(t, f.ctx, runID)[0]
	receiver.feed(t, f.ctx, runID, receiver.echo(t, task), receiver.status(t, task, 1, "claimed-1", "claimed", wire.TaskStateWorking, nil))
	_, values, err := open(t, f, runID, f.poolKeys["pool-c"])
	if err != nil {
		t.Fatal(err)
	}
	endBy := time.Unix(task.submission.Wherry.TaskEndedBy, 0)
	if expiration, _ := policyDocument(t, values.S3Policy); !expiration.Equal(endBy.Add(operator.UploadGrace)) {
		t.Fatalf("the policy expires at %v, want task_ended_by plus the grace, %v", expiration, endBy.Add(operator.UploadGrace))
	}
	key := wire.S3OutputKey("tenant-a", runID, "report.pdf")
	upload := func(at time.Time) int {
		o.mu.Lock()
		o.clock = func() time.Time { return at }
		o.mu.Unlock()
		return postUpload(t, &http.Client{Transport: storageTransport}, values.S3Policy, key, []byte("report")).StatusCode
	}
	if status := upload(endBy.Add(operator.UploadGrace + time.Minute)); status != http.StatusForbidden {
		t.Fatalf("POST past the grace answered %d, want forbidden", status)
	}
	if status := upload(endBy.Add(operator.UploadGrace - time.Minute)); status != http.StatusNoContent {
		t.Fatalf("POST within the grace answered %d", status)
	}
	receiver.feed(t, f.ctx, runID, receiver.s3Result(t, task, []wire.ReturnedOutput{{Name: "report.pdf", SHA256: hexSum([]byte("report")), Size: 6, Key: key}})...)
	awaited, err := f.host.Await(f.ctx, runID, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	downloaded, rejected, err := f.host.DownloadOutputs(f.ctx, runID, awaited, dir)
	if err != nil || len(rejected) != 0 || len(downloaded) != 1 || downloaded[0].Path != filepath.Join(dir, "report.pdf") {
		t.Fatalf("downloaded %+v, rejected %+v, %v", downloaded, rejected, err)
	}
	if written, err := os.ReadFile(downloaded[0].Path); err != nil || !bytes.Equal(written, []byte("report")) {
		t.Fatalf("wrote %q, %v", written, err)
	}
}

// The grace lengthens the policy alone: a task_ended_by that has passed, even within the grace, is refused.
func TestPresignRefusesATaskEndedByThatHasPassed(t *testing.T) {
	f, o, _ := continueFixture(t)
	store, err := f.host.bucket(f.ctx, "pool-c", &operator.Storage{Endpoint: o.endpoint, Bucket: testBucket}, newResolver(f.ctx, ""))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for name, deadline := range map[string]time.Time{"just passed": now.Add(-2 * time.Second), "within the grace": now.Add(-operator.UploadGrace / 2)} {
		if _, _, err := presign(f.ctx, store, testBucket, "tenant-a", "run-a", "run-a", nil, now.Add(15*time.Minute), deadline); err == nil || err.Error() != "task_ended_by has passed" {
			t.Fatalf("%s: presign = %v, want task_ended_by has passed", name, err)
		}
	}
	deadline := now.Add(5 * time.Second).Truncate(time.Second)
	_, policy, err := presign(f.ctx, store, testBucket, "tenant-a", "run-a", "run-a", nil, now.Add(15*time.Minute), deadline)
	if err != nil {
		t.Fatal(err)
	}
	if expiration, _ := policyDocument(t, policy); !expiration.Equal(deadline.Add(operator.UploadGrace)) {
		t.Fatalf("the policy expires at %v, want 5s plus the grace", expiration)
	}
}
