package client

import (
	"bytes"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
)

// lifetime is the X-Amz-Expires of a presigned URL.
func lifetime(t *testing.T, raw string) time.Duration {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	seconds, err := strconv.Atoi(parsed.Query().Get("X-Amz-Expires"))
	if err != nil {
		t.Fatalf("%s: no X-Amz-Expires", raw)
	}
	return time.Duration(seconds) * time.Second
}

// A PUT URL lasts until task_ended_by plus the upload grace: a store enforcing it takes an upload within the grace and
// refuses one past it, and result downloads what was uploaded.
func TestS3OutputsUploadUntilTaskEndedByPlusTheGrace(t *testing.T) {
	f, o, receiver := continueFixture(t)
	submitted, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "do the work", Outputs: []wire.ExpectedOutput{{Name: "report.pdf", Storage: wire.StorageS3}},
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
	put := values.S3Outputs["report.pdf"]
	endBy := time.Unix(task.submission.Wherry.TaskEndedBy, 0)
	if want := time.Until(endBy) + operator.UploadGrace; lifetime(t, put) < want-10*time.Second || lifetime(t, put) > want+2*time.Second {
		t.Fatalf("PUT lives %v, want task_ended_by plus the grace, %v away", lifetime(t, put), want)
	}
	upload := func(at time.Time) int {
		o.mu.Lock()
		o.clock = func() time.Time { return at }
		o.mu.Unlock()
		request, _ := http.NewRequest(http.MethodPut, put, strings.NewReader("report"))
		response, err := (&http.Client{Transport: storageTransport}).Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		return response.StatusCode
	}
	if status := upload(endBy.Add(operator.UploadGrace + time.Minute)); status != http.StatusForbidden {
		t.Fatalf("PUT past the grace answered %d, want forbidden", status)
	}
	if status := upload(endBy.Add(operator.UploadGrace - time.Minute)); status != http.StatusOK {
		t.Fatalf("PUT within the grace answered %d", status)
	}
	key := wire.S3OutputKey("tenant-a", runID, "report.pdf")
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

// The grace lengthens the URL alone: a task_ended_by that has passed, even within the grace, is refused.
func TestPresignRefusesATaskEndedByThatHasPassed(t *testing.T) {
	f, o, _ := continueFixture(t)
	store, err := f.host.bucket(f.ctx, "pool-c", &operator.Storage{Endpoint: o.endpoint, Bucket: testBucket}, newResolver(f.ctx, ""))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for name, deadline := range map[string]time.Time{"just passed": now.Add(-2 * time.Second), "within the grace": now.Add(-operator.UploadGrace / 2)} {
		if _, err := presign(f.ctx, store, testBucket, "tenant-a", "run-a", "run-a", nil, []string{"report.pdf"}, now.Add(15*time.Minute), deadline); err == nil || err.Error() != "task_ended_by has passed" {
			t.Fatalf("%s: presign = %v, want task_ended_by has passed", name, err)
		}
	}
	urls, err := presign(f.ctx, store, testBucket, "tenant-a", "run-a", "run-a", nil, []string{"report.pdf"}, now.Add(15*time.Minute), now.Add(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if got := lifetime(t, string(urls[signed.S3OutputValue("report.pdf")])); got < operator.UploadGrace+3*time.Second || got > operator.UploadGrace+6*time.Second {
		t.Fatalf("PUT lives %v, want 5s plus the grace", got)
	}
}
