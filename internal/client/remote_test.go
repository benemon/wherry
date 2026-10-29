package client

import (
	"bytes"
	"errors"
	"io"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/wire"
)

// remoteStore is the bucket the remote-form tests run against: the fake store, or the real one WHERRY_S3_TEST_ENDPOINT
// names.
type remoteStore struct {
	f        *fixture
	receiver *testReceiver
	object   func(key string) []byte
}

func remoteStores(t *testing.T, test func(t *testing.T, s remoteStore)) {
	t.Run("fake", func(t *testing.T) {
		o := newUploadOracle(t)
		f := newFixture(t)
		terms := testTerms()
		terms.Storage = &admin.Storage{Endpoint: o.endpoint, Bucket: testBucket}
		if err := f.service.PoolCreate(f.ctx, "pool-s3", terms); err != nil {
			t.Fatal(err)
		}
		if err := f.service.TenantAdd(f.ctx, "tenant-a", "pool-s3"); err != nil {
			t.Fatal(err)
		}
		f.host.storage = o.config
		maps.Copy(f.host.credentials, o.sources)
		receiver := f.addReceiver(t, "receiver-s3", "pool-s3")
		receiver.advertise(t, f.ctx, nil)
		test(t, remoteStore{f, receiver, func(key string) []byte { return o.object("/" + testBucket + "/" + key) }})
	})
	t.Run("real", func(t *testing.T) {
		f, o, receiver := storageFixture(t)
		test(t, remoteStore{f, receiver, func(key string) []byte { return o.object(t, key) }})
	})
}

func putURL(t *testing.T, url string, data []byte) {
	t.Helper()
	request, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	response, err := (&http.Client{Transport: storageTransport}).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("PUT: %s", response.Status)
	}
}

// An upload is delivered as an S3 input only when the bytes put match the size and SHA-256 it was presigned for.
func TestUploadIsVerifiedBeforeSubmit(t *testing.T) {
	remoteStores(t, func(t *testing.T, s remoteStore) {
		f := s.f
		data := bytes.Repeat([]byte("dataset "), 1000)
		for name, args := range map[string][]any{
			"sha256": {"data.bin", int64(len(data)), strings.ToUpper(hexSum(data))},
			"name":   {"../data.bin", int64(len(data)), hexSum(data)},
			"size":   {"data.bin", int64(wire.MaxS3Object + 1), hexSum(data)},
		} {
			if _, err := f.host.PresignUpload(f.ctx, args[0].(string), args[1].(int64), args[2].(string), time.Hour); !errors.Is(err, ErrInvalidInput) {
				t.Errorf("%s: %v, want invalid input", name, err)
			}
		}
		upload, err := f.host.PresignUpload(f.ctx, "data.bin", int64(len(data)), hexSum(data), time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(upload.URL, wire.S3InputKey("tenant-a", upload.ID, "data.bin")) || time.Until(upload.Expires) < 59*time.Minute {
			t.Fatalf("upload %+v", upload)
		}
		if _, err := f.host.Uploaded(f.ctx, upload.ID); err == nil || !strings.Contains(err.Error(), "nothing was put") {
			t.Fatalf("before the PUT: %v", err)
		}
		forged := bytes.Clone(data)
		forged[0] = 'D'
		putURL(t, upload.URL, forged)
		if _, err := f.host.Uploaded(f.ctx, upload.ID); err == nil || !strings.Contains(err.Error(), "does not match its size and sha256") {
			t.Fatalf("same size, other bytes: %v", err)
		}
		putURL(t, upload.URL, data[1:])
		if _, err := f.host.Uploaded(f.ctx, upload.ID); err == nil || !strings.Contains(err.Error(), "does not match its size and sha256") {
			t.Fatalf("short: %v", err)
		}
		putURL(t, upload.URL, data)
		file, err := f.host.Uploaded(f.ctx, upload.ID)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Body.(io.Closer).Close()
		// A PUT after the check does not reach the task.
		putURL(t, upload.URL, forged)
		result, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", S3Inputs: []S3File{file}, Deadline: time.Minute, QueueDeadline: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		task := s.receiver.claim(t, f.ctx, result.RunID)[0].submission
		if !slices.Equal(task.Wherry.S3Inputs, []wire.S3Input{{Name: "data.bin", Size: int64(len(data)), SHA256: hexSum(data)}}) {
			t.Fatalf("S3 inputs %+v", task.Wherry.S3Inputs)
		}
		if got := s.object(wire.S3InputKey("tenant-a", result.RunID, "data.bin")); !bytes.Equal(got, data) {
			t.Fatalf("the run's input holds %d bytes, want the verified ones", len(got))
		}
		if err := f.host.DeleteUpload(f.ctx, upload.ID); err != nil {
			t.Fatal(err)
		}
		if got := s.object(wire.S3InputKey("tenant-a", upload.ID, "data.bin")); got != nil {
			t.Fatal("the upload's object survived its deletion")
		}
		if _, err := os.Stat(filepath.Join(f.stateDir, "uploads", upload.ID+".json")); !os.IsNotExist(err) {
			t.Fatalf("the upload's record: %v", err)
		}
		if _, err := f.host.Uploaded(f.ctx, upload.ID); err == nil || !strings.Contains(err.Error(), "not one this host issued") {
			t.Fatalf("a deleted upload: %v", err)
		}
	})
}

// Without a pool with storage or a host storage credential, upload refuses as S3 inputs do.
func TestUploadNeedsStorage(t *testing.T) {
	f := newFixture(t)
	if _, err := f.host.PresignUpload(f.ctx, "data.bin", 1, hexSum([]byte("x")), time.Hour); err == nil || !strings.Contains(err.Error(), "no storage credential") {
		t.Fatalf("upload: %v", err)
	}
}

// An output's presigned GET returns its bytes from a key under the run's prefix that no receiver's PUT URL names.
func TestPresignOutput(t *testing.T) {
	remoteStores(t, func(t *testing.T, s remoteStore) {
		f := s.f
		result, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Deadline: time.Minute, QueueDeadline: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		data := bytes.Repeat([]byte{0xff, 'o'}, 3000)
		if _, err := f.host.WriteOutputs(result.RunID, "", []Output{{Name: "out/big.bin", Data: data}}); err != nil {
			t.Fatal(err)
		}
		url, expires, err := f.host.PresignOutput(f.ctx, result.RunID, "out/big.bin", 2*time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		key := wire.S3Prefix("tenant-a", result.RunID) + "host/out/big.bin"
		if !strings.Contains(url, key) || !strings.Contains(url, "X-Amz-Expires=7200") || time.Until(expires) < 119*time.Minute {
			t.Fatalf("url %s expiring %v; want the host key for two hours", url, expires)
		}
		response, err := (&http.Client{Transport: storageTransport}).Get(url)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("GET: %d bytes, %v", len(got), err)
		}
		if _, _, err := f.host.PresignOutput(f.ctx, result.RunID, "missing.bin", time.Hour); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("a missing output: %v", err)
		}
	})
}

func TestPresignOutputWithoutStorage(t *testing.T) {
	f := newFixture(t)
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
	result, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Deadline: time.Minute, QueueDeadline: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.host.WriteOutputs(result.RunID, "", []Output{{Name: "big.bin", Data: []byte("big")}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.host.PresignOutput(f.ctx, result.RunID, "big.bin", time.Hour); !errors.Is(err, ErrNoStorage) || !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("presign without storage: %v", err)
	}
}

// OpenOutput opens files beneath the run's output directory alone; RemoveOutputs deletes them.
func TestOpenAndRemoveOutputs(t *testing.T) {
	f := newFixture(t)
	if _, err := f.host.WriteOutputs("run-a", "", []Output{{Name: "report.md", Data: []byte("report")}}); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(outside, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(f.stateDir, "runs", "run-a", "outputs", "link")); err != nil {
		t.Fatal(err)
	}
	file, err := f.host.OpenOutput("run-a", "report.md")
	if err != nil {
		t.Fatal(err)
	}
	if data, err := io.ReadAll(file); err != nil || string(data) != "report" {
		t.Fatalf("read %q, %v", data, err)
	}
	file.Close()
	for _, args := range [][2]string{{"run-a", "link"}, {"run-a", "../run-a.json"}, {"run-a", "."}, {"../runs/run-a", "report.md"}, {"run-a", "missing"}} {
		if file, err := f.host.OpenOutput(args[0], args[1]); err == nil {
			file.Close()
			t.Errorf("%v: opened", args)
		}
	}
	if err := f.host.RemoveOutputs("run-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(f.stateDir, "runs", "run-a", "outputs")); !os.IsNotExist(err) {
		t.Fatalf("outputs survived: %v", err)
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatalf("the link's target: %v", err)
	}
	if err := f.host.RemoveOutputs("../runs"); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("remove outside the runs: %v", err)
	}
}
