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
