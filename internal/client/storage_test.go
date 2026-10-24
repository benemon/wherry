package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/twmb/franz-go/pkg/kadm"
)

const testBucket = "wherry-test"

// oracle is the real S3 store the storage tests run against: the S3-compatible server WHERRY_S3_TEST_ENDPOINT names,
// a local MinIO over http, fronted by a TLS proxy that keeps the Host header, so the client and the URLs it presigns
// are https as production requires. store reaches the server directly, as an operator would.
type oracle struct {
	endpoint string
	config   *StorageConfig
	store    *minio.Client
	client   *http.Client
	secret   string
}

type uploadOracle struct {
	endpoint string
	config   *StorageConfig
	mu       sync.Mutex
	objects  map[string][]byte
}

func newUploadOracle(t *testing.T) *uploadOracle {
	t.Helper()
	o := &uploadOracle{objects: map[string][]byte{}}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Has("location") {
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprint(w, "<LocationConstraint></LocationConstraint>")
			return
		}
		if r.Method != http.MethodPut {
			http.Error(w, "method", http.StatusMethodNotAllowed)
			return
		}
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "body", http.StatusBadRequest)
			return
		}
		o.mu.Lock()
		o.objects[r.URL.Path] = data
		o.mu.Unlock()
		w.Header().Set("ETag", `"test"`)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	previous := storageTransport
	storageTransport = server.Client().Transport
	t.Cleanup(func() { storageTransport = previous })
	access, secret := filepath.Join(t.TempDir(), "access"), filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(access, []byte("access"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secret, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	o.endpoint, o.config = server.URL, &StorageConfig{AccessKeyFile: access, SecretKeyFile: secret}
	return o
}

func (o *uploadOracle) object(path string) []byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	return slices.Clone(o.objects[path])
}

func newOracle(t *testing.T) *oracle {
	t.Helper()
	target := os.Getenv("WHERRY_S3_TEST_ENDPOINT")
	if target == "" {
		t.Skip("WHERRY_S3_TEST_ENDPOINT not set")
	}
	config := &StorageConfig{AccessKeyFile: os.Getenv("WHERRY_S3_TEST_ACCESS_KEY_FILE"), SecretKeyFile: os.Getenv("WHERRY_S3_TEST_SECRET_KEY_FILE")}
	access, err := readKey(config.AccessKeyFile)
	if err != nil {
		t.Fatal(err)
	}
	secret, err := readKey(config.SecretKeyFile)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(target)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var requested []string
	proxy := httptest.NewTLSServer(&httputil.ReverseProxy{Rewrite: func(r *httputil.ProxyRequest) {
		mu.Lock()
		requested = append(requested, r.In.URL.String())
		mu.Unlock()
		r.SetURL(parsed)
		r.Out.Host = r.In.Host
	}})
	t.Cleanup(proxy.Close)
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		for _, requested := range requested {
			if strings.Contains(requested, secret) || strings.Contains(requested, url.QueryEscape(secret)) {
				t.Errorf("a request URL holds the secret key: %s", requested)
			}
		}
	})
	previous := storageTransport
	storageTransport = proxy.Client().Transport
	t.Cleanup(func() { storageTransport = previous })
	store, err := minio.New(parsed.Host, &minio.Options{Creds: credentials.NewStaticV4(access, secret, ""), BucketLookup: minio.BucketLookupPath})
	if err != nil {
		t.Fatal(err)
	}
	if exists, err := store.BucketExists(context.Background(), testBucket); err != nil {
		t.Fatal(err)
	} else if !exists {
		if err := store.MakeBucket(context.Background(), testBucket, minio.MakeBucketOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	client := proxy.Client()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &oracle{endpoint: proxy.URL, config: config, store: store, client: client, secret: secret}
}

// object is the content of key in the test bucket, or nil when it does not exist.
func (o *oracle) object(t *testing.T, key string) []byte {
	t.Helper()
	object, err := o.store.GetObject(context.Background(), testBucket, key, minio.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer object.Close()
	data, err := io.ReadAll(object)
	var response minio.ErrorResponse
	if errors.As(err, &response) && response.Code == "NoSuchKey" {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func (o *oracle) put(t *testing.T, key string, data []byte) {
	t.Helper()
	if _, err := o.store.PutObject(context.Background(), testBucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{}); err != nil {
		t.Fatal(err)
	}
}

// storageFixture adds pool-s3, whose terms name the oracle's endpoint and bucket, and a live receiver in it alone, and
// gives the host the oracle's credential.
func storageFixture(t *testing.T) (*fixture, *oracle, *testReceiver) {
	t.Helper()
	o := newOracle(t)
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
	receiver := f.addReceiver(t, "receiver-s3", "pool-s3")
	receiver.advertise(t, f.ctx, nil)
	return f, o, receiver
}

func randomFile(t *testing.T, name string, size int) (*os.File, []byte) {
	t.Helper()
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { file.Close() })
	return file, data
}

func hexSum(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// submitS3 submits a task with a 5 MiB S3 input and one required S3 output through receiver, which claims it and
// writes its echo and claimed status.
func submitS3(t *testing.T, f *fixture, receiver *testReceiver, deadline time.Duration) (claimed, []byte) {
	t.Helper()
	file, data := randomFile(t, "dataset.bin", 5<<20)
	submitted, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "do the work", S3Inputs: []S3File{{Name: "dataset.bin", Body: file}},
		Outputs: []wire.ExpectedOutput{{Name: "out/report.pdf", Required: true, Storage: wire.StorageS3}}, Deadline: deadline, QueueDeadline: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if submitted.Pool != "pool-s3" {
		t.Fatalf("placed in %s", submitted.Pool)
	}
	task := receiver.claim(t, f.ctx, submitted.RunID)[0]
	receiver.feed(t, f.ctx, submitted.RunID, receiver.echo(t, task), receiver.status(t, task, 1, "claimed-1", "claimed", wire.TaskStateWorking, nil))
	return task, data
}

func TestSubmitUploadsS3InputsBeforePublishing(t *testing.T) {
	f, o, receiver := storageFixture(t)
	task, data := submitS3(t, f, receiver, time.Minute)
	runID := task.submission.Wherry.Run
	if got := o.object(t, wire.S3InputKey("tenant-a", runID, "dataset.bin")); !bytes.Equal(got, data) {
		t.Fatalf("bucket holds %d bytes at the input's key; want the file's %d", len(got), len(data))
	}
	if !slices.Equal(task.submission.Wherry.S3Inputs, []wire.S3Input{{Name: "dataset.bin", Size: int64(len(data)), SHA256: hexSum(data)}}) {
		t.Fatalf("submission's S3 inputs %+v", task.submission.Wherry.S3Inputs)
	}
	// The task carries the inputs' coordinates and no URL.
	records, err := admin.ReadTopic(f.ctx, receiver.kafka, admin.PoolTasksTopic("pool-s3"))
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		payload := payloadOf(t, record.Value)
		if bytes.Contains(payload, []byte("X-Amz")) || bytes.Contains(payload, []byte(o.endpoint)) {
			t.Fatalf("the task record holds a URL: %s", payload)
		}
	}
}

func TestSubmitMovesInputsOverSmallestAdapterLimitToS3(t *testing.T) {
	o := newUploadOracle(t)
	f := newFixture(t)
	terms := testTerms()
	terms.Storage = &admin.Storage{Endpoint: o.endpoint, Bucket: testBucket}
	if err := f.service.PoolCreate(f.ctx, "pool-inline", terms); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantAdd(f.ctx, "tenant-a", "pool-inline"); err != nil {
		t.Fatal(err)
	}
	f.host.storage = o.config
	receiver := f.addReceiver(t, "receiver-a", "pool-inline")
	receiver.advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.Offers[0].InlineLimit = 1000 })
	f.addReceiver(t, "receiver-b", "pool-inline").advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.Offers[0].InlineLimit = 5000 })
	data := bytes.Repeat([]byte("x"), 2000)
	result, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Parts: []wire.Part{{Raw: data, Filename: "data.bin"}}, Deadline: time.Minute, QueueDeadline: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	task := receiver.claim(t, f.ctx, result.RunID)[0].submission
	if len(task.Message.Parts) != 1 || !slices.Equal(task.Wherry.S3Inputs, []wire.S3Input{{Name: "data.bin", Size: 2000, SHA256: hexSum(data)}}) {
		t.Fatalf("submission has %d parts and S3 inputs %+v; want only an S3 input", len(task.Message.Parts), task.Wherry.S3Inputs)
	}
	path := "/" + testBucket + "/" + wire.S3InputKey("tenant-a", result.RunID, "data.bin")
	if got := o.object(path); !bytes.Equal(got, data) {
		t.Fatalf("uploaded input is %d bytes, want %d", len(got), len(data))
	}
}

// recordLimitFixture adds pool-r, with storage at a fake S3 store whose credential the host holds when stored is true,
// whose tasks topic carries records of at most 100000 bytes, and a live receiver in it alone.
func recordLimitFixture(t *testing.T, stored bool) (*fixture, *uploadOracle, *testReceiver) {
	t.Helper()
	o := newUploadOracle(t)
	f := newFixture(t)
	terms := testTerms()
	if stored {
		terms.Storage = &admin.Storage{Endpoint: o.endpoint, Bucket: testBucket}
		f.host.storage = o.config
	}
	if err := f.service.PoolCreate(f.ctx, "pool-r", terms); err != nil {
		t.Fatal(err)
	}
	limit := "100000"
	if responses, err := kadm.NewClient(f.service.Admin).AlterTopicConfigs(f.ctx, []kadm.AlterConfig{{Op: kadm.SetConfig, Name: "max.message.bytes", Value: &limit}}, admin.PoolTasksTopic("pool-r")); err != nil || responses[0].Err != nil {
		t.Fatalf("alter topic: %v %v", err, responses)
	}
	if err := f.service.PoolCreate(f.ctx, "pool-r", terms); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantAdd(f.ctx, "tenant-a", "pool-r"); err != nil {
		t.Fatal(err)
	}
	receiver := f.addReceiver(t, "receiver-r", "pool-r")
	receiver.advertise(t, f.ctx, nil)
	return f, o, receiver
}

func TestSubmitMovesInputsOverTheRecordLimitToS3(t *testing.T) {
	f, o, receiver := recordLimitFixture(t, true)
	data := make([]byte, 60000)
	rand.Read(data)
	result, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Parts: []wire.Part{{Raw: data, Filename: "data.bin"}}, Deadline: time.Minute, QueueDeadline: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	task := receiver.claim(t, f.ctx, result.RunID)[0].submission
	if len(task.Message.Parts) != 1 || !slices.Equal(task.Wherry.S3Inputs, []wire.S3Input{{Name: "data.bin", Size: 60000, SHA256: hexSum(data)}}) {
		t.Fatalf("submission has %d parts and S3 inputs %+v; want only an S3 input", len(task.Message.Parts), task.Wherry.S3Inputs)
	}
	if got := o.object("/" + testBucket + "/" + wire.S3InputKey("tenant-a", result.RunID, "data.bin")); !bytes.Equal(got, data) {
		t.Fatalf("uploaded input is %d bytes, want %d", len(got), len(data))
	}
}

func TestSubmitRefusesARecordOverTheLimitWithoutStorage(t *testing.T) {
	f, _, _ := recordLimitFixture(t, false)
	_, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Parts: []wire.Part{{Raw: make([]byte, 60000), Filename: "data.bin"}}, Deadline: time.Minute, QueueDeadline: time.Minute})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "-byte record, over pool pool-r's 100000-byte record limit") {
		t.Fatalf("submit = %v; want the record size and the record limit named", err)
	}
	if n := f.tasksEnd(t, "pool-r"); n != 0 {
		t.Fatalf("pool tasks hold %d records after the refused submission", n)
	}
}

func TestSubmitRefusesAPromptOverTheRecordLimit(t *testing.T) {
	f, _, _ := recordLimitFixture(t, true)
	_, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: strings.Repeat("p", 80000), Parts: []wire.Part{{Raw: []byte("x"), Filename: "data.bin"}}, Deadline: time.Minute, QueueDeadline: time.Minute})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "the 80000-byte prompt makes a ") || !strings.Contains(err.Error(), "over pool pool-r's 100000-byte record limit") {
		t.Fatalf("submit = %v; want the prompt and the record limit named", err)
	}
	if n := f.tasksEnd(t, "pool-r"); n != 0 {
		t.Fatalf("pool tasks hold %d records after the refused submission", n)
	}
}

// A stale receiver keeps the limit its plugin declared, and would fail the run at dispatch with input-too-large if it
// claimed the inputs inline.
func TestSubmitAppliesTheAdapterLimitOfAStaleReceiver(t *testing.T) {
	o := newUploadOracle(t)
	f := newFixture(t)
	terms := testTerms()
	terms.Storage = &admin.Storage{Endpoint: o.endpoint, Bucket: testBucket}
	if err := f.service.PoolCreate(f.ctx, "pool-inline", terms); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantAdd(f.ctx, "tenant-a", "pool-inline"); err != nil {
		t.Fatal(err)
	}
	f.host.storage = o.config
	receiver := f.addReceiver(t, "receiver-a", "pool-inline")
	receiver.advertise(t, f.ctx, nil)
	f.addReceiver(t, "receiver-b", "pool-inline").advertise(t, f.ctx, func(ad *wire.Advertisement) {
		ad.Offers[0].InlineLimit = 1000
		ad.AtMS = time.Now().Add(-10 * liveness).UnixMilli()
	})
	result, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Parts: []wire.Part{{Raw: make([]byte, 2000), Filename: "data.bin"}}, Deadline: time.Minute, QueueDeadline: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	task := receiver.claim(t, f.ctx, result.RunID)[0].submission
	if len(task.Message.Parts) != 1 || len(task.Wherry.S3Inputs) != 1 || task.Wherry.S3Inputs[0].Size != 2000 {
		t.Fatalf("submission has %d parts and S3 inputs %+v; want only an S3 input", len(task.Message.Parts), task.Wherry.S3Inputs)
	}
}

func payloadOf(t *testing.T, value []byte) []byte {
	t.Helper()
	parts := strings.Split(string(value), ".")
	if len(parts) != 3 {
		t.Fatal("not a compact JWS")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestAnswerSealsPresignedURLsForTheRunsObjects(t *testing.T) {
	f, o, receiver := storageFixture(t)
	task, data := submitS3(t, f, receiver, 10*time.Minute)
	runID := task.submission.Wherry.Run
	asked := newRequest(t, receiver.binding(task, 1), "model")
	asked.record.S3Inputs, asked.record.S3Outputs = []string{"dataset.bin"}, []string{"out/report.pdf"}
	receiver.feed(t, f.ctx, runID, receiver.sign(t, &asked.record))
	answering(t, f, f.host)
	f.waitSeals(t, "host-a", 1)
	values := asked.open(t, f, "host-a")
	get, put := string(values[signed.S3InputValue("dataset.bin")]), string(values[signed.S3OutputValue("out/report.pdf")])
	if len(values) != 3 || get == "" || put == "" {
		t.Fatalf("sealed %d values; want the credential and two URLs", len(values))
	}
	for _, sealed := range []string{get, put} {
		if strings.Contains(sealed, o.secret) || strings.Contains(sealed, url.QueryEscape(o.secret)) {
			t.Fatalf("a sealed URL holds the secret key: %s", sealed)
		}
	}
	expires := func(raw string) time.Duration {
		parsed, _ := url.Parse(raw)
		seconds, _ := strconv.Atoi(parsed.Query().Get("X-Amz-Expires"))
		return time.Duration(seconds) * time.Second
	}
	deadline := time.Until(time.UnixMilli(task.submission.Wherry.DeadlineMS))
	if expires(get) != 15*time.Minute || expires(put) < deadline-10*time.Second || expires(put) > deadline+2*time.Second {
		t.Fatalf("GET lives %v and PUT %v; want 15m and the turn's deadline, %v away", expires(get), expires(put), deadline)
	}
	// The store honours each URL for its own object and method alone.
	response, err := o.client.Get(get)
	if err != nil {
		t.Fatal(err)
	}
	fetched, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || !bytes.Equal(fetched, data) {
		t.Fatalf("GET answered %d with %d bytes", response.StatusCode, len(fetched))
	}
	elsewhere := strings.Replace(get, "/inputs/dataset.bin", "/inputs/other.bin", 1)
	if response, err := o.client.Get(elsewhere); err != nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("GET of another key answered %v, %v; want forbidden", response.Status, err)
	}
	request, _ := http.NewRequest(http.MethodPut, get, strings.NewReader("overwrite"))
	if response, err := o.client.Do(request); err != nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("PUT with the GET URL answered %v, %v; want forbidden", response.Status, err)
	}
	request, _ = http.NewRequest(http.MethodPut, put, strings.NewReader("report"))
	if response, err := o.client.Do(request); err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("PUT answered %v, %v", response.Status, err)
	}
	if got := o.object(t, wire.S3OutputKey("tenant-a", runID, "out/report.pdf")); string(got) != "report" {
		t.Fatalf("output key holds %q", got)
	}
	if strings.Contains(f.logs.String(), "X-Amz") {
		t.Fatalf("host logs hold a URL:\n%s", f.logs.String())
	}
}

func TestPutURLExpiresAtTheTurnsDeadline(t *testing.T) {
	f, o, receiver := storageFixture(t)
	task, _ := submitS3(t, f, receiver, 4*time.Second)
	asked := newRequest(t, receiver.binding(task, 1), "model")
	asked.record.S3Outputs = []string{"out/report.pdf"}
	receiver.feed(t, f.ctx, task.submission.Wherry.Run, receiver.sign(t, &asked.record))
	answering(t, f, f.host)
	f.waitSeals(t, "host-a", 1)
	put := string(asked.open(t, f, "host-a")[signed.S3OutputValue("out/report.pdf")])
	time.Sleep(time.Until(time.UnixMilli(task.submission.Wherry.DeadlineMS).Add(2 * time.Second)))
	request, _ := http.NewRequest(http.MethodPut, put, strings.NewReader("late"))
	if response, err := o.client.Do(request); err != nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("PUT after the deadline answered %v, %v; want forbidden", response.Status, err)
	}
}

func TestAnswerRefusesS3ObjectsTheRunDoesNotDeclare(t *testing.T) {
	f, _, receiver := storageFixture(t)
	task, _ := submitS3(t, f, receiver, time.Minute)
	for _, edit := range []func(*wire.SealRequest){
		func(request *wire.SealRequest) { request.S3Inputs = []string{"other.bin"} },
		func(request *wire.SealRequest) { request.S3Outputs = []string{"other.pdf"} },
	} {
		asked := newRequest(t, receiver.binding(task, 1), "model")
		edit(&asked.record)
		receiver.feed(t, f.ctx, task.submission.Wherry.Run, receiver.sign(t, &asked.record))
	}
	answering(t, f, f.host)
	for !strings.Contains(f.logs.String(), "S3 output other.pdf, which the run does not declare") || !strings.Contains(f.logs.String(), "S3 input other.bin, which the run does not declare") {
		if f.ctx.Err() != nil {
			t.Fatalf("logs:\n%s", f.logs.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	if seals := f.seals(t, "host-a"); seals != 0 {
		t.Fatalf("%d seals written", seals)
	}
}

func TestS3UseIsRefusedBeforePublishing(t *testing.T) {
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	file, _ := randomFile(t, "dataset.bin", 16)
	input := SubmitInput{Selector: defaultSelector(), Prompt: "x", S3Inputs: []S3File{{Name: "dataset.bin", Body: file}}, Deadline: time.Minute, QueueDeadline: time.Minute}
	if _, err := f.host.Submit(f.ctx, input); !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "this host's configuration has no storage credential") {
		t.Fatalf("submit without a storage credential = %v", err)
	}
	f.host.storage = &StorageConfig{AccessKeyFile: f.secret, SecretKeyFile: f.secret}
	if _, err := f.host.Submit(f.ctx, input); !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "pool pool-a has no storage for S3 inputs and outputs") {
		t.Fatalf("submit to a pool without storage = %v", err)
	}
	output := SubmitInput{Selector: defaultSelector(), Prompt: "x", Outputs: []wire.ExpectedOutput{{Name: "r", Storage: wire.StorageS3}}, Deadline: time.Minute, QueueDeadline: time.Minute}
	if _, err := f.host.Submit(f.ctx, output); !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "pool pool-a has no storage") {
		t.Fatalf("submit of an S3 output to a pool without storage = %v", err)
	}
	if end := f.tasksEnd(t, "pool-a"); end != 0 {
		t.Fatalf("%d tasks published", end)
	}
}

func TestStorageKeyFilesMustBeTheOwnersAlone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "secret.key")
	if err := os.WriteFile(path, []byte("secret\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readKey(path); err == nil || !strings.Contains(err.Error(), "chmod 600") {
		t.Fatalf("read a key file others can read: %v", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if key, err := readKey(path); err != nil || key != "secret" {
		t.Fatalf("read %q, %v", key, err)
	}
	dir := filepath.Join(t.TempDir(), "keys")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := readKey(dir); err == nil || !strings.Contains(err.Error(), "not a regular file") {
		t.Fatalf("read a directory as a key file: %v", err)
	}
}

// s3Result is a completed result listing outputs as S3 outputs, with its terminal status.
func (r *testReceiver) s3Result(t *testing.T, task claimed, outputs []wire.ReturnedOutput) [][]byte {
	t.Helper()
	metadata, err := json.Marshal(wire.ResultMetadata{ReceiverID: r.id, Pool: r.pool, S3Outputs: outputs})
	if err != nil {
		t.Fatal(err)
	}
	return [][]byte{
		r.result(t, task, 1, wire.TaskStateCompleted, func(result *wire.Result) { result.Task.Metadata = metadata }),
		r.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateCompleted, nil),
	}
}

func TestDownloadVerifiesThenDeletesOnlyTheRunsPrefix(t *testing.T) {
	f, o, receiver := storageFixture(t)
	task, _ := submitS3(t, f, receiver, time.Minute)
	runID := task.submission.Wherry.Run
	report := bytes.Repeat([]byte("r"), 3<<20)
	key := wire.S3OutputKey("tenant-a", runID, "out/report.pdf")
	o.put(t, key, report)
	// Neighbours a prefix match too loose would reach: a run whose ID extends this one's, and this run under another tenant.
	neighbours := []string{wire.S3InputKey("tenant-a", runID+"x", "dataset.bin"), wire.S3InputKey("tenant-b", runID, "dataset.bin")}
	for _, neighbour := range neighbours {
		o.put(t, neighbour, []byte("keep"))
	}
	receiver.feed(t, f.ctx, runID, receiver.s3Result(t, task, []wire.ReturnedOutput{{Name: "out/report.pdf", SHA256: hexSum(report), Size: int64(len(report)), Key: key}})...)
	awaited, err := f.host.Await(f.ctx, runID, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	downloaded, rejected, err := f.host.DownloadOutputs(f.ctx, runID, awaited, dir)
	if err != nil || len(rejected) != 0 || len(downloaded) != 1 || downloaded[0].Path != filepath.Join(dir, "out", "report.pdf") {
		t.Fatalf("downloaded %+v, rejected %+v, %v", downloaded, rejected, err)
	}
	if written, err := os.ReadFile(downloaded[0].Path); err != nil || !bytes.Equal(written, report) {
		t.Fatalf("wrote %d bytes, %v", len(written), err)
	}
	for _, gone := range []string{key, wire.S3InputKey("tenant-a", runID, "dataset.bin")} {
		if o.object(t, gone) != nil {
			t.Fatalf("%s survived the run's cleanup", gone)
		}
	}
	for _, neighbour := range neighbours {
		if o.object(t, neighbour) == nil {
			t.Fatalf("cleanup deleted %s, outside the run's prefix", neighbour)
		}
	}
	// A second await's download finds the verified file and the prefix already gone.
	if again, rejected, err := f.host.DownloadOutputs(f.ctx, runID, awaited, dir); err != nil || len(rejected) != 0 || len(again) != 1 {
		t.Fatalf("download again: %+v, %+v, %v", again, rejected, err)
	}
}

func TestADownloadThatDoesNotVerifyKeepsThePrefix(t *testing.T) {
	f, o, receiver := storageFixture(t)
	for name, tamper := range map[string]func(wire.ReturnedOutput) wire.ReturnedOutput{
		"hash": func(listed wire.ReturnedOutput) wire.ReturnedOutput {
			listed.SHA256 = hexSum([]byte("other"))
			return listed
		},
		"size": func(listed wire.ReturnedOutput) wire.ReturnedOutput { listed.Size--; return listed },
		"key": func(listed wire.ReturnedOutput) wire.ReturnedOutput {
			listed.Key = strings.Replace(listed.Key, "/1/outputs/", "/2/outputs/", 1)
			return listed
		},
		"undeclared name": func(listed wire.ReturnedOutput) wire.ReturnedOutput {
			listed.Name, listed.Key = "out/other.pdf", strings.Replace(listed.Key, "report.pdf", "other.pdf", 1)
			return listed
		},
	} {
		t.Run(name, func(t *testing.T) {
			task, _ := submitS3(t, f, receiver, time.Minute)
			runID := task.submission.Wherry.Run
			key := wire.S3OutputKey("tenant-a", runID, "out/report.pdf")
			o.put(t, key, []byte("report"))
			o.put(t, strings.Replace(key, "/1/outputs/", "/2/outputs/", 1), []byte("report"))
			o.put(t, strings.Replace(key, "report.pdf", "other.pdf", 1), []byte("report"))
			listed := tamper(wire.ReturnedOutput{Name: "out/report.pdf", SHA256: hexSum([]byte("report")), Size: 6, Key: key})
			receiver.feed(t, f.ctx, runID, receiver.s3Result(t, task, []wire.ReturnedOutput{listed})...)
			awaited, err := f.host.Await(f.ctx, runID, 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			downloaded, rejected, err := f.host.DownloadOutputs(f.ctx, runID, awaited, dir)
			if err != nil || len(downloaded) != 0 || len(rejected) != 1 || !strings.HasPrefix(rejected[0].Reason, "rejected by this host: ") {
				t.Fatalf("downloaded %+v, rejected %+v, %v", downloaded, rejected, err)
			}
			if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(listed.Name))); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("an unverified output stayed written: %v", err)
			}
			if o.object(t, key) == nil || o.object(t, wire.S3InputKey("tenant-a", runID, "dataset.bin")) == nil {
				t.Fatal("the prefix was deleted before every output verified")
			}
		})
	}
}

func TestCleanupDeletesTheRunsPrefixOnDemand(t *testing.T) {
	f, o, receiver := storageFixture(t)
	task, _ := submitS3(t, f, receiver, time.Minute)
	runID := task.submission.Wherry.Run
	if err := f.host.Cleanup(f.ctx, runID); !errors.Is(err, ErrInvalidInput) || o.object(t, wire.S3InputKey("tenant-a", runID, "dataset.bin")) == nil {
		t.Fatalf("cleanup of a working run = %v", err)
	}
	receiver.feed(t, f.ctx, runID, receiver.status(t, task, 1, "rejected-1", "rejected", wire.TaskStateRejected, nil))
	if err := f.host.Cleanup(f.ctx, runID); err != nil {
		t.Fatal(err)
	}
	if o.object(t, wire.S3InputKey("tenant-a", runID, "dataset.bin")) != nil {
		t.Fatal("cleanup left the run's input")
	}
	if err := f.host.Cleanup(f.ctx, "run-unknown"); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("cleanup of an unknown run = %v", err)
	}
}

func TestASealOverItsLimitIsRefusedBeforeUploading(t *testing.T) {
	f, _, _ := storageFixture(t)
	var outputs []wire.ExpectedOutput
	for i := range 100 {
		outputs = append(outputs, wire.ExpectedOutput{Name: strings.Repeat("o", 300) + strconv.Itoa(i), Storage: wire.StorageS3})
	}
	file, _ := randomFile(t, "dataset.bin", 16)
	_, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "x", S3Inputs: []S3File{{Name: "dataset.bin", Body: file}}, Outputs: outputs,
		Deadline: time.Minute, QueueDeadline: time.Minute})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "over the 64 KiB limit") {
		t.Fatalf("submit = %v; want the seal's size refused", err)
	}
	if end := f.tasksEnd(t, "pool-s3"); end != 0 {
		t.Fatalf("%d tasks published", end)
	}
}
