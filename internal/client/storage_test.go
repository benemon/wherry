package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"encoding/pem"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const testBucket = "wherry-test"

// oracle is the real S3 store the storage tests run against: the S3-compatible server WHERRY_S3_TEST_ENDPOINT names,
// a local S3 store over http (test/s3/up.sh), fronted by a TLS proxy that keeps the Host header, so the client and the URLs it presigns
// are https as production requires. store reaches the server directly, as an operator would.
type oracle struct {
	endpoint string
	config   *StorageConfig
	sources  map[string]credential.Source
	store    *minio.Client
	client   *http.Client
	roots    *x509.CertPool
	secret   string
}

// uploadOracle is a fake S3 store keeping objects by path, /bucket/key. ops logs its object requests in order: PUT, GET
// and DELETE with the path. access logs the access key that signed each request.
type uploadOracle struct {
	endpoint string
	config   *StorageConfig
	sources  map[string]credential.Source
	// roots is the pool a host's ca_cert holding the oracle's CA builds, and client an HTTP client that trusts it.
	roots  *x509.CertPool
	client *http.Client
	// onRequest, when set, sees each request before the oracle handles it.
	onRequest func(*http.Request)
	mu        sync.Mutex
	objects   map[string][]byte
	ops       []string
	access    []string
	// clock, when set, is the time the oracle checks a presigned URL's X-Amz-Date plus X-Amz-Expires against.
	clock func() time.Time
}

// expired reports whether a presigned request's lifetime has passed on the oracle's clock.
func (o *uploadOracle) expired(query url.Values) bool {
	signedAt, err := time.Parse("20060102T150405Z", query.Get("X-Amz-Date"))
	seconds, _ := strconv.Atoi(query.Get("X-Amz-Expires"))
	if err != nil || seconds == 0 || o.clock == nil {
		return false
	}
	return o.clock().After(signedAt.Add(time.Duration(seconds) * time.Second))
}

// policyExpired reports whether a policy upload's policy has expired on the oracle's clock.
func (o *uploadOracle) policyExpired(field string) bool {
	if o.clock == nil {
		return false
	}
	decoded, err := base64.StdEncoding.DecodeString(field)
	var document struct {
		Expiration string `json:"expiration"`
	}
	if err != nil || json.Unmarshal(decoded, &document) != nil {
		return true
	}
	expiration, err := time.Parse(time.RFC3339Nano, document.Expiration)
	return err != nil || o.clock().After(expiration)
}

// readPostForm reads a policy upload's fields and file, which must come last.
func readPostForm(r *http.Request) (map[string]string, []byte, error) {
	reader, err := r.MultipartReader()
	if err != nil {
		return nil, nil, err
	}
	fields := map[string]string{}
	var data []byte
	for file := false; ; {
		part, err := reader.NextPart()
		if err == io.EOF {
			if !file {
				return nil, nil, errors.New("no file")
			}
			return fields, data, nil
		}
		if err != nil || file {
			return nil, nil, errors.New("a part follows the file")
		}
		value, err := io.ReadAll(part)
		if err != nil {
			return nil, nil, err
		}
		if part.FormName() == "file" {
			data, file = value, true
			continue
		}
		fields[part.FormName()] = string(value)
	}
}

// postUpload posts data at key through policy with client, as the receiver does: the policy's fields with key replaced,
// then the file.
func postUpload(t *testing.T, client *http.Client, policy *signed.S3Policy, key string, data []byte) *http.Response {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for _, name := range slices.Sorted(maps.Keys(policy.Fields)) {
		value := policy.Fields[name]
		if name == "key" {
			value = key
		}
		if err := writer.WriteField(name, value); err != nil {
			t.Fatal(err)
		}
	}
	file, err := writer.CreateFormFile("file", "file")
	if err != nil {
		t.Fatal(err)
	}
	file.Write(data)
	writer.Close()
	request, err := http.NewRequest(http.MethodPost, policy.URL, &body)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", writer.FormDataContentType())
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	return response
}

// policyDocument decodes a policy field's document.
func policyDocument(t *testing.T, policy *signed.S3Policy) (expiration time.Time, conditions []string) {
	t.Helper()
	decoded, err := base64.StdEncoding.DecodeString(policy.Fields["policy"])
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Expiration string           `json:"expiration"`
		Conditions []jsontext.Value `json:"conditions"`
	}
	if err := json.Unmarshal(decoded, &document); err != nil {
		t.Fatalf("policy %s: %v", decoded, err)
	}
	if expiration, err = time.Parse(time.RFC3339Nano, document.Expiration); err != nil {
		t.Fatalf("policy expiration %q: %v", document.Expiration, err)
	}
	for _, condition := range document.Conditions {
		conditions = append(conditions, string(condition))
	}
	return expiration, conditions
}

func newUploadOracle(t *testing.T) *uploadOracle {
	t.Helper()
	o := &uploadOracle{objects: map[string][]byte{}}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if o.onRequest != nil {
			o.onRequest(r)
		}
		o.mu.Lock()
		defer o.mu.Unlock()
		query, bucket := r.URL.Query(), strings.TrimSuffix(r.URL.Path, "/")
		if _, signature, ok := strings.Cut(r.Header.Get("Authorization"), "Credential="); ok {
			access, _, _ := strings.Cut(signature, "/")
			o.access = append(o.access, access)
		}
		if o.expired(query) {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		switch {
		case r.Method == http.MethodGet && query.Has("location"):
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprint(w, "<LocationConstraint></LocationConstraint>")
		case r.Method == http.MethodPut:
			data, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "body", http.StatusBadRequest)
				return
			}
			o.objects[r.URL.Path] = data
			o.ops = append(o.ops, "PUT "+r.URL.Path)
			w.Header().Set("ETag", `"test"`)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && query.Has("list-type"):
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprint(w, "<ListBucketResult><IsTruncated>false</IsTruncated>")
			for _, path := range slices.Sorted(maps.Keys(o.objects)) {
				if key, ok := strings.CutPrefix(path, bucket+"/"); ok && strings.HasPrefix(key, query.Get("prefix")) {
					fmt.Fprintf(w, "<Contents><Key>%s</Key><Size>%d</Size></Contents>", key, len(o.objects[path]))
				}
			}
			fmt.Fprint(w, "</ListBucketResult>")
		case r.Method == http.MethodGet:
			data, ok := o.objects[r.URL.Path]
			o.ops = append(o.ops, "GET "+r.URL.Path)
			if !ok {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprint(w, "<Error><Code>NoSuchKey</Code></Error>")
				return
			}
			w.Header().Set("ETag", `"test"`)
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Write(data)
		case r.Method == http.MethodPost && !query.Has("delete"):
			fields, data, err := readPostForm(r)
			switch {
			case err != nil || fields["key"] == "":
				http.Error(w, "form", http.StatusBadRequest)
			case o.policyExpired(fields["policy"]):
				w.WriteHeader(http.StatusForbidden)
			default:
				o.objects[bucket+"/"+fields["key"]] = data
				o.ops = append(o.ops, "POST "+bucket+"/"+fields["key"])
				w.WriteHeader(http.StatusNoContent)
			}
		case r.Method == http.MethodPost:
			var request struct {
				Objects []struct {
					Key string
				} `xml:"Object"`
			}
			if err := xml.NewDecoder(r.Body).Decode(&request); err != nil {
				http.Error(w, "body", http.StatusBadRequest)
				return
			}
			for _, object := range request.Objects {
				delete(o.objects, bucket+"/"+object.Key)
				o.ops = append(o.ops, "DELETE "+bucket+"/"+object.Key)
			}
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprint(w, "<DeleteResult></DeleteResult>")
		default:
			http.Error(w, "method", http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)
	o.roots, o.client = serverRoots(t, server), server.Client()
	access, secret := filepath.Join(t.TempDir(), "access"), filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(access, []byte("access"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secret, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	o.endpoint, o.config = server.URL, &StorageConfig{AccessKey: "storage-access", SecretKey: "storage-secret"}
	o.sources = map[string]credential.Source{"storage-access": {File: access}, "storage-secret": {File: secret}}
	return o
}

// serverRoots is the pool a ca_cert holding server's certificate builds: the system roots plus it.
func serverRoots(t *testing.T, server *httptest.Server) *x509.CertPool {
	t.Helper()
	roots, err := operator.RootPool(string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})))
	if err != nil {
		t.Fatal(err)
	}
	return roots
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
	config := &StorageConfig{AccessKey: "storage-access", SecretKey: "storage-secret"}
	sources := map[string]credential.Source{"storage-access": {File: os.Getenv("WHERRY_S3_TEST_ACCESS_KEY_FILE")}, "storage-secret": {File: os.Getenv("WHERRY_S3_TEST_SECRET_KEY_FILE")}}
	var keys [2]string
	for i, name := range []string{config.AccessKey, config.SecretKey} {
		key, err := sources[name].Resolve(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		keys[i] = strings.TrimSpace(string(key))
	}
	access, secret := keys[0], keys[1]
	parsed, err := url.Parse(target)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var requested []string
	// written are the test bucket's keys this test PUT through the proxy, deleted when it ends.
	written := map[string]bool{}
	proxy := httptest.NewTLSServer(&httputil.ReverseProxy{Rewrite: func(r *httputil.ProxyRequest) {
		mu.Lock()
		requested = append(requested, r.In.URL.String())
		if key, ok := strings.CutPrefix(r.In.URL.Path, "/"+testBucket+"/"); ok && r.In.Method == http.MethodPut && key != "" {
			written[key] = true
		}
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
	roots := serverRoots(t, proxy)
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
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		for key := range written {
			if err := store.RemoveObject(context.Background(), testBucket, key, minio.RemoveObjectOptions{}); err != nil {
				t.Errorf("remove %s: %v", key, err)
			}
		}
	})
	client := proxy.Client()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &oracle{endpoint: proxy.URL, config: config, sources: sources, store: store, client: client, roots: roots, secret: secret}
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
	t.Cleanup(func() { _ = o.store.RemoveObject(context.Background(), testBucket, key, minio.RemoveObjectOptions{}) })
}

// storageFixture adds pool-s3, whose terms name the oracle's endpoint and bucket, and a live receiver in it alone, and
// gives the host the oracle's credential.
func storageFixture(t *testing.T) (*fixture, *oracle, *testReceiver) {
	t.Helper()
	o := newOracle(t)
	f := newFixture(t)
	f.host.roots = o.roots
	terms := testTerms()
	terms.Storage = &operator.Storage{Endpoint: o.endpoint, Bucket: testBucket}
	if err := f.service.PoolApply(f.ctx, "pool-s3", terms, false); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantPoolAdd(f.ctx, "tenant-a", "pool-s3"); err != nil {
		t.Fatal(err)
	}
	f.host.storage = map[string]StorageConfig{"pool-s3": *o.config}
	maps.Copy(f.host.credentials, o.sources)
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

// submitS3 submits a task with a 5 MiB S3 input and one required output through receiver, which claims it and writes
// its echo and claimed status.
func submitS3(t *testing.T, f *fixture, receiver *testReceiver, deadline time.Duration) (claimed, []byte) {
	t.Helper()
	file, data := randomFile(t, "dataset.bin", 5<<20)
	submitted, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "do the work", S3Inputs: []S3File{{Name: "dataset.bin", Body: file}},
		Outputs: []wire.ExpectedOutput{{Name: "out/report.pdf"}}, EndBy: time.Now().Add(deadline), StartBy: time.Now().Add(time.Minute)})
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
	records, err := readTasks(f.ctx, receiver.kafka, "pool-s3")
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

func TestSubmitMovesInputsOverSmallestProfileLimitToS3(t *testing.T) {
	o := newUploadOracle(t)
	f := newFixture(t)
	f.host.roots = o.roots
	terms := testTerms()
	terms.Storage = &operator.Storage{Endpoint: o.endpoint, Bucket: testBucket}
	if err := f.service.PoolApply(f.ctx, "pool-inline", terms, false); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantPoolAdd(f.ctx, "tenant-a", "pool-inline"); err != nil {
		t.Fatal(err)
	}
	f.host.storage = map[string]StorageConfig{"pool-inline": *o.config}
	maps.Copy(f.host.credentials, o.sources)
	receiver := f.addReceiver(t, "receiver-a", "pool-inline")
	receiver.advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.Offers[0].InlineLimit = 1000 })
	f.addReceiver(t, "receiver-b", "pool-inline").advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.Offers[0].InlineLimit = 5000 })
	data := bytes.Repeat([]byte("x"), 2000)
	result, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Parts: []wire.Part{{Raw: data, Filename: "data.bin"}}, EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)})
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
	f.host.roots = o.roots
	terms := testTerms()
	if stored {
		terms.Storage = &operator.Storage{Endpoint: o.endpoint, Bucket: testBucket}
		f.host.storage = map[string]StorageConfig{"pool-r": *o.config}
		maps.Copy(f.host.credentials, o.sources)
	}
	if err := f.service.PoolApply(f.ctx, "pool-r", terms, false); err != nil {
		t.Fatal(err)
	}
	// The receiver's signature topics exist once it is added; pool apply then reads their limit.
	receiver := f.addReceiver(t, "receiver-r", "pool-r")
	limit := "100000"
	f.setTaskTopicsConfig(t, "pool-r", "max.message.bytes", limit)
	if err := f.service.PoolApply(f.ctx, "pool-r", terms, false); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantPoolAdd(f.ctx, "tenant-a", "pool-r"); err != nil {
		t.Fatal(err)
	}
	receiver.advertise(t, f.ctx, nil)
	return f, o, receiver
}

func TestSubmitMovesInputsOverTheRecordLimitToS3(t *testing.T) {
	f, o, receiver := recordLimitFixture(t, true)
	data := make([]byte, 60000)
	rand.Read(data)
	result, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Parts: []wire.Part{{Raw: data, Filename: "data.bin"}}, EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)})
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

func TestSubmitUnderTheRecordLimitKeepsInputsInline(t *testing.T) {
	f, _, receiver := recordLimitFixture(t, true)
	result, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Parts: []wire.Part{{Raw: []byte("data"), Filename: "data.bin"}}, EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if task := receiver.claim(t, f.ctx, result.RunID)[0].submission; len(task.Message.Parts) != 2 || task.Wherry.S3Inputs != nil {
		t.Fatalf("submission has %d parts and S3 inputs %+v; want the input inline", len(task.Message.Parts), task.Wherry.S3Inputs)
	}
}

func TestSubmitOverTheRecordLimitMovesInputsToS3(t *testing.T) {
	f, _, receiver := recordLimitFixture(t, true)
	data := make([]byte, 60000)
	rand.Read(data)
	result, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Parts: []wire.Part{{Raw: data, Filename: "data.bin"}}, EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if task := receiver.claim(t, f.ctx, result.RunID)[0].submission; !slices.Equal(task.Wherry.S3Inputs, []wire.S3Input{{Name: "data.bin", Size: 60000, SHA256: hexSum(data)}}) {
		t.Fatalf("submission S3 inputs %+v; want the uploaded input", task.Wherry.S3Inputs)
	}
}

func TestSubmitUploadsAFileOverTheRecordLimitFromDisk(t *testing.T) {
	f, o, receiver := recordLimitFixture(t, true)
	big, err := os.Create(filepath.Join(t.TempDir(), "big.bin"))
	if err != nil {
		t.Fatal(err)
	}
	defer big.Close()
	const size = 64 << 20
	if err := big.Truncate(size); err != nil {
		t.Fatal(err)
	}
	small, _ := randomFile(t, "small.bin", 100)
	var before, during runtime.MemStats
	// The heap is read as the PUT arrives, before the oracle reads its body.
	o.onRequest = func(request *http.Request) {
		if request.Method == http.MethodPut && strings.HasSuffix(request.URL.Path, "/big.bin") {
			runtime.GC()
			runtime.ReadMemStats(&during)
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&before)
	result, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Files: []S3File{{Name: "big.bin", Body: big}, {Name: "small.bin", Body: small}},
		EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if during.HeapAlloc == 0 {
		t.Fatal("big.bin was never uploaded")
	}
	if growth := int64(during.HeapAlloc) - int64(before.HeapAlloc); growth > size/4 {
		t.Fatalf("the heap grew %d bytes by the upload of a %d-byte file; want it streamed from disk", growth, size)
	}
	task := receiver.claim(t, f.ctx, result.RunID)[0].submission
	if len(task.Message.Parts) != 2 || task.Message.Parts[1].Filename != "small.bin" || len(task.Wherry.S3Inputs) != 1 || task.Wherry.S3Inputs[0].Name != "big.bin" || task.Wherry.S3Inputs[0].Size != size {
		t.Fatalf("submission has parts %+v and S3 inputs %+v; want small.bin inline and big.bin through S3", task.Message.Parts, task.Wherry.S3Inputs)
	}
}

func TestSubmitRefusesARecordOverTheLimitWithoutStorage(t *testing.T) {
	f, _, _ := recordLimitFixture(t, false)
	_, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Parts: []wire.Part{{Raw: make([]byte, 60000), Filename: "data.bin"}}, EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "-byte record, over pool pool-r's 100000-byte record limit") {
		t.Fatalf("submit = %v; want the record size and the record limit named", err)
	}
	if n := f.tasksEnd(t, "pool-r"); n != 0 {
		t.Fatalf("pool tasks hold %d records after the refused submission", n)
	}
}

func TestSubmitRefusesAPromptOverTheRecordLimit(t *testing.T) {
	f, _, _ := recordLimitFixture(t, true)
	_, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: strings.Repeat("p", 80000), Parts: []wire.Part{{Raw: []byte("x"), Filename: "data.bin"}}, EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "the 80000-byte prompt makes a ") || !strings.Contains(err.Error(), "over pool pool-r's 100000-byte record limit") {
		t.Fatalf("submit = %v; want the prompt and the record limit named", err)
	}
	if n := f.tasksEnd(t, "pool-r"); n != 0 {
		t.Fatalf("pool tasks hold %d records after the refused submission", n)
	}
}

// A stale receiver keeps the limit its plugin declared, and would fail the run at dispatch with input-too-large if it
// claimed the inputs inline.
func TestSubmitAppliesTheProfileLimitOfAStaleReceiver(t *testing.T) {
	o := newUploadOracle(t)
	f := newFixture(t)
	f.host.roots = o.roots
	terms := testTerms()
	terms.Storage = &operator.Storage{Endpoint: o.endpoint, Bucket: testBucket}
	if err := f.service.PoolApply(f.ctx, "pool-inline", terms, false); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantPoolAdd(f.ctx, "tenant-a", "pool-inline"); err != nil {
		t.Fatal(err)
	}
	f.host.storage = map[string]StorageConfig{"pool-inline": *o.config}
	maps.Copy(f.host.credentials, o.sources)
	receiver := f.addReceiver(t, "receiver-a", "pool-inline")
	receiver.advertise(t, f.ctx, nil)
	f.addReceiver(t, "receiver-b", "pool-inline").advertise(t, f.ctx, func(ad *wire.Advertisement) {
		ad.Offers[0].InlineLimit = 1000
		ad.AtMS = time.Now().Add(-10 * operator.Liveness).UnixMilli()
	})
	result, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Parts: []wire.Part{{Raw: make([]byte, 2000), Filename: "data.bin"}}, EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)})
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

func TestTheEnvelopeCarriesPresignedURLsForTheRunsObjects(t *testing.T) {
	f, o, receiver := storageFixture(t)
	task, data := submitS3(t, f, receiver, 10*time.Minute)
	runID := task.submission.Wherry.Run
	_, values, err := open(t, f, runID, f.poolKeys["pool-s3"])
	if err != nil {
		t.Fatal(err)
	}
	get, policy := values.S3Inputs["dataset.bin"], values.S3Policy
	if len(values.S3Inputs) != 1 || get == "" || policy == nil {
		t.Fatalf("enveloped %d input URLs and policy %+v; want one URL and a policy", len(values.S3Inputs), policy)
	}
	if strings.Contains(get, o.secret) || strings.Contains(get, url.QueryEscape(o.secret)) {
		t.Fatalf("an enveloped URL holds the secret key: %s", get)
	}
	for name, value := range policy.Fields {
		if strings.Contains(value, o.secret) {
			t.Fatalf("the policy's %s field holds the secret key", name)
		}
	}
	expires := func(raw string) time.Duration {
		parsed, _ := url.Parse(raw)
		seconds, _ := strconv.Atoi(parsed.Query().Get("X-Amz-Expires"))
		return time.Duration(seconds) * time.Second
	}
	getDeadline := time.Until(time.Unix(task.submission.Wherry.TaskStartedBy, 0).Add(15 * time.Minute))
	if expires(get) < getDeadline-10*time.Second || expires(get) > getDeadline+2*time.Second {
		t.Fatalf("GET lives %v; want the turn's task_started_by plus 15m, %v away", expires(get), getDeadline)
	}
	endBy := time.Unix(task.submission.Wherry.TaskEndedBy, 0)
	if expiration, _ := policyDocument(t, policy); !expiration.Equal(endBy.Add(operator.UploadGrace)) {
		t.Fatalf("the policy expires at %v; want task_ended_by %v plus the 10m grace", expiration, endBy)
	}
	// The store honours the URL for its own object and method alone.
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
	// The policy takes any key under the run's output prefix, and none outside it.
	key := wire.S3OutputKey("tenant-a", runID, "out/report.pdf")
	if response := postUpload(t, o.client, policy, key, []byte("report")); response.StatusCode != http.StatusNoContent {
		t.Fatalf("POST under the prefix answered %v", response.Status)
	}
	t.Cleanup(func() { _ = o.store.RemoveObject(context.Background(), testBucket, key, minio.RemoveObjectOptions{}) })
	if got := o.object(t, key); string(got) != "report" {
		t.Fatalf("output key holds %q", got)
	}
	for name, outside := range map[string]string{"another run": wire.S3OutputKey("tenant-a", "run-other", "out/report.pdf"), "the run's inputs": wire.S3InputKey("tenant-a", runID, "dataset.bin")} {
		if response := postUpload(t, o.client, policy, outside, []byte("elsewhere")); response.StatusCode != http.StatusForbidden {
			t.Fatalf("POST at %s answered %v; want forbidden", name, response.Status)
		}
	}
	if strings.Contains(f.logs.String(), "X-Amz") || strings.Contains(f.logs.String(), policy.Fields["x-amz-signature"]) || strings.Contains(f.logs.String(), policy.Fields["policy"]) {
		t.Fatalf("host logs hold a URL or the policy:\n%s", f.logs.String())
	}
}

// The policy lasts operator.UploadGrace past task_ended_by: its expiration is that instant, and the store accepts an
// upload after end-by.
func TestThePolicyExpiresAtTheGraceAfterEndBy(t *testing.T) {
	f, o, receiver := storageFixture(t)
	task, _ := submitS3(t, f, receiver, 4*time.Second)
	_, values, err := open(t, f, task.submission.Wherry.Run, f.poolKeys["pool-s3"])
	if err != nil {
		t.Fatal(err)
	}
	endBy := time.Unix(task.submission.Wherry.TaskEndedBy, 0)
	if expiration, _ := policyDocument(t, values.S3Policy); !expiration.Equal(endBy.Add(operator.UploadGrace)) {
		t.Fatalf("the policy expires at %v; want task_ended_by %v plus the grace %v", expiration, endBy, operator.UploadGrace)
	}
	time.Sleep(time.Until(endBy.Add(2 * time.Second)))
	key := wire.S3OutputKey("tenant-a", task.submission.Wherry.Run, "out/report.pdf")
	t.Cleanup(func() { _ = o.store.RemoveObject(context.Background(), testBucket, key, minio.RemoveObjectOptions{}) })
	if response := postUpload(t, o.client, values.S3Policy, key, []byte("late")); response.StatusCode != http.StatusNoContent {
		t.Fatalf("POST after task_ended_by, within the grace, answered %v; want no content", response.Status)
	}
}

// The store enforces what the builder sets: through it, with a 1 KiB range and a 2 s expiry, an upload outside the
// prefix, over the range, or after the expiry is refused, and one within all three lands.
func TestTheStoreEnforcesTheBuiltPolicy(t *testing.T) {
	f, o, _ := storageFixture(t)
	store, err := f.host.bucket(f.ctx, "pool-s3", &operator.Storage{Endpoint: o.endpoint, Bucket: testBucket}, newResolver(f.ctx, ""))
	if err != nil {
		t.Fatal(err)
	}
	prefix := wire.S3OutputKey("tenant-a", "run-policy", "")
	policy, err := store.postPolicy(f.ctx, testBucket, prefix, 1024, time.Now().Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{prefix + "small", prefix + "large", prefix + "late", wire.S3OutputKey("tenant-a", "run-other", "small")} {
		t.Cleanup(func() { _ = o.store.RemoveObject(context.Background(), testBucket, key, minio.RemoveObjectOptions{}) })
	}
	if response := postUpload(t, o.client, policy, prefix+"small", bytes.Repeat([]byte("s"), 1024)); response.StatusCode != http.StatusNoContent {
		t.Fatalf("POST within the policy answered %v", response.Status)
	}
	if response := postUpload(t, o.client, policy, wire.S3OutputKey("tenant-a", "run-other", "small"), []byte("s")); response.StatusCode != http.StatusForbidden {
		t.Fatalf("POST outside the prefix answered %v; want forbidden", response.Status)
	}
	if response := postUpload(t, o.client, policy, prefix+"large", bytes.Repeat([]byte("l"), 1025)); response.StatusCode == http.StatusNoContent {
		t.Fatal("POST over the range was accepted")
	}
	if o.object(t, prefix+"large") != nil || o.object(t, wire.S3OutputKey("tenant-a", "run-other", "small")) != nil {
		t.Fatal("a refused upload created its object")
	}
	time.Sleep(2500 * time.Millisecond)
	if response := postUpload(t, o.client, policy, prefix+"late", []byte("l")); response.StatusCode != http.StatusForbidden {
		t.Fatalf("POST after the expiry answered %v; want forbidden", response.Status)
	}
}

func TestS3UseIsRefusedBeforePublishing(t *testing.T) {
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	file, _ := randomFile(t, "dataset.bin", 16)
	input := SubmitInput{Selector: defaultSelector(), Prompt: "x", S3Inputs: []S3File{{Name: "dataset.bin", Body: file}}, EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)}
	if _, err := f.host.Submit(f.ctx, input); !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "this host's configuration has no storage credential") {
		t.Fatalf("submit without a storage credential = %v", err)
	}
	f.host.storage = map[string]StorageConfig{"pool-a": {AccessKey: "model", SecretKey: "model"}}
	if _, err := f.host.Submit(f.ctx, input); !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "pool pool-a has no storage for S3 inputs and outputs") {
		t.Fatalf("submit to a pool without storage = %v", err)
	}
	// A pool without storage takes an output of any name: the route is chosen by size at collection.
	f.host.storage = nil
	if _, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "x", Outputs: []wire.ExpectedOutput{{Name: "r"}}, EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)}); err != nil {
		t.Fatalf("submit of an output to a pool without storage = %v", err)
	}
	if end := f.tasksEnd(t, "pool-a"); end != 1 {
		t.Fatalf("%d tasks published, want the one with an output alone", end)
	}
}

// The storage keys resolve through their credentials' sources, trimmed, and a failure names the credential alone.
func TestStorageKeysResolveThroughSources(t *testing.T) {
	const access, secret = "storage-access-value", "storage-secret-value"
	t.Setenv("WHERRY_TEST_STORAGE_ACCESS", access+"\n")
	h := &Host{storage: map[string]StorageConfig{"pool-s3": {AccessKey: "s3-access", SecretKey: "s3-secret"}}, credentials: map[string]credential.Source{
		"s3-access": {Env: "WHERRY_TEST_STORAGE_ACCESS"},
		"s3-secret": {Command: []string{"printf", secret + "\n"}},
	}}
	pool := &operator.Storage{Endpoint: "https://s3.example.test", Bucket: testBucket}
	store, err := h.bucket(context.Background(), "pool-s3", pool, newResolver(context.Background(), ""))
	if err != nil {
		t.Fatal(err)
	}
	if creds, err := store.client.GetCreds(); err != nil || creds.AccessKeyID != access || creds.SecretAccessKey != secret {
		t.Fatalf("S3 client holds %q and %q, %v; want the env and command sources' values", creds.AccessKeyID, creds.SecretAccessKey, err)
	}
	path := filepath.Join(t.TempDir(), "secret.key")
	if err := os.WriteFile(path, []byte(secret), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	for name, source := range map[string]credential.Source{"open file": {File: path}, "unset env": {Env: "WHERRY_TEST_STORAGE_UNSET"}, "failing command": {Command: []string{"sh", "-c", "printf " + secret + "; exit 2"}}} {
		h.credentials["s3-secret"] = source
		_, err := h.bucket(context.Background(), "pool-s3", pool, newResolver(context.Background(), ""))
		if err == nil || !strings.HasPrefix(err.Error(), "read storage credential s3-secret: its ") {
			t.Fatalf("bucket with a secret key from a %s: %v, want the credential named", name, err)
		}
		for _, held := range []string{access, secret, path} {
			if strings.Contains(err.Error(), held) {
				t.Fatalf("bucket with a secret key from a %s: error %q holds %q", name, err, held)
			}
		}
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
	} {
		t.Run(name, func(t *testing.T) {
			task, _ := submitS3(t, f, receiver, time.Minute)
			runID := task.submission.Wherry.Run
			key := wire.S3OutputKey("tenant-a", runID, "out/report.pdf")
			o.put(t, key, []byte("report"))
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
	// An S3 output the run did not declare downloads like a declared one: the route is chosen by size.
	t.Run("undeclared name", func(t *testing.T) {
		task, _ := submitS3(t, f, receiver, time.Minute)
		runID := task.submission.Wherry.Run
		key := wire.S3OutputKey("tenant-a", runID, "out/other.pdf")
		o.put(t, key, []byte("report"))
		receiver.feed(t, f.ctx, runID, receiver.s3Result(t, task, []wire.ReturnedOutput{{Name: "out/other.pdf", SHA256: hexSum([]byte("report")), Size: 6, Key: key}})...)
		awaited, err := f.host.Await(f.ctx, runID, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		dir := t.TempDir()
		downloaded, rejected, err := f.host.DownloadOutputs(f.ctx, runID, awaited, dir)
		if err != nil || len(rejected) != 0 || len(downloaded) != 1 || downloaded[0].Path != filepath.Join(dir, "out", "other.pdf") {
			t.Fatalf("downloaded %+v, rejected %+v, %v", downloaded, rejected, err)
		}
	})
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

func TestAnEnvelopeOverItsLimitPublishesNothing(t *testing.T) {
	// The limit is the host's own check, so the in-process store serves; 200 long-named uploads strain a real one.
	f, _, _ := fakeStorageFixture(t)
	// Each S3 input's presigned URL is a few hundred bytes of the envelope; 200 of them pass its limit.
	var inputs []S3File
	for i := range 200 {
		inputs = append(inputs, S3File{Name: strings.Repeat("i", 200) + strconv.Itoa(i), Body: bytes.NewReader([]byte("x"))})
	}
	_, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "x", S3Inputs: inputs,
		EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "over the 64 KiB limit") {
		t.Fatalf("submit = %v; want the envelope's size refused", err)
	}
	if end := f.tasksEnd(t, "pool-s3"); end != 0 {
		t.Fatalf("%d tasks published", end)
	}
}

// refusingStore is a synthetic S3 store that answers every signed request with InvalidAccessKeyId naming the access
// key that signed it, as a store does for a key it does not know, trusted by the client for the rest of t.
func refusingStore(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		if r.Method == http.MethodGet && r.URL.Query().Has("location") {
			fmt.Fprint(w, "<LocationConstraint></LocationConstraint>")
			return
		}
		_, signer, _ := strings.Cut(r.Header.Get("Authorization"), "Credential=")
		access, _, _ := strings.Cut(signer, "/")
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprintf(w, "<Error><Code>InvalidAccessKeyId</Code><Message>no access key %s</Message></Error>", access)
	}))
	t.Cleanup(server.Close)
	return server
}

// A storage key file padded with spaces and a tab is held for redaction as the key the host signs with, so an upload
// error carrying that key leaves the store with the key's marker in its place.
func TestPaddedStorageKeyIsRedacted(t *testing.T) {
	server := refusingStore(t)
	access, secret := filepath.Join(t.TempDir(), "access"), filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(access, []byte("  padded-access-key\t\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secret, []byte("\tpadded-secret-key  \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	h := &Host{storage: map[string]StorageConfig{"pool-s3": {AccessKey: "padded-access", SecretKey: "padded-secret"}},
		credentials: map[string]credential.Source{"padded-access": {File: access}, "padded-secret": {File: secret}}, roots: serverRoots(t, server)}
	ctx := context.Background()
	store, err := h.bucket(ctx, "pool-s3", &operator.Storage{Endpoint: server.URL, Bucket: testBucket}, newResolver(ctx, ""))
	if err != nil {
		t.Fatal(err)
	}
	file, _ := randomFile(t, "dataset.bin", 16)
	_, err = uploadInputs(ctx, store, &operator.Storage{Bucket: testBucket}, "tenant-a", "run-1", []S3File{{Name: "dataset.bin", Body: file}})
	if want := "upload S3 input dataset.bin: no access key " + signed.RedactionMarker(nil, "padded-access"); err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("upload = %v; want %q", err, want)
	}
	if strings.Contains(err.Error(), "padded-access-key") {
		t.Fatalf("upload error holds the access key: %v", err)
	}
}

// A pool's storage at http://127.0.0.1 is reached over plain http, and its presigned URLs are http URLs.
func TestLoopbackHTTPStorage(t *testing.T) {
	var puts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		switch {
		case r.Method == http.MethodGet && r.URL.Query().Has("location"):
			fmt.Fprint(w, "<LocationConstraint></LocationConstraint>")
		case r.Method == http.MethodPut:
			io.Copy(io.Discard, r.Body)
			puts.Add(1)
			w.Header().Set("ETag", `"0"`)
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)
	access, secret := filepath.Join(t.TempDir(), "access"), filepath.Join(t.TempDir(), "secret")
	for _, file := range []string{access, secret} {
		if err := os.WriteFile(file, []byte("key"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	h := &Host{storage: map[string]StorageConfig{"pool-s3": {AccessKey: "access", SecretKey: "secret"}},
		credentials: map[string]credential.Source{"access": {File: access}, "secret": {File: secret}}}
	ctx := context.Background()
	storage := &operator.Storage{Endpoint: server.URL, Bucket: testBucket}
	store, err := h.bucket(ctx, "pool-s3", storage, newResolver(ctx, ""))
	if err != nil {
		t.Fatal(err)
	}
	file, _ := randomFile(t, "dataset.bin", 16)
	if _, err := uploadInputs(ctx, store, &operator.Storage{Bucket: testBucket}, "tenant-a", "run-1", []S3File{{Name: "dataset.bin", Body: file}}); err != nil || puts.Load() != 1 {
		t.Fatalf("upload = %v after %d PUTs; want one PUT over http", err, puts.Load())
	}
	urls, policy, err := presign(ctx, store, testBucket, "tenant-a", "run-1", "run-1", []string{"dataset.bin"}, time.Now().Add(getLifetime), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	get, err := url.Parse(urls["dataset.bin"])
	if err != nil || !storage.Serves(get) {
		t.Fatalf("presigned %s, %v; want a URL the pool's storage serves", get, err)
	}
	if target, err := url.Parse(policy.URL); err != nil || !storage.Serves(target) {
		t.Fatalf("policy URL %s, %v; want one the pool's storage serves", policy.URL, err)
	}
}

// Each pool's bucket opens with that pool's storage credentials; a pool the config gives none has no storage here.
func TestStorageCredentialsArePerPool(t *testing.T) {
	h := &Host{storage: map[string]StorageConfig{"pool-a": {AccessKey: "a-access", SecretKey: "a-secret"}, "pool-b": {AccessKey: "b-access", SecretKey: "b-secret"}},
		credentials: map[string]credential.Source{}}
	for _, name := range []string{"a-access", "a-secret", "b-access", "b-secret"} {
		h.credentials[name] = credential.Source{Command: []string{"printf", name + "-value"}}
	}
	pool := &operator.Storage{Endpoint: "https://s3.example.test", Bucket: testBucket}
	for _, name := range []string{"a", "b"} {
		store, err := h.bucket(context.Background(), "pool-"+name, pool, newResolver(context.Background(), ""))
		if err != nil {
			t.Fatal(err)
		}
		if creds, err := store.client.GetCreds(); err != nil || creds.AccessKeyID != name+"-access-value" || creds.SecretAccessKey != name+"-secret-value" {
			t.Fatalf("pool-%s's S3 client holds %q and %q, %v; want pool-%s's pair", name, creds.AccessKeyID, creds.SecretAccessKey, err, name)
		}
	}
	if _, err := h.bucket(context.Background(), "pool-c", pool, newResolver(context.Background(), "")); !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "no storage credential for pool pool-c") {
		t.Fatalf("bucket of a pool without storage credentials = %v", err)
	}
}

// twoPoolS3Fixture adds pool-b beside pool-a, both with storage at a fake S3 store and each with its own storage
// credentials on the host, and a live receiver in pool alone.
func twoPoolS3Fixture(t *testing.T, pool string) (*uploadOracle, *fixture, *testReceiver) {
	t.Helper()
	o := newUploadOracle(t)
	f := newFixture(t)
	f.host.roots = o.roots
	terms := testTerms()
	terms.Storage = &operator.Storage{Endpoint: o.endpoint, Bucket: testBucket}
	f.host.storage = map[string]StorageConfig{}
	for _, name := range []string{"pool-a", "pool-b"} {
		if err := f.service.PoolApply(f.ctx, name, terms, false); err != nil {
			t.Fatal(err)
		}
		f.host.storage[name] = StorageConfig{AccessKey: name + "-access", SecretKey: name + "-secret"}
		for _, key := range []string{name + "-access", name + "-secret"} {
			f.host.credentials[key] = credential.Source{Command: []string{"printf", key + "-value"}}
		}
	}
	if err := f.service.TenantPoolAdd(f.ctx, "tenant-a", "pool-b"); err != nil {
		t.Fatal(err)
	}
	receiver := f.addReceiver(t, "receiver-"+pool, pool)
	receiver.advertise(t, f.ctx, nil)
	return o, f, receiver
}

// Submit, its presigned URLs, resubmit, cleanup and download each reach S3 with the run's pool's storage credentials
// when another pool's are configured too.
func TestStorageReadersUseTheRunsPoolsCredentials(t *testing.T) {
	for _, pool := range []string{"pool-a", "pool-b"} {
		t.Run(pool, func(t *testing.T) {
			o, f, receiver := twoPoolS3Fixture(t, pool)
			want := pool + "-access-value"
			signedBy := func(step string) {
				t.Helper()
				o.mu.Lock()
				defer o.mu.Unlock()
				if len(o.access) == 0 || slices.ContainsFunc(o.access, func(access string) bool { return access != want }) {
					t.Fatalf("%s signed S3 requests with %q; want %s alone", step, o.access, want)
				}
				o.access = nil
			}
			presignedBy := func(step, runID string) {
				t.Helper()
				_, values, err := open(t, f, runID, f.poolKeys[pool])
				if err != nil {
					t.Fatal(err)
				}
				for _, raw := range values.S3Inputs {
					if parsed, err := url.Parse(raw); err != nil || !strings.HasPrefix(parsed.Query().Get("X-Amz-Credential"), want+"/") {
						t.Fatalf("%s presigned %s, %v; want %s's credential", step, raw, err, want)
					}
				}
				if values.S3Policy == nil || !strings.HasPrefix(values.S3Policy.Fields["x-amz-credential"], want+"/") {
					t.Fatalf("%s presigned the policy %+v; want %s's credential", step, values.S3Policy, want)
				}
			}
			submit := func() claimed {
				t.Helper()
				file, _ := randomFile(t, "dataset.bin", 1024)
				submitted, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "do the work", S3Inputs: []S3File{{Name: "dataset.bin", Body: file}},
					Outputs: []wire.ExpectedOutput{{Name: "report.pdf"}}, EndBy: time.Now().Add(time.Hour), StartBy: time.Now().Add(time.Minute)})
				if err != nil || submitted.Pool != pool {
					t.Fatalf("submit = %+v, %v; want pool %s", submitted, err, pool)
				}
				task := receiver.claim(t, f.ctx, submitted.RunID)[0]
				receiver.feed(t, f.ctx, submitted.RunID, receiver.echo(t, task), receiver.status(t, task, 1, "claimed-1", "claimed", wire.TaskStateWorking, nil))
				signedBy("submit")
				presignedBy("submit", submitted.RunID)
				return task
			}
			task := submit()
			receiver.keyLost(t, f.ctx, task)
			replacementID, err := f.host.resubmit(f.ctx, task.submission.Wherry.Run, false)
			if err != nil || replacementID == "" {
				t.Fatalf("resubmit = %q, %v", replacementID, err)
			}
			presignedBy("resubmit", replacementID)
			next := receiver.claim(t, f.ctx, replacementID)[0]
			receiver.feed(t, f.ctx, replacementID, receiver.echo(t, next), receiver.status(t, next, 1, "rejected-1", "rejected", wire.TaskStateRejected, nil))
			if err := f.host.Cleanup(f.ctx, replacementID); err != nil {
				t.Fatal(err)
			}
			signedBy("cleanup")
			task = submit()
			runID := task.submission.Wherry.Run
			key := wire.S3OutputKey("tenant-a", runID, "report.pdf")
			o.mu.Lock()
			o.objects["/"+testBucket+"/"+key] = []byte("report")
			o.mu.Unlock()
			receiver.feed(t, f.ctx, runID, receiver.s3Result(t, task, []wire.ReturnedOutput{{Name: "report.pdf", SHA256: hexSum([]byte("report")), Size: 6, Key: key}})...)
			awaited, err := f.host.Await(f.ctx, runID, 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if downloaded, rejected, err := f.host.DownloadOutputs(f.ctx, runID, awaited, t.TempDir()); err != nil || len(downloaded) != 1 || len(rejected) != 0 {
				t.Fatalf("downloaded %+v, rejected %+v, %v", downloaded, rejected, err)
			}
			signedBy("download")
		})
	}
}

// A pool with storage and no storage entry is storage-less on a host whose config gives another pool one.
func TestSubmitToAPoolWithoutAStorageEntryBesideOneWithIt(t *testing.T) {
	f, o, _ := recordLimitFixture(t, true)
	f.host.storage = map[string]StorageConfig{"pool-other": *o.config}
	_, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Parts: []wire.Part{{Raw: make([]byte, 60000), Filename: "data.bin"}}, EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "-byte record, over pool pool-r's 100000-byte record limit; a pool with storage and a host storage credential") {
		t.Fatalf("submit = %v; want the pool refused as storage-less", err)
	}
}

// The pair a host's bundle carries serves every pool with storage that its configuration maps to no credentials, and a
// mapping for a pool wins over it. Both of the bundle's values are held for redaction before any storage call. A host
// with neither a mapping nor a bundled pair refuses, naming both.
func TestBundleStorageKeysServeUnmappedPools(t *testing.T) {
	bundled := operator.StorageKeys{AccessKey: "AKIABUNDLEACCESSKEY", SecretKey: "bundle-secret-key-value-sentinel"}
	h := &Host{bundleStorage: bundled, storage: map[string]StorageConfig{"pool-b": {AccessKey: "b-access", SecretKey: "b-secret"}}, credentials: map[string]credential.Source{}}
	for _, name := range []string{"b-access", "b-secret"} {
		h.credentials[name] = credential.Source{Command: []string{"printf", name + "-value"}}
	}
	pool := &operator.Storage{Endpoint: "https://s3.example.test", Bucket: testBucket}
	ctx := context.Background()
	store, err := h.bucket(ctx, "pool-a", pool, newResolver(ctx, ""))
	if err != nil {
		t.Fatal(err)
	}
	if creds, err := store.client.GetCreds(); err != nil || creds.AccessKeyID != bundled.AccessKey || creds.SecretAccessKey != bundled.SecretKey {
		t.Fatalf("pool-a's S3 client holds %q and %q, %v; want the bundle's pair", creds.AccessKeyID, creds.SecretAccessKey, err)
	}
	for _, value := range []string{bundled.AccessKey, bundled.SecretKey} {
		if got := Redact("error naming " + value); strings.Contains(got, value) {
			t.Fatalf("redacted text %q holds a bundled key; want it held before the storage call", got)
		}
	}
	store, err = h.bucket(ctx, "pool-b", pool, newResolver(ctx, ""))
	if err != nil {
		t.Fatal(err)
	}
	if creds, err := store.client.GetCreds(); err != nil || creds.AccessKeyID != "b-access-value" || creds.SecretAccessKey != "b-secret-value" {
		t.Fatalf("pool-b's S3 client holds %q and %q, %v; want the configuration's pair, which wins", creds.AccessKeyID, creds.SecretAccessKey, err)
	}
	if !h.credentialed("pool-a") || !h.credentialed("pool-b") || !h.credentialed("pool-c") {
		t.Fatal("a host with a bundled pair reports a pool without storage keys")
	}
	bare := &Host{storage: map[string]StorageConfig{"pool-b": {AccessKey: "b-access", SecretKey: "b-secret"}}, credentials: h.credentials}
	if bare.credentialed("pool-a") || !bare.credentialed("pool-b") {
		t.Fatal("a host without a bundled pair reports storage keys for an unmapped pool")
	}
	_, err = bare.bucket(ctx, "pool-a", pool, newResolver(ctx, ""))
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "no storage credential for pool pool-a's S3 inputs and outputs, and its bundle carries no storage keys") {
		t.Fatalf("bucket without a mapping or a bundled pair = %v; want both named", err)
	}
}

// A host whose bundle carries the tenant's pair signs its storage requests with it. The pair reaches no run record in
// the host's state, no task record on the broker in the clear and no log; the envelope's presigned URLs carry the access
// key identifier alone, never the secret key.
func TestBundleStorageKeysReachNoStateRecordOrLog(t *testing.T) {
	o := newUploadOracle(t)
	f := newFixture(t)
	f.host.roots = o.roots
	terms := testTerms()
	terms.Storage = &operator.Storage{Endpoint: o.endpoint, Bucket: testBucket}
	if err := f.service.PoolApply(f.ctx, "pool-s3", terms, false); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantPoolAdd(f.ctx, "tenant-a", "pool-s3"); err != nil {
		t.Fatal(err)
	}
	bundled := operator.StorageKeys{AccessKey: "AKIABUNDLEACCESSKEY", SecretKey: "bundle-secret-key-value-sentinel"}
	f.host.bundleStorage = bundled
	receiver := f.addReceiver(t, "receiver-s3", "pool-s3")
	receiver.advertise(t, f.ctx, nil)
	file, _ := randomFile(t, "dataset.bin", 1024)
	submitted, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "do the work", S3Inputs: []S3File{{Name: "dataset.bin", Body: file}},
		Outputs: []wire.ExpectedOutput{{Name: "report.pdf"}}, EndBy: time.Now().Add(time.Hour), StartBy: time.Now().Add(time.Minute)})
	if err != nil || submitted.Pool != "pool-s3" {
		t.Fatalf("submit = %+v, %v; want pool-s3", submitted, err)
	}
	o.mu.Lock()
	signedBy := slices.Clone(o.access)
	o.mu.Unlock()
	if len(signedBy) == 0 || slices.ContainsFunc(signedBy, func(access string) bool { return access != bundled.AccessKey }) {
		t.Fatalf("storage requests signed with %q; want the bundle's access key alone", signedBy)
	}
	_, values, err := open(t, f, submitted.RunID, f.poolKeys["pool-s3"])
	if err != nil {
		t.Fatal(err)
	}
	if len(values.S3Inputs) != 1 || values.S3Policy == nil {
		t.Fatalf("enveloped %d input URLs and policy %+v; want one URL and a policy", len(values.S3Inputs), values.S3Policy)
	}
	for _, raw := range values.S3Inputs {
		parsed, err := url.Parse(raw)
		if err != nil || !strings.HasPrefix(parsed.Query().Get("X-Amz-Credential"), bundled.AccessKey+"/") {
			t.Fatalf("enveloped URL %s, %v; want it signed with the bundle's access key", raw, err)
		}
		if strings.Contains(raw, bundled.SecretKey) || strings.Contains(raw, url.QueryEscape(bundled.SecretKey)) {
			t.Fatalf("enveloped URL %s holds the secret key", raw)
		}
	}
	if credential := values.S3Policy.Fields["x-amz-credential"]; !strings.HasPrefix(credential, bundled.AccessKey+"/") {
		t.Fatalf("enveloped policy credential %s; want the bundle's access key", credential)
	}
	for _, value := range values.S3Policy.Fields {
		if strings.Contains(value, bundled.SecretKey) {
			t.Fatal("an enveloped policy field holds the secret key")
		}
	}
	records, err := readTasks(f.ctx, receiver.kafka, "pool-s3")
	if err != nil || len(records) == 0 {
		t.Fatalf("task records %d, %v", len(records), err)
	}
	for _, record := range records {
		for _, value := range []string{bundled.AccessKey, bundled.SecretKey} {
			if bytes.Contains(record.Value, []byte(value)) {
				t.Fatalf("the task record holds a bundled storage key in the clear:\n%s", record.Value)
			}
		}
	}
	err = filepath.WalkDir(f.stateDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, value := range []string{bundled.AccessKey, bundled.SecretKey} {
			if bytes.Contains(contents, []byte(value)) {
				return fmt.Errorf("%s holds a bundled storage key", path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(f.stateDir, "runs", submitted.RunID+".json")); err != nil {
		t.Fatalf("the run's record was not scanned: %v", err)
	}
	for _, value := range []string{bundled.AccessKey, bundled.SecretKey, "X-Amz"} {
		if strings.Contains(f.logs.String(), value) {
			t.Fatalf("host logs hold %q:\n%s", value, f.logs.String())
		}
	}
}

// A store that refuses an upload naming the access key that signed it leaves neither storage key in the error Submit
// returns nor in the host's log of the cleanup that follows: both values are replaced by their markers where they leave
// the storage layer, whether the pair is the bundle's, the pair the configuration maps the pool to, or a bundled pair
// whose access key is seven bytes, under the minimum the redactor keeps for other credentials. The error still unwraps
// to the store's refusal, so callers classify it as before, and no task is published.
func TestStorageErrorsAndLogsNameNoStorageKey(t *testing.T) {
	server := refusingStore(t)
	f := newFixture(t)
	f.host.roots = serverRoots(t, server)
	terms := testTerms()
	terms.Storage = &operator.Storage{Endpoint: server.URL, Bucket: testBucket}
	if err := f.service.PoolApply(f.ctx, "pool-s3", terms, false); err != nil {
		t.Fatal(err)
	}
	if err := f.service.TenantPoolAdd(f.ctx, "tenant-a", "pool-s3"); err != nil {
		t.Fatal(err)
	}
	receiver := f.addReceiver(t, "receiver-s3", "pool-s3")
	receiver.advertise(t, f.ctx, nil)
	mapped := operator.StorageKeys{AccessKey: "AKIAMAPPEDACCESSKEY", SecretKey: "mapped-secret-key-value-sentinel"}
	dir := t.TempDir()
	for name, value := range map[string]string{"s3-access": mapped.AccessKey, "s3-secret": mapped.SecretKey} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		f.host.credentials[name] = credential.Source{File: filepath.Join(dir, name)}
	}
	for _, tc := range []struct {
		name       string
		bundle     operator.StorageKeys
		mapped     bool
		keys       operator.StorageKeys
		accessName string
	}{
		{"bundled pair", operator.StorageKeys{AccessKey: "AKIABUNDLEACCESSKEY", SecretKey: "bundle-secret-key-value-sentinel"}, false, operator.StorageKeys{}, bundleAccessKey},
		{"configured pair wins over the bundle", operator.StorageKeys{AccessKey: "AKIABUNDLEACCESSKEY", SecretKey: "bundle-secret-key-value-sentinel"}, true, mapped, "s3-access"},
		{"seven-byte bundled access key", operator.StorageKeys{AccessKey: "a7x9key", SecretKey: "short-pair-secret-key-sentinel"}, false, operator.StorageKeys{}, bundleAccessKey},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f.host.bundleStorage, f.host.storage = tc.bundle, nil
			if tc.mapped {
				f.host.storage = map[string]StorageConfig{"pool-s3": {AccessKey: "s3-access", SecretKey: "s3-secret"}}
			}
			keys := tc.keys
			if keys == (operator.StorageKeys{}) {
				keys = tc.bundle
			}
			before := len(f.logs.String())
			file, _ := randomFile(t, "dataset.bin", 16)
			_, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", S3Inputs: []S3File{{Name: "dataset.bin", Body: file}},
				EndBy: time.Now().Add(time.Hour), StartBy: time.Now().Add(time.Minute)})
			marker := signed.RedactionMarker(nil, tc.accessName)
			if want := "upload S3 input dataset.bin: no access key " + marker; err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("submit = %v; want %q", err, want)
			}
			var refusal minio.ErrorResponse
			if !errors.As(err, &refusal) || refusal.Code != "InvalidAccessKeyId" {
				t.Fatalf("submit = %v; want it to unwrap to the store's InvalidAccessKeyId", err)
			}
			logged := f.logs.String()[before:]
			if want := "delete the S3 inputs of a task not published: no access key " + marker; !strings.Contains(logged, want) {
				t.Fatalf("host logs\n%s\nwant %q", logged, want)
			}
			for _, value := range []string{keys.AccessKey, keys.SecretKey} {
				for what, text := range map[string]string{"the submit error": err.Error(), "the host's logs": logged, "Redact": Redact("error naming " + value)} {
					if strings.Contains(text, value) {
						t.Fatalf("%s holds the storage key %q:\n%s", what, value, text)
					}
				}
			}
			if end := f.tasksEnd(t, "pool-s3"); end != 0 {
				t.Fatalf("%d tasks published", end)
			}
		})
	}
}

// The store fetches an object at the first Read of the reader get returns, so a refusal naming the access key that
// signed the request arrives through the reader after get succeeded. The reader's errors cross the store's boundary as
// its other errors do: Read and Close name the key's marker, never the key, and the error still unwraps to the store's
// refusal. A reader the store serves ends in io.EOF itself, which io.ReadAll and io.Copy end on.
func TestObjectReaderErrorsNameNoStorageKey(t *testing.T) {
	ctx := context.Background()
	key := wire.S3OutputKey("tenant-a", "run-1", "report.pdf")
	t.Run("a refusal at the first read", func(t *testing.T) {
		server := refusingStore(t)
		bundled := operator.StorageKeys{AccessKey: "AKIAREADERACCESSKEY", SecretKey: "reader-secret-key-value-sentinel"}
		h := &Host{bundleStorage: bundled, roots: serverRoots(t, server)}
		store, err := h.bucket(ctx, "pool-s3", &operator.Storage{Endpoint: server.URL, Bucket: testBucket}, newResolver(ctx, ""))
		if err != nil {
			t.Fatal(err)
		}
		object, err := store.get(ctx, testBucket, key)
		if err != nil {
			t.Fatalf("get = %v; want the refusal deferred to the first read", err)
		}
		_, err = io.ReadAll(object)
		if want := "no access key " + signed.RedactionMarker(nil, bundleAccessKey); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("read = %v; want %q", err, want)
		}
		var refusal minio.ErrorResponse
		if !errors.As(err, &refusal) || refusal.Code != "InvalidAccessKeyId" {
			t.Fatalf("read = %v; want it to unwrap to the store's InvalidAccessKeyId", err)
		}
		closeErr := object.Close()
		for what, text := range map[string]string{"the read error": err.Error(), "the close error": fmt.Sprint(closeErr)} {
			for _, value := range []string{bundled.AccessKey, bundled.SecretKey} {
				if strings.Contains(text, value) {
					t.Fatalf("%s holds the storage key %q: %s", what, value, text)
				}
			}
		}
	})
	t.Run("a served object", func(t *testing.T) {
		o := newUploadOracle(t)
		o.objects["/"+testBucket+"/"+key] = []byte("report")
		h := &Host{storage: map[string]StorageConfig{"pool-s3": *o.config}, credentials: o.sources, roots: o.roots}
		store, err := h.bucket(ctx, "pool-s3", &operator.Storage{Endpoint: o.endpoint, Bucket: testBucket}, newResolver(ctx, ""))
		if err != nil {
			t.Fatal(err)
		}
		object, err := store.get(ctx, testBucket, key)
		if err != nil {
			t.Fatal(err)
		}
		if data, err := io.ReadAll(object); err != nil || string(data) != "report" {
			t.Fatalf("read %q, %v; want the object and io.EOF passed through unchanged", data, err)
		}
		if err := object.Close(); err != nil {
			t.Fatalf("close = %v", err)
		}
	})
}
