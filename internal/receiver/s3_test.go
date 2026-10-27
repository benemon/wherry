package receiver

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benemon/wherry/adapter"
	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	s3Bucket    = "wherry-test"
	s3Signature = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	s3Content   = "dataset bytes"
)

// store is a TLS server standing in for the pool's S3 endpoint: it keeps each object PUT to it, by path.
type store struct {
	server *httptest.Server
	mu     sync.Mutex
	puts   map[string][]byte
	// handle, when set, answers instead.
	handle func(http.ResponseWriter, *http.Request)
}

func newStore(t *testing.T) *store {
	t.Helper()
	s := &store{puts: map[string][]byte{}}
	s.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.handle != nil {
			s.handle(w, r)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || r.Method != http.MethodPut {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.puts[r.URL.Path] = body
		s.mu.Unlock()
	}))
	t.Cleanup(s.server.Close)
	// The upload client keeps its redirect policy; only its transport trusts the test server.
	previous := storageClient.Transport
	storageClient.Transport = s.server.Client().Transport
	t.Cleanup(func() { storageClient.Transport = previous })
	return s
}

func (s *store) endpoint() string { return s.server.URL }

func (s *store) put(path string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.puts[path]
	return data, ok
}

// presigned is a presigned URL as the host mints one, for key in the test bucket at origin.
func presigned(origin, key string) string {
	return origin + "/" + s3Bucket + "/" + key + "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Expires=900&X-Amz-Signature=" + s3Signature
}

func s3Terms(endpoint string) admin.Terms {
	terms := testTerms()
	terms.Storage = &admin.Storage{Endpoint: endpoint, Bucket: s3Bucket}
	return terms
}

func s3Fixture(t *testing.T, s *store, stream []string, outputs []adapter.Output) fixture {
	t.Helper()
	return newFixtureWith(t, stream, s3Terms(s.endpoint()), testConfig(testReceiver), map[string]*fakeAdapter{"nomad": {stream: stream, outputs: outputs}})
}

func withS3(task *wire.Submission) {
	task.Wherry.S3Inputs = []wire.S3Input{{Name: "dataset.bin", Size: int64(len(s3Content)), SHA256: sha([]byte(s3Content))}}
	task.Wherry.Outputs = []wire.ExpectedOutput{{Name: "report.pdf", Required: true, Storage: wire.StorageS3}}
}

// sealS3 answers the turn's seal request with the agent's credential and the URLs urls makes for its S3 objects.
func (f fixture) sealS3(t *testing.T, urls func(input, output string) (string, string)) wire.SealRequest {
	t.Helper()
	request := f.request(t, 1)
	values := map[string][]byte{credentialName: []byte(testToken)}
	for _, input := range request.S3Inputs {
		get, _ := urls(input, "")
		values[signed.S3InputValue(input)] = []byte(get)
	}
	for _, output := range request.S3Outputs {
		_, put := urls("", output)
		values[signed.S3OutputValue(output)] = []byte(put)
	}
	f.sealFor(t, request, values)
	return request
}

func validURLs(origin string) func(string, string) (string, string) {
	return func(input, output string) (string, string) {
		return presigned(origin, wire.S3InputKey(testTenant, "run-a", input)), presigned(origin, wire.S3OutputKey(testTenant, "run-a", output))
	}
}

func TestS3ObjectsTravelOnlyInTheSeal(t *testing.T) {
	s := newStore(t)
	f := s3Fixture(t, s, resultStream, []adapter.Output{{Name: "report.pdf", Size: 6, Body: io.NopCloser(strings.NewReader("report"))}})
	record := f.submit(t, f.host, withS3)
	done := f.executing(t, f.ctx, record, 1)
	request := f.sealS3(t, validURLs(s.endpoint()))
	if !slices.Equal(request.S3Inputs, []string{"dataset.bin"}) || !slices.Equal(request.S3Outputs, []string{"report.pdf"}) {
		t.Fatalf("seal request asks for inputs %v and outputs %v", request.S3Inputs, request.S3Outputs)
	}
	if status := <-done; status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	run := f.lastRun()
	get := presigned(s.endpoint(), wire.S3InputKey(testTenant, "run-a", "dataset.bin"))
	if !slices.Equal(run.Fetches, []adapter.Fetch{{Name: "dataset.bin", URL: get, Size: int64(len(s3Content)), SHA256: sha([]byte(s3Content))}}) {
		t.Fatalf("fetches %+v", run.Fetches)
	}
	if prompt := string(run.Prompt); !strings.Contains(prompt, "Input files: /task/inputs/dataset.bin.") || strings.Contains(prompt, "X-Amz") || strings.Contains(prompt, s.endpoint()) {
		t.Fatalf("prompt %q; want the input's path and no URL", prompt)
	}
	// Every record on the feed and the pool's topics is free of the URLs, the signature and the endpoint's paths.
	for _, topic := range []string{admin.TenantTopic(testTenant), admin.TenantTranscriptTopic(testTenant), admin.PoolTasksTopic(testPool), admin.PoolEventsTopic(testPool)} {
		for _, record := range f.records(t, topic) {
			for _, never := range []string{s3Signature, "X-Amz", s.endpoint()} {
				if bytes.Contains(record.Value, []byte(never)) || bytes.Contains(decodedPayload(record.Value), []byte(never)) {
					t.Fatalf("a record on %s holds %q", topic, never)
				}
			}
		}
	}
	key := wire.S3OutputKey(testTenant, "run-a", "report.pdf")
	if data, ok := s.put("/" + s3Bucket + "/" + key); !ok || string(data) != "report" {
		t.Fatalf("store holds %q, %v for the output", data, ok)
	}
	listed := manifest(t, f.lastTask(t))
	if !slices.Equal(listed.S3Outputs, []wire.ReturnedOutput{{Name: "report.pdf", SHA256: sha([]byte("report")), Size: 6, Key: key}}) || len(listed.Outputs) != 0 {
		t.Fatalf("result lists S3 outputs %+v and outputs %+v", listed.S3Outputs, listed.Outputs)
	}
	for _, output := range f.outputChunks(t) {
		t.Fatalf("an S3 output went on the transcript topic as chunk %s", output.ArtifactUpdate.Artifact.ArtifactID)
	}
}

// decodedPayload is a compact JWS's payload, decoded, or nil.
func decodedPayload(value []byte) []byte {
	parts := bytes.Split(value, []byte("."))
	if len(parts) != 3 {
		return nil
	}
	decoded, _ := base64.RawURLEncoding.DecodeString(string(parts[1]))
	return decoded
}

func TestS3URLsAreCheckedAgainstPoolStorage(t *testing.T) {
	s := newStore(t)
	origin := s.endpoint()
	parsed, _ := url.Parse(origin)
	inputKey := wire.S3InputKey(testTenant, "run-a", "dataset.bin")
	for name, get := range map[string]string{
		"http":           strings.Replace(presigned(origin, inputKey), "https://", "http://", 1),
		"another host":   presigned("https://127.0.0.2:"+parsed.Port(), inputKey),
		"another port":   presigned("https://127.0.0.1:1", inputKey),
		"default port":   presigned("https://127.0.0.1", inputKey),
		"another run":    presigned(origin, wire.S3InputKey(testTenant, "run-b", "dataset.bin")),
		"another tenant": presigned(origin, wire.S3InputKey("tenant-b", "run-a", "dataset.bin")),
		"another bucket": strings.Replace(presigned(origin, inputKey), "/"+s3Bucket+"/", "/other-bucket/", 1),
		"userinfo":       strings.Replace(presigned(origin, inputKey), "https://", "https://user:pass@", 1),
		"dot segments":   presigned(origin, wire.S3InputKey(testTenant, "run-a", "x/../dataset.bin")),
	} {
		t.Run(name, func(t *testing.T) {
			f := s3Fixture(t, s, resultStream, nil)
			done := f.executing(t, f.ctx, f.submit(t, f.host, withS3), 1)
			f.sealS3(t, func(input, output string) (string, string) {
				_, put := validURLs(origin)(input, output)
				return get, put
			})
			if status := <-done; status != kgo.AckReject {
				t.Fatalf("status %v; want the turn failed", status)
			}
			if dispatches, _, _ := f.dispatched(t); dispatches != 0 {
				t.Fatal("dispatched with a URL off the pool's storage")
			}
			status := f.lastTask(t).Status
			if status.State != wire.TaskStateFailed || status.Message.Parts[0].Text != "the URL for S3 input dataset.bin is not one of pool pool-a's storage" {
				t.Fatalf("status %+v", status)
			}
			if strings.Contains(f.logs.String(), s3Signature) {
				t.Fatalf("logs hold the URL:\n%s", f.logs.String())
			}
		})
	}
	t.Run("output", func(t *testing.T) {
		f := s3Fixture(t, s, resultStream, nil)
		done := f.executing(t, f.ctx, f.submit(t, f.host, withS3), 1)
		f.sealS3(t, func(input, output string) (string, string) {
			get, _ := validURLs(origin)(input, output)
			return get, presigned("https://127.0.0.1:1", wire.S3OutputKey(testTenant, "run-a", output))
		})
		if status := <-done; status != kgo.AckReject || f.lastTask(t).Status.Message.Parts[0].Text != "the URL for S3 output report.pdf is not one of pool pool-a's storage" {
			t.Fatalf("status %v, %+v", status, f.lastTask(t).Status)
		}
	})
}

func TestS3UseWithoutPoolStorageIsRejected(t *testing.T) {
	for name, edit := range map[string]func(*wire.Submission){
		"input": func(task *wire.Submission) {
			task.Wherry.S3Inputs = []wire.S3Input{{Name: "dataset.bin", SHA256: sha(nil)}}
		},
		"output": func(task *wire.Submission) {
			task.Wherry.Outputs = []wire.ExpectedOutput{{Name: "report.pdf", Storage: wire.StorageS3}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, resultStream)
			if admitted, ack := f.receiver.admit(f.ctx, f.submit(t, f.host, edit)); admitted != nil || ack != kgo.AckReject {
				t.Fatalf("admitted %v with ack %v", admitted, ack)
			}
			if status := f.feedStatuses(t); len(status) != 1 || status[0].StatusUpdate.Status.Message.Parts[0].Text != "pool pool-a has no storage for S3 inputs and outputs" {
				t.Fatalf("statuses %+v", status)
			}
		})
	}
}

// The agent prints its input's URL whole and its signature alone; neither reaches the transcript or the result.
func TestS3URLsAreRedacted(t *testing.T) {
	s := newStore(t)
	get := presigned(s.endpoint(), wire.S3InputKey(testTenant, "run-a", "dataset.bin"))
	quoted := strings.ReplaceAll(get, "&", `&`)
	stream := []string{`{"type":"assistant","message":"fetched ` + get + `"}`, `{"type":"assistant","message":"signature ` + s3Signature + `"}`,
		completedWith("used " + quoted)}
	f := s3Fixture(t, s, stream, []adapter.Output{{Name: "report.pdf", Size: 6, Body: io.NopCloser(strings.NewReader("report"))}})
	done := f.executing(t, f.ctx, f.submit(t, f.host, withS3), 1)
	f.sealS3(t, validURLs(s.endpoint()))
	if status := <-done; status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	// The URL and its signature alone take a URL's marker; the fixture signs both URLs alike, so the signature is a form
	// of either.
	input, output := signed.RedactionMarker(testRedactionKey, signed.S3InputValue("dataset.bin")), signed.RedactionMarker(testRedactionKey, signed.S3OutputValue("report.pdf"))
	for i, chunk := range f.chunks(t) {
		if text := chunk.ArtifactUpdate.Artifact.Parts[0].Text; strings.Contains(text, s3Signature) || !strings.Contains(text, input) && (i == 0 || !strings.Contains(text, output)) {
			t.Fatalf("transcript line %q, want a URL's marker", text)
		}
	}
	if result := f.lastTask(t).Artifacts[0].Parts[0].Text; strings.Contains(result, "X-Amz-Signature") || strings.Contains(result, s3Signature) {
		t.Fatalf("result %q holds the URL", result)
	}
}

// pausedBody yields its first half, then waits until the store has received it before yielding the rest, so only an
// upload that streams can finish.
type pausedBody struct {
	first, rest []byte
	received    <-chan struct{}
	done        bool
}

func (b *pausedBody) Read(p []byte) (int, error) {
	if len(b.first) > 0 {
		n := copy(p, b.first)
		b.first = b.first[n:]
		return n, nil
	}
	if !b.done {
		select {
		case <-b.received:
		case <-time.After(5 * time.Second):
			return 0, errors.New("the upload read the first half and sent none of it")
		}
		b.done = true
	}
	if len(b.rest) == 0 {
		return 0, io.EOF
	}
	n := copy(p, b.rest)
	b.rest = b.rest[n:]
	return n, nil
}

func (b *pausedBody) Close() error { return nil }

func TestS3OutputIsStreamedAndHashedAsSent(t *testing.T) {
	s := newStore(t)
	half := 512 << 10
	content := append(bytes.Repeat([]byte("a"), half), bytes.Repeat([]byte("b"), half)...)
	// The output holds the delivered credential, which an S3 output keeps: it goes to the host's own bucket.
	copy(content, testToken)
	received := make(chan struct{})
	var stored []byte
	s.handle = func(w http.ResponseWriter, r *http.Request) {
		first := make([]byte, half)
		if _, err := io.ReadFull(r.Body, first); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		close(received)
		rest, err := io.ReadAll(r.Body)
		if err != nil || r.ContentLength != int64(len(content)) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		stored = append(first, rest...)
	}
	body := &pausedBody{first: content[:half], rest: content[half:], received: received}
	f := s3Fixture(t, s, resultStream, []adapter.Output{{Name: "report.pdf", Size: int64(len(content)), Body: body}})
	done := f.executing(t, f.ctx, f.submit(t, f.host, withS3), 1)
	f.sealS3(t, validURLs(s.endpoint()))
	if status := <-done; status != kgo.AckAccept {
		t.Fatalf("status %v; not returned %+v; logs:\n%s", status, manifest(t, f.lastTask(t)).NotReturned, f.logs.String())
	}
	sum := sha256.Sum256(stored)
	listed := manifest(t, f.lastTask(t)).S3Outputs
	if !bytes.Equal(stored, content) || len(listed) != 1 || listed[0].SHA256 != hex.EncodeToString(sum[:]) || listed[0].Size != int64(len(stored)) {
		t.Fatalf("stored %d bytes; result lists %+v; want the bytes sent, unredacted, and their hash", len(stored), listed)
	}
}

// A 302 would turn the PUT into a GET elsewhere, which the client refuses; a 307 would repeat the PUT, which a streamed
// body cannot, so it ends at the redirect's status.
func TestS3OutputUploadFollowsNoRedirect(t *testing.T) {
	for status, reason := range map[int]string{http.StatusFound: "not uploaded: redirected", http.StatusTemporaryRedirect: "not uploaded: status 307"} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			s := newStore(t)
			elsewhere := newStore(t)
			var hits atomic.Int64
			elsewhere.handle = func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }
			s.handle = func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, elsewhere.server.URL+r.URL.RequestURI(), status)
			}
			f := s3Fixture(t, s, resultStream, []adapter.Output{{Name: "report.pdf", Size: 6, Body: io.NopCloser(strings.NewReader("report"))}})
			done := f.executing(t, f.ctx, f.submit(t, f.host, withS3), 1)
			f.sealS3(t, validURLs(s.endpoint()))
			if status := <-done; status != kgo.AckReject {
				t.Fatalf("status %v; want the required output's turn failed", status)
			}
			if hits.Load() != 0 {
				t.Fatal("the upload followed the redirect")
			}
			listed := manifest(t, f.lastTask(t))
			if len(listed.S3Outputs) != 0 || !slices.Equal(listed.NotReturned, []wire.UnreturnedOutput{{Name: "report.pdf", Size: 6, Reason: reason}}) {
				t.Fatalf("result lists %+v and %+v", listed.S3Outputs, listed.NotReturned)
			}
			if status := f.lastTask(t).Status; status.State != wire.TaskStateFailed || !strings.Contains(status.Message.Parts[0].Text, "required output report.pdf not returned") {
				t.Fatalf("status %+v", status)
			}
		})
	}
}

func TestS3OutputRefusedByTheStoreIsNotReturned(t *testing.T) {
	s := newStore(t)
	s.handle = func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		io.WriteString(w, "<Error>"+r.URL.RawQuery+"</Error>")
	}
	f := s3Fixture(t, s, resultStream, []adapter.Output{{Name: "report.pdf", Size: 6, Body: io.NopCloser(strings.NewReader("report"))}})
	done := f.executing(t, f.ctx, f.submit(t, f.host, withS3), 1)
	f.sealS3(t, validURLs(s.endpoint()))
	<-done
	if listed := manifest(t, f.lastTask(t)); !slices.Equal(listed.NotReturned, []wire.UnreturnedOutput{{Name: "report.pdf", Size: 6, Reason: "not uploaded: status 403"}}) {
		t.Fatalf("not returned %+v", listed.NotReturned)
	}
	if strings.Contains(f.logs.String(), s3Signature) {
		t.Fatalf("logs hold the store's answer:\n%s", f.logs.String())
	}
}

func TestS3FetchFailureFailsTheTurn(t *testing.T) {
	s := newStore(t)
	f := newFixtureWith(t, noResultStream, s3Terms(s.endpoint()), testConfig(testReceiver), map[string]*fakeAdapter{
		"nomad": {stream: noResultStream, neverStarted: true, neverClass: adapter.ClassFetch},
	})
	done := f.executing(t, f.ctx, f.submit(t, f.host, withS3), 1)
	f.sealS3(t, validURLs(s.endpoint()))
	if status := <-done; status != kgo.AckReject {
		t.Fatalf("status %v; want the turn failed and the task not returned to the pool", status)
	}
	if status := f.lastTask(t).Status; status.State != wire.TaskStateFailed || status.Message.Parts[0].Text != "an S3 input was not fetched, or did not match its size and SHA-256" {
		t.Fatalf("status %+v", status)
	}
}

// An adopter's seal carries fresh PUT URLs for the turn's S3 outputs and none for its inputs, which the job fetched.
func TestAnAdoptedRunReturnsAnS3Output(t *testing.T) {
	s := newStore(t)
	f := s3Fixture(t, s, resultStream, []adapter.Output{{Name: "report.pdf", Size: 6, Body: io.NopCloser(strings.NewReader("report"))}})
	f.adapter.job = adapter.JobRunning
	done := f.executing(t, f.ctx, f.submit(t, f.host, withS3), 2)
	request := f.sealS3(t, func(input, output string) (string, string) {
		return "", presigned(s.endpoint(), wire.S3OutputKey(testTenant, "run-a", output))
	})
	if len(request.S3Inputs) != 0 || !slices.Equal(request.S3Outputs, []string{"report.pdf"}) {
		t.Fatalf("adopter asks for inputs %v and outputs %v", request.S3Inputs, request.S3Outputs)
	}
	if status := <-done; status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	key := wire.S3OutputKey(testTenant, "run-a", "report.pdf")
	if data, ok := s.put("/" + s3Bucket + "/" + key); !ok || string(data) != "report" {
		t.Fatalf("store holds %q, %v for the adopted run's output", data, ok)
	}
	if listed := manifest(t, f.lastTask(t)); !slices.Equal(listed.S3Outputs, []wire.ReturnedOutput{{Name: "report.pdf", SHA256: sha([]byte("report")), Size: 6, Key: key}}) {
		t.Fatalf("result lists S3 outputs %+v, not returned %+v", listed.S3Outputs, listed.NotReturned)
	}
}

func TestAnAttemptWithoutAnUploadURLStoresNothing(t *testing.T) {
	run := &activeRun{receiver: &Receiver{}, task: testSubmission("run-a")}
	if stored := run.store(t.Context(), adapter.Output{Name: "report.pdf", Size: 6, Body: io.NopCloser(strings.NewReader("report"))}); stored.reason != "not uploaded: no upload URL" {
		t.Fatalf("stored %+v", stored)
	}
}
