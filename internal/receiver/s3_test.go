package receiver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	s3Bucket     = "wherry-test"
	s3Signature  = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	s3Credential = "AKIATESTACCESSKEY/20260101/us-east-1/s3/aws4_request"
	s3Content    = "dataset bytes"
	// s3Limit is the output limit of s3Terms: an output within it returns inline, one over it through the bucket.
	s3Limit = 1024
)

// report is the bytes of report.pdf, over s3Limit, so it goes through the bucket.
var report = bytes.Repeat([]byte("r"), 2*s3Limit)

// reportOutput is report.pdf as a plugin lists it, read as Data and streamed when named.
func reportOutput() pluginsdk.Output {
	return pluginsdk.Output{Name: "report.pdf", Size: int64(len(report)), Data: report}
}

// store is a TLS server standing in for the pool's S3 endpoint: it keeps each object PUT to it, by path, and each object
// a policy upload POSTs, at the form's key. A URL that carries X-Amz-Date and X-Amz-Expires, or a policy whose
// expiration has passed, is refused once clock, by default the wall clock, is past it.
type store struct {
	server *httptest.Server
	mu     sync.Mutex
	puts   map[string][]byte
	// posted records the key field of each policy upload, in order.
	posted []string
	// handle, when set, answers instead.
	handle func(http.ResponseWriter, *http.Request)
	clock  func() time.Time
}

func (s *store) now() time.Time {
	if s.clock != nil {
		return s.clock()
	}
	return time.Now()
}

// expired reports whether a presigned URL's lifetime has passed on the store's clock.
func (s *store) expired(query url.Values) bool {
	signedAt, err := time.Parse("20060102T150405Z", query.Get("X-Amz-Date"))
	seconds, _ := strconv.Atoi(query.Get("X-Amz-Expires"))
	if err != nil || seconds == 0 {
		return false
	}
	return s.now().After(signedAt.Add(time.Duration(seconds) * time.Second))
}

// policyExpired reports whether a policy upload's policy has expired on the store's clock; one that does not decode has.
func (s *store) policyExpired(field string) bool {
	decoded, err := base64.StdEncoding.DecodeString(field)
	var document struct {
		Expiration string `json:"expiration"`
	}
	if err != nil || json.Unmarshal(decoded, &document) != nil {
		return true
	}
	expiration, err := time.Parse(time.RFC3339Nano, document.Expiration)
	return err != nil || s.now().After(expiration)
}

func newStore(t *testing.T) *store {
	t.Helper()
	return startStore(t, httptest.NewTLSServer)
}

func startStore(t *testing.T, start func(http.Handler) *httptest.Server) *store {
	t.Helper()
	s := &store{puts: map[string][]byte{}}
	s.server = start(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.expired(r.URL.Query()) {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if s.handle != nil {
			s.handle(w, r)
			return
		}
		switch r.Method {
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			s.mu.Lock()
			s.puts[r.URL.Path] = body
			s.mu.Unlock()
		case http.MethodPost:
			fields, data, err := readPost(r)
			switch {
			case err != nil || fields["key"] == "":
				w.WriteHeader(http.StatusBadRequest)
			case s.policyExpired(fields["policy"]):
				w.WriteHeader(http.StatusForbidden)
			default:
				s.mu.Lock()
				s.puts[r.URL.Path+fields["key"]] = data
				s.posted = append(s.posted, fields["key"])
				s.mu.Unlock()
				w.WriteHeader(http.StatusNoContent)
			}
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	t.Cleanup(s.server.Close)
	// The upload client keeps its redirect policy; only its transport trusts the test server.
	previous := storageClient.Transport
	storageClient.Transport = s.server.Client().Transport
	t.Cleanup(func() { storageClient.Transport = previous })
	return s
}

// readPost reads a policy upload: its fields, then the file, which must come last, as S3 requires.
func readPost(r *http.Request) (map[string]string, []byte, error) {
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

// presignedUntil is presigned with the URL's signing time and lifetime, which the store enforces.
func presignedUntil(origin, key string, signedAt time.Time, lifetime time.Duration) string {
	return origin + "/" + s3Bucket + "/" + key + "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=" + signedAt.UTC().Format("20060102T150405Z") +
		"&X-Amz-Expires=" + strconv.Itoa(int(lifetime.Seconds())) + "&X-Amz-Signature=" + s3Signature
}

// testPolicy is a POST policy as a host presigns one for run's outputs in the test bucket at origin, signed at signedAt
// and expiring at expires, with s3Signature for its signature and s3Credential for its credential.
func testPolicy(origin, tenant, run string, signedAt, expires time.Time) signed.S3Policy {
	prefix := wire.S3OutputKey(tenant, run, "")
	fields := map[string]string{
		"key": prefix, "bucket": s3Bucket, "x-amz-algorithm": operator.SigV4Algorithm, "x-amz-credential": s3Credential,
		"x-amz-date": signedAt.UTC().Format("20060102T150405Z"), "x-amz-signature": s3Signature,
	}
	conditions := []string{
		`["eq","$bucket","` + s3Bucket + `"]`, `["starts-with","$key","` + prefix + `"]`, `["content-length-range", 0, ` + strconv.FormatInt(wire.MaxS3Object, 10) + `]`,
		`["eq","$x-amz-algorithm","` + operator.SigV4Algorithm + `"]`, `["eq","$x-amz-credential","` + s3Credential + `"]`, `["eq","$x-amz-date","` + fields["x-amz-date"] + `"]`,
	}
	fields["policy"] = base64.StdEncoding.EncodeToString([]byte(`{"expiration":"` + expires.UTC().Format("2006-01-02T15:04:05.000Z") + `","conditions":[` + strings.Join(conditions, ",") + `]}`))
	return signed.S3Policy{URL: origin + "/" + s3Bucket + "/", Fields: fields}
}

// s3Terms is a pool with storage at endpoint whose output limit is s3Limit.
func s3Terms(endpoint string) operator.Terms {
	terms := testTerms()
	terms.Storage = &operator.Storage{Endpoint: endpoint, Bucket: s3Bucket}
	terms.OutputLimit = s3Limit
	return terms
}

func s3Fixture(t *testing.T, s *store, stream []string, outputs []pluginsdk.Output) fixture {
	t.Helper()
	return newFixtureWith(t, stream, s3Terms(s.endpoint()), testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: stream, outputs: outputs}})
}

// withS3 declares an S3 input and report.pdf, which its size sends through the bucket.
func withS3(task *wire.Submission) {
	task.Wherry.S3Inputs = []wire.S3Input{{Name: "dataset.bin", Size: int64(len(s3Content)), SHA256: sha([]byte(s3Content))}}
	task.Wherry.Outputs = []wire.ExpectedOutput{{Name: "report.pdf"}}
}

// withURLs is an envelope edit that seals the URLs urls makes for the task's S3 inputs.
func withURLs(urls func(input string) string) func(*wire.Submission, *signed.EnvelopeValues) {
	return func(task *wire.Submission, values *signed.EnvelopeValues) {
		for _, input := range task.Wherry.S3Inputs {
			values.S3Inputs[input.Name] = urls(input.Name)
		}
	}
}

// withPolicy is an envelope edit that seals the policy edit leaves.
func withPolicy(edit func(policy *signed.S3Policy)) func(*wire.Submission, *signed.EnvelopeValues) {
	return func(_ *wire.Submission, values *signed.EnvelopeValues) { edit(values.S3Policy) }
}

// withoutPolicy is an envelope edit that seals no policy, as a host without a storage credential does.
func withoutPolicy(_ *wire.Submission, values *signed.EnvelopeValues) { values.S3Policy = nil }

func validInputURL(origin string) func(string) string {
	return func(input string) string {
		return presigned(origin, wire.S3InputKey(testTenant, testRun("run-a"), input))
	}
}

// reportKey is report.pdf's object key for the test run.
func reportKey() string { return wire.S3OutputKey(testTenant, testRun("run-a"), "report.pdf") }

func TestS3ObjectsTravelOnlyInTheEnvelope(t *testing.T) {
	s := newStore(t)
	f := s3Fixture(t, s, resultStream, []pluginsdk.Output{reportOutput(), file("notes.md", "notes")})
	record := f.submit(t, f.host, func(task *wire.Submission) {
		withS3(task)
		task.Wherry.Outputs = append(task.Wherry.Outputs, wire.ExpectedOutput{Name: "notes.md"})
	})
	done := f.executing(t, f.ctx, record, 1)
	if status := <-done; status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	run := f.lastRun()
	get := presigned(s.endpoint(), wire.S3InputKey(testTenant, testRun("run-a"), "dataset.bin"))
	if !slices.Equal(run.Fetches, []pluginsdk.Fetch{{Name: "dataset.bin", URL: get, Size: int64(len(s3Content)), SHA256: sha([]byte(s3Content))}}) {
		t.Fatalf("fetches %+v", run.Fetches)
	}
	if prompt := string(run.Prompt); !strings.Contains(prompt, "Input files: /task/inputs/dataset.bin.") || strings.Contains(prompt, "X-Amz") || strings.Contains(prompt, s.endpoint()) {
		t.Fatalf("prompt %q; want the input's path and no URL", prompt)
	}
	// Every record on the feed and the pool's topics is free of the URLs, the policy's signed values and the endpoint.
	policy := testPolicy(s.endpoint(), testTenant, testRun("run-a"), time.Now(), time.Now())
	for _, topic := range []string{operator.TenantTopic(testTenant), operator.TenantTranscriptTopic(testTenant), f.tasksTopic(), operator.PoolEventsTopic(testPool)} {
		for _, record := range f.records(t, topic) {
			for _, never := range []string{s3Signature, s3Credential, policy.Fields["policy"], "X-Amz", s.endpoint()} {
				if bytes.Contains(record.Value, []byte(never)) || bytes.Contains(decodedPayload(record.Value), []byte(never)) {
					t.Fatalf("a record on %s holds %q", topic, never)
				}
			}
		}
	}
	// The small output returns inline; the large one lands at its key under the run's prefix and is listed with its key.
	if data, ok := s.put("/" + s3Bucket + "/" + reportKey()); !ok || !bytes.Equal(data, report) {
		t.Fatalf("store holds %d bytes, %v for the output", len(data), ok)
	}
	listed := manifest(t, f.lastTask(t))
	if !slices.Equal(listed.S3Outputs, []wire.ReturnedOutput{{Name: "report.pdf", SHA256: sha(report), Size: int64(len(report)), Key: reportKey()}}) ||
		len(listed.Outputs) != 1 || listed.Outputs[0].Name != "notes.md" || len(listed.NotReturned) != 0 {
		t.Fatalf("result lists S3 outputs %+v, outputs %+v and not returned %+v", listed.S3Outputs, listed.Outputs, listed.NotReturned)
	}
	for _, output := range f.outputChunks(t) {
		if output.ArtifactUpdate.Artifact.ArtifactID != "notes.md" {
			t.Fatalf("an S3 output went on the transcript topic as chunk %s", output.ArtifactUpdate.Artifact.ArtifactID)
		}
	}
	// The second pass streams the large file alone, reading no other.
	f.plugin.mu.Lock()
	defer f.plugin.mu.Unlock()
	if f.plugin.collects != 2 || !slices.Equal(f.plugin.limits.Stream, []string{"report.pdf"}) || f.plugin.limits.FileBytes != 0 || f.plugin.limits.TotalBytes != 0 {
		t.Fatalf("%d collects, the last under %+v; want a second pass streaming report.pdf alone", f.plugin.collects, f.plugin.limits)
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

// Without a policy, from a pool without storage or a host without a storage credential for it, an output over the
// limit is not returned, and the plugin is asked for no second pass.
func TestAnOutputOverTheLimitWithoutAPolicyExceedsIt(t *testing.T) {
	for name, fixture := range map[string]func(t *testing.T) fixture{
		"pool without storage": func(t *testing.T) fixture {
			terms := testTerms()
			terms.OutputLimit = s3Limit
			return newFixtureWith(t, resultStream, terms, testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream, outputs: []pluginsdk.Output{reportOutput()}}})
		},
		"host without a storage credential": func(t *testing.T) fixture {
			f := s3Fixture(t, newStore(t), resultStream, []pluginsdk.Output{reportOutput()})
			f.envelope = withoutPolicy
			return f
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := fixture(t)
			status := f.execute(t, f.submit(t, f.host, func(task *wire.Submission) { task.Wherry.Outputs = []wire.ExpectedOutput{{Name: "report.pdf"}} }), 1)
			listed := manifest(t, f.lastTask(t))
			if status != kgo.AckReject || len(listed.S3Outputs) != 0 || !slices.Equal(listed.NotReturned, []wire.UnreturnedOutput{{Name: "report.pdf", Size: int64(len(report)), Reason: "exceeds output limit"}}) {
				t.Fatalf("status %v; S3 outputs %+v, not returned %+v; want the output over the limit", status, listed.S3Outputs, listed.NotReturned)
			}
			f.plugin.mu.Lock()
			defer f.plugin.mu.Unlock()
			if f.plugin.collects != 1 {
				t.Fatalf("%d collects, want the first pass alone", f.plugin.collects)
			}
		})
	}
}

func TestS3URLsAreCheckedAgainstPoolStorage(t *testing.T) {
	s := newStore(t)
	origin := s.endpoint()
	parsed, _ := url.Parse(origin)
	inputKey := wire.S3InputKey(testTenant, testRun("run-a"), "dataset.bin")
	for name, get := range map[string]string{
		"http":           strings.Replace(presigned(origin, inputKey), "https://", "http://", 1),
		"another host":   presigned("https://127.0.0.2:"+parsed.Port(), inputKey),
		"another port":   presigned("https://127.0.0.1:1", inputKey),
		"default port":   presigned("https://127.0.0.1", inputKey),
		"another run":    presigned(origin, wire.S3InputKey(testTenant, testRun("run-b"), "dataset.bin")),
		"another tenant": presigned(origin, wire.S3InputKey("tenant-b", testRun("run-a"), "dataset.bin")),
		"another bucket": strings.Replace(presigned(origin, inputKey), "/"+s3Bucket+"/", "/other-bucket/", 1),
		"userinfo":       strings.Replace(presigned(origin, inputKey), "https://", "https://user:pass@", 1),
		"dot segments":   presigned(origin, wire.S3InputKey(testTenant, testRun("run-a"), "x/../dataset.bin")),
	} {
		t.Run(name, func(t *testing.T) {
			f := s3Fixture(t, s, resultStream, nil)
			f.envelope = withURLs(func(string) string { return get })
			done := f.executing(t, f.ctx, f.submit(t, f.host, withS3), 1)
			if status := <-done; status != kgo.AckReject {
				t.Fatalf("status %v; want the turn failed", status)
			}
			if dispatches, _, _ := f.dispatched(t); dispatches != 0 {
				t.Fatal("dispatched with a URL off the pool's storage")
			}
			// The custodian refuses an envelope that does not check against its task, before any attempt wrap.
			status := f.lastTask(t).Status
			if status.State != wire.TaskStateFailed || status.Message.Parts[0].Text != wire.RefusedEnvelopeInvalid || !strings.Contains(f.logs.String(), "the URL for S3 input dataset.bin is not its object in the pool's storage") {
				t.Fatalf("status %+v; logs:\n%s", status, f.logs.String())
			}
			if strings.Contains(f.logs.String(), s3Signature) {
				t.Fatalf("logs hold the URL:\n%s", f.logs.String())
			}
		})
	}
	// A policy for another store, bucket or prefix fails the same way; the receiver's own check is CheckEnvelope's.
	for name, edit := range map[string]func(*signed.S3Policy){
		"another host": func(policy *signed.S3Policy) { policy.URL = "https://127.0.0.1:1/" + s3Bucket + "/" },
		"another bucket": func(policy *signed.S3Policy) {
			policy.URL = origin + "/other-bucket/"
		},
		"another run's prefix": func(policy *signed.S3Policy) {
			*policy = testPolicy(origin, testTenant, testRun("run-b"), time.Now(), time.Now().Add(time.Hour))
		},
	} {
		t.Run("policy "+name, func(t *testing.T) {
			f := s3Fixture(t, s, resultStream, nil)
			f.envelope = withPolicy(edit)
			done := f.executing(t, f.ctx, f.submit(t, f.host, withS3), 1)
			if status := <-done; status != kgo.AckReject || f.lastTask(t).Status.Message.Parts[0].Text != wire.RefusedEnvelopeInvalid ||
				!strings.Contains(f.logs.String(), "the S3 policy is not one for the run's outputs in the pool's storage") {
				t.Fatalf("status %v, %+v; logs:\n%s", status, f.lastTask(t).Status, f.logs.String())
			}
			if strings.Contains(f.logs.String(), s3Signature) || strings.Contains(f.logs.String(), s3Credential) {
				t.Fatalf("logs hold the policy:\n%s", f.logs.String())
			}
		})
	}
}

// A pool whose storage is plain http on 127.0.0.1 takes http URLs and a policy for it, and refuses https ones.
func TestS3OverLoopbackHTTP(t *testing.T) {
	s := startStore(t, httptest.NewServer)
	if !strings.HasPrefix(s.endpoint(), "http://127.0.0.1:") {
		t.Fatalf("endpoint %s", s.endpoint())
	}
	f := s3Fixture(t, s, resultStream, []pluginsdk.Output{reportOutput()})
	done := f.executing(t, f.ctx, f.submit(t, f.host, withS3), 1)
	if status := <-done; status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	if fetches := f.lastRun().Fetches; len(fetches) != 1 || !strings.HasPrefix(fetches[0].URL, s.endpoint()+"/") {
		t.Fatalf("fetches %+v", fetches)
	}
	if data, ok := s.put("/" + s3Bucket + "/" + reportKey()); !ok || !bytes.Equal(data, report) {
		t.Fatalf("store holds %d bytes, %v for the output", len(data), ok)
	}
	if strings.Contains(f.logs.String(), s3Signature) {
		t.Fatalf("logs hold a URL:\n%s", f.logs.String())
	}
	inputKey := wire.S3InputKey(testTenant, testRun("run-a"), "dataset.bin")
	for name, get := range map[string]string{
		"https":        presigned(strings.Replace(s.endpoint(), "http://", "https://", 1), inputKey),
		"default port": presigned("http://127.0.0.1", inputKey),
	} {
		t.Run(name, func(t *testing.T) {
			f := s3Fixture(t, s, resultStream, nil)
			f.envelope = withURLs(func(string) string { return get })
			done := f.executing(t, f.ctx, f.submit(t, f.host, withS3), 1)
			if status := <-done; status != kgo.AckReject || f.lastTask(t).Status.Message.Parts[0].Text != wire.RefusedEnvelopeInvalid ||
				!strings.Contains(f.logs.String(), "the URL for S3 input dataset.bin is not its object in the pool's storage") {
				t.Fatalf("status %v, %+v", status, f.lastTask(t).Status)
			}
			if strings.Contains(f.logs.String(), s3Signature) {
				t.Fatalf("logs hold the URL:\n%s", f.logs.String())
			}
		})
	}
	t.Run("https policy", func(t *testing.T) {
		f := s3Fixture(t, s, resultStream, nil)
		f.envelope = withPolicy(func(policy *signed.S3Policy) { policy.URL = strings.Replace(policy.URL, "http://", "https://", 1) })
		done := f.executing(t, f.ctx, f.submit(t, f.host, withS3), 1)
		if status := <-done; status != kgo.AckReject || f.lastTask(t).Status.Message.Parts[0].Text != wire.RefusedEnvelopeInvalid {
			t.Fatalf("status %v, %+v", status, f.lastTask(t).Status)
		}
	})
}

func TestS3InputWithoutPoolStorageIsRejected(t *testing.T) {
	f := newFixture(t, resultStream)
	edit := func(task *wire.Submission) {
		task.Wherry.S3Inputs = []wire.S3Input{{Name: "dataset.bin", SHA256: sha(nil)}}
	}
	if admitted, ack := f.receiver.admit(f.ctx, f.submit(t, f.host, edit), f.receiver.profiles[0]); admitted != nil || ack != kgo.AckReject {
		t.Fatalf("admitted %v with ack %v", admitted, ack)
	}
	if status := f.feedStatuses(t); len(status) != 1 || status[0].StatusUpdate.Status.Message.Parts[0].Text != "pool pool-a has no storage for S3 inputs" {
		t.Fatalf("statuses %+v", status)
	}
}

// The agent prints its input's URL whole and its signature alone; neither reaches the transcript or the result.
func TestS3URLsAreRedacted(t *testing.T) {
	s := newStore(t)
	get := presigned(s.endpoint(), wire.S3InputKey(testTenant, testRun("run-a"), "dataset.bin"))
	quoted := strings.ReplaceAll(get, "&", `&`)
	stream := []string{`{"type":"assistant","message":"fetched ` + get + `"}`, `{"type":"assistant","message":"signature ` + s3Signature + `"}`,
		completedWith("used " + quoted)}
	f := s3Fixture(t, s, stream, []pluginsdk.Output{reportOutput()})
	done := f.executing(t, f.ctx, f.submit(t, f.host, withS3), 1)
	if status := <-done; status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	// The URL takes its marker; the signature alone, which the fixture gives the policy too, takes one of theirs.
	input, policy := signed.RedactionMarker(testRedactionKey, signed.S3InputValue("dataset.bin")), signed.RedactionMarker(testRedactionKey, signed.S3PolicyValue)
	for i, chunk := range f.chunks(t) {
		if text := chunk.ArtifactUpdate.Artifact.Parts[0].Text; strings.Contains(text, s3Signature) || !strings.Contains(text, input) && (i == 0 || !strings.Contains(text, policy)) {
			t.Fatalf("transcript line %q, want a URL's or the policy's marker", text)
		}
	}
	if result := f.lastTask(t).Artifacts[0].Parts[0].Text; strings.Contains(result, "X-Amz-Signature") || strings.Contains(result, s3Signature) {
		t.Fatalf("result %q holds the URL", result)
	}
}

// The agent prints the policy's signing values, which never reach it in a run, and the receiver logs around an upload:
// none of the policy, its signature or its credential reaches the transcript, the result or the receiver's log.
func TestPolicyValuesAreRedacted(t *testing.T) {
	s := newStore(t)
	// The store refuses the upload with a body echoing the policy, as SignatureDoesNotMatch does.
	s.handle = func(w http.ResponseWriter, r *http.Request) {
		fields, _, _ := readPost(r)
		w.WriteHeader(http.StatusForbidden)
		io.WriteString(w, "<Error><StringToSign>"+fields["policy"]+"</StringToSign><Credential>"+fields["x-amz-credential"]+"</Credential></Error>")
	}
	policy := testPolicy(s.endpoint(), testTenant, testRun("run-a"), time.Now(), time.Now())
	values := []string{policy.Fields["policy"], s3Signature, s3Credential}
	stream := []string{`{"type":"assistant","message":"policy ` + values[0] + `"}`, `{"type":"assistant","message":"signature ` + values[1] + `"}`,
		`{"type":"assistant","message":"credential ` + values[2] + `"}`, completedWith("done")}
	f := s3Fixture(t, s, stream, []pluginsdk.Output{reportOutput()})
	// The fixture signs at its own time; the envelope carries the policy whose values the stream prints. No S3 input,
	// whose URL the fixture signs alike, so the signature is the policy's alone.
	f.envelope = withPolicy(func(sealed *signed.S3Policy) { *sealed = policy })
	done := f.executing(t, f.ctx, f.submit(t, f.host, func(task *wire.Submission) { task.Wherry.Outputs = []wire.ExpectedOutput{{Name: "report.pdf"}} }), 1)
	<-done
	marker := signed.RedactionMarker(testRedactionKey, signed.S3PolicyValue)
	chunks := f.chunks(t)
	if len(chunks) < 3 {
		t.Fatalf("%d transcript chunks", len(chunks))
	}
	for i, chunk := range chunks[:3] {
		text := chunk.ArtifactUpdate.Artifact.Parts[0].Text
		if !strings.Contains(text, marker) || slices.ContainsFunc(values, func(value string) bool { return strings.Contains(text, value) }) {
			t.Fatalf("transcript line %d %q, want the policy's marker and none of its values", i, text)
		}
	}
	for _, value := range values {
		for _, record := range f.records(t, operator.TenantTopic(testTenant)) {
			if bytes.Contains(record.Value, []byte(value)) || bytes.Contains(decodedPayload(record.Value), []byte(value)) {
				t.Fatalf("a feed record holds a policy value")
			}
		}
		if strings.Contains(f.logs.String(), value) {
			t.Fatalf("the receiver log holds a policy value:\n%s", f.logs.String())
		}
	}
	if listed := manifest(t, f.lastTask(t)); !slices.Equal(listed.NotReturned, []wire.UnreturnedOutput{{Name: "report.pdf", Size: int64(len(report)), Reason: "not uploaded: status 403"}}) {
		t.Fatalf("not returned %+v", listed.NotReturned)
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

// filePart returns the file part of a policy upload, positioned to read the file, after its fields.
func filePart(r *http.Request) (io.Reader, error) {
	reader, err := r.MultipartReader()
	if err != nil {
		return nil, err
	}
	for {
		part, err := reader.NextPart()
		if err != nil {
			return nil, err
		}
		if part.FormName() == "file" {
			return part, nil
		}
		io.Copy(io.Discard, part)
	}
}

func TestS3OutputIsStreamedAndHashedAsSent(t *testing.T) {
	s := newStore(t)
	half := 512 << 10
	content := append(bytes.Repeat([]byte("a"), half), bytes.Repeat([]byte("b"), half)...)
	// The output holds the delivered credential, which an S3 output keeps: it goes to the host's own bucket.
	copy(content, testToken)
	received := make(chan struct{})
	var stored []byte
	s.handle = func(w http.ResponseWriter, r *http.Request) {
		file, err := filePart(r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		first := make([]byte, half)
		if _, err := io.ReadFull(file, first); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		close(received)
		rest, err := io.ReadAll(file)
		if err != nil || r.ContentLength <= int64(len(content)) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		stored = append(first, rest...)
		w.WriteHeader(http.StatusNoContent)
	}
	body := &pausedBody{first: content[:half], rest: content[half:], received: received}
	f := s3Fixture(t, s, resultStream, []pluginsdk.Output{{Name: "report.pdf", Size: int64(len(content)), Body: body}})
	done := f.executing(t, f.ctx, f.submit(t, f.host, withS3), 1)
	if status := <-done; status != kgo.AckAccept {
		t.Fatalf("status %v; not returned %+v; logs:\n%s", status, manifest(t, f.lastTask(t)).NotReturned, f.logs.String())
	}
	sum := sha256.Sum256(stored)
	listed := manifest(t, f.lastTask(t)).S3Outputs
	if !bytes.Equal(stored, content) || len(listed) != 1 || listed[0].SHA256 != hex.EncodeToString(sum[:]) || listed[0].Size != int64(len(stored)) {
		t.Fatalf("stored %d bytes; result lists %+v; want the bytes sent, unredacted, and their hash", len(stored), listed)
	}
}

// The policy's key field is the run's prefix, which a form posted as carried lands an object at; the receiver's upload
// replaces it with the output's key, and nothing lands at the bare prefix.
func TestTheUploadReplacesThePolicysKey(t *testing.T) {
	s := newStore(t)
	policy := testPolicy(s.endpoint(), testTenant, testRun("run-a"), time.Now(), time.Now().Add(time.Hour))
	prefix := wire.S3OutputKey(testTenant, testRun("run-a"), "")
	form, err := newPostForm(policy.Fields, policy.Fields["key"], strings.NewReader("carried"), 7)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest(http.MethodPost, policy.URL, form.body)
	request.ContentLength = form.length
	request.Header.Set("Content-Type", form.contentType)
	response, err := storageClient.Do(request)
	if err != nil || response.StatusCode != http.StatusNoContent {
		t.Fatalf("post of the carried fields: %v, %v", response, err)
	}
	response.Body.Close()
	if data, ok := s.put("/" + s3Bucket + "/" + prefix); !ok || string(data) != "carried" {
		t.Fatalf("the carried key field landed %q, %v; want an object at the bare prefix", data, ok)
	}
	f := s3Fixture(t, s, resultStream, []pluginsdk.Output{reportOutput()})
	if status := <-f.executing(t, f.ctx, f.submit(t, f.host, withS3), 1); status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	if data, ok := s.put("/" + s3Bucket + "/" + reportKey()); !ok || !bytes.Equal(data, report) {
		t.Fatalf("store holds %d bytes, %v at the output's key", len(data), ok)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !slices.Equal(s.posted, []string{prefix, prefix + "report.pdf"}) {
		t.Fatalf("posted keys %q; want the receiver's upload at the output's key alone", s.posted)
	}
}

// A second pass that fails leaves the first pass's inline outputs returned and lists its own files not uploaded; a run
// whose outputs all fit needs no second pass.
func TestASecondPassFailureKeepsTheFirstPassOutputs(t *testing.T) {
	for name, failure := range map[string]struct {
		err    error
		reason string
	}{
		"error":    {errors.New("the pod is being deleted"), "not uploaded: collection failed"},
		"deadline": {context.DeadlineExceeded, "not uploaded: timeout"},
		"canceled": {context.Canceled, "not uploaded: canceled"},
	} {
		t.Run(name, func(t *testing.T) {
			s := newStore(t)
			f := s3Fixture(t, s, resultStream, []pluginsdk.Output{file("notes.md", "notes"), reportOutput(), {Name: "big.bin", Size: 3 * s3Limit, Data: bytes.Repeat([]byte("b"), 3*s3Limit)}})
			f.plugin.failCollect = map[int]error{2: failure.err}
			status := f.execute(t, f.submit(t, f.host, func(task *wire.Submission) {
				withS3(task)
				task.Wherry.Outputs = append(task.Wherry.Outputs, wire.ExpectedOutput{Name: "notes.md"}, wire.ExpectedOutput{Name: "big.bin"})
			}), 1)
			task := f.lastTask(t)
			listed := manifest(t, task)
			if status != kgo.AckReject || task.Status.State != wire.TaskStateFailed || !strings.Contains(task.Status.Message.Parts[0].Text, "required output big.bin not returned: "+failure.reason) {
				t.Fatalf("ended %v %s %+v; want FAILED naming the file not uploaded", status, task.Status.State, task.Status.Message)
			}
			if len(listed.Outputs) != 1 || listed.Outputs[0].Name != "notes.md" || listed.Outputs[0].SHA256 != sha([]byte("notes")) {
				t.Fatalf("outputs %+v; want the inline output of the first pass", listed.Outputs)
			}
			want := []wire.UnreturnedOutput{{Name: "big.bin", Size: 3 * s3Limit, Reason: failure.reason}, {Name: "report.pdf", Size: int64(len(report)), Reason: failure.reason}}
			if len(listed.S3Outputs) != 0 || !slices.Equal(listed.NotReturned, want) {
				t.Fatalf("S3 outputs %+v, not returned %+v; want the second pass's files alone not uploaded", listed.S3Outputs, listed.NotReturned)
			}
			if _, ok := s.put("/" + s3Bucket + "/" + reportKey()); ok {
				t.Fatal("the store holds an output of a failed pass")
			}
		})
	}
}

// A file the first pass found over the limit and the second pass does not list is missing.
func TestAFileAbsentFromTheSecondPassIsMissing(t *testing.T) {
	s := newStore(t)
	f := s3Fixture(t, s, resultStream, []pluginsdk.Output{reportOutput()})
	f.plugin.collecting = func() {
		f.plugin.mu.Lock()
		defer f.plugin.mu.Unlock()
		if f.plugin.collects == 1 {
			f.plugin.outputs = nil
		}
	}
	f.execute(t, f.submit(t, f.host, withS3), 1)
	if listed := manifest(t, f.lastTask(t)); len(listed.S3Outputs) != 0 || !slices.Equal(listed.NotReturned, []wire.UnreturnedOutput{{Name: "report.pdf", Reason: "missing"}}) {
		t.Fatalf("S3 outputs %+v, not returned %+v; want report.pdf missing", listed.S3Outputs, listed.NotReturned)
	}
}

// The first pass runs under collectTimeout alone, with a policy as without one: a hung first pass holds the claim no
// longer than that, not until the policy expires.
func TestAHungFirstPassEndsAtCollectTimeout(t *testing.T) {
	s := newStore(t)
	f := s3Fixture(t, s, resultStream, []pluginsdk.Output{reportOutput()})
	collecting := make(chan struct{})
	var once sync.Once
	f.plugin.collectHangs = true
	f.plugin.collecting = func() { once.Do(func() { close(collecting) }) }
	// The fixture's context carries the test's own deadline; the receiver's does not bound a collection.
	ctx, cancel := context.WithCancel(context.WithoutCancel(f.ctx))
	defer cancel()
	started := time.Now()
	done := f.executing(t, ctx, f.submit(t, f.host, withS3), 1)
	select {
	case <-collecting:
	case <-time.After(30 * time.Second):
		t.Fatal("the plugin's Collect never began")
	}
	f.plugin.mu.Lock()
	deadline := f.plugin.collectDeadline
	f.plugin.mu.Unlock()
	if want := started.Add(collectTimeout); deadline.Before(want) || deadline.After(want.Add(30*time.Second)) {
		t.Fatalf("first pass deadline %v, want collectTimeout from %v", deadline, started)
	}
	cancel()
	<-done
}

// A stream shorter or longer than the size the second pass listed is not uploaded, and no object is created.
func TestAStreamOfAnotherSizeIsNotUploaded(t *testing.T) {
	for name, body := range map[string][]byte{"shorter": report[:len(report)-1], "longer": append(slices.Clone(report), 'r')} {
		t.Run(name, func(t *testing.T) {
			s := newStore(t)
			f := s3Fixture(t, s, resultStream, []pluginsdk.Output{{Name: "report.pdf", Size: int64(len(report)), Body: io.NopCloser(bytes.NewReader(body))}})
			f.execute(t, f.submit(t, f.host, withS3), 1)
			if listed := manifest(t, f.lastTask(t)); len(listed.S3Outputs) != 0 || !slices.Equal(listed.NotReturned, []wire.UnreturnedOutput{{Name: "report.pdf", Size: int64(len(report)), Reason: "not uploaded: size changed while read"}}) {
				t.Fatalf("S3 outputs %+v, not returned %+v", listed.S3Outputs, listed.NotReturned)
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			if len(s.puts) != 0 || len(s.posted) != 0 {
				t.Fatalf("the store holds %d objects after %d uploads", len(s.puts), len(s.posted))
			}
		})
	}
}

// A 302 would turn the POST into a GET elsewhere, which the client refuses; a 307 would repeat the POST, which a streamed
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
			f := s3Fixture(t, s, resultStream, []pluginsdk.Output{reportOutput()})
			done := f.executing(t, f.ctx, f.submit(t, f.host, withS3), 1)
			if status := <-done; status != kgo.AckReject {
				t.Fatalf("status %v; want the required output's turn failed", status)
			}
			if hits.Load() != 0 {
				t.Fatal("the upload followed the redirect")
			}
			listed := manifest(t, f.lastTask(t))
			if len(listed.S3Outputs) != 0 || !slices.Equal(listed.NotReturned, []wire.UnreturnedOutput{{Name: "report.pdf", Size: int64(len(report)), Reason: reason}}) {
				t.Fatalf("result lists %+v and %+v", listed.S3Outputs, listed.NotReturned)
			}
			if status := f.lastTask(t).Status; status.State != wire.TaskStateFailed || !strings.Contains(status.Message.Parts[0].Text, "required output report.pdf not returned") {
				t.Fatalf("status %+v", status)
			}
		})
	}
}

// A store that drops the connection ends the output with the failure's class, never the request's URL.
func TestS3OutputUploadNamesTheTransportFailure(t *testing.T) {
	s := newStore(t)
	s.handle = func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		conn.Close()
	}
	f := s3Fixture(t, s, resultStream, []pluginsdk.Output{reportOutput()})
	done := f.executing(t, f.ctx, f.submit(t, f.host, withS3), 1)
	<-done
	if listed := manifest(t, f.lastTask(t)); !slices.Equal(listed.NotReturned, []wire.UnreturnedOutput{{Name: "report.pdf", Size: int64(len(report)), Reason: "not uploaded: request failed (connection closed)"}}) {
		t.Fatalf("not returned %+v", listed.NotReturned)
	}
	if !strings.Contains(f.logs.String(), "request failed (connection closed)") || strings.Contains(f.logs.String(), s3Signature) {
		t.Fatalf("logs:\n%s", f.logs.String())
	}
}

func TestUploadFailureClasses(t *testing.T) {
	wrap := func(err error) error {
		return &url.Error{Op: "Post", URL: "https://store.example/b/", Err: err}
	}
	caused, cancel := context.WithCancelCause(context.Background())
	cancel(errors.New("receiver stopping"))
	for _, c := range []struct {
		want string
		ctx  context.Context
		err  error
	}{
		{"canceled", context.Background(), wrap(context.Canceled)},
		{"canceled", caused, wrap(errors.New("receiver stopping"))},
		{"timeout", context.Background(), wrap(context.DeadlineExceeded)},
		{"dns", context.Background(), wrap(&net.OpError{Op: "dial", Err: &net.DNSError{Err: "no such host", Name: "store.example"}})},
		{"tls", context.Background(), wrap(&tls.CertificateVerificationError{Err: x509.UnknownAuthorityError{}})},
		{"tls", context.Background(), wrap(&net.OpError{Op: "remote error", Err: errors.New("tls: certificate required")})},
		{"connection refused", context.Background(), wrap(&net.OpError{Op: "dial", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)})},
		{"connection closed", context.Background(), wrap(&net.OpError{Op: "write", Err: os.NewSyscallError("write", syscall.EPIPE)})},
		{"connection closed", context.Background(), wrap(&net.OpError{Op: "write", Err: net.ErrClosed})},
		{"connection closed", context.Background(), wrap(io.ErrUnexpectedEOF)},
		{"other", context.Background(), wrap(errors.New("something else"))},
	} {
		if got := uploadFailure(c.ctx, c.err); got != c.want {
			t.Errorf("uploadFailure(%v) = %q, want %q", c.err, got, c.want)
		}
	}
}

// The store's refusal echoes the policy; the result and the log carry the status alone.
func TestS3OutputRefusedByTheStoreIsNotReturned(t *testing.T) {
	s := newStore(t)
	s.handle = func(w http.ResponseWriter, r *http.Request) {
		fields, _, _ := readPost(r)
		w.WriteHeader(http.StatusForbidden)
		io.WriteString(w, "<Error>"+fields["policy"]+fields["x-amz-signature"]+"</Error>")
	}
	f := s3Fixture(t, s, resultStream, []pluginsdk.Output{reportOutput()})
	done := f.executing(t, f.ctx, f.submit(t, f.host, withS3), 1)
	<-done
	if listed := manifest(t, f.lastTask(t)); !slices.Equal(listed.NotReturned, []wire.UnreturnedOutput{{Name: "report.pdf", Size: int64(len(report)), Reason: "not uploaded: status 403"}}) {
		t.Fatalf("not returned %+v", listed.NotReturned)
	}
	policy := testPolicy(s.endpoint(), testTenant, testRun("run-a"), time.Now(), time.Now())
	if strings.Contains(f.logs.String(), s3Signature) || strings.Contains(f.logs.String(), policy.Fields["policy"]) {
		t.Fatalf("logs hold the store's answer:\n%s", f.logs.String())
	}
}

func TestS3FetchFailureFailsTheTurn(t *testing.T) {
	s := newStore(t)
	f := newFixtureWith(t, noResultStream, s3Terms(s.endpoint()), testConfig(testReceiver), map[string]*fakePlugin{
		"nomad": {stream: noResultStream, neverStarted: true, neverClass: pluginsdk.ClassFetch},
	})
	done := f.executing(t, f.ctx, f.submit(t, f.host, withS3), 1)
	if status := <-done; status != kgo.AckReject {
		t.Fatalf("status %v; want the turn failed and the task not returned to the pool", status)
	}
	if status := f.lastTask(t).Status; status.State != wire.TaskStateFailed || status.Message.Parts[0].Text != "an S3 input was not fetched, or did not match its size and SHA-256" {
		t.Fatalf("status %+v", status)
	}
}

func TestS3FetchFailureInDispatchFailsTheTurn(t *testing.T) {
	s := newStore(t)
	f := newFixtureWith(t, noResultStream, s3Terms(s.endpoint()), testConfig(testReceiver), map[string]*fakePlugin{
		"nomad": {stream: noResultStream, dispatchErr: &pluginsdk.Error{Op: "fetch dataset.bin", Class: pluginsdk.ClassFetch, Err: errors.New("403")}, outcome: pluginsdk.OutcomeNothingCreated},
	})
	done := f.executing(t, f.ctx, f.submit(t, f.host, withS3), 1)
	if status := <-done; status != kgo.AckReject || f.plugin.dispatches != 1 {
		t.Fatalf("status %v after %d dispatches; want the turn failed at its one dispatch and the task not returned to the pool", status, f.plugin.dispatches)
	}
	if status := f.lastTask(t).Status; status.State != wire.TaskStateFailed || status.Message.Parts[0].Text != "an S3 input was not fetched, or did not match its size and SHA-256" {
		t.Fatalf("status %+v", status)
	}
}

// An adopter takes the policy for the turn's S3 outputs from its envelope; the job fetched its inputs.
func TestAnAdoptedRunReturnsAnS3Output(t *testing.T) {
	s := newStore(t)
	f := s3Fixture(t, s, resultStream, []pluginsdk.Output{reportOutput()})
	f.plugin.job = pluginsdk.JobRunning
	done := f.executing(t, f.ctx, f.submit(t, f.host, withS3), 2)
	if status := <-done; status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	if data, ok := s.put("/" + s3Bucket + "/" + reportKey()); !ok || !bytes.Equal(data, report) {
		t.Fatalf("store holds %d bytes, %v for the adopted run's output", len(data), ok)
	}
	if listed := manifest(t, f.lastTask(t)); !slices.Equal(listed.S3Outputs, []wire.ReturnedOutput{{Name: "report.pdf", SHA256: sha(report), Size: int64(len(report)), Key: reportKey()}}) {
		t.Fatalf("result lists S3 outputs %+v, not returned %+v", listed.S3Outputs, listed.NotReturned)
	}
}

func TestAnAttemptWithoutAPolicyStoresNothing(t *testing.T) {
	run := &activeRun{receiver: &Receiver{}, task: testSubmission("run-a")}
	if stored := run.store(t.Context(), pluginsdk.Output{Name: "report.pdf", Size: 6, Body: io.NopCloser(strings.NewReader("report"))}, nil); stored.reason != "not uploaded: no upload policy" {
		t.Fatalf("stored %+v", stored)
	}
}
