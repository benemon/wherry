package view

import (
	"bytes"
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/telemetry"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Sentinels the stored records carry and no response may. A working status's line and a rejection's reason are shown
// by design, so they are not among them.
var sentinels = []string{"s3cr3t-reply-text", "s3cr3t-artifact-text", "s3cr3t-span-attr", "s3cr3t-span-name", "s3cr3t-host-record",
	"s3cr3t-label-key", "s3cr3t-label-value", "s3cr3t-profile", "s3cr3t-agent", "s3cr3t-unreturned-reason", "s3cr3t-offer"}

// served is a view with a recorded workload holding every sentinel, served, with the token parsed from the URL.
type served struct {
	f      *fixture
	v      *View
	server *Server
	token  string
	run    string
	logs   *bytes.Buffer
}

func newServed(t *testing.T, opts ServeOptions) *served {
	t.Helper()
	f := newFixture(t)
	t0 := f.now.Add(-10 * time.Minute)
	run := f.echo(t0, receiverA, tenantA, hostA, poolA, "run")
	f.status(t0.Add(time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "claimed", state: wire.TaskStateWorking})
	f.status(t0.Add(2*time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "working", state: wire.TaskStateWorking, text: "s3cr3t-working-line"})
	f.status(t0.Add(3*time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "terminal", state: wire.TaskStateCompleted})
	// Nested metadata sentinels: a label, a profile the configuration does not name, an unreturned reason.
	f.result(t0.Add(3*time.Second), receiverA, tenantA, poolA, run, 1, wire.TaskStateCompleted, wire.ResultMetadata{Profile: "s3cr3t-profile", Labels: map[string]string{"s3cr3t-label-key": "s3cr3t-label-value"},
		Outputs: []wire.ReturnedOutput{{Name: "notes.txt", SHA256: hashOf("x"), Size: 3}}, NotReturned: []wire.UnreturnedOutput{{Name: "gone.txt", Reason: "s3cr3t-unreturned-reason"}}}, "s3cr3t-reply-text s3cr3t-artifact-text")
	rejected := f.echo(t0.Add(4*time.Second), receiverA, tenantA, hostA, poolA, "rejected")
	f.status(t0.Add(5*time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: rejected, kind: "rejected", state: wire.TaskStateRejected, text: "s3cr3t-rejection"})
	unstarted := f.echo(t0.Add(5*time.Second), receiverB, tenantA, hostA, poolA, "unstarted")
	f.startFailureWith(t0.Add(6*time.Second), receiverB, tenantA, poolA, unstarted, 1, "dispatch", "s3cr3t-profile", "s3cr3t-agent", hashOf(unstarted))
	ad := &wire.Advertisement{Receiver: receiverB, Pool: poolA, Offers: []wire.Offer{{Profile: "s3cr3t-offer", Agent: "s3cr3t-agent", Replicas: 1}, {Profile: "profile-a", Agent: "s3cr3t-agent", Replicas: 1}}, Capacity: 2, AtMS: f.now.UnixMilli()}
	f.produce(operator.PoolReceiversTopic(poolA), receiverB, f.sign(ad, f.keys[receiverB]), f.now)
	raiseTraceLimit(t, f)
	tid := id(16)
	f.traceRecord(t0.Add(6*time.Second), "wherry-host."+tenantA+"."+hostA,
		span(tid, id(8), nil, "s3cr3t-span-name", run, t0, str(string(telemetry.AttrOutput), "s3cr3t-span-attr"), str("s3cr3t-host-record", "s3cr3t-host-record")))
	s := serveFixture(t, f, opts)
	s.run = run
	return s
}

// serveFixture rebuilds a view over f's cluster and serves it with opts, its log captured.
func serveFixture(t *testing.T, f *fixture, opts ServeOptions) *served {
	t.Helper()
	logs := &bytes.Buffer{}
	opts.Logger = log.New(logs, "", 0)
	v := f.view(time.Hour, testCaps())
	server, err := Serve(v, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Close(ctx)
	})
	parsed, err := url.Parse(server.URL())
	if err != nil {
		t.Fatal(err)
	}
	token, ok := strings.CutPrefix(parsed.Fragment, "t=")
	if !ok || parsed.RawQuery != "" || strings.Contains(parsed.Path, token) {
		t.Fatalf("the token is not in the fragment alone: %s", server.URL())
	}
	return &served{f: f, v: v, server: server, token: token, logs: logs}
}

// request sends method path to the server with headers and the cookie, reading the whole body.
func (s *served) request(t *testing.T, method, path string, body io.Reader, cookie *http.Cookie, headers map[string]string) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequest(method, "http://"+s.server.Addr()+path, body)
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range headers {
		if key == "Host" {
			req.Host = value
			continue
		}
		req.Header.Set(key, value)
	}
	if cookie != nil {
		req.AddCookie(cookie)
	}
	client := &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	read, _ := io.ReadAll(response.Body)
	return response, string(read)
}

// session posts the token and returns the cookie issued.
func (s *served) session(t *testing.T) *http.Cookie {
	t.Helper()
	form := url.Values{"token": {s.token}}
	response, body := s.request(t, http.MethodPost, "/session", strings.NewReader(form.Encode()), nil, map[string]string{"Content-Type": "application/x-www-form-urlencoded"})
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("POST /session: %d %s", response.StatusCode, body)
	}
	for _, cookie := range response.Cookies() {
		if cookie.Name == "wherry-view-"+strconv.Itoa(s.server.Port()) {
			return cookie
		}
	}
	t.Fatal("no session cookie")
	return nil
}

// Test 9: the API and serving.
func TestServing(t *testing.T) {
	s := newServed(t, ServeOptions{})
	addr, _ := net.ResolveTCPAddr("tcp", s.server.Addr())
	if !addr.IP.Equal(net.IPv4(127, 0, 0, 1)) {
		t.Fatalf("listening on %s, want loopback", s.server.Addr())
	}
	port := strconv.Itoa(s.server.Port())
	// The page and POST /session work without a cookie; the API and the stream do not.
	if response, body := s.request(t, http.MethodGet, "/", nil, nil, nil); response.StatusCode != http.StatusOK || !strings.Contains(body, `src="/assets/`) || !strings.HasPrefix(response.Header.Get("Content-Security-Policy"), "default-src 'none'; script-src 'self'; style-src 'self' 'nonce-") {
		t.Errorf("page: %d, CSP %q", response.StatusCode, response.Header.Get("Content-Security-Policy"))
	}
	for _, path := range []string{"/api/model", "/api/stream", "/api/runs/" + s.run, "/api/runs/" + s.run + "/trace"} {
		if response, _ := s.request(t, http.MethodGet, path, nil, nil, nil); response.StatusCode != http.StatusForbidden {
			t.Errorf("%s without a cookie: %d", path, response.StatusCode)
		}
	}
	// A wrong token gets 403; the right one a cookie with the required attributes and a value that is not the token.
	if response, _ := s.request(t, http.MethodPost, "/session", strings.NewReader("token="+strings.Repeat("0", 64)), nil, map[string]string{"Content-Type": "application/x-www-form-urlencoded"}); response.StatusCode != http.StatusForbidden || len(response.Cookies()) != 0 {
		t.Errorf("wrong token: %d with %d cookies", response.StatusCode, len(response.Cookies()))
	}
	cookie := s.session(t)
	if cookie.Name != "wherry-view-"+port || !cookie.HttpOnly || cookie.SameSite != http.SameSiteStrictMode || cookie.Path != "/" || cookie.Domain != "" || cookie.Value == s.token || len(cookie.Value) != 64 || cookie.Secure {
		t.Errorf("cookie: %+v", cookie)
	}
	// With a valid session, a foreign Host and a foreign Origin get 403; the bound host and origins pass.
	if response, _ := s.request(t, http.MethodGet, "/api/model", nil, cookie, map[string]string{"Host": "evil.example:" + port}); response.StatusCode != http.StatusForbidden {
		t.Errorf("foreign Host: %d", response.StatusCode)
	}
	if response, _ := s.request(t, http.MethodGet, "/api/model", nil, cookie, map[string]string{"Origin": "http://evil.example:" + port}); response.StatusCode != http.StatusForbidden {
		t.Errorf("foreign Origin: %d", response.StatusCode)
	}
	if response, _ := s.request(t, http.MethodGet, "/api/model", nil, cookie, map[string]string{"Origin": "http://127.0.0.1:" + port + "/"}); response.StatusCode != http.StatusForbidden {
		t.Errorf("an Origin with a path is not exact: %d", response.StatusCode)
	}
	if response, _ := s.request(t, http.MethodGet, "/api/model", nil, cookie, map[string]string{"X-Forwarded-Host": "127.0.0.1:" + port, "Host": "evil.example:" + port}); response.StatusCode != http.StatusForbidden {
		t.Errorf("forwarded headers are read: %d", response.StatusCode)
	}
	for _, headers := range []map[string]string{{}, {"Origin": "http://127.0.0.1:" + port}, {"Host": "localhost:" + port, "Origin": "http://localhost:" + port}} {
		if response, _ := s.request(t, http.MethodGet, "/api/model", nil, cookie, headers); response.StatusCode != http.StatusOK {
			t.Errorf("own host and origin %v: %d", headers, response.StatusCode)
		}
	}
	// Every response and error against the sentinels, the headers on each.
	bodies := map[string]string{}
	for _, path := range []string{"/api/model", "/api/runs/" + s.run, "/api/runs/" + s.run + "/trace", "/api/runs/run-missing", "/api/runs/" + s.run + "/trace/x", "/nowhere"} {
		response, body := s.request(t, http.MethodGet, path, nil, cookie, nil)
		bodies[path] = body
		for header, want := range map[string]string{"Cache-Control": "no-store", "Referrer-Policy": "no-referrer", "Content-Security-Policy": csp, "X-Content-Type-Options": "nosniff"} {
			if got := response.Header.Get(header); got != want {
				t.Errorf("%s: %s is %q, want %q", path, header, got, want)
			}
		}
		if response.Header.Get("Access-Control-Allow-Origin") != "" {
			t.Errorf("%s: CORS header present", path)
		}
		for _, sentinel := range sentinels {
			if strings.Contains(body, sentinel) {
				t.Errorf("%s carries %q", path, sentinel)
			}
		}
	}
	if !strings.Contains(bodies["/api/model"], `"runs_total":3`) || !strings.Contains(bodies["/api/model"], `"offers":[{"profile":"profile-a","agent":"unknown","replicas":1,"in_flight":0}]`) || !strings.Contains(bodies["/api/runs/"+s.run], `"path"`) || !strings.Contains(bodies["/api/runs/"+s.run+"/trace"], `"[other]"`) {
		t.Errorf("responses: model %.200s; run %.200s; trace %.300s", bodies["/api/model"], bodies["/api/runs/"+s.run], bodies["/api/runs/"+s.run+"/trace"])
	}
	// The stream serves an event at once with the cookie, and the token never appears in a request line.
	if event := s.firstEvent(t, "/api/stream?t="+s.token, cookie); !strings.HasPrefix(event, "event: model\ndata: {\"version\":") {
		t.Errorf("stream: %q", event)
	}
	if strings.Contains(s.logs.String(), s.token) {
		t.Error("the token reached the log")
	}
	for _, line := range strings.Split(s.logs.String(), "\n") {
		if strings.Contains(line, "?") {
			t.Errorf("a query reached the log: %s", line)
		}
	}
	// A session over the rate is refused.
	refused := false
	for range 30 {
		if response, _ := s.request(t, http.MethodPost, "/session", strings.NewReader("token="+s.token), nil, map[string]string{"Content-Type": "application/x-www-form-urlencoded"}); response.StatusCode == http.StatusTooManyRequests {
			refused = true
			break
		}
	}
	if !refused {
		t.Error("POST /session is not rate-limited")
	}
}

// Each page response carries a fresh nonce, the same in its one Content-Security-Policy header and its meta tag, and
// only there: script-src has neither a nonce nor 'unsafe-inline', and the API and asset responses carry no nonce.
func TestPageNonce(t *testing.T) {
	s := newServed(t, ServeOptions{})
	headerNonce := regexp.MustCompile(`'nonce-([A-Za-z0-9+/=]+)'`)
	metaNonce := regexp.MustCompile(`<meta name="csp-nonce" content="([^"]*)">`)
	seen := map[string]bool{}
	for range 3 {
		response, body := s.request(t, http.MethodGet, "/", nil, nil, nil)
		policies := response.Header.Values("Content-Security-Policy")
		if len(policies) != 1 {
			t.Fatalf("page: %d CSP headers", len(policies))
		}
		policy := policies[0]
		inHeader, inMeta := headerNonce.FindAllStringSubmatch(policy, -1), metaNonce.FindStringSubmatch(body)
		if len(inHeader) != 1 || inMeta == nil || inHeader[0][1] != inMeta[1] || len(inMeta[1]) != 24 {
			t.Fatalf("page: header %q, meta %v", policy, inMeta)
		}
		if seen[inMeta[1]] {
			t.Errorf("page: the nonce %s repeated", inMeta[1])
		}
		seen[inMeta[1]] = true
		if want := "default-src 'none'; script-src 'self'; style-src 'self' 'nonce-" + inMeta[1] + "'; font-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'"; policy != want {
			t.Errorf("page: CSP %q, want %q", policy, want)
		}
		if strings.Contains(body, noncePlaceholder) {
			t.Error("page: the placeholder is still in the body")
		}
	}
	cookie := s.session(t)
	for _, path := range []string{"/api/model", "/api/runs/" + s.run, "/assets/" + s.asset(t), "/nowhere", "/api/stream"} {
		response, _ := s.request(t, http.MethodGet, path, nil, cookie, nil)
		if policies := response.Header.Values("Content-Security-Policy"); len(policies) != 1 || policies[0] != csp || strings.Contains(policies[0], "nonce") || strings.Contains(policies[0], "unsafe-inline") {
			t.Errorf("%s: CSP %v", path, policies)
		}
	}
}

// asset is the name of one script the page references.
func (s *served) asset(t *testing.T) string {
	t.Helper()
	_, body := s.request(t, http.MethodGet, "/", nil, nil, nil)
	match := regexp.MustCompile(`src="/assets/([^"]+\.js)"`).FindStringSubmatch(body)
	if match == nil {
		t.Fatalf("the page names no script: %.300s", body)
	}
	return match[1]
}

// /assets serves the build's files by exact name, cacheable for good; a directory, a name outside the assets
// directory, a traversal and an encoded slash get the fixed 404, and the page's other files are not served elsewhere.
func TestAssets(t *testing.T) {
	s := newServed(t, ServeOptions{})
	name := s.asset(t)
	response, body := s.request(t, http.MethodGet, "/assets/"+name, nil, nil, nil)
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/javascript; charset=utf-8" || response.Header.Get("Cache-Control") != "public, max-age=31536000, immutable" || !strings.Contains(body, "csp-nonce") {
		t.Errorf("asset: %d %q %q", response.StatusCode, response.Header.Get("Content-Type"), response.Header.Get("Cache-Control"))
	}
	for _, path := range []string{"/assets/", "/assets", "/assets/missing.js", "/assets/index.html", "/assets/../index.html", "/assets/..%2findex.html", "/assets/%2e%2e/index.html", "/assets/" + name + "/", "/assets/sub/" + name, "/index.html", "/static/index.html", "/THIRD_PARTY_NOTICES", "/assets/THIRD_PARTY_NOTICES"} {
		response, body := s.request(t, http.MethodGet, path, nil, nil, nil)
		if response.StatusCode != http.StatusNotFound || body != textNotFound+"\n" || response.Header.Get("Cache-Control") != "no-store" {
			t.Errorf("%s: %d %q %q", path, response.StatusCode, body, response.Header.Get("Cache-Control"))
		}
	}
	if response, _ := s.request(t, http.MethodGet, "/", nil, nil, nil); response.Header.Get("Cache-Control") != "no-store" {
		t.Errorf("page: Cache-Control %q", response.Header.Get("Cache-Control"))
	}
}

// The --external-url authority is accepted as Host and Origin, the cookie is Secure for https, and a URL with a path
// is refused.
func TestServingBehindAProxy(t *testing.T) {
	s := newServed(t, ServeOptions{ExternalURL: "https://view.example.com"})
	if !strings.HasPrefix(s.server.URL(), "https://view.example.com/#t=") {
		t.Errorf("URL %s", s.server.URL())
	}
	cookie := s.session(t)
	if !cookie.Secure {
		t.Error("cookie is not Secure behind an https proxy")
	}
	if response, _ := s.request(t, http.MethodGet, "/api/model", nil, cookie, map[string]string{"Host": "view.example.com", "Origin": "https://view.example.com"}); response.StatusCode != http.StatusOK {
		t.Errorf("external host and origin: %d", response.StatusCode)
	}
	if response, _ := s.request(t, http.MethodGet, "/api/model", nil, cookie, map[string]string{"Host": "view.example.com", "Origin": "http://view.example.com"}); response.StatusCode != http.StatusForbidden {
		t.Errorf("external origin with another scheme: %d", response.StatusCode)
	}
	if _, err := Serve(s.v, ServeOptions{ExternalURL: "https://view.example.com/wherry"}); err == nil {
		t.Error("an external URL with a path was accepted")
	}
}

// A stream client that stops reading is dropped within the write deadline: its subscription ends.
func TestSlowStreamClientIsDropped(t *testing.T) {
	s := newServed(t, ServeOptions{WriteDeadline: 200 * time.Millisecond, KeepAlive: 50 * time.Millisecond})
	cookie := s.session(t)
	// A small receive buffer, so the server's writes block early.
	dialer := net.Dialer{Control: func(network, address string, c syscall.RawConn) error {
		return c.Control(func(fd uintptr) { syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 2048) })
	}}
	conn, err := dialer.Dial("tcp", s.server.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, "GET /api/stream HTTP/1.1\r\nHost: "+s.server.Addr()+"\r\nCookie: "+cookie.Name+"="+cookie.Value+"\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	subscribed := func() int {
		s.v.mu.RLock()
		defer s.v.mu.RUnlock()
		return len(s.v.subscribers)
	}
	deadline := time.Now().Add(10 * time.Second)
	for subscribed() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if subscribed() == 0 {
		t.Fatal("the stream never subscribed")
	}
	// Versions flow while the client reads nothing, as fast as the stream writes them.
	for time.Now().Before(deadline) && subscribed() > 0 {
		s.v.mu.Lock()
		s.v.bump()
		s.v.mu.Unlock()
		runtime.Gosched()
	}
	if subscribed() != 0 {
		t.Fatal("the slow client was not dropped")
	}
}

// The response bound cuts the runs list and the event bound the event, each reported.
func TestResponseBounds(t *testing.T) {
	s := newServed(t, ServeOptions{})
	cookie := s.session(t)
	s.v.mu.Lock()
	s.v.m.caps.ResponseBytes, s.v.opts.Caps.ResponseBytes = 3000, 3000
	s.v.m.caps.EventBytes, s.v.opts.Caps.EventBytes = 40, 40
	s.v.mu.Unlock()
	response, body := s.request(t, http.MethodGet, "/api/model", nil, cookie, nil)
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusInsufficientStorage {
		t.Errorf("model under a small bound: %d %s", response.StatusCode, body)
	}
	if response.StatusCode == http.StatusOK && !strings.Contains(body, `"truncated":true`) {
		t.Errorf("a response under the bound was not cut: %s", body)
	}
	if event := s.server.event(7); event != "event: model\ndata: {\"version\":7}\n\n" {
		t.Errorf("event over the bound: %q", event)
	}
	snapshot := s.v.Snapshot(0)
	hits := map[string]int64{}
	for _, hit := range snapshot.Bounds {
		hits[hit.Name] = hit.Hits
	}
	if hits[capResponse] == 0 || hits[capEvent] == 0 {
		t.Errorf("bounds %v; want response_bytes and event_bytes reported", hits)
	}
}

// Many sessions and concurrent requests hold up.
func TestConcurrentRequests(t *testing.T) {
	s := newServed(t, ServeOptions{SessionRate: 100})
	cookie := s.session(t)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 5 {
				if response, _ := s.request(t, http.MethodGet, "/api/model", nil, cookie, nil); response.StatusCode != http.StatusOK {
					t.Errorf("model: %d", response.StatusCode)
				}
			}
		}()
	}
	wg.Wait()
}

// firstEvent reads the stream at path until its first event and returns it.
func (s *served) firstEvent(t *testing.T, path string, cookie *http.Cookie) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+s.server.Addr()+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.AddCookie(cookie)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("stream: %d", response.StatusCode)
	}
	var read []byte
	buf := make([]byte, 1)
	for !strings.HasSuffix(string(read), "\n\n") {
		if _, err := response.Body.Read(buf); err != nil {
			t.Fatalf("stream read: %v after %q", err, read)
		}
		read = append(read, buf[0])
	}
	return string(read)
}

// raiseTraceLimit lets the tests write a trace record over the trace client's limit, as a hostile writer under a
// broker configured otherwise could, and returns the producer that writes one.
func raiseTraceLimit(t *testing.T, f *fixture) *kgo.Client {
	t.Helper()
	value := "4194304"
	responses, err := kadm.NewClient(f.client).AlterTopicConfigs(f.ctx, []kadm.AlterConfig{{Op: kadm.SetConfig, Name: "max.message.bytes", Value: &value}}, telemetry.TraceTopic(tenantA))
	if err == nil {
		err = responses[0].Err
	}
	if err != nil {
		t.Fatal(err)
	}
	large, err := kgo.NewClient(append(f.client.Opts(), kgo.ProducerBatchMaxBytes(4<<20))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(large.Close)
	return large
}

// produceLarge writes value, over the trace client's limit, to tenant-a's trace topic.
func produceLarge(t *testing.T, f *fixture, large *kgo.Client, key string, value []byte, at time.Time) {
	t.Helper()
	if err := large.ProduceSync(f.ctx, &kgo.Record{Topic: telemetry.TraceTopic(tenantA), Key: []byte(key), Value: value, Timestamp: at}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}
