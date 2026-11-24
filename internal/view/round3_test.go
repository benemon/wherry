package view

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	json "encoding/json/v2"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/telemetry"
	"github.com/benemon/wherry/internal/wire"
	jose "github.com/go-jose/go-jose/v4"
)

// Every fact from a signed record is labelled with the principal of the key that verified it, never with a name the
// record's text carries: a wrap the custodian signed naming another receiver is labelled with the custodian. Facts from
// unsigned records carry the topic they were read from.
func TestSignerLabels(t *testing.T) {
	f := newFixture(t)
	t0 := f.now.Add(-10 * time.Minute)
	f.snapshot(t0, poolA, receiverB)
	f.advertise(f.now.Add(-5*time.Second), receiverA, poolA, 2, 1)
	run := f.echo(t0.Add(time.Minute), receiverA, tenantA, hostA, poolA, "signers")
	claim := f.status(t0.Add(time.Minute+time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "claimed", state: wire.TaskStateWorking})
	f.decision(t0.Add(time.Minute+2*time.Second), receiverB, poolA, run, claim, "", wire.ClaimUnchanged, true)
	request := f.openRequest(t0.Add(time.Minute+3*time.Second), receiverA, poolA, run, 1)
	f.wrap(t0.Add(time.Minute+4*time.Second), receiverB, poolA, run, request, receiverA, 1)
	held := f.echo(t0.Add(time.Minute+5*time.Second), receiverA, tenantA, hostA, poolA, "held")
	f.status(t0.Add(time.Minute+5*time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: held, kind: "claimed", state: wire.TaskStateWorking})
	f.progress(t0.Add(time.Minute+5*time.Second), receiverA, held, tenantA, 1, 3)
	f.cancel(t0.Add(time.Minute+6*time.Second), tenantA, hostA, poolA, run, nil)
	f.status(t0.Add(2*time.Minute), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "terminal", state: wire.TaskStateCanceled})
	f.result(t0.Add(2*time.Minute), receiverA, tenantA, poolA, run, 1, wire.TaskStateCanceled, wire.ResultMetadata{Profile: "profile-a"}, "reply")
	f.startFailure(t0.Add(2*time.Minute+time.Second), receiverB, tenantA, poolA, run, 2, "dispatch")
	v := f.view(time.Hour, testCaps())
	a, b, host := operator.ReceiverPrincipal(receiverA), operator.ReceiverPrincipal(receiverB), operator.TenantPrincipal(tenantA, hostA)
	d, ok := v.RunDetail(run)
	if !ok || len(d.Echoes) != 1 || len(d.Attempts) != 2 || len(d.Attempts[0].Events) != 2 || d.Attempts[0].Decision == nil || len(d.Opens) != 1 || len(d.Cancels) != 1 || d.Result == nil || d.Attempts[1].StartFailure == nil {
		t.Fatalf("detail: %+v %v", d, ok)
	}
	for name, got := range map[string]string{
		"echo": d.Echoes[0].Signer, "claimed": d.Attempts[0].Events[0].Signer, "terminal": d.Attempts[0].Events[1].Signer, "open request": d.Opens[0].Signer, "result": d.Result.Signer,
	} {
		if got != a {
			t.Errorf("%s signer %q, want %s", name, got, a)
		}
	}
	for name, got := range map[string]string{"claim decision": d.Attempts[0].Decision.Signer, "wrap naming receiver-a": d.Opens[0].AnswerSigner, "start failure": d.Attempts[1].StartFailure.Signer} {
		if got != b {
			t.Errorf("%s signer %q, want %s", name, got, b)
		}
	}
	if d.Cancels[0].Signer != host {
		t.Errorf("cancel signer %q, want %s", d.Cancels[0].Signer, host)
	}
	if d.Opens[0].Custodian != receiverB || d.Opens[0].Receiver != receiverA {
		t.Errorf("open exchange: %+v", d.Opens)
	}
	s := v.Snapshot(0)
	if len(s.Custodians) != 1 || s.Custodians[0].Signer != b {
		t.Errorf("custodian signer: %+v", s.Custodians)
	}
	// The progress record is unsigned: the run it says receiver-a holds shows the progress topic, no signer.
	holder := receiverOf(t, s, receiverA)
	if len(holder.Held) != 1 || holder.Held[0].Run != held || holder.Held[0].Source != operator.ReceiverProgressTopic(receiverA) {
		t.Errorf("held run's source: %+v", holder.Held)
	}
	encoded, err := json.Marshal(holder.Held)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "signer") || strings.Contains(string(encoded), a) {
		t.Errorf("a progress fact carries a signer: %s", encoded)
	}
}

// A snapshot by a custodian since ended is present: membership over time is the ACLs' decision, and the snapshot is
// labelled with its signer.
func TestEndedCustodianSnapshotPresent(t *testing.T) {
	f := newFixture(t)
	t0 := f.now.Add(-10 * time.Minute)
	keyID := f.snapshot(t0, poolA, receiverB)
	f.operator(t0.Add(time.Minute), operator.Record{Kind: "end", Receiver: receiverB})
	v := f.view(time.Hour, testCaps())
	s := v.Snapshot(0)
	if len(s.Custodians) != 1 || s.Custodians[0].Receiver != receiverB || s.Custodians[0].KeyID != keyID || s.Custodians[0].Status != "ended" || s.Custodians[0].Signer != operator.ReceiverPrincipal(receiverB) {
		t.Fatalf("snapshot by a since-ended custodian: %+v", s.Custodians)
	}
	if s.Counts["refused"] != 0 {
		t.Errorf("counts %v", s.Counts)
	}
	// One naming another pool than the topic it was read from is refused.
	if err := f.service.PoolApply(f.ctx, poolB, testTerms(bucketB), false); err != nil {
		t.Fatal(err)
	}
	f.addReceiver(receiverC, poolB)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := &wire.Snapshot{Pool: poolB, Custodian: receiverC, Reason: "bootstrap", Current: wire.PoolKey{KeyID: signed.KeyThumbprint(&key.PublicKey), Key: jose.JSONWebKey{Key: &key.PublicKey}}}
	f.produce(operator.CustodianTopic(poolA), wire.CustodianKey, f.sign(snapshot, f.keys[receiverC]), f.now)
	f.tick(v)
	f.tick(v)
	s = v.Snapshot(0)
	if len(s.Custodians) != 1 || s.Custodians[0].Receiver != receiverB || s.Counts["refused"] != 1 {
		t.Errorf("snapshots after a wrong-pool one: %+v counts %v", s.Custodians, s.Counts)
	}
}

// A result shows each output name within the output name grammar and the placeholder for one outside it; a span's
// output attribute is still dropped.
func TestOutputNames(t *testing.T) {
	f := newFixture(t)
	t0 := f.now.Add(-10 * time.Minute)
	run := f.echo(t0, receiverA, tenantA, hostA, poolA, "outputs")
	f.status(t0.Add(time.Second), st{receiver: receiverA, tenant: tenantA, pool: poolA, run: run, kind: "terminal", state: wire.TaskStateCompleted})
	valid := "s3cr3t-output/part.bin"
	f.result(t0.Add(2*time.Second), receiverA, tenantA, poolA, run, 1, wire.TaskStateCompleted, wire.ResultMetadata{Profile: "profile-a",
		Outputs:     []wire.ReturnedOutput{{Name: valid, SHA256: hashOf("part"), Size: 3}, {Name: "<script>", SHA256: hashOf("script"), Size: 4}},
		S3Outputs:   []wire.ReturnedOutput{{Name: "/etc/passwd", SHA256: hashOf("passwd"), Size: 5, Key: "k"}, {Name: strings.Repeat("a", 513), SHA256: hashOf("long"), Size: 6, Key: "k"}},
		NotReturned: []wire.UnreturnedOutput{{Name: valid, Reason: "missing"}, {Name: ".hidden", Reason: "missing"}}}, "reply")
	f.traceRecord(t0.Add(3*time.Second), "wherry-host."+tenantA+"."+hostA, span(id(16), id(8), nil, "submit", run, t0, str(string(telemetry.AttrOutput), valid)))
	s := serveFixture(t, f, ServeOptions{})
	cookie := s.session(t)
	response, body := s.request(t, http.MethodGet, "/api/runs/"+run, nil, cookie, nil)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("run: %d %s", response.StatusCode, body)
	}
	var d RunDetail
	if err := json.Unmarshal([]byte(body), &d); err != nil {
		t.Fatal(err)
	}
	if d.Result == nil || len(d.Result.Outputs) != 2 || d.Result.Outputs[0].Name != valid || d.Result.Outputs[1].Name != InvalidName ||
		len(d.Result.S3Outputs) != 2 || d.Result.S3Outputs[0].Name != InvalidName || d.Result.S3Outputs[1].Name != InvalidName ||
		len(d.Result.NotReturned) != 2 || d.Result.NotReturned[0].Name != valid || d.Result.NotReturned[1].Name != InvalidName {
		t.Errorf("output names: %+v", d.Result)
	}
	if !strings.Contains(body, `"name":"`+valid+`"`) {
		t.Errorf("the valid name is not in the JSON: %s", body)
	}
	for _, text := range []string{"<script>", "/etc/passwd", ".hidden", "aaaa"} {
		if strings.Contains(body, text) {
			t.Errorf("an invalid output name reached the JSON: %q in %s", text, body)
		}
	}
	forest, err := s.v.traces.Trace(f.ctx, run)
	if err != nil || forest.Spans != 1 {
		t.Fatalf("trace: %+v %v", forest, err)
	}
	if _, ok := forest.Traces[0].Roots[0].Attributes[string(telemetry.AttrOutput)]; ok || strings.Contains(forest.String(), valid) {
		t.Errorf("a span's output attribute passed: %+v", forest.Traces[0].Roots[0])
	}
}

// The page writes every value as text: its script sets no markup.
func TestPageRendersText(t *testing.T) {
	script, err := static.ReadFile("static/app.js")
	if err != nil {
		t.Fatal(err)
	}
	for _, sink := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write"} {
		if strings.Contains(string(script), sink) {
			t.Errorf("app.js uses %s", sink)
		}
	}
	if !strings.Contains(string(script), "textContent") {
		t.Error("app.js does not render through textContent")
	}
}

// Every request passes the guard before the router: a path the router would redirect, CONNECT, OPTIONS * and a
// foreign Host each get a fixed status and body with every header, nothing of the request reflected, and one log
// line naming no route; an allowed Host with its port passes, and the stream still flushes through the wrapper.
func TestGuardsOutsideTheMux(t *testing.T) {
	s := newServed(t, ServeOptions{})
	port := strconv.Itoa(s.server.Port())
	own := "127.0.0.1:" + port
	cookie := s.session(t)
	for _, c := range []struct {
		name, line, host, cookie string
		status                   int
		body, log                string
	}{
		{"empty segment", "GET /s3cr3t-path//tail?token=s3cr3t HTTP/1.1", own, cookie.Value, http.StatusNotFound, textNotFound + "\n", "view: GET other 404\n"},
		{"dot segment", "GET /api/./model HTTP/1.1", own, cookie.Value, http.StatusNotFound, textNotFound + "\n", "view: GET other 404\n"},
		{"dot-dot segment", "GET /api/s3cr3t/../model HTTP/1.1", own, cookie.Value, http.StatusNotFound, textNotFound + "\n", "view: GET other 404\n"},
		{"encoded dot-dot", "GET /api/%2e%2e/api/model HTTP/1.1", own, cookie.Value, http.StatusNotFound, textNotFound + "\n", "view: GET other 404\n"},
		{"encoded slash", "GET /api%2Fmodel HTTP/1.1", own, cookie.Value, http.StatusNotFound, textNotFound + "\n", "view: GET other 404\n"},
		{"trailing slash", "GET /api/model/ HTTP/1.1", own, cookie.Value, http.StatusNotFound, textNotFound + "\n", "view: GET other 404\n"},
		{"connect", "CONNECT " + own + " HTTP/1.1", own, "", http.StatusMethodNotAllowed, textMethodNotAllowed + "\n", "view: other other 405\n"},
		{"options star", "OPTIONS * HTTP/1.1", own, "", http.StatusBadRequest, "", "view: OPTIONS other 400\n"},
		{"allowed host with port", "GET /api/model HTTP/1.1", own, cookie.Value, http.StatusOK, "", "view: GET model 200\n"},
		{"localhost with port", "GET /api/model HTTP/1.1", "localhost:" + port, cookie.Value, http.StatusOK, "", "view: GET model 200\n"},
		{"foreign host with port", "GET /api/model HTTP/1.1", "s3cr3t.example:" + port, cookie.Value, http.StatusForbidden, textForbidden + "\n", "view: GET other 403\n"},
		{"foreign host without port", "GET /api/model HTTP/1.1", "s3cr3t.example", cookie.Value, http.StatusForbidden, textForbidden + "\n", "view: GET other 403\n"},
		{"foreign host on a redirecting path", "GET /s3cr3t-path//tail HTTP/1.1", "s3cr3t.example", cookie.Value, http.StatusForbidden, textForbidden + "\n", "view: GET other 403\n"},
		{"api without a cookie", "GET /api/model HTTP/1.1", own, "", http.StatusForbidden, textForbidden + "\n", "view: GET model 403\n"},
	} {
		t.Run(c.name, func(t *testing.T) {
			before := s.logs.String()
			request := c.line + "\r\nHost: " + c.host + "\r\nConnection: close\r\n"
			if c.cookie != "" {
				request += "Cookie: " + cookie.Name + "=" + c.cookie + "\r\n"
			}
			response, body := s.raw(t, request+"\r\n")
			if response.StatusCode != c.status || c.body != "" && body != c.body || c.body == "" && response.StatusCode != http.StatusOK && body != "" {
				t.Errorf("status %d body %q, want %d %q", response.StatusCode, body, c.status, c.body)
			}
			for header, want := range map[string]string{"Cache-Control": "no-store", "Referrer-Policy": "no-referrer", "Content-Security-Policy": csp, "X-Content-Type-Options": "nosniff"} {
				if got := response.Header.Get(header); got != want {
					t.Errorf("%s is %q, want %q", header, got, want)
				}
			}
			if location := response.Header.Get("Location"); location != "" || strings.Contains(body, "s3cr3t") {
				t.Errorf("the request is reflected: location %q body %q", location, body)
			}
			// The stream is logged when it ends, so the lines of a stream another test left may land here; the request's
			// own line is one, and no line carries the request's text.
			lines := strings.TrimPrefix(s.logs.String(), before)
			if strings.Count(lines, c.log) != 1 || strings.Contains(lines, "s3cr3t") || strings.Contains(lines, "/") {
				t.Errorf("log %q, want one line %q", lines, c.log)
			}
		})
	}
	if event := s.firstEvent(t, "/api/stream", cookie); !strings.HasPrefix(event, "event: model\ndata: {\"version\":") {
		t.Errorf("the stream does not flush through the wrapper: %q", event)
	}
}

// raw sends request, a whole HTTP/1.1 request, over one connection and returns the response and its body.
func (s *served) raw(t *testing.T, request string) (*http.Response, string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", s.server.Addr(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.WriteString(conn, request); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	return response, string(body)
}
