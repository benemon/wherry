package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	json "encoding/json/v2"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const httpToken = "tok-0123456789abcdefghijklmnopqrstuvwxyz"

// httpHost serves newMCPServer for b's host over Streamable HTTP on loopback until the test ends, and returns its
// endpoint and what it logs.
func httpHost(t *testing.T, connect func() (*busclient.Host, error)) (string, *syncBuffer) {
	t.Helper()
	logs := &syncBuffer{}
	logger := log.New(logs, "", 0)
	open := inputOpener(t.TempDir(), busclient.Config{})
	server := newMCPServer(connect, inputReader(open), open, func(id string) string { return "wherry wait " + id }, logger, &remoteForms{urlLifetime: time.Hour, outputBound: 1 << 20})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveHTTP(ctx, listener, server, []byte(httpToken), "", "", logger) }()
	t.Cleanup(func() {
		stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	return "http://" + listener.Addr().String() + "/mcp", logs
}

// bearerTransport adds a bearer token to each request.
type bearerTransport struct {
	token string
	base  http.RoundTripper
}

func (b bearerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return b.base.RoundTrip(r)
}

// httpSession connects an MCP client to endpoint with token.
func httpSession(t *testing.T, endpoint, token string, base http.RoundTripper) (*mcp.ClientSession, error) {
	t.Helper()
	transport := &mcp.StreamableClientTransport{Endpoint: endpoint, HTTPClient: &http.Client{Transport: bearerTransport{token, base}}, MaxRetries: -1}
	session, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "v0"}, nil).Connect(context.Background(), transport, nil)
	if err == nil {
		t.Cleanup(func() { session.Close() })
	}
	return session, err
}

const submitCall = `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"submit","arguments":{"prompt":"work"}}}`

// A request without the token, or with another, is refused before the MCP server sees it.
func TestHTTPRefusesARequestWithoutTheToken(t *testing.T) {
	b := newBus(t)
	endpoint, logs := httpHost(t, b.connect(t))
	for name, header := range map[string]string{"none": "", "wrong": "Bearer tok-wrong-0123456789abcdefghijklmnopqrstuv", "basic": "Basic " + httpToken} {
		request, err := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(submitCall))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Accept", "application/json, text/event-stream")
		if header != "" {
			request.Header.Set("Authorization", header)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusUnauthorized || response.Header.Get("WWW-Authenticate") != "Bearer" {
			t.Errorf("%s: status %d, WWW-Authenticate %q; want 401 Bearer", name, response.StatusCode, response.Header.Get("WWW-Authenticate"))
		}
	}
	if _, err := httpSession(t, endpoint, "tok-wrong-0123456789abcdefghijklmnopqrstuv", http.DefaultTransport); err == nil {
		t.Fatal("a client with the wrong token connected")
	}
	records, err := readTopic(b.ctx, b.receiver, admin.PoolTasksTopic("pool-a"))
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("a refused request published %d tasks", len(records))
	}
	if !strings.Contains(logs.String(), "refused an MCP request from 127.0.0.1:") {
		t.Fatalf("log %q; want each refusal", logs.String())
	}
	session, err := httpSession(t, endpoint, httpToken, http.DefaultTransport)
	if err != nil {
		t.Fatal(err)
	}
	var submitted submitOutput
	callTool(t, b.ctx, session, "submit", map[string]any{"prompt": "work"}, &submitted)
	if len(b.tasks(t, submitted.ID)) != 1 {
		t.Fatal("the client with the token published no task")
	}
}

// The token reaches no URL, log line or error: the HTTP host's log, the bus's, tool results and start errors.
func TestHTTPTokenInNoLogOrError(t *testing.T) {
	b := newBus(t)
	endpoint, logs := httpHost(t, b.connect(t))
	var seen []string
	recorder := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		seen = append(seen, r.URL.String())
		return http.DefaultTransport.RoundTrip(r)
	})
	const wrong = "tok-wrong-0123456789abcdefghijklmnopqrstuv"
	if _, err := httpSession(t, endpoint, wrong, recorder); err == nil {
		t.Fatal("a client with the wrong token connected")
	} else {
		seen = append(seen, err.Error())
	}
	session, err := httpSession(t, endpoint, httpToken, recorder)
	if err != nil {
		t.Fatal(err)
	}
	for _, call := range []struct {
		tool      string
		arguments map[string]any
	}{{"status", map[string]any{"id": "run-missing"}}, {"submit", map[string]any{"prompt": "work", "deadline": "soon"}}, {"receivers", map[string]any{}}} {
		result, err := session.CallTool(b.ctx, &mcp.CallToolParams{Name: call.tool, Arguments: call.arguments})
		if err != nil {
			seen = append(seen, err.Error())
			continue
		}
		seen = append(seen, toolText(result))
	}
	dir := t.TempDir()
	open := filepath.Join(dir, "open")
	if err := os.WriteFile(open, []byte(httpToken), 0o644); err != nil {
		t.Fatal(err)
	}
	short := filepath.Join(dir, "short")
	if err := os.WriteFile(short, []byte(httpToken[:31]), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{open, short} {
		if _, err := listenToken("127.0.0.1:0", path, "", ""); err == nil {
			t.Fatalf("%s: accepted", path)
		} else {
			seen = append(seen, err.Error())
		}
	}
	seen = append(seen, logs.String(), b.logs.String())
	for _, text := range seen {
		for _, secret := range []string{httpToken, wrong, httpToken[:31]} {
			if strings.Contains(text, secret) {
				t.Fatalf("%q carries a token", text)
			}
		}
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// A listener off loopback needs a token and TLS; one on loopback needs a token.
func TestListenFlags(t *testing.T) {
	dir := t.TempDir()
	token := filepath.Join(dir, "token")
	if err := os.WriteFile(token, []byte(httpToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"--listen", "0.0.0.0:7443", "--token-file", token},
		{"--listen", ":7443", "--token-file", token},
		{"--listen", "localhost:7443", "--token-file", token},
		{"--listen", "192.0.2.1:7443", "--token-file", token, "--tls-cert", token},
		{"--listen", "127.0.0.1:7443"},
		{"--listen", "0.0.0.0:7443", "--tls-cert", token, "--tls-key", token},
		{"--token-file", token},
		{"--tls-cert", token, "--tls-key", token},
		{"--listen", "127.0.0.1"},
	} {
		if err := runMCP(context.Background(), append(args, "--config-file", filepath.Join(dir, "missing.yaml"))); err == nil || strings.Contains(err.Error(), "missing.yaml") {
			t.Errorf("%v: %v; want a refusal before the configuration is read", args, err)
		}
	}
	for _, args := range [][]string{{"127.0.0.1:7443", ""}, {"[::1]:7443", ""}, {"0.0.0.0:7443", "cert"}} {
		if got, err := listenToken(args[0], token, args[1], args[1]); err != nil || string(got) != httpToken {
			t.Errorf("%v: %q, %v; want the token", args, got, err)
		}
	}
}

// With a certificate, the host serves HTTPS alone.
func TestHTTPServesTLS(t *testing.T) {
	b := newBus(t)
	cert, key, pool := selfSigned(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	open := inputOpener(t.TempDir(), busclient.Config{})
	server := newMCPServer(b.connect(t), inputReader(open), open, func(id string) string { return "wherry wait " + id }, log.New(testLog{t}, "", 0), &remoteForms{urlLifetime: time.Hour, outputBound: 1 << 20})
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- serveHTTP(ctx, listener, server, []byte(httpToken), cert, key, log.New(testLog{t}, "", 0))
	}()
	defer func() { stop(); <-done }()
	address := listener.Addr().String()
	if _, err := httpSession(t, "http://"+address+"/mcp", httpToken, http.DefaultTransport); err == nil {
		t.Fatal("plain HTTP connected")
	}
	session, err := httpSession(t, "https://"+address+"/mcp", httpToken, &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}})
	if err != nil {
		t.Fatal(err)
	}
	var receivers receiversOutput
	callTool(t, b.ctx, session, "receivers", map[string]any{}, &receivers)
	if len(receivers.Receivers) != 1 {
		t.Fatalf("receivers %+v", receivers)
	}
}

// selfSigned writes a certificate for 127.0.0.1 and its key, and returns their files and a pool trusting it.
func selfSigned(t *testing.T) (string, string, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "wherry-test"}, NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	encodedKey, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	cert, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encodedKey}), 0o600); err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(parsed)
	return cert, keyFile, pool
}

// Over HTTP an input carries its content; a path or a prompt file, which would name the host's files, is refused.
func TestHTTPInputsCarryTheirContent(t *testing.T) {
	b := newBus(t)
	endpoint, _ := httpHost(t, b.connect(t))
	session, err := httpSession(t, endpoint, httpToken, http.DefaultTransport)
	if err != nil {
		t.Fatal(err)
	}
	for name, arguments := range map[string]map[string]any{
		"path":        {"prompt": "work", "inputs": []any{map[string]any{"path": "/etc/hostname"}}},
		"path, named": {"prompt": "work", "inputs": []any{map[string]any{"path": "/etc/hostname", "name": "hostname"}}},
		"no name":     {"prompt": "work", "inputs": []any{map[string]any{"content": "YQ=="}}},
		"both forms":  {"prompt": "work", "inputs": []any{map[string]any{"name": "a", "content": "YQ==", "upload": "upload-1"}}},
	} {
		if failure := callToolError(t, b.ctx, session, "submit", arguments); failure.Message != "inputs over HTTP carry their content: each is {name, content} or {upload}" {
			t.Errorf("%s: %+v", name, failure)
		}
	}
	if failure := callToolError(t, b.ctx, session, "submit", map[string]any{"prompt_file": "/etc/hostname"}); failure.Message != "prompt_file is refused over HTTP; prompt carries the text" {
		t.Errorf("prompt_file: %+v", failure)
	}
	if failure := callToolError(t, b.ctx, session, "submit", map[string]any{"prompt": "work", "inputs": []any{map[string]any{"upload": "upload-1"}}}); failure.Code != "INVALID_ARGUMENT" || !strings.Contains(failure.Message, "not one this host issued") {
		t.Errorf("unknown upload: %+v", failure)
	}
	if failure := callToolError(t, b.ctx, session, "upload", map[string]any{"name": "big.bin", "size": 1, "sha256": hashOf("x")}); failure.Code != "INVALID_ARGUMENT" || !strings.Contains(failure.Message, "no storage credential") {
		t.Errorf("upload without storage: %+v", failure)
	}
	var submitted submitOutput
	callTool(t, b.ctx, session, "submit", map[string]any{"prompt": "work", "inputs": []any{map[string]any{"name": "data.csv", "content": base64.StdEncoding.EncodeToString([]byte("a,b\n"))}}}, &submitted)
	if submitted.Wait != "" {
		t.Errorf("submit over HTTP returned the host's shell command %q", submitted.Wait)
	}
	parts := b.tasks(t, submitted.ID)[0].submission.Message.Parts
	if len(parts) != 2 || parts[1].Filename != "data.csv" || string(parts[1].Raw) != "a,b\n" {
		t.Fatalf("parts %+v; want the prompt and data.csv", parts)
	}
	stdio := mcpSession(t, b.connect(t), nil)
	if failure := callToolError(t, b.ctx, stdio, "submit", map[string]any{"prompt": "work", "inputs": []any{map[string]any{"name": "data.csv", "content": "YQ=="}}}); failure.Code != "INVALID_ARGUMENT" {
		t.Errorf("content over stdio: %+v", failure)
	}
	tools, err := stdio.ListTools(b.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range tools.Tools {
		if tool.Name == "upload" {
			t.Error("stdio serves upload")
		}
	}
}

// Over HTTP, wait lists outputs and the reply file as resources that resources/read returns, and cleanup deletes them.
func TestHTTPWaitListsOutputsAsResources(t *testing.T) {
	b := newBus(t)
	admitted := b.admitted(t)
	runID := admitted.submission.Wherry.Run
	b.outputResult(t, admitted, "sub/report.md", "# Report\n", hashOf("# Report\n"))
	endpoint, _ := httpHost(t, b.connect(t))
	session, err := httpSession(t, endpoint, httpToken, http.DefaultTransport)
	if err != nil {
		t.Fatal(err)
	}
	result, err := session.CallTool(b.ctx, &mcp.CallToolParams{Name: "wait", Arguments: map[string]any{"id": runID}})
	if err != nil || result.IsError {
		t.Fatalf("wait: %v %s", err, toolText(result))
	}
	uri := "wherry://runs/" + runID + "/outputs/sub/report.md"
	var waited waitReply
	if err := json.Unmarshal([]byte(toolText(result)), &waited); err != nil {
		t.Fatal(err)
	}
	if len(waited.Outputs) != 1 || waited.Outputs[0].URI != uri || waited.Outputs[0].Path != "" || waited.Outputs[0].URL != "" || waited.Outputs[0].Size != 9 {
		t.Fatalf("outputs %+v; want the resource alone", waited.Outputs)
	}
	var links []string
	for _, content := range result.Content {
		if link, ok := content.(*mcp.ResourceLink); ok {
			links = append(links, link.URI)
		}
	}
	if len(links) != 1 || links[0] != uri {
		t.Fatalf("resource links %v, want %s", links, uri)
	}
	read, err := session.ReadResource(b.ctx, &mcp.ReadResourceParams{URI: uri})
	if err != nil || len(read.Contents) != 1 || read.Contents[0].Text != "# Report\n" {
		t.Fatalf("read %+v, %v", read, err)
	}
	for _, escape := range []string{"wherry://runs/" + runID + "/outputs/../../" + runID + ".json", "wherry://runs/../outputs/state.json", "wherry://runs/" + runID + "/outputs/missing"} {
		if _, err := session.ReadResource(b.ctx, &mcp.ReadResourceParams{URI: escape}); err == nil {
			t.Errorf("read %s", escape)
		}
	}
	var cleaned cleanupOutput
	callTool(t, b.ctx, session, "cleanup", map[string]any{"id": runID}, &cleaned)
	if _, err := os.Stat(filepath.Join(b.stateDir, "runs", runID, "outputs")); !os.IsNotExist(err) {
		t.Fatalf("cleanup left the outputs: %v", err)
	}
	if _, err := session.ReadResource(b.ctx, &mcp.ReadResourceParams{URI: uri}); err == nil {
		t.Fatal("read a cleaned output")
	}

	reply := strings.Repeat("r", wire.OutputChunk) + "\xff"
	b, runID = replyRun(t, reply, reply, reply, false)
	endpoint, _ = httpHost(t, b.connect(t))
	if session, err = httpSession(t, endpoint, httpToken, http.DefaultTransport); err != nil {
		t.Fatal(err)
	}
	waited = waitReply{}
	callTool(t, b.ctx, session, "wait", map[string]any{"id": runID}, &waited)
	if uri := "wherry://runs/" + runID + "/outputs/" + wire.ReplyFile; waited.Reply == nil || waited.Reply.URI != uri || waited.Reply.Path != "" {
		t.Fatalf("reply %+v; want %s", waited.Reply, uri)
	}
	read, err = session.ReadResource(b.ctx, &mcp.ReadResourceParams{URI: waited.Reply.URI})
	if err != nil || len(read.Contents) != 1 || string(read.Contents[0].Blob) != reply || read.Contents[0].MIMEType != "application/octet-stream" {
		t.Fatalf("read the reply: %v", err)
	}
}

// A file over the bound or stored in S3 is a presigned GET; one within it, or any without storage, a resource.
func TestRemoteForm(t *testing.T) {
	presigned := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name    string
		size    int64
		stored  bool
		presign error
		want    remoteFile
	}{
		{name: "within", size: 10, want: remoteFile{URI: "wherry://runs/run-a/outputs/out.bin"}},
		{name: "at the bound", size: 100, want: remoteFile{URI: "wherry://runs/run-a/outputs/out.bin"}},
		{name: "over", size: 101, want: remoteFile{URL: "https://s3.example/out.bin", Expires: "2026-10-29T12:00:00Z"}},
		{name: "stored", size: 1, stored: true, want: remoteFile{URL: "https://s3.example/out.bin", Expires: "2026-10-29T12:00:00Z"}},
		{name: "over, no storage", size: 101, presign: fmt.Errorf("%w: no storage", busclient.ErrNoStorage), want: remoteFile{URI: "wherry://runs/run-a/outputs/out.bin"}},
	} {
		got, err := remoteForm("run-a", "out.bin", test.size, test.stored, 100, func(name string) (string, time.Time, error) {
			return "https://s3.example/" + name, presigned, test.presign
		})
		if err != nil || got != test.want {
			t.Errorf("%s: %+v, %v; want %+v", test.name, got, err, test.want)
		}
	}
	if _, err := remoteForm("run-a", "out.bin", 101, false, 100, func(string) (string, time.Time, error) {
		return "", time.Time{}, errors.New("S3 down")
	}); err == nil {
		t.Error("a failed presign gave a form")
	}
}

// The HTTP host answers a seal request for a run whose client submitted it and disconnected.
func TestHTTPHostAnswersWithNoClientConnected(t *testing.T) {
	b := newBus(t)
	connect, closeHost := sharedHost(b.connect(t))
	defer closeHost()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	logger := log.New(testLog{t}, "", 0)
	open := inputOpener(t.TempDir(), busclient.Config{})
	server := newMCPServer(connect, inputReader(open), open, func(string) string { return "" }, logger, &remoteForms{urlLifetime: time.Hour, outputBound: 1 << 20})
	ctx, stop := context.WithCancel(b.ctx)
	done := make(chan error, 1)
	go func() {
		done <- serveMCP(ctx, connect, logger, func(ctx context.Context) error {
			return serveHTTP(ctx, listener, server, []byte(httpToken), "", "", logger)
		})
	}()
	defer func() { stop(); <-done }()
	session, err := httpSession(t, "http://"+listener.Addr().String()+"/mcp", httpToken, http.DefaultTransport)
	if err != nil {
		t.Fatal(err)
	}
	var submitted submitOutput
	callTool(t, b.ctx, session, "submit", map[string]any{"prompt": "work"}, &submitted)
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}

	admitted := b.tasks(t, submitted.ID)[0]
	b.feed(t, submitted.ID, b.echo(t, admitted), b.status(t, admitted, "claimed-1", "claimed", now(wire.TaskStateWorking)))
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	request := wire.SealRequest{ReceiverBinding: b.binding(admitted), Key: jose.JSONWebKey{Key: &key.PublicKey}, KeyID: signed.SealKeyID(&key.PublicKey),
		Credentials: []string{"model"}, ExpiryMS: time.Now().Add(time.Minute).UnixMilli()}
	b.feed(t, submitted.ID, b.sign(t, &request))
	binding := signed.SealBinding{Tenant: request.Tenant, Pool: request.Pool, Receiver: request.Receiver, KeyID: request.KeyID, Run: request.Run,
		Attempt: request.Attempt, Credentials: request.Credentials, ExpiryMS: request.ExpiryMS}
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		records, err := readTopic(b.ctx, b.receiver, admin.HostTopic("tenant-a", "host-a"))
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range records {
			if values, err := signed.OpenSeal(record.Value, binding, key, time.Now()); err == nil {
				if string(values.Values["model"]) != credentialText {
					t.Fatalf("sealed %q", values.Values["model"])
				}
				return
			}
		}
	}
	t.Fatalf("no seal answered with no client connected; logs:\n%s", b.logs.String())
}
