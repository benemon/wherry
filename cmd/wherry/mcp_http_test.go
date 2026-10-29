package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
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
	server := newMCPServer(connect, inputReader(open), open, func(id string) string { return "wherry wait " + id }, logger)
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
	server := newMCPServer(b.connect(t), inputReader(open), open, func(id string) string { return "wherry wait " + id }, log.New(testLog{t}, "", 0))
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
