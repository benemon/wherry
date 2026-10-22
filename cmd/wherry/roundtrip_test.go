package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/benemon/wherry/adapter"
	"github.com/benemon/wherry/internal/admin"
	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/receiver"
	"github.com/benemon/wherry/internal/wire"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"gopkg.in/yaml.v3"
)

// platformText is the receiver bundle's platform credential for the test plugin's adapter, and deployText the host's
// task credential.
const (
	platformText = "s3cr3t-platform-credential"
	deployText   = "s3cr3t-deploy-key-credential"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// tlsCluster is kfake behind TLS with a throwaway CA, so bundles carry a real CA and principals log in over TLS.
func tlsCluster(t *testing.T) (*kfake.Cluster, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "roundtrip"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	pair, err := tls.X509KeyPair(certPEM, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	if err != nil {
		t.Fatal(err)
	}
	cluster := kfake.MustCluster(kfake.TLS(&tls.Config{Certificates: []tls.Certificate{pair}}), kfake.EnableSASL(), kfake.EnableACLs(),
		kfake.Superuser("SCRAM-SHA-512", "admin", "password"), kfake.Superuser("SCRAM-SHA-512", "wherry.operator", "operator-password"))
	t.Cleanup(cluster.Close)
	return cluster, string(certPEM)
}

func connectTLS(t *testing.T, address, ca, user, password string) *kgo.Client {
	t.Helper()
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM([]byte(ca))
	client, err := kgo.NewClient(kgo.SeedBrokers(address), kgo.DialTLSConfig(&tls.Config{RootCAs: roots}), kgo.SASL(scram.Auth{User: user, Pass: password}.AsSha512Mechanism()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

// A password file's last line break is not part of the password, for every client KafkaOpts builds; the admin
// command's client and a revocation probe stand for them.
func TestPasswordLineBreakIsNotThePassword(t *testing.T) {
	ctx := context.Background()
	cluster, ca := tlsCluster(t)
	address := cluster.ListenAddrs()[0]
	dir := t.TempDir()
	caPath, passwordPath := filepath.Join(dir, "ca.pem"), filepath.Join(dir, "password")
	if err := os.WriteFile(caPath, []byte(ca), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(passwordPath, []byte("password\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WHERRY_TRUST_BROKER", address)
	t.Setenv("WHERRY_TRUST_CA", caPath)
	t.Setenv("WHERRY_TEST_USER", "admin")
	t.Setenv("WHERRY_TEST_PASSWORD_FILE", passwordPath)
	client, err := adminKafka("WHERRY_TEST_USER", "WHERRY_TEST_PASSWORD_FILE")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("admin client with a password file ending in a line break: %v", err)
	}
	service := &admin.Service{Admin: connectTLS(t, address, ca, "admin", "password"), Broker: address, CA: ca}
	// The superuser is authorised, so a probe that logs in finds it still authorised.
	if err := service.Probe("wherry.admin")(ctx, "admin", "password\n"); !errors.Is(err, admin.ErrStillAuthorised) {
		t.Fatalf("probe with a password ending in a line break = %v; want a login", err)
	}
	if err := service.Probe("wherry.admin")(ctx, "admin", "other"); errors.Is(err, admin.ErrStillAuthorised) {
		t.Fatal("probe with a wrong password logged in")
	}
}

// resalt works around kfake's SCRAM handshake, which always names 4096 iterations.
func resalt(t *testing.T, service *admin.Service, bundlePath string) {
	t.Helper()
	encoded, err := os.ReadFile(bundlePath)
	if err != nil {
		t.Fatal(err)
	}
	var bundle admin.Bundle
	if err := yaml.Unmarshal(encoded, &bundle); err != nil {
		t.Fatal(err)
	}
	upsert := kadm.UpsertSCRAM{User: bundle.Principal, Mechanism: kadm.ScramSha512, Iterations: 4096, Password: bundle.Password}
	if altered, err := kadm.NewClient(service.Admin).AlterUserSCRAMs(context.Background(), nil, []kadm.UpsertSCRAM{upsert}); err != nil || altered.Error() != nil {
		t.Fatalf("re-salt %s: %v %v", bundle.Principal, err, altered.Error())
	}
}

// TestRoundTrip runs a task from a host's Submit through a receiver and the test plugin process to the host's Await,
// on records each side produces for the other: provisioning through wherry admin, bundles, the host and receiver
// constructors, plugin discovery and the adapter link, the real claim, admission, the seal request Await answers, dual
// write and feed reads.
func TestRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cluster, ca := tlsCluster(t)
	address := cluster.ListenAddrs()[0]
	service := &admin.Service{Admin: connectTLS(t, address, ca, "admin", "password"), Operator: connectTLS(t, address, ca, "wherry.operator", "operator-password"),
		Broker: address, CA: ca, DeliveryCeiling: 10}
	dir := t.TempDir()
	terms := admin.Terms{Agents: []admin.AgentTerms{{Name: "claude", Credentials: []string{"model"}}}, DefaultAgent: "claude",
		DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
	bucket := s3Oracle(t)
	if bucket == nil {
		bucket = fallbackS3(t)
	}
	terms.Storage = &admin.Storage{Endpoint: bucket.endpoint, Bucket: s3Bucket}
	hostBundle, otherBundle, receiverBundle := filepath.Join(dir, "host.yaml"), filepath.Join(dir, "other.yaml"), filepath.Join(dir, "receiver.yaml")
	config := admin.ReceiverConfig{Receiver: "receiver-a", Pool: "pool-a", Capacity: 2,
		Adapters: []admin.AdapterSettings{{Name: "fake", Platform: "fake-platform", Type: "fake", Agent: "claude", Credential: "model", Labels: map[string]string{"gpu": "yes"}, Config: map[string]any{"outputs": true, "inline_limit": 1000}}}}
	for _, step := range []func() error{
		func() error { return service.PoolCreate(ctx, "pool-a", terms) },
		func() error { return service.TenantAdd(ctx, "tenant-a", "pool-a") },
		func() error { return service.HostAdd(ctx, "tenant-a", "host-a", hostBundle) },
		func() error { return service.HostAdd(ctx, "tenant-a", "host-b", otherBundle) },
		func() error {
			return service.ReceiverAdd(ctx, config, map[string]string{"fake-platform": platformText}, receiverBundle)
		},
	} {
		if err := step(); err != nil {
			t.Fatal(err)
		}
	}
	resalt(t, service, hostBundle)
	resalt(t, service, otherBundle)
	resalt(t, service, receiverBundle)

	encoded, err := os.ReadFile(receiverBundle)
	if err != nil {
		t.Fatal(err)
	}
	var bundle admin.Bundle
	if err := yaml.Unmarshal(encoded, &bundle); err != nil {
		t.Fatal(err)
	}
	stateDir := filepath.Join(dir, "receiver-state")
	logs := &syncBuffer{}
	logger := log.New(logs, "receiver: ", 0)
	newReceiver := func(plugins *plugins) *receiver.Receiver {
		t.Helper()
		r, err := receiver.New(bundle, stateDir, plugins.factory(bundle), logger, prometheus.NewRegistry())
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	missing := &plugins{dir: t.TempDir(), logs: logs, registry: prometheus.NewRegistry(), running: map[string]*adapter.Plugin{}}
	if err := newReceiver(missing).Run(ctx); err == nil || err.Error() != "adapter fake: no plugin for type fake" {
		t.Fatalf("receiver without its plugin: %v; want a configuration error", err)
	}
	read := recordPlugin(t)
	plugins := &plugins{dir: pluginDir, logs: logs, registry: prometheus.NewRegistry(), running: map[string]*adapter.Plugin{}}
	defer plugins.close()
	r := newReceiver(plugins)
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go func() { _ = r.Run(runCtx) }()

	credentialPath, deployPath := filepath.Join(dir, "model"), filepath.Join(dir, "deploy-key")
	if err := os.WriteFile(credentialPath, []byte(credentialText), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(deployPath, []byte(deployText), 0o600); err != nil {
		t.Fatal(err)
	}
	hostConfig := busclient.Config{Bundle: hostBundle, HostKey: filepath.Join(dir, "host.key"), StateDir: filepath.Join(dir, "host-state"),
		Credentials: map[string]string{"model": credentialPath, "deploy-key": deployPath}}
	hostConfig.Storage = bucket.config
	if _, err := busclient.Init(ctx, hostConfig); err != nil {
		t.Fatal(err)
	}
	hostLogs := &syncBuffer{}
	host, err := busclient.Connect(hostConfig, log.New(hostLogs, "host: ", 0))
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	var submitted busclient.SubmitResult
	for ctx.Err() == nil {
		submitted, err = host.Submit(ctx, busclient.SubmitInput{Agent: "claude", Selector: []wire.Requirement{{Key: "gpu", Operator: wire.OpExists}},
			Prompt: "hello from the host", Parts: []wire.Part{{Raw: wire.StdBytes("a,b\n"), Filename: "data.csv"}},
			Credentials: []wire.TaskCredential{{Name: "deploy-key", File: "id_ed25519"}}, Outputs: []wire.ExpectedOutput{{Name: "big.bin", Required: true}},
			Deadline: time.Minute, QueueDeadline: time.Minute})
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("submit never placed: %v", err)
	}
	if submitted.Pool != "pool-a" {
		t.Fatalf("placed in %q, want pool-a", submitted.Pool)
	}
	// Await answers the receiver's seal request; nothing else seals.
	awaited, err := host.Await(ctx, submitted.RunID, 20*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if awaited.Status.State != wire.TaskStateCompleted || awaited.Task == nil || len(awaited.Task.Artifacts) != 1 || awaited.Task.Artifacts[0].Parts[0].Text != "done" {
		t.Fatalf("await = %+v; want the receiver's completed result", awaited)
	}
	// The plugin's outputs cross the link in pieces and the feed in chunks the receiver signs; the host joins and verifies them.
	if len(awaited.Outputs) != 2 || awaited.Outputs[0].Name != "big.bin" || !bytes.Equal(awaited.Outputs[0].Data, bytes.Repeat([]byte("b"), 2<<20+1)) ||
		awaited.Outputs[1].Name != "empty" || len(awaited.Rejected) != 0 ||
		len(awaited.NotReturned) != 1 || awaited.NotReturned[0] != (wire.UnreturnedOutput{Name: "link", Size: 4, Reason: "rejected: symlink"}) {
		t.Fatalf("outputs %d, not returned %+v, rejected %+v; want big.bin and empty verified and the link refused", len(awaited.Outputs), awaited.NotReturned, awaited.Rejected)
	}
	otherConfig := busclient.Config{Bundle: otherBundle, HostKey: filepath.Join(dir, "other.key"), StateDir: filepath.Join(dir, "other-state")}
	if _, err := busclient.Init(ctx, otherConfig); err != nil {
		t.Fatal(err)
	}
	other, err := busclient.Connect(otherConfig, log.New(hostLogs, "other: ", 0))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if seen, err := other.Await(ctx, submitted.RunID, 20*time.Second); err != nil || seen.Status.State != wire.TaskStateCompleted {
		t.Fatalf("another host of the tenant: await = %+v, %v; want the run found through its echo", seen, err)
	}
	var runs []*adapter.Run
	var configured adapter.Settings
	for _, record := range read() {
		switch record.Call {
		case "dispatch":
			runs = append(runs, record.Run)
		case "configure":
			configured = record.Settings
		}
	}
	if len(runs) != 1 || runs[0].ID != submitted.RunID || string(runs[0].Credential) != credentialText || string(runs[0].Prompt) == "" ||
		len(runs[0].Inputs) != 1 || runs[0].Inputs[0].Name != "data.csv" || string(runs[0].Inputs[0].Data) != "a,b\n" ||
		len(runs[0].Credentials) != 1 || string(runs[0].Credentials[0].Value) != deployText || runs[0].Credentials[0].File != "id_ed25519" ||
		!strings.Contains(string(runs[0].Prompt), "Input files: /fake/inputs/data.csv.") {
		t.Fatalf("plugin runs %+v; want one dispatch with the host's sealed credentials and the input", runs)
	}
	if configured.Platform != platformText || configured.Pool != "pool-a" || configured.Agent != "claude" || configured.Credential != "model" {
		t.Fatalf("plugin configured with %+v; want the bundle's platform credential and the adapter's settings", configured)
	}
	// A plugin crash: the receiver restarts it and configures its adapter again, platform credential included.
	if err := syscall.Kill(read()[0].PID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	for !strings.Contains(logs.String(), "plugin restarted") {
		if ctx.Err() != nil {
			t.Fatalf("plugin never restarted; logs:\n%s", logs.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
	for _, secret := range []string{credentialText, platformText, deployText} {
		if strings.Contains(logs.String(), secret) {
			t.Fatalf("receiver or plugin logs carry a credential:\n%s", logs.String())
		}
		if strings.Contains(hostLogs.String(), secret) {
			t.Fatalf("host logs carry a credential:\n%s", hostLogs.String())
		}
	}
	if !strings.Contains(hostLogs.String(), "host: run "+submitted.RunID+": sealed [model deploy-key] to receiver-a for attempt 1") {
		t.Fatalf("the host did not answer the seal request; logs:\n%s", hostLogs.String())
	}

	if _, err := host.Submit(ctx, busclient.SubmitInput{Agent: "claude", Selector: []wire.Requirement{{Key: "gpu", Operator: wire.OpDoesNotExist}},
		Prompt: "nowhere to run", Deadline: time.Minute, QueueDeadline: time.Minute}); err == nil {
		t.Fatal("placed a task no adapter's labels satisfy")
	}
	inlineCapRoundTrip(t, ctx, bucket, host, read, logs, hostLogs)
	if bucket.store != nil {
		s3RoundTrip(t, ctx, bucket, host, read, logs, hostLogs)
	}
}

func inlineCapRoundTrip(t *testing.T, ctx context.Context, bucket *s3Store, host *busclient.Host, read func() []pluginRecord, logs, hostLogs *syncBuffer) {
	input := bytes.Repeat([]byte("dataset "), 250)
	submitted, err := host.Submit(ctx, busclient.SubmitInput{Agent: "claude", Selector: []wire.Requirement{{Key: "gpu", Operator: wire.OpExists}}, Prompt: "use the dataset",
		Parts: []wire.Part{{Raw: input, Filename: "dataset.bin"}}, Deadline: time.Minute, QueueDeadline: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	awaited, err := host.Await(ctx, submitted.RunID, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if awaited.Status.State != wire.TaskStateCompleted {
		t.Fatalf("await = %+v; want completed; logs:\n%s\n%s", awaited, logs.String(), hostLogs.String())
	}
	var pluginRun *adapter.Run
	for _, record := range read() {
		if record.Call == "dispatch" && record.Run.ID == submitted.RunID {
			pluginRun = record.Run
		}
	}
	if pluginRun == nil || len(pluginRun.Fetches) != 1 || pluginRun.Fetches[0].Name != "dataset.bin" || pluginRun.Fetches[0].Size != int64(len(input)) ||
		pluginRun.Fetches[0].SHA256 != hexSHA(input) || !strings.Contains(string(pluginRun.Prompt), "Input files: /fake/inputs/dataset.bin.") {
		t.Fatalf("plugin run %+v does not receive dataset.bin as the agent's input", pluginRun)
	}
	response, err := bucket.client.Get(pluginRun.Fetches[0].URL)
	if err != nil {
		t.Fatal(err)
	}
	fetched, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if !bytes.Equal(fetched, input) {
		t.Fatalf("the fetch's URL served %d bytes (%s); want the input", len(fetched), response.Status)
	}
}

// s3RoundTrip runs a turn with a 5 MiB S3 input and the plugin's 2 MiB output returned through the bucket.
func s3RoundTrip(t *testing.T, ctx context.Context, bucket *s3Store, host *busclient.Host, read func() []pluginRecord, logs, hostLogs *syncBuffer) {
	input := bytes.Repeat([]byte("dataset "), 5<<20/8)
	path := filepath.Join(t.TempDir(), "dataset.bin")
	if err := os.WriteFile(path, input, 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	submitted, err := host.Submit(ctx, busclient.SubmitInput{Agent: "claude", Selector: []wire.Requirement{{Key: "gpu", Operator: wire.OpExists}}, Prompt: "use the dataset",
		S3Inputs: []busclient.S3File{{Name: "dataset.bin", Body: file}}, Outputs: []wire.ExpectedOutput{{Name: "big.bin", Required: true, Storage: wire.StorageS3}},
		Deadline: time.Minute, QueueDeadline: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	awaited, err := host.Await(ctx, submitted.RunID, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if awaited.Status.State != wire.TaskStateCompleted || len(awaited.Stored) != 1 || awaited.Stored[0].Key != wire.S3OutputKey("tenant-a", submitted.RunID, "big.bin") {
		t.Fatalf("await = %+v; want the S3 output listed; logs:\n%s\n%s", awaited, logs.String(), hostLogs.String())
	}
	dir := t.TempDir()
	downloaded, rejected, err := host.DownloadOutputs(ctx, submitted.RunID, awaited, dir)
	if err != nil || len(rejected) != 0 || len(downloaded) != 1 {
		t.Fatalf("downloaded %+v, rejected %+v, %v", downloaded, rejected, err)
	}
	if written, err := os.ReadFile(filepath.Join(dir, "big.bin")); err != nil || !bytes.Equal(written, bytes.Repeat([]byte("b"), 2<<20+1)) {
		t.Fatalf("downloaded %d bytes, %v", len(written), err)
	}
	for object := range bucket.store.ListObjects(ctx, s3Bucket, minio.ListObjectsOptions{Prefix: wire.S3Prefix("tenant-a", submitted.RunID), Recursive: true}) {
		t.Fatalf("%s survived the run's cleanup", object.Key)
	}
	for _, text := range []string{logs.String(), hostLogs.String()} {
		if strings.Contains(text, "X-Amz-Signature") {
			t.Fatalf("logs hold a presigned URL:\n%s", text)
		}
	}
}

const s3Bucket = "wherry-test"

// s3Store is the TLS S3 boundary used by the round trip; store is set for the optional real MinIO oracle.
type s3Store struct {
	endpoint string
	config   *busclient.StorageConfig
	store    *minio.Client
	client   *http.Client
}

// s3Oracle starts the proxy and trusts it for the test: the receiver's uploads take Go's default transport, and the
// client's minio-go reads SSL_CERT_FILE. Nil without WHERRY_S3_TEST_ENDPOINT.
func s3Oracle(t *testing.T) *s3Store {
	t.Helper()
	target := os.Getenv("WHERRY_S3_TEST_ENDPOINT")
	if target == "" {
		t.Log("WHERRY_S3_TEST_ENDPOINT not set: no real S3 leg")
		return nil
	}
	config := &busclient.StorageConfig{AccessKeyFile: os.Getenv("WHERRY_S3_TEST_ACCESS_KEY_FILE"), SecretKeyFile: os.Getenv("WHERRY_S3_TEST_SECRET_KEY_FILE")}
	keys := make([]string, 2)
	for i, path := range []string{config.AccessKeyFile, config.SecretKeyFile} {
		key, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		keys[i] = strings.TrimSpace(string(key))
	}
	parsed, err := url.Parse(target)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httptest.NewTLSServer(&httputil.ReverseProxy{Rewrite: func(r *httputil.ProxyRequest) { r.SetURL(parsed); r.Out.Host = r.In.Host }})
	t.Cleanup(proxy.Close)
	previous := http.DefaultTransport
	http.DefaultTransport = proxy.Client().Transport
	t.Cleanup(func() { http.DefaultTransport = previous })
	ca := filepath.Join(t.TempDir(), "proxy.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: proxy.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SSL_CERT_FILE", ca)
	store, err := minio.New(parsed.Host, &minio.Options{Creds: credentials.NewStaticV4(keys[0], keys[1], ""), BucketLookup: minio.BucketLookupPath})
	if err != nil {
		t.Fatal(err)
	}
	if exists, err := store.BucketExists(context.Background(), s3Bucket); err != nil {
		t.Fatal(err)
	} else if !exists {
		if err := store.MakeBucket(context.Background(), s3Bucket, minio.MakeBucketOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	return &s3Store{endpoint: proxy.URL, config: config, store: store, client: proxy.Client()}
}

func fallbackS3(t *testing.T) *s3Store {
	t.Helper()
	var mu sync.Mutex
	objects := map[string][]byte{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Has("location") {
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprint(w, "<LocationConstraint></LocationConstraint>")
			return
		}
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodPut:
			objects[r.URL.Path], _ = io.ReadAll(r.Body)
			w.Header().Set("ETag", `"test"`)
		case http.MethodGet:
			data, ok := objects[r.URL.Path]
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Write(data)
		default:
			http.Error(w, "method", http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)
	previous := http.DefaultTransport
	http.DefaultTransport = server.Client().Transport
	t.Cleanup(func() { http.DefaultTransport = previous })
	dir := t.TempDir()
	ca := filepath.Join(dir, "s3-ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SSL_CERT_FILE", ca)
	access, secret := filepath.Join(dir, "access"), filepath.Join(dir, "secret")
	if err := os.WriteFile(access, []byte("access"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secret, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	return &s3Store{endpoint: server.URL, config: &busclient.StorageConfig{AccessKeyFile: access, SecretKeyFile: secret}, client: server.Client()}
}

func hexSHA(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
