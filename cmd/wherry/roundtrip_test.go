package main

import (
	"bytes"
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
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"gopkg.in/yaml.v3"
)

// platformText is the receiver bundle's platform credential for the test plugin's adapter.
const platformText = "s3cr3t-platform-credential"

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

// tlsCluster is kfake behind TLS with a throwaway CA, so bundles carry a real CA and principals log in as they
// would on the lab.
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
		SealLifetimeMS: int64(time.Hour / time.Millisecond), DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
	hostBundle, otherBundle, receiverBundle := filepath.Join(dir, "host.yaml"), filepath.Join(dir, "other.yaml"), filepath.Join(dir, "receiver.yaml")
	config := admin.ReceiverConfig{Receiver: "receiver-a", Pool: "pool-a", Capacity: 2,
		Adapters: []admin.AdapterSettings{{Name: "fake", Type: "fake", Agent: "claude", Credential: "model", Labels: map[string]string{"gpu": "yes"}, Config: map[string]any{}}}}
	for _, step := range []func() error{
		func() error { return service.PoolCreate(ctx, "pool-a", terms) },
		func() error { return service.TenantAdd(ctx, "tenant-a", "pool-a") },
		func() error { return service.HostAdd(ctx, "tenant-a", "host-a", hostBundle) },
		func() error { return service.HostAdd(ctx, "tenant-a", "host-b", otherBundle) },
		func() error {
			return service.ReceiverAdd(ctx, config, map[string]string{"fake": platformText}, receiverBundle)
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
		r, err := receiver.New(bundle, stateDir, func(pool string, settings admin.AdapterSettings) (receiver.Named, error) {
			return plugins.adapter(pool, settings, bundle.Platform[settings.Name])
		}, logger)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	missing := &plugins{dir: t.TempDir(), logs: logs, running: map[string]*adapter.Plugin{}}
	if err := newReceiver(missing).Run(ctx); err == nil || err.Error() != "adapter fake: no plugin for type fake" {
		t.Fatalf("receiver without its plugin: %v; want a configuration error", err)
	}
	read := recordPlugin(t)
	plugins := &plugins{dir: pluginDir, logs: logs, running: map[string]*adapter.Plugin{}}
	defer plugins.close()
	r := newReceiver(plugins)
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go func() { _ = r.Run(runCtx) }()

	credentialPath := filepath.Join(dir, "model")
	if err := os.WriteFile(credentialPath, []byte(credentialText), 0o600); err != nil {
		t.Fatal(err)
	}
	hostConfig := busclient.Config{Bundle: hostBundle, HostKey: filepath.Join(dir, "host.key"), StateDir: filepath.Join(dir, "host-state"),
		Credentials: map[string]string{"model": credentialPath}}
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
			Prompt: "hello from the host", Deadline: time.Minute, QueueDeadline: time.Minute})
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
	if len(runs) != 1 || runs[0].ID != submitted.RunID || string(runs[0].Credential) != credentialText || string(runs[0].Prompt) == "" {
		t.Fatalf("plugin runs %+v; want one dispatch with the host's sealed credential", runs)
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
	for _, secret := range []string{credentialText, platformText} {
		if strings.Contains(logs.String(), secret) {
			t.Fatalf("receiver or plugin logs carry a credential:\n%s", logs.String())
		}
		if strings.Contains(hostLogs.String(), secret) {
			t.Fatalf("host logs carry a credential:\n%s", hostLogs.String())
		}
	}
	if !strings.Contains(hostLogs.String(), "host: run "+submitted.RunID+": sealed [model] to receiver-a for turn 1 attempt 1") {
		t.Fatalf("the host did not answer the seal request; logs:\n%s", hostLogs.String())
	}

	if _, err := host.Submit(ctx, busclient.SubmitInput{Agent: "claude", Selector: []wire.Requirement{{Key: "gpu", Operator: wire.OpDoesNotExist}},
		Prompt: "nowhere to run", Deadline: time.Minute, QueueDeadline: time.Minute}); err == nil {
		t.Fatal("placed a task no adapter's labels satisfy")
	}
}
