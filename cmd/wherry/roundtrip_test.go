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
	"io"
	"log"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

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

// platform stands in for a platform behind the adapter seam; everything above it is production code.
type platform struct {
	mu   sync.Mutex
	runs []receiver.Run
}

func (p *platform) Ready(context.Context) (bool, error) { return true, nil }

func (p *platform) Dispatch(_ context.Context, run receiver.Run) (receiver.Outcome, receiver.Handle, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.runs = append(p.runs, run)
	return receiver.OutcomeCreated, receiver.Handle{Data: []byte(run.ID), Name: run.ID}, nil
}

func (p *platform) Lookup(context.Context, string, int64) (receiver.Job, error) {
	return receiver.Job{}, nil
}

func (p *platform) Follow(ctx context.Context, _ receiver.Handle, _ int64, lines chan<- receiver.Line) (receiver.Terminal, error) {
	var offset int64
	for _, data := range []string{`{"type":"assistant","message":"working"}`, `{"type":"result","result":"done","is_error":false}`} {
		select {
		case lines <- receiver.Line{Offset: offset, Next: offset + int64(len(data)+1), Data: []byte(data)}:
		case <-ctx.Done():
			return receiver.Terminal{}, ctx.Err()
		}
		offset += int64(len(data) + 1)
	}
	<-ctx.Done()
	return receiver.Terminal{}, ctx.Err()
}

func (p *platform) Stop(context.Context, receiver.Handle) error  { return nil }
func (p *platform) Purge(context.Context, string, int64) error   { return nil }
func (p *platform) Release(context.Context, string, int64) error { return nil }
func (p *platform) Sweep(context.Context) error                  { return nil }
func (p *platform) dispatched() []receiver.Run {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]receiver.Run(nil), p.runs...)
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

// TestRoundTrip runs a task from a host's Submit through a receiver to the host's Await, on records each side
// produces for the other: provisioning through wherry admin, bundles, the host and receiver constructors, the real
// claim, admission, sealing, dual write and feed reads.
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
		Adapters: []admin.AdapterSettings{{Name: "fake", Type: "fake", Agent: "claude", Credential: "model", Labels: map[string]string{"gpu": "yes"}}}}
	for _, step := range []func() error{
		func() error { return service.PoolCreate(ctx, "pool-a", terms) },
		func() error { return service.TenantAdd(ctx, "tenant-a", "pool-a") },
		func() error { return service.HostAdd(ctx, "tenant-a", "host-a", hostBundle) },
		func() error { return service.HostAdd(ctx, "tenant-a", "host-b", otherBundle) },
		func() error { return service.ReceiverAdd(ctx, config, nil, receiverBundle) },
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
	backend := &platform{}
	r, err := receiver.New(bundle, filepath.Join(dir, "receiver-state"), func(_ string, settings admin.AdapterSettings) (receiver.Named, error) {
		return receiver.Named{Name: settings.Name, Agent: settings.Agent, Credential: settings.Credential, Labels: settings.Labels, Adapter: backend}, nil
	}, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
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
	host, err := busclient.Connect(hostConfig)
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
	other, err := busclient.Connect(otherConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if seen, err := other.Await(ctx, submitted.RunID, 20*time.Second); err != nil || seen.Status.State != wire.TaskStateCompleted {
		t.Fatalf("another host of the tenant: await = %+v, %v; want the run found through its echo", seen, err)
	}
	runs := backend.dispatched()
	if len(runs) != 1 || runs[0].ID != submitted.RunID || string(runs[0].Credential) != credentialText || string(runs[0].Prompt) == "" {
		t.Fatalf("platform runs %+v; want one dispatch with the host's sealed credential", runs)
	}

	if _, err := host.Submit(ctx, busclient.SubmitInput{Agent: "claude", Selector: []wire.Requirement{{Key: "gpu", Operator: wire.OpDoesNotExist}},
		Prompt: "nowhere to run", Deadline: time.Minute, QueueDeadline: time.Minute}); err == nil {
		t.Fatal("placed a task no adapter's labels satisfy")
	}
}
