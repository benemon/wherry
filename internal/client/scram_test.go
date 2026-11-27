package client

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"log"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/trust"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// hostFiles is a TLS kfake with tenant-a's host-a added and its key registered, and the host's configuration.
type hostFiles struct {
	cluster *kfake.Cluster
	service *operator.Service
	config  Config
	kept    string
}

func newHostFiles(t *testing.T) hostFiles {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "scram"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, _ := x509.ParseCertificate(der)
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	cluster := kfake.MustCluster(kfake.TLS(&tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}), kfake.EnableSASL(), kfake.EnableACLs(),
		kfake.Superuser("SCRAM-SHA-512", "admin", "password"), kfake.Superuser("SCRAM-SHA-512", "wherry.operator", "operator-password"))
	t.Cleanup(cluster.Close)
	address := cluster.ListenAddrs()[0]
	ctx := context.Background()
	adminOpts, _ := operator.KafkaOpts(address, "admin", "password", roots)
	admin, err := kgo.NewClient(adminOpts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	if err := operator.Delegate(ctx, admin, "operator-password"); err != nil {
		t.Fatal(err)
	}
	operatorOpts, _ := operator.KafkaOpts(address, "wherry.operator", "operator-password", roots)
	operatorClient, err := kgo.NewClient(operatorOpts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operatorClient.Close)
	service := &operator.Service{Operator: operatorClient, Broker: address, Roots: roots, DeliveryCeiling: 10, LockCeilingMS: 60000}
	if err := service.TenantAdd(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	reauth := "1"
	if responses, err := kadm.NewClient(admin).AlterBrokerConfigs(ctx, []kadm.AlterConfig{{Op: kadm.SetConfig, Name: "connections.max.reauth.ms", Value: &reauth}}); err != nil || responses[0].Err != nil {
		t.Fatalf("broker settings: %v %v", err, responses)
	}
	dir := t.TempDir()
	h := hostFiles{cluster: cluster, service: service, kept: filepath.Join(t.TempDir(), "kept.bundle"),
		config: Config{Bundle: filepath.Join(dir, "bundle.yaml"), HostKey: filepath.Join(dir, "host.key"), StateDir: filepath.Join(dir, "state"), Roots: roots}}
	if err := service.HostAdd(ctx, "tenant-a", "host-a", h.kept); err != nil {
		t.Fatal(err)
	}
	h.install(t)
	if _, err := trust.GenerateSigningKeyFile(h.config.HostKey); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Init(ctx, h.config); err != nil {
		t.Fatal(err)
	}
	return h
}

// install copies the operator's kept bundle to the host's, as host init of a new enrolment does.
func (h hostFiles) install(t *testing.T) {
	t.Helper()
	encoded, err := os.ReadFile(h.kept)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(h.config.Bundle, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := RemoveSCRAMNext(h.config.Bundle); err != nil {
		t.Fatal(err)
	}
}

// process is a host process on the same bundle with a state directory of its own.
func (h hostFiles) process(t *testing.T) *Host {
	t.Helper()
	config := h.config
	config.StateDir = t.TempDir()
	host, err := Connect(config, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(host.Close)
	return host
}

func (h hostFiles) next(t *testing.T) string {
	t.Helper()
	next, err := readSCRAMNext(h.config.Bundle)
	if err != nil {
		t.Fatal(err)
	}
	if next.Request == "" {
		return ""
	}
	request, _, err := operator.DecodeCredentialRequest([]byte(next.Request))
	if err != nil {
		t.Fatal(err)
	}
	return request.ID
}

func (h hostFiles) requests(t *testing.T) string {
	t.Helper()
	var out bytes.Buffer
	if err := h.service.ListRequests(context.Background(), time.Now(), &out); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

// pauseAt blocks the next host row to reach step until the returned function is called, and returns once it is there.
func pauseAt(t *testing.T, step string) (reached chan struct{}, resume func()) {
	t.Helper()
	reached, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	scramStep = func(at string) {
		if at == step {
			paused := false
			once.Do(func() { paused = true })
			if paused {
				close(reached)
				<-release
			}
		}
	}
	t.Cleanup(func() { scramStep = func(string) {} })
	return reached, func() { close(release) }
}

// Test 34: a second process files nothing while the first is between its read and its write, and then sends the
// first's ID; processes with different state directories share the lock; the lock file stays.
func TestHostLockOrdersTwoProcessesFiling(t *testing.T) {
	h := newHostFiles(t)
	first, second := h.process(t), h.process(t)
	reached, resume := pauseAt(t, "read")
	done := make(chan error, 2)
	go func() { done <- first.RotateSCRAM(context.Background(), io.Discard) }()
	<-reached
	go func() { done <- second.RotateSCRAM(context.Background(), io.Discard) }()
	time.Sleep(100 * time.Millisecond)
	if h.next(t) != "" {
		t.Fatal("a second process wrote scram.next while the first held the host lock")
	}
	resume()
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	id := h.next(t)
	if out := h.requests(t); strings.Count(out, "request=") != 1 || !strings.Contains(out, "request="+id) {
		t.Fatalf("two processes filed %q; want one request, %s", out, id)
	}
	if _, err := os.Stat(ScramLockPath(h.config.Bundle)); err != nil {
		t.Fatalf("scram.lock is gone: %v", err)
	}
}

// Test 34: a process that read scram.next void does not remove one written since.
func TestHostLockKeepsARequestFiledAfterAVoidRead(t *testing.T) {
	h := newHostFiles(t)
	first, second := h.process(t), h.process(t)
	if err := first.RotateSCRAM(context.Background(), io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := h.service.RejectRequest(context.Background(), h.next(t), io.Discard); err != nil {
		t.Fatal(err)
	}
	reached, resume := pauseAt(t, "read")
	done := make(chan error, 2)
	go func() { done <- first.syncSCRAM(context.Background(), false, io.Discard) }()
	<-reached
	go func() { done <- second.RotateSCRAM(context.Background(), io.Discard) }()
	time.Sleep(100 * time.Millisecond)
	resume()
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if h.next(t) == "" {
		t.Fatal("a process that read scram.next void removed the request another filed since")
	}
}

// Test 34: an approval, a promotion and a new request by another process that land before a row takes the lock are what
// the row reads, so it does not remove that request.
func TestHostRowReadsUnderTheLock(t *testing.T) {
	h := newHostFiles(t)
	// Sessions outlive the test, so every process stays logged in across the approval.
	reauth := "60000"
	if responses, err := kadm.NewClient(h.service.Operator).AlterBrokerConfigs(context.Background(), []kadm.AlterConfig{{Op: kadm.SetConfig, Name: "connections.max.reauth.ms", Value: &reauth}}); err != nil || responses[0].Err != nil {
		t.Fatalf("broker settings: %v %v", err, responses)
	}
	first, other := h.process(t), h.process(t)
	reached, resume := pauseAt(t, "lock")
	done := make(chan error, 1)
	go func() { done <- first.syncSCRAM(context.Background(), false, io.Discard) }()
	<-reached
	scramStep = func(string) {}
	if err := other.RotateSCRAM(context.Background(), io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := h.service.ApproveRequests(context.Background(), h.next(t), false, time.Now(), io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := other.syncSCRAM(context.Background(), false, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := other.RotateSCRAM(context.Background(), io.Discard); err != nil {
		t.Fatal(err)
	}
	filed := h.next(t)
	resume()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if h.next(t) != filed {
		t.Fatal("a row acted on a read of wherry.operator made before it took the lock")
	}
}

// Test 34: a row whose read of wherry.operator hangs fails with G-2's error, changes neither file and lets a waiting
// command proceed.
func TestHostRowReadIsBounded(t *testing.T) {
	h := newHostFiles(t)
	first, second := h.process(t), h.process(t)
	timeout := pingTimeout
	pingTimeout = 500 * time.Millisecond
	t.Cleanup(func() { pingTimeout = timeout })
	release := make(chan struct{})
	h.cluster.ControlKey(int16(kmsg.Fetch), func(request kmsg.Request) (kmsg.Response, error, bool) {
		h.cluster.KeepControl()
		select {
		case <-release:
		default:
			h.cluster.SleepControl(func() { <-release })
		}
		return nil, nil, false
	})
	bundle, _ := os.ReadFile(h.config.Bundle)
	done := make(chan error, 1)
	go func() { done <- second.RotateSCRAM(context.Background(), io.Discard) }()
	err := first.RotateSCRAM(context.Background(), io.Discard)
	if err == nil || err.Error() != "could not read wherry.operator within 500ms; nothing was changed" {
		t.Fatalf("a row whose read hangs = %v", err)
	}
	close(release)
	if err := <-done; err != nil && !strings.Contains(err.Error(), "could not read wherry.operator") {
		t.Fatal(err)
	}
	if after, _ := os.ReadFile(h.config.Bundle); !bytes.Equal(after, bundle) {
		t.Fatal("a row whose read failed changed the bundle")
	}
}

// Test 38: T-H0 with another process promoting and removing scram.next between its two reads connects.
func TestConnectReadsSCRAMNextBeforeTheBundle(t *testing.T) {
	h := newHostFiles(t)
	writer := h.process(t)
	if err := writer.RotateSCRAM(context.Background(), io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := h.service.ApproveRequests(context.Background(), h.next(t), false, time.Now(), io.Discard); err != nil {
		t.Fatal(err)
	}
	// Another process's T-H3 and T-H4: the bundle takes scram.next's password, then scram.next is removed.
	betweenConnectReads = func() {
		betweenConnectReads = func() {}
		next, err := readSCRAMNext(h.config.Bundle)
		bundle, bundleErr := operator.ReadBundle(h.config.Bundle)
		if err != nil || bundleErr != nil {
			t.Error(err, bundleErr)
			return
		}
		bundle.Password = next.Password
		if err := operator.ReplaceBundle(h.config.Bundle, bundle); err != nil {
			t.Error(err)
		}
		if err := RemoveSCRAMNext(h.config.Bundle); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(func() { betweenConnectReads = func() {} })
	if h.next(t) == "" {
		t.Fatal("the test needs an approved scram.next")
	}
	config := h.config
	config.StateDir = t.TempDir()
	host, err := Connect(config, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatalf("connect with a promotion between its reads: %v", err)
	}
	host.Close()
	if h.next(t) != "" {
		t.Fatal("the promotion between the reads did not run")
	}
}

// G-1 and G-2: a lock file that is already there with a wider mode is made 0600 by the command that takes it.
func TestLockFileIsMadePrivate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "operator.lock")
	if err := os.WriteFile(path, []byte("holder"), 0o644); err != nil {
		t.Fatal(err)
	}
	release, err := LockFile(context.Background(), path, nil)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("lock file mode %v, %v; want 0600", info.Mode().Perm(), err)
	}
}

// Test 12: a promotion stopped between its two file writes leaves the approved password in the bundle or in
// scram.next, never in neither.
func TestPromotionStoppedBetweenItsWritesKeepsThePassword(t *testing.T) {
	h := newHostFiles(t)
	// A session long enough to outlive the upsert, so the writer still reads wherry.operator after it.
	reauth := "60000"
	if responses, err := kadm.NewClient(h.service.Operator).AlterBrokerConfigs(context.Background(), []kadm.AlterConfig{{Op: kadm.SetConfig, Name: "connections.max.reauth.ms", Value: &reauth}}); err != nil || responses[0].Err != nil {
		t.Fatalf("broker settings: %v %v", err, responses)
	}
	writer := h.process(t)
	if err := writer.RotateSCRAM(context.Background(), io.Discard); err != nil {
		t.Fatal(err)
	}
	approved, err := readSCRAMNext(h.config.Bundle)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.service.ApproveRequests(context.Background(), h.next(t), false, time.Now(), io.Discard); err != nil {
		t.Fatal(err)
	}
	stopped := false
	scramStep = func(at string) {
		if at != "promoted" {
			return
		}
		stopped = true
		next, _ := readSCRAMNext(h.config.Bundle)
		bundle, _ := operator.ReadBundle(h.config.Bundle)
		if bundle.Password != approved.Password && next.Password != approved.Password {
			t.Error("between the promotion's writes neither file holds the approved password")
		}
	}
	t.Cleanup(func() { scramStep = func(string) {} })
	if err := writer.syncSCRAM(context.Background(), false, io.Discard); err != nil {
		t.Fatal(err)
	}
	if !stopped {
		t.Fatal("the promotion did not run")
	}
}
