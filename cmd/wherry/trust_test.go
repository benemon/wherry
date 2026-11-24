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
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/twmb/franz-go/pkg/kfake"
)

// otherCA is a self-signed CA certificate, as PEM, that signed nothing any test broker presents.
func otherCA(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "other"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

// editBundle rewrites the bundle at path with edit applied.
func editBundle(t *testing.T, path string, edit func(*operator.Bundle)) {
	t.Helper()
	bundle, err := operator.ReadBundle(path)
	if err != nil {
		t.Fatal(err)
	}
	edit(&bundle)
	if err := operator.ReplaceBundle(path, bundle); err != nil {
		t.Fatal(err)
	}
}

// writeFile writes contents to a new file named name in a temporary directory and returns its path.
func writeFile(t *testing.T, name, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// unknownAuthority reports whether err is a connection the broker's certificate failed for chaining to no trusted CA.
func unknownAuthority(err error) bool {
	var unknown x509.UnknownAuthorityError
	return errors.As(err, &unknown)
}

// untrustedBroker is kfake behind TLS with a self-signed certificate the process's system roots do not trust, with
// wherry.operator among its super users.
func untrustedBroker(t *testing.T) *kfake.Cluster {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(3), Subject: pkix.Name{CommonName: "untrusted"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
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
	pair, err := tls.X509KeyPair(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	if err != nil {
		t.Fatal(err)
	}
	cluster := kfake.MustCluster(kfake.TLS(&tls.Config{Certificates: []tls.Certificate{pair}}), kfake.EnableSASL(), kfake.EnableACLs(),
		kfake.Superuser("SCRAM-SHA-512", "wherry.operator", "operator-password"))
	t.Cleanup(cluster.Close)
	return cluster
}

// unknownAuthorityText is the x509 error a broker certificate the connection does not trust fails with, kept at the
// end of its explanation; unknownAuthorityHint is the explanation's one remedy, which precedes it.
const (
	unknownAuthorityText = "x509: certificate signed by unknown authority"
	unknownAuthorityHint = "cannot verify the broker's certificate: install the CA that signed it in this machine's trust store, or set SSL_CERT_FILE, which replaces the system roots ("
)

// trustHelperVariable names, in a re-run of this binary, the host command the trust helper runs.
const trustHelperVariable = "WHERRY_TEST_TRUST_HELPER"

// explained fails t unless err is an unknown authority explained once, with the one hint, ending with the x509 error,
// with no password or certificate in it.
func explained(t *testing.T, what string, err error) {
	t.Helper()
	if err == nil || !unknownAuthority(err) {
		t.Fatalf("%s = %v; want the connection refused on the certificate", what, err)
	}
	text := err.Error()
	if !strings.HasPrefix(text, unknownAuthorityHint) || !strings.Contains(text, unknownAuthorityText) || !strings.HasSuffix(text, ")") {
		t.Fatalf("%s = %q; want %q with the x509 error at the end", what, text, unknownAuthorityHint)
	}
	if n := strings.Count(text, "cannot verify the broker's certificate"); n != 1 {
		t.Fatalf("%s = %q; want the explanation once, not %d times", what, text, n)
	}
	if strings.Contains(text, "password") || strings.Contains(text, "-----BEGIN") {
		t.Fatalf("%s = %q; want no password or certificate in the error", what, text)
	}
}

// A host configuration that keeps a ca_file is refused by host init and every host command naming the key and the trust
// store, not as an unknown field at a line.
func TestHostRefusesACAFileByName(t *testing.T) {
	h := newInitHost(t, nil)
	noTerminal(t)
	h.init(t, h.bundle)
	config := h.written(t)
	if err := os.WriteFile(h.config, []byte("bundle: "+config.Bundle+"\nca_file: "+writeFile(t, "kafka-ca.pem", testCA.pem)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	want := h.config + ": ca_file is no longer read; remove the line and install the broker's CA in this machine's trust store"
	for _, args := range [][]string{{"host", "init"}, {"host", "init", h.bundle}, {"host", "receivers"}, {"status"}} {
		var out bytes.Buffer
		if err := run(args, &out, &out); err == nil || err.Error() != want {
			t.Fatalf("wherry %s with a ca_file = %v; want %q", strings.Join(args, " "), err, want)
		}
	}
}

// A receiver's local configuration that keeps a ca_file is refused naming the key and the trust store.
func TestReceiverRefusesACAFileByName(t *testing.T) {
	dir := t.TempDir()
	bundle := filepath.Join(dir, "receiver.bundle")
	if err := operator.ReplaceBundle(bundle, operator.Bundle{Broker: "127.0.0.1:1", Principal: operator.ReceiverPrincipal("receiver-a"), Password: "p"}); err != nil {
		t.Fatal(err)
	}
	localConfig := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(localConfig, []byte("bundle: "+bundle+"\nca_file: /etc/wherry/kafka-ca.pem\nstate_dir: "+filepath.Join(dir, "state")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	want := localConfig + ": ca_file is no longer read; remove the line and install the broker's CA in this machine's trust store"
	if err := run([]string{"receiver", "run", "--config-file", localConfig}, &out, &out); err == nil || err.Error() != want {
		t.Fatalf("receiver run with a ca_file = %v; want %q", err, want)
	}
}

// An operator.yaml that keeps a ca_file is refused naming the key and the trust store.
func TestOperatorRefusesACAFileByName(t *testing.T) {
	operatorEnv(t)
	path := filepath.Join(".", "operator.yaml")
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(encoded, []byte("ca_file: kafka-ca.pem\n")...), 0o600); err != nil {
		t.Fatal(err)
	}
	want := path + ": ca_file is no longer read; remove the line and install the broker's CA in this machine's trust store"
	if err := runOperator([]string{"check"}, io.Discard, io.Discard); err == nil || err.Error() != want {
		t.Fatalf("operator check with a ca_file = %v; want %q", err, want)
	}
}

// A broker whose certificate the system roots do not trust fails every dial with the one hint: a host's commands and
// host init, a receiver's run, and the operator's commands and init.
func TestUnknownAuthorityGetsTheOneHint(t *testing.T) {
	untrusted := untrustedBroker(t).ListenAddrs()[0]
	t.Run("host", func(t *testing.T) {
		h := newInitHost(t, nil)
		noTerminal(t)
		h.init(t, h.bundle)
		editBundle(t, filepath.Join(h.dir, "bundle.yaml"), func(bundle *operator.Bundle) { bundle.Broker = untrusted })
		editBundle(t, h.bundle, func(bundle *operator.Bundle) { bundle.Broker = untrusted })
		for _, args := range [][]string{{"host", "receivers"}, {"status"}, {"host", "init", h.bundle}} {
			var out bytes.Buffer
			explained(t, "wherry "+strings.Join(args, " "), run(args, &out, &out))
		}
	})
	t.Run("receiver", func(t *testing.T) {
		dir := t.TempDir()
		bundle := filepath.Join(dir, "receiver.bundle")
		if err := operator.ReplaceBundle(bundle, operator.Bundle{Broker: untrusted, Principal: operator.ReceiverPrincipal("receiver-a"), Password: "p"}); err != nil {
			t.Fatal(err)
		}
		localConfig := filepath.Join(dir, "config.yaml")
		if err := os.WriteFile(localConfig, []byte("bundle: "+bundle+"\nstate_dir: "+filepath.Join(dir, "state")+"\nplugin_dir: "+t.TempDir()+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		explained(t, "receiver run", run([]string{"receiver", "run", "--config-file", localConfig}, &out, &out))
	})
	t.Run("operator", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "operator.password"), []byte("operator-password\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := writeYAML(filepath.Join(dir, "operator.yaml"), operatorConfig{Broker: untrusted, PasswordFile: "operator.password"}); err != nil {
			t.Fatal(err)
		}
		explained(t, "operator check", runOperator([]string{"check", "--dir", dir}, io.Discard, io.Discard))
		noTerminal(t)
		init := filepath.Join(t.TempDir(), "operator")
		explained(t, "operator init", runOperator([]string{"init", "--dir", init, "--broker", untrusted, "--super-user", "wherry.operator", "--super-user-password-file", filepath.Join(dir, "operator.password")}, io.Discard, io.Discard))
		if _, err := os.Stat(init); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("a refused init wrote %s: %v", init, err)
		}
	})
}

// A host trusts the broker through the system roots alone, which SSL_CERT_FILE replaces: in a process whose
// SSL_CERT_FILE names a CA that did not sign the broker's certificate, host receivers fails with the one hint; in one
// whose SSL_CERT_FILE names the broker's CA, it succeeds. Go reads the roots once per process, so each case is this
// binary re-run.
func TestSSLCertFileReplacesTheSystemRoots(t *testing.T) {
	h := newInitHost(t, nil)
	noTerminal(t)
	h.init(t, h.bundle)
	for name, c := range map[string]struct{ file, want string }{
		"another CA":      {writeFile(t, "other-ca.pem", otherCA(t)), "ERR: " + unknownAuthorityHint},
		"the broker's CA": {testCA.file, "OK\n"},
	} {
		command := exec.Command(os.Args[0], "-test.run=^TestTrustHelperProcess$", "-test.timeout=2m")
		command.Env = append(os.Environ(), trustHelperVariable+"=host receivers", "SSL_CERT_FILE="+c.file)
		output, err := command.CombinedOutput()
		if !strings.Contains(string(output), c.want) {
			t.Fatalf("%s: helper printed %q, %v; want %q", name, output, err, c.want)
		}
	}
}

// TestTrustHelperProcess runs the host command trustHelperVariable names, in a process whose SSL_CERT_FILE the test
// that re-ran this binary chose, and prints OK or the error.
func TestTrustHelperProcess(t *testing.T) {
	command := os.Getenv(trustHelperVariable)
	if command == "" {
		t.Skip("not a trust helper")
	}
	var out bytes.Buffer
	if err := run(strings.Fields(command), &out, &out); err != nil {
		fmt.Println("ERR: " + err.Error())
		return
	}
	fmt.Println("OK")
}

// A kept bundle that carries the ca earlier operators wrote is refused by tenant host refresh and receiver apply
// naming --force, and --force reissues it without ca; the bundle a refusal leaves is untouched.
func TestKeptBundleWithACAIsReissuedWithForce(t *testing.T) {
	service := operatorEnv(t)
	ctx := context.Background()
	terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude"}}, DefaultAgent: "claude", DeliveryLimit: 5, LockDurationMS: 15000}
	if err := service.PoolApply(ctx, "pool-a", terms, false); err != nil {
		t.Fatal(err)
	}
	localFixture(t, ".", "receiver-a")
	if err := os.MkdirAll("receivers", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join("receivers", "receiver-a.yaml"), []byte("pool: pool-a\nplugin_sha256: {nomad: "+fakeSHA256+"}\n"+basePlugin+item("name: claude\nagent: claude\n", "      ")), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"tenant", "add", "tenant-a"}, {"tenant", "host", "add", "tenant-a", "host-a"}, {"receiver", "apply", "receiver-a"}} {
		if err := runOperator(args, io.Discard, io.Discard); err != nil {
			t.Fatal(err)
		}
	}
	// withCA rewrites the bundle at path as an earlier operator wrote it, the ca after the password.
	withCA := func(path string) []byte {
		t.Helper()
		encoded, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		kept := append(encoded, []byte("ca: |\n  "+strings.ReplaceAll(strings.TrimSpace(testCA.pem), "\n", "\n  ")+"\n")...)
		if err := os.WriteFile(path, kept, 0o600); err != nil {
			t.Fatal(err)
		}
		return kept
	}
	for _, c := range []struct {
		name, path string
		refresh    []string
		force      []string
	}{
		{"host", filepath.Join("bundles", "hosts", "tenant-a", "host-a.bundle"), []string{"tenant", "host", "refresh", "tenant-a", "host-a"}, []string{"tenant", "host", "add", "tenant-a", "host-a", "--force"}},
		{"receiver", filepath.Join("bundles", "receivers", "receiver-a.bundle"), []string{"receiver", "apply", "receiver-a"}, []string{"receiver", "apply", "receiver-a", "--force"}},
	} {
		before, err := operator.ReadBundle(c.path)
		if err != nil {
			t.Fatal(err)
		}
		kept := withCA(c.path)
		err = runOperator(c.refresh, io.Discard, io.Discard)
		if err == nil || !errors.Is(err, operator.ErrBundleCA) || !strings.Contains(err.Error(), c.path+": ca is no longer read; reissue this bundle") || !strings.Contains(err.Error(), "--force") {
			t.Fatalf("%s refresh with a kept bundle carrying a ca = %v; want the key and --force named", c.name, err)
		}
		if after, err := os.ReadFile(c.path); err != nil || !bytes.Equal(after, kept) {
			t.Fatalf("%s: a refused refresh rewrote %s", c.name, c.path)
		}
		if err := runOperator(c.force, io.Discard, io.Discard); err != nil {
			t.Fatalf("%s --force: %v", c.name, err)
		}
		reissued, err := operator.ReadBundle(c.path)
		if err != nil || reissued.Broker != before.Broker || reissued.Principal != before.Principal || reissued.Password == before.Password {
			t.Fatalf("%s reissued bundle %+v, %v; want %s's broker and principal with a new password", c.name, reissued, err, before.Principal)
		}
		if encoded, err := os.ReadFile(c.path); err != nil || strings.Contains(string(encoded), "ca:") {
			t.Fatalf("%s reissued bundle holds\n%s, %v; want no ca", c.name, encoded, err)
		}
	}
}
