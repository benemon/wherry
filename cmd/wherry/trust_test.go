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

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/operator"
	"github.com/creack/pty"
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
// end of its explanation; unknownAuthorityHint is the explanation's one remedy, which precedes it and names the
// configuration file whose ca_cert takes the CA.
const (
	unknownAuthorityText = "x509: certificate signed by unknown authority"
	unknownAuthorityHint = "cannot verify the broker's certificate: install the CA that signed it in this machine's trust store, or set ca_cert in "
)

// trustHelperVariable names, in a re-run of this binary, the host command the trust helper runs.
const trustHelperVariable = "WHERRY_TEST_TRUST_HELPER"

// explained fails t unless err is an unknown authority explained once, with the one hint naming config, ending with the
// x509 error, with neither password nor a certificate in it.
func explained(t *testing.T, what string, err error, config, password string) {
	t.Helper()
	if err == nil || !unknownAuthority(err) {
		t.Fatalf("%s = %v; want the connection refused on the certificate", what, err)
	}
	text := err.Error()
	if !strings.HasPrefix(text, unknownAuthorityHint+config+" (") || !strings.Contains(text, unknownAuthorityText) || !strings.HasSuffix(text, ")") {
		t.Fatalf("%s = %q; want %q with the x509 error at the end", what, text, unknownAuthorityHint+config+" (")
	}
	if n := strings.Count(text, "cannot verify the broker's certificate"); n != 1 {
		t.Fatalf("%s = %q; want the explanation once, not %d times", what, text, n)
	}
	if strings.Contains(text, password) || strings.Contains(text, "-----BEGIN") {
		t.Fatalf("%s = %q; want no password or certificate in the error", what, text)
	}
}

// receiverLocal writes a receiver's local configuration naming bundle, with caCert as its ca_cert when set, in a new
// directory, and returns its path.
func receiverLocal(t *testing.T, bundle, caCert string) string {
	t.Helper()
	dir := t.TempDir()
	text := "bundle: " + bundle + "\nstate_dir: " + filepath.Join(dir, "state") + "\nplugin_dir: " + t.TempDir() + "\n"
	if caCert != "" {
		text += "ca_cert: |\n  " + strings.ReplaceAll(strings.TrimSpace(caCert), "\n", "\n  ") + "\n"
	}
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// operatorDir writes an operator working directory on broker, with caCert as operator.yaml's ca_cert, and returns it.
func operatorDir(t *testing.T, broker, caCert string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "operator.password"), []byte("operator-password\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeYAML(filepath.Join(dir, "operator.yaml"), operatorConfig{Broker: broker, PasswordFile: "operator.password", CACert: caCert}); err != nil {
		t.Fatal(err)
	}
	return dir
}

// A configuration that keeps a ca_file is refused naming ca_cert, not as an unknown field at a line: the host's by host
// init and every host command, the receiver's and the operator's.
func TestCAFileIsRefusedNamingCACert(t *testing.T) {
	t.Run("host", func(t *testing.T) {
		h := newInitHost(t, nil)
		noTerminal(t)
		h.init(t, h.bundle)
		config := h.written(t)
		if err := os.WriteFile(h.config, []byte("bundle: "+config.Bundle+"\nca_file: "+testCA.file+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		want := h.config + ": ca_file is now ca_cert"
		for _, args := range [][]string{{"host", "init"}, {"host", "init", h.bundle}, {"host", "receivers"}, {"status"}} {
			var out bytes.Buffer
			if err := run(args, &out, &out); err == nil || err.Error() != want {
				t.Fatalf("wherry %s with a ca_file = %v; want %q", strings.Join(args, " "), err, want)
			}
		}
	})
	t.Run("receiver", func(t *testing.T) {
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
		want := localConfig + ": ca_file is now ca_cert"
		if err := run([]string{"receiver", "run", "--config-file", localConfig}, &out, &out); err == nil || err.Error() != want {
			t.Fatalf("receiver run with a ca_file = %v; want %q", err, want)
		}
	})
	t.Run("operator", func(t *testing.T) {
		operatorEnv(t)
		path := filepath.Join(".", "operator.yaml")
		encoded, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(encoded, []byte("ca_file: kafka-ca.pem\n")...), 0o600); err != nil {
			t.Fatal(err)
		}
		want := path + ": ca_file is now ca_cert"
		if err := runOperator([]string{"check"}, io.Discard, io.Discard); err == nil || err.Error() != want {
			t.Fatalf("operator check with a ca_file = %v; want %q", err, want)
		}
	})
}

// A ca_cert from which Go appends no certificate, here a value that could be a password pasted in the wrong place, is
// refused in each role's configuration with Go's result alone, naming the key and the file and never the value.
func TestCACertWithoutACertificateIsRefused(t *testing.T) {
	const value = "not-a-certificate-s3cr3t-value"
	refused := func(t *testing.T, what string, err error, config string) {
		t.Helper()
		if want := "ca_cert in " + config + " holds no certificate"; err == nil || err.Error() != want || !errors.Is(err, operator.ErrNoCertificate) {
			t.Fatalf("%s = %v; want %q", what, err, want)
		}
		if strings.Contains(err.Error(), value) {
			t.Fatalf("%s = %q; want the value absent", what, err)
		}
	}
	t.Run("host", func(t *testing.T) {
		h := newInitHost(t, nil)
		noTerminal(t)
		h.init(t, h.bundle)
		config := h.written(t)
		config.CACert = value
		if err := writeYAML(h.config, config); err != nil {
			t.Fatal(err)
		}
		for _, args := range [][]string{{"host", "init"}, {"host", "receivers"}} {
			var out bytes.Buffer
			refused(t, "wherry "+strings.Join(args, " "), run(args, &out, &out), h.config)
		}
	})
	t.Run("receiver", func(t *testing.T) {
		bundle := filepath.Join(t.TempDir(), "receiver.bundle")
		if err := operator.ReplaceBundle(bundle, operator.Bundle{Broker: "127.0.0.1:1", Principal: operator.ReceiverPrincipal("receiver-a"), Password: "p"}); err != nil {
			t.Fatal(err)
		}
		localConfig := receiverLocal(t, bundle, value)
		refused(t, "receiver run", run([]string{"receiver", "run", "--config-file", localConfig}, io.Discard, io.Discard), localConfig)
	})
	t.Run("operator", func(t *testing.T) {
		dir := operatorDir(t, "127.0.0.1:1", value)
		refused(t, "operator check", runOperator([]string{"check", "--dir", dir}, io.Discard, io.Discard), filepath.Join(dir, "operator.yaml"))
	})
}

// A broker whose certificate neither the machine's trust store nor ca_cert trusts fails every dial with the one hint,
// naming the role's configuration file: a host's commands and host init, a receiver's run, and the operator's commands
// and init, whose hint names the operator.yaml it writes.
func TestUnknownAuthorityGetsTheOneHint(t *testing.T) {
	untrusted := untrustedBroker(t).ListenAddrs()[0]
	t.Run("host", func(t *testing.T) {
		h := newInitHost(t, nil)
		noTerminal(t)
		h.init(t, h.bundle)
		bundle, err := operator.ReadBundle(h.bundle)
		if err != nil {
			t.Fatal(err)
		}
		editBundle(t, filepath.Join(h.dir, "bundle.yaml"), func(bundle *operator.Bundle) { bundle.Broker = untrusted })
		editBundle(t, h.bundle, func(bundle *operator.Bundle) { bundle.Broker = untrusted })
		for _, args := range [][]string{{"host", "receivers"}, {"status"}, {"host", "init", h.bundle}} {
			var out bytes.Buffer
			explained(t, "wherry "+strings.Join(args, " "), run(args, &out, &out), h.config, bundle.Password)
		}
	})
	t.Run("receiver", func(t *testing.T) {
		const password = "receiver-password-s3cr3t"
		bundle := filepath.Join(t.TempDir(), "receiver.bundle")
		if err := operator.ReplaceBundle(bundle, operator.Bundle{Broker: untrusted, Principal: operator.ReceiverPrincipal("receiver-a"), Password: password}); err != nil {
			t.Fatal(err)
		}
		localConfig := receiverLocal(t, bundle, testCA.pem)
		var out bytes.Buffer
		explained(t, "receiver run", run([]string{"receiver", "run", "--config-file", localConfig}, &out, &out), localConfig, password)
	})
	t.Run("operator", func(t *testing.T) {
		dir := operatorDir(t, untrusted, testCA.pem)
		explained(t, "operator check", runOperator([]string{"check", "--dir", dir}, io.Discard, io.Discard), filepath.Join(dir, "operator.yaml"), "operator-password")
		noTerminal(t)
		init := filepath.Join(t.TempDir(), "operator")
		err := runOperator([]string{"init", "--dir", init, "--broker", untrusted, "--super-user", "wherry.operator", "--super-user-password-file", filepath.Join(dir, "operator.password"), "--ca-cert", testCA.pem}, io.Discard, io.Discard)
		explained(t, "operator init", err, filepath.Join(init, "operator.yaml"), "operator-password")
		if _, err := os.Stat(init); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("a refused init wrote %s: %v", init, err)
		}
	})
}

// The revocation probe dials as the revoked principal through the service's pool, operator.yaml's: with it, a probe of
// a principal still authorised logs in and says so; without it, the dial fails on the certificate.
func TestRevocationProbeUsesThePool(t *testing.T) {
	ctx := context.Background()
	cluster, ca := tlsCluster(t)
	address := cluster.ListenAddrs()[0]
	root := connectTLS(t, address, ca, "admin", "password")
	with := &operator.Service{Operator: root, Broker: address, Roots: testRoots(t)}
	if err := with.Probe("wherry.operator")(ctx, "admin", "password"); !errors.Is(err, operator.ErrStillAuthorised) {
		t.Fatalf("probe through the pool = %v; want a login finding the principal still authorised", err)
	}
	without := &operator.Service{Operator: root, Broker: address}
	if err := without.Probe("wherry.operator")(ctx, "admin", "password"); err == nil || !unknownAuthority(err) {
		t.Fatalf("probe without the pool = %v; want the certificate refused", err)
	}
}

// trustHelper re-runs this binary as a host command in environment env, with no SSL_CERT_FILE or SSL_CERT_DIR but those
// env sets, and returns what it printed: OK, or ERR: and the error. Go reads the roots once per process, so each case is
// its own process.
func trustHelper(t *testing.T, command string, env ...string) string {
	t.Helper()
	helper := exec.Command(os.Args[0], "-test.run=^TestTrustHelperProcess$", "-test.timeout=2m")
	for _, variable := range os.Environ() {
		if !strings.HasPrefix(variable, "SSL_CERT_FILE=") && !strings.HasPrefix(variable, "SSL_CERT_DIR=") {
			helper.Env = append(helper.Env, variable)
		}
	}
	helper.Env = append(helper.Env, append([]string{trustHelperVariable + "=" + command}, env...)...)
	output, err := helper.CombinedOutput()
	if err != nil {
		t.Fatalf("helper: %v\n%s", err, output)
	}
	return string(output)
}

// ca_cert adds to the machine's trust store and replaces none of it. In a process with no SSL_CERT_FILE or SSL_CERT_DIR,
// a broker under a CA the system roots do not hold verifies through ca_cert, which on macOS runs the platform verifier
// first and the added roots after. In a process whose SSL_CERT_FILE and SSL_CERT_DIR name CA A, a broker under A still
// verifies while ca_cert names CA B; without them, it does not.
func TestCACertAddsToTheSystemRoots(t *testing.T) {
	h := newInitHost(t, nil)
	noTerminal(t)
	h.init(t, h.bundle)
	withCACert := func(pem string) {
		t.Helper()
		config := h.written(t)
		config.CACert = pem
		if err := writeYAML(h.config, config); err != nil {
			t.Fatal(err)
		}
	}
	other := otherCA(t)
	for _, c := range []struct {
		name, caCert, want string
		env                []string
	}{
		{"the broker's CA in ca_cert, no SSL_CERT variable", testCA.pem, "OK\n", nil},
		{"the broker's CA as the system roots, another in ca_cert", other, "OK\n", []string{"SSL_CERT_FILE=" + testCA.file, "SSL_CERT_DIR=" + t.TempDir()}},
		{"another CA in ca_cert, no SSL_CERT variable", other, "ERR: " + unknownAuthorityHint + h.config + " (", nil},
	} {
		withCACert(c.caCert)
		if output := trustHelper(t, "host receivers", c.env...); !strings.Contains(output, c.want) {
			t.Fatalf("%s: helper printed %q; want %q", c.name, output, c.want)
		}
	}
}

// TestTrustHelperProcess runs the host command trustHelperVariable names, in the environment the test that re-ran this
// binary chose, and prints OK or the error.
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

// Every flag that takes a value reads it one way: the string as given, @FILE for the file's contents with ~ expanded, and
// @- for standard input, which a terminal refuses, as does a command with no standard input to read; @ alone and a file
// that cannot be opened are refused naming the path and the rule.
func TestFlagValue(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	if err := os.WriteFile(filepath.Join(dir, "value.txt"), []byte("from the file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]struct {
		value string
		stdin io.Reader
		want  string
	}{
		"a literal":               {"the text", nil, "the text"},
		"a literal with a dash":   {"-", nil, "-"},
		"@FILE":                   {"@" + filepath.Join(dir, "value.txt"), nil, "from the file\n"},
		"@~/FILE":                 {"@~/value.txt", nil, "from the file\n"},
		"@- piped":                {"@-", strings.NewReader("from standard input"), "from standard input"},
		"@- with no stdin":        {"@-", nil, "--flag @- cannot read standard input here; give the text or @FILE"},
		"@ alone":                 {"@", nil, "--flag @ names no file; a value starting with @ names a file, and @- standard input"},
		"@FILE that is not there": {"@" + filepath.Join(dir, "missing.txt"), nil, "--flag @" + filepath.Join(dir, "missing.txt") + ": open " + filepath.Join(dir, "missing.txt") + ": no such file or directory; a value starting with @ names a file"},
	} {
		got, err := flagValue("--flag", c.value, openFlagFile, c.stdin)
		if err != nil {
			got = err.Error()
		}
		if got != c.want {
			t.Errorf("%s: flagValue(%q) = %q, %v; want %q", name, c.value, got, err, c.want)
		}
	}
	master, tty, err := pty.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()
	defer tty.Close()
	if _, err := flagValue("--flag", "@-", openFlagFile, tty); err == nil || err.Error() != "--flag @-: standard input is a terminal; pipe the value in, or give the text or @FILE" {
		t.Fatalf("@- on a terminal: %v; want it refused", err)
	}
}

// --ca-cert on host init and operator init takes the PEM text, @FILE or @-, writes it into the configuration's ca_cert
// as a block scalar with CRLF normalised to LF, which loads back, and refuses a value from which Go appends no
// certificate, a bare path among them; operator init refuses reading the password and the CA from standard input at once.
func TestCACertFlagWritesABlockScalar(t *testing.T) {
	crlf := strings.ReplaceAll(testCA.pem, "\n", "\r\n")
	crlfFile := writeFile(t, "ca-crlf.pem", crlf)
	t.Run("host", func(t *testing.T) {
		h := newInitHost(t, nil)
		noTerminal(t)
		for name, value := range map[string]string{"@FILE": "@" + crlfFile, "the text": crlf, "@-": "@-"} {
			// Each form writes the configuration afresh; the host key, once registered, stays.
			if err := os.RemoveAll(h.config); err != nil {
				t.Fatal(err)
			}
			if value == "@-" {
				pipeStandardInput(t, crlf)
			}
			var out bytes.Buffer
			if err := run([]string{"host", "init", h.bundle, "--ca-cert", value}, &out, &out); err != nil {
				t.Fatalf("host init --ca-cert %s: %v", name, err)
			}
			written, err := os.ReadFile(h.config)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(written), "ca_cert: |\n") || strings.Contains(string(written), "\r") {
				t.Fatalf("host init --ca-cert %s wrote\n%s; want ca_cert as a block scalar without CR", name, written)
			}
			loaded, err := busclient.LoadConfig(h.config)
			if err != nil || loaded.CACert != testCA.pem {
				t.Fatalf("host init --ca-cert %s: loaded ca_cert %q, %v; want the PEM with LF line ends", name, loaded.CACert, err)
			}
		}
		if err := os.RemoveAll(h.config); err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		want := "--ca-cert holds no certificate; give the PEM text, @FILE or @-"
		if err := run([]string{"host", "init", h.bundle, "--ca-cert", testCA.file}, &out, &out); err == nil || err.Error() != want {
			t.Fatalf("host init --ca-cert with a bare path = %v; want %q", err, want)
		}
		if _, err := os.Stat(h.config); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("a refused init wrote %s: %v", h.config, err)
		}
	})
	t.Run("operator", func(t *testing.T) {
		b, _ := kfakeInitBroker(t)
		address, passwordFile := b.address, b.superPasswordFile
		noTerminal(t)
		dir := filepath.Join(t.TempDir(), "operator")
		if err := runOperator([]string{"init", "--dir", dir, "--broker", address, "--super-user", b.superUser, "--super-user-password-file", passwordFile, "--ca-cert", "@" + crlfFile}, io.Discard, io.Discard); err != nil {
			t.Fatalf("operator init --ca-cert @FILE: %v", err)
		}
		written, err := os.ReadFile(filepath.Join(dir, "operator.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(written), "ca_cert: |\n") || strings.Contains(string(written), "\r") {
			t.Fatalf("operator init wrote\n%s; want ca_cert as a block scalar without CR", written)
		}
		if config, _, err := loadOperatorConfig(dir); err != nil || config.CACert != testCA.pem {
			t.Fatalf("operator.yaml loads ca_cert %q, %v; want the PEM with LF line ends", config.CACert, err)
		}
		if err := runOperator([]string{"check", "--dir", dir}, io.Discard, io.Discard); err != nil {
			t.Fatalf("operator check through the written ca_cert: %v", err)
		}
		want := "--ca-cert holds no certificate; give the PEM text, @FILE or @-"
		if err := runOperator([]string{"init", "--dir", filepath.Join(t.TempDir(), "operator"), "--broker", address, "--super-user", b.superUser, "--super-user-password-file", passwordFile, "--ca-cert", crlfFile}, io.Discard, io.Discard); err == nil || err.Error() != want {
			t.Fatalf("operator init --ca-cert with a bare path = %v; want %q", err, want)
		}
		pipeStandardInput(t, "root-secret-value\n")
		want = "--super-user-password-file - and --ca-cert @- both read standard input; give one of them a file"
		if err := runOperator([]string{"init", "--dir", filepath.Join(t.TempDir(), "operator"), "--broker", address, "--super-user", b.superUser, "--super-user-password-file", "-", "--ca-cert", "@-"}, io.Discard, io.Discard); err == nil || err.Error() != want {
			t.Fatalf("operator init reading both from standard input = %v; want %q", err, want)
		}
	})
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
