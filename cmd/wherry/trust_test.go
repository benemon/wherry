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

// certificateRefused reports whether err is a connection the broker's certificate failed, whatever the verifier said of
// it: Go's own verifier names an unknown authority, the platform verifier of macOS and Windows words it its own way,
// and crypto/tls wraps both the same.
func certificateRefused(err error) bool {
	var verification *tls.CertificateVerificationError
	return errors.As(err, &verification)
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

// unknownAuthorityText is the x509 error Go's own verifier gives a broker certificate the connection does not trust,
// kept at the end of its explanation; unknownAuthorityHint is the explanation's one remedy where the configuration
// holds no ca_cert, which precedes it and names the configuration file whose ca_cert takes the CA, and caCertSetHint
// the explanation where it holds one, which asks for no key already set.
const (
	unknownAuthorityText = "x509: certificate signed by unknown authority"
	unknownAuthorityHint = "cannot verify the broker's certificate: install the CA that signed it in this machine's trust store, or set ca_cert in "
	caCertSetHint        = "cannot verify the broker's certificate: it is signed neither by this machine's trust store nor by the CA in ca_cert in "
)

// trustHelperVariable names, in a re-run of this binary, the host command the trust helper runs.
const trustHelperVariable = "WHERRY_TEST_TRUST_HELPER"

// explained fails t unless err is an unknown authority explained once, with the hint for ca, the configuration file and
// whether it holds a ca_cert, ending with the x509 error, with neither password nor a certificate in it. With a ca_cert
// set, Go's own verifier runs after the platform's and names the unknown authority on every OS; without one, the
// platform verifier of macOS and Windows words the refusal its own way, so only x509's prefix is required.
func explained(t *testing.T, what string, err error, ca operator.CASetting, password string) {
	t.Helper()
	if err == nil || !certificateRefused(err) {
		t.Fatalf("%s = %v; want the connection refused on the certificate", what, err)
	}
	text := err.Error()
	hint := unknownAuthorityHint
	if ca.Set {
		hint = caCertSetHint
	}
	if !strings.HasPrefix(text, hint+ca.File+" (") || !strings.Contains(text, "x509: ") || !strings.HasSuffix(text, ")") {
		t.Fatalf("%s = %q; want %q with the x509 error at the end", what, text, hint+ca.File+" (")
	}
	if ca.Set && !strings.Contains(text, unknownAuthorityText) {
		t.Fatalf("%s = %q; want Go's verifier's %q after the added CA failed too", what, text, unknownAuthorityText)
	}
	if strings.Contains(text, "set ca_cert") == ca.Set {
		t.Fatalf("%s = %q; want the hint to say to set ca_cert exactly when the configuration holds none (set: %v)", what, text, ca.Set)
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
		// init wrote the test CA as ca_cert, so the hint says the CA in it signed nothing either.
		for _, args := range [][]string{{"host", "receivers"}, {"status"}, {"host", "init", h.bundle}} {
			var out bytes.Buffer
			explained(t, "wherry "+strings.Join(args, " "), run(args, &out, &out), operator.CASetting{File: h.config, Set: true}, bundle.Password)
		}
		// Without a ca_cert, the hint says to set it.
		config := h.written(t)
		config.CACert = ""
		if err := writeYAML(h.config, config); err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		explained(t, "wherry host receivers without a ca_cert", run([]string{"host", "receivers"}, &out, &out), operator.CASetting{File: h.config}, bundle.Password)
	})
	t.Run("receiver", func(t *testing.T) {
		const password = "receiver-password-s3cr3t"
		bundle := filepath.Join(t.TempDir(), "receiver.bundle")
		if err := operator.ReplaceBundle(bundle, operator.Bundle{Broker: untrusted, Principal: operator.ReceiverPrincipal("receiver-a"), Password: password}); err != nil {
			t.Fatal(err)
		}
		for _, caCert := range []string{testCA.pem, ""} {
			localConfig := receiverLocal(t, bundle, caCert)
			var out bytes.Buffer
			explained(t, "receiver run", run([]string{"receiver", "run", "--config-file", localConfig}, &out, &out), operator.CASetting{File: localConfig, Set: caCert != ""}, password)
		}
	})
	t.Run("operator", func(t *testing.T) {
		for _, caCert := range []string{testCA.pem, ""} {
			dir := operatorDir(t, untrusted, caCert)
			explained(t, "operator check", runOperator([]string{"check", "--dir", dir}, io.Discard, io.Discard), operator.CASetting{File: filepath.Join(dir, "operator.yaml"), Set: caCert != ""}, "operator-password")
		}
		noTerminal(t)
		passwordFile := writeFile(t, "operator.password", "operator-password\n")
		for _, caCert := range []string{testCA.pem, ""} {
			init := filepath.Join(t.TempDir(), "operator")
			args := []string{"init", "--dir", init, "--broker", untrusted, "--super-user", "wherry.operator", "--super-user-password", "@" + passwordFile}
			if caCert != "" {
				args = append(args, "--ca-cert", caCert)
			}
			err := runOperator(args, io.Discard, io.Discard)
			explained(t, "operator init", err, operator.CASetting{File: filepath.Join(init, "operator.yaml"), Set: caCert != ""}, "operator-password")
			if _, err := os.Stat(init); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("a refused init wrote %s: %v", init, err)
			}
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
	if err := without.Probe("wherry.operator")(ctx, "admin", "password"); err == nil || !certificateRefused(err) {
		t.Fatalf("probe without the pool = %v; want the certificate refused", err)
	}
}

// A remove through operatorService probes every broker as the revoked principal through operator.yaml's pool, the
// service's Roots: the probe reaches the broker, which denies the principal Describe on its topic once its ACLs are
// gone, and the revocation completes, deleting the bundle. The probe is not stubbed here; a service built without the
// pool fails the probe on the certificate and keeps the bundle.
func TestRemoveProbesThroughTheServicePool(t *testing.T) {
	service := operatorEnv(t)
	ctx := context.Background()
	if err := service.TenantAdd(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if err := runOperator([]string{"tenant", "host", "add", "tenant-a", "host-a"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	revocationBounds(t, service, "1")
	hostBundle := filepath.Join("bundles", "hosts", "tenant-a", "host-a.bundle")
	var stdout bytes.Buffer
	if err := runOperator([]string{"tenant", "host", "remove", "tenant-a", "host-a", "--wait", "1s"}, &stdout, io.Discard); err != nil {
		t.Fatalf("remove through the service's pool = %v; printed\n%s", err, stdout.String())
	}
	if !strings.Contains(stdout.String(), "step=3 complete=true") {
		t.Fatalf("remove printed\n%s\nwant the probe complete", stdout.String())
	}
	if _, err := os.Stat(hostBundle); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("remove left %s after a complete revocation: %v", hostBundle, err)
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
		{"another CA in ca_cert, no SSL_CERT variable", other, "ERR: " + caCertSetHint + h.config + " (", nil},
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
// as a block scalar with every CR removed, which loads back as the PEM with LF line ends, the text as "$(cat FILE)" gives
// it of a CRLF file included, where the shell strips the final LF and leaves the CR before it; and refuses a value from
// which Go appends no certificate, a bare path among them. operator init refuses reading the password and the CA from
// standard input at once.
func TestCACertFlagWritesABlockScalar(t *testing.T) {
	crlf := strings.ReplaceAll(testCA.pem, "\n", "\r\n")
	crlfFile := writeFile(t, "ca-crlf.pem", crlf)
	forms := map[string]string{"@FILE": "@" + crlfFile, "the text": crlf, "the text as $(cat FILE) gives it": strings.TrimRight(crlf, "\n"), "@-": "@-"}
	t.Run("host", func(t *testing.T) {
		h := newInitHost(t, nil)
		noTerminal(t)
		for name, value := range forms {
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
		for name, value := range forms {
			dir := filepath.Join(t.TempDir(), "operator")
			if value == "@-" {
				pipeStandardInput(t, crlf)
			}
			if err := runOperator([]string{"init", "--dir", dir, "--broker", address, "--super-user", b.superUser, "--super-user-password", "@" + passwordFile, "--ca-cert", value}, io.Discard, io.Discard); err != nil {
				t.Fatalf("operator init --ca-cert %s: %v", name, err)
			}
			written, err := os.ReadFile(filepath.Join(dir, "operator.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(written), "ca_cert: |\n") || strings.Contains(string(written), "\r") {
				t.Fatalf("operator init --ca-cert %s wrote\n%s; want ca_cert as a block scalar without CR", name, written)
			}
			if config, _, err := loadOperatorConfig(dir); err != nil || config.CACert != testCA.pem {
				t.Fatalf("operator init --ca-cert %s: operator.yaml loads ca_cert %q, %v; want the PEM with LF line ends", name, config.CACert, err)
			}
			if err := runOperator([]string{"check", "--dir", dir}, io.Discard, io.Discard); err != nil {
				t.Fatalf("operator check through the ca_cert --ca-cert %s wrote: %v", name, err)
			}
		}
		want := "--ca-cert holds no certificate; give the PEM text, @FILE or @-"
		if err := runOperator([]string{"init", "--dir", filepath.Join(t.TempDir(), "operator"), "--broker", address, "--super-user", b.superUser, "--super-user-password", "@" + passwordFile, "--ca-cert", crlfFile}, io.Discard, io.Discard); err == nil || err.Error() != want {
			t.Fatalf("operator init --ca-cert with a bare path = %v; want %q", err, want)
		}
		pipeStandardInput(t, "root-secret-value\n")
		want = "--super-user-password @- and --ca-cert @- both read standard input; give one of them a file"
		if err := runOperator([]string{"init", "--dir", filepath.Join(t.TempDir(), "operator"), "--broker", address, "--super-user", b.superUser, "--super-user-password", "@-", "--ca-cert", "@-"}, io.Discard, io.Discard); err == nil || err.Error() != want {
			t.Fatalf("operator init reading both from standard input = %v; want %q", err, want)
		}
	})
}

// An empty or blank --ca-cert, however given, is refused before anything is written or dialled, as an empty --prompt is:
// the flag's own text, a file holding nothing and standard input holding nothing.
func TestEmptyCACertIsRefused(t *testing.T) {
	const want = "--ca-cert is empty; give the PEM text, @FILE or @-"
	blank := writeFile(t, "blank.pem", " \r\n\n")
	values := map[string]string{"the empty text": "", "blank text": " \n", "@FILE of nothing": "@/dev/null", "@FILE of whitespace": "@" + blank, "@- holding nothing": "@-"}
	t.Run("host", func(t *testing.T) {
		h := newInitHost(t, nil)
		noTerminal(t)
		for name, value := range values {
			if value == "@-" {
				pipeStandardInput(t, "")
			}
			var out bytes.Buffer
			if err := run([]string{"host", "init", h.bundle, "--ca-cert", value}, &out, &out); err == nil || err.Error() != want {
				t.Fatalf("host init --ca-cert %s = %v; want %q", name, err, want)
			}
			if _, err := os.Stat(h.config); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("host init --ca-cert %s wrote %s: %v", name, h.config, err)
			}
		}
	})
	t.Run("operator", func(t *testing.T) {
		noTerminal(t)
		passwordFile := writeFile(t, "password", "root-secret-value\n")
		for name, value := range values {
			if value == "@-" {
				pipeStandardInput(t, "")
			}
			dir := filepath.Join(t.TempDir(), "operator")
			err := runOperator([]string{"init", "--dir", dir, "--broker", "127.0.0.1:1", "--super-user", "kafka-root", "--super-user-password", "@" + passwordFile, "--ca-cert", value}, io.Discard, io.Discard)
			if err == nil || err.Error() != want {
				t.Fatalf("operator init --ca-cert %s = %v; want %q", name, err, want)
			}
			if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("operator init --ca-cert %s wrote %s: %v", name, dir, err)
			}
		}
	})
}

// operator init run again without --ca-cert keeps the existing operator.yaml's ca_cert, dials through it and writes it
// back, as host init keeps an existing value; with --ca-cert, the new value replaces it.
func TestOperatorInitWithoutCACertKeepsTheExistingOne(t *testing.T) {
	b, _ := kfakeInitBroker(t)
	noTerminal(t)
	dir := filepath.Join(t.TempDir(), "operator")
	if err := b.init(t, dir); err != nil {
		t.Fatal(err)
	}
	again := []string{"init", "--dir", dir, "--broker", b.address, "--super-user", b.superUser, "--super-user-password", "@" + b.superPasswordFile}
	if err := runOperator(again, io.Discard, io.Discard); err != nil {
		t.Fatalf("operator init again without --ca-cert: %v", err)
	}
	b.login(t, dir)
	if config, _, err := loadOperatorConfig(dir); err != nil || config.CACert != b.ca {
		t.Fatalf("after init again, operator.yaml holds ca_cert %q, %v; want the CA the first init wrote", config.CACert, err)
	}
	if err := runOperator([]string{"check", "--dir", dir}, io.Discard, io.Discard); err != nil {
		t.Fatalf("operator check after init again: %v", err)
	}
	other := otherCA(t)
	if err := runOperator(append(again, "--ca-cert", b.ca+other), io.Discard, io.Discard); err != nil {
		t.Fatalf("operator init again with --ca-cert: %v", err)
	}
	if config, _, err := loadOperatorConfig(dir); err != nil || config.CACert != b.ca+other {
		t.Fatalf("after init again with --ca-cert, operator.yaml holds ca_cert %q, %v; want the new value", config.CACert, err)
	}
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
