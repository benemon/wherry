package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
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

// editBundle rewrites the bundle at path with edit applied; a bundle written before the operator trusted its broker
// through a CA file is one with its ca dropped.
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

// A host with no ca_file trusts the broker through its bundle's ca: host init and every later command connect, on a
// TLS broker whose CA is private, with nothing but the bundle.
func TestHostTrustsTheBrokerThroughItsBundle(t *testing.T) {
	h := newInitHost(t, nil)
	noTerminal(t)
	if bundle, err := operator.ReadBundle(h.bundle); err != nil || bundle.CA == "" {
		t.Fatalf("bundle %+v, %v; want the operator's CA in it", bundle, err)
	}
	stdout, _ := h.init(t, h.bundle)
	h.registered(t, strings.TrimSpace(stdout))
	if config := h.written(t); config.CAFile != "" {
		t.Fatalf("host init wrote ca_file %q; want none", config.CAFile)
	}
	var out bytes.Buffer
	if err := run([]string{"host", "receivers"}, &out, &out); err != nil {
		t.Fatalf("host receivers through the bundle's CA: %v", err)
	}
}

// A local ca_file overrides the bundle's ca: one that does not trust the broker fails the connection though the
// bundle's ca would, and one that does connects though the bundle's ca would not.
func TestHostLocalCAFileOverridesTheBundle(t *testing.T) {
	h := newInitHost(t, nil)
	noTerminal(t)
	bundle, err := operator.ReadBundle(h.bundle)
	if err != nil {
		t.Fatal(err)
	}
	right, wrong := writeFile(t, "right-ca.pem", bundle.CA), writeFile(t, "wrong-ca.pem", otherCA(t))
	configure := func(caFile string) {
		t.Helper()
		if err := os.MkdirAll(h.dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := writeYAML(h.config, busclient.Config{Bundle: filepath.Join(h.dir, "bundle.yaml"), CAFile: caFile}); err != nil {
			t.Fatal(err)
		}
	}
	configure(wrong)
	var out bytes.Buffer
	err = run([]string{"host", "init", h.bundle}, &out, &out)
	if err == nil || !unknownAuthority(err) {
		t.Fatalf("host init with a ca_file that does not trust the broker = %v; want the connection refused on the certificate, not the bundle's ca used", err)
	}
	editBundle(t, h.bundle, func(bundle *operator.Bundle) { bundle.CA = otherCA(t) })
	configure(right)
	if err := run([]string{"host", "init", h.bundle}, &out, &out); err != nil {
		t.Fatalf("host init with a ca_file that trusts the broker and a bundle whose ca does not = %v; want the ca_file to win", err)
	}
}

// A bundle written before the operator trusted the broker through a CA file carries none; under the new binary, with
// no ca_file and no ca.pem fallback, a host fails against a private-CA broker, and the failure names the refresh that
// writes a bundle with the CA.
func TestOldBundleWithoutCAFailsNamingRefresh(t *testing.T) {
	h := newInitHost(t, nil)
	noTerminal(t)
	editBundle(t, h.bundle, func(bundle *operator.Bundle) { bundle.CA = "" })
	// The old fallback, ca.pem in the configuration directory, is no longer read.
	if err := os.MkdirAll(h.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(h.dir, "ca.pem"), []byte(h.service.CA), 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	err := run([]string{"host", "init", h.bundle}, &out, &out)
	if err == nil || !unknownAuthority(err) {
		t.Fatalf("host init with a bundle carrying no ca = %v; want the connection refused on the certificate", err)
	}
	if text := err.Error(); !strings.Contains(text, "the system roots do not trust it") || !strings.Contains(text, "wherry operator tenant host refresh tenant-a host-a") {
		t.Fatalf("host init with a bundle carrying no ca = %v; want the system roots and the refresh command named", err)
	}
	if strings.Contains(err.Error(), "password") {
		t.Fatalf("host init = %v; want no password in the error", err)
	}
}

// unknownAuthorityText is the x509 error a broker certificate the connection does not trust fails with, kept at the
// end of its explanation.
const unknownAuthorityText = "x509: certificate signed by unknown authority"

// hostRefreshRemedy is the remedy of a host whose bundle wants a refresh, by the test host's names.
const hostRefreshRemedy = "ask the operator for a new bundle (wherry operator tenant host refresh tenant-a host-a) and install it with wherry host init ("

// explained fails t unless err is an unknown authority explained once, starting with want and ending with the x509
// error, with no password or certificate in it.
func explained(t *testing.T, what string, err error, want string) {
	t.Helper()
	if err == nil || !unknownAuthority(err) {
		t.Fatalf("%s = %v; want the connection refused on the certificate", what, err)
	}
	text := err.Error()
	if !strings.HasPrefix(text, want) || !strings.Contains(text, unknownAuthorityText) || !strings.HasSuffix(text, ")") {
		t.Fatalf("%s = %q; want %q with the x509 error at the end", what, text, want)
	}
	if n := strings.Count(text, "cannot verify the broker's certificate"); n != 1 {
		t.Fatalf("%s = %q; want the explanation once, not %d times", what, text, n)
	}
	if strings.Contains(text, "password") || strings.Contains(text, "-----BEGIN") {
		t.Fatalf("%s = %q; want no password or certificate in the error", what, text)
	}
}

// A connection the broker's certificate fails names the CA it was checked against and the one remedy for that CA, once,
// through host init, a host command and its top-level shortcut: a ca_file by its path in the host configuration, with
// its removal offered only while the bundle carries a CA; the bundle's CA and the system roots by the refresh of the
// host's real tenant and host.
func TestHostUnknownAuthorityNamesTheCAUsed(t *testing.T) {
	h := newInitHost(t, nil)
	noTerminal(t)
	h.init(t, h.bundle)
	installed := filepath.Join(h.dir, "bundle.yaml")
	wrong := writeFile(t, "wrong-ca.pem", otherCA(t))
	right, err := operator.ReadBundle(h.bundle)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name, caFile, bundleCA, want string
	}{
		{"ca_file", wrong, right.CA, "cannot verify the broker's certificate: it does not chain to a CA in ca_file " + wrong + " in your host configuration; point ca_file at the broker's CA, or remove it to use the bundle's ("},
		{"ca_file without a bundle ca", wrong, "", "cannot verify the broker's certificate: it does not chain to a CA in ca_file " + wrong + " in your host configuration; point ca_file at the broker's CA ("},
		{"bundle ca", "", otherCA(t), "cannot verify the broker's certificate: it does not chain to the CA in your bundle; " + hostRefreshRemedy},
		{"system roots", "", "", "cannot verify the broker's certificate: your bundle carries no CA and the system roots do not trust it; " + hostRefreshRemedy},
	} {
		if err := writeYAML(h.config, busclient.Config{Bundle: installed, CAFile: c.caFile}); err != nil {
			t.Fatal(err)
		}
		editBundle(t, h.bundle, func(bundle *operator.Bundle) { bundle.CA = c.bundleCA })
		// host init installs the bundle before it connects, so the commands after it read the same bundle.
		for _, args := range [][]string{{"host", "init", h.bundle}, {"host", "status"}, {"status"}} {
			var out bytes.Buffer
			explained(t, c.name+" through wherry "+strings.Join(args, " "), run(args, &out, &out), c.want)
		}
	}
}

// Connect explains an unknown authority alone: a connection that fails for another reason, here a wrong password,
// returns that failure as it is.
func TestHostConnectPassesOtherFailuresThrough(t *testing.T) {
	h := newInitHost(t, nil)
	noTerminal(t)
	h.init(t, h.bundle)
	editBundle(t, filepath.Join(h.dir, "bundle.yaml"), func(bundle *operator.Bundle) { bundle.Password = "wrong-" + bundle.Password })
	var out bytes.Buffer
	err := run([]string{"status"}, &out, &out)
	if err == nil || unknownAuthority(err) || strings.Contains(err.Error(), "cannot verify the broker's certificate") {
		t.Fatalf("status with a wrong password = %v; want its own failure, unexplained", err)
	}
}

// host init BUNDLE warns when the configuration keeps a ca_file, which overrides the CA the bundle carries; a
// configuration without one, or a bundle without a ca, gets no warning.
func TestHostInitWarnsWhenCAFileOverridesTheBundle(t *testing.T) {
	h := newInitHost(t, nil)
	noTerminal(t)
	bundle, err := operator.ReadBundle(h.bundle)
	if err != nil {
		t.Fatal(err)
	}
	caFile := writeFile(t, "kafka-ca.pem", bundle.CA)
	if err := os.MkdirAll(h.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := writeYAML(h.config, busclient.Config{Bundle: filepath.Join(h.dir, "bundle.yaml"), CAFile: caFile}); err != nil {
		t.Fatal(err)
	}
	_, prompts := h.init(t, h.bundle)
	want := "warning: ca_file " + caFile + " in " + h.config + " overrides the CA the bundle carries; remove the line unless the broker is to be trusted through that file alone\n"
	if !strings.Contains(prompts, want) {
		t.Fatalf("host init with a ca_file and a bundle carrying a ca printed %q; want %q", prompts, want)
	}
	// A rerun without BUNDLE reads no bundle anew and warns nothing.
	if _, prompts := h.init(t); strings.Contains(prompts, "warning") {
		t.Fatalf("host init without BUNDLE printed %q; want no warning", prompts)
	}
	editBundle(t, h.bundle, func(bundle *operator.Bundle) { bundle.CA = "" })
	if _, prompts := h.init(t, h.bundle); strings.Contains(prompts, "warning") {
		t.Fatalf("host init with a bundle carrying no ca printed %q; want no warning", prompts)
	}
	if err := writeYAML(h.config, busclient.Config{Bundle: filepath.Join(h.dir, "bundle.yaml")}); err != nil {
		t.Fatal(err)
	}
	editBundle(t, h.bundle, func(b *operator.Bundle) { b.CA = bundle.CA })
	if _, prompts := h.init(t, h.bundle); strings.Contains(prompts, "warning") {
		t.Fatalf("host init without a ca_file printed %q; want no warning", prompts)
	}
}

// A receiver with no ca_file trusts the broker through its bundle's ca; with a bundle carrying none it fails naming
// receiver apply, which rewrites the bundle.
func TestReceiverTrustsTheBrokerThroughItsBundle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cluster, ca := tlsCluster(t)
	address := cluster.ListenAddrs()[0]
	service := &operator.Service{Operator: connectTLS(t, address, ca, "wherry.operator", "operator-password"),
		Broker: address, CA: []byte(ca), DeliveryCeiling: 10, LockCeilingMS: 60000}
	terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude", Credentials: []operator.AgentCredential{{TaskCredential: wire.TaskCredential{Name: "model", Env: "MODEL_API_KEY"}}}}}, DefaultAgent: "claude",
		DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
	if err := service.PoolApply(ctx, "pool-a", terms, false); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	bundle := filepath.Join(dir, "receiver.bundle")
	config := operator.ReceiverConfig{Receiver: "receiver-a", Pool: "pool-a",
		Profiles: []operator.ProfileSettings{{Name: "fake", Plugin: "fake", Agent: "claude"}}, PluginSHA256: map[string]string{"fake": fakeSHA256}}
	if err := service.ReceiverAdd(ctx, config, service.Broker, bundle); err != nil {
		t.Fatal(err)
	}
	localConfig := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(localConfig, []byte("bundle: "+bundle+"\nstate_dir: "+filepath.Join(dir, "state")+"\nplugin_dir: "+t.TempDir()+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// With no plugin the receiver stops at configuration, which comes after it registers its key on the broker.
	var stdout, stderr bytes.Buffer
	if err := run([]string{"receiver", "run", "--config-file", localConfig}, &stdout, &stderr); err == nil || err.Error() != "profile fake: no plugin for type fake" {
		t.Fatalf("receiver run through the bundle's CA: %v; want it to stop at configuration", err)
	}
	keys, err := operator.ReadKeys(ctx, service.Operator, map[string]string{"receiver-a": operator.ReceiverTopic("receiver-a")})
	if err != nil || keys["receiver-a"].Current == nil {
		t.Fatalf("keys %v, %v; want the receiver registered through the bundle's CA", keys, err)
	}
	// The failure names the CA the receiver used and the receiver's own refresh and install, never the host's.
	const refresh = "ask the operator for a new bundle (wherry operator receiver apply receiver-a) and install the new bundle by your receiver platform's procedure and restart the receiver ("
	receiverRun := func(what, want string) {
		t.Helper()
		err := run([]string{"receiver", "run", "--config-file", localConfig}, &stdout, &stderr)
		explained(t, what, err, want)
		if text := err.Error(); strings.Contains(text, "tenant host refresh") || strings.Contains(text, "wherry host init") {
			t.Fatalf("%s = %q; want the receiver's remedy, not the host's", what, text)
		}
	}
	editBundle(t, bundle, func(bundle *operator.Bundle) { bundle.CA = "" })
	receiverRun("receiver run with a bundle carrying no ca", "cannot verify the broker's certificate: your bundle carries no CA and the system roots do not trust it; "+refresh)
	editBundle(t, bundle, func(bundle *operator.Bundle) { bundle.CA = otherCA(t) })
	receiverRun("receiver run with a bundle whose ca does not trust the broker", "cannot verify the broker's certificate: it does not chain to the CA in your bundle; "+refresh)
	// A ca_file that does not trust the broker overrides the bundle's ca, and is named with the configuration's path.
	editBundle(t, bundle, func(bundle *operator.Bundle) { bundle.CA = ca })
	wrong := writeFile(t, "wrong-ca.pem", otherCA(t))
	if err := os.WriteFile(localConfig, []byte("bundle: "+bundle+"\nca_file: "+wrong+"\nstate_dir: "+filepath.Join(dir, "state")+"\nplugin_dir: "+t.TempDir()+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	receiverRun("receiver run with a ca_file that does not trust the broker", "cannot verify the broker's certificate: it does not chain to a CA in ca_file "+wrong+" in "+localConfig+"; point ca_file at the broker's CA, or remove it to use the bundle's (")
}

// receiver apply of an existing receiver compares the broker and the CA on their own: with the broker unchanged and
// the operator's CA changed, it rewrites the bundle with the new CA and the same password; with another broker it
// drops the CA.
func TestReceiverApplyRewritesTheBundleOnACAChange(t *testing.T) {
	service := operatorEnv(t)
	localFixture(t, ".", "receiver-a")
	ctx := context.Background()
	terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude"}}, DefaultAgent: "claude", DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
	if err := service.PoolApply(ctx, "pool-a", terms, false); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll("receivers", 0o755); err != nil {
		t.Fatal(err)
	}
	kept := filepath.Join("receivers", "receiver-a.yaml")
	write := func(broker string) {
		t.Helper()
		head := "pool: pool-a\nplugin_sha256: {nomad: " + fakeSHA256 + "}\n"
		if broker != "" {
			head += "broker: " + broker + "\n"
		}
		if err := os.WriteFile(kept, []byte(head+basePlugin+item("name: claude\nagent: claude\n", "      ")), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("")
	if err := runOperator([]string{"receiver", "apply", "receiver-a"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("bundles", "receivers", "receiver-a.bundle")
	before, err := operator.ReadBundle(path)
	if err != nil || before.Broker != service.Broker || before.CA != string(service.CA) {
		t.Fatalf("bundle %+v, %v; want the operator's broker and CA", before, err)
	}
	// The operator's CA file gains a certificate, as it does when a platform renews its CA: the operator still trusts
	// the broker, and the file's bytes differ.
	renewed := string(service.CA) + otherCA(t)
	if err := os.WriteFile("ca.pem", []byte(renewed), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runOperator([]string{"receiver", "apply", "receiver-a"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	after, err := operator.ReadBundle(path)
	if err != nil || after != (operator.Bundle{Broker: before.Broker, Principal: before.Principal, Password: before.Password, CA: renewed}) {
		t.Fatalf("bundle %+v, %v; want the renewed CA with the broker, principal and password of %+v", after, err, before)
	}
	write("kafka.platform.internal:9093")
	if err := runOperator([]string{"receiver", "apply", "receiver-a"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if elsewhere, err := operator.ReadBundle(path); err != nil || elsewhere != (operator.Bundle{Broker: "kafka.platform.internal:9093", Principal: before.Principal, Password: before.Password}) {
		t.Fatalf("bundle %+v, %v; want the other broker, no CA and the password of %+v", elsewhere, err, before)
	}
}

// tenant host refresh rewrites a kept host bundle with the operator's CA and the same password, says so with the
// next step, reports a current bundle as current, and refuses a missing bundle naming --force.
func TestTenantHostRefreshCommand(t *testing.T) {
	service := operatorEnv(t)
	for _, args := range [][]string{{"tenant", "add", "tenant-a"}, {"tenant", "host", "add", "tenant-a", "host-a"}} {
		if err := runOperator(args, io.Discard, io.Discard); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join("bundles", "hosts", "tenant-a", "host-a.bundle")
	before, err := operator.ReadBundle(path)
	if err != nil || before.CA != string(service.CA) {
		t.Fatalf("bundle %+v, %v; want the operator's CA", before, err)
	}
	var stdout bytes.Buffer
	if err := runOperator([]string{"tenant", "host", "refresh", "tenant-a", "host-a"}, &stdout, io.Discard); err != nil || stdout.String() != path+" is current\n" {
		t.Fatalf("refresh of a current bundle: %v, %q", err, stdout.String())
	}
	renewed := string(service.CA) + otherCA(t)
	if err := os.WriteFile("ca.pem", []byte(renewed), 0o600); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	if err := runOperator([]string{"tenant", "host", "refresh", "tenant-a", "host-a"}, &stdout, io.Discard); err != nil {
		t.Fatal(err)
	}
	if want := "refreshed " + path + "; hand it to the host, where wherry host init host-a.bundle installs it\n"; stdout.String() != want {
		t.Fatalf("refresh printed %q; want %q", stdout.String(), want)
	}
	after, err := operator.ReadBundle(path)
	if err != nil || after != (operator.Bundle{Broker: before.Broker, Principal: before.Principal, Password: before.Password, CA: renewed}) {
		t.Fatalf("bundle %+v, %v; want the renewed CA with the password of %+v", after, err, before)
	}
	stdout.Reset()
	if err := runOperator([]string{"tenant", "host", "refresh", "tenant-a", "host-a"}, &stdout, io.Discard); err != nil || stdout.String() != path+" is current\n" {
		t.Fatalf("refresh again: %v, %q", err, stdout.String())
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	err = runOperator([]string{"tenant", "host", "refresh", "tenant-a", "host-a"}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "--force") || !strings.Contains(err.Error(), path) {
		t.Fatalf("refresh without a kept bundle = %v; want the missing bundle and --force named", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a refused refresh wrote %s: %v", path, err)
	}
	for _, args := range [][]string{{"tenant", "host", "refresh", "tenant-a"}, {"tenant", "host", "refresh", "tenant-a", "host-a", "extra"}} {
		if err := runOperator(args, io.Discard, io.Discard); err == nil || err.Error() != operatorUsage["tenant host refresh"] {
			t.Fatalf("refresh %v = %v; want its usage", args, err)
		}
	}
	if err := runOperator([]string{"tenant", "host", "refresh", "tenant-a", "host-a", "--force"}, io.Discard, io.Discard); err == nil || err.Error() != "unknown option --force" {
		t.Fatalf("refresh --force = %v; want it refused", err)
	}
}

// receiver apply of an existing receiver checks that the kept bundle is that receiver's before it records or rewrites
// anything, as a host refresh does: with receiver-b's principal in receiver-a's kept bundle and the operator's CA
// changed, apply of receiver-a is refused naming both principals, the bundle is untouched and no record is appended.
func TestReceiverApplyRefusesAnotherPrincipalsBundle(t *testing.T) {
	service := operatorEnv(t)
	ctx := context.Background()
	terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude"}}, DefaultAgent: "claude", DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
	if err := service.PoolApply(ctx, "pool-a", terms, false); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll("receivers", 0o755); err != nil {
		t.Fatal(err)
	}
	for _, receiver := range []string{"receiver-a", "receiver-b"} {
		localFixture(t, ".", receiver)
		head := "pool: pool-a\nplugin_sha256: {nomad: " + fakeSHA256 + "}\n"
		if err := os.WriteFile(filepath.Join("receivers", receiver+".yaml"), []byte(head+basePlugin+item("name: claude\nagent: claude\n", "      ")), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := runOperator([]string{"receiver", "apply", receiver}, io.Discard, io.Discard); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join("bundles", "receivers", "receiver-a.bundle")
	editBundle(t, path, func(bundle *operator.Bundle) { bundle.Principal = operator.ReceiverPrincipal("receiver-b") })
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	records := func() int64 {
		t.Helper()
		offsets, err := kadm.NewClient(service.Operator).ListEndOffsets(ctx, operator.OperatorTopic)
		if err != nil || offsets.Error() != nil {
			t.Fatal(err, offsets.Error())
		}
		offset, _ := offsets.Lookup(operator.OperatorTopic, 0)
		return offset.Offset
	}
	appended := records()
	if err := os.WriteFile("ca.pem", []byte(string(service.CA)+otherCA(t)), 0o600); err != nil {
		t.Fatal(err)
	}
	err = runOperator([]string{"receiver", "apply", "receiver-a"}, io.Discard, io.Discard)
	if want := path + " is the bundle of wherry.rcv.receiver-b, not of wherry.rcv.receiver-a"; err == nil || err.Error() != want {
		t.Fatalf("apply with another principal's bundle = %v; want %q", err, want)
	}
	if after, err := os.ReadFile(path); err != nil || !bytes.Equal(after, before) {
		t.Fatalf("a refused apply rewrote %s", path)
	}
	if records() != appended {
		t.Fatalf("a refused apply appended a record")
	}
	// The bundle's own principal back, the apply rewrites it with the renewed CA.
	editBundle(t, path, func(bundle *operator.Bundle) { bundle.Principal = operator.ReceiverPrincipal("receiver-a") })
	var stdout bytes.Buffer
	if err := runOperator([]string{"receiver", "apply", "receiver-a"}, &stdout, io.Discard); err != nil {
		t.Fatal(err)
	}
	if want := "refreshed " + path + "; hand it to the receiver's platform, whose install guide's bundle step installs it, then restart the receiver\n"; stdout.String() != want {
		t.Fatalf("apply printed %q; want %q", stdout.String(), want)
	}
}

// unknownAuthority reports whether err is a connection the broker's certificate failed for chaining to no trusted CA.
func unknownAuthority(err error) bool {
	var unknown x509.UnknownAuthorityError
	return errors.As(err, &unknown)
}
