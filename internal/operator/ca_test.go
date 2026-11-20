package operator

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// testPEM is a self-signed CA certificate as PEM, named so two differ.
func testPEM(t *testing.T, name string) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

// Every bundle writer carries the CA the operator trusts its broker through, and none for a receiver on another
// address; an operator on the system roots writes no ca.
func TestBundleWritersCarryTheOperatorsCA(t *testing.T) {
	ctx := context.Background()
	for _, trusted := range []bool{true, false} {
		_, service, _ := compromiseFixture(t)
		ca := ""
		if trusted {
			ca = testPEM(t, "operator")
			service.CA = []byte(ca)
		}
		other := "kafka.other.internal:9093"
		dir := t.TempDir()
		for _, writer := range []struct {
			name   string
			write  func(path string) error
			broker string
		}{
			{"host add", func(path string) error { return service.HostAdd(ctx, "tenant-a", "host-b", path, false) }, service.Broker},
			{"host add --force", func(path string) error { return service.HostAdd(ctx, "tenant-a", "host-a", path, true) }, service.Broker},
			{"receiver add", func(path string) error {
				return service.ReceiverAdd(ctx, modelConfig("receiver-b", "pool-a"), service.Broker, path)
			}, service.Broker},
			{"receiver add elsewhere", func(path string) error {
				config := modelConfig("receiver-c", "pool-a")
				config.Broker = other
				return service.ReceiverAdd(ctx, config, other, path)
			}, other},
			{"receiver apply --force", func(path string) error { return service.ReceiverReissue(ctx, "receiver-a", service.Broker, path) }, service.Broker},
			{"receiver apply --force elsewhere", func(path string) error { return service.ReceiverReissue(ctx, "receiver-a", other, path) }, other},
		} {
			path := filepath.Join(dir, strings.ReplaceAll(writer.name, " ", "-")+".bundle")
			if err := writer.write(path); err != nil {
				t.Fatalf("trusted=%t %s: %v", trusted, writer.name, err)
			}
			bundle, err := ReadBundle(path)
			if err != nil {
				t.Fatalf("trusted=%t %s: %v", trusted, writer.name, err)
			}
			want := ""
			if trusted && writer.broker == service.Broker {
				want = ca
			}
			if bundle.Broker != writer.broker || bundle.CA != want {
				t.Fatalf("trusted=%t %s wrote broker %s with ca %q; want %s with ca %q", trusted, writer.name, bundle.Broker, bundle.CA, writer.broker, want)
			}
		}
	}
}

// A bundle's ca is written as a plain YAML string, the PEM as a person reads it, and read back the same; a []byte would
// marshal as !!binary.
func TestBundleCARoundTripsAsAString(t *testing.T) {
	ca := testPEM(t, "round-trip")
	path := filepath.Join(t.TempDir(), "host.bundle")
	written := Bundle{Broker: "broker:9093", Principal: TenantPrincipal("tenant-a", "host-a"), Password: "secret-password", CA: ca}
	if err := ReplaceBundle(path, written); err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if text := string(encoded); !strings.Contains(text, "ca: |") || !strings.Contains(text, "-----BEGIN CERTIFICATE-----") || strings.Contains(text, "!!binary") {
		t.Fatalf("bundle holds\n%s\nwant ca as a block string", text)
	}
	read, err := ReadBundle(path)
	if err != nil || read != written {
		t.Fatalf("read back %+v, %v; want %+v", read, err, written)
	}
	// Without a CA the field is left out, as older bundles are.
	if err := ReplaceBundle(path, Bundle{Broker: "broker:9093", Principal: written.Principal, Password: written.Password}); err != nil {
		t.Fatal(err)
	}
	if encoded, err = os.ReadFile(path); err != nil || strings.Contains(string(encoded), "ca:") {
		t.Fatalf("bundle without a CA holds\n%s, %v", encoded, err)
	}
}

// A bundle whose ca holds no certificate is refused when read, so a host or receiver never falls back to the system
// roots because of a damaged ca.
func TestReadBundleRefusesACAWithoutACertificate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.bundle")
	for _, ca := range []string{"not a certificate", "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n"} {
		if err := os.WriteFile(path, []byte("broker: b\nprincipal: p\npassword: s\nca: |\n  "+strings.ReplaceAll(ca, "\n", "\n  ")+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadBundle(path); err == nil || err.Error() != "bundle ca holds no PEM certificate" {
			t.Fatalf("bundle with ca %q read: %v; want it refused", ca, err)
		}
	}
}

// Refresh rewrites a kept host bundle with the operator's broker and CA and the same password, and leaves a current one
// alone; it refuses an absent, ended or revoking host, an absent tenant, a missing bundle, naming --force, and a bundle
// of another principal.
func TestHostRefresh(t *testing.T) {
	_, service, ctx := compromiseFixture(t)
	service.CA = []byte(testPEM(t, "first"))
	dir := t.TempDir()
	path := filepath.Join(dir, "host-a.bundle")
	if err := service.HostAdd(ctx, "tenant-a", "host-a", path, true); err != nil {
		t.Fatal(err)
	}
	before, err := ReadBundle(path)
	if err != nil || before.CA != string(service.CA) {
		t.Fatalf("bundle %+v, %v; want the first CA", before, err)
	}
	if changed, err := service.HostRefresh(ctx, "tenant-a", "host-a", path); err != nil || changed {
		t.Fatalf("refresh of a current bundle = %t, %v; want nothing changed", changed, err)
	}
	service.CA = []byte(testPEM(t, "second"))
	if changed, err := service.HostRefresh(ctx, "tenant-a", "host-a", path); err != nil || !changed {
		t.Fatalf("refresh after a CA change = %t, %v; want the bundle rewritten", changed, err)
	}
	after, err := ReadBundle(path)
	if err != nil || after.CA != string(service.CA) || after.Password != before.Password || after.Principal != before.Principal || after.Broker != before.Broker {
		t.Fatalf("refreshed bundle %+v, %v; want the second CA with the password of %+v", after, err, before)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("refreshed bundle %v, %v; want mode 0600", info, err)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := service.HostRefresh(ctx, "tenant-a", "host-a", path); err != nil || changed {
		t.Fatalf("refresh again = %t, %v; want it idempotent", changed, err)
	}
	if again, err := os.ReadFile(path); err != nil || string(again) != string(encoded) {
		t.Fatalf("a rerun changed the bundle:\n%s\n%s", encoded, again)
	}
	// Refusals leave the bundle as it is.
	if err := service.ReceiverAdd(ctx, modelConfig("receiver-b", "pool-a"), service.Broker, filepath.Join(dir, "receiver-b.bundle")); err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(dir, "other.bundle")
	if err := ReplaceBundle(other, Bundle{Broker: service.Broker, Principal: TenantPrincipal("tenant-a", "host-b"), Password: "p", CA: string(service.CA)}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.appendChecked(ctx, []byte("tenant-a.host-b"), Record{Kind: "host", Tenant: "tenant-a", Host: "host-b"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.appendChecked(ctx, []byte("tenant-a.host-b"), Record{Kind: "revoking", Tenant: "tenant-a", Host: "host-b"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.appendChecked(ctx, []byte("tenant-a.host-c"), Record{Kind: "host", Tenant: "tenant-a", Host: "host-c"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.appendChecked(ctx, []byte("tenant-a.host-c"), Record{Kind: "end", Tenant: "tenant-a", Host: "host-c"}); err != nil {
		t.Fatal(err)
	}
	for _, refusal := range []struct {
		name, tenant, host, path, want string
	}{
		{"absent tenant", "tenant-b", "host-a", path, "tenant absent"},
		{"absent host", "tenant-a", "host-z", path, "no host by that name"},
		{"revoking host", "tenant-a", "host-b", other, "host is revoking"},
		{"ended host", "tenant-a", "host-c", other, "host cannot return after its membership end"},
		{"missing bundle", "tenant-a", "host-a", filepath.Join(dir, "missing.bundle"), filepath.Join(dir, "missing.bundle") + " does not exist; tenant host add tenant-a host-a --force gives the host a new password and bundle"},
		{"another principal", "tenant-a", "host-a", other, other + " is the bundle of " + TenantPrincipal("tenant-a", "host-b") + ", not of " + TenantPrincipal("tenant-a", "host-a")},
		{"a receiver's bundle", "tenant-a", "host-a", filepath.Join(dir, "receiver-b.bundle"), "is the bundle of " + ReceiverPrincipal("receiver-b")},
	} {
		changed, err := service.HostRefresh(ctx, refusal.tenant, refusal.host, refusal.path)
		if err == nil || !strings.Contains(err.Error(), refusal.want) || changed {
			t.Fatalf("%s: refresh = %t, %v; want %q", refusal.name, changed, err, refusal.want)
		}
	}
	if again, err := os.ReadFile(path); err != nil || string(again) != string(encoded) {
		t.Fatalf("a refused refresh changed the bundle:\n%s\n%s", encoded, again)
	}
	if _, err := os.Stat(filepath.Join(dir, "missing.bundle")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a refused refresh wrote the missing bundle: %v", err)
	}
}
