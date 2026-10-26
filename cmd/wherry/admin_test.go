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
	"io"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
)

func TestKafkaClientRejectsPlaintextListener(t *testing.T) {
	cluster := kfake.MustCluster(kfake.EnableSASL(), kfake.EnableACLs(), kfake.Superuser("SCRAM-SHA-512", "admin", "password"))
	defer cluster.Close()
	dir := t.TempDir()
	passwordPath := filepath.Join(dir, "password")
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(passwordPath, []byte("password"), 0o600); err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WHERRY_TRUST_BROKER", cluster.ListenAddrs()[0])
	t.Setenv("WHERRY_TRUST_CA", caPath)
	t.Setenv("WHERRY_TRUST_ADMIN_USER", "admin")
	t.Setenv("WHERRY_TRUST_ADMIN_PASSWORD_FILE", passwordPath)
	client, err := adminKafka(cluster.ListenAddrs()[0], pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), "WHERRY_TRUST_ADMIN_USER", "", "WHERRY_TRUST_ADMIN_PASSWORD_FILE")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx); err == nil {
		t.Fatal("TLS client accepted a plaintext Kafka listener")
	}
}

// adminEnv points runAdmin at a TLS fake broker and returns an admin service on it.
func adminEnv(t *testing.T) *admin.Service {
	cluster, ca := tlsCluster(t)
	dir := t.TempDir()
	files := map[string]string{"ca.pem": ca, "admin": "password", "operator": "operator-password"}
	for name, contents := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("WHERRY_TRUST_BROKER", cluster.ListenAddrs()[0])
	t.Setenv("WHERRY_TRUST_CA", filepath.Join(dir, "ca.pem"))
	t.Setenv("WHERRY_TRUST_ADMIN_USER", "admin")
	t.Setenv("WHERRY_TRUST_ADMIN_PASSWORD_FILE", filepath.Join(dir, "admin"))
	t.Setenv("WHERRY_TRUST_OPERATOR_USER", "wherry.operator")
	t.Setenv("WHERRY_TRUST_OPERATOR_PASSWORD_FILE", filepath.Join(dir, "operator"))
	address := cluster.ListenAddrs()[0]
	return &admin.Service{Admin: connectTLS(t, address, ca, "admin", "password"), Operator: connectTLS(t, address, ca, "wherry.operator", "operator-password"),
		Broker: address, CA: ca, DeliveryCeiling: 10}
}

func TestAdminCommandUsage(t *testing.T) {
	adminEnv(t)
	bundle := filepath.Join(t.TempDir(), "host.bundle")
	for _, test := range []struct {
		args []string
		want string
	}{
		{[]string{"host", "add", "host-a", "--bundle-file", bundle}, "usage: wherry admin host add HOST --tenant TENANT"},
		{[]string{"host", "add", "tenant-a", "host-a", "--bundle-file", bundle}, "usage: wherry admin host add HOST --tenant TENANT"},
		{[]string{"host", "add", "tenant-a", "host-a", "--tenant", "tenant-a", "--bundle-file", bundle}, "usage: wherry admin host add HOST --tenant TENANT"},
		{[]string{"host", "add", "host-a", "--tenant", "tenant-a"}, "usage: wherry admin host add HOST --tenant TENANT"},
		{[]string{"host", "revoke", "host-a"}, "usage: wherry admin host revoke HOST --tenant TENANT"},
		{[]string{"host", "revoke", "--tenant", "tenant-a"}, "usage: wherry admin host revoke HOST --tenant TENANT"},
		{[]string{"receiver", "revoke"}, "usage: wherry admin receiver revoke RECEIVER"},
		{[]string{"receiver", "revoke", "receiver-a", "receiver-b"}, "usage: wherry admin receiver revoke RECEIVER"},
		{[]string{"pool", "entitle", "build"}, "usage: wherry admin pool entitle POOL --tenant TENANT"},
		{[]string{"pool", "entitle", "--tenant", "tenant-a"}, "usage: wherry admin pool entitle POOL --tenant TENANT"},
		{[]string{"tenant", "add", "tenant-a", "--pool", "build"}, "unknown admin command"},
		{[]string{"revoke", "host", "tenant-a", "host-a"}, "unknown admin command"},
		{[]string{"revoke", "receiver", "receiver-a"}, "unknown admin command"},
	} {
		var stdout, stderr bytes.Buffer
		if err := runAdmin(test.args, &stdout, &stderr); err == nil || !strings.HasPrefix(err.Error(), test.want) {
			t.Errorf("%q = %v, want %q", test.args, err, test.want)
		}
	}
}

func TestRevokePrintsTheCauseOfAFailedStep(t *testing.T) {
	adminEnv(t)
	var stdout, stderr bytes.Buffer
	err := runAdmin([]string{"receiver", "revoke", "receiver-a", "--wait", "1ms"}, &stdout, &stderr)
	if err == nil || err.Error() != "revocation incomplete" {
		t.Fatalf("revoke = %v, want revocation incomplete", err)
	}
	lines := strings.Split(strings.TrimSuffix(stdout.String(), "\n"), "\n")
	last := lines[len(lines)-1]
	if !strings.Contains(last, "complete=false") || !strings.Contains(last, " error=") || strings.HasSuffix(last, " error=") {
		t.Fatalf("last step line %q, want the failed step with its cause", last)
	}
	for _, line := range lines[:len(lines)-1] {
		if !strings.Contains(line, "complete=true") || strings.Contains(line, "error=") {
			t.Fatalf("step line %q, want completed steps without a cause", line)
		}
	}
}

// receiver show prints the recorded configuration; configure --dry-run records nothing and prints what recording the
// file would store, as show prints it once recorded.
// The record decodes numbers as float64; show prints a whole one as an integer.
func TestReceiverShowPrintsWholeNumbersAsIntegers(t *testing.T) {
	var out bytes.Buffer
	config := admin.ReceiverConfig{Capacity: 1, Profiles: []admin.ProfileSettings{{Name: "p", Plugin: "nomad", Agent: "claude",
		Config: map[string]any{"memory_mb": float64(1 << 20), "ratio": 0.5, "sizes": []any{float64(2048000)}}}}}
	if err := showReceiver(&out, config); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"memory_mb: 1048576", "ratio: 0.5", "- 2048000"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("show printed\n%s\nwant %q", out.String(), want)
		}
	}
}

func TestReceiverShowAndDryRun(t *testing.T) {
	service := adminEnv(t)
	ctx := context.Background()
	terms := admin.Terms{Agents: []admin.AgentTerms{{Name: "claude"}}, DefaultAgent: "claude", DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
	if err := service.PoolCreate(ctx, "pool-a", terms); err != nil {
		t.Fatal(err)
	}
	recorded := admin.ReceiverConfig{Receiver: "receiver-a", Pool: "pool-a", Capacity: 3, Profiles: []admin.ProfileSettings{{Name: "old", Plugin: "nomad", Agent: "claude"}}}
	if err := service.ReceiverAdd(ctx, recorded, filepath.Join(t.TempDir(), "bundle.yaml")); err != nil {
		t.Fatal(err)
	}
	records := func() int64 {
		t.Helper()
		offsets, err := kadm.NewClient(service.Admin).ListEndOffsets(ctx, admin.AdminTopic)
		if err != nil || offsets.Error() != nil {
			t.Fatal(err, offsets.Error())
		}
		offset, _ := offsets.Lookup(admin.AdminTopic, 0)
		return offset.Offset
	}
	wherry := func(args ...string) string {
		t.Helper()
		var stdout bytes.Buffer
		if err := runAdmin(args, &stdout, io.Discard); err != nil {
			t.Fatalf("%q: %v", args, err)
		}
		return stdout.String()
	}
	const shown = `capacity: 3
profiles:
    - name: old
      plugin: nomad
      agent: claude
      labels: {}
      config: {}
`
	if got := wherry("receiver", "show", "receiver-a"); got != shown {
		t.Fatalf("show printed\n%s\nwant\n%s", got, shown)
	}
	file := filepath.Join(writeTree(t, map[string]string{"receiver.yaml": basePlugin + item("name: claude\nagent: claude\nlabels: {tier: batch}\nconfig: {runtime: {config: {image: claude:1}}}\n", "      ")}), "receiver.yaml")
	before := records()
	dryRun := wherry("receiver", "configure", "receiver-a", "--config-file", file, "--dry-run")
	if records() != before {
		t.Fatal("configure --dry-run produced a record")
	}
	const resolved = `capacity: 1
profiles:
    - name: claude
      plugin: nomad
      agent: claude
      labels:
        platform: nomad
        tier: batch
      config:
        address: https://nomad.example:4646
        credential: nomad-token
        runtime:
            config:
                args:
                    - a
                    - b
                image: claude:1
            driver: docker
            resources:
                cpu: 1000
`
	if dryRun != resolved {
		t.Fatalf("dry run printed\n%s\nwant\n%s", dryRun, resolved)
	}
	if got := wherry("receiver", "add", "receiver-b", "--pool", "pool-a", "--config-file", file, "--dry-run"); got != resolved || records() != before {
		t.Fatalf("receiver add --dry-run printed\n%s\nand left %d records; want the resolution and %d records", got, records(), before)
	}
	if err := runAdmin([]string{"receiver", "show", "receiver-b"}, io.Discard, io.Discard); err == nil {
		t.Fatal("receiver add --dry-run recorded receiver-b")
	}
	if got := wherry("receiver", "show", "receiver-a"); got != shown {
		t.Fatalf("after a dry run, show printed\n%s\nwant the recorded configuration", got)
	}
	outside := filepath.Join(writeTree(t, map[string]string{"receiver.yaml": basePlugin + item("name: codex\nagent: codex\n", "      ")}), "receiver.yaml")
	if err := runAdmin([]string{"receiver", "configure", "receiver-a", "--config-file", outside, "--dry-run"}, io.Discard, io.Discard); err == nil || err.Error() != "receiver agent is outside pool terms" {
		t.Fatalf("dry run of an agent outside the pool's terms: %v; want it refused", err)
	}
	if got := wherry("receiver", "configure", "receiver-a", "--config-file", file); got != "" || records() != before+1 {
		t.Fatalf("configure printed %q and left %d records; want nothing printed and one record more than %d", got, records(), before)
	}
	if got := wherry("receiver", "show", "receiver-a"); got != dryRun {
		t.Fatalf("show printed\n%s\nwant what the dry run printed\n%s", got, dryRun)
	}
}
