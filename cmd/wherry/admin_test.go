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
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
)

func TestP11KafkaClientRejectsPlaintextListener(t *testing.T) {
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
	client, err := adminKafka("WHERRY_TRUST_ADMIN_USER", "WHERRY_TRUST_ADMIN_PASSWORD_FILE")
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

func TestRevokePrintsTheCauseOfAFailedStep(t *testing.T) {
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
	var stdout, stderr bytes.Buffer
	err := runAdmin([]string{"revoke", "receiver", "receiver-a", "--wait", "1ms"}, &stdout, &stderr)
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
