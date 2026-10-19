package main

import (
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
