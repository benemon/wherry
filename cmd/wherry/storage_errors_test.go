package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/minio/minio-go/v7"
	"github.com/twmb/franz-go/pkg/kgo"
)

// refusingS3 is a synthetic S3 store that answers every signed request with InvalidAccessKeyId naming the access key
// that signed it, as a store does for a key it does not know: its URL and its CA's PEM, which the host's ca_cert holds as
// a production store's CA would be.
func refusingS3(t *testing.T) (string, string) {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		if r.Method == http.MethodGet && r.URL.Query().Has("location") {
			fmt.Fprint(w, "<LocationConstraint></LocationConstraint>")
			return
		}
		_, signer, _ := strings.Cut(r.Header.Get("Authorization"), "Credential=")
		access, _, _ := strings.Cut(signer, "/")
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprintf(w, "<Error><Code>InvalidAccessKeyId</Code><Message>no access key %s</Message></Error>", access)
	}))
	t.Cleanup(server.Close)
	return server.URL, serverCA(server)
}

// A store that refuses an upload naming the access key that signed it leaves neither storage key in the error submit
// returns, which main prints, nor in the host's log of the cleanup that follows, for the pair the host's bundle carries
// and for a pair its configuration maps the pool to alike: both values show as their markers, and the error still
// unwraps to the store's refusal.
func TestSubmitStorageErrorAndLogShowMarkersForTheStorageKeys(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	endpoint, storeCA := refusingS3(t)
	cluster, ca := tlsCluster(t)
	address := cluster.ListenAddrs()[0]
	service := &operator.Service{Operator: connectTLS(t, address, ca, "wherry.operator", "operator-password"),
		Broker: address, DeliveryCeiling: 10, LockCeilingMS: 60000}
	bundled := operator.StorageKeys{AccessKey: "AKIABUNDLEDACCESSKEY", SecretKey: "bundled-secret-key-value-sentinel"}
	mapped := operator.StorageKeys{AccessKey: "AKIAMAPPEDACCESSKEY", SecretKey: "mapped-secret-key-value-sentinel"}
	service.Storage = func(string) (operator.StorageKeys, error) { return bundled, nil }
	terms := storageKeysTerms()
	terms.Storage.Endpoint = endpoint
	dir := t.TempDir()
	hostA, hostB, receiverBundle := filepath.Join(dir, "host-a.yaml"), filepath.Join(dir, "host-b.yaml"), filepath.Join(dir, "receiver.yaml")
	config := operator.ReceiverConfig{Receiver: "receiver-a", Pool: "pool-a", Profiles: []operator.ProfileSettings{{Name: "lab", Plugin: "nomad", Agent: "claude", Labels: map[string]string{"runtime": "exec"}}},
		PluginSHA256: map[string]string{"nomad": fakeSHA256}}
	for _, step := range []func() error{
		func() error { return service.PoolApply(ctx, "pool-a", terms, false) },
		func() error { return service.TenantAdd(ctx, "tenant-a") },
		func() error { return service.TenantPoolAdd(ctx, "tenant-a", "pool-a") },
		func() error { return service.HostAdd(ctx, "tenant-a", "host-a", hostA) },
		func() error { return service.HostAdd(ctx, "tenant-a", "host-b", hostB) },
		func() error { return service.ReceiverAdd(ctx, config, service.Broker, receiverBundle) },
	} {
		if err := step(); err != nil {
			t.Fatal(err)
		}
	}
	// The receiver is live: its key is registered, it advertises claude and it holds the pool key.
	receiver, err := operator.ReadBundle(receiverBundle)
	if err != nil {
		t.Fatal(err)
	}
	kafka := connectTLS(t, address, ca, receiver.Principal, receiver.Password)
	_, receiverKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	registration, err := signed.RegisterKey("receiver-a", receiverKey)
	if err != nil {
		t.Fatal(err)
	}
	advertisement, err := wire.SignRecord(&wire.Advertisement{Receiver: "receiver-a", Pool: "pool-a",
		Offers: []wire.Offer{{Profile: "lab", Agent: "claude", InlineLimit: 49152}}, Capacity: 1, AtMS: time.Now().UnixMilli()}, receiverKey)
	if err != nil {
		t.Fatal(err)
	}
	for topic, value := range map[string][]byte{operator.ReceiverTopic("receiver-a"): registration, operator.PoolReceiversTopic("pool-a"): advertisement} {
		if err := kafka.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte("receiver-a"), Value: value}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	publishSnapshot(t, ctx, kafka, "pool-a", "receiver-a", receiverKey)

	for name, value := range map[string]string{"s3-access": mapped.AccessKey, "s3-secret": mapped.SecretKey} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// One byte over profile lab's inline limit, so submit carries the input through S3.
	data := filepath.Join(dir, "dataset.bin")
	if err := os.WriteFile(data, make([]byte, 49153), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		bundle     string
		storage    map[string]busclient.StorageConfig
		keys       operator.StorageKeys
		accessName string
	}{
		{"bundled pair", hostA, nil, bundled, "bundle-access-key"},
		{"configured pair", hostB, map[string]busclient.StorageConfig{"pool-a": {AccessKey: "s3-access", SecretKey: "s3-secret"}}, mapped, "s3-access"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hostDir := t.TempDir()
			hostConfig := busclient.Config{Bundle: tc.bundle, HostKey: filepath.Join(hostDir, "host.key"), StateDir: filepath.Join(hostDir, "state"), Storage: tc.storage, CACert: caCert(storeCA),
				Credentials: map[string]credential.Source{"s3-access": {File: filepath.Join(dir, "s3-access")}, "s3-secret": {File: filepath.Join(dir, "s3-secret")}}}
			if _, _, err := busclient.Init(ctx, hostConfig); err != nil {
				t.Fatal(err)
			}
			logs := &syncBuffer{}
			host, err := busclient.Connect(hostConfig, log.New(logs, "", 0))
			if err != nil {
				t.Fatal(err)
			}
			defer host.Close()
			var stdout, stderr bytes.Buffer
			err = runSubmit(ctx, host, []string{"--prompt", "x", "--input", data}, &stdout, &stderr)
			marker := signed.RedactionMarker(nil, tc.accessName)
			if want := "upload S3 input dataset.bin: no access key " + marker; err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("submit = %v; want %q", err, want)
			}
			var refusal minio.ErrorResponse
			if !errors.As(err, &refusal) || refusal.Code != "InvalidAccessKeyId" {
				t.Fatalf("submit = %v; want it to unwrap to the store's InvalidAccessKeyId", err)
			}
			if want := "delete the S3 inputs of a task not published: no access key " + marker; !strings.Contains(logs.String(), want) {
				t.Fatalf("host log\n%s\nwant %q", logs.String(), want)
			}
			for _, value := range []string{bundled.AccessKey, bundled.SecretKey, mapped.AccessKey, mapped.SecretKey} {
				for what, text := range map[string]string{"the submit error": err.Error(), "the host log": logs.String(), "stdout": stdout.String(), "stderr": stderr.String()} {
					if strings.Contains(text, value) {
						t.Fatalf("%s holds the storage key %q:\n%s", what, value, text)
					}
				}
			}
		})
	}
}
