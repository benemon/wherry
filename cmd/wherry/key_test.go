package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/trust"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// keyHost is host-a of tenant-a on a TLS kfake cluster, initialised through wherry init.
type keyHost struct {
	ctx      context.Context
	service  *admin.Service
	config   string
	keyPath  string
	stateDir string
}

func newKeyHost(t *testing.T) keyHost {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	cluster, ca := tlsCluster(t)
	address := cluster.ListenAddrs()[0]
	service := &admin.Service{Admin: connectTLS(t, address, ca, "admin", "password"), Operator: connectTLS(t, address, ca, "wherry.operator", "operator-password"),
		Broker: address, CA: ca, DeliveryCeiling: 10}
	terms := admin.Terms{Agents: []admin.AgentTerms{{Name: "claude", Credentials: []admin.AgentCredential{{TaskCredential: wire.TaskCredential{Name: "model", Env: "MODEL_API_KEY"}}}}}, DefaultAgent: "claude",
		DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
	dir := t.TempDir()
	h := keyHost{ctx: ctx, service: service, config: filepath.Join(dir, "config.yaml"), keyPath: filepath.Join(dir, "host.key"), stateDir: filepath.Join(dir, "state")}
	bundle := filepath.Join(dir, "host.yaml")
	for _, step := range []func() error{
		func() error { return service.PoolCreate(ctx, "pool-a", terms) },
		func() error { return service.TenantAdd(ctx, "tenant-a", "pool-a") },
		func() error { return service.HostAdd(ctx, "tenant-a", "host-a", bundle) },
	} {
		if err := step(); err != nil {
			t.Fatal(err)
		}
	}
	resalt(t, service, bundle)
	if err := os.WriteFile(h.config, []byte("bundle: "+bundle+"\nhost_key: "+h.keyPath+"\nstate_dir: "+h.stateDir+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := run([]string{"init", "--config-file", h.config}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	return h
}

func (h keyHost) key(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	key, err := trust.LoadSigningKeyFile(h.keyPath)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

// topic returns the host's key topic records and its folded state.
func (h keyHost) topic(t *testing.T) ([]*kgo.Record, signed.KeyState) {
	t.Helper()
	owner, topic := signed.HostOwner("tenant-a", "host-a"), admin.HostTopic("tenant-a", "host-a")
	records, err := admin.ReadTopic(h.ctx, h.service.Admin, topic)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := admin.ReadKeys(h.ctx, h.service.Admin, map[string]string{owner: topic})
	if err != nil {
		t.Fatal(err)
	}
	return records, keys[owner]
}

func keyID(key ed25519.PrivateKey) string {
	return signed.KeyID(key.Public().(ed25519.PublicKey))
}

func noNextKey(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path + ".next"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("%s.next is still there: %v", path, err)
	}
}

func TestKeyRotateReplacesTheHostKey(t *testing.T) {
	h := newKeyHost(t)
	old := h.key(t)
	var stdout, stderr bytes.Buffer
	if err := run([]string{"key", "rotate", "--config-file", h.config}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	next := h.key(t)
	if next.Equal(old) {
		t.Fatal("the key file still holds the old key")
	}
	noNextKey(t, h.keyPath)
	records, state := h.topic(t)
	if !state.Current.Equal(next.Public()) || len(records) != 2 {
		t.Fatalf("key topic of %d records names %s current; want the rotation to the key file's key", len(records), signed.KeyID(state.Current))
	}
	if want := "old_key_id=" + keyID(old) + " new_key_id=" + keyID(next) + "\n"; stdout.String() != want {
		t.Fatalf("stdout %q, want %q", stdout.String(), want)
	}
	// A command after a finished rotation changes nothing.
	stdout.Reset()
	if err := run([]string{"key", "--config-file", h.config}, &stdout, &stderr); err != nil || stdout.String() != keyID(next)+"\n" {
		t.Fatalf("key after rotation: %v, stdout %q", err, stdout.String())
	}
	if records, _ := h.topic(t); len(records) != 2 || !h.key(t).Equal(next) {
		t.Fatal("a command after the rotation changed the key or its topic")
	}
}

func TestKeyRotateRefusesWhileARunIsUnfinished(t *testing.T) {
	h := newKeyHost(t)
	old := h.key(t)
	if err := os.MkdirAll(filepath.Join(h.stateDir, "runs"), 0o700); err != nil {
		t.Fatal(err)
	}
	record := `{"run":"run-open","pool":"pool-a","placement":{"agent":"claude","selector":[]},"task_hash":"x"}`
	if err := os.WriteFile(filepath.Join(h.stateDir, "runs", "run-open.json"), []byte(record), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	err := run([]string{"key", "rotate", "--config-file", h.config}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "run-open") || !strings.Contains(err.Error(), "MCP server") {
		t.Fatalf("rotate with an unfinished run: %v; want a refusal naming it", err)
	}
	noNextKey(t, h.keyPath)
	if records, state := h.topic(t); len(records) != 1 || !state.Current.Equal(old.Public()) || !h.key(t).Equal(old) || stdout.Len() != 0 {
		t.Fatal("a refused rotation changed the key, its topic or printed key IDs")
	}
}

func TestHostCommandSettlesALeftoverNextKey(t *testing.T) {
	h := newKeyHost(t)
	old := h.key(t)
	var stdout, stderr bytes.Buffer

	// The next key was written and nothing was published.
	if _, err := trust.GenerateSigningKeyFile(h.keyPath + ".next"); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"key", "--config-file", h.config}, &stdout, &stderr); err != nil || stdout.String() != keyID(old)+"\n" {
		t.Fatalf("key with an unpublished next key: %v, stdout %q", err, stdout.String())
	}
	noNextKey(t, h.keyPath)
	if !h.key(t).Equal(old) {
		t.Fatal("an unpublished next key replaced the key file")
	}

	// The rotation was published and the rename not done.
	next, err := trust.GenerateSigningKeyFile(h.keyPath + ".next")
	if err != nil {
		t.Fatal(err)
	}
	rotation, err := signed.RotateKey(signed.HostOwner("tenant-a", "host-a"), old, next)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.service.Admin.ProduceSync(h.ctx, &kgo.Record{Topic: admin.HostTopic("tenant-a", "host-a"), Value: rotation}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	if err := run([]string{"receivers", "--config-file", h.config}, &stdout, &stderr); err != nil {
		t.Fatalf("receivers with a published next key: %v", err)
	}
	noNextKey(t, h.keyPath)
	if !h.key(t).Equal(next) {
		t.Fatal("the key file does not hold the published next key")
	}
	stdout.Reset()
	if err := run([]string{"key", "--config-file", h.config}, &stdout, &stderr); err != nil || stdout.String() != keyID(next)+"\n" {
		t.Fatalf("key after recovery: %v, stdout %q", err, stdout.String())
	}
}

func TestKeyRotateKeepsTheKeyFileWhenTheTopicRefusesTheRotation(t *testing.T) {
	h := newKeyHost(t)
	// A key file whose key is not the topic's current key signs a rotation the topic does not fold.
	if err := os.Remove(h.keyPath); err != nil {
		t.Fatal(err)
	}
	stray, err := trust.GenerateSigningKeyFile(h.keyPath)
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := run([]string{"key", "rotate", "--config-file", h.config}, &stdout, &stderr); err == nil {
		t.Fatal("rotation the key topic refused reported success")
	}
	if !h.key(t).Equal(stray) || stdout.Len() != 0 {
		t.Fatal("the key file was replaced before the key topic named the next key current")
	}
}

func TestReceiverRotateKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cluster, ca := tlsCluster(t)
	address := cluster.ListenAddrs()[0]
	service := &admin.Service{Admin: connectTLS(t, address, ca, "admin", "password"), Operator: connectTLS(t, address, ca, "wherry.operator", "operator-password"),
		Broker: address, CA: ca, DeliveryCeiling: 10}
	terms := admin.Terms{Agents: []admin.AgentTerms{{Name: "claude", Credentials: []admin.AgentCredential{{TaskCredential: wire.TaskCredential{Name: "model", Env: "MODEL_API_KEY"}}}}}, DefaultAgent: "claude",
		DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
	if err := service.PoolCreate(ctx, "pool-a", terms); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	bundle, stateDir := filepath.Join(dir, "receiver.yaml"), filepath.Join(dir, "state")
	keyPath := filepath.Join(stateDir, "signing.key")
	config := admin.ReceiverConfig{Receiver: "receiver-a", Pool: "pool-a", Capacity: 1,
		Adapters: []admin.AdapterSettings{{Name: "fake", Platform: "fake-platform", Type: "fake", Agent: "claude"}}}
	if err := service.ReceiverAdd(ctx, config, map[string]string{"fake-platform": platformText}, bundle); err != nil {
		t.Fatal(err)
	}
	resalt(t, service, bundle)
	topic := admin.ReceiverTopic("receiver-a")
	folded := func() ([]*kgo.Record, signed.KeyState) {
		t.Helper()
		records, err := admin.ReadTopic(ctx, service.Admin, topic)
		if err != nil {
			t.Fatal(err)
		}
		keys, err := admin.ReadKeys(ctx, service.Admin, map[string]string{"receiver-a": topic})
		if err != nil {
			t.Fatal(err)
		}
		return records, keys["receiver-a"]
	}
	fileKey := func() ed25519.PrivateKey {
		t.Helper()
		key, err := trust.LoadSigningKeyFile(keyPath)
		if err != nil {
			t.Fatal(err)
		}
		return key
	}
	// With no plugin the receiver stops at configuration, which comes before it advertises or claims anything.
	start := func(extra ...string) string {
		t.Helper()
		var stdout, stderr bytes.Buffer
		args := append([]string{"receiver", "run", "--bundle-file", bundle, "--state-dir", stateDir, "--plugin-dir", t.TempDir()}, extra...)
		if err := run(args, &stdout, &stderr); err == nil || err.Error() != "adapter fake: no plugin for type fake" {
			t.Fatalf("receiver run %v: %v; want it to stop at configuration", extra, err)
		}
		return stdout.String()
	}

	logs := start("--rotate-key")
	records, state := folded()
	key := fileKey()
	if len(records) != 2 || !state.Current.Equal(key.Public()) || len(state.Retired) != 1 {
		t.Fatalf("key topic of %d records; want a registration and a rotation to the state directory's key", len(records))
	}
	if want := "rotated key: old=" + signed.KeyID(state.Retired[0].Key) + " new=" + keyID(key); !strings.Contains(logs, want) || strings.Contains(logs, "configured:") {
		t.Fatalf("logs %q; want %q before configuration", logs, want)
	}
	noNextKey(t, keyPath)

	// Without the flag a start never rotates, and settles a next key nothing published by removing it.
	if _, err := trust.GenerateSigningKeyFile(keyPath + ".next"); err != nil {
		t.Fatal(err)
	}
	start()
	noNextKey(t, keyPath)
	if records, _ := folded(); len(records) != 2 || !fileKey().Equal(key) {
		t.Fatal("a start without --rotate-key rotated the key")
	}

	// It settles a published next key by renaming it over the key file.
	next, err := trust.GenerateSigningKeyFile(keyPath + ".next")
	if err != nil {
		t.Fatal(err)
	}
	rotation, err := signed.RotateKey("receiver-a", key, next)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Admin.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: rotation}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	start()
	noNextKey(t, keyPath)
	if !fileKey().Equal(next) {
		t.Fatal("the key file does not hold the published next key")
	}
}
