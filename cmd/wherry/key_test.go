package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/trust"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// keyHost is host-a of tenant-a on a TLS kfake cluster, initialised through wherry host init.
type keyHost struct {
	ctx      context.Context
	service  *operator.Service
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
	service := &operator.Service{Operator: connectTLS(t, address, ca, "wherry.operator", "operator-password"),
		Broker: address, DeliveryCeiling: 10, LockCeilingMS: 60000}
	terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude", Credentials: []operator.AgentCredential{{TaskCredential: wire.TaskCredential{Name: "model", Env: "MODEL_API_KEY"}}}}}, DefaultAgent: "claude",
		DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
	dir := t.TempDir()
	h := keyHost{ctx: ctx, service: service, config: filepath.Join(dir, "config.yaml"), keyPath: filepath.Join(dir, "host.key"), stateDir: filepath.Join(dir, "state")}
	bundle := filepath.Join(dir, "host.yaml")
	for _, step := range []func() error{
		func() error { return service.PoolApply(ctx, "pool-a", terms, false) },
		func() error { return service.TenantAdd(ctx, "tenant-a") },
		func() error { return service.TenantPoolAdd(ctx, "tenant-a", "pool-a") },
		func() error { return service.HostAdd(ctx, "tenant-a", "host-a", bundle, false) },
	} {
		if err := step(); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(h.config, []byte("bundle: "+bundle+"\nhost_key: "+h.keyPath+"\nstate_dir: "+h.stateDir+"\n"+caCertYAML()), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := run([]string{"host", "init", "--config-file", h.config}, &stdout, &stderr); err != nil {
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
	owner, topic := signed.HostOwner("tenant-a", "host-a"), operator.HostTopic("tenant-a", "host-a")
	records, err := readTopic(h.ctx, h.service.Operator, topic)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := operator.ReadKeys(h.ctx, h.service.Operator, map[string]string{owner: topic})
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
	if err := run([]string{"host", "rotate", "--key", "--config-file", h.config}, &stdout, &stderr); err != nil {
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
	if err := run([]string{"host", "receivers", "--config-file", h.config}, &stdout, &stderr); err != nil {
		t.Fatalf("receivers after rotation: %v", err)
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
	err := run([]string{"host", "rotate", "--key", "--config-file", h.config}, &stdout, &stderr)
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
	if err := run([]string{"host", "receivers", "--config-file", h.config}, &stdout, &stderr); err != nil {
		t.Fatalf("receivers with an unpublished next key: %v", err)
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
	if err := h.service.Operator.ProduceSync(h.ctx, &kgo.Record{Topic: operator.HostTopic("tenant-a", "host-a"), Value: rotation}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	if err := run([]string{"host", "receivers", "--config-file", h.config}, &stdout, &stderr); err != nil {
		t.Fatalf("receivers with a published next key: %v", err)
	}
	noNextKey(t, h.keyPath)
	if !h.key(t).Equal(next) {
		t.Fatal("the key file does not hold the published next key")
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
	if err := run([]string{"host", "rotate", "--key", "--config-file", h.config}, &stdout, &stderr); err == nil {
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
	service := &operator.Service{Operator: connectTLS(t, address, ca, "wherry.operator", "operator-password"),
		Broker: address, DeliveryCeiling: 10, LockCeilingMS: 60000}
	terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude", Credentials: []operator.AgentCredential{{TaskCredential: wire.TaskCredential{Name: "model", Env: "MODEL_API_KEY"}}}}}, DefaultAgent: "claude",
		DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
	if err := service.PoolApply(ctx, "pool-a", terms, false); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	bundle, stateDir := filepath.Join(dir, "receiver.yaml"), filepath.Join(dir, "state")
	keyPath := filepath.Join(stateDir, "signing.key")
	config := operator.ReceiverConfig{Receiver: "receiver-a", Pool: "pool-a",
		Profiles: []operator.ProfileSettings{{Name: "fake", Plugin: "fake", Agent: "claude"}}, PluginSHA256: map[string]string{"fake": fakeSHA256}}
	if err := service.ReceiverAdd(ctx, config, service.Broker, bundle); err != nil {
		t.Fatal(err)
	}
	topic := operator.ReceiverTopic("receiver-a")
	folded := func() ([]*kgo.Record, signed.KeyState) {
		t.Helper()
		records, err := readTopic(ctx, service.Operator, topic)
		if err != nil {
			t.Fatal(err)
		}
		keys, err := operator.ReadKeys(ctx, service.Operator, map[string]string{"receiver-a": topic})
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
	localConfig := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(localConfig, []byte("bundle: "+bundle+"\nstate_dir: "+stateDir+"\nplugin_dir: "+t.TempDir()+"\n"+caCertYAML()), 0o600); err != nil {
		t.Fatal(err)
	}
	// With no plugin the receiver stops at configuration, which comes before it advertises or claims anything.
	start := func(extra ...string) string {
		t.Helper()
		var stdout, stderr bytes.Buffer
		args := append([]string{"receiver", "run", "--config-file", localConfig}, extra...)
		if err := run(args, &stdout, &stderr); err == nil || err.Error() != "profile fake: no plugin for type fake" {
			t.Fatalf("receiver run %v: %v; want it to stop at configuration", extra, err)
		}
		return stdout.String()
	}

	rotate := func() (string, error) {
		t.Helper()
		var stdout bytes.Buffer
		err := run([]string{"receiver", "rotate", "--key", "--config-file", localConfig}, &stdout, io.Discard)
		return stdout.String(), err
	}
	// A receiver that has never started has an empty key topic and nothing to rotate.
	if _, err := rotate(); err == nil || err.Error() != "receiver has never started, so it has no key to rotate: its first start registers one" {
		t.Fatalf("rotate before the first start = %v; want it refused", err)
	}
	if records, _ := folded(); len(records) != 0 {
		t.Fatalf("a refused rotation left %d records on the key topic", len(records))
	}
	start()
	key := fileKey()
	if records, state := folded(); len(records) != 1 || !state.Current.Equal(key.Public()) {
		t.Fatalf("key topic of %d records after the first start; want the registration alone", len(records))
	}
	// A running receiver holds the state directory, so a rotation beside it is refused and changes nothing.
	held, err := os.Open(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(held.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	if _, err := rotate(); err == nil || !strings.Contains(err.Error(), "state directory "+stateDir+" is held by another receiver process") {
		t.Fatalf("rotate beside a held state directory = %v; want it refused", err)
	}
	held.Close()
	if records, _ := folded(); len(records) != 1 || !fileKey().Equal(key) {
		t.Fatal("a refused rotation rotated the key")
	}
	out, err := rotate()
	if err != nil {
		t.Fatal(err)
	}
	records, state := folded()
	next := fileKey()
	if len(records) != 2 || !state.Current.Equal(next.Public()) || len(state.Retired) != 1 || !state.Retired[0].Key.Equal(key.Public()) {
		t.Fatalf("key topic of %d records; want a registration and a rotation to the state directory's key", len(records))
	}
	if want := "old_key_id=" + keyID(key) + " new_key_id=" + keyID(next) + "\n"; out != want {
		t.Fatalf("rotate printed %q; want %q", out, want)
	}
	noNextKey(t, keyPath)
	key = next

	// A start never rotates, and settles a next key nothing published by removing it.
	if _, err := trust.GenerateSigningKeyFile(keyPath + ".next"); err != nil {
		t.Fatal(err)
	}
	start()
	noNextKey(t, keyPath)
	if records, _ := folded(); len(records) != 2 || !fileKey().Equal(key) {
		t.Fatal("a start rotated the key")
	}

	// It settles a published next key by renaming it over the key file.
	next, err = trust.GenerateSigningKeyFile(keyPath + ".next")
	if err != nil {
		t.Fatal(err)
	}
	rotation, err := signed.RotateKey("receiver-a", key, next)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Operator.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: rotation}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	start()
	noNextKey(t, keyPath)
	if !fileKey().Equal(next) {
		t.Fatal("the key file does not hold the published next key")
	}
}
