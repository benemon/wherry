package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/trust"
	"github.com/benemon/wherry/internal/wire"
)

// initHost is host-a of tenant-a on a TLS kfake cluster, added but not initialised, its bundle where the operator
// wrote it. HOME is a temporary directory without XDG variables, and its ~/.config/wherry/ca.pem trusts the cluster.
type initHost struct {
	ctx     context.Context
	service *operator.Service
	bundle  string
	dir     string
	config  string
}

func newInitHost(t *testing.T, pools map[string]operator.Terms) initHost {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	cluster, ca := tlsCluster(t)
	address := cluster.ListenAddrs()[0]
	service := &operator.Service{Operator: connectTLS(t, address, ca, "wherry.operator", "operator-password"),
		Broker: address, CA: []byte(ca), DeliveryCeiling: 10, LockCeilingMS: 60000}
	home := t.TempDir()
	t.Setenv("HOME", home)
	for _, variable := range []string{"XDG_CONFIG_HOME", "XDG_STATE_HOME", "WHERRY_CONFIG"} {
		t.Setenv(variable, "")
	}
	dir := filepath.Join(home, ".config", "wherry")
	h := initHost{ctx: ctx, service: service, bundle: filepath.Join(t.TempDir(), "host-a.bundle"), dir: dir, config: filepath.Join(dir, "config.yaml")}
	if err := service.TenantAdd(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	for pool, terms := range pools {
		h.entitle(t, pool, terms)
	}
	if err := service.HostAdd(ctx, "tenant-a", "host-a", h.bundle, false); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ca.pem"), []byte(ca), 0o644); err != nil {
		t.Fatal(err)
	}
	return h
}

func (h initHost) entitle(t *testing.T, pool string, terms operator.Terms) {
	t.Helper()
	if err := h.service.PoolApply(h.ctx, pool, terms, false); err != nil {
		t.Fatal(err)
	}
	if err := h.service.TenantPoolAdd(h.ctx, "tenant-a", pool); err != nil {
		t.Fatal(err)
	}
}

// init runs wherry host init with args, returning its stdout and its prompts.
func (h initHost) init(t *testing.T, args ...string) (string, string) {
	t.Helper()
	var stdout, prompts bytes.Buffer
	if err := run(append([]string{"host", "init"}, args...), &stdout, &prompts); err != nil {
		t.Fatalf("host init: %v\n%s", err, prompts.String())
	}
	return stdout.String(), prompts.String()
}

// written is the config init wrote, as written.
func (h initHost) written(t *testing.T) busclient.Config {
	t.Helper()
	var config busclient.Config
	if err := readYAML(h.config, &config); err != nil {
		t.Fatal(err)
	}
	return config
}

// registered fails t unless the host key topic's current key is the key in the host key file, whose ID is keyID.
func (h initHost) registered(t *testing.T, keyID string) {
	t.Helper()
	h.registeredAt(t, "host-a", filepath.Join(h.dir, "host.key"), keyID)
}

// registeredAt fails t unless host's key topic's current key is the key in the file at path, whose ID is keyID.
func (h initHost) registeredAt(t *testing.T, host, path, keyID string) {
	t.Helper()
	key, err := trust.LoadSigningKeyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	owner := signed.HostOwner("tenant-a", host)
	keys, err := operator.ReadKeys(h.ctx, h.service.Operator, map[string]string{owner: operator.HostTopic("tenant-a", host)})
	if err != nil {
		t.Fatal(err)
	}
	if current := keys[owner].Current; current == nil || !current.Equal(key.Public()) || signed.KeyID(current) != keyID {
		t.Fatalf("host %s's key topic holds %v; want the host key %s registered", host, current, keyID)
	}
}

// addHost adds host to tenant-a and returns the bundle the operator wrote.
func (h initHost) addHost(t *testing.T, host string) string {
	t.Helper()
	bundle := filepath.Join(t.TempDir(), host+".bundle")
	if err := h.service.HostAdd(h.ctx, "tenant-a", host, bundle, false); err != nil {
		t.Fatal(err)
	}
	return bundle
}

// absent fails t when a file exists at path.
func absent(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("%s exists (%v); want it absent", path, err)
	}
}

func agentTerms(credentials ...operator.AgentCredential) operator.Terms {
	return operator.Terms{Agents: []operator.AgentTerms{{Name: "claude", Credentials: credentials}}, DefaultAgent: "claude",
		DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 10}
}

func envCredential(name, variable string) operator.AgentCredential {
	return operator.AgentCredential{TaskCredential: wire.TaskCredential{Name: name, Env: variable}}
}

func fileCredential(name, file string) operator.AgentCredential {
	optional := false
	return operator.AgentCredential{TaskCredential: wire.TaskCredential{Name: name, File: file}, Required: &optional}
}

// Without a terminal init asks nothing, suggests nothing, writes the config and lists what still has no source.
func TestHostInitWithoutATerminal(t *testing.T) {
	t.Setenv("MODEL_API_KEY", "model-key-value")
	terms := agentTerms(envCredential("model", "MODEL_API_KEY"))
	terms.VCS = []string{"git.example.test"}
	terms.Storage = &operator.Storage{Endpoint: "https://s3.example.test", Bucket: "bucket-a"}
	h := newInitHost(t, map[string]operator.Terms{"pool-a": terms})
	noTerminal(t)
	stdout, prompts := h.init(t, h.bundle)
	h.registered(t, strings.TrimSpace(stdout))
	for _, asked := range []string{"Source for", "found MODEL_API_KEY", "Git URL prefix", "Credential holding", "CA file"} {
		if strings.Contains(prompts, asked) {
			t.Errorf("init without a terminal printed\n%s\nwant nothing asked", prompts)
		}
	}
	for _, want := range []string{"credential model (pool pool-a as MODEL_API_KEY, required)", "git host git.example.test", "storage of pool pool-a"} {
		if !strings.Contains(prompts, want) {
			t.Errorf("init without a terminal printed\n%s\nwant %q listed", prompts, want)
		}
	}
	if config := h.written(t); config.Bundle != filepath.Join(h.dir, "bundle.yaml") || len(config.Credentials)+len(config.Git)+len(config.Storage) != 0 {
		t.Fatalf("config %+v; want the bundle alone", config)
	}
}

// init makes an existing config it writes mode 0600.
func TestHostInitMakesAnExistingConfigPrivate(t *testing.T) {
	h := newInitHost(t, nil)
	if err := os.WriteFile(h.config, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	noTerminal(t)
	h.init(t, h.bundle)
	if info, err := os.Stat(h.config); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("config %v, %v; want mode 0600", info, err)
	}
}

// A tenant entitled to no pool gets its bundle placed and key registered, and is told to run init again.
func TestHostInitWithoutAnEntitlement(t *testing.T) {
	h := newInitHost(t, nil)
	noTerminal(t)
	stdout, prompts := h.init(t, h.bundle)
	h.registered(t, strings.TrimSpace(stdout))
	if _, err := operator.ReadBundle(filepath.Join(h.dir, "bundle.yaml")); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(prompts, "entitled to no pool yet; run wherry host init again once the operator runs tenant pool add") {
		t.Fatalf("init printed\n%s\nwant it told to run again once entitled", prompts)
	}
}

// Without BUNDLE, init refuses a config that names none.
func TestHostInitWithoutABundle(t *testing.T) {
	noTerminal(t)
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv("WHERRY_CONFIG", "")
	err := run([]string{"host", "init"}, &bytes.Buffer{}, &bytes.Buffer{})
	if want := filepath.Join(home, ".config", "wherry", "config.yaml") + " names no bundle; run wherry host init BUNDLE"; err == nil || err.Error() != want {
		t.Fatalf("init = %v; want %q", err, want)
	}
}

// Two configurations in one directory take their own bundles and host keys, named from each whole path, so they never
// come to share an identity (agb-d1hh). WHERRY_CONFIG names a configuration as --config-file does. The configuration
// directory gets no file of theirs, and the directory they sit in keeps its mode.
func TestHostInitKeepsConfigurationsApart(t *testing.T) {
	h := newInitHost(t, nil)
	bundleB := h.addHost(t, "host-b")
	noTerminal(t)
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	a, b := filepath.Join(dir, "a.yaml"), filepath.Join(dir, "b.yaml")
	keyA, _ := h.init(t, h.bundle, "--config-file", a)
	t.Setenv("WHERRY_CONFIG", b)
	keyB, _ := h.init(t, bundleB)
	t.Setenv("WHERRY_CONFIG", "")
	if strings.TrimSpace(keyA) == strings.TrimSpace(keyB) {
		t.Fatalf("both configurations registered key %s; want a key each", strings.TrimSpace(keyA))
	}
	for _, c := range []struct{ path, host, keyID string }{{a, "host-a", strings.TrimSpace(keyA)}, {b, "host-b", strings.TrimSpace(keyB)}} {
		bundle, err := operator.ReadBundle(c.path + ".bundle.yaml")
		if err != nil || bundle.Principal != operator.TenantPrincipal("tenant-a", c.host) {
			t.Fatalf("%s.bundle.yaml holds %+v, %v; want %s's bundle", c.path, bundle, err, c.host)
		}
		h.registeredAt(t, c.host, c.path+".host.key", c.keyID)
		var config busclient.Config
		if err := readYAML(c.path, &config); err != nil || config.Bundle != c.path+".bundle.yaml" || config.HostKey != "" {
			t.Fatalf("%s holds %+v, %v; want its own bundle named and host_key left to its default", c.path, config, err)
		}
	}
	for _, name := range []string{"config.yaml", "bundle.yaml", "host.key"} {
		absent(t, filepath.Join(h.dir, name))
	}
	if info, err := os.Stat(dir); err != nil || info.Mode().Perm() != 0o755 {
		t.Fatalf("%s is %v, %v; want its mode 0755 kept", dir, info.Mode(), err)
	}
}

// The default configuration keeps the configuration directory's bundle.yaml and host.key when it is named relatively or
// through a symlink, so an initialised host gets no second key; a configuration elsewhere keeps an explicit host_key.
func TestHostInitDefaultPathKeepsItsFiles(t *testing.T) {
	h := newInitHost(t, nil)
	bundleB := h.addHost(t, "host-b")
	noTerminal(t)
	t.Chdir(h.dir)
	keyID, _ := h.init(t, h.bundle, "--config-file", "config.yaml")
	h.registered(t, strings.TrimSpace(keyID))
	if _, err := operator.ReadBundle(filepath.Join(h.dir, "bundle.yaml")); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "link.yaml")
	if err := os.Symlink(h.config, link); err != nil {
		t.Fatal(err)
	}
	if again, _ := h.init(t, "--config-file", link); again != keyID {
		t.Fatalf("init through a symlink registered %s; want the host's key %s kept", strings.TrimSpace(again), strings.TrimSpace(keyID))
	}
	for _, stray := range []string{filepath.Join(h.dir, "config.yaml.host.key"), filepath.Join(h.dir, "config.yaml.bundle.yaml"), link + ".host.key", link + ".bundle.yaml"} {
		absent(t, stray)
	}
	dir := t.TempDir()
	own, keyPath := filepath.Join(dir, "own.yaml"), filepath.Join(dir, "keys", "my.key")
	if err := writeYAML(own, busclient.Config{HostKey: keyPath}); err != nil {
		t.Fatal(err)
	}
	keyB, _ := h.init(t, bundleB, "--config-file", own)
	h.registeredAt(t, "host-b", keyPath, strings.TrimSpace(keyB))
	absent(t, own+".host.key")
	var config busclient.Config
	if err := readYAML(own, &config); err != nil || config.HostKey != keyPath || config.Bundle != own+".bundle.yaml" {
		t.Fatalf("%s holds %+v, %v; want host_key kept and its own bundle named", own, config, err)
	}
}
