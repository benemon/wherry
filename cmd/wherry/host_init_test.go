package main

import (
	"bytes"
	"context"
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
	key, err := trust.LoadSigningKeyFile(filepath.Join(h.dir, "host.key"))
	if err != nil {
		t.Fatal(err)
	}
	owner := signed.HostOwner("tenant-a", "host-a")
	keys, err := operator.ReadKeys(h.ctx, h.service.Operator, map[string]string{owner: operator.HostTopic("tenant-a", "host-a")})
	if err != nil {
		t.Fatal(err)
	}
	if current := keys[owner].Current; current == nil || !current.Equal(key.Public()) || signed.KeyID(current) != keyID {
		t.Fatalf("host key topic holds %v; want the host key %s registered", current, keyID)
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
