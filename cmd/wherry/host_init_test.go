package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/credential"
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

// host init BUNDLE places the bundle in a 0700 directory as a 0600 file, registers the host key, prints its ID, and
// records the CA file it asks for when no ca.pem is present, re-asking for a file that holds no certificate.
func TestHostInitPlacesTheBundle(t *testing.T) {
	t.Setenv("MODEL_API_KEY", "")
	h := newInitHost(t, map[string]operator.Terms{"pool-a": agentTerms(envCredential("model", "MODEL_API_KEY"))})
	caFile := filepath.Join(t.TempDir(), "kafka-ca.pem")
	if err := os.Rename(filepath.Join(h.dir, "ca.pem"), caFile); err != nil {
		t.Fatal(err)
	}
	notCA := filepath.Join(t.TempDir(), "not-ca.pem")
	if err := os.WriteFile(notCA, []byte("not a certificate\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	onTerminal(t, notCA+"\n"+caFile+"\n\n")
	stdout, prompts := h.init(t, h.bundle)
	if strings.Count(prompts, "CA file for the broker's certificate (blank for the system roots)") != 2 || !strings.Contains(prompts, notCA+" holds no PEM certificate\n") {
		t.Fatalf("init asked\n%s\nwant the CA file asked for again after %s", prompts, notCA)
	}
	placed := filepath.Join(h.dir, "bundle.yaml")
	for path, want := range map[string]os.FileMode{h.dir: 0o700, placed: 0o600} {
		if info, err := os.Stat(path); err != nil || info.Mode().Perm() != want {
			t.Fatalf("%s: %v, %v; want mode %v", path, info, err, want)
		}
	}
	original, err := operator.ReadBundle(h.bundle)
	if err != nil {
		t.Fatal(err)
	}
	if copied, err := operator.ReadBundle(placed); err != nil || copied != original {
		t.Fatalf("placed bundle %+v, %v; want the given bundle", copied, err)
	}
	h.registered(t, strings.TrimSpace(stdout))
	if config := h.written(t); config.Bundle != placed || config.CAFile != caFile {
		t.Fatalf("config names bundle %s and ca_file %s; want %s and %s", config.Bundle, config.CAFile, placed, caFile)
	}
	// Without BUNDLE, init uses the bundle the config names.
	onTerminal(t, "\n")
	if again, _ := h.init(t); again != stdout {
		t.Fatalf("second init printed %q; want the same key ID %q", again, stdout)
	}
}

// Each credential the entitled pools' agents take is asked for once, a name two pools share naming both targets.
func TestHostInitAsksEachCredentialOnce(t *testing.T) {
	for _, variable := range []string{"MODEL_API_KEY", "SHARED_TOKEN"} {
		t.Setenv(variable, "")
	}
	h := newInitHost(t, map[string]operator.Terms{
		"pool-a": agentTerms(envCredential("model", "MODEL_API_KEY"), envCredential("shared", "SHARED_TOKEN")),
		"pool-b": agentTerms(fileCredential("openai", "openai.key"), fileCredential("shared", "shared.txt")),
	})
	onTerminal(t, "env:MY_MODEL\nfile:~/keys/openai\ncommand:pass show shared\n")
	_, prompts := h.init(t, h.bundle)
	for _, want := range []string{
		"pool pool-a delivers credential model to its agents as MODEL_API_KEY.",
		"pool pool-b delivers credential openai to its agents as file openai.key.",
		"pool pool-a delivers credential shared to its agents as SHARED_TOKEN.",
		"pool pool-b delivers credential shared to its agents as file shared.txt.",
		"Source for optional credential openai",
	} {
		if !strings.Contains(prompts, want) {
			t.Errorf("init asked\n%s\nwant %q", prompts, want)
		}
	}
	if asked := strings.Count(prompts, "Source for credential shared"); asked != 1 {
		t.Errorf("init asked for shared's source %d times; want once, as required by pool-a", asked)
	}
	want := map[string]credential.Source{"model": {Env: "MY_MODEL"}, "openai": {File: filepath.Join(os.Getenv("HOME"), "keys", "openai")}, "shared": {Command: []string{"pass", "show", "shared"}}}
	if config := h.written(t); !reflect.DeepEqual(config.Credentials, want) {
		t.Fatalf("config credentials %+v; want %+v", config.Credentials, want)
	}
	if _, err := busclient.LoadConfig(h.config); err != nil {
		t.Fatal(err)
	}
}

// A target variable set in the environment is suggested, naming the credential, its pool and target and who it goes
// to; a blank answer is No. A yes writes an env source: the variable's name, never its value.
func TestHostInitSuggestsTheTargetVariable(t *testing.T) {
	const value = "model-key-value"
	t.Setenv("MODEL_API_KEY", value)
	terms := map[string]operator.Terms{"pool-a": agentTerms(envCredential("model", "MODEL_API_KEY"))}
	t.Run("blank", func(t *testing.T) {
		h := newInitHost(t, terms)
		onTerminal(t, "\n\n")
		_, prompts := h.init(t, h.bundle)
		for _, want := range []string{
			"pool pool-a delivers credential model to its agents as MODEL_API_KEY.\n",
			"found MODEL_API_KEY in your environment. Use it for model? It will be sent to agents in every pool, now or later, that names model. [y/N]\n",
			"an environment source is read where submits run, including the MCP server your harness starts.\n",
		} {
			if !strings.Contains(prompts, want) {
				t.Errorf("init asked\n%s\nwant %q", prompts, want)
			}
		}
		if config := h.written(t); len(config.Credentials) != 0 {
			t.Fatalf("config credentials %+v; want none after a blank answer", config.Credentials)
		}
		if !strings.Contains(prompts, "credential model (pool pool-a as MODEL_API_KEY, required)") {
			t.Fatalf("init printed\n%s\nwant model listed without a source", prompts)
		}
	})
	t.Run("yes", func(t *testing.T) {
		h := newInitHost(t, terms)
		onTerminal(t, "y\n")
		h.init(t, h.bundle)
		if config := h.written(t); !reflect.DeepEqual(config.Credentials, map[string]credential.Source{"model": {Env: "MODEL_API_KEY"}}) {
			t.Fatalf("config credentials %+v; want model's source env MODEL_API_KEY", config.Credentials)
		}
		if written, err := os.ReadFile(h.config); err != nil || bytes.Contains(written, []byte(value)) {
			t.Fatalf("config holds the variable's value, or %v:\n%s", err, written)
		}
	})
}

// A re-run once another pool is entitled asks only for that pool's credentials and keeps every entry already there.
func TestHostInitRerunAsksOnlyForTheNewPool(t *testing.T) {
	for _, variable := range []string{"MODEL_API_KEY", "OPENAI_API_KEY"} {
		t.Setenv(variable, "")
	}
	h := newInitHost(t, map[string]operator.Terms{"pool-a": agentTerms(envCredential("model", "MODEL_API_KEY"))})
	onTerminal(t, "env:MY_MODEL\n")
	h.init(t, h.bundle)
	before := h.written(t)
	h.entitle(t, "pool-b", agentTerms(envCredential("openai", "OPENAI_API_KEY")))
	onTerminal(t, "env:MY_OPENAI\n")
	_, prompts := h.init(t)
	if strings.Contains(prompts, "credential model") || !strings.Contains(prompts, "Source for credential openai") {
		t.Fatalf("re-run asked\n%s\nwant openai alone asked for", prompts)
	}
	after := h.written(t)
	if !reflect.DeepEqual(after.Credentials["openai"], credential.Source{Env: "MY_OPENAI"}) {
		t.Fatalf("config credentials %+v; want openai's source added", after.Credentials)
	}
	delete(after.Credentials, "openai")
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("re-run config %+v; want the first run's %+v kept", after, before)
	}
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

// A pool's VCS host gets a git entry at the default prefix, and a pool's storage its two credentials, each new
// credential asked for its source.
func TestHostInitAsksForGitAndStorage(t *testing.T) {
	terms := agentTerms()
	terms.VCS = []string{"git.example.test"}
	terms.Storage = &operator.Storage{Endpoint: "https://s3.example.test", Bucket: "bucket-a"}
	h := newInitHost(t, map[string]operator.Terms{"pool-a": terms, "pool-b": agentTerms()})
	keys := t.TempDir()
	onTerminal(t, "y\ngit-token\nenv:GIT_TOKEN\ns3-access\nfile:"+filepath.Join(keys, "access")+"\ns3-secret\nfile:"+filepath.Join(keys, "secret")+"\n")
	_, prompts := h.init(t, h.bundle)
	for _, want := range []string{"Git URL prefix for git.example.test (y for https://git.example.test/; blank skips git.example.test)", "Credential for https://git.example.test/ (blank: anonymous)", "Credential holding pool pool-a's S3 access key"} {
		if !strings.Contains(prompts, want) {
			t.Errorf("init asked\n%s\nwant %q", prompts, want)
		}
	}
	if strings.Contains(prompts, "pool pool-b's S3") {
		t.Errorf("init asked\n%s\nwant no storage asked for pool-b, which has none", prompts)
	}
	config := h.written(t)
	if !reflect.DeepEqual(config.Git, []busclient.GitCredential{{URL: "https://git.example.test/", Credential: "git-token"}}) ||
		!reflect.DeepEqual(config.Storage, map[string]busclient.StorageConfig{"pool-a": {AccessKey: "s3-access", SecretKey: "s3-secret"}}) ||
		!reflect.DeepEqual(config.Credentials["git-token"], credential.Source{Env: "GIT_TOKEN"}) || config.Credentials["s3-secret"].File != filepath.Join(keys, "secret") {
		t.Fatalf("config %+v; want the git entry, pool-a's storage and their sources", config)
	}
	if _, err := busclient.LoadConfig(h.config); err != nil {
		t.Fatal(err)
	}
}

// A git prefix on another host than the VCS host asked for is refused and re-asked, and a blank prefix skips the host.
func TestHostInitGitPrefixNamesItsHost(t *testing.T) {
	terms := agentTerms()
	terms.VCS = []string{"git.example.test"}
	h := newInitHost(t, map[string]operator.Terms{"pool-a": terms})
	onTerminal(t, "https://other.example.test/\n\n\n")
	_, prompts := h.init(t, h.bundle)
	if strings.Count(prompts, "Git URL prefix for git.example.test") != 2 || !strings.Contains(prompts, "a git URL prefix for git.example.test starts with https://git.example.test or ssh://git.example.test") {
		t.Fatalf("init asked\n%s\nwant the prefix on another host refused and re-asked", prompts)
	}
	if config := h.written(t); len(config.Git) != 0 || !strings.Contains(prompts, "git host git.example.test") {
		t.Fatalf("config git %+v, init printed\n%s\nwant no entry and the host listed", config.Git, prompts)
	}
}

// A credential whose source this run asked for and was skipped is not asked again when git or storage names it.
func TestHostInitAsksASkippedCredentialOnce(t *testing.T) {
	t.Setenv("SHARED_TOKEN", "")
	terms := agentTerms(envCredential("shared", "SHARED_TOKEN"))
	terms.VCS = []string{"git.example.test"}
	terms.Storage = &operator.Storage{Endpoint: "https://s3.example.test", Bucket: "bucket-a"}
	h := newInitHost(t, map[string]operator.Terms{"pool-a": terms})
	onTerminal(t, "\ny\nshared\nshared\n")
	_, prompts := h.init(t, h.bundle)
	if asked := strings.Count(prompts, "Source for credential shared"); asked != 1 {
		t.Fatalf("init asked for shared's source %d times:\n%s\nwant once", asked, prompts)
	}
	for _, want := range []string{"credential shared (pool pool-a as SHARED_TOKEN, required)", "git host git.example.test", "storage of pool pool-a"} {
		if !strings.Contains(prompts, want) {
			t.Errorf("init printed\n%s\nwant %q listed", prompts, want)
		}
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
