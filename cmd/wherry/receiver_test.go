package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/pluginsdk"
	"github.com/prometheus/client_golang/prometheus"
)

// pluginDir holds wherry-plugin-fake, the protocol module's test plugin, for the package's tests.
var pluginDir string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "wherry-plugins")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	pluginDir = dir
	if output, err := exec.Command("go", "build", "-o", filepath.Join(dir, "wherry-plugin-fake"), "github.com/benemon/wherry/pluginsdk/internal/testplugin").CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build test plugin: %v\n%s", err, output)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// pluginRecord is the test plugin's record of one call.
type pluginRecord struct {
	PID      int                `json:"pid"`
	Call     string             `json:"call"`
	Settings pluginsdk.Settings `json:"settings"`
	Run      *pluginsdk.Run     `json:"run"`
}

// recordPlugin points the test plugin's record at a new file and returns a reader for it.
func recordPlugin(t *testing.T) func() []pluginRecord {
	t.Helper()
	path := filepath.Join(t.TempDir(), "record.jsonl")
	t.Setenv("WHERRY_TESTPLUGIN_RECORD", path)
	return func() []pluginRecord {
		t.Helper()
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		var records []pluginRecord
		for scanner := bufio.NewScanner(file); scanner.Scan(); {
			var record pluginRecord
			if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
				t.Fatal(err)
			}
			records = append(records, record)
		}
		return records
	}
}

func TestPluginPathRefusesWritableOrIrregularFiles(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "plugins")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(dir, "wherry-plugin-nomad")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	// The umask must not mask the modes under test.
	chmod := func(path string, mode os.FileMode) {
		t.Helper()
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
	}
	if path, err := pluginPath(dir, "nomad"); err != nil || path != binary {
		t.Fatalf("pluginPath = %q, %v; want the owner-only binary", path, err)
	}
	for _, mode := range []os.FileMode{0o775, 0o757, 0o777} {
		chmod(dir, mode)
		if _, err := pluginPath(dir, "nomad"); err == nil {
			t.Fatalf("accepted a plugin directory with mode %o", mode)
		}
	}
	chmod(dir, 0o755)
	for _, mode := range []os.FileMode{0o775, 0o757, 0o777} {
		chmod(binary, mode)
		if _, err := pluginPath(dir, "nomad"); err == nil {
			t.Fatalf("accepted a plugin binary with mode %o", mode)
		}
	}
	chmod(binary, 0o755)
	if err := os.Symlink(binary, filepath.Join(dir, "wherry-plugin-linked")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "wherry-plugin-folder"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"linked", "folder"} {
		if _, err := pluginPath(dir, kind); err == nil || !strings.Contains(err.Error(), "regular file") {
			t.Fatalf("plugin %s: %v; want refused as not a regular file", kind, err)
		}
	}
	if _, err := pluginPath(dir, "kubernetes"); err == nil || err.Error() != "no plugin for type kubernetes" {
		t.Fatalf("missing plugin: %v", err)
	}
	if _, err := pluginPath(dir, "../nomad"); err == nil {
		t.Fatal("accepted a plugin naming a path")
	}
	if os.Getuid() == 0 {
		t.Log("running as root: every file here is root's, so another owner cannot be stood in")
		return
	}
	previous := receiverUID
	receiverUID = os.Getuid() + 1
	defer func() { receiverUID = previous }()
	if _, err := pluginPath(dir, "nomad"); err == nil {
		t.Fatal("accepted a plugin directory owned by a user other than root or the receiver's")
	}
	if root, err := os.Stat("/"); err != nil || !ownerOnly(root) {
		t.Fatalf("refused root's directory /: %v", err)
	}
}

var discard = log.New(io.Discard, "", 0)

// installPlugin copies the test plugin into a new directory as wherry-plugin-fake and returns the directory.
func installPlugin(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	binary, err := os.ReadFile(filepath.Join(pluginDir, "wherry-plugin-fake"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "wherry-plugin-fake"), binary, 0o755); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestRelativePluginDirRunsTheCheckedBinary(t *testing.T) {
	read := recordPlugin(t)
	dir := installPlugin(t)
	decoys, marker := t.TempDir(), filepath.Join(t.TempDir(), "decoy-ran")
	if err := os.WriteFile(filepath.Join(decoys, "wherry-plugin-fake"), []byte("#!/bin/sh\ntouch "+marker+"\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", decoys+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Chdir(dir)
	plugins := &plugins{dir: ".", logs: io.Discard, logger: discard, registry: prometheus.NewRegistry(), running: map[string]*pluginsdk.Process{}}
	defer plugins.close()
	_, err := plugins.profile("build", admin.ProfileSettings{Name: "exec", Plugin: "fake", Agent: "claude"})
	if _, statErr := os.Stat(marker); statErr == nil {
		t.Fatal("a binary found on PATH ran in place of the checked plugin")
	}
	if err != nil || len(read()) != 1 {
		t.Fatalf("plugin in the relative directory: %v", err)
	}
}

func TestRestartChecksThePluginAgain(t *testing.T) {
	read := recordPlugin(t)
	dir := installPlugin(t)
	logs := &syncBuffer{}
	plugins := &plugins{dir: dir, logs: logs, logger: discard, registry: prometheus.NewRegistry(), running: map[string]*pluginsdk.Process{}}
	defer plugins.close()
	if _, err := plugins.profile("build", admin.ProfileSettings{Name: "exec", Plugin: "fake", Agent: "claude"}); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(dir, "wherry-plugin-fake"), 0o775); err != nil {
		t.Fatal(err)
	}
	first := read()[0].PID
	if err := syscall.Kill(first, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Second)
	for _, record := range read() {
		if record.PID != first {
			t.Fatalf("restarted a plugin binary that group can write, as %d", record.PID)
		}
	}
	if !strings.Contains(logs.String(), "plugin restart failed") || !strings.Contains(logs.String(), "only root or the receiver's user owns and can write") {
		t.Fatalf("refused restart not logged:\n%s", logs.String())
	}
}

func TestMissingPluginIsAConfigurationError(t *testing.T) {
	plugins := &plugins{dir: t.TempDir(), logs: io.Discard, registry: prometheus.NewRegistry(), running: map[string]*pluginsdk.Process{}}
	defer plugins.close()
	_, err := plugins.profile("build", admin.ProfileSettings{Name: "lab", Plugin: "nomad", Agent: "claude"})
	if err == nil || err.Error() != "no plugin for type nomad" {
		t.Fatalf("got %v, want no plugin for type nomad", err)
	}
}

func TestOneProcessPerPlugin(t *testing.T) {
	read := recordPlugin(t)
	dir := t.TempDir()
	for _, kind := range []string{"fake", "other"} {
		binary, err := os.ReadFile(filepath.Join(pluginDir, "wherry-plugin-fake"))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "wherry-plugin-"+kind), binary, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	logged := &syncBuffer{}
	plugins := &plugins{dir: dir, logs: io.Discard, logger: log.New(logged, "receiver: ", 0), registry: prometheus.NewRegistry(), running: map[string]*pluginsdk.Process{}}
	defer plugins.close()
	for _, settings := range []admin.ProfileSettings{
		{Name: "exec", Plugin: "fake", Agent: "claude"}, {Name: "podman", Plugin: "fake", Agent: "claude"}, {Name: "edge", Plugin: "other", Agent: "claude"},
	} {
		named, err := plugins.profile("build", settings)
		if err != nil || named.Name != settings.Name {
			t.Fatalf("profile %s: %+v, %v", settings.Name, named, err)
		}
	}
	processes := map[string]int{}
	for _, record := range read() {
		processes[record.Settings.Name] = record.PID
	}
	if len(processes) != 3 || processes["exec"] != processes["podman"] || processes["exec"] == processes["edge"] {
		t.Fatalf("profiles configured in processes %v; want exec and podman in one, edge in another", processes)
	}
	// Each plugin's identity is logged once, when it starts.
	if want := "receiver: plugin fake: version v0.0.0-test protocol 10\nreceiver: plugin other: version v0.0.0-test protocol 10\n"; logged.String() != want {
		t.Fatalf("logged %q, want %q", logged.String(), want)
	}
}

func TestReadBundleIsStrict(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bundle.yaml")
	if err := os.WriteFile(path, []byte("broker: b\nprincipal: wherry.rcv.r\npassword: p\nca: c\nextra: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ReadBundle(path); err == nil {
		t.Fatal("accepted a bundle with an unknown field")
	}
	// The bundle holds only the receiver's Kafka identity; platform credentials come from the local configuration.
	if err := os.WriteFile(path, []byte("broker: b\nprincipal: wherry.rcv.r\npassword: p\nca: c\nplatform:\n  lab: token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ReadBundle(path); err == nil {
		t.Fatal("accepted a bundle carrying platform credentials")
	}
	if err := os.WriteFile(path, []byte("broker: b\nprincipal: wherry.rcv.r\npassword: p\nca: c\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	bundle, err := admin.ReadBundle(path)
	if err != nil || bundle.Principal != "wherry.rcv.r" {
		t.Fatalf("bundle %#v err %v", bundle, err)
	}
	if err := os.WriteFile(path, []byte("broker: b\nca: c\npassword: p\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ReadBundle(path); err == nil {
		t.Fatal("accepted a bundle without a principal")
	}
}

func TestReadBundleErrorsEchoNoContent(t *testing.T) {
	const pasted = "sk-ant-oat01-pasted-as-a-key"
	path := filepath.Join(t.TempDir(), "bundle.yaml")
	for _, content := range []string{
		"broker: b\nprincipal: wherry.rcv.r\n" + pasted + ": 1\n",
		"broker: [" + pasted + "\n",
		"platform: " + pasted + "\n",
	} {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := admin.ReadBundle(path); err == nil || strings.Contains(err.Error(), "sk-ant") {
			t.Fatalf("bundle error %v must name no YAML content", err)
		}
	}
}

func TestMetricsServeOnlyTheMetricsPath(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer serveMetrics(listener, newRegistry(), log.New(io.Discard, "", 0)).Close()
	base := "http://" + listener.Addr().String()
	for path, want := range map[string]int{"/metrics": http.StatusOK, "/": http.StatusNotFound, "/metrics/x": http.StatusNotFound, "/debug/pprof/": http.StatusNotFound} {
		response, err := http.Get(base + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != want {
			t.Fatalf("GET %s = %d; want %d", path, response.StatusCode, want)
		}
		if path == "/metrics" && (!strings.Contains(string(body), "wherry_build_info{") || !strings.Contains(string(body), "go_goroutines ")) {
			t.Fatalf("metrics lack the build info and runtime collectors:\n%s", body)
		}
	}
}

// writeLocalConfig writes a receiver local configuration file and returns its path.
func writeLocalConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestReceiverLocalConfigExampleParses(t *testing.T) {
	config, err := loadReceiverLocalConfig(filepath.Join("testdata", "example-receiver-config.yaml"))
	want := receiverLocalConfig{Bundle: "/etc/wherry/receiver.bundle", StateDir: "/var/lib/wherry/state", PluginDir: defaultPluginDir, MetricsAddr: ":9464", DrainTimeout: 25 * time.Second,
		Credentials: map[string]credential.Source{"nomad-token": {File: "/etc/wherry/nomad-token"}}}
	if err != nil || !reflect.DeepEqual(config, want) {
		t.Fatalf("example = %+v, %v; want %+v", config, err, want)
	}
	documented(t, "configuration.md", "example-receiver-config.yaml")
	documented(t, "operations.md", "example-receiver-config.yaml")
}

func TestReceiverLocalConfigDefaultsAndExpansion(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	config, err := loadReceiverLocalConfig(writeLocalConfig(t, "bundle: ~/wherry/receiver.bundle\ncredentials:\n  token: {file: ~/token}\n"))
	want := receiverLocalConfig{Bundle: filepath.Join(home, "wherry", "receiver.bundle"), StateDir: filepath.Join(home, "wherry", "state"),
		PluginDir: "/usr/local/libexec/wherry/plugins", Credentials: map[string]credential.Source{"token": {File: filepath.Join(home, "token")}}}
	if err != nil || !reflect.DeepEqual(config, want) {
		t.Fatalf("config = %+v, %v; want %+v", config, err, want)
	}
}

func TestReceiverLocalConfigRefusals(t *testing.T) {
	for name, test := range map[string]struct{ content, want string }{
		"an unknown field":     {"bundle: b\nbundle_file: b\n", "decode config: invalid or unknown field at line 2"},
		"no bundle":            {"state_dir: s\n", "config requires bundle"},
		"a negative drain":     {"bundle: b\ndrain_timeout: -1s\n", "config drain_timeout must not be negative"},
		"two kinds of source":  {"bundle: b\ncredentials:\n  token: {file: x, env: A}\n", "config credential token: needs exactly one of file, env and command"},
		"no kind of source":    {"bundle: b\ncredentials:\n  token: {}\n", "config credential token: needs exactly one of file, env and command"},
		"an empty command":     {"bundle: b\ncredentials:\n  token:\n    command: ['']\n", "config credential token: command names no program"},
		"a literal credential": {"bundle: b\ncredentials:\n  token: nomad-token-value\n", "decode config"},
	} {
		_, err := loadReceiverLocalConfig(writeLocalConfig(t, test.content))
		if err == nil || !strings.Contains(err.Error(), test.want) || strings.Contains(err.Error(), "nomad-tok") {
			t.Errorf("config with %s: %v; want %q and no value", name, err, test.want)
		}
	}
}

// receiver run reads the file --config-file names, and /etc/wherry/config.yaml without it; the flags the file replaced
// are refused.
func TestReceiverRunTakesItsConfigFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.yaml")
	err := run([]string{"receiver", "run", "--config-file", writeLocalConfig(t, "bundle: "+missing+"\n")}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "read bundle") || !strings.Contains(err.Error(), missing) {
		t.Fatalf("receiver run --config-file = %v; want the named file's bundle read", err)
	}
	if _, statErr := os.Stat(defaultReceiverConfig); statErr == nil {
		t.Logf("%s exists here; not checking the default", defaultReceiverConfig)
	} else if err := run([]string{"receiver", "run"}, io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), defaultReceiverConfig) {
		t.Fatalf("receiver run = %v; want %s read", err, defaultReceiverConfig)
	}
	for _, flag := range []string{"--bundle-file", "--state-dir", "--plugin-dir", "--metrics-addr"} {
		var stderr bytes.Buffer
		if err := run([]string{"receiver", "run", flag, "x"}, io.Discard, &stderr); err == nil || !strings.Contains(err.Error(), "flag provided but not defined: "+strings.TrimPrefix(flag, "-")) {
			t.Errorf("receiver run %s = %v; want the flag refused", flag, err)
		}
	}
}

// A profile's config names its platform credential; the plugin gets the source's value, without its trailing newline,
// in Settings.Credential, and a config without the credential key.
func TestProfileCredentialReachesThePluginInPlaceOfItsName(t *testing.T) {
	const value = "nomad-platform-token-value"
	read := recordPlugin(t)
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte(value+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	plugins := &plugins{dir: installPlugin(t), credentials: map[string]credential.Source{"nomad-token": {File: token}}, hold: func(string, []byte) {},
		logs: io.Discard, logger: discard, registry: prometheus.NewRegistry(), running: map[string]*pluginsdk.Process{}}
	defer plugins.close()
	settings := admin.ProfileSettings{Name: "exec", Plugin: "fake", Agent: "claude", Config: map[string]any{"credential": "nomad-token", "outputs": true}}
	if _, err := plugins.profile("build", settings); err != nil {
		t.Fatal(err)
	}
	records := read()
	if len(records) != 1 || records[0].Settings.Credential != value || string(records[0].Settings.Config) != `{"outputs":true}` {
		t.Fatalf("plugin configured with %+v; want the credential's value and the config without its key", records)
	}
	if settings.Config["credential"] != "nomad-token" {
		t.Fatal("configuring the profile changed its recorded settings")
	}
}

// A credential name without a source, or a source that does not resolve, fails the profile's configuration with an
// error naming the credential and neither its value nor its file.
func TestProfileCredentialFailures(t *testing.T) {
	const value = "nomad-platform-token-value"
	dir := t.TempDir()
	open := filepath.Join(dir, "open-token")
	if err := os.WriteFile(open, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
	// The file source permission rule is the host's.
	if err := os.Chmod(open, 0o644); err != nil {
		t.Fatal(err)
	}
	plugins := &plugins{dir: installPlugin(t), credentials: map[string]credential.Source{"open-token": {File: open}}, logs: io.Discard,
		registry: prometheus.NewRegistry(), running: map[string]*pluginsdk.Process{}}
	defer plugins.close()
	for name, want := range map[string]string{
		"nomad-token": `credential "nomad-token" has no source in the receiver's configuration`,
		"open-token":  "resolve credential open-token: its file is writable by its group or open to others; chmod 600 it",
	} {
		_, err := plugins.profile("build", admin.ProfileSettings{Name: "exec", Plugin: "fake", Agent: "claude", Config: map[string]any{"credential": name}})
		if err == nil || err.Error() != want || strings.Contains(err.Error(), value) || strings.Contains(err.Error(), dir) {
			t.Errorf("credential %s: %v; want %q", name, err, want)
		}
	}
	if len(plugins.running) != 0 {
		t.Fatal("a plugin started for a profile whose credential did not resolve")
	}
}
