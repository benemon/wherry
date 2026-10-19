package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/benemon/wherry/adapter"
	"github.com/benemon/wherry/internal/admin"
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
	if output, err := exec.Command("go", "build", "-o", filepath.Join(dir, "wherry-plugin-fake"), "github.com/benemon/wherry/adapter/internal/testplugin").CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build test plugin: %v\n%s", err, output)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// pluginRecord is the test plugin's record of one call.
type pluginRecord struct {
	PID      int              `json:"pid"`
	Call     string           `json:"call"`
	Settings adapter.Settings `json:"settings"`
	Run      *adapter.Run     `json:"run"`
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
		t.Fatal("accepted an adapter type naming a path")
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
	plugins := &plugins{dir: ".", logs: io.Discard, running: map[string]*adapter.Plugin{}}
	defer plugins.close()
	_, err := plugins.adapter("build", admin.AdapterSettings{Name: "exec", Type: "fake", Agent: "claude"}, "")
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
	plugins := &plugins{dir: dir, logs: logs, running: map[string]*adapter.Plugin{}}
	defer plugins.close()
	if _, err := plugins.adapter("build", admin.AdapterSettings{Name: "exec", Type: "fake", Agent: "claude"}, ""); err != nil {
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

func TestGitFollowsThePlugin(t *testing.T) {
	plugins := &plugins{dir: pluginDir, logs: io.Discard, running: map[string]*adapter.Plugin{}}
	defer plugins.close()
	for _, git := range []bool{true, false} {
		named, err := plugins.adapter("build", admin.AdapterSettings{Name: fmt.Sprintf("git-%t", git), Type: "fake", Agent: "claude", Config: map[string]any{"git": git}}, "")
		if err != nil || named.Git != git {
			t.Fatalf("adapter reporting git=%t: Named.Git %t, %v", git, named.Git, err)
		}
	}
}

func TestMissingPluginIsAConfigurationError(t *testing.T) {
	plugins := &plugins{dir: t.TempDir(), logs: io.Discard, running: map[string]*adapter.Plugin{}}
	defer plugins.close()
	_, err := plugins.adapter("build", admin.AdapterSettings{Name: "lab", Type: "nomad", Agent: "claude"}, "")
	if err == nil || err.Error() != "no plugin for type nomad" {
		t.Fatalf("got %v, want no plugin for type nomad", err)
	}
}

func TestOneProcessPerAdapterType(t *testing.T) {
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
	plugins := &plugins{dir: dir, logs: io.Discard, running: map[string]*adapter.Plugin{}}
	defer plugins.close()
	for _, settings := range []admin.AdapterSettings{
		{Name: "exec", Type: "fake", Agent: "claude"}, {Name: "podman", Type: "fake", Agent: "claude"}, {Name: "edge", Type: "other", Agent: "claude"},
	} {
		named, err := plugins.adapter("build", settings, "")
		if err != nil || named.Name != settings.Name {
			t.Fatalf("adapter %s: %+v, %v", settings.Name, named, err)
		}
	}
	processes := map[string]int{}
	for _, record := range read() {
		processes[record.Settings.Name] = record.PID
	}
	if len(processes) != 3 || processes["exec"] != processes["podman"] || processes["exec"] == processes["edge"] {
		t.Fatalf("adapters configured in processes %v; want exec and podman in one, edge in another", processes)
	}
}

func TestReadBundleIsStrict(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bundle.yaml")
	if err := os.WriteFile(path, []byte("broker: b\nprincipal: wherry.rcv.r\npassword: p\nca: c\nextra: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readBundle(path); err == nil {
		t.Fatal("accepted a bundle with an unknown field")
	}
	if err := os.WriteFile(path, []byte("broker: b\nprincipal: wherry.rcv.r\npassword: p\nca: c\nplatform:\n  lab: token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	bundle, err := readBundle(path)
	if err != nil || bundle.Platform["lab"] != "token" || bundle.Principal != "wherry.rcv.r" {
		t.Fatalf("bundle %#v err %v", bundle, err)
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
		if _, err := readBundle(path); err == nil || strings.Contains(err.Error(), "sk-ant") {
			t.Fatalf("bundle error %v must name no YAML content", err)
		}
	}
}
