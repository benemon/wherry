package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/trust"
)

// -f takes a file, or a directory holding a config in the directory form; pool apply and receiver apply refuse
// --config-file and --config-dir.
func TestObjectFileTakesAFileOrADirectory(t *testing.T) {
	file := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for path, want := range map[string]bool{file: false, dir: true} {
		if got, directory, err := objectFile(t.TempDir(), "pools", "pool-a", map[string]string{"f": path}, io.Discard, poolFields, nil); err != nil || got != path || directory != want {
			t.Errorf("objectFile(-f %s) = %s, %t, %v; want the path and directory %t", path, got, directory, err, want)
		}
	}
	for _, command := range [][]string{{"pool", "apply", "pool-a"}, {"receiver", "apply", "receiver-a"}} {
		for _, option := range []string{"--config-file", "--config-dir"} {
			if _, _, err := operatorArgs(append(command, option, file)); err == nil {
				t.Errorf("operatorArgs(%q %s) accepted the option", command, option)
			}
		}
	}
}

func TestPoolConfigDirectoryMatchesSingleFile(t *testing.T) {
	want, err := readPoolConfig(filepath.Join("testdata", "pool-nomad.yaml"), false)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(want.VCS, []string{"github.com"}) {
		t.Fatalf("single-file VCS hosts = %v, want github.com", want.VCS)
	}
	got, err := readPoolConfig(filepath.Join("testdata", "pool-nomad"), true)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("directory config = %+v, want %+v", got, want)
	}
}

// TestPoolConfigExampleParses reads the reference's complete pool terms in both forms; they set every field a pool file
// sets, and their first agent every agent field.
func TestPoolConfigExampleParses(t *testing.T) {
	want, err := readPoolConfig(filepath.Join("testdata", "example-pool.yaml"), false)
	if err != nil {
		t.Fatal(err)
	}
	got, err := readPoolConfig(filepath.Join("testdata", "example-pool"), true)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("directory config = %+v, want %+v", got, want)
	}
	for _, fields := range []reflect.Value{reflect.ValueOf(want), reflect.ValueOf(want.Agents[0])} {
		for i := range fields.NumField() {
			if fields.Field(i).IsZero() && fields.Type().Field(i).Tag.Get("yaml") != "-" {
				t.Errorf("example pool leaves %s.%s unset", fields.Type().Name(), fields.Type().Field(i).Name)
			}
		}
	}
	if _, err := operator.EncodeRecord(operator.Record{Kind: "pool", Pool: "build", Terms: &want}); err != nil {
		t.Fatal(err)
	}
	documented(t, "configuration.md", "example-pool.yaml", "example-pool/pool.yaml", "example-pool/agents/claude.yaml", "example-pool/agents/coder.yaml", "example-pool/agents/codex.yaml")
}

// documented fails unless the page docs/<page> holds each testdata file as a whole code block.
func documented(t *testing.T, page string, files ...string) {
	t.Helper()
	reference, err := os.ReadFile(filepath.Join("..", "..", "docs", page))
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		example, err := os.ReadFile(filepath.Join("testdata", file))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(reference), "```yaml\n"+string(example)+"```\n") {
			t.Errorf("docs/%s has no code block holding testdata/%s", page, file)
		}
	}
}

func TestPoolConfigRejectsSealLifetime(t *testing.T) {
	pool, err := os.ReadFile(filepath.Join("testdata", "pool-nomad.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "pool.yaml")
	if err := os.WriteFile(file, append(pool, "seal_lifetime_ms: 86400000\n"...), 0o600); err != nil {
		t.Fatal(err)
	}
	line := fmt.Sprintf("unknown field at line %d", bytes.Count(pool, []byte("\n"))+1)
	if _, err := readPoolConfig(file, false); err == nil || !strings.Contains(err.Error(), line) {
		t.Fatalf("pool config = %v, want seal_lifetime_ms refused as an unknown field", err)
	}
}

func TestPoolConfigRejectsInlineLimit(t *testing.T) {
	pool, err := os.ReadFile(filepath.Join("testdata", "pool-nomad.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "pool.yaml")
	if err := os.WriteFile(file, append(pool, "inline_limit: 262144\n"...), 0o600); err != nil {
		t.Fatal(err)
	}
	line := fmt.Sprintf("unknown field at line %d", bytes.Count(pool, []byte("\n"))+1)
	if _, err := readPoolConfig(file, false); err == nil || !strings.Contains(err.Error(), line) {
		t.Fatalf("pool config = %v, want inline_limit refused as an unknown field", err)
	}
}

func TestPoolConfigDirectoryRejectsAgentsInPoolYAML(t *testing.T) {
	dir := writeTree(t, map[string]string{
		"pool.yaml":          "agents:\n  - name: claude\n",
		"agents/claude.yaml": "name: claude\n",
	})
	_, err := readPoolConfig(dir, true)
	if err == nil || !strings.Contains(err.Error(), "agents belong in") {
		t.Fatalf("pool config = %v, want agents rejected from pool.yaml", err)
	}
}

func TestPoolConfigDirectoryRejectsInvalidLayout(t *testing.T) {
	for name, files := range map[string]map[string]string{
		"stray entry": {"pool.yaml": "default_agent: claude\n", "agents/claude.yaml": "name: claude\n", "notes.txt": "x"},
		"no agents":   {"pool.yaml": "default_agent: claude\n", "agents/": ""},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := readPoolConfig(writeTree(t, files), true)
			if err == nil {
				t.Fatal("accepted invalid pool directory")
			}
			if name == "stray entry" && !strings.Contains(err.Error(), "notes.txt") {
				t.Fatalf("error %v does not name the stray entry", err)
			}
		})
	}
}

func TestOperatorArgsRejectsUnknownOptions(t *testing.T) {
	for _, args := range [][]string{
		{"pool", "apply", "pool-a", "--terms", "pool.yaml"},
		{"receiver", "apply", "receiver-a", "--config", "receiver.yaml"},
		{"host", "add", "host-a", "--tenant", "tenant-a", "--bundle", "host.yaml"},
		{"pool", "apply", "pool-a", "--tenant", "tenant-a"},
		{"receiver", "apply", "receiver-a", "--platform-credential-file", "nomad=token"},
		{"receiver", "apply", "receiver-a", "--pool", "pool-a"},
		{"receiver", "apply", "receiver-a", "--plugin-sha256", "nomad=" + fakeSHA256},
		{"host", "add", "host-a", "--tenant", "tenant-a", "--bundle-file", "host.bundle"},
		{"pool", "apply", "pool-a", "--super-user", "admin"},
	} {
		_, _, err := operatorArgs(args)
		option := strings.TrimPrefix(args[len(args)-2], "-")
		if err == nil || !strings.Contains(err.Error(), option) {
			t.Fatalf("operatorArgs(%q) = %v, want an error naming %s", args, err, option)
		}
	}
}

func TestHostConfigFileAndIgnoredEnvironment(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("WHERRY_"+"CONFIG", filepath.Join(t.TempDir(), "bad.yaml"))
	keyPath := filepath.Join(home, "host.key")
	if _, err := trust.GenerateSigningKeyFile(keyPath); err != nil {
		t.Fatal(err)
	}
	configDir := filepath.Join(home, ".config", "wherry")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	config := []byte("bundle: missing.yaml\nhost_key: " + keyPath + "\n")
	defaultPath := filepath.Join(configDir, "config.yaml")
	if err := os.WriteFile(defaultPath, config, 0o600); err != nil {
		t.Fatal(err)
	}
	for name, args := range map[string][]string{
		"explicit": {"host", "init", "--config-file", "~/.config/wherry/config.yaml"},
		"default":  {"host", "init"},
	} {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if err := run(args, &stdout, &stderr); err == nil || !strings.Contains(err.Error(), "missing.yaml") || strings.Contains(err.Error(), "bad.yaml") {
				t.Fatalf("host init = %v, want the default path's config read and its bundle looked for", err)
			}
		})
	}
}

func TestHostCommandsReadConfigFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.yaml")
	for name, args := range map[string][]string{
		"host init":       {"host", "init", "--config-file", missing},
		"host key rotate": {"host", "key", "rotate", "--config-file", missing},
		"host receivers":  {"host", "receivers", "--config-file", missing},
		"host submit":     {"host", "submit", "--config-file", missing, "--prompt", "work"},
		"host cancel":     {"host", "cancel", "--config-file", missing, "run-a"},
		"host wait":       {"host", "wait", "run-a", "--config-file", missing},
		"host logs":       {"host", "logs", "run-a", "--config-file", missing},
		"host status":     {"host", "status", "--config-file", missing, "run-a"},
		"host events":     {"host", "events", "--config-file", missing, "run-a"},
		"host mcp":        {"host", "mcp", "--config-file", missing},
		"submit":          {"submit", "--config-file", missing, "--prompt", "work"},
		"cancel":          {"cancel", "--config-file", missing, "run-a"},
		"wait":            {"wait", "run-a", "--config-file", missing},
		"logs":            {"logs", "run-a", "--config-file", missing},
		"status":          {"status", "--config-file", missing, "run-a"},
		"events":          {"events", "--config-file", missing, "run-a"},
	} {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := run(args, &stdout, &stderr)
			if err == nil || !strings.Contains(err.Error(), "missing.yaml") {
				t.Fatalf("run(%q) = %v, want the configured path read", args, err)
			}
		})
	}
}

func TestPoolConfigReadsDurations(t *testing.T) {
	pool, err := os.ReadFile(filepath.Join("testdata", "pool-nomad.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "pool.yaml")
	if err := os.WriteFile(file, append(pool, "start_by_limit: 36h\n"...), 0o600); err != nil {
		t.Fatal(err)
	}
	terms, err := readPoolConfig(file, false)
	if err != nil || terms.LockDurationMS != 15000 || terms.StartByLimitMS != operator.Millis(36*time.Hour/time.Millisecond) {
		t.Fatalf("terms lock %d start_by_limit %d, %v; want 15s and 36h in milliseconds", terms.LockDurationMS, terms.StartByLimitMS, err)
	}
	old := bytes.Replace(pool, []byte("lock_duration: 15s"), []byte("lock_duration_ms: 15000"), 1)
	if err := os.WriteFile(file, old, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readPoolConfig(file, false); err == nil {
		t.Fatal("pool config accepted lock_duration_ms")
	}
}
