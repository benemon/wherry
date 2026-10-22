package main

import (
	"bytes"
	"crypto/ed25519"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/trust"
)

func TestConfigInputRequiresExactlyOne(t *testing.T) {
	file := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	commands := [][]string{
		{"pool", "create", "pool-a"},
		{"receiver", "add", "receiver-a"},
		{"receiver", "configure", "receiver-a"},
	}
	for _, command := range commands {
		name := strings.Join(command[:2], " ")
		t.Run(name, func(t *testing.T) {
			for label, options := range map[string][]string{
				"neither": nil,
				"both":    {"--config-file", file, "--config-dir", dir},
			} {
				t.Run(label, func(t *testing.T) {
					_, parsed, err := adminArgs(append(command, options...))
					if err != nil {
						t.Fatal(err)
					}
					_, _, err = configInput(parsed)
					if err == nil || !strings.Contains(err.Error(), "--config-file") || !strings.Contains(err.Error(), "--config-dir") {
						t.Fatalf("config input = %v, want an error naming both flags", err)
					}
				})
			}
		})
	}
	for name, options := range map[string]map[string]string{
		"directory passed to config-file": {"config-file": dir},
		"file passed to config-dir":       {"config-dir": file},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := configInput(options)
			flagName := "--config-file"
			if options["config-dir"] != "" {
				flagName = "--config-dir"
			}
			if err == nil || !strings.Contains(err.Error(), flagName) {
				t.Fatalf("config input = %v, want an error naming %s", err, flagName)
			}
		})
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

func TestAdminArgsRejectsUnknownOptions(t *testing.T) {
	for _, args := range [][]string{
		{"pool", "create", "pool-a", "--terms", "pool.yaml"},
		{"receiver", "configure", "receiver-a", "--config", "receiver.yaml"},
		{"host", "add", "tenant-a", "host-a", "--bundle", "host.yaml"},
	} {
		_, _, err := adminArgs(args)
		option := args[len(args)-2]
		if err == nil || !strings.Contains(err.Error(), option) {
			t.Fatalf("adminArgs(%q) = %v, want an error naming %s", args, err, option)
		}
	}
}

func TestHostConfigFileAndIgnoredEnvironment(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("WHERRY_"+"CONFIG", filepath.Join(t.TempDir(), "bad.json"))
	keyPath := filepath.Join(home, "host.key")
	key, err := trust.GenerateSigningKeyFile(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	configDir := filepath.Join(home, ".config", "wherry")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	config := []byte(`{"bundle":"missing.yaml","host_key":"` + keyPath + `"}`)
	defaultPath := filepath.Join(configDir, "config.json")
	if err := os.WriteFile(defaultPath, config, 0o600); err != nil {
		t.Fatal(err)
	}
	for name, args := range map[string][]string{
		"explicit": {"key", "--config-file", "~/.config/wherry/config.json"},
		"default":  {"key"},
	} {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if err := run(args, &stdout, &stderr); err != nil {
				t.Fatal(err)
			}
			if want := signed.KeyID(key.Public().(ed25519.PublicKey)) + "\n"; stdout.String() != want || stderr.Len() != 0 {
				t.Fatalf("stdout %q stderr %q, want %q", stdout.String(), stderr.String(), want)
			}
		})
	}
}

func TestHostCommandsReadConfigFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.json")
	for name, args := range map[string][]string{
		"init":      {"init", "--config-file", missing},
		"key":       {"key", "--config-file", missing},
		"receivers": {"receivers", "--config-file", missing},
		"submit":    {"submit", "--config-file", missing, "--prompt", "work"},
		"cancel":    {"cancel", "--config-file", missing, "run-a"},
		"await":     {"await", "run-a", "--config-file", missing},
		"status":    {"status", "--config-file", missing, "run-a"},
		"events":    {"events", "--config-file", missing, "run-a"},
		"cleanup":   {"cleanup", "--config-file", missing, "run-a"},
		"mcp":       {"mcp", "--config-file", missing},
	} {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := run(args, &stdout, &stderr)
			if err == nil || !strings.Contains(err.Error(), "missing.json") {
				t.Fatalf("run(%q) = %v, want the configured path read", args, err)
			}
		})
	}
}
