package main

import (
	"bytes"
	"context"
	json "encoding/json/v2"
	"errors"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/twmb/franz-go/pkg/kfake"
)

// devProvision runs up's provisioning steps, operator to host, for instance on a TLS fake broker whose super user is
// dev's, and returns the ready output and the host config's path.
func devProvision(t *testing.T, instance devInstance) (string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cluster, ca := tlsBroker(t, kfake.Superuser("SCRAM-SHA-512", devSuperUser, "super-secret"))
	address := cluster.ListenAddrs()[0]
	root := connectTLS(t, address, ca, devSuperUser, "super-secret")
	shareCeilings(t, root)
	for _, dir := range []string{"tls", "kafka/cfg", "receiver/state"} {
		if err := os.MkdirAll(instance.env(dir), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for path, contents := range map[string]string{instance.env("tls", "ca.pem"): ca, instance.env("kafka", "cfg", "super-user-password"): "super-secret"} {
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	manifest := writeCache(t, instance, []byte("snapshot"))
	env := devEnv{instance: instance, manifest: manifest, driver: "docker"}
	service, err := devOperator(ctx, instance, address)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Operator.Close()
	if err := devBootstrap(ctx, env, devAgents(manifest, nil), nil, *service); err != nil {
		t.Fatal(err)
	}
	_, config, err := placeHost(ctx, &env, nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	var ready bytes.Buffer
	printReady(&ready, env, config)
	return ready.String(), config
}

// standardHostPaths are the default instance's host files at the standard paths and the standard state directory, which
// dev never writes.
func standardHostPaths(t *testing.T) ([]string, string) {
	t.Helper()
	config, err := busclient.ConfigDir()
	if err != nil {
		t.Fatal(err)
	}
	state, err := busclient.StateDir()
	if err != nil {
		t.Fatal(err)
	}
	return []string{filepath.Join(config, "config.yaml"), filepath.Join(config, "bundle.yaml"), filepath.Join(config, "host.key")}, state
}

// up's operator working directory is operator init's, and operator check passes on it.
func TestDevUpProvisionsThroughOperatorInit(t *testing.T) {
	instance := devFixture(t)
	devProvision(t, instance)
	dir := instance.env("operator")
	if err := runOperator([]string{"check", "--dir", dir}, io.Discard, io.Discard); err != nil {
		t.Fatalf("operator check on %s: %v", dir, err)
	}
	if _, err := os.Stat(instance.env("admin")); err == nil {
		t.Fatal("up wrote env/admin")
	}
	b, _ := kfakeInitBroker(t)
	initDir := t.TempDir()
	if err := b.init(t, initDir); err != nil {
		t.Fatal(err)
	}
	shape := func(dir string) map[string]string {
		files := map[string]string{}
		for _, name := range []string{"operator.yaml", "operator.password", ".gitignore"} {
			info, err := os.Stat(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			files[name] = info.Mode().String()
		}
		ignored, _ := os.ReadFile(filepath.Join(dir, ".gitignore"))
		files[".gitignore contents"] = string(ignored)
		var config map[string]any
		if err := readYAML(filepath.Join(dir, "operator.yaml"), &config); err != nil {
			t.Fatal(err)
		}
		for key := range config {
			files["operator.yaml "+key] = "set"
		}
		return files
	}
	if got, want := shape(dir), shape(initDir); !maps.Equal(got, want) {
		t.Fatalf("env/operator %v; want operator init's %v", got, want)
	}
	for _, bundle := range []string{filepath.Join("bundles", "hosts", devTenant, devHost+".bundle"), filepath.Join("bundles", "receivers", devReceiver+".bundle")} {
		if info, err := os.Stat(filepath.Join(dir, bundle)); err != nil || info.Mode().Perm() != 0o600 {
			t.Errorf("%s: %v, %v; want a 0600 bundle", bundle, info, err)
		}
	}
}

// The default instance writes the standard host paths when none is there, and down, or down --reset, removes them.
func TestDevUpWritesTheStandardHostPaths(t *testing.T) {
	for _, down := range [][]string{{"dev", "down"}, {"dev", "down", "--reset"}} {
		t.Run(strings.Join(down, " "), func(t *testing.T) {
			instance := devFixture(t)
			ready, config := devProvision(t, instance)
			files, state := standardHostPaths(t)
			if config != files[0] {
				t.Fatalf("config at %s; want the standard path %s", config, files[0])
			}
			for _, path := range files {
				if _, err := os.Stat(path); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := os.Lstat(state); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("up wrote the standard state directory %s: %v", state, err)
			}
			if loaded, err := busclient.LoadConfig(config); err != nil || loaded.StateDir != instance.env("host", "state") {
				t.Errorf("state_dir %q, %v; want the instance's %s", loaded.StateDir, err, instance.env("host", "state"))
			}
			for _, want := range []string{"submit: wherry submit --prompt \"...\"\n", "claude: claude mcp add wherry-default -- wherry host mcp\n"} {
				if !strings.Contains(ready, want) {
					t.Errorf("ready output\n%s\nlacks %q", ready, want)
				}
			}
			if strings.Contains(ready, "--config-file") || strings.Contains(ready, "WHERRY_CONFIG") {
				t.Errorf("ready output names the config:\n%s", ready)
			}
			if _, _, err := runCapture(down...); err != nil {
				t.Fatal(err)
			}
			for _, path := range append(files, state) {
				if _, err := os.Stat(path); err == nil {
					t.Errorf("%v left %s", down, path)
				}
			}
		})
	}
}

// A host file changed after up wrote it may be a real host's: down leaves it and removes the rest.
func TestDevDownLeavesAChangedHostFile(t *testing.T) {
	instance := devFixture(t)
	devProvision(t, instance)
	files, _ := standardHostPaths(t)
	changed := []byte("bundle: /elsewhere/bundle.yaml\n")
	if err := os.WriteFile(files[0], changed, 0o600); err != nil {
		t.Fatal(err)
	}
	_, stderr, err := runCapture("dev", "down")
	if err != nil {
		t.Fatal(err)
	}
	if kept, err := os.ReadFile(files[0]); err != nil || !bytes.Equal(kept, changed) {
		t.Fatalf("changed config after down: %q, %v; want it left", kept, err)
	}
	if !strings.Contains(stderr, "left "+files[0]) {
		t.Errorf("down said\n%s\nwant it to name the file it left", stderr)
	}
	for _, path := range files[1:] {
		if _, err := os.Stat(path); err == nil {
			t.Errorf("down left %s, unchanged since up wrote it", path)
		}
	}
	if manifest, err := readManifest(instance); err != nil || manifest.HostFiles != nil {
		t.Fatalf("manifest after down: %+v, %v; want no host files recorded", manifest, err)
	}
}

// A host config up did not write is left byte for byte, and up writes inside the instance and names its config.
func TestDevUpLeavesAnotherHostConfig(t *testing.T) {
	instance := devFixture(t)
	files, state := standardHostPaths(t)
	real := []byte("bundle: /home/someone/.config/wherry/bundle.yaml\n")
	if err := os.MkdirAll(filepath.Dir(files[0]), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(files[0], real, 0o600); err != nil {
		t.Fatal(err)
	}
	ready, config := devProvision(t, instance)
	if config != instance.env("host", "config.yaml") {
		t.Fatalf("config at %s; want it inside the instance", config)
	}
	if kept, err := os.ReadFile(files[0]); err != nil || !bytes.Equal(kept, real) {
		t.Fatalf("existing config after up: %q, %v; want it unchanged", kept, err)
	}
	for _, path := range append(files[1:], state) {
		if _, err := os.Stat(path); err == nil {
			t.Errorf("up wrote %s beside a config it did not write", path)
		}
	}
	for _, want := range []string{"export WHERRY_CONFIG=" + shellQuote(config) + "\n", "claude mcp add wherry-default --env WHERRY_CONFIG=" + shellQuote(config) + " -- wherry host mcp\n"} {
		if !strings.Contains(ready, want) {
			t.Errorf("ready output\n%s\nlacks %q", ready, want)
		}
	}
	if _, _, err := runCapture("dev", "down"); err != nil {
		t.Fatal(err)
	}
	if kept, err := os.ReadFile(files[0]); err != nil || !bytes.Equal(kept, real) {
		t.Fatalf("existing config after down: %q, %v; want it unchanged", kept, err)
	}
}

// An instance other than default never writes the standard host paths.
func TestDevUpOtherInstanceKeepsItsHost(t *testing.T) {
	devFixture(t)
	instance, err := openInstance("other")
	if err != nil {
		t.Fatal(err)
	}
	ready, config := devProvision(t, instance)
	if config != instance.env("host", "config.yaml") {
		t.Fatalf("config at %s; want it inside the instance", config)
	}
	files, state := standardHostPaths(t)
	for _, dir := range []string{filepath.Dir(files[0]), state} {
		if entries, err := os.ReadDir(dir); !errors.Is(err, os.ErrNotExist) || len(entries) != 0 {
			t.Errorf("instance other wrote %s: %v, %v", dir, entries, err)
		}
	}
	if !strings.Contains(ready, "export WHERRY_CONFIG=") {
		t.Errorf("ready output\n%s\nlacks WHERRY_CONFIG", ready)
	}
}

// A host config that appears after up's check of the standard paths is left byte for byte, and up falls back to the
// instance, removing what it had created there.
func TestDevUpFallsBackWhenAHostConfigAppears(t *testing.T) {
	instance := devFixture(t)
	files, state := standardHostPaths(t)
	real := []byte("bundle: /home/someone/.config/wherry/bundle.yaml\n")
	devHostStep = func(path string) {
		if path != "" {
			return
		}
		if err := os.MkdirAll(filepath.Dir(files[0]), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(files[0], real, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { devHostStep = func(string) {} })
	ready, config := devProvision(t, instance)
	if config != instance.env("host", "config.yaml") {
		t.Fatalf("config at %s; want it inside the instance", config)
	}
	if kept, err := os.ReadFile(files[0]); err != nil || !bytes.Equal(kept, real) {
		t.Fatalf("config created after the check: %q, %v; want it unchanged", kept, err)
	}
	for _, path := range append(files[1:], state) {
		if _, err := os.Lstat(path); err == nil {
			t.Errorf("up left %s beside a config it did not create", path)
		}
	}
	if !strings.Contains(ready, "export WHERRY_CONFIG="+shellQuote(config)+"\n") {
		t.Errorf("ready output\n%s\nlacks WHERRY_CONFIG", ready)
	}
}

// A recorded file replaced by another file or a symlink with the same bytes is not the one up created: down leaves it
// and names it.
func TestDevDownLeavesAReplacedHostFile(t *testing.T) {
	for name, replace := range map[string]func(t *testing.T, path string){
		"file": func(t *testing.T, path string) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path+".new", data, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(path+".new", path); err != nil {
				t.Fatal(err)
			}
		},
		"symlink": func(t *testing.T, path string) {
			moved := filepath.Join(t.TempDir(), "bundle.yaml")
			if err := os.Rename(path, moved); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(moved, path); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			instance := devFixture(t)
			devProvision(t, instance)
			files, _ := standardHostPaths(t)
			replace(t, files[1])
			_, stderr, err := runCapture("dev", "down")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := os.Lstat(files[1]); err != nil {
				t.Fatalf("down removed the replaced %s: %v", files[1], err)
			}
			if !strings.Contains(stderr, "left "+files[1]) {
				t.Errorf("down said\n%s\nwant it to name the file it left", stderr)
			}
		})
	}
}

// A key whose device and inode a crash kept up from recording is not shown to be up's by its bytes: down leaves it,
// names it, and removes the files up recorded.
func TestDevDownLeavesAKeyCreatedBeforeACrash(t *testing.T) {
	instance := devFixture(t)
	files, _ := standardHostPaths(t)
	crash := errors.New("crash")
	devHostStep = func(path string) {
		if path == files[2] {
			panic(crash)
		}
	}
	t.Cleanup(func() { devHostStep = func(string) {} })
	func() {
		defer func() {
			if recover() != crash {
				t.Fatal("up did not reach the key")
			}
		}()
		devProvision(t, instance)
	}()
	if _, err := os.Stat(files[2]); err != nil {
		t.Fatal(err)
	}
	_, stderr, err := runCapture("dev", "down")
	if err != nil {
		t.Fatal(err, stderr)
	}
	if _, err := os.Lstat(files[2]); err != nil || !strings.Contains(stderr, "left "+files[2]) {
		t.Errorf("key after down: %v; down said\n%s\nwant it left and named", err, stderr)
	}
	for _, path := range files[:2] {
		if _, err := os.Lstat(path); err == nil {
			t.Errorf("down left %s", path)
		}
	}
}

// A manifest of an older layout is refused by init; down --reset, which cannot read what it records, keeps it and names
// the standard paths.
func TestDevOlderManifestIsRefused(t *testing.T) {
	instance := devFixture(t)
	writeCache(t, instance, []byte("snapshot"))
	encoded, err := os.ReadFile(instance.cache("manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var old map[string]any
	if err := json.Unmarshal(encoded, &old); err != nil {
		t.Fatal(err)
	}
	old["version"] = 1
	old["host_files"] = map[string]any{"files": map[string]string{"/x/config.yaml": "abc"}, "state_dir": "/x/state"}
	if encoded, err = json.Marshal(old); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(instance.cache("manifest.json"), encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := runCapture("dev", "init"); err == nil || !strings.Contains(err.Error(), "has version 1") {
		t.Fatalf("init on a version 1 manifest: %v; want it refused", err)
	}
	files, _ := standardHostPaths(t)
	if _, _, err := runCapture("dev", "down", "--reset"); err == nil || !strings.Contains(err.Error(), files[0]) {
		t.Fatalf("down --reset on a version 1 manifest: %v; want it refused, naming %s", err, files[0])
	}
	if _, err := os.Stat(instance.cache("manifest.json")); err != nil {
		t.Fatalf("down --reset removed a manifest it cannot read: %v", err)
	}
	if err := os.Remove(instance.cache("manifest.json")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(t.TempDir(), "gone.json"), instance.cache("manifest.json")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := runCapture("dev", "down", "--reset"); err == nil || !strings.Contains(err.Error(), files[0]) {
		t.Fatalf("down --reset on a dangling manifest symlink: %v; want it refused, naming %s", err, files[0])
	}
	if _, err := os.Lstat(instance.cache("manifest.json")); err != nil {
		t.Fatalf("down --reset removed a manifest it cannot read: %v", err)
	}
}

// With WHERRY_CONFIG set, up keeps the host in the instance and prints its own WHERRY_CONFIG.
func TestDevUpWithWherryConfigKeepsItsHost(t *testing.T) {
	instance := devFixture(t)
	t.Setenv("WHERRY_CONFIG", filepath.Join(t.TempDir(), "config.yaml"))
	ready, config := devProvision(t, instance)
	if config != instance.env("host", "config.yaml") {
		t.Fatalf("config at %s; want it inside the instance", config)
	}
	files, state := standardHostPaths(t)
	for _, path := range append(files, state) {
		if _, err := os.Lstat(path); err == nil {
			t.Errorf("up wrote %s with WHERRY_CONFIG set", path)
		}
	}
	if !strings.Contains(ready, "export WHERRY_CONFIG="+shellQuote(config)+"\n") {
		t.Errorf("ready output\n%s\nlacks the instance's WHERRY_CONFIG", ready)
	}
}

// The ready lines for each harness, at the standard paths and in the instance.
func TestDevReadyLines(t *testing.T) {
	instance := devFixture(t)
	manifest := writeCache(t, instance, []byte("snapshot"))
	manifest.Agents = []string{"claude", "codex", "gemini"}
	config := "/x/it's/config.yaml"
	for _, c := range []struct {
		files *devHostFiles
		want  []string
	}{
		{&devHostFiles{}, []string{
			"claude: claude mcp add wherry-default -- wherry host mcp\n",
			"codex:  codex mcp add wherry-default -- wherry host mcp\n",
			"gemini: gemini mcp add wherry-default wherry host mcp\n",
		}},
		{nil, []string{
			`export WHERRY_CONFIG='/x/it'\''s/config.yaml'` + "\n",
			`claude: claude mcp add wherry-default --env WHERRY_CONFIG='/x/it'\''s/config.yaml' -- wherry host mcp` + "\n",
			`codex:  codex mcp add wherry-default --env WHERRY_CONFIG='/x/it'\''s/config.yaml' -- wherry host mcp` + "\n",
			`gemini: gemini mcp add --env WHERRY_CONFIG='/x/it'\''s/config.yaml' wherry-default wherry host mcp` + "\n",
		}},
	} {
		manifest.HostFiles = c.files
		var ready bytes.Buffer
		printReady(&ready, devEnv{instance: instance, manifest: manifest}, config)
		for _, want := range c.want {
			if !strings.Contains(ready.String(), want) {
				t.Errorf("ready output\n%s\nlacks %q", ready.String(), want)
			}
		}
	}
}

// A host file up creates is mode 0600 whatever the umask, which may withhold the owner's bits OpenFile asks for.
func TestCreateFileIgnoresTheUmask(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.key")
	defer syscall.Umask(syscall.Umask(0o277))
	if _, err := createFile(path, []byte("key")); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v, %v; want 0600", info.Mode().Perm(), err)
	}
}
