package client

import (
	"os"
	"path/filepath"
	"testing"
)

// A configuration away from the default path takes its own bundle, host key and state directory, named from its whole
// path, so configurations with alike names share nothing; the default path keeps the configuration directory's files
// however it is reached: relatively, through a symlink, or through a symlinked configuration directory.
func TestDefaultPathsKeepConfigurationsApart(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(dir, "xdg"))
	t.Setenv("XDG_STATE_HOME", filepath.Join(dir, "state"))
	for _, test := range []struct {
		path                  string
		bundle, key, stateDir string
	}{
		{filepath.Join(dir, "a"), filepath.Join(dir, "a.bundle.yaml"), filepath.Join(dir, "a.host.key"), filepath.Join(dir, "a.state")},
		{filepath.Join(dir, "a.yaml"), filepath.Join(dir, "a.yaml.bundle.yaml"), filepath.Join(dir, "a.yaml.host.key"), filepath.Join(dir, "a.yaml.state")},
		{filepath.Join(dir, "missing", "config.yaml"), filepath.Join(dir, "missing", "config.yaml.bundle.yaml"), filepath.Join(dir, "missing", "config.yaml.host.key"), filepath.Join(dir, "missing", "config.yaml.state")},
	} {
		paths, err := DefaultPaths(test.path)
		if err != nil {
			t.Fatal(err)
		}
		if paths.Default || paths.Config != test.path || paths.Bundle != test.bundle || paths.HostKey != test.key || paths.StateDir != test.stateDir {
			t.Errorf("DefaultPaths(%s) = %+v; want %s, %s and %s beside it", test.path, paths, test.bundle, test.key, test.stateDir)
		}
	}
	configDir := filepath.Join(dir, "xdg", "wherry")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	standard := filepath.Join(configDir, "config.yaml")
	link := filepath.Join(dir, "link.yaml")
	if err := os.Symlink(standard, link); err != nil {
		t.Fatal(err)
	}
	linkedDir := filepath.Join(dir, "linked")
	if err := os.Symlink(filepath.Join(dir, "xdg"), linkedDir); err != nil {
		t.Fatal(err)
	}
	t.Chdir(configDir)
	for _, path := range []string{standard, "config.yaml", filepath.Join("..", "wherry", "config.yaml"), link, filepath.Join(linkedDir, "wherry", "config.yaml")} {
		paths, err := DefaultPaths(path)
		if err != nil {
			t.Fatal(err)
		}
		if !paths.Default || paths.Config != standard || paths.Bundle != filepath.Join(configDir, "bundle.yaml") || paths.HostKey != filepath.Join(configDir, "host.key") || paths.StateDir != filepath.Join(dir, "state", "wherry") {
			t.Errorf("DefaultPaths(%s) = %+v; want the default files", path, paths)
		}
	}
	// The symlink's target does not exist yet; the configuration it will name is still the default.
	if paths, err := DefaultPaths(link); err != nil || !paths.Default {
		t.Fatalf("DefaultPaths(dangling symlink to the default) = %+v, %v; want the default files", paths, err)
	}
	t.Setenv("XDG_CONFIG_HOME", linkedDir)
	if paths, err := DefaultPaths(standard); err != nil || !paths.Default {
		t.Fatalf("DefaultPaths(%s) under a symlinked XDG_CONFIG_HOME = %+v, %v; want the default files", standard, paths, err)
	}
}

// LoadConfig defaults host_key and state_dir to the configuration's own files away from the default path, and keeps
// explicit ones.
func TestLoadConfigDefaultsHostKeyBesideTheConfiguration(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(dir, "xdg"))
	t.Setenv("XDG_STATE_HOME", filepath.Join(dir, "state"))
	write := func(name, contents string) string {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	own := write("a.yaml", "bundle: bundle.yaml\n")
	config, err := LoadConfig(own)
	if err != nil {
		t.Fatal(err)
	}
	if config.HostKey != own+".host.key" || config.StateDir != own+".state" {
		t.Fatalf("LoadConfig(%s) defaulted host_key %s and state_dir %s; want %s.host.key and %s.state", own, config.HostKey, config.StateDir, own, own)
	}
	explicit := write("b.yaml", "bundle: bundle.yaml\nhost_key: "+filepath.Join(dir, "own.key")+"\nstate_dir: "+filepath.Join(dir, "own-state")+"\n")
	if config, err = LoadConfig(explicit); err != nil || config.HostKey != filepath.Join(dir, "own.key") || config.StateDir != filepath.Join(dir, "own-state") {
		t.Fatalf("LoadConfig(%s) = %+v, %v; want host_key and state_dir kept", explicit, config, err)
	}
	standard := write(filepath.Join("xdg", "wherry", "config.yaml"), "bundle: bundle.yaml\n")
	if config, err = LoadConfig(standard); err != nil || config.HostKey != filepath.Join(dir, "xdg", "wherry", "host.key") || config.StateDir != filepath.Join(dir, "state", "wherry") {
		t.Fatalf("LoadConfig(%s) = %+v, %v; want the configuration directory's host.key and the state directory", standard, config, err)
	}
}
