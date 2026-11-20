package client

import (
	"os"
	"path/filepath"
	"testing"
)

// A configuration away from the default path takes its own bundle, host key and state directory, named from its whole
// path as given, so configurations with alike names share nothing; the default path keeps the configuration directory's
// files however it is reached: relatively, or, once it exists, through a symlink, a hard link or a symlinked
// configuration directory. No symlink is resolved: a link to a file not yet written is a configuration of its own.
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
	wantDefault := func(path string) {
		t.Helper()
		paths, err := DefaultPaths(path)
		if err != nil {
			t.Fatal(err)
		}
		absolute, err := filepath.Abs(path)
		if err != nil {
			t.Fatal(err)
		}
		if !paths.Default || paths.Config != absolute || paths.Bundle != filepath.Join(configDir, "bundle.yaml") || paths.HostKey != filepath.Join(configDir, "host.key") || paths.StateDir != filepath.Join(dir, "state", "wherry") {
			t.Errorf("DefaultPaths(%s) = %+v; want the default files", path, paths)
		}
	}
	wantOwn := func(path string) {
		t.Helper()
		paths, err := DefaultPaths(path)
		if err != nil {
			t.Fatal(err)
		}
		if paths.Default || paths.Config != path || paths.Bundle != path+".bundle.yaml" || paths.HostKey != path+".host.key" || paths.StateDir != path+".state" {
			t.Errorf("DefaultPaths(%s) = %+v; want its own files beside it, named from the path as given", path, paths)
		}
	}
	for _, path := range []string{standard, "config.yaml", filepath.Join("..", "wherry", "config.yaml")} {
		wantDefault(path)
	}
	// Before the default is written nothing can be the same file as it, and no symlink is followed: a link to where it
	// will be is a configuration of its own, with its files beside the link.
	wantOwn(link)
	wantOwn(filepath.Join(linkedDir, "wherry", "config.yaml"))
	if err := os.WriteFile(standard, []byte("bundle: bundle.yaml\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	hard := filepath.Join(dir, "hard.yaml")
	if err := os.Link(standard, hard); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{standard, "config.yaml", filepath.Join("..", "wherry", "config.yaml"), link, hard, filepath.Join(linkedDir, "wherry", "config.yaml")} {
		wantDefault(path)
	}
	// A symlink away from the default derives its files from its own path, whether or not its target exists.
	alias, target := filepath.Join(dir, "alias.yaml"), filepath.Join(dir, "target.yaml")
	if err := os.Symlink(target, alias); err != nil {
		t.Fatal(err)
	}
	wantOwn(alias)
	if err := os.WriteFile(target, []byte("bundle: bundle.yaml\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	wantOwn(alias)
	wantOwn(target)
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
