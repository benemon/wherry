package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/pluginsdk"
	"github.com/prometheus/client_golang/prometheus"
)

// quickstart is the path of a file the quickstart's walkthrough writes.
func quickstart(name string) string {
	return filepath.Join("testdata", "quickstart", name)
}

// TestQuickstartFiles reads each file the quickstart writes with the reader that reads it in use, checks the receiver
// configuration against the pool terms, and checks that each credential the pool terms and the receiver configuration
// name has a source in the host's and the receiver's configuration. docs/quickstart.md holds each file whole.
func TestQuickstartFiles(t *testing.T) {
	terms, err := readPoolConfig(quickstart("build-terms.yaml"), false)
	if err != nil {
		t.Fatal(err)
	}
	config, err := readReceiverConfig(quickstart("nomad-1.yaml"), false)
	if err != nil {
		t.Fatal(err)
	}
	config.Receiver, config.Pool, config.PluginSHA256 = "nomad-1", "build", map[string]string{"nomad": fakeSHA256}
	if err := admin.ValidateConfig(config, terms); err != nil {
		t.Fatal(err)
	}
	if len(config.Profiles) != 2 || config.Profiles[0].Name != "container" || config.Profiles[1].Name != "host" {
		t.Fatalf("profiles %+v; want container then host", config.Profiles)
	}
	runtime := config.Profiles[1].Config["runtime"].(map[string]any)
	if driver := runtime["config"].(map[string]any); runtime["driver"] != "exec" || driver["image"] != nil || driver["command"] != "/bin/sh" {
		t.Fatalf("host runtime %+v; want the exec driver with the inherited command and no image", runtime)
	}
	local, err := loadReceiverLocalConfig(quickstart("receiver-config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", t.TempDir())
	host, err := client.LoadConfig(quickstart("host-config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, agent := range terms.Agents {
		for _, credential := range agent.Credentials {
			if _, ok := host.Credentials[credential.Name]; !ok {
				t.Errorf("agent %s's credential %s has no source in the host configuration", agent.Name, credential.Name)
			}
		}
	}
	for _, profile := range config.Profiles {
		if _, ok := local.Credentials[profile.Config["credential"].(string)]; !ok {
			t.Errorf("profile %s's credential has no source in the receiver's local configuration", profile.Name)
		}
	}
	documented(t, "quickstart.md", "quickstart/build-terms.yaml", "quickstart/nomad-1.yaml", "quickstart/receiver-config.yaml", "quickstart/host-config.yaml")
}

// TestQuickstartMissingSourceError finds in the quickstart, the configuration reference and operations the line the receiver prints
// exits with for a platform credential with no source, as receiver.Run wraps the profile's error.
func TestQuickstartMissingSourceError(t *testing.T) {
	config, err := readReceiverConfig(quickstart("nomad-1.yaml"), false)
	if err != nil {
		t.Fatal(err)
	}
	unsourced := &plugins{dir: t.TempDir(), registry: prometheus.NewRegistry(), running: map[string]*pluginsdk.Process{}}
	_, err = unsourced.profile("build", config.Profiles[0], fakeSHA256)
	if err == nil {
		t.Fatal("configured a profile whose credential has no source")
	}
	line := "profile " + config.Profiles[0].Name + ": " + err.Error() + "\n"
	for _, page := range []string{"quickstart.md", "configuration.md", "operations.md"} {
		doc, err := os.ReadFile(filepath.Join("..", "..", "docs", page))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(doc), "```\n"+line+"```\n") {
			t.Errorf("docs/%s has no code block holding %q", page, line)
		}
	}
}

// TestQuickstartAdminOutput runs the quickstart's wherry admin commands, pool create aside, on its files and finds what receiver add
// --dry-run and receiver show print in docs/quickstart.md, as two YAML code blocks.
func TestQuickstartAdminOutput(t *testing.T) {
	service := adminEnv(t)
	dir := t.TempDir()
	wherry := func(args ...string) string {
		t.Helper()
		var stdout bytes.Buffer
		if err := runAdmin(args, &stdout, io.Discard); err != nil {
			t.Fatalf("%q: %v", args, err)
		}
		return stdout.String()
	}
	// kfake reports no group.share.max.delivery.count.limit, so the pool is created on a service that knows its ceiling.
	terms, err := readPoolConfig(quickstart("build-terms.yaml"), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.PoolCreate(context.Background(), "build", terms); err != nil {
		t.Fatal(err)
	}
	wherry("pool", "entitle", "build", "--tenant", "acme")
	dryRun := wherry("receiver", "add", "nomad-1", "--pool", "build", "--config-file", quickstart("nomad-1.yaml"), "--plugin-sha256", "nomad="+fakeSHA256, "--dry-run")
	wherry("receiver", "add", "nomad-1", "--pool", "build", "--config-file", quickstart("nomad-1.yaml"), "--plugin-sha256", "nomad="+fakeSHA256, "--bundle-file", filepath.Join(dir, "nomad-1.bundle"))
	shown := wherry("receiver", "show", "nomad-1")
	wherry("host", "add", "laptop", "--tenant", "acme", "--bundle-file", filepath.Join(dir, "laptop.bundle"))
	if shown != dryRun {
		t.Fatalf("show printed\n%s\nwant what the dry run printed\n%s", shown, dryRun)
	}
	doc, err := os.ReadFile(filepath.Join("..", "..", "docs", "quickstart.md"))
	if err != nil {
		t.Fatal(err)
	}
	// The docs show an example pin in place of the test plugin's.
	if count := strings.Count(string(doc), "```yaml\n"+strings.ReplaceAll(dryRun, fakeSHA256, strings.Repeat("0123456789abcdef", 4))+"```\n"); count != 2 {
		t.Errorf("docs/quickstart.md holds what receiver add --dry-run and receiver show print in %d code blocks, want 2:\n%s", count, dryRun)
	}
}
