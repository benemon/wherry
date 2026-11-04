package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/pluginsdk"
)

// quickstart is the path of a file the quickstart's walkthrough writes.
func quickstart(name string) string {
	return filepath.Join("testdata", "quickstart", name)
}

// TestQuickstartFiles reads each file the quickstart writes with the reader that reads it in use, checks the receiver
// configuration against the pool terms, and checks that each credential the pool terms and the receiver configuration
// name has a source in the host's and the receiver's configuration. docs/install-operator.md holds the pool terms whole,
// and docs/install-host.md the host configuration; the Nomad record and local configuration come from the Nomad
// plugin's install region in wherry-plugins, which this repository does not hold.
func TestQuickstartFiles(t *testing.T) {
	terms, err := readPoolConfig(quickstart("build-terms.yaml"), false)
	if err != nil {
		t.Fatal(err)
	}
	config, err := readReceiverConfig(quickstart("nomad-1.yaml"), false)
	if err != nil {
		t.Fatal(err)
	}
	config.Receiver = "nomad-1"
	if err := operator.ValidateConfig(config, terms); err != nil {
		t.Fatal(err)
	}
	if len(config.Profiles) != 1 || config.Profiles[0].Name != "container" || config.Profiles[0].Config["credential"] != nil {
		t.Fatalf("profiles %+v; want container alone, naming no credential so the job's workload identity supplies the token", config.Profiles)
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
	if local.Bundle == "" || local.CAFile == "" || local.DrainTimeout <= 0 {
		t.Fatalf("local configuration %+v; want the example's bundle, CA and drain timeout", local)
	}
	documented(t, "install-operator.md", "quickstart/build-terms.yaml")
	documented(t, "install-host.md", "quickstart/host-config.yaml")
}

// TestQuickstartMissingSourceError finds in the configuration reference and operations, as a code block of its own or of
// a numbered step, the line the receiver exits with for a platform credential with no source, as receiver.Run wraps the
// profile's error.
func TestQuickstartMissingSourceError(t *testing.T) {
	config, err := readReceiverConfig(quickstart("nomad-1.yaml"), false)
	if err != nil {
		t.Fatal(err)
	}
	config.Profiles[0].Config["credential"] = "nomad-token"
	unsourced := &plugins{dir: t.TempDir(), running: map[string]*pluginsdk.Process{}}
	_, err = unsourced.profile("build", config.Profiles[0], fakeSHA256)
	if err == nil {
		t.Fatal("configured a profile whose credential has no source")
	}
	line := "profile " + config.Profiles[0].Name + ": " + err.Error() + "\n"
	for _, page := range []string{"configuration-receiver.md", "operations-operator.md"} {
		doc, err := os.ReadFile(filepath.Join("..", "..", "docs", page))
		if err != nil {
			t.Fatal(err)
		}
		if block := "```\n" + line + "```\n"; !strings.Contains(string(doc), block) && !strings.Contains(string(doc), inStep(block)) {
			t.Errorf("docs/%s has no code block holding %q", page, line)
		}
	}
}

// TestQuickstartOperatorOutput runs the quickstart's wherry operator commands, pool apply aside, on its files: receiver
// apply --dry-run prints what receiver show prints once the receiver is recorded.
func TestQuickstartOperatorOutput(t *testing.T) {
	receiverFile, err := filepath.Abs(quickstart("nomad-1.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	// kfake reports no group.share.max.delivery.count.limit, so the pool is created on a service that knows its ceiling.
	terms, err := readPoolConfig(quickstart("build-terms.yaml"), false)
	if err != nil {
		t.Fatal(err)
	}
	service := operatorEnv(t)
	wherry := func(args ...string) string {
		t.Helper()
		var stdout bytes.Buffer
		if err := runOperator(args, &stdout, io.Discard); err != nil {
			t.Fatalf("%q: %v", args, err)
		}
		return stdout.String()
	}
	if err := service.PoolApply(context.Background(), "build", terms, false); err != nil {
		t.Fatal(err)
	}
	wherry("tenant", "add", "acme")
	wherry("tenant", "pool", "add", "acme", "build")
	// The quickstart keeps the receiver's file in the working directory, where apply finds it.
	if err := os.Mkdir("receivers", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(receiverFile, filepath.Join("receivers", "nomad-1.yaml")); err != nil {
		t.Fatal(err)
	}
	localFixture(t, ".", "nomad-1")
	dryRun := wherry("receiver", "apply", "nomad-1", "--dry-run")
	wherry("receiver", "apply", "nomad-1")
	shown := wherry("receiver", "show", "nomad-1")
	wherry("tenant", "host", "add", "acme", "laptop")
	if shown != dryRun {
		t.Fatalf("show printed\n%s\nwant what the dry run printed\n%s", shown, dryRun)
	}
}
