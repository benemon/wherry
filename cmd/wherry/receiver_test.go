package main

import (
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/benemon/wherry/internal/admin"
)

func TestNewAdapterRejectsUnknownType(t *testing.T) {
	_, err := newAdapter("build", admin.AdapterSettings{Name: "k8s", Type: "kubernetes", Agent: "claude", Config: map[string]any{}}, "")
	if err == nil || !strings.Contains(err.Error(), `unknown type "kubernetes"`) {
		t.Fatalf("got %v, want unknown type error", err)
	}
}

func TestNewAdapterDecodesSettingsStrictly(t *testing.T) {
	config := map[string]any{"address": "http://127.0.0.1:1", "namespace": "wherry", "runtime": map[string]any{"driver": "exec"}}
	if _, err := newAdapter("build", admin.AdapterSettings{Name: "lab", Type: "nomad", Agent: "claude", Credential: "token", Config: config}, ""); err == nil || !strings.Contains(err.Error(), "credential_env") {
		t.Fatalf("got %v, want credential_env required", err)
	}
	unknown := map[string]any{"address": "http://127.0.0.1:1", "runtime": map[string]any{"driver": "exec"}, "token": "x"}
	if _, err := newAdapter("build", admin.AdapterSettings{Name: "lab", Type: "nomad", Agent: "claude", Config: unknown}, ""); err == nil {
		t.Fatal("accepted adapter settings with an unknown member")
	}
	labels := map[string]string{"zone": "dc1", "example.com/gpu": "none"}
	named, err := newAdapter("build", admin.AdapterSettings{Name: "lab", Type: "nomad", Agent: "claude", Labels: labels, Config: config}, "platform-token")
	if err != nil || named.Name != "lab" || named.Agent != "claude" || !named.Git || !maps.Equal(named.Labels, labels) {
		t.Fatalf("adapter %#v, err %v; want a git-delivering adapter for its agent with its declared labels", named, err)
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

func TestPlatformTokenFallsBackToNomadToken(t *testing.T) {
	t.Setenv("NOMAD_TOKEN", "workload-identity-token")
	if got := platformToken(admin.Bundle{}, "lab"); got != "workload-identity-token" {
		t.Fatalf("token %q, want the environment's when the bundle has none", got)
	}
	bundle := admin.Bundle{Platform: map[string]string{"lab": "bundle-token"}}
	if got := platformToken(bundle, "lab"); got != "bundle-token" {
		t.Fatalf("token %q, want the bundle's", got)
	}
	if got := platformToken(bundle, "edge"); got != "workload-identity-token" {
		t.Fatalf("token %q for an adapter the bundle names no credential for, want the environment's", got)
	}
}
