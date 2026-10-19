package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/benemon/wherry/internal/admin"
)

func TestNewAdapterRejectsUnknownType(t *testing.T) {
	_, err := newAdapter(admin.AdapterSettings{Name: "k8s", Type: "kubernetes", Agent: "claude", Config: map[string]any{}}, "")
	if err == nil || !strings.Contains(err.Error(), `unknown type "kubernetes"`) {
		t.Fatalf("got %v, want unknown type error", err)
	}
}

func TestNewAdapterDecodesSettingsStrictly(t *testing.T) {
	config := map[string]any{"address": "http://127.0.0.1:1", "namespace": "wherry", "runtimes": map[string]any{}}
	if _, err := newAdapter(admin.AdapterSettings{Name: "lab", Type: "nomad", Agent: "claude", Credential: "token", Config: config}, ""); err == nil || !strings.Contains(err.Error(), "credential_env") {
		t.Fatalf("got %v, want credential_env required", err)
	}
	unknown := map[string]any{"address": "http://127.0.0.1:1", "runtimes": map[string]any{}, "token": "x"}
	if _, err := newAdapter(admin.AdapterSettings{Name: "lab", Type: "nomad", Agent: "claude", Config: unknown}, ""); err == nil {
		t.Fatal("accepted adapter settings with an unknown member")
	}
	named, err := newAdapter(admin.AdapterSettings{Name: "lab", Type: "nomad", Agent: "claude", Config: config}, "platform-token")
	if err != nil || named.Name != "lab" || named.Agent != "claude" || !named.Git {
		t.Fatalf("adapter %#v, err %v; want a git-delivering adapter for its agent", named, err)
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
