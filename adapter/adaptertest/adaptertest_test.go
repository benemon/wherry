package adaptertest_test

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/benemon/wherry/adapter"
	"github.com/benemon/wherry/adapter/adaptertest"
	"github.com/hashicorp/go-hclog"
)

// The suite passes against the in-repo test plugin through the protocol, and the plugin receives each credential at
// its target.
func TestConformanceThroughThePlugin(t *testing.T) {
	plugin := filepath.Join(t.TempDir(), "wherry-plugin-fake")
	if output, err := exec.Command("go", "build", "-o", plugin, "../internal/testplugin").CombinedOutput(); err != nil {
		t.Fatalf("build test plugin: %v\n%s", err, output)
	}
	record := filepath.Join(t.TempDir(), "record.jsonl")
	t.Setenv("WHERRY_TESTPLUGIN_RECORD", record)
	p, err := adapter.Launch(func() (string, error) { return plugin, nil }, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	factory := func(settings adapter.Settings, _ hclog.Logger) (adapter.Adapter, error) { return p.Configure(settings) }
	adaptertest.Run(t, factory,
		adapter.Settings{Name: "completes", Agent: "claude", Config: []byte(`{"jobs":true,"finish":true,"inline_limit":4096}`)},
		adapter.Settings{Name: "never-starts", Agent: "claude", Config: []byte(`{"jobs":true,"never_started":"image pull failed","inline_limit":4096}`)},
		4096)
	file, err := os.Open(record)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	delivered := false
	for scanner := bufio.NewScanner(file); scanner.Scan(); {
		var entry struct {
			Call string       `json:"call"`
			Run  *adapter.Run `json:"run"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			t.Fatal(err)
		}
		delivered = delivered || entry.Call == "dispatch" && entry.Run.Adapter == "completes" && reflect.DeepEqual(entry.Run.Credentials, adaptertest.Credentials)
	}
	if !delivered {
		t.Fatalf("no dispatch carried %+v across the protocol", adaptertest.Credentials)
	}
}
