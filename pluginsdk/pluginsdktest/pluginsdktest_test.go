package pluginsdktest_test

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/benemon/wherry/pluginsdk"
	"github.com/benemon/wherry/pluginsdk/pluginsdktest"
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
	p, err := pluginsdk.Launch(func() (string, error) { return plugin, nil }, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	factory := func(settings pluginsdk.Settings, _ hclog.Logger) (pluginsdk.Plugin, error) {
		return p.Configure(settings)
	}
	pluginsdktest.Run(t, factory,
		pluginsdk.Settings{Name: "completes", Agent: "claude", Config: []byte(`{"jobs":true,"finish":true,"inline_limit":4096}`)},
		pluginsdk.Settings{Name: "never-starts", Agent: "claude", Config: []byte(`{"jobs":true,"never_started":"image pull failed","inline_limit":4096}`)},
		4096)
	file, err := os.Open(record)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	delivered := false
	for scanner := bufio.NewScanner(file); scanner.Scan(); {
		var entry struct {
			Call string         `json:"call"`
			Run  *pluginsdk.Run `json:"run"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			t.Fatal(err)
		}
		delivered = delivered || entry.Call == "dispatch" && entry.Run.Profile == "completes" && reflect.DeepEqual(entry.Run.Credentials, pluginsdktest.Credentials)
	}
	if !delivered {
		t.Fatalf("no dispatch carried %+v across the protocol", pluginsdktest.Credentials)
	}
}
