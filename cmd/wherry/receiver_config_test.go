package main

import (
	"bytes"
	"io"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/benemon/wherry/internal/admin"
)

const labPlugin = `config:
  address: https://nomad.example:4646
  ca_file: /secrets/nomad-ca.crt
  namespace: wherry
`

const labPodman = `name: nomad-podman
agent: claude
labels:
  isolation: container
config:
  node_pool: default
  runtime:
    driver: podman
    config:
      image: localhost/wherry-agent:test
`

const labExec = `name: nomad-exec
agent: claude
labels:
  isolation: host
config:
  node_pool: hosts
  runtime:
    driver: exec
    config:
      command: /bin/sh
`

func writeTree(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, contents := range files {
		path := filepath.Join(dir, name)
		if strings.HasSuffix(name, "/") {
			if err := os.MkdirAll(path, 0o700); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func labTree(t *testing.T, replace map[string]string, remove ...string) string {
	t.Helper()
	files := map[string]string{
		"receiver.yaml":                               "capacity: 2\n",
		"plugins/nomad/plugin.yaml":                   labPlugin,
		"plugins/nomad/profiles/10-nomad-podman.yaml": labPodman,
		"plugins/nomad/profiles/20-nomad-exec.yaml":   labExec,
	}
	maps.Copy(files, replace)
	for _, name := range remove {
		delete(files, name)
	}
	return writeTree(t, files)
}

// indent indents every line of document by prefix.
func indent(document, prefix string) string {
	lines := strings.Split(strings.TrimSuffix(document, "\n"), "\n")
	for i := range lines {
		lines[i] = prefix + lines[i]
	}
	return strings.Join(lines, "\n") + "\n"
}

// item turns a document into an entry of a YAML list indented by prefix.
func item(document, prefix string) string {
	return prefix + "- " + strings.TrimPrefix(indent(document, prefix+"  "), prefix+"  ")
}

func TestReceiverConfigDirectoryMatchesSingleFile(t *testing.T) {
	single := writeTree(t, map[string]string{"receiver.yaml": "capacity: 2\nplugins:\n  nomad:\n" + indent(labPlugin, "    ") + "    profiles:\n" +
		item(labPodman, "      ") + item(labExec, "      ")})
	want, err := readReceiverConfig(filepath.Join(single, "receiver.yaml"), false)
	if err != nil {
		t.Fatal(err)
	}
	got, err := readReceiverConfig(labTree(t, nil), true)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("directory config = %+v, want %+v", got, want)
	}
}

// TestReceiverConfigExampleParses reads the reference's complete receiver configuration in both forms and checks it
// against the example pool's terms.
func TestReceiverConfigExampleParses(t *testing.T) {
	want, err := readReceiverConfig(filepath.Join("testdata", "example-receiver.yaml"), false)
	if err != nil {
		t.Fatal(err)
	}
	got, err := readReceiverConfig(filepath.Join("testdata", "example-receiver"), true)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("directory config = %+v, want %+v", got, want)
	}
	if len(want.Profiles) != 2 || want.Profiles[0].Name != "container" || want.Profiles[1].Name != "host" || want.Profiles[1].Config["credential"] != "nomad-token" {
		t.Fatalf("example profiles %+v; want container then host, each with the plugin's credential", want.Profiles)
	}
	terms, err := readPoolConfig(filepath.Join("testdata", "example-pool.yaml"), false)
	if err != nil {
		t.Fatal(err)
	}
	want.Receiver, want.Pool = "nomad-1", "build"
	if err := admin.ValidateConfig(want, terms); err != nil {
		t.Fatal(err)
	}
	documented(t, "example-receiver.yaml", "example-receiver/receiver.yaml", "example-receiver/plugins/nomad/plugin.yaml",
		"example-receiver/plugins/nomad/profiles/10-container.yaml", "example-receiver/plugins/nomad/profiles/20-host.yaml")
}

// A profile runs with its plugin's config merged into its own, and takes its plugin's type.
func TestReceiverConfigMergesThePluginConfigIntoEachProfile(t *testing.T) {
	config, err := readReceiverConfig(labTree(t, nil), true)
	if err != nil {
		t.Fatal(err)
	}
	want := admin.ProfileSettings{Name: "nomad-exec", Plugin: "nomad", Agent: "claude", Labels: map[string]string{"isolation": "host"}, Config: map[string]any{
		"address": "https://nomad.example:4646", "ca_file": "/secrets/nomad-ca.crt", "namespace": "wherry",
		"node_pool": "hosts", "runtime": map[string]any{"driver": "exec", "config": map[string]any{"command": "/bin/sh"}},
	}}
	if len(config.Profiles) != 2 || !reflect.DeepEqual(config.Profiles[1], want) {
		t.Fatalf("profiles %+v; want nomad-exec as %+v", config.Profiles, want)
	}
	config, err = readReceiverConfig(labTree(t, nil, "plugins/nomad/plugin.yaml"), true)
	if err != nil || config.Profiles[1].Config["address"] != nil || config.Profiles[1].Config["node_pool"] != "hosts" {
		t.Fatalf("without plugin.yaml: %+v, %v; want each profile's own config alone", config.Profiles, err)
	}
}

// Within a plugin, routing order is profile order: list order in a single file and file name order in a directory.
func TestReceiverConfigKeepsProfileOrderWithinAPlugin(t *testing.T) {
	config, err := readReceiverConfig(labTree(t, map[string]string{
		"plugins/nomad/profiles/10-nomad-podman.yaml": labExec, "plugins/nomad/profiles/20-nomad-exec.yaml": labPodman,
	}), true)
	if err != nil || config.Profiles[0].Name != "nomad-exec" || config.Profiles[1].Name != "nomad-podman" {
		t.Fatalf("profiles %+v, %v; want nomad-exec first, as its file sorts first", config.Profiles, err)
	}
	file := writeTree(t, map[string]string{"receiver.yaml": "plugins:\n  nomad:\n    profiles:\n" + item(labExec, "      ") + item(labPodman, "      ")})
	config, err = readReceiverConfig(filepath.Join(file, "receiver.yaml"), false)
	if err != nil || config.Profiles[0].Name != "nomad-exec" || config.Profiles[1].Name != "nomad-podman" {
		t.Fatalf("profiles %+v, %v; want list order", config.Profiles, err)
	}
}

// Plugins are visited sorted by type, whatever order the file lists them in, so host and receiver compute one match.
func TestReceiverConfigVisitsPluginsSortedByType(t *testing.T) {
	other := strings.Replace(labPodman, "name: nomad-podman", "name: zeta-first", 1)
	file := writeTree(t, map[string]string{"receiver.yaml": "plugins:\n  zeta:\n    profiles:\n" + item(other, "      ") +
		"  alpha:\n    profiles:\n" + item(labExec, "      ") + "  mid:\n    profiles:\n" + item(labPodman, "      ")})
	for range 10 {
		config, err := readReceiverConfig(filepath.Join(file, "receiver.yaml"), false)
		if err != nil {
			t.Fatal(err)
		}
		var types []string
		for _, profile := range config.Profiles {
			types = append(types, profile.Plugin)
		}
		if !reflect.DeepEqual(types, []string{"alpha", "mid", "zeta"}) {
			t.Fatalf("profiles of types %v; want them sorted by type", types)
		}
	}
	config, err := readReceiverConfig(labTree(t, map[string]string{"plugins/alpha/profiles/10-x.yaml": other}), true)
	if err != nil || config.Profiles[0].Plugin != "alpha" || config.Profiles[1].Plugin != "nomad" {
		t.Fatalf("directory profiles %+v, %v; want alpha's before nomad's", config.Profiles, err)
	}
}

func TestReceiverConfigRefusesAKeySetByPluginAndProfile(t *testing.T) {
	dir := labTree(t, map[string]string{"plugins/nomad/profiles/20-nomad-exec.yaml": labExec + "  namespace: other\n"})
	_, err := readReceiverConfig(dir, true)
	if err == nil || !strings.Contains(err.Error(), "20-nomad-exec.yaml") || !strings.Contains(err.Error(), `profile "nomad-exec"`) ||
		!strings.Contains(err.Error(), `"namespace"`) || !strings.Contains(err.Error(), "plugin nomad") {
		t.Fatalf("readReceiverConfig = %v, want an error naming the profile, its file, the key and the plugin", err)
	}
}

// Profile names are unique across the receiver, not only within a plugin.
func TestReceiverConfigRefusesAProfileNameTwiceAcrossPlugins(t *testing.T) {
	_, err := readReceiverConfig(labTree(t, map[string]string{"plugins/other/profiles/10-again.yaml": labExec}), true)
	if err == nil || !strings.Contains(err.Error(), `profile name "nomad-exec" is repeated`) {
		t.Fatalf("readReceiverConfig = %v, want the repeated name refused", err)
	}
	file := writeTree(t, map[string]string{"receiver.yaml": "plugins:\n  nomad:\n    profiles:\n" + item(labExec, "      ") +
		"  other:\n    profiles:\n" + item(labExec, "      ")})
	if _, err := readReceiverConfig(filepath.Join(file, "receiver.yaml"), false); err == nil || !strings.Contains(err.Error(), `profile name "nomad-exec" is repeated`) {
		t.Fatalf("single file: readReceiverConfig = %v, want the repeated name refused", err)
	}
}

func TestReceiverConfigDirectoryRejectsInvalidLayout(t *testing.T) {
	for name, test := range map[string]struct {
		replace map[string]string
		remove  []string
		want    string
	}{
		"unknown receiver key":  {replace: map[string]string{"receiver.yaml": "capacity: 2\nplugins: {}\n"}, want: "invalid or unknown field at line"},
		"unknown plugin key":    {replace: map[string]string{"plugins/nomad/plugin.yaml": labPlugin + "region: global\n"}, want: "invalid or unknown field at line"},
		"profiles in plugin":    {replace: map[string]string{"plugins/nomad/plugin.yaml": labPlugin + "profiles: []\n"}, want: "invalid or unknown field at line"},
		"unknown profile key":   {replace: map[string]string{"plugins/nomad/profiles/20-nomad-exec.yaml": labExec + "type: nomad\n"}, want: "invalid or unknown field at line"},
		"duplicate profile":     {replace: map[string]string{"plugins/nomad/profiles/30-again.yaml": labExec}, want: `30-again.yaml: profile name "nomad-exec" is repeated`},
		"invalid type":          {replace: map[string]string{"plugins/Nomad/profiles/10-x.yaml": strings.Replace(labExec, "nomad-exec", "x", 1)}, want: `adapter type "Nomad" is not a valid name`},
		"no profiles":           {replace: map[string]string{"plugins/nomad/profiles/": ""}, remove: []string{"plugins/nomad/profiles/10-nomad-podman.yaml", "plugins/nomad/profiles/20-nomad-exec.yaml"}, want: "holds no profiles"},
		"no plugins":            {replace: map[string]string{"plugins/": ""}, remove: []string{"plugins/nomad/plugin.yaml", "plugins/nomad/profiles/10-nomad-podman.yaml", "plugins/nomad/profiles/20-nomad-exec.yaml"}, want: "holds no plugins"},
		"non-yaml profile file": {replace: map[string]string{"plugins/nomad/profiles/30-typo.yml": labExec}, want: "30-typo.yml is not a .yaml file"},
		"nested directory":      {replace: map[string]string{"plugins/nomad/profiles/nested/30-x.yaml": labExec}, want: "nested is not a .yaml file"},
		"stray plugin file":     {replace: map[string]string{"plugins/nomad/notes.txt": "x"}, want: "notes.txt does not belong in a plugin directory"},
		"file among plugins":    {replace: map[string]string{"plugins/nomad.yaml": labPlugin}, want: "nomad.yaml is not a plugin directory"},
		"stray top-level file":  {replace: map[string]string{"notes.txt": "x"}, want: "notes.txt does not belong"},
		"old platforms":         {replace: map[string]string{"platforms/nomad.yaml": labPlugin}, want: "platforms does not belong"},
		"missing receiver.yaml": {remove: []string{"receiver.yaml"}, want: "receiver.yaml: no such file"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := readReceiverConfig(labTree(t, test.replace, test.remove...), true)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("readReceiverConfig = %v, want an error containing %q", err, test.want)
			}
		})
	}
}

func TestReceiverConfigFileRefusesTheOldSchema(t *testing.T) {
	file := writeTree(t, map[string]string{"receiver.yaml": "capacity: 2\nplatforms: []\nadapters: []\n"})
	if _, err := readReceiverConfig(filepath.Join(file, "receiver.yaml"), false); err == nil || !strings.Contains(err.Error(), "invalid or unknown field") {
		t.Fatalf("readReceiverConfig = %v; want platforms and adapters refused", err)
	}
}

func TestReceiverConfigErrorsQuoteNoValue(t *testing.T) {
	const token = "nomad-token-value-secret"
	for name, replace := range map[string]map[string]string{
		"mistyped value":  {"receiver.yaml": "capacity: " + token + "\n"},
		"misplaced value": {"plugins/nomad/plugin.yaml": labPlugin + token + "\n"},
	} {
		_, err := readReceiverConfig(labTree(t, replace), true)
		// The decoder quotes a value's first 7 bytes when it is longer than 10.
		if err == nil || strings.Contains(err.Error(), token[:7]) {
			t.Fatalf("%s: readReceiverConfig = %v; want an error without the value", name, err)
		}
	}
}

func TestReceiverConfigDirectoryIgnoresHiddenFiles(t *testing.T) {
	want, err := readReceiverConfig(labTree(t, nil), true)
	if err != nil {
		t.Fatal(err)
	}
	for _, hidden := range []string{".DS_Store", "plugins/.DS_Store", "plugins/nomad/.DS_Store", "plugins/nomad/profiles/.DS_Store"} {
		got, err := readReceiverConfig(labTree(t, map[string]string{hidden: "x"}), true)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("with %s: config = %+v, %v; want the config without it", hidden, got, err)
		}
	}
}

func TestReceiverConfigFileRefusesIdentity(t *testing.T) {
	file := writeTree(t, map[string]string{"receiver.yaml": "receiver: receiver-a\ncapacity: 2\n"})
	if _, err := readReceiverConfig(filepath.Join(file, "receiver.yaml"), false); err == nil {
		t.Fatal("a receiver config file naming its receiver was accepted")
	}
}

func TestAdminArgsRefusesARepeatedOption(t *testing.T) {
	if _, _, err := adminArgs([]string{"receiver", "add", "r", "--pool", "a", "--pool", "b"}); err == nil {
		t.Error("a repeated --pool was accepted")
	}
}

func TestReceiverDefaults(t *testing.T) {
	config, err := readReceiverConfig(labTree(t, map[string]string{"receiver.yaml": "{}\n"}), true)
	if err != nil || config.Capacity != 1 {
		t.Fatalf("capacity %d, %v; want 1 when receiver.yaml leaves it out", config.Capacity, err)
	}
	var stderr bytes.Buffer
	local := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(local, []byte("bundle: "+filepath.Join(t.TempDir(), "missing.yaml")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	err = run([]string{"receiver", "run", "--config-file", local}, io.Discard, &stderr)
	if err == nil || !strings.Contains(err.Error(), "read bundle") {
		t.Fatalf("receiver run with a config naming only its bundle = %v; want it to read the bundle", err)
	}
}
