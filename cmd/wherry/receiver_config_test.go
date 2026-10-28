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
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/pluginsdk"
	"github.com/prometheus/client_golang/prometheus"
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
	documented(t, "configuration.md", "example-receiver.yaml", "example-receiver/receiver.yaml", "example-receiver/plugins/nomad/plugin.yaml",
		"example-receiver/plugins/nomad/profiles/10-container.yaml", "example-receiver/plugins/nomad/profiles/20-host.yaml")
	documented(t, "operations.md", "example-receiver/receiver.yaml", "example-receiver/plugins/nomad/plugin.yaml",
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

// basePlugin is a single-file receiver config whose plugin holds a base runtime and labels, its profiles to follow.
const basePlugin = `plugins:
  nomad:
    labels: {platform: nomad, tier: base}
    config:
      address: https://nomad.example:4646
      credential: nomad-token
      runtime:
        driver: docker
        config: {image: base:1, args: [a, b]}
        resources: {cpu: 1000}
    profiles:
`

// readBase reads basePlugin, with replace applied to it, holding profiles.
func readBase(t *testing.T, replace map[string]string, profiles ...string) (admin.ReceiverConfig, error) {
	t.Helper()
	document := basePlugin
	for old, new := range replace {
		document = strings.Replace(document, old, new, 1)
	}
	for _, profile := range profiles {
		document += item(profile, "      ")
	}
	return readReceiverConfig(filepath.Join(writeTree(t, map[string]string{"receiver.yaml": document}), "receiver.yaml"), false)
}

// A profile that sets one nested key keeps every sibling its plugin sets, and the plugin's config stays its own for the
// next profile.
func TestReceiverConfigOverridesANestedKeyKeepingItsSiblings(t *testing.T) {
	config, err := readBase(t, nil, "name: claude\nagent: claude\nconfig: {runtime: {config: {image: claude:1}}}\n", "name: plain\nagent: claude\n")
	if err != nil {
		t.Fatal(err)
	}
	runtime := func(image string) map[string]any {
		return map[string]any{"address": "https://nomad.example:4646", "credential": "nomad-token", "runtime": map[string]any{
			"driver": "docker", "config": map[string]any{"image": image, "args": []any{"a", "b"}}, "resources": map[string]any{"cpu": 1000},
		}}
	}
	if !reflect.DeepEqual(config.Profiles[0].Config, runtime("claude:1")) || !reflect.DeepEqual(config.Profiles[1].Config, runtime("base:1")) {
		t.Fatalf("configs %+v and %+v; want claude's image over the plugin's runtime and plain with the plugin's", config.Profiles[0].Config, config.Profiles[1].Config)
	}
}

func TestReceiverConfigListReplacesWhole(t *testing.T) {
	config, err := readBase(t, nil, "name: claude\nagent: claude\nconfig: {runtime: {config: {args: [c]}}}\n")
	if err != nil {
		t.Fatal(err)
	}
	if got := config.Profiles[0].Config["runtime"].(map[string]any)["config"]; !reflect.DeepEqual(got, map[string]any{"image": "base:1", "args": []any{"c"}}) {
		t.Fatalf("runtime config %+v; want the profile's list in place of the plugin's", got)
	}
}

// A null on a profile removes the inherited key, a map included, and the plugin is configured without it.
func TestReceiverConfigNullDeletesAnInheritedKey(t *testing.T) {
	config, err := readBase(t, nil, "name: host\nagent: claude\nconfig:\n  runtime: {driver: exec, config: {image: null, command: /bin/run}, resources: ~}\n",
		"name: bare\nagent: claude\nconfig: {runtime: null}\n")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"driver": "exec", "config": map[string]any{"args": []any{"a", "b"}, "command": "/bin/run"}}
	if got := config.Profiles[0].Config["runtime"]; !reflect.DeepEqual(got, want) {
		t.Fatalf("runtime %+v; want %+v", got, want)
	}
	if _, ok := config.Profiles[1].Config["runtime"]; ok {
		t.Fatalf("config %+v; want the inherited runtime removed", config.Profiles[1].Config)
	}
	read := recordPlugin(t)
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("nomad-platform-token-value"), 0o600); err != nil {
		t.Fatal(err)
	}
	plugins := &plugins{dir: installPlugin(t), credentials: map[string]credential.Source{"nomad-token": {File: token}}, hold: func(string, []byte) {},
		logs: io.Discard, logger: discard, registry: prometheus.NewRegistry(), running: map[string]*pluginsdk.Process{}}
	defer plugins.close()
	settings := config.Profiles[0]
	settings.Plugin = "fake"
	if _, err := plugins.profile("build", settings); err != nil {
		t.Fatal(err)
	}
	if configured := string(read()[0].Settings.Config); strings.Contains(configured, "image") || strings.Contains(configured, "resources") || strings.Contains(configured, "null") {
		t.Fatalf("plugin configured with %s; want neither deleted key nor a null", configured)
	}
}

func TestReceiverConfigIgnoresANullWithNothingInherited(t *testing.T) {
	config, err := readBase(t, nil, "name: claude\nagent: claude\nlabels: {absent: ~}\nconfig: {extra: null, runtime: {config: {missing: ~}}}\n")
	if err != nil {
		t.Fatal(err)
	}
	profile := config.Profiles[0]
	if _, ok := profile.Config["extra"]; ok || len(profile.Config["runtime"].(map[string]any)["config"].(map[string]any)) != 2 || len(profile.Labels) != 2 {
		t.Fatalf("profile %+v; want the nulls with nothing under them dropped", profile)
	}
}

func TestReceiverConfigRefusesANullInThePlugin(t *testing.T) {
	for _, test := range []struct{ old, new, want string }{
		{"image: base:1", "image: ~", "plugin nomad sets config key runtime.config.image to null"},
		{"tier: base", "tier: null", "plugin nomad sets labels key tier to null"},
	} {
		_, err := readBase(t, map[string]string{test.old: test.new}, "name: claude\nagent: claude\n")
		if err == nil || !strings.Contains(err.Error(), test.want) {
			t.Errorf("readReceiverConfig = %v; want %q", err, test.want)
		}
	}
}

// A map against a scalar or a list at one path is refused, in either direction, naming the profile and the path.
func TestReceiverConfigRefusesATypeClash(t *testing.T) {
	for config, path := range map[string]string{
		"{runtime: docker}":              "runtime",
		"{runtime: {config: [x]}}":       "runtime.config",
		"{address: {host: nomad.local}}": "address",
	} {
		_, err := readBase(t, nil, "name: claude\nagent: claude\nconfig: "+config+"\n")
		if err == nil || !strings.Contains(err.Error(), `profile "claude": config key `+path+" is a map at one level and not at the other") {
			t.Errorf("profile config %s: %v; want a type clash at %s", config, err, path)
		}
	}
}

// A null inside a list deletes nothing, and no plugin receives one.
func TestReceiverConfigRefusesANullInsideAList(t *testing.T) {
	_, err := readBase(t, nil, "name: claude\nagent: claude\nconfig: {runtime: {config: {args: [a, ~]}}}\n")
	if err == nil || !strings.Contains(err.Error(), `profile "claude": config runtime.config.args[1] is null inside a list`) {
		t.Fatalf("a null inside a list: %v; want it refused naming its path", err)
	}
}

// Plugin labels are each profile's defaults, the profile winning per key, in either form.
func TestReceiverConfigLabelsInheritPerKey(t *testing.T) {
	config, err := readBase(t, nil, "name: batch\nagent: claude\nlabels: {tier: batch, gpu: \"yes\"}\n", "name: plain\nagent: claude\n",
		"name: bare\nagent: claude\nlabels: {platform: null}\n")
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []map[string]string{{"platform": "nomad", "tier": "batch", "gpu": "yes"}, {"platform": "nomad", "tier": "base"}, {"tier": "base"}} {
		if !reflect.DeepEqual(config.Profiles[i].Labels, want) {
			t.Errorf("profile %s labels %v; want %v", config.Profiles[i].Name, config.Profiles[i].Labels, want)
		}
	}
	config, err = readReceiverConfig(labTree(t, map[string]string{"plugins/nomad/plugin.yaml": "labels: {platform: nomad, isolation: any}\n" + labPlugin}), true)
	if err != nil || !reflect.DeepEqual(config.Profiles[1].Labels, map[string]string{"platform": "nomad", "isolation": "host"}) {
		t.Fatalf("directory form: %+v, %v; want plugin.yaml's labels under nomad-exec's", config.Profiles, err)
	}
}

func TestReceiverConfigRefusesANonStringCredential(t *testing.T) {
	_, err := readBase(t, nil, "name: claude\nagent: claude\nconfig: {credential: 5}\n")
	if err == nil || !strings.Contains(err.Error(), `profile "claude": credential is not a string`) {
		t.Fatalf("readReceiverConfig = %v; want the credential refused naming the profile", err)
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
		"invalid type":          {replace: map[string]string{"plugins/Bad/profiles/10-x.yaml": strings.Replace(labExec, "nomad-exec", "x", 1)}, want: `plugin "Bad" is not a valid name`},
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
