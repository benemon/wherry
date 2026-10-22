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
)

const labPlatform = `name: lab-nomad
type: nomad
config:
  address: https://nomad.example:4646
  ca_file: /secrets/nomad-ca.crt
  namespace: wherry
  credential_env: CLAUDE_CODE_OAUTH_TOKEN
`

const labPodman = `name: nomad-podman
platform: lab-nomad
agent: claude
output: file
reply_file: out/reply.md
credential: claude-oauth-token
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
platform: lab-nomad
agent: claude
output: codex
credential: claude-oauth-token
labels:
  isolation: host
config:
  node_pool: OCPv
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
		"receiver.yaml":                 "capacity: 2\n",
		"platforms/lab-nomad.yaml":      labPlatform,
		"adapters/10-nomad-podman.yaml": labPodman,
		"adapters/20-nomad-exec.yaml":   labExec,
	}
	maps.Copy(files, replace)
	for _, name := range remove {
		delete(files, name)
	}
	return writeTree(t, files)
}

func TestReceiverConfigDirectoryMatchesSingleFile(t *testing.T) {
	// item turns a document into an entry of a YAML list.
	item := func(document string) string {
		lines := strings.Split(strings.TrimSuffix(document, "\n"), "\n")
		for i := range lines {
			lines[i] = "    " + lines[i]
		}
		return "  - " + strings.TrimPrefix(strings.Join(lines, "\n"), "    ") + "\n"
	}
	single := writeTree(t, map[string]string{"receiver.yaml": "capacity: 2\nplatforms:\n" + item(labPlatform) + "adapters:\n" + item(labPodman) + item(labExec)})
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

func TestReceiverConfigDirectoryAdapterTakesPlatformType(t *testing.T) {
	dir := labTree(t, map[string]string{
		"platforms/other.yaml":        "name: other\ntype: other-type\n",
		"adapters/20-nomad-exec.yaml": strings.Replace(labExec, "platform: lab-nomad", "platform: other", 1),
	})
	config, err := readReceiverConfig(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	if config.Adapters[0].Type != "nomad" || config.Adapters[1].Type != "other-type" {
		t.Fatalf("adapter types = %q, %q, want nomad, other-type", config.Adapters[0].Type, config.Adapters[1].Type)
	}
}

func TestReceiverConfigDirectoryRejectsKeySetByPlatformAndAdapter(t *testing.T) {
	dir := labTree(t, map[string]string{"adapters/20-nomad-exec.yaml": labExec + "  namespace: other\n"})
	_, err := readReceiverConfig(dir, true)
	if err == nil || !strings.Contains(err.Error(), "20-nomad-exec.yaml") || !strings.Contains(err.Error(), `"namespace"`) {
		t.Fatalf("readReceiverConfig = %v, want an error naming the adapter file and the key", err)
	}
}

func TestReceiverConfigDirectoryRejectsInvalidLayout(t *testing.T) {
	for name, test := range map[string]struct {
		replace map[string]string
		remove  []string
		want    string
	}{
		"unknown receiver key":  {replace: map[string]string{"receiver.yaml": "capacity: 2\nadapters: []\n"}, want: "invalid or unknown field at line"},
		"unknown platform key":  {replace: map[string]string{"platforms/lab-nomad.yaml": labPlatform + "region: global\n"}, want: "invalid or unknown field at line"},
		"unknown adapter key":   {replace: map[string]string{"adapters/20-nomad-exec.yaml": labExec + "type: nomad\n"}, want: "invalid or unknown field at line"},
		"unknown platform":      {replace: map[string]string{"adapters/20-nomad-exec.yaml": strings.Replace(labExec, "platform: lab-nomad", "platform: missing", 1)}, want: `unknown platform "missing"`},
		"duplicate adapter":     {replace: map[string]string{"adapters/30-again.yaml": labExec}, want: `30-again.yaml: adapter name "nomad-exec" is repeated`},
		"duplicate platform":    {replace: map[string]string{"platforms/again.yaml": labPlatform}, want: `platform name "lab-nomad" is empty or repeated`},
		"empty adapters":        {replace: map[string]string{"adapters/": ""}, remove: []string{"adapters/10-nomad-podman.yaml", "adapters/20-nomad-exec.yaml"}, want: "holds no adapters"},
		"non-yaml adapter file": {replace: map[string]string{"adapters/30-typo.yml": labExec}, want: "30-typo.yml is not a .yaml file"},
		"nested directory":      {replace: map[string]string{"adapters/nested/30-x.yaml": labExec}, want: "nested is not a .yaml file"},
		"stray top-level file":  {replace: map[string]string{"notes.txt": "x"}, want: "notes.txt does not belong"},
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

func TestReceiverConfigErrorsQuoteNoValue(t *testing.T) {
	const token = "nomad-token-value-secret"
	for name, replace := range map[string]map[string]string{
		"mistyped value":  {"receiver.yaml": "capacity: " + token + "\n"},
		"misplaced value": {"platforms/lab-nomad.yaml": labPlatform + token + "\n"},
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
	for _, hidden := range []string{".DS_Store", "platforms/.DS_Store", "adapters/.DS_Store"} {
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

func TestPlatformCredentialsByPlatform(t *testing.T) {
	config, err := readReceiverConfig(labTree(t, nil), true)
	if err != nil {
		t.Fatal(err)
	}
	if config.Adapters[0].Platform != "lab-nomad" || config.Adapters[1].Platform != "lab-nomad" {
		t.Fatalf("adapter platforms %q, %q, want lab-nomad", config.Adapters[0].Platform, config.Adapters[1].Platform)
	}
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("nomad-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	platform, err := platformCredentials(config, []string{"lab-nomad=" + token})
	if err != nil || !reflect.DeepEqual(platform, map[string]string{"lab-nomad": "nomad-token"}) {
		t.Fatalf("platform credentials %v, %v; want lab-nomad's token", platform, err)
	}
	for name, files := range map[string][]string{
		"adapter name":  {"nomad-exec=" + token},
		"no platform":   {"missing=" + token},
		"named twice":   {"lab-nomad=" + token, "lab-nomad=" + token},
		"not NAME=FILE": {token},
	} {
		if _, err := platformCredentials(config, files); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, _, files, err := adminArgs([]string{"receiver", "add", "r", "--platform-credential-file", "a=x", "--platform-credential-file", "b=y"}); err != nil || len(files) != 2 {
		t.Errorf("repeated --platform-credential-file = %v, %v; want both", files, err)
	}
	if _, _, _, err := adminArgs([]string{"receiver", "add", "r", "--pool", "a", "--pool", "b"}); err == nil {
		t.Error("a repeated --pool was accepted")
	}
}

func TestReceiverDefaults(t *testing.T) {
	config, err := readReceiverConfig(labTree(t, map[string]string{"receiver.yaml": "{}\n"}), true)
	if err != nil || config.Capacity != 1 {
		t.Fatalf("capacity %d, %v; want 1 when receiver.yaml leaves it out", config.Capacity, err)
	}
	var stderr bytes.Buffer
	err = run([]string{"receiver", "run", "--bundle-file", filepath.Join(t.TempDir(), "missing.yaml")}, io.Discard, &stderr)
	if err == nil || !strings.Contains(err.Error(), "read bundle") {
		t.Fatalf("receiver run with only --bundle-file = %v; want it to read the bundle, not ask for more flags", err)
	}
}
