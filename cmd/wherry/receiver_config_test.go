package main

import (
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
	single := writeTree(t, map[string]string{"receiver.yaml": `capacity: 2
adapters:
  - name: nomad-podman
    type: nomad
    agent: claude
    credential: claude-oauth-token
    labels:
      isolation: container
    config:
      address: https://nomad.example:4646
      ca_file: /secrets/nomad-ca.crt
      namespace: wherry
      credential_env: CLAUDE_CODE_OAUTH_TOKEN
      node_pool: default
      runtime:
        driver: podman
        config:
          image: localhost/wherry-agent:test
  - name: nomad-exec
    type: nomad
    agent: claude
    credential: claude-oauth-token
    labels:
      isolation: host
    config:
      address: https://nomad.example:4646
      ca_file: /secrets/nomad-ca.crt
      namespace: wherry
      credential_env: CLAUDE_CODE_OAUTH_TOKEN
      node_pool: OCPv
      runtime:
        driver: exec
        config:
          command: /bin/sh
`})
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
		"unknown receiver key":  {replace: map[string]string{"receiver.yaml": "capacity: 2\nadapters: []\n"}, want: "field adapters not found"},
		"unknown platform key":  {replace: map[string]string{"platforms/lab-nomad.yaml": labPlatform + "region: global\n"}, want: "field region not found"},
		"unknown adapter key":   {replace: map[string]string{"adapters/20-nomad-exec.yaml": labExec + "type: nomad\n"}, want: "field type not found"},
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
