package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	jsonv1 "encoding/json"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/hashicorp/nomad/api"
	"github.com/sigstore/sigstore-go/pkg/root"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// manifestFixture is section 2's manifest with every field set.
const manifestFixture = `{
  "version": 1,
  "updated": "2026-10-28T10:00:00Z",
  "os": "linux",
  "arch": "amd64",
  "runtime": "podman",
  "nomad": {"version": "2.0.7", "file": "nomad", "sha256": "aa", "verified": "SHA256SUMS+pgp"},
  "podman_driver": {"version": "0.6.6", "file": "nomad-plugins/nomad-driver-podman", "sha256": "bb", "verified": "SHA256SUMS+pgp"},
  "plugin": {"tag": "nomad/v0.3.0", "protocol": 10, "file": "plugins/wherry-plugin-nomad", "sha256": "cc", "verified": "checksums.txt+sigstore"},
  "images": {
    "kafka": {"reference": "docker.io/apache/kafka:latest", "digest": "sha256:dd", "id": "sha256:ee", "tag": "wherry-dev/kafka:ee"}
  },
  "snapshot": {"file": "wherry", "sha256": "ff", "version": "v0.1.0-rc.21", "revision": "0123", "modified": true},
  "agents": ["claude", "codex"],
  "default_agent": "claude",
  "storage": {"reference": "quay.io/benjamin_holmes/ceph-aio:v21", "digest": "sha256:11", "id": "sha256:22", "tag": "wherry-dev/ceph-aio:22"},
  "env_denylist": ["CODEX_API_KEY"]
}`

// devFixture points the dev root at a temporary directory and returns the default instance.
func devFixture(t *testing.T) devInstance {
	t.Helper()
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	instance, err := openInstance("default")
	if err != nil {
		t.Fatal(err)
	}
	return instance
}

func runCapture(args ...string) (string, string, error) {
	var stdout, stderr bytes.Buffer
	err := run(args, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}

func TestDevManifestRoundTrip(t *testing.T) {
	instance := devFixture(t)
	var manifest devManifest
	if err := json.Unmarshal([]byte(manifestFixture), &manifest); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(instance.cache(), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := writeManifest(instance, manifest); err != nil {
		t.Fatal(err)
	}
	read, err := readManifest(instance)
	if err != nil || !reflect.DeepEqual(*read, manifest) {
		t.Fatalf("read %+v, %v; want %+v", read, err, manifest)
	}
	// Every field of the fixture survives: compare the written file with the fixture as generic JSON.
	written, err := os.ReadFile(instance.cache("manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var want, got any
	json.Unmarshal([]byte(manifestFixture), &want)
	json.Unmarshal(written, &got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("manifest file\n%s\nwant the fixture's fields\n%s", written, manifestFixture)
	}
	if info, err := os.Stat(instance.cache("manifest.json")); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("manifest mode %v, %v", info.Mode(), err)
	}
	if err := os.WriteFile(instance.cache("manifest.json"), []byte(strings.Replace(manifestFixture, `"version": 1,`, `"version": 2,`, 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readManifest(instance); err == nil || !strings.Contains(err.Error(), instance.cache("manifest.json")) {
		t.Fatalf("version 2 read with %v; want a refusal naming the path", err)
	}
}

func TestDevAgentImageParsing(t *testing.T) {
	for value, want := range map[string]devAgentImage{
		"a=b":                     {"a", "b"},
		"a=reg/img:tag@sha256:x":  {"a", "reg/img:tag@sha256:x"},
		"coder=reg:5000/a=b:tag":  {"coder", "reg:5000/a=b:tag"},
		"claude=ghcr.io/x/claude": {"claude", "ghcr.io/x/claude"},
	} {
		var images devAgentImages
		if err := images.Set(value); err != nil || len(images) != 1 || images[0] != want {
			t.Errorf("%s gave %+v, %v; want %+v", value, images, err, want)
		}
	}
	for _, value := range []string{"=img", "a", "a=", "mock=x", "Bad=x"} {
		var images devAgentImages
		if err := images.Set(value); err == nil {
			t.Errorf("%s accepted as %+v", value, images)
		}
	}
	var images devAgentImages
	if err := images.Set("a=b"); err != nil {
		t.Fatal(err)
	}
	if err := images.Set("a=c"); err == nil {
		t.Fatalf("a repeated accepted as %+v", images)
	}
}

// writeUp writes instance's env/up.json naming pid as its Nomad agent.
func writeUp(t *testing.T, instance devInstance, pid int) {
	t.Helper()
	if err := os.MkdirAll(instance.env(), 0o700); err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(devUpState{NomadPID: pid, Runtime: "docker"})
	if err := os.WriteFile(instance.env("up.json"), encoded, 0o600); err != nil {
		t.Fatal(err)
	}
}

func deadPID(t *testing.T) int {
	t.Helper()
	command := exec.Command("true")
	if err := command.Run(); err != nil {
		t.Fatal(err)
	}
	return command.Process.Pid
}

func TestDevUpRefusals(t *testing.T) {
	instance := devFixture(t)
	writeUp(t, instance, os.Getpid())
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"dev", "up", "--instance", "default"}, "instance default is already up"},
		{[]string{"dev", "up", "--instance", "other"}, "instance default is up; run wherry dev down --instance default first"},
		{[]string{"dev", "init", "--instance", "default"}, "instance default is up; run wherry dev down first"},
		{[]string{"dev", "reset", "--instance", "default"}, "instance default is up; run wherry dev down first"},
	} {
		if _, _, err := runCapture(c.args...); err == nil || err.Error() != c.want {
			t.Errorf("%v: %v; want %q", c.args, err, c.want)
		}
	}
	if _, err := os.Stat(instance.cache()); err == nil {
		t.Fatal("init under a live instance made its cache")
	}

	writeUp(t, instance, deadPID(t))
	if stdout, _, err := runCapture("dev", "reset"); err != nil || stdout != "reset: instance default\n" {
		t.Fatalf("reset with a dead pid: %q, %v; want the instance read as not up", stdout, err)
	}
	if _, _, err := runCapture("dev", "up"); err == nil || err.Error() != "nothing acquired for instance default; run wherry dev init first" {
		t.Fatalf("up with a dead pid: %v; want the manifest check next", err)
	}
}

// writeCache writes a manifest naming files whose contents match it, the snapshot holding snapshot.
func writeCache(t *testing.T, instance devInstance, snapshot []byte) devManifest {
	t.Helper()
	files := map[string][]byte{"nomad": []byte("nomad"), "plugins/wherry-plugin-nomad": []byte("plugin"), "wherry": snapshot}
	for file, data := range files {
		if err := os.MkdirAll(filepath.Dir(instance.cache(file)), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(instance.cache(file), data, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	sum := func(file string) string {
		s, err := fileSHA256(instance.cache(file))
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	manifest := devManifest{
		Version: devManifestVersion, OS: runtime.GOOS, Arch: runtime.GOARCH, Runtime: "docker",
		Nomad:    &devArtefact{Version: "2.0.7", File: "nomad", SHA256: sum("nomad")},
		Plugin:   &devPlugin{Tag: "nomad/v0.1.0-rc.16", Protocol: 10, File: "plugins/wherry-plugin-nomad", SHA256: sum("plugins/wherry-plugin-nomad")},
		Images:   map[string]devImage{"kafka": {Reference: kafkaReference, Digest: "sha256:0123456789abcdef", ID: "sha256:fedcba9876543210", Tag: "wherry-dev/kafka:fedcba987654"}},
		Snapshot: devSnapshot{File: "wherry", SHA256: sum("wherry")},
		Agents:   []string{"claude"}, DefaultAgent: "claude", EnvDenylist: credentialNames(os.Environ()),
		Updated: time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC),
	}
	if err := writeManifest(instance, manifest); err != nil {
		t.Fatal(err)
	}
	return manifest
}

// stubSources replaces init's resolvers with ones that return what the manifest records, and finds harnesses.
func stubSources(t *testing.T, harnesses ...string) *[]string {
	t.Helper()
	previous := devSources
	t.Cleanup(func() { devSources = previous })
	var calls []string
	devSources.hashicorp = func(_ context.Context, product, _, _ string, prev *devArtefact, _ io.Writer) (*devArtefact, error) {
		calls = append(calls, product)
		return prev, nil
	}
	devSources.plugin = func(_ context.Context, _, _ string, prev *devPlugin, _ io.Writer) (*devPlugin, error) {
		calls = append(calls, "plugin")
		return prev, nil
	}
	devSources.image = func(_ context.Context, _, reference, _ string, prev *devImage, _ io.Writer) (*devImage, error) {
		calls = append(calls, reference)
		return prev, nil
	}
	devSources.runtimeOK = func(runtime string) bool { return runtime == "docker" }
	devSources.lookPath = func(file string) (string, error) {
		if slices.Contains(harnesses, file) {
			return "/usr/bin/" + file, nil
		}
		return "", exec.ErrNotFound
	}
	stdin, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	previousStdin := devStdin
	devStdin = stdin
	t.Cleanup(func() { devStdin = previousStdin; stdin.Close() })
	return &calls
}

func testBinary(t *testing.T) []byte {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if self, err = filepath.EvalSymlinks(self); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(self)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestDevUpNeedsInitAndTheSnapshot(t *testing.T) {
	instance := devFixture(t)
	stubSources(t, "claude")
	if _, _, err := runCapture("dev", "up"); err == nil || err.Error() != "nothing acquired for instance default; run wherry dev init first" {
		t.Fatalf("up without a manifest: %v", err)
	}
	writeCache(t, instance, []byte("another build"))
	if _, _, err := runCapture("dev", "up"); err == nil || err.Error() != "the running wherry differs from the snapshot; run wherry dev init" {
		t.Fatalf("up with another snapshot: %v", err)
	}
	if _, err := os.Stat(instance.env()); err == nil {
		t.Fatal("up refused after making env/")
	}
}

func TestDevInitUnchanged(t *testing.T) {
	instance := devFixture(t)
	calls := stubSources(t, "claude")
	writeCache(t, instance, testBinary(t))
	before, err := os.ReadFile(instance.cache("manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	stdout, _, err := runCapture("dev", "init", "--runtime", "docker")
	if err != nil || stdout != "instance default: unchanged\n" {
		t.Fatalf("init: %q, %v; want unchanged", stdout, err)
	}
	if after, _ := os.ReadFile(instance.cache("manifest.json")); !bytes.Equal(after, before) {
		t.Fatalf("init rewrote an unchanged manifest:\n%s", after)
	}
	if want := []string{"nomad", "plugin", kafkaReference}; !slices.Equal(*calls, want) {
		t.Fatalf("resolvers called %v; want %v", *calls, want)
	}

	// A harness installed since is a change.
	stubSources(t, "claude", "codex")
	if stdout, _, err := runCapture("dev", "init", "--runtime", "docker", "--agent", "codex"); err != nil || !strings.HasPrefix(stdout, "instance default: acquired") {
		t.Fatalf("init with codex: %q, %v", stdout, err)
	}
	read, err := readManifest(instance)
	if err != nil || !slices.Equal(read.Agents, []string{"claude", "codex"}) || read.DefaultAgent != "codex" || read.Updated.Equal(time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("manifest %+v, %v", read, err)
	}
}

func TestDevInitChoosesTheDefaultAgent(t *testing.T) {
	devFixture(t)
	for _, c := range []struct {
		harnesses []string
		args      []string
		want      string
	}{
		{nil, nil, "step harnesses: no agent harness found on PATH (looked for claude, codex, gemini); install one, or choose --agent mock"},
		{[]string{"claude", "codex"}, nil, "step harnesses: found claude and codex; choose the default with --agent"},
		{[]string{"claude", "codex", "gemini"}, nil, "step harnesses: found claude, codex and gemini; choose the default with --agent"},
		{[]string{"claude"}, []string{"--agent", "gemini"}, "step harnesses: --agent gemini is neither a detected harness (claude) nor mock"},
	} {
		calls := stubSources(t, c.harnesses...)
		if _, _, err := runCapture(append([]string{"dev", "init", "--runtime", "docker"}, c.args...)...); err == nil || err.Error() != c.want {
			t.Errorf("harnesses %v %v: %v; want %q", c.harnesses, c.args, err, c.want)
		}
		if len(*calls) != 0 {
			t.Errorf("harnesses %v: acquired %v before failing", c.harnesses, *calls)
		}
	}
	for _, c := range []struct {
		harnesses []string
		agent     string
		want      string
	}{{[]string{"claude"}, "", "claude"}, {nil, "mock", "mock"}, {[]string{"claude", "codex"}, "codex", "codex"}} {
		if got, err := chooseAgent(c.harnesses, c.agent, io.Discard); err != nil || got != c.want {
			t.Errorf("chooseAgent(%v, %q) = %q, %v; want %q", c.harnesses, c.agent, got, err, c.want)
		}
	}
}

func TestDevRendering(t *testing.T) {
	instance := devFixture(t)
	env := devEnv{
		instance: instance, driver: "docker", socket: "unix:///var/run/docker.sock", uid: 1000, gid: 1000,
		denylist: denylist([]string{"GITHUB_TOKEN"}, []string{"WHERRY_DEV_MOCK_TOKEN"}),
		manifest: devManifest{Images: map[string]devImage{"kafka": {Tag: "wherry-dev/kafka:0123456789ab"}}},
	}
	properties := renderServerProperties("cluster-id")
	hcl := renderAgentHCL(env)
	job, _ := jsonv1.Marshal(kafkaJob(env, "cluster-id"))
	for name, text := range map[string]string{"server.properties": properties, "agent.hcl": hcl, "kafka job": string(job)} {
		if strings.Contains(text, "localhost") {
			t.Errorf("%s names localhost:\n%s", name, text)
		}
	}
	for _, line := range []string{
		"listeners=INTERNAL://127.0.0.1:9092,CONTROLLER://127.0.0.1:9094,SCRAM://0.0.0.0:9093",
		"advertised.listeners=INTERNAL://127.0.0.1:9092,SCRAM://127.0.0.1:9093",
		"controller.quorum.bootstrap.servers=127.0.0.1:9094",
		"listener.security.protocol.map=INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,SCRAM:SASL_SSL",
		"inter.broker.listener.name=INTERNAL",
		"listener.name.scram.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;",
		"ssl.truststore.location=/cfg/ca.pem",
		"super.users=User:wherry-admin;User:ANONYMOUS",
		"log.dirs=/data/kraft",
		"# cluster.id=cluster-id",
	} {
		if !slices.Contains(strings.Split(properties, "\n"), line) {
			t.Errorf("server.properties lacks %q", line)
		}
	}
	for _, block := range []string{
		"bind_addr  = \"127.0.0.1\"\n",
		"data_dir   = " + strconv.Quote(instance.env("nomad", "data")) + "\n",
		"plugin \"raw_exec\" {\n  config {\n    enabled = true\n  }\n}\n",
		"    volumes {\n      enabled = true\n    }\n",
		"    gc {\n      image = false\n    }\n",
		"    endpoint = \"unix:///var/run/docker.sock\"\n",
		"    extra_labels = [\"job_name\"]\n",
	} {
		if !strings.Contains(hcl, block) {
			t.Errorf("agent.hcl lacks %q:\n%s", block, hcl)
		}
	}
	for _, name := range []string{"NOMAD_TOKEN", "VAULT_TOKEN", "AWS_SECRET_ACCESS_KEY", "GITHUB_TOKEN", "WHERRY_DEV_MOCK_TOKEN"} {
		if !strings.Contains(hcl, name) {
			t.Errorf("env.denylist lacks %s:\n%s", name, hcl)
		}
	}
	if strings.Contains(hcl, "plugin_dir") || strings.Contains(hcl, "nomad-driver-podman") {
		t.Errorf("docker agent.hcl carries the podman driver:\n%s", hcl)
	}
	for _, want := range []string{`"User":"1000:1000"`, `"force_pull":false`, `"image":"wherry-dev/kafka:0123456789ab"`, `"LOG_DIR":"/data/logs"`, `"Value":9093`,
		strconv.Quote(instance.env("kafka", "cfg") + ":/cfg:ro"), strconv.Quote(instance.env("kafka", "data") + ":/data")} {
		if !strings.Contains(string(job), want) {
			t.Errorf("docker kafka job lacks %s:\n%s", want, job)
		}
	}

	env.driver, env.socket, env.selinux = "podman", "unix:///run/user/1000/podman/podman.sock", true
	hcl = renderAgentHCL(env)
	job, _ = jsonv1.Marshal(kafkaJob(env, "cluster-id"))
	for _, block := range []string{"plugin_dir = " + strconv.Quote(instance.cache("nomad-plugins")), "plugin \"nomad-driver-podman\" {\n  config {\n    socket_path = \"unix:///run/user/1000/podman/podman.sock\"\n  }\n}\n"} {
		if !strings.Contains(hcl, block) {
			t.Errorf("podman agent.hcl lacks %q:\n%s", block, hcl)
		}
	}
	if strings.Contains(hcl, "plugin \"docker\"") {
		t.Errorf("podman agent.hcl carries the docker block:\n%s", hcl)
	}
	for _, want := range []string{`"userns":"keep-id"`, strconv.Quote(instance.env("kafka", "cfg") + ":/cfg:ro,Z"), strconv.Quote(instance.env("kafka", "data") + ":/data:Z")} {
		if !strings.Contains(string(job), want) {
			t.Errorf("podman kafka job lacks %s:\n%s", want, job)
		}
	}
	if !strings.Contains(string(job), `"User":"1000:1000"`) {
		t.Errorf("podman kafka job does not run as the user:\n%s", job)
	}
}

func TestDevProfilesAndTerms(t *testing.T) {
	instance := devFixture(t)
	var images devAgentImages
	for _, value := range []string{"codex=reg/codex:1", "coder=reg/coder@sha256:ab"} {
		if err := images.Set(value); err != nil {
			t.Fatal(err)
		}
	}
	manifest := devManifest{Agents: []string{"claude", "gemini"}, DefaultAgent: "claude"}
	agents := devAgents(manifest, images)
	if want := []string{"claude", "codex", "gemini", "mock", "coder"}; !slices.Equal(agents, want) {
		t.Fatalf("agents %v; want %v", agents, want)
	}
	env := devEnv{instance: instance, driver: "docker", images: images}
	profiles := devProfiles(env, agents)
	isolation := map[string]string{}
	for _, profile := range profiles {
		isolation[profile.Name] = profile.Labels["isolation"]
	}
	if want := map[string]string{"claude": "host", "codex": "container", "gemini": "host", "mock": "host", "coder": "container"}; !reflect.DeepEqual(isolation, want) {
		t.Fatalf("isolation %v; want %v", isolation, want)
	}
	mock, _ := json.Marshal(profiles[3].Config)
	if !strings.Contains(string(mock), `"driver":"raw_exec"`) || !strings.Contains(string(mock), instance.cache("wherry")+`' dev mock`) {
		t.Fatalf("mock profile %s", mock)
	}
	terms := devTerms(agents, "claude", true)
	if terms.Storage == nil || terms.Storage.Endpoint != "http://127.0.0.1:18080" || terms.Storage.Secure() {
		t.Fatalf("storage %+v; want plain http on 127.0.0.1:18080", terms.Storage)
	}
	names := map[string][]string{}
	for _, agent := range terms.Agents {
		for _, credential := range agent.Credentials {
			if !credential.Optional() {
				t.Errorf("agent %s credential %s is required", agent.Name, credential.Name)
			}
			names[agent.Name] = append(names[agent.Name], credential.Name+"="+credential.Env)
		}
	}
	if want := map[string][]string{
		"claude": {"claude-oauth-token=CLAUDE_CODE_OAUTH_TOKEN", "anthropic-api-key=ANTHROPIC_API_KEY"},
		"codex":  {"openai-api-key=CODEX_API_KEY"}, "gemini": {"gemini-api-key=GEMINI_API_KEY"}, "mock": {"mock-token=MOCK_TOKEN"},
	}; !reflect.DeepEqual(names, want) {
		t.Fatalf("credentials %v; want %v", names, want)
	}
}

func TestDevDownWaitsForContainers(t *testing.T) {
	previous := devContainers
	t.Cleanup(func() { devContainers = previous })
	var polls int
	var removed []string
	devContainers.list = func(_ context.Context, runtime, alloc string) ([]string, error) {
		if runtime != "docker" || alloc != "alloc-1" {
			return nil, errors.New("unexpected filter")
		}
		polls++
		if polls <= 3 {
			return []string{"c1"}, nil
		}
		return nil, nil
	}
	devContainers.remove = func(_ context.Context, _ string, ids []string) error {
		removed = append(removed, ids...)
		return nil
	}
	forced, err := waitContainers(context.Background(), "docker", []string{"alloc-1"}, time.Minute, time.Millisecond)
	if err != nil || forced != nil || polls != 4 || removed != nil {
		t.Fatalf("forced %v, %v after %d polls, removed %v; want a return once the list is empty", forced, err, polls, removed)
	}
	polls = -1000
	forced, err = waitContainers(context.Background(), "docker", []string{"alloc-1"}, 20*time.Millisecond, time.Millisecond)
	if err != nil || !slices.Equal(forced, []string{"c1"}) || !slices.Equal(removed, []string{"c1"}) {
		t.Fatalf("forced %v, %v, removed %v; want c1 removed past the bound", forced, err, removed)
	}
}

func TestDevNotUpDown(t *testing.T) {
	instance := devFixture(t)
	writeUp(t, instance, deadPID(t))
	stdout, _, err := runCapture("dev", "down")
	if err != nil || stdout != "instance default is not up\n" {
		t.Fatalf("down: %q, %v", stdout, err)
	}
	if _, err := os.Stat(instance.env()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("env/ left: %v", err)
	}
}

func TestHashiCorpSignature(t *testing.T) {
	sums, err := os.ReadFile("testdata/dev/nomad_2.1.0_SHA256SUMS")
	if err != nil {
		t.Fatal(err)
	}
	signature, err := os.ReadFile("testdata/dev/nomad_2.1.0_SHA256SUMS.sig")
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyHashiCorp(sums, signature); err != nil {
		t.Fatal(err)
	}
	if sum, err := listedSHA256(sums, "nomad_2.1.0_linux_amd64.zip"); err != nil || len(sum) != 64 {
		t.Fatalf("listed %q, %v", sum, err)
	}
	tampered := bytes.Clone(sums)
	tampered[0] ^= 1
	if err := verifyHashiCorp(tampered, signature); err == nil {
		t.Fatal("a tampered SHA256SUMS verified")
	}
}

func TestPluginSignature(t *testing.T) {
	trusted, err := root.NewTrustedRootFromPath("testdata/dev/trusted_root.json")
	if err != nil {
		t.Fatal(err)
	}
	read := func(name string) []byte {
		data, err := os.ReadFile("testdata/dev/" + name)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	checksums, bundle := read("nomad-rc16-checksums.txt"), read("nomad-rc16-checksums.txt.sigstore.json")
	if err := verifyBundle(trusted, checksums, bundle); err != nil {
		t.Fatal(err)
	}
	tampered := bytes.Clone(checksums)
	tampered[0] ^= 1
	if err := verifyBundle(trusted, tampered, bundle); err == nil {
		t.Fatal("a tampered checksums.txt verified")
	}
	// The kubernetes plugin's release is signed by the same workflow for another tag, which the identity refuses.
	if err := verifyBundle(trusted, read("kubernetes-rc8-checksums.txt"), read("kubernetes-rc8-checksums.txt.sigstore.json")); err == nil {
		t.Fatal("a kubernetes/v* release verified as a nomad one")
	}
}

func TestNewestRelease(t *testing.T) {
	versions := []string{"2.0.7", "2.1.0-rc.1", "2.1.0+ent", "1.10.12", "2.0.10", "2.1.0-beta.2", "2.0.9+ent.fips1403"}
	if got, ok := newestRelease(versions); !ok || got != "2.0.10" {
		t.Fatalf("newest %q, %v; want 2.0.10", got, ok)
	}
	if _, ok := newestRelease([]string{"2.1.0-rc.1"}); ok {
		t.Fatal("a release candidate counted")
	}
}

func TestCredentialNames(t *testing.T) {
	got := credentialNames([]string{"GITHUB_TOKEN=x", "PATH=/bin", "AWS_KEY=y", "A_SECRET=", "DB_PASSWORD=z", "TOKEN=t", "KEY_ID=k", "GITHUB_TOKEN=again"})
	if want := []string{"AWS_KEY", "A_SECRET", "DB_PASSWORD", "GITHUB_TOKEN"}; !slices.Equal(got, want) {
		t.Fatalf("names %v; want %v", got, want)
	}
}

// fakeTransport answers requests from responses by URL, 404 otherwise, and records each URL.
type fakeTransport struct {
	responses map[string]*http.Response
	requested []string
	headers   []http.Header
}

func (f *fakeTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	f.requested = append(f.requested, request.URL.String())
	f.headers = append(f.headers, request.Header)
	if response, ok := f.responses[request.URL.String()]; ok {
		return response, nil
	}
	return &http.Response{StatusCode: http.StatusNotFound, Status: "404 Not Found", Body: io.NopCloser(strings.NewReader("")), Request: request}, nil
}

// fakeHTTP points devHTTP at a fakeTransport answering each URL with its body.
func fakeHTTP(t *testing.T, bodies map[string][]byte) *fakeTransport {
	t.Helper()
	fake := &fakeTransport{responses: map[string]*http.Response{}}
	for url, body := range bodies {
		fake.responses[url] = &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(body))}
	}
	previous := devHTTP
	devHTTP = &http.Client{Transport: fake}
	t.Cleanup(func() { devHTTP = previous })
	return fake
}

func TestAcquireNomadTakesThePin(t *testing.T) {
	read := func(name string) []byte {
		data, err := os.ReadFile("testdata/dev/" + name)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	base := hashicorpReleases + "/nomad/"
	bodies := map[string][]byte{
		base + "index.json": []byte(`{"versions": {
			"2.0.7": {"shasums": "nomad_2.0.7_SHA256SUMS", "shasums_signature": "nomad_2.0.7_SHA256SUMS.sig"},
			"2.1.0": {"shasums": "nomad_2.1.0_SHA256SUMS", "shasums_signature": "nomad_2.1.0_SHA256SUMS.sig"}}}`),
		base + "2.0.7/nomad_2.0.7_SHA256SUMS": read("nomad_2.0.7_SHA256SUMS"), base + "2.0.7/nomad_2.0.7_SHA256SUMS.sig": read("nomad_2.0.7_SHA256SUMS.sig"),
		base + "2.1.0/nomad_2.1.0_SHA256SUMS": read("nomad_2.1.0_SHA256SUMS"), base + "2.1.0/nomad_2.1.0_SHA256SUMS.sig": read("nomad_2.1.0_SHA256SUMS.sig"),
	}
	fake := fakeHTTP(t, bodies)
	zip := fmt.Sprintf("%s2.0.7/nomad_2.0.7_%s_%s.zip", base, runtime.GOOS, runtime.GOARCH)
	if _, err := acquireHashiCorp(context.Background(), "nomad", t.TempDir(), "nomad", nil, io.Discard); err == nil || !strings.Contains(err.Error(), zip+": 404") {
		t.Fatalf("acquire: %v; want the pinned zip fetched", err)
	}
	for _, url := range fake.requested {
		if strings.Contains(url, "2.1.0") {
			t.Fatalf("fetched %s; want only the pinned 2.0.7", url)
		}
	}

	// A pin the signed SHA256SUMS does not list is refused before the zip.
	previous := hashicorpPins["nomad"]
	t.Cleanup(func() { hashicorpPins["nomad"] = previous })
	hashicorpPins["nomad"] = struct{ version, linuxAMD64 string }{"2.0.7", strings.Repeat("0", 64)}
	fake = fakeHTTP(t, bodies)
	if _, err := acquireHashiCorp(context.Background(), "nomad", t.TempDir(), "nomad", nil, io.Discard); err == nil || !strings.Contains(err.Error(), "pinned SHA-256") {
		t.Fatalf("acquire with another pinned SHA-256: %v", err)
	}
	if slices.Contains(fake.requested, zip) {
		t.Fatal("fetched the zip against a pin SHA256SUMS does not list")
	}
}

// fakeNomad serves the Nomad API calls runJob and stopJobs make, each job with one allocation that runs at once and
// completes on deregistration, and records each call.
func fakeNomad(t *testing.T) (*api.Client, *[]string) {
	t.Helper()
	var mu sync.Mutex
	var calls []string
	status := map[string]string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, r.Method+" "+r.URL.RequestURI())
		path := strings.TrimPrefix(r.URL.Path, "/v1/")
		switch {
		case r.Method == http.MethodPut && path == "jobs":
			var request api.JobRegisterRequest
			jsonv1.NewDecoder(r.Body).Decode(&request)
			status[*request.Job.ID] = api.AllocClientStatusRunning
			jsonv1.NewEncoder(w).Encode(api.JobRegisterResponse{})
		case r.Method == http.MethodGet && path == "jobs":
			var stubs []api.JobListStub
			for id := range status {
				stubs = append(stubs, api.JobListStub{ID: id})
			}
			jsonv1.NewEncoder(w).Encode(stubs)
		case r.Method == http.MethodGet && strings.HasSuffix(path, "/allocations"):
			id := strings.TrimSuffix(strings.TrimPrefix(path, "job/"), "/allocations")
			jsonv1.NewEncoder(w).Encode([]api.AllocationListStub{{ID: id + "-alloc", CreateIndex: 1}})
		case r.Method == http.MethodGet && strings.HasPrefix(path, "allocation/"):
			id := strings.TrimSuffix(strings.TrimPrefix(path, "allocation/"), "-alloc")
			if _, ok := status[id]; !ok {
				http.NotFound(w, r)
				return
			}
			jsonv1.NewEncoder(w).Encode(api.Allocation{ID: id + "-alloc", ClientStatus: status[id]})
		case r.Method == http.MethodDelete && strings.HasPrefix(path, "job/"):
			id := strings.TrimPrefix(path, "job/")
			status[id] = api.AllocClientStatusComplete
			if r.URL.Query().Get("purge") == "true" {
				delete(status, id)
			}
			jsonv1.NewEncoder(w).Encode(api.JobDeregisterResponse{})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	client, err := api.NewClient(&api.Config{Address: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	return client, &calls
}

func TestDevNomadJobs(t *testing.T) {
	instance := devFixture(t)
	env := devEnv{instance: instance, driver: "docker", manifest: devManifest{Images: map[string]devImage{"kafka": {Tag: "wherry-dev/kafka:0123456789ab"}}}}
	nomad, calls := fakeNomad(t)
	alloc, err := runJob(context.Background(), nomad, kafkaJob(env, "cluster-id"))
	if err != nil || alloc != "wherry-default-kafka-alloc" {
		t.Fatalf("runJob: %q, %v", alloc, err)
	}
	if _, err := runJob(context.Background(), nomad, receiverJob(env)); err != nil {
		t.Fatal(err)
	}
	*calls = nil
	seen, err := stopJobs(context.Background(), nomad, instance)
	if err != nil || !slices.Equal(seen, []string{"wherry-default-receiver-alloc", "wherry-default-kafka-alloc"}) {
		t.Fatalf("stopJobs: %v, %v", seen, err)
	}
	var deletes []string
	for _, call := range *calls {
		if strings.HasPrefix(call, http.MethodDelete) {
			deletes = append(deletes, call)
		}
	}
	if want := []string{
		"DELETE /v1/job/wherry-default-receiver?purge=false", "DELETE /v1/job/wherry-default-receiver?purge=true",
		"DELETE /v1/job/wherry-default-kafka?purge=false", "DELETE /v1/job/wherry-default-kafka?purge=true",
	}; !slices.Equal(deletes, want) {
		t.Fatalf("deletes %v; want %v", deletes, want)
	}
	if !slices.Contains(*calls, "GET /v1/jobs?meta=true") {
		t.Fatalf("calls %v; want the job list with meta", *calls)
	}
}

func TestDevStorageUserPassesNoKey(t *testing.T) {
	previous := devExec
	t.Cleanup(func() { devExec = previous })
	const access, secret = "ACCESSKEYVALUE", "secretkeyvalue0123456789"
	var argv []string
	stdout := `{"user_id": "wherry-dev", "keys": [{"user": "wherry-dev", "access_key": "` + access + `", "secret_key": "` + secret + `"}]}`
	devExec = func(_ context.Context, _ *api.Client, alloc, task string, command []string, out, _ io.Writer) (int, error) {
		if alloc != "alloc-1" || task != "s3" {
			t.Errorf("exec in %s task %s", alloc, task)
		}
		argv = command
		io.WriteString(out, stdout)
		return 0, nil
	}
	keys, err := storageUser(context.Background(), nil, "alloc-1")
	if err != nil || keys == nil || *keys != [2]string{access, secret} {
		t.Fatalf("keys %v, %v; want the printed ones", keys, err)
	}
	for _, arg := range argv {
		if strings.Contains(arg, "key") || strings.Contains(arg, access) || strings.Contains(arg, secret) {
			t.Fatalf("argv %q carries a key", argv)
		}
	}
	if !slices.Equal(argv[:3], []string{"radosgw-admin", "user", "create"}) {
		t.Fatalf("argv %q", argv)
	}

	// Output that is no user is refused without repeating it.
	stdout = `{"keys": [{"secret_key": "` + secret + `"`
	if _, err := storageUser(context.Background(), nil, "alloc-1"); err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("truncated output: %v; want a refusal that names no key", err)
	}
}

// TestDevOperatorACL fails when dev's operator ACL drifts from the one wherry admin expects of the operator.
func TestDevOperatorACL(t *testing.T) {
	cluster := kfake.MustCluster()
	defer cluster.Close()
	client, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx := context.Background()
	results, err := kadm.NewClient(client).CreateACLs(ctx, operatorACL())
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range results {
		if result.Err != nil {
			t.Fatal(result.Err)
		}
	}
	described, err := kadm.NewClient(client).DescribeACLs(ctx, kadm.NewACLs().AnyResource().Allow().AllowHosts().Operations(kadm.OpAny).ResourcePatternType(kadm.ACLPatternAny))
	if err != nil {
		t.Fatal(err)
	}
	var got, want []string
	for _, result := range described {
		for _, acl := range result.Described {
			got = append(got, fmt.Sprintf("%s %s %s %s %s %s %s", acl.Principal, acl.Host, acl.Type, acl.Name, acl.Pattern, acl.Operation, acl.Permission))
		}
	}
	for _, acl := range admin.ACLs(admin.State{}) {
		pattern := "LITERAL"
		if acl.Prefix {
			pattern = "PREFIXED"
		}
		for _, operation := range acl.Operations {
			want = append(want, fmt.Sprintf("User:%s * %s %s %s %s ALLOW", acl.Principal, strings.ToUpper(acl.ResourceType), acl.Resource, pattern, operation))
		}
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("dev grants\n%s\nwherry admin expects\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestGitHubLookups(t *testing.T) {
	const token = "gh-token-value"
	t.Setenv("GH_TOKEN", "")
	t.Setenv("GITHUB_TOKEN", token)
	const asset = "https://github.com/benemon/wherry-plugins/releases/download/nomad/v0.1.0/checksums.txt"
	fake := fakeHTTP(t, map[string][]byte{pluginReleases: []byte("[]"), asset: []byte("sums")})
	if _, err := get(context.Background(), pluginReleases); err != nil {
		t.Fatal(err)
	}
	if _, err := get(context.Background(), asset); err != nil {
		t.Fatal(err)
	}
	if got := fake.headers[0].Get("Authorization"); got != "Bearer "+token {
		t.Fatalf("API Authorization %q; want the bearer token", got)
	}
	if got := fake.headers[1].Get("Authorization"); got != "" {
		t.Fatalf("download Authorization %q; want none outside the API", got)
	}

	fake = fakeHTTP(t, nil)
	fake.responses[pluginReleases] = &http.Response{
		StatusCode: http.StatusForbidden, Status: "403 Forbidden", Body: io.NopCloser(strings.NewReader("")),
		Header: http.Header{"X-Ratelimit-Remaining": {"0"}, "X-Ratelimit-Reset": {"1793268000"}},
	}
	_, err := get(context.Background(), pluginReleases)
	if err == nil || !strings.Contains(err.Error(), "rate limit is spent until 2026-10-29T10:00:00Z") || strings.Contains(err.Error(), token) {
		t.Fatalf("rate limited: %v; want the limit and its reset time, without the token", err)
	}
}

// pluginRelease is a nomad/v* release as GitHub lists it, its archive holding binary, and the bodies of its assets.
func pluginRelease(t *testing.T, tag string, binary []byte, bodies map[string][]byte) map[string]any {
	t.Helper()
	base := fmt.Sprintf("wherry-plugin-nomad_%s_%s_%s", strings.TrimPrefix(tag, "nomad/"), runtime.GOOS, runtime.GOARCH)
	var archive bytes.Buffer
	zipped := gzip.NewWriter(&archive)
	tarred := tar.NewWriter(zipped)
	if err := tarred.WriteHeader(&tar.Header{Name: base + "/wherry-plugin-nomad", Typeflag: tar.TypeReg, Mode: 0o755, Size: int64(len(binary))}); err != nil {
		t.Fatal(err)
	}
	tarred.Write(binary)
	tarred.Close()
	zipped.Close()
	url := "https://github.com/benemon/wherry-plugins/releases/download/" + tag + "/"
	bodies[url+base+".tar.gz"] = archive.Bytes()
	bodies[url+"checksums.txt"] = fmt.Appendf(nil, "%x  %s.tar.gz\n", sha256.Sum256(archive.Bytes()), base)
	bodies[url+"checksums.txt.sigstore.json"] = []byte("{}")
	var assets []map[string]string
	for _, name := range []string{base + ".tar.gz", "checksums.txt", "checksums.txt.sigstore.json"} {
		assets = append(assets, map[string]string{"name": name, "browser_download_url": url + name})
	}
	return map[string]any{"tag_name": tag, "draft": false, "assets": assets}
}

func TestAcquirePluginFallsBack(t *testing.T) {
	previous := pluginSignature
	t.Cleanup(func() { pluginSignature = previous })
	pluginSignature = func(_, bundle []byte, _ string) error {
		if string(bundle) != "{}" {
			return errors.New("not the release's bundle")
		}
		return nil
	}
	plugin, err := os.ReadFile(filepath.Join(pluginDir, "wherry-plugin-fake"))
	if err != nil {
		t.Fatal(err)
	}
	bodies := map[string][]byte{}
	missing := pluginRelease(t, "nomad/v0.4.0", plugin, bodies)
	missing["assets"] = missing["assets"].([]map[string]string)[1:]
	releases := []map[string]any{
		missing,
		pluginRelease(t, "nomad/v0.3.0", []byte("#!/bin/sh\necho '1|99|tcp|127.0.0.1:1|grpc'\nsleep 10\n"), bodies),
		{"tag_name": "kubernetes/v0.9.0", "draft": false},
		pluginRelease(t, "nomad/v0.2.0", plugin, bodies),
		pluginRelease(t, "nomad/v0.1.0", []byte("#!/bin/sh\nexit 1\n"), bodies),
	}
	encoded, err := json.Marshal(releases)
	if err != nil {
		t.Fatal(err)
	}
	bodies[pluginReleases] = encoded
	fake := fakeHTTP(t, bodies)
	cache := t.TempDir()
	if err := os.MkdirAll(filepath.Join(cache, "plugins"), 0o700); err != nil {
		t.Fatal(err)
	}
	var progress bytes.Buffer
	got, err := acquirePlugin(context.Background(), cache, filepath.Join("plugins", "wherry-plugin-nomad"), nil, &progress)
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("%x", sha256.Sum256(plugin))
	if got.Tag != "nomad/v0.2.0" || got.File != "plugins/wherry-plugin-nomad" || got.SHA256 != want || got.Protocol == 0 {
		t.Fatalf("plugin %+v; want nomad/v0.2.0 with the fake plugin's SHA-256 %s", got, want)
	}
	if sum, err := fileSHA256(filepath.Join(cache, "plugins", "wherry-plugin-nomad")); err != nil || sum != want {
		t.Fatalf("installed %s, %v", sum, err)
	}
	if _, err := os.Stat(filepath.Join(cache, "plugins", "wherry-plugin-nomad.candidate")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("candidate left: %v", err)
	}
	for _, line := range []string{
		"plugin: nomad/v0.4.0 has no wherry-plugin-nomad_v0.4.0_" + runtime.GOOS + "_" + runtime.GOARCH + ".tar.gz with checksums.txt and its sigstore bundle; trying the next older release",
		"plugin: nomad/v0.3.0 speaks another protocol; trying the next older release",
	} {
		if !strings.Contains(progress.String(), line+"\n") {
			t.Errorf("progress lacks %q:\n%s", line, progress.String())
		}
	}
	for _, url := range fake.requested {
		if strings.Contains(url, "v0.1.0") {
			t.Errorf("fetched %s past the release that handshakes", url)
		}
	}

	// A handshake that fails for another reason ends the search.
	bodies[pluginReleases], _ = json.Marshal([]map[string]any{releases[4], releases[3]})
	fakeHTTP(t, bodies)
	if _, err := acquirePlugin(context.Background(), t.TempDir(), "wherry-plugin-nomad", nil, io.Discard); err == nil || !strings.HasPrefix(err.Error(), "nomad/v0.1.0 handshake: ") {
		t.Fatalf("acquire with a broken newest release: %v", err)
	}
}
