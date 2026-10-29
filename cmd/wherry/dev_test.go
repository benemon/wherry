package main

import (
	"bytes"
	"context"
	json "encoding/json/v2"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sigstore/sigstore-go/pkg/root"
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
	devSources.hashicorp = func(_ context.Context, product, _ string, prev *devArtefact, _ io.Writer) (*devArtefact, error) {
		calls = append(calls, product)
		return prev, nil
	}
	devSources.plugin = func(_ context.Context, _ string, prev *devPlugin, _ io.Writer) (*devPlugin, error) {
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
		if got, err := chooseAgent(c.harnesses, c.agent); err != nil || got != c.want {
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
	job, _ := json.Marshal(kafkaJob(env, "cluster-id"))
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
	job, _ = json.Marshal(kafkaJob(env, "cluster-id"))
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
	if strings.Contains(string(job), `"User"`) {
		t.Errorf("podman kafka job sets a user:\n%s", job)
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
