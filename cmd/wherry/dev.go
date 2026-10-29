package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/benemon/wherry/internal/wire"
	"golang.org/x/term"
)

// devManifestVersion is the manifest layout this build reads and writes.
const devManifestVersion = 1

// devHarnesses are the agent CLIs init looks for on PATH, in profile order.
var devHarnesses = []string{"claude", "codex", "gemini"}

// devManifest is cache/manifest.json: what init acquired, and what up runs.
type devManifest struct {
	Version      int                 `json:"version"`
	Updated      time.Time           `json:"updated"`
	OS           string              `json:"os"`
	Arch         string              `json:"arch"`
	Runtime      string              `json:"runtime"`
	Nomad        *devArtefact        `json:"nomad"`
	PodmanDriver *devArtefact        `json:"podman_driver"`
	Plugin       *devPlugin          `json:"plugin"`
	Images       map[string]devImage `json:"images"`
	Snapshot     devSnapshot         `json:"snapshot"`
	Agents       []string            `json:"agents"`
	DefaultAgent string              `json:"default_agent"`
	Storage      *devImage           `json:"storage"`
	// EnvDenylist holds the names in the user's environment at init that end _TOKEN, _KEY, _SECRET or _PASSWORD.
	EnvDenylist []string `json:"env_denylist"`
}

type devArtefact struct {
	Version  string `json:"version"`
	File     string `json:"file"`
	SHA256   string `json:"sha256"`
	Verified string `json:"verified"`
}

type devPlugin struct {
	Tag      string `json:"tag"`
	Protocol int    `json:"protocol"`
	File     string `json:"file"`
	SHA256   string `json:"sha256"`
	Verified string `json:"verified"`
}

type devImage struct {
	Reference string `json:"reference"`
	Digest    string `json:"digest"`
	ID        string `json:"id"`
	Tag       string `json:"tag"`
}

type devSnapshot struct {
	File     string `json:"file"`
	SHA256   string `json:"sha256"`
	Version  string `json:"version"`
	Revision string `json:"revision"`
	Modified bool   `json:"modified"`
}

// devUpState is env/up.json, written once up is ready.
type devUpState struct {
	NomadPID   int       `json:"nomad_pid"`
	PodmanPID  int       `json:"podman_pid,omitzero"`
	Runtime    string    `json:"runtime"`
	KafkaAlloc string    `json:"kafka_alloc"`
	S3Alloc    string    `json:"s3_alloc,omitzero"`
	Ports      []int     `json:"ports"`
	Started    time.Time `json:"started"`
}

// devInstance is one instance's directory under the dev root.
type devInstance struct {
	name, root, dir string
}

func (i devInstance) cache(parts ...string) string {
	return filepath.Join(append([]string{i.dir, "cache"}, parts...)...)
}

func (i devInstance) env(parts ...string) string {
	return filepath.Join(append([]string{i.dir, "env"}, parts...)...)
}

// devRoot is $XDG_DATA_HOME/wherry/dev, by default ~/.local/share/wherry/dev.
func devRoot() (string, error) {
	if data := os.Getenv("XDG_DATA_HOME"); data != "" {
		return filepath.Join(data, "wherry", "dev"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".local", "share", "wherry", "dev"), nil
}

func openInstance(name string) (devInstance, error) {
	if !wire.ValidName(name) {
		return devInstance{}, fmt.Errorf("instance %q is not a valid name", name)
	}
	root, err := devRoot()
	if err != nil {
		return devInstance{}, err
	}
	return devInstance{name: name, root: root, dir: filepath.Join(root, name)}, nil
}

func runDev(args []string, stdout, stderr io.Writer) error {
	const usage = "usage: wherry dev <init|up|down|reset> [--instance NAME]"
	if len(args) == 0 {
		return errors.New(usage)
	}
	if args[0] == "mock" {
		return runDevMock(args[1:], stdout)
	}
	flags := flag.NewFlagSet("dev "+args[0], flag.ContinueOnError)
	flags.SetOutput(stderr)
	name := flags.String("instance", "default", "instance name")
	var runtimeName, agent *string
	var storage *bool
	var images devAgentImages
	switch args[0] {
	case "init":
		runtimeName = flags.String("runtime", "", "container runtime, podman or docker (default: the one on PATH)")
		storage = flags.Bool("storage-enabled", false, "acquire and run S3 storage")
		agent = flags.String("agent", "", "default agent: a detected harness or mock")
	case "up":
		runtimeName = flags.String("runtime", "", "container runtime; must equal the one init recorded")
		flags.Var(&images, "agent-image", "NAME=IMAGE: run agent NAME in container image IMAGE (repeatable)")
	case "down", "reset":
	default:
		return errors.New(usage)
	}
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New(usage)
	}
	if runtimeName != nil && *runtimeName != "" && *runtimeName != "docker" && *runtimeName != "podman" {
		return errors.New("--runtime is podman or docker")
	}
	instance, err := openInstance(*name)
	if err != nil {
		return err
	}
	ctx := context.Background()
	switch args[0] {
	case "init":
		return devInit(ctx, instance, *runtimeName, *storage, *agent, stdout, stderr)
	case "up":
		return devUp(ctx, instance, *runtimeName, images, stdout, stderr)
	case "down":
		return devDown(ctx, instance, stdout, stderr)
	default:
		return devReset(instance, stdout)
	}
}

// devAgentImages are the --agent-image values, in order.
type devAgentImages []devAgentImage

type devAgentImage struct{ name, image string }

func (images *devAgentImages) String() string { return "" }

// Set splits at the first =, so the image may hold : and @sha256:.
func (images *devAgentImages) Set(value string) error {
	name, image, _ := strings.Cut(value, "=")
	switch {
	case image == "":
		return errors.New("want NAME=IMAGE")
	case name == "mock":
		return errors.New("the mock runs under raw_exec and takes no image")
	case !wire.ValidName(name):
		return fmt.Errorf("agent %q is not a valid name", name)
	case slices.ContainsFunc(*images, func(given devAgentImage) bool { return given.name == name }):
		return fmt.Errorf("agent %s is given twice", name)
	}
	*images = append(*images, devAgentImage{name, image})
	return nil
}

// readManifest reads the instance's manifest, nil when there is none.
func readManifest(instance devInstance) (*devManifest, error) {
	path := instance.cache("manifest.json")
	encoded, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var manifest devManifest
	if err := json.Unmarshal(encoded, &manifest); err != nil {
		return nil, fmt.Errorf("manifest %s: %w", path, err)
	}
	if manifest.Version != devManifestVersion {
		return nil, fmt.Errorf("manifest %s has version %d, which this wherry does not read; run wherry dev reset", path, manifest.Version)
	}
	return &manifest, nil
}

func writeManifest(instance devInstance, manifest devManifest) error {
	encoded, err := json.Marshal(manifest, json.Deterministic(true), jsontext.WithIndent("  "))
	if err != nil {
		return err
	}
	return writeFileAtomic(instance.cache("manifest.json"), append(encoded, '\n'), 0o600)
}

// writeFileAtomic writes data to path through a temporary file in its directory and a rename.
func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	file, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Chmod(mode); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), path)
}

// upState reads the instance's env/up.json, nil when the instance is not up: no file, or a nomad_pid that is not a
// live process.
func upState(instance devInstance) *devUpState {
	encoded, err := os.ReadFile(instance.env("up.json"))
	if err != nil {
		return nil
	}
	var state devUpState
	if json.Unmarshal(encoded, &state) != nil || !alive(state.NomadPID) {
		return nil
	}
	return &state
}

// alive reports whether pid is a live process; EPERM is a live process of another user.
func alive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

// refuseUp refuses init and reset on an instance that is up.
func refuseUp(instance devInstance) error {
	if upState(instance) != nil {
		return fmt.Errorf("instance %s is up; run wherry dev down first", instance.name)
	}
	return nil
}

// refuseAnyUp refuses up when this or any other instance under the root is up.
func refuseAnyUp(instance devInstance) error {
	if upState(instance) != nil {
		return fmt.Errorf("instance %s is already up", instance.name)
	}
	entries, _ := os.ReadDir(instance.root)
	for _, entry := range entries {
		other := devInstance{name: entry.Name(), root: instance.root, dir: filepath.Join(instance.root, entry.Name())}
		if entry.IsDir() && other.name != instance.name && upState(other) != nil {
			return fmt.Errorf("instance %s is up; run wherry dev down --instance %s first", other.name, other.name)
		}
	}
	return nil
}

// devSources are init's resolvers and fetchers; a variable so tests can stub them.
var devSources = struct {
	hashicorp func(ctx context.Context, product, cache, name string, prev *devArtefact, progress io.Writer) (*devArtefact, error)
	plugin    func(ctx context.Context, cache, name string, prev *devPlugin, progress io.Writer) (*devPlugin, error)
	image     func(ctx context.Context, runtime, reference, repository string, prev *devImage, output io.Writer) (*devImage, error)
	runtimeOK func(runtime string) bool
	lookPath  func(file string) (string, error)
}{acquireHashiCorp, acquirePlugin, acquireImage, runtimeAnswers, exec.LookPath}

func runtimeAnswers(runtime string) bool {
	if _, err := exec.LookPath(runtime); err != nil {
		return false
	}
	return exec.Command(runtime, "version").Run() == nil
}

const (
	kafkaReference   = "docker.io/apache/kafka:latest"
	storageReference = "quay.io/benjamin_holmes/ceph-aio:v21"
)

func devInit(ctx context.Context, instance devInstance, runtimeName string, storage bool, agent string, stdout, stderr io.Writer) error {
	if err := refuseUp(instance); err != nil {
		return err
	}
	prev, err := readManifest(instance)
	if err != nil {
		return err
	}
	step := func(name string) { fmt.Fprintln(stderr, name) }
	fail := func(name string, err error) error { return fmt.Errorf("step %s: %w", name, err) }

	step("runtime")
	if runtimeName == "" {
		var found []string
		for _, candidate := range []string{"docker", "podman"} {
			if devSources.runtimeOK(candidate) {
				found = append(found, candidate)
			}
		}
		switch len(found) {
		case 0:
			return fail("runtime", errors.New("neither docker nor podman is on PATH and answers version"))
		case 2:
			return fail("runtime", errors.New("found docker and podman; choose one with --runtime"))
		}
		runtimeName = found[0]
	} else if !devSources.runtimeOK(runtimeName) {
		return fail("runtime", fmt.Errorf("%s is not on PATH or does not answer version", runtimeName))
	}

	step("harnesses")
	var detected []string
	for _, harness := range devHarnesses {
		if _, err := devSources.lookPath(harness); err == nil {
			detected = append(detected, harness)
		}
	}
	defaultAgent, err := chooseAgent(detected, agent, stderr)
	if err != nil {
		return fail("harnesses", err)
	}

	if err := os.MkdirAll(instance.cache("plugins"), 0o700); err != nil {
		return err
	}
	for _, dir := range []string{instance.root, instance.dir, instance.cache()} {
		if err := os.Chmod(dir, 0o700); err != nil {
			return err
		}
	}
	if err := os.Chmod(instance.cache("plugins"), 0o755); err != nil {
		return err
	}
	if prev == nil {
		prev = &devManifest{}
	}
	next := devManifest{
		Version: devManifestVersion, OS: runtime.GOOS, Arch: runtime.GOARCH, Runtime: runtimeName,
		Agents: append([]string{}, detected...), DefaultAgent: defaultAgent, Images: map[string]devImage{},
		EnvDenylist: credentialNames(os.Environ()),
	}
	step("nomad")
	if next.Nomad, err = devSources.hashicorp(ctx, "nomad", instance.cache(), "nomad", prev.Nomad, stderr); err != nil {
		return fail("nomad", err)
	}
	if runtime.GOOS == "linux" && runtimeName == "podman" {
		step("podman-driver")
		if err := os.MkdirAll(instance.cache("nomad-plugins"), 0o700); err != nil {
			return err
		}
		if next.PodmanDriver, err = devSources.hashicorp(ctx, "nomad-driver-podman", instance.cache(), filepath.Join("nomad-plugins", "nomad-driver-podman"), prev.PodmanDriver, stderr); err != nil {
			return fail("podman-driver", err)
		}
	}
	step("plugin")
	if next.Plugin, err = devSources.plugin(ctx, instance.cache(), filepath.Join("plugins", "wherry-plugin-nomad"), prev.Plugin, stderr); err != nil {
		return fail("plugin", err)
	}
	step("image")
	var kafka *devImage
	if image, ok := prev.Images["kafka"]; ok && prev.Runtime == runtimeName {
		kafka = &image
	}
	if kafka, err = devSources.image(ctx, runtimeName, kafkaReference, "wherry-dev/kafka", kafka, stderr); err != nil {
		return fail("image", err)
	}
	next.Images["kafka"] = *kafka
	if storage {
		step("storage")
		previous := prev.Storage
		if prev.Runtime != runtimeName {
			previous = nil
		}
		if next.Storage, err = devSources.image(ctx, runtimeName, storageReference, "wherry-dev/ceph-aio", previous, stderr); err != nil {
			return fail("storage", err)
		}
	}
	step("snapshot")
	if next.Snapshot, err = snapshot(instance.cache("wherry"), prev.Snapshot); err != nil {
		return fail("snapshot", err)
	}

	step("manifest")
	unchanged := *prev
	unchanged.Updated = time.Time{}
	if reflect.DeepEqual(unchanged, next) {
		fmt.Fprintf(stdout, "instance %s: unchanged\n", instance.name)
		return nil
	}
	next.Updated = time.Now().UTC().Truncate(time.Second)
	if err := writeManifest(instance, next); err != nil {
		return fail("manifest", err)
	}
	fmt.Fprintf(stdout, "instance %s: acquired (runtime %s, nomad %s, plugin %s, default agent %s)\n",
		instance.name, next.Runtime, next.Nomad.Version, next.Plugin.Tag, next.DefaultAgent)
	return nil
}

// devStdin is where init asks for the default agent; a variable so tests can give it no terminal.
var devStdin = os.Stdin

// chooseAgent is the default agent from given, the detected harnesses or a terminal's answer.
func chooseAgent(detected []string, given string, stderr io.Writer) (string, error) {
	if given != "" {
		if given != "mock" && !slices.Contains(detected, given) {
			return "", fmt.Errorf("--agent %s is neither a detected harness (%s) nor mock", given, orNone(detected))
		}
		return given, nil
	}
	switch len(detected) {
	case 0:
		return "", errors.New("no agent harness found on PATH (looked for claude, codex, gemini); install one, or choose --agent mock")
	case 1:
		return detected[0], nil
	}
	found := strings.Join(detected[:len(detected)-1], ", ") + " and " + detected[len(detected)-1]
	if !isTerminal(devStdin) {
		return "", fmt.Errorf("found %s; choose the default with --agent", found)
	}
	fmt.Fprintf(stderr, "found %s; default agent [%s]: ", found, detected[0])
	answer, _ := bufio.NewReader(devStdin).ReadString('\n')
	answer = strings.TrimSpace(answer)
	if answer == "" {
		return detected[0], nil
	}
	return chooseAgent(detected, answer, stderr)
}

func orNone(names []string) string {
	if len(names) == 0 {
		return "none"
	}
	return strings.Join(names, ", ")
}

// credentialNames are the names in environ that end _TOKEN, _KEY, _SECRET or _PASSWORD, sorted.
func credentialNames(environ []string) []string {
	names := []string{}
	for _, entry := range environ {
		name, _, _ := strings.Cut(entry, "=")
		for _, suffix := range []string{"_TOKEN", "_KEY", "_SECRET", "_PASSWORD"} {
			if strings.HasSuffix(name, suffix) && !slices.Contains(names, name) {
				names = append(names, name)
			}
		}
	}
	slices.Sort(names)
	return names
}

// snapshot copies the running binary to file, unless prev records its SHA-256 already.
func snapshot(file string, prev devSnapshot) (devSnapshot, error) {
	source, err := os.Executable()
	if err != nil {
		return devSnapshot{}, err
	}
	if source, err = filepath.EvalSymlinks(source); err != nil {
		return devSnapshot{}, err
	}
	sum, err := fileSHA256(source)
	if err != nil {
		return devSnapshot{}, err
	}
	if sum == prev.SHA256 {
		return prev, nil
	}
	data, err := os.ReadFile(source)
	if err != nil {
		return devSnapshot{}, err
	}
	if err := writeFileAtomic(file, data, 0o755); err != nil {
		return devSnapshot{}, err
	}
	taken := devSnapshot{File: filepath.Base(file), SHA256: sum, Version: "(devel)"}
	if info, ok := debug.ReadBuildInfo(); ok {
		taken.Version = info.Main.Version
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				taken.Revision = setting.Value
			case "vcs.modified":
				taken.Modified = setting.Value == "true"
			}
		}
	}
	return taken, nil
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	sum := sha256.New()
	if _, err := io.Copy(sum, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(sum.Sum(nil)), nil
}

func devReset(instance devInstance, stdout io.Writer) error {
	if err := refuseUp(instance); err != nil {
		return err
	}
	if err := os.RemoveAll(instance.cache()); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "reset: instance %s\n", instance.name)
	return nil
}

// os.ModeCharDevice is set for /dev/null too, so a stdlib check would take init </dev/null for a terminal.
func isTerminal(file *os.File) bool {
	return term.IsTerminal(int(file.Fd()))
}
