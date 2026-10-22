package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/twmb/franz-go/pkg/kgo"
)

func runAdmin(args []string, stdout, stderr io.Writer) error {
	pos, options, platformFiles, err := adminArgs(args)
	if err != nil {
		return err
	}
	if len(pos) == 0 {
		return errors.New("usage: wherry admin <pool create|receiver add|receiver configure|tenant add|host add|revoke host|revoke receiver|skip|check>")
	}
	root, err := adminKafka("WHERRY_TRUST_ADMIN_USER", "", "WHERRY_TRUST_ADMIN_PASSWORD_FILE")
	if err != nil {
		return err
	}
	defer root.Close()
	operator, err := adminKafka("WHERRY_TRUST_OPERATOR_USER", admin.OperatorPrincipal, "WHERRY_TRUST_OPERATOR_PASSWORD_FILE")
	if err != nil {
		return err
	}
	defer operator.Close()
	ca, err := os.ReadFile(os.Getenv("WHERRY_TRUST_CA"))
	if err != nil {
		return err
	}
	service := admin.Service{Admin: root, Operator: operator, Broker: os.Getenv("WHERRY_TRUST_BROKER"), CA: string(ca)}
	ctx := context.Background()
	command := strings.Join(pos[:min(len(pos), 2)], " ")
	if pos[0] == "skip" {
		command = "skip"
	}
	switch command {
	case "pool create":
		if len(pos) != 3 {
			return errors.New("usage: wherry admin pool create NAME <--config-file FILE|--config-dir DIR>")
		}
		path, directory, err := configInput(options)
		if err != nil {
			return err
		}
		terms, err := readPoolConfig(path, directory)
		if err != nil {
			return err
		}
		return service.PoolCreate(ctx, pos[2], terms)
	case "receiver add":
		if len(pos) != 3 {
			return errors.New("usage: wherry admin receiver add ID --pool POOL <--config-file FILE|--config-dir DIR> --bundle-file FILE [--platform-credential-file PLATFORM=FILE ...]")
		}
		path, directory, err := configInput(options)
		if err != nil {
			return err
		}
		config, err := readReceiverConfig(path, directory)
		if err != nil {
			return err
		}
		config.Receiver, config.Pool = pos[2], options["pool"]
		platform, err := platformCredentials(config, platformFiles)
		if err != nil {
			return err
		}
		return service.ReceiverAdd(ctx, config, platform, options["bundle-file"])
	case "receiver configure":
		if len(pos) != 3 {
			return errors.New("usage: wherry admin receiver configure ID <--config-file FILE|--config-dir DIR>")
		}
		path, directory, err := configInput(options)
		if err != nil {
			return err
		}
		config, err := readReceiverConfig(path, directory)
		if err != nil {
			return err
		}
		config.Receiver = pos[2]
		return service.ReceiverConfigure(ctx, config)
	case "tenant add":
		if len(pos) != 3 {
			return errors.New("usage: wherry admin tenant add TENANT --pool POOL")
		}
		return service.TenantAdd(ctx, pos[2], options["pool"])
	case "host add":
		if len(pos) != 4 {
			return errors.New("usage: wherry admin host add TENANT HOST --bundle-file FILE")
		}
		return service.HostAdd(ctx, pos[2], pos[3], options["bundle-file"])
	case "revoke host", "revoke receiver":
		if pos[1] == "host" && len(pos) != 4 || pos[1] == "receiver" && len(pos) != 3 {
			return errors.New("invalid revoke command")
		}
		wait, err := time.ParseDuration(cmp.Or(options["wait"], "0s"))
		if err != nil {
			return err
		}
		topic := admin.ReceiverTopic(pos[len(pos)-1])
		if pos[1] == "host" {
			topic = admin.HostTopic(pos[2], pos[3])
		}
		probe := service.Probe(topic)
		var result admin.RevocationResult
		if pos[1] == "host" {
			result = service.RevokeHost(ctx, pos[2], pos[3], wait, probe)
		} else {
			result = service.RevokeReceiver(ctx, pos[2], wait, probe)
		}
		for _, step := range result.Steps {
			fmt.Fprintf(stdout, "step=%d complete=%t name=%s", step.Number, step.Complete, step.Name)
			if step.Err != nil {
				fmt.Fprintf(stdout, " error=%v", step.Err)
			}
			fmt.Fprintln(stdout)
		}
		if !result.Complete {
			return errors.New("revocation incomplete")
		}
		return nil
	case "skip":
		if len(pos) != 2 || options["sha256"] == "" {
			return errors.New("usage: wherry admin skip OFFSET --sha256 HASH")
		}
		offset, err := strconv.ParseInt(pos[1], 10, 64)
		if err != nil {
			return err
		}
		return service.Skip(ctx, offset, options["sha256"])
	case "check":
		if len(pos) != 1 {
			return errors.New("usage: wherry admin check")
		}
		return service.Check(ctx)
	default:
		return errors.New("unknown admin command")
	}
}

func adminArgs(args []string) ([]string, map[string]string, []string, error) {
	var positional, platformFiles []string
	var optionNames []string
	options := map[string]string{}
	for i := 0; i < len(args); i++ {
		if !strings.HasPrefix(args[i], "--") {
			positional = append(positional, args[i])
			continue
		}
		if i+1 == len(args) {
			return nil, nil, nil, errors.New("admin option requires a value")
		}
		name := strings.TrimPrefix(args[i], "--")
		switch _, repeated := options[name]; {
		case name == "platform-credential-file":
			platformFiles = append(platformFiles, args[i+1])
		case repeated:
			return nil, nil, nil, fmt.Errorf("option --%s is given twice", name)
		default:
			options[name] = args[i+1]
		}
		optionNames = append(optionNames, name)
		i++
	}
	command := strings.Join(positional[:min(len(positional), 2)], " ")
	if len(positional) > 0 && positional[0] == "skip" {
		command = "skip"
	}
	allowed := map[string]map[string]bool{
		"pool create":        {"config-file": true, "config-dir": true},
		"receiver add":       {"pool": true, "config-file": true, "config-dir": true, "bundle-file": true, "platform-credential-file": true},
		"receiver configure": {"config-file": true, "config-dir": true},
		"tenant add":         {"pool": true},
		"host add":           {"bundle-file": true},
		"revoke host":        {"wait": true},
		"revoke receiver":    {"wait": true},
		"skip":               {"sha256": true},
		"check":              {},
	}[command]
	if allowed != nil {
		for _, name := range optionNames {
			if !allowed[name] {
				return nil, nil, nil, fmt.Errorf("unknown option --%s", name)
			}
		}
	}
	return positional, options, platformFiles, nil
}

func configInput(options map[string]string) (string, bool, error) {
	file, dir := options["config-file"], options["config-dir"]
	if (file == "") == (dir == "") {
		return "", false, errors.New("exactly one of --config-file or --config-dir is required")
	}
	path, directory, flagName := file, false, "--config-file"
	if dir != "" {
		path, directory, flagName = dir, true, "--config-dir"
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", false, err
	}
	if info.IsDir() != directory {
		kind := "file"
		if directory {
			kind = "directory"
		}
		return "", false, fmt.Errorf("%s requires a %s: %s", flagName, kind, path)
	}
	return path, directory, nil
}

func readYAML(path string, value any) error {
	if path == "" {
		return errors.New("required file option is missing")
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return admin.DecodeYAML(encoded, value)
}

// platformCredentials reads each PLATFORM=FILE of --platform-credential-file, refusing a platform no adapter of config
// uses or one named twice.
func platformCredentials(config admin.ReceiverConfig, files []string) (map[string]string, error) {
	var platform map[string]string
	for _, value := range files {
		name, path, ok := strings.Cut(value, "=")
		if !ok {
			return nil, errors.New("--platform-credential-file takes PLATFORM=FILE")
		}
		if !slices.ContainsFunc(config.Adapters, func(adapter admin.AdapterSettings) bool { return adapter.Platform == name }) {
			return nil, fmt.Errorf("--platform-credential-file names platform %q, which no adapter in the configuration uses", name)
		}
		if _, repeated := platform[name]; repeated {
			return nil, fmt.Errorf("--platform-credential-file names platform %q twice", name)
		}
		credential, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if platform == nil {
			platform = map[string]string{}
		}
		platform[name] = strings.TrimRight(string(credential), "\r\n")
	}
	return platform, nil
}

// receiverPlatform and receiverAdapter are the platform and adapter entries of a receiver config, in either form.
type receiverPlatform struct {
	Name   string         `yaml:"name"`
	Type   string         `yaml:"type"`
	Config map[string]any `yaml:"config"`
}

type receiverAdapter struct {
	Name       string            `yaml:"name"`
	Platform   string            `yaml:"platform"`
	Agent      string            `yaml:"agent"`
	Output     string            `yaml:"output"`
	ReplyFile  string            `yaml:"reply_file"`
	Credential string            `yaml:"credential"`
	Labels     map[string]string `yaml:"labels"`
	Config     map[string]any    `yaml:"config"`
}

// readReceiverConfig reads a receiver config from a single file holding capacity, platforms and adapters, or from a
// directory holding receiver.yaml, platforms/*.yaml and adapters/*.yaml, where adapter file order is routing order.
// An adapter's config takes its platform's top-level keys, which it must not repeat.
func readReceiverConfig(path string, directory bool) (admin.ReceiverConfig, error) {
	if !directory {
		var file struct {
			Capacity  int64              `yaml:"capacity"`
			Platforms []receiverPlatform `yaml:"platforms"`
			Adapters  []receiverAdapter  `yaml:"adapters"`
		}
		if err := readYAML(path, &file); err != nil {
			return admin.ReceiverConfig{}, err
		}
		return receiverConfig(file.Capacity, labelled(path, "platforms", file.Platforms), labelled(path, "adapters", file.Adapters))
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return admin.ReceiverConfig{}, err
	}
	for _, entry := range entries {
		if name := entry.Name(); !strings.HasPrefix(name, ".") && name != "receiver.yaml" && name != "platforms" && name != "adapters" {
			return admin.ReceiverConfig{}, fmt.Errorf("%s does not belong in a receiver config directory", filepath.Join(path, name))
		}
	}
	var receiver struct {
		Capacity int64 `yaml:"capacity"`
	}
	if err := readYAML(filepath.Join(path, "receiver.yaml"), &receiver); err != nil {
		return admin.ReceiverConfig{}, err
	}
	platforms, err := readEntries[receiverPlatform](filepath.Join(path, "platforms"))
	if err != nil {
		return admin.ReceiverConfig{}, err
	}
	adapters, err := readEntries[receiverAdapter](filepath.Join(path, "adapters"))
	if err != nil {
		return admin.ReceiverConfig{}, err
	}
	if len(adapters) == 0 {
		return admin.ReceiverConfig{}, fmt.Errorf("%s holds no adapters", filepath.Join(path, "adapters"))
	}
	return receiverConfig(receiver.Capacity, platforms, adapters)
}

// entry is a decoded platform or adapter with the place it was read from, which errors name.
type entry[T any] struct {
	where string
	value T
}

func labelled[T any](path, list string, values []T) []entry[T] {
	entries := make([]entry[T], len(values))
	for i, value := range values {
		entries[i] = entry[T]{fmt.Sprintf("%s: %s[%d]", path, list, i), value}
	}
	return entries
}

// readEntries decodes each .yaml file in dir, in name order.
func readEntries[T any](dir string) ([]entry[T], error) {
	files, err := yamlFiles(dir)
	if err != nil {
		return nil, err
	}
	var entries []entry[T]
	for _, file := range files {
		var value T
		if err := readYAML(file, &value); err != nil {
			return nil, fmt.Errorf("%s: %w", file, err)
		}
		entries = append(entries, entry[T]{file, value})
	}
	return entries, nil
}

func receiverConfig(capacity int64, platforms []entry[receiverPlatform], adapters []entry[receiverAdapter]) (admin.ReceiverConfig, error) {
	config := admin.ReceiverConfig{Capacity: cmp.Or(capacity, 1)}
	byName := map[string]receiverPlatform{}
	for _, p := range platforms {
		if _, ok := byName[p.value.Name]; ok || p.value.Name == "" {
			return config, fmt.Errorf("%s: platform name %q is empty or repeated", p.where, p.value.Name)
		}
		byName[p.value.Name] = p.value
	}
	names := map[string]bool{}
	for _, a := range adapters {
		adapter := a.value
		if names[adapter.Name] {
			return config, fmt.Errorf("%s: adapter name %q is repeated", a.where, adapter.Name)
		}
		names[adapter.Name] = true
		p, ok := byName[adapter.Platform]
		if !ok {
			return config, fmt.Errorf("%s: unknown platform %q", a.where, adapter.Platform)
		}
		if adapter.Config == nil {
			adapter.Config = map[string]any{}
		}
		for key, value := range p.Config {
			if _, ok := adapter.Config[key]; ok {
				return config, fmt.Errorf("%s: config key %q is also set by platform %q", a.where, key, p.Name)
			}
			adapter.Config[key] = value
		}
		config.Adapters = append(config.Adapters, admin.AdapterSettings{Name: adapter.Name, Platform: p.Name, Type: p.Type, Agent: adapter.Agent, Output: adapter.Output, ReplyFile: adapter.ReplyFile, Credential: adapter.Credential, Labels: adapter.Labels, Config: adapter.Config})
	}
	return config, nil
}

func readPoolConfig(path string, directory bool) (admin.Terms, error) {
	var terms admin.Terms
	if !directory {
		return terms, readYAML(path, &terms)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return terms, err
	}
	for _, entry := range entries {
		if name := entry.Name(); !strings.HasPrefix(name, ".") && name != "pool.yaml" && name != "agents" {
			return terms, fmt.Errorf("%s does not belong in a pool config directory", filepath.Join(path, name))
		}
	}
	if err := readYAML(filepath.Join(path, "pool.yaml"), &terms); err != nil {
		return terms, err
	}
	if len(terms.Agents) > 0 {
		return terms, fmt.Errorf("%s: agents belong in %s", filepath.Join(path, "pool.yaml"), filepath.Join(path, "agents"))
	}
	files, err := yamlFiles(filepath.Join(path, "agents"))
	if err != nil {
		return terms, err
	}
	if len(files) == 0 {
		return terms, fmt.Errorf("%s holds no agents", filepath.Join(path, "agents"))
	}
	for _, file := range files {
		var agent admin.AgentTerms
		if err := readYAML(file, &agent); err != nil {
			return terms, fmt.Errorf("%s: %w", file, err)
		}
		terms.Agents = append(terms.Agents, agent)
	}
	return terms, nil
}

// yamlFiles lists dir's files in lexical order and rejects anything other
// than a .yaml file, so a misnamed file fails instead of being skipped.
func yamlFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		file := filepath.Join(dir, entry.Name())
		if entry.IsDir() || filepath.Ext(file) != ".yaml" {
			return nil, fmt.Errorf("%s is not a .yaml file", file)
		}
		files = append(files, file)
	}
	return files, nil
}

// adminKafka connects as the user userEnv names, or defaultUser when it is unset.
func adminKafka(userEnv, defaultUser, passwordEnv string) (*kgo.Client, error) {
	broker, caPath, user, passwordPath := os.Getenv("WHERRY_TRUST_BROKER"), os.Getenv("WHERRY_TRUST_CA"), cmp.Or(os.Getenv(userEnv), defaultUser), os.Getenv(passwordEnv)
	if broker == "" || caPath == "" || user == "" || passwordPath == "" {
		return nil, errors.New("trust broker credentials are not configured")
	}
	ca, err := os.ReadFile(caPath)
	if err != nil {
		return nil, err
	}
	password, err := os.ReadFile(passwordPath)
	if err != nil {
		return nil, err
	}
	opts, err := admin.KafkaOpts(broker, ca, user, string(password))
	if err != nil {
		return nil, fmt.Errorf("trust %w", err)
	}
	return kgo.NewClient(append(opts, kgo.FetchIsolationLevel(kgo.ReadCommitted()))...)
}
