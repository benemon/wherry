package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"gopkg.in/yaml.v3"
)

func runAdmin(args []string, stdout, stderr io.Writer) error {
	pos, options, err := adminArgs(args)
	if err != nil {
		return err
	}
	if len(pos) == 0 {
		return errors.New("usage: wherry admin <pool create|receiver add|receiver configure|tenant add|host add|revoke host|revoke receiver|skip|check>")
	}
	root, err := adminKafka("WHERRY_TRUST_ADMIN_USER", "WHERRY_TRUST_ADMIN_PASSWORD_FILE")
	if err != nil {
		return err
	}
	defer root.Close()
	operator, err := adminKafka("WHERRY_TRUST_OPERATOR_USER", "WHERRY_TRUST_OPERATOR_PASSWORD_FILE")
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
			return errors.New("usage: wherry admin receiver add ID --pool POOL <--config-file FILE|--config-dir DIR> --bundle-file FILE [--platform-credential-file ADAPTER=FILE]")
		}
		path, directory, err := configInput(options)
		if err != nil {
			return err
		}
		config, err := readReceiverConfig(path, directory)
		if err != nil {
			return err
		}
		if (config.Receiver != "" && config.Receiver != pos[2]) || (config.Pool != "" && config.Pool != options["pool"]) {
			return errors.New("receiver config identity does not match command")
		}
		config.Receiver, config.Pool = pos[2], options["pool"]
		var platform map[string]string
		if value := options["platform-credential-file"]; value != "" {
			adapter, path, ok := strings.Cut(value, "=")
			if !ok {
				return errors.New("--platform-credential-file takes ADAPTER=FILE")
			}
			credential, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			platform = map[string]string{adapter: strings.TrimRight(string(credential), "\r\n")}
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
		if config.Receiver != "" && config.Receiver != pos[2] {
			return errors.New("receiver config identity does not match command")
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
		wait, err := time.ParseDuration(defaultValue(options["wait"], "0s"))
		if err != nil {
			return err
		}
		topic := admin.ReceiverTopic(pos[len(pos)-1])
		if pos[1] == "host" {
			topic = admin.HostTopic(pos[2], pos[3])
		}
		probe := service.Probe(topic)
		var result admin.RevocationResult
		if pos[1] == "host" && len(pos) == 4 {
			result = service.RevokeHost(ctx, pos[2], pos[3], wait, probe)
		} else if pos[1] == "receiver" && len(pos) == 3 {
			result = service.RevokeReceiver(ctx, pos[2], wait, probe)
		} else {
			return errors.New("invalid revoke command")
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

func adminArgs(args []string) ([]string, map[string]string, error) {
	var positional []string
	var optionNames []string
	options := map[string]string{}
	for i := 0; i < len(args); i++ {
		if !strings.HasPrefix(args[i], "--") {
			positional = append(positional, args[i])
			continue
		}
		if i+1 == len(args) {
			return nil, nil, errors.New("admin option requires a value")
		}
		name := strings.TrimPrefix(args[i], "--")
		options[name] = args[i+1]
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
				return nil, nil, fmt.Errorf("unknown option --%s", name)
			}
		}
	}
	return positional, options, nil
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
	decoder := yaml.NewDecoder(bytes.NewReader(encoded))
	decoder.KnownFields(true)
	return decoder.Decode(value)
}

// readReceiverConfig reads a receiver config from a single file or from a
// directory holding receiver.yaml, platforms/*.yaml and adapters/*.yaml, where
// adapter file order is routing order and an adapter's config takes its
// platform's top-level keys, which it must not repeat.
func readReceiverConfig(path string, directory bool) (admin.ReceiverConfig, error) {
	var config admin.ReceiverConfig
	if !directory {
		return config, readYAML(path, &config)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return config, err
	}
	for _, entry := range entries {
		if name := entry.Name(); !strings.HasPrefix(name, ".") && name != "receiver.yaml" && name != "platforms" && name != "adapters" {
			return config, fmt.Errorf("%s does not belong in a receiver config directory", filepath.Join(path, name))
		}
	}
	var receiver struct {
		Capacity int64 `yaml:"capacity"`
	}
	if err := readYAML(filepath.Join(path, "receiver.yaml"), &receiver); err != nil {
		return config, err
	}
	config.Capacity = receiver.Capacity
	type platform struct {
		Name   string         `yaml:"name"`
		Type   string         `yaml:"type"`
		Config map[string]any `yaml:"config"`
	}
	platforms := map[string]platform{}
	files, err := yamlFiles(filepath.Join(path, "platforms"))
	if err != nil {
		return config, err
	}
	for _, file := range files {
		var p platform
		if err := readYAML(file, &p); err != nil {
			return config, fmt.Errorf("%s: %w", file, err)
		}
		if _, ok := platforms[p.Name]; ok || p.Name == "" {
			return config, fmt.Errorf("%s: platform name %q is empty or repeated", file, p.Name)
		}
		platforms[p.Name] = p
	}
	files, err = yamlFiles(filepath.Join(path, "adapters"))
	if err != nil {
		return config, err
	}
	if len(files) == 0 {
		return config, fmt.Errorf("%s holds no adapters", filepath.Join(path, "adapters"))
	}
	names := map[string]bool{}
	for _, file := range files {
		var adapter struct {
			Name       string            `yaml:"name"`
			Platform   string            `yaml:"platform"`
			Agent      string            `yaml:"agent"`
			Credential string            `yaml:"credential"`
			Labels     map[string]string `yaml:"labels"`
			Config     map[string]any    `yaml:"config"`
		}
		if err := readYAML(file, &adapter); err != nil {
			return config, fmt.Errorf("%s: %w", file, err)
		}
		if names[adapter.Name] {
			return config, fmt.Errorf("%s: adapter name %q is repeated", file, adapter.Name)
		}
		names[adapter.Name] = true
		p, ok := platforms[adapter.Platform]
		if !ok {
			return config, fmt.Errorf("%s: unknown platform %q", file, adapter.Platform)
		}
		if adapter.Config == nil {
			adapter.Config = map[string]any{}
		}
		for key, value := range p.Config {
			if _, ok := adapter.Config[key]; ok {
				return config, fmt.Errorf("%s: config key %q is also set by platform %q", file, key, p.Name)
			}
			adapter.Config[key] = value
		}
		config.Adapters = append(config.Adapters, admin.AdapterSettings{Name: adapter.Name, Type: p.Type, Agent: adapter.Agent, Credential: adapter.Credential, Labels: adapter.Labels, Config: adapter.Config})
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

func adminKafka(userEnv, passwordEnv string) (*kgo.Client, error) {
	broker, caPath, user, passwordPath := os.Getenv("WHERRY_TRUST_BROKER"), os.Getenv("WHERRY_TRUST_CA"), os.Getenv(userEnv), os.Getenv(passwordEnv)
	if broker == "" || caPath == "" || user == "" || passwordPath == "" {
		return nil, errors.New("trust broker credentials are not configured")
	}
	ca, err := os.ReadFile(caPath)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("trust CA contains no certificates")
	}
	password, err := os.ReadFile(passwordPath)
	if err != nil {
		return nil, err
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.DialTLSConfig(&tls.Config{RootCAs: roots}), kgo.SASL(scram.Auth{User: user, Pass: strings.TrimRight(string(password), "\r\n")}.AsSha512Mechanism()), kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	return client, err
}

func defaultValue(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
