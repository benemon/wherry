package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
	"gopkg.in/yaml.v3"
)

func runAdmin(args []string, stdout, stderr io.Writer) error {
	pos, options, err := adminArgs(args)
	if err != nil {
		return err
	}
	if len(pos) == 0 {
		return errors.New("usage: wherry admin <pool create|pool entitle|receiver add|receiver configure|receiver show|receiver revoke|host add|host revoke|skip|check>")
	}
	broker, caPath := os.Getenv("WHERRY_TRUST_BROKER"), os.Getenv("WHERRY_TRUST_CA")
	if broker == "" || caPath == "" {
		return errors.New("trust broker credentials are not configured")
	}
	ca, err := os.ReadFile(caPath)
	if err != nil {
		return err
	}
	root, err := adminKafka(broker, ca, os.Getenv("WHERRY_TRUST_ADMIN_USER"), "WHERRY_TRUST_ADMIN_PASSWORD_FILE")
	if err != nil {
		return err
	}
	defer root.Close()
	operator, err := adminKafka(broker, ca, admin.OperatorPrincipal, "WHERRY_TRUST_OPERATOR_PASSWORD_FILE")
	if err != nil {
		return err
	}
	defer operator.Close()
	service := admin.Service{Admin: root, Operator: operator, Broker: broker, CA: string(ca)}
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
		_, dryRun := options["dry-run"]
		if len(pos) != 3 || options["pool"] == "" || options["bundle-file"] == "" && !dryRun {
			return errors.New("usage: wherry admin receiver add ID --pool POOL <--config-file FILE|--config-dir DIR> <--bundle-file FILE|--dry-run>")
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
		if dryRun {
			if config, err = service.ReceiverCheck(ctx, config); err != nil {
				return err
			}
			return showReceiver(stdout, config)
		}
		return service.ReceiverAdd(ctx, config, options["bundle-file"])
	case "receiver configure":
		if len(pos) != 3 {
			return errors.New("usage: wherry admin receiver configure ID <--config-file FILE|--config-dir DIR> [--dry-run]")
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
		_, dryRun := options["dry-run"]
		if config, err = service.ReceiverConfigure(ctx, config, dryRun); err != nil || !dryRun {
			return err
		}
		return showReceiver(stdout, config)
	case "receiver show":
		if len(pos) != 3 {
			return errors.New("usage: wherry admin receiver show ID")
		}
		state, err := service.LoadState(ctx)
		if err != nil {
			return err
		}
		config, ok := state.Receivers[pos[2]]
		if !ok {
			return errors.New("no receiver by that name")
		}
		return showReceiver(stdout, config)
	case "pool entitle":
		if len(pos) != 3 || options["tenant"] == "" {
			return errors.New("usage: wherry admin pool entitle POOL --tenant TENANT")
		}
		return service.TenantAdd(ctx, options["tenant"], pos[2])
	case "host add":
		if len(pos) != 3 || options["tenant"] == "" || options["bundle-file"] == "" {
			return errors.New("usage: wherry admin host add HOST --tenant TENANT --bundle-file FILE")
		}
		return service.HostAdd(ctx, options["tenant"], pos[2], options["bundle-file"])
	case "host revoke", "receiver revoke":
		if pos[0] == "host" && (len(pos) != 3 || options["tenant"] == "") {
			return errors.New("usage: wherry admin host revoke HOST --tenant TENANT [--wait DURATION]")
		}
		if len(pos) != 3 {
			return errors.New("usage: wherry admin receiver revoke RECEIVER [--wait DURATION]")
		}
		wait, err := time.ParseDuration(cmp.Or(options["wait"], "0s"))
		if err != nil {
			return err
		}
		if _, given := options["wait"]; given && wait <= 0 {
			return errors.New("--wait must be positive; omit it to compute the safe wait from the broker's timeouts")
		}
		topic := admin.ReceiverTopic(pos[2])
		if pos[0] == "host" {
			topic = admin.HostTopic(options["tenant"], pos[2])
		}
		probe := service.Probe(topic)
		var result admin.RevocationResult
		if pos[0] == "host" {
			result = service.RevokeHost(ctx, options["tenant"], pos[2], wait, probe)
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

// showReceiver writes config's capacity and resolved profiles, in routing order, to out as YAML.
func showReceiver(out io.Writer, config admin.ReceiverConfig) error {
	// The record decodes numbers as float64, which YAML would print as 1e+06.
	for i := range config.Profiles {
		config.Profiles[i].Config = integral(config.Profiles[i].Config).(map[string]any)
	}
	encoded, err := yaml.Marshal(struct {
		Capacity int64                   `yaml:"capacity"`
		Profiles []admin.ProfileSettings `yaml:"profiles"`
	}{config.Capacity, config.Profiles})
	if err != nil {
		return err
	}
	_, err = out.Write(encoded)
	return err
}

// integral is value with each whole float64 within it as an int64.
func integral(value any) any {
	switch value := value.(type) {
	case float64:
		if value == math.Trunc(value) && math.Abs(value) < 1<<53 {
			return int64(value)
		}
	case map[string]any:
		for key, inner := range value {
			value[key] = integral(inner)
		}
	case []any:
		for i, inner := range value {
			value[i] = integral(inner)
		}
	}
	return value
}

func adminArgs(args []string) ([]string, map[string]string, error) {
	var positional, optionNames []string
	options := map[string]string{}
	for i := 0; i < len(args); i++ {
		if !strings.HasPrefix(args[i], "--") {
			positional = append(positional, args[i])
			continue
		}
		name := strings.TrimPrefix(args[i], "--")
		if _, repeated := options[name]; repeated {
			return nil, nil, fmt.Errorf("option --%s is given twice", name)
		}
		// --dry-run is the one option without a value.
		if name == "dry-run" {
			options[name], optionNames = "", append(optionNames, name)
			continue
		}
		if i+1 == len(args) {
			return nil, nil, errors.New("admin option requires a value")
		}
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
		"receiver add":       {"pool": true, "config-file": true, "config-dir": true, "bundle-file": true, "dry-run": true},
		"receiver configure": {"config-file": true, "config-dir": true, "dry-run": true},
		"receiver show":      {},
		"pool entitle":       {"tenant": true},
		"host add":           {"tenant": true, "bundle-file": true},
		"host revoke":        {"tenant": true, "wait": true},
		"receiver revoke":    {"wait": true},
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
	return admin.DecodeYAML(encoded, value)
}

// receiverPlugin and receiverProfile are a plugin's and a profile's entries in a receiver config, in either form. A nil
// label value is a null.
type receiverPlugin struct {
	Labels   map[string]*string `yaml:"labels"`
	Config   map[string]any     `yaml:"config"`
	Profiles []receiverProfile  `yaml:"profiles"`
}

type receiverProfile struct {
	Name   string             `yaml:"name"`
	Agent  string             `yaml:"agent"`
	Labels map[string]*string `yaml:"labels"`
	Config map[string]any     `yaml:"config"`
}

// readReceiverConfig reads a receiver config from a single file holding capacity and plugins, or from a directory
// holding receiver.yaml and, for each plugin, plugins/<type>/plugin.yaml with plugins/<type>/profiles/*.yaml,
// where profile file order is routing order within the plugin.
func readReceiverConfig(path string, directory bool) (admin.ReceiverConfig, error) {
	if !directory {
		var file struct {
			Capacity int64                     `yaml:"capacity"`
			Plugins  map[string]receiverPlugin `yaml:"plugins"`
		}
		if err := readYAML(path, &file); err != nil {
			return admin.ReceiverConfig{}, err
		}
		var plugins []plugin
		for kind, value := range file.Plugins {
			where := fmt.Sprintf("%s: plugins.%s", path, kind)
			plugins = append(plugins, plugin{where, kind, value.Labels, value.Config, labelled(where, "profiles", value.Profiles)})
		}
		return receiverConfig(file.Capacity, plugins)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return admin.ReceiverConfig{}, err
	}
	for _, entry := range entries {
		if name := entry.Name(); !strings.HasPrefix(name, ".") && name != "receiver.yaml" && name != "plugins" {
			return admin.ReceiverConfig{}, fmt.Errorf("%s does not belong in a receiver config directory", filepath.Join(path, name))
		}
	}
	var receiver struct {
		Capacity int64 `yaml:"capacity"`
	}
	if err := readYAML(filepath.Join(path, "receiver.yaml"), &receiver); err != nil {
		return admin.ReceiverConfig{}, err
	}
	kinds, err := os.ReadDir(filepath.Join(path, "plugins"))
	if err != nil {
		return admin.ReceiverConfig{}, err
	}
	var plugins []plugin
	for _, kind := range kinds {
		dir := filepath.Join(path, "plugins", kind.Name())
		if strings.HasPrefix(kind.Name(), ".") {
			continue
		}
		if !kind.IsDir() {
			return admin.ReceiverConfig{}, fmt.Errorf("%s is not a plugin directory", dir)
		}
		files, err := os.ReadDir(dir)
		if err != nil {
			return admin.ReceiverConfig{}, err
		}
		for _, file := range files {
			if name := file.Name(); !strings.HasPrefix(name, ".") && name != "plugin.yaml" && name != "profiles" {
				return admin.ReceiverConfig{}, fmt.Errorf("%s does not belong in a plugin directory", filepath.Join(dir, name))
			}
		}
		var settings struct {
			Labels map[string]*string `yaml:"labels"`
			Config map[string]any     `yaml:"config"`
		}
		if err := readYAML(filepath.Join(dir, "plugin.yaml"), &settings); err != nil && !errors.Is(err, os.ErrNotExist) {
			return admin.ReceiverConfig{}, fmt.Errorf("%s: %w", filepath.Join(dir, "plugin.yaml"), err)
		}
		profiles, err := readEntries[receiverProfile](filepath.Join(dir, "profiles"))
		if err != nil {
			return admin.ReceiverConfig{}, err
		}
		plugins = append(plugins, plugin{dir, kind.Name(), settings.Labels, settings.Config, profiles})
	}
	return receiverConfig(receiver.Capacity, plugins)
}

// plugin is a decoded plugin entry with the place it was read from, which errors name.
type plugin struct {
	where, kind string
	labels      map[string]*string
	config      map[string]any
	profiles    []entry[receiverProfile]
}

// entry is a decoded profile with the place it was read from, which errors name.
type entry[T any] struct {
	where string
	value T
}

func labelled[T any](path, list string, values []T) []entry[T] {
	entries := make([]entry[T], len(values))
	for i, value := range values {
		entries[i] = entry[T]{fmt.Sprintf("%s.%s[%d]", path, list, i), value}
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

// receiverConfig lists the profiles of plugins sorted by type, each plugin's in their order, every profile's labels and
// config merged onto its plugin's.
func receiverConfig(capacity int64, plugins []plugin) (admin.ReceiverConfig, error) {
	config := admin.ReceiverConfig{Capacity: cmp.Or(capacity, 1)}
	if len(plugins) == 0 {
		return config, errors.New("receiver config holds no plugins")
	}
	slices.SortFunc(plugins, func(a, b plugin) int { return strings.Compare(a.kind, b.kind) })
	names := map[string]bool{}
	for _, p := range plugins {
		if !wire.ValidName(p.kind) {
			return config, fmt.Errorf("%s: plugin %q is not a valid name", p.where, p.kind)
		}
		if len(p.profiles) == 0 {
			return config, fmt.Errorf("%s holds no profiles", p.where)
		}
		labels := nullable(p.labels)
		for field, values := range map[string]map[string]any{"labels": labels, "config": p.config} {
			if path := nullAt(values, ""); path != "" {
				return config, fmt.Errorf("%s: plugin %s sets %s key %s to null", p.where, p.kind, field, path)
			}
		}
		for _, e := range p.profiles {
			profile := e.value
			if names[profile.Name] {
				return config, fmt.Errorf("%s: profile name %q is repeated", e.where, profile.Name)
			}
			names[profile.Name] = true
			merged, err := merge(p.config, profile.Config, "")
			if err != nil {
				return config, fmt.Errorf("%s: profile %q: config %w", e.where, profile.Name, err)
			}
			// A null deletes a key; inside a list it has nothing to delete, and no plugin receives one.
			if path := nullAt(merged, ""); path != "" {
				return config, fmt.Errorf("%s: profile %q: config %s is null inside a list", e.where, profile.Name, path)
			}
			if name, ok := merged["credential"]; ok {
				if _, ok := name.(string); !ok {
					return config, fmt.Errorf("%s: profile %q: credential is not a string", e.where, profile.Name)
				}
			}
			mergedLabels, err := merge(labels, nullable(profile.Labels), "")
			if err != nil {
				return config, fmt.Errorf("%s: profile %q: labels %w", e.where, profile.Name, err)
			}
			settings := admin.ProfileSettings{Name: profile.Name, Plugin: p.kind, Agent: profile.Agent, Config: merged}
			for key, value := range mergedLabels {
				if settings.Labels == nil {
					settings.Labels = map[string]string{}
				}
				settings.Labels[key] = value.(string)
			}
			config.Profiles = append(config.Profiles, settings)
		}
	}
	return config, nil
}

// merge is over laid on base, changing neither: maps merge key by key, over winning on every leaf it sets; any other
// value of over, a list included, replaces base's whole; and a null in over removes base's key, or is dropped where
// base has none. A map against a scalar or a list at one key is refused naming its dotted path.
func merge(base, over map[string]any, path string) (map[string]any, error) {
	merged := maps.Clone(base)
	if merged == nil {
		merged = map[string]any{}
	}
	for key, value := range over {
		at := strings.TrimPrefix(path+"."+key, ".")
		inherited, ok := merged[key]
		baseMap, baseIsMap := inherited.(map[string]any)
		overMap, overIsMap := value.(map[string]any)
		switch {
		case value == nil:
			delete(merged, key)
		case ok && baseIsMap != overIsMap:
			return nil, fmt.Errorf("key %s is a map at one level and not at the other", at)
		case overIsMap:
			var err error
			if merged[key], err = merge(baseMap, overMap, at); err != nil {
				return nil, err
			}
		default:
			merged[key] = value
		}
	}
	return merged, nil
}

// nullAt is the dotted path of a null in values, a list element written as path[i], empty without one.
func nullAt(values map[string]any, path string) string {
	for key, value := range values {
		if found := nullIn(value, strings.TrimPrefix(path+"."+key, ".")); found != "" {
			return found
		}
	}
	return ""
}

func nullIn(value any, at string) string {
	switch value := value.(type) {
	case nil:
		return at
	case map[string]any:
		return nullAt(value, at)
	case []any:
		for i, element := range value {
			if found := nullIn(element, fmt.Sprintf("%s[%d]", at, i)); found != "" {
				return found
			}
		}
	}
	return ""
}

// nullable is labels as merge takes them, a nil value staying a null.
func nullable(labels map[string]*string) map[string]any {
	values := map[string]any{}
	for key, value := range labels {
		values[key] = nil
		if value != nil {
			values[key] = *value
		}
	}
	return values
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

// adminKafka connects to broker, trusting ca, as user with the password in the file passwordEnv names.
func adminKafka(broker string, ca []byte, user, passwordEnv string) (*kgo.Client, error) {
	passwordPath := os.Getenv(passwordEnv)
	if user == "" || passwordPath == "" {
		return nil, errors.New("trust broker credentials are not configured")
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
