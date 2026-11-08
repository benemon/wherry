package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
	"gopkg.in/yaml.v3"
)

// operatorUsage is each operator command's usage, under "" the operator's.
var operatorUsage = map[string]string{
	"":                   "usage: wherry operator <init|pool apply|tenant add|tenant list|tenant pool add|tenant pool remove|tenant host add|tenant host remove|tenant remove|pool suspend|pool resume|pool rotate-key|receiver apply|receiver show|receiver remove|check> [--dir DIR]",
	"init":               "usage: wherry operator init [--dir DIR] [--broker ADDR] [--ca-file PATH] [--super-user NAME --super-user-password-file FILE]",
	"pool apply":         "usage: wherry operator pool apply NAME [-f PATH] [--dry-run]",
	"tenant add":         "usage: wherry operator tenant add TENANT",
	"tenant list":        "usage: wherry operator tenant list",
	"tenant remove":      "usage: wherry operator tenant remove TENANT",
	"tenant pool add":    "usage: wherry operator tenant pool add TENANT POOL",
	"tenant pool remove": "usage: wherry operator tenant pool remove TENANT POOL",
	"pool suspend":       "usage: wherry operator pool suspend POOL",
	"pool resume":        "usage: wherry operator pool resume POOL",
	"pool rotate-key":    "usage: wherry operator pool rotate-key POOL [--suspect-key KID]...",
	"receiver apply":     "usage: wherry operator receiver apply NAME [-f PATH] [--dry-run] | NAME --force",
	"receiver show":      "usage: wherry operator receiver show ID",
	"receiver remove":    "usage: wherry operator receiver remove RECEIVER [--wait DURATION]",
	"tenant host add":    "usage: wherry operator tenant host add TENANT HOST [--force]",
	"tenant host remove": "usage: wherry operator tenant host remove TENANT HOST [--wait DURATION]",
	"check":              "usage: wherry operator check",
}

func runOperator(args []string, stdout, stderr io.Writer) error {
	pos, options, err := operatorArgs(args)
	if len(pos) > 0 && operatorUsage[operatorCommand(pos)] == "" {
		return errors.New("unknown operator command")
	}
	if errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(stderr, cmp.Or(operatorUsage[operatorCommand(pos)], operatorUsage[""]))
	}
	if err != nil {
		return err
	}
	if len(pos) == 0 {
		return errors.New(operatorUsage[""])
	}
	if _, ok := operatorUsage[operatorCommand(pos)]; !ok {
		return errors.New("unknown operator command")
	}
	dir := cmp.Or(options["dir"], ".")
	ctx := context.Background()
	if pos[0] == "init" {
		if len(pos) != 1 {
			return errors.New(operatorUsage["init"])
		}
		return operatorInit(ctx, dir, options, stderr)
	}
	service, err := operatorService(dir)
	if err != nil {
		return err
	}
	defer service.Operator.Close()
	command := operatorCommand(pos)
	switch command {
	case "pool apply":
		if len(pos) != 3 {
			return errors.New(operatorUsage[command])
		}
		_, dryRun := options["dry-run"]
		path, directory, err := objectFile(ctx, dir, "pools", pos[2], options, dryRun, stderr, poolFields, promptPool)
		if err != nil {
			return err
		}
		terms, err := readPoolConfig(path, directory)
		if err != nil {
			return err
		}
		return service.PoolApply(ctx, pos[2], terms, dryRun)
	case "receiver apply":
		_, force := options["force"]
		_, dryRun := options["dry-run"]
		if len(pos) != 3 {
			return errors.New(operatorUsage[command])
		}
		var path string
		var directory bool
		var config operator.ReceiverConfig
		if !force {
			path, directory, err = objectFile(ctx, dir, "receivers", pos[2], options, dryRun, stderr, receiverFields, func(q *questions) (any, error) { return promptReceiver(newPrompter(q.out), service.Broker) })
			if err != nil {
				return err
			}
			config, err = readReceiverConfig(path, directory)
			if err != nil {
				return err
			}
		}
		state, err := service.LoadState(ctx)
		if err != nil {
			return err
		}
		current, exists := state.Receivers[pos[2]]
		bundle, err := bundlePath(dir, "receivers", false, pos[2])
		if err != nil {
			return err
		}
		if force {
			if !exists || options["f"] != "" || dryRun {
				return errors.New("usage: wherry operator receiver apply NAME --force: --force gives an existing receiver a new password and bundle only")
			}
			broker := service.Broker
			if current, err := operator.ReadBundle(bundle); err == nil {
				broker = current.Broker
			}
			if kept, err := readReceiverConfig(filepath.Join(dir, "receivers", pos[2]+".yaml"), false); err == nil && kept.Broker != "" {
				broker = kept.Broker
			}
			if _, err := bundlePath(dir, "receivers", true, pos[2]); err != nil {
				return err
			}
			return service.ReceiverReissue(ctx, pos[2], broker, bundle)
		}
		config.Receiver, config.Broker = pos[2], cmp.Or(config.Broker, service.Broker)
		if exists && config.Pool != "" && config.Pool != current.Pool {
			return errors.New("receiver cannot move pool")
		}
		if !exists && config.Pool == "" {
			return fmt.Errorf("%s names no pool", path)
		}
		var recorded operator.ReceiverConfig
		if exists {
			recorded, err = service.ReceiverUpdate(ctx, config, true)
		} else {
			recorded, err = service.ReceiverCheck(ctx, config)
		}
		if err != nil {
			return err
		}
		if err := receiverLocalFile(dir, pos[2], path, directory, dryRun, stderr, config, service.Broker, service.CA != nil); err != nil {
			return err
		}
		if dryRun {
			return showReceiver(stdout, recorded)
		}
		// The bundle directories are made, or hardened to 0700, only once both configurations have passed their checks.
		if _, err := bundlePath(dir, "receivers", true, pos[2]); err != nil {
			return err
		}
		if exists {
			// Only the bundle's password can carry a new broker, so a missing bundle is refused before recording.
			kept, err := operator.ReadBundle(bundle)
			if errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("%s does not exist, so a new broker could not reach the receiver; --force gives the receiver a new password and bundle", bundle)
			} else if err != nil {
				return err
			}
			if _, err := service.ReceiverUpdate(ctx, config, false); err != nil {
				return err
			}
			return rebroker(bundle, kept, config.Broker)
		}
		return service.ReceiverAdd(ctx, config, config.Broker, bundle)

	case "receiver show":
		if len(pos) != 3 {
			return errors.New(operatorUsage[command])
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
	case "tenant add":
		if len(pos) != 3 {
			return errors.New(operatorUsage[command])
		}
		return service.TenantAdd(ctx, pos[2])
	case "tenant list":
		if len(pos) != 2 {
			return errors.New(operatorUsage[command])
		}
		state, err := service.LoadState(ctx)
		if err != nil {
			return err
		}
		for _, tenant := range slices.Sorted(maps.Keys(state.Tenants)) {
			if state.Tenants[tenant] {
				continue
			}
			fmt.Fprintln(stdout, tenant)
			for _, pool := range slices.Sorted(maps.Keys(state.Entitlements[tenant])) {
				fmt.Fprintf(stdout, "  pool %s\n", pool)
			}
			for _, principal := range slices.Sorted(maps.Keys(state.Hosts)) {
				host := state.Hosts[principal]
				if host.Tenant == tenant && !state.Ended[principal] && !state.Revoking[principal] {
					fmt.Fprintf(stdout, "  host %s\n", host.Name)
				}
			}
		}
		return nil
	case "tenant pool add", "tenant pool remove":
		if len(pos) != 5 {
			return errors.New(operatorUsage[command])
		}
		state, err := service.LoadState(ctx)
		if err != nil {
			return err
		}
		entitled := state.Entitlements[pos[3]][pos[4]]
		if pos[2] == "add" {
			err = service.TenantPoolAdd(ctx, pos[3], pos[4])
			if err == nil && entitled {
				fmt.Fprintln(stdout, "already entitled")
			}
		} else {
			err = service.TenantPoolRemove(ctx, pos[3], pos[4])
			if err == nil && !entitled {
				fmt.Fprintln(stdout, "not entitled")
			}
		}
		return err
	case "tenant remove":
		if len(pos) != 3 {
			return errors.New(operatorUsage[command])
		}
		if !wire.ValidName(pos[2]) {
			return errors.New("invalid tenant name")
		}
		if err := service.TenantRemove(ctx, pos[2]); err != nil {
			return err
		}
		if err := os.RemoveAll(filepath.Join(dir, "bundles", "hosts", pos[2])); err != nil {
			return err
		}
		fmt.Fprintln(stdout, "done")
		return nil
	case "pool suspend", "pool resume":
		if len(pos) != 3 {
			return errors.New(operatorUsage[command])
		}
		outcome, err := service.PoolSuspendAcknowledged(ctx, pos[2], pos[1] == "suspend", stderr)
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, outcome)
		return nil
	case "pool rotate-key":
		if len(pos) != 3 {
			return errors.New(operatorUsage[command])
		}
		var suspects []string
		if options["suspect-key"] != "" {
			suspects = strings.Split(options["suspect-key"], "\n")
		}
		report, err := service.PoolRotateKey(ctx, pos[2], suspects, stderr)
		if err != nil {
			return err
		}
		writeRotateReport(stdout, pos[2], report, time.Now())
		return nil
	case "tenant host add":
		if len(pos) != 5 {
			return errors.New(operatorUsage[command])
		}
		bundle, err := bundlePath(dir, "hosts", true, pos[3], pos[4])
		if err != nil {
			return err
		}
		_, force := options["force"]
		return service.HostAdd(ctx, pos[3], pos[4], bundle, force)
	case "tenant host remove", "receiver remove":
		if pos[0] == "tenant" && (len(pos) != 5) {
			return errors.New(operatorUsage[command])
		}
		if pos[0] == "receiver" && len(pos) != 3 {
			return errors.New(operatorUsage[command])
		}
		wait, err := time.ParseDuration(cmp.Or(options["wait"], "0s"))
		if err != nil {
			return err
		}
		if _, given := options["wait"]; given && wait <= 0 {
			return errors.New("--wait must be positive; omit it to compute the safe wait from the broker's timeouts")
		}
		topic := operator.ReceiverTopic(pos[2])
		if pos[0] == "tenant" {
			topic = operator.HostTopic(pos[3], pos[4])
		}
		probe := service.Probe(topic)
		var result operator.RevocationResult
		if pos[0] == "tenant" {
			result = service.RevokeHost(ctx, pos[3], pos[4], wait, probe)
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
	case "check":
		if len(pos) != 1 {
			return errors.New(operatorUsage[command])
		}
		return service.Check(ctx)
	default:
		return errors.New("unknown operator command")
	}
}

// writeRotateReport writes report's suspect keys, retention horizon and suspect tasks, naming each task and never a value.
func writeRotateReport(out io.Writer, pool string, report operator.RotateReport, now time.Time) {
	fmt.Fprintf(out, "pool %s's key custodian rotated for the rotate-key request; tasks appended through that rotation are listed\n", pool)
	fmt.Fprintf(out, "suspect_keys=%s\n", strings.Join(report.SuspectKeys, ","))
	for _, topic := range report.Topics {
		fmt.Fprintf(out, "horizon topic=%s oldest_retained_offset=%d\n", topic, report.Horizon[topic])
	}
	for _, topic := range slices.Sorted(maps.Keys(report.DeletedFrom)) {
		fmt.Fprintf(out, "horizon topic=%s: retention deleted the tasks at offsets %d to %d during the scan; they may not be listed\n", topic, report.DeletedFrom[topic], report.Horizon[topic]-1)
	}
	if report.OldestMS == 0 {
		fmt.Fprintln(out, "horizon: the task topics retain no task; tasks deleted by retention are not listed")
	} else {
		oldest := time.UnixMilli(report.OldestMS)
		fmt.Fprintf(out, "horizon: the oldest retained task was appended at %s, %s ago; tasks deleted by retention before it are not listed\n", oldest.UTC().Format(time.RFC3339), now.Sub(oldest).Truncate(time.Second))
	}
	for _, task := range report.Tasks {
		fmt.Fprintf(out, "task run=%s tenant=%s key=%s credentials=%s appended=%s\n", task.Run, task.Tenant, task.KeyID, strings.Join(task.Credentials, ","), time.UnixMilli(task.AppendedMS).UTC().Format(time.RFC3339))
	}
	fmt.Fprintf(out, "tasks=%d\n", len(report.Tasks))
}

// showReceiver writes resolved profiles and plugin pins as YAML.
func showReceiver(out io.Writer, config operator.ReceiverConfig) error {
	type shown struct {
		operator.ProfileSettings `yaml:",inline"`
		Signature                yaml.Node `yaml:"signature"`
		Topic                    string    `yaml:"topic"`
	}
	profiles := make([]shown, 0, len(config.Profiles))
	for _, profile := range config.Profiles {
		// The record decodes numbers as float64, which YAML would print as 1e+06.
		profile.Config = integral(profile.Config).(map[string]any)
		signature := yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Style: yaml.DoubleQuotedStyle, Value: operator.Signature(profile.Agent, profile.Labels)}
		if profile.Replicas == 1 {
			profile.Replicas = 0
		}
		profiles = append(profiles, shown{profile, signature, operator.SignatureTopic(config.Pool, profile.Agent, profile.Labels)})
	}
	encoded, err := yaml.Marshal(struct {
		PluginSHA256 map[string]string `yaml:"plugin_sha256,omitempty"`
		Profiles     []shown           `yaml:"profiles"`
	}{config.PluginSHA256, profiles})
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

func operatorArgs(args []string) ([]string, map[string]string, error) {
	flags := flag.NewFlagSet("operator", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	for _, name := range []string{"f", "wait", "sha256", "dir", "broker", "ca-file", "super-user", "super-user-password-file"} {
		flags.String(name, "", "")
	}
	var suspects []string
	flags.Func("suspect-key", "", func(value string) error { suspects = append(suspects, value); return nil })
	flags.Bool("dry-run", false, "")
	flags.Bool("force", false, "")
	positional, err := parseAnywhere(flags, args)
	if err != nil {
		return positional, nil, err
	}
	options := map[string]string{}
	flags.Visit(func(option *flag.Flag) {
		getter, ok := option.Value.(flag.Getter)
		if !ok {
			return
		}
		if set, ok := getter.Get().(bool); ok && !set {
			return
		}
		options[option.Name] = option.Value.String()
	})
	if len(suspects) > 0 {
		options["suspect-key"] = strings.Join(suspects, "\n")
	}
	command := operatorCommand(positional)
	allowed := map[string]map[string]bool{
		"init":           {"broker": true, "ca-file": true, "super-user": true, "super-user-password-file": true},
		"pool apply":     {"f": true, "dry-run": true},
		"receiver apply": {"f": true, "dry-run": true, "force": true},
		"receiver show":  {},
		"tenant add":     {}, "tenant list": {}, "tenant remove": {}, "tenant pool add": {}, "tenant pool remove": {},
		"pool suspend":       {},
		"pool resume":        {},
		"pool rotate-key":    {"suspect-key": true},
		"tenant host add":    {"force": true},
		"tenant host remove": {"wait": true},
		"receiver remove":    {"wait": true},
		"check":              {},
	}[command]
	if allowed != nil {
		for _, name := range slices.Sorted(maps.Keys(options)) {
			if !allowed[name] && name != "dir" {
				return nil, nil, fmt.Errorf("unknown option --%s", name)
			}
		}
	}
	return positional, options, nil
}

func readYAML(path string, value any) error {
	if path == "" {
		return errors.New("required file option is missing")
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return operator.DecodeYAML(encoded, value)
}

// receiverPlugin and receiverProfile are a plugin's and a profile's entries in a receiver config, in either form. A nil
// label value is a null.
type receiverPlugin struct {
	Labels   map[string]*string `yaml:"labels"`
	Config   map[string]any     `yaml:"config"`
	Profiles []receiverProfile  `yaml:"profiles"`
}

type receiverProfile struct {
	Name     string             `yaml:"name"`
	Agent    string             `yaml:"agent"`
	Replicas int                `yaml:"replicas,omitempty"`
	Labels   map[string]*string `yaml:"labels"`
	Config   map[string]any     `yaml:"config"`
}

// readReceiverConfig decodes a receiver file or directory in profile recorded order.
func readReceiverConfig(path string, directory bool) (operator.ReceiverConfig, error) {
	if !directory {
		var file struct {
			receiverHead `yaml:",inline"`
			Plugins      map[string]receiverPlugin `yaml:"plugins"`
		}
		if err := readYAML(path, &file); err != nil {
			return operator.ReceiverConfig{}, err
		}
		var plugins []plugin
		for kind, value := range file.Plugins {
			where := fmt.Sprintf("%s: plugins.%s", path, kind)
			plugins = append(plugins, plugin{where, kind, value.Labels, value.Config, labelled(where, "profiles", value.Profiles)})
		}
		return receiverConfig(file.receiverHead, plugins)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return operator.ReceiverConfig{}, err
	}
	for _, entry := range entries {
		if name := entry.Name(); !strings.HasPrefix(name, ".") && name != "receiver.yaml" && name != "config.yaml" && name != "plugins" {
			return operator.ReceiverConfig{}, fmt.Errorf("%s does not belong in a receiver config directory", filepath.Join(path, name))
		}
	}
	var receiver struct {
		receiverHead `yaml:",inline"`
	}
	if err := readYAML(filepath.Join(path, "receiver.yaml"), &receiver); err != nil {
		return operator.ReceiverConfig{}, err
	}
	kinds, err := os.ReadDir(filepath.Join(path, "plugins"))
	if err != nil {
		return operator.ReceiverConfig{}, err
	}
	var plugins []plugin
	for _, kind := range kinds {
		dir := filepath.Join(path, "plugins", kind.Name())
		if strings.HasPrefix(kind.Name(), ".") {
			continue
		}
		if !kind.IsDir() {
			return operator.ReceiverConfig{}, fmt.Errorf("%s is not a plugin directory", dir)
		}
		files, err := os.ReadDir(dir)
		if err != nil {
			return operator.ReceiverConfig{}, err
		}
		for _, file := range files {
			if name := file.Name(); !strings.HasPrefix(name, ".") && name != "plugin.yaml" && name != "profiles" {
				return operator.ReceiverConfig{}, fmt.Errorf("%s does not belong in a plugin directory", filepath.Join(dir, name))
			}
		}
		var settings struct {
			Labels map[string]*string `yaml:"labels"`
			Config map[string]any     `yaml:"config"`
		}
		if err := readYAML(filepath.Join(dir, "plugin.yaml"), &settings); err != nil && !errors.Is(err, os.ErrNotExist) {
			return operator.ReceiverConfig{}, fmt.Errorf("%s: %w", filepath.Join(dir, "plugin.yaml"), err)
		}
		profiles, err := readEntries[receiverProfile](filepath.Join(dir, "profiles"))
		if err != nil {
			return operator.ReceiverConfig{}, err
		}
		plugins = append(plugins, plugin{dir, kind.Name(), settings.Labels, settings.Config, profiles})
	}
	return receiverConfig(receiver.receiverHead, plugins)
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

// receiverHead is a receiver config's pool, plugin pins and the broker address its bundle names, empty for the
// operator's.
type receiverHead struct {
	Pool         string            `yaml:"pool"`
	PluginSHA256 map[string]string `yaml:"plugin_sha256"`
	Broker       string            `yaml:"broker"`
}

// receiverConfig merges plugin defaults into profiles in recorded order.
func receiverConfig(head receiverHead, plugins []plugin) (operator.ReceiverConfig, error) {
	config := operator.ReceiverConfig{Pool: head.Pool, PluginSHA256: head.PluginSHA256, Broker: head.Broker}
	if config.PluginSHA256 == nil {
		config.PluginSHA256 = map[string]string{}
	}
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
			settings := operator.ProfileSettings{Name: profile.Name, Plugin: p.kind, Agent: profile.Agent, Replicas: profile.Replicas, Config: merged}
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

func readPoolConfig(path string, directory bool) (operator.Terms, error) {
	var terms operator.Terms
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
		var agent operator.AgentTerms
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

// operatorKafka connects to broker as user with password, trusting ca or, when ca is nil, the system roots.
var operatorKafka = func(broker string, ca []byte, user, password string) (*kgo.Client, error) {
	opts, err := operator.KafkaOpts(broker, ca, user, password)
	if err != nil {
		return nil, fmt.Errorf("broker %w", err)
	}
	return kgo.NewClient(append(opts, kgo.FetchIsolationLevel(kgo.ReadCommitted()))...)
}

func operatorCommand(pos []string) string {
	if len(pos) > 0 && (pos[0] == "init" || pos[0] == "check") {
		return pos[0]
	}
	length := 2
	if len(pos) >= 2 && pos[0] == "tenant" && (pos[1] == "pool" || pos[1] == "host") {
		length = 3
	}
	return strings.Join(pos[:min(len(pos), length)], " ")
}
