package main

import (
	"cmp"
	"context"
	"crypto/x509"
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

// operatorUsage is each operator verb's usage, under "" the operator's. A verb with several forms lists them with |.
var operatorUsage = map[string]string{
	"":        "usage: wherry operator <init|check|view|list|show|apply|add|remove|allow|deny|suspend|resume|rotate|storage|refresh> [--dir DIR]",
	"init":    "usage: wherry operator init [--dir DIR] [--broker ADDR] [--super-user NAME --super-user-password @FILE|@-] [--super-user-mechanism SCRAM-SHA-512|SCRAM-SHA-256] [--ca-cert PEM|@FILE|@-]",
	"check":   "usage: wherry operator check",
	"view":    "usage: wherry operator view [--dir DIR] [--window 24h] [--port N] [--external-url URL]",
	"list":    "usage: wherry operator list tenants|pools|receivers | list hosts [--tenant TENANT]",
	"show":    "usage: wherry operator show --receiver RECEIVER",
	"apply":   "usage: wherry operator apply --pool POOL [--file PATH] [--dry-run] | apply --receiver RECEIVER [--file PATH] [--dry-run] | apply --receiver RECEIVER --force",
	"add":     "usage: wherry operator add --tenant TENANT | add --host HOST --tenant TENANT [--force]",
	"remove":  "usage: wherry operator remove --tenant TENANT | remove --host HOST --tenant TENANT [--timeout DURATION] | remove --receiver RECEIVER [--timeout DURATION]",
	"allow":   "usage: wherry operator allow --tenant TENANT --pool POOL",
	"deny":    "usage: wherry operator deny --tenant TENANT --pool POOL",
	"suspend": "usage: wherry operator suspend --pool POOL",
	"resume":  "usage: wherry operator resume --pool POOL",
	"rotate":  "usage: wherry operator rotate --pool POOL [--suspect-key KID]...",
	"storage": "usage: wherry operator storage --tenant TENANT [-]",
	"refresh": "usage: wherry operator refresh --host HOST --tenant TENANT",
}

// operatorForm is one form of a verb: the resource flags it takes, sorted, and the other options allowed beside --dir.
type operatorForm struct {
	resources []string
	options   []string
}

// operatorForms are each verb's forms. A verb's resource flags are matched against them as a set, so a missing, extra
// or mixed resource flag fits no form and is the verb's usage error; an option outside the matched form is refused by
// name. list takes --tenant for hosts alone, which operatorArgs checks against the kind.
var operatorForms = map[string][]operatorForm{
	"init":    {{nil, []string{"broker", "super-user", "super-user-password", "super-user-mechanism", "ca-cert"}}},
	"check":   {{nil, nil}},
	"view":    {{nil, []string{"window", "port", "external-url"}}},
	"list":    {{nil, nil}, {[]string{"tenant"}, nil}},
	"show":    {{[]string{"receiver"}, nil}},
	"apply":   {{[]string{"pool"}, []string{"file", "dry-run"}}, {[]string{"receiver"}, []string{"file", "dry-run", "force"}}},
	"add":     {{[]string{"tenant"}, nil}, {[]string{"host", "tenant"}, []string{"force"}}},
	"remove":  {{[]string{"tenant"}, nil}, {[]string{"host", "tenant"}, []string{"timeout"}}, {[]string{"receiver"}, []string{"timeout"}}},
	"allow":   {{[]string{"pool", "tenant"}, nil}},
	"deny":    {{[]string{"pool", "tenant"}, nil}},
	"suspend": {{[]string{"pool"}, nil}},
	"resume":  {{[]string{"pool"}, nil}},
	"rotate":  {{[]string{"pool"}, []string{"suspect-key"}}},
	"storage": {{[]string{"tenant"}, nil}},
	"refresh": {{[]string{"host", "tenant"}, nil}},
}

// operatorResources are the resource flags, each naming one object of its kind.
var operatorResources = []string{"host", "pool", "receiver", "tenant"}

// listKinds are what list takes positionally.
var listKinds = []string{"tenants", "pools", "receivers", "hosts"}

func runOperator(args []string, stdout, stderr io.Writer) (err error) {
	// Each command's first dial is inside it, so a broker certificate the connection does not trust is explained here,
	// once, naming the working directory's operator.yaml, whose ca_cert takes the CA, and whether it holds one; init
	// explains its own, naming the file it writes, and an explanation made is left as it is.
	ca := operator.CASetting{File: "operator.yaml"}
	defer func() { err = operator.ExplainUnknownAuthority(err, ca) }()
	pos, options, err := operatorArgs(args)
	if len(pos) > 0 && operatorUsage[pos[0]] == "" {
		return errors.New("unknown operator command")
	}
	if errors.Is(err, flag.ErrHelp) {
		if len(pos) > 0 {
			fmt.Fprintln(stderr, operatorUsage[pos[0]])
		} else {
			fmt.Fprintln(stderr, operatorUsage[""])
		}
	}
	if err != nil {
		return err
	}
	if len(pos) == 0 {
		return errors.New(operatorUsage[""])
	}
	command := pos[0]
	switch {
	case command == "list" && len(pos) == 2 && slices.Contains(listKinds, pos[1]):
	case command == "storage" && (len(pos) == 1 || (len(pos) == 2 && pos[1] == "-")):
	case command != "list" && command != "storage" && len(pos) == 1:
	default:
		return errors.New(operatorUsage[command])
	}
	dir := cmp.Or(options["dir"], ".")
	ca.File = filepath.Join(dir, ca.File)
	ctx := context.Background()
	if command == "init" {
		return operatorInit(ctx, dir, options, stderr)
	}
	service, config, err := operatorService(dir)
	ca.Set = config.CACert != ""
	if err != nil {
		return err
	}
	defer service.Operator.Close()
	defer func() {
		if err := service.Tracing.Close(); err != nil {
			fmt.Fprintln(stderr, "wherry: tracing:", err)
		}
	}()
	pool, receiver, tenant, host := options["pool"], options["receiver"], options["tenant"], options["host"]
	switch command {
	case "apply":
		_, dryRun := options["dry-run"]
		if pool != "" {
			path, directory, err := objectFile(ctx, dir, "pools", pool, options, dryRun, stderr, poolFields, promptPool)
			if err != nil {
				return err
			}
			terms, err := readPoolConfig(path, directory)
			if err != nil {
				return err
			}
			return service.PoolApply(ctx, pool, terms, dryRun)
		}
		_, force := options["force"]
		var path string
		var directory bool
		var config operator.ReceiverConfig
		if !force {
			path, directory, err = objectFile(ctx, dir, "receivers", receiver, options, dryRun, stderr, receiverFields, func(q *questions) (any, error) {
				state, err := service.LoadState(q.ctx)
				if err != nil {
					return nil, err
				}
				return promptReceiver(q, receiver, state, service.Broker)
			})
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
		current, exists := state.Receivers[receiver]
		bundle, err := bundlePath(dir, "receivers", false, receiver)
		if err != nil {
			return err
		}
		if force {
			if !exists || options["file"] != "" || dryRun {
				return errors.New("usage: wherry operator apply --receiver RECEIVER --force: --force gives an existing receiver a new password and bundle only")
			}
			broker := service.Broker
			if current, err := operator.ReadBundle(bundle); err == nil {
				broker = current.Broker
			}
			if kept, err := readReceiverConfig(filepath.Join(dir, "receivers", receiver+".yaml"), false); err == nil && kept.Broker != "" {
				broker = kept.Broker
			}
			if _, err := bundlePath(dir, "receivers", true, receiver); err != nil {
				return err
			}
			if err := service.ReceiverReissue(ctx, receiver, broker, bundle); err != nil {
				return err
			}
			return receiverBundleNext(stdout, "wrote", bundle, "start")
		}
		config.Receiver, config.Broker = receiver, cmp.Or(config.Broker, service.Broker)
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
		if err := receiverLocalFile(ctx, dir, receiver, path, directory, dryRun, stderr, config); err != nil {
			return err
		}
		if dryRun {
			return showReceiver(stdout, recorded, nil)
		}
		// The bundle directories are made, or hardened to 0700, only once both configurations have passed their checks.
		if _, err := bundlePath(dir, "receivers", true, receiver); err != nil {
			return err
		}
		if exists {
			// Only the bundle's password can carry a new broker, so a missing bundle is refused before recording.
			kept, err := operator.ReadBundle(bundle)
			if errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("%s does not exist, so a new broker could not reach the receiver; --force gives the receiver a new password and bundle", bundle)
			} else if errors.Is(err, operator.ErrBundleCA) {
				return fmt.Errorf("%w; --force gives the receiver a new password and bundle", err)
			} else if err != nil {
				return err
			}
			// A kept bundle of another principal would be rewritten as this receiver's and handed to it; refused before
			// the record changes, as a host refresh refuses one.
			if principal := operator.ReceiverPrincipal(receiver); kept.Principal != principal {
				return fmt.Errorf("%s is the bundle of %s, not of %s", bundle, kept.Principal, principal)
			}
			if _, err := service.ReceiverUpdate(ctx, config, false); err != nil {
				return err
			}
			written, err := refreshBundle(bundle, kept, service.Bundle(config.Broker, kept.Principal, kept.Password))
			if err != nil || !written {
				return err
			}
			return receiverBundleNext(stdout, "refreshed", bundle, "restart")
		}
		if err := service.ReceiverAdd(ctx, config, config.Broker, bundle); err != nil {
			return err
		}
		return receiverBundleNext(stdout, "wrote", bundle, "start")
	case "show":
		state, err := service.LoadState(ctx)
		if err != nil {
			return err
		}
		config, ok := state.Receivers[receiver]
		if !ok {
			return errors.New("no receiver by that name")
		}
		liveness, err := readReceiverLiveness(ctx, service, state, receiver)
		if err != nil {
			return err
		}
		return showReceiver(stdout, config, liveness)
	case "add":
		if host == "" {
			return service.TenantAdd(ctx, tenant)
		}
		bundle, err := bundlePath(dir, "hosts", true, tenant, host)
		if err != nil {
			return err
		}
		_, force := options["force"]
		if err := service.HostAdd(ctx, tenant, host, bundle, force); err != nil {
			return err
		}
		return hostBundleNext(stdout, "wrote", bundle)
	case "storage":
		// A trailing - takes the pair from standard input.
		return tenantStorage(ctx, service, dir, tenant, len(pos) == 2, stdout, stderr)
	case "list":
		state, err := service.LoadState(ctx)
		if err != nil {
			return err
		}
		return listState(stdout, state, pos[1], tenant)
	case "allow", "deny":
		state, err := service.LoadState(ctx)
		if err != nil {
			return err
		}
		entitled := state.Entitlements[tenant][pool]
		if command == "allow" {
			err = service.TenantPoolAdd(ctx, tenant, pool)
			if err == nil && entitled {
				fmt.Fprintln(stdout, "already entitled")
			}
		} else {
			err = service.TenantPoolRemove(ctx, tenant, pool)
			if err == nil && !entitled {
				fmt.Fprintln(stdout, "not entitled")
			}
		}
		return err
	case "suspend", "resume":
		outcome, err := service.PoolSuspendAcknowledged(ctx, pool, command == "suspend", stderr)
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, outcome)
		return nil
	case "rotate":
		var suspects []string
		if options["suspect-key"] != "" {
			suspects = strings.Split(options["suspect-key"], "\n")
		}
		report, err := service.PoolRotateKey(ctx, pool, suspects, stderr)
		if err != nil {
			return err
		}
		writeRotateReport(stdout, pool, report, time.Now())
		return nil
	case "refresh":
		// The bundle must already be kept, so no directory is made.
		bundle, err := bundlePath(dir, "hosts", false, tenant, host)
		if err != nil {
			return err
		}
		changed, err := service.HostRefresh(ctx, tenant, host, bundle)
		if err != nil {
			return err
		}
		if !changed {
			fmt.Fprintf(stdout, "%s is current\n", bundle)
			return nil
		}
		return hostBundleNext(stdout, "refreshed", bundle)
	case "remove":
		if host == "" && receiver == "" {
			if !wire.ValidName(tenant) {
				return errors.New("invalid tenant name")
			}
			if err := service.TenantRemove(ctx, tenant); err != nil {
				return err
			}
			if err := os.RemoveAll(filepath.Join(dir, "bundles", "hosts", tenant)); err != nil {
				return err
			}
			fmt.Fprintln(stdout, "done")
			return nil
		}
		timeout, err := time.ParseDuration(cmp.Or(options["timeout"], "0s"))
		if err != nil {
			return err
		}
		if _, given := options["timeout"]; given && timeout <= 0 {
			return errors.New("--timeout must be positive; omit it to compute the safe wait from the broker's timeouts")
		}
		topic := operator.ReceiverTopic(receiver)
		if host != "" {
			topic = operator.HostTopic(tenant, host)
		}
		probe := revocationProbe(service, topic)
		// Each step is printed as it ends, and step 5 as it starts, so the wait shows as a wait and not a hang.
		progress := func(step operator.RevocationStep) {
			if !step.Until.IsZero() {
				fmt.Fprintf(stdout, "step=%d started name=%s until=%s\n", step.Number, step.Name, step.Until.UTC().Format(time.RFC3339))
				return
			}
			fmt.Fprintf(stdout, "step=%d complete=%t name=%s", step.Number, step.Complete, step.Name)
			if step.Err != nil {
				fmt.Fprintf(stdout, " error=%v", step.Err)
			}
			fmt.Fprintln(stdout)
		}
		var result operator.RevocationResult
		var bundle string
		if host != "" {
			result = service.RevokeHost(ctx, tenant, host, timeout, probe, progress)
			bundle, err = bundlePath(dir, "hosts", false, tenant, host)
		} else {
			result = service.RevokeReceiver(ctx, receiver, timeout, probe, progress)
			bundle, err = bundlePath(dir, "receivers", false, receiver)
		}
		if !result.Complete {
			return errors.New("revocation incomplete")
		}
		if err != nil {
			return err
		}
		// The bundle's principal can never log in again, so the file is only a password to keep safe; a rerun after an
		// already-ended result deletes a bundle an earlier failure left.
		if err := os.Remove(bundle); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("revocation complete; delete %s: %w", bundle, err)
		}
		return nil
	case "check":
		if err := service.Check(ctx); err != nil {
			return err
		}
		// What removal left is reported only once every check passes; leftovers are no failure.
		leftovers, err := service.Leftovers(ctx)
		if err != nil {
			return err
		}
		return writeLeftovers(stdout, leftovers)
	case "view":
		return operatorView(ctx, service, options, stdout, stderr)
	default:
		return errors.New("unknown operator command")
	}
}

// listState prints the live objects of kind in state: tenants as each tenant's name over its entitled pools and live
// hosts, hosts as each tenant's name over its live hosts, of tenant alone when given, and pools and receivers as one
// name per line.
func listState(out io.Writer, state operator.State, kind, tenant string) error {
	tenants := slices.Sorted(maps.Keys(state.Tenants))
	if tenant != "" {
		if ended, ok := state.Tenants[tenant]; !ok || ended {
			return errors.New("no tenant by that name")
		}
		tenants = []string{tenant}
	}
	hosts := func(tenant string) {
		for _, principal := range slices.Sorted(maps.Keys(state.Hosts)) {
			host := state.Hosts[principal]
			if host.Tenant == tenant && !state.Ended[principal] && !state.Revoking[principal] {
				fmt.Fprintf(out, "  host %s\n", host.Name)
			}
		}
	}
	switch kind {
	case "tenants", "hosts":
		for _, tenant := range tenants {
			if state.Tenants[tenant] {
				continue
			}
			fmt.Fprintln(out, tenant)
			if kind == "tenants" {
				for _, pool := range slices.Sorted(maps.Keys(state.Entitlements[tenant])) {
					fmt.Fprintf(out, "  pool %s\n", pool)
				}
			}
			hosts(tenant)
		}
	case "pools":
		for _, pool := range slices.Sorted(maps.Keys(state.Pools)) {
			fmt.Fprintln(out, pool)
		}
	case "receivers":
		for _, receiver := range slices.Sorted(maps.Keys(state.Receivers)) {
			if !state.Ended[operator.ReceiverPrincipal(receiver)] {
				fmt.Fprintln(out, receiver)
			}
		}
	}
	return nil
}

// hostBundleNext prints that a host bundle was written or refreshed, as did, and the host init that installs it.
func hostBundleNext(out io.Writer, did, bundle string) error {
	_, err := fmt.Fprintf(out, "%s %s; hand it to the host, where wherry host init %s installs it\n", did, bundle, filepath.Base(bundle))
	return err
}

// receiverBundleNext prints that a receiver bundle was written or refreshed, as did, and the platform step that installs
// it before the receiver is started or restarted, as then says.
func receiverBundleNext(out io.Writer, did, bundle, then string) error {
	_, err := fmt.Fprintf(out, "%s %s; hand it to the receiver's platform, whose install guide's bundle step installs it, then %s the receiver\n", did, bundle, then)
	return err
}

// writeLeftovers prints one line per leftover topic with its owner and retained offsets, marking the topics readers
// still need, then a summary, or nothing when there are none.
func writeLeftovers(out io.Writer, leftovers []operator.Leftover) error {
	var offsets int64
	var b strings.Builder
	for _, leftover := range leftovers {
		owner := "tenant=" + leftover.Tenant
		switch {
		case leftover.Pool != "":
			owner = "pool=" + leftover.Pool + " unattributed"
		case leftover.Receiver != "":
			owner = "receiver=" + leftover.Receiver
		case leftover.Host != "":
			owner += " host=" + leftover.Host
		}
		fmt.Fprintf(&b, "leftover %s %s offsets=%d", leftover.Topic, owner, leftover.Offsets)
		if leftover.Kept {
			fmt.Fprint(&b, " kept: holds keys")
		}
		fmt.Fprintln(&b)
		offsets += leftover.Offsets
	}
	if len(leftovers) > 0 {
		fmt.Fprintf(&b, "leftovers: %d topics, %d offsets\n", len(leftovers), offsets)
	}
	_, err := io.WriteString(out, b.String())
	return err
}

// revocationProbe is the probe a remove runs: the service's, which asks every broker as the principal. A variable so
// tests on a fake broker, whose authorisation answers differ from Kafka's, can decide the probe's outcome.
var revocationProbe = func(service *operator.Service, topic string) func(context.Context, string, string) error {
	return service.Probe(topic)
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

// receiverLiveness is a receiver's status and the time of its latest authorised advertisement, which the operator reads
// as a host does, verifying the receiver's signature against its key topic.
type receiverLiveness struct {
	Status         string    `yaml:"status"`
	LastAdvertised time.Time `yaml:"last_advertised,omitempty"`
}

func readReceiverLiveness(ctx context.Context, service *operator.Service, state operator.State, receiver string) (*receiverLiveness, error) {
	pool := state.Receivers[receiver].Pool
	reader := operator.NewReader(service.Operator)
	defer reader.Close()
	keys, err := reader.ReadKeys(ctx, operator.PoolKeyTopics(state, pool))
	if err != nil {
		return nil, err
	}
	now := time.Now()
	// Every age, so a receiver that stopped advertising shows as stale with the time it was last heard, not as silent.
	ads, err := reader.ReadAdvertisements(ctx, state, keys, []string{pool}, now, math.MaxInt64)
	if err != nil {
		return nil, err
	}
	liveness := &receiverLiveness{Status: operator.ReceiverStatus(state, ads, receiver, now)}
	if ad, advertised := ads[receiver]; advertised && liveness.Status != "ended" {
		liveness.LastAdvertised = time.UnixMilli(ad.AtMS).UTC()
	}
	return liveness, nil
}

// showReceiver writes the receiver's liveness, when given, then its resolved profiles and plugin pins as YAML. A dry run
// gives none: the record it would hold has no receiver to be live.
func showReceiver(out io.Writer, config operator.ReceiverConfig, liveness *receiverLiveness) error {
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
		*receiverLiveness `yaml:",inline"`
		PluginSHA256      map[string]string `yaml:"plugin_sha256,omitempty"`
		Profiles          []shown           `yaml:"profiles"`
	}{liveness, config.PluginSHA256, profiles})
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
	for _, name := range []string{"file", "timeout", "dir", "broker", "super-user", "super-user-password", "super-user-mechanism", "ca-cert", "window", "port", "external-url"} {
		flags.String(name, "", "")
	}
	var suspects []string
	flags.Func("suspect-key", "", func(value string) error { suspects = append(suspects, value); return nil })
	flags.Bool("dry-run", false, "")
	flags.Bool("force", false, "")
	// A resource flag names one object, so one given twice or empty fits no form.
	resources := map[string]string{}
	malformed := false
	for _, name := range operatorResources {
		flags.Func(name, "", func(value string) error {
			if _, repeated := resources[name]; repeated || value == "" {
				malformed = true
			}
			resources[name] = value
			return nil
		})
	}
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
	if len(positional) == 0 {
		return positional, options, nil
	}
	verb := positional[0]
	forms, known := operatorForms[verb]
	if !known {
		return positional, options, nil
	}
	given := slices.Sorted(maps.Keys(resources))
	at := slices.IndexFunc(forms, func(form operatorForm) bool { return slices.Equal(form.resources, given) })
	if malformed || at < 0 || (verb == "list" && len(given) > 0 && (len(positional) < 2 || positional[1] != "hosts")) {
		return positional, nil, errors.New(operatorUsage[verb])
	}
	for _, name := range slices.Sorted(maps.Keys(options)) {
		if !slices.Contains(forms[at].options, name) && !slices.Contains(given, name) && name != "dir" {
			return nil, nil, fmt.Errorf("unknown option --%s", name)
		}
	}
	for name, value := range resources {
		options[name] = value
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

// superUserKafka connects to broker as the Kafka super user with password and mechanism, verifying the broker against
// roots.
var superUserKafka = func(broker, user, password, mechanism string, roots *x509.CertPool) (*kgo.Client, error) {
	opts, err := operator.SuperUserKafkaOpts(broker, user, password, mechanism, roots)
	if err != nil {
		return nil, fmt.Errorf("broker %w", err)
	}
	return kgo.NewClient(append(opts, kgo.FetchIsolationLevel(kgo.ReadCommitted()))...)
}

// operatorKafka connects to broker as user with password, verifying the broker against roots.
var operatorKafka = func(broker, user, password string, roots *x509.CertPool) (*kgo.Client, error) {
	opts, err := operator.KafkaOpts(broker, user, password, roots)
	if err != nil {
		return nil, fmt.Errorf("broker %w", err)
	}
	return kgo.NewClient(append(opts, kgo.FetchIsolationLevel(kgo.ReadCommitted()))...)
}
