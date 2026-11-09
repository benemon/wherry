package main

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/wire"
	"github.com/charmbracelet/huh"
	"gopkg.in/yaml.v3"
)

// operatorConfig is a working directory's operator.yaml: how its commands connect as wherry.operator. CAFile is empty
// for the system roots; a relative PasswordFile or CAFile is in the working directory.
type operatorConfig struct {
	Broker       string `yaml:"broker"`
	CAFile       string `yaml:"ca_file,omitempty"`
	PasswordFile string `yaml:"password_file"`
}

// operatorInit, connected as the Kafka super user the flags or prompts name, delegates provisioning to wherry.operator
// with a new password and writes dir's operator.yaml. The super user is written nowhere.
func operatorInit(ctx context.Context, dir string, options map[string]string, prompts io.Writer) (result error) {
	defer func() {
		if result != nil {
			result = presentationError{result}
		}
	}()
	required := []string{"broker", "super-user", "super-user-password-file"}
	onTerminal := promptsEligible(prompts)
	if !onTerminal {
		var missing []string
		for _, name := range required {
			if options[name] == "" {
				missing = append(missing, "--"+name)
			}
		}
		if len(missing) > 0 {
			return fmt.Errorf("operator init has no terminal to prompt on; give %s", strings.Join(missing, ", "))
		}
	}
	answers := map[string]string{}
	q := &questions{ctx, stdin, &promptLatch{out: prompts}}
	heading := false
	passwordPath := ""
	for _, question := range [][2]string{
		{"broker", "Kafka broker address"},
		{"ca-file", "CA certificate"},
		{"super-user", "Kafka super user"},
		{"super-user-password-file", "Kafka super user's password file"},
	} {
		value, given := options[question[0]]
		if !given && onTerminal {
			if !heading {
				if err := q.line("\nBroker\n"); err != nil {
					return err
				}
				heading = true
			}
			var err error
			if question[0] == "ca-file" {
				value, err = q.ca()
			} else {
				kind := ""
				if question[0] == "super-user-password-file" {
					kind = "path"
				}
				value, err = q.ask(question[1], "", "", kind, func(s string) error {
					if s == "" {
						return fmt.Errorf("%s is required", question[1])
					}
					if question[0] == "broker" {
						return checkOperatorBroker(s)
					}
					if question[0] == "super-user-password-file" {
						var err error
						passwordPath, err = readablePath(s)
						return err
					}
					return nil
				})
			}
			if err != nil {
				return err
			}
			if question[0] == "super-user-password-file" {
				value = passwordPath
			}
		}
		answers[question[0]] = value
	}
	for _, name := range required {
		if answers[name] == "" {
			return fmt.Errorf("operator init needs --%s", name)
		}
	}
	if err := checkOperatorBroker(answers["broker"]); err != nil {
		return err
	}
	var ca []byte
	caFile := answers["ca-file"]
	if caFile != "" {
		var err error
		if caFile, err = filepath.Abs(caFile); err != nil {
			return err
		}
		if ca, err = loadCA(caFile); err != nil {
			return err
		}
	}
	superPassword, err := loadPassword(answers["super-user-password-file"])
	if err != nil {
		return err
	}
	superUser, err := operatorKafka(answers["broker"], ca, answers["super-user"], string(superPassword))
	if err != nil {
		return err
	}
	defer superUser.Close()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	password, err := operator.Password()
	if err != nil {
		return err
	}
	// operator.password keeps the old password until the broker holds the new one; a failure between the two is
	// recovered by running init again.
	temp, err := os.CreateTemp(dir, ".operator.password.*")
	if err != nil {
		return err
	}
	_, err = temp.WriteString(password + "\n")
	if closeErr := temp.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = operator.Delegate(ctx, superUser, password)
	}
	if err == nil {
		if err = (&operator.Service{Operator: superUser, Broker: answers["broker"], CA: ca}).WaitOperatorCredential(ctx, password); err != nil {
			err = fmt.Errorf("%w; run operator init again", err)
		}
	}
	if err == nil {
		err = os.Rename(temp.Name(), filepath.Join(dir, "operator.password"))
	}
	if err != nil {
		os.Remove(temp.Name())
		return err
	}
	if err := writeYAML(filepath.Join(dir, "operator.yaml"), operatorConfig{answers["broker"], caFile, "operator.password"}); err != nil {
		return err
	}
	if err := ignore(filepath.Join(dir, ".gitignore"), "operator.password", "bundles/"); err != nil {
		return err
	}
	fmt.Fprintf(prompts, "%s can provision; its password is in %s\n", operator.OperatorPrincipal, filepath.Join(dir, "operator.password"))
	return nil
}

func checkOperatorBroker(s string) error {
	invalid := func() error { return fmt.Errorf("%s is not a valid Kafka broker address", s) }
	if s == "" {
		return invalid()
	}
	host, port, err := net.SplitHostPort(s)
	if err == nil {
		n, err := strconv.ParseUint(port, 10, 16)
		if host == "" || err != nil || n == 0 {
			return invalid()
		}
		return nil
	}
	if strings.HasPrefix(s, "[") {
		if i := strings.Index(s, "]"); i >= 0 && i+1 < len(s) {
			return invalid()
		}
	}
	return nil
}

// ignore appends each of lines that path lacks to it, creating it if absent.
func ignore(path string, lines ...string) error {
	existing, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	present := strings.Split(string(existing), "\n")
	var add []byte
	for _, line := range lines {
		if !slices.Contains(present, line) {
			add = append(add, line+"\n"...)
		}
	}
	if len(add) == 0 {
		return nil
	}
	if len(existing) > 0 && !bytes.HasSuffix(existing, []byte("\n")) {
		add = append([]byte("\n"), add...)
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	_, err = file.Write(add)
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	return err
}

// operatorService connects as wherry.operator through dir's operator.yaml.
func operatorService(dir string) (*operator.Service, error) {
	path := filepath.Join(dir, "operator.yaml")
	var config operatorConfig
	if err := readYAML(path, &config); errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%s does not exist; run wherry operator init", path)
	} else if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if config.Broker == "" || config.PasswordFile == "" {
		return nil, fmt.Errorf("%s requires broker and password_file", path)
	}
	var ca []byte
	if config.CAFile != "" {
		var err error
		if ca, err = os.ReadFile(inDir(dir, config.CAFile)); err != nil {
			return nil, err
		}
	}
	password, err := os.ReadFile(inDir(dir, config.PasswordFile))
	if err != nil {
		return nil, err
	}
	client, err := operatorKafka(config.Broker, ca, operator.OperatorPrincipal, string(password))
	if err != nil {
		return nil, err
	}
	return &operator.Service{Operator: client, Broker: config.Broker, CA: ca}, nil
}

func inDir(dir, path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(dir, path)
}

// bundlePath is the bundle file under dir's bundles/kind named by names, the last the file's, each a directory but the
// last. With create it makes the directories mode 0700, existing ones too: bundles hold passwords. A dry run passes
// false, since it writes nothing.
func bundlePath(dir, kind string, create bool, names ...string) (string, error) {
	for _, name := range names {
		if !wire.ValidName(name) {
			return "", fmt.Errorf("%q is not a valid name", name)
		}
	}
	bundles := filepath.Join(dir, "bundles")
	path := filepath.Join(append([]string{bundles, kind}, names[:len(names)-1]...)...)
	if !create {
		return filepath.Join(path, names[len(names)-1]+".bundle"), nil
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return "", err
	}
	for sub := path; ; sub = filepath.Dir(sub) {
		if err := os.Chmod(sub, 0o700); err != nil {
			return "", err
		}
		if sub == bundles {
			break
		}
	}
	return filepath.Join(path, names[len(names)-1]+".bundle"), nil
}

// rebroker rewrites bundle, read from path, with its password when it names a broker other than broker.
func rebroker(path string, bundle operator.Bundle, broker string) error {
	if bundle.Broker == broker {
		return nil
	}
	bundle.Broker = broker
	return operator.ReplaceBundle(path, bundle)
}

// objectFile is the file apply reads for name and whether it is a directory: -f's, else dir's kind/name.yaml, else
// that file written from prompt's answers. Without a terminal to prompt on, it refuses, naming fields; so does a dry run,
// which writes nothing.
func objectFile(ctx context.Context, dir, kind, name string, options map[string]string, dryRun bool, prompts io.Writer, fields string, prompt func(*questions) (any, error)) (string, bool, error) {
	path := options["f"]
	if path == "" {
		if !wire.ValidName(name) {
			return "", false, fmt.Errorf("%q is not a valid name", name)
		}
		path = filepath.Join(dir, kind, name+".yaml")
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			if dryRun {
				return "", false, fmt.Errorf("%s does not exist and --dry-run writes nothing; write it, or give -f FILE, with %s", path, fields)
			}
			if !promptsEligible(prompts) {
				return "", false, fmt.Errorf("%s does not exist and there is no terminal to prompt on; write it, or give -f FILE, with %s", path, fields)
			}
			value, err := prompt(&questions{ctx, stdin, &promptLatch{out: prompts}})
			if err != nil {
				return "", false, presentationError{err}
			}
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				return "", false, err
			}
			if err := writeYAML(path, value); err != nil {
				return "", false, err
			}
			fmt.Fprintf(prompts, "wrote %s\n", path)
		}
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", false, err
	}
	return path, info.IsDir(), nil
}

// poolFields are what a pool file gives without a terminal, as pool apply's prompts ask for them.
const poolFields = "agents (each with its name, models and credentials), default_agent, producer_byte_rate, request_percentage and start_by_limit"

type poolFile struct {
	Agents            []poolAgent `yaml:"agents"`
	DefaultAgent      string      `yaml:"default_agent"`
	ProducerByteRate  int64       `yaml:"producer_byte_rate"`
	RequestPercentage float64     `yaml:"request_percentage"`
	StartByLimit      string      `yaml:"start_by_limit"`
}

type poolAgent struct {
	Name        string           `yaml:"name"`
	Models      []string         `yaml:"models,omitempty"`
	Credentials []map[string]any `yaml:"credentials"`
}

func promptPool(q *questions) (any, error) {
	var file poolFile
	validName := func(s string) error {
		if !wire.ValidName(s) {
			return errors.New("invalid name")
		}
		return nil
	}
	if err := q.line("\nAgents\n"); err != nil {
		return nil, err
	}
	for {
		name, err := q.ask("Agent name", "", "", "", validName)
		if err != nil {
			return nil, err
		}
		agent := poolAgent{Name: name, Credentials: []map[string]any{}}
		for {
			model, err := q.input("Model (blank ends the list)", "Leave blank for the profiles' choice", "", "", func(string) error { return nil })
			if err != nil {
				return nil, err
			}
			if model == "" {
				break
			}
			agent.Models = append(agent.Models, model)
		}
		models := strings.Join(agent.Models, ", ")
		if models == "" {
			models = "none"
		}
		if err := q.answer("Models of "+name, models); err != nil {
			return nil, err
		}
		var targets []wire.TaskCredential
		for {
			choice := "add"
			if _, err := q.pick("Credentials of "+name, "", []huh.Option[string]{option("Add credential...", "add"), option("Done", "done")}, &choice); err != nil {
				return nil, err
			}
			if choice == "done" {
				break
			}
			credentialName, err := q.input("Credential name", "", "", "", func(s string) error {
				if err := validName(s); err != nil {
					return err
				}
				for _, c := range targets {
					if c.Name == s {
						return errors.New("duplicate credential name")
					}
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
			kind := "env"
			if _, err := q.pick("Delivered as", "", []huh.Option[string]{option("Environment variable", "env"), option("File", "file")}, &kind); err != nil {
				return nil, err
			}
			label, delivery := "Variable", "Environment variable"
			if kind == "file" {
				label, delivery = "File name", "File"
			}
			candidate := wire.TaskCredential{Name: credentialName}
			target, err := q.input(label, "", "", "", func(s string) error {
				if kind == "env" {
					candidate.Env = s
				} else {
					candidate.File = s
				}
				return wire.ValidateCredentials(append(slices.Clone(targets), candidate))
			})
			if err != nil {
				return nil, err
			}
			required := "required"
			if _, err := q.pick("Required", "", []huh.Option[string]{option("Required", "required"), option("Optional", "optional")}, &required); err != nil {
				return nil, err
			}
			description := ""
			descriptionInput := huh.NewInput().Title("Description (blank for none)").Value(&description).Validate(operator.ValidateDescription)
			if err := q.run(descriptionInput); err != nil {
				return nil, err
			}
			description = strings.TrimSpace(description)
			credential := map[string]any{"name": credentialName, kind: target}
			if required == "optional" {
				credential["required"] = false
			}
			answer := delivery + " " + display(target) + ", " + required
			if description != "" {
				credential["description"] = description
				answer += ", \"" + display(description) + "\""
			}
			if err := q.answer("Credential "+credentialName, answer); err != nil {
				return nil, err
			}
			targets = append(targets, candidate)
			agent.Credentials = append(agent.Credentials, credential)
		}
		file.Agents = append(file.Agents, agent)
		another := "add"
		if _, err := q.pick("Another agent", "", []huh.Option[string]{option("Add agent...", "add"), option("Done", "done")}, &another); err != nil {
			return nil, err
		}
		if another == "done" {
			break
		}
	}
	if err := q.line("\nPool\n"); err != nil {
		return nil, err
	}
	options := make([]huh.Option[string], 0, len(file.Agents))
	for _, agent := range file.Agents {
		options = append(options, option(agent.Name, agent.Name))
	}
	file.DefaultAgent = file.Agents[0].Name
	if _, err := q.pick("Default agent", "", options, &file.DefaultAgent); err != nil {
		return nil, err
	}
	if err := q.answer("Default agent", file.DefaultAgent); err != nil {
		return nil, err
	}
	_, err := q.ask("Producer byte rate per host, bytes a second", "", "1048576", "", func(s string) error {
		value, err := strconv.ParseInt(s, 10, 64)
		if err != nil || value <= 0 {
			return errors.New("producer byte rate must be greater than 0")
		}
		file.ProducerByteRate = value
		return nil
	})
	if err != nil {
		return nil, err
	}
	_, err = q.ask("Request percentage per host", "", "10", "", func(s string) error {
		value, err := strconv.ParseFloat(s, 64)
		if err != nil || !(value > 0) {
			return errors.New("request percentage must be greater than 0")
		}
		file.RequestPercentage = value
		return nil
	})
	if err != nil {
		return nil, err
	}
	file.StartByLimit, err = q.ask("Start-by limit", "", (time.Duration(operator.DefaultStartByLimitMS) * time.Millisecond).String(), "", func(s string) error {
		value, err := time.ParseDuration(s)
		if err != nil || value < 0 {
			return errors.New("start-by limit must be a non-negative duration")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return file, nil
}

// receiverFields are what a receiver file gives without a terminal, as receiver apply's prompts ask for them.
const receiverFields = "pool, plugin_sha256, plugins (each with its config and profiles, a profile with its agent, labels and optional replicas) and broker, which defaults to the operator's"

type receiverFile struct {
	Pool         string                    `yaml:"pool"`
	Broker       string                    `yaml:"broker"`
	PluginSHA256 map[string]string         `yaml:"plugin_sha256"`
	Plugins      map[string]receiverPlugin `yaml:"plugins"`
}

// receiverPools are the recorded pools, sorted, with receiver's own first when it exists.
func receiverPools(state operator.State, receiver string) []string {
	pools := slices.Sorted(maps.Keys(state.Pools))
	if current, ok := state.Receivers[receiver]; ok {
		pools = append([]string{current.Pool}, slices.DeleteFunc(pools, func(pool string) bool { return pool == current.Pool })...)
	}
	return pools
}

// receiverAgents are terms' valid agent names in terms order, each once.
func receiverAgents(terms operator.Terms) []string {
	var agents []string
	for _, agent := range terms.Agents {
		if wire.ValidName(agent.Name) && !slices.Contains(agents, agent.Name) {
			agents = append(agents, agent.Name)
		}
	}
	return agents
}

// receiverConfigAnswer is a plugin config as the field takes it, refusing what reading the written file would refuse.
func receiverConfigAnswer(kind, answer string) (map[string]any, error) {
	var config map[string]any
	if err := yaml.Unmarshal([]byte(answer), &config); err != nil {
		return nil, fmt.Errorf("plugin config is not a YAML mapping: %w", err)
	}
	if path := nullAt(config, ""); path != "" {
		return nil, fmt.Errorf("plugin %s sets config key %s to null", kind, path)
	}
	if name, ok := config["credential"]; ok {
		if value, ok := name.(string); !ok {
			return nil, errors.New("credential is not a string")
		} else if !wire.ValidName(value) {
			return nil, fmt.Errorf("credential %q is not a valid name", value)
		}
	}
	return config, nil
}

func promptReceiver(q *questions, receiver string, state operator.State, broker string) (any, error) {
	file := receiverFile{PluginSHA256: map[string]string{}, Plugins: map[string]receiverPlugin{}}
	validName := func(s string) error {
		if !wire.ValidName(s) {
			return errors.New("invalid name")
		}
		return nil
	}
	// Other's value is no valid name, and not empty: huh draws a pick holding an empty value without its options.
	const otherValue = "Other..."
	choose := func(label, other, otherLabel string, names []string, validate func(string) error) (string, error) {
		options := make([]huh.Option[string], 0, len(names)+1)
		for _, name := range names {
			options = append(options, option(name, name))
		}
		value := otherValue
		if len(names) > 0 {
			value = names[0]
		}
		field := huh.NewSelect[string]().Title(display(label)).Options(append(options, option(other, otherValue))...).Value(&value).Validate(func(s string) error {
			if s == otherValue {
				return nil
			}
			if err := validate(s); err != nil {
				return errors.New(display(err.Error()))
			}
			return nil
		})
		if err := q.run(field); err != nil {
			return "", err
		}
		if value == otherValue {
			typed, err := q.input(otherLabel, "", "", "", validate)
			if err != nil {
				return "", err
			}
			value = typed
		}
		return value, q.answer(label, value)
	}
	if err := q.line("\nReceiver\n"); err != nil {
		return nil, err
	}
	var err error
	if file.Pool, err = choose("Pool", "Other pool...", "Pool name", receiverPools(state, receiver), validName); err != nil {
		return nil, err
	}
	// nomad, kubernetes and systemd are the plugin types wherry-plugins ships.
	kind, err := choose("Plugin type", "Other type...", "Plugin type name", []string{"nomad", "kubernetes", "systemd"}, validName)
	if err != nil {
		return nil, err
	}
	shaLabel := fmt.Sprintf("SHA-256 of the %s plugin binary", kind)
	sum, err := q.ask(shaLabel, "The line for the binary in the release's signed checksums.txt", "", "", func(s string) error {
		if !operator.ValidPluginSHA256(s) {
			return errors.New("SHA-256 must be 64 lowercase hex digits")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	file.PluginSHA256[kind] = sum
	var plugin receiverPlugin
	configLabel := fmt.Sprintf("%s plugin config, as a YAML flow mapping", kind)
	answer, err := q.input(configLabel, "", "{}", "", func(s string) error {
		var err error
		plugin.Config, err = receiverConfigAnswer(kind, s)
		return err
	})
	if err != nil {
		return nil, err
	}
	if err := q.answer(configLabel, cmp.Or(answer, "{}")); err != nil {
		return nil, err
	}
	if err := q.line("\nProfiles\n"); err != nil {
		return nil, err
	}
	agents := receiverAgents(state.Pools[file.Pool])
	for {
		agent, err := choose("Agent", "Other agent...", "Agent name", agents, func(s string) error {
			if err := validName(s); err != nil {
				return err
			}
			if slices.ContainsFunc(plugin.Profiles, func(p receiverProfile) bool { return p.Name == s }) {
				return fmt.Errorf("agent %s already has a profile", s)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		profile := receiverProfile{Name: agent, Agent: agent, Labels: map[string]*string{}}
		var entered []string
		for {
			label, err := q.input("Label KEY=VALUE (blank ends the list)", "", "", "", func(s string) error {
				if s == "" {
					return nil
				}
				key, value, ok := strings.Cut(s, "=")
				if !ok {
					return fmt.Errorf("label %q needs the form KEY=VALUE", s)
				}
				if _, ok := profile.Labels[key]; ok {
					return errors.New("duplicate label key")
				}
				return wire.ValidateLabels(map[string]string{key: value})
			})
			if err != nil {
				return nil, err
			}
			if label == "" {
				break
			}
			key, value, _ := strings.Cut(label, "=")
			profile.Labels[key] = &value
			entered = append(entered, label)
		}
		if err := q.answer("Labels of "+agent, cmp.Or(strings.Join(entered, ", "), "none")); err != nil {
			return nil, err
		}
		plugin.Profiles = append(plugin.Profiles, profile)
		another := "add"
		if _, err := q.pick("Another profile", "", []huh.Option[string]{option("Add profile...", "add"), option("Done", "done")}, &another); err != nil {
			return nil, err
		}
		if another == "done" {
			break
		}
	}
	file.Plugins[kind] = plugin
	if err := q.line("\nBroker\n"); err != nil {
		return nil, err
	}
	brokerLabel := "Broker address the receiver connects to"
	if file.Broker, err = q.ask(brokerLabel, "", broker, "", checkOperatorBroker); err != nil {
		return nil, err
	}
	return file, nil
}
