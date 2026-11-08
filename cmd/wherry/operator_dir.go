package main

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
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

// terminal reports whether the prompts, which read stdin, have a terminal.
var terminal = func() bool { return isTerminal(os.Stdin) }

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
				value, err = q.input(question[1], "", kind, func(s string) error {
					s = strings.TrimSpace(s)
					if s == "" {
						return fmt.Errorf("%s is required", question[1])
					}
					if question[0] == "broker" {
						return checkOperatorBroker(s)
					}
					if question[0] == "super-user-password-file" {
						_, err := readablePath(s)
						return err
					}
					return nil
				})
				value = strings.TrimSpace(value)
				if err == nil {
					err = q.answer(question[1], value)
				}
			}
			if err != nil {
				return err
			}
			if question[0] == "super-user-password-file" {
				value, err = localPath(value)
				if err != nil {
					return err
				}
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

type prompter struct {
	in  *bufio.Reader
	out io.Writer
}

func newPrompter(out io.Writer) *prompter { return &prompter{bufio.NewReader(stdin), out} }

// ask writes question with its default, value, and returns the answer, or value for a blank one.
func (p *prompter) ask(question, value string) (string, error) {
	if value != "" {
		fmt.Fprintf(p.out, "%s [%s]: ", question, value)
	} else {
		fmt.Fprintf(p.out, "%s: ", question)
	}
	line, err := p.in.ReadString('\n')
	if err != nil && line == "" {
		return "", fmt.Errorf("%s: %w", question, err)
	}
	return cmp.Or(strings.TrimSpace(line), value), nil
}

// yes writes question and reports whether the answer is y or yes; anything else is No.
func (p *prompter) yes(question string) (bool, error) {
	fmt.Fprint(p.out, question)
	line, err := p.in.ReadString('\n')
	if err != nil && line == "" {
		return false, err
	}
	answer := strings.ToLower(strings.TrimSpace(line))
	return answer == "y" || answer == "yes", nil
}

// list is ask's answer split at commas, without blanks.
func (p *prompter) list(question, value string) ([]string, error) {
	answer, err := p.ask(question, value)
	if err != nil {
		return nil, err
	}
	var items []string
	for item := range strings.SplitSeq(answer, ",") {
		if item = strings.TrimSpace(item); item != "" {
			items = append(items, item)
		}
	}
	return items, nil
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
	input := func(label, initial string, validate func(string) error) (string, error) {
		value, err := q.input(label, initial, "", validate)
		value = strings.TrimSpace(value)
		if err == nil {
			err = q.answer(label, value)
		}
		return value, err
	}
	validName := func(s string) error {
		if !wire.ValidName(strings.TrimSpace(s)) {
			return errors.New("invalid name")
		}
		return nil
	}
	if err := q.line("\nAgents\n"); err != nil {
		return nil, err
	}
	for {
		name, err := input("Agent name", "", validName)
		if err != nil {
			return nil, err
		}
		agent := poolAgent{Name: name, Credentials: []map[string]any{}}
		for {
			model := ""
			field := huh.NewInput().Title("Model (blank ends the list)").Description("Leave blank for the profiles' choice").Value(&model)
			if err := q.run(field); err != nil {
				return nil, err
			}
			model = strings.TrimSpace(model)
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
			credentialName, err := q.input("Credential name", "", "", func(s string) error {
				if err := validName(s); err != nil {
					return err
				}
				for _, c := range targets {
					if c.Name == strings.TrimSpace(s) {
						return errors.New("duplicate credential name")
					}
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
			credentialName = strings.TrimSpace(credentialName)
			kind := "env"
			if _, err := q.pick("Delivered as", "", []huh.Option[string]{option("Environment variable", "env"), option("File", "file")}, &kind); err != nil {
				return nil, err
			}
			label, delivery := "Variable", "Environment variable"
			if kind == "file" {
				label, delivery = "File name", "File"
			}
			candidate := wire.TaskCredential{Name: credentialName}
			target, err := q.input(label, "", "", func(s string) error {
				if kind == "env" {
					candidate.Env = strings.TrimSpace(s)
				} else {
					candidate.File = strings.TrimSpace(s)
				}
				return wire.ValidateCredentials(append(slices.Clone(targets), candidate))
			})
			if err != nil {
				return nil, err
			}
			target = strings.TrimSpace(target)
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
			if err := q.line("? Credential %s  %s\n", display(credentialName), answer); err != nil {
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
	_, err := input("Producer byte rate per host, bytes a second", "1048576", func(s string) error {
		value, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
		if err != nil || value <= 0 {
			return errors.New("producer byte rate must be greater than 0")
		}
		file.ProducerByteRate = value
		return nil
	})
	if err != nil {
		return nil, err
	}
	_, err = input("Request percentage per host", "10", func(s string) error {
		value, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
		if err != nil || !(value > 0) {
			return errors.New("request percentage must be greater than 0")
		}
		file.RequestPercentage = value
		return nil
	})
	if err != nil {
		return nil, err
	}
	file.StartByLimit, err = input("Start-by limit", (time.Duration(operator.DefaultStartByLimitMS) * time.Millisecond).String(), func(s string) error {
		value, err := time.ParseDuration(strings.TrimSpace(s))
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

func promptReceiver(p *prompter, broker string) (any, error) {
	file := receiverFile{PluginSHA256: map[string]string{}, Plugins: map[string]receiverPlugin{}}
	var err error
	if file.Pool, err = p.ask("Pool", ""); err != nil {
		return nil, err
	}
	kind, err := p.ask("Plugin type", "nomad")
	if err != nil {
		return nil, err
	}
	if file.PluginSHA256[kind], err = p.ask(fmt.Sprintf("SHA-256 of the %s plugin binary", kind), ""); err != nil {
		return nil, err
	}
	answer, err := p.ask(fmt.Sprintf("%s plugin config, as a YAML flow mapping", kind), "{}")
	if err != nil {
		return nil, err
	}
	var plugin receiverPlugin
	if err := yaml.Unmarshal([]byte(answer), &plugin.Config); err != nil {
		return nil, fmt.Errorf("plugin config is not a YAML mapping: %w", err)
	}
	agents, err := p.list("Agents to run, comma-separated (one profile each, named after its agent)", "")
	if err != nil {
		return nil, err
	}
	for _, agent := range agents {
		profile := receiverProfile{Name: agent, Agent: agent, Labels: map[string]*string{}}
		labels, err := p.list(fmt.Sprintf("Labels of profile %s, as KEY=VALUE, comma-separated", agent), "")
		if err != nil {
			return nil, err
		}
		for _, label := range labels {
			key, value, ok := strings.Cut(label, "=")
			if !ok {
				return nil, fmt.Errorf("label %q needs the form KEY=VALUE", label)
			}
			profile.Labels[key] = &value
		}
		plugin.Profiles = append(plugin.Profiles, profile)
	}
	file.Plugins[kind] = plugin
	if file.Broker, err = p.ask("Broker address the receiver connects to", broker); err != nil {
		return nil, err
	}
	return file, nil
}
