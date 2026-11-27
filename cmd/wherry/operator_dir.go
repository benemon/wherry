package main

import (
	"bytes"
	"cmp"
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/benemon/wherry/internal/catalog"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/telemetry"
	"github.com/benemon/wherry/internal/wire"
	"github.com/charmbracelet/huh"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"gopkg.in/yaml.v3"
)

// operatorConfig is a working directory's operator.yaml: how its commands connect as wherry.operator; a relative
// PasswordFile is in the working directory. CACert is the PEM of the CA the connection trusts beside the machine's trust
// store; empty for the trust store alone. Tracing writes each command's span to the trace topics; off by default.
type operatorConfig struct {
	Broker       string `yaml:"broker"`
	PasswordFile string `yaml:"password_file"`
	CACert       string `yaml:"ca_cert,omitempty"`
	Tracing      bool   `yaml:"tracing,omitempty"`
}

// operatorInit, connected as the Kafka super user the flags or prompts name, delegates provisioning to wherry.operator
// with a new password and writes dir's operator.yaml. The super user is written nowhere.
func operatorInit(ctx context.Context, dir string, options map[string]string, prompts io.Writer) (result error) {
	// caCert is the PEM operator.yaml gets: --ca-cert's, else the existing operator.yaml's, else none; the hint on an
	// unknown authority names the file init writes and says whether it holds one.
	caCert := ""
	defer func() {
		if result != nil {
			result = presentationError{operator.ExplainUnknownAuthority(result, operator.CASetting{File: filepath.Join(dir, "operator.yaml"), Set: caCert != ""})}
		}
	}()
	var err error
	required := []string{"broker", "super-user", "super-user-password"}
	mechanism := operator.ScramSHA512
	if value, given := options["super-user-mechanism"]; given {
		// The refusal leaves out the value, which may be a password given to the wrong flag.
		if value != operator.ScramSHA512 && value != operator.ScramSHA256 {
			return fmt.Errorf("--super-user-mechanism takes %s or %s", operator.ScramSHA512, operator.ScramSHA256)
		}
		mechanism = value
	}
	// The password reaches init as @FILE or @-, never as the flag's own text, which every user of the machine can read
	// from the process list; the refusal leaves the value out. An empty value is no value, as for every flag.
	if value, given := options["super-user-password"]; given && value != "" && !strings.HasPrefix(value, "@") {
		return errors.New("--super-user-password takes @FILE or @-; a password on the command line is visible to other users")
	}
	// `@-` reads the password from standard input; checked before any prompt. One flag at most reads standard input.
	if options["super-user-password"] == "@-" {
		if options["ca-cert"] == "@-" {
			return errors.New("--super-user-password @- and --ca-cert @- both read standard input; give one of them a file")
		}
		if err := terminalStandardInput("super-user-password", "password"); err != nil {
			return err
		}
	}
	// The CA is resolved first, so a value Go reads no certificate from fails before any prompt or dial: --ca-cert's, or
	// without the flag the existing operator.yaml's, kept as host init keeps one, so a re-run dials as the last run did
	// and writes the key back.
	var roots *x509.CertPool
	if value, given := options["ca-cert"]; given {
		if caCert, roots, err = caCertFlag(value); err != nil {
			return err
		}
	} else {
		if caCert, err = existingCACert(dir); err != nil {
			return err
		}
		if roots, err = operator.ConfigRootPool(filepath.Join(dir, "operator.yaml"), caCert); err != nil {
			return err
		}
	}
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
	// superPassword is the prompt's answer; nil when a flag names a file or standard input.
	var superPassword []byte
	for _, question := range [][2]string{
		{"broker", "Kafka broker address"},
		{"super-user", "Kafka super user"},
		{"super-user-password", "Kafka super user's password"},
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
			switch question[0] {
			case "super-user-password":
				// The password itself, masked and never printed back; it is kept as typed, so a password with spaces at
				// either end is still that password.
				var password string
				password, err = q.secret(question[1], func(s string) error {
					if s == "" {
						return fmt.Errorf("%s is required", question[1])
					}
					return nil
				})
				superPassword = []byte(password)
			default:
				value, err = q.ask(question[1], "", "", "", func(s string) error {
					if s == "" {
						return fmt.Errorf("%s is required", question[1])
					}
					if question[0] == "broker" {
						return checkOperatorBroker(s)
					}
					return nil
				})
			}
			if err != nil {
				return err
			}
		}
		answers[question[0]] = value
	}
	for _, name := range required {
		if answers[name] == "" && !(name == "super-user-password" && superPassword != nil) {
			return fmt.Errorf("operator init needs --%s", name)
		}
	}
	if err := checkOperatorBroker(answers["broker"]); err != nil {
		return err
	}
	if superPassword == nil {
		// @FILE is opened as a password file: a regular file, without blocking. Its contents are the password as they are,
		// but for the trailing line break the connection trims; standard input that holds none is refused naming no value.
		text, err := flagValue("--super-user-password", answers["super-user-password"], openPasswordFile, stdin)
		if err != nil {
			return err
		}
		if answers["super-user-password"] == "@-" && strings.TrimRight(text, "\r\n") == "" {
			return errors.New("standard input held no password")
		}
		superPassword = []byte(text)
	}
	superUser, err := superUserKafka(answers["broker"], answers["super-user"], string(superPassword), mechanism, roots)
	if err != nil {
		return err
	}
	defer superUser.Close()
	// Kafka answers a missing credential for the mechanism as it answers a wrong password; checked before Delegate, whose
	// first error would name wherry.operator.
	if _, err := kadm.NewClient(superUser).ListBrokers(ctx); errors.Is(err, kerr.SaslAuthenticationFailed) {
		hint := ""
		if mechanism == operator.ScramSHA512 {
			hint = "; if its credential is " + operator.ScramSHA256 + ", give --super-user-mechanism " + operator.ScramSHA256
		}
		return fmt.Errorf("super user %s's %s login refused: Kafka answers a wrong password and no %s credential alike%s", answers["super-user"], mechanism, mechanism, hint)
	} else if err != nil {
		return fmt.Errorf("super user %s: %w", answers["super-user"], err)
	}
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
		if err = (&operator.Service{Operator: superUser, Broker: answers["broker"], Roots: roots}).WaitOperatorCredential(ctx, password); err != nil {
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
	// The trace topics of the install, as the super user: wherry.traces and every live tenant's, created where missing
	// and converged on their configs, so a re-run gives an install that predates them its topics.
	if err := (&operator.Service{Operator: superUser}).EnsureTraceTopics(ctx); err != nil {
		return fmt.Errorf("%w; run operator init again", err)
	}
	if err := writeYAML(filepath.Join(dir, "operator.yaml"), operatorConfig{Broker: answers["broker"], PasswordFile: "operator.password", CACert: caCert}); err != nil {
		return err
	}
	// The written file is what every later command reads, so it is read back before success is reported.
	if _, _, err := loadOperatorConfig(dir); err != nil {
		return err
	}
	if err := ignore(filepath.Join(dir, ".gitignore"), "operator.password", "operator.lock", "bundles/", "storage/"); err != nil {
		return err
	}
	fmt.Fprintf(prompts, "%s can provision; its password is in %s\n", operator.OperatorPrincipal, filepath.Join(dir, "operator.password"))
	return nil
}

// storageKeysPath is the file under dir's storage/ holding tenant's S3 key pair; the directory is made, mode 0700, with
// create.
func storageKeysPath(dir, tenant string, create bool) (string, error) {
	if !wire.ValidName(tenant) {
		return "", fmt.Errorf("%q is not a valid name", tenant)
	}
	storage := filepath.Join(dir, "storage")
	if create {
		if err := os.MkdirAll(storage, 0o700); err != nil {
			return "", err
		}
		if err := os.Chmod(storage, 0o700); err != nil {
			return "", err
		}
	}
	return filepath.Join(storage, tenant+".yaml"), nil
}

// tenantStorageKeys is the pair storage --tenant recorded for tenant in dir, zero when none was.
func tenantStorageKeys(dir, tenant string) (operator.StorageKeys, error) {
	path, err := storageKeysPath(dir, tenant, false)
	if err != nil {
		return operator.StorageKeys{}, err
	}
	keys, err := operator.ReadStorageKeys(path)
	if errors.Is(err, os.ErrNotExist) {
		return operator.StorageKeys{}, nil
	}
	return keys, err
}

// checkStorageKey is the validator of one of the pair, what naming it as in "access key". Its messages are fixed: the
// prompt displays them, and the value is a secret. A key never holds whitespace or a control character, and one typed
// with either would sign every request wrong.
func checkStorageKey(what string) func(string) error {
	return func(s string) error {
		if s == "" {
			return fmt.Errorf("%s is required", what)
		}
		if strings.ContainsFunc(s, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
			return fmt.Errorf("%s holds whitespace or a control character", what)
		}
		return nil
	}
}

// storageKeysFromStandardInput reads the pair as one pipe carries it: exactly two non-empty lines, the access key then
// the secret key, with or without a final line break. Anything else is refused with one message naming no content.
func storageKeysFromStandardInput() (operator.StorageKeys, error) {
	b, err := io.ReadAll(stdin)
	if err != nil {
		return operator.StorageKeys{}, fmt.Errorf("standard input: %w", err)
	}
	text := strings.TrimSuffix(strings.TrimSuffix(string(b), "\n"), "\r")
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimSuffix(line, "\r")
	}
	if len(lines) != 2 || lines[0] == "" || lines[1] == "" {
		return operator.StorageKeys{}, errors.New("standard input must hold exactly two lines: the access key, then the secret key")
	}
	return operator.StorageKeys{AccessKey: lines[0], SecretKey: lines[1]}, nil
}

// tenantStorage records tenant's S3 key pair in dir's storage/<tenant>.yaml, mode 0600 and kept out of git, from masked
// prompts or, with fromStdin, standard input, validating both values before the kept pair is replaced atomically;
// neither value reaches a record, an error or the output.
func tenantStorage(ctx context.Context, service *operator.Service, dir, tenant string, fromStdin bool, stdout, prompts io.Writer) (result error) {
	defer func() {
		if result != nil {
			result = presentationError{result}
		}
	}()
	if _, err := storageKeysPath(dir, tenant, false); err != nil {
		return err
	}
	if fromStdin && stdinIsTerminal() {
		return errors.New("standard input is a terminal; leave out - to be asked for the storage keys, or pipe them in, the access key then the secret key, one per line")
	}
	if !fromStdin && !promptsEligible(prompts) {
		return errors.New("storage has no terminal to prompt on; pipe the access key and the secret key in, one per line, with -")
	}
	state, err := service.LoadState(ctx)
	if err != nil {
		return err
	}
	if err := state.LiveTenant(tenant); err != nil {
		return err
	}
	var keys operator.StorageKeys
	if fromStdin {
		if keys, err = storageKeysFromStandardInput(); err != nil {
			return err
		}
		for _, key := range []struct{ what, value string }{{"access key", keys.AccessKey}, {"secret key", keys.SecretKey}} {
			if err := checkStorageKey(key.what)(key.value); err != nil {
				return err
			}
		}
	} else {
		q := &questions{ctx, stdin, &promptLatch{out: prompts}}
		if err := q.line("\nStorage of tenant %s\n", display(tenant)); err != nil {
			return err
		}
		if keys.AccessKey, err = q.secret("S3 access key", checkStorageKey("access key")); err != nil {
			return err
		}
		if keys.SecretKey, err = q.secret("S3 secret key", checkStorageKey("secret key")); err != nil {
			return err
		}
	}
	path, err := storageKeysPath(dir, tenant, true)
	if err != nil {
		return err
	}
	if err := operator.ReplaceStorageKeys(path, keys); err != nil {
		return err
	}
	if err := ignore(filepath.Join(dir, ".gitignore"), "storage/"); err != nil {
		return err
	}
	// The pair reaches a host only in its bundle, so each live host of the tenant is named with the rotation that
	// rewrites it; a host added from now on gets it from add --host.
	var hosts []string
	for _, principal := range slices.Sorted(maps.Keys(state.Hosts)) {
		host := state.Hosts[principal]
		if host.Tenant == tenant && !state.Ended[principal] && !state.Revoking[principal] {
			hosts = append(hosts, host.Name)
		}
	}
	if _, err := fmt.Fprintf(stdout, "recorded the storage keys of %s in %s; add --host writes them into each new host's bundle\n", tenant, path); err != nil {
		return err
	}
	for _, host := range hosts {
		if _, err := fmt.Fprintf(stdout, "refresh %s's bundle: wherry operator rotate --tenant %s --host %s --storage\n", host, tenant, host); err != nil {
			return err
		}
	}
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

// loadOperatorConfig reads dir's operator.yaml and builds the pool its ca_cert gives: the machine's trust store plus the
// CA, refusing a ca_cert from which Go appends no certificate.
func loadOperatorConfig(dir string) (operatorConfig, *x509.CertPool, error) {
	path := filepath.Join(dir, "operator.yaml")
	encoded, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return operatorConfig{}, nil, fmt.Errorf("%s does not exist; run wherry operator init", path)
	} else if err != nil {
		return operatorConfig{}, nil, err
	}
	if err := operator.RefuseCAFile(path, encoded); err != nil {
		return operatorConfig{}, nil, err
	}
	var config operatorConfig
	if err := operator.DecodeYAML(encoded, &config); err != nil {
		return operatorConfig{}, nil, fmt.Errorf("%s: %w", path, err)
	}
	if config.Broker == "" || config.PasswordFile == "" {
		return operatorConfig{}, nil, fmt.Errorf("%s requires broker and password_file", path)
	}
	roots, err := operator.ConfigRootPool(path, config.CACert)
	if err != nil {
		return operatorConfig{}, nil, err
	}
	return config, roots, nil
}

// existingCACert is the ca_cert of dir's operator.yaml when the file exists, which operator init without --ca-cert keeps;
// empty when it does not exist or holds none. A file keeping ca_file is refused naming ca_cert.
func existingCACert(dir string) (string, error) {
	path := filepath.Join(dir, "operator.yaml")
	encoded, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	} else if err != nil {
		return "", err
	}
	if err := operator.RefuseCAFile(path, encoded); err != nil {
		return "", err
	}
	var config operatorConfig
	if err := operator.DecodeYAML(encoded, &config); err != nil {
		return "", fmt.Errorf("%s: %w", path, err)
	}
	return config.CACert, nil
}

// operatorService connects as wherry.operator through dir's operator.yaml, verifying the broker against the machine's
// trust store plus its ca_cert, and returns the configuration it read, zero when it could not read one.
func operatorService(dir string) (*operator.Service, operatorConfig, error) {
	config, roots, err := loadOperatorConfig(dir)
	if err != nil {
		return nil, operatorConfig{}, err
	}
	password, err := os.ReadFile(inDir(dir, config.PasswordFile))
	if err != nil {
		return nil, config, err
	}
	client, err := operatorKafka(config.Broker, operator.OperatorPrincipal, string(password), roots)
	if err != nil {
		return nil, config, err
	}
	service := &operator.Service{Operator: client, Broker: config.Broker, Roots: roots, Storage: func(tenant string) (operator.StorageKeys, error) { return tenantStorageKeys(dir, tenant) }}
	if config.Tracing {
		if service.Tracing, err = telemetry.New(context.Background(), telemetry.RoleOperator, telemetry.Names{}, client.Opts(), log.New(os.Stderr, "wherry: ", 0)); err != nil {
			client.Close()
			return nil, config, fmt.Errorf("tracing: %w", err)
		}
	}
	return service, config, nil
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

// refreshBundle rewrites the kept bundle at path as current, keeping kept's principal and password, when the kept bundle
// differs from the current one, and reports whether it wrote.
func refreshBundle(path string, kept, current operator.Bundle) (bool, error) {
	current.Principal, current.Password = kept.Principal, kept.Password
	if current == kept {
		return false, nil
	}
	return true, operator.ReplaceBundle(path, current)
}

// objectFile is the file apply reads for name and whether it is a directory: -f's, else dir's kind/name.yaml, else
// that file written from prompt's answers. Without a terminal to prompt on, it refuses, naming fields; so does a dry run,
// which writes nothing.
func objectFile(ctx context.Context, dir, kind, name string, options map[string]string, dryRun bool, prompts io.Writer, fields string, prompt func(*questions) (any, error)) (string, bool, error) {
	path := options["file"]
	if path == "" {
		if !wire.ValidName(name) {
			return "", false, fmt.Errorf("%q is not a valid name", name)
		}
		path = filepath.Join(dir, kind, name+".yaml")
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			if dryRun {
				return "", false, fmt.Errorf("%s does not exist and --dry-run writes nothing; write it, or give --file PATH, with %s", path, fields)
			}
			if !promptsEligible(prompts) {
				return "", false, fmt.Errorf("%s does not exist and there is no terminal to prompt on; write it, or give --file PATH, with %s", path, fields)
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

// poolFields are what a pool file gives without a terminal, as apply --pool's prompts ask for them.
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
	c, err := catalog.Embedded()
	if err != nil {
		return nil, err
	}
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
		// A catalog agent's derived credentials are shown, and take part in the checks on the ones added here.
		var targets []wire.TaskCredential
		if derived, ok := c.Expand(name); ok {
			var shown []string
			for _, credential := range derived {
				targets = append(targets, credential.TaskCredential())
				required := "required"
				if !credential.Required {
					required = "optional"
				}
				shown = append(shown, credential.Name+" at "+credential.Env+", "+required)
			}
			if err := q.answer("Catalog credentials of "+name, strings.Join(shown, "; ")); err != nil {
				return nil, err
			}
		}
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
	_, err = q.ask("Producer byte rate per host, bytes a second", "", strconv.Itoa(operator.DefaultProducerByteRate), "", func(s string) error {
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
	_, err = q.ask("Request percentage per host", "", strconv.Itoa(operator.DefaultRequestPercentage), "", func(s string) error {
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

// receiverFields are what a receiver file gives without a terminal, as apply --receiver's prompts ask for them.
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
