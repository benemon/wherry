package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/operator"
	"github.com/charmbracelet/huh"
	"gopkg.in/yaml.v3"
)

func receiverCredentials(config operator.ReceiverConfig) []string {
	names := map[string]bool{}
	for _, profile := range config.Profiles {
		if name, ok := profile.Config["credential"].(string); ok {
			names[name] = true
		}
	}
	result := make([]string, 0, len(names))
	for name := range names {
		result = append(result, name)
	}
	slices.Sort(result)
	return result
}

const receiverLocalFields = "bundle, ca_file, state_dir, plugin_dir, metrics_addr, drain_timeout and credentials"

// receiverLocalFile finds, or asks for and writes, the receiver's local configuration and checks it against config.
// broker and ca are the operator's broker and whether the operator trusts it through a CA file.
func receiverLocalFile(ctx context.Context, dir, name, receiverPath string, directory, dryRun bool, out io.Writer, config operator.ReceiverConfig, broker string, ca bool) error {
	needsCA := config.Broker == broker && ca
	path := filepath.Join(dir, "receivers", name+".config.yaml")
	if directory {
		candidate := filepath.Join(receiverPath, "config.yaml")
		if _, err := os.Stat(candidate); err == nil {
			path = candidate
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	_, err := os.Stat(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	var local receiverLocalConfig
	if err == nil {
		local, err = loadReceiverLocalConfig(path)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
	} else {
		if dryRun {
			fmt.Fprintf(out, "%s is absent; apply will ask for local configuration\n", path)
			return nil
		}
		if !promptsEligible(out) {
			return fmt.Errorf("%s does not exist and there is no terminal to prompt on; write it with %s", path, receiverLocalFields)
		}
		local, err = promptReceiverLocal(&questions{ctx, stdin, &promptLatch{out: out}}, config, needsCA)
		if err != nil {
			return presentationError{err}
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		encoded, err := yaml.Marshal(local)
		if err != nil {
			return err
		}
		// Exclusive creation refuses a symlink, dangling or not, planted at the path while the questions were asked.
		if _, err := createFile(path, encoded); err != nil {
			return err
		}
		fmt.Fprintf(out, "wrote %s\n", path)
	}
	for _, name := range receiverCredentials(config) {
		if _, ok := local.Credentials[name]; !ok {
			return fmt.Errorf("%s: missing credential source %s", path, name)
		}
	}
	if needsCA && local.CAFile == "" {
		fmt.Fprintln(out, "warning: local configuration has no ca_file; the broker must be trusted through system roots")
	}
	if dryRun {
		fmt.Fprintf(out, "would use local configuration %s\n", path)
	}
	return nil
}

var receiverEnv = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
var receiverEnvOther = regexp.MustCompile(`[^A-Z0-9_]`)

func promptReceiverLocal(q *questions, config operator.ReceiverConfig, needsCA bool) (receiverLocalConfig, error) {
	local := receiverLocalConfig{Credentials: map[string]credential.Source{}}
	kinds := map[string]bool{}
	for _, profile := range config.Profiles {
		kinds[profile.Plugin] = true
	}
	margin := max(10*time.Second, 6*time.Second+2*time.Second*time.Duration(len(kinds)))
	// These paths are on the receiver's host, so they get no completion and no local checks.
	input := func(label, initial string, validate func(string) error) (string, error) {
		value, err := q.input(label, initial, "", func(s string) error { return validate(strings.TrimSpace(s)) })
		return strings.TrimSpace(value), err
	}
	notBlank := func(what string) func(string) error {
		return func(s string) error {
			if s == "" {
				return errors.New(what + " must not be blank")
			}
			return nil
		}
	}
	asked := func(label, initial string, validate func(string) error) (string, error) {
		value, err := input(label, initial, validate)
		if err == nil {
			err = q.answer(label, value)
		}
		return value, err
	}
	if err := q.line("\nReceiver host\n"); err != nil {
		return local, err
	}
	var err error
	if local.Bundle, err = asked("Bundle path", "/etc/wherry/receiver.bundle", notBlank("bundle path")); err != nil {
		return local, err
	}
	ca := "roots"
	if needsCA {
		ca = "file"
	}
	if _, err := q.pick("Broker CA", "", []huh.Option[string]{option("CA file", "file"), option("System roots", "roots")}, &ca); err != nil {
		return local, err
	}
	answer := "System roots"
	if ca == "file" {
		if local.CAFile, err = input("CA file path", "/etc/wherry/kafka-ca.pem", notBlank("CA file path")); err != nil {
			return local, err
		}
		answer = local.CAFile
	}
	if err := q.answer("Broker CA", answer); err != nil {
		return local, err
	}
	if local.StateDir, err = asked("State directory (must survive restarts)", "/var/lib/wherry/state", notBlank("state directory")); err != nil {
		return local, err
	}
	if local.PluginDir, err = asked("Plugin directory", defaultPluginDir, notBlank("plugin directory")); err != nil {
		return local, err
	}
	var window time.Duration
	if _, err := asked("Stop window (how long its platform waits before killing it)", "30s", func(s string) error {
		var err error
		if window, err = time.ParseDuration(s); err != nil {
			return fmt.Errorf("stop window: %w", err)
		}
		if window < margin+5*time.Second {
			return errors.New("stop window leaves less than 5s to drain; raise kill_timeout, terminationGracePeriodSeconds or TimeoutStopSec")
		}
		return nil
	}); err != nil {
		return local, err
	}
	local.DrainTimeout = window - margin
	metrics := "listen"
	if _, err := q.pick("Metrics", "", []huh.Option[string]{option("Listen on an address", "listen"), option("No listener", "none")}, &metrics); err != nil {
		return local, err
	}
	answer = "No listener"
	if metrics == "listen" {
		if local.MetricsAddr, err = input("Metrics address", ":9464", notBlank("metrics address")); err != nil {
			return local, err
		}
		answer = local.MetricsAddr
	}
	if err := q.answer("Metrics", answer); err != nil {
		return local, err
	}
	names := receiverCredentials(config)
	if len(names) > 0 {
		if err := q.line("\nCredentials\n"); err != nil {
			return local, err
		}
	}
	for _, name := range names {
		label := "Credential " + name
		kind := "env"
		if _, err := q.pick(label, "", []huh.Option[string]{option("Environment variable", "env"), option("File", "file")}, &kind); err != nil {
			return local, err
		}
		var source credential.Source
		if kind == "env" {
			variable := receiverEnvOther.ReplaceAllString(strings.ToUpper(name), "_")
			if variable[0] >= '0' && variable[0] <= '9' {
				variable = "_" + variable
			}
			if source.Env, err = input("Variable", variable, func(s string) error {
				if !receiverEnv.MatchString(s) {
					return errors.New("invalid environment variable name")
				}
				return nil
			}); err != nil {
				return local, err
			}
			answer = "Environment variable " + display(source.Env)
		} else {
			if source.File, err = input("File path", "", notBlank("credential file path")); err != nil {
				return local, err
			}
			answer = "File " + display(source.File)
		}
		if err := q.line("? %s  %s\n", display(label), answer); err != nil {
			return local, err
		}
		local.Credentials[name] = source
	}
	return local, nil
}
