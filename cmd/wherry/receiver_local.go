package main

import (
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

func receiverLocalFile(dir, name, receiverPath string, directory, dryRun bool, out io.Writer, config operator.ReceiverConfig, op operatorConfig) error {
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
	needsCA := config.Broker == op.Broker && op.CAFile != ""
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
		if !terminal() {
			return fmt.Errorf("%s does not exist and there is no terminal to prompt on; write it with %s", path, receiverLocalFields)
		}
		local, err = promptReceiverLocal(newPrompter(out), config, needsCA)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		if err := writeYAML(path, local); err != nil {
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

func promptReceiverLocal(p *prompter, config operator.ReceiverConfig, needsCA bool) (receiverLocalConfig, error) {
	local := receiverLocalConfig{Credentials: map[string]credential.Source{}}
	var err error
	if local.Bundle, err = p.ask("Where will the receiver read its bundle?", "/etc/wherry/receiver.bundle"); err != nil {
		return local, err
	}
	ca := "none"
	if needsCA {
		ca = "/etc/wherry/kafka-ca.pem"
	}
	if local.CAFile, err = p.ask("Where will it read its broker's CA? none for the system's trusted roots.", ca); err != nil {
		return local, err
	}
	if local.CAFile == "none" {
		local.CAFile = ""
	}
	if local.StateDir, err = p.ask("Where does it keep its state? It must survive restarts.", "/var/lib/wherry/state"); err != nil {
		return local, err
	}
	if local.PluginDir, err = p.ask("Where are its plugins?", defaultPluginDir); err != nil {
		return local, err
	}
	answer, err := p.ask("How long does its platform wait for it to stop before killing it?", "30s")
	if err != nil {
		return local, err
	}
	window, err := time.ParseDuration(answer)
	if err != nil {
		return local, fmt.Errorf("stop window: %w", err)
	}
	kinds := map[string]bool{}
	for _, profile := range config.Profiles {
		kinds[profile.Plugin] = true
	}
	margin := max(10*time.Second, 6*time.Second+2*time.Second*time.Duration(len(kinds)))
	local.DrainTimeout = window - margin
	if local.DrainTimeout < 5*time.Second {
		return local, errors.New("stop window leaves less than 5s to drain; raise kill_timeout, terminationGracePeriodSeconds or TimeoutStopSec")
	}
	if local.MetricsAddr, err = p.ask("Where should it serve metrics? none for no listener.", ":9464"); err != nil {
		return local, err
	}
	if local.MetricsAddr == "none" {
		local.MetricsAddr = ""
	}
	for _, name := range receiverCredentials(config) {
		kind, err := p.ask(fmt.Sprintf("Credential %s: read from an environment variable (env) or a file (file)?", name), "env")
		if err != nil {
			return local, err
		}
		var source credential.Source
		switch kind {
		case "env":
			variable := receiverEnvOther.ReplaceAllString(strings.ToUpper(name), "_")
			if variable[0] >= '0' && variable[0] <= '9' {
				variable = "_" + variable
			}
			source.Env, err = p.ask("Environment variable", variable)
			if err == nil && !receiverEnv.MatchString(source.Env) {
				err = errors.New("invalid environment variable name")
			}
		case "file":
			source.File, err = p.ask("Credential file path", "")
			if err == nil && source.File == "" {
				err = errors.New("credential file path must not be blank")
			}
		default:
			err = errors.New("credential source must be env or file")
		}
		if err != nil {
			return local, err
		}
		local.Credentials[name] = source
	}
	return local, nil
}
