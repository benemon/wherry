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

// receiverLocalWritten is the local configuration apply writes: a source for each platform credential the profiles
// name, with the plugin directory, metrics address and drain timeout a receiver without a file gets, so the file
// changes nothing else; the plugin directory is written out so a receiver whose plugins are elsewhere, a Nomad exec or
// systemd one, has the line to change. The bundle and state directory are left to the receiver's platform.
type receiverLocalWritten struct {
	PluginDir    string                       `yaml:"plugin_dir"`
	MetricsAddr  string                       `yaml:"metrics_addr"`
	DrainTimeout time.Duration                `yaml:"drain_timeout"`
	Credentials  map[string]credential.Source `yaml:"credentials"`
}

// receiverLocalFile checks a kept local configuration against config, or, when the profiles name a platform
// credential and no file is kept, asks for a source for each and writes the file. With no credential named it writes
// nothing: the receiver runs without a file. The broker's CA is not asked for: the receiver's machine trusts it through
// its system roots.
func receiverLocalFile(ctx context.Context, dir, name, receiverPath string, directory, dryRun bool, out io.Writer, config operator.ReceiverConfig) error {
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
	names := receiverCredentials(config)
	var sources map[string]credential.Source
	if err == nil {
		local, _, err := readReceiverLocalConfig(path)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		sources = local.Credentials
	} else {
		if len(names) == 0 {
			fmt.Fprintf(out, "%s is absent and no profile names a platform credential; the receiver runs without a file\n", path)
			return nil
		}
		if dryRun {
			fmt.Fprintf(out, "%s is absent; apply will ask for a source for each platform credential\n", path)
			return nil
		}
		if !promptsEligible(out) {
			return fmt.Errorf("%s does not exist and there is no terminal to prompt on; write it with a credentials source for %s", path, strings.Join(names, ", "))
		}
		sources, err = promptReceiverCredentials(&questions{ctx, stdin, &promptLatch{out: out}}, names)
		if err != nil {
			return presentationError{err}
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		encoded, err := yaml.Marshal(receiverLocalWritten{PluginDir: defaultPluginDir, MetricsAddr: defaultMetricsAddr, DrainTimeout: defaultDrainTimeout, Credentials: sources})
		if err != nil {
			return err
		}
		// Exclusive creation refuses a symlink, dangling or not, planted at the path while the questions were asked.
		if _, err := createFile(path, encoded); err != nil {
			return err
		}
		fmt.Fprintf(out, "wrote %s\n", path)
	}
	for _, name := range names {
		if _, ok := sources[name]; !ok {
			return fmt.Errorf("%s: missing credential source %s", path, name)
		}
	}
	if dryRun {
		fmt.Fprintf(out, "would use local configuration %s\n", path)
	}
	return nil
}

var receiverEnv = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
var receiverEnvOther = regexp.MustCompile(`[^A-Z0-9_]`)

// promptReceiverCredentials asks for a source for each of names, in order. Receiver-host values get no completion and
// no local checks.
func promptReceiverCredentials(q *questions, names []string) (map[string]credential.Source, error) {
	sources := map[string]credential.Source{}
	input := func(label, initial string, validate func(string) error) (string, error) {
		return q.input(label, "", initial, "", validate)
	}
	if err := q.line("\nCredentials\n"); err != nil {
		return sources, err
	}
	for _, name := range names {
		label := "Credential " + name
		kind := "env"
		if _, err := q.pick(label, "", []huh.Option[string]{option("Environment variable", "env"), option("File", "file")}, &kind); err != nil {
			return sources, err
		}
		var source credential.Source
		var answer string
		var err error
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
				return sources, err
			}
			answer = "Environment variable " + source.Env
		} else {
			if source.File, err = input("File path", "", func(s string) error {
				if s == "" {
					return errors.New("credential file path must not be blank")
				}
				return nil
			}); err != nil {
				return sources, err
			}
			answer = "File " + source.File
		}
		if err := q.answer(label, answer); err != nil {
			return sources, err
		}
		sources[name] = source
	}
	return sources, nil
}
