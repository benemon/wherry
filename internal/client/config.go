package client

import (
	"context"
	"crypto/ed25519"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/trust"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Config is the on-disk wherry host configuration. Credentials maps a
// credential name to the source of its value. Git names the credential and
// host key a task cloning under each URL prefix takes. InputRoots are the
// directories the MCP server sends inputs from.
type Config struct {
	Bundle      string            `json:"bundle"`
	HostKey     string            `json:"host_key,omitzero"`
	StateDir    string            `json:"state_dir,omitzero"`
	Credentials map[string]Source `json:"credentials,omitzero"`
	Git         []GitCredential   `json:"git,omitzero"`
	Storage     *StorageConfig    `json:"storage,omitzero"`
	InputRoots  []string          `json:"input_roots,omitzero"`
}

// Source is where a credential's value comes from, exactly one of: File, a file; Env, an environment variable of the
// process answering the seal; Command, an argv run without a shell whose stdout is the value.
type Source struct {
	File    string   `json:"file,omitzero"`
	Env     string   `json:"env,omitzero"`
	Command []string `json:"command,omitzero"`
}

// commandTimeout bounds a command source, long enough for an interactive unlock such as a password manager's; tests
// shorten it.
var commandTimeout = 60 * time.Second

// resolve reads the source's value. Its errors name no value or path, and quote nothing the command writes; its
// stderr is discarded.
func (source Source) resolve(ctx context.Context) ([]byte, error) {
	switch {
	case source.Env != "":
		value := os.Getenv(source.Env)
		if value == "" {
			return nil, errors.New("its environment variable is unset or empty")
		}
		return []byte(value), nil
	case len(source.Command) > 0:
		ctx, cancel := context.WithTimeout(ctx, commandTimeout)
		defer cancel()
		command := exec.CommandContext(ctx, source.Command[0], source.Command[1:]...)
		stdout := &boundedBuffer{limit: signed.MaxSeal}
		// WaitDelay stops a child that keeps the output open from holding the answer past the timeout.
		command.Stdout, command.WaitDelay = stdout, time.Second
		err := command.Run()
		var exit *exec.ExitError
		switch {
		case stdout.over:
			return nil, fmt.Errorf("its command wrote over %d bytes", signed.MaxSeal)
		case errors.Is(ctx.Err(), context.DeadlineExceeded):
			return nil, fmt.Errorf("its command did not finish within %s", commandTimeout)
		case errors.As(err, &exit):
			return nil, fmt.Errorf("its command exited with status %d", exit.ExitCode())
		case err != nil:
			return nil, errors.New("its command did not start")
		}
		return stdout.data, nil
	}
	value, err := os.ReadFile(source.File)
	if err != nil {
		// The cause without the path.
		return nil, errors.Unwrap(err)
	}
	return value, nil
}

// boundedBuffer fails the write that would take it past limit bytes.
type boundedBuffer struct {
	data  []byte
	limit int
	over  bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if len(b.data)+len(p) > b.limit {
		b.over = true
		return 0, errors.New("output over its bound")
	}
	b.data = append(b.data, p...)
	return len(p), nil
}

// GitCredential gives a task whose git remote starts with URL the credential Credential and, for ssh, the known_hosts
// file HostKey.
type GitCredential struct {
	URL        string `json:"url"`
	Credential string `json:"credential,omitzero"`
	HostKey    string `json:"host_key,omitzero"`
}

// StorageConfig names the owner-only files holding the host's S3 access key and secret key.
type StorageConfig struct {
	AccessKeyFile string `json:"access_key_file"`
	SecretKeyFile string `json:"secret_key_file"`
}

// DefaultConfigPath returns the per-user default path.
func DefaultConfigPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".config", "wherry", "config.json"), nil
}

// LoadConfig decodes the config at path, defaulting host_key and state_dir.
func LoadConfig(path string) (Config, error) {
	path, err := expandHome(path)
	if err != nil {
		return Config{}, err
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var config Config
	if err := json.Unmarshal(encoded, &config, signed.Strict); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	if config.Bundle == "" {
		return Config{}, errors.New("config requires bundle")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return Config{}, err
	}
	if config.HostKey == "" {
		config.HostKey = filepath.Join(home, ".config", "wherry", "host.key")
	}
	if config.StateDir == "" {
		config.StateDir = filepath.Join(home, ".config", "wherry", "state")
	}
	paths := []*string{&config.Bundle, &config.HostKey, &config.StateDir}
	if config.Storage != nil {
		if config.Storage.AccessKeyFile == "" || config.Storage.SecretKeyFile == "" {
			return Config{}, errors.New("config storage requires access_key_file and secret_key_file")
		}
		paths = append(paths, &config.Storage.AccessKeyFile, &config.Storage.SecretKeyFile)
	}
	for _, path := range paths {
		if *path, err = expandHome(*path); err != nil {
			return Config{}, err
		}
	}
	for name, source := range config.Credentials {
		if kinds := len(slices.DeleteFunc([]bool{source.File != "", source.Env != "", len(source.Command) > 0}, func(set bool) bool { return !set })); kinds != 1 {
			return Config{}, fmt.Errorf("config credential %s needs exactly one of file, env and command", name)
		}
		if len(source.Command) > 0 && source.Command[0] == "" {
			return Config{}, fmt.Errorf("config credential %s: command names no program", name)
		}
		if source.File, err = expandHome(source.File); err != nil {
			return Config{}, err
		}
		config.Credentials[name] = source
	}
	for i, entry := range config.Git {
		if !strings.HasPrefix(entry.URL, "https://") && !strings.HasPrefix(entry.URL, "ssh://") {
			return Config{}, fmt.Errorf("config git url %s must start with https:// or ssh://", entry.URL)
		}
		if slices.ContainsFunc(config.Git[:i], func(other GitCredential) bool { return other.URL == entry.URL }) {
			return Config{}, fmt.Errorf("config git url %s is listed twice", entry.URL)
		}
		if _, configured := config.Credentials[entry.Credential]; entry.Credential != "" && !configured {
			return Config{}, fmt.Errorf("config git url %s names credential %s, which credentials does not configure", entry.URL, entry.Credential)
		}
		if entry.HostKey != "" {
			if config.Git[i].HostKey, err = expandHome(entry.HostKey); err != nil {
				return Config{}, err
			}
			if _, err := os.ReadFile(config.Git[i].HostKey); err != nil {
				return Config{}, fmt.Errorf("config git url %s: host_key is not a readable file", entry.URL)
			}
		}
	}
	for i, root := range config.InputRoots {
		if config.InputRoots[i], err = expandHome(root); err != nil {
			return Config{}, err
		}
		if !filepath.IsAbs(config.InputRoots[i]) {
			return Config{}, fmt.Errorf("config input root %s must be absolute", root)
		}
		if info, err := os.Stat(config.InputRoots[i]); err != nil || !info.IsDir() {
			return Config{}, fmt.Errorf("config input root %s does not exist or is not a directory", root)
		}
	}
	return config, nil
}

// Init creates the host key if it is missing and registers it on the host's
// key topic, returning its key ID.
func Init(ctx context.Context, config Config) (string, error) {
	if _, err := os.Stat(config.HostKey); errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(filepath.Dir(config.HostKey), 0o700); err != nil {
			return "", err
		}
		if _, err := trust.GenerateSigningKeyFile(config.HostKey); err != nil {
			return "", err
		}
	}
	host, err := Connect(config, log.New(io.Discard, "", 0))
	if err != nil {
		return "", err
	}
	defer host.Close()
	return host.Register(ctx)
}

// Connect opens the bundle's TLS and SCRAM-SHA-512 Kafka connection as its host principal; the host logs to logger.
func Connect(config Config, logger *log.Logger) (*Host, error) {
	bundle, err := admin.ReadBundle(config.Bundle)
	if err != nil {
		return nil, err
	}
	opts, err := admin.KafkaOpts(bundle.Broker, []byte(bundle.CA), bundle.Principal, bundle.Password)
	if err != nil {
		return nil, fmt.Errorf("bundle: %w", err)
	}
	// Aborted transactions are a fenced holder's writes and must never be read.
	kafka, err := kgo.NewClient(append(opts, kgo.FetchIsolationLevel(kgo.ReadCommitted()))...)
	if err != nil {
		return nil, err
	}
	host, err := NewHost(kafka, nil, bundle.Principal, config.StateDir, config.Credentials, config.Git, logger)
	if err != nil {
		kafka.Close()
		return nil, err
	}
	if host.key, err = admin.RecoverKey(context.Background(), kafka, signed.HostOwner(host.tenant, host.host), admin.HostTopic(host.tenant, host.host), config.HostKey); err != nil {
		kafka.Close()
		return nil, fmt.Errorf("load host key: %w", err)
	}
	host.storage = config.Storage
	return host, nil
}

// RotateKey replaces the host key with a new one and returns the old and new key IDs. It refuses while a run this host
// submitted has not ended, since receivers admit a task only under its host's current key.
func RotateKey(ctx context.Context, config Config) (string, string, error) {
	host, err := Connect(config, log.New(io.Discard, "", 0))
	if err != nil {
		return "", "", err
	}
	defer host.Close()
	runs, err := host.Runs(ctx)
	if err != nil {
		return "", "", err
	}
	var unfinished []string
	for _, run := range runs.Runs {
		path, _ := host.runPath(run.RunID)
		if _, err := os.Stat(path); err == nil && !wire.Terminal(run.Status.State) {
			unfinished = append(unfinished, run.RunID)
		}
	}
	if len(unfinished) > 0 {
		slices.Sort(unfinished)
		return "", "", fmt.Errorf("runs %s have not ended; rotate the key once they have, with every other wherry process of this host (the MCP server, any wherry await) stopped", strings.Join(unfinished, ", "))
	}
	old := host.key
	next, err := admin.RotateKey(ctx, host.kafka, signed.HostOwner(host.tenant, host.host), admin.HostTopic(host.tenant, host.host), config.HostKey, old)
	if err != nil {
		return "", "", err
	}
	return signed.KeyID(old.Public().(ed25519.PublicKey)), signed.KeyID(next.Public().(ed25519.PublicKey)), nil
}

func expandHome(path string) (string, error) {
	if path != "~" && !strings.HasPrefix(path, "~/") {
		return path, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	if path == "~" {
		return home, nil
	}
	return filepath.Join(home, strings.TrimPrefix(path, "~/")), nil
}
