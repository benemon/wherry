package client

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/trust"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Config is the on-disk wherry host configuration. Credentials maps a
// credential name to the source of its value. Git names the credential and
// host key a task cloning under each URL prefix takes. InputRoots are the
// directories the MCP server sends inputs from. CAFile holds the PEM certificates the host trusts the broker with; without
// it, the system roots. Storage maps a pool to the credentials of its bucket.
type Config struct {
	Bundle      string                       `yaml:"bundle"`
	CAFile      string                       `yaml:"ca_file,omitempty"`
	HostKey     string                       `yaml:"host_key,omitempty"`
	StateDir    string                       `yaml:"state_dir,omitempty"`
	Credentials map[string]credential.Source `yaml:"credentials,omitempty"`
	Git         []GitCredential              `yaml:"git,omitempty"`
	Storage     map[string]StorageConfig     `yaml:"storage,omitempty"`
	InputRoots  []string                     `yaml:"input_roots,omitempty"`
}

// GitCredential gives a task whose git remote starts with URL the credential Credential and, for ssh, the known_hosts
// file HostKey.
type GitCredential struct {
	URL        string `yaml:"url"`
	Credential string `yaml:"credential,omitempty"`
	HostKey    string `yaml:"host_key,omitempty"`
}

// StorageConfig names the credentials holding the host's S3 access key and secret key for a pool's bucket.
type StorageConfig struct {
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
}

// ConfigDir is $XDG_CONFIG_HOME/wherry, by default ~/.config/wherry on every OS: it holds the host's bundle.yaml,
// host.key, config.yaml and ca.pem.
func ConfigDir() (string, error) {
	return xdgDir("XDG_CONFIG_HOME", ".config")
}

// StateDir is $XDG_STATE_HOME/wherry, by default ~/.local/state/wherry on every OS.
func StateDir() (string, error) {
	return xdgDir("XDG_STATE_HOME", filepath.Join(".local", "state"))
}

func xdgDir(variable, fallback string) (string, error) {
	if dir := os.Getenv(variable); dir != "" {
		return filepath.Join(dir, "wherry"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, fallback, "wherry"), nil
}

// DefaultConfigPath returns the per-user default path.
func DefaultConfigPath() (string, error) {
	dir, err := ConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "config.yaml"), nil
}

// LoadConfig decodes the config at path, defaulting host_key and state_dir.
func LoadConfig(path string) (Config, error) {
	path, err := ExpandHome(path)
	if err != nil {
		return Config{}, err
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var config Config
	if err := operator.DecodeYAML(encoded, &config); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	if config.Bundle == "" {
		return Config{}, errors.New("config requires bundle")
	}
	if config.HostKey == "" {
		dir, err := ConfigDir()
		if err != nil {
			return Config{}, err
		}
		config.HostKey = filepath.Join(dir, "host.key")
	}
	if config.StateDir == "" {
		if config.StateDir, err = StateDir(); err != nil {
			return Config{}, err
		}
	}
	for _, path := range []*string{&config.Bundle, &config.HostKey, &config.StateDir, &config.CAFile} {
		if *path, err = ExpandHome(*path); err != nil {
			return Config{}, err
		}
	}
	if dir, err := ConfigDir(); err == nil && config.CAFile == "" {
		if _, err := os.Stat(filepath.Join(dir, "ca.pem")); err == nil {
			config.CAFile = filepath.Join(dir, "ca.pem")
		}
	}
	for name, source := range config.Credentials {
		if err := source.Validate(); err != nil {
			return Config{}, fmt.Errorf("config credential %s: %w", name, err)
		}
		if source.File, err = ExpandHome(source.File); err != nil {
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
			if config.Git[i].HostKey, err = ExpandHome(entry.HostKey); err != nil {
				return Config{}, err
			}
			if _, err := os.ReadFile(config.Git[i].HostKey); err != nil {
				return Config{}, fmt.Errorf("config git url %s: host_key is not a readable file", entry.URL)
			}
		}
	}
	for pool, storage := range config.Storage {
		if storage.AccessKey == "" || storage.SecretKey == "" {
			return Config{}, fmt.Errorf("config storage of pool %s requires access_key and secret_key", pool)
		}
		for _, key := range [][2]string{{"access_key", storage.AccessKey}, {"secret_key", storage.SecretKey}} {
			if _, configured := config.Credentials[key[1]]; !configured {
				return Config{}, fmt.Errorf("config storage of pool %s: %s names credential %s, which credentials does not configure", pool, key[0], key[1])
			}
		}
	}
	for i, root := range config.InputRoots {
		if config.InputRoots[i], err = ExpandHome(root); err != nil {
			return Config{}, err
		}
		if !filepath.IsAbs(config.InputRoots[i]) {
			return Config{}, fmt.Errorf("config input root %s must be absolute", root)
		}
		if info, err := os.Stat(config.InputRoots[i]); err != nil || !info.IsDir() {
			return Config{}, fmt.Errorf("config input root %s does not exist or is not a directory", root)
		}
		if held := HeldHostPath(config.InputRoots[i], config); held != "" {
			return Config{}, fmt.Errorf("config input root %s holds %s, a directory of the host's own files", root, held)
		}
	}
	return config, nil
}

// HeldHostPath names the first directory of the host's own files beneath root or root itself: state_dir, or the
// directory of the host key, the bundle or a file credential source; it is "" when root holds none. An input opened
// through root can follow a link anywhere beneath root, so a root holding any of them is refused rather than checked
// at each open. Each directory's ancestors are compared with root by identity, so a link, a case-varied spelling or a
// bind mount hides none, and a directory that does not exist yet is placed by its nearest existing ancestor.
func HeldHostPath(root string, config Config) string {
	rootInfo, err := os.Stat(root)
	if err != nil {
		return ""
	}
	paths := []string{config.StateDir, filepath.Dir(config.HostKey), filepath.Dir(config.Bundle)}
	for _, name := range slices.Sorted(maps.Keys(config.Credentials)) {
		if file := config.Credentials[name].File; file != "" {
			paths = append(paths, filepath.Dir(file))
		}
	}
	for _, path := range paths {
		if path == "" {
			continue
		}
		for dir := resolveExisting(path); ; dir = filepath.Dir(dir) {
			if info, err := os.Stat(dir); err == nil && os.SameFile(info, rootInfo) {
				return path
			}
			if dir == filepath.Dir(dir) {
				break
			}
		}
	}
	return ""
}

// resolveExisting is path made absolute with the links of its nearest existing ancestor resolved, and the rest of it
// unchanged.
func resolveExisting(path string) string {
	path, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	rest := ""
	for dir := path; ; dir = filepath.Dir(dir) {
		if resolved, err := filepath.EvalSymlinks(dir); err == nil {
			return filepath.Join(resolved, rest)
		}
		if dir == filepath.Dir(dir) {
			return path
		}
		rest = filepath.Join(filepath.Base(dir), rest)
	}
}

// Init creates the host key if it is missing and registers it on the host's key topic, returning its key ID and the
// terms of every pool the host's tenant is entitled to.
func Init(ctx context.Context, config Config) (string, map[string]operator.Terms, error) {
	if _, err := os.Stat(config.HostKey); errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(filepath.Dir(config.HostKey), 0o700); err != nil {
			return "", nil, err
		}
		if _, err := trust.GenerateSigningKeyFile(config.HostKey); err != nil {
			return "", nil, err
		}
	}
	host, err := Connect(config, log.New(io.Discard, "", 0))
	if err != nil {
		return "", nil, err
	}
	defer host.Close()
	keyID, err := host.Register(ctx)
	if err != nil {
		return "", nil, err
	}
	state, err := host.reader.Load(ctx)
	if err != nil {
		return "", nil, err
	}
	pools := map[string]operator.Terms{}
	for pool := range state.Entitlements[host.tenant] {
		pools[pool] = state.Pools[pool]
	}
	return keyID, pools, nil
}

// Connect opens the bundle's TLS and SCRAM-SHA-512 Kafka connection as its host principal; the host logs to logger.
func Connect(config Config, logger *log.Logger) (*Host, error) {
	bundle, err := operator.ReadBundle(config.Bundle)
	if err != nil {
		return nil, err
	}
	var ca []byte
	if config.CAFile != "" {
		if ca, err = os.ReadFile(config.CAFile); err != nil {
			return nil, fmt.Errorf("ca_file: %w", err)
		}
	}
	opts, err := operator.KafkaOpts(bundle.Broker, ca, bundle.Principal, bundle.Password)
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
	if host.key, err = operator.RecoverKey(context.Background(), kafka, signed.HostOwner(host.tenant, host.host), operator.HostTopic(host.tenant, host.host), config.HostKey); err != nil {
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
		return "", "", fmt.Errorf("runs %s have not ended; rotate the key once they have, with every other wherry process of this host (the MCP server, any wherry wait) stopped", strings.Join(unfinished, ", "))
	}
	old := host.key
	next, err := operator.RotateKey(ctx, host.kafka, signed.HostOwner(host.tenant, host.host), operator.HostTopic(host.tenant, host.host), config.HostKey, old)
	if err != nil {
		return "", "", err
	}
	return signed.KeyID(old.Public().(ed25519.PublicKey)), signed.KeyID(next.Public().(ed25519.PublicKey)), nil
}

// ExpandHome expands a leading ~ or ~/ to the user's home directory.
func ExpandHome(path string) (string, error) {
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
