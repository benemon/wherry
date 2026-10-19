package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/trust"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"gopkg.in/yaml.v3"
)

// Config is the on-disk wherry host configuration. Credentials maps a
// credential name to the file holding its value.
type Config struct {
	Bundle      string            `json:"bundle"`
	HostKey     string            `json:"host_key,omitzero"`
	StateDir    string            `json:"state_dir,omitzero"`
	Credentials map[string]string `json:"credentials,omitzero"`
}

// DefaultConfigPath returns WHERRY_CONFIG or the per-user default path.
func DefaultConfigPath() (string, error) {
	if path := os.Getenv("WHERRY_CONFIG"); path != "" {
		return expandHome(path)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".config", "wherry", "config.json"), nil
}

// LoadConfig decodes the config at path, defaulting host_key and state_dir.
func LoadConfig(path string) (Config, error) {
	encoded, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var config Config
	if err := json.Unmarshal(encoded, &config, json.RejectUnknownMembers(true), jsontext.AllowDuplicateNames(false)); err != nil {
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
	for _, path := range []*string{&config.Bundle, &config.HostKey, &config.StateDir} {
		if *path, err = expandHome(*path); err != nil {
			return Config{}, err
		}
	}
	for name, path := range config.Credentials {
		if config.Credentials[name], err = expandHome(path); err != nil {
			return Config{}, err
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
	host, err := Connect(config)
	if err != nil {
		return "", err
	}
	defer host.Close()
	return host.Register(ctx)
}

// Connect opens the bundle's TLS and SCRAM-SHA-512 Kafka connection as its host principal.
func Connect(config Config) (*Host, error) {
	encoded, err := os.ReadFile(config.Bundle)
	if err != nil {
		return nil, fmt.Errorf("read bundle: %w", err)
	}
	var bundle admin.Bundle
	decoder := yaml.NewDecoder(bytes.NewReader(encoded))
	decoder.KnownFields(true)
	// YAML errors can quote the bundle's text, which holds the password.
	if decoder.Decode(&bundle) != nil || bundle.Broker == "" || bundle.Principal == "" || bundle.Password == "" {
		return nil, errors.New("bundle is not a valid wherry bundle")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(bundle.CA)) {
		return nil, errors.New("bundle CA contains no certificates")
	}
	key, err := trust.LoadSigningKeyFile(config.HostKey)
	if err != nil {
		return nil, fmt.Errorf("load host key: %w", err)
	}
	kafka, err := kgo.NewClient(
		kgo.SeedBrokers(bundle.Broker),
		kgo.DialTLSConfig(&tls.Config{RootCAs: roots}),
		kgo.SASL(scram.Auth{User: bundle.Principal, Pass: bundle.Password}.AsSha512Mechanism()),
		// Aborted transactions are a fenced holder's writes (ADR-0010) and must never be read.
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		return nil, err
	}
	host, err := NewHost(kafka, key, bundle.Principal, config.StateDir, config.Credentials)
	if err != nil {
		kafka.Close()
		return nil, err
	}
	return host, nil
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
