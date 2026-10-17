package client

import (
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/benemon/agentbus/internal/trust"
	"github.com/benemon/agentbus/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Config is the on-disk agentbus client configuration.
type Config struct {
	Broker            string `json:"broker"`
	CAFile            string `json:"ca_file"`
	KafkaUser         string `json:"kafka_user"`
	KafkaPasswordFile string `json:"kafka_password_file"`
	UserKey           string `json:"user_key,omitzero"`
	ReplyTopic        string `json:"reply_topic,omitzero"`
}

// DefaultConfigPath returns AGENTBUS_CONFIG or the per-user default path.
func DefaultConfigPath() (string, error) {
	if path := os.Getenv("AGENTBUS_CONFIG"); path != "" {
		return expandHome(path)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".config", "agentbus", "config.json"), nil
}

// LoadConfig strictly decodes and applies the two settled defaults.
func LoadConfig(path string) (Config, error) {
	encoded, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var config Config
	if err := json.Unmarshal(encoded, &config,
		json.RejectUnknownMembers(true),
		jsontext.AllowDuplicateNames(false),
	); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	if config.Broker == "" || config.CAFile == "" || config.KafkaUser == "" || config.KafkaPasswordFile == "" {
		return Config{}, errors.New("config requires broker, ca_file, kafka_user, and kafka_password_file")
	}
	if config.UserKey == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return Config{}, err
		}
		config.UserKey = filepath.Join(home, ".config", "agentbus", "user.key")
	}
	if config.ReplyTopic == "" {
		config.ReplyTopic = "replies." + config.KafkaUser
	}
	for _, path := range []*string{&config.CAFile, &config.KafkaPasswordFile, &config.UserKey} {
		*path, err = expandHome(*path)
		if err != nil {
			return Config{}, err
		}
	}
	return config, nil
}

// InitUserKey creates the configured user key and returns its fingerprint.
func InitUserKey(path string) (string, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", err
	}
	key, err := trust.GenerateSigningKeyFile(path)
	if err != nil {
		return "", err
	}
	return wire.Fingerprint(key.Public().(ed25519.PublicKey)), nil
}

// Connect opens the configured TLS and SCRAM-SHA-512 Kafka connection.
func Connect(config Config) (*Client, error) {
	caPEM, err := os.ReadFile(config.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read CA file: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("CA file contains no certificates")
	}
	password, err := os.ReadFile(config.KafkaPasswordFile)
	if err != nil {
		return nil, fmt.Errorf("read Kafka password: %w", err)
	}
	userKey, err := trust.LoadSigningKeyFile(config.UserKey)
	if err != nil {
		return nil, fmt.Errorf("load user key: %w", err)
	}
	kafka, err := kgo.NewClient(
		kgo.SeedBrokers(config.Broker),
		kgo.DialTLSConfig(&tls.Config{RootCAs: roots}),
		kgo.SASL(scram.Auth{User: config.KafkaUser, Pass: strings.TrimRight(string(password), "\r\n")}.AsSha512Mechanism()),
		// Aborted transactions are a fenced holder's writes (ADR-0010) and must never be read.
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		// A trailing abort marker is the last offset below a read bound; it must be seen to finish.
		kgo.KeepControlRecords(),
	)
	if err != nil {
		return nil, err
	}
	return New(kafka, userKey, config.ReplyTopic), nil
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
