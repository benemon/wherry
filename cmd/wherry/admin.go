package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"gopkg.in/yaml.v3"
)

func runAdmin(args []string, stdout, stderr io.Writer) error {
	pos, options, err := adminArgs(args)
	if err != nil {
		return err
	}
	if len(pos) == 0 {
		return errors.New("usage: wherry admin <pool create|receiver add|tenant add|host add|revoke host|revoke receiver|skip|check>")
	}
	root, err := adminKafka("WHERRY_TRUST_ADMIN_USER", "WHERRY_TRUST_ADMIN_PASSWORD_FILE")
	if err != nil {
		return err
	}
	defer root.Close()
	operator, err := adminKafka("WHERRY_TRUST_OPERATOR_USER", "WHERRY_TRUST_OPERATOR_PASSWORD_FILE")
	if err != nil {
		return err
	}
	defer operator.Close()
	ca, err := os.ReadFile(os.Getenv("WHERRY_TRUST_CA"))
	if err != nil {
		return err
	}
	service := admin.Service{Admin: root, Operator: operator, Broker: os.Getenv("WHERRY_TRUST_BROKER"), CA: string(ca)}
	ctx := context.Background()
	command := strings.Join(pos[:min(len(pos), 2)], " ")
	if pos[0] == "skip" {
		command = "skip"
	}
	switch command {
	case "pool create":
		if len(pos) != 3 {
			return errors.New("usage: wherry admin pool create NAME --terms FILE")
		}
		var terms admin.Terms
		if err := readYAML(options["terms"], &terms); err != nil {
			return err
		}
		return service.PoolCreate(ctx, pos[2], terms)
	case "receiver add":
		if len(pos) != 3 {
			return errors.New("usage: wherry admin receiver add ID --pool POOL --config FILE --bundle FILE")
		}
		var config admin.ReceiverConfig
		if err := readYAML(options["config"], &config); err != nil {
			return err
		}
		if (config.Receiver != "" && config.Receiver != pos[2]) || (config.Pool != "" && config.Pool != options["pool"]) {
			return errors.New("receiver config identity does not match command")
		}
		config.Receiver, config.Pool = pos[2], options["pool"]
		return service.ReceiverAdd(ctx, config, options["bundle"])
	case "tenant add":
		if len(pos) != 3 {
			return errors.New("usage: wherry admin tenant add TENANT --pool POOL")
		}
		return service.TenantAdd(ctx, pos[2], options["pool"])
	case "host add":
		if len(pos) != 4 {
			return errors.New("usage: wherry admin host add TENANT HOST --bundle FILE")
		}
		return service.HostAdd(ctx, pos[2], pos[3], options["bundle"])
	case "revoke host", "revoke receiver":
		wait, err := time.ParseDuration(defaultValue(options["wait"], "0s"))
		if err != nil {
			return err
		}
		topic := admin.ReceiverTopic(pos[len(pos)-1])
		if pos[1] == "host" {
			topic = admin.HostTopic(pos[2], pos[3])
		}
		probe := service.Probe(topic)
		var result admin.RevocationResult
		if pos[1] == "host" && len(pos) == 4 {
			result = service.RevokeHost(ctx, pos[2], pos[3], wait, probe)
		} else if pos[1] == "receiver" && len(pos) == 3 {
			result = service.RevokeReceiver(ctx, pos[2], wait, probe)
		} else {
			return errors.New("invalid revoke command")
		}
		for _, step := range result.Steps {
			fmt.Fprintf(stdout, "step=%d complete=%t name=%s\n", step.Number, step.Complete, step.Name)
		}
		if !result.Complete {
			return errors.New("revocation incomplete")
		}
		return nil
	case "skip":
		if len(pos) != 2 || options["sha256"] == "" {
			return errors.New("usage: wherry admin skip OFFSET --sha256 HASH")
		}
		offset, err := strconv.ParseInt(pos[1], 10, 64)
		if err != nil {
			return err
		}
		return service.Skip(ctx, offset, options["sha256"])
	case "check":
		if len(pos) != 1 {
			return errors.New("usage: wherry admin check")
		}
		return service.Check(ctx)
	default:
		return errors.New("unknown admin command")
	}
}

func adminArgs(args []string) ([]string, map[string]string, error) {
	var positional []string
	options := map[string]string{}
	for i := 0; i < len(args); i++ {
		if !strings.HasPrefix(args[i], "--") {
			positional = append(positional, args[i])
			continue
		}
		if i+1 == len(args) {
			return nil, nil, errors.New("admin option requires a value")
		}
		options[strings.TrimPrefix(args[i], "--")] = args[i+1]
		i++
	}
	return positional, options, nil
}

func readYAML(path string, value any) error {
	if path == "" {
		return errors.New("required file option is missing")
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(encoded))
	decoder.KnownFields(true)
	return decoder.Decode(value)
}

func adminKafka(userEnv, passwordEnv string) (*kgo.Client, error) {
	broker, caPath, user, passwordPath := os.Getenv("WHERRY_TRUST_BROKER"), os.Getenv("WHERRY_TRUST_CA"), os.Getenv(userEnv), os.Getenv(passwordEnv)
	if broker == "" || caPath == "" || user == "" || passwordPath == "" {
		return nil, errors.New("trust broker credentials are not configured")
	}
	ca, err := os.ReadFile(caPath)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("trust CA contains no certificates")
	}
	password, err := os.ReadFile(passwordPath)
	if err != nil {
		return nil, err
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.DialTLSConfig(&tls.Config{RootCAs: roots}), kgo.SASL(scram.Auth{User: user, Pass: strings.TrimRight(string(password), "\r\n")}.AsSha512Mechanism()), kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	return client, err
}

func defaultValue(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
