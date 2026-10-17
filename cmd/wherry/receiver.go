package main

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/benemon/wherry/internal/adapter/nomad"
	"github.com/benemon/wherry/internal/receiver"
)

func runReceiver(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("receiver", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "receiver configuration file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *configPath == "" {
		return errors.New("usage: wherry receiver --config PATH")
	}
	config, err := receiver.LoadConfig(*configPath)
	if err != nil {
		return err
	}
	adapters, err := newAdapters(config.Adapters, os.Getenv("NOMAD_TOKEN"))
	if err != nil {
		return err
	}
	logger := log.New(stdout, "receiver: ", log.LstdFlags|log.LUTC)
	r, err := receiver.New(config, adapters, logger)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := r.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func newAdapters(configs []receiver.AdapterConfig, nomadToken string) ([]receiver.Named, error) {
	adapters := make([]receiver.Named, 0, len(configs))
	for _, config := range configs {
		switch config.Type {
		case "nomad":
			var adapterConfig nomad.Config
			if err := json.Unmarshal(config.Config, &adapterConfig, json.RejectUnknownMembers(true)); err != nil {
				return nil, fmt.Errorf("decode adapter %s config: %w", config.Name, err)
			}
			if config.Credential != "" && adapterConfig.CredentialEnv == "" {
				return nil, fmt.Errorf("adapter %s has a credential but no credential_env", config.Name)
			}
			adapter, err := nomad.New(adapterConfig, nomadToken)
			if err != nil {
				return nil, fmt.Errorf("adapter %s: %w", config.Name, err)
			}
			adapters = append(adapters, receiver.Named{Name: config.Name, Credential: config.Credential, Adapter: adapter})
		default:
			return nil, fmt.Errorf("adapter %s has unknown type %q", config.Name, config.Type)
		}
	}
	return adapters, nil
}
