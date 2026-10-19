package main

import (
	"bytes"
	"context"
	json "encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"

	"github.com/benemon/wherry/internal/adapter/nomad"
	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/receiver"
	"gopkg.in/yaml.v3"
)

func runReceiver(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("receiver", flag.ContinueOnError)
	flags.SetOutput(stderr)
	bundlePath := flags.String("bundle", "", "receiver bundle file")
	stateDir := flags.String("state-dir", "", "directory holding the receiver's keys")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *bundlePath == "" || *stateDir == "" {
		return errors.New("usage: wherry receiver --bundle FILE --state-dir DIR")
	}
	bundle, err := readBundle(*bundlePath)
	if err != nil {
		return err
	}
	logger := log.New(stdout, "receiver: ", log.LstdFlags|log.LUTC)
	r, err := receiver.New(bundle, *stateDir, func(pool string, settings admin.AdapterSettings) (receiver.Named, error) {
		return newAdapter(pool, settings, platformToken(bundle, settings.Name))
	}, logger)
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

func readBundle(path string) (admin.Bundle, error) {
	encoded, err := os.ReadFile(path)
	if err != nil {
		return admin.Bundle{}, err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(encoded))
	decoder.KnownFields(true)
	var bundle admin.Bundle
	if err := decoder.Decode(&bundle); err != nil {
		// The decoder quotes keys and values, and a credential pasted in the wrong place is still a
		// credential: only the line numbers it names are kept.
		lines := strings.Join(yamlLine.FindAllString(err.Error(), -1), ", ")
		if lines == "" {
			return admin.Bundle{}, errors.New("decode bundle: not a valid bundle")
		}
		return admin.Bundle{}, fmt.Errorf("decode bundle: invalid or unknown field at %s", lines)
	}
	return bundle, nil
}

var yamlLine = regexp.MustCompile(`line \d+`)

// platformToken is the bundle's platform credential for an adapter, or, for a platform that issues
// the receiver's identity itself as Nomad workload identity does, NOMAD_TOKEN from the environment.
func platformToken(bundle admin.Bundle, name string) string {
	if token, ok := bundle.Platform[name]; ok {
		return token
	}
	return os.Getenv("NOMAD_TOKEN")
}

// newAdapter builds one configured adapter; token is the bundle's platform credential for its name.
func newAdapter(pool string, settings admin.AdapterSettings, token string) (receiver.Named, error) {
	switch settings.Type {
	case "nomad":
		encoded, err := json.Marshal(settings.Config)
		if err != nil {
			return receiver.Named{}, err
		}
		var config nomad.Config
		if err := json.Unmarshal(encoded, &config, json.RejectUnknownMembers(true)); err != nil {
			return receiver.Named{}, fmt.Errorf("decode adapter %s config: %w", settings.Name, err)
		}
		if settings.Credential != "" && config.CredentialEnv == "" {
			return receiver.Named{}, fmt.Errorf("adapter %s has a credential but no credential_env", settings.Name)
		}
		adapter, err := nomad.New(config, pool, token)
		if err != nil {
			return receiver.Named{}, fmt.Errorf("adapter %s: %w", settings.Name, err)
		}
		return receiver.Named{Name: settings.Name, Agent: settings.Agent, Credential: settings.Credential, Labels: settings.Labels, Git: true, Adapter: adapter}, nil
	default:
		return receiver.Named{}, fmt.Errorf("adapter %s has unknown type %q", settings.Name, settings.Type)
	}
}
