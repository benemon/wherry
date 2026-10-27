package main

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"maps"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/receiver"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// The defaults of the receiver's local configuration; the docs and the receiver image install plugins in
// defaultPluginDir.
const (
	defaultReceiverConfig = "/etc/wherry/config.yaml"
	defaultPluginDir      = "/usr/local/libexec/wherry/plugins"
)

// receiverLocalConfig is the receiver's local configuration file. Credentials maps the name a profile's config gives
// its platform credential to the source of its value.
type receiverLocalConfig struct {
	Bundle      string                       `yaml:"bundle"`
	StateDir    string                       `yaml:"state_dir"`
	PluginDir   string                       `yaml:"plugin_dir"`
	MetricsAddr string                       `yaml:"metrics_addr"`
	Credentials map[string]credential.Source `yaml:"credentials"`
}

// loadReceiverLocalConfig decodes the receiver's local configuration at path, defaulting state_dir and plugin_dir.
func loadReceiverLocalConfig(path string) (receiverLocalConfig, error) {
	path, err := client.ExpandHome(path)
	if err != nil {
		return receiverLocalConfig{}, err
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return receiverLocalConfig{}, err
	}
	var config receiverLocalConfig
	if err := admin.DecodeYAML(encoded, &config); err != nil {
		return receiverLocalConfig{}, fmt.Errorf("decode config: %w", err)
	}
	if config.Bundle == "" {
		return receiverLocalConfig{}, errors.New("config requires bundle")
	}
	for _, path := range []*string{&config.Bundle, &config.StateDir, &config.PluginDir} {
		if *path, err = client.ExpandHome(*path); err != nil {
			return receiverLocalConfig{}, err
		}
	}
	if config.StateDir == "" {
		config.StateDir = filepath.Join(filepath.Dir(config.Bundle), "state")
	}
	if config.PluginDir == "" {
		config.PluginDir = defaultPluginDir
	}
	for name, source := range config.Credentials {
		if err := source.Validate(); err != nil {
			return receiverLocalConfig{}, fmt.Errorf("config credential %s: %w", name, err)
		}
		if source.File, err = client.ExpandHome(source.File); err != nil {
			return receiverLocalConfig{}, err
		}
		config.Credentials[name] = source
	}
	return config, nil
}

func runReceiver(args []string, stdout, stderr io.Writer) error {
	const usage = "usage: wherry receiver run [--config-file FILE] [--rotate-key]"
	if len(args) == 0 || args[0] != "run" {
		return errors.New(usage)
	}
	flags := flag.NewFlagSet("receiver run", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config-file", defaultReceiverConfig, "the receiver's local configuration file")
	rotateKey := flags.Bool("rotate-key", false, "replace the receiver's signing key at start, before it claims anything")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New(usage)
	}
	config, err := loadReceiverLocalConfig(*configPath)
	if err != nil {
		return err
	}
	bundle, err := admin.ReadBundle(config.Bundle)
	if err != nil {
		return err
	}
	logger := log.New(stdout, "receiver: ", log.LstdFlags|log.LUTC)
	registry := newRegistry()
	if config.MetricsAddr != "" {
		listener, err := net.Listen("tcp", config.MetricsAddr)
		if err != nil {
			return fmt.Errorf("metrics address: %w", err)
		}
		defer serveMetrics(listener, registry, logger).Close()
	}
	plugins := &plugins{dir: config.PluginDir, credentials: config.Credentials, registry: registry, running: map[string]*pluginsdk.Process{}}
	defer plugins.close()
	r, err := receiver.New(bundle, config.StateDir, *rotateKey, plugins.profile, logger, registry)
	if err != nil {
		return err
	}
	plugins.logs, plugins.hold = r.PluginLogs(stdout), r.HoldPlatformCredential
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := r.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// newRegistry is the receiver process's registry, with the Go runtime and process collectors and wherry_build_info.
func newRegistry() *prometheus.Registry {
	version, revision := "(devel)", ""
	if info, ok := debug.ReadBuildInfo(); ok {
		version = info.Main.Version
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				revision = setting.Value
			}
		}
	}
	registry := prometheus.NewRegistry()
	buildInfo := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "wherry_build_info", Help: "The wherry binary's version and revision.", ConstLabels: prometheus.Labels{"version": version, "revision": revision},
	})
	buildInfo.Set(1)
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}), buildInfo)
	return registry
}

// serveMetrics serves registry at /metrics, and nothing else, on listener over plain HTTP; logger gets gather errors.
func serveMetrics(listener net.Listener, registry *prometheus.Registry, logger *log.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{ErrorLog: logger, ErrorHandling: promhttp.ContinueOnError}))
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go server.Serve(listener)
	return server
}

// plugins runs one process per plugin, started for the first profile that names it. hold takes each platform
// credential value before any plugin is handed it, so the receiver redacts it from what logs gets.
type plugins struct {
	dir         string
	credentials map[string]credential.Source
	hold        func(name string, value []byte)
	logs        io.Writer
	registry    prometheus.Registerer
	running     map[string]*pluginsdk.Process
}

// profile configures one profile in its type's plugin, handing the plugin the value of the credential its config names
// in place of the name.
func (p *plugins) profile(pool string, settings admin.ProfileSettings) (receiver.Named, error) {
	config := maps.Clone(settings.Config)
	var value []byte
	if named, ok := config["credential"]; ok {
		delete(config, "credential")
		name, _ := named.(string)
		source, ok := p.credentials[name]
		if !ok {
			return receiver.Named{}, fmt.Errorf("credential %q has no source in the receiver's configuration", name)
		}
		var err error
		if value, err = source.Resolve(context.Background()); err != nil {
			return receiver.Named{}, fmt.Errorf("resolve credential %s: %w", name, err)
		}
		p.hold(name, value)
	}
	running, ok := p.running[settings.Plugin]
	if !ok {
		var err error
		running, err = pluginsdk.Launch(func() (string, error) { return pluginPath(p.dir, settings.Plugin) }, p.logs)
		if err != nil {
			return receiver.Named{}, err
		}
		p.running[settings.Plugin] = running
		p.registry.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "wherry_plugin_restarts_total", Help: "Times a plugin was started again.", ConstLabels: prometheus.Labels{"pool": pool, "type": settings.Plugin},
		}, func() float64 { return float64(running.Restarts()) }))
	}
	encoded, err := json.Marshal(config)
	if err != nil {
		return receiver.Named{}, err
	}
	// A value read from a file ends in the newline its editor wrote, which no platform token holds.
	configured, err := running.Configure(pluginsdk.Settings{
		Name: settings.Name, Pool: pool, Agent: settings.Agent, Config: encoded, Credential: strings.TrimRight(string(value), "\r\n"),
	})
	if err != nil {
		return receiver.Named{}, err
	}
	return receiver.Named{Name: settings.Name, Agent: settings.Agent, Labels: settings.Labels, Plugin: configured}, nil
}

func (p *plugins) close() {
	for _, running := range p.running {
		running.Close()
	}
}

// receiverUID is the user besides root that may own the plugin directory and binaries; a variable so
// tests can stand in for another owner.
var receiverUID = os.Getuid()

// pluginPath is the absolute path of the plugin named kind in dir. Whoever can write the
// directory or the binary runs code as the receiver, so it refuses either when group or others can
// write it or a user other than root or the receiver's owns it, and a binary that is not a regular
// file. An absolute path is never looked up on PATH, so the binary checked is the one run.
func pluginPath(dir, kind string) (string, error) {
	if !wire.ValidName(kind) {
		return "", fmt.Errorf("plugin %q is not a valid name", kind)
	}
	dir, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("plugin directory: %w", err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		return "", fmt.Errorf("plugin directory: %w", err)
	}
	if !info.IsDir() || !ownerOnly(info) {
		return "", fmt.Errorf("plugin directory %s is not a directory that only root or the receiver's user owns and can write", dir)
	}
	path := filepath.Join(dir, "wherry-plugin-"+kind)
	info, err = os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("no plugin for type %s", kind)
	}
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || !ownerOnly(info) {
		return "", fmt.Errorf("plugin %s is not a regular file that only root or the receiver's user owns and can write", path)
	}
	return path, nil
}

// ownerOnly reports that root or the receiver's user owns info and that neither group nor others can write it.
func ownerOnly(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && (stat.Uid == 0 || int(stat.Uid) == receiverUID) && info.Mode().Perm()&0o022 == 0
}
