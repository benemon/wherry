package main

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	json "encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"maps"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/receiver"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// The defaults of the receiver's local configuration; the docs and the receiver image install plugins in
// defaultPluginDir. defaultBundle is the Kubernetes example's Secret mount, and defaultStateDir the examples' volume.
const (
	defaultReceiverConfig = "/etc/wherry/config.yaml"
	defaultPluginDir      = "/usr/local/libexec/wherry/plugins"
	defaultBundle         = "/etc/wherry/bundle/bundle.yaml"
	defaultStateDir       = "/var/lib/wherry/state"
	defaultMetricsAddr    = ":9464"
	defaultDrainTimeout   = 20 * time.Second
)

// inNomadTask reports whether the receiver runs inside a Nomad task, by $NOMAD_SECRETS_DIR, which Nomad sets in a task
// alone. It is looked at before systemd's variables: a Nomad exec task inherits the Nomad client's environment, and a
// client run as a systemd unit with LoadCredential= or StateDirectory= passes $CREDENTIALS_DIRECTORY and
// $STATE_DIRECTORY on, which name the client's directories, not the task's.
func inNomadTask() bool {
	return os.Getenv("NOMAD_SECRETS_DIR") != ""
}

// platformBundle is where the receiver's platform puts its bundle: $NOMAD_SECRETS_DIR/receiver.bundle inside a Nomad
// task, else $CREDENTIALS_DIRECTORY/bundle under systemd, else the Kubernetes Secret mount. The innermost platform
// decides, and a bundle missing at its path is refused naming the path; another platform's path is not tried.
func platformBundle() (string, error) {
	path := defaultBundle
	if inNomadTask() {
		path = filepath.Join(os.Getenv("NOMAD_SECRETS_DIR"), "receiver.bundle")
	} else if dir := os.Getenv("CREDENTIALS_DIRECTORY"); dir != "" {
		path = filepath.Join(dir, "bundle")
	}
	if _, err := os.Stat(path); err != nil {
		return "", fmt.Errorf("%w; put the receiver's bundle there, or name a configuration file with --config-file", err)
	}
	return path, nil
}

// platformStateDir is where the receiver keeps its state without a configured state_dir: defaultStateDir, the
// examples' volume, inside a Nomad task; else state under the first of systemd's $STATE_DIRECTORY; else defaultStateDir.
func platformStateDir() string {
	if dirs := os.Getenv("STATE_DIRECTORY"); dirs != "" && !inNomadTask() {
		first, _, _ := strings.Cut(dirs, ":")
		return filepath.Join(first, "state")
	}
	return defaultStateDir
}

// filelessCA is where a receiver without a file sets ca_cert, for the hint on an unknown authority: a new file at the
// default path, which then gives every field a file's meaning, so the hint names what else it must hold for the
// receiver to go on as it did, bundle, found at the platform's path, among them should it move.
func filelessCA(bundle string) operator.CASetting {
	return operator.CASetting{File: defaultReceiverConfig, Also: "a new file there gives every field a file's meaning, so set metrics_addr: \":9464\" and drain_timeout: 20s in it too, and bundle unless it stays at " + bundle}
}

// filelessConfig is the receiver's configuration without a file: the bundle and state directory where its platform
// puts them, the default plugin directory, a 20s drain and metrics on :9464, and no credentials, ca_cert or tracing.
func filelessConfig() (receiverLocalConfig, error) {
	bundle, err := platformBundle()
	if err != nil {
		return receiverLocalConfig{}, err
	}
	return receiverLocalConfig{Bundle: bundle, StateDir: platformStateDir(), PluginDir: defaultPluginDir, MetricsAddr: defaultMetricsAddr, DrainTimeout: defaultDrainTimeout}, nil
}

// receiverLocalConfig is the receiver's local configuration file. Credentials maps the name a profile's config gives
// its platform credential to the source of its value. DrainTimeout bounds how long held runs may finish after a stop
// signal; it stays below the platform's stop window, which the receiver cannot read. Tracing writes the receiver's
// spans to the trace topics; off by default. CACert is the PEM of the CA the receiver's Kafka and S3 connections trust
// beside the machine's trust store; empty for the trust store alone.
type receiverLocalConfig struct {
	Bundle       string                       `yaml:"bundle"`
	StateDir     string                       `yaml:"state_dir"`
	PluginDir    string                       `yaml:"plugin_dir"`
	CACert       string                       `yaml:"ca_cert,omitempty"`
	MetricsAddr  string                       `yaml:"metrics_addr"`
	DrainTimeout time.Duration                `yaml:"drain_timeout"`
	Credentials  map[string]credential.Source `yaml:"credentials"`
	Tracing      bool                         `yaml:"tracing,omitempty"`
}

// loadReceiverLocalConfig reads the receiver's local configuration at path and fills the paths it leaves out: the
// bundle where the platform puts it, state_dir beside a named bundle or where the platform puts it, and plugin_dir
// the default.
func loadReceiverLocalConfig(path string) (receiverLocalConfig, *x509.CertPool, error) {
	config, roots, err := readReceiverLocalConfig(path)
	if err != nil {
		return receiverLocalConfig{}, nil, err
	}
	named := config.Bundle != ""
	if !named {
		if config.Bundle, err = platformBundle(); err != nil {
			return receiverLocalConfig{}, nil, err
		}
	}
	if config.StateDir == "" {
		config.StateDir = platformStateDir()
		if named {
			config.StateDir = filepath.Join(filepath.Dir(config.Bundle), "state")
		}
	}
	if config.PluginDir == "" {
		config.PluginDir = defaultPluginDir
	}
	return config, roots, nil
}

// readReceiverLocalConfig decodes and checks the receiver's local configuration at path, as apply --receiver does on the
// operator's machine, and builds the pool its ca_cert gives, the machine's trust store plus the CA, refusing a ca_cert
// from which Go appends no certificate; the pool is built here once and carried to the dial. The paths the file leaves
// out stay empty.
func readReceiverLocalConfig(path string) (receiverLocalConfig, *x509.CertPool, error) {
	path, err := client.ExpandHome(path)
	if err != nil {
		return receiverLocalConfig{}, nil, err
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return receiverLocalConfig{}, nil, err
	}
	if err := operator.RefuseCAFile(path, encoded); err != nil {
		return receiverLocalConfig{}, nil, err
	}
	var config receiverLocalConfig
	if err := operator.DecodeYAML(encoded, &config); err != nil {
		return receiverLocalConfig{}, nil, fmt.Errorf("decode config: %w", err)
	}
	roots, err := operator.ConfigRootPool(path, config.CACert)
	if err != nil {
		return receiverLocalConfig{}, nil, err
	}
	if config.DrainTimeout < 0 {
		return receiverLocalConfig{}, nil, errors.New("config drain_timeout must not be negative")
	}
	for _, path := range []*string{&config.Bundle, &config.StateDir, &config.PluginDir} {
		if *path, err = client.ExpandHome(*path); err != nil {
			return receiverLocalConfig{}, nil, err
		}
	}
	for name, source := range config.Credentials {
		if err := source.Validate(); err != nil {
			return receiverLocalConfig{}, nil, fmt.Errorf("config credential %s: %w", name, err)
		}
		if source.File, err = client.ExpandHome(source.File); err != nil {
			return receiverLocalConfig{}, nil, err
		}
		config.Credentials[name] = source
	}
	return config, roots, nil
}

func runReceiver(args []string, stdout, stderr io.Writer) error {
	const usage = "usage: wherry receiver <run|rotate --key> [--config-file FILE]"
	const rotateUsage = "usage: wherry receiver rotate --key [--config-file FILE]"
	if len(args) == 1 && slices.Contains([]string{"-h", "-help", "--help"}, args[0]) {
		fmt.Fprintln(stderr, usage)
		return flag.ErrHelp
	}
	if len(args) == 0 || (args[0] != "run" && args[0] != "rotate") {
		return errors.New(usage)
	}
	flags := flag.NewFlagSet("receiver "+args[0], flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config-file", defaultReceiverConfig, "the receiver's local configuration file")
	var key *bool
	if args[0] == "rotate" {
		key = flags.Bool("key", false, "replace the receiver's signing key")
	}
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New(usage)
	}
	if key != nil && !*key {
		return errors.New(rotateUsage)
	}
	// Without --config-file and without the default file, the platform's paths and the defaults configure the receiver.
	var config receiverLocalConfig
	var roots *x509.CertPool
	// Where ca_cert lives, for the hint on an unknown authority: the file read, or the new file a receiver without one
	// would read.
	var ca operator.CASetting
	if _, err := os.Stat(*configPath); !wasSet(flags, "config-file") && errors.Is(err, fs.ErrNotExist) {
		if config, err = filelessConfig(); err != nil {
			return err
		}
		ca = filelessCA(config.Bundle)
	} else if config, roots, err = loadReceiverLocalConfig(*configPath); err != nil {
		return err
	} else {
		ca = operator.CASetting{File: *configPath, Set: config.CACert != ""}
	}
	bundle, err := operator.ReadBundle(config.Bundle)
	if err != nil {
		return err
	}
	if key != nil {
		old, next, err := receiver.RotateKey(context.Background(), bundle, roots, config.StateDir)
		if err != nil {
			return operator.ExplainUnknownAuthority(err, ca)
		}
		fmt.Fprintf(stdout, "old_key_id=%s new_key_id=%s\n", old, next)
		return nil
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
	plugins := &plugins{dir: config.PluginDir, credentials: config.Credentials, logger: logger, running: map[string]*pluginsdk.Process{}}
	defer plugins.close()
	r, err := receiver.New(bundle, roots, config.StateDir, config.DrainTimeout, plugins.profile, logger, registry)
	if err != nil {
		return operator.ExplainUnknownAuthority(err, ca)
	}
	plugins.logs, plugins.hold = r.PluginLogs(stdout), r.HoldPlatformCredential
	if config.Tracing {
		r.Trace()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := r.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return operator.ExplainUnknownAuthority(err, ca)
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
	logger      *log.Logger
	running     map[string]*pluginsdk.Process
}

// profile configures one profile in its type's plugin, handing the plugin the value of the credential its config names
// in place of the name. The plugin starts only from a binary whose SHA-256 is pluginSHA256.
func (p *plugins) profile(pool string, settings operator.ProfileSettings, pluginSHA256 string) (receiver.Named, error) {
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
		checksum, err := hex.DecodeString(pluginSHA256)
		if err != nil || len(checksum) != sha256.Size {
			return receiver.Named{}, fmt.Errorf("plugin type %s has no pinned SHA-256; set plugin_sha256.%s in the receiver's file and run apply --receiver", settings.Plugin, settings.Plugin)
		}
		running, err = pluginsdk.Launch(settings.Plugin, func() (string, error) { return pluginPath(p.dir, settings.Plugin) }, checksum, p.logs)
		if err != nil {
			return receiver.Named{}, err
		}
		p.running[settings.Plugin] = running
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
	if !ok {
		version, protocol := running.Version()
		if version == "" {
			version = "unreported"
		}
		p.logger.Printf("plugin %s: version %s protocol %d", settings.Plugin, version, protocol)
	}
	return receiver.Named{Name: settings.Name, Agent: settings.Agent, Labels: settings.Labels, Plugin: configured, Exited: running.Exited()}, nil
}

func (p *plugins) close() {
	for _, running := range p.running {
		running.Close()
	}
}

// pluginPath is the absolute path, with every symlink resolved, of the plugin named kind in dir. go-plugin checks the
// file at that path against the pinned SHA-256 and runs it; an absolute path is never looked up on PATH.
func pluginPath(dir, kind string) (string, error) {
	if !wire.ValidName(kind) {
		return "", fmt.Errorf("plugin %q is not a valid name", kind)
	}
	dir, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("plugin directory: %w", err)
	}
	path, err := filepath.EvalSymlinks(filepath.Join(dir, "wherry-plugin-"+kind))
	if errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("no plugin for type %s", kind)
	}
	if err != nil {
		return "", err
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("plugin %s is not a regular file", path)
	}
	return path, nil
}
