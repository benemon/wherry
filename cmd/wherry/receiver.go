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
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/benemon/wherry/adapter"
	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/receiver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/yaml.v3"
)

func runReceiver(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("receiver", flag.ContinueOnError)
	flags.SetOutput(stderr)
	bundlePath := flags.String("bundle-file", "", "receiver bundle file")
	stateDir := flags.String("state-dir", "", "directory holding the receiver's keys")
	pluginDir := flags.String("plugin-dir", "", "directory holding the adapter plugins, wherry-plugin-<type>")
	metricsAddr := flags.String("metrics-addr", "", "HOST:PORT to serve /metrics on, off when empty")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *bundlePath == "" || *stateDir == "" || *pluginDir == "" {
		return errors.New("usage: wherry receiver --bundle-file FILE --state-dir DIR --plugin-dir DIR")
	}
	bundle, err := readBundle(*bundlePath)
	if err != nil {
		return err
	}
	logger := log.New(stdout, "receiver: ", log.LstdFlags|log.LUTC)
	registry := newRegistry()
	if *metricsAddr != "" {
		listener, err := net.Listen("tcp", *metricsAddr)
		if err != nil {
			return fmt.Errorf("metrics address: %w", err)
		}
		defer serveMetrics(listener, registry, logger).Close()
	}
	plugins := &plugins{dir: *pluginDir, logs: stdout, registry: registry, running: map[string]*adapter.Plugin{}}
	defer plugins.close()
	r, err := receiver.New(bundle, *stateDir, func(pool string, settings admin.AdapterSettings) (receiver.Named, error) {
		return plugins.adapter(pool, settings, bundle.Platform[settings.Name])
	}, logger, registry)
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

// plugins runs one plugin process per adapter type, started for the first adapter of that type.
type plugins struct {
	dir      string
	logs     io.Writer
	registry prometheus.Registerer
	running  map[string]*adapter.Plugin
}

// adapter configures one adapter in its type's plugin; platform is the bundle's platform credential for it.
func (p *plugins) adapter(pool string, settings admin.AdapterSettings, platform string) (receiver.Named, error) {
	running, ok := p.running[settings.Type]
	if !ok {
		var err error
		running, err = adapter.Launch(func() (string, error) { return pluginPath(p.dir, settings.Type) }, p.logs)
		if err != nil {
			return receiver.Named{}, err
		}
		p.running[settings.Type] = running
		p.registry.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "wherry_plugin_restarts_total", Help: "Times an adapter plugin was started again.", ConstLabels: prometheus.Labels{"pool": pool, "type": settings.Type},
		}, func() float64 { return float64(running.Restarts()) }))
	}
	config, err := json.Marshal(settings.Config)
	if err != nil {
		return receiver.Named{}, err
	}
	configured, err := running.Configure(adapter.Settings{Name: settings.Name, Pool: pool, Agent: settings.Agent, Credential: settings.Credential, Config: config, Platform: platform})
	if err != nil {
		return receiver.Named{}, err
	}
	return receiver.Named{Name: settings.Name, Agent: settings.Agent, Credential: settings.Credential, Labels: settings.Labels, Adapter: configured}, nil
}

func (p *plugins) close() {
	for _, running := range p.running {
		running.Close()
	}
}

// receiverUID is the user besides root that may own the plugin directory and binaries; a variable so
// tests can stand in for another owner.
var receiverUID = os.Getuid()

// pluginPath is the absolute path of the plugin for an adapter type in dir. Whoever can write the
// directory or the binary runs code as the receiver, so it refuses either when group or others can
// write it or a user other than root or the receiver's owns it, and a binary that is not a regular
// file. An absolute path is never looked up on PATH, so the binary checked is the one run.
func pluginPath(dir, kind string) (string, error) {
	if !admin.ValidName(kind) {
		return "", fmt.Errorf("adapter type %q is not a valid name", kind)
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
