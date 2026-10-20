// Command testplugin is a fake adapter plugin for the protocol's tests and the receiver's round trip.
//
// WHERRY_TESTPLUGIN_RECORD names a file that gets one JSON line per Configure and per Dispatch, with the
// process ID and the value of WHERRY_TESTPLUGIN_ENV. WHERRY_TESTPLUGIN_PLAINTEXT=1 makes the plugin
// ignore the receiver's AutoMTLS certificate. An adapter's config may name a method that panics
// ({"panic": "Sweep"}), fail Configure ({"fail": true}), deliver git context ({"git": true}), refuse an input
// as too large ({"too_large": "name"}) or collect the fixed outputs ({"outputs": true}).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"sync"

	"github.com/benemon/wherry/adapter"
)

var lines = []string{`{"type":"assistant","message":"working"}`, `{"type":"result","result":"done","is_error":false}`}

// outputs are what Collect streams: a file over one message of the link, an empty file and a link it did not read.
var outputs = []adapter.Output{
	{Name: "big.bin", Size: 2<<20 + 1, Data: bytes.Repeat([]byte("b"), 2<<20+1)},
	{Name: "empty", Size: 0, Data: []byte{}},
	{Name: "link", Size: 4, Mode: fs.ModeSymlink},
}

type config struct {
	Panic   string `json:"panic"`
	Fail    bool   `json:"fail"`
	Git     bool   `json:"git"`
	Outputs bool   `json:"outputs"`
	// TooLarge names the input Dispatch refuses.
	TooLarge string `json:"too_large"`
}

type record struct {
	PID      int              `json:"pid"`
	Env      string           `json:"env"`
	Call     string           `json:"call"`
	Settings adapter.Settings `json:"settings"`
	Run      *adapter.Run     `json:"run,omitempty"`
}

var mu sync.Mutex

func write(entry record) {
	path := os.Getenv("WHERRY_TESTPLUGIN_RECORD")
	if path == "" {
		return
	}
	entry.PID, entry.Env = os.Getpid(), os.Getenv("WHERRY_TESTPLUGIN_ENV")
	encoded, _ := json.Marshal(entry)
	mu.Lock()
	defer mu.Unlock()
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer file.Close()
	_, _ = file.Write(append(encoded, '\n'))
}

type fake struct {
	settings adapter.Settings
	config   config
}

func (f *fake) panics(method string) {
	if f.config.Panic == method {
		panic(method)
	}
}

func (f *fake) Git() bool { return f.config.Git }

func (f *fake) Directories() adapter.Directories {
	return adapter.Directories{Input: "/fake/inputs", Output: "/fake/outputs", Credentials: "/fake/credentials"}
}

func (f *fake) Ready(context.Context) (bool, error) {
	f.panics("Ready")
	return true, nil
}

func (f *fake) Dispatch(_ context.Context, run adapter.Run) (adapter.Outcome, adapter.Handle, error) {
	f.panics("Dispatch")
	write(record{Call: "dispatch", Settings: adapter.Settings{Name: f.settings.Name}, Run: &run})
	if f.config.TooLarge != "" {
		return adapter.OutcomeNothingCreated, adapter.Handle{}, &adapter.InputTooLargeError{Name: f.config.TooLarge}
	}
	return adapter.OutcomeCreated, adapter.Handle{Data: []byte(run.ID), Name: f.settings.Name + "/" + run.ID}, nil
}

func (f *fake) Lookup(context.Context, string, int64) (adapter.Job, error) {
	f.panics("Lookup")
	return adapter.Job{}, nil
}

// Follow emits the lines from offset from, then holds the stream open like a running job.
func (f *fake) Follow(ctx context.Context, _ adapter.Handle, from int64, out chan<- adapter.Line) (adapter.Terminal, error) {
	f.panics("Follow")
	var offset int64
	for _, data := range lines {
		next := offset + int64(len(data)+1)
		if offset >= from {
			select {
			case out <- adapter.Line{Offset: offset, Next: next, Data: []byte(data)}:
			case <-ctx.Done():
				return adapter.Terminal{}, ctx.Err()
			}
		}
		offset = next
	}
	<-ctx.Done()
	return adapter.Terminal{}, ctx.Err()
}

func (f *fake) Stop(context.Context, adapter.Handle) error {
	f.panics("Stop")
	return nil
}

func (f *fake) Collect(ctx context.Context, _ adapter.Handle, _ adapter.Limits, out chan<- adapter.Output) error {
	f.panics("Collect")
	if !f.config.Outputs {
		return errors.New("no outputs configured")
	}
	for _, output := range outputs {
		select {
		case out <- output:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (f *fake) Purge(context.Context, string, int64) error {
	f.panics("Purge")
	return nil
}

func (f *fake) Release(context.Context, string, int64) error {
	f.panics("Release")
	return nil
}

func (f *fake) Sweep(context.Context) error {
	f.panics("Sweep")
	return nil
}

func main() {
	if os.Getenv("WHERRY_TESTPLUGIN_PLAINTEXT") == "1" {
		os.Unsetenv("PLUGIN_CLIENT_CERT")
	}
	adapter.Serve(func(settings adapter.Settings) (adapter.Adapter, error) {
		write(record{Call: "configure", Settings: settings})
		var decoded config
		if len(settings.Config) > 0 {
			if err := json.Unmarshal(settings.Config, &decoded); err != nil {
				return nil, err
			}
		}
		if decoded.Fail {
			return nil, errors.New("configured to fail")
		}
		return &fake{settings: settings, config: decoded}, nil
	})
}
