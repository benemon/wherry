// Command testplugin is a fake adapter plugin for the protocol's tests and the receiver's round trip.
//
// WHERRY_TESTPLUGIN_RECORD names a file that gets one JSON line per Configure and per Dispatch, with the
// process ID and the value of WHERRY_TESTPLUGIN_ENV. WHERRY_TESTPLUGIN_PLAINTEXT=1 makes the plugin ignore
// the receiver's AutoMTLS certificate.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"slices"
	"sync"
	"syscall"

	"github.com/benemon/wherry/adapter"
	"github.com/hashicorp/go-hclog"
)

var lines = []string{`{"type":"assistant","message":"working"}`, `{"type":"result","result":"done","is_error":false}`}

// outputs are what Collect streams: a file over one message of the link, an empty file and a link it did not read. A
// regular file the limits name for streaming goes as a Body.
var outputs = []adapter.Output{
	{Name: "big.bin", Size: 2<<20 + 1, Data: bytes.Repeat([]byte("b"), 2<<20+1)},
	{Name: "empty", Size: 0, Data: []byte{}},
	{Name: "link", Size: 4, Mode: fs.ModeSymlink},
}

type config struct {
	Panic   string `json:"panic"`
	Fail    bool   `json:"fail"`
	Outputs bool   `json:"outputs"`
	// Finish makes the job end after its lines and leave an outcome file, as an agent that completed does.
	Finish      bool  `json:"finish"`
	InlineLimit int64 `json:"inline_limit"`
	// TooLarge names the input Dispatch refuses.
	TooLarge string `json:"too_large"`
	// NeverStarted is the reason Follow ends the job with, as one that never started.
	NeverStarted string `json:"never_started"`
	// SweepError is the text of the adapter.Error Sweep returns.
	SweepError string `json:"sweep_error"`
	// Log is a value Configure logs, through its logger and as a raw line on file descriptor 2, which go-plugin relays as
	// it relays a panic; os.Stderr goes to plugin.Serve's stdio stream instead.
	Log string `json:"log"`
	// LogCredential makes Configure write Settings.Credential through its logger, on stdout and as a raw line on file
	// descriptor 2, then fail with an error that holds it.
	LogCredential bool `json:"log_credential"`
	// Jobs makes the plugin keep each run's job as a platform does, for the conformance suite: Lookup finds it, Follow
	// and Stop end it, Purge removes it, and Collect finds no outputs in it unless Outputs is set.
	Jobs bool `json:"jobs"`
	// Hang makes Ready stop the process without it exiting, so it answers nothing more, go-plugin's pings included.
	Hang bool `json:"hang"`
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
	jobsMu   sync.Mutex
	jobs     map[string]*adapter.Job
}

// end marks the job of a run ended, when the plugin keeps jobs.
func (f *fake) end(runID string) {
	f.jobsMu.Lock()
	defer f.jobsMu.Unlock()
	if job := f.jobs[runID]; job != nil && job.State == adapter.JobRunning {
		job.State = adapter.JobEnded
	}
}

func (f *fake) panics(method string) {
	if f.config.Panic == method {
		panic(method)
	}
}

func (f *fake) Directories() adapter.Directories {
	return adapter.Directories{Input: "/fake/inputs", Output: "/fake/outputs", Credentials: "/fake/credentials"}
}

func (f *fake) InlineLimit() int64 { return f.config.InlineLimit }

func (f *fake) Ready(context.Context) (bool, error) {
	f.panics("Ready")
	if f.config.Hang {
		// SIGSTOP reaches the process's threads asynchronously, so this one could answer first; it never does.
		_ = syscall.Kill(os.Getpid(), syscall.SIGSTOP)
		select {}
	}
	return true, nil
}

func (f *fake) Dispatch(_ context.Context, run adapter.Run) (adapter.Outcome, adapter.Handle, error) {
	f.panics("Dispatch")
	write(record{Call: "dispatch", Settings: adapter.Settings{Name: f.settings.Name}, Run: &run})
	if f.config.TooLarge != "" {
		return adapter.OutcomeNothingCreated, adapter.Handle{}, &adapter.InputTooLargeError{Name: f.config.TooLarge}
	}
	handle := adapter.Handle{Data: []byte(run.ID), Name: f.settings.Name + "/" + run.ID}
	if f.config.Jobs {
		f.jobsMu.Lock()
		defer f.jobsMu.Unlock()
		if f.jobs[run.ID] == nil {
			state := adapter.JobRunning
			if f.config.NeverStarted != "" {
				state = adapter.JobNeverStarted
			}
			f.jobs[run.ID] = &adapter.Job{State: state, Handle: handle, Attempt: run.Attempt, Profile: run.Profile, Agent: run.Agent}
		}
		return adapter.OutcomeCreated, f.jobs[run.ID].Handle, nil
	}
	return adapter.OutcomeCreated, handle, nil
}

func (f *fake) Lookup(_ context.Context, runID string) (adapter.Job, error) {
	f.panics("Lookup")
	f.jobsMu.Lock()
	defer f.jobsMu.Unlock()
	if job := f.jobs[runID]; job != nil {
		return *job, nil
	}
	return adapter.Job{}, nil
}

// Follow emits the lines from offset from, then holds the stream open like a running job unless it finishes.
func (f *fake) Follow(ctx context.Context, handle adapter.Handle, from int64, out chan<- adapter.Line) (adapter.Terminal, error) {
	f.panics("Follow")
	if f.config.NeverStarted != "" {
		return adapter.Terminal{Class: "validation", Node: "fake-node", Reason: f.config.NeverStarted}, nil
	}
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
	if f.config.Finish {
		f.end(string(handle.Data))
		return adapter.Terminal{Started: true, Node: "fake-node"}, nil
	}
	<-ctx.Done()
	return adapter.Terminal{}, ctx.Err()
}

func (f *fake) Stop(_ context.Context, handle adapter.Handle) error {
	f.panics("Stop")
	f.end(string(handle.Data))
	return nil
}

func (f *fake) Collect(ctx context.Context, _ adapter.Handle, limits adapter.Limits, out chan<- adapter.Output) error {
	f.panics("Collect")
	if f.config.Jobs && !f.config.Outputs {
		return nil
	}
	if !f.config.Outputs {
		return errors.New("no outputs configured")
	}
	collected := outputs
	if f.config.Finish {
		outcome := []byte(`{"state":"TASK_STATE_COMPLETED","message":{"role":"ROLE_AGENT","parts":[{"text":"done"}]}}`)
		collected = append([]adapter.Output{{Name: ".wherry/outcome.json", Size: int64(len(outcome)), Data: outcome}}, outputs...)
	}
	for _, output := range collected {
		if output.Data != nil && slices.Contains(limits.Stream, output.Name) {
			output.Body, output.Data = io.NopCloser(bytes.NewReader(output.Data)), nil
		}
		select {
		case out <- output:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (f *fake) Purge(_ context.Context, runID string) error {
	f.panics("Purge")
	f.jobsMu.Lock()
	defer f.jobsMu.Unlock()
	delete(f.jobs, runID)
	return nil
}

func (f *fake) Release(context.Context, string) error {
	f.panics("Release")
	return nil
}

func (f *fake) Sweep(context.Context) error {
	f.panics("Sweep")
	if f.config.SweepError != "" {
		return &adapter.Error{Op: "list jobs", Class: "denied", Err: errors.New(f.config.SweepError)}
	}
	return nil
}

// Metrics reports a gauge labelled with the profile's name and a histogram.
func (f *fake) Metrics(context.Context) ([]adapter.Sample, error) {
	f.panics("Metrics")
	return []adapter.Sample{
		{Family: "wherry_adapter_cleanup_pending", Labels: map[string]string{"profile": f.settings.Name}, Kind: adapter.KindGauge, Value: 2},
		{Family: "wherry_adapter_placement_seconds", Kind: adapter.KindHistogram, Histogram: adapter.Histogram{Buckets: []uint64{0, 1, 2}, Sum: 3.5, Count: 2}},
	}, nil
}

func main() {
	if os.Getenv("WHERRY_TESTPLUGIN_PLAINTEXT") == "1" {
		os.Unsetenv("PLUGIN_CLIENT_CERT")
	}
	adapter.Serve(func(settings adapter.Settings, logger hclog.Logger) (adapter.Adapter, error) {
		write(record{Call: "configure", Settings: settings})
		logger.Info("configuring profile", "profile", settings.Name)
		var decoded config
		if len(settings.Config) > 0 {
			if err := json.Unmarshal(settings.Config, &decoded); err != nil {
				return nil, err
			}
		}
		if decoded.Fail {
			return nil, errors.New("configured to fail")
		}
		if decoded.Log != "" {
			logger.Info("configured", "log", decoded.Log)
			fmt.Fprintln(os.NewFile(2, "stderr"), "[ERROR] raw "+decoded.Log)
		}
		if decoded.LogCredential {
			logger.Info("configured", "credential", settings.Credential)
			fmt.Println("[INFO] stdout " + settings.Credential)
			fmt.Fprintln(os.NewFile(2, "stderr"), "[ERROR] raw "+settings.Credential)
			return nil, errors.New("refused credential " + settings.Credential)
		}
		return &fake{settings: settings, config: decoded, jobs: map[string]*adapter.Job{}}, nil
	})
}
