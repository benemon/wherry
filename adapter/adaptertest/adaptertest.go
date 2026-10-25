// Package adaptertest is the conformance suite a plugin runs against its platform.
package adaptertest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"testing"
	"time"

	"github.com/benemon/wherry/adapter"
	"github.com/hashicorp/go-hclog"
)

const maxHandle = 4 << 10

// Credentials are the credentials the completing job's run carries, one at each kind of target.
var Credentials = []adapter.Secret{
	{Name: "conformance-env", Env: "CONFORMANCE_TOKEN", Value: []byte("conformance-env-value")},
	{Name: "conformance-file", File: "conformance-key", Value: []byte("conformance-file-value")},
}

// Run checks the adapters factory, the one a plugin hands Serve, builds against the adapter contract on their
// platform, giving it a logger that discards. completes configures an adapter whose job runs for at least ten seconds
// and exits 0; neverStarts one whose job the platform ends before its agent starts. Every job the suite creates is
// purged. inlineLimit is the cap both configured adapters report. The completing job's run carries Credentials.
func Run(t *testing.T, factory func(adapter.Settings, hclog.Logger) (adapter.Adapter, error), completes, neverStarts adapter.Settings, inlineLimit int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	running, err := factory(completes, hclog.NewNullLogger())
	if err != nil {
		t.Fatalf("configure %s: %v", completes.Name, err)
	}
	failing, err := factory(neverStarts, hclog.NewNullLogger())
	if err != nil {
		t.Fatalf("configure %s: %v", neverStarts.Name, err)
	}
	if running.InlineLimit() != inlineLimit || failing.InlineLimit() != inlineLimit {
		t.Fatalf("inline limits = %d and %d, want %d", running.InlineLimit(), failing.InlineLimit(), inlineLimit)
	}
	id := runID(t)

	t.Run("Ready", func(t *testing.T) {
		if ready, err := running.Ready(ctx); err != nil || !ready {
			t.Fatalf("ready = %v, %v; want the platform ready", ready, err)
		}
	})

	t.Run("Absent", func(t *testing.T) {
		job, err := running.Lookup(ctx, id+"-absent")
		if err != nil || job.State != adapter.JobAbsent {
			t.Fatalf("lookup of an unknown run = %+v, %v; want absent", job, err)
		}
		idempotent(t, ctx, running, id+"-absent", adapter.Handle{})
	})

	t.Run("Completes", func(t *testing.T) {
		run := adapter.Run{ID: id, Adapter: completes.Name, Agent: completes.Agent, Attempt: 3, Prompt: []byte("conformance prompt"), Credentials: Credentials}
		purgeAfter(t, running, run)
		handle := dispatch(t, ctx, running, run)
		if _, again := dispatchOutcome(t, ctx, running, run); again.Name != handle.Name {
			t.Fatalf("second dispatch of the same run returned job %q, first %q", again.Name, handle.Name)
		}
		job, err := running.Lookup(ctx, run.ID)
		if err != nil || job.State != adapter.JobRunning {
			t.Fatalf("lookup while running = %+v, %v; want running", job, err)
		}
		if job.Attempt != run.Attempt || job.Adapter != run.Adapter || job.Agent != run.Agent || job.Handle.Name != handle.Name {
			t.Fatalf("lookup metadata %+v; want attempt %d, adapter %s, agent %s, job %s", job, run.Attempt, run.Adapter, run.Agent, handle.Name)
		}
		if terminal := follow(t, ctx, running, handle); !terminal.Started || terminal.ExitCode != 0 {
			t.Fatalf("terminal %+v; want started with exit code 0", terminal)
		}
		if job, err := running.Lookup(ctx, run.ID); err != nil || job.State != adapter.JobEnded {
			t.Fatalf("lookup after the job ended = %+v, %v; want ended", job, err)
		}
		outputs := make(chan adapter.Output, 1)
		if err := running.Collect(ctx, handle, adapter.Limits{Files: 256, FileBytes: 1 << 20, TotalBytes: 1 << 20}, outputs); err != nil || len(outputs) != 0 {
			t.Fatalf("collect from a job that wrote no outputs = %d outputs, %v; want none", len(outputs), err)
		}
		idempotent(t, ctx, running, run.ID, handle)
		if job, err := running.Lookup(ctx, run.ID); err != nil || job.State != adapter.JobAbsent {
			t.Fatalf("lookup after purge = %+v, %v; want absent", job, err)
		}
	})

	t.Run("Stop", func(t *testing.T) {
		run := adapter.Run{ID: id, Adapter: completes.Name, Agent: completes.Agent, Attempt: 1, Prompt: []byte("conformance prompt")}
		purgeAfter(t, running, run)
		handle := dispatch(t, ctx, running, run)
		for range 2 {
			if err := running.Stop(ctx, handle); err != nil {
				t.Fatalf("stop: %v", err)
			}
		}
		if job, err := running.Lookup(ctx, run.ID); err != nil || job.State == adapter.JobRunning {
			t.Fatalf("lookup after stop = %+v, %v; want the job not running", job, err)
		}
	})

	t.Run("NeverStarts", func(t *testing.T) {
		run := adapter.Run{ID: id + "-never", Adapter: neverStarts.Name, Agent: neverStarts.Agent, Attempt: 1, Prompt: []byte("conformance prompt")}
		purgeAfter(t, failing, run)
		handle := dispatch(t, ctx, failing, run)
		if job, err := failing.Lookup(ctx, run.ID); err != nil || job.State != adapter.JobNeverStarted {
			t.Fatalf("lookup = %+v, %v; want never started", job, err)
		}
		if terminal := follow(t, ctx, failing, handle); terminal.Started {
			t.Fatalf("terminal %+v; want not started", terminal)
		}
		idempotent(t, ctx, failing, run.ID, handle)
	})
}

func runID(t *testing.T) string {
	t.Helper()
	random := make([]byte, 6)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	return "conformance-" + hex.EncodeToString(random)
}

func purgeAfter(t *testing.T, a adapter.Adapter, run adapter.Run) {
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := a.Purge(ctx, run.ID); err != nil {
			t.Errorf("purge %s: %v", run.ID, err)
		}
	})
}

func dispatchOutcome(t *testing.T, ctx context.Context, a adapter.Adapter, run adapter.Run) (adapter.Outcome, adapter.Handle) {
	t.Helper()
	outcome, handle, err := a.Dispatch(ctx, run)
	if err != nil {
		t.Fatalf("dispatch %s: %v", run.ID, err)
	}
	if len(handle.Data) > maxHandle || handle.Name == "" {
		t.Fatalf("handle of %d bytes named %q; want at most %d bytes and a name", len(handle.Data), handle.Name, maxHandle)
	}
	return outcome, handle
}

func dispatch(t *testing.T, ctx context.Context, a adapter.Adapter, run adapter.Run) adapter.Handle {
	t.Helper()
	outcome, handle := dispatchOutcome(t, ctx, a, run)
	if outcome != adapter.OutcomeCreated {
		t.Fatalf("dispatch %s: outcome %d; want created", run.ID, outcome)
	}
	return handle
}

func follow(t *testing.T, ctx context.Context, a adapter.Adapter, handle adapter.Handle) adapter.Terminal {
	t.Helper()
	lines := make(chan adapter.Line, 64)
	type followed struct {
		terminal adapter.Terminal
		err      error
	}
	ended := make(chan followed, 1)
	go func() {
		terminal, err := a.Follow(ctx, handle, 0, lines)
		ended <- followed{terminal, err}
	}()
	for {
		select {
		case <-lines:
		case end := <-ended:
			if end.err != nil {
				t.Fatalf("follow: %v", end.err)
			}
			return end.terminal
		}
	}
}

// idempotent stops a run's job twice, then releases it twice, then purges it twice; every call succeeds.
func idempotent(t *testing.T, ctx context.Context, a adapter.Adapter, runID string, handle adapter.Handle) {
	t.Helper()
	for range 2 {
		if handle.Data != nil {
			if err := a.Stop(ctx, handle); err != nil {
				t.Fatalf("stop: %v", err)
			}
		}
	}
	for range 2 {
		if err := a.Release(ctx, runID); err != nil {
			t.Fatalf("release: %v", err)
		}
	}
	for range 2 {
		if err := a.Purge(ctx, runID); err != nil {
			t.Fatalf("purge: %v", err)
		}
	}
}
