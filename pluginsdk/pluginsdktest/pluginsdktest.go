// Package pluginsdktest is the conformance suite a plugin runs against its platform.
package pluginsdktest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/pluginsdk"
	"github.com/hashicorp/go-hclog"
)

const maxHandle = 4 << 10

// PollDeadline bounds each poll of a dispatched job until it starts or ends: one deadline covers every Lookup, which
// runs under it, and every delay between them. A variable so tests can shorten it.
var PollDeadline = 2 * time.Minute

// pollInterval separates those Lookups.
const pollInterval = time.Second

// Credentials are the credentials the completing job's run carries, one at each kind of target.
var Credentials = []pluginsdk.Secret{
	{Name: "conformance-env", Env: "CONFORMANCE_TOKEN", Value: []byte("conformance-env-value")},
	{Name: "conformance-file", File: "conformance-key", Value: []byte("conformance-file-value")},
}

// Working is the line the completing job reports in pluginsdk.WorkingFile.
const Working = "conformance working"

// Run checks the profiles factory, the one a plugin hands Serve, builds against the plugin contract on their
// platform, giving it a logger that discards. completes configures a profile whose job, as it starts, writes Working
// and a newline to pluginsdk.WorkingFile beneath its output directory by renaming a file over it, runs for at least
// ten seconds and exits 0; neverStarts one whose job the platform ends before its agent starts; pending, optional, one whose job
// the platform never places and says why, which zero Settings skip. Every job the suite creates is purged.
// inlineLimit is the cap every configured profile reports. The completing job's run carries Credentials.
func Run(t *testing.T, factory func(pluginsdk.Settings, hclog.Logger) (pluginsdk.Plugin, error), completes, neverStarts, pending pluginsdk.Settings, inlineLimit int64) {
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
	var unplaced pluginsdk.Plugin
	if !reflect.DeepEqual(pending, pluginsdk.Settings{}) {
		if unplaced, err = factory(pending, hclog.NewNullLogger()); err != nil {
			t.Fatalf("configure %s: %v", pending.Name, err)
		}
		if unplaced.InlineLimit() != inlineLimit {
			t.Fatalf("inline limit of %s = %d, want %d", pending.Name, unplaced.InlineLimit(), inlineLimit)
		}
	}
	id := runID(t)

	t.Run("Ready", func(t *testing.T) {
		if ready, err := running.Ready(ctx); err != nil || !ready {
			t.Fatalf("ready = %v, %v; want the platform ready", ready, err)
		}
	})

	t.Run("Absent", func(t *testing.T) {
		job, err := running.Lookup(ctx, id+"-absent")
		if err != nil || job.State != pluginsdk.JobAbsent {
			t.Fatalf("lookup of an unknown run = %+v, %v; want absent", job, err)
		}
		idempotent(t, ctx, running, id+"-absent", pluginsdk.Handle{})
	})

	t.Run("Completes", func(t *testing.T) {
		run := pluginsdk.Run{ID: id, Profile: completes.Name, Agent: completes.Agent, Attempt: 3, Prompt: []byte("conformance prompt"), Credentials: Credentials}
		purgeAfter(t, running, run)
		handle := dispatch(t, ctx, running, run)
		if _, again := dispatchOutcome(t, ctx, running, run); again.Name != handle.Name {
			t.Fatalf("second dispatch of the same run returned job %q, first %q", again.Name, handle.Name)
		}
		job, err := poll(ctx, running, run.ID, pluginsdk.JobRunning)
		if err != nil || job.State != pluginsdk.JobRunning {
			t.Fatalf("lookup while running = %+v, %v; want running within %s", job, err, PollDeadline)
		}
		if again, err := running.Lookup(ctx, run.ID); err != nil || !reflect.DeepEqual(again, job) {
			t.Fatalf("second lookup while running = %+v, %v; want %+v", again, err, job)
		}
		if job.Attempt != run.Attempt || job.Profile != run.Profile || job.Agent != run.Agent || job.Handle.Name != handle.Name {
			t.Fatalf("lookup metadata %+v; want attempt %d, profile %s, agent %s, job %s", job, run.Attempt, run.Profile, run.Agent, handle.Name)
		}
		if data, err := readWorking(ctx, running, job.Handle); err != nil || string(data) != Working+"\n" {
			t.Fatalf("working file of the running job = %q, %v; want %q within %s", data, err, Working+"\n", PollDeadline)
		}
		terminal, end := follow(t, ctx, running, job.Handle, 0)
		if !terminal.Started || terminal.ExitCode != 0 {
			t.Fatalf("terminal %+v; want started with exit code 0", terminal)
		}
		if job, err := running.Lookup(ctx, run.ID); err != nil || job.State != pluginsdk.JobEnded {
			t.Fatalf("lookup after the job ended = %+v, %v; want ended", job, err)
		}
		outputs := make(chan pluginsdk.Output, 256)
		err = running.Collect(ctx, job.Handle, pluginsdk.Limits{Files: 256, FileBytes: 1 << 20, TotalBytes: 1 << 20}, outputs)
		close(outputs)
		for output := range outputs {
			if !strings.HasPrefix(output.Name, ".wherry/") {
				err = errors.Join(err, errors.New("collected "+output.Name))
			}
		}
		if err != nil {
			t.Fatalf("collect from a job that wrote no outputs but its working file: %v", err)
		}
		if replay, _ := follow(t, ctx, running, job.Handle, end); replay != terminal {
			t.Fatalf("follow from offset %d after the end = %+v; want the terminal %+v", end, replay, terminal)
		}
		if _, again := dispatchOutcome(t, ctx, running, run); again.Name != handle.Name {
			t.Fatalf("dispatch after the job ended returned job %q, first %q", again.Name, handle.Name)
		}
		if job, err := running.Lookup(ctx, run.ID); err != nil || job.State != pluginsdk.JobEnded {
			t.Fatalf("lookup after dispatching the ended run = %+v, %v; want ended", job, err)
		}
		idempotent(t, ctx, running, run.ID, handle)
		if job, err := running.Lookup(ctx, run.ID); err != nil || job.State != pluginsdk.JobAbsent {
			t.Fatalf("lookup after purge = %+v, %v; want absent", job, err)
		}
		if data, _ := running.ReadWorking(ctx, job.Handle); len(data) != 0 {
			t.Fatalf("working file of a purged job = %q; want nothing", data)
		}
	})

	t.Run("Stop", func(t *testing.T) {
		run := pluginsdk.Run{ID: id, Profile: completes.Name, Agent: completes.Agent, Attempt: 1, Prompt: []byte("conformance prompt")}
		purgeAfter(t, running, run)
		handle := dispatch(t, ctx, running, run)
		for range 2 {
			if err := running.Stop(ctx, handle); err != nil {
				t.Fatalf("stop: %v", err)
			}
		}
		if job, err := running.Lookup(ctx, run.ID); err != nil || job.State == pluginsdk.JobRunning {
			t.Fatalf("lookup after stop = %+v, %v; want the job not running", job, err)
		}
	})

	t.Run("NeverStarts", func(t *testing.T) {
		run := pluginsdk.Run{ID: id + "-never", Profile: neverStarts.Name, Agent: neverStarts.Agent, Attempt: 1, Prompt: []byte("conformance prompt")}
		purgeAfter(t, failing, run)
		handle := dispatch(t, ctx, failing, run)
		job, err := poll(ctx, failing, run.ID, pluginsdk.JobNeverStarted)
		if err != nil || job.State != pluginsdk.JobNeverStarted {
			t.Fatalf("lookup = %+v, %v; want never started within %s", job, err, PollDeadline)
		}
		if terminal, _ := follow(t, ctx, failing, job.Handle, 0); terminal.Started {
			t.Fatalf("terminal %+v; want not started", terminal)
		}
		idempotent(t, ctx, failing, run.ID, handle)
	})

	if unplaced == nil {
		return
	}
	t.Run("Pending", func(t *testing.T) {
		run := pluginsdk.Run{ID: id + "-pending", Profile: pending.Name, Agent: pending.Agent, Attempt: 1, Prompt: []byte("conformance prompt")}
		purgeAfter(t, unplaced, run)
		dispatchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		handle := dispatch(t, dispatchCtx, unplaced, run)
		cancel()
		// A platform that never decides leaves Lookup waiting, which fails the profile.
		for _, which := range []string{"first", "second"} {
			lookupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			job, err := unplaced.Lookup(lookupCtx, run.ID)
			cancel()
			if err != nil || job.State != pluginsdk.JobPending || job.Reason == "" {
				t.Fatalf("%s lookup of an unplaced job = %+v, %v; want pending with a reason", which, job, err)
			}
		}
		if err := unplaced.Stop(ctx, handle); err != nil {
			t.Fatalf("stop of an unplaced job: %v", err)
		}
		if job, err := unplaced.Lookup(ctx, run.ID); err != nil || job.State == pluginsdk.JobRunning {
			t.Fatalf("lookup after stop = %+v, %v; want the job not running", job, err)
		}
		if err := unplaced.Purge(ctx, run.ID); err != nil {
			t.Fatalf("purge of an unplaced job: %v", err)
		}
		if job, err := unplaced.Lookup(ctx, run.ID); err != nil || job.State != pluginsdk.JobAbsent {
			t.Fatalf("lookup after purge = %+v, %v; want absent", job, err)
		}
	})
}

// poll repeats Lookup until it reports want, an error or another state that is not pending, or PollDeadline passes.
func poll(ctx context.Context, a pluginsdk.Plugin, runID string, want pluginsdk.JobState) (pluginsdk.Job, error) {
	ctx, cancel := context.WithTimeout(ctx, PollDeadline)
	defer cancel()
	for {
		job, err := a.Lookup(ctx, runID)
		if err != nil || job.State != pluginsdk.JobPending || job.State == want {
			return job, err
		}
		select {
		case <-ctx.Done():
			return job, ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// readWorking repeats ReadWorking on a running job until it returns Working and a newline, or an error, or
// PollDeadline passes.
func readWorking(ctx context.Context, a pluginsdk.Plugin, handle pluginsdk.Handle) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, PollDeadline)
	defer cancel()
	for {
		data, err := a.ReadWorking(ctx, handle)
		if err != nil || string(data) == Working+"\n" {
			return data, err
		}
		select {
		case <-ctx.Done():
			return data, ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

func runID(t *testing.T) string {
	t.Helper()
	random := make([]byte, 6)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	return "conformance-" + hex.EncodeToString(random)
}

func purgeAfter(t *testing.T, a pluginsdk.Plugin, run pluginsdk.Run) {
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := a.Purge(ctx, run.ID); err != nil {
			t.Errorf("purge %s: %v", run.ID, err)
		}
	})
}

func dispatchOutcome(t *testing.T, ctx context.Context, a pluginsdk.Plugin, run pluginsdk.Run) (pluginsdk.Outcome, pluginsdk.Handle) {
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

func dispatch(t *testing.T, ctx context.Context, a pluginsdk.Plugin, run pluginsdk.Run) pluginsdk.Handle {
	t.Helper()
	outcome, handle := dispatchOutcome(t, ctx, a, run)
	if outcome != pluginsdk.OutcomeCreated {
		t.Fatalf("dispatch %s: outcome %d; want created", run.ID, outcome)
	}
	return handle
}

// follow returns the job's terminal and the offset after the last line it took.
func follow(t *testing.T, ctx context.Context, a pluginsdk.Plugin, handle pluginsdk.Handle, from int64) (pluginsdk.Terminal, int64) {
	t.Helper()
	lines := make(chan pluginsdk.Line, 64)
	type followed struct {
		terminal pluginsdk.Terminal
		err      error
	}
	ended := make(chan followed, 1)
	go func() {
		terminal, err := a.Follow(ctx, handle, from, lines)
		ended <- followed{terminal, err}
	}()
	for {
		select {
		case line := <-lines:
			from = line.Next
		case end := <-ended:
			for len(lines) > 0 {
				from = (<-lines).Next
			}
			if end.err != nil {
				t.Fatalf("follow: %v", end.err)
			}
			return end.terminal, from
		}
	}
}

// idempotent stops a run's job twice, then releases it twice, then purges it twice, then stops it again, as a receiver
// adopting a job the platform has removed does; every call succeeds.
func idempotent(t *testing.T, ctx context.Context, a pluginsdk.Plugin, runID string, handle pluginsdk.Handle) {
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
	if handle.Data != nil {
		if err := a.Stop(ctx, handle); err != nil {
			t.Fatalf("stop after purge: %v", err)
		}
	}
}
