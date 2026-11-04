package pluginsdktest_test

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/pluginsdk"
	"github.com/benemon/wherry/pluginsdk/pluginsdktest"
	"github.com/hashicorp/go-hclog"
)

// The suite passes against the in-repo test plugin through the protocol, and the plugin receives each credential at
// its target.
func TestConformanceThroughThePlugin(t *testing.T) {
	record := filepath.Join(t.TempDir(), "record.jsonl")
	t.Setenv("WHERRY_TESTPLUGIN_RECORD", record)
	conform(t, func(p pluginsdk.Plugin) pluginsdk.Plugin { return p })
	file, err := os.Open(record)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	delivered := false
	for scanner := bufio.NewScanner(file); scanner.Scan(); {
		var entry struct {
			Call string         `json:"call"`
			Run  *pluginsdk.Run `json:"run"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			t.Fatal(err)
		}
		delivered = delivered || entry.Call == "dispatch" && entry.Run.Profile == "completes" && reflect.DeepEqual(entry.Run.Credentials, pluginsdktest.Credentials)
	}
	if !delivered {
		t.Fatalf("no dispatch carried %+v across the protocol", pluginsdktest.Credentials)
	}
}

// pollDeadline is the PollDeadline conform runs the suite with.
const pollDeadline = 5 * time.Second

func conform(t *testing.T, wrap func(pluginsdk.Plugin) pluginsdk.Plugin) {
	previous := pluginsdktest.PollDeadline
	pluginsdktest.PollDeadline = pollDeadline
	t.Cleanup(func() { pluginsdktest.PollDeadline = previous })
	plugin := filepath.Join(t.TempDir(), "wherry-plugin-fake")
	if output, err := exec.Command("go", "build", "-o", plugin, "../internal/testplugin").CombinedOutput(); err != nil {
		t.Fatalf("build test plugin: %v\n%s", err, output)
	}
	content, err := os.ReadFile(plugin)
	if err != nil {
		t.Fatal(err)
	}
	pin := sha256.Sum256(content)
	p, err := pluginsdk.Launch("fake", func() (string, error) { return plugin, nil }, pin[:], io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	factory := func(settings pluginsdk.Settings, _ hclog.Logger) (pluginsdk.Plugin, error) {
		profile, err := p.Configure(settings)
		if err != nil {
			return nil, err
		}
		return wrap(profile), nil
	}
	// The completing profile's jobs wait unplaced until the platform places them, a Lookup or two after Dispatch.
	placed := filepath.Join(t.TempDir(), "placed")
	timer := time.AfterFunc(1500*time.Millisecond, func() { _ = os.WriteFile(placed, nil, 0o600) })
	t.Cleanup(func() { timer.Stop() })
	pluginsdktest.Run(t, factory,
		pluginsdk.Settings{Name: "completes", Agent: "claude", Config: fmt.Appendf(nil, `{"jobs":true,"finish":true,"pending":"queued","placed":%q,"inline_limit":4096}`, placed)},
		pluginsdk.Settings{Name: "never-starts", Agent: "claude", Config: []byte(`{"jobs":true,"never_started":"image pull failed","inline_limit":4096}`)},
		pluginsdk.Settings{Name: "pending", Agent: "claude", Config: []byte(`{"jobs":true,"pending":"0 nodes evaluated, 0 filtered, 0 exhausted","inline_limit":4096}`)},
		4096)
}

// redispatches starts a second job for a run whose job has ended.
type redispatches struct{ pluginsdk.Plugin }

func (p redispatches) Dispatch(ctx context.Context, run pluginsdk.Run) (pluginsdk.Outcome, pluginsdk.Handle, error) {
	if job, err := p.Lookup(ctx, run.ID); err == nil && job.State == pluginsdk.JobEnded {
		if err := p.Purge(ctx, run.ID); err != nil {
			return pluginsdk.OutcomeUnknown, pluginsdk.Handle{}, err
		}
	}
	return p.Plugin.Dispatch(ctx, run)
}

// forgets replays no terminal for an ended job.
type forgets struct{ pluginsdk.Plugin }

func (p forgets) Follow(ctx context.Context, handle pluginsdk.Handle, from int64, lines chan<- pluginsdk.Line) (pluginsdk.Terminal, error) {
	if job, err := p.Lookup(ctx, string(handle.Data)); err == nil && job.State == pluginsdk.JobEnded {
		return pluginsdk.Terminal{}, nil
	}
	return p.Plugin.Follow(ctx, handle, from, lines)
}

// drifts returns a different handle at each Lookup.
type drifts struct {
	pluginsdk.Plugin
	lookups *int
}

func (p drifts) Lookup(ctx context.Context, runID string) (pluginsdk.Job, error) {
	job, err := p.Plugin.Lookup(ctx, runID)
	*p.lookups++
	job.Handle.Data = append(job.Handle.Data, strconv.Itoa(*p.lookups)...)
	return job, err
}

// pendingForever never places a job that would run.
type pendingForever struct{ pluginsdk.Plugin }

func (p pendingForever) Lookup(ctx context.Context, runID string) (pluginsdk.Job, error) {
	job, err := p.Plugin.Lookup(ctx, runID)
	if job.State == pluginsdk.JobRunning {
		job.State, job.Reason = pluginsdk.JobPending, "no nodes available"
	}
	return job, err
}

// startsNever waits in Lookup, once the job is placed, until its context ends, as a plugin whose task never starts.
type startsNever struct{ pluginsdk.Plugin }

func (p startsNever) Lookup(ctx context.Context, runID string) (pluginsdk.Job, error) {
	job, err := p.Plugin.Lookup(ctx, runID)
	if err == nil && job.State == pluginsdk.JobRunning {
		<-ctx.Done()
		return pluginsdk.Job{}, ctx.Err()
	}
	return job, err
}

// broken plugins, each with the failure it causes and, when within is set, the bound on the Completes subtest's
// duration.
var broken = map[string]struct {
	wrap   func(pluginsdk.Plugin) pluginsdk.Plugin
	want   string
	within time.Duration
}{
	"redispatches":   {func(p pluginsdk.Plugin) pluginsdk.Plugin { return redispatches{p} }, "lookup after dispatching the ended run", 0},
	"forgets":        {func(p pluginsdk.Plugin) pluginsdk.Plugin { return forgets{p} }, "after the end = {Started:false", 0},
	"drifts":         {func(p pluginsdk.Plugin) pluginsdk.Plugin { return drifts{p, new(int)} }, "second lookup while running", 0},
	"pendingForever": {func(p pluginsdk.Plugin) pluginsdk.Plugin { return pendingForever{p} }, "lookup while running = {State:4", 0},
	"startsNever":    {func(p pluginsdk.Plugin) pluginsdk.Plugin { return startsNever{p} }, "context deadline exceeded; want running within " + pollDeadline.String(), 2 * pollDeadline},
}

// completesTook is the duration the verbose output reports for the Completes subtest.
var completesTook = regexp.MustCompile(`--- FAIL: TestBrokenPlugin/Completes \(([0-9.]+)s\)`)

// Each broken plugin fails the suite at the case that guards against it. A Lookup that blocks fails the suite at
// PollDeadline: one deadline covers every Lookup and every delay of the poll.
func TestConformanceFailsBrokenPlugins(t *testing.T) {
	for name, b := range broken {
		t.Run(name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestBrokenPlugin$", "-test.v")
			cmd.Env = append(os.Environ(), "PLUGINSDKTEST_BROKEN="+name)
			output, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(output), b.want) {
				t.Fatalf("suite against %s = %v; want a failure naming %q\n%s", name, err, b.want, output)
			}
			if b.within == 0 {
				return
			}
			took := completesTook.FindSubmatch(output)
			if took == nil {
				t.Fatalf("no Completes failure in the output\n%s", output)
			}
			seconds, err := strconv.ParseFloat(string(took[1]), 64)
			if d := time.Duration(seconds * float64(time.Second)); err != nil || d < pollDeadline || d >= b.within {
				t.Fatalf("Completes failed after %ss, want at PollDeadline %s, before %s", took[1], pollDeadline, b.within)
			}
		})
	}
}

func TestBrokenPlugin(t *testing.T) {
	b, ok := broken[os.Getenv("PLUGINSDKTEST_BROKEN")]
	if !ok {
		t.Skip("run by TestConformanceFailsBrokenPlugins")
	}
	conform(t, b.wrap)
}
