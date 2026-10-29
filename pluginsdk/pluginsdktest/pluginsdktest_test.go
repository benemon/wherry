package pluginsdktest_test

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

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

func conform(t *testing.T, wrap func(pluginsdk.Plugin) pluginsdk.Plugin) {
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
	pluginsdktest.Run(t, factory,
		pluginsdk.Settings{Name: "completes", Agent: "claude", Config: []byte(`{"jobs":true,"finish":true,"inline_limit":4096}`)},
		pluginsdk.Settings{Name: "never-starts", Agent: "claude", Config: []byte(`{"jobs":true,"never_started":"image pull failed","inline_limit":4096}`)},
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

var broken = map[string]struct {
	wrap func(pluginsdk.Plugin) pluginsdk.Plugin
	want string
}{
	"redispatches": {func(p pluginsdk.Plugin) pluginsdk.Plugin { return redispatches{p} }, "lookup after dispatching the ended run"},
	"forgets":      {func(p pluginsdk.Plugin) pluginsdk.Plugin { return forgets{p} }, "after the end = {Started:false"},
	"drifts":       {func(p pluginsdk.Plugin) pluginsdk.Plugin { return drifts{p, new(int)} }, "second lookup while running"},
}

// Each broken plugin fails the suite at the case that guards against it.
func TestConformanceFailsBrokenPlugins(t *testing.T) {
	for name, b := range broken {
		t.Run(name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestBrokenPlugin$", "-test.v")
			cmd.Env = append(os.Environ(), "PLUGINSDKTEST_BROKEN="+name)
			output, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(output), b.want) {
				t.Fatalf("suite against %s = %v; want a failure naming %q\n%s", name, err, b.want, output)
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
