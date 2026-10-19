package nomad

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/receiver"
	"github.com/benemon/wherry/internal/wire"
	"github.com/hashicorp/nomad/api"
)

const credentialEnv = "WHERRY_TEST_CREDENTIAL"

func testConfig(address string) Config {
	script := `printf '{"type":"result","result":"prompt=%s token=%s env=%s home=%s"}\n' "$(cat ${NOMAD_TASK_DIR}/prompt.txt)" "$` + credentialEnv + `" "$WHERRY_TEST_ENV" "$HOME"`
	return Config{
		Address: address, Namespace: "default", CredentialEnv: credentialEnv,
		Runtime: Runtime{
			Driver:    "raw_exec",
			Config:    map[string]any{"command": "/bin/sh", "args": []any{"-c", script}},
			Env:       map[string]string{"WHERRY_TEST_ENV": "from-runtime"},
			Resources: Resources{CPU: new(100), MemoryMB: new(64)},
		},
		NodePool: "default", Datacenters: []string{"dc1"},
	}
}

func devAdapter(t *testing.T) (*Adapter, context.Context) {
	t.Helper()
	address := os.Getenv("WHERRY_NOMAD_DEV_ADDR")
	if address == "" {
		t.Skip("WHERRY_NOMAD_DEV_ADDR not set")
	}
	adapter, err := New(testConfig(address), "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	return adapter, ctx
}

func testRun(name string) receiver.Run {
	id := fmt.Sprintf("run-%s-%d", name, time.Now().UnixNano())
	return receiver.Run{
		ID: id, Turn: 1,
		Prompt: []byte("hello-prompt"), Credential: []byte("test-credential"),
	}
}

func cleanupRun(t *testing.T, adapter *Adapter, run receiver.Run) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = adapter.Release(ctx, run.ID, run.Turn)
		_, _, _ = adapter.client.Jobs().Deregister(jobID(run.ID, run.Turn), true, (&api.WriteOptions{}).WithContext(ctx))
	})
}

func dispatch(t *testing.T, adapter *Adapter, ctx context.Context, run receiver.Run) receiver.Handle {
	t.Helper()
	cleanupRun(t, adapter, run)
	handle, err := adapter.Dispatch(ctx, run)
	if err != nil {
		t.Fatal(err)
	}
	return handle
}

func collect(t *testing.T, ctx context.Context, adapter *Adapter, handle receiver.Handle) []receiver.Line {
	t.Helper()
	lines := make(chan receiver.Line, 16)
	done := make(chan error, 1)
	go func() { done <- adapter.Follow(ctx, handle, 0, lines) }()
	var got []receiver.Line
	for {
		select {
		case line := <-lines:
			got = append(got, line)
			if strings.Contains(string(line.Data), `"result"`) {
				return got
			}
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			for len(lines) > 0 {
				got = append(got, <-lines)
			}
			return got
		case <-ctx.Done():
			t.Fatal("follow timed out")
		}
	}
}

func TestReadyFromReadyNodes(t *testing.T) {
	adapter, ctx := devAdapter(t)
	if ready, err := adapter.Ready(ctx); err != nil || !ready {
		t.Fatalf("ready = %v, %v; want a ready dev node with raw_exec", ready, err)
	}
	adapter.nodePool = "wherry-no-such-pool"
	if ready, err := adapter.Ready(ctx); err != nil || ready {
		t.Fatalf("ready = %v, %v; want no node in another node pool", ready, err)
	}
}

func retriesOn(group *api.TaskGroup) bool {
	reschedule, restart := group.ReschedulePolicy, group.RestartPolicy
	return reschedule == nil || *reschedule.Attempts != 0 || *reschedule.Unlimited ||
		restart == nil || *restart.Attempts != 0 || *restart.Mode != api.RestartPolicyModeFail
}

func TestBuildJobFromRuntime(t *testing.T) {
	adapter, err := New(testConfig("http://127.0.0.1:1"), "")
	if err != nil {
		t.Fatal(err)
	}
	run := testRun("build")
	job, err := adapter.buildJob(run, variablePath(run.ID, run.Turn))
	if err != nil {
		t.Fatal(err)
	}
	if *job.ID != jobID(run.ID, run.Turn) || *job.Name != jobID(run.ID, run.Turn) || *job.Namespace != "default" || *job.Type != api.JobTypeBatch ||
		*job.NodePool != "default" || !reflect.DeepEqual(job.Datacenters, []string{"dc1"}) || !reflect.DeepEqual(job.Meta, map[string]string{"run_id": run.ID}) {
		t.Fatalf("unexpected job identity or placement: %#v", job)
	}
	if len(job.TaskGroups) != 1 || retriesOn(job.TaskGroups[0]) {
		t.Fatalf("retries not forced off: groups=%#v", job.TaskGroups)
	}
	runtime := testConfig("").Runtime
	tasks := job.TaskGroups[0].Tasks
	if len(tasks) != 1 || tasks[0].Name != taskName || tasks[0].Driver != "raw_exec" || !reflect.DeepEqual(tasks[0].Config, runtime.Config) ||
		*tasks[0].Resources.CPU != 100 || *tasks[0].Resources.MemoryMB != 64 ||
		*tasks[0].LogConfig.MaxFiles != 3 || *tasks[0].LogConfig.MaxFileSizeMB != 10 ||
		!reflect.DeepEqual(tasks[0].Env, map[string]string{"WHERRY_TEST_ENV": "from-runtime", "HOME": "${NOMAD_TASK_DIR}"}) {
		t.Fatalf("runtime not carried: %#v", tasks)
	}
	templates := tasks[0].Templates
	if len(templates) != 2 || *templates[0].DestPath != "local/prompt.txt" ||
		*templates[1].DestPath != "${NOMAD_SECRETS_DIR}/wherry.env" || !*templates[1].Envvars ||
		!strings.Contains(*templates[1].EmbeddedTmpl, credentialEnv+"={{ .credential }}") ||
		*templates[0].ChangeMode != "noop" || *templates[1].ChangeMode != "noop" {
		t.Fatalf("templates %#v", templates)
	}
	run.Credential = nil
	job, err = adapter.buildJob(run, variablePath(run.ID, run.Turn))
	if err != nil {
		t.Fatal(err)
	}
	if templates := job.TaskGroups[0].Tasks[0].Templates; len(templates) != 1 || *templates[0].DestPath != "local/prompt.txt" {
		t.Fatalf("run without a credential has templates %#v", templates)
	}
}

func TestPerTurnJobIDs(t *testing.T) {
	adapter, err := New(testConfig("http://127.0.0.1:1"), "")
	if err != nil {
		t.Fatal(err)
	}
	run := testRun("turns")
	run.ID = "run-turns"
	for _, turn := range []int64{1, 2} {
		run.Turn = turn
		path := variablePath(run.ID, turn)
		job, err := adapter.buildJob(run, path)
		if err != nil {
			t.Fatal(err)
		}
		wantID := fmt.Sprintf("agent-%s-t%d", run.ID, turn)
		if *job.ID != wantID || *job.Name != wantID || path != "nomad/jobs/"+wantID {
			t.Fatalf("turn %d job=%q name=%q path=%q, want %q", turn, *job.ID, *job.Name, path, wantID)
		}
		if template := *job.TaskGroups[0].Tasks[0].Templates[0].EmbeddedTmpl; !strings.Contains(template, path) {
			t.Fatalf("turn %d template %q does not use %q", turn, template, path)
		}
	}
}

func TestNewValidatesConfig(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"driver":         func(config *Config) { config.Runtime = Runtime{} },
		"credential env": func(config *Config) { config.CredentialEnv = `X={{ .prompt }}` },
	} {
		t.Run(name, func(t *testing.T) {
			config := testConfig("http://127.0.0.1:1")
			mutate(&config)
			if _, err := New(config, ""); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
}

func TestDispatchRegistersPerRunJob(t *testing.T) {
	adapter, ctx := devAdapter(t)
	run := testRun("register")
	dispatch(t, adapter, ctx, run)
	job, _, err := adapter.client.Jobs().Info(jobID(run.ID, run.Turn), nil)
	if err != nil {
		t.Fatal(err)
	}
	if *job.ID != jobID(run.ID, run.Turn) || *job.Name != jobID(run.ID, run.Turn) || *job.Type != api.JobTypeBatch ||
		*job.NodePool != "default" || len(job.Datacenters) != 1 || job.Datacenters[0] != "dc1" || job.Meta["run_id"] != run.ID {
		t.Fatalf("unexpected job: %#v", job)
	}
	if retriesOn(job.TaskGroups[0]) {
		t.Fatalf("retries were not forced off: reschedule=%#v restart=%#v", job.TaskGroups[0].ReschedulePolicy, job.TaskGroups[0].RestartPolicy)
	}
	if task := job.TaskGroups[0].Tasks[0]; task.Driver != "raw_exec" || task.Config["command"] != "/bin/sh" || *task.Resources.MemoryMB != 64 {
		t.Fatalf("registered task does not carry the runtime: %#v", task)
	}
}

func TestDispatchAdoptsExistingJob(t *testing.T) {
	adapter, ctx := devAdapter(t)
	run := testRun("adopt")
	dispatch(t, adapter, ctx, run)
	before, _, err := adapter.client.Jobs().Info(jobID(run.ID, run.Turn), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.Dispatch(ctx, run); err != nil {
		t.Fatal(err)
	}
	after, _, err := adapter.client.Jobs().Info(jobID(run.ID, run.Turn), nil)
	if err != nil {
		t.Fatal(err)
	}
	if *after.ModifyIndex != *before.ModifyIndex {
		t.Fatalf("adoption re-registered job: modify index %d -> %d", *before.ModifyIndex, *after.ModifyIndex)
	}
}

func output(t *testing.T, ctx context.Context, adapter *Adapter, handle receiver.Handle) string {
	t.Helper()
	joined := ""
	for _, line := range collect(t, ctx, adapter, handle) {
		joined += string(line.Data)
	}
	return joined
}

func TestDispatchDeliversPromptAndCredential(t *testing.T) {
	adapter, ctx := devAdapter(t)
	run := testRun("render")
	handle := dispatch(t, adapter, ctx, run)
	got := output(t, ctx, adapter, handle)
	if !strings.Contains(got, "prompt=hello-prompt token=test-credential env=from-runtime home=/") || !strings.Contains(got, "/agent/local\"}") {
		t.Fatalf("task output does not prove prompt, credential, env and HOME delivered: %q", got)
	}
}

func TestDispatchWithoutCredential(t *testing.T) {
	adapter, ctx := devAdapter(t)
	run := testRun("no-credential")
	run.Credential = nil
	handle := dispatch(t, adapter, ctx, run)
	if got := output(t, ctx, adapter, handle); !strings.Contains(got, "prompt=hello-prompt token= env=from-runtime") {
		t.Fatalf("task output %q", got)
	}
	variable, _, err := adapter.client.Variables().Peek(variablePath(run.ID, run.Turn), nil)
	if err != nil || variable == nil {
		t.Fatalf("variable = %#v, %v", variable, err)
	}
	if _, ok := variable.Items["credential"]; ok {
		t.Fatal("variable carries a credential item for a run without one")
	}
}

func TestLookupFindsExistingRun(t *testing.T) {
	adapter, ctx := devAdapter(t)
	run := testRun("lookup")
	dispatched := dispatch(t, adapter, ctx, run)
	found, ok, err := adapter.Lookup(ctx, run.ID, run.Turn)
	if err != nil || !ok {
		t.Fatalf("lookup = %v, %v", ok, err)
	}
	if found.Job != dispatched.Job || found.Alloc != dispatched.Alloc || found.Format != dispatched.Format {
		t.Fatalf("lookup handle %#v, dispatch handle %#v", found, dispatched)
	}
	if got := output(t, ctx, adapter, found); !strings.Contains(got, "prompt=hello-prompt") {
		t.Fatalf("followed lookup handle output %q", got)
	}
}

func TestLookupMissingRun(t *testing.T) {
	adapter, ctx := devAdapter(t)
	handle, ok, err := adapter.Lookup(ctx, fmt.Sprintf("run-missing-%d", time.Now().UnixNano()), 1)
	if err != nil || ok || handle != (receiver.Handle{}) {
		t.Fatalf("lookup = %#v, %v, %v", handle, ok, err)
	}
}

func TestLookupWaitsForUnplacedJob(t *testing.T) {
	adapter, ctx := devAdapter(t)
	run := testRun("lookup-unplaced")
	adapter.datacenters = []string{"wherry-no-such-datacenter"}
	cleanupRun(t, adapter, run)
	job, err := adapter.buildJob(run, variablePath(run.ID, run.Turn))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := adapter.client.Jobs().Register(job, nil); err != nil {
		t.Fatal(err)
	}
	lookupCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if _, ok, err := adapter.Lookup(lookupCtx, run.ID, run.Turn); ok || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lookup of a job without an allocation = %v, %v; want a wait that times out", ok, err)
	}
	if _, _, err := adapter.client.Jobs().Info(jobID(run.ID, run.Turn), nil); !notFound(err) {
		t.Fatalf("timed-out job still registered: %v", err)
	}
}

func TestReleaseDeletesVariable(t *testing.T) {
	adapter, ctx := devAdapter(t)
	run := testRun("release")
	dispatch(t, adapter, ctx, run)
	path := variablePath(run.ID, run.Turn)
	if variable, _, err := adapter.client.Variables().Peek(path, nil); err != nil || variable == nil {
		t.Fatalf("variable before release = %#v, %v", variable, err)
	}
	if err := adapter.Release(ctx, run.ID, run.Turn); err != nil {
		t.Fatal(err)
	}
	if variable, _, err := adapter.client.Variables().Peek(path, nil); err != nil || variable != nil {
		t.Fatalf("variable after release = %#v, %v", variable, err)
	}
	if err := adapter.Release(ctx, run.ID, run.Turn); err != nil {
		t.Fatalf("absent release failed: %v", err)
	}
}

func TestDispatchErrorDeletesVariable(t *testing.T) {
	adapter, ctx := devAdapter(t)
	run := testRun("error-cleanup")
	adapter.datacenters = []string{"wherry-no-such-datacenter"}
	cleanupRun(t, adapter, run)
	dispatchCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if _, err := adapter.Dispatch(dispatchCtx, run); err == nil {
		t.Fatal("unplaceable dispatch succeeded")
	}
	path := variablePath(run.ID, run.Turn)
	if variable, _, err := adapter.client.Variables().Peek(path, nil); err != nil || variable != nil {
		t.Fatalf("variable after dispatch error = %#v, %v", variable, err)
	}
}

func TestFollowAfterTaskEnded(t *testing.T) {
	adapter, ctx := devAdapter(t)
	run := testRun("follow-ended")
	handle := dispatch(t, adapter, ctx, run)
	for !adapter.finished(ctx, handle) {
		if ctx.Err() != nil {
			t.Fatal("task never finished")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if lines := collect(t, ctx, adapter, handle); len(lines) == 0 || !strings.Contains(string(lines[len(lines)-1].Data), `"result"`) {
		t.Fatalf("lines = %#v", lines)
	}
}

func TestLineReaderJoinsCRIPartials(t *testing.T) {
	raw := "t stdout P {\"type\":\nt stdout P \"result\",\nt stdout F \"result\":\"ok\"}\nnot framed\n"
	lines := make(chan receiver.Line, 4)
	reader := lineReader{lines: lines, cri: true}
	if err := reader.take(context.Background(), []byte(raw)); err != nil {
		t.Fatal(err)
	}
	first, second := <-lines, <-lines
	if string(first.Data) != `{"type":"result","result":"ok"}` || first.Offset != 0 || second.Offset != first.Next || string(second.Data) != "not framed" {
		t.Fatalf("first=%#v second=%#v", first, second)
	}
}

func TestLineReaderFlushesFinalLine(t *testing.T) {
	lines := make(chan receiver.Line, 1)
	reader := lineReader{lines: lines}
	if err := reader.take(context.Background(), []byte("tail")); err != nil {
		t.Fatal(err)
	}
	if err := reader.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if line := <-lines; string(line.Data) != "tail" || line.Next != 4 {
		t.Fatalf("line = %#v", line)
	}
}

// scriptRun returns a run whose task is the given shell script.
func scriptRun(adapter *Adapter, name, script string) receiver.Run {
	adapter.runtime = Runtime{Driver: "raw_exec", Config: map[string]any{"command": "/bin/sh", "args": []any{"-c", script}}}
	return testRun(name)
}

func waitRunning(t *testing.T, ctx context.Context, adapter *Adapter, handle receiver.Handle) {
	t.Helper()
	for {
		alloc, _, err := adapter.client.Allocations().Info(handle.Alloc, nil)
		if err != nil {
			t.Fatal(err)
		}
		if alloc.ClientStatus == api.AllocClientStatusRunning {
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("allocation never ran: %s", alloc.ClientStatus)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func TestStopDoesNotReplace(t *testing.T) {
	adapter, ctx := devAdapter(t)
	handle := dispatch(t, adapter, ctx, scriptRun(adapter, "stop-replace", `trap '' TERM; sleep 120`))
	waitRunning(t, ctx, adapter, handle)
	if err := adapter.Stop(ctx, handle); err != nil {
		t.Fatal(err)
	}
	var allocs []*api.AllocationListStub
	for watchEnd := time.Now().Add(20 * time.Second); time.Now().Before(watchEnd); time.Sleep(time.Second) {
		var err error
		if allocs, _, err = adapter.client.Jobs().Allocations(handle.Job, true, nil); err != nil {
			t.Fatal(err)
		}
		if len(allocs) != 1 {
			var states []string
			for _, alloc := range allocs {
				states = append(states, alloc.ClientStatus+"/"+alloc.DesiredDescription)
			}
			t.Fatalf("job has %d allocations after stop, want 1: %v", len(allocs), states)
		}
	}
	if !terminal(allocs[0].ClientStatus) {
		t.Fatalf("allocation still %s 20 s after stop", allocs[0].ClientStatus)
	}
}

func TestStopSignalsRun(t *testing.T) {
	adapter, ctx := devAdapter(t)
	script := `trap 'echo "{\"type\":\"result\",\"result\":\"terminated\"}"; exit 143' TERM; echo started; sleep 60 & wait $!`
	handle := dispatch(t, adapter, ctx, scriptRun(adapter, "stop-signal", script))
	waitRunning(t, ctx, adapter, handle)
	if err := adapter.Stop(ctx, handle); err != nil {
		t.Fatal(err)
	}
	lines := collect(t, ctx, adapter, handle)
	if len(lines) == 0 || string(lines[len(lines)-1].Data) != `{"type":"result","result":"terminated"}` {
		t.Fatalf("lines = %#v, want the trap's output last", lines)
	}
	alloc, _, err := adapter.client.Allocations().Info(handle.Alloc, nil)
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := adapter.client.Jobs().Info(handle.Job, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !alloc.ClientTerminalStatus() || alloc.DesiredStatus != api.AllocDesiredStatusRun || *job.Stop {
		t.Fatalf("client=%s desired=%s job stopped=%v; want the signal alone to end the run", alloc.ClientStatus, alloc.DesiredStatus, *job.Stop)
	}
}

func TestStopAlreadyTerminalDoesNotSignal(t *testing.T) {
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requests = append(requests, request.Method+" "+request.URL.Path)
		if request.Method == http.MethodGet && request.URL.Path == "/v1/allocation/alloc-terminal" {
			_ = json.NewEncoder(response).Encode(&api.Allocation{ID: "alloc-terminal", ClientStatus: api.AllocClientStatusComplete})
			return
		}
		http.Error(response, "unexpected request", http.StatusInternalServerError)
	}))
	defer server.Close()
	adapter, err := New(Config{Address: server.URL, Runtime: Runtime{Driver: "raw_exec"}}, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Stop(context.Background(), receiver.Handle{Job: "agent-terminal", Alloc: "alloc-terminal"}); err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 || requests[0] != "GET /v1/allocation/alloc-terminal" {
		t.Fatalf("requests = %v, want terminal check only", requests)
	}
}

func TestDispatchTimeoutDeregisters(t *testing.T) {
	adapter, ctx := devAdapter(t)
	run := testRun("dispatch-timeout")
	adapter.datacenters = []string{"wherry-no-such-datacenter"}
	cleanupRun(t, adapter, run)
	dispatchCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if _, err := adapter.Dispatch(dispatchCtx, run); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("dispatch of an unplaceable job = %v; want a wait that times out", err)
	}
	if _, _, err := adapter.client.Jobs().Info(jobID(run.ID, run.Turn), nil); !notFound(err) {
		t.Fatalf("timed-out job still registered: %v", err)
	}
}

func TestDeregisterTimedOutPurgesJob(t *testing.T) {
	var purge string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodDelete || request.URL.Path != "/v1/job/agent-timed-out" {
			http.Error(response, "unexpected request", http.StatusInternalServerError)
			return
		}
		purge = request.URL.Query().Get("purge")
		_, _ = response.Write([]byte(`{"EvalID":""}`))
	}))
	defer server.Close()
	adapter, err := New(Config{Address: server.URL, Runtime: Runtime{Driver: "raw_exec"}}, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := adapter.deregisterTimedOut(ctx, "agent-timed-out"); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
	if purge != "true" {
		t.Fatalf("purge = %q, want true", purge)
	}
}

func follow(t *testing.T, ctx context.Context, adapter *Adapter, handle receiver.Handle, offset int64, count int) []receiver.Line {
	t.Helper()
	followCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	lines := make(chan receiver.Line, 16)
	done := make(chan error, 1)
	go func() { done <- adapter.Follow(followCtx, handle, offset, lines) }()
	var got []receiver.Line
	for len(got) < count {
		select {
		case line := <-lines:
			got = append(got, line)
		case err := <-done:
			for len(got) < count && len(lines) > 0 {
				got = append(got, <-lines)
			}
			if len(got) < count {
				t.Fatalf("follow ended after %d of %d lines: %v", len(got), count, err)
			}
		case <-ctx.Done():
			t.Fatal("follow timed out")
		}
	}
	return got
}

func TestFollowOffsetsAndReattach(t *testing.T) {
	adapter, ctx := devAdapter(t)
	handle := dispatch(t, adapter, ctx, scriptRun(adapter, "follow-offsets", `echo '{"type":"system"}'; echo '{"type":"assistant"}'; echo '{"type":"result","result":"ok"}'`))
	all := follow(t, ctx, adapter, handle, 0, 3)
	for i, line := range all {
		want := int64(0)
		if i > 0 {
			want = all[i-1].Next
		}
		if line.Offset != want || line.Next != line.Offset+int64(len(line.Data))+1 {
			t.Fatalf("line %d = %#v, want offset %d", i, line, want)
		}
	}
	resumed := follow(t, ctx, adapter, handle, all[1].Offset, 2)
	if !reflect.DeepEqual(resumed, all[1:]) {
		t.Fatalf("follow from offset %d = %#v, want %#v", all[1].Offset, resumed, all[1:])
	}
	first := follow(t, ctx, adapter, handle, 0, 1)
	rest := follow(t, ctx, adapter, handle, first[0].Next, 2)
	if !reflect.DeepEqual(append(first, rest...), all) {
		t.Fatalf("reattach from %d = %#v, want %#v", first[0].Next, rest, all[1:])
	}
}

func TestFollowAcrossLogRotation(t *testing.T) {
	adapter, ctx := devAdapter(t)
	const count = 150000 // 80-byte lines: 12 MB, past the 10 MB log file size
	script := `awk 'BEGIN { for (i = 0; i < 150000; i++) printf "line-%06d-%s\n", i, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" }'; echo '{"type":"result","result":"ok"}'`
	handle := dispatch(t, adapter, ctx, scriptRun(adapter, "rotation", script))
	all := collect(t, ctx, adapter, handle)
	if len(all) != count+1 || all[count].Next <= 10<<20 {
		t.Fatalf("got %d lines ending at %d, want %d past 10 MB", len(all), all[len(all)-1].Next, count+1)
	}
	for i, line := range all[:count] {
		if string(line.Data[:12]) != fmt.Sprintf("line-%06d-", i) || line.Offset != int64(i*80) || line.Next != line.Offset+80 {
			t.Fatalf("line %d = %.20q offset %d next %d", i, line.Data, line.Offset, line.Next)
		}
	}
	alloc, _, err := adapter.client.Allocations().Info(handle.Alloc, nil)
	if err != nil {
		t.Fatal(err)
	}
	files, _, err := adapter.client.AllocFS().List(alloc, "alloc/logs", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(files, func(file *api.AllocFileInfo) bool { return file.Name == taskName+".stdout.1" }) {
		t.Fatalf("log did not rotate: %d files", len(files))
	}
	mid := all[count-10]
	resumed := follow(t, ctx, adapter, handle, mid.Offset, 11)
	if !reflect.DeepEqual(resumed, all[count-10:]) {
		t.Fatalf("follow from %d after rotation = %.80q", mid.Offset, resumed[0].Data)
	}
}

// The fixture is real Podman-driver stdout captured from the lab cluster.
func TestLineReaderDecodesPodmanCRI(t *testing.T) {
	raw, err := os.ReadFile("testdata/podman-cri-stdout.log")
	if err != nil {
		t.Fatal(err)
	}
	lines := make(chan receiver.Line, 16)
	reader := lineReader{lines: lines, cri: true}
	if err := reader.take(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	got := closeAndRead(lines)
	if len(got) != 4 {
		t.Fatalf("got %d lines, want 4", len(got))
	}
	for i, line := range got {
		if !strings.HasPrefix(string(line.Data), "{") {
			t.Fatalf("line %d still framed: %.50q", i, line.Data)
		}
	}
	if !strings.Contains(string(got[3].Data), `"type":"result"`) || got[3].Next != int64(len(raw)) {
		t.Fatalf("last line %.80q next=%d, want result ending at %d", got[3].Data, got[3].Next, len(raw))
	}
}

func TestCRIPartialBounded(t *testing.T) {
	part := strings.Repeat("x", maxPending/2)
	firstRaw := "t stdout P " + part + "\n"
	secondRaw := "t stdout P " + part + "\n"
	finalRaw := "t stdout F tail\n"
	lines := make(chan receiver.Line, 3)
	reader := lineReader{lines: lines, cri: true}
	if err := reader.take(context.Background(), []byte(firstRaw+secondRaw+finalRaw)); err != nil {
		t.Fatal(err)
	}
	got := closeAndRead(lines)
	if len(got) != 2 {
		t.Fatalf("got %d lines, want 2", len(got))
	}
	first, second := got[0], got[1]
	if len(first.Data) != maxPending || first.Offset != 0 || first.Next != int64(len(firstRaw)+len(secondRaw)) {
		t.Fatalf("bounded partial len=%d offset=%d next=%d", len(first.Data), first.Offset, first.Next)
	}
	if string(second.Data) != "tail" || second.Offset != first.Next || second.Next != int64(len(firstRaw)+len(secondRaw)+len(finalRaw)) {
		t.Fatalf("final part = %#v", second)
	}
}

func TestCRIMalformedAfterPartial(t *testing.T) {
	partialRaw := "t stdout P partial\n"
	malformedRaw := "not framed\n"
	lines := make(chan receiver.Line, 2)
	reader := lineReader{lines: lines, cri: true}
	if err := reader.take(context.Background(), []byte(partialRaw+malformedRaw)); err != nil {
		t.Fatal(err)
	}
	got := closeAndRead(lines)
	if len(got) != 2 {
		t.Fatalf("got %d lines, want 2", len(got))
	}
	partial, malformed := got[0], got[1]
	if string(partial.Data) != "partial" || partial.Offset != 0 || partial.Next != int64(len(partialRaw)) {
		t.Fatalf("partial = %#v", partial)
	}
	if string(malformed.Data) != "not framed" || malformed.Offset != partial.Next || malformed.Next != int64(len(partialRaw)+len(malformedRaw)) {
		t.Fatalf("malformed = %#v", malformed)
	}
}

func TestCRIFlushEmitsPendingPartial(t *testing.T) {
	partialRaw := "t stdout P partial\n"
	pendingRaw := "raw-tail"
	lines := make(chan receiver.Line, 2)
	reader := lineReader{lines: lines, cri: true}
	if err := reader.take(context.Background(), []byte(partialRaw+pendingRaw)); err != nil {
		t.Fatal(err)
	}
	if err := reader.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := closeAndRead(lines)
	if len(got) != 2 {
		t.Fatalf("got %d lines, want 2", len(got))
	}
	partial, pending := got[0], got[1]
	if string(partial.Data) != "partial" || partial.Offset != 0 || partial.Next != int64(len(partialRaw)) {
		t.Fatalf("partial = %#v", partial)
	}
	if string(pending.Data) != pendingRaw || pending.Offset != partial.Next || pending.Next != int64(len(partialRaw)+len(pendingRaw)) {
		t.Fatalf("pending = %#v", pending)
	}
}

func closeAndRead(lines chan receiver.Line) []receiver.Line {
	close(lines)
	var got []receiver.Line
	for line := range lines {
		got = append(got, line)
	}
	return got
}

func TestSecretsNeverInRequestURLs(t *testing.T) {
	const token = "nomad-token-secret"
	const commit = "0123456789abcdef0123456789abcdef01234567"
	run := testRun("urls")
	run.ID = "run-urls"
	run.Git = &wire.GitContext{URL: "https://git.example.test/org/private-repo.git", Ref: "main", Path: "repo"}
	var mu sync.Mutex
	var urls []string
	registered := false
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		urls = append(urls, request.Method+" "+request.URL.Path+"?"+request.URL.RawQuery)
		if request.Header.Get("X-Nomad-Token") != token {
			http.Error(response, "missing token", http.StatusForbidden)
			return
		}
		switch request.Method + " " + request.URL.Path {
		case "PUT /v1/var/nomad/jobs/agent-run-urls-t1", "DELETE /v1/var/nomad/jobs/agent-run-urls-t1",
			"PUT /v1/var/nomad/jobs/agent-run-urls-t1/agent/agent", "DELETE /v1/var/nomad/jobs/agent-run-urls-t1/agent/agent",
			"PUT /v1/var/nomad/jobs/agent-run-urls-t1/agent/git", "DELETE /v1/var/nomad/jobs/agent-run-urls-t1/agent/git":
			_, _ = response.Write([]byte(`{}`))
		case "GET /v1/var/nomad/jobs/agent-run-urls-t1":
			_ = json.NewEncoder(response).Encode(&api.Variable{Path: "nomad/jobs/agent-run-urls-t1", Items: map[string]string{"prompt": string(run.Prompt)}})
		case "GET /v1/var/nomad/jobs/agent-run-urls-t1/agent/git":
			_ = json.NewEncoder(response).Encode(&api.Variable{Path: "nomad/jobs/agent-run-urls-t1/agent/git", Items: map[string]string{
				"git_url": run.Git.URL, "git_ref": run.Git.Ref, "git_path": run.Git.Path,
			}})
		case "GET /v1/job/agent-run-urls-t1":
			if !registered {
				http.Error(response, "job not found", http.StatusNotFound)
				return
			}
			_, _ = response.Write([]byte(`{"ID":"agent-run-urls-t1","Status":"dead","Stop":false,"TaskGroups":[{"Tasks":[{"Name":"agent","Driver":"raw_exec"},{"Name":"git","Driver":"raw_exec"}]}]}`))
		case "GET /v1/vars":
			_, _ = response.Write([]byte(`[{"Path":"nomad/jobs/agent-run-urls-t1"},{"Path":"nomad/jobs/agent-run-urls-t1/agent/agent"},{"Path":"nomad/jobs/agent-run-urls-t1/agent/git"}]`))
		case "PUT /v1/jobs":
			registered = true
			_, _ = response.Write([]byte(`{}`))
		case "GET /v1/job/agent-run-urls-t1/allocations":
			_, _ = response.Write([]byte(`[{"ID":"alloc-urls","ClientStatus":"complete","TaskStates":{"git":{"State":"dead"}}}]`))
		case "GET /v1/allocation/alloc-urls":
			_, _ = response.Write([]byte(`{"ID":"alloc-urls","ClientStatus":"complete"}`))
		case "GET /v1/node/":
			http.Error(response, "node not found", http.StatusNotFound)
		case "GET /v1/client/fs/logs/alloc-urls":
			_ = json.NewEncoder(response).Encode(&api.StreamFrame{Data: []byte("/alloc/repo\n" + commit + "\n")})
		default:
			http.Error(response, "unexpected request", http.StatusInternalServerError)
		}
	}))
	defer server.Close()
	adapter, err := New(testConfig(server.URL), token)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	handle, err := adapter.Dispatch(ctx, run)
	if err != nil || handle.Commit != commit {
		t.Fatalf("dispatch = %#v, %v", handle, err)
	}
	if found, ok, err := adapter.Lookup(ctx, run.ID, run.Turn); err != nil || !ok || found.Commit != commit {
		t.Fatalf("lookup = %#v, %v, %v", found, ok, err)
	}
	if err := adapter.Stop(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Release(ctx, run.ID, run.Turn); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(urls) != 30 {
		t.Fatalf("requests = %d %v, want dispatch and lookup with prompt delivery, stop, release and sweep", len(urls), urls)
	}
	for _, url := range urls {
		for _, secret := range []string{token, string(run.Credential), string(run.Prompt), run.Git.URL, "private-repo"} {
			if strings.Contains(url, secret) {
				t.Fatalf("request URL %q contains %q", url, secret)
			}
		}
	}
}

func TestRedactedDropsResponseBody(t *testing.T) {
	const echoed = `{"Items":{"credential":"test-credential","prompt":"hello-prompt"}}`
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		http.Error(response, echoed, http.StatusInternalServerError)
	}))
	defer server.Close()
	adapter, err := New(Config{Address: server.URL, Runtime: Runtime{Driver: "raw_exec"}}, "")
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = adapter.client.Variables().Update(&api.Variable{Path: "nomad/jobs/agent-run-redact", Items: map[string]string{"credential": "test-credential"}}, nil)
	var response api.UnexpectedResponseError
	if !errors.As(err, &response) || !strings.Contains(err.Error(), "test-credential") {
		t.Fatalf("fixture error %v does not echo the body", err)
	}
	got := redacted("write", "nomad/jobs/agent-run-redact", err).Error()
	if strings.Contains(got, "test-credential") || strings.Contains(got, "hello-prompt") {
		t.Fatalf("redacted error %q contains variable items", got)
	}
	if err := adapter.Sweep(context.Background()); err == nil || strings.Contains(err.Error(), "test-credential") {
		t.Fatalf("sweep error %v, want a redacted failure", err)
	}
}

func TestBuildJobOmitsPlaintext(t *testing.T) {
	adapter, err := New(testConfig("http://127.0.0.1:1"), "")
	if err != nil {
		t.Fatal(err)
	}
	run := testRun("plaintext")
	run.Git = &wire.GitContext{URL: "https://git.example.test/org/private-repo.git", Ref: "main", Path: "repo"}
	job, err := adapter.buildJob(run, variablePath(run.ID, run.Turn))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{string(run.Credential), string(run.Prompt), run.Git.URL} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("job JSON contains %q", secret)
		}
	}
}

func sleepRun(adapter *Adapter, name string) receiver.Run {
	adapter.runtime = Runtime{Driver: "raw_exec", Config: map[string]any{"command": "/bin/sleep", "args": []any{"300"}}}
	run := testRun(name)
	return run
}

func putVariable(t *testing.T, adapter *Adapter, ctx context.Context, path string) {
	t.Helper()
	t.Cleanup(func() { _, _ = adapter.client.Variables().Delete(path, nil) })
	if _, _, err := adapter.client.Variables().Update(&api.Variable{Path: path, Items: map[string]string{"credential": "test-credential"}}, (&api.WriteOptions{}).WithContext(ctx)); err != nil {
		t.Fatal(redacted("write", path, err))
	}
}

func sweepLeaves(t *testing.T, adapter *Adapter, ctx context.Context, path string, want bool) {
	t.Helper()
	if err := adapter.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	variable, _, err := adapter.client.Variables().Peek(path, nil)
	if err != nil {
		t.Fatal(redacted("peek", path, err))
	}
	if (variable != nil) != want {
		t.Fatalf("variable %s present after sweep = %v, want %v", path, variable != nil, want)
	}
}

func shortGrace(t *testing.T) {
	previous := sweepGrace
	sweepGrace = 0
	t.Cleanup(func() { sweepGrace = previous })
}

func TestSweepDeletesVariableOfDeadJob(t *testing.T) {
	adapter, ctx := devAdapter(t)
	run := testRun("sweep-dead")
	dispatch(t, adapter, ctx, run)
	for {
		job, _, err := adapter.client.Jobs().Info(jobID(run.ID, run.Turn), (&api.QueryOptions{}).WithContext(ctx))
		if err != nil {
			t.Fatal(err)
		}
		if *job.Status == "dead" {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	sweepLeaves(t, adapter, ctx, variablePath(run.ID, run.Turn), false)
}

func TestSweepDeletesVariableOfStoppedJob(t *testing.T) {
	adapter, ctx := devAdapter(t)
	// A running task that ignores SIGTERM keeps the job stopped but not dead for the kill timeout.
	run := scriptRun(adapter, "sweep-stopped", `trap '' TERM; sleep 300`)
	waitRunning(t, ctx, adapter, dispatch(t, adapter, ctx, run))
	if _, _, err := adapter.client.Jobs().Deregister(jobID(run.ID, run.Turn), false, (&api.WriteOptions{}).WithContext(ctx)); err != nil {
		t.Fatal(err)
	}
	job, _, err := adapter.client.Jobs().Info(jobID(run.ID, run.Turn), (&api.QueryOptions{}).WithContext(ctx))
	if err != nil || !*job.Stop || *job.Status == "dead" {
		t.Fatalf("job after stop = %#v, %v; want stopped and not yet dead", job, err)
	}
	sweepLeaves(t, adapter, ctx, variablePath(run.ID, run.Turn), false)
}

func TestSweepKeepsVariableOfRunningJob(t *testing.T) {
	adapter, ctx := devAdapter(t)
	shortGrace(t)
	run := sleepRun(adapter, "sweep-running")
	dispatch(t, adapter, ctx, run)
	sweepLeaves(t, adapter, ctx, variablePath(run.ID, run.Turn), true)
}

func TestSweepKeepsRecentVariableWithoutJob(t *testing.T) {
	adapter, ctx := devAdapter(t)
	run := testRun("sweep-recent")
	path := variablePath(run.ID, run.Turn)
	putVariable(t, adapter, ctx, path)
	sweepLeaves(t, adapter, ctx, path, true)
}

func TestSweepDeletesStaleVariableWithoutJob(t *testing.T) {
	adapter, ctx := devAdapter(t)
	shortGrace(t)
	run := testRun("sweep-stale")
	path := variablePath(run.ID, run.Turn)
	putVariable(t, adapter, ctx, path)
	sweepLeaves(t, adapter, ctx, path, false)
}

func TestSweepIgnoresNonRunVariables(t *testing.T) {
	adapter, ctx := devAdapter(t)
	shortGrace(t)
	for _, path := range []string{"nomad/jobs/agent-foo-t1/prompt", "nomad/jobs/agent-foo"} {
		putVariable(t, adapter, ctx, path)
		sweepLeaves(t, adapter, ctx, path, true)
	}
}

// bareRepository creates a bare repository with two commits and returns its file URL and the
// commits, oldest first. The URL is a test-only stand-in for the https and ssh URLs that
// wire.ParseGitPart admits.
func bareRepository(t *testing.T) (string, []string) {
	t.Helper()
	dir := t.TempDir()
	work := dir + "/work"
	git := func(args ...string) string {
		t.Helper()
		command := exec.Command("git", append([]string{"-c", "user.name=wherry", "-c", "user.email=wherry@example.test", "-c", "init.defaultBranch=main"}, args...)...)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
		return strings.TrimSpace(string(output))
	}
	git("init", "-q", work)
	var commits []string
	for _, content := range []string{"first-readme", "second-readme"} {
		if err := os.WriteFile(work+"/README", []byte(content+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		git("-C", work, "add", "README")
		git("-C", work, "commit", "-q", "-m", content)
		commits = append(commits, git("-C", work, "rev-parse", "HEAD"))
	}
	git("clone", "-q", "--bare", work, dir+"/repo.git")
	return "file://" + dir + "/repo.git", commits
}

func gitRun(adapter *Adapter, name, script, url, ref string) receiver.Run {
	run := scriptRun(adapter, name, script)
	run.Git = &wire.GitContext{URL: url, Ref: ref, Path: "src/repo"}
	return run
}

func TestDispatchClonesGitContext(t *testing.T) {
	adapter, ctx := devAdapter(t)
	url, commits := bareRepository(t)
	script := `cat "$NOMAD_TASK_DIR/prompt.txt"; echo; echo "readme=$(cat "$NOMAD_ALLOC_DIR/src/repo/README")"; echo "head=$(git -C "$NOMAD_ALLOC_DIR/src/repo" rev-parse HEAD)"; echo '{"type":"result","result":"ok"}'`
	for ref, want := range map[string]struct{ commit, readme string }{
		"main":     {commits[1], "second-readme"},
		commits[0]: {commits[0], "first-readme"},
	} {
		t.Run(ref, func(t *testing.T) {
			run := gitRun(adapter, "git-clone", script, url, ref)
			handle := dispatch(t, adapter, ctx, run)
			if handle.Commit != want.commit {
				t.Fatalf("handle commit %q, want %q", handle.Commit, want.commit)
			}
			var got []string
			for _, line := range collect(t, ctx, adapter, handle) {
				got = append(got, string(line.Data))
			}
			alloc, _, err := adapter.client.Allocations().Info(handle.Alloc, nil)
			if err != nil {
				t.Fatal(err)
			}
			line := fmt.Sprintf("The git repository %s at %s is checked out in ", url, ref)
			if len(got) != 5 || got[0] != "hello-prompt" || !strings.HasPrefix(got[1], line) || !strings.HasSuffix(got[1], alloc.ID+"/alloc/src/repo at commit "+want.commit+".") {
				t.Fatalf("prompt is not the run's prompt and one repository line: %q", got)
			}
			if got[2] != "readme="+want.readme || got[3] != "head="+want.commit {
				t.Fatalf("agent task did not see the checkout: %q", got)
			}
			job, _, err := adapter.client.Jobs().Info(handle.Job, nil)
			if err != nil {
				t.Fatal(err)
			}
			if tasks := job.TaskGroups[0].Tasks; len(tasks) != 2 || tasks[1].Name != gitTaskName || tasks[1].Lifecycle == nil ||
				tasks[1].Lifecycle.Hook != api.TaskLifecycleHookPrestart || tasks[1].Lifecycle.Sidecar || tasks[1].Driver != "raw_exec" {
				t.Fatalf("job tasks %#v, want the agent and a prestart git task", tasks)
			}
		})
	}
}

func TestCloneUsesStrictHostKeyChecking(t *testing.T) {
	adapter, err := New(testConfig("http://127.0.0.1:1"), "")
	if err != nil {
		t.Fatal(err)
	}
	run := testRun("strict")
	adapter.runtime.Env = map[string]string{"GIT_SSH_COMMAND": "ssh -o StrictHostKeyChecking=accept-new"}
	run.Git = &wire.GitContext{URL: "ssh://git@git.example.test/org/repo.git", Ref: "main", Path: "repo"}
	job, err := adapter.buildJob(run, variablePath(run.ID, run.Turn))
	if err != nil {
		t.Fatal(err)
	}
	tasks := job.TaskGroups[0].Tasks
	if len(tasks) != 2 || tasks[1].Name != gitTaskName {
		t.Fatalf("job tasks %#v, want the agent and the git task", tasks)
	}
	if got := tasks[1].Env["GIT_SSH_COMMAND"]; got != "ssh -o StrictHostKeyChecking=yes -o BatchMode=yes" {
		t.Fatalf("git task GIT_SSH_COMMAND = %q, want strict host key checking", got)
	}
	encoded, err := json.Marshal(tasks[1])
	if err != nil {
		t.Fatal(err)
	}
	for _, unwanted := range []string{"accept-new", "IdentityFile", "UserKnownHostsFile", "-i "} {
		if strings.Contains(string(encoded), unwanted) {
			t.Fatalf("git task carries %q", unwanted)
		}
	}
}

func TestGitCloneFailureEndsRun(t *testing.T) {
	adapter, ctx := devAdapter(t)
	missing := "file://" + t.TempDir() + "/missing.git"
	run := gitRun(adapter, "git-failure", `echo '{"type":"result","result":"agent ran"}'`, missing, "main")
	cleanupRun(t, adapter, run)
	_, err := adapter.Dispatch(ctx, run)
	if !errors.Is(err, receiver.ErrGitContext) || err.Error() != "git context unavailable" {
		t.Fatalf("dispatch error %v, want the fixed git context error", err)
	}
	allocs, _, err := adapter.client.Jobs().Allocations(jobID(run.ID, run.Turn), true, nil)
	if err != nil || len(allocs) != 1 {
		t.Fatalf("allocations = %d, %v", len(allocs), err)
	}
	if state := allocs[0].TaskStates[taskName]; state != nil && !state.StartedAt.IsZero() {
		t.Fatalf("agent task started after a failed clone: %#v", state)
	}
	for _, path := range []string{variablePath(run.ID, run.Turn), variablePath(run.ID, run.Turn) + promptSuffix, variablePath(run.ID, run.Turn) + gitSuffix} {
		if variable, _, err := adapter.client.Variables().Peek(path, nil); err != nil || variable != nil {
			t.Fatalf("variable %s after a failed clone = %#v, %v", path, variable, err)
		}
	}
}

func TestRunVariableHoldsNoGitMaterial(t *testing.T) {
	adapter, ctx := devAdapter(t)
	url, _ := bareRepository(t)
	run := gitRun(adapter, "git-variables", `echo '{"type":"result","result":"ok"}'`, url, "main")
	dispatch(t, adapter, ctx, run)
	path := variablePath(run.ID, run.Turn)
	variable, _, err := adapter.client.Variables().Peek(path, nil)
	if err != nil || variable == nil {
		t.Fatalf("run variable = %#v, %v", variable, err)
	}
	for key, value := range variable.Items {
		if strings.HasPrefix(key, "git_") || strings.Contains(value, url) {
			t.Fatalf("job-level run variable carries git material in item %q", key)
		}
	}
	git, _, err := adapter.client.Variables().Peek(path+gitSuffix, nil)
	if err != nil || git == nil || git.Items["git_url"] != url {
		t.Fatal("git task variable does not hold the git context")
	}
	if err := adapter.Release(ctx, run.ID, run.Turn); err != nil {
		t.Fatal(err)
	}
	if git, _, err := adapter.client.Variables().Peek(path+gitSuffix, nil); err != nil || git != nil {
		t.Fatalf("git task variable after release = %v, %v", git != nil, err)
	}
}

func TestBuildJobLeavesUnsetPlacementToNomad(t *testing.T) {
	config := testConfig("http://127.0.0.1:1")
	config.NodePool, config.Datacenters = "", nil
	adapter, err := New(config, "")
	if err != nil {
		t.Fatal(err)
	}
	run := testRun("defaults")
	job, err := adapter.buildJob(run, variablePath(run.ID, run.Turn))
	if err != nil {
		t.Fatal(err)
	}
	if job.NodePool != nil || !reflect.DeepEqual(job.Datacenters, []string{"*"}) {
		t.Fatalf("node pool %v, datacenters %v; want Nomad's defaults", job.NodePool, job.Datacenters)
	}
}

func TestReadyFiltersNodesByPoolDatacenterAndDriver(t *testing.T) {
	node := func(pool, datacenter, driver string, healthy bool) map[string]any {
		return map[string]any{"ID": pool + datacenter + driver, "Status": "ready", "SchedulingEligibility": "eligible", "NodePool": pool, "Datacenter": datacenter,
			"Drivers": map[string]any{driver: map[string]any{"Detected": true, "Healthy": healthy}}}
	}
	cases := []struct {
		name  string
		pool  string
		dcs   []string
		nodes []map[string]any
		want  bool
	}{
		{"default pool when unset", "", nil, []map[string]any{node("OCPv", "dc1", "raw_exec", true)}, false},
		{"unset pool finds default", "", nil, []map[string]any{node("default", "dc1", "raw_exec", true)}, true},
		{"configured pool", "OCPv", nil, []map[string]any{node("OCPv", "dc1", "raw_exec", true)}, true},
		{"datacenter outside the list", "", []string{"dc2"}, []map[string]any{node("default", "dc1", "raw_exec", true)}, false},
		{"unhealthy driver", "", nil, []map[string]any{node("default", "dc1", "raw_exec", false)}, false},
		{"other driver", "", nil, []map[string]any{node("default", "dc1", "podman", true)}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(c.nodes)
			}))
			defer server.Close()
			adapter, err := New(Config{Address: server.URL, Runtime: Runtime{Driver: "raw_exec"}, NodePool: c.pool, Datacenters: c.dcs}, "")
			if err != nil {
				t.Fatal(err)
			}
			if ready, err := adapter.Ready(context.Background()); err != nil || ready != c.want {
				t.Fatalf("ready = %v, %v; want %v", ready, err, c.want)
			}
		})
	}
}
