package nomad

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benemon/agentbus/internal/receiver"
	"github.com/benemon/agentbus/internal/wire"
	"github.com/hashicorp/nomad/api"
)

const credentialEnv = "AGENTBUS_TEST_CREDENTIAL"

func testConfig(address string) Config {
	script := `printf '{"type":"result","result":"prompt=%s token=%s env=%s home=%s"}\n' "$(cat ${NOMAD_TASK_DIR}/prompt.txt)" "$` + credentialEnv + `" "$AGENTBUS_TEST_ENV" "$HOME"`
	return Config{
		Address: address, Namespace: "default", CredentialEnv: credentialEnv,
		Runtimes: map[string]Runtime{"exec": {
			Driver:    "raw_exec",
			Config:    map[string]any{"command": "/bin/sh", "args": []any{"-c", script}},
			Env:       map[string]string{"AGENTBUS_TEST_ENV": "from-runtime"},
			Resources: Resources{CPU: new(100), MemoryMB: new(64)},
		}},
	}
}

func devAdapter(t *testing.T) (*Adapter, context.Context) {
	t.Helper()
	address := os.Getenv("AGENTBUS_NOMAD_DEV_ADDR")
	if address == "" {
		t.Skip("AGENTBUS_NOMAD_DEV_ADDR not set")
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
		ID: id, Offer: map[string]string{"runtime": "exec", "nomad.node_pool": "default", "zone": "dc1"},
		Prompt: []byte("hello-prompt"), Credential: []byte("test-credential"),
	}
}

func cleanupRun(t *testing.T, adapter *Adapter, run receiver.Run) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = adapter.Release(ctx, run.ID)
		_, _, _ = adapter.client.Jobs().Deregister(jobPrefix+run.ID, true, (&api.WriteOptions{}).WithContext(ctx))
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

func TestOffersFromReadyNodes(t *testing.T) {
	adapter, ctx := devAdapter(t)
	offers, err := adapter.Offers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"runtime": "exec", "nomad.node_pool": "default", "zone": "dc1"}
	found := false
	for _, offer := range offers {
		if wire.TermsText(offer) == wire.TermsText(want) {
			found = true
		}
	}
	if !found {
		t.Fatalf("offers %#v do not contain %#v", offers, want)
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
	job, err := adapter.buildJob(run, variablePath(run.ID))
	if err != nil {
		t.Fatal(err)
	}
	if *job.ID != jobPrefix+run.ID || *job.Name != jobPrefix+run.ID || *job.Namespace != "default" || *job.Type != api.JobTypeBatch ||
		*job.NodePool != "default" || !reflect.DeepEqual(job.Datacenters, []string{"dc1"}) || !reflect.DeepEqual(job.Meta, map[string]string{"run_id": run.ID}) {
		t.Fatalf("unexpected job identity or placement: %#v", job)
	}
	if len(job.TaskGroups) != 1 || retriesOn(job.TaskGroups[0]) {
		t.Fatalf("retries not forced off: groups=%#v", job.TaskGroups)
	}
	runtime := testConfig("").Runtimes["exec"]
	tasks := job.TaskGroups[0].Tasks
	if len(tasks) != 1 || tasks[0].Name != taskName || tasks[0].Driver != "raw_exec" || !reflect.DeepEqual(tasks[0].Config, runtime.Config) ||
		*tasks[0].Resources.CPU != 100 || *tasks[0].Resources.MemoryMB != 64 ||
		*tasks[0].LogConfig.MaxFiles != 3 || *tasks[0].LogConfig.MaxFileSizeMB != 10 ||
		!reflect.DeepEqual(tasks[0].Env, map[string]string{"AGENTBUS_TEST_ENV": "from-runtime", "HOME": "${NOMAD_TASK_DIR}"}) {
		t.Fatalf("runtime not carried: %#v", tasks)
	}
	templates := tasks[0].Templates
	if len(templates) != 2 || *templates[0].DestPath != "local/prompt.txt" ||
		*templates[1].DestPath != "${NOMAD_SECRETS_DIR}/agentbus.env" || !*templates[1].Envvars ||
		!strings.Contains(*templates[1].EmbeddedTmpl, credentialEnv+"={{ .credential }}") ||
		*templates[0].ChangeMode != "noop" || *templates[1].ChangeMode != "noop" {
		t.Fatalf("templates %#v", templates)
	}
	run.Credential = nil
	job, err = adapter.buildJob(run, variablePath(run.ID))
	if err != nil {
		t.Fatal(err)
	}
	if templates := job.TaskGroups[0].Tasks[0].Templates; len(templates) != 1 || *templates[0].DestPath != "local/prompt.txt" {
		t.Fatalf("run without a credential has templates %#v", templates)
	}
}

func TestNewValidatesConfig(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"driver":         func(config *Config) { config.Runtimes["exec"] = Runtime{} },
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
	job, _, err := adapter.client.Jobs().Info(jobPrefix+run.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if *job.ID != jobPrefix+run.ID || *job.Name != jobPrefix+run.ID || *job.Type != api.JobTypeBatch ||
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
	before, _, err := adapter.client.Jobs().Info(jobPrefix+run.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.Dispatch(ctx, run); err != nil {
		t.Fatal(err)
	}
	after, _, err := adapter.client.Jobs().Info(jobPrefix+run.ID, nil)
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
	variable, _, err := adapter.client.Variables().Peek(variablePath(run.ID), nil)
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
	found, ok, err := adapter.Lookup(ctx, run.ID)
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
	handle, ok, err := adapter.Lookup(ctx, fmt.Sprintf("run-missing-%d", time.Now().UnixNano()))
	if err != nil || ok || handle != (receiver.Handle{}) {
		t.Fatalf("lookup = %#v, %v, %v", handle, ok, err)
	}
}

func TestLookupWaitsForUnplacedJob(t *testing.T) {
	adapter, ctx := devAdapter(t)
	run := testRun("lookup-unplaced")
	run.Offer["zone"] = "agentbus-no-such-datacenter"
	cleanupRun(t, adapter, run)
	job, err := adapter.buildJob(run, variablePath(run.ID))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := adapter.client.Jobs().Register(job, nil); err != nil {
		t.Fatal(err)
	}
	lookupCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if _, ok, err := adapter.Lookup(lookupCtx, run.ID); ok || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lookup of a job without an allocation = %v, %v; want a wait that times out", ok, err)
	}
	if _, _, err := adapter.client.Jobs().Info(jobPrefix+run.ID, nil); !notFound(err) {
		t.Fatalf("timed-out job still registered: %v", err)
	}
}

func TestReleaseDeletesVariable(t *testing.T) {
	adapter, ctx := devAdapter(t)
	run := testRun("release")
	dispatch(t, adapter, ctx, run)
	path := variablePath(run.ID)
	if variable, _, err := adapter.client.Variables().Peek(path, nil); err != nil || variable == nil {
		t.Fatalf("variable before release = %#v, %v", variable, err)
	}
	if err := adapter.Release(ctx, run.ID); err != nil {
		t.Fatal(err)
	}
	if variable, _, err := adapter.client.Variables().Peek(path, nil); err != nil || variable != nil {
		t.Fatalf("variable after release = %#v, %v", variable, err)
	}
	if err := adapter.Release(ctx, run.ID); err != nil {
		t.Fatalf("absent release failed: %v", err)
	}
}

func TestDispatchErrorDeletesVariable(t *testing.T) {
	adapter, ctx := devAdapter(t)
	run := testRun("error-cleanup")
	run.Offer["zone"] = "agentbus-no-such-datacenter"
	cleanupRun(t, adapter, run)
	dispatchCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if _, err := adapter.Dispatch(dispatchCtx, run); err == nil {
		t.Fatal("unplaceable dispatch succeeded")
	}
	path := variablePath(run.ID)
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
	adapter.runtimes[name] = Runtime{Driver: "raw_exec", Config: map[string]any{"command": "/bin/sh", "args": []any{"-c", script}}}
	run := testRun(name)
	run.Offer["runtime"] = name
	return run
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
	adapter, err := New(Config{Address: server.URL}, "")
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
	run.Offer["zone"] = "agentbus-no-such-datacenter"
	cleanupRun(t, adapter, run)
	dispatchCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if _, err := adapter.Dispatch(dispatchCtx, run); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("dispatch of an unplaceable job = %v; want a wait that times out", err)
	}
	if _, _, err := adapter.client.Jobs().Info(jobPrefix+run.ID, nil); !notFound(err) {
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
	adapter, err := New(Config{Address: server.URL}, "")
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
	run := testRun("urls")
	run.ID = "run-urls"
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
		case "PUT /v1/var/nomad/jobs/agent-run-urls", "DELETE /v1/var/nomad/jobs/agent-run-urls":
			_, _ = response.Write([]byte(`{}`))
		case "GET /v1/job/agent-run-urls":
			if !registered {
				http.Error(response, "job not found", http.StatusNotFound)
				return
			}
			_, _ = response.Write([]byte(`{"ID":"agent-run-urls","TaskGroups":[{"Tasks":[{"Driver":"raw_exec"}]}]}`))
		case "PUT /v1/jobs":
			registered = true
			_, _ = response.Write([]byte(`{}`))
		case "GET /v1/job/agent-run-urls/allocations":
			_, _ = response.Write([]byte(`[{"ID":"alloc-urls","ClientStatus":"complete"}]`))
		case "GET /v1/allocation/alloc-urls":
			_, _ = response.Write([]byte(`{"ID":"alloc-urls","ClientStatus":"complete"}`))
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
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := adapter.Lookup(ctx, run.ID); err != nil || !ok {
		t.Fatalf("lookup = %v, %v", ok, err)
	}
	if err := adapter.Stop(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Release(ctx, run.ID); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(urls) != 8 {
		t.Fatalf("requests = %v, want dispatch, lookup, stop and release", urls)
	}
	for _, url := range urls {
		for _, secret := range []string{token, string(run.Credential), string(run.Prompt)} {
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
	adapter, err := New(Config{Address: server.URL}, "")
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
}

func TestBuildJobOmitsPlaintext(t *testing.T) {
	adapter, err := New(testConfig("http://127.0.0.1:1"), "")
	if err != nil {
		t.Fatal(err)
	}
	run := testRun("plaintext")
	job, err := adapter.buildJob(run, variablePath(run.ID))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{string(run.Credential), string(run.Prompt)} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("job JSON contains %q", secret)
		}
	}
}
