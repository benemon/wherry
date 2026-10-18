// Package nomad is the compile-time Nomad platform adapter (ADR-0018).
package nomad

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"time"

	"github.com/benemon/wherry/internal/receiver"
	"github.com/benemon/wherry/internal/wire"
	"github.com/hashicorp/nomad/api"
)

const (
	jobPrefix         = "agent-"
	jobVariablePrefix = "nomad/jobs/"
	taskName          = "agent"
	gitTaskName       = "git"
	formatCRI         = "cri"
	maxPending        = 1 << 20
	stopGrace         = 15 * time.Second
	followPoll        = time.Second
	cleanupWait       = 10 * time.Second
)

// The suffixes turn a run variable path into a task's own variable path; among the job's tasks,
// Nomad's implicit workload policy lets only that task read it.
const (
	promptSuffix = "/" + taskName + "/" + taskName
	gitSuffix    = "/" + taskName + "/" + gitTaskName
)

// cloneScript reads the git context from a rendered file, so no task value becomes shell syntax or
// meets Nomad interpolation; refs and paths cannot start with a dash (wire.ParseGitPart). Its last
// two stdout lines are the checkout directory and the commit.
const cloneScript = `set -eu
{ read -r url; read -r ref; read -r dir; } < "$NOMAD_TASK_DIR/git"
dest="$NOMAD_ALLOC_DIR/$dir"
git init -q "$dest"
cd "$dest"
git remote add origin "$url"
git fetch -q --depth 1 origin "$ref"
git checkout -q --detach FETCH_HEAD
printf '%s\n' "$dest"
git rev-parse HEAD
`

// Config is the adapter's section of the receiver configuration.
type Config struct {
	Address       string             `json:"address"`
	CAFile        string             `json:"ca_file"`
	Namespace     string             `json:"namespace"`
	CredentialEnv string             `json:"credential_env"`
	Runtimes      map[string]Runtime `json:"runtimes"`
}

// Runtime defines the agent task for one advertised runtime value.
type Runtime struct {
	Driver    string            `json:"driver"`
	Config    map[string]any    `json:"config"`
	Env       map[string]string `json:"env"`
	Resources Resources         `json:"resources"`
}

// Resources are the agent task's resources; unset values take Nomad's defaults.
type Resources struct {
	CPU      *int `json:"cpu"`
	MemoryMB *int `json:"memory_mb"`
}

// Adapter registers per-run agent jobs in one namespace.
type Adapter struct {
	client        *api.Client
	namespace     string
	credentialEnv string
	runtimes      map[string]Runtime
}

var (
	envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	// A run variable, or a task variable of a run with a git context.
	runVariable = regexp.MustCompile(`^` + regexp.QuoteMeta(jobVariablePrefix) + `(` + regexp.QuoteMeta(jobPrefix) + `.+-t[0-9]+)(` + regexp.QuoteMeta(promptSuffix) + `|` + regexp.QuoteMeta(gitSuffix) + `)?$`)
	commit      = regexp.MustCompile(`^[0-9a-f]{40}([0-9a-f]{24})?$`)
	// sweepGrace covers Dispatch's window between writing the variable and registering the job.
	sweepGrace = 10 * time.Minute
)

// New builds a Nomad client; the token is the receiver's workload identity.
func New(config Config, token string) (*Adapter, error) {
	if config.CredentialEnv != "" && !envName.MatchString(config.CredentialEnv) {
		return nil, errors.New("credential_env must be an environment variable name")
	}
	for name, runtime := range config.Runtimes {
		if runtime.Driver == "" {
			return nil, fmt.Errorf("runtime %s requires a driver", name)
		}
	}
	apiConfig := api.DefaultConfig()
	apiConfig.Address = config.Address
	apiConfig.Namespace = config.Namespace
	apiConfig.SecretID = token
	apiConfig.TLSConfig = &api.TLSConfig{CACert: config.CAFile}
	client, err := api.NewClient(apiConfig)
	if err != nil {
		return nil, err
	}
	return &Adapter{client: client, namespace: config.Namespace, credentialEnv: config.CredentialEnv, runtimes: config.Runtimes}, nil
}

// Platform names the scheduler domain for labels.
func (adapter *Adapter) Platform() string { return "nomad" }

// Offers derives available runtime, node-pool and datacenter combinations from ready nodes.
func (adapter *Adapter) Offers(ctx context.Context) ([]map[string]string, error) {
	nodes, _, err := adapter.client.Nodes().List((&api.QueryOptions{}).WithContext(ctx))
	if err != nil {
		return nil, err
	}
	unique := make(map[string]map[string]string)
	for _, node := range nodes {
		if node.Status != api.NodeStatusReady || node.SchedulingEligibility != api.NodeSchedulingEligible {
			continue
		}
		for name, runtime := range adapter.runtimes {
			info := node.Drivers[runtime.Driver]
			if info == nil || !info.Detected || !info.Healthy {
				continue
			}
			offer := map[string]string{"runtime": name, "nomad.node_pool": node.NodePool, "zone": node.Datacenter}
			unique[wire.TermsText(offer)] = offer
		}
	}
	offers := make([]map[string]string, 0, len(unique))
	for _, key := range slices.Sorted(maps.Keys(unique)) {
		offers = append(offers, unique[key])
	}
	return offers, nil
}

// Dispatch starts one run idempotently and returns once its task has started or ended.
func (adapter *Adapter) Dispatch(ctx context.Context, run receiver.Run) (_ receiver.Handle, dispatchErr error) {
	path := variablePath(run.ID, run.Turn)
	job, err := adapter.buildJob(run, path)
	if err != nil {
		return receiver.Handle{}, err
	}
	items := map[string]string{"prompt": string(run.Prompt)}
	if run.Credential != nil {
		items["credential"] = string(run.Credential)
	}
	variable := &api.Variable{Namespace: adapter.namespace, Path: path, Items: items}
	if _, _, err := adapter.client.Variables().Update(variable, (&api.WriteOptions{}).WithContext(ctx)); err != nil {
		return receiver.Handle{}, redacted("write", path, err)
	}
	defer func() {
		if dispatchErr == nil {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupWait)
		defer cancel()
		if err := adapter.Release(cleanupCtx, run.ID, run.Turn); err != nil {
			dispatchErr = errors.Join(dispatchErr, err)
		}
	}()
	if run.Git != nil {
		gitItems := map[string]string{"git_url": run.Git.URL, "git_ref": run.Git.Ref, "git_path": run.Git.Path}
		gitPath := path + gitSuffix
		if _, _, err := adapter.client.Variables().Update(&api.Variable{Namespace: adapter.namespace, Path: gitPath, Items: gitItems}, (&api.WriteOptions{}).WithContext(ctx)); err != nil {
			return receiver.Handle{}, redacted("write", gitPath, err)
		}
	}

	jobID := *job.ID
	if _, _, err := adapter.client.Jobs().Info(jobID, (&api.QueryOptions{}).WithContext(ctx)); err != nil {
		if !notFound(err) {
			return receiver.Handle{}, err
		}
		if _, _, err := adapter.client.Jobs().RegisterOpts(job, &api.RegisterOptions{EnforceIndex: true, ModifyIndex: 0}, (&api.WriteOptions{}).WithContext(ctx)); err != nil {
			if _, _, existsErr := adapter.client.Jobs().Info(jobID, (&api.QueryOptions{}).WithContext(ctx)); existsErr != nil {
				return receiver.Handle{}, fmt.Errorf("register: %w", err)
			}
		}
	}
	return adapter.wait(ctx, jobID, job.TaskGroups[0].Tasks[0].Driver, run.Git != nil, path)
}

// Lookup finds a turn's existing job and waits for it as Dispatch does.
func (adapter *Adapter) Lookup(ctx context.Context, runID string, turn int64) (receiver.Handle, bool, error) {
	job, _, err := adapter.client.Jobs().Info(jobID(runID, turn), (&api.QueryOptions{}).WithContext(ctx))
	if notFound(err) {
		return receiver.Handle{}, false, nil
	}
	if err != nil {
		return receiver.Handle{}, false, err
	}
	if len(job.TaskGroups) != 1 {
		return receiver.Handle{}, false, errors.New("existing job is not an agent job")
	}
	driver, git := "", false
	for _, task := range job.TaskGroups[0].Tasks {
		switch task.Name {
		case taskName:
			driver = task.Driver
		case gitTaskName:
			git = true
		default:
			return receiver.Handle{}, false, errors.New("existing job is not an agent job")
		}
	}
	if driver == "" {
		return receiver.Handle{}, false, errors.New("existing job is not an agent job")
	}
	handle, err := adapter.wait(ctx, *job.ID, driver, git, variablePath(runID, turn))
	if err != nil {
		return receiver.Handle{}, false, err
	}
	return handle, true, nil
}

// wait returns once the job's agent task has started or ended; a wait cut short deregisters the job.
// With a git context, it delivers the agent's prompt once the clone task has finished.
func (adapter *Adapter) wait(ctx context.Context, jobID, driver string, git bool, path string) (receiver.Handle, error) {
	format := ""
	if driver == "podman" {
		format = formatCRI
	}
	checkedOut := ""
	for {
		allocs, _, err := adapter.client.Jobs().Allocations(jobID, false, (&api.QueryOptions{}).WithContext(ctx))
		if err != nil {
			if ctx.Err() != nil {
				return receiver.Handle{}, adapter.deregisterTimedOut(ctx, jobID)
			}
			return receiver.Handle{}, err
		}
		alloc := current(allocs)
		if alloc != nil && git && checkedOut == "" {
			switch state := alloc.TaskStates[gitTaskName]; {
			case state != nil && state.Failed:
				return receiver.Handle{}, receiver.ErrGitContext
			case state != nil && state.State == "dead":
				if checkedOut, err = adapter.deliverPrompt(ctx, alloc.ID, format == formatCRI, path); err != nil {
					return receiver.Handle{}, err
				}
			}
		}
		// The logs API answers 404 until the task has started.
		if alloc != nil && (terminal(alloc.ClientStatus) || started(alloc)) {
			return receiver.Handle{Job: jobID, Alloc: alloc.ID, Format: format, Commit: checkedOut}, nil
		}
		select {
		case <-ctx.Done():
			return receiver.Handle{}, adapter.deregisterTimedOut(ctx, jobID)
		case <-time.After(2 * time.Second):
		}
	}
}

// deliverPrompt reads the clone task's checkout and commit and writes the agent's prompt with a
// line naming them; the agent task's prompt template waits for that variable. It returns the commit.
func (adapter *Adapter) deliverPrompt(ctx context.Context, allocID string, cri bool, path string) (string, error) {
	alloc, _, err := adapter.client.Allocations().Info(allocID, (&api.QueryOptions{}).WithContext(ctx))
	if err != nil {
		return "", err
	}
	var output []byte
	frames, errs := adapter.logs(ctx, alloc, gitTaskName, false, 0)
	for done := false; !done; {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case err := <-errs:
			if err != nil {
				return "", err
			}
		case frame, ok := <-frames:
			if !ok {
				done = true
				break
			}
			output = append(output, frame.Data...)
		}
	}
	lines := make(chan receiver.Line, bytes.Count(output, []byte("\n"))+1)
	reader := lineReader{lines: lines, cri: cri}
	if err := reader.take(ctx, output); err != nil {
		return "", err
	}
	if err := reader.flush(ctx); err != nil {
		return "", err
	}
	close(lines)
	var printed []string
	for line := range lines {
		printed = append(printed, string(line.Data))
	}
	if len(printed) < 2 || !commit.MatchString(printed[len(printed)-1]) {
		return "", receiver.ErrGitContext
	}
	location, checkedOut := printed[len(printed)-2], printed[len(printed)-1]
	variable, _, err := adapter.client.Variables().Read(path, (&api.QueryOptions{}).WithContext(ctx))
	if err != nil {
		return "", redacted("read", path, err)
	}
	gitPath := path + gitSuffix
	git, _, err := adapter.client.Variables().Read(gitPath, (&api.QueryOptions{}).WithContext(ctx))
	if err != nil {
		return "", redacted("read", gitPath, err)
	}
	prompt := fmt.Sprintf("%s\nThe git repository %s at %s is checked out in %s at commit %s.", variable.Items["prompt"], git.Items["git_url"], git.Items["git_ref"], location, checkedOut)
	agentPath := path + promptSuffix
	if _, _, err := adapter.client.Variables().Update(&api.Variable{Namespace: adapter.namespace, Path: agentPath, Items: map[string]string{"prompt": prompt}}, (&api.WriteOptions{}).WithContext(ctx)); err != nil {
		return "", redacted("write", agentPath, err)
	}
	return checkedOut, nil
}

// buildJob takes nothing from the task except the run ID and the matched offer.
func (adapter *Adapter) buildJob(run receiver.Run, path string) (*api.Job, error) {
	runtime, ok := adapter.runtimes[run.Offer["runtime"]]
	if !ok {
		return nil, errors.New("unknown runtime")
	}
	jobID := jobID(run.ID, run.Turn)
	env := maps.Clone(runtime.Env)
	if env == nil {
		env = make(map[string]string)
	}
	env["HOME"] = "${NOMAD_TASK_DIR}"
	promptPath := path
	if run.Git != nil {
		promptPath = path + promptSuffix
	}
	templates := []*api.Template{
		{EmbeddedTmpl: new(`{{ with nomadVar "` + promptPath + `" }}{{ .prompt }}{{ end }}`), DestPath: new("local/prompt.txt"), ChangeMode: new("noop")},
	}
	if run.Credential != nil {
		templates = append(templates, &api.Template{
			EmbeddedTmpl: new(`{{ with nomadVar "` + path + `" }}` + adapter.credentialEnv + `={{ .credential }}{{ end }}`),
			DestPath:     new("${NOMAD_SECRETS_DIR}/wherry.env"), Envvars: new(true), ChangeMode: new("noop"),
		})
	}
	tasks := []*api.Task{{
		Name: taskName, Driver: runtime.Driver, Config: maps.Clone(runtime.Config), Env: env,
		Resources: &api.Resources{CPU: runtime.Resources.CPU, MemoryMB: runtime.Resources.MemoryMB},
		LogConfig: &api.LogConfig{MaxFiles: new(3), MaxFileSizeMB: new(10)},
		Templates: templates,
	}}
	if run.Git != nil {
		tasks = append(tasks, gitTask(runtime, path+gitSuffix))
	}
	return &api.Job{
		ID: new(jobID), Name: new(jobID), Namespace: new(adapter.namespace), Type: new(api.JobTypeBatch),
		NodePool: new(run.Offer["nomad.node_pool"]), Datacenters: []string{run.Offer["zone"]},
		Meta: map[string]string{"run_id": run.ID},
		TaskGroups: []*api.TaskGroup{{
			Name:             new(taskName),
			ReschedulePolicy: &api.ReschedulePolicy{Attempts: new(0), Unlimited: new(false)},
			RestartPolicy:    &api.RestartPolicy{Attempts: new(0), Mode: new(api.RestartPolicyModeFail)},
			Tasks:            tasks,
		}},
	}, nil
}

// gitTask is the prestart task that clones the run's git context into the shared alloc directory;
// repository access and host trust come from the platform (ADR-0024).
func gitTask(runtime Runtime, gitPath string) *api.Task {
	config := maps.Clone(runtime.Config)
	if runtime.Driver == "podman" {
		delete(config, "command")
		config["entrypoint"] = []any{"/bin/sh", "-c"}
		config["args"] = []any{cloneScript}
	} else {
		config["command"] = "/bin/sh"
		config["args"] = []any{"-c", cloneScript}
	}
	env := maps.Clone(runtime.Env)
	if env == nil {
		env = make(map[string]string)
	}
	env["HOME"] = "${NOMAD_TASK_DIR}"
	env["GIT_TERMINAL_PROMPT"] = "0"
	env["GIT_SSH_COMMAND"] = "ssh -o StrictHostKeyChecking=yes -o BatchMode=yes"
	templates := []*api.Template{{
		EmbeddedTmpl: new(`{{ with nomadVar "` + gitPath + `" }}{{ .git_url }}` + "\n" + `{{ .git_ref }}` + "\n" + `{{ .git_path }}` + "\n" + `{{ end }}`),
		DestPath:     new("local/git"), ChangeMode: new("noop"),
	}}
	return &api.Task{
		Name: gitTaskName, Driver: runtime.Driver, Config: config, Env: env,
		Lifecycle: &api.TaskLifecycle{Hook: api.TaskLifecycleHookPrestart},
		Resources: &api.Resources{CPU: runtime.Resources.CPU, MemoryMB: runtime.Resources.MemoryMB},
		LogConfig: &api.LogConfig{MaxFiles: new(3), MaxFileSizeMB: new(10)},
		Templates: templates,
	}
}

func jobID(runID string, turn int64) string {
	return fmt.Sprintf("%s%s-t%d", jobPrefix, runID, turn)
}

func variablePath(runID string, turn int64) string {
	return jobVariablePrefix + jobID(runID, turn)
}

func notFound(err error) bool {
	var response api.UnexpectedResponseError
	return errors.As(err, &response) && response.StatusCode() == 404
}

func (adapter *Adapter) deregisterTimedOut(ctx context.Context, jobID string) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupWait)
	defer cancel()
	_, _, err := adapter.client.Jobs().Deregister(jobID, true, (&api.WriteOptions{}).WithContext(cleanupCtx))
	if err != nil {
		return errors.Join(ctx.Err(), fmt.Errorf("deregister timed-out job: %w", err))
	}
	return ctx.Err()
}

// current prefers a non-terminal allocation, else the newest.
func current(allocs []*api.AllocationListStub) *api.AllocationListStub {
	var newest *api.AllocationListStub
	for _, alloc := range allocs {
		if !terminal(alloc.ClientStatus) {
			return alloc
		}
		if newest == nil || alloc.CreateIndex > newest.CreateIndex {
			newest = alloc
		}
	}
	return newest
}

func terminal(status string) bool {
	switch status {
	case api.AllocClientStatusComplete, api.AllocClientStatusFailed, api.AllocClientStatusLost:
		return true
	}
	return false
}

func started(alloc *api.AllocationListStub) bool {
	state, ok := alloc.TaskStates[taskName]
	return ok && state != nil && !state.StartedAt.IsZero()
}

// Follow streams stdout lines from a byte offset until the task ends.
func (adapter *Adapter) Follow(ctx context.Context, handle receiver.Handle, offset int64, lines chan<- receiver.Line) error {
	alloc, _, err := adapter.client.Allocations().Info(handle.Alloc, (&api.QueryOptions{}).WithContext(ctx))
	if err != nil {
		return err
	}
	reader := lineReader{lines: lines, position: offset, cri: handle.Format == formatCRI}
	streamCtx, stopStream := context.WithCancel(ctx)
	defer stopStream()
	frames, errs := adapter.logs(streamCtx, alloc, taskName, true, offset)
	// A followed stream can stay open with no frames after the task ends, and can
	// lag a task that ended before it opened; completion is polled instead.
	poll := time.NewTicker(followPoll)
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errs:
			return err
		case <-poll.C:
			if adapter.finished(ctx, handle) {
				stopStream()
				return adapter.drain(ctx, alloc, &reader)
			}
		case frame, ok := <-frames:
			if !ok {
				return reader.flush(ctx)
			}
			if err := reader.take(ctx, frame.Data); err != nil {
				return err
			}
		}
	}
}

// drain re-reads the finished task's log from the last line boundary to its end;
// a non-follow read is finite, so nothing written before exit can be missed.
func (adapter *Adapter) drain(ctx context.Context, alloc *api.Allocation, reader *lineReader) error {
	reader.pending, reader.partial, reader.consumed = nil, nil, 0
	frames, errs := adapter.logs(ctx, alloc, taskName, false, reader.position)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errs:
			if err != nil {
				return err
			}
		case frame, ok := <-frames:
			if !ok {
				return reader.flush(ctx)
			}
			if err := reader.take(ctx, frame.Data); err != nil {
				return err
			}
		}
	}
}

func (adapter *Adapter) logs(ctx context.Context, alloc *api.Allocation, task string, follow bool, offset int64) (<-chan *api.StreamFrame, <-chan error) {
	cancel := make(chan struct{})
	go func() { <-ctx.Done(); close(cancel) }()
	return adapter.client.AllocFS().Logs(alloc, follow, task, "stdout", api.OriginStart, offset, cancel, (&api.QueryOptions{}).WithContext(ctx))
}

// lineReader splits log bytes into lines; position is the offset after the last emitted line.
type lineReader struct {
	lines    chan<- receiver.Line
	position int64
	pending  []byte
	cri      bool
	// partial holds CRI "P" parts not yet closed by an "F" part; position stays at their start.
	partial  []byte
	consumed int64
}

func (reader *lineReader) take(ctx context.Context, data []byte) error {
	reader.pending = append(reader.pending, data...)
	for {
		end := bytes.IndexByte(reader.pending, '\n')
		if end < 0 && len(reader.pending) < maxPending {
			return nil
		}
		size, skip := end, 1
		if end < 0 {
			size, skip = len(reader.pending), 0 // over-long line: split rather than grow without bound
		}
		if err := reader.raw(ctx, reader.pending[:size], int64(size+skip)); err != nil {
			return err
		}
		reader.pending = reader.pending[size+skip:]
	}
}

// raw handles one log line occupying advance bytes, decoding CRI framing when enabled.
func (reader *lineReader) raw(ctx context.Context, data []byte, advance int64) error {
	if !reader.cri {
		return reader.emit(ctx, data, advance)
	}
	fields := bytes.SplitN(data, []byte(" "), 4)
	if len(fields) != 4 || (string(fields[2]) != "P" && string(fields[2]) != "F") {
		if reader.consumed > 0 {
			if err := reader.emitPartial(ctx); err != nil {
				return err
			}
		}
		return reader.emit(ctx, data, advance)
	}
	reader.partial = append(reader.partial, fields[3]...)
	reader.consumed += advance
	if len(reader.partial) >= maxPending {
		return reader.emitPartial(ctx)
	}
	if string(fields[2]) == "P" {
		return nil
	}
	return reader.emitPartial(ctx)
}

func (reader *lineReader) flush(ctx context.Context) error {
	if reader.consumed > 0 {
		if err := reader.emitPartial(ctx); err != nil {
			return err
		}
	}
	if len(reader.pending) == 0 {
		return nil
	}
	err := reader.emit(ctx, reader.pending, int64(len(reader.pending)))
	reader.pending = nil
	return err
}

func (reader *lineReader) emitPartial(ctx context.Context) error {
	content, total := reader.partial, reader.consumed
	reader.partial, reader.consumed = nil, 0
	return reader.emit(ctx, content, total)
}

func (reader *lineReader) emit(ctx context.Context, data []byte, advance int64) error {
	line := receiver.Line{Offset: reader.position, Next: reader.position + advance, Data: append([]byte(nil), data...)}
	select {
	case reader.lines <- line:
		reader.position = line.Next
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (adapter *Adapter) finished(ctx context.Context, handle receiver.Handle) bool {
	alloc, _, err := adapter.client.Allocations().Info(handle.Alloc, (&api.QueryOptions{}).WithContext(ctx))
	return err == nil && alloc.ClientTerminalStatus()
}

// Stop sends SIGTERM and stops the job if the task outlives the grace period;
// stopping a batch allocation instead would migrate it to a replacement.
// Requires the alloc-lifecycle and submit-job capabilities.
func (adapter *Adapter) Stop(ctx context.Context, handle receiver.Handle) error {
	alloc, _, err := adapter.client.Allocations().Info(handle.Alloc, (&api.QueryOptions{}).WithContext(ctx))
	if err != nil {
		return err
	}
	if alloc.ClientTerminalStatus() {
		return nil
	}
	if err := adapter.client.Allocations().Signal(alloc, (&api.QueryOptions{}).WithContext(ctx), taskName, "SIGTERM"); err == nil {
		for deadline := time.Now().Add(stopGrace); time.Now().Before(deadline); time.Sleep(time.Second) {
			if adapter.finished(ctx, handle) {
				return nil
			}
		}
	}
	_, _, err = adapter.client.Jobs().Deregister(handle.Job, false, (&api.WriteOptions{}).WithContext(ctx))
	return err
}

// Release deletes the per-turn variables holding the credential, the prompt and the git context.
func (adapter *Adapter) Release(ctx context.Context, runID string, turn int64) error {
	path := variablePath(runID, turn)
	return errors.Join(adapter.deleteVariable(ctx, path), adapter.deleteVariable(ctx, path+promptSuffix), adapter.deleteVariable(ctx, path+gitSuffix))
}

// Sweep deletes per-run variables whose job has ended, been stopped, or never registered.
func (adapter *Adapter) Sweep(ctx context.Context) error {
	prefix := jobVariablePrefix + jobPrefix
	variables, _, err := adapter.client.Variables().PrefixList(prefix, (&api.QueryOptions{}).WithContext(ctx))
	if err != nil {
		return redacted("list", prefix, err)
	}
	var errs []error
	for _, variable := range variables {
		match := runVariable.FindStringSubmatch(variable.Path)
		if match == nil {
			continue
		}
		job, _, err := adapter.client.Jobs().Info(match[1], (&api.QueryOptions{}).WithContext(ctx))
		switch {
		case notFound(err):
			if time.Since(time.Unix(0, variable.ModifyTime)) < sweepGrace {
				continue
			}
		case err != nil:
			errs = append(errs, redacted("sweep", variable.Path, err))
			continue
		case *job.Status != "dead" && !*job.Stop:
			continue
		}
		errs = append(errs, adapter.deleteVariable(ctx, variable.Path))
	}
	return errors.Join(errs...)
}

func (adapter *Adapter) deleteVariable(ctx context.Context, path string) error {
	if _, err := adapter.client.Variables().Delete(path, (&api.WriteOptions{}).WithContext(ctx)); err != nil && !errors.Is(err, api.ErrVariablePathNotFound) && !notFound(err) {
		return redacted("delete", path, err)
	}
	return nil
}

// redacted keeps response bodies, which may echo variable items, out of errors.
func redacted(op, path string, err error) error {
	var response api.UnexpectedResponseError
	if errors.As(err, &response) {
		return fmt.Errorf("%s variable %s: status %d", op, path, response.StatusCode())
	}
	return fmt.Errorf("%s variable %s failed", op, path)
}
