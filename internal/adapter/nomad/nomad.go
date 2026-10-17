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
	jobPrefix   = "agent-"
	taskName    = "agent"
	formatCRI   = "cri"
	maxPending  = 1 << 20
	stopGrace   = 15 * time.Second
	followPoll  = time.Second
	cleanupWait = 10 * time.Second
)

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

var envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

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
		if err := adapter.deleteVariable(cleanupCtx, path); err != nil {
			dispatchErr = errors.Join(dispatchErr, err)
		}
	}()

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
	return adapter.wait(ctx, jobID, job.TaskGroups[0].Tasks[0].Driver)
}

// Lookup finds a run's existing job and waits for it as Dispatch does.
func (adapter *Adapter) Lookup(ctx context.Context, runID string, turn int64) (receiver.Handle, bool, error) {
	job, _, err := adapter.client.Jobs().Info(jobID(runID, turn), (&api.QueryOptions{}).WithContext(ctx))
	if notFound(err) {
		return receiver.Handle{}, false, nil
	}
	if err != nil {
		return receiver.Handle{}, false, err
	}
	if len(job.TaskGroups) != 1 || len(job.TaskGroups[0].Tasks) != 1 {
		return receiver.Handle{}, false, errors.New("existing job is not an agent job")
	}
	handle, err := adapter.wait(ctx, *job.ID, job.TaskGroups[0].Tasks[0].Driver)
	if err != nil {
		return receiver.Handle{}, false, err
	}
	return handle, true, nil
}

// wait returns once the job's task has started or ended; a wait cut short deregisters the job.
func (adapter *Adapter) wait(ctx context.Context, jobID, driver string) (receiver.Handle, error) {
	format := ""
	if driver == "podman" {
		format = formatCRI
	}
	for {
		allocs, _, err := adapter.client.Jobs().Allocations(jobID, false, (&api.QueryOptions{}).WithContext(ctx))
		if err != nil {
			if ctx.Err() != nil {
				return receiver.Handle{}, adapter.deregisterTimedOut(ctx, jobID)
			}
			return receiver.Handle{}, err
		}
		// The logs API answers 404 until the task has started.
		if alloc := current(allocs); alloc != nil && (terminal(alloc.ClientStatus) || started(alloc)) {
			return receiver.Handle{Job: jobID, Alloc: alloc.ID, Format: format}, nil
		}
		select {
		case <-ctx.Done():
			return receiver.Handle{}, adapter.deregisterTimedOut(ctx, jobID)
		case <-time.After(2 * time.Second):
		}
	}
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
	templates := []*api.Template{
		{EmbeddedTmpl: new(`{{ with nomadVar "` + path + `" }}{{ .prompt }}{{ end }}`), DestPath: new("local/prompt.txt"), ChangeMode: new("noop")},
	}
	if run.Credential != nil {
		templates = append(templates, &api.Template{
			EmbeddedTmpl: new(`{{ with nomadVar "` + path + `" }}` + adapter.credentialEnv + `={{ .credential }}{{ end }}`),
			DestPath:     new("${NOMAD_SECRETS_DIR}/wherry.env"), Envvars: new(true), ChangeMode: new("noop"),
		})
	}
	return &api.Job{
		ID: new(jobID), Name: new(jobID), Namespace: new(adapter.namespace), Type: new(api.JobTypeBatch),
		NodePool: new(run.Offer["nomad.node_pool"]), Datacenters: []string{run.Offer["zone"]},
		Meta: map[string]string{"run_id": run.ID},
		TaskGroups: []*api.TaskGroup{{
			Name:             new(taskName),
			ReschedulePolicy: &api.ReschedulePolicy{Attempts: new(0), Unlimited: new(false)},
			RestartPolicy:    &api.RestartPolicy{Attempts: new(0), Mode: new(api.RestartPolicyModeFail)},
			Tasks: []*api.Task{{
				Name: taskName, Driver: runtime.Driver, Config: maps.Clone(runtime.Config), Env: env,
				Resources: &api.Resources{CPU: runtime.Resources.CPU, MemoryMB: runtime.Resources.MemoryMB},
				LogConfig: &api.LogConfig{MaxFiles: new(3), MaxFileSizeMB: new(10)},
				Templates: templates,
			}},
		}},
	}, nil
}

func jobID(runID string, turn int64) string {
	return fmt.Sprintf("%s%s-t%d", jobPrefix, runID, turn)
}

func variablePath(runID string, turn int64) string {
	return "nomad/jobs/" + jobID(runID, turn)
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
	frames, errs := adapter.logs(streamCtx, alloc, true, offset)
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
	frames, errs := adapter.logs(ctx, alloc, false, reader.position)
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

func (adapter *Adapter) logs(ctx context.Context, alloc *api.Allocation, follow bool, offset int64) (<-chan *api.StreamFrame, <-chan error) {
	cancel := make(chan struct{})
	go func() { <-ctx.Done(); close(cancel) }()
	return adapter.client.AllocFS().Logs(alloc, follow, taskName, "stdout", api.OriginStart, offset, cancel, (&api.QueryOptions{}).WithContext(ctx))
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

// Release deletes the per-run credential and prompt variable.
func (adapter *Adapter) Release(ctx context.Context, runID string, turn int64) error {
	return adapter.deleteVariable(ctx, variablePath(runID, turn))
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
