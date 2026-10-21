package client

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"log"
	"maps"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Errors the client returns for a run or placement that cannot proceed.
var (
	ErrNoMatchingReceiver        = errors.New("no matching live receiver")
	ErrRunNotFound               = errors.New("run not found")
	ErrNotAwaitingContinuation   = errors.New("task is not awaiting continuation")
	ErrRunCanceled               = errors.New("task is canceled")
	ErrRunExpired                = errors.New("run expired")
	ErrQueueDeadlinePastLifetime = errors.New("queue deadline exceeds run lifetime")
	ErrTurnSubmitted             = errors.New("continuation turn already submitted")
	// ErrInvalidInput wraps a submission the wire format rejects; its message names the rule, never the value.
	ErrInvalidInput = errors.New("invalid input")
)

// errUndecodable wraps a local state file that does not decode.
var errUndecodable = errors.New("decode")

// liveness is how old an advertisement may be for its receiver to count as live.
const liveness = 120 * time.Second

// SubmitInput is a root task's routing, prompt, inputs and requested outputs.
type SubmitInput struct {
	// Agent names what runs; empty takes the pool's default.
	Agent string
	// Model names the model the agent runs; empty takes the agent's default model.
	Model    string
	Selector []wire.Requirement
	Prompt   string
	// Parts follow the prompt's text part.
	Parts []wire.Part
	// Credentials name credentials this host configures, delivered to the agent as files.
	Credentials []wire.TaskCredential
	// Outputs are the files the agent is asked to leave.
	Outputs []wire.ExpectedOutput
	// Git is the repository the agent clones.
	Git *wire.GitInput
	// GitChanges, patch or bundle, asks for the clone's changes back as that output.
	GitChanges string
	// S3Inputs are files uploaded to the pool's bucket before the task is published, which the adapter fetches.
	S3Inputs      []S3File
	Deadline      time.Duration
	QueueDeadline time.Duration
}

// DefaultDeadline and DefaultQueueDeadline are a turn's deadlines where the caller gives none.
const (
	DefaultDeadline      = 2 * time.Hour
	DefaultQueueDeadline = 10 * time.Minute
)

// SubmitResult describes a produced root task.
type SubmitResult struct {
	RunID string
	Pool  string
}

// ContinueInput contains the new prompt and deadlines for a continuation.
type ContinueInput struct {
	Prompt        string
	Deadline      time.Duration
	QueueDeadline time.Duration
}

// ContinueResult describes a produced continuation.
type ContinueResult struct {
	RunID   string
	Turn    int64
	Skipped int
}

// CancelResult describes a produced cancel.
type CancelResult struct {
	RunID   string
	Skipped int
}

// Receiver describes a receiver of a pool the tenant is entitled to, its adapters as wherry.admin records them and its
// latest advertisement.
type Receiver struct {
	ReceiverID   string    `json:"receiver_id"`
	Pool         string    `json:"pool"`
	Adapters     []Adapter `json:"adapters"`
	Capacity     int64     `json:"capacity"`
	InFlight     int64     `json:"in_flight"`
	HeartbeatAge string    `json:"heartbeat_age"`
	Status       string    `json:"status"`
	Ended        bool      `json:"ended"`
}

// Adapter is one adapter of a receiver: the agent and labels from its config record, the agent's models and default
// model from its pool's terms, and whether the receiver's live advertisement offers it.
type Adapter struct {
	Name         string            `json:"name"`
	Agent        string            `json:"agent"`
	Models       []string          `json:"models,omitzero"`
	DefaultModel string            `json:"default_model,omitzero"`
	Labels       map[string]string `json:"labels"`
	Ready        bool              `json:"ready"`
}

// Host implements the operations of a tenant's host principal.
type Host struct {
	kafka       *kgo.Client
	key         ed25519.PrivateKey
	tenant      string
	host        string
	stateDir    string
	credentials map[string]string
	// storage is the host's S3 credential, nil when its configuration has none.
	storage *StorageConfig
	logger  *log.Logger
}

// NewHost constructs a host over a plain Kafka connection authenticated as principal.
func NewHost(kafka *kgo.Client, key ed25519.PrivateKey, principal, stateDir string, credentials map[string]string, logger *log.Logger) (*Host, error) {
	// TenantPrincipal("", "") is the prefix and the separator between tenant and host.
	names, prefixed := strings.CutPrefix(principal, strings.TrimSuffix(admin.TenantPrincipal("", ""), "."))
	tenant, host, split := strings.Cut(names, ".")
	if !prefixed || !split || !wire.ValidName(tenant) || !wire.ValidName(host) {
		return nil, errors.New("principal is not a host principal wherry.tnt.<tenant>.<host>")
	}
	return &Host{kafka: kafka, key: key, tenant: tenant, host: host, stateDir: stateDir, credentials: credentials, logger: logger}, nil
}

// Close closes the Kafka connection.
func (h *Host) Close() {
	h.kafka.Close()
}

// Register registers the host key on the host's key topic unless that key is already there, and returns its key ID.
func (h *Host) Register(ctx context.Context) (string, error) {
	owner := signed.HostOwner(h.tenant, h.host)
	topic := admin.HostTopic(h.tenant, h.host)
	keys, err := admin.ReadKeys(ctx, h.kafka, map[string]string{owner: topic})
	if err != nil {
		return "", err
	}
	public := h.key.Public().(ed25519.PublicKey)
	switch state := keys[owner]; {
	case state.Revoked:
		return "", errors.New("host key topic is closed by revocation")
	case state.Current == nil:
		record, err := signed.RegisterKey(owner, h.key)
		if err != nil {
			return "", err
		}
		if err := h.produce(ctx, &kgo.Record{Topic: topic, Key: []byte(owner), Value: record}); err != nil {
			return "", err
		}
		// A concurrent first registration may have landed first; only the first counts.
		keys, err := admin.ReadKeys(ctx, h.kafka, map[string]string{owner: topic})
		if err != nil {
			return "", err
		}
		if current := keys[owner].Current; current == nil || !current.Equal(public) {
			return "", errors.New("another host key won the registration of this host's key topic")
		}
	case !state.Current.Equal(public):
		return "", errors.New("host key topic holds another key")
	}
	return signed.KeyID(public), nil
}

// view is wherry.admin and the keys of the receivers of the tenant's pools.
type view struct {
	state admin.State
	keys  signed.Keys
}

func (h *Host) load(ctx context.Context) (view, error) {
	state, err := admin.Load(ctx, h.kafka)
	if err != nil {
		return view{}, err
	}
	topics := map[string]string{}
	for pool := range state.Entitlements[h.tenant] {
		maps.Copy(topics, admin.PoolKeyTopics(state, pool))
	}
	keys, err := admin.ReadKeys(ctx, h.kafka, topics)
	if err != nil {
		return view{}, err
	}
	return view{state, keys}, nil
}

// Submit places a task through the live receiver with the most spare capacity that can take it, and publishes it to
// that receiver's pool.
func (h *Host) Submit(ctx context.Context, input SubmitInput) (SubmitResult, error) {
	// Checked before placement: an invalid key matches no offer and would read as no matching receiver.
	if err := wire.ValidateRequirements(input.Selector); err != nil {
		return SubmitResult{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	outputs := input.Outputs
	if input.Git != nil {
		git := *input.Git
		git.Remote = sshRemote(git.Remote)
		// Validate never quotes a value in its errors, so its text is safe to return.
		if err := git.Validate(input.Credentials); err != nil {
			return SubmitResult{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
		}
		input.Git = &git
		changes := map[string]string{"": "", "patch": wire.GitPatch, "bundle": wire.GitBundle}
		name, ok := changes[input.GitChanges]
		if !ok {
			return SubmitResult{}, fmt.Errorf("%w: git changes must be patch or bundle", ErrInvalidInput)
		}
		if name != "" && !slices.ContainsFunc(outputs, func(output wire.ExpectedOutput) bool { return output.Name == name }) {
			outputs = append(slices.Clone(outputs), wire.ExpectedOutput{Name: name})
		}
	} else if input.GitChanges != "" {
		return SubmitResult{}, fmt.Errorf("%w: git changes need a git input", ErrInvalidInput)
	}
	for _, credential := range input.Credentials {
		if _, configured := h.credentials[credential.Name]; !configured {
			return SubmitResult{}, fmt.Errorf("%w: credential %s is not in this host's configuration", ErrInvalidInput, credential.Name)
		}
	}
	var s3Inputs, s3Outputs []string
	for _, file := range input.S3Inputs {
		s3Inputs = append(s3Inputs, file.Name)
	}
	for _, output := range outputs {
		if output.Storage == wire.StorageS3 {
			s3Outputs = append(s3Outputs, output.Name)
		}
	}
	stored := len(s3Inputs)+len(s3Outputs) > 0
	v, err := h.load(ctx)
	if err != nil {
		return SubmitResult{}, err
	}
	now := time.Now()
	ads, err := admin.ReadAdvertisements(ctx, h.kafka, v.state, v.keys, now, liveness)
	if err != nil {
		return SubmitResult{}, err
	}
	pool, placement, err := resolvePlacement(v.state, h.tenant, input.Agent, input.Model, input.Selector, ads)
	if err != nil {
		return SubmitResult{}, err
	}
	if err := wire.ValidateInputs(input.Parts, v.state.Pools[pool].InlineLimit); err != nil {
		return SubmitResult{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	if input.QueueDeadline > v.state.Pools[pool].RunLifetime() {
		return SubmitResult{}, ErrQueueDeadlinePastLifetime
	}
	if input.Git != nil {
		if host := input.Git.VCSHost(); !slices.Contains(v.state.Pools[pool].VCS, host) {
			return SubmitResult{}, fmt.Errorf("%w: git remote host %s is not in pool %s's vcs hosts", ErrInvalidInput, host, pool)
		}
	}
	runID := wire.RandomID("run-")
	submission, err := h.submission(runID, 1, pool, placement, append([]wire.Part{{Text: input.Prompt}}, input.Parts...), input.Deadline, input.QueueDeadline, now)
	if err != nil {
		return SubmitResult{}, err
	}
	submission.Wherry.Credentials, submission.Wherry.Outputs, submission.Wherry.Git = input.Credentials, outputs, input.Git
	if stored {
		storage := v.state.Pools[pool].Storage
		store, err := h.bucket(pool, storage)
		if err != nil {
			return SubmitResult{}, err
		}
		deadline := time.UnixMilli(submission.Wherry.DeadlineMS)
		if err := h.checkSealSize(ctx, store, storage.Bucket, h.tenant, runID, input.Credentials, s3Inputs, s3Outputs, deadline); err != nil {
			return SubmitResult{}, err
		}
		if submission.Wherry.S3Inputs, err = uploadInputs(ctx, store, storage.Bucket, h.tenant, runID, input.S3Inputs); err == nil {
			err = h.publish(ctx, submission, now, now)
		}
		if err != nil {
			if cleanupErr := h.removePrefix(ctx, store, storage.Bucket, runID); cleanupErr != nil {
				h.logger.Printf("run %s: delete the S3 inputs of a task not published: %v", runID, cleanupErr)
			}
			return SubmitResult{}, err
		}
		return SubmitResult{RunID: runID, Pool: pool}, nil
	}
	if err := h.publish(ctx, submission, now, now); err != nil {
		return SubmitResult{}, err
	}
	return SubmitResult{RunID: runID, Pool: pool}, nil
}

// scpRemote is git's scp-like remote syntax, [user@]host:path, which has no slash before its colon.
var scpRemote = regexp.MustCompile(`^(?:([A-Za-z0-9._-]+)@)?([A-Za-z0-9.-]+):(.+)$`)

// sshRemote writes an scp-like remote as the ssh URL of the same path from the root, which VCS hosts such as GitHub serve
// as the same repository; any other remote is returned as it is.
func sshRemote(remote string) string {
	match := scpRemote.FindStringSubmatch(remote)
	if match == nil || strings.Contains(remote, "://") {
		return remote
	}
	user := ""
	if match[1] != "" {
		user = match[1] + "@"
	}
	return "ssh://" + user + match[2] + "/" + strings.TrimPrefix(match[3], "/")
}

// Continue publishes the next turn of an interrupted run to the pool and placement its echo names.
func (h *Host) Continue(ctx context.Context, runID string, input ContinueInput) (ContinueResult, error) {
	r, err := h.history(ctx, runID)
	if err != nil {
		return ContinueResult{}, err
	}
	if r.pool == "" {
		return ContinueResult{}, fmt.Errorf("continue %s: %w", runID, ErrRunNotFound)
	}
	var filter wire.AttemptFilter
	var status *wire.RunStatus
	for _, candidate := range r.statuses {
		if candidate.Wherry.Kind != "rejected" && admit(&filter, candidate) {
			status = candidate
		}
	}
	if status == nil || status.Wherry.Kind != "interrupted" || !wire.Interrupted(status.StatusUpdate.Status.State) {
		if status != nil && status.StatusUpdate.Status.State == wire.TaskStateCanceled {
			return ContinueResult{}, ErrRunCanceled
		}
		return ContinueResult{}, ErrNotAwaitingContinuation
	}
	next := status.Wherry.Turn + 1
	if _, submitted := r.turns[next]; submitted {
		return ContinueResult{}, ErrTurnSubmitted
	}
	echo := r.turns[status.Wherry.Turn].echo
	if echo == nil {
		return ContinueResult{}, ErrNotAwaitingContinuation
	}
	firstSubmitted := r.turns[1].echoedAt
	if r.local != nil && r.local.FirstSubmittedMS > 0 {
		firstSubmitted = time.UnixMilli(r.local.FirstSubmittedMS)
	}
	now := time.Now()
	expires := firstSubmitted.Add(r.v.state.Pools[echo.Pool].RunLifetime())
	if now.After(expires) {
		return ContinueResult{}, ErrRunExpired
	}
	if now.Add(input.QueueDeadline).After(expires) {
		return ContinueResult{}, ErrQueueDeadlinePastLifetime
	}
	// Another host of the tenant may have submitted the run, and then only admission checks its VCS host and storage.
	if r.local != nil && r.local.VCSHost != "" && !slices.Contains(r.v.state.Pools[echo.Pool].VCS, r.local.VCSHost) {
		return ContinueResult{}, fmt.Errorf("%w: git remote host %s is not in pool %s's vcs hosts", ErrInvalidInput, r.local.VCSHost, echo.Pool)
	}
	if r.local != nil && len(r.local.S3Inputs)+len(r.local.S3Outputs) > 0 && r.v.state.Pools[echo.Pool].Storage == nil {
		return ContinueResult{}, fmt.Errorf("%w: pool %s has no storage for S3 inputs and outputs", ErrInvalidInput, echo.Pool)
	}
	submission, err := h.submission(runID, next, echo.Pool, echo.Placement, []wire.Part{{Text: input.Prompt}}, input.Deadline, input.QueueDeadline, now)
	if err != nil {
		return ContinueResult{}, err
	}
	if err := h.publish(ctx, submission, now, firstSubmitted); err != nil {
		return ContinueResult{}, err
	}
	return ContinueResult{RunID: runID, Turn: next, Skipped: r.skipped}, nil
}

// Cancel publishes a cancel of a run's latest turn to the events topic of its pool.
func (h *Host) Cancel(ctx context.Context, runID string) (CancelResult, error) {
	r, err := h.history(ctx, runID)
	if err != nil {
		return CancelResult{}, err
	}
	if r.pool == "" {
		return CancelResult{}, fmt.Errorf("cancel %s: %w", runID, ErrRunNotFound)
	}
	encoded, err := wire.SignRecord(&wire.Cancel{Tenant: h.tenant, Host: h.host, Pool: r.pool, Run: runID, Turn: r.latest}, h.key)
	if err != nil {
		return CancelResult{}, err
	}
	if err := h.produce(ctx, &kgo.Record{Topic: admin.PoolEventsTopic(r.pool), Key: []byte(runID), Value: encoded}); err != nil {
		return CancelResult{}, err
	}
	return CancelResult{RunID: runID, Skipped: r.skipped}, nil
}

func (h *Host) submission(runID string, turn int64, pool string, placement wire.Placement, parts []wire.Part, deadline, queueDeadline time.Duration, now time.Time) (*wire.Submission, error) {
	return &wire.Submission{
		Message: wire.Message{MessageID: wire.RandomID("msg-"), ContextID: runID, TaskID: runID, Role: wire.RoleUser, Parts: parts, Extensions: []string{wire.ExtensionURI}},
		Wherry: wire.SubmissionBinding{
			Tenant: h.tenant, Host: h.host, Pool: pool, Run: runID, Turn: turn, Placement: placement,
			DeadlineMS: now.Add(deadline).UnixMilli(), QueueDeadlineMS: now.Add(queueDeadline).UnixMilli(),
		},
	}, nil
}

// publish records the run locally, then writes the submission to its pool's task topic. The record comes first so
// this host answers the seal request a receiver may send as soon as it claims the task; a failed write restores the
// record it replaced.
func (h *Host) publish(ctx context.Context, submission *wire.Submission, now, firstSubmitted time.Time) error {
	encoded, err := wire.SignRecord(submission, h.key)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	binding := submission.Wherry
	previous, err := h.localRun(binding.Run)
	if err != nil {
		return err
	}
	record := runRecord{
		Run: binding.Run, Pool: binding.Pool, Turn: binding.Turn, Placement: binding.Placement, TaskHash: wire.TaskHash(encoded), SubmittedMS: now.UnixMilli(),
		FirstSubmittedMS: firstSubmitted.UnixMilli(), DeadlineMS: binding.DeadlineMS,
	}
	for _, credential := range binding.Credentials {
		record.TaskCredentials = append(record.TaskCredentials, credential.Name)
	}
	if binding.Git != nil {
		record.VCSHost = binding.Git.VCSHost()
	}
	for _, input := range binding.S3Inputs {
		record.S3Inputs = append(record.S3Inputs, input.Name)
	}
	for _, output := range binding.Outputs {
		if output.Storage == wire.StorageS3 {
			record.S3Outputs = append(record.S3Outputs, output.Name)
		}
	}
	if binding.Turn > 1 && previous != nil {
		record.TaskCredentials, record.VCSHost, record.S3Inputs, record.S3Outputs, record.RedactionKey =
			previous.TaskCredentials, previous.VCSHost, previous.S3Inputs, previous.S3Outputs, previous.RedactionKey
	}
	// Minted here, not at a seal answer, so every answerer of the run reads one key and none writes the record.
	if len(record.RedactionKey) == 0 {
		record.RedactionKey = make([]byte, 32)
		if _, err := rand.Read(record.RedactionKey); err != nil {
			return err
		}
	}
	if err := h.writeRun(record); err != nil {
		return err
	}
	if err := h.produce(ctx, &kgo.Record{Topic: admin.PoolTasksTopic(binding.Pool), Key: []byte(binding.Run), Value: encoded}); err != nil {
		if previous != nil {
			h.writeRun(*previous)
		} else if path, ok := h.runPath(binding.Run); ok {
			os.Remove(path)
		}
		if errors.Is(err, kerr.MessageTooLarge) {
			return fmt.Errorf("%w: the submission is a %d-byte record, over the broker's record limit; shorten the prompt or the input files", ErrInvalidInput, len(encoded))
		}
		return err
	}
	return nil
}

// Receivers returns the receivers of the pools the tenant is entitled to, with their latest advertisement. An
// advertisement dated beyond the liveness window either side of now is not live.
func (h *Host) Receivers(ctx context.Context) ([]Receiver, error) {
	v, err := h.load(ctx)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	ads, err := admin.ReadAdvertisements(ctx, h.kafka, v.state, v.keys, now, math.MaxInt64)
	if err != nil {
		return nil, err
	}
	result := []Receiver{}
	for id, config := range v.state.Receivers {
		if !v.state.Entitlements[h.tenant][config.Pool] {
			continue
		}
		receiver := Receiver{
			ReceiverID: id, Pool: config.Pool, Adapters: []Adapter{}, HeartbeatAge: "-", Status: "silent",
			Ended: v.state.Ended[admin.ReceiverPrincipal(id)],
		}
		ad, advertised := ads[id]
		if receiver.Ended {
			receiver.Status = "ended"
		} else if advertised {
			age := now.Sub(time.UnixMilli(ad.AtMS))
			receiver.Capacity, receiver.InFlight = ad.Capacity, ad.InFlight
			receiver.HeartbeatAge = max(age, 0).Round(time.Second).String()
			receiver.Status = "stale"
			if age <= liveness && age >= -liveness {
				receiver.Status = "live"
			}
		}
		for _, adapter := range config.Adapters {
			if receiver.Ended {
				break
			}
			labels := adapter.Labels
			if labels == nil {
				labels = map[string]string{}
			}
			ready := receiver.Status == "live" && slices.ContainsFunc(ad.Offers, func(offer wire.Offer) bool { return offer.Adapter == adapter.Name })
			listed := Adapter{Name: adapter.Name, Agent: adapter.Agent, Labels: labels, Ready: ready}
			agents := v.state.Pools[config.Pool].Agents
			if index := slices.IndexFunc(agents, func(agent admin.AgentTerms) bool { return agent.Name == adapter.Agent }); index >= 0 {
				listed.Models, listed.DefaultModel = agents[index].Models, agents[index].DefaultModel
			}
			receiver.Adapters = append(receiver.Adapters, listed)
		}
		result = append(result, receiver)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Pool != result[j].Pool {
			return result[i].Pool < result[j].Pool
		}
		return result[i].ReceiverID < result[j].ReceiverID
	})
	return result, nil
}

// runRecord is the submitting host's own record of a run's latest turn, which lets it report the run before a
// receiver echoes it.
type runRecord struct {
	Run       string         `json:"run"`
	Pool      string         `json:"pool"`
	Turn      int64          `json:"turn"`
	Placement wire.Placement `json:"placement"`
	TaskHash  string         `json:"task_hash"`
	// SubmittedMS is when this host published the turn; records written before it existed hold zero.
	SubmittedMS int64 `json:"submitted_ms,omitzero"`
	// FirstSubmittedMS bounds continuations from the run's first local publication.
	FirstSubmittedMS int64 `json:"first_submitted_ms,omitzero"`
	// TaskCredentials names the task credentials the run's first turn declares, which this host may seal.
	TaskCredentials []string `json:"task_credentials,omitzero"`
	// VCSHost is the host of the git input the run's first turn declares, which every turn must be in the pool's VCS.
	VCSHost string `json:"vcs_host,omitzero"`
	// S3Inputs and S3Outputs name the S3 inputs and outputs the run's first turn declares, which this host may presign.
	S3Inputs  []string `json:"s3_inputs,omitzero"`
	S3Outputs []string `json:"s3_outputs,omitzero"`
	// DeadlineMS is the deadline of the turn, when a PUT URL for its S3 outputs expires.
	DeadlineMS int64 `json:"deadline_ms,omitzero"`
	// RedactionKey is the run's redaction key, minted when this host first submits a turn of the run.
	RedactionKey []byte `json:"redaction_key,omitzero"`
}

func (h *Host) runPath(runID string) (string, bool) {
	if !filepath.IsLocal(runID) || filepath.Base(runID) != runID {
		return "", false
	}
	return filepath.Join(h.stateDir, "runs", runID+".json"), true
}

// outputDir is the default directory of a turn's outputs.
func (h *Host) outputDir(runID string, turn int64) string {
	return filepath.Join(h.stateDir, "runs", runID, "outputs", strconv.FormatInt(turn, 10))
}

// localRun returns the local record of runID, or nil when this host has none.
func (h *Host) localRun(runID string) (*runRecord, error) {
	path, ok := h.runPath(runID)
	if !ok {
		return nil, nil
	}
	var record runRecord
	if err := readState(path, &record); errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	// A record written before submitted_ms existed takes its file's modification time.
	if record.SubmittedMS == 0 {
		if info, err := os.Stat(path); err == nil {
			record.SubmittedMS = info.ModTime().UnixMilli()
		}
	}
	return &record, nil
}

func (h *Host) writeRun(record runRecord) error {
	path, ok := h.runPath(record.Run)
	if !ok {
		return errors.New("run ID does not name a local run path")
	}
	return writeState(path, record)
}

func readState(path string, value any) error {
	encoded, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(encoded, value, signed.Strict); err != nil {
		return fmt.Errorf("%w %s: %w", errUndecodable, path, err)
	}
	return nil
}

func writeState(path string, value any) error {
	encoded, err := json.Marshal(value, json.Deterministic(true))
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	// A reader sees the old file or the new one, never a partial write.
	temp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	if _, err := temp.Write(encoded); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(temp.Name(), path)
}

func (h *Host) produce(ctx context.Context, records ...*kgo.Record) error {
	if err := h.kafka.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("produce: %w", err)
	}
	return nil
}
