package client

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/benemon/wherry/internal/catalog"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/telemetry"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

// Errors the client returns for a run or placement that cannot proceed.
var (
	ErrNoMatchingReceiver = errors.New("no matching live receiver")
	ErrRunNotFound        = errors.New("run not found")
	ErrStartByPastLimit   = errors.New("task_started_by is past the pool's start_by_limit")
	// ErrInvalidInput wraps a submission the wire format rejects; its message names the rule, never the value.
	ErrInvalidInput = errors.New("invalid input")
	// ErrNoSnapshot and ErrCustodianUnavailable refuse a submission whose pool has no current key to wrap its envelope
	// to.
	ErrNoSnapshot           = errors.New("pool has no custodian snapshot")
	ErrCustodianUnavailable = errors.New("pool's current snapshot is not authorised")
)

// errUndecodable wraps a local state file that does not decode.
var errUndecodable = errors.New("decode")

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
	// Files are input files read into raw parts after Parts, except that on a pool with storage one over the pool's
	// record limit, which can never travel inline, uploads from its Body as an S3 input.
	Files []S3File
	// Credentials name credentials this host configures, each delivered to the agent at its target.
	Credentials []wire.TaskCredential
	// Requested names the placed agent's credentials the task asks for: an optional one named here fails the submission
	// when this host cannot resolve it, where an unnamed one is left out.
	Requested []string
	// Outputs are the files the agent is asked to leave.
	Outputs []wire.ExpectedOutput
	// Git is the repository the agent clones; its credential and host key come from the host's git configuration.
	Git *wire.GitInput
	// GitChanges is how the clone's changes come back: as changes.bundle when empty or bundle, as changes.patch for
	// patch, and not at all for none.
	GitChanges string
	// Continue names a run of this host whose collected changes.bundle the agent continues from: the run takes that
	// run's git input and context, and returns its own changes.bundle through S3.
	Continue string
	// S3Inputs are files uploaded to the pool's bucket before the task is published, which the plugin fetches.
	S3Inputs []S3File
	// StartBy bounds the agent job's start and EndBy the run's terminal state; each travels in whole seconds.
	StartBy time.Time
	EndBy   time.Time
}

// DefaultStartBy and DefaultEndBy are a run's bounds, from now, where the caller gives none.
const (
	DefaultStartBy = 10 * time.Minute
	DefaultEndBy   = 2 * time.Hour
)

// ParseBound reads a task bound as an RFC 3339 time or a Go duration from now; either must fall after now.
func ParseBound(text string, now time.Time) (time.Time, error) {
	bound, err := time.Parse(time.RFC3339, text)
	if err != nil {
		duration, durationErr := time.ParseDuration(text)
		if durationErr != nil {
			return time.Time{}, errors.New("not an RFC 3339 time or a Go duration such as 90m")
		}
		bound = now.Add(duration)
	}
	if !bound.After(now) {
		return time.Time{}, errors.New("not after now")
	}
	return bound, nil
}

// SubmitResult describes a produced root task.
type SubmitResult struct {
	RunID string
	Pool  string
	// Unsupplied names the placed agent's optional credentials this host could not resolve, which the run goes without.
	Unsupplied []string
}

// CancelResult describes a produced cancel.
type CancelResult struct {
	RunID   string
	Skipped int
}

// Receiver describes a receiver of a pool the tenant is entitled to, its profiles as wherry.operator records them and its
// latest advertisement.
type Receiver struct {
	ReceiverID     string    `json:"receiver_id"`
	Pool           string    `json:"pool"`
	Profiles       []Profile `json:"profiles"`
	Capacity       int64     `json:"capacity"`
	InFlight       int64     `json:"in_flight"`
	LastAdvertised time.Time `json:"last_advertised,omitzero"`
	Status         string    `json:"status"`
	// Gated is why a live receiver's latest advertisement says it claimed no task, empty while it claimed or not live.
	Gated string `json:"gated,omitzero"`
}

// Profile is one profile of a receiver: its plugin type, agent and labels from its config record, the agent's models,
// default model and credentials from its pool's terms, and whether the receiver's live advertisement offers it.
type Profile struct {
	Name         string            `json:"name"`
	Plugin       string            `json:"plugin"`
	Agent        string            `json:"agent"`
	InlineLimit  int64             `json:"inline_limit,omitzero"`
	Models       []string          `json:"models,omitzero"`
	DefaultModel string            `json:"default_model,omitzero"`
	Credentials  []Credential      `json:"credentials,omitzero"`
	Labels       map[string]string `json:"labels"`
	Ready        bool              `json:"ready"`
	// Replicas and InFlight are the claim slots the ready profile's offer advertises and the runs they hold, both 0 when
	// it is not ready.
	Replicas int64 `json:"replicas"`
	InFlight int64 `json:"in_flight"`
}

// Credential is one of an agent's credentials: its target, the agent's environment variable Env or credentials-directory
// file File; its description from the pool's terms; whether a run goes without it when this host cannot resolve it;
// and what discovery says of its source on this host, which runs no command.
type Credential struct {
	Name        string `json:"name"`
	Env         string `json:"env,omitzero"`
	File        string `json:"file,omitzero"`
	Description string `json:"description,omitzero"`
	Optional    bool   `json:"optional,omitzero"`
	// Configured says this host has a source for the credential: one its configuration names, or the credential's own
	// target variable, which the host reads when it is set because the variable is in its catalog.
	Configured bool `json:"configured"`
	// Source is SourceFile, SourceEnv or SourceCommand for a source the configuration names, SourceVariable for the
	// catalog variable, or empty; Variable is the variable an env or variable source reads; Status is StatusReady,
	// StatusUnchecked, StatusUnset or StatusMissing.
	Source   string `json:"source,omitzero"`
	Variable string `json:"variable,omitzero"`
	Status   string `json:"status"`
}

// Host implements the operations of a tenant's host principal.
type Host struct {
	kafka *kgo.Client
	// reader reads every topic the host reads other than through its follower.
	reader      *operator.Reader
	key         ed25519.PrivateKey
	tenant      string
	host        string
	stateDir    string
	credentials map[string]credential.Source
	// catalog is the credential catalog this host reads variables through: a terms credential whose target is one of
	// its variables resolves from that variable when the configuration names no source.
	catalog *catalog.Catalog
	git     []GitCredential
	// storage holds the host's S3 credentials by pool; a pool without an entry uses bundleStorage, and without that has
	// no storage on this host.
	storage map[string]StorageConfig
	// bundleStorage is the tenant's S3 key pair the bundle carries, zero for none: the keys of every pool with storage
	// that storage does not map. It is no credential, so no pool's terms can request it.
	bundleStorage operator.StorageKeys
	logger        *log.Logger
	// follower is the host's reader of the tenant feed, kept while its context lasts.
	followMu sync.Mutex
	follower *follower
	// tracing is the host role's tracer provider, nil until Trace turns it on; awaited holds, for each run a wait of
	// this process follows, the feed records whose CONSUMER span it has emitted.
	tracing   *telemetry.Tracing
	awaitedMu sync.Mutex
	awaited   map[string]*awaited
	// gitVersionOK records that the host's git reads configuration from the environment, which an https credential for
	// ls-remote needs. Only a pass is kept: a failed check, such as one whose submit was cancelled, runs again.
	gitVersionMu sync.Mutex
	gitVersionOK bool
}

// awaited is a run the process waits on: how many waits follow it and the records traced, by topic and offset.
type awaited struct {
	waiters int
	traced  map[string]bool
}

// NewHost constructs a host over a plain Kafka connection authenticated as principal.
func NewHost(kafka *kgo.Client, key ed25519.PrivateKey, principal, stateDir string, credentials map[string]credential.Source, git []GitCredential, logger *log.Logger) (*Host, error) {
	// TenantPrincipal("", "") is the prefix and the separator between tenant and host.
	names, prefixed := strings.CutPrefix(principal, strings.TrimSuffix(operator.TenantPrincipal("", ""), "."))
	tenant, host, split := strings.Cut(names, ".")
	if !prefixed || !split || !wire.ValidName(tenant) || !wire.ValidName(host) {
		return nil, errors.New("principal is not a host principal wherry.tnt.<tenant>.<host>")
	}
	c, err := catalog.Embedded()
	if err != nil {
		return nil, err
	}
	return &Host{kafka: kafka, reader: operator.NewReader(kafka), key: key, tenant: tenant, host: host, stateDir: stateDir, credentials: credentials, catalog: c, git: git, logger: logger, awaited: map[string]*awaited{}}, nil
}

// Trace turns the host's tracing on, as its configuration's tracing field does: its spans go to its tenant's trace
// topic through a trace client built from its own Kafka client's options.
func (h *Host) Trace() error {
	tracing, err := telemetry.New(context.Background(), telemetry.RoleHost, telemetry.Names{Tenant: h.tenant, Host: h.host}, h.kafka.Opts(), h.logger)
	if err != nil {
		return err
	}
	h.tracing = tracing
	return nil
}

// Close flushes the host's spans within telemetry.ShutdownBound and closes their client, then closes the Kafka
// connections of its own.
func (h *Host) Close() {
	if err := h.tracing.Close(); err != nil {
		h.logger.Printf("tracing: %v", err)
	}
	h.reader.Close()
	h.kafka.Close()
}

// Register registers the host key on the host's key topic unless that key is already there, and returns its key ID.
func (h *Host) Register(ctx context.Context) (string, error) {
	owner := signed.HostOwner(h.tenant, h.host)
	topic := operator.HostTopic(h.tenant, h.host)
	keys, err := h.reader.ReadKeys(ctx, map[string]string{owner: topic})
	if err != nil {
		return "", err
	}
	public := h.key.Public().(ed25519.PublicKey)
	switch state := keys[owner]; {
	case state.Current == nil:
		record, err := signed.RegisterKey(owner, h.key)
		if err != nil {
			return "", err
		}
		if err := h.produce(ctx, &kgo.Record{Topic: topic, Key: []byte(owner), Value: record}); err != nil {
			return "", err
		}
		// A concurrent first registration may have landed first; only the first counts.
		keys, err := h.reader.ReadKeys(ctx, map[string]string{owner: topic})
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

// view is wherry.operator and the keys of the receivers of the tenant's pools.
type view struct {
	state operator.State
	keys  signed.Keys
}

func (h *Host) load(ctx context.Context) (view, error) {
	state, err := h.reader.Load(ctx)
	if err != nil {
		return view{}, err
	}
	topics := map[string]string{}
	for pool := range state.Entitlements[h.tenant] {
		maps.Copy(topics, operator.PoolKeyTopics(state, pool))
	}
	keys, err := h.reader.ReadKeys(ctx, topics)
	if err != nil {
		return view{}, err
	}
	return view{state, keys}, nil
}

// Submit places a task through the live receiver with the most spare capacity that can take it, and publishes it to
// that receiver's pool. It is the root span of the run's trace.
func (h *Host) Submit(ctx context.Context, input SubmitInput) (SubmitResult, error) {
	ctx, span := h.tracing.Root(ctx, "submit", trace.SpanKindInternal)
	defer span.End()
	result, err := h.submit(ctx, input)
	if err != nil {
		telemetry.Fail(span, submitClass(err))
	}
	return result, err
}

// submitClass is the fixed class of a failed submission.
func submitClass(err error) string {
	switch {
	case errors.Is(err, ErrInvalidInput):
		return telemetry.ClassInvalid
	case errors.Is(err, ErrNoMatchingReceiver):
		return telemetry.ClassUnplaced
	case errors.Is(err, ErrStartByPastLimit):
		return telemetry.ClassStartBy
	case errors.Is(err, ErrNoSnapshot), errors.Is(err, ErrCustodianUnavailable):
		return telemetry.ClassNoKey
	}
	return telemetry.ClassError
}

func (h *Host) submit(ctx context.Context, input SubmitInput) (SubmitResult, error) {
	h.resubmitLost(ctx)
	// Checked before placement: an invalid key matches no offer and would read as no matching receiver.
	for _, name := range input.Requested {
		if !wire.ValidName(name) {
			// Not echoed: a malformed name may be a pasted credential value.
			return SubmitResult{}, fmt.Errorf("%w: a requested credential is not a valid credential name", ErrInvalidInput)
		}
	}
	if err := wire.ValidateRequirements(input.Selector); err != nil {
		return SubmitResult{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	asked := input.Outputs
	var continued continuation
	if input.Continue != "" {
		if input.Git != nil || input.GitChanges != "" {
			return SubmitResult{}, fmt.Errorf("%w: continue takes the git input of the run it continues, so not git or git changes", ErrInvalidInput)
		}
		var names []string
		for _, part := range input.Parts {
			if part.Raw != nil || part.Data != nil {
				names = append(names, wire.InputName(part))
			}
		}
		for _, file := range slices.Concat(input.Files, input.S3Inputs) {
			names = append(names, file.Name)
		}
		for _, output := range input.Outputs {
			names = append(names, output.Name)
		}
		if slices.Contains(names, baseBundle) || slices.Contains(names, wire.GitBundle) {
			return SubmitResult{}, fmt.Errorf("%w: with continue, %s and %s are reserved for the bundles", ErrInvalidInput, baseBundle, wire.GitBundle)
		}
		var err error
		if continued, err = h.continued(input.Continue); err != nil {
			return SubmitResult{}, err
		}
		defer continued.bundle.Close()
		previous := continued.last.Task.Wherry.Git
		input.Git = &wire.GitInput{Remote: previous.Remote, Ref: previous.Ref, Path: previous.Path}
		input.S3Inputs = append(slices.Clone(input.S3Inputs), S3File{Name: baseBundle, Body: continued.bundle})
		// A plain expected output: over the inline budget it goes through the pool's bucket, and a receiver never
		// requires a git input's changes file.
		input.Outputs = append(slices.Clone(input.Outputs), wire.ExpectedOutput{Name: wire.GitBundle})
	}
	outputs := input.Outputs
	// One resolver: the bytes that decide what is supplied are the bytes the envelope carries, the storage keys sign
	// with and ls-remote authenticates with.
	resolver := newResolver(ctx, wire.RFC3339(input.EndBy.Unix()))
	if input.Git != nil {
		for _, output := range asked {
			if output.Name == wire.GitBundle || output.Name == wire.GitPatch {
				return SubmitResult{}, fmt.Errorf("%w: the changes file comes back by default with --git; drop --output %s", ErrInvalidInput, output.Name)
			}
		}
		git := *input.Git
		// The current repository and an unpinned remote need git on the host; a pinned remote runs none.
		var runner *hostGit
		var local currentRepo
		if git.Remote == "." || git.Ref == "" {
			var err error
			if runner, err = newHostGit(ctx); err != nil {
				return SubmitResult{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
			}
			defer runner.close()
		}
		if git.Remote == "." {
			if git.Ref != "" {
				return SubmitResult{}, fmt.Errorf("%w: the current repository is sent at its HEAD commit; drop the ref", ErrInvalidInput)
			}
			var err error
			if local, err = runner.current(); err != nil {
				return SubmitResult{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
			}
			git.Remote, git.Ref = local.origin, local.commit
		}
		git.Remote = sshRemote(git.Remote)
		// The entry whose URL is the longest prefix of the remote gives the clone its credential and host key.
		git.Credential, git.HostKey = "", ""
		var matched *GitCredential
		for i, entry := range h.git {
			if strings.HasPrefix(git.Remote, entry.URL) && (matched == nil || len(entry.URL) > len(matched.URL)) {
				matched = &h.git[i]
			}
		}
		if matched != nil && matched.Credential != "" {
			git.Credential = matched.Credential
			input.Credentials = append(slices.Clone(input.Credentials), wire.TaskCredential{Name: matched.Credential, File: wire.GitCredentialFile})
		}
		if matched != nil && matched.HostKey != "" {
			file, err := os.Open(matched.HostKey)
			if err != nil {
				return SubmitResult{}, err
			}
			// One byte over the bound is read, so validation refuses a longer file without reading all of it.
			key, err := io.ReadAll(io.LimitReader(file, wire.MaxGitHostKey+1))
			file.Close()
			if err != nil {
				return SubmitResult{}, err
			}
			git.HostKey = string(key)
		}
		// An unpinned remote is validated under a ref name before git is asked for its HEAD's commit.
		pinned := git.Ref != ""
		if !pinned {
			git.Ref = "HEAD"
		}
		// Validate never quotes a value in its errors, so its text is safe to return.
		if err := git.Validate(input.Credentials); err != nil {
			return SubmitResult{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
		}
		if runner != nil {
			if err := h.remoteGit(runner, git, matched, resolver); err != nil {
				return SubmitResult{}, err
			}
			if pinned {
				on, err := runner.onRemote(git.Remote, git.Ref)
				if err != nil {
					return SubmitResult{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
				}
				if !on {
					hint := "push the commit to a branch first"
					if local.branch != "" {
						hint = "push " + local.branch + " first"
					}
					return SubmitResult{}, fmt.Errorf("%w: commit %s is not on %s: %s, or git fetch and retry", ErrInvalidInput, git.Ref[:12], git.Remote, hint)
				}
			} else {
				commit, err := runner.defaultHead(git.Remote)
				if err != nil {
					return SubmitResult{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
				}
				git.Ref = commit
			}
		}
		input.Git = &git
		changes := map[string]string{"": wire.GitBundle, "bundle": wire.GitBundle, "patch": wire.GitPatch, "none": ""}
		name, ok := changes[input.GitChanges]
		if !ok {
			return SubmitResult{}, fmt.Errorf("%w: git changes must be bundle, patch or none", ErrInvalidInput)
		}
		if name != "" && !slices.ContainsFunc(outputs, func(output wire.ExpectedOutput) bool { return output.Name == name }) {
			outputs = append(slices.Clone(outputs), wire.ExpectedOutput{Name: name})
		}
	} else if input.GitChanges != "" {
		return SubmitResult{}, fmt.Errorf("%w: git changes need a git input", ErrInvalidInput)
	}
	for _, credential := range input.Credentials {
		if _, configured := h.credentials[credential.Name]; !configured {
			return SubmitResult{}, fmt.Errorf("%w: auth required: credential %s is not in this host's configuration", ErrInvalidInput, credential.Name)
		}
	}
	// A receiver rejects a task_ended_by past task_started_by plus the run cap.
	if limit := input.StartBy.Add(time.Duration(operator.MaxRunDurationMS) * time.Millisecond); input.EndBy.After(limit) {
		return SubmitResult{}, fmt.Errorf("%w: task_ended_by (%s) is past task_started_by plus the 6h run cap (%s)", ErrInvalidInput, wire.RFC3339(input.EndBy.Unix()), wire.RFC3339(limit.Unix()))
	}
	stored := len(input.S3Inputs) > 0
	// Connect folds the key topic only when <host_key>.next exists, and receivers refuse a task under any key but the
	// current one without writing a status.
	owner := signed.HostOwner(h.tenant, h.host)
	var keys signed.Keys
	var v view
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() (err error) {
		_, read := telemetry.ChildOf(groupCtx, "read host key", trace.SpanKindClient)
		defer read.End()
		keys, err = h.reader.ReadKeys(groupCtx, map[string]string{owner: operator.HostTopic(h.tenant, h.host)})
		return err
	})
	group.Go(func() (err error) {
		_, read := telemetry.ChildOf(groupCtx, "read state", trace.SpanKindClient)
		defer read.End()
		v, err = h.load(groupCtx)
		return err
	})
	if err := group.Wait(); err != nil {
		return SubmitResult{}, err
	}
	if current := keys[owner].Current; current == nil || !current.Equal(h.key.Public()) {
		return SubmitResult{}, errors.New("the host key is not the current key of this host's key topic: when <host_key>.next exists, the next wherry command completes the interrupted wherry host key rotate; otherwise this host was re-keyed elsewhere and needs wherry host init on a re-provisioned host")
	}
	now := time.Now()
	// A receiver keeps its plugin's declared limit through a restart, so an advertisement of any age counts for the
	// profile limit; placement takes live ones alone.
	_, readAds := telemetry.ChildOf(ctx, "read advertisements", trace.SpanKindClient)
	declared, err := h.reader.ReadAdvertisements(ctx, v.state, v.keys, slices.Sorted(maps.Keys(v.state.Entitlements[h.tenant])), now, math.MaxInt64)
	readAds.End()
	if err != nil {
		return SubmitResult{}, err
	}
	ads := maps.Clone(declared)
	maps.DeleteFunc(ads, func(_ string, ad wire.Advertisement) bool {
		age := now.Sub(time.UnixMilli(ad.AtMS))
		return age > operator.Liveness || age < -operator.Liveness
	})
	pool, placement, topic, err := resolvePlacement(v.state, h.tenant, input.Agent, input.Model, input.Selector, ads)
	if err != nil {
		return SubmitResult{}, err
	}
	trace.SpanFromContext(ctx).SetAttributes(telemetry.AttrPool.String(pool), telemetry.AttrAgent.String(placement.Agent), telemetry.AttrModel.String(placement.Model))
	// Before any credential resolves or input uploads. The state is loaded before the snapshot is read and the snapshot
	// before the produce, so a task carries the outgoing key only if it was read before a successor's snapshot.
	poolKey, err := h.poolKey(ctx, v, pool)
	if err != nil {
		return SubmitResult{}, err
	}
	agents := v.state.Pools[pool].Agents
	agent := agents[slices.IndexFunc(agents, func(agent operator.AgentTerms) bool { return agent.Name == placement.Agent })]
	for _, name := range input.Requested {
		if !slices.ContainsFunc(agent.Credentials, func(credential operator.AgentCredential) bool { return credential.Name == name }) {
			return SubmitResult{}, fmt.Errorf("%w: requested credential %s is not one of agent %s's credentials", ErrInvalidInput, name, agent.Name)
		}
	}
	supplied, unsupplied, err := h.supplied(agent, input.Requested, resolver)
	if err != nil {
		return SubmitResult{}, err
	}
	if err := h.atLeastOne(agent, supplied); err != nil {
		return SubmitResult{}, err
	}
	for _, credential := range input.Credentials {
		if slices.ContainsFunc(agent.Targets(), func(target wire.TaskCredential) bool { return target.Target() == credential.Target() }) {
			return SubmitResult{}, fmt.Errorf("%w: task credential %s takes the %s of one of agent %s's credentials", ErrInvalidInput, credential.Name, credential.Target(), agent.Name)
		}
	}
	storable := v.state.Pools[pool].Storage != nil && h.credentialed(pool)
	input.Parts = slices.Clone(input.Parts)
	for _, file := range input.Files {
		size, err := file.Body.Seek(0, io.SeekEnd)
		if err != nil {
			return SubmitResult{}, err
		}
		if storable && size > v.state.Pools[pool].RecordLimit {
			input.S3Inputs = append(slices.Clone(input.S3Inputs), file)
			stored = true
			continue
		}
		if _, err := file.Body.Seek(0, io.SeekStart); err != nil {
			return SubmitResult{}, err
		}
		data, err := io.ReadAll(file.Body)
		if err != nil {
			return SubmitResult{}, err
		}
		input.Parts = append(input.Parts, wire.Part{Raw: data, Filename: file.Name})
	}
	s3Names := make([]string, len(input.S3Inputs))
	for i, file := range input.S3Inputs {
		s3Names[i] = file.Name
	}
	if err := wire.ValidateInputs(input.Parts, s3Names, math.MaxInt64); err != nil {
		return SubmitResult{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	if continued.last != nil && !storable {
		return SubmitResult{}, fmt.Errorf("%w: continue needs a pool with storage and a host storage credential: the bundles travel through S3", ErrInvalidInput)
	}
	toS3 := func() {
		parts := make([]wire.Part, 0, len(input.Parts))
		for _, part := range input.Parts {
			switch {
			case part.Raw != nil:
				input.S3Inputs = append(input.S3Inputs, S3File{Name: wire.InputName(part), Body: bytes.NewReader(part.Raw)})
			case part.Data != nil:
				input.S3Inputs = append(input.S3Inputs, S3File{Name: wire.InputName(part), Body: bytes.NewReader(part.Data)})
			default:
				parts = append(parts, part)
			}
		}
		input.Parts = parts
		stored = true
	}
	if limit, limiting := profileInlineLimit(v.state, pool, topic, declared); limit > 0 {
		if err := wire.ValidateInputs(input.Parts, s3Names, limit); err != nil {
			if !storable {
				return SubmitResult{}, fmt.Errorf("%w: inline inputs are over profile %s's %d-byte limit; a pool with storage and a host storage credential would carry the inputs through S3", ErrInvalidInput, limiting, limit)
			}
			toS3()
		}
	}
	if input.StartBy.After(now.Add(v.state.Pools[pool].StartByLimit())) {
		return SubmitResult{}, ErrStartByPastLimit
	}
	if input.Git != nil {
		if host := input.Git.VCSHost(); !slices.Contains(v.state.Pools[pool].VCS, host) {
			return SubmitResult{}, fmt.Errorf("%w: git remote host %s is not in pool %s's vcs hosts", ErrInvalidInput, host, pool)
		}
	}
	nonce := wire.RunNonce()
	runID := wire.RunID(h.tenant, h.host, nonce)
	trace.SpanFromContext(ctx).SetAttributes(telemetry.AttrRun.String(runID))
	submission, err := h.submission(runID, nonce, pool, placement, append([]wire.Part{{Text: input.Prompt}}, input.Parts...), input.StartBy, input.EndBy)
	if err != nil {
		return SubmitResult{}, err
	}
	submission.Wherry.Credentials, submission.Wherry.Supplied, submission.Wherry.Outputs, submission.Wherry.Git = input.Credentials, supplied, outputs, input.Git
	if continued.last != nil {
		submission.Message.ContextID, submission.Message.ReferenceTaskIDs = continued.last.Task.Message.ContextID, []string{continued.last.Run}
	}
	encoded, err := wire.SignRecord(submission, h.key)
	if err != nil {
		return SubmitResult{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	if limit := v.state.Pools[pool].RecordLimit; int64(len(encoded)) > limit {
		if !storable {
			return SubmitResult{}, fmt.Errorf("%w: the submission is a %d-byte record, over pool %s's %d-byte record limit; a pool with storage and a host storage credential would carry the inputs through S3", ErrInvalidInput, len(encoded), pool, limit)
		}
		toS3()
		submission.Message.Parts = append([]wire.Part{{Text: input.Prompt}}, input.Parts...)
		if encoded, err = wire.SignRecord(submission, h.key); err != nil {
			return SubmitResult{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
		}
		if int64(len(encoded)) > limit {
			return SubmitResult{}, fmt.Errorf("%w: the %d-byte prompt makes a %d-byte submission record, over pool %s's %d-byte record limit", ErrInvalidInput, len(input.Prompt), len(encoded), pool, limit)
		}
	}
	storage := v.state.Pools[pool].Storage
	// Every storable run gets a POST policy for its outputs, so its storage credential resolves whether or not an input
	// goes through the bucket.
	var store *store
	if storable {
		if store, err = h.bucket(ctx, pool, storage, resolver); err != nil {
			return SubmitResult{}, err
		}
	}
	if stored {
		submission.Wherry.S3Inputs, err = uploadInputs(ctx, store, storage, h.tenant, runID, input.S3Inputs)
	}
	if err == nil && continued.last != nil {
		uploaded := submission.Wherry.S3Inputs[slices.IndexFunc(submission.Wherry.S3Inputs, func(input wire.S3Input) bool { return input.Name == baseBundle })]
		if collected := continued.last.Bundle; uploaded.Size != collected.Size || uploaded.SHA256 != collected.SHA256 {
			err = fmt.Errorf("%w: continue: the %s collected for run %s changed after result verified it", ErrInvalidInput, wire.GitBundle, continued.last.Run)
		}
		// Set once base.bundle is among the S3 inputs, which validation requires of the base.
		submission.Wherry.Git.Base = &wire.GitBase{Bundle: baseBundle, Commit: continued.commit}
	}
	redactionKey := make([]byte, 32)
	if err == nil {
		_, err = rand.Read(redactionKey)
	}
	if err == nil {
		encoded, err = h.enveloped(ctx, submission, v.state.Pools[pool], poolKey, store, redactionKey, resolver)
	}
	if limit := v.state.Pools[pool].RecordLimit; err == nil && int64(len(encoded)) > limit {
		err = fmt.Errorf("%w: with its envelope the submission is a %d-byte record, over pool %s's %d-byte record limit", ErrInvalidInput, len(encoded), pool, limit)
	}
	if err == nil {
		err = h.publish(ctx, submission, topic, encoded, now, redactionKey, store != nil)
	}
	if err != nil {
		if stored {
			if cleanupErr := store.removePrefix(ctx, storage.Bucket, wire.S3Prefix(h.tenant, runID)); cleanupErr != nil {
				store.logf("run %s: delete the S3 inputs of a task not published: %v", runID, cleanupErr)
			}
		}
		return SubmitResult{}, err
	}
	return SubmitResult{RunID: runID, Pool: pool, Unsupplied: unsupplied}, nil
}

// poolKey returns the current key of pool: that of the latest committed record under the snapshot key on the pool's
// snapshot topic, when its writer is the custodian it names and that custodian is unrevoked. An earlier record never
// stands in. A suspended pool has none.
func (h *Host) poolKey(ctx context.Context, v view, pool string) (*ecdsa.PublicKey, error) {
	if v.state.Suspended[pool] {
		return nil, fmt.Errorf("pool %s is suspended; submit again once it is resumed", pool)
	}
	_, read := telemetry.ChildOf(ctx, "read pool key", trace.SpanKindClient, telemetry.AttrPool.String(pool))
	records, err := h.reader.ReadFrom(ctx, operator.CustodianTopic(pool), 0)
	read.End()
	if err != nil {
		return nil, err
	}
	var latest *kgo.Record
	for _, record := range records {
		// A record under another key is not a snapshot: compaction keeps the latest record of each key.
		if string(record.Key) == wire.CustodianKey {
			latest = record
		}
	}
	if latest == nil {
		return nil, fmt.Errorf("%w: pool %s has no custodian yet; submit again once a receiver of the pool has published one", ErrNoSnapshot, pool)
	}
	snapshot, err := operator.AuthorizeSnapshot(v.state, v.keys, latest.Value, pool)
	if err != nil {
		return nil, fmt.Errorf("%w: pool %s's current snapshot: %v", ErrCustodianUnavailable, pool, err)
	}
	if snapshot.Reason == "takeover" || snapshot.Reason == "contested" {
		h.logger.Printf("pool %s: custodian %s holds the pool key by %s (WherryCustodianTakeover)", pool, snapshot.Custodian, snapshot.Reason)
	}
	return snapshot.Current.Key.Key.(*ecdsa.PublicKey), nil
}

// enveloped signs submission with an envelope of its credentials, its S3 links and redactionKey, under a fresh content
// key wrapped to poolKey. The credentials' values come through resolver, which supplied filled for the agent's
// credentials, so no source runs again here; a task credential's source is the configuration's. GET links last until
// task_started_by plus getLifetime; with a store, the run's POST policy lasts until task_ended_by plus the upload grace.
func (h *Host) enveloped(ctx context.Context, submission *wire.Submission, terms operator.Terms, poolKey *ecdsa.PublicKey, store *store, redactionKey []byte, resolver *resolver) ([]byte, error) {
	binding := submission.Wherry
	values := signed.EnvelopeValues{Binding: binding.EnvelopeBinding(), Credentials: map[string][]byte{}, S3Inputs: map[string]string{}, RedactionKey: redactionKey}
	sources := map[string]credential.Source{}
	if i := slices.IndexFunc(terms.Agents, func(agent operator.AgentTerms) bool { return agent.Name == binding.Placement.Agent }); i >= 0 {
		for _, s := range h.sources(terms.Agents[i]) {
			if s.configured {
				sources[s.Name] = s.source
			}
		}
	}
	names := slices.Clone(binding.Supplied)
	for _, credential := range binding.Credentials {
		names = append(names, credential.Name)
		if source, configured := h.credentials[credential.Name]; configured {
			sources[credential.Name] = source
		}
	}
	for _, name := range names {
		if _, resolved := values.Credentials[name]; resolved {
			continue
		}
		source, configured := sources[name]
		if !configured {
			return nil, fmt.Errorf("%w: auth required: credential %s is not in this host's configuration", ErrInvalidInput, name)
		}
		_, resolve := telemetry.Child(ctx, "resolve credential", telemetry.AttrCredential.String(name), telemetry.AttrCredentialSource.String(sourceKind(source)))
		value, err := resolver.resolve(name, source)
		if err != nil {
			telemetry.Fail(resolve, telemetry.ClassError)
		}
		resolve.End()
		if err != nil {
			return nil, fmt.Errorf("resolve credential %s: %w", name, err)
		}
		values.Credentials[name] = value
	}
	if store != nil {
		var inputs []string
		for _, input := range binding.S3Inputs {
			inputs = append(inputs, input.Name)
		}
		var err error
		if values.S3Inputs, values.S3Policy, err = presign(ctx, store, terms.Storage.Bucket, h.tenant, binding.InputRun(), binding.Run, inputs,
			time.Unix(binding.TaskStartedBy, 0).Add(getLifetime), time.Unix(binding.TaskEndedBy, 0)); err != nil {
			return nil, err
		}
	}
	_, seal := telemetry.Child(ctx, "seal envelope")
	defer seal.End()
	if err := operator.CheckEnvelope(terms, submission, values); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	contentKey := make([]byte, 32)
	if _, err := rand.Read(contentKey); err != nil {
		return nil, err
	}
	envelope, err := signed.BuildEnvelope(values, contentKey)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	wrap, err := signed.WrapContentKey(contentKey, poolKey)
	if err != nil {
		return nil, err
	}
	submission.Wherry.Envelope, submission.Wherry.Wrap = string(envelope), string(wrap)
	encoded, err := wire.SignRecord(submission, h.key)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	return encoded, nil
}

// remoteGit gives runner the matched git entry's credential for ls-remote, by the entry's URL scheme: an ssh key goes in
// a 0600 file in the scratch directory, which ssh reads with the entry's host key file and nothing else; an https token
// goes in an Authorization header set through the environment, which needs git 2.31. Its value reaches no command line.
func (h *Host) remoteGit(runner *hostGit, git wire.GitInput, matched *GitCredential, resolver *resolver) error {
	var value []byte
	if matched != nil && matched.Credential != "" {
		source, configured := h.credentials[matched.Credential]
		if !configured {
			return fmt.Errorf("%w: auth required: credential %s is not in this host's configuration", ErrInvalidInput, matched.Credential)
		}
		var err error
		if value, err = resolver.resolve(matched.Credential, source); err != nil {
			return fmt.Errorf("resolve credential %s: %w", matched.Credential, err)
		}
	}
	switch {
	case strings.HasPrefix(git.Remote, "ssh://"):
		identity := "-o IdentityFile=/dev/null"
		if value != nil {
			key := filepath.Join(runner.scratch, "key")
			if err := os.WriteFile(key, slices.Concat(bytes.TrimRight(value, "\n"), []byte{'\n'}), 0o600); err != nil {
				return err
			}
			identity = "-i " + shellQuote(key)
		}
		runner.ssh = "ssh -F /dev/null " + identity + " -o IdentitiesOnly=yes -o IdentityAgent=none -o ProxyCommand=none -o BatchMode=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=" +
			shellQuote(matched.HostKey) + " -o GlobalKnownHostsFile=/dev/null"
	case value != nil:
		h.gitVersionMu.Lock()
		var err error
		if !h.gitVersionOK {
			var major, minor int
			major, minor, err = runner.version()
			switch {
			case err != nil:
			case major < 2 || major == 2 && minor < 31:
				err = fmt.Errorf("git %d.%d is older than 2.31, which sending an https credential needs", major, minor)
			default:
				h.gitVersionOK = true
			}
		}
		h.gitVersionMu.Unlock()
		if err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidInput, err)
		}
		runner.header = "Authorization: Basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:"+strings.TrimSpace(string(value))))
	}
	return nil
}

// sourceKind is the kind of a credential source, file, env or command: what a span may carry of it, never its key.
func sourceKind(source credential.Source) string {
	switch {
	case source.Env != "":
		return SourceEnv
	case len(source.Command) > 0:
		return SourceCommand
	}
	return SourceFile
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

// errReplaced is cancel's refusal of a run a resubmission replaced before cancel took the run's lock.
var errReplaced = errors.New("run replaced")

// Cancel publishes a cancel of the last run of runID's resubmission chain to the events topic of its pool, and records
// when it was first requested in that run's local record.
func (h *Host) Cancel(ctx context.Context, runID string) (CancelResult, error) {
	for {
		chain, err := h.chain(runID)
		if err != nil {
			return CancelResult{}, err
		}
		// A run's record is written under its own lock and, once it is a replacement, under that of the run it replaces.
		result, err := h.cancel(ctx, chain[max(len(chain)-2, 0):])
		if !errors.Is(err, errReplaced) {
			return result, err
		}
	}
}

// cancel cancels the last of runs, holding the lock of each.
func (h *Host) cancel(ctx context.Context, runs []string) (CancelResult, error) {
	for _, run := range runs {
		unlock, err := h.lockRun(ctx, run)
		if err != nil {
			return CancelResult{}, fmt.Errorf("cancel %s: %w", run, err)
		}
		defer unlock()
	}
	runID := runs[len(runs)-1]
	if chain, err := h.chain(runID); err != nil {
		return CancelResult{}, err
	} else if len(chain) > 1 {
		return CancelResult{}, errReplaced
	}
	r, err := h.history(ctx, runID)
	if err != nil {
		return CancelResult{}, err
	}
	if r.pool == "" {
		return CancelResult{}, fmt.Errorf("cancel %s: %w", runID, ErrRunNotFound)
	}
	encoded, err := wire.SignRecord(&wire.Cancel{Tenant: h.tenant, Host: h.host, Pool: r.pool, Run: runID}, h.key)
	if err != nil {
		return CancelResult{}, err
	}
	// In the run's trace, under the submit context its local record saved; a root when this host saved none.
	topic := operator.PoolEventsTopic(r.pool)
	sendCtx, send := h.tracing.ProducerUnder(ctx, topic, telemetry.ParseTraceParent(r.local.traceParent()), telemetry.AttrRun.String(runID), telemetry.AttrPool.String(r.pool))
	record := &kgo.Record{Topic: topic, Key: []byte(runID), Value: encoded}
	h.tracing.Inject(sendCtx, record)
	err = h.produce(ctx, record)
	if err != nil {
		telemetry.Fail(send, telemetry.ClassError)
	}
	send.End()
	if err != nil {
		return CancelResult{}, err
	}
	if local := r.local; local != nil && local.CancelRequestedMS == 0 {
		local.CancelRequestedMS = time.Now().UnixMilli()
		if err := h.writeRun(*local); err != nil {
			return CancelResult{}, fmt.Errorf("cancel %s was published; recording it locally: %w", runID, err)
		}
	}
	return CancelResult{RunID: runID, Skipped: r.skipped}, nil
}

func (h *Host) submission(runID, nonce, pool string, placement wire.Placement, parts []wire.Part, startBy, endBy time.Time) (*wire.Submission, error) {
	return &wire.Submission{
		Message: wire.Message{MessageID: wire.RandomID("msg-"), ContextID: runID, TaskID: runID, Role: wire.RoleUser, Parts: parts, Extensions: []string{wire.ExtensionURI}},
		Wherry: wire.SubmissionBinding{
			Tenant: h.tenant, Host: h.host, Pool: pool, Run: runID, RunNonce: nonce, Placement: placement,
			TaskStartedBy: startBy.Unix(), TaskEndedBy: endBy.Unix(),
		},
	}, nil
}

// publish records the run locally, then writes encoded, the signed submission, to topic, its signature's task topic. A
// failed write removes the record.
func (h *Host) publish(ctx context.Context, submission *wire.Submission, topic string, encoded []byte, now time.Time, redactionKey []byte, policy bool) error {
	record, err := h.newRecord(ctx, submission, topic, encoded, now, redactionKey, policy)
	if err != nil {
		return err
	}
	if err := h.writeRun(record); err != nil {
		return err
	}
	if err := h.produceTask(ctx, topic, submission.Wherry, encoded); err != nil {
		if path, ok := h.runPath(record.Run); ok {
			os.Remove(path)
		}
		return err
	}
	return nil
}

// newRecord is the local record of a run whose signed submission is encoded, bound for topic, holding redactionKey, the
// envelope's, whether the envelope carries a POST policy, and the submission without its envelope as the run's
// resubmission template.
func (h *Host) newRecord(ctx context.Context, submission *wire.Submission, topic string, encoded []byte, now time.Time, redactionKey []byte, policy bool) (runRecord, error) {
	binding := submission.Wherry
	template := *submission
	template.Wherry.Envelope, template.Wherry.Wrap = "", ""
	record := runRecord{
		Run: binding.Run, Pool: binding.Pool, Topic: topic, Placement: binding.Placement, TaskHash: wire.TaskHash(encoded), SubmittedMS: now.UnixMilli(), TaskStartedBy: binding.TaskStartedBy,
		TaskEndedBy: binding.TaskEndedBy, RedactionKey: redactionKey, Task: &template, TraceParent: h.tracing.TraceParent(ctx), Policy: policy,
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
	// Listed before the write: a receiver that claims at once writes the run's first records after the submission.
	ends, err := operator.ListOffsets(ctx, kadm.NewClient(h.kafka).ListCommittedOffsets, operator.TenantTopic(h.tenant))
	if err != nil {
		return runRecord{}, err
	}
	ends.Each(func(end kadm.ListedOffset) { record.FeedOffset = end.Offset })
	return record, nil
}

// produceTask writes encoded, a signed submission, to topic, a task topic of its pool, under the run's PRODUCER span,
// whose context travels in the record's traceparent header.
func (h *Host) produceTask(ctx context.Context, topic string, binding wire.SubmissionBinding, encoded []byte) error {
	sendCtx, send := h.tracing.Producer(ctx, topic, telemetry.AttrRun.String(binding.Run), telemetry.AttrPool.String(binding.Pool),
		telemetry.AttrAgent.String(binding.Placement.Agent), telemetry.AttrModel.String(binding.Placement.Model), telemetry.AttrSize.Int(len(encoded)))
	defer send.End()
	record := &kgo.Record{Topic: topic, Key: []byte(binding.Run), Value: encoded}
	h.tracing.Inject(sendCtx, record)
	err := h.produce(ctx, record)
	switch {
	case errors.Is(err, kerr.MessageTooLarge):
		telemetry.Fail(send, telemetry.ClassTooLarge)
		return fmt.Errorf("%w: the submission is a %d-byte record, over the broker's record limit; shorten the prompt or the input files", ErrInvalidInput, len(encoded))
	case err != nil:
		telemetry.Fail(send, telemetry.ClassError)
	}
	return err
}

// Receivers returns the receivers of the pools the tenant is entitled to, with their latest advertisement and the
// status operator.ReceiverStatus gives it.
func (h *Host) Receivers(ctx context.Context) ([]Receiver, error) {
	v, err := h.load(ctx)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	ads, err := h.reader.ReadAdvertisements(ctx, v.state, v.keys, slices.Sorted(maps.Keys(v.state.Entitlements[h.tenant])), now, math.MaxInt64)
	if err != nil {
		return nil, err
	}
	result := []Receiver{}
	for id, config := range v.state.Receivers {
		if !v.state.Entitlements[h.tenant][config.Pool] {
			continue
		}
		receiver := Receiver{
			ReceiverID: id, Pool: config.Pool, Profiles: []Profile{}, Status: operator.ReceiverStatus(v.state, ads, id, now),
		}
		ended := receiver.Status == "ended"
		ad, advertised := ads[id]
		if !ended && advertised {
			receiver.Capacity, receiver.InFlight = ad.Capacity, ad.InFlight
			receiver.LastAdvertised = time.UnixMilli(ad.AtMS).UTC()
			if receiver.Status == "live" {
				receiver.Gated = ad.Gated
			}
		}
		for _, profile := range config.Profiles {
			if ended {
				break
			}
			labels := profile.Labels
			if labels == nil {
				labels = map[string]string{}
			}
			offerIndex := slices.IndexFunc(ad.Offers, func(offer wire.Offer) bool { return offer.Profile == profile.Name })
			ready := receiver.Status == "live" && offerIndex >= 0
			listed := Profile{Name: profile.Name, Plugin: profile.Plugin, Agent: profile.Agent, Labels: labels, Ready: ready}
			if offerIndex >= 0 {
				listed.InlineLimit = ad.Offers[offerIndex].InlineLimit
			}
			if ready {
				listed.Replicas, listed.InFlight = replicas(ad.Offers[offerIndex]), ad.Offers[offerIndex].InFlight
			}
			agents := v.state.Pools[config.Pool].Agents
			if index := slices.IndexFunc(agents, func(agent operator.AgentTerms) bool { return agent.Name == profile.Agent }); index >= 0 {
				listed.Models, listed.DefaultModel = agents[index].Models, agents[index].DefaultModel
				// From discovery alone: no command runs to list receivers.
				for _, s := range h.sources(agents[index]) {
					listed.Credentials = append(listed.Credentials, Credential{Name: s.Name, Env: s.Env, File: s.File, Description: s.Description, Optional: s.Optional(),
						Configured: s.configured, Source: s.kind(), Variable: s.variable(), Status: s.status()})
				}
			}
			receiver.Profiles = append(receiver.Profiles, listed)
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

// runRecord is the submitting host's own record of a run, which lets it report the run before a
// receiver echoes it.
type runRecord struct {
	Run  string `json:"run"`
	Pool string `json:"pool"`
	// Topic is the signature task topic the run was produced to, which a replacement keeps.
	Topic     string         `json:"topic"`
	Placement wire.Placement `json:"placement"`
	TaskHash  string         `json:"task_hash"`
	// SubmittedMS is when this host published the run.
	SubmittedMS int64 `json:"submitted_at_ms,omitzero"`
	// TaskCredentials names the task credentials the run declares.
	TaskCredentials []string `json:"task_credentials,omitzero"`
	// VCSHost is the host of the git input the run declares.
	VCSHost string `json:"vcs_host,omitzero"`
	// S3Inputs name the S3 inputs the run declares.
	S3Inputs []string `json:"s3_inputs,omitzero"`
	// Policy marks a run whose envelope carries a POST policy for its output prefix, under which objects may land
	// whether or not the result lists them.
	Policy bool `json:"policy,omitzero"`
	// TaskStartedBy and TaskEndedBy are the submission's bounds, in whole seconds; the POST policy for its S3 outputs
	// expires operator.UploadGrace after TaskEndedBy.
	TaskStartedBy int64 `json:"task_started_by,omitzero"`
	TaskEndedBy   int64 `json:"task_ended_by,omitzero"`
	// FeedOffset is the tenant feed's committed end before the submission was published. Every record about the run
	// is written after a receiver reads the submission, so it lies at or after this offset; zero reads the whole feed.
	FeedOffset int64 `json:"feed_offset,omitzero"`
	// RedactionKey is the run's redaction key, minted when this host submits the run.
	RedactionKey []byte `json:"redaction_key,omitzero"`
	// Task is the run's resubmission template: its submission without envelope and wrap, which holds no credential.
	Task *wire.Submission `json:"task,omitzero"`
	// Replacement names the run that resubmits this one, recorded before the replacement is built.
	Replacement string `json:"replacement,omitzero"`
	// ReplacementNonce is the replacement's run nonce.
	ReplacementNonce string `json:"replacement_nonce,omitzero"`
	// Signed is a resubmission's exact signed submission, stored before it is produced, and Produced records that the
	// broker acknowledged it.
	Signed   []byte `json:"signed,omitzero"`
	Produced bool   `json:"produced,omitzero"`
	// CancelRequestedMS is when this host first published a cancel of the run.
	CancelRequestedMS int64 `json:"cancel_requested_at_ms,omitzero"`
	// Bundle is the run's changes.bundle as result last collected and verified it.
	Bundle *collectedBundle `json:"bundle,omitzero"`
	// TraceParent is the submit span's context as a traceparent header, which a cancel is parented to and a
	// resubmission links to; empty when tracing was off.
	TraceParent string `json:"trace_parent,omitzero"`
}

// feedOffset is where the tenant feed's records about the run begin, zero without a local record of it.
func (r *runRecord) feedOffset() int64 {
	if r == nil {
		return 0
	}
	return r.FeedOffset
}

func (r *runRecord) traceParent() string {
	if r == nil {
		return ""
	}
	return r.TraceParent
}

func (h *Host) runPath(runID string) (string, bool) {
	if !filepath.IsLocal(runID) || filepath.Base(runID) != runID {
		return "", false
	}
	return filepath.Join(h.stateDir, "runs", runID+".json"), true
}

// outputDir is the default directory of a run's outputs.
func (h *Host) outputDir(runID string) string {
	return filepath.Join(h.stateDir, "runs", runID, "outputs")
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

// fsync is a variable so tests can fail a sync.
var fsync = (*os.File).Sync

func writeState(path string, value any) error {
	encoded, err := json.Marshal(value, json.Deterministic(true))
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	// A reader sees the old file or the new one, never a partial write, and a crash keeps the file it reported written.
	temp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	if _, err := temp.Write(encoded); err != nil {
		temp.Close()
		return err
	}
	if err := fsync(temp); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(temp.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return fsync(dir)
}

func (h *Host) produce(ctx context.Context, records ...*kgo.Record) error {
	if err := h.kafka.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("produce: %w", err)
	}
	return nil
}
