package client

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
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

	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/minio/minio-go/v7"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
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
	// Credentials name credentials this host configures, each delivered to the agent at its target.
	Credentials []wire.TaskCredential
	// Outputs are the files the agent is asked to leave.
	Outputs []wire.ExpectedOutput
	// Git is the repository the agent clones; its credential and host key come from the host's git configuration.
	Git *wire.GitInput
	// GitChanges, patch or bundle, asks for the clone's changes back as that output.
	GitChanges string
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
}

// Credential is one of an agent's credentials: its target, the agent's environment variable Env or credentials-directory
// file File; whether a run goes without it when its host has no source for it; and whether this host has one.
type Credential struct {
	Name       string `json:"name"`
	Env        string `json:"env,omitzero"`
	File       string `json:"file,omitzero"`
	Optional   bool   `json:"optional,omitzero"`
	Configured bool   `json:"configured"`
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
	git         []GitCredential
	// storage holds the host's S3 credentials by pool; a pool without an entry has no storage on this host.
	storage map[string]StorageConfig
	logger  *log.Logger
	// follower is the host's reader of the tenant feed, kept while its context lasts.
	followMu sync.Mutex
	follower *follower
}

// NewHost constructs a host over a plain Kafka connection authenticated as principal.
func NewHost(kafka *kgo.Client, key ed25519.PrivateKey, principal, stateDir string, credentials map[string]credential.Source, git []GitCredential, logger *log.Logger) (*Host, error) {
	// TenantPrincipal("", "") is the prefix and the separator between tenant and host.
	names, prefixed := strings.CutPrefix(principal, strings.TrimSuffix(operator.TenantPrincipal("", ""), "."))
	tenant, host, split := strings.Cut(names, ".")
	if !prefixed || !split || !wire.ValidName(tenant) || !wire.ValidName(host) {
		return nil, errors.New("principal is not a host principal wherry.tnt.<tenant>.<host>")
	}
	return &Host{kafka: kafka, reader: operator.NewReader(kafka), key: key, tenant: tenant, host: host, stateDir: stateDir, credentials: credentials, git: git, logger: logger}, nil
}

// Close closes the Kafka connections.
func (h *Host) Close() {
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
// that receiver's pool.
func (h *Host) Submit(ctx context.Context, input SubmitInput) (SubmitResult, error) {
	h.resubmitLost(ctx)
	// Checked before placement: an invalid key matches no offer and would read as no matching receiver.
	if err := wire.ValidateRequirements(input.Selector); err != nil {
		return SubmitResult{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	outputs := input.Outputs
	if input.Git != nil {
		git := *input.Git
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
			return SubmitResult{}, fmt.Errorf("%w: auth required: credential %s is not in this host's configuration", ErrInvalidInput, credential.Name)
		}
	}
	// A receiver rejects a task_ended_by past task_started_by plus the run cap.
	if limit := input.StartBy.Add(time.Duration(operator.MaxRunDurationMS) * time.Millisecond); input.EndBy.After(limit) {
		return SubmitResult{}, fmt.Errorf("%w: task_ended_by (%s) is past task_started_by plus the 6h run cap (%s)", ErrInvalidInput, wire.RFC3339(input.EndBy.Unix()), wire.RFC3339(limit.Unix()))
	}
	stored := len(input.S3Inputs)+len(s3OutputNames(outputs)) > 0
	// Connect folds the key topic only when <host_key>.next exists, and receivers refuse a task under any key but the
	// current one without writing a status.
	owner := signed.HostOwner(h.tenant, h.host)
	var keys signed.Keys
	var v view
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() (err error) {
		keys, err = h.reader.ReadKeys(groupCtx, map[string]string{owner: operator.HostTopic(h.tenant, h.host)})
		return err
	})
	group.Go(func() (err error) {
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
	declared, err := h.reader.ReadAdvertisements(ctx, v.state, v.keys, now, math.MaxInt64)
	if err != nil {
		return SubmitResult{}, err
	}
	ads := maps.Clone(declared)
	maps.DeleteFunc(ads, func(_ string, ad wire.Advertisement) bool {
		age := now.Sub(time.UnixMilli(ad.AtMS))
		return age > liveness || age < -liveness
	})
	pool, placement, err := resolvePlacement(v.state, h.tenant, input.Agent, input.Model, input.Selector, ads)
	if err != nil {
		return SubmitResult{}, err
	}
	// Before any credential resolves or input uploads.
	poolKey, err := h.poolKey(ctx, v, pool)
	if err != nil {
		return SubmitResult{}, err
	}
	agents := v.state.Pools[pool].Agents
	agent := agents[slices.IndexFunc(agents, func(agent operator.AgentTerms) bool { return agent.Name == placement.Agent })]
	var unsupplied []string
	for _, credential := range agent.Credentials {
		if _, configured := h.credentials[credential.Name]; configured {
			continue
		}
		if !credential.Optional() {
			return SubmitResult{}, fmt.Errorf("%w: auth required: credential %s is not in this host's configuration", ErrInvalidInput, credential.Name)
		}
		unsupplied = append(unsupplied, credential.Name)
	}
	for _, credential := range input.Credentials {
		if slices.ContainsFunc(agent.Targets(), func(target wire.TaskCredential) bool { return target.Target() == credential.Target() }) {
			return SubmitResult{}, fmt.Errorf("%w: task credential %s takes the %s of one of agent %s's credentials", ErrInvalidInput, credential.Name, credential.Target(), agent.Name)
		}
	}
	if err := wire.ValidateInputs(input.Parts, math.MaxInt64); err != nil {
		return SubmitResult{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	_, credentialed := h.storage[pool]
	storable := v.state.Pools[pool].Storage != nil && credentialed
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
	if limit, limiting := profileInlineLimit(v.state, pool, placement, declared); limit > 0 {
		if err := wire.ValidateInputs(input.Parts, limit); err != nil {
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
	submission, err := h.submission(runID, nonce, pool, placement, append([]wire.Part{{Text: input.Prompt}}, input.Parts...), input.StartBy, input.EndBy)
	if err != nil {
		return SubmitResult{}, err
	}
	submission.Wherry.Credentials, submission.Wherry.Unsupplied, submission.Wherry.Outputs, submission.Wherry.Git = input.Credentials, unsupplied, outputs, input.Git
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
	var store *minio.Client
	if stored {
		if store, err = h.bucket(ctx, pool, storage); err != nil {
			return SubmitResult{}, err
		}
		submission.Wherry.S3Inputs, err = uploadInputs(ctx, store, storage.Bucket, h.tenant, runID, input.S3Inputs)
	}
	redactionKey := make([]byte, 32)
	if err == nil {
		_, err = rand.Read(redactionKey)
	}
	if err == nil {
		encoded, err = h.enveloped(ctx, submission, v.state.Pools[pool], poolKey, store, redactionKey)
	}
	if limit := v.state.Pools[pool].RecordLimit; err == nil && int64(len(encoded)) > limit {
		err = fmt.Errorf("%w: with its envelope the submission is a %d-byte record, over pool %s's %d-byte record limit", ErrInvalidInput, len(encoded), pool, limit)
	}
	if err == nil {
		err = h.publish(ctx, submission, encoded, now, redactionKey)
	}
	if err != nil {
		if stored {
			if cleanupErr := h.removePrefix(ctx, store, storage.Bucket, wire.S3Prefix(h.tenant, runID)); cleanupErr != nil {
				h.logger.Printf("run %s: delete the S3 inputs of a task not published: %v", runID, cleanupErr)
			}
		}
		return SubmitResult{}, err
	}
	return SubmitResult{RunID: runID, Pool: pool}, nil
}

// poolKey returns the current key of pool: that of the latest committed record under the snapshot key on the pool's
// snapshot topic, when its writer is the custodian it names and that custodian is unrevoked. An earlier record never
// stands in. A suspended pool has none.
func (h *Host) poolKey(ctx context.Context, v view, pool string) (*ecdsa.PublicKey, error) {
	if v.state.Suspended[pool] {
		return nil, fmt.Errorf("pool %s is suspended; submit again once it is resumed", pool)
	}
	records, err := h.reader.ReadFrom(ctx, operator.CustodianTopic(pool), 0)
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
// key wrapped to poolKey. Credentials resolve here, a command source's child with the task's task_ended_by; GET links
// last until task_started_by plus getLifetime and PUT links until task_ended_by.
func (h *Host) enveloped(ctx context.Context, submission *wire.Submission, terms operator.Terms, poolKey *ecdsa.PublicKey, store *minio.Client, redactionKey []byte) ([]byte, error) {
	binding := submission.Wherry
	values := signed.EnvelopeValues{Binding: binding.EnvelopeBinding(), Credentials: map[string][]byte{}, S3Inputs: map[string]string{}, S3Outputs: map[string]string{}, RedactionKey: redactionKey}
	var names []string
	if agent := slices.IndexFunc(terms.Agents, func(agent operator.AgentTerms) bool { return agent.Name == binding.Placement.Agent }); agent >= 0 {
		for _, credential := range terms.Agents[agent].Credentials {
			if !slices.Contains(binding.Unsupplied, credential.Name) {
				names = append(names, credential.Name)
			}
		}
	}
	for _, credential := range binding.Credentials {
		names = append(names, credential.Name)
	}
	for _, name := range names {
		if _, resolved := values.Credentials[name]; resolved {
			continue
		}
		source, configured := h.credentials[name]
		if !configured {
			return nil, fmt.Errorf("%w: auth required: credential %s is not in this host's configuration", ErrInvalidInput, name)
		}
		value, err := source.ResolveForTask(ctx, wire.RFC3339(binding.TaskEndedBy))
		if err != nil {
			return nil, fmt.Errorf("resolve credential %s: %w", name, err)
		}
		hold(name, value)
		values.Credentials[name] = value
	}
	if store != nil {
		var inputs []string
		for _, input := range binding.S3Inputs {
			inputs = append(inputs, input.Name)
		}
		outputs := s3OutputNames(binding.Outputs)
		urls, err := presign(ctx, store, terms.Storage.Bucket, h.tenant, binding.InputRun(), binding.Run, inputs, outputs,
			time.Unix(binding.TaskStartedBy, 0).Add(getLifetime), time.Unix(binding.TaskEndedBy, 0))
		if err != nil {
			return nil, err
		}
		for _, input := range inputs {
			values.S3Inputs[input] = string(urls[signed.S3InputValue(input)])
		}
		for _, output := range outputs {
			values.S3Outputs[output] = string(urls[signed.S3OutputValue(output)])
		}
	}
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

// s3OutputNames names the outputs returned through the pool's bucket.
func s3OutputNames(outputs []wire.ExpectedOutput) []string {
	var names []string
	for _, output := range outputs {
		if output.Storage == wire.StorageS3 {
			names = append(names, output.Name)
		}
	}
	return names
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

// Cancel publishes a cancel of a run to the events topic of its pool.
func (h *Host) Cancel(ctx context.Context, runID string) (CancelResult, error) {
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
	if err := h.produce(ctx, &kgo.Record{Topic: operator.PoolEventsTopic(r.pool), Key: []byte(runID), Value: encoded}); err != nil {
		return CancelResult{}, err
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

// publish records the run locally, then writes encoded, the signed submission, to its pool's task topic. A failed write
// removes the record.
func (h *Host) publish(ctx context.Context, submission *wire.Submission, encoded []byte, now time.Time, redactionKey []byte) error {
	record, err := h.newRecord(ctx, submission, encoded, now, redactionKey)
	if err != nil {
		return err
	}
	if err := h.writeRun(record); err != nil {
		return err
	}
	if err := h.produceTask(ctx, submission.Wherry, encoded); err != nil {
		if path, ok := h.runPath(record.Run); ok {
			os.Remove(path)
		}
		return err
	}
	return nil
}

// newRecord is the local record of a run whose signed submission is encoded, holding redactionKey, the envelope's, and
// the submission without its envelope as the run's resubmission template.
func (h *Host) newRecord(ctx context.Context, submission *wire.Submission, encoded []byte, now time.Time, redactionKey []byte) (runRecord, error) {
	binding := submission.Wherry
	template := *submission
	template.Wherry.Envelope, template.Wherry.Wrap = "", ""
	record := runRecord{
		Run: binding.Run, Pool: binding.Pool, Placement: binding.Placement, TaskHash: wire.TaskHash(encoded), SubmittedMS: now.UnixMilli(), TaskStartedBy: binding.TaskStartedBy,
		TaskEndedBy: binding.TaskEndedBy, RedactionKey: redactionKey, Task: &template,
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
	record.S3Outputs = s3OutputNames(binding.Outputs)
	// Listed before the write: a receiver that claims at once writes the run's first records after the submission.
	ends, err := operator.ListOffsets(ctx, kadm.NewClient(h.kafka).ListCommittedOffsets, operator.TenantTopic(h.tenant))
	if err != nil {
		return runRecord{}, err
	}
	ends.Each(func(end kadm.ListedOffset) { record.FeedOffset = end.Offset })
	return record, nil
}

// produceTask writes encoded, a signed submission, to its pool's task topic.
func (h *Host) produceTask(ctx context.Context, binding wire.SubmissionBinding, encoded []byte) error {
	err := h.produce(ctx, &kgo.Record{Topic: operator.PoolTasksTopic(binding.Pool), Key: []byte(binding.Run), Value: encoded})
	switch {
	case errors.Is(err, kerr.MessageTooLarge):
		return fmt.Errorf("%w: the submission is a %d-byte record, over the broker's record limit; shorten the prompt or the input files", ErrInvalidInput, len(encoded))
	}
	return err
}

// Receivers returns the receivers of the pools the tenant is entitled to, with their latest advertisement. An
// advertisement dated beyond the liveness window either side of now is not live.
func (h *Host) Receivers(ctx context.Context) ([]Receiver, error) {
	v, err := h.load(ctx)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	ads, err := h.reader.ReadAdvertisements(ctx, v.state, v.keys, now, math.MaxInt64)
	if err != nil {
		return nil, err
	}
	result := []Receiver{}
	for id, config := range v.state.Receivers {
		if !v.state.Entitlements[h.tenant][config.Pool] {
			continue
		}
		receiver := Receiver{
			ReceiverID: id, Pool: config.Pool, Profiles: []Profile{}, Status: "silent",
		}
		ended := v.state.Ended[operator.ReceiverPrincipal(id)]
		ad, advertised := ads[id]
		if ended {
			receiver.Status = "ended"
		} else if advertised {
			age := now.Sub(time.UnixMilli(ad.AtMS))
			receiver.Capacity, receiver.InFlight = ad.Capacity, ad.InFlight
			receiver.LastAdvertised = time.UnixMilli(ad.AtMS).UTC()
			receiver.Status = "stale"
			if age <= liveness && age >= -liveness {
				receiver.Status = "live"
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
			agents := v.state.Pools[config.Pool].Agents
			if index := slices.IndexFunc(agents, func(agent operator.AgentTerms) bool { return agent.Name == profile.Agent }); index >= 0 {
				listed.Models, listed.DefaultModel = agents[index].Models, agents[index].DefaultModel
				for _, credential := range agents[index].Credentials {
					_, configured := h.credentials[credential.Name]
					listed.Credentials = append(listed.Credentials, Credential{Name: credential.Name, Env: credential.Env, File: credential.File, Optional: credential.Optional(), Configured: configured})
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
	Run       string         `json:"run"`
	Pool      string         `json:"pool"`
	Placement wire.Placement `json:"placement"`
	TaskHash  string         `json:"task_hash"`
	// SubmittedMS is when this host published the run.
	SubmittedMS int64 `json:"submitted_at_ms,omitzero"`
	// TaskCredentials names the task credentials the run declares.
	TaskCredentials []string `json:"task_credentials,omitzero"`
	// VCSHost is the host of the git input the run declares.
	VCSHost string `json:"vcs_host,omitzero"`
	// S3Inputs and S3Outputs name the S3 inputs and outputs the run declares.
	S3Inputs  []string `json:"s3_inputs,omitzero"`
	S3Outputs []string `json:"s3_outputs,omitzero"`
	// TaskStartedBy and TaskEndedBy are the submission's bounds, in whole seconds; a PUT URL for its S3 outputs expires
	// at TaskEndedBy.
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
}

// feedOffset is where the tenant feed's records about the run begin, zero without a local record of it.
func (r *runRecord) feedOffset() int64 {
	if r == nil {
		return 0
	}
	return r.FeedOffset
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
