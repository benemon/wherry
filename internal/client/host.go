package client

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Errors the client returns for a run or placement that cannot proceed.
var (
	ErrNoMatchingReceiver      = errors.New("no matching live receiver")
	ErrRunNotFound             = errors.New("run not found")
	ErrNotAwaitingContinuation = errors.New("task is not awaiting continuation")
	ErrRunCanceled             = errors.New("task is canceled")
	ErrTurnSubmitted           = errors.New("continuation turn already submitted")
	// ErrInvalidInput wraps a submission the wire format rejects; its message names the rule, never the value.
	ErrInvalidInput = errors.New("invalid input")
)

// liveness is how old an advertisement may be for its receiver to count as live.
const liveness = 120 * time.Second

// SubmitInput contains the root-task routing and prompt fields; Parts follow the prompt's text part.
type SubmitInput struct {
	Selector      []wire.Requirement
	Prompt        string
	Parts         []wire.Part
	Deadline      time.Duration
	QueueDeadline time.Duration
}

// SubmitResult describes a produced root task.
type SubmitResult struct {
	RunID   string
	Pool    string
	Skipped int
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

// Receiver describes a receiver of a pool the tenant is entitled to, its latest advertisement and this host's seals to it.
type Receiver struct {
	ReceiverID   string       `json:"receiver_id"`
	Pool         string       `json:"pool"`
	Offers       []wire.Offer `json:"offers"`
	Capacity     int64        `json:"capacity"`
	InFlight     int64        `json:"in_flight"`
	HeartbeatAge string       `json:"heartbeat_age"`
	Status       string       `json:"status"`
	Ended        bool         `json:"ended"`
	Seals        []Seal       `json:"seals"`
}

// Seal is this host's latest seal of one credential to a receiver.
type Seal struct {
	Name  string `json:"name"`
	Pool  string `json:"pool"`
	ExpMS int64  `json:"exp_ms"`
}

// Host implements the operations of a tenant's host principal.
type Host struct {
	kafka       *kgo.Client
	key         ed25519.PrivateKey
	tenant      string
	host        string
	stateDir    string
	credentials map[string]string
}

// NewHost constructs a host over a plain Kafka connection authenticated as principal.
func NewHost(kafka *kgo.Client, key ed25519.PrivateKey, principal, stateDir string, credentials map[string]string) (*Host, error) {
	names, prefixed := strings.CutPrefix(principal, "wherry.tnt.")
	tenant, host, split := strings.Cut(names, ".")
	if !prefixed || !split || !admin.ValidName(tenant) || !admin.ValidName(host) {
		return nil, errors.New("principal is not a host principal wherry.tnt.<tenant>.<host>")
	}
	return &Host{kafka: kafka, key: key, tenant: tenant, host: host, stateDir: stateDir, credentials: credentials}, nil
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
		record, err := signed.RegisterKey(owner, h.key, nil)
		if err != nil {
			return "", err
		}
		if err := h.produce(ctx, &kgo.Record{Topic: topic, Key: []byte(owner), Value: record}); err != nil {
			return "", err
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
	for _, part := range input.Parts {
		if part.MediaType != wire.GitMediaType {
			continue
		}
		// ParseGitPart never quotes the URL in its errors, so its text is safe to return.
		if _, err := wire.ParseGitPart(part); err != nil {
			return SubmitResult{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
		}
	}
	v, err := h.load(ctx)
	if err != nil {
		return SubmitResult{}, err
	}
	now := time.Now()
	ads, err := admin.ReadAdvertisements(ctx, h.kafka, v.state, v.keys, now, liveness)
	if err != nil {
		return SubmitResult{}, err
	}
	pool, placement, ok := resolvePlacement(v.state, h.tenant, input.Selector, ads)
	if !ok {
		return SubmitResult{}, ErrNoMatchingReceiver
	}
	runID, err := randomID("run-")
	if err != nil {
		return SubmitResult{}, err
	}
	submission, err := h.submission(runID, 1, pool, placement, append([]wire.Part{{Text: input.Prompt}}, input.Parts...), input.Deadline, input.QueueDeadline, now)
	if err != nil {
		return SubmitResult{}, err
	}
	if err := h.publish(ctx, v, ads, submission, now); err != nil {
		return SubmitResult{}, err
	}
	return SubmitResult{RunID: runID, Pool: pool}, nil
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
	now := time.Now()
	ads, err := admin.ReadAdvertisements(ctx, h.kafka, r.v.state, r.v.keys, now, liveness)
	if err != nil {
		return ContinueResult{}, err
	}
	submission, err := h.submission(runID, next, echo.Pool, echo.Placement, []wire.Part{{Text: input.Prompt}}, input.Deadline, input.QueueDeadline, now)
	if err != nil {
		return ContinueResult{}, err
	}
	if err := h.publish(ctx, r.v, ads, submission, now); err != nil {
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
	messageID, err := randomID("msg-")
	if err != nil {
		return nil, err
	}
	return &wire.Submission{
		Message: wire.Message{MessageID: messageID, ContextID: runID, TaskID: runID, Role: wire.RoleUser, Parts: parts, Extensions: []string{wire.ExtensionURI}},
		Wherry: wire.SubmissionBinding{
			Tenant: h.tenant, Host: h.host, Pool: pool, Run: runID, Turn: turn, Placement: placement,
			DeadlineMS: now.Add(deadline).UnixMilli(), QueueDeadlineMS: now.Add(queueDeadline).UnixMilli(),
		},
	}, nil
}

// publish seals the placed agent's credentials to the pool, writes the task to the pool's task topic and records the
// run locally.
func (h *Host) publish(ctx context.Context, v view, ads map[string]wire.Advertisement, submission *wire.Submission, now time.Time) error {
	encoded, err := wire.SignRecord(submission, h.key)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	binding := submission.Wherry
	if err := h.seal(ctx, v, ads, binding.Pool, binding.Placement.Agent, time.UnixMilli(binding.QueueDeadlineMS), now); err != nil {
		return err
	}
	if err := h.produce(ctx, &kgo.Record{Topic: admin.PoolTasksTopic(binding.Pool), Key: []byte(binding.Run), Value: encoded}); err != nil {
		return err
	}
	sum := sha256.Sum256(encoded)
	return h.writeRun(runRecord{Run: binding.Run, Pool: binding.Pool, Turn: binding.Turn, Placement: binding.Placement, TaskHash: hex.EncodeToString(sum[:])})
}

type sealKey struct{ receiver, pool, credential string }

// seal seals each configured credential of agent to every live member of pool that lacks a seal under its current
// sealing key lasting until until. A sealing key that differs from the one pinned for its receiver stops the seal.
func (h *Host) seal(ctx context.Context, v view, ads map[string]wire.Advertisement, pool, agent string, until, now time.Time) error {
	terms := v.state.Pools[pool]
	index := slices.IndexFunc(terms.Agents, func(terms admin.AgentTerms) bool { return terms.Name == agent })
	if index < 0 {
		return fmt.Errorf("agent %s is outside the terms of pool %s", agent, pool)
	}
	var credentials []string
	for _, credential := range terms.Agents[index].Credentials {
		if _, ok := h.credentials[credential]; ok {
			credentials = append(credentials, credential)
		}
	}
	if len(credentials) == 0 {
		return nil
	}
	existing, err := h.seals(ctx)
	if err != nil {
		return err
	}
	pins, err := h.readPins()
	if err != nil {
		return err
	}
	plaintexts := map[string][]byte{}
	var records []*kgo.Record
	for _, receiver := range slices.Sorted(maps.Keys(ads)) {
		sealing := v.keys[receiver].Sealing
		if ads[receiver].Pool != pool || sealing == nil {
			continue
		}
		if err := signed.CheckPinnedKey(pins, receiver, sealing); err != nil {
			return fmt.Errorf("receiver %s: %w", receiver, err)
		}
		for _, credential := range credentials {
			current := existing[sealKey{receiver, pool, credential}]
			if current.KeyFingerprint == signed.SealingKeyID(sealing) && current.ExpiryMS > until.UnixMilli() {
				continue
			}
			plaintext, ok := plaintexts[credential]
			if !ok {
				if plaintext, err = os.ReadFile(h.credentials[credential]); err != nil {
					return fmt.Errorf("read credential %s: %w", credential, err)
				}
				plaintexts[credential] = plaintext
			}
			seal, err := signed.SealCredential(h.tenant, receiver, pool, credential, now, now.Add(time.Duration(terms.SealLifetimeMS)*time.Millisecond), sealing, plaintext)
			if err != nil {
				return fmt.Errorf("seal credential %s: %w", credential, err)
			}
			encoded, err := signed.EncodeSeal(seal)
			if err != nil {
				return err
			}
			records = append(records, &kgo.Record{Topic: admin.HostTopic(h.tenant, h.host), Key: []byte(receiver + "/" + credential), Value: encoded})
		}
	}
	if err := h.writePins(pins); err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}
	return h.produce(ctx, records...)
}

// seals returns this host's latest seal, by issued time, per receiver, pool and credential.
func (h *Host) seals(ctx context.Context) (map[sealKey]signed.Seal, error) {
	records, err := admin.ReadTopic(ctx, h.kafka, admin.HostTopic(h.tenant, h.host))
	if err != nil {
		return nil, err
	}
	latest := map[sealKey]signed.Seal{}
	for _, record := range records {
		seal, err := signed.DecodeSeal(record.Value)
		if err != nil || seal.Tenant != h.tenant {
			continue
		}
		key := sealKey{seal.Receiver, seal.Pool, seal.Credential}
		if seal.IssuedMS >= latest[key].IssuedMS {
			latest[key] = seal
		}
	}
	return latest, nil
}

// Receivers returns the receivers of the pools the tenant is entitled to, with their latest advertisement and this
// host's seals to them.
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
	seals, err := h.seals(ctx)
	if err != nil {
		return nil, err
	}
	result := []Receiver{}
	for id, config := range v.state.Receivers {
		if !v.state.Entitlements[h.tenant][config.Pool] {
			continue
		}
		receiver := Receiver{
			ReceiverID: id, Pool: config.Pool, Offers: []wire.Offer{}, HeartbeatAge: "-", Status: "silent",
			Ended: v.state.Ended[admin.ReceiverPrincipal(id)], Seals: []Seal{},
		}
		if ad, ok := ads[id]; receiver.Ended {
			receiver.Status = "ended"
		} else if ok {
			age := now.Sub(time.UnixMilli(ad.AtMS))
			receiver.Offers, receiver.Capacity, receiver.InFlight = ad.Offers, ad.Capacity, ad.InFlight
			receiver.HeartbeatAge = age.Round(time.Second).String()
			receiver.Status = "stale"
			if age <= liveness {
				receiver.Status = "live"
			}
		}
		for key, seal := range seals {
			if key.receiver == id {
				receiver.Seals = append(receiver.Seals, Seal{Name: seal.Credential, Pool: seal.Pool, ExpMS: seal.ExpiryMS})
			}
		}
		sort.Slice(receiver.Seals, func(i, j int) bool { return receiver.Seals[i].Name < receiver.Seals[j].Name })
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
}

func (h *Host) runPath(runID string) (string, bool) {
	if !filepath.IsLocal(runID) || filepath.Base(runID) != runID {
		return "", false
	}
	return filepath.Join(h.stateDir, "runs", runID+".json"), true
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
	path, _ := h.runPath(record.Run) // run IDs from randomID are local names
	return writeState(path, record)
}

func (h *Host) readPins() (map[string]string, error) {
	pins := map[string]string{}
	if err := readState(filepath.Join(h.stateDir, "pins.json"), &pins); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return pins, nil
}

func (h *Host) writePins(pins map[string]string) error {
	return writeState(filepath.Join(h.stateDir, "pins.json"), pins)
}

func readState(path string, value any) error {
	encoded, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(encoded, value, json.RejectUnknownMembers(true), jsontext.AllowDuplicateNames(false)); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
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
	return os.WriteFile(path, encoded, 0o600)
}

func (h *Host) produce(ctx context.Context, records ...*kgo.Record) error {
	if err := h.kafka.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("produce: %w", err)
	}
	return nil
}

func randomID(prefix string) (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(bytes), nil
}
