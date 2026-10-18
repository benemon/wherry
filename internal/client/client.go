package client

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"sort"
	"time"

	"github.com/benemon/wherry/internal/trust"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Errors the client returns for a run or placement that cannot proceed.
var (
	ErrNoMatchingReceiver      = errors.New("no matching live receiver")
	ErrRunNotFound             = errors.New("verified root submission not found")
	ErrNotAwaitingContinuation = errors.New("task is not awaiting continuation")
	ErrRunCanceled             = errors.New("task is canceled")
	ErrTurnSubmitted           = errors.New("continuation turn already submitted")
)

// renewalLifetime is how far a renewal extends an approval from the time it is signed.
const renewalLifetime = 30 * 24 * time.Hour

// taskGrace is how long Await waits after a terminal status for the task record that carries the artifacts.
const taskGrace = 2 * time.Second

// Client implements the user-side wherry operations.
type Client struct {
	kafka      *kgo.Client
	userKey    ed25519.PrivateKey
	replyTopic string
}

// Enrolment is one verified enrolment request and its approval status.
type Enrolment struct {
	ReceiverID  string
	RequestHash string
	Labels      map[string]string
	Adapters    []wire.DetectedAdapter
	Approved    bool
	Generation  int64
	// RenewalRequested reports an unanswered renew request for this approved, current generation.
	RenewalRequested bool
}

// ApprovalResult describes a produced enrolment approval.
type ApprovalResult struct {
	ReceiverID string
	Generation int64
	Skipped    int
}

// RevokeResult describes a produced revocation.
type RevokeResult struct {
	ReceiverID string
	Floor      int64
	Skipped    int
}

// SecretResult describes a produced sealed-secret version.
type SecretResult struct {
	Name       string
	ReceiverID string
	Version    int64
	Waiting    []WaitingTask
	Skipped    int
}

// WaitingTask identifies a run interrupted for a credential.
type WaitingTask struct {
	RunID string
	Turn  int64
}

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

// AwaitResult is the accepted outcome of the latest turn; Task is nil when a status record ended it.
type AwaitResult struct {
	Task   *wire.Task
	Status wire.TaskStatus
	// RevokedAt is the revocation time of the receiver that signed the outcome, zero while it is enrolled.
	RevokedAt time.Time
	Skipped   int
}

// StatusResult is the latest verified status of a run's latest turn.
type StatusResult struct {
	Turn   int64
	Status wire.TaskStatus
	// RevokedAt is the revocation time of the receiver that signed the status, zero while it is enrolled.
	RevokedAt time.Time
	Skipped   int
}

// TurnAttempt identifies one delivery attempt within one submission turn.
type TurnAttempt struct {
	Turn    int64
	Attempt int64
}

// EventsResult contains a run's verified status records, whether the user cancelled it, and its
// transcript chunk count per turn and attempt.
type EventsResult struct {
	Statuses  []EventStatus
	Cancelled bool
	Chunks    map[TurnAttempt]int
	// ChunksRevokedAt holds, per turn and attempt, the revocation time of a revoked receiver that signed its chunks.
	ChunksRevokedAt map[TurnAttempt]time.Time
	Skipped         int
}

// EventStatus is one verified status record and the revocation time of its signer, zero while it is enrolled.
type EventStatus struct {
	wire.StatusRecord
	RevokedAt time.Time
}

// Receiver describes one enrolled receiver and its latest verified capability.
type Receiver struct {
	ReceiverID   string              `json:"receiver_id"`
	Pool         string              `json:"pool"`
	Generation   int64               `json:"generation"`
	Labels       map[string]string   `json:"labels"`
	Adapters     []wire.AdapterGrant `json:"adapters"`
	Offers       []wire.Offer        `json:"offers"`
	Capacity     int64               `json:"capacity"`
	InFlight     int64               `json:"in_flight"`
	HeartbeatAge string              `json:"heartbeat_age"`
	Status       string              `json:"status"`
	Revoked      bool                `json:"revoked"`
	Floor        int64               `json:"floor_generation"`
	Secrets      []Seal              `json:"secrets"`
}

// Seal is the current sealed version of one credential under a receiver's current approval.
type Seal struct {
	Name    string `json:"name"`
	Version int64  `json:"version"`
	ExpMS   int64  `json:"exp_ms"`
}

type requestRecord struct {
	envelope []byte
	request  *wire.EnrolRequest
}

type approvalRecord struct {
	generation   int64
	envelopeHash string
	pool         string
}

type secretKey struct {
	receiverID string
	name       string
}

type loadedState struct {
	state            *trust.State
	requests         map[string]requestRecord
	approvals        map[string]approvalRecord
	approvedRequests map[string]int64
	secretVersions   map[secretKey]wire.SecretVersion
	revocations      map[string][]revocationRecord
	skipped          int
}

type revocationRecord struct {
	floor         int64
	timestampType int8
	timestamp     time.Time
}

type receiverIdentity struct {
	id       string
	enrolled trust.Enrolled
}

// revokedSigner is a revoked receiver's last approved identity, trusted only for records the broker appended before revokedAt.
type revokedSigner struct {
	identity  receiverIdentity
	revokedAt time.Time
}

type verifiedCapability struct {
	receiverID string
	enrolled   trust.Enrolled
	capability *wire.Capability
}

type runSubmission struct {
	record *wire.SubmissionRecord
	hash   string
}

// New constructs a client over an existing direct Kafka connection.
func New(kafka *kgo.Client, userKey ed25519.PrivateKey, replyTopic string) *Client {
	return &Client{kafka: kafka, userKey: userKey, replyTopic: replyTopic}
}

// Close closes the Kafka connection.
func (client *Client) Close() {
	client.kafka.Close()
}

// Enrolments returns every verified request in deterministic order.
func (client *Client) Enrolments(ctx context.Context) ([]Enrolment, int, error) {
	loaded, err := client.loadState(ctx)
	if err != nil {
		return nil, 0, err
	}
	result := make([]Enrolment, 0, len(loaded.requests))
	for hash, record := range loaded.requests {
		id := record.request.ReceiverID
		generation, approved := loaded.approvedRequests[hash]
		_, pending := loaded.state.RenewRequest(id)
		result = append(result, Enrolment{
			ReceiverID: id, RequestHash: hash,
			Labels:   maps.Clone(record.request.Labels),
			Adapters: append([]wire.DetectedAdapter(nil), record.request.Adapters...),
			Approved: approved, Generation: generation,
			RenewalRequested: approved && pending && generation == loaded.approvals[id].generation,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].ReceiverID != result[j].ReceiverID {
			return result[i].ReceiverID < result[j].ReceiverID
		}
		return result[i].RequestHash < result[j].RequestHash
	})
	return result, loaded.skipped, nil
}

// Approve signs and publishes the next approval generation for a request; an empty pool is the receiver ID.
func (client *Client) Approve(ctx context.Context, requestHash, pool string, labels map[string]string, grants []wire.AdapterGrant, renewal string, expires time.Duration) (ApprovalResult, error) {
	loaded, err := client.loadState(ctx)
	if err != nil {
		return ApprovalResult{}, err
	}
	request, ok := loaded.requests[requestHash]
	if !ok {
		return ApprovalResult{}, errors.New("enrolment request not found")
	}
	for _, grant := range grants {
		if !slices.ContainsFunc(request.request.Adapters, func(detected wire.DetectedAdapter) bool {
			return detected.Name == grant.Name && detected.Platform == grant.Platform
		}) {
			return ApprovalResult{}, fmt.Errorf("grant %q does not name a detected adapter with platform %q", grant.Name, grant.Platform)
		}
	}
	if pool == "" {
		pool = request.request.ReceiverID
	}
	current := loaded.approvals[request.request.ReceiverID]
	generation := max(current.generation, loaded.state.Floor(request.request.ReceiverID)) + 1
	envelope, err := trust.Approve(
		request.envelope, request.request, client.userKey, labels, pool, grants, renewal,
		generation, current.envelopeHash, time.Now().Add(expires),
	)
	if err != nil {
		return ApprovalResult{}, err
	}
	if err := client.produce(ctx, &kgo.Record{
		Topic: wire.TopicState, Key: []byte(request.request.ReceiverID), Value: envelope,
	}); err != nil {
		return ApprovalResult{}, err
	}
	return ApprovalResult{ReceiverID: request.request.ReceiverID, Generation: generation, Skipped: loaded.skipped}, nil
}

// Renew publishes a renewal of a receiver's current approval.
func (client *Client) Renew(ctx context.Context, receiverID string) (ApprovalResult, error) {
	loaded, err := client.loadState(ctx)
	if err != nil {
		return ApprovalResult{}, err
	}
	generation, err := client.renew(ctx, loaded, receiverID, time.Now())
	if err != nil {
		return ApprovalResult{}, err
	}
	return ApprovalResult{ReceiverID: receiverID, Generation: generation, Skipped: loaded.skipped}, nil
}

// renew publishes a renewal of a receiver's current approval and applies it to loaded.
func (client *Client) renew(ctx context.Context, loaded *loadedState, receiverID string, now time.Time) (int64, error) {
	current, hash, ok := loaded.state.Approval(receiverID)
	if !ok {
		return 0, fmt.Errorf("receiver %s has never been approved", receiverID)
	}
	if current.Generation <= loaded.state.Floor(receiverID) {
		return 0, trust.ErrRevoked
	}
	envelope, err := trust.Renew(current, hash, client.userKey, now.Add(renewalLifetime))
	if err != nil {
		return 0, err
	}
	if _, err := loaded.state.Apply(envelope); err != nil {
		return 0, err
	}
	if err := client.produce(ctx, &kgo.Record{Topic: wire.TopicState, Key: []byte(receiverID), Value: envelope}); err != nil {
		return 0, err
	}
	loaded.approvals[receiverID] = approvalRecord{generation: current.Generation, envelopeHash: trust.EnvelopeHash(envelope), pool: current.Pool}
	return current.Generation, nil
}

// Revoke publishes a revocation floor at the receiver's highest approved generation.
func (client *Client) Revoke(ctx context.Context, receiverID string) (RevokeResult, error) {
	loaded, err := client.loadState(ctx)
	if err != nil {
		return RevokeResult{}, err
	}
	current, ok := loaded.approvals[receiverID]
	if !ok {
		return RevokeResult{}, fmt.Errorf("receiver %s has never been approved", receiverID)
	}
	envelope, err := trust.NewRevocation(receiverID, current.generation, client.userKey, time.Now())
	if err != nil {
		return RevokeResult{}, err
	}
	if err := client.produce(ctx, &kgo.Record{Topic: wire.TopicState, Key: []byte(receiverID), Value: envelope}); err != nil {
		return RevokeResult{}, err
	}
	return RevokeResult{ReceiverID: receiverID, Floor: current.generation, Skipped: loaded.skipped}, nil
}

// PutSecret seals a file to an enrolled receiver and publishes its next version.
func (client *Client) PutSecret(ctx context.Context, name, receiverID, path string, expires time.Duration) (SecretResult, error) {
	loaded, err := client.loadState(ctx)
	if err != nil {
		return SecretResult{}, err
	}
	now := time.Now()
	enrolled, err := loaded.state.Receiver(receiverID, now)
	if err != nil {
		return SecretResult{}, err
	}
	value, err := os.ReadFile(path)
	if err != nil {
		return SecretResult{}, err
	}
	version := loaded.secretVersions[secretKey{receiverID: receiverID, name: name}].Version + 1
	versionEnvelope, sealedEnvelope, err := trust.Seal(enrolled, receiverID, name, version, value, now.Add(expires), client.userKey)
	if err != nil {
		return SecretResult{}, err
	}
	if err := client.produce(ctx,
		&kgo.Record{Topic: wire.TopicState, Key: []byte(receiverID), Value: versionEnvelope},
		&kgo.Record{Topic: wire.TopicSecret, Key: []byte(receiverID + "/" + name), Value: sealedEnvelope},
	); err != nil {
		return SecretResult{}, err
	}
	waiting, skipped, err := client.waitingTasks(ctx, loaded, enrolled.Pool, name)
	if err != nil {
		return SecretResult{}, err
	}
	return SecretResult{Name: name, ReceiverID: receiverID, Version: version, Waiting: waiting, Skipped: loaded.skipped + skipped}, nil
}

// Receivers returns every enrolled or revoked receiver with its latest verified capability status.
func (client *Client) Receivers(ctx context.Context) ([]Receiver, int, error) {
	loaded, err := client.loadState(ctx)
	if err != nil {
		return nil, 0, err
	}
	now := time.Now()
	receivers := loaded.receiverSigners(now)
	capabilities, skipped, err := client.loadCapabilities(ctx, receivers)
	if err != nil {
		return nil, 0, err
	}
	byReceiver := make(map[string]*wire.Capability, len(capabilities))
	for _, capability := range capabilities {
		byReceiver[capability.receiverID] = capability.capability
	}

	result := make([]Receiver, 0, len(loaded.approvals))
	for _, identity := range receivers {
		receiver := Receiver{
			ReceiverID: identity.id, Pool: identity.enrolled.Pool,
			Generation: identity.enrolled.Generation, Labels: maps.Clone(identity.enrolled.Labels),
			Adapters: identity.enrolled.Adapters, Offers: []wire.Offer{},
			HeartbeatAge: "-", Status: "silent", Floor: loaded.state.Floor(identity.id),
			Secrets: loaded.seals(identity.id, identity.enrolled.ApprovalHash),
		}
		if capability := byReceiver[identity.id]; capability != nil {
			receiver.Offers = capability.Offers
			age := now.Sub(time.UnixMilli(capability.AtMS))
			receiver.Capacity = capability.Capacity
			receiver.InFlight = capability.InFlight
			receiver.HeartbeatAge = age.Round(time.Second).String()
			receiver.Status = "stale"
			if age <= 120*time.Second {
				receiver.Status = "live"
			}
		}
		result = append(result, receiver)
	}
	for id, approval := range loaded.approvals {
		if approval.generation <= loaded.state.Floor(id) {
			result = append(result, Receiver{
				ReceiverID: id, Pool: approval.pool, Generation: approval.generation,
				Labels: map[string]string{}, Adapters: []wire.AdapterGrant{}, Offers: []wire.Offer{},
				HeartbeatAge: "-", Status: "revoked", Revoked: true, Floor: loaded.state.Floor(id), Secrets: []Seal{},
			})
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Pool != result[j].Pool {
			return result[i].Pool < result[j].Pool
		}
		return result[i].ReceiverID < result[j].ReceiverID
	})
	return result, loaded.skipped + skipped, nil
}

// Submit chooses the live matching receiver with the most spare capacity and publishes one root task.
func (client *Client) Submit(ctx context.Context, input SubmitInput) (SubmitResult, error) {
	loaded, err := client.loadState(ctx)
	if err != nil {
		return SubmitResult{}, err
	}
	now := time.Now()
	capabilities, skipped, err := client.loadCapabilities(ctx, loaded.receiverSigners(now))
	if err != nil {
		return SubmitResult{}, err
	}
	loaded.skipped += skipped
	var chosen *wire.Capability
	for _, verified := range capabilities {
		capability := verified.capability
		if now.UnixMilli()-capability.AtMS > int64(120*time.Second/time.Millisecond) {
			continue
		}
		if _, ok := wire.MatchOffer(input.Selector, capability.Labels, verified.enrolled.Adapters, capability.Offers); !ok {
			continue
		}
		if chosen == nil || spare(capability) > spare(chosen) ||
			(spare(capability) == spare(chosen) && capability.ReceiverID < chosen.ReceiverID) {
			chosen = capability
		}
	}
	if chosen == nil {
		return SubmitResult{}, ErrNoMatchingReceiver
	}
	runID, err := randomID("run-")
	if err != nil {
		return SubmitResult{}, err
	}
	messageID, err := randomID("msg-")
	if err != nil {
		return SubmitResult{}, err
	}
	submission := wire.SubmissionRecord{
		Message: wire.Message{
			MessageID: messageID, ContextID: runID, TaskID: runID, Role: wire.RoleUser,
			Parts: append([]wire.Part{{Text: input.Prompt}}, input.Parts...), Extensions: []string{wire.ExtensionURI},
		},
		Wherry: wire.SubmissionExtension{
			RunID: runID, Turn: 1, Attempt: 1, Pool: chosen.Pool, Selector: slices.Clone(input.Selector),
			DeadlineMS: now.Add(input.Deadline).UnixMilli(), QueueDeadlineMS: now.Add(input.QueueDeadline).UnixMilli(),
			ReplyTopic: client.replyTopic,
			Lineage:    wire.Lineage{RootRunID: runID, Depth: 0, Ancestors: []string{}, ParentHash: "", Chain: []wire.Bytes{}},
		},
	}
	envelope, err := trust.SignRootSubmission(submission, client.userKey)
	if err != nil {
		return SubmitResult{}, err
	}
	if err := client.produce(ctx, &kgo.Record{Topic: wire.TopicTask, Key: []byte(runID), Value: envelope}); err != nil {
		return SubmitResult{}, err
	}
	return SubmitResult{RunID: runID, Pool: chosen.Pool, Skipped: loaded.skipped}, nil
}

// Continue publishes the next user-signed turn of an interrupted run.
func (client *Client) Continue(ctx context.Context, runID string, input ContinueInput) (ContinueResult, error) {
	loaded, err := client.loadState(ctx)
	if err != nil {
		return ContinueResult{}, err
	}
	submissions, skipped, err := client.readSubmissions(ctx)
	if err != nil {
		return ContinueResult{}, err
	}
	run := submissions[runID]
	if len(run) == 0 {
		return ContinueResult{}, fmt.Errorf("continue %s: %w", runID, ErrRunNotFound)
	}
	receivers := loaded.receiverSigners(time.Now())
	statuses, statusSkipped, err := client.latestStatuses(ctx, submissions, receivers)
	if err != nil {
		return ContinueResult{}, err
	}
	status := statuses[runID]
	if status == nil || status.Wherry.Kind != "interrupted" || !wire.Interrupted(status.StatusUpdate.Status.State) {
		return ContinueResult{}, ErrNotAwaitingContinuation
	}
	cancelled, cancelSkipped, err := client.cancelled(ctx)
	if err != nil {
		return ContinueResult{}, err
	}
	if cancelled[runID] {
		return ContinueResult{}, ErrRunCanceled
	}
	nextTurn := status.Wherry.Turn + 1
	if run[nextTurn].record != nil {
		return ContinueResult{}, ErrTurnSubmitted
	}
	previous := run[status.Wherry.Turn]
	if previous.record == nil {
		return ContinueResult{}, ErrNotAwaitingContinuation
	}
	messageID, err := randomID("msg-")
	if err != nil {
		return ContinueResult{}, err
	}
	now := time.Now()
	continuation := wire.SubmissionRecord{
		Message: wire.Message{
			MessageID: messageID, ContextID: previous.record.Message.ContextID, TaskID: runID, Role: wire.RoleUser,
			Parts: []wire.Part{{Text: input.Prompt}}, Extensions: []string{wire.ExtensionURI},
		},
		Wherry: wire.SubmissionExtension{
			RunID: runID, Turn: nextTurn, Attempt: 1, Pool: previous.record.Wherry.Pool,
			Selector:   slices.Clone(previous.record.Wherry.Selector),
			DeadlineMS: now.Add(input.Deadline).UnixMilli(), QueueDeadlineMS: now.Add(input.QueueDeadline).UnixMilli(),
			ReplyTopic: previous.record.Wherry.ReplyTopic, Lineage: previous.record.Wherry.Lineage,
		},
	}
	envelope, err := trust.SignRootSubmission(continuation, client.userKey)
	if err != nil {
		return ContinueResult{}, err
	}
	if err := client.produce(ctx, &kgo.Record{Topic: wire.TopicTask, Key: []byte(runID), Value: envelope}); err != nil {
		return ContinueResult{}, err
	}
	return ContinueResult{RunID: runID, Turn: continuation.Wherry.Turn, Skipped: loaded.skipped + skipped + statusSkipped + cancelSkipped}, nil
}

// Cancel publishes a user-signed request to cancel a run.
func (client *Client) Cancel(ctx context.Context, runID string) (CancelResult, error) {
	loaded, err := client.loadState(ctx)
	if err != nil {
		return CancelResult{}, err
	}
	submissions, skipped, err := client.readSubmissions(ctx)
	if err != nil {
		return CancelResult{}, err
	}
	if len(submissions[runID]) == 0 {
		return CancelResult{}, fmt.Errorf("cancel %s: %w", runID, ErrRunNotFound)
	}
	envelope, err := trust.NewCancel(runID, client.userKey)
	if err != nil {
		return CancelResult{}, err
	}
	if err := client.produce(ctx, &kgo.Record{Topic: wire.TopicControl, Key: []byte(runID), Value: envelope}); err != nil {
		return CancelResult{}, err
	}
	return CancelResult{RunID: runID, Skipped: loaded.skipped + skipped}, nil
}

// cancelled returns the runs the user has cancelled and the number of unverifiable control records.
func (client *Client) cancelled(ctx context.Context) (map[string]bool, int, error) {
	records, err := client.readBounded(ctx, wire.TopicControl)
	if err != nil {
		return nil, 0, err
	}
	result := make(map[string]bool)
	skipped := 0
	for _, record := range records {
		runID, err := trust.VerifyCancel(record.Value, client.userKey.Public().(ed25519.PublicKey))
		if err != nil || string(record.Key) != runID {
			skipped++
			continue
		}
		result[runID] = true
	}
	return result, skipped, nil
}

func (client *Client) readSubmissions(ctx context.Context) (map[string]map[int64]runSubmission, int, error) {
	records, err := client.readBounded(ctx, wire.TopicTask)
	if err != nil {
		return nil, 0, err
	}
	result := make(map[string]map[int64]runSubmission)
	skipped := 0
	for _, record := range records {
		submission, err := trust.VerifyRootSubmission(record.Value, client.userKey.Public().(ed25519.PublicKey))
		if err != nil {
			skipped++
			continue
		}
		if string(record.Key) != submission.Wherry.RunID {
			skipped++
			continue
		}
		run := result[submission.Wherry.RunID]
		if run == nil {
			run = make(map[int64]runSubmission)
			result[submission.Wherry.RunID] = run
		}
		if _, duplicate := run[submission.Wherry.Turn]; duplicate {
			skipped++
			continue
		}
		run[submission.Wherry.Turn] = runSubmission{record: submission, hash: trust.EnvelopeHash(record.Value)}
	}
	return result, skipped, nil
}

func (client *Client) latestStatuses(ctx context.Context, submissions map[string]map[int64]runSubmission, receivers map[string]receiverIdentity) (map[string]*wire.StatusRecord, int, error) {
	records, err := client.readBounded(ctx, wire.TopicEvent)
	if err != nil {
		return nil, 0, err
	}
	latest := make(map[string]*wire.StatusRecord)
	var order wire.AttemptFilter
	skipped := 0
	for _, record := range records {
		envelope, err := wire.DecodeEnvelope(record.Value)
		if err != nil {
			skipped++
			continue
		}
		receiver, ok := receivers[envelope.Signer]
		if !ok {
			skipped++
			continue
		}
		verified, err := wire.Verify(envelope, receiver.enrolled.SigningPub, wire.SignerReceiver)
		status, ok := verified.(*wire.StatusRecord)
		if err != nil || !ok || status.Wherry.Kind == "rejected" {
			skipped++
			continue
		}
		submission := submissions[status.StatusUpdate.TaskID][status.Wherry.Turn]
		if submission.record == nil || string(record.Key) != status.StatusUpdate.TaskID ||
			receiver.id != status.Wherry.ReceiverID || receiver.enrolled.Pool != submission.record.Wherry.Pool ||
			status.StatusUpdate.ContextID != submission.record.Wherry.Lineage.RootRunID || status.Wherry.TaskHash != submission.hash {
			skipped++
			continue
		}
		if order.Admit(status.StatusUpdate.TaskID, status.Wherry.Turn, status.Wherry.Attempt) {
			latest[status.StatusUpdate.TaskID] = status
		}
	}
	return latest, skipped, nil
}

func (client *Client) waitingTasks(ctx context.Context, loaded *loadedState, pool, credential string) ([]WaitingTask, int, error) {
	submissions, skipped, err := client.readSubmissions(ctx)
	if err != nil {
		return nil, 0, err
	}
	statuses, statusSkipped, err := client.latestStatuses(ctx, submissions, loaded.receiverSigners(time.Now()))
	if err != nil {
		return nil, 0, err
	}
	cancelled, cancelSkipped, err := client.cancelled(ctx)
	if err != nil {
		return nil, 0, err
	}
	var waiting []WaitingTask
	for runID, status := range statuses {
		submission := submissions[runID][status.Wherry.Turn]
		var metadata wire.CredentialMetadata
		if !cancelled[runID] && submission.record.Wherry.Pool == pool && status.Wherry.Kind == "interrupted" &&
			status.StatusUpdate.Status.State == wire.TaskStateAuthRequired &&
			json.Unmarshal(status.StatusUpdate.Metadata, &metadata) == nil && metadata.Credential == credential {
			waiting = append(waiting, WaitingTask{RunID: runID, Turn: status.Wherry.Turn})
		}
	}
	sort.Slice(waiting, func(i, j int) bool { return waiting[i].RunID < waiting[j].RunID })
	return waiting, skipped + statusSkipped + cancelSkipped, nil
}

// Await follows the reply topic until it accepts a final task for runID: terminal, or waiting on authentication.
// No receiver holds an interrupted task, so the user's own cancel ends one as CANCELED.
func (client *Client) Await(ctx context.Context, runID string, timeout time.Duration) (AwaitResult, error) {
	loaded, err := client.loadState(ctx)
	if err != nil {
		return AwaitResult{}, err
	}
	receivers := loaded.receiverSigners(time.Now())
	revoked := loaded.revokedSigners()
	submissions, skipped, err := client.readSubmissions(ctx)
	if err != nil {
		return AwaitResult{}, err
	}
	loaded.skipped += skipped
	latest, latestTurn := latestSubmission(submissions[runID])
	if latest.record == nil {
		return AwaitResult{}, fmt.Errorf("await %s: %w", runID, ErrRunNotFound)
	}
	cancelled, cancelSkipped, err := client.cancelled(ctx)
	if err != nil {
		return AwaitResult{}, err
	}
	loaded.skipped += cancelSkipped
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	stop, err := client.followFromStart(waitCtx, client.replyTopic, wire.TopicEvent)
	if err != nil {
		return AwaitResult{}, err
	}
	defer stop()
	var attempts wire.AttemptFilter
	// terminal is a terminal status still waiting for its task record, which the receiver commits in the
	// same transaction but a fetch can deliver later; it is returned alone once taskGrace passes.
	var terminal *AwaitResult
	var graceEnd time.Time
	for {
		pollCtx, cancelPoll := waitCtx, context.CancelFunc(func() {})
		if terminal != nil {
			pollCtx, cancelPoll = context.WithDeadline(waitCtx, graceEnd)
		}
		fetches := client.kafka.PollFetches(pollCtx)
		graceOver := terminal != nil && pollCtx.Err() != nil
		cancelPoll()
		if graceOver {
			return *terminal, nil
		}
		if err := waitCtx.Err(); err != nil {
			return AwaitResult{}, fmt.Errorf("await %s: %w", runID, err)
		}
		if err := fetches.Err(); err != nil {
			return AwaitResult{}, err
		}
		records := fetches.Records()
		for _, topic := range []string{client.replyTopic, wire.TopicEvent} {
			for _, record := range records {
				if record.Topic != topic || record.Attrs.IsControl() || string(record.Key) != runID {
					continue
				}
				verified, receiver, revokedAt, err := verifyHistory(record, receivers, revoked)
				if err != nil {
					loaded.skipped++
					continue
				}
				switch value := verified.(type) {
				case *wire.TaskRecord:
					if topic != client.replyTopic || value.Wherry.Turn != latestTurn || receiver.enrolled.Pool != latest.record.Wherry.Pool ||
						value.Wherry.TaskHash != latest.hash || value.Task.ID != runID ||
						value.Task.ContextID != latest.record.Wherry.Lineage.RootRunID || !wire.Terminal(value.Task.Status.State) ||
						len(value.Task.History) < int(latestTurn) || value.Task.History[latestTurn-1].MessageID != latest.record.Message.MessageID ||
						!attempts.Admit(value.Task.ID, value.Wherry.Turn, value.Wherry.Attempt) {
						continue
					}
					return AwaitResult{Task: &value.Task, Status: value.Task.Status, RevokedAt: revokedAt, Skipped: loaded.skipped}, nil
				case *wire.StatusRecord:
					acceptable := value.Wherry.Kind == "interrupted" && wire.Interrupted(value.StatusUpdate.Status.State) ||
						value.Wherry.Kind == "terminal" && wire.Terminal(value.StatusUpdate.Status.State) ||
						value.Wherry.Kind == "rejected" && value.StatusUpdate.Status.State == wire.TaskStateRejected
					if topic != wire.TopicEvent || !turnStatus(value, receiver, latest, latestTurn, runID) || !acceptable ||
						!attempts.Admit(value.StatusUpdate.TaskID, value.Wherry.Turn, value.Wherry.Attempt) {
						continue
					}
					if value.Wherry.Kind == "interrupted" && cancelled[runID] {
						return AwaitResult{Status: wire.TaskStatus{State: wire.TaskStateCanceled}, Skipped: loaded.skipped}, nil
					}
					result := AwaitResult{Status: value.StatusUpdate.Status, RevokedAt: revokedAt, Skipped: loaded.skipped}
					if value.Wherry.Kind != "terminal" {
						return result, nil
					}
					if terminal == nil {
						terminal, graceEnd = &result, time.Now().Add(taskGrace)
					}
				}
			}
		}
	}
}

// Status returns the latest verified status of runID's latest turn, SUBMITTED until a receiver reports one.
// As in Await, the user's own cancel ends an interrupted task as CANCELED.
func (client *Client) Status(ctx context.Context, runID string) (StatusResult, error) {
	loaded, err := client.loadState(ctx)
	if err != nil {
		return StatusResult{}, err
	}
	receivers := loaded.receiverSigners(time.Now())
	revoked := loaded.revokedSigners()
	submissions, skipped, err := client.readSubmissions(ctx)
	if err != nil {
		return StatusResult{}, err
	}
	latest, latestTurn := latestSubmission(submissions[runID])
	if latest.record == nil {
		return StatusResult{}, fmt.Errorf("status %s: %w", runID, ErrRunNotFound)
	}
	cancelled, cancelSkipped, err := client.cancelled(ctx)
	if err != nil {
		return StatusResult{}, err
	}
	records, err := client.readBounded(ctx, wire.TopicEvent)
	if err != nil {
		return StatusResult{}, err
	}
	result := StatusResult{Turn: latestTurn, Status: wire.TaskStatus{State: wire.TaskStateSubmitted}, Skipped: loaded.skipped + skipped + cancelSkipped}
	var attempts wire.AttemptFilter
	var kind string
	for _, record := range records {
		if string(record.Key) != runID {
			continue
		}
		verified, receiver, revokedAt, err := verifyHistory(record, receivers, revoked)
		if err != nil {
			result.Skipped++
			continue
		}
		value, ok := verified.(*wire.StatusRecord)
		if !ok || !turnStatus(value, receiver, latest, latestTurn, runID) {
			continue
		}
		if attempts.Admit(runID, value.Wherry.Turn, value.Wherry.Attempt) {
			result.Status, result.RevokedAt, kind = value.StatusUpdate.Status, revokedAt, value.Wherry.Kind
		}
	}
	if kind == "interrupted" && cancelled[runID] {
		result.Status, result.RevokedAt = wire.TaskStatus{State: wire.TaskStateCanceled}, time.Time{}
	}
	return result, nil
}

// latestSubmission returns the highest turn of a run and its submission; the record is nil for an unknown run.
func latestSubmission(run map[int64]runSubmission) (runSubmission, int64) {
	var latest runSubmission
	var latestTurn int64
	for turn, submission := range run {
		if turn > latestTurn {
			latestTurn, latest = turn, submission
		}
	}
	return latest, latestTurn
}

// turnStatus reports whether a status was signed by its own receiver in the run's pool for the submission of turn.
func turnStatus(value *wire.StatusRecord, receiver receiverIdentity, submission runSubmission, turn int64, runID string) bool {
	return value.Wherry.Turn == turn && receiver.id == value.Wherry.ReceiverID && receiver.enrolled.Pool == submission.record.Wherry.Pool &&
		value.Wherry.TaskHash == submission.hash && value.StatusUpdate.TaskID == runID &&
		value.StatusUpdate.ContextID == submission.record.Wherry.Lineage.RootRunID
}

// Events reads and verifies the bounded status history and transcript chunks for runID.
func (client *Client) Events(ctx context.Context, runID string) (EventsResult, error) {
	loaded, err := client.loadState(ctx)
	if err != nil {
		return EventsResult{}, err
	}
	receivers := loaded.receiverSigners(time.Now())
	revoked := loaded.revokedSigners()
	records, err := client.readBounded(ctx, wire.TopicEvent)
	if err != nil {
		return EventsResult{}, err
	}
	result := EventsResult{Chunks: map[TurnAttempt]int{}, ChunksRevokedAt: map[TurnAttempt]time.Time{}, Skipped: loaded.skipped}
	for _, record := range records {
		if string(record.Key) != runID {
			continue
		}
		verified, receiver, revokedAt, err := verifyHistory(record, receivers, revoked)
		if err != nil {
			result.Skipped++
			continue
		}
		status, ok := verified.(*wire.StatusRecord)
		if !ok || status.Wherry.ReceiverID != receiver.id || status.StatusUpdate.TaskID != runID {
			result.Skipped++
			continue
		}
		result.Statuses = append(result.Statuses, EventStatus{StatusRecord: *status, RevokedAt: revokedAt})
	}
	controls, err := client.readBounded(ctx, wire.TopicControl)
	if err != nil {
		return EventsResult{}, err
	}
	for _, record := range controls {
		if string(record.Key) != runID {
			continue
		}
		if id, err := trust.VerifyCancel(record.Value, client.userKey.Public().(ed25519.PublicKey)); err != nil || id != runID {
			result.Skipped++
			continue
		}
		result.Cancelled = true
	}
	transcripts, err := client.readBounded(ctx, wire.TopicTranscript)
	if err != nil {
		return EventsResult{}, err
	}
	for _, record := range transcripts {
		if string(record.Key) != runID {
			continue
		}
		verified, _, revokedAt, err := verifyHistory(record, receivers, revoked)
		update, ok := verified.(*wire.ArtifactUpdateRecord)
		if err != nil || !ok || update.ArtifactUpdate.TaskID != runID {
			result.Skipped++
			continue
		}
		position := TurnAttempt{Turn: update.Wherry.Turn, Attempt: update.Wherry.Attempt}
		result.Chunks[position]++
		if !revokedAt.IsZero() {
			result.ChunksRevokedAt[position] = revokedAt
		}
	}
	return result, nil
}

func (client *Client) loadCapabilities(ctx context.Context, receivers map[string]receiverIdentity) ([]verifiedCapability, int, error) {
	records, err := client.readBounded(ctx, wire.TopicCapability)
	if err != nil {
		return nil, 0, err
	}
	latest := make(map[string][]byte)
	for _, record := range records {
		key := string(record.Key)
		if record.Value == nil {
			delete(latest, key)
		} else {
			latest[key] = append([]byte(nil), record.Value...)
		}
	}
	result := make([]verifiedCapability, 0, len(latest))
	skipped := 0
	keys := make([]string, 0, len(latest))
	for key := range latest {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		encoded := latest[key]
		envelope, err := wire.DecodeEnvelope(encoded)
		if err != nil {
			skipped++
			continue
		}
		receiver, ok := receivers[envelope.Signer]
		if !ok {
			skipped++
			continue
		}
		capability, err := trust.CapabilityWithin(encoded, receiver.enrolled)
		if err != nil {
			skipped++
			continue
		}
		if capability.ReceiverID != receiver.id {
			continue
		}
		result = append(result, verifiedCapability{receiverID: receiver.id, enrolled: receiver.enrolled, capability: capability})
	}
	return result, skipped, nil
}

func (client *Client) loadState(ctx context.Context) (*loadedState, error) {
	records, err := client.readBounded(ctx, wire.TopicState)
	if err != nil {
		return nil, err
	}
	loaded := &loadedState{
		state:    trust.NewState(client.userKey.Public().(ed25519.PublicKey)),
		requests: make(map[string]requestRecord), approvals: make(map[string]approvalRecord),
		approvedRequests: make(map[string]int64), secretVersions: make(map[secretKey]wire.SecretVersion),
		revocations: make(map[string][]revocationRecord),
	}
	for _, record := range records {
		verified, err := loaded.state.Apply(record.Value)
		if err != nil {
			loaded.skipped++
			continue
		}
		switch payload := verified.(type) {
		case *wire.EnrolRequest:
			hash := trust.EnvelopeHash(record.Value)
			loaded.requests[hash] = requestRecord{envelope: append([]byte(nil), record.Value...), request: payload}
		case *wire.EnrolApproval:
			current, hash, _ := loaded.state.Approval(payload.ReceiverID)
			loaded.approvals[payload.ReceiverID] = approvalRecord{generation: current.Generation, envelopeHash: hash, pool: current.Pool}
			loaded.approvedRequests[current.RequestHash] = current.Generation
		case *wire.SecretVersion:
			key := secretKey{receiverID: payload.ReceiverID, name: payload.Name}
			if payload.Version > loaded.secretVersions[key].Version {
				loaded.secretVersions[key] = *payload
			}
		case *wire.Revocation:
			loaded.revocations[payload.ReceiverID] = append(loaded.revocations[payload.ReceiverID], revocationRecord{
				floor: payload.FloorGeneration, timestampType: record.Attrs.TimestampType(), timestamp: record.Timestamp,
			})
		}
	}
	now := time.Now()
	for id := range loaded.approvals {
		current, _, _ := loaded.state.Approval(id)
		// An approval that already outlasts a renewal is left as it is; the request stays pending.
		extends := now.Add(renewalLifetime).UnixMilli() > current.ExpMS
		if _, pending := loaded.state.RenewRequest(id); pending && current.Renewal == "auto" && extends {
			if _, err := client.renew(ctx, loaded, id, now); err != nil {
				return nil, fmt.Errorf("renew %s: %w", id, err)
			}
		}
	}
	return loaded, nil
}

// seals returns a receiver's current secret versions sealed under approvalHash, by name.
func (loaded *loadedState) seals(receiverID, approvalHash string) []Seal {
	seals := []Seal{}
	for key, version := range loaded.secretVersions {
		if key.receiverID == receiverID && version.ApprovalHash == approvalHash {
			seals = append(seals, Seal{Name: version.Name, Version: version.Version, ExpMS: version.ExpMS})
		}
	}
	sort.Slice(seals, func(i, j int) bool { return seals[i].Name < seals[j].Name })
	return seals
}

func (loaded *loadedState) receiverSigners(now time.Time) map[string]receiverIdentity {
	ids := make([]string, 0, len(loaded.approvals))
	for id := range loaded.approvals {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	receivers := make(map[string]receiverIdentity, len(ids))
	for _, id := range ids {
		enrolled, err := loaded.state.Receiver(id, now)
		if err != nil {
			continue
		}
		receivers[wire.Fingerprint(enrolled.SigningPub)] = receiverIdentity{id: id, enrolled: enrolled}
	}
	return receivers
}

// revokedSigners returns the revoked receivers with a broker revocation time, by signer fingerprint.
func (loaded *loadedState) revokedSigners() map[string]revokedSigner {
	signers := make(map[string]revokedSigner)
	for id, approval := range loaded.approvals {
		if approval.generation > loaded.state.Floor(id) {
			continue
		}
		revokedAt, ok := revocationTime(loaded.revocations[id], approval.generation)
		if !ok {
			continue
		}
		head, _, _ := loaded.state.Approval(id)
		request, ok := loaded.requests[head.RequestHash]
		if !ok {
			continue
		}
		signingPub := ed25519.PublicKey(request.request.SigningPub)
		signers[wire.Fingerprint(signingPub)] = revokedSigner{
			identity:  receiverIdentity{id: id, enrolled: trust.Enrolled{SigningPub: signingPub, Pool: head.Pool, Generation: head.Generation}},
			revokedAt: revokedAt,
		}
	}
	return signers
}

// revocationTime returns the broker append time of the first revocation, in log order, whose floor covers generation.
// It reports false when that record's timestamp is the producer's.
func revocationTime(revocations []revocationRecord, generation int64) (time.Time, bool) {
	for _, revocation := range revocations {
		if revocation.floor >= generation {
			return revocation.timestamp, revocation.timestampType == logAppendTime
		}
	}
	return time.Time{}, false
}

// logAppendTime is the Kafka record timestamp type set by the broker on append.
const logAppendTime = 1

// appendedBefore reports whether the broker appended a record strictly before revokedAt.
func appendedBefore(timestampType int8, timestamp, revokedAt time.Time) bool {
	return timestampType == logAppendTime && timestamp.Before(revokedAt)
}

// verifyHistory verifies a receiver-signed record read as history. Its signer is an enrolled receiver, or a revoked
// one whose record the broker appended before the revocation; revokedAt is zero for an enrolled signer.
func verifyHistory(record *kgo.Record, receivers map[string]receiverIdentity, revoked map[string]revokedSigner) (any, receiverIdentity, time.Time, error) {
	envelope, err := wire.DecodeEnvelope(record.Value)
	if err != nil {
		return nil, receiverIdentity{}, time.Time{}, err
	}
	receiver, ok := receivers[envelope.Signer]
	var revokedAt time.Time
	if !ok {
		signer, ok := revoked[envelope.Signer]
		if !ok || !appendedBefore(record.Attrs.TimestampType(), record.Timestamp, signer.revokedAt) {
			return nil, receiverIdentity{}, time.Time{}, errors.New("record signer is not trusted")
		}
		receiver, revokedAt = signer.identity, signer.revokedAt
	}
	verified, err := wire.Verify(envelope, receiver.enrolled.SigningPub, wire.SignerReceiver)
	if err != nil {
		return nil, receiverIdentity{}, time.Time{}, err
	}
	return verified, receiver, revokedAt, nil
}

func spare(capability *wire.Capability) int64 {
	return capability.Capacity - capability.InFlight
}

func randomID(prefix string) (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(bytes), nil
}
