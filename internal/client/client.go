package client

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/benemon/wherry/internal/trust"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

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

// SubmitInput contains the root-task routing and prompt fields.
type SubmitInput struct {
	Selector      []wire.Requirement
	Labels        map[string]string
	Prompt        string
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

// AwaitResult contains the accepted final task.
type AwaitResult struct {
	Task    *wire.Task
	Status  wire.TaskStatus
	Turn    int64
	Skipped int
}

// TurnAttempt identifies one delivery attempt within one submission turn.
type TurnAttempt struct {
	Turn    int64
	Attempt int64
}

// EventsResult contains a run's verified status records and its transcript chunk count per turn and attempt.
type EventsResult struct {
	Statuses []wire.StatusRecord
	Chunks   map[TurnAttempt]int
	Skipped  int
}

// Receiver describes one enrolled receiver and its latest verified capability.
type Receiver struct {
	ReceiverID   string              `json:"receiver_id"`
	Pool         string              `json:"pool"`
	Generation   int64               `json:"generation"`
	Labels       map[string]string   `json:"labels"`
	Selector     []wire.Requirement  `json:"selector"`
	Adapters     []wire.AdapterGrant `json:"adapters"`
	Offers       []wire.Offer        `json:"offers"`
	Capacity     int64               `json:"capacity"`
	InFlight     int64               `json:"in_flight"`
	HeartbeatAge string              `json:"heartbeat_age"`
	Status       string              `json:"status"`
	Revoked      bool                `json:"revoked"`
	Floor        int64               `json:"floor_generation"`
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
	secretVersions   map[secretKey]int64
	skipped          int
}

type receiverIdentity struct {
	id       string
	enrolled trust.Enrolled
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
		generation, approved := loaded.approvedRequests[hash]
		result = append(result, Enrolment{
			ReceiverID: record.request.ReceiverID, RequestHash: hash,
			Labels:   maps.Clone(record.request.Labels),
			Adapters: append([]wire.DetectedAdapter(nil), record.request.Adapters...),
			Approved: approved, Generation: generation,
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
func (client *Client) Approve(ctx context.Context, requestHash, pool string, labels map[string]string, grants []wire.AdapterGrant, selector []wire.Requirement, renewal string, days int) (ApprovalResult, error) {
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
		request.envelope, request.request, client.userKey, labels, pool, grants, selector, renewal,
		generation, current.envelopeHash, time.Now().Add(time.Duration(days)*24*time.Hour),
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
func (client *Client) PutSecret(ctx context.Context, name, receiverID, path string) (SecretResult, error) {
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
	version := loaded.secretVersions[secretKey{receiverID: receiverID, name: name}] + 1
	versionEnvelope, sealedEnvelope, err := trust.Seal(enrolled, receiverID, name, version, value, now.Add(30*24*time.Hour), client.userKey)
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
			Selector: slices.Clone(identity.enrolled.Selector),
			Adapters: identity.enrolled.Adapters, Offers: []wire.Offer{},
			HeartbeatAge: "-", Status: "silent", Floor: loaded.state.Floor(identity.id),
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
		if _, err := loaded.state.Receiver(id, now); errors.Is(err, trust.ErrRevoked) {
			result = append(result, Receiver{
				ReceiverID: id, Pool: approval.pool, Generation: approval.generation,
				Labels: map[string]string{}, Selector: []wire.Requirement{}, Adapters: []wire.AdapterGrant{}, Offers: []wire.Offer{},
				HeartbeatAge: "-", Status: "revoked", Revoked: true, Floor: loaded.state.Floor(id),
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
		if _, ok := wire.MatchOffer(input.Selector, capability.Labels, verified.enrolled.Adapters, capability.Offers); !ok || !wire.Satisfies(capability.Selector, input.Labels) {
			continue
		}
		if chosen == nil || spare(capability) > spare(chosen) ||
			(spare(capability) == spare(chosen) && capability.ReceiverID < chosen.ReceiverID) {
			chosen = capability
		}
	}
	if chosen == nil {
		return SubmitResult{}, errors.New("no matching live receiver")
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
			Parts: []wire.Part{{Text: input.Prompt}}, Extensions: []string{wire.ExtensionURI},
		},
		Wherry: wire.SubmissionExtension{
			RunID: runID, Turn: 1, Attempt: 1, Pool: chosen.Pool, Selector: slices.Clone(input.Selector), Labels: maps.Clone(input.Labels),
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
	if input.Deadline <= 0 || input.QueueDeadline <= 0 {
		return ContinueResult{}, errors.New("deadlines must be positive")
	}
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
		return ContinueResult{}, fmt.Errorf("continue %s: verified root submission not found", runID)
	}
	receivers := loaded.receiverSigners(time.Now())
	statuses, statusSkipped, err := client.latestStatuses(ctx, submissions, receivers)
	if err != nil {
		return ContinueResult{}, err
	}
	status := statuses[runID]
	if status == nil || status.Wherry.Kind != "interrupted" || !wire.Interrupted(status.StatusUpdate.Status.State) {
		return ContinueResult{}, errors.New("task is not awaiting continuation")
	}
	nextTurn := status.Wherry.Turn + 1
	if run[nextTurn].record != nil {
		return ContinueResult{}, errors.New("continuation turn already submitted")
	}
	previous := run[status.Wherry.Turn]
	if previous.record == nil {
		return ContinueResult{}, errors.New("task is not awaiting continuation")
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
			Selector: slices.Clone(previous.record.Wherry.Selector), Labels: maps.Clone(previous.record.Wherry.Labels),
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
	return ContinueResult{RunID: runID, Turn: continuation.Wherry.Turn, Skipped: loaded.skipped + skipped + statusSkipped}, nil
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
	wantMessage := "credential " + credential + " required"
	var waiting []WaitingTask
	for runID, status := range statuses {
		submission := submissions[runID][status.Wherry.Turn]
		if submission.record.Wherry.Pool == pool && status.Wherry.Kind == "interrupted" &&
			status.StatusUpdate.Status.State == wire.TaskStateAuthRequired && statusText(status.StatusUpdate.Status) == wantMessage {
			waiting = append(waiting, WaitingTask{RunID: runID, Turn: status.Wherry.Turn})
		}
	}
	sort.Slice(waiting, func(i, j int) bool { return waiting[i].RunID < waiting[j].RunID })
	return waiting, skipped + statusSkipped, nil
}

func statusText(status wire.TaskStatus) string {
	if status.Message == nil {
		return ""
	}
	var parts []string
	for _, part := range status.Message.Parts {
		if part.Text != "" {
			parts = append(parts, part.Text)
		}
	}
	return strings.Join(parts, " ")
}

// Await follows the reply topic until it accepts a final task for runID: terminal, or waiting on authentication.
func (client *Client) Await(ctx context.Context, runID string, timeout time.Duration) (AwaitResult, error) {
	loaded, err := client.loadState(ctx)
	if err != nil {
		return AwaitResult{}, err
	}
	receivers := loaded.receiverSigners(time.Now())
	submissions, skipped, err := client.readSubmissions(ctx)
	if err != nil {
		return AwaitResult{}, err
	}
	loaded.skipped += skipped
	run := submissions[runID]
	var latest runSubmission
	var latestTurn int64
	for turn, submission := range run {
		if turn > latestTurn {
			latestTurn, latest = turn, submission
		}
	}
	if latest.record == nil {
		return AwaitResult{}, fmt.Errorf("await %s: verified root submission not found", runID)
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	stop, err := client.followFromStart(waitCtx, client.replyTopic, wire.TopicEvent)
	if err != nil {
		return AwaitResult{}, err
	}
	defer stop()
	var attempts wire.AttemptFilter
	for {
		fetches := client.kafka.PollFetches(waitCtx)
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
				envelope, err := wire.DecodeEnvelope(record.Value)
				if err != nil {
					loaded.skipped++
					continue
				}
				receiver, ok := receivers[envelope.Signer]
				if !ok {
					loaded.skipped++
					continue
				}
				verified, err := wire.Verify(envelope, receiver.enrolled.SigningPub, wire.SignerReceiver)
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
					return AwaitResult{Task: &value.Task, Status: value.Task.Status, Turn: value.Wherry.Turn, Skipped: loaded.skipped}, nil
				case *wire.StatusRecord:
					acceptable := value.Wherry.Kind == "interrupted" && wire.Interrupted(value.StatusUpdate.Status.State) ||
						value.Wherry.Kind == "terminal" && wire.Terminal(value.StatusUpdate.Status.State) ||
						value.Wherry.Kind == "rejected" && value.StatusUpdate.Status.State == wire.TaskStateRejected
					if topic != wire.TopicEvent || value.Wherry.Turn != latestTurn || receiver.id != value.Wherry.ReceiverID || receiver.enrolled.Pool != latest.record.Wherry.Pool ||
						value.Wherry.TaskHash != latest.hash || value.StatusUpdate.TaskID != runID ||
						value.StatusUpdate.ContextID != latest.record.Wherry.Lineage.RootRunID || !acceptable ||
						!attempts.Admit(value.StatusUpdate.TaskID, value.Wherry.Turn, value.Wherry.Attempt) {
						continue
					}
					return AwaitResult{Status: value.StatusUpdate.Status, Turn: value.Wherry.Turn, Skipped: loaded.skipped}, nil
				}
			}
		}
	}
}

// Events reads and verifies the bounded status history and transcript chunks for runID.
func (client *Client) Events(ctx context.Context, runID string) (EventsResult, error) {
	loaded, err := client.loadState(ctx)
	if err != nil {
		return EventsResult{}, err
	}
	receivers := loaded.receiverSigners(time.Now())
	records, err := client.readBounded(ctx, wire.TopicEvent)
	if err != nil {
		return EventsResult{}, err
	}
	result := EventsResult{Chunks: map[TurnAttempt]int{}, Skipped: loaded.skipped}
	for _, record := range records {
		envelope, err := wire.DecodeEnvelope(record.Value)
		if err != nil {
			result.Skipped++
			continue
		}
		receiver, ok := receivers[envelope.Signer]
		if !ok {
			result.Skipped++
			continue
		}
		verified, err := wire.Verify(envelope, receiver.enrolled.SigningPub, wire.SignerReceiver)
		if err != nil {
			result.Skipped++
			continue
		}
		status, ok := verified.(*wire.StatusRecord)
		if !ok || status.Wherry.ReceiverID != receiver.id {
			result.Skipped++
			continue
		}
		if status.StatusUpdate.TaskID == runID {
			result.Statuses = append(result.Statuses, *status)
		}
	}
	transcripts, err := client.readBounded(ctx, wire.TopicTranscript)
	if err != nil {
		return EventsResult{}, err
	}
	// Transcripts are keyed by run ID; only this run's records are verified.
	for _, record := range transcripts {
		if string(record.Key) != runID {
			continue
		}
		envelope, err := wire.DecodeEnvelope(record.Value)
		if err != nil {
			result.Skipped++
			continue
		}
		receiver, ok := receivers[envelope.Signer]
		if !ok {
			result.Skipped++
			continue
		}
		verified, err := wire.Verify(envelope, receiver.enrolled.SigningPub, wire.SignerReceiver)
		update, ok := verified.(*wire.ArtifactUpdateRecord)
		if err != nil || !ok || update.ArtifactUpdate.TaskID != runID {
			result.Skipped++
			continue
		}
		result.Chunks[TurnAttempt{Turn: update.Wherry.Turn, Attempt: update.Wherry.Attempt}]++
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
		approvedRequests: make(map[string]int64), secretVersions: make(map[secretKey]int64),
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
			if payload.Generation > loaded.approvals[payload.ReceiverID].generation {
				loaded.approvals[payload.ReceiverID] = approvalRecord{
					generation: payload.Generation, envelopeHash: trust.EnvelopeHash(record.Value), pool: payload.Pool,
				}
				loaded.approvedRequests[payload.RequestHash] = payload.Generation
			}
		case *wire.SecretVersion:
			key := secretKey{receiverID: payload.ReceiverID, name: payload.Name}
			if payload.Version > loaded.secretVersions[key] {
				loaded.secretVersions[key] = payload.Version
			}
		}
	}
	return loaded, nil
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
