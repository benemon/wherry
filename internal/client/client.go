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
	"time"

	"github.com/benemon/agentbus/internal/trust"
	"github.com/benemon/agentbus/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Client implements the user-side agentbus operations.
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

// SecretResult describes a produced sealed-secret version.
type SecretResult struct {
	Name       string
	ReceiverID string
	Version    int64
	Skipped    int
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

// AwaitResult contains the accepted terminal reply.
type AwaitResult struct {
	Reply   *wire.Reply
	Skipped int
}

// EventsResult contains verified events for a run.
type EventsResult struct {
	Events  []wire.Event
	Skipped int
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
}

type requestRecord struct {
	envelope []byte
	request  *wire.EnrolRequest
}

type approvalRecord struct {
	generation   int64
	envelopeHash string
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
	generation := current.generation + 1
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
	return SecretResult{Name: name, ReceiverID: receiverID, Version: version, Skipped: loaded.skipped}, nil
}

// Receivers returns every enrolled receiver with its latest verified capability status.
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
			HeartbeatAge: "-", Status: "silent",
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
	correlationID, err := randomID("")
	if err != nil {
		return SubmitResult{}, err
	}
	messageID, err := randomID("msg-")
	if err != nil {
		return SubmitResult{}, err
	}
	task := wire.Task{
		RunID: runID, Attempt: 1, RootRunID: runID, Depth: 0,
		Ancestors: []string{}, ParentHash: "", Chain: []wire.Bytes{},
		Pool: chosen.Pool, Labels: maps.Clone(input.Labels), Selector: slices.Clone(input.Selector),
		DeadlineMS: now.Add(input.Deadline).UnixMilli(), QueueDeadlineMS: now.Add(input.QueueDeadline).UnixMilli(),
		ReplyTopic: client.replyTopic, CorrelationID: correlationID,
		Message: wire.Message{MessageID: messageID, Role: wire.RoleUser, Parts: []wire.Part{{Text: input.Prompt}}},
	}
	envelope, err := trust.SignRootTask(task, client.userKey)
	if err != nil {
		return SubmitResult{}, err
	}
	if err := client.produce(ctx, &kgo.Record{Topic: wire.TopicTask, Key: []byte(runID), Value: envelope}); err != nil {
		return SubmitResult{}, err
	}
	return SubmitResult{RunID: runID, Pool: chosen.Pool, Skipped: loaded.skipped}, nil
}

// Await follows the reply topic until it accepts a terminal reply for runID.
func (client *Client) Await(ctx context.Context, runID string, timeout time.Duration) (AwaitResult, error) {
	loaded, err := client.loadState(ctx)
	if err != nil {
		return AwaitResult{}, err
	}
	receivers := loaded.receiverSigners(time.Now())
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	stop, err := client.followFromStart(waitCtx, client.replyTopic)
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
		for _, record := range fetches.Records() {
			if record.Topic != client.replyTopic || record.Attrs.IsControl() {
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
			reply, ok := verified.(*wire.Reply)
			if !ok || reply.RunID != runID || !attempts.Admit(reply.RunID, reply.Attempt) || !terminal(reply.State) {
				continue
			}
			return AwaitResult{Reply: reply, Skipped: loaded.skipped}, nil
		}
	}
}

// Events reads and verifies the bounded event history for runID.
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
	result := EventsResult{Skipped: loaded.skipped}
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
		event, ok := verified.(*wire.Event)
		if !ok || event.ReceiverID != receiver.id {
			result.Skipped++
			continue
		}
		if event.RunID == runID {
			result.Events = append(result.Events, *event)
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
					generation: payload.Generation, envelopeHash: trust.EnvelopeHash(record.Value),
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

func terminal(state wire.TaskState) bool {
	switch state {
	case wire.TaskStateCompleted, wire.TaskStateFailed, wire.TaskStateCanceled, wire.TaskStateRejected:
		return true
	default:
		return false
	}
}

func randomID(prefix string) (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(bytes), nil
}
