package wire

import (
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"
)

// RecordType identifies a wire payload schema.
type RecordType string

const (
	TypeSubmission     RecordType = "submission"
	TypeCancel         RecordType = "cancel"
	TypeStatus         RecordType = "status"
	TypeTask           RecordType = "task"
	TypeArtifactUpdate RecordType = "artifact_update"
	TypeCapability     RecordType = "capability"
	TypeEnrolRequest   RecordType = "enrol_request"
	TypeEnrolApproval  RecordType = "enrol_approval"
	TypeRenewRequest   RecordType = "renew_request"
	TypeRevocation     RecordType = "revocation"
	TypeSecretVersion  RecordType = "secret_version"
	TypeSealedSecret   RecordType = "sealed_secret"
)

// SignerKind identifies the authority behind a signing key.
type SignerKind string

const (
	SignerUser     SignerKind = "user"
	SignerReceiver SignerKind = "receiver"
)

var signerEntitlements = map[RecordType]map[SignerKind]bool{
	TypeSubmission:     {SignerUser: true},
	TypeCancel:         {SignerUser: true},
	TypeStatus:         {SignerReceiver: true},
	TypeTask:           {SignerReceiver: true},
	TypeArtifactUpdate: {SignerReceiver: true},
	TypeCapability:     {SignerReceiver: true},
	TypeEnrolRequest:   {SignerReceiver: true},
	TypeEnrolApproval:  {SignerUser: true},
	TypeRenewRequest:   {SignerReceiver: true},
	TypeRevocation:     {SignerUser: true},
	TypeSecretVersion:  {SignerUser: true},
	TypeSealedSecret:   {SignerUser: true},
}

// entitled reports whether a signer kind may sign a record type.
func entitled(recordType RecordType, kind SignerKind) bool {
	return signerEntitlements[recordType][kind]
}

// Role is an A2A Message role name.
type Role string

const (
	RoleUser  Role = "ROLE_USER"
	RoleAgent Role = "ROLE_AGENT"
)

// TaskState is an A2A TaskState enum name.
type TaskState string

const (
	TaskStateUnspecified   TaskState = "TASK_STATE_UNSPECIFIED"
	TaskStateSubmitted     TaskState = "TASK_STATE_SUBMITTED"
	TaskStateWorking       TaskState = "TASK_STATE_WORKING"
	TaskStateCompleted     TaskState = "TASK_STATE_COMPLETED"
	TaskStateFailed        TaskState = "TASK_STATE_FAILED"
	TaskStateCanceled      TaskState = "TASK_STATE_CANCELED"
	TaskStateInputRequired TaskState = "TASK_STATE_INPUT_REQUIRED"
	TaskStateRejected      TaskState = "TASK_STATE_REJECTED"
	TaskStateAuthRequired  TaskState = "TASK_STATE_AUTH_REQUIRED"
)

var taskStates = map[TaskState]bool{
	TaskStateUnspecified:   true,
	TaskStateSubmitted:     true,
	TaskStateWorking:       true,
	TaskStateCompleted:     true,
	TaskStateFailed:        true,
	TaskStateCanceled:      true,
	TaskStateInputRequired: true,
	TaskStateRejected:      true,
	TaskStateAuthRequired:  true,
}

var terminalStates = map[TaskState]bool{
	TaskStateCompleted: true,
	TaskStateFailed:    true,
	TaskStateCanceled:  true,
	TaskStateRejected:  true,
}

// Terminal reports whether an A2A task state ends a task.
func Terminal(state TaskState) bool {
	return terminalStates[state]
}

// Interrupted reports whether an A2A task state waits for a continuation.
func Interrupted(state TaskState) bool {
	return state == TaskStateAuthRequired || state == TaskStateInputRequired
}

// Part is an A2A Part with exactly one of Text, Raw, URL or Data.
type Part struct {
	Text      string         `json:"text,omitzero"`
	Raw       StdBytes       `json:"raw,omitzero"`
	URL       string         `json:"url,omitzero"`
	Data      jsontext.Value `json:"data,omitzero"`
	Metadata  jsontext.Value `json:"metadata,omitzero"`
	Filename  string         `json:"filename,omitzero"`
	MediaType string         `json:"mediaType,omitzero"`
}

// ExtensionURI identifies the wherry A2A extension.
const ExtensionURI = "https://github.com/benemon/wherry/blob/main/docs/a2a-extension-v1.md"

// Message is an A2A Message.
type Message struct {
	MessageID        string         `json:"messageId"`
	ContextID        string         `json:"contextId,omitzero"`
	TaskID           string         `json:"taskId,omitzero"`
	Role             Role           `json:"role"`
	Parts            []Part         `json:"parts"`
	Metadata         jsontext.Value `json:"metadata,omitzero"`
	Extensions       []string       `json:"extensions,omitzero"`
	ReferenceTaskIDs []string       `json:"referenceTaskIds,omitzero"`
}

// Artifact is an A2A Artifact.
type Artifact struct {
	ArtifactID  string         `json:"artifactId"`
	Name        string         `json:"name,omitzero"`
	Description string         `json:"description,omitzero"`
	Parts       []Part         `json:"parts"`
	Metadata    jsontext.Value `json:"metadata,omitzero"`
	Extensions  []string       `json:"extensions,omitzero"`
}

// Requirement is one selector condition.
type Requirement struct {
	Key      string   `json:"key"`
	Operator string   `json:"operator"`
	Values   []string `json:"values"`
}

// OpIn is the only Requirement operator.
const OpIn = "In"

// TaskStatus is an A2A TaskStatus; Timestamp is RFC 3339, as proto3 JSON writes a Timestamp.
type TaskStatus struct {
	State     TaskState `json:"state"`
	Message   *Message  `json:"message,omitzero"`
	Timestamp string    `json:"timestamp,omitzero"`
}

// Task is an A2A Task.
type Task struct {
	ID        string         `json:"id"`
	ContextID string         `json:"contextId,omitzero"`
	Status    TaskStatus     `json:"status"`
	Artifacts []Artifact     `json:"artifacts,omitzero"`
	History   []Message      `json:"history,omitzero"`
	Metadata  jsontext.Value `json:"metadata,omitzero"`
}

// TaskStatusUpdateEvent is an A2A TaskStatusUpdateEvent.
type TaskStatusUpdateEvent struct {
	TaskID    string         `json:"taskId"`
	ContextID string         `json:"contextId"`
	Status    TaskStatus     `json:"status"`
	Metadata  jsontext.Value `json:"metadata,omitzero"`
}

// TaskArtifactUpdateEvent is an A2A TaskArtifactUpdateEvent.
type TaskArtifactUpdateEvent struct {
	TaskID    string         `json:"taskId"`
	ContextID string         `json:"contextId"`
	Artifact  Artifact       `json:"artifact"`
	Append    bool           `json:"append,omitzero"`
	LastChunk bool           `json:"lastChunk,omitzero"`
	Metadata  jsontext.Value `json:"metadata,omitzero"`
}

// Lineage places a submission in its delegation tree.
type Lineage struct {
	RootRunID  string   `json:"root_run_id"`
	Depth      int64    `json:"depth"`
	Ancestors  []string `json:"ancestors"`
	ParentHash string   `json:"parent_hash"`
	Chain      []Bytes  `json:"chain"`
}

// SubmissionExtension is the wherry extension block of a submission.
type SubmissionExtension struct {
	RunID           string            `json:"run_id"`
	Turn            int64             `json:"turn"`
	Attempt         int64             `json:"attempt"`
	Pool            string            `json:"pool"`
	Selector        []Requirement     `json:"selector"`
	Labels          map[string]string `json:"labels"`
	DeadlineMS      int64             `json:"deadline_ms"`
	QueueDeadlineMS int64             `json:"queue_deadline_ms"`
	ReplyTopic      string            `json:"reply_topic"`
	Lineage         Lineage           `json:"lineage"`
}

// SubmissionRecord is a submission payload: the A2A Message that starts a task.
type SubmissionRecord struct {
	Message Message             `json:"message"`
	Wherry  SubmissionExtension `json:"wherry"`
}

// StatusExtension is the wherry extension block of a status record.
type StatusExtension struct {
	Turn       int64  `json:"turn"`
	Attempt    int64  `json:"attempt"`
	TaskHash   string `json:"task_hash"`
	ReceiverID string `json:"receiver_id"`
	Kind       string `json:"kind"`
	LogOffset  *int64 `json:"log_offset,omitzero"`
}

// StatusRecord is a status payload.
type StatusRecord struct {
	StatusUpdate TaskStatusUpdateEvent `json:"statusUpdate"`
	Wherry       StatusExtension       `json:"wherry"`
}

// ArtifactUpdateExtension is the wherry extension block of an artifact_update record.
type ArtifactUpdateExtension struct {
	Turn    int64 `json:"turn"`
	Attempt int64 `json:"attempt"`
	Seq     int64 `json:"seq"`
}

// ArtifactUpdateRecord is an artifact_update payload.
type ArtifactUpdateRecord struct {
	ArtifactUpdate TaskArtifactUpdateEvent `json:"artifactUpdate"`
	Wherry         ArtifactUpdateExtension `json:"wherry"`
}

// TaskExtension is the wherry extension block of a task record.
type TaskExtension struct {
	Turn     int64  `json:"turn"`
	Attempt  int64  `json:"attempt"`
	TaskHash string `json:"task_hash"`
}

// TaskRecord is a task payload: the final A2A Task.
type TaskRecord struct {
	Task   Task          `json:"task"`
	Wherry TaskExtension `json:"wherry"`
}

// Cancel is a cancellation payload.
type Cancel struct {
	RunID string `json:"run_id"`
	AtMS  int64  `json:"at_ms"`
}

// PointerMetadata is the metadata of a url part that stands in for content not carried inline.
type PointerMetadata struct {
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

// AttemptFilter drops records superseded by a later turn or delivery attempt.
type AttemptFilter struct {
	highest map[string]turnAttempt
}

type turnAttempt struct {
	turn    int64
	attempt int64
}

// Admit reports whether a record is current for its run.
func (filter *AttemptFilter) Admit(runID string, turn, attempt int64) bool {
	highest := filter.highest[runID]
	if turn < highest.turn || turn == highest.turn && attempt < highest.attempt {
		return false
	}
	if filter.highest == nil {
		filter.highest = make(map[string]turnAttempt)
	}
	filter.highest[runID] = turnAttempt{turn: turn, attempt: attempt}
	return true
}

// Offer is one placement combination an adapter currently has.
type Offer struct {
	Adapter string            `json:"adapter"`
	Labels  map[string]string `json:"labels"`
}

// DetectedAdapter is one configured adapter as reported in an enrolment request.
type DetectedAdapter struct {
	Name     string  `json:"name"`
	Platform string  `json:"platform"`
	Offers   []Offer `json:"offers"`
}

// AdapterGrant is the user-approved envelope for one adapter.
type AdapterGrant struct {
	Name     string              `json:"name"`
	Platform string              `json:"platform"`
	Allowed  map[string][]string `json:"allowed"`
	Defaults map[string]string   `json:"defaults"`
}

// Capability is a receiver capability payload.
type Capability struct {
	ReceiverID string            `json:"receiver_id"`
	Labels     map[string]string `json:"labels"`
	Pool       string            `json:"pool"`
	Offers     []Offer           `json:"offers"`
	Selector   []Requirement     `json:"selector"`
	Capacity   int64             `json:"capacity"`
	InFlight   int64             `json:"in_flight"`
	Generation int64             `json:"generation"`
	AtMS       int64             `json:"at_ms"`
}

// EnrolRequest is a receiver enrolment request payload.
type EnrolRequest struct {
	ReceiverID string            `json:"receiver_id"`
	SigningPub Bytes             `json:"signing_pub"`
	SealingPub Bytes             `json:"sealing_pub"`
	Labels     map[string]string `json:"labels"`
	Adapters   []DetectedAdapter `json:"adapters"`
	AtMS       int64             `json:"at_ms"`
}

// EnrolApproval is a user-approved receiver enrolment payload.
type EnrolApproval struct {
	ReceiverID    string            `json:"receiver_id"`
	RequestHash   string            `json:"request_hash"`
	Labels        map[string]string `json:"labels"`
	Pool          string            `json:"pool"`
	Adapters      []AdapterGrant    `json:"adapters"`
	Selector      []Requirement     `json:"selector"`
	Renewal       string            `json:"renewal"`
	Generation    int64             `json:"generation"`
	PrevStateHash string            `json:"prev_state_hash"`
	ExpMS         int64             `json:"exp_ms"`
}

// RenewRequest is a receiver renewal request payload.
type RenewRequest struct {
	ReceiverID string `json:"receiver_id"`
	Generation int64  `json:"generation"`
	AtMS       int64  `json:"at_ms"`
}

// Revocation is a receiver revocation payload.
type Revocation struct {
	ReceiverID      string `json:"receiver_id"`
	FloorGeneration int64  `json:"floor_generation"`
	AtMS            int64  `json:"at_ms"`
}

// SecretVersion is a sealed-secret version header payload.
type SecretVersion struct {
	ReceiverID     string `json:"receiver_id"`
	Name           string `json:"name"`
	Version        int64  `json:"version"`
	SealingKeyFP   string `json:"sealing_key_fp"`
	Generation     int64  `json:"generation"`
	ApprovalHash   string `json:"approval_hash"`
	CiphertextHash string `json:"ciphertext_hash"`
	ExpMS          int64  `json:"exp_ms"`
}

// SealedSecret is a sealed ciphertext payload.
type SealedSecret struct {
	ReceiverID string `json:"receiver_id"`
	Name       string `json:"name"`
	Version    int64  `json:"version"`
	Enc        Bytes  `json:"enc"`
	CT         Bytes  `json:"ct"`
}

// Satisfies reports whether every requirement holds against labels.
func Satisfies(selector []Requirement, labels map[string]string) bool {
	for _, requirement := range selector {
		value, ok := labels[requirement.Key]
		if !ok || requirement.Operator != OpIn || !slices.Contains(requirement.Values, value) {
			return false
		}
	}
	return true
}

// MatchOffer returns the first offer, in slice order, whose combined labels satisfy selector.
func MatchOffer(selector []Requirement, labels map[string]string, grants []AdapterGrant, offers []Offer) (Offer, bool) {
	for _, offer := range offers {
		index := grantIndex(grants, offer.Adapter)
		if index < 0 {
			continue
		}
		grant := grants[index]
		effective := append([]Requirement(nil), selector...)
		for key, value := range grant.Defaults {
			if !slices.ContainsFunc(selector, func(requirement Requirement) bool { return requirement.Key == key }) {
				effective = append(effective, Requirement{Key: key, Operator: OpIn, Values: []string{value}})
			}
		}
		combined := maps.Clone(labels)
		combined["platform"] = grant.Platform
		maps.Copy(combined, offer.Labels)
		if Satisfies(effective, combined) {
			return Offer{Adapter: offer.Adapter, Labels: maps.Clone(offer.Labels)}, true
		}
	}
	return Offer{}, false
}

// SortOffers orders offers by their adapter's grant index, then by their labels.
func SortOffers(grants []AdapterGrant, offers []Offer) {
	rank := func(offer Offer) int {
		if index := grantIndex(grants, offer.Adapter); index >= 0 {
			return index
		}
		return len(grants)
	}
	slices.SortStableFunc(offers, func(left, right Offer) int {
		if order := rank(left) - rank(right); order != 0 {
			return order
		}
		return strings.Compare(TermsText(left.Labels), TermsText(right.Labels))
	})
}

// TermsText joins terms as sorted k=v pairs separated by commas.
func TermsText(terms map[string]string) string {
	keys := slices.Sorted(maps.Keys(terms))
	joined := make([]string, 0, len(keys))
	for _, key := range keys {
		joined = append(joined, key+"="+terms[key])
	}
	return strings.Join(joined, ",")
}

func grantIndex(grants []AdapterGrant, name string) int {
	return slices.IndexFunc(grants, func(grant AdapterGrant) bool { return grant.Name == name })
}

func validatePayload(recordType RecordType, payload any) error {
	switch value := payload.(type) {
	case *SubmissionRecord:
		if recordType != TypeSubmission {
			break
		}
		return validateSubmission(value)
	case *Cancel:
		if recordType != TypeCancel {
			break
		}
		if err := validateID(value.RunID, "run_id"); err != nil {
			return err
		}
		if value.AtMS < 0 {
			return errors.New("invalid cancel time")
		}
		return nil
	case *StatusRecord:
		if recordType != TypeStatus {
			break
		}
		return validateStatus(value)
	case *TaskRecord:
		if recordType != TypeTask {
			break
		}
		return validateTaskRecord(value)
	case *ArtifactUpdateRecord:
		if recordType != TypeArtifactUpdate {
			break
		}
		return validateArtifactUpdate(value)
	case *Capability:
		if recordType != TypeCapability {
			break
		}
		return validateCapability(value)
	case *EnrolRequest:
		if recordType != TypeEnrolRequest {
			break
		}
		return validateEnrolRequest(value)
	case *EnrolApproval:
		if recordType != TypeEnrolApproval {
			break
		}
		return validateEnrolApproval(value)
	case *RenewRequest:
		if recordType != TypeRenewRequest {
			break
		}
		if err := validateID(value.ReceiverID, "receiver_id"); err != nil {
			return err
		}
		if value.Generation < 0 || value.AtMS < 0 {
			return errors.New("invalid renew_request")
		}
		return nil
	case *Revocation:
		if recordType != TypeRevocation {
			break
		}
		if err := validateID(value.ReceiverID, "receiver_id"); err != nil {
			return err
		}
		if value.FloorGeneration < 0 || value.AtMS < 0 {
			return errors.New("invalid revocation")
		}
		return nil
	case *SecretVersion:
		if recordType != TypeSecretVersion {
			break
		}
		return validateSecretVersion(value)
	case *SealedSecret:
		if recordType != TypeSealedSecret {
			break
		}
		if err := validateID(value.ReceiverID, "receiver_id"); err != nil {
			return err
		}
		if value.Name == "" || len(value.Enc) == 0 || len(value.CT) == 0 {
			return errors.New("invalid sealed_secret")
		}
		if value.Version < 0 {
			return errors.New("invalid sealed_secret version")
		}
		return nil
	}
	return errors.New("record type does not match payload")
}

func validateSubmission(value *SubmissionRecord) error {
	extension, lineage := &value.Wherry, &value.Wherry.Lineage
	for name, id := range map[string]string{"run_id": extension.RunID, "root_run_id": lineage.RootRunID} {
		if err := validateID(id, name); err != nil {
			return err
		}
	}
	for _, ancestor := range lineage.Ancestors {
		if err := validateID(ancestor, "ancestor"); err != nil {
			return err
		}
	}
	if extension.Turn < 1 || extension.Attempt < 1 || lineage.Depth < 0 || int64(len(lineage.Ancestors)) != lineage.Depth || lineage.Ancestors == nil || lineage.Chain == nil || extension.Labels == nil || extension.Selector == nil {
		return errors.New("invalid submission lineage")
	}
	if err := validateTerms(extension.Labels, "labels"); err != nil {
		return err
	}
	if err := validateRequirements(extension.Selector); err != nil {
		return err
	}
	if lineage.Depth == 0 {
		if lineage.ParentHash != "" || len(lineage.Chain) != 0 || extension.RunID != lineage.RootRunID {
			return errors.New("invalid root submission lineage")
		}
	} else if !validHash(lineage.ParentHash) || int64(len(lineage.Chain)) != lineage.Depth {
		return errors.New("invalid child submission lineage")
	}
	if err := validateID(extension.Pool, "pool"); err != nil {
		return err
	}
	if extension.ReplyTopic == "" || extension.DeadlineMS < 0 || extension.QueueDeadlineMS < 0 {
		return errors.New("invalid submission")
	}
	if value.Message.TaskID != extension.RunID || value.Message.ContextID != lineage.RootRunID {
		return errors.New("message taskId and contextId must be the run ID and the root run ID")
	}
	return validateMessage(&value.Message)
}

func validateStatus(value *StatusRecord) error {
	update := &value.StatusUpdate
	if err := validateTaskIDs(update.TaskID, update.ContextID); err != nil {
		return err
	}
	if err := validateTaskStatus(&update.Status); err != nil {
		return err
	}
	if err := validateMetadata(update.Metadata); err != nil {
		return err
	}
	if err := validateID(value.Wherry.ReceiverID, "receiver_id"); err != nil {
		return err
	}
	if value.Wherry.Turn < 1 || value.Wherry.Attempt < 1 || !validHash(value.Wherry.TaskHash) {
		return errors.New("invalid status")
	}
	switch value.Wherry.Kind {
	case "claimed", "started", "heartbeat", "interrupted", "rejected", "terminal":
		return nil
	default:
		return errors.New("invalid status kind")
	}
}

func validateTaskRecord(value *TaskRecord) error {
	task := &value.Task
	if err := validateTaskIDs(task.ID, task.ContextID); err != nil {
		return err
	}
	if err := validateTaskStatus(&task.Status); err != nil {
		return err
	}
	if value.Wherry.Turn < 1 || value.Wherry.Attempt < 1 || !validHash(value.Wherry.TaskHash) || task.Artifacts == nil || task.History == nil {
		return errors.New("invalid task")
	}
	for i := range task.Artifacts {
		if err := validateArtifact(&task.Artifacts[i]); err != nil {
			return err
		}
	}
	for i := range task.History {
		if err := validateMessage(&task.History[i]); err != nil {
			return err
		}
	}
	return validateMetadata(task.Metadata)
}

func validateArtifactUpdate(value *ArtifactUpdateRecord) error {
	update := &value.ArtifactUpdate
	if err := validateTaskIDs(update.TaskID, update.ContextID); err != nil {
		return err
	}
	if value.Wherry.Turn < 1 || value.Wherry.Attempt < 1 || value.Wherry.Seq < 0 {
		return errors.New("invalid artifact_update sequence")
	}
	if err := validateArtifact(&update.Artifact); err != nil {
		return err
	}
	if len(update.Artifact.Parts) != 1 {
		return errors.New("artifact_update requires exactly one part")
	}
	if part := update.Artifact.Parts[0]; part.URL != "" {
		var pointer PointerMetadata
		if part.Metadata == nil || json.Unmarshal(part.Metadata, &pointer) != nil || !validHash(pointer.SHA256) || pointer.Size <= 0 {
			return errors.New("url part metadata requires sha256 and a positive size")
		}
	}
	return validateMetadata(update.Metadata)
}

func validateTaskIDs(taskID, contextID string) error {
	if err := validateID(taskID, "task id"); err != nil {
		return err
	}
	return validateID(contextID, "context id")
}

func validateTaskStatus(value *TaskStatus) error {
	if !taskStates[value.State] {
		return errors.New("invalid task state")
	}
	if _, err := time.Parse(time.RFC3339, value.Timestamp); err != nil {
		return errors.New("status timestamp must be RFC 3339")
	}
	if value.Message != nil {
		return validateMessage(value.Message)
	}
	return nil
}

func validateCapability(value *Capability) error {
	if err := validateID(value.ReceiverID, "receiver_id"); err != nil {
		return err
	}
	if err := validateID(value.Pool, "pool"); err != nil {
		return err
	}
	if value.Capacity < 0 || value.InFlight < 0 || value.Generation < 0 || value.AtMS < 0 {
		return errors.New("invalid capability")
	}
	if err := validateReceiverLabels(value.Labels); err != nil {
		return err
	}
	if err := validateOffers(value.Offers); err != nil {
		return err
	}
	return validateRequirements(value.Selector)
}

func validateEnrolRequest(value *EnrolRequest) error {
	if err := validateID(value.ReceiverID, "receiver_id"); err != nil {
		return err
	}
	if len(value.SigningPub) != 32 || len(value.SealingPub) != 32 || value.AtMS < 0 {
		return errors.New("invalid enrol_request")
	}
	if err := validateReceiverLabels(value.Labels); err != nil {
		return err
	}
	if value.Adapters == nil {
		return errors.New("adapters are required")
	}
	names := make(map[string]bool, len(value.Adapters))
	for _, adapter := range value.Adapters {
		if err := validateAdapter(adapter.Name, adapter.Platform, names); err != nil {
			return err
		}
		if err := validateOffers(adapter.Offers); err != nil {
			return err
		}
		for _, offer := range adapter.Offers {
			if offer.Adapter != adapter.Name {
				return errors.New("offer adapter must match its detected adapter")
			}
		}
	}
	return nil
}

func validateEnrolApproval(value *EnrolApproval) error {
	if err := validateID(value.ReceiverID, "receiver_id"); err != nil {
		return err
	}
	if err := validateID(value.Pool, "pool"); err != nil {
		return err
	}
	if !validHash(value.RequestHash) || value.Generation < 0 || value.ExpMS < 0 {
		return errors.New("invalid enrol_approval")
	}
	if err := validateReceiverLabels(value.Labels); err != nil {
		return err
	}
	if value.Adapters == nil {
		return errors.New("adapters are required")
	}
	names := make(map[string]bool, len(value.Adapters))
	for _, grant := range value.Adapters {
		if err := validateAdapter(grant.Name, grant.Platform, names); err != nil {
			return err
		}
		if err := validateGrant(value.Labels, grant); err != nil {
			return err
		}
	}
	if err := validateRequirements(value.Selector); err != nil {
		return err
	}
	if value.PrevStateHash != "" && !validHash(value.PrevStateHash) {
		return errors.New("invalid previous state hash")
	}
	if value.Renewal != "auto" && value.Renewal != "manual" {
		return errors.New("invalid renewal policy")
	}
	return nil
}

func validateSecretVersion(value *SecretVersion) error {
	if err := validateID(value.ReceiverID, "receiver_id"); err != nil {
		return err
	}
	if value.Name == "" || !validFingerprint(value.SealingKeyFP) || !validHash(value.ApprovalHash) || !validHash(value.CiphertextHash) || value.Version < 0 || value.Generation < 0 || value.ExpMS < 0 {
		return errors.New("invalid secret_version")
	}
	return nil
}

func validateMessage(value *Message) error {
	if err := validateID(value.MessageID, "message_id"); err != nil {
		return err
	}
	if value.ContextID != "" {
		if err := validateID(value.ContextID, "context_id"); err != nil {
			return err
		}
	}
	if value.TaskID != "" {
		if err := validateID(value.TaskID, "task_id"); err != nil {
			return err
		}
	}
	if value.Role != RoleUser && value.Role != RoleAgent {
		return errors.New("invalid message role")
	}
	if !slices.Contains(value.Extensions, ExtensionURI) {
		return errors.New("message must declare the wherry extension")
	}
	if value.Parts == nil {
		return errors.New("message parts are required")
	}
	if err := validateParts(value.Parts); err != nil {
		return err
	}
	return validateMetadata(value.Metadata)
}

func validateArtifact(value *Artifact) error {
	if err := validateID(value.ArtifactID, "artifact_id"); err != nil {
		return err
	}
	if value.Parts == nil {
		return errors.New("artifact parts are required")
	}
	if !slices.Contains(value.Extensions, ExtensionURI) {
		return errors.New("artifact must declare the wherry extension")
	}
	if err := validateParts(value.Parts); err != nil {
		return err
	}
	return validateMetadata(value.Metadata)
}

func validateParts(parts []Part) error {
	for _, part := range parts {
		contents := 0
		for _, present := range []bool{part.Text != "", part.Raw != nil, part.URL != "", part.Data != nil} {
			if present {
				contents++
			}
		}
		if contents != 1 {
			return errors.New("part requires exactly one of text, raw, url or data")
		}
		if part.Data != nil && !part.Data.IsValid() {
			return errors.New("part data must be valid JSON")
		}
		if err := validateMetadata(part.Metadata); err != nil {
			return err
		}
	}
	return nil
}

// validateMetadata applies RFC 7493 to opaque metadata, which Go callers can build without decoding.
func validateMetadata(metadata jsontext.Value) error {
	if metadata != nil && (!metadata.IsValid() || metadata.Kind() != '{') {
		return errors.New("metadata must be a JSON object")
	}
	return nil
}

var (
	platformKey = regexp.MustCompile(`^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$`)
	operatorKey = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]*/[a-z0-9][a-z0-9_.-]*$`)
)

func validKey(key string) bool {
	switch key {
	case "platform", "runtime", "region", "zone":
		return true
	}
	return platformKey.MatchString(key) || operatorKey.MatchString(key)
}

func validateTerms(terms map[string]string, name string) error {
	for key, value := range terms {
		if !validKey(key) || value == "" {
			return fmt.Errorf("%s must have valid keys and non-empty values", name)
		}
	}
	return nil
}

func validateRequirements(requirements []Requirement) error {
	if requirements == nil {
		return errors.New("selector is required")
	}
	keys := make(map[string]bool, len(requirements))
	for _, requirement := range requirements {
		if !validKey(requirement.Key) {
			return errors.New("invalid requirement key")
		}
		if keys[requirement.Key] {
			return errors.New("duplicate requirement key")
		}
		keys[requirement.Key] = true
		if requirement.Operator != OpIn {
			return errors.New("unknown operator")
		}
		if len(requirement.Values) == 0 {
			return errors.New("requirement values are required")
		}
		values := make(map[string]bool, len(requirement.Values))
		for _, value := range requirement.Values {
			if value == "" || values[value] {
				return errors.New("requirement values must be non-empty and unique")
			}
			values[value] = true
		}
	}
	return nil
}

func validateReceiverLabels(labels map[string]string) error {
	if labels == nil {
		return errors.New("labels are required")
	}
	if _, ok := labels["platform"]; ok {
		return errors.New("receiver labels must not contain platform")
	}
	return validateTerms(labels, "labels")
}

func validateAdapter(name, platform string, seen map[string]bool) error {
	if err := validateID(name, "adapter name"); err != nil {
		return err
	}
	if seen[name] {
		return errors.New("duplicate adapter name")
	}
	seen[name] = true
	if platform == "" {
		return errors.New("adapter platform is required")
	}
	return nil
}

func validateOffers(offers []Offer) error {
	if offers == nil {
		return errors.New("offers are required")
	}
	for _, offer := range offers {
		if err := validateID(offer.Adapter, "offer adapter"); err != nil {
			return err
		}
		if len(offer.Labels) == 0 {
			return errors.New("offer labels must be non-empty")
		}
		if _, ok := offer.Labels["platform"]; ok {
			return errors.New("offer labels must not contain platform")
		}
		if err := validateTerms(offer.Labels, "offer labels"); err != nil {
			return err
		}
	}
	return nil
}

func validateGrant(labels map[string]string, grant AdapterGrant) error {
	if grant.Allowed == nil || grant.Defaults == nil {
		return errors.New("grant allowed and defaults are required")
	}
	for key, values := range grant.Allowed {
		if !validKey(key) || len(values) == 0 {
			return errors.New("allowed keys and values must be non-empty")
		}
		if _, overlap := labels[key]; overlap || key == "platform" {
			return errors.New("allowed keys must exclude platform and receiver label keys")
		}
		seen := make(map[string]bool, len(values))
		for _, value := range values {
			if value == "" || seen[value] {
				return errors.New("allowed values must be non-empty and unique")
			}
			seen[value] = true
		}
	}
	for key, value := range grant.Defaults {
		if !slices.Contains(grant.Allowed[key], value) {
			return errors.New("default must be allowed")
		}
	}
	return nil
}

func validateID(value, name string) error {
	if value == "" || value != strings.ToLower(value) {
		return fmt.Errorf("%s must be a non-empty lowercase ID", name)
	}
	return nil
}

func validHash(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}

func validFingerprint(value string) bool {
	return strings.HasPrefix(value, "sha256:") && validHash(strings.TrimPrefix(value, "sha256:"))
}
