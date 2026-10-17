package wire

import (
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
)

// RecordType identifies a wire payload schema.
type RecordType string

const (
	TypeTask          RecordType = "task"
	TypeCancel        RecordType = "cancel"
	TypeEvent         RecordType = "event"
	TypeReply         RecordType = "reply"
	TypeTranscript    RecordType = "transcript"
	TypeCapability    RecordType = "capability"
	TypeEnrolRequest  RecordType = "enrol_request"
	TypeEnrolApproval RecordType = "enrol_approval"
	TypeRenewRequest  RecordType = "renew_request"
	TypeRevocation    RecordType = "revocation"
	TypeSecretVersion RecordType = "secret_version"
	TypeSealedSecret  RecordType = "sealed_secret"
)

// SignerKind identifies the authority behind a signing key.
type SignerKind string

const (
	SignerUser     SignerKind = "user"
	SignerReceiver SignerKind = "receiver"
)

var signerEntitlements = map[RecordType]map[SignerKind]bool{
	TypeTask:          {SignerUser: true, SignerReceiver: true},
	TypeCancel:        {SignerUser: true},
	TypeEvent:         {SignerReceiver: true},
	TypeReply:         {SignerReceiver: true},
	TypeTranscript:    {SignerReceiver: true},
	TypeCapability:    {SignerReceiver: true},
	TypeEnrolRequest:  {SignerReceiver: true},
	TypeEnrolApproval: {SignerUser: true},
	TypeRenewRequest:  {SignerReceiver: true},
	TypeRevocation:    {SignerUser: true},
	TypeSecretVersion: {SignerUser: true},
	TypeSealedSecret:  {SignerUser: true},
}

// Entitled reports whether a signer kind may sign a record type.
func Entitled(recordType RecordType, kind SignerKind) bool {
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

// Transition validates an A2A task-state transition.
func Transition(previous, next TaskState) error {
	if !taskStates[previous] || !taskStates[next] {
		return errors.New("unknown task state")
	}
	if terminalStates[previous] && previous != next {
		return fmt.Errorf("cannot transition from terminal state %s", previous)
	}
	return nil
}

// Part is the text-only MVP subset of an A2A Part.
type Part struct {
	Text string `json:"text"`
}

// Message is the MVP subset of an A2A Message.
type Message struct {
	MessageID string `json:"messageId"`
	ContextID string `json:"contextId,omitzero"`
	TaskID    string `json:"taskId,omitzero"`
	Role      Role   `json:"role"`
	Parts     []Part `json:"parts"`
}

// Artifact is the MVP subset of an A2A Artifact.
type Artifact struct {
	ArtifactID  string `json:"artifactId"`
	Name        string `json:"name,omitzero"`
	Description string `json:"description,omitzero"`
	Parts       []Part `json:"parts"`
}

// Requirement is one selector condition.
type Requirement struct {
	Key      string   `json:"key"`
	Operator string   `json:"operator"`
	Values   []string `json:"values"`
}

const OpIn = "In"

// Task is a task payload.
type Task struct {
	RunID           string            `json:"run_id"`
	Attempt         int64             `json:"attempt"`
	RootRunID       string            `json:"root_run_id"`
	Depth           int64             `json:"depth"`
	Ancestors       []string          `json:"ancestors"`
	ParentHash      string            `json:"parent_hash"`
	Chain           []Bytes           `json:"chain"`
	Pool            string            `json:"pool"`
	Labels          map[string]string `json:"labels"`
	Selector        []Requirement     `json:"selector"`
	DeadlineMS      int64             `json:"deadline_ms"`
	QueueDeadlineMS int64             `json:"queue_deadline_ms"`
	ReplyTopic      string            `json:"reply_topic"`
	CorrelationID   string            `json:"correlation_id"`
	Message         Message           `json:"message"`
}

// Cancel is a cancellation payload.
type Cancel struct {
	RunID string `json:"run_id"`
	AtMS  int64  `json:"at_ms"`
}

// Event is a task lifecycle event payload.
type Event struct {
	RunID      string    `json:"run_id"`
	Attempt    int64     `json:"attempt"`
	TaskHash   string    `json:"task_hash"`
	ReceiverID string    `json:"receiver_id"`
	AtMS       int64     `json:"at_ms"`
	State      TaskState `json:"state"`
	Kind       string    `json:"kind"`
	LogOffset  *int64    `json:"log_offset,omitzero"`
	Reason     *string   `json:"reason,omitzero"`
}

// Reply is a task reply payload.
type Reply struct {
	RunID         string     `json:"run_id"`
	Attempt       int64      `json:"attempt"`
	TaskHash      string     `json:"task_hash"`
	CorrelationID string     `json:"correlation_id"`
	State         TaskState  `json:"state"`
	Artifacts     []Artifact `json:"artifacts"`
	AtMS          int64      `json:"at_ms"`
}

// TranscriptPointer points to an oversized transcript line.
type TranscriptPointer struct {
	URL    string `json:"url"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

// Transcript is a transcript payload.
type Transcript struct {
	RunID   string             `json:"run_id"`
	Attempt int64              `json:"attempt"`
	Seq     int64              `json:"seq"`
	Line    *Bytes             `json:"line,omitzero"`
	Pointer *TranscriptPointer `json:"pointer,omitzero"`
}

// AttemptFilter drops records superseded by a later delivery attempt.
type AttemptFilter struct {
	highest map[string]int64
}

// Admit reports whether a record is current for its run.
func (filter *AttemptFilter) Admit(runID string, attempt int64) bool {
	if attempt < filter.highest[runID] {
		return false
	}
	if filter.highest == nil {
		filter.highest = make(map[string]int64)
	}
	filter.highest[runID] = attempt
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

// Narrows reports whether child retains every parent requirement with a value subset.
func Narrows(parent, child []Requirement) bool {
	for _, outer := range parent {
		found := false
		for _, inner := range child {
			if inner.Key != outer.Key || inner.Operator != outer.Operator {
				continue
			}
			found = true
			for _, value := range inner.Values {
				if !slices.Contains(outer.Values, value) {
					return false
				}
			}
			break
		}
		if !found {
			return false
		}
	}
	return true
}

// LabelsEqual reports whether two label maps are identical.
func LabelsEqual(left, right map[string]string) bool {
	return maps.Equal(left, right)
}

// KafkaKey returns the required Kafka record key for a payload.
func KafkaKey(recordType RecordType, payload any) (string, error) {
	switch value := payload.(type) {
	case *Task:
		if recordType == TypeTask {
			return value.RunID, nil
		}
	case *Cancel:
		if recordType == TypeCancel {
			return value.RunID, nil
		}
	case *Event:
		if recordType == TypeEvent {
			return value.RunID, nil
		}
	case *Reply:
		if recordType == TypeReply {
			return value.RunID, nil
		}
	case *Transcript:
		if recordType == TypeTranscript {
			return value.RunID, nil
		}
	case *Capability:
		if recordType == TypeCapability {
			return value.ReceiverID, nil
		}
	case *EnrolRequest:
		if recordType == TypeEnrolRequest {
			return value.ReceiverID, nil
		}
	case *EnrolApproval:
		if recordType == TypeEnrolApproval {
			return value.ReceiverID, nil
		}
	case *RenewRequest:
		if recordType == TypeRenewRequest {
			return value.ReceiverID, nil
		}
	case *Revocation:
		if recordType == TypeRevocation {
			return value.ReceiverID, nil
		}
	case *SecretVersion:
		if recordType == TypeSecretVersion {
			return value.ReceiverID, nil
		}
	case *SealedSecret:
		if recordType == TypeSealedSecret {
			return value.ReceiverID, nil
		}
	}
	return "", errors.New("record type does not match payload")
}

func validatePayload(recordType RecordType, payload any) error {
	switch value := payload.(type) {
	case *Task:
		if recordType != TypeTask {
			break
		}
		return validateTask(value)
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
	case *Event:
		if recordType != TypeEvent {
			break
		}
		return validateEvent(value)
	case *Reply:
		if recordType != TypeReply {
			break
		}
		return validateReply(value)
	case *Transcript:
		if recordType != TypeTranscript {
			break
		}
		return validateTranscript(value)
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

func validateTask(value *Task) error {
	for name, id := range map[string]string{"run_id": value.RunID, "root_run_id": value.RootRunID, "correlation_id": value.CorrelationID} {
		if err := validateID(id, name); err != nil {
			return err
		}
	}
	for _, ancestor := range value.Ancestors {
		if err := validateID(ancestor, "ancestor"); err != nil {
			return err
		}
	}
	if value.Attempt < 1 || value.Depth < 0 || int64(len(value.Ancestors)) != value.Depth || value.Ancestors == nil || value.Chain == nil || value.Labels == nil || value.Selector == nil {
		return errors.New("invalid task lineage")
	}
	if err := validateTerms(value.Labels, "labels"); err != nil {
		return err
	}
	if err := validateRequirements(value.Selector); err != nil {
		return err
	}
	if value.Depth == 0 {
		if value.ParentHash != "" || len(value.Chain) != 0 || value.RunID != value.RootRunID {
			return errors.New("invalid root task lineage")
		}
	} else if !validHash(value.ParentHash) || int64(len(value.Chain)) != value.Depth {
		return errors.New("invalid child task lineage")
	}
	if err := validateID(value.Pool, "pool"); err != nil {
		return err
	}
	if value.ReplyTopic == "" || value.DeadlineMS < 0 || value.QueueDeadlineMS < 0 {
		return errors.New("invalid task")
	}
	return validateMessage(&value.Message)
}

func validateEvent(value *Event) error {
	if err := validateID(value.RunID, "run_id"); err != nil {
		return err
	}
	if err := validateID(value.ReceiverID, "receiver_id"); err != nil {
		return err
	}
	if value.Attempt < 1 || !validHash(value.TaskHash) || !taskStates[value.State] || value.AtMS < 0 {
		return errors.New("invalid event")
	}
	switch value.Kind {
	case "claimed", "started", "heartbeat", "terminal":
		return nil
	default:
		return errors.New("invalid event kind")
	}
}

func validateReply(value *Reply) error {
	if err := validateID(value.RunID, "run_id"); err != nil {
		return err
	}
	if err := validateID(value.CorrelationID, "correlation_id"); err != nil {
		return err
	}
	if value.Attempt < 1 || !validHash(value.TaskHash) || !taskStates[value.State] || value.Artifacts == nil || value.AtMS < 0 {
		return errors.New("invalid reply")
	}
	for i := range value.Artifacts {
		if err := validateArtifact(&value.Artifacts[i]); err != nil {
			return err
		}
	}
	return nil
}

func validateTranscript(value *Transcript) error {
	if err := validateID(value.RunID, "run_id"); err != nil {
		return err
	}
	if value.Attempt < 1 || value.Seq < 0 {
		return errors.New("invalid transcript sequence")
	}
	if (value.Line == nil) == (value.Pointer == nil) {
		return errors.New("transcript requires exactly one of line or pointer")
	}
	if value.Pointer != nil && (value.Pointer.URL == "" || !validHash(value.Pointer.SHA256) || value.Pointer.Size <= 0) {
		return errors.New("invalid transcript pointer")
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
	if value.Name == "" || !validFingerprint(value.SealingKeyFP) || !validHash(value.CiphertextHash) || value.Version < 0 || value.Generation < 0 || value.ExpMS < 0 {
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
	if value.Parts == nil {
		return errors.New("message parts are required")
	}
	for _, part := range value.Parts {
		if part.Text == "" {
			return errors.New("text part is required")
		}
	}
	return nil
}

func validateArtifact(value *Artifact) error {
	if err := validateID(value.ArtifactID, "artifact_id"); err != nil {
		return err
	}
	if value.Parts == nil {
		return errors.New("artifact parts are required")
	}
	for _, part := range value.Parts {
		if part.Text == "" {
			return errors.New("text part is required")
		}
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
