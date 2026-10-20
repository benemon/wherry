package wire

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"regexp"
	"slices"

	"github.com/benemon/wherry/internal/signed"
	jose "github.com/go-jose/go-jose/v4"
)

// Record is a Kafka-native record: a versioned JWS signed by its owner.
type Record interface {
	jwsType() string
	owner() string
	version() *int
	validate() error
}

// SignRecord signs value at version 1 with its owner's key.
func SignRecord(value Record, key ed25519.PrivateKey) ([]byte, error) {
	*value.version() = 1
	if err := value.validate(); err != nil {
		return nil, err
	}
	return signed.Sign(value.jwsType(), value.owner(), value, key)
}

// ParseRecord verifies encoded against the current key of the owner its kid names,
// decodes it into value and requires the payload to name that owner.
func ParseRecord(encoded []byte, keys signed.Keys, value Record) error {
	return parse(signed.Verify, encoded, keys, value)
}

// ParseHistoryRecord is ParseRecord for a run's earlier records: it also
// accepts keys the owner has since rotated away.
func ParseHistoryRecord(encoded []byte, keys signed.Keys, value Record) error {
	return parse(signed.VerifyHistory, encoded, keys, value)
}

func parse(verify func([]byte, string, signed.Keys, any) (string, error), encoded []byte, keys signed.Keys, value Record) error {
	owner, err := verify(encoded, value.jwsType(), keys, value)
	if err != nil {
		return err
	}
	if *value.version() != 1 {
		return errors.New("unknown record version")
	}
	if err := value.validate(); err != nil {
		return err
	}
	if owner != value.owner() {
		return errors.New("record is signed by another owner than it names")
	}
	return nil
}

// Placement is what a run asks of its pool: an agent and the labels an
// adapter must carry (ADR-0027 Placement).
type Placement struct {
	Agent    string        `json:"agent"`
	Selector []Requirement `json:"selector"`
}

// SubmissionBinding is the wherry block of a submission.
type SubmissionBinding struct {
	Tenant          string    `json:"tenant"`
	Host            string    `json:"host"`
	Pool            string    `json:"pool"`
	Run             string    `json:"run"`
	Turn            int64     `json:"turn"`
	Placement       Placement `json:"placement"`
	DeadlineMS      int64     `json:"deadline_ms"`
	QueueDeadlineMS int64     `json:"queue_deadline_ms"`
	// Credentials, Outputs and Git are declared by a run's first turn and hold for all of its turns.
	Credentials []TaskCredential `json:"credentials,omitzero"`
	Outputs     []ExpectedOutput `json:"outputs,omitzero"`
	Git         *GitInput        `json:"git,omitzero"`
}

// TaskCredential is a credential of the host that the run's agent gets as the file File in its credentials
// directory.
type TaskCredential struct {
	Name string `json:"name"`
	File string `json:"file"`
}

// ExpectedOutput is a file the agent is asked to leave in its outputs directory; a turn that does not return a
// required one fails.
type ExpectedOutput struct {
	Name     string `json:"name"`
	Required bool   `json:"required,omitzero"`
}

// Submission is a host's task or continuation on its pool's task topic.
type Submission struct {
	V       int               `json:"v"`
	Message Message           `json:"message"`
	Wherry  SubmissionBinding `json:"wherry"`
}

// Cancel is a host's request, on the pool's event topic, to cancel a run.
type Cancel struct {
	V      int    `json:"v"`
	Tenant string `json:"tenant"`
	Host   string `json:"host"`
	Pool   string `json:"pool"`
	Run    string `json:"run"`
	Turn   int64  `json:"turn"`
}

// ReceiverBinding names the receiver, tenant, pool and attempt of a receiver
// record.
type ReceiverBinding struct {
	Receiver string `json:"receiver"`
	Tenant   string `json:"tenant"`
	Pool     string `json:"pool"`
	Run      string `json:"run"`
	Turn     int64  `json:"turn"`
	Attempt  int64  `json:"attempt"`
}

// RunStatus is a receiver's status for one attempt of a turn.
type RunStatus struct {
	V            int                   `json:"v"`
	StatusUpdate TaskStatusUpdateEvent `json:"statusUpdate"`
	Wherry       StatusBinding         `json:"wherry"`
}

// StatusBinding is the wherry block of a run status.
type StatusBinding struct {
	ReceiverBinding
	EventID   string `json:"event_id"`
	Kind      string `json:"kind"`
	TaskHash  string `json:"task_hash"`
	LogOffset *int64 `json:"log_offset,omitzero"`
}

// Chunk is a transcript artifact update on the tenant feed.
type Chunk struct {
	V              int                     `json:"v"`
	ArtifactUpdate TaskArtifactUpdateEvent `json:"artifactUpdate"`
	Wherry         ChunkBinding            `json:"wherry"`
}

// ChunkBinding is the wherry block of a chunk.
type ChunkBinding struct {
	ReceiverBinding
	TaskHash string `json:"task_hash"`
	Seq      int64  `json:"seq"`
}

// Output is one chunk of an output file, as the A2A artifact the file names, on the tenant feed.
type Output struct {
	V              int                     `json:"v"`
	ArtifactUpdate TaskArtifactUpdateEvent `json:"artifactUpdate"`
	Wherry         ResultBinding           `json:"wherry"`
}

// Result is a turn's final A2A task on the tenant feed.
type Result struct {
	V      int           `json:"v"`
	Task   Task          `json:"task"`
	Wherry ResultBinding `json:"wherry"`
}

// ResultBinding is the wherry block of a result or an output chunk.
type ResultBinding struct {
	ReceiverBinding
	TaskHash string `json:"task_hash"`
}

// Echo tells every host of a tenant where a turn was admitted.
type Echo struct {
	V               int       `json:"v"`
	Receiver        string    `json:"receiver"`
	Tenant          string    `json:"tenant"`
	Pool            string    `json:"pool"`
	Run             string    `json:"run"`
	Turn            int64     `json:"turn"`
	Placement       Placement `json:"placement"`
	TaskHash        string    `json:"task_hash"`
	MessageID       string    `json:"message_id"`
	DeadlineMS      int64     `json:"deadline_ms"`
	QueueDeadlineMS int64     `json:"queue_deadline_ms"`
}

// Advertisement is a receiver's live offers on wherry.receivers.
type Advertisement struct {
	V        int     `json:"v"`
	Receiver string  `json:"receiver"`
	Pool     string  `json:"pool"`
	Offers   []Offer `json:"offers"`
	Capacity int64   `json:"capacity"`
	InFlight int64   `json:"in_flight"`
	AtMS     int64   `json:"at_ms"`
}

const SealRequestType = "application/vnd.wherry.seal-request.v1+jws"

// SealRequest asks a run's host, on the tenant feed, to seal credentials to key for one attempt.
type SealRequest struct {
	V int `json:"v"`
	ReceiverBinding
	Key         jose.JSONWebKey `json:"key"`
	KeyID       string          `json:"key_id"`
	Credentials []string        `json:"credentials"`
	// TaskCredentials are the run's task credentials, outside the pool's terms.
	TaskCredentials []string `json:"task_credentials,omitzero"`
	ExpiryMS        int64    `json:"expiry_ms"`
}

// StartFailure records that a receiver could not start a run's attempt.
type StartFailure struct {
	V       int             `json:"v"`
	Wherry  ReceiverBinding `json:"wherry"`
	Adapter string          `json:"adapter"`
	Agent   string          `json:"agent"`
	Class   string          `json:"class"`
}

func (*Submission) jwsType() string    { return "application/vnd.wherry.submission.v1+jws" }
func (*Cancel) jwsType() string        { return "application/vnd.wherry.cancel.v1+jws" }
func (*RunStatus) jwsType() string     { return "application/vnd.wherry.status.v1+jws" }
func (*Chunk) jwsType() string         { return "application/vnd.wherry.chunk.v1+jws" }
func (*Result) jwsType() string        { return "application/vnd.wherry.result.v1+jws" }
func (*Output) jwsType() string        { return "application/vnd.wherry.output.v1+jws" }
func (*Echo) jwsType() string          { return "application/vnd.wherry.echo.v1+jws" }
func (*Advertisement) jwsType() string { return "application/vnd.wherry.advertisement.v1+jws" }
func (*StartFailure) jwsType() string  { return "application/vnd.wherry.start-failure.v1+jws" }
func (*SealRequest) jwsType() string   { return SealRequestType }

func (value *Submission) owner() string {
	return signed.HostOwner(value.Wherry.Tenant, value.Wherry.Host)
}
func (value *Cancel) owner() string        { return signed.HostOwner(value.Tenant, value.Host) }
func (value *RunStatus) owner() string     { return value.Wherry.Receiver }
func (value *Chunk) owner() string         { return value.Wherry.Receiver }
func (value *Result) owner() string        { return value.Wherry.Receiver }
func (value *Output) owner() string        { return value.Wherry.Receiver }
func (value *Echo) owner() string          { return value.Receiver }
func (value *Advertisement) owner() string { return value.Receiver }
func (value *StartFailure) owner() string  { return value.Wherry.Receiver }
func (value *SealRequest) owner() string   { return value.Receiver }

func (value *Submission) version() *int    { return &value.V }
func (value *Cancel) version() *int        { return &value.V }
func (value *RunStatus) version() *int     { return &value.V }
func (value *Chunk) version() *int         { return &value.V }
func (value *Result) version() *int        { return &value.V }
func (value *Output) version() *int        { return &value.V }
func (value *Echo) version() *int          { return &value.V }
func (value *Advertisement) version() *int { return &value.V }
func (value *StartFailure) version() *int  { return &value.V }
func (value *SealRequest) version() *int   { return &value.V }

// runID keeps run IDs to name characters: a run's transactional ID
// p.<pool>.<run> can then never equal another producer's, such as a receiver's
// p.<pool>.admission.<receiver>, and a run ID is safe in logs and job names.
var runID = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

// credentialName is the grammar of an admin name, which a pool's credentials already follow.
var credentialName = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,30}[a-z0-9])?$`)

func validateIDs(values map[string]string) error {
	for name, value := range values {
		if err := validateID(value, name); err != nil {
			return err
		}
	}
	if run, ok := values["run"]; ok && !runID.MatchString(run) {
		return errors.New("run must be lowercase letters, digits and hyphens, at most 63")
	}
	return nil
}

func (value Placement) validate() error {
	if err := validateID(value.Agent, "agent"); err != nil {
		return err
	}
	return ValidateRequirements(value.Selector)
}

func (value *Submission) validate() error {
	binding := &value.Wherry
	if err := validateIDs(map[string]string{"tenant": binding.Tenant, "host": binding.Host, "pool": binding.Pool, "run": binding.Run}); err != nil {
		return err
	}
	if binding.Turn < 1 || binding.DeadlineMS < 0 || binding.QueueDeadlineMS < 0 {
		return errors.New("invalid submission")
	}
	if err := binding.Placement.validate(); err != nil {
		return err
	}
	if value.Message.TaskID != binding.Run || value.Message.ContextID != binding.Run {
		return errors.New("message taskId and contextId must be the run ID")
	}
	if err := validateMessage(&value.Message); err != nil {
		return err
	}
	if _, err := inputBytes(value.Message.Parts); err != nil {
		return err
	}
	if binding.Turn > 1 && (binding.Credentials != nil || binding.Outputs != nil || binding.Git != nil) {
		return errors.New("only a run's first turn declares credentials, outputs and git")
	}
	files := map[string]bool{}
	for _, credential := range binding.Credentials {
		if !credentialName.MatchString(credential.Name) {
			return errors.New("credential names are at most 32 lowercase letters, digits and hyphens")
		}
		if !ValidFileName(credential.File) || files[credential.File] {
			return errors.New("credential files must be unique portable file names")
		}
		files[credential.File] = true
	}
	if len(binding.Outputs) > MaxOutputs {
		return errors.New("a submission expects at most 256 outputs")
	}
	outputs := map[string]bool{}
	for _, output := range binding.Outputs {
		if !ValidOutputName(output.Name) || outputs[output.Name] {
			return errors.New("expected outputs must be unique relative paths of portable file names")
		}
		outputs[output.Name] = true
	}
	if binding.Git == nil {
		return nil
	}
	if err := binding.Git.Validate(binding.Credentials); err != nil {
		return err
	}
	if binding.Git.HostKey != "" && slices.ContainsFunc(value.Message.Parts, func(part Part) bool {
		return (part.Raw != nil || part.Data != nil) && InputName(part) == KnownHostsFile
	}) {
		return errors.New("an input named known_hosts conflicts with the git host key")
	}
	return nil
}

// ValidateInputs checks the raw and data parts of a message: each names a unique portable file, and together they
// fit limit.
func ValidateInputs(parts []Part, limit int64) error {
	inline, err := inputBytes(parts)
	if err != nil {
		return err
	}
	if inline > limit {
		return fmt.Errorf("inline inputs are %d bytes, over the %d-byte limit", inline, limit)
	}
	return nil
}

// inputBytes checks that each raw and data part names a unique portable file, and returns their bytes together.
func inputBytes(parts []Part) (int64, error) {
	names := map[string]bool{}
	var inline int64
	for _, part := range parts {
		if part.Raw == nil && part.Data == nil {
			continue
		}
		name := InputName(part)
		if !ValidFileName(part.Filename) || !ValidFileName(name) {
			return 0, errors.New("a raw or data part needs a filename of letters, digits, '.', '_' and '-', not starting with a dot, at most 255 bytes")
		}
		if names[name] {
			return 0, errors.New("input file names must be unique")
		}
		names[name] = true
		inline += int64(len(part.Raw) + len(part.Data))
	}
	return inline, nil
}

func (value *Cancel) validate() error {
	if err := validateIDs(map[string]string{"tenant": value.Tenant, "host": value.Host, "pool": value.Pool, "run": value.Run}); err != nil {
		return err
	}
	if value.Turn < 1 {
		return errors.New("invalid cancel turn")
	}
	return nil
}

func (value ReceiverBinding) validate() error {
	if err := validateIDs(map[string]string{"receiver": value.Receiver, "tenant": value.Tenant, "pool": value.Pool, "run": value.Run}); err != nil {
		return err
	}
	if value.Turn < 1 || value.Attempt < 1 {
		return errors.New("invalid turn or attempt")
	}
	return nil
}

func (value *RunStatus) validate() error {
	binding, update := &value.Wherry, &value.StatusUpdate
	if err := binding.ReceiverBinding.validate(); err != nil {
		return err
	}
	if err := validateID(binding.EventID, "event_id"); err != nil {
		return err
	}
	if update.TaskID != binding.Run || update.ContextID != binding.Run || !validHash(binding.TaskHash) {
		return errors.New("invalid status")
	}
	if err := validateTaskStatus(&update.Status); err != nil {
		return err
	}
	if err := validateMetadata(update.Metadata); err != nil {
		return err
	}
	switch binding.Kind {
	case "claimed", "started", "heartbeat", "interrupted", "rejected", "terminal":
		return nil
	default:
		return errors.New("invalid status kind")
	}
}

func (value *Chunk) validate() error {
	update := &value.ArtifactUpdate
	if err := value.Wherry.ReceiverBinding.validate(); err != nil {
		return err
	}
	if update.TaskID != value.Wherry.Run || update.ContextID != value.Wherry.Run || value.Wherry.Seq < 0 || !validHash(value.Wherry.TaskHash) {
		return errors.New("invalid chunk")
	}
	return validateArtifactUpdate(update)
}

func (value *Output) validate() error {
	update := &value.ArtifactUpdate
	if err := value.Wherry.ReceiverBinding.validate(); err != nil {
		return err
	}
	if update.TaskID != value.Wherry.Run || update.ContextID != value.Wherry.Run || !validHash(value.Wherry.TaskHash) {
		return errors.New("invalid output")
	}
	artifact := &update.Artifact
	if !ValidOutputName(artifact.ArtifactID) || !slices.Contains(artifact.Extensions, ExtensionURI) || len(artifact.Parts) != 1 || artifact.Parts[0].Raw == nil ||
		len(artifact.Parts[0].Raw) > OutputChunk {
		return errors.New("an output chunk is one raw part of at most 256 KiB of the artifact its output names")
	}
	if err := validateParts(artifact.Parts); err != nil {
		return err
	}
	if err := validateMetadata(artifact.Metadata); err != nil {
		return err
	}
	var metadata OutputMetadata
	if update.Metadata == nil || json.Unmarshal(update.Metadata, &metadata, json.RejectUnknownMembers(true)) != nil || metadata.Index < 0 || update.Append != (metadata.Index > 0) {
		return errors.New("output chunk metadata requires its index, and append exactly from index 1")
	}
	return nil
}

func (value *Result) validate() error {
	if err := value.Wherry.ReceiverBinding.validate(); err != nil {
		return err
	}
	if value.Task.ID != value.Wherry.Run || value.Task.ContextID != value.Wherry.Run || !validHash(value.Wherry.TaskHash) {
		return errors.New("invalid result")
	}
	return validateTask(&value.Task)
}

func (value *Echo) validate() error {
	if err := validateIDs(map[string]string{"receiver": value.Receiver, "tenant": value.Tenant, "pool": value.Pool, "run": value.Run, "message_id": value.MessageID}); err != nil {
		return err
	}
	if value.Turn < 1 || !validHash(value.TaskHash) || value.DeadlineMS < 0 || value.QueueDeadlineMS < 0 {
		return errors.New("invalid echo")
	}
	return value.Placement.validate()
}

func (value *Advertisement) validate() error {
	if err := validateIDs(map[string]string{"receiver": value.Receiver, "pool": value.Pool}); err != nil {
		return err
	}
	if value.Capacity < 0 || value.InFlight < 0 || value.AtMS <= 0 {
		return errors.New("invalid advertisement")
	}
	return validateOffers(value.Offers)
}

func (value *StartFailure) validate() error {
	if err := value.Wherry.validate(); err != nil {
		return err
	}
	return validateIDs(map[string]string{"adapter": value.Adapter, "agent": value.Agent, "class": value.Class})
}

func (value *SealRequest) validate() error {
	if err := value.ReceiverBinding.validate(); err != nil {
		return err
	}
	key, ok := value.Key.Key.(*ecdsa.PublicKey)
	names := append(slices.Clone(value.Credentials), value.TaskCredentials...)
	if !ok || key.Curve != elliptic.P256() || value.KeyID != signed.SealKeyID(key) || value.ExpiryMS <= 0 || len(names) == 0 {
		return errors.New("invalid seal request")
	}
	for i, credential := range names {
		if err := validateID(credential, "credential"); err != nil {
			return err
		}
		if slices.Contains(names[:i], credential) {
			return errors.New("a seal request names each credential once")
		}
	}
	return nil
}
