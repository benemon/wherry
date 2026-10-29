package wire

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"time"

	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/pluginsdk"
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

// ParseHistory is ParseRecord for a feed record the broker appended at appended,
// which also verifies against a key its owner retired after that.
func ParseHistory(appended time.Time) func([]byte, signed.Keys, Record) error {
	return func(encoded []byte, keys signed.Keys, value Record) error {
		return parse(func(encoded []byte, typ string, keys signed.Keys, dst any) (string, error) {
			return signed.VerifyHistory(encoded, typ, keys, appended, dst)
		}, encoded, keys, value)
	}
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

// Placement is what a run asks of its pool: an agent, the labels a profile
// must carry, and the model the agent runs, empty for an agent whose terms
// name no models.
type Placement struct {
	Agent    string        `json:"agent"`
	Selector []Requirement `json:"selector"`
	Model    string        `json:"model,omitzero"`
}

// SubmissionBinding is the wherry block of a submission.
type SubmissionBinding struct {
	Tenant          string           `json:"tenant"`
	Host            string           `json:"host"`
	Pool            string           `json:"pool"`
	Run             string           `json:"run"`
	Placement       Placement        `json:"placement"`
	DeadlineMS      int64            `json:"deadline_at_ms"`
	QueueDeadlineMS int64            `json:"queue_deadline_at_ms"`
	Credentials     []TaskCredential `json:"credentials,omitzero"`
	// Unsupplied names the agent's optional credentials this host has no source for, which the run goes without.
	Unsupplied []string         `json:"unsupplied_credentials,omitzero"`
	Outputs    []ExpectedOutput `json:"outputs,omitzero"`
	Git        *GitInput        `json:"git,omitzero"`
	S3Inputs   []S3Input        `json:"s3_inputs,omitzero"`
}

// TaskCredential is a credential of the host that the run's agent gets at exactly one target: the environment variable
// Env, or the file File in its credentials directory.
type TaskCredential struct {
	Name string `json:"name"`
	Env  string `json:"env,omitzero"`
	File string `json:"file,omitzero"`
}

// Target names where a credential is delivered.
func (c TaskCredential) Target() string {
	if c.Env != "" {
		return "environment variable " + c.Env
	}
	return "file " + c.File
}

var envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// ValidateCredentials checks a list of credentials: unique valid names, each with exactly one target, an environment
// variable no plugin reserves or a portable file name, and no two sharing a target. Errors never quote a value.
func ValidateCredentials(credentials []TaskCredential) error {
	for i, credential := range credentials {
		if !ValidName(credential.Name) {
			return errors.New("credential names are at most 32 lowercase letters, digits and hyphens")
		}
		switch {
		case (credential.Env == "") == (credential.File == ""):
			return fmt.Errorf("credential %s needs exactly one of env and file", credential.Name)
		case credential.Env != "" && (!envName.MatchString(credential.Env) || pluginsdk.ReservedEnv(credential.Env)):
			return fmt.Errorf("credential %s: env must match [A-Za-z_][A-Za-z0-9_]* and not be a variable plugins set", credential.Name)
		case credential.File != "" && !ValidFileName(credential.File):
			return fmt.Errorf("credential %s: file must be a portable file name", credential.Name)
		}
		for _, other := range credentials[:i] {
			if other.Name == credential.Name {
				return fmt.Errorf("credential %s is listed twice", credential.Name)
			}
			if other.Target() == credential.Target() {
				return fmt.Errorf("credentials %s and %s share a target", other.Name, credential.Name)
			}
		}
	}
	return nil
}

// ExpectedOutput is a file the agent is asked to leave in its outputs directory; a run that does not return a
// required one fails. Storage StorageS3 returns it through the pool's bucket instead of the feed.
type ExpectedOutput struct {
	Name     string `json:"name"`
	Required bool   `json:"required,omitzero"`
	Storage  string `json:"storage,omitzero"`
}

// Submission is a host's task on its pool's task topic.
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
}

// ReceiverBinding names the receiver, tenant, pool and attempt of a receiver
// record.
type ReceiverBinding struct {
	Receiver string `json:"receiver"`
	Tenant   string `json:"tenant"`
	Pool     string `json:"pool"`
	Run      string `json:"run"`
	Attempt  int64  `json:"attempt"`
}

// RunStatus is a receiver's status for one attempt of a run.
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
	// TranscriptChunks is the number of transcript chunks the attempt wrote, on a terminal status.
	TranscriptChunks int64 `json:"transcript_chunks,omitzero"`
}

// Chunk is a transcript artifact update on the tenant transcript topic.
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

// Output is one chunk of an output file, as the A2A artifact the file names, on the tenant transcript topic.
type Output struct {
	V              int                     `json:"v"`
	ArtifactUpdate TaskArtifactUpdateEvent `json:"artifactUpdate"`
	Wherry         ResultBinding           `json:"wherry"`
}

// Result is a run's final A2A task on the tenant feed.
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

// Echo tells every host of a tenant where a run was admitted.
type Echo struct {
	V         int       `json:"v"`
	Receiver  string    `json:"receiver"`
	Tenant    string    `json:"tenant"`
	Pool      string    `json:"pool"`
	Run       string    `json:"run"`
	Placement Placement `json:"placement"`
	TaskHash  string    `json:"task_hash"`
	MessageID string    `json:"message_id"`
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
	// S3Inputs and S3Outputs name the run's S3 inputs and outputs the seal is to carry presigned URLs for.
	S3Inputs  []string `json:"s3_inputs,omitzero"`
	S3Outputs []string `json:"s3_outputs,omitzero"`
	ExpiryMS  int64    `json:"expiry_at_ms"`
}

var startFailureClasses = []string{"dispatch", "platform", "input-too-large"}

// StartFailure records that a receiver could not start a run's attempt.
type StartFailure struct {
	V        int             `json:"v"`
	Wherry   ReceiverBinding `json:"wherry"`
	Profile  string          `json:"profile"`
	Agent    string          `json:"agent"`
	Class    string          `json:"class"`
	TaskHash string          `json:"task_hash"`
}

// Progress is the latest transcript offset a receiver committed for a run it holds, keyed by the run on the receiver's
// progress topic. It is unsigned: the topic has one writer, which Kafka authenticates.
type Progress struct {
	V         int    `json:"v"`
	Tenant    string `json:"tenant"`
	TaskHash  string `json:"task_hash"`
	Attempt   int64  `json:"attempt"`
	LogOffset int64  `json:"log_offset"`
}

// EncodeProgress encodes value at version 1.
func EncodeProgress(value Progress) ([]byte, error) {
	value.V = 1
	if err := value.validate(); err != nil {
		return nil, err
	}
	return json.Marshal(value, json.Deterministic(true))
}

// DecodeProgress decodes a progress record.
func DecodeProgress(encoded []byte) (Progress, error) {
	var value Progress
	if err := json.Unmarshal(encoded, &value, signed.Tolerant); err != nil {
		return Progress{}, err
	}
	if value.V != 1 {
		return Progress{}, errors.New("unknown record version")
	}
	return value, value.validate()
}

func (value Progress) validate() error {
	if err := validateID(value.Tenant, "tenant"); err != nil {
		return err
	}
	if !ValidHash(value.TaskHash) || value.Attempt < 1 || value.LogOffset < 0 {
		return errors.New("invalid progress")
	}
	return nil
}

// The JWS types of the tenant feed's and transcript topic's records, which a reader routes each record by.
const (
	StatusType = "application/vnd.wherry.status.v1+jws"
	ChunkType  = "application/vnd.wherry.chunk.v1+jws"
	ResultType = "application/vnd.wherry.result.v1+jws"
	OutputType = "application/vnd.wherry.output.v1+jws"
	EchoType   = "application/vnd.wherry.echo.v1+jws"
)

// StartFailureType is the JWS type of a start failure, which a pool events reader routes each record by.
const StartFailureType = "application/vnd.wherry.start-failure.v1+jws"

func (*Submission) jwsType() string    { return "application/vnd.wherry.submission.v1+jws" }
func (*Cancel) jwsType() string        { return "application/vnd.wherry.cancel.v1+jws" }
func (*RunStatus) jwsType() string     { return StatusType }
func (*Chunk) jwsType() string         { return ChunkType }
func (*Result) jwsType() string        { return ResultType }
func (*Output) jwsType() string        { return OutputType }
func (*Echo) jwsType() string          { return EchoType }
func (*Advertisement) jwsType() string { return "application/vnd.wherry.advertisement.v1+jws" }
func (*StartFailure) jwsType() string  { return StartFailureType }
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

var namePattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,30}[a-z0-9])?$`)

// ValidName reports whether name follows the grammar of the names wherry admin issues, which a pool's credentials
// follow too.
func ValidName(name string) bool { return namePattern.MatchString(name) }

// TaskHash is the SHA-256, in lowercase hex, of a task record's encoded value.
func TaskHash(encoded []byte) string {
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

// RandomID is prefix and 128 random bits in hex.
func RandomID(prefix string) string {
	id := make([]byte, 16)
	rand.Read(id) // never fails
	return prefix + hex.EncodeToString(id)
}

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
	if binding.DeadlineMS < 0 || binding.QueueDeadlineMS < 0 {
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
	if value.Message.Role != RoleUser {
		return errors.New("a submission's message role must be ROLE_USER")
	}
	if _, err := inputBytes(value.Message.Parts); err != nil {
		return err
	}
	if err := ValidateCredentials(binding.Credentials); err != nil {
		return err
	}
	for i, name := range binding.Unsupplied {
		if !ValidName(name) || slices.Contains(binding.Unsupplied[:i], name) {
			return errors.New("unsupplied credentials are unique credential names")
		}
	}
	if len(binding.Outputs) > MaxOutputs {
		return errors.New("a submission expects at most 256 outputs")
	}
	outputs := map[string]bool{}
	for _, output := range binding.Outputs {
		if !ValidOutputName(output.Name) || outputs[output.Name] {
			return errors.New("expected outputs must be unique relative paths of portable file names")
		}
		if output.Name == ReplyFile {
			return errors.New("expected output " + ReplyFile + " is reserved for the run's reply")
		}
		if output.Storage != "" && output.Storage != StorageS3 {
			return errors.New("an expected output's storage is s3 or absent")
		}
		outputs[output.Name] = true
	}
	if len(binding.S3Inputs)+len(value.Message.Parts) > MaxParts {
		return errors.New("a run's parts and S3 inputs are at most 256")
	}
	inputs := map[string]bool{}
	for _, part := range value.Message.Parts {
		if part.Raw != nil || part.Data != nil {
			inputs[InputName(part)] = true
		}
	}
	for _, input := range binding.S3Inputs {
		if !ValidFileName(input.Name) || inputs[input.Name] {
			return errors.New("S3 inputs need unique portable file names, distinct from the run's input files")
		}
		if input.Size < 0 || input.Size > MaxS3Object || !ValidHash(input.SHA256) {
			return errors.New("an S3 input has a size of at most 5 GiB and a lowercase hex SHA-256")
		}
		inputs[input.Name] = true
	}
	if binding.Git == nil {
		return nil
	}
	if err := binding.Git.Validate(binding.Credentials); err != nil {
		return err
	}
	if binding.Git.HostKey != "" && inputs[KnownHostsFile] {
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
	return nil
}

func (value ReceiverBinding) validate() error {
	if err := validateIDs(map[string]string{"receiver": value.Receiver, "tenant": value.Tenant, "pool": value.Pool, "run": value.Run}); err != nil {
		return err
	}
	if value.Attempt < 1 {
		return errors.New("invalid attempt")
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
	if update.TaskID != binding.Run || update.ContextID != binding.Run || !ValidHash(binding.TaskHash) ||
		binding.TranscriptChunks < 0 || binding.TranscriptChunks > 0 && binding.Kind != "terminal" {
		return errors.New("invalid status")
	}
	if err := validateTaskStatus(&update.Status); err != nil {
		return err
	}
	if err := validateMetadata(update.Metadata); err != nil {
		return err
	}
	switch binding.Kind {
	case "claimed", "started", "rejected", "terminal":
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
	if update.TaskID != value.Wherry.Run || update.ContextID != value.Wherry.Run || value.Wherry.Seq < 0 || !ValidHash(value.Wherry.TaskHash) {
		return errors.New("invalid chunk")
	}
	return validateArtifactUpdate(update)
}

func (value *Output) validate() error {
	update := &value.ArtifactUpdate
	if err := value.Wherry.ReceiverBinding.validate(); err != nil {
		return err
	}
	if update.TaskID != value.Wherry.Run || update.ContextID != value.Wherry.Run || !ValidHash(value.Wherry.TaskHash) {
		return errors.New("invalid output")
	}
	artifact := &update.Artifact
	if !(ValidOutputName(artifact.ArtifactID) || artifact.ArtifactID == ReplyArtifact) || !slices.Contains(artifact.Extensions, ExtensionURI) || len(artifact.Parts) != 1 || artifact.Parts[0].Raw == nil ||
		len(artifact.Parts[0].Raw) > OutputChunk {
		return errors.New("an output chunk is one raw part of at most 256 KiB of the artifact its output or the reply names")
	}
	if err := validateParts(artifact.Parts); err != nil {
		return err
	}
	if err := validateMetadata(artifact.Metadata); err != nil {
		return err
	}
	var metadata OutputMetadata
	if update.Metadata == nil || json.Unmarshal(update.Metadata, &metadata, signed.Tolerant) != nil || metadata.Index < 0 || update.Append != (metadata.Index > 0) {
		return errors.New("output chunk metadata requires its index, and append exactly from index 1")
	}
	return nil
}

func (value *Result) validate() error {
	if err := value.Wherry.ReceiverBinding.validate(); err != nil {
		return err
	}
	if value.Task.ID != value.Wherry.Run || value.Task.ContextID != value.Wherry.Run || !ValidHash(value.Wherry.TaskHash) {
		return errors.New("invalid result")
	}
	return validateTask(&value.Task)
}

func (value *Echo) validate() error {
	if err := validateIDs(map[string]string{"receiver": value.Receiver, "tenant": value.Tenant, "pool": value.Pool, "run": value.Run, "message_id": value.MessageID}); err != nil {
		return err
	}
	if !ValidHash(value.TaskHash) {
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
	if !ValidHash(value.TaskHash) || !slices.Contains(startFailureClasses, value.Class) {
		return errors.New("invalid start failure")
	}
	return validateIDs(map[string]string{"profile": value.Profile, "agent": value.Agent})
}

func (value *SealRequest) validate() error {
	if err := value.ReceiverBinding.validate(); err != nil {
		return err
	}
	key, ok := value.Key.Key.(*ecdsa.PublicKey)
	names := append(slices.Clone(value.Credentials), value.TaskCredentials...)
	if !ok || key.Curve != elliptic.P256() || value.KeyID != signed.SealKeyID(key) || value.ExpiryMS <= 0 || len(names)+len(value.S3Inputs)+len(value.S3Outputs) == 0 {
		return errors.New("invalid seal request")
	}
	for i, input := range value.S3Inputs {
		if !ValidFileName(input) || slices.Contains(value.S3Inputs[:i], input) {
			return errors.New("a seal request names unique S3 inputs")
		}
	}
	for i, output := range value.S3Outputs {
		if !ValidOutputName(output) || slices.Contains(value.S3Outputs[:i], output) {
			return errors.New("a seal request names unique S3 outputs")
		}
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
