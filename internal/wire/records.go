package wire

import (
	"crypto/ed25519"
	"errors"

	"github.com/benemon/wherry/internal/signed"
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
	owner, err := signed.Verify(encoded, value.jwsType(), keys, value)
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

// Result is a turn's final A2A task on the tenant feed.
type Result struct {
	V      int           `json:"v"`
	Task   Task          `json:"task"`
	Wherry ResultBinding `json:"wherry"`
}

// ResultBinding is the wherry block of a result.
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
	V          int     `json:"v"`
	Receiver   string  `json:"receiver"`
	Pool       string  `json:"pool"`
	Offers     []Offer `json:"offers"`
	Capacity   int64   `json:"capacity"`
	InFlight   int64   `json:"in_flight"`
	SealingKey string  `json:"sealing_key"`
	AtMS       int64   `json:"at_ms"`
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
func (*Echo) jwsType() string          { return "application/vnd.wherry.echo.v1+jws" }
func (*Advertisement) jwsType() string { return "application/vnd.wherry.advertisement.v1+jws" }
func (*StartFailure) jwsType() string  { return "application/vnd.wherry.start-failure.v1+jws" }

func (value *Submission) owner() string {
	return signed.HostOwner(value.Wherry.Tenant, value.Wherry.Host)
}
func (value *Cancel) owner() string        { return signed.HostOwner(value.Tenant, value.Host) }
func (value *RunStatus) owner() string     { return value.Wherry.Receiver }
func (value *Chunk) owner() string         { return value.Wherry.Receiver }
func (value *Result) owner() string        { return value.Wherry.Receiver }
func (value *Echo) owner() string          { return value.Receiver }
func (value *Advertisement) owner() string { return value.Receiver }
func (value *StartFailure) owner() string  { return value.Wherry.Receiver }

func (value *Submission) version() *int    { return &value.V }
func (value *Cancel) version() *int        { return &value.V }
func (value *RunStatus) version() *int     { return &value.V }
func (value *Chunk) version() *int         { return &value.V }
func (value *Result) version() *int        { return &value.V }
func (value *Echo) version() *int          { return &value.V }
func (value *Advertisement) version() *int { return &value.V }
func (value *StartFailure) version() *int  { return &value.V }

func validateIDs(values map[string]string) error {
	for name, value := range values {
		if err := validateID(value, name); err != nil {
			return err
		}
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
	gitParts := 0
	for _, part := range value.Message.Parts {
		if part.MediaType != GitMediaType {
			continue
		}
		if gitParts++; gitParts > 1 {
			return errors.New("a submission carries at most one git part")
		}
		if _, err := ParseGitPart(part); err != nil {
			return err
		}
	}
	return nil
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
	if value.Capacity < 0 || value.InFlight < 0 || value.AtMS <= 0 || value.SealingKey == "" {
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
