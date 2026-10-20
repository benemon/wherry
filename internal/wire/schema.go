package wire

import (
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"
)

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

// TaskStatus is an A2A TaskStatus.
type TaskStatus struct {
	State   TaskState `json:"state"`
	Message *Message  `json:"message,omitzero"`
	// Timestamp is RFC 3339, as proto3 JSON writes a Timestamp.
	Timestamp string `json:"timestamp,omitzero"`
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

// PointerMetadata is the metadata of a url part that stands in for content not carried inline.
type PointerMetadata struct {
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

// CredentialMetadata is the metadata of an interrupted status that waits on a credential.
type CredentialMetadata struct {
	Credential string `json:"credential"`
}

// OutputMetadata is the metadata of an output chunk's artifact update.
type OutputMetadata struct {
	Index int64 `json:"index"`
}

// ReturnedOutput is an output a turn returned, as a result's task metadata lists it; SHA256 and Size are of the
// bytes its chunks carry.
type ReturnedOutput struct {
	Name   string `json:"name"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

// UnreturnedOutput is an output a turn did not return, and why.
type UnreturnedOutput struct {
	Name   string `json:"name"`
	Size   int64  `json:"size,omitzero"`
	Reason string `json:"reason"`
}

// OutputChunk bounds the data of one output chunk, below a record's limit.
const OutputChunk = 256 << 10

// MaxParts bounds the parts of a message.
const MaxParts = 256

// MaxLine bounds the transcript line a chunk carries; a longer one is not stored.
const MaxLine = 512 << 10

// MaxOutputs bounds the outputs a submission expects and the entries a receiver collects.
const MaxOutputs = 256

// MaxText bounds a result's text, and each status text and result metadata string, once redacted.
const MaxText = 64 << 10

// MaxAgentResult bounds the agent's result record, less its result, that a result's metadata passes through.
const MaxAgentResult = 16 << 10

// ResultMetadata is a result's task metadata: where its turn ran, and the outputs it returned and did not.
type ResultMetadata struct {
	ReceiverID  string             `json:"receiver_id"`
	Pool        string             `json:"pool"`
	Adapter     string             `json:"adapter,omitempty"`
	Labels      map[string]string  `json:"labels,omitempty"`
	Job         string             `json:"job,omitempty"`
	Outputs     []ReturnedOutput   `json:"outputs,omitempty"`
	NotReturned []UnreturnedOutput `json:"not_returned,omitempty"`
}

// fileName is a portable file name: no separator, no leading dot, nothing a shell or a template interprets.
var fileName = regexp.MustCompile(`^[A-Za-z0-9_-][A-Za-z0-9._-]*$`)

// ValidFileName reports whether name is a portable file name of at most 255 bytes: letters, digits, '.', '_' and
// '-', not starting with a dot.
func ValidFileName(name string) bool {
	return len(name) <= 255 && fileName.MatchString(name)
}

// ValidOutputName reports whether name is a relative path of at most 512 bytes whose every slash-separated
// segment is a portable file name.
func ValidOutputName(name string) bool {
	return len(name) <= 512 && !slices.ContainsFunc(strings.Split(name, "/"), func(segment string) bool { return !ValidFileName(segment) })
}

// InputName is the file name a raw or data part is delivered as: its filename, with .json added for a data part.
func InputName(part Part) string {
	if part.Data != nil {
		return part.Filename + ".json"
	}
	return part.Filename
}

// CommitMetadata is the metadata of a started status for a run with a git context.
type CommitMetadata struct {
	GitCommit string `json:"git_commit"`
}

// GitMediaType is the media type of a url part that names a git repository (ADR-0024).
const GitMediaType = "application/vnd.wherry.git"

// GitMetadata is the metadata of a git context part.
type GitMetadata struct {
	Ref  string `json:"ref"`
	Path string `json:"path,omitzero"`
}

// GitContext is a validated git context part; Path has its default applied.
type GitContext struct {
	URL  string
	Ref  string
	Path string
}

var (
	// Refs and paths reach git as arguments, so neither may start with a dash.
	gitRef     = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._/-]*$`)
	gitSegment = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._-]*$`)
)

// ParseGitPart validates a git context part.
func ParseGitPart(part Part) (GitContext, error) {
	if part.URL == "" {
		return GitContext{}, errors.New("git part must be a url part")
	}
	// Error text from url.Parse quotes the URL, so it is never returned.
	parsed, err := url.Parse(part.URL)
	if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "ssh") || parsed.Host == "" {
		return GitContext{}, errors.New("git url must be an https or ssh url with a host")
	}
	if parsed.User != nil {
		if parsed.Scheme != "ssh" {
			return GitContext{}, errors.New("https git url must not carry user information")
		}
		if _, ok := parsed.User.Password(); ok {
			return GitContext{}, errors.New("ssh git url must not carry a password")
		}
		if parsed.User.Username() != "git" {
			return GitContext{}, errors.New("ssh git url user must be git")
		}
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" || parsed.ForceQuery {
		return GitContext{}, errors.New("git url must not carry a query or fragment")
	}
	var metadata GitMetadata
	if part.Metadata == nil || json.Unmarshal(part.Metadata, &metadata, json.RejectUnknownMembers(true)) != nil || metadata.Ref == "" {
		return GitContext{}, errors.New("git part metadata requires a ref")
	}
	if !gitRef.MatchString(metadata.Ref) || strings.Contains(metadata.Ref, "..") || strings.Contains(metadata.Ref, "//") ||
		strings.HasSuffix(metadata.Ref, "/") || strings.HasSuffix(metadata.Ref, ".") || strings.HasSuffix(metadata.Ref, ".lock") {
		return GitContext{}, errors.New("git ref must be a branch, tag or commit name")
	}
	path := metadata.Path
	if path == "" {
		path = strings.TrimSuffix(parsed.Path[strings.LastIndex(parsed.Path, "/")+1:], ".git")
	}
	for _, segment := range strings.Split(path, "/") {
		if !gitSegment.MatchString(segment) {
			return GitContext{}, errors.New("git path must be a relative path without . or .. segments")
		}
	}
	return GitContext{URL: part.URL, Ref: metadata.Ref, Path: path}, nil
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

// TermsText joins terms as sorted k=v pairs separated by commas.
func TermsText(terms map[string]string) string {
	keys := slices.Sorted(maps.Keys(terms))
	joined := make([]string, 0, len(keys))
	for _, key := range keys {
		joined = append(joined, key+"="+terms[key])
	}
	return strings.Join(joined, ",")
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
	if len(value.Parts) > MaxParts {
		return errors.New("a message has at most 256 parts")
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
