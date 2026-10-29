package wire

import (
	"cmp"
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/benemon/wherry/internal/signed"
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

// ValidTaskState reports whether state is an A2A task state.
func ValidTaskState(state TaskState) bool {
	return taskStates[state]
}

// Terminal reports whether an A2A task state ends a task.
func Terminal(state TaskState) bool {
	return terminalStates[state]
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
// SealWait is how long a receiver waits for the host's seal after it asks for one.
const SealWait = 5 * time.Minute

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

// ReferenceMediaType is the media type of a data part that stands in for content a record does not carry.
const ReferenceMediaType = "application/vnd.wherry.reference.v1+json"

// Reference is the data of a ReferenceMediaType part: the SHA-256 and size of the content, and why it is not stored.
type Reference struct {
	SHA256   string `json:"sha256"`
	Size     int64  `json:"size"`
	Unstored string `json:"unstored"`
}

// The reasons a Reference gives for content not stored: an input already carried by the submission, or a transcript
// line over MaxLine.
const (
	UnstoredInput    = "input"
	UnstoredOversize = "oversize"
)

// OutputMetadata is the metadata of an output chunk's artifact update.
type OutputMetadata struct {
	Index int64 `json:"index"`
}

// ReturnedOutput is an output a turn returned, as a result's task metadata lists it; SHA256 and Size are of the
// bytes its chunks carry, or for an S3 output of the bytes the receiver uploaded to Key in its pool's bucket.
type ReturnedOutput struct {
	Name   string `json:"name"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
	Key    string `json:"key,omitzero"`
}

// UnreturnedOutput is an output a run did not return, and why.
type UnreturnedOutput struct {
	Name   string `json:"name"`
	Size   int64  `json:"size,omitzero"`
	Reason string `json:"reason"`
}

// OutputMediaType is the media type of an output chunk's part.
const OutputMediaType = "application/octet-stream"

// OutputChunk bounds the data of one output chunk, below a record's limit.
const OutputChunk = 256 << 10

// MaxParts bounds the parts of a message.
const MaxParts = 256

// MaxLine bounds the transcript line a chunk carries; a longer one is not stored.
const MaxLine = 512 << 10

// MaxOutputs bounds the outputs a submission expects and the entries a receiver collects.
const MaxOutputs = 256

// MaxText bounds a result's text and a status's text, once redacted; a longer reply travels as reply chunks.
const MaxText = 64 << 10

// CutUTF8 is s cut to at most n bytes, at a rune boundary.
func CutUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// ResultMetadata is a result's task metadata: where its run ran, the outputs it returned and did not, and its reply
// chunks.
type ResultMetadata struct {
	ReceiverID  string             `json:"receiver_id"`
	Pool        string             `json:"pool"`
	Profile     string             `json:"profile,omitempty"`
	Labels      map[string]string  `json:"labels,omitempty"`
	Job         string             `json:"job,omitempty"`
	Outputs     []ReturnedOutput   `json:"outputs,omitempty"`
	S3Outputs   []ReturnedOutput   `json:"s3_outputs,omitempty"`
	NotReturned []UnreturnedOutput `json:"not_returned,omitempty"`
	Reply       *ReturnedReply     `json:"reply,omitzero"`
}

// ReturnedReply is a reply over MaxText that a turn returned as reply chunks; SHA256 and Size are of the bytes the
// chunks carry, and Cut is set when the redacted reply was longer than its pool's reply limit.
type ReturnedReply struct {
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
	Cut    bool   `json:"cut,omitzero"`
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

// S3Input is a file the run's host uploaded to the run's prefix in its pool's bucket, of Size bytes and SHA-256
// SHA256, which the plugin fetches into the input directory under Name.
type S3Input struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// StorageS3 is the storage of an expected output that returns through its pool's bucket.
const StorageS3 = "s3"

// MaxS3Object bounds an S3 input or output: 5 GiB, the S3 single-PUT maximum.
const MaxS3Object = 5 << 30

// S3Prefix is the prefix of a run's objects in its pool's bucket.
func S3Prefix(tenant, run string) string { return "wherry/" + tenant + "/" + run + "/" }

// S3InputKey is the key of a run's S3 input.
func S3InputKey(tenant, run, name string) string { return S3Prefix(tenant, run) + "inputs/" + name }

// S3OutputKey is the key of a run's S3 output.
func S3OutputKey(tenant, run, name string) string {
	return S3Prefix(tenant, run) + "outputs/" + name
}

// GitInput is the repository a run's agent clones itself: Remote at Ref into Path, with the task credential
// Credential and, for an ssh remote, the OpenSSH known_hosts lines HostKey.
type GitInput struct {
	Remote     string `json:"remote"`
	Ref        string `json:"ref"`
	Path       string `json:"path,omitzero"`
	Credential string `json:"credential,omitzero"`
	HostKey    string `json:"host_key,omitzero"`
}

// Bounds of a git input's fields, in bytes; a path is bounded as an output name is.
const (
	MaxGitRemote  = 2048
	MaxGitRef     = 255
	MaxGitHostKey = 8 << 10
)

// KnownHostsFile is the input file a git input's host key is delivered as.
const KnownHostsFile = "known_hosts"

// GitCredentialFile is the credentials file a git input's credential is delivered as.
const GitCredentialFile = "git-credential"

// OutcomeDir is reserved beneath a run's output directory, and OutcomeFile in it is where the agent reports how the
// run ended. No output is collected from OutcomeDir.
const (
	OutcomeDir  = ".wherry"
	OutcomeFile = OutcomeDir + "/outcome.json"
)

// ReplyArtifact is the artifact ID of a reply's chunks. It lies beneath OutcomeDir, so no collected output takes it.
const ReplyArtifact = OutcomeDir + "/reply"

// ReplyFile is where an MCP host writes a run's reply among its outputs, so no output takes its name.
const ReplyFile = "reply.txt"

// ResultArtifact and TranscriptArtifact are the artifact IDs of a result's text and of a run's transcript. A result's
// reference artifacts take each output's name as its ID, and A2A requires a task's artifact IDs unique, so no output
// takes either name.
const (
	ResultArtifact     = "result"
	TranscriptArtifact = "transcript"
)

// ReservedArtifact reports whether name is an artifact ID a result takes for itself, which no expected output names
// and no receiver collects.
func ReservedArtifact(name string) bool {
	return name == ResultArtifact || name == TranscriptArtifact
}

// StreamedReference is the data of the reference part in a result's reference artifact for an output or the reply: the
// SHA-256 and size of the bytes its chunks carry. The transcript's reference artifact has an empty object: an adopter
// cannot hash or count an earlier attempt's lines, and a reader reads the transcript to its lastChunk chunk.
type StreamedReference struct {
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

// GitPatch and GitBundle are the outputs a run asking for its git changes back expects: a patch series, or a bundle.
const (
	GitPatch  = "changes.patch"
	GitBundle = "changes.bundle"
)

var (
	// The agent pastes the remote, ref and path into shell commands and git arguments, so none carries a quote,
	// a space or a shell metacharacter, and neither a ref nor a path segment starts with a dash.
	gitRemote  = regexp.MustCompile(`^(https|ssh)://[A-Za-z0-9._~%+@:/-]+$`)
	gitRef     = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._/-]*$`)
	gitSegment = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._-]*$`)
	vcsHost    = regexp.MustCompile(`^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$`)
	vcsPort    = regexp.MustCompile(`^[1-9][0-9]{0,4}$`)
)

// ValidVCSHost reports whether host is a lowercase host name, optionally with a port of 1 to 65535.
func ValidVCSHost(host string) bool {
	host, port, hasPort := strings.Cut(host, ":")
	if !vcsHost.MatchString(host) {
		return false
	}
	if !hasPort {
		return true
	}
	number, err := strconv.Atoi(port)
	return vcsPort.MatchString(port) && err == nil && number <= 65535
}

// VCSHost is the lowercase host of the remote, with its port when that is not the scheme's default.
func (g GitInput) VCSHost() string {
	parsed, err := url.Parse(g.Remote)
	if err != nil {
		return ""
	}
	return hostOf(parsed)
}

func hostOf(remote *url.URL) string {
	host, port := strings.ToLower(remote.Hostname()), remote.Port()
	if port == "" || remote.Scheme == "https" && port == "443" || remote.Scheme == "ssh" && port == "22" {
		return host
	}
	return host + ":" + port
}

// Checkout is the path the agent clones into: Path, or the last segment of the remote's path without .git.
func (g GitInput) Checkout() string {
	if g.Path != "" {
		return g.Path
	}
	return strings.TrimSuffix(g.Remote[strings.LastIndex(g.Remote, "/")+1:], ".git")
}

// Validate checks a git input against the task credentials its submission declares. Errors never quote a value.
func (g *GitInput) Validate(credentials []TaskCredential) error {
	if len(g.Remote) > MaxGitRemote || !gitRemote.MatchString(g.Remote) {
		return errors.New("git remote must be an https or ssh url of at most 2048 letters, digits and . _ ~ % + @ : / -")
	}
	parsed, err := url.Parse(g.Remote)
	if err != nil || !ValidVCSHost(hostOf(parsed)) || parsed.Path == "" {
		return errors.New("git remote must name a host and a repository path")
	}
	if parsed.User != nil {
		if parsed.Scheme != "ssh" {
			return errors.New("https git remote must not carry user information")
		}
		if _, ok := parsed.User.Password(); ok {
			return errors.New("ssh git remote must not carry a password")
		}
	}
	if len(g.Ref) > MaxGitRef || !gitRef.MatchString(g.Ref) || strings.Contains(g.Ref, "..") || strings.Contains(g.Ref, "//") ||
		strings.HasSuffix(g.Ref, "/") || strings.HasSuffix(g.Ref, ".") || strings.HasSuffix(g.Ref, ".lock") {
		return errors.New("git ref must be a branch, tag or commit name of at most 255 bytes")
	}
	checkout := g.Checkout()
	if len(checkout) > 512 || slices.ContainsFunc(strings.Split(checkout, "/"), func(segment string) bool { return !gitSegment.MatchString(segment) }) {
		return errors.New("git path must be a relative path of at most 512 bytes without . or .. segments; name one when the repository's name is not a portable file name")
	}
	if g.Credential != "" && !slices.ContainsFunc(credentials, func(credential TaskCredential) bool { return credential.Name == g.Credential && credential.File != "" }) {
		return errors.New("git credential must be one of the submission's task credentials, delivered as a file")
	}
	switch {
	case parsed.Scheme == "ssh" && g.HostKey == "":
		return errors.New("ssh git remote requires host_key")
	case parsed.Scheme == "https" && g.HostKey != "":
		return errors.New("host_key applies only to an ssh git remote")
	case g.HostKey != "":
		return checkHostKey(g.HostKey, knownHost(parsed))
	}
	return nil
}

// Origin is the remote's scheme and lowercase host, with its port when it names one: the URL a credential is scoped to.
func (g GitInput) Origin() string {
	parsed, err := url.Parse(g.Remote)
	if err != nil {
		return ""
	}
	return parsed.Scheme + "://" + strings.ToLower(parsed.Host)
}

// knownHost is the name known_hosts gives an ssh remote's host: the host, or [host]:port for a port other than 22.
func knownHost(remote *url.URL) string {
	host := strings.ToLower(remote.Hostname())
	if port := remote.Port(); port != "" && port != "22" {
		return "[" + host + "]:" + port
	}
	return host
}

// checkHostKey accepts at most MaxGitHostKey bytes of printable ASCII lines, each blank, a comment or a known_hosts
// entry, an optional marker then host patterns, key type and key, with at least one entry and every entry naming host
// among its patterns and holding no wildcard, so ssh trusts the key for that host alone.
func checkHostKey(key, host string) error {
	malformed := errors.New("git host_key must be at most 8 KiB of OpenSSH known_hosts lines")
	if len(key) > MaxGitHostKey {
		return malformed
	}
	entries := 0
	for line := range strings.Lines(key) {
		line = strings.TrimSuffix(line, "\n")
		if strings.ContainsFunc(line, func(r rune) bool { return r < ' ' || r > '~' }) {
			return malformed
		}
		fields := strings.Fields(line)
		if len(fields) == 0 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		if strings.HasPrefix(fields[0], "@") {
			fields = fields[1:]
		}
		if len(fields) < 3 {
			return malformed
		}
		patterns := strings.Split(strings.ToLower(fields[0]), ",")
		if !slices.Contains(patterns, host) || slices.ContainsFunc(patterns, func(pattern string) bool { return strings.ContainsAny(pattern, "*?") }) {
			return errors.New("git host_key entries must each name the remote's host, as host or [host]:port for a port other than 22, and no wildcard")
		}
		entries++
	}
	if entries == 0 {
		return malformed
	}
	return nil
}

// AttemptFilter drops records superseded by a later delivery attempt.
type AttemptFilter struct {
	highest map[string]int64
}

// Admit reports whether a record is current for its run.
func (filter *AttemptFilter) Admit(runID string, attempt int64) bool {
	highest := filter.highest[runID]
	if attempt < highest {
		return false
	}
	if filter.highest == nil {
		filter.highest = make(map[string]int64)
	}
	filter.highest[runID] = attempt
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

// validateReferenceArtifact checks an artifact a result lists after its text: one reference part standing in for a
// streamed artifact of the same ID. An output's has the output's name and the reply's none, each with the sha256 and
// size of its bytes; the transcript's has no name and an object naming nothing more.
func validateReferenceArtifact(value *Artifact) error {
	if !slices.Contains(value.Extensions, ExtensionURI) {
		return errors.New("artifact must declare the wherry extension")
	}
	if len(value.Parts) != 1 {
		return errors.New("a reference artifact has exactly one part")
	}
	part := value.Parts[0]
	if part.MediaType != ReferenceMediaType || part.Data == nil || part.Text != "" || part.Raw != nil || part.URL != "" || part.Filename != "" ||
		!part.Data.IsValid() || part.Data.Kind() != '{' {
		return errors.New("a reference artifact's part is a reference data part with no filename")
	}
	switch {
	case value.ArtifactID == TranscriptArtifact && value.Name == "":
		return cmp.Or(validateMetadata(part.Metadata), validateMetadata(value.Metadata))
	case value.ArtifactID == ReplyArtifact && value.Name == "",
		ValidOutputName(value.ArtifactID) && !ReservedArtifact(value.ArtifactID) && value.Name == value.ArtifactID:
	default:
		return errors.New("a reference artifact names an output by its name, the reply or the transcript")
	}
	var reference struct {
		SHA256 string `json:"sha256"`
		Size   *int64 `json:"size"`
	}
	if json.Unmarshal(part.Data, &reference, signed.Tolerant) != nil || !ValidHash(reference.SHA256) || reference.Size == nil || *reference.Size < 0 {
		return errors.New("an output's or the reply's reference data requires sha256 and a size of 0 or more")
	}
	return cmp.Or(validateMetadata(part.Metadata), validateMetadata(value.Metadata))
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
		if part.MediaType == ReferenceMediaType {
			var reference Reference
			if part.Data == nil || json.Unmarshal(part.Data, &reference, signed.Tolerant) != nil || !ValidHash(reference.SHA256) || reference.Size < 0 ||
				reference.Unstored != UnstoredInput && reference.Unstored != UnstoredOversize {
				return errors.New("a reference part's data requires sha256, a size of 0 or more, and unstored input or oversize")
			}
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

// ValidHash reports whether value is a SHA-256 in lowercase hex.
func ValidHash(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}
