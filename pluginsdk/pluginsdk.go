// Package pluginsdk is the wherry plugin protocol: the interface a plugin implements for each plugin profile, the
// server that hosts a plugin's profiles out of process, and the client the receiver core uses to reach them.
package pluginsdk

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"maps"
	"slices"
)

// Handle identifies one platform instance of a run.
type Handle struct {
	// Data is plugin-private and at most 4 KiB; the plugin versions its encoding.
	Data []byte
	// Name is the platform's reference for the job, for diagnosis, at most 253 bytes; the core logs it and never
	// interprets it.
	Name string
}

// Line is one output line, the byte offset where it starts and the offset after it.
type Line struct {
	Offset int64
	Next   int64
	Data   []byte
}

// Run is the material needed to start one platform job; the plugin records Attempt, Profile and Agent
// in platform metadata.
type Run struct {
	ID, Profile, Agent string
	// Model is the model the agent runs, as the pool's terms name it; empty leaves the agent's own. The plugin passes
	// it to the agent as WHERRY_MODEL and never interprets it.
	Model   string
	Attempt int64
	Prompt  []byte
	// Inputs and Fetches go in the directory WHERRY_INPUT_DIR names, each as a file under its name. Credentials are every
	// credential the agent gets, each at its target; the core has validated every name and target.
	Inputs      []File
	Credentials []Secret
	Fetches     []Fetch
}

// Fetch is an input the plugin fetches from URL, a presigned GET the core has checked against its pool's
// storage, and verifies as Size bytes with SHA256 before the agent starts. URL is a credential: never log it or put it
// in an error. A fetch that fails or does not verify ends the job before its agent starts, with Terminal.Class
// ClassFetch; one that fails in Dispatch before any job exists is an Error of Class ClassFetch with
// OutcomeNothingCreated, which ends the run.
type Fetch struct {
	Name, URL string
	Size      int64
	SHA256    string
}

// ClassFetch is the Terminal.Class of a job that ended before its agent started because a Fetch failed, and the
// Error.Class of a Dispatch whose Fetch failed before it created a job.
const ClassFetch = "fetch"

// File is one input file.
type File struct {
	Name string
	Data []byte
}

// Secret is one credential of a run, delivered at exactly one target: the environment variable Env, or the file File in
// the directory WHERRY_CREDENTIALS_DIR names. Value is a credential: never log it or put it in an error.
type Secret struct {
	Name, Env, File string
	Value           []byte
}

// Directories are the absolute paths of a job's input, output and credentials directories as the agent sees them. The
// core writes them into the prompt; a plugin whose paths depend on where the job lands reports tokens of its own
// and resolves them when it delivers the prompt. Empty means the plugin does not know them.
type Directories struct {
	Input, Output, Credentials string
}

// Limits bound what Collect reads: the number of entries beneath the outputs directory, the bytes of one file and
// the bytes of all of them. The outputs Stream names are streamed instead, each of at most StreamBytes, and count
// against neither FileBytes nor TotalBytes.
type Limits struct {
	Files       int
	FileBytes   int64
	TotalBytes  int64
	Stream      []string
	StreamBytes int64
}

// Output is one entry beneath a job's outputs directory, named by its slash-separated path there. Mode holds its
// type bits; Data is set only for a regular file read within the limits, and Body only for a regular file Limits.Stream
// names within StreamBytes, which streams its Size bytes and which whoever receives the Output closes.
type Output struct {
	Name string
	Size int64
	Mode fs.FileMode
	Data []byte
	Body io.ReadCloser
}

// InputTooLargeError is the error Dispatch returns, with OutcomeNothingCreated, when its platform cannot deliver
// input Name with the inputs before it.
type InputTooLargeError struct{ Name string }

func (e *InputTooLargeError) Error() string {
	return "input " + e.Name + " is too large for the platform"
}

// Error is a plugin's error from the platform operation Op, with the platform's failure class. The core logs Op,
// Class and Err's text, redacted.
type Error struct {
	Op, Class string
	Err       error
}

func (e *Error) Error() string {
	if e.Err == nil {
		return e.Op
	}
	return e.Op + ": " + e.Err.Error()
}

func (e *Error) Unwrap() error { return e.Err }

// Outcome is what a Dispatch left on the platform.
type Outcome int

const (
	// OutcomeUnknown means a job may exist; the core reconciles it with Lookup.
	OutcomeUnknown Outcome = iota
	// OutcomeNothingCreated means the plugin knows no job exists.
	OutcomeNothingCreated
	OutcomeCreated
)

// JobState is the state of a run's job as Lookup finds it.
type JobState int

const (
	JobAbsent JobState = iota
	JobRunning
	JobEnded
	// JobNeverStarted means the platform ended the job before its agent task started.
	JobNeverStarted
)

// Job is a run's job as Lookup finds it, with the attempt, profile and agent its platform metadata records.
type Job struct {
	State JobState
	// Handle is set unless State is JobAbsent.
	Handle         Handle
	Attempt        int64
	Profile, Agent string
}

// Terminal is how a followed job ended.
type Terminal struct {
	Started  bool
	ExitCode int
	Signal   string
	// Class is the platform's failure class for a job that never started.
	Class string
	Node  string
	// Reason is the platform's account of why a job never started or why a started job ended, which can quote the run's
	// material; it may be empty.
	Reason string
}

// ErrLinkLost is the error a profile's call returns when the link to its plugin process is down.
var ErrLinkLost = errors.New("plugin link lost")

// ErrMessageTooLarge is the error a profile's call returns when a message exceeds the link's size limit.
var ErrMessageTooLarge = errors.New("message too large")

// Plugin is the platform seam: a plugin's handle on one configured plugin profile. Implementations hold platform
// credentials only.
type Plugin interface {
	// Directories reports where the agent finds its input, output and credentials directories.
	Directories() Directories
	// InlineLimit reports the most inline input bytes the plugin delivers, or zero when it declares none.
	InlineLimit() int64
	// Ready reports whether the profile can currently start a job.
	Ready(ctx context.Context) (bool, error)
	// Dispatch starts a run's job, converging on an existing one, and returns once it has started or ended.
	// The core treats an error as OutcomeUnknown.
	Dispatch(ctx context.Context, run Run) (Outcome, Handle, error)
	// Lookup finds a run's job without creating one, and waits for it as Dispatch does.
	Lookup(ctx context.Context, runID string) (Job, error)
	// Follow streams stdout lines from a byte offset and returns the Terminal once the job has ended;
	// a Follow cut short returns an error and a zero Terminal. Follow may fail at any time, and the core calls it
	// again from the end of the last line it took. On an ended job it reads the rest of the log and returns the Terminal.
	Follow(ctx context.Context, handle Handle, from int64, lines chan<- Line) (Terminal, error)
	Stop(ctx context.Context, handle Handle) error
	// Collect streams every entry beneath a stopped job's outputs directory but the directories it descends, in name
	// order, reading only regular files within limits and never following a link. It reads nothing when the tree
	// holds more than limits.Files entries. An error means the outputs were not collected.
	Collect(ctx context.Context, handle Handle, limits Limits, outputs chan<- Output) error
	// Purge removes a run's job and its delivered material, with or without a handle; an absent job is success.
	Purge(ctx context.Context, runID string) error
	Release(ctx context.Context, runID string) error
	Sweep(ctx context.Context) error
	// Metrics reports the profile's samples of the families the core declares for plugins. The core drops a sample
	// outside them, and gives every sample's profile label the profile's own name.
	Metrics(ctx context.Context) ([]Sample, error)
}

// Kind is the kind of a Sample's value.
type Kind int

const (
	KindCounter Kind = iota + 1
	KindGauge
	KindHistogram
)

// Sample is one value of a metric family the core declares for plugins, with its labels.
type Sample struct {
	Family string
	Labels map[string]string
	Kind   Kind
	// Value is a counter's or a gauge's.
	Value     float64
	Histogram Histogram
}

// Histogram is a snapshot over its family's buckets: Buckets holds the cumulative count of observations at or below
// each bound.
type Histogram struct {
	Buckets []uint64
	Sum     float64
	Count   uint64
}

// PlacementBuckets are the bounds, in seconds, of wherry_plugin_placement_seconds.
var PlacementBuckets = []float64{1, 2, 5, 10, 30, 60, 120, 300, 600, 1800}

// AgentEnvironment is the environment every plugin gives its agent over the environment it configures: git reads only
// the clone's own configuration, never prompts and traces nothing. It protects a cooperative agent from ambient
// configuration; an agent that runs commands can undo all of it.
var AgentEnvironment = map[string]string{
	"GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_COUNT": "0", "GIT_CONFIG_PARAMETERS": "",
	"GIT_TEMPLATE_DIR": "", "GIT_TERMINAL_PROMPT": "0", "GIT_ASKPASS": "", "SSH_ASKPASS": "",
	"GIT_TRACE": "0", "GIT_TRACE_PACKET": "0", "GIT_TRACE_PACK_ACCESS": "0", "GIT_TRACE_PERFORMANCE": "0", "GIT_TRACE_SETUP": "0",
	"GIT_TRACE_SHALLOW": "0", "GIT_TRACE_CURL": "0", "GIT_TRACE2": "0", "GIT_TRACE2_EVENT": "0", "GIT_TRACE2_PERF": "0",
	"GIT_TRACE_REDACT": "1",
}

// AgentUnset are the variables a plugin removes from the environment it configures, since no value switches them off.
// A plugin cannot remove what the platform or the image sets, so neither may set them.
var AgentUnset = []string{
	"GIT_SSH", "GIT_SSH_COMMAND", "GIT_SSH_VARIANT", "GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE",
	"GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_EXEC_PATH", "GIT_SSL_NO_VERIFY", "GIT_CURL_VERBOSE",
}

// AgentEnv returns configured without AgentUnset and with AgentEnvironment over it.
func AgentEnv(configured map[string]string) map[string]string {
	env := maps.Clone(configured)
	if env == nil {
		env = map[string]string{}
	}
	for _, name := range AgentUnset {
		delete(env, name)
	}
	maps.Copy(env, AgentEnvironment)
	return env
}

// ReservedEnv reports whether a plugin sets the variable name for the agent, so a credential under it would be lost.
func ReservedEnv(name string) bool {
	_, set := AgentEnvironment[name]
	return set || slices.Contains([]string{"HOME", "WHERRY_INPUT_DIR", "WHERRY_OUTPUT_DIR", "WHERRY_CREDENTIALS_DIR", "WHERRY_MODEL"}, name)
}

// Settings describe one profile in a plugin.
type Settings struct {
	Name, Pool, Agent string
	// Config is the profile's merged config from the receiver configuration, without its credential key, as JSON.
	Config []byte
	// Credential is the value of the platform credential the profile's config names, empty for none.
	Credential string
}
