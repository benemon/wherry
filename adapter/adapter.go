// Package adapter is the wherry adapter protocol: the interface a platform adapter implements, the
// plugin server that hosts adapters out of process, and the client the receiver core uses to reach them.
package adapter

import (
	"context"
	"errors"
)

// Handle identifies one platform instance of a run.
type Handle struct {
	// Data is adapter-private and at most 4 KiB; the adapter versions its encoding.
	Data []byte
	// Name is the platform's reference for the job, for diagnosis; the core logs it and never interprets it.
	Name string
	// Commit is the git commit checked out for a run with a git context.
	Commit string
}

// Line is one output line, the byte offset where it starts and the offset after it.
type Line struct {
	Offset int64
	Next   int64
	Data   []byte
}

// GitContext is a validated git context part; Path has its default applied.
type GitContext struct {
	URL  string
	Ref  string
	Path string
}

// Run is the material needed to start one platform job; the adapter records Attempt, Adapter and Agent
// in platform metadata.
type Run struct {
	ID, Adapter, Agent string
	Turn, Attempt      int64
	Prompt, Credential []byte
	Git                *GitContext
}

// Outcome is what a Dispatch left on the platform.
type Outcome int

const (
	// OutcomeUnknown means a job may exist; the core reconciles it with Lookup.
	OutcomeUnknown Outcome = iota
	// OutcomeNothingCreated means the adapter knows no job exists.
	OutcomeNothingCreated
	OutcomeCreated
)

// JobState is the state of a turn's job as Lookup finds it.
type JobState int

const (
	JobAbsent JobState = iota
	JobRunning
	JobEnded
	// JobNeverStarted means the platform ended the job before its agent task started.
	JobNeverStarted
)

// Job is a turn's job as Lookup finds it, with the attempt, adapter and agent its platform metadata records.
type Job struct {
	State JobState
	// Handle is set unless State is JobAbsent.
	Handle         Handle
	Attempt        int64
	Adapter, Agent string
}

// Terminal is how a followed job ended.
type Terminal struct {
	Started  bool
	ExitCode int
	Signal   string
	// Class is the platform's failure class for a job that never started.
	Class string
	Node  string
}

// ErrGitContext is the error an adapter returns when a run's git context cannot be checked out.
var ErrGitContext = errors.New("git context unavailable")

// ErrLinkLost is the error a plugin adapter's call returns when the link to its plugin process is down.
var ErrLinkLost = errors.New("plugin link lost")

// ErrMessageTooLarge is the error a plugin adapter's call returns when a message exceeds the link's size limit.
var ErrMessageTooLarge = errors.New("message too large")

// Adapter is the platform seam; implementations hold platform credentials only.
type Adapter interface {
	// Git reports whether the adapter delivers a run's git context; the core rejects a task carrying one otherwise.
	Git() bool
	// Ready reports whether the adapter can currently start a job.
	Ready(ctx context.Context) (bool, error)
	// Dispatch starts a turn's job, converging on an existing one, and returns once it has started or ended.
	// The core treats an error as OutcomeUnknown.
	Dispatch(ctx context.Context, run Run) (Outcome, Handle, error)
	// Lookup finds a turn's job without creating one, and waits for it as Dispatch does.
	Lookup(ctx context.Context, runID string, turn int64) (Job, error)
	// Follow streams stdout lines from a byte offset and returns the Terminal once the job has ended;
	// a Follow cut short returns an error and a zero Terminal.
	Follow(ctx context.Context, handle Handle, from int64, lines chan<- Line) (Terminal, error)
	Stop(ctx context.Context, handle Handle) error
	// Purge removes a turn's job and its delivered material, with or without a handle; an absent job is success.
	Purge(ctx context.Context, runID string, turn int64) error
	Release(ctx context.Context, runID string, turn int64) error
	Sweep(ctx context.Context) error
}

// Settings configure one adapter in a plugin.
type Settings struct {
	Name, Pool, Agent string
	// Credential names the credential the adapter's agent takes, empty for none.
	Credential string
	// Config is the adapter's config section of the receiver configuration, as JSON.
	Config []byte
	// Platform is the receiver bundle's platform credential for the adapter, empty for none.
	Platform string
}
