package main

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/wire"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// awaitBound stays below the shortest common harness tool-call timeout, so every model can loop on await.
const awaitBound = 45 * time.Second

// retryAfter is the pause, in seconds, that await suggests before the next call while a run is unfinished.
const retryAfter = 5

const progressInterval = 5 * time.Second

func runMCP(ctx context.Context, config busclient.Config, args []string) error {
	if len(args) != 0 {
		return errors.New("usage: wherry mcp")
	}
	return newMCPServer(func() (*busclient.Host, error) { return busclient.Connect(config) }).Run(ctx, &mcp.StdioTransport{})
}

// toolError is a tool failure whose code and message carry no underlying error text.
type toolError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *toolError) Error() string {
	encoded, _ := json.Marshal(e) // two strings always encode
	return string(encoded)
}

func invalidArgument(message string) error {
	return &toolError{Code: "INVALID_ARGUMENT", Message: message}
}

// toolFailure maps a client error to a toolError. Errors from the bus, the broker or the local key are reported by
// class only, since their text can name files, hosts or record contents.
func toolFailure(err error) error {
	switch {
	case errors.Is(err, busclient.ErrRunNotFound):
		return &toolError{Code: "RUN_NOT_FOUND", Message: "this host has not submitted the run and no receiver has admitted it"}
	case errors.Is(err, busclient.ErrNoMatchingReceiver):
		return &toolError{Code: "NO_MATCHING_RECEIVER", Message: "no live receiver of an entitled pool offers a placement matching the selector"}
	case errors.Is(err, busclient.ErrNotAwaitingContinuation):
		return &toolError{Code: "NOT_AWAITING_CONTINUATION", Message: "the run is not waiting for a continuation"}
	case errors.Is(err, busclient.ErrRunCanceled):
		return &toolError{Code: "RUN_CANCELED", Message: "the run is canceled"}
	case errors.Is(err, busclient.ErrTurnSubmitted):
		return &toolError{Code: "TURN_ALREADY_SUBMITTED", Message: "a continuation for this turn is already submitted"}
	case errors.Is(err, busclient.ErrInvalidInput):
		return invalidArgument(err.Error())
	case errors.Is(err, context.Canceled):
		return &toolError{Code: "CANCELLED", Message: "the request was cancelled"}
	}
	return &toolError{Code: "UNAVAILABLE", Message: "the wherry bus could not complete the request; the wherry CLI reports the cause"}
}

type receiversInput struct{}

type receiversOutput struct {
	Receivers []busclient.Receiver `json:"receivers"`
}

type gitInput struct {
	URL  string `json:"url" jsonschema:"https repository url, or ssh url whose only user information is the user git"`
	Ref  string `json:"ref" jsonschema:"branch, tag or commit to check out"`
	Path string `json:"path,omitempty" jsonschema:"checkout directory, relative; defaults to the repository name"`
}

type submitInput struct {
	Prompt               string              `json:"prompt" jsonschema:"the task for the remote agent"`
	Selector             map[string][]string `json:"selector" jsonschema:"offer label key to its accepted values; a key is agent, platform, runtime, region or zone, a platform key such as nomad.node_pool, or an operator key containing a slash such as example.com/gpu"`
	Git                  *gitInput           `json:"git,omitempty" jsonschema:"repository to check out for the agent"`
	DeadlineSeconds      int64               `json:"deadline_seconds,omitempty" jsonschema:"task deadline in seconds; default 1800"`
	QueueDeadlineSeconds int64               `json:"queue_deadline_seconds,omitempty" jsonschema:"queue deadline in seconds; default 600"`
}

type submitOutput struct {
	ID      string `json:"id" jsonschema:"run id"`
	Pool    string `json:"pool" jsonschema:"pool the task was placed in"`
	Skipped int    `json:"skipped,omitempty" jsonschema:"records that failed verification and were ignored"`
}

type runInput struct {
	ID string `json:"id" jsonschema:"run id"`
}

type statusOutput struct {
	ID      string `json:"id"`
	State   string `json:"state"`
	Turn    int64  `json:"turn"`
	Message string `json:"message,omitempty"`
	Skipped int    `json:"skipped,omitempty"`
}

type resultPart struct {
	Text      string `json:"text,omitempty"`
	Data      any    `json:"data,omitempty"`
	URL       string `json:"url,omitempty"`
	MediaType string `json:"media_type,omitempty"`
	RawBytes  int    `json:"raw_bytes,omitempty" jsonschema:"size of a binary part, which is not included"`
}

type resultArtifact struct {
	ArtifactID string       `json:"artifact_id"`
	Parts      []resultPart `json:"parts"`
}

type awaitReply struct {
	ID         string           `json:"id"`
	State      string           `json:"state"`
	RetryAfter int              `json:"retry_after,omitempty" jsonschema:"set while the run is unfinished: seconds to wait before calling await again"`
	Message    string           `json:"message,omitempty" jsonschema:"status message, such as the credential an AUTH_REQUIRED run needs"`
	Artifacts  []resultArtifact `json:"artifacts,omitempty"`
	Skipped    int              `json:"skipped,omitempty"`
}

type continueInput struct {
	ID                   string `json:"id" jsonschema:"run id of an interrupted run"`
	Prompt               string `json:"prompt,omitempty" jsonschema:"continuation text; default continue"`
	DeadlineSeconds      int64  `json:"deadline_seconds,omitempty" jsonschema:"task deadline in seconds; default 1800"`
	QueueDeadlineSeconds int64  `json:"queue_deadline_seconds,omitempty" jsonschema:"queue deadline in seconds; default 600"`
}

type continueOutput struct {
	ID      string `json:"id"`
	Turn    int64  `json:"turn"`
	Skipped int    `json:"skipped,omitempty"`
}

type cancelOutput struct {
	ID      string `json:"id"`
	Skipped int    `json:"skipped,omitempty"`
}

type eventOutput struct {
	Turn     int64  `json:"turn"`
	Attempt  int64  `json:"attempt"`
	Kind     string `json:"kind"`
	State    string `json:"state"`
	Receiver string `json:"receiver"`
	Time     string `json:"time"`
	Message  string `json:"message,omitempty"`
}

type transcriptOutput struct {
	Turn    int64 `json:"turn"`
	Attempt int64 `json:"attempt"`
	Chunks  int   `json:"chunks"`
}

type eventsOutput struct {
	ID          string             `json:"id"`
	Events      []eventOutput      `json:"events"`
	Cancelled   bool               `json:"cancelled" jsonschema:"whether the run ended canceled"`
	Transcripts []transcriptOutput `json:"transcripts"`
	Skipped     int                `json:"skipped,omitempty"`
}

// newMCPServer serves the wherry tools. Each call opens its own connection: a Client's consumer serves one
// operation at a time, and a harness may call status while an await is in flight.
func newMCPServer(connect func() (*busclient.Host, error)) *mcp.Server {
	version := "(devel)"
	if info, ok := debug.ReadBuildInfo(); ok {
		version = info.Main.Version
	}
	server := mcp.NewServer(&mcp.Implementation{Name: "wherry", Version: version}, nil)
	readOnly := &mcp.ToolAnnotations{ReadOnlyHint: true}

	mcp.AddTool(server, &mcp.Tool{
		Name:        "receivers",
		Description: "List the receivers of the pools this tenant may use, with their offers, spare capacity and the credentials this host has sealed to them. Read it to choose a selector before submit.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ receiversInput) (*mcp.CallToolResult, receiversOutput, error) {
		client, err := connect()
		if err != nil {
			return nil, receiversOutput{}, toolFailure(err)
		}
		defer client.Close()
		receivers, err := client.Receivers(ctx)
		if err != nil {
			return nil, receiversOutput{}, toolFailure(err)
		}
		return nil, receiversOutput{Receivers: receivers}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "submit",
		Description: "Sign and publish a task to the pool of the live receiver whose offers match the selector, sealing the agent's configured credentials to that pool's live receivers. Returns the run id at once; collect the result with await.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input submitInput) (*mcp.CallToolResult, submitOutput, error) {
		client, err := connect()
		if err != nil {
			return nil, submitOutput{}, toolFailure(err)
		}
		defer client.Close()
		submission, err := submitFromInput(input)
		if err != nil {
			return nil, submitOutput{}, err
		}
		result, err := client.Submit(ctx, submission)
		if err != nil {
			return nil, submitOutput{}, toolFailure(err)
		}
		return nil, submitOutput{ID: result.RunID, Pool: result.Pool, Skipped: result.Skipped}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "status",
		Description: "Return the latest verified state of a run's latest turn without waiting.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input runInput) (*mcp.CallToolResult, statusOutput, error) {
		client, err := connect()
		if err != nil {
			return nil, statusOutput{}, toolFailure(err)
		}
		defer client.Close()
		result, err := client.Status(ctx, input.ID)
		if err != nil {
			return nil, statusOutput{}, toolFailure(err)
		}
		return nil, statusOutput{
			ID: input.ID, State: string(result.Status.State), Turn: result.Turn, Message: statusText(result.Status), Skipped: result.Skipped,
		}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name: "await",
		Description: fmt.Sprintf("Wait up to %d seconds for a run to finish or stop for input. Returns the result, an interrupted, "+
			"canceled or rejected state with its message, or {id, state, retry_after} while the run is unfinished: call await again.", int(awaitBound.Seconds())),
		Annotations: readOnly,
	}, func(ctx context.Context, request *mcp.CallToolRequest, input runInput) (*mcp.CallToolResult, awaitReply, error) {
		client, err := connect()
		if err != nil {
			return nil, awaitReply{}, toolFailure(err)
		}
		defer client.Close()
		if token := request.Params.GetProgressToken(); token != nil {
			stop := notifyProgress(ctx, request.Session, token, input.ID)
			defer stop()
		}
		result, err := client.Await(ctx, input.ID, awaitBound)
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			status, err := client.Status(ctx, input.ID)
			if err != nil {
				return nil, awaitReply{}, toolFailure(err)
			}
			return nil, awaitReply{ID: input.ID, State: string(status.Status.State), RetryAfter: retryAfter, Skipped: status.Skipped}, nil
		}
		if err != nil {
			return nil, awaitReply{}, toolFailure(err)
		}
		output := awaitReply{
			ID: input.ID, State: string(result.Status.State), Message: statusText(result.Status), Skipped: result.Skipped,
		}
		if result.Task != nil {
			for _, artifact := range result.Task.Artifacts {
				output.Artifacts = append(output.Artifacts, resultArtifact{ArtifactID: artifact.ArtifactID, Parts: resultParts(artifact.Parts)})
			}
		}
		return nil, output, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "continue",
		Description: "Sign and publish the next turn of a run interrupted in AUTH_REQUIRED or INPUT_REQUIRED to the pool that admitted it, resealing its configured credentials; add a missing credential to the wherry config first.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input continueInput) (*mcp.CallToolResult, continueOutput, error) {
		client, err := connect()
		if err != nil {
			return nil, continueOutput{}, toolFailure(err)
		}
		defer client.Close()
		if input.Prompt == "" {
			input.Prompt = "continue"
		}
		deadline, queueDeadline, err := deadlines(input.DeadlineSeconds, input.QueueDeadlineSeconds)
		if err != nil {
			return nil, continueOutput{}, err
		}
		result, err := client.Continue(ctx, input.ID, busclient.ContinueInput{Prompt: input.Prompt, Deadline: deadline, QueueDeadline: queueDeadline})
		if err != nil {
			return nil, continueOutput{}, toolFailure(err)
		}
		return nil, continueOutput{ID: result.RunID, Turn: result.Turn, Skipped: result.Skipped}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "cancel",
		Description: "Sign and publish a cancel for a run whose result is no longer needed.",
		Annotations: &mcp.ToolAnnotations{IdempotentHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input runInput) (*mcp.CallToolResult, cancelOutput, error) {
		client, err := connect()
		if err != nil {
			return nil, cancelOutput{}, toolFailure(err)
		}
		defer client.Close()
		result, err := client.Cancel(ctx, input.ID)
		if err != nil {
			return nil, cancelOutput{}, toolFailure(err)
		}
		return nil, cancelOutput{ID: result.RunID, Skipped: result.Skipped}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "events",
		Description: "Return a run's verified status history, whether it was cancelled, and its transcript chunk counts.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input runInput) (*mcp.CallToolResult, eventsOutput, error) {
		client, err := connect()
		if err != nil {
			return nil, eventsOutput{}, toolFailure(err)
		}
		defer client.Close()
		result, err := client.Events(ctx, input.ID)
		if err != nil {
			return nil, eventsOutput{}, toolFailure(err)
		}
		output := eventsOutput{ID: input.ID, Events: []eventOutput{}, Cancelled: result.Cancelled, Transcripts: []transcriptOutput{}, Skipped: result.Skipped}
		for _, record := range result.Statuses {
			status := record.StatusUpdate.Status
			output.Events = append(output.Events, eventOutput{
				Turn: record.Wherry.Turn, Attempt: record.Wherry.Attempt, Kind: record.Wherry.Kind, State: string(status.State),
				Receiver: record.Wherry.Receiver, Time: status.Timestamp, Message: statusText(status),
			})
		}
		for position, chunks := range result.Chunks {
			output.Transcripts = append(output.Transcripts, transcriptOutput{Turn: position.Turn, Attempt: position.Attempt, Chunks: chunks})
		}
		sort.Slice(output.Transcripts, func(i, j int) bool {
			if output.Transcripts[i].Turn != output.Transcripts[j].Turn {
				return output.Transcripts[i].Turn < output.Transcripts[j].Turn
			}
			return output.Transcripts[i].Attempt < output.Transcripts[j].Attempt
		})
		return nil, output, nil
	})
	return server
}

func submitFromInput(input submitInput) (busclient.SubmitInput, error) {
	if input.Prompt == "" {
		return busclient.SubmitInput{}, invalidArgument("prompt is required")
	}
	keys := make([]string, 0, len(input.Selector))
	for key := range input.Selector {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	selector := []wire.Requirement{}
	for _, key := range keys {
		selector = append(selector, wire.Requirement{Key: key, Operator: wire.OpIn, Values: input.Selector[key]})
	}
	var parts []wire.Part
	if input.Git != nil {
		metadata, _ := json.Marshal(wire.GitMetadata{Ref: input.Git.Ref, Path: input.Git.Path}) // strings always encode
		parts = append(parts, wire.Part{URL: input.Git.URL, MediaType: wire.GitMediaType, Metadata: metadata})
	}
	deadline, queueDeadline, err := deadlines(input.DeadlineSeconds, input.QueueDeadlineSeconds)
	if err != nil {
		return busclient.SubmitInput{}, err
	}
	return busclient.SubmitInput{
		Selector: selector, Prompt: input.Prompt, Parts: parts,
		Deadline: deadline, QueueDeadline: queueDeadline,
	}, nil
}

// deadlines applies the CLI's defaults to unset deadlines.
func deadlines(deadlineSeconds, queueDeadlineSeconds int64) (time.Duration, time.Duration, error) {
	if deadlineSeconds < 0 || queueDeadlineSeconds < 0 {
		return 0, 0, invalidArgument("deadlines must be positive")
	}
	deadline, queueDeadline := 30*time.Minute, 10*time.Minute
	if deadlineSeconds > 0 {
		deadline = time.Duration(deadlineSeconds) * time.Second
	}
	if queueDeadlineSeconds > 0 {
		queueDeadline = time.Duration(queueDeadlineSeconds) * time.Second
	}
	return deadline, queueDeadline, nil
}

// notifyProgress reports the elapsed wait every progressInterval until the returned stop is called.
func notifyProgress(ctx context.Context, session *mcp.ServerSession, token any, runID string) func() {
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		ticker := time.NewTicker(progressInterval)
		defer ticker.Stop()
		start := time.Now()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = session.NotifyProgress(ctx, &mcp.ProgressNotificationParams{
					ProgressToken: token, Progress: time.Since(start).Seconds(), Total: awaitBound.Seconds(),
					Message: "waiting for " + runID,
				}) // a lost notification only delays the client's keep-alive
			}
		}
	}()
	return func() {
		close(done)
		<-finished
	}
}

func statusText(status wire.TaskStatus) string {
	if status.Message == nil {
		return ""
	}
	texts := make([]string, 0, len(status.Message.Parts))
	for _, part := range status.Message.Parts {
		texts = append(texts, partText(part))
	}
	return strings.Join(texts, "\n")
}

func resultParts(parts []wire.Part) []resultPart {
	result := make([]resultPart, 0, len(parts))
	for _, part := range parts {
		converted := resultPart{Text: part.Text, URL: part.URL, MediaType: part.MediaType, RawBytes: len(part.Raw)}
		if part.Data != nil {
			var data any
			if err := json.Unmarshal(part.Data, &data); err == nil {
				converted.Data = data
			}
		}
		result = append(result, converted)
	}
	return result
}
