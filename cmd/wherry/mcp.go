package main

import (
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"sort"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/benemon/wherry/internal/admin"
	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/wire"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// awaitBound stays below the shortest common harness tool-call timeout, so every model can loop on await.
const awaitBound = 45 * time.Second

// retryAfter is the pause, in seconds, that await suggests before the next call while a run is unfinished.
const retryAfter = 5

const progressInterval = 5 * time.Second

// transcriptLimit and transcriptMaxLimit are the default and the largest page of transcript lines.
const transcriptLimit, transcriptMaxLimit = 100, 500

// transcriptBytes bounds a transcript page's JSON, within the 25,000-token default of Claude Code's MAX_MCP_OUTPUT_TOKENS.
const transcriptBytes = 24 << 10

// answerRetry separates the server's attempts to answer seal requests after a failure.
const answerRetry = 5 * time.Second

func runMCP(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("mcp", flag.ContinueOnError)
	configFile := flags.String("config-file", "", "host configuration file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("usage: wherry mcp")
	}
	config, err := loadConfig(*configFile)
	if err != nil {
		return err
	}
	logger := log.New(os.Stderr, "", 0)
	connect := func() (*busclient.Host, error) { return busclient.Connect(config, logger) }
	dir, err := os.Getwd()
	if err != nil {
		return err
	}
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	go answer(ctx, connect, logger)
	open := inputOpener(dir, config)
	await, err := awaitCommand(*configFile)
	if err != nil {
		return err
	}
	return newMCPServer(connect, inputReader(open), open, await).Run(ctx, &mcp.StdioTransport{})
}

// awaitCommand returns the shell command that waits for a run, naming configFile, made absolute and quoted, when
// it is set.
func awaitCommand(configFile string) (func(id string) string, error) {
	if configFile == "" {
		return func(id string) string { return "wherry await " + id }, nil
	}
	path, err := filepath.Abs(configFile)
	if err != nil {
		return nil, err
	}
	quoted := "'" + strings.ReplaceAll(path, "'", `'\''`) + "'"
	return func(id string) string { return "wherry await " + id + " --config-file " + quoted }, nil
}

// answer answers seal requests for as long as the server runs, so a credentialed task is sealed while the harness
// is between await calls.
func answer(ctx context.Context, connect func() (*busclient.Host, error), logger *log.Logger) {
	for ctx.Err() == nil {
		host, err := connect()
		if err == nil {
			err = host.Answer(ctx)
			host.Close()
		}
		if err != nil {
			logger.Printf("answer seal requests: %v", err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(answerRetry):
		}
	}
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
	case errors.Is(err, busclient.ErrQueueDeadlinePastLifetime):
		return &toolError{Code: "RUN_EXPIRED", Message: "the queue deadline falls after the pool's run lifetime"}
	case errors.Is(err, busclient.ErrInvalidInput):
		return invalidArgument(err.Error())
	case errors.Is(err, context.Canceled):
		return &toolError{Code: "CANCELLED", Message: "the request was cancelled"}
	case errors.Is(err, admin.ErrNewerRecord):
		return &toolError{Code: "VERSION_SKEW", Message: "this wherry is older than the release that wrote the pool's records; upgrade wherry and restart this MCP server"}
	}
	return &toolError{Code: "UNAVAILABLE", Message: "the wherry bus could not complete the request; the wherry CLI reports the cause"}
}

type receiversInput struct{}

type receiversOutput struct {
	Receivers []busclient.Receiver `json:"receivers"`
}

type gitInput struct {
	Remote     string `json:"remote" jsonschema:"repository the agent clones: an https or ssh url, or user@host:path"`
	Ref        string `json:"ref" jsonschema:"branch, tag or commit to check out"`
	Path       string `json:"path,omitempty" jsonschema:"checkout directory, relative; defaults to the repository name"`
	Credential string `json:"credential,omitempty" jsonschema:"name of one of the credentials, which the agent clones with: a token for https, a private key for ssh"`
	HostKey    string `json:"host_key,omitempty" jsonschema:"local file of OpenSSH known_hosts lines for the host of an ssh remote, read as an input is; required for ssh"`
	Changes    string `json:"changes,omitempty" jsonschema:"patch or bundle: ask for the agent's commits back as the output changes.patch or changes.bundle"`
}

type credentialInput struct {
	Name string `json:"name" jsonschema:"credential name in this host's wherry config"`
	File string `json:"file" jsonschema:"file name the agent finds it under in WHERRY_CREDENTIALS_DIR"`
}

type outputInput struct {
	Name     string `json:"name" jsonschema:"relative path the agent is asked to write in WHERRY_OUTPUT_DIR"`
	Required bool   `json:"required,omitempty" jsonschema:"fail the run when the output is not returned"`
	Storage  string `json:"storage,omitempty" jsonschema:"s3 to return the output through the pool's bucket, up to 5 GiB, instead of the feed"`
}

type submitInput struct {
	Prompt               string             `json:"prompt,omitempty" jsonschema:"the task for the remote agent; exactly one of prompt and prompt_file is required"`
	PromptFile           string             `json:"prompt_file,omitempty" jsonschema:"local file holding the prompt, read as an input is"`
	Agent                string             `json:"agent,omitempty" jsonschema:"agent to run, as the pool's terms name it; default: the pool's default agent"`
	Model                string             `json:"model,omitempty" jsonschema:"model the agent runs, one of its models as receivers lists them; default: the agent's default model"`
	Selector             []wire.Requirement `json:"selector,omitempty" jsonschema:"labels the adapter must carry, each {key, operator, values}; operator is In or NotIn (with values), Exists or DoesNotExist (without); keys are arbitrary operator labels such as gpu or example.com/segment"`
	Git                  *gitInput          `json:"git,omitempty" jsonschema:"repository the agent clones itself; its host must be one of the pool's vcs hosts"`
	Inputs               []string           `json:"inputs,omitempty" jsonschema:"local files delivered to the agent's WHERRY_INPUT_DIR under their base names; the pool's inline limit in all, 256 KiB by default"`
	S3Inputs             []string           `json:"s3_inputs,omitempty" jsonschema:"local files of up to 5 GiB each uploaded to the pool's bucket and fetched into the agent's WHERRY_INPUT_DIR under their base names"`
	Credentials          []credentialInput  `json:"credentials,omitempty" jsonschema:"configured credentials delivered to the agent as files in WHERRY_CREDENTIALS_DIR"`
	Outputs              []outputInput      `json:"outputs,omitempty" jsonschema:"files the agent is asked to leave in WHERRY_OUTPUT_DIR; await writes the returned ones locally"`
	DeadlineSeconds      int64              `json:"deadline_seconds,omitempty" jsonschema:"task deadline in seconds; default 7200"`
	QueueDeadlineSeconds int64              `json:"queue_deadline_seconds,omitempty" jsonschema:"queue deadline in seconds; default 600"`
}

type submitOutput struct {
	ID    string `json:"id" jsonschema:"run id"`
	Pool  string `json:"pool" jsonschema:"pool the task was placed in"`
	Await string `json:"await" jsonschema:"shell command that waits for the run to end; run it in the background"`
}

type runInput struct {
	ID string `json:"id" jsonschema:"run id"`
}

type statusInput struct {
	ID string `json:"id,omitempty" jsonschema:"run id; omit to list every run this host can see"`
}

type runOutput struct {
	ID       string         `json:"id"`
	Pool     string         `json:"pool"`
	State    string         `json:"state"`
	Model    string         `json:"model,omitempty" jsonschema:"model the run's placement names"`
	Message  string         `json:"message,omitempty"`
	Updated  string         `json:"updated" jsonschema:"RFC 3339 time of the latest status; while the run has none, of this host's submission, else of the receiver's echo"`
	Redacted map[string]int `json:"redacted,omitempty" jsonschema:"redaction markers in the message by the credential or S3 object each names"`
}

type statusOutput struct {
	Runs    []runOutput `json:"runs"`
	Skipped int         `json:"skipped,omitempty"`
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

type outputReply struct {
	Name   string `json:"name"`
	Path   string `json:"path" jsonschema:"where this host wrote the output"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

type awaitReply struct {
	ID           string                  `json:"id"`
	State        string                  `json:"state"`
	Model        string                  `json:"model,omitempty" jsonschema:"model the run's placement names"`
	ServedModels []string                `json:"served_models,omitempty" jsonschema:"models the agent's result reports serving the run, where its harness reports them"`
	RetryAfter   int                     `json:"retry_after,omitempty" jsonschema:"set while the run is unfinished: seconds to wait before calling await again"`
	Message      string                  `json:"message,omitempty" jsonschema:"status message, such as a credential the run could not obtain"`
	Artifacts    []resultArtifact        `json:"artifacts,omitempty"`
	Outputs      []outputReply           `json:"outputs,omitempty" jsonschema:"the run's outputs, verified and written locally"`
	NotReturned  []wire.UnreturnedOutput `json:"not_returned,omitempty" jsonschema:"outputs the run did not return or this host could not verify, each with its reason"`
	Redacted     map[string]int          `json:"redacted,omitempty" jsonschema:"redaction markers in the message and artifacts by the credential or S3 object each names"`
	Skipped      int                     `json:"skipped,omitempty"`
}

type cleanupOutput struct {
	ID string `json:"id"`
}

type cancelOutput struct {
	ID      string `json:"id"`
	Skipped int    `json:"skipped,omitempty"`
}

type eventOutput struct {
	Attempt  int64  `json:"attempt"`
	Kind     string `json:"kind"`
	State    string `json:"state"`
	Receiver string `json:"receiver"`
	Time     string `json:"time"`
	Message  string `json:"message,omitempty"`
}

type transcriptOutput struct {
	Attempt int64 `json:"attempt"`
	Chunks  int   `json:"chunks"`
}

type eventsOutput struct {
	ID          string             `json:"id"`
	Events      []eventOutput      `json:"events"`
	Transcripts []transcriptOutput `json:"transcripts"`
	Redacted    map[string]int     `json:"redacted,omitempty" jsonschema:"redaction markers in the events' messages by the credential or S3 object each names"`
	Skipped     int                `json:"skipped,omitempty"`
}

type transcriptInput struct {
	ID     string `json:"id" jsonschema:"run id"`
	Offset int    `json:"offset,omitempty" jsonschema:"lines to skip"`
	Limit  int    `json:"limit,omitempty" jsonschema:"most lines to return, at most 500; default 100"`
}

type transcriptLine struct {
	Seq       int64                 `json:"seq" jsonschema:"the line's byte offset in the agent's output"`
	Attempt   int64                 `json:"attempt" jsonschema:"the delivery attempt that published the line"`
	Text      string                `json:"text" jsonschema:"the line of the agent's standard output, without its newline; empty for a line with raw_bytes or unstored"`
	Truncated bool                  `json:"truncated,omitempty" jsonschema:"set when text holds only the start of a line too long for a page"`
	Bytes     int                   `json:"bytes,omitempty" jsonschema:"size of a truncated line"`
	RawBytes  int                   `json:"raw_bytes,omitempty" jsonschema:"size of a line that is not UTF-8, which is not included"`
	Unstored  *wire.PointerMetadata `json:"unstored,omitempty" jsonschema:"sha256 and size of a line over 512 KiB, which the receiver did not store"`
}

type transcriptReply struct {
	ID         string           `json:"id"`
	Attempt    int64            `json:"attempt" jsonschema:"the delivery attempt whose lines these are; 0 while no receiver has reported on the run"`
	Total      int              `json:"total" jsonschema:"lines of the attempt on the feed so far, less those the view drops"`
	NextOffset int              `json:"next_offset,omitempty" jsonschema:"set while lines remain past this page: the offset to call with next"`
	Lines      []transcriptLine `json:"lines"`
	Redacted   map[string]int   `json:"redacted,omitempty" jsonschema:"redaction markers in every line of the attempt, not only this page, by the credential or S3 object each names"`
	Skipped    int              `json:"skipped,omitempty"`
}

// newMCPServer serves the wherry tools. Each call opens its own connection: a Host's consumer serves one operation at a
// time, and a harness may call status while an await is in flight.
func newMCPServer(connect func() (*busclient.Host, error), readInput func(string, int64) (wire.Part, error), openInput func(string) (*os.File, error), awaitCommand func(id string) string) *mcp.Server {
	version := "(devel)"
	if info, ok := debug.ReadBuildInfo(); ok {
		version = info.Main.Version
	}
	server := mcp.NewServer(&mcp.Implementation{Name: "wherry", Version: version}, nil)
	readOnly := &mcp.ToolAnnotations{ReadOnlyHint: true}

	mcp.AddTool(server, &mcp.Tool{
		Name:        "receivers",
		Description: "List the receivers of the pools this tenant may use: each receiver's status (live, stale, silent or ended) and capacity, and each adapter's agent and labels as wherry.admin records them, its inline_limit from the offer, and its agent's models and default_model from the pool's terms, with ready set when the receiver's live advertisement offers the adapter. A live receiver with no ready adapter lists every adapter with ready false. Read it to choose an agent and selector before submit.",
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
		Description: "Sign and publish a task to the pool of the live receiver whose offers match the selector. Returns the run id at once; collect the result with await, or check the run without waiting with status. Inputs are local files the agent finds in WHERRY_INPUT_DIR; credentials are configured credentials it finds as files in WHERRY_CREDENTIALS_DIR; outputs are files it is asked to leave in WHERRY_OUTPUT_DIR, which await writes locally. While this server runs, it seals the agent's and the task's configured credentials to the receiver that claims the task when that receiver asks for them.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input submitInput) (*mcp.CallToolResult, submitOutput, error) {
		submission, err := submitFromInput(input, readInput, openInput)
		if err != nil {
			return nil, submitOutput{}, err
		}
		defer func() {
			for _, file := range submission.S3Inputs {
				if closer, ok := file.Body.(io.Closer); ok {
					closer.Close()
				}
			}
		}()
		client, err := connect()
		if err != nil {
			return nil, submitOutput{}, toolFailure(err)
		}
		defer client.Close()
		result, err := client.Submit(ctx, submission)
		if err != nil {
			return nil, submitOutput{}, toolFailure(err)
		}
		return nil, submitOutput{ID: result.RunID, Pool: result.Pool, Await: awaitCommand(result.RunID)}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name: "status",
		Description: "Return the latest verified state of a run without waiting. With id, runs holds that run; " +
			"without it, runs holds every run this host submitted or the tenant's receivers admitted, latest update first.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input statusInput) (*mcp.CallToolResult, statusOutput, error) {
		client, err := connect()
		if err != nil {
			return nil, statusOutput{}, toolFailure(err)
		}
		defer client.Close()
		var runs busclient.RunsResult
		if input.ID != "" {
			result, err := client.Status(ctx, input.ID)
			if err != nil {
				return nil, statusOutput{}, toolFailure(err)
			}
			runs = busclient.RunsResult{Runs: []busclient.StatusResult{result}, Skipped: result.Skipped}
		} else if runs, err = client.Runs(ctx); err != nil {
			return nil, statusOutput{}, toolFailure(err)
		}
		output := statusOutput{Runs: []runOutput{}, Skipped: runs.Skipped}
		for _, result := range runs.Runs {
			output.Runs = append(output.Runs, runOutput{
				ID: result.RunID, Pool: result.Pool, State: string(result.Status.State), Model: result.Model, Message: statusText(result.Status),
				Updated: result.Updated.UTC().Format(time.RFC3339Nano), Redacted: result.Redacted,
			})
		}
		return nil, output, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name: "await",
		Description: fmt.Sprintf("Wait up to %d seconds for a run to finish. Returns the result, a failed, canceled or rejected state "+
			"with its message, or {id, state, retry_after} while the run is unfinished: call await again. "+
			"A finished run's outputs are verified and written under this host's state directory; outputs lists their paths and hashes. "+
			"Use status to check a run without waiting.", int(awaitBound.Seconds())),
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
			return nil, awaitReply{ID: input.ID, State: string(status.Status.State), Model: status.Model, RetryAfter: retryAfter, Skipped: status.Skipped}, nil
		}
		if err != nil {
			return nil, awaitReply{}, toolFailure(err)
		}
		output := awaitReply{
			ID: input.ID, State: string(result.Status.State), Model: result.Model, ServedModels: result.ServedModels, Message: statusText(result.Status),
			Redacted: result.Redacted, Skipped: result.Skipped,
		}
		if result.Task != nil {
			for _, artifact := range result.Task.Artifacts {
				output.Artifacts = append(output.Artifacts, resultArtifact{ArtifactID: artifact.ArtifactID, Parts: resultParts(artifact.Parts)})
			}
		}
		if len(result.Outputs) > 0 {
			paths, err := client.WriteOutputs(input.ID, "", result.Outputs)
			if err != nil {
				return nil, awaitReply{}, &toolError{Code: "OUTPUTS_NOT_WRITTEN", Message: "the run's outputs could not be written to the state directory"}
			}
			for i, written := range result.Outputs {
				output.Outputs = append(output.Outputs, outputReply{Name: written.Name, Path: paths[i], SHA256: written.SHA256, Size: written.Size})
			}
		}
		downloaded, rejected, err := client.DownloadOutputs(ctx, input.ID, result, "")
		if err != nil {
			return nil, awaitReply{}, &toolError{Code: "OUTPUTS_NOT_WRITTEN", Message: "the run's S3 outputs could not be downloaded, or the run's prefix deleted; the wherry CLI's await reports the cause"}
		}
		for _, written := range downloaded {
			output.Outputs = append(output.Outputs, outputReply{Name: written.Name, Path: written.Path, SHA256: written.SHA256, Size: written.Size})
		}
		output.NotReturned = append(append(result.NotReturned, result.Rejected...), rejected...)
		return nil, output, nil
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
		Description: "Return a run's verified status history and its transcript chunk counts per attempt. Read the lines with transcript.",
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
		output := eventsOutput{ID: input.ID, Events: []eventOutput{}, Transcripts: []transcriptOutput{}, Redacted: result.Redacted, Skipped: result.Skipped}
		for _, record := range result.Statuses {
			status := record.StatusUpdate.Status
			output.Events = append(output.Events, eventOutput{
				Attempt: record.Wherry.Attempt, Kind: record.Wherry.Kind, State: string(status.State),
				Receiver: record.Wherry.Receiver, Time: status.Timestamp, Message: statusText(status),
			})
		}
		for attempt, chunks := range result.Chunks {
			output.Transcripts = append(output.Transcripts, transcriptOutput{Attempt: attempt, Chunks: chunks})
		}
		sort.Slice(output.Transcripts, func(i, j int) bool {
			return output.Transcripts[i].Attempt < output.Transcripts[j].Attempt
		})
		return nil, output, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name: "transcript",
		Description: fmt.Sprintf("Return a page of a run's transcript: the lines of the agent's standard output, redacted by the "+
			"receiver, in output order. The lines are the run's latest delivery attempt's, and before its first line, or while it "+
			"has none, each earlier attempt's line, so an adopted run reads from the start; each line names its attempt. A page "+
			"holds at most limit lines and %d KiB of JSON, which fits a harness's default tool output limit; a line too long for "+
			"a page alone comes back truncated, with its size in bytes. next_offset is set while more lines are on the feed. The "+
			"transcript grows while the run runs.", transcriptBytes>>10),
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input transcriptInput) (*mcp.CallToolResult, transcriptReply, error) {
		if input.Offset < 0 || input.Limit < 0 || input.Limit > transcriptMaxLimit {
			return nil, transcriptReply{}, invalidArgument(fmt.Sprintf("offset must not be negative, and limit must be 0 to %d", transcriptMaxLimit))
		}
		if input.Limit == 0 {
			input.Limit = transcriptLimit
		}
		client, err := connect()
		if err != nil {
			return nil, transcriptReply{}, toolFailure(err)
		}
		defer client.Close()
		result, err := client.Transcript(ctx, input.ID)
		if err != nil {
			return nil, transcriptReply{}, toolFailure(err)
		}
		result.Lines = viewLines(result.Lines)
		output := transcriptReply{ID: input.ID, Attempt: result.Attempt, Total: len(result.Lines), Lines: []transcriptLine{},
			Redacted: result.Redacted, Skipped: result.Skipped}
		size := func(value any) int {
			encoded, _ := json.Marshal(value, jsontext.EscapeForHTML(true), jsontext.EscapeForJS(true)) // as the SDK's encoding/json writes it
			return len(encoded)
		}
		output.NextOffset = len(result.Lines)
		room := transcriptBytes - size(output)
		output.NextOffset = 0
		for i := min(input.Offset, len(result.Lines)); i < len(result.Lines); i++ {
			line := transcriptLine{Seq: result.Lines[i].Seq, Attempt: result.Lines[i].Attempt}
			switch part := result.Lines[i].Part; {
			case part.URL != "":
				line.Unstored = &wire.PointerMetadata{}
				json.Unmarshal(part.Metadata, line.Unstored) // validation decoded it
			case part.Raw != nil && !utf8.Valid(part.Raw):
				line.RawBytes = len(part.Raw)
			case part.Raw != nil:
				line.Text = string(part.Raw)
			default:
				line.Text = part.Text
			}
			// A comma separates each line from the one before it.
			if len(output.Lines) == input.Limit || len(output.Lines) > 0 && size(line)+1 > room {
				output.NextOffset = i
				break
			}
			if over := size(line) - room; over > 0 {
				line.Truncated, line.Bytes = true, len(line.Text)
				for over = size(line) - room; over > 0; over = size(line) - room {
					line.Text = wire.CutUTF8(line.Text, max(len(line.Text)-over, 0))
				}
			}
			room -= size(line) + 1
			output.Lines = append(output.Lines, line)
		}
		return nil, output, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name: "cleanup",
		Description: "Delete every object under a run's prefix in its pool's bucket: its S3 inputs and outputs. await does this once a " +
			"run is terminal and its S3 outputs verify; use cleanup for a run that ended while nothing awaited it.",
		Annotations: &mcp.ToolAnnotations{IdempotentHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input runInput) (*mcp.CallToolResult, cleanupOutput, error) {
		client, err := connect()
		if err != nil {
			return nil, cleanupOutput{}, toolFailure(err)
		}
		defer client.Close()
		if err := client.Cleanup(ctx, input.ID); err != nil {
			return nil, cleanupOutput{}, toolFailure(err)
		}
		return nil, cleanupOutput{ID: input.ID}, nil
	})
	return server
}

// viewLines drops what a reader of a Claude stream-json transcript gets nothing from: each thinking block's signature,
// and system thinking_tokens lines. The feed keeps both.
func viewLines(lines []busclient.TranscriptLine) []busclient.TranscriptLine {
	kept := make([]busclient.TranscriptLine, 0, len(lines))
	for _, line := range lines {
		data := line.Part.Raw
		if data == nil {
			data = []byte(line.Part.Text)
		}
		var event struct {
			Type    string `json:"type"`
			Subtype string `json:"subtype"`
		}
		if line.Part.URL == "" && json.Unmarshal(data, &event) == nil {
			if event.Type == "system" && event.Subtype == "thinking_tokens" {
				continue
			}
			if unsigned, ok := withoutSignatures(data); ok {
				line.Part.Raw, line.Part.Text = unsigned, ""
			}
		}
		kept = append(kept, line)
	}
	return kept
}

// withoutSignatures returns an assistant event with the signature of each thinking block in its message's content
// removed, and false when it has none.
func withoutSignatures(data []byte) ([]byte, bool) {
	var event, message map[string]jsontext.Value
	var content []map[string]jsontext.Value
	var kind string
	if json.Unmarshal(data, &event) != nil || json.Unmarshal(event["type"], &kind) != nil || kind != "assistant" || json.Unmarshal(event["message"], &message) != nil || json.Unmarshal(message["content"], &content) != nil {
		return nil, false
	}
	removed := false
	for _, block := range content {
		if _, signed := block["signature"]; signed && json.Unmarshal(block["type"], &kind) == nil && kind == "thinking" {
			delete(block, "signature")
			removed = true
		}
	}
	if !removed {
		return nil, false
	}
	message["content"], _ = json.Marshal(content, json.Deterministic(true)) // values decoded from valid JSON always encode
	event["message"], _ = json.Marshal(message, json.Deterministic(true))
	unsigned, _ := json.Marshal(event, json.Deterministic(true))
	return unsigned, true
}

func submitFromInput(input submitInput, readInput func(string, int64) (wire.Part, error), openInput func(string) (*os.File, error)) (busclient.SubmitInput, error) {
	if (input.Prompt == "") == (input.PromptFile == "") {
		return busclient.SubmitInput{}, invalidArgument("exactly one of prompt and prompt_file is required")
	}
	if input.PromptFile != "" {
		part, err := readInput(input.PromptFile, math.MaxInt64)
		if err != nil {
			return busclient.SubmitInput{}, err
		}
		if len(part.Raw) == 0 {
			return busclient.SubmitInput{}, invalidArgument("prompt_file is empty")
		}
		input.Prompt = string(part.Raw)
	}
	selector := input.Selector
	if selector == nil {
		selector = []wire.Requirement{}
	}
	var parts []wire.Part
	for _, path := range input.Inputs {
		part, err := readInput(path, math.MaxInt64)
		if err != nil {
			return busclient.SubmitInput{}, err
		}
		parts = append(parts, part)
	}
	deadline, queueDeadline, err := deadlines(input.DeadlineSeconds, input.QueueDeadlineSeconds)
	if err != nil {
		return busclient.SubmitInput{}, err
	}
	submission := busclient.SubmitInput{
		Agent: input.Agent, Model: input.Model, Selector: selector, Prompt: input.Prompt, Parts: parts,
		Deadline: deadline, QueueDeadline: queueDeadline,
	}
	if input.Git != nil {
		submission.Git = &wire.GitInput{Remote: input.Git.Remote, Ref: input.Git.Ref, Path: input.Git.Path, Credential: input.Git.Credential}
		submission.GitChanges = input.Git.Changes
		if input.Git.HostKey != "" {
			// One byte over the bound is read, so validation refuses a longer file without reading all of it.
			key, err := readInput(input.Git.HostKey, wire.MaxGitHostKey+1)
			if err != nil {
				return busclient.SubmitInput{}, err
			}
			submission.Git.HostKey = string(key.Raw)
		}
	}
	for _, credential := range input.Credentials {
		submission.Credentials = append(submission.Credentials, wire.TaskCredential{Name: credential.Name, File: credential.File})
	}
	for _, output := range input.Outputs {
		submission.Outputs = append(submission.Outputs, wire.ExpectedOutput{Name: output.Name, Required: output.Required, Storage: output.Storage})
	}
	for _, path := range input.S3Inputs {
		file, err := openInput(path)
		if err != nil {
			for _, opened := range submission.S3Inputs {
				if closer, ok := opened.Body.(io.Closer); ok {
					closer.Close()
				}
			}
			return busclient.SubmitInput{}, err
		}
		submission.S3Inputs = append(submission.S3Inputs, busclient.S3File{Name: filepath.Base(path), Body: file})
	}
	return submission, nil
}

// deadlines applies the CLI's defaults to unset deadlines.
func deadlines(deadlineSeconds, queueDeadlineSeconds int64) (time.Duration, time.Duration, error) {
	if deadlineSeconds < 0 || queueDeadlineSeconds < 0 {
		return 0, 0, invalidArgument("deadlines must be positive")
	}
	deadline, queueDeadline := busclient.DefaultDeadline, busclient.DefaultQueueDeadline
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

// inputOpener opens an MCP input: a regular file beneath one of the configured input roots, or dir when there are
// none, and none of the host's own files, so a client steered by its prompt cannot send the host key, the bundle, the
// state, a credential or a storage key. Files are compared by identity, since path strings miss case-insensitive
// spellings and hard links. Relative paths resolve against dir.
func inputOpener(dir string, config busclient.Config) func(string) (*os.File, error) {
	configured := config.InputRoots
	if len(configured) == 0 {
		configured = []string{dir}
	}
	var roots []os.FileInfo
	for _, root := range configured {
		if info, err := os.Stat(root); err == nil {
			roots = append(roots, info)
		}
	}
	protected := []string{config.StateDir, config.HostKey, config.Bundle}
	for _, path := range config.Credentials {
		protected = append(protected, path)
	}
	if config.Storage != nil {
		protected = append(protected, config.Storage.AccessKeyFile, config.Storage.SecretKeyFile)
	}
	refused := invalidArgument("inputs must be readable regular files beneath one of the MCP server's input roots, outside the wherry configuration")
	// The host's files are looked up on each call, since the state directory and host key can appear after startup.
	allowed := func(resolved string, info os.FileInfo) bool {
		var own []os.FileInfo
		for _, path := range protected {
			if info, err := os.Stat(path); err == nil {
				own = append(own, info)
			}
		}
		is := func(set []os.FileInfo, info os.FileInfo) bool {
			return slices.ContainsFunc(set, func(other os.FileInfo) bool { return os.SameFile(other, info) })
		}
		if !info.Mode().IsRegular() || is(own, info) {
			return false
		}
		contained := false
		for ancestor := filepath.Dir(resolved); ; ancestor = filepath.Dir(ancestor) {
			info, err := os.Stat(ancestor)
			if err != nil || is(own, info) {
				return false
			}
			contained = contained || is(roots, info)
			if ancestor == filepath.Dir(ancestor) {
				return contained
			}
		}
	}
	return func(path string) (*os.File, error) {
		if !filepath.IsAbs(path) {
			path = filepath.Join(dir, path)
		}
		resolved, err := filepath.EvalSymlinks(path)
		if err != nil {
			return nil, refused
		}
		// O_NONBLOCK keeps opening a FIFO from blocking until a writer appears; the regular-file check refuses it.
		file, err := os.OpenFile(resolved, os.O_RDONLY|syscall.O_NONBLOCK, 0)
		if err != nil {
			return nil, refused
		}
		info, err := file.Stat()
		if err != nil || !allowed(resolved, info) {
			file.Close()
			return nil, refused
		}
		return file, nil
	}
}

// inputReader reads at most limit bytes of an MCP input that open opens, named by the base name of its path.
func inputReader(open func(string) (*os.File, error)) func(string, int64) (wire.Part, error) {
	return func(path string, limit int64) (wire.Part, error) {
		file, err := open(path)
		if err != nil {
			return wire.Part{}, err
		}
		defer file.Close()
		data, err := io.ReadAll(io.LimitReader(file, limit))
		if err != nil {
			return wire.Part{}, invalidArgument("inputs must be readable regular files beneath one of the MCP server's input roots, outside the wherry configuration")
		}
		return wire.Part{Raw: append(wire.StdBytes{}, data...), Filename: filepath.Base(path)}, nil
	}
}
