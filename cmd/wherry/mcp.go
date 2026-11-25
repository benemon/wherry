package main

import (
	"cmp"
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/wire"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// waitBound is the waiting budget, less submission time for submit.
var waitBound = 45 * time.Second

// retryAfter is the pause, in seconds, that wait suggests before the next call while a run is unfinished.
const retryAfter = 5

const progressInterval = 5 * time.Second

// logsLimit and logsMaxLimit are the default and the largest page of transcript lines.
const logsLimit, logsMaxLimit = 100, 500

// logsBytes bounds a transcript page's JSON, within the 25,000-token default of Claude Code's MAX_MCP_OUTPUT_TOKENS.
const logsBytes = 24 << 10

func runMCP(ctx context.Context, args []string, stderr io.Writer) error {
	flags := flag.NewFlagSet("mcp", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configFile := flags.String("config-file", "", "host configuration file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("usage: wherry host mcp [--config-file FILE]")
	}
	config, err := loadConfig(*configFile)
	if err != nil {
		return err
	}
	logger := log.New(os.Stderr, "", 0)
	dir, err := os.Getwd()
	if err != nil {
		return err
	}
	connect, closeHost := sharedHost(func() (*busclient.Host, error) { return busclient.Connect(config, logger) })
	defer closeHost()
	open := inputOpener(dir, config)
	wait, err := waitCommand(*configFile)
	if err != nil {
		return err
	}
	return newMCPServer(connect, open, wait, logger).Run(ctx, &mcp.StdioTransport{})
}

// waitCommand returns the shell command that waits for a run and prints its reply, naming the config configFile, else
// $WHERRY_CONFIG, names, with ~ expanded, made absolute and quoted, when one is set.
func waitCommand(configFile string) (func(id string) string, error) {
	configFile = cmp.Or(configFile, os.Getenv("WHERRY_CONFIG"))
	if configFile == "" {
		return func(id string) string { return "wherry result " + id }, nil
	}
	path, err := busclient.ExpandHome(configFile)
	if err != nil {
		return nil, err
	}
	if path, err = filepath.Abs(path); err != nil {
		return nil, err
	}
	quoted := shellQuote(path)
	return func(id string) string { return "wherry result " + id + " --config-file " + quoted }, nil
}

// shellQuote quotes s as one POSIX shell word.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// sharedHost returns a connect that opens one host on its first successful call and returns it to every later call, and
// a close for that host. A failed open is retried on the next call.
func sharedHost(open func() (*busclient.Host, error)) (func() (*busclient.Host, error), func()) {
	var mu sync.Mutex
	var host *busclient.Host
	connect := func() (*busclient.Host, error) {
		mu.Lock()
		defer mu.Unlock()
		if host == nil {
			opened, err := open()
			if err != nil {
				return nil, err
			}
			host = opened
		}
		return host, nil
	}
	return connect, func() {
		mu.Lock()
		defer mu.Unlock()
		if host != nil {
			host.Close()
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
	case errors.Is(err, busclient.ErrStartByPastLimit):
		return invalidArgument(err.Error())
	case errors.Is(err, busclient.ErrInvalidInput):
		return invalidArgument(err.Error())
	case errors.Is(err, context.Canceled):
		return &toolError{Code: "CANCELLED", Message: "the request was cancelled"}
	case errors.Is(err, operator.ErrNewerRecord):
		return &toolError{Code: "VERSION_SKEW", Message: "this wherry is older than the release that wrote the pool's records; upgrade wherry and restart this MCP server"}
	}
	return &toolError{Code: "UNAVAILABLE", Message: "the wherry bus could not complete the request; the wherry CLI reports the cause"}
}

// collectFailure names the Collect step err failed in, without its cause, whose text can name local paths.
func collectFailure(err error) error {
	message := "the run's results could not be collected; the MCP server's log holds the cause"
	if failed := (*busclient.CollectError)(nil); errors.As(err, &failed) {
		message = failed.Step + ": " + message
	}
	return &toolError{Code: "OUTPUTS_NOT_WRITTEN", Message: message}
}

type receiversInput struct{}

type receiversOutput struct {
	Receivers []busclient.Receiver `json:"receivers"`
}

type gitInput struct {
	Remote  string `json:"remote" jsonschema:"repository the agent clones: an https or ssh url, user@host:path, or . for the current directory's repository, sent as its origin at the commit checked out, which must be on the remote"`
	Ref     string `json:"ref,omitempty" jsonschema:"branch, tag or commit to check out; without it, the remote's default branch pinned to its commit at submit, and none with remote ."`
	Path    string `json:"path,omitempty" jsonschema:"checkout directory, relative; defaults to the repository name"`
	Changes string `json:"changes,omitempty" jsonschema:"bundle, patch or none: the agent's commits come back as the output changes.bundle (default), as changes.patch, or not at all"`
}

type credentialInput struct {
	Name string `json:"name" jsonschema:"credential name in this host's wherry config; alone, the placed agent's credential to request"`
	File string `json:"file,omitempty" jsonschema:"file name the agent finds it under in WHERRY_CREDENTIALS_DIR; at most one of file and env, neither to request the agent's own credential of that name at its target"`
	Env  string `json:"env,omitempty" jsonschema:"environment variable the agent finds it in; at most one of file and env, neither to request the agent's own credential of that name at its target"`
}

type fileInput struct {
	Path string `json:"path" jsonschema:"local file; it goes in the task, or through the pool's bucket, up to 5 GiB, when over the broker's record limit or a profile's declared limit and the pool has storage and the host a storage credential, else submit refuses"`
}

type outputInput struct {
	Name string `json:"name" jsonschema:"relative path the agent must write in WHERRY_OUTPUT_DIR; the run fails without it; one over the pool's output limit returns through the pool's bucket, up to 5 GiB, when the pool has storage and the host a storage credential"`
}

type submitInput struct {
	Wait        bool               `json:"wait,omitempty" jsonschema:"collect the result within one result call's wait; only worth setting for runs expected to finish within it, otherwise call result"`
	Prompt      string             `json:"prompt,omitempty" jsonschema:"the task for the remote agent: its text, or @FILE for the contents of a local file read as an input is; required"`
	Agent       string             `json:"agent,omitempty" jsonschema:"agent to run, as the pool's terms name it; default: the pool's default agent"`
	Model       string             `json:"model,omitempty" jsonschema:"model the agent runs, one of its models as receivers lists them; default: the agent's default model"`
	Selector    []wire.Requirement `json:"selector,omitempty" jsonschema:"labels the profile must carry, each {key, operator, values}; operator is In or NotIn (with values), Exists or DoesNotExist (without); keys are arbitrary operator labels such as gpu or example.com/segment"`
	Git         *gitInput          `json:"git,omitempty" jsonschema:"repository the agent clones itself; its host must be one of the pool's vcs hosts, and its credential and an ssh remote's host key come from the git entry of this host's wherry config whose url is the longest prefix of the remote"`
	Continue    string             `json:"continue,omitempty" jsonschema:"run id of a run of this host whose changes.bundle result collected: the agent clones that run's repository, continues from the bundle and returns changes.bundle through the pool's bucket, which needs a pool with storage and a host storage credential; not with git, nor an input or output named base.bundle or changes.bundle"`
	Inputs      []fileInput        `json:"inputs,omitempty" jsonschema:"local files delivered to the agent's WHERRY_INPUT_DIR under their base names"`
	Credentials []credentialInput  `json:"credentials,omitempty" jsonschema:"configured credentials delivered to the agent, each as a file in WHERRY_CREDENTIALS_DIR or in an environment variable, or by name alone a requirement for one of the placed agent's optional credentials, which submit then refuses to go without; no two share a target, and none takes the target of one of the agent's own credentials"`
	Outputs     []outputInput      `json:"outputs,omitempty" jsonschema:"files the agent is asked to leave in WHERRY_OUTPUT_DIR; result writes the returned ones locally"`
	StartBy     string             `json:"start_by,omitempty" jsonschema:"task_started_by, by which the agent's job starts or the task fails: an RFC 3339 time or a Go duration from now such as 30m; default 10m"`
	EndBy       string             `json:"end_by,omitempty" jsonschema:"task_ended_by, by which the task ends or fails: an RFC 3339 time or a Go duration from now such as 90m; default 2h"`
}

type submitOutput struct {
	ID         string     `json:"id" jsonschema:"run id"`
	Pool       string     `json:"pool" jsonschema:"pool the task was placed in"`
	Command    string     `json:"command" jsonschema:"to collect later; not needed when result is final"`
	Result     *waitReply `json:"result,omitempty"`
	Error      *toolError `json:"error,omitempty"`
	Unsupplied []string   `json:"unsupplied,omitempty" jsonschema:"the placed agent's optional credentials this host could not resolve, which the run goes without"`
}

type runInput struct {
	ID string `json:"id" jsonschema:"run id"`
}

type statusInput struct {
	ID string `json:"id,omitempty" jsonschema:"run id; omit to list every run this host can see"`
}

type runOutput struct {
	ID         string         `json:"id"`
	Pool       string         `json:"pool"`
	State      string         `json:"state"`
	Model      string         `json:"model,omitempty" jsonschema:"model the run's placement names"`
	Message    string         `json:"message,omitempty" jsonschema:"status message, or while no receiver has reported a status, this host's note of why the run waits as the receivers' live advertisements show it now and of a cancel waiting for a claim"`
	Updated    string         `json:"updated" jsonschema:"RFC 3339 time of the latest status; while the run has none, of this host's submission, else of the receiver's echo"`
	Redacted   map[string]int `json:"redacted,omitempty" jsonschema:"redaction markers in the message by the credential or S3 object each names"`
	ReplacedBy string         `json:"replaced_by,omitempty" jsonschema:"the run that resubmits this one, which lost its pool key before it started"`
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

type replyFile struct {
	Path     string `json:"path,omitempty" jsonschema:"where this host wrote the whole reply"`
	SHA256   string `json:"sha256,omitempty"`
	Size     int64  `json:"size,omitempty"`
	Cut      bool   `json:"cut,omitempty" jsonschema:"set when the reply was longer than the pool's reply limit and the file holds its first bytes"`
	Rejected string `json:"rejected,omitempty" jsonschema:"why this host could not verify the reply, of which message holds the first 64 KiB"`
}

type waitReply struct {
	ID          string                  `json:"id"`
	State       string                  `json:"state"`
	Model       string                  `json:"model,omitempty" jsonschema:"model the run's placement names"`
	RetryAfter  int                     `json:"retry_after,omitempty" jsonschema:"set while the run is unfinished and within its bound, its task_ended_by plus the upload grace: seconds to wait before calling result again; absent past the bound, where message reports the timeout wherry result exits 6 with"`
	Message     string                  `json:"message,omitempty" jsonschema:"status message: the agent's answer or what went wrong, a credential the run could not obtain, the step the agent last reported while the run is unfinished, or, while no receiver has reported a status, this host's note of why the run waits as the receivers' live advertisements show it now (a run past task_started_by: that no receiver claimed the task) and of a cancel waiting for a claim; an answer over 64 KiB is cut to its first 64 KiB, and reply names the whole"`
	Reply       *replyFile              `json:"reply,omitempty" jsonschema:"set when the agent's answer is over 64 KiB"`
	Artifacts   []resultArtifact        `json:"artifacts,omitempty"`
	Outputs     []outputReply           `json:"outputs,omitempty" jsonschema:"the run's outputs, verified and written locally"`
	NotReturned []wire.UnreturnedOutput `json:"not_returned,omitempty" jsonschema:"outputs the run did not return or this host could not verify, each with its reason"`
	FetchedRef  string                  `json:"fetched_ref,omitempty" jsonschema:"for a run submitted with git remote . whose changes.bundle came back: the branch, wherry/<run>, the bundle was fetched into in that repository, to merge from; the working tree, index and HEAD are untouched"`
	NotFetched  string                  `json:"not_fetched,omitempty" jsonschema:"for such a run, why the bundle was not fetched: exists (wherry/<run> names another commit and was left alone), no git, no repository, git refused, unreadable bundle, wrong commit or timeout; the bundle at its output path can be fetched by hand with git fetch <path> HEAD"`
	Redacted    map[string]int          `json:"redacted,omitempty" jsonschema:"redaction markers in the message and artifacts by the credential or S3 object each names"`
	Skipped     int                     `json:"skipped,omitempty"`
	Runs        []string                `json:"runs,omitempty" jsonschema:"set when the run lost its pool key before it started and was resubmitted: every run followed, the run whose outcome this is last"`
}

type cancelOutput struct {
	ID      string `json:"id" jsonschema:"the run cancelled: the last run of the run's resubmission chain"`
	Cancel  string `json:"cancel" jsonschema:"requested: the cancel is published; a run no receiver has claimed ends CANCELED once one claims it"`
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

type chunksOutput struct {
	Attempt int64 `json:"attempt"`
	Chunks  int   `json:"chunks"`
}

type eventsOutput struct {
	ID       string         `json:"id"`
	Events   []eventOutput  `json:"events"`
	Chunks   []chunksOutput `json:"chunks"`
	Redacted map[string]int `json:"redacted,omitempty" jsonschema:"redaction markers in the events' messages by the credential or S3 object each names"`
	Skipped  int            `json:"skipped,omitempty"`
}

type logsInput struct {
	ID     string `json:"id" jsonschema:"run id"`
	Offset int    `json:"offset,omitempty" jsonschema:"lines to skip"`
	Limit  int    `json:"limit,omitempty" jsonschema:"most lines to return, at most 500; default 100"`
}

type logsLine struct {
	Seq       int64         `json:"seq" jsonschema:"the line's byte offset in the agent's output"`
	Attempt   int64         `json:"attempt" jsonschema:"the delivery attempt that published the line"`
	Text      string        `json:"text" jsonschema:"the line of the agent's standard output, without its newline; empty for a line with raw_bytes or unstored"`
	Truncated bool          `json:"truncated,omitempty" jsonschema:"set when text holds only the start of a line too long for a page"`
	Bytes     int           `json:"bytes,omitempty" jsonschema:"size of a truncated line"`
	RawBytes  int           `json:"raw_bytes,omitempty" jsonschema:"size of a line that is not UTF-8, which is not included"`
	Unstored  *unstoredLine `json:"unstored,omitempty" jsonschema:"sha256 and size of a line over 512 KiB, which the receiver did not store"`
}

// unstoredLine is the SHA-256 and size of a line the receiver did not store.
type unstoredLine struct {
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

type logsReply struct {
	ID         string         `json:"id"`
	Attempt    int64          `json:"attempt" jsonschema:"the delivery attempt whose lines these are; 0 while no receiver has reported on the run"`
	Total      int            `json:"total" jsonschema:"lines of the attempt on the feed so far, less those the view drops"`
	NextOffset int            `json:"next_offset,omitempty" jsonschema:"set while lines remain past this page: the offset to call with next"`
	Lines      []logsLine     `json:"lines"`
	Redacted   map[string]int `json:"redacted,omitempty" jsonschema:"redaction markers in every line of the attempt, not only this page, by the credential or S3 object each names"`
	Skipped    int            `json:"skipped,omitempty"`
}

// newMCPServer serves the wherry tools. Every call uses the host connect returns, so the process runs one tenant-feed
// follower.
func newMCPServer(connect func() (*busclient.Host, error), openInput func(string) (*os.File, error), waitCommand func(id string) string, logger *log.Logger) *mcp.Server {
	// The model sees an UNAVAILABLE's class alone; the server's log, which only the host's user reads, keeps its cause
	// with every credential value the host resolved redacted.
	failure := func(err error) error {
		mapped := toolFailure(err)
		if failed, ok := mapped.(*toolError); ok && failed.Code == "UNAVAILABLE" {
			logger.Print("tool failed: " + busclient.Redact(err.Error()))
		}
		return mapped
	}
	version := "(devel)"
	if info, ok := debug.ReadBuildInfo(); ok {
		version = info.Main.Version
	}
	server := mcp.NewServer(&mcp.Implementation{Name: "wherry", Version: version}, nil)
	server.AddReceivingMiddleware(droppedFields)
	readOnly := &mcp.ToolAnnotations{ReadOnlyHint: true}

	collectResult := func(ctx context.Context, request *mcp.CallToolRequest, input runInput, budget time.Duration) (*mcp.CallToolResult, waitReply, error) {
		client, err := connect()
		if err != nil {
			return nil, waitReply{}, failure(err)
		}
		if token := request.Params.GetProgressToken(); token != nil {
			stop := notifyProgress(ctx, request.Session, token, input.ID)
			defer stop()
		}
		var result busclient.AwaitResult
		if budget <= 0 {
			err = context.DeadlineExceeded
		} else {
			result, err = client.Await(ctx, input.ID, budget)
		}
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			status, err := client.LastStatus(ctx, input.ID)
			if err != nil {
				return nil, waitReply{}, failure(err)
			}
			reply := waitReply{
				ID: input.ID, State: string(status.Status.State), Model: status.Model, Message: cmp.Or(statusText(status.Status), notes(status.Unclaimed, status.Waiting, status.CancelRequested)),
				Redacted: status.Redacted, RetryAfter: retryAfter, Skipped: status.Skipped,
			}
			// Past the bound wherry result waits to, the run's recorded task_ended_by plus the upload grace, or without one
			// the fallback from its submission, the run is reported as the CLI's exit 6 reports it: timed out, with no
			// retry_after.
			submitted, endBy := client.RecordedBounds(input.ID)
			if deadline := resultDeadline(submitted, endBy); !deadline.IsZero() && !time.Now().Before(deadline) {
				reply.RetryAfter = 0
				reply.Message = notes(resultTimeout(submitted, endBy), reply.Message)
			}
			return nil, reply, nil
		}
		if err != nil {
			return nil, waitReply{}, failure(err)
		}
		output := waitReply{
			ID: input.ID, State: string(result.Status.State), Model: result.Model, Message: cmp.Or(statusText(result.Status), notes(result.Unclaimed, result.CancelRequested)),
			Redacted: result.Redacted, Skipped: result.Skipped,
		}
		final := input.ID
		if len(result.Runs) > 1 {
			output.Runs, final = result.Runs, result.Runs[len(result.Runs)-1]
		}
		if result.Task != nil {
			for _, artifact := range result.Task.Artifacts {
				output.Artifacts = append(output.Artifacts, resultArtifact{ArtifactID: artifact.ArtifactID, Parts: resultParts(artifact.Parts)})
			}
		}
		collected, err := client.Collect(ctx, input.ID, result, "")
		if err != nil {
			// The model sees the step that failed; the server's log keeps the cause.
			logger.Print("tool failed: " + busclient.Redact(err.Error()))
			return nil, waitReply{}, collectFailure(err)
		}
		for _, written := range collected.Outputs {
			output.Outputs = append(output.Outputs, outputReply{Name: written.Name, Path: written.Path, SHA256: written.SHA256, Size: written.Size})
		}
		output.NotReturned = append(result.NotReturned, collected.Rejected...)
		output.FetchedRef, output.NotFetched = collected.Fetched, collected.NotFetched
		if reply := result.Reply; reply != nil {
			output.Reply = &replyFile{Rejected: reply.Rejected}
			if reply.Rejected == "" {
				paths, err := client.WriteOutputs(final, "", []busclient.Output{{Name: wire.ReplyFile, SHA256: reply.SHA256, Size: reply.Size, Data: reply.Data}})
				if err != nil {
					logger.Print("tool failed: " + busclient.Redact(err.Error()))
					return nil, waitReply{}, collectFailure(err)
				}
				output.Reply = &replyFile{Path: paths[0], SHA256: reply.SHA256, Size: reply.Size, Cut: reply.Cut}
			}
		}
		return nil, output, nil
	}

	mcp.AddTool(server, &mcp.Tool{
		Name:        "receivers",
		Description: "List the receivers of the pools this tenant may use: each receiver's status (live, stale, silent or ended), gated, for a live receiver, why its latest advertisement says it claimed no task (suspended, no_ready_offer, pending_run or draining; omitted while it claims), so a gated receiver takes no task even with ready profiles below replicas, and its capacity and in_flight, totals over all its profiles, and each profile's plugin type, agent and labels as wherry.operator records them, its replicas, in_flight and inline_limit from the offer, and its agent's models, default_model and credentials from the pool's terms, each credential with its target (env, the agent's environment variable, or file, in its credentials directory), its description from the pool's terms, optional, which a run goes without when this host cannot resolve it, configured, whether this host has a source for it, source (file, env or command from the host's configuration, or variable, the credential's own target variable read in the process that submits because it is in the host's catalog), variable and status (ready, unchecked for a command source, which receivers never runs, unset for an env source whose variable is unset, or missing) (the agent's credentials, delivered into the run; a git clone uses the host's git entries), with ready set when the receiver's live advertisement offers the profile. A live receiver with no ready profile lists every profile with ready false. A profile claims one task per replica, so a profile at in_flight equal to replicas is full whatever its receiver's totals; a queued run waits for its own profile. Read it to choose an agent and selector before submit.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ receiversInput) (*mcp.CallToolResult, receiversOutput, error) {
		client, err := connect()
		if err != nil {
			return nil, receiversOutput{}, failure(err)
		}
		receivers, err := client.Receivers(ctx)
		if err != nil {
			return nil, receiversOutput{}, failure(err)
		}
		return nil, receiversOutput{Receivers: receivers}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "submit",
		Description: fmt.Sprintf("Sign and publish a task to the task topic of a signature, an agent and all the labels of the profiles that carry it, that live receivers offer matching the selector, preferring the default agent, then the greatest spare capacity, then the lowest topic name. Returns the run id; set wait only for runs expected to finish within %d seconds, otherwise collect the result with result, or check the run without waiting with status. Inputs are local files the agent finds in WHERRY_INPUT_DIR; credentials are configured credentials it finds as files in WHERRY_CREDENTIALS_DIR or in environment variables, or, by name alone, the placed agent's own optional credentials the task cannot go without: every agent credential this host can resolve is delivered at its target, required or optional, named or not, and unsupplied lists the optional ones it could not resolve, which the run goes without; outputs are files it is asked to leave in WHERRY_OUTPUT_DIR, which result writes locally. Submit encrypts the agent's and the task's configured credentials into the task's envelope under the pool's key; the run needs nothing more from this host. Submit refuses with auth required when this host has no source for a credential the agent requires or the task names, when none of a catalog agent's model credentials resolves, and refuses a name that is not one of the placed agent's credentials.", int(waitBound.Seconds())),
	}, func(ctx context.Context, request *mcp.CallToolRequest, input submitInput) (*mcp.CallToolResult, submitOutput, error) {
		started := time.Now()
		submission, err := submitFromInput(input, openInput)
		defer closeFiles(submission.Files)
		if err != nil {
			return nil, submitOutput{}, err
		}
		client, err := connect()
		if err != nil {
			return nil, submitOutput{}, failure(err)
		}
		result, err := client.Submit(ctx, submission)
		if err != nil {
			return nil, submitOutput{}, failure(err)
		}
		output := submitOutput{ID: result.RunID, Pool: result.Pool, Command: waitCommand(result.RunID), Unsupplied: result.Unsupplied}
		if input.Wait {
			_, reply, err := collectResult(ctx, request, runInput{ID: result.RunID}, waitBound-time.Since(started))
			if err != nil {
				var safe *toolError
				if !errors.As(err, &safe) {
					_ = errors.As(toolFailure(err), &safe)
				}
				output.Error = safe
			} else {
				output.Result = &reply
			}
		}
		return nil, output, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name: "status",
		Description: "Return the latest verified state of a run without waiting. With id, runs holds that run; " +
			"without it, runs holds every run this host submitted or the tenant's receivers admitted, latest update first. " +
			"A run of this host no receiver has claimed says in message why it waits, from the receivers' advertisements, which can be up to 120 seconds old.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input statusInput) (*mcp.CallToolResult, statusOutput, error) {
		client, err := connect()
		if err != nil {
			return nil, statusOutput{}, failure(err)
		}
		var runs busclient.RunsResult
		if input.ID != "" {
			result, err := client.Status(ctx, input.ID)
			if err != nil {
				return nil, statusOutput{}, failure(err)
			}
			runs = busclient.RunsResult{Runs: []busclient.StatusResult{result}, Skipped: result.Skipped}
		} else if runs, err = client.Runs(ctx); err != nil {
			return nil, statusOutput{}, failure(err)
		}
		output := statusOutput{Runs: []runOutput{}, Skipped: runs.Skipped}
		for _, result := range runs.Runs {
			output.Runs = append(output.Runs, runOutput{
				ID: result.RunID, Pool: result.Pool, State: string(result.Status.State), Model: result.Model, Message: cmp.Or(statusText(result.Status), notes(result.Unclaimed, result.Waiting, result.CancelRequested)),
				Updated: result.Updated.UTC().Format(time.RFC3339Nano), Redacted: result.Redacted, ReplacedBy: result.Replacement,
			})
		}
		return nil, output, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name: "result",
		Description: fmt.Sprintf("Wait up to %d seconds for a run to finish. Returns the result, a failed, canceled or rejected state "+
			"with its message, or {id, state, message, redacted, retry_after} while the run is unfinished, message the latest status message, "+
			"such as the step the agent last reported: call result again. Past the run's task_ended_by plus the upload grace, or without a recorded "+
			"task_ended_by past 2h10m from its submission, an unfinished run returns no retry_after and message reports the timeout, as wherry result's exit 6 does. "+
			"A run whose task_started_by passed with no receiver claiming it returns its state and a message saying so, without retry_after. "+
			"A finished run's outputs are verified and written under this host's state directory; outputs lists their paths and hashes. "+
			"For a run submitted with git remote ., a returned changes.bundle is also fetched into that repository's branch wherry/<run>, named in fetched_ref; not_fetched says why it was not. "+
			"Use status to check a run without waiting.", int(waitBound.Seconds())),
	}, func(ctx context.Context, request *mcp.CallToolRequest, input runInput) (*mcp.CallToolResult, waitReply, error) {
		return collectResult(ctx, request, input, waitBound)
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "cancel",
		Description: "Sign and publish a cancel for a run whose result is no longer needed.",
		Annotations: &mcp.ToolAnnotations{IdempotentHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input runInput) (*mcp.CallToolResult, cancelOutput, error) {
		client, err := connect()
		if err != nil {
			return nil, cancelOutput{}, failure(err)
		}
		result, err := client.Cancel(ctx, input.ID)
		if err != nil {
			return nil, cancelOutput{}, failure(err)
		}
		return nil, cancelOutput{ID: result.RunID, Cancel: "requested", Skipped: result.Skipped}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "events",
		Description: "Return a run's verified status history and its transcript chunk counts per attempt. Read the lines with logs.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input runInput) (*mcp.CallToolResult, eventsOutput, error) {
		client, err := connect()
		if err != nil {
			return nil, eventsOutput{}, failure(err)
		}
		result, err := client.Events(ctx, input.ID)
		if err != nil {
			return nil, eventsOutput{}, failure(err)
		}
		output := eventsOutput{ID: input.ID, Events: []eventOutput{}, Chunks: []chunksOutput{}, Redacted: result.Redacted, Skipped: result.Skipped}
		for _, record := range result.Statuses {
			status := record.StatusUpdate.Status
			output.Events = append(output.Events, eventOutput{
				Attempt: record.Wherry.Attempt, Kind: record.Wherry.Kind, State: string(status.State),
				Receiver: record.Wherry.Receiver, Time: status.Timestamp, Message: statusText(status),
			})
		}
		for attempt, chunks := range result.Chunks {
			output.Chunks = append(output.Chunks, chunksOutput{Attempt: attempt, Chunks: chunks})
		}
		sort.Slice(output.Chunks, func(i, j int) bool {
			return output.Chunks[i].Attempt < output.Chunks[j].Attempt
		})
		return nil, output, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name: "logs",
		Description: fmt.Sprintf("Return a page of a run's transcript: the lines of the agent's standard output, redacted by the "+
			"receiver, in output order. The lines are the run's latest delivery attempt's, and before its first line, or while it "+
			"has none, each earlier attempt's line, so an adopted run reads from the start; each line names its attempt. A page "+
			"holds at most limit lines and %d KiB of JSON, which fits a harness's default tool output limit; a line too long for "+
			"a page alone comes back truncated, with its size in bytes. next_offset is set while more lines are on the feed. The "+
			"transcript grows while the run runs.", logsBytes>>10),
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input logsInput) (*mcp.CallToolResult, logsReply, error) {
		if input.Offset < 0 || input.Limit < 0 || input.Limit > logsMaxLimit {
			return nil, logsReply{}, invalidArgument(fmt.Sprintf("offset must not be negative, and limit must be 0 to %d", logsMaxLimit))
		}
		if input.Limit == 0 {
			input.Limit = logsLimit
		}
		client, err := connect()
		if err != nil {
			return nil, logsReply{}, failure(err)
		}
		result, err := client.Transcript(ctx, input.ID)
		if err != nil {
			return nil, logsReply{}, failure(err)
		}
		result.Lines = busclient.ViewLines(result.Lines)
		output := logsReply{ID: input.ID, Attempt: result.Attempt, Total: len(result.Lines), Lines: []logsLine{},
			Redacted: result.Redacted, Skipped: result.Skipped}
		size := func(value any) int {
			encoded, _ := json.Marshal(value, jsontext.EscapeForHTML(true), jsontext.EscapeForJS(true)) // as the SDK's encoding/json writes it
			return len(encoded)
		}
		output.NextOffset = len(result.Lines)
		room := logsBytes - size(output)
		output.NextOffset = 0
		for i := min(input.Offset, len(result.Lines)); i < len(result.Lines); i++ {
			line := logsLine{Seq: result.Lines[i].Seq, Attempt: result.Lines[i].Attempt}
			switch part := result.Lines[i].Part; {
			case part.MediaType == wire.ReferenceMediaType:
				var reference wire.Reference
				json.Unmarshal(part.Data, &reference) // validation decoded it
				line.Unstored = &unstoredLine{SHA256: reference.SHA256, Size: reference.Size}
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

	return server
}

// droppedFields refuses a submit whose arguments carry a field this version dropped, with the hint submit gives for the
// flag suffix, before the schema refuses the field as unknown.
func droppedFields(next mcp.MethodHandler) mcp.MethodHandler {
	return func(ctx context.Context, method string, request mcp.Request) (mcp.Result, error) {
		call, ok := request.(*mcp.CallToolRequest)
		if !ok || method != "tools/call" || call.Params == nil || call.Params.Name != "submit" {
			return next(ctx, method, request)
		}
		type dropped struct {
			Storage  jsontext.Value `json:"storage"`
			Required jsontext.Value `json:"required"`
		}
		var arguments struct {
			Inputs  []dropped `json:"inputs"`
			Outputs []dropped `json:"outputs"`
		}
		hint := ""
		if json.Unmarshal(call.Params.Arguments, &arguments) == nil {
			switch {
			case slices.ContainsFunc(arguments.Inputs, func(input dropped) bool { return input.Storage != nil }):
				hint = "inputs move to S3 by size; drop :s3"
			case slices.ContainsFunc(arguments.Outputs, func(output dropped) bool { return output.Storage != nil }):
				hint = "outputs move to S3 by size; drop :s3"
			case slices.ContainsFunc(arguments.Outputs, func(output dropped) bool { return output.Required != nil }):
				hint = "outputs are always required; drop :required"
			}
		}
		if hint == "" {
			return next(ctx, method, request)
		}
		var refused mcp.CallToolResult
		refused.SetError(invalidArgument(hint))
		return &refused, nil
	}
}

func submitFromInput(input submitInput, openInput func(string) (*os.File, error)) (busclient.SubmitInput, error) {
	if input.Prompt == "" {
		return busclient.SubmitInput{}, invalidArgument("prompt is required")
	}
	// @FILE opens through the input roots as an input does; the server's standard input carries the MCP protocol, so
	// @- is refused.
	prompt, err := flagValue("prompt", input.Prompt, openInput, nil)
	if err != nil {
		return busclient.SubmitInput{}, argumentError(err)
	}
	if prompt == "" {
		return busclient.SubmitInput{}, invalidArgument("prompt is empty")
	}
	input.Prompt = prompt
	selector := input.Selector
	if selector == nil {
		selector = []wire.Requirement{}
	}
	startBy, endBy, err := bounds(input.StartBy, input.EndBy, time.Now())
	if err != nil {
		return busclient.SubmitInput{}, err
	}
	submission := busclient.SubmitInput{
		Agent: input.Agent, Model: input.Model, Selector: selector, Prompt: input.Prompt,
		Continue: input.Continue, StartBy: startBy, EndBy: endBy,
	}
	if input.Git != nil {
		submission.Git = &wire.GitInput{Remote: input.Git.Remote, Ref: input.Git.Ref, Path: input.Git.Path}
		submission.GitChanges = input.Git.Changes
	}
	for _, credential := range input.Credentials {
		if credential.Env == "" && credential.File == "" {
			submission.Requested = append(submission.Requested, credential.Name)
			continue
		}
		submission.Credentials = append(submission.Credentials, wire.TaskCredential{Name: credential.Name, Env: credential.Env, File: credential.File})
	}
	for _, output := range input.Outputs {
		submission.Outputs = append(submission.Outputs, wire.ExpectedOutput{Name: output.Name})
	}
	for _, inputFile := range input.Inputs {
		file, err := openInput(inputFile.Path)
		if err != nil {
			closeFiles(submission.Files)
			return busclient.SubmitInput{}, err
		}
		submission.Files = append(submission.Files, busclient.S3File{Name: filepath.Base(inputFile.Path), Body: file})
	}
	return submission, nil
}

// argumentError is err as a tool error: the toolError it wraps, as an input the roots refuse carries, else
// INVALID_ARGUMENT with its text, which names a path and never a value.
func argumentError(err error) error {
	var failed *toolError
	if errors.As(err, &failed) {
		return failed
	}
	return invalidArgument(err.Error())
}

// bounds parses start_by and end_by and applies the CLI's defaults to unset ones.
func bounds(startBy, endBy string, now time.Time) (time.Time, time.Time, error) {
	parsed := []time.Time{now.Add(busclient.DefaultStartBy), now.Add(busclient.DefaultEndBy)}
	for i, field := range []struct{ name, text string }{{"start_by", startBy}, {"end_by", endBy}} {
		if field.text == "" {
			continue
		}
		value, err := busclient.ParseBound(field.text, now)
		if err != nil {
			return time.Time{}, time.Time{}, invalidArgument(field.name + ": " + err.Error())
		}
		parsed[i] = value
	}
	return parsed[0], parsed[1], nil
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
					ProgressToken: token, Progress: time.Since(start).Seconds(), Total: waitBound.Seconds(),
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

// resolveInput is a variable so tests can swap a link in between an input's resolution and its open.
var resolveInput = filepath.EvalSymlinks

// inputOpener opens an MCP input: a regular file beneath one of the configured input roots, or dir when there are
// none, and none of the host's own files, so a client steered by its prompt cannot send the host key, the bundle, the
// state or a credential, the storage keys included. Each input opens through the root containing its resolved path, so a
// link that escapes it, even one swapped in after resolution, is refused. The host's files are compared by identity,
// since path strings miss case-insensitive spellings and hard links. Relative paths resolve against dir.
func inputOpener(dir string, config busclient.Config) func(string) (*os.File, error) {
	configured := config.InputRoots
	if len(configured) == 0 {
		configured = []string{dir}
	}
	roots := map[string]*os.Root{}
	for _, path := range configured {
		resolved, err := filepath.EvalSymlinks(path)
		if err != nil {
			continue
		}
		if root, err := os.OpenRoot(resolved); err == nil {
			roots[resolved] = root
		}
	}
	protected := []string{config.StateDir, config.HostKey, config.Bundle}
	for _, source := range config.Credentials {
		if source.File != "" {
			protected = append(protected, source.File)
		}
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
		is := func(info os.FileInfo) bool {
			return slices.ContainsFunc(own, func(other os.FileInfo) bool { return os.SameFile(other, info) })
		}
		if !info.Mode().IsRegular() || is(info) {
			return false
		}
		for ancestor := filepath.Dir(resolved); ; ancestor = filepath.Dir(ancestor) {
			info, err := os.Stat(ancestor)
			if err != nil || is(info) {
				return false
			}
			if ancestor == filepath.Dir(ancestor) {
				return true
			}
		}
	}
	return func(path string) (*os.File, error) {
		if !filepath.IsAbs(path) {
			path = filepath.Join(dir, path)
		}
		resolved, err := resolveInput(path)
		if err != nil {
			return nil, refused
		}
		for path, root := range roots {
			relative, err := filepath.Rel(path, resolved)
			if err != nil || !filepath.IsLocal(relative) {
				continue
			}
			// O_NONBLOCK keeps opening a FIFO from blocking until a writer appears; the regular-file check refuses it.
			file, err := root.OpenFile(relative, os.O_RDONLY|syscall.O_NONBLOCK, 0)
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
		return nil, refused
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

// closeFiles closes the input files a submission opened.
func closeFiles(files []busclient.S3File) {
	for _, file := range files {
		file.Body.(io.Closer).Close()
	}
}

// notes joins the notes a host gives of a run no receiver has reported a status of.
func notes(texts ...string) string {
	return strings.Join(slices.DeleteFunc(texts, func(text string) bool { return text == "" }), "; ")
}
