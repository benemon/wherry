package main

import (
	"bytes"
	"cmp"
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/netip"
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

	"github.com/benemon/wherry/internal/admin"
	busclient "github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/wire"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// waitBound stays below the shortest common harness tool-call timeout, so every model can loop on wait.
const waitBound = 45 * time.Second

// retryAfter is the pause, in seconds, that wait suggests before the next call while a run is unfinished.
const retryAfter = 5

const progressInterval = 5 * time.Second

// logsLimit and logsMaxLimit are the default and the largest page of transcript lines.
const logsLimit, logsMaxLimit = 100, 500

// logsBytes bounds a transcript page's JSON, within the 25,000-token default of Claude Code's MAX_MCP_OUTPUT_TOKENS.
const logsBytes = 24 << 10

// sessionTimeout closes an MCP session that sends no request for this long; a waiting client calls wait at least every
// waitBound, and a client whose session closed starts another.
const sessionTimeout = time.Hour

// answerRetry separates the server's attempts to answer seal requests after a failure.
const answerRetry = 5 * time.Second

func runMCP(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("mcp", flag.ContinueOnError)
	configFile := flags.String("config-file", "", "host configuration file")
	listen := flags.String("listen", "", "serve MCP Streamable HTTP at /mcp on this address instead of stdio")
	tokenFile := flags.String("token-file", "", "file holding the bearer token every HTTP request carries")
	tlsCert := flags.String("tls-cert", "", "TLS certificate file; with --tls-key, serves HTTPS")
	tlsKey := flags.String("tls-key", "", "TLS private key file")
	urlLifetime := flags.Duration("url-lifetime", time.Hour, "lifetime of the presigned URLs upload and wait return over HTTP")
	outputBound := flags.Int64("output-bound", logsBytes, "largest output wait returns over HTTP as an MCP resource, within a harness's tool output limit; a larger one, or one returned through S3, is a presigned GET URL")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("usage: wherry mcp [--config-file FILE] [--listen ADDR --token-file FILE [--tls-cert FILE --tls-key FILE]]")
	}
	token, err := listenToken(*listen, *tokenFile, *tlsCert, *tlsKey)
	if err != nil {
		return err
	}
	if *urlLifetime < time.Second {
		return errors.New("--url-lifetime must be at least 1s")
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
	if *listen == "" {
		return serveMCP(ctx, connect, logger, func(ctx context.Context) error {
			return newMCPServer(connect, inputReader(open), open, wait, logger, nil).Run(ctx, &mcp.StdioTransport{})
		})
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	server := newMCPServer(connect, inputReader(open), open, wait, logger, &remoteForms{urlLifetime: *urlLifetime, outputBound: *outputBound})
	return serveMCP(ctx, connect, logger, func(ctx context.Context) error {
		return serveHTTP(ctx, listener, server, token, *tlsCert, *tlsKey, logger)
	})
}

// serveMCP runs serve while it answers seal requests for the host's runs, so a run is sealed whether or not a client
// is connected.
func serveMCP(ctx context.Context, connect func() (*busclient.Host, error), logger *log.Logger, serve func(context.Context) error) error {
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	go answer(ctx, connect, logger)
	return serve(ctx)
}

// listenToken checks the HTTP flags and returns the token, nil for stdio. Every listener takes a token; one off
// loopback also takes TLS.
func listenToken(listen, tokenFile, tlsCert, tlsKey string) ([]byte, error) {
	if listen == "" {
		if tokenFile != "" || tlsCert != "" || tlsKey != "" {
			return nil, errors.New("--token-file, --tls-cert and --tls-key need --listen")
		}
		return nil, nil
	}
	if (tlsCert == "") != (tlsKey == "") {
		return nil, errors.New("--tls-cert and --tls-key go together")
	}
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		return nil, fmt.Errorf("--listen: %w", err)
	}
	if address, err := netip.ParseAddr(host); (err != nil || !address.IsLoopback()) && tlsCert == "" {
		return nil, errors.New("--listen off loopback needs --tls-cert and --tls-key")
	}
	if tokenFile == "" {
		return nil, errors.New("--listen needs --token-file")
	}
	file, err := os.Open(tokenFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("token file %s is open to group or others; make it mode 0600", tokenFile)
	}
	data, err := io.ReadAll(io.LimitReader(file, 4<<10))
	if err != nil {
		return nil, err
	}
	token := bytes.TrimSpace(data)
	if len(token) < 32 {
		return nil, fmt.Errorf("token file %s holds fewer than 32 characters", tokenFile)
	}
	return token, nil
}

// serveHTTP serves server's Streamable HTTP at /mcp on listener until ctx ends, to requests that carry the token.
func serveHTTP(ctx context.Context, listener net.Listener, server *mcp.Server, token []byte, tlsCert, tlsKey string, logger *log.Logger) error {
	mux := http.NewServeMux()
	mux.Handle("/mcp", mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, &mcp.StreamableHTTPOptions{SessionTimeout: sessionTimeout}))
	httpServer := &http.Server{Handler: bearer(token, mux, logger), ErrorLog: logger, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		httpServer.Close()
	}()
	logger.Printf("serving MCP at %s/mcp", listener.Addr())
	var err error
	if tlsCert != "" {
		err = httpServer.ServeTLS(listener, tlsCert, tlsKey)
	} else {
		err = httpServer.Serve(listener)
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// bearer passes on the requests that carry the token as a bearer token. A refusal's log line names the peer alone.
func bearer(token []byte, next http.Handler, logger *log.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		presented, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(presented), token) != 1 {
			logger.Printf("refused an MCP request from %s without a valid bearer token", r.RemoteAddr)
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// waitCommand returns the shell command that waits for a run and prints its reply, naming configFile, made absolute
// and quoted, when it is set.
func waitCommand(configFile string) (func(id string) string, error) {
	if configFile == "" {
		return func(id string) string { return "wherry wait " + id }, nil
	}
	path, err := filepath.Abs(configFile)
	if err != nil {
		return nil, err
	}
	quoted := "'" + strings.ReplaceAll(path, "'", `'\''`) + "'"
	return func(id string) string { return "wherry wait " + id + " --config-file " + quoted }, nil
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

// answer answers seal requests for as long as the server runs, so a credentialed task is sealed while the harness
// is between wait calls.
func answer(ctx context.Context, connect func() (*busclient.Host, error), logger *log.Logger) {
	for ctx.Err() == nil {
		host, err := connect()
		if err == nil {
			err = host.Answer(ctx)
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
	case errors.Is(err, busclient.ErrQueueDeadlinePastMaximum):
		return invalidArgument("the queue deadline is longer than the pool's max_queue_deadline")
	case errors.Is(err, busclient.ErrInvalidInput):
		return invalidArgument(err.Error())
	case errors.Is(err, context.Canceled):
		return &toolError{Code: "CANCELLED", Message: "the request was cancelled"}
	case errors.Is(err, admin.ErrNewerRecord):
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
	Remote  string `json:"remote" jsonschema:"repository the agent clones: an https or ssh url, or user@host:path"`
	Ref     string `json:"ref" jsonschema:"branch, tag or commit to check out"`
	Path    string `json:"path,omitempty" jsonschema:"checkout directory, relative; defaults to the repository name"`
	Changes string `json:"changes,omitempty" jsonschema:"patch or bundle: ask for the agent's commits back as the output changes.patch or changes.bundle"`
}

type credentialInput struct {
	Name string `json:"name" jsonschema:"credential name in this host's wherry config"`
	File string `json:"file,omitempty" jsonschema:"file name the agent finds it under in WHERRY_CREDENTIALS_DIR; exactly one of file and env"`
	Env  string `json:"env,omitempty" jsonschema:"environment variable the agent finds it in; exactly one of file and env"`
}

type fileInput struct {
	Path    string `json:"path,omitempty" jsonschema:"local file; refused over HTTP"`
	Name    string `json:"name,omitempty" jsonschema:"over HTTP: the file name the agent finds content or an upload under"`
	Content string `json:"content,omitempty" jsonschema:"over HTTP: the file's bytes, base64"`
	Upload  string `json:"upload,omitempty" jsonschema:"over HTTP: the id upload returned, for a file put to the pool's bucket, which the host verifies before it publishes the task"`
	Storage string `json:"storage,omitempty" jsonschema:"s3 to upload the file, up to 5 GiB, to the pool's bucket, from which the agent's plugin fetches it; absent, it goes in the task, or through the pool's bucket when over the broker's record limit or a profile's declared limit and the pool has storage and the host a storage credential, else submit refuses"`
}

type outputInput struct {
	Name     string `json:"name" jsonschema:"relative path the agent is asked to write in WHERRY_OUTPUT_DIR"`
	Required bool   `json:"required,omitempty" jsonschema:"fail the run when the output is not returned"`
	Storage  string `json:"storage,omitempty" jsonschema:"s3 to return the output through the pool's bucket, up to 5 GiB, instead of the feed"`
}

type submitInput struct {
	Prompt        string             `json:"prompt,omitempty" jsonschema:"the task for the remote agent; exactly one of prompt and prompt_file is required"`
	PromptFile    string             `json:"prompt_file,omitempty" jsonschema:"local file holding the prompt, read as an input is"`
	Agent         string             `json:"agent,omitempty" jsonschema:"agent to run, as the pool's terms name it; default: the pool's default agent"`
	Model         string             `json:"model,omitempty" jsonschema:"model the agent runs, one of its models as receivers lists them; default: the agent's default model"`
	Selector      []wire.Requirement `json:"selector,omitempty" jsonschema:"labels the profile must carry, each {key, operator, values}; operator is In or NotIn (with values), Exists or DoesNotExist (without); keys are arbitrary operator labels such as gpu or example.com/segment"`
	Git           *gitInput          `json:"git,omitempty" jsonschema:"repository the agent clones itself; its host must be one of the pool's vcs hosts, and its credential and an ssh remote's host key come from the git entry of this host's wherry config whose url is the longest prefix of the remote"`
	Inputs        []fileInput        `json:"inputs,omitempty" jsonschema:"local files delivered to the agent's WHERRY_INPUT_DIR under their base names"`
	Credentials   []credentialInput  `json:"credentials,omitempty" jsonschema:"configured credentials delivered to the agent, each as a file in WHERRY_CREDENTIALS_DIR or in an environment variable; no two share a target, and none takes the target of one of the agent's own credentials"`
	Outputs       []outputInput      `json:"outputs,omitempty" jsonschema:"files the agent is asked to leave in WHERRY_OUTPUT_DIR; wait writes the returned ones locally"`
	Deadline      string             `json:"deadline,omitempty" jsonschema:"task deadline, a Go duration such as 90m; default 2h"`
	QueueDeadline string             `json:"queue_deadline,omitempty" jsonschema:"queue deadline, a Go duration such as 30m; default 10m"`
}

type submitOutput struct {
	ID   string `json:"id" jsonschema:"run id"`
	Pool string `json:"pool" jsonschema:"pool the task was placed in"`
	Wait string `json:"wait,omitempty" jsonschema:"shell command that waits for the run to end and prints its reply; run it in the background; absent over HTTP"`
}

type uploadInput struct {
	Name   string `json:"name" jsonschema:"file name the agent finds the input under"`
	Size   int64  `json:"size" jsonschema:"the file's size in bytes, at most 5 GiB"`
	SHA256 string `json:"sha256" jsonschema:"the file's SHA-256, lowercase hex"`
}

type uploadOutput struct {
	ID      string `json:"id" jsonschema:"upload id: name it in a submit input as {upload: id}"`
	URL     string `json:"url" jsonschema:"presigned URL to PUT the file's bytes to, such as with curl --upload-file"`
	Expires string `json:"expires" jsonschema:"RFC 3339 time the URL expires"`
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
	Path   string `json:"path,omitempty" jsonschema:"where this host wrote the output; absent over HTTP"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
	remoteFile
}

// remoteFile is where a client over HTTP reads a file the host wrote.
type remoteFile struct {
	URI     string `json:"uri,omitempty" jsonschema:"over HTTP: the MCP resource resources/read returns the file's bytes from"`
	URL     string `json:"url,omitempty" jsonschema:"over HTTP, in place of uri for a file over the host's output bound or returned through S3: a presigned URL to GET the file from"`
	Expires string `json:"expires,omitempty" jsonschema:"RFC 3339 time url expires"`
}

type replyFile struct {
	Path     string `json:"path,omitempty" jsonschema:"where this host wrote the whole reply"`
	SHA256   string `json:"sha256,omitempty"`
	Size     int64  `json:"size,omitempty"`
	Cut      bool   `json:"cut,omitempty" jsonschema:"set when the reply was longer than the pool's reply limit and the file holds its first bytes"`
	Rejected string `json:"rejected,omitempty" jsonschema:"why this host could not verify the reply, of which message holds the first 64 KiB"`
	remoteFile
}

// outputURI is the MCP resource of a file the host wrote for a run.
func outputURI(runID, name string) string {
	return "wherry://runs/" + runID + "/outputs/" + name
}

// remoteForm is a file's HTTP form: a presigned GET URL for one over bound or stored in S3, else its resource URI. A
// host or pool without storage leaves every file a resource.
func remoteForm(runID, name string, size int64, stored bool, bound int64, presign func(name string) (string, time.Time, error)) (remoteFile, error) {
	if stored || size > bound {
		url, expires, err := presign(name)
		if err == nil {
			return remoteFile{URL: url, Expires: expires.UTC().Format(time.RFC3339)}, nil
		}
		if !errors.Is(err, busclient.ErrNoStorage) {
			return remoteFile{}, err
		}
	}
	return remoteFile{URI: outputURI(runID, name)}, nil
}

type waitReply struct {
	ID          string                  `json:"id"`
	State       string                  `json:"state"`
	Model       string                  `json:"model,omitempty" jsonschema:"model the run's placement names"`
	RetryAfter  int                     `json:"retry_after,omitempty" jsonschema:"set while the run is unfinished: seconds to wait before calling wait again"`
	Message     string                  `json:"message,omitempty" jsonschema:"status message: the agent's answer or what went wrong, a credential the run could not obtain, or this host's note that the queue deadline passed and no receiver claimed the run; an answer over 64 KiB is cut to its first 64 KiB, and reply names the whole"`
	Reply       *replyFile              `json:"reply,omitempty" jsonschema:"set when the agent's answer is over 64 KiB"`
	Artifacts   []resultArtifact        `json:"artifacts,omitempty"`
	Outputs     []outputReply           `json:"outputs,omitempty" jsonschema:"the run's outputs, verified and written locally"`
	NotReturned []wire.UnreturnedOutput `json:"not_returned,omitempty" jsonschema:"outputs the run did not return or this host could not verify, each with its reason"`
	Redacted    map[string]int          `json:"redacted,omitempty" jsonschema:"redaction markers in the message and artifacts by the credential or S3 object each names"`
	Skipped     int                     `json:"skipped,omitempty"`
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

// remoteForms are the settings of the HTTP forms of the tools.
type remoteForms struct {
	urlLifetime time.Duration
	outputBound int64
}

// newMCPServer serves the wherry tools, in their HTTP forms when remote is set. Every call uses the host connect
// returns, which the server's seal-request answering shares, so the process runs one tenant-feed follower.
func newMCPServer(connect func() (*busclient.Host, error), readInput func(string, int64) (wire.Part, error), openInput func(string) (*os.File, error), waitCommand func(id string) string, logger *log.Logger, remote *remoteForms) *mcp.Server {
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
	readOnly := &mcp.ToolAnnotations{ReadOnlyHint: true}

	mcp.AddTool(server, &mcp.Tool{
		Name:        "receivers",
		Description: "List the receivers of the pools this tenant may use: each receiver's status (live, stale, silent or ended) and capacity, and each profile's plugin type, agent and labels as wherry.admin records them, its inline_limit from the offer, and its agent's models, default_model and credentials from the pool's terms, each credential with optional and configured, whether this host has a source for it, with ready set when the receiver's live advertisement offers the profile. A live receiver with no ready profile lists every profile with ready false. Read it to choose an agent and selector before submit.",
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
		Description: "Sign and publish a task to the pool of the live receiver whose offers match the selector. Returns the run id at once; collect the result with wait, or check the run without waiting with status. Inputs are local files the agent finds in WHERRY_INPUT_DIR; over HTTP each is {name, content} with content base64, or {upload} naming an id from upload, and prompt_file is refused; credentials are configured credentials it finds as files in WHERRY_CREDENTIALS_DIR or in environment variables; outputs are files it is asked to leave in WHERRY_OUTPUT_DIR, which wait writes locally. While this server runs, it seals the agent's and the task's configured credentials to the receiver that claims the task when that receiver asks for them. Submit refuses with auth required when this host has no source for a credential the agent requires or the task names.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input submitInput) (*mcp.CallToolResult, submitOutput, error) {
		files := input.Inputs
		if remote != nil {
			input.Inputs = nil
		}
		submission, err := submitFromInput(input, readInput, openInput, remote != nil)
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
			return nil, submitOutput{}, failure(err)
		}
		var uploads []string
		if remote != nil {
			for _, file := range files {
				content, err := base64.StdEncoding.DecodeString(file.Content)
				switch {
				case file.Path != "" || file.Name == "" == (file.Upload == ""):
					return nil, submitOutput{}, invalidArgument("inputs over HTTP carry their content: each is {name, content} or {upload}")
				case err != nil:
					return nil, submitOutput{}, invalidArgument("an input's content is base64")
				case file.Upload != "":
					uploaded, err := client.Uploaded(ctx, file.Upload)
					if err != nil {
						return nil, submitOutput{}, failure(err)
					}
					submission.S3Inputs = append(submission.S3Inputs, uploaded)
					uploads = append(uploads, file.Upload)
				case file.Storage == wire.StorageS3:
					submission.S3Inputs = append(submission.S3Inputs, busclient.S3File{Name: file.Name, Body: bytes.NewReader(content)})
				case file.Storage == "":
					submission.Parts = append(submission.Parts, wire.Part{Raw: append(wire.StdBytes{}, content...), Filename: file.Name})
				default:
					return nil, submitOutput{}, invalidArgument("an input's storage is s3 or absent")
				}
			}
		}
		result, err := client.Submit(ctx, submission)
		if err != nil {
			return nil, submitOutput{}, failure(err)
		}
		for _, id := range uploads {
			if err := client.DeleteUpload(ctx, id); err != nil {
				logger.Print("delete upload " + id + ": " + busclient.Redact(err.Error()))
			}
		}
		if remote != nil {
			return nil, submitOutput{ID: result.RunID, Pool: result.Pool}, nil
		}
		return nil, submitOutput{ID: result.RunID, Pool: result.Pool, Wait: waitCommand(result.RunID)}, nil
	})

	if remote != nil {
		mcp.AddTool(server, &mcp.Tool{
			Name: "upload",
			Description: "Presign a PUT to the pool's bucket for an input too large to send inline. PUT the file's bytes to url before " +
				"it expires, then name the id in a submit input as {upload: id}; submit verifies the size and sha256 given here before " +
				"it publishes the task. Needs a pool with storage and a host storage credential.",
		}, func(ctx context.Context, _ *mcp.CallToolRequest, input uploadInput) (*mcp.CallToolResult, uploadOutput, error) {
			client, err := connect()
			if err != nil {
				return nil, uploadOutput{}, failure(err)
			}
			upload, err := client.PresignUpload(ctx, input.Name, input.Size, input.SHA256, remote.urlLifetime)
			if err != nil {
				return nil, uploadOutput{}, failure(err)
			}
			return nil, uploadOutput{ID: upload.ID, URL: upload.URL, Expires: upload.Expires.UTC().Format(time.RFC3339)}, nil
		})
	}

	mcp.AddTool(server, &mcp.Tool{
		Name: "status",
		Description: "Return the latest verified state of a run without waiting. With id, runs holds that run; " +
			"without it, runs holds every run this host submitted or the tenant's receivers admitted, latest update first.",
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
				ID: result.RunID, Pool: result.Pool, State: string(result.Status.State), Model: result.Model, Message: cmp.Or(statusText(result.Status), result.Unclaimed),
				Updated: result.Updated.UTC().Format(time.RFC3339Nano), Redacted: result.Redacted,
			})
		}
		return nil, output, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name: "wait",
		Description: fmt.Sprintf("Wait up to %d seconds for a run to finish. Returns the result, a failed, canceled or rejected state "+
			"with its message, or {id, state, retry_after} while the run is unfinished: call wait again. "+
			"A run whose queue deadline passed with no receiver claiming it returns its state and a message saying so, without retry_after. "+
			"A finished run's outputs are verified and written under this host's state directory; outputs lists their paths and hashes. "+
			"Over HTTP each output, and the reply file, has a uri that resources/read returns, or, over the host's output bound or returned through S3, a presigned url to GET. "+
			"Use status to check a run without waiting.", int(waitBound.Seconds())),
	}, func(ctx context.Context, request *mcp.CallToolRequest, input runInput) (*mcp.CallToolResult, waitReply, error) {
		client, err := connect()
		if err != nil {
			return nil, waitReply{}, failure(err)
		}
		if token := request.Params.GetProgressToken(); token != nil {
			stop := notifyProgress(ctx, request.Session, token, input.ID)
			defer stop()
		}
		result, err := client.Await(ctx, input.ID, waitBound)
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			status, err := client.Status(ctx, input.ID)
			if err != nil {
				return nil, waitReply{}, failure(err)
			}
			return nil, waitReply{ID: input.ID, State: string(status.Status.State), Model: status.Model, RetryAfter: retryAfter, Skipped: status.Skipped}, nil
		}
		if err != nil {
			return nil, waitReply{}, failure(err)
		}
		output := waitReply{
			ID: input.ID, State: string(result.Status.State), Model: result.Model, Message: cmp.Or(statusText(result.Status), result.Unclaimed),
			Redacted: result.Redacted, Skipped: result.Skipped,
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
		if reply := result.Reply; reply != nil {
			output.Reply = &replyFile{Rejected: reply.Rejected}
			if reply.Rejected == "" {
				paths, err := client.WriteOutputs(input.ID, "", []busclient.Output{{Name: wire.ReplyFile, SHA256: reply.SHA256, Size: reply.Size, Data: reply.Data}})
				if err != nil {
					logger.Print("tool failed: " + busclient.Redact(err.Error()))
					return nil, waitReply{}, collectFailure(err)
				}
				output.Reply = &replyFile{Path: paths[0], SHA256: reply.SHA256, Size: reply.Size, Cut: reply.Cut}
			}
		}
		if remote == nil {
			return nil, output, nil
		}
		presign := func(name string) (string, time.Time, error) {
			return client.PresignOutput(ctx, input.ID, name, remote.urlLifetime)
		}
		var links []mcp.Content
		form := func(name string, size int64, stored bool) (remoteFile, error) {
			file, err := remoteForm(input.ID, name, size, stored, remote.outputBound, presign)
			if err != nil {
				logger.Print("tool failed: " + busclient.Redact(err.Error()))
				return file, collectFailure(&busclient.CollectError{Step: "presign outputs", Err: err})
			}
			if file.URI != "" {
				links = append(links, &mcp.ResourceLink{URI: file.URI, Name: name, Size: &size})
			}
			return file, nil
		}
		for i, written := range output.Outputs {
			stored := slices.ContainsFunc(result.Stored, func(listed wire.ReturnedOutput) bool { return listed.Name == written.Name })
			if output.Outputs[i].remoteFile, err = form(written.Name, written.Size, stored); err != nil {
				return nil, waitReply{}, err
			}
			output.Outputs[i].Path = ""
		}
		if output.Reply != nil && output.Reply.Path != "" {
			if output.Reply.remoteFile, err = form(wire.ReplyFile, output.Reply.Size, false); err != nil {
				return nil, waitReply{}, err
			}
			output.Reply.Path = ""
		}
		encoded, err := json.Marshal(output)
		if err != nil {
			return nil, waitReply{}, err
		}
		return &mcp.CallToolResult{Content: append([]mcp.Content{&mcp.TextContent{Text: string(encoded)}}, links...)}, output, nil
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
		return nil, cancelOutput{ID: result.RunID, Skipped: result.Skipped}, nil
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

	mcp.AddTool(server, &mcp.Tool{
		Name: "cleanup",
		Description: "Delete every object under a run's prefix in its pool's bucket: its S3 inputs and outputs. wait does this once a " +
			"run is terminal and its S3 outputs verify; use cleanup for a run that ended while nothing waited for it. Over HTTP it also deletes the outputs wait wrote on the host.",
		Annotations: &mcp.ToolAnnotations{IdempotentHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input runInput) (*mcp.CallToolResult, cleanupOutput, error) {
		client, err := connect()
		if err != nil {
			return nil, cleanupOutput{}, failure(err)
		}
		// Over HTTP a run whose pool or host has no storage has nothing in S3 and its local outputs to delete.
		if err := client.Cleanup(ctx, input.ID); err != nil && (remote == nil || !errors.Is(err, busclient.ErrNoStorage)) {
			return nil, cleanupOutput{}, failure(err)
		}
		if remote != nil {
			if err := client.RemoveOutputs(input.ID); err != nil {
				return nil, cleanupOutput{}, failure(err)
			}
		}
		return nil, cleanupOutput{ID: input.ID}, nil
	})

	if remote != nil {
		server.AddResourceTemplate(&mcp.ResourceTemplate{
			Name: "output", URITemplate: "wherry://runs/{run}/outputs/{+name}",
			Description: "A file wait wrote for a run on this host: one of its outputs, or reply.txt; text or a base64 blob.",
		}, func(ctx context.Context, request *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
			uri := request.Params.URI
			runID, name, ok := strings.Cut(strings.TrimPrefix(uri, "wherry://runs/"), "/outputs/")
			if !ok || !strings.HasPrefix(uri, "wherry://runs/") {
				return nil, mcp.ResourceNotFoundError(uri)
			}
			client, err := connect()
			if err != nil {
				return nil, failure(err)
			}
			file, err := client.OpenOutput(runID, name)
			if err != nil {
				return nil, mcp.ResourceNotFoundError(uri)
			}
			defer file.Close()
			data, err := io.ReadAll(file)
			if err != nil {
				return nil, mcp.ResourceNotFoundError(uri)
			}
			contents := &mcp.ResourceContents{URI: uri, MIMEType: "application/octet-stream", Blob: data}
			if utf8.Valid(data) {
				contents.MIMEType, contents.Text, contents.Blob = "text/plain; charset=utf-8", string(data), nil
			}
			return &mcp.ReadResourceResult{Contents: []*mcp.ResourceContents{contents}}, nil
		})
	}
	return server
}

// submitFromInput reads input's prompt file and inputs through readInput and openInput; remote refuses a prompt file.
func submitFromInput(input submitInput, readInput func(string, int64) (wire.Part, error), openInput func(string) (*os.File, error), remote bool) (busclient.SubmitInput, error) {
	if remote && input.PromptFile != "" {
		return busclient.SubmitInput{}, invalidArgument("prompt_file is refused over HTTP; prompt carries the text")
	}
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
	var s3Inputs []string
	for _, file := range input.Inputs {
		if file.Name != "" || file.Content != "" || file.Upload != "" {
			return busclient.SubmitInput{}, invalidArgument("name, content and upload are the forms of inputs over HTTP; over stdio an input is a path")
		}
		switch file.Storage {
		case "":
			part, err := readInput(file.Path, math.MaxInt64)
			if err != nil {
				return busclient.SubmitInput{}, err
			}
			parts = append(parts, part)
		case wire.StorageS3:
			s3Inputs = append(s3Inputs, file.Path)
		default:
			return busclient.SubmitInput{}, invalidArgument("an input's storage is s3 or absent")
		}
	}
	deadline, queueDeadline, err := deadlines(input.Deadline, input.QueueDeadline)
	if err != nil {
		return busclient.SubmitInput{}, err
	}
	submission := busclient.SubmitInput{
		Agent: input.Agent, Model: input.Model, Selector: selector, Prompt: input.Prompt, Parts: parts,
		Deadline: deadline, QueueDeadline: queueDeadline,
	}
	if input.Git != nil {
		submission.Git = &wire.GitInput{Remote: input.Git.Remote, Ref: input.Git.Ref, Path: input.Git.Path}
		submission.GitChanges = input.Git.Changes
	}
	for _, credential := range input.Credentials {
		submission.Credentials = append(submission.Credentials, wire.TaskCredential{Name: credential.Name, Env: credential.Env, File: credential.File})
	}
	for _, output := range input.Outputs {
		submission.Outputs = append(submission.Outputs, wire.ExpectedOutput{Name: output.Name, Required: output.Required, Storage: output.Storage})
	}
	for _, path := range s3Inputs {
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

// deadlines parses the deadlines and applies the CLI's defaults to unset ones.
func deadlines(deadline, queueDeadline string) (time.Duration, time.Duration, error) {
	parsed := []time.Duration{busclient.DefaultDeadline, busclient.DefaultQueueDeadline}
	for i, text := range []string{deadline, queueDeadline} {
		if text == "" {
			continue
		}
		value, err := time.ParseDuration(text)
		if err != nil || value <= 0 {
			return 0, 0, invalidArgument("deadlines must be positive Go durations, such as 90m")
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

// inputOpener opens an MCP input: a regular file beneath one of the configured input roots, or dir when there are
// none, and none of the host's own files, so a client steered by its prompt cannot send the host key, the bundle, the
// state or a credential, the storage keys included. Files are compared by identity, since path strings miss case-insensitive
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
