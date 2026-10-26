package adapter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os/exec"
	"slices"
	"sync"
	"time"

	"github.com/benemon/wherry/adapter/internal/adapterv1"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Variables so tests can change them.
var (
	logLevel          = hclog.Info
	exitPoll          = 100 * time.Millisecond
	restartBackoff    = time.Second
	maxRestartBackoff = 30 * time.Second
	configureTimeout  = 30 * time.Second
	pingInterval      = 10 * time.Second
	pingTimeout       = 5 * time.Second
)

// Plugin is one plugin process and every profile configured in it. When the process exits, or fails or does not
// answer a ping, Plugin starts it again with back-off and configures every profile again; until then their calls
// return ErrLinkLost.
type Plugin struct {
	// resolve checks the plugin binary and returns its path, before every start.
	resolve func() (string, error)
	// path is the binary the latest start ran, for log lines.
	path string
	// cmd is the process the latest start ran, killed when it stops answering pings.
	cmd    *exec.Cmd
	logger hclog.Logger
	// configure serialises Configure with a restart, which configures every profile in settings.
	configure sync.Mutex
	settings  []Settings
	mu        sync.Mutex
	client    *plugin.Client
	// rpc is nil while the process is down.
	rpc      adapterv1.AdapterClient
	closed   bool
	restarts int64
}

// Launch starts the plugin binary resolve returns over mutual TLS, and calls resolve again before every
// restart; logs receives the link's and the plugin's log lines.
func Launch(resolve func() (string, error), logs io.Writer) (*Plugin, error) {
	p := &Plugin{resolve: resolve, logger: hclog.New(&hclog.LoggerOptions{Name: "plugin", Output: logs, Level: logLevel})}
	client, rpc, err := p.start()
	if err != nil {
		return nil, err
	}
	p.client, p.rpc = client, rpc
	go p.supervise(client, pingInterval)
	return p, nil
}

// start launches the process and configures every profile in settings; the caller holds configure.
func (p *Plugin) start() (*plugin.Client, adapterv1.AdapterClient, error) {
	path, err := p.resolve()
	if err != nil {
		return nil, nil, err
	}
	p.path, p.cmd = path, exec.Command(path)
	config := &plugin.ClientConfig{
		HandshakeConfig:  handshake,
		Plugins:          plugin.PluginSet{pluginName: &grpcPlugin{}},
		Cmd:              p.cmd,
		AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC},
		AutoMTLS:         true,
		Logger:           p.logger,
	}
	client := plugin.NewClient(config)
	protocol, err := client.Client()
	if err != nil {
		client.Kill()
		return nil, nil, fmt.Errorf("plugin %s: %w", path, err)
	}
	// AutoMTLS makes the certificate the plugin's handshake offers the link's only root; a plugin that
	// offered none would otherwise get a link without one.
	if config.TLSConfig == nil || config.TLSConfig.RootCAs == nil {
		client.Kill()
		return nil, nil, fmt.Errorf("plugin %s offered no mutual TLS", path)
	}
	raw, err := protocol.Dispense(pluginName)
	if err != nil {
		client.Kill()
		return nil, nil, err
	}
	rpc := raw.(adapterv1.AdapterClient)
	for _, settings := range p.settings {
		if _, err := configure(rpc, settings); err != nil {
			client.Kill()
			return nil, nil, fmt.Errorf("profile %s: %w", settings.Name, err)
		}
	}
	return client, rpc, nil
}

// configure builds settings' profile in the plugin and returns what the plugin reports of it.
func configure(rpc adapterv1.AdapterClient, settings Settings) (*adapterv1.ConfigureResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), configureTimeout)
	defer cancel()
	resp, err := rpc.Configure(ctx, &adapterv1.ConfigureRequest{
		Profile: settings.Name, Pool: settings.Pool, Agent: settings.Agent, Config: settings.Config, Credential: settings.Credential,
	})
	if err != nil {
		return nil, callError(ctx, err)
	}
	return resp, nil
}

// supervise restarts the process each time it exits, until Close. A process that fails or does not answer a ping every
// interval is killed, since go-plugin's Kill waits on a graceful shutdown a hung process never makes.
func (p *Plugin) supervise(client *plugin.Client, interval time.Duration) {
	for {
		pinged := time.Now()
		for !client.Exited() {
			time.Sleep(exitPoll)
			if time.Since(pinged) < interval {
				continue
			}
			if err := ping(client); err != nil && !client.Exited() {
				p.logger.Warn("plugin did not answer a ping; killing it", "path", p.path, "error", err)
				_ = p.cmd.Process.Kill()
			}
			pinged = time.Now()
		}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return
		}
		p.rpc = nil
		p.mu.Unlock()
		p.logger.Warn("plugin exited; restarting", "path", p.path)
		for backoff := restartBackoff; ; backoff = min(2*backoff, maxRestartBackoff) {
			time.Sleep(backoff)
			p.mu.Lock()
			closed := p.closed
			p.mu.Unlock()
			if closed {
				return
			}
			p.configure.Lock()
			next, rpc, err := p.start()
			p.configure.Unlock()
			if err != nil {
				p.logger.Error("plugin restart failed", "path", p.path, "error", err)
				continue
			}
			p.mu.Lock()
			if p.closed {
				p.mu.Unlock()
				next.Kill()
				return
			}
			p.client, p.rpc = next, rpc
			p.restarts++
			p.mu.Unlock()
			p.logger.Info("plugin restarted", "path", p.path)
			client = next
			break
		}
	}
}

// ping checks the process answers go-plugin's health check within pingTimeout.
func ping(client *plugin.Client) error {
	protocol, err := client.Client()
	if err != nil {
		return err
	}
	answered := make(chan error, 1)
	go func() { answered <- protocol.Ping() }()
	select {
	case err := <-answered:
		return err
	case <-time.After(pingTimeout):
		return fmt.Errorf("no answer within %s", pingTimeout)
	}
}

// Configure builds a profile in the plugin and returns its handle; the plugin configures it again after a restart.
func (p *Plugin) Configure(settings Settings) (Adapter, error) {
	p.configure.Lock()
	defer p.configure.Unlock()
	rpc, err := p.link()
	if err != nil {
		return nil, err
	}
	resp, err := configure(rpc, settings)
	if err != nil {
		return nil, err
	}
	p.settings = append(p.settings, settings)
	directories := resp.GetDirectories()
	return &remote{plugin: p, name: settings.Name, directories: Directories{
		Input: directories.GetInput(), Output: directories.GetOutput(), Credentials: directories.GetCredentials(),
	}, inlineLimit: resp.GetInlineLimit()}, nil
}

// Close stops the process and its restarts.
func (p *Plugin) Close() {
	p.mu.Lock()
	p.closed, p.rpc = true, nil
	client := p.client
	p.mu.Unlock()
	client.Kill()
}

// Restarts is the number of times the process has been started again since Launch.
func (p *Plugin) Restarts() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.restarts
}

func (p *Plugin) link() (adapterv1.AdapterClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.rpc == nil {
		return nil, ErrLinkLost
	}
	return p.rpc, nil
}

// callError maps a call's status back to the errors the core distinguishes. A connection closed under a
// call that its caller did not cancel is a lost link.
func callError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.Canceled:
		return ErrLinkLost
	case codes.DeadlineExceeded:
		return context.DeadlineExceeded
	case codes.ResourceExhausted:
		return ErrMessageTooLarge
	case codes.OutOfRange:
		return &InputTooLargeError{Name: status.Convert(err).Message()}
	}
	call := status.Convert(err)
	for _, detail := range call.Details() {
		if detail, ok := detail.(*adapterv1.ErrorDetail); ok {
			return &Error{Op: detail.Op, Class: detail.Class, Err: errors.New(detail.Message)}
		}
	}
	return errors.New(call.Message())
}

// remote is one profile in a plugin process.
type remote struct {
	plugin      *Plugin
	name        string
	directories Directories
	inlineLimit int64
}

func (r *remote) Directories() Directories { return r.directories }
func (r *remote) InlineLimit() int64       { return r.inlineLimit }

func (r *remote) Ready(ctx context.Context) (bool, error) {
	rpc, err := r.plugin.link()
	if err != nil {
		return false, err
	}
	resp, err := rpc.Ready(ctx, &adapterv1.ReadyRequest{Profile: r.name})
	if err != nil {
		return false, callError(ctx, err)
	}
	return resp.Ready, nil
}

func (r *remote) Dispatch(ctx context.Context, run Run) (Outcome, Handle, error) {
	rpc, err := r.plugin.link()
	if err != nil {
		return OutcomeUnknown, Handle{}, err
	}
	message := &adapterv1.Run{
		Id: run.ID, Profile: run.Profile, Agent: run.Agent, Model: run.Model, Attempt: run.Attempt, Prompt: run.Prompt,
	}
	for _, input := range run.Inputs {
		message.Inputs = append(message.Inputs, &adapterv1.File{Name: input.Name, Data: input.Data})
	}
	for _, secret := range run.Credentials {
		message.Credentials = append(message.Credentials, &adapterv1.Secret{Name: secret.Name, Env: secret.Env, File: secret.File, Value: secret.Value})
	}
	for _, fetch := range run.Fetches {
		message.Fetches = append(message.Fetches, &adapterv1.Fetch{Name: fetch.Name, Url: fetch.URL, Size: fetch.Size, Sha256: fetch.SHA256})
	}
	resp, err := rpc.Dispatch(ctx, &adapterv1.DispatchRequest{Profile: r.name, Run: message})
	if err != nil {
		err = callError(ctx, err)
		var tooLarge *InputTooLargeError
		if errors.As(err, &tooLarge) {
			return OutcomeNothingCreated, Handle{}, err
		}
		return OutcomeUnknown, Handle{}, err
	}
	return Outcome(resp.Outcome), handleOf(resp.Handle), nil
}

func (r *remote) Lookup(ctx context.Context, runID string) (Job, error) {
	rpc, err := r.plugin.link()
	if err != nil {
		return Job{}, err
	}
	resp, err := rpc.Lookup(ctx, &adapterv1.LookupRequest{Profile: r.name, RunId: runID})
	if err != nil {
		return Job{}, callError(ctx, err)
	}
	job := resp.GetJob()
	return Job{State: JobState(job.GetState()), Handle: handleOf(job.GetHandle()), Attempt: job.GetAttempt(), Profile: job.GetProfile(), Agent: job.GetAgent()}, nil
}

func (r *remote) Follow(ctx context.Context, handle Handle, from int64, lines chan<- Line) (Terminal, error) {
	rpc, err := r.plugin.link()
	if err != nil {
		return Terminal{}, err
	}
	stream, err := rpc.Follow(ctx, &adapterv1.FollowRequest{Profile: r.name, Handle: handleMessage(handle), From: from})
	if err != nil {
		return Terminal{}, callError(ctx, err)
	}
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return Terminal{}, errors.New("follow ended without a terminal")
		}
		if err != nil {
			return Terminal{}, callError(ctx, err)
		}
		if terminal := resp.GetTerminal(); terminal != nil {
			return Terminal{Started: terminal.Started, ExitCode: int(terminal.ExitCode), Signal: terminal.Signal, Class: terminal.Class, Node: terminal.Node, Reason: terminal.Reason}, nil
		}
		line := resp.GetLine()
		select {
		case lines <- Line{Offset: line.GetOffset(), Next: line.GetNext(), Data: line.GetData()}:
		case <-ctx.Done():
			return Terminal{}, ctx.Err()
		}
	}
}

func (r *remote) Stop(ctx context.Context, handle Handle) error {
	rpc, err := r.plugin.link()
	if err != nil {
		return err
	}
	_, err = rpc.Stop(ctx, &adapterv1.StopRequest{Profile: r.name, Handle: handleMessage(handle)})
	return callError(ctx, err)
}

// Collect bounds each output's data by limits.FileBytes as it joins the pieces, so a plugin cannot grow one without
// limit. An output limits.Stream names whose first message carries data is handed on at once, its Body fed the pieces as
// they arrive and bounded by limits.StreamBytes; pieces arriving after the Body is closed are dropped.
func (r *remote) Collect(ctx context.Context, handle Handle, limits Limits, outputs chan<- Output) (err error) {
	rpc, err := r.plugin.link()
	if err != nil {
		return err
	}
	stream, err := rpc.Collect(ctx, &adapterv1.CollectRequest{Profile: r.name, Handle: handleMessage(handle), Limits: &adapterv1.Limits{
		Files: int64(limits.Files), FileBytes: limits.FileBytes, TotalBytes: limits.TotalBytes, Stream: limits.Stream, StreamBytes: limits.StreamBytes,
	}})
	if err != nil {
		return callError(ctx, err)
	}
	var output *Output
	var body *io.PipeWriter
	var streamed int64
	defer func() {
		if body != nil {
			body.CloseWithError(errors.Join(err, errors.New("collect ended inside an output")))
		}
	}()
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			if output != nil {
				return errors.New("collect ended inside an output")
			}
			return nil
		}
		if err != nil {
			return callError(ctx, err)
		}
		if output == nil {
			output = &Output{Name: resp.Name, Size: resp.Size, Mode: fs.FileMode(resp.Mode)}
			if resp.Data != nil && slices.Contains(limits.Stream, resp.Name) {
				var reader *io.PipeReader
				reader, body = io.Pipe()
				output.Body = reader
				select {
				case outputs <- *output:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		} else if resp.Name != output.Name {
			return errors.New("collect interleaved two outputs")
		}
		if body != nil {
			if streamed += int64(len(resp.Data)); streamed > limits.StreamBytes {
				return errors.New("collect sent a streamed output over the stream limit")
			}
			_, _ = body.Write(resp.Data) // fails only once the receiver of the output has closed it
			if !resp.More {
				body.Close()
				output, body, streamed = nil, nil, 0
			}
			continue
		}
		if int64(len(output.Data)+len(resp.Data)) > limits.FileBytes {
			return errors.New("collect sent an output over the file limit")
		}
		if resp.Data != nil {
			output.Data = append(append([]byte{}, output.Data...), resp.Data...)
		}
		if resp.More {
			continue
		}
		select {
		case outputs <- *output:
		case <-ctx.Done():
			return ctx.Err()
		}
		output = nil
	}
}

func (r *remote) Purge(ctx context.Context, runID string) error {
	rpc, err := r.plugin.link()
	if err != nil {
		return err
	}
	_, err = rpc.Purge(ctx, &adapterv1.PurgeRequest{Profile: r.name, RunId: runID})
	return callError(ctx, err)
}

func (r *remote) Release(ctx context.Context, runID string) error {
	rpc, err := r.plugin.link()
	if err != nil {
		return err
	}
	_, err = rpc.Release(ctx, &adapterv1.ReleaseRequest{Profile: r.name, RunId: runID})
	return callError(ctx, err)
}

func (r *remote) Sweep(ctx context.Context) error {
	rpc, err := r.plugin.link()
	if err != nil {
		return err
	}
	_, err = rpc.Sweep(ctx, &adapterv1.SweepRequest{Profile: r.name})
	return callError(ctx, err)
}

func (r *remote) Metrics(ctx context.Context) ([]Sample, error) {
	rpc, err := r.plugin.link()
	if err != nil {
		return nil, err
	}
	resp, err := rpc.Metrics(ctx, &adapterv1.MetricsRequest{Profile: r.name})
	if err != nil {
		return nil, callError(ctx, err)
	}
	samples := make([]Sample, 0, len(resp.GetSamples()))
	for _, sample := range resp.GetSamples() {
		histogram := sample.GetHistogram()
		samples = append(samples, Sample{
			Family: sample.GetFamily(), Labels: sample.GetLabels(), Kind: Kind(sample.GetKind()), Value: sample.GetValue(),
			Histogram: Histogram{Buckets: histogram.GetBuckets(), Sum: histogram.GetSum(), Count: histogram.GetCount()},
		})
	}
	return samples, nil
}
