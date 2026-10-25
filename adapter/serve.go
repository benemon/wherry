package adapter

import (
	"context"
	"errors"
	"io"
	"os"
	"runtime/debug"
	"sync"

	"github.com/benemon/wherry/adapter/internal/adapterv1"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// maxMessage bounds a message the plugin receives. A Dispatch carries a prompt from a run's submission, one Kafka
// record, which brokers cap at a few MiB; 64 MiB leaves room.
const maxMessage = 64 << 20

// Serve runs a plugin process for the receiver that started it, building one profile with factory for
// each Configure and routing every call to the profile it names. factory gets the logger whose lines the
// receiver relays. It returns when the receiver ends the link.
func Serve(factory func(Settings, hclog.Logger) (Adapter, error)) {
	// JSON lines keep their level when the receiver relays them; os.Stderr is read before plugin.Serve
	// redirects it.
	logger := hclog.New(&hclog.LoggerOptions{Output: os.Stderr, JSONFormat: true, Level: hclog.Info})
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: handshake,
		Plugins:         plugin.PluginSet{pluginName: &grpcPlugin{server: &server{factory: factory, logger: logger, profiles: map[string]Adapter{}}}},
		GRPCServer: func(options []grpc.ServerOption) *grpc.Server {
			return grpc.NewServer(append(options, grpc.MaxRecvMsgSize(maxMessage),
				grpc.ChainUnaryInterceptor(recoverUnary(logger)), grpc.ChainStreamInterceptor(recoverStream(logger)))...)
		},
		Logger: logger,
	})
}

// recovered turns a panic in one call into that call's error; the panic value is not logged, since it
// can hold the call's material. profile is read after the panic, since a stream names it in its first message.
func recovered(logger hclog.Logger, method string, profile *string, err *error) {
	if value := recover(); value != nil {
		logger.Error("profile call panicked", "method", method, "profile", *profile, "stack", string(debug.Stack()))
		*err = status.Error(codes.Internal, "profile "+*profile+" panicked in "+method)
	}
}

type named interface{ GetProfile() string }

func recoverUnary(logger hclog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (_ any, err error) {
		// go-plugin's own services share the server, and their requests name no profile.
		profile := ""
		if request, ok := req.(named); ok {
			profile = request.GetProfile()
		}
		defer recovered(logger, info.FullMethod, &profile, &err)
		return handler(ctx, req)
	}
}

func recoverStream(logger hclog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		wrapped := &namingStream{ServerStream: stream}
		defer recovered(logger, info.FullMethod, &wrapped.profile, &err)
		return handler(srv, wrapped)
	}
}

// namingStream records the profile a stream's request names as the handler receives it.
type namingStream struct {
	grpc.ServerStream
	profile string
}

func (s *namingStream) RecvMsg(m any) error {
	err := s.ServerStream.RecvMsg(m)
	if request, ok := m.(named); ok && err == nil {
		s.profile = request.GetProfile()
	}
	return err
}

type server struct {
	adapterv1.UnimplementedAdapterServer
	factory  func(Settings, hclog.Logger) (Adapter, error)
	logger   hclog.Logger
	mu       sync.RWMutex
	profiles map[string]Adapter
}

func (s *server) profile(name string) (Adapter, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	adapter, ok := s.profiles[name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "profile %s is not configured", name)
	}
	return adapter, nil
}

// callStatus carries the errors the core distinguishes across the link. Only an Error the adapter returns itself
// carries its op and class as a detail; one wrapped or joined travels as its text, which names each op.
func callStatus(err error) error {
	var tooLarge *InputTooLargeError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &tooLarge):
		return status.Error(codes.OutOfRange, tooLarge.Name)
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	}
	if failure, ok := err.(*Error); ok {
		// A detail whose text is not UTF-8 does not marshal; the error then travels as its text alone.
		detailed, detailErr := status.New(codes.Unknown, err.Error()).WithDetails(&adapterv1.ErrorDetail{Op: failure.Op, Class: failure.Class, Message: failure.Err.Error()})
		if detailErr == nil {
			return detailed.Err()
		}
	}
	return status.Error(codes.Unknown, err.Error())
}

func (s *server) Configure(_ context.Context, req *adapterv1.ConfigureRequest) (*adapterv1.ConfigureResponse, error) {
	adapter, err := s.factory(Settings{Name: req.Profile, Pool: req.Pool, Agent: req.Agent, Config: req.Config, Credential: req.Credential}, s.logger)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	s.mu.Lock()
	s.profiles[req.Profile] = adapter
	s.mu.Unlock()
	directories := adapter.Directories()
	return &adapterv1.ConfigureResponse{Directories: &adapterv1.Directories{
		Input: directories.Input, Output: directories.Output, Credentials: directories.Credentials,
	}, InlineLimit: adapter.InlineLimit()}, nil
}

func (s *server) Ready(ctx context.Context, req *adapterv1.ReadyRequest) (*adapterv1.ReadyResponse, error) {
	adapter, err := s.profile(req.Profile)
	if err != nil {
		return nil, err
	}
	ready, err := adapter.Ready(ctx)
	if err != nil {
		return nil, callStatus(err)
	}
	return &adapterv1.ReadyResponse{Ready: ready}, nil
}

func (s *server) Dispatch(ctx context.Context, req *adapterv1.DispatchRequest) (*adapterv1.DispatchResponse, error) {
	adapter, err := s.profile(req.Profile)
	if err != nil {
		return nil, err
	}
	message := req.GetRun()
	run := Run{
		ID: message.GetId(), Profile: message.GetProfile(), Agent: message.GetAgent(), Model: message.GetModel(), Attempt: message.GetAttempt(),
		Prompt: message.GetPrompt(),
	}
	for _, input := range message.GetInputs() {
		run.Inputs = append(run.Inputs, File{Name: input.GetName(), Data: input.GetData()})
	}
	for _, secret := range message.GetCredentials() {
		run.Credentials = append(run.Credentials, Secret{Name: secret.GetName(), Env: secret.GetEnv(), File: secret.GetFile(), Value: secret.GetValue()})
	}
	for _, fetch := range message.GetFetches() {
		run.Fetches = append(run.Fetches, Fetch{Name: fetch.GetName(), URL: fetch.GetUrl(), Size: fetch.GetSize(), SHA256: fetch.GetSha256()})
	}
	outcome, handle, err := adapter.Dispatch(ctx, run)
	if err != nil {
		return nil, callStatus(err)
	}
	return &adapterv1.DispatchResponse{Outcome: adapterv1.Outcome(outcome), Handle: handleMessage(handle)}, nil
}

func (s *server) Lookup(ctx context.Context, req *adapterv1.LookupRequest) (*adapterv1.LookupResponse, error) {
	adapter, err := s.profile(req.Profile)
	if err != nil {
		return nil, err
	}
	job, err := adapter.Lookup(ctx, req.RunId)
	if err != nil {
		return nil, callStatus(err)
	}
	return &adapterv1.LookupResponse{Job: &adapterv1.Job{
		State: adapterv1.JobState(job.State), Handle: handleMessage(job.Handle), Attempt: job.Attempt, Profile: job.Profile, Agent: job.Agent,
	}}, nil
}

// Follow runs the adapter's Follow on the handler's goroutine, so a panic in it is recovered, and sends
// its lines from another.
func (s *server) Follow(req *adapterv1.FollowRequest, stream grpc.ServerStreamingServer[adapterv1.FollowResponse]) error {
	adapter, err := s.profile(req.Profile)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	lines := make(chan Line)
	sent := make(chan error, 1)
	go func() {
		var err error
		for line := range lines {
			if err == nil {
				err = stream.Send(&adapterv1.FollowResponse{Event: &adapterv1.FollowResponse_Line{Line: &adapterv1.Line{Offset: line.Offset, Next: line.Next, Data: line.Data}}})
				if err != nil {
					cancel()
				}
			}
		}
		sent <- err
	}()
	terminal, err := func() (Terminal, error) {
		defer close(lines)
		return adapter.Follow(ctx, handleOf(req.Handle), req.From, lines)
	}()
	if sendErr := <-sent; sendErr != nil {
		return sendErr
	}
	if err != nil {
		return callStatus(err)
	}
	return stream.Send(&adapterv1.FollowResponse{Event: &adapterv1.FollowResponse_Terminal{Terminal: &adapterv1.Terminal{
		Started: terminal.Started, ExitCode: int64(terminal.ExitCode), Signal: terminal.Signal, Class: terminal.Class, Node: terminal.Node, Reason: terminal.Reason,
	}}})
}

func (s *server) Stop(ctx context.Context, req *adapterv1.StopRequest) (*adapterv1.StopResponse, error) {
	adapter, err := s.profile(req.Profile)
	if err != nil {
		return nil, err
	}
	return &adapterv1.StopResponse{}, callStatus(adapter.Stop(ctx, handleOf(req.Handle)))
}

// maxPiece bounds the data of one CollectResponse below gRPC's default 4 MiB receive limit.
const maxPiece = 1 << 20

// Collect runs the adapter's Collect on the handler's goroutine, as Follow does, and sends its outputs in pieces
// from another.
func (s *server) Collect(req *adapterv1.CollectRequest, stream grpc.ServerStreamingServer[adapterv1.CollectResponse]) error {
	adapter, err := s.profile(req.Profile)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	outputs := make(chan Output)
	sent := make(chan error, 1)
	go func() {
		var err error
		for output := range outputs {
			if err == nil {
				if err = sendOutput(stream, output); err != nil {
					cancel()
				}
			}
			if output.Body != nil {
				output.Body.Close()
			}
		}
		sent <- err
	}()
	limits := req.GetLimits()
	err = func() error {
		defer close(outputs)
		return adapter.Collect(ctx, handleOf(req.Handle), Limits{
			Files: int(limits.GetFiles()), FileBytes: limits.GetFileBytes(), TotalBytes: limits.GetTotalBytes(), Stream: limits.GetStream(), StreamBytes: limits.GetStreamBytes(),
		}, outputs)
	}()
	if sendErr := <-sent; sendErr != nil {
		return sendErr
	}
	return callStatus(err)
}

func sendOutput(stream grpc.ServerStreamingServer[adapterv1.CollectResponse], output Output) error {
	if output.Body != nil {
		return sendBody(stream, output)
	}
	data := output.Data
	for {
		piece := data[:min(len(data), maxPiece)]
		data = data[len(piece):]
		resp := &adapterv1.CollectResponse{Name: output.Name, Size: output.Size, Mode: uint32(output.Mode), More: len(data) > 0}
		if output.Data != nil {
			resp.Data = piece
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
		if len(data) == 0 {
			return nil
		}
	}
}

// sendBody sends a streamed output's Body in pieces as it reads them, then a last message with no data.
func sendBody(stream grpc.ServerStreamingServer[adapterv1.CollectResponse], output Output) error {
	piece := make([]byte, maxPiece)
	for {
		n, err := io.ReadFull(output.Body, piece)
		if n > 0 || err == nil {
			if sendErr := stream.Send(&adapterv1.CollectResponse{Name: output.Name, Size: output.Size, Mode: uint32(output.Mode), Data: piece[:n], More: true}); sendErr != nil {
				return sendErr
			}
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return stream.Send(&adapterv1.CollectResponse{Name: output.Name, Size: output.Size, Mode: uint32(output.Mode), Data: []byte{}})
		}
		if err != nil {
			return err
		}
	}
}

func (s *server) Purge(ctx context.Context, req *adapterv1.PurgeRequest) (*adapterv1.PurgeResponse, error) {
	adapter, err := s.profile(req.Profile)
	if err != nil {
		return nil, err
	}
	return &adapterv1.PurgeResponse{}, callStatus(adapter.Purge(ctx, req.RunId))
}

func (s *server) Release(ctx context.Context, req *adapterv1.ReleaseRequest) (*adapterv1.ReleaseResponse, error) {
	adapter, err := s.profile(req.Profile)
	if err != nil {
		return nil, err
	}
	return &adapterv1.ReleaseResponse{}, callStatus(adapter.Release(ctx, req.RunId))
}

func (s *server) Sweep(ctx context.Context, req *adapterv1.SweepRequest) (*adapterv1.SweepResponse, error) {
	adapter, err := s.profile(req.Profile)
	if err != nil {
		return nil, err
	}
	return &adapterv1.SweepResponse{}, callStatus(adapter.Sweep(ctx))
}

func (s *server) Metrics(ctx context.Context, req *adapterv1.MetricsRequest) (*adapterv1.MetricsResponse, error) {
	adapter, err := s.profile(req.Profile)
	if err != nil {
		return nil, err
	}
	samples, err := adapter.Metrics(ctx)
	if err != nil {
		return nil, callStatus(err)
	}
	resp := &adapterv1.MetricsResponse{}
	for _, sample := range samples {
		resp.Samples = append(resp.Samples, &adapterv1.Sample{
			Family: sample.Family, Labels: sample.Labels, Kind: adapterv1.Kind(sample.Kind), Value: sample.Value,
			Histogram: &adapterv1.Histogram{Buckets: sample.Histogram.Buckets, Sum: sample.Histogram.Sum, Count: sample.Histogram.Count},
		})
	}
	return resp, nil
}
