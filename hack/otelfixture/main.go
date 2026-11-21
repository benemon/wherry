// Command otelfixture emits a known wherry topology as OpenTelemetry traces, so a trace backend's service map can
// be judged before wherry itself emits a span (spike brief agb-pyn4, step 1).
//
// The topology is two hosts, two pools with three receivers, and a custodian per pool. Every node has its own
// tracer provider and resource. Each run is one trace whose producer and consumer spans are parented through a
// W3C traceparent carried in a map, standing in for Kafka record headers. Consumer-side spans are exported in a
// later batch than their producers, as they would be from a real receiver.
//
// Spans go to the OTLP/HTTP endpoint in OTEL_EXPORTER_OTLP_ENDPOINT (or OTEL_EXPORTER_OTLP_TRACES_ENDPOINT).
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// Roles, which are also each resource's service.namespace.
const (
	roleHost      = "host"
	roleReceiver  = "receiver"
	roleCustodian = "custodian"
)

// Error classes: the only status descriptions a span may carry (brief rule 5).
const (
	classTimeout  = "timeout"
	classReleased = "released"
)

// Attribute keys the brief allowlists for wherry, plus the semconv 1.45 system name the Go SDK's semconv 1.43
// package predates.
const (
	attrSystemName = attribute.Key("messaging.system.name")
	attrRun        = attribute.Key("wherry.run")
	attrAttempt    = attribute.Key("wherry.attempt")
	attrTenant     = attribute.Key("wherry.tenant")
	attrHost       = attribute.Key("wherry.host")
	attrPool       = attribute.Key("wherry.pool")
	attrReceiver   = attribute.Key("wherry.receiver")
	attrProfile    = attribute.Key("wherry.profile")
	attrAgent      = attribute.Key("wherry.agent")
)

const (
	profileName = "mock"
	agentName   = "mock"

	// shutdownBound caps every flush and shutdown of a tracer provider.
	shutdownBound = 10 * time.Second
	// runSpacing separates the simulated start of one run from the next on the same edge.
	runSpacing = 2 * time.Second
	// successDuration and timeoutDuration are the simulated run lengths.
	successDuration = 4 * time.Second
	timeoutDuration = 30 * time.Second
)

var propagator = propagation.TraceContext{}

// config is the fixture's command line.
type config struct {
	variant string
	runs    int
	delay   time.Duration
}

// node is one process of a role, with its own tracer provider and resource.
type node struct {
	role     string
	service  string
	provider *sdktrace.TracerProvider
	tracer   trace.Tracer
	// Identity attributes every span of this node carries.
	identity []attribute.KeyValue
}

type host struct {
	tenant, name string
	node         *node
}

type pool struct {
	name      string
	receivers []*receiver
	custodian *node
}

type receiver struct {
	name string
	pool *pool
	node *node
}

// fixture holds the topology and the exporter its providers share.
type fixture struct {
	cfg       config
	hosts     []*host
	pools     []*pool
	providers []*sdktrace.TracerProvider
}

// summary is what the fixture prints at the end.
type summary struct {
	spans            int
	runs             int
	failedTrace      trace.TraceID
	redeliveredTrace trace.TraceID
}

// pending is a consumer-side span whose End is deferred to the second export batch.
type pending struct {
	span trace.Span
	end  time.Time
}

func main() {
	cfg, err := parseFlags(os.Args[1:], os.Stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(2)
		}
		fmt.Fprintln(os.Stderr, "otelfixture:", err)
		os.Exit(2)
	}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" && os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") == "" {
		fmt.Fprintln(os.Stderr, "otelfixture: set OTEL_EXPORTER_OTLP_ENDPOINT to the collector's OTLP/HTTP endpoint")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "otelfixture:", err)
		os.Exit(1)
	}
	f, err := newFixture(cfg, exporter)
	if err != nil {
		fmt.Fprintln(os.Stderr, "otelfixture:", err)
		os.Exit(1)
	}
	result, runErr := f.run(ctx)
	shutdownErr := f.shutdown(context.Background())
	if runErr != nil {
		fmt.Fprintln(os.Stderr, "otelfixture:", runErr)
		os.Exit(1)
	}
	if shutdownErr != nil {
		fmt.Fprintln(os.Stderr, "otelfixture: shutdown:", shutdownErr)
		os.Exit(1)
	}
	printSummary(os.Stdout, cfg, result)
}

func parseFlags(args []string, stderr io.Writer) (config, error) {
	var cfg config
	fs := flag.NewFlagSet("otelfixture", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&cfg.variant, "variant", "a", "naming variant: a (one service per receiver) or b (one service per pool)")
	fs.IntVar(&cfg.runs, "runs", 10, "runs per host -> receiver edge; one in five fails")
	fs.DurationVar(&cfg.delay, "delay", 60*time.Second, "how long consumer spans are exported after their producers")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if fs.NArg() != 0 {
		return cfg, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	return cfg, validate(cfg)
}

func validate(cfg config) error {
	if cfg.variant != "a" && cfg.variant != "b" {
		return fmt.Errorf("-variant must be a or b, not %q", cfg.variant)
	}
	if cfg.runs < 1 {
		return fmt.Errorf("-runs must be at least 1, not %d", cfg.runs)
	}
	if cfg.delay < 0 {
		return fmt.Errorf("-delay must not be negative")
	}
	return nil
}

func printSummary(w io.Writer, cfg config, s summary) {
	fmt.Fprintf(w, "variant %s: %d runs, %d spans\n", cfg.variant, s.runs, s.spans)
	fmt.Fprintf(w, "failed run trace id:      %s\n", traceIDString(s.failedTrace, "none: fewer than 5 runs per edge"))
	fmt.Fprintf(w, "redelivered run trace id: %s\n", traceIDString(s.redeliveredTrace, "none"))
}

func traceIDString(id trace.TraceID, missing string) string {
	if !id.IsValid() {
		return missing
	}
	return id.String()
}

// newFixture builds the topology. Every node gets a tracer provider with always-on sampling, its own resource and a
// batch processor over the shared exporter.
func newFixture(cfg config, exporter sdktrace.SpanExporter) (*fixture, error) {
	if err := validate(cfg); err != nil {
		return nil, err
	}
	f := &fixture{cfg: cfg}
	newNode := func(role, service string, extra ...attribute.KeyValue) (*node, error) {
		attrs := join([]attribute.KeyValue{semconv.ServiceName(service), semconv.ServiceNamespace(role)}, extra)
		res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(semconv.SchemaURL, attrs...))
		if err != nil {
			return nil, fmt.Errorf("resource for %s: %w", service, err)
		}
		provider := sdktrace.NewTracerProvider(
			sdktrace.WithSampler(sdktrace.AlwaysSample()),
			sdktrace.WithResource(res),
			sdktrace.WithSpanProcessor(sdktrace.NewBatchSpanProcessor(exporter)),
		)
		f.providers = append(f.providers, provider)
		return &node{role: role, service: service, provider: provider, tracer: provider.Tracer("wherry/hack/otelfixture")}, nil
	}

	for _, h := range []struct{ tenant, name string }{{"ben", "mini"}, {"alice", "laptop"}} {
		n, err := newNode(roleHost, "wherry-host."+h.tenant+"."+h.name)
		if err != nil {
			return nil, err
		}
		n.identity = []attribute.KeyValue{attrTenant.String(h.tenant), attrHost.String(h.name)}
		f.hosts = append(f.hosts, &host{tenant: h.tenant, name: h.name, node: n})
	}
	for _, p := range []struct {
		name      string
		receivers []string
	}{{"p1", []string{"r1", "r2"}}, {"p2", []string{"r3"}}} {
		custodian, err := newNode(roleCustodian, "wherry-custodian."+p.name)
		if err != nil {
			return nil, err
		}
		custodian.identity = []attribute.KeyValue{attrPool.String(p.name)}
		pl := &pool{name: p.name, custodian: custodian}
		for _, r := range p.receivers {
			var n *node
			switch cfg.variant {
			case "a":
				n, err = newNode(roleReceiver, "wherry-receiver."+p.name+"."+r)
			case "b":
				n, err = newNode(roleReceiver, "wherry-pool."+p.name, semconv.ServiceInstanceID(r))
			}
			if err != nil {
				return nil, err
			}
			n.identity = []attribute.KeyValue{attrPool.String(p.name), attrReceiver.String(r)}
			pl.receivers = append(pl.receivers, &receiver{name: r, pool: pl, node: n})
		}
		f.pools = append(f.pools, pl)
	}
	return f, nil
}

// run emits every run's spans in two export batches: producer-side spans first, then, after the configured delay,
// the consumer-side spans.
func (f *fixture) run(ctx context.Context) (summary, error) {
	var s summary
	var consumers []pending

	// The simulated timeline ends before now, so no span is exported before its own end time.
	span := time.Duration(f.cfg.runs)*runSpacing + timeoutDuration + time.Minute
	base := time.Now().Add(-span)

	edge := 0
	for _, h := range f.hosts {
		for _, p := range f.pools {
			for _, r := range p.receivers {
				for i := range f.cfg.runs {
					spec := runSpec{
						host:     h,
						receiver: r,
						index:    i,
						failed:   failsAt(i),
						start:    base.Add(time.Duration(i)*runSpacing + time.Duration(edge)*150*time.Millisecond),
					}
					// The first run from the first host to a pool's first receiver is redelivered; the second attempt
					// lands on the pool's next receiver, or the same one where the pool has only one.
					if i == 0 && h == f.hosts[0] && r == p.receivers[0] {
						spec.redelivered = true
						spec.secondAttemptOn = p.receivers[len(p.receivers)-1]
						if len(p.receivers) > 1 {
							spec.secondAttemptOn = p.receivers[1]
						}
					}
					traceID, pend := f.emit(spec)
					consumers = append(consumers, pend...)
					s.runs++
					if spec.failed && !s.failedTrace.IsValid() {
						s.failedTrace = traceID
					}
					if spec.redelivered && !s.redeliveredTrace.IsValid() {
						s.redeliveredTrace = traceID
					}
				}
				edge++
			}
		}
	}
	s.spans = f.producerSpans() + len(consumers)

	// Batch one: everything ended so far, which is the producer side.
	if err := f.flush(ctx); err != nil {
		return s, fmt.Errorf("flush producers: %w", err)
	}
	if err := sleep(ctx, f.cfg.delay); err != nil {
		return s, err
	}
	// Batch two: the consumer side.
	for _, c := range consumers {
		c.span.End(trace.WithTimestamp(c.end))
	}
	if err := f.flush(ctx); err != nil {
		return s, fmt.Errorf("flush consumers: %w", err)
	}
	return s, nil
}

// producerSpans counts the spans the producer batch holds: submit, send, open request, wrap and terminal per run.
func (f *fixture) producerSpans() int {
	n := 0
	for _, p := range f.pools {
		n += len(p.receivers)
	}
	return n * len(f.hosts) * f.cfg.runs * 5
}

// failsAt reports whether the run at index i on an edge fails: one in five, from the fifth.
func failsAt(i int) bool { return i%5 == 4 }

// runSpec describes one run from a host to a receiver.
type runSpec struct {
	host     *host
	receiver *receiver
	index    int
	failed   bool
	// redelivered runs have a first CONSUMER attempt on receiver that ends released, and a second attempt on
	// secondAttemptOn.
	redelivered     bool
	secondAttemptOn *receiver
	start           time.Time
}

func (spec runSpec) runID() string {
	sum := sha256.Sum256([]byte(strings.Join([]string{spec.host.tenant, spec.host.name, spec.receiver.pool.name,
		spec.receiver.name, fmt.Sprint(spec.index)}, "\x00")))
	return "run-" + hex.EncodeToString(sum[:])[:32]
}

// emit starts every span of one run. Producer-side spans end at once; consumer-side spans are returned for the
// caller to end in the later batch. It returns the run's trace id.
func (f *fixture) emit(spec runSpec) (trace.TraceID, []pending) {
	var consumers []pending
	at := func(d time.Duration) time.Time { return spec.start.Add(d) }
	runID := spec.runID()
	pool := spec.receiver.pool
	common := []attribute.KeyValue{attrRun.String(runID), attrProfile.String(profileName), attrAgent.String(agentName)}

	taskTopic := "wherry.pool." + pool.name + ".tasks." + agentName + "." + taskHash(agentName)
	custodianTopic := "wherry.pool." + pool.name + ".custodian"
	hostTopic := "wherry.host." + spec.host.tenant + "." + spec.host.name

	// submit (INTERNAL, root) at the host.
	hostNode := spec.host.node
	ctx, submit := hostNode.tracer.Start(context.Background(), "submit",
		trace.WithSpanKind(trace.SpanKindInternal), trace.WithTimestamp(at(0)),
		trace.WithAttributes(join(hostNode.identity, common)...))
	traceID := submit.SpanContext().TraceID()

	// send task (PRODUCER) at the host; the header travels in a map carrier.
	ctx, send := startProducer(ctx, hostNode, taskTopic, at(5*time.Millisecond), common)
	taskHeaders := propagation.MapCarrier{}
	propagator.Inject(ctx, taskHeaders)
	send.End(trace.WithTimestamp(at(25 * time.Millisecond)))
	submit.End(trace.WithTimestamp(at(40 * time.Millisecond)))

	taskAttempt := func(attempt int) []attribute.KeyValue {
		return join(common, []attribute.KeyValue{attrAttempt.Int(attempt), semconv.MessagingKafkaOffset(spec.index),
			semconv.MessagingConsumerGroupName("p-" + pool.name)})
	}
	attempt := 1
	processing := spec.receiver
	if spec.redelivered {
		// First attempt: claimed, then released at admission. No run follows it.
		_, released := startConsumer(spec.receiver.node, taskHeaders, taskTopic, at(60*time.Millisecond), taskAttempt(1))
		released.SetStatus(codes.Error, classReleased)
		consumers = append(consumers, pending{released, at(90 * time.Millisecond)})
		attempt = 2
		processing = spec.secondAttemptOn
	}
	receiverNode := processing.node

	// process task (CONSUMER, parent = header) at the receiver, ending at admission.
	ctx, process := startConsumer(receiverNode, taskHeaders, taskTopic, at(120*time.Millisecond), taskAttempt(attempt))
	consumers = append(consumers, pending{process, at(160 * time.Millisecond)})

	// run (INTERNAL child of the process span), outliving admission.
	runEnd := at(160*time.Millisecond + successDuration)
	ctx, run := receiverNode.tracer.Start(ctx, "run", trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithTimestamp(at(160*time.Millisecond)),
		trace.WithAttributes(join(receiverNode.identity, common, []attribute.KeyValue{attrAttempt.Int(attempt)})...))
	if spec.failed {
		runEnd = at(160*time.Millisecond + timeoutDuration)
		run.SetStatus(codes.Error, classTimeout)
	}
	consumers = append(consumers, pending{run, runEnd})

	// open request (PRODUCER) from the receiver to the custodian.
	openCtx, openRequest := startProducer(ctx, receiverNode, custodianTopic, at(170*time.Millisecond), common)
	openHeaders := propagation.MapCarrier{}
	propagator.Inject(openCtx, openHeaders)
	openRequest.End(trace.WithTimestamp(at(180 * time.Millisecond)))

	// custodian process (CONSUMER) and its wrap (PRODUCER) back to the receiver.
	custodianCtx, custodianProcess := startConsumer(pool.custodian, openHeaders, custodianTopic, at(200*time.Millisecond),
		join(common, []attribute.KeyValue{semconv.MessagingConsumerGroupName("p-" + pool.name + ".custodian")}))
	consumers = append(consumers, pending{custodianProcess, at(230 * time.Millisecond)})
	wrapCtx, wrap := startProducer(custodianCtx, pool.custodian, custodianTopic, at(220*time.Millisecond), common)
	wrapHeaders := propagation.MapCarrier{}
	propagator.Inject(wrapCtx, wrapHeaders)
	wrap.End(trace.WithTimestamp(at(228 * time.Millisecond)))

	// receiver process wrap (CONSUMER): the receiver reads the custodian topic by partition, so no group.
	_, processWrap := startConsumer(receiverNode, wrapHeaders, custodianTopic, at(250*time.Millisecond), common)
	consumers = append(consumers, pending{processWrap, at(270 * time.Millisecond)})

	// publish terminal (PRODUCER) from the receiver to the host, after the run.
	terminalCtx, terminal := startProducer(ctx, receiverNode, hostTopic, runEnd, common)
	if spec.failed {
		terminal.SetStatus(codes.Error, classTimeout)
	}
	terminalHeaders := propagation.MapCarrier{}
	propagator.Inject(terminalCtx, terminalHeaders)
	terminal.End(trace.WithTimestamp(runEnd.Add(10 * time.Millisecond)))

	// host process result (CONSUMER): the host reads its topic by partition, so no group.
	_, result := startConsumer(hostNode, terminalHeaders, hostTopic, runEnd.Add(40*time.Millisecond), common)
	consumers = append(consumers, pending{result, runEnd.Add(60 * time.Millisecond)})

	return traceID, consumers
}

// startProducer starts a PRODUCER span named for a send to topic, as a child of ctx.
func startProducer(ctx context.Context, n *node, topic string, start time.Time, extra []attribute.KeyValue) (context.Context, trace.Span) {
	attrs := join(messagingAttributes("send", semconv.MessagingOperationTypeSend, topic), n.identity, extra)
	return n.tracer.Start(ctx, "send "+topic, trace.WithSpanKind(trace.SpanKindProducer), trace.WithTimestamp(start),
		trace.WithAttributes(attrs...))
}

// startConsumer starts a CONSUMER span named for processing a record from topic. Its parent is the traceparent in
// headers, extracted into a fresh context the way a receiver would from a record.
func startConsumer(n *node, headers propagation.MapCarrier, topic string, start time.Time, extra []attribute.KeyValue) (context.Context, trace.Span) {
	parent := propagator.Extract(context.Background(), headers)
	attrs := join(messagingAttributes("process", semconv.MessagingOperationTypeProcess, topic), n.identity, extra)
	return n.tracer.Start(parent, "process "+topic, trace.WithSpanKind(trace.SpanKindConsumer), trace.WithTimestamp(start),
		trace.WithAttributes(attrs...))
}

// join concatenates attribute groups into a new slice, so no caller's slice is appended to in place.
func join(groups ...[]attribute.KeyValue) []attribute.KeyValue {
	n := 0
	for _, g := range groups {
		n += len(g)
	}
	out := make([]attribute.KeyValue, 0, n)
	for _, g := range groups {
		out = append(out, g...)
	}
	return out
}

// messagingAttributes are the semconv messaging attributes of a span. messaging.system.name is the semconv 1.45
// key; messaging.system is the older key SigNoz's messaging view still reads.
func messagingAttributes(operation string, operationType attribute.KeyValue, topic string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attrSystemName.String("kafka"),
		semconv.MessagingSystemKafka,
		operationType,
		semconv.MessagingOperationName(operation),
		semconv.MessagingDestinationName(topic),
	}
}

// taskHash stands in for the SHA-256 of a signature task's encoded value in the task topic's name.
func taskHash(agent string) string {
	sum := sha256.Sum256([]byte("wherry-otelfixture\x00" + agent))
	return hex.EncodeToString(sum[:])
}

// flush forces every provider's pending spans out within the shutdown bound.
func (f *fixture) flush(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, shutdownBound)
	defer cancel()
	var errs []error
	for _, p := range f.providers {
		errs = append(errs, p.ForceFlush(ctx))
	}
	return errors.Join(errs...)
}

// shutdown stops every provider within the shutdown bound.
func (f *fixture) shutdown(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, shutdownBound)
	defer cancel()
	var errs []error
	for _, p := range f.providers {
		errs = append(errs, p.Shutdown(ctx))
	}
	return errors.Join(errs...)
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
