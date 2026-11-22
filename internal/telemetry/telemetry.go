// Package telemetry is wherry's OpenTelemetry tracing for the network-map spike (agb-pyn4, step 2): one tracer
// provider per role, W3C trace context in Kafka record headers, and the attribute allowlist every span keeps to.
//
// Tracing is on only when OTEL_EXPORTER_OTLP_ENDPOINT is set. Off, every provider is the no-op one and no record
// carries a header. WHERRY_OTEL_NAMING selects the service naming variant, a (default) or b.
package telemetry

import (
	"context"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Roles, each the service.namespace of its resource.
const (
	RoleHost      = "host"
	RoleReceiver  = "receiver"
	RoleCustodian = "custodian"
	RoleOperator  = "operator"
)

// NamingEnv selects the naming variant, a or b. EndpointEnv is the SDK's exporter variable that turns tracing on;
// TracesEndpointEnv is the SDK's traces-only override, which the exporter reads once tracing is on but which alone
// leaves it off (brief rule 3).
const (
	NamingEnv         = "WHERRY_OTEL_NAMING"
	EndpointEnv       = "OTEL_EXPORTER_OTLP_ENDPOINT"
	TracesEndpointEnv = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
)

// ShutdownBound caps a short process's flush and shutdown of its provider, so an unreachable endpoint adds no more
// than this to a command.
const ShutdownBound = 2 * time.Second

// Header names: the only record headers injected and extracted. The bounds are the W3C sizes: a version 00
// traceparent is exactly 55 bytes, and tracestate is bounded to 512.
const (
	traceparentHeader = "traceparent"
	tracestateHeader  = "tracestate"
	maxTraceparent    = 55
	maxTracestate     = 512
)

// Attribute keys the brief allowlists for wherry, plus the semconv 1.45 system name the Go SDK's semconv 1.43
// package predates.
const (
	AttrSystemName       = attribute.Key("messaging.system.name")
	AttrRun              = attribute.Key("wherry.run")
	AttrAttempt          = attribute.Key("wherry.attempt")
	AttrRetry            = attribute.Key("wherry.retry")
	AttrTenant           = attribute.Key("wherry.tenant")
	AttrHost             = attribute.Key("wherry.host")
	AttrPool             = attribute.Key("wherry.pool")
	AttrReceiver         = attribute.Key("wherry.receiver")
	AttrProfile          = attribute.Key("wherry.profile")
	AttrAgent            = attribute.Key("wherry.agent")
	AttrModel            = attribute.Key("wherry.model")
	AttrCredential       = attribute.Key("wherry.credential")
	AttrCredentialSource = attribute.Key("wherry.credential.source")
	AttrOutput           = attribute.Key("wherry.output")
	AttrCount            = attribute.Key("wherry.count")
	AttrSize             = attribute.Key("wherry.size")
	AttrChunks           = attribute.Key("wherry.chunks")
)

// ResourceAllowlist is every attribute key a resource may carry: the role's identity and the SDK's own.
var ResourceAllowlist = []attribute.Key{
	semconv.ServiceNameKey, semconv.ServiceNamespaceKey, semconv.ServiceInstanceIDKey,
	semconv.TelemetrySDKNameKey, semconv.TelemetrySDKLanguageKey, semconv.TelemetrySDKVersionKey,
}

// Allowlist is every attribute key a span, an event or a link may carry.
var Allowlist = []attribute.Key{
	AttrSystemName, semconv.MessagingSystemKey, semconv.MessagingOperationTypeKey, semconv.MessagingOperationNameKey,
	semconv.MessagingDestinationNameKey, semconv.MessagingKafkaOffsetKey, semconv.MessagingConsumerGroupNameKey,
	AttrRun, AttrAttempt, AttrRetry, AttrTenant, AttrHost, AttrPool, AttrReceiver, AttrProfile, AttrAgent, AttrModel,
	AttrCredential, AttrCredentialSource, AttrOutput, AttrCount, AttrSize, AttrChunks,
}

// Allowed reports whether key is in the allowlist.
func Allowed(key attribute.Key) bool { return slices.Contains(Allowlist, key) }

// ResourceAllowed reports whether key is in the resource allowlist.
func ResourceAllowed(key attribute.Key) bool { return slices.Contains(ResourceAllowlist, key) }

// Error classes: the fixed status descriptions this package sets. Roles add their own closed sets, the receiver's
// rejection labels and upload failure classes among them.
const (
	ClassReleased  = "released"
	ClassRejected  = "rejected"
	ClassFailed    = "failed"
	ClassCanceled  = "canceled"
	ClassTimeout   = "timeout"
	ClassError     = "error"
	ClassLinkLost  = "link lost"
	ClassTooLarge  = "too large"
	ClassFetch     = "fetch"
	ClassPlugin    = "plugin error"
	ClassRedirect  = "redirected"
	ClassStatus    = "status"
	ClassInvalid   = "invalid input"
	ClassUnplaced  = "no matching receiver"
	ClassStartBy   = "start-by limit"
	ClassNoKey     = "no pool key"
	ClassUnsealed  = "sealing failed"
	ClassNotLoaded = "read failed"
)

// Classes are the error classes this package sets.
var Classes = []string{
	ClassReleased, ClassRejected, ClassFailed, ClassCanceled, ClassTimeout, ClassError, ClassLinkLost, ClassTooLarge,
	ClassFetch, ClassPlugin, ClassRedirect, ClassStatus, ClassInvalid, ClassUnplaced, ClassStartBy, ClassNoKey,
	ClassUnsealed, ClassNotLoaded,
}

// NewExporter builds the exporter every provider sends to. A variable so tests can install an in-memory one.
var NewExporter = func(ctx context.Context) (sdktrace.SpanExporter, error) {
	return otlptracehttp.New(ctx)
}

var propagator = propagation.TraceContext{}

// Enabled reports whether tracing is on: OTEL_EXPORTER_OTLP_ENDPOINT is set. The traces-only variable does not count.
func Enabled() bool {
	return os.Getenv(EndpointEnv) != ""
}

// Names identify the process in its role; the ones a role does not have stay empty.
type Names struct {
	Tenant, Host, Pool, Receiver string
}

// Tracing is one role's tracer provider. A nil Tracing is off.
type Tracing struct {
	Tracer   trace.Tracer
	identity []attribute.KeyValue
	// tenant is the one tenant every span of the role belongs to, a host's; empty for the roles that serve many.
	tenant string
	sdk    *sdktrace.TracerProvider
	global bool
}

// New is role's tracing for the process named by names, built from the environment: off unless an endpoint is set.
func New(ctx context.Context, role string, names Names) (*Tracing, error) {
	identity := identityOf(role, names)
	tenant := ""
	if role == RoleHost {
		tenant = names.Tenant
	}
	if !Enabled() {
		return &Tracing{Tracer: noop.NewTracerProvider().Tracer("wherry"), identity: identity, tenant: tenant}, nil
	}
	// The resource is the role's identity and the SDK's own attributes alone: no environment, host or process detector.
	attrs := append(serviceOf(role, names, os.Getenv(NamingEnv) == "b"), semconv.ServiceNamespace(role))
	res, err := resource.New(ctx, resource.WithSchemaURL(semconv.SchemaURL), resource.WithTelemetrySDK(), resource.WithAttributes(attrs...))
	if err != nil {
		return nil, err
	}
	exporter, err := NewExporter(ctx)
	if err != nil {
		return nil, err
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(sdktrace.NewBatchSpanProcessor(resourced{exporter, res})),
	)
	t := &Tracing{Tracer: provider.Tracer("wherry"), identity: identity, tenant: tenant, sdk: provider}
	// The process's first provider stands as the global one; the custodian's, inside a receiver, does not.
	if role != RoleCustodian {
		if _, set := otel.GetTracerProvider().(*sdktrace.TracerProvider); !set {
			otel.SetTracerProvider(provider)
			t.global = true
		}
	}
	return t, nil
}

// resourced exports every span with res as its resource. The provider's WithResource folds OTEL_RESOURCE_ATTRIBUTES
// under the resource it is given, so what leaves the process is fixed here instead.
type resourced struct {
	sdktrace.SpanExporter
	res *resource.Resource
}

func (e resourced) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	fixed := make([]sdktrace.ReadOnlySpan, len(spans))
	for i, span := range spans {
		fixed[i] = withResource{span, e.res}
	}
	return e.SpanExporter.ExportSpans(ctx, fixed)
}

type withResource struct {
	sdktrace.ReadOnlySpan
	res *resource.Resource
}

func (s withResource) Resource() *resource.Resource { return s.res }

func serviceOf(role string, names Names, variantB bool) []attribute.KeyValue {
	switch role {
	case RoleHost:
		return []attribute.KeyValue{semconv.ServiceName("wherry-host." + names.Tenant + "." + names.Host)}
	case RoleReceiver:
		if variantB {
			return []attribute.KeyValue{semconv.ServiceName("wherry-pool." + names.Pool), semconv.ServiceInstanceID(names.Receiver)}
		}
		return []attribute.KeyValue{semconv.ServiceName("wherry-receiver." + names.Pool + "." + names.Receiver)}
	case RoleCustodian:
		return []attribute.KeyValue{semconv.ServiceName("wherry-custodian." + names.Pool)}
	}
	return []attribute.KeyValue{semconv.ServiceName("wherry-operator")}
}

func identityOf(role string, names Names) []attribute.KeyValue {
	switch role {
	case RoleHost:
		// The tenant comes through the context, as every role's does.
		return []attribute.KeyValue{AttrHost.String(names.Host)}
	case RoleReceiver:
		return []attribute.KeyValue{AttrPool.String(names.Pool), AttrReceiver.String(names.Receiver)}
	case RoleCustodian:
		return []attribute.KeyValue{AttrPool.String(names.Pool)}
	}
	return nil
}

// Enabled reports whether this tracing exports spans.
func (t *Tracing) Enabled() bool { return t != nil && t.sdk != nil }

// Shutdown flushes and stops the provider within ctx, and gives the global provider back to the no-op one when this
// was it.
func (t *Tracing) Shutdown(ctx context.Context) error {
	if !t.Enabled() {
		return nil
	}
	if t.global {
		otel.SetTracerProvider(noop.NewTracerProvider())
	}
	return t.sdk.Shutdown(ctx)
}

// Flush exports the spans ended so far within ctx.
func (t *Tracing) Flush(ctx context.Context) error {
	if !t.Enabled() {
		return nil
	}
	return t.sdk.ForceFlush(ctx)
}

// Close is Shutdown within ShutdownBound, for a short process.
func (t *Tracing) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), ShutdownBound)
	defer cancel()
	return t.Shutdown(ctx)
}

func (t *Tracing) tracer() trace.Tracer {
	if t == nil {
		return noop.NewTracerProvider().Tracer("wherry")
	}
	return t.Tracer
}

// attributes are the role's identity and the tenant ctx carries, for a span started under ctx.
func (t *Tracing) attributes(ctx context.Context) []attribute.KeyValue {
	if t == nil {
		return tenantOf(ctx)
	}
	return append(slices.Clip(t.identity), tenantOf(ctx)...)
}

// scope is ctx carrying the role's one tenant when it has one and ctx names none: a host's spans are all its tenant's,
// and the spans started under one of them inherit it through the context it returns.
func (t *Tracing) scope(ctx context.Context) context.Context {
	if t != nil && t.tenant != "" && TenantOf(ctx) == "" {
		return WithTenant(ctx, t.tenant)
	}
	return ctx
}

// Start starts a span of kind named name under ctx, carrying the role's identity, the tenant ctx carries and attrs.
func (t *Tracing) Start(ctx context.Context, name string, kind trace.SpanKind, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	ctx = t.scope(ctx)
	return t.tracer().Start(ctx, name, trace.WithSpanKind(kind), trace.WithAttributes(t.attributes(ctx)...), trace.WithAttributes(attrs...))
}

// Root starts a span with no parent, whatever ctx carries.
func (t *Tracing) Root(ctx context.Context, name string, kind trace.SpanKind, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	ctx = t.scope(ctx)
	return t.tracer().Start(ctx, name, trace.WithNewRoot(), trace.WithSpanKind(kind), trace.WithAttributes(t.attributes(ctx)...), trace.WithAttributes(attrs...))
}

// Producer starts the PRODUCER span of a send to topic, as a child of ctx.
func (t *Tracing) Producer(ctx context.Context, topic string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return t.Start(ctx, "send "+topic, trace.SpanKindProducer, append(messaging("send", semconv.MessagingOperationTypeSend, topic), attrs...)...)
}

// ProducerLinked is Producer as the root of a new trace, linked to link when it is valid.
func (t *Tracing) ProducerLinked(ctx context.Context, topic string, link trace.SpanContext, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return t.Linked(ctx, "send "+topic, trace.SpanKindProducer, link, append(messaging("send", semconv.MessagingOperationTypeSend, topic), attrs...)...)
}

// ProducerUnder is Producer as a child of parent when it is valid, in parent's trace; otherwise a root of its own.
func (t *Tracing) ProducerUnder(ctx context.Context, topic string, parent trace.SpanContext, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if parent.IsValid() {
		return t.Producer(trace.ContextWithRemoteSpanContext(ctx, parent), topic, attrs...)
	}
	return t.ProducerLinked(ctx, topic, trace.SpanContext{}, attrs...)
}

// Linked starts a span as the root of a new trace, linked to link when it is valid.
func (t *Tracing) Linked(ctx context.Context, name string, kind trace.SpanKind, link trace.SpanContext, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	ctx = t.scope(ctx)
	opts := []trace.SpanStartOption{trace.WithNewRoot(), trace.WithSpanKind(kind), trace.WithAttributes(t.attributes(ctx)...), trace.WithAttributes(attrs...)}
	if link.IsValid() {
		opts = append(opts, trace.WithLinks(trace.Link{SpanContext: link}))
	}
	return t.tracer().Start(ctx, name, opts...)
}

// Consumer starts the CONSUMER span of processing record. Its parent is the traceparent in the record's headers;
// without one, ctx's span.
func (t *Tracing) Consumer(ctx context.Context, record *kgo.Record, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return t.ConsumerNamed(ctx, record, "process "+record.Topic, attrs...)
}

// ConsumerNamed is Consumer with the span's name given.
func (t *Tracing) ConsumerNamed(ctx context.Context, record *kgo.Record, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	attrs = append(messaging("process", semconv.MessagingOperationTypeProcess, record.Topic), attrs...)
	return t.Start(t.Extract(ctx, record), name, trace.SpanKindConsumer, attrs...)
}

// messaging are the semconv messaging attributes of a span. messaging.system.name is the semconv 1.45 key;
// messaging.system is the older key SigNoz's messaging view still reads.
func messaging(operation string, operationType attribute.KeyValue, topic string) []attribute.KeyValue {
	return []attribute.KeyValue{
		AttrSystemName.String("kafka"), semconv.MessagingSystemKafka, operationType,
		semconv.MessagingOperationName(operation), semconv.MessagingDestinationName(topic),
	}
}

// Inject puts ctx's span context on record as a traceparent header. Off, the record is left as it is.
func (t *Tracing) Inject(ctx context.Context, record *kgo.Record) {
	if !t.Enabled() {
		return
	}
	propagator.Inject(ctx, carrier{&record.Headers})
}

// Extract returns ctx with the span context of record's traceparent header as its remote parent. A header that is
// missing, malformed or over its bound leaves ctx as it is; a tracestate is read and dropped, so no span carries it.
func (t *Tracing) Extract(ctx context.Context, record *kgo.Record) context.Context {
	if !t.Enabled() {
		return ctx
	}
	extracted := propagator.Extract(context.Background(), carrier{&record.Headers})
	sc := trace.SpanContextFromContext(extracted)
	if !sc.IsValid() {
		return ctx
	}
	return trace.ContextWithRemoteSpanContext(ctx, sc.WithTraceState(trace.TraceState{}))
}

// TraceParent is ctx's span context as a traceparent header value, or "" when it has none or tracing is off.
func (t *Tracing) TraceParent(ctx context.Context) string {
	if !t.Enabled() {
		return ""
	}
	headers := propagation.MapCarrier{}
	propagator.Inject(ctx, headers)
	return headers.Get(traceparentHeader)
}

// ParseTraceParent is the span context a traceparent header value names, invalid for any other text.
func ParseTraceParent(value string) trace.SpanContext {
	if len(value) > maxTraceparent {
		return trace.SpanContext{}
	}
	ctx := propagator.Extract(context.Background(), propagation.MapCarrier{traceparentHeader: value})
	return trace.SpanContextFromContext(ctx)
}

// carrier reads and writes the two trace context headers of a record, and no other.
type carrier struct {
	headers *[]kgo.RecordHeader
}

func (c carrier) Get(key string) string {
	bound := 0
	switch key {
	case traceparentHeader:
		bound = maxTraceparent
	case tracestateHeader:
		bound = maxTracestate
	default:
		return ""
	}
	for _, header := range *c.headers {
		if header.Key == key {
			if len(header.Value) > bound {
				return ""
			}
			return string(header.Value)
		}
	}
	return ""
}

func (c carrier) Set(key, value string) {
	if key != traceparentHeader && key != tracestateHeader {
		return
	}
	for i, header := range *c.headers {
		if header.Key == key {
			(*c.headers)[i].Value = []byte(value)
			return
		}
	}
	*c.headers = append(*c.headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func (c carrier) Keys() []string {
	keys := make([]string, 0, 2)
	for _, header := range *c.headers {
		if header.Key == traceparentHeader || header.Key == tracestateHeader {
			keys = append(keys, header.Key)
		}
	}
	return keys
}

// tenantKey is the context key of the tenant a run's spans carry.
type tenantKey struct{}

// WithTenant returns ctx carrying tenant: every span this package starts under it carries wherry.tenant set to it. The
// receiver sets it once a task's binding is known, so the run's spans need not name it one by one; an empty tenant
// names none.
func WithTenant(ctx context.Context, tenant string) context.Context {
	return context.WithValue(ctx, tenantKey{}, tenant)
}

// TenantOf is the tenant ctx carries, or "" outside a run.
func TenantOf(ctx context.Context) string {
	tenant, _ := ctx.Value(tenantKey{}).(string)
	return tenant
}

// tenantOf is wherry.tenant for a span started under ctx: the tenant it carries, or nothing.
func tenantOf(ctx context.Context) []attribute.KeyValue {
	if tenant := TenantOf(ctx); tenant != "" {
		return []attribute.KeyValue{AttrTenant.String(tenant)}
	}
	return nil
}

// Child starts an INTERNAL span under ctx's span, through that span's provider, so a child needs no tracer of its own.
// It carries the tenant ctx carries.
func Child(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return ChildOf(ctx, name, trace.SpanKindInternal, attrs...)
}

// ChildOf is Child with the span's kind given.
func ChildOf(ctx context.Context, name string, kind trace.SpanKind, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return trace.SpanFromContext(ctx).TracerProvider().Tracer("wherry").Start(ctx, name, trace.WithSpanKind(kind), trace.WithAttributes(tenantOf(ctx)...), trace.WithAttributes(attrs...))
}

// Event adds an event to ctx's span.
func Event(ctx context.Context, name string, attrs ...attribute.KeyValue) {
	trace.SpanFromContext(ctx).AddEvent(name, trace.WithAttributes(attrs...))
}

// Fail sets span's status to Error with class, a fixed text, as its description.
func Fail(span trace.Span, class string) {
	span.SetStatus(codes.Error, class)
}

// FailUnset is Fail for a span whose status is not yet set, so a class set earlier stands.
func FailUnset(span trace.Span, class string) {
	if read, ok := span.(sdktrace.ReadOnlySpan); ok && read.Status().Code != codes.Unset {
		return
	}
	Fail(span, class)
}

// FailCtx is Fail for ctx's span.
func FailCtx(ctx context.Context, class string) {
	Fail(trace.SpanFromContext(ctx), class)
}

// StateClass is the fixed class of a task state: its lowercase word, as the metrics label it.
func StateClass(state string) string {
	return strings.ToLower(strings.TrimPrefix(state, "TASK_STATE_"))
}
