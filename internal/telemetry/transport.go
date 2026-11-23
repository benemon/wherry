package telemetry

import (
	"context"
	"encoding/hex"
	"errors"
	"log"
	"slices"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const traceTopic = "wherry.traces"

// TraceTopic is the topic of tenant's spans, wherry.traces.<tenant>, or wherry.traces for spans of no tenant.
func TraceTopic(tenant string) string {
	if tenant == "" {
		return traceTopic
	}
	return traceTopic + "." + tenant
}

// The trace client's bounds: a small buffer; every produce request and record delivery ended within ExportBound; a
// short wait between produce retries and before a metadata refresh a failed produce asks for, since the client
// re-evaluates a buffered record, and fails one whose export has timed out, only when it is about to retry it.
const (
	MaxBufferedRecords = 256
	MaxBufferedBytes   = 4 << 20
	retryWait          = 250 * time.Millisecond
)

// TraceBatchMaxBytes is the trace client's batch maximum on every role, whatever the process's own client batches, and
// the trace topics' max.message.bytes. TraceRecordLimit is the largest value one trace record carries: the batch
// maximum less the record batch header, the record's framing and its key, a trace ID in hex. A group over it splits by
// spans; a span over it alone is dropped as oversize.
const (
	TraceBatchMaxBytes  = 1 << 20
	traceRecordOverhead = 256
	TraceRecordLimit    = TraceBatchMaxBytes - traceRecordOverhead
)

// ClientOpts are the options of the trace client built from base, the process's own Kafka options: the same brokers,
// credentials and trust, with idempotence off (traces are best-effort and a record is failed, never resent, once its
// export has timed out), the bounds above, and no topic creation.
func ClientOpts(base []kgo.Opt) []kgo.Opt {
	return append(slices.Clone(base),
		kgo.DisableIdempotentWrite(),
		kgo.ProduceRequestTimeout(ExportBound),
		kgo.RecordDeliveryTimeout(ExportBound),
		kgo.RequestTimeoutOverhead(ExportBound),
		kgo.RetryBackoffFn(func(int) time.Duration { return retryWait }),
		kgo.MetadataMinAge(retryWait),
		kgo.MaxBufferedRecords(MaxBufferedRecords),
		kgo.MaxBufferedBytes(MaxBufferedBytes),
		kgo.ProducerBatchMaxBytes(TraceBatchMaxBytes),
	)
}

// Transport is the otlptrace.Client that writes spans to the trace topics through a Kafka client of its own, built
// from the process's options under its principal: one OTLP record per tenant and trace, keyed by the trace ID.
type Transport struct {
	opts    []kgo.Opt
	kafka   *kgo.Client
	mu      sync.Mutex
	stopped bool
	uploads sync.WaitGroup
}

var errStopped = errors.New("tracing: transport stopped")

// NewTransport is a transport whose client Start builds from opts, the process's own Kafka options.
func NewTransport(opts []kgo.Opt) *Transport { return &Transport{opts: opts} }

// Start builds the client and connects it in the background, so the first export does not pay the session setup.
func (c *Transport) Start(context.Context) error {
	kafka, err := kgo.NewClient(ClientOpts(c.opts)...)
	if err != nil {
		return err
	}
	c.kafka = kafka
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), ShutdownBound)
		defer cancel()
		_ = kafka.Ping(ctx)
	}()
	return nil
}

// Stop refuses later uploads, waits within ctx for the ones in flight and for the client's buffer to drain, then
// closes the client, failing whatever is still unacknowledged.
func (c *Transport) Stop(ctx context.Context) error {
	c.mu.Lock()
	c.stopped = true
	c.mu.Unlock()
	if c.kafka == nil {
		return nil
	}
	defer c.kafka.Close()
	done := make(chan struct{})
	go func() {
		c.uploads.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	return c.kafka.Flush(ctx)
}

// UploadTraces produces spans' records in one synchronous call and returns once every record is acknowledged or
// failed, with one error per failed topic, the first first: records are failed per partition, so a refused topic
// fails its own alone. A record unacknowledged when ctx ends is failed by the client, reported as a timeout and
// never resent, so the next export starts with an empty buffer.
func (c *Transport) UploadTraces(ctx context.Context, spans []*tracepb.ResourceSpans) error {
	c.mu.Lock()
	if c.stopped || c.kafka == nil {
		c.mu.Unlock()
		return errStopped
	}
	c.uploads.Add(1)
	c.mu.Unlock()
	defer c.uploads.Done()
	records, oversize, err := Records(spans)
	if err != nil {
		return err
	}
	results := map[*kgo.Record]error{}
	for _, result := range c.kafka.ProduceSync(ctx, records...) {
		results[result.Record] = result.Err
	}
	var errs []error
	failed := map[string]bool{}
	for _, record := range records {
		if err := results[record]; err != nil && !failed[record.Topic] {
			failed[record.Topic] = true
			errs = append(errs, exportError(record.Topic, err))
		}
	}
	for _, topic := range oversize {
		errs = append(errs, &ExportError{Topic: topic, Class: ClassOversize})
	}
	return errors.Join(errs...)
}

// Records are the records of spans: one ExportTraceServiceRequest per tenant and trace, in the order the spans came,
// on the tenant's topic and keyed by the trace ID, each value within TraceRecordLimit. A span's tenant is its
// wherry.tenant attribute, else its resource's. A trace's group over the limit is split by spans into records under
// the same key; a span over the limit alone is left out, and oversize names each topic that lost one, once.
func Records(spans []*tracepb.ResourceSpans) (records []*kgo.Record, oversize []string, err error) {
	type key struct{ topic, trace string }
	var order []key
	groups := map[key]*tracepb.ResourceSpans{}
	scopes := map[key]map[*tracepb.ScopeSpans]*tracepb.ScopeSpans{}
	for _, rs := range spans {
		for _, ss := range rs.ScopeSpans {
			for _, span := range ss.Spans {
				tenant := tenantAttribute(span.Attributes)
				if tenant == "" {
					tenant = tenantAttribute(rs.Resource.GetAttributes())
				}
				k := key{TraceTopic(tenant), hex.EncodeToString(span.TraceId)}
				group := groups[k]
				if group == nil {
					group = &tracepb.ResourceSpans{Resource: rs.Resource, SchemaUrl: rs.SchemaUrl}
					groups[k], scopes[k] = group, map[*tracepb.ScopeSpans]*tracepb.ScopeSpans{}
					order = append(order, k)
				}
				scope := scopes[k][ss]
				if scope == nil {
					scope = &tracepb.ScopeSpans{Scope: ss.Scope, SchemaUrl: ss.SchemaUrl}
					scopes[k][ss] = scope
					group.ScopeSpans = append(group.ScopeSpans, scope)
				}
				scope.Spans = append(scope.Spans, span)
			}
		}
	}
	records = make([]*kgo.Record, 0, len(order))
	lost := map[string]bool{}
	for _, k := range order {
		value, err := proto.Marshal(request(groups[k]))
		if err != nil {
			return nil, nil, err
		}
		if len(value) <= TraceRecordLimit {
			records = append(records, &kgo.Record{Topic: k.topic, Key: []byte(k.trace), Value: value})
			continue
		}
		chunks, dropped := split(groups[k])
		if dropped > 0 && !lost[k.topic] {
			lost[k.topic] = true
			oversize = append(oversize, k.topic)
		}
		for _, chunk := range chunks {
			if value, err = proto.Marshal(request(chunk)); err != nil {
				return nil, nil, err
			}
			records = append(records, &kgo.Record{Topic: k.topic, Key: []byte(k.trace), Value: value})
		}
	}
	return records, oversize, nil
}

func request(group *tracepb.ResourceSpans) *coltracepb.ExportTraceServiceRequest {
	return &coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{group}}
}

// split is group, whose request is over TraceRecordLimit, in chunks whose requests each stay within it, and the count
// of spans over the limit alone, which no chunk holds. A chunk is bounded from its spans' encoded sizes, so its
// request may fall a few bytes short of the limit; a span that the bound puts over it is measured exactly, alone.
func split(group *tracepb.ResourceSpans) (chunks []*tracepb.ResourceSpans, dropped int) {
	empty := &tracepb.ResourceSpans{Resource: group.Resource, SchemaUrl: group.SchemaUrl}
	for _, ss := range group.ScopeSpans {
		empty.ScopeSpans = append(empty.ScopeSpans, &tracepb.ScopeSpans{Scope: ss.Scope, SchemaUrl: ss.SchemaUrl})
	}
	// base bounds a chunk's request beyond its span fields: the resource, every scope and the nested length prefixes,
	// each of which grows by at most 4 bytes as spans fill it.
	base := proto.Size(request(empty)) + 4*(1+len(group.ScopeSpans))
	var chunk *tracepb.ResourceSpans
	var source, scope *tracepb.ScopeSpans
	size := base
	flush := func() {
		if chunk != nil {
			chunks = append(chunks, chunk)
		}
		chunk, scope, size = nil, nil, base
	}
	for _, ss := range group.ScopeSpans {
		for _, span := range ss.Spans {
			field := proto.Size(span)
			field += 1 + protowire.SizeVarint(uint64(field))
			if base+field > TraceRecordLimit {
				alone := &tracepb.ResourceSpans{Resource: group.Resource, SchemaUrl: group.SchemaUrl, ScopeSpans: []*tracepb.ScopeSpans{{Scope: ss.Scope, SchemaUrl: ss.SchemaUrl, Spans: []*tracepb.Span{span}}}}
				if proto.Size(request(alone)) > TraceRecordLimit {
					dropped++
					continue
				}
				flush()
				chunks = append(chunks, alone)
				continue
			}
			if size+field > TraceRecordLimit {
				flush()
			}
			if chunk == nil {
				chunk = &tracepb.ResourceSpans{Resource: group.Resource, SchemaUrl: group.SchemaUrl}
			}
			if scope == nil || source != ss {
				source, scope = ss, &tracepb.ScopeSpans{Scope: ss.Scope, SchemaUrl: ss.SchemaUrl}
				chunk.ScopeSpans = append(chunk.ScopeSpans, scope)
			}
			scope.Spans = append(scope.Spans, span)
			size += field
		}
	}
	flush()
	return chunks, dropped
}

func tenantAttribute(attrs []*commonpb.KeyValue) string {
	for _, attr := range attrs {
		if attr.Key == string(AttrTenant) {
			return attr.GetValue().GetStringValue()
		}
	}
	return ""
}

// ExportError is a failed export to one topic. Its text names the topic and the failure's class, the Kafka error's
// name or a fixed word, and nothing of the spans.
type ExportError struct {
	Topic string
	Class string
	// Expected marks a failure a topic not yet created or a tenant whose entitlement has ended explains.
	Expected bool
}

func (e *ExportError) Error() string { return "tracing: export to " + e.Topic + " failed: " + e.Class }

func exportError(topic string, err error) *ExportError {
	var kafka *kerr.Error
	switch {
	case errors.As(err, &kafka):
		return &ExportError{topic, kafka.Message, errors.Is(err, kerr.UnknownTopicOrPartition) || errors.Is(err, kerr.TopicAuthorizationFailed)}
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, kgo.ErrRecordTimeout):
		return &ExportError{Topic: topic, Class: ClassTimeout}
	case errors.Is(err, context.Canceled):
		return &ExportError{Topic: topic, Class: ClassCanceled}
	case errors.Is(err, kgo.ErrClientClosed):
		return &ExportError{Topic: topic, Class: "client closed"}
	}
	return &ExportError{Topic: topic, Class: ClassError}
}

// An export failure is logged at most once per logInterval for each topic and class; one marked expected, once per
// expectedInterval.
const (
	logInterval      = time.Minute
	expectedInterval = time.Hour
)

// otherFailure is the fixed text of a failure the handler is given that is no export's: never the error's own text,
// since the handler is the process's and any SDK error reaches it.
const otherFailure = "tracing: export failed: " + ClassError

// reporter is the SDK's error handler, one per process: the place an export failure is logged, through the logger of
// the latest provider built.
type reporter struct {
	mu     sync.Mutex
	logger *log.Logger
	last   map[string]time.Time
	now    func() time.Time
}

var (
	reports   = &reporter{last: map[string]time.Time{}, now: time.Now}
	installed sync.Once
)

func reportTo(logger *log.Logger) {
	reports.mu.Lock()
	reports.logger = logger
	reports.mu.Unlock()
	installed.Do(func() { otel.SetErrorHandler(reports) })
}

// Handle logs each failure at the leaves of err, however it is wrapped: the exporter wraps the joined per-topic errors
// of an export in one more error.
func (r *reporter) Handle(err error) {
	for _, err := range leaves(err) {
		var export *ExportError
		if !errors.As(err, &export) {
			r.log(otherFailure, logInterval)
			continue
		}
		text, interval := export.Error(), logInterval
		if export.Expected {
			text, interval = text+" (expected for a topic not yet created or an ended entitlement)", expectedInterval
		}
		r.log(text, interval)
	}
}

func (r *reporter) log(text string, interval time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	if last, ok := r.last[text]; ok && now.Sub(last) < interval {
		return
	}
	r.last[text] = now
	if r.logger != nil {
		r.logger.Print(text)
	}
}

// leaves are the errors at the bottom of err's wrapping, through Unwrap() []error and Unwrap() error alike, or err
// itself.
func leaves(err error) []error {
	switch wrapped := err.(type) {
	case nil:
		return nil
	case interface{ Unwrap() []error }:
		var errs []error
		for _, err := range wrapped.Unwrap() {
			errs = append(errs, leaves(err)...)
		}
		return errs
	case interface{ Unwrap() error }:
		if inner := wrapped.Unwrap(); inner != nil {
			return leaves(inner)
		}
	}
	return []error{err}
}
