package telemetry

import (
	"cmp"
	"context"
	"encoding/hex"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
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

// Transport is the otlptrace.Client that writes spans to the trace topics through the process's own Kafka client,
// under its principal: one OTLP record per tenant and trace, keyed by the trace ID.
type Transport struct {
	kafka   *kgo.Client
	mu      sync.Mutex
	stopped bool
	uploads sync.WaitGroup
}

var errStopped = errors.New("tracing: transport stopped")

// NewTransport is a transport over kafka, which it never closes.
func NewTransport(kafka *kgo.Client) *Transport { return &Transport{kafka: kafka} }

// Start is nothing: the client is connected already.
func (c *Transport) Start(context.Context) error { return nil }

// Stop refuses later uploads and waits within ctx for the ones in flight. The client stays open.
func (c *Transport) Stop(ctx context.Context) error {
	c.mu.Lock()
	c.stopped = true
	c.mu.Unlock()
	done := make(chan struct{})
	go func() {
		c.uploads.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// UploadTraces produces spans' records together and returns once every record is acknowledged or ctx ends, with one
// error per failed topic, the first first: franz-go fails records per partition, so a refused topic fails its own
// records alone. A record ctx leaves unacknowledged is reported as a timeout; the client completes or fails it with
// its batch, and nothing is retried here.
func (c *Transport) UploadTraces(ctx context.Context, spans []*tracepb.ResourceSpans) error {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return errStopped
	}
	c.uploads.Add(1)
	c.mu.Unlock()
	defer c.uploads.Done()
	records, err := Records(spans)
	if err != nil {
		return err
	}
	var (
		mu     sync.Mutex
		wg     sync.WaitGroup
		failed = map[string]error{}
		acked  = map[string]int{}
	)
	wg.Add(len(records))
	for _, record := range records {
		c.kafka.Produce(ctx, record, func(record *kgo.Record, err error) {
			defer wg.Done()
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				acked[record.Topic]++
			case failed[record.Topic] == nil:
				failed[record.Topic] = err
			}
		})
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
	mu.Lock()
	defer mu.Unlock()
	var errs []error
	counts := map[string]int{}
	for _, record := range records {
		counts[record.Topic]++
	}
	for _, record := range records {
		topic := record.Topic
		if counts[topic] == 0 {
			continue
		}
		switch {
		case failed[topic] != nil:
			errs = append(errs, exportError(topic, failed[topic]))
		case acked[topic] < counts[topic]:
			errs = append(errs, exportError(topic, cmp.Or(ctx.Err(), context.DeadlineExceeded)))
		}
		counts[topic] = 0
	}
	return errors.Join(errs...)
}

// Records are the records of spans: one ExportTraceServiceRequest per tenant and trace, in the order the spans came,
// on the tenant's topic and keyed by the trace ID. A span's tenant is its wherry.tenant attribute, else its resource's.
func Records(spans []*tracepb.ResourceSpans) ([]*kgo.Record, error) {
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
	records := make([]*kgo.Record, 0, len(order))
	for _, k := range order {
		value, err := proto.Marshal(&coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{groups[k]}})
		if err != nil {
			return nil, err
		}
		records = append(records, &kgo.Record{Topic: k.topic, Key: []byte(k.trace), Value: value})
	}
	return records, nil
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

func (r *reporter) Handle(err error) {
	for _, err := range leaves(err) {
		text, interval := "tracing: "+err.Error(), logInterval
		var export *ExportError
		if errors.As(err, &export) {
			text = export.Error()
			if export.Expected {
				text, interval = text+" (expected for a topic not yet created or an ended entitlement)", expectedInterval
			}
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

// leaves are the errors joined into err, or err itself.
func leaves(err error) []error {
	if err == nil {
		return nil
	}
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok {
		return []error{err}
	}
	var errs []error
	for _, err := range joined.Unwrap() {
		errs = append(errs, leaves(err)...)
	}
	return errs
}
