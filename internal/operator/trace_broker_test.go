package operator

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/kafkatest"
	"github.com/benemon/wherry/internal/telemetry"
	"github.com/benemon/wherry/internal/telemetry/teletest"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// Brief test 4, on the real broker: a host writes and reads its tenant's trace topic and can neither read nor write
// another tenant's, which that tenant's host writes (the positive control); a receiver cannot write a non-entitled
// tenant's; the operator principal describes a share group after delegation. Brief test 6 on the topic: the largest
// permitted export, compression off, from the host's trace client configuration, is accepted by the topic.
func TestTraceTopicGrantsOnRealBroker(t *testing.T) {
	if os.Getenv("WHERRY_TRUST_BROKER") == "" {
		t.Skip("WHERRY_TRUST_BROKER is not set")
	}
	f := provision(t)
	ctx := context.Background()
	tracesA, tracesB := telemetry.TraceTopic(f.tenantA), telemetry.TraceTopic(f.tenantB)
	if err := kafkatest.Led(ctx, f.service.Operator, tracesA, tracesB, telemetry.TraceTopic("")); err != nil {
		t.Fatal(err)
	}
	write := func(client *kgo.Client, topic, value string) error {
		return client.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte("trace"), Value: []byte(value)}).FirstErr()
	}
	hostA, hostB := f.connect(t, TenantPrincipal(f.tenantA, f.hostA1)), f.connect(t, TenantPrincipal(f.tenantB, f.hostB1))
	receiverA := f.connect(t, ReceiverPrincipal(f.receiverA))

	t.Run("a host writes and reads its tenant's trace topic", func(t *testing.T) {
		if err := write(hostA, tracesA, "host-a"); err != nil {
			t.Fatal(err)
		}
		if err := write(f.connect(t, TenantPrincipal(f.tenantA, f.hostA2)), tracesA, "host-a2"); err != nil {
			t.Fatal(err)
		}
		if err := write(receiverA, tracesA, "receiver-a"); err != nil {
			t.Fatal(err)
		}
		expectReadable(t, hostA, tracesA)
		seen := readAll(t, f.connect(t, TenantPrincipal(f.tenantA, f.hostA1)), tracesA)
		for _, value := range []string{"host-a", "host-a2", "receiver-a"} {
			if !seen[value] {
				t.Fatalf("host a read %v from %s, want %s among them", seen, tracesA, value)
			}
		}
	})

	t.Run("a host neither writes nor reads another tenant's trace topic", func(t *testing.T) {
		if err := write(hostB, tracesB, "host-b"); err != nil {
			t.Fatalf("positive control, tenant b's host on %s: %v", tracesB, err)
		}
		expectDenied(t, write(hostA, tracesB, "forged"))
		expectDenied(t, listEnd(hostA, tracesB))
		expectDenied(t, write(hostA, telemetry.TraceTopic(""), "untenanted"))
	})

	t.Run("a receiver cannot write a non-entitled tenant's trace topic", func(t *testing.T) {
		if err := write(receiverA, telemetry.TraceTopic(""), "receiver"); err != nil {
			t.Fatal(err)
		}
		expectDenied(t, write(receiverA, tracesB, "non-entitled"))
	})

	t.Run("the operator principal describes a share group", func(t *testing.T) {
		member := f.connect(t, ReceiverPrincipal(f.receiverA), kgo.ConsumeTopics(modelTopic(f.poolA)), kgo.ShareGroup(PoolGroup(f.poolA)), kgo.ShareMaxRecords(1))
		pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		member.PollRecords(pollCtx, 1)
		deadline := time.Now().Add(30 * time.Second)
		for {
			described, err := kadm.NewClient(f.service.Operator).DescribeShareGroups(ctx, PoolGroup(f.poolA))
			if err == nil {
				err = described[PoolGroup(f.poolA)].Err
			}
			if err == nil && len(described[PoolGroup(f.poolA)].Members) > 0 {
				return
			}
			if errors.Is(err, kerr.GroupAuthorizationFailed) || time.Now().After(deadline) {
				t.Fatalf("%s describes %s: %v, %+v; want the group with its member", OperatorPrincipal, PoolGroup(f.poolA), err, described)
			}
			time.Sleep(200 * time.Millisecond)
		}
	})

	t.Run("the topic takes the largest permitted export", func(t *testing.T) {
		transport := telemetry.NewTransport(append(hostA.Opts(), kgo.ProducerBatchCompression(kgo.NoCompression())))
		if err := transport.Start(ctx); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = transport.Stop(ctx) }()
		spans := []*tracepb.ResourceSpans{{ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{{TraceId: []byte("0123456789abcdef"), SpanId: []byte("spanspan"), Name: "submit",
			Attributes: []*commonpb.KeyValue{{Key: string(telemetry.AttrTenant), Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: f.tenantA}}}}}}}}}}
		// The span's name pads its record to the largest size Records still keeps: a binary search, since the
		// nested length prefixes grow with the name and Records drops a span over the limit.
		span := spans[0].ScopeSpans[0].Spans[0]
		kept := func(n int) int {
			span.Name = strings.Repeat("x", n)
			records, _, err := telemetry.Records(spans)
			if err != nil {
				t.Fatal(err)
			}
			if len(records) != 1 {
				return 0
			}
			return len(records[0].Value)
		}
		low, high := 0, telemetry.TraceRecordLimit
		for low < high {
			if middle := (low + high + 1) / 2; kept(middle) > 0 {
				low = middle
			} else {
				high = middle - 1
			}
		}
		size := kept(low)
		if size < telemetry.TraceRecordLimit-4 || size > telemetry.TraceRecordLimit {
			t.Fatalf("largest kept record is %d bytes, want within 4 bytes under the limit %d", size, telemetry.TraceRecordLimit)
		}
		if err := transport.UploadTraces(ctx, spans); err != nil {
			t.Fatalf("upload of the largest permitted export = %v, want the topic to take it", err)
		}
		largest := false
		for _, record := range teletest.Read(t, ctx, hostA.Opts(), tracesA) {
			largest = largest || len(record.Value) == size
		}
		if !largest {
			t.Fatalf("no record of %d bytes on %s", size, tracesA)
		}
	})
}
