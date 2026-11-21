package receiver

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type shareFetchesInFlight struct {
	current atomic.Int32
	maximum atomic.Int32
	below   atomic.Bool
}

func (h *shareFetchesInFlight) OnBrokerWrite(_ kgo.BrokerMetadata, key int16, _ int, _, _ time.Duration, err error) {
	if key != kmsg.ShareFetch.Int16() || err != nil {
		return
	}
	current := h.current.Add(1)
	for maximum := h.maximum.Load(); current > maximum && !h.maximum.CompareAndSwap(maximum, current); maximum = h.maximum.Load() {
	}
}

func (h *shareFetchesInFlight) OnBrokerRead(_ kgo.BrokerMetadata, key int16, _ int, _, _ time.Duration, _ error) {
	if key != kmsg.ShareFetch.Int16() {
		return
	}
	if h.current.Add(-1) < 0 {
		h.below.Store(true)
	}
}

func installShareFetchCount(t *testing.T, count *shareFetchesInFlight) {
	t.Helper()
	old := newClaimClient
	newClaimClient = func(r *Receiver, _ Named, opts ...kgo.Opt) (*kgo.Client, error) {
		return r.newClient(append(opts, kgo.WithHooks(count))...)
	}
	t.Cleanup(func() { newClaimClient = old })
}

func stopPollingTurnReceiver(t *testing.T, stop context.CancelFunc, done <-chan error, count *shareFetchesInFlight) {
	t.Helper()
	stop()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("receiver did not stop")
	}
	for end := time.Now().Add(5 * time.Second); count.current.Load() != 0 && time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
	}
	if count.below.Load() || count.current.Load() != 0 {
		t.Fatalf("ShareFetch count ended at %d, below zero=%t", count.current.Load(), count.below.Load())
	}
}

func TestOneShareFetchInFlightPerIdleProfileOnRealBroker(t *testing.T) {
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" {
		t.Skip("WHERRY_KAFKA_TEST_BROKER not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	b := newBrokerPool(t, ctx, address)
	b.configure(t, ctx, brokerProfile(20))
	count := &shareFetchesInFlight{}
	installShareFetchCount(t, count)
	r, logs := b.laneMember(t, b.receiver, signed.Keys{}, map[string]*fakePlugin{"nomad": {stream: resultStream}})
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- r.Run(runCtx) }()
	defer func() {
		if stop != nil {
			stopPollingTurnReceiver(t, stop, done, count)
		}
	}()
	for b.groupMembers(t, ctx, b.receiver) != 20 {
		if ctx.Err() != nil {
			t.Fatalf("20 claim clients never joined; logs:\n%s", logs.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(40 * time.Second)
	stopPollingTurnReceiver(t, stop, done, count)
	stop = nil
	if maximum := count.maximum.Load(); maximum != 1 {
		t.Fatalf("maximum ShareFetch count %d, want 1; logs:\n%s", maximum, logs.String())
	}
}

func TestWarmReplicasClaimBurstOnRealBroker(t *testing.T) {
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" {
		t.Skip("WHERRY_KAFKA_TEST_BROKER not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	b := newBrokerPool(t, ctx, address)
	b.configure(t, ctx, brokerProfile(20))
	hostKey := b.laneHost(t, ctx)
	count := &shareFetchesInFlight{}
	installShareFetchCount(t, count)
	barrier := make(chan struct{})
	plugin := &fakePlugin{stream: resultStream, followHold: barrier}
	keys := signed.Keys{}
	r, logs := b.laneMember(t, b.receiver, keys, map[string]*fakePlugin{"nomad": plugin})
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- r.Run(runCtx) }()
	defer func() {
		select {
		case <-barrier:
		default:
			close(barrier)
		}
		if stop != nil {
			stopPollingTurnReceiver(t, stop, done, count)
		}
	}()
	for b.groupMembers(t, ctx, b.receiver) != 20 {
		if ctx.Err() != nil {
			t.Fatalf("20 claim clients never joined; logs:\n%s", logs.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
	topic := operator.SignatureTopic(b.pool, r.profiles[0].Agent, r.profiles[0].Labels)
	var records []*kgo.Record
	runs := map[string]bool{}
	for i := range 20 {
		submission := testSubmission(fmt.Sprintf("burst-%d", i))
		submission.Wherry.Tenant, submission.Wherry.Host, submission.Wherry.Pool = b.tenant, b.host, b.pool
		run := wire.RunID(b.tenant, b.host, submission.Wherry.RunNonce)
		submission.Wherry.Run, submission.Message.TaskID, submission.Message.ContextID = run, run, run
		b.seal(t, ctx, submission)
		value, err := wire.SignRecord(submission, hostKey)
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, &kgo.Record{Topic: topic, Key: []byte(run), Value: value})
		runs[run] = true
	}
	produced := time.Now()
	if err := b.kafka.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	claimed := map[string]int64{}
	// Past the 15 s lock, so a task a stray fetch buffered returns to the pool and is claimed within the budget.
	for time.Since(produced) < 30*time.Second && len(claimed) < len(runs) {
		events, err := readTopic(ctx, b.kafka, operator.PoolEventsTopic(b.pool))
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range events {
			var status wire.RunStatus
			if !runs[string(event.Key)] || wire.ParseRecord(event.Value, keys, &status) != nil || status.Wherry.Kind != "claimed" {
				continue
			}
			claimed[status.Wherry.Run] = status.Wherry.Attempt
		}
		if len(claimed) < len(runs) {
			time.Sleep(50 * time.Millisecond)
		}
	}
	pickup := time.Since(produced)
	for run, attempt := range claimed {
		t.Logf("%s delivery_count=%d", run, attempt)
	}
	t.Logf("burst pickup: %s", pickup)
	if len(claimed) != len(runs) {
		t.Fatalf("claimed %d of %d tasks within 30 s; logs:\n%s", len(claimed), len(runs), logs.String())
	}
	if maximum := count.maximum.Load(); maximum > 2 {
		t.Fatalf("maximum ShareFetch count %d, want at most 2; logs:\n%s", maximum, logs.String())
	}
	close(barrier)
	stopPollingTurnReceiver(t, stop, done, count)
	stop = nil
}
