package receiver

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// staleClaimConn partitions only A's dedicated claim client. Its producer, pool
// reader and plugin remain reachable; reconnecting cannot restore renewals.
type staleClaimConn struct {
	net.Conn
	blocked *atomic.Bool
	dropped *atomic.Int64
	fault   error
}

func (c *staleClaimConn) Write(p []byte) (int, error) {
	if c.blocked.Load() {
		c.dropped.Add(1)
		// Break the underlying socket on the first failed write. Returning the
		// selected error lets franz-go apply its real retry policy.
		_ = c.Conn.Close()
		return 0, c.fault
	}
	return c.Conn.Write(p)
}

// staleHolderTrace observes A and identifies B's adoption transaction. Produce results are not
// transaction outcomes: keep both, including records rejected before EndTxn.
type staleHolderTrace struct {
	mu                                  sync.Mutex
	run, txn                            string
	adopted                             time.Time
	held                                context.Context
	cut, nextRenewalWrite, nextCallback time.Time
	entries                             []string
	forbidden                           []string
	producer                            atomic.Pointer[kgo.Client]
	closed                              chan struct{}
	keys                                signed.Keys
	adoptionTarget                      *staleHolderTrace
	adopting                            atomic.Bool
}

func (s *staleHolderTrace) note(action string, forbidden bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if !s.cut.IsZero() {
		if s.nextRenewalWrite.IsZero() && strings.HasPrefix(action, "claim wire write api=ShareAcknowledge ") {
			s.nextRenewalWrite = now
		}
		if s.nextCallback.IsZero() && strings.HasPrefix(action, "claim acknowledgement callback") {
			s.nextCallback = now
		}
	}
	since := "before adoption"
	if !s.adopted.IsZero() {
		since = fmt.Sprintf("+%s after adoption", now.Sub(s.adopted))
	}
	entry := fmt.Sprintf("%s (%s): %s", now.UTC().Format(time.RFC3339Nano), since, action)
	s.entries = append(s.entries, entry)
	if forbidden && (!s.adopted.IsZero() || s.held != nil && errors.Is(context.Cause(s.held), errClaimLost)) {
		s.forbidden = append(s.forbidden, entry)
	}
}

func (s *staleHolderTrace) OnNewClient(c *kgo.Client) {
	if c.OptValue(kgo.TransactionalID) == s.txn {
		s.producer.Store(c)
	}
}
func (s *staleHolderTrace) OnClientClosed(c *kgo.Client) {
	if s.producer.Load() == c {
		if s.closed != nil {
			close(s.closed)
		}
	}
}
func (s *staleHolderTrace) OnProduceRecordBuffered(r *kgo.Record) {
	if string(r.Key) == s.run {
		if s.adoptionTarget != nil {
			var status wire.RunStatus
			if wire.ParseRecord(r.Value, s.keys, &status) == nil && status.Wherry.Kind == "adopting" {
				s.adopting.Store(true)
			}
		}
		s.note(fmt.Sprintf("produce attempt topic=%s key=%s bytes=%d sha256=%x", r.Topic, r.Key, len(r.Value), sha256.Sum256(r.Value)), true)
	}
}
func (s *staleHolderTrace) OnProduceRecordUnbuffered(r *kgo.Record, err error) {
	if string(r.Key) == s.run {
		s.note(fmt.Sprintf("produce result topic=%s partition=%d offset=%d err=%v (not an EndTxn result)", r.Topic, r.Partition, r.Offset, err), false)
	}
}

// Each receiver has its own observer and Follow gate over ONE platform/job table.
// B stays following while A alone is allowed to reach its natural end path.
type staleHolderPlugin struct {
	*fakePlugin
	trace    *staleHolderTrace
	finish   <-chan struct{}
	followed chan<- time.Time
	waiting  *atomic.Bool
}

func (p *staleHolderPlugin) note(call string, forbidden bool) {
	if p.trace != nil {
		p.trace.note(call, forbidden)
	}
}
func (p *staleHolderPlugin) Dispatch(ctx context.Context, run pluginsdk.Run) (pluginsdk.Outcome, pluginsdk.Handle, error) {
	p.note("Dispatch "+run.ID, true)
	return p.fakePlugin.Dispatch(ctx, run)
}
func (p *staleHolderPlugin) Lookup(ctx context.Context, run string) (pluginsdk.Job, error) {
	p.note("Lookup "+run, false)
	return p.fakePlugin.Lookup(ctx, run)
}
func (p *staleHolderPlugin) Follow(ctx context.Context, h pluginsdk.Handle, from int64, lines chan<- pluginsdk.Line) (pluginsdk.Terminal, error) {
	if p.trace != nil {
		p.trace.mu.Lock()
		p.trace.held = ctx
		p.trace.mu.Unlock()
	}
	p.note("Follow "+h.Name, false)
	if p.waiting != nil {
		p.waiting.Store(true)
		defer p.waiting.Store(false)
	}
	if p.followed != nil {
		select {
		case p.followed <- time.Now():
		default:
		}
	}
	select {
	case <-p.finish:
	case <-ctx.Done():
		return pluginsdk.Terminal{}, ctx.Err()
	}
	return p.fakePlugin.Follow(ctx, h, from, lines)
}
func (p *staleHolderPlugin) Stop(ctx context.Context, h pluginsdk.Handle) error {
	p.note("Stop "+h.Name, true)
	return p.fakePlugin.Stop(ctx, h)
}
func (p *staleHolderPlugin) Purge(ctx context.Context, run string) error {
	p.note("Purge "+run, true)
	return p.fakePlugin.Purge(ctx, run)
}
func (p *staleHolderPlugin) Release(ctx context.Context, run string) error {
	p.note("Release "+run, true)
	return p.fakePlugin.Release(ctx, run)
}
func (p *staleHolderPlugin) Collect(ctx context.Context, h pluginsdk.Handle, limits pluginsdk.Limits, out chan<- pluginsdk.Output) error {
	p.note("Collect "+h.Name, false)
	return p.fakePlugin.Collect(ctx, h, limits, out)
}

// TestStaleHolderDoesNotActAfterAdoptionOnRealBroker demonstrates agb-w7i0.
// A partitioned holder must stop acting while the adopter serves the job.
func TestStaleHolderDoesNotActAfterAdoptionOnRealBroker(t *testing.T) {
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" {
		t.Skip("WHERRY_KAFKA_TEST_BROKER not set")
	}
	for _, variant := range []struct {
		name     string
		interval time.Duration
		fault    error
	}{
		{"short_nonretryable", 200 * time.Millisecond, io.ErrClosedPipe},
		{"production_retryable", 10 * time.Second, &os.SyscallError{Syscall: "write", Err: syscall.ECONNRESET}},
	} {
		t.Run(variant.name, func(t *testing.T) { staleHolderBrokerVariant(t, address, variant.interval, variant.fault) })
	}
}

func staleHolderBrokerVariant(t *testing.T, address string, interval time.Duration, fault error) {
	shortIntervals(t)
	// Set once before starting either receiver: no races on the package interval.
	// B may also renew slowly; only A is partitioned. Short custody/advertising
	// intervals keep the demonstration bounded without accelerating A's renewer.
	renewInterval = interval
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
	defer cancel()
	b := newBrokerPool(t, ctx, address)
	terms := testTerms()
	terms.LockDurationMS = 15000
	b.applyTerms(t, ctx, terms)
	b.configure(t, ctx, brokerProfile(1))
	second := b.receiver + "-b"
	b.member(t, ctx, second)
	secondPool := b
	secondPool.receiver = second
	secondPool.configure(t, ctx, brokerProfile(1))
	hostKey := b.laneHost(t, ctx)
	run := wire.RunID(b.tenant, b.host, testNonce("run-stale-holder"))
	trace := &staleHolderTrace{run: run, txn: operator.TransactionalPrefix(b.pool) + run, closed: make(chan struct{})}
	profile := brokerProfile(1)
	platform := &fakePlugin{pool: b.pool, name: profile.Name, agent: profile.Agent}
	finishA, finishB := make(chan struct{}), make(chan struct{})
	keys := signed.Keys{}
	makeReceiver := func(id string, plugin pluginsdk.Plugin, hooks ...kgo.Hook) (*Receiver, *syncBuffer) {
		base, err := kgo.NewClient(append(append([]kgo.Opt(nil), b.opts...), kgo.ClientID(id), kgo.WithHooks(hooks...))...)
		if err != nil {
			t.Fatal(err)
		}
		_, signing, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		keys[id] = signed.KeyState{Current: signing.Public().(ed25519.PublicKey)}
		logs := &syncBuffer{}
		r := &Receiver{id: id, base: base, reader: operator.NewReader(base), signing: signing, stateDir: t.TempDir(), logger: log.New(logs, "", 0), metrics: newMetrics(prometheus.NewRegistry()),
			factory: func(_ string, settings operator.ProfileSettings, _ string) (Named, error) {
				return Named{Name: settings.Name, Agent: settings.Agent, Labels: settings.Labels, Plugin: plugin}, nil
			}}
		return r, logs
	}
	var aWaiting atomic.Bool
	a, logsA := makeReceiver(b.receiver, &staleHolderPlugin{fakePlugin: platform, trace: trace, finish: finishA, waiting: &aWaiting}, trace)
	served := make(chan time.Time, 1)
	traceB := &staleHolderTrace{run: run, txn: trace.txn, adoptionTarget: trace, keys: keys}
	later, logsB := makeReceiver(second, &staleHolderPlugin{fakePlugin: platform, finish: finishB, followed: served}, traceB)
	// Existing factory seam: no change to the renewer or any production code.
	var blocked atomic.Bool
	var dropped atomic.Int64
	oldClaim := newClaimClient
	newClaimClient = func(r *Receiver, profile Named, opts ...kgo.Opt) (*kgo.Client, error) {
		if r == a {
			// Inspect the supplied option without polling/dialing, preserving the real
			// callback rather than replacing its metrics/logging behaviour.
			probe, err := kgo.NewClient(opts...)
			if err != nil {
				return nil, err
			}
			callback := probe.OptValue(kgo.ShareAckCallback).(func(*kgo.Client, kgo.ShareAckResults))
			probe.Close()
			opts = append(opts, kgo.ShareAckCallback(func(c *kgo.Client, results kgo.ShareAckResults) {
				trace.note(fmt.Sprintf("claim acknowledgement callback results=%+v", results), false)
				callback(c, results)
			}), kgo.WithHooks(staleClaimWrites{trace}))
			opts = append(opts, kgo.Dialer(func(ctx context.Context, network, address string) (net.Conn, error) {
				conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
				if err != nil {
					return nil, err
				}
				return &staleClaimConn{Conn: conn, blocked: &blocked, dropped: &dropped, fault: fault}, nil
			}))
		}
		return oldClaim(r, profile, opts...)
	}
	t.Cleanup(func() { newClaimClient = oldClaim })
	adoptionCommitted := make(chan struct{}, 1)
	oldCommit := commitTransaction
	commitTransaction = func(ctx context.Context, producer *kgo.Client) error {
		err := oldCommit(ctx, producer)
		if traceB.producer.Load() == producer && err == nil && traceB.adopting.Swap(false) {
			trace.mu.Lock()
			trace.adopted = time.Now()
			trace.mu.Unlock()
			trace.note("B EndTxn adoption commit returned successfully", false)
			select {
			case adoptionCommitted <- struct{}{}:
			default:
			}
		}
		if trace.producer.Load() == producer {
			trace.note(fmt.Sprintf("EndTxn commit result err=%v", err), err == nil)
		}
		return err
	}
	t.Cleanup(func() { commitTransaction = oldCommit })
	// Registered before receiver cleanups so failures also retain a complete trace.
	t.Cleanup(func() {
		trace.mu.Lock()
		defer trace.mu.Unlock()
		t.Logf("%s\nA timeline (includes pre-adoption renewal observations):\n%s", trace.observationsLocked(), strings.Join(trace.entries, "\n"))
	})
	runLane(t, ctx, a)
	b.joined(t, ctx, logsA)
	if got := b.laneTask(t, ctx, hostKey, "run-stale-holder", b.tasksTopic(t, ctx), func(s *wire.Submission) {
		s.Wherry.TaskStartedBy = time.Now().Add(55 * time.Second).Unix()
		s.Wherry.TaskEndedBy = time.Now().Add(65 * time.Second).Unix()
	}); got != run {
		t.Fatalf("run = %s, want %s", got, run)
	}
	awaitStatus := func(receiver, kind string, attempt int64) {
		t.Helper()
		for ctx.Err() == nil {
			records, err := readTopic(ctx, b.kafka, operator.PoolEventsTopic(b.pool))
			if err != nil {
				t.Fatal(err)
			}
			for _, record := range records {
				var status wire.RunStatus
				if string(record.Key) == run && wire.ParseRecord(record.Value, keys, &status) == nil && status.Wherry.Receiver == receiver && status.Wherry.Kind == kind && status.Wherry.Attempt == attempt {
					return
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("no %s by %s at attempt %d; A:\n%s\nB:\n%s", kind, receiver, attempt, logsA.String(), logsB.String())
	}
	// With an empty stream A has no progress writes to discover fencing early.
	awaitStatus(a.id, "started", 1)
	// started is committed before Follow is entered; wait for the gate too.
	for !aWaiting.Load() && ctx.Err() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	if !aWaiting.Load() {
		t.Fatal("A never entered Follow")
	}
	blockedAt := time.Now()
	trace.mu.Lock()
	trace.cut = blockedAt
	trace.mu.Unlock()
	// Cut writes, leaving the idle socket until its next write so the first
	// renewal itself encounters the selected fault rather than a preceding EOF.
	// Every reconnect uses the same wrapper and remains partitioned.
	blocked.Store(true)
	trace.note(fmt.Sprintf("A claim connection writes cut; renewInterval=%s fault=%v", interval, fault), false)
	runLane(t, ctx, later)
	awaitStatus(later.id, "adopting", 2)
	// A read-committed reader can see adoption before EndTxn returns to B.
	select {
	case <-adoptionCommitted:
	case <-ctx.Done():
		t.Fatal("B adoption became visible but its commit did not return before the deadline")
	}
	platform.mu.Lock()
	dispatches, exists := platform.dispatches, platform.created[run]
	platform.mu.Unlock()
	if dispatches != 1 || !exists {
		t.Fatalf("B did not adopt A's single shared job: dispatches=%d exists=%t", dispatches, exists)
	}
	trace.mu.Lock()
	adopted := trace.adopted
	trace.mu.Unlock()
	t.Logf("A claim writes blocked at %s; B committed adoption observed at %s after %s; failed claim writes=%d", blockedAt.UTC().Format(time.RFC3339Nano), adopted.UTC().Format(time.RFC3339Nano), adopted.Sub(blockedAt), dropped.Load())
	if adopted.IsZero() {
		t.Fatal("B adoption observed without a successful intercepted commit")
	}
	// A remains in Follow and its receiver keeps advertising throughout this
	// bounded liveness check. Do not let a Q2 failure hide the independent Q1.
	select {
	case at := <-served:
		t.Logf("B served: Follow on adopted job at %s (+%s after adoption), while A remains running", at.UTC().Format(time.RFC3339Nano), at.Sub(adopted))
	case <-time.After(5 * time.Second):
		t.Errorf("B not served within 5s after adoption observation while A runs/advertises; custodian/refusal logs A:\n%s\nB:\n%s", logsA.String(), logsB.String())
	case <-ctx.Done():
		t.Errorf("B service check exceeded scenario deadline; A:\n%s\nB:\n%s", logsA.String(), logsB.String())
	}
	if !aWaiting.Load() && !strings.Contains(logsA.String(), ": claim lost:") {
		t.Error("A stopped without reporting claim loss")
	}
	trace.note("B service window ended; allowing A Follow to finish", false)
	close(finishA)
	select {
	case <-trace.closed: // All of A's run operations, including failed writes, finished.
	case <-ctx.Done():
		trace.note("A did not finish before scenario deadline", false)
		trace.mu.Lock()
		timeline := trace.observationsLocked() + "\n" + strings.Join(trace.entries, "\n")
		trace.mu.Unlock()
		t.Fatalf("A did not finish; cut=%s adopted=%s dropped=%d; timeline:\n%s\nA:\n%s\nB:\n%s", blockedAt, adopted, dropped.Load(), timeline, logsA.String(), logsB.String())
	}
	for !strings.Contains(logsA.String(), ": claim lost:") && ctx.Err() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	var sample dto.Metric
	if err := a.metrics.claimLost.Write(&sample); err != nil {
		t.Fatal(err)
	}
	if sample.GetCounter().GetValue() != 1 || strings.Count(logsA.String(), ": claim lost:") != 1 {
		t.Errorf("A did not report exactly one claim loss: count=%v logs:\n%s", sample.GetCounter().GetValue(), logsA.String())
	}
	if dropped.Load() == 0 {
		t.Error("fault injection never intercepted a claim write")
	}
	trace.mu.Lock()
	defer trace.mu.Unlock()
	if len(trace.forbidden) != 0 {
		t.Fatalf("stale holder A acted after B adopted %s at %s (cut=%s, renewal interval=%s, failed claim writes=%d); forbidden actions/commits:\n%s\ncomplete A post-adoption trace (produce errors include fencing):\n%s", run, adopted.UTC().Format(time.RFC3339Nano), blockedAt.UTC().Format(time.RFC3339Nano), interval, dropped.Load(), strings.Join(trace.forbidden, "\n"), trace.observationsLocked()+"\n"+strings.Join(trace.entries, "\n"))
	}
}

// Only attached to A's claim client. Wire writes identify attempts/retries;
// acknowledgement callbacks above identify completed broker results separately.
type staleClaimWrites struct{ trace *staleHolderTrace }

func (h staleClaimWrites) OnBrokerWrite(_ kgo.BrokerMetadata, key int16, _ int, _, _ time.Duration, err error) {
	h.trace.note(fmt.Sprintf("claim wire write api=%s err=%v (ShareAcknowledge writes are renewal attempts/retries while A Follow is held)", kmsg.NameForKey(key), err), false)
}

// Caller holds mu. A zero observation is explicit evidence of no completed
// wire attempt/callback observed by this point, not evidence the timer did not fire.
func (s *staleHolderTrace) observationsLocked() string {
	stamp := func(at time.Time) string {
		if at.IsZero() {
			return "none observed"
		}
		return at.UTC().Format(time.RFC3339Nano)
	}
	return fmt.Sprintf("cut=%s; B adoption commit returned=%s; next renewal wire attempt=%s; next acknowledgement callback=%s", stamp(s.cut), stamp(s.adopted), stamp(s.nextRenewalWrite), stamp(s.nextCallback))
}
