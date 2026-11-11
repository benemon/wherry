package receiver

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// staleClaimConn partitions only A's dedicated claim client. Its producer, pool
// reader and plugin remain reachable; reconnecting cannot restore renewals.
type staleClaimConn struct {
	net.Conn
	blocked *atomic.Bool
	dropped *atomic.Int64
}

func (c *staleClaimConn) Write(p []byte) (int, error) {
	if c.blocked.Load() {
		c.dropped.Add(1)
		return 0, io.ErrClosedPipe
	}
	return c.Conn.Write(p)
}

// staleHolderTrace is installed only on A's clients. Produce results are not
// transaction outcomes: keep both, including records rejected before EndTxn.
type staleHolderTrace struct {
	mu        sync.Mutex
	run, txn  string
	adopted   time.Time
	entries   []string
	forbidden []string
	producer  atomic.Pointer[kgo.Client]
	closed    chan struct{}
}

func (s *staleHolderTrace) note(action string, forbidden bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.adopted.IsZero() {
		return
	}
	entry := fmt.Sprintf("%s (+%s): %s", time.Now().UTC().Format(time.RFC3339Nano), time.Since(s.adopted), action)
	s.entries = append(s.entries, entry)
	if forbidden {
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
		close(s.closed)
	}
}
func (s *staleHolderTrace) OnProduceRecordBuffered(r *kgo.Record) {
	if string(r.Key) == s.run {
		s.note(fmt.Sprintf("produce attempt topic=%s key=%s bytes=%d sha256=%x", r.Topic, r.Key, len(r.Value), sha256.Sum256(r.Value)), false)
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
	trace  *staleHolderTrace
	finish <-chan struct{}
}

func (p *staleHolderPlugin) note(call string, forbidden bool) {
	if p.trace != nil {
		p.trace.note(call, forbidden)
	}
}
func (p *staleHolderPlugin) Dispatch(ctx context.Context, run pluginsdk.Run) (pluginsdk.Outcome, pluginsdk.Handle, error) {
	p.note("Dispatch "+run.ID, false)
	return p.fakePlugin.Dispatch(ctx, run)
}
func (p *staleHolderPlugin) Lookup(ctx context.Context, run string) (pluginsdk.Job, error) {
	p.note("Lookup "+run, false)
	return p.fakePlugin.Lookup(ctx, run)
}
func (p *staleHolderPlugin) Follow(ctx context.Context, h pluginsdk.Handle, from int64, lines chan<- pluginsdk.Line) (pluginsdk.Terminal, error) {
	p.note("Follow "+h.Name, false)
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
// The assertion describes the required behaviour; today's conclude calls Stop
// before discovering producer fencing, so this test is expected to fail.
func TestStaleHolderDoesNotActAfterAdoptionOnRealBroker(t *testing.T) {
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" {
		t.Skip("WHERRY_KAFKA_TEST_BROKER not set")
	}
	shortIntervals(t)
	ctx, cancel := context.WithTimeout(context.Background(), 110*time.Second)
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
	a, logsA := makeReceiver(b.receiver, &staleHolderPlugin{fakePlugin: platform, trace: trace, finish: finishA}, trace)
	later, logsB := makeReceiver(second, &staleHolderPlugin{fakePlugin: platform, finish: finishB})
	// Existing factory seam: no change to the renewer or any production code.
	var blocked atomic.Bool
	var dropped atomic.Int64
	oldClaim := newClaimClient
	newClaimClient = func(r *Receiver, profile Named, opts ...kgo.Opt) (*kgo.Client, error) {
		if r == a {
			opts = append(opts, kgo.Dialer(func(ctx context.Context, network, address string) (net.Conn, error) {
				conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
				if err != nil {
					return nil, err
				}
				return &staleClaimConn{Conn: conn, blocked: &blocked, dropped: &dropped}, nil
			}))
		}
		return oldClaim(r, profile, opts...)
	}
	t.Cleanup(func() { newClaimClient = oldClaim })
	oldCommit := commitTransaction
	commitTransaction = func(ctx context.Context, producer *kgo.Client) error {
		err := oldCommit(ctx, producer)
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
		t.Logf("A post-adoption trace:\n%s", strings.Join(trace.entries, "\n"))
	})
	runLane(t, ctx, a)
	b.joined(t, ctx, logsA)
	if got := b.laneTask(t, ctx, hostKey, "run-stale-holder", b.tasksTopic(t, ctx), func(s *wire.Submission) {
		s.Wherry.TaskStartedBy = time.Now().Add(90 * time.Second).Unix()
		s.Wherry.TaskEndedBy = time.Now().Add(100 * time.Second).Unix()
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
	blockedAt := time.Now()
	blocked.Store(true)
	runLane(t, ctx, later)
	awaitStatus(later.id, "adopting", 2)
	platform.mu.Lock()
	dispatches, exists := platform.dispatches, platform.created[run]
	platform.mu.Unlock()
	if dispatches != 1 || !exists {
		t.Fatalf("B did not adopt A's single shared job: dispatches=%d exists=%t", dispatches, exists)
	}
	trace.mu.Lock()
	trace.adopted = time.Now()
	adopted := trace.adopted
	trace.mu.Unlock()
	t.Logf("A claim writes blocked at %s; B committed adoption observed at %s after %s; failed claim writes=%d", blockedAt.UTC().Format(time.RFC3339Nano), adopted.UTC().Format(time.RFC3339Nano), adopted.Sub(blockedAt), dropped.Load())
	if dropped.Load() == 0 {
		t.Fatal("fault injection never intercepted a claim write")
	}
	close(finishA)
	select {
	case <-trace.closed: // All of A's run operations, including failed writes, finished.
	case <-ctx.Done():
		t.Fatalf("A did not finish after its job ended; A:\n%s\nB:\n%s", logsA.String(), logsB.String())
	}
	trace.mu.Lock()
	defer trace.mu.Unlock()
	if len(trace.forbidden) != 0 {
		t.Fatalf("stale holder A acted after B adopted %s at %s; forbidden actions/commits:\n%s\ncomplete A post-adoption trace (produce errors include fencing):\n%s", run, adopted.UTC().Format(time.RFC3339Nano), strings.Join(trace.forbidden, "\n"), strings.Join(trace.entries, "\n"))
	}
}
