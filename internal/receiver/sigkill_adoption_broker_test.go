package receiver

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/kafkatest"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/trust"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	sigkillChildEnv = "WHERRY_SIGKILL_ADOPTION_CHILD"
	sigkillRoleEnv  = "WHERRY_SIGKILL_ADOPTION_ROLE"
)

type sigkillMarkerPlugin struct {
	*fakePlugin
	marker string
}

func (p *sigkillMarkerPlugin) Follow(ctx context.Context, handle pluginsdk.Handle, from int64, lines chan<- pluginsdk.Line) (pluginsdk.Terminal, error) {
	if err := os.WriteFile(p.marker, nil, 0o600); err != nil {
		return pluginsdk.Terminal{}, err
	}
	return p.fakePlugin.Follow(ctx, handle, from, lines)
}

// TestSIGKILLAdoptionChildProcess is the process half of
// TestSIGKILLedReceiverRunIsAdoptedOnRealBroker. It is not useful on its own.
func TestSIGKILLAdoptionChildProcess(t *testing.T) {
	if os.Getenv(sigkillChildEnv) == "" {
		t.Skip("child helper")
	}
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	id := os.Getenv("WHERRY_SIGKILL_ADOPTION_RECEIVER")
	stateDir := os.Getenv("WHERRY_SIGKILL_ADOPTION_STATE_DIR")
	marker := os.Getenv("WHERRY_SIGKILL_ADOPTION_MARKER")
	role := os.Getenv(sigkillRoleEnv)
	signing, err := trust.LoadSigningKeyFile(filepath.Join(stateDir, "signing.key"))
	if err != nil {
		t.Fatal(err)
	}
	base, err := kgo.NewClient(kgo.SeedBrokers(address), kgo.FetchMaxWait(10*time.Millisecond), kgo.ClientID(id))
	if err != nil {
		t.Fatal(err)
	}
	logger := log.New(os.Stdout, "receiver: ", log.LstdFlags|log.Lmicroseconds|log.LUTC)
	registry := prometheus.NewRegistry()
	plugin := &sigkillMarkerPlugin{marker: marker}
	switch role {
	case "holder":
		plugin.fakePlugin = &fakePlugin{stream: noResultStream, endDelay: 10 * time.Minute}
	case "adopter":
		plugin.fakePlugin = &fakePlugin{stream: resultStream, endDelay: 30 * time.Second, job: pluginsdk.JobRunning}
	default:
		t.Fatalf("unknown child role %q", role)
	}
	r := &Receiver{
		id: id, base: base, reader: operator.NewReader(base), signing: signing,
		stateDir: stateDir, drainTimeout: 5 * time.Second,
		logger: logger, metrics: newMetrics(registry),
		factory: func(pool string, settings operator.ProfileSettings, _ string) (Named, error) {
			plugin.mu.Lock()
			plugin.pool, plugin.name, plugin.agent = pool, settings.Name, settings.Agent
			plugin.mu.Unlock()
			return Named{Name: settings.Name, Agent: settings.Agent, Labels: settings.Labels, Plugin: plugin}, nil
		},
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err = r.Run(ctx)
	logger.Printf("Run returned err=%v", err)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

type sigkillChild struct {
	cmd     *exec.Cmd
	done    chan error
	logPath string
}

func startSIGKILLChild(t *testing.T, address, id, stateDir, marker, role, logPath string) *sigkillChild {
	t.Helper()
	file, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestSIGKILLAdoptionChildProcess$", "-test.v")
	cmd.Env = append(os.Environ(),
		sigkillChildEnv+"=1",
		sigkillRoleEnv+"="+role,
		"WHERRY_KAFKA_TEST_BROKER="+address,
		"WHERRY_SIGKILL_ADOPTION_RECEIVER="+id,
		"WHERRY_SIGKILL_ADOPTION_STATE_DIR="+stateDir,
		"WHERRY_SIGKILL_ADOPTION_MARKER="+marker,
	)
	cmd.Stdout, cmd.Stderr = file, file
	if err := cmd.Start(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	child := &sigkillChild{cmd: cmd, done: make(chan error, 1), logPath: logPath}
	go func() { child.done <- cmd.Wait() }()
	return child
}

func (c *sigkillChild) signalAndWait(t *testing.T, signal os.Signal, timeout time.Duration) error {
	t.Helper()
	if err := c.cmd.Process.Signal(signal); err != nil {
		select {
		case waitErr := <-c.done:
			return waitErr
		default:
		}
		t.Fatal(err)
	}
	select {
	case err := <-c.done:
		return err
	case <-time.After(timeout):
		_ = c.cmd.Process.Kill()
		<-c.done
		t.Fatalf("child did not stop within %s", timeout)
		return nil
	}
}

func (c *sigkillChild) logs(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(c.logPath)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func sigkillLogEvidence(logs string) string {
	var evidence []string
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, "configured:") || strings.Contains(line, "continuing as custodian") ||
			strings.Contains(line, "Run returned") {
			evidence = append(evidence, line)
		}
	}
	return strings.Join(evidence, "\n")
}

func sigkillAwaitFile(t *testing.T, ctx context.Context, path string) {
	t.Helper()
	for ctx.Err() == nil {
		if _, err := os.Stat(path); err == nil {
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("marker %s was never written", path)
}

func sigkillStatuses(t *testing.T, ctx context.Context, b brokerPool, keys signed.Keys, run string) []wire.RunStatus {
	t.Helper()
	records, err := readTopic(ctx, b.kafka, operator.PoolEventsTopic(b.pool))
	if err != nil {
		t.Fatal(err)
	}
	var statuses []wire.RunStatus
	for _, record := range records {
		var status wire.RunStatus
		if string(record.Key) == run && wire.ParseRecord(record.Value, keys, &status) == nil {
			statuses = append(statuses, status)
		}
	}
	return statuses
}

func sigkillAwaitStatus(t *testing.T, ctx context.Context, b brokerPool, keys signed.Keys, run, kind string) {
	t.Helper()
	for ctx.Err() == nil {
		if slices.ContainsFunc(sigkillStatuses(t, ctx, b, keys, run), func(status wire.RunStatus) bool { return status.Wherry.Kind == kind }) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("run %s never committed %s", run, kind)
}

func sigkillAwaitOutcome(t *testing.T, ctx context.Context, b brokerPool, keys signed.Keys, run string, timeout time.Duration) []wire.RunStatus {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for ctx.Err() == nil && time.Now().Before(deadline) {
		statuses := sigkillStatuses(t, ctx, b, keys, run)
		if slices.ContainsFunc(statuses, func(status wire.RunStatus) bool {
			return status.Wherry.Kind == "terminal" && status.StatusUpdate.Status.State == wire.TaskStateCompleted
		}) {
			return statuses
		}
		time.Sleep(200 * time.Millisecond)
	}
	return sigkillStatuses(t, ctx, b, keys, run)
}

func sigkillOffsetSnapshot(t *testing.T, ctx context.Context, b brokerPool, topic string) string {
	t.Helper()
	offsets, err := kadm.NewClient(b.kafka).DescribeShareGroupOffsets(ctx, operator.PoolGroup(b.pool))
	if err != nil {
		t.Fatal(err)
	}
	group := offsets[operator.PoolGroup(b.pool)]
	return fmt.Sprintf("topic=%s offsets=%+v", topic, group.Offsets)
}

func sigkillProbe(t *testing.T, ctx context.Context, b brokerPool, topic, run string, stopped time.Time) *kgo.Record {
	t.Helper()
	probe, err := kgo.NewClient(append(slices.Clone(b.opts), kgo.ClientID("sigkill-probe"), kgo.ConsumeTopics(topic), kgo.ShareGroup(operator.PoolGroup(b.pool)), kgo.ShareMaxRecords(1), kgo.ShareMaxRecordsStrict())...)
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()
	// The receiver's clients closed no later than its process exit. Poll through
	// one lock duration and another 15 seconds so an early empty poll cannot be
	// mistaken for archival.
	deadline := stopped.Add(30 * time.Second)
	for ctx.Err() == nil && time.Now().Before(deadline) {
		pollCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		for _, record := range probe.PollRecords(pollCtx, 1).Records() {
			if string(record.Key) == run {
				cancel()
				return record
			}
		}
		cancel()
	}
	return nil
}

func sigkillStatusSummary(statuses []wire.RunStatus) string {
	var summary []string
	for _, status := range statuses {
		summary = append(summary, fmt.Sprintf("%s(attempt=%d,state=%s,receiver=%s)", status.Wherry.Kind, status.Wherry.Attempt, status.StatusUpdate.Status.State, status.Wherry.Receiver))
	}
	return strings.Join(summary, ", ")
}

func sigkillAdoptingAttempt(statuses []wire.RunStatus) (int64, bool) {
	for claimed := range statuses {
		attempt := statuses[claimed].Wherry.Attempt
		if statuses[claimed].Wherry.Kind != "claimed" || attempt <= 1 {
			continue
		}
		adopting, started := false, false
		for _, status := range statuses[claimed+1:] {
			if status.Wherry.Attempt == attempt {
				adopting = adopting || status.Wherry.Kind == "adopting"
				started = started || adopting && status.Wherry.Kind == "started"
			}
			if adopting && started && status.Wherry.Kind == "terminal" && status.StatusUpdate.Status.State == wire.TaskStateCompleted {
				return attempt, true
			}
		}
	}
	return 0, false
}

// TestSIGKILLedReceiverRunIsAdoptedOnRealBroker requires adoption after each way the first receiver stops serving its
// share session. SIGTERM is the graceful control.
func TestSIGKILLedReceiverRunIsAdoptedOnRealBroker(t *testing.T) {
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" {
		t.Skip("WHERRY_KAFKA_TEST_BROKER not set")
	}
	// A crashed receiver's parked fetches spent the task's deliveries only on a multi-broker cluster.
	probe, err := kgo.NewClient(kgo.SeedBrokers(address))
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()
	kafkatest.Brokers(t, t.Context(), kadm.NewClient(probe))
	for _, test := range []struct {
		name   string
		signal os.Signal
	}{
		{"sigterm_control", syscall.SIGTERM},
		{"sigkill", syscall.SIGKILL},
		// SIGSTOP keeps child 1's connections open and silent, as a container whose network namespace goes away without
		// a FIN or RST leaves them, until the broker's session timeout ends its share sessions.
		{"sigstop_halfopen", syscall.SIGSTOP},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			b := newBrokerPool(t, ctx, address)
			profile := brokerProfile(20)
			b.configure(t, ctx, profile)
			b.deliveryLimit(t, ctx, 5)
			hostKey := b.laneHost(t, ctx)
			stateDir := t.TempDir()
			signing, err := trust.GenerateSigningKeyFile(filepath.Join(stateDir, "signing.key"))
			if err != nil {
				t.Fatal(err)
			}
			keys := signed.Keys{b.receiver: {Current: signing.Public().(ed25519.PublicKey)}}
			marker1 := filepath.Join(t.TempDir(), "holder-followed")
			child1 := startSIGKILLChild(t, address, b.receiver, stateDir, marker1, "holder", filepath.Join(t.TempDir(), "child-1.log"))
			child1Done := false
			defer func() {
				if !child1Done {
					_ = child1.signalAndWait(t, syscall.SIGKILL, 10*time.Second)
				}
				t.Logf("child 1 evidence:\n%s", sigkillLogEvidence(child1.logs(t)))
			}()
			b.joined(t, ctx, &syncBuffer{})
			topic := b.tasksTopic(t, ctx)
			run := b.laneTask(t, ctx, hostKey, "run-sigkill-adoption", topic, func(task *wire.Submission) {
				task.Wherry.TaskStartedBy = time.Now().Add(5 * time.Minute).Unix()
				task.Wherry.TaskEndedBy = time.Now().Add(10 * time.Minute).Unix()
			})
			sigkillAwaitStatus(t, ctx, b, keys, run, "started")
			sigkillAwaitFile(t, ctx, marker1)
			t.Logf("offsets before signal: %s", sigkillOffsetSnapshot(t, ctx, b, topic))
			time.Sleep(4 * time.Second)
			signalled := time.Now()
			var waitErr error
			if test.signal == syscall.SIGSTOP {
				if err := child1.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
					t.Fatal(err)
				}
				waitErr = errors.New("stopped")
			} else {
				waitErr = child1.signalAndWait(t, test.signal, 15*time.Second)
				child1Done = true
			}
			if test.signal == syscall.SIGTERM && waitErr != nil {
				t.Fatalf("SIGTERM child exited with %v", waitErr)
			}
			if test.signal != syscall.SIGTERM && waitErr == nil {
				t.Fatal("SIGKILL child exited successfully")
			}
			t.Logf("child 1 exit after %s: %v", time.Since(signalled), waitErr)
			t.Logf("offsets after signal: %s", sigkillOffsetSnapshot(t, ctx, b, topic))
			if wait := 20*time.Second - time.Since(signalled); wait > 0 {
				time.Sleep(wait)
			}
			marker2 := filepath.Join(t.TempDir(), "adopter-followed")
			child2 := startSIGKILLChild(t, address, b.receiver, stateDir, marker2, "adopter", filepath.Join(t.TempDir(), "child-2.log"))
			child2Done := false
			defer func() {
				if !child2Done {
					_ = child2.signalAndWait(t, syscall.SIGTERM, 15*time.Second)
				}
				t.Logf("child 2 evidence:\n%s", sigkillLogEvidence(child2.logs(t)))
			}()
			statuses := sigkillAwaitOutcome(t, ctx, b, keys, run, 120*time.Second)
			stopAt := time.Now()
			if err := child2.signalAndWait(t, syscall.SIGTERM, 15*time.Second); err != nil {
				t.Fatalf("child 2 exited with %v", err)
			}
			child2Done = true
			stopped := time.Now()
			t.Logf("child 2 stopped in %s", stopped.Sub(stopAt))
			probe := sigkillProbe(t, ctx, b, topic, run, stopped)
			if probe == nil {
				t.Log("fresh share consumer probe: none")
			} else {
				t.Logf("fresh share consumer probe: topic=%s partition=%d offset=%d delivery_count=%d", probe.Topic, probe.Partition, probe.Offset, probe.DeliveryCount())
			}
			t.Logf("offsets after child 2 settled: %s", sigkillOffsetSnapshot(t, ctx, b, topic))
			t.Logf("run statuses: %s", sigkillStatusSummary(statuses))
			attempt, adopted := sigkillAdoptingAttempt(statuses)
			t.Logf("adopting attempt: %d", attempt)
			if !adopted {
				t.Fatalf("run was not claimed, adopted, started at one later attempt and completed: %s", sigkillStatusSummary(statuses))
			}
		})
	}
}
