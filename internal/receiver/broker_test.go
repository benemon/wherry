package receiver

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"gopkg.in/yaml.v3"
)

// brokerPool is a pool of its own on the plaintext broker named by WHERRY_KAFKA_TEST_BROKER, with one receiver, one
// host and its tenant, provisioned by operator.Service as the super user that listener's anonymous principal is. The
// broker's wherry.operator is shared with the later packages' tests, whose operator check reads every object this
// records: each needs the topic configs and ACLs the operator gives it.
type brokerPool struct {
	opts                         []kgo.Opt
	kafka                        *kgo.Client
	service                      *operator.Service
	receiver, pool, tenant, host string
}

func newBrokerPool(t *testing.T, ctx context.Context, address string) brokerPool {
	t.Helper()
	opts := []kgo.Opt{kgo.SeedBrokers(address), kgo.FetchMaxWait(10 * time.Millisecond)}
	kafka, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kafka.Close)
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	b := brokerPool{opts: opts, kafka: kafka, receiver: "receiver-" + suffix, pool: "pool-" + suffix, tenant: "tenant-" + suffix, host: "host-" + suffix}
	b.service = &operator.Service{Operator: kafka, Broker: address}
	b.applyTerms(t, ctx, testTerms())
	if err := b.service.TenantAdd(ctx, b.tenant); err != nil {
		t.Fatal(err)
	}
	if err := b.service.TenantPoolAdd(ctx, b.tenant, b.pool); err != nil {
		t.Fatal(err)
	}
	if err := b.service.HostAdd(ctx, b.tenant, b.host, filepath.Join(t.TempDir(), "host.bundle"), false); err != nil {
		t.Fatal(err)
	}
	b.member(t, ctx, b.receiver)
	return b
}

// applyTerms applies terms to the pool as pool apply does.
func (b brokerPool) applyTerms(t *testing.T, ctx context.Context, terms operator.Terms) {
	t.Helper()
	if err := b.service.PoolApply(ctx, b.pool, terms, false); err != nil {
		t.Fatal(err)
	}
}

// joined waits until the receiver holds a partition of its pool's share group, and a second more for its first share
// fetch. The group's share.auto.offset.reset is earliest, so a task published before that fetch is still delivered.
func (b brokerPool) joined(t *testing.T, ctx context.Context, logs *syncBuffer) {
	t.Helper()
	for {
		groups, err := kadm.NewClient(b.kafka).DescribeShareGroups(ctx, operator.PoolGroup(b.pool))
		if group := groups[operator.PoolGroup(b.pool)]; err == nil && len(group.AssignedPartitions()) > 0 {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("receiver never joined its pool's share group; logs:\n%s", logs.String())
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(time.Second)
}

// TestShutdownReleasesClaimOnRealBroker runs a receiver in a broker pool; kfake does not model share-group
// acquisition. The receiver has more slots than the pool's delivery limit, so a release its idle slots' closing
// clients hand back and forth would archive the task before another member could take it.
func TestShutdownReleasesClaimOnRealBroker(t *testing.T) {
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" {
		t.Skip("WHERRY_KAFKA_TEST_BROKER not set")
	}
	for name, stopping := range map[string]struct {
		stop func(t *testing.T, ctx context.Context, b brokerPool, cancel context.CancelFunc)
		want error
	}{
		"cancelled": {func(_ *testing.T, _ context.Context, _ brokerPool, cancel context.CancelFunc) { cancel() }, context.Canceled},
	} {
		t.Run(name, func(t *testing.T) {
			shortIntervals(t)
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()
			b := newBrokerPool(t, ctx, address)
			b.configure(t, ctx, 6)
			b.deliveryLimit(t, ctx, 5)
			opts, kafka, receiverID, pool, tenant, host := b.opts, b.kafka, b.receiver, b.pool, b.tenant, b.host
			_, hostKey, _ := ed25519.GenerateKey(rand.Reader)
			_, signing, _ := ed25519.GenerateKey(rand.Reader)
			registration, err := signed.RegisterKey(signed.HostOwner(tenant, host), hostKey)
			if err != nil {
				t.Fatal(err)
			}
			if err := kafka.ProduceSync(ctx, &kgo.Record{Topic: operator.HostTopic(tenant, host), Value: registration}).FirstErr(); err != nil {
				t.Fatal(err)
			}
			base, err := kgo.NewClient(opts...)
			if err != nil {
				t.Fatal(err)
			}
			logs := &syncBuffer{}
			plugin := &fakePlugin{stream: noResultStream, endDelay: time.Minute}
			r := &Receiver{id: receiverID, base: base, reader: operator.NewReader(base), signing: signing, stateDir: t.TempDir(), logger: log.New(logs, "", 0), metrics: newMetrics(prometheus.NewRegistry()),
				factory: func(_ string, settings operator.ProfileSettings, _ string) (Named, error) {
					return Named{Name: settings.Name, Agent: settings.Agent, Plugin: plugin}, nil
				}}
			runCtx, stop := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- r.Run(runCtx) }()
			defer func() { stop(); <-done }()
			b.joined(t, ctx, logs)
			submission := testSubmission("run-broker")
			submission.Wherry.Tenant, submission.Wherry.Host, submission.Wherry.Pool = tenant, host, pool
			run := wire.RunID(tenant, host, submission.Wherry.RunNonce)
			submission.Wherry.Run, submission.Message.TaskID, submission.Message.ContextID = run, run, run
			b.seal(t, ctx, submission)
			value, err := wire.SignRecord(submission, hostKey)
			if err != nil {
				t.Fatal(err)
			}
			if err := kafka.ProduceSync(ctx, &kgo.Record{Topic: b.tasksTopic(t, ctx), Key: []byte(run), Value: value}).FirstErr(); err != nil {
				t.Fatal(err)
			}
			for {
				if _, _, follows, _ := plugin.counts(); follows == 1 {
					break
				}
				if ctx.Err() != nil {
					t.Fatalf("run never started; logs:\n%s", logs.String())
				}
				time.Sleep(50 * time.Millisecond)
			}
			stopping.stop(t, ctx, b, stop)
			if err := <-done; !errors.Is(err, stopping.want) {
				t.Fatalf("receiver stopped with %v, want %v; logs:\n%s", err, stopping.want, logs.String())
			}
			done <- nil
			stopped := time.Now()
			member, err := kgo.NewClient(append(append([]kgo.Opt(nil), opts...), kgo.ConsumeTopics(b.tasksTopic(t, ctx)), kgo.ShareGroup(operator.PoolGroup(pool)), kgo.ShareMaxRecords(1))...)
			if err != nil {
				t.Fatal(err)
			}
			defer member.Close()
			var redelivered *kgo.Record
			for redelivered == nil {
				pollCtx, cancelPoll := context.WithTimeout(ctx, 2*time.Second)
				for _, record := range member.PollRecords(pollCtx, 1).Records() {
					if string(record.Key) == run {
						redelivered = record
					}
				}
				cancelPoll()
				if ctx.Err() != nil {
					t.Fatalf("run-broker never delivered to another member; logs:\n%s", logs.String())
				}
			}
			if waited := time.Since(stopped); waited > 10*time.Second {
				t.Fatalf("task returned to the pool %s after shutdown, want at once (the acquisition lock expired instead)", waited)
			}
			if count := redelivered.DeliveryCount(); count != 2 {
				t.Fatalf("redelivered with delivery count %d, want 2: the receiver's claim and this one", count)
			}
			offsets, err := kadm.NewClient(kafka).DescribeShareGroupOffsets(ctx, operator.PoolGroup(pool))
			if err != nil {
				t.Fatal(err)
			}
			if offset, ok := offsets[operator.PoolGroup(pool)].Offsets.Lookup(b.tasksTopic(t, ctx), redelivered.Partition); !ok || offset.Err != nil || offset.StartOffset > redelivered.Offset {
				t.Fatalf("share group start offset %+v past the task at %d: the task was archived", offset, redelivered.Offset)
			}
		})
	}
}

// seal puts in submission an envelope wrapped to the pool's current key once its custodian has published one.
func (b brokerPool) seal(t *testing.T, ctx context.Context, submission *wire.Submission) {
	t.Helper()
	for {
		records, err := readTopic(ctx, b.kafka, operator.CustodianTopic(b.pool))
		if err != nil {
			t.Fatal(err)
		}
		if len(records) > 0 {
			var snapshot wire.Snapshot
			if err := json.Unmarshal(decodedPayload(records[len(records)-1].Value), &snapshot); err != nil {
				t.Fatal(err)
			}
			sealTask(t, submission, testTerms(), snapshot.Current.Key.Key.(*ecdsa.PublicKey), nil)
			return
		}
		if ctx.Err() != nil {
			t.Fatal("the pool's custodian never published a snapshot")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// tasksTopic is the signature topic, under the pool's recorded terms, of a receiver's testConfig profile, where a claude
// task with no selector goes.
func (b brokerPool) tasksTopic(t *testing.T, ctx context.Context) string {
	t.Helper()
	state, err := b.service.LoadState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	profile := testConfig(b.receiver).Profiles[0]
	return operator.SignatureTopic(b.pool, state.Pools[b.pool], profile.Agent, profile.Labels)
}

// groupMembers counts the members of the pool's share group whose client ID is clientID.
func (b brokerPool) groupMembers(t *testing.T, ctx context.Context, clientID string) int {
	t.Helper()
	groups, err := kadm.NewClient(b.kafka).DescribeShareGroups(ctx, operator.PoolGroup(b.pool))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, member := range groups[operator.PoolGroup(b.pool)].Members {
		if member.ClientID == clientID {
			n++
		}
	}
	return n
}

// configure records the receiver's configuration with capacity.
func (b brokerPool) configure(t *testing.T, ctx context.Context, capacity int64) {
	t.Helper()
	config := testConfig(b.receiver)
	config.Pool, config.Capacity = b.pool, capacity
	if _, err := b.service.ReceiverUpdate(ctx, config, false); err != nil {
		t.Fatal(err)
	}
}

// deliveryLimit sets the pool's delivery limit in its terms and on its share group, as pool apply does.
func (b brokerPool) deliveryLimit(t *testing.T, ctx context.Context, limit int32) {
	t.Helper()
	terms := testTerms()
	terms.DeliveryLimit = limit
	b.applyTerms(t, ctx, terms)
}

// member adds receiver id to the pool as receiver apply does.
func (b brokerPool) member(t *testing.T, ctx context.Context, id string) {
	t.Helper()
	config := testConfig(id)
	config.Pool = b.pool
	if err := b.service.ReceiverAdd(ctx, config, b.service.Broker, filepath.Join(t.TempDir(), id+".bundle")); err != nil {
		t.Fatal(err)
	}
}

// laneMember is a receiver of the broker pool under id, its plugins by profile name, built as the broker tests build one,
// with its client ID its receiver ID so DescribeShareGroups names its members.
func (b brokerPool) laneMember(t *testing.T, id string, keys signed.Keys, plugins map[string]*fakePlugin) (*Receiver, *syncBuffer) {
	t.Helper()
	base, err := kgo.NewClient(append(slices.Clone(b.opts), kgo.ClientID(id))...)
	if err != nil {
		t.Fatal(err)
	}
	_, signing, _ := ed25519.GenerateKey(rand.Reader)
	keys[id] = signed.KeyState{Current: signing.Public().(ed25519.PublicKey)}
	logs := &syncBuffer{}
	return &Receiver{id: id, base: base, reader: operator.NewReader(base), signing: signing, stateDir: t.TempDir(), logger: log.New(logs, "", 0), metrics: newMetrics(prometheus.NewRegistry()),
		factory: func(_ string, settings operator.ProfileSettings, _ string) (Named, error) {
			return Named{Name: settings.Name, Agent: settings.Agent, Labels: settings.Labels, Plugin: plugins[settings.Name]}, nil
		}}, logs
}

// runLane runs r until the test ends.
func runLane(t *testing.T, ctx context.Context, r *Receiver) {
	t.Helper()
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- r.Run(runCtx) }()
	t.Cleanup(func() { stop(); <-done })
}

// laneHost registers a host key for the pool's host and returns it.
func (b brokerPool) laneHost(t *testing.T, ctx context.Context) ed25519.PrivateKey {
	t.Helper()
	_, hostKey, _ := ed25519.GenerateKey(rand.Reader)
	registration, err := signed.RegisterKey(signed.HostOwner(b.tenant, b.host), hostKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.kafka.ProduceSync(ctx, &kgo.Record{Topic: operator.HostTopic(b.tenant, b.host), Value: registration}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	return hostKey
}

// laneTask seals and signs name's task with hostKey, edited by mutate, puts it on topic and returns its run.
func (b brokerPool) laneTask(t *testing.T, ctx context.Context, hostKey ed25519.PrivateKey, name, topic string, mutate func(*wire.Submission)) string {
	t.Helper()
	submission := testSubmission(name)
	submission.Wherry.Tenant, submission.Wherry.Host, submission.Wherry.Pool = b.tenant, b.host, b.pool
	run := wire.RunID(b.tenant, b.host, submission.Wherry.RunNonce)
	submission.Wherry.Run, submission.Message.TaskID, submission.Message.ContextID = run, run, run
	if mutate != nil {
		mutate(submission)
	}
	b.seal(t, ctx, submission)
	value, err := wire.SignRecord(submission, hostKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.kafka.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(run), Value: value}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	return run
}

// laneEvents returns run's terminal statuses and the receivers of its start failures, in log order.
func (b brokerPool) laneEvents(t *testing.T, ctx context.Context, keys signed.Keys, run string) ([]wire.RunStatus, []string) {
	t.Helper()
	records, err := readTopic(ctx, b.kafka, operator.PoolEventsTopic(b.pool))
	if err != nil {
		t.Fatal(err)
	}
	var terminals []wire.RunStatus
	var failed []string
	for _, record := range records {
		var status wire.RunStatus
		var failure wire.StartFailure
		switch {
		case string(record.Key) != run:
		case wire.ParseRecord(record.Value, keys, &status) == nil && status.Wherry.Kind == "terminal":
			terminals = append(terminals, status)
		case wire.ParseRecord(record.Value, keys, &failure) == nil:
			failed = append(failed, failure.Wherry.Receiver)
		}
	}
	return terminals, failed
}

// awaitTerminal waits for run's first terminal and returns its terminals and start failures then.
func (b brokerPool) awaitTerminal(t *testing.T, ctx context.Context, keys signed.Keys, run string, logs ...*syncBuffer) ([]wire.RunStatus, []string) {
	t.Helper()
	for {
		if terminals, failed := b.laneEvents(t, ctx, keys, run); len(terminals) > 0 {
			return terminals, failed
		}
		if ctx.Err() != nil {
			var all []string
			for _, log := range logs {
				all = append(all, log.String())
			}
			t.Fatalf("no terminal for %s; logs:\n%s", run, strings.Join(all, "\n---\n"))
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// noRedelivery polls topic as another member for 3 s and fails when run's task is delivered.
func (b brokerPool) noRedelivery(t *testing.T, ctx context.Context, topic, run string) {
	t.Helper()
	pollCtx, cancelPoll := context.WithTimeout(ctx, 3*time.Second)
	defer cancelPoll()
	poller, err := kgo.NewClient(append(slices.Clone(b.opts), kgo.ConsumeTopics(topic), kgo.ShareGroup(operator.PoolGroup(b.pool)), kgo.ShareMaxRecords(1))...)
	if err != nil {
		t.Fatal(err)
	}
	defer poller.Close()
	for _, record := range poller.PollRecords(pollCtx, 1).Records() {
		if string(record.Key) == run {
			t.Fatal("the ended task was delivered again")
		}
	}
}

// TestAllAttemptsFailEndAtTheBudgetOnRealBroker runs two members of one signature whose plugins both fail to start,
// whichever receives each delivery: the task ends in one budget terminal at the third attempt under a delivery limit of
// 4, with one start failure and one dispatch per attempt, and is not delivered again.
func TestAllAttemptsFailEndAtTheBudgetOnRealBroker(t *testing.T) {
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" {
		t.Skip("WHERRY_KAFKA_TEST_BROKER not set")
	}
	shortIntervals(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	b := newBrokerPool(t, ctx, address)
	b.deliveryLimit(t, ctx, 4)
	second := b.receiver + "-b"
	b.member(t, ctx, second)
	hostKey := b.laneHost(t, ctx)
	keys := signed.Keys{}
	firstPlugin, laterPlugin := &fakePlugin{outcome: pluginsdk.OutcomeNothingCreated}, &fakePlugin{outcome: pluginsdk.OutcomeNothingCreated}
	first, firstLogs := b.laneMember(t, b.receiver, keys, map[string]*fakePlugin{"nomad": firstPlugin})
	later, laterLogs := b.laneMember(t, second, keys, map[string]*fakePlugin{"nomad": laterPlugin})
	runLane(t, ctx, first)
	runLane(t, ctx, later)
	b.joined(t, ctx, firstLogs)
	topic := b.tasksTopic(t, ctx)
	run := b.laneTask(t, ctx, hostKey, "run-budget", topic, nil)
	terminals, failed := b.awaitTerminal(t, ctx, keys, run, firstLogs, laterLogs)
	b.noRedelivery(t, ctx, topic, run)
	terminals, failed = b.laneEvents(t, ctx, keys, run)
	terminal := terminals[0]
	reason := terminal.StatusUpdate.Status.Message.Parts[0].Text
	if len(terminals) != 1 || terminal.StatusUpdate.Status.State != wire.TaskStateFailed || reason != "delivery budget exhausted after 3 attempts; last: profile nomad created no job" {
		t.Fatalf("terminals %d, first by %s: %s %q", len(terminals), terminal.Wherry.Receiver, terminal.StatusUpdate.Status.State, reason)
	}
	firstDispatches, _, _, _ := firstPlugin.counts()
	laterDispatches, _, _, _ := laterPlugin.counts()
	if len(failed) != 3 || firstDispatches+laterDispatches != 3 {
		t.Fatalf("start failures by %v and dispatches %d and %d, want one of each per attempt", failed, firstDispatches, laterDispatches)
	}
	t.Logf("attempts by %v", failed)
}

// TestOneBrokenOneWorkingMemberOnRealBroker runs two members of one signature, one whose plugin fails to start and one
// whose plugin runs the task: the run completes when the working member receives an attempt, or ends at the budget when
// the broken one receives every attempt. Either outcome is the accepted consequence; one terminal is not.
func TestOneBrokenOneWorkingMemberOnRealBroker(t *testing.T) {
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" {
		t.Skip("WHERRY_KAFKA_TEST_BROKER not set")
	}
	shortIntervals(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	b := newBrokerPool(t, ctx, address)
	b.deliveryLimit(t, ctx, 4)
	second := b.receiver + "-b"
	b.member(t, ctx, second)
	hostKey := b.laneHost(t, ctx)
	keys := signed.Keys{}
	broken, brokenLogs := b.laneMember(t, b.receiver, keys, map[string]*fakePlugin{"nomad": {outcome: pluginsdk.OutcomeNothingCreated}})
	working, workingLogs := b.laneMember(t, second, keys, map[string]*fakePlugin{"nomad": {stream: resultStream}})
	runLane(t, ctx, broken)
	runLane(t, ctx, working)
	b.joined(t, ctx, brokenLogs)
	topic := b.tasksTopic(t, ctx)
	run := b.laneTask(t, ctx, hostKey, "run-mixed", topic, nil)
	b.awaitTerminal(t, ctx, keys, run, brokenLogs, workingLogs)
	b.noRedelivery(t, ctx, topic, run)
	terminals, failed := b.laneEvents(t, ctx, keys, run)
	terminal := terminals[0]
	switch state := terminal.StatusUpdate.Status.State; {
	case len(terminals) != 1:
		t.Fatalf("%d terminals, want one", len(terminals))
	case state == wire.TaskStateCompleted && terminal.Wherry.Receiver == second:
		t.Logf("completed by the working member after start failures by %v", failed)
	case state == wire.TaskStateFailed && strings.HasPrefix(terminal.StatusUpdate.Status.Message.Parts[0].Text, budgetReason) && len(failed) == 3:
		t.Logf("ended at the budget: every attempt reached the broken member")
	default:
		t.Fatalf("terminal %s by %s after start failures by %v", state, terminal.Wherry.Receiver, failed)
	}
}

// TestUnreadyReleaseSpendsAtMostTwoDeliveriesOnRealBroker runs a member of capacity 2 subscribed to profiles A and B and
// makes A unready as a task on A's topic arrives: a slot that fetches it releases it and the other slot, still polling,
// may re-acquire it once before its watcher acts, so the task has spent at most two deliveries when another member
// subscribed to A's topic later receives it.
func TestUnreadyReleaseSpendsAtMostTwoDeliveriesOnRealBroker(t *testing.T) {
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" {
		t.Skip("WHERRY_KAFKA_TEST_BROKER not set")
	}
	shortIntervals(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	b := newBrokerPool(t, ctx, address)
	terms := testTerms()
	terms.Routable = []string{"sig"}
	b.applyTerms(t, ctx, terms)
	config := testConfig(b.receiver)
	config.Pool, config.Capacity = b.pool, 2
	config.Profiles = []operator.ProfileSettings{
		{Name: "a", Plugin: "fake", Agent: "claude", Labels: map[string]string{"sig": "a"}, Config: map[string]any{}},
		{Name: "b", Plugin: "fake", Agent: "claude", Labels: map[string]string{"sig": "b"}, Config: map[string]any{}},
	}
	if _, err := b.service.ReceiverUpdate(ctx, config, false); err != nil {
		t.Fatal(err)
	}
	state, err := b.service.LoadState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	topicA := operator.SignatureTopic(b.pool, state.Pools[b.pool], "claude", map[string]string{"sig": "a"})
	hostKey := b.laneHost(t, ctx)
	keys := signed.Keys{}
	a := &fakePlugin{stream: resultStream}
	r, logs := b.laneMember(t, b.receiver, keys, map[string]*fakePlugin{"a": a, "b": {stream: resultStream}})
	stop := func() func() {
		runCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- r.Run(runCtx) }()
		return func() { stop(); <-done }
	}()
	for b.groupMembers(t, ctx, b.receiver) != 2 {
		if ctx.Err() != nil {
			t.Fatalf("the member's two slots never joined; logs:\n%s", logs.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(time.Second)
	a.mu.Lock()
	a.unready = true
	a.mu.Unlock()
	run := b.laneTask(t, ctx, hostKey, "run-unready", topicA, nil)
	// One 200 ms advertisement, the watcher's 100 ms and the close; then nothing more of the member polls A's topic.
	time.Sleep(5 * time.Second)
	stop()
	t.Logf("releases: %d", strings.Count(logs.String(), "no ready profile for topic"))
	member, err := kgo.NewClient(append(slices.Clone(b.opts), kgo.ConsumeTopics(topicA), kgo.ShareGroup(operator.PoolGroup(b.pool)), kgo.ShareMaxRecords(1))...)
	if err != nil {
		t.Fatal(err)
	}
	defer member.Close()
	for {
		pollCtx, cancelPoll := context.WithTimeout(ctx, 2*time.Second)
		records := member.PollRecords(pollCtx, 1).Records()
		cancelPoll()
		if len(records) > 0 && string(records[0].Key) == run {
			if count := records[0].DeliveryCount(); count > 3 {
				t.Fatalf("delivered here with count %d, want at most 2 earlier deliveries; logs:\n%s", count, logs.String())
			}
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("the task never reached another member of A's topic; logs:\n%s", logs.String())
		}
	}
}

// TestUnknownSubscribedTopicOnRealBroker documents, without asserting it, the broker's answer to a share member whose
// only subscribed topic does not exist: what its poll returns and how its heartbeat fares. The build report quotes the
// log.
func TestUnknownSubscribedTopicOnRealBroker(t *testing.T) {
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" {
		t.Skip("WHERRY_KAFKA_TEST_BROKER not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	b := newBrokerPool(t, ctx, address)
	missing := operator.PoolTasksPrefix(b.pool) + "claude." + strings.Repeat("0", 64)
	member, err := kgo.NewClient(append(slices.Clone(b.opts), kgo.ClientID("unknown-topic"), kgo.ConsumeTopics(missing), kgo.ShareGroup(operator.PoolGroup(b.pool)), kgo.ShareMaxRecords(1), kgo.FetchMaxWait(time.Second))...)
	if err != nil {
		t.Fatal(err)
	}
	defer member.Close()
	for i := range 3 {
		pollCtx, cancelPoll := context.WithTimeout(ctx, 5*time.Second)
		fetches := member.PollRecords(pollCtx, 1)
		cancelPoll()
		t.Logf("poll %d: %d records, error %v", i, len(fetches.Records()), fetches.Err())
	}
	groups, err := kadm.NewClient(b.kafka).DescribeShareGroups(ctx, operator.PoolGroup(b.pool))
	if err != nil {
		t.Logf("describe the pool's share group: %v", err)
		return
	}
	group := groups[operator.PoolGroup(b.pool)]
	t.Logf("group state %q, error %v", group.GroupState, group.Err)
	for _, described := range group.Members {
		if described.ClientID == "unknown-topic" {
			t.Logf("member %s epoch %d subscribed %v assigned %v", described.MemberID, described.MemberEpoch, described.SubscribedTopicNames, described.Assignment)
		}
	}
}

// TestCredentialedRunOnRealBroker runs a credentialed task from a host's Submit to its Await on the real broker: the
// receiver's seal request is a transaction on the tenant feed, Await answers it, and the receiver opens the seal.
func TestCredentialedRunOnRealBroker(t *testing.T) {
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" {
		t.Skip("WHERRY_KAFKA_TEST_BROKER not set")
	}
	shortIntervals(t)
	openWait = 30 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	b := newBrokerPool(t, ctx, address)
	credentialFile := filepath.Join(t.TempDir(), credentialName)
	if err := os.WriteFile(credentialFile, []byte(testToken), 0o600); err != nil {
		t.Fatal(err)
	}
	hostLogs := &syncBuffer{}
	_, hostKey, _ := ed25519.GenerateKey(rand.Reader)
	hostKafka, err := kgo.NewClient(append(slices.Clone(b.opts), kgo.FetchIsolationLevel(kgo.ReadCommitted()))...)
	if err != nil {
		t.Fatal(err)
	}
	host, err := client.NewHost(hostKafka, hostKey, operator.TenantPrincipal(b.tenant, b.host), t.TempDir(), map[string]credential.Source{credentialName: {File: credentialFile}}, nil, log.New(hostLogs, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if _, err := host.Register(ctx); err != nil {
		t.Fatal(err)
	}
	base, err := kgo.NewClient(b.opts...)
	if err != nil {
		t.Fatal(err)
	}
	_, signing, _ := ed25519.GenerateKey(rand.Reader)
	logs := &syncBuffer{}
	plugin := &fakePlugin{stream: resultStream}
	r := &Receiver{id: b.receiver, base: base, reader: operator.NewReader(base), signing: signing, stateDir: t.TempDir(), logger: log.New(logs, "", 0), metrics: newMetrics(prometheus.NewRegistry()),
		factory: func(_ string, settings operator.ProfileSettings, _ string) (Named, error) {
			return Named{Name: settings.Name, Agent: settings.Agent, Plugin: plugin}, nil
		}}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- r.Run(runCtx) }()
	defer func() { stop(); <-done }()
	b.joined(t, ctx, logs)
	var submitted client.SubmitResult
	for ctx.Err() == nil {
		if submitted, err = host.Submit(ctx, client.SubmitInput{Prompt: "hello", EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)}); err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("submit never placed: %v", err)
	}
	awaited, err := host.Await(ctx, submitted.RunID, 60*time.Second)
	if err != nil || awaited.Status.State != wire.TaskStateCompleted {
		t.Fatalf("await %+v, %v; receiver logs:\n%s\nhost logs:\n%s", awaited, err, logs.String(), hostLogs.String())
	}
	if dispatches, _, _, _ := plugin.counts(); dispatches != 1 || len(plugin.lastRun.Credentials) != 1 ||
		plugin.lastRun.Credentials[0].Env != credentialEnv || string(plugin.lastRun.Credentials[0].Value) != testToken {
		t.Fatalf("dispatches %d with %d credentials, want the host's credential once", dispatches, len(plugin.lastRun.Credentials))
	}
	if strings.Contains(logs.String()+hostLogs.String(), testToken) {
		t.Fatalf("logs carry the credential:\n%s\n%s", logs.String(), hostLogs.String())
	}
}

// TestFilesOnRealBroker runs a task with an input, a task credential and outputs filling the output limit from a
// host's Submit to its Await on the real broker: the outputs' chunks commit in the result's transaction, within the
// broker's record limit, and the host verifies them. A 480 KiB input travels inline, and 250 long-named outputs over the
// limit lengthen the result, so the result commits only because its history does not carry the input.
func TestFilesOnRealBroker(t *testing.T) {
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" {
		t.Skip("WHERRY_KAFKA_TEST_BROKER not set")
	}
	shortIntervals(t)
	openWait = 30 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	b := newBrokerPool(t, ctx, address)
	b.applyTerms(t, ctx, testTerms())
	input := make([]byte, 480<<10)
	rand.Read(input)
	dir := t.TempDir()
	credentialFile, deployKey := filepath.Join(dir, credentialName), filepath.Join(dir, "deploy-key")
	if err := os.WriteFile(credentialFile, []byte(testToken), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(deployKey, []byte(deployValue), 0o600); err != nil {
		t.Fatal(err)
	}
	hostLogs := &syncBuffer{}
	_, hostKey, _ := ed25519.GenerateKey(rand.Reader)
	hostKafka, err := kgo.NewClient(append(slices.Clone(b.opts), kgo.FetchIsolationLevel(kgo.ReadCommitted()))...)
	if err != nil {
		t.Fatal(err)
	}
	host, err := client.NewHost(hostKafka, hostKey, operator.TenantPrincipal(b.tenant, b.host), t.TempDir(),
		map[string]credential.Source{credentialName: {File: credentialFile}, "deploy-key": {File: deployKey}}, nil, log.New(hostLogs, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if _, err := host.Register(ctx); err != nil {
		t.Fatal(err)
	}
	base, err := kgo.NewClient(b.opts...)
	if err != nil {
		t.Fatal(err)
	}
	_, signing, _ := ed25519.GenerateKey(rand.Reader)
	logs := &syncBuffer{}
	report := []byte("key " + deployValue + "\n")
	big := make([]byte, operator.DefaultOutputLimit-len(report))
	for i := range big {
		big[i] = byte(i * 7)
	}
	outputs := []pluginsdk.Output{{Name: "big.bin", Size: int64(len(big)), Data: big}, {Name: "report.md", Size: int64(len(report)), Data: report}}
	// Outputs past the limit, listed in the result by their long names, leave no room for the input in its history.
	for i := range 250 {
		outputs = append(outputs, pluginsdk.Output{Name: strings.Repeat(fmt.Sprintf("x%0249d/", i), 2) + "over", Size: 1})
	}
	fake := &fakePlugin{stream: resultStream, outputs: outputs}
	r := &Receiver{id: b.receiver, base: base, reader: operator.NewReader(base), signing: signing, stateDir: t.TempDir(), logger: log.New(logs, "", 0), metrics: newMetrics(prometheus.NewRegistry()),
		factory: func(_ string, settings operator.ProfileSettings, _ string) (Named, error) {
			return Named{Name: settings.Name, Agent: settings.Agent, Plugin: fake}, nil
		}}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- r.Run(runCtx) }()
	defer func() { stop(); <-done }()
	b.joined(t, ctx, logs)
	var submitted client.SubmitResult
	for ctx.Err() == nil {
		if submitted, err = host.Submit(ctx, client.SubmitInput{Prompt: "hello", Parts: []wire.Part{{Raw: input, Filename: "data.bin"}},
			Credentials: []wire.TaskCredential{{Name: "deploy-key", File: "id_ed25519"}}, Outputs: []wire.ExpectedOutput{{Name: "report.md", Required: true}},
			EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute)}); err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("submit never placed: %v", err)
	}
	awaited, err := host.Await(ctx, submitted.RunID, 60*time.Second)
	if err != nil || awaited.Status.State != wire.TaskStateCompleted {
		t.Fatalf("await %+v, %v; receiver logs:\n%s\nhost logs:\n%s", awaited.Status, err, logs.String(), hostLogs.String())
	}
	if len(awaited.Outputs) != 2 || !bytes.Equal(awaited.Outputs[0].Data, big) || !regexp.MustCompile(`^key \[redacted:[A-Za-z0-9_-]{16}\]\n$`).Match(awaited.Outputs[1].Data) || len(awaited.Rejected) != 0 || len(awaited.NotReturned) != 250 {
		t.Fatalf("outputs %d, rejected %+v, not returned %d; want big.bin intact, report.md redacted and 250 over the limit", len(awaited.Outputs), awaited.Rejected, len(awaited.NotReturned))
	}
	run := fake.lastRun
	if len(run.Inputs) != 1 || !bytes.Equal(run.Inputs[0].Data, input) || len(run.Credentials) != 2 || string(run.Credentials[1].Value) != deployValue {
		t.Fatalf("dispatched inputs %+v and credentials %d", run.Inputs, len(run.Credentials))
	}
	if strings.Contains(logs.String()+hostLogs.String(), deployValue) {
		t.Fatalf("logs carry the task credential:\n%s\n%s", logs.String(), hostLogs.String())
	}
}

// TestGitInputOnRealBroker runs a git task from a host's Submit to its Await on the real broker: the client writes an
// scp-like remote as an ssh URL, takes the credential and host key of the git entry the remote falls under, refuses a
// remote outside the pool's VCS hosts before publishing, and declares the patch output; the receiver delivers the host key as known_hosts and the clone commands with the key's path, never its value.
func TestGitInputOnRealBroker(t *testing.T) {
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" {
		t.Skip("WHERRY_KAFKA_TEST_BROKER not set")
	}
	shortIntervals(t)
	openWait = 30 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	b := newBrokerPool(t, ctx, address)
	b.applyTerms(t, ctx, vcsTerms("git.example.test"))
	dir := t.TempDir()
	credentialFile, deployKey := filepath.Join(dir, credentialName), filepath.Join(dir, "deploy-key")
	if err := os.WriteFile(credentialFile, []byte(testToken), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(deployKey, []byte(deployValue), 0o600); err != nil {
		t.Fatal(err)
	}
	hostKeyFile := filepath.Join(dir, "known_hosts")
	if err := os.WriteFile(hostKeyFile, []byte(testHostKey), 0o600); err != nil {
		t.Fatal(err)
	}
	hostLogs := &syncBuffer{}
	_, hostKey, _ := ed25519.GenerateKey(rand.Reader)
	hostKafka, err := kgo.NewClient(append(slices.Clone(b.opts), kgo.FetchIsolationLevel(kgo.ReadCommitted()))...)
	if err != nil {
		t.Fatal(err)
	}
	host, err := client.NewHost(hostKafka, hostKey, operator.TenantPrincipal(b.tenant, b.host), t.TempDir(),
		map[string]credential.Source{credentialName: {File: credentialFile}, "deploy-key": {File: deployKey}},
		[]client.GitCredential{{URL: "ssh://git@git.example.test/", Credential: "deploy-key", HostKey: hostKeyFile}}, log.New(hostLogs, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if _, err := host.Register(ctx); err != nil {
		t.Fatal(err)
	}
	base, err := kgo.NewClient(b.opts...)
	if err != nil {
		t.Fatal(err)
	}
	_, signing, _ := ed25519.GenerateKey(rand.Reader)
	logs := &syncBuffer{}
	patch := []byte("From 0000000000000000000000000000000000000000 Mon Sep 17 00:00:00 2001\n")
	fake := &fakePlugin{stream: resultStream, outputs: []pluginsdk.Output{{Name: wire.GitPatch, Size: int64(len(patch)), Data: patch}}}
	r := &Receiver{id: b.receiver, base: base, reader: operator.NewReader(base), signing: signing, stateDir: t.TempDir(), logger: log.New(logs, "", 0), metrics: newMetrics(prometheus.NewRegistry()),
		factory: func(_ string, settings operator.ProfileSettings, _ string) (Named, error) {
			return Named{Name: settings.Name, Agent: settings.Agent, Plugin: fake}, nil
		}}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- r.Run(runCtx) }()
	defer func() { stop(); <-done }()
	b.joined(t, ctx, logs)
	input := client.SubmitInput{
		Prompt: "change the README", Git: &wire.GitInput{Remote: "git@git.example.test:org/repo.git", Ref: "main"},
		GitChanges: "patch", EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(time.Minute),
	}
	var submitted client.SubmitResult
	for ctx.Err() == nil {
		if submitted, err = host.Submit(ctx, input); err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("submit never placed: %v", err)
	}
	tasks, err := readTopic(ctx, b.kafka, b.tasksTopic(t, ctx))
	if err != nil {
		t.Fatal(err)
	}
	outside := input
	outside.Git = &wire.GitInput{Remote: "https://github.com/org/repo.git", Ref: "main"}
	if _, err := host.Submit(ctx, outside); !errors.Is(err, client.ErrInvalidInput) || !strings.Contains(err.Error(), "git remote host github.com is not in pool "+b.pool+"'s vcs hosts") {
		t.Fatalf("submit outside the vcs hosts = %v; want a refusal naming the host and the pool", err)
	}
	if after, err := readTopic(ctx, b.kafka, b.tasksTopic(t, ctx)); err != nil || len(after) != len(tasks) {
		t.Fatalf("task topic holds %d records after the refusal, %v; want %d", len(after), err, len(tasks))
	}
	awaited, err := host.Await(ctx, submitted.RunID, 60*time.Second)
	if err != nil || awaited.Status.State != wire.TaskStateCompleted {
		t.Fatalf("await %+v, %v; receiver logs:\n%s\nhost logs:\n%s", awaited.Status, err, logs.String(), hostLogs.String())
	}
	if len(awaited.Outputs) != 1 || awaited.Outputs[0].Name != wire.GitPatch || !bytes.Equal(awaited.Outputs[0].Data, patch) {
		t.Fatalf("outputs %+v, want the patch", awaited.Outputs)
	}
	run := fake.lastRun
	if len(run.Inputs) != 1 || run.Inputs[0].Name != wire.KnownHostsFile || string(run.Inputs[0].Data) != testHostKey ||
		len(run.Credentials) != 2 || run.Credentials[1].File != wire.GitCredentialFile || string(run.Credentials[1].Value) != deployValue {
		t.Fatalf("dispatched inputs %+v and credentials %d", run.Inputs, len(run.Credentials))
	}
	prompt := string(run.Prompt)
	for _, want := range []string{
		"-- 'ssh://git@git.example.test/org/repo.git' 'repo'", `-i "/task/secrets/credentials/git-credential"`, `UserKnownHostsFile="/task/inputs/known_hosts"`,
		`format-patch --output="/task/outputs/changes.patch" refs/wherry/start..HEAD`, "/task/outputs/changes.patch.",
	} {
		if !strings.Contains(prompt, want) {
			t.Errorf("prompt lacks %q:\n%s", want, prompt)
		}
	}
	if strings.Contains(prompt+logs.String()+hostLogs.String(), deployValue) {
		t.Fatalf("prompt or logs carry the deploy key")
	}
}

// The real-broker fixture provisions objects operator check accepts, as the tests sharing that broker's wherry.operator
// and running check after this package need.
func TestBrokerPoolPassesOperatorCheck(t *testing.T) {
	cluster := kfake.MustCluster()
	t.Cleanup(cluster.Close)
	ctx := t.Context()
	admin, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	// kfake reports no share-group ceilings until they are set.
	var alters []kadm.AlterConfig
	for name, value := range map[string]string{"group.share.max.delivery.count.limit": "10", "group.share.max.record.lock.duration.ms": "60000"} {
		alters = append(alters, kadm.AlterConfig{Op: kadm.SetConfig, Name: name, Value: &value})
	}
	if responses, err := kadm.NewClient(admin).AlterBrokerConfigs(ctx, alters); err != nil || responses[0].Err != nil {
		t.Fatal(err, responses)
	}
	// The real broker's wherry.operator holds these grants from the tests that delegate to it.
	if err := operator.Delegate(ctx, admin, "operator-password"); err != nil {
		t.Fatal(err)
	}
	b := newBrokerPool(t, ctx, cluster.ListenAddrs()[0])
	b.configure(t, ctx, 6)
	b.deliveryLimit(t, ctx, 4)
	b.member(t, ctx, b.receiver+"-b")
	if err := b.service.Check(ctx); err != nil {
		t.Fatalf("operator check after the fixture = %v", err)
	}
}

// tenantEndReceiver authenticates as a receiver, so broker authorization is exercised.
func tenantEndReceiver(t *testing.T, ctx context.Context, b brokerPool) *Receiver {
	t.Helper()
	ca, err := os.ReadFile(os.Getenv("WHERRY_TRUST_CA"))
	if err != nil {
		t.Fatal(err)
	}
	config := testConfig("auth-" + strings.TrimPrefix(b.receiver, "receiver-"))
	config.Pool = b.pool
	path := filepath.Join(t.TempDir(), "receiver.bundle")
	if err := b.service.ReceiverAdd(ctx, config, os.Getenv("WHERRY_TRUST_BROKER"), path); err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var bundle operator.Bundle
	if err := yaml.Unmarshal(encoded, &bundle); err != nil {
		t.Fatal(err)
	}
	r, err := New(bundle, ca, t.TempDir(), false, time.Second, nil, log.New(&syncBuffer{}, "", 0), prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.base.Close)
	t.Cleanup(r.reader.Close)
	// configure requires a factory for the configured profile.
	r.factory = func(_ string, settings operator.ProfileSettings, _ string) (Named, error) {
		return Named{Name: settings.Name, Agent: settings.Agent, Plugin: &fakePlugin{stream: resultStream}}, nil
	}
	if err := r.register(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.configure(ctx); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestHeldRunFinishesAfterEntitlementEndOnRealBroker(t *testing.T) {
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" || os.Getenv("WHERRY_TRUST_BROKER") == "" {
		t.Skip("real broker listeners are not configured")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()
	b := newBrokerPool(t, ctx, address)
	r := tenantEndReceiver(t, ctx, b)
	task := testSubmission("run-a")
	task.Wherry.Tenant, task.Wherry.Host, task.Wherry.Pool = b.tenant, b.host, b.pool
	task.Wherry.Run = wire.RunID(b.tenant, b.host, task.Wherry.RunNonce)
	task.Message.TaskID, task.Message.ContextID = task.Wherry.Run, task.Wherry.Run
	producer, err := r.newProducer(task.Wherry.Run)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	run := &activeRun{receiver: r, producer: producer, task: task, taskHash: wire.Digest([]byte("held-task")), attempt: 1, history: []wire.Message{task.Message}, redactionKey: testRedactionKey}
	if err := r.transact(ctx, producer, run.statusRecords("started", taskStatus(wire.TaskStateWorking, ""), nil, nil)...); err != nil {
		t.Fatal(err)
	}
	if err := b.service.TenantPoolRemove(ctx, b.tenant, b.pool); err != nil {
		t.Fatal(err)
	}
	halted := make(chan error, 1)
	r.halt = func(err error) { halted <- err }
	if ack := run.end(ctx, taskStatus(wire.TaskStateCompleted, ""), "done", nil, kgo.AckAccept); ack != kgo.AckAccept {
		t.Fatalf("held run ack=%v", ack)
	}
	if err := r.refresh(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-halted:
		t.Fatalf("receiver halted: %v", err)
	default:
	}
	reader := operator.NewReader(b.kafka)
	defer reader.Close()
	records, err := reader.ReadFrom(ctx, operator.TenantTopic(b.tenant), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) < 2 {
		t.Fatalf("held run did not finish into feed: %d records", len(records))
	}
}

func TestEndedHostKeyReadsOnRealBroker(t *testing.T) {
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" || os.Getenv("WHERRY_TRUST_BROKER") == "" {
		t.Skip("real broker listeners are not configured")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()
	b := newBrokerPool(t, ctx, address)
	_, hostKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	registration, err := signed.RegisterKey(signed.HostOwner(b.tenant, b.host), hostKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.kafka.ProduceSync(ctx, &kgo.Record{Topic: operator.HostTopic(b.tenant, b.host), Value: registration}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	r := tenantEndReceiver(t, ctx, b)
	// Hold the state from before the end, as a refresh straddling it would.
	state, err := b.service.LoadState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := operator.EncodeRecord(operator.Record{Kind: "end", Tenant: b.tenant, Host: b.host})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.kafka.ProduceSync(ctx, &kgo.Record{Topic: operator.OperatorTopic, Key: []byte("end." + operator.TenantPrincipal(b.tenant, b.host)), Value: encoded}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	// Any operator command converges the recorded end.
	if err := b.service.TenantPoolAdd(ctx, b.tenant, b.pool); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reader.ReadKeys(ctx, operator.HostKeyTopics(state, b.pool)); err != nil {
		t.Fatalf("straddled refresh: %v", err)
	}
	if _, err := r.rereadKey(ctx, &r.hosts, signed.HostOwner(b.tenant, b.host), operator.HostTopic(b.tenant, b.host)); err != nil {
		t.Fatalf("reread ended key: %v", err)
	}
	if err := r.refresh(ctx); err != nil {
		t.Fatal(err)
	}
}
