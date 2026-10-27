package receiver

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/client"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/kafkatest"
	"github.com/benemon/wherry/internal/wire"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// clusterRig is a broker pool on the multi-broker cluster test/kafka/up-cluster.sh starts, with a registered host
// holding the credential and a receiver whose plugin streams a result.
type clusterRig struct {
	brokerPool
	adm            *kadm.Client
	brokers        []int32
	hostClient     *client.Host
	plugin         *fakePlugin
	logs, hostLogs *syncBuffer
}

// newClusterRig skips t unless the cluster has several brokers.
func newClusterRig(t *testing.T, ctx context.Context) *clusterRig {
	t.Helper()
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" {
		t.Skip("WHERRY_KAFKA_TEST_BROKER not set")
	}
	kafka, err := kgo.NewClient(kgo.SeedBrokers(address))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kafka.Close)
	adm := kadm.NewClient(kafka)
	brokers := kafkatest.Brokers(t, ctx, adm)
	return &clusterRig{brokerPool: newBrokerPool(t, ctx, address), adm: adm, brokers: brokers, logs: &syncBuffer{}, hostLogs: &syncBuffer{}}
}

// start registers the host and runs the receiver until the test ends, returning once it has joined its pool's share
// group.
func (c *clusterRig) start(t *testing.T, ctx context.Context) {
	t.Helper()
	credentialFile := filepath.Join(t.TempDir(), credentialName)
	if err := os.WriteFile(credentialFile, []byte(testToken), 0o600); err != nil {
		t.Fatal(err)
	}
	_, hostKey, _ := ed25519.GenerateKey(rand.Reader)
	hostKafka, err := kgo.NewClient(append(slices.Clone(c.opts), kgo.FetchIsolationLevel(kgo.ReadCommitted()))...)
	if err != nil {
		t.Fatal(err)
	}
	host, err := client.NewHost(hostKafka, hostKey, admin.TenantPrincipal(c.tenant, c.host), t.TempDir(), map[string]credential.Source{credentialName: {File: credentialFile}}, nil, log.New(c.hostLogs, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(host.Close)
	if _, err := host.Register(ctx); err != nil {
		t.Fatal(err)
	}
	c.hostClient = host
	base, err := kgo.NewClient(c.opts...)
	if err != nil {
		t.Fatal(err)
	}
	_, signing, _ := ed25519.GenerateKey(rand.Reader)
	c.plugin = &fakePlugin{stream: resultStream}
	r := &Receiver{id: c.receiver, base: base, reader: admin.NewReader(base), signing: signing, logger: log.New(c.logs, "", 0), metrics: newMetrics(prometheus.NewRegistry()),
		factory: func(_ string, settings admin.ProfileSettings) (Named, error) {
			return Named{Name: settings.Name, Agent: settings.Agent, Plugin: c.plugin}, nil
		}}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- r.Run(runCtx) }()
	t.Cleanup(func() { stop(); <-done })
	c.joined(t, ctx, c.logs)
}

// run submits a credentialed task and awaits it, failing t unless it completes after one dispatch with the host's
// credential.
func (c *clusterRig) run(t *testing.T, ctx context.Context) string {
	t.Helper()
	var submitted client.SubmitResult
	var err error
	for ctx.Err() == nil {
		if submitted, err = c.hostClient.Submit(ctx, client.SubmitInput{Prompt: "hello", Deadline: time.Minute, QueueDeadline: time.Minute}); err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("submit never placed: %v", err)
	}
	awaited, err := c.hostClient.Await(ctx, submitted.RunID, 90*time.Second)
	if err != nil || awaited.Status.State != wire.TaskStateCompleted {
		t.Fatalf("await %+v, %v; receiver logs:\n%s\nhost logs:\n%s", awaited, err, c.logs.String(), c.hostLogs.String())
	}
	if dispatches, _, _, _ := c.plugin.counts(); dispatches != 1 || len(c.plugin.lastRun.Credentials) != 1 || string(c.plugin.lastRun.Credentials[0].Value) != testToken {
		t.Fatalf("dispatches %d with %d credentials, want the host's credential once; logs:\n%s", dispatches, len(c.plugin.lastRun.Credentials), c.logs.String())
	}
	return submitted.RunID
}

// leaders is the current leader of partition 0 of each of topics.
func (c *clusterRig) leaders(t *testing.T, ctx context.Context, topics ...string) map[string]int32 {
	t.Helper()
	leaders := map[string]int32{}
	for _, topic := range topics {
		detail, err := kafkatest.Partition(ctx, c.adm, topic, 0)
		if err != nil {
			t.Fatal(err)
		}
		leaders[topic] = detail.Leader
	}
	return leaders
}

// committed is the values of the run's records on topic that a read_committed reader sees.
func (c *clusterRig) committed(t *testing.T, ctx context.Context, topic, key string) [][]byte {
	t.Helper()
	reader := admin.NewReader(c.kafka)
	defer reader.Close()
	records, err := reader.ReadFrom(ctx, topic, 0)
	if err != nil {
		t.Fatal(err)
	}
	var values [][]byte
	for _, record := range records {
		if !record.Attrs.IsControl() && string(record.Key) == key {
			values = append(values, record.Value)
		}
	}
	return values
}

// TestLeaderMovesDuringReadAndAdmissionOnCluster moves partition leaders under a receiver while it reads and admits a
// task. After the receiver joins its pool's share group the pool tasks topic's leader moves, so the receiver's share
// fetch has to find the new leader, which takes over the share partition's state, before the task is even published.
// While the run is in flight the leaders of the host's key topic, wherry.admin and the tenant's transcript topic,
// which the receiver and host read, rotate continually; and every transaction the receiver commits, the admission
// first, has the leaders of the pool's events and the tenant feed it has just written to moved before it commits, so
// its commit markers go to brokers other than the ones that took its records. The run must still complete once.
func TestLeaderMovesDuringReadAndAdmissionOnCluster(t *testing.T) {
	shortIntervals(t)
	sealWait = 60 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := newClusterRig(t, ctx)
	var mu sync.Mutex
	var commits, moved int
	previous := commitTransaction
	t.Cleanup(func() { commitTransaction = previous })
	commitTransaction = func(ctx context.Context, producer *kgo.Client) error {
		mu.Lock()
		commits++
		mu.Unlock()
		for _, topic := range []string{admin.PoolEventsTopic(c.pool), admin.TenantTopic(c.tenant)} {
			moveCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
			_, err := kafkatest.Rotate(moveCtx, c.adm, topic, 0)
			cancel()
			if err != nil {
				t.Errorf("move %s's leader before a commit: %v", topic, err)
				continue
			}
			mu.Lock()
			moved++
			mu.Unlock()
		}
		return previous(ctx, producer)
	}
	c.start(t, ctx)

	tasks := admin.PoolTasksTopic(c.pool)
	before := c.leaders(t, ctx, tasks)[tasks]
	if _, err := kafkatest.Rotate(ctx, c.adm, tasks, 0); err != nil {
		t.Fatal(err)
	}
	if after := c.leaders(t, ctx, tasks)[tasks]; after == before {
		t.Fatalf("%s's leader stayed on broker %d", tasks, before)
	}
	c.joined(t, ctx, c.logs)

	churnCtx, stopChurn := context.WithCancel(ctx)
	churn := kafkatest.StartChurn(churnCtx, c.adm, 500*time.Millisecond, admin.HostTopic(c.tenant, c.host), admin.AdminTopic, admin.TenantTranscriptTopic(c.tenant))
	c.run(t, ctx)
	stopChurn()
	if moves := churn.Moves(); moves == 0 {
		t.Fatal("no leader moved while the run was in flight")
	}
	mu.Lock()
	defer mu.Unlock()
	if commits == 0 || moved < 2 {
		t.Fatalf("%d commits with %d leader moves before them, want the admission's at least", commits, moved)
	}
}

// TestTransactionsAcrossLeadersOnCluster leads the tenant feed, the tenant's transcript topic and the pool's events by
// three different brokers, and the receiver's progress topic by the first of them, and runs a task: the receiver's
// admission spans the feed and the events, and its conclusion the feed, the transcript, the events and the progress
// topic, so each commit's markers go to every broker. A read_committed reader must then see each of the run's records
// on every topic, the progress topic ending in the run's tombstone; and a transaction across the same partitions that
// aborts must leave nothing any of them shows.
func TestTransactionsAcrossLeadersOnCluster(t *testing.T) {
	shortIntervals(t)
	sealWait = 30 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := newClusterRig(t, ctx)
	feed, transcript, events, progress := admin.TenantTopic(c.tenant), admin.TenantTranscriptTopic(c.tenant), admin.PoolEventsTopic(c.pool), admin.ReceiverProgressTopic(c.receiver)
	spanned := []string{feed, transcript, events}
	if err := kafkatest.Spread(ctx, c.adm, c.brokers, append(slices.Clone(spanned), progress)...); err != nil {
		t.Fatal(err)
	}
	distinct := func() {
		t.Helper()
		leaders := c.leaders(t, ctx, spanned...)
		seen := map[int32]string{}
		for topic, leader := range leaders {
			if other, ok := seen[leader]; ok {
				t.Fatalf("%s and %s are both led by broker %d", topic, other, leader)
			}
			seen[leader] = topic
		}
	}
	distinct()
	c.start(t, ctx)
	run := c.run(t, ctx)
	distinct()

	for _, topic := range spanned {
		if values := c.committed(t, ctx, topic, run); len(values) == 0 {
			t.Fatalf("%s carries none of run %s's records", topic, run)
		}
	}
	transcriptResult, err := c.hostClient.Transcript(ctx, run)
	if err != nil || len(transcriptResult.Lines) == 0 {
		t.Fatalf("transcript %+v, %v, want the plugin's lines", transcriptResult, err)
	}
	if values := c.committed(t, ctx, progress, run); len(values) == 0 || values[len(values)-1] != nil {
		t.Fatalf("progress topic has %d records for run %s, want the run's tombstone last", len(values), run)
	}

	producer, err := kgo.NewClient(append(slices.Clone(c.opts), kgo.TransactionalID(admin.TransactionalPrefix(c.pool)+"aborted"))...)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	aborted := fmt.Sprintf("aborted-%d", time.Now().UnixNano())
	if err := producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	for _, topic := range spanned {
		if err := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(aborted), Value: []byte("aborted")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	if err := producer.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatal(err)
	}
	for _, topic := range spanned {
		if values := c.committed(t, ctx, topic, aborted); len(values) != 0 {
			t.Fatalf("%s shows %d records of an aborted transaction", topic, len(values))
		}
	}
}
