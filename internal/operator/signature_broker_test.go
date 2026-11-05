package operator

import (
	"context"
	"errors"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// These tests pin the platform facts signature topics rely on (F31 to F35) and the prefixed task topic grants, against
// the real broker.

// signatureTopics creates the share pool's signature topics for zones, as receiver apply would.
func (s shareTest) signatureTopics(t *testing.T, zones ...string) []string {
	t.Helper()
	terms := modelTerms()
	var topics []string
	for _, zone := range zones {
		topics = append(topics, SignatureTopic(s.pool, "agent-a", map[string]string{"zone": zone}))
	}
	bounds, err := poolRetention(withDefaults(terms))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.service.createTaskTopics(context.Background(), bounds.tasks, topics...); err != nil {
		t.Fatal(err)
	}
	return topics
}

func (s shareTest) produceTo(t *testing.T, topic, value string) {
	t.Helper()
	if err := s.service.Operator.ProduceSync(context.Background(), &kgo.Record{Topic: topic, Value: []byte(value)}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// members waits up to 30 s for the share group to describe, through the super user, exactly the subscriptions want
// with each member assigned its own topics' partitions, and fails t otherwise.
func (s shareTest) members(t *testing.T, want ...[]string) {
	t.Helper()
	admin := kadm.NewClient(realRoot(t))
	deadline := time.Now().Add(30 * time.Second)
	for {
		described, err := admin.DescribeShareGroups(context.Background(), PoolGroup(s.pool))
		var got [][]string
		assigned := true
		if err == nil {
			for _, member := range described[PoolGroup(s.pool)].Members {
				subscribed := slices.Sorted(slices.Values(member.SubscribedTopicNames))
				got = append(got, subscribed)
				for topic := range member.Assignment {
					assigned = assigned && slices.Contains(subscribed, topic)
				}
				assigned = assigned && len(member.Assignment) == len(subscribed)
			}
		}
		slices.SortFunc(got, slices.Compare)
		slices.SortFunc(want, slices.Compare)
		if err == nil && assigned && slices.EqualFunc(got, want, slices.Equal) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("share group members subscribe to %v (assigned their own: %t, %v); want %v", got, assigned, err, want)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// nothing fails t if member acquires a record within wait.
func nothing(t *testing.T, member *kgo.Client, wait time.Duration) {
	t.Helper()
	if record := acquire(t, member, wait); record != nil {
		t.Fatalf("acquired %q from %s, want nothing", record.Value, record.Topic)
	}
}

// F31: one share group holds members subscribed to different topics, each assigned its own.
func TestOneShareGroupTakesMembersWithDifferentSubscriptionsOnRealBroker(t *testing.T) {
	if os.Getenv("WHERRY_TRUST_BROKER") == "" {
		t.Skip("WHERRY_TRUST_BROKER is not set")
	}
	s := sharePool(t)
	topics := s.signatureTopics(t, "a", "b")
	x, y := s.subscriber(t, nil, topics[0]), s.subscriber(t, nil, topics[1])
	s.produceTo(t, topics[0], "task-a")
	s.produceTo(t, topics[1], "task-b")
	for member, want := range map[*kgo.Client]string{x: "task-a", y: "task-b"} {
		record := acquire(t, member, 30*time.Second)
		if record == nil || string(record.Value) != want || record.DeliveryCount() != 1 {
			t.Fatalf("acquired %+v, want %s at delivery 1", record, want)
		}
		record.Ack(kgo.AckAccept)
		if err := member.FlushAcks(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	s.members(t, []string{topics[0]}, []string{topics[1]})
}

// F32: a record reaches only its topic's subscribers.
func TestATaskReachesOnlyItsTopicsSubscribersOnRealBroker(t *testing.T) {
	if os.Getenv("WHERRY_TRUST_BROKER") == "" {
		t.Skip("WHERRY_TRUST_BROKER is not set")
	}
	s := sharePool(t)
	topics := s.signatureTopics(t, "a", "b")
	x := s.subscriber(t, nil, topics[0])
	// Positive control: x takes its own topic's task.
	s.produceTo(t, topics[0], "task-a")
	if record := acquire(t, x, 30*time.Second); record == nil || string(record.Value) != "task-a" || record.DeliveryCount() != 1 {
		t.Fatalf("x acquired %+v, want task-a at delivery 1", record)
	} else {
		record.Ack(kgo.AckAccept)
		if err := x.FlushAcks(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	s.produceTo(t, topics[1], "task-b")
	nothing(t, x, 2*s.lock)
	y := s.subscriber(t, nil, topics[1])
	s.members(t, []string{topics[0]}, []string{topics[1]})
	if record := acquire(t, y, 30*time.Second); record == nil || string(record.Value) != "task-b" || record.DeliveryCount() != 1 {
		t.Fatalf("y acquired %+v, want task-b at delivery 1", record)
	}
	nothing(t, x, 5*time.Second)
}

// F33: a member that drops a topic, by closing its client for one without it, releases only the record it holds; a
// record it never acquired keeps its count.
func TestDroppingATopicReleasesOnlyTheHeldRecordOnRealBroker(t *testing.T) {
	if os.Getenv("WHERRY_TRUST_BROKER") == "" {
		t.Skip("WHERRY_TRUST_BROKER is not set")
	}
	s := sharePool(t)
	topics := s.signatureTopics(t, "a", "b")
	x := s.subscriber(t, nil, topics...)
	s.produceTo(t, topics[0], "held")
	held := acquire(t, x, 30*time.Second)
	if held == nil || string(held.Value) != "held" || held.DeliveryCount() != 1 {
		t.Fatalf("x acquired %+v, want held at delivery 1", held)
	}
	s.produceTo(t, topics[0], "never-acquired")
	x.Close()
	s.subscriber(t, nil, topics[1])
	y := s.subscriber(t, nil, topics[0])
	s.members(t, []string{topics[0]}, []string{topics[1]})
	counts := map[string]int32{}
	for len(counts) < 2 {
		record := acquire(t, y, 30*time.Second)
		if record == nil {
			t.Fatalf("y acquired %v, want both A records", counts)
		}
		counts[string(record.Value)] = record.DeliveryCount()
		record.Ack(kgo.AckAccept)
		if err := y.FlushAcks(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if counts["held"] != 2 || counts["never-acquired"] != 1 {
		t.Fatalf("delivery counts %v; want held at 2, never-acquired at 1", counts)
	}
}

// F34: a record on a topic no member subscribes to waits, with no delivery counted, until a subscriber joins.
func TestAnUnsubscribedTopicWaitsWithNoDeliveryCountOnRealBroker(t *testing.T) {
	if os.Getenv("WHERRY_TRUST_BROKER") == "" {
		t.Skip("WHERRY_TRUST_BROKER is not set")
	}
	s := sharePool(t)
	topics := s.signatureTopics(t, "a", "b")
	x := s.subscriber(t, nil, topics[0])
	s.members(t, []string{topics[0]})
	s.produceTo(t, topics[1], "waiting")
	nothing(t, x, s.lock+5*time.Second)
	y := s.subscriber(t, nil, topics[1])
	if record := acquire(t, y, 30*time.Second); record == nil || string(record.Value) != "waiting" || record.DeliveryCount() != 1 {
		t.Fatalf("the joining subscriber acquired %+v, want waiting at delivery 1", record)
	}
}

// F35: neither a receiver nor a host principal creates a topic, by CreateTopics or by producing to a missing one.
func TestReceiversAndHostsCannotCreateTopicsOnRealBroker(t *testing.T) {
	if os.Getenv("WHERRY_TRUST_BROKER") == "" {
		t.Skip("WHERRY_TRUST_BROKER is not set")
	}
	f := provision(t)
	ctx := context.Background()
	host := f.connect(t, TenantPrincipal(f.tenantA, f.hostA1), kgo.UnknownTopicRetries(2))
	// Positive control: the host writes its pool's existing signature topic.
	if err := host.ProduceSync(ctx, &kgo.Record{Topic: modelTopic(f.poolA), Value: []byte("existing")}).FirstErr(); err != nil {
		t.Fatalf("host produce to %s: %v", modelTopic(f.poolA), err)
	}
	missing := SignatureTopic(f.poolA, "agent-a", map[string]string{"zone": "nowhere"})
	for _, principal := range []string{ReceiverPrincipal(f.receiverA), TenantPrincipal(f.tenantA, f.hostA1)} {
		created, err := kadm.NewClient(f.connect(t, principal)).CreateTopics(ctx, 1, -1, nil, missing)
		if err == nil {
			err = created.Error()
		}
		if !errors.Is(err, kerr.TopicAuthorizationFailed) && !errors.Is(err, kerr.ClusterAuthorizationFailed) {
			t.Fatalf("%s CreateTopics = %v; want an authorisation refusal", principal, err)
		}
	}
	produceCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := host.ProduceSync(produceCtx, &kgo.Record{Topic: missing, Value: []byte("x")}).FirstErr(); err == nil {
		t.Fatalf("host produced to the missing %s", missing)
	}
	listed, err := PoolTaskTopics(ctx, kadm.NewClient(f.service.Operator), f.poolA)
	if err != nil || slices.Contains(listed, missing) {
		t.Fatalf("pool %s task topics %v, %v; want %s still missing", f.poolA, listed, err, missing)
	}
}

// Task topics are granted by the pool's prefix: a receiver reads its pool's signature topic and lists its pool's topics,
// and neither lists nor reads another pool's; a host writes one, and is refused while the pool is suspended.
func TestTaskTopicGrantsAreThePoolsPrefixOnRealBroker(t *testing.T) {
	if os.Getenv("WHERRY_TRUST_BROKER") == "" {
		t.Skip("WHERRY_TRUST_BROKER is not set")
	}
	f := provision(t)
	ctx := context.Background()
	host := f.connect(t, TenantPrincipal(f.tenantA, f.hostA1))
	if err := host.ProduceSync(ctx, &kgo.Record{Topic: modelTopic(f.poolA), Value: []byte("granted-by-prefix")}).FirstErr(); err != nil {
		t.Fatalf("host produce: %v", err)
	}
	receiver := f.connect(t, ReceiverPrincipal(f.receiverA))
	if !readAll(t, receiver, modelTopic(f.poolA))["granted-by-prefix"] {
		t.Fatalf("receiver did not read %s", modelTopic(f.poolA))
	}
	admin := kadm.NewClient(f.connect(t, ReceiverPrincipal(f.receiverA)))
	if listed, err := PoolTaskTopics(ctx, admin, f.poolA); err != nil || !slices.Equal(listed, []string{modelTopic(f.poolA)}) {
		t.Fatalf("receiver lists %v, %v in its pool; want %s", listed, err, modelTopic(f.poolA))
	}
	if listed, err := PoolTaskTopics(ctx, admin, f.poolB); err != nil || len(listed) != 0 {
		t.Fatalf("receiver lists %v, %v in another pool; want none", listed, err)
	}
	expectDenied(t, listEnd(f.connect(t, ReceiverPrincipal(f.receiverA)), modelTopic(f.poolB)))
	if err := f.service.PoolSuspend(ctx, f.poolA, true); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.service.PoolSuspend(ctx, f.poolA, false) })
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := f.connect(t, TenantPrincipal(f.tenantA, f.hostA1)).ProduceSync(ctx, &kgo.Record{Topic: modelTopic(f.poolA), Value: []byte("suspended")}).FirstErr()
		if errors.Is(err, kerr.TopicAuthorizationFailed) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("host produce while suspended = %v; want TOPIC_AUTHORIZATION_FAILED", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
