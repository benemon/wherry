package operator

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// poolWithReceiver applies modelTerms to pool-a and adds modelConfig's receiver-a, whose one profile's signature topic,
// modelTopic("pool-a"), receiver apply creates.
func poolWithReceiver(t *testing.T, service *Service) {
	t.Helper()
	if err := service.PoolApply(context.Background(), "pool-a", modelTerms(), false); err != nil {
		t.Fatal(err)
	}
	addModelReceiver(t, service, "receiver-a")
}

// addModelReceiver adds modelConfig's receiver named receiver to pool-a.
func addModelReceiver(t *testing.T, service *Service, receiver string) {
	t.Helper()
	if err := service.ReceiverAdd(context.Background(), modelConfig(receiver, "pool-a"), service.Broker, filepath.Join(t.TempDir(), receiver+".yaml")); err != nil {
		t.Fatal(err)
	}
}

// zoned is modelConfig's receiver with one profile per zone, each labelled with it.
func zoned(receiver string, zones ...string) ReceiverConfig {
	config := modelConfig(receiver, "pool-a")
	config.Profiles = nil
	for _, zone := range zones {
		config.Profiles = append(config.Profiles, ProfileSettings{Name: "profile-" + zone, Plugin: "fake", Agent: "agent-a", Labels: map[string]string{"zone": zone}})
	}
	return config
}

// zoneTopic is the signature topic of a pool-a profile in zone.
func zoneTopic(zone string) string {
	return SignatureTopic("pool-a", "agent-a", map[string]string{"zone": zone})
}

// listedTaskTopics lists pool's task topics, failing t on an error.
func listedTaskTopics(t *testing.T, service *Service, pool string) []string {
	t.Helper()
	topics, err := service.taskTopics(context.Background(), pool)
	if err != nil {
		t.Fatal(err)
	}
	return topics
}

// assertTaskTopics fails unless each of topics exists with one partition, poolConfigs, the broker's timestamps and
// retention.ms at least retentionMS.
func assertTaskTopics(t *testing.T, service *Service, retentionMS int64, topics ...string) {
	t.Helper()
	ctx := context.Background()
	if err := service.onePartition(ctx, false, topics...); err != nil {
		t.Fatal(err)
	}
	retentions, err := service.topicRetentions(ctx, topics...)
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range poolConfigs {
		values, err := service.topicConfigs(ctx, name, topics...)
		if err != nil {
			t.Fatal(err)
		}
		for _, topic := range topics {
			if values[topic] != want {
				t.Fatalf("%s %s is %s, want %s", topic, name, values[topic], want)
			}
		}
	}
	types, err := service.topicConfigs(ctx, "message.timestamp.type", topics...)
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range topics {
		if !retentionAtLeast(retentions[topic], retentionMS) || types[topic] != "LogAppendTime" {
			t.Fatalf("%s retention.ms %d, timestamps %s; want at least %d, LogAppendTime", topic, retentions[topic], types[topic], retentionMS)
		}
	}
}

// onAppend has appended call check with each record of kind once it is on wherry.operator, as long as the test runs.
func onAppend(t *testing.T, kind string, check func(Record)) {
	t.Helper()
	previous := appended
	t.Cleanup(func() { appended = previous })
	appended = func(record Record) {
		if record.Kind == kind {
			check(record)
		}
	}
}

// receiver apply creates every signature topic of the receiver's profiles, configured, before the record that names
// them; a failed create records nothing, an update creates a new or changed profile's topic before its record and a dry
// run creates none.
func TestReceiverApplyCreatesSignatureTopicsBeforeTheRecord(t *testing.T) {
	cluster, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	if err := service.PoolApply(ctx, "pool-a", modelTerms(), false); err != nil {
		t.Fatal(err)
	}
	bounds, err := poolRetention(withDefaults(modelTerms()))
	if err != nil {
		t.Fatal(err)
	}
	var checked []string
	onAppend(t, "receiver", func(record Record) {
		topics := profileTopics(*record.Config)
		assertTaskTopics(t, service, bounds.tasks, topics...)
		checked = append(checked, topics...)
	})
	if err := service.ReceiverAdd(ctx, zoned("receiver-a", "a", "b"), service.Broker, filepath.Join(t.TempDir(), "a.yaml")); err != nil {
		t.Fatal(err)
	}
	want := slices.Sorted(slices.Values([]string{zoneTopic("a"), zoneTopic("b")}))
	if !slices.Equal(checked, want) || !slices.Equal(listedTaskTopics(t, service, "pool-a"), want) {
		t.Fatalf("topics checked at the record %v, listed %v; want %v", checked, listedTaskTopics(t, service, "pool-a"), want)
	}
	// A create refused for the signature topics leaves no record.
	var refused atomic.Bool
	cluster.ControlKey(int16(kmsg.CreateTopics), func(request kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		create := request.(*kmsg.CreateTopicsRequest)
		if !slices.ContainsFunc(create.Topics, func(topic kmsg.CreateTopicsRequestTopic) bool {
			return strings.HasPrefix(topic.Topic, PoolTasksPrefix("pool-a"))
		}) || !refused.Load() {
			return nil, nil, false
		}
		response := create.ResponseKind().(*kmsg.CreateTopicsResponse)
		for _, topic := range create.Topics {
			failed := kmsg.NewCreateTopicsResponseTopic()
			failed.Topic, failed.ErrorCode = topic.Topic, kerr.PolicyViolation.Code
			response.Topics = append(response.Topics, failed)
		}
		return response, nil, true
	})
	refused.Store(true)
	if err := service.ReceiverAdd(ctx, zoned("receiver-b", "c"), service.Broker, filepath.Join(t.TempDir(), "b.yaml")); err == nil || !strings.Contains(err.Error(), "create topic "+zoneTopic("c")) {
		t.Fatalf("receiver apply with its topic refused = %v; want the create's failure", err)
	}
	if state, err := service.LoadState(ctx); err != nil || len(state.Receivers) != 1 {
		t.Fatalf("receivers %v, %v; want receiver-b unrecorded", state.Receivers, err)
	}
	refused.Store(false)
	// A dry run creates nothing; the update creates its new profile's topic before the record.
	if _, err := service.ReceiverUpdate(ctx, zoned("receiver-a", "a", "b", "d"), true); err != nil {
		t.Fatal(err)
	}
	if slices.Contains(listedTaskTopics(t, service, "pool-a"), zoneTopic("d")) {
		t.Fatal("receiver apply --dry-run created a topic")
	}
	checked = nil
	if _, err := service.ReceiverUpdate(ctx, zoned("receiver-a", "a", "b", "d"), false); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(checked, zoneTopic("d")) {
		t.Fatalf("topics checked at the update's record %v; want %s", checked, zoneTopic("d"))
	}
	// A changed label is a new signature, whose topic exists before the record that moves the profile to it.
	changed := zoned("receiver-a", "a", "b", "d")
	changed.Profiles[1].Labels = map[string]string{"zone": "e"}
	checked = nil
	if _, err := service.ReceiverUpdate(ctx, changed, false); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(checked, zoneTopic("e")) {
		t.Fatalf("topics checked at the changed profile's record %v; want %s", checked, zoneTopic("e"))
	}
}

// pool apply recreates, before its record, the deleted signature topic of a recorded profile of a receiver whose
// membership has not ended, and none for an ended receiver; a retention change converges every topic under the prefix,
// an old signature's included; its dry run creates nothing, a new pool none, and a re-apply of the same terms recreates
// a deleted topic before its no-update path.
func TestPoolApplyCreatesMissingSignatureTopicsBeforeTheRecord(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	if err := service.PoolApply(ctx, "pool-a", modelTerms(), false); err != nil {
		t.Fatal(err)
	}
	if topics := listedTaskTopics(t, service, "pool-a"); len(topics) != 0 {
		t.Fatalf("a new pool has task topics %v, want none", topics)
	}
	for _, receiver := range [][2]string{{"receiver-a", "a"}, {"receiver-b", "b"}, {"receiver-c", "c"}} {
		if err := service.ReceiverAdd(ctx, zoned(receiver[0], receiver[1]), service.Broker, filepath.Join(t.TempDir(), receiver[0]+".yaml")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := service.appendChecked(ctx, []byte("end.receiver-c"), Record{Kind: "end", Receiver: "receiver-c"}); err != nil {
		t.Fatal(err)
	}
	// receiver-b's profile moves to zone b2, so zone b's topic is an old signature's that no recorded profile has.
	moved := zoned("receiver-b", "b")
	moved.Profiles[0].Labels = map[string]string{"zone": "b2"}
	if _, err := service.ReceiverUpdate(ctx, moved, false); err != nil {
		t.Fatal(err)
	}
	want := slices.Sorted(slices.Values([]string{zoneTopic("a"), zoneTopic("b"), zoneTopic("b2"), zoneTopic("c")}))
	if topics := listedTaskTopics(t, service, "pool-a"); !slices.Equal(topics, want) {
		t.Fatalf("task topics %v, want %v", topics, want)
	}
	deleteTopics(t, service, zoneTopic("a"), zoneTopic("c"))
	setTopicConfig(t, service, zoneTopic("b"), "cleanup.policy", "compact")
	longer := modelTerms()
	longer.StartByLimitMS = Millis((10 * 24 * time.Hour).Milliseconds())
	if err := service.PoolApply(ctx, "pool-a", longer, true); err != nil {
		t.Fatal(err)
	}
	if topics := listedTaskTopics(t, service, "pool-a"); slices.Contains(topics, zoneTopic("a")) {
		t.Fatalf("pool apply --dry-run left task topics %v, want %s still missing", topics, zoneTopic("a"))
	}
	bounds, err := poolRetention(withDefaults(longer))
	if err != nil {
		t.Fatal(err)
	}
	recorded := 0
	onAppend(t, "pool", func(Record) {
		recorded++
		assertTaskTopics(t, service, bounds.tasks, zoneTopic("a"))
		if slices.Contains(listedTaskTopics(t, service, "pool-a"), zoneTopic("c")) {
			t.Errorf("pool apply created ended receiver-c's topic %s", zoneTopic("c"))
		}
	})
	if err := service.PoolApply(ctx, "pool-a", longer, false); err != nil {
		t.Fatal(err)
	}
	if recorded != 1 {
		t.Fatalf("pool apply recorded its terms %d times, want once", recorded)
	}
	want = slices.Sorted(slices.Values([]string{zoneTopic("a"), zoneTopic("b"), zoneTopic("b2")}))
	if topics := listedTaskTopics(t, service, "pool-a"); !slices.Equal(topics, want) {
		t.Fatalf("task topics %v, want %v", topics, want)
	}
	assertTaskTopics(t, service, bounds.tasks, want...)
	// The same terms again record nothing, yet recreate a topic deleted since.
	deleteTopics(t, service, zoneTopic("b2"))
	if err := service.Check(ctx); err == nil || !strings.Contains(err.Error(), zoneTopic("b2")+" is missing") {
		t.Fatalf("check without %s = %v; want it named missing", zoneTopic("b2"), err)
	}
	if err := service.PoolApply(ctx, "pool-a", longer, false); err != nil {
		t.Fatal(err)
	}
	if recorded != 1 {
		t.Fatalf("pool apply of the same terms recorded them, %d records in all; want the no-update path", recorded)
	}
	assertTaskTopics(t, service, bounds.tasks, zoneTopic("b2"))
	if err := service.Check(ctx); err != nil {
		t.Fatalf("check after the re-apply: %v", err)
	}
	if err := service.PoolApply(ctx, "pool-b", modelTerms(), false); err != nil {
		t.Fatal(err)
	}
	if topics := listedTaskTopics(t, service, "pool-b"); len(topics) != 0 {
		t.Fatalf("a new pool has task topics %v, want none", topics)
	}
}

// deleteTopics deletes topics, failing t on an error.
func deleteTopics(t *testing.T, service *Service, topics ...string) {
	t.Helper()
	responses, err := kadm.NewClient(service.Operator).DeleteTopics(context.Background(), topics...)
	if err == nil {
		err = responses.Error()
	}
	if err != nil {
		t.Fatal(err)
	}
}

// operator check names a live profile's missing signature topic, a signature topic's drifted config or second partition,
// and a missing prefixed grant.
func TestCheckNamesSignatureTopicDrift(t *testing.T) {
	t.Parallel()
	host := ACL{TenantPrincipal("tenant-a", "host-a"), "topic", PoolTasksPrefix("pool-a"), true, []string{"WRITE"}}
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *Service)
		want   string
	}{
		{"topic missing", func(t *testing.T, service *Service) {
			responses, err := kadm.NewClient(service.Operator).DeleteTopics(context.Background(), modelTopic("pool-a"))
			if err == nil {
				err = responses.Error()
			}
			if err != nil {
				t.Fatal(err)
			}
		}, "receiver receiver-a profile profile-a: signature topic " + modelTopic("pool-a") + " is missing"},
		{"config drifted", func(t *testing.T, service *Service) {
			setTopicConfig(t, service, modelTopic("pool-a"), "cleanup.policy", "compact")
		}, modelTopic("pool-a") + " cleanup.policy is compact, want delete"},
		{"second partition", func(t *testing.T, service *Service) {
			addPartition(t, service, modelTopic("pool-a"))
		}, modelTopic("pool-a") + " has 2 partitions"},
		{"prefixed grant missing", func(t *testing.T, service *Service) {
			builder, err := aclBuilder(host)
			if err != nil {
				t.Fatal(err)
			}
			if err := service.deleteACLs(context.Background(), builder); err != nil {
				t.Fatal(err)
			}
		}, "missing WRITE ACL on " + PoolTasksPrefix("pool-a") + " for " + TenantPrincipal("tenant-a", "host-a")},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, service, closeFixture := kfakeService(t)
			defer closeFixture()
			ctx := context.Background()
			poolWithReceiver(t, service)
			if err := entitleFixture(t, service, ctx, "tenant-a", "pool-a"); err != nil {
				t.Fatal(err)
			}
			if err := hostFixture(t, service, ctx, "tenant-a", "host-a", filepath.Join(t.TempDir(), "host.yaml"), false); err != nil {
				t.Fatal(err)
			}
			if ok, err := granted(ctx, service, host); err != nil || !ok {
				t.Fatalf("host's prefixed WRITE granted %t, %v", ok, err)
			}
			if err := service.Check(ctx); err != nil {
				t.Fatalf("check before drift: %v", err)
			}
			test.mutate(t, service)
			if err := service.Check(ctx); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("check = %v, want %q", err, test.want)
			}
		})
	}
}

// Receivers read and hosts write a pool's task topics through one prefixed grant each, which the broker holds as
// prefixed; no task topic has a literal grant. A suspension drops the hosts' prefixed WRITE to DESCRIBE.
func TestTaskTopicsAreGrantedByPrefix(t *testing.T) {
	state := modelState(t)
	tasks := func(acls []ACL) map[string]ACL {
		found := map[string]ACL{}
		for _, acl := range acls {
			if acl.ResourceType == "topic" && strings.HasPrefix(acl.Resource, "wherry.pool.pool-a.tasks") {
				found[acl.Principal] = acl
			}
		}
		return found
	}
	want := map[string]ACL{
		ReceiverPrincipal("receiver-a"):       {ReceiverPrincipal("receiver-a"), "topic", PoolTasksPrefix("pool-a"), true, []string{"READ", "DESCRIBE"}},
		TenantPrincipal("tenant-a", "host-a"): {TenantPrincipal("tenant-a", "host-a"), "topic", PoolTasksPrefix("pool-a"), true, []string{"WRITE", "DESCRIBE"}},
		TenantPrincipal("tenant-b", "host-b"): {TenantPrincipal("tenant-b", "host-b"), "topic", PoolTasksPrefix("pool-a"), true, []string{"WRITE", "DESCRIBE"}},
	}
	if got := tasks(ACLs(state)); !reflect.DeepEqual(got, want) {
		t.Fatalf("pool-a task topic grants %v, want %v", got, want)
	}
	state.Suspended["pool-a"] = true
	for _, principal := range []string{TenantPrincipal("tenant-a", "host-a"), TenantPrincipal("tenant-b", "host-b")} {
		suspended := want[principal]
		suspended.Operations = []string{"DESCRIBE"}
		want[principal] = suspended
	}
	if got := tasks(ACLs(state)); !reflect.DeepEqual(got, want) {
		t.Fatalf("suspended pool-a task topic grants %v, want %v", got, want)
	}
	_, service, ctx := compromiseFixture(t)
	acls := brokerACLs(t, ctx, service)
	for _, held := range []string{
		"User:" + ReceiverPrincipal("receiver-a") + " TOPIC " + PoolTasksPrefix("pool-a") + " PREFIXED READ",
		"User:" + TenantPrincipal("tenant-a", "host-a") + " TOPIC " + PoolTasksPrefix("pool-a") + " PREFIXED WRITE",
	} {
		if !slices.Contains(acls, held) {
			t.Fatalf("broker ACLs %v lack %q", acls, held)
		}
	}
	for _, acl := range acls {
		if strings.Contains(acl, " TOPIC wherry.pool.pool-a.tasks") && !strings.Contains(acl, " PREFIXED ") {
			t.Fatalf("a task topic has a literal grant: %s", acl)
		}
	}
}

// PoolTaskTopics lists the pool's prefix alone, sorted, and returns a failed request or an error on a topic under the
// prefix as an error, never as an empty pool.
func TestPoolTaskTopicsListsThePrefixAndFailsOnAnError(t *testing.T) {
	cluster, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	topics := []string{zoneTopic("b"), zoneTopic("a"), SignatureTopic("pool-b", "agent-a", nil), PoolEventsTopic("pool-a"), "wherry.pool.pool-a.tasks"}
	if err := service.createTopics(ctx, "-1", topics...); err != nil {
		t.Fatal(err)
	}
	admin := kadm.NewClient(service.Operator)
	want := slices.Sorted(slices.Values([]string{zoneTopic("a"), zoneTopic("b")}))
	if got, err := PoolTaskTopics(ctx, admin, "pool-a"); err != nil || !slices.Equal(got, want) {
		t.Fatalf("listed %v, %v; want %v", got, err, want)
	}
	var fail atomic.Value
	cluster.ControlKey(kmsg.Metadata.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		mode, _ := fail.Load().(string)
		metadata := request.(*kmsg.MetadataRequest)
		if mode == "" || metadata.Topics != nil {
			return nil, nil, false
		}
		if mode == "request" {
			return nil, errors.New("listing refused"), true
		}
		response := metadata.ResponseKind().(*kmsg.MetadataResponse)
		for _, name := range want {
			topic := kmsg.NewMetadataResponseTopic()
			topic.Topic = kmsg.StringPtr(name)
			if name == zoneTopic("b") {
				topic.ErrorCode = kerr.LeaderNotAvailable.Code
			}
			response.Topics = append(response.Topics, topic)
		}
		return response, nil, true
	})
	fail.Store("topic")
	if got, err := PoolTaskTopics(ctx, admin, "pool-a"); err == nil || !errors.Is(err, kerr.LeaderNotAvailable) {
		t.Fatalf("listed %v, %v; want the topic's error", got, err)
	}
	fail.Store("request")
	if got, err := PoolTaskTopics(ctx, admin, "pool-a"); err == nil {
		t.Fatalf("listed %v with the request failing; want an error", got)
	}
}

// The events and progress topics keep at least the greatest task topic's retention, unlimited included; check names
// one below it.
func TestCheckComparesEventsAndProgressWithTheGreatestTaskRetention(t *testing.T) {
	for _, test := range []struct {
		name            string
		tasks, below    int64
		belowTopic, msg string
	}{
		{"events below a longer task topic", (20 * 24 * time.Hour).Milliseconds(), 0, PoolEventsTopic("pool-a"), "below tasks retention.ms 1728000000"},
		{"progress below a longer task topic", (20 * 24 * time.Hour).Milliseconds(), 0, ReceiverProgressTopic("receiver-a"), "below tasks retention.ms 1728000000"},
		{"events below an unlimited task topic", -1, (30 * 24 * time.Hour).Milliseconds(), PoolEventsTopic("pool-a"), "below tasks retention.ms -1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, service, closeFixture := kfakeService(t)
			defer closeFixture()
			ctx := context.Background()
			poolWithReceiver(t, service)
			second := zoneTopic("b")
			if err := service.createTaskTopics(ctx, 367200000, second); err != nil {
				t.Fatal(err)
			}
			if err := service.Check(ctx); err != nil {
				t.Fatalf("check before drift: %v", err)
			}
			setTopicRetention(t, service, second, test.tasks)
			below := test.below
			if below == 0 {
				below = test.tasks - 1
			}
			// Every other topic keeps the greatest retention, so the one below it is the only drift.
			for _, topic := range []string{PoolEventsTopic("pool-a"), ReceiverProgressTopic("receiver-a")} {
				value := test.tasks
				if topic == test.belowTopic {
					value = below
				}
				setTopicRetention(t, service, topic, value)
			}
			want := fmt.Sprintf("topic %s retention.ms %s is %s", test.belowTopic, strconv.FormatInt(below, 10), test.msg)
			if err := service.Check(ctx); err == nil || err.Error() != want {
				t.Fatalf("check = %v, want %q", err, want)
			}
		})
	}
}
