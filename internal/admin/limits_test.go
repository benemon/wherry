package admin

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	json "encoding/json/v2"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/kafkatest"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"gopkg.in/yaml.v3"
)

// A pool record written before the limits existed, as EncodeRecord writes modelTerms.
const poolRecordWithoutLimits = `{"v":1,"kind":"pool","pool":"pool-a","terms":{"agents":[{"name":"agent-a","credentials":[{"name":"model","env":"MODEL_API_KEY"}]}],"default_agent":"agent-a","delivery_limit":3,"lock_duration_ms":15000,"producer_byte_rate":1000000,"request_percentage":50},"at_ms":1}`

func TestPoolLimitsDefaultWhereTermsLeaveThemOut(t *testing.T) {
	terms := modelTerms()
	encoded, err := EncodeRecord(Record{Kind: "pool", Pool: "pool-a", Terms: &terms, AtMS: 1})
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != poolRecordWithoutLimits {
		t.Fatalf("encoded %s, want the record without limits", encoded)
	}
	record, err := DecodeRecord([]byte(poolRecordWithoutLimits))
	if err != nil {
		t.Fatal(err)
	}
	state, err := Fold([]Record{record})
	if err != nil {
		t.Fatal(err)
	}
	if got := state.Pools["pool-a"]; got.OutputLimit != 8<<20 || got.MaxQueueDeadlineMS != DefaultMaxQueueDeadlineMS {
		t.Fatalf("defaults %#v, want 8 MiB and a 14-day max queue deadline", got)
	}
	terms.OutputLimit = 2000
	state, err = Fold([]Record{{Version: 1, Kind: "pool", Pool: "pool-a", Terms: &terms, AtMS: 1}})
	if got := state.Pools["pool-a"]; err != nil || got.OutputLimit != 2000 {
		t.Fatalf("limit %d, %v; want the terms' own", got.OutputLimit, err)
	}
	terms.OutputLimit = -1
	if _, err := EncodeRecord(Record{Kind: "pool", Pool: "pool-a", Terms: &terms}); err == nil {
		t.Fatal("encoded a negative output limit")
	}
}

func TestReplyLimit(t *testing.T) {
	record, err := DecodeRecord([]byte(poolRecordWithoutLimits))
	if err != nil {
		t.Fatal(err)
	}
	state, err := Fold([]Record{record})
	if got := state.Pools["pool-a"].ReplyLimit; err != nil || got != 1<<20 || DefaultReplyLimit != 1<<20 {
		t.Fatalf("reply limit %d, %v; want the 1 MiB default", got, err)
	}
	terms := modelTerms()
	terms.OutputLimit, terms.ReplyLimit = 4<<20, 2<<20
	encoded, err := EncodeRecord(Record{Kind: "pool", Pool: "pool-a", Terms: &terms, AtMS: 1})
	if err != nil || !bytes.Contains(encoded, []byte(`"reply_limit":2097152`)) {
		t.Fatalf("encoded %s, %v; want the terms' reply_limit", encoded, err)
	}
	var parsed Terms
	if err := yaml.Unmarshal([]byte("reply_limit: 3000\n"), &parsed); err != nil || parsed.ReplyLimit != 3000 {
		t.Fatalf("YAML reply_limit %d, %v", parsed.ReplyLimit, err)
	}
	terms.OutputLimit = 1000
	state, err = Fold([]Record{{Version: 1, Kind: "pool", Pool: "pool-a", Terms: &Terms{Agents: terms.Agents, DefaultAgent: terms.DefaultAgent,
		DeliveryLimit: 3, LockDurationMS: 15000, ProducerByteRate: 1, RequestPercentage: 1, OutputLimit: 1000}, AtMS: 1}})
	if got := state.Pools["pool-a"].ReplyLimit; err != nil || got != 1000 {
		t.Fatalf("reply limit %d, %v; want an output limit under the default", got, err)
	}
	for name, limit := range map[string]int64{"negative": -1, "over the output limit": 1001} {
		terms.ReplyLimit = limit
		if _, err := EncodeRecord(Record{Kind: "pool", Pool: "pool-a", Terms: &terms}); err == nil {
			t.Errorf("encoded a reply limit %s", name)
		}
	}
	terms.ReplyLimit = 1000
	if _, err := EncodeRecord(Record{Kind: "pool", Pool: "pool-a", Terms: &terms, AtMS: 1}); err != nil {
		t.Fatalf("a reply limit at the output limit: %v", err)
	}
}

// The largest results carry a reply entry, so the measured result stays the largest a receiver writes.
func TestLargestResultsCarryAReply(t *testing.T) {
	feed, err := largestRecords()
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range feed {
		if !strings.HasPrefix(record.name, "a result") {
			continue
		}
		payload, err := base64.RawURLEncoding.DecodeString(string(bytes.Split(record.value, []byte("."))[1]))
		if err != nil {
			t.Fatal(err)
		}
		var result wire.Result
		var metadata wire.ResultMetadata
		if json.Unmarshal(payload, &result) != nil || json.Unmarshal(result.Task.Metadata, &metadata) != nil {
			t.Fatalf("%s does not decode", record.name)
		}
		if metadata.Reply == nil || !metadata.Reply.Cut || metadata.Reply.Size != math.MaxInt64 {
			t.Errorf("%s carries reply %+v; want one at its bounds", record.name, metadata.Reply)
		}
	}
}

func topicRetention(t *testing.T, service *Service, topic string) int64 {
	t.Helper()
	values, err := service.topicRetentions(context.Background(), topic)
	if err != nil {
		t.Fatal(err)
	}
	return values[topic]
}

func setTopicRetention(t *testing.T, service *Service, topic string, value int64) {
	t.Helper()
	setTopicConfig(t, service, topic, "retention.ms", strconv.FormatInt(value, 10))
}

func setTopicConfig(t *testing.T, service *Service, topic, name, value string) {
	t.Helper()
	responses, err := kadm.NewClient(service.Admin).AlterTopicConfigs(context.Background(), []kadm.AlterConfig{{Op: kadm.SetConfig, Name: name, Value: &value}}, topic)
	if err == nil {
		err = responses[0].Err
	}
	if err != nil {
		t.Fatal(err)
	}
}

func TestPoolCreateSetsDerivedRetentionOnNewAndExistingTopics(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	if err := service.PoolCreate(ctx, "pool-a", modelTerms()); err != nil {
		t.Fatal(err)
	}
	want := int64(1317600000)
	topics := []string{PoolTasksTopic("pool-a"), PoolEventsTopic("pool-a")}
	for _, topic := range topics {
		if got := topicRetention(t, service, topic); got != want {
			t.Fatalf("%s retention.ms = %d, want %d", topic, got, want)
		}
		setTopicRetention(t, service, topic, 1)
		setTopicConfig(t, service, topic, "message.timestamp.type", "CreateTime")
	}
	if err := service.PoolUpdate(ctx, "pool-a", modelTerms(), false); err != nil {
		t.Fatal(err)
	}
	types, err := service.topicConfigs(ctx, "message.timestamp.type", topics...)
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range topics {
		if got := topicRetention(t, service, topic); got != want {
			t.Fatalf("existing %s retention.ms = %d, want %d", topic, got, want)
		}
		if types[topic] != "LogAppendTime" {
			t.Fatalf("existing %s message.timestamp.type = %q, want LogAppendTime", topic, types[topic])
		}
	}
}

func TestPoolCreateRefusesMaxQueueDeadlinePastFeedRetention(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	terms := modelTerms()
	terms.MaxQueueDeadlineMS = 30 * 24 * 60 * 60 * 1000
	err := service.PoolCreate(context.Background(), "pool-a", terms)
	if err == nil || err.Error() != "max queue deadline requires tenant feed retention.ms of at least 2613600000" {
		t.Fatalf("pool create = %v", err)
	}
}

func TestPoolUpdateRefusesMaxQueueDeadlinePastAnEntitledFeed(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	if err := service.PoolCreate(ctx, "pool-a", modelTerms()); err != nil {
		t.Fatal(err)
	}
	if err := service.TenantAdd(ctx, "tenant-a", "pool-a"); err != nil {
		t.Fatal(err)
	}
	setTopicRetention(t, service, TenantTopic("tenant-a"), 1231199999)
	err := service.PoolUpdate(ctx, "pool-a", modelTerms(), false)
	if err == nil || err.Error() != "topic wherry.tenant.tenant-a retention.ms 1231199999 is below required 1231200000" {
		t.Fatalf("pool update = %v", err)
	}
}

func TestPoolUpdateRefusesTermsThatStrandALiveReceiver(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	terms := modelTerms()
	terms.Agents = append(terms.Agents, AgentTerms{Name: "codex"})
	if err := service.PoolCreate(ctx, "pool-a", terms); err != nil {
		t.Fatal(err)
	}
	config := ReceiverConfig{Receiver: "receiver-a", Pool: "pool-a", Capacity: 1, Profiles: []ProfileSettings{{Name: "profile-codex", Plugin: "fake", Agent: "codex"}},
		PluginSHA256: map[string]string{"fake": strings.Repeat("ab", 32)}}
	if err := service.ReceiverAdd(ctx, config, filepath.Join(t.TempDir(), "bundle.yaml")); err != nil {
		t.Fatal(err)
	}
	err := service.PoolUpdate(ctx, "pool-a", modelTerms(), false)
	if err == nil || err.Error() != "pool terms would strand live receivers: receiver receiver-a plugin fake profile profile-codex: receiver agent is outside pool terms" {
		t.Fatalf("pool update without codex = %v", err)
	}
	if state, err := service.LoadState(ctx); err != nil || len(state.Pools["pool-a"].Agents) != 2 {
		t.Fatalf("pool-a terms %v, %v; want the terms with codex kept", state.Pools["pool-a"], err)
	}
	terms.Agents = terms.Agents[1:]
	terms.DefaultAgent = "codex"
	if err := service.PoolUpdate(ctx, "pool-a", terms, false); err != nil {
		t.Fatalf("pool update keeping codex = %v", err)
	}
}

func TestTenantAddRefusesAFeedBelowThePoolsMaxQueueDeadline(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	if err := service.PoolCreate(ctx, "pool-a", modelTerms()); err != nil {
		t.Fatal(err)
	}
	if err := service.createTopics(ctx, "1231199999", TenantTopic("tenant-a")); err != nil {
		t.Fatal(err)
	}
	err := service.TenantAdd(ctx, "tenant-a", "pool-a")
	if err == nil || err.Error() != "topic wherry.tenant.tenant-a retention.ms 1231199999 is below required 1231200000" {
		t.Fatalf("tenant add = %v", err)
	}
	if state, err := service.LoadState(ctx); err != nil || len(state.Entitlements) != 0 {
		t.Fatalf("entitlements %v, %v; want none written", state.Entitlements, err)
	}
}

func TestCheckReportsRetentionDrift(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *Service)
		want   string
	}{
		{"tasks below max queue deadline plus run time and margin", func(t *testing.T, service *Service) {
			setTopicRetention(t, service, PoolTasksTopic("pool-a"), 1317599999)
		}, "wherry.pool.pool-a.tasks"},
		{"events below tasks", func(t *testing.T, service *Service) {
			setTopicRetention(t, service, PoolTasksTopic("pool-a"), 1320000000)
		}, "wherry.pool.pool-a.events"},
		{"feed below max queue deadline plus run time", func(t *testing.T, service *Service) {
			setTopicRetention(t, service, TenantTopic("tenant-a"), 1231199999)
		}, "wherry.tenant.tenant-a"},
		{"transcript topic below max queue deadline plus run time", func(t *testing.T, service *Service) {
			setTopicRetention(t, service, TenantTranscriptTopic("tenant-a"), 1231199999)
		}, "wherry.tenant.tenant-a.transcript"},
		{"events not stamped by the broker", func(t *testing.T, service *Service) {
			setTopicConfig(t, service, PoolEventsTopic("pool-a"), "message.timestamp.type", "CreateTime")
		}, "wherry.pool.pool-a.events message.timestamp.type"},
		{"identity is not forever", func(t *testing.T, service *Service) {
			setTopicRetention(t, service, AdminTopic, 1)
		}, "wherry.admin"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, service, closeFixture := kfakeService(t)
			defer closeFixture()
			ctx := context.Background()
			if err := service.PoolCreate(ctx, "pool-a", modelTerms()); err != nil {
				t.Fatal(err)
			}
			if err := service.TenantAdd(ctx, "tenant-a", "pool-a"); err != nil {
				t.Fatal(err)
			}
			test.mutate(t, service)
			if err := service.Check(ctx); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("check = %v, want drift on %s", err, test.want)
			}
		})
	}
}

func setTopicLimit(t *testing.T, service *Service, topic, value string) {
	t.Helper()
	responses, err := kadm.NewClient(service.Admin).AlterTopicConfigs(context.Background(), []kadm.AlterConfig{{Op: kadm.SetConfig, Name: "max.message.bytes", Value: &value}}, topic)
	if err == nil {
		err = responses[0].Err
	}
	if err != nil {
		t.Fatal(err)
	}
}

// kfake's brokers accept 1048588-byte batches, so franz-go's 1000012-byte producer maximum is the limit until the
// broker or the tasks topic sets a lower one.
func TestPoolCreateRecordsTheRecordLimit(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	terms := modelTerms()
	if err := service.PoolCreate(ctx, "pool-a", terms); err != nil {
		t.Fatal(err)
	}
	recorded := func() int64 {
		t.Helper()
		if err := service.PoolUpdate(ctx, "pool-a", terms, false); err != nil {
			t.Fatal(err)
		}
		state, err := service.LoadState(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return state.Pools["pool-a"].RecordLimit
	}
	if got := recorded(); got != 1000012 {
		t.Fatalf("record limit %d; want the producer maximum", got)
	}
	message := "1000000"
	responses, err := kadm.NewClient(service.Admin).AlterBrokerConfigs(ctx, []kadm.AlterConfig{{Op: kadm.SetConfig, Name: "message.max.bytes", Value: &message}})
	if err == nil {
		err = responses[0].Err
	}
	if err != nil {
		t.Fatal(err)
	}
	if got := recorded(); got != 1000000 {
		t.Fatalf("record limit %d; want the broker's message.max.bytes", got)
	}
	setTopicLimit(t, service, PoolTasksTopic("pool-a"), "600000")
	if got := recorded(); got != 600000 {
		t.Fatalf("record limit %d; want the task topic's max.message.bytes", got)
	}
	if err := service.TenantAdd(ctx, "tenant-a", "pool-a"); err != nil {
		t.Fatal(err)
	}
	setTopicLimit(t, service, TenantTopic("tenant-a"), "900000")
	if err := service.PoolUpdate(ctx, "pool-a", terms, false); err == nil || !strings.Contains(err.Error(), "a transcript chunk record is ") || !strings.Contains(err.Error(), "over the 900000-byte record limit") {
		t.Fatalf("pool update = %v; want the transcript chunk refused against the tenant feed's limit", err)
	}
	setTopicLimit(t, service, TenantTopic("tenant-a"), "700000")
	if err := service.PoolUpdate(ctx, "pool-a", terms, false); err == nil || !strings.Contains(err.Error(), "a result record is ") || !strings.Contains(err.Error(), "over the 700000-byte record limit") {
		t.Fatalf("pool update = %v; want the result refused against the tenant feed's limit", err)
	}
	setTopicLimit(t, service, TenantTopic("tenant-a"), "400000")
	if err := service.PoolUpdate(ctx, "pool-a", terms, false); err == nil || !strings.Contains(err.Error(), "an output chunk record is ") || !strings.Contains(err.Error(), "over the 400000-byte record limit") {
		t.Fatalf("pool update = %v; want the output chunk refused against the tenant feed's limit", err)
	}
}

func TestCheckReportsLimitsTheBrokerNoLongerCarries(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	if err := service.PoolCreate(ctx, "pool-a", modelTerms()); err != nil {
		t.Fatal(err)
	}
	// Terms that leave the output limit out are recorded without it; its default applies where the terms are read.
	records, err := readTopic(ctx, service.Operator, AdminTopic)
	if err != nil || len(records) != 1 || strings.Contains(string(records[0].Value), "output_limit") {
		t.Fatalf("pool record %v, %v; want one without an output limit", records, err)
	}
	if err := service.TenantAdd(ctx, "tenant-a", "pool-a"); err != nil {
		t.Fatal(err)
	}
	if err := service.Check(ctx); err != nil {
		t.Fatalf("check before drift: %v", err)
	}
	setTopicLimit(t, service, TenantTopic("tenant-a"), "400000")
	if err := service.Check(ctx); err == nil || !strings.HasPrefix(err.Error(), "pool pool-a: ") || !strings.HasSuffix(err.Error(), "over the 400000-byte record limit") {
		t.Fatalf("check = %v; want the pool's drift reported", err)
	}
}

func TestTenantAddRefusesAFeedThePoolDoesNotFit(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	if err := service.PoolCreate(ctx, "pool-a", modelTerms()); err != nil {
		t.Fatal(err)
	}
	if err := service.createTopics(ctx, strconv.Itoa(tenantRetentionMS), TenantTopic("tenant-a")); err != nil {
		t.Fatal(err)
	}
	setTopicLimit(t, service, TenantTopic("tenant-a"), "400000")
	if err := service.TenantAdd(ctx, "tenant-a", "pool-a"); err == nil || !strings.HasSuffix(err.Error(), "over the 400000-byte record limit") {
		t.Fatalf("tenant add = %v; want the tenant feed's limit", err)
	}
	if state, err := service.LoadState(ctx); err != nil || len(state.Entitlements) != 0 {
		t.Fatalf("entitlements %v, %v; want none written", state.Entitlements, err)
	}
}

func TestCheckFailsOnAMissingTenantFeed(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	if err := service.PoolCreate(ctx, "pool-a", modelTerms()); err != nil {
		t.Fatal(err)
	}
	if err := service.TenantAdd(ctx, "tenant-a", "pool-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := kadm.NewClient(service.Admin).DeleteTopics(ctx, TenantTopic("tenant-a")); err != nil {
		t.Fatal(err)
	}
	if err := service.Check(ctx); err == nil || !strings.Contains(err.Error(), "describe topic wherry.tenant.tenant-a") {
		t.Fatalf("check = %v; want the missing feed reported", err)
	}
}

// The largest records are what the broker measures: on a real broker, a topic at the broker's defaults carries the
// largest chunks.
func TestLargestRecordsFitTheBrokerOnRealBroker(t *testing.T) {
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" {
		t.Skip("WHERRY_KAFKA_TEST_BROKER not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// Uncompressed, the records' zeros weigh what incompressible outputs would.
	client, err := kgo.NewClient(kgo.SeedBrokers(address), kgo.ProducerBatchCompression(kgo.NoCompression()), kgo.RecordRetries(0))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	feed := PoolEventsTopic(strings.ReplaceAll("limits-"+time.Now().Format("150405.000000"), ".", ""))
	if _, err := kadm.NewClient(client).CreateTopics(ctx, 1, -1, nil, feed); err != nil {
		t.Fatal(err)
	}
	if err := kafkatest.Led(ctx, client, feed); err != nil {
		t.Fatal(err)
	}
	records, err := largestRecords()
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if err := client.ProduceSync(ctx, &kgo.Record{Topic: feed, Key: []byte(largestKey), Value: record.value}).FirstErr(); err != nil {
			t.Fatalf("%s, %d bytes: %v", record.name, len(record.value), err)
		}
		t.Logf("%s: %d-byte batch", record.name, batchSize(record.value))
	}
}

func TestPoolVCS(t *testing.T) {
	terms := modelTerms()
	terms.VCS = []string{"github.com", "git.example.test:2222"}
	encoded, err := EncodeRecord(Record{Kind: "pool", Pool: "pool-a", Terms: &terms, AtMS: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"vcs":["github.com","git.example.test:2222"]`)) {
		t.Fatalf("encoded terms %s do not use the vcs member", encoded)
	}
	record, err := DecodeRecord(encoded)
	if err != nil {
		t.Fatal(err)
	}
	state, err := Fold([]Record{record})
	if err != nil || !slices.Equal(state.Pools["pool-a"].VCS, terms.VCS) {
		t.Fatalf("vcs hosts %v, %v; want the terms' own", state.Pools["pool-a"].VCS, err)
	}
	for _, hosts := range [][]string{{"GitHub.com"}, {"github.com", "github.com"}, {"github.com:0"}, {"https://github.com"}, {""}} {
		terms.VCS = hosts
		if err := validateTerms(terms); err == nil || err.Error() != "vcs hosts must be unique lowercase host names, each optionally with :port" {
			t.Errorf("vcs hosts %q: %v", hosts, err)
		}
		if _, err := EncodeRecord(Record{Kind: "pool", Pool: "pool-a", Terms: &terms, AtMS: 1}); err == nil {
			t.Errorf("vcs hosts %q encoded", hosts)
		}
	}
}

func TestPoolModels(t *testing.T) {
	terms := modelTerms()
	terms.Agents[0].Models, terms.Agents[0].DefaultModel = []string{"haiku", "sonnet"}, "haiku"
	encoded, err := EncodeRecord(Record{Kind: "pool", Pool: "pool-a", Terms: &terms, AtMS: 1})
	if err != nil {
		t.Fatal(err)
	}
	record, err := DecodeRecord(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if agent := record.Terms.Agents[0]; !slices.Equal(agent.Models, []string{"haiku", "sonnet"}) || agent.DefaultModel != "haiku" {
		t.Fatalf("agent terms %+v, want the models and default model encoded", agent)
	}
	required := modelTerms()
	required.Agents[0].Models = []string{"haiku", "sonnet"}
	if err := validateTerms(required); err != nil {
		t.Fatalf("models without a default refused: %v", err)
	}
	for name, edit := range map[string]func(*AgentTerms){
		"default outside the models": func(agent *AgentTerms) { agent.DefaultModel = "opus" },
		"default without models":     func(agent *AgentTerms) { agent.Models = nil },
	} {
		terms := modelTerms()
		terms.Agents[0].Models, terms.Agents[0].DefaultModel = []string{"haiku", "sonnet"}, "haiku"
		edit(&terms.Agents[0])
		if err := validateTerms(terms); err == nil || err.Error() != "an agent's default_model must be one of its models" {
			t.Errorf("%s: %v", name, err)
		}
		if _, err := EncodeRecord(Record{Kind: "pool", Pool: "pool-a", Terms: &terms, AtMS: 1}); err == nil {
			t.Errorf("%s encoded", name)
		}
	}
}

func TestPoolCredentialTargets(t *testing.T) {
	var terms Terms
	if err := yaml.Unmarshal([]byte(`
agents:
  - name: codex
    credentials:
      - {name: openai-api-key, env: CODEX_API_KEY}
      - {name: github-token, env: GH_TOKEN, required: false}
      - {name: deploy-key, file: id_ed25519}
producer_byte_rate: 1000000
request_percentage: 50
`), &terms); err != nil {
		t.Fatal(err)
	}
	if err := validateTerms(withDefaults(terms)); err != nil {
		t.Fatalf("valid credential targets refused: %v", err)
	}
	if credentials := terms.Agents[0].Credentials; credentials[0].Optional() || !credentials[1].Optional() || credentials[2].Optional() || credentials[2].File != "id_ed25519" {
		t.Fatalf("credentials %+v, want only github-token optional and deploy-key at its file", credentials)
	}
	credential := func(name, env, file string) AgentCredential {
		return AgentCredential{TaskCredential: wire.TaskCredential{Name: name, Env: env, File: file}}
	}
	for name, credentials := range map[string][]AgentCredential{
		"a reserved env target":    {credential("model", "HOME", "")},
		"a reserved WHERRY_ env":   {credential("model", "WHERRY_CREDENTIALS_DIR", "")},
		"an invalid env name":      {credential("model", "MODEL-KEY", "")},
		"a duplicate name":         {credential("model", "MODEL_API_KEY", ""), credential("model", "OTHER_KEY", "")},
		"two on one env target":    {credential("model", "MODEL_API_KEY", ""), credential("other", "MODEL_API_KEY", "")},
		"two on one file target":   {credential("model", "", "key"), credential("other", "", "key")},
		"both env and file":        {credential("model", "MODEL_API_KEY", "key")},
		"neither env nor file":     {credential("model", "", "")},
		"a file outside the rules": {credential("model", "", "../key")},
	} {
		terms := modelTerms()
		terms.Agents[0].Credentials = credentials
		if err := validateTerms(terms); err == nil {
			t.Errorf("accepted %s", name)
		}
		if _, err := EncodeRecord(Record{Kind: "pool", Pool: "pool-a", Terms: &terms, AtMS: 1}); err == nil {
			t.Errorf("%s encoded", name)
		}
		if err := (&Service{}).PoolCreate(context.Background(), "pool-a", terms); err == nil || !strings.Contains(err.Error(), "agent agent-a: credential") {
			t.Errorf("pool create with %s: %v; want the credential refused before the broker is read", name, err)
		}
	}
}

func TestPoolStorage(t *testing.T) {
	terms := modelTerms()
	terms.Storage = &Storage{Endpoint: "https://s3.example.test:8443", Bucket: "wherry-lab"}
	encoded, err := EncodeRecord(Record{Kind: "pool", Pool: "pool-a", Terms: &terms, AtMS: 1})
	if err != nil {
		t.Fatal(err)
	}
	record, err := DecodeRecord(encoded)
	if err != nil {
		t.Fatal(err)
	}
	state, err := Fold([]Record{record})
	if err != nil || *state.Pools["pool-a"].Storage != *terms.Storage || state.Pools["pool-a"].Storage.Host() != "s3.example.test:8443" {
		t.Fatalf("storage %+v, %v; want the terms' own", state.Pools["pool-a"].Storage, err)
	}
	for endpoint, host := range map[string]string{
		"https://s3.example.test": "s3.example.test:443", "http://127.0.0.1": "127.0.0.1:80", "http://[::1]": "[::1]:80", "http://[::1]:18080": "[::1]:18080",
	} {
		if got := (Storage{Endpoint: endpoint}).Host(); got != host {
			t.Errorf("host of %s is %s; want %s", endpoint, got, host)
		}
	}
	for _, endpoint := range []string{"https://s3.example.test", "https://127.0.0.1:8443", "http://127.0.0.1:18080", "http://[::1]:18080", "http://127.0.0.1"} {
		terms.Storage = &Storage{Endpoint: endpoint, Bucket: "wherry-lab"}
		if err := validateTerms(terms); err != nil {
			t.Errorf("storage %s: %v", endpoint, err)
		}
	}
	for _, storage := range []Storage{
		{Endpoint: "http://s3.example.test", Bucket: "wherry-lab"},
		{Endpoint: "http://localhost", Bucket: "wherry-lab"},
		{Endpoint: "http://localhost:18080", Bucket: "wherry-lab"},
		{Endpoint: "http://10.0.0.1", Bucket: "wherry-lab"},
		{Endpoint: "http://127.0.0.1.nip.io", Bucket: "wherry-lab"},
		{Endpoint: "http://127.0.0.2", Bucket: "wherry-lab"},
		{Endpoint: "http://127.0.0.1:", Bucket: "wherry-lab"},
		{Endpoint: "http://127.0.0.1:0", Bucket: "wherry-lab"},
		{Endpoint: "http://127.0.0.1/path", Bucket: "wherry-lab"},
		{Endpoint: "http://[::1]/path", Bucket: "wherry-lab"},
		{Endpoint: "http://user@127.0.0.1", Bucket: "wherry-lab"},
		{Endpoint: "http://[::1]:x", Bucket: "wherry-lab"},
		{Endpoint: "http://0177.0.0.1", Bucket: "wherry-lab"},
		{Endpoint: "http://127.1", Bucket: "wherry-lab"},
		{Endpoint: "http://[::ffff:127.0.0.1]", Bucket: "wherry-lab"},
		{Endpoint: "http://[::1%25lo0]", Bucket: "wherry-lab"},
		{Endpoint: "HTTP://127.0.0.1", Bucket: "wherry-lab"},
		{Endpoint: "https://[::1]:18080", Bucket: "wherry-lab"},
		{Endpoint: "https://s3.example.test/path", Bucket: "wherry-lab"},
		{Endpoint: "https://S3.example.test", Bucket: "wherry-lab"},
		{Endpoint: "https://user@s3.example.test", Bucket: "wherry-lab"},
		{Endpoint: "https://s3.example.test:0", Bucket: "wherry-lab"},
		{Endpoint: "https://s3.example.test?x=1", Bucket: "wherry-lab"},
		{Endpoint: "https://s3.example.test", Bucket: "Wherry"},
		{Endpoint: "https://s3.example.test", Bucket: "ab"},
		{Endpoint: "https://s3.example.test", Bucket: "a..b"},
		{Endpoint: "https://s3.example.test", Bucket: "wherry/lab"},
	} {
		terms.Storage = &storage
		if err := validateTerms(terms); err == nil {
			t.Errorf("storage %+v validated", storage)
		}
	}
}

// A pool's limits must carry a result returning every output through S3, which lists each one's key.
func TestLargestResultReturnsS3OutputsAtTheirBounds(t *testing.T) {
	feed, err := largestRecords()
	if err != nil {
		t.Fatal(err)
	}
	index := slices.IndexFunc(feed, func(record feedRecord) bool { return record.name == "a result returning S3 outputs" })
	if index < 0 {
		t.Fatal("no largest result returning S3 outputs")
	}
	payload, err := base64.RawURLEncoding.DecodeString(string(bytes.Split(feed[index].value, []byte("."))[1]))
	if err != nil {
		t.Fatal(err)
	}
	var result wire.Result
	if err := json.Unmarshal(payload, &result); err != nil {
		t.Fatal(err)
	}
	var metadata wire.ResultMetadata
	if err := json.Unmarshal(result.Task.Metadata, &metadata); err != nil {
		t.Fatal(err)
	}
	if len(metadata.S3Outputs) != wire.MaxOutputs || len(metadata.S3Outputs[0].Name) != 512 ||
		metadata.S3Outputs[0].Key != wire.S3OutputKey(strings.Repeat("n", 32), result.Wherry.Run, metadata.S3Outputs[0].Name) {
		t.Fatalf("largest result lists %d S3 outputs; want %d at their bounds", len(metadata.S3Outputs), wire.MaxOutputs)
	}
}

func TestSingleAnswerTermsDefault(t *testing.T) {
	terms := Terms{Agents: []AgentTerms{{Name: "agent-a", Models: []string{"m1"}}}, ProducerByteRate: 1000000, RequestPercentage: 50}
	encoded, err := EncodeRecord(Record{Kind: "pool", Pool: "pool-a", Terms: &terms, AtMS: 1})
	if err != nil {
		t.Fatal(err)
	}
	record, err := DecodeRecord(encoded)
	if err != nil {
		t.Fatalf("a pool record leaving out single-answer terms: %v", err)
	}
	state, err := Fold([]Record{record})
	if err != nil {
		t.Fatal(err)
	}
	got := state.Pools["pool-a"]
	if got.DefaultAgent != "agent-a" || got.Agents[0].DefaultModel != "m1" || got.DeliveryLimit != DefaultDeliveryLimit || got.LockDurationMS != DefaultLockDurationMS {
		t.Fatalf("terms %#v, want the one agent and model, delivery limit 5 and a 15s lock", got)
	}
	if terms.Agents[0].DefaultModel != "" {
		t.Fatal("folding wrote the default into the record's terms")
	}
	two := terms
	two.Agents = []AgentTerms{{Name: "agent-a"}, {Name: "agent-b"}}
	if _, err := EncodeRecord(Record{Kind: "pool", Pool: "pool-a", Terms: &two, AtMS: 1}); err == nil {
		t.Fatal("two agents without a default agent encoded")
	}
}

// wherry.receivers is created compacted, and one created before converges on the next admin write.
func TestProgressTopicIsCompactedOnNewAndExistingTopics(t *testing.T) {
	cluster, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	topic := ReceiverProgressTopic("receiver-a")
	var mu sync.Mutex
	created := map[string]string{}
	cluster.ControlKey(int16(kmsg.CreateTopics), func(request kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		mu.Lock()
		defer mu.Unlock()
		for _, requested := range request.(*kmsg.CreateTopicsRequest).Topics {
			if requested.Topic == topic {
				for _, config := range requested.Configs {
					created[config.Name] = *config.Value
				}
			}
		}
		return nil, nil, false
	})
	if err := service.PoolCreate(ctx, "pool-a", modelTerms()); err != nil {
		t.Fatal(err)
	}
	if err := service.ReceiverAdd(ctx, modelConfig("receiver-a", "pool-a"), filepath.Join(t.TempDir(), "bundle.yaml")); err != nil {
		t.Fatal(err)
	}
	retention, err := poolRetention(withDefaults(modelTerms()))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"cleanup.policy": "compact,delete", "message.timestamp.type": "LogAppendTime", "retention.ms": strconv.FormatInt(retention.tasks, 10), "segment.ms": "3600000", "max.compaction.lag.ms": "3600000"}
	mu.Lock()
	if !reflect.DeepEqual(created, want) {
		t.Errorf("created %s with %v, want %v", topic, created, want)
	}
	mu.Unlock()
	configs := func() map[string]string {
		t.Helper()
		got := map[string]string{}
		for name := range want {
			values, err := service.topicConfigs(ctx, name, topic)
			if err != nil {
				t.Fatal(err)
			}
			got[name] = values[topic]
		}
		return got
	}
	if got := configs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("new %s configs %v, want %v", topic, got, want)
	}
	if err := service.Check(ctx); err != nil {
		t.Fatalf("check of a new progress topic: %v", err)
	}
	setTopicConfig(t, service, topic, "cleanup.policy", "delete")
	setTopicConfig(t, service, topic, "segment.ms", "604800000")
	setTopicRetention(t, service, topic, 60000)
	if err := service.Check(ctx); err == nil || !strings.Contains(err.Error(), topic) {
		t.Fatalf("check of a progress topic below the pool's retention: %v", err)
	}
	if err := service.PoolUpdate(ctx, "pool-a", modelTerms(), false); err != nil {
		t.Fatal(err)
	}
	if got := configs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("existing %s configs %v, want %v", topic, got, want)
	}
}

func TestReceiversTopicIsCompactedOnNewAndExistingTopics(t *testing.T) {
	cluster, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	var mu sync.Mutex
	created := map[string]string{}
	cluster.ControlKey(int16(kmsg.CreateTopics), func(request kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		mu.Lock()
		defer mu.Unlock()
		for _, topic := range request.(*kmsg.CreateTopicsRequest).Topics {
			if topic.Topic == ReceiversTopic {
				for _, config := range topic.Configs {
					created[config.Name] = *config.Value
				}
			}
		}
		return nil, nil, false
	})
	if err := service.PoolCreate(ctx, "pool-a", modelTerms()); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"cleanup.policy": "compact,delete", "message.timestamp.type": "LogAppendTime", "retention.ms": "604800000", "segment.ms": "3600000", "max.compaction.lag.ms": "3600000"}
	mu.Lock()
	if !reflect.DeepEqual(created, want) {
		t.Errorf("created %s with %v, want %v", ReceiversTopic, created, want)
	}
	mu.Unlock()
	configs := func() map[string]string {
		t.Helper()
		got := map[string]string{}
		for name := range want {
			values, err := service.topicConfigs(ctx, name, ReceiversTopic)
			if err != nil {
				t.Fatal(err)
			}
			got[name] = values[ReceiversTopic]
		}
		return got
	}
	if got := configs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("new %s configs %v, want %v", ReceiversTopic, got, want)
	}
	setTopicConfig(t, service, ReceiversTopic, "cleanup.policy", "delete")
	setTopicConfig(t, service, ReceiversTopic, "segment.ms", "604800000")
	if err := service.PoolCreate(ctx, "pool-b", modelTerms()); err != nil {
		t.Fatal(err)
	}
	if got := configs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("existing %s configs %v, want %v", ReceiversTopic, got, want)
	}
}

// A change to any record's shape or bounds changes its signed size, which checkLimits holds as a constant.
func TestFeedRecordsArePinned(t *testing.T) {
	records, err := largestRecords()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != len(feedRecords) {
		t.Fatalf("%d largest records, %d pinned", len(records), len(feedRecords))
	}
	for i, record := range records {
		if pinned := feedRecords[i]; pinned.name != record.name || pinned.batch != batchSize(record.value) {
			t.Errorf("%s is a %d-byte batch; pinned %+v", record.name, batchSize(record.value), pinned)
		}
	}
}

// largestKey is the longest run ID, which keys submissions and output chunks.
var largestKey = strings.Repeat("r", 63)

// batchSize is the size of a record batch holding value alone under largestKey: a v2 batch's 61-byte header and at
// most 36 bytes of one record's varint framing.
func batchSize(value []byte) int64 {
	return int64(61 + 36 + len(largestKey) + len(value))
}

// feedRecord is a receiver record at its largest, as a refusal names it.
type feedRecord struct {
	name  string
	value []byte
}

// largestRecords signs, with a throwaway key, the largest bounded records a receiver writes to a feed. Fields with no
// bound are left empty.
func largestRecords() ([]feedRecord, error) {
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, err
	}
	name, run, hash := strings.Repeat("n", 32), largestKey, strings.Repeat("0", 64)
	pointers := make([]wire.Part, wire.MaxParts)
	pointer, _ := json.Marshal(wire.PointerMetadata{SHA256: hash, Size: math.MaxInt64}) // a string and an integer always encode
	for i := range pointers {
		pointers[i] = wire.Part{URL: "unstored:input", Filename: fmt.Sprintf("%0255d", i), Metadata: pointer}
	}
	message := wire.Message{MessageID: "msg-" + strings.Repeat("0", 32), ContextID: run, TaskID: run, Role: wire.RoleUser, Extensions: []string{wire.ExtensionURI}}
	binding := wire.ReceiverBinding{Receiver: name, Tenant: name, Pool: name, Run: run, Attempt: math.MaxInt64}
	outputName := strings.Repeat("a/", 255) + "aa"
	// A result lists at most 256 collected entries and the expected outputs missing among them. An S3 output is an
	// expected one, listed with its key, so a result returning 256 of them misses none.
	// A reply over wire.MaxText adds its entry to either result.
	reply := &wire.ReturnedReply{SHA256: hash, Size: math.MaxInt64, Cut: true}
	metadata, s3Metadata := wire.ResultMetadata{ReceiverID: name, Pool: name, Profile: name, Reply: reply}, wire.ResultMetadata{ReceiverID: name, Pool: name, Profile: name, Reply: reply}
	for range wire.MaxOutputs {
		metadata.Outputs = append(metadata.Outputs, wire.ReturnedOutput{Name: outputName, SHA256: hash, Size: math.MaxInt64})
		metadata.NotReturned = append(metadata.NotReturned, wire.UnreturnedOutput{Name: outputName, Size: math.MaxInt64, Reason: "rejected: size changed while read"})
		s3Metadata.S3Outputs = append(s3Metadata.S3Outputs, wire.ReturnedOutput{Name: outputName, SHA256: hash, Size: math.MaxInt64, Key: wire.S3OutputKey(name, run, outputName)})
	}
	encodedMetadata, err := json.Marshal(metadata)
	if err != nil {
		return nil, err
	}
	encodedS3Metadata, err := json.Marshal(s3Metadata)
	if err != nil {
		return nil, err
	}
	history := message
	history.Parts = pointers
	result := func(metadata []byte) *wire.Result {
		return &wire.Result{
			Task: wire.Task{
				ID: run, ContextID: run, History: []wire.Message{history}, Metadata: metadata,
				Status: wire.TaskStatus{State: wire.TaskStateFailed, Timestamp: "2006-01-02T15:04:05.000Z", Message: &wire.Message{
					MessageID: "msg-" + strings.Repeat("0", 32), Role: wire.RoleAgent, Extensions: []string{wire.ExtensionURI}, Parts: []wire.Part{
						{Raw: make(wire.StdBytes, wire.MaxText), MediaType: "text/plain"}, {Data: []byte(`{"credential":"` + name + `"}`), MediaType: "application/json"},
					},
				}},
				Artifacts: []wire.Artifact{{ArtifactID: "result", Parts: []wire.Part{{Raw: make(wire.StdBytes, wire.MaxText), MediaType: "text/plain"}}, Extensions: []string{wire.ExtensionURI}}},
			},
			Wherry: wire.ResultBinding{ReceiverBinding: binding, TaskHash: hash},
		}
	}
	records := []struct {
		name  string
		value wire.Record
	}{
		{"an output chunk", &wire.Output{
			ArtifactUpdate: wire.TaskArtifactUpdateEvent{
				TaskID: run, ContextID: run, Append: true, LastChunk: true, Metadata: []byte(`{"index":9223372036854775807}`),
				Artifact: wire.Artifact{ArtifactID: outputName, Name: outputName, Parts: []wire.Part{{Raw: make(wire.StdBytes, wire.OutputChunk), Filename: "aa", MediaType: wire.OutputMediaType}}, Extensions: []string{wire.ExtensionURI}},
			},
			Wherry: wire.ResultBinding{ReceiverBinding: binding, TaskHash: hash},
		}},
		// Receiver texts are names and fixed words, which JSON does not escape; an agent's text that it would travels raw.
		{"a result", result(encodedMetadata)},
		{"a result returning S3 outputs", result(encodedS3Metadata)},
		{"a transcript chunk", &wire.Chunk{
			ArtifactUpdate: wire.TaskArtifactUpdateEvent{
				TaskID: run, ContextID: run, Append: true, LastChunk: true,
				Artifact: wire.Artifact{ArtifactID: "transcript", Parts: []wire.Part{{Raw: make(wire.StdBytes, wire.MaxLine), MediaType: "application/x-ndjson"}}, Extensions: []string{wire.ExtensionURI}},
			},
			Wherry: wire.ChunkBinding{ReceiverBinding: binding, TaskHash: hash, Seq: math.MaxInt64},
		}},
	}
	feed := make([]feedRecord, len(records))
	for i, record := range records {
		value, err := wire.SignRecord(record.value, key)
		if err != nil {
			return nil, fmt.Errorf("sign %s: %w", record.name, err)
		}
		feed[i] = feedRecord{record.name, value}
	}
	return feed, nil
}

// A broker a tenant's new feeds have not reached describes them UNKNOWN_TOPIC_OR_PARTITION; TenantAdd waits up to
// topicWait for every broker's metadata to carry them, and past it reports the describe's failure.
func TestTenantAddWaitsForItsFeedsToReachEveryBroker(t *testing.T) {
	for _, test := range []struct {
		name   string
		misses int32
		want   string
	}{
		{"reached after one miss", 1, ""},
		{"never reached", -1, "describe topic wherry.tenant.tenant-a: UNKNOWN_TOPIC_OR_PARTITION"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cluster, service, closeFixture := kfakeService(t)
			defer closeFixture()
			ctx, cancel := context.WithTimeout(context.Background(), 2*topicWait)
			defer cancel()
			if err := service.PoolCreate(ctx, "pool-a", modelTerms()); err != nil {
				t.Fatal(err)
			}
			feed := func(topic string) bool {
				return topic == TenantTopic("tenant-a") || topic == TenantTranscriptTopic("tenant-a")
			}
			var misses atomic.Int32
			var reached atomic.Bool
			cluster.ControlKey(int16(kmsg.Metadata), func(request kmsg.Request) (kmsg.Response, error, bool) {
				cluster.KeepControl()
				metadata := request.(*kmsg.MetadataRequest)
				if !slices.ContainsFunc(metadata.Topics, func(topic kmsg.MetadataRequestTopic) bool { return topic.Topic != nil && feed(*topic.Topic) }) {
					return nil, nil, false
				}
				if test.misses >= 0 && misses.Add(1) > test.misses {
					reached.Store(true)
					return nil, nil, false
				}
				response := metadata.ResponseKind().(*kmsg.MetadataResponse)
				for _, topic := range metadata.Topics {
					missing := kmsg.NewMetadataResponseTopic()
					missing.Topic = topic.Topic
					missing.ErrorCode = kerr.UnknownTopicOrPartition.Code
					response.Topics = append(response.Topics, missing)
				}
				return response, nil, true
			})
			cluster.ControlKey(int16(kmsg.DescribeConfigs), func(request kmsg.Request) (kmsg.Response, error, bool) {
				cluster.KeepControl()
				describe := request.(*kmsg.DescribeConfigsRequest)
				if reached.Load() || !slices.ContainsFunc(describe.Resources, func(resource kmsg.DescribeConfigsRequestResource) bool { return feed(resource.ResourceName) }) {
					return nil, nil, false
				}
				response := describe.ResponseKind().(*kmsg.DescribeConfigsResponse)
				for _, resource := range describe.Resources {
					described := kmsg.NewDescribeConfigsResponseResource()
					described.ResourceType = resource.ResourceType
					described.ResourceName = resource.ResourceName
					if feed(resource.ResourceName) {
						described.ErrorCode = kerr.UnknownTopicOrPartition.Code
					}
					response.Resources = append(response.Resources, described)
				}
				return response, nil, true
			})
			start := time.Now()
			err := service.TenantAdd(ctx, "tenant-a", "pool-a")
			if test.want == "" && err != nil || test.want != "" && (err == nil || !strings.HasPrefix(err.Error(), test.want) || time.Since(start) < topicWait) {
				t.Fatalf("tenant add = %v after %v; want %q", err, time.Since(start), test.want)
			}
		})
	}
}
