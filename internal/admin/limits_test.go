package admin

import (
	"bytes"
	"context"
	"encoding/base64"
	json "encoding/json/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// A pool record written before the limits existed, as EncodeRecord writes modelTerms.
const poolRecordWithoutLimits = `{"v":1,"kind":"pool","pool":"pool-a","terms":{"agents":[{"name":"agent-a","credentials":["model"]}],"default_agent":"agent-a","delivery_limit":3,"lock_duration_ms":15000,"producer_byte_rate":1000000,"request_percentage":50},"at_ms":1}`

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
	if err := service.PoolCreate(ctx, "pool-a", modelTerms()); err != nil {
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

func TestPoolCreateRefusesMaxQueueDeadlinePastAnEntitledFeed(t *testing.T) {
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
	err := service.PoolCreate(ctx, "pool-a", modelTerms())
	if err == nil || err.Error() != "topic wherry.tenant.tenant-a retention.ms 1231199999 is below required 1231200000" {
		t.Fatalf("pool create = %v", err)
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
	recorded := func() int64 {
		t.Helper()
		if err := service.PoolCreate(ctx, "pool-a", terms); err != nil {
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
	if err := service.PoolCreate(ctx, "pool-a", terms); err == nil || !strings.Contains(err.Error(), "a transcript chunk record is ") || !strings.Contains(err.Error(), "over the 900000-byte record limit") {
		t.Fatalf("pool create = %v; want the transcript chunk refused against the tenant feed's limit", err)
	}
	setTopicLimit(t, service, TenantTopic("tenant-a"), "700000")
	if err := service.PoolCreate(ctx, "pool-a", terms); err == nil || !strings.Contains(err.Error(), "a result record is ") || !strings.Contains(err.Error(), "over the 700000-byte record limit") {
		t.Fatalf("pool create = %v; want the result refused against the tenant feed's limit", err)
	}
	setTopicLimit(t, service, TenantTopic("tenant-a"), "400000")
	if err := service.PoolCreate(ctx, "pool-a", terms); err == nil || !strings.Contains(err.Error(), "an output chunk record is ") || !strings.Contains(err.Error(), "over the 400000-byte record limit") {
		t.Fatalf("pool create = %v; want the output chunk refused against the tenant feed's limit", err)
	}
}

func TestCheckReportsLimitsTheBrokerNoLongerCarries(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	if err := service.PoolCreate(ctx, "pool-a", modelTerms()); err != nil {
		t.Fatal(err)
	}
	// Terms that leave the output limit out are recorded without it, so a release that predates it still reads the pool.
	records, err := ReadTopic(ctx, service.Operator, AdminTopic)
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
	if host := (Storage{Endpoint: "https://s3.example.test"}).Host(); host != "s3.example.test:443" {
		t.Fatalf("host %s; want the https default port", host)
	}
	for _, storage := range []Storage{
		{Endpoint: "http://s3.example.test", Bucket: "wherry-lab"},
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
