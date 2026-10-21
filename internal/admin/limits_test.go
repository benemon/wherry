package admin

import (
	"bytes"
	"context"
	"encoding/base64"
	json "encoding/json/v2"
	"errors"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// A pool record written before the limits existed, as EncodeRecord writes modelTerms.
const poolRecordWithoutLimits = `{"v":1,"kind":"pool","pool":"pool-a","terms":{"agents":[{"name":"agent-a","credentials":["model"]}],"default_agent":"agent-a","seal_lifetime_ms":60000,"delivery_limit":3,"lock_duration_ms":15000,"producer_byte_rate":1000000,"request_percentage":50},"at_ms":1}`

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
	if got := state.Pools["pool-a"]; got.InlineLimit != 256<<10 || got.OutputLimit != 8<<20 || got.RunLifetimeMS != DefaultRunLifetimeMS {
		t.Fatalf("defaults %#v, want 256 KiB, 8 MiB and a 14-day run lifetime", got)
	}
	terms.InlineLimit, terms.OutputLimit = 1000, 2000
	state, err = Fold([]Record{{Version: 1, Kind: "pool", Pool: "pool-a", Terms: &terms, AtMS: 1}})
	if got := state.Pools["pool-a"]; err != nil || got.InlineLimit != 1000 || got.OutputLimit != 2000 {
		t.Fatalf("limits %d and %d, %v; want the terms' own", got.InlineLimit, got.OutputLimit, err)
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
	want := int64(1299600000)
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

func TestPoolCreateRefusesRunLifetimePastFeedRetention(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	terms := modelTerms()
	terms.RunLifetimeMS = 30 * 24 * 60 * 60 * 1000
	err := service.PoolCreate(context.Background(), "pool-a", terms)
	if err == nil || err.Error() != "run lifetime requires tenant feed retention.ms of at least 2595600000" {
		t.Fatalf("pool create = %v", err)
	}
}

func TestPoolCreateRefusesRunLifetimePastAnEntitledFeed(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	if err := service.PoolCreate(ctx, "pool-a", modelTerms()); err != nil {
		t.Fatal(err)
	}
	if err := service.TenantAdd(ctx, "tenant-a", "pool-a"); err != nil {
		t.Fatal(err)
	}
	setTopicRetention(t, service, TenantTopic("tenant-a"), 1213199999)
	err := service.PoolCreate(ctx, "pool-a", modelTerms())
	if err == nil || err.Error() != "topic wherry.tenant.tenant-a retention.ms 1213199999 is below required 1213200000" {
		t.Fatalf("pool create = %v", err)
	}
}

func TestTenantAddRefusesAFeedBelowThePoolsRunLifetime(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	if err := service.PoolCreate(ctx, "pool-a", modelTerms()); err != nil {
		t.Fatal(err)
	}
	if err := service.createTopics(ctx, "1213199999", TenantTopic("tenant-a")); err != nil {
		t.Fatal(err)
	}
	err := service.TenantAdd(ctx, "tenant-a", "pool-a")
	if err == nil || err.Error() != "topic wherry.tenant.tenant-a retention.ms 1213199999 is below required 1213200000" {
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
		{"tasks below lifetime plus deadline and margin", func(t *testing.T, service *Service) {
			setTopicRetention(t, service, PoolTasksTopic("pool-a"), 1299599999)
		}, "wherry.pool.pool-a.tasks"},
		{"events below tasks", func(t *testing.T, service *Service) {
			setTopicRetention(t, service, PoolTasksTopic("pool-a"), 1300000000)
		}, "wherry.pool.pool-a.events"},
		{"feed below lifetime plus deadline", func(t *testing.T, service *Service) {
			setTopicRetention(t, service, TenantTopic("tenant-a"), 1213199999)
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

// kfake's brokers accept 1048588-byte batches, so franz-go's 1000012-byte producer maximum is the limit until a
// topic sets a lower one.
func TestPoolCreateRefusesLimitsTheBrokerCannotCarry(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	terms := modelTerms()
	terms.InlineLimit = math.MaxInt64
	if err := service.PoolCreate(ctx, "pool-a", terms); err == nil || err.Error() != "inline limit 9223372036854775807 bytes is over the 1000012-byte record limit" {
		t.Fatalf("pool create = %v; want an inline limit no record carries refused before it is measured", err)
	}
	terms.InlineLimit = 520 << 10
	err := service.PoolCreate(ctx, "pool-a", terms)
	if err == nil || !strings.Contains(err.Error(), "inline limit 532480 bytes makes a ") || !strings.Contains(err.Error(), "-byte submission record, over the 1000012-byte record limit") {
		t.Fatalf("pool create = %v; want the submission refused against the producer maximum", err)
	}
	if state, err := service.LoadState(ctx); err != nil || len(state.Pools) != 0 {
		t.Fatalf("pools %v, %v; want none written", state.Pools, err)
	}
	terms.InlineLimit = 480 << 10
	if err := service.PoolCreate(ctx, "pool-a", terms); err != nil {
		t.Fatalf("480 KiB inline limit: %v", err)
	}
	setTopicLimit(t, service, PoolTasksTopic("pool-a"), "600000")
	if err := service.PoolCreate(ctx, "pool-a", terms); err == nil || !strings.Contains(err.Error(), "over the 600000-byte record limit") {
		t.Fatalf("pool create = %v; want the task topic's limit", err)
	}
	terms.InlineLimit = 1 << 10
	if err := service.PoolCreate(ctx, "pool-a", terms); err != nil {
		t.Fatal(err)
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
	// Terms that leave the limits out are recorded without them, so a release that predates them still reads the pool.
	records, err := ReadTopic(ctx, service.Operator, AdminTopic)
	if err != nil || len(records) != 1 || strings.Contains(string(records[0].Value), "inline_limit") || strings.Contains(string(records[0].Value), "output_limit") {
		t.Fatalf("pool record %v, %v; want one without limits", records, err)
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

// The largest records are what the broker measures: on a real broker with a topic limit, the largest submission
// at the highest inline limit pool create accepts is produced, and one 1 KiB over is refused by the broker; a topic
// at the broker's defaults carries the largest chunks.
func TestLargestRecordsFitTheBrokerOnRealBroker(t *testing.T) {
	address := os.Getenv("WHERRY_KAFKA_TEST_BROKER")
	if address == "" {
		t.Skip("WHERRY_KAFKA_TEST_BROKER not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// Uncompressed, the records' zeros weigh what incompressible inputs would.
	client, err := kgo.NewClient(kgo.SeedBrokers(address), kgo.ProducerBatchCompression(kgo.NoCompression()), kgo.RecordRetries(0))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	service := &Service{Admin: client}
	pool := strings.ReplaceAll("limits-"+time.Now().Format("150405.000000"), ".", "")
	limit := "600000"
	if _, err := kadm.NewClient(client).CreateTopics(ctx, 1, -1, map[string]*string{"max.message.bytes": &limit}, PoolTasksTopic(pool)); err != nil {
		t.Fatal(err)
	}
	accepted := int64(0)
	for low, high := int64(0), int64(600000); low <= high; {
		middle := (low + high) / 2
		terms := modelTerms()
		terms.InlineLimit = middle
		if err := service.checkLimits(ctx, NewState(), pool, terms); err == nil {
			accepted, low = middle, middle+1
		} else {
			high = middle - 1
		}
	}
	if accepted < 200000 {
		t.Fatalf("highest accepted inline limit %d", accepted)
	}
	for _, test := range []struct {
		inline int64
		fits   bool
	}{{accepted, true}, {accepted + 1<<10, false}} {
		submission, _, err := largestRecords(test.inline)
		if err != nil {
			t.Fatal(err)
		}
		err = client.ProduceSync(ctx, &kgo.Record{Topic: PoolTasksTopic(pool), Key: []byte(largestKey), Value: submission}).FirstErr()
		if test.fits && err != nil {
			t.Fatalf("inline %d (%d-byte value): %v", test.inline, len(submission), err)
		}
		if !test.fits && !errors.Is(err, kerr.MessageTooLarge) {
			t.Fatalf("inline %d (%d-byte value) = %v; want the broker to refuse it", test.inline, len(submission), err)
		}
	}
	// A feed at the broker's defaults carries the largest transcript and output chunks.
	feed := PoolEventsTopic(pool)
	if _, err := kadm.NewClient(client).CreateTopics(ctx, 1, -1, nil, feed); err != nil {
		t.Fatal(err)
	}
	_, records, err := largestRecords(0)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if err := client.ProduceSync(ctx, &kgo.Record{Topic: feed, Key: []byte(largestKey), Value: record.value}).FirstErr(); err != nil {
			t.Fatalf("%s, %d bytes: %v", record.name, len(record.value), err)
		}
		t.Logf("%s: %d-byte batch", record.name, batchSize(record.value))
	}
	t.Logf("topic limit %s: highest inline limit %d", limit, accepted)
}

// The largest submission must hold a git input at every bound, or a pool could accept terms whose git submissions the
// broker refuses.
func TestLargestSubmissionHoldsTheLargestGitInput(t *testing.T) {
	submission, _, err := largestRecords(0)
	if err != nil {
		t.Fatal(err)
	}
	segments := bytes.Split(submission, []byte("."))
	if len(segments) != 3 {
		t.Fatalf("submission is not a compact JWS")
	}
	payload, err := base64.RawURLEncoding.DecodeString(string(segments[1]))
	if err != nil {
		t.Fatal(err)
	}
	var decoded wire.Submission
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	git := decoded.Wherry.Git
	if git == nil || decoded.Wherry.Turn != 1 || len(git.Remote) != wire.MaxGitRemote || len(git.Ref) != wire.MaxGitRef || len(git.Path) != 512 ||
		len(git.HostKey) != wire.MaxGitHostKey || strings.Count(git.HostKey, `"`) != wire.MaxGitHostKey-len("h k ") || len(git.Credential) != 32 {
		t.Fatalf("largest submission's git input %+v at turn %d; want every field at its bound on a first turn", git, decoded.Wherry.Turn)
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
	_, feed, err := largestRecords(0)
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
		metadata.S3Outputs[0].Key != wire.S3OutputKey(strings.Repeat("n", 32), result.Wherry.Run, math.MaxInt64, metadata.S3Outputs[0].Name) {
		t.Fatalf("largest result lists %d S3 outputs; want %d at their bounds", len(metadata.S3Outputs), wire.MaxOutputs)
	}
}
