package admin

import (
	"context"
	"errors"
	"math"
	"os"
	"strings"
	"testing"
	"time"

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
	if got := state.Pools["pool-a"]; got.InlineLimit != 256<<10 || got.OutputLimit != 8<<20 {
		t.Fatalf("limits %d and %d, want 256 KiB and 8 MiB", got.InlineLimit, got.OutputLimit)
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
	if err := service.createTopics(ctx, tenantRetentionMS, TenantTopic("tenant-a")); err != nil {
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
