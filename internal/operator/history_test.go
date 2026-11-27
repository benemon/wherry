package operator

import (
	"context"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// operatorRecords appends records to wherry.operator, each stamped with its time, as kfake keeps a producer's
// timestamps, so the history folds with the append times given.
func operatorRecords(t *testing.T, client *kgo.Client, records ...timedRecord) {
	t.Helper()
	for _, record := range records {
		encoded, err := EncodeRecord(record.Record)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.ProduceSync(context.Background(), &kgo.Record{Topic: OperatorTopic, Value: encoded, Timestamp: record.at}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

// historyFixture is modelState's deployment with receiver-b moved to pool-a, then, at ended, receiver-b's membership,
// host-b and tenant-b's entitlement to pool-a ended, and pool-a's terms changed at retermed.
func historyFixture(t *testing.T, client *kgo.Client, base, ended, retermed time.Time) History {
	t.Helper()
	terms := modelTerms()
	changed := modelTerms()
	changed.Storage = &Storage{Endpoint: "https://s3.example.com", Bucket: "bucket-b"}
	configA, configB := modelConfig("receiver-a", "pool-a"), modelConfig("receiver-b", "pool-a")
	at := func(record Record, when time.Time) timedRecord {
		record.Version, record.AtMS = RecordVersion, when.UnixMilli()
		return timedRecord{record, when}
	}
	operatorRecords(t, client,
		at(Record{Kind: "tenant", Tenant: "tenant-a"}, base),
		at(Record{Kind: "tenant", Tenant: "tenant-b"}, base),
		at(Record{Kind: "pool", Pool: "pool-a", Terms: &terms}, base),
		at(Record{Kind: "host", Enrolment: "00000000000000000000000000000000", Tenant: "tenant-a", Host: "host-a"}, base),
		at(Record{Kind: "host", Enrolment: "00000000000000000000000000000000", Tenant: "tenant-b", Host: "host-b"}, base),
		at(Record{Kind: "entitlement", Tenant: "tenant-a", Pool: "pool-a"}, base),
		at(Record{Kind: "entitlement", Tenant: "tenant-b", Pool: "pool-a"}, base),
		at(Record{Kind: "receiver", Pool: "pool-a", Receiver: "receiver-a", Config: &configA}, base),
		at(Record{Kind: "receiver", Pool: "pool-a", Receiver: "receiver-b", Config: &configB}, base),
		at(Record{Kind: "end", Receiver: "receiver-b"}, ended),
		at(Record{Kind: "end", Tenant: "tenant-b", Host: "host-b"}, ended),
		at(Record{Kind: "end", Tenant: "tenant-b", Pool: "pool-a"}, ended),
		at(Record{Kind: "pool", Pool: "pool-a", Terms: &changed}, retermed),
	)
	reader := NewReader(client)
	defer reader.Close()
	history, err := reader.ReadHistory(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return history
}

func TestHistoryKeepsTermsAndKeyTopics(t *testing.T) {
	client := readerCluster(t, OperatorTopic)
	base := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	ended, retermed := base.Add(20*time.Minute), base.Add(40*time.Minute)
	history := historyFixture(t, client, base, ended, retermed)
	// The ends are folded into the state as Load folds them; no end time is kept.
	if !history.State.Ended[ReceiverPrincipal("receiver-b")] || !history.State.Ended[TenantPrincipal("tenant-b", "host-b")] || history.State.Entitlements["tenant-b"]["pool-a"] || !history.State.EndedEntitlements["tenant-b"]["pool-a"] {
		t.Fatalf("the ends are not in the state: %+v", history.State)
	}
	if terms, ok := history.TermsAt("pool-a", retermed.Add(-time.Second)); !ok || terms.Storage != nil {
		t.Errorf("terms before the change: %v %+v", ok, terms.Storage)
	}
	if terms, ok := history.TermsAt("pool-a", retermed); !ok || terms.Storage == nil || terms.Storage.Bucket != "bucket-b" {
		t.Errorf("terms at the change: %v %+v", ok, terms.Storage)
	}
	if _, ok := history.TermsAt("pool-a", base.Add(-time.Second)); ok {
		t.Error("terms before the first record")
	}
	// Extending with nothing keeps the fold and moves the cursor to the end given.
	if err := history.Extend(nil, history.Next+3); err != nil || history.Next != 16 || len(history.Terms["pool-a"]) != 2 {
		t.Fatalf("extend: %v, next %d, terms %d", err, history.Next, len(history.Terms["pool-a"]))
	}
	if got := AllKeyTopics(history.State); !maps.Equal(got, map[string]string{
		"receiver-a": ReceiverTopic("receiver-a"), "receiver-b": ReceiverTopic("receiver-b"),
		signed.HostOwner("tenant-a", "host-a"): HostTopic("tenant-a", "host-a"), signed.HostOwner("tenant-b", "host-b"): HostTopic("tenant-b", "host-b"),
	}) {
		t.Fatalf("AllKeyTopics names %v; want every receiver and host, ended or not", got)
	}
	if live := PoolKeyTopics(history.State, "pool-a"); len(live) != 1 {
		t.Fatalf("PoolKeyTopics names %v; want the live member alone", live)
	}
}

// A verified record is held to the topic it was read from: a status, start failure or result naming another pool than
// the events topic or another tenant than the feed, an echo on another tenant's feed and a cancel naming another pool
// or tenant than its run are refused. Who signed it, and whether they were a member then, is not asked here.
func TestRecordTopicConsistency(t *testing.T) {
	status := func(tenant, pool string) wire.RunStatus { return *testStatus("receiver-a", tenant, pool, "run-a") }
	binding := func(tenant, pool string) wire.ReceiverBinding {
		return wire.ReceiverBinding{Receiver: "receiver-a", Tenant: tenant, Pool: pool, Run: "run-a", Attempt: 1}
	}
	for _, c := range []struct {
		name string
		err  error
		ok   bool
	}{
		{"status on its pool's events", CheckStatus(status("tenant-a", "pool-a"), "pool-a", ""), true},
		{"status on its tenant's feed", CheckStatus(status("tenant-a", "pool-a"), "", "tenant-a"), true},
		{"status naming another pool than its topic", CheckStatus(status("tenant-a", "pool-b"), "pool-a", ""), false},
		{"status naming another tenant than its feed", CheckStatus(status("tenant-a", "pool-a"), "", "tenant-b"), false},
		{"status of an ended receiver", CheckStatus(*testStatus("receiver-b", "tenant-a", "pool-a", "run-a"), "pool-a", ""), true},
		{"start failure on its pool's events", CheckStartFailure(wire.StartFailure{Wherry: binding("tenant-a", "pool-a")}, "pool-a"), true},
		{"start failure naming another pool", CheckStartFailure(wire.StartFailure{Wherry: binding("tenant-a", "pool-b")}, "pool-a"), false},
		{"result on its tenant's feed", CheckResult(wire.Result{Wherry: wire.ResultBinding{ReceiverBinding: binding("tenant-a", "pool-a")}}, "tenant-a"), true},
		{"result naming another tenant", CheckResult(wire.Result{Wherry: wire.ResultBinding{ReceiverBinding: binding("tenant-b", "pool-a")}}, "tenant-a"), false},
		{"echo on its tenant's feed", CheckEcho(wire.Echo{Tenant: "tenant-a", Pool: "pool-a"}, "tenant-a"), true},
		{"echo on another tenant's feed", CheckEcho(wire.Echo{Tenant: "tenant-a", Pool: "pool-a"}, "tenant-b"), false},
		{"cancel of its run", CheckCancel(wire.Cancel{Tenant: "tenant-a", Host: "host-a", Pool: "pool-a", Run: "run-a"}, "pool-a", "tenant-a"), true},
		{"cancel naming another pool", CheckCancel(wire.Cancel{Tenant: "tenant-a", Host: "host-a", Pool: "pool-b", Run: "run-a"}, "pool-a", "tenant-a"), false},
		{"cancel naming another tenant than its run", CheckCancel(wire.Cancel{Tenant: "tenant-a", Host: "host-a", Pool: "pool-a", Run: "run-a"}, "pool-a", "tenant-b"), false},
	} {
		if (c.err == nil) != c.ok {
			t.Errorf("%s: %v, want accepted %v", c.name, c.err, c.ok)
		}
	}
}

// ReadRanges keeps at most the bounds over every topic of a call and tells each topic where to resume: just past its
// last record kept when the bounds cut it short, the last stable offset it listed when it read to it.
func TestReadRangesKeepsAtMostTheBounds(t *testing.T) {
	client := readerCluster(t, "topic-a", "topic-b")
	ctx := context.Background()
	for i := range 6 {
		for _, topic := range []string{"topic-a", "topic-b"} {
			if err := client.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(strings.Repeat(topic[len(topic)-1:], 100+i))}).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
	}
	reader := NewReader(client)
	defer reader.Close()
	offsets := map[string]int64{"topic-a": 0, "topic-b": 0}
	var read []*kgo.Record
	calls := 0
	for calls < 20 {
		calls++
		ranges, err := reader.ReadRanges(ctx, offsets, Bounds{Records: 5})
		if err != nil {
			t.Fatal(err)
		}
		kept := 0
		for topic, r := range ranges {
			if r.Err != nil {
				t.Fatal(r.Err)
			}
			kept += len(r.Records)
			read = append(read, r.Records...)
			if r.End != 6 || r.Next > r.End || r.Next < offsets[topic] {
				t.Fatalf("%s: end %d next %d from %d", topic, r.End, r.Next, offsets[topic])
			}
			if len(r.Records) > 0 && r.Next < r.Records[len(r.Records)-1].Offset+1 {
				t.Fatalf("%s resumes at %d below its last record %d", topic, r.Next, r.Records[len(r.Records)-1].Offset)
			}
			offsets[topic] = r.Next
		}
		if kept > 5 {
			t.Fatalf("call %d kept %d records, over the bound of 5", calls, kept)
		}
		if offsets["topic-a"] == 6 && offsets["topic-b"] == 6 {
			break
		}
	}
	if len(read) != 12 || calls < 3 {
		t.Fatalf("read %d records in %d calls; want all 12 in at least 3", len(read), calls)
	}
	for _, topic := range []string{"topic-a", "topic-b"} {
		var got []int64
		for _, record := range read {
			if record.Topic == topic {
				got = append(got, record.Offset)
			}
		}
		if !slices.Equal(got, []int64{0, 1, 2, 3, 4, 5}) {
			t.Fatalf("%s read offsets %v; want each once in order", topic, got)
		}
	}
	// A byte bound is passed by at most the record that crosses it, and an unbounded call reads to the end.
	ranges, err := reader.ReadRanges(ctx, map[string]int64{"topic-a": 0}, Bounds{Bytes: 250})
	if err != nil || ranges["topic-a"].Err != nil || len(ranges["topic-a"].Records) != 3 || ranges["topic-a"].Next != 3 {
		t.Fatalf("byte bound: %v %v %d records, next %d; want 3 records and next 3", err, ranges["topic-a"].Err, len(ranges["topic-a"].Records), ranges["topic-a"].Next)
	}
	ranges, err = reader.ReadRanges(ctx, map[string]int64{"topic-a": 2, "topic-b": 6}, Bounds{})
	if err != nil || len(ranges["topic-a"].Records) != 4 || ranges["topic-a"].Next != 6 || len(ranges["topic-b"].Records) != 0 || ranges["topic-b"].Next != 6 {
		t.Fatalf("unbounded: %v %+v", err, ranges)
	}
}

// Under an open transaction the listed end is the last stable offset, below the records appended after it; a bounded
// read cut short before it resumes below it, and one that reached it resumes exactly there.
func TestReadRangesResumesAtTheLastStableOffset(t *testing.T) {
	client := readerCluster(t, "topic-a")
	ctx := context.Background()
	for range 3 {
		if err := client.ProduceSync(ctx, &kgo.Record{Topic: "topic-a", Value: []byte("committed")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	producer, err := kgo.NewClient(append(client.Opts(), kgo.TransactionalID("p.pool-a.run-a"))...)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if err := producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := producer.ProduceSync(ctx, &kgo.Record{Topic: "topic-a", Value: []byte("open")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := client.ProduceSync(ctx, &kgo.Record{Topic: "topic-a", Value: []byte("after the open one")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	reader := NewReader(client)
	defer reader.Close()
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ranges, err := reader.ReadRanges(readCtx, map[string]int64{"topic-a": 0}, Bounds{Records: 2})
	if err != nil || ranges["topic-a"].Err != nil || len(ranges["topic-a"].Records) != 2 || ranges["topic-a"].Next != 2 || ranges["topic-a"].End != 3 {
		t.Fatalf("bounded under an open transaction: %v %+v", err, ranges["topic-a"])
	}
	ranges, err = reader.ReadRanges(readCtx, map[string]int64{"topic-a": 2}, Bounds{Records: 10})
	if err != nil || ranges["topic-a"].Err != nil || len(ranges["topic-a"].Records) != 1 || ranges["topic-a"].Next != 3 {
		t.Fatalf("to the end under an open transaction: %v %+v", err, ranges["topic-a"])
	}
	if err := producer.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}
	ranges, err = reader.ReadRanges(readCtx, map[string]int64{"topic-a": 3}, Bounds{Records: 10})
	if err != nil || ranges["topic-a"].Err != nil || len(ranges["topic-a"].Records) != 2 || ranges["topic-a"].Next != 6 {
		t.Fatalf("after the commit: %v %+v", err, ranges["topic-a"])
	}
}
