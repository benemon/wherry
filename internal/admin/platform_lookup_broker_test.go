package admin

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Facts the task lookup by offset (agb-lu1d.6) relies on, pinned against the real broker.

// A share delivery carries the record's partition, offset and LogAppendTime, as a plain read of the tasks topic does.
func TestShareDeliveryCarriesTheRecordsPositionAndAppendTimeOnRealBroker(t *testing.T) {
	if os.Getenv("WHERRY_TRUST_BROKER") == "" {
		t.Skip("WHERRY_TRUST_BROKER is not set")
	}
	s := sharePool(t)
	member := s.member(t, nil)
	for _, value := range []string{"first", "second", "third"} {
		s.produce(t, value)
	}
	reader := NewReader(s.service.Admin)
	defer reader.Close()
	records, err := reader.ReadFrom(context.Background(), PoolTasksTopic(s.pool), 0)
	if err != nil {
		t.Fatal(err)
	}
	// Each of the three deliveries must match the plain read at its own offset, so a share decoder that reported one
	// offset for every record fails.
	seen := map[int64]bool{}
	for range records {
		delivered := acquire(t, member, 30*time.Second)
		if delivered == nil {
			t.Fatalf("delivered %d of %d records", len(seen), len(records))
		}
		var read *kgo.Record
		for _, record := range records {
			if record.Offset == delivered.Offset {
				read = record
			}
		}
		if read == nil || seen[delivered.Offset] {
			t.Fatalf("delivered offset %d is not a distinct offset of the topic", delivered.Offset)
		}
		seen[delivered.Offset] = true
		if delivered.Partition != 0 || !bytes.Equal(delivered.Value, read.Value) || !delivered.Timestamp.Equal(read.Timestamp) {
			t.Fatalf("delivered partition %d value %q at %v; read %q at %v", delivered.Partition, delivered.Value, delivered.Timestamp, read.Value, read.Timestamp)
		}
		if delivered.Attrs.TimestampType() != 1 || read.Attrs.TimestampType() != 1 {
			t.Fatalf("timestamp types %d and %d, want LogAppendTime (1)", delivered.Attrs.TimestampType(), read.Attrs.TimestampType())
		}
		delivered.Ack(kgo.AckAccept)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := member.FlushAcks(ctx)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
}

// With retention.bytes -1, a record younger than retention.ms survives its segment's roll and retention's checks,
// while the same record on a topic with a short retention.ms is deleted.
func TestRetentionKeepsARecordYoungerThanRetentionMsOnRealBroker(t *testing.T) {
	if os.Getenv("WHERRY_TRUST_BROKER") == "" {
		t.Skip("WHERRY_TRUST_BROKER is not set")
	}
	service, closeService := realService(t)
	defer closeService()
	ctx := context.Background()
	admin := kadm.NewClient(service.Admin)
	suffix := time.Now().UnixNano()
	short, long := fmt.Sprintf("pin-retention-short-%d", suffix), fmt.Sprintf("pin-retention-long-%d", suffix)
	for topic, retention := range map[string]string{short: "15000", long: "3600000"} {
		createPinTopic(t, admin, topic, map[string]*string{"retention.ms": kadm.StringPtr(retention), "segment.ms": kadm.StringPtr("5000")})
	}
	produce := func(value string) {
		for _, topic := range []string{short, long} {
			if err := service.Admin.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(value)}).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
	}
	produce("first")
	time.Sleep(6 * time.Second)
	produce("second") // past segment.ms: rolls the first record's segment
	deadline := time.Now().Add(90 * time.Second)
	for {
		starts, err := admin.ListStartOffsets(ctx, short, long)
		if err != nil {
			t.Fatal(err)
		}
		shortStart, _ := starts.Lookup(short, 0)
		longStart, _ := starts.Lookup(long, 0)
		if longStart.Offset != 0 {
			t.Fatalf("long-retention topic's log start moved to %d", longStart.Offset)
		}
		if shortStart.Offset > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("retention never deleted the short-retention topic's rolled segment; the contrast is not shown")
		}
		time.Sleep(time.Second)
	}
	reader := NewReader(service.Admin)
	defer reader.Close()
	records, err := reader.ReadFrom(ctx, long, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) == 0 || records[0].Offset != 0 || string(records[0].Value) != "first" {
		t.Fatalf("long-retention topic does not hold its first record at offset 0: %d records", len(records))
	}
}

// A read below the log start begins at the log start, and a read past the end returns nothing without waiting for an
// append: a reader of one exact offset must compare the offset it gets.
func TestReadOutsideTheLogReturnsPromptlyOnRealBroker(t *testing.T) {
	if os.Getenv("WHERRY_TRUST_BROKER") == "" {
		t.Skip("WHERRY_TRUST_BROKER is not set")
	}
	service, closeService := realService(t)
	defer closeService()
	ctx := context.Background()
	admin := kadm.NewClient(service.Admin)
	topic := fmt.Sprintf("pin-read-%d", time.Now().UnixNano())
	createPinTopic(t, admin, topic, nil)
	for _, value := range []string{"zero", "one", "two", "three"} {
		if err := service.Admin.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(value)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	to := kadm.Offsets{}
	to.Add(kadm.Offset{Topic: topic, Partition: 0, At: 2})
	deleted, err := admin.DeleteRecords(ctx, to)
	if err == nil {
		err = deleted.Error()
	}
	if err != nil {
		t.Fatal(err)
	}
	reader := NewReader(service.Admin)
	defer reader.Close()
	for _, c := range []struct {
		from  int64
		first int64 // -1: no records
	}{{0, 2}, {1, 2}, {3, 3}, {4, -1}, {100, -1}} {
		readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		started := time.Now()
		records, err := reader.ReadFrom(readCtx, topic, c.from)
		cancel()
		if err != nil {
			t.Fatalf("read from %d: %v", c.from, err)
		}
		if took := time.Since(started); took > 5*time.Second {
			t.Fatalf("read from %d took %v", c.from, took)
		}
		switch {
		case c.first < 0 && len(records) != 0:
			t.Fatalf("read from %d returned %d records, want none", c.from, len(records))
		case c.first >= 0 && (len(records) == 0 || records[0].Offset != c.first):
			t.Fatalf("read from %d does not begin at offset %d", c.from, c.first)
		}
	}
}

// createPinTopic creates a one-partition delete-policy LogAppendTime topic with retention.bytes -1 and configs, and
// deletes it when the test ends.
func createPinTopic(t *testing.T, admin *kadm.Client, topic string, configs map[string]*string) {
	t.Helper()
	all := map[string]*string{"cleanup.policy": kadm.StringPtr("delete"), "message.timestamp.type": kadm.StringPtr("LogAppendTime"), "retention.bytes": kadm.StringPtr("-1")}
	for name, value := range configs {
		all[name] = value
	}
	if _, err := admin.CreateTopic(context.Background(), 1, -1, all, topic); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.DeleteTopics(context.Background(), topic) })
}
