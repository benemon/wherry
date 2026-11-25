package operator

import (
	"bytes"
	"context"
	json "encoding/json/v2"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// poolRecords are the pool records on wherry.operator, as written, in offset order.
func poolRecords(t *testing.T, ctx context.Context, service *Service) []Record {
	t.Helper()
	reader := NewReader(service.Operator)
	t.Cleanup(reader.Close)
	raw, err := readRaw(ctx, reader)
	if err != nil {
		t.Fatal(err)
	}
	var records []Record
	for _, entry := range raw {
		var record Record
		if err := json.Unmarshal(entry.Value, &record); err != nil {
			t.Fatal(err)
		}
		if record.Kind == "pool" {
			records = append(records, record)
		}
	}
	return records
}

// A pool file without producer_byte_rate and request_percentage records 1048576 and 10, explicit values win, and
// re-applying the same file appends nothing.
func TestPoolApplyRecordsTheQuotaDefaults(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	omitted := modelTerms()
	omitted.ProducerByteRate, omitted.RequestPercentage = 0, 0
	if err := service.PoolApply(ctx, "pool-a", omitted, false); err != nil {
		t.Fatal(err)
	}
	records := poolRecords(t, ctx, service)
	if len(records) != 1 || records[0].Terms.ProducerByteRate != DefaultProducerByteRate || records[0].Terms.RequestPercentage != DefaultRequestPercentage {
		t.Fatalf("pool records %+v; want one holding producer_byte_rate %d and request_percentage %d", records, DefaultProducerByteRate, DefaultRequestPercentage)
	}
	state, err := service.LoadState(ctx)
	if err != nil || state.Pools["pool-a"].ProducerByteRate != DefaultProducerByteRate || state.Pools["pool-a"].RequestPercentage != DefaultRequestPercentage {
		t.Fatalf("loaded terms %+v, %v; want the defaults", state.Pools["pool-a"], err)
	}
	if err := service.PoolApply(ctx, "pool-a", omitted, false); err != nil {
		t.Fatal(err)
	}
	if again := poolRecords(t, ctx, service); len(again) != 1 {
		t.Fatalf("re-applying the same file appended a record: %+v", again)
	}
	explicit := modelTerms()
	explicit.ProducerByteRate, explicit.RequestPercentage = 2000000, 25
	if err := service.PoolApply(ctx, "pool-b", explicit, false); err != nil {
		t.Fatal(err)
	}
	records = poolRecords(t, ctx, service)
	if len(records) != 2 || records[1].Pool != "pool-b" || records[1].Terms.ProducerByteRate != 2000000 || records[1].Terms.RequestPercentage != 25 {
		t.Fatalf("pool records %+v; want pool-b recorded with the explicit quotas", records)
	}
}

// zeroedRecord decodes encoded as a Record, as decode does before validateRecord.
func zeroedRecord(t *testing.T, encoded []byte) Record {
	t.Helper()
	var record Record
	if err := json.Unmarshal(encoded, &record); err != nil {
		t.Fatal(err)
	}
	return record
}

// A recorded producer_byte_rate or request_percentage of 0 is still refused on load, so a damaged record never lets
// applyQuota remove a principal's quotas.
func TestLoadRefusesARecordedZeroQuota(t *testing.T) {
	for _, field := range []string{"producer_byte_rate", "request_percentage"} {
		t.Run(field, func(t *testing.T) {
			_, service, closeFixture := kfakeService(t)
			defer closeFixture()
			ctx := context.Background()
			if err := service.PoolApply(ctx, "pool-a", modelTerms(), false); err != nil {
				t.Fatal(err)
			}
			reader := NewReader(service.Operator)
			t.Cleanup(reader.Close)
			raw, err := readRaw(ctx, reader)
			if err != nil {
				t.Fatal(err)
			}
			var pool []byte
			for _, entry := range raw {
				if bytes.Contains(entry.Value, []byte(`"kind":"pool"`)) {
					pool = entry.Value
				}
			}
			if pool == nil {
				t.Fatal("no pool record")
			}
			start := bytes.Index(pool, []byte(`"`+field+`":`))
			if start < 0 {
				t.Fatalf("pool record %s names no %s", pool, field)
			}
			end := start + len(field) + 3
			for end < len(pool) && pool[end] != ',' && pool[end] != '}' {
				end++
			}
			zeroed := append(append(append([]byte{}, pool[:start+len(field)+3]...), '0'), pool[end:]...)
			if err := service.Operator.ProduceSync(ctx, &kgo.Record{Topic: OperatorTopic, Key: []byte("pool.pool-a"), Value: zeroed}).FirstErr(); err != nil {
				t.Fatal(err)
			}
			// decode names the undecodable record and the reset, never the record's own fault.
			if _, err := service.LoadState(ctx); err == nil || !strings.Contains(err.Error(), "reset the deployment") {
				t.Fatalf("load over a pool record with %s 0: %v; want it refused", field, err)
			}
			if err := validateRecord(zeroedRecord(t, zeroed)); err == nil || !strings.Contains(err.Error(), "invalid pool terms") {
				t.Fatalf("validateRecord of a pool record with %s 0: %v; want invalid pool terms", field, err)
			}
		})
	}
}
