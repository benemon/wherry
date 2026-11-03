package operator

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/kafkatest"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// These tests pin platform facts wherry's coordination relies on, against the real broker.

// F3: a member that renews, flushed, at an interval below the lock duration keeps a record acquired well past the
// lock duration; no other member of the group receives it meanwhile.
func TestShareRenewalHoldsARecordBeyondTheLockOnRealBroker(t *testing.T) {
	if os.Getenv("WHERRY_TRUST_BROKER") == "" {
		t.Skip("WHERRY_TRUST_BROKER is not set")
	}
	s := sharePool(t)
	holderResults := make(chan kgo.ShareAckResults, 64)
	holder := s.member(t, holderResults)
	other := s.member(t, nil)
	s.produce(t, "renewed")
	record := acquire(t, holder, 30*time.Second)
	if record == nil {
		t.Fatal("no member acquired the record")
	}
	ctx := context.Background()
	hold := 3 * s.lock
	renewInterval := s.lock / 3
	deadline := time.Now().Add(hold)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(renewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				record.Ack(kgo.AckRenew)
				if err := holder.FlushAcks(ctx); err != nil {
					t.Errorf("flush renewal: %v", err)
				}
			}
		}
	}()
	for time.Now().Before(deadline) {
		if stolen := acquire(t, other, time.Until(deadline)); stolen != nil {
			close(stop)
			wg.Wait()
			t.Fatalf("another member received the renewed record %s after %v of a %v lock", stolen.Value, hold-time.Until(deadline), s.lock)
		}
	}
	close(stop)
	wg.Wait()
	record.Ack(kgo.AckAccept)
	if err := holder.FlushAcks(ctx); err != nil {
		t.Fatal(err)
	}
	close(holderResults)
	renewals := 0
	for results := range holderResults {
		if err := results.Error(); err != nil {
			t.Fatalf("broker refused a renewal or the final accept: %v", err)
		}
		renewals++
	}
	// Ticks can drop under load; one renewal per lock duration is what keeps the record.
	if renewals < int(hold/s.lock) {
		t.Fatalf("%d acknowledgement results for a %v hold renewed every %v", renewals, hold, renewInterval)
	}
}

// F4: once a member's lock lapses and another member acquires the record, acknowledging the old acquisition flushes
// without error; the broker's refusal arrives only through ShareAckCallback.
func TestShareAckRefusalArrivesOnlyThroughTheCallbackOnRealBroker(t *testing.T) {
	if os.Getenv("WHERRY_TRUST_BROKER") == "" {
		t.Skip("WHERRY_TRUST_BROKER is not set")
	}
	s := sharePool(t)
	lapsedResults := make(chan kgo.ShareAckResults, 64)
	lapsed := s.member(t, lapsedResults)
	successor := s.member(t, nil)
	s.produce(t, "lapsed")
	record := acquire(t, lapsed, 30*time.Second)
	if record == nil {
		t.Fatal("no member acquired the record")
	}
	// lapsed neither renews nor polls: the lock runs out and the broker redelivers the record to successor.
	acquiredAt := time.Now()
	again := acquire(t, successor, 3*s.lock)
	if again == nil {
		t.Fatalf("no other member acquired the record within %v of a %v lock lapsing", 3*s.lock, s.lock)
	}
	if again.Offset != record.Offset || again.DeliveryCount() < 2 {
		t.Fatalf("successor acquired offset %d delivery %d; want offset %d redelivered", again.Offset, again.DeliveryCount(), record.Offset)
	}
	t.Logf("redelivered after %v", time.Since(acquiredAt))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	record.Ack(kgo.AckAccept)
	if err := lapsed.FlushAcks(ctx); err != nil {
		t.Fatalf("FlushAcks returned %v; the broker's answer to an acknowledgement is expected only through the callback", err)
	}
	var refusal error
	for len(lapsedResults) > 0 && refusal == nil {
		refusal = (<-lapsedResults).Error()
	}
	var brokerErr *kerr.Error
	if !errors.As(refusal, &brokerErr) {
		t.Fatalf("callback reported %v for an acknowledgement of a lapsed acquisition; want the broker's refusal", refusal)
	}
	t.Logf("broker refusal through the callback: %v", refusal)
	again.Ack(kgo.AckAccept)
	if err := successor.FlushAcks(ctx); err != nil {
		t.Fatal(err)
	}
}

// F18: host add sets the host principal's producer_byte_rate and request_percentage client quotas to its pool's
// terms.
func TestHostAddAppliesPoolQuotasOnRealBroker(t *testing.T) {
	if os.Getenv("WHERRY_TRUST_BROKER") == "" {
		t.Skip("WHERRY_TRUST_BROKER is not set")
	}
	service, closeService := realService(t)
	defer closeService()
	ctx := context.Background()
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	pool, tenant, host := "quota"+suffix, "tq"+suffix, "h1"
	principal := TenantPrincipal(tenant, host)
	terms := modelTerms()
	terms.ProducerByteRate, terms.RequestPercentage = 123457, 37
	t.Cleanup(func() {
		_ = service.deletePrincipalACLs(ctx, principal)
		_, _ = kadm.NewClient(service.SuperUser).AlterUserSCRAMs(ctx, []kadm.DeleteSCRAM{{User: principal, Mechanism: kadm.ScramSha512}}, nil)
		_, _ = service.appendChecked(ctx, []byte("end."+principal), Record{Kind: "end", Tenant: tenant, Host: host})
	})
	if err := service.PoolApply(ctx, pool, terms, false); err != nil {
		t.Fatal(err)
	}
	if err := service.TenantAdd(ctx, tenant, pool); err != nil {
		t.Fatal(err)
	}
	if err := service.HostAdd(ctx, tenant, host, filepath.Join(t.TempDir(), "bundle"), false); err != nil {
		t.Fatal(err)
	}
	// A broker describes the quotas only once the alter reaches its metadata.
	deadline := time.Now().Add(30 * time.Second)
	for {
		described, err := kadm.NewClient(service.SuperUser).DescribeClientQuotas(ctx, true, []kadm.DescribeClientQuotaComponent{{Type: "user", MatchName: &principal}})
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]float64{}
		for _, quota := range described {
			for _, value := range quota.Values {
				got[value.Key] = value.Value
			}
		}
		if got["producer_byte_rate"] == float64(terms.ProducerByteRate) && got["request_percentage"] == terms.RequestPercentage {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("client quotas on %s are %v; want producer_byte_rate %d and request_percentage %v", principal, got, terms.ProducerByteRate, terms.RequestPercentage)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// shareTest is a pool of its own whose share group locks records for lock, the broker's floor
// (group.share.min.record.lock.duration.ms, 15 s by default); its members connect as the Kafka super user.
type shareTest struct {
	service *Service
	pool    string
	lock    time.Duration
}

func sharePool(t *testing.T) shareTest {
	t.Helper()
	service, closeService := realService(t)
	t.Cleanup(closeService)
	terms := modelTerms()
	terms.LockDurationMS = 15000
	s := shareTest{service: service, pool: fmt.Sprintf("share-%d", time.Now().UnixNano()), lock: terms.LockDuration()}
	ctx := context.Background()
	if err := service.PoolApply(ctx, s.pool, terms, false); err != nil {
		t.Fatal(err)
	}
	groupTerms(t, service, s.pool, strconv.Itoa(int(terms.DeliveryLimit)), strconv.FormatInt(int64(terms.LockDurationMS), 10))
	if err := kafkatest.Led(ctx, service.SuperUser, PoolTasksTopic(s.pool)); err != nil {
		t.Fatal(err)
	}
	return s
}

// member joins the pool's share group taking one record at a time, as a receiver does. It fetches only while polled,
// so a record whose lock lapses goes to another member rather than back to it. Acknowledgement results go to results
// if it is not nil.
func (s shareTest) member(t *testing.T, results chan<- kgo.ShareAckResults) *kgo.Client {
	t.Helper()
	ca, err := os.ReadFile(os.Getenv("WHERRY_TRUST_CA"))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("invalid CA")
	}
	password, err := os.ReadFile(os.Getenv("WHERRY_TRUST_SUPER_USER_PASSWORD_FILE"))
	if err != nil {
		t.Fatal(err)
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(os.Getenv("WHERRY_TRUST_BROKER")), kgo.DialTLSConfig(&tls.Config{RootCAs: roots}),
		kgo.SASL(scram.Auth{User: os.Getenv("WHERRY_TRUST_SUPER_USER"), Pass: strings.TrimSpace(string(password))}.AsSha512Mechanism()),
		kgo.ConsumeTopics(PoolTasksTopic(s.pool)), kgo.ShareGroup(PoolGroup(s.pool)), kgo.ShareMaxRecords(1), kgo.ShareMaxRecordsStrict(),
		kgo.MaxConcurrentFetches(0), kgo.FetchMaxWait(time.Second),
		kgo.ShareAckCallback(func(_ *kgo.Client, r kgo.ShareAckResults) {
			if results != nil {
				results <- r
			}
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

func (s shareTest) produce(t *testing.T, value string) {
	t.Helper()
	if err := s.service.SuperUser.ProduceSync(context.Background(), &kgo.Record{Topic: PoolTasksTopic(s.pool), Value: []byte(value)}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// acquire polls member for up to wait and returns the first record it acquires, or nil.
func acquire(t *testing.T, member *kgo.Client, wait time.Duration) *kgo.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	for ctx.Err() == nil {
		fetches := member.PollRecords(ctx, 1)
		for _, fetchErr := range fetches.Errors() {
			if !errors.Is(fetchErr.Err, context.DeadlineExceeded) {
				t.Fatalf("share fetch: %v", fetchErr.Err)
			}
		}
		if records := fetches.Records(); len(records) > 0 {
			return records[0]
		}
	}
	return nil
}
