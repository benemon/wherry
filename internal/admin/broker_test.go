package admin

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/trust"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"gopkg.in/yaml.v3"
)

// fixture is provisioned on a real broker through Service: two pools, two
// tenants (a entitled to pool a, b to pool b), two hosts for tenant a, one
// receiver per pool. Names carry a per-run suffix; ACLs and SCRAM users are
// removed afterwards, topics stay.
type fixture struct {
	service                        *Service
	poolA, poolB, tenantA, tenantB string
	hostA1, hostA2, hostB1         string
	receiverA, receiverB           string
	bundles                        map[string]Bundle
}

func TestRealBrokerFixture(t *testing.T) {
	if os.Getenv("WHERRY_TRUST_BROKER") == "" {
		t.Skip("WHERRY_TRUST_BROKER is not set")
	}
	f := provision(t)
	ctx := context.Background()

	t.Run("a task submitted before the first fetch is delivered", func(t *testing.T) {
		submitter := f.connect(t, TenantPrincipal(f.tenantA, f.hostA1))
		if err := submitter.ProduceSync(ctx, &kgo.Record{Topic: PoolTasksTopic(f.poolA), Value: []byte("before-first-fetch")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
		member := f.connect(t, ReceiverPrincipal(f.receiverA), kgo.ConsumeTopics(PoolTasksTopic(f.poolA)), kgo.ShareGroup(PoolGroup(f.poolA)), kgo.ShareMaxRecords(1))
		pollCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		for pollCtx.Err() == nil {
			fetches := member.PollRecords(pollCtx, 1)
			for _, fetchErr := range fetches.Errors() {
				if !errors.Is(fetchErr.Err, context.DeadlineExceeded) {
					t.Fatalf("share fetch: %v", fetchErr.Err)
				}
			}
			for _, record := range fetches.Records() {
				if string(record.Value) == "before-first-fetch" {
					record.Ack(kgo.AckAccept)
					_ = member.FlushAcks(ctx)
					return
				}
			}
		}
		t.Fatal("task submitted before the first fetch was not delivered")
	})

	t.Run("a tenant cannot write a pool it is not entitled to", func(t *testing.T) {
		host := f.connect(t, TenantPrincipal(f.tenantB, f.hostB1))
		expectDenied(t, host.ProduceSync(ctx, &kgo.Record{Topic: PoolTasksTopic(f.poolA), Value: []byte("x")}).FirstErr())
	})

	t.Run("a tenant reads only its own feed", func(t *testing.T) {
		expectReadable(t, f.connect(t, TenantPrincipal(f.tenantA, f.hostA2)), TenantTopic(f.tenantA))
		expectDenied(t, listEnd(f.connect(t, TenantPrincipal(f.tenantB, f.hostB1)), TenantTopic(f.tenantA)))
	})

	t.Run("key topics have one writer", func(t *testing.T) {
		sibling := f.connect(t, TenantPrincipal(f.tenantA, f.hostA2))
		expectDenied(t, sibling.ProduceSync(ctx, &kgo.Record{Topic: HostTopic(f.tenantA, f.hostA1), Value: []byte("x")}).FirstErr())
		peer := f.connect(t, ReceiverPrincipal(f.receiverA))
		expectDenied(t, peer.ProduceSync(ctx, &kgo.Record{Topic: ReceiverTopic(f.receiverB), Value: []byte("x")}).FirstErr())
		_, key, _ := ed25519.GenerateKey(rand.Reader)
		registration, err := signed.RegisterKey(signed.HostOwner(f.tenantA, f.hostA1), key)
		if err != nil {
			t.Fatal(err)
		}
		owner := f.connect(t, TenantPrincipal(f.tenantA, f.hostA1))
		if err := owner.ProduceSync(ctx, &kgo.Record{Topic: HostTopic(f.tenantA, f.hostA1), Value: registration}).FirstErr(); err != nil {
			t.Fatalf("owner registration: %v", err)
		}
	})

	t.Run("a host and a receiver rotate their own keys", func(t *testing.T) {
		for _, c := range []struct{ owner, principal, topic string }{
			{signed.HostOwner(f.tenantA, f.hostA2), TenantPrincipal(f.tenantA, f.hostA2), HostTopic(f.tenantA, f.hostA2)},
			{f.receiverB, ReceiverPrincipal(f.receiverB), ReceiverTopic(f.receiverB)},
		} {
			client := f.connect(t, c.principal)
			path := filepath.Join(t.TempDir(), "signing.key")
			key, err := trust.GenerateSigningKeyFile(path)
			if err != nil {
				t.Fatal(err)
			}
			registration, err := signed.RegisterKey(c.owner, key)
			if err != nil {
				t.Fatal(err)
			}
			if err := client.ProduceSync(ctx, &kgo.Record{Topic: c.topic, Key: []byte(c.owner), Value: registration}).FirstErr(); err != nil {
				t.Fatalf("%s registration: %v", c.owner, err)
			}
			next, err := RotateKey(ctx, client, c.owner, c.topic, path, key)
			if err != nil {
				t.Fatalf("%s rotation: %v", c.owner, err)
			}
			if _, err := os.Stat(path + ".next"); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("%s next key file left: %v", c.owner, err)
			}
			if loaded, err := trust.LoadSigningKeyFile(path); err != nil || !loaded.Equal(next) {
				t.Fatalf("%s key file does not hold the next key: %v", c.owner, err)
			}
			records, err := readTopic(ctx, client, c.topic)
			if err != nil {
				t.Fatal(err)
			}
			keys, err := ReadKeys(ctx, client, map[string]string{c.owner: c.topic})
			if err != nil {
				t.Fatal(err)
			}
			state := keys[c.owner]
			// The retirement time is the broker's: key topics use LogAppendTime.
			if len(records) != 2 || records[1].Attrs.TimestampType() != 1 || !state.Current.Equal(next.Public()) || len(state.Retired) != 1 ||
				!state.Retired[0].Key.Equal(key.Public()) || !state.Retired[0].At.Equal(records[1].Timestamp) {
				t.Fatalf("%s key topic of %d records folded to %#v; want the old key retired at the rotation's log append time", c.owner, len(records), state)
			}
		}
	})

	t.Run("a receiver writes only its own pool", func(t *testing.T) {
		receiver := f.connect(t, ReceiverPrincipal(f.receiverA))
		if err := receiver.ProduceSync(ctx, &kgo.Record{Topic: PoolEventsTopic(f.poolA), Value: []byte("x")}).FirstErr(); err != nil {
			t.Fatalf("own pool events: %v", err)
		}
		expectDenied(t, receiver.ProduceSync(ctx, &kgo.Record{Topic: PoolEventsTopic(f.poolB), Value: []byte("x")}).FirstErr())
		expectDenied(t, receiver.ProduceSync(ctx, &kgo.Record{Topic: TenantTopic(f.tenantB), Value: []byte("x")}).FirstErr())
		foreign := f.connect(t, ReceiverPrincipal(f.receiverA), kgo.TransactionalID(TransactionalPrefix(f.poolB)+"run"))
		err := foreign.BeginTransaction()
		if err == nil {
			err = foreign.ProduceSync(ctx, &kgo.Record{Topic: PoolEventsTopic(f.poolA), Value: []byte("x")}).FirstErr()
		}
		// franz-go reports the failed producer-ID request without wrapping kerr.
		if err == nil || !strings.Contains(err.Error(), kerr.TransactionalIDAuthorizationFailed.Message) {
			t.Fatalf("another pool's transactional ID: got %v", err)
		}
	})

	t.Run("a status is on both topics or on neither", func(t *testing.T) {
		writer := f.connect(t, ReceiverPrincipal(f.receiverA), kgo.TransactionalID(TransactionalPrefix(f.poolA)+"run-p14"))
		committed, aborted := "committed-"+f.poolA, "aborted-"+f.poolA
		if err := writer.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		for _, topic := range []string{PoolEventsTopic(f.poolA), TenantTopic(f.tenantA)} {
			if err := writer.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(committed), Value: []byte(committed)}).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
		if err := writer.EndTransaction(ctx, kgo.TryCommit); err != nil {
			t.Fatal(err)
		}
		if err := writer.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		for _, topic := range []string{PoolEventsTopic(f.poolA), TenantTopic(f.tenantA)} {
			if err := writer.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(aborted), Value: []byte(aborted)}).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
		if err := writer.EndTransaction(ctx, kgo.TryAbort); err != nil {
			t.Fatal(err)
		}
		pool := readAll(t, f.connect(t, ReceiverPrincipal(f.receiverA), kgo.FetchIsolationLevel(kgo.ReadCommitted())), PoolEventsTopic(f.poolA))
		feed := readAll(t, f.connect(t, TenantPrincipal(f.tenantA, f.hostA1), kgo.FetchIsolationLevel(kgo.ReadCommitted())), TenantTopic(f.tenantA))
		for name, values := range map[string]map[string]bool{"pool events": pool, "tenant feed": feed} {
			if !values[committed] || values[aborted] {
				t.Fatalf("%s: committed=%t aborted=%t", name, values[committed], values[aborted])
			}
		}
	})

	t.Run("only the operator writes wherry.admin", func(t *testing.T) {
		receiver := f.connect(t, ReceiverPrincipal(f.receiverA), kgo.ProducerBatchMaxBytes(4<<20), kgo.ProducerBatchCompression(kgo.NoCompression()))
		expectReadable(t, receiver, AdminTopic)
		// wherry.admin is never deleted, so the write probe must be unable to
		// land even if the ACL is wrong: an incompressible record above the
		// broker's size limit. The broker authorises a produce before checking
		// size, so denial still reports as denial.
		probe := make([]byte, 2<<20)
		if _, err := rand.Read(probe); err != nil {
			t.Fatal(err)
		}
		err := receiver.ProduceSync(ctx, &kgo.Record{Topic: PoolEventsTopic(f.poolA), Value: probe}).FirstErr()
		if !errors.Is(err, kerr.MessageTooLarge) && !errors.Is(err, kerr.RecordListTooLarge) {
			t.Fatalf("write probe could land on a writable topic: %v", err)
		}
		expectDenied(t, receiver.ProduceSync(ctx, &kgo.Record{Topic: AdminTopic, Value: probe}).FirstErr())
	})

	t.Run("provisioning survives Strimzi reconciliation", func(t *testing.T) {
		waitText := os.Getenv("WHERRY_TRUST_RECONCILE_WAIT")
		if waitText == "" {
			t.Skip("WHERRY_TRUST_RECONCILE_WAIT is not set")
		}
		wait, err := time.ParseDuration(waitText)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(wait)
		state, err := f.service.LoadState(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.service.verifyACLs(ctx, state); err != nil {
			t.Fatal(err)
		}
		expectReadable(t, f.connect(t, ReceiverPrincipal(f.receiverB)), PoolEventsTopic(f.poolB))
	})

	t.Run("a revoked principal is denied and recorded as ended", func(t *testing.T) {
		principal := TenantPrincipal(f.tenantB, f.hostB1)
		old := f.connect(t, principal)
		expectReadable(t, old, TenantTopic(f.tenantB))
		result := f.service.RevokeHost(ctx, f.tenantB, f.hostB1, time.Second, f.service.Probe(HostTopic(f.tenantB, f.hostB1)))
		for _, step := range result.Steps {
			t.Logf("step %d %s complete=%t err=%v", step.Number, step.Name, step.Complete, step.Err)
		}
		if !result.Complete {
			t.Fatal("revocation incomplete")
		}
		denied := f.connect(t, principal)
		if err := denied.Ping(ctx); !errors.Is(err, kerr.SaslAuthenticationFailed) {
			t.Fatalf("revoked principal authenticated: %v", err)
		}
		state, err := f.service.LoadState(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !state.Ended[principal] {
			t.Fatal("membership end not recorded")
		}
	})
}

func provision(t *testing.T) *fixture {
	suffixBytes := make([]byte, 3)
	if _, err := rand.Read(suffixBytes); err != nil {
		t.Fatal(err)
	}
	s := hex.EncodeToString(suffixBytes)
	f := &fixture{poolA: "pa" + s, poolB: "pb" + s, tenantA: "ta" + s, tenantB: "tb" + s, hostA1: "h1", hostA2: "h2", hostB1: "h1", receiverA: "ra" + s, receiverB: "rb" + s, bundles: map[string]Bundle{}}
	service, closeService := realService(t)
	t.Cleanup(closeService)
	f.service = service
	ctx := context.Background()
	terms := modelTerms()
	dir := t.TempDir()
	ends := map[string]Record{
		ReceiverPrincipal(f.receiverA):       {Kind: "end", Receiver: f.receiverA},
		ReceiverPrincipal(f.receiverB):       {Kind: "end", Receiver: f.receiverB},
		TenantPrincipal(f.tenantA, f.hostA1): {Kind: "end", Tenant: f.tenantA, Host: f.hostA1},
		TenantPrincipal(f.tenantA, f.hostA2): {Kind: "end", Tenant: f.tenantA, Host: f.hostA2},
		TenantPrincipal(f.tenantB, f.hostB1): {Kind: "end", Tenant: f.tenantB, Host: f.hostB1},
	}
	// Membership ends stop later provisioning from re-granting these ACLs.
	t.Cleanup(func() {
		state, _ := service.LoadState(ctx)
		for principal, end := range ends {
			_ = service.deletePrincipalACLs(ctx, principal)
			_, _ = kadm.NewClient(service.Admin).AlterUserSCRAMs(ctx, []kadm.DeleteSCRAM{{User: principal, Mechanism: kadm.ScramSha512}}, nil)
			if !state.Ended[principal] {
				_, _ = service.appendChecked(ctx, []byte("end."+principal), end)
			}
		}
	})
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(service.PoolCreate(ctx, f.poolA, terms))
	must(service.PoolCreate(ctx, f.poolB, terms))
	must(service.TenantAdd(ctx, f.tenantA, f.poolA))
	must(service.TenantAdd(ctx, f.tenantB, f.poolB))
	for _, receiver := range [][2]string{{f.receiverA, f.poolA}, {f.receiverB, f.poolB}} {
		path := filepath.Join(dir, receiver[0])
		must(service.ReceiverAdd(ctx, modelConfig(receiver[0], receiver[1]), path))
		f.bundles[ReceiverPrincipal(receiver[0])] = readBundle(t, path)
	}
	for _, host := range [][2]string{{f.tenantA, f.hostA1}, {f.tenantA, f.hostA2}, {f.tenantB, f.hostB1}} {
		path := filepath.Join(dir, host[0]+"."+host[1])
		must(service.HostAdd(ctx, host[0], host[1], path))
		f.bundles[TenantPrincipal(host[0], host[1])] = readBundle(t, path)
	}
	must(service.Check(ctx))
	return f
}

func readBundle(t *testing.T, path string) Bundle {
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("bundle mode %v", info.Mode().Perm())
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var bundle Bundle
	if err := yaml.Unmarshal(encoded, &bundle); err != nil {
		t.Fatal(err)
	}
	return bundle
}

func (f *fixture) connect(t *testing.T, principal string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	bundle, ok := f.bundles[principal]
	if !ok {
		t.Fatalf("no bundle for %s", principal)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(bundle.CA)) {
		t.Fatal("bundle CA holds no certificate")
	}
	client, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(bundle.Broker), kgo.DialTLSConfig(&tls.Config{RootCAs: roots}), kgo.SASL(scram.Auth{User: bundle.Principal, Pass: bundle.Password}.AsSha512Mechanism()), kgo.RequestRetries(1)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

func expectDenied(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, kerr.TopicAuthorizationFailed) {
		t.Fatalf("want TOPIC_AUTHORIZATION_FAILED, got %v", err)
	}
}

func expectReadable(t *testing.T, client *kgo.Client, topic string) {
	t.Helper()
	if err := listEnd(client, topic); err != nil {
		t.Fatalf("read %s: %v", topic, err)
	}
}

func listEnd(client *kgo.Client, topic string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	offsets, err := kadm.NewClient(client).ListEndOffsets(ctx, topic)
	if err != nil {
		return err
	}
	return offsets.Error()
}

// readAll reads a topic from the start until a poll returns nothing.
func readAll(t *testing.T, client *kgo.Client, topic string) map[string]bool {
	t.Helper()
	client.AddConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}})
	seen := map[string]bool{}
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		fetches := client.PollFetches(ctx)
		cancel()
		for _, fetchErr := range fetches.Errors() {
			if !errors.Is(fetchErr.Err, context.DeadlineExceeded) {
				t.Fatalf("fetch %s: %v", topic, fetchErr.Err)
			}
		}
		if fetches.NumRecords() == 0 {
			return seen
		}
		fetches.EachRecord(func(record *kgo.Record) { seen[string(record.Value)] = true })
	}
}

// The pool's share group takes the terms' defaults when the pool file leaves delivery_limit and lock_duration out.
func TestPoolGroupTakesTermDefaultsOnRealBroker(t *testing.T) {
	if os.Getenv("WHERRY_TRUST_BROKER") == "" {
		t.Skip("WHERRY_TRUST_BROKER is not set")
	}
	service, closeService := realService(t)
	defer closeService()
	ctx := context.Background()
	terms := modelTerms()
	terms.DeliveryLimit, terms.LockDurationMS = 0, 0
	pool := fmt.Sprintf("defaults-%d", time.Now().UnixNano())
	if err := service.PoolCreate(ctx, pool, terms); err != nil {
		t.Fatal(err)
	}
	groupTerms(t, service, pool, "5", "15000")
}

// pool update re-applies the share group's delivery limit and lock duration; its dry run leaves them.
func TestPoolUpdateReappliesTheShareGroupOnRealBroker(t *testing.T) {
	if os.Getenv("WHERRY_TRUST_BROKER") == "" {
		t.Skip("WHERRY_TRUST_BROKER is not set")
	}
	service, closeService := realService(t)
	defer closeService()
	ctx := context.Background()
	terms := modelTerms()
	terms.DeliveryLimit, terms.LockDurationMS = 5, 15000
	pool := fmt.Sprintf("update-%d", time.Now().UnixNano())
	if err := service.PoolCreate(ctx, pool, terms); err != nil {
		t.Fatal(err)
	}
	terms.DeliveryLimit, terms.LockDurationMS = 3, 30000
	if err := service.PoolUpdate(ctx, pool, terms, true); err != nil {
		t.Fatal(err)
	}
	groupTerms(t, service, pool, "5", "15000")
	if err := service.PoolUpdate(ctx, pool, terms, false); err != nil {
		t.Fatal(err)
	}
	groupTerms(t, service, pool, "3", "30000")
}

// groupTerms fails unless DescribeConfigs reports limit and lock as the delivery limit and lock duration of pool's share
// group within 30 seconds: a broker reports the group's configuration only once the alter reaches its metadata.
func groupTerms(t *testing.T, service *Service, pool, limit, lock string) {
	t.Helper()
	request := kmsg.NewPtrDescribeConfigsRequest()
	resource := kmsg.NewDescribeConfigsRequestResource()
	resource.ResourceType, resource.ResourceName = kmsg.ConfigResourceTypeGroupConfig, PoolGroup(pool)
	request.Resources = append(request.Resources, resource)
	deadline := time.Now().Add(30 * time.Second)
	for {
		response, err := service.Admin.Request(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]string{}
		for _, config := range response.(*kmsg.DescribeConfigsResponse).Resources[0].Configs {
			if config.Value != nil {
				got[config.Name] = *config.Value
			}
		}
		gotLimit, gotLock := got["share.delivery.count.limit"], got["share.record.lock.duration.ms"]
		if gotLimit == limit && gotLock == lock {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("share group delivery limit %q, lock %q; want %q and %q", gotLimit, gotLock, limit, lock)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
