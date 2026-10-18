package admin

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/signed"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func TestP01TaskKeyMustBelongToSubmittingTenant(t *testing.T) {
	state := modelState(t)
	_, own, _ := ed25519.GenerateKey(rand.Reader)
	_, foreign, _ := ed25519.GenerateKey(rand.Reader)
	keys := signed.Keys{signed.HostOwner("tenant-a", "host-a"): {Current: own.Public().(ed25519.PublicKey)}, signed.HostOwner("tenant-b", "host-b"): {Current: foreign.Public().(ed25519.PublicKey)}}
	task, err := signed.SignTask(signed.Task{Tenant: "tenant-a", Host: "host-a", Run: "run-a", Pool: "pool-a", Agent: "agent-a"}, own)
	if err != nil {
		t.Fatal(err)
	}
	if err := AuthorizeTask(state, keys, task, "pool-a"); err != nil {
		t.Fatal(err)
	}
	forged, err := signed.SignTask(signed.Task{Tenant: "tenant-a", Host: "host-a", Run: "run-a", Pool: "pool-a", Agent: "agent-a"}, foreign)
	if err != nil {
		t.Fatal(err)
	}
	if err := AuthorizeTask(state, keys, forged, "pool-a"); err == nil {
		t.Fatal("accepted a task signed by a key on another tenant's host topic")
	}
	claimed, err := signed.SignTask(signed.Task{Tenant: "tenant-a", Host: "host-b", Run: "run-a", Pool: "pool-a", Agent: "agent-a"}, foreign)
	if err != nil {
		t.Fatal(err)
	}
	if err := AuthorizeTask(state, keys, claimed, "pool-a"); err == nil {
		t.Fatal("accepted a task naming a host its tenant does not own")
	}
	if err := AuthorizeTask(state, keys, task, "pool-b"); err == nil {
		t.Fatal("accepted a task on a pool topic it does not name")
	}
	keys[signed.HostOwner("tenant-a", "host-z")] = signed.KeyState{Current: own.Public().(ed25519.PublicKey)}
	unmapped, err := signed.SignTask(signed.Task{Tenant: "tenant-a", Host: "host-z", Run: "run-a", Pool: "pool-a", Agent: "agent-a"}, own)
	if err != nil {
		t.Fatal(err)
	}
	if err := AuthorizeTask(state, keys, unmapped, "pool-a"); err == nil {
		t.Fatal("accepted a task from a host with no host-to-tenant mapping")
	}
	unentitled, err := signed.SignTask(signed.Task{Tenant: "tenant-b", Host: "host-b", Run: "run-b", Pool: "pool-b", Agent: "agent-a"}, foreign)
	if err != nil {
		t.Fatal(err)
	}
	if err := AuthorizeTask(state, keys, unentitled, "pool-b"); err == nil {
		t.Fatal("accepted a task for a pool the tenant is not entitled to")
	}
	state.Ended[TenantPrincipal("tenant-a", "host-a")] = true
	if err := AuthorizeTask(state, keys, task, "pool-a"); err == nil {
		t.Fatal("accepted a task from a host after its membership end")
	}
}

func TestP02CancelMustMatchRunTenant(t *testing.T) {
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	keys := signed.Keys{signed.HostOwner("tenant-b", "host-b"): {Current: key.Public().(ed25519.PublicKey)}}
	cancel, err := signed.SignCancel(signed.Cancel{Tenant: "tenant-b", Host: "host-b", Run: "run-a", Pool: "pool-a"}, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := signed.ParseCancel(cancel, keys, "tenant-b"); err != nil {
		t.Fatal(err)
	}
	if _, err := signed.ParseCancel(cancel, keys, "tenant-a"); err == nil {
		t.Fatal("accepted another tenant's cancel")
	}
}

func TestP03TenantFeedACLIsIsolated(t *testing.T) {
	state := modelState(t)
	if hasACL(ACLs(state), TenantPrincipal("tenant-b", "host-b"), TenantTopic("tenant-a"), "READ") {
		t.Fatal("second tenant can read the first tenant's feed")
	}
	if !hasACL(ACLs(state), TenantPrincipal("tenant-a", "host-a"), TenantTopic("tenant-a"), "READ") {
		t.Fatal("own tenant feed read ACL missing")
	}
}

func TestP05ConstructedNamesDoNotAlias(t *testing.T) {
	if HostTopic("a", "b-c") == HostTopic("a-b", "c") {
		t.Fatal("host tuples alias")
	}
	state := NewState()
	state.Hosts[TenantPrincipal("a", "b-c")] = Host{"a", "b-c"}
	state.Hosts[TenantPrincipal("a-b", "c")] = Host{"a-b", "c"}
	if err := state.CheckNames(); err != nil {
		t.Fatal(err)
	}
}

func TestP06StatusBindsPlacedPool(t *testing.T) {
	state := modelState(t)
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	keys := signed.Keys{"receiver-a": {Current: key.Public().(ed25519.PublicKey)}}
	status, err := signed.SignStatus(signed.Status{EventID: "event-a", Receiver: "receiver-a", Tenant: "tenant-a", Run: "run-a", Pool: "pool-a", State: "running"}, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := AuthorizeStatus(state, keys, status, "pool-a", "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if err := AuthorizeStatus(state, keys, status, "pool-b", "tenant-a"); err == nil {
		t.Fatal("accepted pool-a receiver status for a pool-b run")
	}
	if err := AuthorizeStatus(state, keys, status, "pool-a", "tenant-b"); err == nil {
		t.Fatal("accepted a status moved to another tenant's run")
	}
	forged, err := signed.SignStatus(signed.Status{EventID: "event-a", Receiver: "receiver-b", Tenant: "tenant-a", Run: "run-a", Pool: "pool-a", State: "running"}, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := AuthorizeStatus(state, keys, forged, "pool-a", "tenant-a"); err == nil {
		t.Fatal("accepted a status naming a receiver whose key did not sign it")
	}
	claimed, err := signed.SignStatus(signed.Status{EventID: "event-a", Receiver: "receiver-a", Tenant: "tenant-a", Run: "run-b", Pool: "pool-b", State: "running"}, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := AuthorizeStatus(state, keys, claimed, "pool-b", "tenant-a"); err == nil {
		t.Fatal("accepted a pool-b status from a pool-a receiver")
	}
	keys["receiver-b"] = signed.KeyState{Current: key.Public().(ed25519.PublicKey)}
	unentitled, err := signed.SignStatus(signed.Status{EventID: "event-b", Receiver: "receiver-b", Tenant: "tenant-b", Run: "run-b", Pool: "pool-b", State: "running"}, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := AuthorizeStatus(state, keys, unentitled, "pool-b", "tenant-b"); err == nil {
		t.Fatal("accepted a status for a tenant not entitled to the pool")
	}
	state.Ended[ReceiverPrincipal("receiver-a")] = true
	if err := AuthorizeStatus(state, keys, status, "pool-a", "tenant-a"); err == nil {
		t.Fatal("accepted a status from a receiver after its membership end")
	}
}

func TestP08ReceiverACLIsLimitedToItsPool(t *testing.T) {
	acls := ACLs(modelState(t))
	if hasACL(acls, ReceiverPrincipal("receiver-a"), PoolEventsTopic("pool-b"), "WRITE") {
		t.Fatal("receiver can write another pool's events")
	}
	if !hasACL(acls, ReceiverPrincipal("receiver-a"), PoolEventsTopic("pool-a"), "WRITE") {
		t.Fatal("receiver cannot write its pool's events")
	}
}

func TestP09RevocationRunsAllSixStepsInOrder(t *testing.T) {
	cluster, service, closeFixture := kfakeService(t)
	defer closeFixture()
	_ = cluster
	ctx := context.Background()
	terms := modelTerms()
	if err := service.PoolCreate(ctx, "pool-a", terms); err != nil {
		t.Fatal(err)
	}
	config := ReceiverConfig{Receiver: "receiver-a", Pool: "pool-a", Agents: terms.Agents, Runtimes: terms.Runtimes}
	if err := service.ReceiverAdd(ctx, config, filepath.Join(t.TempDir(), "bundle.yaml")); err != nil {
		t.Fatal(err)
	}
	wait := 500 * time.Millisecond
	started := time.Now()
	result := service.RevokeReceiver(ctx, "receiver-a", wait, func(ctx context.Context, user, password string) error {
		described, err := kadm.NewClient(service.Admin).DescribeUserSCRAMs(ctx, user)
		if err != nil {
			return err
		}
		if entry, ok := described[user]; !ok || entry.Err != nil {
			return errors.New("credential deleted before ACL probe")
		}
		state, err := service.LoadState(ctx)
		if err != nil {
			return err
		}
		if state.Ended[user] {
			return errors.New("membership end recorded before ACL probe")
		}
		return ErrAuthorizationDenied
	})
	if !result.Complete || len(result.Steps) != 6 {
		t.Fatalf("incomplete revocation: %+v", result)
	}
	for i, step := range result.Steps {
		if step.Number != i+1 || !step.Complete {
			t.Fatalf("step %d: %+v", i+1, step)
		}
	}
	if time.Since(started) < wait {
		t.Fatal("revocation skipped the interval wait")
	}
	state, err := service.LoadState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !state.Ended[ReceiverPrincipal("receiver-a")] {
		t.Fatal("revocation omitted membership end")
	}
}

func TestRealBrokerTLSListener(t *testing.T) {
	broker := os.Getenv("WHERRY_TRUST_BROKER")
	if broker == "" {
		t.Skip("WHERRY_TRUST_BROKER is not set")
	}
	password, err := os.ReadFile(os.Getenv("WHERRY_TRUST_ADMIN_PASSWORD_FILE"))
	if err != nil {
		t.Fatal(err)
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.SASL(scram.Auth{User: os.Getenv("WHERRY_TRUST_ADMIN_USER"), Pass: strings.TrimSpace(string(password))}.AsSha512Mechanism()), kgo.RequestTimeoutOverhead(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Ping(ctx); err == nil {
		t.Fatal("plaintext Kafka listener accepted a connection")
	}
}

func TestP12ProvisioningSurvivesReconciliation(t *testing.T) {
	waitText := os.Getenv("WHERRY_TRUST_RECONCILE_WAIT")
	if waitText == "" || os.Getenv("WHERRY_TRUST_BROKER") == "" {
		t.Skip("real-broker reconciliation environment is not set")
	}
	wait, err := time.ParseDuration(waitText)
	if err != nil {
		t.Fatal(err)
	}
	service, closeService := realService(t)
	defer closeService()
	suffixBytes := make([]byte, 4)
	if _, err := rand.Read(suffixBytes); err != nil {
		t.Fatal(err)
	}
	suffix := hex.EncodeToString(suffixBytes)
	pool, receiver := "p"+suffix, "r"+suffix
	ctx := context.Background()
	terms := modelTerms()
	if err := service.PoolCreate(ctx, pool, terms); err != nil {
		t.Fatal(err)
	}
	config := ReceiverConfig{Receiver: receiver, Pool: pool, Agents: terms.Agents, Runtimes: terms.Runtimes}
	bundlePath := filepath.Join(t.TempDir(), "bundle.yaml")
	if err := service.ReceiverAdd(ctx, config, bundlePath); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = service.deletePrincipalACLs(ctx, ReceiverPrincipal(receiver))
		_, _ = kadm.NewClient(service.Admin).AlterUserSCRAMs(ctx, []kadm.DeleteSCRAM{{User: ReceiverPrincipal(receiver), Mechanism: kadm.ScramSha512}}, nil)
	}()
	time.Sleep(wait)
	described, err := kadm.NewClient(service.Admin).DescribeUserSCRAMs(ctx, ReceiverPrincipal(receiver))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := described[ReceiverPrincipal(receiver)]; !ok {
		t.Fatal("provisioned receiver was removed during reconciliation")
	}
}

func TestP13ShareGroupStartsAtEarliest(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	if err := service.PoolCreate(ctx, "pool-a", modelTerms()); err != nil {
		t.Fatal(err)
	}
	if err := service.Admin.ProduceSync(ctx, &kgo.Record{Topic: PoolTasksTopic("pool-a"), Value: []byte("submitted-before-fetch")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	member, err := kgo.NewClient(kgo.SeedBrokers(service.Broker), kgo.SASL(scram.Auth{User: "admin", Pass: "password"}.AsSha512Mechanism()), kgo.ConsumeTopics(PoolTasksTopic("pool-a")), kgo.ShareGroup(PoolGroup("pool-a")), kgo.ShareMaxRecords(1))
	if err != nil {
		t.Fatal(err)
	}
	defer member.Close()
	pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	records := member.PollRecords(pollCtx, 1).Records()
	if len(records) != 1 || string(records[0].Value) != "submitted-before-fetch" {
		t.Fatalf("pre-fetch task was not delivered: %v", records)
	}
}

func TestP15ReceiverAcceptsOnlyItsRecordedConfiguration(t *testing.T) {
	terms := modelTerms()
	config := ReceiverConfig{Receiver: "receiver-b", Pool: "pool-a", Agents: terms.Agents, Runtimes: terms.Runtimes}
	record := Record{Version: 1, Kind: "receiver", Pool: "pool-a", Receiver: "receiver-b", Config: &config, AtMS: 1}
	if err := ValidateReceiverRecord(record, "receiver-a", terms); err == nil {
		t.Fatal("accepted another receiver's configuration")
	}
	config.Receiver, record.Receiver = "receiver-a", "receiver-a"
	record.Config = &config
	if err := ValidateReceiverRecord(record, "receiver-a", terms); err != nil {
		t.Fatal(err)
	}
	for name, outside := range map[string]ReceiverConfig{
		"runtime":    {Receiver: "receiver-a", Pool: "pool-a", Agents: terms.Agents, Runtimes: []string{"runtime-z"}},
		"agent":      {Receiver: "receiver-a", Pool: "pool-a", Agents: []AgentTerms{{Name: "agent-z"}}, Runtimes: terms.Runtimes},
		"credential": {Receiver: "receiver-a", Pool: "pool-a", Agents: []AgentTerms{{Name: "agent-a", Credentials: []string{"other"}}}, Runtimes: terms.Runtimes},
	} {
		record.Config = &outside
		if err := ValidateReceiverRecord(record, "receiver-a", terms); err == nil {
			t.Fatalf("accepted a configuration with a %s outside the terms", name)
		}
	}
}

func TestAdminRecordDecodeIsStrict(t *testing.T) {
	for name, encoded := range map[string]string{
		"unknown member":   `{"v":1,"kind":"host","tenant":"t","host":"h","at_ms":1,"extra":1}`,
		"duplicate member": `{"v":1,"kind":"host","tenant":"t","host":"h","host":"g","at_ms":1}`,
	} {
		if _, err := DecodeRecord([]byte(encoded)); err == nil {
			t.Fatalf("accepted an admin record with a %s", name)
		}
	}
}

func modelTerms() Terms {
	return Terms{Agents: []AgentTerms{{Name: "agent-a", Credentials: []string{"model"}}}, Runtimes: []string{"runtime-a"}, DefaultAgent: "agent-a", DefaultRuntime: "runtime-a", SealLifetimeMS: 60000, DeliveryLimit: 3, LockDurationMS: 15000, ProducerByteRate: 1000000, RequestPercentage: 50}
}

func modelState(t *testing.T) State {
	terms := modelTerms()
	records := []Record{{Version: 1, Kind: "pool", Pool: "pool-a", Terms: &terms, AtMS: 1}, {Version: 1, Kind: "pool", Pool: "pool-b", Terms: &terms, AtMS: 1}, {Version: 1, Kind: "host", Tenant: "tenant-a", Host: "host-a", AtMS: 1}, {Version: 1, Kind: "host", Tenant: "tenant-b", Host: "host-b", AtMS: 1}, {Version: 1, Kind: "entitlement", Tenant: "tenant-a", Pool: "pool-a", AtMS: 1}, {Version: 1, Kind: "entitlement", Tenant: "tenant-a", Pool: "pool-b", AtMS: 1}, {Version: 1, Kind: "entitlement", Tenant: "tenant-b", Pool: "pool-a", AtMS: 1}}
	for _, config := range []ReceiverConfig{{Receiver: "receiver-a", Pool: "pool-a", Agents: terms.Agents, Runtimes: terms.Runtimes}, {Receiver: "receiver-b", Pool: "pool-b", Agents: terms.Agents, Runtimes: terms.Runtimes}} {
		copy := config
		records = append(records, Record{Version: 1, Kind: "receiver", Pool: config.Pool, Receiver: config.Receiver, Config: &copy, AtMS: 1})
	}
	state, err := Fold(records)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func hasACL(acls []ACL, principal, resource, operation string) bool {
	for _, acl := range acls {
		if acl.Principal == principal && acl.Resource == resource {
			for _, candidate := range acl.Operations {
				if candidate == operation {
					return true
				}
			}
		}
	}
	return false
}

func kfakeService(t *testing.T) (*kfake.Cluster, *Service, func()) {
	cluster := kfake.MustCluster(kfake.EnableSASL(), kfake.EnableACLs(), kfake.Superuser("SCRAM-SHA-512", "admin", "password"), kfake.Superuser("PLAIN", "wherry.operator", "operator-password"))
	if err := cluster.CreateTopic(AdminTopic, 1, nil); err != nil {
		t.Fatal(err)
	}
	root, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.SASL(scram.Auth{User: "admin", Pass: "password"}.AsSha512Mechanism()), kgo.FetchMaxWait(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	operator, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.SASL(plain.Auth{User: "wherry.operator", Pass: "operator-password"}.AsMechanism()), kgo.FetchMaxWait(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := operator.Ping(context.Background()); err != nil {
		t.Fatalf("operator ping: %v", err)
	}
	service := &Service{Admin: root, Operator: operator, Broker: cluster.ListenAddrs()[0], DeliveryCeiling: 10}
	return cluster, service, func() { operator.Close(); root.Close(); cluster.Close() }
}

func realService(t *testing.T) (*Service, func()) {
	ca, err := os.ReadFile(os.Getenv("WHERRY_TRUST_CA"))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("invalid CA")
	}
	connect := func(user, passwordFile string) *kgo.Client {
		password, err := os.ReadFile(passwordFile)
		if err != nil {
			t.Fatal(err)
		}
		client, err := kgo.NewClient(kgo.SeedBrokers(os.Getenv("WHERRY_TRUST_BROKER")), kgo.DialTLSConfig(&tls.Config{RootCAs: roots}), kgo.SASL(scram.Auth{User: user, Pass: strings.TrimSpace(string(password))}.AsSha512Mechanism()), kgo.FetchIsolationLevel(kgo.ReadCommitted()))
		if err != nil {
			t.Fatal(err)
		}
		return client
	}
	root := connect(os.Getenv("WHERRY_TRUST_ADMIN_USER"), os.Getenv("WHERRY_TRUST_ADMIN_PASSWORD_FILE"))
	operator := connect(os.Getenv("WHERRY_TRUST_OPERATOR_USER"), os.Getenv("WHERRY_TRUST_OPERATOR_PASSWORD_FILE"))
	return &Service{Admin: root, Operator: operator, Broker: os.Getenv("WHERRY_TRUST_BROKER"), CA: string(ca)}, func() { operator.Close(); root.Close() }
}
