package admin

import (
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
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
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func testSubmission(tenant, host, pool, run string) *wire.Submission {
	return &wire.Submission{
		Message: wire.Message{MessageID: "message-" + run, ContextID: run, TaskID: run, Role: wire.RoleUser, Parts: []wire.Part{{Text: "hello"}}, Extensions: []string{wire.ExtensionURI}},
		Wherry:  wire.SubmissionBinding{Tenant: tenant, Host: host, Pool: pool, Run: run, Turn: 1, Placement: wire.Placement{Agent: "agent-a"}},
	}
}

func testStatus(receiver, tenant, pool, run string) *wire.RunStatus {
	return &wire.RunStatus{
		StatusUpdate: wire.TaskStatusUpdateEvent{TaskID: run, ContextID: run, Status: wire.TaskStatus{State: wire.TaskStateWorking, Timestamp: "2026-10-19T10:00:00Z"}},
		Wherry:       wire.StatusBinding{ReceiverBinding: wire.ReceiverBinding{Receiver: receiver, Tenant: tenant, Pool: pool, Run: run, Turn: 1, Attempt: 1}, EventID: "event-" + run, Kind: "started", TaskHash: strings.Repeat("a", 64)},
	}
}

func sign(t *testing.T, record wire.Record, key ed25519.PrivateKey) []byte {
	t.Helper()
	encoded, err := wire.SignRecord(record, key)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestP01TaskKeyMustBelongToSubmittingTenant(t *testing.T) {
	state := modelState(t)
	_, own, _ := ed25519.GenerateKey(rand.Reader)
	_, foreign, _ := ed25519.GenerateKey(rand.Reader)
	keys := signed.Keys{signed.HostOwner("tenant-a", "host-a"): {Current: own.Public().(ed25519.PublicKey)}, signed.HostOwner("tenant-b", "host-b"): {Current: foreign.Public().(ed25519.PublicKey)}}
	task := sign(t, testSubmission("tenant-a", "host-a", "pool-a", "run-a"), own)
	if _, err := AuthorizeSubmission(state, keys, task, "pool-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := AuthorizeSubmission(state, keys, sign(t, testSubmission("tenant-a", "host-a", "pool-a", "run-a"), foreign), "pool-a"); err == nil {
		t.Fatal("accepted a task signed by a key on another tenant's host topic")
	}
	if _, err := AuthorizeSubmission(state, keys, sign(t, testSubmission("tenant-a", "host-b", "pool-a", "run-a"), foreign), "pool-a"); err == nil {
		t.Fatal("accepted a task naming a host its tenant does not own")
	}
	if _, err := AuthorizeSubmission(state, keys, task, "pool-b"); err == nil {
		t.Fatal("accepted a task on a pool topic it does not name")
	}
	keys[signed.HostOwner("tenant-a", "host-z")] = signed.KeyState{Current: own.Public().(ed25519.PublicKey)}
	if _, err := AuthorizeSubmission(state, keys, sign(t, testSubmission("tenant-a", "host-z", "pool-a", "run-a"), own), "pool-a"); err == nil {
		t.Fatal("accepted a task from a host with no host-to-tenant mapping")
	}
	if _, err := AuthorizeSubmission(state, keys, sign(t, testSubmission("tenant-b", "host-b", "pool-b", "run-b"), foreign), "pool-b"); err == nil {
		t.Fatal("accepted a task for a pool the tenant is not entitled to")
	}
	state.Ended[TenantPrincipal("tenant-a", "host-a")] = true
	if _, err := AuthorizeSubmission(state, keys, task, "pool-a"); err == nil {
		t.Fatal("accepted a task from a host after its membership end")
	}
}

func TestP02CancelMustMatchRunTenant(t *testing.T) {
	state := modelState(t)
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	keys := signed.Keys{signed.HostOwner("tenant-b", "host-b"): {Current: key.Public().(ed25519.PublicKey)}}
	cancel := sign(t, &wire.Cancel{Tenant: "tenant-b", Host: "host-b", Pool: "pool-a", Run: "run-a", Turn: 1}, key)
	if _, err := AuthorizeCancel(state, keys, cancel, "pool-a", "tenant-b"); err != nil {
		t.Fatal(err)
	}
	if _, err := AuthorizeCancel(state, keys, cancel, "pool-a", "tenant-a"); err == nil {
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
	status := sign(t, testStatus("receiver-a", "tenant-a", "pool-a", "run-a"), key)
	if _, err := AuthorizeStatus(state, keys, status, "pool-a", "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := AuthorizeStatus(state, keys, status, "pool-b", "tenant-a"); err == nil {
		t.Fatal("accepted pool-a receiver status for a pool-b run")
	}
	if _, err := AuthorizeStatus(state, keys, status, "pool-a", "tenant-b"); err == nil {
		t.Fatal("accepted a status moved to another tenant's run")
	}
	if _, err := AuthorizeStatus(state, keys, sign(t, testStatus("receiver-b", "tenant-a", "pool-a", "run-a"), key), "pool-a", "tenant-a"); err == nil {
		t.Fatal("accepted a status naming a receiver whose key did not sign it")
	}
	if _, err := AuthorizeStatus(state, keys, sign(t, testStatus("receiver-a", "tenant-a", "pool-b", "run-b"), key), "pool-b", "tenant-a"); err == nil {
		t.Fatal("accepted a pool-b status from a pool-a receiver")
	}
	keys["receiver-b"] = signed.KeyState{Current: key.Public().(ed25519.PublicKey)}
	if _, err := AuthorizeStatus(state, keys, sign(t, testStatus("receiver-b", "tenant-b", "pool-b", "run-b"), key), "pool-b", "tenant-b"); err == nil {
		t.Fatal("accepted a status for a tenant not entitled to the pool")
	}
	state.Ended[ReceiverPrincipal("receiver-a")] = true
	if _, err := AuthorizeStatus(state, keys, status, "pool-a", "tenant-a"); err == nil {
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
	config := modelConfig("receiver-a", "pool-a")
	if err := service.ReceiverAdd(ctx, config, nil, filepath.Join(t.TempDir(), "bundle.yaml")); err != nil {
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
	config := modelConfig(receiver, pool)
	bundlePath := filepath.Join(t.TempDir(), "bundle.yaml")
	if err := service.ReceiverAdd(ctx, config, nil, bundlePath); err != nil {
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
	config := modelConfig("receiver-b", "pool-a")
	record := Record{Version: 1, Kind: "receiver", Pool: "pool-a", Receiver: "receiver-b", Config: &config, AtMS: 1}
	if err := ValidateReceiverRecord(record, "receiver-a", terms); err == nil {
		t.Fatal("accepted another receiver's configuration")
	}
	config.Receiver, record.Receiver = "receiver-a", "receiver-a"
	record.Config = &config
	if err := ValidateReceiverRecord(record, "receiver-a", terms); err != nil {
		t.Fatal(err)
	}
	badLabels, outsideAgent, outsideCredential := modelConfig("receiver-a", "pool-a"), modelConfig("receiver-a", "pool-a"), modelConfig("receiver-a", "pool-a")
	badLabels.Adapters[0].Labels = map[string]string{"wherry.io/gpu": "yes"}
	outsideAgent.Adapters[0].Agent = "agent-z"
	outsideCredential.Adapters[0].Credential = "other"
	for name, outside := range map[string]ReceiverConfig{
		"reserved label": badLabels,
		"agent":          outsideAgent,
		"credential":     outsideCredential,
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
	return Terms{Agents: []AgentTerms{{Name: "agent-a", Credentials: []string{"model"}}}, DefaultAgent: "agent-a", SealLifetimeMS: 60000, DeliveryLimit: 3, LockDurationMS: 15000, ProducerByteRate: 1000000, RequestPercentage: 50}
}

func modelConfig(receiver, pool string) ReceiverConfig {
	return ReceiverConfig{Receiver: receiver, Pool: pool, Capacity: 1, Adapters: []AdapterSettings{{Name: "adapter-a", Type: "fake", Agent: "agent-a", Credential: "model"}}}
}

func modelState(t *testing.T) State {
	terms := modelTerms()
	records := []Record{{Version: 1, Kind: "pool", Pool: "pool-a", Terms: &terms, AtMS: 1}, {Version: 1, Kind: "pool", Pool: "pool-b", Terms: &terms, AtMS: 1}, {Version: 1, Kind: "host", Tenant: "tenant-a", Host: "host-a", AtMS: 1}, {Version: 1, Kind: "host", Tenant: "tenant-b", Host: "host-b", AtMS: 1}, {Version: 1, Kind: "entitlement", Tenant: "tenant-a", Pool: "pool-a", AtMS: 1}, {Version: 1, Kind: "entitlement", Tenant: "tenant-a", Pool: "pool-b", AtMS: 1}, {Version: 1, Kind: "entitlement", Tenant: "tenant-b", Pool: "pool-a", AtMS: 1}}
	for _, config := range []ReceiverConfig{modelConfig("receiver-a", "pool-a"), modelConfig("receiver-b", "pool-b")} {
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

func TestAdminSkipRecoversOnlyUndecodableRecords(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	if err := service.PoolCreate(ctx, "pool-a", modelTerms()); err != nil {
		t.Fatal(err)
	}
	bad := []byte("not a record")
	produced := service.Operator.ProduceSync(ctx, &kgo.Record{Topic: AdminTopic, Value: bad})
	if err := produced.FirstErr(); err != nil {
		t.Fatal(err)
	}
	badOffset := produced[0].Record.Offset
	sum := sha256.Sum256(bad)
	hash := hex.EncodeToString(sum[:])
	if _, err := service.LoadState(ctx); err == nil || !strings.Contains(err.Error(), "sha256="+hash) {
		t.Fatalf("loaded state over an uncovered undecodable record: %v", err)
	}
	raw, err := readRaw(ctx, service.Operator)
	if err != nil {
		t.Fatal(err)
	}
	validSum := sha256.Sum256(raw[0].Value)
	if err := service.Skip(ctx, raw[0].Offset, hex.EncodeToString(validSum[:])); err == nil {
		t.Fatal("skipped a record that decodes")
	}
	if err := service.Skip(ctx, badOffset, strings.Repeat("0", 64)); err == nil {
		t.Fatal("skipped with the wrong hash")
	}
	if err := service.Skip(ctx, badOffset+100, hash); err == nil {
		t.Fatal("skipped an offset that does not exist")
	}
	misbound, err := EncodeRecord(Record{Kind: "skip", Offset: badOffset, SHA256: strings.Repeat("0", 64)})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Operator.ProduceSync(ctx, &kgo.Record{Topic: AdminTopic, Value: misbound}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if _, err := service.LoadState(ctx); err == nil {
		t.Fatal("accepted a skip record whose hash does not match")
	}
	forged, err := EncodeRecord(Record{Kind: "skip", Offset: raw[0].Offset, SHA256: hex.EncodeToString(validSum[:])})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Operator.ProduceSync(ctx, &kgo.Record{Topic: AdminTopic, Value: forged}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if _, err := service.LoadState(ctx); err == nil {
		t.Fatal("accepted a skip record naming a decodable record")
	}
}

func TestAdminSkipRestoresState(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	if err := service.PoolCreate(ctx, "pool-a", modelTerms()); err != nil {
		t.Fatal(err)
	}
	bad := []byte("not a record")
	produced := service.Operator.ProduceSync(ctx, &kgo.Record{Topic: AdminTopic, Value: bad})
	if err := produced.FirstErr(); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(bad)
	if err := service.Skip(ctx, produced[0].Record.Offset, hex.EncodeToString(sum[:])); err != nil {
		t.Fatal(err)
	}
	state, err := service.LoadState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := state.Pools["pool-a"]; !ok {
		t.Fatal("state lost the pool after the skip")
	}
	if err := service.TenantAdd(ctx, "tenant-a", "pool-a"); err != nil {
		t.Fatalf("admin commands still fail after the skip: %v", err)
	}
}

func TestEchoAndAdvertisementBindTheirPool(t *testing.T) {
	state := modelState(t)
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	keys := signed.Keys{"receiver-a": {Current: key.Public().(ed25519.PublicKey)}}
	echo := func(tenant, pool string) []byte {
		return sign(t, &wire.Echo{Receiver: "receiver-a", Tenant: tenant, Pool: pool, Run: "run-a", Turn: 1, Placement: wire.Placement{Agent: "agent-a"}, TaskHash: strings.Repeat("a", 64), MessageID: "message-a"}, key)
	}
	if _, err := AuthorizeEcho(state, keys, echo("tenant-a", "pool-a"), "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := AuthorizeEcho(state, keys, echo("tenant-a", "pool-a"), "tenant-b"); err == nil {
		t.Fatal("accepted an echo on another tenant's feed")
	}
	if _, err := AuthorizeEcho(state, keys, echo("tenant-a", "pool-b"), "tenant-a"); err == nil {
		t.Fatal("accepted an echo placing a run in a pool the receiver is not in")
	}
	sealing, _ := ecdh.X25519().GenerateKey(rand.Reader)
	keys["receiver-a"] = signed.KeyState{Current: key.Public().(ed25519.PublicKey), Sealing: sealing.PublicKey()}
	fingerprint := signed.SealingKeyID(sealing.PublicKey())
	advertisement := func(pool string) []byte {
		return sign(t, &wire.Advertisement{Receiver: "receiver-a", Pool: pool, Offers: []wire.Offer{{Adapter: "nomad", Agent: "agent-a"}}, Capacity: 1, SealingKey: fingerprint, AtMS: 1}, key)
	}
	if _, err := AuthorizeAdvertisement(state, keys, advertisement("pool-a")); err != nil {
		t.Fatal(err)
	}
	fingerprint = "sha256:other"
	if _, err := AuthorizeAdvertisement(state, keys, advertisement("pool-a")); err == nil {
		t.Fatal("accepted an advertisement naming a sealing key the receiver has not registered")
	}
	fingerprint = signed.SealingKeyID(sealing.PublicKey())
	if _, err := AuthorizeAdvertisement(state, keys, advertisement("pool-b")); err == nil {
		t.Fatal("accepted an advertisement for a pool the receiver is not in")
	}
	state.Ended[ReceiverPrincipal("receiver-a")] = true
	if _, err := AuthorizeAdvertisement(state, keys, advertisement("pool-a")); err == nil {
		t.Fatal("accepted an advertisement after the receiver's membership end")
	}
}
