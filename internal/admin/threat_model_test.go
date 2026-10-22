package admin

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
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
	jose "github.com/go-jose/go-jose/v4"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"gopkg.in/yaml.v3"
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

func TestTaskKeyMustBelongToSubmittingTenant(t *testing.T) {
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

func TestCancelMustMatchRunTenant(t *testing.T) {
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

func TestTenantFeedACLIsIsolated(t *testing.T) {
	state := modelState(t)
	if hasACL(ACLs(state), TenantPrincipal("tenant-b", "host-b"), TenantTopic("tenant-a"), "READ") {
		t.Fatal("second tenant can read the first tenant's feed")
	}
	if !hasACL(ACLs(state), TenantPrincipal("tenant-a", "host-a"), TenantTopic("tenant-a"), "READ") {
		t.Fatal("own tenant feed read ACL missing")
	}
}

func TestConstructedNamesDoNotAlias(t *testing.T) {
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

func TestStatusBindsPlacedPool(t *testing.T) {
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

func TestReceiverACLIsLimitedToItsPool(t *testing.T) {
	acls := ACLs(modelState(t))
	if hasACL(acls, ReceiverPrincipal("receiver-a"), PoolEventsTopic("pool-b"), "WRITE") {
		t.Fatal("receiver can write another pool's events")
	}
	if !hasACL(acls, ReceiverPrincipal("receiver-a"), PoolEventsTopic("pool-a"), "WRITE") {
		t.Fatal("receiver cannot write its pool's events")
	}
}

func TestRevocationRunsAllSixStepsInOrder(t *testing.T) {
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

func TestProvisioningSurvivesReconciliation(t *testing.T) {
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

func TestShareGroupStartsAtEarliest(t *testing.T) {
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

func TestReceiverAcceptsOnlyItsRecordedConfiguration(t *testing.T) {
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
	badLabels, outsideAgent, outsideCredential, unknownOutput := modelConfig("receiver-a", "pool-a"), modelConfig("receiver-a", "pool-a"), modelConfig("receiver-a", "pool-a"), modelConfig("receiver-a", "pool-a")
	badLabels.Adapters[0].Labels = map[string]string{"wherry.io/gpu": "yes"}
	outsideAgent.Adapters[0].Agent = "agent-z"
	outsideCredential.Adapters[0].Credential = "other"
	unknownOutput.Adapters[0].Output = "other"
	for name, outside := range map[string]ReceiverConfig{
		"reserved label": badLabels,
		"agent":          outsideAgent,
		"credential":     outsideCredential,
		"output":         unknownOutput,
	} {
		record.Config = &outside
		if err := ValidateReceiverRecord(record, "receiver-a", terms); err == nil {
			t.Fatalf("accepted a configuration with a %s outside the terms", name)
		}
	}
}

func TestAdapterReplyFile(t *testing.T) {
	for _, test := range []struct {
		output, reply string
		ok            bool
	}{
		{"file", "", true},
		{"file", "out/reply.txt", true},
		{"file", "../reply.md", false},
		{"", "reply.md", false},
		{"claude", "reply.md", false},
		{"codex", "reply.md", false},
	} {
		config := modelConfig("receiver-a", "pool-a")
		config.Adapters[0].Output, config.Adapters[0].ReplyFile = test.output, test.reply
		if err := ValidateConfig(config, modelTerms()); (err == nil) != test.ok {
			t.Fatalf("output %q reply_file %q: err %v, want ok %t", test.output, test.reply, err, test.ok)
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
	return Terms{Agents: []AgentTerms{{Name: "agent-a", Credentials: []string{"model"}}}, DefaultAgent: "agent-a", DeliveryLimit: 3, LockDurationMS: 15000, ProducerByteRate: 1000000, RequestPercentage: 50}
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
	if err := cluster.CreateTopic(AdminTopic, 1, map[string]string{"retention.ms": "-1"}); err != nil {
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

func TestLoadNamesALaterReleasesRecord(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	if err := service.PoolCreate(ctx, "pool-a", modelTerms()); err != nil {
		t.Fatal(err)
	}
	raw, err := readRaw(ctx, service.Operator)
	if err != nil {
		t.Fatal(err)
	}
	later := append([]byte(`{"later_member":true,`), raw[0].Value[1:]...)
	if err := service.Operator.ProduceSync(ctx, &kgo.Record{Topic: AdminTopic, Value: later}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if _, err := service.LoadState(ctx); !errors.Is(err, ErrNewerRecord) || strings.Contains(err.Error(), "wherry admin skip") {
		t.Fatalf("load over a later release's record: %v; want ErrNewerRecord and no skip advice", err)
	}
	raw, err = readRaw(ctx, service.Operator)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(later)
	if err := service.Skip(ctx, raw[len(raw)-1].Offset, hex.EncodeToString(sum[:])); !errors.Is(err, ErrNewerRecord) {
		t.Fatalf("skip of a later release's record: %v; want ErrNewerRecord", err)
	}
	if err := service.Operator.ProduceSync(ctx, &kgo.Record{Topic: AdminTopic, Value: []byte("not a record")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if _, err := service.LoadState(ctx); errors.Is(err, ErrNewerRecord) || err == nil || !strings.Contains(err.Error(), "wherry admin skip") {
		t.Fatalf("load over a later release's record and a corrupt one: %v; want the skip advice", err)
	}
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

func TestEchoAdvertisementAndSealRequestBindTheirPool(t *testing.T) {
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
	advertisement := func(pool string) []byte {
		return sign(t, &wire.Advertisement{Receiver: "receiver-a", Pool: pool, Offers: []wire.Offer{{Adapter: "nomad", Agent: "agent-a"}}, Capacity: 1, AtMS: 1}, key)
	}
	if _, err := AuthorizeAdvertisement(state, keys, advertisement("pool-a")); err != nil {
		t.Fatal(err)
	}
	sealKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	request := func(tenant, pool string) []byte {
		return sign(t, &wire.SealRequest{ReceiverBinding: wire.ReceiverBinding{Receiver: "receiver-a", Tenant: tenant, Pool: pool, Run: "run-a", Turn: 1, Attempt: 1},
			Key: jose.JSONWebKey{Key: &sealKey.PublicKey}, KeyID: signed.SealKeyID(&sealKey.PublicKey), Credentials: []string{"model"}, ExpiryMS: 1}, key)
	}
	if _, err := AuthorizeSealRequest(state, keys, request("tenant-a", "pool-a"), "pool-a", "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := AuthorizeSealRequest(state, keys, request("tenant-a", "pool-b"), "pool-b", "tenant-a"); err == nil {
		t.Fatal("accepted a seal request for a pool the receiver is not in")
	}
	if _, err := AuthorizeSealRequest(state, keys, request("tenant-b", "pool-a"), "pool-a", "tenant-a"); err == nil {
		t.Fatal("accepted a seal request naming another tenant than its run")
	}
	if _, err := AuthorizeAdvertisement(state, keys, advertisement("pool-b")); err == nil {
		t.Fatal("accepted an advertisement for a pool the receiver is not in")
	}
	state.Ended[ReceiverPrincipal("receiver-a")] = true
	if _, err := AuthorizeAdvertisement(state, keys, advertisement("pool-a")); err == nil {
		t.Fatal("accepted an advertisement after the receiver's membership end")
	}
	if _, err := AuthorizeSealRequest(state, keys, request("tenant-a", "pool-a"), "pool-a", "tenant-a"); err == nil {
		t.Fatal("accepted a seal request after the receiver's membership end")
	}
}

func TestReceiverConfigureKeepsThePrincipal(t *testing.T) {
	cluster, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	if err := service.PoolCreate(ctx, "pool-a", modelTerms()); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "bundle.yaml")
	if err := service.ReceiverAdd(ctx, modelConfig("receiver-a", "pool-a"), nil, path); err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var bundle Bundle
	if err := yaml.Unmarshal(encoded, &bundle); err != nil {
		t.Fatal(err)
	}
	// kfake's SCRAM handshake always names 4096 iterations.
	upsert := kadm.UpsertSCRAM{User: bundle.Principal, Mechanism: kadm.ScramSha512, Iterations: 4096, Password: bundle.Password}
	if altered, err := kadm.NewClient(service.Admin).AlterUserSCRAMs(ctx, nil, []kadm.UpsertSCRAM{upsert}); err != nil || altered.Error() != nil {
		t.Fatalf("re-salt: %v %v", err, altered.Error())
	}
	login := func() error {
		client, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.SASL(scram.Auth{User: bundle.Principal, Pass: bundle.Password}.AsSha512Mechanism()))
		if err != nil {
			return err
		}
		defer client.Close()
		return client.Ping(ctx)
	}
	if err := login(); err != nil {
		t.Fatal(err)
	}
	changed := modelConfig("receiver-a", "pool-b")
	changed.Capacity = 3
	if err := service.ReceiverConfigure(ctx, changed); err != nil {
		t.Fatal(err)
	}
	state, err := service.LoadState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if config := state.Receivers["receiver-a"]; config.Capacity != 3 || config.Pool != "pool-a" {
		t.Fatalf("configuration %+v; want capacity 3 in the receiver's own pool", config)
	}
	if err := login(); err != nil {
		t.Fatalf("configure changed the receiver's credential: %v", err)
	}
	if err := service.ReceiverAdd(ctx, modelConfig("receiver-a", "pool-a"), nil, filepath.Join(t.TempDir(), "again.yaml")); err == nil {
		t.Fatal("re-added an existing receiver, rotating its password")
	}
	if err := service.ReceiverConfigure(ctx, modelConfig("receiver-z", "pool-a")); err == nil {
		t.Fatal("configured a receiver that was never added")
	}
}

func TestRevocationProbeRetriesOnlyWhileABrokerAuthorises(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	if err := service.PoolCreate(ctx, "pool-a", modelTerms()); err != nil {
		t.Fatal(err)
	}
	if err := service.ReceiverAdd(ctx, modelConfig("receiver-a", "pool-a"), nil, filepath.Join(t.TempDir(), "bundle.yaml")); err != nil {
		t.Fatal(err)
	}
	window, interval := probeWindow, probeInterval
	defer func() { probeWindow, probeInterval = window, interval }()
	probeInterval = time.Millisecond
	probe := func(answers ...error) (func(context.Context, string, string) error, *int, *string) {
		calls, password := 0, ""
		return func(_ context.Context, _, rotated string) error {
			password = rotated
			calls++
			return answers[min(calls, len(answers))-1]
		}, &calls, &password
	}
	stepThree := func(result RevocationResult) RevocationStep {
		if len(result.Steps) < 3 {
			t.Fatalf("revocation stopped before the probe: %+v", result)
		}
		return result.Steps[2]
	}

	lagging, calls, _ := probe(ErrStillAuthorised, ErrStillAuthorised, ErrAuthorizationDenied)
	if step := stepThree(service.RevokeReceiver(ctx, "receiver-a", time.Millisecond, lagging)); !step.Complete || *calls != 3 {
		t.Fatalf("lagging broker: step %+v after %d probes; want complete after 3", step, *calls)
	}
	if err := service.ReceiverAdd(ctx, modelConfig("receiver-b", "pool-a"), nil, filepath.Join(t.TempDir(), "bundle.yaml")); err != nil {
		t.Fatal(err)
	}
	login, calls, password := probe(ErrAuthenticationFailure)
	step := stepThree(service.RevokeReceiver(ctx, "receiver-b", time.Millisecond, login))
	if step.Complete || *calls != 1 {
		t.Fatalf("failed login: step %+v after %d probes; want one failed probe", step, *calls)
	}
	if *password == "" || strings.Contains(step.Err.Error(), *password) {
		t.Fatal("a revocation step error carries the rotated password")
	}
	probeWindow = 5 * time.Millisecond
	stuck, _, _ := probe(ErrStillAuthorised)
	if step := stepThree(service.RevokeReceiver(ctx, "receiver-b", time.Millisecond, stuck)); step.Complete || !errors.Is(step.Err, ErrStillAuthorised) {
		t.Fatalf("broker never denies: step %+v; want failure after the probe window", step)
	}
}

// One grammar holds for every name: one admin issues and the wire records carry must not be valid to one side alone.
func TestNameGrammarIsOneOnBothSides(t *testing.T) {
	for name, want := range map[string]bool{
		"a": true, "pool-a": true, "0": true, strings.Repeat("a", 32): true,
		"": false, "-a": false, "a-": false, "A": false, "a.b": false, "a_b": false, strings.Repeat("a", 33): false,
	} {
		if got := wire.ValidName(name); got != want {
			t.Errorf("wire.ValidName(%q) = %v, want %v", name, got, want)
		}
		if got := validateNames(name) == nil; got != want {
			t.Errorf("admin accepts %q = %v, want %v", name, got, want)
		}
	}
}

func TestKafkaOptsRefuseACAWithoutCertificates(t *testing.T) {
	if _, err := KafkaOpts("127.0.0.1:1", []byte("not a certificate"), "user", "password"); err == nil {
		t.Fatal("built options trusting no certificate")
	}
}

func TestKafkaOptsRefuseAPasswordOfALineBreak(t *testing.T) {
	if _, err := KafkaOpts("127.0.0.1:1", nil, "user", "\r\n"); err == nil || err.Error() != "password is empty" {
		t.Fatalf("options for a password of a line break: %v; want it refused as empty", err)
	}
}
