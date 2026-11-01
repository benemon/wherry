package admin

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// brokerACLs lists every ACL the broker holds as principal, resource type, resource and operation.
func brokerACLs(t *testing.T, ctx context.Context, service *Service) []string {
	t.Helper()
	builder := kadm.NewACLs().AnyResource().Allow().AllowHosts().Operations(kadm.OpAny).ResourcePatternType(kadm.ACLPatternAny)
	results, err := kadm.NewClient(service.Admin).DescribeACLs(ctx, builder)
	if err != nil {
		t.Fatal(err)
	}
	var acls []string
	for _, result := range results {
		if result.Err != nil {
			t.Fatal(result.Err)
		}
		for _, described := range result.Described {
			acls = append(acls, described.Principal+" "+described.Type.String()+" "+described.Name+" "+described.Operation.String())
		}
	}
	slices.Sort(acls)
	return slices.Compact(acls)
}

func compromiseFixture(t *testing.T) (*Service, context.Context) {
	t.Helper()
	_, service, closeFixture := kfakeService(t)
	t.Cleanup(closeFixture)
	ctx := context.Background()
	if err := service.PoolCreate(ctx, "pool-a", modelTerms()); err != nil {
		t.Fatal(err)
	}
	if err := service.TenantAdd(ctx, "tenant-a", "pool-a"); err != nil {
		t.Fatal(err)
	}
	if err := service.HostAdd(ctx, "tenant-a", "host-a", filepath.Join(t.TempDir(), "host.yaml")); err != nil {
		t.Fatal(err)
	}
	if err := service.ReceiverAdd(ctx, modelConfig("receiver-a", "pool-a"), filepath.Join(t.TempDir(), "receiver.yaml")); err != nil {
		t.Fatal(err)
	}
	return service, ctx
}

// Suspend withdraws the hosts' WRITE and the receivers' READ on the pool's task topic and nothing else; resume restores
// both.
func TestSuspendWithdrawsTaskTopicAccessOnlyAndResumeRestoresIt(t *testing.T) {
	service, ctx := compromiseFixture(t)
	before := brokerACLs(t, ctx, service)
	if err := service.PoolSuspend(ctx, "pool-a", true); err != nil {
		t.Fatal(err)
	}
	state, err := service.LoadState(ctx)
	if err != nil || !state.Suspended["pool-a"] {
		t.Fatalf("state after suspend %v, %v", state.Suspended, err)
	}
	suspended := brokerACLs(t, ctx, service)
	var withdrawn []string
	for _, acl := range before {
		if !slices.Contains(suspended, acl) {
			withdrawn = append(withdrawn, acl)
		}
	}
	want := []string{
		"User:" + ReceiverPrincipal("receiver-a") + " TOPIC " + PoolTasksTopic("pool-a") + " READ",
		"User:" + TenantPrincipal("tenant-a", "host-a") + " TOPIC " + PoolTasksTopic("pool-a") + " WRITE",
	}
	slices.Sort(want)
	if !reflect.DeepEqual(withdrawn, want) || len(suspended) != len(before)-len(want) {
		t.Fatalf("suspend withdrew %v, want exactly %v", withdrawn, want)
	}
	if err := service.Check(ctx); err != nil {
		t.Fatalf("check of a suspended pool: %v", err)
	}
	if err := service.PoolSuspend(ctx, "pool-a", false); err != nil {
		t.Fatal(err)
	}
	if resumed := brokerACLs(t, ctx, service); !reflect.DeepEqual(resumed, before) {
		t.Fatalf("resume left ACLs %v, want %v", resumed, before)
	}
	if state, err := service.LoadState(ctx); err != nil || state.Suspended["pool-a"] {
		t.Fatalf("state after resume %v, %v", state.Suspended, err)
	}
	if err := service.PoolSuspend(ctx, "pool-b", true); err == nil {
		t.Fatal("suspended an unknown pool")
	}
}

// The rotate-key report lists every retained task under a suspect key by run, tenant and credential names, from the
// oldest retained offset, which it states; the record names its append time.
func TestRotateKeyReportListsRetainedTasksUnderSuspectKeys(t *testing.T) {
	service, ctx := compromiseFixture(t)
	_, receiverKey, _ := ed25519.GenerateKey(rand.Reader)
	if err := service.Admin.ProduceSync(ctx, &kgo.Record{Topic: ReceiverTopic("receiver-a"), Key: []byte("receiver-a"), Value: mustBytes(t)(signed.RegisterKey("receiver-a", receiverKey))}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	current, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	other, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	currentID, otherID := signed.KeyThumbprint(&current.PublicKey), signed.KeyThumbprint(&other.PublicKey)
	snapshot := mustBytes(t)(signed.Sign(wire.CustodianType, "receiver-a", &wire.Snapshot{V: 1, Pool: "pool-a", Custodian: "receiver-a", Reason: "bootstrap",
		Current: wire.PoolKey{KeyID: currentID, Key: jose.JSONWebKey{Key: &current.PublicKey}}}, receiverKey))
	if err := service.Admin.ProduceSync(ctx, &kgo.Record{Topic: CustodianTopic("pool-a"), Key: []byte(wire.CustodianKey), Value: snapshot}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	_, hostKey, _ := ed25519.GenerateKey(rand.Reader)
	tasks := map[string]string{}
	submit := func(run string, poolKey *ecdsa.PrivateKey) *kgo.Record {
		contentKey := make([]byte, 32)
		_, _ = rand.Read(contentKey)
		binding := wire.SubmissionBinding{Tenant: "tenant-a", Host: "host-a", Pool: "pool-a", Run: run, Placement: wire.Placement{Agent: "agent-a", Selector: []wire.Requirement{}},
			TaskStartedBy: time.Now().Add(time.Hour).Unix(), TaskEndedBy: time.Now().Add(time.Hour).Unix(), Credentials: []wire.TaskCredential{{Name: "deploy", Env: "DEPLOY_TOKEN"}}}
		values := signed.EnvelopeValues{Binding: binding.EnvelopeBinding(), Credentials: map[string][]byte{"model": []byte("model-secret-value"), "deploy": []byte("deploy-secret-value")},
			S3Inputs: map[string]string{}, S3Outputs: map[string]string{}, RedactionKey: make([]byte, 32)}
		binding.Envelope = string(mustBytes(t)(signed.BuildEnvelope(values, contentKey)))
		binding.Wrap = string(mustBytes(t)(signed.WrapContentKey(contentKey, &poolKey.PublicKey)))
		tasks[run] = binding.Envelope + binding.Wrap
		submission := &wire.Submission{Message: wire.Message{MessageID: "msg-" + run, ContextID: run, TaskID: run, Role: wire.RoleUser, Parts: []wire.Part{{Text: "hi"}}, Extensions: []string{wire.ExtensionURI}}, Wherry: binding}
		record := &kgo.Record{Topic: PoolTasksTopic("pool-a"), Key: []byte(run), Value: mustBytes(t)(wire.SignRecord(submission, hostKey))}
		if err := service.Admin.ProduceSync(ctx, record).FirstErr(); err != nil {
			t.Fatal(err)
		}
		return record
	}
	expired := submit("run-expired", current)
	deleted, err := kadm.NewClient(service.Admin).DeleteRecords(ctx, kadm.Offsets{expired.Topic: {expired.Partition: {Topic: expired.Topic, Partition: expired.Partition, At: expired.Offset + 1}}})
	if err == nil {
		err = deleted.Error()
	}
	if err != nil {
		t.Fatal(err)
	}
	retained := submit("run-current", current)
	submit("run-other", other)
	requested := time.Now()
	report, err := service.PoolRotateKey(ctx, "pool-a", nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []SuspectTask{{"run-current", "tenant-a", currentID, []string{"deploy", "model"}, retained.Timestamp.UnixMilli()}}
	if !reflect.DeepEqual(report.SuspectKeys, []string{currentID}) || !reflect.DeepEqual(report.Tasks, want) {
		t.Fatalf("report %+v, want the current key's retained task %+v", report, want)
	}
	if report.Horizon[expired.Partition] != expired.Offset+1 || report.OldestMS != retained.Timestamp.UnixMilli() {
		t.Fatalf("horizon %v from %d, want partition %d from offset %d, the oldest retained task's time", report.Horizon, report.OldestMS, expired.Partition, expired.Offset+1)
	}
	for _, task := range report.Tasks {
		for _, value := range append([]string{"model-secret-value", "deploy-secret-value", tasks[task.Run]}, strings.Split(tasks[task.Run], ".")...) {
			if value != "" && strings.Contains(strings.Join(append(task.Credentials, task.Run, task.Tenant, task.KeyID), " "), value) {
				t.Fatalf("report lists a value of task %s", task.Run)
			}
		}
	}
	state, err := service.LoadState(ctx)
	if err != nil || state.RotateRequested["pool-a"] < requested.Add(-time.Second).UnixMilli() {
		t.Fatalf("rotate-key request at %d, %v; want its append time", state.RotateRequested["pool-a"], err)
	}
	named, err := service.PoolRotateKey(ctx, "pool-a", []string{otherID, currentID})
	if err != nil || len(named.Tasks) != 2 {
		t.Fatalf("report under both keys %+v, %v; want both retained tasks", named, err)
	}
}

func mustBytes(t *testing.T) func([]byte, error) []byte {
	return func(value []byte, err error) []byte {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
}
