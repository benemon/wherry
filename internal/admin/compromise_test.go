package admin

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"io"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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

func compromiseFixture(t *testing.T) (*kfake.Cluster, *Service, context.Context) {
	t.Helper()
	cluster, service, closeFixture := kfakeService(t)
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
	return cluster, service, ctx
}

// Suspend withdraws the hosts' WRITE on the pool's task topic and nothing else: the receivers' READ, and an unrelated
// principal's READ and WRITE on the topic, are kept; resume restores the WRITE.
func TestSuspendWithdrawsTaskTopicAccessOnlyAndResumeRestoresIt(t *testing.T) {
	_, service, ctx := compromiseFixture(t)
	unrelated := kadm.NewACLs().Topics(PoolTasksTopic("pool-a")).Allow("User:mirror").AllowHosts("*").Operations(kadm.OpRead, kadm.OpWrite).ResourcePatternType(kadm.ACLPatternLiteral)
	created, err := kadm.NewClient(service.Admin).CreateACLs(ctx, unrelated)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range created {
		if result.Err != nil {
			t.Fatal(result.Err)
		}
	}
	before := brokerACLs(t, ctx, service)
	for _, operation := range []string{"READ", "WRITE"} {
		if !slices.Contains(before, "User:mirror TOPIC "+PoolTasksTopic("pool-a")+" "+operation) {
			t.Fatalf("unrelated %s ACL absent before suspend: %v", operation, before)
		}
	}
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
	want := []string{"User:" + TenantPrincipal("tenant-a", "host-a") + " TOPIC " + PoolTasksTopic("pool-a") + " WRITE"}
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
		var missing []string
		for _, acl := range before {
			if !slices.Contains(resumed, acl) {
				missing = append(missing, acl)
			}
		}
		t.Fatalf("resume left %d ACLs, want %d; missing %v", len(resumed), len(before), missing)
	}
	if state, err := service.LoadState(ctx); err != nil || state.Suspended["pool-a"] {
		t.Fatalf("state after resume %v, %v", state.Suspended, err)
	}
	if err := service.PoolSuspend(ctx, "pool-b", true); err == nil {
		t.Fatal("suspended an unknown pool")
	}
}

// The rotate-key report of a suspended pool lists every task retained under a suspect key through the rotated snapshot,
// one appended after the request included, by run, tenant and the credential names of the terms in force at its append,
// from the oldest retained offset, which it states. A pool not suspended is refused.
func TestRotateKeyReportListsRetainedTasksUnderSuspectKeys(t *testing.T) {
	cluster, service, ctx := compromiseFixture(t)
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
	// The terms renaming the agent's credential come after run-current and run-other.
	renamed := modelTerms()
	renamed.Agents[0].Credentials[0].Name = "model-renamed"
	if err := service.PoolUpdate(ctx, "pool-a", renamed, false); err != nil {
		t.Fatal(err)
	}
	if _, err := service.PoolRotateKey(ctx, "pool-a", nil, io.Discard); err == nil || !strings.Contains(err.Error(), "pool pool-a is not suspended") {
		t.Fatalf("rotate-key of a pool not suspended = %v, want a refusal naming the suspension", err)
	}
	if err := service.PoolSuspend(ctx, "pool-a", true); err != nil {
		t.Fatal(err)
	}
	// rotate runs rotate-key and plays the key custodian: once the command reports its wait, it calls between, then
	// commits a snapshot rotated for the request.
	rotations := 0
	rotate := func(suspects []string, between func()) RotateReport {
		t.Helper()
		type rotated struct {
			report RotateReport
			err    error
		}
		done, waiting := make(chan rotated, 1), &signalWriter{signal: make(chan struct{}, 1)}
		go func() {
			report, err := service.PoolRotateKey(ctx, "pool-a", suspects, waiting)
			done <- rotated{report, err}
		}()
		select {
		case <-waiting.signal:
		case result := <-done:
			t.Fatalf("rotate-key returned %+v, %v before its custodian rotated", result.report, result.err)
		}
		state, err := service.LoadState(ctx)
		if err != nil {
			t.Fatal(err)
		}
		between()
		rotations++
		next, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		snapshot := mustBytes(t)(signed.Sign(wire.CustodianType, "receiver-a", &wire.Snapshot{V: 1, Pool: "pool-a", Custodian: "receiver-a", Reason: "rotation",
			Current: wire.PoolKey{KeyID: signed.KeyThumbprint(&next.PublicKey), Key: jose.JSONWebKey{Key: &next.PublicKey}}, RotatedFor: state.RotateRequested["pool-a"]}, receiverKey))
		if err := service.Admin.ProduceSync(ctx, &kgo.Record{Topic: CustodianTopic("pool-a"), Key: []byte(wire.CustodianKey), Value: snapshot}).FirstErr(); err != nil {
			t.Fatal(err)
		}
		result := <-done
		if result.err != nil {
			t.Fatal(result.err)
		}
		if !strings.Contains(waiting.String(), "waiting for pool pool-a's key custodian to rotate for the request at wherry.admin offset ") {
			t.Fatalf("progress %q", waiting.String())
		}
		return result.report
	}
	var late *kgo.Record
	report := rotate(nil, func() { late = submit("run-late", current) })
	want := []SuspectTask{
		// Every terms record's credentials for the agent: a superset, never a miss across the terms change.
		{"run-current", "tenant-a", currentID, []string{"deploy", "model", "model-renamed"}, retained.Timestamp.UnixMilli()},
		{"run-late", "tenant-a", currentID, []string{"deploy", "model", "model-renamed"}, late.Timestamp.UnixMilli()},
	}
	if !reflect.DeepEqual(report.SuspectKeys, []string{currentID}) || !reflect.DeepEqual(report.Tasks, want) {
		t.Fatalf("report %+v, want the current key's retained tasks %+v", report, want)
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
	if named := rotate([]string{otherID, currentID}, func() {}); len(named.Tasks) != 3 || rotations != 2 {
		t.Fatalf("report under both keys %+v; want all three retained tasks", named)
	}
	// Retention moving the log start between its listing and the scan's fetch: the report states the start in effect
	// and the offsets deleted during the scan.
	var once sync.Once
	cluster.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Fetch}, Topic: retained.Topic, Observe: true, When: func(kmsg.Request) bool {
		once.Do(func() {
			if err := cluster.DeleteRecords(retained.Topic, retained.Partition, retained.Offset+1); err != nil {
				t.Error(err)
			}
		})
		return true
	}})
	moved := rotate([]string{otherID, currentID}, func() {})
	if moved.Horizon[retained.Partition] != retained.Offset+1 || moved.DeletedFrom[retained.Partition] != expired.Offset+1 ||
		slices.ContainsFunc(moved.Tasks, func(task SuspectTask) bool { return task.Run == "run-current" }) {
		t.Fatalf("horizon %v, deleted from %v, tasks %+v; want the horizon past run-current, deleted from offset %d", moved.Horizon, moved.DeletedFrom, moved.Tasks, expired.Offset+1)
	}
}

// signalWriter keeps what is written to it and signals the first write.
type signalWriter struct {
	mu     sync.Mutex
	buffer bytes.Buffer
	signal chan struct{}
}

func (w *signalWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	select {
	case w.signal <- struct{}{}:
	default:
	}
	return w.buffer.Write(p)
}

func (w *signalWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buffer.String()
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
