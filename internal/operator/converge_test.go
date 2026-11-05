package operator

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// afterRecord runs act once, when the first record of kind is on wherry.operator, as a concurrent command would.
func afterRecord(t *testing.T, kind string, act func() error) {
	t.Helper()
	var err error
	appended = func(record Record) {
		if record.Kind == kind {
			appended = func(Record) {}
			err = act()
		}
	}
	t.Cleanup(func() {
		appended = func(Record) {}
		if err != nil {
			t.Fatalf("concurrent command after the %s record: %v", kind, err)
		}
	})
}

func granted(ctx context.Context, service *Service, acl ACL) (bool, error) {
	builder, err := aclBuilder(acl)
	if err != nil {
		return false, err
	}
	results, err := kadm.NewClient(service.Operator).DescribeACLs(ctx, builder)
	if err != nil {
		return false, err
	}
	for _, result := range results {
		if result.Err != nil || len(result.Described) == 0 {
			return false, result.Err
		}
	}
	return true, nil
}

func hasCredential(ctx context.Context, service *Service, user string) (bool, error) {
	described, err := kadm.NewClient(service.Operator).DescribeUserSCRAMs(ctx)
	if err != nil {
		return false, err
	}
	_, ok := described[user]
	return ok, nil
}

func denied(context.Context, string, string) error { return ErrAuthorizationDenied }

// A host add --force that loaded state before a host remove and resumes after the remove's end record issues a
// credential and grants from its snapshot; the remove's final pass deletes both.
func TestRemoveUndoesAStaleHostAddForce(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	removeUndoesAStaleHostAddForce(t, service, "pool-a", "tenant-a", "host-a", func() { revocationBounds(t, service, "1", "1", "1") }, denied)
}

func TestRemoveUndoesAStaleHostAddForceOnRealBroker(t *testing.T) {
	if os.Getenv("WHERRY_TRUST_BROKER") == "" {
		t.Skip("WHERRY_TRUST_BROKER is not set")
	}
	service, closeService := realService(t)
	defer closeService()
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	tenant, host := "tc"+suffix, "h1"
	removeUndoesAStaleHostAddForce(t, service, "converge"+suffix, tenant, host, func() {}, service.Probe(HostTopic(tenant, host)))
}

func removeUndoesAStaleHostAddForce(t *testing.T, service *Service, pool, tenant, host string, bound func(), probe func(context.Context, string, string) error) {
	t.Helper()
	ctx := context.Background()
	if err := service.PoolApply(ctx, pool, modelTerms(), false); err != nil {
		t.Fatal(err)
	}
	if err := hostFixture(t, service, ctx, tenant, host, filepath.Join(t.TempDir(), "host.yaml"), false); err != nil {
		t.Fatal(err)
	}
	if err := entitleFixture(t, service, ctx, tenant, pool); err != nil {
		t.Fatal(err)
	}
	loaded, err := service.LoadState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	principal := TenantPrincipal(tenant, host)
	afterRecord(t, "end", func() error {
		if _, err := service.upsertUser(ctx, principal); err != nil {
			return err
		}
		return service.reconcileACLs(ctx, loaded)
	})
	bound()
	if result := service.RevokeHost(ctx, tenant, host, 0, probe); !result.Complete {
		t.Fatalf("incomplete revocation: %+v", result)
	}
	if count, err := principalACLs(ctx, service, principal); err != nil || count != 0 {
		t.Fatalf("removed host keeps %d grants: %v", count, err)
	}
	if ok, err := hasCredential(ctx, service, principal); err != nil || ok {
		t.Fatalf("removed host keeps a credential: %t, %v", ok, err)
	}
}

// A stale command restores a removed receiver's grants after the next command loaded its state; that command's final
// pass, from the records it re-reads, deletes them.
func TestNextCommandWithdrawsARemovedReceiversRestoredGrants(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	if err := service.PoolApply(ctx, "pool-a", modelTerms(), false); err != nil {
		t.Fatal(err)
	}
	for _, receiver := range []string{"receiver-a", "receiver-b"} {
		if err := service.ReceiverAdd(ctx, modelConfig(receiver, "pool-a"), service.Broker, filepath.Join(t.TempDir(), receiver+".yaml")); err != nil {
			t.Fatal(err)
		}
	}
	loaded, err := service.LoadState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	revocationBounds(t, service, "1", "1", "1")
	afterRecord(t, "host", func() error {
		if result := service.RevokeReceiver(ctx, "receiver-a", 0, denied); !result.Complete {
			return errors.New("incomplete revocation")
		}
		return service.reconcileACLs(ctx, loaded)
	})
	if err := hostFixture(t, service, ctx, "tenant-a", "host-a", filepath.Join(t.TempDir(), "host.yaml"), false); err != nil {
		t.Fatal(err)
	}
	if count, err := principalACLs(ctx, service, ReceiverPrincipal("receiver-a")); err != nil || count != 0 {
		t.Fatalf("removed receiver keeps %d grants: %v", count, err)
	}
	peerRead := ACL{ReceiverPrincipal("receiver-b"), "topic", ReceiverProgressTopic("receiver-a"), false, []string{"READ"}}
	if ok, err := granted(ctx, service, peerRead); err != nil || ok {
		t.Fatalf("peer keeps READ on the removed receiver's progress topic: %t, %v", ok, err)
	}
}

// agb-eok3: removing a receiver withdraws its peers' READ on its progress topic and keeps their READ on its key topic.
func TestRemovalWithdrawsPeersReadOnTheProgressTopic(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	if err := service.PoolApply(ctx, "pool-a", modelTerms(), false); err != nil {
		t.Fatal(err)
	}
	for _, receiver := range []string{"receiver-a", "receiver-b"} {
		if err := service.ReceiverAdd(ctx, modelConfig(receiver, "pool-a"), service.Broker, filepath.Join(t.TempDir(), receiver+".yaml")); err != nil {
			t.Fatal(err)
		}
	}
	progress := ACL{ReceiverPrincipal("receiver-b"), "topic", ReceiverProgressTopic("receiver-a"), false, []string{"READ"}}
	if ok, err := granted(ctx, service, progress); err != nil || !ok {
		t.Fatalf("peer lacks READ on a live receiver's progress topic: %v", err)
	}
	revocationBounds(t, service, "1", "1", "1")
	if result := service.RevokeReceiver(ctx, "receiver-a", 0, denied); !result.Complete {
		t.Fatalf("incomplete revocation: %+v", result)
	}
	if ok, err := granted(ctx, service, progress); err != nil || ok {
		t.Fatalf("peer keeps READ on the removed receiver's progress topic: %t, %v", ok, err)
	}
	keys := ACL{ReceiverPrincipal("receiver-b"), "topic", ReceiverTopic("receiver-a"), false, []string{"READ"}}
	if ok, err := granted(ctx, service, keys); err != nil || !ok {
		t.Fatalf("peer lost READ on the removed receiver's key topic: %v", err)
	}
}

// Every command's pass leaves wherry.operator's grants and every grant of a principal or on a resource wherry does
// not name, and deletes a wherry principal's grant on a wherry resource that no record calls for. Revocation alone
// deletes every grant of the principal it revokes, on any resource.
func TestConvergeTouchesOnlyWhatWherryManages(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	foreign := []ACL{
		{"other", "topic", "other.topic", false, []string{"READ"}},
		{"other", "topic", ReceiverTopic("receiver-a"), false, []string{"READ"}},
		{"other", "group", PoolGroup("pool-b"), false, []string{"READ"}},
		{ReceiverPrincipal("receiver-b"), "topic", "other.topic", false, []string{"READ"}},
		{TenantPrincipal("tenant-a", "host-a"), "group", "other.group", false, []string{"READ"}},
	}
	revokedOwn := ACL{ReceiverPrincipal("receiver-a"), "topic", "other.topic", false, []string{"READ"}}
	for _, acl := range append(foreign, stray, revokedOwn) {
		builder, err := aclBuilder(acl)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := kadm.NewClient(service.Operator).CreateACLs(ctx, builder); err != nil {
			t.Fatal(err)
		}
	}
	commands := []func() error{
		func() error { return service.PoolApply(ctx, "pool-a", modelTerms(), false) },
		func() error {
			return hostFixture(t, service, ctx, "tenant-a", "host-a", filepath.Join(t.TempDir(), "host.yaml"), false)
		},
		func() error { return entitleFixture(t, service, ctx, "tenant-a", "pool-a") },
		func() error {
			return service.ReceiverAdd(ctx, modelConfig("receiver-a", "pool-a"), service.Broker, filepath.Join(t.TempDir(), "receiver.yaml"))
		},
		func() error { return service.PoolSuspend(ctx, "pool-a", true) },
		func() error { return service.PoolSuspend(ctx, "pool-a", false) },
		func() error {
			revocationBounds(t, service, "1", "1", "1")
			if result := service.RevokeReceiver(ctx, "receiver-a", 0, denied); !result.Complete {
				return errors.New("incomplete revocation")
			}
			return nil
		},
	}
	for i, command := range commands {
		if err := command(); err != nil {
			t.Fatalf("command %d: %v", i, err)
		}
		for _, acl := range foreign {
			if ok, err := granted(ctx, service, acl); err != nil || !ok {
				t.Fatalf("command %d deleted %+v: %v", i, acl, err)
			}
		}
		if err := service.verifyACLs(ctx, OperatorACLs); err != nil {
			t.Fatalf("command %d deleted a grant of %s: %v", i, OperatorPrincipal, err)
		}
		if ok, err := granted(ctx, service, revokedOwn); err != nil || ok != (i < len(commands)-1) {
			t.Fatalf("after command %d the revoked receiver's grant outside wherry is granted: %t, %v", i, ok, err)
		}
	}
	if ok, err := granted(ctx, service, stray); err != nil || ok {
		t.Fatalf("a grant no record calls for survived: %t, %v", ok, err)
	}
}

// A stale command restores a suspended pool's task-topic WRITE after the suspend record; the suspension's pass after
// its record deletes it again.
func TestSuspensionReassertsItsDeletionAfterItsRecord(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	if err := service.PoolApply(ctx, "pool-a", modelTerms(), false); err != nil {
		t.Fatal(err)
	}
	if err := hostFixture(t, service, ctx, "tenant-a", "host-a", filepath.Join(t.TempDir(), "host.yaml"), false); err != nil {
		t.Fatal(err)
	}
	if err := entitleFixture(t, service, ctx, "tenant-a", "pool-a"); err != nil {
		t.Fatal(err)
	}
	loaded, err := service.LoadState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	afterRecord(t, "suspend", func() error { return service.reconcileACLs(ctx, loaded) })
	if err := service.PoolSuspend(ctx, "pool-a", true); err != nil {
		t.Fatal(err)
	}
	write := ACL{TenantPrincipal("tenant-a", "host-a"), "topic", PoolTasksPrefix("pool-a"), true, []string{"WRITE"}}
	if ok, err := granted(ctx, service, write); err != nil || ok {
		t.Fatalf("suspended pool's task topic keeps a host's WRITE: %t, %v", ok, err)
	}
	describe := write
	describe.Operations = []string{"DESCRIBE"}
	if ok, err := granted(ctx, service, describe); err != nil || !ok {
		t.Fatalf("suspension withdrew the host's DESCRIBE: %v", err)
	}
}

// duringPass runs act once, in the first convergence pass after the pass has read wherry.operator and before it changes
// Kafka, as a concurrent command would.
func duringPass(t *testing.T, act func() error) {
	t.Helper()
	var err error
	converging = func() {
		converging = func() {}
		err = act()
	}
	t.Cleanup(func() {
		converging = func() {}
		if err != nil {
			t.Fatalf("concurrent command during a pass: %v", err)
		}
	})
}

func grant(t *testing.T, service *Service, acl ACL) {
	t.Helper()
	builder, err := aclBuilder(acl)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kadm.NewClient(service.Operator).CreateACLs(context.Background(), builder); err != nil {
		t.Fatal(err)
	}
}

// entitledHost is pool-a with tenant-a's host-a entitled to it.
func entitledHost(t *testing.T, service *Service) {
	t.Helper()
	ctx := context.Background()
	if err := service.PoolApply(ctx, "pool-a", modelTerms(), false); err != nil {
		t.Fatal(err)
	}
	if err := hostFixture(t, service, ctx, "tenant-a", "host-a", filepath.Join(t.TempDir(), "host.yaml"), false); err != nil {
		t.Fatal(err)
	}
	if err := entitleFixture(t, service, ctx, "tenant-a", "pool-a"); err != nil {
		t.Fatal(err)
	}
}

// stray is a grant no record calls for, of a wherry principal on a wherry topic.
var stray = ACL{ReceiverPrincipal("ghost"), "topic", ReceiverTopic("receiver-a"), false, []string{"READ"}}

func withdrawn(t *testing.T, service *Service, acl ACL) {
	t.Helper()
	if ok, err := granted(context.Background(), service, acl); err != nil || ok {
		t.Fatalf("%+v survived: %t, %v", acl, ok, err)
	}
}

// A suspension that completes while another command's pass holds the state from before it: the pass restores the
// suspended pool's task-topic WRITE, sees wherry.operator moved, and repeats from the suspension's record.
func TestAPassDuringWhichTheLogMovedRepeats(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	entitledHost(t, service)
	duringPass(t, func() error { return service.PoolSuspend(ctx, "pool-a", true) })
	if err := hostFixture(t, service, ctx, "tenant-b", "host-b", filepath.Join(t.TempDir(), "host.yaml"), false); err != nil {
		t.Fatal(err)
	}
	withdrawn(t, service, ACL{TenantPrincipal("tenant-a", "host-a"), "topic", PoolTasksPrefix("pool-a"), true, []string{"WRITE"}})
}

func TestConvergeStopsAtItsBound(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	entitledHost(t, service)
	// wherry.operator moves during twice as many passes as the bound, then stops.
	moved := 0
	var err error
	converging = func() {
		if moved < 2*convergePasses && err == nil {
			moved++
			_, _, err = service.appendRecord(ctx, []byte("entitlement.tenant-a.pool-a"), Record{Kind: "entitlement", Tenant: "tenant-a", Pool: "pool-a"})
		}
	}
	defer func() { converging = func() {} }()
	converged := service.converge(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if converged == nil || !strings.Contains(converged.Error(), "moved during each of 5 convergence passes") || moved != convergePasses {
		t.Fatalf("converge = %v after %d moves, want the bound's error after %d", converged, moved, convergePasses)
	}
}

// A host add --force that checked the host was live, while a removal ends it before its pass: it refuses and writes no
// bundle.
func TestStaleReissueRefusesAndWritesNoBundle(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	entitledHost(t, service)
	revocationBounds(t, service, "1", "1", "1")
	duringPass(t, func() error {
		if result := service.RevokeHost(ctx, "tenant-a", "host-a", 0, denied); !result.Complete {
			return errors.New("incomplete revocation")
		}
		return nil
	})
	dir := t.TempDir()
	err := hostFixture(t, service, ctx, "tenant-a", "host-a", filepath.Join(dir, "host.yaml"), true)
	if err == nil || !strings.Contains(err.Error(), "cannot return after its membership end") {
		t.Fatalf("stale reissue = %v, want a refusal", err)
	}
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
		t.Fatalf("stale reissue left %v: %v", entries, err)
	}
	if ok, err := hasCredential(ctx, service, TenantPrincipal("tenant-a", "host-a")); err != nil || ok {
		t.Fatalf("removed host keeps a credential: %t, %v", ok, err)
	}
}

// A pool apply whose snapshot topic creation fails after its record still converges.
func TestPoolApplyFailingAfterItsRecordConverges(t *testing.T) {
	cluster, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	grant(t, service, stray)
	afterRecord(t, "pool", func() error {
		cluster.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.CreateTopics}, Topic: CustodianTopic("pool-a"), Err: kerr.PolicyViolation})
		return nil
	})
	if err := service.PoolApply(ctx, "pool-a", modelTerms(), false); !errors.Is(err, kerr.PolicyViolation) {
		t.Fatalf("pool apply = %v, want the injected failure", err)
	}
	withdrawn(t, service, stray)
}

// A stale command restores a revoked receiver's grants after step 2 and the probe then fails: the failed revocation
// converges, and the receiver, revoking, holds no grant.
func TestRevocationFailingAfterAStaleRestoreConverges(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	if err := service.PoolApply(ctx, "pool-a", modelTerms(), false); err != nil {
		t.Fatal(err)
	}
	if err := service.ReceiverAdd(ctx, modelConfig("receiver-a", "pool-a"), service.Broker, filepath.Join(t.TempDir(), "receiver.yaml")); err != nil {
		t.Fatal(err)
	}
	loaded, err := service.LoadState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	revocationBounds(t, service, "1", "1", "1")
	probe := func(context.Context, string, string) error {
		if err := service.reconcileACLs(ctx, loaded); err != nil {
			return err
		}
		return errors.New("broker unreachable")
	}
	result := service.RevokeReceiver(ctx, "receiver-a", 0, probe)
	if result.Complete || len(result.Steps) != 3 || result.Steps[2].Err == nil {
		t.Fatalf("revocation %+v, want a failure at step 3", result)
	}
	if count, err := principalACLs(ctx, service, ReceiverPrincipal("receiver-a")); err != nil || count != 0 {
		t.Fatalf("revoking receiver keeps %d grants: %v", count, err)
	}
}

// A stale command restores a removed host's credential and its describe fails: withdraw fails rather than leave it.
func TestWithdrawFailsOnASCRAMDescribeError(t *testing.T) {
	cluster, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	entitledHost(t, service)
	revocationBounds(t, service, "1", "1", "1")
	principal := TenantPrincipal("tenant-a", "host-a")
	if result := service.RevokeHost(ctx, "tenant-a", "host-a", 0, denied); !result.Complete {
		t.Fatalf("incomplete revocation: %+v", result)
	}
	if _, err := service.upsertUser(ctx, principal); err != nil {
		t.Fatal(err)
	}
	cluster.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.DescribeUserSCRAMCredentials}, Resource: principal, Err: kerr.UnknownServerError, Count: -1})
	if err := service.converge(ctx); !errors.Is(err, kerr.UnknownServerError) {
		t.Fatalf("converge = %v, want the describe error", err)
	}
}

func TestReceiverUpdateConverges(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	if err := service.PoolApply(ctx, "pool-a", modelTerms(), false); err != nil {
		t.Fatal(err)
	}
	config := modelConfig("receiver-a", "pool-a")
	if err := service.ReceiverAdd(ctx, config, service.Broker, filepath.Join(t.TempDir(), "receiver.yaml")); err != nil {
		t.Fatal(err)
	}
	grant(t, service, stray)
	config.Capacity = 2
	if _, err := service.ReceiverUpdate(ctx, config, false); err != nil {
		t.Fatal(err)
	}
	withdrawn(t, service, stray)
}

func TestReissueConverges(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	if err := service.PoolApply(ctx, "pool-a", modelTerms(), false); err != nil {
		t.Fatal(err)
	}
	if err := service.ReceiverAdd(ctx, modelConfig("receiver-a", "pool-a"), service.Broker, filepath.Join(t.TempDir(), "receiver.yaml")); err != nil {
		t.Fatal(err)
	}
	grant(t, service, stray)
	if err := service.ReceiverReissue(ctx, "receiver-a", service.Broker, filepath.Join(t.TempDir(), "receiver.yaml")); err != nil {
		t.Fatal(err)
	}
	withdrawn(t, service, stray)
}

// rotate-key converges before it waits for the custodian, which this test does not run.
func TestPoolRotateKeyConverges(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	wait := snapshotWait
	t.Cleanup(func() { snapshotWait = wait })
	snapshotWait = 300 * time.Millisecond
	ctx := context.Background()
	if err := service.PoolApply(ctx, "pool-a", modelTerms(), false); err != nil {
		t.Fatal(err)
	}
	if err := service.PoolSuspend(ctx, "pool-a", true); err != nil {
		t.Fatal(err)
	}
	grant(t, service, stray)
	if _, err := service.PoolRotateKey(ctx, "pool-a", []string{"suspect"}, io.Discard); err == nil || !strings.Contains(err.Error(), "published no snapshot") {
		t.Fatalf("rotate-key = %v, want the custodian wait to time out", err)
	}
	withdrawn(t, service, stray)
}

func TestTenantAddConverges(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	grant(t, service, stray)
	if err := service.TenantAdd(context.Background(), "tenant-a"); err != nil {
		t.Fatal(err)
	}
	withdrawn(t, service, stray)
}

// ReceiverAdd's grants before its record only create; its pass after the record withdraws.
func TestReceiverAddConvergesAfterItsRecord(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	if err := service.PoolApply(ctx, "pool-a", modelTerms(), false); err != nil {
		t.Fatal(err)
	}
	grant(t, service, stray)
	if err := service.ReceiverAdd(ctx, modelConfig("receiver-a", "pool-a"), service.Broker, filepath.Join(t.TempDir(), "receiver.yaml")); err != nil {
		t.Fatal(err)
	}
	withdrawn(t, service, stray)
}

// A grant the records call for, bound to one client host rather than every host, is not the grant they call for: a
// pass deletes it and keeps the binding for every host.
func TestConvergeWithdrawsAHostSpecificBinding(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	if err := service.PoolApply(ctx, "pool-a", modelTerms(), false); err != nil {
		t.Fatal(err)
	}
	if err := service.ReceiverAdd(ctx, modelConfig("receiver-a", "pool-a"), service.Broker, filepath.Join(t.TempDir(), "receiver.yaml")); err != nil {
		t.Fatal(err)
	}
	wanted := ACL{ReceiverPrincipal("receiver-a"), "topic", ReceiverTopic("receiver-a"), false, []string{"READ"}}
	bound := func() *kadm.ACLBuilder {
		builder := kadm.NewACLs().Allow(wanted.Principal).AllowHosts("192.0.2.1").Topics(wanted.Resource).
			ResourcePatternType(kadm.ACLPatternLiteral).Operations(kadm.OpRead)
		builder.PrefixUser()
		return builder
	}
	if _, err := kadm.NewClient(service.Operator).CreateACLs(ctx, bound()); err != nil {
		t.Fatal(err)
	}
	if err := service.converge(ctx); err != nil {
		t.Fatal(err)
	}
	described, err := kadm.NewClient(service.Operator).DescribeACLs(ctx, bound())
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range described {
		if result.Err != nil || len(result.Described) != 0 {
			t.Fatalf("the host-specific binding survived: %+v, %v", result.Described, result.Err)
		}
	}
	if ok, err := granted(ctx, service, wanted); err != nil || !ok {
		t.Fatalf("the binding for every host was withdrawn: %v", err)
	}
}
