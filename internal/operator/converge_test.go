package operator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
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
	if err := service.HostAdd(ctx, tenant, host, filepath.Join(t.TempDir(), "host.yaml"), false); err != nil {
		t.Fatal(err)
	}
	if err := service.TenantAdd(ctx, tenant, pool); err != nil {
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
	if err := service.HostAdd(ctx, "tenant-a", "host-a", filepath.Join(t.TempDir(), "host.yaml"), false); err != nil {
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
// not name, and deletes a wherry principal's grant on a wherry resource that no record calls for.
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
	stray := ACL{ReceiverPrincipal("ghost"), "topic", ReceiverTopic("receiver-a"), false, []string{"READ"}}
	for _, acl := range append(foreign, stray) {
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
			return service.HostAdd(ctx, "tenant-a", "host-a", filepath.Join(t.TempDir(), "host.yaml"), false)
		},
		func() error { return service.TenantAdd(ctx, "tenant-a", "pool-a") },
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
	if err := service.HostAdd(ctx, "tenant-a", "host-a", filepath.Join(t.TempDir(), "host.yaml"), false); err != nil {
		t.Fatal(err)
	}
	if err := service.TenantAdd(ctx, "tenant-a", "pool-a"); err != nil {
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
	write := ACL{TenantPrincipal("tenant-a", "host-a"), "topic", PoolTasksTopic("pool-a"), false, []string{"WRITE"}}
	if ok, err := granted(ctx, service, write); err != nil || ok {
		t.Fatalf("suspended pool's task topic keeps a host's WRITE: %t, %v", ok, err)
	}
	describe := write
	describe.Operations = []string{"DESCRIBE"}
	if ok, err := granted(ctx, service, describe); err != nil || !ok {
		t.Fatalf("suspension withdrew the host's DESCRIBE: %v", err)
	}
}
