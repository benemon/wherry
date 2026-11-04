package operator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func tenantRecord(kind string) Record { return Record{Version: 1, Kind: kind, Tenant: "t", AtMS: 1} }

func TestTenantLoadRules(t *testing.T) {
	tenant, end := tenantRecord("tenant"), tenantRecord("end")
	entitlement := tenantRecord("entitlement")
	entitlement.Pool = "p"
	endPool := entitlement
	endPool.Kind = "end"
	host := tenantRecord("host")
	host.Host = "h"
	revoking := host
	revoking.Kind = "revoking"
	endHost := host
	endHost.Kind = "end"
	terms := modelTerms()
	pool := Record{Version: 1, Kind: "pool", Pool: "p", Terms: &terms, AtMS: 1}
	for _, test := range []struct {
		name    string
		records []Record
		valid   bool
	}{
		{"entitlement before tenant", []Record{pool, entitlement}, false},
		{"host before tenant", []Record{host}, false},
		{"entitlement after end", []Record{pool, tenant, end, entitlement}, false},
		{"host after end", []Record{tenant, end, host}, false},
		{"duplicate tenant", []Record{tenant, tenant}, false},
		{"tenant cannot return", []Record{tenant, end, tenant}, false},
		{"absent tenant end", []Record{end}, false},
		{"absent entitlement end", []Record{pool, tenant, endPool}, false},
		{"tenant with entitlement", []Record{pool, tenant, entitlement, end}, false},
		{"tenant with live host", []Record{tenant, host, end}, false},
		{"tenant with revoking host", []Record{tenant, host, revoking, end}, false},
		{"tenant with ended revoking host", []Record{tenant, host, revoking, endHost, end}, true},
		{"repeated entitlement lifecycle", []Record{pool, tenant, entitlement, endPool, entitlement, endPool}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var raw []rawRecord
			for i, record := range test.records {
				encoded, err := EncodeRecord(record)
				if err != nil {
					t.Fatal(err)
				}
				raw = append(raw, rawRecord{int64(i), encoded})
			}
			state, err := load(raw)
			if (err == nil) != test.valid {
				t.Fatalf("load = %v, valid=%v", err, test.valid)
			}
			if test.name == "repeated entitlement lifecycle" && (!state.EndedEntitlements["t"]["p"] || len(state.EndedEntitlements["t"]) != 1 || len(state.Entitlements) != 0) {
				t.Fatalf("state=%+v", state)
			}
			if test.name == "tenant with ended revoking host" && (len(state.Ended) != 1 || !state.Tenants["t"]) {
				t.Fatalf("tenant end leaked into principal ends: %+v", state)
			}
		})
	}
}

func TestTenantEndShapes(t *testing.T) {
	for _, record := range []Record{tenantRecord("tenant"), tenantRecord("end"), {Kind: "end", Tenant: "t", Pool: "p"}, {Kind: "end", Tenant: "t", Host: "h"}, {Kind: "end", Receiver: "r"}} {
		encoded, err := EncodeRecord(record)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeRecord(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if record.Kind == "end" && got.Host == "" && got.Receiver == "" && !strings.HasPrefix(subject(got), "tenant.") && !strings.HasPrefix(subject(got), "entitlement.") {
			t.Fatal(subject(got))
		}
	}
	for _, record := range []Record{{Kind: "revoking", Tenant: "t"}, {Kind: "revoking", Tenant: "t", Pool: "p"}, {Kind: "revoking", Receiver: "r", Tenant: "t"}, {Kind: "end", Tenant: "t", Host: "h", Pool: "p"}, {Kind: "end", Receiver: "r", Tenant: "t"}} {
		if _, err := EncodeRecord(record); err == nil {
			t.Fatalf("accepted %+v", record)
		}
	}
}

func TestTenantKeptACLMatrix(t *testing.T) {
	for _, phase := range []string{"pool end", "tenant end", "host end", "new receiver", "readded"} {
		t.Run(phase, func(t *testing.T) {
			state := NewState()
			state.Tenants["t"] = phase == "tenant end"
			state.Hosts[TenantPrincipal("t", "live")] = Host{"t", "live"}
			state.Hosts[TenantPrincipal("t", "old")] = Host{"t", "old"}
			state.Ended[TenantPrincipal("t", "old")] = true
			state.Revoking[TenantPrincipal("t", "old")] = true
			for _, r := range []string{"r", "revoking", "ended"} {
				state.Receivers[r] = ReceiverConfig{Receiver: r, Pool: "p"}
			}
			state.Revoking[ReceiverPrincipal("revoking")] = true
			state.Ended[ReceiverPrincipal("ended")] = true
			if phase == "host end" || phase == "readded" {
				state.Entitlements["t"] = map[string]bool{"p": true}
			} else {
				state.EndedEntitlements["t"] = map[string]bool{"p": true}
			}
			if phase == "new receiver" {
				_ = ACLs(state)
				state.Receivers["later"] = ReceiverConfig{Receiver: "later", Pool: "p"}
				state.Hosts[TenantPrincipal("t", "later")] = Host{"t", "later"}
			}
			if phase == "tenant end" {
				state.Ended[TenantPrincipal("t", "live")] = true
			}
			acls := ACLs(state)
			var got []string
			for _, acl := range acls {
				if strings.HasPrefix(acl.Resource, "wherry.tenant.t") || strings.HasPrefix(acl.Resource, "wherry.host.t.") {
					if strings.HasPrefix(acl.Principal, "wherry.rcv.") {
						got = append(got, fmt.Sprintf("%s %s %s", acl.Principal, acl.Resource, strings.Join(acl.Operations, ",")))
					}
				}
				if phase != "readded" && phase != "host end" && strings.HasPrefix(acl.Principal, "wherry.tnt.") && strings.Contains(acl.Resource, "wherry.pool.p.") {
					t.Fatalf("host retains pool grant: %+v", acl)
				}
			}
			want := []string{
				"wherry.rcv.r wherry.host.t.live READ,DESCRIBE",
				"wherry.rcv.r wherry.host.t.old READ,DESCRIBE",
				"wherry.rcv.r wherry.tenant.t WRITE,DESCRIBE",
				"wherry.rcv.r wherry.tenant.t.transcript WRITE,DESCRIBE",
			}
			if phase == "new receiver" {
				want = append(want, "wherry.rcv.r wherry.host.t.later READ,DESCRIBE")
				for _, resource := range []string{"wherry.host.t.live READ,DESCRIBE", "wherry.host.t.old READ,DESCRIBE", "wherry.host.t.later READ,DESCRIBE", "wherry.tenant.t WRITE,DESCRIBE", "wherry.tenant.t.transcript WRITE,DESCRIBE"} {
					want = append(want, "wherry.rcv.later "+resource)
				}
			}
			slices.Sort(got)
			slices.Sort(want)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("matrix:\n%v\nwant:\n%v", got, want)
			}
			if phase == "readded" && !hasACL(acls, TenantPrincipal("t", "live"), PoolTasksTopic("p"), "WRITE") {
				t.Fatal("re-add did not restore host grants")
			}
		})
	}
}

func TestAbsentTenantCommands(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := t.Context()
	if err := service.PoolApply(ctx, "p", modelTerms(), false); err != nil {
		t.Fatal(err)
	}
	for name, command := range map[string]func() error{
		"pool add":    func() error { return service.TenantPoolAdd(ctx, "absent", "p") },
		"pool remove": func() error { return service.TenantPoolRemove(ctx, "absent", "p") },
		"host add":    func() error { return service.HostAdd(ctx, "absent", "h", filepath.Join(t.TempDir(), "bundle"), false) },
		"host remove": func() error { return revocationError(service.RevokeHost(ctx, "absent", "h", 0, denied)) },
		"remove":      func() error { return removeTenantFixture(service, ctx, "absent") },
	} {
		t.Run(name, func(t *testing.T) {
			if err := command(); err == nil || !strings.Contains(err.Error(), "tenant add") {
				t.Fatalf("error=%v", err)
			}
		})
	}
	state, err := service.LoadState(ctx)
	if err != nil || len(state.Tenants) != 0 {
		t.Fatalf("implicit tenant: %+v, %v", state.Tenants, err)
	}
}

func TestTenantPoolQuotaRetries(t *testing.T) {
	cluster, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := t.Context()
	terms := modelTerms()
	terms.ProducerByteRate = 100
	terms.RequestPercentage = 10
	if err := service.PoolApply(ctx, "p1", terms, false); err != nil {
		t.Fatal(err)
	}
	terms.ProducerByteRate = 200
	terms.RequestPercentage = 20
	if err := service.PoolApply(ctx, "p2", terms, false); err != nil {
		t.Fatal(err)
	}
	if err := service.TenantAdd(ctx, "t"); err != nil {
		t.Fatal(err)
	}
	if err := service.HostAdd(ctx, "t", "h", filepath.Join(t.TempDir(), "bundle"), false); err != nil {
		t.Fatal(err)
	}
	principal := TenantPrincipal("t", "h")
	quota := func(want float64) {
		t.Helper()
		entries, err := kadm.NewClient(service.Operator).DescribeClientQuotas(ctx, true, []kadm.DescribeClientQuotaComponent{{Type: "user", MatchName: &principal}})
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]float64{}
		for _, e := range entries {
			for _, v := range e.Values {
				got[v.Key] = v.Value
			}
		}
		if got["producer_byte_rate"] != want || got["request_percentage"] != want/10 || want == 0 && len(got) != 0 {
			t.Fatalf("quota=%v want rate %v", got, want)
		}
	}
	failQuota := func() {
		cluster.ControlKey(kmsg.AlterClientQuotas.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
			response := request.(*kmsg.AlterClientQuotasRequest).ResponseKind().(*kmsg.AlterClientQuotasResponse)
			response.Entries = append(response.Entries, kmsg.AlterClientQuotasResponseEntry{ErrorCode: kerr.UnknownServerError.Code})
			return response, nil, true
		})
	}
	failQuota()
	if err := service.TenantPoolAdd(ctx, "t", "p1"); err == nil {
		t.Fatal("expected post-append quota failure")
	}
	if err := service.TenantPoolAdd(ctx, "t", "p1"); err != nil {
		t.Fatal(err)
	}
	quota(100)
	if err := service.TenantPoolAdd(ctx, "t", "p2"); err != nil {
		t.Fatal(err)
	}
	quota(100)
	failQuota()
	if err := service.TenantPoolRemove(ctx, "t", "p1"); err == nil {
		t.Fatal("expected post-end quota failure")
	}
	if err := service.TenantPoolRemove(ctx, "t", "p1"); err != nil {
		t.Fatal(err)
	}
	quota(200)
	if err := service.TenantPoolRemove(ctx, "t", "p2"); err != nil {
		t.Fatal(err)
	}
	quota(0)
	if err := service.TenantPoolRemove(ctx, "t", "p2"); err != nil {
		t.Fatal(err)
	}
	quota(0)
	if err := service.TenantPoolAdd(ctx, "t", "p2"); err != nil {
		t.Fatal(err)
	}
	quota(200)
	state, err := service.LoadState(ctx)
	if err != nil || state.EndedEntitlements["t"]["p2"] {
		t.Fatalf("re-add state=%+v err=%v", state, err)
	}
	bundle := filepath.Join(t.TempDir(), "bundle")
	failQuota()
	if err := service.HostAdd(ctx, "t", "h2", bundle, false); err == nil {
		t.Fatal("expected post-append host quota failure")
	}
	failQuota()
	if err := service.HostAdd(ctx, "t", "h2", bundle, true); err == nil {
		t.Fatal("expected --force quota failure")
	}
	if _, err := os.Stat(bundle); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a failed --force left a bundle: %v", err)
	}
	if err := service.HostAdd(ctx, "t", "h2", bundle, true); err != nil {
		t.Fatal(err)
	}
	principal = TenantPrincipal("t", "h2")
	quota(200)
}

func TestTenantFeedsOnlyLiveTenants(t *testing.T) {
	state := NewState()
	state.Tenants["live"] = false
	state.Tenants["ended"] = true
	state.Hosts[TenantPrincipal("ended", "h")] = Host{"ended", "h"}
	if got := tenantFeeds(state); !reflect.DeepEqual(got, []string{TenantTopic("live"), TenantTranscriptTopic("live")}) {
		t.Fatal(got)
	}
}

func TestForceRefusesRevokingHost(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := context.Background()
	if err := service.TenantAdd(ctx, "t"); err != nil {
		t.Fatal(err)
	}
	if err := service.HostAdd(ctx, "t", "h", filepath.Join(t.TempDir(), "first"), false); err != nil {
		t.Fatal(err)
	}
	if _, err := service.appendChecked(ctx, []byte("revoking"), Record{Kind: "revoking", Tenant: "t", Host: "h"}); err != nil {
		t.Fatal(err)
	}
	if err := service.HostAdd(ctx, "t", "h", filepath.Join(t.TempDir(), "new"), true); err == nil || !strings.Contains(err.Error(), "revoking") {
		t.Fatal(err)
	}
}

func TestEndedTenantDoesNotWithdrawSCRAM(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := t.Context()
	if err := service.TenantAdd(ctx, "t"); err != nil {
		t.Fatal(err)
	}
	principal := TenantPrincipal("t", "")
	if _, err := service.upsertUser(ctx, principal); err != nil {
		t.Fatal(err)
	}
	if err := removeTenantFixture(service, ctx, "t"); err != nil {
		t.Fatal(err)
	}
	if ok, err := hasCredential(ctx, service, principal); err != nil || !ok {
		t.Fatalf("tenant-named SCRAM removed: %t %v", ok, err)
	}
	if err := removeTenantFixture(service, ctx, "t"); err != nil {
		t.Fatal(err)
	}
	if err := service.TenantAdd(ctx, "t"); err == nil {
		t.Fatal("tenant name returned")
	}
}

func TestTenantRemoveRetriesAndSerializesRecords(t *testing.T) {
	for _, failKind := range []string{"entitlement", "revoking", "host end", "tenant end", "none"} {
		t.Run(failKind, func(t *testing.T) {
			_, service, closeFixture := kfakeService(t)
			defer closeFixture()
			ctx := t.Context()
			if err := service.PoolApply(ctx, "p", modelTerms(), false); err != nil {
				t.Fatal(err)
			}
			if err := service.TenantAdd(ctx, "t"); err != nil {
				t.Fatal(err)
			}
			if err := service.TenantPoolAdd(ctx, "t", "p"); err != nil {
				t.Fatal(err)
			}
			for _, host := range []string{"a", "b"} {
				if err := service.HostAdd(ctx, "t", host, filepath.Join(t.TempDir(), "bundle"), false); err != nil {
					t.Fatal(err)
				}
			}
			revocationBounds(t, service, "1", "1", "1")
			attempt, cancel := context.WithCancel(ctx)
			defer cancel()
			var records []Record
			var appends atomic.Int32
			var overlap atomic.Bool
			var recordMu sync.Mutex
			recordAppend := func(record Record) {
				if appends.Add(1) != 1 {
					overlap.Store(true)
				}
				defer appends.Add(-1)
				time.Sleep(10 * time.Millisecond)
				recordMu.Lock()
				defer recordMu.Unlock()
				records = append(records, record)
			}
			appended = func(record Record) {
				recordAppend(record)
				if failKind == "entitlement" && record.Kind == "end" && record.Pool != "" || failKind == "revoking" && record.Kind == "revoking" || failKind == "host end" && record.Kind == "end" && record.Host != "" || failKind == "tenant end" && record.Kind == "end" && record.Host == "" && record.Pool == "" {
					cancel()
				}
			}
			t.Cleanup(func() { appended = func(Record) {} })
			err := removeTenantFixture(service, attempt, "t")
			if failKind != "none" && err == nil {
				t.Fatal("injected interruption did not fail")
			}
			appended = recordAppend
			if err := removeTenantFixture(service, ctx, "t"); err != nil {
				t.Fatal(err)
			}
			state, err := service.LoadState(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !state.Tenants["t"] || len(state.Entitlements) != 0 {
				t.Fatalf("incomplete: %+v", state)
			}
			for principal := range state.Hosts {
				if !state.Ended[principal] {
					t.Fatalf("host not ended: %s", principal)
				}
			}
			if overlap.Load() {
				t.Fatal("concurrent record appends")
			}
			// Both revoking records precede the first host end; no end precedes its own revoking record.
			seen := map[string]bool{}
			for _, record := range records {
				if record.Kind == "revoking" {
					seen[record.Host] = true
				}
				if record.Kind == "end" && record.Host != "" && len(seen) != 2 {
					t.Fatalf("host end before all preparations: %+v", records)
				}
			}
			if err := removeTenantFixture(service, ctx, "t"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTenantRemoveRepairsQuotaAfterEntitlementEnd(t *testing.T) {
	cluster, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := t.Context()
	if err := service.PoolApply(ctx, "p", modelTerms(), false); err != nil {
		t.Fatal(err)
	}
	if err := service.TenantAdd(ctx, "t"); err != nil {
		t.Fatal(err)
	}
	if err := service.TenantPoolAdd(ctx, "t", "p"); err != nil {
		t.Fatal(err)
	}
	if err := service.HostAdd(ctx, "t", "h", filepath.Join(t.TempDir(), "bundle"), false); err != nil {
		t.Fatal(err)
	}
	cluster.ControlKey(kmsg.AlterClientQuotas.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		response := request.(*kmsg.AlterClientQuotasRequest).ResponseKind().(*kmsg.AlterClientQuotasResponse)
		response.Entries = append(response.Entries, kmsg.AlterClientQuotasResponseEntry{ErrorCode: kerr.UnknownServerError.Code})
		return response, nil, true
	})
	if err := removeTenantFixture(service, ctx, "t"); err == nil {
		t.Fatal("expected quota failure")
	}
	state, err := service.LoadState(ctx)
	if err != nil || len(state.Entitlements) != 0 {
		t.Fatalf("end not appended: %v %v", state.Entitlements, err)
	}
	revocationBounds(t, service, "1", "1", "1")
	if err := removeTenantFixture(service, ctx, "t"); err != nil {
		t.Fatal(err)
	}
	principal := TenantPrincipal("t", "h")
	entries, err := kadm.NewClient(service.Operator).DescribeClientQuotas(ctx, true, []kadm.DescribeClientQuotaComponent{{Type: "user", MatchName: &principal}})
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if len(entry.Values) != 0 {
			t.Fatalf("quota remains: %+v", entry)
		}
	}
}

func TestChecksIgnoreEndedTopicsAndUnrelatedPoolRetention(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := t.Context()
	if err := service.PoolApply(ctx, "p", modelTerms(), false); err != nil {
		t.Fatal(err)
	}
	for _, tenant := range []string{"t", "unrelated"} {
		if err := service.TenantAdd(ctx, tenant); err != nil {
			t.Fatal(err)
		}
	}
	if err := service.TenantPoolAdd(ctx, "t", "p"); err != nil {
		t.Fatal(err)
	}
	if err := service.HostAdd(ctx, "t", "h", filepath.Join(t.TempDir(), "bundle"), false); err != nil {
		t.Fatal(err)
	}
	revocationBounds(t, service, "1", "1", "1")
	if result := service.RevokeHost(ctx, "t", "h", 0, denied); !result.Complete {
		t.Fatal(result)
	}
	if err := removeTenantFixture(service, ctx, "t"); err != nil {
		t.Fatal(err)
	}
	results, err := kadm.NewClient(service.Operator).DeleteTopics(ctx, TenantTopic("t"), TenantTranscriptTopic("t"), HostTopic("t", "h"))
	if err != nil {
		t.Fatal(err)
	}
	if err := results.Error(); err != nil {
		t.Fatal(err)
	}
	setTopicRetention(t, service, TenantTopic("unrelated"), 1)
	revocationBounds(t, service, "1", "60000", "1")
	if err := service.PoolApply(ctx, "p", modelTerms(), false); err != nil {
		t.Fatal(err)
	}
	if err := service.Check(ctx); err != nil {
		t.Fatal(err)
	}
}

func removeTenantFixture(service *Service, ctx context.Context, tenant string) error {
	return service.removeTenant(ctx, tenant, func(string) func(context.Context, string, string) error { return denied })
}

func TestCheckTenantWithoutEntitlements(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := t.Context()
	if err := service.TenantAdd(ctx, "empty"); err != nil {
		t.Fatal(err)
	}
	if err := service.Check(ctx); err != nil {
		t.Fatal(err)
	}
	deleted, err := kadm.NewClient(service.Operator).DeleteTopics(ctx, TenantTranscriptTopic("empty"))
	if err != nil {
		t.Fatal(err)
	}
	if err := deleted.Error(); err != nil {
		t.Fatal(err)
	}
	if err := service.Check(ctx); err == nil {
		t.Fatal("check ignored a live tenant without entitlements")
	}
}
