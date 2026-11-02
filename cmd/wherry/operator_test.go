package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestKafkaClientRejectsPlaintextListener(t *testing.T) {
	cluster := kfake.MustCluster(kfake.EnableSASL(), kfake.EnableACLs(), kfake.Superuser("SCRAM-SHA-512", "admin", "password"))
	defer cluster.Close()
	dir := t.TempDir()
	passwordPath := filepath.Join(dir, "password")
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(passwordPath, []byte("password"), 0o600); err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WHERRY_TRUST_BROKER", cluster.ListenAddrs()[0])
	t.Setenv("WHERRY_TRUST_CA", caPath)
	t.Setenv("WHERRY_TRUST_SUPER_USER", "admin")
	t.Setenv("WHERRY_TRUST_SUPER_USER_PASSWORD_FILE", passwordPath)
	client, err := operatorKafka(cluster.ListenAddrs()[0], pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), "admin", "WHERRY_TRUST_SUPER_USER_PASSWORD_FILE")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx); err == nil {
		t.Fatal("TLS client accepted a plaintext Kafka listener")
	}
}

// operatorEnv points runOperator at a TLS fake broker and returns an operator service on it.
func operatorEnv(t *testing.T) *operator.Service {
	cluster, ca := tlsCluster(t)
	dir := t.TempDir()
	files := map[string]string{"ca.pem": ca, "admin": "password", "operator": "operator-password"}
	for name, contents := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("WHERRY_TRUST_BROKER", cluster.ListenAddrs()[0])
	t.Setenv("WHERRY_TRUST_CA", filepath.Join(dir, "ca.pem"))
	t.Setenv("WHERRY_TRUST_SUPER_USER", "admin")
	t.Setenv("WHERRY_TRUST_SUPER_USER_PASSWORD_FILE", filepath.Join(dir, "admin"))
	t.Setenv("WHERRY_TRUST_OPERATOR_PASSWORD_FILE", filepath.Join(dir, "operator"))
	address := cluster.ListenAddrs()[0]
	return &operator.Service{SuperUser: connectTLS(t, address, ca, "admin", "password"), Operator: connectTLS(t, address, ca, "wherry.operator", "operator-password"),
		Broker: address, CA: ca, DeliveryCeiling: 10, LockCeilingMS: 60000}
}

func TestOperatorCommandUsage(t *testing.T) {
	operatorEnv(t)
	bundle := filepath.Join(t.TempDir(), "host.bundle")
	for _, test := range []struct {
		args []string
		want string
	}{
		{[]string{"host", "add", "host-a", "--bundle-file", bundle}, "usage: wherry operator host add HOST --tenant TENANT"},
		{[]string{"host", "add", "tenant-a", "host-a", "--bundle-file", bundle}, "usage: wherry operator host add HOST --tenant TENANT"},
		{[]string{"host", "add", "tenant-a", "host-a", "--tenant", "tenant-a", "--bundle-file", bundle}, "usage: wherry operator host add HOST --tenant TENANT"},
		{[]string{"host", "add", "host-a", "--tenant", "tenant-a"}, "usage: wherry operator host add HOST --tenant TENANT"},
		{[]string{"host", "remove", "host-a"}, "usage: wherry operator host remove HOST --tenant TENANT"},
		{[]string{"host", "remove", "--tenant", "tenant-a"}, "usage: wherry operator host remove HOST --tenant TENANT"},
		{[]string{"receiver", "remove"}, "usage: wherry operator receiver remove RECEIVER"},
		{[]string{"receiver", "remove", "receiver-a", "receiver-b"}, "usage: wherry operator receiver remove RECEIVER"},
		{[]string{"pool", "entitle", "build"}, "usage: wherry operator pool entitle POOL --tenant TENANT"},
		{[]string{"pool", "entitle", "--tenant", "tenant-a"}, "usage: wherry operator pool entitle POOL --tenant TENANT"},
		{[]string{"tenant", "add", "tenant-a", "--pool", "build"}, "unknown operator command"},
		{[]string{"revoke", "host", "tenant-a", "host-a"}, "unknown operator command"},
		{[]string{"revoke", "receiver", "receiver-a"}, "unknown operator command"},
		{[]string{"receiver", "configure", "receiver-a"}, "unknown operator command"},
		{[]string{"skip", "42", "--sha256", strings.Repeat("0", 64)}, "unknown operator command"},
		{[]string{"skip", "42"}, "unknown operator command"},
		{[]string{"pool", "apply"}, "usage: wherry operator pool apply NAME -f PATH"},
		{[]string{"receiver", "apply"}, "usage: wherry operator receiver apply NAME"},
		{[]string{"pool", "create", "pool-a"}, "unknown operator command"},
		{[]string{"pool", "update", "pool-a"}, "unknown operator command"},
		{[]string{"receiver", "add", "receiver-a"}, "unknown operator command"},
		{[]string{"receiver", "update", "receiver-a"}, "unknown operator command"},
		{[]string{"receiver", "revoke", "receiver-a"}, "unknown operator command"},
		{[]string{"host", "revoke", "host-a", "--tenant", "tenant-a"}, "unknown operator command"},
		{[]string{"pool", "suspend"}, "usage: wherry operator pool suspend POOL"},
		{[]string{"pool", "resume", "pool-a", "pool-b"}, "usage: wherry operator pool resume POOL"},
		{[]string{"pool", "rotate-key"}, "usage: wherry operator pool rotate-key POOL [--suspect-key KID]..."},
		{[]string{"pool", "suspend", "pool-a", "--suspect-key", "kid"}, "unknown option --suspect-key"},
	} {
		var stdout, stderr bytes.Buffer
		if err := runOperator(test.args, &stdout, &stderr); err == nil || !strings.HasPrefix(err.Error(), test.want) {
			t.Errorf("%q = %v, want %q", test.args, err, test.want)
		}
	}
}

// The rotate-key report names each task's run, tenant, key and credentials and states its horizon, and the tasks
// retention deleted during its scan.
func TestRotateKeyReportStatesItsHorizon(t *testing.T) {
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	var out bytes.Buffer
	writeRotateReport(&out, "pool-a", operator.RotateReport{SuspectKeys: []string{"kid-a", "kid-b"}, Horizon: map[int32]int64{1: 7, 0: 3}, OldestMS: now.Add(-50 * time.Hour).UnixMilli(),
		Tasks: []operator.SuspectTask{{Run: "run-a", Tenant: "tenant-a", KeyID: "kid-a", Credentials: []string{"deploy", "model"}, AppendedMS: now.Add(-time.Hour).UnixMilli()}}}, now)
	want := `pool pool-a's key custodian rotated for the rotate-key request; tasks appended through that rotation are listed
suspect_keys=kid-a,kid-b
horizon partition=0 oldest_retained_offset=3
horizon partition=1 oldest_retained_offset=7
horizon: the oldest retained task was appended at 2026-10-30T10:00:00Z, 50h0m0s ago; tasks deleted by retention before it are not listed
task run=run-a tenant=tenant-a key=kid-a credentials=deploy,model appended=2026-11-01T11:00:00Z
tasks=1
`
	if out.String() != want {
		t.Fatalf("report:\n%s\nwant:\n%s", out.String(), want)
	}
	out.Reset()
	writeRotateReport(&out, "pool-a", operator.RotateReport{SuspectKeys: []string{"kid-a"}, Horizon: map[int32]int64{0: 0}}, now)
	if !strings.Contains(out.String(), "horizon: the task topic retains no task; tasks deleted by retention are not listed\n") {
		t.Fatalf("empty report states no horizon:\n%s", out.String())
	}
	out.Reset()
	writeRotateReport(&out, "pool-a", operator.RotateReport{SuspectKeys: []string{"kid-a"}, Horizon: map[int32]int64{0: 9}, DeletedFrom: map[int32]int64{0: 3}}, now)
	if !strings.Contains(out.String(), "horizon partition=0 oldest_retained_offset=9\nhorizon partition=0: retention deleted the tasks at offsets 3 to 8 during the scan; they may not be listed\n") {
		t.Fatalf("report does not state the tasks deleted during the scan:\n%s", out.String())
	}
}

func TestRevokePrintsTheCauseOfAFailedStep(t *testing.T) {
	operatorEnv(t)
	var stdout, stderr bytes.Buffer
	err := runOperator([]string{"receiver", "remove", "receiver-a", "--wait", "1ms"}, &stdout, &stderr)
	if err == nil || err.Error() != "revocation incomplete" {
		t.Fatalf("revoke = %v, want revocation incomplete", err)
	}
	lines := strings.Split(strings.TrimSuffix(stdout.String(), "\n"), "\n")
	last := lines[len(lines)-1]
	if !strings.Contains(last, "complete=false") || !strings.Contains(last, " error=") || strings.HasSuffix(last, " error=") {
		t.Fatalf("last step line %q, want the failed step with its cause", last)
	}
	for _, line := range lines[:len(lines)-1] {
		if !strings.Contains(line, "complete=true") || strings.Contains(line, "error=") {
			t.Fatalf("step line %q, want completed steps without a cause", line)
		}
	}
}

func TestRevokeRefusesAWaitThatIsNotPositive(t *testing.T) {
	operatorEnv(t)
	const want = "--wait must be positive; omit it to compute the safe wait from the broker's timeouts"
	for _, args := range [][]string{
		{"receiver", "remove", "receiver-a", "--wait", "0s"},
		{"receiver", "remove", "receiver-a", "--wait", "-1s"},
		{"host", "remove", "host-a", "--tenant", "tenant-a", "--wait", "0s"},
		{"host", "remove", "host-a", "--tenant", "tenant-a", "--wait", "-1s"},
	} {
		var stdout bytes.Buffer
		if err := runOperator(args, &stdout, io.Discard); err == nil || err.Error() != want || stdout.Len() != 0 {
			t.Errorf("%q = %v with output %q, want %q before any step", args, err, stdout.String(), want)
		}
	}
	var stdout bytes.Buffer
	if err := runOperator([]string{"receiver", "remove", "receiver-a"}, &stdout, io.Discard); err != nil && err.Error() == want || !strings.HasPrefix(stdout.String(), "step=1 ") {
		t.Fatalf("revoke without --wait = %v with output %q, want the revocation steps run", err, stdout.String())
	}
}

// receiver show prints the recorded configuration; apply --dry-run records nothing and prints what recording the
// file would store, as show prints it once recorded.
// The record decodes numbers as float64; show prints a whole one as an integer.
func TestReceiverShowPrintsWholeNumbersAsIntegers(t *testing.T) {
	var out bytes.Buffer
	config := operator.ReceiverConfig{Capacity: 1, Profiles: []operator.ProfileSettings{{Name: "p", Plugin: "nomad", Agent: "claude",
		Config: map[string]any{"memory_mb": float64(1 << 20), "ratio": 0.5, "sizes": []any{float64(2048000)}}}}}
	if err := showReceiver(&out, config); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"memory_mb: 1048576", "ratio: 0.5", "- 2048000"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("show printed\n%s\nwant %q", out.String(), want)
		}
	}
}

func TestReceiverShowAndDryRun(t *testing.T) {
	service := operatorEnv(t)
	ctx := context.Background()
	terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude"}}, DefaultAgent: "claude", DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
	if err := service.PoolApply(ctx, "pool-a", terms, false); err != nil {
		t.Fatal(err)
	}
	recorded := operator.ReceiverConfig{Receiver: "receiver-a", Pool: "pool-a", Capacity: 3, Profiles: []operator.ProfileSettings{{Name: "old", Plugin: "nomad", Agent: "claude"}},
		PluginSHA256: map[string]string{"nomad": fakeSHA256}}
	if err := service.ReceiverAdd(ctx, recorded, filepath.Join(t.TempDir(), "bundle.yaml")); err != nil {
		t.Fatal(err)
	}
	records := func() int64 {
		t.Helper()
		offsets, err := kadm.NewClient(service.SuperUser).ListEndOffsets(ctx, operator.OperatorTopic)
		if err != nil || offsets.Error() != nil {
			t.Fatal(err, offsets.Error())
		}
		offset, _ := offsets.Lookup(operator.OperatorTopic, 0)
		return offset.Offset
	}
	wherry := func(args ...string) string {
		t.Helper()
		var stdout bytes.Buffer
		if err := runOperator(args, &stdout, io.Discard); err != nil {
			t.Fatalf("%q: %v", args, err)
		}
		return stdout.String()
	}
	shown := `capacity: 3
plugin_sha256:
    nomad: ` + fakeSHA256 + `
profiles:
    - name: old
      plugin: nomad
      agent: claude
      labels: {}
      config: {}
`
	if got := wherry("receiver", "show", "receiver-a"); got != shown {
		t.Fatalf("show printed\n%s\nwant\n%s", got, shown)
	}
	file := filepath.Join(writeTree(t, map[string]string{"receiver.yaml": basePlugin + item("name: claude\nagent: claude\nlabels: {tier: batch}\nconfig: {runtime: {config: {image: claude:1}}}\n", "      ")}), "receiver.yaml")
	before := records()
	pin := "nomad=" + fakeSHA256
	for _, bad := range []string{"nomad=" + strings.ToUpper(fakeSHA256), "nomad=" + fakeSHA256[1:], "nomad=" + fakeSHA256 + "0", "nomad=" + strings.Repeat("g", 64), "Nomad=" + fakeSHA256, "nomad"} {
		if err := runOperator([]string{"receiver", "apply", "receiver-a", "-f", file, "--plugin-sha256", bad}, io.Discard, io.Discard); err == nil || records() != before {
			t.Fatalf("apply --plugin-sha256 %s: %v; want it refused before writing", bad, err)
		}
	}
	if err := runOperator([]string{"receiver", "apply", "receiver-a", "-f", file}, io.Discard, io.Discard); err == nil || err.Error() != "plugin type nomad has no pinned SHA-256; set it with receiver apply --plugin-sha256 nomad=HEX" || records() != before {
		t.Fatalf("apply without a pin: %v; want it refused before writing", err)
	}
	dryRun := wherry("receiver", "apply", "receiver-a", "-f", file, "--plugin-sha256", pin, "--dry-run")
	if records() != before {
		t.Fatal("apply --dry-run produced a record")
	}
	resolved := `capacity: 1
plugin_sha256:
    nomad: ` + fakeSHA256 + `
profiles:
    - name: claude
      plugin: nomad
      agent: claude
      labels:
        platform: nomad
        tier: batch
      config:
        address: https://nomad.example:4646
        credential: nomad-token
        runtime:
            config:
                args:
                    - a
                    - b
                image: claude:1
            driver: docker
            resources:
                cpu: 1000
`
	if dryRun != resolved {
		t.Fatalf("dry run printed\n%s\nwant\n%s", dryRun, resolved)
	}
	if got := wherry("receiver", "apply", "receiver-b", "--pool", "pool-a", "-f", file, "--plugin-sha256", pin, "--dry-run"); got != resolved || records() != before {
		t.Fatalf("apply --dry-run of a new receiver printed\n%s\nand left %d records; want the resolution and %d records", got, records(), before)
	}
	if err := runOperator([]string{"receiver", "show", "receiver-b"}, io.Discard, io.Discard); err == nil {
		t.Fatal("apply --dry-run recorded receiver-b")
	}
	if got := wherry("receiver", "show", "receiver-a"); got != shown {
		t.Fatalf("after a dry run, show printed\n%s\nwant the recorded configuration", got)
	}
	outside := filepath.Join(writeTree(t, map[string]string{"receiver.yaml": basePlugin + item("name: codex\nagent: codex\n", "      ")}), "receiver.yaml")
	if err := runOperator([]string{"receiver", "apply", "receiver-a", "-f", outside, "--plugin-sha256", pin, "--dry-run"}, io.Discard, io.Discard); err == nil || err.Error() != "receiver agent is outside pool terms" {
		t.Fatalf("dry run of an agent outside the pool's terms: %v; want it refused", err)
	}
	if err := runOperator([]string{"receiver", "apply", "receiver-a", "--pool", "pool-b", "-f", file, "--plugin-sha256", pin}, io.Discard, io.Discard); err == nil || err.Error() != "receiver cannot move pool" || records() != before {
		t.Fatalf("apply naming another pool: %v; want it refused before writing", err)
	}
	if got := wherry("receiver", "apply", "receiver-a", "--pool", "pool-a", "-f", file, "--plugin-sha256", pin); got != "" || records() != before+1 {
		t.Fatalf("apply printed %q and left %d records; want nothing printed and one record more than %d", got, records(), before)
	}
	if got := wherry("receiver", "show", "receiver-a"); got != dryRun {
		t.Fatalf("show printed\n%s\nwant what the dry run printed\n%s", got, dryRun)
	}
}

// pool apply creates a pool, records other terms for it and re-applies the share group's delivery limit and lock
// duration, records nothing for the same terms, and with --dry-run writes nothing. A lock duration above the broker's
// ceiling is refused before anything is recorded (agb-345n).
func TestPoolApply(t *testing.T) {
	service := operatorEnv(t)
	ctx := context.Background()
	// runOperator reads the delivery and lock ceilings from the broker, and kfake reports neither until it is set.
	ceiling, lockCeiling := "10", "60000"
	responses, err := kadm.NewClient(service.SuperUser).AlterBrokerConfigs(ctx, []kadm.AlterConfig{{Op: kadm.SetConfig, Name: "group.share.max.delivery.count.limit", Value: &ceiling}, {Op: kadm.SetConfig, Name: "group.share.max.record.lock.duration.ms", Value: &lockCeiling}})
	if err == nil {
		err = responses[0].Err
	}
	if err != nil {
		t.Fatal(err)
	}
	pool := func(limit int, lock string) string {
		return filepath.Join(writeTree(t, map[string]string{"pool.yaml": fmt.Sprintf("agents:\n  - name: claude\ndefault_agent: claude\ndelivery_limit: %d\nlock_duration: %s\nproducer_byte_rate: 1048576\nrequest_percentage: 50\n", limit, lock)}), "pool.yaml")
	}
	records := func() int64 {
		t.Helper()
		offsets, err := kadm.NewClient(service.SuperUser).ListEndOffsets(ctx, operator.OperatorTopic)
		if err != nil || offsets.Error() != nil {
			t.Fatal(err, offsets.Error())
		}
		offset, _ := offsets.Lookup(operator.OperatorTopic, 0)
		return offset.Offset
	}
	terms := func() (string, string, operator.Terms) {
		t.Helper()
		request := kmsg.NewPtrDescribeConfigsRequest()
		resource := kmsg.NewDescribeConfigsRequestResource()
		resource.ResourceType, resource.ResourceName = kmsg.ConfigResourceTypeGroupConfig, operator.PoolGroup("pool-a")
		request.Resources = append(request.Resources, resource)
		response, err := service.SuperUser.Request(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		group := map[string]string{}
		for _, config := range response.(*kmsg.DescribeConfigsResponse).Resources[0].Configs {
			if config.Value != nil {
				group[config.Name] = *config.Value
			}
		}
		state, err := service.LoadState(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return group["share.delivery.count.limit"], group["share.record.lock.duration.ms"], state.Pools["pool-a"]
	}
	apply := func(path string, options ...string) error {
		return runOperator(append([]string{"pool", "apply", "pool-a", "-f", path}, options...), io.Discard, io.Discard)
	}
	if err := apply(pool(5, "15s"), "--dry-run"); err != nil {
		t.Fatal(err)
	}
	if state, err := service.LoadState(ctx); err != nil || len(state.Pools) != 0 {
		t.Fatalf("pools %v, %v; want none recorded by a dry run", state.Pools, err)
	}
	if err := apply(pool(5, "2m")); err == nil || err.Error() != "lock_duration 2m0s exceeds the broker's group.share.max.record.lock.duration.ms 1m0s" {
		t.Fatalf("apply above the lock ceiling = %v, want it refused naming the field and the ceiling", err)
	}
	if state, err := service.LoadState(ctx); err != nil || len(state.Pools) != 0 {
		t.Fatalf("pools %v, %v; want none recorded", state.Pools, err)
	}
	if err := apply(pool(5, "15s")); err != nil {
		t.Fatal(err)
	}
	before := records()
	if limit, lock, recorded := terms(); limit != "5" || lock != "15000" || recorded.DeliveryLimit != 5 || recorded.LockDurationMS != 15000 {
		t.Fatalf("after create: share group %s/%s, terms %d/%d; want 5/15000 throughout", limit, lock, recorded.DeliveryLimit, recorded.LockDurationMS)
	}
	if err := apply(pool(5, "15s")); err != nil || records() != before {
		t.Fatalf("apply of the same terms = %v with %d records; want %d", err, records(), before)
	}
	var stdout bytes.Buffer
	if err := runOperator([]string{"pool", "apply", "pool-a", "-f", pool(3, "30s"), "--dry-run"}, &stdout, io.Discard); err != nil {
		t.Fatal(err)
	}
	if limit, lock, recorded := terms(); records() != before || stdout.Len() != 0 || limit != "5" || lock != "15000" || recorded.DeliveryLimit != 5 || recorded.LockDurationMS != 15000 {
		t.Fatalf("after a dry run: %d records, output %q, share group %s/%s, terms %d/%d; want %d records, no output and 5/15000 throughout", records(), stdout.String(), limit, lock, recorded.DeliveryLimit, recorded.LockDurationMS, before)
	}
	if err := apply(pool(3, "2m")); err == nil || records() != before {
		t.Fatalf("update above the lock ceiling = %v with %d records; want it refused with %d", err, records(), before)
	}
	if err := apply(pool(3, "30s")); err != nil {
		t.Fatal(err)
	}
	if limit, lock, recorded := terms(); records() != before+1 || limit != "3" || lock != "30000" || recorded.DeliveryLimit != 3 || recorded.LockDurationMS != 30000 {
		t.Fatalf("after update: %d records, share group %s/%s, terms %d/%d; want %d records and 3/30000 throughout", records(), limit, lock, recorded.DeliveryLimit, recorded.LockDurationMS, before+1)
	}
}

// bundleWorks reports whether the broker takes the bundle at path, once its credential is stored as kfake reads it.
func bundleWorks(t *testing.T, service *operator.Service, path string, resalted bool) bool {
	t.Helper()
	bundle, err := operator.ReadBundle(path)
	if err != nil {
		t.Fatal(err)
	}
	if resalted {
		resalt(t, service, path)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return connectTLS(t, bundle.Broker, bundle.CA, bundle.Principal, bundle.Password).Ping(ctx) == nil
}

// receiver apply adds a receiver and writes its bundle, refuses to write another for it without --force, and with
// --force gives it a new password and bundle without a record; the old bundle stops working.
func TestReceiverApplyForceReissuesTheIdentity(t *testing.T) {
	service := operatorEnv(t)
	ctx := context.Background()
	terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude"}}, DefaultAgent: "claude", DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
	if err := service.PoolApply(ctx, "pool-a", terms, false); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(writeTree(t, map[string]string{"receiver.yaml": basePlugin + item("name: claude\nagent: claude\n", "      ")}), "receiver.yaml")
	pin := "nomad=" + fakeSHA256
	dir := t.TempDir()
	bundle, other := filepath.Join(dir, "receiver-a.bundle"), filepath.Join(dir, "other.bundle")
	apply := func(options ...string) error {
		return runOperator(append([]string{"receiver", "apply", "receiver-a"}, options...), io.Discard, io.Discard)
	}
	if err := apply("--pool", "pool-a", "-f", file, "--plugin-sha256", pin, "--bundle-file", bundle); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(bundle); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("bundle %v, %v; want it written with mode 0600", info, err)
	}
	state, err := service.LoadState(ctx)
	if err != nil || state.Receivers["receiver-a"].Pool != "pool-a" {
		t.Fatalf("receivers %v, %v; want receiver-a recorded", state.Receivers, err)
	}
	if !bundleWorks(t, service, bundle, true) {
		t.Fatal("the broker refused the new receiver's bundle")
	}
	records := func() int64 {
		t.Helper()
		offsets, err := kadm.NewClient(service.SuperUser).ListEndOffsets(ctx, operator.OperatorTopic)
		if err != nil || offsets.Error() != nil {
			t.Fatal(err, offsets.Error())
		}
		offset, _ := offsets.Lookup(operator.OperatorTopic, 0)
		return offset.Offset
	}
	before := records()
	if err := apply("--pool", "pool-a", "-f", file, "--plugin-sha256", pin, "--bundle-file", other); err == nil || err.Error() != "receiver exists; --force gives it a new password and bundle" {
		t.Fatalf("apply of an existing receiver with a bundle file = %v; want it refused without --force", err)
	}
	if _, err := os.Stat(other); !errors.Is(err, os.ErrNotExist) || records() != before || !bundleWorks(t, service, bundle, false) {
		t.Fatalf("a refused apply wrote %s (%v), left %d records or changed the password", other, err, records())
	}
	if err := apply("--force", "-f", file, "--bundle-file", bundle); err == nil || !strings.HasPrefix(err.Error(), "usage: wherry operator receiver apply NAME --force --bundle-file FILE") {
		t.Fatalf("apply --force with a config = %v; want it refused", err)
	}
	old, err := os.ReadFile(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if err := apply("--force", "--bundle-file", bundle); err != nil {
		t.Fatal(err)
	}
	if records() != before {
		t.Fatalf("apply --force left %d records, want %d", records(), before)
	}
	oldBundle := filepath.Join(dir, "old.bundle")
	if err := os.WriteFile(oldBundle, old, 0o600); err != nil {
		t.Fatal(err)
	}
	if bundleWorks(t, service, oldBundle, false) {
		t.Fatal("the broker took the old bundle after apply --force")
	}
	if !bundleWorks(t, service, bundle, true) {
		t.Fatal("the broker refused the bundle apply --force wrote")
	}
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 2 {
		t.Fatalf("bundle directory holds %v, %v; want the bundle and the old copy alone", entries, err)
	}
}

// host add refuses an existing host without --force, and with --force replaces its bundle with one holding a new
// password; the old bundle stops working.
func TestHostAddForceReissuesTheIdentity(t *testing.T) {
	service := operatorEnv(t)
	dir := t.TempDir()
	bundle, other := filepath.Join(dir, "host-a.bundle"), filepath.Join(dir, "other.bundle")
	add := func(path string, options ...string) error {
		return runOperator(append([]string{"host", "add", "host-a", "--tenant", "tenant-a", "--bundle-file", path}, options...), io.Discard, io.Discard)
	}
	if err := add(bundle); err != nil {
		t.Fatal(err)
	}
	if !bundleWorks(t, service, bundle, true) {
		t.Fatal("the broker refused the new host's bundle")
	}
	for _, path := range []string{other, bundle} {
		if err := add(path); err == nil || err.Error() != "host exists; --force gives it a new password and bundle" {
			t.Fatalf("host add of an existing host to %s = %v; want it refused without --force", path, err)
		}
	}
	if _, err := os.Stat(other); !errors.Is(err, os.ErrNotExist) || !bundleWorks(t, service, bundle, false) {
		t.Fatalf("a refused host add wrote %s (%v) or changed the password", other, err)
	}
	old, err := os.ReadFile(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if err := add(bundle, "--force"); err != nil {
		t.Fatal(err)
	}
	oldBundle := filepath.Join(dir, "old.bundle")
	if err := os.WriteFile(oldBundle, old, 0o600); err != nil {
		t.Fatal(err)
	}
	if bundleWorks(t, service, oldBundle, false) {
		t.Fatal("the broker took the old bundle after host add --force")
	}
	if !bundleWorks(t, service, bundle, true) {
		t.Fatal("the broker refused the bundle host add --force wrote")
	}
	if err := runOperator([]string{"host", "add", "host-b", "--tenant", "tenant-a", "--bundle-file", bundle}, io.Discard, io.Discard); err == nil || !errors.Is(err, os.ErrExist) {
		t.Fatalf("host add of a new host over an existing bundle file = %v; want it refused", err)
	}
}
