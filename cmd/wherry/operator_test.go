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
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestKafkaClientRejectsPlaintextListener(t *testing.T) {
	cluster := kfake.MustCluster(kfake.EnableSASL(), kfake.EnableACLs(), kfake.Superuser("SCRAM-SHA-512", "admin", "password"))
	defer cluster.Close()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	client, err := operatorKafka(cluster.ListenAddrs()[0], pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), "admin", "password")
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

// noTerminal makes the prompts find no terminal for the rest of t, so a command that would prompt refuses.
func noTerminal(t *testing.T) {
	onTerminal(t, "")
	terminal = func() bool { return false }
}

// onTerminal makes the prompts find a terminal for the rest of t, reading answers.
func onTerminal(t *testing.T, answers string) {
	saved, savedStdin := terminal, stdin
	t.Cleanup(func() { terminal, stdin = saved, savedStdin })
	terminal, stdin = func() bool { return true }, strings.NewReader(answers)
}

// operatorEnv makes a working directory on a TLS fake broker, where wherry.operator is a super user, changes into it,
// and returns an operator service on the broker. No terminal is found.
func operatorEnv(t *testing.T) *operator.Service {
	cluster, ca := tlsCluster(t)
	dir := t.TempDir()
	for name, contents := range map[string]string{"ca.pem": ca, "operator.password": "operator-password\n"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	address := cluster.ListenAddrs()[0]
	if err := writeYAML(filepath.Join(dir, "operator.yaml"), operatorConfig{address, "ca.pem", "operator.password"}); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	noTerminal(t)
	return &operator.Service{Operator: connectTLS(t, address, ca, "wherry.operator", "operator-password"),
		Broker: address, CA: []byte(ca), DeliveryCeiling: 10, LockCeilingMS: 60000}
}

func TestOperatorCommandUsage(t *testing.T) {
	operatorEnv(t)
	for _, test := range []struct {
		args []string
		want string
	}{
		{[]string{"host", "add", "host-a"}, "unknown operator command"},
		{[]string{"host", "add", "host-a", "--tenant", "tenant-a"}, "unknown operator command"},
		{[]string{"host", "remove", "host-a", "--tenant", "tenant-a"}, "unknown operator command"},
		{[]string{"pool", "entitle", "p", "--tenant", "tenant-a"}, "unknown operator command"},
		{[]string{"host", "add", "tenant-a", "host-a"}, "unknown operator command"},
		{[]string{"init", "extra"}, "usage: wherry operator init [--dir DIR]"},
		{[]string{"host", "remove", "host-a"}, "unknown operator command"},
		{[]string{"receiver", "remove"}, "usage: wherry operator receiver remove RECEIVER"},
		{[]string{"receiver", "remove", "receiver-a", "receiver-b"}, "usage: wherry operator receiver remove RECEIVER"},
		{[]string{"pool", "entitle", "build"}, "unknown operator command"},
		{[]string{"tenant", "add", "tenant-a", "extra"}, "usage: wherry operator tenant add TENANT"},
		{[]string{"revoke", "host", "tenant-a", "host-a"}, "unknown operator command"},
		{[]string{"revoke", "receiver", "receiver-a"}, "unknown operator command"},
		{[]string{"receiver", "configure", "receiver-a"}, "unknown operator command"},
		{[]string{"skip", "42", "--sha256", strings.Repeat("0", 64)}, "unknown operator command"},
		{[]string{"skip", "42"}, "unknown operator command"},
		{[]string{"pool", "apply"}, "usage: wherry operator pool apply NAME [-f PATH]"},
		{[]string{"receiver", "apply"}, "usage: wherry operator receiver apply NAME"},
		{[]string{"pool", "create", "pool-a"}, "unknown operator command"},
		{[]string{"pool", "update", "pool-a"}, "unknown operator command"},
		{[]string{"receiver", "add", "receiver-a"}, "unknown operator command"},
		{[]string{"receiver", "update", "receiver-a"}, "unknown operator command"},
		{[]string{"receiver", "revoke", "receiver-a"}, "unknown operator command"},
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
	a, b := "wherry.pool.pool-a.tasks.claude."+strings.Repeat("a", 64), "wherry.pool.pool-a.tasks.codex."+strings.Repeat("b", 64)
	var out bytes.Buffer
	writeRotateReport(&out, "pool-a", operator.RotateReport{SuspectKeys: []string{"kid-a", "kid-b"}, Topics: []string{a, b}, Horizon: map[string]int64{b: 7, a: 3}, OldestMS: now.Add(-50 * time.Hour).UnixMilli(),
		Tasks: []operator.SuspectTask{{Run: "run-a", Tenant: "tenant-a", KeyID: "kid-a", Credentials: []string{"deploy", "model"}, AppendedMS: now.Add(-time.Hour).UnixMilli()}}}, now)
	want := `pool pool-a's key custodian rotated for the rotate-key request; tasks appended through that rotation are listed
suspect_keys=kid-a,kid-b
horizon topic=` + a + ` oldest_retained_offset=3
horizon topic=` + b + ` oldest_retained_offset=7
horizon: the oldest retained task was appended at 2026-10-30T10:00:00Z, 50h0m0s ago; tasks deleted by retention before it are not listed
task run=run-a tenant=tenant-a key=kid-a credentials=deploy,model appended=2026-11-01T11:00:00Z
tasks=1
`
	if out.String() != want {
		t.Fatalf("report:\n%s\nwant:\n%s", out.String(), want)
	}
	out.Reset()
	writeRotateReport(&out, "pool-a", operator.RotateReport{SuspectKeys: []string{"kid-a"}, Topics: []string{a}, Horizon: map[string]int64{a: 0}}, now)
	if !strings.Contains(out.String(), "horizon: the task topics retain no task; tasks deleted by retention are not listed\n") {
		t.Fatalf("empty report states no horizon:\n%s", out.String())
	}
	out.Reset()
	writeRotateReport(&out, "pool-a", operator.RotateReport{SuspectKeys: []string{"kid-a"}, Topics: []string{a, b}, Horizon: map[string]int64{a: 9, b: 4}, DeletedFrom: map[string]int64{a: 3}}, now)
	if !strings.Contains(out.String(), "horizon topic="+a+" oldest_retained_offset=9\nhorizon topic="+b+" oldest_retained_offset=4\nhorizon topic="+a+": retention deleted the tasks at offsets 3 to 8 during the scan; they may not be listed\n") ||
		strings.Contains(out.String(), "topic="+b+": retention deleted") {
		t.Fatalf("report does not state, per topic, the tasks deleted during the scan:\n%s", out.String())
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
		{"tenant", "host", "remove", "tenant-a", "host-a", "--wait", "0s"},
		{"tenant", "host", "remove", "tenant-a", "host-a", "--wait", "-1s"},
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
	localFixture(t, ".", "receiver-a")
	ctx := context.Background()
	terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude"}}, DefaultAgent: "claude", DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
	if err := service.PoolApply(ctx, "pool-a", terms, false); err != nil {
		t.Fatal(err)
	}
	recorded := operator.ReceiverConfig{Receiver: "receiver-a", Pool: "pool-a", Capacity: 3, Profiles: []operator.ProfileSettings{{Name: "old", Plugin: "nomad", Agent: "claude"}},
		PluginSHA256: map[string]string{"nomad": fakeSHA256}}
	bundle, err := bundlePath(".", "receivers", true, "receiver-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := service.ReceiverAdd(ctx, recorded, service.Broker, bundle); err != nil {
		t.Fatal(err)
	}
	records := func() int64 {
		t.Helper()
		offsets, err := kadm.NewClient(service.Operator).ListEndOffsets(ctx, operator.OperatorTopic)
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
      signature: "claude\n"
      topic: wherry.pool.pool-a.tasks.claude.98038b25280788a45a2f06c209513024c99adee81c6416b691b4b5872db7e027
`
	if got := wherry("receiver", "show", "receiver-a"); got != shown {
		t.Fatalf("show printed\n%s\nwant\n%s", got, shown)
	}
	profile := item("name: claude\nagent: claude\nlabels: {tier: batch}\nconfig: {runtime: {config: {image: claude:1}}}\n", "      ")
	receiverFile := func(head string) string {
		return filepath.Join(writeTree(t, map[string]string{"receiver.yaml": head + basePlugin + profile}), "receiver.yaml")
	}
	file := receiverFile("plugin_sha256: {nomad: " + fakeSHA256 + "}\n")
	before := records()
	for _, bad := range []string{"{nomad: " + strings.ToUpper(fakeSHA256) + "}", "{nomad: " + fakeSHA256[1:] + "}", "{nomad: " + fakeSHA256 + "0}", "{nomad: " + strings.Repeat("g", 64) + "}", "{Nomad: " + fakeSHA256 + "}", "nomad"} {
		if err := runOperator([]string{"receiver", "apply", "receiver-a", "-f", receiverFile("plugin_sha256: " + bad + "\n")}, io.Discard, io.Discard); err == nil || records() != before {
			t.Fatalf("apply with plugin_sha256 %s: %v; want it refused before writing", bad, err)
		}
	}
	if err := runOperator([]string{"receiver", "apply", "receiver-a", "-f", receiverFile("")}, io.Discard, io.Discard); err == nil || err.Error() != "plugin type nomad has no pinned SHA-256; set plugin_sha256.nomad in the receiver's file and run receiver apply" || records() != before {
		t.Fatalf("apply without a pin: %v; want it refused before writing", err)
	}
	dryRun := wherry("receiver", "apply", "receiver-a", "-f", file, "--dry-run")
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
      signature: "claude\nplatform=nomad\ntier=batch\n"
      topic: wherry.pool.pool-a.tasks.claude.cead32c5802443c217b9da08a4754cb354f23e5010c834676e5c976ab43ef341
`
	if dryRun != resolved {
		t.Fatalf("dry run printed\n%s\nwant\n%s", dryRun, resolved)
	}
	if got := wherry("receiver", "apply", "receiver-b", "-f", receiverFile("pool: pool-a\nplugin_sha256: {nomad: "+fakeSHA256+"}\n"), "--dry-run"); got != resolved || records() != before {
		t.Fatalf("apply --dry-run of a new receiver printed\n%s\nand left %d records; want the resolution and %d records", got, records(), before)
	}
	if err := runOperator([]string{"receiver", "show", "receiver-b"}, io.Discard, io.Discard); err == nil {
		t.Fatal("apply --dry-run recorded receiver-b")
	}
	if got := wherry("receiver", "show", "receiver-a"); got != shown {
		t.Fatalf("after a dry run, show printed\n%s\nwant the recorded configuration", got)
	}
	outside := filepath.Join(writeTree(t, map[string]string{"receiver.yaml": "plugin_sha256: {nomad: " + fakeSHA256 + "}\n" + basePlugin + item("name: codex\nagent: codex\n", "      ")}), "receiver.yaml")
	if err := runOperator([]string{"receiver", "apply", "receiver-a", "-f", outside, "--dry-run"}, io.Discard, io.Discard); err == nil || err.Error() != "receiver agent is outside pool terms" {
		t.Fatalf("dry run of an agent outside the pool's terms: %v; want it refused", err)
	}
	if err := runOperator([]string{"receiver", "apply", "receiver-a", "-f", receiverFile("pool: pool-b\nplugin_sha256: {nomad: " + fakeSHA256 + "}\n")}, io.Discard, io.Discard); err == nil || err.Error() != "receiver cannot move pool" || records() != before {
		t.Fatalf("apply naming another pool: %v; want it refused before writing", err)
	}
	if got := wherry("receiver", "apply", "receiver-a", "-f", receiverFile("pool: pool-a\nplugin_sha256: {nomad: "+fakeSHA256+"}\n")); got != "" || records() != before+1 {
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
	responses, err := kadm.NewClient(service.Operator).AlterBrokerConfigs(ctx, []kadm.AlterConfig{{Op: kadm.SetConfig, Name: "group.share.max.delivery.count.limit", Value: &ceiling}, {Op: kadm.SetConfig, Name: "group.share.max.record.lock.duration.ms", Value: &lockCeiling}})
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
		offsets, err := kadm.NewClient(service.Operator).ListEndOffsets(ctx, operator.OperatorTopic)
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
		response, err := service.Operator.Request(ctx, request)
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
	// A dry run and a refused apply on a fresh cluster record nothing and create no wherry.operator. The metadata request
	// goes through its own client, whose cache of the absent topic the later listings must not read.
	absent := func() bool {
		t.Helper()
		client, err := kgo.NewClient(service.Operator.Opts()...)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		request := kmsg.NewPtrMetadataRequest()
		topic := kmsg.NewMetadataRequestTopic()
		topic.Topic = kmsg.StringPtr(operator.OperatorTopic)
		request.Topics = append(request.Topics, topic)
		response, err := request.RequestWith(ctx, client)
		if err != nil {
			t.Fatal(err)
		}
		return response.Topics[0].ErrorCode == kerr.UnknownTopicOrPartition.Code
	}
	if err := apply(pool(5, "15s"), "--dry-run"); err != nil {
		t.Fatal(err)
	}
	if !absent() {
		t.Fatal("a dry run created wherry.operator")
	}
	if err := apply(pool(5, "2m")); err == nil || err.Error() != "lock_duration 2m0s exceeds the broker's group.share.max.record.lock.duration.ms 1m0s" {
		t.Fatalf("apply above the lock ceiling = %v, want it refused naming the field and the ceiling", err)
	}
	if !absent() {
		t.Fatal("an apply refused for its lock duration created wherry.operator")
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

// bundleWorks reports whether the broker takes the bundle at path.
func bundleWorks(t *testing.T, service *operator.Service, path string) bool {
	t.Helper()
	bundle, err := operator.ReadBundle(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return connectTLS(t, bundle.Broker, string(service.CA), bundle.Principal, bundle.Password).Ping(ctx) == nil
}

// receiver apply adds a receiver and writes its bundle under bundles/receivers, mode 0600 in a directory of mode 0700;
// applied again it updates the receiver and keeps the password; with --force it gives it a new password and bundle
// without a record, and the old bundle stops working.
func TestReceiverApplyForceReissuesTheIdentity(t *testing.T) {
	service := operatorEnv(t)
	localFixture(t, ".", "receiver-a")
	ctx := context.Background()
	terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude"}}, DefaultAgent: "claude", DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
	if err := service.PoolApply(ctx, "pool-a", terms, false); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(writeTree(t, map[string]string{"receiver.yaml": "pool: pool-a\nplugin_sha256: {nomad: " + fakeSHA256 + "}\n" + basePlugin + item("name: claude\nagent: claude\n", "      ")}), "receiver.yaml")
	bundle := filepath.Join("bundles", "receivers", "receiver-a.bundle")
	apply := func(options ...string) error {
		return runOperator(append([]string{"receiver", "apply", "receiver-a"}, options...), io.Discard, io.Discard)
	}
	if err := apply("-f", file); err != nil {
		t.Fatal(err)
	}
	for path, mode := range map[string]os.FileMode{"bundles": 0o700, filepath.Join("bundles", "receivers"): 0o700, bundle: 0o600} {
		if info, err := os.Stat(path); err != nil || info.Mode().Perm() != mode {
			t.Fatalf("%s: %v, %v; want mode %v", path, info, err, mode)
		}
	}
	state, err := service.LoadState(ctx)
	if err != nil || state.Receivers["receiver-a"].Pool != "pool-a" {
		t.Fatalf("receivers %v, %v; want receiver-a recorded", state.Receivers, err)
	}
	if !bundleWorks(t, service, bundle) {
		t.Fatal("the broker refused the new receiver's bundle")
	}
	records := func() int64 {
		t.Helper()
		offsets, err := kadm.NewClient(service.Operator).ListEndOffsets(ctx, operator.OperatorTopic)
		if err != nil || offsets.Error() != nil {
			t.Fatal(err, offsets.Error())
		}
		offset, _ := offsets.Lookup(operator.OperatorTopic, 0)
		return offset.Offset
	}
	before := records()
	old, err := os.ReadFile(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if err := apply("-f", file); err != nil {
		t.Fatal(err)
	}
	if again, err := os.ReadFile(bundle); err != nil || !bytes.Equal(again, old) || records() != before+1 || !bundleWorks(t, service, bundle) {
		t.Fatalf("apply of an existing receiver left %d records and changed its bundle (%v) or password; want an update record alone", records(), err)
	}
	if err := apply("--force", "-f", file); err == nil || !strings.HasPrefix(err.Error(), "usage: wherry operator receiver apply NAME --force") {
		t.Fatalf("apply --force with a config = %v; want it refused", err)
	}
	if err := apply("--force"); err != nil {
		t.Fatal(err)
	}
	if records() != before+1 {
		t.Fatalf("apply --force left %d records, want %d", records(), before+1)
	}
	oldBundle := filepath.Join(t.TempDir(), "old.bundle")
	if err := os.WriteFile(oldBundle, old, 0o600); err != nil {
		t.Fatal(err)
	}
	if bundleWorks(t, service, oldBundle) {
		t.Fatal("the broker took the old bundle after apply --force")
	}
	if !bundleWorks(t, service, bundle) {
		t.Fatal("the broker refused the bundle apply --force wrote")
	}
	if entries, err := os.ReadDir(filepath.Join("bundles", "receivers")); err != nil || len(entries) != 1 {
		t.Fatalf("bundles/receivers holds %v, %v; want the bundle alone", entries, err)
	}
}

// host add writes the host's bundle under bundles/hosts/<tenant>, refuses an existing host without --force, and with
// --force replaces its bundle with one holding a new password; the old bundle stops working. A host of the same name in
// another tenant has a bundle of its own, which neither its add nor its --force touches the first's.
func TestHostAddForceReissuesTheIdentity(t *testing.T) {
	service := operatorEnv(t)
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		if err := service.TenantAdd(t.Context(), tenant); err != nil {
			t.Fatal(err)
		}
	}
	bundle := filepath.Join("bundles", "hosts", "tenant-a", "host-a.bundle")
	add := func(tenant string, options ...string) error {
		return runOperator(append([]string{"tenant", "host", "add", tenant, "host-a"}, options...), io.Discard, io.Discard)
	}
	if err := add("tenant-a"); err != nil {
		t.Fatal(err)
	}
	for path, mode := range map[string]os.FileMode{"bundles": 0o700, filepath.Join("bundles", "hosts"): 0o700, filepath.Dir(bundle): 0o700, bundle: 0o600} {
		if info, err := os.Stat(path); err != nil || info.Mode().Perm() != mode {
			t.Fatalf("%s: %v, %v; want mode %v", path, info, err, mode)
		}
	}
	if !bundleWorks(t, service, bundle) {
		t.Fatal("the broker refused the new host's bundle")
	}
	// The bundle's file is reserved before the host is looked up.
	if err := add("tenant-a"); !errors.Is(err, os.ErrExist) {
		t.Fatalf("host add of an existing host to its bundle = %v; want the file refused", err)
	}
	aside := filepath.Join(t.TempDir(), "aside.bundle")
	if err := os.Rename(bundle, aside); err != nil {
		t.Fatal(err)
	}
	if err := add("tenant-a"); err == nil || err.Error() != "host exists; --force gives it a new password and bundle" {
		t.Fatalf("host add of an existing host = %v; want it refused without --force", err)
	}
	if _, err := os.Stat(bundle); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a refused host add left %s: %v", bundle, err)
	}
	if err := os.Rename(aside, bundle); err != nil {
		t.Fatal(err)
	}
	if !bundleWorks(t, service, bundle) {
		t.Fatal("a refused host add changed the password")
	}
	old, err := os.ReadFile(bundle)
	if err != nil {
		t.Fatal(err)
	}
	hosts := func() int {
		t.Helper()
		reader := operator.NewReader(service.Operator)
		defer reader.Close()
		records, err := reader.ReadFrom(context.Background(), operator.OperatorTopic, 0)
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for _, record := range records {
			if decoded, err := operator.DecodeRecord(record.Value); err == nil && decoded.Kind == "host" {
				count++
			}
		}
		return count
	}
	before := hosts()
	if err := add("tenant-a", "--force"); err != nil {
		t.Fatal(err)
	}
	if after := hosts(); before != 1 || after != before {
		t.Fatalf("host add --force left %d host records, %d before; want the one record untouched", after, before)
	}
	oldBundle := filepath.Join(t.TempDir(), "old.bundle")
	if err := os.WriteFile(oldBundle, old, 0o600); err != nil {
		t.Fatal(err)
	}
	if bundleWorks(t, service, oldBundle) {
		t.Fatal("the broker took the old bundle after host add --force")
	}
	if !bundleWorks(t, service, bundle) {
		t.Fatal("the broker refused the bundle host add --force wrote")
	}
	first, err := os.ReadFile(bundle)
	if err != nil {
		t.Fatal(err)
	}
	other := filepath.Join("bundles", "hosts", "tenant-b", "host-a.bundle")
	for _, options := range [][]string{nil, {"--force"}} {
		if err := add("tenant-b", options...); err != nil {
			t.Fatalf("host add of host-a in another tenant %v = %v", options, err)
		}
		if !bundleWorks(t, service, other) {
			t.Fatalf("the broker refused the other tenant's bundle host add %v wrote", options)
		}
		if after, err := os.ReadFile(bundle); err != nil || !bytes.Equal(after, first) {
			t.Fatalf("host add of host-a in another tenant %v changed %s: %v", options, bundle, err)
		}
	}
	if !bundleWorks(t, service, bundle) {
		t.Fatal("the broker refused the first tenant's bundle after the other tenant's host add")
	}
}

func TestTenantTreeAndList(t *testing.T) {
	service := operatorEnv(t)
	command := func(args ...string) (string, error) {
		var out bytes.Buffer
		err := runOperator(args, &out, io.Discard)
		return out.String(), err
	}
	for _, tenant := range []string{"z", "a"} {
		if _, err := command("tenant", "add", tenant); err != nil {
			t.Fatal(err)
		}
	}
	if err := service.PoolApply(t.Context(), "p", agentTerms(), false); err != nil {
		t.Fatal(err)
	}
	if _, err := command("tenant", "pool", "add", "a", "p"); err != nil {
		t.Fatal(err)
	}
	if out, err := command("tenant", "pool", "add", "a", "p"); err != nil || out != "already entitled\n" {
		t.Fatalf("already path: %q %v", out, err)
	}
	if _, err := command("tenant", "host", "add", "a", "live"); err != nil {
		t.Fatal(err)
	}
	if _, err := command("tenant", "host", "add", "a", "revoking"); err != nil {
		t.Fatal(err)
	}
	if _, err := command("tenant", "host", "add", "a", "ended"); err != nil {
		t.Fatal(err)
	}
	for _, record := range []operator.Record{{Kind: "revoking", Tenant: "a", Host: "revoking"}, {Kind: "end", Tenant: "a", Host: "ended"}} {
		encoded, err := operator.EncodeRecord(record)
		if err != nil {
			t.Fatal(err)
		}
		if err := service.Operator.ProduceSync(t.Context(), &kgo.Record{Topic: operator.OperatorTopic, Key: []byte(record.Kind + record.Host), Value: encoded}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	if out, err := command("tenant", "list"); err != nil || out != "a\n  pool p\n  host live\nz\n" {
		t.Fatalf("list=%q %v", out, err)
	}
	if _, err := command("tenant", "pool", "remove", "a", "p"); err != nil {
		t.Fatal(err)
	}
	if out, err := command("tenant", "pool", "remove", "a", "p"); err != nil || out != "not entitled\n" {
		t.Fatalf("absent path: %q %v", out, err)
	}
	if _, err := command("tenant", "remove", "z"); err != nil {
		t.Fatal(err)
	}
	bundleDir := filepath.Join("bundles", "hosts", "z")
	if err := os.MkdirAll(bundleDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundleDir, "leftover.bundle"), []byte("leftover"), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := command("tenant", "remove", "z"); err != nil || out != "done\n" {
		t.Fatalf("ended retry: %q %v", out, err)
	}
	if _, err := os.Stat(bundleDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("bundle directory remains: %v", err)
	}
	if out, err := command("tenant", "list"); err != nil || out != "a\n  host live\n" {
		t.Fatalf("list=%q %v", out, err)
	}
}
