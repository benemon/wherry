package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestKafkaClientRejectsPlaintextListener(t *testing.T) {
	cluster := kfake.MustCluster(kfake.EnableSASL(), kfake.EnableACLs(), kfake.Superuser("SCRAM-SHA-512", "admin", "password"))
	defer cluster.Close()
	client, err := operatorKafka(cluster.ListenAddrs()[0], "admin", "password", nil)
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

// noTerminal gives the prompts a stdin that is no terminal for the rest of t, so a command that would prompt refuses.
func noTerminal(t *testing.T) {
	saved := stdin
	t.Cleanup(func() { stdin = saved })
	stdin = strings.NewReader("")
}

// operatorEnv makes a working directory on a TLS fake broker, where wherry.operator is a super user, changes into it,
// and returns an operator service on the broker. No terminal is found.
func operatorEnv(t *testing.T) *operator.Service {
	cluster, ca := tlsCluster(t)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "operator.password"), []byte("operator-password\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	address := cluster.ListenAddrs()[0]
	if err := writeYAML(filepath.Join(dir, "operator.yaml"), operatorConfig{Broker: address, PasswordFile: "operator.password", CACert: testCA.pem}); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	noTerminal(t)
	return &operator.Service{Operator: connectTLS(t, address, ca, "wherry.operator", "operator-password"),
		Broker: address, Roots: testRoots(t), DeliveryCeiling: 10, LockCeilingMS: 60000}
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
		{[]string{"remove"}, operatorUsage["remove"]},
		{[]string{"remove", "--receiver", "receiver-a", "receiver-b"}, operatorUsage["remove"]},
		{[]string{"pool", "entitle", "build"}, "unknown operator command"},
		{[]string{"add", "--tenant", "tenant-a", "extra"}, operatorUsage["add"]},
		{[]string{"revoke", "host", "tenant-a", "host-a"}, "unknown operator command"},
		{[]string{"revoke", "receiver", "receiver-a"}, "unknown operator command"},
		{[]string{"receiver", "configure", "receiver-a"}, "unknown operator command"},
		{[]string{"skip", "42", "--sha256", strings.Repeat("0", 64)}, "unknown operator command"},
		{[]string{"skip", "42"}, "unknown operator command"},
		{[]string{"apply"}, operatorUsage["apply"]},
		{[]string{"apply", "--receiver"}, "flag needs an argument: -receiver"},
		{[]string{"pool", "create", "pool-a"}, "unknown operator command"},
		{[]string{"pool", "update", "pool-a"}, "unknown operator command"},
		{[]string{"receiver", "add", "receiver-a"}, "unknown operator command"},
		{[]string{"receiver", "update", "receiver-a"}, "unknown operator command"},
		{[]string{"receiver", "revoke", "receiver-a"}, "unknown operator command"},
		{[]string{"suspend"}, operatorUsage["suspend"]},
		{[]string{"resume", "--pool", "pool-a", "pool-b"}, operatorUsage["resume"]},
		{[]string{"rotate"}, operatorUsage["rotate"]},
		{[]string{"suspend", "--pool", "pool-a", "--suspect-key", "kid"}, "unknown option --suspect-key"},
		{[]string{"list"}, operatorUsage["list"]},
		{[]string{"list", "pools", "extra"}, operatorUsage["list"]},
		{[]string{"list", "hosts", "--tenant", "tenant-a", "extra"}, operatorUsage["list"]},
		{[]string{"list", "all"}, operatorUsage["list"]},
		{[]string{"storage", "--tenant", "tenant-a", "x"}, operatorUsage["storage"]},
		// The forms before the verb-first grammar are unknown commands, as are their shortest prefixes.
		{[]string{"tenant", "add", "tenant-a"}, "unknown operator command"},
		{[]string{"tenant", "list"}, "unknown operator command"},
		{[]string{"tenant", "storage", "tenant-a", "-"}, "unknown operator command"},
		{[]string{"tenant", "remove", "tenant-a"}, "unknown operator command"},
		{[]string{"tenant", "pool", "add", "tenant-a", "pool-a"}, "unknown operator command"},
		{[]string{"tenant", "pool", "remove", "tenant-a", "pool-a"}, "unknown operator command"},
		{[]string{"tenant", "host", "add", "tenant-a", "host-a"}, "unknown operator command"},
		{[]string{"tenant", "host", "refresh", "tenant-a", "host-a"}, "unknown operator command"},
		{[]string{"tenant", "host", "remove", "tenant-a", "host-a"}, "unknown operator command"},
		{[]string{"pool", "apply", "pool-a"}, "unknown operator command"},
		{[]string{"pool", "suspend", "pool-a"}, "unknown operator command"},
		{[]string{"pool", "resume", "pool-a"}, "unknown operator command"},
		{[]string{"pool", "rotate-key", "pool-a"}, "unknown operator command"},
		{[]string{"receiver", "apply", "receiver-a"}, "unknown operator command"},
		{[]string{"receiver", "show", "receiver-a"}, "unknown operator command"},
		{[]string{"receiver", "remove", "receiver-a"}, "unknown operator command"},
		{[]string{"tenant"}, "unknown operator command"},
		{[]string{"pool"}, "unknown operator command"},
		{[]string{"receiver"}, "unknown operator command"},
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
	err := runOperator([]string{"remove", "--receiver", "receiver-a", "--timeout", "1ms"}, &stdout, &stderr)
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
	const want = "--timeout must be positive; omit it to compute the safe wait from the broker's timeouts"
	for _, args := range [][]string{
		{"remove", "--receiver", "receiver-a", "--timeout", "0s"},
		{"remove", "--receiver", "receiver-a", "--timeout", "-1s"},
		{"remove", "--host", "host-a", "--tenant", "tenant-a", "--timeout", "0s"},
		{"remove", "--host", "host-a", "--tenant", "tenant-a", "--timeout", "-1s"},
	} {
		var stdout bytes.Buffer
		if err := runOperator(args, &stdout, io.Discard); err == nil || err.Error() != want || stdout.Len() != 0 {
			t.Errorf("%q = %v with output %q, want %q before any step", args, err, stdout.String(), want)
		}
	}
	var stdout bytes.Buffer
	if err := runOperator([]string{"remove", "--receiver", "receiver-a"}, &stdout, io.Discard); err != nil && err.Error() == want || !strings.HasPrefix(stdout.String(), "step=1 ") {
		t.Fatalf("revoke without --wait = %v with output %q, want the revocation steps run", err, stdout.String())
	}
}

// receiver show prints the recorded configuration; apply --dry-run records nothing and prints what recording the
// file would store, as show prints it once recorded.
// The record decodes numbers as float64; show prints a whole one as an integer.
func TestReceiverShowPrintsWholeNumbersAsIntegers(t *testing.T) {
	var out bytes.Buffer
	config := operator.ReceiverConfig{Profiles: []operator.ProfileSettings{{Name: "p", Plugin: "nomad", Agent: "claude",
		Config: map[string]any{"memory_mb": float64(1 << 20), "ratio": 0.5, "sizes": []any{float64(2048000)}}}}}
	if err := showReceiver(&out, config, nil); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"memory_mb: 1048576", "ratio: 0.5", "- 2048000"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("show printed\n%s\nwant %q", out.String(), want)
		}
	}
}

// receiver show reports the receiver's liveness as a host does: live within 120 s of its latest authorised
// advertisement, stale beyond it, past or future, silent without one and ended once revoked; only the receiver's own
// key signs an advertisement that counts.
func TestReceiverShowReportsLiveness(t *testing.T) {
	service := operatorEnv(t)
	ctx := context.Background()
	terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude"}}, DefaultAgent: "claude", DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
	if err := service.PoolApply(ctx, "pool-a", terms, false); err != nil {
		t.Fatal(err)
	}
	config := operator.ReceiverConfig{Receiver: "receiver-a", Pool: "pool-a", Profiles: []operator.ProfileSettings{{Name: "claude", Plugin: "nomad", Agent: "claude"}}, PluginSHA256: map[string]string{"nomad": fakeSHA256}}
	path, err := bundlePath(".", "receivers", true, "receiver-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := service.ReceiverAdd(ctx, config, service.Broker, path); err != nil {
		t.Fatal(err)
	}
	bundle, err := operator.ReadBundle(path)
	if err != nil {
		t.Fatal(err)
	}
	receiver := connectTLS(t, bundle.Broker, testCA.pem, bundle.Principal, bundle.Password)
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, stranger, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	produce := func(topic, recordKey string, value []byte) {
		t.Helper()
		if err := receiver.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(recordKey), Value: value}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	advertise := func(at time.Time, key ed25519.PrivateKey) {
		t.Helper()
		encoded, err := wire.SignRecord(&wire.Advertisement{Receiver: "receiver-a", Pool: "pool-a", Offers: []wire.Offer{{Profile: "claude", Agent: "claude"}}, Capacity: 1, AtMS: at.UnixMilli()}, key)
		if err != nil {
			t.Fatal(err)
		}
		produce(operator.PoolReceiversTopic("pool-a"), "receiver-a", encoded)
	}
	status := func() (string, string) {
		t.Helper()
		var stdout bytes.Buffer
		if err := runOperator([]string{"show", "--receiver", "receiver-a"}, &stdout, io.Discard); err != nil {
			t.Fatal(err)
		}
		lines := strings.SplitN(stdout.String(), "\n", 3)
		if !strings.HasPrefix(lines[0], "status: ") {
			t.Fatalf("show printed\n%s\nwant the status first", stdout.String())
		}
		advertised := ""
		if strings.HasPrefix(lines[1], "last_advertised: ") {
			advertised = strings.TrimPrefix(lines[1], "last_advertised: ")
		}
		return strings.TrimPrefix(lines[0], "status: "), advertised
	}
	if got, at := status(); got != "silent" || at != "" {
		t.Fatalf("status before any advertisement %q at %q; want silent", got, at)
	}
	registration, err := signed.RegisterKey("receiver-a", key)
	if err != nil {
		t.Fatal(err)
	}
	produce(operator.ReceiverTopic("receiver-a"), "receiver-a", registration)
	advertise(time.Now(), stranger)
	if got, _ := status(); got != "silent" {
		t.Fatalf("status with an advertisement under another key %q; want silent", got)
	}
	old := time.Now().Add(-121 * time.Second)
	advertise(old, key)
	if got, at := status(); got != "stale" || !strings.HasPrefix(at, old.UTC().Format(time.RFC3339)[:19]) {
		t.Fatalf("status with a 121 s old advertisement %q at %q; want stale at %s", got, at, old.UTC().Format(time.RFC3339))
	}
	advertise(time.Now(), key)
	if got, at := status(); got != "live" || at == "" {
		t.Fatalf("status with a current advertisement %q at %q; want live with its time", got, at)
	}
	// The latest advertisement by its own date counts, so one dated ahead masks the receiver until that time passes.
	advertise(time.Now().Add(121*time.Second), key)
	if got, _ := status(); got != "stale" {
		t.Fatalf("status with an advertisement dated 121 s ahead %q; want stale", got)
	}
	revocationBounds(t, service, "1")
	if result := service.RevokeReceiver(ctx, "receiver-a", 0, deniedProbe(service, ""), nil); !result.Complete {
		t.Fatalf("revocation incomplete: %+v", result)
	}
	if got, at := status(); got != "ended" || at != "" {
		t.Fatalf("status after revocation %q at %q; want ended", got, at)
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
	recorded := operator.ReceiverConfig{Receiver: "receiver-a", Pool: "pool-a", Profiles: []operator.ProfileSettings{{Name: "old", Plugin: "nomad", Agent: "claude"}},
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
	shown := `plugin_sha256:
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
	// Nothing has advertised as receiver-a, so show reports it silent before the record.
	if got := wherry("show", "--receiver", "receiver-a"); got != "status: silent\n"+shown {
		t.Fatalf("show printed\n%s\nwant\n%s", got, "status: silent\n"+shown)
	}
	profile := item("name: claude\nagent: claude\nlabels: {tier: batch}\nconfig: {runtime: {config: {image: claude:1}}}\n", "      ")
	receiverFile := func(head string) string {
		return filepath.Join(writeTree(t, map[string]string{"receiver.yaml": head + basePlugin + profile}), "receiver.yaml")
	}
	file := receiverFile("plugin_sha256: {nomad: " + fakeSHA256 + "}\n")
	before := records()
	for _, bad := range []string{"{nomad: " + strings.ToUpper(fakeSHA256) + "}", "{nomad: " + fakeSHA256[1:] + "}", "{nomad: " + fakeSHA256 + "0}", "{nomad: " + strings.Repeat("g", 64) + "}", "{Nomad: " + fakeSHA256 + "}", "nomad"} {
		if err := runOperator([]string{"apply", "--receiver", "receiver-a", "--file", receiverFile("plugin_sha256: " + bad + "\n")}, io.Discard, io.Discard); err == nil || records() != before {
			t.Fatalf("apply with plugin_sha256 %s: %v; want it refused before writing", bad, err)
		}
	}
	if err := runOperator([]string{"apply", "--receiver", "receiver-a", "--file", receiverFile("")}, io.Discard, io.Discard); err == nil || err.Error() != "plugin type nomad has no pinned SHA-256; set plugin_sha256.nomad in the receiver's file and run apply --receiver" || records() != before {
		t.Fatalf("apply without a pin: %v; want it refused before writing", err)
	}
	dryRun := wherry("apply", "--receiver", "receiver-a", "--file", file, "--dry-run")
	if records() != before {
		t.Fatal("apply --dry-run produced a record")
	}
	resolved := `plugin_sha256:
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
	if got := wherry("apply", "--receiver", "receiver-b", "--file", receiverFile("pool: pool-a\nplugin_sha256: {nomad: "+fakeSHA256+"}\n"), "--dry-run"); got != resolved || records() != before {
		t.Fatalf("apply --dry-run of a new receiver printed\n%s\nand left %d records; want the resolution and %d records", got, records(), before)
	}
	if err := runOperator([]string{"show", "--receiver", "receiver-b"}, io.Discard, io.Discard); err == nil {
		t.Fatal("apply --dry-run recorded receiver-b")
	}
	if got := wherry("show", "--receiver", "receiver-a"); got != "status: silent\n"+shown {
		t.Fatalf("after a dry run, show printed\n%s\nwant the recorded configuration", got)
	}
	outside := filepath.Join(writeTree(t, map[string]string{"receiver.yaml": "plugin_sha256: {nomad: " + fakeSHA256 + "}\n" + basePlugin + item("name: codex\nagent: codex\n", "      ")}), "receiver.yaml")
	if err := runOperator([]string{"apply", "--receiver", "receiver-a", "--file", outside, "--dry-run"}, io.Discard, io.Discard); err == nil || err.Error() != "receiver agent is outside pool terms" {
		t.Fatalf("dry run of an agent outside the pool's terms: %v; want it refused", err)
	}
	if err := runOperator([]string{"apply", "--receiver", "receiver-a", "--file", receiverFile("pool: pool-b\nplugin_sha256: {nomad: " + fakeSHA256 + "}\n")}, io.Discard, io.Discard); err == nil || err.Error() != "receiver cannot move pool" || records() != before {
		t.Fatalf("apply naming another pool: %v; want it refused before writing", err)
	}
	if got := wherry("apply", "--receiver", "receiver-a", "--file", receiverFile("pool: pool-a\nplugin_sha256: {nomad: "+fakeSHA256+"}\n")); got != "" || records() != before+1 {
		t.Fatalf("apply printed %q and left %d records; want nothing printed and one record more than %d", got, records(), before)
	}
	if got := wherry("show", "--receiver", "receiver-a"); got != "status: silent\n"+dryRun {
		t.Fatalf("show printed\n%s\nwant what the dry run printed, after the status\n%s", got, dryRun)
	}
}

// revocationBounds sets the broker timeouts that bound revocation's step 5 on the fake broker, which otherwise reports
// none.
func revocationBounds(t *testing.T, service *operator.Service, value string) {
	t.Helper()
	var alters []kadm.AlterConfig
	for _, name := range []string{"connections.max.reauth.ms", "group.share.max.record.lock.duration.ms", "transaction.max.timeout.ms"} {
		alters = append(alters, kadm.AlterConfig{Op: kadm.SetConfig, Name: name, Value: &value})
	}
	responses, err := kadm.NewClient(service.Operator).AlterBrokerConfigs(context.Background(), alters)
	if err == nil {
		err = responses[0].Err
	}
	if err != nil {
		t.Fatal(err)
	}
}

// deniedProbe stands for every broker denying the revoked principal, which the fake broker's authorisation answers do
// not reproduce.
func deniedProbe(*operator.Service, string) func(context.Context, string, string) error {
	return func(context.Context, string, string) error { return operator.ErrAuthorizationDenied }
}

// stubProbe replaces the remove commands' probe for the test.
func stubProbe(t *testing.T, probe func(*operator.Service, string) func(context.Context, string, string) error) {
	t.Helper()
	saved := revocationProbe
	revocationProbe = probe
	t.Cleanup(func() { revocationProbe = saved })
}

// lineWriter tells each complete line written to it, with the time the line arrived.
type lineWriter struct {
	mu   sync.Mutex
	rest []byte
	told func(line string, at time.Time)
}

func (w *lineWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.rest = append(w.rest, p...)
	for {
		i := bytes.IndexByte(w.rest, '\n')
		if i < 0 {
			return len(p), nil
		}
		w.told(string(w.rest[:i]), time.Now())
		w.rest = w.rest[i+1:]
	}
}

// remove prints each step as it ends and step 5 as it starts, while the command is still in its wait, with the time
// that wait ends; it deletes the principal's bundle once the revocation is complete, keeps it after an incomplete one,
// and deletes it on a rerun that finds the principal already ended.
func TestRemoveDeletesTheBundleOnceRevocationCompletes(t *testing.T) {
	service := operatorEnv(t)
	ctx := context.Background()
	if err := service.TenantAdd(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if err := runOperator([]string{"add", "--host", "host-a", "--tenant", "tenant-a"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	hostBundle := filepath.Join("bundles", "hosts", "tenant-a", "host-a.bundle")
	terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude"}}, DefaultAgent: "claude", DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
	if err := service.PoolApply(ctx, "pool-a", terms, false); err != nil {
		t.Fatal(err)
	}
	receiverBundle, err := bundlePath(".", "receivers", true, "receiver-a")
	if err != nil {
		t.Fatal(err)
	}
	config := operator.ReceiverConfig{Receiver: "receiver-a", Pool: "pool-a", Profiles: []operator.ProfileSettings{{Name: "claude", Plugin: "nomad", Agent: "claude"}}, PluginSHA256: map[string]string{"nomad": fakeSHA256}}
	if err := service.ReceiverAdd(ctx, config, service.Broker, receiverBundle); err != nil {
		t.Fatal(err)
	}
	// After the pool, whose lock duration the share-group ceiling bounds.
	revocationBounds(t, service, "1")
	exists := func(path string) bool {
		t.Helper()
		_, err := os.Stat(path)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		return err == nil
	}
	stubProbe(t, func(*operator.Service, string) func(context.Context, string, string) error {
		return func(context.Context, string, string) error { return errors.New("injected probe failure") }
	})
	var stdout bytes.Buffer
	if err := runOperator([]string{"remove", "--host", "host-a", "--tenant", "tenant-a"}, &stdout, io.Discard); err == nil || err.Error() != "revocation incomplete" {
		t.Fatalf("remove with a failing probe = %v; want revocation incomplete", err)
	}
	if !exists(hostBundle) {
		t.Fatalf("remove deleted %s after an incomplete revocation:\n%s", hostBundle, stdout.String())
	}
	stubProbe(t, deniedProbe)
	// A wait well above the broker-derived minimum, watched as it runs: the started line must arrive while the command is
	// still in step 5, naming the time that wait ends, and step 5 must end about that long after it.
	const wait = 2 * time.Second
	var mu sync.Mutex
	var lines []string
	var startedAt, completedAt time.Time
	started, completed := make(chan struct{}), make(chan struct{})
	printed := func() string {
		mu.Lock()
		defer mu.Unlock()
		return strings.Join(lines, "\n")
	}
	out := &lineWriter{told: func(line string, at time.Time) {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, line)
		switch {
		case strings.HasPrefix(line, "step=5 started "):
			startedAt = at
			close(started)
		case strings.HasPrefix(line, "step=5 complete="):
			completedAt = at
			close(completed)
		}
	}}
	done := make(chan error, 1)
	go func() {
		done <- runOperator([]string{"remove", "--host", "host-a", "--tenant", "tenant-a", "--timeout", wait.String()}, out, io.Discard)
	}()
	select {
	case <-started:
	case err := <-done:
		t.Fatalf("remove = %v, printing\n%s\nwant step 5 announced as it starts", err, printed())
	}
	select {
	case <-completed:
		t.Fatalf("step 5 ended within %s of its announcement:\n%s\nwant the announcement while the wait runs", wait/2, printed())
	case <-time.After(wait / 2):
	}
	if err := <-done; err != nil {
		t.Fatalf("remove = %v\n%s", err, printed())
	}
	index := slices.IndexFunc(lines, func(line string) bool { return strings.HasPrefix(line, "step=5 started name=wait until=") })
	if index < 1 || index+1 >= len(lines) || !strings.HasPrefix(lines[index-1], "step=4 complete=true") || !strings.HasPrefix(lines[index+1], "step=5 complete=true") {
		t.Fatalf("remove printed\n%s\nwant step 5 announced as it starts, between steps 4 and 5 ending", printed())
	}
	until, err := time.Parse(time.RFC3339, strings.TrimPrefix(lines[index], "step=5 started name=wait until="))
	if err != nil {
		t.Fatalf("progress line %q; want until as the RFC 3339 time the wait ends", lines[index])
	}
	// until is written to the second, so it is up to a second before the announcement plus the wait, never after.
	if ahead := until.Sub(startedAt.Add(wait)); ahead <= -2*time.Second || ahead > 0 {
		t.Fatalf("step 5 announced at %s until %s; want until the wait %s after the announcement", startedAt.Format(time.RFC3339Nano), until.Format(time.RFC3339), wait)
	}
	if actual := completedAt.Sub(startedAt); actual < wait-100*time.Millisecond {
		t.Fatalf("step 5 ended %s after its announcement; want the announced wait %s", actual, wait)
	}
	if exists(hostBundle) {
		t.Fatalf("remove kept %s after a complete revocation:\n%s", hostBundle, printed())
	}
	// A rerun finds the host already ended; it deletes a bundle an earlier run left, and a missing one is no error.
	if err := runOperator([]string{"remove", "--host", "host-a", "--tenant", "tenant-a"}, io.Discard, io.Discard); err != nil {
		t.Fatalf("rerun without a bundle = %v", err)
	}
	if err := os.WriteFile(hostBundle, []byte("left over"), 0o600); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	if err := runOperator([]string{"remove", "--host", "host-a", "--tenant", "tenant-a"}, &stdout, io.Discard); err != nil || !strings.HasPrefix(stdout.String(), "step=6 complete=true name=already ended") {
		t.Fatalf("rerun = %v with output %q; want the already-ended step", err, stdout.String())
	}
	if exists(hostBundle) {
		t.Fatalf("rerun kept %s after finding the host ended", hostBundle)
	}
	if err := runOperator([]string{"remove", "--receiver", "receiver-a"}, io.Discard, io.Discard); err != nil {
		t.Fatalf("receiver remove = %v", err)
	}
	if exists(receiverBundle) {
		t.Fatalf("receiver remove kept %s after a complete revocation", receiverBundle)
	}
}

// A receiver remove retried after the receiver's revocation completed but its bundle was left behind finds the receiver
// already ended: it runs no new revocation, so it completes although the broker's timeouts would now refuse one, and
// deletes the leftover bundle; a further rerun without a bundle is no error.
func TestReceiverRemoveRetryDeletesTheBundleOfAnEndedReceiver(t *testing.T) {
	service := operatorEnv(t)
	ctx := context.Background()
	terms := operator.Terms{Agents: []operator.AgentTerms{{Name: "claude"}}, DefaultAgent: "claude", DeliveryLimit: 5, LockDurationMS: 15000, ProducerByteRate: 1 << 20, RequestPercentage: 50}
	if err := service.PoolApply(ctx, "pool-a", terms, false); err != nil {
		t.Fatal(err)
	}
	bundle, err := bundlePath(".", "receivers", true, "receiver-a")
	if err != nil {
		t.Fatal(err)
	}
	config := operator.ReceiverConfig{Receiver: "receiver-a", Pool: "pool-a", Profiles: []operator.ProfileSettings{{Name: "claude", Plugin: "nomad", Agent: "claude"}}, PluginSHA256: map[string]string{"nomad": fakeSHA256}}
	if err := service.ReceiverAdd(ctx, config, service.Broker, bundle); err != nil {
		t.Fatal(err)
	}
	revocationBounds(t, service, "1")
	// The revocation completes without the command, which is what leaves its bundle: a cleanup interrupted after step 6.
	if result := service.RevokeReceiver(ctx, "receiver-a", 0, deniedProbe(service, ""), nil); !result.Complete {
		t.Fatalf("revocation incomplete: %+v", result)
	}
	// Unbounded timeouts refuse any new revocation at step 1, so only a retry that recognises the ended receiver completes.
	revocationBounds(t, service, "0")
	stubProbe(t, deniedProbe)
	var stdout bytes.Buffer
	if err := runOperator([]string{"remove", "--receiver", "receiver-a"}, &stdout, io.Discard); err != nil || !strings.HasPrefix(stdout.String(), "step=6 complete=true name=already ended") {
		t.Fatalf("retry = %v with output %q; want the already-ended step", err, stdout.String())
	}
	if _, err := os.Stat(bundle); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retry left %s (%v); want the ended receiver's bundle deleted", bundle, err)
	}
	if err := runOperator([]string{"remove", "--receiver", "receiver-a"}, io.Discard, io.Discard); err != nil {
		t.Fatalf("rerun without a bundle = %v", err)
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
		return runOperator(append([]string{"apply", "--pool", "pool-a", "--file", path}, options...), io.Discard, io.Discard)
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
	if err := runOperator([]string{"apply", "--pool", "pool-a", "--file", pool(3, "30s"), "--dry-run"}, &stdout, io.Discard); err != nil {
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
	return connectTLS(t, bundle.Broker, testCA.pem, bundle.Principal, bundle.Password).Ping(ctx) == nil
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
		return runOperator(append([]string{"apply", "--receiver", "receiver-a"}, options...), io.Discard, io.Discard)
	}
	if err := apply("--file", file); err != nil {
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
	if err := apply("--file", file); err != nil {
		t.Fatal(err)
	}
	if again, err := os.ReadFile(bundle); err != nil || !bytes.Equal(again, old) || records() != before+1 || !bundleWorks(t, service, bundle) {
		t.Fatalf("apply of an existing receiver left %d records and changed its bundle (%v) or password; want an update record alone", records(), err)
	}
	if err := apply("--force", "--file", file); err == nil || !strings.HasPrefix(err.Error(), "usage: wherry operator apply --receiver RECEIVER --force") {
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
		return runOperator(append([]string{"add", "--host", "host-a", "--tenant", tenant}, options...), io.Discard, io.Discard)
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
		if _, err := command("add", "--tenant", tenant); err != nil {
			t.Fatal(err)
		}
	}
	if err := service.PoolApply(t.Context(), "p", agentTerms(), false); err != nil {
		t.Fatal(err)
	}
	if _, err := command("allow", "--tenant", "a", "--pool", "p"); err != nil {
		t.Fatal(err)
	}
	if out, err := command("allow", "--tenant", "a", "--pool", "p"); err != nil || out != "already entitled\n" {
		t.Fatalf("already path: %q %v", out, err)
	}
	if _, err := command("add", "--host", "live", "--tenant", "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := command("add", "--host", "revoking", "--tenant", "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := command("add", "--host", "ended", "--tenant", "a"); err != nil {
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
	if out, err := command("list", "tenants"); err != nil || out != "a\n  pool p\n  host live\nz\n" {
		t.Fatalf("list=%q %v", out, err)
	}
	if _, err := command("deny", "--tenant", "a", "--pool", "p"); err != nil {
		t.Fatal(err)
	}
	if out, err := command("deny", "--tenant", "a", "--pool", "p"); err != nil || out != "not entitled\n" {
		t.Fatalf("absent path: %q %v", out, err)
	}
	if _, err := command("remove", "--tenant", "z"); err != nil {
		t.Fatal(err)
	}
	bundleDir := filepath.Join("bundles", "hosts", "z")
	if err := os.MkdirAll(bundleDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundleDir, "leftover.bundle"), []byte("leftover"), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := command("remove", "--tenant", "z"); err != nil || out != "done\n" {
		t.Fatalf("ended retry: %q %v", out, err)
	}
	if _, err := os.Stat(bundleDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("bundle directory remains: %v", err)
	}
	if out, err := command("list", "tenants"); err != nil || out != "a\n  host live\n" {
		t.Fatalf("list=%q %v", out, err)
	}
}
