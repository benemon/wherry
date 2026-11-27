package operator

import (
	"context"
	"errors"
	"io"
	"log"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/benemon/wherry/internal/telemetry"
	"github.com/benemon/wherry/internal/telemetry/teletest"
	"github.com/twmb/franz-go/pkg/kadm"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"gopkg.in/yaml.v3"
)

// Brief rule 5 at the operator: a command that returns an error, or a revocation whose result carries a failed step,
// marks its span Error with a fixed class and never the error's text; the bundle's password, the operator's own
// credential and the tenant's storage keys reach no span.
func TestTracedCommandFailuresCarryAClass(t *testing.T) {
	exporter := teletest.Install(t)
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := t.Context()
	tracing, err := telemetry.New(ctx, telemetry.RoleOperator, telemetry.Names{}, service.Operator.Opts(), log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	service.Tracing = tracing
	const access, secret, probeText = "AKIAS3CR3TACCESSKEY", "s3cr3t-secret-key-value", "s3cr3t-probe-detail"
	service.Storage = func(string) (StorageKeys, error) { return StorageKeys{AccessKey: access, SecretKey: secret}, nil }
	dir := t.TempDir()
	bundlePath := filepath.Join(dir, "host.yaml")
	for _, command := range []func() error{
		func() error { return service.PoolApply(ctx, "p", modelTerms(), false) },
		func() error { return service.TenantAdd(ctx, "t") },
		func() error { return service.TenantPoolAdd(ctx, "t", "p") },
		func() error { return service.HostAdd(ctx, "t", "h", bundlePath) },
	} {
		if err := command(); err != nil {
			t.Fatal(err)
		}
	}
	encoded, err := os.ReadFile(bundlePath)
	if err != nil {
		t.Fatal(err)
	}
	var bundle Bundle
	if err := yaml.Unmarshal(encoded, &bundle); err != nil {
		t.Fatal(err)
	}
	if bundle.Password == "" {
		t.Fatal("the host bundle carries no password")
	}
	// The failing paths: an invalid pool name, a host that exists, a refresh of a bundle that is not the host's, an
	// unknown host's revocation, and a revocation whose probe fails at step 3 with a reason quoting a sentinel.
	failures := map[string]func() error{
		"pool apply":      func() error { return service.PoolApply(ctx, "Not A Pool", modelTerms(), false) },
		"tenant host add": func() error { return service.HostAdd(ctx, "t", "h", bundlePath) },
		"tenant host refresh": func() error {
			return revocationErrorOf(service.HostRefresh(ctx, "t", "absent", filepath.Join(dir, "absent.yaml")))
		},
		"tenant pool remove": func() error { return service.TenantPoolRemove(ctx, "absent", "p") },
	}
	for name, command := range failures {
		if err := command(); err == nil {
			t.Fatalf("%s succeeded, want a failure", name)
		}
	}
	revocationBounds(t, service, "100", "400", "200")
	unknown := service.RevokeHost(ctx, "t", "absent", 0, denied, nil)
	if unknown.Complete || revocationError(unknown) == nil {
		t.Fatalf("revocation of an unknown host %+v, want a failed step", unknown)
	}
	probeFailed := service.RevokeHost(ctx, "t", "h", 0, func(context.Context, string, string) error { return errors.New(probeText) }, nil)
	if probeFailed.Complete || len(probeFailed.Steps) != 3 || probeFailed.Steps[2].Err == nil || !strings.Contains(probeFailed.Steps[2].Err.Error(), probeText) {
		t.Fatalf("revocation with a failing probe %+v, want step 3 failed with the probe's reason", probeFailed)
	}
	if err := tracing.Close(); err != nil {
		t.Fatal(err)
	}
	spans := exporter.Spans()
	failed := func(name string) []tracetest.SpanStub {
		var found []tracetest.SpanStub
		for _, s := range teletest.Named(spans, name) {
			if s.Status.Code == codes.Error {
				found = append(found, s)
			}
		}
		return found
	}
	for name, want := range map[string]int{"operator pool apply": 1, "operator tenant host add": 1, "operator tenant host refresh": 1, "operator tenant pool remove": 1, "operator tenant host remove": 2} {
		spans := failed(name)
		if len(spans) != want {
			t.Errorf("%s spans with an error status: %d, want %d", name, len(spans), want)
		}
		for _, s := range spans {
			if s.Status.Description != telemetry.ClassError {
				t.Errorf("%s status %+v, want the class %q", name, s.Status, telemetry.ClassError)
			}
		}
	}
	for name, count := range map[string]int{"operator tenant add": 1, "operator tenant pool add": 1} {
		if all, errored := teletest.Named(spans, name), failed(name); len(all) != count || len(errored) != 0 {
			t.Errorf("%s spans %d with %d failed, want %d successes", name, len(all), len(errored), count)
		}
	}
	steps := teletest.Named(spans, "revocation step ")
	if len(steps) != 3 {
		t.Fatalf("revocation step spans %d, want three of the probe-failed revocation", len(steps))
	}
	for _, step := range steps {
		switch {
		case step.Name == "revocation step 3" && (step.Status.Code != codes.Error || step.Status.Description != telemetry.ClassError):
			t.Errorf("step 3 status %+v, want the class %q", step.Status, telemetry.ClassError)
		case step.Name != "revocation step 3" && step.Status.Code != codes.Unset:
			t.Errorf("%s status %+v, want none", step.Name, step.Status)
		}
	}
	teletest.Check(t, spans)
	teletest.Sentinels(t, spans, bundle.Password, "operator-password", access, secret, probeText, "s3cr3t")
}

// revocationErrorOf discards HostRefresh's changed flag.
func revocationErrorOf(_ bool, err error) error { return err }

// An identity attribute is set on a command's span only once its argument passes the name validation the command
// applies, on the failure path as on any other: invalid arguments carrying a credential-bearing URL, a presigned
// signature, a prompt and an argv line reach no span and no trace record. The commands run with tracing on through
// the Kafka transport, so the records' bytes and the broker's topic names are searched too: a marker of no valid
// tenant goes to wherry.traces, never to a topic named after the argument.
func TestTracedCommandsValidateArgumentsBeforeAttributing(t *testing.T) {
	_, service, closeFixture := kfakeService(t)
	defer closeFixture()
	ctx := t.Context()
	logs := &strings.Builder{}
	tracing, err := telemetry.New(ctx, telemetry.RoleOperator, telemetry.Names{}, service.Operator.Opts(), log.New(logs, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	service.Tracing = tracing
	if err := service.TenantAdd(ctx, "t"); err != nil {
		t.Fatal(err)
	}
	sentinels := []string{
		"https://wherry:s3cr3t-password@broker.example:9093/wherry?token=s3cr3t-token",
		"X-Amz-Signature=0123s3cr3t-signature",
		"please summarise s3cr3t-prompt-text",
		"cat /tmp/s3cr3t-source-directory/deploy-key",
	}
	dir := t.TempDir()
	var names []string
	for _, bad := range sentinels {
		commands := map[string]func() error{
			"pool apply":         func() error { return service.PoolApply(ctx, bad, modelTerms(), false) },
			"tenant add":         func() error { return service.TenantAdd(ctx, bad) },
			"tenant pool add":    func() error { return service.TenantPoolAdd(ctx, "t", bad) },
			"tenant pool remove": func() error { return service.TenantPoolRemove(ctx, bad, "p") },
			"tenant host add":    func() error { return service.HostAdd(ctx, "t", bad, filepath.Join(dir, "host.yaml")) },
			"tenant host refresh": func() error {
				return revocationErrorOf(service.HostRefresh(ctx, bad, "h", filepath.Join(dir, "absent.yaml")))
			},
			"tenant remove":      func() error { return service.TenantRemove(ctx, bad) },
			"tenant host remove": func() error { return revocationError(service.RevokeHost(ctx, "t", bad, 0, denied, nil)) },
			"receiver provision": func() error {
				return service.ReceiverAdd(ctx, ReceiverConfig{Receiver: bad, Pool: "p"}, service.Broker, filepath.Join(dir, "receiver.yaml"))
			},
			"receiver reissue": func() error {
				return service.ReceiverReissue(ctx, bad, service.Broker, filepath.Join(dir, "reissue.yaml"))
			},
			"receiver reconfigure": func() error { _, err := service.ReceiverUpdate(ctx, ReceiverConfig{Receiver: bad}, true); return err },
			"receiver remove":      func() error { return revocationError(service.RevokeReceiver(ctx, bad, 0, denied, nil)) },
		}
		names = slices.Sorted(maps.Keys(commands))
		for _, name := range names {
			if err := commands[name](); err == nil {
				t.Fatalf("%s with %q succeeded, want a refusal", name, bad)
			}
		}
	}
	if err := tracing.Close(); err != nil {
		t.Fatal(err)
	}
	// A marker naming the valid tenant is on its topic; one whose tenant argument was refused is on wherry.traces.
	records := append(teletest.Read(t, ctx, service.Operator.Opts(), telemetry.TraceTopic("")), teletest.Read(t, ctx, service.Operator.Opts(), telemetry.TraceTopic("t"))...)
	spans := teletest.Spans(t, records)
	for _, name := range names {
		if markers := teletest.Named(spans, "operator "+name); len(markers) < len(sentinels) {
			t.Errorf("operator %s spans: %d, want one per refused argument", name, len(markers))
		}
	}
	control := false
	for _, s := range teletest.Named(spans, "operator tenant add") {
		control = control || teletest.Attr(s, telemetry.AttrTenant) == "t"
	}
	if !control {
		t.Error("the valid tenant add carries no wherry.tenant")
	}
	teletest.Check(t, spans)
	teletest.Sentinels(t, spans, append(sentinels, "s3cr3t")...)
	teletest.Bytes(t, records, append(sentinels, "s3cr3t")...)
	topics, err := kadm.NewClient(service.Operator).ListTopics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for topic := range topics {
		if strings.Contains(topic, "s3cr3t") {
			t.Errorf("the broker knows topic %q", topic)
		}
	}
	if strings.Contains(logs.String(), "s3cr3t") {
		t.Fatalf("logged:\n%s", logs)
	}
}
