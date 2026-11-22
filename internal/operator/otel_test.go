package operator

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/benemon/wherry/internal/telemetry"
	"github.com/benemon/wherry/internal/telemetry/teletest"
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
	tracing, err := telemetry.New(ctx, telemetry.RoleOperator, telemetry.Names{}, service.Operator, log.New(io.Discard, "", 0))
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
		func() error { return service.HostAdd(ctx, "t", "h", bundlePath, false) },
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
		"tenant host add": func() error { return service.HostAdd(ctx, "t", "h", bundlePath, false) },
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
