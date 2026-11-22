package telemetry_test

import (
	"context"
	"io"
	"log"
	"testing"

	"github.com/benemon/wherry/internal/telemetry"
	"github.com/benemon/wherry/internal/telemetry/teletest"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// Brief rule 5 on resources: a value set in OTEL_RESOURCE_ATTRIBUTES, on an allowed key or a forbidden one, reaches
// no role's resource; OTEL_SERVICE_NAME names nothing; a resource carries identity and SDK attributes alone.
func TestResourceIgnoresTheEnvironment(t *testing.T) {
	exporter := teletest.Install(t)
	const sentinel = "s3cr3t-resource"
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "wherry.credential="+sentinel+"-allowed,deployment.secret="+sentinel+"-forbidden,host.name="+sentinel+"-host")
	t.Setenv("OTEL_SERVICE_NAME", sentinel+"-service")
	names := telemetry.Names{Tenant: "t", Host: "h", Pool: "p", Receiver: "r"}
	for _, role := range []string{telemetry.RoleHost, telemetry.RoleReceiver, telemetry.RoleCustodian, telemetry.RoleOperator} {
		tracing, err := telemetry.New(context.Background(), role, names, nil, log.New(io.Discard, "", 0))
		if err != nil {
			t.Fatal(err)
		}
		_, span := tracing.Root(context.Background(), role, trace.SpanKindInternal)
		span.End()
		if err := tracing.Close(); err != nil {
			t.Fatal(err)
		}
	}
	spans := exporter.Spans()
	if len(spans) != 4 {
		t.Fatalf("spans %d, want one per role", len(spans))
	}
	for _, s := range spans {
		keys := map[string]bool{}
		for _, kv := range s.Resource.Attributes() {
			keys[string(kv.Key)] = true
		}
		if !keys[string(semconv.ServiceNameKey)] || !keys[string(semconv.ServiceNamespaceKey)] || !keys[string(semconv.TelemetrySDKNameKey)] || !keys[string(semconv.TelemetrySDKVersionKey)] || !keys[string(semconv.TelemetrySDKLanguageKey)] {
			t.Errorf("%s resource %v, want the service identity and the SDK attributes", s.Name, s.Resource.Attributes())
		}
		if teletest.Resource(s, semconv.ServiceNamespaceKey) != s.Name {
			t.Errorf("%s resource namespace %q", s.Name, teletest.Resource(s, semconv.ServiceNamespaceKey))
		}
		if teletest.Resource(s, telemetry.AttrCredential) != "" {
			t.Errorf("%s resource carries the environment's value on an allowed key", s.Name)
		}
	}
	teletest.Check(t, spans)
	teletest.Sentinels(t, spans, sentinel)
}
