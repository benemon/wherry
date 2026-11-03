package operator

import (
	"context"
	"testing"
)

func tenantFixture(t *testing.T, service *Service, ctx context.Context, tenant string) {
	t.Helper()
	state, err := service.LoadState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := state.Tenants[tenant]; !exists {
		if err := service.TenantAdd(ctx, tenant); err != nil {
			t.Fatal(err)
		}
	}
}

func entitleFixture(t *testing.T, service *Service, ctx context.Context, tenant, pool string) error {
	t.Helper()
	tenantFixture(t, service, ctx, tenant)
	return service.TenantPoolAdd(ctx, tenant, pool)
}

func hostFixture(t *testing.T, service *Service, ctx context.Context, tenant, host, path string, force bool) error {
	t.Helper()
	tenantFixture(t, service, ctx, tenant)
	return service.HostAdd(ctx, tenant, host, path, force)
}
