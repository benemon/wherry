package operator

import (
	"context"
	"testing"
)

func tenantFixture(t *testing.T, service *Service, ctx context.Context, tenant string) error {
	t.Helper()
	state, err := service.LoadState(ctx)
	if err != nil {
		return err
	}
	if _, exists := state.Tenants[tenant]; !exists {
		if err := service.TenantAdd(ctx, tenant); err != nil {
			return err
		}
	}
	return nil
}

func entitleFixture(t *testing.T, service *Service, ctx context.Context, tenant, pool string) error {
	t.Helper()
	if err := tenantFixture(t, service, ctx, tenant); err != nil {
		return err
	}
	return service.TenantPoolAdd(ctx, tenant, pool)
}

func hostFixture(t *testing.T, service *Service, ctx context.Context, tenant, host, path string, force bool) error {
	t.Helper()
	if err := tenantFixture(t, service, ctx, tenant); err != nil {
		return err
	}
	return service.HostAdd(ctx, tenant, host, path, force)
}
