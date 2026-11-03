package operator

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestTenantRemoveRetriesRevocationSteps(t *testing.T) {
	for _, step := range []int{1, 2, 3, 4, 5} {
		t.Run(string(rune('0'+step)), func(t *testing.T) {
			cluster, service, closeFixture := kfakeService(t)
			defer closeFixture()
			ctx := t.Context()
			if err := service.TenantAdd(ctx, "t"); err != nil {
				t.Fatal(err)
			}
			if err := service.HostAdd(ctx, "t", "h", filepath.Join(t.TempDir(), "bundle"), false); err != nil {
				t.Fatal(err)
			}
			revocationBounds(t, service, "1", "1", "1")
			probe := func(string) func(context.Context, string, string) error { return denied }
			switch step {
			case 1, 4:
				cluster.ControlKey(kmsg.AlterUserSCRAMCredentials.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
					req := request.(*kmsg.AlterUserSCRAMCredentialsRequest)
					if step == 4 && len(req.Deletions) == 0 {
						cluster.KeepControl()
						return nil, nil, false
					}
					response := req.ResponseKind().(*kmsg.AlterUserSCRAMCredentialsResponse)
					response.Results = []kmsg.AlterUserSCRAMCredentialsResponseResult{{User: TenantPrincipal("t", "h"), ErrorCode: kerr.InvalidRequest.Code}}
					return response, nil, true
				})
			case 2:
				cluster.ControlKey(kmsg.DeleteACLs.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
					response := request.(*kmsg.DeleteACLsRequest).ResponseKind().(*kmsg.DeleteACLsResponse)
					response.Results = []kmsg.DeleteACLsResponseResult{{ErrorCode: kerr.InvalidRequest.Code}}
					return response, nil, true
				})
			case 3:
				probe = func(string) func(context.Context, string, string) error {
					return func(context.Context, string, string) error { return errors.New("injected probe failure") }
				}
			case 5:
				// Prepare and delete credentials first, then interrupt the wait itself.
				r := &revocation{service: service, principal: TenantPrincipal("t", "h"), end: Record{Kind: "end", Tenant: "t", Host: "h"}, probe: denied}
				if result := r.steps(ctx, 1, 4); len(result.Steps) != 4 || !result.Steps[3].Complete {
					t.Fatal(result)
				}
				interrupted, cancel := context.WithCancel(ctx)
				cancel()
				r.wait = time.Hour
				if result := r.steps(interrupted, 5, 5); len(result.Steps) != 5 || result.Steps[4].Complete {
					t.Fatal(result)
				}
			}
			if step != 5 {
				if err := service.removeTenant(ctx, "t", probe); err == nil {
					t.Fatal("injected revocation failure succeeded")
				}
			}
			state, err := service.LoadState(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !state.Revoking[TenantPrincipal("t", "h")] || state.Ended[TenantPrincipal("t", "h")] {
				t.Fatalf("failed step did not leave resumable host: %+v", state)
			}
			if err := removeTenantFixture(service, ctx, "t"); err != nil {
				t.Fatal(err)
			}
			state, err = service.LoadState(ctx)
			if err != nil || !state.Tenants["t"] || !state.Ended[TenantPrincipal("t", "h")] {
				t.Fatalf("retry incomplete: %+v %v", state, err)
			}
		})
	}
}
