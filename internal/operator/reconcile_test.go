package operator

import (
	"context"
	"strings"
	"testing"
)

// operator check fails without any grant operator init gives wherry.operator.
func TestCheckRequiresTheOperatorsGrants(t *testing.T) {
	for _, acl := range OperatorACLs {
		for _, operation := range acl.Operations {
			t.Run(acl.ResourceType+"/"+operation, func(t *testing.T) {
				_, service, closeFixture := kfakeService(t)
				defer closeFixture()
				ctx := context.Background()
				if err := service.Check(ctx); err != nil {
					t.Fatal(err)
				}
				dropped := acl
				dropped.Operations = []string{operation}
				builder, err := aclBuilder(dropped)
				if err != nil {
					t.Fatal(err)
				}
				if err := service.deleteACLs(ctx, builder); err != nil {
					t.Fatal(err)
				}
				err = service.Check(ctx)
				if want := " ACL on " + acl.Resource + " for " + OperatorPrincipal; err == nil || !strings.HasPrefix(err.Error(), "missing ") || !strings.HasSuffix(err.Error(), want) {
					t.Fatalf("check without %s on the %s = %v; want it missing", operation, acl.ResourceType, err)
				}
			})
		}
	}
}
