package operator

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// OperatorACLs are what wherry.operator needs to provision. Alter on the cluster lets it grant itself any other ACL, so
// it is close to the Kafka super user in effect.
var OperatorACLs = []ACL{
	{OperatorPrincipal, "cluster", "kafka-cluster", false, []string{"ALTER", "ALTER_CONFIGS"}},
	{OperatorPrincipal, "topic", "wherry.", true, []string{"CREATE", "ALTER_CONFIGS", "READ", "WRITE"}},
	{OperatorPrincipal, "group", PoolGroup(""), true, []string{"ALTER_CONFIGS"}},
}

// Delegate, connected as the Kafka super user, grants wherry.operator OperatorACLs, creates the installation-wide
// topics and then sets wherry.operator's password, creating the principal or replacing its password.
func Delegate(ctx context.Context, superUser *kgo.Client, password string) error {
	client := kadm.NewClient(superUser)
	for _, acl := range OperatorACLs {
		builder, err := aclBuilder(acl)
		if err != nil {
			return err
		}
		results, err := client.CreateACLs(ctx, builder)
		if err != nil {
			return fmt.Errorf("grant %s: %w", OperatorPrincipal, err)
		}
		for _, result := range results {
			if result.Err != nil {
				return fmt.Errorf("grant %s %s on %s: %w", OperatorPrincipal, result.Operation, acl.Resource, result.Err)
			}
		}
	}
	service := &Service{Operator: superUser}
	if err := service.createTopics(ctx, "-1", OperatorTopic); err != nil {
		return err
	}
	return setSCRAM(ctx, superUser, OperatorPrincipal, password)
}
