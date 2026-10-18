package admin

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
)

func (service *Service) reconcileACLs(ctx context.Context, state State) error {
	client := kadm.NewClient(service.Admin)
	for _, acl := range ACLs(state) {
		builder, err := aclBuilder(acl)
		if err != nil {
			return err
		}
		results, err := client.CreateACLs(ctx, builder)
		if err != nil {
			return fmt.Errorf("ACL plan incomplete: %w", err)
		}
		for _, result := range results {
			if result.Err != nil {
				return fmt.Errorf("ACL plan incomplete: %w", result.Err)
			}
		}
	}
	if err := service.verifyACLs(ctx, state); err != nil {
		return fmt.Errorf("ACL plan incomplete after re-read: %w", err)
	}
	return nil
}

func (service *Service) verifyACLs(ctx context.Context, state State) error {
	client := kadm.NewClient(service.Admin)
	for _, acl := range ACLs(state) {
		builder, err := aclBuilder(acl)
		if err != nil {
			return err
		}
		results, err := client.DescribeACLs(ctx, builder)
		if err != nil {
			return err
		}
		for _, result := range results {
			if result.Err != nil {
				return result.Err
			}
			if len(result.Described) == 0 {
				return fmt.Errorf("missing %s ACL on %s for %s", result.Operation, acl.Resource, acl.Principal)
			}
		}
	}
	return nil
}

func (service *Service) deletePrincipalACLs(ctx context.Context, principal string) error {
	builder := kadm.NewACLs().AnyResource().Allow(principal).AllowHosts("*").Operations(kadm.OpAny).ResourcePatternType(kadm.ACLPatternAny)
	builder.PrefixUser()
	results, err := kadm.NewClient(service.Admin).DeleteACLs(ctx, builder)
	if err != nil {
		return err
	}
	for _, result := range results {
		if result.Err != nil {
			return result.Err
		}
	}
	remaining, err := kadm.NewClient(service.Admin).DescribeACLs(ctx, builder)
	if err != nil {
		return err
	}
	for _, result := range remaining {
		if result.Err != nil {
			return result.Err
		}
		if len(result.Described) != 0 {
			return errors.New("principal ACLs remain after deletion")
		}
	}
	return nil
}

func aclBuilder(acl ACL) (*kadm.ACLBuilder, error) {
	builder := kadm.NewACLs().Allow(acl.Principal).AllowHosts("*")
	switch acl.ResourceType {
	case "topic":
		builder.Topics(acl.Resource)
	case "group":
		builder.Groups(acl.Resource)
	case "transactional":
		builder.TransactionalIDs(acl.Resource)
	default:
		return nil, errors.New("unknown ACL resource type")
	}
	if acl.Prefix {
		builder.ResourcePatternType(kadm.ACLPatternPrefixed)
	} else {
		builder.ResourcePatternType(kadm.ACLPatternLiteral)
	}
	operations := make([]kadm.ACLOperation, len(acl.Operations))
	for i, operation := range acl.Operations {
		switch operation {
		case "READ":
			operations[i] = kadm.OpRead
		case "WRITE":
			operations[i] = kadm.OpWrite
		case "DESCRIBE":
			operations[i] = kadm.OpDescribe
		case "DESCRIBE_CONFIGS":
			operations[i] = kadm.OpDescribeConfigs
		default:
			return nil, errors.New("unknown ACL operation")
		}
	}
	builder.Operations(operations...)
	builder.PrefixUser()
	return builder, nil
}
