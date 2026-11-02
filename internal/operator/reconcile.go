package operator

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

func (service *Service) reconcileACLs(ctx context.Context, state State) error {
	client := kadm.NewClient(service.Operator)
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
	// A suspended pool's task-topic WRITE grants go by filters naming each principal, so no other grant is touched.
	for pool := range state.Suspended {
		resumed := state
		resumed.Suspended = maps.Clone(state.Suspended)
		delete(resumed.Suspended, pool)
		for _, acl := range ACLs(resumed) {
			if acl.ResourceType != "topic" || acl.Resource != PoolTasksTopic(pool) {
				continue
			}
			acl.Operations = slices.DeleteFunc(slices.Clone(acl.Operations), func(operation string) bool { return operation != "WRITE" })
			if len(acl.Operations) == 0 {
				continue
			}
			builder, err := aclBuilder(acl)
			if err != nil {
				return err
			}
			if err := service.deleteACLs(ctx, builder); err != nil {
				return fmt.Errorf("withdraw suspended pool %s's task topic ACLs: %w", pool, err)
			}
		}
	}
	return nil
}

func (service *Service) verifyACLs(ctx context.Context, state State) error {
	client := kadm.NewClient(service.Operator)
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
	return service.deleteACLs(ctx, builder)
}

// deleteACLs deletes the ACLs builder matches and waits until no broker describes one.
func (service *Service) deleteACLs(ctx context.Context, builder *kadm.ACLBuilder) error {
	results, err := kadm.NewClient(service.Operator).DeleteACLs(ctx, builder)
	if err != nil {
		return err
	}
	for _, result := range results {
		if result.Err != nil {
			return result.Err
		}
	}
	// Brokers apply an ACL deletion in turn, so a describe can briefly still list it; retry within the probe window.
	deadline := time.Now().Add(probeWindow)
	for {
		remaining, err := kadm.NewClient(service.Operator).DescribeACLs(ctx, builder)
		if err != nil {
			return err
		}
		left := false
		for _, result := range remaining {
			if result.Err != nil {
				return result.Err
			}
			left = left || len(result.Described) != 0
		}
		if !left {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("ACLs remain after deletion")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(probeInterval):
		}
	}
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
	case "cluster":
		builder.Clusters()
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
		case "CREATE":
			operations[i] = kadm.OpCreate
		case "ALTER":
			operations[i] = kadm.OpAlter
		case "ALTER_CONFIGS":
			operations[i] = kadm.OpAlterConfigs
		default:
			return nil, errors.New("unknown ACL operation")
		}
	}
	builder.Operations(operations...)
	builder.PrefixUser()
	return builder, nil
}
