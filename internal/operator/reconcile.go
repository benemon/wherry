package operator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// reconcileACLs creates the grants state calls for and deletes none: only converge, from the latest records, deletes.
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
	if err := service.verifyACLs(ctx, ACLs(state)); err != nil {
		return fmt.Errorf("ACL plan incomplete after re-read: %w", err)
	}
	return nil
}

// converge makes Kafka match wherry.operator as read to its end, not as the command loaded it: a concurrent command's
// record may have ended a principal or suspended a pool since.
func (service *Service) converge(ctx context.Context) error {
	state, err := service.LoadState(ctx)
	if err != nil {
		return err
	}
	if err := service.reconcileACLs(ctx, state); err != nil {
		return err
	}
	return service.withdraw(ctx, state)
}

// withdraw deletes each grant of wherry's tenant and receiver principals on wherry's topics, groups and transactional
// IDs that state does not call for, and the SCRAM credentials of each principal whose membership state has ended. It
// touches no other grant or credential: wherry.operator's are operator init's and the rest are not wherry's.
func (service *Service) withdraw(ctx context.Context, state State) error {
	type grant struct {
		principal, resourceType, resource string
		prefix                            bool
		operation                         string
	}
	wanted := map[grant]bool{}
	for _, acl := range ACLs(state) {
		for _, operation := range acl.Operations {
			wanted[grant{acl.Principal, acl.ResourceType, acl.Resource, acl.Prefix, operation}] = true
		}
	}
	client := kadm.NewClient(service.Operator)
	results, err := client.DescribeACLs(ctx, kadm.NewACLs().AnyResource().Allow().AllowHosts().Operations(kadm.OpAny).ResourcePatternType(kadm.ACLPatternAny))
	if err != nil {
		return err
	}
	for _, result := range results {
		if result.Err != nil {
			return result.Err
		}
		for _, described := range result.Described {
			principal, _ := strings.CutPrefix(described.Principal, "User:")
			resourceType := managed(described)
			if resourceType == "" || described.Host == "*" && wanted[grant{principal, resourceType, described.Name, described.Pattern == kadm.ACLPatternPrefixed, described.Operation.String()}] {
				continue
			}
			builder := kadm.NewACLs().Allow(described.Principal).AllowHosts(described.Host).Operations(described.Operation).ResourcePatternType(described.Pattern)
			switch resourceType {
			case "topic":
				builder.Topics(described.Name)
			case "group":
				builder.Groups(described.Name)
			default:
				builder.TransactionalIDs(described.Name)
			}
			if err := service.deleteACLs(ctx, builder); err != nil {
				return fmt.Errorf("withdraw %s %s on %s: %w", principal, described.Operation, described.Name, err)
			}
		}
	}
	users, err := client.DescribeUserSCRAMs(ctx)
	if err != nil {
		return err
	}
	for user, described := range users {
		if !state.Ended[user] || described.Err != nil {
			continue
		}
		var deletions []kadm.DeleteSCRAM
		for _, info := range described.CredInfos {
			deletions = append(deletions, kadm.DeleteSCRAM{User: user, Mechanism: info.Mechanism})
		}
		altered, err := client.AlterUserSCRAMs(ctx, deletions, nil)
		if err != nil {
			return err
		}
		if err := altered.Error(); err != nil {
			return fmt.Errorf("delete %s's SCRAM credential: %w", user, err)
		}
	}
	return nil
}

// managed is described's resource type as ACL names it when described is a grant of a wherry.tnt or wherry.rcv
// principal on a wherry topic, pool group or pool transactional ID, and "" otherwise.
func managed(described kadm.DescribedACL) string {
	principal, user := strings.CutPrefix(described.Principal, "User:")
	if !user || !strings.HasPrefix(principal, "wherry.tnt.") && !strings.HasPrefix(principal, ReceiverPrincipal("")) {
		return ""
	}
	switch {
	case described.Type == kmsg.ACLResourceTypeTopic && strings.HasPrefix(described.Name, "wherry."):
		return "topic"
	case described.Type == kmsg.ACLResourceTypeGroup && strings.HasPrefix(described.Name, PoolGroup("")):
		return "group"
	case described.Type == kmsg.ACLResourceTypeTransactionalId && strings.HasPrefix(described.Name, "p."):
		return "transactional"
	}
	return ""
}

func (service *Service) verifyACLs(ctx context.Context, acls []ACL) error {
	client := kadm.NewClient(service.Operator)
	for _, acl := range acls {
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
