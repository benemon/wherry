package operator

import (
	"slices"
	"strings"
)

type ACL struct {
	Principal, ResourceType, Resource string
	Prefix                            bool
	Operations                        []string
}

func ACLs(state State) []ACL {
	readDescribe := []string{"READ", "DESCRIBE"}
	writeDescribe := []string{"WRITE", "DESCRIBE"}
	// A suspended pool's task topic takes no task; its receivers keep reading it, so running jobs renew and acknowledge.
	tasks := func(pool string) []string {
		if state.Suspended[pool] {
			return []string{"DESCRIBE"}
		}
		return writeDescribe
	}
	// A revoking or ended principal is granted nothing, so no reconcile during its revocation restores a grant.
	acls := []ACL{{OperatorPrincipal, "topic", OperatorTopic, false, []string{"READ", "WRITE", "DESCRIBE"}}}
	for principal, host := range state.Hosts {
		if !state.Ended[principal] && !state.Revoking[principal] {
			acls = append(acls, ACL{principal, "topic", OperatorTopic, false, readDescribe}, ACL{principal, "topic", ReceiversTopic, false, readDescribe}, ACL{principal, "topic", TenantTopic(host.Tenant), false, readDescribe}, ACL{principal, "topic", TenantTranscriptTopic(host.Tenant), false, readDescribe}, ACL{principal, "topic", HostTopic(host.Tenant, host.Name), false, []string{"READ", "WRITE", "DESCRIBE"}})
		}
	}
	for receiver, config := range state.Receivers {
		principal := ReceiverPrincipal(receiver)
		if state.Ended[principal] || state.Revoking[principal] {
			continue
		}
		acls = append(acls, ACL{principal, "topic", OperatorTopic, false, readDescribe}, ACL{principal, "topic", ReceiverTopic(receiver), false, []string{"READ", "WRITE", "DESCRIBE"}}, ACL{principal, "topic", ReceiverProgressTopic(receiver), false, []string{"READ", "WRITE", "DESCRIBE"}}, ACL{principal, "topic", ReceiversTopic, false, []string{"READ", "WRITE", "DESCRIBE"}}, ACL{principal, "topic", PoolTasksTopic(config.Pool), false, readDescribe}, ACL{principal, "topic", PoolEventsTopic(config.Pool), false, []string{"READ", "WRITE", "DESCRIBE"}}, ACL{principal, "group", PoolGroup(config.Pool), false, []string{"READ", "DESCRIBE", "DESCRIBE_CONFIGS"}}, ACL{principal, "topic", CustodianTopic(config.Pool), false, []string{"READ", "WRITE", "DESCRIBE"}}, ACL{principal, "group", CustodianGroup(config.Pool), false, readDescribe}, ACL{principal, "transactional", TransactionalPrefix(config.Pool), true, writeDescribe})
		for peer, peerConfig := range state.Receivers {
			if peer == receiver || peerConfig.Pool != config.Pool {
				continue
			}
			// An ended peer's key records verify the open records it appended while live.
			acls = append(acls, ACL{principal, "topic", ReceiverTopic(peer), false, readDescribe})
			if !state.Ended[ReceiverPrincipal(peer)] {
				acls = append(acls, ACL{principal, "topic", ReceiverProgressTopic(peer), false, readDescribe})
			}
		}
	}
	for tenant, pools := range state.Entitlements {
		for pool := range pools {
			for principal, host := range state.Hosts {
				if host.Tenant != tenant || state.Ended[principal] || state.Revoking[principal] {
					continue
				}
				acls = append(acls, ACL{principal, "topic", PoolTasksTopic(pool), false, tasks(pool)}, ACL{principal, "topic", PoolEventsTopic(pool), false, writeDescribe}, ACL{principal, "topic", CustodianTopic(pool), false, readDescribe})
				for receiver, config := range state.Receivers {
					if config.Pool == pool && !state.Ended[ReceiverPrincipal(receiver)] {
						acls = append(acls, ACL{principal, "topic", ReceiverTopic(receiver), false, readDescribe})
					}
				}
			}
			for receiver, config := range state.Receivers {
				principal := ReceiverPrincipal(receiver)
				if config.Pool != pool || state.Ended[principal] || state.Revoking[principal] {
					continue
				}
				acls = append(acls, ACL{principal, "topic", TenantTopic(tenant), false, writeDescribe}, ACL{principal, "topic", TenantTranscriptTopic(tenant), false, writeDescribe})
				for hostPrincipal, host := range state.Hosts {
					if host.Tenant == tenant && !state.Ended[hostPrincipal] {
						acls = append(acls, ACL{principal, "topic", HostTopic(host.Tenant, host.Name), false, readDescribe})
					}
				}
			}
		}
	}
	slices.SortFunc(acls, func(a, b ACL) int {
		if a.Principal != b.Principal {
			return strings.Compare(a.Principal, b.Principal)
		}
		if a.Resource != b.Resource {
			return strings.Compare(a.Resource, b.Resource)
		}
		return strings.Compare(a.ResourceType, b.ResourceType)
	})
	return acls
}
