package admin

import "slices"

type ACL struct {
	Principal, ResourceType, Resource string
	Prefix                            bool
	Operations                        []string
}

func ACLs(state State) []ACL {
	readDescribe := []string{"READ", "DESCRIBE"}
	writeDescribe := []string{"WRITE", "DESCRIBE"}
	acls := []ACL{{"wherry.operator", "topic", AdminTopic, false, []string{"READ", "WRITE", "DESCRIBE"}}}
	for principal, host := range state.Hosts {
		if !state.Ended[principal] {
			acls = append(acls, ACL{principal, "topic", AdminTopic, false, readDescribe}, ACL{principal, "topic", ReceiversTopic, false, readDescribe}, ACL{principal, "topic", TenantTopic(host.Tenant), false, readDescribe}, ACL{principal, "topic", HostTopic(host.Tenant, host.Name), false, []string{"READ", "WRITE", "DESCRIBE"}})
		}
	}
	for receiver, config := range state.Receivers {
		principal := ReceiverPrincipal(receiver)
		if state.Ended[principal] {
			continue
		}
		acls = append(acls, ACL{principal, "topic", AdminTopic, false, readDescribe}, ACL{principal, "topic", ReceiverTopic(receiver), false, []string{"READ", "WRITE", "DESCRIBE"}}, ACL{principal, "topic", ReceiversTopic, false, []string{"READ", "WRITE", "DESCRIBE"}}, ACL{principal, "topic", PoolTasksTopic(config.Pool), false, readDescribe}, ACL{principal, "topic", PoolEventsTopic(config.Pool), false, []string{"READ", "WRITE", "DESCRIBE"}}, ACL{principal, "group", PoolGroup(config.Pool), false, []string{"READ", "DESCRIBE", "DESCRIBE_CONFIGS"}}, ACL{principal, "transactional", TransactionalPrefix(config.Pool), true, writeDescribe})
		for peer, peerConfig := range state.Receivers {
			if peer != receiver && peerConfig.Pool == config.Pool && !state.Ended[ReceiverPrincipal(peer)] {
				acls = append(acls, ACL{principal, "topic", ReceiverTopic(peer), false, readDescribe})
			}
		}
	}
	for tenant, pools := range state.Entitlements {
		for pool := range pools {
			for principal, host := range state.Hosts {
				if host.Tenant != tenant || state.Ended[principal] {
					continue
				}
				acls = append(acls, ACL{principal, "topic", PoolTasksTopic(pool), false, writeDescribe}, ACL{principal, "topic", PoolEventsTopic(pool), false, writeDescribe})
				for receiver, config := range state.Receivers {
					if config.Pool == pool && !state.Ended[ReceiverPrincipal(receiver)] {
						acls = append(acls, ACL{principal, "topic", ReceiverTopic(receiver), false, readDescribe})
					}
				}
			}
			for receiver, config := range state.Receivers {
				principal := ReceiverPrincipal(receiver)
				if config.Pool != pool || state.Ended[principal] {
					continue
				}
				acls = append(acls, ACL{principal, "topic", TenantTopic(tenant), false, writeDescribe})
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
			return cmp(a.Principal, b.Principal)
		}
		if a.Resource != b.Resource {
			return cmp(a.Resource, b.Resource)
		}
		return cmp(a.ResourceType, b.ResourceType)
	})
	return acls
}

func cmp(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
