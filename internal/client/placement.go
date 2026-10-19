package client

import (
	"maps"
	"slices"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/wire"
)

// resolvePlacement chooses the pool through the live advertisement, among the tenant's entitled pools, that can place
// selector and has the most spare capacity, then the lowest receiver ID; it places the task against that pool's terms
// and that receiver's offers.
func resolvePlacement(state admin.State, tenant string, selector []wire.Requirement, ads map[string]wire.Advertisement) (string, wire.Placement, bool) {
	var chosen *wire.Advertisement
	var placement wire.Placement
	for _, receiver := range slices.Sorted(maps.Keys(ads)) {
		ad := ads[receiver]
		if !state.Entitlements[tenant][ad.Pool] {
			continue
		}
		candidate, ok := place(state.Pools[ad.Pool], state.Receivers[receiver], selector, ad.Offers)
		if ok && (chosen == nil || spare(ad) > spare(*chosen)) {
			chosen, placement = &ad, candidate
		}
	}
	if chosen == nil {
		return "", wire.Placement{}, false
	}
	return chosen.Pool, placement, true
}

// place returns the first offer, trying agents and runtimes that the terms allow with their defaults first and then
// in declared order, whose labels and adapter platform satisfy selector.
func place(terms admin.Terms, config admin.ReceiverConfig, selector []wire.Requirement, offers []wire.Offer) (wire.Placement, bool) {
	agents := make([]string, 0, len(terms.Agents))
	for _, agent := range terms.Agents {
		agents = append(agents, agent.Name)
	}
	for _, agent := range preferred(terms.DefaultAgent, agents) {
		for _, runtime := range preferred(terms.DefaultRuntime, terms.Runtimes) {
			for _, offer := range offers {
				index := slices.IndexFunc(config.Adapters, func(adapter admin.AdapterSettings) bool { return adapter.Name == offer.Adapter })
				if index < 0 || offer.Labels["agent"] != agent || offer.Labels["runtime"] != runtime {
					continue
				}
				labels := maps.Clone(offer.Labels)
				labels["platform"] = config.Adapters[index].Type
				if satisfies(selector, labels) {
					return wire.Placement{Agent: agent, Runtime: runtime, Labels: maps.Clone(offer.Labels)}, true
				}
			}
		}
	}
	return wire.Placement{}, false
}

func preferred(first string, declared []string) []string {
	return append([]string{first}, slices.DeleteFunc(slices.Clone(declared), func(value string) bool { return value == first })...)
}

func satisfies(selector []wire.Requirement, labels map[string]string) bool {
	for _, requirement := range selector {
		if value, ok := labels[requirement.Key]; !ok || !slices.Contains(requirement.Values, value) {
			return false
		}
	}
	return true
}

func spare(ad wire.Advertisement) int64 {
	return ad.Capacity - ad.InFlight
}
