package client

import (
	"maps"
	"slices"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/wire"
)

// resolvePlacement chooses, among live advertisements in the tenant's entitled pools, the member with the most spare
// capacity, then the lowest receiver ID, that has a ready adapter launching the agent with labels satisfying selector.
// An empty agent takes the pool's default agent, then its declared order. Labels come from the operator's config
// record, never the advertisement (ADR-0027 Placement).
func resolvePlacement(state admin.State, tenant, agent string, selector []wire.Requirement, ads map[string]wire.Advertisement) (string, wire.Placement, bool) {
	var chosen *wire.Advertisement
	var placement wire.Placement
	for _, receiver := range slices.Sorted(maps.Keys(ads)) {
		ad := ads[receiver]
		if !state.Entitlements[tenant][ad.Pool] {
			continue
		}
		placed, ok := place(state.Pools[ad.Pool], state.Receivers[receiver], agent, selector, ad.Offers)
		if ok && (chosen == nil || spare(ad) > spare(*chosen)) {
			chosen, placement = &ad, placed
		}
	}
	if chosen == nil {
		return "", wire.Placement{}, false
	}
	return chosen.Pool, placement, true
}

// place returns the first agent, by preference, that one of the member's ready adapters launches with labels
// satisfying selector.
func place(terms admin.Terms, config admin.ReceiverConfig, agent string, selector []wire.Requirement, offers []wire.Offer) (wire.Placement, bool) {
	agents := make([]string, 0, len(terms.Agents))
	for _, allowed := range terms.Agents {
		agents = append(agents, allowed.Name)
	}
	candidates := []string{agent}
	if agent == "" {
		candidates = append([]string{terms.DefaultAgent}, slices.DeleteFunc(agents, func(name string) bool { return name == terms.DefaultAgent })...)
	} else if !slices.Contains(agents, agent) {
		return wire.Placement{}, false
	}
	for _, candidate := range candidates {
		for _, offer := range offers {
			index := slices.IndexFunc(config.Adapters, func(adapter admin.AdapterSettings) bool { return adapter.Name == offer.Adapter })
			if index >= 0 && config.Adapters[index].Agent == candidate && wire.Satisfies(selector, config.Adapters[index].Labels) {
				return wire.Placement{Agent: candidate, Selector: selector}, true
			}
		}
	}
	return wire.Placement{}, false
}

func spare(ad wire.Advertisement) int64 {
	return ad.Capacity - ad.InFlight
}
