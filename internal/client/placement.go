package client

import (
	"cmp"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/wire"
)

// resolvePlacement chooses, among live advertisements in the tenant's entitled pools, the member with the most spare
// capacity, then the lowest receiver ID, that has a ready adapter launching the agent with labels satisfying selector.
// An empty agent takes the pool's default agent, then its declared order. Labels come from the operator's config
// record, never the advertisement. When no member places the task and a pool's terms refuse its model, the error is
// that refusal.
func resolvePlacement(state admin.State, tenant, agent, model string, selector []wire.Requirement, ads map[string]wire.Advertisement) (string, wire.Placement, error) {
	var chosen *wire.Advertisement
	var placement wire.Placement
	var refused error
	for _, receiver := range slices.Sorted(maps.Keys(ads)) {
		ad := ads[receiver]
		if !state.Entitlements[tenant][ad.Pool] {
			continue
		}
		placed, err := place(state.Pools[ad.Pool], ad.Pool, state.Receivers[receiver], agent, model, selector, ad.Offers)
		switch {
		case err == nil && (chosen == nil || spare(ad) > spare(*chosen)):
			chosen, placement = &ad, placed
		case errors.Is(err, ErrInvalidInput) && refused == nil:
			refused = err
		}
	}
	if chosen == nil {
		return "", wire.Placement{}, cmp.Or(refused, ErrNoMatchingReceiver)
	}
	return chosen.Pool, placement, nil
}

// place returns the first agent, by preference, whose terms in pool hold model and that one of the member's ready
// adapters launches with labels satisfying selector. An empty model takes the agent's default model, and an agent with
// models but no default requires one.
func place(terms admin.Terms, pool string, config admin.ReceiverConfig, agent, model string, selector []wire.Requirement, offers []wire.Offer) (wire.Placement, error) {
	if agent != "" {
		index := slices.IndexFunc(terms.Agents, func(allowed admin.AgentTerms) bool { return allowed.Name == agent })
		if index < 0 {
			return wire.Placement{}, ErrNoMatchingReceiver
		}
		if model != "" && !slices.Contains(terms.Agents[index].Models, model) {
			return wire.Placement{}, fmt.Errorf("%w: model %s is not in agent %s's models in pool %s", ErrInvalidInput, model, agent, pool)
		}
		if model == "" && requiresModel(terms.Agents[index]) {
			return wire.Placement{}, fmt.Errorf("%w: agent %s in pool %s requires a model", ErrInvalidInput, agent, pool)
		}
	}
	var candidates []admin.AgentTerms
	for _, allowed := range terms.Agents {
		switch {
		case agent != "" && allowed.Name != agent, model != "" && !slices.Contains(allowed.Models, model), model == "" && requiresModel(allowed):
		case allowed.Name == terms.DefaultAgent:
			candidates = slices.Insert(candidates, 0, allowed)
		default:
			candidates = append(candidates, allowed)
		}
	}
	if len(candidates) == 0 && model == "" {
		return wire.Placement{}, fmt.Errorf("%w: every agent in pool %s requires a model", ErrInvalidInput, pool)
	}
	if len(candidates) == 0 {
		return wire.Placement{}, fmt.Errorf("%w: model %s is in no agent's models in pool %s", ErrInvalidInput, model, pool)
	}
	for _, candidate := range candidates {
		for _, offer := range offers {
			index := slices.IndexFunc(config.Adapters, func(adapter admin.AdapterSettings) bool { return adapter.Name == offer.Adapter })
			if index >= 0 && config.Adapters[index].Agent == candidate.Name && wire.Satisfies(selector, config.Adapters[index].Labels) {
				placed := wire.Placement{Agent: candidate.Name, Selector: selector, Model: model}
				if model == "" {
					placed.Model = candidate.DefaultModel
				}
				return placed, nil
			}
		}
	}
	return wire.Placement{}, ErrNoMatchingReceiver
}

// requiresModel reports whether agent's terms make every task name a model: models without a default.
func requiresModel(agent admin.AgentTerms) bool {
	return len(agent.Models) > 0 && agent.DefaultModel == ""
}

func spare(ad wire.Advertisement) int64 {
	return ad.Capacity - ad.InFlight
}

// adapterInlineLimit returns the smallest declared limit of the advertised adapters in pool that satisfy placement.
func adapterInlineLimit(state admin.State, pool string, placement wire.Placement, ads map[string]wire.Advertisement) (int64, string) {
	var limit int64
	var limiting string
	for _, receiver := range slices.Sorted(maps.Keys(ads)) {
		ad := ads[receiver]
		if ad.Pool != pool {
			continue
		}
		config := state.Receivers[receiver]
		for _, offer := range ad.Offers {
			index := slices.IndexFunc(config.Adapters, func(adapter admin.AdapterSettings) bool { return adapter.Name == offer.Adapter })
			if index < 0 || config.Adapters[index].Agent != placement.Agent || !wire.Satisfies(placement.Selector, config.Adapters[index].Labels) {
				continue
			}
			if offer.InlineLimit > 0 && (limit == 0 || offer.InlineLimit < limit) {
				limit, limiting = offer.InlineLimit, offer.Adapter
			}
		}
	}
	return limit, limiting
}
