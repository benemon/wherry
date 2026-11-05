package client

import (
	"cmp"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/wire"
)

// resolvePlacement chooses, among live advertisements in the tenant's entitled pools, a signature: a pool and the task
// topic of a ready profile launching the agent with labels satisfying selector. Labels come from the operator's config
// record, never the advertisement. A pool whose routable labels lack a selector key is excluded before any advertisement
// is read. Each member adds its spare capacity once to every signature it offers, an estimate: capacity is shared across
// a member's signatures. A default-agent signature wins, then the greatest spare, then the lowest topic. When nothing
// places the task, every entitled pool excluded by a key is that refusal, else a pool's terms refusing its model is
// that refusal.
func resolvePlacement(state operator.State, tenant, agent, model string, selector []wire.Requirement, ads map[string]wire.Advertisement) (string, wire.Placement, string, error) {
	var pools []string
	excluded := map[string]error{}
	for _, pool := range slices.Sorted(maps.Keys(state.Entitlements[tenant])) {
		if !state.Entitlements[tenant][pool] {
			continue
		}
		pools = append(pools, pool)
		if err := routable(state.Pools[pool], pool, selector); err != nil {
			excluded[pool] = err
		}
	}
	type signature struct {
		pool, topic string
		placement   wire.Placement
		isDefault   bool
		spare       int64
	}
	signatures := map[[2]string]*signature{}
	var refused error
	for _, receiver := range slices.Sorted(maps.Keys(ads)) {
		ad := ads[receiver]
		if !state.Entitlements[tenant][ad.Pool] || excluded[ad.Pool] != nil {
			continue
		}
		terms, config := state.Pools[ad.Pool], state.Receivers[receiver]
		candidates, err := candidates(terms, ad.Pool, agent, model)
		if errors.Is(err, ErrInvalidInput) && refused == nil {
			refused = err
		}
		offered := map[[2]string]bool{}
		for _, candidate := range candidates {
			for _, offer := range ad.Offers {
				index := slices.IndexFunc(config.Profiles, func(profile operator.ProfileSettings) bool { return profile.Name == offer.Profile })
				if index < 0 || config.Profiles[index].Agent != candidate.Name || !wire.Satisfies(selector, config.Profiles[index].Labels) {
					continue
				}
				topic := operator.SignatureTopic(ad.Pool, terms, candidate.Name, config.Profiles[index].Labels)
				key := [2]string{ad.Pool, topic}
				if offered[key] {
					continue
				}
				offered[key] = true
				if signatures[key] == nil {
					placed := wire.Placement{Agent: candidate.Name, Selector: selector, Model: cmp.Or(model, candidate.DefaultModel)}
					signatures[key] = &signature{pool: ad.Pool, topic: topic, placement: placed, isDefault: agent == "" && candidate.Name == terms.DefaultAgent}
				}
				signatures[key].spare += spare(ad)
			}
		}
	}
	var chosen *signature
	for _, key := range slices.SortedFunc(maps.Keys(signatures), func(a, b [2]string) int { return cmp.Compare(a[1], b[1]) }) {
		candidate := signatures[key]
		if chosen == nil || candidate.isDefault && !chosen.isDefault || candidate.isDefault == chosen.isDefault && candidate.spare > chosen.spare {
			chosen = candidate
		}
	}
	if chosen == nil {
		if len(pools) > 0 && len(excluded) == len(pools) {
			return "", wire.Placement{}, "", excluded[pools[0]]
		}
		return "", wire.Placement{}, "", cmp.Or(refused, ErrNoMatchingReceiver)
	}
	return chosen.pool, chosen.placement, chosen.topic, nil
}

// routable refuses a selector naming a key outside pool's routable labels.
func routable(terms operator.Terms, pool string, selector []wire.Requirement) error {
	var keys []string
	for _, requirement := range selector {
		if !slices.Contains(terms.Routable, requirement.Key) {
			keys = append(keys, requirement.Key)
		}
	}
	if len(keys) == 0 {
		return nil
	}
	allowed := "none"
	if len(terms.Routable) > 0 {
		allowed = strings.Join(slices.Sorted(slices.Values(terms.Routable)), ", ")
	}
	slices.Sort(keys)
	return fmt.Errorf("%w: selector keys %s are not routable in pool %s (routable: %s)", ErrInvalidInput, strings.Join(keys, ", "), pool, allowed)
}

// candidates returns the agents, by preference, whose terms in pool hold model: the default agent first. An empty model
// takes the agent's default model, and an agent with models but no default requires one.
func candidates(terms operator.Terms, pool string, agent, model string) ([]operator.AgentTerms, error) {
	if agent != "" {
		index := slices.IndexFunc(terms.Agents, func(allowed operator.AgentTerms) bool { return allowed.Name == agent })
		if index < 0 {
			return nil, ErrNoMatchingReceiver
		}
		if model != "" && !slices.Contains(terms.Agents[index].Models, model) {
			return nil, fmt.Errorf("%w: model %s is not in agent %s's models in pool %s", ErrInvalidInput, model, agent, pool)
		}
		if model == "" && requiresModel(terms.Agents[index]) {
			return nil, fmt.Errorf("%w: agent %s in pool %s requires a model", ErrInvalidInput, agent, pool)
		}
	}
	var candidates []operator.AgentTerms
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
		return nil, fmt.Errorf("%w: every agent in pool %s requires a model", ErrInvalidInput, pool)
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("%w: model %s is in no agent's models in pool %s", ErrInvalidInput, model, pool)
	}
	return candidates, nil
}

// requiresModel reports whether agent's terms make every task name a model: models without a default.
func requiresModel(agent operator.AgentTerms) bool {
	return len(agent.Models) > 0 && agent.DefaultModel == ""
}

func spare(ad wire.Advertisement) int64 {
	return ad.Capacity - ad.InFlight
}

// profileInlineLimit returns the smallest declared limit of the advertised profiles in pool whose signature topic is
// topic.
func profileInlineLimit(state operator.State, pool, topic string, ads map[string]wire.Advertisement) (int64, string) {
	var limit int64
	var limiting string
	for _, receiver := range slices.Sorted(maps.Keys(ads)) {
		ad := ads[receiver]
		if ad.Pool != pool {
			continue
		}
		config := state.Receivers[receiver]
		for _, offer := range ad.Offers {
			index := slices.IndexFunc(config.Profiles, func(profile operator.ProfileSettings) bool { return profile.Name == offer.Profile })
			if index < 0 || operator.SignatureTopic(pool, state.Pools[pool], config.Profiles[index].Agent, config.Profiles[index].Labels) != topic {
				continue
			}
			if offer.InlineLimit > 0 && (limit == 0 || offer.InlineLimit < limit) {
				limit, limiting = offer.InlineLimit, offer.Profile
			}
		}
	}
	return limit, limiting
}
