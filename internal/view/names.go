package view

import (
	"regexp"
	"slices"
	"strings"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/telemetry"
	"github.com/benemon/wherry/internal/wire"
)

// Unknown is the placeholder for a name a record carries that the operator's configuration does not: no string from
// a record or span reaches a response unless it is rebuilt from a configured value or a fixed set.
const Unknown = "unknown"

// catalog is every name the configuration yields, from the history's state and each pool's terms over the window:
// what a response may carry. A string from a record or span is emitted only when it matches one.
type catalog struct {
	tenants, pools, receivers map[string]bool
	// hosts holds each tenant's host principals and hostNames every host name, for the span attribute that carries a
	// name alone.
	hosts, hostNames map[string]bool
	// agents and models are by pool, over every terms record read; profiles by receiver.
	agents, models, profiles map[string]map[string]bool
	// topics is every topic the configuration derives: the operator topic, each pool's events, custodian and receivers
	// topics and the signature task topics of its receivers' profiles, each tenant's feed, transcript and trace
	// topics, each receiver's key and progress topics and each host's key topic. taskTopics is the task topics alone.
	topics, taskTopics                map[string]bool
	credentials                       map[string]bool
	endpoints, buckets                map[string]bool
	groups                            map[string]bool
	allAgents, allModels, allProfiles map[string]bool
}

func newCatalog(h operator.History) *catalog {
	c := &catalog{tenants: map[string]bool{}, pools: map[string]bool{}, receivers: map[string]bool{}, hosts: map[string]bool{}, hostNames: map[string]bool{},
		agents: map[string]map[string]bool{}, models: map[string]map[string]bool{}, profiles: map[string]map[string]bool{}, topics: map[string]bool{}, taskTopics: map[string]bool{},
		credentials: map[string]bool{}, endpoints: map[string]bool{}, buckets: map[string]bool{}, groups: map[string]bool{}, allAgents: map[string]bool{}, allModels: map[string]bool{}, allProfiles: map[string]bool{}}
	state := h.State
	c.topics[operator.OperatorTopic] = true
	c.topics[telemetry.TraceTopic("")] = true
	for pool, terms := range state.Pools {
		c.pools[pool] = true
		c.agents[pool], c.models[pool] = map[string]bool{}, map[string]bool{}
		versions := []operator.Terms{terms}
		for _, entry := range h.Terms[pool] {
			versions = append(versions, entry.Terms)
		}
		for _, version := range versions {
			for _, agent := range version.Agents {
				c.agents[pool][agent.Name], c.allAgents[agent.Name] = true, true
				for _, model := range agent.Models {
					c.models[pool][model], c.allModels[model] = true, true
				}
				for _, credential := range agent.Credentials {
					c.credentials[credential.Name] = true
				}
			}
			if version.Storage != nil {
				c.endpoints[version.Storage.Endpoint], c.buckets[version.Storage.Bucket] = true, true
			}
		}
		for _, topic := range []string{operator.PoolEventsTopic(pool), operator.CustodianTopic(pool), operator.PoolReceiversTopic(pool)} {
			c.topics[topic] = true
		}
		c.groups[operator.PoolGroup(pool)], c.groups[operator.CustodianGroup(pool)] = true, true
	}
	for tenant := range state.Tenants {
		c.tenants[tenant] = true
		for _, topic := range []string{operator.TenantTopic(tenant), operator.TenantTranscriptTopic(tenant), telemetry.TraceTopic(tenant)} {
			c.topics[topic] = true
		}
	}
	for principal, host := range state.Hosts {
		c.hosts[principal], c.hostNames[host.Name] = true, true
		c.topics[operator.HostTopic(host.Tenant, host.Name)] = true
	}
	for receiver, config := range state.Receivers {
		c.receivers[receiver] = true
		c.profiles[receiver] = map[string]bool{}
		c.topics[operator.ReceiverTopic(receiver)], c.topics[operator.ReceiverProgressTopic(receiver)] = true, true
		for _, profile := range config.Profiles {
			c.profiles[receiver][profile.Name], c.allProfiles[profile.Name] = true, true
			topic := operator.SignatureTopic(config.Pool, profile.Agent, profile.Labels)
			c.topics[topic], c.taskTopics[topic] = true, true
		}
	}
	return c
}

// pick is name when set holds it, empty when name is empty, else the placeholder.
func pick(set map[string]bool, name string) string {
	switch {
	case name == "":
		return ""
	case set[name]:
		return name
	}
	return Unknown
}

func (c *catalog) tenant(name string) string   { return pick(c.tenants, name) }
func (c *catalog) pool(name string) string     { return pick(c.pools, name) }
func (c *catalog) receiver(name string) string { return pick(c.receivers, name) }
func (c *catalog) host(tenant, name string) string {
	if name != "" && !c.hosts[operator.TenantPrincipal(tenant, name)] {
		return Unknown
	}
	return name
}
func (c *catalog) agent(pool, name string) string    { return pick(c.agents[pool], name) }
func (c *catalog) model(pool, name string) string    { return pick(c.models[pool], name) }
func (c *catalog) profile(rcv, name string) string   { return pick(c.profiles[rcv], name) }
func (c *catalog) taskTopic(name string) string      { return pick(c.taskTopics, name) }
func (c *catalog) topic(name string) bool            { return c.topics[name] }
func (c *catalog) storage(s operator.Storage) string { return pick(c.endpoints, s.Endpoint) }

// fixed is name when it is one of set, empty when empty, else other.
func fixed(set []string, name string) string {
	switch {
	case name == "":
		return ""
	case slices.Contains(set, name):
		return name
	}
	return "other"
}

// The fixed sets a record's enumerated fields are rebuilt from: a status kind, a task state, a status event, a start
// failure class, a claim decision's reason, an open refusal's reason, an advertisement's gate and a share group's
// state.
var (
	statusKinds   = []string{"claimed", "started", "rejected", "terminal", "released", "adopting", "reconciling", "placing", "working"}
	taskStates    = []string{string(wire.TaskStateUnspecified), string(wire.TaskStateSubmitted), string(wire.TaskStateWorking), string(wire.TaskStateCompleted), string(wire.TaskStateFailed), string(wire.TaskStateCanceled), string(wire.TaskStateInputRequired), string(wire.TaskStateRejected), string(wire.TaskStateAuthRequired)}
	statusEvents  = []string{wire.EventKeyLost}
	startClasses  = []string{"dispatch", "platform", "input-too-large"}
	claimReasons  = []string{wire.ClaimUnchanged, wire.ClaimReleased, wire.ClaimLease, wire.ClaimLive}
	refuseReasons = []string{wire.RefusedTwoClaimants, wire.RefusedNotClaimant, wire.RefusedTerminal, wire.RefusedPastBound, wire.RefusedKeyUnavailable, wire.RefusedEnvelopeInvalid, wire.RefusedPoolSuspended}
	gates         = []string{wire.GatedSuspended, wire.GatedNoReadyOffer, wire.GatedPendingRun, wire.GatedDraining}
	groupStates   = []string{"Empty", "Stable", "Dead", "PreparingRebalance", "CompletingRebalance", "Assigning", "Reconciling"}
	httpMethods   = []string{"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"}
)

// taskState is state as a fixed task state, the unspecified one for any other.
func taskState(state wire.TaskState) wire.TaskState {
	if slices.Contains(taskStates, string(state)) {
		return state
	}
	return wire.TaskStateUnspecified
}

// stateWord is a task state as a stage word: completed, failed, canceled, rejected, working.
func stateWord(state wire.TaskState) string {
	return strings.ToLower(strings.TrimPrefix(string(taskState(state)), "TASK_STATE_"))
}

// The grammars of the identifiers wherry derives, whose bodies are hex: a run ID, an event ID, a key ID. Anything
// else in their place is the placeholder.
var (
	runIDPattern   = regexp.MustCompile(`^run-[0-9a-f]{32}$`)
	eventIDPattern = regexp.MustCompile(`^event-[0-9a-f]{32}$`)
	keyIDPattern   = regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)
	plainPattern   = regexp.MustCompile(`^[A-Za-z0-9.:/_\[\]-]{1,253}$`)
)

func validRunID(id string) bool { return runIDPattern.MatchString(id) }

// eventID is id when it is one wherry issues, else one derived from the record's digest, so the model never holds
// the text a record put in its place.
func eventID(id, digest string) string {
	if eventIDPattern.MatchString(id) {
		return id
	}
	return "event-" + digest[:32]
}

// plainText is value when it is a hostname, rack or listener of plain characters, else other.
func plainText(value string) string {
	if value == "" || plainPattern.MatchString(value) {
		return value
	}
	return "other"
}
