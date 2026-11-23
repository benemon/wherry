package operator

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/benemon/wherry/internal/telemetry"
	"github.com/twmb/franz-go/pkg/kadm"
)

// Leftover is a topic that removal left on the broker: no operator command deletes a topic or group.
type Leftover struct {
	Topic string
	// Tenant names an ended tenant, whose feed, transcript, trace and host key topics are its; Tenant and Host name an
	// ended host of a live tenant; Receiver names an ended receiver. Pool names the pool of a signature task topic that
	// no live receiver's recorded profile uses, which belongs to the pool and not to one actor.
	Tenant, Host, Receiver, Pool string
	// Offsets is the end offset less the start offset summed over the topic's partitions: an upper bound on the records
	// retained, as transaction markers and compacted records take offsets too.
	Offsets int64
	// Kept marks a topic readers still need: an ended receiver's key topic, which the pool's history is verified against.
	Kept bool
}

// Leftovers lists the topics of ended tenants, of ended hosts of live tenants and of ended receivers that still exist,
// then each pool's signature task topics that no live receiver's recorded profile uses, with the offsets each retains.
// A pool's custodian, receivers and events topics and the trace topics are shared, so never listed. It deletes nothing.
func (service *Service) Leftovers(ctx context.Context) ([]Leftover, error) {
	state, err := service.LoadState(ctx)
	if err != nil {
		return nil, err
	}
	admin := kadm.NewClient(service.Operator)
	// Asking for authorized operations is what makes kadm bypass its metadata cache, which can list a deleted topic.
	listed, err := admin.ListTopics(kadm.WithAuthorizedOps(ctx))
	if err != nil {
		return nil, err
	}
	tasks := map[string][]string{}
	for pool := range state.Pools {
		for topic := range listed {
			if strings.HasPrefix(topic, PoolTasksPrefix(pool)) {
				tasks[pool] = append(tasks[pool], topic)
			}
		}
	}
	leftovers := slices.DeleteFunc(leftoverTopics(state, tasks), func(leftover Leftover) bool { return !listed.Has(leftover.Topic) })
	if len(leftovers) == 0 {
		// kadm lists every topic when it is given none.
		return nil, nil
	}
	var topics []string
	for _, leftover := range leftovers {
		if err := listed[leftover.Topic].Err; err != nil {
			return nil, fmt.Errorf("topic %s: %w", leftover.Topic, err)
		}
		topics = append(topics, leftover.Topic)
	}
	starts, err := admin.ListStartOffsets(ctx, topics...)
	if err == nil {
		err = starts.Error()
	}
	if err != nil {
		return nil, err
	}
	ends, err := admin.ListEndOffsets(ctx, topics...)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		return nil, err
	}
	for i, leftover := range leftovers {
		for _, end := range ends[leftover.Topic] {
			start, ok := starts.Lookup(leftover.Topic, end.Partition)
			if !ok {
				return nil, fmt.Errorf("topic %s partition %d: no start offset listed", leftover.Topic, end.Partition)
			}
			leftovers[i].Offsets += end.Offset - start.Offset
		}
	}
	return leftovers, nil
}

// leftoverTopics names, with no offsets, every topic Leftovers would list if it existed: ended tenants' topics, ended
// hosts' of live tenants, ended receivers', then each pool's topics among tasks that no live receiver's profile uses.
func leftoverTopics(state State, tasks map[string][]string) []Leftover {
	var leftovers []Leftover
	for _, tenant := range slices.Sorted(maps.Keys(state.Tenants)) {
		if !state.Tenants[tenant] {
			continue
		}
		for _, topic := range []string{TenantTopic(tenant), TenantTranscriptTopic(tenant), telemetry.TraceTopic(tenant)} {
			leftovers = append(leftovers, Leftover{Topic: topic, Tenant: tenant})
		}
		for _, principal := range slices.Sorted(maps.Keys(state.Hosts)) {
			if host := state.Hosts[principal]; host.Tenant == tenant {
				leftovers = append(leftovers, Leftover{Topic: HostTopic(tenant, host.Name), Tenant: tenant})
			}
		}
	}
	for _, principal := range slices.Sorted(maps.Keys(state.Hosts)) {
		if host := state.Hosts[principal]; state.Ended[principal] && !state.Tenants[host.Tenant] {
			leftovers = append(leftovers, Leftover{Topic: HostTopic(host.Tenant, host.Name), Tenant: host.Tenant, Host: host.Name})
		}
	}
	for _, receiver := range slices.Sorted(maps.Keys(state.Receivers)) {
		if state.Ended[ReceiverPrincipal(receiver)] {
			leftovers = append(leftovers, Leftover{Topic: ReceiverTopic(receiver), Receiver: receiver, Kept: true}, Leftover{Topic: ReceiverProgressTopic(receiver), Receiver: receiver})
		}
	}
	for _, pool := range slices.Sorted(maps.Keys(state.Pools)) {
		used := signatureTopics(state, pool)
		for _, topic := range slices.Sorted(slices.Values(tasks[pool])) {
			if !slices.Contains(used, topic) {
				leftovers = append(leftovers, Leftover{Topic: topic, Pool: pool})
			}
		}
	}
	return leftovers
}
