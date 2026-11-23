package operator

import (
	"context"
	"errors"
	"time"

	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// History is wherry.operator folded with the broker's append time of each end, so a record can be checked against the
// membership and entitlements in force when it was appended rather than now. Load drops the times; the operator view
// keeps them.
type History struct {
	State State
	// EndedAt is the append time of each ended receiver's or host's end record, by principal; EntitlementEndedAt that
	// of each ended entitlement, by tenant then pool. An entitlement granted again leaves EntitlementEndedAt.
	EndedAt            map[string]time.Time
	EntitlementEndedAt map[string]map[string]time.Time
	// Terms is each pool's terms records in append order, the terms in force from each record's append time.
	Terms map[string][]TermsAt
	// Next is the wherry.operator offset the next read resumes at.
	Next    int64
	records []timedRecord
}

// TermsAt is a pool's terms from the append time of the record that set them.
type TermsAt struct {
	At    time.Time
	Terms Terms
}

type timedRecord struct {
	Record
	at time.Time
}

// ReadHistory folds wherry.operator from its start with the append time of each record; it fails closed as Load does.
func (r *Reader) ReadHistory(ctx context.Context) (History, error) {
	records, _, end, err := r.ReadRange(ctx, OperatorTopic, 0)
	if err != nil {
		return History{}, err
	}
	history := History{}
	if err := history.Extend(records, end); err != nil {
		return History{}, err
	}
	return history, nil
}

// Extend folds records, read from wherry.operator at Next, into the history and moves Next to end, the last stable
// offset they were read to. A record this release cannot decode fails the fold, as it fails Load.
func (h *History) Extend(records []*kgo.Record, end int64) error {
	raw := make([]rawRecord, len(records))
	for i, record := range records {
		raw[i] = rawRecord{record.Offset, record.Value}
	}
	decoded, err := decode(raw)
	if err != nil {
		return err
	}
	timed := make([]timedRecord, 0, len(h.records)+len(decoded))
	timed = append(timed, h.records...)
	for i, record := range decoded {
		timed = append(timed, timedRecord{record, records[i].Timestamp})
	}
	folded, err := foldHistory(timed)
	if err != nil {
		return err
	}
	folded.Next = max(h.Next, end)
	*h = folded
	return nil
}

func foldHistory(records []timedRecord) (History, error) {
	h := History{State: NewState(), EndedAt: map[string]time.Time{}, EntitlementEndedAt: map[string]map[string]time.Time{}, Terms: map[string][]TermsAt{}, records: records}
	for _, record := range records {
		if err := apply(&h.State, record.Record); err != nil {
			return History{}, err
		}
		switch record.Kind {
		case "pool":
			h.Terms[record.Pool] = append(h.Terms[record.Pool], TermsAt{record.at, h.State.Pools[record.Pool]})
		case "entitlement":
			delete(h.EntitlementEndedAt[record.Tenant], record.Pool)
			if len(h.EntitlementEndedAt[record.Tenant]) == 0 {
				delete(h.EntitlementEndedAt, record.Tenant)
			}
		case "end":
			switch {
			case record.Receiver != "" || record.Host != "":
				h.EndedAt[subject(record.Record)] = record.at
			case record.Pool != "":
				if h.EntitlementEndedAt[record.Tenant] == nil {
					h.EntitlementEndedAt[record.Tenant] = map[string]time.Time{}
				}
				h.EntitlementEndedAt[record.Tenant][record.Pool] = record.at
			}
		}
	}
	if err := h.State.CheckNames(); err != nil {
		return History{}, err
	}
	return h, nil
}

// liveAt reports whether principal, a receiver's or host's, had not ended at at: it is not ended, or its end was
// appended after at.
func (h History) liveAt(principal string, at time.Time) bool {
	if !h.State.Ended[principal] {
		return true
	}
	ended, known := h.EndedAt[principal]
	return known && at.Before(ended)
}

// MemberAt reports whether receiver was a live member of pool at at.
func (h History) MemberAt(receiver, pool string, at time.Time) bool {
	config, ok := h.State.Receivers[receiver]
	return ok && config.Pool == pool && h.liveAt(ReceiverPrincipal(receiver), at)
}

// HostAt reports whether host was a live host of tenant at at.
func (h History) HostAt(tenant, host string, at time.Time) bool {
	principal := TenantPrincipal(tenant, host)
	_, ok := h.State.Hosts[principal]
	return ok && h.liveAt(principal, at)
}

// EntitledAt reports whether tenant was entitled to pool at at: it is, or the entitlement ended after at.
func (h History) EntitledAt(tenant, pool string, at time.Time) bool {
	if h.State.Entitlements[tenant][pool] {
		return true
	}
	ended, known := h.EntitlementEndedAt[tenant][pool]
	return known && at.Before(ended)
}

// EverEntitled reports whether tenant is or was entitled to pool.
func (h History) EverEntitled(tenant, pool string) bool {
	return h.State.Entitlements[tenant][pool] || h.State.EndedEntitlements[tenant][pool]
}

// TermsAt is the terms of pool in force at at: those of the latest terms record appended at or before at. It is false
// when the history read holds none by then.
func (h History) TermsAt(pool string, at time.Time) (Terms, bool) {
	var terms Terms
	found := false
	for _, entry := range h.Terms[pool] {
		if entry.At.After(at) {
			break
		}
		terms, found = entry.Terms, true
	}
	return terms, found
}

// AllKeyTopics names the key topic of every receiver and every host the state records, ended or not: the keys that
// verify history. HostKeyTopics and PoolKeyTopics name the live ones a reader admitting work needs.
func AllKeyTopics(state State) map[string]string {
	topics := map[string]string{}
	for receiver := range state.Receivers {
		topics[receiver] = ReceiverTopic(receiver)
	}
	for _, host := range state.Hosts {
		topics[signed.HostOwner(host.Tenant, host.Name)] = HostTopic(host.Tenant, host.Name)
	}
	return topics
}

var errEndedMember = errors.New("record was appended after its receiver's membership ended")

// historicReceiver checks a receiver record appended at appended: its receiver was a live member of its pool then,
// which is topicPool when the record was read from a pool's events, and its tenant feedTenant's when read from a
// tenant feed. Its tenant was entitled to the pool then, or, with joined, the run it belongs to was admitted to the
// pool before the entitlement ended: the caller joins it to that echo.
func (h History) historicReceiver(binding wire.ReceiverBinding, topicPool, feedTenant string, appended time.Time, joined bool) error {
	if topicPool != "" && binding.Pool != topicPool {
		return errors.New("record names another pool than its topic")
	}
	if feedTenant != "" && binding.Tenant != feedTenant {
		return errors.New("record names another tenant than its feed")
	}
	if !h.MemberAt(binding.Receiver, binding.Pool, appended) {
		return errEndedMember
	}
	if !h.EntitledAt(binding.Tenant, binding.Pool, appended) && !(joined && h.EverEntitled(binding.Tenant, binding.Pool)) {
		return errors.New("tenant was not entitled to the pool when the record was appended")
	}
	return nil
}

// AuthorizeHistoricStatus accepts a status appended at appended, verified with the keys current then, from a receiver
// that was a live member of its pool then; topicPool, feedTenant and joined are as CheckStatus takes them.
func AuthorizeHistoricStatus(h History, keys signed.Keys, encoded []byte, topicPool, feedTenant string, appended time.Time, joined bool) (wire.RunStatus, error) {
	var status wire.RunStatus
	if err := wire.ParseHistory(appended)(encoded, keys, &status); err != nil {
		return wire.RunStatus{}, err
	}
	if err := h.CheckStatus(status, topicPool, feedTenant, appended, joined); err != nil {
		return wire.RunStatus{}, err
	}
	return status, nil
}

// CheckStatus is AuthorizeHistoricStatus's check on a status already verified: a reader that verifies in parallel
// applies it in order. topicPool is the events topic's pool, or empty on a tenant feed; feedTenant the feed's tenant,
// or empty on pool events; joined whether the run has its echo.
func (h History) CheckStatus(status wire.RunStatus, topicPool, feedTenant string, appended time.Time, joined bool) error {
	return h.historicReceiver(status.Wherry.ReceiverBinding, topicPool, feedTenant, appended, joined)
}

// AuthorizeHistoricStartFailure is AuthorizeHistoricStatus for a start failure on topicPool's events.
func AuthorizeHistoricStartFailure(h History, keys signed.Keys, encoded []byte, topicPool string, appended time.Time, joined bool) (wire.StartFailure, error) {
	var failure wire.StartFailure
	if err := wire.ParseHistory(appended)(encoded, keys, &failure); err != nil {
		return wire.StartFailure{}, err
	}
	if err := h.CheckStartFailure(failure, topicPool, appended, joined); err != nil {
		return wire.StartFailure{}, err
	}
	return failure, nil
}

// CheckStartFailure is CheckStatus for a start failure.
func (h History) CheckStartFailure(failure wire.StartFailure, topicPool string, appended time.Time, joined bool) error {
	return h.historicReceiver(failure.Wherry, topicPool, "", appended, joined)
}

// AuthorizeHistoricResult is AuthorizeHistoricStatus for a result on feedTenant's feed.
func AuthorizeHistoricResult(h History, keys signed.Keys, encoded []byte, feedTenant string, appended time.Time, joined bool) (wire.Result, error) {
	var result wire.Result
	if err := wire.ParseHistory(appended)(encoded, keys, &result); err != nil {
		return wire.Result{}, err
	}
	if err := h.CheckResult(result, feedTenant, appended, joined); err != nil {
		return wire.Result{}, err
	}
	return result, nil
}

// CheckResult is CheckStatus for a result.
func (h History) CheckResult(result wire.Result, feedTenant string, appended time.Time, joined bool) error {
	return h.historicReceiver(result.Wherry.ReceiverBinding, "", feedTenant, appended, joined)
}

// AuthorizeHistoricEcho accepts an echo on feedTenant's feed appended at appended from a receiver that was a live
// member of the pool it names then, for a tenant entitled to that pool then. The echo is what admits a run, so an ended
// entitlement admits nothing after its end.
func AuthorizeHistoricEcho(h History, keys signed.Keys, encoded []byte, feedTenant string, appended time.Time) (wire.Echo, error) {
	var echo wire.Echo
	if err := wire.ParseHistory(appended)(encoded, keys, &echo); err != nil {
		return wire.Echo{}, err
	}
	if err := h.CheckEcho(echo, feedTenant, appended); err != nil {
		return wire.Echo{}, err
	}
	return echo, nil
}

// CheckEcho is AuthorizeHistoricEcho's check on an echo already verified.
func (h History) CheckEcho(echo wire.Echo, feedTenant string, appended time.Time) error {
	if echo.Tenant != feedTenant {
		return errors.New("echo names another tenant than its feed")
	}
	if !h.MemberAt(echo.Receiver, echo.Pool, appended) {
		return errEndedMember
	}
	if !h.EntitledAt(feedTenant, echo.Pool, appended) {
		return errors.New("tenant was not entitled to the pool when the echo was appended")
	}
	return nil
}

// AuthorizeHistoricCancel accepts a cancel on topicPool's events appended at appended, verified with the host's key
// current then, from a host of runTenant that was live then; the tenant is or was entitled to the pool, since a host
// may cancel a run held after its entitlement ended.
func AuthorizeHistoricCancel(h History, keys signed.Keys, encoded []byte, topicPool, runTenant string, appended time.Time) (wire.Cancel, error) {
	var cancel wire.Cancel
	if err := wire.ParseHistory(appended)(encoded, keys, &cancel); err != nil {
		return wire.Cancel{}, err
	}
	if err := h.CheckCancel(cancel, topicPool, runTenant, appended); err != nil {
		return wire.Cancel{}, err
	}
	return cancel, nil
}

// CheckCancel is AuthorizeHistoricCancel's check on a cancel already verified.
func (h History) CheckCancel(cancel wire.Cancel, topicPool, runTenant string, appended time.Time) error {
	if cancel.Pool != topicPool || cancel.Tenant != runTenant {
		return errors.New("cancel names another pool or tenant than its run")
	}
	if !h.HostAt(cancel.Tenant, cancel.Host, appended) {
		return errors.New("cancel was appended after its host's membership ended")
	}
	if !h.EverEntitled(cancel.Tenant, topicPool) {
		return errors.New("tenant was never entitled to the pool")
	}
	return nil
}
