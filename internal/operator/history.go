package operator

import (
	"context"
	"errors"
	"time"

	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// History is wherry.operator folded with each pool's terms by the broker's append time of the record that set them, so
// a run's storage can be read from the terms in force when it was admitted. Load drops the times; the operator view
// keeps them. Membership and entitlement over time are not kept: who may write a topic, and when, is decided by the
// Kafka ACLs at write time.
type History struct {
	State State
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
	h := History{State: NewState(), Terms: map[string][]TermsAt{}, records: records}
	for _, record := range records {
		if err := apply(&h.State, record.Record); err != nil {
			return History{}, err
		}
		if record.Kind == "pool" {
			h.Terms[record.Pool] = append(h.Terms[record.Pool], TermsAt{record.at, h.State.Pools[record.Pool]})
		}
	}
	if err := h.State.CheckNames(); err != nil {
		return History{}, err
	}
	return h, nil
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

// The checks of a verified record against the topic it was read from. A record's text names its pool and tenant, which
// a reader of history takes as the run's, so a receiver's record is held to the pool whose events, or the tenant whose
// feed, it was read from. Who may write the topic at all is the ACL's decision, not re-derived here.

// checkReceiver checks a receiver record's binding: its pool is topicPool when the record was read from a pool's
// events, and its tenant feedTenant when read from a tenant's feed.
func checkReceiver(binding wire.ReceiverBinding, topicPool, feedTenant string) error {
	if topicPool != "" && binding.Pool != topicPool {
		return errors.New("record names another pool than its topic")
	}
	if feedTenant != "" && binding.Tenant != feedTenant {
		return errors.New("record names another tenant than its feed")
	}
	return nil
}

// CheckStatus checks a verified status against the pool events or tenant feed it was read from; topicPool is empty
// on a tenant feed and feedTenant on pool events.
func CheckStatus(status wire.RunStatus, topicPool, feedTenant string) error {
	return checkReceiver(status.Wherry.ReceiverBinding, topicPool, feedTenant)
}

// CheckStartFailure is CheckStatus for a start failure on topicPool's events.
func CheckStartFailure(failure wire.StartFailure, topicPool string) error {
	return checkReceiver(failure.Wherry, topicPool, "")
}

// CheckResult is CheckStatus for a result on feedTenant's feed.
func CheckResult(result wire.Result, feedTenant string) error {
	return checkReceiver(result.Wherry.ReceiverBinding, "", feedTenant)
}

// CheckEcho checks a verified echo against the feed it was read from.
func CheckEcho(echo wire.Echo, feedTenant string) error {
	if echo.Tenant != feedTenant {
		return errors.New("echo names another tenant than its feed")
	}
	return nil
}

// CheckCancel checks a verified cancel against the pool whose events it was read from and the tenant of the run it
// names.
func CheckCancel(cancel wire.Cancel, topicPool, runTenant string) error {
	if cancel.Pool != topicPool || cancel.Tenant != runTenant {
		return errors.New("cancel names another pool or tenant than its run")
	}
	return nil
}
