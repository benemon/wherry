package operator

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/benemon/wherry/internal/telemetry"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var ErrAuthorizationDenied = errors.New("authorisation denied")
var ErrStillAuthorised = errors.New("broker still authorises the principal")

// ACL deletion and a SCRAM credential reach each broker on its own schedule, so a probe retries within ProbeWindow.
var ProbeWindow, probeInterval = 2 * time.Minute, time.Second

// errLoginRefused is a probe whose login a broker refused.
var errLoginRefused = errors.New("authentication failure")

type RevocationStep struct {
	Number   int
	Name     string
	Complete bool
	Err      error
	// Until is set only on the step 5 a RevocationProgress is told as it starts: the time its wait ends.
	Until time.Time
}
type RevocationResult struct {
	Steps    []RevocationStep
	Complete bool
}

// RevocationProgress is told each revocation step as it ends, and step 5 as it starts, with Until set and Complete
// false, since its wait is usually the longest. The ones it is told as they end are the result's steps.
type RevocationProgress func(RevocationStep)

// RevokeReceiver runs the revocation steps for receiver, progress nil allowed, and reports a receiver whose membership
// already ended converged and complete, as a host is, since its earlier revocation served its wait.
func (service *Service) RevokeReceiver(ctx context.Context, receiver string, wait time.Duration, probe func(context.Context, string, string) error, progress RevocationProgress) (result RevocationResult) {
	ctx, span := service.marker(ctx, "receiver remove", telemetry.AttrReceiver.String(receiver))
	defer revoked(span, &result)
	state, err := service.LoadState(ctx)
	if err != nil {
		return reported(progress, RevocationResult{Steps: []RevocationStep{{Number: 1, Err: err}}})
	}
	if state.Ended[ReceiverPrincipal(receiver)] {
		return reported(progress, service.alreadyEnded(ctx))
	}
	return service.revoke(ctx, Record{Kind: "end", Receiver: receiver}, ReceiverPrincipal(receiver), wait, probe, progress)
}

// RevokeHost runs the revocation steps for tenant's host; progress may be nil.
func (service *Service) RevokeHost(ctx context.Context, tenant, host string, wait time.Duration, probe func(context.Context, string, string) error, progress RevocationProgress) (result RevocationResult) {
	ctx, span := service.marker(ctx, "tenant host remove", telemetry.AttrTenant.String(tenant), telemetry.AttrHost.String(host))
	defer revoked(span, &result)
	state, err := service.LoadState(ctx)
	if err == nil {
		err = state.LiveTenant(tenant)
	}
	if err != nil {
		return reported(progress, RevocationResult{Steps: []RevocationStep{{Number: 1, Err: err}}})
	}
	if state.Ended[TenantPrincipal(tenant, host)] {
		return reported(progress, service.alreadyEnded(ctx))
	}
	if _, exists := state.Hosts[TenantPrincipal(tenant, host)]; !exists {
		return reported(progress, RevocationResult{Steps: []RevocationStep{{Number: 1, Err: errors.New("unknown host")}}})
	}
	return service.revoke(ctx, Record{Kind: "end", Tenant: tenant, Host: host}, TenantPrincipal(tenant, host), wait, probe, progress)
}

// alreadyEnded converges Kafka for a principal whose membership has ended and reports that as the one step of a complete
// revocation; a failed convergence is the step's error.
func (service *Service) alreadyEnded(ctx context.Context) RevocationResult {
	err := service.converge(ctx)
	return RevocationResult{Complete: err == nil, Steps: []RevocationStep{{Number: 6, Name: "already ended", Complete: err == nil, Err: err}}}
}

// reported tells progress, when set, the steps of a result decided before any step ran, as the run ones are told.
func reported(progress RevocationProgress, result RevocationResult) RevocationResult {
	if progress != nil {
		for _, step := range result.Steps {
			progress(step)
		}
	}
	return result
}

func (service *Service) revoke(ctx context.Context, end Record, principal string, wait time.Duration, probe func(context.Context, string, string) error, progress RevocationProgress) RevocationResult {
	r := &revocation{service: service, end: end, principal: principal, wait: wait, probe: probe, progress: progress}
	return r.steps(ctx, 1, 6)
}

type revocation struct {
	service   *Service
	end       Record
	principal string
	wait      time.Duration
	probe     func(context.Context, string, string) error
	progress  RevocationProgress
	password  string
	recorded  bool
	result    RevocationResult
}

func (r *revocation) steps(ctx context.Context, first, last int) RevocationResult {
	service, end, principal, probe := r.service, r.end, r.principal, r.probe
	run := func(number int, name string, fn func() error) bool {
		if number < first || number > last {
			return true
		}
		_, span := telemetry.Child(ctx, "revocation step "+strconv.Itoa(number))
		err := fn()
		// Once revoking is recorded, a failed step converges: a stale concurrent command may have restored the
		// principal's grants after step 2. Step 6 converges itself.
		if err != nil && r.recorded && number < 6 {
			err = errors.Join(err, service.converge(ctx))
		}
		ended(span, &err)
		step := RevocationStep{Number: number, Name: name, Complete: err == nil, Err: err}
		r.result.Steps = append(r.result.Steps, step)
		if r.progress != nil {
			r.progress(step)
		}
		return err == nil
	}
	if !run(1, "record revoking, rotate SCRAM password", func() error {
		// A connection, share lock or transaction from before this step can outlive a shorter wait; refuse one before
		// changing anything.
		longest := func(a, b int64) int64 { return max(a, b) }
		var minimum time.Duration
		for _, name := range []string{"connections.max.reauth.ms", "group.share.max.record.lock.duration.ms", "transaction.max.timeout.ms"} {
			ms, err := service.brokerSetting(ctx, name, longest)
			if err != nil {
				return err
			}
			if ms <= 0 {
				return fmt.Errorf("%s is unbounded, so revocation cannot complete", name)
			}
			minimum = max(minimum, time.Duration(ms)*time.Millisecond)
		}
		if r.wait > 0 && r.wait < minimum {
			return fmt.Errorf("wait %s is below the broker-derived minimum %s", r.wait, minimum)
		}
		r.wait = max(r.wait, minimum)
		state, err := service.LoadState(ctx)
		if err != nil {
			return err
		}
		// A re-run continues from the revoking record of the run that failed.
		if !state.Revoking[principal] {
			revoking := end
			revoking.Kind = "revoking"
			if _, err := service.appendChecked(ctx, []byte("revoking."+principal), revoking); err != nil {
				return err
			}
		}
		r.recorded = true
		r.password, err = service.upsertUser(ctx, principal)
		return err
	}) {
		return r.result
	}
	if !run(2, "delete ACLs", func() error { return service.deletePrincipalACLs(ctx, principal) }) {
		return r.result
	}
	if !run(3, "probe every broker", func() error {
		if probe == nil {
			return errors.New("probe is not configured")
		}
		deadline := time.Now().Add(ProbeWindow)
		for {
			err := probe(ctx, principal, r.password)
			if errors.Is(err, ErrAuthorizationDenied) {
				return nil
			}
			// A broker that has not yet applied the rotated SCRAM credential refuses its password, as one that has not
			// yet dropped the ACLs still authorises it; both settle within the window.
			if !errors.Is(err, ErrStillAuthorised) && !errors.Is(err, errLoginRefused) || time.Now().After(deadline) {
				return fmt.Errorf("probe incomplete: %w", err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(probeInterval):
			}
		}
	}) {
		return r.result
	}
	if !run(4, "delete SCRAM credential", func() error {
		altered, err := kadm.NewClient(service.Operator).AlterUserSCRAMs(ctx, []kadm.DeleteSCRAM{{User: principal, Mechanism: kadm.ScramSha512}}, nil)
		if err != nil {
			return err
		}
		return altered.Error()
	}) {
		return r.result
	}
	if !run(5, "wait revocation intervals", func() error {
		if r.progress != nil {
			r.progress(RevocationStep{Number: 5, Name: "wait", Until: time.Now().Add(r.wait)})
		}
		timer := time.NewTimer(r.wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	}) {
		return r.result
	}
	// A stale concurrent command can restore a grant or credential after steps 2 and 4; the pass after the end record
	// deletes it again.
	if !run(6, "record membership end", func() error {
		_, err := service.appendChecked(ctx, []byte("end."+principal), end)
		return errors.Join(err, service.converge(ctx))
	}) {
		return r.result
	}
	r.result.Complete = last == 6
	return r.result
}

// Probe asks every broker, as the revoked principal, for metadata on topic:
// denial is TOPIC_AUTHORIZATION_FAILED, a failed login is
// SASL_AUTHENTICATION_FAILED.
func (service *Service) Probe(topic string) func(context.Context, string, string) error {
	return func(ctx context.Context, user, password string) error {
		brokers, err := kadm.NewClient(service.Operator).ListBrokers(ctx)
		if err != nil {
			return err
		}
		opts, err := KafkaOpts(service.Broker, user, password, service.Roots)
		if err != nil {
			return fmt.Errorf("service %w", err)
		}
		client, err := kgo.NewClient(opts...)
		if err != nil {
			return err
		}
		defer client.Close()
		for _, id := range brokers.NodeIDs() {
			request := kmsg.NewPtrMetadataRequest()
			requestTopic := kmsg.NewMetadataRequestTopic()
			requestTopic.Topic = kmsg.StringPtr(topic)
			request.Topics = append(request.Topics, requestTopic)
			response, err := client.Broker(int(id)).Request(ctx, request)
			if errors.Is(err, kerr.SaslAuthenticationFailed) {
				return fmt.Errorf("broker %d: %w", id, errLoginRefused)
			}
			if err != nil {
				return err
			}
			topics := response.(*kmsg.MetadataResponse).Topics
			if len(topics) != 1 || topics[0].ErrorCode != kerr.TopicAuthorizationFailed.Code {
				return fmt.Errorf("broker %d: %w", id, ErrStillAuthorised)
			}
		}
		return ErrAuthorizationDenied
	}
}

// WaitOperatorCredential waits, within topicWait, until every listed broker accepts wherry.operator's login with password.
func (service *Service) WaitOperatorCredential(ctx context.Context, password string) error {
	return service.WaitCredential(ctx, OperatorPrincipal, password, topicWait)
}

// WaitCredential waits, within window, until every broker service.Operator lists accepts principal's login with
// password. Brokers apply a SCRAM credential in turn, and a login refused before then succeeds after.
func (service *Service) WaitCredential(ctx context.Context, principal, password string, window time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, window)
	defer cancel()
	// A dial past the deadline fails with an i/o timeout before waitCtx's timer fires and sets waitCtx.Err(), so the
	// allowance is spent once the clock reaches the deadline, not once waitCtx.Err() says so.
	deadline, _ := waitCtx.Deadline()
	brokers, err := kadm.NewClient(service.Operator).ListBrokers(waitCtx)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		return fmt.Errorf("list brokers: %w", err)
	}
	ids := brokers.NodeIDs()
	if len(ids) == 0 {
		return errors.New("list brokers: none listed")
	}
	opts, err := KafkaOpts(service.Broker, principal, password, service.Roots)
	if err != nil {
		return err
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	defer client.Close()
	request := kmsg.NewPtrMetadataRequest()
	request.Topics = []kmsg.MetadataRequestTopic{}
	for _, id := range ids {
		var refused error
		for {
			_, err := client.Broker(int(id)).Request(waitCtx, request)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err == nil {
				break
			}
			if errors.Is(err, kerr.SaslAuthenticationFailed) {
				refused = err
			} else if refused == nil || time.Now().Before(deadline) {
				return fmt.Errorf("broker %d as %s: %w", id, principal, err)
			}
			select {
			case <-waitCtx.Done():
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("broker %d as %s: %w", id, principal, refused)
			case <-time.After(listBackoff):
			}
		}
	}
	return ctx.Err()
}

func (service *Service) TenantRemove(ctx context.Context, tenant string) (err error) {
	ctx, span := service.marker(ctx, "tenant remove", telemetry.AttrTenant.String(tenant))
	defer ended(span, &err)
	return service.removeTenant(ctx, tenant, service.Probe)
}

func (service *Service) removeTenant(ctx context.Context, tenant string, probe func(string) func(context.Context, string, string) error) error {
	state, err := service.LoadState(ctx)
	if err != nil {
		return err
	}
	ended, exists := state.Tenants[tenant]
	if !exists {
		return errors.New("tenant absent; use add --tenant first")
	}
	if ended {
		return service.converge(ctx)
	}
	for _, pool := range slices.Sorted(maps.Keys(state.Entitlements[tenant])) {
		if err := service.TenantPoolRemove(ctx, tenant, pool); err != nil {
			return err
		}
	}
	state, err = service.LoadState(ctx)
	if err != nil {
		return err
	}
	if err := service.tenantQuotas(ctx, state, tenant); err != nil {
		return err
	}
	var revocations []*revocation
	for _, principal := range slices.Sorted(maps.Keys(state.Hosts)) {
		host := state.Hosts[principal]
		if host.Tenant != tenant || state.Ended[principal] {
			continue
		}
		r := &revocation{service: service, principal: principal, end: Record{Kind: "end", Tenant: tenant, Host: host.Name}, probe: probe(HostTopic(tenant, host.Name))}
		if result := r.steps(ctx, 1, 1); len(result.Steps) != 1 || !result.Steps[0].Complete {
			return revocationError(result)
		}
		revocations = append(revocations, r)
	}
	var group sync.WaitGroup
	for _, r := range revocations {
		group.Go(func() { r.steps(ctx, 2, 5) })
	}
	group.Wait()
	for _, r := range revocations {
		if len(r.result.Steps) != 5 || !r.result.Steps[4].Complete {
			return revocationError(r.result)
		}
	}
	for _, r := range revocations {
		if result := r.steps(ctx, 6, 6); !result.Complete {
			return revocationError(result)
		}
	}
	if _, err := service.appendChecked(ctx, []byte("end.tenant."+tenant), Record{Kind: "end", Tenant: tenant}); err != nil {
		return err
	}
	return service.converge(ctx)
}

func revocationError(result RevocationResult) error {
	for _, step := range result.Steps {
		if step.Err != nil {
			return step.Err
		}
	}
	return errors.New("revocation incomplete")
}
