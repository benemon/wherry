package operator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/benemon/wherry/internal/telemetry"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// RevokeHostSCRAM gives a live host a random password and a new enrolment, writes its kept bundle to bundlePath, and
// returns connections.max.reauth.ms, the longest a session opened with an old password during its wait can last.
func (service *Service) RevokeHostSCRAM(ctx context.Context, tenant, host, bundlePath string) (_ time.Duration, err error) {
	ctx, span := service.marker(ctx, "tenant host reset", telemetry.AttrTenant.String(tenant), telemetry.AttrHost.String(host))
	defer ended(span, &err)
	state, err := service.LoadState(ctx)
	if err == nil {
		err = state.LiveTenant(tenant)
	}
	if err != nil {
		return 0, err
	}
	principal := TenantPrincipal(tenant, host)
	if _, ok := state.Hosts[principal]; !ok || state.Ended[principal] || state.Revoking[principal] {
		return 0, errors.New("no live host by that name")
	}
	ms, err := service.brokerSetting(ctx, "connections.max.reauth.ms", func(a, b int64) int64 { return max(a, b) })
	if err != nil {
		return 0, err
	}
	if ms <= 0 {
		return 0, errors.New("connections.max.reauth.ms is unbounded, so revoke cannot complete")
	}
	wait := time.Duration(ms) * time.Millisecond
	if state, err = service.appendChecked(ctx, []byte("resetting."+principal), Record{Kind: "resetting", Tenant: tenant, Host: host}); err != nil {
		return 0, err
	}
	// The quota comes first so that a failure leaves no working bundle for an unquoted host.
	if err := service.applyQuota(ctx, state, tenant, principal); err != nil {
		return 0, err
	}
	password, err := service.upsertUser(ctx, principal)
	if err != nil {
		return 0, err
	}
	if err := service.WaitCredential(ctx, principal, password, ProbeWindow); err != nil {
		return 0, fmt.Errorf("probe incomplete: %w", err)
	}
	RevokeProbed()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-timer.C:
	}
	if err := service.loginEveryBroker(ctx, principal, password); errors.Is(err, kerr.SaslAuthenticationFailed) {
		return 0, errors.New("the password revoke set is refused after its wait: an upsert from an earlier command was applied after it, and an old password may work; run revoke again")
	} else if err != nil {
		return 0, fmt.Errorf("probe incomplete: %w", err)
	}
	enrolment, err := CredentialID()
	if err != nil {
		return 0, err
	}
	if _, err := service.appendChecked(ctx, []byte("enrolled."+principal), Record{Kind: "enrolled", Tenant: tenant, Host: host, Enrolment: enrolment}); err != nil {
		return 0, err
	}
	storage, err := service.tenantStorage(tenant)
	if err != nil {
		return 0, err
	}
	return wait, ReplaceBundle(bundlePath, service.hostBundle(principal, password, enrolment, storage))
}

// RevokeProbed runs once revoke's first probe has passed on every listed broker, before its wait; tests in other
// packages replace it.
var RevokeProbed = func() {}

// loginEveryBroker logs in once as principal with password on every broker the metadata lists.
func (service *Service) loginEveryBroker(ctx context.Context, principal, password string) error {
	brokers, err := kadm.NewClient(service.Operator).ListBrokers(ctx)
	if err != nil {
		return fmt.Errorf("list brokers: %w", err)
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
	for _, id := range brokers.NodeIDs() {
		if _, err := client.Broker(int(id)).Request(ctx, request); err != nil {
			return fmt.Errorf("broker %d as %s: %w", id, principal, err)
		}
	}
	return nil
}
