package admin

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var ErrAuthenticationFailure = errors.New("authentication failure")
var ErrAuthorizationDenied = errors.New("authorisation denied")
var ErrStillAuthorised = errors.New("broker still authorises the principal")

// ACL deletion reaches each broker on its own schedule, so step 3 retries a broker that still authorises.
var probeWindow, probeInterval = 2 * time.Minute, time.Second

type RevocationStep struct {
	Number   int
	Name     string
	Complete bool
	Err      error
}
type RevocationResult struct {
	Steps    []RevocationStep
	Complete bool
}

func (service *Service) RevokeReceiver(ctx context.Context, receiver string, wait time.Duration, probe func(context.Context, string, string) error) RevocationResult {
	return service.revoke(ctx, Record{Kind: "end", Receiver: receiver}, ReceiverPrincipal(receiver), wait, probe)
}

func (service *Service) RevokeHost(ctx context.Context, tenant, host string, wait time.Duration, probe func(context.Context, string, string) error) RevocationResult {
	return service.revoke(ctx, Record{Kind: "end", Tenant: tenant, Host: host}, TenantPrincipal(tenant, host), wait, probe)
}

func (service *Service) revoke(ctx context.Context, end Record, principal string, wait time.Duration, probe func(context.Context, string, string) error) RevocationResult {
	result := RevocationResult{}
	run := func(number int, name string, fn func() error) bool {
		err := fn()
		result.Steps = append(result.Steps, RevocationStep{number, name, err == nil, err})
		return err == nil
	}
	var password string
	if !run(1, "rotate SCRAM password", func() error { var err error; password, err = service.upsertUser(ctx, principal); return err }) {
		return result
	}
	if !run(2, "delete ACLs", func() error { return service.deletePrincipalACLs(ctx, principal) }) {
		return result
	}
	if !run(3, "probe every broker", func() error {
		if probe == nil {
			return errors.New("probe is not configured")
		}
		deadline := time.Now().Add(probeWindow)
		for {
			err := probe(ctx, principal, password)
			if errors.Is(err, ErrAuthorizationDenied) {
				return nil
			}
			if !errors.Is(err, ErrStillAuthorised) || time.Now().After(deadline) {
				return fmt.Errorf("probe incomplete: %w", err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(probeInterval):
			}
		}
	}) {
		return result
	}
	if !run(4, "delete SCRAM credential", func() error {
		altered, err := kadm.NewClient(service.Admin).AlterUserSCRAMs(ctx, []kadm.DeleteSCRAM{{User: principal, Mechanism: kadm.ScramSha512}}, nil)
		if err != nil {
			return err
		}
		return altered.Error()
	}) {
		return result
	}
	if !run(5, "wait revocation intervals", func() error {
		if wait <= 0 {
			longest := func(a, b int64) int64 { return max(a, b) }
			for _, name := range []string{"connections.max.reauth.ms", "group.share.max.record.lock.duration.ms", "transaction.max.timeout.ms"} {
				ms, err := service.brokerSetting(ctx, name, longest)
				if err != nil {
					return err
				}
				if ms <= 0 {
					return fmt.Errorf("%s is unbounded, so revocation cannot complete", name)
				}
				wait = max(wait, time.Duration(ms)*time.Millisecond)
			}
		}
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	}) {
		return result
	}
	if !run(6, "record membership end", func() error { _, err := service.appendChecked(ctx, []byte("end."+principal), end); return err }) {
		return result
	}
	result.Complete = true
	return result
}

// Probe asks every broker, as the revoked principal, for metadata on topic:
// denial is TOPIC_AUTHORIZATION_FAILED, a failed login is
// SASL_AUTHENTICATION_FAILED.
func (service *Service) Probe(topic string) func(context.Context, string, string) error {
	return func(ctx context.Context, user, password string) error {
		brokers, err := kadm.NewClient(service.Admin).ListBrokers(ctx)
		if err != nil {
			return err
		}
		opts, err := KafkaOpts(service.Broker, []byte(service.CA), user, password)
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
				return ErrAuthenticationFailure
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
