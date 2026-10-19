package admin

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"gopkg.in/yaml.v3"
)

type Service struct {
	Admin           *kgo.Client
	Operator        *kgo.Client
	Broker          string
	CA              string // PEM, copied into bundles
	DeliveryCeiling int32
}

type Bundle struct {
	Broker    string `yaml:"broker"`
	CA        string `yaml:"ca"`
	Principal string `yaml:"principal"`
	Password  string `yaml:"password"`
	// Platform holds platform credentials by adapter name, such as a Nomad
	// token, for platforms that do not issue one by their own identity.
	Platform map[string]string `yaml:"platform,omitempty"`
}

func (service *Service) PoolCreate(ctx context.Context, pool string, terms Terms) error {
	if service.DeliveryCeiling == 0 {
		ceiling, err := service.brokerSetting(ctx, "group.share.max.delivery.count.limit", func(a, b int64) int64 { return min(a, b) })
		if err != nil {
			return err
		}
		service.DeliveryCeiling = int32(ceiling)
	}
	if terms.DeliveryLimit > service.DeliveryCeiling {
		return fmt.Errorf("delivery limit exceeds broker ceiling %d", service.DeliveryCeiling)
	}
	record := Record{Kind: "pool", Pool: pool, Terms: &terms}
	state, err := service.appendChecked(ctx, []byte("pool."+pool), record)
	if err != nil {
		return err
	}
	if err := service.createTopics(ctx, "-1", PoolTasksTopic(pool), PoolEventsTopic(pool)); err != nil {
		return err
	}
	if err := service.configureGroup(ctx, pool, terms); err != nil {
		return err
	}
	return service.reconcileACLs(ctx, state)
}

func (service *Service) ReceiverAdd(ctx context.Context, config ReceiverConfig, platform map[string]string, bundlePath string) (err error) {
	state, err := service.LoadState(ctx)
	if err != nil {
		return err
	}
	terms, ok := state.Pools[config.Pool]
	if !ok {
		return errors.New("receiver names unknown pool")
	}
	if err := ValidateConfig(config, terms); err != nil {
		return err
	}
	bundle, err := createBundle(bundlePath)
	if err != nil {
		return err
	}
	defer closeBundle(bundle, &err)
	record := Record{Kind: "receiver", Pool: config.Pool, Receiver: config.Receiver, Config: &config}
	state, err = service.appendChecked(ctx, []byte(config.Receiver), record)
	if err != nil {
		return err
	}
	password, err := service.upsertUser(ctx, ReceiverPrincipal(config.Receiver))
	if err != nil {
		return err
	}
	if err := service.createTopics(ctx, "-1", ReceiverTopic(config.Receiver)); err != nil {
		return err
	}
	if err := service.reconcileACLs(ctx, state); err != nil {
		return err
	}
	return writeBundle(bundle, Bundle{service.Broker, service.CA, ReceiverPrincipal(config.Receiver), password, platform})
}

func (service *Service) HostAdd(ctx context.Context, tenant, host, bundlePath string) (err error) {
	bundle, err := createBundle(bundlePath)
	if err != nil {
		return err
	}
	defer closeBundle(bundle, &err)
	record := Record{Kind: "host", Tenant: tenant, Host: host}
	state, err := service.appendChecked(ctx, []byte(tenant+"."+host), record)
	if err != nil {
		return err
	}
	principal := TenantPrincipal(tenant, host)
	password, err := service.upsertUser(ctx, principal)
	if err != nil {
		return err
	}
	if err := service.createTopics(ctx, "-1", HostTopic(tenant, host)); err != nil {
		return err
	}
	if err := service.createTopics(ctx, tenantRetentionMS, TenantTopic(tenant)); err != nil {
		return err
	}
	if err := service.reconcileACLs(ctx, state); err != nil {
		return err
	}
	if err := service.applyQuota(ctx, state, tenant, principal); err != nil {
		return err
	}
	return writeBundle(bundle, Bundle{service.Broker, service.CA, principal, password, nil})
}

func (service *Service) TenantAdd(ctx context.Context, tenant, pool string) error {
	record := Record{Kind: "entitlement", Tenant: tenant, Pool: pool}
	state, err := service.appendChecked(ctx, []byte("entitlement."+tenant+"."+pool), record)
	if err != nil {
		return err
	}
	if err := service.createTopics(ctx, tenantRetentionMS, TenantTopic(tenant)); err != nil {
		return err
	}
	if err := service.reconcileACLs(ctx, state); err != nil {
		return err
	}
	for principal, host := range state.Hosts {
		if host.Tenant == tenant {
			if err := service.applyQuota(ctx, state, tenant, principal); err != nil {
				return err
			}
		}
	}
	return nil
}

func (service *Service) Check(ctx context.Context) error {
	state, err := service.LoadState(ctx)
	if err != nil {
		return err
	}
	if err := state.CheckNames(); err != nil {
		return err
	}
	for receiver, config := range state.Receivers {
		if err := ValidateConfig(config, state.Pools[config.Pool]); err != nil {
			return fmt.Errorf("receiver %s: %w", receiver, err)
		}
	}
	return service.verifyACLs(ctx, state)
}

func (service *Service) LoadState(ctx context.Context) (State, error) {
	return Load(ctx, service.Operator)
}

// Skip records the operator's decision to pass over the undecodable record at
// offset, bound to its SHA-256. It writes directly: loading state fails until
// the skip exists.
func (service *Service) Skip(ctx context.Context, offset int64, hash string) error {
	raw, err := readRaw(ctx, service.Operator)
	if err != nil {
		return err
	}
	for _, entry := range raw {
		if entry.Offset != offset {
			continue
		}
		if _, err := DecodeRecord(entry.Value); err == nil {
			return fmt.Errorf("record at offset %d decodes and cannot be skipped", offset)
		}
		sum := sha256.Sum256(entry.Value)
		if hex.EncodeToString(sum[:]) != hash {
			return fmt.Errorf("record at offset %d does not have that SHA-256", offset)
		}
		encoded, err := EncodeRecord(Record{Kind: "skip", Offset: offset, SHA256: hash})
		if err != nil {
			return err
		}
		return service.Operator.ProduceSync(ctx, &kgo.Record{Topic: AdminTopic, Key: []byte("skip." + strconv.FormatInt(offset, 10)), Value: encoded}).FirstErr()
	}
	return fmt.Errorf("wherry.admin has no record at offset %d", offset)
}

// tenantRetentionMS is the tenant feed's default: it carries transcripts,
// which hold anything the agent printed (Ben, 2026-10-19).
const tenantRetentionMS = "2592000000"

// receiversRetentionMS bounds wherry.receivers: readers want only each
// receiver's latest advertisement.
const receiversRetentionMS = "604800000"

func (service *Service) appendChecked(ctx context.Context, key []byte, record Record) (State, error) {
	if err := service.createTopics(ctx, "-1", AdminTopic); err != nil {
		return State{}, err
	}
	if err := service.createTopics(ctx, receiversRetentionMS, ReceiversTopic); err != nil {
		return State{}, err
	}
	state, err := service.LoadState(ctx)
	if err != nil {
		return State{}, err
	}
	encoded, err := EncodeRecord(record)
	if err != nil {
		return State{}, err
	}
	decoded, _ := DecodeRecord(encoded)
	if err := apply(&state, decoded); err != nil {
		return State{}, err
	}
	if err := state.CheckNames(); err != nil {
		return State{}, err
	}
	if err := service.Operator.ProduceSync(ctx, &kgo.Record{Topic: AdminTopic, Key: key, Value: encoded}).FirstErr(); err != nil {
		return State{}, fmt.Errorf("write admin record: %w", err)
	}
	return state, nil
}

func (service *Service) createTopics(ctx context.Context, retentionMS string, topics ...string) error {
	configs := map[string]*string{"cleanup.policy": kadm.StringPtr("delete"), "message.timestamp.type": kadm.StringPtr("LogAppendTime"), "retention.ms": &retentionMS}
	responses, err := kadm.NewClient(service.Admin).CreateTopics(ctx, 1, -1, configs, topics...)
	if err != nil {
		return err
	}
	for _, response := range responses {
		if response.Err != nil && !errors.Is(response.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w", response.Topic, response.Err)
		}
	}
	return nil
}

func (service *Service) configureGroup(ctx context.Context, pool string, terms Terms) error {
	values := map[string]string{"share.auto.offset.reset": "earliest", "share.delivery.count.limit": strconv.Itoa(int(terms.DeliveryLimit)), "share.record.lock.duration.ms": strconv.FormatInt(terms.LockDurationMS, 10), "share.isolation.level": "read_committed"}
	resource := kmsg.NewIncrementalAlterConfigsRequestResource()
	resource.ResourceType = kmsg.ConfigResourceTypeGroupConfig
	resource.ResourceName = PoolGroup(pool)
	for name, value := range values {
		config := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
		config.Name = name
		config.Value = &value
		config.Op = kmsg.IncrementalAlterConfigOpSet
		resource.Configs = append(resource.Configs, config)
	}
	request := kmsg.NewPtrIncrementalAlterConfigsRequest()
	request.Resources = []kmsg.IncrementalAlterConfigsRequestResource{resource}
	response, err := service.Admin.Request(ctx, request)
	if err != nil {
		return err
	}
	result := response.(*kmsg.IncrementalAlterConfigsResponse).Resources[0]
	if result.ErrorCode != 0 {
		return fmt.Errorf("configure share group: %w", kerr.ErrorForCode(result.ErrorCode))
	}
	return nil
}

func (service *Service) upsertUser(ctx context.Context, user string) (string, error) {
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		return "", err
	}
	password := base64.RawURLEncoding.EncodeToString(random)
	results, err := kadm.NewClient(service.Admin).AlterUserSCRAMs(ctx, nil, []kadm.UpsertSCRAM{{User: user, Mechanism: kadm.ScramSha512, Iterations: 8192, Password: password}})
	if err != nil {
		return "", err
	}
	if err := results.Error(); err != nil {
		return "", fmt.Errorf("create SCRAM credential: %w", err)
	}
	return password, nil
}

func (service *Service) applyQuota(ctx context.Context, state State, tenant, principal string) error {
	var rate int64
	var request float64
	for pool := range state.Entitlements[tenant] {
		terms := state.Pools[pool]
		if rate == 0 || terms.ProducerByteRate < rate {
			rate = terms.ProducerByteRate
		}
		if request == 0 || terms.RequestPercentage < request {
			request = terms.RequestPercentage
		}
	}
	if rate == 0 {
		return nil
	}
	results, err := kadm.NewClient(service.Admin).AlterClientQuotas(ctx, []kadm.AlterClientQuotaEntry{{Entity: kadm.ClientQuotaEntity{{Type: "user", Name: &principal}}, Ops: []kadm.AlterClientQuotaOp{{Key: "producer_byte_rate", Value: float64(rate)}, {Key: "request_percentage", Value: request}}}})
	if err != nil {
		return err
	}
	for _, result := range results {
		if result.Err != nil {
			return fmt.Errorf("alter client quota: %w", result.Err)
		}
	}
	return nil
}

// brokerSetting reads a numeric setting from every broker, combining values with pick.
func (service *Service) brokerSetting(ctx context.Context, name string, pick func(a, b int64) int64) (int64, error) {
	client := kadm.NewClient(service.Admin)
	brokers, err := client.ListBrokers(ctx)
	if err != nil {
		return 0, err
	}
	resources, err := client.DescribeBrokerConfigs(ctx, brokers.NodeIDs()...)
	if err != nil {
		return 0, err
	}
	var value int64
	found := 0
	for _, resource := range resources {
		if resource.Err != nil {
			return 0, resource.Err
		}
		for _, config := range resource.Configs {
			if config.Key != name || config.Value == nil {
				continue
			}
			parsed, err := strconv.ParseInt(*config.Value, 10, 64)
			if err != nil {
				return 0, err
			}
			if found == 0 {
				value = parsed
			} else {
				value = pick(value, parsed)
			}
			found++
		}
	}
	if found != len(brokers) {
		return 0, fmt.Errorf("broker setting %s is not readable on every broker", name)
	}
	return value, nil
}

func createBundle(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
}

// closeBundle removes a bundle whose command failed, so a retry can create it.
func closeBundle(file *os.File, err *error) {
	file.Close()
	if *err != nil {
		os.Remove(file.Name())
	}
}

func writeBundle(file *os.File, bundle Bundle) error {
	encoded, err := yaml.Marshal(bundle)
	if err != nil {
		return err
	}
	_, err = file.Write(encoded)
	return err
}
