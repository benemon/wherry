package admin

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/scram"
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
}

// DecodeYAML decodes encoded into value, refusing unknown fields. An error names only the lines it found, since the
// decoder quotes the text and a credential pasted in the wrong place is still a credential.
func DecodeYAML(encoded []byte, value any) error {
	decoder := yaml.NewDecoder(bytes.NewReader(encoded))
	decoder.KnownFields(true)
	if err := decoder.Decode(value); err != nil {
		lines := strings.Join(yamlLine.FindAllString(err.Error(), -1), ", ")
		if lines == "" {
			return errors.New("not valid YAML")
		}
		return fmt.Errorf("invalid or unknown field at %s", lines)
	}
	return nil
}

// ReadBundle reads the bundle at path, refusing unknown fields and a bundle without its broker, principal or password.
func ReadBundle(path string) (Bundle, error) {
	encoded, err := os.ReadFile(path)
	if err != nil {
		return Bundle{}, fmt.Errorf("read bundle: %w", err)
	}
	var bundle Bundle
	if err := DecodeYAML(encoded, &bundle); err != nil {
		return Bundle{}, fmt.Errorf("decode bundle: %w", err)
	}
	if bundle.Broker == "" || bundle.Principal == "" || bundle.Password == "" {
		return Bundle{}, errors.New("bundle requires broker, principal and password")
	}
	return bundle, nil
}

var yamlLine = regexp.MustCompile(`line \d+`)

// KafkaOpts are the options of a TLS and SCRAM-SHA-512 connection to broker as user, trusting the PEM certificates in
// ca. A trailing line break, as a password file ends with, is not part of password, and nothing else is no password.
func KafkaOpts(broker string, ca []byte, user, password string) ([]kgo.Opt, error) {
	password = strings.TrimRight(password, "\r\n")
	if password == "" {
		return nil, errors.New("password is empty")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("CA contains no certificates")
	}
	return []kgo.Opt{
		kgo.SeedBrokers(broker),
		kgo.DialTLSConfig(&tls.Config{RootCAs: roots}),
		kgo.SASL(scram.Auth{User: user, Pass: password}.AsSha512Mechanism()),
		kgo.DisableClientMetrics(),
	}, nil
}

func (service *Service) PoolCreate(ctx context.Context, pool string, terms Terms) error {
	effective := withDefaults(terms)
	if err := validateTerms(effective); err != nil {
		return err
	}
	retention, err := poolRetention(effective)
	if err != nil {
		return err
	}
	if service.DeliveryCeiling == 0 {
		ceiling, err := service.brokerSetting(ctx, "group.share.max.delivery.count.limit", func(a, b int64) int64 { return min(a, b) })
		if err != nil {
			return err
		}
		service.DeliveryCeiling = int32(ceiling)
	}
	if effective.DeliveryLimit > service.DeliveryCeiling {
		return fmt.Errorf("delivery limit exceeds broker ceiling %d", service.DeliveryCeiling)
	}
	if err := service.createTopics(ctx, "-1", AdminTopic); err != nil {
		return err
	}
	current, err := service.LoadState(ctx)
	if err != nil {
		return err
	}
	if _, exists := current.Pools[pool]; exists {
		if err := strandedReceivers(current, pool, terms); err != nil {
			return err
		}
	}
	if terms.RecordLimit, err = service.checkLimits(ctx, current, pool); err != nil {
		return err
	}
	if err := service.checkPoolFeedsRetention(ctx, current, pool, retention.feed); err != nil {
		return err
	}
	record := Record{Kind: "pool", Pool: pool, Terms: &terms}
	state, err := service.appendChecked(ctx, []byte("pool."+pool), record)
	if err != nil {
		return err
	}
	value := strconv.FormatInt(retention.tasks, 10)
	if err := service.createTopics(ctx, value, PoolTasksTopic(pool), PoolEventsTopic(pool)); err != nil {
		return err
	}
	if err := service.configureTopicRetention(ctx, value, PoolTasksTopic(pool), PoolEventsTopic(pool)); err != nil {
		return err
	}
	if err := service.configureGroup(ctx, pool, effective); err != nil {
		return err
	}
	return service.reconcileACLs(ctx, state)
}

// strandedReceivers refuses terms for pool under which a live receiver's recorded configuration fails ValidateConfig,
// naming each receiver and profile that fails and why.
func strandedReceivers(state State, pool string, terms Terms) error {
	var failures []error
	for _, receiver := range slices.Sorted(maps.Keys(state.Receivers)) {
		config := state.Receivers[receiver]
		if config.Pool != pool || state.Ended[ReceiverPrincipal(receiver)] {
			continue
		}
		for _, profile := range config.Profiles {
			single := config
			single.Profiles = []ProfileSettings{profile}
			if err := ValidateConfig(single, terms); err != nil {
				failures = append(failures, fmt.Errorf("receiver %s plugin %s profile %s: %w", receiver, profile.Plugin, profile.Name, err))
			}
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("pool terms would strand live receivers: %w", errors.Join(failures...))
	}
	return nil
}

// ReceiverCheck makes ReceiverAdd's checks of config and returns it as the record would hold it, writing nothing.
func (service *Service) ReceiverCheck(ctx context.Context, config ReceiverConfig) (ReceiverConfig, error) {
	state, err := service.LoadState(ctx)
	if err != nil {
		return ReceiverConfig{}, err
	}
	if _, exists := state.Receivers[config.Receiver]; exists {
		return ReceiverConfig{}, errors.New("receiver exists; change its configuration with receiver configure")
	}
	terms, ok := state.Pools[config.Pool]
	if !ok {
		return ReceiverConfig{}, errors.New("receiver names unknown pool")
	}
	if err := ValidateConfig(config, terms); err != nil {
		return ReceiverConfig{}, err
	}
	return asRecorded(Record{Kind: "receiver", Pool: config.Pool, Receiver: config.Receiver, Config: &config})
}

func (service *Service) ReceiverAdd(ctx context.Context, config ReceiverConfig, bundlePath string) (err error) {
	state, err := service.LoadState(ctx)
	if err != nil {
		return err
	}
	if _, exists := state.Receivers[config.Receiver]; exists {
		return errors.New("receiver exists; change its configuration with receiver configure")
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
	return writeBundle(bundle, Bundle{service.Broker, service.CA, ReceiverPrincipal(config.Receiver), password})
}

// ReceiverConfigure records a new configuration for an existing receiver in its
// pool and returns it as recorded. The principal, its password and the bundle
// are untouched; the receiver exits on the new record and its platform restarts
// it. With dryRun it writes nothing and returns the configuration as the record
// would hold it.
func (service *Service) ReceiverConfigure(ctx context.Context, config ReceiverConfig, dryRun bool) (ReceiverConfig, error) {
	state, err := service.LoadState(ctx)
	if err != nil {
		return ReceiverConfig{}, err
	}
	current, ok := state.Receivers[config.Receiver]
	if !ok || state.Ended[ReceiverPrincipal(config.Receiver)] {
		return ReceiverConfig{}, errors.New("no live receiver by that name")
	}
	config.Pool = current.Pool
	if err := ValidateConfig(config, state.Pools[config.Pool]); err != nil {
		return ReceiverConfig{}, err
	}
	record := Record{Kind: "receiver", Pool: config.Pool, Receiver: config.Receiver, Config: &config}
	if !dryRun {
		state, err = service.appendChecked(ctx, []byte(config.Receiver), record)
		return state.Receivers[config.Receiver], err
	}
	return asRecorded(record)
}

// asRecorded is record's configuration as a reader of the encoded record would decode it.
func asRecorded(record Record) (ReceiverConfig, error) {
	encoded, err := EncodeRecord(record)
	if err != nil {
		return ReceiverConfig{}, err
	}
	decoded, err := DecodeRecord(encoded)
	if err != nil {
		return ReceiverConfig{}, err
	}
	return *decoded.Config, nil
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
	if err := service.createTopics(ctx, strconv.Itoa(tenantRetentionMS), TenantTopic(tenant)); err != nil {
		return err
	}
	if err := service.reconcileACLs(ctx, state); err != nil {
		return err
	}
	if err := service.applyQuota(ctx, state, tenant, principal); err != nil {
		return err
	}
	return writeBundle(bundle, Bundle{service.Broker, service.CA, principal, password})
}

func (service *Service) TenantAdd(ctx context.Context, tenant, pool string) error {
	current, err := service.LoadState(ctx)
	if err != nil {
		return err
	}
	terms, ok := current.Pools[pool]
	if !ok {
		return errors.New("entitlement names unknown pool")
	}
	// The pool's records must fit the tenant's feed, so it exists before the entitlement does.
	if err := service.createTopics(ctx, strconv.Itoa(tenantRetentionMS), TenantTopic(tenant)); err != nil {
		return err
	}
	if current.Entitlements[tenant] == nil {
		current.Entitlements[tenant] = map[string]bool{}
	}
	current.Entitlements[tenant][pool] = true
	if _, err := service.checkLimits(ctx, current, pool); err != nil {
		return err
	}
	retention, err := poolRetention(terms)
	if err != nil {
		return err
	}
	if err := service.checkPoolFeedsRetention(ctx, current, pool, retention.feed); err != nil {
		return err
	}
	record := Record{Kind: "entitlement", Tenant: tenant, Pool: pool}
	state, err := service.appendChecked(ctx, []byte("entitlement."+tenant+"."+pool), record)
	if err != nil {
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
	for pool := range state.Pools {
		if _, err := service.checkLimits(ctx, state, pool); err != nil {
			return fmt.Errorf("pool %s: %w", pool, err)
		}
	}
	if err := service.checkRetention(ctx, state); err != nil {
		return err
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
	reader := NewReader(service.Operator)
	defer reader.Close()
	raw, err := readRaw(ctx, reader)
	if err != nil {
		return err
	}
	for _, entry := range raw {
		if entry.Offset != offset {
			continue
		}
		if _, err := DecodeRecord(entry.Value); err == nil {
			return fmt.Errorf("record at offset %d decodes and cannot be skipped", offset)
		} else if errors.Is(err, json.ErrUnknownName) {
			return fmt.Errorf("%w at offset %d; upgrade this wherry instead of skipping it", ErrNewerRecord, offset)
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

// tenantRetentionMS is the tenant feed's default: it carries transcripts, which
// hold anything the agent printed.
const tenantRetentionMS = 30 * 24 * 60 * 60 * 1000

// receiversRetentionMS bounds wherry.receivers: readers want only each
// receiver's latest advertisement.
const receiversRetentionMS = "604800000"

const retentionMarginMS = 24 * 60 * 60 * 1000

type retentionBounds struct {
	tasks int64
	feed  int64
}

// poolRetention derives the pool topics' and its tenant feeds' minimum retention.ms from terms with defaults applied.
func poolRetention(terms Terms) (retentionBounds, error) {
	if terms.MaxQueueDeadlineMS > tenantRetentionMS-MaxRunDurationMS {
		return retentionBounds{}, fmt.Errorf("max queue deadline requires tenant feed retention.ms of at least %d", terms.MaxQueueDeadlineMS+MaxRunDurationMS)
	}
	feed := int64(terms.MaxQueueDeadlineMS) + MaxRunDurationMS
	return retentionBounds{tasks: feed + retentionMarginMS, feed: feed}, nil
}

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

func (service *Service) configureTopicRetention(ctx context.Context, value string, topics ...string) error {
	request := kmsg.NewPtrIncrementalAlterConfigsRequest()
	for _, topic := range topics {
		resource := kmsg.NewIncrementalAlterConfigsRequestResource()
		resource.ResourceType = kmsg.ConfigResourceTypeTopic
		resource.ResourceName = topic
		// Admission measures a run's age from the broker's timestamp of its submission.
		for name, value := range map[string]string{"retention.ms": value, "message.timestamp.type": "LogAppendTime"} {
			config := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
			config.Name = name
			config.Value = &value
			config.Op = kmsg.IncrementalAlterConfigOpSet
			resource.Configs = append(resource.Configs, config)
		}
		request.Resources = append(request.Resources, resource)
	}
	response, err := service.Admin.Request(ctx, request)
	if err != nil {
		return err
	}
	for _, resource := range response.(*kmsg.IncrementalAlterConfigsResponse).Resources {
		if resource.ErrorCode != 0 {
			return fmt.Errorf("configure topic %s retention: %w", resource.ResourceName, kerr.ErrorForCode(resource.ErrorCode))
		}
	}
	return nil
}

func (service *Service) topicRetentions(ctx context.Context, topics ...string) (map[string]int64, error) {
	values, err := service.topicConfigs(ctx, "retention.ms", topics...)
	if err != nil {
		return nil, err
	}
	retentions := map[string]int64{}
	for topic, value := range values {
		if retentions[topic], err = strconv.ParseInt(value, 10, 64); err != nil {
			return nil, fmt.Errorf("topic %s retention.ms: %w", topic, err)
		}
	}
	return retentions, nil
}

func (service *Service) topicConfigs(ctx context.Context, name string, topics ...string) (map[string]string, error) {
	resources, err := kadm.NewClient(service.Admin).DescribeTopicConfigs(ctx, topics...)
	if err != nil {
		return nil, err
	}
	values := map[string]string{}
	for _, resource := range resources {
		if resource.Err != nil {
			return nil, fmt.Errorf("describe topic %s: %w", resource.Name, resource.Err)
		}
		for _, config := range resource.Configs {
			if config.Key == name && config.Value != nil {
				values[resource.Name] = *config.Value
			}
		}
	}
	for _, topic := range topics {
		if _, ok := values[topic]; !ok {
			return nil, fmt.Errorf("topic %s has no %s", topic, name)
		}
	}
	return values, nil
}

func (service *Service) checkPoolFeedsRetention(ctx context.Context, state State, pool string, minimum int64) error {
	var topics []string
	for tenant, pools := range state.Entitlements {
		if pools[pool] {
			topics = append(topics, TenantTopic(tenant))
		}
	}
	if len(topics) == 0 {
		return nil
	}
	retentions, err := service.topicRetentions(ctx, topics...)
	if err != nil {
		return err
	}
	for _, topic := range topics {
		if !retentionAtLeast(retentions[topic], minimum) {
			return fmt.Errorf("topic %s retention.ms %d is below required %d", topic, retentions[topic], minimum)
		}
	}
	return nil
}

func (service *Service) checkRetention(ctx context.Context, state State) error {
	topics := []string{AdminTopic}
	for receiver := range state.Receivers {
		topics = append(topics, ReceiverTopic(receiver))
	}
	for _, host := range state.Hosts {
		topics = append(topics, HostTopic(host.Tenant, host.Name))
	}
	for pool := range state.Pools {
		topics = append(topics, PoolTasksTopic(pool), PoolEventsTopic(pool))
	}
	for tenant := range state.Entitlements {
		topics = append(topics, TenantTopic(tenant))
	}
	retentions, err := service.topicRetentions(ctx, topics...)
	if err != nil {
		return err
	}
	for _, topic := range topics[:1+len(state.Receivers)+len(state.Hosts)] {
		if retentions[topic] != -1 {
			return fmt.Errorf("topic %s retention.ms is %d, want -1", topic, retentions[topic])
		}
	}
	var poolTopics []string
	for pool := range state.Pools {
		poolTopics = append(poolTopics, PoolTasksTopic(pool), PoolEventsTopic(pool))
	}
	types, err := service.topicConfigs(ctx, "message.timestamp.type", poolTopics...)
	if err != nil {
		return err
	}
	for _, topic := range poolTopics {
		if types[topic] != "LogAppendTime" {
			return fmt.Errorf("topic %s message.timestamp.type is %s, want LogAppendTime", topic, types[topic])
		}
	}
	for pool, terms := range state.Pools {
		bounds, err := poolRetention(terms)
		if err != nil {
			return fmt.Errorf("pool %s: %w", pool, err)
		}
		tasksTopic, eventsTopic := PoolTasksTopic(pool), PoolEventsTopic(pool)
		if !retentionAtLeast(retentions[tasksTopic], bounds.tasks) {
			return fmt.Errorf("topic %s retention.ms %d is below required %d", tasksTopic, retentions[tasksTopic], bounds.tasks)
		}
		if !retentionAtLeast(retentions[eventsTopic], retentions[tasksTopic]) {
			return fmt.Errorf("topic %s retention.ms %d is below tasks retention.ms %d", eventsTopic, retentions[eventsTopic], retentions[tasksTopic])
		}
		for tenant, pools := range state.Entitlements {
			feed := TenantTopic(tenant)
			if pools[pool] && !retentionAtLeast(retentions[feed], bounds.feed) {
				return fmt.Errorf("topic %s retention.ms %d is below required %d", feed, retentions[feed], bounds.feed)
			}
		}
	}
	return nil
}

func retentionAtLeast(actual, minimum int64) bool {
	return actual == -1 || minimum != -1 && actual >= minimum
}

func (service *Service) configureGroup(ctx context.Context, pool string, terms Terms) error {
	values := map[string]string{"share.auto.offset.reset": "earliest", "share.delivery.count.limit": strconv.Itoa(int(terms.DeliveryLimit)), "share.record.lock.duration.ms": strconv.FormatInt(int64(terms.LockDurationMS), 10), "share.isolation.level": "read_committed"}
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

// checkLimits refuses a pool whose largest receiver records the broker or a producer would refuse, and returns the
// record limit of its tasks topic.
func (service *Service) checkLimits(ctx context.Context, state State, pool string) (int64, error) {
	tasks, feeds, err := service.recordLimits(ctx, state, pool)
	if err != nil {
		return 0, err
	}
	feed, err := largestRecords()
	if err != nil {
		return 0, err
	}
	for _, record := range feed {
		if size := batchSize(record.value); size > feeds {
			return 0, fmt.Errorf("%s record is %d bytes, over the %d-byte record limit", record.name, size, feeds)
		}
	}
	return tasks, nil
}

// recordLimits are the largest record batches that every broker and franz-go's producers accept, and with them the
// pool's task topic, for submissions, and the feeds of its tenants, for receiver records.
func (service *Service) recordLimits(ctx context.Context, state State, pool string) (int64, int64, error) {
	limit, err := service.brokerSetting(ctx, "message.max.bytes", func(a, b int64) int64 { return min(a, b) })
	if err != nil {
		return 0, 0, err
	}
	// Hosts and receivers, like the admin client, produce with franz-go's default batch maximum.
	limit = min(limit, int64(service.Admin.OptValue(kgo.ProducerBatchMaxBytes).(int32)))
	topics := []string{PoolTasksTopic(pool)}
	for tenant, pools := range state.Entitlements {
		if pools[pool] {
			topics = append(topics, TenantTopic(tenant))
		}
	}
	resources, err := kadm.NewClient(service.Admin).DescribeTopicConfigs(ctx, topics...)
	if err != nil {
		return 0, 0, err
	}
	tasks, feeds := limit, limit
	for _, resource := range resources {
		// A pool's task topic does not exist before its first create.
		if errors.Is(resource.Err, kerr.UnknownTopicOrPartition) && resource.Name == PoolTasksTopic(pool) {
			continue
		}
		if resource.Err != nil {
			return 0, 0, fmt.Errorf("describe topic %s: %w", resource.Name, resource.Err)
		}
		for _, config := range resource.Configs {
			if config.Key != "max.message.bytes" || config.Value == nil {
				continue
			}
			parsed, err := strconv.ParseInt(*config.Value, 10, 64)
			if err != nil {
				return 0, 0, err
			}
			if resource.Name == PoolTasksTopic(pool) {
				tasks = min(tasks, parsed)
			} else {
				feeds = min(feeds, parsed)
			}
		}
	}
	return tasks, feeds, nil
}

// largestKey is the longest run ID, which keys submissions and output chunks.
var largestKey = strings.Repeat("r", 63)

// batchSize is the size of a record batch holding value alone under largestKey: a v2 batch's 61-byte header and at
// most 36 bytes of one record's varint framing.
func batchSize(value []byte) int64 {
	return int64(61 + 36 + len(largestKey) + len(value))
}

// feedRecord is a receiver record at its largest, as a refusal names it.
type feedRecord struct {
	name  string
	value []byte
}

// largestRecords signs, with a throwaway key, the largest bounded records a receiver writes to a feed. Fields with no
// bound are left empty.
func largestRecords() ([]feedRecord, error) {
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, err
	}
	name, run, hash := strings.Repeat("n", 32), largestKey, strings.Repeat("0", 64)
	pointers := make([]wire.Part, wire.MaxParts)
	pointer, _ := json.Marshal(wire.PointerMetadata{SHA256: hash, Size: math.MaxInt64}) // a string and an integer always encode
	for i := range pointers {
		pointers[i] = wire.Part{URL: "unstored:input", Filename: fmt.Sprintf("%0255d", i), Metadata: pointer}
	}
	message := wire.Message{MessageID: "msg-" + strings.Repeat("0", 32), ContextID: run, TaskID: run, Role: wire.RoleUser, Extensions: []string{wire.ExtensionURI}}
	binding := wire.ReceiverBinding{Receiver: name, Tenant: name, Pool: name, Run: run, Attempt: math.MaxInt64}
	outputName := strings.Repeat("a/", 255) + "aa"
	// A result lists at most 256 collected entries and the expected outputs missing among them. An S3 output is an
	// expected one, listed with its key, so a result returning 256 of them misses none.
	metadata, s3Metadata := wire.ResultMetadata{ReceiverID: name, Pool: name, Profile: name}, wire.ResultMetadata{ReceiverID: name, Pool: name, Profile: name}
	for range wire.MaxOutputs {
		metadata.Outputs = append(metadata.Outputs, wire.ReturnedOutput{Name: outputName, SHA256: hash, Size: math.MaxInt64})
		metadata.NotReturned = append(metadata.NotReturned, wire.UnreturnedOutput{Name: outputName, Size: math.MaxInt64, Reason: "rejected: size changed while read"})
		s3Metadata.S3Outputs = append(s3Metadata.S3Outputs, wire.ReturnedOutput{Name: outputName, SHA256: hash, Size: math.MaxInt64, Key: wire.S3OutputKey(name, run, outputName)})
	}
	encodedMetadata, err := json.Marshal(metadata)
	if err != nil {
		return nil, err
	}
	encodedS3Metadata, err := json.Marshal(s3Metadata)
	if err != nil {
		return nil, err
	}
	history := message
	history.Parts = pointers
	result := func(metadata []byte) *wire.Result {
		return &wire.Result{
			Task: wire.Task{
				ID: run, ContextID: run, History: []wire.Message{history}, Metadata: metadata,
				Status: wire.TaskStatus{State: wire.TaskStateAuthRequired, Timestamp: "2006-01-02T15:04:05.000Z", Message: &wire.Message{
					MessageID: "msg-" + strings.Repeat("0", 32), Role: wire.RoleAgent, Extensions: []string{wire.ExtensionURI}, Parts: []wire.Part{
						{Raw: make(wire.StdBytes, wire.MaxText), MediaType: "text/plain"}, {Data: []byte(`{"credential":"` + name + `"}`), MediaType: "application/json"},
					},
				}},
				Artifacts: []wire.Artifact{{ArtifactID: "result", Parts: []wire.Part{{Raw: make(wire.StdBytes, wire.MaxText), MediaType: "text/plain"}}, Extensions: []string{wire.ExtensionURI}}},
			},
			Wherry: wire.ResultBinding{ReceiverBinding: binding, TaskHash: hash},
		}
	}
	records := []struct {
		name  string
		value wire.Record
	}{
		{"an output chunk", &wire.Output{
			ArtifactUpdate: wire.TaskArtifactUpdateEvent{
				TaskID: run, ContextID: run, Append: true, LastChunk: true, Metadata: []byte(`{"index":9223372036854775807}`),
				Artifact: wire.Artifact{ArtifactID: outputName, Parts: []wire.Part{{Raw: make(wire.StdBytes, wire.OutputChunk)}}, Extensions: []string{wire.ExtensionURI}},
			},
			Wherry: wire.ResultBinding{ReceiverBinding: binding, TaskHash: hash},
		}},
		// Receiver texts are names and fixed words, which JSON does not escape; an agent's text that it would travels raw.
		{"a result", result(encodedMetadata)},
		{"a result returning S3 outputs", result(encodedS3Metadata)},
		{"a transcript chunk", &wire.Chunk{
			ArtifactUpdate: wire.TaskArtifactUpdateEvent{
				TaskID: run, ContextID: run, Append: true, LastChunk: true,
				Artifact: wire.Artifact{ArtifactID: "transcript", Parts: []wire.Part{{Raw: make(wire.StdBytes, wire.MaxLine), MediaType: "application/x-ndjson"}}, Extensions: []string{wire.ExtensionURI}},
			},
			Wherry: wire.ChunkBinding{ReceiverBinding: binding, TaskHash: hash, Seq: math.MaxInt64},
		}},
	}
	feed := make([]feedRecord, len(records))
	for i, record := range records {
		value, err := wire.SignRecord(record.value, key)
		if err != nil {
			return nil, fmt.Errorf("sign %s: %w", record.name, err)
		}
		feed[i] = feedRecord{record.name, value}
	}
	return feed, nil
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
