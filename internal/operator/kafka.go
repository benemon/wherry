package operator

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"gopkg.in/yaml.v3"
)

type Service struct {
	SuperUser       *kgo.Client // connected as the Kafka super user
	Operator        *kgo.Client
	Broker          string
	CA              string // PEM, copied into bundles
	DeliveryCeiling int32
	LockCeilingMS   int64
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
	// Brokers that answer ApiVersions at v4 at most refuse v5, the default, at a round trip per broker per client.
	versions := kversion.Stable()
	versions.SetMaxKeyVersion(18, 4)
	return []kgo.Opt{
		kgo.SeedBrokers(broker),
		kgo.DialTLSConfig(&tls.Config{RootCAs: roots}),
		kgo.SASL(scram.Auth{User: user, Pass: password}.AsSha512Mechanism()),
		kgo.DisableClientMetrics(),
		kgo.MaxVersions(versions),
	}, nil
}

// PoolApply records terms for pool and converges its topics and share group: an absent pool is created, a present one
// with other terms is updated, and one with the same terms has its missing topics completed. With dryRun it makes the
// checks and writes nothing.
func (service *Service) PoolApply(ctx context.Context, pool string, terms Terms, dryRun bool) error {
	if err := validateNames(pool); err != nil {
		return err
	}
	effective := withDefaults(terms)
	if err := validateTerms(effective); err != nil {
		return err
	}
	retention, err := poolRetention(effective)
	if err != nil {
		return err
	}
	// A run's presigned URLs must last from submit to its end.
	if effective.Storage != nil && effective.StartByLimitMS+MaxRunDurationMS > Millis(MaxPresign.Milliseconds()) {
		return errors.New("start_by_limit plus the 6h run cap exceeds 7 days, the longest an S3 SigV4 presigned URL lasts")
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
	// The broker refuses the share group's lock duration only once the terms are recorded.
	if service.LockCeilingMS == 0 {
		if service.LockCeilingMS, err = service.brokerSetting(ctx, "group.share.max.record.lock.duration.ms", func(a, b int64) int64 { return min(a, b) }); err != nil {
			return err
		}
	}
	if int64(effective.LockDurationMS) > service.LockCeilingMS {
		return fmt.Errorf("lock_duration %s exceeds the broker's group.share.max.record.lock.duration.ms %s", effective.LockDuration(), time.Duration(service.LockCeilingMS)*time.Millisecond)
	}
	current, err := service.loadPoolState(ctx, dryRun)
	if err != nil {
		return err
	}
	_, exists := current.Pools[pool]
	if exists {
		if err := strandedReceivers(current, pool, terms); err != nil {
			return err
		}
	}
	if !dryRun {
		if err := service.configureTopics(ctx, feedConfigs, tenantFeeds(current)...); err != nil {
			return err
		}
	}
	// A dry run measures each feed at the limit the update would raise it to.
	if terms.RecordLimit, err = service.checkLimits(ctx, current, pool, dryRun); err != nil {
		return err
	}
	// The same terms at the same record limit again complete an apply that failed after recording them.
	recorded := current.Pools[pool]
	update := exists && (!sameTerms(recorded, terms) || recorded.RecordLimit != terms.RecordLimit)
	if err := service.checkPoolFeedsRetention(ctx, current, pool, retention.feed); err != nil {
		return err
	}
	if dryRun {
		return service.onePartition(ctx, true, PoolTasksTopic(pool), PoolEventsTopic(pool))
	}
	// Terms with a longer start_by_limit take effect only once the retention they need has.
	if update {
		if err := service.poolTopics(ctx, current, pool, retention.tasks); err != nil {
			return err
		}
	} else if err := service.onePartition(ctx, true, PoolTasksTopic(pool), PoolEventsTopic(pool)); err != nil {
		return err
	}
	state := current
	if update || !exists {
		record := Record{Kind: "pool", Pool: pool, Terms: &terms}
		if state, err = service.appendChecked(ctx, []byte("pool."+pool), record); err != nil {
			return err
		}
	}
	// A new pool's topics are created only once CheckNames has accepted its record.
	if !update {
		if err := service.poolTopics(ctx, state, pool, retention.tasks); err != nil {
			return err
		}
	}
	if err := service.createTopicsWith(ctx, custodianConfigs, CustodianTopic(pool)); err != nil {
		return err
	}
	if err := service.configureTopics(ctx, custodianConfigs, CustodianTopic(pool)); err != nil {
		return err
	}
	if err := service.configureGroup(ctx, pool, effective); err != nil {
		return err
	}
	return service.reconcileACLs(ctx, state)
}

// loadPoolState is the recorded state, creating wherry.operator unless dryRun: a dry run changes nothing, and an absent
// wherry.operator records nothing.
func (service *Service) loadPoolState(ctx context.Context, dryRun bool) (State, error) {
	if !dryRun {
		if err := service.createTopics(ctx, "-1", OperatorTopic); err != nil {
			return State{}, err
		}
		return service.LoadState(ctx)
	}
	request := kmsg.NewPtrMetadataRequest()
	requested := kmsg.NewMetadataRequestTopic()
	requested.Topic = kmsg.StringPtr(OperatorTopic)
	request.Topics = append(request.Topics, requested)
	response, err := request.RequestWith(ctx, service.SuperUser)
	if err != nil {
		return State{}, err
	}
	if len(response.Topics) == 1 && response.Topics[0].ErrorCode == kerr.UnknownTopicOrPartition.Code {
		return NewState(), nil
	}
	return service.LoadState(ctx)
}

// poolTopics creates pool's tasks and events topics and converges them, and its live receivers' progress topics, on
// their configs.
func (service *Service) poolTopics(ctx context.Context, state State, pool string, retentionMS int64) error {
	topics := []string{PoolTasksTopic(pool), PoolEventsTopic(pool)}
	if err := service.createTopics(ctx, strconv.FormatInt(retentionMS, 10), topics...); err != nil {
		return err
	}
	if err := service.onePartition(ctx, false, topics...); err != nil {
		return err
	}
	if err := service.configureTopicRetention(ctx, retentionMS, topics...); err != nil {
		return err
	}
	if err := service.configureTopics(ctx, poolConfigs, topics...); err != nil {
		return err
	}
	return service.progressTopics(ctx, state, pool)
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

// sameTerms reports whether terms are recorded's but for the record limit, which pool apply reads from the broker.
func sameTerms(recorded, terms Terms) bool {
	terms = withDefaults(terms)
	terms.RecordLimit = recorded.RecordLimit
	a, errA := json.Marshal(recorded)
	b, errB := json.Marshal(terms)
	return errA == nil && errB == nil && bytes.Equal(a, b)
}

// ReceiverCheck makes ReceiverAdd's checks of config and returns it as the record would hold it, writing nothing.
func (service *Service) ReceiverCheck(ctx context.Context, config ReceiverConfig) (ReceiverConfig, error) {
	state, err := service.LoadState(ctx)
	if err != nil {
		return ReceiverConfig{}, err
	}
	if _, exists := state.Receivers[config.Receiver]; exists {
		return ReceiverConfig{}, errors.New("receiver exists")
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
	if _, err := service.ReceiverCheck(ctx, config); err != nil {
		return err
	}
	bundle, err := createBundle(bundlePath, false)
	if err != nil {
		return err
	}
	defer closeBundle(bundle, &err)
	record := Record{Kind: "receiver", Pool: config.Pool, Receiver: config.Receiver, Config: &config}
	state, _, err := service.planRecord(ctx, record)
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
	if err := service.progressTopics(ctx, state, config.Pool); err != nil {
		return err
	}
	// A live peer that reads the record before it may read the new receiver's topics exits (agb-yk39).
	if err := service.reconcileACLs(ctx, state); err != nil {
		return err
	}
	if _, err := service.appendChecked(ctx, []byte(config.Receiver), record); err != nil {
		return err
	}
	return writeBundle(bundle, Bundle{service.Broker, service.CA, ReceiverPrincipal(config.Receiver), password})
}

// ReceiverReissue gives a live receiver a new password and writes its bundle to bundlePath, replacing any file there.
// Its configuration record is untouched; Kafka keeps one SCRAM credential per user, so the old password stops working.
func (service *Service) ReceiverReissue(ctx context.Context, receiver, bundlePath string) (err error) {
	state, err := service.LoadState(ctx)
	if err != nil {
		return err
	}
	if _, ok := state.Receivers[receiver]; !ok || state.Ended[ReceiverPrincipal(receiver)] {
		return errors.New("no live receiver by that name")
	}
	bundle, err := createBundle(bundlePath, true)
	if err != nil {
		return err
	}
	defer closeBundle(bundle, &err)
	password, err := service.upsertUser(ctx, ReceiverPrincipal(receiver))
	if err != nil {
		return err
	}
	return writeBundle(bundle, Bundle{service.Broker, service.CA, ReceiverPrincipal(receiver), password})
}

// ReceiverUpdate records a new configuration for an existing receiver in its
// pool and returns it as recorded. The principal, its password and the bundle
// are untouched; the receiver exits on the new record and its platform restarts
// it. With dryRun it writes nothing and returns the configuration as the record
// would hold it.
func (service *Service) ReceiverUpdate(ctx context.Context, config ReceiverConfig, dryRun bool) (ReceiverConfig, error) {
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

// HostAdd provisions host in tenant and writes its bundle to bundlePath. A host that exists is refused unless force,
// which gives it a new password and replaces the bundle at bundlePath.
func (service *Service) HostAdd(ctx context.Context, tenant, host, bundlePath string, force bool) (err error) {
	if err := service.createTopics(ctx, "-1", OperatorTopic); err != nil {
		return err
	}
	current, err := service.LoadState(ctx)
	if err != nil {
		return err
	}
	if _, exists := current.Hosts[TenantPrincipal(tenant, host)]; exists && !force {
		return errors.New("host exists; --force gives it a new password and bundle")
	}
	bundle, err := createBundle(bundlePath, force)
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
	if err := service.tenantTopics(ctx, tenant); err != nil {
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
	// The pool's records must fit the tenant's feed, so it exists, at FeedRecordLimit, before the entitlement does.
	if err := service.tenantTopics(ctx, tenant); err != nil {
		return err
	}
	if current.Entitlements[tenant] == nil {
		current.Entitlements[tenant] = map[string]bool{}
	}
	current.Entitlements[tenant][pool] = true
	if _, err := service.checkLimits(ctx, current, pool, false); err != nil {
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
		if _, err := service.checkLimits(ctx, state, pool, false); err != nil {
			return fmt.Errorf("pool %s: %w", pool, err)
		}
	}
	if err := service.checkRetention(ctx, state); err != nil {
		return err
	}
	if err := service.checkFeedLimits(ctx, state); err != nil {
		return err
	}
	return service.verifyACLs(ctx, state)
}

func (service *Service) LoadState(ctx context.Context) (State, error) {
	return Load(ctx, service.Operator)
}

// FeedRecordLimit is the largest record batch a tenant's feed and transcript topic take, their max.message.bytes, and
// the most a receiver's producer batches: a result lists a reference artifact for each output it returns.
const FeedRecordLimit = 2 << 20

// feedConfigs are the configs host add, pool entitle and pool apply set on tenant feeds beyond the
// retention they are created with.
var feedConfigs = map[string]string{"max.message.bytes": strconv.Itoa(FeedRecordLimit)}

// tenantTopics creates tenant's feed and transcript topic, then sets feedConfigs on them, new or existing.
func (service *Service) tenantTopics(ctx context.Context, tenant string) error {
	topics := []string{TenantTopic(tenant), TenantTranscriptTopic(tenant)}
	if err := service.createTopics(ctx, strconv.Itoa(tenantRetentionMS), topics...); err != nil {
		return err
	}
	return service.configureTopics(ctx, feedConfigs, topics...)
}

// tenantFeeds are the feeds and transcript topics of every tenant with a host or an entitlement.
func tenantFeeds(state State) []string {
	tenants := map[string]bool{}
	for _, host := range state.Hosts {
		tenants[host.Tenant] = true
	}
	for tenant := range state.Entitlements {
		tenants[tenant] = true
	}
	var topics []string
	for _, tenant := range slices.Sorted(maps.Keys(tenants)) {
		topics = append(topics, TenantTopic(tenant), TenantTranscriptTopic(tenant))
	}
	return topics
}

// tenantRetentionMS is the tenant feed's and transcript topic's default: the transcript topic carries transcripts, which
// hold anything the agent printed.
const tenantRetentionMS = 30 * 24 * 60 * 60 * 1000

// receiversConfigs are wherry.receivers' topic configs: readers want only each receiver's latest advertisement, which
// is its key's latest record. Compaction cleans closed segments alone, so a segment closes within the compaction lag.
var receiversConfigs = map[string]string{
	"cleanup.policy":         "compact,delete",
	"message.timestamp.type": "LogAppendTime",
	"retention.ms":           "604800000",
	"segment.ms":             "3600000",
	"max.compaction.lag.ms":  "3600000",
}

// poolConfigs are a pool's tasks and events topic configs beyond retention.ms and the timestamp type: retention by time
// alone, deleting a record within an hour of its bound as segments roll hourly.
var poolConfigs = map[string]string{"cleanup.policy": "delete", "retention.bytes": "-1", "segment.ms": "3600000"}

// custodianConfigs are a pool's snapshot topic configs: readers want the latest committed snapshot, which compaction
// keeps under its one key forever.
var custodianConfigs = map[string]string{
	"cleanup.policy":         "compact",
	"message.timestamp.type": "LogAppendTime",
	"retention.ms":           "-1",
}

// progressConfigs are a receiver's progress topic configs: an adopter wants each run's latest offset, its key's latest
// record, while the pool's tasks topic, at retentionMS, keeps the run.
func progressConfigs(retentionMS int64) map[string]string {
	configs := maps.Clone(receiversConfigs)
	configs["retention.ms"] = strconv.FormatInt(retentionMS, 10)
	return configs
}

// progressTopics creates the progress topics of pool's live receivers and converges existing ones on their configs.
func (service *Service) progressTopics(ctx context.Context, state State, pool string) error {
	var topics []string
	for receiver, config := range state.Receivers {
		if config.Pool == pool && !state.Ended[ReceiverPrincipal(receiver)] {
			topics = append(topics, ReceiverProgressTopic(receiver))
		}
	}
	if len(topics) == 0 {
		return nil
	}
	retentions, err := service.topicRetentions(ctx, PoolTasksTopic(pool))
	if err != nil {
		return err
	}
	minimum := retentions[PoolTasksTopic(pool)]
	if err := service.createTopicsWith(ctx, progressConfigs(minimum), topics...); err != nil {
		return err
	}
	current, err := service.topicRetentions(ctx, topics...)
	if err != nil {
		return err
	}
	// A progress topic's retention, like the tasks topic's, is never lowered.
	for _, topic := range topics {
		value := minimum
		if retentionAtLeast(current[topic], minimum) {
			value = current[topic]
		}
		if err := service.configureTopics(ctx, progressConfigs(value), topic); err != nil {
			return err
		}
	}
	return nil
}

const retentionMarginMS = 24 * 60 * 60 * 1000

type retentionBounds struct {
	tasks int64
	feed  int64
}

// poolRetention derives the pool topics' and its tenant feeds' minimum retention.ms from terms with defaults applied.
func poolRetention(terms Terms) (retentionBounds, error) {
	if terms.StartByLimitMS > tenantRetentionMS-MaxRunDurationMS {
		return retentionBounds{}, fmt.Errorf("start_by_limit requires tenant feed retention.ms of at least %d", terms.StartByLimitMS+MaxRunDurationMS)
	}
	feed := int64(terms.StartByLimitMS) + MaxRunDurationMS
	return retentionBounds{tasks: feed + retentionMarginMS, feed: feed}, nil
}

func (service *Service) appendChecked(ctx context.Context, key []byte, record Record) (State, error) {
	state, _, err := service.appendRecord(ctx, key, record)
	return state, err
}

// appendRecord is appendChecked that also returns the record's wherry.operator offset.
func (service *Service) appendRecord(ctx context.Context, key []byte, record Record) (State, int64, error) {
	state, encoded, err := service.planRecord(ctx, record)
	if err != nil {
		return State{}, 0, err
	}
	produced, err := service.Operator.ProduceSync(ctx, &kgo.Record{Topic: OperatorTopic, Key: key, Value: encoded}).First()
	if err != nil {
		return State{}, 0, fmt.Errorf("write operator record: %w", err)
	}
	return state, produced.Offset, nil
}

// planRecord is the state once record is appended and record encoded, refusing a record the state refuses.
func (service *Service) planRecord(ctx context.Context, record Record) (State, []byte, error) {
	if err := service.createTopics(ctx, "-1", OperatorTopic); err != nil {
		return State{}, nil, err
	}
	if err := service.createTopicsWith(ctx, receiversConfigs, ReceiversTopic); err != nil {
		return State{}, nil, err
	}
	// A topic created with older configs converges on the next operator write.
	if err := service.configureTopics(ctx, receiversConfigs, ReceiversTopic); err != nil {
		return State{}, nil, err
	}
	state, err := service.LoadState(ctx)
	if err != nil {
		return State{}, nil, err
	}
	encoded, err := EncodeRecord(record)
	if err != nil {
		return State{}, nil, err
	}
	decoded, _ := DecodeRecord(encoded)
	if err := apply(&state, decoded); err != nil {
		return State{}, nil, err
	}
	if err := state.CheckNames(); err != nil {
		return State{}, nil, err
	}
	return state, encoded, nil
}

func (service *Service) createTopics(ctx context.Context, retentionMS string, topics ...string) error {
	return service.createTopicsWith(ctx, map[string]string{"cleanup.policy": "delete", "message.timestamp.type": "LogAppendTime", "retention.ms": retentionMS}, topics...)
}

func (service *Service) createTopicsWith(ctx context.Context, values map[string]string, topics ...string) error {
	configs := map[string]*string{}
	for name, value := range values {
		configs[name] = kadm.StringPtr(value)
	}
	responses, err := kadm.NewClient(service.SuperUser).CreateTopics(ctx, 1, -1, configs, topics...)
	if err != nil {
		return err
	}
	for _, response := range responses {
		if response.Err != nil && !errors.Is(response.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w", response.Topic, response.Err)
		}
	}
	deadline := time.Now().Add(topicWait)
	for service.propagating(ctx, topics) && time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(listBackoff):
		}
	}
	return nil
}

// propagating reports whether a broker's own metadata lacks one of topics or a leader for one of their partitions, as
// it does until a created topic's metadata reaches it; describes and config changes it answers fail until then.
func (service *Service) propagating(ctx context.Context, topics []string) bool {
	metadata, err := kadm.NewClient(service.SuperUser).BrokerMetadata(ctx)
	if err != nil {
		return false
	}
	request := kmsg.NewPtrMetadataRequest()
	for _, topic := range topics {
		requested := kmsg.NewMetadataRequestTopic()
		requested.Topic = kmsg.StringPtr(topic)
		request.Topics = append(request.Topics, requested)
	}
	for _, broker := range metadata.Brokers {
		response, err := request.RequestWith(ctx, service.SuperUser.Broker(int(broker.NodeID)))
		if err != nil {
			return false
		}
		for _, topic := range response.Topics {
			if kerr.IsRetriable(kerr.ErrorForCode(topic.ErrorCode)) || len(topic.Partitions) == 0 {
				return true
			}
			for _, partition := range topic.Partitions {
				if kerr.IsRetriable(kerr.ErrorForCode(partition.ErrorCode)) || partition.Leader < 0 {
					return true
				}
			}
		}
	}
	return false
}

// configureTopicRetention sets each of topics' retention.ms to minimum unless it is already at least that: a run
// submitted under longer terms needs the retention they derived. Each topic also ends at least at the one before it, as
// a pool's events topic must keep a run's events as long as its tasks topic keeps the run.
func (service *Service) configureTopicRetention(ctx context.Context, minimum int64, topics ...string) error {
	retentions, err := service.topicRetentions(ctx, topics...)
	if err != nil {
		return err
	}
	for _, topic := range topics {
		value := minimum
		if retentionAtLeast(retentions[topic], minimum) {
			value = retentions[topic]
		}
		minimum = value
		// Admission measures a run's age from the broker's timestamp of its submission.
		if err := service.configureTopics(ctx, map[string]string{"retention.ms": strconv.FormatInt(value, 10), "message.timestamp.type": "LogAppendTime"}, topic); err != nil {
			return err
		}
	}
	return nil
}

func (service *Service) configureTopics(ctx context.Context, values map[string]string, topics ...string) error {
	if len(topics) == 0 {
		return nil
	}
	var configs []kadm.AlterConfig
	for name, value := range values {
		configs = append(configs, kadm.AlterConfig{Op: kadm.SetConfig, Name: name, Value: &value})
	}
	responses, err := kadm.NewClient(service.SuperUser).AlterTopicConfigs(ctx, configs, topics...)
	if err != nil {
		return err
	}
	for _, response := range responses {
		if response.Err != nil {
			return fmt.Errorf("configure topic %s: %w", response.Name, response.Err)
		}
	}
	// The controller accepts the change before each broker applies it from the metadata log, and a describe that broker
	// answers meanwhile returns the old value.
	deadline := time.Now().Add(topicWait)
	for {
		applied, err := service.configured(ctx, values, topics)
		if applied {
			return nil
		}
		if !time.Now().Before(deadline) {
			if err != nil {
				return fmt.Errorf("configure topics %s: %w", strings.Join(topics, ", "), err)
			}
			return fmt.Errorf("configure topics %s: not applied within %s", strings.Join(topics, ", "), topicWait)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(listBackoff):
		}
	}
}

// configured reports whether a describe of topics shows each of values. A failed describe confirms nothing.
func (service *Service) configured(ctx context.Context, values map[string]string, topics []string) (bool, error) {
	for name, value := range values {
		got, err := service.topicConfigs(ctx, name, topics...)
		if err != nil {
			return false, err
		}
		for _, topic := range topics {
			if got[topic] != value {
				return false, nil
			}
		}
	}
	return true, nil
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
	resources, err := kadm.NewClient(service.SuperUser).DescribeTopicConfigs(ctx, topics...)
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
			topics = append(topics, TenantTopic(tenant), TenantTranscriptTopic(tenant))
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
	topics := []string{OperatorTopic}
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
		topics = append(topics, TenantTopic(tenant), TenantTranscriptTopic(tenant))
	}
	for receiver := range state.Receivers {
		if !state.Ended[ReceiverPrincipal(receiver)] {
			topics = append(topics, ReceiverProgressTopic(receiver))
		}
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
	for _, name := range slices.Sorted(maps.Keys(poolConfigs)) {
		values, err := service.topicConfigs(ctx, name, poolTopics...)
		if err != nil {
			return err
		}
		for _, topic := range poolTopics {
			if values[topic] != poolConfigs[name] {
				return fmt.Errorf("topic %s %s is %s, want %s", topic, name, values[topic], poolConfigs[name])
			}
		}
	}
	var custodianTopics []string
	for pool := range state.Pools {
		custodianTopics = append(custodianTopics, CustodianTopic(pool))
	}
	for _, name := range slices.Sorted(maps.Keys(custodianConfigs)) {
		values, err := service.topicConfigs(ctx, name, custodianTopics...)
		if err != nil {
			return err
		}
		for _, topic := range custodianTopics {
			if values[topic] != custodianConfigs[name] {
				return fmt.Errorf("topic %s %s is %s, want %s", topic, name, values[topic], custodianConfigs[name])
			}
		}
	}
	if err := service.onePartition(ctx, false, slices.Concat(poolTopics, custodianTopics)...); err != nil {
		return err
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
		for receiver, config := range state.Receivers {
			progress := ReceiverProgressTopic(receiver)
			if config.Pool == pool && !state.Ended[ReceiverPrincipal(receiver)] && !retentionAtLeast(retentions[progress], retentions[tasksTopic]) {
				return fmt.Errorf("topic %s retention.ms %d is below tasks retention.ms %d", progress, retentions[progress], retentions[tasksTopic])
			}
		}
		for tenant, pools := range state.Entitlements {
			for _, feed := range []string{TenantTopic(tenant), TenantTranscriptTopic(tenant)} {
				if pools[pool] && !retentionAtLeast(retentions[feed], bounds.feed) {
					return fmt.Errorf("topic %s retention.ms %d is below required %d", feed, retentions[feed], bounds.feed)
				}
			}
		}
	}
	return nil
}

// onePartition refuses any of topics without exactly one partition, passing one that does not exist if absent allows
// it. One partition is what gives the custodian group a single assignee and readers one order; partitions cannot be
// removed, so a pool topic with more is recreated.
func (service *Service) onePartition(ctx context.Context, absent bool, topics ...string) error {
	// kadm's ListTopics answers from metadata up to 5s old, which can predate a topic's creation.
	request := kmsg.NewPtrMetadataRequest()
	for _, topic := range topics {
		requested := kmsg.NewMetadataRequestTopic()
		requested.Topic = kmsg.StringPtr(topic)
		request.Topics = append(request.Topics, requested)
	}
	response, err := request.RequestWith(ctx, service.SuperUser)
	if err != nil {
		return err
	}
	for _, topic := range response.Topics {
		if absent && topic.ErrorCode == kerr.UnknownTopicOrPartition.Code {
			continue
		}
		if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
			return fmt.Errorf("topic %s: %w", *topic.Topic, err)
		}
		if partitions := len(topic.Partitions); partitions != 1 {
			return fmt.Errorf("topic %s has %d partitions, want 1", *topic.Topic, partitions)
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
	response, err := service.SuperUser.Request(ctx, request)
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
	results, err := kadm.NewClient(service.SuperUser).AlterUserSCRAMs(ctx, nil, []kadm.UpsertSCRAM{{User: user, Mechanism: kadm.ScramSha512, Iterations: 8192, Password: password}})
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
	results, err := kadm.NewClient(service.SuperUser).AlterClientQuotas(ctx, []kadm.AlterClientQuotaEntry{{Entity: kadm.ClientQuotaEntity{{Type: "user", Name: &principal}}, Ops: []kadm.AlterClientQuotaOp{{Key: "producer_byte_rate", Value: float64(rate)}, {Key: "request_percentage", Value: request}}}})
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

// checkFeedLimits reports a tenant feed or transcript topic whose max.message.bytes is below FeedRecordLimit.
func (service *Service) checkFeedLimits(ctx context.Context, state State) error {
	feeds := tenantFeeds(state)
	if len(feeds) == 0 {
		return nil
	}
	values, err := service.topicConfigs(ctx, "max.message.bytes", feeds...)
	if err != nil {
		return err
	}
	for _, feed := range feeds {
		limit, err := strconv.ParseInt(values[feed], 10, 64)
		if err != nil {
			return fmt.Errorf("topic %s max.message.bytes: %w", feed, err)
		}
		if limit < FeedRecordLimit {
			return fmt.Errorf("topic %s max.message.bytes %d is below required %d", feed, limit, FeedRecordLimit)
		}
	}
	return nil
}

// brokerSetting reads a numeric setting from every broker, combining values with pick.
func (service *Service) brokerSetting(ctx context.Context, name string, pick func(a, b int64) int64) (int64, error) {
	client := kadm.NewClient(service.SuperUser)
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
// record limit of its tasks topic. With raised, a feed below FeedRecordLimit counts as raised to it.
func (service *Service) checkLimits(ctx context.Context, state State, pool string, raised bool) (int64, error) {
	tasks, feeds, err := service.recordLimits(ctx, state, pool, raised)
	if err != nil {
		return 0, err
	}
	for _, record := range feedRecords {
		if record.batch > feeds {
			return 0, fmt.Errorf("%s record is %d bytes, over the %d-byte record limit", record.name, record.batch, feeds)
		}
	}
	if taskRecord.batch > tasks {
		return 0, fmt.Errorf("%s record is %d bytes, over the tasks topic's %d-byte record limit", taskRecord.name, taskRecord.batch, tasks)
	}
	return tasks, nil
}

// recordLimits are the largest record batches that the pool's task topic, for submissions, and the feeds of its
// tenants, for receiver records, accept, each within what its producers batch. A topic's max.message.bytes overrides
// the brokers' message.max.bytes, which bounds a topic that does not describe its own.
func (service *Service) recordLimits(ctx context.Context, state State, pool string, raised bool) (int64, int64, error) {
	limit, err := service.brokerSetting(ctx, "message.max.bytes", func(a, b int64) int64 { return min(a, b) })
	if err != nil {
		return 0, 0, err
	}
	topics := []string{PoolTasksTopic(pool)}
	for tenant, pools := range state.Entitlements {
		if pools[pool] {
			topics = append(topics, TenantTopic(tenant), TenantTranscriptTopic(tenant))
		}
	}
	resources, err := kadm.NewClient(service.SuperUser).DescribeTopicConfigs(ctx, topics...)
	if err != nil {
		return 0, 0, err
	}
	// Hosts, like the super-user client, produce with franz-go's default batch maximum, and receivers with FeedRecordLimit.
	tasks, feeds := int64(service.SuperUser.OptValue(kgo.ProducerBatchMaxBytes).(int32)), int64(FeedRecordLimit)
	for _, resource := range resources {
		// A topic without its own max.message.bytes, as a pool's task topic before its first create, takes each
		// broker's message.max.bytes; one set on the topic overrides them.
		topic := limit
		if !errors.Is(resource.Err, kerr.UnknownTopicOrPartition) || resource.Name != PoolTasksTopic(pool) {
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
				if config.Source == kmsg.ConfigSourceDynamicTopicConfig {
					topic = parsed
				} else {
					topic = min(topic, parsed)
				}
			}
		}
		switch {
		case resource.Name == PoolTasksTopic(pool):
			tasks = min(tasks, topic)
		case raised:
			feeds = min(feeds, max(topic, FeedRecordLimit))
		default:
			feeds = min(feeds, topic)
		}
	}
	return tasks, feeds, nil
}

// taskRecord is the smallest task a host submits, carrying a maximum envelope and a submit wrap, with the size of a
// record batch holding it alone, signed. TestTaskRecordIsPinned builds and signs it to pin this.
var taskRecord = struct {
	name  string
	batch int64
}{"a task with a maximum envelope", 89326}

// feedRecords are the largest bounded records a receiver writes to a feed, as a refusal names them, each with the size
// of a record batch holding it alone, signed. TestFeedRecordsArePinned builds and signs each record to pin these.
var feedRecords = []struct {
	name  string
	batch int64
}{
	{"an output chunk", 468880},
	{"a result", 1259677},
	{"a result returning S3 outputs", 822752},
	{"a transcript chunk", 933498},
}

// createBundle opens a temporary file beside path that writeBundle fills and closeBundle moves to path, so a reader
// of path never sees part of a bundle. Without replace, an existing file at path is refused.
func createBundle(path string, replace bool) (*bundleFile, error) {
	if _, err := os.Lstat(path); err == nil && !replace {
		return nil, fmt.Errorf("bundle %s: %w", path, os.ErrExist)
	}
	file, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return nil, err
	}
	return &bundleFile{file, path, replace}, nil
}

type bundleFile struct {
	*os.File
	path    string
	replace bool
}

// Tests replace these to fail a bundle's write, close, link or rename.
var (
	writeFile  = (*os.File).Write
	closeFile  = (*os.File).Close
	linkFile   = os.Link
	renameFile = os.Rename
)

// closeBundle moves a written bundle to its path and removes its temporary name, so a retry can create it. Without
// replace it links rather than renames: a link refuses a file created at path since createBundle checked.
func closeBundle(bundle *bundleFile, err *error) {
	if closeErr := closeFile(bundle.File); *err == nil {
		*err = closeErr
	}
	switch {
	case *err != nil:
	case bundle.replace:
		*err = renameFile(bundle.Name(), bundle.path)
	default:
		*err = linkFile(bundle.Name(), bundle.path)
	}
	if *err != nil || !bundle.replace {
		os.Remove(bundle.Name())
	}
}

func writeBundle(bundle *bundleFile, contents Bundle) error {
	encoded, err := yaml.Marshal(contents)
	if err != nil {
		return err
	}
	_, err = writeFile(bundle.File, encoded)
	return err
}
