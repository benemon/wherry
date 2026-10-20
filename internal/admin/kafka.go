package admin

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/benemon/wherry/internal/wire"
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
	// Platform holds platform credentials by adapter name, for platforms that
	// do not issue one by their own identity.
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
	if err := service.createTopics(ctx, "-1", AdminTopic); err != nil {
		return err
	}
	current, err := service.LoadState(ctx)
	if err != nil {
		return err
	}
	if err := service.checkLimits(ctx, current, pool, withDefaults(terms)); err != nil {
		return err
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
	return writeBundle(bundle, Bundle{service.Broker, service.CA, ReceiverPrincipal(config.Receiver), password, platform})
}

// ReceiverConfigure records a new configuration for an existing receiver in its
// pool. The principal, its password and the bundle are untouched; the receiver
// exits on the new record and its platform restarts it.
func (service *Service) ReceiverConfigure(ctx context.Context, config ReceiverConfig) error {
	state, err := service.LoadState(ctx)
	if err != nil {
		return err
	}
	current, ok := state.Receivers[config.Receiver]
	if !ok || state.Ended[ReceiverPrincipal(config.Receiver)] {
		return errors.New("no live receiver by that name")
	}
	config.Pool = current.Pool
	if err := ValidateConfig(config, state.Pools[config.Pool]); err != nil {
		return err
	}
	_, err = service.appendChecked(ctx, []byte(config.Receiver), Record{Kind: "receiver", Pool: config.Pool, Receiver: config.Receiver, Config: &config})
	return err
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
	current, err := service.LoadState(ctx)
	if err != nil {
		return err
	}
	terms, ok := current.Pools[pool]
	if !ok {
		return errors.New("entitlement names unknown pool")
	}
	// The pool's records must fit the tenant's feed, so it exists before the entitlement does.
	if err := service.createTopics(ctx, tenantRetentionMS, TenantTopic(tenant)); err != nil {
		return err
	}
	if current.Entitlements[tenant] == nil {
		current.Entitlements[tenant] = map[string]bool{}
	}
	current.Entitlements[tenant][pool] = true
	if err := service.checkLimits(ctx, current, pool, terms); err != nil {
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
	for pool, terms := range state.Pools {
		if err := service.checkLimits(ctx, state, pool, terms); err != nil {
			return fmt.Errorf("pool %s: %w", pool, err)
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

// checkLimits refuses terms whose largest submission or output chunk is a record the broker or a producer would
// refuse.
func (service *Service) checkLimits(ctx context.Context, state State, pool string, terms Terms) error {
	tasks, feeds, err := service.recordLimits(ctx, state, pool)
	if err != nil {
		return err
	}
	// Inputs only grow on the wire, so this refuses an inline limit no record carries before allocating it.
	if terms.InlineLimit > tasks {
		return fmt.Errorf("inline limit %d bytes is over the %d-byte record limit", terms.InlineLimit, tasks)
	}
	submission, feed, err := largestRecords(terms.InlineLimit)
	if err != nil {
		return err
	}
	if size := batchSize(submission); size > tasks {
		return fmt.Errorf("inline limit %d bytes makes a %d-byte submission record, over the %d-byte record limit", terms.InlineLimit, size, tasks)
	}
	for _, record := range feed {
		if size := batchSize(record.value); size > feeds {
			return fmt.Errorf("%s record is %d bytes, over the %d-byte record limit", record.name, size, feeds)
		}
	}
	return nil
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

// largestRecords signs, with a throwaway key, the largest bounded submission whose inputs fit inlineLimit, and the
// largest bounded records a receiver writes to a feed: an output chunk, a result, and a transcript chunk of a line of
// wire.MaxLine raw bytes. Every field with a bound is at its maximum; the submission's parts are all input files with
// the longest names, one holding all the inputs. A git input exists only on a first turn and outweighs the longest
// turn number, so the submission is a first turn with a git input at its bounds and the one task credential it names.
// A prompt, part metadata and media types, a selector, the other task credentials, the outputs and the S3 inputs, a
// receiver's labels and an adapter's job name have no bound and are left empty.
func largestRecords(inlineLimit int64) ([]byte, []feedRecord, error) {
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, nil, err
	}
	name, run, hash := strings.Repeat("n", 32), largestKey, strings.Repeat("0", 64)
	parts := make([]wire.Part, wire.MaxParts)
	pointers := make([]wire.Part, wire.MaxParts)
	pointer, _ := json.Marshal(wire.PointerMetadata{SHA256: hash, Size: math.MaxInt64}) // a string and an integer always encode
	for i := range parts {
		parts[i] = wire.Part{Raw: wire.StdBytes{}, Filename: fmt.Sprintf("%0255d", i)}
		pointers[i] = wire.Part{URL: "unstored:input", Filename: parts[i].Filename, Metadata: pointer}
	}
	parts[0].Raw = make(wire.StdBytes, inlineLimit)
	message := wire.Message{MessageID: "msg-" + strings.Repeat("0", 32), ContextID: run, TaskID: run, Role: wire.RoleUser, Extensions: []string{wire.ExtensionURI}}
	submitted := message
	submitted.Parts = parts
	submission, err := wire.SignRecord(&wire.Submission{
		Message: submitted,
		Wherry: wire.SubmissionBinding{
			Tenant: name, Host: name, Pool: name, Run: run, Turn: 1, Placement: wire.Placement{Agent: name},
			DeadlineMS: math.MaxInt64, QueueDeadlineMS: math.MaxInt64,
			Credentials: []wire.TaskCredential{{Name: name, File: strings.Repeat("f", 255)}},
			// A quote is the printable character JSON lengthens most.
			Git: &wire.GitInput{
				Remote: "ssh://h/" + strings.Repeat("a", wire.MaxGitRemote-len("ssh://h/")), Ref: strings.Repeat("r", wire.MaxGitRef),
				Path: strings.Repeat("a/", 255) + "aa", Credential: name, HostKey: "h k " + strings.Repeat(`"`, wire.MaxGitHostKey-len("h k ")),
			},
		},
	}, key)
	if err != nil {
		return nil, nil, err
	}
	binding := wire.ReceiverBinding{Receiver: name, Tenant: name, Pool: name, Run: run, Turn: math.MaxInt64, Attempt: math.MaxInt64}
	outputName := strings.Repeat("a/", 255) + "aa"
	// A result lists at most 256 collected entries and the expected outputs missing among them. An S3 output is an
	// expected one, listed with its key, so a result returning 256 of them misses none.
	metadata, s3Metadata := wire.ResultMetadata{ReceiverID: name, Pool: name, Adapter: name}, wire.ResultMetadata{ReceiverID: name, Pool: name, Adapter: name}
	for range wire.MaxOutputs {
		metadata.Outputs = append(metadata.Outputs, wire.ReturnedOutput{Name: outputName, SHA256: hash, Size: math.MaxInt64})
		metadata.NotReturned = append(metadata.NotReturned, wire.UnreturnedOutput{Name: outputName, Size: math.MaxInt64, Reason: "rejected: size changed while read"})
		s3Metadata.S3Outputs = append(s3Metadata.S3Outputs, wire.ReturnedOutput{Name: outputName, SHA256: hash, Size: math.MaxInt64, Key: wire.S3OutputKey(name, run, math.MaxInt64, outputName)})
	}
	encodedMetadata, err := json.Marshal(metadata)
	if err != nil {
		return nil, nil, err
	}
	encodedS3Metadata, err := json.Marshal(s3Metadata)
	if err != nil {
		return nil, nil, err
	}
	// Redaction lengthens the agent's result record by at most 10 bytes in 8.
	agentResult, _ := json.Marshal(map[string]map[string]string{"agent_result": {"k": strings.Repeat("x", wire.MaxAgentResult*10/8)}}) // strings always encode
	history := message
	history.Parts = pointers
	result := func(metadata []byte) *wire.Result {
		return &wire.Result{
			Task: wire.Task{
				ID: run, ContextID: run, History: []wire.Message{history}, Metadata: metadata,
				Status: wire.TaskStatus{State: wire.TaskStateFailed, Timestamp: "2006-01-02T15:04:05.000Z", Message: &wire.Message{
					MessageID: "msg-" + strings.Repeat("0", 32), Role: wire.RoleAgent, Parts: []wire.Part{{Text: strings.Repeat("x", wire.MaxText)}}, Extensions: []string{wire.ExtensionURI},
				}},
				Artifacts: []wire.Artifact{{ArtifactID: "result", Parts: []wire.Part{{Raw: make(wire.StdBytes, wire.MaxText), MediaType: "text/plain"}}, Metadata: agentResult, Extensions: []string{wire.ExtensionURI}}},
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
		// Receiver texts are names and fixed words, which JSON does not escape; a result text that it would travels raw.
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
			return nil, nil, fmt.Errorf("sign %s: %w", record.name, err)
		}
		feed[i] = feedRecord{record.name, value}
	}
	return submission, feed, nil
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
