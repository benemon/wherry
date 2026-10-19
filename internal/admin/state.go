package admin

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
)

type AgentTerms struct {
	Name        string   `json:"name" yaml:"name"`
	Credentials []string `json:"credentials" yaml:"credentials"`
}
type Terms struct {
	Agents            []AgentTerms `json:"agents" yaml:"agents"`
	DefaultAgent      string       `json:"default_agent" yaml:"default_agent"`
	SealLifetimeMS    int64        `json:"seal_lifetime_ms" yaml:"seal_lifetime_ms"`
	DeliveryLimit     int32        `json:"delivery_limit" yaml:"delivery_limit"`
	LockDurationMS    int64        `json:"lock_duration_ms" yaml:"lock_duration_ms"`
	ProducerByteRate  int64        `json:"producer_byte_rate" yaml:"producer_byte_rate"`
	RequestPercentage float64      `json:"request_percentage" yaml:"request_percentage"`
}

// ReceiverConfig is a receiver's whole configuration, recorded on
// wherry.admin and read by the receiver after it authenticates.
type ReceiverConfig struct {
	Receiver string            `json:"receiver" yaml:"receiver"`
	Pool     string            `json:"pool" yaml:"pool"`
	Capacity int64             `json:"capacity" yaml:"capacity"`
	Adapters []AdapterSettings `json:"adapters" yaml:"adapters"`
}

// AdapterSettings configures one adapter: the agent it launches, the
// credential that agent takes, the labels its operator declares for it, and
// settings its type decodes (runtime, node pool and the like stay there).
type AdapterSettings struct {
	Name       string            `json:"name" yaml:"name"`
	Type       string            `json:"type" yaml:"type"`
	Agent      string            `json:"agent" yaml:"agent"`
	Credential string            `json:"credential,omitzero" yaml:"credential"`
	Labels     map[string]string `json:"labels,omitzero" yaml:"labels"`
	Config     map[string]any    `json:"config" yaml:"config"`
}

type Record struct {
	Version  int             `json:"v"`
	Kind     string          `json:"kind"`
	Pool     string          `json:"pool,omitzero"`
	Tenant   string          `json:"tenant,omitzero"`
	Host     string          `json:"host,omitzero"`
	Receiver string          `json:"receiver,omitzero"`
	Terms    *Terms          `json:"terms,omitzero"`
	Config   *ReceiverConfig `json:"config,omitzero"`
	Offset   int64           `json:"offset,omitzero"`
	SHA256   string          `json:"sha256,omitzero"`
	AtMS     int64           `json:"at_ms"`
}
type Host struct{ Tenant, Name string }
type State struct {
	Pools        map[string]Terms
	Receivers    map[string]ReceiverConfig
	Hosts        map[string]Host
	Entitlements map[string]map[string]bool
	Ended        map[string]bool
}

func NewState() State {
	return State{map[string]Terms{}, map[string]ReceiverConfig{}, map[string]Host{}, map[string]map[string]bool{}, map[string]bool{}}
}

func EncodeRecord(record Record) ([]byte, error) {
	if record.Version == 0 {
		record.Version = 1
	}
	if record.AtMS == 0 {
		record.AtMS = time.Now().UnixMilli()
	}
	if err := validateRecord(record); err != nil {
		return nil, err
	}
	return json.Marshal(record, json.Deterministic(true))
}

func DecodeRecord(encoded []byte) (Record, error) {
	var record Record
	if err := json.Unmarshal(encoded, &record, json.RejectUnknownMembers(true), jsontext.AllowDuplicateNames(false)); err != nil {
		return Record{}, fmt.Errorf("decode admin record: %w", err)
	}
	if err := validateRecord(record); err != nil {
		return Record{}, err
	}
	return record, nil
}

func Fold(records []Record) (State, error) {
	state := NewState()
	for _, record := range records {
		if err := apply(&state, record); err != nil {
			return State{}, err
		}
	}
	if err := state.CheckNames(); err != nil {
		return State{}, err
	}
	return state, nil
}

func apply(state *State, record Record) error {
	switch record.Kind {
	case "pool":
		state.Pools[record.Pool] = *record.Terms
	case "receiver":
		if _, ok := state.Pools[record.Pool]; !ok {
			return errors.New("receiver names unknown pool")
		}
		if previous, ok := state.Receivers[record.Receiver]; (ok && previous.Pool != record.Pool) || state.Ended[ReceiverPrincipal(record.Receiver)] {
			return errors.New("receiver cannot move pool or return after its membership end")
		}
		state.Receivers[record.Receiver] = *record.Config
	case "host":
		if state.Ended[TenantPrincipal(record.Tenant, record.Host)] {
			return errors.New("host cannot return after its membership end")
		}
		state.Hosts[TenantPrincipal(record.Tenant, record.Host)] = Host{record.Tenant, record.Host}
	case "entitlement":
		if _, ok := state.Pools[record.Pool]; !ok {
			return errors.New("entitlement names unknown pool")
		}
		if state.Entitlements[record.Tenant] == nil {
			state.Entitlements[record.Tenant] = map[string]bool{}
		}
		state.Entitlements[record.Tenant][record.Pool] = true
	case "end":
		state.Ended[subject(record)] = true
	}
	return nil
}

func (state State) CheckNames() error {
	seen := map[string]string{AdminTopic: "admin topic", ReceiversTopic: "receivers topic"}
	add := func(value, owner string) error {
		if previous, ok := seen[value]; ok {
			return fmt.Errorf("name collision between %s and %s", previous, owner)
		}
		seen[value] = owner
		return nil
	}
	for pool := range state.Pools {
		if err := validateNames(pool); err != nil {
			return err
		}
		for _, value := range []string{PoolTasksTopic(pool), PoolEventsTopic(pool), PoolGroup(pool), TransactionalPrefix(pool)} {
			if err := add(value, "pool "+pool); err != nil {
				return err
			}
		}
	}
	for receiver := range state.Receivers {
		if err := validateNames(receiver); err != nil {
			return err
		}
		for _, value := range []string{ReceiverPrincipal(receiver), ReceiverTopic(receiver)} {
			if err := add(value, "receiver "+receiver); err != nil {
				return err
			}
		}
	}
	for _, host := range state.Hosts {
		if err := validateNames(host.Tenant, host.Name); err != nil {
			return err
		}
		for _, value := range []string{TenantPrincipal(host.Tenant, host.Name), HostTopic(host.Tenant, host.Name)} {
			if err := add(value, "host "+host.Tenant+"."+host.Name); err != nil {
				return err
			}
		}
		if _, ok := seen[TenantTopic(host.Tenant)]; !ok {
			seen[TenantTopic(host.Tenant)] = "tenant " + host.Tenant
		}
	}
	return nil
}

func ValidateConfig(config ReceiverConfig, terms Terms) error {
	if err := validateNames(config.Receiver, config.Pool); err != nil {
		return err
	}
	if config.Capacity <= 0 || len(config.Adapters) == 0 {
		return errors.New("receiver config needs a capacity and adapters")
	}
	names := map[string]bool{}
	for _, adapter := range config.Adapters {
		if validateNames(adapter.Name) != nil || names[adapter.Name] || adapter.Type == "" {
			return errors.New("adapters need unique valid names and a type")
		}
		names[adapter.Name] = true
		if err := wire.ValidateLabels(adapter.Labels); err != nil {
			return err
		}
		index := slices.IndexFunc(terms.Agents, func(agent AgentTerms) bool { return agent.Name == adapter.Agent })
		if index < 0 {
			return errors.New("receiver agent is outside pool terms")
		}
		if adapter.Credential != "" && !slices.Contains(terms.Agents[index].Credentials, adapter.Credential) {
			return errors.New("receiver credential is outside pool terms")
		}
	}
	return nil
}

func (state State) liveHost(tenant, host, pool string) error {
	principal := TenantPrincipal(tenant, host)
	if _, ok := state.Hosts[principal]; !ok || state.Ended[principal] || !state.Entitlements[tenant][pool] {
		return errors.New("record is not from a live host of a tenant entitled to the pool")
	}
	return nil
}

func (state State) liveMember(receiver, pool string) error {
	config, ok := state.Receivers[receiver]
	if !ok || config.Pool != pool || state.Ended[ReceiverPrincipal(receiver)] {
		return errors.New("record is not from a live member of the pool")
	}
	return nil
}

// receiverRecord checks a receiver record against the run it belongs to.
func (state State) receiverRecord(binding wire.ReceiverBinding, placedPool, runTenant string) error {
	if binding.Pool != placedPool || binding.Tenant != runTenant {
		return errors.New("record names another pool or tenant than its run")
	}
	if err := state.liveMember(binding.Receiver, placedPool); err != nil {
		return err
	}
	if !state.Entitlements[runTenant][placedPool] {
		return errors.New("tenant is not entitled to the pool")
	}
	return nil
}

// AuthorizeSubmission accepts a submission that verifies against its host's
// own key topic, from a live host of a tenant entitled to the pool whose topic
// it arrived on.
func AuthorizeSubmission(state State, keys signed.Keys, encoded []byte, topicPool string) (wire.Submission, error) {
	var submission wire.Submission
	if err := wire.ParseRecord(encoded, keys, &submission); err != nil {
		return wire.Submission{}, err
	}
	if submission.Wherry.Pool != topicPool {
		return wire.Submission{}, errors.New("submission names another pool than its topic")
	}
	if err := state.liveHost(submission.Wherry.Tenant, submission.Wherry.Host, topicPool); err != nil {
		return wire.Submission{}, err
	}
	return submission, nil
}

// AuthorizeCancel accepts a cancel from a live host of the run's own tenant.
func AuthorizeCancel(state State, keys signed.Keys, encoded []byte, topicPool, runTenant string) (wire.Cancel, error) {
	var cancel wire.Cancel
	if err := wire.ParseRecord(encoded, keys, &cancel); err != nil {
		return wire.Cancel{}, err
	}
	if cancel.Pool != topicPool || cancel.Tenant != runTenant {
		return wire.Cancel{}, errors.New("cancel names another pool or tenant than its run")
	}
	if err := state.liveHost(cancel.Tenant, cancel.Host, topicPool); err != nil {
		return wire.Cancel{}, err
	}
	return cancel, nil
}

// AuthorizeStatus accepts a status from a live member of the run's placed pool.
func AuthorizeStatus(state State, keys signed.Keys, encoded []byte, placedPool, runTenant string) (wire.RunStatus, error) {
	var status wire.RunStatus
	if err := wire.ParseRecord(encoded, keys, &status); err != nil {
		return wire.RunStatus{}, err
	}
	if err := state.receiverRecord(status.Wherry.ReceiverBinding, placedPool, runTenant); err != nil {
		return wire.RunStatus{}, err
	}
	return status, nil
}

// AuthorizeChunk accepts a transcript chunk from a live member of the run's placed pool.
func AuthorizeChunk(state State, keys signed.Keys, encoded []byte, placedPool, runTenant string) (wire.Chunk, error) {
	var chunk wire.Chunk
	if err := wire.ParseRecord(encoded, keys, &chunk); err != nil {
		return wire.Chunk{}, err
	}
	if err := state.receiverRecord(chunk.Wherry.ReceiverBinding, placedPool, runTenant); err != nil {
		return wire.Chunk{}, err
	}
	return chunk, nil
}

// AuthorizeResult accepts a result from a live member of the run's placed pool.
func AuthorizeResult(state State, keys signed.Keys, encoded []byte, placedPool, runTenant string) (wire.Result, error) {
	var result wire.Result
	if err := wire.ParseRecord(encoded, keys, &result); err != nil {
		return wire.Result{}, err
	}
	if err := state.receiverRecord(result.Wherry.ReceiverBinding, placedPool, runTenant); err != nil {
		return wire.Result{}, err
	}
	return result, nil
}

// AuthorizeStartFailure accepts a start failure from a live member of the run's placed pool.
func AuthorizeStartFailure(state State, keys signed.Keys, encoded []byte, placedPool, runTenant string) (wire.StartFailure, error) {
	var failure wire.StartFailure
	if err := wire.ParseRecord(encoded, keys, &failure); err != nil {
		return wire.StartFailure{}, err
	}
	if err := state.receiverRecord(failure.Wherry, placedPool, runTenant); err != nil {
		return wire.StartFailure{}, err
	}
	return failure, nil
}

// AuthorizeEcho accepts an echo on feedTenant's feed from a live member of the
// pool it names. The echo is what places a run, so its pool comes from itself.
func AuthorizeEcho(state State, keys signed.Keys, encoded []byte, feedTenant string) (wire.Echo, error) {
	var echo wire.Echo
	if err := wire.ParseRecord(encoded, keys, &echo); err != nil {
		return wire.Echo{}, err
	}
	if echo.Tenant != feedTenant {
		return wire.Echo{}, errors.New("echo names another tenant than its feed")
	}
	if err := state.liveMember(echo.Receiver, echo.Pool); err != nil {
		return wire.Echo{}, err
	}
	if !state.Entitlements[feedTenant][echo.Pool] {
		return wire.Echo{}, errors.New("tenant is not entitled to the pool")
	}
	return echo, nil
}

// AuthorizeAdvertisement accepts an advertisement from a live member of the
// pool it names.
func AuthorizeAdvertisement(state State, keys signed.Keys, encoded []byte) (wire.Advertisement, error) {
	var advertisement wire.Advertisement
	if err := wire.ParseRecord(encoded, keys, &advertisement); err != nil {
		return wire.Advertisement{}, err
	}
	if err := state.liveMember(advertisement.Receiver, advertisement.Pool); err != nil {
		return wire.Advertisement{}, err
	}
	// Hosts seal to the key on the receiver's own topic; an advertisement
	// naming another cannot place work.
	if sealing := keys[advertisement.Receiver].Sealing; sealing == nil || advertisement.SealingKey != signed.SealingKeyID(sealing) {
		return wire.Advertisement{}, errors.New("advertisement names a sealing key the receiver has not registered")
	}
	return advertisement, nil
}

func ValidateReceiverRecord(record Record, receiver string, terms Terms) error {
	if record.Kind != "receiver" || record.Receiver != receiver || record.Config == nil || record.Config.Receiver != receiver {
		return errors.New("configuration belongs to another receiver")
	}
	return ValidateConfig(*record.Config, terms)
}

func subject(record Record) string {
	if record.Receiver != "" {
		return ReceiverPrincipal(record.Receiver)
	}
	return TenantPrincipal(record.Tenant, record.Host)
}

func validateRecord(record Record) error {
	if record.Version != 1 || record.AtMS <= 0 {
		return errors.New("invalid admin record")
	}
	switch record.Kind {
	case "pool":
		if validateNames(record.Pool) != nil || record.Terms == nil || validateTerms(*record.Terms) != nil {
			return errors.New("invalid pool record")
		}
	case "receiver":
		if validateNames(record.Pool, record.Receiver) != nil || record.Config == nil || record.Config.Receiver != record.Receiver || record.Config.Pool != record.Pool {
			return errors.New("invalid receiver record")
		}
	case "host":
		if validateNames(record.Tenant, record.Host) != nil {
			return errors.New("invalid host record")
		}
	case "entitlement":
		if validateNames(record.Tenant, record.Pool) != nil {
			return errors.New("invalid entitlement record")
		}
	case "skip":
		if record.Offset < 0 || !sha256Pattern.MatchString(record.SHA256) {
			return errors.New("invalid skip record")
		}
	case "end":
		if (record.Receiver == "") == (record.Host == "" || record.Tenant == "") || validateNames(record.Receiver+record.Tenant, record.Host+record.Receiver) != nil {
			return errors.New("invalid membership end")
		}
	default:
		return errors.New("unknown admin record kind")
	}
	return nil
}

var sha256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type rawRecord struct {
	Offset int64
	Value  []byte
}

// load folds wherry.admin. An undecodable record is tolerated only when a
// skip record names its offset and hash; a skip naming a decodable record is
// refused, so a skip can never hide state such as a membership end.
func load(raw []rawRecord) (State, error) {
	undecodable := map[int64]string{}
	var records, skips []Record
	for _, entry := range raw {
		record, err := DecodeRecord(entry.Value)
		if err != nil {
			sum := sha256.Sum256(entry.Value)
			undecodable[entry.Offset] = hex.EncodeToString(sum[:])
			continue
		}
		if record.Kind == "skip" {
			skips = append(skips, record)
		} else {
			records = append(records, record)
		}
	}
	for _, skip := range skips {
		// A decodable offset is absent from undecodable, and "" never matches.
		if undecodable[skip.Offset] != skip.SHA256 {
			return State{}, fmt.Errorf("skip record for offset %d names no undecodable record with that hash", skip.Offset)
		}
		delete(undecodable, skip.Offset)
	}
	if len(undecodable) > 0 {
		var uncovered []string
		for _, entry := range raw {
			if hash, ok := undecodable[entry.Offset]; ok {
				uncovered = append(uncovered, fmt.Sprintf("offset=%d size=%d sha256=%s", entry.Offset, len(entry.Value), hash))
			}
		}
		return State{}, fmt.Errorf("wherry.admin has undecodable records; review each and run wherry admin skip: %s", strings.Join(uncovered, "; "))
	}
	return Fold(records)
}

func validateTerms(terms Terms) error {
	if len(terms.Agents) == 0 || terms.SealLifetimeMS <= 0 || terms.DeliveryLimit <= 0 || terms.LockDurationMS <= 0 || terms.ProducerByteRate <= 0 || terms.RequestPercentage <= 0 {
		return errors.New("invalid pool terms")
	}
	if !slices.ContainsFunc(terms.Agents, func(agent AgentTerms) bool { return agent.Name == terms.DefaultAgent }) {
		return errors.New("default agent is outside pool terms")
	}
	for _, agent := range terms.Agents {
		if validateNames(agent.Name) != nil {
			return errors.New("invalid agent name")
		}
		for _, credential := range agent.Credentials {
			if validateNames(credential) != nil {
				return errors.New("invalid credential name")
			}
		}
	}
	return nil
}
