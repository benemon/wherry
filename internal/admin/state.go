package admin

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/benemon/wherry/internal/signed"
)

type AgentTerms struct {
	Name        string   `json:"name" yaml:"name"`
	Credentials []string `json:"credentials" yaml:"credentials"`
}
type Terms struct {
	Agents            []AgentTerms `json:"agents" yaml:"agents"`
	Runtimes          []string     `json:"runtimes" yaml:"runtimes"`
	DefaultAgent      string       `json:"default_agent" yaml:"default_agent"`
	DefaultRuntime    string       `json:"default_runtime" yaml:"default_runtime"`
	SealLifetimeMS    int64        `json:"seal_lifetime_ms" yaml:"seal_lifetime_ms"`
	DeliveryLimit     int32        `json:"delivery_limit" yaml:"delivery_limit"`
	LockDurationMS    int64        `json:"lock_duration_ms" yaml:"lock_duration_ms"`
	ProducerByteRate  int64        `json:"producer_byte_rate" yaml:"producer_byte_rate"`
	RequestPercentage float64      `json:"request_percentage" yaml:"request_percentage"`
}
type ReceiverConfig struct {
	Receiver string       `json:"receiver" yaml:"receiver"`
	Pool     string       `json:"pool" yaml:"pool"`
	Agents   []AgentTerms `json:"agents" yaml:"agents"`
	Runtimes []string     `json:"runtimes" yaml:"runtimes"`
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
	if len(config.Agents) == 0 || len(config.Runtimes) == 0 {
		return errors.New("receiver config has no offers")
	}
	allowedRuntimes := map[string]bool{}
	for _, runtime := range terms.Runtimes {
		allowedRuntimes[runtime] = true
	}
	allowedAgents := map[string]map[string]bool{}
	for _, agent := range terms.Agents {
		allowedAgents[agent.Name] = map[string]bool{}
		for _, credential := range agent.Credentials {
			allowedAgents[agent.Name][credential] = true
		}
	}
	for _, runtime := range config.Runtimes {
		if !allowedRuntimes[runtime] {
			return errors.New("receiver runtime is outside pool terms")
		}
	}
	for _, agent := range config.Agents {
		credentials, ok := allowedAgents[agent.Name]
		if !ok {
			return errors.New("receiver agent is outside pool terms")
		}
		for _, credential := range agent.Credentials {
			if !credentials[credential] {
				return errors.New("receiver credential is outside pool terms")
			}
		}
	}
	return nil
}

// AuthorizeTask accepts a task that verifies against its host's own key topic,
// from a live host of the named tenant, entitled to the pool whose topic it
// arrived on.
func AuthorizeTask(state State, keys signed.Keys, encoded []byte, topicPool string) error {
	task, err := signed.ParseTask(encoded, keys)
	if err != nil {
		return err
	}
	principal := TenantPrincipal(task.Tenant, task.Host)
	if _, ok := state.Hosts[principal]; !ok || state.Ended[principal] || task.Pool != topicPool || !state.Entitlements[task.Tenant][topicPool] {
		return errors.New("task is not authorised")
	}
	return nil
}

// AuthorizeStatus accepts a status from a live member of the run's placed pool
// whose tenant is entitled to that pool.
func AuthorizeStatus(state State, keys signed.Keys, encoded []byte, placedPool, runTenant string) error {
	status, err := signed.ParseStatus(encoded, keys, placedPool, runTenant)
	if err != nil {
		return err
	}
	config, ok := state.Receivers[status.Receiver]
	if !ok || config.Pool != placedPool || state.Ended[ReceiverPrincipal(status.Receiver)] || !state.Entitlements[status.Tenant][placedPool] {
		return errors.New("status is not authorised")
	}
	return nil
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
	case "end":
		if (record.Receiver == "") == (record.Host == "" || record.Tenant == "") || validateNames(record.Receiver+record.Tenant, record.Host+record.Receiver) != nil {
			return errors.New("invalid membership end")
		}
	default:
		return errors.New("unknown admin record kind")
	}
	return nil
}

func validateTerms(terms Terms) error {
	if len(terms.Agents) == 0 || len(terms.Runtimes) == 0 || terms.SealLifetimeMS <= 0 || terms.DeliveryLimit <= 0 || terms.LockDurationMS <= 0 || terms.ProducerByteRate <= 0 || terms.RequestPercentage <= 0 {
		return errors.New("invalid pool terms")
	}
	if !slices.ContainsFunc(terms.Agents, func(agent AgentTerms) bool { return agent.Name == terms.DefaultAgent }) || !slices.Contains(terms.Runtimes, terms.DefaultRuntime) {
		return errors.New("defaults are outside pool terms")
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
	for _, runtime := range terms.Runtimes {
		if validateNames(runtime) != nil {
			return errors.New("invalid runtime name")
		}
	}
	return nil
}
