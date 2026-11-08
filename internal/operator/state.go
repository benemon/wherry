package operator

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"gopkg.in/yaml.v3"
)

type AgentTerms struct {
	Name        string            `json:"name" yaml:"name"`
	Credentials []AgentCredential `json:"credentials" yaml:"credentials"`
	// Models are the models a task may name for the agent, and DefaultModel the one a task naming none runs. Models
	// without a DefaultModel make every task name one; neither leaves the model to the agent's profiles.
	Models       []string `json:"models,omitzero" yaml:"models"`
	DefaultModel string   `json:"default_model,omitzero" yaml:"default_model"`
}

// AgentCredential names an agent target and display description; Required defaults to true and refuses submission without a source.
type AgentCredential struct {
	wire.TaskCredential `yaml:",inline"`
	Description         string `json:"description,omitzero" yaml:"description,omitempty"`
	Required            *bool  `json:"required,omitzero" yaml:"required"`
}

// Optional reports whether a run goes without the credential when its host has no source for it.
func (c AgentCredential) Optional() bool { return c.Required != nil && !*c.Required }

// Targets are the agent credentials as task credentials, for the checks they share.
func (agent AgentTerms) Targets() []wire.TaskCredential {
	targets := make([]wire.TaskCredential, 0, len(agent.Credentials))
	for _, credential := range agent.Credentials {
		targets = append(targets, credential.TaskCredential)
	}
	return targets
}

type Terms struct {
	Agents            []AgentTerms `json:"agents" yaml:"agents"`
	DefaultAgent      string       `json:"default_agent,omitzero" yaml:"default_agent"`
	StartByLimitMS    Millis       `json:"start_by_limit_ms,omitzero" yaml:"start_by_limit"`
	DeliveryLimit     int32        `json:"delivery_limit,omitzero" yaml:"delivery_limit"`
	LockDurationMS    Millis       `json:"lock_duration_ms,omitzero" yaml:"lock_duration"`
	ProducerByteRate  int64        `json:"producer_byte_rate" yaml:"producer_byte_rate"`
	RequestPercentage float64      `json:"request_percentage" yaml:"request_percentage"`
	OutputLimit       int64        `json:"output_limit,omitzero" yaml:"output_limit"`
	// ReplyLimit bounds the redacted reply a result returns; a longer one is cut to it.
	ReplyLimit int64 `json:"reply_limit,omitzero" yaml:"reply_limit"`
	// VCS holds the hosts, as wire.GitInput.VCSHost names them, a git input may clone from; none allows no git input.
	VCS []string `json:"vcs,omitzero" yaml:"vcs"`
	// Storage is the pool's S3 lane; nil allows no S3 input or output.
	Storage *Storage `json:"storage,omitzero" yaml:"storage"`
	// RecordLimit is the tasks topic's record limit as pool apply reads it from the broker; no pool file sets it.
	RecordLimit int64 `json:"record_limit,omitzero" yaml:"-"`
}

// Millis is a duration in milliseconds, which pool YAML writes as a Go duration such as 15s or 72h.
type Millis int64

func (m *Millis) UnmarshalYAML(node *yaml.Node) error {
	var text string
	if err := node.Decode(&text); err != nil {
		return err
	}
	duration, err := time.ParseDuration(text)
	if err != nil {
		return err
	}
	*m = Millis(duration.Milliseconds())
	return nil
}

// StartByLimit is StartByLimitMS as a duration.
func (terms Terms) StartByLimit() time.Duration {
	return time.Duration(terms.StartByLimitMS) * time.Millisecond
}

// LockDuration is LockDurationMS as a duration.
func (terms Terms) LockDuration() time.Duration {
	return time.Duration(terms.LockDurationMS) * time.Millisecond
}

// Storage is the S3-compatible endpoint, an origin with no path, and the bucket a pool's S3 inputs and outputs live
// in. The endpoint is https, or http when its host is the literal 127.0.0.1 or [::1], whose traffic stays on the machine.
type Storage struct {
	Endpoint string `json:"endpoint" yaml:"endpoint"`
	Bucket   string `json:"bucket" yaml:"bucket"`
}

// bucketName is an S3 bucket name as every S3-compatible store accepts one.
var bucketName = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

// Secure reports whether the endpoint is https.
func (storage Storage) Secure() bool {
	return strings.HasPrefix(storage.Endpoint, "https://")
}

// Host is the endpoint's host and port, the port defaulting to the scheme's.
func (storage Storage) Host() string {
	host, port := strings.TrimPrefix(strings.TrimPrefix(storage.Endpoint, "https://"), "http://"), "80"
	if storage.Secure() {
		port = "443"
	}
	if _, _, err := net.SplitHostPort(host); err != nil {
		host = net.JoinHostPort(strings.Trim(host, "[]"), port)
	}
	return host
}

// Serves reports whether u has the endpoint's scheme, host and port.
func (storage Storage) Serves(u *url.URL) bool {
	scheme, port := "http", "80"
	if storage.Secure() {
		scheme, port = "https", "443"
	}
	return u.Scheme == scheme && net.JoinHostPort(u.Hostname(), cmp.Or(u.Port(), port)) == storage.Host()
}

func (storage Storage) validate() error {
	host, secure := strings.CutPrefix(storage.Endpoint, "https://")
	if secure && !wire.ValidVCSHost(host) || !secure && !loopbackHTTP(storage.Endpoint) {
		return errors.New("storage endpoint must be https:// and a lowercase host name, or http:// and 127.0.0.1 or [::1], optionally with :port, and nothing else")
	}
	if !bucketName.MatchString(storage.Bucket) || strings.Contains(storage.Bucket, "..") {
		return errors.New("storage bucket must be 3 to 63 lowercase letters, digits, dots and hyphens")
	}
	return nil
}

// loopbackHTTP reports whether endpoint is http:// and the literal 127.0.0.1 or [::1], optionally with a port of 1 to
// 65535. A name that resolves to loopback does not count: its resolution is outside the pool's terms.
func loopbackHTTP(endpoint string) bool {
	for _, origin := range []string{"http://127.0.0.1", "http://[::1]"} {
		if port, ok := strings.CutPrefix(endpoint, origin); ok && (port == "" || strings.HasPrefix(port, ":") && wire.ValidVCSHost("127.0.0.1"+port)) {
			return true
		}
	}
	return false
}

// The Default values are a pool's terms where its terms leave them out; MaxRunDurationMS caps a run after its
// task_started_by.
const (
	DefaultDeliveryLimit  = 5
	DefaultLockDurationMS = 15000
	DefaultOutputLimit    = 8 << 20
	DefaultReplyLimit     = 1 << 20
	DefaultStartByLimitMS = 72 * 60 * 60 * 1000
	MaxRunDurationMS      = 6 * 60 * 60 * 1000
	// MaxPresign is the longest lifetime S3 SigV4 allows a presigned URL.
	MaxPresign = 7 * 24 * time.Hour
)

func withDefaults(terms Terms) Terms {
	if terms.DefaultAgent == "" && len(terms.Agents) == 1 {
		terms.DefaultAgent = terms.Agents[0].Name
	}
	terms.Agents = slices.Clone(terms.Agents)
	for i, agent := range terms.Agents {
		if agent.DefaultModel == "" && len(agent.Models) == 1 {
			terms.Agents[i].DefaultModel = agent.Models[0]
		}
	}
	if terms.DeliveryLimit == 0 {
		terms.DeliveryLimit = DefaultDeliveryLimit
	}
	if terms.LockDurationMS == 0 {
		terms.LockDurationMS = DefaultLockDurationMS
	}
	if terms.StartByLimitMS == 0 {
		terms.StartByLimitMS = DefaultStartByLimitMS
	}
	if terms.OutputLimit == 0 {
		terms.OutputLimit = DefaultOutputLimit
	}
	// validateTerms requires the reply limit to be at most the output limit.
	if terms.ReplyLimit == 0 {
		terms.ReplyLimit = min(DefaultReplyLimit, terms.OutputLimit)
	}
	return terms
}

// ReceiverConfig is the receiver configuration recorded on wherry.operator.
type ReceiverConfig struct {
	Receiver string            `json:"receiver"`
	Pool     string            `json:"pool"`
	Profiles []ProfileSettings `json:"profiles"`
	// PluginSHA256 maps each plugin type to the lowercase hex SHA-256 its binary must have; the receiver refuses to
	// start a plugin whose type has none.
	PluginSHA256 map[string]string `json:"plugin_sha256"`
	// Broker is the broker address the receiver's bundle names; it is not recorded.
	Broker string `json:"-"`
}

// ProfileSettings configures a profile and its replica count.
type ProfileSettings struct {
	Name     string            `json:"name"`
	Plugin   string            `json:"plugin"`
	Agent    string            `json:"agent"`
	Replicas int               `json:"replicas,omitzero" yaml:"replicas,omitempty"`
	Labels   map[string]string `json:"labels,omitzero"`
	Config   map[string]any    `json:"config"`
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
	// At is the record's offset on wherry.operator, which load sets; offsets, never clocks, order operator requests.
	At int64 `json:"-"`
}
type Host struct{ Tenant, Name string }
type State struct {
	Tenants           map[string]bool // false is live; true is ended.
	EndedEntitlements map[string]map[string]bool
	Pools             map[string]Terms
	Receivers         map[string]ReceiverConfig
	Hosts             map[string]Host
	Entitlements      map[string]map[string]bool
	Ended             map[string]bool
	Revoking          map[string]bool
	Suspended         map[string]bool
	// RotateRequested is the wherry.operator offset of each pool's latest rotate-key record.
	RotateRequested map[string]int64
	// SuspendChanged is the wherry.operator offset of each pool's latest suspend or resume record.
	SuspendChanged map[string]int64
}

func NewState() State {
	return State{Tenants: map[string]bool{}, EndedEntitlements: map[string]map[string]bool{}, Pools: map[string]Terms{}, Receivers: map[string]ReceiverConfig{}, Hosts: map[string]Host{}, Entitlements: map[string]map[string]bool{}, Ended: map[string]bool{}, Revoking: map[string]bool{}, Suspended: map[string]bool{}, RotateRequested: map[string]int64{}, SuspendChanged: map[string]int64{}}
}

// RecordVersion is the wherry.operator record version this release writes and the highest it reads.
const RecordVersion = 1

func EncodeRecord(record Record) ([]byte, error) {
	if record.Version == 0 {
		record.Version = RecordVersion
	}
	if record.AtMS == 0 {
		record.AtMS = time.Now().UnixMilli()
	}
	if err := validateRecord(record); err != nil {
		return nil, err
	}
	return json.Marshal(record, json.Deterministic(true))
}

// DecodeRecord ignores unknown members; a record with a version above RecordVersion is ErrNewerRecord.
func DecodeRecord(encoded []byte) (Record, error) {
	var version struct {
		V int `json:"v"`
	}
	if err := json.Unmarshal(encoded, &version, signed.Tolerant); err != nil {
		return Record{}, fmt.Errorf("decode operator record: %w", err)
	}
	if version.V > RecordVersion {
		return Record{}, fmt.Errorf("%w: record version %d", ErrNewerRecord, version.V)
	}
	var record Record
	if err := json.Unmarshal(encoded, &record, signed.Tolerant); err != nil {
		return Record{}, fmt.Errorf("decode operator record: %w", err)
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
	case "tenant":
		if _, exists := state.Tenants[record.Tenant]; exists {
			return errors.New("tenant exists; a tenant name cannot return")
		}
		state.Tenants[record.Tenant] = false
	case "pool":
		state.Pools[record.Pool] = withDefaults(*record.Terms)
	case "receiver":
		if _, ok := state.Pools[record.Pool]; !ok {
			return errors.New("receiver names unknown pool")
		}
		if previous, ok := state.Receivers[record.Receiver]; (ok && previous.Pool != record.Pool) || state.Ended[ReceiverPrincipal(record.Receiver)] {
			return errors.New("receiver cannot move pool or return after its membership end")
		}
		state.Receivers[record.Receiver] = *record.Config
	case "host":
		if err := state.LiveTenant(record.Tenant); err != nil {
			return err
		}
		if state.Ended[TenantPrincipal(record.Tenant, record.Host)] {
			return errors.New("host cannot return after its membership end")
		}
		state.Hosts[TenantPrincipal(record.Tenant, record.Host)] = Host{record.Tenant, record.Host}
	case "entitlement":
		if err := state.LiveTenant(record.Tenant); err != nil {
			return err
		}
		if _, ok := state.Pools[record.Pool]; !ok {
			return errors.New("entitlement names unknown pool")
		}
		if state.Entitlements[record.Tenant] == nil {
			state.Entitlements[record.Tenant] = map[string]bool{}
		}
		state.Entitlements[record.Tenant][record.Pool] = true
		delete(state.EndedEntitlements[record.Tenant], record.Pool)
		if len(state.EndedEntitlements[record.Tenant]) == 0 {
			delete(state.EndedEntitlements, record.Tenant)
		}
	case "end":
		if record.Receiver == "" && record.Host == "" {
			if err := state.LiveTenant(record.Tenant); err != nil {
				return err
			}
			if record.Pool != "" {
				if !state.Entitlements[record.Tenant][record.Pool] {
					return errors.New("end names absent entitlement")
				}
				delete(state.Entitlements[record.Tenant], record.Pool)
				if len(state.Entitlements[record.Tenant]) == 0 {
					delete(state.Entitlements, record.Tenant)
				}
				if state.EndedEntitlements[record.Tenant] == nil {
					state.EndedEntitlements[record.Tenant] = map[string]bool{}
				}
				state.EndedEntitlements[record.Tenant][record.Pool] = true
			} else {
				if len(state.Entitlements[record.Tenant]) != 0 {
					return errors.New("tenant still has entitlements")
				}
				for principal, host := range state.Hosts {
					if host.Tenant == record.Tenant && !state.Ended[principal] {
						return errors.New("tenant still has hosts")
					}
				}
				state.Tenants[record.Tenant] = true
			}
		} else {
			state.Ended[subject(record)] = true
		}
	case "revoking":
		state.Revoking[subject(record)] = true
	case "suspend", "resume", "rotate-key":
		if _, ok := state.Pools[record.Pool]; !ok {
			return errors.New(record.Kind + " names unknown pool")
		}
		switch record.Kind {
		case "suspend":
			state.Suspended[record.Pool], state.SuspendChanged[record.Pool] = true, record.At
		case "resume":
			delete(state.Suspended, record.Pool)
			state.SuspendChanged[record.Pool] = record.At
		default:
			state.RotateRequested[record.Pool] = record.At
		}
	}
	return nil
}

func (state State) LiveTenant(tenant string) error {
	ended, exists := state.Tenants[tenant]
	if !exists {
		return errors.New("tenant absent; use tenant add first")
	}
	if ended {
		return errors.New("tenant has ended")
	}
	return nil
}

func (state State) CheckNames() error {
	seen := map[string]string{OperatorTopic: "operator topic"}
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
		for _, value := range []string{PoolEventsTopic(pool), PoolGroup(pool), TransactionalPrefix(pool)} {
			if err := add(value, "pool "+pool); err != nil {
				return err
			}
		}
	}
	for receiver := range state.Receivers {
		if err := validateNames(receiver); err != nil {
			return err
		}
		for _, value := range []string{ReceiverPrincipal(receiver), ReceiverTopic(receiver), ReceiverProgressTopic(receiver)} {
			if err := add(value, "receiver "+receiver); err != nil {
				return err
			}
		}
	}
	for tenant := range state.Tenants {
		if err := validateNames(tenant); err != nil {
			return err
		}
		for _, topic := range []string{TenantTopic(tenant), TenantTranscriptTopic(tenant)} {
			if err := add(topic, "tenant "+tenant); err != nil {
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
	}
	return nil
}

// pluginSHA256 is a pinned plugin binary's SHA-256 as the release's checksums.txt writes it.
var pluginSHA256 = regexp.MustCompile(`^[0-9a-f]{64}$`)

func ValidateConfig(config ReceiverConfig, terms Terms) error {
	if err := validateNames(config.Receiver, config.Pool); err != nil {
		return err
	}
	if len(config.Profiles) == 0 {
		return errors.New("receiver config needs profiles")
	}
	for kind, sum := range config.PluginSHA256 {
		if !wire.ValidName(kind) || !pluginSHA256.MatchString(sum) {
			return fmt.Errorf("plugin SHA-256 %q=%q needs a valid plugin type and 64 lowercase hex digits", kind, sum)
		}
	}
	names := map[string]bool{}
	for _, profile := range config.Profiles {
		if profile.Replicas < 0 {
			return errors.New("profile replicas must not be negative")
		}
		if validateNames(profile.Name) != nil || names[profile.Name] || !wire.ValidName(profile.Plugin) {
			return errors.New("profiles need unique valid names and a valid plugin")
		}
		if name, ok := profile.Config["credential"]; ok {
			value, valid := name.(string)
			if !valid || !wire.ValidName(value) {
				return fmt.Errorf("credential %q is not a valid name", name)
			}
		}
		names[profile.Name] = true
		if _, ok := config.PluginSHA256[profile.Plugin]; !ok {
			return fmt.Errorf("plugin type %s has no pinned SHA-256; set plugin_sha256.%s in the receiver's file and run receiver apply", profile.Plugin, profile.Plugin)
		}
		if err := wire.ValidateLabels(profile.Labels); err != nil {
			return err
		}
		if !slices.ContainsFunc(terms.Agents, func(agent AgentTerms) bool { return agent.Name == profile.Agent }) {
			return errors.New("receiver agent is outside pool terms")
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

// poolRecord checks a receiver record read with no run tenant, such as on a pool's events: it names the reader's
// pool, comes from a live member of it, and declares a tenant entitled to it. The record binds its run's tenant
// through the task hash, which the reader matches downstream.
func (state State) poolRecord(binding wire.ReceiverBinding, pool string) error {
	if binding.Pool != pool {
		return errors.New("record names another pool than its reader")
	}
	if err := state.liveMember(binding.Receiver, pool); err != nil {
		return err
	}
	if !state.Entitlements[binding.Tenant][pool] {
		return errors.New("tenant is not entitled to the pool")
	}
	return nil
}

// AuthorizeSubmission accepts a submission that verifies against its host's
// own key topic, from a live host of a tenant entitled to the pool whose topic
// it arrived on.
func AuthorizeSubmission(state State, keys signed.Keys, encoded []byte, topicPool string) (wire.Submission, error) {
	return authorizeSubmission(wire.ParseRecord, state, keys, encoded, topicPool)
}

func authorizeSubmission(parse func([]byte, signed.Keys, wire.Record) error, state State, keys signed.Keys, encoded []byte, topicPool string) (wire.Submission, error) {
	var submission wire.Submission
	if err := parse(encoded, keys, &submission); err != nil {
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
// parse is wire.ParseRecord for a reader that admits work, and
// wire.ParseHistory for a host reading its tenant feed.
func AuthorizeStatus(parse func([]byte, signed.Keys, wire.Record) error, state State, keys signed.Keys, encoded []byte, placedPool, runTenant string) (wire.RunStatus, error) {
	var status wire.RunStatus
	if err := parse(encoded, keys, &status); err != nil {
		return wire.RunStatus{}, err
	}
	if err := state.receiverRecord(status.Wherry.ReceiverBinding, placedPool, runTenant); err != nil {
		return wire.RunStatus{}, err
	}
	return status, nil
}

// AuthorizePoolStatus accepts a status from a live member of pool for a tenant entitled to it.
func AuthorizePoolStatus(parse func([]byte, signed.Keys, wire.Record) error, state State, keys signed.Keys, encoded []byte, pool string) (wire.RunStatus, error) {
	var status wire.RunStatus
	if err := parse(encoded, keys, &status); err != nil {
		return wire.RunStatus{}, err
	}
	if err := state.poolRecord(status.Wherry.ReceiverBinding, pool); err != nil {
		return wire.RunStatus{}, err
	}
	return status, nil
}

// AuthorizeChunk accepts a transcript chunk from a live member of the run's placed pool.
func AuthorizeChunk(parse func([]byte, signed.Keys, wire.Record) error, state State, keys signed.Keys, encoded []byte, placedPool, runTenant string) (wire.Chunk, error) {
	var chunk wire.Chunk
	if err := parse(encoded, keys, &chunk); err != nil {
		return wire.Chunk{}, err
	}
	if err := state.receiverRecord(chunk.Wherry.ReceiverBinding, placedPool, runTenant); err != nil {
		return wire.Chunk{}, err
	}
	return chunk, nil
}

// AuthorizeResult accepts a result from a live member of the run's placed pool.
func AuthorizeResult(parse func([]byte, signed.Keys, wire.Record) error, state State, keys signed.Keys, encoded []byte, placedPool, runTenant string) (wire.Result, error) {
	var result wire.Result
	if err := parse(encoded, keys, &result); err != nil {
		return wire.Result{}, err
	}
	if err := state.receiverRecord(result.Wherry.ReceiverBinding, placedPool, runTenant); err != nil {
		return wire.Result{}, err
	}
	return result, nil
}

// AuthorizeOutput accepts an output chunk from a live member of the run's placed pool.
func AuthorizeOutput(parse func([]byte, signed.Keys, wire.Record) error, state State, keys signed.Keys, encoded []byte, placedPool, runTenant string) (wire.Output, error) {
	var output wire.Output
	if err := parse(encoded, keys, &output); err != nil {
		return wire.Output{}, err
	}
	if err := state.receiverRecord(output.Wherry.ReceiverBinding, placedPool, runTenant); err != nil {
		return wire.Output{}, err
	}
	return output, nil
}

// AuthorizePoolStartFailure accepts a start failure from a live member of pool for a tenant entitled to it.
func AuthorizePoolStartFailure(parse func([]byte, signed.Keys, wire.Record) error, state State, keys signed.Keys, encoded []byte, pool string) (wire.StartFailure, error) {
	var failure wire.StartFailure
	if err := parse(encoded, keys, &failure); err != nil {
		return wire.StartFailure{}, err
	}
	if err := state.poolRecord(failure.Wherry, pool); err != nil {
		return wire.StartFailure{}, err
	}
	return failure, nil
}

// AuthorizeEcho accepts an echo on feedTenant's feed from a live member of the
// pool it names. The echo is what places a run, so its pool comes from itself.
func AuthorizeEcho(parse func([]byte, signed.Keys, wire.Record) error, state State, keys signed.Keys, encoded []byte, feedTenant string) (wire.Echo, error) {
	var echo wire.Echo
	if err := parse(encoded, keys, &echo); err != nil {
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
	return advertisement, nil
}

func ValidateReceiverRecord(record Record, receiver string, terms Terms) error {
	if record.Kind != "receiver" || record.Receiver != receiver || record.Config == nil || record.Config.Receiver != receiver {
		return errors.New("configuration belongs to another receiver")
	}
	return ValidateConfig(*record.Config, terms)
}

func subject(record Record) string {
	if record.Receiver == "" && record.Host == "" {
		if record.Pool != "" {
			return "entitlement." + record.Tenant + "." + record.Pool
		}
		return "tenant." + record.Tenant
	}
	if record.Receiver != "" {
		return ReceiverPrincipal(record.Receiver)
	}
	return TenantPrincipal(record.Tenant, record.Host)
}

func validateRecord(record Record) error {
	if record.Version != RecordVersion || record.AtMS <= 0 {
		return errors.New("invalid operator record")
	}
	switch record.Kind {
	case "tenant":
		if validateNames(record.Tenant) != nil || record.Pool != "" || record.Host != "" || record.Receiver != "" || record.Terms != nil || record.Config != nil {
			return errors.New("invalid tenant record")
		}
	case "pool":
		if validateNames(record.Pool) != nil || record.Terms == nil {
			return errors.New("invalid pool record")
		}
		if err := validateTerms(withDefaults(*record.Terms)); err != nil {
			return fmt.Errorf("invalid pool record: %w", err)
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
		if record.Terms != nil || record.Config != nil {
			return errors.New("invalid end record")
		}
		switch {
		case record.Receiver != "":
			if validateNames(record.Receiver) != nil || record.Tenant != "" || record.Host != "" || record.Pool != "" {
				return errors.New("invalid receiver end")
			}
		case record.Host != "":
			if validateNames(record.Tenant, record.Host) != nil || record.Pool != "" {
				return errors.New("invalid host end")
			}
		default:
			if validateNames(record.Tenant) != nil || record.Pool != "" && validateNames(record.Pool) != nil {
				return errors.New("invalid tenant or entitlement end")
			}
		}
	case "revoking":
		if record.Receiver != "" && (record.Tenant != "" || record.Host != "" || validateNames(record.Receiver) != nil) ||
			record.Receiver == "" && validateNames(record.Tenant, record.Host) != nil ||
			record.Pool != "" || record.Terms != nil || record.Config != nil {
			return errors.New("invalid revoking record")
		}
	case "suspend", "resume", "rotate-key":
		if validateNames(record.Pool) != nil || record.Tenant != "" || record.Host != "" || record.Receiver != "" || record.Terms != nil || record.Config != nil {
			return errors.New("invalid " + record.Kind + " record")
		}
	default:
		return errors.New("unknown operator record kind")
	}
	return nil
}

type rawRecord struct {
	Offset int64
	Value  []byte
}

// ErrNewerRecord is a wherry.operator record whose version is above RecordVersion, written by a later release.
var ErrNewerRecord = errors.New("wherry.operator holds records a later wherry release wrote")

// load folds wherry.operator, which is append-only and read from offset 0, so an
// undecodable record leaves no state this release can trust.
func load(raw []rawRecord) (State, error) {
	var records []Record
	var undecodable []string
	allNewer := true
	for _, entry := range raw {
		record, err := DecodeRecord(entry.Value)
		if err != nil {
			sum := sha256.Sum256(entry.Value)
			undecodable = append(undecodable, fmt.Sprintf("offset=%d size=%d sha256=%s", entry.Offset, len(entry.Value), hex.EncodeToString(sum[:])))
			allNewer = allNewer && errors.Is(err, ErrNewerRecord)
			continue
		}
		record.At = entry.Offset
		records = append(records, record)
	}
	if len(undecodable) > 0 {
		if allNewer {
			return State{}, fmt.Errorf("%w; upgrade this wherry: %s", ErrNewerRecord, strings.Join(undecodable, "; "))
		}
		return State{}, fmt.Errorf("this wherry release cannot read wherry.operator; reset the deployment: %s", strings.Join(undecodable, "; "))
	}
	return Fold(records)
}

func validateTerms(terms Terms) error {
	if len(terms.Agents) == 0 || terms.DeliveryLimit <= 0 || terms.LockDurationMS <= 0 || terms.ProducerByteRate <= 0 || terms.RequestPercentage <= 0 ||
		terms.StartByLimitMS < 0 || terms.RecordLimit < 0 || terms.OutputLimit < 0 || terms.ReplyLimit < 0 {
		return errors.New("invalid pool terms")
	}
	// The outcome file is bounded by the output limit, so it can hold a reply at the reply limit.
	if terms.ReplyLimit > terms.OutputLimit {
		return errors.New("reply limit exceeds output limit")
	}
	if !slices.ContainsFunc(terms.Agents, func(agent AgentTerms) bool { return agent.Name == terms.DefaultAgent }) {
		return errors.New("default agent is outside pool terms")
	}
	for i, host := range terms.VCS {
		if !wire.ValidVCSHost(host) || slices.Contains(terms.VCS[:i], host) {
			return errors.New("vcs hosts must be unique lowercase host names, each optionally with :port")
		}
	}
	if terms.Storage != nil {
		if err := terms.Storage.validate(); err != nil {
			return err
		}
	}
	for _, agent := range terms.Agents {
		if validateNames(agent.Name) != nil {
			return errors.New("invalid agent name")
		}
		if agent.DefaultModel != "" && !slices.Contains(agent.Models, agent.DefaultModel) {
			return errors.New("an agent's default_model must be one of its models")
		}
		if err := wire.ValidateCredentials(agent.Targets()); err != nil {
			return fmt.Errorf("agent %s: %w", agent.Name, err)
		}
		for _, credential := range agent.Credentials {
			if err := ValidateDescription(credential.Description); err != nil {
				return fmt.Errorf("agent %s credential %s: %w", agent.Name, credential.Name, err)
			}
		}
	}
	return nil
}

// ValidateDescription checks the display description of an agent credential.
func ValidateDescription(description string) error {
	invalid := !utf8.ValidString(description) || utf8.RuneCountInString(description) > 80
	for _, r := range description {
		invalid = invalid || unicode.IsControl(r) || r == '\u2028' || r == '\u2029'
	}
	if invalid {
		return errors.New("invalid description")
	}
	return nil
}
