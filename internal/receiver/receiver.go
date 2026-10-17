// Package receiver is the platform-independent receiver core (ADR-0018).
package receiver

import (
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/benemon/wherry/internal/trust"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Handle identifies one platform instance of a run; Format is adapter-private.
type Handle struct {
	Job    string
	Alloc  string
	Format string
	// Commit is the git commit checked out for a run with a git context.
	Commit string
}

// Line is one output line, the byte offset where it starts and the offset after it.
type Line struct {
	Offset int64
	Next   int64
	Data   []byte
}

// Run is the material needed to start one platform job.
type Run struct {
	ID         string
	Turn       int64
	Offer      map[string]string
	Prompt     []byte
	Credential []byte
	Git        *wire.GitContext
}

// ErrGitContext is the error an adapter returns when a run's git context cannot be checked out.
var ErrGitContext = errors.New("git context unavailable")

// Adapter is the platform seam of ADR-0018; implementations hold platform credentials only.
type Adapter interface {
	Platform() string
	Offers(ctx context.Context) ([]map[string]string, error)
	Dispatch(ctx context.Context, run Run) (Handle, error)
	Lookup(ctx context.Context, runID string, turn int64) (Handle, bool, error)
	Follow(ctx context.Context, handle Handle, offset int64, lines chan<- Line) error
	Stop(ctx context.Context, handle Handle) error
	Release(ctx context.Context, runID string, turn int64) error
	Sweep(ctx context.Context) error
}

// Config is the receiver's local configuration.
type Config struct {
	Broker            string            `json:"broker"`
	CAFile            string            `json:"ca_file"`
	KafkaUser         string            `json:"kafka_user"`
	KafkaPasswordFile string            `json:"kafka_password_file"`
	ReceiverID        string            `json:"receiver_id"`
	StateDir          string            `json:"state_dir"`
	UserPublicKey     wire.Bytes        `json:"user_public_key"`
	Capacity          int64             `json:"capacity,omitzero"`
	Labels            map[string]string `json:"labels,omitzero"`
	Adapters          []AdapterConfig   `json:"adapters"`
}

// AdapterConfig configures one adapter instance; Config is decoded by the adapter type.
type AdapterConfig struct {
	Name       string         `json:"name"`
	Type       string         `json:"type"`
	Credential string         `json:"credential,omitzero"`
	Config     jsontext.Value `json:"config"`
}

// Named is a constructed adapter with its configured name and sealed-secret name.
type Named struct {
	Name       string
	Credential string
	// Git is set when the adapter delivers git context parts (ADR-0024).
	Git     bool
	Adapter Adapter
}

// LoadConfig strictly decodes the receiver configuration.
func LoadConfig(path string) (Config, error) {
	encoded, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var config Config
	if err := json.Unmarshal(encoded, &config, json.RejectUnknownMembers(true), jsontext.AllowDuplicateNames(false)); err != nil {
		return Config{}, fmt.Errorf("decode receiver config: %w", err)
	}
	if config.Broker == "" || config.CAFile == "" || config.KafkaUser == "" || config.KafkaPasswordFile == "" ||
		config.ReceiverID == "" || config.StateDir == "" || len(config.UserPublicKey) != ed25519.PublicKeySize {
		return Config{}, errors.New("receiver config requires broker, ca_file, kafka_user, kafka_password_file, receiver_id, state_dir and a 32-byte user_public_key")
	}
	if len(config.Adapters) == 0 {
		return Config{}, errors.New("receiver config requires at least one adapter")
	}
	names := make(map[string]bool, len(config.Adapters))
	for _, adapter := range config.Adapters {
		if adapter.Name == "" || names[adapter.Name] {
			return Config{}, errors.New("adapter names must be non-empty and unique")
		}
		names[adapter.Name] = true
	}
	if config.Capacity == 0 {
		config.Capacity = 2
	}
	if config.Labels == nil {
		config.Labels = map[string]string{}
	}
	return config, nil
}

// Receiver claims tasks for one pool and runs them through its adapters.
type Receiver struct {
	config      Config
	adapters    []Named
	kafkaOpt    []kgo.Opt
	base        *kgo.Client
	userPub     ed25519.PublicKey
	signing     ed25519.PrivateKey
	sealing     *ecdh.PrivateKey
	logger      *log.Logger
	mu          sync.Mutex
	enrolled    *trust.Enrolled
	offers      []wire.Offer
	signers     []ed25519.PublicKey
	inFlight    int64
	credentials map[string]heldCredential
	// revoked is set when refresh last saw this receiver revoked; in-flight runs stop on their next heartbeat.
	revoked bool
	// cancelled holds the runs the user has cancelled, as of the last control topic read.
	cancelled map[string]bool
}

type heldCredential struct {
	version int64
	value   []byte
	exp     time.Time
}

// New loads or creates the receiver's keys and builds its Kafka client; adapters are in configuration order.
func New(config Config, adapters []Named, logger *log.Logger) (*Receiver, error) {
	caPEM, err := os.ReadFile(config.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read CA file: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("CA file contains no certificates")
	}
	password, err := os.ReadFile(config.KafkaPasswordFile)
	if err != nil {
		return nil, fmt.Errorf("read Kafka password: %w", err)
	}
	if err := os.MkdirAll(config.StateDir, 0o700); err != nil {
		return nil, err
	}
	signing, err := loadOrCreate(filepath.Join(config.StateDir, "signing.key"), trust.LoadSigningKeyFile, trust.GenerateSigningKeyFile)
	if err != nil {
		return nil, fmt.Errorf("signing key: %w", err)
	}
	sealing, err := loadOrCreate(filepath.Join(config.StateDir, "sealing.key"), trust.LoadSealingKeyFile, trust.GenerateSealingKeyFile)
	if err != nil {
		return nil, fmt.Errorf("sealing key: %w", err)
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(config.Broker),
		kgo.DialTLSConfig(&tls.Config{RootCAs: roots}),
		kgo.SASL(scram.Auth{User: config.KafkaUser, Pass: strings.TrimRight(string(password), "\r\n")}.AsSha512Mechanism()),
	}
	base, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Receiver{
		config: config, adapters: adapters, kafkaOpt: opts, base: base,
		userPub: ed25519.PublicKey(config.UserPublicKey), signing: signing, sealing: sealing, logger: logger,
		credentials: make(map[string]heldCredential),
	}, nil
}

func loadOrCreate[K any](path string, load func(string) (K, error), create func(string) (K, error)) (K, error) {
	key, err := load(path)
	if errors.Is(err, os.ErrNotExist) {
		return create(path)
	}
	return key, err
}

// Run enrols, publishes capabilities, delivers credentials and claims tasks until ctx ends.
func (r *Receiver) Run(ctx context.Context) error {
	defer r.base.Close()
	if err := r.enrol(ctx); err != nil {
		return err
	}
	// Claiming waits for one read of the control topic, so a cancelled submission is not dispatched on start.
	if err := r.refreshCancelled(ctx); err != nil {
		return err
	}
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		r.every(ctx, capabilityInterval, r.publishCapability)
	}()
	go func() {
		defer wg.Done()
		r.every(ctx, refreshInterval, r.refresh)
	}()
	// A separate step, so a control topic read failure never delays noticing a revocation.
	go func() {
		defer wg.Done()
		r.every(ctx, refreshInterval, r.refreshCancelled)
	}()
	for slot := int64(0); slot < r.config.Capacity; slot++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.claimLoop(ctx)
		}()
	}
	wg.Wait()
	return ctx.Err()
}

func (r *Receiver) every(ctx context.Context, interval time.Duration, step func(context.Context) error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := step(ctx); err != nil && ctx.Err() == nil {
			r.logger.Printf("periodic step: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *Receiver) enrol(ctx context.Context) error {
	if err := r.refresh(ctx); err != nil {
		return err
	}
	if r.current() == nil {
		detected := make([]wire.DetectedAdapter, 0, len(r.adapters))
		for _, named := range r.adapters {
			found, err := named.Adapter.Offers(ctx)
			if err != nil {
				return fmt.Errorf("discover offers for adapter %s: %w", named.Name, err)
			}
			offers := make([]wire.Offer, 0, len(found))
			for _, labels := range found {
				offers = append(offers, wire.Offer{Adapter: named.Name, Labels: labels})
			}
			detected = append(detected, wire.DetectedAdapter{Name: named.Name, Platform: named.Adapter.Platform(), Offers: offers})
		}
		request, err := trust.NewEnrolRequest(r.config.ReceiverID, r.signing, r.sealing, r.config.Labels, detected, time.Now())
		if err != nil {
			return err
		}
		if err := r.base.ProduceSync(ctx, &kgo.Record{Topic: wire.TopicState, Key: []byte(r.config.ReceiverID), Value: request}).FirstErr(); err != nil {
			return fmt.Errorf("publish enrolment request: %w", err)
		}
		r.logger.Printf("enrolment requested: receiver=%s request=%s signing=%s", r.config.ReceiverID, trust.EnvelopeHash(request), wire.Fingerprint(r.signing.Public().(ed25519.PublicKey)))
	}
	for r.current() == nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(enrolPollInterval):
		}
		if err := r.refresh(ctx); err != nil {
			r.logger.Printf("refresh: %v", err)
		}
	}
	r.logger.Printf("enrolled: pool=%s generation=%d", r.current().Pool, r.current().Generation)
	return nil
}

func (r *Receiver) current() *trust.Enrolled {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.enrolled
}

// refresh rebuilds trust state from the append-only log and delivers a newer credential.
func (r *Receiver) refresh(ctx context.Context) error {
	// A bounded read can wait on another holder's open transaction; never longer than this.
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	records, err := readBounded(ctx, r.kafkaOpt, wire.TopicState)
	if err != nil {
		return err
	}
	state := trust.NewState(r.userPub)
	var approved []string
	issued := make(map[string]time.Time)
	for _, record := range records {
		verified, _ := state.Apply(record.Value) // unverifiable records change nothing
		if approval, ok := verified.(*wire.EnrolApproval); ok {
			approved = append(approved, approval.ReceiverID)
			issued[trust.EnvelopeHash(record.Value)] = record.Timestamp
		}
	}
	now := time.Now()
	if err := r.requestRenewal(ctx, state, issued, now); err != nil {
		r.logger.Printf("renew request: %v", err)
	}
	enrolled, err := state.Receiver(r.config.ReceiverID, now)
	if err != nil || !enrolled.SigningPub.Equal(r.signing.Public()) {
		revoked := errors.Is(err, trust.ErrRevoked)
		if revoked {
			r.logger.Printf("revoked at generation %d", state.Floor(r.config.ReceiverID))
		}
		// Expired, revoked or re-keyed: stop claiming, advertising and delivering credentials.
		r.mu.Lock()
		r.enrolled, r.offers, r.signers, r.revoked = nil, nil, nil, revoked
		clear(r.credentials)
		r.mu.Unlock()
		return nil
	}
	if reason := r.grantMismatch(enrolled.Adapters); reason != "" {
		r.logger.Printf("enrolment rejected: %s", reason)
		r.mu.Lock()
		r.enrolled, r.offers, r.signers = nil, nil, nil
		r.mu.Unlock()
		return nil
	}
	ids := make(map[string]bool)
	var signers []ed25519.PublicKey
	for _, id := range approved {
		if ids[id] {
			continue
		}
		peer, err := state.Receiver(id, now)
		if err == nil && peer.Pool == enrolled.Pool {
			ids[id] = true
			signers = append(signers, peer.SigningPub)
		}
	}
	r.mu.Lock()
	r.enrolled, r.signers, r.revoked = &enrolled, signers, false
	r.mu.Unlock()

	r.dropLapsed(state)
	secrets, err := readBounded(ctx, r.kafkaOpt, wire.TopicSecret)
	if err != nil {
		return err
	}
	rejected := false
	for _, named := range r.adapters {
		if named.Credential != "" && !r.openCredential(state, secrets, named.Credential) {
			rejected = true
		}
	}
	if rejected {
		return errors.New("credential rejected") // never wrap: the cause may describe secret material
	}
	return nil
}

// refreshCancelled rebuilds the set of runs the user has cancelled from the control topic.
func (r *Receiver) refreshCancelled(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	records, err := readBounded(ctx, r.kafkaOpt, wire.TopicControl)
	if err != nil {
		return err
	}
	cancelled := make(map[string]bool)
	for _, record := range records {
		if runID, err := trust.VerifyCancel(record.Value, r.userPub); err == nil && string(record.Key) == runID {
			cancelled[runID] = true
		}
	}
	r.mu.Lock()
	r.cancelled = cancelled
	r.mu.Unlock()
	return nil
}

// requestRenewal publishes a renew request for the current generation once less than a third of
// the approval's lifetime is left; expired generations ask too.
// The lifetime starts at the approval record's broker timestamp, which is unsigned and only schedules the request.
func (r *Receiver) requestRenewal(ctx context.Context, state *trust.State, issued map[string]time.Time, now time.Time) error {
	id := r.config.ReceiverID
	current, hash, ok := state.Approval(id)
	if !ok || current.Generation <= state.Floor(id) {
		return nil
	}
	exp := time.UnixMilli(current.ExpMS)
	if exp.Sub(now) >= exp.Sub(issued[hash])/3 {
		return nil
	}
	if pending, ok := state.RenewRequest(id); ok && now.Sub(time.UnixMilli(pending.AtMS)) < renewRequestInterval {
		return nil
	}
	request, err := sign(wire.TypeRenewRequest, &wire.RenewRequest{ReceiverID: id, Generation: current.Generation, AtMS: now.UnixMilli()}, r.signing)
	if err != nil {
		return err
	}
	// Applying first checks the request is signed by the enrolled key; a re-keyed receiver does not ask.
	if _, err := state.Apply(request); err != nil {
		return nil
	}
	if err := r.base.ProduceSync(ctx, &kgo.Record{Topic: wire.TopicState, Key: []byte(id), Value: request}).FirstErr(); err != nil {
		return err
	}
	r.logger.Printf("renewal requested: generation=%d expires=%s", current.Generation, exp.UTC().Format(time.RFC3339))
	return nil
}

// dropLapsed forgets held credentials whose seal is no longer current. It reads only user-signed trust
// state, so a forged secrets record cannot evict a credential, and it runs before the secrets read can fail.
func (r *Receiver) dropLapsed(state *trust.State) {
	for _, named := range r.adapters {
		if named.Credential == "" {
			continue
		}
		r.mu.Lock()
		held, ok := r.credentials[named.Credential]
		dropped := ok && !state.SecretCurrent(r.config.ReceiverID, named.Credential, held.version, time.Now())
		if dropped {
			delete(r.credentials, named.Credential)
		}
		r.mu.Unlock()
		if dropped {
			r.logger.Printf("credential %s version %d no longer current; dropped", named.Credential, held.version)
		}
	}
}

// openCredential holds the newest verified version of a sealed secret; false means rejected.
func (r *Receiver) openCredential(state *trust.State, secrets []*kgo.Record, name string) bool {
	key := r.config.ReceiverID + "/" + name
	var latest []byte
	for _, record := range secrets {
		if string(record.Key) == key {
			latest = record.Value
		}
	}
	if latest == nil {
		return true
	}
	// Open first: an unverified record must not decide which version is current.
	value, version, exp, err := trust.Open(latest, state, r.config.ReceiverID, name, r.sealing, r.userPub, time.Now())
	if err != nil {
		return false
	}
	r.mu.Lock()
	newer := version > r.credentials[name].version
	if newer {
		r.credentials[name] = heldCredential{version: version, value: value, exp: exp}
	}
	r.mu.Unlock()
	if newer {
		r.logger.Printf("credential %s version %d opened", name, version)
	}
	return true
}

// grantMismatch explains why the approved grants do not fit the configured adapters, or returns "".
func (r *Receiver) grantMismatch(grants []wire.AdapterGrant) string {
	for _, grant := range grants {
		named, ok := r.named(grant.Name)
		if !ok {
			return fmt.Sprintf("grant %q names no configured adapter", grant.Name)
		}
		if platform := named.Adapter.Platform(); grant.Platform != platform {
			return fmt.Sprintf("grant %q platform %q does not match adapter platform %q", grant.Name, grant.Platform, platform)
		}
	}
	return ""
}

func (r *Receiver) named(name string) (Named, bool) {
	for _, named := range r.adapters {
		if named.Name == name {
			return named, true
		}
	}
	return Named{}, false
}

func (r *Receiver) publishCapability(ctx context.Context) error {
	for _, named := range r.adapters {
		if err := named.Adapter.Sweep(ctx); err != nil {
			r.logger.Printf("adapter %s sweep: %T", named.Name, err)
		}
	}
	enrolled := r.current()
	if enrolled == nil {
		return nil
	}
	offers := []wire.Offer{}
	for _, named := range r.adapters {
		found, err := named.Adapter.Offers(ctx)
		if err != nil {
			r.logger.Printf("adapter %s offers: %v", named.Name, err)
			continue
		}
		for _, labels := range found {
			offers = append(offers, wire.Offer{Adapter: named.Name, Labels: labels})
		}
	}
	offers = r.filterOffers(enrolled, offers)
	wire.SortOffers(enrolled.Adapters, offers)
	r.mu.Lock()
	r.offers = offers
	inFlight := r.inFlight
	r.mu.Unlock()
	capability := &wire.Capability{
		ReceiverID: r.config.ReceiverID, Labels: enrolled.Labels, Pool: enrolled.Pool, Offers: offers, Selector: enrolled.Selector,
		Capacity: r.config.Capacity, InFlight: inFlight, Generation: enrolled.Generation, AtMS: time.Now().UnixMilli(),
	}
	value, err := sign(wire.TypeCapability, capability, r.signing)
	if err != nil {
		return err
	}
	return r.base.ProduceSync(ctx, &kgo.Record{Topic: wire.TopicCapability, Key: []byte(r.config.ReceiverID), Value: value}).FirstErr()
}

func (r *Receiver) filterOffers(enrolled *trust.Enrolled, offers []wire.Offer) []wire.Offer {
	filtered := make([]wire.Offer, 0, len(offers))
	for _, offer := range offers {
		index := slices.IndexFunc(enrolled.Adapters, func(grant wire.AdapterGrant) bool { return grant.Name == offer.Adapter })
		if index < 0 {
			r.logger.Printf("dropping offer from adapter %s without a grant", offer.Adapter)
			continue
		}
		if outside := outsideKeys(enrolled.Adapters[index], offer); len(outside) > 0 {
			r.logger.Printf("dropping offer outside grant: adapter=%s key=%s", offer.Adapter, strings.Join(outside, ","))
			continue
		}
		filtered = append(filtered, offer)
	}
	return filtered
}

func outsideKeys(grant wire.AdapterGrant, offer wire.Offer) []string {
	var keys []string
	for key, value := range offer.Labels {
		if !slices.Contains(grant.Allowed[key], value) {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	return keys
}

func sign(recordType wire.RecordType, payload any, key ed25519.PrivateKey) ([]byte, error) {
	encoded, err := wire.MarshalPayload(recordType, payload)
	if err != nil {
		return nil, err
	}
	return wire.MarshalEnvelope(wire.Sign(recordType, encoded, key))
}

// readBounded reads committed records from a topic's start to the last stable
// offset captured at call time; aborted transactions (fenced holders) are never seen.
func readBounded(ctx context.Context, opts []kgo.Opt, topic string) ([]*kgo.Record, error) {
	probe, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	admin := kadm.NewClient(probe)
	starts, err := admin.ListStartOffsets(ctx, topic)
	if err == nil {
		err = starts.Error()
	}
	var ends kadm.ListedOffsets
	if err == nil {
		ends, err = admin.ListCommittedOffsets(ctx, topic)
	}
	if err == nil {
		err = ends.Error()
	}
	probe.Close()
	if err != nil {
		return nil, err
	}
	remaining := make(map[int32]int64)
	partitions := make(map[int32]kgo.Offset)
	ends.Each(func(end kadm.ListedOffset) {
		if start, ok := starts.Lookup(topic, end.Partition); ok && start.Offset < end.Offset {
			remaining[end.Partition] = end.Offset
			partitions[end.Partition] = kgo.NewOffset().At(start.Offset)
		}
	})
	if len(remaining) == 0 {
		return nil, nil
	}
	reader, err := kgo.NewClient(append(append([]kgo.Opt(nil), opts...),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		// A trailing abort marker is the last offset below the bound; it must be seen to finish.
		kgo.KeepControlRecords(),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: partitions}))...)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	var records []*kgo.Record
	for len(remaining) > 0 {
		fetches := reader.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := fetches.Err(); err != nil {
			return nil, err
		}
		fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
			end, wanted := remaining[partition.Partition]
			if !wanted {
				return
			}
			for _, record := range partition.Records {
				if record.Offset < end && !record.Attrs.IsControl() {
					records = append(records, record)
				}
				if record.Offset >= end-1 {
					delete(remaining, partition.Partition)
				}
			}
			// Compaction or aborted records can leave nothing to return below the bound.
			if len(partition.Records) == 0 && partition.LastStableOffset >= end {
				delete(remaining, partition.Partition)
			}
		})
	}
	return records, nil
}
