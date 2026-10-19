// Package receiver is the platform-independent receiver core (ADR-0018).
package receiver

import (
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"log"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/trust"
	"github.com/benemon/wherry/internal/wire"
	"github.com/twmb/franz-go/pkg/kerr"
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

// ErrConfigChanged is returned by Run when wherry.admin holds a configuration other than the one it started with.
var ErrConfigChanged = errors.New("receiver configuration changed")

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

// Named is a constructed adapter with its configured name, the agent it launches and that agent's credential.
type Named struct {
	Name       string
	Agent      string
	Credential string
	// Git is set when the adapter delivers git context parts (ADR-0024).
	Git     bool
	Adapter Adapter
}

// AdapterFactory builds one adapter from its settings in the receiver's configuration record.
type AdapterFactory func(admin.AdapterSettings) (Named, error)

// Receiver claims tasks for one pool and runs them through its adapters.
type Receiver struct {
	id       string
	kafkaOpt []kgo.Opt
	base     *kgo.Client
	signing  ed25519.PrivateKey
	sealing  *ecdh.PrivateKey
	factory  AdapterFactory
	logger   *log.Logger
	// config and adapters are set by configure before any claim and never change.
	config   admin.ReceiverConfig
	adapters []Named
	mu       sync.Mutex
	state    admin.State
	// peers holds the pool's receiver keys, this receiver's included; hosts the keys of entitled tenants' hosts.
	peers    signed.Keys
	hosts    signed.Keys
	offers   []wire.Offer
	inFlight int64
	// deniedSince is when an authorisation error was first seen; zero while access holds.
	deniedSince time.Time
	// cancelled holds, per run, the lowest turn a host of the run's tenant has cancelled.
	cancelled map[string]int64
}

// New loads or creates the receiver's keys in stateDir and builds its Kafka client from bundle.
func New(bundle admin.Bundle, stateDir string, factory AdapterFactory, logger *log.Logger) (*Receiver, error) {
	id, ok := strings.CutPrefix(bundle.Principal, admin.ReceiverPrincipal(""))
	if !ok || !admin.ValidName(id) {
		return nil, errors.New("bundle principal is not a receiver principal")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(bundle.CA)) {
		return nil, errors.New("bundle CA contains no certificates")
	}
	if stateDir == "" {
		return nil, errors.New("receiver requires a state directory")
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return nil, err
	}
	signing, err := loadOrCreate(filepath.Join(stateDir, "signing.key"), trust.LoadSigningKeyFile, trust.GenerateSigningKeyFile)
	if err != nil {
		return nil, fmt.Errorf("signing key: %w", err)
	}
	sealing, err := loadOrCreate(filepath.Join(stateDir, "sealing.key"), trust.LoadSealingKeyFile, trust.GenerateSealingKeyFile)
	if err != nil {
		return nil, fmt.Errorf("sealing key: %w", err)
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(bundle.Broker),
		kgo.DialTLSConfig(&tls.Config{RootCAs: roots}),
		kgo.SASL(scram.Auth{User: bundle.Principal, Pass: bundle.Password}.AsSha512Mechanism()),
	}
	base, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Receiver{id: id, kafkaOpt: opts, base: base, signing: signing, sealing: sealing, factory: factory, logger: logger}, nil
}

func loadOrCreate[K any](path string, load func(string) (K, error), create func(string) (K, error)) (K, error) {
	key, err := load(path)
	if errors.Is(err, os.ErrNotExist) {
		return create(path)
	}
	return key, err
}

// Run registers the receiver's keys, waits for its configuration, then advertises and claims
// until ctx ends or the configuration changes.
func (r *Receiver) Run(ctx context.Context) error {
	defer r.base.Close()
	if err := r.register(ctx); err != nil {
		return err
	}
	if err := r.configure(ctx); err != nil {
		return err
	}
	// Claiming waits for one read of the pool's events, so a cancelled submission is not dispatched on start.
	if err := r.readCancels(ctx); err != nil {
		return err
	}
	if err := r.advertise(ctx); err != nil {
		r.logger.Printf("advertise: %v", err)
	}
	ctx, stop := context.WithCancelCause(ctx)
	defer stop(nil)
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		r.every(ctx, advertiseInterval, r.advertise)
	}()
	go func() {
		defer wg.Done()
		r.every(ctx, refreshInterval, func(ctx context.Context) error {
			err := r.refresh(ctx)
			if errors.Is(err, ErrConfigChanged) {
				r.logger.Printf("configuration changed; exiting")
				stop(ErrConfigChanged)
			}
			return err
		})
	}()
	// A separate step, so a pool events read failure never delays noticing a configuration change.
	go func() {
		defer wg.Done()
		r.every(ctx, cancelReadInterval, r.readCancels)
	}()
	for slot := int64(0); slot < r.config.Capacity; slot++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.claimLoop(ctx)
		}()
	}
	wg.Wait()
	return context.Cause(ctx)
}

// every runs step at each interval until ctx ends; Run has already run each step once.
func (r *Receiver) every(ctx context.Context, interval time.Duration, step func(context.Context) error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if err := step(ctx); err != nil && ctx.Err() == nil && !errors.Is(err, ErrConfigChanged) {
			r.logger.Printf("periodic step: %v", err)
		}
	}
}

// register puts the signing and sealing keys on an empty key topic and refuses a topic holding other keys.
func (r *Receiver) register(ctx context.Context) error {
	topic := admin.ReceiverTopic(r.id)
	public := r.signing.Public().(ed25519.PublicKey)
	for attempt := 0; ; attempt++ {
		keys, err := admin.ReadKeys(ctx, r.base, map[string]string{r.id: topic})
		if err != nil {
			return fmt.Errorf("read key topic: %w", err)
		}
		state := keys[r.id]
		if state.Current == nil && !state.Revoked && attempt == 0 {
			record, err := signed.RegisterKey(r.id, r.signing, r.sealing.PublicKey())
			if err != nil {
				return err
			}
			if err := r.base.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(r.id), Value: record}).FirstErr(); err != nil {
				return fmt.Errorf("register keys: %w", err)
			}
			r.logger.Printf("registered keys: signing=%s sealing=%s", signed.KeyID(public), signed.SealingKeyID(r.sealing.PublicKey()))
			continue
		}
		if state.Revoked || !public.Equal(state.Current) || state.Sealing == nil || !state.Sealing.Equal(r.sealing.PublicKey()) {
			return errors.New("receiver key topic holds other keys than the state directory")
		}
		return nil
	}
}

// configure waits for this receiver's configuration record, validates it against its pool's
// terms and builds its adapters.
func (r *Receiver) configure(ctx context.Context) error {
	waiting := false
	for {
		state, err := admin.Load(ctx, r.base)
		if err != nil {
			return fmt.Errorf("read wherry.admin: %w", err)
		}
		if state.Ended[admin.ReceiverPrincipal(r.id)] {
			return errors.New("receiver membership has ended")
		}
		if config, ok := state.Receivers[r.id]; ok {
			if config.Receiver != r.id {
				return errors.New("configuration belongs to another receiver")
			}
			if err := admin.ValidateConfig(config, state.Pools[config.Pool]); err != nil {
				return fmt.Errorf("configuration: %w", err)
			}
			adapters := make([]Named, 0, len(config.Adapters))
			for _, settings := range config.Adapters {
				named, err := r.factory(settings)
				if err != nil {
					return fmt.Errorf("adapter %s: %w", settings.Name, err)
				}
				adapters = append(adapters, named)
			}
			r.config, r.adapters = config, adapters
			r.logger.Printf("configured: pool=%s adapters=%d capacity=%d", config.Pool, len(adapters), config.Capacity)
			return r.refresh(ctx)
		}
		if !waiting {
			r.logger.Printf("waiting for configuration")
			waiting = true
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(refreshInterval):
		}
	}
}

// refresh folds wherry.admin and the pool's key topics; it returns ErrConfigChanged when the
// configuration record is no longer the one the receiver started with.
func (r *Receiver) refresh(ctx context.Context) error {
	// A bounded read can wait on another holder's open transaction; never longer than this.
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	state, err := admin.Load(ctx, r.base)
	var peers, hosts signed.Keys
	if err == nil {
		peers, err = admin.ReadKeys(ctx, r.base, admin.PoolKeyTopics(state, r.config.Pool))
	}
	if err == nil {
		hosts, err = admin.ReadKeys(ctx, r.base, admin.HostKeyTopics(state, r.config.Pool))
	}
	if err != nil {
		r.deny(err)
		return err
	}
	if !reflect.DeepEqual(state.Receivers[r.id], r.config) {
		return ErrConfigChanged
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state, r.peers, r.hosts = state, peers, hosts
	if state.Ended[admin.ReceiverPrincipal(r.id)] {
		if r.deniedSince.IsZero() {
			r.deniedSince = time.Now()
			r.logger.Printf("denied: membership ended")
		}
		return nil
	}
	if !r.deniedSince.IsZero() {
		r.deniedSince = time.Time{}
		r.logger.Printf("access restored")
	}
	return nil
}

// deny records an authorisation error; claiming stops until a refresh succeeds.
func (r *Receiver) deny(err error) {
	if !authorisationError(err) {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.deniedSince.IsZero() {
		r.deniedSince = time.Now()
		r.logger.Printf("denied: %v", err)
	}
}

func authorisationError(err error) bool {
	for _, denial := range []error{kerr.TopicAuthorizationFailed, kerr.GroupAuthorizationFailed, kerr.TransactionalIDAuthorizationFailed, kerr.ClusterAuthorizationFailed, kerr.SaslAuthenticationFailed} {
		if errors.Is(err, denial) {
			return true
		}
	}
	return false
}

func (r *Receiver) denied() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.deniedSince.IsZero()
}

// stopForDenial reports whether denial has lasted one lock interval, after which the broker
// can hand this receiver's held tasks to another member.
func (r *Receiver) stopForDenial() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	lock := time.Duration(r.state.Pools[r.config.Pool].LockDurationMS) * time.Millisecond
	return !r.deniedSince.IsZero() && time.Since(r.deniedSince) >= lock
}

func (r *Receiver) snapshot() (admin.State, signed.Keys, signed.Keys) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state, r.peers, r.hosts
}

// authorizeHost calls authorize with the host keys, and once more after re-reading the signing
// owner's key topic when the record names a key ID other than the owner's current one.
func (r *Receiver) authorizeHost(ctx context.Context, encoded []byte, authorize func(admin.State, signed.Keys) error) error {
	state, _, hosts := r.snapshot()
	err := authorize(state, hosts)
	if err == nil {
		return nil
	}
	owner, keyID, ok := kid(encoded)
	topic, live := admin.HostKeyTopics(state, r.config.Pool)[owner]
	if !ok || !live || (hosts[owner].Current != nil && signed.KeyID(hosts[owner].Current) == keyID) {
		return err
	}
	fresh, readErr := admin.ReadKeys(ctx, r.base, map[string]string{owner: topic})
	if readErr != nil {
		r.deny(readErr)
		return readErr
	}
	r.mu.Lock()
	r.hosts = maps.Clone(r.hosts)
	r.hosts[owner] = fresh[owner]
	hosts = r.hosts
	r.mu.Unlock()
	return authorize(state, hosts)
}

// kid returns the owner and key ID a compact JWS names in its protected header.
func kid(encoded []byte) (string, string, bool) {
	header, _, ok := strings.Cut(string(encoded), ".")
	if !ok {
		return "", "", false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(header)
	if err != nil {
		return "", "", false
	}
	var protected struct {
		KID string `json:"kid"`
	}
	if json.Unmarshal(decoded, &protected) != nil {
		return "", "", false
	}
	return strings.Cut(protected.KID, "/")
}

func (r *Receiver) named(name string) (Named, bool) {
	for _, named := range r.adapters {
		if named.Name == name {
			return named, true
		}
	}
	return Named{}, false
}

// advertise sweeps each adapter, then publishes its offers within the configured runtimes,
// labelled with the adapter's agent and the receiver's labels.
func (r *Receiver) advertise(ctx context.Context) error {
	for _, named := range r.adapters {
		if err := named.Adapter.Sweep(ctx); err != nil {
			r.logger.Printf("adapter %s sweep: %T", named.Name, err)
		}
	}
	offers := []wire.Offer{}
	for _, named := range r.adapters {
		found, err := named.Adapter.Offers(ctx)
		if err != nil {
			r.logger.Printf("adapter %s offers: %v", named.Name, err)
			continue
		}
		for _, labels := range found {
			if !slices.Contains(r.config.Runtimes, labels["runtime"]) {
				continue
			}
			offer := map[string]string{}
			maps.Copy(offer, r.config.Labels)
			maps.Copy(offer, labels)
			offer["agent"] = named.Agent
			offers = append(offers, wire.Offer{Adapter: named.Name, Labels: offer})
		}
	}
	r.mu.Lock()
	r.offers = offers
	inFlight := r.inFlight
	r.mu.Unlock()
	value, err := wire.SignRecord(&wire.Advertisement{
		Receiver: r.id, Pool: r.config.Pool, Offers: offers, Capacity: r.config.Capacity, InFlight: inFlight,
		SealingKey: signed.SealingKeyID(r.sealing.PublicKey()), AtMS: time.Now().UnixMilli(),
	}, r.signing)
	if err != nil {
		return err
	}
	if err := r.base.ProduceSync(ctx, &kgo.Record{Topic: admin.ReceiversTopic, Key: []byte(r.id), Value: value}).FirstErr(); err != nil {
		r.deny(err)
		return err
	}
	return nil
}

// offer returns this receiver's first current offer for the placement.
func (r *Receiver) offer(placement wire.Placement) (wire.Offer, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, offer := range r.offers {
		if offer.Labels["agent"] != placement.Agent || offer.Labels["runtime"] != placement.Runtime {
			continue
		}
		matched := true
		for key, value := range placement.Labels {
			matched = matched && offer.Labels[key] == value
		}
		if matched {
			return offer, true
		}
	}
	return wire.Offer{}, false
}

// poolEvents is one read of the pool's events topic.
type poolEvents struct {
	// statuses holds, per run, the statuses from live pool members in log order, one per event ID.
	statuses map[string][]wire.RunStatus
	// others holds, per run, every other record, such as cancels.
	others map[string][][]byte
}

func (r *Receiver) readEvents(ctx context.Context) (poolEvents, error) {
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	records, err := admin.ReadTopic(ctx, r.base, admin.PoolEventsTopic(r.config.Pool))
	if err != nil {
		r.deny(err)
		return poolEvents{}, err
	}
	state, peers, _ := r.snapshot()
	events := poolEvents{statuses: map[string][]wire.RunStatus{}, others: map[string][][]byte{}}
	seen := map[string]bool{}
	for _, record := range records {
		run := string(record.Key)
		var named wire.RunStatus
		if wire.ParseRecord(record.Value, peers, &named) != nil {
			events.others[run] = append(events.others[run], record.Value)
			continue
		}
		// A status binds its run's tenant through the task hash; the pool and membership are checked here.
		status, err := admin.AuthorizeStatus(state, peers, record.Value, r.config.Pool, named.Wherry.Tenant)
		if err != nil || status.Wherry.Run != run || seen[status.Wherry.EventID] {
			continue
		}
		seen[status.Wherry.EventID] = true
		events.statuses[run] = append(events.statuses[run], status)
	}
	return events, nil
}

// committedTerminal reports a terminal status for the exact task record on the pool's events.
func (events poolEvents) committedTerminal(run, taskHash string) bool {
	return slices.ContainsFunc(events.statuses[run], func(status wire.RunStatus) bool {
		return status.Wherry.Kind == "terminal" && status.Wherry.TaskHash == taskHash
	})
}

// cancelledTurn returns the lowest turn of run that a live host of tenant has cancelled.
func (r *Receiver) cancelledTurn(ctx context.Context, events poolEvents, run, tenant string) (int64, bool) {
	var lowest int64
	found := false
	for _, encoded := range events.others[run] {
		err := r.authorizeHost(ctx, encoded, func(state admin.State, hosts signed.Keys) error {
			cancel, err := admin.AuthorizeCancel(state, hosts, encoded, r.config.Pool, tenant)
			if err == nil && cancel.Run != run {
				return errors.New("cancel names another run than its key")
			}
			if err == nil && (!found || cancel.Turn < lowest) {
				lowest, found = cancel.Turn, true
			}
			return err
		})
		_ = err // a record that is not an authorised cancel changes nothing
	}
	return lowest, found
}

// readCancels rebuilds the cancelled runs from the pool's events, and ends each cancelled run
// that waits interrupted, held by no member.
func (r *Receiver) readCancels(ctx context.Context) error {
	events, err := r.readEvents(ctx)
	if err != nil {
		return err
	}
	cancelled := map[string]int64{}
	for run, statuses := range events.statuses {
		if len(events.others[run]) == 0 {
			continue
		}
		tenant := statuses[0].Wherry.Tenant
		turn, ok := r.cancelledTurn(ctx, events, run, tenant)
		if !ok {
			continue
		}
		cancelled[run] = turn
		latest := latestStatus(statuses, tenant)
		if latest != nil && latest.Wherry.Kind == "interrupted" && latest.Wherry.Turn >= turn {
			if err := r.cancelInterrupted(ctx, *latest); err != nil {
				r.logger.Printf("run %s: cancel interrupted turn: %v", run, err)
			}
		}
	}
	r.mu.Lock()
	r.cancelled = cancelled
	r.mu.Unlock()
	return nil
}

// latestStatus is the run's latest status of tenant, superseded attempts and rejections dropped.
func latestStatus(statuses []wire.RunStatus, tenant string) *wire.RunStatus {
	var latest *wire.RunStatus
	var order wire.AttemptFilter
	for i := range statuses {
		status := &statuses[i]
		if status.Wherry.Tenant != tenant || status.Wherry.Kind == "rejected" || !order.Admit(status.Wherry.Run, status.Wherry.Turn, status.Wherry.Attempt) {
			continue
		}
		latest = status
	}
	return latest
}

// credentialOutcome is what the seals on a tenant's host topics say about one credential.
type credentialOutcome int

const (
	credentialOpened credentialOutcome = iota
	// credentialWithPeer means another pool member holds a current seal: a start failure.
	credentialWithPeer
	// credentialMissing means no member holds one: the run waits for the tenant (AUTH_REQUIRED).
	credentialMissing
)

// credential opens the latest seal by issued time for this receiver, pool and credential across
// the tenant's host topics; the agent's terms name the credentials it may take.
func (r *Receiver) credential(ctx context.Context, tenant, agent, name string) ([]byte, credentialOutcome, error) {
	if name == "" {
		return nil, credentialOpened, nil
	}
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	state, _, _ := r.snapshot()
	latest := map[string]signed.Seal{}
	for principal, host := range state.Hosts {
		if host.Tenant != tenant || state.Ended[principal] {
			continue
		}
		records, err := admin.ReadTopic(ctx, r.base, admin.HostTopic(host.Tenant, host.Name))
		if err != nil {
			r.deny(err)
			return nil, 0, err
		}
		for _, record := range records {
			seal, err := signed.DecodeSeal(record.Value)
			if err != nil || seal.Tenant != tenant || seal.Pool != r.config.Pool || seal.Credential != name {
				continue
			}
			if previous, ok := latest[seal.Receiver]; !ok || seal.IssuedMS > previous.IssuedMS {
				latest[seal.Receiver] = seal
			}
		}
	}
	now := time.Now()
	if seal, ok := latest[r.id]; ok {
		var allowed []string
		if index := slices.IndexFunc(state.Pools[r.config.Pool].Agents, func(terms admin.AgentTerms) bool { return terms.Name == agent }); index >= 0 {
			allowed = state.Pools[r.config.Pool].Agents[index].Credentials
		}
		value, err := signed.OpenCredential(seal, tenant, r.id, r.config.Pool, name, now, allowed, r.sealing)
		if err == nil {
			return value, credentialOpened, nil
		}
		// never log err: the cause may describe secret material
		r.logger.Printf("credential %s for tenant %s: latest seal does not open", name, tenant)
	}
	for receiver, seal := range latest {
		config, member := state.Receivers[receiver]
		if receiver != r.id && member && config.Pool == r.config.Pool && !state.Ended[admin.ReceiverPrincipal(receiver)] && now.Before(time.UnixMilli(seal.ExpiryMS)) {
			return nil, credentialWithPeer, nil
		}
	}
	return nil, credentialMissing, nil
}
