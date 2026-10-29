// Package receiver is the platform-independent receiver core.
package receiver

import (
	"context"
	"crypto/ed25519"
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
	"github.com/benemon/wherry/internal/redact"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/trust"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// parseHistory is wire.ParseHistory, swapped by tests that count the verifications of a read.
var parseHistory = wire.ParseHistory

// ErrConfigChanged is returned by Run when wherry.admin holds a configuration other than the one it started with.
var ErrConfigChanged = errors.New("receiver configuration changed")

// Named is a configured profile with its name, the agent it launches and its plugin's handle on it.
type Named struct {
	Name  string
	Agent string
	// Labels are the operator's declared labels for the profile.
	Labels map[string]string
	Plugin pluginsdk.Plugin
}

// ProfileFactory configures one profile for the receiver's pool from its settings in the receiver's configuration
// record.
type ProfileFactory func(pool string, settings admin.ProfileSettings) (Named, error)

// Receiver claims tasks for one pool and runs them through its profiles.
type Receiver struct {
	id   string
	base *kgo.Client
	// reader reads every topic the receiver reads.
	reader  *admin.Reader
	signing ed25519.PrivateKey
	factory ProfileFactory
	logger  *log.Logger
	metrics *metrics
	// rotateKey, set by New only, has Run replace the signing key in keyFile before it claims anything.
	keyFile   string
	rotateKey bool
	// drainTimeout bounds how long Run lets held runs end after its context ends.
	drainTimeout time.Duration
	// config and profiles are set by configure before any claim and never change.
	config   admin.ReceiverConfig
	profiles []Named
	mu       sync.Mutex
	state    admin.State
	// peers holds the pool's receiver keys, this receiver's included; hosts the keys of entitled tenants' hosts.
	peers    signed.Keys
	hosts    signed.Keys
	offers   []wire.Offer
	inFlight int64
	// denials holds the capabilities an authorisation error took away; deniedSince is when the first was
	// lost, zero while access holds.
	denials     map[capability]bool
	deniedSince time.Time
	// backoffUntil is when the receiver may fetch from its pool again after releasing a task it failed to start, and
	// fetching counts the share sessions polling the pool for a task.
	backoffUntil time.Time
	fetching     int
	// admissionMu serialises the admission transactional ID, which one producer at a time may hold.
	admissionMu sync.Mutex
	// cancelled holds the runs cancelled by a host of their tenant.
	cancelled map[runKey]bool
	// live holds the forms of the values delivered to each run this receiver holds, and platform those of its platform
	// credentials.
	liveMu   sync.Mutex
	live     map[*activeRun][]redact.Form
	platform []redact.Form
}

// New builds the receiver's Kafka client from bundle and loads or creates its signing key in stateDir, settling a
// rotation an earlier start left. With rotateKey, Run rotates the key. Once configured, the receiver registers its
// metric families on registry.
func New(bundle admin.Bundle, stateDir string, rotateKey bool, drainTimeout time.Duration, factory ProfileFactory, logger *log.Logger, registry *prometheus.Registry) (*Receiver, error) {
	id, ok := strings.CutPrefix(bundle.Principal, admin.ReceiverPrincipal(""))
	if !ok || !wire.ValidName(id) {
		return nil, errors.New("bundle principal is not a receiver principal")
	}
	opts, err := admin.KafkaOpts(bundle.Broker, []byte(bundle.CA), bundle.Principal, bundle.Password)
	if err != nil {
		return nil, fmt.Errorf("bundle: %w", err)
	}
	if stateDir == "" {
		return nil, errors.New("receiver requires a state directory")
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return nil, err
	}
	base, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	keyFile := filepath.Join(stateDir, "signing.key")
	signing, err := admin.RecoverKey(context.Background(), base, id, admin.ReceiverTopic(id), keyFile)
	if errors.Is(err, os.ErrNotExist) {
		signing, err = trust.GenerateSigningKeyFile(keyFile)
	}
	if err != nil {
		base.Close()
		return nil, fmt.Errorf("signing key: %w", err)
	}
	return &Receiver{id: id, base: base, reader: admin.NewReader(base), signing: signing, keyFile: keyFile, rotateKey: rotateKey, drainTimeout: drainTimeout, factory: factory, logger: logger, metrics: newMetrics(registry)}, nil
}

// newClient is a client of the receiver's connection with extra options.
func (r *Receiver) newClient(extra ...kgo.Opt) (*kgo.Client, error) {
	return kgo.NewClient(append(slices.Clone(r.base.Opts()), extra...)...)
}

// Run registers the receiver's keys, rotates them when New was asked to, waits for its configuration, then advertises
// and claims until ctx ends or the configuration changes. Held runs then go on: after a configuration change until they
// end, and after ctx ends for at most the drain timeout. A forced configuration change releases their tasks at once.
func (r *Receiver) Run(ctx context.Context) error {
	defer r.base.Close()
	defer r.reader.Close()
	if err := r.register(ctx); err != nil {
		return err
	}
	if r.rotateKey {
		next, err := admin.RotateKey(ctx, r.base, r.id, admin.ReceiverTopic(r.id), r.keyFile, r.signing)
		if err != nil {
			return fmt.Errorf("rotate key: %w", err)
		}
		r.logger.Printf("rotated key: old=%s new=%s", signed.KeyID(r.signing.Public().(ed25519.PublicKey)), signed.KeyID(next.Public().(ed25519.PublicKey)))
		r.signing = next
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
	release := make(chan struct{})
	releaseHeld := sync.OnceFunc(func() { close(release) })
	defer context.AfterFunc(ctx, func() { time.AfterFunc(r.drainTimeout, releaseHeld) })()
	ctx, stop := context.WithCancelCause(ctx)
	defer stop(nil)
	// Held runs still read cancels and denials, so the periodic steps last until held tasks are released.
	running, stopRunning := context.WithCancel(context.WithoutCancel(ctx))
	defer stopRunning()
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		r.every(running, advertiseInterval, r.advertise)
	}()
	go func() {
		defer wg.Done()
		r.every(running, refreshInterval, func(running context.Context) error {
			err := r.refresh(running)
			if errors.Is(err, ErrConfigChanged) {
				if state, _, _ := r.snapshot(); state.Receivers[r.id].Force {
					releaseHeld()
				}
				if ctx.Err() == nil {
					r.logger.Printf("configuration changed; exiting once held runs end")
					stop(ErrConfigChanged)
				}
			}
			return err
		})
	}()
	// A separate step, so a pool events read failure never delays noticing a configuration change.
	go func() {
		defer wg.Done()
		r.every(running, cancelReadInterval, r.readCancels)
	}()
	// Held tasks are released only once every idle slot's share client has closed: a closing client releases what its
	// pending fetch acquired, so a task released earlier would hop through each, one delivery per hop.
	var idle, slots sync.WaitGroup
	idle.Add(int(r.config.Capacity))
	for slot := int64(0); slot < r.config.Capacity; slot++ {
		slots.Go(func() { r.claimLoop(ctx, running, sync.OnceFunc(idle.Done)) })
	}
	idle.Wait()
	go func() {
		slots.Wait()
		releaseHeld()
	}()
	<-release
	stopRunning()
	slots.Wait()
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

// register puts the signing key on a key topic with no records at all, and refuses a topic holding anything but
// this receiver's own registration.
func (r *Receiver) register(ctx context.Context) error {
	topic := admin.ReceiverTopic(r.id)
	public := r.signing.Public().(ed25519.PublicKey)
	records, err := r.reader.ReadFrom(ctx, topic, 0)
	if err != nil {
		return fmt.Errorf("read key topic: %w", err)
	}
	if len(records) == 0 {
		record, err := signed.RegisterKey(r.id, r.signing)
		if err != nil {
			return err
		}
		if err := r.base.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(r.id), Value: record}).FirstErr(); err != nil {
			return fmt.Errorf("register keys: %w", err)
		}
		r.logger.Printf("registered key: signing=%s", signed.KeyID(public))
		if records, err = r.reader.ReadFrom(ctx, topic, 0); err != nil {
			return fmt.Errorf("read key topic: %w", err)
		}
	}
	var state signed.KeyState
	for _, record := range records {
		if state, err = signed.ApplyKeyRecord(state, r.id, record.Value, record.Timestamp); err != nil {
			return errors.New("receiver key topic holds a record that does not fold")
		}
	}
	if !public.Equal(state.Current) {
		return errors.New("receiver key topic holds another key than the state directory")
	}
	return nil
}

// configure waits for this receiver's configuration record, validates it against its pool's
// terms and configures its profiles.
func (r *Receiver) configure(ctx context.Context) error {
	waiting := false
	for {
		state, err := r.reader.Load(ctx)
		if err != nil {
			return fmt.Errorf("read wherry.admin: %w", err)
		}
		if state.Ended[admin.ReceiverPrincipal(r.id)] {
			return errors.New("receiver membership has ended")
		}
		if config, ok := state.Receivers[r.id]; ok {
			record := admin.Record{Kind: "receiver", Pool: config.Pool, Receiver: r.id, Config: &config}
			if err := admin.ValidateReceiverRecord(record, r.id, state.Pools[config.Pool]); err != nil {
				return fmt.Errorf("configuration: %w", err)
			}
			profiles := make([]Named, 0, len(config.Profiles))
			for _, settings := range config.Profiles {
				named, err := r.factory(config.Pool, settings)
				if err != nil {
					return fmt.Errorf("profile %s: %s", settings.Name, logText(r.redactLive, err))
				}
				profiles = append(profiles, named)
			}
			r.config, r.profiles = config, profiles
			r.metrics.registerFor(r)
			r.metrics.capacity.Set(float64(config.Capacity))
			r.logger.Printf("configured: pool=%s profiles=%d capacity=%d", config.Pool, len(profiles), config.Capacity)
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
	state, err := r.reader.Load(ctx)
	var peers, hosts signed.Keys
	if err == nil {
		peers, err = r.reader.ReadKeys(ctx, admin.PoolKeyTopics(state, r.config.Pool))
	}
	if err == nil {
		hosts, err = r.reader.ReadKeys(ctx, admin.HostKeyTopics(state, r.config.Pool))
	}
	if err != nil {
		r.deny(err, capRead)
		return err
	}
	r.mu.Lock()
	r.state, r.peers, r.hosts = state, peers, hosts
	r.mu.Unlock()
	// A membership end comes before a configuration change, so a revoked receiver does not drain.
	if state.Ended[admin.ReceiverPrincipal(r.id)] {
		r.deny(kerr.ClusterAuthorizationFailed, capMembership)
		return nil
	}
	if !reflect.DeepEqual(state.Receivers[r.id], r.config) {
		return ErrConfigChanged
	}
	r.restore(capRead)
	if r.holds(capWrite) || r.holds(capTransactional) {
		if err := r.probeWrite(ctx); err != nil {
			r.deny(err, capWrite)
			return nil
		}
		r.restore(capWrite)
		r.restore(capTransactional)
	}
	return nil
}

// capability is what an authorisation error took away. Each is restored only by an operation that
// exercises it: a refresh for reads, an aborted admission transaction for writes and the transactional
// ID, and the next claim for the share group.
type capability string

const (
	capRead          capability = "read"
	capWrite         capability = "write"
	capTransactional capability = "transactional ID"
	capClaim         capability = "share group"
	// capMembership is never restored: an ended member is re-provisioned under a new principal.
	capMembership capability = "membership"
)

// deny counts an error from an operation that used capability, and records it when it is an authorisation error.
func (r *Receiver) deny(err error, used capability) {
	if used != capMembership && !errors.Is(err, context.Canceled) {
		r.metrics.busErrors.WithLabelValues(string(used)).Inc()
	}
	if !authorisationError(err) {
		return
	}
	switch {
	case errors.Is(err, kerr.TransactionalIDAuthorizationFailed):
		used = capTransactional
	case errors.Is(err, kerr.GroupAuthorizationFailed):
		used = capClaim
	}
	r.metrics.denials.WithLabelValues(string(used)).Inc()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.denials[used] {
		return
	}
	if r.denials == nil {
		r.denials = map[capability]bool{}
	}
	if len(r.denials) == 0 {
		r.deniedSince = time.Now()
	}
	r.denials[used] = true
	r.logger.Printf("denied (%s): %v", used, err)
}

// restore clears a denial of capability; access is restored once no denial remains.
func (r *Receiver) restore(c capability) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.denials[c] {
		return
	}
	delete(r.denials, c)
	if len(r.denials) == 0 {
		r.deniedSince = time.Time{}
		r.logger.Printf("access restored")
	}
}

func (r *Receiver) holds(c capability) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.denials[c]
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
	return len(r.denials) > 0
}

// blocked reports a denial other than the share group's, which only a claim attempt can re-test.
func (r *Receiver) blocked() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.denials) > 1 || len(r.denials) == 1 && !r.denials[capClaim]
}

// backoff is how long the receiver waits before it fetches from its pool again.
func (r *Receiver) backoff() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return time.Until(r.backoffUntil)
}

// sessions counts the share sessions polling the pool for a task.
func (r *Receiver) sessions() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fetching
}

// stopForDenial reports whether denial has lasted one lock interval, after which the broker
// can hand this receiver's held tasks to another member.
func (r *Receiver) stopForDenial() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	lock := r.state.Pools[r.config.Pool].LockDuration()
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
	owner, topic, stale := staleKey(encoded, hosts, admin.HostKeyTopics(state, r.config.Pool))
	if !stale {
		return err
	}
	hosts, readErr := r.rereadKey(ctx, &r.hosts, owner, topic)
	if readErr != nil {
		return readErr
	}
	return authorize(state, hosts)
}

// staleKey reports that encoded names, in its kid, a key ID other than the current one of an owner in
// topics, and returns that owner's key topic.
func staleKey(encoded []byte, keys signed.Keys, topics map[string]string) (string, string, bool) {
	owner, keyID, ok := kid(encoded)
	topic, live := topics[owner]
	if !ok || !live || (keys[owner].Current != nil && signed.KeyID(keys[owner].Current) == keyID) {
		return "", "", false
	}
	return owner, topic, true
}

// rereadKey refolds owner's key topic into keys, which is r.hosts or r.peers, and returns the new keys.
func (r *Receiver) rereadKey(ctx context.Context, keys *signed.Keys, owner, topic string) (signed.Keys, error) {
	fresh, err := r.reader.ReadKeys(ctx, map[string]string{owner: topic})
	if err != nil {
		r.deny(err, capRead)
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	*keys = maps.Clone(*keys)
	(*keys)[owner] = fresh[owner]
	return *keys, nil
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

// recordType returns the record type encoded's JWS header names, unverified and empty when encoded is no
// single-signature JWS.
func recordType(encoded []byte) string {
	jws, err := jose.ParseSignedCompact(string(encoded), []jose.SignatureAlgorithm{jose.EdDSA})
	if err != nil || len(jws.Signatures) != 1 {
		return ""
	}
	typ, _ := jws.Signatures[0].Protected.ExtraHeaders[jose.HeaderType].(string)
	return typ
}

// advertise sweeps each profile, then publishes one offer per ready profile with its agent; with none ready it
// publishes no offers and keeps its liveness and capacity visible. Each profile call has one advertise
// interval, so a hung plugin delays the next advertisement by at most that much per call.
func (r *Receiver) advertise(ctx context.Context) error {
	for _, named := range r.profiles {
		callCtx, cancel := context.WithTimeout(ctx, advertiseInterval)
		err := named.Plugin.Sweep(callCtx)
		cancel()
		if err != nil {
			r.metrics.sweepFailures.WithLabelValues(named.Name).Inc()
		}
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			r.logger.Printf("profile %s sweep: timed out", named.Name)
		case err != nil:
			r.logger.Printf("profile %s sweep: %s", named.Name, logText(r.redactLive, err))
		}
	}
	offers := []wire.Offer{}
	for _, named := range r.profiles {
		callCtx, cancel := context.WithTimeout(ctx, advertiseInterval)
		ready, err := named.Plugin.Ready(callCtx)
		cancel()
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			r.logger.Printf("profile %s readiness: timed out", named.Name)
			continue
		case err != nil:
			r.logger.Printf("profile %s readiness: %s", named.Name, logText(r.redactLive, err))
			continue
		}
		if ready {
			offers = append(offers, wire.Offer{Profile: named.Name, Agent: named.Agent, InlineLimit: named.Plugin.InlineLimit()})
		}
	}
	r.mu.Lock()
	r.offers = offers
	inFlight := r.inFlight
	r.mu.Unlock()
	value, err := wire.SignRecord(&wire.Advertisement{
		Receiver: r.id, Pool: r.config.Pool, Offers: offers, Capacity: r.config.Capacity, InFlight: inFlight, AtMS: time.Now().UnixMilli(),
	}, r.signing)
	if err != nil {
		return err
	}
	if err := r.base.ProduceSync(ctx, &kgo.Record{Topic: admin.ReceiversTopic, Key: []byte(r.id), Value: value}).FirstErr(); err != nil {
		r.deny(err, capWrite)
		return err
	}
	r.metrics.advertised.SetToCurrentTime()
	return nil
}

// offer returns this receiver's first profile, in record order, that is in its current offers and launches the
// placement's agent with configured labels satisfying its selector. The record lists each plugin's profiles in their
// declared order, plugins sorted by type, so the host's placement and this routing compute the same match.
func (r *Receiver) offer(placement wire.Placement) (Named, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, named := range r.profiles {
		if named.Agent == placement.Agent && wire.Satisfies(placement.Selector, named.Labels) &&
			slices.ContainsFunc(r.offers, func(offer wire.Offer) bool { return offer.Profile == named.Name }) {
			return named, true
		}
	}
	return Named{}, false
}

// poolEvents is one read of the pool's events topic.
type poolEvents struct {
	// statuses holds, per run, the statuses from live pool members in log order, one per event ID.
	statuses map[string][]wire.RunStatus
	// startFailures holds, per run, this receiver's start failures in log order.
	startFailures map[string][]wire.StartFailure
	// others holds, per run, every other record, such as cancels.
	others map[string][][]byte
}

func (r *Receiver) readEvents(ctx context.Context) (poolEvents, error) {
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	records, err := r.reader.ReadFrom(ctx, admin.PoolEventsTopic(r.config.Pool), 0)
	if err != nil {
		r.deny(err, capRead)
		return poolEvents{}, err
	}
	state, peers, _ := r.snapshot()
	peerTopics := admin.PoolKeyTopics(state, r.config.Pool)
	reread := map[string]bool{}
	events := poolEvents{statuses: map[string][]wire.RunStatus{}, startFailures: map[string][]wire.StartFailure{}, others: map[string][][]byte{}}
	seen := map[string]bool{}
	for _, record := range records {
		run := string(record.Key)
		history := parseHistory(record.Timestamp)
		// The JWS type picks the one parse a record gets; the parse checks the type again under the signature. parseErr
		// tells a record that does not verify, kept for the cancel check, from one that verifies and is refused.
		var status wire.RunStatus
		var failure wire.StartFailure
		var parseErr error
		typ := recordType(record.Value)
		authorize := func(peers signed.Keys) error {
			parse := func(encoded []byte, keys signed.Keys, value wire.Record) error {
				parseErr = history(encoded, keys, value)
				return parseErr
			}
			var err error
			switch typ {
			case wire.StatusType:
				status, err = admin.AuthorizePoolStatus(parse, state, peers, record.Value, r.config.Pool)
			case wire.StartFailureType:
				failure, err = admin.AuthorizePoolStartFailure(parse, state, peers, record.Value, r.config.Pool)
			default:
				parseErr = errors.New("record is neither a status nor a start failure")
			}
			return err
		}
		err := authorize(peers)
		// A member that rotated its key since the last refresh is re-read once per read of the topic.
		if owner, topic, stale := staleKey(record.Value, peers, peerTopics); parseErr != nil && stale && !reread[owner] {
			reread[owner] = true
			if peers, err = r.rereadKey(ctx, &r.peers, owner, topic); err != nil {
				return poolEvents{}, err
			}
			err = authorize(peers)
		}
		switch {
		case parseErr != nil:
			events.others[run] = append(events.others[run], record.Value)
		case err != nil:
			// A status or start failure that verifies but is not authorised counts for nothing.
		case typ == wire.StartFailureType:
			// A start failure binds its run's tenant through the task hash, which failedHere matches.
			if failure.Wherry.Run == run && failure.Wherry.Receiver == r.id {
				events.startFailures[run] = append(events.startFailures[run], failure)
			}
		case status.Wherry.Run == run && !seen[status.Wherry.EventID]:
			// A status binds its run's tenant through the task hash; the pool, membership and entitlement are checked here.
			seen[status.Wherry.EventID] = true
			events.statuses[run] = append(events.statuses[run], status)
		}
	}
	return events, nil
}

// committedTerminal reports a status in a terminal A2A state, a rejection included, for the exact task
// record on the pool's events.
func (events poolEvents) committedTerminal(run, taskHash string) bool {
	return slices.ContainsFunc(events.statuses[run], func(status wire.RunStatus) bool {
		return wire.Terminal(status.StatusUpdate.Status.State) && status.Wherry.TaskHash == taskHash
	})
}

// failedHere reports a start failure for the task record hash, which binds the failure to its run's tenant.
func failedHere(failures []wire.StartFailure, hash string) bool {
	return slices.ContainsFunc(failures, func(failure wire.StartFailure) bool { return failure.TaskHash == hash })
}

type runKey struct{ tenant, run string }

// cancelledRun reports whether a live host of tenant has cancelled run.
func (r *Receiver) cancelledRun(ctx context.Context, events poolEvents, run, tenant string) bool {
	found := false
	for _, encoded := range events.others[run] {
		err := r.authorizeHost(ctx, encoded, func(state admin.State, hosts signed.Keys) error {
			cancel, err := admin.AuthorizeCancel(state, hosts, encoded, r.config.Pool, tenant)
			if err == nil && cancel.Run != run {
				return errors.New("cancel names another run than its key")
			}
			if err == nil {
				found = true
			}
			return err
		})
		_ = err // a record that is not an authorised cancel changes nothing
	}
	return found
}

// readCancels rebuilds the cancelled runs from the pool's events.
func (r *Receiver) readCancels(ctx context.Context) error {
	events, err := r.readEvents(ctx)
	if err != nil {
		return err
	}
	cancelled := map[runKey]bool{}
	for run, statuses := range events.statuses {
		// A cancel counts for its own tenant only, so a status of another tenant under the run ID, such as a
		// rejection, cannot lend that tenant's host the run.
		tenants := map[string]bool{}
		for _, status := range statuses {
			tenants[status.Wherry.Tenant] = true
		}
		for tenant := range tenants {
			if r.cancelledRun(ctx, events, run, tenant) {
				cancelled[runKey{tenant, run}] = true
			}
		}
	}
	r.mu.Lock()
	r.cancelled = cancelled
	r.mu.Unlock()
	return nil
}
