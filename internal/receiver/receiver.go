// Package receiver is the platform-independent receiver core.
package receiver

import (
	"cmp"
	"context"
	"crypto/ed25519"
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

	"github.com/benemon/wherry/internal/operator"
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

// parseHistory, parseStrict and authorizeSubmission are wire.ParseHistory, wire.ParseStrict and
// operator.AuthorizeSubmission, swapped by tests that count the verifications of a read; loadState is the custodian's read
// of wherry.operator, swapped by tests that count its reads.
var (
	parseHistory        = wire.ParseHistory
	parseStrict         = wire.ParseStrict
	authorizeSubmission = operator.AuthorizeSubmission
	loadState           = (*operator.Reader).Load
)

// ErrConfigChanged is returned by Run when wherry.operator holds a configuration other than the one it started with.
var ErrConfigChanged = errors.New("receiver configuration changed")

// Named is a configured profile with its name, the agent it launches and its plugin's handle on it.
type Named struct {
	Name  string
	Agent string
	// Labels are the operator's declared labels for the profile.
	Labels map[string]string
	Plugin pluginsdk.Plugin
	// Exited, when set, is closed once the profile's plugin process exits, which stops the receiver.
	Exited <-chan struct{}
}

// ProfileFactory configures one profile for the receiver's pool from its settings in the receiver's configuration
// record and the SHA-256 the record pins for its plugin's binary.
type ProfileFactory func(pool string, settings operator.ProfileSettings, pluginSHA256 string) (Named, error)

// Receiver claims tasks for one pool and runs them through its profiles.
type Receiver struct {
	id   string
	base *kgo.Client
	// reader reads every topic the receiver reads.
	reader  *operator.Reader
	signing ed25519.PrivateKey
	factory ProfileFactory
	logger  *log.Logger
	metrics *metrics
	// rotateKey, set by New only, has Run replace the signing key in keyFile before it claims anything.
	keyFile   string
	rotateKey bool
	// stateDir holds the signing key and the pool keys.
	stateDir string
	custody  custody
	// drainTimeout bounds how long Run lets held runs end after its context ends.
	drainTimeout time.Duration
	// config and profiles are set by configure before any claim and never change.
	config   operator.ReceiverConfig
	profiles []Named
	mu       sync.Mutex
	state    operator.State
	// peers holds the pool's receiver keys, this receiver's included; hosts the keys of entitled tenants' hosts.
	peers signed.Keys
	hosts signed.Keys
	// ended holds the keys of the pool's ended receivers, each retired at the append time of the record that ended
	// it, so the open records they appended while live still verify.
	ended    signed.Keys
	offers   []wire.Offer
	inFlight int64
	// shutdownBy, set at a stop, is when every step of the stop must have ended.
	shutdownBy time.Time
	// halt, set by Run once it claims, ends claiming and held runs at once with the error Run returns.
	halt func(error)
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
	// events is the pool's events folded so far, which every read of them shares.
	events eventsFold
}

// New builds the receiver's Kafka client from bundle, trusting the broker through the PEM certificates in ca or, when ca
// is nil, the system roots, and loads or creates its signing key in stateDir, settling a
// rotation an earlier start left. With rotateKey, Run rotates the key. Once configured, the receiver registers its
// metric families on registry.
func New(bundle operator.Bundle, ca []byte, stateDir string, rotateKey bool, drainTimeout time.Duration, factory ProfileFactory, logger *log.Logger, registry *prometheus.Registry) (*Receiver, error) {
	id, ok := strings.CutPrefix(bundle.Principal, operator.ReceiverPrincipal(""))
	if !ok || !wire.ValidName(id) {
		return nil, errors.New("bundle principal is not a receiver principal")
	}
	opts, err := operator.KafkaOpts(bundle.Broker, ca, bundle.Principal, bundle.Password)
	if err != nil {
		return nil, fmt.Errorf("bundle: %w", err)
	}
	if stateDir == "" {
		return nil, errors.New("receiver requires a state directory")
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return nil, err
	}
	base, err := kgo.NewClient(feedBatches(opts)...)
	if err != nil {
		return nil, err
	}
	keyFile := filepath.Join(stateDir, "signing.key")
	signing, err := operator.RecoverKey(context.Background(), base, id, operator.ReceiverTopic(id), keyFile)
	if errors.Is(err, os.ErrNotExist) {
		signing, err = trust.GenerateSigningKeyFile(keyFile)
	}
	if err != nil {
		base.Close()
		return nil, fmt.Errorf("signing key: %w", err)
	}
	return &Receiver{id: id, base: base, reader: operator.NewReader(base), signing: signing, keyFile: keyFile, stateDir: stateDir, rotateKey: rotateKey, drainTimeout: drainTimeout, factory: factory, logger: logger, metrics: newMetrics(registry)}, nil
}

// feedBatches is opts with producer batches up to what a tenant's feed takes, so the largest result fits. Every client
// of the receiver's connection carries it.
func feedBatches(opts []kgo.Opt) []kgo.Opt {
	return append(opts, kgo.ProducerBatchMaxBytes(operator.FeedRecordLimit))
}

// newClient is a client of the receiver's connection with extra options.
func (r *Receiver) newClient(extra ...kgo.Opt) (*kgo.Client, error) {
	return kgo.NewClient(append(slices.Clone(r.base.Opts()), extra...)...)
}

// Run registers the receiver's keys, rotates them when New was asked to, waits for its configuration, then advertises
// and claims until ctx ends or the configuration changes. Held runs then go on: after a configuration change until they
// end, and after ctx ends for at most the drain timeout.
// A Kafka authorisation error or a plugin process's exit ends Run at once with that error: held runs are left
// unacknowledged and their jobs running, for an adopter to find once the broker redelivers their tasks.
func (r *Receiver) Run(ctx context.Context) error {
	defer r.base.Close()
	defer r.reader.Close()
	if err := r.register(ctx); err != nil {
		return err
	}
	if r.rotateKey {
		next, err := operator.RotateKey(ctx, r.base, r.id, operator.ReceiverTopic(r.id), r.keyFile, r.signing)
		if err != nil {
			return r.denyStep(fmt.Errorf("rotate key: %w", err), capWrite, "rotate key")
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
	release := make(chan struct{})
	releaseHeld := sync.OnceFunc(func() { close(release) })
	// A stop fixes one shutdown deadline, the custodian's step-down bound, which held runs' final steps share.
	defer context.AfterFunc(ctx, func() {
		r.mu.Lock()
		r.shutdownBy = time.Now().Add(r.drainTimeout + stepDownMargin)
		r.mu.Unlock()
		time.AfterFunc(r.drainTimeout, releaseHeld)
	})()
	ctx, stop := context.WithCancelCause(ctx)
	defer stop(nil)
	// Held runs still read cancels, so the periodic steps last until held tasks are released.
	running, stopRunning := context.WithCancelCause(context.WithoutCancel(ctx))
	defer stopRunning(nil)
	// running ends first, so custody is given up without a step-down.
	r.mu.Lock()
	r.halt = func(err error) {
		stopRunning(err)
		stop(err)
		releaseHeld()
	}
	r.mu.Unlock()
	for i, named := range r.profiles {
		if named.Exited != nil {
			go func() {
				select {
				case <-named.Exited:
					r.exit(fmt.Errorf("plugin %s exited", r.config.Profiles[i].Plugin))
				case <-running.Done():
				}
			}()
		}
	}
	if err := r.advertise(ctx); err != nil {
		r.logger.Printf("advertise: %v", err)
	}
	var wg sync.WaitGroup
	wg.Add(5)
	go func() {
		defer wg.Done()
		r.keepCustody(ctx, running)
	}()
	go func() {
		defer wg.Done()
		r.every(running, openInterval, r.serveOpens)
	}()
	go func() {
		defer wg.Done()
		r.every(running, advertiseInterval, r.advertise)
	}()
	go func() {
		defer wg.Done()
		r.every(running, refreshInterval, func(running context.Context) error {
			err := r.refresh(running)
			if errors.Is(err, ErrConfigChanged) {
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
	stopRunning(nil)
	slots.Wait()
	wg.Wait()
	if halted(running) {
		return context.Cause(running)
	}
	return context.Cause(ctx)
}

// exit stops the receiver for err once Run claims; before that, the step that failed ends Run.
func (r *Receiver) exit(err error) {
	r.mu.Lock()
	halt := r.halt
	r.mu.Unlock()
	r.logger.Printf("stopping: %v", err)
	if halt != nil {
		halt(err)
	}
}

// halted reports that ctx, or the running context it derives from, ended for a denial or a plugin's exit: nothing is
// then acknowledged, stopped or handed over.
func halted(ctx context.Context) bool {
	return ctx.Err() != nil && context.Cause(ctx) != ctx.Err()
}

// shutdownContext outlives ctx for a held run's final step, ending after shutdownTimeout or at the shutdown deadline.
func (r *Receiver) shutdownContext(ctx context.Context) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(shutdownTimeout)
	r.mu.Lock()
	if !r.shutdownBy.IsZero() && r.shutdownBy.Before(deadline) {
		deadline = r.shutdownBy
	}
	r.mu.Unlock()
	return context.WithDeadline(context.WithoutCancel(ctx), deadline)
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
	topic := operator.ReceiverTopic(r.id)
	public := r.signing.Public().(ed25519.PublicKey)
	records, err := r.reader.ReadFrom(ctx, topic, 0)
	if err != nil {
		return r.denyStep(fmt.Errorf("read key topic: %w", err), capRead, "register")
	}
	if len(records) == 0 {
		record, err := signed.RegisterKey(r.id, r.signing)
		if err != nil {
			return err
		}
		if err := r.base.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(r.id), Value: record}).FirstErr(); err != nil {
			return r.denyStep(fmt.Errorf("register keys: %w", err), capWrite, "register")
		}
		r.logger.Printf("registered key: signing=%s", signed.KeyID(public))
		if records, err = r.reader.ReadFrom(ctx, topic, 0); err != nil {
			return r.denyStep(fmt.Errorf("read key topic: %w", err), capRead, "register")
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
			return r.denyStep(fmt.Errorf("read wherry.operator: %w", err), capRead, "configure")
		}
		if state.Ended[operator.ReceiverPrincipal(r.id)] {
			r.metrics.denials.WithLabelValues(string(capMembership)).Inc()
			return errors.New("receiver membership has ended")
		}
		if config, ok := state.Receivers[r.id]; ok {
			record := operator.Record{Kind: "receiver", Pool: config.Pool, Receiver: r.id, Config: &config}
			if err := operator.ValidateReceiverRecord(record, r.id, state.Pools[config.Pool]); err != nil {
				return fmt.Errorf("configuration: %w", err)
			}
			profiles := make([]Named, 0, len(config.Profiles))
			for _, settings := range config.Profiles {
				named, err := r.factory(config.Pool, settings, config.PluginSHA256[settings.Plugin])
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

// refresh folds wherry.operator and the pool's key topics; it returns ErrConfigChanged when the
// configuration record is no longer the one the receiver started with.
func (r *Receiver) refresh(ctx context.Context) error {
	// A bounded read can wait on another holder's open transaction; never longer than this.
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	state, err := r.reader.Load(ctx)
	var peers, hosts, ended signed.Keys
	if err == nil {
		peers, err = r.reader.ReadKeys(ctx, operator.PoolKeyTopics(state, r.config.Pool))
	}
	if err == nil {
		ended, err = r.reader.ReadKeys(ctx, operator.EndedPoolKeyTopics(state, r.config.Pool))
	}
	if err == nil {
		hosts, err = r.reader.ReadKeys(ctx, operator.HostKeyTopics(state, r.config.Pool))
	}
	if err != nil {
		return r.deny(err, capRead)
	}
	// Revocation removes an ended receiver's write access before it records the end, so the broker refuses any record
	// it signs afterwards: its keys verify only what it wrote while a member.
	r.mu.Lock()
	r.state, r.peers, r.hosts, r.ended = state, peers, hosts, ended
	r.mu.Unlock()
	// A membership end comes before a configuration change, so a revoked receiver does not drain.
	if state.Ended[operator.ReceiverPrincipal(r.id)] {
		r.deny(kerr.ClusterAuthorizationFailed, capMembership)
		return nil
	}
	if !reflect.DeepEqual(state.Receivers[r.id], r.config) {
		return ErrConfigChanged
	}
	return nil
}

// capability is the operation an error came from, as the bus error and denial metrics label it.
type capability string

const (
	capRead          capability = "read"
	capWrite         capability = "write"
	capTransactional capability = "transactional ID"
	capClaim         capability = "share group"
	capMembership    capability = "membership"
)

// deny counts an error from an operation that used capability and returns it; an authorisation error stops the
// receiver, and deny returns the error it stops for, naming the capability denied.
func (r *Receiver) deny(err error, used capability) error {
	return r.denyStep(err, used, "")
}

// denyStep is deny for an error of Run's startup step op, which the error a denial stops for names.
func (r *Receiver) denyStep(err error, used capability, op string) error {
	if used != capMembership && !errors.Is(err, context.Canceled) {
		r.metrics.busErrors.WithLabelValues(string(used)).Inc()
	}
	if !authorisationError(err) {
		return err
	}
	switch {
	case errors.Is(err, kerr.TransactionalIDAuthorizationFailed):
		used = capTransactional
	case errors.Is(err, kerr.GroupAuthorizationFailed):
		used = capClaim
	}
	r.metrics.denials.WithLabelValues(string(used)).Inc()
	err = fmt.Errorf("denied %s: %w", cmp.Or(op, string(used)), err)
	r.exit(err)
	return err
}

func authorisationError(err error) bool {
	for _, denial := range []error{kerr.TopicAuthorizationFailed, kerr.GroupAuthorizationFailed, kerr.TransactionalIDAuthorizationFailed, kerr.ClusterAuthorizationFailed, kerr.SaslAuthenticationFailed} {
		if errors.Is(err, denial) {
			return true
		}
	}
	return false
}

// suspended reports the receiver's pool suspended as it holds wherry.operator.
func (r *Receiver) suspended() bool {
	state, _, _ := r.snapshot()
	return state.Suspended[r.config.Pool]
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

func (r *Receiver) snapshot() (operator.State, signed.Keys, signed.Keys) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state, r.peers, r.hosts
}

// authorizeHost calls authorize with the host keys, and once more after re-reading the signing
// owner's key topic when the record names a key ID other than the owner's current one.
func (r *Receiver) authorizeHost(ctx context.Context, encoded []byte, authorize func(operator.State, signed.Keys) error) error {
	state, _, hosts := r.snapshot()
	err := authorize(state, hosts)
	if err == nil {
		return nil
	}
	owner, topic, stale := staleKey(encoded, hosts, operator.HostKeyTopics(state, r.config.Pool))
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
		return nil, r.deny(err, capRead)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	*keys = maps.Clone(*keys)
	(*keys)[owner] = fresh[owner]
	return *keys, nil
}

// kid returns the owner and key ID a compact JWS names in its protected header.
func kid(encoded []byte) (string, string, bool) {
	jws, err := jose.ParseSignedCompact(string(encoded), []jose.SignatureAlgorithm{jose.EdDSA})
	if err != nil || len(jws.Signatures) != 1 {
		return "", "", false
	}
	return strings.Cut(jws.Signatures[0].Protected.KeyID, "/")
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
	if err := r.base.ProduceSync(ctx, &kgo.Record{Topic: operator.PoolReceiversTopic(r.config.Pool), Key: []byte(r.id), Value: value}).FirstErr(); err != nil {
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
	// statuses holds, per run, the statuses from live pool members in log order, one per event ID, and records the
	// record of each.
	statuses map[string][]wire.RunStatus
	records  map[string][]*kgo.Record
	// startFailures holds, per run, this receiver's start failures in log order.
	startFailures map[string][]wire.StartFailure
	// others holds, per run, every other record, such as cancels.
	others map[string][][]byte
	// requests holds the open requests in log order; answered holds the digest of each request an attempt wrap or a
	// refusal answers.
	requests []openRequest
	answered map[string]bool
	// rekeys holds, per task hash, its re-key wraps; decisions the first decision on each claim, by the claim's digest.
	rekeys    map[string][]wire.RekeyWrap
	decisions map[string]wire.ClaimDecision
	// seen holds the event ID of each status in statuses; it is the fold's alone.
	seen map[string]bool
}

func newPoolEvents() poolEvents {
	return poolEvents{
		statuses: map[string][]wire.RunStatus{}, records: map[string][]*kgo.Record{}, startFailures: map[string][]wire.StartFailure{}, others: map[string][][]byte{},
		answered: map[string]bool{}, rekeys: map[string][]wire.RekeyWrap{}, decisions: map[string]wire.ClaimDecision{}, seen: map[string]bool{},
	}
}

// view is a copy of events a reader may keep while the fold goes on: the fold only appends past each slice's length.
func (events poolEvents) view() poolEvents {
	events.statuses, events.records, events.startFailures, events.others = maps.Clone(events.statuses), maps.Clone(events.records), maps.Clone(events.startFailures), maps.Clone(events.others)
	events.answered, events.rekeys, events.decisions, events.seen = maps.Clone(events.answered), maps.Clone(events.rekeys), maps.Clone(events.decisions), nil
	return events
}

// eventsFold is the pool's events folded once per process, from the log start to next.
type eventsFold struct {
	mu   sync.Mutex
	next int64
	// entries holds what each folded record that counts adds to events, in log order.
	entries []foldedEvent
	events  poolEvents
	// state, peers and ended are what every entry was verified and authorised against.
	state        operator.State
	peers, ended signed.Keys
}

// foldedEvent is what the record at offset adds to a view of the pool's events.
type foldedEvent struct {
	offset int64
	apply  func(*poolEvents)
}

func (fold *eventsFold) reset() {
	fold.next, fold.entries, fold.events = 0, nil, newPoolEvents()
}

// openRequest is an open request's record, with the request when it verifies as a live pool member's.
type openRequest struct {
	record  *kgo.Record
	digest  string
	request *wire.OpenRequest
}

// readEvents reads the pool's events after those already folded and returns the view a read of the whole topic gives.
func (r *Receiver) readEvents(ctx context.Context) (poolEvents, error) {
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	fold := &r.events
	fold.mu.Lock()
	defer fold.mu.Unlock()
	state, peers, _ := r.snapshot()
	r.mu.Lock()
	ended := r.ended
	r.mu.Unlock()
	// Any record's verification or authorisation may differ under other state or keys, so the fold starts again.
	if fold.events.seen == nil || !reflect.DeepEqual(fold.state, state) || !reflect.DeepEqual(fold.peers, peers) || !reflect.DeepEqual(fold.ended, ended) {
		fold.reset()
	}
	records, start, end, err := r.reader.ReadRange(ctx, operator.PoolEventsTopic(r.config.Pool), fold.next)
	if err != nil {
		return poolEvents{}, r.deny(err, capRead)
	}
	entries, err := r.foldEvents(ctx, records, state, peers, ended)
	if err != nil {
		return poolEvents{}, err
	}
	// Retention deletes from the log start: what a read of the whole topic no longer sees is dropped, records this read
	// fetched before the start moved included.
	all := append(fold.entries, entries...)
	retained := slices.IndexFunc(all, func(entry foldedEvent) bool { return entry.offset >= start })
	if retained < 0 {
		retained = len(all)
	}
	if retained > 0 {
		fold.entries, fold.events = slices.Clone(all[retained:]), newPoolEvents()
		for _, entry := range fold.entries {
			entry.apply(&fold.events)
		}
	} else {
		for _, entry := range entries {
			entry.apply(&fold.events)
		}
		fold.entries = all
	}
	fold.next, fold.state, fold.peers, fold.ended = max(fold.next, end), state, peers, ended
	return fold.events.view(), nil
}

// foldEvents verifies and authorises each record of the pool's events against state, peers and ended, and returns
// what each that counts adds to their view.
func (r *Receiver) foldEvents(ctx context.Context, records []*kgo.Record, state operator.State, peers, ended signed.Keys) ([]foldedEvent, error) {
	peerTopics := operator.PoolKeyTopics(state, r.config.Pool)
	reread := map[string]bool{}
	var entries []foldedEvent
	for _, record := range records {
		run := string(record.Key)
		history := parseHistory(record.Timestamp)
		// The JWS type picks the one parse a record gets; the parse checks the type again under the signature. parseErr
		// tells a record that does not verify, kept for the cancel check, from one that verifies and is refused.
		var status wire.RunStatus
		var failure wire.StartFailure
		var request wire.OpenRequest
		var attemptWrap wire.AttemptWrap
		var refusal wire.OpenRefusal
		var rekey wire.RekeyWrap
		var decision wire.ClaimDecision
		var parseErr error
		typ := recordType(record.Value)
		authorize := func(peers signed.Keys) error {
			parse := func(encoded []byte, keys signed.Keys, value wire.Record) error {
				parseErr = history(encoded, keys, value)
				return parseErr
			}
			// Open requests and re-key wraps verify only against live pool members' keys, which is their authorisation.
			strict := func(value wire.Record) error {
				parseErr = parseStrict(record.Value, peers, record.Timestamp, value)
				return parseErr
			}
			// A custodian's answers and claim decisions verify against the key current at their append time, also after
			// its membership ended.
			historic := func(value wire.Record) error {
				keys := peers
				if owner, _, _ := kid(record.Value); keys[owner].Current == nil && ended[owner].Current != nil {
					keys = maps.Clone(peers)
					keys[owner] = ended[owner]
				}
				parseErr = parseStrict(record.Value, keys, record.Timestamp, value)
				return parseErr
			}
			var err error
			switch typ {
			case wire.StatusType:
				status, err = operator.AuthorizePoolStatus(parse, state, peers, record.Value, r.config.Pool)
			case wire.StartFailureType:
				failure, err = operator.AuthorizePoolStartFailure(parse, state, peers, record.Value, r.config.Pool)
			case wire.OpenRequestType:
				err = strict(&request)
			case wire.AttemptWrapType:
				err = historic(&attemptWrap)
			case wire.OpenRefusalType:
				err = historic(&refusal)
			case wire.RekeyWrapType:
				err = strict(&rekey)
			case wire.ClaimDecisionType:
				err = historic(&decision)
			default:
				parseErr = errors.New("record is not a pool event")
			}
			return err
		}
		err := authorize(peers)
		// A member that rotated its key since the last refresh is re-read once per read of the topic.
		if owner, topic, stale := staleKey(record.Value, peers, peerTopics); parseErr != nil && stale && !reread[owner] {
			reread[owner] = true
			if peers, err = r.rereadKey(ctx, &r.peers, owner, topic); err != nil {
				return nil, err
			}
			err = authorize(peers)
		}
		var apply func(*poolEvents)
		switch {
		case typ == wire.OpenRequestType:
			// A request that does not verify is refused by its digest.
			opened := openRequest{record: record, digest: wire.Digest(record.Value)}
			if err == nil && request.Run == run {
				opened.request = &request
			}
			apply = func(events *poolEvents) { events.requests = append(events.requests, opened) }
		case parseErr != nil:
			apply = func(events *poolEvents) { events.others[run] = append(events.others[run], record.Value) }
		case err != nil:
			// A status or start failure that verifies but is not authorised counts for nothing.
		case typ == wire.StartFailureType:
			// A start failure binds its run's tenant through the task hash, which failedHere matches.
			if failure.Wherry.Run == run && failure.Wherry.Receiver == r.id {
				apply = func(events *poolEvents) { events.startFailures[run] = append(events.startFailures[run], failure) }
			}
		case typ == wire.AttemptWrapType:
			apply = func(events *poolEvents) { events.answered[attemptWrap.Request] = true }
		case typ == wire.OpenRefusalType:
			apply = func(events *poolEvents) { events.answered[refusal.Request] = true }
		case typ == wire.RekeyWrapType:
			apply = func(events *poolEvents) { events.rekeys[rekey.TaskHash] = append(events.rekeys[rekey.TaskHash], rekey) }
		case typ == wire.ClaimDecisionType:
			apply = func(events *poolEvents) {
				if _, decided := events.decisions[decision.Claim]; !decided {
					events.decisions[decision.Claim] = decision
				}
			}
		case status.Wherry.Run == run:
			// A status binds its run's tenant through the task hash; the pool, membership and entitlement are checked here.
			apply = func(events *poolEvents) {
				if !events.seen[status.Wherry.EventID] {
					events.seen[status.Wherry.EventID] = true
					events.statuses[run] = append(events.statuses[run], status)
					events.records[run] = append(events.records[run], record)
				}
			}
		}
		if apply != nil {
			entries = append(entries, foldedEvent{offset: record.Offset, apply: apply})
		}
	}
	return entries, nil
}

// committedTerminal reports a status in a terminal A2A state, a rejection included, for the exact task
// record on the pool's events.
func (events poolEvents) committedTerminal(run, taskHash string) bool {
	return slices.ContainsFunc(events.statuses[run], func(status wire.RunStatus) bool {
		return wire.Terminal(status.StatusUpdate.Status.State) && status.Wherry.TaskHash == taskHash
	})
}

// task is the run's task hash: that of its first status other than a rejection, or empty when it has none.
func (events poolEvents) task(run string) string {
	for _, status := range events.statuses[run] {
		if status.Wherry.Kind != "rejected" {
			return status.Wherry.TaskHash
		}
	}
	return ""
}

// claim is the latest claimed status of hash in run, or nil.
func (events poolEvents) claim(run, hash string) *wire.RunStatus {
	statuses := events.statuses[run]
	for i := len(statuses) - 1; i >= 0; i-- {
		if statuses[i].Wherry.Kind == "claimed" && statuses[i].Wherry.TaskHash == hash {
			return &statuses[i]
		}
	}
	return nil
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
		// A record that is not an authorised cancel changes nothing.
		if r.authorizeHost(ctx, encoded, func(state operator.State, hosts signed.Keys) error {
			cancel, err := operator.AuthorizeCancel(state, hosts, encoded, r.config.Pool, tenant)
			if err == nil && cancel.Run != run {
				return errors.New("cancel names another run than its key")
			}
			return err
		}) == nil {
			found = true
		}
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
