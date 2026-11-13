package receiver

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
	"github.com/twmb/franz-go/pkg/kgo"
)

// advertised advertises once and returns the advertisement the receiver wrote.
func (f fixture) advertised(t *testing.T) wire.Advertisement {
	t.Helper()
	if err := f.receiver.advertise(f.ctx); err != nil {
		t.Fatal(err)
	}
	return f.advertisedOffers(t)
}

// offerUse maps each offer of ad to its in_flight and replicas.
func offerUse(ad wire.Advertisement) map[string][2]int64 {
	use := map[string][2]int64{}
	for _, offer := range ad.Offers {
		use[offer.Profile] = [2]int64{offer.InFlight, offer.Replicas}
	}
	return use
}

// Each offer counts the runs its profile's slots hold, apart from the other profile's, rising as a run is claimed and
// falling once it ends, and carries the profile's replicas.
func TestAnOfferCountsItsProfilesHeldRuns(t *testing.T) {
	a := signatureProfile("a", "claude", map[string]string{"sig": "a"})
	a.Replicas = 2
	f := signatureFixture(t, a, signatureProfile("b", "claude", map[string]string{"sig": "b"}))
	f.plugins["a"].endDelay = 3 * time.Second
	if use := offerUse(f.advertised(t)); use["a"] != [2]int64{0, 2} || use["b"] != [2]int64{0, 1} {
		t.Fatalf("offers in use %v before any run, want a 0/2 and b 0/1", use)
	}
	f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.Placement.Selector = []wire.Requirement{in("sig", "a")}
	})
	f.run(t, f.ctx)
	waitFor(t, f, func() bool { _, _, follows, _ := f.plugins["a"].counts(); return follows == 1 })
	ad := f.advertised(t)
	if use := offerUse(ad); use["a"] != [2]int64{1, 2} || use["b"] != [2]int64{0, 1} || ad.InFlight != 1 || ad.Gated != "" {
		t.Fatalf("advertisement %+v while a holds a run, want a 1/2, b 0/1, 1 in flight and no gate", ad)
	}
	waitFor(t, f, func() bool { return f.receiver.held() == 0 })
	if use := offerUse(f.advertised(t)); use["a"] != [2]int64{0, 2} {
		t.Fatalf("offers in use %v after the run ended, want a back to 0/2", use)
	}
}

// A claimed run past its bound whose profile went unready after its slot polled it is routed to no profile, yet the
// slot's profile counts it while the run ends.
func TestAnEndingRunRoutedToNoProfileCountsAgainstItsSlot(t *testing.T) {
	f := signatureFixture(t, signatureProfile("nomad", "claude", map[string]string{"zone": "dc1"}))
	f.plugin.job = pluginsdk.JobRunning
	startBy := time.Now().Add(time.Second).Unix()
	record := f.submit(t, f.host, func(task *wire.Submission) {
		task.Wherry.TaskStartedBy = startBy
		task.Wherry.Placement.Selector = []wire.Requirement{in("zone", "dc1")}
	})
	f.publishStatus(t, record, testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
	passed(startBy)
	f.ready(t, "nomad", true)
	counted := int64(-1)
	f.plugin.stopCheck = func() error {
		f.receiver.mu.Lock()
		counted = f.receiver.profileInFlight["nomad"]
		f.receiver.mu.Unlock()
		return nil
	}
	old := newClaimClient
	t.Cleanup(func() { newClaimClient = old })
	// The first delivery is released as on a suspension, so the second is a redelivery that finds the run's job.
	// The receiver replaces its state whole and readers share its maps, so the suspension is a new map.
	suspended := f.receiver.state.Suspended
	claimOnce(t, f, func() { f.receiver.state.Suspended = map[string]bool{testPool: true} })
	f.receiver.mu.Lock()
	f.receiver.state.Suspended = suspended
	f.receiver.mu.Unlock()
	claimOnce(t, f, func() {
		f.plugin.mu.Lock()
		f.plugin.unready = true
		f.plugin.mu.Unlock()
		f.receiver.offers = nil
	})
	f.plugin.mu.Lock()
	stops := f.plugin.stops
	f.plugin.mu.Unlock()
	if stops != 1 || counted != 1 {
		t.Fatalf("stops %d, nomad counted %d while the job stopped; want the ending run counted to its slot; logs:\n%s", stops, counted, f.logs.String())
	}
	if f.receiver.profileInFlight["nomad"] != 0 {
		t.Fatalf("nomad counts %d once the run ended, want 0", f.receiver.profileInFlight["nomad"])
	}
}

// claimOnce runs one claim of the fixture's first profile, calling withdraw under the receiver's lock as a poll returns
// a record, as a change between poll and admission does.
func claimOnce(t *testing.T, f fixture, withdraw func()) {
	t.Helper()
	newClaimClient = func(r *Receiver, _ Named, opts ...kgo.Opt) (*kgo.Client, error) {
		return r.newClient(append(opts, kgo.WithHooks(onPoll(func() {
			r.mu.Lock()
			withdraw()
			r.mu.Unlock()
		})))...)
	}
	if err := f.receiver.claimOne(f.ctx, f.ctx, f.receiver.profiles[0], func() {}); err != nil {
		t.Fatal(err)
	}
}

type onPoll func()

func (hook onPoll) OnFetchRecordUnbuffered(_ *kgo.Record, polled bool) {
	if polled {
		hook()
	}
}

// A receiver draining held runs after a configuration change advertises draining, its held run still counted.
func TestAConfigChangeDrainAdvertisesDraining(t *testing.T) {
	f := newFixtureWith(t, resultStream, testTerms(), testConfig(testReceiver), map[string]*fakePlugin{"nomad": {stream: resultStream, endDelay: 3 * time.Second}})
	done, _ := holdRun(t, f)
	if ad := f.advertised(t); ad.Gated != "" {
		t.Fatalf("gated %q while claiming, want none", ad.Gated)
	}
	config := testConfig(testReceiver)
	config.Profiles[0].Replicas = 2
	f.appendConfig(t, config)
	waitFor(t, f, func() bool { return strings.Contains(f.logs.String(), "configuration changed") })
	ad := f.advertised(t)
	if index := slices.IndexFunc(ad.Offers, func(offer wire.Offer) bool { return offer.Profile == "nomad" }); ad.Gated != wire.GatedDraining || index < 0 ||
		ad.Offers[index].InFlight != 1 || ad.Offers[index].Replicas != 1 {
		t.Fatalf("advertisement %+v while draining, want gated draining with the held run counted against the running process's 1 replica", ad)
	}
	if err := f.stopped(t, done, 20*time.Second); !errors.Is(err, ErrConfigChanged) {
		t.Fatalf("Run returned %v, want ErrConfigChanged", err)
	}
}

// A receiver that claims nothing advertises why: its pool suspended, no ready offer, or a run waiting for its platform.
func TestAGatedReceiverAdvertisesWhy(t *testing.T) {
	f := newBareFixture(t)
	r := f.receiver
	if ad := f.advertised(t); ad.Gated != "" {
		t.Fatalf("gated %q while claiming, want none", ad.Gated)
	}
	r.mu.Lock()
	r.state.Suspended = map[string]bool{testPool: true}
	r.mu.Unlock()
	if ad := f.advertised(t); ad.Gated != wire.GatedSuspended {
		t.Fatalf("gated %q while the pool is suspended, want %s", ad.Gated, wire.GatedSuspended)
	}
	r.mu.Lock()
	r.state.Suspended = nil
	r.mu.Unlock()
	run := &activeRun{receiver: r}
	run.pending()
	if ad := f.advertised(t); ad.Gated != wire.GatedPendingRun {
		t.Fatalf("gated %q while a run waits for its platform, want %s", ad.Gated, wire.GatedPendingRun)
	}
	run.placed()
	f.plugin.mu.Lock()
	f.plugin.unready = true
	f.plugin.mu.Unlock()
	if ad := f.advertised(t); ad.Gated != wire.GatedNoReadyOffer || len(ad.Offers) != 0 {
		t.Fatalf("advertisement %+v with no ready profile, want gated %s", ad, wire.GatedNoReadyOffer)
	}
}
