package client

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
)

// The dogfood case: nomad-1 has receiver-wide room, but its one claude replica is busy, so a claude task goes to the
// pool whose claude profile has room.
func TestResolvePlacementPrefersTheProfileWithRoomOverReceiverSpare(t *testing.T) {
	state := operator.NewState()
	state.Pools["pool-a"], state.Pools["pool-b"] = testTerms(), testTerms()
	state.Entitlements["tenant-a"] = map[string]bool{"pool-a": true, "pool-b": true}
	state.Receivers["nomad-1"] = operator.ReceiverConfig{Receiver: "nomad-1", Pool: "pool-a", Profiles: []operator.ProfileSettings{
		{Name: "claude", Plugin: "nomad", Agent: "claude"}, {Name: "codex", Plugin: "nomad", Agent: "codex", Replicas: 2},
	}}
	state.Receivers["nomad-2"] = operator.ReceiverConfig{Receiver: "nomad-2", Pool: "pool-b", Profiles: []operator.ProfileSettings{
		{Name: "claude", Plugin: "nomad", Agent: "claude"},
	}}
	ads := map[string]wire.Advertisement{
		"nomad-1": {Receiver: "nomad-1", Pool: "pool-a", Capacity: 3, InFlight: 1, Offers: []wire.Offer{
			{Profile: "claude", Agent: "claude", Replicas: 1, InFlight: 1}, {Profile: "codex", Agent: "codex", Replicas: 2},
		}},
		"nomad-2": {Receiver: "nomad-2", Pool: "pool-b", Capacity: 1, Offers: []wire.Offer{{Profile: "claude", Agent: "claude", Replicas: 1}}},
	}
	if pool, _, _, err := resolvePlacement(state, "tenant-a", "claude", "", nil, ads); err != nil || pool != "pool-b" {
		t.Fatalf("placed %q, %v; want pool-b, whose claude profile has room", pool, err)
	}
}

// A receiver that advertises it is not claiming adds no spare, and a profile holding more runs than its replicas adds
// none rather than taking spare away.
func TestResolvePlacementCreditsNoGatedOrOverfullSpare(t *testing.T) {
	state := operator.NewState()
	state.Pools["pool-a"], state.Pools["pool-b"] = testTerms(), testTerms()
	state.Entitlements["tenant-a"] = map[string]bool{"pool-a": true, "pool-b": true}
	for id, pool := range map[string]string{"receiver-a": "pool-a", "receiver-b": "pool-a", "receiver-c": "pool-b"} {
		state.Receivers[id] = operator.ReceiverConfig{Receiver: id, Pool: pool, Profiles: []operator.ProfileSettings{{Name: "lab", Plugin: "nomad", Agent: "claude"}}}
	}
	offer := func(replicas, inFlight int64) []wire.Offer {
		return []wire.Offer{{Profile: "lab", Agent: "claude", Replicas: replicas, InFlight: inFlight}}
	}
	gated := map[string]wire.Advertisement{
		"receiver-a": {Receiver: "receiver-a", Pool: "pool-a", Gated: wire.GatedPendingRun, Offers: offer(5, 0)},
		"receiver-c": {Receiver: "receiver-c", Pool: "pool-b", Offers: offer(1, 0)},
	}
	if pool, _, _, err := resolvePlacement(state, "tenant-a", "claude", "", nil, gated); err != nil || pool != "pool-b" {
		t.Fatalf("placed %q, %v; want pool-b over a gated receiver's 5 free replicas", pool, err)
	}
	// pool-a: 0 for the overfull profile and 1; pool-b: 1. Equal, so the lower topic.
	overfull := map[string]wire.Advertisement{
		"receiver-a": {Receiver: "receiver-a", Pool: "pool-a", Offers: offer(1, 3)},
		"receiver-b": {Receiver: "receiver-b", Pool: "pool-a", Offers: offer(1, 0)},
		"receiver-c": {Receiver: "receiver-c", Pool: "pool-b", Offers: offer(1, 0)},
	}
	want := "pool-a"
	if operator.SignatureTopic("pool-b", "claude", nil) < operator.SignatureTopic("pool-a", "claude", nil) {
		want = "pool-b"
	}
	if pool, _, _, err := resolvePlacement(state, "tenant-a", "claude", "", nil, overfull); err != nil || pool != want {
		t.Fatalf("placed %q, %v; want %s, the lower topic at equal spare", pool, err, want)
	}
}

func TestWaitReason(t *testing.T) {
	state := operator.NewState()
	state.Pools["pool-a"], state.Pools["pool-b"] = testTerms(), testTerms()
	zone := map[string]string{"zone": "a"}
	state.Receivers["receiver-a"] = operator.ReceiverConfig{Receiver: "receiver-a", Pool: "pool-a", Profiles: []operator.ProfileSettings{
		{Name: "claude", Plugin: "nomad", Agent: "claude", Labels: zone},
		// It satisfies any selector the run's profile does, but carries one more label: another signature topic.
		{Name: "wide", Plugin: "nomad", Agent: "claude", Labels: map[string]string{"zone": "a", "gpu": "x"}},
	}}
	state.Receivers["receiver-b"] = operator.ReceiverConfig{Receiver: "receiver-b", Pool: "pool-a", Profiles: []operator.ProfileSettings{
		{Name: "claude", Plugin: "nomad", Agent: "claude", Labels: zone}, {Name: "twin", Plugin: "nomad", Agent: "claude", Labels: zone},
	}}
	state.Receivers["receiver-c"] = operator.ReceiverConfig{Receiver: "receiver-c", Pool: "pool-b", Profiles: []operator.ProfileSettings{
		{Name: "claude", Plugin: "nomad", Agent: "claude", Labels: zone},
	}}
	topic := operator.SignatureTopic("pool-a", "claude", zone)
	offer := func(profile string, replicas, inFlight int64) wire.Offer {
		return wire.Offer{Profile: profile, Agent: "claude", Replicas: replicas, InFlight: inFlight}
	}
	ad := func(receiver, gated string, offers ...wire.Offer) wire.Advertisement {
		return wire.Advertisement{Receiver: receiver, Pool: state.Receivers[receiver].Pool, Gated: gated, Offers: offers}
	}
	full, room := offer("claude", 1, 1), offer("claude", 2, 1)
	cases := map[string]struct {
		suspended bool
		ads       []wire.Advertisement
		want      string
		queued    bool
	}{
		"full":                            {false, []wire.Advertisement{ad("receiver-a", "", full)}, "claude on receiver-a 1/1 in use", true},
		"room":                            {false, []wire.Advertisement{ad("receiver-a", "", room)}, "claude on receiver-a advertises room", true},
		"a superset-label profile":        {false, []wire.Advertisement{ad("receiver-a", "", full, offer("wide", 1, 0))}, "claude on receiver-a 1/1 in use", true},
		"only another topic":              {false, []wire.Advertisement{ad("receiver-a", "", offer("wide", 1, 0))}, "no live profile offers the run's signature", false},
		"another pool":                    {false, []wire.Advertisement{ad("receiver-c", "", room)}, "no live profile offers the run's signature", false},
		"none":                            {false, nil, "no live profile offers the run's signature", false},
		"two profiles of one signature":   {false, []wire.Advertisement{ad("receiver-b", "", full, offer("twin", 1, 0))}, "twin on receiver-b advertises room", true},
		"every profile full":              {false, []wire.Advertisement{ad("receiver-a", "", full), ad("receiver-b", "", offer("claude", 2, 2), offer("twin", 1, 1))}, "claude on receiver-a 1/1 in use, claude on receiver-b 2/2 in use, twin on receiver-b 1/1 in use", true},
		"a gated receiver once":           {false, []wire.Advertisement{ad("receiver-b", wire.GatedPendingRun, room, offer("twin", 1, 0))}, "receiver-b is not claiming (pending_run)", true},
		"gated beside full":               {false, []wire.Advertisement{ad("receiver-a", "", full), ad("receiver-b", wire.GatedDraining, room)}, "claude on receiver-a 1/1 in use, receiver-b is not claiming (draining)", true},
		"gated beside room":               {false, []wire.Advertisement{ad("receiver-a", wire.GatedSuspended, room), ad("receiver-b", "", room)}, "claude on receiver-b advertises room", true},
		"suspended with room":             {true, []wire.Advertisement{ad("receiver-a", "", room)}, "pool pool-a is suspended", true},
		"suspended with no live profiles": {true, nil, "pool pool-a is suspended", true},
	}
	for _, gated := range []string{wire.GatedSuspended, wire.GatedNoReadyOffer, wire.GatedPendingRun, wire.GatedDraining} {
		cases["gated "+gated] = struct {
			suspended bool
			ads       []wire.Advertisement
			want      string
			queued    bool
		}{false, []wire.Advertisement{ad("receiver-a", gated, room)}, "receiver-a is not claiming (" + gated + ")", true}
	}
	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			state.Suspended = map[string]bool{"pool-a": test.suspended}
			ads := map[string]wire.Advertisement{}
			for _, ad := range test.ads {
				ads[ad.Receiver] = ad
			}
			if reason, queued := waitReason(state, ads, "pool-a", topic); reason != test.want || queued != test.queued {
				t.Fatalf("reason %q queued %t, want %q %t", reason, queued, test.want, test.queued)
			}
		})
	}
}

// A queued run's status says why it waits, as the latest advertisement shows it at each call, and says nothing once a
// receiver reports a status.
func TestStatusSaysWhyAQueuedRunWaits(t *testing.T) {
	f := newFixture(t)
	runID := f.submitQueued(t, 10*time.Minute)
	receiver := f.receivers["receiver-a"]
	waiting := func(want string) {
		t.Helper()
		status, err := f.host.Status(f.ctx, runID)
		if err != nil || status.Waiting != want || status.Unclaimed != "" {
			t.Fatalf("status %#v, %v; want waiting %q", status, err, want)
		}
	}
	waiting("queued: lab on receiver-a advertises room")
	receiver.advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.Offers[0].Replicas, ad.Offers[0].InFlight = 1, 1 })
	waiting("queued: lab on receiver-a 1/1 in use")
	receiver.advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.Gated = wire.GatedPendingRun })
	waiting("queued: receiver-a is not claiming (pending_run)")
	receiver.advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.Offers = []wire.Offer{} })
	waiting("no live profile offers the run's signature")
	if err := f.service.PoolSuspend(f.ctx, "pool-a", true); err != nil {
		t.Fatal(err)
	}
	waiting("queued: pool pool-a is suspended")
	if err := f.service.PoolSuspend(f.ctx, "pool-a", false); err != nil {
		t.Fatal(err)
	}
	task := receiver.claim(t, f.ctx, runID)[0]
	receiver.feed(t, f.ctx, runID, receiver.echo(t, task), receiver.status(t, task, 1, "claimed-1", "claimed", wire.TaskStateWorking, nil))
	waiting("")
}

// Past task_started_by the reason follows the unclaimed text, as of the call, and changes with the advertisements.
func TestTheUnclaimedTextSaysWhyAsOfTheCall(t *testing.T) {
	f := newFixture(t)
	runID := f.submitQueued(t, 2*time.Second)
	receiver := f.receivers["receiver-a"]
	receiver.advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.Offers[0].Replicas, ad.Offers[0].InFlight = 1, 1 })
	awaited, err := f.host.Await(f.ctx, runID, 20*time.Second)
	if err != nil || !strings.HasPrefix(awaited.Unclaimed, "not started by task_started_by (") ||
		!strings.HasSuffix(awaited.Unclaimed, "): no receiver claimed the task; now: lab on receiver-a 1/1 in use") {
		t.Fatalf("await %#v, %v; want the full profile as the reason", awaited, err)
	}
	receiver.advertise(t, f.ctx, func(ad *wire.Advertisement) { ad.Offers = []wire.Offer{} })
	status, err := f.host.Status(f.ctx, runID)
	if err != nil || status.Waiting != "" || !strings.HasSuffix(status.Unclaimed, "): no receiver claimed the task; now: no live profile offers the run's signature") {
		t.Fatalf("status %#v, %v; want the reason recomputed", status, err)
	}
}

// Runs reads the advertisements once, however many runs wait.
func TestRunsReadsTheAdvertisementsOnce(t *testing.T) {
	f := newFixture(t)
	first := f.submitQueued(t, 10*time.Minute)
	second := f.submit(t, defaultSelector()).RunID
	reads := 0
	read := liveAds
	liveAds = func(ctx context.Context, h *Host, v view) (map[string]wire.Advertisement, error) {
		reads++
		return read(ctx, h, v)
	}
	t.Cleanup(func() { liveAds = read })
	runs, err := f.host.Runs(f.ctx)
	if err != nil || len(runs.Runs) != 2 || reads != 1 {
		t.Fatalf("runs %#v, %v after %d reads; want both runs from one read", runs, err, reads)
	}
	for _, run := range runs.Runs {
		if (run.RunID != first && run.RunID != second) || run.Waiting != "queued: lab on receiver-a advertises room" {
			t.Fatalf("run %#v, want it queued with room", run)
		}
	}
}

// The dogfood scenario: receiver nomad-1 has three profiles and room overall, but its one claude replica is held, so a
// second claude run is queued on the full profile, which receivers shows.
func TestAQueuedRunOnAFullProfileSaysSo(t *testing.T) {
	f := newFixture(t)
	f.addReceiverConfig(t, operator.ReceiverConfig{Receiver: "nomad-1", Pool: "pool-a", Profiles: []operator.ProfileSettings{
		{Name: "claude", Plugin: "nomad", Agent: "claude", Labels: map[string]string{"runtime": "podman"}},
		{Name: "codex", Plugin: "nomad", Agent: "codex", Labels: map[string]string{"runtime": "podman"}},
		{Name: "exec", Plugin: "nomad", Agent: "claude", Labels: map[string]string{"runtime": "exec"}},
	}, PluginSHA256: map[string]string{"nomad": strings.Repeat("ab", 32)}}).advertise(t, f.ctx, func(ad *wire.Advertisement) {
		ad.Capacity, ad.InFlight = 3, 1
		ad.Offers = []wire.Offer{
			{Profile: "claude", Agent: "claude", Replicas: 1, InFlight: 1}, {Profile: "codex", Agent: "codex", Replicas: 1}, {Profile: "exec", Agent: "claude", Replicas: 1},
		}
	})
	submitted, err := f.host.Submit(f.ctx, SubmitInput{Agent: "claude", Selector: defaultSelector(), Prompt: "work", EndBy: time.Now().Add(time.Hour), StartBy: time.Now().Add(10 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if status, err := f.host.Status(f.ctx, submitted.RunID); err != nil || status.Waiting != "queued: claude on nomad-1 1/1 in use" {
		t.Fatalf("status %#v, %v; want the full claude profile", status, err)
	}
	receivers, err := f.host.Receivers(f.ctx)
	if err != nil || len(receivers) != 1 || receivers[0].Capacity != 3 || receivers[0].InFlight != 1 {
		t.Fatalf("receivers %#v, %v", receivers, err)
	}
	if claude := receivers[0].Profiles[0]; claude.Name != "claude" || !claude.Ready || claude.Replicas != 1 || claude.InFlight != 1 {
		t.Fatalf("claude profile %#v, want it full at 1/1", claude)
	}
}

// cancelledRuns lists the runs the cancels on pool's events name, unverified.
func (f *fixture) cancelledRuns(t *testing.T, pool string) []string {
	t.Helper()
	records, err := readTopic(f.ctx, f.service.Operator, operator.PoolEventsTopic(pool))
	if err != nil {
		t.Fatal(err)
	}
	var runs []string
	for _, record := range records {
		var cancel wire.Cancel
		if err := json.Unmarshal(payload(t, record.Value), &cancel, signed.Tolerant); err != nil {
			t.Fatal(err)
		}
		runs = append(runs, cancel.Run)
	}
	return runs
}

// A cancel of a run no receiver has claimed is recorded at its first request, shown as pending until a receiver reports
// a status, and a repeated cancel keeps the first time.
func TestACancelWaitsForAClaim(t *testing.T) {
	f := newFixture(t)
	runID := f.submitQueued(t, 10*time.Minute)
	if result, err := f.host.Cancel(f.ctx, runID); err != nil || result.RunID != runID {
		t.Fatalf("cancel %#v, %v", result, err)
	}
	local, err := f.host.localRun(runID)
	if err != nil || local.CancelRequestedMS == 0 {
		t.Fatalf("local record %#v, %v; want the cancel recorded", local, err)
	}
	want := "cancel requested " + time.UnixMilli(local.CancelRequestedMS).UTC().Format(time.RFC3339) + "; applies when a receiver claims the task"
	if status, err := f.host.Status(f.ctx, runID); err != nil || status.CancelRequested != want || status.Waiting == "" {
		t.Fatalf("status %#v, %v; want %q beside the waiting reason", status, err, want)
	}
	time.Sleep(10 * time.Millisecond)
	if _, err := f.host.Cancel(f.ctx, runID); err != nil {
		t.Fatal(err)
	}
	if again, err := f.host.localRun(runID); err != nil || again.CancelRequestedMS != local.CancelRequestedMS {
		t.Fatalf("cancel time %d after a second cancel, want the first %d", again.CancelRequestedMS, local.CancelRequestedMS)
	}
	if runs := f.cancelledRuns(t, "pool-a"); len(runs) != 2 || runs[0] != runID || runs[1] != runID {
		t.Fatalf("cancels for %v, want two of %s", runs, runID)
	}
	receiver := f.receivers["receiver-a"]
	task := receiver.claim(t, f.ctx, runID)[0]
	receiver.feed(t, f.ctx, runID, receiver.echo(t, task), receiver.status(t, task, 1, "claimed-1", "claimed", wire.TaskStateWorking, nil))
	if status, err := f.host.Status(f.ctx, runID); err != nil || status.Status.State != wire.TaskStateWorking || status.CancelRequested != "" {
		t.Fatalf("status %#v, %v; want a claimed, unfinished run without the pending cancel", status, err)
	}
}

// A pending cancel is still shown once task_started_by passes, by wait and status.
func TestACancelWaitsForAClaimPastTheStartBound(t *testing.T) {
	f := newFixture(t)
	runID := f.submitQueued(t, 2*time.Second)
	if _, err := f.host.Cancel(f.ctx, runID); err != nil {
		t.Fatal(err)
	}
	awaited, err := f.host.Await(f.ctx, runID, 20*time.Second)
	if err != nil || awaited.Unclaimed == "" || !strings.HasPrefix(awaited.CancelRequested, "cancel requested ") {
		t.Fatalf("await %#v, %v; want unclaimed with the pending cancel", awaited, err)
	}
	if status, err := f.host.Status(f.ctx, runID); err != nil || status.Unclaimed == "" || status.CancelRequested != awaited.CancelRequested {
		t.Fatalf("status %#v, %v; want unclaimed with %q", status, err, awaited.CancelRequested)
	}
}

// A cancel of a run that was resubmitted cancels its replacement, which shows it, and leaves the original's record.
func TestCancellingAChainHeadCancelsItsReplacement(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	receiver.keyLost(t, f.ctx, task)
	replacement, err := f.host.resubmit(f.ctx, runID, true)
	if err != nil || replacement == "" {
		t.Fatalf("resubmit = %q, %v", replacement, err)
	}
	if result, err := f.host.Cancel(f.ctx, runID); err != nil || result.RunID != replacement {
		t.Fatalf("cancel %#v, %v; want the replacement %s cancelled", result, err, replacement)
	}
	if runs := f.cancelledRuns(t, "pool-a"); len(runs) != 1 || runs[0] != replacement {
		t.Fatalf("cancels for %v, want the replacement's", runs)
	}
	if status, err := f.host.Status(f.ctx, replacement); err != nil || !strings.HasPrefix(status.CancelRequested, "cancel requested ") {
		t.Fatalf("replacement status %#v, %v; want the pending cancel", status, err)
	}
	if original, err := f.host.localRun(runID); err != nil || original.Replacement != replacement || original.CancelRequestedMS != 0 {
		t.Fatalf("original record %#v, %v; want its replacement kept and no cancel", original, err)
	}
}

// A cancel issued while a resubmission holds the run's lock waits for it, keeps the replacement it records, and cancels
// the replacement.
func TestACancelRacingAResubmissionCancelsTheReplacement(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	receiver.keyLost(t, f.ctx, task)
	entered := make(chan struct{})
	replacing = func(string) {
		close(entered)
		time.Sleep(time.Second)
	}
	t.Cleanup(func() { replacing = func(string) {} })
	resubmitted := make(chan string, 1)
	go func() {
		replacement, err := f.host.resubmit(f.ctx, runID, true)
		if err != nil {
			t.Error(err)
		}
		resubmitted <- replacement
	}()
	<-entered
	result, err := f.host.Cancel(f.ctx, runID)
	replacement := <-resubmitted
	if err != nil || replacement == "" || result.RunID != replacement {
		t.Fatalf("cancel %#v, %v; want the replacement %q cancelled", result, err, replacement)
	}
	if original, err := f.host.localRun(runID); err != nil || original.Replacement != replacement {
		t.Fatalf("original record %#v, %v; want the replacement %s kept", original, err, replacement)
	}
	if local, err := f.host.localRun(replacement); err != nil || local.CancelRequestedMS == 0 || !local.Produced {
		t.Fatalf("replacement record %#v, %v; want it produced and its cancel recorded", local, err)
	}
}

// A cancel recorded before a run that ended key-lost is resubmitted stops the resubmission, so no replacement runs the
// cancelled task.
func TestACancelBeforeAResubmissionStopsIt(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	receiver.keyLost(t, f.ctx, task)
	if result, err := f.host.Cancel(f.ctx, runID); err != nil || result.RunID != runID {
		t.Fatalf("cancel %#v, %v", result, err)
	}
	if replacement, err := f.host.resubmit(f.ctx, runID, true); err != nil || replacement != "" {
		t.Fatalf("resubmit = %q, %v; want no replacement of a cancelled run", replacement, err)
	}
	if status, err := f.host.Status(f.ctx, runID); err != nil || status.Replacement != "" {
		t.Fatalf("status %#v, %v; want no replacement", status, err)
	}
	if local, err := f.host.localRun(runID); err != nil || local.Replacement != "" {
		t.Fatalf("record %#v, %v; want no replacement recorded", local, err)
	}
	if replacements := f.replacements(t, "pool-a", runID); len(replacements) != 0 {
		t.Fatalf("%d replacements produced, want none", len(replacements))
	}
}

// A cancel issued once the replacement's record is written, while the resubmission produces it, waits for the run it
// replaces, so neither write loses the other's: the replacement stays produced and keeps its cancel.
func TestACancelDuringAReplacementsProduceKeepsBoth(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	receiver.keyLost(t, f.ctx, task)
	produce := produceReplacement
	cancelled := make(chan error, 1)
	produceReplacement = func(ctx context.Context, h *Host, topic string, binding wire.SubmissionBinding, encoded []byte) error {
		go func() {
			_, err := h.Cancel(ctx, runID)
			cancelled <- err
		}()
		// A cancel holding only the replacement's lock would finish here, writing the record before it is produced.
		select {
		case err := <-cancelled:
			cancelled <- err
		case <-time.After(time.Second):
		}
		return produce(ctx, h, topic, binding, encoded)
	}
	t.Cleanup(func() { produceReplacement = produce })
	replacement, err := f.host.resubmit(f.ctx, runID, true)
	if err != nil || replacement == "" {
		t.Fatalf("resubmit = %q, %v", replacement, err)
	}
	if err := <-cancelled; err != nil {
		t.Fatal(err)
	}
	if local, err := f.host.localRun(replacement); err != nil || !local.Produced || local.CancelRequestedMS == 0 {
		t.Fatalf("replacement record %#v, %v; want it produced and its cancel recorded", local, err)
	}
	if replacements := f.replacements(t, "pool-a", runID); len(replacements) != 1 {
		t.Fatalf("%d replacements produced, want one", len(replacements))
	}
}

// When the advertisements cannot be read, status, the run list and wait past task_started_by still answer, without a
// reason; the run list reads them only for a run that waits.
func TestAQueuedRunsStatusWithoutAdvertisements(t *testing.T) {
	f := newFixture(t)
	f.admitted(t)
	reads := 0
	read := liveAds
	liveAds = func(context.Context, *Host, view) (map[string]wire.Advertisement, error) {
		reads++
		return nil, errors.New("receivers topic unreadable")
	}
	t.Cleanup(func() { liveAds = read })
	if runs, err := f.host.Runs(f.ctx); err != nil || len(runs.Runs) != 1 || reads != 0 {
		t.Fatalf("runs %#v, %v after %d reads; want the claimed run without reading advertisements", runs, err, reads)
	}
	submitted, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", EndBy: time.Now().Add(time.Minute), StartBy: time.Now().Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	runID := submitted.RunID
	if status, err := f.host.Status(f.ctx, runID); err != nil || status.Status.State != wire.TaskStateSubmitted || status.Waiting != "" {
		t.Fatalf("status %#v, %v; want it submitted without a reason", status, err)
	}
	if runs, err := f.host.Runs(f.ctx); err != nil || len(runs.Runs) != 2 || reads != 2 {
		t.Fatalf("runs %#v, %v after %d reads; want both runs", runs, err, reads)
	}
	awaited, err := f.host.Await(f.ctx, runID, 20*time.Second)
	if err != nil || !strings.HasSuffix(awaited.Unclaimed, ": no receiver claimed the task") {
		t.Fatalf("await %#v, %v; want the unclaimed text without a reason", awaited, err)
	}
}

// LastStatus reads the last run of a resubmission chain, which shows the waiting reason and the pending cancel.
func TestLastStatusFollowsTheChain(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	receiver.keyLost(t, f.ctx, task)
	replacement, err := f.host.resubmit(f.ctx, runID, true)
	if err != nil || replacement == "" {
		t.Fatalf("resubmit = %q, %v", replacement, err)
	}
	if _, err := f.host.Cancel(f.ctx, runID); err != nil {
		t.Fatal(err)
	}
	status, err := f.host.LastStatus(f.ctx, runID)
	if err != nil || status.RunID != replacement || status.Waiting == "" || !strings.HasPrefix(status.CancelRequested, "cancel requested ") {
		t.Fatalf("last status %#v, %v; want the replacement %s's reason and pending cancel", status, err, replacement)
	}
}
