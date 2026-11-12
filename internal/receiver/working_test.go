package receiver

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	"github.com/benemon/wherry/pluginsdk"
	"github.com/twmb/franz-go/pkg/kgo"
)

// workingTexts are the message texts of the working statuses among statuses, raw parts as their bytes.
func workingTexts(statuses []wire.RunStatus) []string {
	var texts []string
	for _, status := range statuses {
		if status.Wherry.Kind == "working" {
			texts = append(texts, workingText(status))
		}
	}
	return texts
}

// readsInTurn makes the fixture's job read each of files in turn, the last one ever after, and end once every file
// has been read and three reads more have passed, so every read has had a tick to publish.
func readsInTurn(f fixture, files ...[]byte) {
	hold := make(chan struct{})
	var once sync.Once
	reads := 0
	f.plugin.followHold = hold
	f.plugin.readWorking = func(context.Context) ([]byte, error) {
		f.plugin.mu.Lock()
		defer f.plugin.mu.Unlock()
		reads++
		if reads >= len(files)+3 {
			once.Do(func() { close(hold) })
		}
		return files[min(reads, len(files))-1], nil
	}
}

// workingFixture is a fixture whose job runs until test ends it, read on every tick.
func workingFixture(t *testing.T) fixture {
	t.Helper()
	setDuration(t, &workingFloor, 0)
	return newFixture(t, resultStream)
}

// The tick after a read whose normalised line differs from the last one publishes it, on the pool's events and the
// feed, with nothing else to flush. Only the first line counts, cut at 512 bytes; a change past either, an oversized
// file, an empty one or a missing one publishes nothing, and none is an error.
func TestWorkingPublishesEachChangedLine(t *testing.T) {
	f := workingFixture(t)
	long := strings.Repeat("a", wire.MaxWorkingText)
	readsInTurn(f,
		[]byte("step one\nfirst detail"), []byte("step one\nsecond detail"), []byte("step one\n"),
		[]byte(long+"b"), []byte(long+"c\nmore"),
		[]byte(strings.Repeat("z", pluginsdk.MaxWorking+1)), []byte(""), nil, []byte("  \t\n"),
		[]byte("step two\r\n"))
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	want := []string{"step one", long, "step two"}
	for name, statuses := range map[string][]wire.RunStatus{"events": f.eventStatuses(t), "feed": f.feedStatuses(t)} {
		if texts := workingTexts(statuses); !slices.Equal(texts, want) {
			t.Fatalf("%s working %q, want %q", name, texts, want)
		}
	}
	if kinds := f.kinds(t); !slices.Equal(kinds, []string{"claimed", "started", "working", "working", "working", "terminal"}) {
		t.Fatalf("statuses %v, want the working statuses between started and terminal", kinds)
	}
	if strings.Contains(f.logs.String(), "read working") {
		t.Fatalf("a missing, empty or oversized file was logged as an error:\n%s", f.logs.String())
	}
}

// The whole file is redacted before its first line is taken and cut, so a credential longer than the cap and one
// that spans lines are redacted; a URL is stripped and control characters become spaces.
func TestWorkingIsRedactedWhole(t *testing.T) {
	f := workingFixture(t)
	long := strings.Repeat("L", 600)
	multiline := "deploy-secret-first-half\ndeploy-secret-second-half"
	f.envelope = sealing(map[string][]byte{credentialName: []byte(long), "deploy-key": []byte(multiline)})
	readsInTurn(f,
		[]byte("using "+long+" now"),
		[]byte("pushing with "+multiline),
		[]byte("fetching\x1b[31m https://example.com/object?X-Amz-Signature=abc red"))
	if status := f.execute(t, f.submit(t, f.host, withDeployKey), 1); status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	want := []string{"using " + redacted + " now", "pushing with " + signed.RedactionMarker(testRedactionKey, "deploy-key"), "fetching [31m [redacted-url] red"}
	texts := workingTexts(f.feedStatuses(t))
	if !slices.Equal(texts, want) {
		t.Fatalf("working %q, want %q", texts, want)
	}
	for _, text := range texts {
		if strings.Contains(text, "LLLLLLLL") || strings.Contains(text, "deploy-secret") || strings.Contains(text, "X-Amz") {
			t.Fatalf("working %q holds a credential or a URL", text)
		}
	}
}

// After a publish, no read runs within workingFloor, so a later change publishes nothing in it.
func TestWorkingPublishesAtMostOncePerFloor(t *testing.T) {
	f := newFixture(t, resultStream)
	hold := make(chan struct{})
	f.plugin.followHold = hold
	f.plugin.readWorking = func(context.Context) ([]byte, error) {
		f.plugin.mu.Lock()
		defer f.plugin.mu.Unlock()
		if f.plugin.workingReads == 1 {
			return []byte("step one"), nil
		}
		return []byte("step two"), nil
	}
	done := f.executing(t, f.ctx, f.submit(t, f.host, nil), 1)
	f.awaitKind(t, "working")
	time.Sleep(10 * renewInterval)
	close(hold)
	if status := <-done; status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	f.plugin.mu.Lock()
	reads := f.plugin.workingReads
	f.plugin.mu.Unlock()
	if texts := workingTexts(f.feedStatuses(t)); !slices.Equal(texts, []string{"step one"}) || reads != 1 {
		t.Fatalf("working %q after %d reads, want step one alone from one read", texts, reads)
	}
}

// A read that hangs runs off the run's loop: a held cancel ends the run at its next check, and the run's end cancels
// the read.
func TestWorkingHungReadDoesNotDelayACancel(t *testing.T) {
	f := workingFixture(t)
	setDuration(t, &workingTimeout, time.Minute)
	f.plugin.followHold = make(chan struct{})
	reading := make(chan context.Context, 1)
	f.plugin.readWorking = func(ctx context.Context) ([]byte, error) {
		reading <- ctx
		<-ctx.Done()
		return nil, ctx.Err()
	}
	done := f.executing(t, f.ctx, f.submit(t, f.host, nil), 1)
	read := <-reading
	start := time.Now()
	f.publishCancel(t, testRun("run-a"), 1, f.host, nil)
	select {
	case status := <-done:
		if status != kgo.AckAccept {
			t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the run did not end on its cancel")
	}
	if took := time.Since(start); took > stateCheckInterval+time.Second {
		t.Fatalf("the cancel took %s with a read hung, want within its %s check", took, stateCheckInterval)
	}
	if state, _ := terminalReason(t, f, testRun("run-a")); state != wire.TaskStateCanceled {
		t.Fatalf("terminal %s, want canceled", state)
	}
	select {
	case <-read.Done():
	case <-time.After(time.Second):
		t.Fatal("the run's end left its read running")
	}
}

// A read that returns after the run's terminal status publishes nothing.
func TestWorkingNothingAfterTheTerminal(t *testing.T) {
	f := workingFixture(t)
	hold, late := make(chan struct{}), make(chan struct{})
	f.plugin.followHold = hold
	var once sync.Once
	f.plugin.readWorking = func(context.Context) ([]byte, error) {
		f.plugin.mu.Lock()
		reads := f.plugin.workingReads
		f.plugin.mu.Unlock()
		if reads == 1 {
			return []byte("step one"), nil
		}
		once.Do(func() { close(hold) })
		<-late
		return []byte("late step"), nil
	}
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	close(late)
	time.Sleep(5 * renewInterval)
	if kinds := f.kinds(t); !slices.Equal(kinds, []string{"claimed", "started", "working", "terminal"}) {
		t.Fatalf("statuses %v, want nothing after the terminal", kinds)
	}
	if texts := workingTexts(f.feedStatuses(t)); !slices.Equal(texts, []string{"step one"}) {
		t.Fatalf("working %q, want step one alone", texts)
	}
}

// A failed read is logged once per class, redacted, and publishes nothing.
func TestWorkingReadFailuresAreLoggedOncePerClass(t *testing.T) {
	f := workingFixture(t)
	hold := make(chan struct{})
	f.plugin.followHold = hold
	f.plugin.readWorking = func(context.Context) ([]byte, error) {
		f.plugin.mu.Lock()
		reads := f.plugin.workingReads
		f.plugin.mu.Unlock()
		if reads == 6 {
			close(hold)
		}
		if reads%2 == 0 {
			return nil, pluginsdk.ErrLinkLost
		}
		return nil, &pluginsdk.Error{Op: "read alloc file", Class: "denied", Err: errors.New("403 for " + testToken)}
	}
	if status := f.execute(t, f.submit(t, f.host, nil), 1); status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	logs := f.logs.String()
	denied := "read working in profile nomad failed (op=read alloc file class=denied: 403 for " + redacted + ")"
	lost := "read working in profile nomad failed (" + pluginsdk.ErrLinkLost.Error() + ")"
	if strings.Count(logs, denied) != 1 || strings.Count(logs, lost) != 1 || strings.Contains(logs, testToken) || slices.Contains(f.kinds(t), "working") {
		t.Fatalf("logs:\n%s\nwant each failure class once, redacted, and no working status", logs)
	}
}

// An adopter publishes the run's last working line again after its started status, with its time, and holds the
// floor from it: it reads nothing within workingFloor.
func TestWorkingAnAdopterRepublishesTheLastLine(t *testing.T) {
	f, record := adopting(t)
	hold := make(chan struct{})
	f.plugin.followHold = hold
	f.plugin.working = []byte("a newer step")
	earlier := taskStatus(wire.TaskStateWorking, "step from attempt 1")
	earlier.Timestamp = "2026-11-11T10:00:00.000Z"
	f.publishStatus(t, record, testReceiver, f.receiver.signing, "claimed", wire.TaskStateWorking, nil)
	f.publishStatus(t, record, testReceiver, f.receiver.signing, "started", wire.TaskStateWorking, nil)
	f.publishStatus(t, record, testReceiver, f.receiver.signing, "working", wire.TaskStateWorking, func(status *wire.RunStatus) {
		status.StatusUpdate.Status = earlier
	})
	done := f.executing(t, f.ctx, record, 2)
	time.Sleep(5 * renewInterval)
	close(hold)
	if status := <-done; status != kgo.AckAccept {
		t.Fatalf("status %v; logs:\n%s", status, f.logs.String())
	}
	var kinds []string
	var republished []wire.RunStatus
	for _, status := range f.eventStatuses(t) {
		if status.Wherry.Attempt == 2 {
			kinds = append(kinds, status.Wherry.Kind)
			if status.Wherry.Kind == "working" {
				republished = append(republished, status)
			}
		}
	}
	if !slices.Equal(kinds, []string{"claimed", "adopting", "started", "working", "terminal"}) || len(republished) != 1 ||
		workingText(republished[0]) != "step from attempt 1" || republished[0].StatusUpdate.Status.Timestamp != earlier.Timestamp {
		t.Fatalf("adopter's statuses %v, working %+v; want the earlier line once after started, with its time", kinds, republished)
	}
	f.plugin.mu.Lock()
	defer f.plugin.mu.Unlock()
	if f.plugin.workingReads != 0 {
		t.Fatalf("the adopter read the working file %d times within the floor", f.plugin.workingReads)
	}
}
