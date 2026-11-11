package receiver

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/pluginsdk"
	dto "github.com/prometheus/client_model/go"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func claimFixture(t *testing.T) (fixture, *runClaim) {
	t.Helper()
	f := newFixture(t, resultStream)
	record := f.submit(t, f.host, nil)
	ctx, cancel := context.WithCancelCause(f.ctx)
	t.Cleanup(func() { cancel(nil) })
	claim := &runClaim{ctx: ctx, cancel: cancel, gate: make(chan struct{}, 1), duration: 15 * time.Second}
	share, err := f.receiver.newClient(kgo.ConsumeTopics(record.Topic), kgo.ShareGroup(operator.PoolGroup(testPool)), kgo.ShareMaxRecords(1), kgo.ShareMaxRecordsStrict(), kgo.ShareAckCallback(func(_ *kgo.Client, results kgo.ShareAckResults) {
		for _, result := range results {
			claim.answer(result.Err)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(share.Close)
	records := share.PollRecords(f.ctx, 1)
	if records.Err() != nil || len(records.Records()) != 1 {
		t.Fatalf("poll: %v", records.Err())
	}
	claim.share, claim.record, claim.anchor = share, records.Records()[0], time.Now()
	return f, claim
}

func TestClaimCheckAnswers(t *testing.T) {
	t.Run("successive", func(t *testing.T) {
		_, claim := claimFixture(t)
		for i := 0; i < 3; i++ {
			before := time.Now()
			if !claim.renew(claim.ctx, true) {
				t.Fatal("successful renewal refused")
			}
			claim.mu.Lock()
			sequence, answered, anchor := claim.sequence, claim.answered, claim.anchor
			claim.mu.Unlock()
			if sequence != uint64(i+1) || answered != sequence || anchor.Before(before) {
				t.Fatalf("sequence=%d answered=%d anchor=%v", sequence, answered, anchor)
			}
		}
	})
	t.Run("refused", func(t *testing.T) {
		f, claim := claimFixture(t)
		f.refuseAck(t, kgo.AckRenew)
		if claim.renew(claim.ctx, true) || !errors.Is(context.Cause(claim.ctx), errClaimLost) {
			t.Fatal("refused renewal held")
		}
		claim.answer(nil)
		if claim.renew(claim.ctx, true) {
			t.Fatal("late success restored claim")
		}
	})
	t.Run("no answer", func(t *testing.T) {
		_, claim := claimFixture(t)
		if !claim.renew(claim.ctx, true) {
			t.Fatal("initial renewal refused")
		}
		// A record with no share acknowledgement state makes Ack a no-op.
		claim.record = &kgo.Record{}
		if claim.renew(claim.ctx, true) || !errors.Is(context.Cause(claim.ctx), errClaimLost) {
			t.Fatal("missing answer held")
		}
	})
	t.Run("parent cancelled", func(t *testing.T) {
		_, claim := claimFixture(t)
		ctx, cancel := context.WithCancel(claim.ctx)
		cancel()
		if claim.renew(ctx, true) || claim.ctx.Err() != nil {
			t.Fatal("parent cancellation lost claim")
		}
	})
	t.Run("authorisation", func(t *testing.T) {
		_, claim := claimFixture(t)
		claim.answer(kerr.GroupAuthorizationFailed)
		if claim.ctx.Err() != nil {
			t.Fatal("authorisation counted as loss")
		}
	})
}

func TestClaimCheckWithheldAnswer(t *testing.T) {
	for _, mode := range []string{"check", "gate", "background", "stop", "parent", "anchor"} {
		t.Run(mode, func(t *testing.T) {
			f, claim := claimFixture(t)
			entered, release := make(chan struct{}), make(chan struct{})
			var once, arrived sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			f.cluster.ControlKey(79, func(request kmsg.Request) (kmsg.Response, error, bool) {
				arrived.Do(func() { close(entered) })
				f.cluster.SleepControl(func() { <-release })
				return nil, nil, false
			})
			if mode == "anchor" {
				done := make(chan bool, 1)
				go func() { done <- claim.renew(claim.ctx, true) }()
				<-entered
				time.Sleep(50 * time.Millisecond)
				claim.mu.Lock()
				sent := claim.sent
				claim.mu.Unlock()
				unblock()
				if !<-done {
					t.Fatal("delayed success refused")
				}
				claim.mu.Lock()
				anchor := claim.anchor
				claim.mu.Unlock()
				if !anchor.Equal(sent) {
					t.Fatalf("anchor=%v sent=%v", anchor, sent)
				}
				if !claim.renew(claim.ctx, false) {
					t.Fatal("background did not use updated anchor")
				}
				return
			}
			ctx, cancel := context.WithCancel(claim.ctx)
			defer cancel()
			background := mode != "check"
			if mode == "background" {
				claim.duration = 150 * time.Millisecond
			}
			done := make(chan bool, 1)
			go func() { done <- claim.renew(ctx, !background) }()
			select {
			case <-entered:
			case <-f.ctx.Done():
				t.Fatal("no renewal")
			}
			start := time.Now()
			switch mode {
			case "gate":
				run := &activeRun{claim: claim, receiver: f.receiver, task: f.parse(t, claim.record), profile: f.receiver.profiles[0]}
				run.stopAndFail(claim.ctx, pluginsdk.Handle{}, "failed", nil)
				f.plugin.mu.Lock()
				stops := f.plugin.stops
				f.plugin.mu.Unlock()
				if stops != 0 {
					t.Fatal("Stop called while waiting for renewal")
				}
				if time.Since(start) > 2500*time.Millisecond {
					t.Fatal("gate exceeded check bound")
				}
			case "stop", "parent":
				cancel()
			}
			select {
			case held := <-done:
				if held {
					t.Fatal("withheld answer held")
				}
			case <-time.After(2500 * time.Millisecond):
				t.Fatal("renewal exceeded bound")
			}
			lost := errors.Is(context.Cause(claim.ctx), errClaimLost)
			if lost != (mode != "stop" && mode != "parent") {
				t.Fatalf("cause: %v", context.Cause(claim.ctx))
			}
		})
	}
}

func TestClaimCheckBeforeActions(t *testing.T) {
	for _, mode := range []string{"refused", "withheld"} {
		t.Run(mode, func(t *testing.T) {
			for _, action := range []string{"stop fail", "stop cancel", "conclude", "purge", "purge retry", "find stop", "find purge", "release"} {
				t.Run(action, func(t *testing.T) {
					f, claim := claimFixture(t)
					run := &activeRun{claim: claim, receiver: f.receiver, task: f.admitted(t, claim.record).task, profile: f.receiver.profiles[0]}
					if action == "stop fail" || action == "stop cancel" || action == "conclude" {
						f.plugin.stopErr = errors.New("stop failed")
					}
					if action != "purge retry" && action != "find purge" {
						failRenewal(t, f, 1, mode)
					}
					switch action {
					case "stop fail":
						run.stopAndFail(claim.ctx, pluginsdk.Handle{}, "failed", nil)
					case "stop cancel":
						run.stopAndCancel(claim.ctx, pluginsdk.Handle{}, nil)
					case "conclude":
						run.conclude(claim.ctx, pluginsdk.Handle{}, nil, pluginsdk.Terminal{})
					case "purge":
						_ = run.purge(claim.ctx)
					case "purge retry":
						previous := purgeDeadline
						purgeDeadline = 500 * time.Millisecond
						t.Cleanup(func() { purgeDeadline = previous })
						f.plugin.purgeFailures = 1
						// The first call is allowed; its failure must be followed by another check.
						f.plugin.mu.Lock()
						f.plugin.purgeErr = errors.New("purge")
						f.plugin.mu.Unlock()
						go func() {
							for {
								f.plugin.mu.Lock()
								n := f.plugin.purges
								f.plugin.mu.Unlock()
								if n > 0 {
									break
								}
								time.Sleep(time.Millisecond)
							}
							failRenewal(t, f, 1, mode)
						}()
						_ = run.purge(claim.ctx)
					case "find stop", "find purge":
						f.plugin.created = map[string]bool{run.task.Wherry.Run: true}
						f.plugin.jobAgent = "another-agent"
						if action == "find purge" {
							f.plugin.stopCheck = func() error { failRenewal(t, f, 1, mode); return nil }
						}
						_, _, _ = run.findJob(claim.ctx, claim.ctx)
					case "release":
						run.release(claim.ctx)
					}
					if !errors.Is(context.Cause(claim.ctx), errClaimLost) {
						t.Fatalf("cause %v", context.Cause(claim.ctx))
					}
					f.plugin.mu.Lock()
					stops, purges, releases := f.plugin.stops, f.plugin.purges, f.plugin.releases
					f.plugin.mu.Unlock()
					wantStops, wantPurges := 0, 0
					if action == "find purge" {
						wantStops = 1
					}
					if action == "purge retry" {
						wantPurges = 1
					}
					if stops != wantStops || purges != wantPurges || releases != 0 {
						t.Fatalf("stops=%d purges=%d releases=%d", stops, purges, releases)
					}
					if run.abandon(context.Canceled) != 0 || strings.Contains(f.logs.String(), "write failed") {
						t.Fatal("loss reported as write failure")
					}
				})
			}

		})
	}
}

func TestClaimCheckDuringExecution(t *testing.T) {
	for _, mode := range []string{"refused", "withheld"} {
		t.Run(mode, func(t *testing.T) {
			for _, site := range []string{"dispatch", "oversized", "following", "placing", "start purge", "release"} {
				t.Run(site, func(t *testing.T) {
					f, claim := claimFixture(t)
					admitted := f.admitted(t, claim.record)
					switch site {
					case "dispatch":
						failRenewal(t, f, 1, mode)
					case "oversized":
						f.plugin.handleSize = maxHandle + 1
						// Dispatch is the first check; refuse the next acknowledgement.
						failRenewal(t, f, 2, mode)
					case "following", "placing", "start purge", "release":
						if site == "placing" {
							f.plugin.placing = true
						}
						if site == "start purge" {
							f.plugin.dispatchErr = errors.New("start failed")
						}
						if site == "release" {
							f.plugin.collecting = func() { failRenewal(t, f, 1, mode) }
						} else {
							failRenewal(t, f, 2, mode)
						}
					}
					ctx := context.WithValue(claim.ctx, claimContextKey{}, claim)
					ack := f.receiver.execute(ctx, claim.share, claim.record, admitted, 1)
					if ack != 0 || !errors.Is(context.Cause(ctx), errClaimLost) {
						t.Fatalf("ack=%v cause=%v; logs:\n%s", ack, context.Cause(ctx), f.logs.String())
					}
					f.plugin.mu.Lock()
					dispatches, stops, purges, releases := f.plugin.dispatches, f.plugin.stops, f.plugin.purges, f.plugin.releases
					f.plugin.mu.Unlock()
					if site == "dispatch" && dispatches != 0 || site != "release" && stops != 0 || purges != 0 || releases != 0 {
						t.Fatalf("dispatches=%d stops=%d purges=%d releases=%d", dispatches, stops, purges, releases)
					}
					if got := strings.Count(f.logs.String(), ": claim lost:"); got != 1 {
						t.Fatalf("claim lost logs=%d: %s", got, f.logs.String())
					}
					if got := claimLostCount(t, f); got != 1 {
						t.Fatalf("claim lost count=%v", got)
					}
					for _, result := range f.eventStatuses(t) {
						if result.Wherry.Kind == "released" {
							t.Fatal("released after loss")
						}
					}
				})
			}

		})
	}
}

func failRenewal(t *testing.T, f fixture, nth int, mode string) {
	t.Helper()
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	n := 0
	f.cluster.ControlKey(79, func(request kmsg.Request) (kmsg.Response, error, bool) {
		f.cluster.KeepControl()
		ack := request.(*kmsg.ShareAcknowledgeRequest)
		renewal := false
		for _, topic := range ack.Topics {
			for _, partition := range topic.Partitions {
				for _, batch := range partition.AcknowledgementBatches {
					for _, kind := range batch.AcknowledgeTypes {
						renewal = renewal || kind == int8(kgo.AckRenew)
					}
				}
			}
		}
		if !renewal {
			return nil, nil, false
		}
		n++
		if n != nth {
			return nil, nil, false
		}
		if mode == "withheld" {
			f.cluster.SleepControl(func() { <-release })
			return nil, nil, false
		}
		response := kmsg.NewPtrShareAcknowledgeResponse()
		response.SetVersion(ack.GetVersion())
		for _, topic := range ack.Topics {
			result := kmsg.ShareAcknowledgeResponseTopic{TopicID: topic.TopicID}
			for _, partition := range topic.Partitions {
				result.Partitions = append(result.Partitions, kmsg.ShareAcknowledgeResponseTopicPartition{Partition: partition.Partition, ErrorCode: renewalError(mode)})
			}
			response.Topics = append(response.Topics, result)
		}
		return response, nil, true
	})
}

func claimLostCount(t *testing.T, f fixture) float64 {
	t.Helper()
	var sample dto.Metric
	if err := f.receiver.metrics.claimLost.Write(&sample); err != nil {
		t.Fatal(err)
	}
	return sample.GetCounter().GetValue()
}

func TestClaimLossHaltsTranscript(t *testing.T) {
	f, claim := claimFixture(t)
	admitted := f.admitted(t, claim.record)
	f.plugin.endDelay = time.Hour
	// Avoid a progress tick flushing the pending transcript before the loss.
	renewInterval = time.Hour
	ctx := context.WithValue(claim.ctx, claimContextKey{}, claim)
	done := make(chan kgo.AckStatus, 1)
	go func() { done <- f.receiver.execute(ctx, claim.share, claim.record, admitted, 1) }()
	waitFor(t, f, func() bool {
		f.plugin.mu.Lock()
		defer f.plugin.mu.Unlock()
		return f.plugin.delivered == len(resultStream)
	})
	claim.answer(kerr.InvalidRecordState)
	select {
	case ack := <-done:
		if ack != 0 {
			t.Fatalf("ack=%v", ack)
		}
	case <-f.ctx.Done():
		t.Fatal("lost run did not halt")
	}
	if n := len(f.chunks(t)); n != 0 {
		t.Fatalf("shutdown wrote %d transcript chunks", n)
	}
	for _, status := range f.eventStatuses(t) {
		if status.Wherry.Kind == "released" {
			t.Fatal("lost claim released")
		}
	}
	if claimLostCount(t, f) != 1 || strings.Count(f.logs.String(), ": claim lost:") != 1 {
		t.Fatalf("loss not counted once: %s", f.logs.String())
	}
}

func renewalError(mode string) int16 {
	if mode == "authorisation" {
		return kerr.GroupAuthorizationFailed.Code
	}
	return kerr.InvalidRecordState.Code
}

func TestClaimCheckAuthorisationDenies(t *testing.T) {
	f := newFixture(t, resultStream)
	f.submit(t, f.host, nil)
	failRenewal(t, f, 1, "authorisation")
	done := f.run(t, f.ctx)
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "denied") {
			t.Fatalf("receiver exit: %v; logs: %s", err, f.logs.String())
		}
	case <-f.ctx.Done():
		t.Fatal("authorisation did not halt receiver")
	}
	if claimLostCount(t, f) != 0 || strings.Contains(f.logs.String(), ": claim lost:") {
		t.Fatal("authorisation counted as claim loss")
	}
	f.plugin.mu.Lock()
	defer f.plugin.mu.Unlock()
	if f.plugin.dispatches != 0 {
		t.Fatal("dispatched after denied renewal")
	}
}

func TestClaimRenewerStop(t *testing.T) {
	f, claim := claimFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	var arrived sync.Once
	f.cluster.ControlKey(79, func(kmsg.Request) (kmsg.Response, error, bool) {
		arrived.Do(func() { close(entered) })
		f.cluster.SleepControl(func() { <-release })
		return nil, nil, false
	})
	stop := renewer(claim.ctx, claim)
	select {
	case <-entered:
	case <-f.ctx.Done():
		t.Fatal("renewer did not send")
	}
	stop()
	if claim.ctx.Err() != nil {
		t.Fatalf("stopping renewer lost claim: %v", context.Cause(claim.ctx))
	}
}
