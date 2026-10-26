package client

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/credential"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// request is a seal request as the receiver's awaitSeal builds it: the attempt's binding, a fresh P-256 key and its
// thumbprint, the credentials and an expiry.
type request struct {
	record wire.SealRequest
	key    *ecdsa.PrivateKey
}

func newRequest(t *testing.T, binding wire.ReceiverBinding, credentials ...string) request {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return request{wire.SealRequest{ReceiverBinding: binding, Key: jose.JSONWebKey{Key: &key.PublicKey}, KeyID: signed.SealKeyID(&key.PublicKey),
		Credentials: credentials, ExpiryMS: time.Now().Add(time.Minute).UnixMilli()}, key}
}

// open opens the seal answering r from host's topic, failing the test unless there is exactly one.
func (r request) open(t *testing.T, f *fixture, host string) map[string][]byte {
	t.Helper()
	return r.openSealed(t, f, host).Values
}

func (r request) openSealed(t *testing.T, f *fixture, host string) signed.SealValues {
	t.Helper()
	records, err := readTopic(f.ctx, f.service.Admin, admin.HostTopic("tenant-a", host))
	if err != nil {
		t.Fatal(err)
	}
	binding := signed.SealBinding{Tenant: r.record.Tenant, Pool: r.record.Pool, Receiver: r.record.Receiver, KeyID: r.record.KeyID, Run: r.record.Run,
		Attempt: r.record.Attempt, Credentials: r.record.Credentials, TaskCredentials: r.record.TaskCredentials,
		S3Inputs: r.record.S3Inputs, S3Outputs: r.record.S3Outputs, ExpiryMS: r.record.ExpiryMS}
	var opened []signed.SealValues
	for _, record := range records {
		if values, err := signed.OpenSeal(record.Value, binding, r.key, time.Now()); err == nil && string(record.Key) == r.record.Run {
			opened = append(opened, values)
		}
	}
	if len(opened) != 1 {
		t.Fatalf("%d seals answer the request, want 1", len(opened))
	}
	return opened[0]
}

// seals counts the records on host's topic other than its key registration.
func (f *fixture) seals(t *testing.T, host string) int {
	t.Helper()
	records, err := readTopic(f.ctx, f.service.Admin, admin.HostTopic("tenant-a", host))
	if err != nil {
		t.Fatal(err)
	}
	return len(records) - 1
}

// answering runs host's Answer until the test ends.
func answering(t *testing.T, f *fixture, host *Host) {
	t.Helper()
	ctx, stop := context.WithCancel(f.ctx)
	done := make(chan struct{})
	t.Cleanup(func() { stop(); <-done })
	go func() {
		defer close(done)
		if err := host.Answer(ctx); err != nil {
			t.Errorf("answer: %v", err)
		}
	}()
}

// waitSeals waits until host's topic holds n seals.
func (f *fixture) waitSeals(t *testing.T, host string, n int) {
	t.Helper()
	for f.seals(t, host) < n {
		if f.ctx.Err() != nil {
			t.Fatalf("host topic never held %d seals; logs:\n%s", n, f.logs.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestAwaitAnswersTheSealRequest(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	asked := newRequest(t, receiver.binding(task, 1), "model")
	receiver.feed(t, f.ctx, runID, receiver.sign(t, &asked.record))
	done := make(chan AwaitResult, 1)
	go func() {
		awaited, err := f.host.Await(f.ctx, runID, 20*time.Second)
		if err != nil {
			t.Errorf("await: %v", err)
		}
		done <- awaited
	}()
	f.waitSeals(t, "host-a", 1)
	if values := asked.open(t, f, "host-a"); string(values["model"]) != "model-token" || len(values) != 1 {
		t.Fatalf("sealed %q, want the model credential alone", values)
	}
	receiver.feed(t, f.ctx, runID, receiver.result(t, task, 1, wire.TaskStateCompleted, nil), receiver.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateCompleted, nil))
	if awaited := <-done; awaited.Status.State != wire.TaskStateCompleted || awaited.Skipped != 0 {
		t.Fatalf("await %#v; want completed with the seal request unskipped", awaited)
	}
	if logs := f.logs.String(); !strings.Contains(logs, "run "+runID+": sealed [model] to receiver-a for attempt 1") || strings.Contains(logs, "model-token") {
		t.Fatalf("logs:\n%s", logs)
	}
}

// anotherRun submits a second run and admits it through receiver.
func (f *fixture) anotherRun(t *testing.T, receiver *testReceiver) claimed {
	t.Helper()
	submitted := f.submit(t, defaultSelector())
	task := receiver.claim(t, f.ctx, submitted.RunID)[0]
	receiver.feed(t, f.ctx, submitted.RunID, receiver.echo(t, task), receiver.status(t, task, 1, "claimed-1", "claimed", wire.TaskStateWorking, nil))
	return task
}

// A second request for an attempt already answered, under a fresh key, gets no second seal.
func TestEachAttemptIsAnsweredOnce(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	answering(t, f, f.host)
	asked := newRequest(t, receiver.binding(task, 1), "model")
	receiver.feed(t, f.ctx, runID, receiver.sign(t, &asked.record))
	f.waitSeals(t, "host-a", 1)
	again := newRequest(t, receiver.binding(task, 1), "model")
	receiver.feed(t, f.ctx, runID, receiver.sign(t, &again.record))
	// A request for another run's attempt, after the second, shows the second has been read.
	next := f.anotherRun(t, receiver)
	control := newRequest(t, receiver.binding(next, 1), "model")
	receiver.feed(t, f.ctx, next.submission.Wherry.Run, receiver.sign(t, &control.record))
	f.waitSeals(t, "host-a", 2)
	time.Sleep(200 * time.Millisecond)
	asked.open(t, f, "host-a")
	control.open(t, f, "host-a")
	if seals := f.seals(t, "host-a"); seals != 2 {
		t.Fatalf("host topic holds %d seals; the attempt was answered twice", seals)
	}
}

// A seal written but not recorded, as when the host stops between the two, is written again on the next pass.
func TestAnUnrecordedAnswerIsAnsweredAgain(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	previous := produceSeal
	t.Cleanup(func() { produceSeal = previous })
	failed := make(chan struct{})
	var once sync.Once
	produceSeal = func(context.Context, *Host, *kgo.Record) error {
		once.Do(func() { close(failed) })
		return errors.New("host stopped")
	}
	asked := newRequest(t, receiver.binding(task, 1), "model")
	receiver.feed(t, f.ctx, runID, receiver.sign(t, &asked.record))
	ctx, stop := context.WithCancel(f.ctx)
	go f.host.Answer(ctx)
	<-failed
	stop()
	produceSeal = previous
	answering(t, f, f.host)
	f.waitSeals(t, "host-a", 1)
	asked.open(t, f, "host-a")
}

func TestARequestExpiringBeforeTheWriteIsNotAnswered(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	previous := seal
	t.Cleanup(func() { seal = previous })
	asked := newRequest(t, receiver.binding(task, 1), "model")
	asked.record.ExpiryMS = time.Now().Add(2 * time.Second).UnixMilli()
	built := make(chan struct{})
	seal = func(binding signed.SealBinding, key *ecdsa.PublicKey, values signed.SealValues) ([]byte, error) {
		time.Sleep(time.Until(time.UnixMilli(binding.ExpiryMS)))
		defer close(built)
		return previous(binding, key, values)
	}
	receiver.feed(t, f.ctx, runID, receiver.sign(t, &asked.record))
	answering(t, f, f.host)
	<-built
	time.Sleep(500 * time.Millisecond)
	if seals := f.seals(t, "host-a"); seals != 0 {
		t.Fatalf("wrote %d seals for a request that expired while its seal was built", seals)
	}
}

func TestSweepRemovesSpentMarkers(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	ended := f.submit(t, defaultSelector())
	endedTask := receiver.claim(t, f.ctx, ended.RunID)[0]
	receiver.feed(t, f.ctx, ended.RunID, receiver.echo(t, endedTask), receiver.result(t, endedTask, 1, wire.TaskStateCompleted, nil),
		receiver.status(t, endedTask, 1, "terminal-1", "terminal", wire.TaskStateCompleted, nil))
	live := time.Now().Add(time.Minute).UnixMilli()
	markers := map[string]int64{
		f.host.markerPath(runID, 1):       live,
		f.host.markerPath(runID, 2):       time.Now().Add(-time.Second).UnixMilli(),
		f.host.markerPath(ended.RunID, 1): live,
	}
	for path, expiry := range markers {
		if err := writeState(path, answerMarker{ExpiryMS: expiry}); err != nil {
			t.Fatal(err)
		}
	}
	f.host.sweep(f.ctx)
	for path, kept := range map[string]bool{f.host.markerPath(runID, 1): true, f.host.markerPath(runID, 2): false, f.host.markerPath(ended.RunID, 1): false} {
		if _, err := os.Stat(path); (err == nil) != kept {
			t.Fatalf("marker %s kept %t, want %t", filepath.Base(path), err == nil, kept)
		}
	}
}

// A live pool member can sign a claimed status it never received from the broker; claims by two receivers for one
// turn, or a request from a receiver other than the turn's first claimant, are its trace.
func TestAnswerRefusesForgedClaims(t *testing.T) {
	for name, forge := range map[string]func(*testing.T, *fixture, *testReceiver, *testReceiver, claimed) request{
		"a second receiver's claim": func(t *testing.T, f *fixture, receiver, forger *testReceiver, task claimed) request {
			runID := task.submission.Wherry.Run
			// The first claimant adopted attempt 2; the forger's claim for attempt 1 follows it on the feed.
			receiver.feed(t, f.ctx, runID, receiver.status(t, task, 2, "claimed-2", "claimed", wire.TaskStateWorking, nil))
			forger.feed(t, f.ctx, runID, forger.status(t, task, 1, "claimed-forged", "claimed", wire.TaskStateWorking, nil))
			asked := newRequest(t, receiver.binding(task, 2), "model")
			receiver.feed(t, f.ctx, runID, receiver.sign(t, &asked.record))
			return asked
		},
		"a request from another receiver than the first claimant": func(t *testing.T, f *fixture, _, forger *testReceiver, task claimed) request {
			runID := task.submission.Wherry.Run
			forger.feed(t, f.ctx, runID, forger.status(t, task, 2, "started-forged", "started", wire.TaskStateWorking, nil))
			asked := newRequest(t, forger.binding(task, 2), "model")
			forger.feed(t, f.ctx, runID, forger.sign(t, &asked.record))
			return asked
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			receiver, task := f.admitted(t)
			forger := f.addReceiver(t, "receiver-b", "pool-a")
			forge(t, f, receiver, forger, task)
			answering(t, f, f.host)
			for !strings.Contains(f.logs.String(), "refused to seal") {
				if f.ctx.Err() != nil {
					t.Fatalf("no refusal; logs:\n%s", f.logs.String())
				}
				time.Sleep(50 * time.Millisecond)
			}
			if seals := f.seals(t, "host-a"); seals != 0 {
				t.Fatalf("sealed %d times to a turn with forged claims", seals)
			}
		})
	}
}

// Each case writes a request the host must skip, then a control request for another run's current attempt; the
// tenant feed has one partition, so once the control's seal is written the skipped request has been read, and it
// must have no seal.
func TestAnswerSkipsRequestsItCannotTrust(t *testing.T) {
	for name, skipped := range map[string]func(*testing.T, *fixture, *testReceiver, claimed) (request, []byte){
		"bad signature": func(t *testing.T, _ *fixture, receiver *testReceiver, task claimed) (request, []byte) {
			// The receiver's own header and payload, signed by another key: the kid names the receiver's key.
			asked := newRequest(t, receiver.binding(task, 1), "model")
			header, payload, _ := strings.Cut(string(receiver.sign(t, &asked.record)), ".")
			payload, _, _ = strings.Cut(payload, ".")
			_, impostor, _ := ed25519.GenerateKey(rand.Reader)
			signature := ed25519.Sign(impostor, []byte(header+"."+payload))
			return asked, []byte(header + "." + payload + "." + base64.RawURLEncoding.EncodeToString(signature))
		},
		"non-member": func(t *testing.T, f *fixture, _ *testReceiver, task claimed) (request, []byte) {
			f.pool(t, "pool-b")
			outsider := f.addReceiver(t, "receiver-b", "pool-b")
			asked := newRequest(t, outsider.binding(task, 1), "model")
			return asked, outsider.sign(t, &asked.record)
		},
		"another member of the pool": func(t *testing.T, f *fixture, _ *testReceiver, task claimed) (request, []byte) {
			peer := f.addReceiver(t, "receiver-b", "pool-a")
			asked := newRequest(t, peer.binding(task, 1), "model")
			return asked, peer.sign(t, &asked.record)
		},
		"stale attempt": func(t *testing.T, f *fixture, receiver *testReceiver, task claimed) (request, []byte) {
			receiver.feed(t, f.ctx, task.submission.Wherry.Run, receiver.status(t, task, 2, "claimed-2", "claimed", wire.TaskStateWorking, nil))
			asked := newRequest(t, receiver.binding(task, 1), "model")
			return asked, receiver.sign(t, &asked.record)
		},
		"credential outside the agent's terms": func(t *testing.T, _ *fixture, receiver *testReceiver, task claimed) (request, []byte) {
			asked := newRequest(t, receiver.binding(task, 1), "openai")
			return asked, receiver.sign(t, &asked.record)
		},
		"ended attempt": func(t *testing.T, f *fixture, receiver *testReceiver, task claimed) (request, []byte) {
			runID := task.submission.Wherry.Run
			asked := newRequest(t, receiver.binding(task, 1), "model")
			receiver.feed(t, f.ctx, runID, receiver.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateFailed, nil))
			return asked, receiver.sign(t, &asked.record)
		},
		"expiry beyond the seal wait": func(t *testing.T, _ *fixture, receiver *testReceiver, task claimed) (request, []byte) {
			asked := newRequest(t, receiver.binding(task, 1), "model")
			asked.record.ExpiryMS = time.Now().Add(10 * time.Minute).UnixMilli()
			return asked, receiver.sign(t, &asked.record)
		},
		"expired": func(t *testing.T, _ *fixture, receiver *testReceiver, task claimed) (request, []byte) {
			asked := newRequest(t, receiver.binding(task, 1), "model")
			asked.record.ExpiryMS = time.Now().Add(-time.Second).UnixMilli()
			return asked, receiver.sign(t, &asked.record)
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			receiver, task := f.admitted(t)
			runID := task.submission.Wherry.Run
			_, encoded := skipped(t, f, receiver, task)
			receiver.feed(t, f.ctx, runID, encoded)
			next := f.anotherRun(t, receiver)
			control := newRequest(t, receiver.binding(next, 1), "model")
			receiver.feed(t, f.ctx, next.submission.Wherry.Run, receiver.sign(t, &control.record))
			answering(t, f, f.host)
			f.waitSeals(t, "host-a", 1)
			time.Sleep(200 * time.Millisecond)
			control.open(t, f, "host-a")
			if seals := f.seals(t, "host-a"); seals != 1 {
				t.Fatalf("host topic holds %d seals; the %s request was answered", seals, name)
			}
			if strings.Contains(f.logs.String(), "model-token") {
				t.Fatalf("logs carry the credential:\n%s", f.logs.String())
			}
		})
	}
}

// logged waits until the fixture's logs hold want and then for settle, and returns the lines holding want, failing the
// test unless each starts with the answerer prefix of host's logger.
func (f *fixture) logged(t *testing.T, host, want string, settle time.Duration) []string {
	t.Helper()
	for !strings.Contains(f.logs.String(), want) {
		if f.ctx.Err() != nil {
			t.Fatalf("no line holds %q; logs:\n%s", want, f.logs.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(settle)
	prefix := fmt.Sprintf("%s: answerer pid=%d version=", host, os.Getpid())
	var lines []string
	for line := range strings.Lines(f.logs.String()) {
		if strings.Contains(line, want) {
			if !strings.HasPrefix(line, prefix) || strings.HasPrefix(line, prefix+":") {
				t.Fatalf("line %q lacks the answerer prefix %q and a version", line, prefix)
			}
			lines = append(lines, line)
		}
	}
	return lines
}

func TestAnswerLogsARequestForARunWithoutALocalRecord(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	other := f.newHost(t, "host-b", t.TempDir())
	answering(t, f, other)
	f.logged(t, "host-b", "following the tenant feed for seal requests", 0)
	asked := newRequest(t, receiver.binding(task, 1), "model")
	receiver.feed(t, f.ctx, runID, receiver.sign(t, &asked.record))
	if lines := f.logged(t, "host-b", fmt.Sprintf("run %q: skipped a seal request: this host has no local record of the run", runID), 1500*time.Millisecond); len(lines) != 1 {
		t.Fatalf("want one line; logs:\n%s", f.logs.String())
	}
}

func TestAnswerLogsAnUnauthorisedRequest(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	f.pool(t, "pool-b")
	outsider := f.addReceiver(t, "receiver-b", "pool-b")
	binding := outsider.binding(task, 1)
	binding.Pool = "pool-a"
	asked := newRequest(t, binding, "model")
	receiver.feed(t, f.ctx, runID, outsider.sign(t, &asked.record))
	answering(t, f, f.host)
	if lines := f.logged(t, "host-a", "run "+runID+": skipped a seal request: record is not from a live member of the pool", 1500*time.Millisecond); len(lines) != 1 {
		t.Fatalf("want one line; logs:\n%s", f.logs.String())
	}
	if seals := f.seals(t, "host-a"); seals != 0 {
		t.Fatalf("host topic holds %d seals", seals)
	}
}

// A second answerer on the state directory, reading the request after the first sealed it, logs the marker.
func TestAnswerLogsAnAttemptAnotherAnswererSealed(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	asked := newRequest(t, receiver.binding(task, 1), "model")
	receiver.feed(t, f.ctx, runID, receiver.sign(t, &asked.record))
	answering(t, f, f.host)
	f.waitSeals(t, "host-a", 1)
	twin, err := NewHost(f.host.kafka, f.host.key, admin.TenantPrincipal("tenant-a", "host-a"), f.stateDir, f.host.credentials, f.host.git, log.New(f.logs, "twin: ", 0))
	if err != nil {
		t.Fatal(err)
	}
	answering(t, f, twin)
	want := "run " + runID + ": skipped a seal request from receiver-a for attempt 1, which another answerer already sealed"
	if lines := f.logged(t, "twin", want, 1500*time.Millisecond); len(lines) != 1 {
		t.Fatalf("want one line; logs:\n%s", f.logs.String())
	}
	if seals := f.seals(t, "host-a"); seals != 1 {
		t.Fatalf("host topic holds %d seals", seals)
	}
}

// Every host of the tenant reads the feed; only the host that submitted the run answers its request.
func TestAnswerSkipsAnotherHostsRun(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	other := f.newHost(t, "host-b", t.TempDir())
	answering(t, f, other)
	asked := newRequest(t, receiver.binding(task, 1), "model")
	receiver.feed(t, f.ctx, task.submission.Wherry.Run, receiver.sign(t, &asked.record))
	answering(t, f, f.host)
	f.waitSeals(t, "host-a", 1)
	time.Sleep(500 * time.Millisecond)
	if seals := f.seals(t, "host-b"); seals != 0 {
		t.Fatalf("host-b wrote %d seals for host-a's run", seals)
	}
}

func TestAnswerLogsACredentialThisHostLacks(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	f.host.credentials = map[string]credential.Source{}
	asked := newRequest(t, receiver.binding(task, 1), "model")
	receiver.feed(t, f.ctx, runID, receiver.sign(t, &asked.record))
	answering(t, f, f.host)
	want := "run " + runID + ": skipped a seal request for credential model, which this host does not configure"
	for !strings.Contains(f.logs.String(), want) {
		if f.ctx.Err() != nil {
			t.Fatalf("logs:\n%s", f.logs.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	if seals := f.seals(t, "host-a"); seals != 0 {
		t.Fatalf("host topic holds %d seals", seals)
	}
}

// A request read after its run ended, as by a later answerer once sweep removed its attempt's marker, resolves no
// source and logs nothing.
func TestARequestForAnEndedRunIsSettledQuietly(t *testing.T) {
	f := newFixture(t)
	resolved := filepath.Join(t.TempDir(), "resolved")
	f.host.credentials["model"] = credential.Source{Command: []string{"sh", "-c", `echo >> "$0"; printf model-token`, resolved}}
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	asked := newRequest(t, receiver.binding(task, 1), "model")
	receiver.feed(t, f.ctx, runID, receiver.sign(t, &asked.record), receiver.result(t, task, 1, wire.TaskStateCompleted, nil),
		receiver.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateCompleted, nil))
	answering(t, f, f.host)
	// A request for another run, after the first, shows the first has been read.
	next := f.anotherRun(t, receiver)
	control := newRequest(t, receiver.binding(next, 1), "model")
	receiver.feed(t, f.ctx, next.submission.Wherry.Run, receiver.sign(t, &control.record))
	f.waitSeals(t, "host-a", 1)
	time.Sleep(1500 * time.Millisecond)
	control.open(t, f, "host-a")
	if ran, _ := os.ReadFile(resolved); len(ran) != 1 {
		t.Fatalf("the command ran %d times, want once, for the live run; logs:\n%s", len(ran), f.logs.String())
	}
	if logs := f.logs.String(); strings.Contains(logs, "run "+runID+":") {
		t.Fatalf("logged the ended run's request:\n%s", logs)
	}
}

// A status that ends the attempt while its seal is built stops the write.
func TestAnEndWhileSealingStopsTheWrite(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	previous := seal
	t.Cleanup(func() { seal = previous })
	built := make(chan struct{})
	seal = func(binding signed.SealBinding, key *ecdsa.PublicKey, values signed.SealValues) ([]byte, error) {
		defer close(built)
		receiver.feed(t, f.ctx, runID, receiver.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateFailed, nil))
		return previous(binding, key, values)
	}
	asked := newRequest(t, receiver.binding(task, 1), "model")
	receiver.feed(t, f.ctx, runID, receiver.sign(t, &asked.record))
	answering(t, f, f.host)
	<-built
	time.Sleep(500 * time.Millisecond)
	if seals := f.seals(t, "host-a"); seals != 0 {
		t.Fatalf("wrote %d seals for an attempt that ended while its seal was built", seals)
	}
}

// Two processes sharing the state directory answer an attempt with one seal.
func TestConcurrentAnswerersWriteOneSeal(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	twin, err := NewHost(f.host.kafka, f.host.key, admin.TenantPrincipal("tenant-a", "host-a"), f.stateDir, f.host.credentials, f.host.git, f.host.logger)
	if err != nil {
		t.Fatal(err)
	}
	previous := seal
	t.Cleanup(func() { seal = previous })
	seal = func(binding signed.SealBinding, key *ecdsa.PublicKey, values signed.SealValues) ([]byte, error) {
		time.Sleep(500 * time.Millisecond)
		return previous(binding, key, values)
	}
	asked := newRequest(t, receiver.binding(task, 1), "model")
	receiver.feed(t, f.ctx, runID, receiver.sign(t, &asked.record))
	answering(t, f, f.host)
	answering(t, f, twin)
	f.waitSeals(t, "host-a", 1)
	time.Sleep(1500 * time.Millisecond)
	if seals := f.seals(t, "host-a"); seals != 1 {
		t.Fatalf("two answerers wrote %d seals for one attempt", seals)
	}
}

// A request read while its run's local record is absent is reconsidered on a later pass.
// A request read before its attempt shows as claimed on the feed, as when the feed is slow to read, is answered once
// the claim lands; refusing it once must not drop it.
func TestARequestBeforeTheClaimIsAnswered(t *testing.T) {
	f := newFixture(t)
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	submitted := f.submit(t, defaultSelector())
	task := receiver.claim(t, f.ctx, submitted.RunID)[0]
	receiver.feed(t, f.ctx, submitted.RunID, receiver.echo(t, task))
	answering(t, f, f.host)
	asked := newRequest(t, receiver.binding(task, 1), "model")
	receiver.feed(t, f.ctx, submitted.RunID, receiver.sign(t, &asked.record))
	time.Sleep(1500 * time.Millisecond)
	if seals := f.seals(t, "host-a"); seals != 0 {
		t.Fatalf("sealed %d times before the attempt was claimed", seals)
	}
	receiver.feed(t, f.ctx, submitted.RunID, receiver.status(t, task, 1, "claimed-1", "claimed", wire.TaskStateWorking, nil))
	f.waitSeals(t, "host-a", 1)
	asked.open(t, f, "host-a")
	if logs := f.logs.String(); strings.Count(logs, "is not the run's active attempt") != 1 {
		t.Fatalf("want the refusal logged once while it waited; logs:\n%s", logs)
	}
}

func TestARequestBeforeTheLocalRecordIsAnswered(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	path, _ := f.host.runPath(runID)
	aside := path + ".aside"
	if err := os.Rename(path, aside); err != nil {
		t.Fatal(err)
	}
	answering(t, f, f.host)
	asked := newRequest(t, receiver.binding(task, 1), "model")
	receiver.feed(t, f.ctx, runID, receiver.sign(t, &asked.record))
	// Several retry passes read the request while the record is aside; each logs the same reason.
	if lines := f.logged(t, "host-a", fmt.Sprintf("run %q: skipped a seal request: this host has no local record of the run", runID), 3*retryInterval+500*time.Millisecond); len(lines) != 1 {
		t.Fatalf("want the reason logged once while it waited; logs:\n%s", f.logs.String())
	}
	if seals := f.seals(t, "host-a"); seals != 0 {
		t.Fatalf("sealed %d times without a local record", seals)
	}
	if err := os.Rename(aside, path); err != nil {
		t.Fatal(err)
	}
	f.waitSeals(t, "host-a", 1)
	asked.open(t, f, "host-a")
}

// The local record precedes the task, so a submission whose record cannot be written publishes nothing.
func TestSubmitRecordsTheRunBeforeProducing(t *testing.T) {
	f := newFixture(t)
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
	// The runs directory reads, so the run has no earlier record, but takes no new one.
	if err := os.MkdirAll(filepath.Join(f.stateDir, "runs"), 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(filepath.Join(f.stateDir, "runs"), 0o700) })
	before := f.tasksEnd(t, "pool-a")
	if _, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "work", Deadline: time.Minute, QueueDeadline: time.Minute}); err == nil {
		t.Fatal("submitted without a local record")
	}
	if f.tasksEnd(t, "pool-a") != before {
		t.Fatal("published a task whose local record was not written")
	}
}

// A task write that fails puts back the record it replaced, so the turn can be submitted again.
func TestAFailedTaskWriteLeavesNoRecord(t *testing.T) {
	f := newFixture(t)
	_, task := f.admitted(t)
	cancelled, cancel := context.WithCancel(f.ctx)
	cancel()
	fresh, err := f.host.submission("run-fresh", "pool-a", task.submission.Wherry.Placement, []wire.Part{{Text: "new"}}, time.Minute, time.Minute, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := wire.SignRecord(fresh, f.host.key)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.host.publish(cancelled, fresh, encoded, time.Now()); err == nil {
		t.Fatal("publish succeeded on a cancelled context")
	}
	if local, err := f.host.localRun("run-fresh"); err != nil || local != nil {
		t.Fatalf("local record %#v, %v; want none for a run never published", local, err)
	}
}

func TestSweepLeavesAHeldLock(t *testing.T) {
	f := newFixture(t)
	marker := f.host.markerPath("run-a", 1)
	if err := writeState(marker, answerMarker{ExpiryMS: time.Now().Add(-time.Second).UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	unlock, err := f.host.lockAttempt(f.ctx, "run-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	f.host.sweep(f.ctx)
	if _, err := os.Stat(marker + ".lock"); err != nil {
		t.Fatalf("sweep removed a held lock: %v", err)
	}
	unlock()
	f.host.sweep(f.ctx)
	if _, err := os.Stat(marker + ".lock"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("sweep kept an idle lock without a marker: %v", err)
	}
}

func TestALockWaitEndsWithItsContext(t *testing.T) {
	f := newFixture(t)
	unlock, err := f.host.lockAttempt(f.ctx, "run-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	ctx, cancel := context.WithTimeout(f.ctx, 200*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		release, err := f.host.lockAttempt(ctx, "run-a", 1)
		if err == nil {
			release()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("took a lock another holder holds")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the lock wait outlived its context")
	}
}

// admittedWith submits a task declaring credentials through receiver-a, and writes its echo and claimed status.
func (f *fixture) admittedWith(t *testing.T, credentials []wire.TaskCredential) (*testReceiver, claimed) {
	t.Helper()
	receiver := f.addReceiver(t, "receiver-a", "pool-a")
	receiver.advertise(t, f.ctx, nil)
	submitted, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "do the work", Credentials: credentials, Deadline: time.Minute, QueueDeadline: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	task := receiver.claim(t, f.ctx, submitted.RunID)[0]
	receiver.feed(t, f.ctx, submitted.RunID, receiver.echo(t, task), receiver.status(t, task, 1, "claimed-1", "claimed", wire.TaskStateWorking, nil))
	return receiver, task
}

func TestAnswerSealsTheRunsTaskCredentials(t *testing.T) {
	f := newFixture(t)
	deployKey := filepath.Join(t.TempDir(), "deploy-key")
	if err := os.WriteFile(deployKey, []byte("deploy-key-value"), 0o600); err != nil {
		t.Fatal(err)
	}
	f.host.credentials["deploy-key"] = credential.Source{File: deployKey}
	receiver, task := f.admittedWith(t, []wire.TaskCredential{{Name: "deploy-key", File: "id_ed25519"}})
	runID := task.submission.Wherry.Run
	asked := newRequest(t, receiver.binding(task, 1), "model")
	asked.record.TaskCredentials = []string{"deploy-key"}
	receiver.feed(t, f.ctx, runID, receiver.sign(t, &asked.record))
	answering(t, f, f.host)
	f.waitSeals(t, "host-a", 1)
	if values := asked.open(t, f, "host-a"); string(values["model"]) != "model-token" || string(values["deploy-key"]) != "deploy-key-value" || len(values) != 2 {
		t.Fatalf("sealed %q, want the agent's and the task's credentials", values)
	}
}

// Redact replaces each credential value the host resolved, trimmed and JSON-escaped, with the marker of its name
// under no key, and leaves a value under 8 bytes.
func TestRedactReplacesResolvedCredentials(t *testing.T) {
	f := newFixture(t)
	dir := t.TempDir()
	for name, value := range map[string]string{"quoted-key": "say \"held\" key\n", "short-key": "abc1234"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
		f.host.credentials[name] = credential.Source{File: filepath.Join(dir, name)}
	}
	receiver, task := f.admittedWith(t, []wire.TaskCredential{{Name: "quoted-key", File: "quoted"}, {Name: "short-key", File: "short"}})
	asked := newRequest(t, receiver.binding(task, 1), "model")
	asked.record.TaskCredentials = []string{"quoted-key", "short-key"}
	receiver.feed(t, f.ctx, task.submission.Wherry.Run, receiver.sign(t, &asked.record))
	answering(t, f, f.host)
	f.waitSeals(t, "host-a", 1)
	marker := signed.RedactionMarker(nil, "quoted-key")
	if got, want := Redact(`raw say "held" key; json "say \"held\" key"; short abc1234`), "raw "+marker+"; json \""+marker+"\"; short abc1234"; got != want {
		t.Fatalf("Redact = %q, want %q", got, want)
	}
}

// A task credential outside the agent's terms is sealed only when the run declares it, so a pool member cannot ask
// for any credential the host configures.
func TestAnswerRefusesAnUndeclaredTaskCredential(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admittedWith(t, nil)
	runID := task.submission.Wherry.Run
	asked := newRequest(t, receiver.binding(task, 1), "model")
	asked.record.TaskCredentials = []string{"openai"}
	receiver.feed(t, f.ctx, runID, receiver.sign(t, &asked.record))
	answering(t, f, f.host)
	want := "run " + runID + ": skipped a seal request for task credential openai, which the run does not declare"
	for !strings.Contains(f.logs.String(), want) {
		if f.ctx.Err() != nil {
			t.Fatalf("logs:\n%s", f.logs.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	if seals := f.seals(t, "host-a"); seals != 0 {
		t.Fatalf("host topic holds %d seals", seals)
	}
}

func TestSubmitRefusesAnUnconfiguredCredential(t *testing.T) {
	f := newFixture(t)
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
	_, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "p", Credentials: []wire.TaskCredential{{Name: "absent", File: "token"}}, Deadline: time.Minute, QueueDeadline: time.Minute})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "auth required: credential absent is not in this host's configuration") {
		t.Fatalf("submit = %v; want the unconfigured credential named", err)
	}
	if records, _ := readTopic(f.ctx, f.service.Admin, admin.PoolTasksTopic("pool-a")); len(records) != 0 {
		t.Fatal("published a task naming an unconfigured credential")
	}
}

// Submit mints the run's redaction key into its local record, and every attempt's seal carries it.
func TestEverySealCarriesTheKeyMintedAtSubmit(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	local, err := f.host.localRun(runID)
	if err != nil || local == nil || len(local.RedactionKey) != 32 {
		t.Fatalf("local record %+v, %v; want a 32-byte redaction key before any seal", local, err)
	}
	answering(t, f, f.host)
	first := newRequest(t, receiver.binding(task, 1), "model")
	receiver.feed(t, f.ctx, runID, receiver.sign(t, &first.record))
	f.waitSeals(t, "host-a", 1)
	second := newRequest(t, receiver.binding(task, 2), "model")
	receiver.feed(t, f.ctx, runID, receiver.status(t, task, 2, "claimed-2", "claimed", wire.TaskStateWorking, nil), receiver.sign(t, &second.record))
	f.waitSeals(t, "host-a", 2)
	for _, asked := range []request{first, second} {
		if opened := asked.openSealed(t, f, "host-a"); !bytes.Equal(opened.RedactionKey, local.RedactionKey) {
			t.Fatalf("attempt %d sealed redaction key %x, want the key minted at submit %x", asked.record.Attempt, opened.RedactionKey, local.RedactionKey)
		}
	}
}

// Two answerers each reading the run's record before either seals, for different attempts, seal the one key.
func TestConcurrentAnswerersForTwoAttemptsSealOneKey(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	twin, err := NewHost(f.host.kafka, f.host.key, admin.TenantPrincipal("tenant-a", "host-a"), f.stateDir, f.host.credentials, f.host.git, f.host.logger)
	if err != nil {
		t.Fatal(err)
	}
	previous := seal
	t.Cleanup(func() { seal = previous })
	var mu sync.Mutex
	sealed := map[int64][]byte{}
	held, release := make(chan struct{}), make(chan struct{})
	seal = func(binding signed.SealBinding, key *ecdsa.PublicKey, values signed.SealValues) ([]byte, error) {
		mu.Lock()
		sealed[binding.Attempt] = values.RedactionKey
		mu.Unlock()
		if binding.Attempt == 1 {
			close(held)
			<-release
		}
		return previous(binding, key, values)
	}
	// Attempt 1's request expires while its seal is held, so the twin, starting later, passes over it to attempt 2.
	asked := newRequest(t, receiver.binding(task, 1), "model")
	asked.record.ExpiryMS = time.Now().Add(2 * time.Second).UnixMilli()
	receiver.feed(t, f.ctx, runID, receiver.sign(t, &asked.record))
	answering(t, f, f.host)
	select {
	case <-held:
	case <-f.ctx.Done():
		t.Fatalf("attempt 1's seal was never built; logs:\n%s", f.logs.String())
	}
	time.Sleep(time.Until(time.UnixMilli(asked.record.ExpiryMS)))
	later := newRequest(t, receiver.binding(task, 2), "model")
	receiver.feed(t, f.ctx, runID, receiver.status(t, task, 2, "claimed-2", "claimed", wire.TaskStateWorking, nil), receiver.sign(t, &later.record))
	answering(t, f, twin)
	f.waitSeals(t, "host-a", 1)
	close(release)
	local, err := f.host.localRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !bytes.Equal(sealed[1], local.RedactionKey) || !bytes.Equal(sealed[2], local.RedactionKey) {
		t.Fatalf("attempts sealed redaction keys %x, want both the run's %x", sealed, local.RedactionKey)
	}
}

// A record whose key is not 32 bytes is never sealed or re-keyed, and its markers stay unnamed.
func TestACorruptRedactionKeyIsRefused(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	local, err := f.host.localRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	local.RedactionKey = local.RedactionKey[:31]
	if err := f.host.writeRun(*local); err != nil {
		t.Fatal(err)
	}
	path, _ := f.host.runPath(runID)
	asked := newRequest(t, receiver.binding(task, 1), "model")
	receiver.feed(t, f.ctx, runID, receiver.sign(t, &asked.record))
	answering(t, f, f.host)
	for !strings.Contains(f.logs.String(), "run "+runID+": refused to seal: the redaction key in "+path+" is 31 bytes, not 32") {
		if f.ctx.Err() != nil {
			t.Fatalf("no refusal naming the record; logs:\n%s", f.logs.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	if seals := f.seals(t, "host-a"); seals != 0 {
		t.Fatalf("wrote %d seals under a corrupt redaction key", seals)
	}
	if after, err := f.host.localRun(runID); err != nil || !bytes.Equal(after.RedactionKey, local.RedactionKey) {
		t.Fatalf("local record %+v, %v; want the corrupt key kept", after, err)
	}
	marker := signed.RedactionMarker(local.RedactionKey, "model")
	receiver.feed(t, f.ctx, runID,
		receiver.result(t, task, 1, wire.TaskStateCompleted, func(result *wire.Result) {
			result.Task.Artifacts[0].Parts = []wire.Part{{Text: "printed " + marker}}
		}),
		receiver.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateCompleted, func(status *wire.RunStatus) {
			status.StatusUpdate.Status.Message = &wire.Message{MessageID: "msg-s", Role: wire.RoleAgent, Parts: []wire.Part{{Text: "done with " + marker}}, Extensions: []string{wire.ExtensionURI}}
		}))
	awaited, err := f.host.Await(f.ctx, runID, 10*time.Second)
	if err != nil || awaited.Task == nil || awaited.Task.Artifacts[0].Parts[0].Text != "printed "+marker || len(awaited.Redacted) != 0 {
		t.Fatalf("await %#v, %v; want the marker unmapped", awaited, err)
	}
	status, err := f.host.Status(f.ctx, runID)
	if err != nil || status.Status.Message.Parts[0].Text != "done with "+marker || len(status.Redacted) != 0 {
		t.Fatalf("status %#v, %v; want the marker unmapped", status, err)
	}
}

// A local record turning unreadable while await waits does not turn the delivered outcome into an error.
func TestAwaitDeliversTheOutcomeOfARecordTurnedUnreadable(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	local, err := f.host.localRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	marker := signed.RedactionMarker(local.RedactionKey, "model")
	done := make(chan error, 1)
	var awaited AwaitResult
	go func() {
		var err error
		awaited, err = f.host.Await(f.ctx, runID, 20*time.Second)
		done <- err
	}()
	time.Sleep(time.Second)
	path, _ := f.host.runPath(runID)
	if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	receiver.feed(t, f.ctx, runID,
		receiver.result(t, task, 1, wire.TaskStateCompleted, func(result *wire.Result) {
			result.Task.Artifacts[0].Parts = []wire.Part{{Text: "printed " + marker}}
		}),
		receiver.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateCompleted, nil))
	if err := <-done; err != nil || awaited.Status.State != wire.TaskStateCompleted || awaited.Task.Artifacts[0].Parts[0].Text != "printed [redacted:model]" {
		t.Fatalf("await %#v, %v; want the completed outcome", awaited, err)
	}
}

// Await and status name each marker the run's key gives a credential; the feed carries only the marker.
func TestAwaitAndStatusNameTheRunsMarkers(t *testing.T) {
	f := newFixture(t)
	receiver, task := f.admitted(t)
	runID := task.submission.Wherry.Run
	asked := newRequest(t, receiver.binding(task, 1), "model")
	receiver.feed(t, f.ctx, runID, receiver.sign(t, &asked.record))
	answering(t, f, f.host)
	f.waitSeals(t, "host-a", 1)
	sealed := asked.openSealed(t, f, "host-a")
	marker, other := signed.RedactionMarker(sealed.RedactionKey, "model"), signed.RedactionMarker(bytes.Repeat([]byte{1}, 32), "model")
	receiver.feed(t, f.ctx, runID,
		receiver.result(t, task, 1, wire.TaskStateCompleted, func(result *wire.Result) {
			result.Task.Artifacts[0].Parts = []wire.Part{{Text: "printed " + marker + " and " + marker + " and " + other}}
		}),
		receiver.status(t, task, 1, "terminal-1", "terminal", wire.TaskStateCompleted, func(status *wire.RunStatus) {
			status.StatusUpdate.Status.Message = &wire.Message{MessageID: "msg-s", Role: wire.RoleAgent, Parts: []wire.Part{{Text: "done with " + marker}}, Extensions: []string{wire.ExtensionURI}}
		}))
	awaited, err := f.host.Await(f.ctx, runID, 10*time.Second)
	if err != nil || awaited.Task == nil {
		t.Fatalf("await %#v, %v", awaited, err)
	}
	if text := awaited.Task.Artifacts[0].Parts[0].Text; text != "printed [redacted:model] and [redacted:model] and "+other || !maps.Equal(awaited.Redacted, map[string]int{"model": 2}) {
		t.Fatalf("await shows %q counting %v, want model's two markers named and another run's left", text, awaited.Redacted)
	}
	status, err := f.host.Status(f.ctx, runID)
	if err != nil || status.Status.Message.Parts[0].Text != "done with [redacted:model]" || !maps.Equal(status.Redacted, map[string]int{"model": 1}) {
		t.Fatalf("status %#v, %v; want the marker named", status, err)
	}
}

func TestSubmitRefusesALongCredentialName(t *testing.T) {
	f := newFixture(t)
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
	name := strings.Repeat("c", 33)
	f.host.credentials[name] = credential.Source{File: f.secret}
	_, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "p", Credentials: []wire.TaskCredential{{Name: name, File: "token"}}, Deadline: time.Minute, QueueDeadline: time.Minute})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "credential names are at most 32") {
		t.Fatalf("submit = %v; want the name refused", err)
	}
	if records, _ := readTopic(f.ctx, f.service.Admin, admin.PoolTasksTopic("pool-a")); len(records) != 0 {
		t.Fatal("published a credential name over 32 characters")
	}
}

func TestSubmitRefusesOver256ExpectedOutputs(t *testing.T) {
	f := newFixture(t)
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
	var outputs []wire.ExpectedOutput
	for i := range wire.MaxOutputs + 1 {
		outputs = append(outputs, wire.ExpectedOutput{Name: "o" + strings.Repeat("x", i%10) + string(rune('a'+i%26)) + strings.Repeat("y", i/26)})
	}
	_, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: "p", Outputs: outputs, Deadline: time.Minute, QueueDeadline: time.Minute})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "a submission expects at most 256 outputs") {
		t.Fatalf("submit = %v; want the outputs refused", err)
	}
	if records, _ := readTopic(f.ctx, f.service.Admin, admin.PoolTasksTopic("pool-a")); len(records) != 0 {
		t.Fatal("published over 256 expected outputs")
	}
}

func TestSubmitNamesARecordTheBrokerRefuses(t *testing.T) {
	f := newFixture(t)
	f.addReceiver(t, "receiver-a", "pool-a").advertise(t, f.ctx, nil)
	limit := "2000"
	if responses, err := kadm.NewClient(f.service.Admin).AlterTopicConfigs(f.ctx, []kadm.AlterConfig{{Op: kadm.SetConfig, Name: "max.message.bytes", Value: &limit}}, admin.PoolTasksTopic("pool-a")); err != nil || responses[0].Err != nil {
		t.Fatalf("alter topic: %v %v", err, responses)
	}
	random := make([]byte, 10000)
	rand.Read(random)
	_, err := f.host.Submit(f.ctx, SubmitInput{Selector: defaultSelector(), Prompt: hex.EncodeToString(random), Deadline: time.Minute, QueueDeadline: time.Minute})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "-byte record, over the broker's record limit; shorten the prompt or the input files") {
		t.Fatalf("submit = %v; want the record size and a remedy", err)
	}
}
