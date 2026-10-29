package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"strings"
	"testing"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	jose "github.com/go-jose/go-jose/v4"
)

// logs -f prints lines while the run is live, in order and once each, though the feed carries a line twice, and
// returns once the run is terminal.
func TestLogsFollowPrintsEachLineOnceUntilTheRunEnds(t *testing.T) {
	b := newBus(t)
	admitted := b.admitted(t)
	runID := admitted.submission.Wherry.Run
	b.feed(t, runID, b.chunk(t, admitted, 1, 0, wire.Part{Text: "first"}), b.chunk(t, admitted, 1, 6, wire.Part{Text: "second"}))
	stdout, stderr := &syncBuffer{}, &syncBuffer{}
	followed := make(chan error, 1)
	go func() { followed <- runLogs(b.ctx, b.client, []string{runID, "-f"}, stdout, stderr) }()
	for !strings.Contains(stdout.String(), "second\n") {
		select {
		case err := <-followed:
			t.Fatalf("logs -f ended before the run: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
	}
	if stdout.String() != "first\nsecond\n" {
		t.Fatalf("streamed %q", stdout.String())
	}
	b.feed(t, runID, b.chunk(t, admitted, 1, 6, wire.Part{Text: "second"}), b.chunk(t, admitted, 1, 13, wire.Part{Text: "third"}))
	for !strings.Contains(stdout.String(), "third\n") {
		select {
		case err := <-followed:
			t.Fatalf("logs -f ended before the run: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
	}
	select {
	case err := <-followed:
		t.Fatalf("logs -f ended before the run: %v", err)
	case <-time.After(2 * time.Second):
	}
	b.feed(t, runID, b.result(t, admitted, []wire.Part{{Text: "done"}}), b.status(t, admitted, "terminal-1", "terminal", now(wire.TaskStateCompleted)))
	select {
	case err := <-followed:
		if err != nil {
			t.Fatalf("logs -f: %v; stderr %s", err, stderr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("logs -f still running after the run ended")
	}
	if stdout.String() != "first\nsecond\nthird\n" || stderr.String() != "" {
		t.Fatalf("stdout %q stderr %q", stdout.String(), stderr.String())
	}
	if err := runLogs(b.ctx, b.client, []string{"run-missing", "-f"}, stdout, stderr); err == nil || err.Error() != "transcript run-missing: run not found" {
		t.Fatalf("logs -f of an unknown run: %v", err)
	}
}

// logs -f answers no seal request, and says once on stderr that the run waits for one no host has answered.
func TestLogsFollowAnswersNoSealRequest(t *testing.T) {
	b := newBus(t)
	admitted := b.admitted(t)
	runID := admitted.submission.Wherry.Run
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	request := wire.SealRequest{ReceiverBinding: b.binding(admitted), Key: jose.JSONWebKey{Key: &key.PublicKey}, KeyID: signed.SealKeyID(&key.PublicKey),
		Credentials: []string{"model"}, ExpiryMS: time.Now().Add(time.Minute).UnixMilli()}
	b.feed(t, runID, b.sign(t, &request))
	stdout, stderr := &syncBuffer{}, &syncBuffer{}
	ctx, stop := context.WithCancel(b.ctx)
	defer stop()
	followed := make(chan error, 1)
	go func() { followed <- runLogs(ctx, b.client, []string{runID, "--follow"}, stdout, stderr) }()
	const waiting = "run is waiting for credentials; run wherry wait "
	deadline := time.Now().Add(20 * time.Second)
	for !strings.Contains(stderr.String(), waiting) {
		if time.Now().After(deadline) {
			t.Fatalf("logs -f never said the run is waiting; stderr %q", stderr.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Past another poll, the line has not repeated.
	time.Sleep(streamRetry + time.Second)
	if want := waiting + runID + "\n"; stderr.String() != want {
		t.Fatalf("stderr %q, want %q once", stderr.String(), want)
	}
	records, err := readTopic(b.ctx, b.receiver, admin.HostTopic("tenant-a", "host-a"))
	if err != nil {
		t.Fatal(err)
	}
	binding := signed.SealBinding{Tenant: request.Tenant, Pool: request.Pool, Receiver: request.Receiver, KeyID: request.KeyID, Run: request.Run,
		Attempt: request.Attempt, Credentials: request.Credentials, ExpiryMS: request.ExpiryMS}
	for _, record := range records {
		if _, err := signed.OpenSeal(record.Value, binding, key, time.Now()); err == nil {
			t.Fatal("logs -f answered the seal request")
		}
	}
	b.feed(t, runID, b.resultIn(t, admitted, wire.TaskStateFailed), b.status(t, admitted, "terminal-1", "terminal", now(wire.TaskStateFailed)))
	select {
	case err := <-followed:
		if err != nil {
			t.Fatalf("logs -f: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("logs -f still running after the run ended")
	}
}
