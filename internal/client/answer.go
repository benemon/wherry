package client

import (
	"context"
	"crypto/ecdsa"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/benemon/wherry/internal/admin"
	"github.com/benemon/wherry/internal/signed"
	"github.com/benemon/wherry/internal/wire"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Answer follows the tenant feed from its start and answers each seal request for a run this host submitted, until
// ctx ends.
func (h *Host) Answer(ctx context.Context) error {
	reader, err := kgo.NewClient(append(h.kafka.Opts(), kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.ConsumeTopics(admin.TenantTopic(h.tenant)), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))...)
	if err != nil {
		return err
	}
	defer reader.Close()
	h.sweep(ctx)
	// pending holds unexpired requests read before their run's local record existed, retried at each poll.
	type request struct {
		run      string
		value    []byte
		expiryMS int64
	}
	pending := map[string]request{}
	for {
		pollCtx, cancel := context.WithTimeout(ctx, retryInterval)
		fetches := reader.PollFetches(pollCtx)
		cancel()
		if ctx.Err() != nil {
			return nil
		}
		if err := fetches.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		fetches.EachRecord(func(record *kgo.Record) {
			jws, err := jose.ParseSignedCompact(string(record.Value), []jose.SignatureAlgorithm{jose.EdDSA})
			if err != nil || len(jws.Signatures) != 1 || jws.Signatures[0].Protected.ExtraHeaders[jose.HeaderType] != wire.SealRequestType {
				return
			}
			// Read before verifying, since every earlier request on the feed has expired; an unverified expiry can only
			// skip a request.
			var unverified struct {
				ExpiryMS int64 `json:"expiry_ms"`
			}
			if json.Unmarshal(jws.UnsafePayloadWithoutVerification(), &unverified) == nil {
				pending[fmt.Sprintf("%d/%d", record.Partition, record.Offset)] = request{string(record.Key), record.Value, unverified.ExpiryMS}
			}
		})
		for key, waiting := range pending {
			if !time.Now().Before(time.UnixMilli(waiting.expiryMS)) || h.answer(ctx, waiting.run, waiting.value) {
				delete(pending, key)
			}
		}
	}
}

// retryInterval separates Answer's passes over requests whose run has no local record yet; lockRetry separates
// attempts on a busy attempt lock.
const (
	retryInterval = time.Second
	lockRetry     = 50 * time.Millisecond
)

// requestLifetime bounds a seal request's expiry: the receiver's 5-minute seal wait and a minute of clock skew.
const requestLifetime = 6 * time.Minute

// seal and produceSeal are variables so tests can delay building a seal and fail its write.
var (
	seal        = signed.Seal
	produceSeal = func(ctx context.Context, h *Host, record *kgo.Record) error { return h.produce(ctx, record) }
)

// answerMarker records that a run's attempt was answered; ExpiryMS is when its request expires.
type answerMarker struct {
	ExpiryMS int64 `json:"expiry_ms"`
}

func (h *Host) markerPath(run string, turn, attempt int64) string {
	return filepath.Join(h.stateDir, "answered", fmt.Sprintf("%s.%d.%d", run, turn, attempt))
}

// sweep removes the markers of attempts whose request has expired or whose run's latest turn has an outcome, and
// lock files without a marker that no answerer holds.
func (h *Host) sweep(ctx context.Context) {
	entries, err := os.ReadDir(filepath.Join(h.stateDir, "answered"))
	if err != nil {
		return
	}
	for _, entry := range entries {
		path := filepath.Join(h.stateDir, "answered", entry.Name())
		if marker, ok := strings.CutSuffix(path, ".lock"); ok {
			if _, err := os.Stat(marker); errors.Is(err, os.ErrNotExist) {
				removeLock(path)
			}
			continue
		}
		var marker answerMarker
		if readState(path, &marker) != nil {
			continue
		}
		runID, _, _ := strings.Cut(entry.Name(), ".")
		if !time.Now().Before(time.UnixMilli(marker.ExpiryMS)) {
			os.Remove(path)
			removeLock(path + ".lock")
		} else if r, err := h.history(ctx, runID); err == nil && r.outcome != nil {
			os.Remove(path)
			removeLock(path + ".lock")
		}
	}
}

// active reports why request is not for the active attempt of the turn this host last submitted, reading the run
// afresh, or "" when it is. A pool member can sign a claimed status it does not hold; claims by two receivers for one
// turn are the trace.
func (h *Host) active(ctx context.Context, request wire.SealRequest) string {
	local, err := h.localRun(request.Run)
	if err != nil || local == nil {
		return "the run's local record is unreadable"
	}
	r, err := h.history(ctx, request.Run)
	if err != nil {
		return "the run is unreadable"
	}
	if request.Turn != local.Turn || request.Turn != r.latest {
		return fmt.Sprintf("turn %d is not the turn this host last submitted", request.Turn)
	}
	var claimant string
	for _, status := range r.statuses {
		if status.Wherry.Turn != request.Turn || status.Wherry.Kind != "claimed" {
			continue
		}
		if claimant == "" {
			claimant = status.Wherry.Receiver
		}
		if status.Wherry.Receiver != claimant {
			return fmt.Sprintf("refused to seal: turn %d is claimed by %s and %s", request.Turn, claimant, status.Wherry.Receiver)
		}
	}
	if claimant != "" && request.Receiver != claimant {
		return fmt.Sprintf("refused to seal: %s asks for turn %d, which %s claimed first", request.Receiver, request.Turn, claimant)
	}
	var current *wire.RunStatus
	for _, status := range r.statuses {
		if status.Wherry.Turn == request.Turn && (current == nil || status.Wherry.Attempt >= current.Wherry.Attempt) {
			current = status
		}
	}
	if current == nil || current.Wherry.ReceiverBinding != request.ReceiverBinding ||
		current.Wherry.Kind != "claimed" && current.Wherry.Kind != "started" && current.Wherry.Kind != "heartbeat" {
		return fmt.Sprintf("attempt %d of turn %d from %s is not the run's active attempt", request.Attempt, request.Turn, request.Receiver)
	}
	return ""
}

// lockAttempt takes an exclusive lock on a run's attempt, shared by every process using the state directory, and
// returns its release. It gives up when ctx ends.
func (h *Host) lockAttempt(ctx context.Context, run string, turn, attempt int64) (func(), error) {
	path := h.markerPath(run, turn, attempt) + ".lock"
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	for {
		lock, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			return nil, err
		}
		err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			// sweep unlinks a lock file it holds; a lock on an unlinked file excludes no one, so take the path again.
			held, statErr := lock.Stat()
			current, pathErr := os.Stat(path)
			if statErr == nil && pathErr == nil && os.SameFile(held, current) {
				return func() { lock.Close() }, nil
			}
			lock.Close()
			continue
		}
		lock.Close()
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(lockRetry):
		}
	}
}

// removeLock unlinks a lock file only while holding it, so no answerer is inside it; a busy lock stays.
func removeLock(path string) {
	lock, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return
	}
	defer lock.Close()
	if syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) == nil {
		os.Remove(path)
	}
}

// answer seals the credentials a request asks for when it comes from a live member of the run's pool holding the
// active attempt of the turn this host last submitted, unanswered and unexpired, and names only credentials this host
// holds: the run's agent's under the pool's terms, and task credentials the run declares. It logs a trusted request
// for its own run that it skips, by run, receiver and credential names only.
func (h *Host) answer(ctx context.Context, runID string, encoded []byte) bool {
	local, err := h.localRun(runID)
	if err != nil || local == nil {
		return false
	}
	r, err := h.history(ctx, runID)
	if err != nil {
		h.logger.Printf("run %s: read the run for a seal request: %v", runID, err)
		return true
	}
	request, err := authorize(ctx, r, encoded, func(v view) (wire.SealRequest, error) {
		return admin.AuthorizeSealRequest(v.state, v.keys, encoded, r.pool, h.tenant)
	})
	if err != nil || request.Run != runID {
		return true
	}
	// Answer passes over expired requests; the expiry is checked again before the write.
	if time.UnixMilli(request.ExpiryMS).After(time.Now().Add(requestLifetime)) {
		h.logger.Printf("run %s: skipped a seal request from %s that expires beyond the seal wait", runID, request.Receiver)
		return true
	}
	// The check, the write and the marker run under the attempt's lock, so concurrent answerers write one seal.
	unlock, err := h.lockAttempt(ctx, runID, request.Turn, request.Attempt)
	if err != nil {
		h.logger.Printf("run %s: lock the attempt: %v", runID, err)
		return true
	}
	defer unlock()
	marker := h.markerPath(runID, request.Turn, request.Attempt)
	if _, err := os.Stat(marker); err == nil {
		return true
	}
	if reason := h.active(ctx, request); reason != "" {
		h.logger.Printf("run %s: %s", runID, reason)
		return true
	}
	terms := r.v.state.Pools[r.pool].Agents
	agent := slices.IndexFunc(terms, func(terms admin.AgentTerms) bool { return terms.Name == local.Placement.Agent })
	values := map[string][]byte{}
	for i, name := range append(slices.Clone(request.Credentials), request.TaskCredentials...) {
		path, configured := h.credentials[name]
		switch {
		case i >= len(request.Credentials) && !slices.Contains(local.TaskCredentials, name):
			h.logger.Printf("run %s: skipped a seal request for task credential %s, which the run does not declare", runID, name)
			return true
		case i < len(request.Credentials) && (agent < 0 || !slices.Contains(terms[agent].Credentials, name)):
			h.logger.Printf("run %s: skipped a seal request for credential %s, which agent %s's terms do not approve", runID, name, local.Placement.Agent)
			return true
		case !configured:
			h.logger.Printf("run %s: skipped a seal request for credential %s, which this host does not configure", runID, name)
			return true
		}
		if values[name], err = os.ReadFile(path); err != nil {
			h.logger.Printf("run %s: read credential %s: %v", runID, name, errors.Unwrap(err))
			return true
		}
	}
	sealed, err := seal(signed.SealBinding{
		Tenant: request.Tenant, Pool: request.Pool, Receiver: request.Receiver, KeyID: request.KeyID, Run: request.Run,
		Turn: request.Turn, Attempt: request.Attempt, Credentials: request.Credentials, TaskCredentials: request.TaskCredentials, ExpiryMS: request.ExpiryMS,
	}, request.Key.Key.(*ecdsa.PublicKey), values)
	if err != nil {
		// Seal's errors name credentials, never values.
		h.logger.Printf("run %s: seal for %s not built: %v", runID, request.Receiver, err)
		return true
	}
	if !time.Now().Before(time.UnixMilli(request.ExpiryMS)) {
		return true
	}
	// A terminal status or another claim that landed while the seal was built stops the write.
	if reason := h.active(ctx, request); reason != "" {
		h.logger.Printf("run %s: %s", runID, reason)
		return true
	}
	// The marker follows the acknowledged write: a crash between them answers again, and the receiver opens the first
	// seal of the two.
	if err := produceSeal(ctx, h, &kgo.Record{Topic: admin.HostTopic(h.tenant, h.host), Key: []byte(runID), Value: sealed}); err != nil {
		h.logger.Printf("run %s: seal for %s not written: %v", runID, request.Receiver, err)
		return true
	}
	if err := writeState(marker, answerMarker{ExpiryMS: request.ExpiryMS}); err != nil {
		h.logger.Printf("run %s: record a seal answer: %v", runID, err)
	}
	h.logger.Printf("run %s: sealed %v to %s for turn %d attempt %d", runID, append(slices.Clone(request.Credentials), request.TaskCredentials...), request.Receiver, request.Turn, request.Attempt)
	return true
}
