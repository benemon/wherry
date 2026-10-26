package client

import (
	"context"
	"crypto/ecdsa"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"log"
	"maps"
	"os"
	"path/filepath"
	"runtime/debug"
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

// Answer follows the tenant feed from the earliest run of this host that has not ended and answers each seal request for
// a run this host submitted, until ctx ends. Each line it logs names its process and release, since every answerer of a
// state directory reads every request.
func (h *Host) Answer(ctx context.Context) error {
	f, err := h.follow(ctx)
	if err != nil {
		return ignoreCancel(ctx, err)
	}
	version := "(devel)"
	if info, ok := debug.ReadBuildInfo(); ok {
		version = info.Main.Version
	}
	logger := log.New(h.logger.Writer(), fmt.Sprintf("%sanswerer pid=%d version=%s: ", h.logger.Prefix(), os.Getpid(), version), h.logger.Flags())
	logger.Print("following the tenant feed for seal requests")
	h.sweep(ctx)
	// pending holds unexpired requests not yet settled, as when read before their run's local record existed, retried at
	// each retry interval.
	type request struct {
		run      string
		value    []byte
		expiryMS int64
		// failed holds when each of its credentials' command sources last failed.
		failed map[string]time.Time
	}
	pending := map[string]request{}
	// logged holds each pending request's last logged reason, so a request retried each interval logs a reason once.
	logged := map[string]string{}
	settle := func(key string) {
		retry := func(format string, args ...any) {
			if ctx.Err() != nil {
				return
			}
			if reason := fmt.Sprintf(format, args...); logged[key] != reason {
				logged[key] = reason
				logger.Print(reason)
			}
		}
		if waiting := pending[key]; !time.Now().Before(time.UnixMilli(waiting.expiryMS)) || h.answer(ctx, logger, waiting.run, waiting.value, waiting.failed, retry) {
			delete(pending, key)
			delete(logged, key)
		}
	}
	retries := time.NewTicker(retryInterval)
	defer retries.Stop()
	next := f.start
	for {
		records, read, woken, _ := f.from(next)
		next = max(next, read)
		for _, record := range records {
			jws, err := jose.ParseSignedCompact(string(record.Value), []jose.SignatureAlgorithm{jose.EdDSA})
			if err != nil || len(jws.Signatures) != 1 || jws.Signatures[0].Protected.ExtraHeaders[jose.HeaderType] != wire.SealRequestType {
				continue
			}
			// Read before verifying, since every earlier request on the feed has expired; an unverified expiry can only
			// skip a request.
			var unverified struct {
				ExpiryMS int64 `json:"expiry_at_ms"`
			}
			if json.Unmarshal(jws.UnsafePayloadWithoutVerification(), &unverified) == nil {
				key := fmt.Sprintf("%d/%d", record.Partition, record.Offset)
				pending[key] = request{string(record.Key), record.Value, unverified.ExpiryMS, map[string]time.Time{}}
				settle(key)
			}
		}
		select {
		case <-ctx.Done():
		case <-woken:
		case <-f.ctx.Done():
		case <-retries.C:
			for key := range pending {
				settle(key)
			}
		}
		if ctx.Err() != nil {
			return nil
		}
		if f.ctx.Err() != nil {
			// The follower's owner stopped it; another follower reads every run that has not ended.
			if f, err = h.follow(ctx); err != nil {
				return ignoreCancel(ctx, err)
			}
			next = max(next, f.start)
		}
	}
}

// ignoreCancel drops err when ctx has ended, since work the answerer's own end cuts short is no failure.
func ignoreCancel(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return nil
	}
	return err
}

// retryInterval separates Answer's passes over requests whose run has no local record yet; lockRetry separates
// attempts on a busy attempt lock.
const (
	retryInterval = time.Second
	lockRetry     = 50 * time.Millisecond
)

// commandBackoff separates the runs of a command source that failed for a seal request: its command's timeout, so an
// interactive unlock prompts no more often than it can be answered. Tests shorten it.
var commandBackoff = 60 * time.Second

// requestLifetime bounds a seal request's expiry: the receiver's seal wait and a minute of clock skew.
const requestLifetime = wire.SealWait + time.Minute

// seal and produceSeal are variables so tests can delay building a seal and fail its write.
var (
	seal        = signed.Seal
	produceSeal = func(ctx context.Context, h *Host, record *kgo.Record) error { return h.produce(ctx, record) }
)

// answerMarker records that a run's attempt was answered; ExpiryMS is when its request expires.
type answerMarker struct {
	ExpiryMS int64 `json:"expiry_at_ms"`
}

func (h *Host) markerPath(run string, attempt int64) string {
	return filepath.Join(h.stateDir, "answered", fmt.Sprintf("%s.%d", run, attempt))
}

// sweep removes the markers of attempts whose request has expired or whose run has an outcome, and
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

// active reports why request is not for the active attempt of the run, reading the run afresh, or "" when it is.
func (h *Host) active(ctx context.Context, request wire.SealRequest) string {
	local, err := h.localRun(request.Run)
	if err != nil || local == nil {
		return "the run's local record is unreadable"
	}
	r, err := h.history(ctx, request.Run)
	if err != nil {
		return "the run is unreadable"
	}
	var claimant string
	for _, status := range r.statuses {
		if status.Wherry.Kind != "claimed" {
			continue
		}
		if claimant == "" {
			claimant = status.Wherry.Receiver
		}
		if status.Wherry.Receiver != claimant {
			return fmt.Sprintf("refused to seal: run is claimed by %s and %s", claimant, status.Wherry.Receiver)
		}
	}
	if claimant != "" && request.Receiver != claimant {
		return fmt.Sprintf("refused to seal: %s asks for the run, which %s claimed first", request.Receiver, claimant)
	}
	var current *wire.RunStatus
	for _, status := range r.statuses {
		if current == nil || status.Wherry.Attempt >= current.Wherry.Attempt {
			current = status
		}
	}
	if current == nil || current.Wherry.ReceiverBinding != request.ReceiverBinding ||
		current.Wherry.Kind != "claimed" && current.Wherry.Kind != "started" && current.Wherry.Kind != "heartbeat" {
		return fmt.Sprintf("attempt %d from %s is not the run's active attempt", request.Attempt, request.Receiver)
	}
	return ""
}

// lockAttempt takes an exclusive lock on a run's attempt, shared by every process using the state directory, and
// returns its release. It gives up when ctx ends.
func (h *Host) lockAttempt(ctx context.Context, run string, attempt int64) (func(), error) {
	path := h.markerPath(run, attempt) + ".lock"
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

// answer seals the credentials a valid request asks for. Its logs carry names and causes, never a value or a source's
// output. It reports whether the request is settled; a failure a later pass can clear is logged through retry and
// leaves it unsettled, so Answer tries it again until it expires. failed holds when each of the request's command
// sources last failed; while one has failed within commandBackoff, no source is resolved.
func (h *Host) answer(ctx context.Context, logger *log.Logger, runID string, encoded []byte, failed map[string]time.Time, retry func(format string, args ...any)) bool {
	local, err := h.localRun(runID)
	if err != nil {
		retry("run %q: skipped a seal request: the run's local record is unreadable", runID)
		return false
	}
	if local == nil {
		retry("run %q: skipped a seal request: this host has no local record of the run", runID)
		return false
	}
	r, err := h.history(ctx, runID)
	if err != nil {
		retry("run %s: read the run for a seal request: %v", runID, err)
		return false
	}
	// A run that has ended needs no seal; sweep has removed the marker of an attempt answered before it ended.
	if r.outcome != nil {
		return true
	}
	_, kid := header(encoded)
	request, err := authorize(ctx, r, kid, func(v view) (wire.SealRequest, error) {
		return admin.AuthorizeSealRequest(v.state, v.keys, encoded, r.pool, h.tenant)
	})
	if err != nil {
		logger.Printf("run %s: skipped a seal request: %v", runID, err)
		return true
	}
	if request.Run != runID {
		logger.Printf("run %s: skipped a seal request from %s for attempt %d, which names run %q", runID, request.Receiver, request.Attempt, request.Run)
		return true
	}
	// Answer passes over expired requests; the expiry is checked again before the write.
	if time.UnixMilli(request.ExpiryMS).After(time.Now().Add(requestLifetime)) {
		logger.Printf("run %s: skipped a seal request from %s that expires beyond the seal wait", runID, request.Receiver)
		return true
	}
	for _, at := range failed {
		if time.Since(at) < commandBackoff {
			return false
		}
	}
	// Sources resolve before the attempt's lock, so a slow command does not hold up the host's other answerers.
	terms := r.v.state.Pools[r.pool].Agents
	agent := slices.IndexFunc(terms, func(terms admin.AgentTerms) bool { return terms.Name == local.Placement.Agent })
	values := map[string][]byte{}
	for i, name := range append(slices.Clone(request.Credentials), request.TaskCredentials...) {
		source, configured := h.credentials[name]
		switch {
		case i >= len(request.Credentials) && !slices.Contains(local.TaskCredentials, name):
			logger.Printf("run %s: skipped a seal request for task credential %s, which the run does not declare", runID, name)
			return true
		case i < len(request.Credentials) && (agent < 0 || !slices.ContainsFunc(terms[agent].Credentials, func(credential admin.AgentCredential) bool { return credential.Name == name })):
			logger.Printf("run %s: skipped a seal request for credential %s, which agent %s's terms do not approve", runID, name, local.Placement.Agent)
			return true
		case !configured:
			logger.Printf("run %s: skipped a seal request for credential %s, which this host does not configure", runID, name)
			return true
		}
		if values[name], err = source.Resolve(ctx); err != nil {
			if len(source.Command) == 0 {
				retry("run %s: resolve credential %s: %v", runID, name, err)
			} else if ctx.Err() == nil {
				if _, before := failed[name]; !before {
					logger.Printf("run %s: resolve credential %s: %v", runID, name, err)
				}
				failed[name] = time.Now()
			}
			return false
		}
		hold(name, values[name])
	}
	// The check, the write and the marker run under the attempt's lock, so concurrent answerers write one seal.
	unlock, err := h.lockAttempt(ctx, runID, request.Attempt)
	if err != nil {
		if ctx.Err() == nil {
			logger.Printf("run %s: lock the attempt: %v", runID, err)
		}
		return true
	}
	defer unlock()
	marker := h.markerPath(runID, request.Attempt)
	if _, err := os.Stat(marker); err == nil {
		logger.Printf("run %s: skipped a seal request from %s for attempt %d, which another answerer already sealed", runID, request.Receiver, request.Attempt)
		return true
	}
	if reason := h.active(ctx, request); reason != "" {
		retry("run %s: %s", runID, reason)
		return false
	}
	local, err = h.localRun(runID)
	if err != nil || local == nil {
		logger.Printf("run %s: read the local record before sealing: %v", runID, err)
		return true
	}
	// A new key would stop the markers already on the feed mapping to their names.
	if len(local.RedactionKey) != 32 {
		path, _ := h.runPath(runID)
		logger.Printf("run %s: refused to seal: the redaction key in %s is %d bytes, not 32", runID, path, len(local.RedactionKey))
		return true
	}
	if len(request.S3Inputs)+len(request.S3Outputs) > 0 {
		for _, input := range request.S3Inputs {
			if !slices.Contains(local.S3Inputs, input) {
				logger.Printf("run %s: skipped a seal request for S3 input %s, which the run does not declare", runID, input)
				return true
			}
		}
		for _, output := range request.S3Outputs {
			if !slices.Contains(local.S3Outputs, output) {
				logger.Printf("run %s: skipped a seal request for S3 output %s, which the run does not declare", runID, output)
				return true
			}
		}
		storage := r.v.state.Pools[r.pool].Storage
		store, err := h.bucket(ctx, r.pool, storage)
		if err != nil {
			// The error names the storage credentials; submit reports it, so the answerer logs a fixed reason.
			if ctx.Err() == nil {
				logger.Printf("run %s: skipped a seal request for S3 URLs: the host's storage credential could not be used", runID)
			}
			return true
		}
		urls, err := presign(ctx, store, storage.Bucket, h.tenant, runID, request.S3Inputs, request.S3Outputs, time.UnixMilli(local.DeadlineMS))
		if err != nil {
			if ctx.Err() == nil {
				logger.Printf("run %s: skipped a seal request for S3 URLs: %v", runID, err)
			}
			return true
		}
		maps.Copy(values, urls)
	}
	sealed, err := seal(signed.SealBinding{
		Tenant: request.Tenant, Pool: request.Pool, Receiver: request.Receiver, KeyID: request.KeyID, Run: request.Run,
		Attempt: request.Attempt, Credentials: request.Credentials, TaskCredentials: request.TaskCredentials,
		S3Inputs: request.S3Inputs, S3Outputs: request.S3Outputs, ExpiryMS: request.ExpiryMS,
	}, request.Key.Key.(*ecdsa.PublicKey), signed.SealValues{Values: values, RedactionKey: local.RedactionKey})
	if err != nil {
		// Seal's errors name credentials, never values.
		logger.Printf("run %s: seal for %s not built: %v", runID, request.Receiver, err)
		return true
	}
	if !time.Now().Before(time.UnixMilli(request.ExpiryMS)) {
		logger.Printf("run %s: skipped a seal request from %s for attempt %d, which expired while its seal was built", runID, request.Receiver, request.Attempt)
		return true
	}
	// A terminal status or another claim that landed while the seal was built stops the write.
	if reason := h.active(ctx, request); reason != "" {
		if ctx.Err() == nil {
			logger.Printf("run %s: %s", runID, reason)
		}
		return true
	}
	// The marker follows the acknowledged write: a crash between them answers again, and the receiver opens the first
	// seal of the two.
	if err := produceSeal(ctx, h, &kgo.Record{Topic: admin.HostTopic(h.tenant, h.host), Key: []byte(runID), Value: sealed}); err != nil {
		retry("run %s: seal for %s not written: %v", runID, request.Receiver, err)
		return false
	}
	if err := writeState(marker, answerMarker{ExpiryMS: request.ExpiryMS}); err != nil {
		logger.Printf("run %s: record a seal answer: %v", runID, err)
	}
	logger.Printf("run %s: sealed %v to %s for attempt %d", runID, append(slices.Clone(request.Credentials), request.TaskCredentials...), request.Receiver, request.Attempt)
	if len(request.S3Inputs)+len(request.S3Outputs) > 0 {
		logger.Printf("run %s: sealed URLs for S3 inputs %v and outputs %v to %s", runID, request.S3Inputs, request.S3Outputs, request.Receiver)
	}
	return true
}
