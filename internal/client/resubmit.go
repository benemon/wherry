package client

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/benemon/wherry/internal/operator"
	"github.com/benemon/wherry/internal/wire"
	"github.com/minio/minio-go/v7"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

// produceReplacement and replacing are variables so tests can fail a replacement's write and hold a pass between
// reading a failed run's record and recording its replacement.
var (
	produceReplacement = func(ctx context.Context, h *Host, topic string, binding wire.SubmissionBinding, encoded []byte) error {
		return h.produceTask(ctx, topic, binding, encoded)
	}
	replacing = func(string) {}
)

// resubmitLost resubmits each run of this host's local records that ended key-lost and has no produced replacement,
// judging the runs before their task_started_by from one read of the tenant feed.
func (h *Host) resubmitLost(ctx context.Context) {
	h.sweep()
	entries, err := os.ReadDir(filepath.Join(h.stateDir, "runs"))
	if err != nil {
		return
	}
	now := time.Now()
	pending := map[string]*runRecord{}
	offset := int64(math.MaxInt64)
	for _, entry := range entries {
		runID, ok := strings.CutSuffix(entry.Name(), ".json")
		if !ok || entry.IsDir() {
			continue
		}
		local, err := h.localRun(runID)
		if err != nil || local == nil || local.Task == nil {
			continue
		}
		if local.Replacement == "" {
			if now.Before(wire.PastBound(local.TaskStartedBy)) {
				pending[runID] = local
				offset = min(offset, local.FeedOffset)
			}
			continue
		}
		if replacement, err := h.localRun(local.Replacement); err == nil && replacement != nil && replacement.Produced {
			continue
		}
		h.logResubmit(ctx, runID, false)
	}
	if len(pending) == 0 {
		return
	}
	h.followMu.Lock()
	f := h.follower
	h.followMu.Unlock()
	var v view
	var records []*kgo.Record
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() (err error) {
		v, err = h.load(groupCtx)
		return err
	})
	group.Go(func() (err error) {
		records, _, err = h.readFeed(groupCtx, f, offset)
		return err
	})
	if err := group.Wait(); err != nil {
		if ctx.Err() == nil {
			h.logger.Printf("resubmit: %v", err)
		}
		return
	}
	byRun := map[string][]*kgo.Record{}
	for _, record := range records {
		if _, ok := pending[string(record.Key)]; ok {
			byRun[string(record.Key)] = append(byRun[string(record.Key)], record)
		}
	}
	// One reread map and a view carried forward bound the reloads to one per unknown kid across all runs.
	reread := map[string]bool{}
	for runID, local := range pending {
		r := newRunView(h, v, runID, local)
		r.reread = reread
		r.read(ctx, byRun[runID])
		v = r.v
		h.resubmitIfLost(ctx, r)
	}
}

// resubmitIfLost resubmits r's run when it ended key-lost, logging a failure; a later pass retries it.
func (h *Host) resubmitIfLost(ctx context.Context, r *runView) {
	if !r.keyLost() {
		return
	}
	h.logResubmit(ctx, r.run, true)
}

// logResubmit resubmits runID as resubmit does, logging a failure.
func (h *Host) logResubmit(ctx context.Context, runID string, lost bool) {
	if _, err := h.resubmit(ctx, runID, lost); err != nil && ctx.Err() == nil {
		h.logger.Printf("run %s: resubmit: %v", runID, err)
	}
}

// resubmit replaces runID, a run of this host that ended key-lost before its task_started_by, and returns its
// replacement, or "" when the run is not to be resubmitted; lost reports that the caller read the run's latest status as
// key-lost. The lock on the run is shared by every process using the state directory, so a run is replaced at most once.
func (h *Host) resubmit(ctx context.Context, runID string, lost bool) (string, error) {
	if _, ok := h.runPath(runID); !ok {
		return "", fmt.Errorf("resubmit %s: %w", runID, ErrRunNotFound)
	}
	if local, err := h.localRun(runID); err != nil || local == nil || local.Task == nil {
		return "", err
	}
	unlock, err := h.lockRun(ctx, runID)
	if err != nil {
		return "", err
	}
	defer unlock()
	local, err := h.localRun(runID)
	if err != nil || local == nil || local.Task == nil {
		return "", err
	}
	if local.Replacement != "" {
		replacement, err := h.localRun(local.Replacement)
		if err != nil {
			return "", err
		}
		if replacement != nil && len(replacement.Signed) > 0 {
			if !replacement.Produced {
				if err := produceReplacement(ctx, h, replacement.Topic, replacement.Task.Wherry, replacement.Signed); err != nil {
					return "", err
				}
				replacement.Produced = true
				if err := h.writeRun(*replacement); err != nil {
					return "", err
				}
			}
			return local.Replacement, nil
		}
	}
	// A cancel recorded on the run is not carried to a replacement.
	if local.CancelRequestedMS != 0 {
		return "", nil
	}
	now := time.Now()
	if !now.Before(wire.PastBound(local.TaskStartedBy)) {
		return "", nil
	}
	if local.Replacement == "" {
		if !lost {
			r, err := h.history(ctx, runID)
			if err != nil {
				return "", err
			}
			if !r.keyLost() {
				return "", nil
			}
		}
		replacing(runID)
		local.ReplacementNonce = wire.RunNonce()
		local.Replacement = wire.RunID(h.tenant, h.host, local.ReplacementNonce)
		if err := h.writeRun(*local); err != nil {
			return "", err
		}
	}
	v, err := h.load(ctx)
	if err != nil {
		return "", err
	}
	poolKey, err := h.poolKey(ctx, v, local.Pool)
	if err != nil {
		return "", err
	}
	terms := v.state.Pools[local.Pool]
	submission := *local.Task
	// The replacement keeps the original's contextId, referenceTaskIds and git base.
	submission.Message.TaskID, submission.Message.MessageID = local.Replacement, wire.RandomID("msg-")
	submission.Wherry.Run, submission.Wherry.RunNonce = local.Replacement, local.ReplacementNonce
	submission.Wherry.Replaces, submission.Wherry.S3InputRun = runID, local.Task.Wherry.InputRun()
	var agent operator.AgentTerms
	if i := slices.IndexFunc(terms.Agents, func(agent operator.AgentTerms) bool { return agent.Name == submission.Wherry.Placement.Agent }); i >= 0 {
		agent = terms.Agents[i]
	}
	if submission.Wherry.Supplied, err = h.supplied(agent, local.Task.Wherry.Supplied); err != nil {
		return "", err
	}
	var store *minio.Client
	if len(submission.Wherry.S3Inputs)+len(s3OutputNames(submission.Wherry.Outputs)) > 0 {
		if store, err = h.bucket(ctx, local.Pool, terms.Storage); err != nil {
			return "", err
		}
	}
	redactionKey := make([]byte, 32)
	if _, err := rand.Read(redactionKey); err != nil {
		return "", err
	}
	encoded, err := h.enveloped(ctx, &submission, terms, poolKey, store, redactionKey)
	if err != nil {
		return "", err
	}
	// The replacement keeps the original's signature topic: agent and selector cannot reconstruct the signature a task
	// was placed on.
	record, err := h.newRecord(ctx, &submission, local.Topic, encoded, now, redactionKey)
	if err != nil {
		return "", err
	}
	record.Signed = encoded
	if err := h.writeRun(record); err != nil {
		return "", err
	}
	if err := produceReplacement(ctx, h, record.Topic, submission.Wherry, encoded); err != nil {
		return "", err
	}
	record.Produced = true
	if err := h.writeRun(record); err != nil {
		return "", err
	}
	h.logger.Printf("run %s: lost its pool key before it started; resubmitted as run %s", runID, record.Run)
	return record.Run, nil
}

// keyLost reports whether the run's latest attempt ended with the wherry event key-lost.
func (r *runView) keyLost() bool {
	var filter wire.AttemptFilter
	var latest *wire.RunStatus
	for _, status := range r.statuses {
		if admit(&filter, status) {
			latest = status
		}
	}
	return latest != nil && latest.Wherry.Kind == "terminal" && latest.Wherry.Event == wire.EventKeyLost
}

// resubmittable reports whether the run ended key-lost and this host may still resubmit it at now.
func (r *runView) resubmittable(now time.Time) bool {
	return r.local != nil && r.local.Task != nil && r.keyLost() && now.Before(wire.PastBound(r.local.TaskStartedBy))
}

// chain is runID followed by each replacement its local records name whose signed bytes are stored. A chain that names
// a run twice is refused.
func (h *Host) chain(runID string) ([]string, error) {
	chain := []string{runID}
	for {
		local, err := h.localRun(chain[len(chain)-1])
		if err != nil || local == nil || local.Replacement == "" {
			return chain, err
		}
		if slices.Contains(chain, local.Replacement) {
			return nil, fmt.Errorf("%w: run %s's replacements form a cycle at run %s", ErrInvalidInput, runID, local.Replacement)
		}
		next, err := h.localRun(local.Replacement)
		if err != nil || next == nil || len(next.Signed) == 0 {
			return chain, err
		}
		chain = append(chain, local.Replacement)
	}
}

// lockRetry separates attempts on a busy lock.
const lockRetry = 50 * time.Millisecond

// sweep removes the resubmission lock files no process holds.
func (h *Host) sweep() {
	locks, err := os.ReadDir(filepath.Join(h.stateDir, "resubmit"))
	if err != nil {
		return
	}
	for _, lock := range locks {
		removeLock(filepath.Join(h.stateDir, "resubmit", lock.Name()))
	}
}

// lockRun takes the lock on runID that every process using the state directory shares, returning its release.
func (h *Host) lockRun(ctx context.Context, runID string) (func(), error) {
	path, ok := h.runPath(runID)
	if !ok {
		return nil, ErrRunNotFound
	}
	lock := filepath.Join(h.stateDir, "resubmit", filepath.Base(path)+".lock")
	unlock, err := h.lockFile(ctx, lock)
	if err != nil {
		return nil, err
	}
	return func() {
		os.Remove(lock)
		unlock()
	}, nil
}

// lockFile takes an exclusive lock on the file at path, shared by every process using the state directory, and returns
// its release. It gives up when ctx ends.
func (h *Host) lockFile(ctx context.Context, path string) (func(), error) {
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

// removeLock unlinks a lock file only while holding it, so no process is inside it; a busy lock stays.
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
