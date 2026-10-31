package client

import (
	"context"
	"crypto/rand"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/benemon/wherry/internal/wire"
	"github.com/minio/minio-go/v7"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

// produceReplacement and replacing are variables so tests can fail a replacement's write and hold a pass between
// reading a failed run's record and recording its replacement.
var (
	produceReplacement = func(ctx context.Context, h *Host, binding wire.SubmissionBinding, encoded []byte) error {
		return h.produceTask(ctx, binding, encoded)
	}
	replacing = func(string) {}
)

// resubmitLost resubmits each run of this host's local records that ended key-lost and has no produced replacement. A
// run past its task_started_by with no replacement recorded is passed over. The runs still before their bound are judged
// from one read of the tenant feed from the lowest of their feed offsets. Failures are logged; a later pass retries them.
func (h *Host) resubmitLost(ctx context.Context) {
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
	if r.local == nil || r.local.Task == nil || !r.keyLost() {
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
// replacement, or "" when the run is not to be resubmitted. lost reports that the caller read the run's latest status as
// key-lost, which no later record changes; otherwise resubmit reads the run under the lock. Under a lock on the run
// shared by every process using the state directory, it produces a stored replacement not yet produced and builds
// nothing new; otherwise it records the replacement's run ID, builds and signs the replacement from the run's template,
// stores the signed bytes, produces them and marks them produced. A replacement whose bytes were not stored is built
// again. A run without a local record takes no lock.
func (h *Host) resubmit(ctx context.Context, runID string, lost bool) (string, error) {
	path, ok := h.runPath(runID)
	if !ok {
		return "", fmt.Errorf("resubmit %s: %w", runID, ErrRunNotFound)
	}
	if local, err := h.localRun(runID); err != nil || local == nil || local.Task == nil {
		return "", err
	}
	lock := filepath.Join(h.stateDir, "resubmit", filepath.Base(path)+".lock")
	unlock, err := h.lockFile(ctx, lock)
	if err != nil {
		return "", err
	}
	defer func() {
		os.Remove(lock)
		unlock()
	}()
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
				if err := produceReplacement(ctx, h, replacement.Task.Wherry, replacement.Signed); err != nil {
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
		local.Replacement = wire.RandomID("run-")
		if err := h.writeRun(*local); err != nil {
			return "", err
		}
	}
	v, err := h.load(ctx)
	if err != nil {
		return "", err
	}
	ads, err := h.reader.ReadAdvertisements(ctx, v.state, v.keys, now, liveness)
	if err != nil {
		return "", err
	}
	poolKey, err := h.poolKey(ctx, v, local.Pool, ads)
	if err != nil {
		return "", err
	}
	terms := v.state.Pools[local.Pool]
	submission := *local.Task
	submission.Message.TaskID, submission.Message.ContextID, submission.Message.MessageID = local.Replacement, local.Replacement, wire.RandomID("msg-")
	submission.Wherry.Run, submission.Wherry.Replaces, submission.Wherry.S3InputRun = local.Replacement, runID, local.Task.Wherry.InputRun()
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
	record, err := h.newRecord(ctx, &submission, encoded, now, redactionKey)
	if err != nil {
		return "", err
	}
	record.Signed = encoded
	if err := h.writeRun(record); err != nil {
		return "", err
	}
	if err := produceReplacement(ctx, h, submission.Wherry, encoded); err != nil {
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
