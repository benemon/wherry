package client

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/benemon/wherry/internal/wire"
	"github.com/minio/minio-go/v7"
)

// produceReplacement and replacing are variables so tests can fail a replacement's write and hold a pass between
// reading a failed run's record and recording its replacement.
var (
	produceReplacement = func(ctx context.Context, h *Host, binding wire.SubmissionBinding, encoded []byte) error {
		return h.produceTask(ctx, binding, encoded)
	}
	replacing = func(string) {}
)

// resubmitLost resubmits each run of this host's local records that may have ended key-lost and has no produced
// replacement. A run past its task_started_by with no replacement recorded is passed over. Failures are logged; a later
// pass retries them.
func (h *Host) resubmitLost(ctx context.Context) {
	entries, err := os.ReadDir(filepath.Join(h.stateDir, "runs"))
	if err != nil {
		return
	}
	for _, entry := range entries {
		runID, ok := strings.CutSuffix(entry.Name(), ".json")
		if !ok || entry.IsDir() {
			continue
		}
		local, err := h.localRun(runID)
		if err != nil || local == nil || local.Task == nil {
			continue
		}
		if local.Replacement == "" && !time.Now().Before(wire.PastBound(local.TaskStartedBy)) {
			continue
		}
		if local.Replacement != "" {
			if replacement, err := h.localRun(local.Replacement); err == nil && replacement != nil && replacement.Produced {
				continue
			}
		}
		if _, err := h.resubmit(ctx, runID); err != nil && ctx.Err() == nil {
			h.logger.Printf("run %s: resubmit: %v", runID, err)
		}
	}
}

// resubmitIfLost resubmits r's run when it ended key-lost, logging a failure; a later pass retries it.
func (h *Host) resubmitIfLost(ctx context.Context, r *runView) {
	if r.local == nil || r.local.Task == nil || !r.keyLost() {
		return
	}
	if _, err := h.resubmit(ctx, r.run); err != nil && ctx.Err() == nil {
		h.logger.Printf("run %s: resubmit: %v", r.run, err)
	}
}

// resubmit replaces runID, a run of this host that ended key-lost before its task_started_by, and returns its
// replacement, or "" when the run is not to be resubmitted. Under a lock on the run shared by every process using the
// state directory, it produces a stored replacement not yet produced and builds nothing new; otherwise it records the
// replacement's run ID, builds and signs the replacement from the run's template, stores the signed bytes, produces
// them and marks them produced. A replacement whose bytes were not stored is built again.
func (h *Host) resubmit(ctx context.Context, runID string) (string, error) {
	path, ok := h.runPath(runID)
	if !ok {
		return "", fmt.Errorf("resubmit %s: %w", runID, ErrRunNotFound)
	}
	unlock, err := h.lockFile(ctx, filepath.Join(h.stateDir, "resubmit", filepath.Base(path)+".lock"))
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
		r, err := h.history(ctx, runID)
		if err != nil {
			return "", err
		}
		if !r.keyLost() {
			return "", nil
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
