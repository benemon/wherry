package client

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/benemon/wherry/internal/wire"
)

// baseBundle is the input a continuation delivers its previous run's changes.bundle as.
const baseBundle = "base.bundle"

// maxBundleHeader bounds the bundle header a continuation reads.
const maxBundleHeader = 64 << 10

// collectedBundle is the changes.bundle Collect wrote for a run: its absolute path and the size and SHA-256 it verified.
type collectedBundle struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// continuation is what a run takes from the run it continues: the last run of its chain, that run's collected bundle
// opened, and the bundle's base commit.
type continuation struct {
	last   *runRecord
	bundle *os.File
	commit string
}

// continued resolves runID, a run this host submitted under its tenant, to the last run of its resubmission chain and
// opens that run's collected changes.bundle. Its refusals name the run and the rule, never a path.
func (h *Host) continued(runID string) (continuation, error) {
	chain, err := h.chain(runID)
	if err != nil {
		return continuation{}, fmt.Errorf("%w: continue: the local records of run %s's chain cannot be read", ErrInvalidInput, runID)
	}
	var local *runRecord
	for _, run := range []string{runID, chain[len(chain)-1]} {
		if local, err = h.localRun(run); err != nil {
			return continuation{}, fmt.Errorf("%w: continue: the local record of run %s cannot be read", ErrInvalidInput, run)
		}
		if local == nil || local.Task == nil || local.Run != run || local.Task.Wherry.Run != run || local.Task.Wherry.Tenant != h.tenant ||
			local.Task.Wherry.Host != h.host || wire.CheckRunID(local.Task.Wherry) != nil {
			return continuation{}, fmt.Errorf("%w: continue: run %s is not a run this host submitted under its tenant", ErrInvalidInput, run)
		}
	}
	if local.Task.Wherry.Git == nil {
		return continuation{}, fmt.Errorf("%w: continue: run %s has no git input", ErrInvalidInput, local.Run)
	}
	if local.Bundle == nil {
		return continuation{}, fmt.Errorf("%w: continue: wait has collected no %s of run %s", ErrInvalidInput, wire.GitBundle, local.Run)
	}
	// A FIFO would block Open.
	info, err := os.Lstat(local.Bundle.Path)
	if err != nil || !info.Mode().IsRegular() {
		return continuation{}, fmt.Errorf("%w: continue: the %s collected for run %s cannot be read", ErrInvalidInput, wire.GitBundle, local.Run)
	}
	bundle, err := os.Open(local.Bundle.Path)
	if err != nil {
		return continuation{}, fmt.Errorf("%w: continue: the %s collected for run %s cannot be read", ErrInvalidInput, wire.GitBundle, local.Run)
	}
	commit, err := bundleBase(bundle)
	if err != nil {
		bundle.Close()
		return continuation{}, fmt.Errorf("%w: continue: the %s collected for run %s %v", ErrInvalidInput, wire.GitBundle, local.Run, err)
	}
	return continuation{last: local, bundle: bundle, commit: commit}, nil
}

// bundleBase reads a v2 or v3 git bundle's header and returns its one prerequisite, which its HEAD ref builds on.
func bundleBase(bundle io.Reader) (string, error) {
	scanner := bufio.NewScanner(io.LimitReader(bundle, maxBundleHeader))
	scanner.Buffer(nil, maxBundleHeader)
	var prerequisites []string
	var head string
	for first := true; scanner.Scan(); first = false {
		line := scanner.Text()
		switch {
		case first:
			if line != "# v2 git bundle" && line != "# v3 git bundle" {
				return "", errors.New("is not a v2 or v3 git bundle")
			}
		case line == "":
			if len(prerequisites) != 1 || head == "" || head == prerequisites[0] {
				return "", errors.New("needs exactly one prerequisite and a HEAD ref other than it")
			}
			return prerequisites[0], nil
		case strings.HasPrefix(line, "@"):
		case strings.HasPrefix(line, "-"):
			commit, _, _ := strings.Cut(line[1:], " ")
			if !wire.ValidCommit(commit) {
				return "", errors.New("has a malformed prerequisite")
			}
			prerequisites = append(prerequisites, commit)
		default:
			oid, ref, _ := strings.Cut(line, " ")
			if ref == "HEAD" {
				if !wire.ValidCommit(oid) {
					return "", errors.New("has a malformed HEAD ref")
				}
				head = oid
			}
		}
	}
	return "", errors.New("has no header end within 64 KiB")
}

// recordBundle records the changes.bundle among outputs in the local record of the last of runs, holding its lock and
// that of the run it replaces, under which a replacement's record is written. Without one the record keeps the bundle
// it holds.
func (h *Host) recordBundle(ctx context.Context, runs []string, outputs []Output) error {
	i := slices.IndexFunc(outputs, func(output Output) bool { return output.Name == wire.GitBundle })
	if i < 0 {
		return nil
	}
	path, err := filepath.Abs(outputs[i].Path)
	if err != nil {
		return err
	}
	for _, run := range runs[max(len(runs)-2, 0):] {
		unlock, err := h.lockRun(ctx, run)
		if err != nil {
			return err
		}
		defer unlock()
	}
	local, err := h.localRun(runs[len(runs)-1])
	if err != nil || local == nil {
		return err
	}
	local.Bundle = &collectedBundle{Path: path, Size: outputs[i].Size, SHA256: outputs[i].SHA256}
	return h.writeRun(*local)
}
