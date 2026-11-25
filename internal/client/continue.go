package client

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"

	"github.com/benemon/wherry/internal/wire"
)

// baseBundle is the input a continuation delivers its previous run's changes.bundle as.
const baseBundle = "base.bundle"

// previousPrefix is the directory a non-git continuation delivers its previous run's collected outputs under; no
// caller's input is named beneath it.
const previousPrefix = "previous/"

// maxBundleHeader bounds the bundle header a continuation reads.
const maxBundleHeader = 64 << 10

// collectedOutput is an output Collect wrote for a run: its name, its absolute path, and the size and SHA-256 Collect
// verified.
type collectedOutput struct {
	Name   string `json:"name"`
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// matches reports whether data, of size bytes, is what Collect verified.
func (o collectedOutput) matches(size int64, data []byte) bool {
	sum := sha256.Sum256(data)
	return size == o.Size && hex.EncodeToString(sum[:]) == o.SHA256
}

// previousFile is output, collected for run, as the input previous/<name> of a run continuing it. Its refusals name the
// output and the run, never the path.
func previousFile(output collectedOutput, run string) sendFile {
	return sendFile{InputFile: InputFile{Name: previousPrefix + output.Name, Open: func() (io.ReadSeekCloser, error) {
		file, err := openOutput(output.Path)
		if err != nil {
			return nil, fmt.Errorf("the output %s collected for run %s cannot be read", output.Name, run)
		}
		return file, nil
	}}, recorded: &output}
}

// changedOutput refuses a continuation whose bytes of output, collected for run, no longer hash to what result verified.
func changedOutput(output, run string) error {
	return fmt.Errorf("%w: continue: the %s collected for run %s changed after result verified it", ErrInvalidInput, output, run)
}

// continuation is what a run takes from the run it continues: the last run of its chain and, for a run with a git
// input, that run's collected bundle opened and the bundle's base commit.
type continuation struct {
	last   *runRecord
	bundle io.ReadSeekCloser
	commit string
}

// continued resolves runID, a run this host submitted under its tenant, to the last run of its resubmission chain and,
// for a run with a git input, opens that run's collected changes.bundle; a run without one must have collected outputs.
// Its refusals name the run and the rule, never a path.
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
		if len(local.Outputs) == 0 {
			return continuation{}, fmt.Errorf("%w: continue: run %s has no collected outputs; run wherry result %s first", ErrInvalidInput, local.Run, local.Run)
		}
		return continuation{last: local}, nil
	}
	i := slices.IndexFunc(local.Outputs, func(output collectedOutput) bool { return output.Name == wire.GitBundle })
	if i < 0 {
		return continuation{}, fmt.Errorf("%w: continue: result has collected no %s of run %s", ErrInvalidInput, wire.GitBundle, local.Run)
	}
	bundle, err := openOutput(local.Outputs[i].Path)
	if err != nil {
		return continuation{}, fmt.Errorf("%w: continue: the %s collected for run %s cannot be read", ErrInvalidInput, wire.GitBundle, local.Run)
	}
	commit, _, err := bundleHeader(bundle)
	if err != nil {
		bundle.Close()
		return continuation{}, fmt.Errorf("%w: continue: the %s collected for run %s %v", ErrInvalidInput, wire.GitBundle, local.Run, err)
	}
	return continuation{last: local, bundle: bundle, commit: commit}, nil
}

// openOutput opens a collected output as the host: through an os.Root at its recorded directory, without blocking, and
// as a regular file alone, so neither a FIFO nor a link swapped in since result wrote it is read. A variable so tests
// count what submit opens and reads.
var openOutput = func(path string) (io.ReadSeekCloser, error) {
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	defer root.Close()
	file, err := root.OpenFile(filepath.Base(path), os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		file.Close()
		return nil, errors.New("not a regular file")
	}
	return file, nil
}

// bundleHeader reads a v2 or v3 git bundle's header and returns its one prerequisite and the commit its HEAD ref
// names, which builds on the prerequisite.
func bundleHeader(bundle io.Reader) (base, head string, err error) {
	scanner := bufio.NewScanner(io.LimitReader(bundle, maxBundleHeader))
	scanner.Buffer(nil, maxBundleHeader)
	var prerequisites []string
	for first := true; scanner.Scan(); first = false {
		line := scanner.Text()
		switch {
		case first:
			if line != "# v2 git bundle" && line != "# v3 git bundle" {
				return "", "", errors.New("is not a v2 or v3 git bundle")
			}
		case line == "":
			if len(prerequisites) != 1 || head == "" || head == prerequisites[0] {
				return "", "", errors.New("needs exactly one prerequisite and a HEAD ref other than it")
			}
			return prerequisites[0], head, nil
		case strings.HasPrefix(line, "@"):
		case strings.HasPrefix(line, "-"):
			commit, _, _ := strings.Cut(line[1:], " ")
			if !wire.ValidCommit(commit) {
				return "", "", errors.New("has a malformed prerequisite")
			}
			prerequisites = append(prerequisites, commit)
		default:
			oid, ref, _ := strings.Cut(line, " ")
			if ref == "HEAD" {
				if !wire.ValidCommit(oid) {
					return "", "", errors.New("has a malformed HEAD ref")
				}
				head = oid
			}
		}
	}
	return "", "", errors.New("has no header end within 64 KiB")
}

// lockRuns takes the lock of each of runs, in order, and returns the release of all of them.
func (h *Host) lockRuns(ctx context.Context, runs []string) (func(), error) {
	var unlocks []func()
	release := func() {
		for i := len(unlocks) - 1; i >= 0; i-- {
			unlocks[i]()
		}
	}
	for _, run := range runs {
		unlock, err := h.lockRun(ctx, run)
		if err != nil {
			release()
			return nil, err
		}
		unlocks = append(unlocks, unlock)
	}
	return release, nil
}

// recordOutputs records outputs, the collected outputs at their absolute paths, in the local record of the last of
// runs, merged by name into what it held: an output replaces the entry of its name and the entries this collect did not
// verify stay, since the run's S3 prefix is deleted once a collect verifies every output and a later collect into
// another directory gets the inline outputs alone. It returns the record, nil for a run this host has no record of. The
// caller holds that run's lock and that of the run it replaces, under which a replacement's record is written.
func (h *Host) recordOutputs(runs []string, outputs []collectedOutput) (*runRecord, error) {
	local, err := h.localRun(runs[len(runs)-1])
	if err != nil || local == nil {
		return nil, err
	}
	merged := slices.Clone(local.Outputs)
	for _, output := range outputs {
		if i := slices.IndexFunc(merged, func(kept collectedOutput) bool { return kept.Name == output.Name }); i >= 0 {
			merged[i] = output
		} else {
			merged = append(merged, output)
		}
	}
	local.Outputs = merged
	return local, h.writeRun(*local)
}
