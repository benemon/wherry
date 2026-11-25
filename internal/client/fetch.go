package client

import (
	"context"
	"errors"
	"os"
	"strings"
	"time"
)

// Why a collected changes.bundle was not fetched into its run's repository. Each is a fixed word: git's own output can
// quote the repository's paths and remotes, so none of it is carried.
const (
	// NotFetchedExists: refs/heads/wherry/<run> names another commit, which is left as it is.
	NotFetchedExists = "exists"
	// NotFetchedNoGit: git is not on PATH, or is older than --git . needs.
	NotFetchedNoGit = "no git"
	// NotFetchedNoRepository: the recorded repository is gone or is no longer a repository.
	NotFetchedNoRepository = "no repository"
	// NotFetchedRefused: git refused the fetch: a missing prerequisite, a corrupt object, a path the repository's
	// configuration rewrote to somewhere the bundle's head is not.
	NotFetchedRefused = "git refused"
	// NotFetchedUnreadable: the bundle's header does not read as a bundle of one prerequisite and a HEAD.
	NotFetchedUnreadable = "unreadable bundle"
	// NotFetchedWrongCommit: the branch the fetch made named a commit other than the bundle's head, and was deleted.
	NotFetchedWrongCommit = "wrong commit"
	// NotFetchedTimeout: the fetch, its checks before and after included, did not end within fetchTimeout.
	NotFetchedTimeout = "timeout"
)

// fetchTimeout bounds a fetch, the checks before and after it included, through the runner's context; a variable so
// tests can shorten it.
var fetchTimeout = 10 * time.Minute

// fetchRef is the branch a run's bundle is fetched into, wherry/<run>.
func fetchRef(runID string) string {
	return "wherry/" + runID
}

// fetchBundle fetches bundle, the absolute path of runID's collected changes.bundle, into refs/heads/wherry/<runID> of
// repo, the repository recorded for the run, and returns the ref, or the reason it was not fetched. The ref is created
// when absent and left alone when present: at the bundle's head there is nothing to do, at any other commit, an
// ancestor of the head included, it is not moved. The fetch imports the one commit the bundle's header names, so the
// branch takes that commit or nothing, wherever the repository's configuration sends the path; afterwards the branch is
// read back and must name it. The fetch turns fetch.fsckObjects on over a repository's fetch.fsckObjects=false; the
// check honours the repository's own fetch.fsck.<msg-id> and fetch.fsck.skipList exceptions, which no command line
// setting covers. The working tree, the index and HEAD are not touched. The caller holds the run's lock, so two
// collects of one run do not interleave these steps. Nothing here fails the collect.
func fetchBundle(ctx context.Context, repo, runID, bundle string) (ref, reason string) {
	file, err := os.Open(bundle)
	if err != nil {
		return "", NotFetchedUnreadable
	}
	_, head, err := bundleHeader(file)
	file.Close()
	if err != nil {
		return "", NotFetchedUnreadable
	}
	ctx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()
	runner, err := recordedGit(ctx, repo)
	if err != nil {
		return "", NotFetchedNoGit
	}
	defer runner.close()
	ref = fetchRef(runID)
	full := "refs/heads/" + ref
	switch current, err := runner.run(repo, "rev-parse", "--verify", "--quiet", full); {
	case err == nil && strings.TrimSpace(current) == head:
		return ref, ""
	case err == nil:
		return "", NotFetchedExists
	case !exited(err, 1):
		return "", timedOut(ctx, NotFetchedNoRepository)
	}
	if _, err := runner.run(repo, "-c", "fetch.fsckObjects=true", "-c", "maintenance.auto=false", "fetch", "--no-tags", "--no-recurse-submodules", bundle, head+":"+full); err != nil {
		return "", timedOut(ctx, NotFetchedRefused)
	}
	// The pre-check found no branch, so the fetch made this one; at anything but the head it goes, guarded by the
	// commit it was read at.
	got, err := runner.run(repo, "rev-parse", "--verify", "--quiet", full)
	if err == nil && strings.TrimSpace(got) == head {
		return ref, ""
	}
	if err == nil {
		runner.run(repo, "update-ref", "-d", full, strings.TrimSpace(got))
	}
	return "", timedOut(ctx, NotFetchedWrongCommit)
}

// timedOut is NotFetchedTimeout once ctx's deadline has passed, else reason.
func timedOut(ctx context.Context, reason string) string {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return NotFetchedTimeout
	}
	return reason
}
