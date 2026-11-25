package client

import (
	"context"
	"os"
	"strings"
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
	// NotFetchedRefused: git refused the fetch: a missing prerequisite, a corrupt object, the branch checked out.
	NotFetchedRefused = "git refused"
	// NotFetchedUnreadable: the bundle's header does not read as a bundle of one prerequisite and a HEAD.
	NotFetchedUnreadable = "unreadable bundle"
)

// fetchRef is the branch a run's bundle is fetched into, wherry/<run>.
func fetchRef(runID string) string {
	return "wherry/" + runID
}

// fetchBundle fetches bundle, the absolute path of runID's collected changes.bundle, into refs/heads/wherry/<runID> of
// repo, the repository recorded for the run, and returns the ref, or the reason it was not fetched. The ref is created
// when absent and left alone when present: at the bundle's head there is nothing to do, at any other commit, an
// ancestor of the head included, it is not moved. The fetch imports the named ref alone, checks every object it
// receives whatever the repository's own fetch.fsckObjects says, and touches neither the working tree, the index nor
// HEAD. Nothing here fails the collect.
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
		return "", NotFetchedNoRepository
	}
	if _, err := runner.run(repo, "-c", "fetch.fsckObjects=true", "-c", "maintenance.auto=false", "fetch", "--no-tags", "--no-recurse-submodules", bundle, "HEAD:"+full); err != nil {
		return "", NotFetchedRefused
	}
	return ref, ""
}
