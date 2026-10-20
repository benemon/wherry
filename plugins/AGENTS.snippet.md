## Delegating tasks with wherry

`wherry` runs a self-contained task on other compute and returns the result.
Use it for long tasks, tasks that need a platform or isolation this machine
lacks, or tasks that can run while you do other work. The remote agent sees
only the prompt, the files you send and, if given, a pushed git ref it
clones itself.

1. `wherry receivers --json` lists receivers with each adapter's `agent`,
   `labels` and `ready`. Pick an agent and build a selector from the labels.
2. `wherry submit --prompt TEXT` prints `run_id` and `pool`. Add `--agent NAME`
   to choose the agent, and `--selector` with comma-separated terms
   (`key=v1|v2`, `key!=v`, `key`, `!key`) to require labels. Add
   `--git REMOTE@REF` for a repository the agent clones (its host must be one of
   the pool's VCS hosts), with `--git-credential NAME` naming a
   `--credential-file`
   credential to clone with, `--git-host-key FILE` (`known_hosts` lines naming
   the host) for an `ssh` remote, and `--git-changes patch` to get its commits
   back as `changes.patch` (`git am` it). Add `--input FILE` for each file the
   agent needs (the pool's inline limit in all, 256 KiB by default),
   `--credential-file NAME=FILE` for each configured credential it needs as a
   file, and `--output NAME[:required]` for each file you want back. Where the
   pool has storage and your configuration a storage credential,
   `--s3-input FILE` sends a larger file (5 GiB at most) through the pool's
   bucket, and `--output NAME:s3` returns one that way. Keep working.
3. `wherry await RUN_ID` blocks until the run ends (30 minutes at most by
   default), then prints the state and the result, and writes each returned
   output, S3 outputs included, printing an `output=<name> path=<path>` line for
   it. It also answers the receiver's request for the agent's credential, so
   start it within 5 minutes of submitting. Run it in the background where your
   harness reports background command completion.
4. On `TASK_STATE_AUTH_REQUIRED`, tell the user which credential the
   message names, and to add a file holding it to `credentials` in their
   wherry configuration. Never handle the value yourself. Then run
   `wherry continue RUN_ID` and await again.
5. `wherry cancel RUN_ID` when the result is no longer needed.
6. `wherry status RUN_ID` prints a run's state without waiting.
   `wherry status` alone lists every run this host can see, latest update
   first.

Where the `wherry` MCP server (`wherry mcp`) is configured, its tools do the
same: `receivers`, `submit`, `status`, `await`, `continue`, `cancel` and
`events`. Its `await` waits up to 45 seconds; call it again while the
reply carries `retry_after`.
