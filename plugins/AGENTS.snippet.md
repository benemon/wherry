## Delegating tasks with wherry

`wherry` runs a self-contained task on other compute and returns the result.
Use it for long tasks, tasks that need a platform or isolation this machine
lacks, or tasks that can run while you do other work. The remote agent sees
only the prompt, the files you send and, if given, a pushed git ref it
clones itself.

1. `wherry receivers --json` lists receivers with each plugin profile's `agent`,
   `labels`, `ready` and `credentials`, each agent credential's `name`, with
   `optional` and `configured`, whether this host has a source for it. Pick
   an agent and build a selector from the labels.
2. `wherry submit --prompt TEXT` prints `run_id` and `pool`. Add `--agent NAME`
   to choose the agent, and `--selector` with comma-separated terms
   (`key=v1|v2`, `key!=v`, `key`, `!key`) to require labels. Add
   `--git REMOTE@REF` for a repository the agent clones (its host must be one of
   the pool's VCS hosts), and `--git-changes patch` to get its commits back as
   `changes.patch` (`git am` it). The clone's credential and an `ssh` remote's
   host key come from the `git` entries of your wherry configuration. Add `--input FILE` for each file the
   agent needs (inputs that would take the task over the broker's record limit
   or a matching profile's limit go through S3 when the pool and host have
   storage; otherwise submit refuses before publishing),
   `--credential NAME:file=FILE` or `--credential NAME:env=VAR` for each
   configured credential it needs as a file or an environment variable, and `--output NAME[:required]` for each file you want back. Where the
   pool has storage and your configuration a storage credential,
   `--input FILE:s3` sends a larger file (5 GiB at most) through the pool's
   bucket, and `--output NAME:s3` returns one that way. Keep working.
3. `wherry wait RUN_ID` blocks until the run ends (2 hours 10 minutes at
   most by default), then prints the agent's reply and an
   `output=<name> path=<path>` line for each output, and writes each returned
   output, S3 outputs included. Its exit status is 0 for
   `TASK_STATE_COMPLETED`, 2 failed, 3 canceled, 4 rejected, 5 auth required,
   6 timed out and 1 when it could not wait or collect. It also answers the receiver's
   request for the run's credentials, so start it within 5 minutes of
   submitting, and do not stop it while that request is open: unanswered, the
   run ends `TASK_STATE_FAILED`. Start it in the background right after
   submitting, where your harness reports background command completion: its
   completion is how you learn the run ended.
4. `submit` refuses with `auth required: credential NAME is not in this
   host's configuration` when the host has no source for a credential the
   agent requires or the task names. A run whose credential request goes
   unanswered for 5 minutes ends `TASK_STATE_FAILED`, and a run whose agent
   needs a credential it lacks ends `TASK_STATE_AUTH_REQUIRED`; each message
   names the credential. Tell the user which credential it names, and to add
   a source for it (a file, an environment variable or a command) under
   `credentials` in their wherry configuration. Never handle the value
   yourself. Then submit the task again and wait for the new run.
5. `wherry cancel RUN_ID` when the result is no longer needed.
6. `wherry status RUN_ID` prints a run's state without waiting.
   `wherry status` alone lists every run this host can see, latest update
   first. `wherry logs RUN_ID` prints the agent's output lines, and
   `wherry logs -f RUN_ID` prints them as the agent runs until the run ends.
7. `wherry cleanup RUN_ID` deletes the S3 objects of a run that ended while
   no `wait` ran.

Where the `wherry` MCP server (`wherry mcp`) is configured, its tools do the
same: `receivers`, `submit`, `status`, `wait`, `cancel`, `events`,
`logs` and `cleanup`. Its `submit` returns `wait`,
the `wherry wait` command to start in the background. Its `wait` tool
waits up to 45 seconds; call it again while the reply carries `retry_after`. Its `logs`
returns a page of lines; call it again with `next_offset` while it is set.
