## Delegating tasks with wherry

`wherry` runs a self-contained task on other compute and returns the result.
Use it for long tasks, tasks that need a platform or isolation this machine
lacks, or tasks that can run while you do other work. The remote agent sees
only the prompt, the files you send and, if given, a pushed git ref it
clones itself.

1. `wherry host receivers --json` lists receivers with each plugin profile's `agent`,
   the agent's `models` and `default_model` when it has them, `labels`,
   `ready` and `credentials`, each agent credential's `name`, with
   its target (`env` or `file`), `optional` and `configured`, whether this
   host has a source for it, and a live receiver's `gated`, why it claimed no
   task as of its latest advertisement, when it claimed none. Pick
   an agent and build a selector from the labels.
2. `wherry submit --prompt TEXT` prints `run_id` and `pool`. Add `--agent NAME`
   to choose the agent, `--model NAME` to choose one of its `models` in place
   of its `default_model`, and `--selector` with comma-separated terms
   (`key=v1|v2`, `key!=v`, `key`, `!key`) to require labels. Add
   `--git REMOTE[@REF]` for a repository the agent clones (its host must be one
   of the pool's VCS hosts), pinned without `@REF` to its default branch's
   commit at submit, or `--git .` for the repository you are in, at its
   current commit, which must be on its `origin`; and `--git-changes patch` to
   get its commits back as `changes.patch` (`git am` it). The clone's
   credential and an `ssh` remote's host key come from the `git` entries of
   your wherry configuration. To build
   on a finished run's commits after `wherry result` collected its
   `changes.bundle`, use `--continue RUN_ID` in place of `--git`; the pool needs
   storage. Add
   `--input FILE` for each file the agent needs. Inputs over the broker's
   record limit or a matching profile's limit go through S3 when the pool
   and host have storage, and otherwise submit refuses before publishing.
   Add `--credential NAME:file=FILE` or `--credential NAME:env=VAR` for each
   configured credential it needs as a file or an environment variable.
   Every credential of the agent this host can resolve is delivered at the
   agent's own target, required or optional (`wherry host receivers` marks
   them `optional`), whether or not the task names it; `--credential NAME`
   names an optional one the task cannot go without, so `submit` fails
   when the host cannot resolve it, and the `submit` line's
   `unsupplied=` names the optional credentials the host could not resolve.
   Leaving a credential unnamed does not withhold it: the user withholds
   one, a billing credential such as an API key included, by removing its
   source from their wherry configuration and unsetting its variable where
   `wherry` runs. Add `--output NAME` for each file you want back; the run
   fails without it. Where the pool has storage and your configuration a
   storage credential for it, an `--input` over the inline limits and an
   output over the pool's output limit (5 GiB at most) go through the pool's
   bucket by themselves. With `--git`, the agent's commits come back as
   `changes.bundle` unless `--git-changes` says `patch` or `none`. Keep working.
3. `wherry result RUN_ID` blocks until the run ends (2 hours 10 minutes at
   most by default), then writes each returned output, S3 outputs included,
   prints an `output=<name> path=<path>` line for each, and prints the
   agent's reply last. Its exit status is 0 for
   `TASK_STATE_COMPLETED`, 2 failed, 3 canceled, 4 rejected,
   6 timed out and 1 when it could not wait or collect. Start it in the
   background right after submitting, where your harness reports background command completion: its
   completion is how you learn the run ended.
4. `submit` refuses with `auth required: credential NAME is not in this
   host's configuration` when the host has no source for a credential the
   agent requires or the task names, and names the credential when its
   source fails. A run whose agent needs a credential it lacks ends
   `TASK_STATE_FAILED`, and its message names the credential. The host
   sends every credential it can resolve, so the run went without one the
   host could not resolve when it submitted. When `wherry host receivers`
   now shows the credential without `missing`, a source or variable arrived
   since: submit again with `--credential NAME`, so the new submit fails
   when the host cannot resolve it.
   Otherwise tell the user which credential it names, and to run
   `wherry host init`, which asks for a source for it (a file, an environment
   variable or a command), or to add one under `credentials` in their wherry
   configuration. Never handle the value
   yourself. Then submit the task again and wait for the new run.
5. A run whose pool lost its key before the run started ends
   `TASK_STATE_FAILED` with `cannot be opened: the pool's custodian changed
   and its key was lost`. Before its `task_started_by`, the host that
   submitted it resubmits it under a new run ID.
   `wherry result` follows the new run and prints
   `run <id> lost its pool key before it started; resubmitted as run <id>`
   on standard error. `wherry status RUN_ID` prints `replaced_by=<id>`, and
   the MCP `result` tool lists every run ID it followed in `runs`, the last
   one the run whose result it returns. Use the newest run ID from then on,
   and do not submit the task again yourself.
6. `submit` refuses a pool that takes no task now, with an error that says `pool <pool> is
   suspended; submit again once it is resumed` or `pool <pool> has no
   custodian yet; submit again once a receiver of the pool has published
   one`. Tell the user about a suspended
   pool, since its operator resumes it. Otherwise submit again after a few
   minutes.
7. `wherry cancel RUN_ID` when the result is no longer needed.
   A run takes no new input after `submit`: for a requirement that arrives
   mid-run, cancel and submit again with it, or let the run finish and
   submit a follow-up with its outputs as inputs.
8. `wherry status RUN_ID` prints a run's state without waiting.
   `wherry status` alone lists every run this host can see, latest update
   first. `wherry logs RUN_ID` prints the agent's output lines, and
   `wherry logs --follow RUN_ID` prints them as the agent runs until the run ends.

Where the `wherry` MCP server (`wherry host mcp`) is configured, its tools do the
same: `receivers`, `submit`, `status`, `result`, `cancel`, `events`
and `logs`. Its `submit` returns `command`,
the `wherry result` command to start in the background. Its `result` tool
waits up to 45 seconds; call it again while the reply carries `retry_after`. Its `logs`
returns a page of lines; call it again with `next_offset` while it is set.

For a short run, use `wherry submit --wait` to submit and collect in one
command, which exits with `wherry result`'s exit codes, with `--outputs-dir DIR` when needed. In MCP, set submit's `wait` to
`true` only when the run is expected to finish within 45 seconds. Read its
nested `result`, or its sanitised `error` after publish; `command` collects
later. For longer runs submit first and call `result` separately.
