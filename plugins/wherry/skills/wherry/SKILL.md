---
name: wherry
description: >-
  Run a subagent task on other compute through wherry and collect its
  result. Use when a self-contained task is long, needs a platform or
  isolation this machine lacks, or can run in parallel with your own work.
---

# wherry

wherry sends a task to a receiver on other compute. The receiver runs an
agent on the task and returns the result over Kafka. The tools come from the
`wherry` MCP server (`wherry mcp`). Every tool has a `wherry` CLI command
of the same name.

## When to delegate

Delegate a task when all of these hold:

- The prompt can stand on its own. The remote agent sees the prompt, the
  input files you pass and, if you pass one, a pushed git ref it clones
  itself. It does not see this session, your other files or uncommitted
  changes.
- The task takes minutes or more, or needs something this machine does not
  have (an OS, architecture, isolation level or network).
- You have other work to do while it runs, or several such tasks to run
  at once.

Do short or interactive work yourself.

## Choose a placement

Call `receivers` first. For each receiver it returns:

- `adapters`: each with the `agent` it runs, its `labels`, and `ready`,
  whether the receiver's live advertisement offers it now. A live receiver
  with no ready adapter lists every adapter with `ready` false, and no task
  is placed on it.
- `status` (`live`, `stale`, `silent` or `ended`), `capacity` and
  `in_flight`: whether it is live and has room.

Pick an `agent` from the adapters, or omit it to take the pool's default.
Build `selector` from the labels of the adapters you want: a list of
`{key, operator, values}` where `operator` is `In` or `NotIn` (with
`values`), or `Exists` or `DoesNotExist` (without). Every requirement must
hold. Omit `selector` when any adapter will do. A malformed key or value
returns `INVALID_ARGUMENT`; a selector no live adapter satisfies returns
`NO_MATCHING_RECEIVER`.

Labels are the operator's description of each adapter. wherry matches them
literally and interprets only the agent name.

An adapter whose agent lists `models` runs `default_model` unless you pass
`model`, one of those `models`, to `submit`; an agent with one model runs
it, and with two or more and no `default_model` you must pass one. A model outside them, or none where one is required, returns
`INVALID_ARGUMENT`. `await` returns the `model` asked for and, where the
agent reports them, the `served_models` that ran it.

## Submit, then keep working

Call `submit` with `prompt`, and `agent`, `selector` and `git` when you
need them. For a long brief, write it to a file and pass its path as
`prompt_file` instead of `prompt`; the server reads it as it reads `inputs`,
and refuses an empty one.
It returns `id`, `pool` and `await` straight away. `await` is a shell
command, `wherry await <id>`. Where your harness runs shell commands in the
background and tells you when they finish, start that command in the
background now, before anything else: it blocks until the run ends (or for 2
hours 10 minutes; `--timeout` changes that), prints the state and the result, writes
returned outputs, answers the receiver's credential request, and exits
non-zero for any state other than `TASK_STATE_COMPLETED`, and at once when
the queue deadline passes with no receiver claiming the run. Its completion is
how you learn the run ended, so you skip the await loop below. Then carry on
with other work.

`git` names a repository the agent clones itself: `remote` (an `https` or `ssh`
URL, or `user@host:path`) and `ref` are required. Add `credential`, the name of
one of your `credentials`, for a private repository; `host_key`, a file of
`known_hosts` lines naming the host (or `[host]:port`), for an `ssh` remote; and
`changes`, `patch` or `bundle`, to get its commits back as the output
`changes.patch` or `changes.bundle`, which you apply with `git am` or
`git fetch`. The remote's host must be one of the pool's VCS hosts, or
`submit` returns `INVALID_ARGUMENT` naming the host and the pool. The agent
pushes only when your prompt asks it to, and only to the branch
`wherry/<run>`.

To send files, add `inputs`: paths of regular files beneath one of the wherry
MCP server's input roots (the directory it runs in, unless the user's wherry
configuration lists `input_roots`), the pool's inline limit in all (256 KiB by
default), which the agent finds under their base names in its input directory.
An adapter can declare a lower limit. When the chosen pool has storage and the
host has its storage credential, wherry automatically carries all inline inputs
through S3 when they exceed the smallest matching adapter limit; otherwise
submit refuses before publishing and names that limit.
The server refuses any other path, and the host's state directory, host key,
bundle, credential files and storage key files. To give the agent a credential
as a file, add `credentials`: `{name, file}` with `name` a credential in the
user's wherry configuration and `file` the name it gets in
`WHERRY_CREDENTIALS_DIR`; you never see the value. To get files back, add
`outputs`: `{name, required}` for each file the agent should leave in
`WHERRY_OUTPUT_DIR`. The prompt the agent receives names all of them.

For a file over the pool inline limit, up to 5 GiB, add its path to `s3_inputs`
instead: the server uploads it to the pool's bucket and the agent finds it
in its input directory like any input. Add `storage: "s3"` to an output to
return it through the bucket. Both need a pool with storage and a storage
credential in the user's wherry configuration; without them `submit`
returns `INVALID_ARGUMENT` saying which is missing. `await` downloads S3
outputs like any output, and deletes the run's objects from the bucket
once the run has ended and every one has verified. A run that ended while
nothing awaited it keeps them until you call `cleanup` with its id;
`cleanup` refuses a run that has not ended.

## Collect the result

Without a background command, call the `await` tool with the id, and call it
again until the run ends; nothing tells you when it does. It waits up to 45
seconds and returns one of:

- `TASK_STATE_COMPLETED` with `artifacts`: the result. `outputs` lists
  each returned file with the local `path` it was written to and its
  `sha256`; read the files there. `not_returned` lists each expected file
  that did not come back, with the reason.
- `TASK_STATE_FAILED` with `artifacts` holding the agent's result when the
  agent reported the failure, or a `message` when the receiver could not
  run it, a required output did not come back, or a credential went
  unanswered (see the next section).
- `TASK_STATE_REJECTED` or `TASK_STATE_CANCELED` with a `message` that
  says why.
- `TASK_STATE_SUBMITTED` with the `message`
  `queue deadline passed at <time> and no receiver claimed the run` and no
  `retry_after`: no receiver took the run before its queue deadline and none
  will. Check `receivers` for a live one that fits, then `submit` again.
- `retry_after` with the current `state`: the run has not finished. Wait
  that many seconds, or do other work, then call `await` again.

`status` with the id returns the run's latest state without waiting. Without
an id it lists every run this host can see, latest update first, each with
its `pool`, `state` and `updated` time; use it to find a run whose
id you no longer hold. `events` returns the run's status history.

`transcript` with the id returns the agent's standard output as the
receiver redacted it: up to 100 lines from `offset`,
and `next_offset` while more remain. Call it again with `next_offset` to read
on. Use it to follow a long run or to see why one failed. Each line names the
delivery attempt that published it; a run another receiver took over reads
from the start, the earlier attempt's lines first. A page stays within 24 KiB,
which fits a harness's default limit on tool output, such as Claude Code's
`MAX_MCP_OUTPUT_TOKENS`. A line too long for a page comes back as its start,
with `truncated` set and its full size in `bytes`.

## A run that needs a credential

The receiver that claims a task asks this host for the credential the
agent needs, and the wherry MCP server answers while it runs, as `wherry
await` does. When no answer arrives within 5 minutes, usually because the
credential is not configured, the run ends `TASK_STATE_FAILED` with a
`message` that names the credential and never holds its value. Tell the user
to put the credential in a file and add it under that name to `credentials`
in their wherry configuration. Never ask for the value or handle it
yourself. When the user says it is in place, call `submit` again with the
same input, and start its `await` as before.

## Cancel unneeded work

When you no longer need a run's result, call `cancel` with its id. The run
ends in `TASK_STATE_CANCELED`.

## Errors

A failed call returns `{"code": ..., "message": ...}`. `RUN_NOT_FOUND`,
`NO_MATCHING_RECEIVER` and `INVALID_ARGUMENT` call for a change to your input or
end the run's handling; a queue deadline longer than the pool's
`max_queue_deadline` is an `INVALID_ARGUMENT`, so ask for a shorter one. `CANCELLED` means the call itself was
cancelled. `VERSION_SKEW` means the pool's records come from a later wherry
release than the running MCP server: tell the user to upgrade `wherry` and
restart the server, since a harness keeps the old process running. `UNAVAILABLE` is any other failure; the user can run the same
operation with the `wherry` CLI to see the cause. `OUTPUTS_NOT_WRITTEN` means
the result arrived but its outputs could not be written, downloaded or cleaned
up; the `wherry await` CLI reports the cause.
