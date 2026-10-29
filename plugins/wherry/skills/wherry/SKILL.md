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

- `profiles`: each with its `plugin`, the `agent` it runs, its `labels`, `credentials`,
  the agent's credentials by `name` with `optional` and `configured`,
  whether this host has a source for each, and `ready`, whether the
  receiver's live advertisement offers it now. A live receiver
  with no ready plugin profile lists every profile with `ready` false, and no task
  is placed on it.
- `status` (`live`, `stale`, `silent` or `ended`), `capacity` and
  `in_flight`: whether it is live and has room.

Pick an `agent` from the profiles, or omit it to take the pool's default
agent, or the first other agent in the pool's terms that a ready profile
runs when none runs the default.
Build `selector` from the labels of the profiles you want: a list of
`{key, operator, values}` where `operator` is `In` or `NotIn` (with
`values`), or `Exists` or `DoesNotExist` (without). Every requirement must
hold. Omit `selector` when any profile will do. A malformed key or value
returns `INVALID_ARGUMENT`; a selector no live profile satisfies returns
`NO_MATCHING_RECEIVER`.

Labels are the operator's description of each profile. wherry matches them
literally and interprets only the agent name.

A profile whose agent lists `models` runs `default_model` unless you pass
`model`, one of those `models`, to `submit`; an agent with one model runs
it, and with two or more and no `default_model` you must pass one. A model outside them, or none where one is required, returns
`INVALID_ARGUMENT`. `wait` returns the `model` asked for.

## Submit, then keep working

Call `submit` with `prompt`, and `agent`, `selector` and `git` when you
need them. For a long brief, write it to a file and pass its path as
`prompt_file` instead of `prompt`; the server reads it as it reads `inputs`,
and refuses an empty one.
It returns `id`, `pool` and `wait` straight away. `wait` is a shell
command, `wherry wait <id>`. Where your harness runs shell
commands in the background and tells you when they finish, start that command
in the background now, before anything else: it blocks until the run ends (or
for 2 hours 10 minutes; `--timeout` changes that), prints the agent's reply
and a line for each returned output, writes the outputs, answers the
receiver's credential request, and exits 0 for `TASK_STATE_COMPLETED`, 2
failed, 3 canceled, 4 rejected, 5 auth required, 6 timed out, and 1 when it
could not wait or collect, at once when the queue deadline passes with no
receiver claiming the run. Its completion is how you learn the run ended, so
you skip the `wait` tool loop below. Then carry on with other work. Do not stop
the command while the run waits for a credential: an unanswered credential
request ends the run `TASK_STATE_FAILED` after 5 minutes, unless the wherry
MCP server answers it.

`git` names a repository the agent clones itself: `remote` (an `https` or `ssh`
URL, or `user@host:path`) and `ref` are required. The clone's credential, and an
`ssh` remote's host key, come from the `git` entry of the user's wherry
configuration whose `url` is the longest prefix of the remote; a remote under no
entry clones anonymously, and an `ssh` one then returns `INVALID_ARGUMENT`. Add
`changes`, `patch` or `bundle`, to get its commits back as the output
`changes.patch` or `changes.bundle`, which you apply with `git am` or
`git fetch`. The remote's host must be one of the pool's VCS hosts, or
`submit` returns `INVALID_ARGUMENT` naming the host and the pool. The agent
pushes only when your prompt asks it to, and only to the branch
`wherry/<run>`.

To send files, add `inputs`: `{path}` for each regular file beneath one of the wherry
MCP server's input roots (the directory it runs in, unless the user's wherry
configuration lists `input_roots`), which the agent finds under their base
names in its input directory. Inputs travel inside the task's broker record up
to the broker's record limit and the smallest matching plugin profile's limit. When
they would exceed either and the chosen pool has storage and the host its
storage credential, wherry automatically carries them through S3; otherwise
submit refuses before publishing and names the limit.
The server refuses any other path, and the host's state directory, host key,
bundle and every file a credential source names, including the storage keys' files. To give the agent a credential,
add `credentials`: `{name, file}` or `{name, env}` with `name` a credential in
the user's wherry configuration and `file` the name it gets in
`WHERRY_CREDENTIALS_DIR`, or `env` the environment variable it gets; you never
see the value. Two credentials cannot share a target, nor can one take the
target of an agent's own credential. To get files back, add
`outputs`: `{name, required}` for each file the agent should leave in
`WHERRY_OUTPUT_DIR`. The prompt the agent receives names all of them.

To send a large file through S3 directly, up to 5 GiB, add `storage: "s3"`
to its input: the server uploads it to the pool's bucket and the agent finds it
in its input directory like any input. Add `storage: "s3"` to an output to
return it through the bucket. Both need a pool with storage and a storage
credential in the user's wherry configuration; without them `submit`
returns `INVALID_ARGUMENT` saying which is missing. `wait` downloads S3
outputs like any output, and deletes the run's objects from the bucket
once the run has ended and every one has verified. A run that ended while
nothing waited for it keeps them until you call `cleanup` with its id;
`cleanup` refuses a run that has not ended.

## Collect the result

Without a background command, call the `wait` tool with the id, and call it
again until the run ends; nothing tells you when it does. It waits up to 45
seconds and returns one of:

- `TASK_STATE_COMPLETED` with the agent's answer in `message` and
  `artifacts`. `outputs` lists each returned file with the local `path` it
  was written to and its `sha256`; read the files there. `not_returned`
  lists each expected file that did not come back, with the reason.
- `TASK_STATE_FAILED` with a `message`: what went wrong as the agent
  reported it, or why the receiver could not run it, a required output did
  not come back, or a credential went unanswered (see the next section).
- `TASK_STATE_AUTH_REQUIRED` with a `message` from the agent naming the
  credential it needs, often an optional one the host has no source for. The
  run has ended; add a source for the credential to the host's
  configuration and submit again.
- `TASK_STATE_REJECTED` or `TASK_STATE_CANCELED` with a `message` that
  says why.
- `TASK_STATE_SUBMITTED` with the `message`
  `queue deadline passed at <time> and no receiver claimed the run` and no
  `retry_after`: no receiver took the run before its queue deadline and none
  will. Check `receivers` for a live one that fits, then `submit` again.
- `retry_after` with the current `state`: the run has not finished. Wait
  that many seconds, or do other work, then call `wait` again.

`status` with the id returns the run's latest state without waiting. Without
an id it lists every run this host can see, latest update first, each with
its `pool`, `state` and `updated` time; use it to find a run whose
id you no longer hold. `events` returns the run's status history.

`logs` with the id returns the agent's standard output as the
receiver redacted it: up to 100 lines from `offset`,
and `next_offset` while more remain. Call it again with `next_offset` to read
on. Use it to follow a long run or to see why one failed. Each line names the
delivery attempt that published it; a run another receiver took over reads
from the start, the earlier attempt's lines first. A page stays within 24 KiB,
which fits a harness's default limit on tool output, such as Claude Code's
`MAX_MCP_OUTPUT_TOKENS`. A line too long for a page comes back as its start,
with `truncated` set and its full size in `bytes`.

## A run that needs a credential

`submit` returns `INVALID_ARGUMENT` with `auth required: credential NAME is not
in this host's configuration` when the host has no source for a credential the
agent requires or the task names. An optional credential the host lacks is not
delivered, and the agent's prompt names it as not supplied.

The receiver that claims a task asks this host for the run's credentials, and
the wherry MCP server answers while it runs, as `wherry wait` does. When no
answer arrives within 5 minutes, usually because a credential's source failed
to resolve, the run ends `TASK_STATE_FAILED` with a `message` that names the
credentials and never holds their values. Tell the user to add a source for
the credential under that name to `credentials` in their wherry configuration:
a file, an environment variable of the process answering, or a command. Never ask for the value or handle it
yourself. When the user says it is in place, call `submit` again with the
same input, and start its `wait` as before.

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
restart the server, since a harness keeps the old process running. `UNAVAILABLE` is any other failure; its cause is in the MCP
server's log, and the user can also run the same operation with the `wherry`
CLI to see it. `OUTPUTS_NOT_WRITTEN` means
the result arrived but its outputs could not be written, downloaded or cleaned
up; the message starts with the step that failed,
and the MCP server's log holds the cause.
