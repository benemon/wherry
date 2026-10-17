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
`wherry` MCP server (`wherry mcp`). The same operations exist as `wherry`
CLI commands.

## When to delegate

Delegate a task when all of these hold:

- The prompt can stand on its own. The remote agent sees the prompt and,
  if you pass one, a checkout of a pushed git ref. It does not see this
  session, your files or uncommitted changes.
- The task takes minutes or more, or needs labels this machine does not
  have (an OS, architecture, isolation level or network).
- You have other work to do while it runs, or several such tasks to run
  at once.

Do short or interactive work yourself.

## Choose a placement

Call `receivers` first. For each receiver it returns:

- `offers`: label sets the receiver can run a task with. Build `selector`
  from these: each key maps to the values you accept, and an offer must
  match every key.
- `selector`: the labels this receiver requires of a task. Your `labels`
  must satisfy it.
- `status`, `capacity` and `in_flight`: whether it is live and has room.
- `secrets`: the credential names sealed to it.

Pick the offer that fits the task. Labels are descriptive, so reason from
them. The user's approval of each receiver still bounds what can run.

## Submit, then keep working

Call `submit` with `prompt`, `selector`, `labels` and, when the agent needs
a repository, `git` (`url`, `ref`, optional `path`). It returns `id` and
`pool` straight away. Note the id and carry on with other work.

## Collect the result

Call `await` with the id. It waits up to 45 seconds and returns one of:

- `TASK_STATE_COMPLETED` with `artifacts`: the result.
- `TASK_STATE_FAILED`, `TASK_STATE_REJECTED` or `TASK_STATE_CANCELED`
  with a `message` that says why.
- `TASK_STATE_AUTH_REQUIRED`: see the next section.
- `retry_after` with the current `state`: the run has not finished. Wait
  that many seconds, or do other work, then call `await` again.

`status` returns the latest state without waiting. `events` returns the
run's status history.

Where your harness runs shell commands in the background and tells you when
they finish, run `wherry await <id>` as a background command. It blocks
until the run ends (or for 30 minutes; `--timeout` changes that), prints the
state and the result, and exits non-zero for any state other than
`TASK_STATE_COMPLETED`. You then skip the await loop.

## A run that needs a credential

`TASK_STATE_AUTH_REQUIRED` means the receiver lacks a credential. The
`message` names the credential and never holds its value. The receiver is
the one in the run's `pool`, as `receivers` lists it. Tell the user which
credential to seal to that receiver, with the command:

    wherry secret put NAME --to RECEIVER_ID --from-file PATH

Never ask for the credential value or handle it yourself. When the user says
it is sealed, call `continue` with the id (and a `prompt` if the agent needs
more to go on), then `await` again.

## Cancel unneeded work

When you no longer need a run's result, call `cancel` with its id. The run
ends in `TASK_STATE_CANCELED`.

## Errors

A failed call returns `{"code": ..., "message": ...}`. `RUN_NOT_FOUND`,
`NO_MATCHING_RECEIVER` and `INVALID_ARGUMENT` call for a change to your
input. `UNAVAILABLE` means the bus could not be reached; the user can run
the same operation with the `wherry` CLI to see the cause.
