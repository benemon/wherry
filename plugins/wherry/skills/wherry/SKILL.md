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
`wherry` MCP server (`wherry mcp`). Every tool except `status` has a
`wherry` CLI command of the same name.

## When to delegate

Delegate a task when all of these hold:

- The prompt can stand on its own. The remote agent sees the prompt and,
  if you pass one, a checkout of a pushed git ref. It does not see this
  session, your files or uncommitted changes.
- The task takes minutes or more, or needs something this machine does not
  have (an OS, architecture, isolation level or network).
- You have other work to do while it runs, or several such tasks to run
  at once.

Do short or interactive work yourself.

## Choose a placement

Call `receivers` first. For each receiver it returns:

- `adapters`: each with the `agent` it runs, its `labels`, and `ready`,
  whether the receiver can start it now.
- `status`, `capacity` and `in_flight`: whether it is live and has room.
- `seals`: the credentials this host has sealed to it.

Pick an `agent` from the adapters, or omit it to take the pool's default.
Build `selector` from the labels of the adapters you want: a list of
`{key, operator, values}` where `operator` is `In` or `NotIn` (with
`values`), or `Exists` or `DoesNotExist` (without). Every requirement must
hold. Omit `selector` when any adapter will do. A malformed key or value
returns `INVALID_ARGUMENT`; a selector no live adapter satisfies returns
`NO_MATCHING_RECEIVER`.

Labels are the operator's description of each adapter. wherry matches them
literally and interprets only the agent name.

## Submit, then keep working

Call `submit` with `prompt`, and `agent`, `selector` and `git` (`url`,
`ref`, optional `path`) when you need them. It returns `id` and `pool`
straight away. Note the id and carry on with other work.

## Collect the result

Call `await` with the id. It waits up to 45 seconds and returns one of:

- `TASK_STATE_COMPLETED` with `artifacts`: the result.
- `TASK_STATE_FAILED` with `artifacts` holding the agent's result when the
  agent reported the failure, or a `message` when the receiver could not
  run it.
- `TASK_STATE_REJECTED` or `TASK_STATE_CANCELED` with a `message` that
  says why.
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

`TASK_STATE_AUTH_REQUIRED` means no receiver in the pool could open a
sealed credential the agent needs. The `message` names the credential and never
holds its value. Tell the user to put the credential in a file and add it
under that name to `credentials` in their wherry configuration. Never ask
for the value or handle it yourself. When the user says it is in place,
call `continue` with the id (and a `prompt` if the agent needs more to go
on), which seals it, then `await` again.

## Cancel unneeded work

When you no longer need a run's result, call `cancel` with its id. The run
ends in `TASK_STATE_CANCELED`.

## Errors

A failed call returns `{"code": ..., "message": ...}`. `RUN_NOT_FOUND`,
`NO_MATCHING_RECEIVER`, `INVALID_ARGUMENT`, `NOT_AWAITING_CONTINUATION`,
`RUN_CANCELED` and `TURN_ALREADY_SUBMITTED` call for a change to your input
or end the run's handling. `CANCELLED` means the call itself was cancelled.
`UNAVAILABLE` is any other failure; the user can run the same operation
with the `wherry` CLI to see the cause.
