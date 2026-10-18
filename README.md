# wherry

A coding agent runs its subagents on the machine the agent itself runs on.
wherry sends a task to other compute and returns the result to the caller.

## What ships

- `wherry`, a CLI for the submitting side:
  - `init` and `key` create and show the user signing key.
  - `enrol list` shows enrolment and renewal requests. `enrol approve`
    approves one; `--renewal auto|manual` sets the renewal policy (`auto` by
    default) and `--expires` sets the approval lifetime (30 days by default).
    `enrol renew` renews a receiver approved with `--renewal manual`.
    `enrol revoke` revokes a receiver; credentials sealed to it are not
    rotated, so rotate them at their source.
  - Receivers ask for renewal before their approval expires. With
    `--renewal auto`, any command that connects to Kafka answers the request,
    and only when the renewal extends the approval. A renewal runs 30 days
    from when it is signed.
  - `secret put` seals a credential to one receiver; `--expires` sets its
    lifetime (30 days by default). It lists the runs in the receiver's pool
    that are waiting for that credential.
  - `receivers` lists enrolled receivers, their offers and sealed credentials,
    and warns about credentials that expire within 7 days.
  - `submit` publishes a task to the matching live receiver with the most
    spare capacity.
    `--git URL --git-ref REF [--git-path PATH]` adds a git repository that
    the receiver checks out for the agent before it starts. The receiver's
    platform supplies host trust and repository access: a CA bundle,
    `/etc/ssh/ssh_known_hosts`, and deploy keys or a configured credential
    helper.
  - `continue` (alias `resume`) publishes the next turn of an interrupted
    run. `--prompt` sets its text (`continue` by default);
    `--deadline` and `--queue-deadline` set its deadlines.
  - `cancel` cancels a run, which ends in `TASK_STATE_CANCELED`.
  - `await` waits for a task's reply.
  - `events` prints a task's event history.
  - `mcp` serves these operations as MCP tools over stdio; see
    [Harness integration](#harness-integration).
- `wherry receiver`, which claims tasks for its pool and runs each one as a
  job through a compile-time Nomad adapter.
- Kafka as the record of each exchange. Tasks, events, transcripts and replies
  are signed records that chain to the user key.
- Task, event, transcript and reply records are A2A v1.0 objects;
  [the wherry A2A extension](docs/a2a-extension-v1.md) defines their added
  fields.

## Documentation

- [Concepts](docs/concepts.md)
- [Getting started](docs/getting-started.md)
- [Operator guide](docs/operator-guide.md)
- [Reference](docs/reference.md)
- [The wherry A2A extension](docs/a2a-extension-v1.md)

## Harness integration

`wherry mcp` is an MCP server (protocol 2026-07-28 and earlier) on stdio.
It reads the CLI's configuration and offers seven tools: `receivers`,
`submit`, `status`, `await`, `continue`, `cancel` and `events`. Each takes
and returns JSON with a declared schema. `await` waits up to 45 seconds,
then returns the result, an interrupted, canceled or rejected state, or
`retry_after` while the run is unfinished.

[`plugins/wherry`](plugins/wherry) packages the server with a `wherry`
Agent Skill that teaches the submit and await protocol. The
[operator guide](docs/operator-guide.md#harness-integration) covers
installing it.

## Tests

Tests that need a Kafka broker run when `WHERRY_KAFKA_TEST_BROKER` is set to
a plaintext broker address, such as `127.0.0.1:9092`, and skip otherwise.
The broker needs share groups for the receiver tests; they run against Kafka
4.3.
