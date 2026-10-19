# wherry

A coding agent runs its subagents on the machine the agent itself runs on.
wherry sends a task to other compute, starts an unmodified agent there, and
returns the result to the caller. Kafka carries every exchange between
hosts and receivers. [Concepts](docs/concepts.md) explains the model.

## What ships

One `wherry` binary with three sides:

- **Operator:** `wherry admin` creates pools and their terms, entitles
  tenants, adds hosts and receivers with their Kafka principals, ACLs and
  bundles, records receiver configurations, revokes access, and checks the
  result against the broker.
- **Host:** `wherry init` and `wherry key` register and show the host's
  signing key. `receivers` lists the receivers the tenant may use with their
  adapters and labels. `submit` places a task by agent and label selector,
  seals the agent's credentials to the chosen pool, and publishes it.
  `await`, `status`, `events`, `continue` (alias `resume`) and `cancel`
  follow, extend and stop a run; `status` without a run ID lists every run
  the host can see. `mcp` serves `receivers`, `submit`, `status`, `await`,
  `continue`, `cancel` and `events` as MCP tools; see
  [Harness integration](#harness-integration).
- **Receiver:** `wherry receiver` claims its pool's tasks and runs each one
  as a job through a compile-time Nomad adapter.

Submissions, statuses, transcript chunks and results are A2A v1.0 objects;
[the wherry A2A extension](docs/a2a-extension-v1.md) specifies every
record.

## Documentation

- [Concepts](docs/concepts.md)
- [Getting started](docs/getting-started.md)
- [Operator guide](docs/operator-guide.md)
- [Reference](docs/reference.md)
- [The wherry A2A extension](docs/a2a-extension-v1.md)

## Harness integration

`wherry mcp` is an MCP server (protocol 2026-07-28 and earlier) on stdio.
It reads the host's configuration and offers seven tools: `receivers`,
`submit`, `status`, `await`, `continue`, `cancel` and `events`. Each takes
and returns JSON with a declared schema. `await` waits up to 45 seconds,
then returns the final or interrupted state with its result or message,
or `retry_after` while the run is unfinished.

[`plugins/wherry`](plugins/wherry) packages the server with a `wherry`
Agent Skill that teaches the submit and await protocol. The
[operator guide](docs/operator-guide.md#harness-integration) covers
installing it.

## Tests

Unit tests run against an in-process Kafka. Tests that need real services
run when their variables are set and skip otherwise:

- `WHERRY_KAFKA_TEST_BROKER`: a plaintext Kafka 4.3 broker with share
  groups, such as `127.0.0.1:9092`, for the receiver's claim tests.
- `WHERRY_TRUST_BROKER` with the other `WHERRY_TRUST_*` variables of
  `wherry admin`: a TLS broker with an authoriser, for the provisioning,
  ACL and revocation tests. `WHERRY_TRUST_RECONCILE_WAIT` adds the check
  that provisioning survives a Strimzi reconciliation.
- `WHERRY_NOMAD_DEV_ADDR`: a Nomad dev agent, for the adapter tests.
