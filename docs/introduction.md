# Introduction

A coding agent runs its subagents on the machine the agent itself runs on.
wherry sends a task to other compute, starts an unmodified agent there, and
returns the result to the caller. Kafka carries every exchange between
hosts and receivers. [Key concepts](concepts.md) explains the model.

## The problem

A coding agent that delegates work runs its subagents on the machine the
agent itself runs on, or in the agent vendor's cloud. Neither is
infrastructure its user operates, so a subagent cannot use the user's own
clusters, hardware, network placement or isolation.

wherry sends a subagent task to a platform the user runs, starts an
unmodified agent there, and returns the result to the caller. Every
exchange is a record on a Kafka cluster the user also runs. Kafka supplies
identity, authorisation, isolation, history, work distribution and atomic
writes.

wherry's capabilities sit between the caller and the platform that runs
the agent.

![wherry's capabilities: harness integration, placement, platform adapters, context delivery, results, and authority and trust](diagrams/architecture.svg)

## What ships

One `wherry` binary with three sides:

- **Operator:** `wherry admin` creates pools and their terms, entitles
  tenants, adds hosts and receivers with their Kafka principals, ACLs and
  bundles, records receiver configurations, revokes access, and checks the
  result against the broker.
- **Host:** `wherry init` and `wherry key` register and show the host's signing
  key. `receivers` lists the receivers the tenant may use with their adapters
  and labels. `submit` places a task by agent and label selector and publishes
  it. `await`, `status`, `events` and `cancel` follow and stop a run, and
  `await` answers the claiming receiver's request for the agent's
  credential. `status` without a run ID lists every run the host
  can see, `transcript` prints a run's agent output, and `cleanup` deletes
  a run's S3 objects. `mcp` serves every one of these commands but `init`
  and `key` as an MCP tool of the same name; see
  [Harness integration](operations.md#harness-integration).
- **Receiver:** `wherry receiver run` claims its pool's tasks and runs each one
  as a job through an adapter plugin, such as the Nomad plugin from
  [wherry-plugins](https://github.com/benemon/wherry-plugins). The adapter
  protocol is the Go module in
  [`adapter`](https://github.com/benemon/wherry/tree/main/adapter).

Submissions, statuses, transcript chunks and results are A2A v1.0 objects;
[the wherry A2A extension](a2a-extension-v1.md) specifies every
record.
