# Introduction

Running an agent has well-known answers: containers, sandboxes, clusters.
Handing a coding agent's work to that infrastructure, across machines,
tenants and trust boundaries, and getting the result back, has no standard
answer. wherry is agent task delegation over Kafka.
[Key concepts](concepts.md) explains the model.

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

wherry is open source under the Mozilla Public License 2.0 and has no
hosted control plane. The hosts, the receivers, the Kafka cluster and the
object storage are all the user's, so the tasks, their files and their
results stay on infrastructure the user governs.
[Where your data goes](governance.md#where-your-data-goes) traces each one.

wherry's capabilities sit between the caller and the platform that runs
the agent.

![wherry's capabilities: harness integration, placement, platform adapters, context delivery, results, and authority and trust](diagrams/architecture.svg)

## Why wherry

wherry is an execution fabric for delegated agent work. It gives an
organisation control over where that work runs, under whose authority, and
how it is governed and recovered. Each value below states what wherry does,
where the docs show it, and what it does not claim. A documented capability
shows the design. It measures neither performance, cost nor production
maturity.

### Run delegated AI workloads on infrastructure you control

Subagent work leaves the originating machine and the agent vendor's cloud
and runs on infrastructure the organisation chooses and operates.

- **Evidence:** receivers launch unmodified agents through adapter plugins
  for Nomad, Kubernetes and OpenShift, and systemd
  ([Adapters](adapters.md)). A task names an agent and a label selector, so
  placement picks environments with particular properties, such as GPUs,
  network segments or isolation levels ([Placement](concepts.md#placement)).
  Records stay on the organisation's Kafka cluster and large files in its
  own bucket ([Where your data goes](governance.md#where-your-data-goes)).
- **What it gives:** existing compute serves agents, placement requirements
  are met, and delegated agents reach environments a developer's
  workstation cannot.

### Delegate execution without surrendering control of identity and credentials

Explicit trust boundaries separate the agent asking for work, the
infrastructure doing it and the credentials it needs.

- **Evidence:** every host and receiver is its own Kafka principal with its
  own Ed25519 signing key, and Kafka ACLs confine each to its pools and its
  tenant's topics ([Identity and grants](governance.md#identity-and-grants)).
  Credentials never travel in the submission: the host seals each one to
  one receiver's ephemeral key for one attempt
  ([Sealed credentials](security.md#sealed-credentials)).
- **What it gives:** less credential exposure, enforced tenant
  entitlements, and one operator who controls who may submit and run work.
- **Limits:** this controls how a credential is delegated. An execution
  environment that is compromised can read a credential legitimately
  delivered to it.

### Make delegated agent execution recoverable and observable

Delegated work is managed distributed work, whose state outlives a
dropped connection or a failed receiver.

- **Evidence:** Kafka share groups distribute tasks with renewed
  acquisition locks, redelivery and attempt tracking. A receiver adopts a
  run's existing job, resumes its transcript from the recorded offset, and
  transactional fencing stops an earlier attempt from publishing
  ([Runs and attempts](concepts.md#runs-and-attempts)). Status, events,
  transcripts and cancellation are available from the CLI and the MCP
  server ([Commands](commands.md), [MCP tools](mcp.md)).
- **What it gives:** recovery from receiver failures without manual
  intervention, and remote runs that can be diagnosed.
- **Limits:** the recovery mechanisms are designed and tested; they are no
  proof of exactly-once execution or of a measured availability.

### Decouple agent orchestration from execution platforms

An existing coding-agent workflow delegates tasks without coupling itself
to one scheduler.

- **Evidence:** the MCP server exposes submission, monitoring and results
  to any harness ([Harness integration](operations.md#harness-integration)).
  Every record is an A2A v1.0 object
  ([the wherry A2A extension](a2a-extension-v1.md)), and adapter plugins
  hold everything platform-specific. The agent that runs is unmodified.
- **What it gives:** freedom to change execution environments, and less
  bespoke orchestration inside an agent harness.
- **Limits:** protocol compatibility does not mean every harness or
  platform works without integration work; each agent still needs an image
  or install and an adapter configuration.

### Trace distributed agent execution from request to result

The link between the submitted work, the identity that ran it, its
attempts and the files it produced is kept.

- **Evidence:** submissions and statuses are signed records naming their
  run and attempt, results name the receiver, pool and adapter, and every
  returned file carries its SHA-256, which the host checks before it writes
  the file ([Signed records](governance.md#signed-records),
  [Files in and out](concepts.md#files-in-and-out)).
- **What it gives:** a basis for troubleshooting, operational
  accountability and stronger audit processes for autonomous workloads.
- **Limits:** these are traceability primitives. They are no tamper-proof
  audit trail and no provenance of everything an agent did inside its job,
  and Kafka retention limits how long a run's records remain.

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
