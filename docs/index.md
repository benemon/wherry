---
layout: home
hero:
  image:
    light: /svg/wherry-stacked.svg
    dark: /svg/wherry-stacked-cream.svg
    alt: wherry
  tagline: An execution fabric for delegated agent work.
  actions:
    - theme: brand
      text: Quickstart
      link: /quickstart
    - theme: alt
      text: How it works
      link: /introduction
features:
  - title: Run delegated AI workloads on infrastructure you control
    link: /#run-delegated-ai-workloads-on-infrastructure-you-control
  - title: Keep control of identity and credentials when you delegate
    link: /#keep-control-of-identity-and-credentials-when-you-delegate
  - title: Follow every delegated run, and recover it when a receiver restarts
    link: /#follow-every-delegated-run-and-recover-it-when-a-receiver-restarts
  - title: Keep your agent and workflow when you change where work runs
    link: /#keep-your-agent-and-workflow-when-you-change-where-work-runs
  - title: Trace every run from request to result
    link: /#trace-every-run-from-request-to-result
---

## Why wherry

A coding agent can hand its subagent work to other compute, but usually
only to the machine it runs on or to its vendor's cloud. wherry gives an
organisation control over where that work runs, under whose authority, and
how it is governed and recovered.

### Run delegated AI workloads on infrastructure you control

Delegated agent work runs on the compute an organisation already operates:
Nomad, Kubernetes and OpenShift, or plain hosts under systemd. Placement by
label sends each task to the environment it needs, whether that is a GPU
node, a particular network segment or a stronger isolation level. wherry
carries tasks, files and results through the organisation's own Kafka
cluster and object storage; the agent's own calls, such as those to its
model provider, go where its environment and configuration send them.
[Placement](concepts.md#placement) and
[Where your data goes](governance.md#where-your-data-goes) explain how.

### Keep control of identity and credentials when you delegate

The operator decides who may submit work and who may run it. Every host
and receiver has its own identity, the broker enforces what each may
reach, and each credential is sealed to the one receiver and the one
attempt that needs it. [Governance](governance.md) and
[Security](security.md) cover the model.

### Follow every delegated run, and recover it when a receiver restarts

A delegated run is managed work, so it outlives a dropped connection. When
a receiver stops renewing its claim, Kafka offers the task again. The next
receiver adopts the run's job when it matches and otherwise starts it
afresh, and fencing stops the earlier attempt from publishing. Credentials
bound recovery: a host seals a run's credentials only to the receiver that
first claimed it, so a credentialed run that another receiver adopts fails
and is submitted again. Every run's status, events and transcript can be
followed from the CLI or from the agent's own harness.
[Runs and attempts](concepts.md#runs-and-attempts) and
[Sealed credentials](security.md#sealed-credentials) give the detail.

### Keep your agent and workflow when you change where work runs

A coding agent delegates through the same tools whatever runs the work.
The MCP server fits any harness, the agent that runs the task is
unmodified, and everything platform-specific stays in an adapter,
so changing where work runs leaves the workflow as it is.
[Harness integration](operations.md#harness-integration) shows the setup.

### Trace every run from request to result

Each run keeps the thread from the request to its result: who submitted
it, which receiver ran each attempt, what it reported, and which files came
back. Every shared record is signed, and every returned file is checked
against its digest before it is written.
[Run history](governance.md#run-history) shows what is kept and for how
long.

## What you need

wherry is self-hosted and needs Kafka. An operator runs a Kafka 4.3 cluster
with share groups, SCRAM over TLS and an authoriser, creates pools and
grants access with `wherry admin`, and runs receivers with the adapters
for the target platforms. A coding agent's harness talks to wherry
through the `wherry` CLI or its MCP server. [Prepare Kafka](operations.md#prepare-kafka)
lists the cluster settings, [Why Kafka](design.md#why-kafka) explains the
choice, and the [Quickstart](quickstart.md) runs a first task.
