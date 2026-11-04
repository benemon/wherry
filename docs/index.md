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
      text: Install
      link: /install
    - theme: alt
      text: How it works
      link: /introduction
features:
  - title: Hand off work and walk away
    link: /use-cases#hand-off-work-and-walk-away
  - title: Run agents where the network access exists
    link: /use-cases#run-agents-where-the-network-access-exists
  - title: Run tasks in the environment they need
    link: /use-cases#run-tasks-in-the-environment-they-need
  - title: Fan out independent tasks
    link: /use-cases#fan-out-independent-tasks
  - title: Share agent compute across teams
    link: /use-cases#share-agent-compute-across-teams
  - title: Trace delegated work from request to result
    link: /use-cases#trace-delegated-work-from-request-to-result
---

## Why wherry

Coding agents can delegate work, but where that work runs is often decided for you: on the machine running the agent,
or in infrastructure its vendor provides. Neither is necessarily where the work belongs. A task might need a GPU, access
to a private network, or stronger isolation than your laptop gives.

wherry separates the decision to delegate work from the infrastructure that runs it. Tasks travel through your Kafka
cluster to receivers on infrastructure you control. Platform plugins start the agent, as its harness ships it, on
Nomad, Kubernetes, OpenShift or hosts under systemd, and labels on each receiver's profiles let a task choose the
capabilities it needs. [Placement](concepts.md#placement) explains how a task reaches a receiver.

Moving work between machines also means crossing trust boundaries, and wherry makes those boundaries explicit.
Operators define pools and which tenants may use them. Hosts and receivers sign the tasks, statuses, transcripts and results they share. A task's credentials travel with it only
in encrypted form, and the pool's key custodian checks each claim before it releases the key to that attempt. [Security](security.md) describes the trust model.

Once submitted, a run has a lifecycle of its own. You can leave the session that started it, come back to its status
and transcript, and collect its result later. When a receiver stops, Kafka offers the task again, and a later
attempt adopts the existing job or starts it afresh. The run's records show each attempt along the way.
[Runs and attempts](concepts.md#runs-and-attempts) explains the recovery guarantees and their limits, and
[Run history](governance.md#run-history) describes what is kept.
