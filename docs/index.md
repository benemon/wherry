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
      link: /quickstart-production
    - theme: alt
      text: How it works
      link: /introduction
features:
  - title: Hand off work and walk away
    link: /use-cases#hand-off-work-and-walk-away
  - title: Reach systems only a particular network can reach
    link: /use-cases#reach-systems-only-a-particular-network-can-reach
  - title: Run where the hardware, OS or isolation is different
    link: /use-cases#run-where-the-hardware-os-or-isolation-is-different
  - title: Fan out independent tasks
    link: /use-cases#fan-out-independent-tasks
  - title: Share agent compute across teams
    link: /use-cases#share-agent-compute-across-teams
  - title: Audit what delegated agents did
    link: /use-cases#audit-what-delegated-agents-did
---

## Why wherry

You choose where delegated agent work runs.
wherry sends tasks to your receivers through your Kafka cluster.
Platform plugins run agents on Nomad, Kubernetes, OpenShift or hosts under systemd.
[Placement](concepts.md#placement) selects profiles by agent and labels.

Your operator creates tenants and grants each tenant access to pools.
Hosts submit signed tasks.
Receivers publish status, transcripts and results.
The pool's key custodian releases each credential envelope to its claiming attempt.
[Security](security.md) explains the trust boundaries.

A run can outlast the session that submits it.
Use `wherry host wait RUN_ID` to collect its result later.
[Runs and attempts](concepts.md#runs-and-attempts) explains recovery and its limits.
[Run history](governance.md#run-history) explains retention.
