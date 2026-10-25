---
layout: home
hero:
  image:
    light: /svg/wherry-stacked.svg
    dark: /svg/wherry-stacked-cream.svg
    alt: wherry
  tagline: Agent task delegation over Kafka.
  actions:
    - theme: brand
      text: Quickstart
      link: /quickstart
    - theme: alt
      text: Key concepts
      link: /concepts
features:
  - title: Any agent harness
    details: For developers. The MCP server and CLI fit any harness, and the agent runs unmodified.
    link: /operations#harness-integration
  - title: Runs that survive restarts
    details: For developers. A receiver's runs continue after it restarts, through adoption and redelivery.
    link: /concepts#runs-and-attempts
  - title: Your compute, your Kafka
    details: For platform teams. Agents run on Nomad, Kubernetes and OpenShift, or systemd, and every record stays on your own Kafka cluster.
    link: /adapters
  - title: Open source, no hosted control plane
    details: For platform teams. wherry is MPL-2.0 and sends nothing to a third party; the hosts and receivers are yours.
    link: /governance#where-your-data-goes
  - title: Governed by the operator
    details: For governance. The operator issues every identity, the broker enforces every grant, and every shared record is signed and kept.
    link: /governance
  - title: Sealed credentials
    details: For security. Each credential is sealed to one receiver for one attempt and delivered to its target in the agent.
    link: /security#sealed-credentials
---
