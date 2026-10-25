---
layout: home
hero:
  image:
    light: /svg/wherry-stacked.svg
    dark: /svg/wherry-stacked-cream.svg
    alt: wherry
  tagline: Agent task delegation over Kafka.
  text: wherry sends a coding agent's tasks to receivers on other compute, runs an unmodified agent there, and returns the result.
  actions:
    - theme: brand
      text: Quickstart
      link: /quickstart
    - theme: alt
      text: Key concepts
      link: /concepts
features:
  - title: Any agent harness
    details: The MCP server and CLI fit any harness, and the agent runs unmodified.
    link: /operations#harness-integration
  - title: Any platform
    details: Adapter plugins run agents on Nomad, Kubernetes and OpenShift, and systemd.
    link: /adapters
  - title: Sealed credentials
    details: Each credential is sealed to one receiver for one attempt.
    link: /security#sealed-credentials
  - title: Signed records
    details: Every shared record is signed and checked against the operator's state.
    link: /design#topics
  - title: Large files through S3
    details: Inputs and outputs of up to 5 GiB travel through an S3-compatible bucket.
    link: /concepts#files-in-and-out
  - title: Runs that survive restarts
    details: A receiver's runs continue after it restarts, through adoption and redelivery.
    link: /concepts#runs-and-attempts
---
