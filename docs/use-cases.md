# Use cases

## Offload a long build from a laptop

A task that takes minutes or hours runs on a receiver while the laptop
carries on with other work. The agent clones the repository itself and
returns its commits as a patch or a bundle, which the laptop applies.
[Quickstart](quickstart.md) runs a first task, and
[Git repositories](operations.md#git-repositories) covers the clone and the
changes it returns.

## Run on another OS, architecture or isolation level

The operator labels each adapter with what it offers, such as a GPU, an
architecture or an isolation level, and a task's selector picks among them.
[Placement](concepts.md#placement) explains the labels and selectors, and the
[receiver configuration example](configuration.md#receiver-configuration-example)
declares two adapters with different isolation.

## Fan out independent tasks

A harness submits several independent tasks, each its own run, and awaits
each one. The pool's receivers claim them as their capacity allows, so the
tasks run side by side. [MCP tools](mcp.md) lists the `submit` and `await`
tools a harness calls, and [Runs and attempts](concepts.md#runs-and-attempts)
describes a run.

## Compare agent harnesses

A pool's terms can name several agents, each launched by its own harness.
Submitting the same prompt once per agent, with `--agent`, runs the same task
through each harness, to check behaviour that should not depend on the
harness. The [pool terms example](configuration.md#pool-terms-example) names
three agents, and [`wherry submit`](commands.md#wherry-submit) takes the
agent.
