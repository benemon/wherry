# Use cases

## Offload a long build from a laptop

A task that takes minutes or hours runs on a receiver while the laptop
carries on with other work. The agent clones the repository itself and
returns its commits as a patch or a bundle, which the laptop applies. The
run goes on whether or not the caller is watching, and the caller can
follow it or collect its result later. [Quickstart](quickstart.md) runs a
first task, and [Git repositories](operations.md#git-repositories) covers
the clone and the changes it returns.

## Delegate work into controlled environments

A coding agent needs a repository or a service that only a particular
network can reach. The task runs on a receiver inside that network, so the
developer's machine never needs the access. The operator decides which
tenants may use the pool, the broker enforces it, and the receiver gets
only the credentials its run declares, sealed to it for that attempt.
[Identity and grants](governance.md#identity-and-grants),
[Placement](concepts.md#placement) and
[Sealed credentials](security.md#sealed-credentials) describe access,
placement and credential delivery.

## Run on another OS, architecture or isolation level

Some tasks need specialised hardware, such as a GPU or another
architecture, or a stronger isolation level than the developer's machine
gives. The operator labels each profile with what it offers, and a task's
selector picks among them. [Placement](concepts.md#placement) explains the
labels and selectors, and the
[receiver configuration example](configuration.md#receiver-configuration-example)
declares two profiles with different isolation.

## Fan out independent tasks

A harness splits work into independent tasks, submits each as its own run
and awaits each one. The pool's receivers claim them as their capacity
allows, so the tasks run side by side. The harness does the splitting and
combines the results. [MCP tools](mcp.md) lists the `submit` and `await`
tools a harness calls.

## Follow delegated work across machines

A delegated task runs on another machine and can outlast the session that
started it. The caller sees whether the run was claimed, which receiver
handled each attempt, every status it reported and its transcript, and
receives its result with the output files checked against their digests.
[Runs and attempts](concepts.md#runs-and-attempts) explains a run's
lifecycle, and [Run history](governance.md#run-history) describes which
records are kept and for how long.

## Compare agent harnesses

A pool's terms can name several agents, each launched by its own harness.
Submitting the same prompt once per agent, with `--agent`, runs the same task
through each harness, to check behaviour that should not depend on the
harness. The [pool terms example](configuration.md#pool-terms-example) names
three agents, and [`wherry submit`](commands.md#wherry-submit) takes the
agent.
