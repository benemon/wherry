# Use cases

## Hand off work and walk away

You have a task that takes an hour, and you don't want to keep your laptop awake for it.
Submit it to wherry and close the lid.
The run keeps going on a receiver, and its result waits in Kafka until you ask for it.
When you're back, `wherry host wait RUN_ID` collects the result from any session on the same host.
If the agent changed a repository, the result brings its commits back as a patch or a bundle.

[Runs and attempts](concepts.md#runs-and-attempts) explains what happens if a receiver stops mid-run.
[Git repositories](operations-hosts.md#git-repositories) shows how to get changes back.

## Reach systems only a particular network can reach

Your agent needs a repository or a service that only one network can reach, and your laptop isn't on it.
Run a receiver inside that network and send the task there.
The agent runs next to the system it needs, so your laptop never needs the access.
The credentials the run needs travel encrypted, and only the attempt that runs the task can open them.

[Placement](concepts.md#placement) explains how a task reaches a receiver.
[Credential envelopes](security.md#credential-envelopes) explains how credentials travel.

## Run where the hardware, OS or isolation is different

Some tasks need a GPU, another architecture, or stronger isolation than your machine gives.
Your operator labels each receiver's profiles with what they offer.
You choose one with a selector when you submit, and the rest of the command stays the same.

[Placement](concepts.md#placement) explains labels and selectors.
[Receiver configuration](configuration-receiver.md#receiver-configuration-example) shows profiles with container and host
isolation.

## Fan out independent tasks

Your work splits into pieces that don't depend on each other.
Submit each piece as its own run, and the pool's receivers take them as they have room, side by side.
Your coding agent's harness splits the work and combines the results, through the same tools it uses for one task.

[MCP tools](mcp.md) lists the tools a harness calls.

## Share agent compute across teams

You run one set of receivers, and several teams want to use them.
Make each team a tenant and give it the pools it may use.
Each team's hosts see only that team's runs.
When a team no longer needs access, remove its tenant: its access to every pool ends, its hosts are revoked, and runs
already going finish.

[Tenants](concepts.md#tenants) explains what a tenant is.
[Operator operations](operations-operator.md#tenant-onboarding-and-offboarding) gives the steps.

## Audit what delegated agents did

An agent acted on your behalf, and you need to know what it did.
Every record of a run is signed by the host or receiver that wrote it.
`wherry host events` shows which receiver ran each attempt, and when.
`wherry host logs` shows the agent's transcript, with its credentials redacted.
Every file the run returns is checked against its digest before wherry writes it.

[Run history](governance.md#run-history) explains what is kept and for how long.
[Host commands](commands.md#host-commands) lists the commands.
