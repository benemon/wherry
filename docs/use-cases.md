# Use cases

## Hand off work and walk away

You have a task that takes an hour, and you don't want to keep your laptop awake for it.
Submit it to wherry and close the lid.
The run keeps going on a receiver.
Its result waits in Kafka, for 30 days by default.
When you're back, run `wherry host wait RUN_ID` on the same host to collect it.
If you asked for the agent's commits with `--git-changes`, they come back as a patch or a bundle.

[Runs and attempts](concepts.md#runs-and-attempts) explains what happens if a receiver stops mid-run.
[Git repositories](operations-hosts.md#git-repositories) shows how to get changes back.

## Reach systems only a particular network can reach

Your agent needs a repository or a service that only one network can reach.
Your laptop isn't on that network.
Run a receiver inside it and send the task there.
The agent runs next to the system it needs, so your laptop never needs the access.
The credentials the run needs travel encrypted, and only receivers in the pool can open them.
The receiver that claims the task gets them for that one attempt.

[Placement](concepts.md#placement) explains how a task reaches a receiver.
[Credential envelopes](security.md#credential-envelopes) explains how credentials travel.

## Run where the hardware, OS or isolation is different

Some tasks need a GPU, another architecture, or stronger isolation than your machine gives.
Your operator labels each receiver's profiles with what they offer.
You choose one with a selector when you submit.
The rest of the command stays the same.

[Placement](concepts.md#placement) explains labels and selectors.
[Receiver configuration](configuration-receiver.md#receiver-configuration-example) shows profiles with container and host
isolation.

## Fan out independent tasks

Your work splits into pieces that don't depend on each other.
Submit each piece as its own run.
The pool's receivers take them as they have room.
They run side by side, as far as the pool's capacity allows.
You, or your coding agent, split the work and combine the results.
A coding agent uses the same tools for many runs as for one.

[MCP tools](mcp.md) lists the tools a coding agent calls.

## Share agent compute across teams

You run one set of receivers, and several teams want to use them.
Make each team a tenant and give it the pools it may use.
Each team's hosts see only that team's runs.
When a team no longer needs access, remove its tenant.
Its hosts lose access to every pool, and its queued tasks never start.
Jobs a receiver is already running finish, but the team can no longer collect their results.

[Tenants](concepts.md#tenants) explains what a tenant is.
[Operator operations](operations-operator.md#tenant-onboarding-and-offboarding) gives the steps.

## Audit what delegated agents did

An agent acted on your behalf, and you need to know what it did.
The host or receiver that wrote each task, status and result signs it.
`wherry host events` shows which receiver ran each attempt, and when.
`wherry host logs` shows the agent's transcript.
The receiver redacts the run's credentials from it, by the rules [Credential envelopes](security.md#credential-envelopes) gives.
Every file the run returns is checked against its digest, and a file that doesn't match is not kept.

[Run history](governance.md#run-history) explains what is kept and for how long.
[Host commands](commands.md#host-commands) lists the commands.
