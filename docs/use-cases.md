# Use cases

## Hand off work and walk away

Submit a task and close your laptop.
The run outlasts the session that starts it.
Use `wherry host wait RUN_ID` in a later session to collect its result.
Keep the host configuration and state available to that session.
[Runs and attempts](concepts.md#runs-and-attempts) explains recovery limits.
[Git repositories](operations-hosts.md#git-repositories) explains how to return changes.

## Reach systems only a particular network can reach

Place a receiver inside the network that can reach your repository or service.
Select its profile when you submit the task.
The operator controls your tenant's access to the pool.
[Placement](concepts.md#placement) explains selectors.
[Credential envelopes](security.md#credential-envelopes) explains how the run receives credentials.

## Run where the hardware, OS or isolation is different

Choose a profile with the hardware, operating system or isolation your task needs.
The operator labels each profile with the capabilities it offers.
[Receiver configuration](configuration-receiver.md#receiver-configuration-example) shows profiles for container and host isolation.

## Fan out independent tasks

Submit each independent task as its own run.
Receivers claim the tasks as their capacity allows.
Your harness splits the work and combines the results.
[MCP tools](mcp.md) describes the tools it can call.

## Share agent compute across teams

Use one set of receivers for several tenants.
Entitle each tenant to the pools it may use.
Remove a tenant to end its entitlements and revoke its hosts.
Removal waits for host revocation and keeps retained data.
[Tenants](concepts.md#tenants) explains the lifecycle.
[Operator operations](operations-operator.md#tenant-onboarding-and-offboarding) gives the procedure.

## Audit what delegated agents did

Inspect who submitted a run and which receiver ran each attempt.
Read its status history and transcript.
Collect outputs with their digests checked before they are written.
[Run history](governance.md#run-history) explains retention.
[Host commands](commands.md#host-commands) lists the inspection commands.
