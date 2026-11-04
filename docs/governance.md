# Governance

Use this page to understand access, retained history and data flow.
[Security](security.md) explains credential delivery and its trust boundaries.

## Identity and grants

The operator creates each host and receiver as a Kafka principal.
You receive its bundle outside Kafka.
The broker enforces the ACLs that `wherry operator` generates from installation state.
`operator init` grants the operator principal its own ACLs.

Your tenant's hosts can reach the pools it is entitled to.
A receiver reaches its own pool.
It retains access to the feeds, transcript topics and host key topics of previously entitled tenants.
This lets runs it already holds finish after entitlement removal.
See [Removal and retained data](security.md#removal-and-retained-data).
[Identity and authorisation](concepts.md#identity-and-authorisation) lists the principals.
[ACLs](design.md#acls) lists their grants.

Create a tenant explicitly before adding hosts or entitlements.
Removing a tenant ends its entitlements and revokes its hosts.
Its name cannot be reused.
Removal keeps retained data.

## Signed records

Hosts and receivers sign shared records as EdDSA JWS objects.
Operator records and receiver progress records are unsigned.
Kafka controls who can write those topics.

Readers load an owner's keys from its key topic.
They verify the signature and check the record against installation state.
A submission requires a live host of a tenant entitled to its pool.
A receiver record requires a live receiver in the run's pool.
No host or receiver can vouch for another's key.

History readers also accept a retired signing key for records appended before its rotation.
New submissions require a current key.
[Topics](design.md#topics) explains these checks.

## Run history

Operator records are retained indefinitely.
Pool task and event retention is at least `start_by_limit` plus one day and six hours.
The default is four days and six hours.
Applying a shorter start-by limit does not lower existing retention.
Tenant feeds and transcript topics retain records for 30 days.
Receiver progress records track committed transcript offsets.
Terminal runs have their progress records tombstoned for compaction.

Use `wherry host events RUN_ID` to read status history.
Use `wherry host logs RUN_ID` to read the transcript.
Retention limits the available history.
Activity inside the job that the transcript doesn't show is visible only if the platform records it.

## Where your data goes

| Data | Destination |
| --- | --- |
| Prompt, inline inputs, status, transcript and result | Your Kafka cluster. |
| S3 inputs and outputs | Your S3-compatible bucket through presigned URLs. |
| S3 storage credentials | The submitting host. |
| Run credentials | The encrypted task envelope and the receiving agent's configured targets. |
| Local run records and redaction keys | The host's state directory. |

The key custodian opens credential envelopes to check their task binding.
The claiming attempt opens its envelope with an attempt key.
A former custodian can open envelopes under keys it still holds to re-key them.
[Security](security.md) describes these access boundaries.
Local host records contain no credential values in the clear.

wherry has no hosted control plane.
Kafka client telemetry is disabled.
An agent's external calls go where its own environment and configuration send them.
wherry and its plugins use the Mozilla Public License 2.0.
