# Design

How wherry lays its records out on Kafka, how a receiver gets its
configuration, and the topics and ACLs `wherry admin` creates.

## Why Kafka

wherry puts Kafka at the centre of identity, authorisation, task
distribution and history, and runs no server of its own. Each need is one
Kafka already meets:

- identity is a SCRAM principal over TLS, and authorisation is ACLs the
  broker enforces;
- a share group hands each task to one receiver, holds it under a renewed
  acquisition lock, and redelivers it when that receiver dies;
- transactions fence an earlier attempt and commit a run's last records
  together;
- the log is the history every host and receiver reads, and a topic per
  audience keeps each reader to its own records.

The cost is a Kafka cluster to operate: version 4.3, with share groups, a
SCRAM listener and an authoriser, as
[Prepare Kafka](operations.md#prepare-kafka) lists. An organisation that
already runs Kafka adds topics and principals to it. One that does not
takes on a cluster to run.

## Topics

Kafka cannot filter records on the broker, so records consumed
differently live on different topics.

| Topic | Writers | Readers | Holds |
| --- | --- | --- | --- |
| `wherry.admin` | the operator | every principal | pool terms, receiver configurations, hosts, entitlements, membership ends, skips |
| `wherry.receivers` | receivers | receivers, hosts | advertisements |
| `wherry.receiver.<receiver>` | that receiver | its pool's members, entitled tenants' hosts | its keys |
| `wherry.receiver.<receiver>.progress` | that receiver | its pool's live members | each run's latest committed transcript offset |
| `wherry.pool.<pool>.tasks` | entitled tenants' hosts | the pool's receivers, through its share group | submissions |
| `wherry.pool.<pool>.events` | the pool's receivers, entitled tenants' hosts | the pool's receivers | statuses, start failures, cancels |
| `wherry.tenant.<tenant>` | receivers of the tenant's pools | the tenant's hosts | statuses, results, echoes, seal requests |
| `wherry.tenant.<tenant>.transcript` | receivers of the tenant's pools | the tenant's hosts | transcript chunks, output chunks |
| `wherry.host.<tenant>.<host>` | that host | that host, receivers of the tenant's pools | its key, seals |

A host and a receiver read and write these topics as their grants allow,
beside the platform, object storage and git remote a run uses.

![wherry's implementation: a host, the Kafka topics, a receiver with its plugins, a platform job, object storage and a git remote](diagrams/implementation.svg)

`wherry.admin` records, progress records and seals carry no signature: each
sits on a topic only one principal can write, so Kafka authenticates it. Every other record, key
records included, is a versioned EdDSA
[JWS](https://datatracker.ietf.org/doc/html/rfc7515). A record on a shared topic
names its owner and key in its `kid`, and a reader verifies it against that
owner's current key, read from the owner's own key topic, before it decodes the
payload. A host reading its tenant feed and transcript topic, and a receiver
reading its pool's statuses, also accept a key the owner has rotated away,
for records the broker appended before that rotation. No host or receiver
can vouch for another's key.

A reader then checks the record against `wherry.admin`. A submission or
cancel must come from a live host of a tenant entitled to the pool whose
topic it arrived on. A receiver record must come from a live member of the
pool the run was placed in, on behalf of a tenant entitled to it.
[The wherry A2A extension](a2a-extension-v1.md) specifies every record.

Every topic has one partition and `LogAppendTime` timestamps, and no topic
is deleted. A topic is compacted only when its readers need the latest
record per key and nothing else, and no principal but the record's owner can
write that key, or a superseded record costs nothing: `wherry.receivers` and
the progress topics have `cleanup.policy=compact,delete`, every other topic
`cleanup.policy=delete`. A pool's task and event topics and its receivers'
progress topics keep records for its maximum queue deadline plus six hours
plus a day, 15 days 6 hours by default, the tenant feed and transcript
topic for 30 days and `wherry.receivers` for 7; the identity and
configuration topics keep them forever.

A deployment has 2 + 2R + 2P + 2T + H topics for R receivers, P pools, T
tenants and H hosts. The count grows with configuration, never with runs.

Every principal reads all of `wherry.admin`, so tenants see each other's
names, hosts, entitlements and pools.

## Configuration delivery

A receiver's configuration is a record on `wherry.admin`: its capacity
(the runs it holds at once) and its plugin profiles, each with a name, its plugin's
type, an agent, labels and its merged settings for the plugin. It holds no
secrets and names no agent credential: every profile of an agent delivers
the agent's credentials as the pool's terms give them. A profile's settings
may name a platform credential, whose source the receiver's local
configuration holds. `wherry admin` refuses a configuration that names an
agent outside the pool's terms.

A receiver starts with its local configuration, which names its bundle, a
state directory, a plugin directory and the sources of its platform
credentials. It registers its signing key, waits for its configuration
record, validates it against the terms and configures its profiles. It then advertises every 30 seconds.
When a new configuration record appears, the receiver stops and exits, so its
platform restarts it with the new configuration. Runs in flight are adopted on
redelivery.

## Kafka resources

### Topics

`wherry admin` creates each topic with one partition, the broker's default
replication factor, `cleanup.policy=delete` and
`message.timestamp.type=LogAppendTime`, except `wherry.receivers` and the
progress topics, which it creates with `cleanup.policy=compact,delete`,
`segment.ms=3600000` and `max.compaction.lag.ms=3600000`, so a segment
closes and compacts within an hour.

| Topic | Created by | `retention.ms` |
| --- | --- | --- |
| `wherry.admin` | any recording command | `-1` |
| `wherry.receivers` | any recording command | `604800000` (7 days) |
| `wherry.pool.<pool>.tasks` | `pool create` | `max_queue_deadline` plus 30 hours, in milliseconds; default `1317600000` |
| `wherry.pool.<pool>.events` | `pool create` | at least tasks; `pool create` sets the same value |
| `wherry.tenant.<tenant>` | `pool entitle`, `host add` | `2592000000` (30 days) |
| `wherry.tenant.<tenant>.transcript` | `pool entitle`, `host add` | as the tenant feed, `2592000000` (30 days) |
| `wherry.receiver.<receiver>` | `receiver add` | `-1` |
| `wherry.receiver.<receiver>.progress` | `receiver add`, `pool create` | as the pool's tasks topic |
| `wherry.host.<tenant>.<host>` | `host add` | `-1` |

The tasks retention keeps `max_queue_deadline`, the six-hour maximum run time,
and a one-day margin. Events and the pool's progress topics stay at least as
long as tasks. A tenant feed and its transcript topic must stay for at least
`max_queue_deadline` plus six hours; their 30-day default exceeds the
default requirement of `1231200000`. Identity and configuration topics stay
at `-1`, and `wherry.receivers` remains a seven-day liveness topic.

`wherry admin` leaves an existing topic's configuration as it is, except
that `pool create` sets the retention and timestamp type of the pool's tasks
and events topics, and `receiver add` and `pool create` set every
configuration above on the progress topics of the pool's live receivers. A
tenant feed's or transcript topic's retention can be changed with Kafka's
own tools. `pool create` and `pool entitle` refuse terms that need more than
an entitled tenant's feed or transcript topic keeps, and `wherry admin
check` reports a topic below its bound, a progress topic that keeps less
than its pool's tasks topic, and a pool topic whose timestamp type is not
`LogAppendTime`.

### ACLs

Every grant allows from any host. Resources are literal except the
transactional ID prefix.

| Principal | Resource | Operations |
| --- | --- | --- |
| `wherry.operator` | topic `wherry.admin` | Read, Write, Describe |
| host | topic `wherry.admin` | Read, Describe |
| host | topic `wherry.receivers` | Read, Describe |
| host | topic `wherry.tenant.<own tenant>` | Read, Describe |
| host | topic `wherry.tenant.<own tenant>.transcript` | Read, Describe |
| host | topic `wherry.host.<own tenant>.<own host>` | Read, Write, Describe |
| host | topics `wherry.pool.<p>.tasks` and `.events`, each entitled pool | Write, Describe |
| host | topic `wherry.receiver.<r>`, each receiver of an entitled pool | Read, Describe |
| receiver | topic `wherry.admin` | Read, Describe |
| receiver | topic `wherry.receiver.<own>` | Read, Write, Describe |
| receiver | topic `wherry.receiver.<own>.progress` | Read, Write, Describe |
| receiver | topic `wherry.receiver.<peer>`, each other member of its pool | Read, Describe |
| receiver | topic `wherry.receiver.<peer>.progress`, each other member of its pool | Read, Describe |
| receiver | topic `wherry.receivers` | Read, Write, Describe |
| receiver | topic `wherry.pool.<own pool>.tasks` | Read, Describe |
| receiver | topic `wherry.pool.<own pool>.events` | Read, Write, Describe |
| receiver | group `p-<own pool>` | Read, Describe, DescribeConfigs |
| receiver | transactional ID prefix `p.<own pool>.` | Write, Describe |
| receiver | topic `wherry.tenant.<t>`, each tenant entitled to its pool | Write, Describe |
| receiver | topic `wherry.tenant.<t>.transcript`, each tenant entitled to its pool | Write, Describe |
| receiver | topic `wherry.host.<t>.<h>`, each host of those tenants | Read, Describe |

A principal whose membership has ended has no grants.
