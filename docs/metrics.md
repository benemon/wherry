# Metrics reference

Set `metrics_addr` in the receiver's local configuration to serve `/metrics`.
Use [Receiver metrics](operations-receivers.md#metrics) to configure scraping, dashboards and alerts.
[Command reference](commands.md#metrics) describes the listener.

## Labels

Every label value comes from the receiver's configuration, its pool's terms and
entitlements, or a fixed list. No label or value carries a run ID, prompt,
output, error text, URL, image, path, node or credential name. An agent the
pool's terms do not name is `other`. `model` is the placement's model when the
agent's `models` in the terms hold it, `other` when they do not, and empty for
an agent with no `models`. A `state` outside the A2A task states is `other`.
A run ended without a plugin profile, one whose profile went unready between
its claim's subscription and its fetch and that was given back at the
delivery budget, has `profile="none"` in `wherry_runs_total` and
`wherry_delivery_budget_exhausted_total`.
`topic` is the configured profile's signature task topic: its pool and agent in
clear, followed by the SHA-256 of the canonical agent-and-label signature
encoding. It carries configuration, not placement values; the deterministic
hash is not secret protection.

## Families

| Family | Type | Labels | Counts |
| --- | --- | --- | --- |
| `wherry_claims_total` | counter | `tenant` | claimed tasks whose submission verifies |
| `wherry_admissions_total` | counter | `tenant`, `profile` | tasks admitted, by the profile they were routed to |
| `wherry_rejections_total` | counter | `tenant`, `reason` | rejections, by reason |
| `wherry_runs_total` | counter | `tenant`, `profile`, `agent`, `model`, `state` | runs ended `completed`, `failed` or `canceled`, except a run whose terminal's commit had an unknown outcome and committed, which no receiver counts |
| `wherry_delivery_budget_exhausted_total` | counter | `tenant`, `profile` | runs ended `failed` with `delivery budget exhausted` |
| `wherry_run_duration_seconds` | histogram | `tenant`, `profile`, `agent`, `model`, `state` | time from this attempt's claim to the run's terminal state; an adopted run counts the adopting attempt only, and a run whose terminal's commit had an unknown outcome and committed is not observed |
| `wherry_queue_wait_seconds` | histogram | `tenant`, `profile` | time from submission to first claim |
| `wherry_adoptions_total` | counter | `tenant`, `profile` | runs that adopted an earlier attempt's job |
| `wherry_redeliveries_total` | counter | `tenant`, `profile` | runs executed on a redelivery |
| `wherry_in_flight` | gauge | `profile` | tasks held |
| `wherry_capacity` | gauge | none | the configured capacity |
| `wherry_runs_past_deadline` | gauge | `profile` | tasks held past their task_ended_by that have not ended |
| `wherry_profile_ready` | gauge | `profile`, `topic` | 1 when the profile is in the receiver's current ready offers; 0 otherwise |
| `wherry_claim_gated` | gauge | `reason` | 1 while the restriction prevents a new poll: `suspended`, `no_ready_offer`, `pending_run` |
| `wherry_advertised_timestamp_seconds` | gauge | none | Unix time of the latest advertisement Kafka acknowledged, 0 before the first |
| `wherry_start_failures_total` | counter | `profile`, `class` | start failures: `input-too-large`, `dispatch`, `platform` |
| `wherry_dispatch_outcomes_total` | counter | `profile`, `outcome` | dispatches: `created`, `nothing`, `unknown` |
| `wherry_stop_failures_total` | counter | `profile` | failed `Stop` calls |
| `wherry_purge_failures_total` | counter | `profile` | failed `Purge` calls |
| `wherry_sweep_failures_total` | counter | `profile` | failed `Sweep` calls |
| `wherry_collection_failures_total` | counter | `profile`, `reason` | runs whose outputs were not collected: `not_stopped`, `over_limits`, `timeout`, `failed` |
| `wherry_open_waits_total` | counter | `tenant`, `outcome` | open request waits: `opened`, `refused`, `unanswered`, `expired` |
| `wherry_open_wait_seconds` | histogram | `tenant` | time a claimed run waited for its attempt wrap or refusal |
| `wherry_redactions_total` | counter | `tenant`, `profile` | credential values replaced in published output |
| `wherry_bus_errors_total` | counter | `op` | Kafka errors: `read`, `write`, `transactional ID`, `share group` |
| `wherry_denials_total` | counter | `op` | Kafka authorisation errors, by the capability denied: `read`, `write`, `transactional ID`, `share group`, `membership` |
| `wherry_renewal_failures_total` | counter | none | failed claim renewals |
| `wherry_transaction_aborts_total` | counter | none | transactions aborted after a failed produce |
| `wherry_ambiguous_commits_total` | counter | none | commits with an unknown outcome: a run gives its task back for redelivery, an open request is answered again only when its answer is absent, and the key custodian reads back its snapshot topic |
| `wherry_fenced_total` | counter | none | runs given up to a newer holder |
| `wherry_custodian_takeovers_total` | counter | `reason` | snapshots this receiver published taking over its pool's key custody: `takeover`, `contested` |
| `wherry_runs_key_lost_total` | counter | none | runs this receiver ended with the wherry event `key-lost` |
| `wherry_claimant_changes_total` | counter | `reason` | claims the key custodian served to a new claimant: `released`, `lease` |
| `wherry_ignored_wraps_total` | counter | none | wraps whose key does not decrypt the task's envelope |
| `wherry_metrics_dropped_total` | counter | `profile`, `reason` | plugin samples dropped and `Metrics` calls that failed |
| `wherry_plugin_platform_errors_total` | counter | `profile`, `class` | platform API errors: `unavailable`, `denied`, `conflict`, `timeout`, `other` |
| `wherry_plugin_cleanup_pending` | gauge | `profile` | the profile's ended jobs awaiting cleanup |
| `wherry_plugin_sweep_deletions_total` | counter | `profile`, `kind` | material a sweep deleted: `job`, `variable`, `directory` |
| `wherry_plugin_placement_seconds` | histogram | `profile` | time from Dispatch returning to the first observation of the agent task started, by the receiver's clock |
| `wherry_plugin_storage_free_bytes` | gauge | `profile` | free bytes of the storage the profile's runs share |

The other `wherry_plugin_` families come from the plugins. The receiver
observes `wherry_plugin_placement_seconds` itself, only for an attempt whose
Dispatch returned the job created, so an adopter observes nothing. It drops a
plugin's sample of it and counts the sample in `wherry_metrics_dropped_total`.

## Reasons and outcomes

A rejection's `reason` is its fixed text, such as `duplicate submission`. A
reason that quotes the task counts as `git remote host outside the pool's vcs
hosts`, `pool has no storage`, `not started by task_started_by` or
`not ended by task_ended_by`.

| Open wait outcome | Meaning |
| --- | --- |
| `opened` | The attempt wrap arrives. |
| `refused` | The custodian refuses the request. |
| `unanswered` | No answer arrives within five minutes and 120 seconds of the request's append time. |
| `expired` | The start bound passes, or the end bound passes for an adopted job. |

An unanswered request releases the task.

## Histogram buckets

The core histograms' buckets are 1, 5, 15, 30 and 60 seconds, 2, 5, 10 and 30
minutes, and 1, 2, 4 and 6 hours. `wherry_plugin_placement_seconds` uses
`pluginsdk.PlacementBuckets`: 1, 2, 5, 10 and 30 seconds, and 1, 2, 5, 10 and 30
minutes.

## Series created at zero

At construction, the receiver creates `bus_errors_total` for `read`, `write`,
`transactional ID` and `share group`; `denials_total` for those operations plus
`membership`; and `claimant_changes_total` for `released` and `lease`.
`custodian_takeovers_total` also starts at zero for `takeover` and `contested`,
as do the unlabelled core counters.

After successful startup and subsequent state refreshes, before publishing the
new state, the receiver creates these core children (all names have the
`wherry_` prefix). Histograms have zero buckets, count and sum, with no observation.

| Families | Seeded labels besides pool |
| --- | --- |
| `claims_total` | entitled tenant |
| `admissions_total`, `redeliveries_total`, `queue_wait_seconds` | tenant × (configured profiles union {empty profile}) |
| `adoptions_total` | tenant × configured profiles |
| `delivery_budget_exhausted_total`, `redactions_total` | tenant × (configured profiles union {`none`}) |
| `runs_total`, `run_duration_seconds` | tenant × configured (profile, agent, model) × `completed`, `failed`, `canceled` |
| `rejections_total` | tenant × the 18 rejection reasons below |
| `open_waits_total` | tenant × `opened`, `refused`, `expired`, `unanswered` |
| `open_wait_seconds` | tenant |
| `start_failures_total` | profile × `input-too-large`, `dispatch`, `platform` |
| `dispatch_outcomes_total` | profile × `created`, `nothing`, `unknown` |
| `collection_failures_total` | profile × `failed`, `not_stopped`, `over_limits`, `timeout` |
| `metrics_dropped_total` | profile × `timeout`, `error`, `duplicate`, `unknown_family`, `unknown_label`, `kind`, `label_value`, `value` |
| `stop_failures_total`, `purge_failures_total`, `sweep_failures_total`, `plugin_placement_seconds` | profile |

A tenant is seeded only while entitled to this receiver's pool. Each configured
profile contributes the distinct models of its agent's first entry in the current
pool terms, or the empty model when that entry lists none; a profile whose agent
the terms do not name contributes no run tuples. Sentinel unions are deduplicated.
The run matrix is a partial contract: runtime placement and current terms can
produce other tuples. Run families for the `none` profile, agent and model
`other` fallbacks, state `other`, and other valid task states stay lazy. A
configured profile literally named `none`, or agent or model named `other`,
is seeded normally. Plugin-emitted families stay lazy and belong to the plugin;
`plugin_placement_seconds` belongs to the core. Gauges read current state.

The rejection reasons are `run ID does not match its signer`, `duplicate
submission`, `agent is outside the pool terms`, `model not in the pool's terms`,
`not started by task_started_by`, `not ended by task_ended_by`, `agent does not
match the task topic`, `selector does not match the task topic`,
`task_started_by is past the pool's start_by_limit`, `the host does not supply
a required credential of the agent`, `a task credential takes the target of
one of the agent's credentials`, `git remote host outside the pool's vcs hosts`,
`pool has no storage`, `unsupported message part`, `the task carries no envelope`,
`task_started_by is before its append time`, `task_ended_by is before
task_started_by`, and `task_ended_by is past task_started_by plus the 6h run cap`.

Prometheus must scrape the zero before an event for its first increase to count.
An event before the first scrape after startup, a new entitlement or a terms
change can still be missed. The receiver does not wait for a scrape. Children
are never reset or deleted: tenants first seen ended get no children, while
children created during entitlement remain until the process ends.

For T entitled tenants and M the sum over profiles of distinct seeded
(agent, model) pairs, run counters add 3TM classic text series and run duration
histograms add 48TM, excluding optional `_created` series. With P distinct
profiles, A = |profiles union {empty}| and B = |profiles union {none}|, all
seeded families in the table plus the construction domains add
`11 + 34P + T(39 + 18A + P + 2B + 51M)` series, or
`11 + 34P + T(59 + 21P + 51M)` without sentinel collisions. Existing takeovers
add two; unlabelled counters, gauges and plugin metrics are additional.
This is one snapshot's contribution per receiver. The retained population is
the union across entitlement and terms changes plus lazy runtime tuples.
