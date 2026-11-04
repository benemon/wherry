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
A run ended without a plugin profile, one given back at the delivery budget
before any profile took it, has `profile="none"` in `wherry_runs_total` and
`wherry_delivery_budget_exhausted_total`.

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
| `wherry_advertised_timestamp_seconds` | gauge | none | Unix time of the latest advertisement Kafka acknowledged, 0 before the first |
| `wherry_start_failures_total` | counter | `profile`, `class` | start failures: `input-too-large`, `dispatch`, `platform`, and `offer-unavailable`, which counts a release by a member with no matching profile, has no start-failure record and carries `profile="none"` |
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
| `wherry_custodian_takeovers_total` | counter | `reason` | snapshots this receiver published taking over its pool's key custody: `takeover`, `contested`; both start at 0 |
| `wherry_runs_key_lost_total` | counter | none | runs this receiver ended with the wherry event `key-lost`; starts at 0 |
| `wherry_claimant_changes_total` | counter | `reason` | claims the key custodian served to a new claimant: `released`, `lease` |
| `wherry_ignored_wraps_total` | counter | none | wraps whose key does not decrypt the task's envelope |
| `wherry_metrics_dropped_total` | counter | `profile`, `reason` | plugin samples dropped and `Metrics` calls that failed |
| `wherry_plugin_platform_errors_total` | counter | `profile`, `class` | platform API errors: `unavailable`, `denied`, `conflict`, `timeout`, `other` |
| `wherry_plugin_cleanup_pending` | gauge | `profile` | the profile's ended jobs awaiting cleanup |
| `wherry_plugin_sweep_deletions_total` | counter | `profile`, `kind` | material a sweep deleted: `job`, `variable`, `directory` |
| `wherry_plugin_placement_seconds` | histogram | `profile` | time from dispatch to the agent task starting |
| `wherry_plugin_storage_free_bytes` | gauge | `profile` | free bytes of the storage the profile's runs share |

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

