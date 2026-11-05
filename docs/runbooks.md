# Runbooks

Use the section named by the firing alert.
The [shipped rules and dashboard](https://github.com/benemon/wherry/tree/main/contrib/observability) link to these sections.
[Receiver metrics](operations-receivers.md#metrics) explains installation.
[Metrics reference](metrics.md) lists the metric families.

Read receiver logs through your platform:

| Platform | Command |
| --- | --- |
| Nomad | `nomad alloc logs` with the receiver allocation. |
| Kubernetes or OpenShift | `kubectl logs` with the receiver pod. |
| systemd | `journalctl -u` with the receiver unit. |

Each receiver log line starts with `receiver: ` and a UTC timestamp.
Run host checks from a tenant entitled to the pool.
Use that host's [configuration](configuration-host.md#client-configuration).
Run operator checks from the [operator working directory](operations-operator.md#set-up-wherry-operator).

## WherryReceiverNotAdvertising

A receiver advertises its offers on Kafka every 30 seconds, and hosts place
tasks only on a receiver whose latest advertisement is under 120 seconds
old. `wherry_advertised_timestamp_seconds` holds the time of the latest
advertisement Kafka acknowledged. The alert fires when one of these holds
for 5 minutes:

- That time is more than 120 seconds old: advertisements fail, or the
  receiver has never advertised since it read its configuration.
- Prometheus cannot scrape the receiver (`up` is 0): the process is down,
  or its `metrics_addr` is unreachable.
- The process runs but has not read its configuration: `wherry_build_info`
  is served and `wherry_advertised_timestamp_seconds` is not.

[A receiver that is not running](operations-receivers.md#a-receiver-that-is-not-running)
covers a receiver whose scrape target service discovery has dropped. An
alert on the platform's series it lists follows this runbook.

Confirm it:

1. Run `wherry host receivers`. The receiver's `status` is `stale` or `silent`,
   or `ended` after a revocation, and `advertisement_age` shows the age of its
   latest advertisement.
2. Read the receiver's log.
   - `waiting for configuration` repeats while `wherry.operator` holds no
     configuration for the receiver.
   - `advertise: <error>` and `periodic step: <error>` name a failed
     advertisement and its cause.
   - `stopping: denied write: <error>` or
     `stopping: denied membership: <error>` name the authorisation failure
     the receiver exited for. The supervisor restarts it, and a receiver
     with revoked or wrong credentials exits again each time.
   - `stopping: plugin <type> exited` names a plugin whose process exited,
     or was killed after `plugin did not answer a ping; killing it`; the
     plugin's own lines before it give the cause.
   - `configuration changed; exiting once held runs end` comes before a
     restart the supervisor is expected to make, once the runs the
     receiver holds have ended.
3. Run `wherry operator receiver show RECEIVER` to check the receiver has a
   configuration.

Then:

- For a stopped process, or a receiver the platform no longer runs, check
  the supervisor and restart the receiver, as in
  [Run a receiver](operations-receivers.md#run-a-receiver).
- For `waiting for configuration`, record the configuration, as in
  [Add a receiver](operations-operator.md#add-a-receiver).
- For write errors, follow [WherryBrokerUnreachable](#wherrybrokerunreachable).
  For `denied write`, run `wherry operator check`, which confirms that every
  ACL the state calls for exists.
- For `plugin <type> exited`, fix the cause the plugin's lines give, such as
  an unreachable platform API or memory pressure on the receiver host. For
  a protocol version mismatch, record the pin of the plugin release that
  speaks the receiver's protocol and install it, as in
  [Install plugins](operations-receivers.md#install-plugins). The
  runs the receiver held are adopted once their tasks are delivered again.
- A receiver whose membership has ended stays ended. Provision a new
  receiver under a new name and stop the old process.

## WherryRunStuckPastDeadline

At a run's `task_ended_by` the receiver stops the run's job and ends the run
`TASK_STATE_FAILED` with `not ended by task_ended_by (<RFC 3339 time>)`. When the plugin's `Stop` fails, the
receiver returns the task to the pool instead, with the job still running
and the run still `TASK_STATE_WORKING`. The next delivery looks the job up
and stops it, until the delivery budget ends the run and leaves the job to
the plugin's sweep. `wherry_runs_past_deadline` counts, by profile, the
tasks a receiver holds whose run's `task_ended_by` has passed and that have not
ended. The alert fires when it stays above 0 for 10 minutes: a receiver has
held a run past its `task_ended_by` that long, as when a `Stop` call or the run's
terminal commit does not return. A task the receiver returns to the pool
leaves the count until a receiver holds it again. A receiver that is down
stops nothing either; its runs are adopted on redelivery, and
[WherryReceiverNotAdvertising](#wherryreceivernotadvertising) covers it.

Confirm it:

1. Run `wherry status`. A stuck run is listed `TASK_STATE_WORKING`.
2. Run `wherry status RUN_ID` and `wherry events RUN_ID`. `events` prints
   the run's statuses by attempt, each with its receiver and time; a run
   whose job cannot be stopped shows attempts after its `task_ended_by`.
3. Read the receiver's log for `run <id>: stop: <text>`, and for the
   plugin's `op=<op> class=<class>: <text>` lines.
4. Check the job on the platform with the plugin's own tools.

Then:

- Restore the plugin's access to its platform, such as an expired platform
  credential or a missing permission in the plugin's policy. The plugin's
  README lists the permissions:
  [Nomad](https://github.com/benemon/wherry-plugins/tree/main/nomad#acl-policy),
  [Kubernetes](https://github.com/benemon/wherry-plugins/tree/main/kubernetes#rbac)
  and [systemd](https://github.com/benemon/wherry-plugins/tree/main/systemd#privileges).
- When the platform cannot stop the job, stop it with the platform's tools.
  The next delivery finds no running job and ends the run. A
  `wherry cancel` also ends the run through the plugin's `Stop`, so it
  waits on the same fix.

## WherryDeliveryBudgetExhausted

Each start failure returns a task to the pool and spends one of its pool's
`delivery_limit` deliveries, as does each release of a task whose profile
went unready between its claim's subscription and its fetch. From attempt
`delivery_limit` minus 1, a receiver ends the run `TASK_STATE_FAILED` with
`delivery budget exhausted` instead.
`wherry_delivery_budget_exhausted_total` counts these runs by tenant and
profile. The alert fires on any such run in the last hour, for each tenant
and profile:

- `profile="none"`: runs given back at the budget because the profile of
  their signature went unready on the member that fetched them;
- any other profile: runs the profile took and could not start, or
  returned to the pool after another failure, such as a failed job lookup
  or `Stop` call.

Confirm it:

1. Run `wherry status RUN_ID` for a failed run. Its message reads
   `delivery budget exhausted`, or
   `delivery budget exhausted after <n> attempts; last: <cause>` naming the
   last start failure.
2. Run `wherry events RUN_ID` to see each attempt and the receiver that
   made it.
3. Read the receivers' logs for
   `run <id>: no ready profile for topic <topic>; releasing`,
   `run <id>: dispatch failed: <text>` and
   `run <id>: job never started (<class>): <reason>`.

Then:

- For releases of an unready profile, restore the profile's readiness, the
  `readiness: not ready` lines name it.
- For start failures, fix the cause the log names in the plugin's platform,
  such as a missing image, an unschedulable node pool or a full namespace
  quota.
- Size `delivery_limit` by the attempts one task may burn plus a margin, as
  in [Create a pool](operations-operator.md#create-a-pool), never by the
  number of members.
- Resubmit the failed runs once the cause is fixed.

## A task unclaimed on a signature nobody serves

A task waits on its signature's topic, spending no delivery, until a member
with a ready profile of that signature fetches it. `wherry wait` reports one
that none fetched by its `task_started_by` with
`not started by task_started_by (<RFC 3339 time>): no receiver claimed the task`.

1. Run `wherry host receivers` and check that a `live` receiver of the pool
   has a profile with `ready=true` for the run's agent whose labels satisfy
   its selector over the `routable=` keys.
2. Run `wherry operator receiver show RECEIVER` for that receiver: the
   profile's `topic` is the signature topic it subscribes to.

Then start or reconfigure a receiver with a profile of that signature, or
resubmit with a selector a live signature satisfies. A replacement of a
key-lost run keeps its original signature's topic.

## `operator check` names a missing signature topic

`wherry operator check` fails with
`receiver <receiver> profile <profile>: signature topic <topic> is missing; pool apply <pool> with the same terms creates it`
when a live receiver's profile has no task topic under the pool's current
terms, as after two operator commands applied at once. Run
`wherry operator pool apply <pool>` with the pool's recorded terms, then
`wherry operator check` again.

## WherryOpenRequestsUnanswered

A receiver that claims a run writes an open request on the pool's events,
and the pool's key custodian answers it with an attempt wrap or a refusal.
A receiver whose request no key custodian answers within 5 minutes and 120
seconds logs `run <id>: no answer to the open request for attempt <n>` and
releases the task to the pool for another delivery. The alert fires on 2
or more `unanswered` open waits (`wherry_open_waits_total`) for one tenant
in a pool in an hour.

Confirm it:

1. Read the claiming receivers' logs for
   `run <id>: no answer to the open request for attempt <n>`.
2. Find the pool's key custodian from its receivers' log lines, as in step 1
   of [Stop a pool's key custodian](operations-receivers.md#stop-a-pool-s-key-custodian),
   and check that it is `live` in `wherry host receivers`.
3. Read the pool's receivers' logs for lines starting `custodian:`, such
   as `custodian: published pool <pool> key <id> (<reason>)`,
   `custodian: lost the custodian group's assignment for pool <pool>` and
   `custodian group: <cause>`, and for Kafka denials
   (`wherry_denials_total`).

Then:

- Start or restart a receiver of the pool, so that the custodian group
  assigns a live member and it publishes its snapshot.
- Fix the broker access a denial names, as in
  [Prepare Kafka](operations-operator.md#prepare-kafka).

## WherryCustodianTakeover

The custodian group assigns a pool's key custody to one receiver. A
receiver that becomes custodian after another, whether the previous
custodian drained in a planned stop, crashed or lost its state directory,
publishes a `takeover` snapshot with a new pool key. A draining custodian
re-keys its tasks to the new key; otherwise the previous custodian's tasks
that never started fail key-lost unless a receiver holding the previous key
re-keys them, and their hosts resubmit them before their
`task_started_by`. A custodian whose producer is fenced while the
group still assigns it the partition re-initialises its transactional ID
and republishes its snapshot as `contested`; a second fence within the
45-second session timeout stands it down until it is assigned again. The
alert fires on any increase of `wherry_custodian_takeovers_total` within 15
minutes, by `reason`, `takeover` or `contested`. `bootstrap` and `rotation`
snapshots do not count.

Every custodian change raises it, a planned drain's included, so expect it
at each planned stop of a custodian. It also signals a compromised receiver
taking custody through the custodian group
([What the pool key protects](security.md#what-the-pool-key-protects)).
Treat a takeover that no planned stop or crash explains as a compromise.

Confirm it:

1. Read the receiver's log for
   `custodian: took over pool <pool> (WherryCustodianTakeover)` or
   `custodian: fenced while assigned in pool <pool>; re-initialising to republish (WherryCustodianTakeover)`,
   and for
   `custodian: fenced again within the session timeout in pool <pool>; standing down until assigned again`.
2. Read the previous custodian's log for its stop: a planned drain logs
   `custodian: stepping down in pool <pool>; waiting up to <timeout> for a successor`,
   and a stop with `drain_timeout` at `0s` logs
   `custodian: leaving pool <pool> with no drain timeout; keeping the pool keys to their deadlines`.
3. Find the pool's key custodian from its receivers' log lines, as in step 1
   of [Stop a pool's key custodian](operations-receivers.md#stop-a-pool-s-key-custodian),
   and check that it is `live` in `wherry host receivers`.

Then:

| Cause | Action |
| --- | --- |
| a planned stop whose previous custodian logged `custodian: re-keyed <n> tasks to pool key <id> and deleted <n> pool keys` | none |
| a crash or a lost state directory | none: the hosts of the lost tasks resubmit them when they next run |
| a planned stop with no re-key | raise `drain_timeout`, so that the stopping custodian waits for a successor's snapshot and re-keys its tasks, as in [Stop a pool's key custodian](operations-receivers.md#stop-a-pool-s-key-custodian) |
| no planned stop, crash or lost state directory before the takeover | treat the new custodian as suspect and revoke it with `wherry operator receiver remove` |
| a `contested` snapshot with no custodian change | find the writer that initialised `p.<pool>.custodian`, which every member of the pool can; revoke a suspect receiver with `wherry operator receiver remove` |

## WherryRunsKeyLost

A run whose envelope no holder of a pool key can open cannot start. A
receiver ends such a run that never started `TASK_STATE_FAILED` with the
wherry event `key-lost`, and counts it in `wherry_runs_key_lost_total`,
whatever custody change came before: a takeover whose previous custodian
did not re-key its tasks, a drain whose custodian exited before re-keying,
or a task appended under an old key after a drain's scan. Its host resubmits it before its
`task_started_by` ([Lost pool keys](concepts.md#lost-pool-keys)). A run
whose job had started ends with the same message without `key-lost`, and
counts nothing. The alert fires on any increase within 15 minutes.

Confirm it:

1. Read the receiver's run log for terminal statuses with the message
   `cannot be opened: the pool's custodian changed and its key was lost`.
2. Read the receivers' logs for the custody change before it: a
   `custodian: took over pool <pool> (WherryCustodianTakeover)` line, or a
   draining custodian's
   `custodian: hand over to <receiver>: <error>; keeping the pool keys to their deadlines`
   or `custodian: no successor in pool <pool>; keeping the pool keys to their deadlines`.

Then:

- The hosts resubmit each run before its `task_started_by` when `submit` or
  `status` next runs there, or `wait`, `logs -f` or `status` for the run, or
  the MCP server's `submit`, `status` or `wait`
  ([Lost pool keys](concepts.md#lost-pool-keys));
  `wherry status RUN_ID` then shows `replaced_by=<run>`. A run past its
  `task_started_by`, or whose host does not come back, is submitted again by
  hand.
- For losses at planned stops, set `drain_timeout` and the platform's stop
  window as in
  [Drain timeout and the stop window](configuration-receiver.md#drain-timeout-and-the-stop-window),
  and stop a pool's receivers one at a time, as in
  [Stop a pool's key custodian](operations-receivers.md#stop-a-pool-s-key-custodian),
  so that a stopping custodian has a live successor.

## WherryPlatformErrorsRising

A plugin counts its failed platform API calls by class:
`unavailable`, `denied`, `conflict`, `timeout` and `other`
(`wherry_plugin_platform_errors_total`). The alert fires when one profile
counts 5 or more of one class in 10 minutes, for 10 minutes. Failed calls
fail dispatches, stops, purges and sweeps, and raise start failures and
`wherry_plugin_cleanup_pending`.

Confirm it:

1. Read the receiver's log for `op=<op> class=<class>: <text>` lines, and
   for the plugin's own warning, `nomad call failed`,
   `kubernetes call failed` or `systemd call failed`, naming the profile,
   operation and class.
2. Run `wherry host receivers`. A profile whose readiness call fails shows
   `ready=false`.
3. Check the platform's API health with its own tools.

Then:

- `denied`: renew the platform credential at its source and restart the
  receiver, as in [Add a receiver](operations-operator.md#add-a-receiver), or
  restore the plugin's permissions.
- `unavailable` and `timeout`: restore the platform API or the receiver
  host's route to it.
- `conflict`: the platform refused a change that conflicts with the
  object's current state. Read the operation the log line names and the
  object on the platform.
- Each plugin's README states how it classes errors:
  [Nomad](https://github.com/benemon/wherry-plugins/tree/main/nomad#metrics),
  [Kubernetes](https://github.com/benemon/wherry-plugins/tree/main/kubernetes#metrics)
  and [systemd](https://github.com/benemon/wherry-plugins/tree/main/systemd#metrics).

## WherryBrokerUnreachable

The receiver reads the pool's topics, claims tasks through its share group
and renews each claim every 10 seconds. A receiver that cannot renew a
claim loses it after the pool's `lock_duration`, and the broker hands the
task to another member. The alert fires when, for 5 minutes, a receiver
counts 3 or more Kafka read or share group errors
(`wherry_bus_errors_total` with `op` `read` or `share group`) or failed
claim renewals (`wherry_renewal_failures_total`) in 5 minutes.

Confirm it:

1. Run `wherry host receivers`. A receiver that cannot write its advertisements
   turns `stale`.
2. Read the receiver's log for `periodic step: <error>`,
   `run <id>: renew: <error>`, `run <id>: read events: <error>` and
   `stopping: denied <op>: <error>`. At an authorisation error the receiver
   exits, naming the operation denied, and its supervisor restarts it; the
   runs it held are adopted from the pool once their tasks are delivered
   again.
3. From the receiver host, check the broker address and port in the
   receiver's bundle are reachable over TLS.

Then:

- Restore the route from the receiver host to the broker's TLS listener,
  or the brokers themselves.
- For a TLS or SCRAM error, check the receiver's `ca_file`, or the system
  roots when it names none, against the listener's certificate, and the
  bundle's principal and password against the cluster's SCRAM credentials.
- For `denied <op>`, run `wherry operator check` to confirm the ACLs the state
  calls for exist, and fix the bundle's credentials; until then the
  receiver restarts and exits again.
- A task whose claim lapsed during the outage is redelivered, and its next
  holder adopts the running job.
