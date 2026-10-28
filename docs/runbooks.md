# Runbooks

One section per rule of the shipped Prometheus alert rules. Each rule's
`runbook_url` links its section. The rules and the Grafana dashboard live in
[`contrib/observability`](https://github.com/benemon/wherry/tree/main/contrib/observability);
[Metrics](operations.md#metrics) says how to scrape a receiver and load them,
and [Metrics](commands.md#metrics) lists every family.

Each section says what the alert means, how to confirm it, and what to do.
The receiver logs to standard output, one line per event, each prefixed
`receiver: ` and a UTC timestamp; on Nomad, `nomad alloc logs` shows them,
and under systemd, `journalctl -u` with the receiver's unit. The confirming
commands run on a host of a tenant entitled to the pool, with its
[client configuration](configuration.md#client-configuration), except
`wherry admin` commands, which need the
[`wherry admin` environment](operations.md#set-up-wherry-admin).

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

Confirm it:

1. Run `wherry receivers`. The receiver's `status` is `stale` or `silent`,
   or `ended` after a revocation, and `heartbeat_age` shows the age of its
   latest advertisement.
2. Read the receiver's log.
   - `waiting for configuration` repeats while `wherry.admin` holds no
     configuration for the receiver.
   - `advertise: <error>` and `periodic step: <error>` name a failed
     advertisement and its cause.
   - `denied (write): <error>` or `denied (membership): <error>` name an
     authorisation failure.
   - `configuration changed; exiting` comes before a restart the
     supervisor is expected to make.
3. Run `wherry admin receiver show RECEIVER` to check the receiver has a
   configuration.

Then:

- For a stopped process, check the supervisor and restart the receiver,
  as in [Run a receiver](operations.md#run-a-receiver).
- For `waiting for configuration`, record the configuration, as in
  [Add a receiver](operations.md#add-a-receiver).
- For write errors, follow [WherryBrokerUnreachable](#wherrybrokerunreachable).
  For `denied (write)`, run `wherry admin check`, which confirms that every
  ACL the state calls for exists.
- A receiver whose membership has ended stays ended. Provision a new
  receiver under a new name and stop the old process.

## WherryRunStuckPastDeadline

At a run's deadline the receiver stops the run's job and ends the run
`TASK_STATE_FAILED` with `timeout`. When the plugin's `Stop` fails, the
receiver returns the task to the pool instead, with the job still running
and the run still `TASK_STATE_WORKING`. The next delivery looks the job up
and stops it, until the delivery budget ends the run and leaves the job to
the plugin's sweep. `wherry_runs_past_deadline` counts, by profile, the
tasks a receiver holds whose run's deadline has passed and that have not
ended. The alert fires when it stays above 0 for 10 minutes: a receiver has
held a run past its deadline that long, as when a `Stop` call or the run's
terminal commit does not return. A task the receiver returns to the pool
leaves the count until a receiver holds it again. A receiver that is down
stops nothing either; its runs are adopted on redelivery, and
[WherryReceiverNotAdvertising](#wherryreceivernotadvertising) covers it.

Confirm it:

1. Run `wherry status`. A stuck run is listed `TASK_STATE_WORKING`.
2. Run `wherry status RUN_ID` and `wherry events RUN_ID`. `events` prints
   the run's statuses by attempt, each with its receiver and time; a run
   whose job cannot be stopped shows attempts after its deadline.
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

Each start failure, and each member with no matching plugin profile, returns
a task to the pool and spends one of its pool's `delivery_limit`
deliveries. From attempt `delivery_limit` minus 1, a receiver ends the run
`TASK_STATE_FAILED` with `delivery budget exhausted` instead.
`wherry_delivery_budget_exhausted_total` counts these runs by tenant and
profile. The alert fires on any such run in the last hour, for each tenant
and profile:

- `profile="none"`: runs given back at the budget before any profile took
  them, because no live receiver offered a profile matching their agent and
  selector;
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
3. Run `wherry receivers`. Check that a `live` receiver offers a profile
   with `ready=true` for the run's agent, with labels that satisfy the run's
   selector.
4. Read the receivers' logs for
   `run <id>: no current offer for agent <agent> satisfying its selector`,
   `run <id>: dispatch failed: <text>` and
   `run <id>: job never started (<class>): <reason>`.

Then:

- For no matching offer, start or reconfigure a receiver with a profile for
  the agent and the labels tasks select, or have the tenant change its
  selector.
- For start failures, fix the cause the log names in the plugin's platform,
  such as a missing image, an unschedulable node pool or a full namespace
  quota.
- Set `delivery_limit` to at least the number of receivers in the pool plus
  a margin, as in [Create a pool](operations.md#create-a-pool).
- Resubmit the failed runs once the cause is fixed.

## WherryCredentialRequestsUnanswered

A receiver that claims a run with credentials to deliver publishes a seal
request, and the submitting host answers it while `wherry watch` or
`wherry mcp` runs. A request no host answers within 5 minutes ends the run
`TASK_STATE_FAILED`. The alert fires on 2 or more `unanswered` seal waits
(`wherry_seal_waits_total`) for one tenant in an hour.

Confirm it:

1. Run `wherry status RUN_ID`. The message reads
   `credential <name> was not answered within 5m` and names each credential
   the host did not answer.
2. Run `wherry receivers`. An agent's `credentials` marks `missing` each
   credential the host's configuration has no source for.
3. Read the standard error of the host's `wherry watch` or `wherry mcp`. A
   failed source logs
   `run <id>: resolve credential <name>: <cause>`.

Then, on the tenant's host, as in
[Run credentialed tasks](security.md#run-credentialed-tasks):

- Add a source for each `missing` credential, or fix the source the log
  names, such as a file mode or a command that exits non-zero.
- Keep `wherry watch RUN_ID` or `wherry mcp` running until the run starts.
- Submit the run again.

## WherryPluginRestartLoop

A plugin that exits is started again after one second, the wait doubling
after each failed start up to 30 seconds, and its profiles are configured
again. Until then those profiles are not ready, and the receiver's
advertisement withdraws their offers. The alert fires on 3 or more restarts
of one plugin in 15 minutes (`wherry_plugin_restarts_total`).

Confirm it:

1. Run `wherry receivers`. The plugin's profiles show `ready=false` while it
   is down.
2. Read the receiver's log for `plugin exited; restarting`,
   `plugin restarted`, `plugin restart failed` and
   `plugin did not answer a ping; killing it`, and for the plugin's own
   lines before each exit.

Then:

- For `plugin restart failed` naming ownership or mode, reinstall the
  plugin as in [Install plugins](operations.md#install-plugins). The
  receiver checks the binary again before every restart.
- For a protocol version mismatch, install the plugin release that matches
  the receiver's, as in [Upgrade wherry](operations.md#upgrade-wherry).
- For a crash or a missed ping, read the plugin's lines for the cause,
  such as an unreachable platform API or memory pressure on the receiver
  host, and fix it. Runs whose job the receiver follows read again from
  their last offset once the plugin is back; a plugin down for one lock
  duration fails them with `log stream failed`.

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
2. Run `wherry receivers`. A profile whose readiness call fails shows
   `ready=false`.
3. Check the platform's API health with its own tools.

Then:

- `denied`: renew the platform credential at its source and restart the
  receiver, as in [Add a receiver](operations.md#add-a-receiver), or
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

1. Run `wherry receivers`. A receiver that cannot write its advertisements
   turns `stale`.
2. Read the receiver's log for `periodic step: <error>`,
   `run <id>: renew: <error>`, `run <id>: read events: <error>` and
   `denied (<capability>): <error>`. An authorisation error names the
   capability denied, and `access restored` follows its recovery.
3. From the receiver host, check the broker address and port in the
   receiver's bundle are reachable over TLS.

Then:

- Restore the route from the receiver host to the broker's TLS listener,
  or the brokers themselves.
- For a TLS or SCRAM error, check the bundle's CA against the listener's
  certificate and its principal and password against the cluster's SCRAM
  credentials.
- For `denied (<capability>)`, run `wherry admin check` to confirm the ACLs
  the state calls for exist.
- A task whose claim lapsed during the outage is redelivered, and its next
  holder adopts the running job.
