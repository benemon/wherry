# Observability

A Grafana dashboard and Prometheus alert rules for wherry receivers, built from the metric families a receiver serves at
`/metrics`.

| File | Content |
| --- | --- |
| `wherry-dashboard.json` | Grafana dashboard of the receiver families, with `datasource`, `pool` and `instance` variables |
| `wherry-alerts.yaml` | Prometheus alerting rules, each with a `runbook_url` |
| `routing.test.yml` | promtool unit tests of the dashboard's Routing queries |

[Metrics](https://benemon.github.io/wherry/operations#metrics) in the operator guide says how to scrape a receiver and
load both files, and [Runbooks](https://benemon.github.io/wherry/runbooks) has a section for each rule.
`TestObservabilityNamesShippedSeries` in `internal/receiver` fails when either file names a series no receiver exports.
`promtool test rules contrib/observability/routing.test.yml` (promtool 3.15.1, as CI runs it) evaluates the Routing
panels' queries; `TestDashboardRoutingQueriesAreTested` fails when a Routing query has no test there.
