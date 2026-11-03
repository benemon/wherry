# Observability

A Grafana dashboard and Prometheus alert rules for wherry receivers, built from the metric families a receiver serves at
`/metrics`.

| File | Content |
| --- | --- |
| `wherry-dashboard.json` | Grafana dashboard of the receiver families, with `datasource`, `pool` and `instance` variables |
| `wherry-alerts.yaml` | Prometheus alerting rules, each with a `runbook_url` |

[Metrics](https://benemon.github.io/wherry/operations#metrics) in the operator guide says how to scrape a receiver and
load both files, and [Runbooks](https://benemon.github.io/wherry/runbooks) has a section for each rule.
`TestObservabilityNamesShippedSeries` in `internal/receiver` fails when either file names a series no receiver exports.
