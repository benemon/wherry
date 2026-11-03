# Upgrading

## Upgrade wherry

Readers ignore unknown members on `wherry.operator` records and reject every
record with a higher `v` than their release knows as written by a later
release. Upgrade the reader. A harness does not restart a running
`wherry host mcp` when its binary changes, so restart the MCP server after
upgrading.

The [release notes](https://github.com/benemon/wherry/releases) mark a
release that changes the record formats, the key file formats or the plugin
protocol, and give its upgrade steps.

wherry's key files are private JWKs. A host key or a receiver's
`signing.key` in an older format fails to load. On a clean break, as a
[reset](operations-operator.md#reset-a-deployment), move it aside so that the new release writes
a new one.

Between releases that keep the record formats, upgrade in place.
Prerequisites: the new `wherry` binary, and each receiver's local
configuration, bundle and state directory.

1. Replace the binary on every host with the new release's, verified as in
   [Install wherry](quickstart-production.md#install-wherry).
2. For each receiver whose plugins change, verify the new plugin release and
   record its pin with `wherry operator receiver apply`, as in
   [Install plugins](operations-receivers.md#install-plugins). A receiver refuses a plugin of
   another [plugin protocol version](commands.md#plugins), so a release that
   changes the protocol takes plugins built for it.
3. Roll each receiver's binary and plugins, or its image, to the new
   versions at once, and restart it with the same local configuration,
   bundle and state directory. A restart between steps 2 and 3 refuses the
   old plugin binary. On
   `SIGINT` or `SIGTERM` it stops claiming, closes its idle claims and lets
   the runs it holds go on for up to `drain_timeout`. It then exits without
   stopping the jobs still running and releases their tasks to the pool.
   The next delivery of each task adopts the running
   job, on the restarted receiver or another member, and resumes the
   transcript from the run's latest committed offset. A stop takes up to
   `drain_timeout` plus 5 seconds. Allow the receiver 30 seconds to stop
   before a forced kill, with `drain_timeout` plus 5 seconds below that
   window. Restart a pool's receivers one at a time, so that a stopping key
   custodian re-keys its tasks to a live successor
   ([Stop a pool's key custodian](operations-operator.md#stop-a-pool-s-key-custodian)).
4. Restart the harness sessions that run `wherry host mcp`.

