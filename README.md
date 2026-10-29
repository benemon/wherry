<h1>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="brand/svg/wherry-horizontal-cream.svg">
    <img alt="wherry" src="brand/svg/wherry-horizontal.svg" width="320">
  </picture>
</h1>

An execution fabric for delegated agent work. The
[Introduction](https://benemon.github.io/wherry/introduction) describes the
model and [what ships](https://benemon.github.io/wherry/introduction#what-ships).

## Getting started

The [Quickstart](https://benemon.github.io/wherry/quickstart) installs
`wherry` from a release, provisions one pool with one receiver, and runs a
first task.

## Configuration

[Configuration](https://benemon.github.io/wherry/configuration) lists the
client configuration, the receiver's local configuration, bundle, pool
terms, receiver configuration and environment variables.

## Operations

[Operations](https://benemon.github.io/wherry/operations) covers preparing
Kafka, adding tenants, hosts and receivers, revocation, key rotation and
upgrades.

## Harness integration

[Harness integration](https://benemon.github.io/wherry/operations#harness-integration)
installs the [`plugins/wherry`](plugins/wherry) MCP server and Agent Skill,
and [MCP tools](https://benemon.github.io/wherry/mcp) lists the tools.

## Building from source

wherry needs the Go version `go.mod` names. Build the binary with either:

```sh
make build
go build -o wherry ./cmd/wherry
```

The plugin protocol is its own Go module in [`pluginsdk`](pluginsdk), with its
own `go.mod` and `pluginsdk/v*` tags.

## Make targets

`make help` lists the targets. `make check` runs every CI gate: gofmt, vet,
the unit tests of both modules, the dependency licence check and the
attribution-marker check.

## Tests

The plugin protocol is its own module: run `go test ./...` in `pluginsdk`
as well as at the root.

Unit tests run against an in-process Kafka. Tests that need real services
run when their variables are set and skip otherwise:

- `WHERRY_KAFKA_TEST_BROKER`: a plaintext Kafka 4.3 broker with share
  groups, such as `127.0.0.1:9092`, for the receiver's claim tests.
- `WHERRY_TRUST_BROKER` with the other `WHERRY_TRUST_*` variables of
  `wherry admin`: the TLS broker `test/kafka/up.sh` or
  `test/kafka/up-cluster.sh` starts, for the provisioning, ACL and
  revocation tests. They write to the broker's `wherry.admin`, so they fail
  on any broker whose cluster ID is not the `WHERRY_TRUST_CLUSTER_ID` in the
  script's env file: never point them at a shared cluster. `WHERRY_TRUST_RECONCILE_WAIT` adds the check that
  provisioning survives a reconciliation.
- `WHERRY_S3_TEST_ENDPOINT`, `WHERRY_S3_TEST_ACCESS_KEY_FILE` and
  `WHERRY_S3_TEST_SECRET_KEY_FILE`: an S3-compatible store, such as Ceph RGW,
  and the files holding its access key and secret key, for the host's storage
  tests and the S3 leg of the round-trip test.

`test/kafka/up.sh DIR` starts one Kafka 4.3 container serving both brokers
and writes their variables to `DIR/env`; CI runs these tests against it:

```sh
test/kafka/up.sh /tmp/wherry-kafka && . /tmp/wherry-kafka/env
go test -p 1 -count=1 ./internal/admin/ ./internal/receiver/ ./internal/client/ ./cmd/wherry/
```

The packages share the broker's `wherry.admin`, so they run one at a time.

`test/s3/up.sh DIR` starts [ceph-aio](https://github.com/benemon/ceph-aio), a
single-node Ceph with RGW, creates a user with generated keys and writes the S3
variables to `DIR/env`; CI runs the S3 tests against it:

```sh
test/s3/up.sh /tmp/wherry-s3 && . /tmp/wherry-s3/env
go test -count=1 ./internal/client/ ./cmd/wherry/
```

Each S3 test deletes the objects it wrote.

## Documentation site

The site is [VitePress](https://vitepress.dev/) in `docs-site`, built from
the pages in `docs`. Its Plugins page embeds the receiver examples of
[wherry-plugins](https://github.com/benemon/wherry-plugins), checked out
beside this repository or named by `WHERRY_PLUGINS_DIR`:

```sh
(cd docs-site && npm ci)
make docs
```

The build fails on a link to a missing page or anchor. The `pages` workflow
publishes the site from `main`.

## Releases

A `v*` tag cuts a release: `.github/workflows/release.yml` checks that the
tagged commit's `ci` run on `main` passed, then runs
[goreleaser](https://goreleaser.com/) from `.goreleaser.yaml`, which builds the
archives, writes `checksums.txt` and an SBOM per archive
(`<archive>.sbom.json`), signs `checksums.txt` with cosign, publishes the
release and updates the Homebrew formula; the workflow then attests the
archives' provenance. A tag with a hyphenated qualifier, such as
`v0.1.0-rc.1`, publishes a prerelease. `pluginsdk/v*` tags version the
`pluginsdk` module and publish nothing. The same build, unsigned and
unpublished, writes the archives into `dist` locally:

```sh
goreleaser release --snapshot --clean --skip=sign
```
