<h1>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="brand/svg/wherry-horizontal-cream.svg">
    <img alt="wherry" src="brand/svg/wherry-horizontal.svg" width="320">
  </picture>
</h1>

[![CI](https://github.com/benemon/wherry/actions/workflows/ci.yml/badge.svg)](https://github.com/benemon/wherry/actions/workflows/ci.yml)
[![CodeQL](https://github.com/benemon/wherry/actions/workflows/codeql.yml/badge.svg)](https://github.com/benemon/wherry/actions/workflows/codeql.yml)
[![Scorecard](https://github.com/benemon/wherry/actions/workflows/scorecard.yml/badge.svg)](https://github.com/benemon/wherry/actions/workflows/scorecard.yml)
[![govulncheck](https://github.com/benemon/wherry/actions/workflows/govulncheck.yml/badge.svg)](https://github.com/benemon/wherry/actions/workflows/govulncheck.yml)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/benemon/wherry/badge)](https://securityscorecards.dev/viewer/?uri=github.com/benemon/wherry)

An execution fabric for delegated agent work. The
[Introduction](https://benemon.github.io/wherry/introduction) describes the
model and [how it works](https://benemon.github.io/wherry/introduction#how-does-wherry-work).

## Getting started

The [development quickstart](https://benemon.github.io/wherry/quickstart-development)
runs a whole deployment on one machine with `wherry dev`.
[Install](https://benemon.github.io/wherry/install)
provisions a pool, tenant and receiver on Nomad, Kubernetes or systemd and runs a first task.

## Configuration

The configuration guides cover the
[operator](https://benemon.github.io/wherry/configuration-operator),
[receiver](https://benemon.github.io/wherry/configuration-receiver),
[host](https://benemon.github.io/wherry/configuration-host) and
[development instance](https://benemon.github.io/wherry/configuration-development).

## Operations

[Operator operations](https://benemon.github.io/wherry/operations-operator)
covers Kafka, pools, tenants, receiver provisioning, revocation and reset.
[Receiver operations](https://benemon.github.io/wherry/operations-receivers)
covers platforms, plugins, metrics and custody.
[Host operations](https://benemon.github.io/wherry/operations-hosts)
covers enrolment, harnesses, files and key rotation.
[Upgrade and uninstall](https://benemon.github.io/wherry/upgrade) gives the rolling procedure.

## Harness integration

[Harness integration](https://benemon.github.io/wherry/operations-hosts#harness-integration)
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
the unit tests of both modules, the dependency licence check, the
attribution-marker check and the old-name check.

## Tests

The plugin protocol is its own module: run `go test ./...` in `pluginsdk`
as well as at the root.

Unit tests run against an in-process Kafka. Tests that need real services
run when their variables are set and skip otherwise:

- `WHERRY_KAFKA_TEST_BROKER`: a plaintext Kafka 4.3 broker with share
  groups, such as `127.0.0.1:9092`, for the receiver's claim tests.
- `WHERRY_TRUST_BROKER` with the other `WHERRY_TRUST_*` variables the
  test fixtures read (the CLI reads none): the TLS broker `test/kafka/up.sh` or
  `test/kafka/up-cluster.sh` starts, with its Kafka super user, for the
  provisioning, `wherry operator init`, ACL and revocation tests. The
  fixtures delegate to `wherry.operator` as `init` does. They write to the broker's `wherry.operator`, so they fail
  on any broker whose cluster ID is not the `WHERRY_TRUST_CLUSTER_ID` in the
  script's env file: never point them at a shared cluster. `WHERRY_TRUST_RECONCILE_WAIT` adds the check that
  provisioning survives a reconciliation.
- `WHERRY_S3_TEST_ENDPOINT`, `WHERRY_S3_TEST_ACCESS_KEY_FILE` and
  `WHERRY_S3_TEST_SECRET_KEY_FILE`: an S3-compatible store, such as Ceph RGW,
  and the files holding its access key and secret key, for the host's storage
  tests and the S3 leg of the round-trip test.

`test/kafka/up.sh DIR` starts one Kafka 4.3 container serving both brokers
and writes their variables to `DIR/env`. CI runs these tests against it:

```sh
test/kafka/up.sh /tmp/wherry-kafka && . /tmp/wherry-kafka/env
go test -p 1 -count=1 ./internal/operator/ ./internal/receiver/ ./internal/client/ ./cmd/wherry/
```

The packages share the broker's `wherry.operator`, so they run one at a time.

`test/s3/up.sh DIR` starts [ceph-aio](https://github.com/benemon/ceph-aio), a
single-node Ceph with RGW, creates a user with generated keys and writes the S3
variables to `DIR/env`. CI runs the S3 tests against it:

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
[goreleaser](https://goreleaser.com/) from `.goreleaser.yaml`. goreleaser builds
the archives, writes `checksums.txt` and an SBOM per archive
(`<archive>.sbom.json`), signs `checksums.txt` with cosign, and uploads them to a draft release. The
workflow then attests the archives' provenance, publishes the release, and
pushes the Homebrew formula to `benemon/homebrew-tap`. Its `verify` jobs check
the published release as a user receives it: the assets against
`.github/release-contents.txt`, the checksums' signature and the checksums,
each archive's attestation, and, on macOS, `brew install benemon/tap/wherry`
and `brew test wherry`. A tag with a hyphenated qualifier, such as
`v0.1.0-rc.1`, publishes a prerelease. The Homebrew formula follows
prereleases until the first stable tag (`vX.Y.Z`) exists, then stable releases
only. `pluginsdk/v*` tags version the
`pluginsdk` module and publish nothing. The same build, unsigned and
unpublished, writes the archives into `dist` locally:

```sh
goreleaser release --snapshot --clean --skip=sign
```

### Changing the plugin protocol

wherry's `dev` lanes acquire a signed nomad plugin release that speaks the
current protocol, and wherry-plugins' receiver lanes run a wherry release, so a
protocol change releases in this order, and no required check is ever merged
red:

1. Open the wherry pull request that changes the protocol, and tag
   `pluginsdk/vX.Y.Z-rc.N` on its head. The tag publishes nothing. A module
   proxy keeps a tag once it has served it, so a later change to `pluginsdk`
   needs a new rc tag and step 2 again.
2. In wherry-plugins, require that pluginsdk tag in every plugin's `go.mod`
   and set `WHERRY_REF` in `ci.yml` and `e2e.yml` to the full SHA the tag
   names. The receiver lanes then build wherry from that commit instead of
   running `WHERRY_VERSION`. Merge when green, and cut plugin prereleases
   (`nomad/vA.B.C-rc.N` and the others); their release workflow refuses a
   stable tag while `WHERRY_REF` is set.
3. Re-run the wherry pull request's `dev` lanes. They acquire the plugin
   prerelease and pass; merge it.
4. Tag `pluginsdk/vX.Y.Z` and the wherry release from `main`.
5. In wherry-plugins, move to the final pluginsdk tag, set `WHERRY_VERSION` to
   the wherry release, remove `WHERRY_REF`, and release the plugins.

The order stops being needed once plugin protocol negotiation accepts the
previous protocol (go-plugin's versioned plugins, deferred to the first
protocol change after 1.0): a pull request at the new protocol then passes
its `dev` lanes against the existing plugin release.

The documentation site embeds the wherry-plugins examples at the commit
`WHERRY_PLUGINS_REF` names in `.github/workflows/pages.yml`. After a
wherry-plugins release, set `WHERRY_PLUGINS_REF` to the full SHA of the
commit its tags were cut from (`git rev-list -n 1 nomad/<version>` in
wherry-plugins) and merge that change to `main`, which republishes the site.
