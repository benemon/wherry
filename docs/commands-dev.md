# Development commands

## Dev commands

`wherry dev` runs a whole deployment on one machine for trying wherry and
developing against it, as [Try it on one machine](quickstart-development.md#start-your-instance)
walks through. Each command takes `--instance NAME`, default `default`, a
[name](commands.md#names). An instance lives in `$XDG_DATA_HOME/wherry/dev/NAME`, by
default `~/.local/share/wherry/dev/NAME`, mode 0700, in two parts:
`cache/`, which `init` fills and `down --reset` removes, and `env/`, which `up`
creates and `down` removes. The ports are fixed, so one instance is up at
a time: Nomad on 4646, 4647 and 4648, Kafka on 9093 and, with storage, S3
on 18080, all on 127.0.0.1.

An instance is up while its `env/up.json` names a live Nomad agent that
`up` started. Every step prints its name on standard error, and a failed
step fails with `step <name>: <cause>`, with two exceptions: a `preflight`
refusal of `up` is the whole error, and the `host` step of `down` prints no
name.

Commands run at once on one instance are not serialized: run one at a
time. `XDG_DATA_HOME`, or `HOME` without it, must be the same for `up` and
`down`, since it locates the instance and its record of the host files `up`
created. `XDG_CONFIG_HOME` may change, since the record holds absolute
paths.

### `wherry dev init`

```sh
wherry dev init [--instance NAME] [--runtime podman|docker] [--storage-enabled] [--agent NAME]
```

Acquires everything `up` runs into `cache/` and records it in
`cache/manifest.json`:

- Nomad CE 2.1.1, the release the nomad plugin's CI tests. Its
  `SHA256SUMS` must carry a signature by HashiCorp's release key,
  fingerprint `C874 011F 0AB4 0511 0D02 1055 3436 5D94 72D7 468F`, and list
  the pinned SHA-256 of the `linux_amd64` zip. The zip must match
  `SHA256SUMS`.
- On Linux with Podman, the newest `nomad-driver-podman`, checked the same
  way.
- The newest `nomad/v*` release of the nomad plugin that passes three
  checks: its `checksums.txt` verifies against its sigstore bundle from the
  plugin's release workflow, its archive matches `checksums.txt`, and its
  binary speaks this wherry's plugin protocol. A binary that speaks another
  is dropped for the next older release.
- Kafka 4.3.1, the `docker.io/apache/kafka` image the real-broker tests
  pin by digest, pulled and tagged
  `wherry-dev/kafka:<id12>` in the runtime's own store. With
  `--storage-enabled`, also `quay.io/benjamin_holmes/ceph-aio:v21`, tagged
  `wherry-dev/ceph-aio:<id12>`. Under Podman the tags start `localhost/`.
- A copy of the running wherry binary, which the receiver job runs.

`--runtime` is required when both `docker` and `podman` answer `version`,
and found otherwise. `init` looks for `claude`, `codex` and `gemini` on
`PATH`. One found is the default agent. With several it asks on a
terminal, and fails with `found claude and codex; choose the default with
--agent` without one. With none and no `--agent` it fails with
`no agent harness found on PATH (looked for claude, codex, gemini); install one, or choose --agent mock`
before it downloads anything. `--agent NAME` takes a harness it found or
`mock`. `init` without `--storage-enabled` drops storage from an instance
that has it.

`init` sends `GH_TOKEN`, else `GITHUB_TOKEN`, to GitHub's API as a bearer
token when one is set. Without one, GitHub allows 60 API requests an hour
from an address, and a spent limit fails naming the time it resets.

`init` downloads only what changed and prints `instance NAME: unchanged`
when nothing did. A harness installed later needs `init` again. It refuses
while the instance is up with `instance NAME is up; run wherry dev down
first`. A manifest of another layout version is refused naming its path.

### `wherry dev up`

```sh
wherry dev up [--instance NAME] [--runtime podman|docker] [--agent-image NAME=IMAGE]...
```

Builds and starts the environment from `cache/`, downloading nothing, and
returns once a run of the mock agent completes. Its steps:

1. `preflight` refuses while this instance is up (`instance NAME is
   already up`) or another is (`instance OTHER is up; run wherry dev down
   --instance OTHER first`), without a manifest (`nothing acquired for
   instance NAME; run wherry dev init first`), with a cached file whose
   SHA-256 differs from the manifest's, with a `--runtime` other than the
   manifest's, with a port in use (`port 9093 is in use`), and when the
   running binary differs from the copy (`the running wherry differs from
   the snapshot; run wherry dev init`).
2. `harnesses`, `credentials`: the agents are the harnesses `init` found,
   the `--agent-image` names and `mock`. For each, the set variables of
   `CLAUDE_CODE_OAUTH_TOKEN`, `ANTHROPIC_API_KEY`, `CODEX_API_KEY` (else
   `OPENAI_API_KEY`), `GEMINI_API_KEY` and `WHERRY_DEV_MOCK_TOKEN` are
   copied into `env/host/credentials/`, mode 0600.
3. `tls`, `super-user`: an instance CA, the broker's certificate for
   127.0.0.1, and the password of the Kafka super user `wherry-admin`, the
   only SCRAM credential the broker starts with. These are the Kafka
   administrator's part.
4. `image` checks that the manifest's tags are still in the runtime, and
   fails with `image TAG is no longer in the runtime; run wherry dev init`.
5. `nomad` starts `nomad agent -dev` as you, detached, its log in
   `env/nomad/nomad.log` and its data directory mode 0700. Its
   `env.denylist` holds Nomad's default list, the copied variables and
   every variable whose name ends `_TOKEN`, `_KEY`, `_SECRET` or
   `_PASSWORD` when `init` ran, so the agents' environment carries none of
   them. On Linux without a Podman socket it starts `podman system
   service` beside Nomad.
6. `kafka` runs the broker as a Nomad job from the local tag and waits for
   a metadata request over SCRAM.
7. `operator` runs [`operator init`](commands.md#wherry-operator-init) with
   `wherry-admin` given as flags, never prompting, on `env/operator`, the
   instance's operator
   [working directory](configuration-operator.md#operator-working-directory):
   `wherry operator --dir <instance>/env/operator check` runs against it.
8. `bootstrap` makes the operator's calls through that directory: pool
   `dev`, tenant `dev`, host `dev` and receiver `dev-1` with a plugin
   profile per agent, their bundles under `env/operator/bundles/`. The
   receiver's `config.yaml` names the instance CA as `ca_file`.
9. `host` writes the host's `config.yaml`, bundle and key, and registers
   the key. For instance `default`, unless `WHERRY_CONFIG` is set, they go
   at the standard [host paths](configuration-host.md#client-configuration);
   the config names `env/host/state` as `state_dir`, so dev writes no state
   at the standard state path. `up` creates each file exclusively, so it
   never replaces anything there. `cache/manifest.json` records each path,
   its directory resolved through symlinks, before `up` creates it, and the
   file's device, inode, size and modification time after; no digest of a
   file holding a credential is kept.
   If the config, bundle or key is already there, or appears while
   `up` creates them, `up` removes what it created there and, as for every
   other instance and with `WHERRY_CONFIG` set, writes them in
   `env/host/`. The config names the instance CA as `ca_file`, and the dev
   pool's [storage](configuration-host.md#client-configuration) credentials
   with storage.
10. `receiver` runs `wherry receiver run` from the copy as a `raw_exec` job
    and waits for `dev-1` to be live with every profile ready.
11. `smoke` runs the mock agent and checks the output it returns. `ready`
    writes `env/up.json` and prints the summary, a `submit:` line and an
    `mcp add` line for each harness. When the config is in `env/host/`, it
    prints `export WHERRY_CONFIG=<path>` first, and each `mcp add` line
    passes `WHERRY_CONFIG` to the server; otherwise neither names the
    config.

The harness profiles and `mock` run under `raw_exec` with label
`isolation: host`. `--agent-image NAME=IMAGE` splits at the first `=`. It
runs agent `NAME` in `IMAGE` with label `isolation: container`, never
pulled, so the image must be in the runtime. For `claude`, `codex` and
`gemini` the profile runs the harness from `/local`. Any other `NAME` runs
the image's entrypoint, which meets the
[agent contract](configuration-receiver.md#agent-contract).
[`agent.Containerfile`](https://github.com/benemon/wherry/blob/main/docs/agent.Containerfile) builds one for `claude`.

With storage, `storage`, `s3-user` and `bucket` run after `kafka`. ceph-aio
serves S3 on `http://127.0.0.1:18080`. `s3-user` has `radosgw-admin`
generate the user's keys and writes them to `env/host/credentials/`. Pool
`dev` takes the endpoint as its [storage](configuration-operator.md#pool-terms), and
the smoke run sends an S3 input and returns its output through the bucket.
The ceph-aio job reserves 2048 MB of memory, and its data lives in the
container, so `down` removes it.

A failed step leaves the environment for `down` to remove.

### `wherry dev down`

```sh
wherry dev down [--instance NAME] [--reset]
```

Its steps, each run after a failed one:

1. `jobs` deregisters the receiver, every agent job, storage and Kafka,
   waits up to 60 seconds for their allocations to end, and purges them.
2. `containers` waits up to 60 seconds for the runtime to list no
   container of theirs, then removes those left by ID.
3. `nomad` stops Nomad and the Podman service with `SIGTERM`, and
   `SIGKILL` after 30 seconds.
4. `ports` checks that the ports are free.
5. `data` removes `env/`.
6. `host` removes each file the manifest records `up` creating at the
   standard host paths while it is still that file: a regular file with no symbolic link, with the recorded device, inode, size and modification time, in a directory
   that still resolves to the recorded one. So a stale default never
   shadows a real host's, and a real host's file stays. Anything else, a
   missing file or one whose device and inode a crash kept `up` from
   recording included, it leaves and names:
   `left <path>: <reason>`. Two cases remain: a file replaced between
   this check and its removal is removed, since the operating system
   offers no removal conditional on identity; and a file edited in place
   that keeps its size and has its modification time set back is removed,
   since `down` keeps no digest of a file holding a credential.

`down` fails naming every failed step. It keeps `cache/`, prints
`down: instance NAME` on standard output, and then prints a
`mcp remove` line for each harness, since the registration names a
configuration that is gone. An instance that is not up prints
`instance NAME is not up`, removes a leftover `env/` and the host files as
in step 6, and exits 0.

With `--reset`, `down` removes the host files as in step 6, then
`cache/`. It refuses `--reset` while the instance is up, and when it
cannot read the manifest, which may record host files: it names the
standard paths to check by hand before you remove the manifest. The
images stay in the runtime's store. Remove `wherry-dev/kafka:<id12>`, and
`wherry-dev/ceph-aio:<id12>` with storage, with the runtime.
