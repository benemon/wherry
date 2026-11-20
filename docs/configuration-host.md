# Host configuration

## Client configuration

A host reads its settings from `config.yaml`.
`wherry host init` writes this file for you.
You can also write it yourself.

### Where wherry looks for the file

wherry uses the first path that is set:

1. The path you pass with `--config-file`.
2. The path in `WHERRY_CONFIG`.
3. `config.yaml` in the configuration directory, `~/.config/wherry/` by default.

Unknown and duplicate fields are errors.
A decode error reports line numbers without field values.
A leading `~/` expands in the flag and configuration paths.

The host's paths are the same on every OS, macOS included:

| Directory | Path | Holds |
| --- | --- | --- |
| configuration directory | `$XDG_CONFIG_HOME/wherry/`, by default `~/.config/wherry/` | `config.yaml`, `bundle.yaml`, `host.key` and, for a private CA, `ca.pem` |
| state directory | `$XDG_STATE_HOME/wherry/`, by default `~/.local/state/wherry/` | the host's state, `state_dir` |

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `bundle` | path | required; `host init BUNDLE` writes the [default bundle path](#a-configuration-away-from-the-default-path) when the configuration names none | Read the host's bundle from this file; `host init BUNDLE` copies the bundle here. |
| `ca_file` | path | `ca.pem` in the configuration directory if present | Trust these PEM CA certificates for the broker connection. |
| `host_key` | path | `host.key` in the configuration directory, or `<path>.host.key` beside a configuration [away from the default path](#a-configuration-away-from-the-default-path) | Read the host's private Ed25519 JWK. |
| `state_dir` | path | the host state directory, or `<path>.state/` beside a configuration [away from the default path](#a-configuration-away-from-the-default-path) | Store local run records here. |
| `credentials` | mapping | none | Map credential names to [sources](security.md#credential-sources). |
| `git` | list | none | Map URL prefixes to credential names and SSH known-hosts files. |
| `storage` | mapping | none | Map pool names to S3 `access_key` and `secret_key` credential names. |
| `input_roots` | list of paths | MCP server working directory | Allow the MCP server to send inputs from these directories. |

Without a CA file, the host trusts system roots.
Each credential source uses exactly one of `file`, `env` and `command`.

### A configuration away from the default path

A host identity is its host key, and a configuration names one. A
configuration at any path other than the default keeps its files beside
it, named from its whole path, nothing stripped:

| File | Default configuration | Configuration at `<path>` |
| --- | --- | --- |
| the bundle, where `host init BUNDLE` copies it when the configuration names none | `bundle.yaml` in the configuration directory, always | `<path>.bundle.yaml` |
| the host key file | `host.key` in the configuration directory | `<path>.host.key` |
| the state directory | the state directory | `<path>.state/` |

So `--config-file /work/a.yaml` and `--config-file /work/b.yaml` are two
hosts with two keys, and never share one; `/work/a` and `/work/a.yaml` are
distinct too. The path is made absolute and no symlink is resolved: it is
the default when it is the default path, so the default configuration
named relatively keeps the configuration directory's files, or when both
exist and are one file, so a symlink or hard link to an existing default
configuration does too. A link to a file not yet written is a
configuration of its own, with its files beside the link. Explicit
`bundle`, `host_key` and `state_dir` values are kept wherever the
configuration is: `host init BUNDLE` copies the bundle to the `bundle` a
configuration away from the default path names, and to `<path>.bundle.yaml`
only when it names none; the default configuration's copy is always
`bundle.yaml` in the configuration directory. `host init` makes the
configuration directory mode 0700; elsewhere it creates a missing directory
for these files with mode 0700 and leaves an existing one's mode alone.

Before it writes anything, `host init` refuses when the bundle's
destination or one of the derived paths already exists and is not the file
the configuration names: another configuration at `<path>.bundle.yaml`,
another host's key at `<path>.host.key`, a file where `<path>.state/` would
go, or a directory where the named `bundle` would go. The error names the
path and the setting, `bundle`, `host_key` or `state_dir`, that avoids it.
A rerun on an initialised configuration, whose files it already names,
succeeds. Explicit `host_key` and `state_dir` paths are not checked this
way, and nor are the default configuration's files in the configuration
directory.

Git entries contain `url`, optional `credential` and `host_key`.
An SSH remote requires `host_key`.
An HTTPS remote refuses `host_key`.
See [Git credentials](security.md#git-credentials).
A pool without a storage entry has no S3 lane on this host.
MCP input roots must be absolute paths to existing directories.
CLI commands can read any input path you give them.

`host init` writes `bundle`, a `ca_file` it asked for, and an entry in
`credentials`, `git` and `storage` for each answer, each a reference to a
source and never a value; a re-run keeps every entry and adds only what a
newly entitled pool needs. Without a terminal it asks nothing and lists what
still has no source. It asks for the CA only on a terminal, so without one a
broker under a private CA needs `ca.pem` in the configuration directory, or
`ca_file` in the file, before `host init` runs.

`state_dir` holds:

- `runs/<run>.json` for each run this host submitted, readable by its owner
  alone: the run's submission
  without its envelope and wrap, kept to
  [resubmit](concepts.md#lost-pool-keys) the run if its key is lost, and
  `redaction_key`, the run's redaction key, made when this host submits the
  run. A replacement's record also holds its signed bytes, envelope
  included. No record holds a credential value in the clear.
- `runs/<run>/outputs/`, where `wait` writes the run's outputs by default.
- `resubmit/<run>.json.lock`, which every process sharing the directory
  holds while it resubmits that run.

`submit` writes the run's record before it publishes the task, and removes
it only when publishing fails.

### Client configuration example

The configuration of host `laptop`, setting every field:

```yaml
bundle: ~/.config/wherry/laptop.bundle
# A private CA the broker's certificate chains to; without ca_file, ~/.config/wherry/ca.pem if it exists, else the
# system roots.
ca_file: ~/.config/wherry/kafka-ca.pem
host_key: ~/.config/wherry/laptop.key
state_dir: ~/.local/state/wherry/laptop
credentials:
  claude-oauth-token:
    file: ~/.config/wherry/credentials/claude-oauth-token
  model-api-key:
    file: ~/.config/wherry/credentials/model-api-key
  openai-api-key:
    command: [op, read, "op://wherry/openai/credential"]
  github-token:
    env: GITHUB_TOKEN
  deploy-key:
    file: ~/.config/wherry/credentials/deploy-key
  s3-access-key:
    file: ~/.config/wherry/credentials/s3-access-key
  s3-secret-key:
    command: [op, read, "op://wherry/s3/secret-key"]
git:
  - url: https://github.com/acme/
    credential: github-token
  - url: ssh://git@gitlab.example/
    credential: deploy-key
    host_key: ~/.config/wherry/known_hosts/gitlab
input_roots: [~/src, ~/notes]
storage:
  build:
    access_key: s3-access-key
    secret_key: s3-secret-key
```

`credentials` holds a source for each credential the agents of the
[pool terms example](configuration-operator.md#pool-terms-example) take: two files, a password
manager's command for `openai-api-key`, and the submitting process's
`GITHUB_TOKEN` for `github-token`, which `git` also gives every task cloning
under `https://github.com/acme/`. `storage` gives pool `build`'s bucket the S3
access key from a file and the secret key from the password manager; another
pool with storage takes its own entry. Each source resolves at
each submit.

## Bundle

A YAML file that `wherry operator` writes with mode 0600 under the working
directory's `bundles/`. Unknown fields are errors, so a bundle carrying a
`ca` is refused, and `broker`, `principal` and `password` are required; a
decode error names only its line numbers. A trailing line break ends the
password and is not part of it, and a password of nothing else is refused.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `broker` | string | required | Kafka bootstrap address, `host:port` |
| `principal` | string | required | SCRAM-SHA-512 user name |
| `password` | string | required | SCRAM password |

A bundle carries nothing about the broker's TLS CA. The Kafka platform owns
and renews its CA; each host and receiver trusts the broker through the
system's roots or the `ca_file` of its own configuration, kept current by
whoever distributes the Kafka CA. A `ca_file` with no certificate fails with
`CA contains no certificates`.

## Environment variables

| Variable | Read by | Effect |
| --- | --- | --- |
| `WHERRY_CONFIG` | host commands | the [client configuration](#client-configuration) path in place of `config.yaml` in the configuration directory; `--config-file` takes precedence |
| `XDG_CONFIG_HOME`, `XDG_STATE_HOME` | host commands | when set, the parents of the host's `wherry/` configuration and state directories in place of `~/.config` and `~/.local/state` |

 `wherry operator`'s connection is
[`operator.yaml`](configuration-operator.md#operator-working-directory).

Go's TLS stack reads `SSL_CERT_FILE` as its
[crypto/x509](https://pkg.go.dev/crypto/x509#SystemCertPool) package
documents. On Linux, a broker connection without `ca_file`, in any wherry
command, trusts the roots it names. macOS ignores it for that connection:
use `ca_file` there. The host's S3 client adds the certificates in
`SSL_CERT_FILE` to the system roots for a pool's storage endpoint on every
OS.

A plugin reads its own variables from the receiver's environment; the
[Nomad plugin](https://github.com/benemon/wherry-plugins/tree/main/nomad#credentials)
lists its.

Trailing CR and LF are trimmed from password files: `operator.password`, the
super user's password file `operator init` reads, and a bundle's password.
