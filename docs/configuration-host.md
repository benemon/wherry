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
| configuration directory | `$XDG_CONFIG_HOME/wherry/`, by default `~/.config/wherry/` | `config.yaml`, `bundle.yaml` and `host.key` |
| state directory | `$XDG_STATE_HOME/wherry/`, by default `~/.local/state/wherry/` | the host's state, `state_dir` |

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `bundle` | path | required; `host init BUNDLE` writes the [default bundle path](#a-configuration-away-from-the-default-path) when the configuration names none | Read the host's bundle from this file; `host init BUNDLE` copies the bundle here. |
| `host_key` | path | `host.key` in the configuration directory, or `<path>.host.key` beside a configuration [away from the default path](#a-configuration-away-from-the-default-path) | Read the host's private Ed25519 JWK. |
| `state_dir` | path | the host state directory, or `<path>.state/` beside a configuration [away from the default path](#a-configuration-away-from-the-default-path) | Store local run records here. |
| `ca_cert` | string | none | Trust this CA's PEM, or several concatenated, beside the machine's trust store for the host's Kafka and S3 connections; `host init --ca-cert` writes it ([Broker and S3 trust](security.md#broker-and-s3-trust)). |
| `credentials` | mapping | none | Map credential names to [sources](security.md#credential-sources). |
| `git` | list | none | Map URL prefixes to credential names and SSH known-hosts files. |
| `storage` | mapping | none | Map pool names to S3 `access_key` and `secret_key` credential names, which win over the `storage` pair the bundle carries for that pool. |
| `input_roots` | list of paths | MCP server working directory | Allow the MCP server to send inputs from these directories. |
| `tracing` | bool | `false` | Write the host's spans to its tenant's trace topic through the host's Kafka connection ([Tracing](tracing.md)). |

The host verifies the broker and each pool's bucket against the machine's trust store plus `ca_cert`; a configuration
keeping the CA file setting of earlier releases is refused naming `ca_cert`.
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

`--config-file /work/a.yaml` and `--config-file /work/b.yaml` are two hosts
with two keys. `/work/a` and `/work/a.yaml` are distinct too. Explicit
`bundle`, `host_key` and `state_dir` values are kept wherever the
configuration is. `host init BUNDLE` copies the bundle to the `bundle` a
configuration away from the default path names, and to `<path>.bundle.yaml`
only when it names none. The default configuration's copy is always
`bundle.yaml` in the configuration directory. `host init` makes the
configuration directory mode 0700. Elsewhere it creates a missing directory
for these files with mode 0700 and leaves an existing one's mode alone.

The path is made absolute and no symlink is resolved. A configuration is
the default in two cases:

- Its path is the default path. The default configuration named relatively
  keeps the configuration directory's files.
- Both exist and are one file. A symlink or hard link to an existing default
  configuration keeps them too.

A link to a file not yet written is a configuration of its own, with its
files beside the link. It stays one when that file is the default
configuration: writing through the link makes it the default file, so
`host init` names the `host_key` and `state_dir` it chose beside the link
in the configuration it writes, and every later load, through the link or
the default path, keeps them.

Before it writes anything, `host init` refuses when the bundle's
destination or one of the derived paths already exists and is not the file
the configuration names: another configuration at `<path>.bundle.yaml`,
another host's key at `<path>.host.key`, a file where `<path>.state/` would
go, a directory where the named `bundle` would go, or a symlink to any of
these or to nothing. A symlink is refused or accepted by what it points at:
one to the file the configuration names is its own, one whose target does
not exist is refused, and nothing is written through it. The error names the
path and the setting, `bundle`, `host_key` or `state_dir`, that avoids it.
A rerun on an initialised configuration, whose files it already names,
succeeds. Explicit `host_key` and `state_dir` paths are not checked this
way, and nor are the default configuration's files in the configuration
directory.

Git entries contain `url`, optional `credential` and `host_key`.
An SSH remote requires `host_key`.
An HTTPS remote refuses `host_key`.
See [Git credentials](security.md#git-credentials).
A pool without a storage entry uses the pair the bundle carries as
`storage`; without that it has no S3 lane on this host.
MCP input roots must be absolute paths to existing directories.
CLI commands can read any input path you give them.

`host init` writes `bundle`, `host_key` and
`state_dir` when a configuration written through a link to the default
path needs them, and an entry in `credentials`, `git` and `storage` for
each answer, each a reference to a source and never a value; a re-run
keeps every entry and adds only what a newly entitled pool needs. It writes
no entry for a credential whose target is a
[catalog variable](credential-variables.md), and asks nothing for it: the
host reads the variable where submits run. Without a terminal it asks
nothing, prints one line per entitled agent, ready or not, and lists what
else still has no source. It asks
nothing about the broker's CA: the machine's trust store holds it.

`state_dir` holds:

- `runs/<run>.json` for each run this host submitted, readable by its owner
  alone: the run's submission
  without its envelope and wrap, kept to
  [resubmit](concepts.md#lost-pool-keys) the run if its key is lost, and
  `redaction_key`, the run's redaction key, made when this host submits the
  run. A replacement's record also holds its signed bytes, envelope
  included. No record holds a credential value in the clear.
- `runs/<run>/outputs/`, where `result` writes the run's outputs by default.
- `resubmit/<run>.json.lock`, which every process sharing the directory
  holds while it resubmits that run.

`submit` writes the run's record before it publishes the task, and removes
it only when publishing fails.

### Client configuration example

The configuration of host `laptop`, setting every field:

```yaml
bundle: ~/.config/wherry/laptop.bundle
host_key: ~/.config/wherry/laptop.key
state_dir: ~/.local/state/wherry/laptop
# The CA that signed the broker's and the bucket's certificates, when the machine's trust store lacks it. This value
# is an example: a certificate that expired in 2021 and verifies nothing. Put the PEM of your CA here.
ca_cert: |
  -----BEGIN CERTIFICATE-----
  MIIBbjCCAROgAwIBAgIBATAKBggqhkjOPQQDAjAeMRwwGgYDVQQDExNFeGFtcGxl
  IENBLCBleHBpcmVkMB4XDTIwMDEwMTAwMDAwMFoXDTIxMDEwMTAwMDAwMFowHjEc
  MBoGA1UEAxMTRXhhbXBsZSBDQSwgZXhwaXJlZDBZMBMGByqGSM49AgEGCCqGSM49
  AwEHA0IABKDgH46Z0Jb9ijDRVLk48an3uitOHZeFCHLHl//CE3czOjBUK8aYhf8i
  KDOaGR3cXQMiPQtlhg4wWYT4iLg313ijQjBAMA4GA1UdDwEB/wQEAwICBDAPBgNV
  HRMBAf8EBTADAQH/MB0GA1UdDgQWBBSnYcbbuJwFY2RscVcSxkcCI8YfxTAKBggq
  hkjOPQQDAgNJADBGAiEAtMamhnY5+6DmGNUyq5lrYAwO9q3hy4UBzj6rpOxyY4QC
  IQDwU2k2agkVRkUA3eGCeDwXqbGOQ+zWrjYwV85RVNKMHQ==
  -----END CERTIFICATE-----
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
# Write the host's spans to wherry.traces.<tenant>; off without it.
tracing: true
```

`credentials` holds a source for each credential the agents of the
[pool terms example](configuration-operator.md#pool-terms-example) take: two files, a password
manager's command for `openai-api-key`, and the submitting process's
`GITHUB_TOKEN` for `github-token`, which `git` also gives every task cloning
under `https://github.com/acme/`. `storage` gives pool `build`'s bucket the S3
access key from a file and the secret key from the password manager, in place
of the pair the bundle carries; another pool with storage uses the bundle's
pair, or takes an entry of its own. Each distinct source resolves once at
each submit. A credential whose target is a [catalog variable](credential-variables.md),
such as `claude-code-oauth-token` at `CLAUDE_CODE_OAUTH_TOKEN`, needs no
entry when the variable is set where submits run; an entry for it wins over
the variable.

## Credentials from the submitting process

The host takes each credential of the placed agent from the first source
it has:

1. the source `credentials` names for the credential's name, `file`, `env`
   or `command`;
2. else, when the credential's target is an environment variable that
   appears in the host's credential catalog, that variable in the process
   that submits, when it is set and non-empty.

Any other environment target, and every file target, needs an entry in
`credentials`, so a pool's terms cannot make a host read a variable of the
operator's choosing. The catalog is the one this binary carries: wherry's
harness entries and the committed models.dev snapshot, whose variables are
listed in [Variables read implicitly](credential-variables.md). A required
credential with neither fails the submit before any record is produced,
naming the variable to set and the configuration as the alternative; an
optional one is left out and named `unsupplied`. A configured source that
fails refuses the task and is never passed over for the variable.

::: warning A set catalog variable reaches every run of its agent
An optional credential whose variable is set where submits run reaches
every run of that agent, with no request. Only the
[catalog's variables](credential-variables.md) are read this way: the model
providers' keys, the cloud credentials models.dev lists, such as
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AZURE_API_KEY` and
`GOOGLE_APPLICATION_CREDENTIALS`, and `GITHUB_TOKEN`. Note `GITHUB_TOKEN`.
It is a catalog variable through models.dev's
`github-copilot`, so a pool whose terms give an agent's credential that
target, as the [example](#client-configuration-example)'s `github-token`
has, gets the token from every submitting process that has it set; the
example's explicit `env: GITHUB_TOKEN` reads the same variable and reaches
the same runs. `GH_TOKEN` is not read implicitly. To hold a credential back
from an agent's runs, unset its variable in the process that submits and
remove any explicit source for that credential from this configuration: an
explicit source wins over the variable, so unsetting the variable alone
leaves a configured source in force.
:::

`wherry host init` reports one line per entitled agent, ready or not, from
the sources above, and asks no question for a credential whose target is a
catalog variable; `wherry host receivers` marks each credential's source
and status. Neither runs a `command` source, which they report
`unchecked`. A submit resolves each distinct source once, and a
resubmission after a lost pool key resolves afresh: a variable unset by then
leaves its optional credential out, where a configured source that fails is
an error.

### Passing variables to an MCP server

The process that submits is the MCP server, `wherry host mcp`, so a catalog
variable has to be set in the server's environment. Forward the variable by
name in the server's entry in the harness's MCP configuration, as the table
below shows for each harness. A variable exported in the shell that starts
the harness reaches the server only where the harness passes its own
environment through: Gemini CLI strips variables that look like secrets,
model keys among them, from what its servers inherit, and Codex starts its
servers with a baseline environment of its own, to which the server's
entry adds the parent variables it allowlists. The server's entry is also
the route when the harness is started from a launcher without the
variable. Where neither fits, an explicit `credentials` entry with a `file`
or `command` source is the fallback, and wins over the variable.

| Harness | How a variable reaches its MCP server, per the linked documentation |
| --- | --- |
| [Claude Code](https://docs.anthropic.com/en/docs/claude-code/mcp) | Set the `env` map of the server's entry in `.mcp.json`, or pass `--env VAR=value` to `claude mcp add`. |
| [Codex](https://developers.openai.com/codex/config-reference) | List the parent variables to add to the baseline environment in `env_vars` of the server's `[mcp_servers.<name>]` table in `~/.codex/config.toml`, for example `env_vars = ["CODEX_API_KEY"]`, or set literal values in its `env` table. |
| [Gemini CLI](https://geminicli.com/docs/tools/mcp-server/) | Forward the variable in the `env` map of the server's `mcpServers` entry in `settings.json` with a `$VAR` value, for example `"GEMINI_API_KEY": "$GEMINI_API_KEY"`, since sensitive variables are removed from the environment its servers inherit. |
| [Cursor](https://cursor.com/docs/mcp) | Set the `env` map of the server's `mcpServers` entry in `.cursor/mcp.json`. |
| [Copilot CLI](https://docs.github.com/en/copilot/how-tos/copilot-cli/customize-copilot/add-mcp-servers) | Set the `env` map of the server's entry in `~/.copilot/mcp-config.json`. |

Prefer a variable forwarded by name, as Codex's `env_vars` and Gemini CLI's
`$VAR` do, or a `command` source that reads a keychain or Vault. A literal
value written into such an entry is a copy of the credential in a file the
harness reads. Keep these files out of version control. After changing an entry, call
the `receivers` tool from the harness: it reports each credential's
`source` and `status` as the server sees them, where `wherry host
receivers` in a shell reports that shell's environment.

## Bundle

A YAML file that `wherry operator` writes with mode 0600 under the working
directory's `bundles/`. Unknown fields are errors, and `broker`, `principal`
and `password` are required; a decode error names only its line numbers. A
trailing line break ends the password and is not part of it, and a password
of nothing else is refused. A `storage` block holds both keys or is refused.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `broker` | string | required | Kafka bootstrap address, `host:port` |
| `principal` | string | required | SCRAM-SHA-512 user name |
| `password` | string | required | SCRAM password |
| `storage` | mapping | none | the tenant's S3 `access_key` and `secret_key`, plain strings, written when [`tenant storage`](commands.md#wherry-operator-tenant-storage-tenant) recorded them; used for every pool with storage that `storage` in the configuration does not map |

The Kafka platform owns and renews its CA. The host trusts it through the
machine's trust store plus the configuration's `ca_cert`; a bundle carries
no CA, and one keeping the CA earlier operators wrote is refused naming the
key ([Upgrade](upgrade.md#broker-trust)).
[Broker and S3 trust](security.md#broker-and-s3-trust) says how to install a private CA.

## Environment variables

| Variable | Read by | Effect |
| --- | --- | --- |
| `WHERRY_CONFIG` | host commands | the [client configuration](#client-configuration) path in place of `config.yaml` in the configuration directory; `--config-file` takes precedence |
| `XDG_CONFIG_HOME`, `XDG_STATE_HOME` | host commands | when set, the parents of the host's `wherry/` configuration and state directories in place of `~/.config` and `~/.local/state` |

 `wherry operator`'s connection is
[`operator.yaml`](configuration-operator.md#operator-working-directory).

Go's TLS stack reads its own `SSL_CERT_FILE` and `SSL_CERT_DIR`, no wherry
setting, as [Broker and S3 trust](security.md#broker-and-s3-trust) notes; a
CA the machine's trust store lacks goes in `ca_cert`, for wherry's Kafka and
S3 connections alone.

A plugin reads its own variables from the receiver's environment; the
[Nomad plugin](https://github.com/benemon/wherry-plugins/tree/main/nomad#credentials)
lists its.

Trailing CR and LF are trimmed from password files: `operator.password`, the
super user's password `operator init` reads from a file, standard input or its
prompt, and a bundle's password.
