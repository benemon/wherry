# Development instance configuration

## Where your instance lives

Pass `--instance NAME` to select an instance.
The default name is `default`.
An instance lives in `$XDG_DATA_HOME/wherry/dev/NAME`.
Without `XDG_DATA_HOME`, use `~/.local/share/wherry/dev/NAME`.
The instance directory has mode 0700.

| Path | Purpose | Lifecycle |
| --- | --- | --- |
| `cache/` | Downloads and manifest. | `init` fills it; `down --reset` removes it. |
| `env/` | Running environment and data. | `up` creates it; `down` removes it. |
| `env/up.json` | Record of the Nomad agent started by `up`. | A live recorded agent identifies an instance that is up. |
| `env/host/credentials/` | Credential files copied for the instance. | Files have mode 0600. |
| `env/host/state/` | Host run records. | Removed with the environment. |
| `env/operator/` | Instance operator working directory and bundles. | Created during bootstrap. |

Run one instance at a time.
Ports are fixed on `127.0.0.1`:

| Service | Ports |
| --- | --- |
| Nomad | 4646, 4647, 4648 |
| Kafka | 9093 |
| S3, when enabled | 18080 |

Run one command at a time for each instance.
Keep `XDG_DATA_HOME` unchanged between `up` and `down`.
Without it, keep `HOME` unchanged.
The manifest records host paths as absolute paths.

## Where your host files go

For instance `default`, `up` tries the standard host configuration directory.
It writes `config.yaml`, `bundle.yaml` and `host.key` there only when all three are absent and `WHERRY_CONFIG` is unset.
The configuration points `state_dir` inside the instance.

If a host file already exists, `up` places all host files in `env/host/`.
It also uses `env/host/` for a named instance or when `WHERRY_CONFIG` is set.
It prints `export WHERRY_CONFIG=<path>` for you to use.
It does not write to the path previously named by `WHERRY_CONFIG`.

`down` removes standard host files only while they match the files recorded as created by `up`.
Changed or replaced files remain.
[Development commands](commands-dev.md#wherry-dev-down) gives the matching rules.

## Which environment variables apply

| Variable | Type | Default | Description |
| --- | --- | --- | --- |
| `XDG_DATA_HOME` | path | `~/.local/share` | Parent of `wherry/dev/NAME`. |
| `XDG_CONFIG_HOME` | path | `~/.config` | Parent of the default host configuration directory. |
| `XDG_STATE_HOME` | path | `~/.local/state` | Parent of the default host state directory. |
| `WHERRY_CONFIG` | path | unset | Makes `up` place host files inside the instance and print the path to use. |

See [Development commands](commands-dev.md) for file acquisition and lifecycle steps.
