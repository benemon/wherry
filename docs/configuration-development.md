# Development instance configuration

## Where your instance lives

`wherry dev` runs a whole deployment on one machine for trying wherry and
developing against it, as [Try it on one machine](quickstart-development.md#start-your-instance)
walks through. Each command takes `--instance NAME`, default `default`, a
[name](#names). An instance lives in `$XDG_DATA_HOME/wherry/dev/NAME`, by
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

## Where your host files go

[`wherry dev up`](commands-dev.md#wherry-dev-up) creates `config.yaml`,
`bundle.yaml` and `host.key` here for its `default` instance when none of
them is there and `WHERRY_CONFIG` is not set, with `state_dir` inside the
instance, and records what it created; `wherry dev down` removes only those, while each
is still the file `up` created. It never replaces a real host's files:
otherwise, and for every other instance, it writes them inside the
instance and prints the `WHERRY_CONFIG` to use.

## Which environment variables apply

| Variable | Type | Default | Description |
| --- | --- | --- | --- |
| `XDG_DATA_HOME` | path | `~/.local/share` | Parent of `wherry/dev/NAME`. |
| `XDG_CONFIG_HOME` | path | `~/.config` | Parent of the default host configuration directory. |
| `XDG_STATE_HOME` | path | `~/.local/state` | Parent of the default host state directory. |
| `WHERRY_CONFIG` | path | unset | Makes `up` place host files inside the instance and print the path to use. |

See [Development commands](commands-dev.md) for file acquisition and lifecycle steps.
