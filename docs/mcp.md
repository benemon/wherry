# MCP tools

Run `wherry host mcp` to serve seven tools over stdio.
Each tool takes and returns JSON with a declared schema.
All tools use the server's host identity.

`start_by` and `end_by` set `task_started_by` and `task_ended_by`.
Pass an RFC 3339 time or a Go duration from now, such as `90m`.
The defaults are `10m` and `2h`.

| Tool | Input | Output |
| --- | --- | --- |
| `receivers` | none | `receivers`, the objects `wherry host receivers --json` prints |
| `submit` | `prompt`, `agent`, `model`, `selector`, `git`, `continue`, `inputs`, `credentials`, `outputs`, `start_by`, `end_by`, `wait` | `id`, `pool`, `command`, `unsupplied`, `skipped_hidden`, `result`, `error` |
| `status` | `id`, optional | `runs`, `skipped` |
| `result` | `id` | `id`, `state`, `model`, `retry_after`, `message`, `reply`, `artifacts`, `outputs`, `not_returned`, `fetched_ref`, `not_fetched`, `redacted`, `skipped`, `runs` |
| `cancel` | `id` | `id`, `cancel`, `skipped` |
| `events` | `id` | `id`, `events`, `chunks`, `redacted`, `skipped` |
| `logs` | `id`, `offset`, `limit` | `id`, `attempt`, `total`, `next_offset`, `lines`, `redacted`, `skipped` |

- `submit` returns `command`, the shell command that waits for
  the run and prints its reply, `wherry result ID`, with
  `--config-file` and the server's absolute configuration path when the
  server was started with one or with `WHERRY_CONFIG` set.
- `submit` requires `prompt`: the text, or `@FILE` for the contents of a
  file read as an input is, beneath the input roots and never one of the
  host's own files; an empty prompt, however given, fails with `prompt is
  empty`. The server's standard input carries the MCP protocol, so `@-` is
  refused.
  `model` is as `--model` takes it, and a model outside the agent's `models`
  fails with `INVALID_ARGUMENT` naming the model, agent and pool.
  `selector` is an array of `{key, operator, values}`. `git` is
  `{remote, ref, path, changes}`, as `--git`, `--git-path` and `--git-changes`
  take them, with `remote` required and `changes` `bundle` unless
  set to `patch` or `none`; its credential and host key come
  from the client configuration's `git` entries, as for `submit`. Without
  `ref`, the task carries the commit the remote's `HEAD` names at submit.
  A `remote` of `.` is the repository holding the server's working
  directory, its `origin` at the commit checked out, which must be on the
  remote, and takes no `ref`. Both run `git` on the host, as
  [`submit`](commands.md#wherry-host-submit) describes, and fail with
  `INVALID_ARGUMENT` without it, or naming the step that failed. `continue`
  is a run ID, as `--continue` takes it, and is refused with `git`: a run
  with `git` continues from its `changes.bundle`, and any other run delivers
  the outputs `result` collected as the inputs `previous/<name>`, read from
  where `result` wrote them, the default outputs directory included, which
  the input roots do not apply to; a run never collected is refused with
  `run <id> has no collected outputs; run wherry result <id> first`. `inputs` is
  an array of `{path}`, `credentials` an array of `{name}`,
  `{name, file}` or `{name, env}` and `outputs` an array of `{name}`,
  as `--input`, `--credential` and `--output` take them; every output is
  required, and one over the pool's output limit returns through the pool's
  bucket. An input's or an output's `storage` is refused with `inputs move to
  S3 by size; drop :s3` or `outputs move to S3 by size; drop :s3`, and an
  output's `required` with `outputs are always required; drop :required`.
  An input's delivered name, like an output's, is a relative path of file
  names joined by `/`, no name a directory of another and none under
  `previous/`; a `{path}` input that is a file is delivered under its base
  name, and one that is a directory delivers every file beneath it as
  `<name>/<relative path>`, as `--input DIR` does: entries named with a
  leading dot are skipped and counted in `skipped_hidden`, and a symbolic
  link, device, socket or FIFO beneath it, or a directory with no file left,
  refuses the submission naming it.
  An input must resolve, through any
  symbolic links, to a regular file or directory beneath one of the
  [`input_roots`](configuration-host.md#client-configuration), and opens through that
  root, so a link that leads out of it, even one that appears after the path
  resolves, is refused. An input root is matched by its path with symbolic links
  resolved when the server starts; an input that spells a root in different case,
  even on a case-insensitive file system, is refused. It must resolve to none of `state_dir`,
  `host_key`, `bundle`, a file a `file` source names or a storage key file, and a
  directory holding one of them refuses the whole submission; a relative
  path is taken from the server's working directory. Keep `state_dir` outside every
  input root. With the default `state_dir`, `~/.local/state/wherry`, a root of `~`,
  `~/.local`, `~/.local/state` or `~/.local/state/wherry` holds it; `XDG_STATE_HOME`
  moves it.
- A `credentials` entry with a name alone names one of the placed agent's
  optional credentials the task cannot go without. Every agent credential
  the host resolves is delivered at the target the pool's terms give it,
  required or optional, named or not; `receivers` lists each credential with
  its `description`, `optional`, `configured`, `source`, `variable` and
  `status`, so read it to see what a run will get. `submit` returns
  `unsupplied`, the placed agent's optional credentials the host could not
  resolve; the run goes without them. A name that is not one of the placed
  agent's credentials, or one the host cannot resolve, fails with
  `INVALID_ARGUMENT` and the message `wherry host submit` gives.
- `status` returns the latest verified state of a run without
  waiting. `runs` holds one entry for the run `id` names, or,
  without `id`, one per run `wherry status` lists, latest update first.
  Each entry is `{id, pool, state, model, message, updated, redacted, replaced_by}`,
  with `model` the run's placement's model, when it has one. The
  state is `TASK_STATE_SUBMITTED` until a receiver reports one. For a run
  this host submitted whose `task_started_by` passed with no status from a
  receiver, `message` is
  `not started by task_started_by (<RFC 3339 time>): no receiver claimed the task; now: <reason>`.
  Before then, while no receiver has reported a status, `message` is the
  run's [waiting reason](commands.md#why-a-run-waits), such as
  `queued: claude on nomad-1 1/1 in use`, recomputed from the receivers' live
  advertisements at each call. After `cancel`, and until a receiver reports
  a status, `message` adds
  `cancel requested <RFC 3339 time>; applies when a receiver claims the task`,
  the notes joined by `; `. `updated` is
  the RFC 3339 time of the latest status. While the run has none, it is this
  host's submission of the run when its record holds one, else the Kafka
  timestamp of the run's echo. A run that ended with the wherry event
  `key-lost` before its `task_started_by` is resubmitted under a new run ID,
  as `wherry status` does, and `replaced_by` is the replacement's run ID.
- `receivers` gives each ready profile `replicas` and `in_flight` from its
  offer. A receiver's `capacity` and `in_flight` are totals over its
  profiles. A profile claims one task per replica, so a profile at
  `in_flight` equal to `replicas` is full whatever its receiver's totals.
- `cancel` returns `cancel` `requested` and `id`, the run cancelled: for a
  run resubmitted after it lost its pool key, the last replacement. The
  cancel applies when a receiver holds or claims the run.
- `result` waits up to 45 seconds for the run to end;
  [The `result` tool](#the-result-tool) gives its fields.
- A run that ends with the wherry event `key-lost` before its
  `task_started_by` never started: its pool key was lost. `result` resubmits
  it under a new run ID and follows each replacement, as `wherry result` does.
  `runs` then lists every run it followed, the run whose outcome it returns
  last, and `outputs` are that run's. `id` stays the run the call named. A
  resubmission that fails, for example on a suspended pool, fails the call.
- `status`, `result`, `events` and `logs` show each redaction marker as
  `wherry status` and `wherry result` print it, and `redacted` maps each name
  to the number of its markers in the messages, the artifacts, or every line
  of the transcript's attempt.
- Before it connects to the bus, `submit` checks that `prompt` is given and
  reads it, each input's `storage`, and `start_by` and `end_by`, and walks
  its input directories. It checks the rest, such as the agent,
  model and git host against the pool's terms, once connected, and opens
  its input files once every input's name has passed its checks.
- `events` returns `events`, each
  `{attempt, kind, state, receiver, time, message}`, and
  `chunks`, each `{attempt, chunks}`.
- `logs` returns the lines `wherry logs` prints, from `offset`
  lines in. `attempt` is the run's highest attempt, 0 while no receiver has
  reported on the run. `total` counts the
  transcript's lines on the transcript topic so far, less those `wherry
  logs` drops, and `next_offset` is set while lines remain past the
  page.
- Each line is `{seq, attempt, text}`, with `seq` its byte offset in the
  agent's output and `attempt` the attempt that published it. `text` is empty
  for an empty line, and for a line that is not UTF-8, which has `raw_bytes`,
  its size, instead, and a line over 512 KiB, which has `unstored`,
  `{sha256, size}`.
- A harness caps a tool's output, as its documentation states, such as
  [Claude Code's](https://docs.anthropic.com/en/docs/claude-code/mcp). A
  `logs` page holds at most `limit` lines, 100 by default and 500 at most (`wherry logs` prints
  every line). A `limit` over 500 or a negative `offset` is refused with
  `INVALID_ARGUMENT`. The `logs` tool returns one page per call and has no
  follow mode. A page holds at most 24 KiB of
  JSON. It ends before the line that would take it past that. A line too long
  for a page alone comes back as the start of its text that fits, with
  `truncated` true and `bytes` its full size, and `next_offset` passes it.
- `skipped` counts the run's records that failed verification; for a
  `status` list, the records of every listed run.

## The `result` tool

`result` waits up to 45 seconds, and with a progress token sends a progress
notification every 5 seconds. While the run is unfinished it returns the
run's `state` with `retry_after`, and the `message` `status` gives, such as
the step the agent last reported; for a resubmitted run, those of the last
replacement, the run `cancel` names. Past the run's recorded `task_ended_by`
plus the upload grace, the bound `wherry result` waits to, or, for a run
recorded before `task_ended_by` was, past `2h10m` from its submission, the
fallback `wherry result` takes, an unfinished run returns no `retry_after`,
and `message` starts with the timeout `wherry result` exits 6 with. For a run whose `task_started_by` passes
with no status from a receiver, it returns at once with `state`
`TASK_STATE_SUBMITTED`, the `message` `status` gives and no `retry_after`,
and writes nothing.

Once the run ends, `result` writes the run's verified outputs as `wherry result`
does, to the default outputs directory. It downloads and verifies the S3
outputs, then deletes the run's prefix, as `wherry result` does. For a run
submitted with `git` `remote` `.`, or a `continue` of one, it fetches a
returned `changes.bundle` into that repository's branch `wherry/<run>` as
`wherry result` does, and reports it in `fetched_ref`, or why it did not in
`not_fetched` ([`result`](commands.md#wherry-host-result-run-id)). It returns no
transcript; `logs` pages it.

| Field | Content |
| --- | --- |
| `id` | the run the call named |
| `state` | the run's state |
| `model` | the run's placement's model, when it has one |
| `retry_after` | 5, while the run is unfinished and within its bound |
| `message` | the status message's parts; for a [reply](a2a-extension-v1.md#the-reply) over 64 KiB, its first 64 KiB; while the run is unfinished, its latest status message |
| `reply` | for a reply over 64 KiB, `{path, sha256, size, cut}` of `reply.txt`, which `result` writes among the run's outputs, with `cut` set when the reply was longer than the pool's `reply_limit`; `{rejected}` with the reason when the reply's chunks do not verify; absent for a reply within 64 KiB |
| `artifacts` | the result's: `result`, then a [reference](a2a-extension-v1.md#reference-artifacts) for each returned output, the reply over 64 KiB and the transcript; each is `{artifact_id, parts}`, and each part has one of `text`, `data`, `url` or `raw_bytes` (the size of a binary part, which is not included), with `media_type` |
| `outputs` | `{name, path, sha256, size}` for each output written, S3 outputs included |
| `not_returned` | `{name, size, reason}` for each output the result lists as not returned and each one that fails verification |
| `fetched_ref` | for a run submitted with `git` `remote` `.` whose `changes.bundle` came back, the branch it was fetched into, or already stood at, `wherry/<run>`, in the repository the run's record names, wherever the server runs; `<run>` the run whose bundle it is; the working tree, index and `HEAD` are untouched, and `git merge wherry/<run>` is the next step |
| `not_fetched` | for such a run, why the bundle was not fetched: `exists`, the branch names another commit and was left; `no git`; `no repository`; `git refused`; `unreadable bundle`; `wrong commit`, the branch the fetch made named another commit and was deleted; or `timeout`, past 10 minutes; the bundle at its output path can be fetched by hand with `git fetch <path> HEAD` |
| `redacted` | each redaction marker's name and its count |
| `skipped` | the run's records that failed verification |
| `runs` | for a run resubmitted after `key-lost`, every run followed, the run whose outcome this is last |

No expected output takes the name `reply.txt`.

## Errors

A failed call returns `{"code", "message"}`. The message never carries
error text from the bus, the broker or the host key.

| Code | Meaning |
| --- | --- |
| `INVALID_ARGUMENT` | an input is missing or malformed; the message names the rule it breaks |
| `RUN_NOT_FOUND` | this host has not submitted the run and no receiver has admitted it |
| `NO_MATCHING_RECEIVER` | no live receiver of an entitled pool can take the agent and selector |
| `OUTPUTS_NOT_WRITTEN` | the run's verified outputs could not be written, its S3 outputs downloaded or the run's prefix deleted; the message starts with the step, `write outputs` or `S3 outputs`, and the server writes the cause to standard error |
| `CANCELLED` | the request was cancelled |
| `VERSION_SKEW` | `wherry.operator` holds records a later release wrote, with a `v` higher than this release knows; upgrade `wherry` and restart the MCP server |
| `UNAVAILABLE` | any other failure; the server writes its cause to standard error, which the harness keeps in its MCP server log, and the CLI reports it too |

Set submit's `wait` to `true` only for runs expected to finish within 45 seconds;
otherwise call `result`. Omitted or false returns the submission alone.
`command` is always present, to collect later; it is not needed when `result`
is final. With `wait: true`, the reply is nested under `result`, including
`retry_after` while unfinished. Its `id` is the submitted run and `runs` lists
any replacements followed.

Publishing is not cut short. The time spent preparing and publishing the task
reduces the 45 second waiting budget. When none remains, submit returns the
latest status without waiting. Output collection after completion is outside
that budget. Progress starts after publish when there is a run ID.

A failure after successful publication returns `id`, `pool`, `command` and a
sanitised `error` object with `code` and `message`, without `result`. A response
lost to transport cancellation loses the ID as any tool response would;
`status` lists this host's runs.
