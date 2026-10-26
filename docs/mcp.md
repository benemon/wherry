# MCP tools

`wherry mcp` serves eight tools over stdio. Each takes and returns JSON with
a declared schema and uses the server's one host. `deadline` and
`queue_deadline` are Go durations, such as `90m`; omitted, they take the CLI
defaults, `2h` and `10m`.

| Tool | Input | Output |
| --- | --- | --- |
| `receivers` | none | `receivers`, the objects `wherry receivers --json` prints |
| `submit` | `prompt`, `prompt_file`, `agent`, `model`, `selector`, `git`, `inputs`, `s3_inputs`, `credentials`, `outputs`, `deadline`, `queue_deadline` | `id`, `pool`, `await` |
| `status` | `id`, optional | `runs`, `skipped` |
| `await` | `id` | `id`, `state`, `model`, `retry_after`, `message`, `artifacts`, `outputs`, `not_returned`, `transcript`, `redacted`, `skipped` |
| `cancel` | `id` | `id`, `skipped` |
| `events` | `id` | `id`, `events`, `transcripts`, `redacted`, `skipped` |
| `transcript` | `id`, `offset`, `limit` | `id`, `attempt`, `total`, `next_offset`, `lines`, `redacted`, `skipped` |
| `cleanup` | `id` | `id` |

- `submit` returns `await`, the shell command that waits for
  the run, `wherry await ID`, with `--config-file` and the server's absolute
  configuration path when the server was started with one.
- `submit` requires exactly one of `prompt` and `prompt_file`, a file path read
  as an input is; an empty file fails with `prompt_file is empty`.
  `model` is as `--model` takes it, and a model outside the agent's `models`
  fails with `INVALID_ARGUMENT` naming the model, agent and pool.
  `selector` is an array of `{key, operator, values}`. `git` is
  `{remote, ref, path, changes}`, as `--git`, `--git-path` and `--git-changes`
  take them, with `remote` and `ref` required; its credential and host key come
  from the client configuration's `git` entries, as for `submit`. `inputs` and
  `s3_inputs` are arrays of file paths, `credentials` an array of `{name, file}`
  or `{name, env}` and `outputs` an array of `{name, required, storage}`,
  as `--input`, `--s3-input`, `--credential` and `--output` take them;
  `storage` `s3` is `:s3`. An input or S3 input must resolve, through any
  symbolic links, to a regular file beneath one of the
  [`input_roots`](configuration.md#client-configuration), and to none of `state_dir`,
  `host_key`, `bundle`, a file a `file` source names or a storage key file; a relative
  path is taken from the server's working directory.
- `status` returns the latest verified state of a run without
  waiting. `runs` holds one entry for the run `id` names, or,
  without `id`, one per run `wherry status` lists, latest update first.
  Each entry is `{id, pool, state, model, message, updated, redacted}`,
  with `model` the run's placement's model, when it has one. The
  state is `TASK_STATE_SUBMITTED` until a receiver reports one. For a run
  this host submitted whose queue deadline passed with no status from a
  receiver, `message` is
  `queue deadline passed at <RFC 3339 time> and no receiver claimed the run`. `updated` is
  the RFC 3339 time of the latest status. While the run has none, it is this
  host's submission of the run when its record holds one, else the Kafka
  timestamp of the run's echo.
- `await` waits up to 45 seconds. It returns the outcome with `message`, the
  status message's parts as `wherry await` prints them, and `artifacts`, or, while the run is unfinished, its current `state` with
  `retry_after` set to 5. For a run whose queue deadline passes with no
  status from a receiver, it returns at once with `state`
  `TASK_STATE_SUBMITTED`, the `message` `status` gives, and no
  `retry_after`. With a progress token it sends a progress notification
  every 5 seconds. Each artifact is `{artifact_id, parts}`, and each part has
  one of `text`, `data`, `url` or `raw_bytes` (the size of a binary part, which
  is not included), with `media_type`. `await` writes the run's verified
  outputs as `wherry await` does, to the default outputs directory, and returns
  each in `outputs` as `{name, path, sha256, size}`, S3 outputs included, which
  it downloads and verifies, then deletes the run's prefix, as `wherry await`
  does. `not_returned` holds `{name, size, reason}` for each output the result
  lists as not returned and each one that fails verification. `transcript` is
  the path of the `transcript.jsonl` it writes as `wherry await` does. `model`
  is the run's placement's model.
- `status`, `await`, `events` and `transcript` show each redaction marker as
  `wherry status` and `wherry await` print it, and `redacted` maps each name
  to the number of its markers in the messages, the artifacts, or every line
  of the transcript's attempt.
- `submit` checks its arguments before it connects to the bus.
- `events` returns `events`, each
  `{attempt, kind, state, receiver, time, message}`, and
  `transcripts`, each `{attempt, chunks}`.
- `transcript` returns the lines `wherry transcript` prints, from `offset`
  lines in. `attempt` is the run's highest attempt, 0 while no receiver has
  reported on the run. `total` counts the
  transcript's lines on the feed so far, less those `wherry transcript`
  drops, and `next_offset` is set while lines remain past the page.
- Each line is `{seq, attempt, text}`, with `seq` its byte offset in the
  agent's output and `attempt` the attempt that published it. `text` is empty
  for an empty line, and for a line that is not UTF-8, which has `raw_bytes`,
  its size, instead, and a line over 512 KiB, which has `unstored`,
  `{sha256, size}`.
- A harness caps a tool's output; Claude Code's `MAX_MCP_OUTPUT_TOKENS`
  defaults to about 25,000 tokens. A `transcript` page fits it: it holds at
  most `limit` lines, 100 by default and 500 at most (the CLI's `--limit`
  of 0 prints every line); a `limit` over 500 or a negative `offset` is
  refused with `INVALID_ARGUMENT`. A page holds at most 24 KiB of
  JSON. It ends before the line that would take it past that. A line too long
  for a page alone comes back as the start of its text that fits, with
  `truncated` true and `bytes` its full size, and `next_offset` passes it.
- `cleanup` deletes the run's S3 objects as `wherry cleanup` does, and refuses
  with `INVALID_ARGUMENT` while the run has not ended.
- `skipped` counts the run's records that failed verification; for a
  `status` list, the records of every listed run.

A failed call returns `{"code", "message"}`. The message never carries
error text from the bus, the broker or the host key.

| Code | Meaning |
| --- | --- |
| `INVALID_ARGUMENT` | an input is missing or malformed; the message names the rule it breaks |
| `RUN_NOT_FOUND` | this host has not submitted the run and no receiver has admitted it |
| `NO_MATCHING_RECEIVER` | no live receiver of an entitled pool can take the agent and selector |
| `OUTPUTS_NOT_WRITTEN` | the run's verified outputs could not be written to the state directory, or its S3 outputs downloaded, or the run's prefix deleted |
| `CANCELLED` | the request was cancelled |
| `VERSION_SKEW` | `wherry.admin` holds records a later release wrote; upgrade `wherry` and restart the MCP server |
| `UNAVAILABLE` | any other failure; the server writes its cause to standard error, which the harness keeps in its MCP server log, and the CLI reports it too |
