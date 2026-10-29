# MCP tools

`wherry mcp` serves eight tools over stdio, and with `--listen` the same
tools and `upload` over Streamable HTTP, in the [forms](#over-http) a remote
client uses. Each takes and returns JSON with
a declared schema and uses the server's one host. `deadline` and
`queue_deadline` are Go durations, such as `90m`; omitted, they take the CLI
defaults, `2h` and `10m`.

| Tool | Input | Output |
| --- | --- | --- |
| `receivers` | none | `receivers`, the objects `wherry receivers --json` prints |
| `submit` | `prompt`, `prompt_file`, `agent`, `model`, `selector`, `git`, `inputs`, `credentials`, `outputs`, `deadline`, `queue_deadline` | `id`, `pool`, `wait` |
| `status` | `id`, optional | `runs`, `skipped` |
| `wait` | `id` | `id`, `state`, `model`, `retry_after`, `message`, `reply`, `artifacts`, `outputs`, `not_returned`, `redacted`, `skipped` |
| `cancel` | `id` | `id`, `skipped` |
| `events` | `id` | `id`, `events`, `chunks`, `redacted`, `skipped` |
| `logs` | `id`, `offset`, `limit` | `id`, `attempt`, `total`, `next_offset`, `lines`, `redacted`, `skipped` |
| `cleanup` | `id` | `id` |

- `submit` returns `wait`, the shell command that waits for
  the run and prints its reply, `wherry wait ID`, with
  `--config-file` and the server's absolute configuration path when the
  server was started with one.
- `submit` requires exactly one of `prompt` and `prompt_file`, a file path read
  as an input is; an empty file fails with `prompt_file is empty`. The
  server's standard input carries the MCP protocol, so `prompt_file` `-`
  names a file called `-`.
  `model` is as `--model` takes it, and a model outside the agent's `models`
  fails with `INVALID_ARGUMENT` naming the model, agent and pool.
  `selector` is an array of `{key, operator, values}`. `git` is
  `{remote, ref, path, changes}`, as `--git`, `--git-path` and `--git-changes`
  take them, with `remote` and `ref` required; its credential and host key come
  from the client configuration's `git` entries, as for `submit`. `inputs` is
  an array of `{path, storage}`, `credentials` an array of `{name, file}`
  or `{name, env}` and `outputs` an array of `{name, required, storage}`,
  as `--input`, `--credential` and `--output` take them;
  `storage` is `s3` or absent, and `s3` is `:s3`. An input must resolve, through any
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
- `wait` waits up to 45 seconds. It returns the outcome with `message`, the
  status message's parts, and `artifacts`, or, while the run is unfinished, its current `state` with
  `retry_after` set to 5. For a run whose queue deadline passes with no
  status from a receiver, it returns at once with `state`
  `TASK_STATE_SUBMITTED`, the `message` `status` gives, and no
  `retry_after`. With a progress token it sends a progress notification
  every 5 seconds. The artifacts are the result's: `result`, then a
  [reference](a2a-extension-v1.md#reference-artifacts) for each returned
  output, the reply over 64 KiB and the transcript. Each artifact is `{artifact_id, parts}`, and each part has
  one of `text`, `data`, `url` or `raw_bytes` (the size of a binary part, which
  is not included), with `media_type`. `wait` writes the run's verified
  outputs as `wherry wait` does, to the default outputs directory, and returns
  each in `outputs` as `{name, path, sha256, size}`, S3 outputs included, which
  it downloads and verifies, then deletes the run's prefix, as `wherry wait`
  does. `not_returned` holds `{name, size, reason}` for each output the result
  lists as not returned and each one that fails verification. For a
  [reply](a2a-extension-v1.md#the-reply) over 64 KiB, `message` holds its
  first 64 KiB, and `wait` writes the verified reply to `reply.txt` among the
  run's outputs and returns `reply` as `{path, sha256, size, cut}`, with
  `sha256` and `size` of the file and `cut` set when the reply was longer than
  the pool's `reply_limit`; when the chunks do not verify, `reply` is
  `{rejected}` with the reason. A reply within 64 KiB is `message` whole, with
  no `reply`. No expected output takes the name `reply.txt`. For a
  run whose queue deadline passed unclaimed it writes nothing. `model`
  is the run's placement's model. The `wait` tool returns no transcript;
  `logs` pages it.
- `status`, `wait`, `events` and `logs` show each redaction marker as
  `wherry status` and `wherry wait` print it, and `redacted` maps each name
  to the number of its markers in the messages, the artifacts, or every line
  of the transcript's attempt.
- `submit` checks its arguments before it connects to the bus.
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
- A harness caps a tool's output; Claude Code's `MAX_MCP_OUTPUT_TOKENS`
  defaults to about 25,000 tokens. A `logs` page fits it: it holds at
  most `limit` lines, 100 by default and 500 at most (`wherry logs` prints
  every line). A `limit` over 500 or a negative `offset` is refused with
  `INVALID_ARGUMENT`. The `logs` tool returns one page per call and has no
  follow mode. A page holds at most 24 KiB of
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
| `OUTPUTS_NOT_WRITTEN` | the run's verified outputs could not be written, its S3 outputs downloaded, the run's prefix deleted or, over HTTP, an output presigned; the message starts with the step, `write outputs`, `S3 outputs` or `presign outputs`, and the server writes the cause to standard error |
| `CANCELLED` | the request was cancelled |
| `VERSION_SKEW` | `wherry.admin` holds records a later release wrote, with a `v` higher than this release knows; upgrade `wherry` and restart the MCP server |
| `UNAVAILABLE` | any other failure; the server writes its cause to standard error, which the harness keeps in its MCP server log, and the CLI reports it too |

## Over HTTP

`wherry mcp --listen` serves the tools to a client on another machine, which
names none of the host's files and reads the outputs through MCP or S3.
Every other tool and result is as over stdio.

- `submit` takes each of `inputs` as `{name, content}`, with `content` the
  file's bytes in base64, or as `{upload}`, naming an `upload` id. `storage`
  `s3` sends `content` through the pool's bucket. An input with `path` fails
  with `INVALID_ARGUMENT`
  `inputs over HTTP carry their content: each is {name, content} or {upload}`,
  and `prompt_file` with `prompt_file is refused over HTTP; prompt carries the text`.
  Content is bounded by the profile's inline limit and the pool's record limit
  as a path input is, and moves to the pool's bucket under the same rules.
  The result has no `wait`.
- `upload` takes `{name, size, sha256}`, with `sha256` in lowercase hex, and
  returns `{id, url, expires}`: a presigned PUT to the bucket of the first of
  the tenant's pools by name with storage, valid for `--url-lifetime`, `1h` by
  default. The client PUTs the file to `url`, for example with
  `curl --upload-file FILE URL`, and names `{upload: id}` in `submit`. `submit`
  copies the object to an unlinked file in `state_dir` as it hashes it, fails
  with `nothing was put to upload <id>` or
  `upload <id> does not match its size and sha256`, and otherwise sends the
  verified bytes as the run's S3 input. Once the task is published it deletes
  the upload's object and record. Without a pool with storage or a host
  storage credential, `upload` fails as an S3 input does.
- `wait` returns each output as `{name, sha256, size, uri}`, with `uri`
  `wherry://runs/<run>/outputs/<name>`, and adds a resource link for each
  after the JSON text. An output over `--output-bound`, 1 MiB by default, or
  returned through S3 is `{name, sha256, size, url, expires}` instead: the
  host puts the verified file at `wherry/<tenant>/<run>/host/<name>` in the
  run's pool's bucket, a key no receiver's PUT URL names, and presigns a GET
  valid for `--url-lifetime`. A later `wait` reuses an object of the same
  size. A run whose pool or host has no storage returns every output as a
  `uri`. `reply` takes the same form for `reply.txt`. No result carries a
  host path.
- `resources/read` of an output's `uri` returns the file the host wrote
  beneath `state_dir/runs/<run>/outputs/`: `text` when it is UTF-8, else
  `blob` in base64. `resources/templates/list` names the template
  `wherry://runs/{run}/outputs/{+name}`. A name that leaves the run's output
  directory, through `..` or a symbolic link, is not found.
- `cleanup` also deletes `state_dir/runs/<run>/outputs/`. A run whose pool
  or host has no storage has nothing in S3, and `cleanup` deletes its local
  outputs alone.
