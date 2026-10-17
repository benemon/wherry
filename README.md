# wherry

A coding agent runs its subagents on the machine the agent itself runs on.
wherry sends a task to other compute and returns the result to the caller.

## What ships

- `wherry`, a CLI for the submitting side:
  - `init` and `key` create and show the user signing key.
  - `enrol list` shows enrolment and renewal requests. `enrol approve`
    approves one; `--renewal auto|manual` sets the renewal policy (`auto` by
    default) and `--expires` sets the approval lifetime (30 days by default).
    `enrol renew` renews a receiver approved with `--renewal manual`.
    `enrol revoke` revokes a receiver; credentials sealed to it are not
    rotated, so rotate them at their source.
  - Receivers ask for renewal before their approval expires. With
    `--renewal auto`, any command that connects to Kafka answers the request,
    and only when the renewal extends the approval. A renewal runs 30 days
    from when it is signed.
  - `secret put` seals a credential to one receiver; `--expires` sets its
    lifetime (30 days by default). It lists the runs in the receiver's pool
    that are waiting for that credential.
  - `receivers` lists enrolled receivers, their offers and sealed credentials,
    and warns about credentials that expire within 7 days.
  - `submit` publishes a task to the matching live receiver with the most
    spare capacity.
  - `continue` (alias `resume`) publishes the next turn of an interrupted
    run. `--prompt` sets its text (`continue` by default);
    `--deadline` and `--queue-deadline` set its deadlines.
  - `await` waits for a task's reply.
  - `events` prints a task's event history.
- `wherry receiver`, which claims tasks for its pool and runs each one as a
  job through a compile-time Nomad adapter.
- Kafka as the record of each exchange. Tasks, events, transcripts and replies
  are signed records that chain to the user key.
- Task, event, transcript and reply records are A2A v1.0 objects;
  [the wherry A2A extension](docs/a2a-extension-v1.md) defines their added
  fields.
