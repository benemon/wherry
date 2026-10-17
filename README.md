# wherry

A coding agent runs its subagents on the machine the agent itself runs on.
wherry sends a task to other compute and returns the result to the caller.

## What ships

- `wherry`, a CLI for the submitting side:
  - `init` and `key` create and show the user signing key.
  - `enrol list` shows enrolment and renewal requests, `enrol approve` approves
    one, `enrol renew` renews a receiver approved with `--renewal manual` and
    `enrol revoke` revokes a receiver. Receivers ask for renewal before their
    approval expires; with `--renewal auto` any client command answers.
  - `secret put` seals a credential to one receiver; `--expires` sets its
    lifetime (30 days by default).
  - `receivers` lists enrolled receivers, their offers and sealed credentials,
    and warns about credentials that expire within 7 days.
  - `submit` publishes a task to the matching live receiver with the most spare capacity.
  - `await` waits for a task's reply.
  - `events` prints a task's event history.
- `wherry receiver`, which claims tasks for its pool and runs each one as a
  job through a compile-time Nomad adapter.
- Kafka as the record of each exchange. Tasks, events, transcripts and replies
  are signed records that chain to the user key.
- Records are A2A v1.0 objects; [the wherry A2A extension](docs/a2a-extension-v1.md) defines their added fields.
