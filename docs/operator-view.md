# Operator view

`wherry operator view` is a read-only map of one cluster for its operator: every broker, pool, receiver, custodian,
host, tenant and S3 destination the records name, the runs of a window with their path, state, status messages,
failure class and reason, what each receiver holds, each pool's share-group lag, and for a selected run the spans its
tenant's trace topic holds and the live transcript its tenant's transcript topic holds. It rebuilds the window from
the records Kafka keeps, then tails them, and serves JSON, a Server-Sent Events stream and a placeholder page of plain
tables on loopback. It never produces to Kafka.

```sh
wherry operator view [--dir DIR] [--window 24h] [--port N] [--external-url URL]
```

The view connects as `wherry.operator` through the working directory's `operator.yaml`
([Working directory](configuration-operator.md#operator-working-directory)); it needs the operator's `Describe` on
the pool groups for the share-group state, which `init` grants
([The operator principal](security.md#the-operator-principal)).

## Running it locally

```sh
wherry operator view --dir /srv/wherry/operator
```

The command reads `wherry.operator`, every key topic, the advertisements, custodian snapshots and progress topics,
then every pool's events and every tenant's feed from `--window` ago, 24 hours by default, in bounded batches until
each reaches the end it listed. It prints how many runs it rebuilt and how long that took on standard error, then
one line on standard output:

```
http://127.0.0.1:41873/#t=0b6d...e1
```

Open that address in a browser on the same machine. The token after `#t=` is in the fragment, which browsers never
send to a server, so it reaches no access log; the page posts it once to `/session`, gets a cookie with a session ID
the server issued, and clears the fragment from the address bar. Every API and stream request then carries the
cookie. Ctrl-C stops the view.

The view binds `127.0.0.1` only, on `--port` or a free port. No flag names another address.

## What the model holds

Every signed record is verified against its writer's key as the signed records are everywhere in wherry, with the
key history: a key verifies the records appended before its retirement, so a record signed before a rotation is
present and one signed with the retired key after it is not. A host cannot sign a status, nor a receiver a cancel: a
record whose text names another principal than the one whose key verified it is refused. A record is also checked
against the topic it was read from: a status, start failure or result names the pool whose events, or the tenant
whose feed, it was read from, an echo names the tenant whose feed holds it, a cancel the pool and tenant of its run.
A tenant's feed is open to every receiver entitled to the tenant, so an echo, status or result on it must also name
the pool its receiver is recorded in.
Each record is verified once; the copy of a status on the tenant feed is the copy on the pool's events, found by
digest.

Membership and entitlement over time are not re-derived: who may write a topic, and when, is decided by the Kafka
ACLs at write time, and by the hosts and receivers that act on the records. A run completed by a receiver since
removed is present, and so is a status a still-credentialed receiver wrote during a closed membership or entitlement
interval; that is the accepted residual, and each such fact is shown with its signer. Every fact from a signed record
carries `signer`, the principal of the key that verified it, `wherry.rcv.<receiver>` or `wherry.tnt.<tenant>.<host>`,
built from the configuration, never from the record's text: a wrap the custodian signed naming another receiver is
labelled with the custodian. Operator, progress and trace records are unsigned; they are authenticated by their topic,
written under `wherry.operator`'s or the receiver's ACL, so a run a receiver holds carries `source`, the progress
topic it was read from, not a signer.

The key topics of every receiver and host, ended or not, are tailed before each batch of records is folded, so a
rotation is seen within a tick. A rotation retires a key from its append time; a record signed with it and appended
after that no longer verifies, and when the rotation is read after such records were folded, the facts taken from
them are retracted and counted as `retracted`. Facts under other keys stand, an echo's dependents included.

The model keeps named fields and, of what an agent wrote, the text that says what is going on and why a run ended as
it did: from a status its kind, state, event ID, attempt, receiver, append time, log offset, event, a failure class
and its message; from a result the output names, sizes and hashes, the reply's size and where it ran. The message kept
is the status's text as the receiver redacted it: a working status's line, at most 512 bytes, and a failed, canceled
or rejected terminal's reason, cut at 1 KiB and marked `truncated`; a start failure carries a class and no text, so it
has no message. A completed terminal's text is the agent's reply and is not kept, nor is a reply, an artifact or a task's text, nor a result's labels or its job name. Message bytes
count in the run's retained size, so the `run_bytes` cap bounds them as it does every other fact, the oldest events
dropped first. The run's detail carries each message on its event and the failure reason, the highest attempt's
terminal message, as `reason`. The transcript's lines are not held in the model; they are read from the tenant's
transcript topic on request, as [The transcript](#the-transcript) describes. No other string a record or span carries
reaches a response, the stream or the log unless it is rebuilt from the operator's configuration or a fixed set: a
tenant, pool, receiver, host, agent, model, profile or topic is shown when the configuration, the pool's terms over
the window or the topic names wherry derives from them name it, and as `unknown` otherwise; a status kind, task state,
start failure class, claim or refusal reason, advertisement gate, share-group state and HTTP method come from their
fixed sets, anything else showing as `other`; a run ID, event ID, task hash, key ID or SHA-256 is shown when it follows
the grammar wherry issues, an event ID outside it being replaced by one derived from the record's digest and a record
keyed by anything but a run ID refused. Output names are the tenant's own: a result shows each name when it is
slash-separated segments of letters, digits, `.`, `_` and `-`, the grammar receivers and hosts enforce, and
`(invalid name)` otherwise; that alphabet needs no escaping on any surface. Messages, lines and output names are JSON
strings, and the page renders them with `textContent` like everything else, under a Content-Security-Policy that
allows no inline script. The failure class comes from a fixed table, since the records carry no class but a start
failure's: `start-failure:dispatch`, `start-failure:platform` and `start-failure:input-too-large` from a start
failure; `key-lost`, `failed` and `canceled` from a failed or canceled terminal status; `rejected` from a rejection;
`unclassified` for a terminal status in another state.

What the messages and lines can hold is what the receiver let through: it redacts each credential of 8 bytes or more
that it delivered to the agent, every key, presigned URL and signature it holds, from everything it publishes for the
attempt except output files ([Credential envelopes](security.md#credential-envelopes)). A value shorter than 8 bytes is not
redacted, nor is a value the agent transformed before writing it, so the view shows what the tenant's own hosts and
calling agents read on the same topics, nothing the operator could not read with `wherry.operator`'s grant.

A receiver's runs in flight come from its progress records and its advertised count: a record of a run whose terminal
the model holds at the same or a higher attempt, or naming another tenant or task than the run's echo, is discarded;
a live receiver's advertised count is its number in flight, the progress records supply identities up to that count,
newest first, and the rest show as stale; where the count exceeds the identities the remainder shows as `in flight,
not identified`. A stale or silent receiver's runs show as `unknown`, not ended. A released run that exhausted its
pool's delivery limit without a terminal shows as `no terminal recorded`.

A run's S3 destination is the pool's `storage` in the terms in force when the run was admitted, from the terms
history on `wherry.operator`; a run without an echo in the window, or admitted before the first terms record read,
shows its storage as unknown. Each pool's lag is the share group's start offset against its task topics' end offsets,
labelled records not yet acknowledged past the start offset: `ok` with a count, `zero`, `unavailable` when the group
or its offsets cannot be described, and `error` with the error's class, polled every 10 seconds with the
advertisements and broker metadata.

A topic the state calls for but the cluster lacks is listed as missing and skipped. Where the earliest retained
record of a topic is newer than the window's start, the topic is marked as predating the window: retention cut the
window short.

## The transcript

`GET /api/runs/{id}/transcript` serves a page of a run's transcript, the lines the agent wrote to standard output as
its receiver published them, one chunk each on `wherry.tenant.<T>.transcript`
([Chunk](a2a-extension-v1.md#chunk)). The query names exactly one of `tail=N`, the last N lines; `before=<seq>&limit=N`,
the N lines below a seq; and `from=<seq>&limit=N`, the N lines at or above one; N is 1 to 1,000. A request with
`before` or `from` also carries the cursor's `attempt` and, when the last response had one, its `first`. The response
holds `lines`, each with `at`, the record's append time, `attempt`, `receiver`, the signer's principal, `seq`, the
line's byte offset in the agent's output, and `text`; a line that is not valid UTF-8 has `raw: true` and its `size`
instead, and one the receiver did not store, over 512 KiB, `oversize: true` with its `size`; a raw part that is valid
UTF-8, an empty line included, is `text`; a line over 16 KiB is cut with `truncated: true` and its `size`. `prev` is
the seq to ask `before` for older lines and `next` the seq to ask `from` for later ones, present while the run may have
more or the page was cut; `attempt` and `first` are the attempt shown and the lowest seq of its lines, and `reset:
true` says either differs from the cursor's: an adopted run's new attempt hides the earlier attempt's lines at or above
its first seq, so the page reloads from the tail. `complete` is set once the attempt's last chunk was read, or the run
is terminal and the scan reached the end listed after its terminal; `predates_log` when retention removed the span's
start; `missing` when the tenant has no transcript topic. `retry: true` says a request budget cut a `tail` request's
scan before the run's span was indexed: the response has no lines, since the lines indexed so far are not the tail,
and the page asks `tail` again until the scan ends. `read` and `seek` report the records and bytes the request's scan
and seek read, `truncated` names each cap that cut it, and `evicted` counts the other runs' indexes dropped to make
room for this run's lines. A whole older page's `next` is the seq asked `before`, so the page tells a cut older page,
whose `next` is lower, from a whole one.

Which lines a run shows is the attempt rule every reader follows, the host's `wherry host logs` included: the highest
attempt's lines, and below that attempt's first seq the lines of earlier attempts, since an adopter resumes the job's
output past the run's latest committed offset and a seq can repeat across attempts; of chunks sharing a seq the
highest attempt's is the line, and the empty final chunk is no line. A chunk counts when it is a chunk by its JWS
type, an output chunk on the same topic being dropped before it is verified; verifies under the key history, as every
signed record does, which binds the receiver it names to its signer; names the run's tenant, its recorded pool and
task hash; and is signed by a receiver recorded in the run's pool. The last rule is the model's own for the feed: the
tenant's topics are open to every receiver entitled to the tenant, so the ACL does not bind the pool, and a receiver of
another pool could otherwise claim the run's pool in a chunk of its own. A removed receiver's chunks stay, shown with
their signer, as the view's other facts do: that is the same residual as elsewhere in the model. The index is bound
to the pool and task hash the model records for the run and is rebuilt when either changes. When a rotation read later retires a key, the indexes of the runs holding that
owner's chunks are dropped and rebuilt on the next request, so a chunk signed with the retired key after its retirement
is gone with them; `transcript_retracted` counts the indexes dropped. A rotation read while a request was indexing is
told from the request's key snapshot, and the index is dropped at the request's end; a line whose record no longer
verifies when a page seeks it leaves the index and the page passes it.

No tick tails the transcript topics. The first request for a run scans its span of the tenant's topic, from one minute
before the earlier of its first record and its admission, as the trace drill-down does, and keeps seq to offset for
each of the run's chunks; a later request resumes the scan where the last stopped, then seeks the lines selected, so a
request for lines already indexed reads nothing for its scan. The seek reads the topic's span between the lowest and
the highest offset of the lines selected, other runs' records interleaved with them included, so its cost follows the
tenant's traffic over the page's span rather than the page's line count; on a busy tenant a page can come in parts,
each with `next` at the cut. A working run's page asks `from=<next>`
on each `model` event and at most every 2 seconds; there is no new stream event type. The chunks and the terminal
status commit in one transaction but are read from two topics, so a terminal on the feed can show before its attempt's
last chunks are stable on the transcript topic; `complete` waits for them, and an open transaction pins the last
stable offset as [Freshness](#freshness) describes, for the whole tenant's transcript topic, for up to the broker's
transaction timeout. Each finished attempt's `transcript_chunks` in the run detail is the count its terminal status
carries; it counts the empty final chunk, so the page treats it as an estimate.

Scans and seeks run under caps, each reported by name in the response's `truncated` and the model's `bounds` when
hit: 32 MiB and 20,000 records per request over its scan and seek together, `transcript_bytes` and
`transcript_records`; 15 seconds per request, `transcript_scan`; two requests at once, `transcript_scans`, a third
answered 503; 500,000 lines indexed per run and 2,000,000 over every run, `transcript_lines`; the indexes of 32 runs
kept, the least recently used dropped beyond them, `transcript_runs`. A quarter of the bytes, records and time is the
seek's share, kept from the scan, so a scan that spends the rest never starves the page's seek. A scan cut by its
request's bytes, records or time resumes on the next request, and a `tail` so cut is answered with `retry`; a scan
stopped by the lines cap per run is final. When the total over every run is reached, the least recently used indexes
other than the one being built are evicted to make room, counted as `transcript_evicted` and reported in the
response's `evicted`, before the scan is stopped. A response stops at a byte budget under the 8 MiB response bound,
returning fewer lines with `next` at the cut and `response_bytes` named. Index state is guarded by a lock per run,
taken before a scan slot, so concurrent requests for one run take turns and a request waiting its turn holds no slot.

## Bounds

The caps apply while reading, not after, and each reports in the model's `bounds` list when hit: a record count and
byte budget per read, a record over 2 MiB skipped before decoding, 50,000 runs kept with the oldest by last activity
dropped, 400,000 digests, 10,000 records pending a key or an echo, 10,000 topics, 8 MiB per JSON response with the
runs list cut by halves to fit, 64 KiB per stream event, 1,024 sessions with the oldest dropped. The retained state is
bounded as each record is folded, every collection per run and over every run: one run keeps at most 1,000 events
and 512 KiB, its message bytes counted, its oldest events dropped first from its lowest attempt so its state stands, 64 attempts with the lowest
dropped, a newly arriving lower one included, 16 echoes with the oldest other than the joined one dropped, 16
cancels and 64 open requests with the oldest dropped; a run so cut lists the caps in its `truncated` field, and one
whose joined echo and result alone are over a bound is dropped. The model keeps at most 2,000,000 events, 1,000,000
attempts, 200,000 echoes, 200,000 cancels, 200,000 open requests and 512 MiB over every run, the oldest runs dropped
beyond them as under the runs cap and the growing run too when it alone exceeds a cap, and 200,000 progress records
over every receiver. Each cap reports under its name: `run_events`, `run_attempts`, `run_echoes`, `run_cancels`,
`run_opens`, `run_bytes`, `events`, `attempts`, `echoes`, `cancels`, `opens`, `bytes`, `progress`, `sessions`. A
JSON response is encoded into a buffer that stops at the response bound, so no larger encoding is built, and a run
detail whose element counts alone put it over the bound is refused before encoding. The drill-down scans at most
32 MiB, 20,000 records and 5,000 spans per refresh, keeps 16 links per span, builds a forest of at most 5,000 nodes,
runs at most 5 seconds and two scans at once, and keeps the scan state of 32 runs. The transcript's caps are listed
under [The transcript](#the-transcript).

## Freshness

The view reads every followed topic once a tick, about every 1.5 seconds, each from the last stable offset its
previous read listed, so a transaction committed between two reads is read whole and a record of an aborted one is
never read. In the normal path a stage change reaches the stream within 5 seconds. An open or orphaned transaction
pins a topic's last stable offset, so the records after it wait until it commits, aborts or times out at the
broker's transaction timeout, 15 minutes by default; a stage change can then take that long to show. A dropped
connection resumes from the same offsets on the next tick.

## The API

Every endpoint is read-only, answers JSON built from named fields, and needs the session cookie except the page, its
two assets and `POST /session`.

| Endpoint | Returns |
| --- | --- |
| `GET /api/model` | the model: nodes of every kind, idle ones included, the runs of the window newest first, the topics followed, counters and the bounds hit |
| `GET /api/runs/{id}` | one run: its echo, attempts and events, open requests and answers, claim decisions, cancels, result references and path, each signed fact with its signer |
| `GET /api/runs/{id}/trace` | the run's spans from its tenant's trace topic as a forest per trace ID, with markers for what was cut; a second call resumes where the first stopped |
| `GET /api/runs/{id}/transcript` | a page of the run's transcript from its tenant's transcript topic, by `tail`, `before` or `from`, as [The transcript](#the-transcript) describes |
| `GET /api/stream` | Server-Sent Events: one `model` event per change of the model, with its version |
| `POST /session` | takes `token` in a form or JSON body, compared in constant time and rate-limited, and sets the session cookie |

Span facts are advisory: any host of a tenant can write that tenant's trace topic, so the drill-down adds timing
and never takes a run's state, failure or S3 facts from a span ([Tracing](tracing.md#topics-and-grants)). It tests
each record for the run ID as bytes before decoding it, skips one over the trace client's record limit before
decoding, and passes span names, attribute keys and values through an allowlist of what the configuration yields: a
span name is rebuilt from validated components, a fixed name, a topic the configuration derives, the signature task
topics of the receivers' profiles included, a profile a receiver's configuration names or an operator command of the
fixed set; a destination or group attribute passes the same test, an output attribute never: the output-name
exception is a result's alone, and a span's attribute is not a result's; anything else shows as `[other]` or is
dropped and marked. A span whose IDs are not their fixed length is skipped as malformed.

Every request passes one guard before the router sees it, in this order: the `Host`, the `Origin` when present, the
path, then the session cookie on the routes that need one. A path with an empty segment (`//`), a `.` or `..`
segment, or an encoded dot or slash (`%2e`, `%2f`) gets a fixed 404 rather than the router's redirect to a cleaned
path, which would reflect the request; `CONNECT` gets 405 and `OPTIONS *` 400. Every response the guard or a handler
writes carries the fixed headers. A request Go's server cannot read, a malformed line, a header over the bound or
one too slow to arrive, is answered 400, 431 or 408 by the server itself and passes through no handler.

Errors carry a fixed text. Request bodies, the token, the cookie and the request path are never logged; the log
holds one line per request, the method as one of the fixed set (`GET`, `HEAD`, `POST`, `PUT`, `PATCH`, `DELETE`,
`OPTIONS` or `other`), a fixed identifier of the route matched (`page`, `asset`, `session`, `model`, `run`, `trace`,
`transcript`, `stream` or `other`) and the status, written when the response is complete, so a stream is logged when it ends. A
failed tick logs its error class alone, a Kafka error's name, `timeout`, `canceled` or `error`.

## Behind a reverse proxy

To use the view from another machine, run it on the operator's host behind a reverse proxy that terminates TLS,
such as HAProxy, nginx, Caddy or an OpenShift Route, and tell it the origin the proxy serves:

```sh
wherry operator view --dir /srv/wherry/operator --port 8440 --external-url https://wherry-view.example.com
```

The command prints `https://wherry-view.example.com/#t=...`. The view then accepts `wherry-view.example.com` as the
request's `Host` and `https://wherry-view.example.com` as its `Origin`, next to `127.0.0.1:8440` and
`localhost:8440`; any other `Host`, any other `Origin` when one is present, and a request with no or a wrong cookie
get 403. The proxy must pass the client's `Host` through, as HAProxy does by default, and send it exactly as the
authority of `--external-url`: the comparison is of the header as sent, so `wherry-view.example.com:443` is another
host and gets 403 when the flag named the bare name. Forwarded headers such as `X-Forwarded-Host` are ignored. An
`--external-url` with a path, query or fragment is refused: the view serves at the origin's root.

The stream is a long request, so raise the proxy's tunnel and server timeouts above the view's 15-second keep-alive,
for example in HAProxy:

```
frontend view
    bind :443 ssl crt /etc/haproxy/view.pem
    default_backend view

backend view
    timeout server 1h
    timeout tunnel 1h
    server local 127.0.0.1:8440
```

The token is one shared secret for everyone who opens the printed address, and it now guards every tenant's agent
output: the status messages and transcript lines of every run in the window. When more than one person uses the
view, authenticate at the proxy, with its own user accounts or single sign-on, and hand the printed address only to
them; the cookie the view issues says a browser has presented the token, nothing more.

## The cookie

The cookie is named `wherry-view-<port>`, is host-only with no `Domain`, has `Path=/`, `HttpOnly` and
`SameSite=Strict`, and is `Secure` when `--external-url` is https. Its value is a session ID the server issued,
never the token. Cookies are not isolated by port, so on the same browser every other service on `127.0.0.1` or on
the external host receives it too: that residual is why the value is a session ID good for this view alone and why
the name carries the port. The page sets a Content-Security-Policy that allows its own assets only, no inline script
and no framing, and every response is `Cache-Control: no-store` with `Referrer-Policy: no-referrer`.

## The placeholder page

The page renders the model as plain tables, every value as text, output names, messages and lines included. Click a
run for its path, echo, attempts with their messages, failure reason, open exchange, cancels, result references and
storage, each signed fact with its signer, the tail of its transcript with older pages on request, following while it
works, and load its spans from there. The page applies a transcript response only to the run it was asked for, follows
again after any error, a 503 included, and asks at once on a model event; each line's attempt and seq sit in their own
elements and its text in a `bdi`, so a bidi control or line break in agent text cannot imitate a neighbouring row's
prefix. It is the surface slices 1 to 4 are accepted on; the designed interface follows
against the same API.
