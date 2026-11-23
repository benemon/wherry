# Operator view

`wherry operator view` is a read-only map of one cluster for its operator: every broker, pool, receiver, custodian,
host, tenant and S3 destination the records name, the runs of a window with their path, state and failure class, what
each receiver holds, each pool's share-group lag, and for a selected run the spans its tenant's trace topic holds. It
rebuilds the window from the records Kafka keeps, then tails them, and serves JSON, a Server-Sent Events stream and a
placeholder page of plain tables on loopback. It never produces to Kafka.

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

Every record is verified against its writer's key as the signed records are everywhere in wherry, and checked
against the membership, entitlements and keys in force when the broker appended it, not now: a run completed by a
receiver since removed is present, a status signed with a receiver's key after its membership ended is not, and a
status, result or start failure of a run admitted before its tenant's entitlement ended counts only when it joins
that run's echo: the same run under the record's key, the same tenant, pool and task hash, the echo appended while
the tenant was entitled. Such a record carrying another task hash is refused. Each record is verified once; the copy
of a status on the tenant feed is the copy on the pool's events, found by digest.

The key topics of every receiver and host, ended or not, are tailed before each batch of records is folded, so a
rotation is seen within a tick. A rotation retires a key from its append time; a record signed with it and appended
after that no longer verifies, and when the rotation is read after such records were folded, the facts taken from
them are retracted and counted as `retracted`.

The model keeps named fields and nothing of what an agent wrote: from a status its kind, state, event ID, attempt,
receiver, append time, log offset, event and a failure class; from a result the output names and sizes, the reply's
size and where it ran. The text of a status message, a reply, an artifact or a task never enters the model, so no
response carries it; nor do a result's labels. A profile or agent a result, start failure or advertisement names is
shown only when the receiver's configuration or the pool's terms name it, else it is empty or the offer dropped. The
failure class comes from a fixed table, since the records carry no class but a start
failure's: `start-failure:dispatch`, `start-failure:platform` and `start-failure:input-too-large` from a start
failure; `key-lost`, `failed` and `canceled` from a failed or canceled terminal status; `rejected` from a rejection;
`unclassified` for a terminal status in another state. The reason text in the record is never shown.

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

## Bounds

The caps apply while reading, not after, and each reports in the model's `bounds` list when hit: a record count and
byte budget per read, a record over 2 MiB skipped before decoding, 50,000 runs kept with the oldest by last activity
dropped, 400,000 digests, 10,000 records pending a key or an echo, 10,000 topics, 8 MiB per JSON response with the
runs list cut by halves to fit, 64 KiB per stream event. The retained state is bounded as each record is folded: one
run keeps at most 1,000 events and 512 KiB, its oldest events dropped first from its lowest attempt so its state
stands, 64 attempts with the lowest dropped, and 64 open requests with the oldest dropped, and a run so cut lists the
caps in its `truncated` field; the model keeps at most 2,000,000 events and 512 MiB over every run, the oldest runs
dropped beyond them as under the runs cap, and 200,000 progress records over every receiver. A JSON response is
encoded into a buffer that stops at the response bound, so no larger encoding is built, and a run detail whose
element counts alone put it over the bound is refused before encoding. The drill-down scans at most 32 MiB, 20,000
records and 5,000 spans per refresh, builds a forest of at most 5,000 nodes, runs at most 5 seconds and two scans
at once, and keeps the scan state of 32 runs.

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
| `GET /api/runs/{id}` | one run: its echo, attempts and events, open requests and answers, claim decisions, cancels, result references and path |
| `GET /api/runs/{id}/trace` | the run's spans from its tenant's trace topic as a forest per trace ID, with markers for what was cut; a second call resumes where the first stopped |
| `GET /api/stream` | Server-Sent Events: one `model` event per change of the model, with its version |
| `POST /session` | takes `token` in a form or JSON body, compared in constant time and rate-limited, and sets the session cookie |

Span facts are advisory: any host of a tenant can write that tenant's trace topic, so the drill-down adds timing
and never takes a run's state, failure or S3 facts from a span ([Tracing](tracing.md#topics-and-grants)). It tests
each record for the run ID as bytes before decoding it, skips one over the trace client's record limit before
decoding, and passes span names, attribute keys and values through an allowlist of what the model knows: a span name
is rebuilt from validated components, a fixed name, a topic the model follows, a profile a receiver's configuration
names or an operator command of the fixed set; anything else shows as `[other]` or is dropped and marked.

Errors carry a fixed text. Request bodies, the token, the cookie and the request path are never logged; the log
holds the method, a fixed identifier of the route matched (`page`, `asset`, `session`, `model`, `run`, `trace`,
`stream` or `other`) and the status.

## Behind a reverse proxy

To use the view from another machine, run it on the operator's host behind a reverse proxy that terminates TLS,
such as HAProxy, nginx, Caddy or an OpenShift Route, and tell it the origin the proxy serves:

```sh
wherry operator view --dir /srv/wherry/operator --port 8440 --external-url https://wherry-view.example.com
```

The command prints `https://wherry-view.example.com/#t=...`. The view then accepts `wherry-view.example.com` as the
request's `Host` and `https://wherry-view.example.com` as its `Origin`, next to `127.0.0.1:8440` and
`localhost:8440`; any other `Host`, any other `Origin` when one is present, and a request with no or a wrong cookie
get 403. The proxy must pass the client's `Host` through, as HAProxy does by default; forwarded headers such as
`X-Forwarded-Host` are ignored. An `--external-url` with a path, query or fragment is refused: the view serves at
the origin's root.

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

The token is one shared secret for everyone who opens the printed address. When more than one person uses the view,
authenticate at the proxy, with its own user accounts or single sign-on, and hand the printed address only to them;
the cookie the view issues says a browser has presented the token, nothing more.

## The cookie

The cookie is named `wherry-view-<port>`, is host-only with no `Domain`, has `Path=/`, `HttpOnly` and
`SameSite=Strict`, and is `Secure` when `--external-url` is https. Its value is a session ID the server issued,
never the token. Cookies are not isolated by port, so on the same browser every other service on `127.0.0.1` or on
the external host receives it too: that residual is why the value is a session ID good for this view alone and why
the name carries the port. The page sets a Content-Security-Policy that allows its own assets only, no inline script
and no framing, and every response is `Cache-Control: no-store` with `Referrer-Policy: no-referrer`.

## The placeholder page

The page renders the model as plain tables, every name as text. Click a run for its path, attempts, open exchange,
cancels, result references and storage, and load its spans from there. It is the surface slices 1 to 3 are accepted
on; the designed interface follows against the same API.
