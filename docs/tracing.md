# Tracing

wherry can write OpenTelemetry spans for every step of a run: the host's
submit, the receiver's attempts and plugin calls, the custodian's key exchange
and the terminal records the host reads back. Each process writes its own
spans to Kafka under its own principal, as OTLP records on a trace topic per
tenant, through a small second Kafka connection kept for traces alone. There
is no collector in the write path and no OTLP endpoint; any trace viewer that
reads Kafka, or an OpenTelemetry Collector with its Kafka receiver, reads the
topics.

Tracing is off by default and costs nothing when off: no span is built, no
record carries a header, no second connection is opened, and the tracing
calls on the run path allocate nothing and leave their context as it was.

## Turning it on

Each role turns tracing on in its own configuration; nothing else does. The
`OTEL_EXPORTER_OTLP_*` and `OTEL_TRACES_EXPORTER` variables are not read.

| Role | File | Field |
| --- | --- | --- |
| host | [`config.yaml`](configuration-host.md#client-configuration) | `tracing: true` |
| receiver | the [local configuration](configuration-receiver.md#receiver-local-configuration) | `tracing: true` |
| operator | [`operator.yaml`](configuration-operator.md#operator-working-directory) | `tracing: true` |

A host's spans go to its tenant's topic. A receiver writes the spans of each
run to the run's tenant's topic and the spans outside a run, its start and its
custodian's key rotations, to `wherry.traces`. The operator writes a span per
command: one naming a tenant goes to that tenant's topic, the others to
`wherry.traces`.

## Topics and grants

| Topic | Holds |
| --- | --- |
| `wherry.traces.<tenant>` | every span of the tenant's runs, from every role, and the operator's commands on the tenant |
| `wherry.traces` | spans of no tenant: receiver start, custodian rotation, pool and receiver commands |

Each record is one OTLP `ExportTraceServiceRequest` in protobuf, holding the
spans of one trace that one export of one process produced, keyed by the trace
ID in lowercase hex. A trace spans the roles, so a run's trace is several
records with the same key, in append order. Records of different traces are
never mixed.

A principal needs these grants, literal, on the topics it writes; the
operator does not yet create the topics or the grants, which come with the
trace-topic slice, so until then an operator creates them with the broker's
tools, auto-creation staying off:

| Principal | Topic | Operations |
| --- | --- | --- |
| host `wherry.tnt.<tenant>.<host>` | `wherry.traces.<tenant>` | WRITE, DESCRIBE |
| receiver `wherry.rcv.<receiver>` | `wherry.traces.<t>` for each tenant `<t>` entitled to its pool, and `wherry.traces` | WRITE, DESCRIBE |
| `wherry.operator` | `wherry.traces` and `wherry.traces.<tenant>` | WRITE, DESCRIBE, held through its `wherry.` prefix grant |

A host can write only its own tenant's topic, so a span claiming another
tenant is refused by the broker with `TOPIC_AUTHORIZATION_FAILED` and lands
nowhere. The same holds the other way: any host of a tenant can write that
tenant's topic, so a span is evidence of timing, not of a run's state; the
signed records on the tenant feed stay the authority for what happened.

## Delivery

Delivery is best-effort. Spans are batched by the OpenTelemetry SDK and
exported through a trace client of the process's own: a second franz-go
client built from the same broker, credentials and trust as the process's
Kafka client, authenticated as the same principal. A host or operator command
holds one trace client; a receiver holds two, one for the receiver and one for
its custodian. Each trace client can open its own metadata and produce
connections to the brokers. The trace client is connected in the
background when tracing starts, so no submit waits for its session. It never
creates a topic, keeps a small buffer (256 records, 4 MiB per trace client, so
512 records and 8 MiB in a receiver) and produces
without idempotence: a trace record is written once or dropped, never resent
after its export has timed out. The process's own client, the one its task
records go through, is never touched by tracing.

An export groups its spans by tenant and trace, produces every group in one
call and waits for each record's acknowledgement or failure:

- An export has one second to be acknowledged. The trace client's produce
  request timeout and delivery timeout are that second too, both under the
  two-second shutdown bound. A record the broker has not acknowledged by then
  is failed by the client, reported as a `timeout` and dropped, and nothing is
  retried beyond the client's own produce retries within the second.
- A refused topic fails its own records alone: the other tenants' groups in
  the same export are delivered.
- A short process, a host command or an operator command, flushes its spans
  and closes the trace client within two seconds before it closes its own
  Kafka client; a receiver does the same when it stops. A refused or
  unreachable broker adds at most those bounds to a command.
- A topic that does not exist is reported as a `timeout` rather than
  `UNKNOWN_TOPIC_OR_PARTITION` when the client is still asking the broker for
  it at the bound.

On a stall, a broker that answers produce requests with a retriable error or
not at all, each export waits out its second, fails its records and reports
one `timeout`; the next export starts with an empty buffer and is not failed
by the earlier deadline. The spans of exports that time out are lost. The
process's own produces, a submit on the host or a status on the receiver,
go through its own client and are not delayed; the trace client's buffer cap
bounds what a stall can hold. A broker that never answers holds a produce
request for at most two seconds, the request timeout plus the client's one
second of overhead, after which the request's records are failed.

A failed export is logged with the topic and the error's class, the Kafka
error name or `timeout`, `canceled`, `client closed` or `error`, and nothing of
the spans: at most once a minute for each topic and class, every refused or
timed-out topic of an export on its own line. A refusal a topic not yet created
or an ended entitlement explains, `UNKNOWN_TOPIC_OR_PARTITION` or
`TOPIC_AUTHORIZATION_FAILED`, is logged once an hour and marked expected. A
failure that is no export's is logged as `tracing: export failed: error`,
never with its own text.

## Trace context on the records

The only header a wherry record carries is `traceparent`, 55 bytes under an
11-byte key, on the records that start or continue a trace: the task record
the host produces, the open request and its answers between a receiver and
its custodian, the result and the terminal status a receiver writes to the
tenant feed, and a cancel. A `tracestate` header is read and dropped; a
malformed or oversized header is ignored and the consumer starts a trace of
its own. With tracing off a record carries no header at all.

## Spans

The names below are fixed; the attributes are listed in the next section.

| Role | Span | Kind | Parent |
| --- | --- | --- | --- |
| host | `submit` | INTERNAL | root of the run's trace; a resubmission is a new root linked to the original |
| host | `read host key`, `read state`, `read advertisements`, `read pool key` | CLIENT | `submit` |
| host | `resolve credential`, `seal envelope`, `upload inputs` | INTERNAL, INTERNAL, CLIENT | `submit` |
| host | `send <task topic>` | PRODUCER | `submit`; its context travels on the task record |
| host | `send wherry.pool.<pool>.events` | PRODUCER | a cancel, under the run's `submit` when this host submitted it |
| host | `process wherry.tenant.<tenant>` | CONSUMER | the receiver's send of the result or terminal status, once per record the wait accepts |
| receiver | `process <task topic> <profile>` | CONSUMER | the host's send, one per attempt |
| receiver | `admit` | INTERNAL | the attempt |
| receiver | `run` | INTERNAL | the attempt; `wherry.retry` counts in-place start retries |
| receiver | `deliver credentials`, `await start`, `find job`, `return task`, `released` | INTERNAL | `run` |
| receiver | `plugin Dispatch`, `plugin Lookup`, `plugin Follow`, `plugin Stop` | CLIENT | `run` or the span calling the plugin |
| receiver | `upload output` | CLIENT | `run` |
| receiver | `send wherry.pool.<pool>.events` | PRODUCER | `run`; the open request, its context on the record |
| receiver | `process wherry.pool.<pool>.events` | CONSUMER | the custodian's answer |
| receiver | `send wherry.tenant.<tenant>` | PRODUCER | `run`; one for the result, one for the terminal status, one for a rejection |
| receiver | `receiver start` | INTERNAL | root, no tenant |
| custodian | `process wherry.pool.<pool>.events` | CONSUMER | the receiver's open request |
| custodian | `send wherry.pool.<pool>.events` | PRODUCER | the custodian's CONSUMER; the wrap or refusal |
| custodian | `custodian rotation` | INTERNAL | root, no tenant |
| operator | `operator <command>` | INTERNAL | root, one per command; `revocation step <n>` under a removal |

A span that fails carries an error status whose description is a fixed class:
`released`, `rejected`, `failed`, `canceled`, `timeout`, `error`, `link lost`,
`too large`, `fetch`, `plugin error`, `redirected`, `status`, `invalid input`,
`no matching receiver`, `start-by limit`, `no pool key`, `sealing failed`,
`read failed`, a task state, or a receiver's rejection label. Never the
error's text.

## Attributes

Every span, event and link attribute is one of these keys. The list is
enforced by the tests of each role, which check every exported span against
it and search the spans for sentinel values, not by a filter at export: a span
is built only from fixed keys and values the role already holds.

| Key | Value |
| --- | --- |
| `wherry.tenant` | the run's tenant, on every span of a run from every role; a host's spans carry its tenant always |
| `wherry.run`, `wherry.attempt`, `wherry.retry` | the run ID, the delivery attempt, the in-place start retry |
| `wherry.host`, `wherry.pool`, `wherry.receiver`, `wherry.profile` | names from the configuration and the task's binding; an operator command's span carries an argument only once it has passed name validation, so a refused argument reaches no span |
| `wherry.agent`, `wherry.model` | the placement |
| `wherry.credential`, `wherry.credential.source` | a credential's name and the kind of its source, `file`, `env` or `command`; never its value or path |
| `wherry.output`, `wherry.count`, `wherry.size`, `wherry.chunks` | an output's name, a count of files or credentials, bytes, transcript chunks |
| `wherry.storage.endpoint`, `wherry.storage.bucket` | the pool terms' S3 endpoint and bucket, on `upload inputs` and `upload output`; no URL is parsed or carried |
| `messaging.system.name`, `messaging.operation.type`, `messaging.operation.name`, `messaging.destination.name`, `messaging.kafka.offset`, `messaging.consumer.group.name` | the semantic conventions for Kafka |

A resource carries `service.name` (`wherry-host.<tenant>.<host>`,
`wherry-receiver.<pool>.<receiver>`, `wherry-custodian.<pool>`,
`wherry-operator`), `service.namespace` (the role) and the SDK's own
`telemetry.sdk.*` attributes. `OTEL_RESOURCE_ATTRIBUTES` and
`OTEL_SERVICE_NAME` are ignored.

## What is never exported

- Credential values, their file paths, environment variable names and
  commands; the platform credential; the pool key and the envelope; the
  redaction key.
- The prompt, the transcript, outputs, replies and any part of a task.
- Presigned URLs and their signatures, object keys, the storage keys, the
  store's answers.
- Error text, plugin error messages, rejection reasons quoting the task,
  bundle passwords, the broker CA.
- Exception events: no span records one.

The tests of each role submit sentinel values through every path and fail on
any of them in a span's name, attributes, events, links, status or resource,
and in the bytes of the trace-topic records.

## Reading the topics

The records are standard OTLP, so a collector reads them with its Kafka
receiver, under a principal granted READ and DESCRIBE on the topics it
follows:

```yaml
receivers:
  kafka/wherry:
    brokers: [broker.example.com:9093]
    protocol_version: 3.6.0
    traces:
      topics: [wherry.traces.acme]
      encoding: otlp_proto
    tls:
      ca_file: /etc/wherry/kafka-ca.pem
    auth:
      sasl:
        mechanism: SCRAM-SHA-512
        username: traces-reader
        password: ${env:KAFKA_PASSWORD}
exporters:
  otlp:
    endpoint: tempo.example.com:4317
service:
  pipelines:
    traces:
      receivers: [kafka/wherry]
      exporters: [otlp]
```

The topics are not compacted; records expire by the topics' retention.

## Cost

With tracing on, a submit builds its spans, puts one 55-byte header on the
task record and queues the spans for export; the export itself runs in the
background through the trace client, except the flush inside the command's
close. The trace client is the one cost a process pays up front: its dial,
TLS handshake, SCRAM exchange and first metadata took about 20 ms on an
in-process broker with TLS, in the background, and it holds its own
connections and SASL sessions on the broker for as long as the process runs:
one trace client for a host or operator command, two for a receiver. In
the round-trip test on that broker, the submit's median wall time was within
the run-to-run noise of tracing off, the host's close with the flush took
under 5 ms, and a run of the test plugin produced 28 spans, about 9 KB on
the tenant's topic.
