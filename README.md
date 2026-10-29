# leenustechs-cia-state

State machine built with Spring Boot, Java 21, Kafka and Firebase Realtime Database.

## State store

Each workflow is persisted under a configurable RTDB root:

```text
/workflows/{transactionId}
```

The workflow node contains the durable current state (`status`, `currentStage`,
`steps`, `completedStages`, `stepResults`, `payload`, `result`, timestamps).
Worker completion and stage claims are coordinated with Firebase Realtime
Database transactions on the workflow node, so concurrent workers do not use
last-writer-wins semantics for state-machine transitions.

`stepResults` are stored as JSON-native nested values. A small `present` wrapper
keeps an explicit result slot even when a worker result is JSON `null`.

Workflow records have no automatic deletion policy. Stage execution deadlines
terminate stalled work without deleting its history. Retention, archival and
analytics remain separate concerns from state-machine correctness.

## Configuration

```properties
firebase.database-url=${FIREBASE_DATABASE_URL}
firebase.rtdb.workflow-path=${FIREBASE_WORKFLOW_PATH:workflows}
firebase.rtdb.operation-timeout-seconds=${FIREBASE_OPERATION_TIMEOUT_SECONDS:10}
```

The server uses Google Application Default Credentials (ADC). For local
development, point `GOOGLE_APPLICATION_CREDENTIALS` at a service-account file
outside the repository; in cloud environments use workload/service identity.
Do not commit credentials to this project.

## Realtime clients

The workflow path is RTDB-native, so an authorized POS/TPV client can observe a
transaction in realtime. Production clients should normally receive a sanitized
read-only projection rather than unrestricted access to internal workflow
payloads.

## Reliable workflow execution

A single RTDB transaction records each accepted worker result, aggregates a
completed stage, advances or terminates the workflow, and writes its outgoing
events to `pendingOutbox` on that same workflow node. No Kafka send occurs in a
Firebase transaction callback: Firebase may retry the callback.

Firebase removes empty containers/null children and may coerce numeric-key
objects into arrays. Canonical internal JSON companions retain exact payloads
and outbox envelopes; RTDB-native fields remain available as realtime
projections. Payloads with Firebase-unsafe object keys retain their canonical
representation while omitting the unsupported native projection. This also
applies to numbers outside Firebase's supported types and projections exceeding
Firebase path/depth limits. The adapter validates the configured root, transaction
path and the complete required metadata with the pinned SDK before writing.
Unwritable required paths/metadata are rejected with sanitized diagnostics; their
values are never silently truncated. Server-side write-size limits still apply.

The scheduled outbox publisher scans `outboxPending == true`, waits for Kafka's
send acknowledgement and only then removes that individual event atomically.
Failed or interrupted sends remain pending while still relevant and are retried
after a restart. Accepted worker results remove their own command intents. A
terminal transition cancels all remaining worker commands atomically and keeps
only the final notification. The cursor rotates across workflows so an
unavailable destination does not starve later batches.

The publisher confirms the current intent in a server-checked RTDB transaction
before each send, so an older scan cannot authorize an already-cancelled command.
A send authorized just before cancellation may still finish; Kafka sends and
worker side effects cannot be recalled by the RTDB transaction. Workers should
check execution validity when cancellation affects their side effects, in
addition to deduplicating event IDs.

Corrupt pending workflows are skipped individually and reported by database key
without logging payloads. The cursor advances over raw rows even if the entire
page is corrupt, so healthy later workflows can proceed. Corrupt source data is
retained for repair; a database outage still fails the scan and is retried.

Delivery is **at least once**. A crash after Kafka accepts a send but before
Firebase records its acknowledgement can publish it again. Concurrent service
instances can also publish the same pending event. Each event keeps a stable
`eventId` (also in the `event-id` Kafka header); workers and final-response
consumers must deduplicate it before applying their own side effects. Producer
idempotence alone does not provide cross-restart deduplication.

The initial `CIA_API` event creates a workflow only if its transaction ID does not
exist. Repeating the same request returns its current state. Reusing the ID with
a different command, execution ID or input is rejected; completed and failed
workflows cannot be reopened. An intentional new execution needs a new
transaction ID. Each worker can appear only once in a workflow declaration.

### Event envelope

```json
{
  "transactionId": "login-123",
  "producer": "SECURITY",
  "command": "LOGIN",
  "payload": {"userId": "example"},
  "eventId": "security-response-123",
  "executionId": "login-123",
  "stageIndex": 0,
  "outcome": "SUCCESS"
}
```

Workers respond with their own `StepType` as `producer`, echo `executionId` and
`stageIndex` from the command, and assign a stable ID to each response. Commands
and final responses published by the orchestrator use `producer: CIA_STATE`.
The final response uses `outcome: SUCCESS` or `FAILURE` and its payload contains
the result. `outcome` explicitly controls worker success/failure; error-topic
listeners always set `FAILURE`.

For compatibility, the original four fields are still accepted. Missing
`executionId` means the transaction ID; a response to an explicitly different
execution ID must echo it. Missing `stageIndex` is inferred from the unique
worker. Only when `outcome` is absent does the service recognize legacy payload
fields `exception` or the pair `error_code`/`error_message` as failure. Prefer the
explicit envelope for new integrations. Invalid identifiers, unknown commands
and responses to inactive future stages are rejected.

Parallel results are merged against the stage's persisted input, independently
of response arrival order. Conflicting writes atomically fail the workflow with
`INVALID_STAGE_RESULTS` and enqueue its final failure response. Payloads and
conversion exception details are omitted from application logs.

### Runtime configuration

Java 21 and Maven >= 3.9.12, < 4 are required. Spring Boot 4.0.9 and Spring Cloud
2025.1.1 are aligned with the default compatibility verifier. Configure:

- `FIREBASE_DATABASE_URL` and Google ADC (workload identity, or a service-account
  file outside the checkout referenced by `GOOGLE_APPLICATION_CREDENTIALS`).
- `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_USERNAME`, `KAFKA_PASSWORD` and optionally
  `KAFKA_GROUP_ID` (default `cia-state`). Kafka uses SASL_SSL/SCRAM-SHA-512.
- Optional Spring properties `outbox.poll-interval-ms`, `outbox.batch-size`,
  `outbox.send-timeout-seconds`, or `outbox.enabled=false` to suspend publishing.

Add an RTDB index at the configured workflow root in your existing database
rules; merge these index names with any existing indices without changing
access permissions:

```json
{"rules": {"workflows": {".indexOn": ["outboxPending", "stageDeadlineAt", "status"]}}}
```

Provision `<original-topic>.DLT` for each consumed topic (lobby, response,
deadletter and failure-response), plus the existing workflow and final-response
topics. Permit the service to read its inputs and publish workflow/output/DLT
events. Processing errors get up to three retries with 1/2/4-second backoff;
invalid arguments and deserialization errors go directly to DLT. A DLT send must
be acknowledged before recovery succeeds. DLT bodies retain the failed message;
restrict access and retention appropriately. DLTs are not automatically consumed
or replayed by this service.

### Stage deadlines and reconciliation

`workflows.stage-timeout-seconds` defaults to 300 seconds. A stage's persisted
`stageDeadlineAt` starts when that stage is entered, including time waiting for
command delivery. Partial results do not extend it; advancing starts a fresh
stage deadline. Terminal transitions remove the deadline index entry.

A result received after the deadline cannot complete the stage. The indexed
reconciler also handles workers that never respond: in one transaction it marks
waiting steps as errors, changes the workflow to `FAILED`, cancels worker
commands and queues the final `STAGE_TIMEOUT` response. Rechecks inside that
transaction prevent a stale scan from expiring a stage that already advanced.
Multiple instances may reconcile the same workflow safely.

Defaults are `workflows.reconciliation.poll-interval-ms=1000` and
`workflows.reconciliation.batch-size=100`. Detection can take longer than one
poll under backlog or database outages. A second bounded scan by `status`
finds legacy active workflows without deadlines every
`workflows.reconciliation.legacy-poll-interval-ms=30000`. Those workflows get a
full stage grace period from discovery; old timestamps do not fail them
immediately. Both scans advance past corrupt rows and retain their source data.

`workflows.reconciliation.enabled=false` disables these background scans; it
should be used deliberately (for example, in tests). Incoming events still
respect stored deadlines and the outbox defers expired worker commands, so
leave reconciliation enabled for normal operation. The default scheduling pool
has three threads to keep outbox delivery, expiry and legacy discovery from
waiting behind one another. Keep clocks synchronized across service instances.

### Outbox limits and metrics

Producer defaults bound synchronous enqueue waiting with
`kafka.producer.max-block-ms=1000`, broker delivery with
`kafka.producer.delivery-timeout-ms=10000`, and requests with
`kafka.producer.request-timeout-ms=5000` (`linger-ms=0`). Startup rejects invalid
values or a delivery timeout shorter than request plus linger. The outbox's
15-second future wait is separate from synchronous enqueue and Firebase
transaction timeouts; it is not a bound on the entire poll. Delivery remains
sequential and bounded by the batch size. Tune these values from measured load.

Micrometer registers these meters through the existing Actuator integration:

| Meter | Meaning |
| --- | --- |
| `cia.outbox.delivery.success` | Kafka-acknowledged sends whose outbox acknowledgement also completed |
| `cia.outbox.delivery.failures` / `timeouts` | Failed attempts (including preflight/ack failures); timeouts are a subset |
| `cia.outbox.delivery.duration` | Attempted send, future wait and acknowledgement time |
| `cia.outbox.poll.duration` | Full scan and sequential batch time |
| `cia.outbox.scan.rejected` / `failures` | Invalid source rows versus unavailable scans |
| `cia.outbox.page.pending.workflows` / `events` | Counts in the last successfully scanned healthy page |
| `cia.outbox.page.oldest.age` | Age in seconds of the oldest known enqueue timestamp in that page |
| `cia.outbox.page.unknown.timestamps` | Events in that page without enqueue timestamps |
| `cia.workflows.reconciliation` | Changed, unchanged and failed reconciliation attempts, tagged by scan type |
| `cia.workflows.reconciliation.scan.rejected` / `failures` | Corrupt source rows and unavailable deadline/legacy scans |

Page gauges carry `scope=last_scan_page`; they are not global backlog counts and
can still describe events delivered after the scan. Legacy events without
`enqueuedAt` are counted as unknown and excluded from oldest-age calculations.
No transaction/event IDs are metric tags. Expose Actuator metrics only through
your configured monitoring access; default HTTP endpoint exposure is unchanged.

### Validation and rollout

```sh
mvn verify
mvn spring-boot:run
```

The tests start the real Spring application context with external Firebase
objects mocked and Kafka listeners stopped, and exercise the default Spring
Cloud compatibility check. The checks retain the verifier and also confirm
incompatible combinations fail. This validates application wiring without
opening service connections.

The tests exercise event contracts, Kafka factories/recovery, atomic transitions,
concurrent and duplicate results, aggregation conflicts, persistence codecs and
outbox restart/retry behavior without requiring external service credentials.
Regression checks also cover cancellation after stale scans, all-corrupt scan
pages and fallback JSON persistence through the actual Firebase SDK validators.
Fixed clocks test deadline boundaries, late results, parallel results, legacy
grace periods and simultaneous reconcilers. Codec tests persist the new deadline
and enqueue timestamps through Firebase normalization.
They do not replace a smoke test against your actual Firebase/Kafka deployment.

Before rollout, update workers/final consumers to accept the envelope and
perform event-ID deduplication, provision DLT topics and the RTDB index, then
validate a login in a development namespace. Drain active workflows before
upgrading. Legacy states can be read, including numeric completed-stage keys,
but the old version's interrupted transitions or lost Kafka sends have no
outbox record and cannot be reconstructed automatically by this change.
