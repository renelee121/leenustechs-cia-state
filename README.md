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

There is no expiration policy in the workflow model. Retention, archival and
analytics are separate concerns from state-machine correctness.

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
