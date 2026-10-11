# Phase Status

## Implemented and verified

- Gateway user accounts persist in the Gateway-owned MongoDB database; CS, IT, and ENTC each own separate databases for resources, reservations, and borrow records.
- Lifecycle updates use conditional, atomic single-resource-document changes. Quantity counters, reservation/borrow states, version metadata, and newly generated event outbox entries change in the same MongoDB update.
- Reservation, approval/rejection, cancellation, checkout, and return authorization and transitions are exercised by MongoDB-backed integration tests. Last-unit concurrent reservations are tested both within one service process and across two processes sharing MongoDB.
- The embedded outbox publisher uses leases with expiration recovery, publisher confirmations, stable event IDs, configurable bounded exponential retry delay, pending counts/retry/failure logs, bounded graceful shutdown, and completion cleanup. Writes are capped at 500 outbox entries per resource by default.
- MongoDB-backed tests verify failed broker retry, stable event IDs across retries, confirmation before marking RabbitMQ status confirmed, competing publisher claims, expired leases, cleanup preserving pending entries, no success event for failed reservation, and pending event persistence after a department service restart.
- Resource creation/update/deletion and successful reservation, decision, cancellation, checkout, and return operations record events. Failed state transitions do not append success events.
- Gateway event receipts are persisted, deduplicate event IDs, and distinguish relay attempts with clients (`relayed`) from acceptance without clients (`accepted_no_clients`). The latter does not mean a browser received an event.
- Existing authentication, department ownership, gRPC cross-department search, lifecycle routes, and WebSocket behavior remain covered by the smoke integration suite.
- Docker Compose configures per-department outbox worker settings, MongoDB health dependencies, RabbitMQ service DNS, and named volumes. Configuration validation is distinct from runtime verification.
- FA-2 Lamport logical clocks, current-state monitoring, and Bully leader election are implemented. Vector clocks and Ricart–Agrawala mutual exclusion remain outside FA-2 and are not implemented concepts.

## Implemented but not runtime-verified

- Live RabbitMQ delivery: the integration suite includes a conditional actual broker test. The latest run skipped it because no broker accepted a connection. Controlled publisher tests with real MongoDB cover failure, retry, leases, and confirmation semantics but are not proof of live broker delivery.
- Docker image builds, Compose service startup, container-to-container connections, named-volume persistence across recreation, and full Docker runtime behavior. A successful `docker compose config` only validates Compose syntax and interpolation.
- Delivery to and acknowledgement by downstream RabbitMQ consumers. This application has no configured durable consumer queue or consumer acknowledgement protocol.
- Browser receipt or replay of WebSocket events. `relayed` only records that the Gateway attempted a broadcast while clients were connected.

## Design and limitations

- The outbox is embedded in the authoritative department resource document to preserve atomic state-plus-event recording without requiring a MongoDB replica set. No multi-document transaction is used.
- Delivery is at-least-once, not exactly-once. A broker confirmation followed by a process crash before its MongoDB confirmation update can produce another publish with the same event ID. Consumers must deduplicate by event ID.
- Confirmed/terminal outbox entries are retained for seven days by default; Gateway receipts expire after 30 days. Per-resource event writes return `503` at the default 500-entry cap until delivery recovery/cleanup.
- Resource data, embedded lifecycle history, and outbox entries share MongoDB's 16 MiB BSON document limit. The cap and retention reduce outbox growth but do not replace a lifecycle archival strategy.
- The Gateway receipt lease prevents ordinary concurrent duplicate relay attempts. A crash after a WebSocket broadcast but before recording the terminal receipt can still cause a duplicate broadcast after lease expiry.
- MongoDB must be reachable for department startup and test execution. There is no in-memory fallback. Legacy data that existed only in process memory is not migrated.

## Latest verification

- `npx vitest run tests/distributed.test.js` — 13 focused tests passed for Lamport semantics, department clock independence, Bully outcomes/recovery/deduplication/concurrent triggers, single-service election forwarding, and healthy/partial/unknown/all-node health states.
- `node --check` on changed backend modules — passed.
- `npm run lint` — passed.
- `npm --prefix frontend run build` — passed.
- `docker compose config --quiet` — passed.
- Full MongoDB-backed smoke suite was not run during this change because its teardown drops isolated databases and it binds the same service ports used by deployments.
- Docker image rebuild/startup and runtime deployment were not performed. No service containers, databases, or volumes were changed.
- Live RabbitMQ delivery and container-to-container election traffic remain runtime-unverified. This application has no RabbitMQ consumer.
