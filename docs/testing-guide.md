# Testing Guide

## Automated lifecycle/API tests

Run from the repository root with MongoDB reachable on `localhost:27017`:

```powershell
npx vitest run tests/smoke.test.js
npm --prefix frontend run build
node --check gateway/server.js
node --check services/baseService.js
docker compose config
```

The suite uses five uniquely named MongoDB databases per run (Gateway auth, CS, IT, ENTC, and an isolated outbox test database), based on the process ID and start time, and drops only those exact databases after the department child processes stop. It starts isolated department-service processes. Gateway, HTTP service, and gRPC ports can be overridden with `GATEWAY_PORT`, `CS_PORT`, `IT_PORT`, `ENTC_PORT`, `CS_GRPC_PORT`, `IT_GRPC_PORT`, `ENTC_GRPC_PORT`, `TEST_SECONDARY_CS_PORT`, and `TEST_SECONDARY_CS_GRPC_PORT`. Gateway service targets must match the selected ports through `CS_SERVICE_URL`, `IT_SERVICE_URL`, `ENTC_SERVICE_URL`, `CS_GRPC_ADDRESS`, `IT_GRPC_ADDRESS`, and `ENTC_GRPC_ADDRESS`. Check that chosen ports are unused before running the suite. It covers MongoDB startup failure reporting, authentication persistence after reconnect, resource/reservation/pending-outbox persistence across an owning-service restart, simultaneous last-unit requests within one process and across two service processes, aggregated search, no-stock rejection without an outbox success event, cancellation/rejection and quantity restoration, reservation privacy, department-admin ownership enforcement, approval-gated checkout, return, duplicate return, stable event IDs, and WebSocket event delivery.

Concurrency tests use a real MongoDB server. Two processes sharing the department database concurrently reserve the last available unit; exactly one succeeds (`201`) and the other receives `409`. This verifies the tested multi-process scenario, not arbitrary MongoDB replica-set failover or cross-region behavior.

## RabbitMQ

Outbox publisher tests run against real MongoDB with controlled publisher/relay dependencies. They verify broker-failure retry with a stable ID, confirmation before marking an event published, competing worker leases, expired lease recovery, and completed-entry cleanup that preserves pending entries. The service-level smoke tests deliberately point department workers at an unavailable local broker so pending-event recovery behavior is deterministic. One conditional test attempts actual RabbitMQ publication against `RABBITMQ_URL` (default `amqp://127.0.0.1:5672`); it skips if no broker accepts a connection. A successful live test means the broker confirmed publication, not that a consumer durably processed it.

The Gateway relay tests verify event delivery to connected WebSocket clients. Relay status only records the Gateway's attempt/acceptance state; browser receipt is not acknowledged or replayed. Gateway event receipts have a 30-day TTL.

The distributed suite also uses isolated in-memory outbox and Gateway receipt models with an ephemeral HTTP port to verify Lamport metadata through the RabbitMQ publisher callback and Gateway-to-WebSocket relay without requiring a broker or database. RabbitMQ publication and Gateway WebSocket relay are separate paths from each outbox entry: a broker subscriber needs a queue bound to `campus.resource.events`, but no RabbitMQ consumer is needed for the direct Gateway HTTP relay.

The smoke suite's Gateway, HTTP service, and gRPC ports can be overridden with `GATEWAY_PORT`, `CS_PORT`, `IT_PORT`, `ENTC_PORT`, `CS_GRPC_PORT`, `IT_GRPC_PORT`, `ENTC_GRPC_PORT`, `TEST_SECONDARY_CS_PORT`, and `TEST_SECONDARY_CS_GRPC_PORT`. Gateway service targets must match those selections through `CS_SERVICE_URL`, `IT_SERVICE_URL`, `ENTC_SERVICE_URL`, `CS_GRPC_ADDRESS`, `IT_GRPC_ADDRESS`, and `ENTC_GRPC_ADDRESS`. Check that selected ports are unused before running it.

## Persistence and deployment

The persistence tests require an available MongoDB server. The test's unique database names and database-only teardown protect any non-test data. The embedded outbox and lifecycle history share MongoDB's 16 MiB document limit; the default outbox cap is 500 entries per resource, and completed entries are retained seven days. Docker runtime behavior is not verified unless containers are actually started and exercised; `docker compose config` checks configuration only.
