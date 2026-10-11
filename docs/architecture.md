# Architecture Overview

Campus Share follows a layered distributed design:

- Browser clients communicate only with the API Gateway.
- The Gateway routes authentication, resource search, and service orchestration.
- Department services own their own resources and health state.
- gRPC is used for peer-to-peer communication among CS, IT, and ENTC nodes.
- RabbitMQ relays resource events to the Gateway and then to connected clients over WebSocket.

## Current implementation status

The Gateway provides registration/login, cross-department search aggregation, and authenticated lifecycle routing. Each department service owns its resources, reservations, and borrow records in a separate MongoDB database. The React client uses the Gateway HTTP API and WebSocket endpoint.

## Persistence

The Gateway persists accounts in `campus_share_auth`; CS, IT, and ENTC persist their authoritative data in `campus_share_cs`, `campus_share_it`, and `campus_share_entc`. Configure `AUTH_MONGO_URI`, `CS_MONGO_URI`, `IT_MONGO_URI`, and `ENTC_MONGO_URI`. Compose uses Mongo service DNS names and named volumes retain database files. Startup waits for a database connection and fails clearly instead of falling back to process memory. Seed records use stable IDs and insert-only upserts.

## Reservation quantity transitions

- `PENDING`: reservation creation atomically transfers one unit from available to reserved.
- `APPROVED`: administrator decision; quantity remains reserved.
- `REJECTED` or `CANCELLED`: releases reserved quantity back to available.
- `FULFILLED`: checkout transfers reserved to borrowed and creates one borrow record.
- Return marks the borrow record returned and transfers borrowed quantity back to available.

Every lifecycle transition conditionally matches the current embedded state and required quantity in the owning resource document. A single MongoDB document update changes both the embedded lifecycle record and quantity counters; concurrent requests serialize against that resource document. Integration tests issue simultaneous last-unit requests to two department processes sharing MongoDB. Queries check the quantity invariant and never decrement a counter below zero. This avoids multi-document transaction requirements and works with standalone MongoDB. The outbox event is appended in the same conditional single-document update as its state/quantity transition, so the database state cannot commit without recording its event. This does not make other multi-document operations transactional.

Reservations and borrow records are embedded in their resource document to keep each lifecycle transition atomic. This bounds lifecycle history by MongoDB's 16 MiB document limit. Indexes cover resource ownership/IDs and embedded reservation and borrow lookups.

## Events

Each successful resource create/update/delete, reservation, decision, cancellation, checkout, and return appends an outbox event with a stable ID, event type, source department, timestamp, resource ID, and relevant lifecycle ID. A department worker atomically leases pending entries in their owning resource document, publishes persistent messages to the durable `campus.resource.events` topic exchange, waits for publisher confirmation, then conditionally records the status and releases the lease. RabbitMQ failures leave the event pending; retries use configurable exponential backoff and expire/recover leases after publisher crashes. Multiple department workers may compete safely for an event.

Delivery is at-least-once, not exactly-once. If a broker confirms and the publisher crashes before persisting that confirmation, a retry may publish the same stable event ID again. Consumers must deduplicate by ID before applying side effects. RabbitMQ `published` means publisher confirmation, not durable consumer processing.

Each department also calls the authenticated Gateway internal event endpoint. MongoDB event receipts deduplicate stable IDs and have a 30-day TTL. `relayed` means the Gateway attempted a WebSocket broadcast while at least one client was connected; `accepted_no_clients` means no client was connected and is not reported as client delivery. WebSocket delivery is ephemeral: there are no client acknowledgements or replay guarantees. A crash between broadcast and marking the receipt terminal may result in a duplicate broadcast after the relay lease expires.

Confirmed/terminal entries are retained for seven days by default, then removed by the worker. A resource allows at most 500 outbox entries by default; writes are rejected with `503` if this bound is reached. Embedded resources, lifecycle history, and outbox payloads remain subject to MongoDB's 16 MiB document limit. Monitor document growth and archive historical records before reaching the server limit. This cap bounds typical pending-event growth but is not a substitute for an archival strategy.

Polling, lease duration, retry base/max delay, publish/confirm/relay timeouts, retention, batch size, and the per-resource outbox cap are configurable with `OUTBOX_*` environment variables. RabbitMQ connection/channel errors are handled by discarding the broken connection and retrying on a later worker poll. Worker shutdown stops new claims, waits up to ten seconds for in-flight operations, then closes the broker connection.

## Internal caller authentication

Lifecycle calls and department resource writes require `X-Internal-Service-Token`. The Gateway derives actor headers from its verified JWT; department services enforce role, student ownership, and department ownership independently. The local fallback token is for development only and must be overridden for deployment.

Public signup cannot assign the administrator role without the separately configured `ADMIN_REGISTRATION_KEY`. The supplied key is compared server-side and is not placed in the JWT.

## System boundaries

- Gateway: authentication, authorization, search aggregation, WebSocket relay
- CS service: owns CS department resource state
- IT service: owns IT department resource state
- ENTC service: owns ENTC department resource state
