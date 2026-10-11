# API Documentation

All public endpoints are served by the Gateway. Use `Authorization: Bearer <JWT>` for protected routes. User ID, role, and department are taken from the verified JWT; values in a request body do not override identity.

## Authentication and resource discovery

### `POST /api/auth/register`

Body: `{ "name": "Sam", "email": "sam@example.test", "password": "...", "role": "STUDENT" }`

Department administrators must additionally supply `"department": "CS"`, `"IT"`, or `"ENTC"` and a server-configured `"adminRegistrationKey"` value. Admin registration returns `403` if the server has no enrollment key configured or if the supplied key does not match. This prevents public request-body role selection from being sufficient to create an administrator. Success is `201` with a JWT and safe user summary. Duplicate email is `409`; invalid role/department or missing values are `400`.

### `POST /api/auth/login`

Body: `{ "email": "sam@example.test", "password": "..." }`. Success is `200`; invalid credentials are `401`.

### `GET /api/resources/search?name=Arduino`

Searches department gRPC services and returns `{ query, total, departments, nodesUnreachable }`. `name` is required (`400` when missing). Partial department results are returned when a node is unreachable.

### `GET /api/resources`

Returns the public resource catalogue as `{ resources, nodesUnreachable }`, with safe resource fields aggregated from CS, IT, and ENTC. Partial results identify unavailable departments. If all department services are unreachable, the Gateway returns `503` with an error and the unreachable department list rather than a successful empty catalogue.

### `GET /api/resources/:id`

Returns one resource from its authoritative department service (`404` when not found).

## Reservations

### `POST /api/reservations` — student

Body: `{ "resourceId": "resource_..." }`. The Gateway finds the owner and forwards verified identity. Success is `201` with `reservation`, updated `resource`, and `eventDelivery`. The new state is `PENDING`; one unit moves from available to reserved. Missing ID is `400`, unknown resource is `404`, and no available units is `409`.

### `GET /api/reservations` or `GET /api/reservations/my`

Requires authentication. Students receive only their own reservations across reachable departments. A department admin receives records for their assigned department. The response is `{ reservations, nodesUnreachable }`. `/my` is the student-specific alias.

### `GET /api/reservations/:id`

Students can view only their own reservation; another student's reservation returns `403`. Administrators are routed only to their assigned department; a reservation owned by another department is not exposed and returns `404`.

### `DELETE /api/reservations/:id`

The owner student or owning-department admin can cancel only `PENDING` or `APPROVED` reservations. Success is `200`; ownership failure is `403`; terminal/invalid state is `409`. Cancellation restores one reserved unit to available and publishes `RESOURCE_CANCELLED`.

### `PATCH /api/reservations/:id/status` — owning-department admin

Body: `{ "status": "APPROVED" }` or `{ "status": "REJECTED" }`. Only `PENDING` may transition to either state (`409` otherwise). Rejection releases reserved quantity; approval keeps the unit reserved. Wrong department is not authorized.

## Borrow and return

### `POST /api/resources/:id/borrow`

Requires an authenticated student (or authorized owning-department admin) and body `{ "reservationId": "reservation_..." }`. Checkout requires an `APPROVED` reservation for this resource. It marks the reservation `FULFILLED`, transfers one unit from reserved to borrowed, creates a `BORROWED` record, and publishes `RESOURCE_BORROWED`. Missing reservation ID is `400`; ineligible or already fulfilled reservation is `409`.

### `GET /api/borrow-records/my`

Students receive their own borrow records; administrators receive records for their assigned department. Response: `{ borrowRecords, nodesUnreachable }`.

### `POST /api/resources/:id/return`

Body: `{ "borrowRecordId": "borrow_..." }`. The borrower student or owning-department admin may return it once. Success marks it `RETURNED`, records `returnedAt`, restores availability, and publishes `RESOURCE_RETURNED`. Wrong owner is `403`; missing record is `404`; duplicate return is `409`.

## Resource administration

`POST /api/resources`, `PUT /api/resources/:id`, and `DELETE /api/resources/:id` require a department administrator and are scoped to the admin's own department. Quantity changes must preserve the total-quantity invariant.

## Distributed and event endpoints

- `GET /api/nodes`, `GET /api/nodes/health`
- `GET /api/distributed/leader`
- `GET /api/distributed/lamport`
- `GET /api/distributed/vector`
- `POST /api/distributed/election`
- WebSocket `/ws/updates`

`GET /api/nodes` independently probes each department and returns `department`, `nodeId`, `priority`, `status` (`online`, `offline`, or `unknown`), `serviceStatus`, leader/election state, `lamportClock`, heartbeat/health-check time, and per-probe errors. Timeout or ambiguous results are `unknown`; unavailable services are not represented as healthy. This is a current-state view, not a formal consistent global snapshot.

`GET /api/distributed/leader` and `GET /api/distributed/lamport` return one result per department. An unavailable leader is reported with `status: "unknown"` and an error; an unavailable clock has `lamportClock: null`.

`POST /api/distributed/election` has no request body. The Gateway forwards the trigger to one reachable department, which runs the Bully algorithm among CS (priority 1), IT (priority 2), and ENTC (priority 3). A successful response has the shape `{ "triggerNode": "ENTC", "result": { "coordinator": "ENTC", "leaderPriority": 3, "electionStatus": "coordinator_announced", "latestResult": { "success": true, "coordinator": "ENTC" } } }`. If no department can accept the request, or the election returns no coordinator, the Gateway returns `503` with per-node errors or the election result.

Department-only routes `GET /internal/distributed/lamport`, `GET /internal/leader`, `GET /internal/election`, `POST /internal/election`, and `POST /internal/election/message` require `X-Internal-Service-Token`. Election messages carry `type`, `sender`, `messageId`, `electionId`, and a non-negative safe-integer `lamportTimestamp`. Message types are `ELECTION` and `COORDINATOR`; invalid messages return `400`. Each department process owns its own non-persistent Lamport counter.

The existing `GET /api/distributed/vector` endpoint is legacy scaffolding and is not part of the implemented FA-2 concepts. Internal department write and lifecycle endpoints require `X-Internal-Service-Token`. Actor headers are generated by the Gateway from verified JWT claims and are not public API inputs.

## Persistence, quantity, and event semantics

Accounts persist in the Gateway authentication database. Each department persists its own resources and embedded lifecycle records in its department database. MongoDB connection failure prevents service startup; there is no memory fallback. Lifecycle state/quantity changes are conditional single-document atomic writes, so no replica-set transaction is required for these operations. A multi-document transaction would require MongoDB configured as a replica set or sharded cluster.

Every lifecycle response containing a resource maintains `totalQuantity = availableQuantity + reservedQuantity + borrowedQuantity` with non-negative integer quantities. Resource create/update/delete and successful reservation, approval/rejection, cancellation, checkout, and return operations append an event to the owning resource's embedded outbox in the same atomic MongoDB update as the state transition.

`eventDelivery.rabbitmq` is `published` only after a RabbitMQ publisher confirmation; otherwise it remains `pending`. Department workers retry pending events with stable event IDs and a lease, including after restart. Delivery is at-least-once: a broker-confirmed event can be published again if the process crashes before MongoDB records the confirmation. Consumers must deduplicate by event ID. A broker confirmation does not prove that a downstream consumer stored or processed the event.

`eventDelivery.websocketRelay` is `relayed` when the Gateway attempted to broadcast while clients were connected, `accepted_no_clients` when no WebSocket clients were connected, or `pending` while relay is outstanding. Neither status confirms browser receipt. Gateway receipts deduplicate event IDs and expire after 30 days. WebSocket delivery is ephemeral and does not support client acknowledgement or replay. A crash during the relay can result in a repeated broadcast after the relay lease expires.

The outbox is embedded in the authoritative resource document. Completed entries are cleaned up after seven days by default; at most 500 entries per resource are allowed by default. If the cap is reached, further writes fail with `503` until the outbox drains. The entire resource document remains subject to MongoDB's 16 MiB limit. Configure polling, retry, lease, timeouts, retention, batch size, and capacity using the `OUTBOX_*` variables in `.env.example`.

For local development, run MongoDB at `localhost:27017` or set `AUTH_MONGO_URI`, `CS_MONGO_URI`, `IT_MONGO_URI`, and `ENTC_MONGO_URI`. Start RabbitMQ at `localhost:5672` for live broker publishing; otherwise events remain persisted and pending for retry. Docker Compose uses service-DNS URIs and persistent named volumes.
