# Campus Share

Campus Share is a distributed campus resource-sharing platform with a React frontend, an API gateway, and three independent department services for CS, IT, and ENTC.

## Architecture

- Frontend: React + Vite
- Gateway: Express API and WebSocket relay
- Department services: CS, IT, ENTC
- Persistence: Gateway-owned MongoDB auth database and one MongoDB database per department service
- Messaging: RabbitMQ topic exchange
- Coordination: gRPC peer communication

## Reservation and borrowing lifecycle

- Students search across the department services, then create a reservation with `POST /api/reservations`.
- A reservation is created as `PENDING` and immediately moves one quantity from available to reserved.
- A department administrator for the resource's owning department can approve or reject it with `PATCH /api/reservations/:id/status`.
- A rejection or cancellation restores the reserved quantity. Only pending or approved reservations can be cancelled.
- Borrowing requires an approved reservation. Checkout moves one item from reserved to borrowed and marks the reservation `FULFILLED`.
- A borrow record belongs to the student on that reservation. Only that student or an administrator of the owning department can return it.
- Each department stores each resource and its lifecycle records in one MongoDB document. Conditional single-document updates atomically change reservation/borrow states and all quantity counters; integration tests race two department processes against the same last unit.
- Lifecycle updates append stable-ID events to an embedded outbox in the same atomic MongoDB resource-document update. Department workers publish events to RabbitMQ using publisher confirms and retry transient failures after restart.
- The Gateway stores relay receipts and deduplicates event IDs. Responses distinguish RabbitMQ confirmation from Gateway WebSocket relay status; neither status claims durable browser receipt.

## Persistence and security status

Gateway users are persisted in the Gateway authentication database. Each department service is authoritative for its own resource documents, embedded reservations, and embedded borrow records. MongoDB connection failure prevents the service from starting; there is no in-memory fallback. Development seed resources are inserted idempotently with `$setOnInsert` and are never overwritten on startup.

Lifecycle state, quantity changes, and event creation need no MongoDB multi-document transaction: each is one conditional atomic update to the owning resource document. Delivery is at-least-once. A crash after broker confirmation but before recording confirmation can cause a duplicate publish with the same event ID; consumers should deduplicate by event ID. RabbitMQ confirmation means broker acceptance only; the application does not configure a durable consumer queue.

The Gateway keeps relay receipts with a 30-day TTL. `relayed` means the Gateway attempted a WebSocket broadcast while clients were connected, not that a browser acknowledged receipt. `accepted_no_clients` is not client delivery. WebSocket delivery remains ephemeral; there is no client acknowledgement or replay guarantee.

Completed outbox entries are removed after seven days by default. Entries per resource are capped at 500 by default; resource/lifecycle writes receive `503` when the cap is reached. The resource, embedded lifecycle history, and outbox are still subject to MongoDB's 16 MiB document limit. Archive/export history before a resource approaches that limit.

There is no automatic migration for records that existed only in an old process's memory; those records must be exported before stopping that version if they need to be retained.

The Gateway forwards verified JWT identity to department lifecycle routes using the shared `INTERNAL_SERVICE_TOKEN`. Configure a unique high-entropy value outside local development. Department-admin signup additionally requires the server-side `ADMIN_REGISTRATION_KEY`; it is never accepted from a normal login or lifecycle request. The checked-in internal-token fallback exists only for the local academic demo.

## Quick start

1. Copy `.env.example` to `.env` and set unique values for `JWT_SECRET`, `INTERNAL_SERVICE_TOKEN`, and `ADMIN_REGISTRATION_KEY` (admin enrollment is disabled without its key).
2. Install dependencies:
   npm install
   npm --prefix frontend install
3. Start MongoDB on `localhost:27017` (or configure `AUTH_MONGO_URI`, `CS_MONGO_URI`, `IT_MONGO_URI`, and `ENTC_MONGO_URI` in `.env`). Start RabbitMQ on `localhost:5672` for publishing; when RabbitMQ is unavailable, outbox entries remain pending and retry.
4. Start the services in separate terminals:
   node gateway/server.js
   node services/cs/server.js
   node services/it/server.js
   node services/entc/server.js
5. Start the frontend:
   npm --prefix frontend run dev -- --host 0.0.0.0
6. Open the frontend at http://localhost:5173

Create a student account and a department-admin account from the sidebar. Resource requests must be approved by an admin of the resource-owning department before checkout.

## Test

npx vitest run tests/smoke.test.js
npm --prefix frontend run build

## Notes

MongoDB-backed integration tests use five unique test databases, exercise concurrent reservations, restart a department service to verify persisted resource, reservation, and pending outbox data, and test retry, lease, confirmation, and cleanup behavior. They require reachable MongoDB. A live RabbitMQ test runs only when a broker is reachable; otherwise it is skipped. `docker compose config` does not verify Docker runtime. Phase 6 algorithms remain incomplete.
