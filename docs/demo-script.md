# Campus Share lifecycle demo

1. Start MongoDB, then the API Gateway, CS/IT/ENTC services, and frontend using the README commands. Each service must connect to its configured database before it accepts requests.
2. Register and sign in as a student; search for an item such as Arduino. Open its details to inspect department ownership and availability.
3. Request a reservation. Show the `PENDING` state, available quantity decrement, and a `RESOURCE_RESERVED` WebSocket event.
4. Sign in as an administrator assigned to the same department. Approve the student's pending request. As a contrast, try an admin account assigned to another department and show that it cannot review or edit the resource.
5. Return to the student account and check out the approved reservation. Show `FULFILLED`, the active borrow record, and available/reserved/borrowed quantities.
6. Return the item, then submit a duplicate return and show the `409` rejection without another success event.
7. Demonstrate cancellation on a separate pending reservation; show that its quantity is released.
8. Run `npx vitest run tests/smoke.test.js` to show MongoDB persistence across an owning-service restart, account persistence after database reconnect, same-process and two-process last-unit races, and API authorization tests.

9. To demonstrate outbox recovery, start the department services with RabbitMQ unavailable, create a reservation, and inspect the owning resource's embedded `outbox` entry. It remains pending in MongoDB. Start RabbitMQ and wait for the worker poll/retry; verify the same event ID is confirmed. Run the publisher retry/lease tests when a live broker is not available.

Explain the prototype boundary accurately: Gateway accounts and department-owned resource/lifecycle data persist in separate MongoDB databases. Each lifecycle transition and its event append atomically update one resource document. The publisher offers at-least-once delivery, so a crash after broker confirmation may cause a duplicate event with the same ID; consumers must deduplicate. No durable consumer queue is configured, so publisher confirmation is not proof of downstream processing. Gateway relay receipts are persisted for 30 days, but WebSocket delivery is ephemeral: `relayed` means a broadcast was attempted with connected clients, not acknowledged by a browser. The outbox is capped at 500 entries per resource by default and completed entries are retained for seven days; all embedded data remains subject to MongoDB's 16 MiB document cap. Docker runtime is only verified if the Compose services are actually started and exercised.
