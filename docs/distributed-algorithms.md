# Distributed State and Coordination

Campus Share implements three distributed-systems concepts across its CS, IT, and ENTC services: Lamport logical clocks, a current-state monitor, and Bully leader election. The monitor is not a formal consistent global snapshot.

## Lamport logical clocks

Each department process owns an independent counter, initially zero. `tick()` advances it for a local event. `receive(remoteTimestamp)` validates a non-negative safe integer and sets the local value to `max(local, remote) + 1`.

Successful resource creation/update/deletion, reservation, approval/rejection, cancellation, checkout, and return operations create an outbox event stamped with the department’s Lamport value. The outbox publisher includes `lamportTimestamp` in the RabbitMQ event and Gateway WebSocket relay payload without changing existing event names or removing existing fields. Election requests, coordinator announcements, and acknowledgements also carry Lamport timestamps; receiving the same message ID twice does not receive its timestamp twice.

The publisher can deliver RabbitMQ messages, but this application has no RabbitMQ consumer. Consequently, this change does not claim to receive or merge clock values from RabbitMQ. The `/internal/distributed/lamport` endpoint reports the process-local counter; a service restart resets it because clocks are not persisted.

## Current-state monitoring

Each department reports an authenticated heartbeat to the Gateway every five seconds. The Gateway also probes department health and leader state independently and provides the combined view at `GET /api/nodes`. The response identifies department and priority, health/availability, leader and election state, logical clock, last available heartbeat or successful health check, and per-node errors.

Each probe has a bounded timeout. Healthy peers remain visible when another node fails. The status is `online`, `offline`, or `unknown`: a healthy response is online; a clear refusal or responding service failure is offline; a timeout or otherwise ambiguous result is unknown. Heartbeat data is the last successful report, not proof of a current health check. Docker Compose lets the Gateway start when department containers have started rather than requiring all three health checks to pass.

This is a best-effort collection of current observations made at different times. It is not an atomic snapshot and does not establish a consistent global state.

## Bully election

Priorities are CS=1, IT=2, and ENTC=3. An initiating department sends authenticated `ELECTION` messages to every higher-priority department. A higher-priority receiver acknowledges and initiates its own election. A node that receives no higher-priority response within the bounded timeout announces itself as coordinator to the peer departments. A follower periodically probes its known coordinator; an unavailable coordinator triggers another election. A recovered higher-priority peer triggers re-election so the highest available node can regain leadership.

Election and coordinator messages have unique IDs and are deduplicated. Lower-priority announcements cannot replace a known higher-priority coordinator. HTTP requests have timeouts; an election performs at most two request rounds before reporting failure. Per-process election state is in memory and is not durable. Network partitions can produce temporary disagreement until nodes communicate again.

The Gateway only routes an explicit election trigger to a reachable department; the department nodes perform the election. The algorithm does not provide data replication, database failover, or exactly-once processing.

## Interfaces

- `GET /api/nodes` and `GET /api/nodes/health` — aggregated current node observations.
- `GET /api/distributed/leader` — per-department leader state, preserving errors for unavailable nodes.
- `GET /api/distributed/lamport` — per-department logical clock; unavailable values are `null`, not fabricated zeroes.
- `POST /api/distributed/election` — ask a reachable department to start the Bully election and return its result.
- Internal department routes: `GET /internal/leader`, `GET /internal/election`, `POST /internal/election`, and authenticated `POST /internal/election/message`.
- `GET /internal/distributed/lamport` — inspect one department’s process-local clock.

The existing vector-clock endpoint is outside this implementation and is not an implemented FA-2 concept. Ricart–Agrawala mutual exclusion is not implemented.
