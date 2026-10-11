# Docker Setup

The Docker Compose configuration defines the frontend, Gateway, CS/IT/ENTC services, RabbitMQ, and four MongoDB instances on `campus-net`. The Gateway uses `mongo-auth`; each department service uses its own database container. Named volumes preserve the four MongoDB data directories. Container builds use the repository root as their build context and each component Dockerfile copies only its required files.

Applications connect to `mongo-auth`, `mongo-cs`, `mongo-it`, and `mongo-entc` via Docker DNS; never use `localhost` for container-to-container MongoDB access. Database URIs are wired in Compose as `AUTH_MONGO_URI`, `CS_MONGO_URI`, `IT_MONGO_URI`, and `ENTC_MONGO_URI`. For local development, `.env.example` uses `localhost` URIs. Set `JWT_SECRET`, `INTERNAL_SERVICE_TOKEN`, and `ADMIN_REGISTRATION_KEY` in `.env` before using admin enrollment. Replace all sample values with unique private values.

## Intended workflow

- docker compose config
- docker compose up --build -d
- docker compose ps
- docker compose logs
- docker compose down

## Persistence and transaction behavior

Each database has a Mongo healthcheck. Gateway and department services wait for their required MongoDB services to be healthy and then establish Mongoose connections before listening. Failed database startup is explicit; Mongoose reconnects after transient connection loss once connected. Department lifecycle changes update one resource document atomically, avoiding replica-set transactions. If future work introduces a transaction spanning multiple documents, configure MongoDB as a replica set or sharded cluster.

`docker compose config` validates the Compose model only. Do not report Docker runtime persistence as verified unless `docker compose up --build -d` succeeds and resource/auth data is confirmed after container restarts. The current host environment has a local MongoDB available for integration tests; Docker runtime must be verified separately.
