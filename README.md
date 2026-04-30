# hi-volume-services

Foundation scaffold for a high-volume event-driven microservices platform.

## M1 status

M1 is implemented with:

- Local development stack via Docker Compose (PostgreSQL, Zookeeper, Kafka, Debezium, declaration-service)
- `declaration-service` in Go with:
  - environment-based configuration
  - database connection and startup retry
  - automatic SQL migrations on boot
  - `/health` and `/ready` endpoints
  - graceful shutdown handling
- `make dev-up` command that boots the stack and verifies service health

## M2 status

M2 is implemented with:

- `declaration-service` Kafka consumer on `inbound`
- Transactional declaration persistence plus outbox write
- Publish of `declaration.created` event after successful commit
- Idempotency via `source_event_id` uniqueness

## Quick start

```bash
make dev-up
```

Check status:

```bash
make dev-ps
curl http://localhost:8001/health
curl http://localhost:8001/ready
```

Stop stack:

```bash
make dev-down
```

If Kafka/Zookeeper metadata drifts during local iteration, reset volumes:

```bash
make dev-reset
```
