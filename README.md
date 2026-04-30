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
