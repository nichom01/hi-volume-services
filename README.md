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

## M3 status

M3 is implemented with:

- `validate-service`, `audit-service`, and `risk-service` as consumers of `declaration.created`
- Independent PostgreSQL schemas and outbox tables for each new service
- Emitted events:
  - `validation.passed` / `validation.failed`
  - `audit.logged`
  - `risk.assessed` / `risk.flagged`
- Structured JSON-style log lines for service processing events

## M4 status

M4 is implemented with:

- `calculate-service`, `payment-service`, `notify-service`, and `present-service`
- Full flow from `validation.passed + risk.assessed` to `transaction.completed`
- Aggregation state tables for `calculate` and `present` to coordinate multi-event joins
- Health/readiness endpoints on ports `8005` to `8008`

## M5 status

M5 is implemented with:

- Kubernetes manifests under `k8s/base` with:
  - namespace, config map, secret template
  - StatefulSets for PostgreSQL, Zookeeper, Kafka
  - Deployments + Services for all 8 microservices
  - baseline network policies (default deny + internal allow)
- Dev overlay under `k8s/overlays/dev` for single-replica deployment
- CI/CD workflow in `.github/workflows/ci-cd.yml`:
  - service-level `go test` matrix
  - Docker image build (and GHCR push on `main`)
  - kustomize validation for base + dev overlay
  - manual `workflow_dispatch` deploy job to apply dev overlay

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

## Kubernetes quick start

Render base manifests:

```bash
kubectl kustomize k8s/base
```

Render dev overlay:

```bash
kubectl kustomize k8s/overlays/dev
```
