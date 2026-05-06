# Declaration service benchmark runbook

## Prerequisites

- Local stack running: `make dev-up` (or `docker compose -f docker-compose.dev.yml up -d --build`)
- Containers reachable: `hvs-kafka`, `hvs-postgres`, `hvs-declaration-service`
- Declaration service rebuilt after metrics/tuning changes: `docker compose -f docker-compose.dev.yml up -d --build declaration-service`

## One-command benchmarks

```bash
# Quick gate (~30s load + drain)
./scripts/declaration_benchmark.sh --profile smoke

# Target: 300s at 200 msg/s (heavy; ~60k messages)
./scripts/declaration_benchmark.sh --profile target

# Soak: 900s at 200 msg/s (very heavy)
./scripts/declaration_benchmark.sh --profile soak
```

Write JSON summary to a file:

```bash
./scripts/declaration_benchmark.sh --profile smoke --json-out /tmp/declaration-bench.json
```

### Stepped baseline (50 → 100 → 150 → 200 msg/s)

```bash
./scripts/declaration_benchmark.sh --stepped
```

Override step length and drain timeout:

```bash
./scripts/declaration_benchmark.sh --stepped --step-duration-sec 20 --step-drain-timeout-sec 240
```

### Faster iteration (shorter target run)

```bash
./scripts/declaration_benchmark.sh --profile target --duration-sec 60 --rate 200
```

## Acceptance gates (script-enforced)

- **Producer pace**: achieved publish rate ≥ **92%** of the target rate (`kafka-console-producer` + flush jitter; short runs are noisier).
- **Reliability**: `failure_pct` ≤ 1% (incomplete vs sent after drain window) and drain completes before `--drain-timeout-sec`.
- **Service latency (primary)**: p95 of `declaration_processing_seconds` ≤ **500 ms**, computed from a **start/end scrape delta** on the Prometheus histogram so long-lived counters do not dilute the result.
- **DB timestamps (informational)**: `created_at` vs embedded `payload.sentAt` reflects end-to-end pipeline backlog (other consumers, disk, etc.) and is **not** used for the primary pass/fail gate when histogram data exists.

Exit code `0` = all gates passed; `1` otherwise.

## Observability

### Prometheus (declaration-service)

```bash
curl -s http://localhost:8001/metrics | grep ^declaration_
```

Key series:

- `declaration_declarations_processed_total`
- `declaration_declarations_failed_total`
- `declaration_declarations_idempotent_total`
- `declaration_processing_seconds_bucket` (histogram)

### Consumer lag

```bash
docker exec hvs-kafka kafka-consumer-groups --bootstrap-server kafka:9092 \
  --describe --group declaration-service-group
```

The benchmark prints aggregate LAG at the end of the run.

## Topic partitions

The benchmark creates `inbound` and `declaration.created` with `--if-not-exists` and a default of **6 partitions**. Existing topics keep their partition count; to apply a higher partition count you must alter topics (or reset dev volumes with `make dev-reset` and recreate).

## Troubleshooting

| Symptom | Checks |
|--------|--------|
| Low throughput | CPU on `hvs-declaration-service`, consumer LAG, Postgres write latency |
| High p95 | Tune `DECLARATION_WORKERS`, `DECLARATION_JOB_BUFFER`, writer batch env vars; ensure enough `inbound` partitions |
| `failure_pct` / timeouts | Increase `--drain-timeout-sec`; verify declaration-service logs for `failed processing` |
| Gates flaky on laptop | Shorten `--duration-sec` while iterating; close other heavy processes |

## Makefile

```bash
make declaration-benchmark-smoke
make declaration-benchmark-stepped
```
