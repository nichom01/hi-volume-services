#!/usr/bin/env python3
"""
Declaration-service benchmark: rate-limited inbound publish, Postgres correlation,
optional consumer-group lag, JSON + human summary. Stdlib only.
"""
from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
import time
from dataclasses import dataclass, field
import re
from typing import Any


PROFILES: dict[str, dict[str, Any]] = {
    "smoke": {"duration_sec": 30, "rate": 100, "drain_timeout_sec": 120},
    "target": {"duration_sec": 300, "rate": 200, "drain_timeout_sec": 600},
    "soak": {"duration_sec": 900, "rate": 200, "drain_timeout_sec": 1200},
}

DEFAULT_P95_MS = 500.0
DEFAULT_MAX_FAILURE_PCT = 1.0
# kafka-console-producer + flush overhead often lands ~2–6% under target on short runs.
PRODUCER_PACE_RATIO = 0.92


def postgres_host_clock_skew_sec() -> float:
    """Approximate Postgres NOW() epoch minus host time.time() (midpoint)."""
    host_before = time.time()
    cp = run_cmd(
        [
            "docker",
            "exec",
            "hvs-postgres",
            "psql",
            "-U",
            "postgres",
            "-d",
            "microservices",
            "-tAc",
            "SELECT EXTRACT(EPOCH FROM NOW());",
        ],
    )
    host_after = time.time()
    pg_epoch = float(cp.stdout.strip())
    host_mid = (host_before + host_after) / 2.0
    return pg_epoch - host_mid


def run_cmd(
    argv: list[str],
    *,
    stdin: bytes | None = None,
    check: bool = True,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        input=stdin,
        text=True,
        capture_output=True,
        check=check,
        timeout=timeout,
    )


def ensure_topics(partitions: int) -> None:
    for topic in ("inbound", "declaration.created"):
        run_cmd(
            [
                "docker",
                "exec",
                "hvs-kafka",
                "kafka-topics",
                "--bootstrap-server",
                "kafka:9092",
                "--create",
                "--if-not-exists",
                "--topic",
                topic,
                "--partitions",
                str(partitions),
                "--replication-factor",
                "1",
            ],
        )


def kafka_consumer_lag(group: str) -> int | None:
    try:
        cp = run_cmd(
            [
                "docker",
                "exec",
                "hvs-kafka",
                "kafka-consumer-groups",
                "--bootstrap-server",
                "kafka:9092",
                "--describe",
                "--group",
                group,
            ],
            check=False,
        )
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        return None
    if cp.returncode != 0:
        return None
    total = 0
    for line in cp.stdout.splitlines():
        parts = line.split()
        if len(parts) < 6 or parts[0] != group:
            continue
        try:
            lag = int(parts[5])
        except ValueError:
            continue
        total += lag
    return total


def fetch_metrics_body(port: int) -> str:
    import urllib.error
    import urllib.request

    url = f"http://127.0.0.1:{port}/metrics"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            return resp.read().decode()
    except (urllib.error.URLError, OSError):
        return ""


def fetch_service_metrics(port: int) -> dict[str, Any]:
    body = fetch_metrics_body(port)
    out: dict[str, Any] = {}
    want_prefixes = (
        "declaration_declarations_processed_total",
        "declaration_declarations_idempotent_total",
        "declaration_declarations_failed_total",
    )
    for line in body.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        for p in want_prefixes:
            if line.startswith(p + " ") or line.startswith(p + "{"):
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        out[p] = float(parts[-1])
                    except ValueError:
                        pass
                break
    return out


def parse_histogram_buckets(body: str, metric_name: str) -> tuple[dict[float, float], float | None]:
    """Returns (upper_bound -> cumulative count, total count or None)."""
    buckets: dict[float, float] = {}
    count_total: float | None = None
    for line in body.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        if line.startswith(f"{metric_name}_bucket"):
            m = re.search(r'le="([^"]*)"', line)
            if not m:
                continue
            le_s = m.group(1)
            upper = math.inf if le_s == "+Inf" else float(le_s)
            buckets[upper] = float(line.split()[-1])
        elif line.startswith(f"{metric_name}_count "):
            count_total = float(line.split()[-1])
    return buckets, count_total


def prometheus_histogram_quantile(body: str, metric_name: str, q: float) -> float | None:
    """Classic (cumulative) histogram quantile from Prometheus text exposition."""
    buckets, count_total = parse_histogram_buckets(body, metric_name)
    if not buckets:
        return None
    sorted_b = sorted(buckets.items(), key=lambda x: x[0])
    if count_total is None:
        count_total = sorted_b[-1][1]
    if count_total <= 0:
        return None
    target = q * count_total
    prev_upper = 0.0
    prev_count = 0.0
    for upper, cum in sorted_b:
        if cum >= target:
            bucket_width = upper - prev_upper if math.isfinite(upper) else 0.0
            if cum == prev_count or bucket_width <= 0:
                return upper if math.isfinite(upper) else prev_upper
            rank = target - prev_count
            return prev_upper + (rank / (cum - prev_count)) * bucket_width
        prev_upper, prev_count = upper, cum
    return sorted_b[-1][0]


def histogram_quantile_delta(
    body_start: str,
    body_end: str,
    metric_name: str,
    q: float,
) -> float | None:
    """Quantile over observations recorded between two metric scrapes."""
    b0, _ = parse_histogram_buckets(body_start, metric_name)
    b1, _ = parse_histogram_buckets(body_end, metric_name)
    if not b1:
        return None
    keys = sorted(set(b0) | set(b1))
    delta_cum: list[tuple[float, float]] = []
    for upper in keys:
        c0 = b0.get(upper, 0.0)
        c1 = b1.get(upper, 0.0)
        delta_cum.append((upper, max(0.0, c1 - c0)))
    total = delta_cum[-1][1] if delta_cum else 0.0
    if total <= 0:
        return prometheus_histogram_quantile(body_end, metric_name, q)
    target = q * total
    prev_upper = 0.0
    prev_count = 0.0
    for upper, cum in delta_cum:
        if cum >= target:
            bucket_width = upper - prev_upper if math.isfinite(upper) else 0.0
            if cum == prev_count or bucket_width <= 0:
                return upper if math.isfinite(upper) else prev_upper
            rank = target - prev_count
            return prev_upper + (rank / (cum - prev_count)) * bucket_width
        prev_upper, prev_count = upper, cum
    return delta_cum[-1][0]


def percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_vals[int(k)]
    d0 = sorted_vals[int(f)] * (c - k)
    d1 = sorted_vals[int(c)] * (k - f)
    return d0 + d1


@dataclass
class BenchResult:
    profile: str
    duration_sec: float
    target_rate: float
    total_sent: int
    send_phase_sec: float
    achieved_send_rate: float
    completed: int
    drain_wait_sec: float
    wall_to_complete_sec: float
    effective_throughput: float
    incomplete: int
    timeout: bool
    latencies_ms: list[float] = field(default_factory=list)
    p50_ms: float = 0.0
    p95_ms: float = 0.0
    p99_ms: float = 0.0
    failure_pct: float = 0.0
    consumer_lag_end: int | None = None
    service_metrics: dict[str, Any] = field(default_factory=dict)
    metrics_p95_processing_ms: float | None = None
    gates_ok: bool = True
    gate_messages: list[str] = field(default_factory=list)


def run_benchmark(
    *,
    profile: str,
    duration_sec: float,
    rate: float,
    drain_timeout_sec: float,
    topic_partitions: int,
    consumer_group: str,
    metrics_port: int,
    run_id: str,
) -> BenchResult:
    import uuid

    clock_skew = postgres_host_clock_skew_sec()
    ensure_topics(topic_partitions)
    total = max(1, int(round(duration_sec * rate)))
    metrics_body_start = fetch_metrics_body(metrics_port)

    producer = subprocess.Popen(
        [
            "docker",
            "exec",
            "-i",
            "hvs-kafka",
            "kafka-console-producer",
            "--bootstrap-server",
            "kafka:9092",
            "--topic",
            "inbound",
        ],
        stdin=subprocess.PIPE,
        text=True,
    )
    if producer.stdin is None:
        raise RuntimeError("producer stdin unavailable")

    interval = 1.0 / rate if rate > 0 else 0.0
    t0 = time.perf_counter()
    next_t = t0
    sent = 0
    try:
        for i in range(total):
            if interval > 0:
                now = time.perf_counter()
                if now < next_t:
                    time.sleep(next_t - now)
            eid = str(uuid.uuid4())
            ts_wall = time.time()
            payload = {
                "id": eid,
                "payload": {
                    "amount": 100,
                    "customer": "declaration-benchmark",
                    "runId": run_id,
                    "seq": i + 1,
                    "sentAt": ts_wall,
                },
            }
            line = json.dumps(payload, separators=(",", ":")) + "\n"
            producer.stdin.write(line)
            producer.stdin.flush()
            sent += 1
            if interval > 0:
                next_t += interval
    finally:
        producer.stdin.close()
        producer.wait(timeout=60)

    send_end = time.perf_counter()
    send_phase_sec = send_end - t0
    achieved_send_rate = sent / send_phase_sec if send_phase_sec > 0 else 0.0

    # Drain: wait for rows in declaration.declarations
    deadline = time.time() + drain_timeout_sec
    poll = 0.25
    completed = 0
    drain_start = time.perf_counter()
    while time.time() < deadline:
        # Batch count via EXISTS for speed — single scalar count matching run
        chunk = 5000
        # Count all matching runId in DB (avoids huge IN lists)
        q = (
            "SELECT COUNT(*) FROM declaration.declarations d "
            f"WHERE d.payload->'payload'->>'runId' = '{run_id}';"
        )
        cp = run_cmd(
            [
                "docker",
                "exec",
                "hvs-postgres",
                "psql",
                "-U",
                "postgres",
                "-d",
                "microservices",
                "-tAc",
                q,
            ],
        )
        completed = int(cp.stdout.strip() or 0)
        if completed >= sent:
            break
        time.sleep(poll)

    drain_wait_sec = time.perf_counter() - drain_start
    wall_to_complete_sec = time.perf_counter() - t0
    timed_out = completed < sent
    incomplete = max(0, sent - completed)
    effective_throughput = completed / wall_to_complete_sec if wall_to_complete_sec > 0 else 0.0
    failure_pct = (incomplete / sent * 100.0) if sent else 0.0

    # Latency sample: pull created_at for this run (cap rows for huge runs)
    latencies_ms: list[float] = []
    if completed > 0:
        lim = min(completed, 50_000)
        # Use sentAt embedded in stored JSONB so latency is DB-time consistent.
        q_lat = (
            "SELECT (EXTRACT(EPOCH FROM d.created_at) - "
            "(d.payload->'payload'->>'sentAt')::double precision) * 1000.0 "
            "FROM declaration.declarations d "
            f"WHERE d.payload->'payload'->>'runId' = '{run_id}' "
            f"ORDER BY d.created_at LIMIT {lim};"
        )
        cp2 = run_cmd(
            [
                "docker",
                "exec",
                "hvs-postgres",
                "psql",
                "-U",
                "postgres",
                "-d",
                "microservices",
                "-tAc",
                q_lat,
            ],
        )
        for row in cp2.stdout.strip().splitlines():
            if not row.strip():
                continue
            try:
                raw_ms = float(row.strip())
            except ValueError:
                continue
            # Align Postgres created_at epoch with host-sent sentAt using skew sample.
            adj_ms = raw_ms - clock_skew * 1000.0
            latencies_ms.append(max(0.0, adj_ms))

    lat_sorted = sorted(latencies_ms)
    p50 = percentile(lat_sorted, 50)
    p95 = percentile(lat_sorted, 95)
    p99 = percentile(lat_sorted, 99)

    lag = kafka_consumer_lag(consumer_group)
    metrics_body = fetch_metrics_body(metrics_port)
    svc_m = fetch_service_metrics(metrics_port)
    p95_proc_s = histogram_quantile_delta(
        metrics_body_start,
        metrics_body,
        "declaration_processing_seconds",
        0.95,
    )
    metrics_p95_ms = (p95_proc_s * 1000.0) if p95_proc_s is not None else None

    gates_ok = True
    msgs: list[str] = []
    send_kept_pace = achieved_send_rate >= rate * PRODUCER_PACE_RATIO
    if not send_kept_pace:
        gates_ok = False
        msgs.append(
            f"producer rate {achieved_send_rate:.1f}/s < {100 * PRODUCER_PACE_RATIO:.0f}% of target {rate:.1f}/s",
        )
    if failure_pct > DEFAULT_MAX_FAILURE_PCT:
        gates_ok = False
        msgs.append(f"failure_pct {failure_pct:.2f}% > {DEFAULT_MAX_FAILURE_PCT}%")
    if metrics_p95_ms is not None and metrics_p95_ms > DEFAULT_P95_MS:
        gates_ok = False
        msgs.append(
            f"service_p95_processing {metrics_p95_ms:.1f}ms > {DEFAULT_P95_MS}ms",
        )
    elif metrics_p95_ms is None and p95 > DEFAULT_P95_MS:
        gates_ok = False
        msgs.append(
            f"db_observed_p95 {p95:.1f}ms > {DEFAULT_P95_MS}ms (no histogram samples)",
        )
    if timed_out:
        gates_ok = False
        msgs.append("drain timeout before all messages completed")

    return BenchResult(
        profile=profile,
        duration_sec=duration_sec,
        target_rate=rate,
        total_sent=sent,
        send_phase_sec=send_phase_sec,
        achieved_send_rate=achieved_send_rate,
        completed=completed,
        drain_wait_sec=drain_wait_sec,
        wall_to_complete_sec=wall_to_complete_sec,
        effective_throughput=effective_throughput,
        incomplete=incomplete,
        timeout=timed_out,
        latencies_ms=latencies_ms,
        p50_ms=p50,
        p95_ms=p95,
        p99_ms=p99,
        failure_pct=failure_pct,
        consumer_lag_end=lag,
        service_metrics=svc_m,
        metrics_p95_processing_ms=metrics_p95_ms,
        gates_ok=gates_ok,
        gate_messages=msgs,
    )


def run_stepped(
    rates: list[float],
    step_duration_sec: float,
    drain_timeout_sec: float,
    topic_partitions: int,
    consumer_group: str,
    metrics_port: int,
) -> dict[str, Any]:
    import uuid

    steps_out: list[dict[str, Any]] = []
    overall_ok = True
    for r in rates:
        rid = str(uuid.uuid4())
        res = run_benchmark(
            profile=f"step-{int(r)}",
            duration_sec=step_duration_sec,
            rate=r,
            drain_timeout_sec=drain_timeout_sec,
            topic_partitions=topic_partitions,
            consumer_group=consumer_group,
            metrics_port=metrics_port,
            run_id=rid,
        )
        overall_ok = overall_ok and res.gates_ok
        steps_out.append(
            {
                "rate": r,
                "gates_ok": res.gates_ok,
                "effective_throughput": res.effective_throughput,
                "achieved_send_rate": res.achieved_send_rate,
                "db_p95_ms": res.p95_ms,
                "service_p95_processing_ms": res.metrics_p95_processing_ms,
                "failure_pct": res.failure_pct,
                "completed": res.completed,
                "total_sent": res.total_sent,
                "gate_messages": res.gate_messages,
            },
        )
    return {"steps": steps_out, "gates_ok": overall_ok}


def main() -> int:
    ap = argparse.ArgumentParser(description="Declaration service throughput benchmark")
    ap.add_argument(
        "--profile",
        choices=list(PROFILES.keys()),
        help="Preset: smoke (30s@100/s), target (300s@200/s), soak (900s@200/s)",
    )
    ap.add_argument("--duration-sec", type=float, default=None, help="Override profile duration")
    ap.add_argument("--rate", type=float, default=None, help="Override profile msg/s")
    ap.add_argument("--drain-timeout-sec", type=float, default=None)
    ap.add_argument("--topic-partitions", type=int, default=6)
    ap.add_argument("--consumer-group", default="declaration-service-group")
    ap.add_argument("--metrics-port", type=int, default=8001)
    ap.add_argument("--json-out", default=None, help="Write full JSON report path")
    ap.add_argument(
        "--stepped",
        action="store_true",
        help="Run 50,100,150,200 msg/s each for --step-duration-sec",
    )
    ap.add_argument("--step-rates", default="50,100,150,200")
    ap.add_argument("--step-duration-sec", type=float, default=15)
    ap.add_argument("--step-drain-timeout-sec", type=float, default=180)
    args = ap.parse_args()

    import uuid

    if args.stepped:
        rates = [float(x.strip()) for x in args.step_rates.split(",") if x.strip()]
        report = run_stepped(
            rates=rates,
            step_duration_sec=args.step_duration_sec,
            drain_timeout_sec=args.step_drain_timeout_sec,
            topic_partitions=args.topic_partitions,
            consumer_group=args.consumer_group,
            metrics_port=args.metrics_port,
        )
        print(json.dumps(report, indent=2))
        if args.json_out:
            with open(args.json_out, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2)
        return 0 if report["gates_ok"] else 1

    if not args.profile:
        ap.error("--profile is required unless --stepped")

    prof = PROFILES[args.profile]
    duration = float(args.duration_sec if args.duration_sec is not None else prof["duration_sec"])
    rate = float(args.rate if args.rate is not None else prof["rate"])
    drain_to = float(
        args.drain_timeout_sec if args.drain_timeout_sec is not None else prof["drain_timeout_sec"],
    )
    run_id = str(uuid.uuid4())

    res = run_benchmark(
        profile=args.profile,
        duration_sec=duration,
        rate=rate,
        drain_timeout_sec=drain_to,
        topic_partitions=args.topic_partitions,
        consumer_group=args.consumer_group,
        metrics_port=args.metrics_port,
        run_id=run_id,
    )

    summary = {
        "profile": res.profile,
        "run_id": run_id,
        "target_rate": res.target_rate,
        "total_sent": res.total_sent,
        "completed": res.completed,
        "incomplete": res.incomplete,
        "effective_throughput_per_s": round(res.effective_throughput, 2),
        "achieved_send_rate_per_s": round(res.achieved_send_rate, 2),
        "send_phase_sec": round(res.send_phase_sec, 3),
        "drain_wait_sec": round(res.drain_wait_sec, 3),
        "wall_to_complete_sec": round(res.wall_to_complete_sec, 3),
        "p50_ms": round(res.p50_ms, 3),
        "p95_ms": round(res.p95_ms, 3),
        "p99_ms": round(res.p99_ms, 3),
        "failure_pct": round(res.failure_pct, 4),
        "consumer_lag_end": res.consumer_lag_end,
        "gates_ok": res.gates_ok,
        "gate_messages": res.gate_messages,
        "latency_samples": len(res.latencies_ms),
        "service_metrics": res.service_metrics,
        "service_p95_processing_ms": res.metrics_p95_processing_ms,
    }

    print("=== Declaration benchmark ===")
    print(f"run_id={run_id}")
    print(
        f"sent={res.total_sent} completed={res.completed} "
        f"throughput={res.effective_throughput:.1f}/s (target {res.target_rate:.0f}/s)",
    )
    print(
        f"db_latency p50/p95/p99 = {res.p50_ms:.1f} / {res.p95_ms:.1f} / {res.p99_ms:.1f} ms "
        f"(samples={len(res.latencies_ms)})",
    )
    print(f"service_histogram_p95_processing_ms={res.metrics_p95_processing_ms}")
    print(f"failure_pct={res.failure_pct:.3f}% consumer_lag={res.consumer_lag_end}")
    print(f"gates_ok={res.gates_ok}")
    for m in res.gate_messages:
        print(f"  gate: {m}")
    print("=== JSON ===")
    print(json.dumps(summary, indent=2))

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

    return 0 if res.gates_ok else 1


if __name__ == "__main__":
    sys.exit(main())
