#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

MESSAGES="${1:-1000}"
THROUGHPUT="${2:--1}"
TOPIC="${3:-inbound.perf}"

echo "Preparing topic ${TOPIC} for load test..."
docker exec hvs-kafka kafka-topics --bootstrap-server kafka:9092 \
  --create --if-not-exists --topic "$TOPIC" --partitions 3 --replication-factor 1 >/dev/null

echo "Running kafka producer perf test: topic=${TOPIC}, messages=${MESSAGES}, throughput=${THROUGHPUT}"

docker exec hvs-kafka kafka-producer-perf-test \
  --topic "$TOPIC" \
  --num-records "$MESSAGES" \
  --throughput "$THROUGHPUT" \
  --record-size 120 \
  --producer-props bootstrap.servers=kafka:9092 acks=1 linger.ms=5 >/tmp/load-test-result.txt

echo "Load test results:"
cat /tmp/load-test-result.txt
