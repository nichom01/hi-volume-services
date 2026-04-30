#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

MESSAGES="${1:-1000}"
THROUGHPUT="${2:--1}"

echo "Preparing topic inbound for load test..."
docker exec hvs-kafka kafka-topics --bootstrap-server kafka:9092 \
  --create --if-not-exists --topic inbound --partitions 3 --replication-factor 1 >/dev/null

PAYLOAD_FILE="/tmp/inbound-load-payload.json"
cat > "$PAYLOAD_FILE" <<'EOF'
{"id":"00000000-0000-0000-0000-000000000000","payload":{"amount":999,"customer":"load-test"}}
EOF

echo "Running kafka producer perf test: messages=${MESSAGES}, throughput=${THROUGHPUT}"
docker cp "$PAYLOAD_FILE" hvs-kafka:/tmp/inbound-load-payload.json

docker exec hvs-kafka kafka-producer-perf-test \
  --topic inbound \
  --num-records "$MESSAGES" \
  --throughput "$THROUGHPUT" \
  --record-size 120 \
  --producer-props bootstrap.servers=kafka:9092 acks=1 linger.ms=5 >/tmp/load-test-result.txt

echo "Load test results:"
cat /tmp/load-test-result.txt
