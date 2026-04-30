#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "Running M6 smoke test..."

TOPICS=(
  inbound
  declaration.created
  calculation.completed
  payment.completed
  notification.sent
  transaction.completed
)

for topic in "${TOPICS[@]}"; do
  docker exec hvs-kafka kafka-topics --bootstrap-server kafka:9092 \
    --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1 >/dev/null
done

INBOUND_ID="$(python3 - <<'PY'
import uuid
print(uuid.uuid4())
PY
)"

echo "Publishing inbound event: ${INBOUND_ID}"
printf '{"id":"%s","payload":{"amount":1800,"customer":"smoke-e2e"}}\n' "$INBOUND_ID" \
  | docker exec -i hvs-kafka kafka-console-producer --bootstrap-server kafka:9092 --topic inbound >/dev/null

echo "Checking declaration.created emission..."
docker exec hvs-kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic declaration.created \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 10000 >/tmp/declaration-event.json

CALC_ID="$(python3 - <<'PY'
import uuid
print(uuid.uuid4())
PY
)"
CID="$(python3 - <<'PY'
import uuid
print(uuid.uuid4())
PY
)"

printf '{"id":"%s","timestamp":"2026-01-01T00:00:00Z","aggregateId":"%s","aggregateType":"calculation","eventType":"calculation.completed","version":1,"payload":{"calculationId":"%s","result":"ok"},"metadata":{"correlationId":"%s","causationId":"%s","traceId":"%s","source":"calculate-service"}}\n' \
  "$CALC_ID" "$CALC_ID" "$CALC_ID" "$CID" "$CALC_ID" "$CID" \
  | docker exec -i hvs-kafka kafka-console-producer --bootstrap-server kafka:9092 --topic calculation.completed >/dev/null

echo "Checking downstream transaction.completed emission..."
docker exec hvs-kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic transaction.completed \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 15000 >/tmp/transaction-event.json

echo "Validating persistence..."
docker exec hvs-postgres psql -U postgres -d microservices -tAc "SELECT COUNT(*) FROM payment.transactions;" | awk '{exit !($1>=1)}'
docker exec hvs-postgres psql -U postgres -d microservices -tAc "SELECT COUNT(*) FROM notification.messages;" | awk '{exit !($1>=1)}'
docker exec hvs-postgres psql -U postgres -d microservices -tAc "SELECT COUNT(*) FROM presentation.transactions;" | awk '{exit !($1>=1)}'

echo "M6 smoke test passed."
