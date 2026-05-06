#!/usr/bin/env bash
set -euo pipefail

N="${1:-2000}"
TIMEOUT_SEC="${2:-240}"
POLL_SEC="${3:-3}"
AMOUNT="${4:-100}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TOPICS=(
  inbound
  declaration.created
  validation.passed
  validation.failed
  risk.assessed
  risk.flagged
  calculation.completed
  payment.completed
  notification.sent
  transaction.completed
  audit.logged
)

echo "Ensuring topics exist..."
for topic in "${TOPICS[@]}"; do
  docker exec hvs-kafka kafka-topics --bootstrap-server kafka:9092 \
    --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1 >/dev/null
done

psql_scalar() {
  local query="$1"
  docker exec hvs-postgres psql -U postgres -d microservices -tAc "$query" | tr -d '[:space:]'
}

RUN_ID="$(python3 - <<'PY'
import uuid
print(uuid.uuid4())
PY
)"

TMP_DIR="/tmp/hvs-load-e2e-${RUN_ID}"
mkdir -p "$TMP_DIR"
IDS_FILE="${TMP_DIR}/ids.txt"
PAYLOADS_FILE="${TMP_DIR}/payloads.ndjson"
: > "$IDS_FILE"
: > "$PAYLOADS_FILE"

echo "Capturing baselines..."
BASE_CALC="$(psql_scalar "SELECT COUNT(*) FROM calculation.results;")"
BASE_PRESENT="$(psql_scalar "SELECT COUNT(*) FROM presentation.transactions;")"
BASE_VAL="$(psql_scalar "SELECT COUNT(*) FROM validation.validations;")"
BASE_RISK="$(psql_scalar "SELECT COUNT(*) FROM risk.assessments;")"
BASE_AUDIT="$(psql_scalar "SELECT COUNT(*) FROM audit.logs;")"

echo "Generating ${N} payloads (run_id=${RUN_ID})..."
python3 - "$N" "$RUN_ID" "$AMOUNT" "$IDS_FILE" "$PAYLOADS_FILE" <<'PY'
import json
import sys
import uuid

n = int(sys.argv[1])
run_id = sys.argv[2]
amount = int(sys.argv[3])
ids_file = sys.argv[4]
payloads_file = sys.argv[5]

with open(ids_file, "w", encoding="utf-8") as f_ids, open(payloads_file, "w", encoding="utf-8") as f_out:
    for i in range(1, n + 1):
        event_id = str(uuid.uuid4())
        f_ids.write(event_id + "\n")
        msg = {
            "id": event_id,
            "payload": {
                "amount": amount,
                "customer": "load-e2e",
                "runId": run_id,
                "seq": i
            }
        }
        f_out.write(json.dumps(msg, separators=(",", ":")) + "\n")
PY

echo "Publishing ${N} inbound events..."
docker exec -i hvs-kafka kafka-console-producer \
  --bootstrap-server kafka:9092 \
  --topic inbound < "$PAYLOADS_FILE" >/dev/null

ID_LIST="$(awk '{printf "%s\047%s\047", (NR==1?"":","), $0}' "$IDS_FILE")"

echo "Waiting for downstream processing..."
deadline=$(( $(date +%s) + TIMEOUT_SEC ))

dec=0
val=0
risk=0
aud=0
d_calc=0
d_present=0
d_val=0
d_risk=0
d_aud=0

while :; do
  now=$(date +%s)
  if (( now > deadline )); then
    echo "TIMEOUT waiting for completion"
    break
  fi

  dec="$(psql_scalar "SELECT COUNT(*) FROM declaration.declarations WHERE source_event_id IN (${ID_LIST});")"
  cur_val="$(psql_scalar "SELECT COUNT(*) FROM validation.validations;")"
  cur_risk="$(psql_scalar "SELECT COUNT(*) FROM risk.assessments;")"
  cur_aud="$(psql_scalar "SELECT COUNT(*) FROM audit.logs;")"
  d_val=$((cur_val - BASE_VAL))
  d_risk=$((cur_risk - BASE_RISK))
  d_aud=$((cur_aud - BASE_AUDIT))
  val="$d_val"
  risk="$d_risk"
  aud="$d_aud"

  cur_calc="$(psql_scalar "SELECT COUNT(*) FROM calculation.results;")"
  cur_present="$(psql_scalar "SELECT COUNT(*) FROM presentation.transactions;")"
  d_calc=$((cur_calc - BASE_CALC))
  d_present=$((cur_present - BASE_PRESENT))

  echo "progress dec=${dec}/${N} val_delta=${d_val}/${N} risk_delta=${d_risk}/${N} audit_delta=${d_aud}/${N} calc_delta=${d_calc}/${N} present_delta=${d_present}/${N}"

  if (( dec >= N && val >= N && risk >= N && aud >= N && d_calc >= N && d_present >= N )); then
    echo "PASS run_id=${RUN_ID}"
    exit 0
  fi

  sleep "$POLL_SEC"
done

echo "FAIL run_id=${RUN_ID}"
echo "Final diagnostics:"
echo "  declaration=${dec}/${N}"
echo "  validation_delta=${d_val}/${N}"
echo "  risk_delta=${d_risk}/${N}"
echo "  audit_delta=${d_aud}/${N}"
echo "  calc_delta=${d_calc}/${N}"
echo "  present_delta=${d_present}/${N}"
exit 1
