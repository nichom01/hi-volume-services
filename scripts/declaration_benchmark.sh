#!/usr/bin/env bash
# Declaration-service throughput benchmark (local Docker).
# Usage:
#   ./scripts/declaration_benchmark.sh --profile smoke
#   ./scripts/declaration_benchmark.sh --profile target
#   ./scripts/declaration_benchmark.sh --profile soak
#   ./scripts/declaration_benchmark.sh --stepped
#   ./scripts/declaration_benchmark.sh --profile target --duration-sec 60 --rate 200
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

exec python3 "${ROOT_DIR}/scripts/declaration_benchmark_runner.py" "$@"
