#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NAMESPACE="${K8S_NAMESPACE:-rohrpost}"
ROUTER_LOCAL_PORT="${K8S_SOAK_ROUTER_LOCAL_PORT:-30082}"
CONTROL_API_LOCAL_PORT="${K8S_SOAK_CONTROL_API_LOCAL_PORT:-30081}"
NATS_LOCAL_PORT="${K8S_SOAK_NATS_LOCAL_PORT:-42222}"
RUN_ID="${LOAD_TEST_RUN_ID:-nats-transform-nats-soak-$(date +%Y%m%d%H%M%S)}"
SUMMARY_FILE="${LOAD_TEST_SUMMARY_FILE:-/tmp/${RUN_ID}-summary.json}"
PROGRESS_FILE="${LOAD_TEST_PROGRESS_FILE:-/tmp/${RUN_ID}-progress.log}"

cd "$ROOT_DIR"

PIDS=()
cleanup() {
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT INT TERM

wait_for_port() {
  local port="$1"
  local attempts=0
  until nc -z 127.0.0.1 "$port" >/dev/null 2>&1; do
    attempts=$((attempts + 1))
    if [ "$attempts" -gt 50 ]; then
      echo "Timed out waiting for localhost:$port" >&2
      exit 1
    fi
    sleep 1
  done
}

kubectl -n "$NAMESPACE" port-forward svc/router-workers "${ROUTER_LOCAL_PORT}:3002" --address 127.0.0.1 >/tmp/rohrpost-soak-router-forward.log 2>&1 &
PIDS+=("$!")
kubectl -n "$NAMESPACE" port-forward svc/control-api "${CONTROL_API_LOCAL_PORT}:3001" --address 127.0.0.1 >/tmp/rohrpost-soak-control-api-forward.log 2>&1 &
PIDS+=("$!")
kubectl -n "$NAMESPACE" port-forward svc/nats "${NATS_LOCAL_PORT}:4222" --address 127.0.0.1 >/tmp/rohrpost-soak-nats-forward.log 2>&1 &
PIDS+=("$!")

wait_for_port "$ROUTER_LOCAL_PORT"
wait_for_port "$CONTROL_API_LOCAL_PORT"
wait_for_port "$NATS_LOCAL_PORT"

LOAD_TEST_RUN_ID="$RUN_ID" \
LOAD_TEST_ROUTER_URL="http://127.0.0.1:${ROUTER_LOCAL_PORT}" \
LOAD_TEST_CONTROL_API_URL="http://127.0.0.1:${CONTROL_API_LOCAL_PORT}" \
LOAD_TEST_CONTROL_API_TOKEN="${LOAD_TEST_CONTROL_API_TOKEN:-dev-admin-token}" \
LOAD_TEST_NATS_URL="nats://127.0.0.1:${NATS_LOCAL_PORT}" \
LOAD_TEST_PREFLIGHT=true \
LOAD_TEST_MEMORY_MODE=kubernetes \
LOAD_TEST_K8S_NAMESPACE="$NAMESPACE" \
bun deploy/bin/soak-nats-transform-nats.ts \
  2> >(tee "$PROGRESS_FILE" >&2) \
  | tee "$SUMMARY_FILE"

echo
echo "Progress log: $PROGRESS_FILE"
echo "Summary:      $SUMMARY_FILE"
