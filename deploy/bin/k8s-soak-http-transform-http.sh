#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NAMESPACE="${K8S_NAMESPACE:-rohrpost}"
DEPLOYMENT_ID="${LOAD_TEST_DEPLOYMENT_ID:-${K8S_DEPLOYMENT_ID:-}}"
ROUTER_LOCAL_PORT="${K8S_SOAK_ROUTER_LOCAL_PORT:-30082}"
SINK_LOCAL_PORT="${K8S_SOAK_SINK_LOCAL_PORT:-40111}"
RUN_ID="${LOAD_TEST_RUN_ID:-http-transform-http-soak-$(date +%Y%m%d%H%M%S)}"
SUMMARY_FILE="${LOAD_TEST_SUMMARY_FILE:-/tmp/${RUN_ID}-summary.json}"
PROGRESS_FILE="${LOAD_TEST_PROGRESS_FILE:-/tmp/${RUN_ID}-progress.log}"

if [ -z "$DEPLOYMENT_ID" ]; then
  echo "LOAD_TEST_DEPLOYMENT_ID is required for the http-transform-http soak." >&2
  exit 1
fi

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
kubectl -n "$NAMESPACE" port-forward svc/http-counting-sink "${SINK_LOCAL_PORT}:4011" --address 127.0.0.1 >/tmp/rohrpost-soak-sink-forward.log 2>&1 &
PIDS+=("$!")

wait_for_port "$ROUTER_LOCAL_PORT"
wait_for_port "$SINK_LOCAL_PORT"

curl --fail --silent --show-error -X POST "http://127.0.0.1:${SINK_LOCAL_PORT}/reset" >/dev/null

LOAD_TEST_RUN_ID="$RUN_ID" \
LOAD_TEST_DEPLOYMENT_ID="$DEPLOYMENT_ID" \
LOAD_TEST_ROUTER_URL="http://127.0.0.1:${ROUTER_LOCAL_PORT}" \
LOAD_TEST_SINK_STATUS_URL="http://127.0.0.1:${SINK_LOCAL_PORT}/status" \
LOAD_TEST_PREFLIGHT=true \
LOAD_TEST_MEMORY_MODE=kubernetes \
LOAD_TEST_K8S_NAMESPACE="$NAMESPACE" \
bun deploy/bin/soak-http-transform-http.ts \
  2> >(tee "$PROGRESS_FILE" >&2) \
  | tee "$SUMMARY_FILE"

echo
echo "Progress log: $PROGRESS_FILE"
echo "Summary:      $SUMMARY_FILE"
