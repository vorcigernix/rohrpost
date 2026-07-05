#!/bin/bash
set -euo pipefail

NAMESPACE="${K8S_NAMESPACE:-rohrpost}"
CONSOLE_LOCAL_PORT="${CONSOLE_LOCAL_PORT:-3000}"
CONTROL_API_LOCAL_PORT="${CONTROL_API_LOCAL_PORT:-3001}"
ROUTER_LOCAL_PORT="${ROUTER_LOCAL_PORT:-3002}"
ADAPTER_LOCAL_PORT="${ADAPTER_LOCAL_PORT:-3003}"
RUNTIME_MANAGER_LOCAL_PORT="${RUNTIME_MANAGER_LOCAL_PORT:-7102}"

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

start_forward() {
  local service="$1"
  local local_port="$2"
  local remote_port="$3"
  local log_file="/tmp/rohrpost-port-forward-${service}.log"

  kubectl -n "$NAMESPACE" port-forward "svc/${service}" "${local_port}:${remote_port}" --address 127.0.0.1 >"$log_file" 2>&1 &
  local pid=$!
  PIDS+=("$pid")
  wait_for_port "$local_port"
}

start_forward console "$CONSOLE_LOCAL_PORT" 80
start_forward control-api "$CONTROL_API_LOCAL_PORT" 3001
start_forward router-workers "$ROUTER_LOCAL_PORT" 3002
start_forward adapter-redpanda "$ADAPTER_LOCAL_PORT" 3003
start_forward runtime-manager "$RUNTIME_MANAGER_LOCAL_PORT" 7102

echo "Port-forwards active:"
echo "  console:         http://127.0.0.1:${CONSOLE_LOCAL_PORT}"
echo "  control-api:     http://127.0.0.1:${CONTROL_API_LOCAL_PORT}"
echo "  router-workers:  http://127.0.0.1:${ROUTER_LOCAL_PORT}"
echo "  adapter:         http://127.0.0.1:${ADAPTER_LOCAL_PORT}"
echo "  runtime-manager: http://127.0.0.1:${RUNTIME_MANAGER_LOCAL_PORT}"
echo
echo "Press Ctrl-C to stop all port-forwards."

wait
