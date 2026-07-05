#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NAMESPACE="${K8S_NAMESPACE:-rohrpost}"
K8S_CONTEXT="${K8S_CONTEXT:-$(kubectl config current-context)}"
IMAGE_TAG_SUFFIX="${K8S_IMAGE_TAG_SUFFIX:-$(date +%Y%m%d%H%M%S)}"
RUNTIME_IMAGE="${K8S_RUNTIME_IMAGE:-rohrpost/runtime:dev-local-$IMAGE_TAG_SUFFIX}"
CONSOLE_IMAGE="${K8S_CONSOLE_IMAGE:-rohrpost/console:dev-local-$IMAGE_TAG_SUFFIX}"
NATS_IMAGE="${K8S_NATS_IMAGE:-rohrpost/nats:dev-local-$IMAGE_TAG_SUFFIX}"
NATS_BOX_IMAGE="${K8S_NATS_BOX_IMAGE:-rohrpost/nats-box:dev-local-$IMAGE_TAG_SUFFIX}"
CONSOLE_VITE_USE_MOCK_API="${VITE_USE_MOCK_API:-false}"
CONSOLE_VITE_API_BASE_URL="${VITE_API_BASE_URL:-}"
CONSOLE_VITE_API_TOKEN="${VITE_API_TOKEN:-${BOOTSTRAP_API_TOKEN:-dev-admin-token}}"

cd "$ROOT_DIR"

echo "Building local runtime image: $RUNTIME_IMAGE"
docker build -f deploy/Dockerfile.workspace-runtime -t "$RUNTIME_IMAGE" .

echo "Building local console image: $CONSOLE_IMAGE"
echo "Console build args: VITE_USE_MOCK_API=$CONSOLE_VITE_USE_MOCK_API, VITE_API_BASE_URL=${CONSOLE_VITE_API_BASE_URL:-<same-origin>}"
docker build -f deploy/Dockerfile.console \
  --build-arg VITE_USE_MOCK_API="$CONSOLE_VITE_USE_MOCK_API" \
  --build-arg VITE_API_BASE_URL="$CONSOLE_VITE_API_BASE_URL" \
  --build-arg VITE_API_TOKEN="$CONSOLE_VITE_API_TOKEN" \
  -t "$CONSOLE_IMAGE" .

echo "Building local NATS image: $NATS_IMAGE"
docker build -f deploy/Dockerfile.nats -t "$NATS_IMAGE" .

echo "Building local NATS toolbox image: $NATS_BOX_IMAGE"
docker build -f deploy/Dockerfile.nats-box -t "$NATS_BOX_IMAGE" .

case "$K8S_CONTEXT" in
  kind-*)
    if ! command -v kind >/dev/null 2>&1; then
      echo "Kubernetes context is $K8S_CONTEXT but the 'kind' CLI is not installed." >&2
      exit 1
    fi
    KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-${K8S_CONTEXT#kind-}}"
    echo "Loading images into kind cluster: $KIND_CLUSTER_NAME"
    kind load docker-image --name "$KIND_CLUSTER_NAME" "$RUNTIME_IMAGE" "$CONSOLE_IMAGE" "$NATS_IMAGE" "$NATS_BOX_IMAGE"
    ;;
  docker-desktop|docker-for-desktop)
    echo "Using Docker Desktop Kubernetes context; local Docker images will be referenced directly."
    ;;
  *)
    echo "Using Kubernetes context: $K8S_CONTEXT"
    echo "Make sure $RUNTIME_IMAGE and $CONSOLE_IMAGE are reachable from that cluster."
    ;;
esac

kubectl apply -f deploy/k8s/namespace.yaml >/dev/null
kubectl -n "$NAMESPACE" delete job nats-bootstrap --ignore-not-found >/dev/null 2>&1 || true
kubectl apply -k deploy/k8s

kubectl -n "$NAMESPACE" set image deployment/nats \
  nats="$NATS_IMAGE" >/dev/null
kubectl -n "$NAMESPACE" set image deployment/control-api \
  control-api="$RUNTIME_IMAGE" >/dev/null
kubectl -n "$NAMESPACE" set image deployment/http-counting-sink \
  http-counting-sink="$RUNTIME_IMAGE" >/dev/null
kubectl -n "$NAMESPACE" set image deployment/runtime-manager \
  wait-for-control-api="$RUNTIME_IMAGE" \
  wait-for-router-workers="$RUNTIME_IMAGE" \
  wait-for-adapter="$RUNTIME_IMAGE" \
  runtime-manager="$RUNTIME_IMAGE" >/dev/null
kubectl -n "$NAMESPACE" set image deployment/console \
  console="$CONSOLE_IMAGE" >/dev/null

bootstrap_job_manifest="$(mktemp)"
sed "s|image: rohrpost/nats-box:dev-local|image: $NATS_BOX_IMAGE|g" \
  deploy/k8s/nats-bootstrap-job.yaml >"$bootstrap_job_manifest"
kubectl -n "$NAMESPACE" delete job nats-bootstrap --ignore-not-found >/dev/null 2>&1 || true
kubectl -n "$NAMESPACE" wait --for=delete job/nats-bootstrap --timeout=60s >/dev/null 2>&1 || true

kubectl -n "$NAMESPACE" rollout status deployment/nats --timeout=180s
kubectl -n "$NAMESPACE" create -f "$bootstrap_job_manifest" >/dev/null
rm -f "$bootstrap_job_manifest"
kubectl -n "$NAMESPACE" wait --for=condition=complete job/nats-bootstrap --timeout=180s

kubectl -n "$NAMESPACE" set image deployment/router-workers \
  wait-for-control-api="$RUNTIME_IMAGE" \
  wait-for-nats="$RUNTIME_IMAGE" \
  wait-for-streams="$NATS_BOX_IMAGE" \
  router-workers="$RUNTIME_IMAGE" >/dev/null
kubectl -n "$NAMESPACE" set image deployment/adapter-redpanda \
  wait-for-control-api="$RUNTIME_IMAGE" \
  wait-for-nats="$RUNTIME_IMAGE" \
  wait-for-streams="$NATS_BOX_IMAGE" \
  adapter-redpanda="$RUNTIME_IMAGE" >/dev/null

for deployment in control-api http-counting-sink router-workers adapter-redpanda runtime-manager console; do
  kubectl -n "$NAMESPACE" rollout status "deployment/$deployment" --timeout=240s
done

echo
kubectl -n "$NAMESPACE" get pods -o wide
echo
echo "Cluster deploy is ready."
echo "Next:"
echo "  bash deploy/bin/k8s-port-forward.sh"
