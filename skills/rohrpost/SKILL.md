---
name: rohrpost
description: Set up a local Rohrpost test drive, then create, validate, publish, and inspect event-routing flows from plain-language requests. Use when the user asks to try Rohrpost locally, install or run it, or send website, webhook, HTTP, NATS, or Kafka event data to S3, BigQuery, Snowflake, Kafka, NATS, or HTTP destinations without configuring flows in the UI.
compatibility: Requires Docker, kind, kubectl, Bun, and network access to a running Rohrpost control-api for flow operations.
---

# Rohrpost

Use this skill when the user wants to test-drive Rohrpost locally or route event data without hand-building the flow in the UI.

## First-run local test drive

If the user is new, does not have a running Rohrpost backend, or asks where to start, guide them through the local Kubernetes path. This gives every later step known localhost ports.

Expected local ports after setup:

- console: `http://127.0.0.1:3000`
- control API: `http://127.0.0.1:3001`
- router-workers HTTP ingress: `http://127.0.0.1:3002`
- adapter-redpanda: `http://127.0.0.1:3003`
- runtime-manager: `http://127.0.0.1:7102`
- API token: `dev-admin-token`

### Prerequisites

Check these before deployment:

```bash
docker version
kind version
kubectl version --client
bun --version
```

If Docker is missing or not running, stop and ask the user to install/start Docker Desktop or Docker Engine.

If `kind` is missing, tell the user to install it:

```bash
brew install kind
```

On non-macOS, point them to the official kind install method for their OS rather than inventing commands.

If `kubectl` is missing on macOS:

```bash
brew install kubectl
```

### Create the local cluster

From the Rohrpost repository root:

```bash
kind create cluster --name rohrpost
kubectl cluster-info --context kind-rohrpost
```

If the cluster already exists, do not recreate it. Use it:

```bash
kubectl config use-context kind-rohrpost
```

### Deploy Rohrpost

Know the script behind this command: `bun run k8s:deploy` runs `deploy/bin/k8s-deploy.sh`. That script builds the local runtime, console, NATS, and NATS toolbox images, loads them into kind, applies `deploy/k8s`, runs the `nats-bootstrap` job, and waits for rollouts.

```bash
bun install
K8S_CONTEXT=kind-rohrpost bun run k8s:deploy
```

This builds local images, loads them into kind, applies the Kubernetes manifests, bootstraps NATS JetStream, and waits for the stack.

### Keep port-forwarding open

Know the script behind this command: `bun run k8s:port-forward` runs `deploy/bin/k8s-port-forward.sh`. It opens the known localhost ports for console, control API, router-workers, adapter-redpanda, and runtime-manager.

Start this in a dedicated terminal and keep it running:

```bash
bun run k8s:port-forward
```

Then verify the known ports:

```bash
curl -fsS http://127.0.0.1:3001/health
curl -fsS -H "Authorization: Bearer dev-admin-token" http://127.0.0.1:3001/api/capabilities
curl -fsS http://127.0.0.1:7102/ready
```

Tell the user the console is at `http://127.0.0.1:3000`.

### Troubleshooting

- If port-forwarding fails, check whether something already uses ports `3000`, `3001`, `3002`, `3003`, or `7102`.
- If a rollout times out, run `kubectl -n rohrpost get pods` and inspect the pod that is not ready.
- If images cannot be pulled in kind, rerun `K8S_CONTEXT=kind-rohrpost bun run k8s:deploy`; the script reloads local images into the cluster.
- To reset the test drive: `kind delete cluster --name rohrpost`, then create and deploy again.

## Inputs to collect

Ask only for missing information:

- Control API base URL. Default to `ROHRPOST_API_BASE_URL`, then `http://127.0.0.1:3001`.
- API token. Default to `ROHRPOST_API_TOKEN`, then `dev-admin-token` for the local test drive.
- Source type: `http`, `nats`, or `kafka`. Default to `http` for website/webhook/app events.
- Destination: one of `s3_sink`, `bigquery_sink`, `snowflake_sink`, `kafka_out`, `nats_out`, or `http_out`.
- A small sample JSON payload. If unavailable, ask for one before publishing.
- Destination connector details if the sink is not already configured.

Do not imply Rohrpost can collect data from websites the user does not control. For website data, create an HTTP ingress flow and tell the user their site or tag manager must POST authorized event payloads to the generated ingress endpoint.

## API calls

Use bearer auth for every request:

```bash
BASE_URL="${ROHRPOST_API_BASE_URL:-http://127.0.0.1:3001}"
TOKEN="${ROHRPOST_API_TOKEN:-dev-admin-token}"
AUTH=(-H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json")
```

Check the backend first:

```bash
curl -fsS "$BASE_URL/health"
curl -fsS "${AUTH[@]}" "$BASE_URL/api/capabilities"
```

For prompt-based JSON routing, prefer `compose-json-transform` because it returns a draft, preview, validation details, and source binding:

```bash
curl -fsS "${AUTH[@]}" \
  -X POST "$BASE_URL/api/flows/compose-json-transform" \
  -d '{
    "name": "Website events to S3",
    "prompt": "Send website analytics events to S3, preserving useful ecommerce fields.",
    "sourceKind": "http",
    "sinkCapabilityId": "s3_sink",
    "samplePayload": {
      "event": "purchase",
      "orderId": "ord_123",
      "email": "customer@example.com",
      "total": 42
    }
  }'
```

Before publishing, make sure the destination connector exists when a specific connector is needed:

```bash
curl -fsS "${AUTH[@]}" "$BASE_URL/api/connectors?capabilityId=s3_sink"
```

Create or update a sink connector only when the user gave the destination details:

```bash
curl -fsS "${AUTH[@]}" \
  -X POST "$BASE_URL/api/connectors" \
  -d '{
    "name": "Production S3",
    "capabilityId": "s3_sink",
    "config": {
      "bucket": "example-bucket",
      "prefix": "events/"
    }
  }'
```

If a connector id is supplied or created, either pass it to `compose-json-transform` as `sinkConnectorId` or patch the returned draft sink connector id before saving.

Save and publish the validated draft:

```bash
curl -fsS "${AUTH[@]}" \
  -X POST "$BASE_URL/api/flows" \
  -d '{
    "name": "Website events to S3",
    "samplePayload": {"event":"purchase","orderId":"ord_123","total":42},
    "sourceBinding": { "...": "use the sourceBinding returned by compose-json-transform" },
    "spec": { "...": "use the draft returned by compose-json-transform" }
  }'
```

Then publish the returned revision:

```bash
curl -fsS "${AUTH[@]}" \
  -X POST "$BASE_URL/api/flows/<flowId>/publish" \
  -d '{"revisionId":"<revisionId>"}'
```

## Response to the user

Keep the result short:

- Flow name, flow id, revision id, and deployment id.
- Generated source binding, such as `/ingest/website-events-to-s3`, NATS subject, or Kafka topic.
- Destination connector used.
- One test command for sending a sample event to the generated ingress.
- Any remaining manual step, usually adding credentials to the connector config or wiring the website to POST events.

If the backend is unreachable, stop and report the failed URL and status. Do not invent a flow id or endpoint.

For local test-drive flows, use `http://127.0.0.1:3002<sourceBinding.ref>` as the event ingestion URL after the flow is published.
