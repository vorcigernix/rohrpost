# Deploy

This directory contains the self-hosted bootstrap for Rohrpost.

## What Is Included

- `docker-compose.yml` for the core stack.
- `docker-compose.console.yml` for the real console build context.
- `nats/` for JetStream server config and stream bootstrap.
- `Dockerfile.bun-runtime` for Bun-based services.
- `.env.example` for local overrides.

## Core Stack

Start the core stack from the repo root:

```bash
docker compose -f deploy/docker-compose.yml up --build
```

This brings up:

- NATS JetStream
- `control-api`
- `runtime-manager`
- `router-workers`
- `adapter-redpanda`

The Bun services use the workspace packages already present in this repo. Each container runs its package `dev` script after ensuring root workspace dependencies are installed in the mounted checkout.
`control-api`, `router-workers`, `adapter-redpanda`, and `runtime-manager` now have container health checks. `runtime-manager` waits for the control plane and both execution targets before it reports ready.

## Bootstrap Mode

Bootstrap mode adds the NATS stream initializer:

```bash
docker compose -f deploy/docker-compose.yml --profile bootstrap up --build
```

That profile enables:

- JetStream stream bootstrap for `router.ingress.>`, `router.work.>`, `router.retry.>`, `router.dlq.>`, `router.audit.>`, and `router.replay.>`

Run bootstrap once before the worker stack if you want the streams provisioned ahead of time:

```bash
docker compose -f deploy/docker-compose.yml --profile bootstrap up nats nats-bootstrap
```

If an existing local broker starts rejecting publishes with `err_code: 10023` / `insufficient resources`, rerun the same bootstrap command. The bootstrap step now reconciles stream retention settings on existing streams instead of only creating missing ones.

## Real Console

Once `apps/console` exists, run the console overlay instead of the placeholder:

```bash
docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.console.yml up --build
```

The console overlay runs against the real control API by default with `VITE_USE_MOCK_API=false`.

## Env

Copy the example file and adjust the values you need:

```bash
cp deploy/.env.example deploy/.env
```

The important values are:

- `BOOTSTRAP_API_TOKEN` for local admin auth.
- `NATS_URL` for the JetStream endpoint.
- `CONTROL_API_PORT`, `RUNTIME_MANAGER_PORT`, `ADAPTER_REDPANDA_PORT`, and `CONSOLE_PORT` if you want non-default host bindings.
- `RUNTIME_MANAGER_REQUEST_TIMEOUT_MS` if you want a different probe and control-api request timeout.
- `CONTROL_API_DB_PATH` if you want the SQLite metadata file somewhere else.
- `ADAPTER_REDPANDA_DELIVERY_LOG_PATH` if you want adapter sink handoff logs written somewhere else.
- `VITE_USE_MOCK_API=false` when you want the console to hit the real API.

## NATS Streams

The bootstrap script creates the canonical streams used by the runtime:

- `router.ingress.>`
- `router.work.>`
- `router.retry.>`
- `router.dlq.>`
- `router.audit.>`
- `router.replay.>`

All are file-backed JetStream streams with a conservative duplicate tracking window and bounded retention defaults. Operational streams default to `DiscardNew`, so capacity exhaustion rejects new publishes and forces upstream retry/backpressure instead of silently evicting already accepted work. Local Kubernetes gives NATS a `20Gi` PVC and defaults the work stream to `12GB`; tune `NATS_STREAM_*_MAX_BYTES` for larger soak tests. `router.audit.>` and `router.dlq.>` default to `DiscardOld`: audit history is diagnostic rather than part of the delivery guarantee, and when the DLQ hits its cap the newest failures are the ones operators need to see.

## Adapter Work Handoff

The Compose stack wires `control-api` to `adapter-redpanda` and gives the adapter access to both NATS and the control plane:

- `router-workers` publish adapter sink deliveries to `router.work.>`
- `adapter-redpanda` consumes those work items, records local delivery history, and writes audit records back to `control-api`
- the default adapter delivery log path in Compose is `/data/adapter-deliveries.jsonl`

## Runtime Manager

`runtime-manager` now runs as a real control loop instead of a static preview surface:

- it probes `control-api`, `router-workers`, and `adapter-redpanda`
- it caches reconciliation snapshots on a refresh interval
- it exposes `/ready` for readiness and `/status` for probe detail
- it updates active deployment rollout states in `control-api` as `activated`, `pending_activation`, or `degraded`

Useful local checks:

```bash
curl -s http://127.0.0.1:${RUNTIME_MANAGER_PORT:-3002}/ready
curl -s http://127.0.0.1:${RUNTIME_MANAGER_PORT:-3002}/status
curl -s -X POST http://127.0.0.1:${RUNTIME_MANAGER_PORT:-3002}/reconcile/run \
  -H 'content-type: application/json' \
  -d '{}'
```

## Local Kubernetes

The repo now includes a local Kubernetes path in `deploy/k8s/`. This is the preferred real-life test workflow because it is much closer to the intended production shape than keeping each Bun service alive in its own terminal.

Use the helper scripts from the repo root:

```bash
bun run k8s:deploy
```

That script:

- builds `rohrpost/runtime:dev-local` and `rohrpost/console:dev-local`
- auto-detects a `kind-*` context and loads those images into the cluster when the `kind` CLI is installed
- also works with Docker Desktop Kubernetes, where the local Docker image store is already shared
- applies the manifests in `deploy/k8s/`
- recreates the `nats-bootstrap` job
- waits for the deployments to become ready

Expose the stack locally from one terminal:

```bash
bun run k8s:port-forward
```

That forwards:

- `console` to `localhost:3000`
- `control-api` to `localhost:3001`
- `router-workers` to `localhost:3002`
- `adapter-redpanda` to `localhost:3003`
- `runtime-manager` to `localhost:7102`

For a Kubernetes-backed soak run with preflight and pod-memory sampling, use the scenario that matches the deployment under test.

The seeded local Kubernetes stack currently defaults to a native `nats -> transform -> nats` demo flow:

```bash
bun run k8s:soak:nats-transform-nats
```

That wrapper:

- port-forwards `control-api`, `router-workers`, and `nats`
- auto-selects the newest active `nats -> transform -> nats` deployment when `LOAD_TEST_DEPLOYMENT_ID` is unset
- subscribes to the sink subject directly instead of relying on `http-counting-sink`
- records publish latency, end-to-end delivery latency, and Kubernetes pod memory samples

For an explicit native `http -> transform -> http` soak:

```bash
LOAD_TEST_DEPLOYMENT_ID=<deployment-id> \
bun run k8s:soak:http-transform-http
```

That wrapper resets the in-cluster counting sink, starts the required port-forwards, verifies one message can make it through the deployment, and then runs `deploy/bin/soak-http-transform-http.ts` with Kubernetes memory probes enabled.

`bun run k8s:soak` now aliases the NATS scenario so the default command matches the seeded Kubernetes deployment shape.
