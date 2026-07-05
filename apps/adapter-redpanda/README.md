# Adapter Redpanda Scaffold

This package describes the adapter-executed connector surface for commodity integrations and now includes a minimal work-consumer runtime for adapter sink handoff.

## What It Exposes

- `GET /health`
- `GET /status`
- `GET /manifests`
- `GET /manifests/:connectorId`
- `GET /deliveries`
- `GET /workloads`
- `POST /connectors/test`

## Environment

- `ADAPTER_REDPANDA_HOST` or `HOST` defaults to `0.0.0.0`
- `ADAPTER_REDPANDA_PORT` or `PORT` defaults to `3003`
- `REDPANDA_CONNECT_IMAGE` defaults to `redpandadata/connect:latest`
- `MANIFEST_SOURCE` defaults to `local`
- `ADAPTER_REDPANDA_NATS_URL` or `NATS_URL` defaults to `nats://127.0.0.1:4222`
- `ADAPTER_REDPANDA_CONTROL_API_URL` or `CONTROL_API_URL` defaults to `http://127.0.0.1:3001`
- `ADAPTER_REDPANDA_CONTROL_API_TOKEN` or `CONTROL_API_TOKEN` defaults to `dev-admin-token`
- `ADAPTER_REDPANDA_DELIVERY_LOG_PATH` defaults to `data/adapter-deliveries.jsonl`
- `ADAPTER_REDPANDA_CONNECT_BACKEND` accepts `auto`, `docker`, `kubernetes`, or `disabled`
- `ADAPTER_REDPANDA_CONNECT_K8S_NAMESPACE` sets the namespace for managed Redpanda Connect workloads
- `ADAPTER_REDPANDA_CONNECT_K8S_SERVICE_ACCOUNT_NAME` defaults to `adapter-redpanda-connect`

## Notes

- Connector entries are explicitly marked as `adapter` execution mode.
- Kafka is represented here as adapter-owned, not first-party native execution.
- Adapter sink work is consumed from `router.work.>` and written to a local delivery log for inspection.
- Kafka sources and S3 sinks can be promoted to Redpanda Connect workloads. Docker is used for local supervision; Kubernetes creates a ConfigMap plus Deployment per managed workload.
- Managed Redpanda Connect workload snapshots are reported to `control-api` at `POST /api/runtime/adapter-workloads` and can be read back with `GET /api/runtime/adapter-workloads`.
