# Runtime Manager

This package owns the control loop that reconciles desired state into runtime targets.

## What It Exposes

- `GET /health`
- `GET /ready`
- `GET /status`
- `GET /runtime-targets`
- `GET /desired-state`
- `GET /snapshots`
- `POST /reconcile/preview`
- `POST /reconcile/run`

## Environment

- `RUNTIME_MANAGER_HOST` or `HOST` defaults to `0.0.0.0`
- `RUNTIME_MANAGER_PORT` or `PORT` defaults to `7102`
- `CONTROL_API_URL` defaults to `http://127.0.0.1:3001`
- `CONTROL_API_TOKEN` defaults to `dev-admin-token`
- `RUNTIME_MANAGER_ROUTER_WORKERS_URL` defaults to `http://127.0.0.1:3002`
- `RUNTIME_MANAGER_ADAPTER_REDPANDA_URL` defaults to `http://127.0.0.1:3003`
- `RUNTIME_MANAGER_REQUEST_TIMEOUT_MS` defaults to `3000`
- `TENANT_ID` defaults to `tenant-local`
- `RUNTIME_MANAGER_SNAPSHOT_REFRESH_MS` defaults to `5000`

## Notes

- The runtime manager now probes `control-api`, `router-workers`, and `adapter-redpanda` on a refresh loop.
- `/ready` only reports success when the control plane can be queried and the cached snapshot is fresh.
- `/reconcile/run` writes `activated`, `pending_activation`, or `degraded` rollout status updates back to `control-api`.
- This is still narrower than a full orchestrator. It does not create or destroy infrastructure; it reconciles deployment rollout state against the observed runtime services that already exist.
