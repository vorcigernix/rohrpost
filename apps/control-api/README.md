# Control API

The control API is the phase-1 system of record for control-plane metadata. It exposes the planned v1 REST routes, stores metadata in SQLite using Bun's built-in driver, and seeds a local bootstrap admin token for development.

## Environment

- `CONTROL_API_PORT` — defaults to `3001`
- `CONTROL_API_HOST` — defaults to `0.0.0.0`
- `CONTROL_API_DB_PATH` — defaults to `data/control-plane.db`
- `BOOTSTRAP_ADMIN_EMAIL` — defaults to `admin@local.rohrpost`
- `BOOTSTRAP_API_TOKEN` — defaults to `dev-admin-token`
- `DEFAULT_TENANT_ID` — defaults to `tenant_demo`
- `DEFAULT_TENANT_NAME` — defaults to `Demo Tenant`
- `ADAPTER_REDPANDA_URL` — optional adapter runtime URL for capability aggregation and adapter connector tests

## Development

```bash
bun run dev
```

Authenticate API requests with `Authorization: Bearer dev-admin-token` unless you override the bootstrap token.
