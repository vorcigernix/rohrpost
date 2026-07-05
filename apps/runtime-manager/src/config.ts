export interface RuntimeManagerConfig {
  host: string;
  port: number;
  controlApiUrl: string;
  controlApiToken: string;
  requestTimeoutMs: number;
  routerWorkersUrl: string;
  adapterRedpandaUrl: string;
  tenantId: string;
  serviceName: string;
  snapshotRefreshMs: number;
}

const DEFAULTS: RuntimeManagerConfig = {
  host: "0.0.0.0",
  port: 7102,
  controlApiUrl: "http://127.0.0.1:3001",
  controlApiToken: "dev-admin-token",
  requestTimeoutMs: 3_000,
  routerWorkersUrl: "http://127.0.0.1:3002",
  adapterRedpandaUrl: "http://127.0.0.1:3003",
  tenantId: "tenant-local",
  serviceName: "runtime-manager",
  snapshotRefreshMs: 5_000,
};

function parsePort(value: string | undefined, fallback: number): number {
  if (!value) return fallback;

  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > 65535) {
    throw new Error(`Invalid port value: ${value}`);
  }

  return parsed;
}

function parsePositiveNumber(value: string | undefined, fallback: number): number {
  if (!value) return fallback;

  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`Invalid numeric value: ${value}`);
  }

  return parsed;
}

export function loadRuntimeManagerConfig(
  env: Record<string, string | undefined> = process.env,
): RuntimeManagerConfig {
  return {
    host: env.RUNTIME_MANAGER_HOST ?? env.HOST ?? DEFAULTS.host,
    port: parsePort(env.RUNTIME_MANAGER_PORT ?? env.PORT, DEFAULTS.port),
    controlApiUrl: env.CONTROL_API_URL ?? DEFAULTS.controlApiUrl,
    controlApiToken: env.CONTROL_API_TOKEN ?? DEFAULTS.controlApiToken,
    requestTimeoutMs: parsePositiveNumber(
      env.RUNTIME_MANAGER_REQUEST_TIMEOUT_MS,
      DEFAULTS.requestTimeoutMs,
    ),
    routerWorkersUrl: env.RUNTIME_MANAGER_ROUTER_WORKERS_URL ?? DEFAULTS.routerWorkersUrl,
    adapterRedpandaUrl: env.RUNTIME_MANAGER_ADAPTER_REDPANDA_URL ?? DEFAULTS.adapterRedpandaUrl,
    tenantId: env.TENANT_ID ?? DEFAULTS.tenantId,
    serviceName: DEFAULTS.serviceName,
    snapshotRefreshMs: parsePositiveNumber(env.RUNTIME_MANAGER_SNAPSHOT_REFRESH_MS, DEFAULTS.snapshotRefreshMs),
  };
}
