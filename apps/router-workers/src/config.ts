import type { DeploymentTargetMap, RouterWorkerRuntimeConfig, SinkTarget } from "./phase2-types";

function parsePort(value: string | undefined, fallback: number): number {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function parseNumber(value: string | undefined, fallback: number): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function parseJson<T>(value: string | undefined): T | undefined {
  if (!value) return undefined;
  try {
    return JSON.parse(value) as T;
  } catch {
    return undefined;
  }
}

function normalizeSinkTargets(value: unknown): Record<string, SinkTarget> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  const result: Record<string, SinkTarget> = {};

  for (const [connectorId, target] of Object.entries(value as Record<string, unknown>)) {
    if (!target || typeof target !== "object") continue;
    const item = target as Record<string, unknown>;

    if (item.kind === "http" && typeof item.url === "string") {
      result[connectorId] = {
        kind: "http",
        connectorId,
        url: item.url,
        method: item.method === "PUT" || item.method === "PATCH" ? item.method : "POST",
        headers: typeof item.headers === "object" && item.headers ? (item.headers as Record<string, string>) : undefined,
        timeoutMs: typeof item.timeoutMs === "number" ? item.timeoutMs : undefined,
      };
    } else if (item.kind === "nats" && typeof item.subject === "string") {
      result[connectorId] = {
        kind: "nats",
        connectorId,
        subject: item.subject,
        headers: typeof item.headers === "object" && item.headers ? (item.headers as Record<string, string>) : undefined,
      };
    }
  }

  return result;
}

export function loadRouterWorkerConfig(env: NodeJS.ProcessEnv = process.env): RouterWorkerRuntimeConfig {
  const serviceName = env.ROUTER_WORKER_NAME ?? env.WORKER_ID ?? "router-workers";
  const sinkTargets = normalizeSinkTargets(parseJson<Record<string, unknown>>(env.ROUTER_SINK_TARGETS_JSON));
  const backlogWarningThreshold = parseNumber(env.ROUTER_BACKLOG_WARNING_THRESHOLD, 10_000);

  return {
    serviceName,
    httpHost: env.ROUTER_HTTP_HOST ?? "0.0.0.0",
    httpPort: parsePort(env.ROUTER_HTTP_PORT, 3002),
    controlApiUrl: env.ROUTER_CONTROL_API_URL ?? env.CONTROL_API_URL,
    controlApiToken: env.ROUTER_CONTROL_API_TOKEN ?? env.CONTROL_API_TOKEN,
    natsUrl: env.ROUTER_NATS_URL ?? env.NATS_URL,
    pollIntervalMs: parseNumber(env.ROUTER_POLL_INTERVAL_MS, 5_000),
    subscriptionConcurrency: parseNumber(env.ROUTER_SUBSCRIPTION_CONCURRENCY, 16),
    httpTimeoutMs: parseNumber(env.ROUTER_HTTP_TIMEOUT_MS, 2_500),
    retryBaseDelayMs: parseNumber(env.ROUTER_RETRY_BASE_DELAY_MS, 250),
    maxAttempts: parseNumber(env.ROUTER_MAX_ATTEMPTS, 3),
    runHistoryLimit: parseNumber(env.ROUTER_RUN_HISTORY_LIMIT, 5_000),
    dlqHistoryLimit: parseNumber(env.ROUTER_DLQ_HISTORY_LIMIT, 1_000),
    metricsFlushIntervalMs: parseNumber(env.ROUTER_METRICS_FLUSH_INTERVAL_MS, 2_000),
    runtimeSampleCaptureIntervalMs: parseNumber(env.ROUTER_RUNTIME_SAMPLE_CAPTURE_INTERVAL_MS, 15_000),
    runtimeSampleMaxPayloadBytes: parseNumber(env.ROUTER_RUNTIME_SAMPLE_MAX_PAYLOAD_BYTES, 16_384),
    backlogWarningThreshold,
    ingressMaxBufferedPerDeployment: parseNumber(
      env.ROUTER_INGRESS_MAX_BUFFERED_PER_DEPLOYMENT,
      Math.max(250, Math.floor(backlogWarningThreshold / 4)),
    ),
    ingressMaxBufferedTotal: parseNumber(
      env.ROUTER_INGRESS_MAX_BUFFERED_TOTAL,
      Math.max(1_000, backlogWarningThreshold),
    ),
    ingressRetryAfterMs: parseNumber(env.ROUTER_INGRESS_RETRY_AFTER_MS, 1_000),
    processingStallMs: parseNumber(env.ROUTER_PROCESSING_STALL_MS, 30_000),
    replayStream: env.ROUTER_REPLAY_STREAM ?? "replay",
    dlqStream: env.ROUTER_DLQ_STREAM ?? "dlq",
    ingressStream: env.ROUTER_INGRESS_STREAM ?? "ingress",
  };
}

export function loadDeploymentTargetMap(env: NodeJS.ProcessEnv = process.env): DeploymentTargetMap {
  return {
    sinks: normalizeSinkTargets(parseJson<Record<string, unknown>>(env.ROUTER_SINK_TARGETS_JSON)),
  };
}
