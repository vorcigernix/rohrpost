import type { RuntimeManagerConfig } from "./config";
import type { RuntimeTarget } from "./runtime-targets";

export interface RuntimeTargetProbe {
  targetId: string;
  checkedAt: string;
  url?: string;
  skipped?: boolean;
  reachable: boolean;
  healthy: boolean;
  statusCode?: number;
  error?: string;
  details?: Record<string, unknown>;
}

export interface ObservedRuntimeTargets {
  targets: RuntimeTarget[];
  probes: RuntimeTargetProbe[];
}

function isoNow(): string {
  return new Date().toISOString();
}

function abortSignal(timeoutMs: number): { signal: AbortSignal; clear(): void } {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort("target probe timeout"), timeoutMs);

  return {
    signal: controller.signal,
    clear() {
      clearTimeout(timeout);
    },
  };
}

function targetUrl(target: RuntimeTarget, config: RuntimeManagerConfig): string | undefined {
  switch (target.id) {
    case "control-api":
      return `${config.controlApiUrl.replace(/\/$/, "")}/health`;
    case "router-workers":
      return `${config.routerWorkersUrl.replace(/\/$/, "")}/status`;
    case "adapter-redpanda":
      return `${config.adapterRedpandaUrl.replace(/\/$/, "")}/status`;
    default:
      return undefined;
  }
}

function summarizeTarget(
  target: RuntimeTarget,
  url: string | undefined,
  checkedAt: string,
  detail: {
    healthy: boolean;
    replicas: number;
    lastHeartbeatAt?: string;
    details?: Record<string, unknown>;
  },
): RuntimeTarget {
  return {
    ...target,
    healthy: detail.healthy,
    observedReplicas: detail.replicas,
    sourceUrl: url,
    lastObservedAt: checkedAt,
    lastHeartbeatAt: detail.lastHeartbeatAt,
    lastError: undefined,
    details: detail.details,
  };
}

function runtimeConnected(body: Record<string, unknown>): boolean {
  const runtime = body.runtime;
  if (!runtime || typeof runtime !== "object") {
    return false;
  }

  return (runtime as Record<string, unknown>).connected === true;
}

function routerWorkersHealthy(body: Record<string, unknown>): boolean {
  const health = body.health;
  if (!health || typeof health !== "object") {
    return true;
  }

  return (health as Record<string, unknown>).ok !== false;
}

async function probeTarget(
  target: RuntimeTarget,
  config: RuntimeManagerConfig,
  fetchImpl: typeof fetch,
): Promise<{ target: RuntimeTarget; probe: RuntimeTargetProbe }> {
  const checkedAt = isoNow();
  const url = targetUrl(target, config);
  if (!url) {
    return {
      target: {
        ...target,
        lastObservedAt: checkedAt,
      },
      probe: {
        targetId: target.id,
        checkedAt,
        skipped: true,
        reachable: false,
        healthy: target.healthy,
        error: "probe skipped: no probe url configured",
      },
    };
  }

  const { signal, clear } = abortSignal(config.requestTimeoutMs);
  try {
    const response = await fetchImpl(url, {
      headers: {
        accept: "application/json",
      },
      signal,
    });
    clear();

    const body = (await response.json().catch(() => ({}))) as Record<string, unknown>;

    if (!response.ok) {
      return {
        target: {
          ...target,
          healthy: false,
          observedReplicas: 0,
          sourceUrl: url,
          lastObservedAt: checkedAt,
          lastError: `probe failed with status ${response.status}`,
          details: body,
        },
        probe: {
          targetId: target.id,
          checkedAt,
          url,
          reachable: true,
          healthy: false,
          statusCode: response.status,
          error: `probe failed with status ${response.status}`,
          details: body,
        },
      };
    }

    const detail =
      target.id === "control-api"
        ? {
            healthy: body.ok === true,
            replicas: body.ok === true ? 1 : 0,
            details: body,
          }
        : target.id === "router-workers"
          ? {
              healthy: routerWorkersHealthy(body),
              replicas: typeof body.deployments === "number" ? Math.max(1, Number(body.deployments)) : 1,
              details: body,
            }
        : target.id === "adapter-redpanda"
          ? {
              healthy: runtimeConnected(body),
              replicas: runtimeConnected(body) ? 1 : 0,
              details: body,
            }
          : {
              healthy: true,
              replicas: 1,
              details: body,
            };

    return {
      target: summarizeTarget(target, url, checkedAt, detail),
      probe: {
        targetId: target.id,
        checkedAt,
        url,
        reachable: true,
        healthy: detail.healthy,
        statusCode: response.status,
        details: detail.details,
      },
    };
  } catch (error) {
    clear();
    const message =
      error instanceof Error && error.name === "AbortError"
        ? `probe timed out after ${config.requestTimeoutMs}ms`
        : error instanceof Error
          ? error.message
          : String(error);

    return {
      target: {
        ...target,
        healthy: false,
        observedReplicas: 0,
        sourceUrl: url,
        lastObservedAt: checkedAt,
        lastError: message,
      },
      probe: {
        targetId: target.id,
        checkedAt,
        url,
        reachable: false,
        healthy: false,
        error: message,
      },
    };
  }
}

export async function observeRuntimeTargets(input: {
  config: RuntimeManagerConfig;
  targets: RuntimeTarget[];
  fetchImpl?: typeof fetch;
}): Promise<ObservedRuntimeTargets> {
  const fetchImpl = input.fetchImpl ?? fetch;
  const observations = await Promise.all(
    input.targets.map((target) => probeTarget(target, input.config, fetchImpl)),
  );

  return {
    targets: observations.map((entry) => entry.target),
    probes: observations.map((entry) => entry.probe),
  };
}
