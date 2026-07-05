import { Elysia, t } from "elysia";

import { createControlApiClient, type ControlApiClient } from "./control-api";
import { loadRuntimeManagerConfig, type RuntimeManagerConfig } from "./config";
import {
  buildReconciliationPlan,
  DEFAULT_RUNTIME_TARGETS,
  summarizeReconciliationPlan,
  type RuntimeTarget,
} from "./runtime-targets";
import { observeRuntimeTargets, type ObservedRuntimeTargets } from "./target-observer";
import { RuntimeManagerService } from "./runtime-service";

export interface CreateRuntimeManagerAppOptions {
  config?: RuntimeManagerConfig;
  controlApiClient?: ControlApiClient;
  runtimeTargets?: RuntimeTarget[];
  fetchImpl?: typeof fetch;
  observeTargets?: (targets: RuntimeTarget[]) => Promise<ObservedRuntimeTargets>;
  startBackgroundRefresh?: boolean;
}

function createClient(config: RuntimeManagerConfig, options: CreateRuntimeManagerAppOptions): ControlApiClient {
  return (
    options.controlApiClient ??
    createControlApiClient(config.controlApiUrl, config.controlApiToken, {
      fetchImpl: options.fetchImpl,
      timeoutMs: config.requestTimeoutMs,
    })
  );
}

export function createRuntimeManagerApp(options: CreateRuntimeManagerAppOptions = {}) {
  const config = options.config ?? loadRuntimeManagerConfig();
  const runtimeTargets = options.runtimeTargets ?? DEFAULT_RUNTIME_TARGETS;
  const controlApi = createClient(config, options);
  const observeTargetsFn =
    options.observeTargets ??
    ((targets: RuntimeTarget[]) =>
      observeRuntimeTargets({
        config,
        targets,
        fetchImpl: options.fetchImpl,
      }));
  const manager = new RuntimeManagerService(config, controlApi, runtimeTargets, observeTargetsFn);

  if (options.startBackgroundRefresh) {
    manager.start();
  }

  return new Elysia({ name: "runtime-manager" })
    .state("runtimeManager", manager)
    .get("/health", () => ({
      ok: true,
      service: config.serviceName,
      tenantId: config.tenantId,
    }))
    .get("/ready", async ({ status }) => {
      const snapshot = await manager.getSnapshot();
      const state = manager.getState();

      if (!state.ready) {
        return status(503, {
          ok: false,
          service: config.serviceName,
          tenantId: config.tenantId,
          reason: state.lastError ?? "runtime snapshot is not fresh enough for reconciliation",
          snapshotAt: snapshot.at,
          snapshotAgeMs: state.snapshotAgeMs,
        });
      }

      return {
        ok: true,
        service: config.serviceName,
        tenantId: config.tenantId,
        snapshotAt: snapshot.at,
        snapshotAgeMs: state.snapshotAgeMs,
      };
    })
    .get("/status", async () => {
      await manager.getSnapshot();
      const state = manager.getState();

      return {
        service: config.serviceName,
        host: config.host,
        port: config.port,
        controlApiUrl: config.controlApiUrl,
        routerWorkersUrl: config.routerWorkersUrl,
        adapterRedpandaUrl: config.adapterRedpandaUrl,
        tenantId: config.tenantId,
        snapshotRefreshMs: config.snapshotRefreshMs,
        requestTimeoutMs: config.requestTimeoutMs,
        ready: state.ready,
        lastRefreshAt: state.lastRefreshAt,
        lastSuccessfulRefreshAt: state.lastSuccessfulRefreshAt,
        snapshotAgeMs: state.snapshotAgeMs,
        runtimeTargets: state.targetSummary,
        targetProbeSummary: state.targetProbeSummary,
        targetProbes: state.targetProbes,
        controlApi: state.controlApi,
        reconciliationModel:
          "desired deployments are fetched from control-api, compared with live runtime target probes and adapter workload snapshots, and applied as activated, pending_activation, or degraded rollout status updates",
      };
    })
    .get("/runtime-targets", async () => {
      const snapshot = await manager.getSnapshot();
      return {
        targets: snapshot.targets,
      };
    })
    .get("/desired-state", async () => {
      const snapshot = await manager.getSnapshot();

      return {
        tenantId: config.tenantId,
        desired: snapshot.desired,
      };
    })
    .get("/snapshots", async () => {
      const snapshot = await manager.getSnapshot();
      const state = manager.getState();

      return {
        ...snapshot,
        readiness: {
          ready: state.ready,
          lastRefreshAt: state.lastRefreshAt,
          lastSuccessfulRefreshAt: state.lastSuccessfulRefreshAt,
          snapshotAgeMs: state.snapshotAgeMs,
          lastError: state.lastError,
          controlApi: state.controlApi,
        },
      };
    })
    .post(
      "/reconcile/preview",
      async ({ body }) => {
        const snapshot = await manager.getSnapshot(Boolean(body.forceRefresh));
        const targets = body.targets ?? snapshot.targets;
        const desired = body.desired ?? snapshot.desired;
        const observed = body.observed ?? snapshot.observed;
        const plan = buildReconciliationPlan(desired, observed);

        return {
          service: config.serviceName,
          plan,
          summary: summarizeReconciliationPlan(plan),
          snapshot,
        };
      },
      {
        body: t.Object({
          forceRefresh: t.Optional(t.Boolean()),
          targets: t.Optional(
            t.Array(
              t.Object({
                id: t.String(),
                kind: t.String(),
                executionMode: t.String(),
                description: t.String(),
                capabilities: t.Array(t.String()),
                healthy: t.Boolean(),
                desiredReplicas: t.Number({ minimum: 0 }),
                observedReplicas: t.Number({ minimum: 0 }),
                sourceUrl: t.Optional(t.String()),
                lastObservedAt: t.Optional(t.String()),
                lastHeartbeatAt: t.Optional(t.String()),
                lastError: t.Optional(t.String()),
                details: t.Optional(t.Record(t.String(), t.Any())),
              }),
            ),
          ),
          desired: t.Optional(
            t.Object({
              activations: t.Array(
                t.Object({
                  flowId: t.String(),
                  revisionId: t.String(),
                  targetId: t.String(),
                  desiredReplicas: t.Number({ minimum: 0 }),
                }),
              ),
            }),
          ),
          observed: t.Optional(
            t.Object({
              targets: t.Array(
                t.Object({
                  targetId: t.String(),
                  healthy: t.Boolean(),
                  replicas: t.Number({ minimum: 0 }),
                  lastObservedAt: t.Optional(t.String()),
                  lastHeartbeatAt: t.Optional(t.String()),
                  lastError: t.Optional(t.String()),
                }),
              ),
            }),
          ),
        }),
      },
    )
    .post(
      "/reconcile/run",
      async ({ body }) => {
        const result = await manager.reconcile(body.forceRefresh ?? true);

        return {
          service: config.serviceName,
          updated: result.updated,
          unchanged: result.unchanged,
          summary: summarizeReconciliationPlan(result.snapshot.plan),
          snapshot: result.snapshot,
        };
      },
      {
        body: t.Object({
          forceRefresh: t.Optional(t.Boolean()),
        }),
      },
    );
}
