import { isConnectManagedAdapterCapability } from "@rohrpost/control-api-contracts";
import type {
  ControlApiAdapterWorkloadRecord,
  ControlApiClient,
  ControlApiDeploymentRecord,
  ControlApiFlowRecord,
  ControlApiOverview,
  ControlApiRuntimeDeploymentRecord,
} from "./control-api";
import type { RuntimeManagerConfig } from "./config";
import { buildRuntimeSnapshot, targetIdForSpec, type RuntimeSnapshot } from "./snapshots";
import { observeRuntimeTargets, type ObservedRuntimeTargets, type RuntimeTargetProbe } from "./target-observer";
import {
  DEFAULT_RUNTIME_TARGETS,
  deriveRolloutStatus,
  summarizeTargets,
  type RuntimeTarget,
  type DeploymentRolloutStatus,
  type ObservedTargetState,
} from "./runtime-targets";

const EMPTY_OVERVIEW: ControlApiOverview = {
  flows: 0,
  activeDeployments: 0,
  runs: 0,
  pendingReplays: 0,
  capabilities: 0,
  guarantees: {
    mode: "unknown",
    ordering: "unknown",
    duplicatesPossible: true,
  },
};

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function safeValue<T>(load: () => Promise<T>, fallback: T) {
  try {
    return {
      ok: true,
      value: await load(),
      error: undefined,
    };
  } catch (error) {
    return {
      ok: false,
      value: fallback,
      error: errorMessage(error),
    };
  }
}

function tenantIdForDeployment(
  deployment: ControlApiRuntimeDeploymentRecord,
  fallbackTenantId: string,
): string {
  return deployment.revision.spec.metadata?.tenantId ?? fallbackTenantId;
}

function deploymentConnectManagedConnectorIds(deployment: ControlApiRuntimeDeploymentRecord): Set<string> {
  const connectorIds = new Set<string>();
  const nodes = [
    ...(deployment.revision.spec.sources ?? []),
    ...(deployment.revision.spec.sinks ?? []),
  ];

  for (const node of nodes) {
    const connectorId = node.connector?.connectorId;
    if (!connectorId || node.connector?.executionMode !== "adapter") {
      continue;
    }

    const connector = deployment.connectors[connectorId];
    if (connector && isConnectManagedAdapterCapability(connector.capabilityId)) {
      connectorIds.add(connector.id);
    }
  }

  return connectorIds;
}

function adapterWorkloadsForDeployment(
  deploymentId: string,
  workloads: ControlApiAdapterWorkloadRecord[],
): ControlApiAdapterWorkloadRecord[] {
  return workloads.filter((workload) => workload.deploymentIds.includes(deploymentId));
}

export function deriveAdapterAwareRolloutStatus(input: {
  targetId: string;
  observed: ObservedTargetState | undefined;
  desiredReplicas: number;
  deployment: ControlApiRuntimeDeploymentRecord;
  adapterWorkloads: ControlApiAdapterWorkloadRecord[];
}): DeploymentRolloutStatus {
  const baseStatus = deriveRolloutStatus(input.observed, input.desiredReplicas);
  if (baseStatus !== "activated" || input.targetId !== "adapter-redpanda") {
    return baseStatus;
  }

  const requiredConnectorIds = deploymentConnectManagedConnectorIds(input.deployment);
  if (requiredConnectorIds.size === 0) {
    return baseStatus;
  }

  const linkedWorkloads = adapterWorkloadsForDeployment(
    input.deployment.deployment.id,
    input.adapterWorkloads,
  ).filter((workload) => requiredConnectorIds.has(workload.connectorId));

  const reportedConnectorIds = new Set(linkedWorkloads.map((workload) => workload.connectorId));
  const hasMissingWorkload = [...requiredConnectorIds].some((connectorId) => !reportedConnectorIds.has(connectorId));
  if (linkedWorkloads.length === 0 || hasMissingWorkload) {
    return "pending_activation";
  }

  if (linkedWorkloads.some((workload) => workload.status === "degraded" || workload.status === "stopped")) {
    return "degraded";
  }

  if (linkedWorkloads.some((workload) => workload.status === "starting")) {
    return "pending_activation";
  }

  return "activated";
}

function statusSummary(probes: RuntimeTargetProbe[]) {
  const activeProbes = probes.filter((probe) => !probe.skipped);

  return {
    total: probes.length,
    active: activeProbes.length,
    skipped: probes.filter((probe) => probe.skipped).length,
    reachable: activeProbes.filter((probe) => probe.reachable).length,
    unreachable: activeProbes.filter((probe) => !probe.reachable).length,
    healthy: activeProbes.filter((probe) => probe.healthy).length,
    degraded: activeProbes.filter((probe) => !probe.healthy).length,
  };
}

export interface RuntimeManagerSnapshotState {
  ready: boolean;
  lastRefreshAt?: string;
  lastSuccessfulRefreshAt?: string;
  snapshotAgeMs?: number;
  lastError?: string;
  targetSummary: ReturnType<typeof summarizeTargets>;
  targetProbes: RuntimeTargetProbe[];
  targetProbeSummary: ReturnType<typeof statusSummary>;
  controlApi: {
    reachable: boolean;
    lastError?: string;
    errors: string[];
  };
  snapshot?: RuntimeSnapshot;
}

export interface ReconciliationResult {
  snapshot: RuntimeSnapshot;
  updated: Array<{
    deploymentId: string;
    from: string;
    to: string;
    record: ControlApiDeploymentRecord;
  }>;
  unchanged: string[];
}

export class RuntimeManagerService {
  private refreshTimer: ReturnType<typeof setInterval> | undefined;
  private refreshInFlight: Promise<RuntimeSnapshot> | undefined;
  private snapshot: RuntimeSnapshot | undefined;
  private targetProbes: RuntimeTargetProbe[] = [];
  private lastRefreshAt: string | undefined;
  private lastSuccessfulRefreshAt: string | undefined;
  private lastError: string | undefined;
  private controlApiErrors: string[] = [];

  public constructor(
    private readonly config: RuntimeManagerConfig,
    private readonly controlApi: ControlApiClient,
    private readonly baseTargets: RuntimeTarget[] = DEFAULT_RUNTIME_TARGETS,
    private readonly observeTargets: (targets: RuntimeTarget[]) => Promise<ObservedRuntimeTargets>,
  ) {}

  public start(): void {
    if (this.refreshTimer) {
      return;
    }

    void this.refresh().catch(() => undefined);
    this.refreshTimer = setInterval(() => {
      void this.refresh().catch(() => undefined);
    }, this.config.snapshotRefreshMs);
  }

  public async stop(): Promise<void> {
    if (this.refreshTimer) {
      clearInterval(this.refreshTimer);
      this.refreshTimer = undefined;
    }
  }

  public getState(): RuntimeManagerSnapshotState {
    const observedTargets = this.snapshot?.targets ?? this.baseTargets;
    const snapshotAgeMs =
      this.snapshot?.at ? Math.max(0, Date.now() - Date.parse(this.snapshot.at)) : undefined;
    const lastControlApiError =
      this.controlApiErrors.length > 0
        ? this.controlApiErrors[this.controlApiErrors.length - 1]
        : undefined;
    const ready =
      Boolean(this.snapshot) &&
      this.controlApiErrors.length === 0 &&
      this.targetProbes.some((probe) => probe.targetId === "control-api" && probe.healthy) &&
      (snapshotAgeMs ?? Number.POSITIVE_INFINITY) <= this.config.snapshotRefreshMs * 3;

    return {
      ready,
      lastRefreshAt: this.lastRefreshAt,
      lastSuccessfulRefreshAt: this.lastSuccessfulRefreshAt,
      snapshotAgeMs,
      lastError: this.lastError,
      targetSummary: summarizeTargets(observedTargets),
      targetProbes: this.targetProbes,
      targetProbeSummary: statusSummary(this.targetProbes),
      controlApi: {
        reachable: this.targetProbes.some((probe) => probe.targetId === "control-api" && probe.reachable),
        lastError: lastControlApiError,
        errors: [...this.controlApiErrors],
      },
      snapshot: this.snapshot,
    };
  }

  public async getSnapshot(forceRefresh = false): Promise<RuntimeSnapshot> {
    if (!forceRefresh && this.snapshot) {
      return this.snapshot;
    }

    return this.refresh();
  }

  public async refresh(): Promise<RuntimeSnapshot> {
    if (this.refreshInFlight) {
      return this.refreshInFlight;
    }

    this.refreshInFlight = this.doRefresh().finally(() => {
      this.refreshInFlight = undefined;
    });

    return this.refreshInFlight;
  }

  public async reconcile(forceRefresh = true): Promise<ReconciliationResult> {
    const snapshot = await this.getSnapshot(forceRefresh);
    const [deployments, adapterWorkloads] = await Promise.all([
      this.controlApi.fetchActiveDeployments(),
      this.controlApi.fetchAdapterWorkloads(),
    ]);
    const observedById = new Map(snapshot.observed.targets.map((target) => [target.targetId, target]));
    const updated: ReconciliationResult["updated"] = [];
    const unchanged: string[] = [];

    for (const record of deployments) {
      if (tenantIdForDeployment(record, this.config.tenantId) !== snapshot.tenantId) {
        continue;
      }

      const targetId = targetIdForSpec(record.revision.spec);
      const desiredActivation = snapshot.desired.activations.find(
        (activation) =>
          activation.flowId === record.deployment.flowId &&
          activation.revisionId === record.deployment.revisionId &&
          activation.targetId === targetId,
      );
      const nextRolloutStatus = deriveAdapterAwareRolloutStatus({
        targetId,
        observed: observedById.get(targetId),
        desiredReplicas: desiredActivation?.desiredReplicas ?? 1,
        deployment: record,
        adapterWorkloads,
      });

      if (record.deployment.rolloutStatus === nextRolloutStatus) {
        unchanged.push(record.deployment.id);
        continue;
      }

      const next = await this.controlApi.updateDeploymentStatus(record.deployment.id, {
        status: record.deployment.status,
        rolloutStatus: nextRolloutStatus,
      });

      updated.push({
        deploymentId: record.deployment.id,
        from: record.deployment.rolloutStatus,
        to: nextRolloutStatus,
        record: next,
      });
    }

    return {
      snapshot,
      updated,
      unchanged,
    };
  }

  private async doRefresh(): Promise<RuntimeSnapshot> {
    const observedTargets = await this.observeTargets(this.baseTargets);
    this.targetProbes = observedTargets.probes;

    const [overview, flows] = await Promise.all([
      safeValue(() => this.controlApi.fetchOverview(), EMPTY_OVERVIEW),
      safeValue<ControlApiFlowRecord[]>(() => this.controlApi.fetchFlows(), []),
    ]);

    const snapshot = buildRuntimeSnapshot({
      tenantId: this.config.tenantId,
      targets: observedTargets.targets,
      controlApi: overview.value,
      flows: flows.value,
    });

    this.controlApiErrors = [overview.error, flows.error].filter(
      (value): value is string => typeof value === "string",
    );
    this.lastRefreshAt = new Date().toISOString();
    this.lastError = undefined;
    if (this.controlApiErrors.length > 0) {
      this.lastError = this.controlApiErrors.join("; ");
    } else if (observedTargets.probes.some((probe) => !probe.healthy)) {
      this.lastError = "one or more runtime targets are degraded";
    }

    this.snapshot = snapshot;
    if (this.controlApiErrors.length === 0) {
      this.lastSuccessfulRefreshAt = this.lastRefreshAt;
    }

    return snapshot;
  }
}
