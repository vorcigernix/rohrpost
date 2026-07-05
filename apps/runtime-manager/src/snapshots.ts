import type {
  DesiredActivation,
  DesiredStateSnapshot,
  ObservedStateSnapshot,
  ReconciliationPlanStep,
  RuntimeTarget,
} from "./runtime-targets";
import { buildReconciliationPlan, summarizeReconciliationPlan } from "./runtime-targets";
import type { ControlApiFlowRecord, ControlApiOverview } from "./control-api";

interface RuntimeSnapshotFlowSpec {
  sources?: Array<{
    id: string;
    connector?: {
      executionMode?: "native" | "adapter";
    };
  }>;
  sinks?: Array<{
    id: string;
    connector?: {
      executionMode?: "native" | "adapter";
    };
  }>;
}

export interface RuntimeSnapshot {
  at: string;
  tenantId: string;
  desired: DesiredStateSnapshot;
  observed: ObservedStateSnapshot;
  plan: ReconciliationPlanStep[];
  summary: ReturnType<typeof summarizeReconciliationPlan>;
  targets: RuntimeTarget[];
  controlApi: ControlApiOverview;
}

function isNativeFlow(record: ControlApiFlowRecord): boolean {
  return record.status === "active";
}

function desiredReplicasForFlow(record: ControlApiFlowRecord): number {
  return isNativeFlow(record) ? 1 : 0;
}

export function targetIdForSpec(spec: RuntimeSnapshotFlowSpec | undefined): string {
  return spec?.sources?.some((source) => source.connector?.executionMode === "adapter")
    || spec?.sinks?.some((sink) => sink.connector?.executionMode === "adapter")
    ? "adapter-redpanda"
    : "router-workers";
}

function targetIdForFlow(record: ControlApiFlowRecord): string {
  return targetIdForSpec(record.spec);
}

export function buildDesiredStateFromControlApi(
  flows: ControlApiFlowRecord[],
  tenantId: string,
): DesiredStateSnapshot {
  return {
    activations: flows
      .filter((flow) => flow.tenantId === tenantId)
      .filter(isNativeFlow)
      .map(
        (flow): DesiredActivation => ({
          flowId: flow.id,
          revisionId: flow.activeRevisionId ?? flow.latestRevisionId ?? flow.revisionId ?? "unpublished",
          targetId: targetIdForFlow(flow),
          desiredReplicas: desiredReplicasForFlow(flow),
        }),
      ),
  };
}

export function buildObservedStateSnapshot(targets: RuntimeTarget[]): ObservedStateSnapshot {
  return {
    targets: targets.map((target) => ({
      targetId: target.id,
      healthy: target.healthy,
      replicas: target.observedReplicas,
      lastObservedAt: target.lastObservedAt,
      lastHeartbeatAt: target.lastHeartbeatAt,
      lastError: target.lastError,
    })),
  };
}

export function buildRuntimeSnapshot(input: {
  tenantId: string;
  targets: RuntimeTarget[];
  controlApi: ControlApiOverview;
  flows: ControlApiFlowRecord[];
}): RuntimeSnapshot {
  const desired = buildDesiredStateFromControlApi(input.flows, input.tenantId);
  const observed = buildObservedStateSnapshot(input.targets);
  const plan = buildReconciliationPlan(desired, observed);

  return {
    at: new Date().toISOString(),
    tenantId: input.tenantId,
    desired,
    observed,
    plan,
    summary: summarizeReconciliationPlan(plan),
    targets: input.targets,
    controlApi: input.controlApi,
  };
}
