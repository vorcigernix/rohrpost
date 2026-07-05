export type RuntimeTargetKind =
  | "control-api"
  | "router-workers"
  | "adapter-redpanda"
  | "console";

export type ExecutionMode = "native" | "adapter";

export interface RuntimeTarget {
  id: string;
  kind: RuntimeTargetKind;
  executionMode: ExecutionMode;
  description: string;
  capabilities: string[];
  healthy: boolean;
  desiredReplicas: number;
  observedReplicas: number;
  sourceUrl?: string;
  lastObservedAt?: string;
  lastHeartbeatAt?: string;
  lastError?: string;
  details?: Record<string, unknown>;
}

export interface DesiredActivation {
  flowId: string;
  revisionId: string;
  targetId: string;
  desiredReplicas: number;
}

export interface ObservedTargetState {
  targetId: string;
  healthy: boolean;
  replicas: number;
  lastObservedAt?: string;
  lastHeartbeatAt?: string;
  lastError?: string;
}

export interface ReconciliationPlanStep {
  action: "no-op" | "scale-up" | "scale-down" | "activate-revision" | "report-drift";
  targetId: string;
  reason: string;
  desired?: DesiredActivation;
  observed?: ObservedTargetState;
}

export interface DesiredStateSnapshot {
  activations: DesiredActivation[];
}

export interface ObservedStateSnapshot {
  targets: ObservedTargetState[];
}

export type DeploymentRolloutStatus =
  | "activated"
  | "pending_activation"
  | "degraded";

export const DEFAULT_RUNTIME_TARGETS: RuntimeTarget[] = [
  {
    id: "control-api",
    kind: "control-api",
    executionMode: "native",
    description: "Owns flow definitions, revisions, deployments, secrets, and replay metadata.",
    capabilities: ["auth", "flow-publish", "deployment-tracking"],
    healthy: true,
    desiredReplicas: 1,
    observedReplicas: 1,
  },
  {
    id: "router-workers",
    kind: "router-workers",
    executionMode: "native",
    description: "Consumes JetStream work items and executes deterministic flow processors.",
    capabilities: ["routing", "retry", "dlq", "replay"],
    healthy: true,
    desiredReplicas: 1,
    observedReplicas: 1,
  },
  {
    id: "adapter-redpanda",
    kind: "adapter-redpanda",
    executionMode: "adapter",
    description: "Hosts adapter-executed connectors such as Kafka and other commodity integrations.",
    capabilities: ["kafka-in", "kafka-out", "connector-manifests"],
    healthy: true,
    desiredReplicas: 1,
    observedReplicas: 1,
  },
  {
    id: "console",
    kind: "console",
    executionMode: "native",
    description: "Management UI for flow authoring, simulation, deployment, and observability.",
    capabilities: ["ui", "drafting", "simulation"],
    healthy: true,
    desiredReplicas: 1,
    observedReplicas: 1,
  },
];

export function buildReconciliationPlan(
  desired: DesiredStateSnapshot,
  observed: ObservedStateSnapshot,
): ReconciliationPlanStep[] {
  const observedById = new Map(observed.targets.map((target) => [target.targetId, target]));

  return desired.activations.map((activation) => {
    const current = observedById.get(activation.targetId);

    if (!current) {
      return {
        action: "activate-revision",
        targetId: activation.targetId,
        reason: "target is not yet reported as active, so the revision needs to be activated",
        desired: activation,
      } satisfies ReconciliationPlanStep;
    }

    if (!current.healthy) {
      return {
        action: "report-drift",
        targetId: activation.targetId,
        reason: "target is unhealthy and needs operator attention before reconciliation can continue",
        desired: activation,
        observed: current,
      } satisfies ReconciliationPlanStep;
    }

    if (current.replicas < activation.desiredReplicas) {
      return {
        action: "scale-up",
        targetId: activation.targetId,
        reason: "observed replicas are below the desired count",
        desired: activation,
        observed: current,
      } satisfies ReconciliationPlanStep;
    }

    if (current.replicas > activation.desiredReplicas) {
      return {
        action: "scale-down",
        targetId: activation.targetId,
        reason: "observed replicas are above the desired count",
        desired: activation,
        observed: current,
      } satisfies ReconciliationPlanStep;
    }

    return {
      action: "no-op",
      targetId: activation.targetId,
      reason: "desired and observed state already match",
      desired: activation,
      observed: current,
    } satisfies ReconciliationPlanStep;
  });
}

export function summarizeReconciliationPlan(steps: ReconciliationPlanStep[]) {
  const counts = {
    "no-op": 0,
    "scale-up": 0,
    "scale-down": 0,
    "activate-revision": 0,
    "report-drift": 0,
  };

  for (const step of steps) {
    counts[step.action] += 1;
  }

  return {
    counts,
    total: steps.length,
    requiresAction: steps.some((step) => step.action !== "no-op"),
  };
}

export function summarizeTargets(targets: RuntimeTarget[]) {
  return {
    total: targets.length,
    healthy: targets.filter((target) => target.healthy).length,
    degraded: targets.filter((target) => !target.healthy).length,
    native: targets.filter((target) => target.executionMode === "native").length,
    adapter: targets.filter((target) => target.executionMode === "adapter").length,
    desiredReplicas: targets.reduce((count, target) => count + target.desiredReplicas, 0),
    observedReplicas: targets.reduce((count, target) => count + target.observedReplicas, 0),
  };
}

export function deriveRolloutStatus(
  observed: ObservedTargetState | undefined,
  desiredReplicas: number,
): DeploymentRolloutStatus {
  if (!observed) {
    return "pending_activation";
  }

  if (!observed.healthy) {
    return "degraded";
  }

  if (observed.replicas < desiredReplicas) {
    return "pending_activation";
  }

  return "activated";
}
