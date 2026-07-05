import type { CanonicalEnvelope } from "@rohrpost/shared-flow-spec";

export type AdapterWorkDeliveryMode = "inline" | "connect";
export type AdapterWorkloadBackend = "docker" | "kubernetes" | "disabled";
export type AdapterWorkloadRuntimeRole = "source" | "sink";
export type AdapterWorkloadState = "starting" | "running" | "stopped" | "degraded";
export type AdapterWorkloadTargetKind = "aws_s3" | "file" | "nats_jetstream";

export interface AdapterWorkItem {
  workId: string;
  runId: string;
  enqueuedAt: string;
  deploymentId: string;
  flowId: string;
  revisionId: string;
  sinkId: string;
  connectorId: string;
  capabilityId: string;
  tenantId: string;
  attempt: number;
  sourceRef: string;
  traceId: string;
  messageId: string;
  connectorConfig?: Record<string, unknown>;
  envelope: CanonicalEnvelope;
  payload: unknown;
}

export interface AdapterWorkloadStatusReport {
  key: string;
  connectorId: string;
  capabilityId: string;
  manifestId: string;
  deploymentIds: string[];
  flowIds: string[];
  revisionIds: string[];
  runtimeRole: AdapterWorkloadRuntimeRole;
  inputRef: string;
  outputRef: string;
  status: AdapterWorkloadState;
  backend: AdapterWorkloadBackend;
  consumerRef?: string | null;
  targetKind: AdapterWorkloadTargetKind;
  artifactPath?: string | null;
  configPath: string;
  containerName?: string | null;
  startedAt?: string | null;
  stoppedAt?: string | null;
  lastError?: string | null;
  restartCount: number;
  recentLogs: string[];
}

export const CONNECT_MANAGED_ADAPTER_CAPABILITIES = ["kafka_in", "s3_sink"] as const;

function sanitizeSubjectSegment(value: string): string {
  return value.replace(/[^a-zA-Z0-9_-]+/g, "_");
}

export function isConnectManagedAdapterCapability(capabilityId: string): boolean {
  return (CONNECT_MANAGED_ADAPTER_CAPABILITIES as readonly string[]).includes(capabilityId);
}

export function buildAdapterWorkSubject(input: {
  deliveryMode: AdapterWorkDeliveryMode;
  connectorId: string;
  tenantId: string;
  flowId: string;
  revisionId: string;
  workId: string;
}): string {
  return [
    "router",
    "work",
    input.deliveryMode,
    sanitizeSubjectSegment(input.connectorId),
    input.tenantId,
    input.flowId,
    input.revisionId,
    input.workId,
  ].join(".");
}

export function buildAdapterWorkSubjectPattern(
  deliveryMode: AdapterWorkDeliveryMode,
  connectorId?: string,
): string {
  if (deliveryMode === "connect") {
    if (!connectorId) {
      throw new Error("connectorId is required for connect-managed adapter work subjects");
    }

    return `router.work.connect.${sanitizeSubjectSegment(connectorId)}.>`;
  }

  return "router.work.inline.>";
}
