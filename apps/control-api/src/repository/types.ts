import type { AdapterWorkloadStatusReport } from "@rohrpost/control-api-contracts";
import type { Database } from "bun:sqlite";
import type { ControlApiOverview, ControlApiRuntimeStatsResponse } from "@rohrpost/control-api-contracts";
import type {
  CompiledFlowSummary,
  FlowSpec,
  FlowSpecValidationResult,
  SimulationReport,
} from "@rohrpost/shared-flow-spec";

export interface ApiTokenRecord {
  userId: string;
  label: string;
}

export interface FlowListItem {
  id: string;
  tenantId: string;
  name: string;
  status: string;
  createdAt: string;
  updatedAt: string;
  latestRevisionId: string | null;
  activeRevisionId: string | null;
  revisionId?: string | null;
  spec?: FlowSpec;
  compiler?: CompiledFlowSummary;
}

export interface FlowRevisionRecord {
  id: string;
  flowId: string;
  revisionNumber: number;
  spec: FlowSpec;
  compiler: CompiledFlowSummary;
  simulation: SimulationReport;
  publishedAt: string | null;
  createdAt: string;
}

export interface DeploymentRecord {
  id: string;
  flowId: string;
  revisionId: string;
  status: string;
  rolloutStatus: string;
  createdAt: string;
  rolledBackFrom: string | null;
}

export interface ReplayRequestRecord {
  id: string;
  flowId: string;
  revisionId: string;
  reason: string;
  sourceStream: string;
  status: string;
  createdAt: string;
}

export interface ResolvedConnectorRecord {
  id: string;
  tenantId: string;
  name: string;
  capabilityId: string;
  executionMode: "native" | "adapter";
  config: Record<string, unknown>;
  createdAt: string;
}

export interface RuntimeDeploymentRecord {
  deployment: DeploymentRecord;
  revision: FlowRevisionRecord;
  connectors: Record<string, ResolvedConnectorRecord>;
}

export interface RuntimeRunInput {
  id?: string;
  flowId: string;
  revisionId: string;
  deploymentId?: string | null;
  messageId?: string;
  status: string;
  sourceRef: string;
  traceId: string;
  processedCount: number;
  errorCount: number;
  startedAt: string;
  finishedAt: string;
  lastError?: string | null;
  targetSinkIds?: string[];
  awaitedSinkIds?: string[];
}

export interface AdapterRunResultInput {
  runId: string;
  sinkId: string;
  connectorId: string;
  capabilityId: string;
  status: "succeeded" | "failed";
  targetRef?: string | null;
  artifactPath?: string | null;
  objectKey?: string | null;
  error?: string | null;
  startedAt: string;
  finishedAt: string;
}

export interface RuntimeDeploymentStatsInput {
  deploymentId: string;
  reporterId: string;
  flowId: string;
  revisionId: string;
  acceptedCount: number;
  processedCount: number;
  deliveredCount: number;
  retryingCount: number;
  dlqCount: number;
  failedCount: number;
  filteredCount: number;
  dedupedCount: number;
  sinkAttemptCount: number;
  sinkSuccessCount: number;
  sinkFailureCount: number;
  inflightCount: number;
  backlogCount: number;
  lastAcceptedAt?: string | null;
  lastProcessedAt?: string | null;
  lastError?: string | null;
  updatedAt?: string;
}

export interface RuntimeSampleInput {
  deploymentId: string;
  flowId: string;
  revisionId: string;
  sourceKind: "http" | "nats" | "kafka";
  sourceRef: string;
  payload: unknown;
  observedAt?: string;
}

export interface RuntimeSampleRecord {
  deploymentId: string;
  flowId: string;
  flowName: string;
  revisionId: string;
  sourceKind: "http" | "nats" | "kafka";
  sourceRef: string;
  payload: unknown;
  observedAt: string;
}

export type AdapterWorkloadStatusInput = AdapterWorkloadStatusReport;

export interface AdapterWorkloadStatusRecord extends AdapterWorkloadStatusReport {
  reporterId: string;
  reportedAt: string;
}

export interface RuntimeDeploymentStatsRecord {
  deploymentId: string;
  flowId: string;
  flowName: string;
  revisionId: string;
  rolloutStatus: string;
  reporterIds: string[];
  acceptedCount: number;
  processedCount: number;
  deliveredCount: number;
  retryingCount: number;
  dlqCount: number;
  failedCount: number;
  filteredCount: number;
  dedupedCount: number;
  sinkAttemptCount: number;
  sinkSuccessCount: number;
  sinkFailureCount: number;
  inflightCount: number;
  backlogCount: number;
  lastAcceptedAt: string | null;
  lastProcessedAt: string | null;
  updatedAt: string | null;
  lastError: string | null;
  state: "healthy" | "backlogged" | "degraded" | "idle";
}

export interface RuntimeAuditInput {
  tenantId: string;
  actor: string;
  action: string;
  subjectType: string;
  subjectId: string;
  details: unknown;
}

export interface AiProviderSettingsRecord {
  provider: "gemini";
  enabled: boolean;
  apiKey: string | null;
  model: string;
  apiBaseUrl: string;
  source: "database" | "environment" | "unset";
  updatedAt: string | null;
}

export interface SaveAiProviderSettingsInput {
  provider: "gemini";
  enabled: boolean;
  apiKey?: string | null;
  clearApiKey?: boolean;
  model?: string;
  apiBaseUrl?: string;
}

export interface SaveConnectorInput {
  id?: string;
  tenantId: string;
  name: string;
  capabilityId: string;
  executionMode: "native" | "adapter";
  config: Record<string, unknown>;
}

export interface CreateFlowInput {
  tenantId: string;
  name: string;
  spec: FlowSpec;
  simulation: SimulationReport;
  validation: FlowSpecValidationResult;
}

export interface Repository {
  db: Database;
  authenticate(token: string | null): ApiTokenRecord | null;
  getAiProviderSettings(): AiProviderSettingsRecord;
  saveAiProviderSettings(input: SaveAiProviderSettingsInput): AiProviderSettingsRecord;
  listCapabilities(): typeof import("@rohrpost/domain-connectors").CONNECTOR_CAPABILITIES;
  listConnectors(options?: {
    capabilityId?: string;
    tenantId?: string;
  }): ResolvedConnectorRecord[];
  saveConnector(input: SaveConnectorInput): ResolvedConnectorRecord;
  listFlows(): FlowListItem[];
  listRevisions(flowId: string): FlowRevisionRecord[];
  createOrUpdateFlow(input: CreateFlowInput): FlowRevisionRecord;
  deleteFlow(flowId: string): { flowId: string; deleted: true } | null;
  publishFlow(flowId: string, revisionId?: string | null): { deployment: DeploymentRecord; revision: FlowRevisionRecord };
  rollbackDeployment(
    deploymentId: string,
    targetRevisionId?: string | null,
  ): { deployment: DeploymentRecord; revision: FlowRevisionRecord };
  listRuns(): Array<Record<string, unknown>>;
  listDlq(): Array<Record<string, unknown>>;
  getRuntimeStats(): ControlApiRuntimeStatsResponse;
  listActiveRuntimeDeployments(): RuntimeDeploymentRecord[];
  appendRunSummary(input: RuntimeRunInput): { id: string };
  appendRunSummaries(inputs: RuntimeRunInput[]): { inserted: number };
  recordAdapterRunResult(input: AdapterRunResultInput): { updated: boolean; status: string | null };
  replaceRuntimeDeploymentStats(inputs: RuntimeDeploymentStatsInput[]): { updated: number };
  replaceRuntimeSamples(inputs: RuntimeSampleInput[]): { updated: number };
  listRuntimeSamples(options?: {
    sourceKind?: "http" | "nats" | "kafka";
    limit?: number;
  }): RuntimeSampleRecord[];
  replaceAdapterWorkloadStatuses(input: {
    reporterId: string;
    reportedAt?: string;
    workloads: AdapterWorkloadStatusInput[];
  }): { updated: number; deleted: number; reportedAt: string };
  listAdapterWorkloadStatuses(): AdapterWorkloadStatusRecord[];
  appendAuditRecord(input: RuntimeAuditInput): { id: string };
  listPendingReplayRequests(limit?: number): ReplayRequestRecord[];
  claimReplayRequest(replayId: string): ReplayRequestRecord | null;
  completeReplayRequest(replayId: string, status: "completed" | "failed"): ReplayRequestRecord | null;
  updateDeploymentRuntimeStatus(
    deploymentId: string,
    input: { status?: string; rolloutStatus: string },
  ): DeploymentRecord | null;
  createReplayRequest(input: {
    flowId: string;
    revisionId: string;
    reason: string;
    sourceStream: string;
  }): ReplayRequestRecord;
  getOverview(): ControlApiOverview;
}
