import type {
  CompiledFlowSummary,
  ConnectorCapability,
  FlowSpec,
  SimulationReport,
} from "@rohrpost/shared-flow-spec";
import type { AdapterWorkloadStatusReport } from "./adapter-work";

export interface ControlApiOverviewRuntimeSummary {
  acceptedCount: number;
  processedCount: number;
  deliveredCount: number;
  backlogCount: number;
  inflightCount: number;
  healthyDeployments: number;
  degradedDeployments: number;
  lastProcessedAt: string | null;
}

export interface ControlApiOverview {
  flows: number;
  activeDeployments: number;
  runs: number;
  pendingReplays: number;
  capabilities: number;
  runtime?: ControlApiOverviewRuntimeSummary;
  observability?: {
    mode: string;
    consoleRole: string;
  };
  guarantees: {
    mode: string;
    ordering: string;
    duplicatesPossible: boolean;
  };
}

export interface ControlApiFlowRecord {
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

export interface ControlApiConnectorRecord {
  id: string;
  tenantId?: string;
  name?: string;
  capabilityId: string;
  executionMode: "native" | "adapter";
  config: Record<string, unknown>;
  createdAt?: string;
}

export interface ControlApiFlowRevisionRecord {
  id: string;
  flowId: string;
  revisionNumber: number;
  spec: FlowSpec;
  compiler: CompiledFlowSummary;
  simulation: SimulationReport;
  publishedAt: string | null;
  createdAt: string;
}

export interface ControlApiDeploymentRecord {
  id: string;
  flowId: string;
  revisionId: string;
  status: string;
  rolloutStatus: string;
  createdAt: string;
  rolledBackFrom: string | null;
}

export interface ControlApiRuntimeDeploymentRecord {
  deployment: ControlApiDeploymentRecord;
  revision: ControlApiFlowRevisionRecord;
  connectors: Record<string, ControlApiConnectorRecord>;
}

export interface ControlApiRuntimeDeploymentsResponse {
  generatedAt: string;
  deployments: ControlApiRuntimeDeploymentRecord[];
}

export interface ControlApiRuntimeStatsRecord {
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

export interface ControlApiRuntimeStatsResponse {
  observability: {
    mode: string;
    consoleRole: string;
  };
  summary: ControlApiOverviewRuntimeSummary;
  deployments: ControlApiRuntimeStatsRecord[];
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

export interface RuntimeRunSummaryInput {
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

export interface RuntimeSampleInput {
  deploymentId: string;
  flowId: string;
  revisionId: string;
  sourceKind: "http" | "nats" | "kafka";
  sourceRef: string;
  payload: unknown;
  observedAt?: string;
}

export interface ControlApiRuntimeSampleRecord {
  deploymentId: string;
  flowId: string;
  flowName: string;
  revisionId: string;
  sourceKind: "http" | "nats" | "kafka";
  sourceRef: string;
  payload: unknown;
  observedAt: string;
}

export interface RuntimeAuditInput {
  tenantId: string;
  actor: string;
  action: string;
  subjectType: string;
  subjectId: string;
  details: unknown;
}

export interface ControlApiAdapterWorkloadRecord extends AdapterWorkloadStatusReport {
  reporterId: string;
  reportedAt: string;
}

export interface ControlApiAdapterWorkloadsResponse {
  generatedAt: string;
  workloads: ControlApiAdapterWorkloadRecord[];
}

export interface ControlApiAdapterWorkloadStatusInput {
  reporterId: string;
  reportedAt?: string;
  workloads: AdapterWorkloadStatusReport[];
}

export interface ControlApiCapabilitiesResponse {
  native: ConnectorCapability[];
  adapter: ConnectorCapability[];
  guarantees: {
    delivery: string;
    ordering: string;
    duplicatesPossible: boolean;
  };
}

export interface ControlApiAiProviderSettings {
  provider: "gemini";
  enabled: boolean;
  model: string;
  apiBaseUrl: string;
  apiKeyConfigured: boolean;
  source: "database" | "environment" | "unset";
  activeProvider: "gemini" | "heuristic";
  updatedAt: string | null;
}

export interface SaveAiProviderSettingsInput {
  enabled: boolean;
  apiKey?: string;
  clearApiKey?: boolean;
  model: string;
  apiBaseUrl: string;
}

export interface ControlApiOidcConfig {
  enabled: boolean;
  loginRequired: boolean;
  issuerUrl?: string;
  clientId?: string;
  authorizationEndpoint?: string;
  scope?: string;
}

export interface ControlApiAuthSession {
  enabled: boolean;
  authenticated: boolean;
  mode: "api-token" | "oidc";
  user?: {
    sub: string;
    email?: string;
    name?: string;
  };
}

export interface ControlApiReplayRequest {
  id: string;
  flowId: string;
  revisionId: string;
  reason: string;
  sourceStream: string;
  status: string;
  createdAt: string;
}

export interface ControlApiPendingReplayRequestsResponse {
  requests: ControlApiReplayRequest[];
}

export interface ControlApiCountResponse {
  updated: number;
}

export interface ControlApiInsertedResponse {
  inserted: number;
}

export interface ControlApiIdResponse {
  id: string;
}

export interface ControlApiFlowSaveResponse {
  id: string;
  flowId: string;
}

export interface ControlApiFlowPublishResponse {
  deployment: ControlApiDeploymentRecord;
  revision: ControlApiFlowRevisionRecord;
}

export interface ControlApiDraftFlowResponse {
  draft: FlowSpec;
}

export interface ControlApiRuntimeSamplesResponse {
  samples: ControlApiRuntimeSampleRecord[];
}

export interface ControlApiConsoleEventMessage {
  id: string;
  kind: "runtime" | "flows" | "connectors";
  at: string;
}

export interface ControlApiRunRecord {
  id: string;
  flowId: string;
  flowName: string;
  revisionId: string;
  deploymentId?: string | null;
  status: string;
  sourceRef: string;
  traceId: string;
  processedCount: number;
  errorCount: number;
  startedAt: string;
  finishedAt: string;
  lastError?: string | null;
  attempt: number;
  messageId: string;
  partitionKey: string;
  latencyMs: number;
  detail: string;
}

export interface ControlApiSourceBinding {
  sourceKind: "http" | "nats" | "kafka";
  capabilityId: "http_in" | "nats_in" | "kafka_in";
  executionMode: "native" | "adapter";
  connectorId: string;
  connectorName: string;
  ref: string;
  config: Record<string, unknown>;
  generated: boolean;
}

export interface ControlApiTransformComposerResponse {
  assistant: {
    provider: string;
    model: string;
    note?: string;
  };
  plan: {
    suggestedName: string;
    summary: string;
    fieldMappings: Array<{ from: string; to: string }>;
    filterSummary?: string;
    explanation: string[];
    recommendedSinkCapabilityIds: string[];
  };
  preview: {
    accepted: boolean;
    output?: unknown;
    droppedReason?: string;
    notes: string[];
  };
  exportOptions: ConnectorCapability[];
  sourceBinding?: ControlApiSourceBinding;
  draft?: FlowSpec;
}

export type ReplayCompletionStatus = "completed" | "failed";

export interface UpdateDeploymentStatusInput {
  status?: string;
  rolloutStatus: string;
}

export const controlApiPaths = {
  overview: () => "/api/overview",
  capabilities: () => "/api/capabilities",
  aiSettings: () => "/api/setup/ai",
  oidcConfig: () => "/api/auth/oidc",
  authSession: () => "/api/auth/session",
  oidcToken: () => "/api/auth/oidc/token",
  authLogout: () => "/api/auth/logout",
  connectors: (input?: { capabilityId?: string; tenantId?: string }) => {
    const params = new URLSearchParams();
    if (input?.capabilityId) params.set("capabilityId", input.capabilityId);
    if (input?.tenantId) params.set("tenantId", input.tenantId);
    return `/api/connectors${params.size > 0 ? `?${params.toString()}` : ""}`;
  },
  connectorTest: () => "/api/connectors/test",
  flows: () => "/api/flows",
  flow: (flowId: string) => `/api/flows/${encodeURIComponent(flowId)}`,
  flowComposeJsonTransform: () => "/api/flows/compose-json-transform",
  flowDraftFromPrompt: () => "/api/flows/draft-from-prompt",
  flowValidate: () => "/api/flows/validate",
  flowPublish: (flowId: string) => `/api/flows/${encodeURIComponent(flowId)}/publish`,
  deploymentRollback: (deploymentId: string) => `/api/deployments/${encodeURIComponent(deploymentId)}/rollback`,
  replayRequests: () => "/api/replays",
  runs: () => "/api/runs",
  dlq: () => "/api/dlq",
  activeDeployments: () => "/api/runtime/deployments/active",
  runtimeStats: () => "/api/runtime/stats",
  adapterWorkloads: () => "/api/runtime/adapter-workloads",
  runtimeSamples: () => "/api/runtime/samples",
  recentRuntimeSamples: (input?: { sourceKind?: "http" | "nats" | "kafka"; limit?: number }) => {
    const params = new URLSearchParams();
    if (input?.sourceKind) params.set("sourceKind", input.sourceKind);
    if (typeof input?.limit === "number") params.set("limit", String(input.limit));
    return `/api/runtime/samples/recent${params.size > 0 ? `?${params.toString()}` : ""}`;
  },
  runtimeRunsBatch: () => "/api/runtime/runs/batch",
  runtimeRuns: () => "/api/runtime/runs",
  runtimeAdapterResults: () => "/api/runtime/adapter-results",
  runtimeAudit: () => "/api/runtime/audit",
  eventsStream: () => "/api/events/stream",
  pendingReplayRequests: (limit = 25) => `/api/runtime/replays/pending?limit=${encodeURIComponent(String(limit))}`,
  claimReplayRequest: (replayId: string) => `/api/runtime/replays/${encodeURIComponent(replayId)}/claim`,
  completeReplayRequest: (replayId: string) => `/api/runtime/replays/${encodeURIComponent(replayId)}/complete`,
  deploymentStatus: (deploymentId: string) => `/api/runtime/deployments/${encodeURIComponent(deploymentId)}/status`,
} as const;

export const controlApiRuntimePaths = controlApiPaths;

export class ControlApiHttpError extends Error {
  public readonly status: number;
  public readonly path: string;

  public constructor(path: string, status: number) {
    super(`control-api ${path} failed with status ${status}`);
    this.name = "ControlApiHttpError";
    this.path = path;
    this.status = status;
  }
}

export interface ControlApiClientOptions {
  baseUrl: string;
  token?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}

async function requestJson<T>(
  options: ControlApiClientOptions,
  path: string,
  init?: RequestInit,
): Promise<T> {
  const fetchImpl = options.fetchImpl ?? fetch;
  const timeoutMs = options.timeoutMs;
  const controller = timeoutMs ? new AbortController() : undefined;
  const timeout = controller && timeoutMs
    ? setTimeout(() => controller.abort(`control-api timeout after ${timeoutMs}ms`), timeoutMs)
    : undefined;

  try {
    const response = await fetchImpl(`${options.baseUrl.replace(/\/$/, "")}${path}`, {
      ...init,
      signal: controller?.signal,
      headers: {
        Accept: "application/json",
        ...(init?.body ? { "Content-Type": "application/json" } : {}),
        ...(options.token ? { Authorization: `Bearer ${options.token}` } : {}),
        ...(init?.headers ?? {}),
      },
    });

    if (!response.ok) {
      throw new ControlApiHttpError(path, response.status);
    }

    return (await response.json()) as T;
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError" && timeoutMs) {
      throw new Error(`control-api ${path} timed out after ${timeoutMs}ms`);
    }
    throw error;
  } finally {
    if (timeout) {
      clearTimeout(timeout);
    }
  }
}

export interface ControlApiClient {
  fetchOverview(): Promise<ControlApiOverview>;
  fetchFlows(): Promise<ControlApiFlowRecord[]>;
  fetchActiveDeployments(): Promise<ControlApiRuntimeDeploymentRecord[]>;
  fetchRuntimeStats(): Promise<ControlApiRuntimeStatsResponse>;
  fetchAdapterWorkloads(): Promise<ControlApiAdapterWorkloadRecord[]>;
  replaceAdapterWorkloadStatuses(input: ControlApiAdapterWorkloadStatusInput): Promise<ControlApiCountResponse>;
  replaceRuntimeDeploymentStats(inputs: RuntimeDeploymentStatsInput[]): Promise<ControlApiCountResponse>;
  appendRunSummaries(inputs: RuntimeRunSummaryInput[]): Promise<ControlApiInsertedResponse>;
  replaceRuntimeSamples(inputs: RuntimeSampleInput[]): Promise<ControlApiCountResponse>;
  appendAudit(input: RuntimeAuditInput): Promise<ControlApiIdResponse>;
  listPendingReplayRequests(limit?: number): Promise<ControlApiReplayRequest[]>;
  claimReplayRequest(replayId: string): Promise<ControlApiReplayRequest>;
  completeReplayRequest(replayId: string, status: ReplayCompletionStatus): Promise<ControlApiReplayRequest>;
  updateDeploymentStatus(
    deploymentId: string,
    input: UpdateDeploymentStatusInput,
  ): Promise<ControlApiDeploymentRecord>;
}

export function createControlApiClient(options: ControlApiClientOptions): ControlApiClient {
  return {
    fetchOverview: () =>
      requestJson<ControlApiOverview>(options, controlApiPaths.overview()),
    fetchFlows: () =>
      requestJson<ControlApiFlowRecord[]>(options, controlApiPaths.flows()),
    fetchActiveDeployments: async () => {
      const payload = await requestJson<ControlApiRuntimeDeploymentsResponse>(
        options,
        controlApiPaths.activeDeployments(),
      );
      return payload.deployments;
    },
    fetchRuntimeStats: () =>
      requestJson<ControlApiRuntimeStatsResponse>(options, controlApiPaths.runtimeStats()),
    fetchAdapterWorkloads: async () => {
      const payload = await requestJson<ControlApiAdapterWorkloadsResponse>(
        options,
        controlApiPaths.adapterWorkloads(),
      );
      return payload.workloads;
    },
    replaceAdapterWorkloadStatuses: (input) =>
      requestJson<ControlApiCountResponse>(options, controlApiPaths.adapterWorkloads(), {
        method: "POST",
        body: JSON.stringify(input),
      }),
    replaceRuntimeDeploymentStats: (inputs) =>
      requestJson<ControlApiCountResponse>(options, controlApiPaths.runtimeStats(), {
        method: "POST",
        body: JSON.stringify(inputs),
      }),
    appendRunSummaries: (inputs) =>
      requestJson<ControlApiInsertedResponse>(options, controlApiPaths.runtimeRunsBatch(), {
        method: "POST",
        body: JSON.stringify(inputs),
      }),
    replaceRuntimeSamples: (inputs) =>
      requestJson<ControlApiCountResponse>(options, controlApiPaths.runtimeSamples(), {
        method: "POST",
        body: JSON.stringify(inputs),
      }),
    appendAudit: (input) =>
      requestJson<ControlApiIdResponse>(options, controlApiPaths.runtimeAudit(), {
        method: "POST",
        body: JSON.stringify(input),
      }),
    listPendingReplayRequests: async (limit = 25) => {
      const payload = await requestJson<ControlApiPendingReplayRequestsResponse>(
        options,
        controlApiPaths.pendingReplayRequests(limit),
      );
      return payload.requests;
    },
    claimReplayRequest: (replayId) =>
      requestJson<ControlApiReplayRequest>(options, controlApiPaths.claimReplayRequest(replayId), {
        method: "POST",
      }),
    completeReplayRequest: (replayId, status) =>
      requestJson<ControlApiReplayRequest>(options, controlApiPaths.completeReplayRequest(replayId), {
        method: "POST",
        body: JSON.stringify({ status }),
      }),
    updateDeploymentStatus: (deploymentId, input) =>
      requestJson<ControlApiDeploymentRecord>(options, controlApiPaths.deploymentStatus(deploymentId), {
        method: "POST",
        body: JSON.stringify(input),
      }),
  };
}
