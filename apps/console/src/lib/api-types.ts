import type {
  CompiledFlowSummary,
  ConnectorExecutionMode,
  FlowSpec,
  FlowSpecValidationResult,
  SimulationReport,
  SinkDeliveryGuarantee,
} from '@rohrpost/shared-flow-spec';

export type ExecutionMode = ConnectorExecutionMode;
export type SinkGuarantee = SinkDeliveryGuarantee;

export interface OverviewData {
  totals: {
    activeFlows: number;
    healthyPipelines: number;
    replayQueue: number;
    backlogMessages: number;
    deliveredMessages: number;
    adapterConnectors: number;
  };
  guardrails: Array<{
    label: string;
    detail: string;
    tone: 'good' | 'info' | 'warn';
  }>;
  activity: Array<{
    time: string;
    title: string;
    detail: string;
    tone: 'good' | 'info' | 'warn' | 'danger';
  }>;
}

export interface FlowRecord {
  id: string;
  name: string;
  status: 'draft' | 'published' | 'degraded' | 'paused';
  revisionId: string;
  execution: ExecutionMode;
  sourceLabel: string;
  sourceKind: string;
  processors: string[];
  sinkLabel: string;
  sinkGuarantee: SinkGuarantee;
  updatedAt: string;
  compiled: CompiledFlowSummary;
  backendSpec?: FlowSpec;
}

export interface RunRecord {
  id: string;
  flowName: string;
  status: 'succeeded' | 'failed' | 'retrying' | 'dlq';
  attempt: number;
  messageId: string;
  partitionKey: string;
  traceId: string;
  latencyMs: number;
  startedAt: string;
  finishedAt: string;
  detail: string;
}

export interface RuntimeDeploymentRecord {
  deploymentId: string;
  flowId: string;
  flowName: string;
  revisionId: string;
  rolloutStatus: 'activated' | 'pending_activation' | 'degraded';
  state: 'healthy' | 'backlogged' | 'degraded' | 'idle';
  acceptedCount: number;
  processedCount: number;
  deliveredCount: number;
  backlogCount: number;
  inflightCount: number;
  retryingCount: number;
  dlqCount: number;
  lastAcceptedAt: string | null;
  lastProcessedAt: string | null;
  updatedAt: string | null;
  lastError: string | null;
}

export interface AdapterWorkloadRecord {
  reporterId: string;
  reportedAt: string;
  key: string;
  connectorId: string;
  capabilityId: string;
  manifestId: string;
  deploymentIds: string[];
  flowIds: string[];
  revisionIds: string[];
  runtimeRole: 'source' | 'sink';
  inputRef: string;
  outputRef: string;
  status: 'starting' | 'running' | 'stopped' | 'degraded';
  backend: 'docker' | 'kubernetes' | 'disabled';
  consumerRef?: string | null;
  targetKind: 'aws_s3' | 'file' | 'nats_jetstream';
  artifactPath?: string | null;
  containerName?: string | null;
  lastError?: string | null;
  restartCount: number;
  recentLogs: string[];
}

export interface RuntimeData {
  observability: {
    mode: string;
    consoleRole: string;
  };
  summary: {
    acceptedCount: number;
    processedCount: number;
    deliveredCount: number;
    backlogCount: number;
    inflightCount: number;
    healthyDeployments: number;
    degradedDeployments: number;
    lastProcessedAt: string | null;
  };
  deployments: RuntimeDeploymentRecord[];
}

export interface CapabilityRecord {
  id: string;
  label: string;
  execution: ExecutionMode;
  mode: 'source' | 'sink' | 'both';
  status: 'ready' | 'planned';
  notes: string[];
}

export interface ConnectorRecord {
  id: string;
  tenantId: string;
  name: string;
  capabilityId: string;
  executionMode: ExecutionMode;
  config: Record<string, unknown>;
  createdAt: string;
}

export interface CapabilityData {
  native: CapabilityRecord[];
  adapter: CapabilityRecord[];
  sinkGuarantees: Array<{
    guarantee: SinkGuarantee;
    label: string;
    detail: string;
    retryPolicy: string;
  }>;
}

export interface DraftFlowResponse {
  draft: FlowSpec;
  validation: FlowSpecValidationResult;
  compilation: CompiledFlowSummary;
  simulation: SimulationReport;
  backendSpec?: FlowSpec;
}

export interface TransformComposerResponse {
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
    recommendedExportIds: string[];
  };
  preview: {
    accepted: boolean;
    output?: unknown;
    droppedReason?: string;
    notes: string[];
  };
  exportOptions: CapabilityRecord[];
  sourceBinding?: {
    sourceKind: 'http' | 'nats' | 'kafka';
    capabilityId: 'http_in' | 'nats_in' | 'kafka_in';
    executionMode: ExecutionMode;
    connectorId: string;
    connectorName: string;
    ref: string;
    config: Record<string, unknown>;
    generated: boolean;
  };
  draft?: DraftFlowResponse;
}

export interface AiProviderSettings {
  provider: 'gemini';
  enabled: boolean;
  model: string;
  apiBaseUrl: string;
  apiKeyConfigured: boolean;
  source: 'database' | 'environment' | 'unset';
  activeProvider: 'gemini' | 'heuristic';
  updatedAt: string | null;
}

export interface OidcSettings {
  enabled: boolean;
  loginRequired: boolean;
  issuerUrl?: string;
  clientId?: string;
  authorizationEndpoint?: string;
  scope?: string;
}

export interface RuntimeSampleRecord {
  deploymentId: string;
  flowId: string;
  flowName: string;
  revisionId: string;
  sourceKind: 'http' | 'nats' | 'kafka';
  sourceRef: string;
  payload: unknown;
  observedAt: string;
}
