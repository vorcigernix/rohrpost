import type { AdapterWorkItem } from "@rohrpost/control-api-contracts";
import type {
  CanonicalEnvelope,
  CompiledFlowSummary,
  FlowSpec,
  FlowSpecIssue,
  SinkNode,
} from "@rohrpost/shared-flow-spec";

export type RuntimeExecutionMode = "native" | "adapter";

export interface HttpSinkTarget {
  kind: "http";
  connectorId: string;
  url: string;
  method?: "POST" | "PUT" | "PATCH";
  headers?: Record<string, string>;
  timeoutMs?: number;
}

export interface NatsSinkTarget {
  kind: "nats";
  connectorId: string;
  subject: string;
  headers?: Record<string, string>;
}

export interface AdapterSinkTarget {
  kind: "adapter";
  connectorId: string;
  capabilityId: string;
  workStream: "work";
}

export type SinkTarget = HttpSinkTarget | NatsSinkTarget | AdapterSinkTarget;

export interface DeploymentTargetMap {
  sinks: Record<string, SinkTarget>;
}

export interface ResolvedConnector {
  id: string;
  capabilityId: string;
  executionMode: RuntimeExecutionMode;
  config: Record<string, unknown>;
}

export interface RouterDeployment {
  id: string;
  tenantId: string;
  flowId: string;
  revisionId: string;
  active: boolean;
  spec: FlowSpec;
  compiled: CompiledFlowSummary;
  sourceSubjects: string[];
  natsSourceSubjects: string[];
  httpSourcePaths?: string[];
  sinkTargets: Record<string, SinkTarget>;
  connectors: Record<string, ResolvedConnector>;
}

export interface ReplayRequest {
  deploymentId: string;
  envelope: CanonicalEnvelope;
  reason: string;
  requestedAt: string;
}

export interface DeliveryAttempt {
  sinkId: string;
  connectorId: string;
  executionMode: RuntimeExecutionMode;
  attempt: number;
  succeeded: boolean;
  startedAt: string;
  finishedAt: string;
  statusCode?: number;
  error?: string;
}

export interface RunRecord {
  runId: string;
  deploymentId: string;
  flowId: string;
  revisionId: string;
  messageId: string;
  traceId: string;
  sourceRef: string;
  status: "delivered" | "enqueued" | "retrying" | "dlq" | "failed" | "filtered" | "deduped";
  targetSinkIds: string[];
  awaitedSinkIds: string[];
  attempts: DeliveryAttempt[];
  receivedAt: string;
  finishedAt: string;
  reason?: string;
}

export interface DlqRecord {
  dlqId: string;
  deploymentId: string;
  flowId: string;
  revisionId: string;
  messageId: string;
  reason: string;
  envelope: CanonicalEnvelope;
  createdAt: string;
}

export interface RuntimeSummary {
  deployments: number;
  runs: number;
  dlq: number;
  activeSubjects: string[];
  mode: string;
  observability: {
    mode: string;
    controlPlane: string;
  };
  health: {
    ok: boolean;
    reasons: string[];
    warnings: string[];
    backlogCount: number;
    inflightCount: number;
    lastStatsFlushAt?: string;
    lastStatsFlushError?: string;
  };
  deploymentStats: RuntimeDeploymentStats[];
}

export interface RuntimeDeploymentStats {
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
  lastAcceptedAt?: string;
  lastProcessedAt?: string;
  lastError?: string;
  updatedAt: string;
  state: "healthy" | "backlogged" | "stalled" | "idle";
}

export interface DeliveryPayload {
  envelope: CanonicalEnvelope;
  payload: unknown;
  deploymentId: string;
  sinkId: string;
  flowId: string;
  revisionId: string;
}

export interface AdapterDeliveryPayload extends DeliveryPayload {
  connectorId: string;
  capabilityId: string;
  sourceRef: string;
}

export type AdapterWorkEnvelope = AdapterWorkItem;

export interface DeliveryResponse {
  ok: boolean;
  statusCode?: number;
  body?: string;
  error?: string;
}

export interface DeploymentSource {
  loadDeployments(): Promise<RouterDeployment[]>;
  getLoadErrors?(): DeploymentLoadError[];
}

export interface DeploymentLoadError {
  deploymentId?: string;
  tenantId?: string;
  flowId?: string;
  revisionId?: string;
  reason: string;
  issues?: FlowSpecIssue[];
}

export interface RouterWorkerRuntimeConfig {
  serviceName: string;
  httpHost: string;
  httpPort: number;
  controlApiUrl?: string;
  controlApiToken?: string;
  natsUrl?: string;
  pollIntervalMs: number;
  subscriptionConcurrency: number;
  httpTimeoutMs: number;
  retryBaseDelayMs: number;
  maxAttempts: number;
  runHistoryLimit: number;
  dlqHistoryLimit: number;
  metricsFlushIntervalMs: number;
  runtimeSampleCaptureIntervalMs: number;
  runtimeSampleMaxPayloadBytes: number;
  backlogWarningThreshold: number;
  ingressMaxBufferedPerDeployment: number;
  ingressMaxBufferedTotal: number;
  ingressRetryAfterMs: number;
  processingStallMs: number;
  replayStream: string;
  dlqStream: string;
  ingressStream: string;
}

export interface IngressAdmissionStatus {
  allowed: boolean;
  scope?: "deployment" | "global";
  reason?: "deployment_backpressure" | "global_backpressure";
  deploymentId: string;
  bufferedForDeployment: number;
  bufferedTotal: number;
  limitForDeployment: number;
  limitTotal: number;
  retryAfterMs: number;
}

export interface MessageBus {
  publish(subject: string, data: Uint8Array): Promise<void>;
  publishToJetStream(subject: string, data: Uint8Array): Promise<void>;
  subscribe(
    subject: string,
    handler: (data: Uint8Array, metadata: { subject: string }) => Promise<void>,
  ): Promise<{ unsubscribe(): Promise<void> }>;
  subscribeToJetStream(
    subject: string,
    handler: (data: Uint8Array, metadata: { subject: string; sequence?: number }) => Promise<void>,
    options: {
      durableName: string;
      concurrency?: number;
      partitionKey?: (data: Uint8Array) => string;
    },
  ): Promise<{ unsubscribe(): Promise<void> }>;
  close(): Promise<void>;
}

export interface NatsRuntime {
  bus: MessageBus;
  ready: Promise<void>;
}

export interface ProcessedMessage {
  deployment: RouterDeployment;
  run: RunRecord;
  dlqRecord?: DlqRecord;
}

export interface IngressEnvelopeInput {
  deploymentId: string;
  tenantId: string;
  flowId: string;
  revisionId: string;
  messageId: string;
  sourceRef: string;
  partitionKey: string;
  headers?: Record<string, string>;
  payload: unknown;
  receivedAt?: string;
  traceId?: string;
}

export type SinkDefinition = SinkNode;
