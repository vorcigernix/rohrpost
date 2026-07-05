import type { CanonicalStreamName } from "./constants";

export type ConnectorExecutionMode = "native" | "adapter";

export type ConnectorKind = "source" | "sink" | "processor";

export type SinkDeliveryGuarantee = "idempotent" | "append_only" | "best_effort";

export interface CanonicalEnvelope {
  tenantId: string;
  flowId: string;
  revisionId: string;
  messageId: string;
  sourceRef: string;
  partitionKey: string;
  headers: Record<string, string>;
  payload: unknown;
  receivedAt: string;
  traceId: string;
}

export interface ConnectorCapability {
  id: string;
  name: string;
  kind: ConnectorKind;
  executionMode: ConnectorExecutionMode;
  streamCompatibility: CanonicalStreamName[];
  deliveryGuarantees: SinkDeliveryGuarantee[];
  adapterManaged?: boolean;
}

export interface ConnectorRef {
  capabilityId: string;
  connectorId: string;
  executionMode: ConnectorExecutionMode;
}

export interface FlowSpecMetadata {
  tenantId: string;
  flowId: string;
  revisionId: string;
  name: string;
  description?: string;
  tags?: string[];
}

export interface FlowSpec {
  version: 1;
  metadata: FlowSpecMetadata;
  sources: SourceNode[];
  processors: ProcessorNode[];
  routes: RouteRule[];
  sinks: SinkNode[];
  retryPolicy: RetryPolicy;
  dlqPolicy: DlqPolicy;
  batchingPolicy?: BatchingPolicy;
  idempotencyStrategy: IdempotencyStrategy;
}

export interface SourceNode {
  id: string;
  kind: "http" | "nats" | "kafka" | "webhook" | "custom";
  connector: ConnectorRef;
  stream: CanonicalStreamName;
  nextNodeIds: string[];
}

export type ProcessorNode =
  | MapProcessorNode
  | FilterProcessorNode
  | BranchProcessorNode
  | TemplateProcessorNode
  | RedactProcessorNode
  | EnrichStaticProcessorNode
  | EnrichLookupProcessorNode
  | BatchProcessorNode
  | RetryProcessorNode
  | RateLimitProcessorNode
  | DedupeWindowProcessorNode;

export interface ProcessorBase {
  id: string;
  connector?: ConnectorRef;
  nextNodeIds: string[];
}

export interface MapProcessorNode extends ProcessorBase {
  kind: "map";
  mode?: "merge" | "project";
  mappings: Array<{
    from: string;
    to: string;
  }>;
}

export interface FilterProcessorNode extends ProcessorBase {
  kind: "filter";
  predicate: PredicateExpr;
}

export interface BranchProcessorNode extends ProcessorBase {
  kind: "branch";
  cases: Array<{
    id: string;
    predicate: PredicateExpr;
    nextNodeIds: string[];
  }>;
}

export interface TemplateProcessorNode extends ProcessorBase {
  kind: "template";
  template: string;
  targetPath?: string;
}

export interface RedactProcessorNode extends ProcessorBase {
  kind: "redact";
  paths: string[];
  mask?: string;
}

export interface EnrichStaticProcessorNode extends ProcessorBase {
  kind: "enrich_static";
  values: Record<string, unknown>;
}

export type EnrichLookupMissingBehavior = "skip" | "null" | "fail";

export interface EnrichLookupProcessorNode extends ProcessorBase {
  kind: "enrich_lookup";
  keyPath: string;
  targetPath?: string;
  lookup: {
    mode: "inline";
    table: Record<string, unknown>;
    missing?: EnrichLookupMissingBehavior;
  };
}

export interface BatchProcessorNode extends ProcessorBase {
  kind: "batch";
  size: number;
  timeoutMs?: number;
  keyPath?: string;
}

export interface RetryProcessorNode extends ProcessorBase {
  kind: "retry";
  maxAttempts: number;
  backoffMs: number[];
}

export interface RateLimitProcessorNode extends ProcessorBase {
  kind: "rate_limit";
  perSecond: number;
}

export interface DedupeWindowProcessorNode extends ProcessorBase {
  kind: "dedupe_window";
  keyPath: string;
  windowMs: number;
}

export type PredicateExpr =
  | { type: "always" }
  | { type: "field_exists"; path: string }
  | { type: "field_equals"; path: string; value: unknown }
  | { type: "field_contains"; path: string; value: string }
  | { type: "field_gt"; path: string; value: number }
  | { type: "field_gte"; path: string; value: number }
  | { type: "field_lt"; path: string; value: number }
  | { type: "field_lte"; path: string; value: number }
  | { type: "and"; all: PredicateExpr[] }
  | { type: "or"; any: PredicateExpr[] }
  | { type: "not"; predicate: PredicateExpr };

export interface RouteRule {
  id: string;
  fromNodeId: string;
  predicate: PredicateExpr;
  toSinkIds: string[];
  priority?: number;
}

export interface SinkNode {
  id: string;
  kind: "http" | "nats" | "snowflake" | "bigquery" | "s3" | "kafka" | "custom";
  connector: ConnectorRef;
  deliveryGuarantee: SinkDeliveryGuarantee;
  stream?: CanonicalStreamName;
}

export type IdempotencyStrategy = "message_id" | "partition_key" | "payload_hash" | "none";

export interface RetryPolicy {
  maxAttempts: number;
  initialBackoffMs: number;
  maxBackoffMs: number;
  multiplier: number;
  retryableStatusCodes?: number[];
}

export interface DlqPolicy {
  enabled: boolean;
  sinkId?: string;
  reasonFormat?: "plain" | "json";
}

export interface BatchingPolicy {
  enabled: boolean;
  batchSize: number;
  flushIntervalMs?: number;
  keyPath?: string;
}

export interface FlowSpecIssue {
  path: string;
  message: string;
}

export interface FlowSpecValidationResult {
  valid: boolean;
  issues: FlowSpecIssue[];
}

export interface CompiledFlowSummary {
  flowId: string;
  revisionId: string;
  name: string;
  sourceCount: number;
  processorCount: number;
  routeCount: number;
  sinkCount: number;
  nativeConnectorCount: number;
  adapterConnectorCount: number;
  deliveryGuarantees: Record<SinkDeliveryGuarantee, number>;
  processorKinds: Record<ProcessorNode["kind"], number>;
  warnings: string[];
}

export interface SimulationSample {
  envelope: Partial<CanonicalEnvelope>;
  payload: unknown;
  sourceId?: string;
}

export interface SimulationResultItem {
  sampleIndex: number;
  dropped: boolean;
  dropReason?: string;
  outputs: Array<{
    sinkId: string;
    payload: unknown;
    envelope: CanonicalEnvelope;
  }>;
  trace: string[];
}

export interface SimulationReport {
  accepted: number;
  dropped: number;
  outputs: number;
  items: SimulationResultItem[];
}
