import {
  CANONICAL_ENVELOPE_FIELDS,
  CANONICAL_STREAMS,
  FLOW_SPEC_VERSION,
} from "./constants";
import type {
  BatchProcessorNode,
  BranchProcessorNode,
  CanonicalEnvelope,
  DlqPolicy,
  FlowSpec,
  FlowSpecIssue,
  FlowSpecValidationResult,
  IdempotencyStrategy,
  ProcessorNode,
  RouteRule,
  SinkNode,
} from "./types";

export class FlowSpecValidationError extends Error {
  constructor(public readonly issues: FlowSpecIssue[]) {
    super("FlowSpec validation failed");
  }
}

function issue(path: string, message: string): FlowSpecIssue {
  return { path, message };
}

function validateString(value: unknown, path: string, issues: FlowSpecIssue[]): value is string {
  if (typeof value !== "string" || value.trim().length === 0) {
    issues.push(issue(path, "must be a non-empty string"));
    return false;
  }

  return true;
}

function validateObject(value: unknown, path: string, issues: FlowSpecIssue[]): value is Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    issues.push(issue(path, "must be an object"));
    return false;
  }

  return true;
}

function validateEnvelopeShape(envelope: CanonicalEnvelope, issues: FlowSpecIssue[]): void {
  for (const field of CANONICAL_ENVELOPE_FIELDS) {
    if (!(field in envelope)) {
      issues.push(issue(`envelope.${field}`, "is required"));
    }
  }
}

function validateSink(sink: SinkNode, issues: FlowSpecIssue[], index: number): void {
  validateString(sink.id, `sinks[${index}].id`, issues);
  validateString(sink.kind, `sinks[${index}].kind`, issues);
  validateString(sink.connector.capabilityId, `sinks[${index}].connector.capabilityId`, issues);
  validateString(sink.connector.connectorId, `sinks[${index}].connector.connectorId`, issues);

  if (!["native", "adapter"].includes(sink.connector.executionMode)) {
    issues.push(issue(`sinks[${index}].connector.executionMode`, "must be native or adapter"));
  }

  if (!["idempotent", "append_only", "best_effort"].includes(sink.deliveryGuarantee)) {
    issues.push(issue(`sinks[${index}].deliveryGuarantee`, "must be idempotent, append_only, or best_effort"));
  }

  if (sink.stream !== undefined && !CANONICAL_STREAMS.includes(sink.stream)) {
    issues.push(issue(`sinks[${index}].stream`, "must be one of the canonical streams"));
  }
}

function validateProcessorBase(
  processor: ProcessorNode,
  issues: FlowSpecIssue[],
  index: number,
): void {
  validateString(processor.id, `processors[${index}].id`, issues);
  if (processor.connector) {
    validateString(processor.connector.capabilityId, `processors[${index}].connector.capabilityId`, issues);
    validateString(processor.connector.connectorId, `processors[${index}].connector.connectorId`, issues);
    if (!["native", "adapter"].includes(processor.connector.executionMode)) {
      issues.push(issue(`processors[${index}].connector.executionMode`, "must be native or adapter"));
    }
    if (processor.connector.executionMode === "adapter") {
      issues.push(issue(`processors[${index}].connector.executionMode`, "adapter processor execution is not supported yet"));
    }
  }
}

function validateRoute(route: RouteRule, issues: FlowSpecIssue[], index: number): void {
  validateString(route.id, `routes[${index}].id`, issues);
  validateString(route.fromNodeId, `routes[${index}].fromNodeId`, issues);
  if (!Array.isArray(route.toSinkIds) || route.toSinkIds.length === 0) {
    issues.push(issue(`routes[${index}].toSinkIds`, "must contain at least one sink id"));
  }
}

function validateRetryPolicy(retryPolicy: FlowSpec["retryPolicy"], issues: FlowSpecIssue[]): void {
  if (retryPolicy.maxAttempts < 1) {
    issues.push(issue("retryPolicy.maxAttempts", "must be at least 1"));
  }
  if (retryPolicy.initialBackoffMs < 0 || retryPolicy.maxBackoffMs < 0) {
    issues.push(issue("retryPolicy", "backoff values must be positive"));
  }
  if (retryPolicy.multiplier < 1) {
    issues.push(issue("retryPolicy.multiplier", "must be at least 1"));
  }
}

function validateDlqPolicy(dlqPolicy: DlqPolicy, issues: FlowSpecIssue[]): void {
  if (dlqPolicy.enabled && !dlqPolicy.sinkId) {
    issues.push(issue("dlqPolicy.sinkId", "is required when DLQ is enabled"));
  }
}

function validateBatchingPolicy(batchingPolicy: FlowSpec["batchingPolicy"], issues: FlowSpecIssue[]): void {
  if (!batchingPolicy?.enabled) {
    return;
  }

  if (batchingPolicy.batchSize < 2 || batchingPolicy.batchSize > 100) {
    issues.push(issue("batchingPolicy.batchSize", "enabled batching requires batchSize between 2 and 100"));
  }

  const flushIntervalMs = batchingPolicy.flushIntervalMs;
  if (flushIntervalMs !== undefined && (flushIntervalMs < 10 || flushIntervalMs > 5_000)) {
    issues.push(
      issue("batchingPolicy.flushIntervalMs", "enabled batching requires flushIntervalMs between 10 and 5000"),
    );
  }
}

function validateProcessorSpecific(
  processor: ProcessorNode,
  issues: FlowSpecIssue[],
  index: number,
): void {
  switch (processor.kind) {
    case "map":
      if (processor.mode && !["merge", "project"].includes(processor.mode)) {
        issues.push(issue(`processors[${index}].mode`, "must be merge or project"));
      }
      if (processor.mappings.length === 0) {
        issues.push(issue(`processors[${index}].mappings`, "must contain at least one mapping"));
      }
      break;
    case "filter":
      validateObject(processor.predicate, `processors[${index}].predicate`, issues);
      break;
    case "branch":
      if (processor.cases.length === 0) {
        issues.push(issue(`processors[${index}].cases`, "must contain at least one branch case"));
      }
      break;
    case "template":
      validateString(processor.template, `processors[${index}].template`, issues);
      break;
    case "redact":
      if (processor.paths.length === 0) {
        issues.push(issue(`processors[${index}].paths`, "must contain at least one path"));
      }
      break;
    case "enrich_static":
      validateObject(processor.values, `processors[${index}].values`, issues);
      break;
    case "enrich_lookup":
      validateString(processor.keyPath, `processors[${index}].keyPath`, issues);
      if (processor.targetPath !== undefined) {
        validateString(processor.targetPath, `processors[${index}].targetPath`, issues);
      }
      validateObject(processor.lookup, `processors[${index}].lookup`, issues);
      if (processor.lookup.mode !== "inline") {
        issues.push(issue(`processors[${index}].lookup.mode`, "must be inline"));
      }
      validateObject(processor.lookup.table, `processors[${index}].lookup.table`, issues);
      if (processor.lookup.missing !== undefined && !["skip", "null", "fail"].includes(processor.lookup.missing)) {
        issues.push(issue(`processors[${index}].lookup.missing`, "must be skip, null, or fail"));
      }
      break;
    case "batch":
      if (processor.size < 1) {
        issues.push(issue(`processors[${index}].size`, "must be at least 1"));
      }
      break;
    case "retry":
      if (processor.maxAttempts < 1) {
        issues.push(issue(`processors[${index}].maxAttempts`, "must be at least 1"));
      }
      break;
    case "rate_limit":
      if (processor.perSecond < 1) {
        issues.push(issue(`processors[${index}].perSecond`, "must be at least 1"));
      }
      break;
    case "dedupe_window":
      if (processor.windowMs < 1) {
        issues.push(issue(`processors[${index}].windowMs`, "must be at least 1"));
      }
      validateString(processor.keyPath, `processors[${index}].keyPath`, issues);
      break;
  }
}

function collectIds(spec: FlowSpec): Map<string, string> {
  const ids = new Map<string, string>();
  for (const source of spec.sources) ids.set(source.id, "source");
  for (const processor of spec.processors) ids.set(processor.id, "processor");
  for (const sink of spec.sinks) ids.set(sink.id, "sink");
  for (const route of spec.routes) ids.set(route.id, "route");
  return ids;
}

export function validateFlowSpec(spec: FlowSpec): FlowSpecValidationResult {
  const issues: FlowSpecIssue[] = [];

  if (spec.version !== FLOW_SPEC_VERSION) {
    issues.push(issue("version", "must be 1"));
  }

  validateString(spec.metadata.tenantId, "metadata.tenantId", issues);
  validateString(spec.metadata.flowId, "metadata.flowId", issues);
  validateString(spec.metadata.revisionId, "metadata.revisionId", issues);
  validateString(spec.metadata.name, "metadata.name", issues);

  if (spec.sources.length === 0) {
    issues.push(issue("sources", "must contain at least one source"));
  }
  if (spec.sinks.length === 0) {
    issues.push(issue("sinks", "must contain at least one sink"));
  }

  spec.sources.forEach((source, index) => {
    validateString(source.id, `sources[${index}].id`, issues);
    validateString(source.kind, `sources[${index}].kind`, issues);
    validateString(source.connector.capabilityId, `sources[${index}].connector.capabilityId`, issues);
    validateString(source.connector.connectorId, `sources[${index}].connector.connectorId`, issues);
    if (!["native", "adapter"].includes(source.connector.executionMode)) {
      issues.push(issue(`sources[${index}].connector.executionMode`, "must be native or adapter"));
    }
    if (!CANONICAL_STREAMS.includes(source.stream)) {
      issues.push(issue(`sources[${index}].stream`, "must be one of the canonical streams"));
    }
    if (source.nextNodeIds.length === 0) {
      issues.push(issue(`sources[${index}].nextNodeIds`, "must contain at least one node id"));
    }
  });

  spec.processors.forEach((processor, index) => {
    validateProcessorBase(processor, issues, index);
    if (processor.nextNodeIds.length === 0 && processor.kind !== "batch" && processor.kind !== "retry") {
      issues.push(issue(`processors[${index}].nextNodeIds`, "must contain at least one node id"));
    }
    validateProcessorSpecific(processor, issues, index);
  });

  spec.routes.forEach((route, index) => {
    validateRoute(route, issues, index);
  });

  spec.sinks.forEach((sink, index) => {
    validateSink(sink, issues, index);
  });

  validateRetryPolicy(spec.retryPolicy, issues);
  validateDlqPolicy(spec.dlqPolicy, issues);
  validateBatchingPolicy(spec.batchingPolicy, issues);

  const ids = collectIds(spec);
  const knownNodeIds = new Set(ids.keys());

  for (const source of spec.sources) {
    for (const nextNodeId of source.nextNodeIds) {
      if (!knownNodeIds.has(nextNodeId)) {
        issues.push(issue(`sources.${source.id}.nextNodeIds`, `references unknown node ${nextNodeId}`));
      }
    }
  }

  for (const processor of spec.processors) {
    for (const nextNodeId of processor.nextNodeIds) {
      if (!knownNodeIds.has(nextNodeId)) {
        issues.push(issue(`processors.${processor.id}.nextNodeIds`, `references unknown node ${nextNodeId}`));
      }
    }

    if (processor.kind === "branch") {
      const branch = processor as BranchProcessorNode;
      branch.cases.forEach((branchCase, caseIndex) => {
        if (branchCase.nextNodeIds.length === 0) {
          issues.push(issue(`processors.${processor.id}.cases[${caseIndex}].nextNodeIds`, "must contain at least one node id"));
        }
        branchCase.nextNodeIds.forEach((nextNodeId) => {
          if (!knownNodeIds.has(nextNodeId)) {
            issues.push(
              issue(
                `processors.${processor.id}.cases[${caseIndex}].nextNodeIds`,
                `references unknown node ${nextNodeId}`,
              ),
            );
          }
        });
      });
    }
  }

  for (const route of spec.routes) {
    if (!knownNodeIds.has(route.fromNodeId)) {
      issues.push(issue(`routes.${route.id}.fromNodeId`, `references unknown node ${route.fromNodeId}`));
    }
    for (const sinkId of route.toSinkIds) {
      if (!spec.sinks.some((sink) => sink.id === sinkId)) {
        issues.push(issue(`routes.${route.id}.toSinkIds`, `references unknown sink ${sinkId}`));
      }
    }
  }

  const nonIdempotentSinks = spec.sinks.filter((sink) => sink.deliveryGuarantee !== "idempotent");
  if (nonIdempotentSinks.length > 0 && spec.retryPolicy.maxAttempts > 1) {
    issues.push(
      issue(
        "retryPolicy.maxAttempts",
        "retries are only allowed for idempotent sinks because repeated writes are unsafe",
      ),
    );
  }

  if (spec.idempotencyStrategy === "none" && spec.retryPolicy.maxAttempts > 1) {
    issues.push(issue("idempotencyStrategy", "must not be none when retries are enabled"));
  }

  return {
    valid: issues.length === 0,
    issues,
  };
}

export function assertValidFlowSpec(spec: FlowSpec): FlowSpec {
  const result = validateFlowSpec(spec);
  if (!result.valid) {
    throw new FlowSpecValidationError(result.issues);
  }
  return spec;
}

export function isCanonicalEnvelope(value: unknown): value is CanonicalEnvelope {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  return CANONICAL_ENVELOPE_FIELDS.every((field) => field in value);
}

export function isValidIdempotencyStrategy(value: unknown): value is IdempotencyStrategy {
  return value === "message_id" || value === "partition_key" || value === "payload_hash" || value === "none";
}
