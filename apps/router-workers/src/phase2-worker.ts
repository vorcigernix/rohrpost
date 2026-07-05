import {
  compileFlowSpec,
  evaluatePredicate,
  getPath,
  setPath,
  type CanonicalEnvelope,
  type FlowSpec,
  type ProcessorNode,
  type SinkNode,
} from "@rohrpost/shared-flow-spec";
import {
  buildAdapterWorkSubject,
  isConnectManagedAdapterCapability,
  type AdapterWorkItem,
} from "@rohrpost/control-api-contracts";
import type { PendingReplayRequest, RouterControlApiClient, RuntimeSampleInput } from "./control-api";
import {
  buildDlqSubject,
  buildJetStreamSubject,
  buildReplaySubject,
  buildRetrySubject,
} from "./jetstream";
import { deliverHttpSink, deliverNatsSink, sleep, toDeliveryAttempt } from "./delivery";
import { HttpSinkBatcher } from "./http-sink-batcher";
import { decodeBusMessage, decodeJsonMessage, encodeJsonMessage } from "./nats";
import type {
  DeliveryAttempt,
  DeploymentLoadError,
  DlqRecord,
  DeploymentSource,
  DeploymentTargetMap,
  IngressAdmissionStatus,
  IngressEnvelopeInput,
  MessageBus,
  ProcessedMessage,
  ReplayRequest,
  RouterDeployment,
  RuntimeDeploymentStats,
  RuntimeSummary,
  RouterWorkerRuntimeConfig,
  RunRecord,
  SinkTarget,
} from "./phase2-types";

const ATTEMPT_HEADER = "x-router-attempt";
const NOT_BEFORE_HEADER = "x-router-not-before";
// Cap how long a consumer will hold a not-yet-due retry. Must stay well
// below the consumer ack wait so held messages are not redelivered mid-hold.
const MAX_RETRY_HOLD_MS = 10_000;
// Bound for the per-deployment dedupe map before expired entries are pruned.
const DEDUPE_STATE_MAX_ENTRIES = 50_000;
// Bound for the per-deployment rate-limit bucket map before old buckets are pruned.
const RATE_STATE_MAX_ENTRIES = 10_000;
const jsonTextEncoder = new TextEncoder();

interface ExecutionTraceEntry {
  nodeId: string;
  kind: "source" | "processor" | "route" | "sink";
  note: string;
}

interface ExecutionContext {
  deployment: RouterDeployment;
  trace: ExecutionTraceEntry[];
  sinkDeliveries: Array<{ sinkId: string; payload: unknown }>;
  attemptedSinkIds: Set<string>;
  state: {
    dedupeWindow: Map<string, { expiresAt: number; messageId: string }>;
    rateLimit: Map<string, { bucketAt: number; count: number }>;
  };
}

interface ExecutionResult {
  payload: unknown;
  status: RunRecord["status"];
  reason?: string;
}

interface DeliveryOutcome {
  attempt: DeliveryAttempt;
  ok: boolean;
  error?: string;
  awaitedCompletion?: boolean;
}

type IngressAdmissionMode = "reject" | "wait";

export class IngressOverloadedError extends Error {
  public constructor(public readonly admission: IngressAdmissionStatus) {
    super(admission.reason ?? "ingress_backpressure");
    this.name = "IngressOverloadedError";
  }
}

function terminalCount(stats: RuntimeDeploymentStats): number {
  return stats.deliveredCount + stats.dlqCount + stats.failedCount + stats.filteredCount + stats.dedupedCount;
}

function createRuntimeStats(deployment: RouterDeployment, reporterId: string): RuntimeDeploymentStats {
  return {
    deploymentId: deployment.id,
    reporterId,
    flowId: deployment.flowId,
    revisionId: deployment.revisionId,
    acceptedCount: 0,
    processedCount: 0,
    deliveredCount: 0,
    retryingCount: 0,
    dlqCount: 0,
    failedCount: 0,
    filteredCount: 0,
    dedupedCount: 0,
    sinkAttemptCount: 0,
    sinkSuccessCount: 0,
    sinkFailureCount: 0,
    inflightCount: 0,
    backlogCount: 0,
    updatedAt: isoNow(),
    state: "idle",
  };
}

function isoNow(): string {
  return new Date().toISOString();
}

function cloneJson<T>(value: T): T {
  return structuredClone(value);
}

function stringifyUnknown(value: unknown): string {
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function currentAttempt(envelope: CanonicalEnvelope): number {
  const headerValue = Number(envelope.headers[ATTEMPT_HEADER] ?? "1");
  return Number.isInteger(headerValue) && headerValue > 0 ? headerValue : 1;
}

function nextAttemptEnvelope(envelope: CanonicalEnvelope): CanonicalEnvelope {
  return {
    ...envelope,
    headers: {
      ...envelope.headers,
      [ATTEMPT_HEADER]: String(currentAttempt(envelope) + 1),
    },
  };
}

function sinkTargetFor(deployment: RouterDeployment, sink: SinkNode): SinkTarget | undefined {
  return deployment.sinkTargets[sink.connector.connectorId] ?? deployment.sinkTargets[sink.id];
}

function connectorConfigFor(
  deployment: RouterDeployment,
  connectorId: string,
): Record<string, unknown> | undefined {
  return deployment.connectors[connectorId]?.config;
}

function createIngressEnvelope(input: IngressEnvelopeInput): CanonicalEnvelope {
  return {
    tenantId: input.tenantId,
    flowId: input.flowId,
    revisionId: input.revisionId,
    messageId: input.messageId,
    sourceRef: input.sourceRef,
    partitionKey: input.partitionKey,
    headers: input.headers ?? {},
    payload: input.payload,
    receivedAt: input.receivedAt ?? isoNow(),
    traceId: input.traceId ?? `${input.flowId}:${input.messageId}`,
  };
}

function resolveSourceKind(deployment: RouterDeployment, sourceRef: string): RuntimeSampleInput["sourceKind"] {
  if (deployment.natsSourceSubjects.includes(sourceRef)) {
    return "nats";
  }

  if (deployment.spec.sources.some((source) => source.kind === "kafka")) {
    return "kafka";
  }

  if (deployment.spec.sources.some((source) => source.kind === "nats")) {
    return "nats";
  }

  return "http";
}

function sanitizeRuntimeSamplePayload(payload: unknown, maxBytes: number): unknown | undefined {
  try {
    const serialized = JSON.stringify(payload);
    if (typeof serialized !== "string") {
      return undefined;
    }

    if (jsonTextEncoder.encode(serialized).length > maxBytes) {
      return undefined;
    }

    return JSON.parse(serialized) as unknown;
  } catch {
    return undefined;
  }
}

function buildJetStreamSubscriptionKey(subject: string): string {
  return `js:${subject}`;
}

function buildNatsSubscriptionKey(subject: string): string {
  return `nats:${subject}`;
}

function findDeploymentBySourceSubject(
  deployments: Iterable<RouterDeployment>,
  subject: string,
  sourceType: "jetstream" | "nats",
): RouterDeployment | undefined {
  let match: RouterDeployment | undefined;
  for (const deployment of deployments) {
    const subjects = sourceType === "jetstream" ? deployment.sourceSubjects : deployment.natsSourceSubjects;
    if (subjects.includes(subject)) {
      match = deployment;
    }
  }

  return match;
}

function isSubjectUsedByAnotherDeployment(
  deployments: Iterable<RouterDeployment>,
  subject: string,
  sourceType: "jetstream" | "nats",
  deploymentId: string,
): boolean {
  for (const deployment of deployments) {
    if (deployment.id === deploymentId) {
      continue;
    }

    const subjects = sourceType === "jetstream" ? deployment.sourceSubjects : deployment.natsSourceSubjects;
    if (subjects.includes(subject)) {
      return true;
    }
  }

  return false;
}

function buildRunRecord(
  runId: string,
  deployment: RouterDeployment,
  envelope: CanonicalEnvelope,
  status: RunRecord["status"],
  targetSinkIds: string[],
  awaitedSinkIds: string[],
  attempts: DeliveryAttempt[],
  reason?: string,
): RunRecord {
  return {
    runId,
    deploymentId: deployment.id,
    flowId: deployment.flowId,
    revisionId: deployment.revisionId,
    messageId: envelope.messageId,
    traceId: envelope.traceId,
    sourceRef: envelope.sourceRef,
    status,
    targetSinkIds,
    awaitedSinkIds,
    attempts,
    receivedAt: envelope.receivedAt,
    finishedAt: isoNow(),
    reason,
  };
}

function mapRunStatusForControlApi(status: RunRecord["status"]): "succeeded" | "enqueued" | "retrying" | "dlq" | "failed" {
  switch (status) {
    case "delivered":
    case "filtered":
    case "deduped":
      return "succeeded";
    case "enqueued":
      return "enqueued";
    case "retrying":
      return "retrying";
    case "dlq":
      return "dlq";
    case "failed":
      return "failed";
  }
}

function errorCountForRunStatus(status: RunRecord["status"]): number {
  switch (status) {
    case "retrying":
    case "dlq":
    case "failed":
      return 1;
    case "enqueued":
    case "delivered":
    case "filtered":
    case "deduped":
      return 0;
  }
}

function buildDlqRecord(
  deployment: RouterDeployment,
  envelope: CanonicalEnvelope,
  reason: string,
): DlqRecord {
  return {
    dlqId: `${envelope.messageId}:dlq`,
    deploymentId: deployment.id,
    flowId: deployment.flowId,
    revisionId: deployment.revisionId,
    messageId: envelope.messageId,
    reason,
    envelope: cloneJson(envelope),
    createdAt: isoNow(),
  };
}

function buildAdapterWorkItem(
  runId: string,
  deployment: RouterDeployment,
  sink: SinkNode,
  envelope: CanonicalEnvelope,
  payload: unknown,
): AdapterWorkItem {
  return {
    workId: `work:${envelope.messageId}:${sink.id}:${crypto.randomUUID().slice(0, 8)}`,
    runId,
    enqueuedAt: isoNow(),
    deploymentId: deployment.id,
    flowId: deployment.flowId,
    revisionId: deployment.revisionId,
    sinkId: sink.id,
    connectorId: sink.connector.connectorId,
    capabilityId: sink.connector.capabilityId,
    tenantId: deployment.tenantId,
    attempt: currentAttempt(envelope),
    sourceRef: envelope.sourceRef,
    traceId: envelope.traceId,
    messageId: envelope.messageId,
    connectorConfig: connectorConfigFor(deployment, sink.connector.connectorId),
    envelope: cloneJson(envelope),
    payload: cloneJson(payload),
  };
}

function applyMap(payload: unknown, processor: Extract<ProcessorNode, { kind: "map" }>): unknown {
  let nextPayload: unknown = processor.mode === "project" ? {} : payload;
  for (const mapping of processor.mappings) {
    const value = getPath(payload, mapping.from);
    if (value !== undefined) {
      nextPayload = setPath(nextPayload, mapping.to, value);
    }
  }
  return nextPayload;
}

function applyTemplate(payload: unknown, processor: Extract<ProcessorNode, { kind: "template" }>): unknown {
  const rendered = processor.template.replace(/\$\{([^}]+)\}/g, (_, path: string) => {
    const value = getPath(payload, path.trim());
    return value == null ? "" : stringifyUnknown(value);
  });

  if (processor.targetPath) {
    return setPath(payload, processor.targetPath, rendered);
  }

  if (typeof payload === "string") {
    return rendered;
  }

  return { rendered, payload };
}

function applyRedact(payload: unknown, processor: Extract<ProcessorNode, { kind: "redact" }>): unknown {
  let nextPayload = payload;
  for (const path of processor.paths) {
    nextPayload = setPath(nextPayload, path, processor.mask ?? "[redacted]");
  }
  return nextPayload;
}

function applyEnrichStatic(payload: unknown, processor: Extract<ProcessorNode, { kind: "enrich_static" }>): unknown {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    return { value: payload, ...processor.values };
  }

  return {
    ...(payload as Record<string, unknown>),
    ...processor.values,
  };
}

function writeEnrichment(payload: unknown, value: unknown, targetPath?: string): unknown {
  if (targetPath) {
    return setPath(payload, targetPath, value);
  }

  if (value && typeof value === "object" && !Array.isArray(value)) {
    if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
      return { value: payload, ...(value as Record<string, unknown>) };
    }
    return {
      ...(payload as Record<string, unknown>),
      ...(value as Record<string, unknown>),
    };
  }

  return setPath(payload, "enrichment", value);
}

function applyEnrichLookup(
  payload: unknown,
  processor: Extract<ProcessorNode, { kind: "enrich_lookup" }>,
): { payload: unknown; found: boolean } {
  const key = getPath(payload, processor.keyPath);
  const tableKey = key == null ? null : String(key);
  const found = tableKey !== null && Object.prototype.hasOwnProperty.call(processor.lookup.table, tableKey);

  if (found && tableKey !== null) {
    return {
      payload: writeEnrichment(payload, processor.lookup.table[tableKey], processor.targetPath),
      found,
    };
  }

  if ((processor.lookup.missing ?? "skip") === "null") {
    return {
      payload: writeEnrichment(payload, null, processor.targetPath),
      found,
    };
  }

  return { payload, found };
}

function isNodeProcessor(
  spec: FlowSpec,
  nodeId: string,
): ProcessorNode | undefined {
  return spec.processors.find((processor) => processor.id === nodeId);
}

function routeSinkIdsForNode(
  deployment: RouterDeployment,
  nodeId: string,
  payload: unknown,
): string[] {
  return deployment.spec.routes
    .filter((route) => route.fromNodeId === nodeId)
    .sort((left, right) => (right.priority ?? 0) - (left.priority ?? 0))
    .filter((route) => evaluatePredicate(route.predicate, payload))
    .flatMap((route) => route.toSinkIds);
}

function executeDeployment(
  deployment: RouterDeployment,
  envelope: CanonicalEnvelope,
  context: ExecutionContext,
): ExecutionResult {
  const source = deployment.spec.sources.find((entry) => entry.id === deployment.spec.sources[0]?.id);
  if (!source) {
    return { payload: envelope.payload, status: "failed", reason: "missing source" };
  }

  context.trace.push({ nodeId: source.id, kind: "source", note: `source ${source.kind} accepted` });

  const walkNode = (nodeId: string, currentPayload: unknown): ExecutionResult => {
    const processor = isNodeProcessor(deployment.spec, nodeId);

    if (!processor) {
      const sinkIds = routeSinkIdsForNode(deployment, nodeId, currentPayload);
      for (const sinkId of sinkIds) {
        context.sinkDeliveries.push({ sinkId, payload: currentPayload });
      }
      if (sinkIds.length > 0) {
        context.trace.push({ nodeId, kind: "route", note: `routed to sinks [${sinkIds.join(", ")}]` });
      }
      return { payload: currentPayload, status: "delivered" };
    }

    context.trace.push({ nodeId: processor.id, kind: "processor", note: processor.kind });

    let nextPayload = currentPayload;
    let matchedNextIds: string[] = processor.nextNodeIds ?? [];

    switch (processor.kind) {
      case "map":
        nextPayload = applyMap(nextPayload, processor);
        break;
      case "filter":
        if (!evaluatePredicate(processor.predicate, nextPayload)) {
          return { payload: nextPayload, status: "filtered", reason: `filtered by ${processor.id}` };
        }
        break;
      case "branch": {
        const matchedCase = processor.cases.find((branchCase) => evaluatePredicate(branchCase.predicate, nextPayload));
        if (!matchedCase) {
          return { payload: nextPayload, status: "filtered", reason: `no branch case matched for ${processor.id}` };
        }
        matchedNextIds = matchedCase.nextNodeIds;
        break;
      }
      case "template":
        nextPayload = applyTemplate(nextPayload, processor);
        break;
      case "redact":
        nextPayload = applyRedact(nextPayload, processor);
        break;
      case "enrich_static":
        nextPayload = applyEnrichStatic(nextPayload, processor);
        break;
      case "enrich_lookup": {
        const result = applyEnrichLookup(nextPayload, processor);
        if (!result.found && processor.lookup.missing === "fail") {
          return { payload: nextPayload, status: "failed", reason: `lookup miss for ${processor.id}` };
        }
        nextPayload = result.payload;
        context.trace.push({
          nodeId: processor.id,
          kind: "processor",
          note: result.found ? `lookup hit on ${processor.keyPath}` : `lookup miss on ${processor.keyPath}`,
        });
        break;
      }
      case "batch":
        nextPayload = Array.isArray(nextPayload) ? nextPayload : [nextPayload];
        break;
      case "retry":
        break;
      case "rate_limit": {
        // Attempts beyond the first were already admitted by the limiter on
        // their original execution; retries must not re-count or be
        // re-limited, otherwise a saturated bucket turns transient sink
        // failures into permanent drops.
        if (currentAttempt(envelope) > 1) {
          break;
        }
        const windowBucket = Math.floor(Date.parse(envelope.receivedAt) / 1000);
        if (!Number.isFinite(windowBucket)) {
          break;
        }
        const bucketKey = `${processor.id}:${windowBucket}`;
        const current = context.state.rateLimit.get(bucketKey);
        if (!current) {
          context.state.rateLimit.set(bucketKey, { bucketAt: windowBucket, count: 1 });
          if (context.state.rateLimit.size > RATE_STATE_MAX_ENTRIES) {
            for (const [key, entry] of context.state.rateLimit) {
              if (entry.bucketAt < windowBucket - 1) {
                context.state.rateLimit.delete(key);
              }
            }
          }
        } else if (current.count >= processor.perSecond) {
          return { payload: nextPayload, status: "failed", reason: `rate limited by ${processor.id}` };
        } else {
          current.count += 1;
        }
        break;
      }
      case "dedupe_window": {
        const dedupeKeyValue = getPath(nextPayload, processor.keyPath);
        const dedupeKey = `${processor.id}:${JSON.stringify(dedupeKeyValue)}`;
        const currentTime = Date.parse(envelope.receivedAt);
        if (!Number.isFinite(currentTime)) {
          break;
        }
        const existing = context.state.dedupeWindow.get(dedupeKey);
        // Re-executions of the same message (retries, replays) pass through;
        // only genuinely different messages within the window are duplicates.
        if (existing && existing.messageId !== envelope.messageId && currentTime < existing.expiresAt) {
          return { payload: nextPayload, status: "deduped", reason: `duplicate within dedupe window for ${processor.id}` };
        }
        if (!existing || existing.messageId !== envelope.messageId) {
          context.state.dedupeWindow.set(dedupeKey, {
            expiresAt: currentTime + processor.windowMs,
            messageId: envelope.messageId,
          });
        }
        if (context.state.dedupeWindow.size > DEDUPE_STATE_MAX_ENTRIES) {
          for (const [key, entry] of context.state.dedupeWindow) {
            if (currentTime >= entry.expiresAt) {
              context.state.dedupeWindow.delete(key);
            }
          }
          let overflow = context.state.dedupeWindow.size - DEDUPE_STATE_MAX_ENTRIES;
          for (const key of context.state.dedupeWindow.keys()) {
            if (overflow <= 0) {
              break;
            }
            context.state.dedupeWindow.delete(key);
            overflow -= 1;
          }
        }
        break;
      }
    }

    const sinkIds = routeSinkIdsForNode(deployment, nodeId, nextPayload);
    for (const sinkId of sinkIds) {
      context.sinkDeliveries.push({ sinkId, payload: nextPayload });
    }
    if (sinkIds.length > 0) {
      context.trace.push({ nodeId, kind: "route", note: `routed to sinks [${sinkIds.join(", ")}]` });
    }

    let finalStatus: ExecutionResult["status"] = "delivered";
    let finalReason: string | undefined;

    for (const nextId of matchedNextIds) {
      const child = walkNode(nextId, nextPayload);
      if (child.status !== "delivered") {
        finalStatus = child.status;
        finalReason = child.reason;
      }
    }

    return {
      payload: nextPayload,
      status: finalStatus,
      reason: finalReason,
    };
  };

  let result: ExecutionResult = { payload: envelope.payload, status: "delivered" };
  for (const nextNodeId of source.nextNodeIds) {
    const child = walkNode(nextNodeId, result.payload);
    result = child;
    if (child.status !== "delivered") {
      break;
    }
  }

  return result;
}

async function deliverSinkOnce(
  runId: string,
  deployment: RouterDeployment,
  sink: SinkNode,
  payload: unknown,
  envelope: CanonicalEnvelope,
  bus: MessageBus | undefined,
  fetchImpl: typeof fetch,
  attemptNumber: number,
  httpBatcher?: HttpSinkBatcher,
): Promise<DeliveryOutcome> {
  const target = sinkTargetFor(deployment, sink);
  const startedAt = isoNow();

  if (sink.connector.executionMode === "adapter") {
    if (!bus) {
      const finishedAt = isoNow();
      return {
        ok: false,
        error: `no message bus configured for adapter sink ${sink.id}`,
        attempt: toDeliveryAttempt(
          sink.id,
          sink.connector.connectorId,
          sink.connector.executionMode,
          attemptNumber,
          startedAt,
          finishedAt,
          { ok: false, error: `no message bus configured for adapter sink ${sink.id}` },
        ),
      };
    }

    const deliveryMode = isConnectManagedAdapterCapability(sink.connector.capabilityId)
      ? "connect"
      : "inline";
    const workItem = buildAdapterWorkItem(runId, deployment, sink, envelope, payload);
    const subject = buildAdapterWorkSubject({
      deliveryMode,
      connectorId: sink.connector.connectorId,
      tenantId: deployment.tenantId,
      flowId: deployment.flowId,
      revisionId: deployment.revisionId,
      workId: workItem.workId,
    });

    try {
      await bus.publishToJetStream(subject, encodeJsonMessage(workItem));
      const finishedAt = isoNow();
      return {
        ok: true,
        awaitedCompletion: deliveryMode === "inline",
        attempt: toDeliveryAttempt(
          sink.id,
          sink.connector.connectorId,
          sink.connector.executionMode,
          attemptNumber,
          startedAt,
          finishedAt,
          { ok: true, body: subject },
        ),
      };
    } catch (error) {
      const finishedAt = isoNow();
      return {
        ok: false,
        error: error instanceof Error ? error.message : String(error),
        attempt: toDeliveryAttempt(
          sink.id,
          sink.connector.connectorId,
          sink.connector.executionMode,
          attemptNumber,
          startedAt,
          finishedAt,
          { ok: false, error: error instanceof Error ? error.message : String(error) },
        ),
      };
    }
  }

  if (!target) {
    const finishedAt = isoNow();
    return {
      ok: false,
      error: `missing target for sink ${sink.id}`,
      attempt: toDeliveryAttempt(
        sink.id,
        sink.connector.connectorId,
        sink.connector.executionMode,
        attemptNumber,
        startedAt,
        finishedAt,
        { ok: false, error: `missing target for sink ${sink.id}` },
      ),
    };
  }

  const response =
    target.kind === "http"
      ? httpBatcher
        ? await httpBatcher.enqueue({
            envelope,
            payload,
            deploymentId: deployment.id,
            sinkId: sink.id,
            flowId: deployment.flowId,
            revisionId: deployment.revisionId,
          })
        : await deliverHttpSink(
            target,
            {
              envelope,
              payload,
              deploymentId: deployment.id,
              sinkId: sink.id,
              flowId: deployment.flowId,
              revisionId: deployment.revisionId,
            },
            fetchImpl,
          )
      : target.kind === "nats"
        ? bus
          ? await deliverNatsSink(bus, target, {
              envelope,
              payload,
              deploymentId: deployment.id,
              sinkId: sink.id,
              flowId: deployment.flowId,
              revisionId: deployment.revisionId,
            })
          : { ok: false, error: `no message bus configured for sink ${sink.id}` }
        : { ok: false, error: `unsupported native sink target ${target.kind}` };

  const finishedAt = isoNow();
  return {
    ok: response.ok,
    error: response.error,
    attempt: toDeliveryAttempt(
      sink.id,
      target.connectorId,
      sink.connector.executionMode,
      attemptNumber,
      startedAt,
      finishedAt,
      response,
    ),
  };
}

function canRetrySink(
  deployment: RouterDeployment,
  sink: SinkNode,
  envelope: CanonicalEnvelope,
  config: RouterWorkerRuntimeConfig,
): boolean {
  const maxAttempts = Math.min(config.maxAttempts, deployment.spec.retryPolicy.maxAttempts);
  return sink.deliveryGuarantee === "idempotent" && currentAttempt(envelope) < maxAttempts;
}

class BoundedHistory<T> {
  private readonly values: Array<T | undefined>;
  private writeIndex = 0;
  private count = 0;

  public constructor(private readonly limit: number) {
    this.values = Array.from({ length: Math.max(1, limit) });
  }

  public push(value: T): void {
    if (this.limit <= 0) {
      return;
    }

    this.values[this.writeIndex] = value;
    this.writeIndex = (this.writeIndex + 1) % this.limit;
    this.count = Math.min(this.count + 1, this.limit);
  }

  public toArray(): T[] {
    if (this.count === 0) {
      return [];
    }

    const result: T[] = [];
    const start = this.count === this.limit ? this.writeIndex : 0;

    for (let index = 0; index < this.count; index += 1) {
      const value = this.values[(start + index) % this.limit];
      if (value !== undefined) {
        result.push(value);
      }
    }

    return result;
  }
}

export class RouterWorkerRuntime {
  private readonly deployments = new Map<string, RouterDeployment>();
  private readonly subscriptions = new Map<string, { unsubscribe(): Promise<void> }>();
  private readonly runRecords: BoundedHistory<RunRecord>;
  private readonly dlqRecords: BoundedHistory<DlqRecord>;
  private readonly deploymentStats = new Map<string, RuntimeDeploymentStats>();
  private readonly dirtyDeploymentStats = new Set<string>();
  private readonly processorState = new Map<
    string,
    {
      dedupeWindow: Map<string, { expiresAt: number; messageId: string }>;
      rateLimit: Map<string, { bucketAt: number; count: number }>;
    }
  >();
  private readonly httpSinkBatchers = new Map<string, HttpSinkBatcher>();
  private readonly deploymentTargetMap: DeploymentTargetMap;
  private pollingTimer: ReturnType<typeof setInterval> | undefined;
  private replayTimer: ReturnType<typeof setInterval> | undefined;
  private metricsFlushTimer: ReturnType<typeof setInterval> | undefined;
  private readonly controlApiClient?: RouterControlApiClient;
  private messageBus: MessageBus | undefined;
  private readonly invalidDeployments = new Map<string, string>();
  private readonly lastRuntimeSampleAt = new Map<string, number>();
  private readonly pendingIngressReservations = new Map<string, number>();
  private readonly admissionWaiters = new Set<() => void>();
  private totalPendingIngressReservations = 0;
  private totalRunCount = 0;
  private totalDlqCount = 0;
  private statsFlushInFlight: Promise<void> | undefined;
  private lastStatsFlushAt: string | undefined;
  private lastStatsFlushError: string | undefined;

  public constructor(
    private readonly config: RouterWorkerRuntimeConfig,
    private readonly deploymentSource: DeploymentSource,
    options: {
      deploymentTargetMap?: DeploymentTargetMap;
      messageBus?: MessageBus;
      fetchImpl?: typeof fetch;
      controlApiClient?: RouterControlApiClient;
    } = {},
  ) {
    this.deploymentTargetMap = options.deploymentTargetMap ?? { sinks: {} };
    this.messageBus = options.messageBus;
    this.fetchImpl = options.fetchImpl ?? fetch;
    this.controlApiClient = options.controlApiClient;
    this.runRecords = new BoundedHistory(this.config.runHistoryLimit);
    this.dlqRecords = new BoundedHistory(this.config.dlqHistoryLimit);
  }

  private readonly fetchImpl: typeof fetch;

  private ensureDeploymentStats(deployment: RouterDeployment): RuntimeDeploymentStats {
    const existing = this.deploymentStats.get(deployment.id);
    if (existing) {
      return existing;
    }

    const created = createRuntimeStats(deployment, this.config.serviceName);
    this.deploymentStats.set(deployment.id, created);
    return created;
  }

  private bufferedCountForDeployment(deploymentId: string): number {
    return (
      (this.deploymentStats.get(deploymentId)?.backlogCount ?? 0) +
      (this.pendingIngressReservations.get(deploymentId) ?? 0)
    );
  }

  private bufferedCountTotal(): number {
    const statsBuffered = [...this.deploymentStats.values()].reduce((sum, stats) => sum + stats.backlogCount, 0);
    return statsBuffered + this.totalPendingIngressReservations;
  }

  private buildIngressAdmissionStatus(deploymentId: string): IngressAdmissionStatus {
    const bufferedForDeployment = this.bufferedCountForDeployment(deploymentId);
    const bufferedTotal = this.bufferedCountTotal();
    const limitForDeployment = this.config.ingressMaxBufferedPerDeployment;
    const limitTotal = this.config.ingressMaxBufferedTotal;
    const deploymentBlocked = bufferedForDeployment >= limitForDeployment;
    const globalBlocked = bufferedTotal >= limitTotal;

    return {
      allowed: !deploymentBlocked && !globalBlocked,
      scope: deploymentBlocked ? "deployment" : globalBlocked ? "global" : undefined,
      reason: deploymentBlocked
        ? "deployment_backpressure"
        : globalBlocked
          ? "global_backpressure"
          : undefined,
      deploymentId,
      bufferedForDeployment,
      bufferedTotal,
      limitForDeployment,
      limitTotal,
      retryAfterMs: this.config.ingressRetryAfterMs,
    };
  }

  private signalAdmissionCapacityChanged(): void {
    for (const resolve of this.admissionWaiters) {
      resolve();
    }
    this.admissionWaiters.clear();
  }

  private waitForAdmissionCapacitySignal(): Promise<void> {
    return new Promise((resolve) => {
      this.admissionWaiters.add(resolve);
    });
  }

  private async reserveIngressCapacity(
    deploymentId: string,
    mode: IngressAdmissionMode,
  ): Promise<{ release(): void }> {
    for (;;) {
      const admission = this.buildIngressAdmissionStatus(deploymentId);
      if (admission.allowed) {
        const nextDeploymentReservations = (this.pendingIngressReservations.get(deploymentId) ?? 0) + 1;
        this.pendingIngressReservations.set(deploymentId, nextDeploymentReservations);
        this.totalPendingIngressReservations += 1;

        return {
          release: () => {
            const currentDeploymentReservations = this.pendingIngressReservations.get(deploymentId) ?? 0;
            if (currentDeploymentReservations <= 1) {
              this.pendingIngressReservations.delete(deploymentId);
            } else {
              this.pendingIngressReservations.set(deploymentId, currentDeploymentReservations - 1);
            }

            this.totalPendingIngressReservations = Math.max(0, this.totalPendingIngressReservations - 1);
            this.signalAdmissionCapacityChanged();
          },
        };
      }

      if (mode === "reject") {
        throw new IngressOverloadedError(admission);
      }

      await this.waitForAdmissionCapacitySignal();
    }
  }

  private refreshStatsState(stats: RuntimeDeploymentStats): void {
    const stalled =
      stats.backlogCount > 0 &&
      (!stats.lastProcessedAt || (Date.now() - Date.parse(stats.lastProcessedAt)) >= this.config.processingStallMs);

    if (stats.acceptedCount === 0 && stats.processedCount === 0) {
      stats.state = "idle";
      return;
    }

    if (stalled) {
      stats.state = "stalled";
      return;
    }

    if (
      stats.backlogCount >= this.config.backlogWarningThreshold ||
      stats.inflightCount > 0 ||
      stats.retryingCount > 0 ||
      stats.backlogCount > 0
    ) {
      stats.state = "backlogged";
      return;
    }

    stats.state = "healthy";
  }

  private markDeploymentStatsDirty(stats: RuntimeDeploymentStats): void {
    stats.updatedAt = isoNow();
    stats.backlogCount = Math.max(0, stats.acceptedCount - terminalCount(stats));
    this.refreshStatsState(stats);
    this.dirtyDeploymentStats.add(stats.deploymentId);
    this.signalAdmissionCapacityChanged();
  }

  private noteAccepted(deployment: RouterDeployment, acceptedAt: string): void {
    const stats = this.ensureDeploymentStats(deployment);
    stats.acceptedCount += 1;
    stats.lastAcceptedAt = acceptedAt;
    this.markDeploymentStatsDirty(stats);
  }

  private maybeCaptureRuntimeSample(deployment: RouterDeployment, envelope: CanonicalEnvelope): void {
    if (!this.controlApiClient || this.config.runtimeSampleCaptureIntervalMs <= 0) {
      return;
    }

    const sourceKind = resolveSourceKind(deployment, envelope.sourceRef);
    const sampleKey = `${deployment.id}:${sourceKind}:${envelope.sourceRef}`;
    const now = Date.now();
    const lastCapturedAt = this.lastRuntimeSampleAt.get(sampleKey);
    if (
      typeof lastCapturedAt === "number" &&
      now - lastCapturedAt < this.config.runtimeSampleCaptureIntervalMs
    ) {
      return;
    }

    const payload = sanitizeRuntimeSamplePayload(
      envelope.payload,
      this.config.runtimeSampleMaxPayloadBytes,
    );
    if (payload === undefined) {
      return;
    }

    this.lastRuntimeSampleAt.set(sampleKey, now);
    void this.controlApiClient.appendRuntimeSample({
      deploymentId: deployment.id,
      flowId: deployment.flowId,
      revisionId: deployment.revisionId,
      sourceKind,
      sourceRef: envelope.sourceRef,
      payload,
      observedAt: envelope.receivedAt,
    }).catch(() => undefined);
  }

  private noteInflight(deployment: RouterDeployment, delta: number): void {
    const stats = this.ensureDeploymentStats(deployment);
    stats.inflightCount = Math.max(0, stats.inflightCount + delta);
    this.markDeploymentStatsDirty(stats);
  }

  private noteRun(deployment: RouterDeployment, run: RunRecord): void {
    const stats = this.ensureDeploymentStats(deployment);
    const sinkAttemptCount = run.attempts.length;
    const sinkSuccessCount = run.attempts.filter((attempt) => attempt.succeeded).length;
    const sinkFailureCount = sinkAttemptCount - sinkSuccessCount;

    stats.processedCount += 1;
    stats.sinkAttemptCount += sinkAttemptCount;
    stats.sinkSuccessCount += sinkSuccessCount;
    stats.sinkFailureCount += sinkFailureCount;
    stats.inflightCount = Math.max(0, stats.inflightCount - 1);
    stats.lastProcessedAt = run.finishedAt;
    stats.lastError = run.reason ?? stats.lastError;

    switch (run.status) {
      case "delivered":
      case "enqueued":
        stats.deliveredCount += 1;
        break;
      case "retrying":
        stats.retryingCount += 1;
        break;
      case "dlq":
        stats.dlqCount += 1;
        break;
      case "filtered":
        stats.filteredCount += 1;
        break;
      case "deduped":
        stats.dedupedCount += 1;
        break;
      default:
        stats.failedCount += 1;
        break;
    }

    this.markDeploymentStatsDirty(stats);
  }

  private getDeploymentStats(): RuntimeDeploymentStats[] {
    return [...this.deploymentStats.values()]
      .map((stats) => {
        this.refreshStatsState(stats);
        return { ...stats };
      })
      .sort((left, right) => Date.parse(right.updatedAt) - Date.parse(left.updatedAt));
  }

  private buildHealthSummary(): RuntimeSummary["health"] {
    const deploymentStats = this.getDeploymentStats();
    const reasons: string[] = [];
    const warnings: string[] = [];
    const backlogCount = deploymentStats.reduce((sum, stats) => sum + stats.backlogCount, 0);
    const inflightCount = deploymentStats.reduce((sum, stats) => sum + stats.inflightCount, 0);
    const stalledDeployments = deploymentStats.filter((stats) => stats.state === "stalled");
    const backloggedDeployments = deploymentStats.filter((stats) => stats.state === "backlogged");

    if (this.lastStatsFlushError) {
      reasons.push(`runtime stats flush failed: ${this.lastStatsFlushError}`);
    }

    if (stalledDeployments.length > 0) {
      reasons.push(
        `processing stalled for ${stalledDeployments.length} deployment${stalledDeployments.length === 1 ? "" : "s"}`,
      );
    }

    if (backloggedDeployments.length > 0) {
      warnings.push(
        `${backloggedDeployments.length} deployment${backloggedDeployments.length === 1 ? "" : "s"} currently carry backlog`,
      );
    }

    return {
      ok: reasons.length === 0,
      reasons,
      warnings,
      backlogCount,
      inflightCount,
      lastStatsFlushAt: this.lastStatsFlushAt,
      lastStatsFlushError: this.lastStatsFlushError,
    };
  }

  private async flushRuntimeStats(force = false): Promise<void> {
    if (!this.controlApiClient) {
      return;
    }

    if (this.statsFlushInFlight) {
      return this.statsFlushInFlight;
    }

    const deploymentIds = force
      ? [...this.deploymentStats.keys()]
      : [...this.dirtyDeploymentStats.values()];
    if (deploymentIds.length === 0) {
      return;
    }

    for (const deploymentId of deploymentIds) {
      this.dirtyDeploymentStats.delete(deploymentId);
    }

    const payload = deploymentIds
      .map((deploymentId) => this.deploymentStats.get(deploymentId))
      .filter((stats): stats is RuntimeDeploymentStats => Boolean(stats))
      .map((stats) => {
        this.refreshStatsState(stats);
        return { ...stats };
      });

    this.statsFlushInFlight = (async () => {
      try {
        await this.controlApiClient?.reportRuntimeStats(payload);
        this.lastStatsFlushAt = isoNow();
        this.lastStatsFlushError = undefined;
      } catch (error) {
        for (const deploymentId of deploymentIds) {
          this.dirtyDeploymentStats.add(deploymentId);
        }
        this.lastStatsFlushError = error instanceof Error ? error.message : String(error);
      }
    })().finally(() => {
      this.statsFlushInFlight = undefined;
    });

    return this.statsFlushInFlight;
  }

  private invalidDeploymentKey(loadError: DeploymentLoadError): string {
    return loadError.deploymentId ?? `${loadError.flowId ?? "unknown"}:${loadError.revisionId ?? "latest"}`;
  }

  private async reportInvalidDeployment(loadError: DeploymentLoadError): Promise<void> {
    const key = this.invalidDeploymentKey(loadError);
    const nextReason = [
      loadError.reason,
      ...(loadError.issues?.map((issue) => `${issue.path}: ${issue.message}`) ?? []),
    ].join("; ");
    const previousReason = this.invalidDeployments.get(key);
    this.invalidDeployments.set(key, nextReason);

    if (!this.controlApiClient) {
      return;
    }

    if (previousReason === nextReason || !loadError.tenantId || !loadError.deploymentId) {
      return;
    }

    await this.controlApiClient.appendAudit({
      tenantId: loadError.tenantId,
      actor: this.config.serviceName,
      action: "deployment.validation_failed",
      subjectType: "deployment",
      subjectId: loadError.deploymentId,
      details: {
        flowId: loadError.flowId,
        revisionId: loadError.revisionId,
        reason: loadError.reason,
        issues: loadError.issues ?? [],
      },
    });
  }

  public async syncDeployments(): Promise<void> {
    const deployments = await this.deploymentSource.loadDeployments();
    const loadErrors = this.deploymentSource.getLoadErrors?.() ?? [];
    const nextDeploymentIds = new Set(deployments.map((deployment) => deployment.id));
    const nextInvalidKeys = new Set(loadErrors.map((loadError) => this.invalidDeploymentKey(loadError)));

    for (const deployment of deployments) {
      const mergedDeployment = {
        ...deployment,
        sinkTargets: {
          ...deployment.sinkTargets,
          ...this.deploymentTargetMap.sinks,
        },
      };

      this.deployments.set(deployment.id, mergedDeployment);
      await this.ensureSubscriptions(mergedDeployment);
      this.invalidDeployments.delete(deployment.id);
    }

    for (const loadError of loadErrors) {
      await this.reportInvalidDeployment(loadError);
    }

    for (const key of [...this.invalidDeployments.keys()]) {
      if (!nextInvalidKeys.has(key)) {
        this.invalidDeployments.delete(key);
      }
    }

    for (const [deploymentId, deployment] of this.deployments) {
      if (!nextDeploymentIds.has(deploymentId)) {
        await this.dropSubscriptionsForDeployment(deployment);
        this.deployments.delete(deploymentId);
        this.deploymentStats.delete(deploymentId);
        this.processorState.delete(deploymentId);
        for (const key of [...this.httpSinkBatchers.keys()]) {
          if (key.startsWith(`${deploymentId}:`)) {
            const batcher = this.httpSinkBatchers.get(key);
            this.httpSinkBatchers.delete(key);
            void batcher?.flush().catch(() => undefined);
          }
        }
        this.dirtyDeploymentStats.delete(deploymentId);
        const reserved = this.pendingIngressReservations.get(deploymentId) ?? 0;
        this.pendingIngressReservations.delete(deploymentId);
        this.totalPendingIngressReservations = Math.max(0, this.totalPendingIngressReservations - reserved);
        this.signalAdmissionCapacityChanged();
      }
    }
  }

  public async start(): Promise<void> {
    try {
      await this.syncDeployments();
    } catch (error) {
      console.error(
        `[${this.config.serviceName}] initial deployment sync failed; continuing with empty deployment set`,
        error,
      );
    }
    if (this.config.pollIntervalMs > 0) {
      this.pollingTimer = setInterval(() => {
        void this.syncDeployments().catch(() => undefined);
      }, this.config.pollIntervalMs);
      this.replayTimer = setInterval(() => {
        void this.processPendingReplays().catch(() => undefined);
      }, this.config.pollIntervalMs);
    }
    if (this.controlApiClient && this.config.metricsFlushIntervalMs > 0) {
      this.metricsFlushTimer = setInterval(() => {
        void this.flushRuntimeStats().catch(() => undefined);
      }, this.config.metricsFlushIntervalMs);
    }
  }

  public async stop(): Promise<void> {
    if (this.pollingTimer) {
      clearInterval(this.pollingTimer);
      this.pollingTimer = undefined;
    }
    if (this.replayTimer) {
      clearInterval(this.replayTimer);
      this.replayTimer = undefined;
    }
    if (this.metricsFlushTimer) {
      clearInterval(this.metricsFlushTimer);
      this.metricsFlushTimer = undefined;
    }

    await this.flushRuntimeStats(true).catch(() => undefined);
    await this.controlApiClient?.flushRunSummaries().catch(() => undefined);
    await this.controlApiClient?.flushRuntimeSamples().catch(() => undefined);

    for (const subscription of this.subscriptions.values()) {
      await subscription.unsubscribe();
    }
    this.subscriptions.clear();

    for (const batcher of this.httpSinkBatchers.values()) {
      await batcher.flush().catch(() => undefined);
    }
    this.httpSinkBatchers.clear();

    if (this.messageBus) {
      await this.messageBus.close();
    }
  }

  public getDeployments(): RouterDeployment[] {
    return [...this.deployments.values()];
  }

  public getRuns(): RunRecord[] {
    return this.runRecords.toArray();
  }

  public getDlq(): DlqRecord[] {
    return this.dlqRecords.toArray();
  }

  public getSummary(): RuntimeSummary {
    return {
      deployments: this.deployments.size,
      runs: this.totalRunCount,
      dlq: this.totalDlqCount,
      activeSubjects: [...this.subscriptions.keys()],
      mode: this.messageBus ? "nats" : "local",
      observability: {
        mode: "otel-primary",
        controlPlane: "aggregated runtime stats only",
      },
      health: this.buildHealthSummary(),
      deploymentStats: this.getDeploymentStats(),
    };
  }

  public async ingestEnvelope(
    input: IngressEnvelopeInput,
    options: { admissionMode?: IngressAdmissionMode } = {},
  ): Promise<ProcessedMessage> {
    const deployment = this.deployments.get(input.deploymentId);
    if (!deployment) {
      throw new Error(`unknown deployment ${input.deploymentId}`);
    }

    const reservation = await this.reserveIngressCapacity(deployment.id, options.admissionMode ?? "wait");
    const envelope = createIngressEnvelope(input);
    try {
      this.noteAccepted(deployment, envelope.receivedAt);
      this.maybeCaptureRuntimeSample(deployment, envelope);
    } finally {
      reservation.release();
    }
    return this.processDeploymentMessage(deployment, envelope);
  }

  public async enqueueIngress(
    input: IngressEnvelopeInput,
    options: { admissionMode?: IngressAdmissionMode } = {},
  ): Promise<{ subject: string; envelope: CanonicalEnvelope }> {
    const deployment = this.deployments.get(input.deploymentId);
    if (!deployment) {
      throw new Error(`unknown deployment ${input.deploymentId}`);
    }
    const reservation = await this.reserveIngressCapacity(input.deploymentId, options.admissionMode ?? "wait");
    const envelope = createIngressEnvelope(input);
    const subject = buildJetStreamSubject(
      "ingress",
      envelope.tenantId,
      envelope.flowId,
      envelope.revisionId,
      envelope.messageId,
    );

    try {
      if (this.messageBus) {
        await this.messageBus.publishToJetStream(subject, encodeJsonMessage(envelope));
      }

      if (deployment) {
        this.noteAccepted(deployment, envelope.receivedAt);
        this.maybeCaptureRuntimeSample(deployment, envelope);
      }
    } finally {
      reservation.release();
    }

    return { subject, envelope };
  }

  public async replay(request: ReplayRequest): Promise<ProcessedMessage> {
    const deployment = this.deployments.get(request.deploymentId);
    if (!deployment) {
      throw new Error(`unknown deployment ${request.deploymentId}`);
    }

    return this.processDeploymentMessage(deployment, request.envelope, request.reason);
  }

  private async processPendingReplays(): Promise<void> {
    if (!this.controlApiClient) {
      return;
    }

    const requests = await this.controlApiClient.listPendingReplayRequests();
    for (const request of requests) {
      await this.handlePendingReplayRequest(request);
    }
  }

  private async handlePendingReplayRequest(request: PendingReplayRequest): Promise<void> {
    if (!this.controlApiClient) {
      return;
    }

    const claimed = await this.controlApiClient.claimReplayRequest(request.id);
    if (!claimed) {
      return;
    }

    const deployment = this.getDeployments().find(
      (entry) => entry.flowId === claimed.flowId && entry.revisionId === claimed.revisionId,
    );
    const matchingDlq = this.getDlq().filter(
      (record) => record.flowId === claimed.flowId && record.revisionId === claimed.revisionId,
    );

    if (!deployment || matchingDlq.length === 0) {
      await this.controlApiClient.appendAudit({
        tenantId: deployment?.tenantId ?? "unknown-tenant",
        actor: this.config.serviceName,
        action: "replay.failed",
        subjectType: "replay",
        subjectId: claimed.id,
        details: {
          reason: "no matching DLQ messages available for replay",
          flowId: claimed.flowId,
          revisionId: claimed.revisionId,
        },
      });
      await this.controlApiClient.completeReplayRequest(claimed.id, "failed");
      return;
    }

    for (const dlq of matchingDlq) {
      if (this.messageBus) {
        await this.messageBus.publishToJetStream(
          buildReplaySubject(
            dlq.envelope.tenantId,
            dlq.envelope.flowId,
            dlq.envelope.revisionId,
            dlq.envelope.messageId,
          ),
          encodeJsonMessage(dlq.envelope),
        );
      } else {
        await this.replay({
          deploymentId: deployment.id,
          envelope: dlq.envelope,
          reason: claimed.reason,
          requestedAt: claimed.createdAt,
        });
      }
    }

    await this.controlApiClient.completeReplayRequest(claimed.id, "completed");
  }

  private async ensureSubscriptions(deployment: RouterDeployment): Promise<void> {
    if (!this.messageBus) return;

    for (const subject of deployment.sourceSubjects) {
      const subscriptionKey = buildJetStreamSubscriptionKey(subject);
      if (this.subscriptions.has(subscriptionKey)) continue;

      const handle = await this.messageBus.subscribeToJetStream(
        subject,
        async (data) => {
          const envelope = decodeJsonMessage<CanonicalEnvelope>(data);
          const notBefore = envelope.headers[NOT_BEFORE_HEADER];
          if (notBefore) {
            const waitMs = Date.parse(notBefore) - Date.now();
            if (Number.isFinite(waitMs) && waitMs > 0) {
              await sleep(Math.min(waitMs, MAX_RETRY_HOLD_MS));
            }
          }
          const currentDeployment = findDeploymentBySourceSubject(this.deployments.values(), subject, "jetstream");
          if (!currentDeployment) {
            return;
          }

          await this.processDeploymentMessage(currentDeployment, envelope);
        },
        {
          durableName: subject,
          concurrency: this.config.subscriptionConcurrency,
          partitionKey: (data) => {
            try {
              return decodeJsonMessage<CanonicalEnvelope>(data).partitionKey ?? "default";
            } catch {
              return "default";
            }
          },
        },
      );

      this.subscriptions.set(subscriptionKey, handle);
    }

    for (const subject of deployment.natsSourceSubjects) {
      const subscriptionKey = buildNatsSubscriptionKey(subject);
      if (this.subscriptions.has(subscriptionKey)) continue;

      const handle = await this.messageBus.subscribe(subject, async (data, metadata) => {
        const currentDeployment = findDeploymentBySourceSubject(this.deployments.values(), subject, "nats");
        if (!currentDeployment) {
          return;
        }

        let payload: unknown;
        try {
          payload = JSON.parse(decodeBusMessage(data));
        } catch {
          payload = decodeBusMessage(data);
        }

        const ingress = await this.enqueueIngress({
          deploymentId: currentDeployment.id,
          tenantId: currentDeployment.tenantId,
          flowId: currentDeployment.flowId,
          revisionId: currentDeployment.revisionId,
          messageId: crypto.randomUUID(),
          sourceRef: metadata.subject,
          partitionKey: currentDeployment.tenantId,
          payload,
        });

        if (!this.messageBus) {
          await this.processDeploymentMessage(currentDeployment, ingress.envelope);
        }
      });

      this.subscriptions.set(subscriptionKey, handle);
    }
  }

  private async dropSubscriptionsForDeployment(deployment: RouterDeployment): Promise<void> {
    for (const subject of deployment.sourceSubjects) {
      const key = buildJetStreamSubscriptionKey(subject);
      if (isSubjectUsedByAnotherDeployment(this.deployments.values(), subject, "jetstream", deployment.id)) {
        continue;
      }

      const handle = this.subscriptions.get(key);
      if (handle) {
        await handle.unsubscribe();
        this.subscriptions.delete(key);
      }
    }

    for (const subject of deployment.natsSourceSubjects) {
      const key = buildNatsSubscriptionKey(subject);
      if (isSubjectUsedByAnotherDeployment(this.deployments.values(), subject, "nats", deployment.id)) {
        continue;
      }

      const handle = this.subscriptions.get(key);
      if (handle) {
        await handle.unsubscribe();
        this.subscriptions.delete(key);
      }
    }
  }

  private async scheduleRetry(
    deployment: RouterDeployment,
    envelope: CanonicalEnvelope,
  ): Promise<void> {
    const nextEnvelope = nextAttemptEnvelope(envelope);
    const delayMs = this.config.retryBaseDelayMs * currentAttempt(envelope);

    if (this.messageBus) {
      // Publish before the current message is acked so a crash between the
      // ack and the retry cannot drop the event. The delay is carried in the
      // envelope and honored by the retry consumer.
      nextEnvelope.headers[NOT_BEFORE_HEADER] = new Date(Date.now() + delayMs).toISOString();
      await this.messageBus.publishToJetStream(
        buildRetrySubject(
          nextEnvelope.tenantId,
          nextEnvelope.flowId,
          nextEnvelope.revisionId,
          nextEnvelope.messageId,
        ),
        encodeJsonMessage(nextEnvelope),
      );
      return;
    }

    const processLocally = async () => {
      const currentDeployment = this.deployments.get(deployment.id);
      if (currentDeployment) {
        await this.processDeploymentMessage(currentDeployment, nextEnvelope, "scheduled retry");
      }
    };

    if (delayMs <= 0) {
      await processLocally();
      return;
    }

    setTimeout(() => {
      void processLocally().catch(() => undefined);
    }, delayMs);
  }

  private async reportRun(deployment: RouterDeployment, run: RunRecord, dlqRecord?: DlqRecord): Promise<void> {
    if (!this.controlApiClient) {
      return;
    }

    await this.controlApiClient.appendRunSummary({
      id: run.runId,
      flowId: run.flowId,
      revisionId: run.revisionId,
      deploymentId: run.deploymentId,
      messageId: run.messageId,
      status: mapRunStatusForControlApi(run.status),
      sourceRef: run.sourceRef,
      traceId: run.traceId,
      processedCount: 1,
      errorCount: errorCountForRunStatus(run.status),
      startedAt: run.receivedAt,
      finishedAt: run.finishedAt,
      lastError: run.reason ?? null,
      targetSinkIds: run.targetSinkIds,
      awaitedSinkIds: run.awaitedSinkIds,
    });

    if (run.status !== "delivered" && run.status !== "enqueued") {
      await this.controlApiClient.appendAudit({
        tenantId: deployment.tenantId,
        actor: this.config.serviceName,
        action: `run.${run.status}`,
        subjectType: "run",
        subjectId: run.runId,
        details: {
          flowId: run.flowId,
          revisionId: run.revisionId,
          messageId: run.messageId,
          reason: run.reason,
          targetSinkIds: run.targetSinkIds,
          dlqId: dlqRecord?.dlqId,
        },
      });
    }
  }

  private processorStateFor(deploymentId: string): {
    dedupeWindow: Map<string, { expiresAt: number; messageId: string }>;
    rateLimit: Map<string, { bucketAt: number; count: number }>;
  } {
    let state = this.processorState.get(deploymentId);
    if (!state) {
      state = { dedupeWindow: new Map(), rateLimit: new Map() };
      this.processorState.set(deploymentId, state);
    }
    return state;
  }

  private httpBatcherFor(deployment: RouterDeployment, sink: SinkNode): HttpSinkBatcher | undefined {
    const policy = deployment.spec.batchingPolicy;
    if (!policy?.enabled || policy.batchSize <= 1) {
      return undefined;
    }
    if (sink.connector.executionMode !== "native") {
      return undefined;
    }

    const target = sinkTargetFor(deployment, sink);
    if (!target || target.kind !== "http") {
      return undefined;
    }

    const key = `${deployment.id}:${deployment.revisionId}:${sink.id}`;
    let batcher = this.httpSinkBatchers.get(key);
    if (!batcher) {
      for (const staleKey of [...this.httpSinkBatchers.keys()]) {
        if (staleKey.startsWith(`${deployment.id}:`) && !staleKey.startsWith(`${deployment.id}:${deployment.revisionId}:`)) {
          const stale = this.httpSinkBatchers.get(staleKey);
          this.httpSinkBatchers.delete(staleKey);
          void stale?.flush().catch(() => undefined);
        }
      }
      batcher = new HttpSinkBatcher(target, policy.batchSize, policy.flushIntervalMs ?? 250, this.fetchImpl);
      this.httpSinkBatchers.set(key, batcher);
    }
    return batcher;
  }

  private async processDeploymentMessage(
    deployment: RouterDeployment,
    envelope: CanonicalEnvelope,
    replayReason?: string,
  ): Promise<ProcessedMessage> {
    this.noteInflight(deployment, 1);
    const context: ExecutionContext = {
      deployment,
      trace: [],
      sinkDeliveries: [],
      attemptedSinkIds: new Set<string>(),
      state: this.processorStateFor(deployment.id),
    };
    let runRecorded = false;
    const runId = `${envelope.messageId}:run:${currentAttempt(envelope)}:${crypto.randomUUID().slice(0, 8)}`;

    try {
      const execution = executeDeployment(deployment, envelope, context);

      const attempts: DeliveryAttempt[] = [];
      const awaitedSinkIds: string[] = [];
      let status: RunRecord["status"] = execution.status;
      let reason = execution.reason ?? replayReason;

      const deliveries = [...context.sinkDeliveries].sort((left, right) => {
        const leftSink = deployment.spec.sinks.find((candidate) => candidate.id === left.sinkId);
        const rightSink = deployment.spec.sinks.find((candidate) => candidate.id === right.sinkId);
        const leftWeight = leftSink?.connector.executionMode === "adapter" ? 1 : 0;
        const rightWeight = rightSink?.connector.executionMode === "adapter" ? 1 : 0;
        return leftWeight - rightWeight;
      });

      for (const delivery of deliveries) {
        const sink = deployment.spec.sinks.find((candidate) => candidate.id === delivery.sinkId);
        if (!sink) {
          status = "failed";
          reason = `unknown sink ${delivery.sinkId}`;
          continue;
        }

        context.attemptedSinkIds.add(sink.id);
        const outcome = await deliverSinkOnce(
          runId,
          deployment,
          sink,
          delivery.payload,
          envelope,
          this.messageBus,
          this.fetchImpl,
          currentAttempt(envelope),
          this.httpBatcherFor(deployment, sink),
        );

        attempts.push(outcome.attempt);

        if (outcome.ok) {
          if (outcome.awaitedCompletion) {
            awaitedSinkIds.push(sink.id);
          }
          continue;
        }

        if (canRetrySink(deployment, sink, envelope, this.config)) {
          status = "retrying";
          reason = outcome.error ?? `delivery failed for sink ${sink.id}`;
          await this.scheduleRetry(deployment, envelope);
        } else {
          status = "dlq";
          reason = outcome.error ?? `delivery failed for sink ${sink.id}`;
        }

        break;
      }

      if (status === "delivered" && context.sinkDeliveries.length === 0 && execution.status === "delivered") {
        status = "failed";
        reason = "no sink deliveries were produced by the flow";
      }

      if (status === "delivered" && awaitedSinkIds.length > 0) {
        status = "enqueued";
      }

      const run = buildRunRecord(
        runId,
        deployment,
        envelope,
        status,
        [...new Set(context.sinkDeliveries.map((entry) => entry.sinkId))],
        status === "enqueued" ? [...new Set(awaitedSinkIds)] : [],
        attempts,
        reason,
      );
      this.runRecords.push(run);
      this.totalRunCount += 1;
      this.noteRun(deployment, run);
      runRecorded = true;

      let dlqRecord: DlqRecord | undefined;
      if (status === "dlq" || status === "failed") {
        dlqRecord = buildDlqRecord(deployment, envelope, reason ?? "delivery failed");
        this.dlqRecords.push(dlqRecord);
        this.totalDlqCount += 1;
        if (this.messageBus) {
          await this.messageBus.publishToJetStream(
            buildDlqSubject(
              envelope.tenantId,
              envelope.flowId,
              envelope.revisionId,
              envelope.messageId,
            ),
            encodeJsonMessage(dlqRecord),
          );
        }
      }

      void this.reportRun(deployment, run, dlqRecord).catch(() => undefined);

      return {
        deployment,
        run,
        dlqRecord,
      };
    } finally {
      if (!runRecorded) {
        this.noteInflight(deployment, -1);
      }
    }
  }
}

export async function createRouterWorkerRuntime(options: {
  config: RouterWorkerRuntimeConfig;
  deploymentSource: DeploymentSource;
  deploymentTargetMap?: DeploymentTargetMap;
  messageBus?: MessageBus;
  fetchImpl?: typeof fetch;
  controlApiClient?: RouterControlApiClient;
}): Promise<RouterWorkerRuntime> {
  const runtime = new RouterWorkerRuntime(
    options.config,
    options.deploymentSource,
    {
      deploymentTargetMap: options.deploymentTargetMap,
      messageBus: options.messageBus,
      fetchImpl: options.fetchImpl,
      controlApiClient: options.controlApiClient,
    },
  );

  await runtime.start();
  return runtime;
}

export { buildDlqSubject, buildJetStreamSubject, buildReplaySubject, compileFlowSpec };
