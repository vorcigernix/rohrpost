import { compileFlowSpec } from "./compiler";
import { evaluatePredicate, getPath, setPath } from "./predicates";
import type {
  CanonicalEnvelope,
  EnrichLookupProcessorNode,
  FlowSpec,
  SimulationReport,
  SimulationSample,
  SinkNode,
} from "./types";

function cloneEnvelope(sample: SimulationSample, spec: FlowSpec, sampleIndex: number): CanonicalEnvelope {
  const envelope = sample.envelope;
  return {
    tenantId: envelope.tenantId ?? spec.metadata.tenantId,
    flowId: envelope.flowId ?? spec.metadata.flowId,
    revisionId: envelope.revisionId ?? spec.metadata.revisionId,
    messageId: envelope.messageId ?? `${spec.metadata.revisionId}-${sampleIndex + 1}`,
    sourceRef: envelope.sourceRef ?? sample.sourceId ?? spec.sources[0]?.id ?? "unknown-source",
    partitionKey: envelope.partitionKey ?? "default",
    headers: envelope.headers ?? {},
    payload: envelope.payload ?? sample.payload,
    receivedAt: envelope.receivedAt ?? new Date().toISOString(),
    traceId: envelope.traceId ?? `${spec.metadata.flowId}-${sampleIndex + 1}`,
  };
}

function applyMap(payload: unknown, mappings: Array<{ from: string; to: string }>): unknown {
  let nextPayload = payload;
  for (const mapping of mappings) {
    nextPayload = setPath(nextPayload, mapping.to, getPath(nextPayload, mapping.from));
  }
  return nextPayload;
}

function applyProjectedMap(
  payload: unknown,
  mappings: Array<{ from: string; to: string }>,
): unknown {
  let nextPayload: unknown = {};

  for (const mapping of mappings) {
    const value = getPath(payload, mapping.from);
    if (value !== undefined) {
      nextPayload = setPath(nextPayload, mapping.to, value);
    }
  }

  return nextPayload;
}

function applyTemplate(payload: unknown, template: string, targetPath?: string): unknown {
  const rendered = template.replace(/\$\{([^}]+)\}/g, (_, path: string) => {
    const value = getPath(payload, path.trim());
    return value == null ? "" : String(value);
  });

  if (targetPath) {
    return setPath(payload, targetPath, rendered);
  }

  if (typeof payload === "string") {
    return rendered;
  }

  return { rendered, payload };
}

function applyRedact(
  payload: unknown,
  paths: string[],
  mask = "[redacted]",
): unknown {
  let nextPayload = payload;
  for (const path of paths) {
    nextPayload = setPath(nextPayload, path, mask);
  }
  return nextPayload;
}

function applyEnrichStatic(payload: unknown, values: Record<string, unknown>): unknown {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    return { ...values, value: payload };
  }
  return { ...(payload as Record<string, unknown>), ...values };
}

function writeEnrichment(payload: unknown, value: unknown, targetPath?: string): unknown {
  if (targetPath) {
    return setPath(payload, targetPath, value);
  }

  if (value && typeof value === "object" && !Array.isArray(value)) {
    if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
      return { value: payload, ...(value as Record<string, unknown>) };
    }
    return { ...(payload as Record<string, unknown>), ...(value as Record<string, unknown>) };
  }

  return setPath(payload, "enrichment", value);
}

function applyEnrichLookup(
  payload: unknown,
  processor: EnrichLookupProcessorNode,
): { payload: unknown; found: boolean } {
  const key = getPath(payload, processor.keyPath);
  const tableKey = key == null ? null : String(key);
  const found = tableKey !== null && Object.prototype.hasOwnProperty.call(processor.lookup.table, tableKey);

  if (found) {
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

function sinkTargetsForNode(flow: FlowSpec, nodeId: string): SinkNode[] {
  const routes = flow.routes
    .filter((route) => route.fromNodeId === nodeId)
    .sort((left, right) => (right.priority ?? 0) - (left.priority ?? 0));

  return routes.flatMap((route) => route.toSinkIds.map((sinkId) => flow.sinks.find((sink) => sink.id === sinkId)))
    .filter((sink): sink is SinkNode => Boolean(sink));
}

export function simulateFlowSpec(spec: FlowSpec, samples: SimulationSample[]): SimulationReport {
  const compiled = compileFlowSpec(spec);
  void compiled;

  const items: SimulationReport["items"] = [];
  let accepted = 0;
  let dropped = 0;

  const dedupeWindowByKey = new Map<string, number>();
  let batchBuffer: unknown[] = [];

  for (let sampleIndex = 0; sampleIndex < samples.length; sampleIndex += 1) {
    const sample = samples[sampleIndex];
    const trace: string[] = [];
    let droppedSample = false;
    let dropReason: string | undefined;
    let payload: unknown = sample.payload;
    const envelope = cloneEnvelope(sample, spec, sampleIndex);
    const outputs: SimulationReport["items"][number]["outputs"] = [];

    const source = spec.sources.find((entry) => entry.id === (sample.sourceId ?? spec.sources[0]?.id));
    if (!source) {
      droppedSample = true;
      dropReason = "missing source";
    } else {
      trace.push(`source:${source.id}`);

      const walkNode = (nodeId: string, currentPayload: unknown): void => {
        if (droppedSample) return;

        const processor = spec.processors.find((entry) => entry.id === nodeId);
        if (!processor) {
          for (const sink of sinkTargetsForNode(spec, nodeId)) {
            outputs.push({
              sinkId: sink.id,
              payload: currentPayload,
              envelope: { ...envelope, payload: currentPayload },
            });
          }
          return;
        }

        trace.push(`processor:${processor.kind}:${processor.id}`);

        switch (processor.kind) {
          case "map":
            payload =
              processor.mode === "project"
                ? applyProjectedMap(currentPayload, processor.mappings)
                : applyMap(currentPayload, processor.mappings);
            break;
          case "filter":
            if (!evaluatePredicate(processor.predicate, currentPayload)) {
              droppedSample = true;
              dropReason = `filtered by ${processor.id}`;
              return;
            }
            payload = currentPayload;
            break;
          case "branch": {
            const branchCase = processor.cases.find((candidate) =>
              evaluatePredicate(candidate.predicate, currentPayload),
            );
            if (!branchCase) {
              droppedSample = true;
              dropReason = `no matching branch case for ${processor.id}`;
              return;
            }
            for (const nextNodeId of branchCase.nextNodeIds) {
              walkNode(nextNodeId, currentPayload);
            }
            return;
          }
          case "template":
            payload = applyTemplate(currentPayload, processor.template, processor.targetPath);
            break;
          case "redact":
            payload = applyRedact(currentPayload, processor.paths, processor.mask);
            break;
          case "enrich_static":
            payload = applyEnrichStatic(currentPayload, processor.values);
            break;
          case "enrich_lookup": {
            const result = applyEnrichLookup(currentPayload, processor);
            if (!result.found && processor.lookup.missing === "fail") {
              droppedSample = true;
              dropReason = `lookup miss for ${processor.id}`;
              return;
            }
            payload = result.payload;
            break;
          }
          case "batch":
            batchBuffer.push(currentPayload);
            if (batchBuffer.length >= processor.size) {
              outputs.push({
                sinkId: "batch",
                payload: batchBuffer,
                envelope: { ...envelope, payload: batchBuffer },
              });
              batchBuffer = [];
            }
            payload = currentPayload;
            break;
          case "retry":
            payload = currentPayload;
            break;
          case "rate_limit": {
            const receivedAtBucket = new Date(envelope.receivedAt).toISOString().slice(0, 19);
            const key = `${processor.id}:${receivedAtBucket}`;
            const current = dedupeWindowByKey.get(key) ?? 0;
            if (current >= processor.perSecond) {
              droppedSample = true;
              dropReason = `rate limited by ${processor.id}`;
              return;
            }
            dedupeWindowByKey.set(key, current + 1);
            payload = currentPayload;
            break;
          }
          case "dedupe_window": {
            const keyValue = getPath(currentPayload, processor.keyPath);
            const dedupeKey = `${processor.id}:${String(keyValue)}`;
            const currentTime = new Date(envelope.receivedAt).getTime();
            const previousTime = dedupeWindowByKey.get(dedupeKey);
            if (previousTime !== undefined && currentTime - previousTime < processor.windowMs) {
              droppedSample = true;
              dropReason = `duplicate within dedupe window for ${processor.id}`;
              return;
            }
            dedupeWindowByKey.set(dedupeKey, currentTime);
            payload = currentPayload;
            break;
          }
        }

        const routedSinks = sinkTargetsForNode(spec, nodeId);
        for (const sink of routedSinks) {
          outputs.push({
            sinkId: sink.id,
            payload,
            envelope: { ...envelope, payload },
          });
        }

        for (const nextNodeId of processor.nextNodeIds) {
          walkNode(nextNodeId, payload);
        }
      };

      for (const nextNodeId of source.nextNodeIds) {
        walkNode(nextNodeId, payload);
      }
    }

    if (droppedSample) {
      dropped += 1;
    } else {
      accepted += 1;
    }

    items.push({
      sampleIndex,
      dropped: droppedSample,
      dropReason,
      outputs,
      trace,
    });
  }

  if (batchBuffer.length > 0) {
    items.push({
      sampleIndex: samples.length,
      dropped: false,
      outputs: [
        {
          sinkId: "batch",
          payload: batchBuffer,
          envelope: {
            tenantId: spec.metadata.tenantId,
            flowId: spec.metadata.flowId,
            revisionId: spec.metadata.revisionId,
            messageId: `${spec.metadata.revisionId}-batch`,
            sourceRef: "batch-buffer",
            partitionKey: "default",
            headers: {},
            payload: batchBuffer,
            receivedAt: new Date().toISOString(),
            traceId: `${spec.metadata.flowId}-batch`,
          },
        },
      ],
      trace: ["batch:flush"],
    });
  }

  return {
    accepted,
    dropped,
    outputs: items.reduce((count, item) => count + item.outputs.length, 0),
    items,
  };
}
