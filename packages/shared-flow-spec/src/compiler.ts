import { assertValidFlowSpec } from "./validation";
import type { CompiledFlowSummary, FlowSpec, ProcessorNode } from "./types";

function countProcessors(processors: ProcessorNode[]): CompiledFlowSummary["processorKinds"] {
  const counts: CompiledFlowSummary["processorKinds"] = {
    map: 0,
    filter: 0,
    branch: 0,
    template: 0,
    redact: 0,
    enrich_static: 0,
    enrich_lookup: 0,
    batch: 0,
    retry: 0,
    rate_limit: 0,
    dedupe_window: 0,
  };

  for (const processor of processors) {
    counts[processor.kind] += 1;
  }

  return counts;
}

export function compileFlowSpec(spec: FlowSpec): CompiledFlowSummary {
  assertValidFlowSpec(spec);

  const nativeConnectorIds = new Set<string>();
  const adapterConnectorIds = new Set<string>();
  const warnings: string[] = [];

  const recordConnector = (label: string, connector: { connectorId: string; executionMode: "native" | "adapter" }) => {
    if (connector.executionMode === "native") {
      nativeConnectorIds.add(connector.connectorId);
    } else {
      adapterConnectorIds.add(connector.connectorId);
      warnings.push(`${label} is adapter-executed`);
    }
  };

  for (const source of spec.sources) {
    recordConnector(`Source ${source.id}`, source.connector);
  }

  for (const processor of spec.processors) {
    if (processor.connector) {
      recordConnector(`Processor ${processor.id}`, processor.connector);
    }
  }

  for (const sink of spec.sinks) {
    recordConnector(`Sink ${sink.id}`, sink.connector);
  }

  return {
    flowId: spec.metadata.flowId,
    revisionId: spec.metadata.revisionId,
    name: spec.metadata.name,
    sourceCount: spec.sources.length,
    processorCount: spec.processors.length,
    routeCount: spec.routes.length,
    sinkCount: spec.sinks.length,
    nativeConnectorCount: nativeConnectorIds.size,
    adapterConnectorCount: adapterConnectorIds.size,
    deliveryGuarantees: {
      idempotent: spec.sinks.filter((sink) => sink.deliveryGuarantee === "idempotent").length,
      append_only: spec.sinks.filter((sink) => sink.deliveryGuarantee === "append_only").length,
      best_effort: spec.sinks.filter((sink) => sink.deliveryGuarantee === "best_effort").length,
    },
    processorKinds: countProcessors(spec.processors),
    warnings,
  };
}
