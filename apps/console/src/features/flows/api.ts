import {
  controlApiPaths,
  type ControlApiFlowRecord,
  type ControlApiFlowPublishResponse,
  type ControlApiFlowRevisionRecord,
} from '@rohrpost/control-api-contracts';
import {
  compileFlowSpec,
  simulateFlowSpec,
  type CompiledFlowSummary,
  type FlowSpec,
  type ProcessorNode,
  type SinkDeliveryGuarantee,
  validateFlowSpec,
} from '@rohrpost/shared-flow-spec';
import type { DraftFlowResponse, FlowRecord } from '../../lib/api-types';
import { requestJson } from '../../lib/api-base';

export type BackendFlowSpec = FlowSpec;

function sourceKind(kind: BackendFlowSpec['sources'][number]['kind'] | undefined): FlowRecord['sourceKind'] {
  if (kind === 'nats' || kind === 'kafka') return kind;
  return 'http';
}

function mapFlowStatus(status: string): FlowRecord['status'] {
  if (status === 'active') return 'published';
  if (status === 'draft') return 'draft';
  if (status === 'paused') return 'paused';
  return 'degraded';
}

function emptyProcessorKinds(): Record<ProcessorNode['kind'], number> {
  return {
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
}

function emptyDeliveryGuarantees(): Record<SinkDeliveryGuarantee, number> {
  return {
    idempotent: 0,
    append_only: 0,
    best_effort: 0,
  };
}

function buildFallbackCompiledSummary(spec: BackendFlowSpec): CompiledFlowSummary {
  return {
    flowId: spec.metadata.flowId,
    revisionId: spec.metadata.revisionId,
    name: spec.metadata.name,
    sourceCount: spec.sources.length,
    processorCount: spec.processors.length,
    routeCount: spec.routes.length,
    sinkCount: spec.sinks.length,
    nativeConnectorCount: spec.sources.filter((source) => source.connector.executionMode === 'native').length
      + spec.sinks.filter((sink) => sink.connector.executionMode === 'native').length,
    adapterConnectorCount: spec.sources.filter((source) => source.connector.executionMode === 'adapter').length
      + spec.sinks.filter((sink) => sink.connector.executionMode === 'adapter').length,
    deliveryGuarantees: spec.sinks.reduce((counts, sink) => {
      counts[sink.deliveryGuarantee] += 1;
      return counts;
    }, emptyDeliveryGuarantees()),
    processorKinds: spec.processors.reduce((counts, processor) => {
      counts[processor.kind] += 1;
      return counts;
    }, emptyProcessorKinds()),
    warnings: ['FlowSpec validation failed.'],
  };
}

function compileBackendFlowSpec(spec: BackendFlowSpec): {
  compiled: CompiledFlowSummary;
  valid: boolean;
} {
  const validation = validateFlowSpec(spec);
  if (!validation.valid) {
    return {
      compiled: buildFallbackCompiledSummary(spec),
      valid: false,
    };
  }

  return {
    compiled: compileFlowSpec(spec),
    valid: true,
  };
}

function fallbackBackendSpec(record: ControlApiFlowRecord): BackendFlowSpec {
  return {
    version: 1,
    metadata: {
      tenantId: record.tenantId,
      flowId: record.id,
      revisionId: record.revisionId ?? record.activeRevisionId ?? record.latestRevisionId ?? `${record.id}_draft`,
      name: record.name,
    },
    sources: [],
    processors: [],
    routes: [],
    sinks: [],
    retryPolicy: {
      maxAttempts: 1,
      initialBackoffMs: 0,
      maxBackoffMs: 0,
      multiplier: 1,
    },
    dlqPolicy: {
      enabled: true,
    },
    batchingPolicy: {
      enabled: false,
      batchSize: 1,
    },
    idempotencyStrategy: 'message_id',
  };
}

function mapFlowRecord(record: ControlApiFlowRecord): FlowRecord {
  const backendSpec = record.spec ?? fallbackBackendSpec(record);
  const compiledState = compileBackendFlowSpec(backendSpec);
  const source = backendSpec.sources[0];
  const sink = backendSpec.sinks[0];

  return {
    id: record.id,
    name: record.name,
    status: compiledState.valid ? mapFlowStatus(record.status) : 'degraded',
    revisionId: record.revisionId ?? record.activeRevisionId ?? record.latestRevisionId ?? 'unpublished',
    execution: source?.connector.executionMode ?? 'native',
    sourceLabel: source?.connector.connectorId ?? source?.id ?? 'source',
    sourceKind: sourceKind(source?.kind),
    processors: backendSpec.processors.map((processor) => processor.kind),
    sinkLabel: sink?.connector.connectorId ?? sink?.id ?? 'unknown-sink',
    sinkGuarantee: sink?.deliveryGuarantee ?? 'best_effort',
    updatedAt: record.updatedAt,
    compiled: compiledState.compiled,
    backendSpec,
  };
}

export function mapBackendDraft(spec: BackendFlowSpec, samplePayload?: unknown): DraftFlowResponse {
  return {
    draft: spec,
    validation: validateFlowSpec(spec),
    compilation: compileFlowSpec(spec),
    simulation: simulateFlowSpec(spec, [
      {
        envelope: {},
        payload: samplePayload ?? {},
      },
    ]),
    backendSpec: spec,
  };
}

export async function fetchFlows(): Promise<FlowRecord[]> {
  return (await requestJson<ControlApiFlowRecord[]>(controlApiPaths.flows())).map(mapFlowRecord);
}

export async function deleteFlow(flowId: string): Promise<{ flowId: string; deleted: true }> {
  return requestJson<{ flowId: string; deleted: true }>(controlApiPaths.flow(flowId), {
    method: 'DELETE',
  });
}

export async function publishBackendFlowSpec(input: {
  spec: BackendFlowSpec;
  name?: string;
  tenantId?: string;
  samplePayload?: unknown;
}): Promise<{ flowId: string; revisionId: string; deploymentId: string }> {
  const createResponse = await requestJson<ControlApiFlowRevisionRecord>(controlApiPaths.flows(), {
    method: 'POST',
    body: JSON.stringify({
      tenantId: input.tenantId ?? input.spec.metadata.tenantId,
      name: input.name ?? input.spec.metadata.name,
      samplePayload: input.samplePayload,
      spec: input.spec,
    }),
  });

  const publishResponse = await requestJson<ControlApiFlowPublishResponse>(
    controlApiPaths.flowPublish(input.spec.metadata.flowId),
    {
      method: 'POST',
      body: JSON.stringify({
        revisionId: createResponse.id,
      }),
    },
  );

  return {
    flowId: createResponse.flowId,
    revisionId: createResponse.id,
    deploymentId: publishResponse.deployment.id,
  };
}
