import type { FlowSpec as BackendFlowSpec, ProcessorNode, RouteRule, SinkNode } from '@rohrpost/shared-flow-spec';
import type { AdapterWorkloadRecord, RuntimeDeploymentRecord } from '../../lib/api-types';

export type WorkflowNodeKind = 'source' | 'transform' | 'branch' | 'enrichment' | 'queue' | 'destination';
export type WorkflowHealth = 'healthy' | 'backlogged' | 'degraded' | 'idle' | 'unknown';

export interface WorkflowMetric {
  label: string;
  value: string;
  muted?: boolean;
}

export type WorkflowNodeRef =
  | { type: 'source'; id: string }
  | { type: 'processor'; id: string }
  | { type: 'route'; id: string }
  | { type: 'sink'; id: string };

export interface WorkflowNode {
  id: string;
  kind: WorkflowNodeKind;
  label: string;
  detail?: string;
  ref?: WorkflowNodeRef;
  status: WorkflowHealth;
  metrics: WorkflowMetric[];
  bottleneck?: boolean;
}

export interface WorkflowEdge {
  id: string;
  source: string;
  target: string;
  label?: string;
  status: WorkflowHealth;
  bottleneck?: boolean;
}

export interface WorkflowGraphModel {
  nodes: WorkflowNode[];
  edges: WorkflowEdge[];
  bottleneckSummary: string | null;
}

export interface DeriveWorkflowGraphInput {
  spec?: BackendFlowSpec | null;
  deployment?: RuntimeDeploymentRecord | null;
  adapterWorkloads?: AdapterWorkloadRecord[];
}

type NodeEndpoint = {
  entry: string;
  exit: string;
};

function compactId(prefix: string, id: string): string {
  return `${prefix}:${id}`;
}

function formatCount(value: number | undefined): string {
  if (typeof value !== 'number') return '—';
  return value.toLocaleString();
}

function statusFromDeployment(deployment?: RuntimeDeploymentRecord | null): WorkflowHealth {
  if (!deployment) return 'unknown';
  if (deployment.state === 'backlogged') return 'backlogged';
  if (deployment.state === 'degraded') return 'degraded';
  if (deployment.state === 'idle') return 'idle';
  return 'healthy';
}

function destinationStatus(
  sink: SinkNode,
  deployment?: RuntimeDeploymentRecord | null,
  adapterWorkloads: AdapterWorkloadRecord[] = [],
): WorkflowHealth {
  const workload = adapterWorkloads.find((entry) => entry.connectorId === sink.connector.connectorId);
  if (workload?.status === 'degraded' || deployment?.state === 'degraded') return 'degraded';
  if (deployment?.backlogCount && deployment.backlogCount > 0) return 'backlogged';
  if (workload?.status === 'starting') return 'idle';
  return statusFromDeployment(deployment);
}

function queueStatus(deployment?: RuntimeDeploymentRecord | null): WorkflowHealth {
  if (!deployment) return 'unknown';
  if (deployment.state === 'degraded') return 'degraded';
  if (deployment.backlogCount > 0) return 'backlogged';
  return deployment.state === 'idle' ? 'idle' : 'healthy';
}

function isEnrichmentProcessor(processor: ProcessorNode): boolean {
  return processor.kind === 'enrich_static' || processor.kind === 'enrich_lookup';
}

function isAsyncEnrichment(processor: ProcessorNode): boolean {
  return isEnrichmentProcessor(processor) && processor.connector?.executionMode === 'adapter';
}

function processorKind(processor: ProcessorNode): WorkflowNodeKind {
  if (processor.kind === 'branch') return 'branch';
  if (isEnrichmentProcessor(processor)) return 'enrichment';
  return 'transform';
}

function processorLabel(processor: ProcessorNode): string {
  switch (processor.kind) {
    case 'map':
      return processor.mode === 'merge' ? 'Merge fields' : 'Map fields';
    case 'filter':
      return 'Filter';
    case 'branch':
      return 'Route';
    case 'template':
      return 'Template';
    case 'redact':
      return 'Redact';
    case 'enrich_static':
      return processor.connector?.executionMode === 'adapter' ? 'Enrichment lookup' : 'Static enrichment';
    case 'enrich_lookup':
      return 'Lookup enrichment';
    case 'batch':
      return 'Batch';
    case 'retry':
      return 'Retry policy';
    case 'rate_limit':
      return 'Rate limit';
    case 'dedupe_window':
      return 'Dedupe window';
  }
}

function processorDetail(processor: ProcessorNode): string {
  switch (processor.kind) {
    case 'map':
      return `${processor.mappings.length} mapping${processor.mappings.length === 1 ? '' : 's'}`;
    case 'filter':
      return predicateLabel(processor.predicate);
    case 'branch':
      return `${processor.cases.length} case${processor.cases.length === 1 ? '' : 's'}`;
    case 'redact':
      return `${processor.paths.length} path${processor.paths.length === 1 ? '' : 's'}`;
    case 'enrich_static':
      return processor.connector?.connectorId ?? `${Object.keys(processor.values).length} field${Object.keys(processor.values).length === 1 ? '' : 's'}`;
    case 'enrich_lookup':
      return `${processor.keyPath} -> ${processor.targetPath ?? 'payload'}`;
    case 'batch':
      return `${processor.size} events`;
    case 'retry':
      return `${processor.maxAttempts} attempts`;
    case 'rate_limit':
      return `${processor.perSecond}/s`;
    case 'dedupe_window':
      return processor.keyPath;
    case 'template':
      return processor.targetPath ?? 'payload';
  }
}

function routeRequiresBranch(routes: RouteRule[]): boolean {
  return routes.length > 1 || routes.some((route) => route.toSinkIds.length > 1 || route.predicate.type !== 'always');
}

function predicateLabel(predicate: RouteRule['predicate']): string {
  switch (predicate.type) {
    case 'always':
      return 'All events';
    case 'field_exists':
      return `${predicate.path} exists`;
    case 'field_equals':
      return `${predicate.path} = ${String(predicate.value)}`;
    case 'field_contains':
      return `${predicate.path} contains ${predicate.value}`;
    case 'field_gt':
      return `${predicate.path} > ${predicate.value}`;
    case 'field_gte':
      return `${predicate.path} >= ${predicate.value}`;
    case 'field_lt':
      return `${predicate.path} < ${predicate.value}`;
    case 'field_lte':
      return `${predicate.path} <= ${predicate.value}`;
    case 'and':
      return predicate.all.map(predicateLabel).join(' and ');
    case 'or':
      return predicate.any.map(predicateLabel).join(' or ');
    case 'not':
      return `not ${predicateLabel(predicate.predicate)}`;
  }
}

function addEdge(
  edges: WorkflowEdge[],
  source: string | undefined,
  target: string | undefined,
  label?: string,
  status: WorkflowHealth = 'unknown',
): void {
  if (!source || !target || source === target) return;
  const id = `edge:${source}:${target}:${label ?? ''}`;
  if (edges.some((edge) => edge.id === id)) return;
  edges.push({ id, source, target, label, status });
}

function buildSinkEndpoints(
  nodes: WorkflowNode[],
  edges: WorkflowEdge[],
  sink: SinkNode,
  deployment?: RuntimeDeploymentRecord | null,
  adapterWorkloads?: AdapterWorkloadRecord[],
): NodeEndpoint {
  const sinkStatus = destinationStatus(sink, deployment, adapterWorkloads);
  const sinkNodeId = compactId('sink', sink.id);
  const needsQueue = sink.connector.executionMode === 'adapter' || sink.kind === 'bigquery' || sink.kind === 'snowflake' || sink.kind === 's3';

  nodes.push({
    id: sinkNodeId,
    kind: 'destination',
    label: sink.connector.connectorId,
    detail: `${sink.kind} · ${sink.deliveryGuarantee}`,
    ref: { type: 'sink', id: sink.id },
    status: sinkStatus,
    metrics: [
      { label: 'delivered', value: formatCount(deployment?.deliveredCount), muted: !deployment },
      { label: 'errors', value: deployment?.lastError ? 'yes' : '0', muted: !deployment },
    ],
  });

  if (!needsQueue) {
    return { entry: sinkNodeId, exit: sinkNodeId };
  }

  const queueNodeId = compactId('queue', sink.id);
  const status = queueStatus(deployment);
  nodes.push({
    id: queueNodeId,
    kind: 'queue',
    label: 'Destination queue',
    detail: sink.connector.executionMode === 'adapter' ? 'adapter handoff' : 'batch buffer',
    ref: { type: 'sink', id: sink.id },
    status,
    metrics: [
      { label: 'backlog', value: formatCount(deployment?.backlogCount), muted: !deployment },
      { label: 'oldest', value: '—', muted: true },
    ],
  });
  addEdge(edges, queueNodeId, sinkNodeId, undefined, status);
  return { entry: queueNodeId, exit: sinkNodeId };
}

function markBottleneck(model: WorkflowGraphModel, deployment?: RuntimeDeploymentRecord | null): WorkflowGraphModel {
  if (!deployment) return model;

  const queueNode = model.nodes.find((node) => node.kind === 'queue');
  const degradedNode = model.nodes.find((node) => node.status === 'degraded');
  const candidate =
    deployment.state === 'degraded'
      ? degradedNode ?? queueNode
      : deployment.backlogCount > 0
        ? queueNode ?? model.nodes.find((node) => node.kind === 'destination')
        : null;

  if (!candidate) return model;

  const nodes = model.nodes.map((node) =>
    node.id === candidate.id ? { ...node, bottleneck: true } : node,
  );
  const edges = model.edges.map((edge) =>
    edge.target === candidate.id || edge.source === candidate.id
      ? { ...edge, bottleneck: true, status: candidate.status }
      : edge,
  );
  const suffix = deployment.backlogCount > 0
    ? `, backlog ${deployment.backlogCount.toLocaleString()}`
    : deployment.lastError
      ? `, ${deployment.lastError}`
      : '';

  return {
    nodes,
    edges,
    bottleneckSummary: `Bottleneck: ${candidate.label}${suffix}`,
  };
}

export function deriveWorkflowGraph({
  spec,
  deployment,
  adapterWorkloads = [],
}: DeriveWorkflowGraphInput): WorkflowGraphModel {
  if (!spec) {
    return {
      nodes: [],
      edges: [],
      bottleneckSummary: null,
    };
  }

  const nodes: WorkflowNode[] = [];
  const edges: WorkflowEdge[] = [];
  const endpoints = new Map<string, NodeEndpoint>();
  const deploymentStatus = statusFromDeployment(deployment);

  for (const source of spec.sources) {
    const sourceNodeId = compactId('source', source.id);
    nodes.push({
      id: sourceNodeId,
      kind: 'source',
      label: source.connector.connectorId,
      detail: `${source.kind} ingest`,
      ref: { type: 'source', id: source.id },
      status: deploymentStatus,
      metrics: [
        { label: 'accepted', value: formatCount(deployment?.acceptedCount), muted: !deployment },
        { label: 'inflight', value: formatCount(deployment?.inflightCount), muted: !deployment },
      ],
    });
    endpoints.set(source.id, { entry: sourceNodeId, exit: sourceNodeId });
  }

  for (const processor of spec.processors) {
    const processorNodeId = compactId('processor', processor.id);
    const status = processor.connector?.executionMode === 'adapter'
      ? queueStatus(deployment)
      : deploymentStatus;

    nodes.push({
      id: processorNodeId,
      kind: processorKind(processor),
      label: processorLabel(processor),
      detail: processorDetail(processor),
      ref: { type: 'processor', id: processor.id },
      status,
      metrics: [
        { label: 'processed', value: formatCount(deployment?.processedCount), muted: !deployment },
        { label: 'p95', value: '—', muted: true },
      ],
    });

    if (isAsyncEnrichment(processor)) {
      const queueInId = compactId('queue-in', processor.id);
      const queueOutId = compactId('queue-out', processor.id);
      const qStatus = queueStatus(deployment);
      nodes.push({
        id: queueInId,
        kind: 'queue',
        label: 'Enrichment queue',
        detail: 'lookup handoff',
        ref: { type: 'processor', id: processor.id },
        status: qStatus,
        metrics: [
          { label: 'backlog', value: formatCount(deployment?.backlogCount), muted: !deployment },
          { label: 'oldest', value: '—', muted: true },
        ],
      });
      nodes.push({
        id: queueOutId,
        kind: 'queue',
        label: 'Enriched queue',
        detail: 'post-lookup buffer',
        ref: { type: 'processor', id: processor.id },
        status: qStatus,
        metrics: [
          { label: 'backlog', value: formatCount(deployment?.backlogCount), muted: !deployment },
          { label: 'oldest', value: '—', muted: true },
        ],
      });
      addEdge(edges, queueInId, processorNodeId, undefined, qStatus);
      addEdge(edges, processorNodeId, queueOutId, undefined, qStatus);
      endpoints.set(processor.id, { entry: queueInId, exit: queueOutId });
    } else {
      endpoints.set(processor.id, { entry: processorNodeId, exit: processorNodeId });
    }
  }

  const sinkEndpoints = new Map<string, NodeEndpoint>();
  for (const sink of spec.sinks) {
    sinkEndpoints.set(
      sink.id,
      buildSinkEndpoints(nodes, edges, sink, deployment, adapterWorkloads),
    );
  }

  for (const source of spec.sources) {
    const from = endpoints.get(source.id)?.exit;
    for (const nextNodeId of source.nextNodeIds) {
      const target = endpoints.get(nextNodeId)?.entry;
      addEdge(edges, from, target, undefined, deploymentStatus);
    }
  }

  for (const processor of spec.processors) {
    const from = endpoints.get(processor.id)?.exit;
    for (const nextNodeId of processor.nextNodeIds) {
      const target = endpoints.get(nextNodeId)?.entry;
      addEdge(edges, from, target, undefined, deploymentStatus);
    }
  }

  const routesBySource = new Map<string, RouteRule[]>();
  for (const route of spec.routes) {
    routesBySource.set(route.fromNodeId, [...(routesBySource.get(route.fromNodeId) ?? []), route]);
  }

  for (const [fromNodeId, routes] of routesBySource.entries()) {
    const routeSource = endpoints.get(fromNodeId)?.exit ?? endpoints.get(spec.sources[0]?.id)?.exit;
    if (!routeSource) continue;

    if (!routeRequiresBranch(routes)) {
      const route = routes[0];
      addEdge(edges, routeSource, sinkEndpoints.get(route.toSinkIds[0])?.entry, undefined, deploymentStatus);
      continue;
    }

    for (const route of routes) {
      const branchNodeId = compactId('route', route.id);
      nodes.push({
        id: branchNodeId,
        kind: 'branch',
        label: 'Branch',
        detail: predicateLabel(route.predicate),
        ref: { type: 'route', id: route.id },
        status: deploymentStatus,
        metrics: [
          { label: 'matches', value: '—', muted: true },
          { label: 'p95', value: '—', muted: true },
        ],
      });
      addEdge(edges, routeSource, branchNodeId, undefined, deploymentStatus);
      for (const sinkId of route.toSinkIds) {
        addEdge(edges, branchNodeId, sinkEndpoints.get(sinkId)?.entry, predicateLabel(route.predicate), deploymentStatus);
      }
    }
  }

  if (edges.length === 0 && spec.sources[0]) {
    const from = endpoints.get(spec.sources[0].id)?.exit;
    for (const sink of spec.sinks) {
      addEdge(edges, from, sinkEndpoints.get(sink.id)?.entry, undefined, deploymentStatus);
    }
  }

  return markBottleneck({ nodes, edges, bottleneckSummary: null }, deployment);
}
