import { describe, expect, test } from 'bun:test';
import type { FlowSpec as BackendFlowSpec } from '@rohrpost/shared-flow-spec';
import { deriveWorkflowGraph } from './workflow-graph';
import type { RuntimeDeploymentRecord } from '../../lib/api-types';

function baseSpec(overrides: Partial<BackendFlowSpec> = {}): BackendFlowSpec {
  return {
    version: 1,
    metadata: {
      tenantId: 'tenant_demo',
      flowId: 'flow_demo',
      revisionId: 'rev_demo',
      name: 'Demo flow',
    },
    sources: [
      {
        id: 'source_primary',
        kind: 'http',
        connector: {
          capabilityId: 'http_in',
          connectorId: 'http_in_default',
          executionMode: 'native',
        },
        stream: 'ingress',
        nextNodeIds: ['processor_map'],
      },
    ],
    processors: [
      {
        id: 'processor_map',
        kind: 'map',
        mode: 'project',
        mappings: [{ from: 'event_name', to: 'event_name' }],
        nextNodeIds: [],
      },
    ],
    routes: [
      {
        id: 'route_terminal',
        fromNodeId: 'processor_map',
        predicate: { type: 'always' },
        toSinkIds: ['sink_primary'],
      },
    ],
    sinks: [
      {
        id: 'sink_primary',
        kind: 'bigquery',
        connector: {
          capabilityId: 'bigquery_sink',
          connectorId: 'bq_orders',
          executionMode: 'adapter',
        },
        deliveryGuarantee: 'append_only',
        stream: 'work',
      },
    ],
    retryPolicy: {
      maxAttempts: 1,
      initialBackoffMs: 250,
      maxBackoffMs: 5_000,
      multiplier: 2,
    },
    dlqPolicy: {
      enabled: true,
      sinkId: 'sink_primary',
      reasonFormat: 'json',
    },
    batchingPolicy: {
      enabled: true,
      batchSize: 100,
      flushIntervalMs: 1_000,
    },
    idempotencyStrategy: 'message_id',
    ...overrides,
  };
}

function deployment(overrides: Partial<RuntimeDeploymentRecord> = {}): RuntimeDeploymentRecord {
  return {
    deploymentId: 'deploy_demo',
    flowId: 'flow_demo',
    flowName: 'Demo flow',
    revisionId: 'rev_demo',
    rolloutStatus: 'activated',
    state: 'healthy',
    acceptedCount: 100,
    processedCount: 96,
    deliveredCount: 96,
    backlogCount: 0,
    inflightCount: 0,
    retryingCount: 0,
    dlqCount: 0,
    lastAcceptedAt: '2026-04-27T12:00:00.000Z',
    lastProcessedAt: '2026-04-27T12:00:01.000Z',
    updatedAt: '2026-04-27T12:00:01.000Z',
    lastError: null,
    ...overrides,
  };
}

describe('workflow graph derivation', () => {
  test('keeps a linear adapter sink visible as a queue and destination', () => {
    const graph = deriveWorkflowGraph({ spec: baseSpec(), deployment: deployment() });

    expect(graph.nodes.map((node) => node.kind)).toEqual([
      'source',
      'transform',
      'destination',
      'queue',
    ]);
    expect(graph.edges.some((edge) => edge.source === 'processor:processor_map' && edge.target === 'queue:sink_primary')).toBe(true);
    expect(graph.edges.some((edge) => edge.source === 'queue:sink_primary' && edge.target === 'sink:sink_primary')).toBe(true);
  });

  test('preserves branch routes as branch nodes with route labels', () => {
    const spec = baseSpec({
      routes: [
        {
          id: 'route_purchase',
          fromNodeId: 'processor_map',
          predicate: { type: 'field_equals', path: 'event_name', value: 'purchase' },
          toSinkIds: ['sink_primary'],
        },
        {
          id: 'route_cart',
          fromNodeId: 'processor_map',
          predicate: { type: 'field_equals', path: 'event_name', value: 'add_to_cart' },
          toSinkIds: ['sink_cart'],
        },
      ],
      sinks: [
        baseSpec().sinks[0],
        {
          id: 'sink_cart',
          kind: 'nats',
          connector: {
            capabilityId: 'nats_out',
            connectorId: 'nats_cart',
            executionMode: 'native',
          },
          deliveryGuarantee: 'idempotent',
          stream: 'work',
        },
      ],
    });
    const graph = deriveWorkflowGraph({ spec, deployment: deployment() });

    const branchNodes = graph.nodes.filter((node) => node.kind === 'branch');
    expect(branchNodes).toHaveLength(2);
    expect(branchNodes.map((node) => node.detail).sort()).toEqual([
      'event_name = add_to_cart',
      'event_name = purchase',
    ]);
  });

  test('marks a queued destination as the bottleneck when backlog builds', () => {
    const graph = deriveWorkflowGraph({
      spec: baseSpec(),
      deployment: deployment({ state: 'backlogged', backlogCount: 42, inflightCount: 3 }),
    });

    expect(graph.bottleneckSummary).toContain('Destination queue');
    expect(graph.nodes.find((node) => node.id === 'queue:sink_primary')?.bottleneck).toBe(true);
    expect(graph.edges.some((edge) => edge.bottleneck)).toBe(true);
  });

  test('models async enrichment as queue, lookup, and post-lookup queue', () => {
    const graph = deriveWorkflowGraph({
      spec: baseSpec({
        processors: [
          {
            id: 'processor_enrich',
            kind: 'enrich_lookup',
            connector: {
              capabilityId: 'credit_score_lookup',
              connectorId: 'credit_score_postgres',
              executionMode: 'adapter',
            },
            keyPath: 'customer_id',
            targetPath: 'customer.risk',
            lookup: {
              mode: 'inline',
              table: {
                'cust-1': { band: 'low' },
              },
            },
            nextNodeIds: [],
          },
        ],
        sources: [
          {
            ...baseSpec().sources[0],
            nextNodeIds: ['processor_enrich'],
          },
        ],
        routes: [
          {
            id: 'route_terminal',
            fromNodeId: 'processor_enrich',
            predicate: { type: 'always' },
            toSinkIds: ['sink_primary'],
          },
        ],
      }),
      deployment: deployment(),
    });

    expect(graph.nodes.find((node) => node.id === 'queue-in:processor_enrich')?.label).toBe('Enrichment queue');
    expect(graph.nodes.find((node) => node.id === 'processor:processor_enrich')?.kind).toBe('enrichment');
    expect(graph.nodes.find((node) => node.id === 'queue-out:processor_enrich')?.label).toBe('Enriched queue');
  });
});
