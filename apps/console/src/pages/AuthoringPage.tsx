import { useEffect, useMemo, useRef, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Link, useNavigate } from '@tanstack/react-router';
import { SegmentedControl, SegmentedControlItem } from '@astryxdesign/core/SegmentedControl';
import { CheckIcon, CommandLineIcon, SparklesIcon } from '@heroicons/react/24/outline';
import { api, type ConnectorRecord } from '../lib/api';
import type { BackendFlowSpec } from '../features/flows/api';
import { mapBackendDraft } from '../features/flows/api';
import {
  demoBusinessExamples,
  readSelectedDemoBusinessExample,
  rememberSelectedDemoBusinessExample,
  type DemoBusinessExample,
} from '../lib/business-examples';
import type {
  CapabilityData,
  RuntimeSampleRecord,
  TransformComposerResponse,
} from '../lib/api-types';
import { readDemoEventHistory, readLatestDemoEvent, type DemoEventRecord } from '../lib/demo-events';
import { Card } from '@astryxdesign/core/Card';
import { ChatComposer } from '@astryxdesign/core/Chat';
import { CodeBlock } from '@astryxdesign/core/CodeBlock';
import { FieldStatus } from '@astryxdesign/core/FieldStatus';
import { Grid, GridSpan } from '@astryxdesign/core/Grid';
import { Heading } from '@astryxdesign/core/Heading';
import { Icon } from '@astryxdesign/core/Icon';
import { Selector } from '@astryxdesign/core/Selector';
import { TextArea } from '@astryxdesign/core/TextArea';
import { TextInput } from '@astryxdesign/core/TextInput';
import { ToggleButton, ToggleButtonGroup } from '@astryxdesign/core/ToggleButton';
import { Token } from '@astryxdesign/core/Token';
import { HStack } from '@astryxdesign/core/HStack';
import { Text } from '@astryxdesign/core/Text';
import { VStack } from '@astryxdesign/core/VStack';
import { ActionButton, LoadingBlock, Panel, Pill, SectionHeader } from '../components/ui';
import { WorkflowGraph } from '../components/WorkflowGraph';
import { JsonSyntaxEditor } from '../components/JsonSyntaxEditor';
import {
  useComposeStepNavigation,
  type ComposeWizardStep,
} from '../lib/compose-navigation';

type SourceKind = 'http' | 'nats' | 'kafka';
type SampleOrigin = 'live' | 'manual' | 'demo' | null;
type WizardStep = ComposeWizardStep;
type ReviewPane = 'sample' | 'edit';
type ComposeDesignerPanel = 'ingest' | 'sample' | 'transform' | 'destination' | 'advanced';
type DiffRow = {
  path: string;
  before?: string;
  after?: string;
  kind: 'added' | 'removed' | 'changed' | 'unchanged';
};

type DestinationField = {
  key: string;
  label: string;
  placeholder: string;
  multiline?: boolean;
  wide?: boolean;
};

type SinkCapabilityId = 'http_out' | 'nats_out' | 'snowflake_sink' | 'bigquery_sink' | 's3_sink' | 'kafka_out';

type ResolvedSink = {
  kind: BackendFlowSpec['sinks'][number]['kind'];
  capabilityId: SinkCapabilityId;
  connectorId: string;
  executionMode: BackendFlowSpec['sinks'][number]['connector']['executionMode'];
  deliveryGuarantee: BackendFlowSpec['sinks'][number]['deliveryGuarantee'];
};

type RuntimeSampleShape = {
  id: string;
  label: string;
  sample: RuntimeSampleRecord;
  count: number;
};

type DemoEventShape = {
  id: string;
  label: string;
  event: DemoEventRecord;
  count: number;
};

const NEW_DESTINATION_VALUE = '__new_destination__';

function readEditingFlowId(): string | null {
  if (typeof window === 'undefined') return null;
  return new URLSearchParams(window.location.search).get('flowId');
}

function parseSamplePayload(text: string): { value?: unknown; error?: string } {
  if (!text.trim()) return { value: undefined };
  try {
    return { value: JSON.parse(text) };
  } catch (error) {
    return { error: error instanceof Error ? error.message : 'Invalid JSON.' };
  }
}

function formatJson(value: unknown): string {
  return JSON.stringify(value, null, 2) ?? 'null';
}

function sourceLabel(kind: SourceKind): string {
  switch (kind) {
    case 'nats': return 'NATS';
    case 'kafka': return 'Kafka';
    default: return 'HTTP';
  }
}

function previewTone(result: TransformComposerResponse['preview']): 'good' | 'warn' {
  return result.accepted ? 'good' : 'warn';
}

function assistantTone(provider: string): 'good' | 'info' | 'warn' {
  if (provider === 'existing-flow') return 'info';
  return provider === 'gemini' ? 'good' : 'warn';
}

function assistantLabel(provider: string): string {
  if (provider === 'existing-flow') return 'Existing flow';
  return provider === 'gemini' ? 'Gemini AI' : 'Heuristic fallback';
}

function requireSamplePayload(samplePayload: { value?: unknown; error?: string }): unknown {
  if (samplePayload.error) throw new Error(samplePayload.error);
  if (typeof samplePayload.value === 'undefined') {
    throw new Error('Select an event or paste JSON first.');
  }
  return samplePayload.value;
}

function formatObservedAt(value: string): string {
  return new Date(value).toLocaleString(undefined, {
    month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit',
  });
}

function isWarehouseCapability(capabilityId: string | null): capabilityId is 'snowflake_sink' | 'bigquery_sink' | 's3_sink' {
  return capabilityId === 'snowflake_sink' || capabilityId === 'bigquery_sink' || capabilityId === 's3_sink';
}

function isSinkCapabilityId(value: string | null): value is SinkCapabilityId {
  return value === 'http_out'
    || value === 'nats_out'
    || value === 'snowflake_sink'
    || value === 'bigquery_sink'
    || value === 's3_sink'
    || value === 'kafka_out';
}

function resolveSink(capabilityId: SinkCapabilityId, connectorId?: string | null): ResolvedSink {
  switch (capabilityId) {
    case 'snowflake_sink':
      return {
        kind: 'snowflake',
        capabilityId,
        connectorId: connectorId?.trim() || 'snowflake_sink_default',
        executionMode: 'adapter',
        deliveryGuarantee: 'idempotent',
      };
    case 'bigquery_sink':
      return {
        kind: 'bigquery',
        capabilityId,
        connectorId: connectorId?.trim() || 'bigquery_sink_default',
        executionMode: 'adapter',
        deliveryGuarantee: 'append_only',
      };
    case 's3_sink':
      return {
        kind: 's3',
        capabilityId,
        connectorId: connectorId?.trim() || 's3_sink_default',
        executionMode: 'adapter',
        deliveryGuarantee: 'append_only',
      };
    case 'kafka_out':
      return {
        kind: 'kafka',
        capabilityId,
        connectorId: connectorId?.trim() || 'kafka_out_default',
        executionMode: 'adapter',
        deliveryGuarantee: 'append_only',
      };
    case 'nats_out':
      return {
        kind: 'nats',
        capabilityId,
        connectorId: connectorId?.trim() || 'nats_out_default',
        executionMode: 'native',
        deliveryGuarantee: 'idempotent',
      };
    default:
      return {
        kind: 'http',
        capabilityId: 'http_out',
        connectorId: connectorId?.trim() || 'http_out_default',
        executionMode: 'native',
        deliveryGuarantee: 'best_effort',
      };
  }
}

function sinkCapabilityIdFromKind(kind: BackendFlowSpec['sinks'][number]['kind'] | undefined): SinkCapabilityId | null {
  switch (kind) {
    case 'snowflake':
      return 'snowflake_sink';
    case 'bigquery':
      return 'bigquery_sink';
    case 's3':
      return 's3_sink';
    case 'kafka':
      return 'kafka_out';
    case 'nats':
      return 'nats_out';
    case 'http':
      return 'http_out';
    default:
      return null;
  }
}

function sourceCapabilityId(kind: SourceKind): string {
  switch (kind) {
    case 'nats':
      return 'nats_in';
    case 'kafka':
      return 'kafka_in';
    default:
      return 'http_in';
  }
}

function buildComposeShellSpec(input: {
  sourceKind: SourceKind;
  name: string;
  sinkCapabilityId: SinkCapabilityId | null;
  sinkConnectorId?: string | null;
}): BackendFlowSpec {
  const sink = resolveSink(input.sinkCapabilityId ?? 'http_out', input.sinkConnectorId);

  return {
    version: 1,
    metadata: {
      tenantId: 'tenant_demo',
      flowId: 'draft_visual_flow',
      revisionId: 'draft_visual_flow_v1',
      name: input.name.trim() || 'Untitled flow',
      description: 'Visual draft',
    },
    sources: [
      {
        id: 'source_ingest',
        kind: input.sourceKind,
        connector: {
          capabilityId: sourceCapabilityId(input.sourceKind),
          connectorId: `${input.sourceKind}_in_draft`,
          executionMode: 'native',
        },
        stream: 'ingress',
        nextNodeIds: ['route_terminal'],
      },
    ],
    processors: [],
    routes: [
      {
        id: 'route_terminal',
        fromNodeId: 'source_ingest',
        predicate: { type: 'always' },
        toSinkIds: ['sink_primary'],
        priority: 100,
      },
    ],
    sinks: [
      {
        id: 'sink_primary',
        kind: sink.kind,
        connector: {
          capabilityId: sink.capabilityId,
          connectorId: sink.connectorId,
          executionMode: sink.executionMode,
        },
        deliveryGuarantee: sink.deliveryGuarantee,
        stream: 'work',
      },
    ],
    retryPolicy: {
      maxAttempts: sink.deliveryGuarantee === 'idempotent' ? 3 : 1,
      initialBackoffMs: 250,
      maxBackoffMs: 5_000,
      multiplier: 2,
      retryableStatusCodes: [408, 429, 500, 502, 503, 504],
    },
    dlqPolicy: {
      enabled: true,
      sinkId: 'sink_primary',
      reasonFormat: 'json',
    },
    batchingPolicy: {
      enabled: sink.kind === 'bigquery' || sink.kind === 'snowflake' || sink.kind === 's3',
      batchSize: 100,
      flushIntervalMs: 5_000,
      keyPath: 'tenantId',
    },
    idempotencyStrategy: sink.deliveryGuarantee === 'best_effort' ? 'partition_key' : 'message_id',
  };
}

function patchBackendSpecDestination(
  spec: BackendFlowSpec,
  capabilityId: SinkCapabilityId,
  connectorId?: string | null,
): BackendFlowSpec {
  const sink = resolveSink(capabilityId, connectorId);
  const next: BackendFlowSpec = JSON.parse(JSON.stringify(spec));
  const currentSinkId = next.sinks[0]?.id ?? 'sink_primary';

  next.sinks = [
    {
      id: currentSinkId,
      kind: sink.kind,
      connector: {
        capabilityId: sink.capabilityId,
        connectorId: sink.connectorId,
        executionMode: sink.executionMode,
      },
      deliveryGuarantee: sink.deliveryGuarantee,
      stream: 'work',
    },
    ...next.sinks.slice(1),
  ];

  if (next.routes.length === 0) {
    next.routes = [
      {
        id: 'route_terminal',
        fromNodeId: next.processors.at(-1)?.id ?? next.sources[0]?.id ?? 'route_terminal',
        predicate: { type: 'always' },
        toSinkIds: [currentSinkId],
        priority: 100,
      },
    ];
  } else {
    next.routes = next.routes.map((route, index) =>
      index === 0 ? { ...route, toSinkIds: [currentSinkId] } : route,
    );
  }

  next.retryPolicy = {
    ...next.retryPolicy,
    maxAttempts: sink.deliveryGuarantee === 'idempotent' ? 3 : 1,
    retryableStatusCodes: next.retryPolicy.retryableStatusCodes ?? [408, 429, 500, 502, 503, 504],
  };
  next.dlqPolicy = {
    ...next.dlqPolicy,
    enabled: true,
    sinkId: currentSinkId,
    reasonFormat: next.dlqPolicy.reasonFormat ?? 'json',
  };
  next.batchingPolicy = {
    enabled: sink.kind === 'snowflake' || sink.kind === 'bigquery' || sink.kind === 's3',
    batchSize: next.batchingPolicy?.batchSize ?? 100,
    flushIntervalMs: next.batchingPolicy?.flushIntervalMs ?? 5_000,
    keyPath: next.batchingPolicy?.keyPath ?? 'tenantId',
  };
  next.idempotencyStrategy = sink.deliveryGuarantee === 'best_effort' ? 'partition_key' : 'message_id';

  return next;
}

function destinationFields(capabilityId: string | null): DestinationField[] {
  switch (capabilityId) {
    case 'snowflake_sink':
      return [
        { key: 'account', label: 'Account', placeholder: 'acme-org.eu-central-1' },
        { key: 'database', label: 'Database', placeholder: 'ANALYTICS' },
        { key: 'schema', label: 'Schema', placeholder: 'PUBLIC' },
        { key: 'table', label: 'Table', placeholder: 'EVENTS_ROUTER_INGEST' },
      ];
    case 'bigquery_sink':
      return [
        { key: 'project', label: 'Project', placeholder: 'acme-analytics' },
        { key: 'jobProject', label: 'Job project', placeholder: 'acme-analytics' },
        { key: 'dataset', label: 'Dataset', placeholder: 'event_router' },
        { key: 'table', label: 'Table', placeholder: 'ingest_events' },
        { key: 'location', label: 'Location', placeholder: 'US' },
        { key: 'writeMethod', label: 'Write method', placeholder: 'storage_write_api' },
        { key: 'maxInFlight', label: 'Max in flight', placeholder: '16' },
        { key: 'batchCount', label: 'Batch count', placeholder: '500' },
        { key: 'batchPeriod', label: 'Batch period', placeholder: '5s' },
        {
          key: 'credentialsJson',
          label: 'Credentials JSON',
          placeholder: '{\n  "type": "service_account",\n  "project_id": "acme-analytics"\n}',
          multiline: true,
          wide: true,
        },
      ];
    case 's3_sink':
      return [
        { key: 'bucket', label: 'Bucket', placeholder: 'event-router-v1' },
        { key: 'prefix', label: 'Prefix', placeholder: 'events/' },
      ];
    default:
      return [];
  }
}

function connectorDraftConfig(connector: ConnectorRecord | null, capabilityId: string | null): Record<string, unknown> {
  const fields = destinationFields(capabilityId);
  const config = connector?.config && typeof connector.config === 'object'
    ? { ...connector.config }
    : {};

  for (const field of fields) {
    if (!(field.key in config)) {
      config[field.key] = '';
    }
  }

  return config;
}

function configFieldValue(config: Record<string, unknown>, key: string): string {
  const value = config[key];
  if (value == null) return '';
  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);
  return formatJson(value);
}

function parseObjectConfig(text: string): { value?: Record<string, unknown>; error?: string } {
  try {
    const value = JSON.parse(text) as unknown;
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
      return { error: 'Connector config must be a JSON object.' };
    }
    return { value: value as Record<string, unknown> };
  } catch (error) {
    return { error: error instanceof Error ? error.message : 'Invalid JSON.' };
  }
}

function normalizeConnectorConfig(config: Record<string, unknown>): Record<string, unknown> {
  const numericFields = new Set(['maxInFlight', 'batchCount', 'batchByteSize']);
  const booleanFields = new Set(['autoDetect', 'ignoreUnknownValues']);
  const normalized: Record<string, unknown> = {};

  for (const [key, value] of Object.entries(config)) {
    if (typeof value === 'string') {
      const trimmed = value.trim();
      if (!trimmed) continue;
      if (numericFields.has(key)) {
        const numberValue = Number(trimmed);
        normalized[key] = Number.isFinite(numberValue) ? numberValue : trimmed;
        continue;
      }
      if (booleanFields.has(key)) {
        normalized[key] = trimmed === 'true' || trimmed === '1' || trimmed.toLowerCase() === 'yes';
        continue;
      }
      normalized[key] = trimmed;
      continue;
    }

    if (value != null) {
      normalized[key] = value;
    }
  }

  return normalized;
}

function currentDraftConnectorId(result: TransformComposerResponse | null): string | null {
  const backendSpec = result?.draft?.backendSpec as
    | { sinks?: Array<{ connector?: { connectorId?: string } }> }
    | undefined;
  return backendSpec?.sinks?.[0]?.connector?.connectorId ?? null;
}

function defaultDemoBusinessExample(): DemoBusinessExample {
  return demoBusinessExamples[0];
}

type TransformSyntaxProcessor =
  | {
      id: string;
      kind: 'map';
      mode?: 'project' | 'merge';
      mappings: Array<{ from: string; to: string }>;
    }
  | {
      id: string;
      kind: 'filter';
      predicate: Record<string, unknown>;
    }
  | {
      id: string;
      kind: 'redact';
      paths: string[];
      mask?: string;
    }
  | {
      id: string;
      kind: 'enrich_static';
      values: Record<string, unknown>;
    }
  | {
      id: string;
      kind: 'enrich_lookup';
      keyPath: string;
      targetPath?: string;
      lookup: {
        mode: 'inline';
        table: Record<string, unknown>;
        missing?: 'skip' | 'null' | 'fail';
      };
    };

type TransformSyntax = {
  version: 1;
  name?: string;
  summary?: string;
  input?: {
    sourceKind: SourceKind;
  };
  processors: TransformSyntaxProcessor[];
  explanation?: string[];
};

function transformSyntaxProcessorsFromSpec(spec: BackendFlowSpec): TransformSyntaxProcessor[] {
  return spec.processors.flatMap((processor): TransformSyntaxProcessor[] => {
    switch (processor.kind) {
      case 'map':
        return [{
          id: processor.id,
          kind: 'map',
          mode: processor.mode ?? 'project',
          mappings: processor.mappings,
        }];
      case 'filter':
        return [{ id: processor.id, kind: 'filter', predicate: processor.predicate as Record<string, unknown> }];
      case 'redact':
        return [{ id: processor.id, kind: 'redact', paths: processor.paths, mask: processor.mask }];
      case 'enrich_static':
        return [{ id: processor.id, kind: 'enrich_static', values: processor.values }];
      case 'enrich_lookup':
        return [{
          id: processor.id,
          kind: 'enrich_lookup',
          keyPath: processor.keyPath,
          targetPath: processor.targetPath,
          lookup: processor.lookup,
        }];
      default:
        return [];
    }
  });
}

function buildTransformSyntaxFromSpec(
  spec: BackendFlowSpec,
  fallbackMappings: Array<{ from: string; to: string }> = [],
): TransformSyntax {
  const processors = transformSyntaxProcessorsFromSpec(spec);

  return {
    version: 1,
    name: spec.metadata.name,
    summary: spec.metadata.description,
    processors: processors.length > 0
      ? processors
      : [{
          id: 'processor_project',
          kind: 'map',
          mode: 'project',
          mappings: fallbackMappings,
        }],
    explanation: [],
  };
}

function buildTransformSyntax(result: TransformComposerResponse): TransformSyntax {
  const backendSpec = result.draft?.backendSpec as BackendFlowSpec | undefined;
  const processors: TransformSyntaxProcessor[] = backendSpec?.processors.length
    ? transformSyntaxProcessorsFromSpec(backendSpec)
    : [{
        id: 'processor_project',
        kind: 'map',
        mode: 'project',
        mappings: result.plan.fieldMappings,
      }];

  return {
    version: 1,
    name: result.plan.suggestedName,
    summary: result.plan.summary,
    processors,
    explanation: result.plan.explanation,
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === 'object' && !Array.isArray(value));
}

function parseStringArray(value: unknown): string[] | null {
  if (!Array.isArray(value)) return null;
  const values = value.map((entry) => (typeof entry === 'string' ? entry.trim() : ''));
  return values.every(Boolean) ? values : null;
}

function parseMappings(value: unknown, path: string): Array<{ from: string; to: string }> {
  if (!Array.isArray(value) || value.length === 0) {
    throw new Error(`${path} must contain at least one mapping.`);
  }

  return value.map((mapping, index) => {
    if (
      !isRecord(mapping) ||
      typeof mapping.from !== 'string' ||
      typeof mapping.to !== 'string' ||
      mapping.from.trim().length === 0 ||
      mapping.to.trim().length === 0
    ) {
      throw new Error(`${path}[${index}] must include non-empty from and to strings.`);
    }

    return { from: mapping.from.trim(), to: mapping.to.trim() };
  });
}

function parseTransformProcessor(value: unknown, index: number): TransformSyntaxProcessor {
  if (!isRecord(value)) {
    throw new Error(`processors[${index}] must be an object.`);
  }

  const kind = value.kind;
  const id = typeof value.id === 'string' && value.id.trim() ? value.id.trim() : `processor_${index + 1}`;

  switch (kind) {
    case 'map': {
      const mode = value.mode === 'merge' ? 'merge' : 'project';
      return {
        id,
        kind,
        mode,
        mappings: parseMappings(value.mappings, `processors[${index}].mappings`),
      };
    }
    case 'filter':
      if (!isRecord(value.predicate)) {
        throw new Error(`processors[${index}].predicate must be an object.`);
      }
      return { id, kind, predicate: value.predicate };
    case 'redact': {
      const paths = parseStringArray(value.paths);
      if (!paths?.length) {
        throw new Error(`processors[${index}].paths must contain at least one path.`);
      }
      return {
        id,
        kind,
        paths,
        mask: typeof value.mask === 'string' ? value.mask : undefined,
      };
    }
    case 'enrich_static':
      if (!isRecord(value.values)) {
        throw new Error(`processors[${index}].values must be an object.`);
      }
      return { id, kind, values: value.values };
    case 'enrich_lookup': {
      if (typeof value.keyPath !== 'string' || value.keyPath.trim().length === 0) {
        throw new Error(`processors[${index}].keyPath must be a non-empty string.`);
      }
      if (value.targetPath !== undefined && (typeof value.targetPath !== 'string' || value.targetPath.trim().length === 0)) {
        throw new Error(`processors[${index}].targetPath must be a non-empty string when provided.`);
      }
      if (!isRecord(value.lookup)) {
        throw new Error(`processors[${index}].lookup must be an object.`);
      }
      if (value.lookup.mode !== 'inline') {
        throw new Error(`processors[${index}].lookup.mode must be inline.`);
      }
      if (!isRecord(value.lookup.table)) {
        throw new Error(`processors[${index}].lookup.table must be an object.`);
      }
      const missing = value.lookup.missing;
      if (missing !== undefined && missing !== 'skip' && missing !== 'null' && missing !== 'fail') {
        throw new Error(`processors[${index}].lookup.missing must be skip, null, or fail.`);
      }
      return {
        id,
        kind,
        keyPath: value.keyPath.trim(),
        targetPath: typeof value.targetPath === 'string' ? value.targetPath.trim() : undefined,
        lookup: {
          mode: 'inline',
          table: value.lookup.table,
          missing,
        },
      };
    }
    default:
      throw new Error(`processors[${index}].kind must be map, filter, redact, enrich_static, or enrich_lookup.`);
  }
}

function parseTransformSyntax(text: string): { value?: TransformSyntax; error?: string } {
  try {
    const parsed = JSON.parse(text) as Record<string, unknown>;
    if (!isRecord(parsed)) {
      return { error: 'Transformation syntax must be a JSON object.' };
    }

    const processors = Array.isArray(parsed.processors)
      ? parsed.processors.map(parseTransformProcessor)
      : [
          {
            id: 'processor_project',
            kind: 'map' as const,
            mode: parsed.mode === 'merge' ? 'merge' as const : 'project' as const,
            mappings: parseMappings(parsed.fieldMappings, 'fieldMappings'),
          },
        ];

    if (processors.length === 0) {
      return { error: 'processors must include at least one processor.' };
    }

    return {
      value: {
        version: 1,
        name: typeof parsed.name === 'string' ? parsed.name.trim() : undefined,
        summary: typeof parsed.summary === 'string' ? parsed.summary.trim() : undefined,
        processors,
        explanation: Array.isArray(parsed.explanation)
          ? parsed.explanation.filter((line): line is string => typeof line === 'string')
          : undefined,
      },
    };
  } catch (error) {
    return { error: error instanceof Error ? error.message : 'Invalid transformation syntax.' };
  }
}

function fieldMappingsFromSyntax(syntax: TransformSyntax): Array<{ from: string; to: string }> {
  return syntax.processors.flatMap((processor) => processor.kind === 'map' ? processor.mappings : []);
}

function readPathValue(value: unknown, path: string): unknown {
  return path.split('.').filter(Boolean).reduce<unknown>((current, segment) => {
    if (!current || typeof current !== 'object') return undefined;
    return (current as Record<string, unknown>)[segment];
  }, value);
}

const preferredLookupKeyPaths = [
  'customer_id',
  'customer.id',
  'customer.user_id',
  'user_id',
  'user.id',
  'anonymous_id',
  'email',
  'customer.email',
];

function collectScalarPaths(value: unknown, prefix = ''): Array<{ path: string; value: string | number | boolean }> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return [];

  return Object.entries(value as Record<string, unknown>).flatMap(([key, nestedValue]) => {
    const path = prefix ? `${prefix}.${key}` : key;
    if (typeof nestedValue === 'string' || typeof nestedValue === 'number' || typeof nestedValue === 'boolean') {
      return [{ path, value: nestedValue }];
    }
    return collectScalarPaths(nestedValue, path);
  });
}

function lookupKeyPathFromPayload(payload: unknown): { path: string; value: string | number | boolean } {
  const scalarPaths = collectScalarPaths(payload);
  for (const candidate of preferredLookupKeyPaths) {
    const value = readPathValue(payload, candidate);
    if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
      return { path: candidate, value };
    }
  }

  return scalarPaths[0] ?? { path: 'customer_id', value: 'sample-customer' };
}

function uniqueProcessorId(processors: TransformSyntaxProcessor[], base: string): string {
  const existing = new Set(processors.map((processor) => processor.id));
  if (!existing.has(base)) return base;
  let index = 2;
  while (existing.has(`${base}_${index}`)) {
    index += 1;
  }
  return `${base}_${index}`;
}

function buildLookupEnrichmentProcessor(
  payload: unknown,
  processors: TransformSyntaxProcessor[],
): TransformSyntaxProcessor {
  const key = lookupKeyPathFromPayload(payload);
  return {
    id: uniqueProcessorId(processors, 'processor_lookup_enrichment'),
    kind: 'enrich_lookup',
    keyPath: key.path,
    targetPath: 'customer.enrichment',
    lookup: {
      mode: 'inline',
      table: {
        [String(key.value)]: {
          risk_band: 'medium',
          loyalty_tier: 'standard',
        },
      },
      missing: 'skip',
    },
  };
}

function writePathValue(target: unknown, path: string, value: unknown): unknown {
  const parts = path.split('.').filter(Boolean);
  if (parts.length === 0) return target;
  const root: Record<string, unknown> =
    target && typeof target === 'object' && !Array.isArray(target)
      ? { ...(target as Record<string, unknown>) }
      : {};
  let cursor = root;

  for (const part of parts.slice(0, -1)) {
    const next = cursor[part];
    const nextObject = next && typeof next === 'object' && !Array.isArray(next)
      ? { ...(next as Record<string, unknown>) }
      : {};
    cursor[part] = nextObject;
    cursor = nextObject;
  }

  cursor[parts[parts.length - 1]] = value;
  return root;
}

function redactPathValue(target: unknown, path: string, mask: string): unknown {
  return typeof readPathValue(target, path) === 'undefined' ? target : writePathValue(target, path, mask);
}

function writeEnrichmentValue(payload: unknown, value: unknown, targetPath?: string): unknown {
  if (targetPath) {
    return writePathValue(payload, targetPath, value);
  }

  if (value && typeof value === 'object' && !Array.isArray(value)) {
    if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
      return { value: payload, ...(value as Record<string, unknown>) };
    }
    return { ...(payload as Record<string, unknown>), ...(value as Record<string, unknown>) };
  }

  return writePathValue(payload, 'enrichment', value);
}

function previewTransformProcessor(payload: unknown, processor: TransformSyntaxProcessor): unknown {
  switch (processor.kind) {
    case 'map':
      return processor.mappings.reduce<unknown>(
        (current, mapping) => {
          const value = readPathValue(payload, mapping.from);
          return typeof value === 'undefined' ? current : writePathValue(current, mapping.to, value);
        },
        processor.mode === 'merge' && payload && typeof payload === 'object'
          ? JSON.parse(JSON.stringify(payload))
          : {},
      );
    case 'redact':
      return processor.paths.reduce<unknown>(
        (current, path) => redactPathValue(current, path, processor.mask ?? '[redacted]'),
        payload && typeof payload === 'object' ? JSON.parse(JSON.stringify(payload)) : payload,
      );
    case 'enrich_static':
      return payload && typeof payload === 'object' && !Array.isArray(payload)
        ? { ...(payload as Record<string, unknown>), ...processor.values }
        : { ...processor.values, value: payload };
    case 'enrich_lookup': {
      const key = readPathValue(payload, processor.keyPath);
      const lookupKey = key == null ? null : String(key);
      const found = lookupKey !== null && Object.prototype.hasOwnProperty.call(processor.lookup.table, lookupKey);
      if (found && lookupKey !== null) {
        return writeEnrichmentValue(payload, processor.lookup.table[lookupKey], processor.targetPath);
      }
      if ((processor.lookup.missing ?? 'skip') === 'null') {
        return writeEnrichmentValue(payload, null, processor.targetPath);
      }
      return payload;
    }
    case 'filter':
      return payload;
  }
}

function previewTransformSyntax(samplePayload: unknown, syntax: TransformSyntax): TransformComposerResponse['preview'] {
  let output = samplePayload;
  for (const processor of syntax.processors) {
    if (processor.kind === 'enrich_lookup' && processor.lookup.missing === 'fail') {
      const key = readPathValue(output, processor.keyPath);
      const lookupKey = key == null ? null : String(key);
      const found = lookupKey !== null && Object.prototype.hasOwnProperty.call(processor.lookup.table, lookupKey);
      if (!found) {
        return {
          accepted: false,
          droppedReason: `Lookup miss on ${processor.keyPath}.`,
          notes: [`Applied ${syntax.processors.length} manual processor${syntax.processors.length === 1 ? '' : 's'}.`],
        };
      }
    }
    output = previewTransformProcessor(output, processor);
  }

  return {
    accepted: true,
    output,
    notes: [`Applied ${syntax.processors.length} manual processor${syntax.processors.length === 1 ? '' : 's'}.`],
  };
}

function toBackendProcessors(syntax: TransformSyntax): BackendFlowSpec['processors'] {
  return syntax.processors.map((processor, index) => {
    const nextNodeIds = index === syntax.processors.length - 1
      ? ['route_terminal']
      : [syntax.processors[index + 1].id];

    switch (processor.kind) {
      case 'map':
        return {
          id: processor.id,
          kind: processor.kind,
          mode: processor.mode ?? 'project',
          mappings: processor.mappings,
          nextNodeIds,
        };
      case 'filter':
        return {
          id: processor.id,
          kind: processor.kind,
          predicate: processor.predicate as BackendFlowSpec['processors'][number] extends { predicate: infer Predicate } ? Predicate : never,
          nextNodeIds,
        };
      case 'redact':
        return {
          id: processor.id,
          kind: processor.kind,
          paths: processor.paths,
          mask: processor.mask,
          nextNodeIds,
        };
      case 'enrich_static':
        return {
          id: processor.id,
          kind: processor.kind,
          values: processor.values,
          nextNodeIds,
        };
      case 'enrich_lookup':
        return {
          id: processor.id,
          kind: processor.kind,
          keyPath: processor.keyPath,
          targetPath: processor.targetPath,
          lookup: processor.lookup,
          nextNodeIds,
        };
    }
  });
}

function patchBackendSpecWithSyntax(spec: BackendFlowSpec, syntax: TransformSyntax): BackendFlowSpec {
  const next: BackendFlowSpec = JSON.parse(JSON.stringify(spec));
  next.metadata.description = syntax.summary || next.metadata.description;
  next.processors = toBackendProcessors(syntax);
  for (const source of next.sources) {
    source.nextNodeIds = [next.processors[0]?.id ?? 'route_terminal'];
  }
  for (const route of next.routes) {
    route.fromNodeId = next.processors.at(-1)?.id ?? 'route_terminal';
  }
  return next;
}

function applyTransformSyntaxToResult(
  result: TransformComposerResponse,
  syntax: TransformSyntax,
  samplePayload: unknown,
): TransformComposerResponse {
  const patchedBackendSpec = result.draft?.backendSpec
    ? patchBackendSpecWithSyntax(result.draft.backendSpec as BackendFlowSpec, syntax)
    : null;
  const fieldMappings = fieldMappingsFromSyntax(syntax);

  return {
    ...result,
    plan: {
      ...result.plan,
      summary: syntax.summary || result.plan.summary,
      fieldMappings,
      explanation: syntax.explanation?.length
        ? syntax.explanation
        : [`Applied ${syntax.processors.length} manual processor${syntax.processors.length === 1 ? '' : 's'}.`],
    },
    preview: previewTransformSyntax(samplePayload, syntax),
    draft: patchedBackendSpec ? mapBackendDraft(patchedBackendSpec, samplePayload) : result.draft,
  };
}

function sourceKindFromBackendSpec(spec: BackendFlowSpec): SourceKind {
  const kind = spec.sources[0]?.kind;
  return kind === 'nats' || kind === 'kafka' ? kind : 'http';
}

function sourceBindingFromBackendSpec(spec: BackendFlowSpec): TransformComposerResponse['sourceBinding'] | undefined {
  const source = spec.sources[0];
  const sourceKind = sourceKindFromBackendSpec(spec);
  if (!source) return undefined;

  return {
    sourceKind,
    capabilityId: source.connector.capabilityId as NonNullable<TransformComposerResponse['sourceBinding']>['capabilityId'],
    executionMode: source.connector.executionMode,
    connectorId: source.connector.connectorId,
    connectorName: source.connector.connectorId,
    ref: source.connector.connectorId,
    config: {},
    generated: false,
  };
}

function sinkExportOptionsFromCapabilities(capabilities: CapabilityData | undefined): TransformComposerResponse['exportOptions'] {
  return [...(capabilities?.native ?? []), ...(capabilities?.adapter ?? [])]
    .filter((capability) => capability.mode === 'sink');
}

function fieldMappingsFromSpec(spec: BackendFlowSpec): Array<{ from: string; to: string }> {
  return spec.processors.flatMap((processor) => processor.kind === 'map' ? processor.mappings : []);
}

function buildComposerResultFromBackendSpec(
  spec: BackendFlowSpec,
  samplePayload: unknown,
  capabilities: CapabilityData | undefined,
): TransformComposerResponse {
  const fieldMappings = fieldMappingsFromSpec(spec);
  const draft = mapBackendDraft(spec, samplePayload);
  const selectedSinkCapabilityId = sinkCapabilityIdFromKind(spec.sinks[0]?.kind);
  const preview = previewTransformSyntax(samplePayload, buildTransformSyntaxFromSpec(spec, fieldMappings));

  return {
    assistant: {
      provider: 'existing-flow',
      model: spec.metadata.revisionId,
      note: 'Loaded from the active flow revision.',
    },
    plan: {
      suggestedName: spec.metadata.name,
      summary: spec.metadata.description || mappingSummary(fieldMappings),
      fieldMappings,
      explanation: [],
      recommendedExportIds: selectedSinkCapabilityId ? [selectedSinkCapabilityId] : [],
    },
    preview,
    exportOptions: sinkExportOptionsFromCapabilities(capabilities),
    sourceBinding: sourceBindingFromBackendSpec(spec),
    draft,
  };
}

function truncateJson(value: unknown, maxLen = 80): string {
  const s = JSON.stringify(value);
  if (!s || s.length <= maxLen) return s ?? '';
  return s.slice(0, maxLen) + '\u2026';
}

function sampleOriginTone(
  origin: SampleOrigin,
): 'good' | 'info' | 'warn' | 'neutral' {
  switch (origin) {
    case 'live':
      return 'good';
    case 'demo':
      return 'good';
    case 'manual':
      return 'info';
    default:
      return 'neutral';
  }
}

function sampleOriginLabel(origin: SampleOrigin): string {
  switch (origin) {
    case 'live':
      return 'Live ingest sample';
    case 'demo':
      return 'Demo store event';
    case 'manual':
      return 'Manual JSON';
    default:
      return 'No sample selected';
  }
}

function sourceBindingHint(
  binding: NonNullable<TransformComposerResponse['sourceBinding']>,
): string {
  switch (binding.sourceKind) {
    case 'kafka':
      return 'Publish new events to this Kafka topic.';
    case 'nats':
      return 'Publish new events to this NATS subject.';
    default:
      return 'POST new JSON events to this HTTP path.';
  }
}

function eventLabel(name: string): string {
  return name.replaceAll('_', ' ');
}

function eventNameFromPayload(payload: unknown): string {
  if (!isRecord(payload)) return 'event';
  const value = payload.event_name ?? payload.eventName ?? payload.type;
  return typeof value === 'string' ? value : 'event';
}

function eventShapeKey(payload: unknown): string {
  const eventName = eventNameFromPayload(payload);
  if (eventName !== 'event') return `event:${eventName}`;
  if (!isRecord(payload)) return 'event:unknown';
  return `shape:${Object.keys(payload).sort().join('|') || 'empty'}`;
}

function runtimeSampleShapeId(sample: RuntimeSampleRecord): string {
  return `live-${sample.sourceKind}:${sample.flowId}:${eventShapeKey(sample.payload)}`;
}

function groupRuntimeSampleShapes(samples: RuntimeSampleRecord[] | undefined): RuntimeSampleShape[] {
  const byShape = new Map<string, RuntimeSampleShape>();

  for (const sample of samples ?? []) {
    const key = runtimeSampleShapeId(sample);
    const existing = byShape.get(key);
    if (existing) {
      existing.count += 1;
      continue;
    }
    byShape.set(key, {
      id: key,
      label: eventLabel(eventNameFromPayload(sample.payload)),
      sample,
      count: 1,
    });
  }

  return Array.from(byShape.values());
}

function groupDemoEventShapes(events: DemoEventRecord[]): DemoEventShape[] {
  const byShape = new Map<string, DemoEventShape>();

  for (const event of events) {
    const existing = byShape.get(event.name);
    if (existing) {
      existing.count += 1;
      continue;
    }
    byShape.set(event.name, {
      id: `demo-shape-${event.name}`,
      label: eventLabel(event.name),
      event,
      count: 1,
    });
  }

  return Array.from(byShape.values());
}

function sampleShapeLabel(count: number): string {
  return count === 1 ? '1 sample' : `${count} samples`;
}

function buildJsonDiff(before: unknown, after: unknown): DiffRow[] {
  const beforeMap = flattenJson(before);
  const afterMap = flattenJson(after);
  const paths = Array.from(new Set([...beforeMap.keys(), ...afterMap.keys()])).sort((a, b) => a.localeCompare(b));

  return paths.map((path) => {
    const beforeValue = beforeMap.get(path);
    const afterValue = afterMap.get(path);
    if (typeof beforeValue === 'undefined') {
      return { path, after: afterValue, kind: 'added' };
    }
    if (typeof afterValue === 'undefined') {
      return { path, before: beforeValue, kind: 'removed' };
    }
    if (beforeValue !== afterValue) {
      return { path, before: beforeValue, after: afterValue, kind: 'changed' };
    }
    return { path, before: beforeValue, after: afterValue, kind: 'unchanged' };
  });
}

function diffKindLabel(kind: DiffRow['kind']): string {
  switch (kind) {
    case 'added':
      return 'Added';
    case 'removed':
      return 'Deleted';
    case 'changed':
      return 'Transformed';
    default:
      return 'Kept';
  }
}

function mappingSummary(mappings: Array<{ from: string; to: string }>): string {
  if (mappings.length === 0) return 'Pass the sample through without field projection.';
  const renamedCount = mappings.filter((mapping) => mapping.from !== mapping.to).length;
  if (renamedCount === 0) {
    return `Keep ${mappings.length} field${mappings.length === 1 ? '' : 's'} from the sample.`;
  }
  return `Project ${mappings.length} field${mappings.length === 1 ? '' : 's'} into the output, including ${renamedCount} renamed path${renamedCount === 1 ? '' : 's'}.`;
}

function flattenJson(value: unknown, path = '$', rows = new Map<string, string>()): Map<string, string> {
  if (Array.isArray(value)) {
    if (value.length === 0) {
      rows.set(path, '[]');
      return rows;
    }
    value.forEach((item, index) => flattenJson(item, `${path}[${index}]`, rows));
    return rows;
  }

  if (isRecord(value)) {
    const entries = Object.entries(value);
    if (entries.length === 0) {
      rows.set(path, '{}');
      return rows;
    }
    entries.forEach(([key, item]) => flattenJson(item, `${path}.${key}`, rows));
    return rows;
  }

  rows.set(path, JSON.stringify(value));
  return rows;
}

export function AuthoringPage() {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const {
    currentStep: wizardStep,
    setCurrentStep: setWizardStep,
    updateStepNavigation,
  } = useComposeStepNavigation();
  const [designerPanel, setDesignerPanel] = useState<ComposeDesignerPanel>('ingest');
  const [sourceKind, setSourceKind] = useState<SourceKind>('http');
  const [instruction, setInstruction] = useState('');
  const [name, setName] = useState('');
  const [samplePayloadText, setSamplePayloadText] = useState('');
  const [selectedSampleId, setSelectedSampleId] = useState<string | null>(null);
  const [showJsonEditor, setShowJsonEditor] = useState(false);
  const [showSourcePicker, setShowSourcePicker] = useState(false);
  const [composerResult, setComposerResult] = useState<TransformComposerResponse | null>(null);
  const [selectedExportId, setSelectedExportId] = useState<string | null>(null);
  const [selectedConnectorId, setSelectedConnectorId] = useState<string | null>(null);
  const [destinationName, setDestinationName] = useState('');
  const [destinationConfig, setDestinationConfig] = useState<Record<string, unknown>>({});
  const [destinationConfigText, setDestinationConfigText] = useState('{}');
  const [destinationConfigError, setDestinationConfigError] = useState<string | null>(null);
  const [isEditingDestinationConfig, setIsEditingDestinationConfig] = useState(false);
  const [publishResult, setPublishResult] = useState<{
    flowId: string; revisionId: string; deploymentId: string;
  } | null>(null);
  const [demoEvents, setDemoEvents] = useState<DemoEventRecord[]>(() => readDemoEventHistory());
  const [loadedDemoEvent, setLoadedDemoEvent] = useState<DemoEventRecord | null>(null);
  const [selectedExampleId, setSelectedExampleId] = useState(() =>
    (readSelectedDemoBusinessExample() ?? defaultDemoBusinessExample()).id,
  );
  const [isEditingTransform, setIsEditingTransform] = useState(false);
  const [transformSyntaxText, setTransformSyntaxText] = useState('');
  const [transformSyntaxError, setTransformSyntaxError] = useState<string | null>(null);
  const [appliedTransformSyntax, setAppliedTransformSyntax] = useState<TransformSyntax | null>(null);
  const [reviewPane, setReviewPane] = useState<ReviewPane>('sample');
  const [hydratedFlowId, setHydratedFlowId] = useState<string | null>(null);
  const hasUserEditedSample = useRef(false);
  const editingFlowId = readEditingFlowId();

  const liveSamplesQuery = useQuery({
    queryKey: ['runtime-samples', sourceKind],
    queryFn: () => api.fetchRuntimeSamples({ sourceKind, limit: 40 }),
  });

  const flowsQuery = useQuery({
    queryKey: ['flows'],
    queryFn: api.fetchFlows,
    enabled: Boolean(editingFlowId),
  });
  const capabilitiesQuery = useQuery({
    queryKey: ['capabilities'],
    queryFn: api.fetchCapabilities,
    enabled: Boolean(editingFlowId),
  });
  const connectorsQuery = useQuery({
    queryKey: ['connectors', selectedExportId],
    queryFn: () => api.fetchConnectors({ capabilityId: selectedExportId ?? undefined, tenantId: 'tenant_demo' }),
    enabled: Boolean(selectedExportId && isWarehouseCapability(selectedExportId)),
  });
  const aiSettingsQuery = useQuery({ queryKey: ['ai-settings'], queryFn: api.fetchAiSettings });
  const demoEventShapes = useMemo(() => groupDemoEventShapes(demoEvents), [demoEvents]);
  const runtimeSampleShapes = useMemo(() => groupRuntimeSampleShapes(liveSamplesQuery.data), [liveSamplesQuery.data]);

  useEffect(() => {
    if (hasUserEditedSample.current) return;
    const history = readDemoEventHistory();
    setDemoEvents(history);
    const latest = readLatestDemoEvent();
    const demoEvent = history.find((event) => event.id === latest?.id) ?? history[0] ?? latest;
    if (!demoEvent) return;

    hasUserEditedSample.current = true;
    setLoadedDemoEvent(demoEvent);
    setSourceKind('http');
    setSamplePayloadText(formatJson(demoEvent.payload));
    setSelectedSampleId(`demo-shape-${demoEvent.name}`);
    setShowJsonEditor(false);
    const selectedExample = readSelectedDemoBusinessExample() ?? defaultDemoBusinessExample();
    setSelectedExampleId(selectedExample.id);
    setInstruction((current) =>
      current || selectedExample.prompt,
    );
    setName((current) => current || 'Demo store ecommerce export');
  }, []);

  useEffect(() => {
    if (hasUserEditedSample.current) return;
    const firstShape = runtimeSampleShapes[0];
    if (!firstShape) return;
    setSamplePayloadText(formatJson(firstShape.sample.payload));
    setSelectedSampleId(firstShape.id);
  }, [runtimeSampleShapes]);

  const samplePayload = parseSamplePayload(samplePayloadText);

  const selectSample = (sample: RuntimeSampleRecord) => {
    hasUserEditedSample.current = true;
    setLoadedDemoEvent(null);
    setWizardStep('build');
    setSamplePayloadText(formatJson(sample.payload));
    setSelectedSampleId(runtimeSampleShapeId(sample));
    setShowJsonEditor(false);
    setComposerResult(null);
    setPublishResult(null);
    setAppliedTransformSyntax(null);
    setIsEditingTransform(false);
    setDesignerPanel('sample');
  };

  const switchSource = (kind: SourceKind) => {
    hasUserEditedSample.current = false;
    setLoadedDemoEvent(null);
    setWizardStep('build');
    setSourceKind(kind);
    setComposerResult(null);
    setPublishResult(null);
    setSamplePayloadText('');
    setSelectedSampleId(null);
    setAppliedTransformSyntax(null);
    setIsEditingTransform(false);
    setDesignerPanel('ingest');
  };

  const selectDemoEvent = (event: DemoEventRecord) => {
    hasUserEditedSample.current = true;
    setWizardStep('build');
    setLoadedDemoEvent(event);
    setSourceKind('http');
    setSamplePayloadText(formatJson(event.payload));
    setSelectedSampleId(`demo-shape-${event.name}`);
    setShowJsonEditor(false);
    setShowSourcePicker(false);
    setComposerResult(null);
    setPublishResult(null);
    setAppliedTransformSyntax(null);
    setIsEditingTransform(false);
    setDesignerPanel('sample');
  };

  const designMutation = useMutation({
    mutationFn: async () => {
      const payload = requireSamplePayload(samplePayload);
      return api.composeJsonTransform({ prompt: instruction, samplePayload: payload, sourceKind, name: name.trim() || undefined, tenantId: 'tenant_demo' });
    },
    onSuccess: (result) => {
      setPublishResult(null);
      setSelectedExportId(null);
      setComposerResult(result);
      setIsEditingTransform(false);
      setTransformSyntaxError(null);
      setAppliedTransformSyntax(null);
      setReviewPane('sample');
      setWizardStep('review');
      setDesignerPanel('transform');
      if (!name.trim()) setName(result.plan.suggestedName);
    },
  });

  const exportMutation = useMutation({
    mutationFn: async (input: { sinkCapabilityId: string; sinkConnectorId?: string }) => {
      const payload = requireSamplePayload(samplePayload);
      return api.composeJsonTransform({
        prompt: instruction,
        samplePayload: payload,
        sourceKind,
        sinkCapabilityId: input.sinkCapabilityId,
        sinkConnectorId: input.sinkConnectorId,
        name: name.trim() || undefined,
        tenantId: 'tenant_demo',
      });
    },
    onSuccess: (result, input) => {
      setPublishResult(null);
      setSelectedExportId(input.sinkCapabilityId);
      if (input.sinkConnectorId) {
        setSelectedConnectorId(input.sinkConnectorId);
      }
      setComposerResult(
        appliedTransformSyntax
          ? applyTransformSyntaxToResult(result, appliedTransformSyntax, samplePayload.value)
          : result,
      );
      setWizardStep('deliver');
      setDesignerPanel('destination');
      if (!name.trim()) setName(result.plan.suggestedName);
    },
  });

  const saveConnectorMutation = useMutation({
    mutationFn: async () => {
      if (!selectedExportId || !isWarehouseCapability(selectedExportId)) {
        throw new Error('Choose a configurable destination first.');
      }

      let config = destinationConfig;
      if (isEditingDestinationConfig) {
        const parsed = parseObjectConfig(destinationConfigText);
        if (parsed.error || !parsed.value) {
          throw new Error(parsed.error ?? 'Invalid connector config.');
        }
        config = parsed.value;
        setDestinationConfig(parsed.value);
        setDestinationConfigText(formatJson(parsed.value));
        setDestinationConfigError(null);
      }

      return api.saveConnector({
        id: selectedConnectorId ?? undefined,
        name: destinationName.trim() || `${sourceLabel(sourceKind)} ${selectedExportId} destination`,
        capabilityId: selectedExportId,
        tenantId: 'tenant_demo',
        config: normalizeConnectorConfig(config),
      });
    },
    onSuccess: async (connector) => {
      setSelectedConnectorId(connector.id);
      setDestinationName(connector.name);
      setDestinationConfig(connectorDraftConfig(connector, selectedExportId));
      setDestinationConfigText(formatJson(connectorDraftConfig(connector, selectedExportId)));
      setDestinationConfigError(null);
      await queryClient.invalidateQueries({ queryKey: ['connectors', selectedExportId] });
      if (selectedExportId) {
        if (isSinkCapabilityId(selectedExportId) && applyDestinationToCurrentDraft(selectedExportId, connector.id)) {
          return;
        }
        exportMutation.mutate({
          sinkCapabilityId: selectedExportId,
          sinkConnectorId: connector.id,
        });
      }
    },
  });

  const publishMutation = useMutation({
    mutationFn: async () => {
      if (!composerResult?.draft) throw new Error('Choose a destination first.');
      const payload = requireSamplePayload(samplePayload);
      return api.publishDraft({ draft: composerResult.draft, name: name.trim() || composerResult.plan.suggestedName, tenantId: 'tenant_demo', samplePayload: payload });
    },
    onSuccess: async (result) => {
      setPublishResult(result);
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ['flows'] }),
        queryClient.invalidateQueries({ queryKey: ['overview'] }),
        queryClient.invalidateQueries({ queryKey: ['runtime-stats'] }),
      ]);
      await navigate({ to: '/flows/$flowId', params: { flowId: result.flowId } });
    },
  });

  const isBusy = designMutation.isPending || exportMutation.isPending || publishMutation.isPending;
  const hasSample = typeof samplePayload.value !== 'undefined' && !samplePayload.error;
  const hasInstruction = instruction.trim().length > 0;
  const selectedSample =
    runtimeSampleShapes.find((shape) => shape.id === selectedSampleId)?.sample ?? null;
  const sampleOrigin: SampleOrigin = loadedDemoEvent
    ? 'demo'
    : showJsonEditor
      ? 'manual'
      : selectedSample
        ? 'live'
        : null;
  const selectedConnector =
    connectorsQuery.data?.find((connector) => connector.id === selectedConnectorId) ?? null;
  const configurableDestination = isWarehouseCapability(selectedExportId);
  const aiSettings = aiSettingsQuery.data;
  const selectedBusinessExample =
    demoBusinessExamples.find((example) => example.id === selectedExampleId) ?? defaultDemoBusinessExample();
  const isEditingExistingFlow = Boolean(editingFlowId);

  useEffect(() => {
    if (!editingFlowId || hydratedFlowId === editingFlowId || !flowsQuery.data || !capabilitiesQuery.data) {
      return;
    }

    const flow = flowsQuery.data.find((item) => item.id === editingFlowId);
    const backendSpec = flow?.backendSpec as BackendFlowSpec | undefined;
    if (!flow || !backendSpec) return;

    const payload = samplePayload.error || typeof samplePayload.value === 'undefined' ? {} : samplePayload.value;
    const source = sourceKindFromBackendSpec(backendSpec);
    const sinkCapabilityId = sinkCapabilityIdFromKind(backendSpec.sinks[0]?.kind);
    const sinkConnectorId = backendSpec.sinks[0]?.connector.connectorId ?? null;

    hasUserEditedSample.current = true;
    setSourceKind(source);
    if (!samplePayloadText.trim()) {
      setSamplePayloadText(formatJson(payload));
    }
    setName(flow.name);
    setInstruction(backendSpec.metadata.description || mappingSummary(fieldMappingsFromSpec(backendSpec)));
    setComposerResult(buildComposerResultFromBackendSpec(backendSpec, payload, capabilitiesQuery.data));
    setSelectedExportId(sinkCapabilityId);
    setSelectedConnectorId(sinkCapabilityId && isWarehouseCapability(sinkCapabilityId) ? sinkConnectorId : null);
    setPublishResult(null);
    setAppliedTransformSyntax(buildTransformSyntaxFromSpec(backendSpec, fieldMappingsFromSpec(backendSpec)));
    setIsEditingTransform(false);
    setReviewPane('sample');
    setWizardStep('deliver');
    setDesignerPanel('destination');
    setHydratedFlowId(editingFlowId);
  }, [
    capabilitiesQuery.data,
    editingFlowId,
    flowsQuery.data,
    hydratedFlowId,
    samplePayload.error,
    samplePayload.value,
    samplePayloadText,
    setWizardStep,
  ]);

  const applyBusinessExample = (example: DemoBusinessExample) => {
    rememberSelectedDemoBusinessExample(example.id);
    setSelectedExampleId(example.id);
    setInstruction(example.prompt);
    setComposerResult(null);
    setPublishResult(null);
    setIsEditingTransform(false);
    setTransformSyntaxError(null);
    setAppliedTransformSyntax(null);
    setWizardStep('build');
    setDesignerPanel('transform');
  };

  const openTransformEditor = () => {
    if (!composerResult) return;
    setTransformSyntaxText(formatJson(buildTransformSyntax(composerResult)));
    setTransformSyntaxError(null);
    setIsEditingTransform(true);
    setReviewPane('edit');
  };

  const insertLookupEnrichment = () => {
    if (!hasSample) {
      setTransformSyntaxError('Select a valid sample before adding lookup enrichment.');
      return;
    }

    const parsed = parseTransformSyntax(transformSyntaxText);
    if (parsed.error || !parsed.value) {
      setTransformSyntaxError(parsed.error ?? 'Invalid transformation syntax.');
      return;
    }

    const processor = buildLookupEnrichmentProcessor(samplePayload.value, parsed.value.processors);
    setTransformSyntaxText(formatJson({
      ...parsed.value,
      processors: [...parsed.value.processors, processor],
    }));
    setTransformSyntaxError(null);
  };

  const applyTransformEditor = () => {
    if (!composerResult) return;
    if (!hasSample) {
      setTransformSyntaxError('Select a valid sample before applying syntax.');
      return;
    }

    const parsed = parseTransformSyntax(transformSyntaxText);
    if (parsed.error || !parsed.value) {
      setTransformSyntaxError(parsed.error ?? 'Invalid transformation syntax.');
      return;
    }

    try {
      setComposerResult(applyTransformSyntaxToResult(composerResult, parsed.value, samplePayload.value));
      setTransformSyntaxError(null);
      setIsEditingTransform(false);
      setAppliedTransformSyntax(parsed.value);
      setReviewPane('sample');
      setPublishResult(null);
    } catch (error) {
      setTransformSyntaxError(error instanceof Error ? error.message : 'Unable to apply transformation syntax.');
    }
  };

  const applySyntaxToCurrentDraft = (syntax: TransformSyntax) => {
    if (!composerResult || !hasSample) return;
    setComposerResult(applyTransformSyntaxToResult(composerResult, syntax, samplePayload.value));
    setAppliedTransformSyntax(syntax);
    setTransformSyntaxText(formatJson(syntax));
    setTransformSyntaxError(null);
    setIsEditingTransform(false);
    setReviewPane('sample');
    setPublishResult(null);
    setWizardStep('review');
    setDesignerPanel('transform');
  };

  const addLookupStepToDraft = () => {
    if (!composerResult || !hasSample) {
      setTransformSyntaxError('Create a transform from a valid sample before adding lookup enrichment.');
      return;
    }

    try {
      const syntax = appliedTransformSyntax ?? buildTransformSyntax(composerResult);
      applySyntaxToCurrentDraft({
        ...syntax,
        processors: [...syntax.processors, buildLookupEnrichmentProcessor(samplePayload.value, syntax.processors)],
      });
    } catch (error) {
      setTransformSyntaxError(error instanceof Error ? error.message : 'Unable to add lookup enrichment.');
    }
  };

  const addFilterStepToDraft = () => {
    if (!composerResult || !hasSample) {
      setTransformSyntaxError('Create a transform from a valid sample before adding a filter.');
      return;
    }

    const eventName = eventNameFromPayload(samplePayload.value);
    const syntax = appliedTransformSyntax ?? buildTransformSyntax(composerResult);
    applySyntaxToCurrentDraft({
      ...syntax,
      processors: [
        ...syntax.processors,
        {
          id: uniqueProcessorId(syntax.processors, 'processor_filter'),
          kind: 'filter',
          predicate: eventName === 'event'
            ? { type: 'field_exists', path: 'event_name' }
            : { type: 'field_equals', path: 'event_name', value: eventName },
        },
      ],
    });
  };

  const openDesignerPanel = (panel: ComposeDesignerPanel) => {
    setDesignerPanel(panel);
    if (panel === 'ingest' || panel === 'sample') {
      setWizardStep('build');
      return;
    }
    if (panel === 'destination') {
      setWizardStep(composerResult ? 'deliver' : 'build');
      return;
    }
    setWizardStep(composerResult ? 'review' : 'build');
    if (panel === 'advanced' && composerResult && !isEditingTransform) {
      openTransformEditor();
    }
  };

  const openDestinationConfigEditor = () => {
    setDestinationConfigText(formatJson(destinationConfig));
    setDestinationConfigError(null);
    setIsEditingDestinationConfig(true);
  };

  const applyDestinationConfigEditor = () => {
    const parsed = parseObjectConfig(destinationConfigText);
    if (parsed.error || !parsed.value) {
      setDestinationConfigError(parsed.error ?? 'Invalid connector config.');
      return;
    }

    setDestinationConfig(parsed.value);
    setDestinationConfigText(formatJson(parsed.value));
    setDestinationConfigError(null);
    setIsEditingDestinationConfig(false);
  };

  const applyDestinationToCurrentDraft = (capabilityId: SinkCapabilityId, connectorId?: string | null): boolean => {
    if (!composerResult?.draft?.backendSpec) return false;

    const payload = samplePayload.error || typeof samplePayload.value === 'undefined' ? {} : samplePayload.value;
    setComposerResult((current) => {
      const backendSpec = current?.draft?.backendSpec as BackendFlowSpec | undefined;
      if (!current || !backendSpec) return current;
      const patchedSpec = patchBackendSpecDestination(backendSpec, capabilityId, connectorId);
      return {
        ...current,
        plan: {
          ...current.plan,
          recommendedExportIds: [capabilityId],
        },
        draft: mapBackendDraft(patchedSpec, payload),
      };
    });
    setSelectedExportId(capabilityId);
    setSelectedConnectorId(isWarehouseCapability(capabilityId) ? connectorId ?? null : null);
    setPublishResult(null);
    setWizardStep('deliver');
    return true;
  };

  const chooseExportOption = (capabilityId: string) => {
    if (!isSinkCapabilityId(capabilityId)) return;
    setDesignerPanel('destination');
    const connectorId = isWarehouseCapability(capabilityId) && selectedExportId === capabilityId
      ? selectedConnectorId
      : null;

    if (applyDestinationToCurrentDraft(capabilityId, connectorId)) {
      return;
    }

    exportMutation.mutate({
      sinkCapabilityId: capabilityId,
      sinkConnectorId: connectorId ?? undefined,
    });
  };

  useEffect(() => {
    if (!configurableDestination) {
      setSelectedConnectorId(null);
      setDestinationName('');
      setDestinationConfig({});
      setDestinationConfigText('{}');
      setDestinationConfigError(null);
      setIsEditingDestinationConfig(false);
      return;
    }

    const connectors = connectorsQuery.data ?? [];
    const connector = connectors.find((item) => item.id === selectedConnectorId) ?? null;

    if (!connector) {
      const blankConfig = connectorDraftConfig(null, selectedExportId);
      setSelectedConnectorId(null);
      setDestinationName(`${name.trim() || sourceLabel(sourceKind)} ${selectedExportId} destination`);
      setDestinationConfig(blankConfig);
      setDestinationConfigText(formatJson(blankConfig));
      setDestinationConfigError(null);
      return;
    }

    if (selectedConnectorId !== connector.id) {
      setSelectedConnectorId(connector.id);
    }
    const connectorConfig = connectorDraftConfig(connector, selectedExportId);
    setDestinationName(connector.name);
    setDestinationConfig(connectorConfig);
    setDestinationConfigText(formatJson(connectorConfig));
    setDestinationConfigError(null);
  }, [configurableDestination, connectorsQuery.data, name, selectedConnectorId, selectedExportId, sourceKind]);

  useEffect(() => {
    if (!configurableDestination || !selectedExportId || !selectedConnectorId || !composerResult) {
      return;
    }

    const draftConnectorId = currentDraftConnectorId(composerResult);
    if (draftConnectorId === selectedConnectorId) {
      return;
    }

    if (isSinkCapabilityId(selectedExportId) && applyDestinationToCurrentDraft(selectedExportId, selectedConnectorId)) {
      return;
    }

    exportMutation.mutate({
      sinkCapabilityId: selectedExportId,
      sinkConnectorId: selectedConnectorId,
    });
  }, [composerResult, configurableDestination, selectedConnectorId, selectedExportId]);

  const eventPickerOpen = showSourcePicker || (!loadedDemoEvent && !showJsonEditor);
  const diffRows = composerResult?.preview.accepted
    ? buildJsonDiff(samplePayload.value, composerResult.preview.output)
    : [];
  const diffCounts = diffRows.reduce<Record<DiffRow['kind'], number>>(
    (counts, row) => ({ ...counts, [row.kind]: counts[row.kind] + 1 }),
    { added: 0, removed: 0, changed: 0, unchanged: 0 },
  );
  const changedMappings = composerResult?.plan.fieldMappings.filter((mapping) => mapping.from !== mapping.to) ?? [];
  const reviewSummary = composerResult ? mappingSummary(composerResult.plan.fieldMappings) : '';
  const activeEventName = loadedDemoEvent
    ? eventLabel(loadedDemoEvent.name)
    : eventNameFromPayload(samplePayload.value);
  const completedWizardIds: WizardStep[] = composerResult
    ? wizardStep === 'deliver'
      ? ['build', 'review']
      : wizardStep === 'review'
        ? ['build']
        : []
    : [];
  const availableWizardIds: WizardStep[] = composerResult ? ['build', 'review', 'deliver'] : ['build'];
  const workflowSpec = composerResult?.draft?.backendSpec as BackendFlowSpec | undefined;
  const workflowPreviewSpec = useMemo(
    () => workflowSpec ?? buildComposeShellSpec({
      sourceKind,
      name,
      sinkCapabilityId: isSinkCapabilityId(selectedExportId) ? selectedExportId : null,
      sinkConnectorId: selectedConnectorId,
    }),
    [name, selectedConnectorId, selectedExportId, sourceKind, workflowSpec],
  );
  const selectedDesignerNodeId =
    designerPanel === 'ingest'
      ? `source:${workflowPreviewSpec.sources[0]?.id ?? 'source_ingest'}`
      : designerPanel === 'destination'
        ? `sink:${workflowPreviewSpec.sinks[0]?.id ?? 'sink_primary'}`
        : designerPanel === 'transform' && workflowPreviewSpec.processors[0]
          ? `processor:${workflowPreviewSpec.processors[0].id}`
          : null;

  useEffect(() => {
    if (!composerResult && wizardStep !== 'build') {
      setWizardStep('build');
    }
  }, [composerResult, setWizardStep, wizardStep]);

  useEffect(() => {
    updateStepNavigation({
      currentStep: wizardStep,
      completedSteps: completedWizardIds,
      availableSteps: availableWizardIds,
    });
  }, [composerResult, updateStepNavigation, wizardStep]);

  return (
    <VStack gap={5}>
      <HStack justify="between" align="center" gap={4} wrap="wrap">
        <VStack gap={1}>
          <Text type="supporting" color="secondary" weight="semibold" display="block">VISUAL BUILDER</Text>
          <Heading level={1}>{isEditingExistingFlow ? 'Edit flow' : 'New flow'}</Heading>
          <Text type="supporting" color="secondary" display="block">
            Build the workflow directly: ingest, transform, enrich, and choose a destination.
          </Text>
        </VStack>
        <HStack gap={2} align="center" wrap="wrap" justify="end">
          <Link to="/setup" className="ai-provider-link">
            <Icon icon={aiSettings?.activeProvider === 'gemini' ? SparklesIcon : CommandLineIcon} size="xsm" />
            <span>{aiSettings ? assistantLabel(aiSettings.activeProvider) : 'AI setup'}</span>
          </Link>
          <TextInput
            label="Flow name"
            isLabelHidden
            value={name}
            onChange={(value) => setName(value)}
            placeholder="Name your flow"
          />
          <ActionButton
            variant="primary"
            type="button"
            onClick={() => publishMutation.mutate()}
            disabled={isBusy || !composerResult?.draft || !selectedExportId}
          >
            Publish
          </ActionButton>
        </HStack>
      </HStack>

      <SegmentedControl
        value={designerPanel}
        onChange={(value) => openDesignerPanel(value as ComposeDesignerPanel)}
        label="Flow builder sections"
        layout="fill"
      >
        {[
          { id: 'ingest' as const, label: 'Ingest', ready: hasSample },
          { id: 'sample' as const, label: 'Sample', ready: hasSample },
          { id: 'transform' as const, label: 'Transform', ready: Boolean(composerResult) },
          { id: 'destination' as const, label: 'Destination', ready: Boolean(selectedExportId) },
          { id: 'advanced' as const, label: 'Advanced', ready: Boolean(composerResult) },
        ].map((item) => (
          <SegmentedControlItem
            key={item.id}
            value={item.id}
            label={item.label}
            icon={item.ready ? <Icon icon={CheckIcon} size="xsm" /> : undefined}
          />
        ))}
      </SegmentedControl>

      <div className="compose-designer-grid">
        <div className="compose-designer-main">
          <Panel className="compose-designer-canvas-panel">
            <div className="pipeline-preview-head">
              <div>
                <p className="eyebrow">Workflow</p>
                <h3 className="panel-title">{composerResult?.plan.suggestedName || name || 'Untitled flow'}</h3>
              </div>
              <div className="assistant-provider-stack">
                <Pill tone={composerResult ? assistantTone(composerResult.assistant.provider) : 'neutral'}>
                  {composerResult ? assistantLabel(composerResult.assistant.provider) : 'Draft'}
                </Pill>
                <span>{composerResult?.assistant.model ?? sourceLabel(sourceKind)}</span>
              </div>
            </div>
            {composerResult?.assistant.note ? (
              <p className="assistant-provider-note">{composerResult.assistant.note}</p>
            ) : null}
            <WorkflowGraph
              spec={workflowPreviewSpec}
              selectedNodeId={selectedDesignerNodeId}
              onNodeSelect={(node) => {
                if (node?.ref?.type === 'source') openDesignerPanel('ingest');
                if (node?.ref?.type === 'processor') openDesignerPanel('transform');
                if (node?.ref?.type === 'sink') openDesignerPanel('destination');
              }}
              large
              designer
            />
          </Panel>

          {composerResult?.preview.accepted ? (
            <Panel className="compose-designer-preview-panel">
              <div className="event-diff-summary" aria-label="Event diff summary">
                <span className="event-diff-chip is-added">{diffCounts.added} added</span>
                <span className="event-diff-chip is-removed">{diffCounts.removed} deleted</span>
                <span className="event-diff-chip is-changed">{diffCounts.changed} transformed</span>
                <span className="event-diff-chip is-unchanged">{diffCounts.unchanged} kept</span>
              </div>
              <div className="compose-designer-diff">
                {diffRows.slice(0, 8).map((row) => (
                  <div key={row.path} className={`json-diff-row is-${row.kind}`}>
                    <span className={`json-diff-badge is-${row.kind}`}>{diffKindLabel(row.kind)}</span>
                    <span className="json-diff-path">{row.path}</span>
                    <pre className="json-diff-value json-diff-value-after">{row.after ?? row.before ?? '—'}</pre>
                  </div>
                ))}
              </div>
            </Panel>
          ) : null}
        </div>

        <Panel className="compose-designer-inspector">
          {designerPanel === 'ingest' ? (
            <>
              <SectionHeader
                eyebrow="Ingest"
                title="Source controller"
                description="Choose where events enter the workflow."
              />
              <SegmentedControl value={sourceKind} onChange={(value) => switchSource(value as SourceKind)} label="Source kind">
                {(['http', 'nats', 'kafka'] as const).map((kind) => (
                  <SegmentedControlItem
                    key={kind}
                    value={kind}
                    label={sourceLabel(kind)}
                  />
                ))}
              </SegmentedControl>
              {composerResult?.sourceBinding ? (
                <VStack gap={2}>
                  <VStack gap={0.5}>
                    <Text type="body" weight="semibold" display="block">{composerResult.sourceBinding.ref}</Text>
                    <Text type="supporting" color="secondary" display="block">{sourceBindingHint(composerResult.sourceBinding)}</Text>
                  </VStack>
                  <CodeBlock language="json" size="sm" width="100%" code={formatJson(composerResult.sourceBinding.config)} />
                </VStack>
              ) : (
                <Card variant="muted" padding={3}>
                  <VStack gap={0.5}>
                    <Text type="supporting" color="secondary" weight="semibold" display="block">BINDING</Text>
                    <Text type="supporting" color="secondary" display="block">Created when the flow is published.</Text>
                  </VStack>
                </Card>
              )}
              <HStack gap={2} wrap="wrap">
                <ActionButton variant="secondary" type="button" onClick={() => openDesignerPanel('sample')}>
                  Choose sample
                </ActionButton>
                <ActionButton variant="primary" type="button" onClick={() => openDesignerPanel('transform')} disabled={!hasSample}>
                  Configure transform
                </ActionButton>
              </HStack>
            </>
          ) : null}

          {designerPanel === 'sample' ? (
            <>
              <SectionHeader eyebrow="Sample" title="Event shape" description="Use one event type as the design sample." />
              <HStack gap={2} wrap="wrap">
                <ActionButton
                  variant="secondary"
                  type="button"
                  onClick={() => {
                    setLoadedDemoEvent(null);
                    setShowJsonEditor(true);
                    setSelectedSampleId(null);
                    setShowSourcePicker(false);
                  }}
                >
                  Paste JSON
                </ActionButton>
                <ActionButton
                  variant="secondary"
                  type="button"
                  onClick={() => setShowSourcePicker((value) => !value)}
                >
                  {showSourcePicker ? 'Hide live sources' : 'Live sources'}
                </ActionButton>
              </HStack>

              {demoEventShapes.length > 0 ? (
                <div className="build-demo-event-list compose-event-list">
                  {demoEventShapes.map((shape) => (
                    <button
                      key={shape.id}
                      type="button"
                      className={`build-demo-event-row ${selectedSampleId === shape.id ? 'is-selected' : ''}`}
                      onClick={() => selectDemoEvent(shape.event)}
                    >
                      <span>
                        <strong>{shape.label}</strong>
                        <small>{shape.event.summary}</small>
                      </span>
                      <em>{sampleShapeLabel(shape.count)}</em>
                    </button>
                  ))}
                </div>
              ) : null}

              {showSourcePicker ? (
                <div className="event-list compose-event-list">
                  {runtimeSampleShapes.map((shape) => (
                    <button
                      key={shape.id}
                      type="button"
                      className={`event-row ${selectedSampleId === shape.id ? 'is-selected' : ''}`}
                      onClick={() => {
                        selectSample(shape.sample);
                        setShowSourcePicker(false);
                      }}
                    >
                      <div className="event-row-main">
                        <strong>{shape.label}</strong>
                        <span className="event-row-ref">{shape.sample.sourceRef}</span>
                      </div>
                      <div className="event-row-preview">{truncateJson(shape.sample.payload)}</div>
                    </button>
                  ))}
                  {liveSamplesQuery.isLoading ? <LoadingBlock lines={3} /> : null}
                </div>
              ) : null}

              <div className="event-shape-card">
                <div className="event-shape-head">
                  <div>
                    <p className="eyebrow">Selected shape</p>
                    <h3 className="panel-title">{activeEventName}</h3>
                  </div>
                  <Pill tone={sampleOriginTone(sampleOrigin)}>{sampleOriginLabel(sampleOrigin)}</Pill>
                </div>
                {showJsonEditor ? (
                  <TextArea
                    label="Sample event JSON"
                    isLabelHidden
                    rows={8}
                    value={samplePayloadText}
                    onChange={(value) => {
                      hasUserEditedSample.current = true;
                      setLoadedDemoEvent(null);
                      setSamplePayloadText(value);
                      setComposerResult(null);
                      setAppliedTransformSyntax(null);
                    }}
                    placeholder='{"event_name": "purchase"}'
                    status={samplePayload.error ? { type: 'error', message: `Invalid JSON: ${samplePayload.error}` } : undefined}
                  />
                ) : hasSample ? (
                  <CodeBlock language="json" size="sm" width="100%" maxHeight={320} code={formatJson(samplePayload.value)} />
                ) : (
                  <Text type="supporting" color="secondary" display="block">Select an event or paste JSON.</Text>
                )}
              </div>
            </>
          ) : null}

          {designerPanel === 'transform' ? (
            <>
              <SectionHeader
                eyebrow="Transform"
                title={composerResult ? 'Processing steps' : 'Create processing'}
                description={composerResult ? reviewSummary : 'Describe the output and let AI create the first transform.'}
              />
              <ToggleButtonGroup
                label="Business examples"
                type="single"
                size="sm"
                value={selectedBusinessExample.id}
                onChange={(value) => {
                  const example = demoBusinessExamples.find((candidate) => candidate.id === value);
                  if (example) applyBusinessExample(example);
                }}
              >
                {demoBusinessExamples.map((example) => (
                  <ToggleButton key={example.id} label={example.title} value={example.id} />
                ))}
              </ToggleButtonGroup>
              <ChatComposer
                value={instruction}
                onChange={(value) => setInstruction(value)}
                placeholder={selectedBusinessExample.prompt}
                isDisabled={isBusy || !hasSample}
                onSubmit={() => {
                  if (!isBusy && hasSample && hasInstruction) designMutation.mutate();
                }}
                footerActions={
                  <HStack gap={1}>
                    <ActionButton variant="ghost" size="sm" type="button" onClick={addFilterStepToDraft} disabled={!composerResult || !hasSample || isBusy}>
                      Add filter
                    </ActionButton>
                    <ActionButton variant="ghost" size="sm" type="button" onClick={addLookupStepToDraft} disabled={!composerResult || !hasSample || isBusy}>
                      Add lookup
                    </ActionButton>
                  </HStack>
                }
                status={designMutation.isError ? { type: 'error', message: designMutation.error.message } : undefined}
              />
              {designMutation.isPending ? <LoadingBlock lines={4} /> : null}
              {transformSyntaxError ? <FieldStatus type="error" variant="detached" message={transformSyntaxError} /> : null}
              {composerResult ? (
                <HStack gap={1} wrap="wrap">
                  {(changedMappings.length > 0 ? changedMappings : composerResult.plan.fieldMappings).slice(0, 10).map((mapping) => (
                    <Token key={`${mapping.from}-${mapping.to}`} label={`${mapping.from} → ${mapping.to}`} />
                  ))}
                </HStack>
              ) : null}
            </>
          ) : null}

          {designerPanel === 'destination' ? (
            <>
              <SectionHeader eyebrow="Destination" title="Export target" description="Choose where accepted events should land." />
              {!composerResult ? <p className="section-copy">Create the transform before selecting a destination.</p> : null}
              {composerResult ? (
                <>
                  {exportMutation.isPending ? <LoadingBlock lines={3} /> : null}
                  <div className="export-list">
                    {composerResult.exportOptions.map((option) => {
                      const isSelected = selectedExportId === option.id;
                      const isRecommended = composerResult.plan.recommendedExportIds.includes(option.id);
                      return (
                        <button
                          key={option.id}
                          type="button"
                          className={`export-row ${isSelected ? 'is-selected' : ''}`}
                          onClick={() => chooseExportOption(option.id)}
                          disabled={isBusy}
                        >
                          <div className="export-row-main">
                            <strong>{option.label}</strong>
                            <span className="export-row-detail">{option.execution} · {option.notes.join(' · ')}</span>
                          </div>
                          <div className="export-row-status">
                            {isSelected ? <Pill tone="good">Selected</Pill> : isRecommended ? <Pill tone="info">Recommended</Pill> : null}
                          </div>
                        </button>
                      );
                    })}
                  </div>

                  {configurableDestination ? (
                    <div className="destination-config">
                      <div className="destination-config-header">
                        <div>
                          <p className="eyebrow">Destination settings</p>
                          <h3 className="panel-title">
                            {selectedExportId === 'snowflake_sink'
                              ? 'Snowflake destination'
                              : selectedExportId === 'bigquery_sink'
                                ? 'BigQuery destination'
                                : 'S3 destination'}
                          </h3>
                        </div>
                        <ActionButton
                          variant="secondary"
                          type="button"
                          onClick={isEditingDestinationConfig ? applyDestinationConfigEditor : openDestinationConfigEditor}
                        >
                          {isEditingDestinationConfig ? 'Apply JSON' : 'Edit JSON'}
                        </ActionButton>
                      </div>

                      {connectorsQuery.data && connectorsQuery.data.length > 0 ? (
                        <Selector
                          label="Saved destination"
                          value={selectedConnectorId ?? NEW_DESTINATION_VALUE}
                          options={[
                            { value: NEW_DESTINATION_VALUE, label: 'New destination' },
                            ...connectorsQuery.data.map((connector) => ({ value: connector.id, label: connector.name })),
                          ]}
                          onChange={(value) => setSelectedConnectorId(value === NEW_DESTINATION_VALUE ? null : value)}
                        />
                      ) : null}

                      {isEditingDestinationConfig ? (
                        <div className="destination-config-json">
                          <JsonSyntaxEditor
                            value={destinationConfigText}
                            onChange={(value) => {
                              setDestinationConfigText(value);
                              setDestinationConfigError(null);
                            }}
                            compact
                          />
                        </div>
                      ) : (
                        <Grid columns={{ minWidth: 220 }} gap={3}>
                          <TextInput
                            label="Connector name"
                            value={destinationName}
                            onChange={(value) => setDestinationName(value)}
                            placeholder="Warehouse destination"
                          />
                          {destinationFields(selectedExportId).map((field) => (
                            field.multiline ? (
                              <GridSpan key={field.key} columns='full'>
                                <TextArea
                                  label={field.label}
                                  rows={5}
                                  value={configFieldValue(destinationConfig, field.key)}
                                  onChange={(value) => setDestinationConfig((current) => ({ ...current, [field.key]: value }))}
                                  placeholder={field.placeholder}
                                />
                              </GridSpan>
                            ) : (
                              <TextInput
                                key={field.key}
                                label={field.label}
                                value={configFieldValue(destinationConfig, field.key)}
                                onChange={(value) => setDestinationConfig((current) => ({ ...current, [field.key]: value }))}
                                placeholder={field.placeholder}
                              />
                            )
                          ))}
                        </Grid>
                      )}
                      {destinationConfigError ? <FieldStatus type="error" variant="detached" message={destinationConfigError} /> : null}
                      <ActionButton variant="secondary" type="button" onClick={() => saveConnectorMutation.mutate()} disabled={isBusy || saveConnectorMutation.isPending}>
                        {selectedConnector ? 'Update destination settings' : 'Save destination'}
                      </ActionButton>
                    </div>
                  ) : null}
                </>
              ) : null}
              {exportMutation.isError ? <FieldStatus type="error" variant="detached" message={exportMutation.error.message} /> : null}
              {saveConnectorMutation.isError ? <FieldStatus type="error" variant="detached" message={saveConnectorMutation.error instanceof Error ? saveConnectorMutation.error.message : 'Failed to save destination.'} /> : null}
              {publishMutation.isError ? <FieldStatus type="error" variant="detached" message={publishMutation.error.message} /> : null}
            </>
          ) : null}

          {designerPanel === 'advanced' ? (
            <>
              <SectionHeader eyebrow="Advanced" title="Transformation JSON" description="Use this when the visual controls are not enough." />
              {composerResult ? (
                <>
                  {!isEditingTransform ? (
                    <ActionButton variant="secondary" type="button" onClick={openTransformEditor}>
                      Load transformation JSON
                    </ActionButton>
                  ) : null}
                  <JsonSyntaxEditor
                    value={transformSyntaxText}
                    onChange={(nextValue) => {
                      setTransformSyntaxText(nextValue);
                      setTransformSyntaxError(null);
                    }}
                    compact
                  />
                  {transformSyntaxError ? <FieldStatus type="error" variant="detached" message={transformSyntaxError} /> : null}
                  <div className="flow-node-inspector-actions">
                    <ActionButton variant="secondary" type="button" onClick={openTransformEditor}>
                      Reset
                    </ActionButton>
                    <ActionButton variant="primary" type="button" onClick={applyTransformEditor}>
                      Apply
                    </ActionButton>
                  </div>
                </>
              ) : (
                <p className="section-copy">Generate a transform first.</p>
              )}
            </>
          ) : null}
        </Panel>
      </div>
    </VStack>
  );

}
