import { useState } from 'react';
import { Link, useParams } from '@tanstack/react-router';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { SegmentedControl, SegmentedControlItem } from '@astryxdesign/core/SegmentedControl';
import type { FlowSpec as BackendFlowSpec, PredicateExpr, ProcessorNode } from '@rohrpost/shared-flow-spec';
import {
  ArrowDownTrayIcon,
  ArrowPathIcon,
  ArrowRightIcon,
  CheckIcon,
  ChevronLeftIcon,
  EyeIcon,
  PauseIcon,
  PencilSquareIcon,
  PlusIcon,
  ShareIcon,
  XMarkIcon,
} from '@heroicons/react/24/outline';
import { api } from '../lib/api';
import { describeConsoleError } from '../lib/error-state';
import { Card } from '@astryxdesign/core/Card';
import { CodeBlock } from '@astryxdesign/core/CodeBlock';
import { EmptyState } from '@astryxdesign/core/EmptyState';
import { Grid } from '@astryxdesign/core/Grid';
import { Icon } from '@astryxdesign/core/Icon';
import { IconButton } from '@astryxdesign/core/IconButton';
import { SelectableCard } from '@astryxdesign/core/SelectableCard';
import { Selector } from '@astryxdesign/core/Selector';
import { StackItem } from '@astryxdesign/core/Stack';
import { TextArea } from '@astryxdesign/core/TextArea';
import { TextInput } from '@astryxdesign/core/TextInput';
import { Heading } from '@astryxdesign/core/Heading';
import { HStack } from '@astryxdesign/core/HStack';
import { List, ListItem } from '@astryxdesign/core/List';
import { Table, proportional, type TableColumn } from '@astryxdesign/core/Table';
import { Text } from '@astryxdesign/core/Text';
import { VStack } from '@astryxdesign/core/VStack';
import { ActionButton, ActionLink, LoadingBlock, Panel, Pill } from '../components/ui';
import { WorkflowGraph } from '../components/WorkflowGraph';
import { JsonSyntaxEditor } from '../components/JsonSyntaxEditor';
import type { WorkflowNode, WorkflowNodeRef } from '../features/workflow/workflow-graph';

type Tab = 'workflow' | 'runs' | 'revs' | 'config' | 'dlq';

function stateTone(state: string): 'good' | 'warn' | 'danger' | 'info' | 'neutral' {
  if (state === 'healthy' || state === 'published') return 'good';
  if (state === 'backlogged') return 'warn';
  if (state === 'degraded') return 'danger';
  if (state === 'idle' || state === 'paused' || state === 'draft') return 'neutral';
  return 'info';
}

type EditableFlowPart =
  | { type: 'source'; label: string; value: BackendFlowSpec['sources'][number] }
  | { type: 'processor'; label: string; value: BackendFlowSpec['processors'][number] }
  | { type: 'route'; label: string; value: BackendFlowSpec['routes'][number] }
  | { type: 'sink'; label: string; value: BackendFlowSpec['sinks'][number] };

type DesignerPanel = 'configure' | 'add' | 'advanced';
type VisualProcessorKind = 'map' | 'filter' | 'enrich_lookup';
type PredicateOperator =
  | 'always'
  | 'field_exists'
  | 'field_equals'
  | 'field_contains'
  | 'field_gt'
  | 'field_gte'
  | 'field_lt'
  | 'field_lte';

interface VisualProcessorDraft {
  kind: VisualProcessorKind;
  mapMode: 'merge' | 'project';
  mappings: Array<{ from: string; to: string }>;
  predicateOperator: PredicateOperator;
  predicatePath: string;
  predicateValue: string;
  keyPath: string;
  targetPath: string;
  missing: 'skip' | 'null' | 'fail';
  lookupRows: Array<{ key: string; valueText: string }>;
}

const processorKindOptions: Array<{ kind: VisualProcessorKind; label: string; detail: string }> = [
  { kind: 'map', label: 'Transform', detail: 'Map input fields to output fields.' },
  { kind: 'filter', label: 'Filter', detail: 'Pass only events matching a condition.' },
  { kind: 'enrich_lookup', label: 'Lookup', detail: 'Attach values from a keyed table.' },
];

const predicateOperatorOptions: Array<{ value: PredicateOperator; label: string }> = [
  { value: 'always', label: 'Always pass' },
  { value: 'field_exists', label: 'Field exists' },
  { value: 'field_equals', label: 'Field equals' },
  { value: 'field_contains', label: 'Field contains' },
  { value: 'field_gt', label: 'Greater than' },
  { value: 'field_gte', label: 'Greater or equal' },
  { value: 'field_lt', label: 'Less than' },
  { value: 'field_lte', label: 'Less or equal' },
];

function cloneBackendSpec(spec: BackendFlowSpec): BackendFlowSpec {
  return JSON.parse(JSON.stringify(spec)) as BackendFlowSpec;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function findEditableFlowPart(spec: BackendFlowSpec, node: WorkflowNode | null): EditableFlowPart | null {
  const ref = node?.ref;
  if (!ref) return null;

  if (ref.type === 'source') {
    const value = spec.sources.find((source) => source.id === ref.id);
    return value ? { type: ref.type, label: 'Source', value } : null;
  }
  if (ref.type === 'processor') {
    const value = spec.processors.find((processor) => processor.id === ref.id);
    return value ? { type: ref.type, label: 'Processor', value } : null;
  }
  if (ref.type === 'route') {
    const value = spec.routes.find((route) => route.id === ref.id);
    return value ? { type: ref.type, label: 'Route', value } : null;
  }

  const value = spec.sinks.find((sink) => sink.id === ref.id);
  return value ? { type: ref.type, label: 'Destination', value } : null;
}

function parseEditableJson(text: string, ref: WorkflowNodeRef): Record<string, unknown> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (error) {
    throw new Error(error instanceof Error ? error.message : 'Invalid JSON.');
  }
  if (!isRecord(parsed)) {
    throw new Error('Selected node must be a JSON object.');
  }
  if (parsed.id !== ref.id) {
    throw new Error('Keep the selected node id unchanged.');
  }
  return parsed;
}

function replaceEditableFlowPart(spec: BackendFlowSpec, node: WorkflowNode, text: string): BackendFlowSpec {
  if (!node.ref) throw new Error('This graph node is derived at runtime and cannot be edited directly.');
  const parsed = parseEditableJson(text, node.ref);
  const next = cloneBackendSpec(spec);

  if (node.ref.type === 'source') {
    next.sources = next.sources.map((source) =>
      source.id === node.ref?.id ? parsed as unknown as BackendFlowSpec['sources'][number] : source,
    );
  } else if (node.ref.type === 'processor') {
    next.processors = next.processors.map((processor) =>
      processor.id === node.ref?.id ? parsed as unknown as BackendFlowSpec['processors'][number] : processor,
    );
  } else if (node.ref.type === 'route') {
    next.routes = next.routes.map((route) =>
      route.id === node.ref?.id ? parsed as unknown as BackendFlowSpec['routes'][number] : route,
    );
  } else {
    next.sinks = next.sinks.map((sink) =>
      sink.id === node.ref?.id ? parsed as unknown as BackendFlowSpec['sinks'][number] : sink,
    );
  }

  return next;
}

function uniqueProcessorId(spec: BackendFlowSpec, base: string): string {
  const ids = new Set([
    ...spec.sources.map((source) => source.id),
    ...spec.processors.map((processor) => processor.id),
    ...spec.routes.map((route) => route.id),
    ...spec.sinks.map((sink) => sink.id),
  ]);
  let candidate = base;
  let index = 2;
  while (ids.has(candidate)) {
    candidate = `${base}_${index}`;
    index += 1;
  }
  return candidate;
}

function defaultVisualProcessorDraft(kind: VisualProcessorKind = 'map'): VisualProcessorDraft {
  return {
    kind,
    mapMode: 'merge',
    mappings: [{ from: 'event_name', to: 'event_name' }],
    predicateOperator: 'field_exists',
    predicatePath: 'event_name',
    predicateValue: '',
    keyPath: 'customer_id',
    targetPath: 'customer.enrichment',
    missing: 'skip',
    lookupRows: [
      {
        key: 'demo_customer',
        valueText: JSON.stringify({ segment: 'high_value', creditScore: 720 }, null, 2),
      },
    ],
  };
}

function visualProcessorKindLabel(kind: VisualProcessorKind): string {
  return processorKindOptions.find((option) => option.kind === kind)?.label ?? 'Step';
}

function processorToVisualDraft(processor: ProcessorNode): VisualProcessorDraft | null {
  if (processor.kind === 'map') {
    return {
      ...defaultVisualProcessorDraft('map'),
      mapMode: processor.mode ?? 'merge',
      mappings: processor.mappings.length > 0 ? processor.mappings.map((mapping) => ({ ...mapping })) : [{ from: '', to: '' }],
    };
  }

  if (processor.kind === 'filter') {
    return {
      ...defaultVisualProcessorDraft('filter'),
      ...predicateToDraft(processor.predicate),
    };
  }

  if (processor.kind === 'enrich_lookup') {
    return {
      ...defaultVisualProcessorDraft('enrich_lookup'),
      keyPath: processor.keyPath,
      targetPath: processor.targetPath ?? '',
      missing: processor.lookup.missing ?? 'skip',
      lookupRows: Object.entries(processor.lookup.table).map(([key, value]) => ({
        key,
        valueText: JSON.stringify(value, null, 2),
      })),
    };
  }

  return null;
}

function predicateToDraft(predicate: PredicateExpr): Pick<VisualProcessorDraft, 'predicateOperator' | 'predicatePath' | 'predicateValue'> {
  switch (predicate.type) {
    case 'always':
      return { predicateOperator: 'always', predicatePath: '', predicateValue: '' };
    case 'field_exists':
      return { predicateOperator: 'field_exists', predicatePath: predicate.path, predicateValue: '' };
    case 'field_equals':
    case 'field_contains':
    case 'field_gt':
    case 'field_gte':
    case 'field_lt':
    case 'field_lte':
      return {
        predicateOperator: predicate.type,
        predicatePath: predicate.path,
        predicateValue: String(predicate.value ?? ''),
      };
    default:
      return { predicateOperator: 'field_exists', predicatePath: '', predicateValue: '' };
  }
}

function draftToPredicate(draft: VisualProcessorDraft): PredicateExpr {
  if (draft.predicateOperator === 'always') return { type: 'always' };

  const path = draft.predicatePath.trim();
  if (!path) throw new Error('Filter field path is required.');
  if (draft.predicateOperator === 'field_exists') return { type: 'field_exists', path };

  if (
    draft.predicateOperator === 'field_gt'
    || draft.predicateOperator === 'field_gte'
    || draft.predicateOperator === 'field_lt'
    || draft.predicateOperator === 'field_lte'
  ) {
    const parsedValue = Number(draft.predicateValue);
    if (!Number.isFinite(parsedValue)) {
      throw new Error('Filter value must be numeric for this operator.');
    }
    return { type: draft.predicateOperator, path, value: parsedValue };
  }

  if (draft.predicateOperator === 'field_contains') {
    return { type: 'field_contains', path, value: draft.predicateValue };
  }

  return { type: 'field_equals', path, value: draft.predicateValue };
}

function cleanMappings(draft: VisualProcessorDraft): Array<{ from: string; to: string }> {
  const mappings = draft.mappings
    .map((mapping) => ({ from: mapping.from.trim(), to: mapping.to.trim() }))
    .filter((mapping) => mapping.from && mapping.to);

  if (mappings.length === 0) {
    throw new Error('Add at least one complete field mapping.');
  }

  return mappings;
}

function lookupTableFromDraft(draft: VisualProcessorDraft): Record<string, unknown> {
  const table: Record<string, unknown> = {};
  for (const row of draft.lookupRows) {
    const key = row.key.trim();
    if (!key) continue;

    try {
      table[key] = JSON.parse(row.valueText);
    } catch (error) {
      throw new Error(`Lookup value for ${key} is not valid JSON.`);
    }
  }

  if (Object.keys(table).length === 0) {
    throw new Error('Add at least one lookup row.');
  }

  return table;
}

function processorFromVisualDraft(
  draft: VisualProcessorDraft,
  id: string,
  nextNodeIds: string[],
  existing?: ProcessorNode,
): ProcessorNode {
  const base = {
    id,
    nextNodeIds,
    ...(existing?.connector ? { connector: existing.connector } : {}),
  };

  if (draft.kind === 'map') {
    return {
      ...base,
      kind: 'map',
      mode: draft.mapMode,
      mappings: cleanMappings(draft),
    };
  }

  if (draft.kind === 'filter') {
    return {
      ...base,
      kind: 'filter',
      predicate: draftToPredicate(draft),
    };
  }

  const keyPath = draft.keyPath.trim();
  if (!keyPath) throw new Error('Lookup key path is required.');

  return {
    ...base,
    kind: 'enrich_lookup',
    keyPath,
    targetPath: draft.targetPath.trim() || undefined,
    lookup: {
      mode: 'inline',
      table: lookupTableFromDraft(draft),
      missing: draft.missing,
    },
  };
}

function replaceProcessorWithVisualDraft(
  spec: BackendFlowSpec,
  processorId: string,
  draft: VisualProcessorDraft,
): BackendFlowSpec {
  const next = cloneBackendSpec(spec);
  const existing = next.processors.find((processor) => processor.id === processorId);
  if (!existing) throw new Error('Selected processor no longer exists in the flow.');

  next.processors = next.processors.map((processor) =>
    processor.id === processorId
      ? processorFromVisualDraft(draft, processor.id, processor.nextNodeIds, processor)
      : processor,
  );
  return next;
}

function processorBaseId(kind: VisualProcessorKind): string {
  switch (kind) {
    case 'filter':
      return 'processor_filter';
    case 'enrich_lookup':
      return 'processor_lookup_enrichment';
    case 'map':
      return 'processor_transform';
  }
}

function insertProcessorAfter(spec: BackendFlowSpec, ref: WorkflowNodeRef, processor: ProcessorNode): BackendFlowSpec {
  if (ref.type !== 'source' && ref.type !== 'processor') {
    throw new Error('Select an ingest or processor node before adding a step.');
  }

  const next = cloneBackendSpec(spec);
  const routeIdsFromAnchor = next.routes
    .filter((route) => route.fromNodeId === ref.id)
    .map((route) => route.id);

  const anchor = ref.type === 'source'
    ? next.sources.find((source) => source.id === ref.id)
    : next.processors.find((processor) => processor.id === ref.id);

  if (!anchor) {
    throw new Error('Selected node no longer exists in the flow.');
  }

  const previousNextNodeIds = [...anchor.nextNodeIds];
  const stepNextNodeIds = previousNextNodeIds.length > 0 ? previousNextNodeIds : routeIdsFromAnchor;
  if (stepNextNodeIds.length === 0) {
    throw new Error('Selected node does not point to another workflow step yet.');
  }

  const insertedProcessor = {
    ...processor,
    nextNodeIds: stepNextNodeIds,
  };

  anchor.nextNodeIds = [insertedProcessor.id];
  next.routes = next.routes.map((route) =>
    route.fromNodeId === ref.id ? { ...route, fromNodeId: insertedProcessor.id } : route,
  );

  if (ref.type === 'processor') {
    const anchorIndex = next.processors.findIndex((candidate) => candidate.id === ref.id);
    next.processors.splice(anchorIndex + 1, 0, insertedProcessor);
  } else {
    next.processors.unshift(insertedProcessor);
  }

  return next;
}

function switchDraftKind(draft: VisualProcessorDraft, kind: VisualProcessorKind): VisualProcessorDraft {
  return draft.kind === kind ? draft : defaultVisualProcessorDraft(kind);
}

function VisualProcessorEditor({
  draft,
  onChange,
  showStepTypePicker = false,
}: {
  draft: VisualProcessorDraft;
  onChange: (draft: VisualProcessorDraft) => void;
  showStepTypePicker?: boolean;
}) {
  const setMapping = (index: number, patch: Partial<{ from: string; to: string }>) => {
    onChange({
      ...draft,
      mappings: draft.mappings.map((mapping, mappingIndex) =>
        mappingIndex === index ? { ...mapping, ...patch } : mapping,
      ),
    });
  };
  const setLookupRow = (index: number, patch: Partial<{ key: string; valueText: string }>) => {
    onChange({
      ...draft,
      lookupRows: draft.lookupRows.map((row, rowIndex) =>
        rowIndex === index ? { ...row, ...patch } : row,
      ),
    });
  };

  return (
    <VStack gap={4}>
      {showStepTypePicker ? (
        <Grid columns={{ minWidth: 150 }} gap={2} aria-label="Step type">
          {processorKindOptions.map((option) => (
            <SelectableCard
              key={option.kind}
              label={option.label}
              isSelected={draft.kind === option.kind}
              onChange={() => onChange(switchDraftKind(draft, option.kind))}
              padding={3}
            >
              <VStack gap={0.5}>
                <Text type="body" weight="semibold" display="block">{option.label}</Text>
                <Text type="supporting" color="secondary" display="block">{option.detail}</Text>
              </VStack>
            </SelectableCard>
          ))}
        </Grid>
      ) : null}

      {draft.kind === 'map' ? (
        <VStack gap={3}>
          <Selector
            label="Output mode"
            value={draft.mapMode}
            options={[
              { value: 'merge', label: 'Merge into payload' },
              { value: 'project', label: 'Project selected fields' },
            ]}
            onChange={(value) => onChange({ ...draft, mapMode: value as VisualProcessorDraft['mapMode'] })}
          />
          <VStack gap={2}>
            {draft.mappings.map((mapping, index) => (
              <HStack key={index} gap={2} align="center">
                <StackItem size="fill">
                  <TextInput
                    label={`Mapping ${index + 1} source path`}
                    isLabelHidden
                    value={mapping.from}
                    onChange={(value) => setMapping(index, { from: value })}
                    placeholder="Source path"
                  />
                </StackItem>
                <Icon icon={ArrowRightIcon} size="xsm" />
                <StackItem size="fill">
                  <TextInput
                    label={`Mapping ${index + 1} target path`}
                    isLabelHidden
                    value={mapping.to}
                    onChange={(value) => setMapping(index, { to: value })}
                    placeholder="Target path"
                  />
                </StackItem>
                <IconButton
                  label="Remove mapping"
                  variant="ghost"
                  size="sm"
                  icon={<Icon icon={XMarkIcon} size="sm" />}
                  onClick={() => onChange({
                    ...draft,
                    mappings: draft.mappings.filter((_, mappingIndex) => mappingIndex !== index),
                  })}
                />
              </HStack>
            ))}
          </VStack>
          <HStack>
            <ActionButton
              type="button"
              variant="secondary"
              onClick={() => onChange({ ...draft, mappings: [...draft.mappings, { from: '', to: '' }] })}
              icon={PlusIcon}
            >
              Add mapping
            </ActionButton>
          </HStack>
        </VStack>
      ) : null}

      {draft.kind === 'filter' ? (
        <VStack gap={3}>
          <Selector
            label="Condition"
            value={draft.predicateOperator}
            options={predicateOperatorOptions.map((option) => ({ value: option.value, label: option.label }))}
            onChange={(value) => onChange({ ...draft, predicateOperator: value as PredicateOperator })}
          />
          {draft.predicateOperator !== 'always' ? (
            <TextInput
              label="Field path"
              value={draft.predicatePath}
              onChange={(value) => onChange({ ...draft, predicatePath: value })}
              placeholder="event_name"
            />
          ) : null}
          {draft.predicateOperator !== 'always' && draft.predicateOperator !== 'field_exists' ? (
            <TextInput
              label="Value"
              value={draft.predicateValue}
              onChange={(value) => onChange({ ...draft, predicateValue: value })}
              placeholder="purchase"
            />
          ) : null}
        </VStack>
      ) : null}

      {draft.kind === 'enrich_lookup' ? (
        <VStack gap={3}>
          <TextInput
            label="Lookup key path"
            value={draft.keyPath}
            onChange={(value) => onChange({ ...draft, keyPath: value })}
            placeholder="customer_id"
          />
          <TextInput
            label="Write to"
            value={draft.targetPath}
            onChange={(value) => onChange({ ...draft, targetPath: value })}
            placeholder="customer.enrichment"
          />
          <Selector
            label="Missing key"
            value={draft.missing}
            options={[
              { value: 'skip', label: 'Skip enrichment' },
              { value: 'null', label: 'Write null' },
              { value: 'fail', label: 'Fail event' },
            ]}
            onChange={(value) => onChange({ ...draft, missing: value as VisualProcessorDraft['missing'] })}
          />
          <VStack gap={3}>
            {draft.lookupRows.map((row, index) => (
              <Card key={index} variant="muted" padding={3}>
                <VStack gap={2}>
                  <HStack justify="between" align="center" gap={2}>
                    <StackItem size="fill">
                      <TextInput
                        label="Key"
                        value={row.key}
                        onChange={(value) => setLookupRow(index, { key: value })}
                        placeholder="demo_customer"
                      />
                    </StackItem>
                    <IconButton
                      label="Remove lookup row"
                      variant="ghost"
                      size="sm"
                      icon={<Icon icon={XMarkIcon} size="sm" />}
                      onClick={() => onChange({
                        ...draft,
                        lookupRows: draft.lookupRows.filter((_, rowIndex) => rowIndex !== index),
                      })}
                    />
                  </HStack>
                  <TextArea
                    label="Value JSON"
                    value={row.valueText}
                    onChange={(value) => setLookupRow(index, { valueText: value })}
                    rows={4}
                  />
                </VStack>
              </Card>
            ))}
          </VStack>
          <HStack>
            <ActionButton
              type="button"
              variant="secondary"
              onClick={() => onChange({
                ...draft,
                lookupRows: [...draft.lookupRows, { key: '', valueText: '{\n  \n}' }],
              })}
              icon={PlusIcon}
            >
              Add lookup row
            </ActionButton>
          </HStack>
        </VStack>
      ) : null}
    </VStack>
  );
}

export function FlowDetailPage() {
  const { flowId } = useParams({ from: '/flows/$flowId' });
  const queryClient = useQueryClient();
  const [tab, setTab] = useState<Tab>('workflow');
  const [selectedWorkflowNode, setSelectedWorkflowNode] = useState<WorkflowNode | null>(null);
  const [designerPanel, setDesignerPanel] = useState<DesignerPanel>('add');
  const [configDraft, setConfigDraft] = useState<VisualProcessorDraft>(defaultVisualProcessorDraft('map'));
  const [newStepDraft, setNewStepDraft] = useState<VisualProcessorDraft>(defaultVisualProcessorDraft('map'));
  const [selectedNodeJson, setSelectedNodeJson] = useState('');
  const [designerError, setDesignerError] = useState<string | null>(null);
  const [designerMessage, setDesignerMessage] = useState<string | null>(null);
  const flowsQuery = useQuery({ queryKey: ['flows'], queryFn: api.fetchFlows });
  const runtimeQuery = useQuery({ queryKey: ['runtime-stats'], queryFn: api.fetchRuntimeStats });
  const adapterWorkloadsQuery = useQuery({ queryKey: ['adapter-workloads'], queryFn: api.fetchAdapterWorkloads });
  const publishWorkflowMutation = useMutation({
    mutationFn: api.publishBackendFlowSpec,
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ['flows'] }),
        queryClient.invalidateQueries({ queryKey: ['overview'] }),
        queryClient.invalidateQueries({ queryKey: ['runtime-stats'] }),
        queryClient.invalidateQueries({ queryKey: ['adapter-workloads'] }),
      ]);
    },
  });

  if (flowsQuery.isPending || runtimeQuery.isPending) {
    return <Panel><LoadingBlock lines={5} /></Panel>;
  }
  if (flowsQuery.isError || runtimeQuery.isError) {
    const errorState = describeConsoleError(flowsQuery.error ?? runtimeQuery.error);
    return (
      <Panel>
        <VStack gap={2}>
          <Heading level={1}>Flow unavailable</Heading>
          <Text type="supporting" color="secondary" display="block">{errorState.message}</Text>
          <Text type="supporting" color="secondary" display="block">{errorState.hint}</Text>
        </VStack>
      </Panel>
    );
  }

  const flow = flowsQuery.data.find((f) => f.id === flowId);
  if (!flow) {
    return (
      <Panel>
        <VStack gap={2} align="start">
          <Heading level={1}>Flow not found</Heading>
          <Text type="supporting" color="secondary" display="block">
            The flow id <Text type="code">{flowId}</Text> doesn&rsquo;t match a published flow.
          </Text>
          <ActionLink to="/flows" variant="secondary">Back to flows</ActionLink>
        </VStack>
      </Panel>
    );
  }

  const deployment = runtimeQuery.data.deployments.find((d) => d.flowId === flow.id || d.flowName === flow.name);
  const adapterWorkloads = adapterWorkloadsQuery.data?.filter((workload) =>
    workload.flowIds.includes(flow.id) || (deployment ? workload.deploymentIds.includes(deployment.deploymentId) : false),
  );
  const backlog = deployment?.backlogCount ?? 0;
  const accepted = deployment?.acceptedCount ?? 0;
  const delivered = deployment?.deliveredCount ?? 0;
  const errorRate = accepted > 0 ? Math.min(0.08, backlog / accepted) : 0;
  const errored = deployment?.state === 'degraded';
  const summaryMetrics = [
    { label: 'Accepted', value: accepted.toLocaleString(), tone: 'neutral' as const },
    { label: 'Delivered', value: delivered.toLocaleString(), tone: 'good' as const },
    { label: 'Backlog', value: backlog.toLocaleString(), tone: backlog > 0 ? 'warn' as const : 'neutral' as const },
    { label: 'Errors', value: `${(errorRate * 100).toFixed(2)}%`, tone: errored ? 'danger' as const : 'neutral' as const },
  ];
  const backendSpec = flow.backendSpec as BackendFlowSpec | undefined;
  const selectedFlowPart = backendSpec ? findEditableFlowPart(backendSpec, selectedWorkflowNode) : null;
  const selectedProcessor = selectedFlowPart?.type === 'processor' ? selectedFlowPart.value : null;
  const canConfigureVisually = Boolean(selectedProcessor && processorToVisualDraft(selectedProcessor));
  const canAddStep = Boolean(
    backendSpec
      && selectedWorkflowNode?.ref
      && (selectedWorkflowNode.ref.type === 'source' || selectedWorkflowNode.ref.type === 'processor'),
  );

  function handleWorkflowNodeSelect(node: WorkflowNode | null) {
    setSelectedWorkflowNode(node);
    setDesignerError(null);
    setDesignerMessage(null);
    const selectedPart = backendSpec ? findEditableFlowPart(backendSpec, node) : null;
    const visualDraft = selectedPart?.type === 'processor' ? processorToVisualDraft(selectedPart.value) : null;
    if (visualDraft) {
      setConfigDraft(visualDraft);
      setDesignerPanel('configure');
    } else {
      setDesignerPanel(node?.ref?.type === 'source' || node?.ref?.type === 'processor' ? 'add' : 'advanced');
    }
    setNewStepDraft(defaultVisualProcessorDraft('map'));
    setSelectedNodeJson(selectedPart ? JSON.stringify(selectedPart.value, null, 2) : '');
  }

  function publishDesignerSpec(nextSpec: BackendFlowSpec, successMessage: string) {
    setDesignerError(null);
    setDesignerMessage(null);
    publishWorkflowMutation.mutate(
      {
        spec: nextSpec,
        name: nextSpec.metadata.name,
        tenantId: nextSpec.metadata.tenantId,
      },
      {
        onSuccess: (result) => {
          setDesignerMessage(`${successMessage} Published ${result.revisionId}.`);
        },
        onError: (error) => {
          setDesignerError(error instanceof Error ? error.message : 'Workflow update failed.');
        },
      },
    );
  }

  function handlePublishSelectedNode() {
    if (!backendSpec || !selectedWorkflowNode) return;
    try {
      publishDesignerSpec(
        replaceEditableFlowPart(backendSpec, selectedWorkflowNode, selectedNodeJson),
        'Node updated.',
      );
    } catch (error) {
      setDesignerError(error instanceof Error ? error.message : 'Invalid node update.');
    }
  }

  function handlePublishVisualProcessor() {
    if (!backendSpec || !selectedWorkflowNode?.ref || selectedWorkflowNode.ref.type !== 'processor') return;
    try {
      publishDesignerSpec(
        replaceProcessorWithVisualDraft(backendSpec, selectedWorkflowNode.ref.id, configDraft),
        `${visualProcessorKindLabel(configDraft.kind)} updated.`,
      );
    } catch (error) {
      setDesignerError(error instanceof Error ? error.message : 'Invalid step configuration.');
    }
  }

  function handleAddVisualStep() {
    if (!backendSpec || !selectedWorkflowNode?.ref) return;
    try {
      const processorId = uniqueProcessorId(backendSpec, processorBaseId(newStepDraft.kind));
      const processor = processorFromVisualDraft(newStepDraft, processorId, []);
      publishDesignerSpec(
        insertProcessorAfter(backendSpec, selectedWorkflowNode.ref, processor),
        `${visualProcessorKindLabel(newStepDraft.kind)} step added.`,
      );
    } catch (error) {
      setDesignerError(error instanceof Error ? error.message : 'Unable to add workflow step.');
    }
  }

  function handleResetSelectedNode() {
    if (!selectedFlowPart) return;
    setSelectedNodeJson(JSON.stringify(selectedFlowPart.value, null, 2));
    setDesignerError(null);
    setDesignerMessage(null);
  }

  return (
    <VStack gap={5}>
      <HStack justify="between" align="end" gap={4} wrap="wrap">
        <VStack gap={1} align="start">
          <Link to="/flows" className="flow-back-link">
            <Icon icon={ChevronLeftIcon} size="xsm" /> back to flows
          </Link>
          <HStack gap={2} align="center">
            <Pill tone={stateTone(deployment?.state ?? flow.status)}>{deployment?.state ?? flow.status}</Pill>
            <Heading level={1}>{flow.name}</Heading>
          </HStack>
          <Text type="supporting" color="secondary" display="block">
            {flow.sourceLabel} → {flow.processors.join(' → ') || '—'} → {flow.sinkLabel} · {flow.execution} · {flow.revisionId}
          </Text>
        </VStack>
        <HStack gap={2} align="center" wrap="wrap" justify="end">
          <ActionButton type="button" variant="secondary" icon={EyeIcon}>
            Sample payload
          </ActionButton>
          <ActionButton type="button" variant="secondary" icon={ShareIcon}>
            Revisions
          </ActionButton>
          <ActionButton type="button" variant="secondary" icon={PauseIcon}>
            Pause
          </ActionButton>
          <ActionLink to="/compose" search={{ flowId: flow.id }} variant="primary" icon={PencilSquareIcon}>
            Edit
          </ActionLink>
        </HStack>
      </HStack>

      <Panel className="flow-detail-tabs-panel">
        <SegmentedControl value={tab} onChange={(value) => setTab(value as Tab)} label="Flow detail tabs">
          <SegmentedControlItem value="workflow" label="Workflow" />
          <SegmentedControlItem value="runs" label="Runs" />
          <SegmentedControlItem value="revs" label="Revisions" />
          <SegmentedControlItem value="config" label="Configuration" />
          <SegmentedControlItem value="dlq" label={`DLQ${backlog > 0 ? ` ${backlog}` : ''}`} />
        </SegmentedControl>

        <div className={`flow-tab-body ${tab === 'workflow' ? 'flow-tab-body-workflow' : ''}`}>
          {tab === 'workflow' ? (
            <div className={`flow-designer-layout ${selectedWorkflowNode ? 'has-selection' : ''}`}>
              <WorkflowGraph
                spec={backendSpec}
                deployment={deployment}
                adapterWorkloads={adapterWorkloads}
                summaryMetrics={summaryMetrics}
                selectedNodeId={selectedWorkflowNode?.id}
                onNodeSelect={handleWorkflowNodeSelect}
                designer
                large
              />
              {selectedWorkflowNode ? (
                <aside className="flow-node-inspector">
                  <div className="flow-node-inspector-head">
                    <div>
                      <p className="eyebrow">{selectedFlowPart?.label ?? 'Runtime node'}</p>
                      <h2>{selectedWorkflowNode.label}</h2>
                      {selectedWorkflowNode.detail ? <span>{selectedWorkflowNode.detail}</span> : null}
                    </div>
                    <IconButton
                      label="Close editor"
                      tooltip="Close editor"
                      variant="ghost"
                      size="sm"
                      icon={<Icon icon={XMarkIcon} size="sm" />}
                      onClick={() => handleWorkflowNodeSelect(null)}
                    />
                  </div>
                  {selectedFlowPart ? (
                    <>
                      <div className="flow-node-panel-tabs">
                        {canConfigureVisually ? (
                          <button
                            type="button"
                            className={`flow-node-panel-tab ${designerPanel === 'configure' ? 'is-active' : ''}`}
                            onClick={() => setDesignerPanel('configure')}
                          >
                            Configure
                          </button>
                        ) : null}
                        {canAddStep ? (
                          <button
                            type="button"
                            className={`flow-node-panel-tab ${designerPanel === 'add' ? 'is-active' : ''}`}
                            onClick={() => setDesignerPanel('add')}
                          >
                            Add step
                          </button>
                        ) : null}
                        <button
                          type="button"
                          className={`flow-node-panel-tab ${designerPanel === 'advanced' ? 'is-active' : ''}`}
                          onClick={() => setDesignerPanel('advanced')}
                        >
                          Advanced
                        </button>
                      </div>

                      {designerPanel === 'configure' && canConfigureVisually ? (
                        <>
                          <VisualProcessorEditor
                            draft={configDraft}
                            onChange={(draft) => {
                              setConfigDraft(draft);
                              setDesignerError(null);
                              setDesignerMessage(null);
                            }}
                          />
                          <div className="flow-node-inspector-actions">
                            <ActionButton
                              type="button"
                              variant="primary"
                              onClick={handlePublishVisualProcessor}
                              disabled={publishWorkflowMutation.isPending}
                              icon={CheckIcon}
                            >
                              Save and publish
                            </ActionButton>
                            <ActionButton
                              type="button"
                              variant="secondary"
                              onClick={() => {
                                if (!selectedProcessor) return;
                                setConfigDraft(processorToVisualDraft(selectedProcessor) ?? defaultVisualProcessorDraft('map'));
                                setDesignerError(null);
                                setDesignerMessage(null);
                              }}
                              disabled={publishWorkflowMutation.isPending}
                            >
                              Reset
                            </ActionButton>
                          </div>
                        </>
                      ) : null}

                      {designerPanel === 'add' && canAddStep ? (
                        <>
                          <VisualProcessorEditor
                            draft={newStepDraft}
                            onChange={(draft) => {
                              setNewStepDraft(draft);
                              setDesignerError(null);
                              setDesignerMessage(null);
                            }}
                            showStepTypePicker
                          />
                          <div className="flow-node-inspector-actions">
                            <ActionButton
                              type="button"
                              variant="primary"
                              onClick={handleAddVisualStep}
                              disabled={publishWorkflowMutation.isPending}
                              icon={PlusIcon}
                            >
                              Add step and publish
                            </ActionButton>
                            <ActionButton
                              type="button"
                              variant="secondary"
                              onClick={() => {
                                setNewStepDraft(defaultVisualProcessorDraft(newStepDraft.kind));
                                setDesignerError(null);
                                setDesignerMessage(null);
                              }}
                              disabled={publishWorkflowMutation.isPending}
                            >
                              Reset
                            </ActionButton>
                          </div>
                        </>
                      ) : null}

                      {designerPanel === 'advanced' ? (
                        <>
                          <JsonSyntaxEditor
                            value={selectedNodeJson}
                            onChange={(value) => {
                              setSelectedNodeJson(value);
                              setDesignerError(null);
                              setDesignerMessage(null);
                            }}
                            compact
                          />
                          <div className="flow-node-inspector-actions">
                            <ActionButton
                              type="button"
                              variant="primary"
                              onClick={handlePublishSelectedNode}
                              disabled={publishWorkflowMutation.isPending}
                              icon={CheckIcon}
                            >
                              Apply and publish
                            </ActionButton>
                            <ActionButton
                              type="button"
                              variant="secondary"
                              onClick={handleResetSelectedNode}
                              disabled={publishWorkflowMutation.isPending}
                            >
                              Reset
                            </ActionButton>
                          </div>
                        </>
                      ) : null}

                      {designerError ? <p className="inline-error">{designerError}</p> : null}
                      {designerMessage ? <p className="inline-success">{designerMessage}</p> : null}
                    </>
                  ) : (
                    <p className="section-copy">Select a source, processor, route, or destination node to edit it.</p>
                  )}
                </aside>
              ) : null}
            </div>
          ) : null}
          {tab === 'runs' ? (
            <Table
              data={[
                { t: '18:54:02', ev: 'purchase', id: 'evt_91a2', res: '200 OK', lat: 142 },
                { t: '18:54:01', ev: 'add_to_cart', id: 'evt_91a1', res: '200 OK', lat: 128 },
                { t: '18:54:00', ev: 'purchase', id: 'evt_91a0', res: '200 OK', lat: 156 },
                { t: '18:53:59', ev: 'view_item', id: 'evt_919f', res: '200 OK', lat: 122 },
                { t: '18:53:58', ev: 'identify', id: 'evt_919e', res: errored ? '429' : '200 OK', lat: errored ? 2400 : 118 },
              ]}
              columns={[
                {
                  key: 't',
                  header: 'Time',
                  width: proportional(0.6),
                  renderCell: (row) => <Text type="code" color="secondary">{row.t}</Text>,
                },
                {
                  key: 'ev',
                  header: 'Event',
                  width: proportional(1),
                  renderCell: (row) => (
                    <VStack gap={0.5}>
                      <Text type="body" weight="semibold" display="block">{row.ev}</Text>
                      <Text type="code" color="secondary" display="block">{row.id}</Text>
                    </VStack>
                  ),
                },
                {
                  key: 'transforms',
                  header: 'Transformations',
                  width: proportional(1.4),
                  renderCell: () => (
                    <Text type="code" color="secondary">{flow.processors.join(' → ') || '—'}</Text>
                  ),
                },
                {
                  key: 'res',
                  header: 'Sink result',
                  width: proportional(0.7),
                  renderCell: (row) => (
                    <Pill tone={row.res === '200 OK' ? 'good' : 'danger'}>{row.res}</Pill>
                  ),
                },
                {
                  key: 'lat',
                  header: 'Latency',
                  width: proportional(0.5),
                  align: 'end',
                  renderCell: (row) => <Text type="body" hasTabularNumbers>{row.lat}ms</Text>,
                },
              ] satisfies TableColumn<{ t: string; ev: string; id: string; res: string; lat: number }>[]}
              idKey="id"
              density="compact"
              hasHover
            />
          ) : null}
          {tab === 'revs' ? (
            <List density="balanced" hasDividers>
              {[
                { rev: flow.revisionId, when: 'active · 2h ago', by: 'bernard@rohrpost.dev', note: 'added redact step' },
                { rev: 'rev_8f213', when: 'archived · 1d ago', by: 'mika@rohrpost.dev', note: 'initial publish' },
                { rev: 'rev_7ac02', when: 'archived · 8d ago', by: 'bernard@rohrpost.dev', note: 'prototype' },
              ].map((r, i) => (
                <ListItem
                  key={r.rev}
                  label={`${r.rev} · ${r.note}`}
                  description={`${r.when} · ${r.by}`}
                  startContent={<Icon icon={ShareIcon} size="sm" />}
                  endContent={
                    i === 0 ? (
                      <Pill tone="good">active</Pill>
                    ) : (
                      <ActionButton type="button" variant="secondary">Rollback</ActionButton>
                    )
                  }
                />
              ))}
            </List>
          ) : null}
          {tab === 'config' ? (
            <CodeBlock language="json" size="sm" width="100%" code={JSON.stringify(
              {
                id: flow.id,
                revisionId: flow.revisionId,
                source: { kind: flow.sourceKind, label: flow.sourceLabel },
                processors: flow.processors.map((kind) => ({ kind })),
                sink: { label: flow.sinkLabel, guarantee: flow.sinkGuarantee },
                execution: flow.execution,
              },
              null,
              2,
            )} />
          ) : null}
          {tab === 'dlq' ? (
            backlog > 0 ? (
              <VStack gap={3}>
                <HStack gap={2} align="center" wrap="wrap">
                  <Pill tone="danger">{backlog.toLocaleString()} in DLQ</Pill>
                  <ActionButton type="button" variant="primary" icon={ArrowPathIcon}>
                    Replay all
                  </ActionButton>
                  <ActionButton type="button" variant="secondary" icon={ArrowDownTrayIcon}>
                    Download
                  </ActionButton>
                </HStack>
                <Table
                  data={Array.from({ length: 5 }).map((_, i) => ({
                    id: String(i),
                    time: `18:${(42 + i).toString().padStart(2, '0')}:${((i * 7) % 60).toString().padStart(2, '0')}`,
                    reason: deployment?.lastError ?? 'schema: missing order_id',
                    attempts: '4/4',
                    size: '248 B',
                  }))}
                  columns={[
                    {
                      key: 'time',
                      header: 'Time',
                      width: proportional(0.6),
                      renderCell: (row) => <Text type="code" color="secondary">{row.time}</Text>,
                    },
                    {
                      key: 'reason',
                      header: 'Reason',
                      width: proportional(2),
                      renderCell: (row) => <Text type="body">{row.reason}</Text>,
                    },
                    {
                      key: 'attempts',
                      header: 'Attempts',
                      width: proportional(0.5),
                      align: 'end',
                      renderCell: (row) => <Text type="body" hasTabularNumbers>{row.attempts}</Text>,
                    },
                    {
                      key: 'size',
                      header: 'Size',
                      width: proportional(0.5),
                      align: 'end',
                      renderCell: (row) => <Text type="body" hasTabularNumbers>{row.size}</Text>,
                    },
                  ] satisfies TableColumn<{ id: string; time: string; reason: string; attempts: string; size: string }>[]}
                  idKey="id"
                  density="compact"
                />
              </VStack>
            ) : (
              <EmptyState isCompact title="DLQ empty" description="Nothing to replay." />
            )
          ) : null}
        </div>
      </Panel>
    </VStack>
  );
}
