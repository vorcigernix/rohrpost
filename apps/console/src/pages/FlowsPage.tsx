import { useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Banner } from '@astryxdesign/core/Banner';
import { Button } from '@astryxdesign/core/Button';
import { EmptyState } from '@astryxdesign/core/EmptyState';
import { HStack } from '@astryxdesign/core/HStack';
import { SegmentedControl, SegmentedControlItem } from '@astryxdesign/core/SegmentedControl';
import { Table, pixel, proportional, type TableColumn } from '@astryxdesign/core/Table';
import { Badge } from '@astryxdesign/core/Badge';
import { Text } from '@astryxdesign/core/Text';
import { Token } from '@astryxdesign/core/Token';
import { VStack } from '@astryxdesign/core/VStack';
import { api } from '../lib/api';
import { describeConsoleError } from '../lib/error-state';
import { ActionLink, LoadingBlock, PageHeader, Panel, Pill, StatusDot } from '../components/ui';
import { Icon } from '@astryxdesign/core/Icon';
import { PlusIcon, TrashIcon } from '@heroicons/react/24/outline';
import type { FlowRecord, RuntimeDeploymentRecord } from '../lib/api-types';

type Tab = 'all' | 'issues' | 'paused';

function stateOfFlow(flow: FlowRecord, deployments: RuntimeDeploymentRecord[]): string {
  const d = deployments.find((x) => x.flowName === flow.name);
  if (d) return d.state;
  if (flow.status === 'paused') return 'idle';
  if (flow.status === 'draft') return 'idle';
  return 'healthy';
}

function stateTone(state: string): 'good' | 'info' | 'warn' | 'danger' | 'neutral' {
  if (state === 'healthy') return 'good';
  if (state === 'backlogged') return 'warn';
  if (state === 'degraded') return 'danger';
  if (state === 'idle') return 'neutral';
  return 'info';
}

function stateLabel(state: string): string {
  if (state === 'healthy') return 'Healthy';
  if (state === 'backlogged') return 'Backlog';
  if (state === 'degraded') return 'Degraded';
  if (state === 'idle') return 'Idle';
  return state;
}

function formatTime(value: string): string {
  return new Date(value).toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

interface FlowTableRow extends Record<string, unknown> {
  id: string;
  flow: FlowRecord;
  state: string;
  throughput: number;
  delivered: number;
  backlog: number;
}

export function FlowsPage() {
  const [tab, setTab] = useState<Tab>('all');
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const queryClient = useQueryClient();
  const flowsQuery = useQuery({ queryKey: ['flows'], queryFn: api.fetchFlows });
  const runtimeQuery = useQuery({ queryKey: ['runtime-stats'], queryFn: api.fetchRuntimeStats });
  const deleteMutation = useMutation({
    mutationFn: api.deleteFlow,
    onSuccess: async () => {
      setDeleteError(null);
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ['flows'] }),
        queryClient.invalidateQueries({ queryKey: ['runtime-stats'] }),
        queryClient.invalidateQueries({ queryKey: ['overview'] }),
      ]);
    },
    onError: (error) => {
      const errorState = describeConsoleError(error);
      setDeleteError(errorState.message);
    },
  });

  if (flowsQuery.isPending || runtimeQuery.isPending) {
    return (
      <VStack gap={5}>
        <PageHeader eyebrow="Build" title="Flows" />
        <Panel><LoadingBlock lines={4} /></Panel>
      </VStack>
    );
  }
  if (flowsQuery.isError || runtimeQuery.isError) {
    const errorState = describeConsoleError(flowsQuery.error ?? runtimeQuery.error);
    return (
      <VStack gap={5}>
        <PageHeader eyebrow="Build" title="Flows" />
        <Banner status="error" title={errorState.message} description={errorState.hint} />
      </VStack>
    );
  }

  const flows = flowsQuery.data;
  const deployments = runtimeQuery.data.deployments;

  const filtered = flows.filter((flow) => {
    if (tab === 'all') return true;
    if (tab === 'paused') return flow.status === 'paused';
    const state = stateOfFlow(flow, deployments);
    return state !== 'healthy' && state !== 'idle';
  });

  const issuesCount = flows.filter((f) => {
    const s = stateOfFlow(f, deployments);
    return s !== 'healthy' && s !== 'idle';
  }).length;

  const requestDeleteFlow = (flow: FlowRecord) => {
    const confirmed = window.confirm(`Delete "${flow.name}"? This removes the flow and deactivates its runtime deployment.`);
    if (!confirmed) {
      return;
    }

    setDeleteError(null);
    deleteMutation.mutate(flow.id);
  };

  const rows: FlowTableRow[] = filtered.map((flow) => {
    const d = deployments.find((x) => x.flowName === flow.name);
    return {
      id: flow.id,
      flow,
      state: stateOfFlow(flow, deployments),
      throughput: d ? Math.max(0, d.deliveredCount / 3600) : 0,
      delivered: d?.deliveredCount ?? 0,
      backlog: d?.backlogCount ?? 0,
    };
  });

  const columns: TableColumn<FlowTableRow>[] = [
    {
      key: 'status',
      header: '',
      width: pixel(36),
      renderCell: (row) => <StatusDot tone={stateTone(row.state)} />,
    },
    {
      key: 'name',
      header: 'Flow',
      width: proportional(1.4),
      renderCell: (row) => (
        <VStack gap={0.5}>
          <Text type="body" weight="semibold" display="block" maxLines={1}>{row.flow.name}</Text>
          <Text type="supporting" color="secondary" display="block" maxLines={1}>
            {row.flow.execution} · {row.flow.revisionId} · {formatTime(row.flow.updatedAt)}
          </Text>
        </VStack>
      ),
    },
    {
      key: 'pipeline',
      header: 'Pipeline',
      width: proportional(1.6),
      renderCell: (row) => (
        <HStack gap={1} wrap="wrap">
          <Token label={row.flow.sourceKind} color="blue" />
          {row.flow.processors.map((p, i) => (
            <Token key={`${p}-${i}`} label={p} />
          ))}
          <Token label={row.flow.sinkGuarantee} color="green" />
        </HStack>
      ),
    },
    {
      key: 'state',
      header: 'State',
      width: pixel(110),
      renderCell: (row) => <Pill tone={stateTone(row.state)}>{stateLabel(row.state)}</Pill>,
    },
    {
      key: 'throughput',
      header: 'Throughput',
      width: proportional(0.6),
      align: 'end',
      renderCell: (row) => <Text type="body" hasTabularNumbers>{row.throughput.toFixed(1)}/s</Text>,
    },
    {
      key: 'delivered',
      header: 'Delivered · 1h',
      width: proportional(0.6),
      align: 'end',
      renderCell: (row) => <Text type="body" hasTabularNumbers>{row.delivered.toLocaleString()}</Text>,
    },
    {
      key: 'backlog',
      header: 'Backlog',
      width: proportional(0.5),
      align: 'end',
      renderCell: (row) =>
        row.backlog > 0 ? (
          <Badge variant="warning" label={row.backlog.toLocaleString()} />
        ) : (
          <Text type="body" color="secondary" hasTabularNumbers>0</Text>
        ),
    },
    {
      key: 'actions',
      header: '',
      width: pixel(170),
      align: 'end',
      renderCell: (row) => (
        <HStack gap={1} justify="end">
          <ActionLink to="/flows/$flowId" params={{ flowId: row.flow.id }} variant="secondary" size="sm">
            Open
          </ActionLink>
          <Button
            label={`Delete ${row.flow.name}`}
            icon={<Icon icon={TrashIcon} />}
            variant="destructive"
            size="sm"
            isDisabled={deleteMutation.isPending && deleteMutation.variables === row.flow.id}
            onClick={() => requestDeleteFlow(row.flow)}
          >
            Delete
          </Button>
        </HStack>
      ),
    },
  ];

  return (
    <VStack gap={5}>
      <PageHeader
        eyebrow="Build"
        title="Flows"
        sub="Every flow, with its source, transform chain, and sink at a glance. Click through for runtime detail."
        actions={
          <>
            <SegmentedControl value={tab} onChange={(value) => setTab(value as Tab)} label="Flow filter">
              <SegmentedControlItem value="all" label={`All ${flows.length}`} />
              <SegmentedControlItem value="issues" label={`Needs attention ${issuesCount}`} />
              <SegmentedControlItem value="paused" label="Paused" />
            </SegmentedControl>
            <ActionLink to="/compose" variant="primary" icon={PlusIcon}>
              New flow
            </ActionLink>
          </>
        }
      />

      {deleteError ? (
        <Banner status="error" title="Could not delete flow" description={deleteError} />
      ) : null}

      {rows.length > 0 ? (
        <Panel>
          <Table<FlowTableRow>
            data={rows}
            columns={columns}
            idKey="id"
            density="compact"
            hasHover
          />
        </Panel>
      ) : (
        <Panel>
          <EmptyState
            title={tab === 'all' ? 'No flows yet' : 'Nothing matches this filter'}
            description={tab === 'all' ? 'Compose your first flow to see it here.' : 'Switch the filter to see other flows.'}
            actions={
              <ActionLink to="/compose" variant="primary">
                New flow
              </ActionLink>
            }
          />
        </Panel>
      )}
    </VStack>
  );
}
