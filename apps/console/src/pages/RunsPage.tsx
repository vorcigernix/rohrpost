import { useQuery } from '@tanstack/react-query';
import { Banner } from '@astryxdesign/core/Banner';
import { EmptyState } from '@astryxdesign/core/EmptyState';
import { Grid } from '@astryxdesign/core/Grid';
import { HStack } from '@astryxdesign/core/HStack';
import { Table, proportional, type TableColumn } from '@astryxdesign/core/Table';
import { Text } from '@astryxdesign/core/Text';
import { VStack } from '@astryxdesign/core/VStack';
import { api } from '../lib/api';
import type { AdapterWorkloadRecord, RuntimeDeploymentRecord } from '../lib/api-types';
import { describeConsoleError } from '../lib/error-state';
import { LoadingBlock, MetricCard, PageHeader, Panel, PanelHeader, StatusDot } from '../components/ui';

function formatTime(value: string | null | undefined): string {
  if (!value) return '—';
  return new Date(value).toLocaleString(undefined, {
    month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit',
  });
}

function statusTone(status: string): 'good' | 'info' | 'warn' | 'danger' {
  switch (status) {
    case 'healthy': return 'good';
    case 'idle': return 'info';
    case 'backlogged': return 'warn';
    default: return 'danger';
  }
}

function workloadTone(status: string): 'good' | 'info' | 'warn' | 'danger' {
  switch (status) {
    case 'running': return 'good';
    case 'starting': return 'info';
    case 'stopped': return 'warn';
    default: return 'danger';
  }
}

interface DeploymentRow extends Record<string, unknown> {
  id: string;
  d: RuntimeDeploymentRecord;
}

interface WorkloadRow extends Record<string, unknown> {
  id: string;
  w: AdapterWorkloadRecord;
}

const deploymentColumns: TableColumn<DeploymentRow>[] = [
  {
    key: 'flow',
    header: 'Flow',
    width: proportional(1.6),
    renderCell: ({ d }) => (
      <VStack gap={0.5}>
        <Text type="body" weight="semibold" display="block" maxLines={1}>{d.flowName}</Text>
        <Text type="supporting" color="secondary" display="block" maxLines={1}>
          {d.revisionId}{d.lastError ? ` · ${d.lastError}` : ''}
        </Text>
      </VStack>
    ),
  },
  {
    key: 'rollout',
    header: 'Rollout',
    width: proportional(0.8),
    renderCell: ({ d }) => <Text type="body" color="secondary">{d.rolloutStatus}</Text>,
  },
  {
    key: 'state',
    header: 'State',
    width: proportional(0.8),
    renderCell: ({ d }) => (
      <HStack gap={1.5} align="center">
        <StatusDot tone={statusTone(d.state)} />
        <Text type="body">{d.state}</Text>
      </HStack>
    ),
  },
  {
    key: 'accepted',
    header: 'Accepted',
    width: proportional(0.6),
    align: 'end',
    renderCell: ({ d }) => <Text type="body" hasTabularNumbers>{d.acceptedCount.toLocaleString()}</Text>,
  },
  {
    key: 'delivered',
    header: 'Delivered',
    width: proportional(0.6),
    align: 'end',
    renderCell: ({ d }) => <Text type="body" hasTabularNumbers>{d.deliveredCount.toLocaleString()}</Text>,
  },
  {
    key: 'backlog',
    header: 'Backlog',
    width: proportional(0.6),
    align: 'end',
    renderCell: ({ d }) => <Text type="body" hasTabularNumbers>{d.backlogCount.toLocaleString()}</Text>,
  },
  {
    key: 'inflight',
    header: 'Inflight',
    width: proportional(0.5),
    align: 'end',
    renderCell: ({ d }) => <Text type="body" hasTabularNumbers>{d.inflightCount}</Text>,
  },
  {
    key: 'lastProcessed',
    header: 'Last processed',
    width: proportional(0.9),
    renderCell: ({ d }) => (
      <Text type="supporting" color="secondary" hasTabularNumbers>{formatTime(d.lastProcessedAt)}</Text>
    ),
  },
];

const workloadColumns: TableColumn<WorkloadRow>[] = [
  {
    key: 'connector',
    header: 'Connector',
    width: proportional(1.8),
    renderCell: ({ w }) => (
      <VStack gap={0.5}>
        <Text type="body" weight="semibold" display="block" maxLines={1}>{w.connectorId}</Text>
        <Text type="supporting" color="secondary" display="block" maxLines={1}>
          {w.capabilityId}{' · '}{w.deploymentIds.length} deployment{w.deploymentIds.length === 1 ? '' : 's'}{w.lastError ? ` · ${w.lastError}` : ''}
        </Text>
      </VStack>
    ),
  },
  {
    key: 'role',
    header: 'Role',
    width: proportional(0.7),
    renderCell: ({ w }) => <Text type="body" color="secondary">{w.runtimeRole}</Text>,
  },
  {
    key: 'backend',
    header: 'Backend',
    width: proportional(0.7),
    renderCell: ({ w }) => <Text type="body" color="secondary">{w.backend}</Text>,
  },
  {
    key: 'status',
    header: 'Status',
    width: proportional(0.8),
    renderCell: ({ w }) => (
      <HStack gap={1.5} align="center">
        <StatusDot tone={workloadTone(w.status)} />
        <Text type="body">{w.status}</Text>
      </HStack>
    ),
  },
  {
    key: 'restarts',
    header: 'Restarts',
    width: proportional(0.5),
    align: 'end',
    renderCell: ({ w }) => <Text type="body" hasTabularNumbers>{w.restartCount}</Text>,
  },
  {
    key: 'lastReport',
    header: 'Last report',
    width: proportional(0.9),
    renderCell: ({ w }) => (
      <Text type="supporting" color="secondary" hasTabularNumbers>{formatTime(w.reportedAt)}</Text>
    ),
  },
];

export function RunsPage() {
  const runtimeQuery = useQuery({ queryKey: ['runtime-stats'], queryFn: api.fetchRuntimeStats });
  const adapterWorkloadsQuery = useQuery({ queryKey: ['adapter-workloads'], queryFn: api.fetchAdapterWorkloads });

  if (runtimeQuery.isPending) {
    return (
      <VStack gap={5}>
        <PageHeader eyebrow="Runtime" title="Runtime health" sub="Loading deployment posture." />
        <Panel><LoadingBlock lines={5} /></Panel>
      </VStack>
    );
  }

  if (runtimeQuery.isError) {
    const errorState = describeConsoleError(runtimeQuery.error);
    return (
      <VStack gap={5}>
        <PageHeader eyebrow="Runtime" title="Runtime health" />
        <Banner status="error" title={errorState.message} description={errorState.hint} />
      </VStack>
    );
  }

  const runtime = runtimeQuery.data;
  const adapterWorkloads = adapterWorkloadsQuery.data ?? [];
  const deploymentRows: DeploymentRow[] = runtime.deployments.map((d) => ({ id: d.deploymentId, d }));
  const workloadRows: WorkloadRow[] = adapterWorkloads.map((w) => ({ id: `${w.reporterId}:${w.key}`, w }));

  return (
    <VStack gap={5}>
      <PageHeader
        eyebrow="Runtime"
        title="Health and backlog"
        sub="Operational posture. Spans, logs, and fine-grained metrics belong in OTEL."
      />

      <Grid columns={{ minWidth: 200 }} gap={3}>
        <MetricCard label="Healthy" value={String(runtime.summary.healthyDeployments)} detail="Deployments without degradation" tone="good" />
        <MetricCard label="Backlog" value={String(runtime.summary.backlogCount)} detail="Undrained messages" tone="neutral" />
        <MetricCard label="Delivered" value={runtime.summary.deliveredCount.toLocaleString()} detail="Sink acknowledgements" tone="info" />
      </Grid>

      <Panel>
        <VStack gap={2}>
          <PanelHeader eyebrow="OTEL" title="Observability mode" />
          <Text type="supporting" color="secondary" display="block">
            Traces and metrics: {runtime.observability.mode}. Console shows compact health only.
          </Text>
          <Text type="code" color="secondary" display="block">
            Last processed: {formatTime(runtime.summary.lastProcessedAt)} &middot; Inflight: {runtime.summary.inflightCount}
          </Text>
        </VStack>
      </Panel>

      <Panel>
        <VStack gap={3}>
          <PanelHeader eyebrow="Deployments" title="Runtime deployments" />
          {deploymentRows.length > 0 ? (
            <Table<DeploymentRow>
              data={deploymentRows}
              columns={deploymentColumns}
              idKey="id"
              density="compact"
              hasHover
            />
          ) : (
            <EmptyState isCompact title="No deployments yet" description="Publish a flow to see its runtime deployment here." />
          )}
        </VStack>
      </Panel>

      <Panel>
        <VStack gap={3}>
          <PanelHeader eyebrow="Adapters" title="Managed workloads" />
          <Text type="supporting" color="secondary" display="block">
            Redpanda Connect sources and sinks supervised by the adapter plane.
          </Text>
          {adapterWorkloadsQuery.isPending ? (
            <LoadingBlock lines={3} />
          ) : adapterWorkloadsQuery.isError ? (
            <Banner status="error" title={describeConsoleError(adapterWorkloadsQuery.error).message} />
          ) : workloadRows.length === 0 ? (
            <EmptyState isCompact title="No adapter workloads" description="No adapter-managed workloads have reported yet." />
          ) : (
            <Table<WorkloadRow>
              data={workloadRows}
              columns={workloadColumns}
              idKey="id"
              density="compact"
              hasHover
            />
          )}
        </VStack>
      </Panel>
    </VStack>
  );
}
