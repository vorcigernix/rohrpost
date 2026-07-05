import { useMemo, useState } from 'react';
import { useNavigate } from '@tanstack/react-router';
import { useQuery } from '@tanstack/react-query';
import { Badge } from '@astryxdesign/core/Badge';
import { EmptyState } from '@astryxdesign/core/EmptyState';
import { Grid } from '@astryxdesign/core/Grid';
import { HStack } from '@astryxdesign/core/HStack';
import { List, ListItem } from '@astryxdesign/core/List';
import { SegmentedControl, SegmentedControlItem } from '@astryxdesign/core/SegmentedControl';
import { StackItem } from '@astryxdesign/core/Stack';
import { Table, pixel, proportional, type TableColumn } from '@astryxdesign/core/Table';
import { Text } from '@astryxdesign/core/Text';
import { Token } from '@astryxdesign/core/Token';
import { VStack } from '@astryxdesign/core/VStack';
import { api } from '../lib/api';
import { describeConsoleError } from '../lib/error-state';
import {
  ActionLink,
  LoadingBlock,
  MetricTile,
  PageHeader,
  Panel,
  PanelHeader,
  Pipeline,
  StatusDot,
} from '../components/ui';
import { ArrowRightIcon, PlusIcon } from '@heroicons/react/24/outline';
import { Sparkline, genSeries } from '../components/primitives';
import { deriveActivityItems, deriveInboxItems, formatRelative, isFreshRuntimeSignal } from '../lib/inbox';

function flowTone(state: string): 'good' | 'info' | 'warn' | 'danger' | 'neutral' {
  switch (state) {
    case 'healthy': return 'good';
    case 'backlogged': return 'warn';
    case 'degraded': return 'danger';
    case 'idle': return 'neutral';
    default: return 'info';
  }
}

function Metric({ label, value, unit, sub, seed, color }: {
  label: string;
  value: string;
  unit?: string;
  sub?: string;
  seed?: number;
  color?: string;
}) {
  const data = useMemo(() => (seed !== undefined ? genSeries(seed) : []), [seed]);
  return (
    <MetricTile
      label={label}
      value={value}
      unit={unit}
      sub={sub}
      spark={seed !== undefined ? <Sparkline data={data} color={color} /> : undefined}
    />
  );
}

interface FlowRow extends Record<string, unknown> {
  id: string;
  name: string;
  execution: string;
  revisionId: string;
  sourceKind: string;
  processors: string[];
  sinkGuarantee: string;
  state: string;
  throughput: number;
  backlog: number;
}

function deriveThroughput(deployments: Array<{ deliveredCount: number; acceptedCount: number }>): number {
  return deployments.reduce((sum, d) => sum + Math.max(0, d.deliveredCount - d.acceptedCount * 0), 0);
}

export function PulsePage() {
  const [range, setRange] = useState('1h');
  const navigate = useNavigate();
  const flowsQuery = useQuery({ queryKey: ['flows'], queryFn: api.fetchFlows });
  const runtimeQuery = useQuery({ queryKey: ['runtime-stats'], queryFn: api.fetchRuntimeStats });
  const adapterWorkloadsQuery = useQuery({ queryKey: ['adapter-workloads'], queryFn: api.fetchAdapterWorkloads });

  if (flowsQuery.isPending || runtimeQuery.isPending) {
    return (
      <VStack gap={5}>
        <PageHeader eyebrow="Operate · Live" title="Pulse" />
        <Panel><LoadingBlock lines={5} /></Panel>
      </VStack>
    );
  }

  if (flowsQuery.isError || runtimeQuery.isError) {
    const errorState = describeConsoleError(flowsQuery.error ?? runtimeQuery.error);
    return (
      <VStack gap={5}>
        <PageHeader eyebrow="Operate · Live" title="Pulse" />
        <Panel>
          <VStack gap={1}>
            <Text type="supporting" color="secondary" display="block">{errorState.message}</Text>
            <Text type="supporting" color="secondary" display="block">{errorState.hint}</Text>
          </VStack>
        </Panel>
      </VStack>
    );
  }

  const flows = flowsQuery.data;
  const runtime = runtimeQuery.data;
  const freshDeployments = runtime.deployments.filter((deployment) =>
    isFreshRuntimeSignal(deployment.updatedAt, deployment.lastProcessedAt, deployment.lastAcceptedAt),
  );
  const topIssues = deriveInboxItems({
    runtime,
    adapterWorkloads: adapterWorkloadsQuery.data,
  }).filter((i) => i.severity !== 'info').slice(0, 3);
  const activityItems = deriveActivityItems(runtime);
  const deliveredCount = freshDeployments.reduce((sum, deployment) => sum + deployment.deliveredCount, 0);
  const processedCount = freshDeployments.reduce((sum, deployment) => sum + deployment.processedCount, 0);
  const backlogCount = freshDeployments.reduce((sum, deployment) => sum + deployment.backlogCount, 0);
  const dlqTotal = freshDeployments.reduce((sum, deployment) => sum + deployment.dlqCount, 0);
  const dlqFlowCount = freshDeployments.filter((deployment) => deployment.dlqCount > 0).length;
  const errorRate = processedCount > 0
    ? (dlqTotal / processedCount) * 100
    : 0;
  const degradedFlow = flows.find((f) => freshDeployments.find((d) => d.flowName === f.name && d.state === 'degraded'))
    ?? flows.find((f) => f.status === 'degraded')
    ?? null;

  const stateForFlow = (flowName: string): string => {
    const d = freshDeployments.find((x) => x.flowName === flowName);
    return d?.state ?? 'idle';
  };

  const metricsForFlow = (flowName: string) => {
    const d = freshDeployments.find((x) => x.flowName === flowName);
    return {
      delivered: d?.deliveredCount ?? 0,
      backlog: d?.backlogCount ?? 0,
      throughput: d ? Math.max(0, d.deliveredCount / 3600) : 0,
    };
  };

  const flowRows: FlowRow[] = flows.map((f) => {
    const m = metricsForFlow(f.name);
    return {
      id: f.id,
      name: f.name,
      execution: f.execution,
      revisionId: f.revisionId,
      sourceKind: f.sourceKind,
      processors: f.processors,
      sinkGuarantee: f.sinkGuarantee,
      state: stateForFlow(f.name),
      throughput: m.throughput,
      backlog: m.backlog,
    };
  });

  const flowColumns: TableColumn<FlowRow>[] = [
    {
      key: 'state',
      header: '',
      width: pixel(36),
      renderCell: (row) => <StatusDot tone={flowTone(row.state)} />,
    },
    {
      key: 'name',
      header: 'Flow',
      width: proportional(1.2),
      renderCell: (row) => (
        <VStack gap={0.5}>
          <Text type="body" weight="semibold" display="block" maxLines={1}>{row.name}</Text>
          <Text type="supporting" color="secondary" display="block" maxLines={1}>
            {row.execution} · {row.revisionId}
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
          <Token label={row.sourceKind} color="blue" />
          {row.processors.slice(0, 2).map((p, i) => (
            <Token key={`${p}-${i}`} label={p} />
          ))}
          {row.processors.length > 2 ? <Token label={`+${row.processors.length - 2}`} /> : null}
          <Token label={row.sinkGuarantee} color="green" />
        </HStack>
      ),
    },
    {
      key: 'throughput',
      header: 'Throughput',
      width: pixel(110),
      align: 'end',
      renderCell: (row) => (
        <Text type="body" hasTabularNumbers>{row.throughput.toFixed(1)}/s</Text>
      ),
    },
    {
      key: 'backlog',
      header: 'Backlog',
      width: pixel(96),
      align: 'end',
      renderCell: (row) =>
        row.backlog > 0 ? (
          <Badge variant="warning" label={row.backlog.toLocaleString()} />
        ) : (
          <Text type="body" color="secondary" hasTabularNumbers>0</Text>
        ),
    },
  ];

  return (
    <VStack gap={5}>
      <PageHeader
        eyebrow="Operate · Live"
        title="Pulse"
        sub="Throughput, backlog, errors and the flows that need attention — all at a glance."
        actions={
          <>
            <SegmentedControl value={range} onChange={setRange} label="Pulse time range">
              <SegmentedControlItem value="1h" label="Last 1 hour" />
              <SegmentedControlItem value="6h" label="6h" />
              <SegmentedControlItem value="24h" label="24h" />
            </SegmentedControl>
            <ActionLink to="/compose" variant="primary" icon={PlusIcon}>
              New flow
            </ActionLink>
          </>
        }
      />

      <Grid columns={{ minWidth: 180 }} gap={3}>
        <Metric label="Throughput" value={deriveThroughput(freshDeployments).toLocaleString()} unit="msgs/h" sub="delivered" seed={1} color="var(--color-accent)" />
        <Metric label="Delivered" value={deliveredCount.toLocaleString()} sub="last 24h" seed={2} color="var(--color-success)" />
        <Metric label="Backlog" value={backlogCount.toLocaleString()} sub="active backlog" seed={3.3} color="var(--color-warning)" />
        <Metric label="Error rate" value={errorRate.toFixed(2)} unit="%" sub="DLQ / processed" seed={4} color="var(--color-error)" />
        <Metric label="DLQ total" value={dlqTotal.toLocaleString()} sub={`${dlqFlowCount} flow${dlqFlowCount === 1 ? '' : 's'}`} seed={5.7} color="var(--color-error)" />
        <Metric label="p95 latency" value="184" unit="ms" sub="in SLO" seed={6} color="var(--color-accent)" />
      </Grid>

      <HStack gap={3} align="start">
        <StackItem size="fill" isScrollable={false}>
          <Panel>
            <VStack gap={3}>
              <PanelHeader
                eyebrow="Flows"
                title={`${flows.length} active · ${flows.filter((f) => f.status === 'published').length} published · ${flows.filter((f) => f.status === 'draft').length} draft`}
                actions={
                  <ActionLink to="/flows" variant="secondary" endIcon={ArrowRightIcon}>
                    View all
                  </ActionLink>
                }
              />
              <Table<FlowRow>
                data={flowRows}
                columns={flowColumns}
                idKey="id"
                density="compact"
                hasHover
              />
            </VStack>
          </Panel>
        </StackItem>

        <StackItem>
        <VStack gap={3} width={360}>
          <Panel>
            <VStack gap={3}>
              <PanelHeader
                eyebrow="Needs attention"
                title={`${topIssues.length} active incident${topIssues.length !== 1 ? 's' : ''}`}
                actions={
                  <ActionLink to="/inbox" variant="secondary" endIcon={ArrowRightIcon}>
                    Inbox
                  </ActionLink>
                }
              />
              {topIssues.length > 0 ? (
                <List density="compact" hasDividers>
                  {topIssues.map((i) => (
                    <ListItem
                      key={i.id}
                      label={i.title}
                      description={`${i.flowName} · ${formatRelative(i.occurredAt)}`}
                      startContent={<StatusDot tone={i.severity === 'danger' ? 'danger' : 'warn'} />}
                      onClick={() => void navigate({ to: '/inbox' })}
                    />
                  ))}
                </List>
              ) : (
                <EmptyState isCompact title="All clear" description="Nothing needs attention." />
              )}
            </VStack>
          </Panel>

          <Panel>
            <VStack gap={3}>
              <PanelHeader eyebrow="Recent activity" title="Last 24 hours" />
              {activityItems.length > 0 ? (
                <List density="compact" hasDividers>
                  {activityItems.map((a, i) => (
                    <ListItem
                      key={`${a.title}-${i}`}
                      label={a.title}
                      description={a.detail}
                      startContent={<StatusDot tone={a.tone} />}
                      endContent={
                        <Text type="supporting" color="secondary" hasTabularNumbers>
                          {formatRelative(a.time)}
                        </Text>
                      }
                    />
                  ))}
                </List>
              ) : (
                <EmptyState isCompact title="No runtime activity" description="No runtime activity reported yet." />
              )}
            </VStack>
          </Panel>
        </VStack>
        </StackItem>
      </HStack>

      {degradedFlow ? (
        <Panel>
          <VStack gap={3}>
            <PanelHeader
              eyebrow="Degraded flow"
              title={degradedFlow.name}
              actions={
                <ActionLink to="/flows/$flowId" params={{ flowId: degradedFlow.id }} variant="secondary" endIcon={ArrowRightIcon}>
                  Open
                </ActionLink>
              }
            />
            <Pipeline
              variant="hero"
              error
              nodes={[
                { role: 'source', kind: degradedFlow.sourceKind, label: degradedFlow.sourceLabel },
                ...degradedFlow.processors.map((p) => ({ role: 'processor' as const, kind: p, label: p })),
                { role: 'sink', kind: degradedFlow.sinkGuarantee, label: degradedFlow.sinkLabel },
              ]}
            />
          </VStack>
        </Panel>
      ) : null}
    </VStack>
  );
}
