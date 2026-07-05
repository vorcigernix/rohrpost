import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Badge } from '@astryxdesign/core/Badge';
import { Banner } from '@astryxdesign/core/Banner';
import { CodeBlock } from '@astryxdesign/core/CodeBlock';
import { EmptyState } from '@astryxdesign/core/EmptyState';
import { Heading } from '@astryxdesign/core/Heading';
import { HStack } from '@astryxdesign/core/HStack';
import { List, ListItem } from '@astryxdesign/core/List';
import { SegmentedControl, SegmentedControlItem } from '@astryxdesign/core/SegmentedControl';
import { StackItem } from '@astryxdesign/core/Stack';
import { Text } from '@astryxdesign/core/Text';
import { VStack } from '@astryxdesign/core/VStack';
import { Icon, type IconType } from '@astryxdesign/core/Icon';
import {
  ArchiveBoxIcon,
  ArrowDownTrayIcon,
  ArrowPathIcon,
  BellIcon,
  BoltIcon,
  CheckIcon,
  ExclamationTriangleIcon,
  EyeIcon,
  InboxIcon,
  PauseIcon,
  PencilSquareIcon,
  RectangleGroupIcon,
  ShareIcon,
  TrashIcon,
} from '@heroicons/react/24/outline';
import { api } from '../lib/api';
import { describeConsoleError } from '../lib/error-state';
import { ActionButton, ActionLink, LoadingBlock, PageHeader, Panel, Pill, StatusDot } from '../components/ui';
import { deriveInboxItems, formatRelative, inboxUnreadCount, type InboxKind } from '../lib/inbox';

type Filter = 'all' | 'unread' | InboxKind;

const KIND_LABELS: Record<InboxKind, string> = {
  dlq: 'DLQ',
  degraded: 'Degraded',
  schema: 'Schema',
  deploy: 'Deploy',
  adapter: 'Adapter',
};

function kindIcon(kind: InboxKind): IconType {
  if (kind === 'dlq') return ArchiveBoxIcon;
  if (kind === 'degraded' || kind === 'adapter') return ExclamationTriangleIcon;
  if (kind === 'schema') return RectangleGroupIcon;
  return ShareIcon;
}

function severityTone(severity: string): 'danger' | 'warn' | 'info' {
  if (severity === 'danger') return 'danger';
  if (severity === 'warn') return 'warn';
  return 'info';
}

export function InboxPage() {
  const [filter, setFilter] = useState<Filter>('all');
  const [selectedId, setSelectedId] = useState<string>('');
  const runtimeQuery = useQuery({ queryKey: ['runtime-stats'], queryFn: api.fetchRuntimeStats });
  const adapterWorkloadsQuery = useQuery({ queryKey: ['adapter-workloads'], queryFn: api.fetchAdapterWorkloads });
  const inboxItems = deriveInboxItems({
    runtime: runtimeQuery.data,
    adapterWorkloads: adapterWorkloadsQuery.data,
  });

  const filtered = inboxItems.filter((i) => {
    if (filter === 'all') return true;
    if (filter === 'unread') return i.unread;
    return i.kind === filter;
  });

  const selected = inboxItems.find((i) => i.id === selectedId) ?? filtered[0] ?? null;
  const unreadCount = inboxUnreadCount(inboxItems);

  if (runtimeQuery.isPending) {
    return (
      <VStack gap={5}>
        <PageHeader eyebrow="Operate · Inbox" title="Incidents & activity" />
        <Panel><LoadingBlock lines={5} /></Panel>
      </VStack>
    );
  }

  if (runtimeQuery.isError) {
    const errorState = describeConsoleError(runtimeQuery.error);
    return (
      <VStack gap={5}>
        <PageHeader eyebrow="Operate · Inbox" title="Incidents & activity" />
        <Banner status="error" title={errorState.message} description={errorState.hint} />
      </VStack>
    );
  }

  return (
    <VStack gap={5}>
      <PageHeader
        eyebrow="Operate · Inbox"
        title="Incidents & activity"
        sub="Every failure, DLQ event, schema reject and deploy in one feed. Replay, silence, or jump into the flow."
        actions={
          <>
            <ActionButton type="button" variant="secondary" icon={BellIcon}>
              Alert routes
            </ActionButton>
            <ActionButton type="button" variant="secondary" icon={ArrowDownTrayIcon}>
              Export
            </ActionButton>
          </>
        }
      />

      <HStack justify="between" align="center" gap={3} wrap="wrap">
        <SegmentedControl value={filter} onChange={(value) => setFilter(value as Filter)} label="Inbox filter">
          <SegmentedControlItem value="all" label={`All ${inboxItems.length}`} />
          <SegmentedControlItem value="unread" label={`Unread ${unreadCount}`} />
          {(['dlq', 'degraded', 'adapter', 'deploy'] as InboxKind[]).map((kind) => (
            <SegmentedControlItem key={kind} value={kind} label={KIND_LABELS[kind]} />
          ))}
        </SegmentedControl>
        <Text type="supporting" color="secondary">Auto-refreshes every 15s</Text>
      </HStack>

      <HStack gap={3} align="start" wrap="wrap">
        <VStack gap={2} width={340}>
          <HStack justify="between" align="center" gap={2}>
            <Text type="supporting" color="secondary" weight="semibold">
              {filtered.length} item{filtered.length === 1 ? '' : 's'}
            </Text>
            <ActionButton type="button" variant="secondary" size="sm" icon={CheckIcon} disabled={filtered.length === 0}>
              Mark all read
            </ActionButton>
          </HStack>
          {filtered.length > 0 ? (
            <Panel>
              <List density="compact" hasDividers>
                {filtered.map((i) => (
                  <ListItem
                    key={i.id}
                    label={i.title}
                    description={i.flowName}
                    isSelected={selected?.id === i.id}
                    startContent={<Icon icon={kindIcon(i.kind)} size="sm" />}
                    endContent={
                      <VStack gap={0.5} align="end">
                        <Text type="supporting" color="secondary" hasTabularNumbers>
                          {formatRelative(i.occurredAt)}
                        </Text>
                        {i.count > 1 && i.severity !== 'info' ? (
                          <Badge variant="neutral" label={i.count.toLocaleString()} />
                        ) : null}
                      </VStack>
                    }
                    onClick={() => setSelectedId(i.id)}
                  />
                ))}
              </List>
            </Panel>
          ) : (
            <Panel>
              <EmptyState isCompact title="No active items" description="No active items for this filter." />
            </Panel>
          )}
        </VStack>

        <StackItem size="fill">
          <Panel>
            {selected ? (
              <VStack gap={4}>
                <HStack justify="between" align="start" gap={3} wrap="wrap">
                  <VStack gap={1.5}>
                    <HStack gap={2} align="center" wrap="wrap">
                      <Pill tone={severityTone(selected.severity)}>{selected.kind.toUpperCase()}</Pill>
                      <Text type="code" color="secondary">
                        {new Date(selected.occurredAt).toLocaleString()} · {selected.id}
                      </Text>
                    </HStack>
                    <Heading level={3}>{selected.title}</Heading>
                    <Text type="supporting" color="secondary" display="block">{selected.summary}</Text>
                  </VStack>
                  {selected.flowId ? (
                    <ActionLink to="/flows/$flowId" params={{ flowId: selected.flowId }} variant="secondary" icon={RectangleGroupIcon}>
                      Open flow
                    </ActionLink>
                  ) : null}
                </HStack>

                <HStack gap={2} wrap="wrap">
                  {selected.kind === 'dlq' ? (
                    <>
                      <ActionButton type="button" variant="primary" icon={ArrowPathIcon} label={`Replay ${selected.count.toLocaleString()} messages`}>
                        {`Replay ${selected.count.toLocaleString()} msgs`}
                      </ActionButton>
                      <ActionButton type="button" variant="secondary" icon={ArrowDownTrayIcon}>
                        Download NDJSON
                      </ActionButton>
                      <ActionButton type="button" variant="destructive" icon={TrashIcon}>
                        Purge
                      </ActionButton>
                    </>
                  ) : null}
                  {selected.kind === 'degraded' ? (
                    <>
                      <ActionButton type="button" variant="primary" icon={PauseIcon}>
                        Pause sink
                      </ActionButton>
                      <ActionButton type="button" variant="secondary" icon={BoltIcon}>
                        Throttle
                      </ActionButton>
                    </>
                  ) : null}
                  {selected.kind === 'schema' ? (
                    <>
                      <ActionButton type="button" variant="primary" icon={PencilSquareIcon}>
                        Edit mapping
                      </ActionButton>
                      <ActionButton type="button" variant="secondary" icon={EyeIcon}>
                        View payload
                      </ActionButton>
                    </>
                  ) : null}
                  {selected.kind === 'deploy' ? (
                    <>
                      <ActionButton type="button" variant="secondary" icon={ShareIcon}>
                        Diff vs previous
                      </ActionButton>
                      <ActionButton type="button" variant="secondary" icon={ArrowPathIcon}>
                        Rollback
                      </ActionButton>
                    </>
                  ) : null}
                  <ActionButton type="button" variant="secondary" icon={BellIcon}>
                    Silence 1h
                  </ActionButton>
                </HStack>

                <VStack gap={1.5}>
                  <Text type="supporting" color="secondary" weight="semibold" display="block">
                    STACK TRACE · SAMPLE
                  </Text>
                  <CodeBlock
                    code={selected.stack.map((l) => l.line).join('\n')}
                    language="plaintext"
                    size="sm"
                    width="100%"
                    highlightLines={selected.stack
                      .map((l, idx) => (l.kind === 'error' ? idx + 1 : null))
                      .filter((n): n is number => n !== null)}
                  />
                </VStack>

                <VStack gap={1.5}>
                  <Text type="supporting" color="secondary" weight="semibold" display="block">
                    TIMELINE
                  </Text>
                  <List density="compact">
                    {selected.timeline.map((t, idx) => (
                      <ListItem
                        key={idx}
                        label={t.title}
                        description={t.detail}
                        startContent={
                          <HStack gap={2} align="center">
                            <Text type="code" color="secondary" hasTabularNumbers>{t.time}</Text>
                            <StatusDot tone={t.kind === 'fail' ? 'danger' : t.kind === 'ok' ? 'good' : 'info'} />
                          </HStack>
                        }
                      />
                    ))}
                  </List>
                </VStack>
              </VStack>
            ) : (
              <EmptyState
                title="No active incidents"
                description="Incidents and runtime activity will appear here."
                icon={<Icon icon={InboxIcon} size="lg" />}
              />
            )}
          </Panel>
        </StackItem>
      </HStack>
    </VStack>
  );
}
