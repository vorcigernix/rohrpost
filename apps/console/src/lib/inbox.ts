import type {
  AdapterWorkloadRecord,
  RuntimeData,
  RuntimeDeploymentRecord,
} from './api-types';

export type InboxKind = 'dlq' | 'degraded' | 'schema' | 'deploy' | 'adapter';
export type InboxSeverity = 'danger' | 'warn' | 'info';

export interface InboxStackLine {
  line: string;
  kind: 'error' | 'dim' | 'normal';
}

export interface InboxTimelineItem {
  time: string;
  kind: 'fail' | 'ok' | 'info';
  title: string;
  detail?: string;
}

export interface InboxItem {
  id: string;
  kind: InboxKind;
  severity: InboxSeverity;
  flowId?: string;
  flowName: string;
  title: string;
  summary: string;
  count: number;
  unread: boolean;
  occurredAt: string;
  stack: InboxStackLine[];
  timeline: InboxTimelineItem[];
}

export interface ActivityItem {
  time: string;
  title: string;
  detail: string;
  tone: 'good' | 'info' | 'warn' | 'danger' | 'neutral';
}

const ACTIVE_INCIDENT_WINDOW_MS = 24 * 60 * 60 * 1000;

export function formatRelative(iso: string): string {
  const d = new Date(iso);
  const diff = Date.now() - d.getTime();
  if (diff < 60_000) return `${Math.max(0, Math.floor(diff / 1000))}s ago`;
  if (diff < 3_600_000) return `${Math.floor(diff / 60_000)}m ago`;
  if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)}h ago`;
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
}

function plural(value: number, singular: string, pluralValue = `${singular}s`): string {
  return `${value.toLocaleString()} ${value === 1 ? singular : pluralValue}`;
}

function eventTime(...values: Array<string | null | undefined>): string {
  return values.find(Boolean) ?? new Date().toISOString();
}

function shortTime(iso: string): string {
  return new Date(iso).toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' });
}

function isFreshIncidentTime(iso: string): boolean {
  const time = new Date(iso).getTime();
  if (!Number.isFinite(time)) return false;
  return Date.now() - time <= ACTIVE_INCIDENT_WINDOW_MS;
}

export function isFreshRuntimeSignal(...values: Array<string | null | undefined>): boolean {
  return isFreshIncidentTime(eventTime(...values));
}

function deploymentBaseStack(deployment: RuntimeDeploymentRecord): InboxStackLine[] {
  return [
    { line: `flow: ${deployment.flowName}`, kind: 'dim' },
    { line: `deployment: ${deployment.deploymentId}`, kind: 'dim' },
    { line: `revision: ${deployment.revisionId}`, kind: 'dim' },
  ];
}

function deploymentTimeline(deployment: RuntimeDeploymentRecord, occurredAt: string, title: string): InboxTimelineItem[] {
  return [
    {
      time: shortTime(occurredAt),
      kind: deployment.state === 'healthy' ? 'ok' : deployment.state === 'idle' ? 'info' : 'fail',
      title,
      detail: `${deployment.state} · ${deployment.rolloutStatus}`,
    },
  ];
}

function deploymentItems(deployment: RuntimeDeploymentRecord): InboxItem[] {
  const occurredAt = eventTime(deployment.updatedAt, deployment.lastProcessedAt, deployment.lastAcceptedAt);
  if (!isFreshRuntimeSignal(deployment.updatedAt, deployment.lastProcessedAt, deployment.lastAcceptedAt)) {
    return [];
  }

  const items: InboxItem[] = [];

  if (deployment.dlqCount > 0) {
    items.push({
      id: `dlq:${deployment.deploymentId}`,
      kind: 'dlq',
      severity: 'danger',
      flowId: deployment.flowId,
      flowName: deployment.flowName,
      title: `${plural(deployment.dlqCount, 'message')} in DLQ`,
      summary: deployment.lastError
        ? `${deployment.flowName} is sending messages to DLQ. Latest runtime error: ${deployment.lastError}`
        : `${deployment.flowName} has messages in DLQ for the active deployment.`,
      count: deployment.dlqCount,
      unread: true,
      occurredAt,
      stack: [
        ...deploymentBaseStack(deployment),
        { line: `dlqCount: ${deployment.dlqCount.toLocaleString()}`, kind: 'error' },
        ...(deployment.lastError ? [{ line: deployment.lastError, kind: 'error' as const }] : []),
      ],
      timeline: deploymentTimeline(deployment, occurredAt, 'DLQ count reported'),
    });
  }

  if (deployment.state === 'degraded' || deployment.lastError) {
    items.push({
      id: `degraded:${deployment.deploymentId}`,
      kind: 'degraded',
      severity: deployment.lastError ? 'danger' : 'warn',
      flowId: deployment.flowId,
      flowName: deployment.flowName,
      title: `${deployment.flowName} is degraded`,
      summary: deployment.lastError
        ? `The active runtime reports: ${deployment.lastError}`
        : 'The active deployment is reporting degraded runtime health.',
      count: Math.max(deployment.retryingCount, deployment.backlogCount, 1),
      unread: true,
      occurredAt,
      stack: [
        ...deploymentBaseStack(deployment),
        { line: `state: ${deployment.state}`, kind: 'error' },
        ...(deployment.lastError ? [{ line: deployment.lastError, kind: 'error' as const }] : []),
      ],
      timeline: deploymentTimeline(deployment, occurredAt, 'Runtime health changed'),
    });
  } else if (deployment.state === 'backlogged' || deployment.backlogCount > 0) {
    items.push({
      id: `backlog:${deployment.deploymentId}`,
      kind: 'degraded',
      severity: 'warn',
      flowId: deployment.flowId,
      flowName: deployment.flowName,
      title: `Backlog growing — ${plural(deployment.backlogCount, 'message')} pending`,
      summary: `${deployment.flowName} has pending messages in the active runtime deployment.`,
      count: deployment.backlogCount,
      unread: true,
      occurredAt,
      stack: [
        ...deploymentBaseStack(deployment),
        { line: `backlogCount: ${deployment.backlogCount.toLocaleString()}`, kind: 'error' },
        { line: `inflightCount: ${deployment.inflightCount.toLocaleString()}`, kind: 'dim' },
      ],
      timeline: deploymentTimeline(deployment, occurredAt, 'Backlog reported'),
    });
  }

  if (deployment.retryingCount > 0) {
    items.push({
      id: `retrying:${deployment.deploymentId}`,
      kind: 'degraded',
      severity: 'warn',
      flowId: deployment.flowId,
      flowName: deployment.flowName,
      title: `${plural(deployment.retryingCount, 'message')} retrying`,
      summary: `${deployment.flowName} has messages in retry for the active deployment.`,
      count: deployment.retryingCount,
      unread: true,
      occurredAt,
      stack: [
        ...deploymentBaseStack(deployment),
        { line: `retryingCount: ${deployment.retryingCount.toLocaleString()}`, kind: 'error' },
      ],
      timeline: deploymentTimeline(deployment, occurredAt, 'Retry count reported'),
    });
  }

  if (deployment.rolloutStatus === 'pending_activation') {
    items.push({
      id: `deploy:${deployment.deploymentId}`,
      kind: 'deploy',
      severity: 'info',
      flowId: deployment.flowId,
      flowName: deployment.flowName,
      title: `${deployment.revisionId} pending activation`,
      summary: `${deployment.flowName} has a deployment waiting for runtime activation.`,
      count: 1,
      unread: false,
      occurredAt,
      stack: [
        ...deploymentBaseStack(deployment),
        { line: `rolloutStatus: ${deployment.rolloutStatus}`, kind: 'normal' },
      ],
      timeline: deploymentTimeline(deployment, occurredAt, 'Deployment pending activation'),
    });
  }

  return items;
}

function adapterItem(workload: AdapterWorkloadRecord): InboxItem | null {
  if (workload.status === 'running' && workload.restartCount === 0 && !workload.lastError) {
    return null;
  }

  const occurredAt = eventTime(workload.reportedAt);
  if (!isFreshRuntimeSignal(workload.reportedAt)) {
    return null;
  }

  const stopped = workload.status === 'stopped';
  const degraded = workload.status === 'degraded' || stopped || Boolean(workload.lastError);
  const flowId = workload.flowIds[0];
  const title = degraded
    ? `${workload.connectorId} adapter ${workload.status}`
    : `${workload.connectorId} adapter restarted`;

  return {
    id: `adapter:${workload.key}`,
    kind: 'adapter',
    severity: stopped || workload.lastError ? 'danger' : 'warn',
    flowId,
    flowName: flowId ?? workload.connectorId,
    title,
    summary: workload.lastError
      ? `Connector ${workload.connectorId} reports: ${workload.lastError}`
      : `Connector ${workload.connectorId} reported ${workload.restartCount.toLocaleString()} restart${workload.restartCount === 1 ? '' : 's'}.`,
    count: Math.max(workload.restartCount, 1),
    unread: true,
    occurredAt,
    stack: [
      { line: `connector: ${workload.connectorId}`, kind: 'dim' },
      { line: `capability: ${workload.capabilityId}`, kind: 'dim' },
      { line: `role: ${workload.runtimeRole}`, kind: 'dim' },
      { line: `status: ${workload.status}`, kind: degraded ? 'error' : 'normal' },
      ...(workload.lastError ? [{ line: workload.lastError, kind: 'error' as const }] : []),
      ...workload.recentLogs.slice(-3).map((line) => ({ line, kind: 'dim' as const })),
    ],
    timeline: [
      {
        time: shortTime(occurredAt),
        kind: degraded ? 'fail' : 'info',
        title: 'Adapter status reported',
        detail: `${workload.backend} · ${workload.manifestId}`,
      },
    ],
  };
}

export function deriveInboxItems(input: {
  runtime?: RuntimeData | null;
  adapterWorkloads?: AdapterWorkloadRecord[] | null;
}): InboxItem[] {
  const deployments = input.runtime?.deployments ?? [];
  const adapterWorkloads = input.adapterWorkloads ?? [];

  return [
    ...deployments.flatMap(deploymentItems),
    ...adapterWorkloads.map(adapterItem).filter((item): item is InboxItem => Boolean(item)),
  ].sort((a, b) => new Date(b.occurredAt).getTime() - new Date(a.occurredAt).getTime());
}

export function deriveActivityItems(runtime?: RuntimeData | null): ActivityItem[] {
  return (runtime?.deployments ?? [])
    .slice()
    .filter((deployment) => isFreshRuntimeSignal(deployment.updatedAt, deployment.lastProcessedAt, deployment.lastAcceptedAt))
    .sort((a, b) => new Date(eventTime(b.updatedAt)).getTime() - new Date(eventTime(a.updatedAt)).getTime())
    .slice(0, 4)
    .map((deployment) => ({
      time: eventTime(deployment.updatedAt, deployment.lastProcessedAt, deployment.lastAcceptedAt),
      title: `${deployment.flowName} ${deployment.state}`,
      detail: `${deployment.revisionId} · ${deployment.rolloutStatus}`,
      tone:
        deployment.state === 'healthy'
          ? 'good'
          : deployment.state === 'degraded'
            ? 'danger'
            : deployment.state === 'backlogged'
              ? 'warn'
              : 'neutral',
    }));
}

export function inboxUnreadCount(items: InboxItem[]): number {
  return items.filter((item) => item.unread).length;
}
