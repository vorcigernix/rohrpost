import {
  controlApiPaths,
  type ControlApiAdapterWorkloadsResponse,
  type ControlApiConsoleEventMessage,
  type ControlApiRunRecord,
  type ControlApiRuntimeSamplesResponse,
  type ControlApiRuntimeStatsResponse,
} from '@rohrpost/control-api-contracts';
import type {
  AdapterWorkloadRecord,
  RunRecord,
  RuntimeData,
  RuntimeDeploymentRecord,
  RuntimeSampleRecord,
} from '../../lib/api-types';
import { buildEventStreamUrl, requestJson } from '../../lib/api-base';
import { isOidcAuthEnabled } from '../../lib/auth-state';

type LiveRunRecord = ControlApiRunRecord;

function mapRuntimeDeployment(
  record: ControlApiRuntimeStatsResponse['deployments'][number],
): RuntimeDeploymentRecord {
  const rolloutStatus =
    record.rolloutStatus === 'activated' || record.rolloutStatus === 'pending_activation' || record.rolloutStatus === 'degraded'
      ? record.rolloutStatus
      : 'degraded';
  const state =
    record.state === 'healthy' || record.state === 'backlogged' || record.state === 'degraded' || record.state === 'idle'
      ? record.state
      : 'degraded';

  return {
    deploymentId: record.deploymentId,
    flowId: record.flowId,
    flowName: record.flowName,
    revisionId: record.revisionId,
    rolloutStatus,
    state,
    acceptedCount: record.acceptedCount,
    processedCount: record.processedCount,
    deliveredCount: record.deliveredCount,
    backlogCount: record.backlogCount,
    inflightCount: record.inflightCount,
    retryingCount: record.retryingCount,
    dlqCount: record.dlqCount,
    lastAcceptedAt: record.lastAcceptedAt,
    lastProcessedAt: record.lastProcessedAt,
    updatedAt: record.updatedAt,
    lastError: record.lastError,
  };
}

function mapRuntimeStats(data: ControlApiRuntimeStatsResponse): RuntimeData {
  return {
    observability: data.observability,
    summary: data.summary,
    deployments: data.deployments.map(mapRuntimeDeployment),
  };
}

function mapRun(record: LiveRunRecord): RunRecord {
  return {
    ...record,
    status:
      record.status === 'succeeded' || record.status === 'failed' || record.status === 'retrying' || record.status === 'dlq'
        ? record.status
        : 'failed',
  };
}

export async function fetchRuntimeStats(): Promise<RuntimeData> {
  return mapRuntimeStats(await requestJson<ControlApiRuntimeStatsResponse>(controlApiPaths.runtimeStats()));
}

export async function fetchAdapterWorkloads(): Promise<AdapterWorkloadRecord[]> {
  const payload = await requestJson<ControlApiAdapterWorkloadsResponse>(
    controlApiPaths.adapterWorkloads(),
  );
  return payload.workloads;
}

export async function fetchRuntimeSamples(input?: {
  sourceKind?: 'http' | 'nats' | 'kafka';
  limit?: number;
}): Promise<RuntimeSampleRecord[]> {
  return (await requestJson<ControlApiRuntimeSamplesResponse>(controlApiPaths.recentRuntimeSamples(input))).samples;
}

export async function fetchRuns(): Promise<RunRecord[]> {
  return (await requestJson<LiveRunRecord[]>(controlApiPaths.runs())).map(mapRun);
}

export type ConsoleEventMessage = ControlApiConsoleEventMessage;

export function subscribeToConsoleEvents(
  onEvent: (event: ConsoleEventMessage) => void,
  onConnectionChange?: (state: 'connected' | 'reconnecting' | 'disabled') => void,
) {
  if (typeof window === 'undefined' || typeof EventSource === 'undefined') {
    onConnectionChange?.('disabled');
    return () => undefined;
  }

  const eventSource = new EventSource(buildEventStreamUrl(controlApiPaths.eventsStream()), {
    withCredentials: isOidcAuthEnabled(),
  });
  eventSource.onopen = () => onConnectionChange?.('connected');
  eventSource.onerror = () => onConnectionChange?.('reconnecting');
  eventSource.onmessage = (event) => {
    try {
      onEvent(JSON.parse(event.data) as ConsoleEventMessage);
    } catch {
      // ignore malformed frames
    }
  };
  eventSource.addEventListener('ready', (event) => {
    onConnectionChange?.('connected');
    const messageEvent = event as MessageEvent<string>;
    try {
      onEvent(JSON.parse(messageEvent.data) as ConsoleEventMessage);
    } catch {
      // ignore malformed frames
    }
  });
  eventSource.addEventListener('ping', () => {
    onConnectionChange?.('connected');
  });

  return () => {
    eventSource.close();
  };
}
