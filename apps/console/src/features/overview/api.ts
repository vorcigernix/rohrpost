import {
  controlApiPaths,
  type ControlApiOverview,
} from '@rohrpost/control-api-contracts';
import type { OverviewData } from '../../lib/api-types';
import { requestJson } from '../../lib/api-base';

function mapControlApiOverview(data: ControlApiOverview): OverviewData {
  return {
    totals: {
      activeFlows: data.flows,
      healthyPipelines: data.runtime?.healthyDeployments ?? data.activeDeployments,
      replayQueue: data.pendingReplays,
      backlogMessages: data.runtime?.backlogCount ?? 0,
      deliveredMessages: data.runtime?.deliveredCount ?? data.runs,
      adapterConnectors: data.capabilities,
    },
    guardrails: [
      {
        label: data.guarantees.mode,
        detail: 'Duplicate delivery is part of the platform contract.',
        tone: 'info',
      },
      {
        label: data.guarantees.ordering,
        detail: 'Ordering is only preserved within a partition key.',
        tone: 'good',
      },
      {
        label: data.guarantees.duplicatesPossible ? 'Replay enabled' : 'Replay restricted',
        detail: 'Replay and rollback are modeled in the control plane metadata.',
        tone: 'warn',
      },
    ],
    activity: [
      {
        time: new Date().toISOString(),
        title: 'Control plane snapshot',
        detail: `${data.flows} flows, ${data.activeDeployments} active deployments, ${data.runtime?.backlogCount ?? 0} messages currently backlogged.`,
        tone: 'info',
      },
    ],
  };
}

export async function fetchOverview(): Promise<OverviewData> {
  return mapControlApiOverview(await requestJson<ControlApiOverview>(controlApiPaths.overview()));
}
