import {
  ControlApiHttpError,
  createControlApiClient as createContractControlApiClient,
  type ControlApiReplayRequest,
  type ReplayCompletionStatus,
  type RuntimeAuditInput,
  type RuntimeDeploymentStatsInput,
  type RuntimeRunSummaryInput,
  type RuntimeSampleInput,
} from "@rohrpost/control-api-contracts";
import type { DeploymentTargetMap, RouterDeployment, RuntimeDeploymentStats } from "./phase2-types";
import { ControlApiDeploymentSource } from "./deployment-source";

export type {
  ReplayCompletionStatus,
  RuntimeAuditInput,
  RuntimeRunSummaryInput,
  RuntimeSampleInput,
} from "@rohrpost/control-api-contracts";

const RUN_SUMMARY_BATCH_SIZE = 250;
const RUN_SUMMARY_FLUSH_INTERVAL_MS = 250;
const RUN_SUMMARY_MAX_PENDING = 10_000;
const RUNTIME_SAMPLE_BATCH_SIZE = 50;
const RUNTIME_SAMPLE_FLUSH_INTERVAL_MS = 1_000;
const RUNTIME_SAMPLE_MAX_PENDING = 250;

export type PendingReplayRequest = ControlApiReplayRequest;

export interface RouterControlApiClient {
  deploymentSource: ControlApiDeploymentSource;
  reportRuntimeStats(stats: RuntimeDeploymentStats[]): Promise<void>;
  appendRunSummary(input: RuntimeRunSummaryInput): Promise<void>;
  flushRunSummaries(): Promise<void>;
  appendRuntimeSample(input: RuntimeSampleInput): Promise<void>;
  flushRuntimeSamples(): Promise<void>;
  appendAudit(input: RuntimeAuditInput): Promise<void>;
  listPendingReplayRequests(limit?: number): Promise<PendingReplayRequest[]>;
  claimReplayRequest(replayId: string): Promise<PendingReplayRequest | null>;
  completeReplayRequest(replayId: string, status: ReplayCompletionStatus): Promise<void>;
}

export function createRouterControlApiClient(options: {
  controlApiUrl: string;
  controlApiToken?: string;
  sinkTargets?: DeploymentTargetMap;
  fetchImpl?: typeof fetch;
}): RouterControlApiClient {
  const fetchImpl = options.fetchImpl ?? fetch;
  const contractClient = createContractControlApiClient({
    baseUrl: options.controlApiUrl,
    token: options.controlApiToken,
    fetchImpl,
  });
  const deploymentSource = new ControlApiDeploymentSource({
    controlApiUrl: options.controlApiUrl,
    controlApiToken: options.controlApiToken,
    sinkTargets: options.sinkTargets,
    fetchImpl,
  });
  let pendingRunSummaries: RuntimeRunSummaryInput[] = [];
  let flushTimer: ReturnType<typeof setTimeout> | undefined;
  let flushInFlight: Promise<void> | undefined;
  let droppedRunSummaries = 0;
  let pendingRuntimeSamples = new Map<string, RuntimeSampleInput>();
  let runtimeSampleFlushTimer: ReturnType<typeof setTimeout> | undefined;
  let runtimeSampleFlushInFlight: Promise<void> | undefined;

  const runtimeSampleKey = (input: RuntimeSampleInput) =>
    `${input.deploymentId}:${input.sourceKind}:${input.sourceRef}`;

  const warnDroppedRunSummaries = () => {
    if (droppedRunSummaries === 0) {
      return;
    }

    if (droppedRunSummaries === 1 || droppedRunSummaries % 1_000 === 0) {
      console.warn(
        `[router-workers] dropped ${droppedRunSummaries} pending run summaries while control-api was unavailable`,
      );
    }
  };

  const trimPendingRunSummaries = () => {
    if (pendingRunSummaries.length <= RUN_SUMMARY_MAX_PENDING) {
      return;
    }

    const overflow = pendingRunSummaries.length - RUN_SUMMARY_MAX_PENDING;
    pendingRunSummaries.splice(0, overflow);
    droppedRunSummaries += overflow;
    warnDroppedRunSummaries();
  };

  const scheduleRunSummaryFlush = () => {
    if (flushTimer || pendingRunSummaries.length === 0) {
      return;
    }

    flushTimer = setTimeout(() => {
      flushTimer = undefined;
      void flushPendingRunSummaries().catch(() => undefined);
    }, RUN_SUMMARY_FLUSH_INTERVAL_MS);
  };

  const flushPendingRunSummaries = async (): Promise<void> => {
    if (flushTimer) {
      clearTimeout(flushTimer);
      flushTimer = undefined;
    }

    if (flushInFlight) {
      await flushInFlight;
    }

    if (pendingRunSummaries.length === 0) {
      return;
    }

    const batch = pendingRunSummaries.splice(0, RUN_SUMMARY_BATCH_SIZE);
    flushInFlight = (async () => {
      try {
        await contractClient.appendRunSummaries(batch);
      } catch (error) {
        pendingRunSummaries = [...batch, ...pendingRunSummaries];
        scheduleRunSummaryFlush();
        throw error;
      }
    })().finally(() => {
      flushInFlight = undefined;
    });

    await flushInFlight;

    if (pendingRunSummaries.length > 0) {
      if (pendingRunSummaries.length >= RUN_SUMMARY_BATCH_SIZE) {
        await flushPendingRunSummaries();
      } else {
        scheduleRunSummaryFlush();
      }
    }
  };

  const trimPendingRuntimeSamples = () => {
    while (pendingRuntimeSamples.size > RUNTIME_SAMPLE_MAX_PENDING) {
      const oldestKey = pendingRuntimeSamples.keys().next().value;
      if (!oldestKey) {
        break;
      }
      pendingRuntimeSamples.delete(oldestKey);
    }
  };

  const scheduleRuntimeSampleFlush = () => {
    if (runtimeSampleFlushTimer || pendingRuntimeSamples.size === 0) {
      return;
    }

    runtimeSampleFlushTimer = setTimeout(() => {
      runtimeSampleFlushTimer = undefined;
      void flushPendingRuntimeSamples().catch(() => undefined);
    }, RUNTIME_SAMPLE_FLUSH_INTERVAL_MS);
  };

  const flushPendingRuntimeSamples = async (): Promise<void> => {
    if (runtimeSampleFlushTimer) {
      clearTimeout(runtimeSampleFlushTimer);
      runtimeSampleFlushTimer = undefined;
    }

    if (runtimeSampleFlushInFlight) {
      await runtimeSampleFlushInFlight;
    }

    if (pendingRuntimeSamples.size === 0) {
      return;
    }

    const entries = [...pendingRuntimeSamples.entries()];
    const batchEntries = entries.slice(0, RUNTIME_SAMPLE_BATCH_SIZE);
    const batchKeys = batchEntries.map(([key]) => key);
    const batch = batchEntries.map(([, input]) => input);
    for (const key of batchKeys) {
      pendingRuntimeSamples.delete(key);
    }

    runtimeSampleFlushInFlight = (async () => {
      try {
        await contractClient.replaceRuntimeSamples(batch);
      } catch (error) {
        pendingRuntimeSamples = new Map([
          ...batchEntries,
          ...pendingRuntimeSamples.entries(),
        ]);
        trimPendingRuntimeSamples();
        scheduleRuntimeSampleFlush();
        throw error;
      }
    })().finally(() => {
      runtimeSampleFlushInFlight = undefined;
    });

    await runtimeSampleFlushInFlight;

    if (pendingRuntimeSamples.size > 0) {
      if (pendingRuntimeSamples.size >= RUNTIME_SAMPLE_BATCH_SIZE) {
        await flushPendingRuntimeSamples();
      } else {
        scheduleRuntimeSampleFlush();
      }
    }
  };

  return {
    deploymentSource,
    async reportRuntimeStats(stats) {
      const payload: RuntimeDeploymentStatsInput[] = stats.map((entry) => ({
        deploymentId: entry.deploymentId,
        reporterId: entry.reporterId,
        flowId: entry.flowId,
        revisionId: entry.revisionId,
        acceptedCount: entry.acceptedCount,
        processedCount: entry.processedCount,
        deliveredCount: entry.deliveredCount,
        retryingCount: entry.retryingCount,
        dlqCount: entry.dlqCount,
        failedCount: entry.failedCount,
        filteredCount: entry.filteredCount,
        dedupedCount: entry.dedupedCount,
        sinkAttemptCount: entry.sinkAttemptCount,
        sinkSuccessCount: entry.sinkSuccessCount,
        sinkFailureCount: entry.sinkFailureCount,
        inflightCount: entry.inflightCount,
        backlogCount: entry.backlogCount,
        lastAcceptedAt: entry.lastAcceptedAt ?? null,
        lastProcessedAt: entry.lastProcessedAt ?? null,
        lastError: entry.lastError ?? null,
        updatedAt: entry.updatedAt,
      }));
      await contractClient.replaceRuntimeDeploymentStats(payload);
    },
    async appendRunSummary(input) {
      pendingRunSummaries.push(input);
      trimPendingRunSummaries();
      if (pendingRunSummaries.length >= RUN_SUMMARY_BATCH_SIZE) {
        await flushPendingRunSummaries();
        return;
      }

      scheduleRunSummaryFlush();
    },
    async flushRunSummaries() {
      await flushPendingRunSummaries();
    },
    async appendRuntimeSample(input) {
      const key = runtimeSampleKey(input);
      pendingRuntimeSamples.delete(key);
      pendingRuntimeSamples.set(key, input);
      trimPendingRuntimeSamples();
      if (pendingRuntimeSamples.size >= RUNTIME_SAMPLE_BATCH_SIZE) {
        await flushPendingRuntimeSamples();
        return;
      }

      scheduleRuntimeSampleFlush();
    },
    async flushRuntimeSamples() {
      await flushPendingRuntimeSamples();
    },
    async appendAudit(input) {
      await contractClient.appendAudit(input);
    },
    async listPendingReplayRequests(limit = 25) {
      return contractClient.listPendingReplayRequests(limit);
    },
    async claimReplayRequest(replayId) {
      try {
        return await contractClient.claimReplayRequest(replayId);
      } catch (error) {
        if (error instanceof ControlApiHttpError && error.status === 404) {
          return null;
        }
        return null;
      }
    },
    async completeReplayRequest(replayId, status) {
      await contractClient.completeReplayRequest(replayId, status);
    },
  };
}

export class NoopRouterControlApiClient implements RouterControlApiClient {
  public readonly deploymentSource: ControlApiDeploymentSource;

  public constructor(controlApiUrl: string, sinkTargets?: DeploymentTargetMap) {
    this.deploymentSource = new ControlApiDeploymentSource({
      controlApiUrl,
      sinkTargets,
    });
  }

  public async reportRuntimeStats(): Promise<void> {}
  public async appendRunSummary(): Promise<void> {}
  public async flushRunSummaries(): Promise<void> {}
  public async appendRuntimeSample(): Promise<void> {}
  public async flushRuntimeSamples(): Promise<void> {}
  public async appendAudit(): Promise<void> {}
  public async listPendingReplayRequests(): Promise<PendingReplayRequest[]> {
    return [];
  }
  public async claimReplayRequest(): Promise<PendingReplayRequest | null> {
    return null;
  }
  public async completeReplayRequest(): Promise<void> {}
}
