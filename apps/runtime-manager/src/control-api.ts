import {
  createControlApiClient as createContractControlApiClient,
  type ControlApiAdapterWorkloadRecord as ContractAdapterWorkloadRecord,
  type ControlApiDeploymentRecord as ContractDeploymentRecord,
  type ControlApiFlowRecord as ContractFlowRecord,
  type ControlApiOverview as ContractOverview,
  type ControlApiRuntimeDeploymentRecord as ContractRuntimeDeploymentRecord,
} from "@rohrpost/control-api-contracts";

interface RuntimeManagerFlowSpec {
  metadata?: {
    tenantId?: string;
  };
  sources?: Array<{
    id: string;
    connector?: {
      connectorId?: string;
      executionMode?: "native" | "adapter";
    };
  }>;
  sinks: Array<{
    id: string;
    connector?: {
      connectorId?: string;
      executionMode?: "native" | "adapter";
    };
  }>;
}

export interface ControlApiFlowRecord extends Pick<
  ContractFlowRecord,
  "id" | "tenantId" | "name" | "status" | "activeRevisionId" | "latestRevisionId" | "revisionId" | "updatedAt"
> {
  spec?: {
    metadata?: {
      tenantId?: string;
      flowId?: string;
      revisionId?: string;
      name?: string;
    };
    sources?: Array<{
      id: string;
      connector?: {
        connectorId?: string;
        executionMode?: "native" | "adapter";
      };
    }>;
    sinks?: Array<{
      id: string;
      connector?: {
        connectorId?: string;
        executionMode?: "native" | "adapter";
      };
    }>;
  };
}

export interface ControlApiOverview extends ContractOverview {}

export interface ControlApiDeploymentRecord extends ContractDeploymentRecord {}

export interface ControlApiRuntimeDeploymentRecord {
  deployment: ControlApiDeploymentRecord;
  revision: {
    id: string;
    flowId: string;
    spec: RuntimeManagerFlowSpec;
  };
  connectors: Record<
    string,
    {
      id: string;
      capabilityId: string;
      executionMode: "native" | "adapter";
      config: Record<string, unknown>;
    }
  >;
}

export interface ControlApiAdapterWorkloadRecord extends Pick<
  ContractAdapterWorkloadRecord,
  | "reporterId"
  | "reportedAt"
  | "key"
  | "connectorId"
  | "capabilityId"
  | "manifestId"
  | "deploymentIds"
  | "flowIds"
  | "revisionIds"
  | "runtimeRole"
  | "inputRef"
  | "outputRef"
  | "status"
  | "backend"
  | "consumerRef"
  | "targetKind"
  | "artifactPath"
  | "configPath"
  | "containerName"
  | "startedAt"
  | "stoppedAt"
  | "lastError"
  | "restartCount"
  | "recentLogs"
> {}

export interface ControlApiClient {
  fetchOverview(): Promise<ControlApiOverview>;
  fetchFlows(): Promise<ControlApiFlowRecord[]>;
  fetchActiveDeployments(): Promise<ControlApiRuntimeDeploymentRecord[]>;
  fetchAdapterWorkloads(): Promise<ControlApiAdapterWorkloadRecord[]>;
  updateDeploymentStatus(
    deploymentId: string,
    input: { status?: string; rolloutStatus: string },
  ): Promise<ControlApiDeploymentRecord>;
}

function narrowFlowRecord(record: ContractFlowRecord): ControlApiFlowRecord {
  return {
    id: record.id,
    tenantId: record.tenantId,
    name: record.name,
    status: record.status,
    activeRevisionId: record.activeRevisionId,
    latestRevisionId: record.latestRevisionId,
    revisionId: record.revisionId,
    updatedAt: record.updatedAt,
    spec: record.spec
      ? {
          metadata: record.spec.metadata,
          sources: record.spec.sources,
          sinks: record.spec.sinks,
        }
      : undefined,
  };
}

function narrowRuntimeDeploymentRecord(
  record: ContractRuntimeDeploymentRecord,
): ControlApiRuntimeDeploymentRecord {
  return {
    deployment: record.deployment,
    revision: {
      id: record.revision.id,
      flowId: record.revision.flowId,
      spec: {
        metadata: record.revision.spec.metadata,
        sources: record.revision.spec.sources,
        sinks: record.revision.spec.sinks,
      },
    },
    connectors: Object.fromEntries(
      Object.entries(record.connectors).map(([key, connector]) => [
        key,
        {
          id: connector.id,
          capabilityId: connector.capabilityId,
          executionMode: connector.executionMode,
          config: connector.config,
        },
      ]),
    ),
  };
}

export function createControlApiClient(
  controlApiUrl: string,
  controlApiToken: string,
  options: {
    fetchImpl?: typeof fetch;
    timeoutMs?: number;
  } = {},
): ControlApiClient {
  const client = createContractControlApiClient({
    baseUrl: controlApiUrl,
    token: controlApiToken,
    fetchImpl: options.fetchImpl,
    timeoutMs: options.timeoutMs,
  });

  return {
    fetchOverview: () => client.fetchOverview(),
    fetchFlows: async () => (await client.fetchFlows()).map(narrowFlowRecord),
    fetchActiveDeployments: async () => (await client.fetchActiveDeployments()).map(narrowRuntimeDeploymentRecord),
    fetchAdapterWorkloads: () => client.fetchAdapterWorkloads(),
    updateDeploymentStatus: (deploymentId, input) => client.updateDeploymentStatus(deploymentId, input),
  };
}
