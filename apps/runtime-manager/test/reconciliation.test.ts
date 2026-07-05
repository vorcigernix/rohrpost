import { describe, expect, it } from "bun:test";

import { createRuntimeManagerApp } from "../src/app";
import { deriveAdapterAwareRolloutStatus } from "../src/runtime-service";
import { deriveRolloutStatus, buildReconciliationPlan, summarizeReconciliationPlan } from "../src/runtime-targets";
import type { ObservedRuntimeTargets } from "../src/target-observer";
import type { RuntimeTarget } from "../src/runtime-targets";

const config = {
  host: "127.0.0.1",
  port: 7102,
  controlApiUrl: "http://control-api",
  controlApiToken: "token",
  requestTimeoutMs: 2_500,
  routerWorkersUrl: "http://router-workers",
  adapterRedpandaUrl: "http://adapter-redpanda",
  tenantId: "tenant-a",
  serviceName: "runtime-manager",
  snapshotRefreshMs: 1_000,
};

function createFakeClient(options: {
  deployment?: {
    id?: string;
    flowId?: string;
    revisionId?: string;
    rolloutStatus?: string;
    sourceExecutionMode?: "native" | "adapter";
    sourceConnectorId?: string;
    sourceCapabilityId?: string;
    sinkExecutionMode?: "native" | "adapter";
    sinkConnectorId?: string;
    sinkCapabilityId?: string;
  };
  adapterWorkloads?: Array<{
    deploymentIds: string[];
    connectorId: string;
    capabilityId: string;
    status: "starting" | "running" | "stopped" | "degraded";
  }>;
} = {}) {
  const updates: Array<{ deploymentId: string; rolloutStatus: string }> = [];
  const deploymentConfig = {
    id: "deploy-1",
    flowId: "flow-1",
    revisionId: "rev-1",
    rolloutStatus: "pending_activation",
    sourceExecutionMode: "native" as const,
    sourceConnectorId: "http_in_default",
    sourceCapabilityId: "http_in",
    sinkExecutionMode: "native" as const,
    sinkConnectorId: "nats_out_default",
    sinkCapabilityId: "nats_out",
    ...options.deployment,
  };

  return {
    updates,
    client: {
      fetchOverview: async () => ({
        flows: 1,
        activeDeployments: 1,
        runs: 1,
        pendingReplays: 0,
        capabilities: 9,
        runtime: {
          acceptedCount: 1,
          processedCount: 1,
          deliveredCount: 1,
          backlogCount: 0,
          inflightCount: 0,
          healthyDeployments: 1,
          degradedDeployments: 0,
          lastProcessedAt: "2026-04-01T00:00:01.000Z",
        },
        observability: {
          mode: "otel-primary",
          consoleRole: "health-and-flow-mapping",
        },
        guarantees: {
          mode: "at-least-once",
          ordering: "per partition key",
          duplicatesPossible: true,
        },
      }),
      fetchFlows: async () => [
        {
          id: "flow-1",
          tenantId: "tenant-a",
          name: "Orders",
          status: "active",
          activeRevisionId: "rev-1",
          latestRevisionId: "rev-1",
          updatedAt: "2026-04-01T00:00:00.000Z",
          spec: {
            metadata: {
              tenantId: "tenant-a",
            },
            sinks: [
              {
                id: "sink-1",
                connector: {
                  executionMode: "native" as const,
                },
              },
            ],
          },
        },
      ],
      fetchActiveDeployments: async () => [
        {
          deployment: {
            id: deploymentConfig.id,
            flowId: deploymentConfig.flowId,
            revisionId: deploymentConfig.revisionId,
            status: "active",
            rolloutStatus: deploymentConfig.rolloutStatus,
            createdAt: "2026-04-01T00:00:00.000Z",
            rolledBackFrom: null,
          },
          revision: {
            id: deploymentConfig.revisionId,
            flowId: deploymentConfig.flowId,
            spec: {
              metadata: {
                tenantId: "tenant-a",
              },
              sources: [
                {
                  id: "source-1",
                  connector: {
                    connectorId: deploymentConfig.sourceConnectorId,
                    executionMode: deploymentConfig.sourceExecutionMode,
                  },
                },
              ],
              sinks: [
                {
                  id: "sink-1",
                  connector: {
                    connectorId: deploymentConfig.sinkConnectorId,
                    executionMode: deploymentConfig.sinkExecutionMode,
                  },
                },
              ],
            },
          },
          connectors: {
            [deploymentConfig.sourceConnectorId]: {
              id: deploymentConfig.sourceConnectorId,
              capabilityId: deploymentConfig.sourceCapabilityId,
              executionMode: deploymentConfig.sourceExecutionMode,
              config: {},
            },
            [deploymentConfig.sinkConnectorId]: {
              id: deploymentConfig.sinkConnectorId,
              capabilityId: deploymentConfig.sinkCapabilityId,
              executionMode: deploymentConfig.sinkExecutionMode,
              config: {},
            },
          },
        },
      ],
      fetchAdapterWorkloads: async () =>
        (options.adapterWorkloads ?? []).map((workload, index) => ({
          reporterId: "adapter-redpanda",
          reportedAt: "2026-04-01T00:00:00.000Z",
          key: `${workload.connectorId}:${index}`,
          connectorId: workload.connectorId,
          capabilityId: workload.capabilityId,
          manifestId: workload.capabilityId,
          deploymentIds: workload.deploymentIds,
          flowIds: [deploymentConfig.flowId],
          revisionIds: [deploymentConfig.revisionId],
          runtimeRole: "sink" as const,
          inputRef: "router.work.connect.test.>",
          outputRef: "file:///tmp/out.ndjson",
          status: workload.status,
          backend: "kubernetes" as const,
          consumerRef: "rpc_test",
          targetKind: "file" as const,
          artifactPath: "/tmp/out.ndjson",
          configPath: "/etc/redpanda-connect/connect.json",
          containerName: "redpanda-connect-test",
          startedAt: "2026-04-01T00:00:00.000Z",
          stoppedAt: null,
          lastError: workload.status === "degraded" ? "connect workload failed" : null,
          restartCount: 0,
          recentLogs: [],
        })),
      updateDeploymentStatus: async (
        deploymentId: string,
        input: { status?: string; rolloutStatus: string },
      ) => {
        updates.push({
          deploymentId,
          rolloutStatus: input.rolloutStatus,
        });

        return {
          id: deploymentId,
          flowId: deploymentConfig.flowId,
          revisionId: deploymentConfig.revisionId,
          status: input.status ?? "active",
          rolloutStatus: input.rolloutStatus,
          createdAt: "2026-04-01T00:00:00.000Z",
          rolledBackFrom: null,
        };
      },
    },
  };
}

function observeTargets(overrides: Partial<RuntimeTarget>): (targets: RuntimeTarget[]) => Promise<ObservedRuntimeTargets> {
  return async (targets) => ({
    targets: targets.map((target) =>
      target.id === "router-workers"
        ? {
            ...target,
            ...overrides,
            lastObservedAt: "2026-04-01T00:00:00.000Z",
            sourceUrl: "http://router-workers/status",
          }
        : {
            ...target,
            healthy: true,
            observedReplicas: 1,
            lastObservedAt: "2026-04-01T00:00:00.000Z",
            sourceUrl: `http://${target.id}`,
          },
    ),
    probes: targets.map((target) => ({
      targetId: target.id,
      checkedAt: "2026-04-01T00:00:00.000Z",
      url: `http://${target.id}`,
      reachable: true,
      healthy:
        target.id === "router-workers"
          ? overrides.healthy ?? true
          : true,
      statusCode: 200,
      details: {},
    })),
  });
}

describe("buildReconciliationPlan", () => {
  it("describes activation and drift in plain terms", () => {
    const plan = buildReconciliationPlan(
      {
        activations: [
          {
            flowId: "flow-1",
            revisionId: "rev-1",
            targetId: "router-workers",
            desiredReplicas: 2,
          },
          {
            flowId: "flow-2",
            revisionId: "rev-2",
            targetId: "adapter-redpanda",
            desiredReplicas: 1,
          },
        ],
      },
      {
        targets: [
          {
            targetId: "router-workers",
            healthy: true,
            replicas: 1,
          },
        ],
      },
    );

    expect(plan[0]?.action).toBe("scale-up");
    expect(plan[1]?.action).toBe("activate-revision");
    expect(summarizeReconciliationPlan(plan).requiresAction).toBe(true);
  });
});

describe("deriveRolloutStatus", () => {
  it("marks healthy replicas as activated", () => {
    expect(
      deriveRolloutStatus(
        {
          targetId: "router-workers",
          healthy: true,
          replicas: 1,
        },
        1,
      ),
    ).toBe("activated");
  });

  it("marks missing or undersized replicas as pending activation", () => {
    expect(deriveRolloutStatus(undefined, 1)).toBe("pending_activation");
    expect(
      deriveRolloutStatus(
        {
          targetId: "router-workers",
          healthy: true,
          replicas: 0,
        },
        1,
      ),
    ).toBe("pending_activation");
  });

  it("marks unhealthy targets as degraded", () => {
    expect(
      deriveRolloutStatus(
        {
          targetId: "router-workers",
          healthy: false,
          replicas: 1,
        },
        1,
      ),
    ).toBe("degraded");
  });
});

describe("deriveAdapterAwareRolloutStatus", () => {
  const observed = {
    targetId: "adapter-redpanda",
    healthy: true,
    replicas: 1,
  };

  it("keeps connect-managed adapter deployments pending until a linked workload reports", async () => {
    const fake = createFakeClient({
      deployment: {
        sinkExecutionMode: "adapter",
        sinkConnectorId: "s3_sink_default",
        sinkCapabilityId: "s3_sink",
      },
    });
    const [deployment] = await fake.client.fetchActiveDeployments();

    expect(
      deriveAdapterAwareRolloutStatus({
        targetId: "adapter-redpanda",
        observed,
        desiredReplicas: 1,
        deployment: deployment!,
        adapterWorkloads: [],
      }),
    ).toBe("pending_activation");
  });

  it("marks linked degraded or stopped connect workloads as degraded", async () => {
    const fake = createFakeClient({
      deployment: {
        sinkExecutionMode: "adapter",
        sinkConnectorId: "s3_sink_default",
        sinkCapabilityId: "s3_sink",
      },
      adapterWorkloads: [
        {
          deploymentIds: ["deploy-1"],
          connectorId: "s3_sink_default",
          capabilityId: "s3_sink",
          status: "degraded",
        },
      ],
    });
    const [deployment] = await fake.client.fetchActiveDeployments();
    const adapterWorkloads = await fake.client.fetchAdapterWorkloads();

    expect(
      deriveAdapterAwareRolloutStatus({
        targetId: "adapter-redpanda",
        observed,
        desiredReplicas: 1,
        deployment: deployment!,
        adapterWorkloads,
      }),
    ).toBe("degraded");
  });

  it("allows activation when every required connect workload is running", async () => {
    const fake = createFakeClient({
      deployment: {
        sourceExecutionMode: "adapter",
        sourceConnectorId: "kafka_in_orders",
        sourceCapabilityId: "kafka_in",
        sinkExecutionMode: "adapter",
        sinkConnectorId: "s3_sink_default",
        sinkCapabilityId: "s3_sink",
      },
      adapterWorkloads: [
        {
          deploymentIds: ["deploy-1"],
          connectorId: "kafka_in_orders",
          capabilityId: "kafka_in",
          status: "running",
        },
        {
          deploymentIds: ["deploy-1"],
          connectorId: "s3_sink_default",
          capabilityId: "s3_sink",
          status: "running",
        },
      ],
    });
    const [deployment] = await fake.client.fetchActiveDeployments();
    const adapterWorkloads = await fake.client.fetchAdapterWorkloads();

    expect(
      deriveAdapterAwareRolloutStatus({
        targetId: "adapter-redpanda",
        observed,
        desiredReplicas: 1,
        deployment: deployment!,
        adapterWorkloads,
      }),
    ).toBe("activated");
  });

  it("keeps activation pending when one required connect workload has not reported", async () => {
    const fake = createFakeClient({
      deployment: {
        sourceExecutionMode: "adapter",
        sourceConnectorId: "kafka_in_orders",
        sourceCapabilityId: "kafka_in",
        sinkExecutionMode: "adapter",
        sinkConnectorId: "s3_sink_default",
        sinkCapabilityId: "s3_sink",
      },
      adapterWorkloads: [
        {
          deploymentIds: ["deploy-1"],
          connectorId: "kafka_in_orders",
          capabilityId: "kafka_in",
          status: "running",
        },
      ],
    });
    const [deployment] = await fake.client.fetchActiveDeployments();
    const adapterWorkloads = await fake.client.fetchAdapterWorkloads();

    expect(
      deriveAdapterAwareRolloutStatus({
        targetId: "adapter-redpanda",
        observed,
        desiredReplicas: 1,
        deployment: deployment!,
        adapterWorkloads,
      }),
    ).toBe("pending_activation");
  });
});

describe("reconcile/run", () => {
  it("marks active deployments as activated when the runtime target is healthy", async () => {
    const fake = createFakeClient();
    const app = createRuntimeManagerApp({
      config,
      controlApiClient: fake.client,
      observeTargets: observeTargets({
        healthy: true,
        observedReplicas: 1,
      }),
    });

    const response = await app.handle(
      new Request("http://localhost/reconcile/run", {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({}),
      }),
    );

    expect(response.status).toBe(200);
    expect(fake.updates).toEqual([
      {
        deploymentId: "deploy-1",
        rolloutStatus: "activated",
      },
    ]);
  });

  it("marks unhealthy runtime targets as degraded", async () => {
    const fake = createFakeClient();
    const app = createRuntimeManagerApp({
      config,
      controlApiClient: fake.client,
      observeTargets: observeTargets({
        healthy: false,
        observedReplicas: 1,
        lastError: "router-workers disconnected from nats",
      }),
    });

    await app.handle(
      new Request("http://localhost/reconcile/run", {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({}),
      }),
    );

    expect(fake.updates).toEqual([
      {
        deploymentId: "deploy-1",
        rolloutStatus: "degraded",
      },
    ]);
  });

  it("marks adapter deployments degraded when their linked connect workload is degraded", async () => {
    const fake = createFakeClient({
      deployment: {
        sinkExecutionMode: "adapter",
        sinkConnectorId: "s3_sink_default",
        sinkCapabilityId: "s3_sink",
      },
      adapterWorkloads: [
        {
          deploymentIds: ["deploy-1", "deploy-2"],
          connectorId: "s3_sink_default",
          capabilityId: "s3_sink",
          status: "degraded",
        },
      ],
    });
    const app = createRuntimeManagerApp({
      config,
      controlApiClient: fake.client,
      observeTargets: observeTargets({
        healthy: true,
        observedReplicas: 1,
      }),
    });

    const response = await app.handle(
      new Request("http://localhost/reconcile/run", {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({}),
      }),
    );

    expect(response.status).toBe(200);
    expect(fake.updates).toEqual([
      {
        deploymentId: "deploy-1",
        rolloutStatus: "degraded",
      },
    ]);
  });
});
