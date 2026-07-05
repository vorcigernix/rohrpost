import { describe, expect, it } from "bun:test";

import { createRuntimeManagerApp } from "../src/app";
import { createControlApiClient } from "../src/control-api";
import { loadDesiredState, loadRuntimeSnapshot } from "../src/runtime-state";
import { DEFAULT_RUNTIME_TARGETS } from "../src/runtime-targets";
import type { ObservedRuntimeTargets } from "../src/target-observer";

const flows = [
  {
    id: "flow-1",
    tenantId: "tenant-a",
    name: "Orders",
    status: "active",
    activeRevisionId: "rev-1",
    latestRevisionId: "rev-1",
    updatedAt: "2026-04-01T00:00:00.000Z",
    spec: {
      sources: [
        {
          id: "source-1",
          connector: {
            executionMode: "native" as const,
          },
        },
      ],
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
  {
    id: "flow-2",
    tenantId: "tenant-a",
    name: "Kafka Mirror",
    status: "active",
    activeRevisionId: "rev-2",
    latestRevisionId: "rev-2",
    updatedAt: "2026-04-01T00:00:00.000Z",
    spec: {
      sources: [
        {
          id: "source-2",
          connector: {
            executionMode: "adapter" as const,
          },
        },
      ],
      sinks: [
        {
          id: "sink-2",
          connector: {
            executionMode: "native" as const,
          },
        },
      ],
    },
  },
];

function createFakeClient() {
  return {
    fetchOverview: async () => ({
      flows: 2,
      activeDeployments: 2,
      runs: 4,
      pendingReplays: 1,
      capabilities: 9,
      runtime: {
        acceptedCount: 4,
        processedCount: 4,
        deliveredCount: 4,
        backlogCount: 0,
        inflightCount: 0,
        healthyDeployments: 2,
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
    fetchFlows: async () => flows,
    fetchActiveDeployments: async () => [
      {
        deployment: {
          id: "deploy-1",
          flowId: "flow-1",
          revisionId: "rev-1",
          status: "active",
          rolloutStatus: "pending_activation",
          createdAt: "2026-04-01T00:00:00.000Z",
          rolledBackFrom: null,
        },
        revision: {
          id: "rev-1",
          flowId: "flow-1",
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
        connectors: {},
      },
    ],
    fetchAdapterWorkloads: async () => [],
    updateDeploymentStatus: async (deploymentId: string, input: { status?: string; rolloutStatus: string }) => ({
      id: deploymentId,
      flowId: "flow-1",
      revisionId: "rev-1",
      status: input.status ?? "active",
      rolloutStatus: input.rolloutStatus,
      createdAt: "2026-04-01T00:00:00.000Z",
      rolledBackFrom: null,
    }),
  };
}

function observeHealthyTargets(targets: typeof DEFAULT_RUNTIME_TARGETS): Promise<ObservedRuntimeTargets> {
  return Promise.resolve({
    targets: targets.map((target) => ({
      ...target,
      healthy: true,
      observedReplicas: 1,
      lastObservedAt: "2026-04-01T00:00:00.000Z",
      sourceUrl: `http://${target.id}`,
    })),
    probes: targets.map((target) => ({
      targetId: target.id,
      checkedAt: "2026-04-01T00:00:00.000Z",
      url: `http://${target.id}`,
      reachable: true,
      healthy: true,
      statusCode: 200,
      details: {},
    })),
  });
}

describe("runtime state", () => {
  it("fetches desired data from control-api with bearer auth", async () => {
    const originalFetch = globalThis.fetch;
    const calls: Array<{ url: string; authorization: string | null }> = [];

    globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = new Request(input, init);
      calls.push({
        url: request.url,
        authorization: request.headers.get("authorization"),
      });

      return new Response(
          JSON.stringify({
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
        {
          headers: { "content-type": "application/json" },
        },
      );
    }) as typeof fetch;

    try {
      const client = createControlApiClient("http://control-api", "test-token");
      await client.fetchOverview();

      expect(calls[0]?.url).toBe("http://control-api/api/overview");
      expect(calls[0]?.authorization).toBe("Bearer test-token");
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  it("derives desired activations from control-api flows", async () => {
    const desired = await loadDesiredState(createFakeClient(), "tenant-a");

    expect(desired.activations).toHaveLength(2);
    expect(desired.activations[0]?.targetId).toBe("router-workers");
    expect(desired.activations[1]?.targetId).toBe("adapter-redpanda");
  });

  it("builds a runtime snapshot and reconciliation summary", async () => {
    const snapshot = await loadRuntimeSnapshot(
      createFakeClient(),
      "tenant-a",
      DEFAULT_RUNTIME_TARGETS,
    );

    expect(snapshot.plan.length).toBeGreaterThan(0);
    expect(snapshot.summary.total).toBe(snapshot.plan.length);
    expect(snapshot.controlApi.flows).toBe(2);
  });

  it("serves runtime-manager endpoints from the injected client", async () => {
    const app = createRuntimeManagerApp({
      controlApiClient: createFakeClient(),
      runtimeTargets: DEFAULT_RUNTIME_TARGETS,
      config: {
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
      },
      observeTargets: observeHealthyTargets,
    });

    const response = await app.handle(new Request("http://localhost/snapshots"));
    expect(response.status).toBe(200);

    const body = (await response.json()) as {
      tenantId: string;
      summary: { total: number };
      readiness: { ready: boolean };
    };
    expect(body.tenantId).toBe("tenant-a");
    expect(body.summary.total).toBeGreaterThan(0);
    expect(body.readiness.ready).toBe(true);
  });
});
