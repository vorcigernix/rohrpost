import { describe, expect, test } from "bun:test";
import type { FlowSpec } from "@rohrpost/shared-flow-spec";
import { ControlApiDeploymentSource } from "../deployment-source";

function buildSpec(): FlowSpec {
  return {
    version: 1,
    metadata: {
      tenantId: "tenant_test",
      flowId: "flow_http_customers",
      revisionId: "rev_http_customers_v1",
      name: "Customers HTTP Flow",
    },
    sources: [
      {
        id: "source_primary",
        kind: "http",
        connector: {
          capabilityId: "http_in",
          connectorId: "http_in_ingest_customers",
          executionMode: "native",
        },
        stream: "ingress",
        nextNodeIds: ["route_terminal"],
      },
    ],
    processors: [],
    routes: [
      {
        id: "route_terminal",
        fromNodeId: "source_primary",
        predicate: { type: "always" },
        toSinkIds: ["sink_primary"],
      },
    ],
    sinks: [
      {
        id: "sink_primary",
        kind: "http",
        connector: {
          capabilityId: "http_out",
          connectorId: "http_out_default",
          executionMode: "native",
        },
        deliveryGuarantee: "best_effort",
        stream: "work",
      },
    ],
    retryPolicy: {
      maxAttempts: 1,
      initialBackoffMs: 250,
      maxBackoffMs: 500,
      multiplier: 2,
    },
    dlqPolicy: {
      enabled: true,
      sinkId: "sink_primary",
      reasonFormat: "json",
    },
    batchingPolicy: {
      enabled: false,
      batchSize: 1,
    },
    idempotencyStrategy: "message_id",
  };
}

describe("ControlApiDeploymentSource", () => {
  test("maps HTTP source connector paths onto runtime deployments", async () => {
    const source = new ControlApiDeploymentSource({
      controlApiUrl: "https://control-api.test",
      controlApiToken: "token",
      fetchImpl: (async (input: RequestInfo | URL) => {
        const url = String(input);
        if (url.endsWith("/api/runtime/deployments/active")) {
          return new Response(
            JSON.stringify({
              generatedAt: new Date().toISOString(),
              deployments: [
                {
                  deployment: {
                    id: "deploy-http-customers",
                    flowId: "flow_http_customers",
                    revisionId: "rev_http_customers_v1",
                    status: "active",
                    rolloutStatus: "activated",
                  },
                  revision: {
                    id: "rev_http_customers_v1",
                    revisionNumber: 1,
                    createdAt: new Date().toISOString(),
                    publishedAt: new Date().toISOString(),
                    spec: buildSpec(),
                    compiler: {},
                    simulation: {},
                  },
                  connectors: {
                    http_in_ingest_customers: {
                      id: "http_in_ingest_customers",
                      capabilityId: "http_in",
                      executionMode: "native",
                      config: {
                        path: "/ingest/customers-cleaned",
                        method: "POST",
                      },
                    },
                    http_out_default: {
                      id: "http_out_default",
                      capabilityId: "http_out",
                      executionMode: "native",
                      config: {
                        url: "https://example.test/webhook",
                      },
                    },
                  },
                },
              ],
            }),
            {
              status: 200,
              headers: {
                "content-type": "application/json",
              },
            },
          );
        }

        return new Response("not found", { status: 404 });
      }) as unknown as typeof fetch,
    });

    const deployments = await source.loadDeployments();
    expect(deployments).toHaveLength(1);
    expect(deployments[0]?.httpSourcePaths).toEqual(["/ingest/customers-cleaned"]);
  });
});
