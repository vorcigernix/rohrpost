import { afterAll, describe, expect, it } from "bun:test";
import { createHmac } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { FlowSpec } from "@rohrpost/shared-flow-spec";
import { buildDraftFromPrompt } from "../src/flows/drafts";
import { createApp } from "../src/app";

const fixtureDir = mkdtempSync(join(tmpdir(), "control-api-"));
const extraFixtureDirs: string[] = [];
const app = createApp({
  config: {
    host: "127.0.0.1",
    port: 0,
    databasePath: join(fixtureDir, "control-plane.db"),
    runSummaryRetentionLimit: 10,
    bootstrapAdminEmail: "admin@test.local",
    bootstrapApiToken: "test-token",
    defaultTenantId: "tenant_test",
    defaultTenantName: "Tenant Test",
  },
});

async function createPublishedFlow(
  targetApp: ReturnType<typeof createApp>,
  suffix: string,
): Promise<{ deploymentId: string; flowId: string; revisionId: string }> {
  const spec = buildDraftFromPrompt({
    prompt: `HTTP ${suffix} events to S3`,
    tenantId: "tenant_test",
    flowId: `flow_test_${suffix}`,
    revisionId: `rev_test_${suffix}_v1`,
    name: `Test ${suffix}`,
  }).draft;

  const createResponse = await targetApp.handle(
    new Request("http://localhost/api/flows", {
      method: "POST",
      headers: {
        authorization: "Bearer test-token",
        "content-type": "application/json",
      },
      body: JSON.stringify({ spec, name: spec.metadata.name }),
    }),
  );
  expect(createResponse.status).toBe(201);
  const created = (await createResponse.json()) as { id: string; flowId: string };

  const publishResponse = await targetApp.handle(
    new Request(`http://localhost/api/flows/${created.flowId}/publish`, {
      method: "POST",
      headers: {
        authorization: "Bearer test-token",
        "content-type": "application/json",
      },
      body: JSON.stringify({ revisionId: created.id }),
    }),
  );
  expect(publishResponse.status).toBe(200);
  const published = (await publishResponse.json()) as {
    deployment: { id: string; flowId: string; revisionId: string };
  };

  return {
    deploymentId: published.deployment.id,
    flowId: published.deployment.flowId,
    revisionId: published.deployment.revisionId,
  };
}

afterAll(() => {
  rmSync(fixtureDir, { recursive: true, force: true });
  for (const dir of extraFixtureDirs) {
    rmSync(dir, { recursive: true, force: true });
  }
});

describe("control-api", () => {
  it("serves health without auth", async () => {
    const response = await app.handle(
      new Request("http://localhost/health", {
        headers: {
          origin: "http://127.0.0.1:5173",
        },
      }),
    );
    expect(response.status).toBe(200);
    expect(response.headers.get("access-control-allow-origin")).toBe("http://127.0.0.1:5173");
  });

  it("answers CORS preflight for authenticated API requests", async () => {
    const response = await app.handle(
      new Request("http://localhost/api/overview", {
        method: "OPTIONS",
        headers: {
          origin: "http://127.0.0.1:5173",
          "access-control-request-method": "GET",
          "access-control-request-headers": "authorization,content-type",
        },
      }),
    );

    expect(response.status).toBe(204);
    expect(response.headers.get("access-control-allow-origin")).toBe("http://127.0.0.1:5173");
    expect(response.headers.get("access-control-allow-headers")).toContain("Authorization");
  });

  it("reports OIDC as disabled by default", async () => {
    const response = await app.handle(new Request("http://localhost/api/auth/oidc"));

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toEqual({
      enabled: false,
      loginRequired: false,
    });
  });

  it("reports configured OIDC discovery metadata", async () => {
    const oidcFixtureDir = mkdtempSync(join(tmpdir(), "control-api-oidc-"));
    extraFixtureDirs.push(oidcFixtureDir);
    const oidcApp = createApp({
      config: {
        host: "127.0.0.1",
        port: 0,
        databasePath: join(oidcFixtureDir, "control-plane.db"),
        runSummaryRetentionLimit: 10,
        bootstrapAdminEmail: "admin@test.local",
        bootstrapApiToken: "test-token",
        defaultTenantId: "tenant_test",
        defaultTenantName: "Tenant Test",
        oidc: {
          issuerUrl: "https://issuer.test",
          clientId: "console-client",
          scope: "openid email",
          sessionCookieName: "test_oidc",
          sessionSecret: "test-secret",
        },
      },
      fetchImpl: Object.assign(async () => Response.json({
        issuer: "https://issuer.test",
        authorization_endpoint: "https://issuer.test/authorize",
        token_endpoint: "https://issuer.test/token",
        jwks_uri: "https://issuer.test/jwks",
      }), { preconnect: fetch.preconnect }) as typeof fetch,
    });

    const configResponse = await oidcApp.handle(new Request("http://localhost/api/auth/oidc"));
    expect(configResponse.status).toBe(200);
    await expect(configResponse.json()).resolves.toEqual({
      enabled: true,
      loginRequired: true,
      issuerUrl: "https://issuer.test",
      clientId: "console-client",
      authorizationEndpoint: "https://issuer.test/authorize",
      scope: "openid email",
    });

    const sessionResponse = await oidcApp.handle(new Request("http://localhost/api/auth/session"));
    expect(sessionResponse.status).toBe(200);
    await expect(sessionResponse.json()).resolves.toEqual({
      enabled: true,
      authenticated: false,
      mode: "oidc",
    });

    const malformedPayload = Buffer.from("not-json").toString("base64url");
    const malformedSignature = createHmac("sha256", "test-secret").update(malformedPayload).digest("base64url");
    const malformedSessionResponse = await oidcApp.handle(
      new Request("http://localhost/api/auth/session", {
        headers: { cookie: `test_oidc=${malformedPayload}.${malformedSignature}` },
      }),
    );
    expect(malformedSessionResponse.status).toBe(200);
    await expect(malformedSessionResponse.json()).resolves.toEqual({
      enabled: true,
      authenticated: false,
      mode: "oidc",
    });
  });

  it("opens the console event stream with a token query parameter", async () => {
    const response = await app.handle(
      new Request("http://localhost/api/events/stream?access_token=test-token"),
    );

    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toContain("text/event-stream");

    const reader = response.body?.getReader();
    expect(reader).toBeDefined();
    const firstChunk = await reader?.read();
    const text = new TextDecoder().decode(firstChunk?.value);
    expect(text).toContain("event: ready");
    await reader?.cancel();
  });

  it("validates and creates a flow with auth", async () => {
    const spec = buildDraftFromPrompt({
      prompt: "Route HTTP orders to NATS with static enrichment",
      tenantId: "tenant_test",
      flowId: "flow_test_created_orders",
      revisionId: "rev_test_created_orders_v1",
      name: "Created Orders Pipeline",
    }).draft;

    const validateResponse = await app.handle(
      new Request("http://localhost/api/flows/validate", {
        method: "POST",
        headers: {
          authorization: "Bearer test-token",
          "content-type": "application/json",
        },
        body: JSON.stringify({ spec }),
      }),
    );

    expect(validateResponse.status).toBe(200);

    const createResponse = await app.handle(
      new Request("http://localhost/api/flows", {
        method: "POST",
        headers: {
          authorization: "Bearer test-token",
          "content-type": "application/json",
        },
        body: JSON.stringify({ spec, name: "Created in test" }),
      }),
    );

    expect(createResponse.status).toBe(201);
    const created = (await createResponse.json()) as { flowId: string };
    expect(created.flowId).toBe(spec.metadata.flowId);
  });

  it("deletes a flow and removes its active deployment", async () => {
    const created = await createPublishedFlow(app, "delete_me");

    const deleteResponse = await app.handle(
      new Request(`http://localhost/api/flows/${created.flowId}`, {
        method: "DELETE",
        headers: {
          authorization: "Bearer test-token",
        },
      }),
    );

    expect(deleteResponse.status).toBe(200);
    expect(await deleteResponse.json()).toEqual({
      flowId: created.flowId,
      deleted: true,
    });

    const flowsResponse = await app.handle(
      new Request("http://localhost/api/flows", {
        headers: {
          authorization: "Bearer test-token",
        },
      }),
    );
    expect(flowsResponse.status).toBe(200);
    const flows = (await flowsResponse.json()) as Array<{ id: string }>;
    expect(flows.some((flow) => flow.id === created.flowId)).toBe(false);

    const runtimeResponse = await app.handle(
      new Request("http://localhost/api/runtime/stats", {
        headers: {
          authorization: "Bearer test-token",
        },
      }),
    );
    expect(runtimeResponse.status).toBe(200);
    const runtime = (await runtimeResponse.json()) as {
      deployments?: Array<{ deploymentId?: string; flowId?: string }>;
    };
    expect(runtime.deployments?.some((deployment) => deployment.deploymentId === created.deploymentId)).toBe(false);
    expect(runtime.deployments?.some((deployment) => deployment.flowId === created.flowId)).toBe(false);
  });

  it("lists and updates sink connectors", async () => {
    const listResponse = await app.handle(
      new Request("http://localhost/api/connectors?capabilityId=snowflake_sink", {
        headers: {
          authorization: "Bearer test-token",
        },
      }),
    );

    expect(listResponse.status).toBe(200);
    const listed = (await listResponse.json()) as Array<{
      id: string;
      capabilityId: string;
      executionMode: string;
      config: Record<string, unknown>;
    }>;
    expect(listed.length).toBeGreaterThan(0);
    expect(listed[0]?.capabilityId).toBe("snowflake_sink");
    expect(listed[0]?.executionMode).toBe("adapter");

    const saveResponse = await app.handle(
      new Request("http://localhost/api/connectors", {
        method: "POST",
        headers: {
          authorization: "Bearer test-token",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          id: "snowflake_sink_default",
          name: "Snowflake Default",
          capabilityId: "snowflake_sink",
          config: {
            account: "acme-org",
            database: "ANALYTICS",
            schema: "PUBLIC",
            table: "PEOPLE_EXPORT",
          },
        }),
      }),
    );

    expect(saveResponse.status).toBe(200);
    const saved = (await saveResponse.json()) as {
      id: string;
      executionMode: string;
      config: Record<string, unknown>;
    };
    expect(saved.id).toBe("snowflake_sink_default");
    expect(saved.executionMode).toBe("adapter");
    expect(saved.config.table).toBe("PEOPLE_EXPORT");
  });

  it("stores AI provider setup in the database", async () => {
    const setupFixtureDir = mkdtempSync(join(tmpdir(), "control-api-setup-"));
    extraFixtureDirs.push(setupFixtureDir);
    const setupApp = createApp({
      config: {
        host: "127.0.0.1",
        port: 0,
        databasePath: join(setupFixtureDir, "control-plane.db"),
        runSummaryRetentionLimit: 10,
        bootstrapAdminEmail: "admin@test.local",
        bootstrapApiToken: "test-token",
        defaultTenantId: "tenant_test",
        defaultTenantName: "Tenant Test",
      },
    });

    const saveResponse = await setupApp.handle(
      new Request("http://localhost/api/setup/ai", {
        method: "POST",
        headers: {
          authorization: "Bearer test-token",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          provider: "gemini",
          enabled: true,
          apiKey: "test-ui-gemini-token",
          model: "gemini-2.5-flash",
          apiBaseUrl: "https://generativelanguage.googleapis.com/v1beta",
        }),
      }),
    );

    expect(saveResponse.status).toBe(200);
    const saved = (await saveResponse.json()) as {
      apiKeyConfigured: boolean;
      source: string;
      activeProvider: string;
      model: string;
    };
    expect(saved.apiKeyConfigured).toBe(true);
    expect(saved.source).toBe("database");
    expect(saved.activeProvider).toBe("gemini");
    expect(saved.model).toBe("gemini-2.5-flash");

    const loadResponse = await setupApp.handle(
      new Request("http://localhost/api/setup/ai", {
        headers: {
          authorization: "Bearer test-token",
        },
      }),
    );

    expect(loadResponse.status).toBe(200);
    const loaded = (await loadResponse.json()) as {
      apiKeyConfigured: boolean;
      source: string;
      activeProvider: string;
    };
    expect(loaded).toEqual(
      expect.objectContaining({
        apiKeyConfigured: true,
        source: "database",
        activeProvider: "gemini",
      }),
    );
  });

  it("builds a JSON transform preview and export-specific draft", async () => {
    const aiFixtureDir = mkdtempSync(join(tmpdir(), "control-api-ai-"));
    extraFixtureDirs.push(aiFixtureDir);
    const aiApp = createApp({
      config: {
        host: "127.0.0.1",
        port: 0,
        databasePath: join(aiFixtureDir, "control-plane.db"),
        runSummaryRetentionLimit: 10,
        bootstrapAdminEmail: "admin@test.local",
        bootstrapApiToken: "test-token",
        defaultTenantId: "tenant_test",
        defaultTenantName: "Tenant Test",
        geminiApiKey: "test-gemini-key",
        geminiModel: "gemini-2.5-flash",
        geminiApiBaseUrl: "https://generativelanguage.googleapis.com/v1beta",
      },
      fetchImpl: (async (input: string | URL | Request) => {
        const url = String(input);
        if (url.includes(":generateContent")) {
          return new Response(
            JSON.stringify({
              candidates: [
                {
                  content: {
                    parts: [
                      {
                        text: JSON.stringify({
                          suggestedName: "EU People Export",
                          summary: "Keep name, surname, and email. Exclude people living in Poland.",
                          fieldMappings: [
                            { from: "name", to: "name" },
                            { from: "surname", to: "surname" },
                            { from: "email", to: "email" },
                          ],
                          filter: {
                            type: "not",
                            predicate: {
                              type: "field_equals",
                              path: "country",
                              value: "Poland",
                            },
                          },
                          filterSummary: "Exclude records where country equals Poland.",
                          explanation: [
                            "Project the user identity fields into a new JSON object.",
                            "Drop records that belong to Poland before export.",
                          ],
                          recommendedSinkCapabilityIds: ["nats_out", "snowflake_sink"],
                        }),
                      },
                    ],
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

    const previewResponse = await aiApp.handle(
      new Request("http://localhost/api/flows/compose-json-transform", {
        method: "POST",
        headers: {
          authorization: "Bearer test-token",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          prompt: "Let's keep the name, surname and email fields and filter out people living in Poland.",
          samplePayload: {
            name: "Ada",
            surname: "Lovelace",
            email: "ada@example.com",
            country: "Germany",
          },
          sourceKind: "http",
        }),
      }),
    );

    expect(previewResponse.status).toBe(200);
    const previewPayload = (await previewResponse.json()) as {
      assistant: { provider: string };
      preview: { accepted: boolean; output?: unknown };
      plan: { fieldMappings: Array<{ from: string; to: string }> };
      sourceBinding?: { ref: string; connectorId: string };
      exportOptions: Array<{ id: string }>;
    };
    expect(previewPayload.assistant.provider).toBe("gemini");
    expect(previewPayload.preview.accepted).toBe(true);
    expect(previewPayload.preview.output).toEqual({
      name: "Ada",
      surname: "Lovelace",
      email: "ada@example.com",
    });
    expect(previewPayload.plan.fieldMappings).toHaveLength(3);
    expect(previewPayload.sourceBinding?.ref).toBe("/ingest/eu-people-export");
    expect(previewPayload.exportOptions[0]?.id).toBe("nats_out");

    const draftResponse = await aiApp.handle(
      new Request("http://localhost/api/flows/compose-json-transform", {
        method: "POST",
        headers: {
          authorization: "Bearer test-token",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          prompt: "Let's keep the name, surname and email fields and filter out people living in Poland.",
          samplePayload: {
            name: "Ada",
            surname: "Lovelace",
            email: "ada@example.com",
            country: "Germany",
          },
          sourceKind: "http",
          sinkCapabilityId: "nats_out",
        }),
      }),
    );

    expect(draftResponse.status).toBe(200);
    const draftPayload = (await draftResponse.json()) as {
      draft: FlowSpec;
      simulation: { accepted: number };
    };
    expect(draftPayload.draft.processors[0]).toEqual(
      expect.objectContaining({
        kind: "filter",
      }),
    );
    expect(draftPayload.draft.processors[1]).toEqual(
      expect.objectContaining({
        kind: "map",
        mode: "project",
      }),
    );
    expect(draftPayload.draft.sinks[0]?.connector.capabilityId).toBe("nats_out");
    expect(draftPayload.simulation.accepted).toBe(1);
  });

  it("uses an explicit sink connector override when composing a draft", async () => {
    const response = await app.handle(
      new Request("http://localhost/api/flows/compose-json-transform", {
        method: "POST",
        headers: {
          authorization: "Bearer test-token",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          prompt: "Keep name and email for analytics.",
          samplePayload: {
            name: "Ada",
            email: "ada@example.com",
            country: "Germany",
          },
          sourceKind: "http",
          sinkCapabilityId: "snowflake_sink",
          sinkConnectorId: "snowflake_sink_default",
        }),
      }),
    );

    expect(response.status).toBe(200);
    const payload = (await response.json()) as { draft?: FlowSpec };
    expect(payload.draft?.sinks[0]?.connector.connectorId).toBe("snowflake_sink_default");
    expect(payload.draft?.sinks[0]?.connector.executionMode).toBe("adapter");
  });

  it("expands ecommerce keep instructions in the heuristic planner", async () => {
    const response = await app.handle(
      new Request("http://localhost/api/flows/compose-json-transform", {
        method: "POST",
        headers: {
          authorization: "Bearer test-token",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          prompt: "Keep ecommerce identity, product, cart, revenue, and attribution fields. Drop UI metadata.",
          samplePayload: {
            event_name: "purchase",
            user_id: "user_123",
            order_id: "ord_456",
            product_id: "sku_789",
            amount: 29.99,
            currency: "USD",
            ui_color: "blue",
            cart: {
              items: 2,
            },
          },
          sourceKind: "http",
        }),
      }),
    );

    expect(response.status).toBe(200);
    const payload = (await response.json()) as {
      assistant: { provider: string };
      preview: { accepted: boolean; output?: Record<string, unknown> };
      plan: { fieldMappings: Array<{ from: string; to: string }> };
    };
    expect(payload.assistant.provider).toBe("heuristic");
    expect(payload.preview.accepted).toBe(true);
    expect(payload.preview.output).toEqual({
      event_name: "purchase",
      user_id: "user_123",
      order_id: "ord_456",
      product_id: "sku_789",
      amount: 29.99,
      currency: "USD",
      cart: {
        items: 2,
      },
    });
    expect(payload.plan.fieldMappings.map((mapping) => mapping.from)).toContain("user_id");
    expect(payload.plan.fieldMappings.map((mapping) => mapping.from)).toContain("cart.items");
  });

  it("auto-provisions a concrete source connector when publishing an authored flow", async () => {
    const composeResponse = await app.handle(
      new Request("http://localhost/api/flows/compose-json-transform", {
        method: "POST",
        headers: {
          authorization: "Bearer test-token",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          prompt: "Keep email only.",
          name: "Customers Cleaned",
          samplePayload: {
            email: "ada@example.com",
            country: "Germany",
          },
          sourceKind: "http",
          sinkCapabilityId: "http_out",
        }),
      }),
    );

    expect(composeResponse.status).toBe(200);
    const composePayload = (await composeResponse.json()) as {
      draft: FlowSpec;
      sourceBinding: { ref: string; connectorId: string };
    };
    expect(composePayload.sourceBinding.ref).toBe("/ingest/customers-cleaned");
    expect(composePayload.draft.sources[0]?.connector.connectorId).toBe(
      composePayload.sourceBinding.connectorId,
    );

    const createResponse = await app.handle(
      new Request("http://localhost/api/flows", {
        method: "POST",
        headers: {
          authorization: "Bearer test-token",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          tenantId: "tenant_test",
          name: "Customers Cleaned",
          samplePayload: {
            email: "ada@example.com",
          },
          spec: composePayload.draft,
        }),
      }),
    );

    expect(createResponse.status).toBe(201);

    const connectorsResponse = await app.handle(
      new Request("http://localhost/api/connectors?capabilityId=http_in", {
        headers: {
          authorization: "Bearer test-token",
        },
      }),
    );

    expect(connectorsResponse.status).toBe(200);
    const connectors = (await connectorsResponse.json()) as Array<{
      id: string;
      config: Record<string, unknown>;
    }>;
    expect(connectors).toContainEqual(
      expect.objectContaining({
        id: composePayload.sourceBinding.connectorId,
        config: expect.objectContaining({
          path: "/ingest/customers-cleaned",
        }),
      }),
    );
  });

  it("lists overview data with auth", async () => {
    const response = await app.handle(
      new Request("http://localhost/api/overview", {
        headers: {
          authorization: "Bearer test-token",
        },
      }),
    );

    expect(response.status).toBe(200);
    const data = (await response.json()) as { flows: number };
    expect(data.flows).toBeGreaterThanOrEqual(0);
  });

  it("exposes runtime deployments and accepts aggregated runtime telemetry writes", async () => {
    const published = await createPublishedFlow(app, "runtime-stats");
    const deploymentsResponse = await app.handle(
      new Request("http://localhost/api/runtime/deployments/active", {
        headers: {
          authorization: "Bearer test-token",
        },
      }),
    );

    expect(deploymentsResponse.status).toBe(200);
    const deploymentsPayload = (await deploymentsResponse.json()) as {
      deployments: Array<{ deployment: { id: string; rolloutStatus: string } }>;
    };
    expect(deploymentsPayload.deployments.length).toBeGreaterThan(0);
    const activeDeployment = deploymentsPayload.deployments.find(
      (record) => record.deployment.id === published.deploymentId,
    )?.deployment;
    expect(activeDeployment).toBeDefined();
    const activeDeploymentId = activeDeployment?.id ?? "";

    const runtimeStatsResponse = await app.handle(
      new Request("http://localhost/api/runtime/stats", {
        method: "POST",
        headers: {
          authorization: "Bearer test-token",
          "content-type": "application/json",
        },
        body: JSON.stringify([
          {
            deploymentId: activeDeploymentId,
            reporterId: "router-workers-test",
            flowId: published.flowId,
            revisionId: published.revisionId,
            acceptedCount: 10,
            processedCount: 9,
            deliveredCount: 8,
            retryingCount: 1,
            dlqCount: 0,
            failedCount: 0,
            filteredCount: 0,
            dedupedCount: 0,
            sinkAttemptCount: 9,
            sinkSuccessCount: 8,
            sinkFailureCount: 1,
            inflightCount: 1,
            backlogCount: 2,
            lastAcceptedAt: "2026-04-01T00:00:00.000Z",
            lastProcessedAt: "2026-04-01T00:00:01.000Z",
            lastError: "HTTP 503",
            updatedAt: "2026-04-01T00:00:01.000Z",
          },
        ]),
      }),
    );

    expect(runtimeStatsResponse.status).toBe(202);

    const runtimeStatsViewResponse = await app.handle(
      new Request("http://localhost/api/runtime/stats", {
        headers: {
          authorization: "Bearer test-token",
        },
      }),
    );
    expect(runtimeStatsViewResponse.status).toBe(200);
    const runtimeStatsPayload = (await runtimeStatsViewResponse.json()) as {
      summary: { backlogCount: number; deliveredCount: number };
      observability: { mode: string };
      deployments: Array<{ deploymentId: string; state: string }>;
    };
    expect(runtimeStatsPayload.summary.backlogCount).toBe(2);
    expect(runtimeStatsPayload.summary.deliveredCount).toBe(8);
    expect(runtimeStatsPayload.observability.mode).toBe("otel-primary");
    expect(runtimeStatsPayload.deployments[0]?.deploymentId).toBe(activeDeploymentId);

    const deploymentStatusResponse = await app.handle(
      new Request(`http://localhost/api/runtime/deployments/${activeDeploymentId}/status`, {
        method: "POST",
        headers: {
          authorization: "Bearer test-token",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          rolloutStatus: "activated",
        }),
      }),
    );

    expect(deploymentStatusResponse.status).toBe(200);
  });

  it("stores adapter workload status snapshots by reporter", async () => {
    const writeResponse = await app.handle(
      new Request("http://localhost/api/runtime/adapter-workloads", {
        method: "POST",
        headers: {
          authorization: "Bearer test-token",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          reporterId: "adapter-redpanda-test",
          reportedAt: "2026-04-07T09:00:00.000Z",
          workloads: [
            {
              key: "sink:s3_sink_default",
              connectorId: "s3_sink_default",
              capabilityId: "s3_sink",
              manifestId: "s3-sink",
              deploymentIds: ["deploy-s3"],
              flowIds: ["flow-s3"],
              revisionIds: ["rev-s3-v1"],
              runtimeRole: "sink",
              inputRef: "router.work.connect.s3_sink_default.>",
              outputRef: "file:///tmp/s3/events.ndjson",
              status: "running",
              backend: "kubernetes",
              consumerRef: "rpc_s3_sink_default",
              targetKind: "file",
              artifactPath: "/tmp/s3/events.ndjson",
              configPath: "/etc/redpanda-connect/connect.json",
              containerName: "redpanda-connect-sink-s3",
              startedAt: "2026-04-07T08:59:59.000Z",
              restartCount: 0,
              recentLogs: ["Applied Kubernetes Redpanda Connect deployment redpanda-connect-sink-s3"],
            },
          ],
        }),
      }),
    );

    expect(writeResponse.status).toBe(202);
    expect(await writeResponse.json()).toEqual({
      updated: 1,
      deleted: 0,
      reportedAt: "2026-04-07T09:00:00.000Z",
    });

    const listResponse = await app.handle(
      new Request("http://localhost/api/runtime/adapter-workloads", {
        headers: {
          authorization: "Bearer test-token",
        },
      }),
    );

    expect(listResponse.status).toBe(200);
    const listPayload = (await listResponse.json()) as {
      workloads: Array<{
        reporterId: string;
        key: string;
        deploymentIds: string[];
        flowIds: string[];
        revisionIds: string[];
        backend: string;
        status: string;
        recentLogs: string[];
      }>;
    };
    expect(listPayload.workloads).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          reporterId: "adapter-redpanda-test",
          key: "sink:s3_sink_default",
          deploymentIds: ["deploy-s3"],
          flowIds: ["flow-s3"],
          revisionIds: ["rev-s3-v1"],
          backend: "kubernetes",
          status: "running",
          recentLogs: ["Applied Kubernetes Redpanda Connect deployment redpanda-connect-sink-s3"],
        }),
      ]),
    );

    const replaceWithEmptyResponse = await app.handle(
      new Request("http://localhost/api/runtime/adapter-workloads", {
        method: "POST",
        headers: {
          authorization: "Bearer test-token",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          reporterId: "adapter-redpanda-test",
          workloads: [],
        }),
      }),
    );

    expect(replaceWithEmptyResponse.status).toBe(202);
    expect((await replaceWithEmptyResponse.json()) as { deleted: number }).toEqual(
      expect.objectContaining({ updated: 0, deleted: 1 }),
    );
  });

  it("claims and completes pending replay requests", async () => {
    const published = await createPublishedFlow(app, "replay");
    const createReplayResponse = await app.handle(
      new Request("http://localhost/api/replays", {
        method: "POST",
        headers: {
          authorization: "Bearer test-token",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          flowId: published.flowId,
          revisionId: published.revisionId,
          reason: "Replay failed sink delivery",
          sourceStream: "router.dlq.test",
        }),
      }),
    );
    expect(createReplayResponse.status).toBe(201);

    const pendingResponse = await app.handle(
      new Request("http://localhost/api/runtime/replays/pending", {
        headers: {
          authorization: "Bearer test-token",
        },
      }),
    );

    expect(pendingResponse.status).toBe(200);
    const pendingPayload = (await pendingResponse.json()) as {
      requests: Array<{ id: string }>;
    };
    expect(pendingPayload.requests.length).toBeGreaterThan(0);

    const replayId = pendingPayload.requests[0]?.id;

    const claimResponse = await app.handle(
      new Request(`http://localhost/api/runtime/replays/${replayId}/claim`, {
        method: "POST",
        headers: {
          authorization: "Bearer test-token",
        },
      }),
    );
    expect(claimResponse.status).toBe(200);

    const completeResponse = await app.handle(
      new Request(`http://localhost/api/runtime/replays/${replayId}/complete`, {
        method: "POST",
        headers: {
          authorization: "Bearer test-token",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          status: "completed",
        }),
      }),
    );
    expect(completeResponse.status).toBe(200);
  });

  it("stores and lists recent runtime samples for the builder", async () => {
    const published = await createPublishedFlow(app, "runtime-samples");
    const deploymentsResponse = await app.handle(
      new Request("http://localhost/api/runtime/deployments/active", {
        headers: {
          authorization: "Bearer test-token",
        },
      }),
    );

    expect(deploymentsResponse.status).toBe(200);
    const deploymentsPayload = (await deploymentsResponse.json()) as {
      deployments: Array<{ deployment: { id: string; flowId: string; revisionId: string } }>;
    };

    const deployment = deploymentsPayload.deployments.find(
      (record) => record.deployment.id === published.deploymentId,
    )?.deployment;
    expect(deployment).toBeDefined();

    const writeResponse = await app.handle(
      new Request("http://localhost/api/runtime/samples", {
        method: "POST",
        headers: {
          authorization: "Bearer test-token",
          "content-type": "application/json",
        },
        body: JSON.stringify([
          {
            deploymentId: deployment?.id,
            flowId: deployment?.flowId,
            revisionId: deployment?.revisionId,
            sourceKind: "http",
            sourceRef: "/ingest/customers",
            payload: {
              name: "Ada",
              email: "ada@example.com",
              country: "DE",
            },
            observedAt: "2026-04-03T10:00:00.000Z",
          },
        ]),
      }),
    );

    expect(writeResponse.status).toBe(202);
    expect(await writeResponse.json()).toEqual({ updated: 1 });

    const listResponse = await app.handle(
      new Request("http://localhost/api/runtime/samples/recent?sourceKind=http&limit=5", {
        headers: {
          authorization: "Bearer test-token",
        },
      }),
    );

    expect(listResponse.status).toBe(200);
    const listPayload = (await listResponse.json()) as {
      samples: Array<{
        deploymentId: string;
        flowId: string;
        flowName: string;
        sourceKind: string;
        sourceRef: string;
        payload: Record<string, unknown>;
      }>;
    };
    expect(listPayload.samples[0]).toEqual(
      expect.objectContaining({
        deploymentId: deployment?.id,
        flowId: deployment?.flowId,
        sourceKind: "http",
        sourceRef: "/ingest/customers",
        payload: {
          name: "Ada",
          email: "ada@example.com",
          country: "DE",
        },
      }),
    );
    expect(listPayload.samples[0]?.flowName).toBeTruthy();
  });

  it("accepts batched run summaries and prunes retention once per batch", async () => {
    const batchFixtureDir = mkdtempSync(join(tmpdir(), "control-api-batch-"));
    extraFixtureDirs.push(batchFixtureDir);
    const batchApp = createApp({
      config: {
        host: "127.0.0.1",
        port: 0,
        databasePath: join(batchFixtureDir, "control-plane.db"),
        runSummaryRetentionLimit: 5,
        bootstrapAdminEmail: "admin@test.local",
        bootstrapApiToken: "test-token",
        defaultTenantId: "tenant_test",
        defaultTenantName: "Tenant Test",
      },
    });

    const batch = Array.from({ length: 7 }, (_, index) => ({
      flowId: "flow_test_batch_runs",
      revisionId: "rev_test_batch_runs_v1",
      deploymentId: `deploy-batch-${index}`,
      messageId: `message-batch-${index}`,
      status: "succeeded",
      sourceRef: "events.source.nats",
      traceId: `trace-batch-${index}`,
      processedCount: 1,
      errorCount: 0,
      startedAt: `2026-04-05T00:00:0${index}.000Z`,
      finishedAt: `2026-04-05T00:00:0${index}.100Z`,
      lastError: null,
    }));

    const insertResponse = await batchApp.handle(
      new Request("http://localhost/api/runtime/runs/batch", {
        method: "POST",
        headers: {
          authorization: "Bearer test-token",
          "content-type": "application/json",
        },
        body: JSON.stringify(batch),
      }),
    );

    expect(insertResponse.status).toBe(202);
    expect(await insertResponse.json()).toEqual({ inserted: 7 });

    const runsResponse = await batchApp.handle(
      new Request("http://localhost/api/runs", {
        headers: {
          authorization: "Bearer test-token",
        },
      }),
    );

    expect(runsResponse.status).toBe(200);
    const runs = (await runsResponse.json()) as Array<{ traceId: string }>;
    expect(runs).toHaveLength(5);
    const traceIds = runs.map((run) => run.traceId);
    expect(traceIds).toEqual(
      expect.arrayContaining([
        "trace-batch-6",
        "trace-batch-5",
        "trace-batch-4",
      ]),
    );
    expect(traceIds).not.toContain("trace-batch-1");
    expect(traceIds).not.toContain("trace-batch-0");
  });

  it("finalizes enqueued adapter-backed runs when adapter results arrive", async () => {
    const runId = "run-adapter-1";

    const enqueueResponse = await app.handle(
      new Request("http://localhost/api/runtime/runs", {
        method: "POST",
        headers: {
          authorization: "Bearer test-token",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          id: runId,
          flowId: "flow_test_adapter_runs",
          revisionId: "rev_test_adapter_runs_v1",
          deploymentId: "deploy-adapter",
          messageId: "message-adapter-1",
          status: "enqueued",
          sourceRef: "events.source.nats",
          traceId: "trace-adapter-1",
          processedCount: 1,
          errorCount: 0,
          startedAt: "2026-04-05T10:00:00.000Z",
          finishedAt: "2026-04-05T10:00:00.100Z",
          lastError: null,
          targetSinkIds: ["sink-kafka"],
          awaitedSinkIds: ["sink-kafka"],
        }),
      }),
    );

    expect(enqueueResponse.status).toBe(201);

    const enqueuedRunsResponse = await app.handle(
      new Request("http://localhost/api/runs", {
        headers: {
          authorization: "Bearer test-token",
        },
      }),
    );
    const enqueuedRuns = (await enqueuedRunsResponse.json()) as Array<{
      id: string;
      status: string;
      messageId: string;
      detail: string;
    }>;
    expect(enqueuedRuns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          id: runId,
          status: "enqueued",
          messageId: "message-adapter-1",
        }),
      ]),
    );

    const finalizeResponse = await app.handle(
      new Request("http://localhost/api/runtime/adapter-results", {
        method: "POST",
        headers: {
          authorization: "Bearer test-token",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          runId,
          sinkId: "sink-kafka",
          connectorId: "kafka_out_default",
          capabilityId: "kafka_out",
          status: "succeeded",
          targetRef: "kafka://orders.created",
          error: null,
          startedAt: "2026-04-05T10:00:00.100Z",
          finishedAt: "2026-04-05T10:00:00.300Z",
        }),
      }),
    );

    expect(finalizeResponse.status).toBe(202);
    expect(await finalizeResponse.json()).toEqual({ updated: true, status: "succeeded" });

    const finalizedRunsResponse = await app.handle(
      new Request("http://localhost/api/runs", {
        headers: {
          authorization: "Bearer test-token",
        },
      }),
    );
    const finalizedRuns = (await finalizedRunsResponse.json()) as Array<{
      id: string;
      status: string;
      messageId: string;
      detail: string;
    }>;
    expect(finalizedRuns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          id: runId,
          status: "succeeded",
          messageId: "message-adapter-1",
          detail: "Delivery completed without sink errors.",
        }),
      ]),
    );
  });

  it("serves adapter capabilities from the domain catalog when adapter is configured", async () => {
    const adapterAwareApp = createApp({
      config: {
        host: "127.0.0.1",
        port: 0,
        databasePath: join(fixtureDir, "control-plane-adapter.db"),
        runSummaryRetentionLimit: 10,
        bootstrapAdminEmail: "admin@test.local",
        bootstrapApiToken: "test-token",
        defaultTenantId: "tenant_test",
        defaultTenantName: "Tenant Test",
        adapterRedpandaUrl: "http://adapter-redpanda.test:7103",
      },
      fetchImpl: (async () => {
        throw new Error("adapter catalog should not be fetched");
      }) as unknown as typeof fetch,
    });

    const response = await adapterAwareApp.handle(
      new Request("http://localhost/api/capabilities", {
        headers: {
          authorization: "Bearer test-token",
        },
      }),
    );

    expect(response.status).toBe(200);
    const payload = (await response.json()) as {
      adapter: Array<{ id: string }>;
    };
    expect(payload.adapter.map((capability) => capability.id)).toEqual([
      "snowflake_sink",
      "bigquery_sink",
      "s3_sink",
      "kafka_in",
      "kafka_out",
    ]);
  });

});
