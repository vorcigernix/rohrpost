import { describe, expect, test } from "bun:test";
import { compileFlowSpec, type CanonicalEnvelope, type FlowSpec } from "@rohrpost/shared-flow-spec";
import { ControlApiDeploymentSource, StaticDeploymentSource } from "../deployment-source";
import { encodeJsonMessage } from "../nats";
import { IngressOverloadedError, RouterWorkerRuntime, createRouterWorkerRuntime } from "../phase2-worker";
import {
  buildDeploymentIngressPattern,
  buildDeploymentReplayPattern,
  buildDeploymentRetryPattern,
} from "../jetstream";
import type { DeploymentTargetMap, MessageBus, RouterDeployment, RouterWorkerRuntimeConfig } from "../phase2-types";

function buildRuntimeConfig(): RouterWorkerRuntimeConfig {
  return {
    serviceName: "router-workers-test",
    httpHost: "127.0.0.1",
    httpPort: 0,
    pollIntervalMs: 0,
    subscriptionConcurrency: 4,
    httpTimeoutMs: 500,
    retryBaseDelayMs: 1,
    maxAttempts: 3,
    runHistoryLimit: 3,
    dlqHistoryLimit: 2,
    metricsFlushIntervalMs: 5,
    runtimeSampleCaptureIntervalMs: 10_000,
    runtimeSampleMaxPayloadBytes: 8_192,
    backlogWarningThreshold: 10,
    ingressMaxBufferedPerDeployment: 5,
    ingressMaxBufferedTotal: 20,
    ingressRetryAfterMs: 250,
    processingStallMs: 100,
    replayStream: "replay",
    dlqStream: "dlq",
    ingressStream: "ingress",
  };
}

function buildHttpFlow(): FlowSpec {
  return {
    version: 1,
    metadata: {
      tenantId: "tenant-a",
      flowId: "flow-http",
      revisionId: "rev-http-v1",
      name: "HTTP Flow",
      description: "HTTP sink delivery",
    },
    sources: [
      {
        id: "source-http",
        kind: "http",
        connector: {
          capabilityId: "http_in",
          connectorId: "http_in_default",
          executionMode: "native",
        },
        stream: "ingress",
        nextNodeIds: ["processor-enrich"],
      },
    ],
    processors: [
      {
        id: "processor-enrich",
        kind: "enrich_static",
        values: {
          routedBy: "router-workers",
        },
        nextNodeIds: ["route-http"],
      },
    ],
    routes: [
      {
        id: "route-http",
        fromNodeId: "processor-enrich",
        predicate: { type: "always" },
        toSinkIds: ["sink-http"],
        priority: 10,
      },
    ],
    sinks: [
      {
        id: "sink-http",
        kind: "http",
        connector: {
          capabilityId: "http_out",
          connectorId: "http_out_default",
          executionMode: "native",
        },
        deliveryGuarantee: "idempotent",
        stream: "work",
      },
    ],
    retryPolicy: {
      maxAttempts: 3,
      initialBackoffMs: 25,
      maxBackoffMs: 250,
      multiplier: 2,
      retryableStatusCodes: [408, 429, 500, 502, 503, 504],
    },
    dlqPolicy: {
      enabled: true,
      sinkId: "sink-http",
      reasonFormat: "json",
    },
    batchingPolicy: {
      enabled: false,
      batchSize: 1,
    },
    idempotencyStrategy: "message_id",
  };
}

function buildNatsFlow(): FlowSpec {
  return {
    version: 1,
    metadata: {
      tenantId: "tenant-b",
      flowId: "flow-nats",
      revisionId: "rev-nats-v1",
      name: "NATS Flow",
      description: "NATS sink delivery",
    },
    sources: [
      {
        id: "source-http",
        kind: "http",
        connector: {
          capabilityId: "http_in",
          connectorId: "http_in_default",
          executionMode: "native",
        },
        stream: "ingress",
        nextNodeIds: ["processor-enrich"],
      },
    ],
    processors: [
      {
        id: "processor-enrich",
        kind: "enrich_static",
        values: {
          routedBy: "router-workers",
        },
        nextNodeIds: ["route-nats"],
      },
    ],
    routes: [
      {
        id: "route-nats",
        fromNodeId: "processor-enrich",
        predicate: { type: "always" },
        toSinkIds: ["sink-nats"],
        priority: 10,
      },
    ],
    sinks: [
      {
        id: "sink-nats",
        kind: "nats",
        connector: {
          capabilityId: "nats_out",
          connectorId: "nats_out_default",
          executionMode: "native",
        },
        deliveryGuarantee: "idempotent",
        stream: "work",
      },
    ],
    retryPolicy: {
      maxAttempts: 2,
      initialBackoffMs: 25,
      maxBackoffMs: 250,
      multiplier: 2,
      retryableStatusCodes: [408, 429, 500, 502, 503, 504],
    },
    dlqPolicy: {
      enabled: true,
      sinkId: "sink-nats",
      reasonFormat: "json",
    },
    batchingPolicy: {
      enabled: false,
      batchSize: 1,
    },
    idempotencyStrategy: "message_id",
  };
}

function buildAdapterFlow(input?: {
  sinkKind?: "kafka" | "s3";
  sinkId?: string;
  routeId?: string;
  sinkCapabilityId?: "kafka_out" | "s3_sink";
  sinkConnectorId?: string;
  flowId?: string;
  revisionId?: string;
  name?: string;
  description?: string;
}): FlowSpec {
  const sinkKind = input?.sinkKind ?? "kafka";
  const sinkId = input?.sinkId ?? "sink-kafka";
  const routeId = input?.routeId ?? "route-kafka";
  const sinkCapabilityId = input?.sinkCapabilityId ?? "kafka_out";
  const sinkConnectorId = input?.sinkConnectorId ?? "kafka_out_default";
  return {
    version: 1,
    metadata: {
      tenantId: "tenant-c",
      flowId: input?.flowId ?? "flow-kafka",
      revisionId: input?.revisionId ?? "rev-kafka-v1",
      name: input?.name ?? "Kafka Adapter Flow",
      description: input?.description ?? "Adapter sink handoff",
    },
    sources: [
      {
        id: "source-http",
        kind: "http",
        connector: {
          capabilityId: "http_in",
          connectorId: "http_in_default",
          executionMode: "native",
        },
        stream: "ingress",
        nextNodeIds: ["processor-enrich"],
      },
    ],
    processors: [
      {
        id: "processor-enrich",
        kind: "enrich_static",
        values: {
          routedBy: "router-workers",
        },
        nextNodeIds: [routeId],
      },
    ],
    routes: [
      {
        id: routeId,
        fromNodeId: "processor-enrich",
        predicate: { type: "always" },
        toSinkIds: [sinkId],
        priority: 10,
      },
    ],
    sinks: [
      {
        id: sinkId,
        kind: sinkKind,
        connector: {
          capabilityId: sinkCapabilityId,
          connectorId: sinkConnectorId,
          executionMode: "adapter",
        },
        deliveryGuarantee: "append_only",
        stream: "work",
      },
    ],
    retryPolicy: {
      maxAttempts: 1,
      initialBackoffMs: 25,
      maxBackoffMs: 250,
      multiplier: 2,
      retryableStatusCodes: [408, 429, 500, 502, 503, 504],
    },
    dlqPolicy: {
      enabled: true,
      sinkId: "sink-kafka",
      reasonFormat: "json",
    },
    batchingPolicy: {
      enabled: false,
      batchSize: 1,
    },
    idempotencyStrategy: "message_id",
  };
}

function buildDeployment(
  spec: FlowSpec,
  sinkTargets: DeploymentTargetMap,
): RouterDeployment {
  return {
    id: `${spec.metadata.flowId}-deployment`,
    tenantId: spec.metadata.tenantId,
    flowId: spec.metadata.flowId,
    revisionId: spec.metadata.revisionId,
    active: true,
    spec,
    compiled: compileFlowSpec(spec),
    sourceSubjects: [
      buildDeploymentIngressPattern(spec.metadata.tenantId, spec.metadata.flowId, spec.metadata.revisionId),
      buildDeploymentRetryPattern(spec.metadata.tenantId, spec.metadata.flowId, spec.metadata.revisionId),
      buildDeploymentReplayPattern(spec.metadata.tenantId, spec.metadata.flowId, spec.metadata.revisionId),
    ],
    natsSourceSubjects: [],
    sinkTargets: sinkTargets.sinks,
    connectors: {},
  };
}

function buildStatefulProcessorFlow(processor: FlowSpec["processors"][number]): FlowSpec {
  const spec = buildHttpFlow();
  spec.sources[0].nextNodeIds = [processor.id];
  spec.processors = [processor];
  spec.routes[0].fromNodeId = processor.id;
  return spec;
}

function buildTestEnvelope(spec: FlowSpec, messageId: string, payload: unknown): CanonicalEnvelope {
  return {
    tenantId: spec.metadata.tenantId,
    flowId: spec.metadata.flowId,
    revisionId: spec.metadata.revisionId,
    messageId,
    sourceRef: "test-source",
    partitionKey: spec.metadata.tenantId,
    headers: {},
    payload,
    receivedAt: "2026-07-03T10:00:00.500Z",
    traceId: messageId,
  };
}

function createBusSpy() {
  const publishes: Array<{ subject: string; data: string }> = [];
  const corePublishes: Array<{ subject: string; data: string }> = [];
  const jetstreamPublishes: Array<{ subject: string; data: string }> = [];
  const subscriptions: Array<{
    kind: "core" | "jetstream";
    subject: string;
    durableName?: string;
    handler: (data: Uint8Array, metadata: { subject: string; sequence?: number }) => Promise<void>;
    unsubscribed: boolean;
  }> = [];
  const bus: MessageBus = {
    async publish(subject, data) {
      const entry = { subject, data: new TextDecoder().decode(data) };
      publishes.push(entry);
      corePublishes.push(entry);
    },
    async publishToJetStream(subject, data) {
      const entry = { subject, data: new TextDecoder().decode(data) };
      publishes.push(entry);
      jetstreamPublishes.push(entry);
    },
    async subscribe(subject, handler) {
      const entry = {
        kind: "core" as const,
        subject,
        handler,
        unsubscribed: false,
      };
      subscriptions.push(entry);
      return {
        async unsubscribe() {
          entry.unsubscribed = true;
          return undefined;
        },
      };
    },
    async subscribeToJetStream(subject, handler, options) {
      const entry = {
        kind: "jetstream" as const,
        subject,
        durableName: options.durableName,
        handler,
        unsubscribed: false,
      };
      subscriptions.push(entry);
      return {
        async unsubscribe() {
          entry.unsubscribed = true;
          return undefined;
        },
      };
    },
    async close() {
      return undefined;
    },
  };

  return { bus, publishes, corePublishes, jetstreamPublishes, subscriptions };
}

describe("phase2 worker runtime", () => {
  test("maps control-api active flows into deployments", async () => {
    const spec = buildHttpFlow();
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
                    id: `${spec.metadata.flowId}-deployment`,
                    flowId: spec.metadata.flowId,
                    revisionId: spec.metadata.revisionId,
                    status: "active",
                    rolloutStatus: "activated",
                  },
                  revision: {
                    id: spec.metadata.revisionId,
                    spec,
                  },
                  connectors: {
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
    expect(deployments[0].sourceSubjects[0]).toBe(
      "router.ingress.tenant-a.flow-http.rev-http-v1.>",
    );
  });

  test("skips invalid runtime deployments instead of crashing startup", async () => {
    const invalidSpec = {
      ...buildAdapterFlow(),
      metadata: {
        ...buildAdapterFlow().metadata,
        flowId: "flow-invalid",
        revisionId: "rev-invalid-v1",
      },
      retryPolicy: {
        maxAttempts: 3,
        initialBackoffMs: 25,
        maxBackoffMs: 250,
        multiplier: 2,
        retryableStatusCodes: [408, 429, 500, 502, 503, 504],
      },
    } satisfies FlowSpec;
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
                    id: "deploy-invalid",
                    flowId: invalidSpec.metadata.flowId,
                    revisionId: invalidSpec.metadata.revisionId,
                    status: "active",
                    rolloutStatus: "activated",
                  },
                  revision: {
                    id: invalidSpec.metadata.revisionId,
                    spec: invalidSpec,
                  },
                  connectors: {
                    kafka_out_default: {
                      id: "kafka_out_default",
                      capabilityId: "kafka_out",
                      executionMode: "adapter",
                      config: {
                        topic: "orders.created",
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
    expect(deployments).toHaveLength(0);
    expect(source.getLoadErrors?.()).toEqual([
      expect.objectContaining({
        deploymentId: "deploy-invalid",
        flowId: "flow-invalid",
        revisionId: "rev-invalid-v1",
        reason: "FlowSpec validation failed",
      }),
    ]);
  });

  test("survives an initial deployment sync failure and retries later", async () => {
    const spec = buildHttpFlow();
    const deployment = buildDeployment(spec, {
      sinks: {
        http_out_default: {
          kind: "http",
          connectorId: "http_out_default",
          url: "https://example.test/webhook",
        },
      },
    });

    let attempts = 0;
    const source = {
      async loadDeployments() {
        attempts += 1;
        if (attempts === 1) {
          throw new Error("control-api unavailable");
        }

        return [deployment];
      },
      getLoadErrors() {
        return [];
      },
    };

    const runtime = await createRouterWorkerRuntime({
      config: buildRuntimeConfig(),
      deploymentSource: source,
    });

    expect(runtime.getDeployments()).toHaveLength(0);

    await runtime.syncDeployments();

    expect(runtime.getDeployments()).toHaveLength(1);

    await runtime.stop();
  });

  test("delivers to HTTP sinks and records runs", async () => {
    const spec = buildHttpFlow();
    let fetchCalls = 0;
    const deployment = buildDeployment(spec, {
      sinks: {
        http_out_default: {
          kind: "http",
          connectorId: "http_out_default",
          url: "https://example.test/webhook",
        },
      },
    });

    const runtime = new RouterWorkerRuntime(buildRuntimeConfig(), new StaticDeploymentSource([deployment]), {
      fetchImpl: (async () => {
        fetchCalls += 1;
        return new Response("ok", { status: 200 });
      }) as unknown as typeof fetch,
    });

    await runtime.syncDeployments();
    const result = await runtime.ingestEnvelope({
      deploymentId: deployment.id,
      tenantId: spec.metadata.tenantId,
      flowId: spec.metadata.flowId,
      revisionId: spec.metadata.revisionId,
      messageId: "message-1",
      sourceRef: "http://localhost/events",
      partitionKey: spec.metadata.tenantId,
      payload: { kind: "order", amount: 42 },
      headers: {
        "content-type": "application/json",
      },
    });

    expect(fetchCalls).toBe(1);
    expect(result.run.status).toBe("delivered");
    expect(runtime.getRuns()).toHaveLength(1);
    expect(runtime.getDlq()).toHaveLength(0);
  });

  test("projects JSON payloads before HTTP delivery when map mode is project", async () => {
    const spec = buildHttpFlow();
    spec.processors = [
      {
        id: "processor-project",
        kind: "map",
        mode: "project",
        mappings: [
          { from: "name", to: "name" },
          { from: "surname", to: "surname" },
          { from: "email", to: "email" },
        ],
        nextNodeIds: ["route-http"],
      },
    ];
    spec.routes = [
      {
        id: "route-http",
        fromNodeId: "processor-project",
        predicate: { type: "always" },
        toSinkIds: ["sink-http"],
        priority: 10,
      },
    ];
    spec.sources[0].nextNodeIds = ["processor-project"];

    const deployment = buildDeployment(spec, {
      sinks: {
        http_out_default: {
          kind: "http",
          connectorId: "http_out_default",
          url: "https://example.test/webhook",
        },
      },
    });

    let deliveredBody: unknown;
    const runtime = new RouterWorkerRuntime(buildRuntimeConfig(), new StaticDeploymentSource([deployment]), {
      fetchImpl: (async (input: RequestInfo | URL, init?: RequestInit) => {
        void input;
        deliveredBody = init?.body ? JSON.parse(String(init.body)) : null;
        return new Response("ok", { status: 200 });
      }) as unknown as typeof fetch,
    });

    await runtime.syncDeployments();
    const result = await runtime.ingestEnvelope({
      deploymentId: deployment.id,
      tenantId: spec.metadata.tenantId,
      flowId: spec.metadata.flowId,
      revisionId: spec.metadata.revisionId,
      messageId: "message-project-1",
      sourceRef: "http://localhost/events",
      partitionKey: spec.metadata.tenantId,
      payload: {
        name: "Ada",
        surname: "Lovelace",
        email: "ada@example.com",
        country: "UK",
      },
      headers: {
        "content-type": "application/json",
      },
    });

    expect(result.run.status).toBe("delivered");
    expect(deliveredBody).toMatchObject({
      payload: {
        name: "Ada",
        surname: "Lovelace",
        email: "ada@example.com",
      },
    });
    expect((deliveredBody as { payload?: Record<string, unknown> }).payload).not.toHaveProperty("country");
  });

  test("enriches payloads from inline lookup tables before delivery", async () => {
    const spec = buildHttpFlow();
    spec.processors = [
      {
        id: "processor-lookup",
        kind: "enrich_lookup",
        keyPath: "customerId",
        targetPath: "customer.risk",
        lookup: {
          mode: "inline",
          table: {
            "cust-1": {
              band: "gold",
              score: 742,
            },
          },
        },
        nextNodeIds: ["route-http"],
      },
    ];
    spec.routes = [
      {
        id: "route-http",
        fromNodeId: "processor-lookup",
        predicate: { type: "always" },
        toSinkIds: ["sink-http"],
        priority: 10,
      },
    ];
    spec.sources[0].nextNodeIds = ["processor-lookup"];

    const deployment = buildDeployment(spec, {
      sinks: {
        http_out_default: {
          kind: "http",
          connectorId: "http_out_default",
          url: "https://example.test/webhook",
        },
      },
    });

    let deliveredBody: unknown;
    const runtime = new RouterWorkerRuntime(buildRuntimeConfig(), new StaticDeploymentSource([deployment]), {
      fetchImpl: (async (input: RequestInfo | URL, init?: RequestInit) => {
        void input;
        deliveredBody = init?.body ? JSON.parse(String(init.body)) : null;
        return new Response("ok", { status: 200 });
      }) as unknown as typeof fetch,
    });

    await runtime.syncDeployments();
    const result = await runtime.ingestEnvelope({
      deploymentId: deployment.id,
      tenantId: spec.metadata.tenantId,
      flowId: spec.metadata.flowId,
      revisionId: spec.metadata.revisionId,
      messageId: "message-lookup-1",
      sourceRef: "http://localhost/events",
      partitionKey: spec.metadata.tenantId,
      payload: {
        customerId: "cust-1",
        amount: 42,
      },
      headers: {
        "content-type": "application/json",
      },
    });

    expect(result.run.status).toBe("delivered");
    expect(deliveredBody).toMatchObject({
      payload: {
        customerId: "cust-1",
        amount: 42,
        customer: {
          risk: {
            band: "gold",
            score: 742,
          },
        },
      },
    });
  });

  test("sends lookup misses to DLQ when missing mode is fail", async () => {
    const spec = buildHttpFlow();
    spec.processors = [
      {
        id: "processor-lookup",
        kind: "enrich_lookup",
        keyPath: "customerId",
        lookup: {
          mode: "inline",
          table: {},
          missing: "fail",
        },
        nextNodeIds: ["route-http"],
      },
    ];
    spec.routes = [
      {
        id: "route-http",
        fromNodeId: "processor-lookup",
        predicate: { type: "always" },
        toSinkIds: ["sink-http"],
        priority: 10,
      },
    ];
    spec.sources[0].nextNodeIds = ["processor-lookup"];

    const deployment = buildDeployment(spec, {
      sinks: {
        http_out_default: {
          kind: "http",
          connectorId: "http_out_default",
          url: "https://example.test/webhook",
        },
      },
    });

    let fetchCalls = 0;
    const runtime = new RouterWorkerRuntime(buildRuntimeConfig(), new StaticDeploymentSource([deployment]), {
      fetchImpl: (async () => {
        fetchCalls += 1;
        return new Response("ok", { status: 200 });
      }) as unknown as typeof fetch,
    });

    await runtime.syncDeployments();
    const result = await runtime.ingestEnvelope({
      deploymentId: deployment.id,
      tenantId: spec.metadata.tenantId,
      flowId: spec.metadata.flowId,
      revisionId: spec.metadata.revisionId,
      messageId: "message-lookup-miss-1",
      sourceRef: "http://localhost/events",
      partitionKey: spec.metadata.tenantId,
      payload: {
        customerId: "cust-missing",
        amount: 42,
      },
      headers: {
        "content-type": "application/json",
      },
    });

    expect(result.run.status).toBe("failed");
    expect(result.dlqRecord?.reason).toContain("lookup miss");
    expect(fetchCalls).toBe(0);
  });

  test("retries and sends to DLQ when HTTP delivery keeps failing", async () => {
    const spec = buildHttpFlow();
    let fetchCalls = 0;
    const deployment = buildDeployment(spec, {
      sinks: {
        http_out_default: {
          kind: "http",
          connectorId: "http_out_default",
          url: "https://example.test/webhook",
        },
      },
    });

    const runtime = new RouterWorkerRuntime(buildRuntimeConfig(), new StaticDeploymentSource([deployment]), {
      fetchImpl: (async () => {
        fetchCalls += 1;
        return new Response("boom", { status: 500 });
      }) as unknown as typeof fetch,
    });

    await runtime.syncDeployments();
    const result = await runtime.ingestEnvelope({
      deploymentId: deployment.id,
      tenantId: spec.metadata.tenantId,
      flowId: spec.metadata.flowId,
      revisionId: spec.metadata.revisionId,
      messageId: "message-2",
      sourceRef: "http://localhost/events",
      partitionKey: spec.metadata.tenantId,
      payload: { kind: "order", amount: 42 },
      headers: {},
    });

    expect(result.run.status).toBe("retrying");
    await new Promise((resolve) => setTimeout(resolve, 25));
    expect(fetchCalls).toBeGreaterThanOrEqual(2);
    expect(runtime.getDlq().length).toBeGreaterThanOrEqual(1);
    expect(runtime.getDlq()[0].reason).toContain("HTTP 500");
  });

  test("publishes retries durably to the retry stream before returning", async () => {
    const spec = buildHttpFlow();
    const deployment = buildDeployment(spec, {
      sinks: {
        http_out_default: {
          kind: "http",
          connectorId: "http_out_default",
          url: "https://example.test/webhook",
        },
      },
    });

    const spy = createBusSpy();
    const runtime = new RouterWorkerRuntime(buildRuntimeConfig(), new StaticDeploymentSource([deployment]), {
      messageBus: spy.bus,
      fetchImpl: (async () => new Response("boom", { status: 500 })) as unknown as typeof fetch,
    });

    await runtime.syncDeployments();
    const result = await runtime.ingestEnvelope({
      deploymentId: deployment.id,
      tenantId: spec.metadata.tenantId,
      flowId: spec.metadata.flowId,
      revisionId: spec.metadata.revisionId,
      messageId: "message-durable-retry",
      sourceRef: "http://localhost/events",
      partitionKey: spec.metadata.tenantId,
      payload: { kind: "order", amount: 42 },
      headers: {},
    });

    expect(result.run.status).toBe("retrying");

    // The retry must already be durable — no setTimeout window.
    const retryPublish = spy.jetstreamPublishes.find((entry) => entry.subject.startsWith("router.retry."));
    expect(retryPublish).toBeDefined();
    const retryEnvelope = JSON.parse(retryPublish!.data) as {
      headers: Record<string, string>;
    };
    expect(retryEnvelope.headers["x-router-attempt"]).toBe("2");
    expect(retryEnvelope.headers["x-router-not-before"]).toBeDefined();
    await runtime.stop();
  });

  test("rate limits across messages received in the same second", async () => {
    const spec = buildStatefulProcessorFlow({
      id: "processor-rate",
      kind: "rate_limit",
      perSecond: 1,
      nextNodeIds: ["route-http"],
    });
    const deployment = buildDeployment(spec, {
      sinks: {
        http_out_default: { kind: "http", connectorId: "http_out_default", url: "https://example.test/webhook" },
      },
    });
    const runtime = new RouterWorkerRuntime(buildRuntimeConfig(), new StaticDeploymentSource([deployment]), {
      fetchImpl: (async () => new Response("ok", { status: 200 })) as unknown as typeof fetch,
    });
    await runtime.syncDeployments();

    const first = await runtime.replay({
      deploymentId: deployment.id,
      envelope: buildTestEnvelope(spec, "rate-1", { kind: "order" }),
      reason: "test",
      requestedAt: "2026-07-03T10:00:01.000Z",
    });
    const second = await runtime.replay({
      deploymentId: deployment.id,
      envelope: buildTestEnvelope(spec, "rate-2", { kind: "order" }),
      reason: "test",
      requestedAt: "2026-07-03T10:00:01.000Z",
    });

    expect(first.run.status).toBe("delivered");
    expect(second.run.status).toBe("failed");
    expect(second.run.reason).toContain("rate limited by processor-rate");
    await runtime.stop();
  });

  test("dedupes across messages within the dedupe window", async () => {
    const spec = buildStatefulProcessorFlow({
      id: "processor-dedupe",
      kind: "dedupe_window",
      keyPath: "orderId",
      windowMs: 60_000,
      nextNodeIds: ["route-http"],
    });
    const deployment = buildDeployment(spec, {
      sinks: {
        http_out_default: { kind: "http", connectorId: "http_out_default", url: "https://example.test/webhook" },
      },
    });
    const runtime = new RouterWorkerRuntime(buildRuntimeConfig(), new StaticDeploymentSource([deployment]), {
      fetchImpl: (async () => new Response("ok", { status: 200 })) as unknown as typeof fetch,
    });
    await runtime.syncDeployments();

    const first = await runtime.replay({
      deploymentId: deployment.id,
      envelope: buildTestEnvelope(spec, "dedupe-1", { orderId: "o-1" }),
      reason: "test",
      requestedAt: "2026-07-03T10:00:01.000Z",
    });
    const second = await runtime.replay({
      deploymentId: deployment.id,
      envelope: buildTestEnvelope(spec, "dedupe-2", { orderId: "o-1" }),
      reason: "test",
      requestedAt: "2026-07-03T10:00:01.000Z",
    });

    expect(first.run.status).toBe("delivered");
    expect(second.run.status).toBe("deduped");
    await runtime.stop();
  });

  test("retries after sink failure are not swallowed by the dedupe window", async () => {
    const spec = buildStatefulProcessorFlow({
      id: "processor-dedupe",
      kind: "dedupe_window",
      keyPath: "orderId",
      windowMs: 60_000,
      nextNodeIds: ["route-http"],
    });
    const deployment = buildDeployment(spec, {
      sinks: {
        http_out_default: { kind: "http", connectorId: "http_out_default", url: "https://example.test/webhook" },
      },
    });

    let fetchCalls = 0;
    const runtime = new RouterWorkerRuntime(buildRuntimeConfig(), new StaticDeploymentSource([deployment]), {
      fetchImpl: (async () => {
        fetchCalls += 1;
        return fetchCalls === 1 ? new Response("boom", { status: 500 }) : new Response("ok", { status: 200 });
      }) as unknown as typeof fetch,
    });
    await runtime.syncDeployments();

    const result = await runtime.replay({
      deploymentId: deployment.id,
      envelope: buildTestEnvelope(spec, "retry-dedupe-1", { orderId: "o-retry" }),
      reason: "test",
      requestedAt: "2026-07-03T10:00:01.000Z",
    });

    expect(result.run.status).toBe("retrying");
    await new Promise((resolve) => setTimeout(resolve, 25));
    expect(fetchCalls).toBe(2);
    expect(runtime.getDlq().length).toBe(0);
    await runtime.stop();
  });

  test("masks redacted paths instead of deleting them, matching the simulator", async () => {
    const spec = buildStatefulProcessorFlow({
      id: "processor-redact",
      kind: "redact",
      paths: ["ssn"],
      nextNodeIds: ["route-http"],
    });
    const deployment = buildDeployment(spec, {
      sinks: {
        http_out_default: { kind: "http", connectorId: "http_out_default", url: "https://example.test/webhook" },
      },
    });

    const bodies: Array<{ payload: Record<string, unknown> }> = [];
    const runtime = new RouterWorkerRuntime(buildRuntimeConfig(), new StaticDeploymentSource([deployment]), {
      fetchImpl: (async (_input: RequestInfo | URL, init?: RequestInit) => {
        bodies.push(JSON.parse(String(init?.body)) as { payload: Record<string, unknown> });
        return new Response("ok", { status: 200 });
      }) as unknown as typeof fetch,
    });
    await runtime.syncDeployments();

    const result = await runtime.ingestEnvelope({
      deploymentId: deployment.id,
      tenantId: spec.metadata.tenantId,
      flowId: spec.metadata.flowId,
      revisionId: spec.metadata.revisionId,
      messageId: "message-redact",
      sourceRef: "http://localhost/events",
      partitionKey: spec.metadata.tenantId,
      payload: { name: "Ada", ssn: "123-45-6789" },
      headers: {},
    });

    expect(result.run.status).toBe("delivered");
    expect(bodies[0]?.payload.ssn).toBe("[redacted]");
    expect(bodies[0]?.payload.name).toBe("Ada");
    await runtime.stop();
  });

  test("rate-limited buckets do not swallow retries of admitted messages", async () => {
    const spec = buildStatefulProcessorFlow({
      id: "processor-rate",
      kind: "rate_limit",
      perSecond: 1,
      nextNodeIds: ["route-http"],
    });
    const deployment = buildDeployment(spec, {
      sinks: {
        http_out_default: { kind: "http", connectorId: "http_out_default", url: "https://example.test/webhook" },
      },
    });

    let fetchCalls = 0;
    const runtime = new RouterWorkerRuntime(buildRuntimeConfig(), new StaticDeploymentSource([deployment]), {
      fetchImpl: (async () => {
        fetchCalls += 1;
        return fetchCalls === 1 ? new Response("boom", { status: 500 }) : new Response("ok", { status: 200 });
      }) as unknown as typeof fetch,
    });
    await runtime.syncDeployments();

    const result = await runtime.replay({
      deploymentId: deployment.id,
      envelope: buildTestEnvelope(spec, "retry-rate-1", { kind: "order" }),
      reason: "test",
      requestedAt: "2026-07-03T10:00:01.000Z",
    });

    expect(result.run.status).toBe("retrying");
    await new Promise((resolve) => setTimeout(resolve, 25));
    expect(fetchCalls).toBe(2);
    expect(runtime.getDlq().length).toBe(0);
    await runtime.stop();
  });

  test("replaying the same message id passes the dedupe window", async () => {
    const spec = buildStatefulProcessorFlow({
      id: "processor-dedupe",
      kind: "dedupe_window",
      keyPath: "orderId",
      windowMs: 60_000,
      nextNodeIds: ["route-http"],
    });
    const deployment = buildDeployment(spec, {
      sinks: {
        http_out_default: { kind: "http", connectorId: "http_out_default", url: "https://example.test/webhook" },
      },
    });
    let fetchCalls = 0;
    const runtime = new RouterWorkerRuntime(buildRuntimeConfig(), new StaticDeploymentSource([deployment]), {
      fetchImpl: (async () => {
        fetchCalls += 1;
        return new Response("ok", { status: 200 });
      }) as unknown as typeof fetch,
    });
    await runtime.syncDeployments();

    const envelope = buildTestEnvelope(spec, "replay-same-id", { orderId: "o-replay" });
    const first = await runtime.replay({ deploymentId: deployment.id, envelope, reason: "test", requestedAt: "2026-07-03T10:00:01.000Z" });
    const second = await runtime.replay({ deploymentId: deployment.id, envelope, reason: "replay", requestedAt: "2026-07-03T10:00:02.000Z" });

    expect(first.run.status).toBe("delivered");
    expect(second.run.status).toBe("delivered");
    expect(fetchCalls).toBe(2);
    await runtime.stop();
  });

  test("publishes to native NATS sinks and JetStream ingress subjects", async () => {
    const spec = buildNatsFlow();
    const { bus, publishes, corePublishes, jetstreamPublishes } = createBusSpy();
    const deployment = buildDeployment(spec, {
      sinks: {
        nats_out_default: {
          kind: "nats",
          connectorId: "nats_out_default",
          subject: "orders.processed",
        },
      },
    });

    const runtime = new RouterWorkerRuntime(buildRuntimeConfig(), new StaticDeploymentSource([deployment]), {
      messageBus: bus,
    });

    await runtime.syncDeployments();
    const published = await runtime.enqueueIngress({
      deploymentId: deployment.id,
      tenantId: spec.metadata.tenantId,
      flowId: spec.metadata.flowId,
      revisionId: spec.metadata.revisionId,
      messageId: "message-3",
      sourceRef: "http://localhost/events",
      partitionKey: spec.metadata.tenantId,
      payload: { kind: "order", amount: 100 },
      headers: {},
    });

    expect(published.subject).toBe(
      "router.ingress.tenant-b.flow-nats.rev-nats-v1.message-3",
    );

    const processed = await runtime.ingestEnvelope({
      deploymentId: deployment.id,
      tenantId: spec.metadata.tenantId,
      flowId: spec.metadata.flowId,
      revisionId: spec.metadata.revisionId,
      messageId: "message-4",
      sourceRef: "http://localhost/events",
      partitionKey: spec.metadata.tenantId,
      payload: { kind: "order", amount: 100 },
      headers: {},
    });

    expect(processed.run.status).toBe("delivered");
    expect(publishes.some((entry) => entry.subject === "orders.processed")).toBe(true);
    expect(publishes.some((entry) => entry.subject.startsWith("router.ingress."))).toBe(true);
    expect(corePublishes.some((entry) => entry.subject === "orders.processed")).toBe(true);
    expect(jetstreamPublishes.some((entry) => entry.subject === "orders.processed")).toBe(false);
    expect(jetstreamPublishes.some((entry) => entry.subject.startsWith("router.ingress."))).toBe(true);
  });

  test("rejects HTTP-style ingress when the bounded admission queue is full", async () => {
    const spec = buildNatsFlow();
    const { bus } = createBusSpy();
    const deployment = buildDeployment(spec, {
      sinks: {
        nats_out_default: {
          kind: "nats",
          connectorId: "nats_out_default",
          subject: "orders.processed",
        },
      },
    });

    const runtime = new RouterWorkerRuntime(
      {
        ...buildRuntimeConfig(),
        ingressMaxBufferedPerDeployment: 1,
        ingressMaxBufferedTotal: 1,
        ingressRetryAfterMs: 500,
      },
      new StaticDeploymentSource([deployment]),
      {
        messageBus: bus,
      },
    );

    await runtime.syncDeployments();

    await runtime.enqueueIngress({
      deploymentId: deployment.id,
      tenantId: spec.metadata.tenantId,
      flowId: spec.metadata.flowId,
      revisionId: spec.metadata.revisionId,
      messageId: "message-queued",
      sourceRef: "http://localhost/events",
      partitionKey: spec.metadata.tenantId,
      payload: { kind: "order", amount: 10 },
      headers: {},
    });

    let rejection: unknown;
    try {
      await runtime.enqueueIngress(
        {
          deploymentId: deployment.id,
          tenantId: spec.metadata.tenantId,
          flowId: spec.metadata.flowId,
          revisionId: spec.metadata.revisionId,
          messageId: "message-rejected",
          sourceRef: "http://localhost/events",
          partitionKey: spec.metadata.tenantId,
          payload: { kind: "order", amount: 11 },
          headers: {},
        },
        { admissionMode: "reject" },
      );
    } catch (error) {
      rejection = error;
    }

    expect(rejection).toBeInstanceOf(IngressOverloadedError);
    expect((rejection as IngressOverloadedError).admission).toMatchObject({
      allowed: false,
      scope: "deployment",
      reason: "deployment_backpressure",
      limitForDeployment: 1,
      limitTotal: 1,
      retryAfterMs: 500,
    });
  });

  test("keeps JetStream subscriptions stable across deployment id churn", async () => {
    const spec = buildHttpFlow();
    const { bus, subscriptions } = createBusSpy();
    const firstDeployment = buildDeployment(spec, {
      sinks: {
        http_out_default: {
          kind: "http",
          connectorId: "http_out_default",
          url: "https://example.test/webhook",
        },
      },
    });
    firstDeployment.id = "deploy-old";

    const nextDeployment = buildDeployment(spec, {
      sinks: {
        http_out_default: {
          kind: "http",
          connectorId: "http_out_default",
          url: "https://example.test/webhook",
        },
      },
    });
    nextDeployment.id = "deploy-new";

    let activeDeployments = [firstDeployment];
    const runtime = new RouterWorkerRuntime(
      buildRuntimeConfig(),
      {
        async loadDeployments() {
          return activeDeployments;
        },
      },
      {
        messageBus: bus,
        fetchImpl: (async () => new Response("ok", { status: 200 })) as unknown as typeof fetch,
      },
    );

    await runtime.syncDeployments();

    const jetstreamSubscriptions = subscriptions.filter((entry) => entry.kind === "jetstream");
    expect(jetstreamSubscriptions).toHaveLength(3);
    expect(runtime.getSummary().activeSubjects).toEqual([
      `js:${firstDeployment.sourceSubjects[0]}`,
      `js:${firstDeployment.sourceSubjects[1]}`,
      `js:${firstDeployment.sourceSubjects[2]}`,
    ]);
    expect(jetstreamSubscriptions[0]?.durableName).toBe(firstDeployment.sourceSubjects[0]);

    activeDeployments = [nextDeployment];
    await runtime.syncDeployments();

    expect(subscriptions.filter((entry) => entry.kind === "jetstream")).toHaveLength(3);
    expect(subscriptions.some((entry) => entry.kind === "jetstream" && entry.unsubscribed)).toBe(false);

    const ingressSubscription = subscriptions.find(
      (entry) => entry.kind === "jetstream" && entry.subject === firstDeployment.sourceSubjects[0],
    );
    expect(ingressSubscription).toBeDefined();

    await ingressSubscription?.handler(
      encodeJsonMessage({
        tenantId: spec.metadata.tenantId,
        flowId: spec.metadata.flowId,
        revisionId: spec.metadata.revisionId,
        messageId: "message-reseed-1",
        sourceRef: "http://localhost/events",
        partitionKey: spec.metadata.tenantId,
        headers: {},
        payload: { kind: "order", amount: 5 },
        receivedAt: new Date().toISOString(),
        traceId: "trace-reseed-1",
      }),
      { subject: firstDeployment.sourceSubjects[0], sequence: 1 },
    );

    expect(runtime.getRuns()).toHaveLength(1);
    expect(runtime.getRuns()[0]?.deploymentId).toBe("deploy-new");

    await runtime.stop();
  });

  test("hands adapter sinks off to the work stream", async () => {
    const spec = buildAdapterFlow();
    const { bus, publishes } = createBusSpy();
    const deployment = buildDeployment(spec, {
      sinks: {},
    });
    deployment.connectors = {
      kafka_out_default: {
        id: "kafka_out_default",
        capabilityId: "kafka_out",
        executionMode: "adapter",
        config: {
          topic: "orders.created",
        },
      },
    };

    const runtime = new RouterWorkerRuntime(buildRuntimeConfig(), new StaticDeploymentSource([deployment]), {
      messageBus: bus,
    });

    await runtime.syncDeployments();
    const processed = await runtime.ingestEnvelope({
      deploymentId: deployment.id,
      tenantId: spec.metadata.tenantId,
      flowId: spec.metadata.flowId,
      revisionId: spec.metadata.revisionId,
      messageId: "message-5",
      sourceRef: "http://localhost/events",
      partitionKey: spec.metadata.tenantId,
      payload: { kind: "order", amount: 100 },
      headers: {},
    });

    expect(processed.run.status).toBe("enqueued");
    expect(processed.run.awaitedSinkIds).toEqual(["sink-kafka"]);
    const handoff = publishes.find((entry) => entry.subject.startsWith("router.work."));
    expect(handoff).toBeDefined();
    expect(handoff?.subject).toStartWith("router.work.inline.");
    expect(handoff?.data).toContain("\"capabilityId\":\"kafka_out\"");
    expect(handoff?.data).toContain("\"topic\":\"orders.created\"");
    expect(handoff?.data).toContain(`"runId":"${processed.run.runId}"`);
  });

  test("routes connect-managed adapter sinks onto connector-qualified work subjects", async () => {
    const spec = buildAdapterFlow({
      sinkKind: "s3",
      sinkId: "sink-s3",
      routeId: "route-s3",
      sinkCapabilityId: "s3_sink",
      sinkConnectorId: "s3_sink_default",
      flowId: "flow-s3",
      revisionId: "rev-s3-v1",
      name: "S3 Adapter Flow",
      description: "Connect-managed adapter sink handoff",
    });
    const deployment = buildDeployment(spec, {
      sinks: {
        s3_sink_default: {
          kind: "adapter",
          connectorId: "s3_sink_default",
          capabilityId: "s3_sink",
          workStream: "work",
        },
      },
    });
    deployment.connectors = {
      s3_sink_default: {
        id: "s3_sink_default",
        capabilityId: "s3_sink",
        executionMode: "adapter",
        config: {
          bucket: "event-router-v1",
          prefix: "exports/",
        },
      },
    };

    const { bus, publishes } = createBusSpy();
    const runtime = new RouterWorkerRuntime(buildRuntimeConfig(), new StaticDeploymentSource([deployment]), {
      messageBus: bus,
    });

    await runtime.syncDeployments();
    const processed = await runtime.ingestEnvelope({
      deploymentId: deployment.id,
      tenantId: spec.metadata.tenantId,
      flowId: spec.metadata.flowId,
      revisionId: spec.metadata.revisionId,
      messageId: "message-s3-1",
      sourceRef: "http://localhost/events",
      partitionKey: spec.metadata.tenantId,
      payload: { kind: "profile", city: "Prague" },
      headers: {},
    });

    expect(processed.run.status).toBe("delivered");
    const handoff = publishes.find((entry) => entry.subject.startsWith("router.work.connect."));
    expect(handoff).toBeDefined();
    expect(handoff?.subject).toStartWith("router.work.connect.s3_sink_default.");
    expect(handoff?.data).toContain("\"capabilityId\":\"s3_sink\"");
  });

  test("caps retained history while preserving total counters", async () => {
    const spec = buildHttpFlow();
    const deployment = buildDeployment(spec, {
      sinks: {
        http_out_default: {
          kind: "http",
          connectorId: "http_out_default",
          url: "https://example.test/webhook",
        },
      },
    });

    const runtime = new RouterWorkerRuntime(buildRuntimeConfig(), new StaticDeploymentSource([deployment]), {
      fetchImpl: (async () => new Response("ok", { status: 200 })) as unknown as typeof fetch,
    });

    await runtime.syncDeployments();

    for (let index = 0; index < 5; index += 1) {
      await runtime.ingestEnvelope({
        deploymentId: deployment.id,
        tenantId: spec.metadata.tenantId,
        flowId: spec.metadata.flowId,
        revisionId: spec.metadata.revisionId,
        messageId: `message-history-${index}`,
        sourceRef: "http://localhost/events",
        partitionKey: spec.metadata.tenantId,
        payload: { kind: "order", amount: index },
        headers: {},
      });
    }

    expect(runtime.getRuns()).toHaveLength(3);
    expect(runtime.getRuns().map((run) => run.messageId)).toEqual([
      "message-history-2",
      "message-history-3",
      "message-history-4",
    ]);
    expect(runtime.getSummary().runs).toBe(5);
  });

  test("batches HTTP sink deliveries when batchingPolicy is enabled", async () => {
    const spec = buildHttpFlow();
    // Long interval on purpose (capped at the publish-time validation max):
    // only the batch-size trigger may flush. If the two ingests accidentally
    // serialize, this test times out instead of passing flakily with two
    // single-message requests.
    spec.batchingPolicy = { enabled: true, batchSize: 2, flushIntervalMs: 5_000 };
    const deployment = buildDeployment(spec, {
      sinks: {
        http_out_default: { kind: "http", connectorId: "http_out_default", url: "https://example.test/webhook" },
      },
    });

    const requests: Array<{ messages: unknown[] }> = [];
    const runtime = new RouterWorkerRuntime(buildRuntimeConfig(), new StaticDeploymentSource([deployment]), {
      fetchImpl: (async (_input: RequestInfo | URL, init?: RequestInit) => {
        requests.push(JSON.parse(String(init?.body)) as { messages: unknown[] });
        return new Response("ok", { status: 200 });
      }) as unknown as typeof fetch,
    });
    await runtime.syncDeployments();

    const ingest = (messageId: string) =>
      runtime.ingestEnvelope({
        deploymentId: deployment.id,
        tenantId: spec.metadata.tenantId,
        flowId: spec.metadata.flowId,
        revisionId: spec.metadata.revisionId,
        messageId,
        sourceRef: "http://localhost/events",
        partitionKey: spec.metadata.tenantId,
        payload: { messageId },
        headers: {},
      });

    const [first, second] = await Promise.all([ingest("batch-1"), ingest("batch-2")]);

    expect(first.run.status).toBe("delivered");
    expect(second.run.status).toBe("delivered");
    expect(requests.length).toBe(1);
    expect(requests[0].messages.length).toBe(2);
    await runtime.stop();
  });

  test("retries a failed batch and clears the DLQ without swallowing deliverable messages", async () => {
    const spec = buildHttpFlow();
    // Long interval on purpose (capped at the publish-time validation max):
    // only the batch-size trigger may flush the first batch.
    spec.batchingPolicy = { enabled: true, batchSize: 2, flushIntervalMs: 5_000 };
    const deployment = buildDeployment(spec, {
      sinks: {
        http_out_default: { kind: "http", connectorId: "http_out_default", url: "https://example.test/webhook" },
      },
    });

    const requests: Array<{ messages: unknown[] }> = [];
    let fetchCalls = 0;
    const runtime = new RouterWorkerRuntime(buildRuntimeConfig(), new StaticDeploymentSource([deployment]), {
      fetchImpl: (async (_input: RequestInfo | URL, init?: RequestInit) => {
        fetchCalls += 1;
        const body = JSON.parse(String(init?.body)) as { messages: unknown[] };
        requests.push(body);
        return fetchCalls === 1 ? new Response("boom", { status: 500 }) : new Response("ok", { status: 200 });
      }) as unknown as typeof fetch,
    });
    await runtime.syncDeployments();

    const ingest = (messageId: string) =>
      runtime.ingestEnvelope({
        deploymentId: deployment.id,
        tenantId: spec.metadata.tenantId,
        flowId: spec.metadata.flowId,
        revisionId: spec.metadata.revisionId,
        messageId,
        sourceRef: "http://localhost/events",
        partitionKey: spec.metadata.tenantId,
        payload: { messageId },
        headers: {},
      });

    const [first, second] = await Promise.all([ingest("batch-fail-1"), ingest("batch-fail-2")]);

    expect(first.run.status).toBe("retrying");
    expect(second.run.status).toBe("retrying");

    await new Promise((resolve) => setTimeout(resolve, 50));

    expect(runtime.getDlq().length).toBe(0);
    expect(requests.length).toBeGreaterThanOrEqual(2);
    expect(requests[0].messages.length).toBe(2);
    for (const request of requests) {
      expect(request.messages.length).toBeGreaterThan(0);
    }
    await runtime.stop();
  });

  test("flushes aggregated runtime stats asynchronously to the control plane", async () => {
    const spec = buildHttpFlow();
    const deployment = buildDeployment(spec, {
      sinks: {
        http_out_default: {
          kind: "http",
          connectorId: "http_out_default",
          url: "https://example.test/webhook",
        },
      },
    });
    const reported: Array<Array<{ deploymentId: string; acceptedCount: number; deliveredCount: number; backlogCount: number }>> = [];

    const runtime = new RouterWorkerRuntime(buildRuntimeConfig(), new StaticDeploymentSource([deployment]), {
      fetchImpl: (async () => new Response("ok", { status: 200 })) as unknown as typeof fetch,
      controlApiClient: {
        deploymentSource: new ControlApiDeploymentSource({
          controlApiUrl: "https://control-api.test",
        }),
        async reportRuntimeStats(stats) {
          reported.push(
            stats.map((entry) => ({
              deploymentId: entry.deploymentId,
              acceptedCount: entry.acceptedCount,
              deliveredCount: entry.deliveredCount,
              backlogCount: entry.backlogCount,
            })),
          );
        },
        async appendRunSummary() {},
        async flushRunSummaries() {},
        async appendRuntimeSample() {},
        async flushRuntimeSamples() {},
        async appendAudit() {},
        async listPendingReplayRequests() {
          return [];
        },
        async claimReplayRequest() {
          return null;
        },
        async completeReplayRequest() {},
      },
    });

    await runtime.start();
    await runtime.syncDeployments();
    await runtime.ingestEnvelope({
      deploymentId: deployment.id,
      tenantId: spec.metadata.tenantId,
      flowId: spec.metadata.flowId,
      revisionId: spec.metadata.revisionId,
      messageId: "message-flush-1",
      sourceRef: "http://localhost/events",
      partitionKey: spec.metadata.tenantId,
      payload: { kind: "order", amount: 1 },
      headers: {},
    });

    await new Promise((resolve) => setTimeout(resolve, 20));
    await runtime.stop();

    expect(reported.length).toBeGreaterThan(0);
    expect(reported.flat()).toContainEqual(
      expect.objectContaining({
        deploymentId: deployment.id,
        acceptedCount: 1,
        deliveredCount: 1,
        backlogCount: 0,
      }),
    );
  });

  test("marks invalid deployments degraded instead of throwing on sync", async () => {
    const audits: Array<{ action: string; subjectId: string }> = [];
    const source = {
      async loadDeployments() {
        return [] as RouterDeployment[];
      },
      getLoadErrors() {
        return [
          {
            deploymentId: "deploy-invalid",
            tenantId: "tenant-demo",
            flowId: "flow-invalid",
            revisionId: "rev-invalid-v1",
            reason: "FlowSpec validation failed",
            issues: [
              {
                path: "retryPolicy.maxAttempts",
                message: "retries are only allowed for idempotent sinks because repeated writes are unsafe",
              },
            ],
          },
        ];
      },
    };

    const runtime = new RouterWorkerRuntime(buildRuntimeConfig(), source, {
      controlApiClient: {
        deploymentSource: new ControlApiDeploymentSource({
          controlApiUrl: "https://control-api.test",
        }),
        async reportRuntimeStats() {},
        async appendRunSummary() {},
        async flushRunSummaries() {},
        async appendRuntimeSample() {},
        async flushRuntimeSamples() {},
        async appendAudit(input) {
          audits.push({ action: input.action, subjectId: input.subjectId });
        },
        async listPendingReplayRequests() {
          return [];
        },
        async claimReplayRequest() {
          return null;
        },
        async completeReplayRequest() {},
      },
    });

    await runtime.syncDeployments();

    expect(audits).toEqual([
      {
        action: "deployment.validation_failed",
        subjectId: "deploy-invalid",
      },
    ]);
  });

  test("reports delivered and failing runs to the control plane", async () => {
    const summaries: Array<{ status: string; traceId: string; errorCount: number; lastError: string | null | undefined }> = [];
    const audits: Array<{ action: string; subjectId: string }> = [];
    const spec = buildHttpFlow();
    spec.retryPolicy.maxAttempts = 1;
    const deployment = buildDeployment(spec, {
      sinks: {
        http_out_default: {
          kind: "http",
          connectorId: "http_out_default",
          url: "https://example.test/webhook",
        },
      },
    });

    let shouldFail = false;
    const runtime = new RouterWorkerRuntime(buildRuntimeConfig(), new StaticDeploymentSource([deployment]), {
      fetchImpl: (async () =>
        shouldFail ? new Response("boom", { status: 503 }) : new Response("ok", { status: 200 })) as unknown as typeof fetch,
      controlApiClient: {
        deploymentSource: new ControlApiDeploymentSource({
          controlApiUrl: "https://control-api.test",
        }),
        async reportRuntimeStats() {},
        async appendRunSummary(input) {
          summaries.push({
            status: input.status,
            traceId: input.traceId,
            errorCount: input.errorCount,
            lastError: input.lastError,
          });
        },
        async flushRunSummaries() {},
        async appendRuntimeSample() {},
        async flushRuntimeSamples() {},
        async appendAudit(input) {
          audits.push({ action: input.action, subjectId: input.subjectId });
        },
        async listPendingReplayRequests() {
          return [];
        },
        async claimReplayRequest() {
          return null;
        },
        async completeReplayRequest() {},
      },
    });

    await runtime.start();
    await runtime.syncDeployments();

    await runtime.ingestEnvelope({
      deploymentId: deployment.id,
      tenantId: spec.metadata.tenantId,
      flowId: spec.metadata.flowId,
      revisionId: spec.metadata.revisionId,
      messageId: "message-report-success",
      sourceRef: "http://localhost/events",
      partitionKey: spec.metadata.tenantId,
      payload: { kind: "order", amount: 1 },
      headers: {},
      traceId: "trace-report-success",
    });

    shouldFail = true;

    await runtime.ingestEnvelope({
      deploymentId: deployment.id,
      tenantId: spec.metadata.tenantId,
      flowId: spec.metadata.flowId,
      revisionId: spec.metadata.revisionId,
      messageId: "message-report-failure",
      sourceRef: "http://localhost/events",
      partitionKey: spec.metadata.tenantId,
      payload: { kind: "order", amount: 2 },
      headers: {},
      traceId: "trace-report-failure",
    });

    await new Promise((resolve) => setTimeout(resolve, 0));
    await runtime.stop();

    expect(summaries).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          status: "succeeded",
          traceId: "trace-report-success",
          errorCount: 0,
          lastError: null,
        }),
        expect.objectContaining({
          status: "dlq",
          traceId: "trace-report-failure",
          errorCount: 1,
          lastError: "HTTP 503",
        }),
      ]),
    );
    expect(audits).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          action: "run.dlq",
        }),
      ]),
    );
  });

  test("reports inline adapter handoff as enqueued to the control plane", async () => {
    const summaries: Array<{
      status: string;
      traceId: string;
      messageId?: string;
      targetSinkIds?: string[];
      awaitedSinkIds?: string[];
    }> = [];
    const spec = buildAdapterFlow();
    const deployment = buildDeployment(spec, {
      sinks: {
        kafka_out_default: {
          kind: "adapter",
          connectorId: "kafka_out_default",
          capabilityId: "kafka_out",
          workStream: "work",
        },
      },
    });
    deployment.connectors = {
      kafka_out_default: {
        id: "kafka_out_default",
        capabilityId: "kafka_out",
        executionMode: "adapter",
        config: {
          topic: "orders.created",
        },
      },
    };

    const { bus } = createBusSpy();
    const runtime = new RouterWorkerRuntime(buildRuntimeConfig(), new StaticDeploymentSource([deployment]), {
      messageBus: bus,
      controlApiClient: {
        deploymentSource: new ControlApiDeploymentSource({
          controlApiUrl: "https://control-api.test",
        }),
        async reportRuntimeStats() {},
        async appendRunSummary(input) {
          summaries.push({
            status: input.status,
            traceId: input.traceId,
            messageId: input.messageId,
            targetSinkIds: input.targetSinkIds,
            awaitedSinkIds: input.awaitedSinkIds,
          });
        },
        async flushRunSummaries() {},
        async appendRuntimeSample() {},
        async flushRuntimeSamples() {},
        async appendAudit() {},
        async listPendingReplayRequests() {
          return [];
        },
        async claimReplayRequest() {
          return null;
        },
        async completeReplayRequest() {},
      },
    });

    await runtime.start();
    await runtime.syncDeployments();

    const processed = await runtime.ingestEnvelope({
      deploymentId: deployment.id,
      tenantId: spec.metadata.tenantId,
      flowId: spec.metadata.flowId,
      revisionId: spec.metadata.revisionId,
      messageId: "message-report-enqueued",
      sourceRef: "http://localhost/events",
      partitionKey: spec.metadata.tenantId,
      payload: { kind: "order", amount: 7 },
      headers: {},
      traceId: "trace-report-enqueued",
    });

    await new Promise((resolve) => setTimeout(resolve, 0));
    await runtime.stop();

    expect(processed.run.status).toBe("enqueued");
    expect(summaries).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          status: "enqueued",
          traceId: "trace-report-enqueued",
          messageId: "message-report-enqueued",
          targetSinkIds: ["sink-kafka"],
          awaitedSinkIds: ["sink-kafka"],
        }),
      ]),
    );
  });

  test("captures a bounded live sample at ingress without sampling every message", async () => {
    const runtimeSamples: Array<{ sourceRef: string; payload: unknown }> = [];
    const spec = buildHttpFlow();
    const deployment = buildDeployment(spec, {
      sinks: {
        http_out_default: {
          kind: "http",
          connectorId: "http_out_default",
          url: "https://example.test/webhook",
        },
      },
    });

    const runtime = new RouterWorkerRuntime(
      {
        ...buildRuntimeConfig(),
        runtimeSampleCaptureIntervalMs: 60_000,
        runtimeSampleMaxPayloadBytes: 512,
      },
      new StaticDeploymentSource([deployment]),
      {
        fetchImpl: (async () => new Response("ok", { status: 200 })) as unknown as typeof fetch,
        controlApiClient: {
          deploymentSource: new ControlApiDeploymentSource({
            controlApiUrl: "https://control-api.test",
          }),
          async reportRuntimeStats() {},
          async appendRunSummary() {},
          async flushRunSummaries() {},
          async appendRuntimeSample(input) {
            runtimeSamples.push({
              sourceRef: input.sourceRef,
              payload: input.payload,
            });
          },
          async flushRuntimeSamples() {},
          async appendAudit() {},
          async listPendingReplayRequests() {
            return [];
          },
          async claimReplayRequest() {
            return null;
          },
          async completeReplayRequest() {},
        },
      },
    );

    await runtime.start();
    await runtime.syncDeployments();

    await runtime.ingestEnvelope({
      deploymentId: deployment.id,
      tenantId: spec.metadata.tenantId,
      flowId: spec.metadata.flowId,
      revisionId: spec.metadata.revisionId,
      messageId: "message-sample-1",
      sourceRef: "http-ingress",
      partitionKey: spec.metadata.tenantId,
      payload: { customerId: "cus-1", country: "DE" },
    });

    await runtime.ingestEnvelope({
      deploymentId: deployment.id,
      tenantId: spec.metadata.tenantId,
      flowId: spec.metadata.flowId,
      revisionId: spec.metadata.revisionId,
      messageId: "message-sample-2",
      sourceRef: "http-ingress",
      partitionKey: spec.metadata.tenantId,
      payload: { customerId: "cus-2", country: "PL" },
    });

    expect(runtimeSamples).toEqual([
      {
        sourceRef: "http-ingress",
        payload: { customerId: "cus-1", country: "DE" },
      },
    ]);
  });
});
