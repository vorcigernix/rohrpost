import { describe, expect, test } from "bun:test";
import {
  createRouterControlApiClient,
  type RuntimeRunSummaryInput,
  type RuntimeSampleInput,
} from "../control-api";

function buildRunSummary(traceId: string): RuntimeRunSummaryInput {
  return {
    flowId: "flow-demo",
    revisionId: "rev-demo-v1",
    deploymentId: "deploy-demo",
    status: "succeeded",
    sourceRef: "events.source.nats",
    traceId,
    processedCount: 1,
    errorCount: 0,
    startedAt: "2026-04-02T00:00:00.000Z",
    finishedAt: "2026-04-02T00:00:00.100Z",
    lastError: null,
  };
}

function buildRuntimeSample(sourceRef: string): RuntimeSampleInput {
  return {
    deploymentId: "deploy-demo",
    flowId: "flow-demo",
    revisionId: "rev-demo-v1",
    sourceKind: "http",
    sourceRef,
    payload: {
      orderId: "ord-1",
      amount: 42,
    },
    observedAt: "2026-04-03T10:00:00.000Z",
  };
}

describe("createRouterControlApiClient", () => {
  test("batches run summaries into a single control-api request", async () => {
    const requests: Array<{ url: string; method: string; body: unknown }> = [];
    const client = createRouterControlApiClient({
      controlApiUrl: "https://control-api.test",
      controlApiToken: "token",
      fetchImpl: (async (input: RequestInfo | URL, init?: RequestInit) => {
        requests.push({
          url: String(input),
          method: init?.method ?? "GET",
          body: init?.body ? JSON.parse(String(init.body)) : null,
        });

        return new Response(JSON.stringify({ inserted: 1, deployments: [], generatedAt: new Date().toISOString() }), {
          status: 200,
          headers: {
            "content-type": "application/json",
          },
        });
      }) as unknown as typeof fetch,
    });

    await client.appendRunSummary(buildRunSummary("trace-1"));
    await client.appendRunSummary(buildRunSummary("trace-2"));
    await client.appendRunSummary(buildRunSummary("trace-3"));
    await client.flushRunSummaries();

    const batchRequests = requests.filter((request) => request.url.endsWith("/api/runtime/runs/batch"));

    expect(batchRequests).toHaveLength(1);
    expect(batchRequests[0]?.method).toBe("POST");
    expect(batchRequests[0]?.body).toEqual([
      buildRunSummary("trace-1"),
      buildRunSummary("trace-2"),
      buildRunSummary("trace-3"),
    ]);
  });

  test("upserts runtime samples into a single control-api request", async () => {
    const requests: Array<{ url: string; method: string; body: unknown }> = [];
    const client = createRouterControlApiClient({
      controlApiUrl: "https://control-api.test",
      controlApiToken: "token",
      fetchImpl: (async (input: RequestInfo | URL, init?: RequestInit) => {
        requests.push({
          url: String(input),
          method: init?.method ?? "GET",
          body: init?.body ? JSON.parse(String(init.body)) : null,
        });

        return new Response(JSON.stringify({ updated: 1 }), {
          status: 200,
          headers: {
            "content-type": "application/json",
          },
        });
      }) as unknown as typeof fetch,
    });

    await client.appendRuntimeSample(buildRuntimeSample("/ingest/alpha"));
    await client.appendRuntimeSample(buildRuntimeSample("/ingest/alpha"));
    await client.appendRuntimeSample(buildRuntimeSample("/ingest/beta"));
    await client.flushRuntimeSamples();

    const sampleRequests = requests.filter((request) => request.url.endsWith("/api/runtime/samples"));

    expect(sampleRequests).toHaveLength(1);
    expect(sampleRequests[0]?.method).toBe("POST");
    expect(sampleRequests[0]?.body).toEqual([
      buildRuntimeSample("/ingest/alpha"),
      buildRuntimeSample("/ingest/beta"),
    ]);
  });
});
