import { describe, expect, test } from "bun:test";
import { HttpSinkBatcher } from "../http-sink-batcher";
import type { DeliveryPayload, HttpSinkTarget } from "../phase2-types";

const target: HttpSinkTarget = {
  kind: "http",
  connectorId: "http_out_default",
  url: "https://example.test/webhook",
};

function buildPayload(messageId: string): DeliveryPayload {
  return {
    envelope: {
      tenantId: "tenant-a",
      flowId: "flow-http",
      revisionId: "rev-http-v1",
      messageId,
      sourceRef: "test",
      partitionKey: "tenant-a",
      headers: {},
      payload: { messageId },
      receivedAt: "2026-07-03T10:00:00.000Z",
      traceId: messageId,
    },
    payload: { messageId },
    deploymentId: "deployment-1",
    sinkId: "sink-http",
    flowId: "flow-http",
    revisionId: "rev-http-v1",
  };
}

describe("HttpSinkBatcher", () => {
  test("flushes one request once batchSize is reached", async () => {
    const requests: Array<{ messages: unknown[] }> = [];
    const batcher = new HttpSinkBatcher(target, 2, 10_000, (async (_input: RequestInfo | URL, init?: RequestInit) => {
      requests.push(JSON.parse(String(init?.body)) as { messages: unknown[] });
      return new Response("ok", { status: 200 });
    }) as unknown as typeof fetch);

    const [first, second] = await Promise.all([
      batcher.enqueue(buildPayload("m-1")),
      batcher.enqueue(buildPayload("m-2")),
    ]);

    expect(requests.length).toBe(1);
    expect(requests[0].messages.length).toBe(2);
    expect(first.ok).toBe(true);
    expect(second.ok).toBe(true);
  });

  test("flushes a partial batch when the interval elapses", async () => {
    const requests: Array<{ messages: unknown[] }> = [];
    const batcher = new HttpSinkBatcher(target, 10, 20, (async (_input: RequestInfo | URL, init?: RequestInit) => {
      requests.push(JSON.parse(String(init?.body)) as { messages: unknown[] });
      return new Response("ok", { status: 200 });
    }) as unknown as typeof fetch);

    const response = await batcher.enqueue(buildPayload("m-solo"));

    expect(requests.length).toBe(1);
    expect(requests[0].messages.length).toBe(1);
    expect(response.ok).toBe(true);
  });

  test("resolves every member with the failure when the batch request fails", async () => {
    const batcher = new HttpSinkBatcher(target, 2, 10_000, (async () =>
      new Response("boom", { status: 502 })) as unknown as typeof fetch);

    const [first, second] = await Promise.all([
      batcher.enqueue(buildPayload("m-1")),
      batcher.enqueue(buildPayload("m-2")),
    ]);

    expect(first.ok).toBe(false);
    expect(first.error).toBe("HTTP 502");
    expect(second.ok).toBe(false);
  });
});
