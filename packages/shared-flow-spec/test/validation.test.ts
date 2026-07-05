import { describe, expect, test } from "bun:test";
import { FLOW_SPEC_VERSION, validateFlowSpec } from "../src";
import type { FlowSpec } from "../src";

const baseSpec: FlowSpec = {
  version: FLOW_SPEC_VERSION,
  metadata: {
    tenantId: "tenant-1",
    flowId: "flow-1",
    revisionId: "rev-1",
    name: "Test flow",
  },
  sources: [
    {
      id: "source-1",
      kind: "http",
      connector: {
        capabilityId: "cap-http-in",
        connectorId: "http-in-1",
        executionMode: "native",
      },
      stream: "ingress",
      nextNodeIds: ["processor-1"],
    },
  ],
  processors: [
    {
      id: "processor-1",
      kind: "filter",
      predicate: { type: "field_exists", path: "customerId" },
      nextNodeIds: ["sink-1"],
    },
  ],
  routes: [
    {
      id: "route-1",
      fromNodeId: "processor-1",
      predicate: { type: "always" },
      toSinkIds: ["sink-1"],
    },
  ],
  sinks: [
    {
      id: "sink-1",
      kind: "nats",
      connector: {
        capabilityId: "cap-nats-out",
        connectorId: "nats-out-1",
        executionMode: "native",
      },
      deliveryGuarantee: "idempotent",
      stream: "work",
    },
  ],
  retryPolicy: {
    maxAttempts: 3,
    initialBackoffMs: 100,
    maxBackoffMs: 5000,
    multiplier: 2,
  },
  dlqPolicy: {
    enabled: true,
    sinkId: "sink-1",
  },
  idempotencyStrategy: "partition_key",
};

describe("validateFlowSpec", () => {
  test("accepts a valid spec", () => {
    const result = validateFlowSpec(baseSpec);
    expect(result.valid).toBe(true);
    expect(result.issues).toHaveLength(0);
  });

  test("rejects retries for best effort sinks", () => {
    const result = validateFlowSpec({
      ...baseSpec,
      sinks: [{ ...baseSpec.sinks[0], deliveryGuarantee: "best_effort" }],
    });

    expect(result.valid).toBe(false);
    expect(result.issues.some((issue) => issue.path === "retryPolicy.maxAttempts")).toBe(true);
  });

  test("rejects retries for append-only sinks", () => {
    const result = validateFlowSpec({
      ...baseSpec,
      sinks: [{ ...baseSpec.sinks[0], kind: "s3", deliveryGuarantee: "append_only" }],
    });

    expect(result.valid).toBe(false);
    expect(result.issues.some((issue) => issue.path === "retryPolicy.maxAttempts")).toBe(true);
  });

  test("accepts lookup enrichment with an inline table", () => {
    const result = validateFlowSpec({
      ...baseSpec,
      processors: [
        {
          id: "processor-1",
          kind: "enrich_lookup",
          keyPath: "customerId",
          targetPath: "customer.risk",
          lookup: {
            mode: "inline",
            table: {
              "cust-1": { band: "low" },
            },
            missing: "skip",
          },
          nextNodeIds: ["sink-1"],
        },
      ],
    });

    expect(result.valid).toBe(true);
  });

  test("rejects enabled batching with batchSize below the minimum", () => {
    const result = validateFlowSpec({
      ...baseSpec,
      batchingPolicy: { enabled: true, batchSize: 1 },
    });

    expect(result.valid).toBe(false);
    expect(result.issues.some((issue) => issue.path === "batchingPolicy.batchSize")).toBe(true);
  });

  test("rejects enabled batching with batchSize above the maximum", () => {
    const result = validateFlowSpec({
      ...baseSpec,
      batchingPolicy: { enabled: true, batchSize: 500 },
    });

    expect(result.valid).toBe(false);
    expect(result.issues.some((issue) => issue.path === "batchingPolicy.batchSize")).toBe(true);
  });

  test("rejects enabled batching with an excessive flushIntervalMs", () => {
    const result = validateFlowSpec({
      ...baseSpec,
      batchingPolicy: { enabled: true, batchSize: 10, flushIntervalMs: 3_600_000 },
    });

    expect(result.valid).toBe(false);
    expect(result.issues.some((issue) => issue.path === "batchingPolicy.flushIntervalMs")).toBe(true);
  });

  test("accepts enabled batching within bounds", () => {
    const result = validateFlowSpec({
      ...baseSpec,
      batchingPolicy: { enabled: true, batchSize: 10, flushIntervalMs: 250 },
    });

    expect(result.valid).toBe(true);
  });

  test("accepts disabled batching regardless of batchSize", () => {
    const result = validateFlowSpec({
      ...baseSpec,
      batchingPolicy: { enabled: false, batchSize: 1 },
    });

    expect(result.valid).toBe(true);
  });

  test("rejects adapter-executed processors until async processor runtime exists", () => {
    const result = validateFlowSpec({
      ...baseSpec,
      processors: [
        {
          id: "processor-1",
          kind: "enrich_lookup",
          connector: {
            capabilityId: "credit_score_lookup",
            connectorId: "credit_score_postgres",
            executionMode: "adapter",
          },
          keyPath: "customerId",
          lookup: {
            mode: "inline",
            table: {},
          },
          nextNodeIds: ["sink-1"],
        },
      ],
    });

    expect(result.valid).toBe(false);
    expect(result.issues.some((entry) => entry.path === "processors[0].connector.executionMode")).toBe(true);
  });
});
