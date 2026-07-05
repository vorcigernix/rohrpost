import { describe, expect, test } from "bun:test";
import { FLOW_SPEC_VERSION, simulateFlowSpec } from "../src";
import type { FlowSpec } from "../src";

const spec: FlowSpec = {
  version: FLOW_SPEC_VERSION,
  metadata: {
    tenantId: "tenant-1",
    flowId: "flow-sim",
    revisionId: "rev-1",
    name: "Simulation flow",
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
      nextNodeIds: ["processor-map"],
    },
  ],
  processors: [
    {
      id: "processor-map",
      kind: "map",
      mappings: [{ from: "customerId", to: "customer.id" }],
      nextNodeIds: ["processor-enrich"],
    },
    {
      id: "processor-enrich",
      kind: "enrich_static",
      values: { region: "eu" },
      nextNodeIds: ["sink-1"],
    },
  ],
  routes: [
    {
      id: "route-1",
      fromNodeId: "processor-enrich",
      predicate: { type: "always" },
      toSinkIds: ["sink-1"],
    },
  ],
  sinks: [
    {
      id: "sink-1",
      kind: "s3",
      connector: {
        capabilityId: "cap-s3",
        connectorId: "s3-1",
        executionMode: "native",
      },
      deliveryGuarantee: "append_only",
    },
  ],
  retryPolicy: {
    maxAttempts: 1,
    initialBackoffMs: 10,
    maxBackoffMs: 1000,
    multiplier: 2,
  },
  dlqPolicy: {
    enabled: false,
  },
  batchingPolicy: {
    enabled: false,
    batchSize: 1,
  },
  idempotencyStrategy: "message_id",
};

describe("simulateFlowSpec", () => {
  test("produces transformed output", () => {
    const result = simulateFlowSpec(spec, [
      {
        envelope: {
          receivedAt: "2026-03-31T10:00:00.000Z",
        },
        payload: {
          customerId: "cust-1",
          message: "hello",
        },
      },
    ]);

    expect(result.accepted).toBe(1);
    expect(result.dropped).toBe(0);
    expect(result.outputs).toBe(1);
    expect(result.items[0]?.outputs[0]?.payload).toEqual({
      customer: { id: "cust-1" },
      customerId: "cust-1",
      message: "hello",
      region: "eu",
    });
  });

  test("projects a new payload shape when map mode is project", () => {
    const projectedSpec: FlowSpec = {
      ...spec,
      metadata: {
        ...spec.metadata,
        flowId: "flow-project",
        revisionId: "rev-project-1",
      },
      processors: [
        {
          id: "processor-map",
          kind: "map",
          mode: "project",
          mappings: [
            { from: "customerId", to: "customer.id" },
            { from: "message", to: "note" },
          ],
          nextNodeIds: ["sink-1"],
        },
      ],
      routes: [
        {
          id: "route-project",
          fromNodeId: "processor-map",
          predicate: { type: "always" },
          toSinkIds: ["sink-1"],
        },
      ],
    };

    const result = simulateFlowSpec(projectedSpec, [
      {
        envelope: {},
        payload: {
          customerId: "cust-9",
          message: "hello",
          ignored: "drop-me",
        },
      },
    ]);

    expect(result.accepted).toBe(1);
    expect(result.items[0]?.outputs[0]?.payload).toEqual({
      customer: { id: "cust-9" },
      note: "hello",
    });
  });

  test("enriches from an inline lookup table", () => {
    const lookupSpec: FlowSpec = {
      ...spec,
      metadata: {
        ...spec.metadata,
        flowId: "flow-lookup",
        revisionId: "rev-lookup-1",
      },
      sources: [
        {
          ...spec.sources[0],
          nextNodeIds: ["processor-enrich"],
        },
      ],
      processors: [
        {
          id: "processor-enrich",
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
          nextNodeIds: ["sink-1"],
        },
      ],
      routes: [
        {
          id: "route-lookup",
          fromNodeId: "processor-enrich",
          predicate: { type: "always" },
          toSinkIds: ["sink-1"],
        },
      ],
    };

    const result = simulateFlowSpec(lookupSpec, [
      {
        envelope: {},
        payload: {
          customerId: "cust-1",
          message: "hello",
        },
      },
    ]);

    expect(result.accepted).toBe(1);
    expect(result.items[0]?.outputs[0]?.payload).toEqual({
      customerId: "cust-1",
      message: "hello",
      customer: {
        risk: {
          band: "gold",
          score: 742,
        },
      },
    });
  });

  test("drops lookup misses when configured to fail", () => {
    const lookupSpec: FlowSpec = {
      ...spec,
      metadata: {
        ...spec.metadata,
        flowId: "flow-lookup-miss",
        revisionId: "rev-lookup-miss-1",
      },
      sources: [
        {
          ...spec.sources[0],
          nextNodeIds: ["processor-enrich"],
        },
      ],
      processors: [
        {
          id: "processor-enrich",
          kind: "enrich_lookup",
          keyPath: "customerId",
          lookup: {
            mode: "inline",
            table: {},
            missing: "fail",
          },
          nextNodeIds: ["sink-1"],
        },
      ],
      routes: [
        {
          id: "route-lookup",
          fromNodeId: "processor-enrich",
          predicate: { type: "always" },
          toSinkIds: ["sink-1"],
        },
      ],
    };

    const result = simulateFlowSpec(lookupSpec, [
      {
        envelope: {},
        payload: {
          customerId: "cust-missing",
        },
      },
    ]);

    expect(result.accepted).toBe(0);
    expect(result.dropped).toBe(1);
    expect(result.items[0]?.dropReason).toContain("lookup miss");
  });
});
