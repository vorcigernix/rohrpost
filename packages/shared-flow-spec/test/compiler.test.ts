import { describe, expect, test } from "bun:test";
import { compileFlowSpec, FLOW_SPEC_VERSION } from "../src";
import type { FlowSpec } from "../src";

const spec: FlowSpec = {
  version: FLOW_SPEC_VERSION,
  metadata: {
    tenantId: "tenant-1",
    flowId: "flow-compiler",
    revisionId: "rev-1",
    name: "Compiler flow",
  },
  sources: [
    {
      id: "source-1",
      kind: "nats",
      connector: {
        capabilityId: "cap-nats-in",
        connectorId: "nats-in-1",
        executionMode: "native",
      },
      stream: "ingress",
      nextNodeIds: ["processor-1"],
    },
    {
      id: "source-2",
      kind: "kafka",
      connector: {
        capabilityId: "cap-kafka-in",
        connectorId: "kafka-adapter-1",
        executionMode: "adapter",
      },
      stream: "work",
      nextNodeIds: ["processor-1"],
    },
  ],
  processors: [
    {
      id: "processor-1",
      kind: "map",
      mappings: [{ from: "customerId", to: "customer.id" }],
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
      kind: "snowflake",
      connector: {
        capabilityId: "cap-snowflake",
        connectorId: "snowflake-1",
        executionMode: "native",
      },
      deliveryGuarantee: "idempotent",
    },
  ],
  retryPolicy: {
    maxAttempts: 2,
    initialBackoffMs: 50,
    maxBackoffMs: 2000,
    multiplier: 2,
  },
  dlqPolicy: {
    enabled: false,
  },
  idempotencyStrategy: "message_id",
};

describe("compileFlowSpec", () => {
  test("summarizes the topology", () => {
    const summary = compileFlowSpec(spec);

    expect(summary.sourceCount).toBe(2);
    expect(summary.nativeConnectorCount).toBe(2);
    expect(summary.adapterConnectorCount).toBe(1);
    expect(summary.processorKinds.map).toBe(1);
    expect(summary.deliveryGuarantees.idempotent).toBe(1);
    expect(summary.warnings.some((warning) => warning.includes("adapter-executed"))).toBe(true);
  });

  test("counts lookup enrichment processors", () => {
    const summary = compileFlowSpec({
      ...spec,
      metadata: {
        ...spec.metadata,
        flowId: "flow-compiler-lookup",
        revisionId: "rev-lookup-1",
      },
      sources: [
        {
          ...spec.sources[0],
          nextNodeIds: ["processor-lookup"],
        },
      ],
      processors: [
        {
          id: "processor-lookup",
          kind: "enrich_lookup",
          keyPath: "customerId",
          lookup: {
            mode: "inline",
            table: {
              "cust-1": { band: "low" },
            },
          },
          nextNodeIds: ["sink-1"],
        },
      ],
      routes: [
        {
          ...spec.routes[0],
          fromNodeId: "processor-lookup",
        },
      ],
    });

    expect(summary.processorKinds.enrich_lookup).toBe(1);
    expect(summary.processorKinds.map).toBe(0);
  });
});
