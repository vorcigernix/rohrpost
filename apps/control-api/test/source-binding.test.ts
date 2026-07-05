import { describe, expect, it } from "bun:test";
import {
  applySourceBindingToSpec,
  buildAutoSourceBinding,
  isDefaultSourceConnectorId,
} from "@rohrpost/domain-connectors";
import type { FlowSpec } from "@rohrpost/shared-flow-spec";

function buildSpec(): FlowSpec {
  return {
    version: 1,
    metadata: {
      tenantId: "tenant_test",
      flowId: "flow_test",
      revisionId: "rev_test_v1",
      name: "Test Flow",
    },
    sources: [
      {
        id: "source_primary",
        kind: "http",
        connector: {
          capabilityId: "http_in",
          connectorId: "http_in_default",
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

describe("source binding", () => {
  it("generates a unique HTTP ingress path when the base ref already exists", () => {
    const binding = buildAutoSourceBinding({
      sourceKind: "http",
      flowName: "People",
      existingConnectors: [
        {
          id: "http_in_ingest_people",
          capabilityId: "http_in",
          config: {
            path: "/ingest/people",
            method: "POST",
          },
        },
      ],
    });

    expect(binding.ref).toBe("/ingest/people-2");
    expect(binding.connectorId).toBe("http_in_ingest_people_2");
  });

  it("patches the source connector on a draft spec", () => {
    const binding = buildAutoSourceBinding({
      sourceKind: "http",
      flowName: "Customers Cleaned",
      existingConnectors: [],
    });

    const patched = applySourceBindingToSpec(buildSpec(), binding);
    expect(patched.sources[0]?.connector.connectorId).toBe(binding.connectorId);
    expect(patched.sources[0]?.connector.capabilityId).toBe("http_in");
  });

  it("includes a usable default broker when generating Kafka ingress bindings", () => {
    const binding = buildAutoSourceBinding({
      sourceKind: "kafka",
      flowName: "Orders Ingest",
      existingConnectors: [],
    });

    expect(binding.ref).toBe("router.ingress.orders-ingest");
    expect(binding.config).toEqual({
      topic: "router.ingress.orders-ingest",
      brokers: ["host.docker.internal:9092"],
    });
    expect(binding.executionMode).toBe("adapter");
  });

  it("recognizes the generic default source connector ids", () => {
    expect(isDefaultSourceConnectorId("http_in_default")).toBe(true);
    expect(isDefaultSourceConnectorId("nats_in_default")).toBe(true);
    expect(isDefaultSourceConnectorId("kafka_in_default")).toBe(true);
    expect(isDefaultSourceConnectorId("http_in_ingest_people")).toBe(false);
  });
});
