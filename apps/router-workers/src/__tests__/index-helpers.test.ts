import { describe, expect, test } from "bun:test";
import type { FlowSpec } from "@rohrpost/shared-flow-spec";
import { compileFlowSpec } from "@rohrpost/shared-flow-spec";
import {
  buildDeploymentIngressPattern,
  buildDeploymentReplayPattern,
  buildDeploymentRetryPattern,
} from "../jetstream";
import { findHttpIngressDeployment } from "../index-helpers";
import type { RouterDeployment } from "../phase2-types";

function buildDeployment(id: string, path: string): RouterDeployment {
  const spec: FlowSpec = {
    version: 1,
    metadata: {
      tenantId: "tenant_test",
      flowId: id,
      revisionId: `${id}_v1`,
      name: id,
    },
    sources: [
      {
        id: "source_primary",
        kind: "http",
        connector: {
          capabilityId: "http_in",
          connectorId: `http_in_${id}`,
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

  return {
    id,
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
    httpSourcePaths: [path],
    sinkTargets: {},
    connectors: {},
  };
}

describe("findHttpIngressDeployment", () => {
  test("returns the deployment that owns a concrete HTTP ingress path", () => {
    const deployment = buildDeployment("flow-a", "/ingest/customers");
    const resolved = findHttpIngressDeployment([deployment], "/ingest/customers");
    expect(resolved.conflict).toBe(false);
    expect(resolved.deployment?.id).toBe("flow-a");
  });

  test("flags conflicts when multiple deployments claim the same HTTP ingress path", () => {
    const first = buildDeployment("flow-a", "/ingest/customers");
    const second = buildDeployment("flow-b", "/ingest/customers");
    const resolved = findHttpIngressDeployment([first, second], "/ingest/customers");
    expect(resolved.conflict).toBe(true);
    expect(resolved.deployment).toBeNull();
  });
});
