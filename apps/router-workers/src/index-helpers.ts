import {
  compileFlowSpec,
  type CanonicalEnvelope,
  type FlowSpec,
} from "@rohrpost/shared-flow-spec";
import {
  buildDeploymentIngressPattern,
  buildDeploymentReplayPattern,
  buildDeploymentRetryPattern,
} from "./jetstream";
import type {
  DeploymentTargetMap,
  ReplayRequest,
  RouterDeployment,
  SinkTarget,
} from "./phase2-types";

function buildDemoFlow(): FlowSpec {
  return {
    version: 1,
    metadata: {
      tenantId: "tenant_demo",
      flowId: "flow_demo_http",
      revisionId: "rev_demo_http_v1",
      name: "Demo HTTP Router",
      description: "Local Phase 2 demo flow",
    },
    sources: [
      {
        id: "source_http",
        kind: "http",
        connector: {
          capabilityId: "http_in",
          connectorId: "http_in_default",
          executionMode: "native",
        },
        stream: "ingress",
        nextNodeIds: ["processor_enrich"],
      },
    ],
    processors: [
      {
        id: "processor_enrich",
        kind: "enrich_static",
        values: {
          routedBy: "router-workers",
        },
        nextNodeIds: ["route_primary"],
      },
    ],
    routes: [
      {
        id: "route_primary",
        fromNodeId: "processor_enrich",
        predicate: { type: "always" },
        toSinkIds: ["sink_http"],
        priority: 100,
      },
    ],
    sinks: [
      {
        id: "sink_http",
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
      initialBackoffMs: 250,
      maxBackoffMs: 2_000,
      multiplier: 2,
      retryableStatusCodes: [408, 429, 500, 502, 503, 504],
    },
    dlqPolicy: {
      enabled: true,
      sinkId: "sink_http",
      reasonFormat: "json",
    },
    batchingPolicy: {
      enabled: false,
      batchSize: 1,
    },
    idempotencyStrategy: "message_id",
  };
}

function defaultSinkTargets(sinkTargets: Record<string, SinkTarget>): Record<string, SinkTarget> {
  if (Object.keys(sinkTargets).length > 0) {
    return sinkTargets;
  }

  return {
    http_out_default: {
      kind: "http",
      connectorId: "http_out_default",
      url: "http://127.0.0.1:3999/echo",
      method: "POST",
    },
  };
}

export function buildDemoDeployment(
  serviceName: string,
  sinkTargets: Record<string, SinkTarget>,
): RouterDeployment[] {
  const spec = buildDemoFlow();
  const compiled = compileFlowSpec(spec);
  const resolvedSinkTargets = defaultSinkTargets(sinkTargets);

  return [
    {
      id: `${serviceName}-demo`,
      tenantId: spec.metadata.tenantId,
      flowId: spec.metadata.flowId,
      revisionId: spec.metadata.revisionId,
      active: true,
      spec,
      compiled,
      sourceSubjects: [
        buildDeploymentIngressPattern(spec.metadata.tenantId, spec.metadata.flowId, spec.metadata.revisionId),
        buildDeploymentRetryPattern(spec.metadata.tenantId, spec.metadata.flowId, spec.metadata.revisionId),
        buildDeploymentReplayPattern(spec.metadata.tenantId, spec.metadata.flowId, spec.metadata.revisionId),
      ],
      natsSourceSubjects: [],
      httpSourcePaths: [],
      sinkTargets: resolvedSinkTargets,
      connectors: {},
    },
  ];
}

export function findHttpIngressDeployment(
  deployments: RouterDeployment[],
  pathname: string,
): { deployment: RouterDeployment | null; conflict: boolean } {
  const matches = deployments.filter((deployment) =>
    (deployment.httpSourcePaths ?? []).includes(pathname),
  );

  if (matches.length > 1) {
    return { deployment: null, conflict: true };
  }

  return {
    deployment: matches[0] ?? null,
    conflict: false,
  };
}

export function buildHttpIngressEnvelopeFromBody(
  body: Record<string, unknown>,
  deployment: RouterDeployment,
): CanonicalEnvelope {
  return {
    tenantId: String(body.tenantId ?? deployment.tenantId),
    flowId: String(body.flowId ?? deployment.flowId),
    revisionId: String(body.revisionId ?? deployment.revisionId),
    messageId: String(body.messageId ?? crypto.randomUUID()),
    sourceRef: String(body.sourceRef ?? "http-ingress"),
    partitionKey: String(body.partitionKey ?? deployment.tenantId),
    headers: typeof body.headers === "object" && body.headers ? (body.headers as Record<string, string>) : {},
    payload: body.payload ?? body,
    receivedAt: typeof body.receivedAt === "string" ? body.receivedAt : new Date().toISOString(),
    traceId: String(body.traceId ?? `${deployment.flowId}:${Date.now()}`),
  };
}

export function buildReplayRequestFromBody(body: Record<string, unknown>): ReplayRequest {
  const envelope = body.envelope as CanonicalEnvelope | undefined;
  if (!envelope) {
    throw new Error("replay.envelope is required");
  }

  return {
    deploymentId: String(body.deploymentId ?? ""),
    envelope,
    reason: String(body.reason ?? "manual replay"),
    requestedAt: typeof body.requestedAt === "string" ? body.requestedAt : new Date().toISOString(),
  };
}
