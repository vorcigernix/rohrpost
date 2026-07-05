import { classifyNatsPublishError, createNatsRuntime } from "./nats";
import { createRouterControlApiClient } from "./control-api";
import { loadDeploymentTargetMap, loadRouterWorkerConfig } from "./config";
import { ControlApiDeploymentSource, StaticDeploymentSource } from "./deployment-source";
import { IngressOverloadedError, RouterWorkerRuntime, createRouterWorkerRuntime } from "./phase2-worker";
import {
  buildDemoDeployment,
  buildHttpIngressEnvelopeFromBody,
  findHttpIngressDeployment,
  buildReplayRequestFromBody,
} from "./index-helpers";

export * from "./config";
export * from "./control-api";
export * from "./deployment-source";
export * from "./delivery";
export * from "./jetstream";
export * from "./nats";
export * from "./phase2-types";
export * from "./phase2-worker";

function parseJsonBody(request: Request): Promise<unknown> {
  return request.json().catch(() => ({}));
}

function overloadResponse(error: IngressOverloadedError): Response {
  return Response.json(
    {
      error: "ingress_backpressure",
      reason: error.admission.reason,
      scope: error.admission.scope,
      deploymentId: error.admission.deploymentId,
      bufferedForDeployment: error.admission.bufferedForDeployment,
      bufferedTotal: error.admission.bufferedTotal,
      limitForDeployment: error.admission.limitForDeployment,
      limitTotal: error.admission.limitTotal,
      retryAfterMs: error.admission.retryAfterMs,
    },
    {
      status: 429,
      headers: {
        "retry-after": String(Math.max(1, Math.ceil(error.admission.retryAfterMs / 1000))),
      },
    },
  );
}

const config = loadRouterWorkerConfig();

async function createRuntimeFromConfig(runtimeConfig = config): Promise<RouterWorkerRuntime> {
  const deploymentTargets = loadDeploymentTargetMap();
  const controlApiClient = runtimeConfig.controlApiUrl
    ? createRouterControlApiClient({
        controlApiUrl: runtimeConfig.controlApiUrl,
        controlApiToken: runtimeConfig.controlApiToken,
        sinkTargets: deploymentTargets,
      })
    : undefined;

  let deploymentSource;
  if (controlApiClient) {
    deploymentSource = controlApiClient.deploymentSource;
  } else {
    deploymentSource = new StaticDeploymentSource(
      buildDemoDeployment(runtimeConfig.serviceName, deploymentTargets.sinks),
    );
  }

  const messageBus = runtimeConfig.natsUrl
    ? (await createNatsRuntime(runtimeConfig.natsUrl, runtimeConfig.serviceName)).bus
    : undefined;

  return createRouterWorkerRuntime({
    config: runtimeConfig,
    deploymentSource,
    deploymentTargetMap: deploymentTargets,
    messageBus,
    controlApiClient,
  });
}

const runtimeReady = createRuntimeFromConfig();

const server = Bun.serve({
  hostname: config.httpHost,
  port: config.httpPort,
  async fetch(request) {
    const runtime = await runtimeReady;
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/health") {
      return Response.json({
        ok: true,
        service: config.serviceName,
      });
    }

    if (request.method === "GET" && url.pathname === "/status") {
      return Response.json(runtime.getSummary());
    }

    if (request.method === "GET" && url.pathname === "/runs") {
      return Response.json(runtime.getRuns());
    }

    if (request.method === "GET" && url.pathname === "/dlq") {
      return Response.json(runtime.getDlq());
    }

    if (request.method === "POST" && url.pathname === "/sync") {
      await runtime.syncDeployments();
      return Response.json({ ok: true, deployments: runtime.getDeployments().length });
    }

    if (request.method === "POST" && url.pathname === "/ingress") {
      const body = (await parseJsonBody(request)) as Record<string, unknown>;
      const deploymentId = String(body.deploymentId ?? "");
      const deployment = runtime.getDeployments().find((entry) => entry.id === deploymentId);
      if (!deployment) {
        return Response.json({ error: "unknown deployment" }, { status: 404 });
      }

      const envelope = buildHttpIngressEnvelopeFromBody(body, deployment);
      if (runtime.getSummary().mode === "nats") {
        try {
          const result = await runtime.enqueueIngress({
            deploymentId: deployment.id,
            tenantId: envelope.tenantId,
            flowId: envelope.flowId,
            revisionId: envelope.revisionId,
            messageId: envelope.messageId,
            sourceRef: envelope.sourceRef,
            partitionKey: envelope.partitionKey,
            headers: envelope.headers,
            payload: envelope.payload,
            receivedAt: envelope.receivedAt,
            traceId: envelope.traceId,
          }, { admissionMode: "reject" });
          return Response.json({ accepted: true, subject: result.subject, envelope: result.envelope }, { status: 202 });
        } catch (error) {
          if (error instanceof IngressOverloadedError) {
            return overloadResponse(error);
          }
          const failure = classifyNatsPublishError(error);
          if (failure) {
            return Response.json(failure.body, { status: failure.status });
          }

          throw error;
        }
      }

      try {
        const processed = await runtime.ingestEnvelope({
          deploymentId: deployment.id,
          tenantId: envelope.tenantId,
          flowId: envelope.flowId,
          revisionId: envelope.revisionId,
          messageId: envelope.messageId,
          sourceRef: envelope.sourceRef,
          partitionKey: envelope.partitionKey,
          headers: envelope.headers,
          payload: envelope.payload,
          receivedAt: envelope.receivedAt,
          traceId: envelope.traceId,
        }, { admissionMode: "reject" });
        return Response.json(processed, { status: 200 });
      } catch (error) {
        if (error instanceof IngressOverloadedError) {
          return overloadResponse(error);
        }

        throw error;
      }
    }

    if (request.method === "POST") {
      const { deployment, conflict } = findHttpIngressDeployment(runtime.getDeployments(), url.pathname);
      if (conflict) {
        return Response.json({ error: "ambiguous_http_ingress_path" }, { status: 409 });
      }

      if (deployment) {
        const body = (await parseJsonBody(request)) as Record<string, unknown>;
        const envelope = buildHttpIngressEnvelopeFromBody(
          {
            ...body,
            sourceRef: url.pathname,
          },
          deployment,
        );

        if (runtime.getSummary().mode === "nats") {
          try {
            const result = await runtime.enqueueIngress({
              deploymentId: deployment.id,
              tenantId: envelope.tenantId,
              flowId: envelope.flowId,
              revisionId: envelope.revisionId,
              messageId: envelope.messageId,
              sourceRef: envelope.sourceRef,
              partitionKey: envelope.partitionKey,
              headers: envelope.headers,
              payload: envelope.payload,
              receivedAt: envelope.receivedAt,
              traceId: envelope.traceId,
            }, { admissionMode: "reject" });
            return Response.json(
              { accepted: true, subject: result.subject, envelope: result.envelope },
              { status: 202 },
            );
          } catch (error) {
            if (error instanceof IngressOverloadedError) {
              return overloadResponse(error);
            }
            const failure = classifyNatsPublishError(error);
            if (failure) {
              return Response.json(failure.body, { status: failure.status });
            }

            throw error;
          }
        }

        try {
          const processed = await runtime.ingestEnvelope({
            deploymentId: deployment.id,
            tenantId: envelope.tenantId,
            flowId: envelope.flowId,
            revisionId: envelope.revisionId,
            messageId: envelope.messageId,
            sourceRef: envelope.sourceRef,
            partitionKey: envelope.partitionKey,
            headers: envelope.headers,
            payload: envelope.payload,
            receivedAt: envelope.receivedAt,
            traceId: envelope.traceId,
          }, { admissionMode: "reject" });
          return Response.json(processed, { status: 200 });
        } catch (error) {
          if (error instanceof IngressOverloadedError) {
            return overloadResponse(error);
          }

          throw error;
        }
      }
    }

    if (request.method === "POST" && url.pathname === "/replay") {
      const body = (await parseJsonBody(request)) as Record<string, unknown>;
      const replay = buildReplayRequestFromBody(body);
      if (runtime.getSummary().mode === "nats") {
        const processed = await runtime.replay(replay);
        return Response.json(processed, { status: 202 });
      }
      const processed = await runtime.replay(replay);
      return Response.json(processed, { status: 200 });
    }

    return Response.json({ error: "not found" }, { status: 404 });
  },
});

if (import.meta.main) {
  console.log(`[${config.serviceName}] listening on http://${server.hostname}:${server.port}`);
}

export { server };
