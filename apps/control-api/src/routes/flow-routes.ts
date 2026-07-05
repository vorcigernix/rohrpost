import { Elysia, t } from "elysia";
import { type FlowSpec, FlowSpecValidationError } from "@rohrpost/shared-flow-spec";
import type { ControlApiRouteDeps } from "./route-context";

export function createFlowRoutes(deps: ControlApiRouteDeps) {
  return new Elysia()
    .get("/api/flows", ({ request, status }) => {
      const auth = deps.requireAuth(request);
      if (!auth) return status(401, { error: "Unauthorized" });
      return deps.flowAuthoring.listFlows();
    })
    .post(
      "/api/flows/compose-json-transform",
      async ({ request, body, status }) => {
        const auth = deps.requireAuth(request);
        if (!auth) return status(401, { error: "Unauthorized" });
        return deps.flowAuthoring.composeJsonTransform(body);
      },
      {
        body: t.Object({
          prompt: t.String({ minLength: 3 }),
          samplePayload: t.Any(),
          sourceKind: t.Optional(
            t.Union([t.Literal("http"), t.Literal("nats"), t.Literal("kafka")]),
          ),
          sinkCapabilityId: t.Optional(
            t.Union([
              t.Literal("http_out"),
              t.Literal("nats_out"),
              t.Literal("snowflake_sink"),
              t.Literal("bigquery_sink"),
              t.Literal("s3_sink"),
              t.Literal("kafka_out"),
            ]),
          ),
          sinkConnectorId: t.Optional(t.String()),
          name: t.Optional(t.String()),
          tenantId: t.Optional(t.String()),
        }),
      },
    )
    .post(
      "/api/flows/draft-from-prompt",
      ({ request, body, status }) => {
        const auth = deps.requireAuth(request);
        if (!auth) return status(401, { error: "Unauthorized" });

        return deps.flowAuthoring.createDraftFromPrompt(body);
      },
      {
        body: t.Object({
          prompt: t.String({ minLength: 3 }),
          name: t.Optional(t.String()),
          tenantId: t.Optional(t.String()),
          samplePayload: t.Optional(t.Any()),
        }),
      },
    )
    .post(
      "/api/flows/validate",
      ({ request, body, status }) => {
        const auth = deps.requireAuth(request);
        if (!auth) return status(401, { error: "Unauthorized" });

        try {
          return deps.flowAuthoring.validateFlow({
            spec: body.spec as FlowSpec,
            samplePayload: body.samplePayload,
          });
        } catch (error) {
          if (error instanceof FlowSpecValidationError) {
            return status(400, { error: error.message, issues: error.issues });
          }

          throw error;
        }
      },
      {
        body: t.Object({
          spec: t.Any(),
          samplePayload: t.Optional(t.Any()),
        }),
      },
    )
    .post(
      "/api/flows",
      ({ request, body, status }) => {
        const auth = deps.requireAuth(request);
        if (!auth) return status(401, { error: "Unauthorized" });

        const result = deps.flowAuthoring.saveFlow({
          tenantId: body.tenantId,
          name: body.name,
          samplePayload: body.samplePayload,
          sourceBinding: body.sourceBinding,
          spec: body.spec as FlowSpec,
        });
        if (!result.ok) {
          return status(400, { error: "Invalid FlowSpec", issues: result.issues });
        }

        deps.publishConsoleEvents("flows");
        return status(201, result.revision);
      },
      {
        body: t.Object({
          tenantId: t.Optional(t.String()),
          name: t.Optional(t.String()),
          samplePayload: t.Optional(t.Any()),
          sourceBinding: t.Optional(t.Any()),
          spec: t.Any(),
        }),
      },
    )
    .post(
      "/api/flows/:id/publish",
      ({ request, params, body, status }) => {
        const auth = deps.requireAuth(request);
        if (!auth) return status(401, { error: "Unauthorized" });

        try {
          const published = deps.flowAuthoring.publishFlow(params.id, body.revisionId);
          deps.publishConsoleEvents("flows", "runtime");
          return published;
        } catch (error) {
          return status(404, {
            error: error instanceof Error ? error.message : "Publish failed",
          });
        }
      },
      {
        body: t.Object({
          revisionId: t.Optional(t.String()),
        }),
      },
    )
    .delete("/api/flows/:id", ({ request, params, status }) => {
      const auth = deps.requireAuth(request);
      if (!auth) return status(401, { error: "Unauthorized" });

      const deleted = deps.flowAuthoring.deleteFlow(params.id);
      if (!deleted) {
        return status(404, { error: "Flow not found" });
      }

      deps.publishConsoleEvents("flows", "runtime", "connectors");
      return deleted;
    })
    .post(
      "/api/deployments/:id/rollback",
      ({ request, params, body, status }) => {
        const auth = deps.requireAuth(request);
        if (!auth) return status(401, { error: "Unauthorized" });

        try {
          const rolledBack = deps.flowAuthoring.rollbackDeployment(params.id, body.targetRevisionId);
          deps.publishConsoleEvents("flows", "runtime");
          return rolledBack;
        } catch (error) {
          return status(404, {
            error: error instanceof Error ? error.message : "Rollback failed",
          });
        }
      },
      {
        body: t.Object({
          targetRevisionId: t.Optional(t.String()),
        }),
      },
    )
    .post(
      "/api/replays",
      ({ request, body, status }) => {
        const auth = deps.requireAuth(request);
        if (!auth) return status(401, { error: "Unauthorized" });

        return status(
          201,
          deps.flowAuthoring.createReplayRequest({
            flowId: body.flowId,
            revisionId: body.revisionId,
            reason: body.reason,
            sourceStream: body.sourceStream,
          }),
        );
      },
      {
        body: t.Object({
          flowId: t.String(),
          revisionId: t.String(),
          reason: t.String({ minLength: 3 }),
          sourceStream: t.String(),
        }),
      },
    );
}
