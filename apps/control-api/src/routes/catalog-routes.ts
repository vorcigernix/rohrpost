import { Elysia, t } from "elysia";
import type { ControlApiCapabilitiesResponse } from "@rohrpost/control-api-contracts";
import { findCapability } from "@rohrpost/domain-connectors";
import type { ControlApiRouteDeps } from "./route-context";

export function createCatalogRoutes(deps: ControlApiRouteDeps) {
  return new Elysia()
    .get("/api/overview", ({ request, status }) => {
      const auth = deps.requireAuth(request);
      if (!auth) return status(401, { error: "Unauthorized" });
      return deps.repository.getOverview();
    })
    .get("/api/capabilities", ({ request, status }) => {
      const auth = deps.requireAuth(request);
      if (!auth) return status(401, { error: "Unauthorized" });

      const native = deps.repository.listCapabilities().filter((capability) => capability.executionMode === "native");
      const adapter = deps.repository.listCapabilities().filter((capability) => capability.executionMode === "adapter");

      const response: ControlApiCapabilitiesResponse = {
        native,
        adapter,
        guarantees: {
          delivery: "at-least-once",
          ordering: "per partition key only",
          duplicatesPossible: true,
        },
      };

      return response;
    })
    .get("/api/setup/ai", ({ request, status }) => {
      const auth = deps.requireAuth(request);
      if (!auth) return status(401, { error: "Unauthorized" });

      return deps.serializeAiSettings(deps.repository.getAiProviderSettings());
    })
    .post(
      "/api/setup/ai",
      ({ request, body, status }) => {
        const auth = deps.requireAuth(request);
        if (!auth) return status(401, { error: "Unauthorized" });

        const settings = deps.repository.saveAiProviderSettings({
          provider: body.provider,
          enabled: body.enabled,
          apiKey: body.apiKey,
          clearApiKey: body.clearApiKey,
          model: body.model,
          apiBaseUrl: body.apiBaseUrl,
        });

        return deps.serializeAiSettings(settings);
      },
      {
        body: t.Object({
          provider: t.Literal("gemini"),
          enabled: t.Boolean(),
          apiKey: t.Optional(t.String()),
          clearApiKey: t.Optional(t.Boolean()),
          model: t.Optional(t.String()),
          apiBaseUrl: t.Optional(t.String()),
        }),
      },
    )
    .get("/api/connectors", ({ request, query, status }) => {
      const auth = deps.requireAuth(request);
      if (!auth) return status(401, { error: "Unauthorized" });

      return deps.repository.listConnectors({
        capabilityId: query.capabilityId,
        tenantId: query.tenantId,
      });
    }, {
      query: t.Object({
        capabilityId: t.Optional(t.String()),
        tenantId: t.Optional(t.String()),
      }),
    })
    .post("/api/connectors", ({ request, body, status }) => {
      const auth = deps.requireAuth(request);
      if (!auth) return status(401, { error: "Unauthorized" });

      const capability = findCapability(body.capabilityId);
      if (!capability) {
        return status(404, { error: "Unknown capability" });
      }

      const connector = deps.repository.saveConnector({
        id: body.id,
        tenantId: body.tenantId ?? deps.config.defaultTenantId,
        name: body.name,
        capabilityId: capability.id,
        executionMode: capability.executionMode,
        config: body.config ?? {},
      });
      deps.publishConsoleEvents("connectors", "flows");
      return connector;
    }, {
      body: t.Object({
        id: t.Optional(t.String()),
        tenantId: t.Optional(t.String()),
        name: t.String({ minLength: 1 }),
        capabilityId: t.String(),
        config: t.Optional(t.Any()),
      }),
    })
    .post(
      "/api/connectors/test",
      async ({ request, body, status }) => {
        const auth = deps.requireAuth(request);
        if (!auth) return status(401, { error: "Unauthorized" });

        const capability = findCapability(body.capabilityId);
        if (!capability) {
          return status(404, { error: "Unknown capability" });
        }

        if ((capability.kind === "source" || capability.kind === "sink") && !body.config) {
          return status(400, { error: "Connector config is required" });
        }

        if (capability.executionMode === "adapter" && deps.adapterClient) {
          const direction = capability.kind === "source"
            ? "source"
            : capability.kind === "sink"
              ? "sink"
              : "bidirectional";

          try {
            const adapterResult = await deps.adapterClient.testConnector({
              capabilityId: body.capabilityId,
              direction,
            });

            return {
              ok: adapterResult.ok,
              capability,
              validatedAt: new Date().toISOString(),
              adapterManaged: true,
              adapter: adapterResult,
            };
          } catch (error) {
            return status(502, {
              error: error instanceof Error ? error.message : "Adapter connector test failed",
            });
          }
        }

        return {
          ok: true,
          capability,
          validatedAt: new Date().toISOString(),
          adapterManaged: capability.executionMode === "adapter",
        };
      },
      {
        body: t.Object({
          capabilityId: t.String(),
          config: t.Optional(t.Any()),
        }),
      },
    );
}
