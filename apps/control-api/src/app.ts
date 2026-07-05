import { Elysia, t } from "elysia";
import { AdapterRedpandaClient } from "./adapter-client";
import type { ControlApiConfig } from "./config";
import { loadConfig } from "./config";
import { createFlowAuthoringService } from "./flows/service";
import { createOidcAuth } from "./oidc";
import { createRepository } from "./repository";
import { createCatalogRoutes } from "./routes/catalog-routes";
import { createFlowRoutes } from "./routes/flow-routes";
import type { ConsoleEventKind, ConsoleEventEnvelope, ControlApiRouteDeps } from "./routes/route-context";
import { createRuntimeRoutes } from "./routes/runtime-routes";
export interface CreateAppOptions {
  config?: ControlApiConfig;
  fetchImpl?: typeof fetch;
}

function applyCors(set: { headers: Record<string, string | number> }, request: Request) {
  const origin = request.headers.get("origin");
  set.headers["access-control-allow-origin"] = origin ?? "*";
  set.headers["vary"] = "Origin";
  set.headers["access-control-allow-methods"] = "GET, POST, OPTIONS";
  set.headers["access-control-allow-headers"] = "Authorization, Content-Type";
  if (origin) set.headers["access-control-allow-credentials"] = "true";
  set.headers["access-control-max-age"] = "86400";
}

function bearerToken(request: Request): string | null {
  const header = request.headers.get("authorization");
  if (!header) return null;
  const [scheme, token] = header.split(" ");
  if (scheme?.toLowerCase() !== "bearer" || !token) return null;
  return token;
}

function queryToken(request: Request): string | null {
  const url = new URL(request.url);
  return url.searchParams.get("access_token") ?? url.searchParams.get("token");
}

export function createApp(options: CreateAppOptions = {}) {
  const config = options.config ?? loadConfig();
  const repository = createRepository(config);
  const oidcAuth = createOidcAuth(config, options.fetchImpl);
  const adapterClient = config.adapterRedpandaUrl
    ? new AdapterRedpandaClient(config.adapterRedpandaUrl, options.fetchImpl)
    : null;
  const consoleEventListeners = new Set<(event: ConsoleEventEnvelope) => void>();
  let consoleEventVersion = 0;

  function requireAuth(request: Request) {
    return repository.authenticate(bearerToken(request)) ?? oidcAuth.authenticate(request);
  }

  function requireStreamAuth(request: Request) {
    return repository.authenticate(bearerToken(request) ?? queryToken(request)) ?? oidcAuth.authenticate(request);
  }

  function publishConsoleEvents(...kinds: ConsoleEventKind[]) {
    const at = new Date().toISOString();
    for (const kind of kinds) {
      const event = {
        id: String(++consoleEventVersion),
        kind,
        at,
      } satisfies ConsoleEventEnvelope;

      for (const listener of consoleEventListeners) {
        listener(event);
      }
    }
  }

  function serializeAiSettings(settings: ReturnType<typeof repository.getAiProviderSettings>) {
    return {
      provider: settings.provider,
      enabled: settings.enabled,
      model: settings.model,
      apiBaseUrl: settings.apiBaseUrl,
      apiKeyConfigured: Boolean(settings.apiKey),
      source: settings.source,
      updatedAt: settings.updatedAt,
      activeProvider: settings.enabled && settings.apiKey ? "gemini" : "heuristic",
    };
  }

  const flowAuthoring = createFlowAuthoringService({
    config,
    repository,
    fetchImpl: options.fetchImpl,
  });

  const routeDeps = {
    config,
    repository,
    flowAuthoring,
    adapterClient,
    requireAuth,
    requireStreamAuth,
    publishConsoleEvents,
    onConsoleEvent(listener) {
      consoleEventListeners.add(listener);
      return () => {
        consoleEventListeners.delete(listener);
      };
    },
    getConsoleEventVersion() {
      return consoleEventVersion;
    },
    serializeAiSettings,
  } satisfies ControlApiRouteDeps;

  return new Elysia()
    .onRequest(({ request, set }) => {
      applyCors(set, request);
    })
    .options("/*", ({ request, set }) => {
      applyCors(set, request);
      set.status = 204;
      return "";
    })
    .get("/", () => ({
      service: "control-api",
      bootstrapTokenHint: "Use Authorization: Bearer dev-admin-token in development",
    }))
    .get("/health", () => ({
      ok: true,
      service: "control-api",
      databasePath: config.databasePath,
    }))
    .get("/api/auth/oidc", async ({ status }) => {
      try {
        return await oidcAuth.publicConfig();
      } catch (error) {
        return status(502, { error: error instanceof Error ? error.message : "OIDC configuration failed." });
      }
    })
    .get("/api/auth/session", ({ request }) => oidcAuth.sessionResponse(request))
    .post(
      "/api/auth/oidc/token",
      async ({ body, request, set, status }) => {
        try {
          const result = await oidcAuth.exchangeCode(body, request);
          set.headers["set-cookie"] = result.cookie;
          return result.body;
        } catch (error) {
          return status(401, { error: error instanceof Error ? error.message : "OIDC login failed." });
        }
      },
      {
        body: t.Object({
          code: t.String(),
          codeVerifier: t.String(),
          redirectUri: t.String(),
        }),
      },
    )
    .post("/api/auth/logout", ({ request, set }) => {
      set.headers["set-cookie"] = oidcAuth.clearCookie(request);
      return { ok: true };
    })
    .use(createCatalogRoutes(routeDeps))
    .use(createRuntimeRoutes(routeDeps))
    .use(createFlowRoutes(routeDeps));
}
