import { Elysia, t } from "elysia";

import { loadAdapterRedpandaConfig } from "./config";
import {
  ADAPTER_CONNECTOR_CAPABILITIES,
  buildConnectorManifests,
  findManifest,
  findManifestForConnectorRef,
} from "./manifests";
import { RedpandaConnectSupervisor } from "./connect-supervisor";
import { AdapterRedpandaRuntime } from "./runtime";

const config = loadAdapterRedpandaConfig();
const manifests = buildConnectorManifests(config.redpandaConnectImage);
const runtime = new AdapterRedpandaRuntime(config);
const supervisor = new RedpandaConnectSupervisor(config);
await runtime.start();
await supervisor.start();

const app = new Elysia({ name: "adapter-redpanda" })
  .get("/health", () => ({
    ok: true,
    service: config.serviceName,
    manifestSource: config.manifestSource,
    natsUrl: config.natsUrl,
  }))
  .get("/status", () => ({
    service: config.serviceName,
    host: config.host,
    port: config.port,
    manifestSource: config.manifestSource,
    image: config.redpandaConnectImage,
    connectorCount: manifests.length,
    executionMode: "adapter",
    runtime: runtime.getSummary(),
    supervisor: supervisor.getSummary(),
  }))
  .get("/manifests", () => ({
    service: config.serviceName,
    manifests,
  }))
  .get("/deliveries", () => ({
    service: config.serviceName,
    runtime: runtime.getSummary(),
    deliveries: runtime.getDeliveries(),
  }))
  .get("/workloads", () => ({
    service: config.serviceName,
    supervisor: supervisor.getSummary(),
    workloads: supervisor.getWorkloads(),
  }))
  .get("/manifests/:connectorId", ({ params, set }) => {
    const manifest = findManifest(manifests, params.connectorId);

    if (!manifest) {
      set.status = 404;
      return {
        ok: false,
        error: `Unknown connector manifest: ${params.connectorId}`,
      };
    }

    return {
      ok: true,
      manifest,
    };
  })
  .post(
    "/connectors/test",
    ({ body, set }) => {
      const connectorRef = body.connectorId ?? body.capabilityId;
      if (!connectorRef) {
        set.status = 400;
        return {
          ok: false,
          error: "connectorId or capabilityId is required",
        };
      }

      return {
        ok: true,
        service: config.serviceName,
        connectorId: connectorRef,
        executionMode: "adapter",
        image: config.redpandaConnectImage,
        requestedDirection: body.direction,
        resolvedManifest: findManifestForConnectorRef(
          manifests,
          body.capabilityId ?? connectorRef,
        ) ?? null,
        runtime: runtime.getSummary(),
        message:
          body.capabilityId === "snowflake_sink"
            || body.capabilityId === "bigquery_sink"
            || body.capabilityId === "s3_sink"
            ? "Adapter runtime is available. Warehouse and object-storage sinks now materialize local artifacts through the adapter path; cloud credential validation is still out of scope."
            : "Adapter runtime is available. Connector tests currently validate manifest resolution and adapter ownership, not external broker credentials.",
      };
    },
    {
      body: t.Object({
        connectorId: t.Optional(t.String()),
        capabilityId: t.Optional(t.String()),
        direction: t.Union([
          t.Literal("source"),
          t.Literal("sink"),
          t.Literal("bidirectional"),
        ]),
      }),
    },
  );

if (import.meta.main) {
  const server = Bun.serve({
    hostname: config.host,
    port: config.port,
    fetch: app.fetch,
  });
  let shuttingDown = false;

  const shutdown = async (signal: string) => {
    if (shuttingDown) {
      return;
    }

    shuttingDown = true;
    console.log(`[${config.serviceName}] received ${signal}, shutting down`);
    server.stop(true);
    await runtime.stop();
    await supervisor.stop();
    process.exit(0);
  };

  process.on("SIGTERM", () => {
    void shutdown("SIGTERM");
  });
  process.on("SIGINT", () => {
    void shutdown("SIGINT");
  });

  console.log(
    `[${config.serviceName}] listening on http://${server.hostname}:${server.port}`,
  );
}

export type AdapterRedpandaApp = typeof app;
export { app };
