import { loadRuntimeManagerConfig } from "./config";
import { createRuntimeManagerApp } from "./app";

const config = loadRuntimeManagerConfig();
const app = createRuntimeManagerApp({
  config,
  startBackgroundRefresh: true,
});

if (import.meta.main) {
  const server = Bun.serve({
    hostname: config.host,
    port: config.port,
    fetch: app.fetch,
  });

  console.log(
    `[${config.serviceName}] listening on http://${server.hostname}:${server.port}`,
  );
}

export type RuntimeManagerApp = typeof app;
export { app };
