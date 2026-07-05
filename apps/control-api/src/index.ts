import { loadConfig } from "./config";
import { createApp } from "./app";

const config = loadConfig();
const app = createApp({ config }).listen({
  hostname: config.host,
  port: config.port,
});

console.log(`control-api listening on http://${app.server?.hostname}:${app.server?.port}`);
