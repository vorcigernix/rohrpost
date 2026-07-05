import { describe, expect, it } from "bun:test";

import { loadRuntimeManagerConfig } from "../src/config";

describe("loadRuntimeManagerConfig", () => {
  it("applies defaults when env is empty", () => {
    const config = loadRuntimeManagerConfig({});

    expect(config.host).toBe("0.0.0.0");
    expect(config.port).toBe(7102);
    expect(config.controlApiUrl).toBe("http://127.0.0.1:3001");
    expect(config.controlApiToken).toBe("dev-admin-token");
    expect(config.requestTimeoutMs).toBe(3000);
    expect(config.routerWorkersUrl).toBe("http://127.0.0.1:3002");
    expect(config.adapterRedpandaUrl).toBe("http://127.0.0.1:3003");
    expect(config.tenantId).toBe("tenant-local");
    expect(config.snapshotRefreshMs).toBe(5000);
  });

  it("reads explicit overrides", () => {
    const config = loadRuntimeManagerConfig({
      RUNTIME_MANAGER_HOST: "127.0.0.1",
      RUNTIME_MANAGER_PORT: "8123",
      CONTROL_API_URL: "http://localhost:9999",
      CONTROL_API_TOKEN: "test-token",
      RUNTIME_MANAGER_REQUEST_TIMEOUT_MS: "2300",
      RUNTIME_MANAGER_ROUTER_WORKERS_URL: "http://router:3002",
      RUNTIME_MANAGER_ADAPTER_REDPANDA_URL: "http://adapter:3003",
      TENANT_ID: "tenant-acme",
      RUNTIME_MANAGER_SNAPSHOT_REFRESH_MS: "1200",
    });

    expect(config.host).toBe("127.0.0.1");
    expect(config.port).toBe(8123);
    expect(config.controlApiUrl).toBe("http://localhost:9999");
    expect(config.controlApiToken).toBe("test-token");
    expect(config.requestTimeoutMs).toBe(2300);
    expect(config.routerWorkersUrl).toBe("http://router:3002");
    expect(config.adapterRedpandaUrl).toBe("http://adapter:3003");
    expect(config.tenantId).toBe("tenant-acme");
    expect(config.snapshotRefreshMs).toBe(1200);
  });

  it("rejects invalid numeric values", () => {
    expect(() =>
      loadRuntimeManagerConfig({
        RUNTIME_MANAGER_REQUEST_TIMEOUT_MS: "0",
      }),
    ).toThrow("Invalid numeric value");

    expect(() =>
      loadRuntimeManagerConfig({
        RUNTIME_MANAGER_SNAPSHOT_REFRESH_MS: "-1",
      }),
    ).toThrow("Invalid numeric value");
  });
});
