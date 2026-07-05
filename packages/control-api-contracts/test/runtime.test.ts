import { describe, expect, it } from "bun:test";
import { createControlApiClient, controlApiPaths } from "../src";

describe("createControlApiClient", () => {
  it("posts adapter workload statuses through the shared contract path", async () => {
    const requests: Array<{ url: string; init: RequestInit | undefined }> = [];
    const client = createControlApiClient({
      baseUrl: "https://control-api.test/",
      token: "test-token",
      fetchImpl: (async (url: string | URL | Request, init?: RequestInit) => {
        requests.push({ url: String(url), init });
        return new Response(JSON.stringify({ updated: 1 }), {
          status: 202,
          headers: { "content-type": "application/json" },
        });
      }) as typeof fetch,
    });

    await expect(client.replaceAdapterWorkloadStatuses({
      reporterId: "adapter-redpanda",
      reportedAt: "2026-04-07T08:00:00.000Z",
      workloads: [
        {
          key: "sink:s3_sink_default",
          connectorId: "s3_sink_default",
          capabilityId: "s3_sink",
          manifestId: "s3-sink",
          deploymentIds: ["deploy-1"],
          flowIds: ["flow-1"],
          revisionIds: ["rev-1"],
          runtimeRole: "sink",
          inputRef: "router.work.connect.s3_sink_default.>",
          outputRef: "file:///tmp/out.ndjson",
          status: "running",
          backend: "docker",
          targetKind: "file",
          configPath: "/tmp/connect.json",
          restartCount: 0,
          recentLogs: [],
        },
      ],
    })).resolves.toEqual({ updated: 1 });

    expect(requests).toHaveLength(1);
    expect(requests[0]?.url).toBe(`https://control-api.test${controlApiPaths.adapterWorkloads()}`);
    expect(requests[0]?.init?.method).toBe("POST");
    expect(requests[0]?.init?.headers).toMatchObject({
      Authorization: "Bearer test-token",
      "Content-Type": "application/json",
    });
    expect(JSON.parse(String(requests[0]?.init?.body))).toMatchObject({
      reporterId: "adapter-redpanda",
      workloads: [{ connectorId: "s3_sink_default" }],
    });
  });
});
