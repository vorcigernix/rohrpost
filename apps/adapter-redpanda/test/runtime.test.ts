import { afterEach, describe, expect, it } from "bun:test";
import { generateKeyPairSync } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { dirname, join } from "node:path";
import { tmpdir } from "node:os";
import {
  AdapterRedpandaRuntime,
  selectAdapterRuntimeConsumersToDelete,
  setBigQueryStorageWriterFactoryForTest,
  shouldTerminateWorkMessage,
  workNakDelayMs,
} from "../src/runtime";
import type { AdapterRedpandaConfig } from "../src/config";

const tempDirs: string[] = [];
const originalFetch = globalThis.fetch;

afterEach(() => {
  globalThis.fetch = originalFetch;
  setBigQueryStorageWriterFactoryForTest();
  while (tempDirs.length > 0) {
    const dir = tempDirs.pop();
    if (dir) {
      rmSync(dir, { recursive: true, force: true });
    }
  }
});

function buildConfig(deliveryLogPath: string): AdapterRedpandaConfig {
  const artifactRoot = join(dirname(deliveryLogPath), "artifacts");
  return {
    host: "127.0.0.1",
    port: 3003,
    redpandaConnectImage: "example/connect:latest",
    manifestSource: "test",
    connectBackend: "disabled",
    connectPollIntervalMs: 5_000,
    connectDockerBinary: "docker",
    connectWorkdir: join(dirname(deliveryLogPath), "connect-workdir"),
    connectKubernetesNamespace: "rohrpost",
    connectKubernetesServiceAccountName: "adapter-redpanda-connect",
    connectKubernetesConfigMapPrefix: "redpanda-connect",
    connectKubernetesDeploymentPrefix: "redpanda-connect",
    connectKubernetesImagePullPolicy: "IfNotPresent",
    connectKubernetesArtifactVolumeClaimName: "adapter-redpanda-artifacts",
    serviceName: "adapter-redpanda",
    natsUrl: undefined,
    controlApiUrl: undefined,
    controlApiToken: undefined,
    deliveryLogPath,
    deliveryLogEnabled: true,
    artifactRoot,
  };
}

describe("adapter runtime", () => {
  it("selects stale work consumers that block the narrowed inline work filter", () => {
    expect(
      selectAdapterRuntimeConsumersToDelete(
        [
          {
            name: "adapter_redpanda_work",
            config: { filter_subject: "router.work.>" },
          },
          {
            name: "adapter_redpanda_work_v2",
            config: { filter_subject: "router.work.>" },
          },
          {
            name: "adapter_redpanda_work_v2",
            config: {
              filter_subject: "router.work.inline.>",
              ack_wait: 60_000_000_000,
              max_ack_pending: 1024,
              max_deliver: 5,
            },
          },
          {
            name: "adapter_redpanda_work_v2",
            config: {
              filter_subject: "router.work.inline.>",
              ack_wait: 30_000_000_000,
              max_ack_pending: 1000,
              max_deliver: 5,
            },
          },
          {
            name: "redpanda_connect_s3",
            config: { filter_subject: "router.work.connect.s3_sink_default.>" },
          },
        ],
        "adapter_redpanda_work_v2",
        "router.work.inline.>",
      ),
    ).toEqual(["adapter_redpanda_work", "adapter_redpanda_work_v2", "adapter_redpanda_work_v2"]);
  });

  it("records kafka sink work items and writes them to the delivery log", async () => {
    const dir = mkdtempSync(join(tmpdir(), "adapter-runtime-"));
    tempDirs.push(dir);
    const logPath = join(dir, "deliveries.jsonl");
    const runtime = new AdapterRedpandaRuntime(buildConfig(logPath));

    const record = await runtime.processWorkItem({
      workId: "work-1",
      runId: "run-1",
      enqueuedAt: "2026-04-01T00:00:00.000Z",
      deploymentId: "deploy-1",
      flowId: "flow-1",
      revisionId: "rev-1",
      sinkId: "sink-1",
      connectorId: "kafka_out_default",
      capabilityId: "kafka_out",
      tenantId: "tenant-a",
      attempt: 1,
      sourceRef: "manual",
      traceId: "trace-1",
      messageId: "message-1",
      connectorConfig: {
        topic: "orders.processed",
      },
      envelope: {
        tenantId: "tenant-a",
        flowId: "flow-1",
        revisionId: "rev-1",
        messageId: "message-1",
        sourceRef: "manual",
        partitionKey: "tenant-a",
        headers: {},
        payload: { orderId: "order-1" },
        receivedAt: "2026-04-01T00:00:00.000Z",
        traceId: "trace-1",
      },
      payload: { orderId: "order-1" },
    });

    expect(record.status).toBe("delivered");
    expect(record.topic).toBe("orders.processed");
    expect(record.mirrorSubject).toBe("adapter.kafka.orders_processed");
    expect(runtime.getSummary().deliveries).toBe(1);

    const logContents = readFileSync(logPath, "utf8");
    expect(logContents).toContain("\"workId\":\"work-1\"");
    expect(logContents).toContain("\"mirrorSubject\":\"adapter.kafka.orders_processed\"");
  });

  it("materializes snowflake sink work items into an idempotent table artifact", async () => {
    const dir = mkdtempSync(join(tmpdir(), "adapter-runtime-"));
    tempDirs.push(dir);
    const logPath = join(dir, "deliveries.jsonl");
    const runtime = new AdapterRedpandaRuntime(buildConfig(logPath));

    const workItem = {
      workId: "work-snowflake-1",
      runId: "run-snowflake-1",
      enqueuedAt: "2026-04-01T00:00:00.000Z",
      deploymentId: "deploy-1",
      flowId: "flow-1",
      revisionId: "rev-1",
      sinkId: "sink-snowflake",
      connectorId: "snowflake_sink_default",
      capabilityId: "snowflake_sink" as const,
      tenantId: "tenant-a",
      attempt: 1,
      sourceRef: "manual",
      traceId: "trace-snowflake-1",
      messageId: "message-snowflake-1",
      connectorConfig: {
        account: "acme-account",
        database: "analytics",
        schema: "raw",
        table: "people_events",
      },
      envelope: {
        tenantId: "tenant-a",
        flowId: "flow-1",
        revisionId: "rev-1",
        messageId: "message-snowflake-1",
        sourceRef: "manual",
        partitionKey: "tenant-a",
        headers: {},
        payload: { email: "ada@example.com" },
        receivedAt: "2026-04-01T00:00:00.000Z",
        traceId: "trace-snowflake-1",
      },
      payload: { email: "ada@example.com" },
    };

    const firstRecord = await runtime.processWorkItem(workItem);
    const secondRecord = await runtime.processWorkItem({
      ...workItem,
      workId: "work-snowflake-2",
    });

    expect(firstRecord.status).toBe("delivered");
    expect(firstRecord.targetRef).toBe("snowflake://acme-account/analytics/raw/people_events");
    expect(firstRecord.artifactPath).toBeTruthy();
    expect(secondRecord.deduplicated).toBe(true);

    const artifactContents = readFileSync(firstRecord.artifactPath!, "utf8").trim().split("\n");
    expect(artifactContents).toHaveLength(1);
    expect(artifactContents[0]).toContain("\"messageId\":\"message-snowflake-1\"");
  });

  it("materializes bigquery and s3 sink work items into adapter artifacts", async () => {
    const dir = mkdtempSync(join(tmpdir(), "adapter-runtime-"));
    tempDirs.push(dir);
    const logPath = join(dir, "deliveries.jsonl");
    const runtime = new AdapterRedpandaRuntime(buildConfig(logPath));

    const bigQueryRecord = await runtime.processWorkItem({
      workId: "work-bigquery-1",
      runId: "run-bigquery-1",
      enqueuedAt: "2026-04-01T00:00:00.000Z",
      deploymentId: "deploy-1",
      flowId: "flow-1",
      revisionId: "rev-1",
      sinkId: "sink-bigquery",
      connectorId: "bigquery_sink_default",
      capabilityId: "bigquery_sink",
      tenantId: "tenant-a",
      attempt: 1,
      sourceRef: "manual",
      traceId: "trace-bigquery-1",
      messageId: "message-bigquery-1",
      connectorConfig: {
        project: "demo-project",
        dataset: "event_router",
        table: "processed_events",
      },
      envelope: {
        tenantId: "tenant-a",
        flowId: "flow-1",
        revisionId: "rev-1",
        messageId: "message-bigquery-1",
        sourceRef: "manual",
        partitionKey: "tenant-a",
        headers: {},
        payload: { country: "CZ" },
        receivedAt: "2026-04-01T00:00:00.000Z",
        traceId: "trace-bigquery-1",
      },
      payload: { country: "CZ" },
    });

    const s3Record = await runtime.processWorkItem({
      workId: "work-s3-1",
      runId: "run-s3-1",
      enqueuedAt: "2026-04-01T00:00:00.000Z",
      deploymentId: "deploy-1",
      flowId: "flow-1",
      revisionId: "rev-1",
      sinkId: "sink-s3",
      connectorId: "s3_sink_default",
      capabilityId: "s3_sink",
      tenantId: "tenant-a",
      attempt: 1,
      sourceRef: "manual",
      traceId: "trace-s3-1",
      messageId: "message-s3-1",
      connectorConfig: {
        bucket: "event-router-v1",
        prefix: "exports/people",
      },
      envelope: {
        tenantId: "tenant-a",
        flowId: "flow-1",
        revisionId: "rev-1",
        messageId: "message-s3-1",
        sourceRef: "manual",
        partitionKey: "tenant-a",
        headers: {},
        payload: { city: "Prague" },
        receivedAt: "2026-04-01T12:34:56.000Z",
        traceId: "trace-s3-1",
      },
      payload: { city: "Prague" },
    });

    expect(bigQueryRecord.targetRef).toBe("bigquery://demo-project/event_router.processed_events");
    expect(readFileSync(bigQueryRecord.artifactPath!, "utf8")).toContain("\"messageId\":\"message-bigquery-1\"");

    expect(s3Record.targetRef).toContain("s3://event-router-v1/");
    expect(s3Record.objectKey).toContain("exports");
    expect(readFileSync(s3Record.artifactPath!, "utf8")).toContain("\"messageId\": \"message-s3-1\"");
  });

  it("inserts bigquery sink work items when service account credentials are configured", async () => {
    const dir = mkdtempSync(join(tmpdir(), "adapter-runtime-"));
    tempDirs.push(dir);
    const logPath = join(dir, "deliveries.jsonl");
    const runtime = new AdapterRedpandaRuntime(buildConfig(logPath));
    const { privateKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
    const privateKeyPem = privateKey.export({ type: "pkcs8", format: "pem" }).toString();
    const calls: Array<{ url: string; init?: RequestInit }> = [];

    globalThis.fetch = (async (url: string | URL | Request, init?: RequestInit) => {
      calls.push({ url: String(url), init });
      if (String(url).includes("oauth2.googleapis.com/token")) {
        return new Response(JSON.stringify({ access_token: "test-access-token", expires_in: 3600 }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }
      return new Response("{}", {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }) as typeof fetch;

    const record = await runtime.processWorkItem({
      workId: "work-bigquery-api-1",
      runId: "run-bigquery-api-1",
      enqueuedAt: "2026-04-01T00:00:00.000Z",
      deploymentId: "deploy-1",
      flowId: "flow-1",
      revisionId: "rev-1",
      sinkId: "sink-bigquery",
      connectorId: "bigquery_sink_default",
      capabilityId: "bigquery_sink",
      tenantId: "tenant-a",
      attempt: 1,
      sourceRef: "manual",
      traceId: "trace-bigquery-api-1",
      messageId: "message-bigquery-api-1",
      connectorConfig: {
        writeMethod: "insertAll",
        project: "demo-project",
        jobProject: "billing-project",
        dataset: "event_router",
        table: "processed_events",
        credentialsJson: JSON.stringify({
          type: "service_account",
          client_email: "writer@example.iam.gserviceaccount.com",
          private_key: privateKeyPem,
          token_uri: "https://oauth2.googleapis.com/token",
        }),
      },
      envelope: {
        tenantId: "tenant-a",
        flowId: "flow-1",
        revisionId: "rev-1",
        messageId: "message-bigquery-api-1",
        sourceRef: "manual",
        partitionKey: "tenant-a",
        headers: {},
        payload: { country: "CZ" },
        receivedAt: "2026-04-01T00:00:00.000Z",
        traceId: "trace-bigquery-api-1",
      },
      payload: { country: "CZ" },
    });

    expect(record.status).toBe("delivered");
    expect(record.targetRef).toBe("bigquery://demo-project/event_router.processed_events");
    expect(calls).toHaveLength(2);
    expect(calls[1]?.url).toBe(
      "https://bigquery.googleapis.com/bigquery/v2/projects/billing-project/datasets/event_router/tables/processed_events/insertAll",
    );
    expect(calls[1]?.init?.headers).toMatchObject({
      authorization: "Bearer test-access-token",
      "content-type": "application/json",
    });
    expect(JSON.parse(String(calls[1]?.init?.body))).toMatchObject({
      ignoreUnknownValues: true,
      skipInvalidRows: false,
      rows: [{ insertId: "message-bigquery-api-1", json: { country: "CZ" } }],
    });

    const logContents = readFileSync(logPath, "utf8");
    expect(logContents).toContain("\"credentialsJson\":\"[redacted]\"");
    expect(logContents).not.toContain("BEGIN PRIVATE KEY");
  });

  it("batches bigquery sink work items with the Storage Write API writer", async () => {
    const dir = mkdtempSync(join(tmpdir(), "adapter-runtime-"));
    tempDirs.push(dir);
    const logPath = join(dir, "deliveries.jsonl");
    const runtime = new AdapterRedpandaRuntime(buildConfig(logPath));
    const batches: Array<Array<Record<string, unknown>>> = [];
    const targets: Array<Record<string, unknown>> = [];

    setBigQueryStorageWriterFactoryForTest((target) => {
      targets.push({
        project: target.project,
        jobProject: target.jobProject,
        dataset: target.dataset,
        table: target.table,
        batchCount: target.batchCount,
        maxInFlightBatches: target.maxInFlightBatches,
      });
      return {
        async appendRows(rows) {
          batches.push(rows);
        },
        close() {
          return undefined;
        },
      };
    });

    const baseWorkItem = {
      runId: "run-bigquery-storage-1",
      enqueuedAt: "2026-04-01T00:00:00.000Z",
      deploymentId: "deploy-1",
      flowId: "flow-1",
      revisionId: "rev-1",
      sinkId: "sink-bigquery",
      connectorId: "bigquery_sink_default",
      capabilityId: "bigquery_sink" as const,
      tenantId: "tenant-a",
      attempt: 1,
      sourceRef: "manual",
      connectorConfig: {
        project: "demo-project",
        jobProject: "billing-project",
        dataset: "event_router",
        table: "processed_events",
        batchCount: 2,
        batchPeriod: "5s",
        maxInFlight: 2,
        credentialsJson: JSON.stringify({
          type: "service_account",
          client_email: "writer@example.iam.gserviceaccount.com",
          private_key: "-----BEGIN PRIVATE KEY-----\nfake\n-----END PRIVATE KEY-----\n",
          token_uri: "https://oauth2.googleapis.com/token",
        }),
      },
    };

    const [firstRecord, secondRecord] = await Promise.all([
      runtime.processWorkItem({
        ...baseWorkItem,
        workId: "work-bigquery-storage-1",
        traceId: "trace-bigquery-storage-1",
        messageId: "message-bigquery-storage-1",
        envelope: {
          tenantId: "tenant-a",
          flowId: "flow-1",
          revisionId: "rev-1",
          messageId: "message-bigquery-storage-1",
          sourceRef: "manual",
          partitionKey: "tenant-a",
          headers: {},
          payload: { country: "CZ" },
          receivedAt: "2026-04-01T00:00:00.000Z",
          traceId: "trace-bigquery-storage-1",
        },
        payload: { country: "CZ" },
      }),
      runtime.processWorkItem({
        ...baseWorkItem,
        workId: "work-bigquery-storage-2",
        traceId: "trace-bigquery-storage-2",
        messageId: "message-bigquery-storage-2",
        envelope: {
          tenantId: "tenant-a",
          flowId: "flow-1",
          revisionId: "rev-1",
          messageId: "message-bigquery-storage-2",
          sourceRef: "manual",
          partitionKey: "tenant-a",
          headers: {},
          payload: { country: "SK" },
          receivedAt: "2026-04-01T00:00:00.000Z",
          traceId: "trace-bigquery-storage-2",
        },
        payload: { country: "SK" },
      }),
    ]);

    expect(firstRecord.status).toBe("delivered");
    expect(secondRecord.status).toBe("delivered");
    expect(targets).toEqual([
      {
        project: "demo-project",
        jobProject: "billing-project",
        dataset: "event_router",
        table: "processed_events",
        batchCount: 2,
        maxInFlightBatches: 2,
      },
    ]);
    expect(batches).toEqual([[{ country: "CZ" }, { country: "SK" }]]);
  });
});

describe("shouldTerminateWorkMessage", () => {
  it("terminates once redelivery count reaches the cap", () => {
    expect(shouldTerminateWorkMessage(5, 5)).toBe(true);
    expect(shouldTerminateWorkMessage(6, 5)).toBe(true);
  });

  it("naks below the cap", () => {
    expect(shouldTerminateWorkMessage(1, 5)).toBe(false);
    expect(shouldTerminateWorkMessage(4, 5)).toBe(false);
  });
});

describe("workNakDelayMs", () => {
  it("scales the nak delay with the delivery count", () => {
    expect(workNakDelayMs(1)).toBe(5_000);
    expect(workNakDelayMs(4)).toBe(20_000);
  });

  it("floors the delivery count at 1", () => {
    expect(workNakDelayMs(0)).toBe(5_000);
  });
});

describe("nakOrTerminate wiring", () => {
  function buildRuntime(): AdapterRedpandaRuntime {
    const dir = mkdtempSync(join(tmpdir(), "adapter-runtime-"));
    tempDirs.push(dir);
    const logPath = join(dir, "deliveries.jsonl");
    return new AdapterRedpandaRuntime(buildConfig(logPath));
  }

  it("terminates the message once redelivery count reaches the cap", () => {
    const runtime = buildRuntime();
    let termCalls = 0;
    let nakCalls: number[] = [];
    const jsMessage = {
      info: { redeliveryCount: 5, streamSequence: 1 },
      term: () => {
        termCalls += 1;
      },
      nak: (delay: number) => {
        nakCalls.push(delay);
      },
    };

    (runtime as any).nakOrTerminate(jsMessage);

    expect(termCalls).toBe(1);
    expect(nakCalls).toEqual([]);
  });

  it("naks with an escalated delay below the cap", () => {
    const runtime = buildRuntime();
    let termCalls = 0;
    let nakCalls: number[] = [];
    const jsMessage = {
      info: { redeliveryCount: 2, streamSequence: 2 },
      term: () => {
        termCalls += 1;
      },
      nak: (delay: number) => {
        nakCalls.push(delay);
      },
    };

    (runtime as any).nakOrTerminate(jsMessage);

    expect(termCalls).toBe(0);
    expect(nakCalls).toEqual([10_000]);
  });
});

describe("adapter consumer max_deliver drift", () => {
  it("recreates the work consumer when max_deliver drifts", () => {
    const consumers = [
      {
        name: "adapter_redpanda_work_v2",
        config: {
          filter_subject: "router.work.>",
          ack_wait: 60_000 * 1_000_000,
          max_ack_pending: 1_024,
          max_deliver: 3,
        },
      },
    ];

    expect(
      selectAdapterRuntimeConsumersToDelete(consumers, "adapter_redpanda_work_v2", "router.work.>"),
    ).toEqual(["adapter_redpanda_work_v2"]);
  });
});
