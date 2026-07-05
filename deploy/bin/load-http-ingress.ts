import { readFileSync } from "node:fs";

type SizeClass = "small" | "medium" | "large";

interface EventResult {
  ok: boolean;
  status: number;
  latencyMs: number;
  sizeClass: SizeClass;
  bytes: number;
  error?: string;
}

interface RunSummary {
  runId: string;
  deploymentId: string;
  routerUrl: string;
  sinkFile: string;
  ratesPerSecond: number[];
  stepSeconds: number;
  targetEvents: number;
  attempted: number;
  accepted: number;
  failed: number;
  achievedIngressRps: number;
  sendDurationMs: number;
  totalDurationMs: number;
  ingressLatencyMs: {
    min: number;
    p50: number;
    p95: number;
    p99: number;
    max: number;
    avg: number;
  };
  bySize: Record<SizeClass, {
    attempted: number;
    accepted: number;
    failed: number;
    avgLatencyMs: number;
  }>;
  sinkDelivered: number;
  sinkMissing: number;
  routerSummary: unknown;
  dlqCount: number;
  startedAt: string;
  finishedAt: string;
}

function parseInteger(value: string | undefined, fallback: number): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : fallback;
}

function parseRates(value: string | undefined): number[] {
  const raw = value ?? "800,1000,1200";
  const parsed = raw
    .split(",")
    .map((part) => Number(part.trim()))
    .filter((part) => Number.isFinite(part) && part > 0)
    .map((part) => Math.floor(part));

  if (parsed.length === 0) {
    throw new Error(`Invalid LOAD_TEST_RATES value: ${raw}`);
  }

  return parsed;
}

function percentile(sorted: number[], ratio: number): number {
  if (sorted.length === 0) return 0;
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * ratio) - 1));
  return sorted[index] ?? 0;
}

function average(values: number[]): number {
  if (values.length === 0) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function countSinkDeliveries(path: string, runId: string): number {
  try {
    const text = readFileSync(path, "utf8");
    return text
      .split("\n")
      .filter((line) => line.includes(`"loadRunId":"${runId}"`))
      .length;
  } catch {
    return 0;
  }
}

function sizeClassFor(sequence: number): SizeClass {
  if (sequence % 10 === 0) return "large";
  if (sequence % 3 === 0) return "medium";
  return "small";
}

function targetBytesFor(sizeClass: SizeClass): number {
  switch (sizeClass) {
    case "medium":
      return parseInteger(process.env.LOAD_TEST_MEDIUM_BYTES, 4_096);
    case "large":
      return parseInteger(process.env.LOAD_TEST_LARGE_BYTES, 32_768);
    default:
      return parseInteger(process.env.LOAD_TEST_SMALL_BYTES, 512);
  }
}

function buildPayload(runId: string, sequence: number): {
  payload: Record<string, unknown>;
  sizeClass: SizeClass;
  bytes: number;
} {
  const sizeClass = sizeClassFor(sequence);
  const targetBytes = targetBytesFor(sizeClass);
  const payload: Record<string, unknown> = {
    loadRunId: runId,
    sequence,
    sizeClass,
    issuedAt: new Date().toISOString(),
    orderId: `${runId}-order-${sequence}`,
    customerId: `customer-${sequence % 500}`,
    amount: (sequence % 997) + 1,
    pii: {
      email: `load-${sequence}@example.com`,
    },
    tags: {
      scenario: "mixed-http-load",
      rateProfile: process.env.LOAD_TEST_RATES ?? "800,1000,1200",
    },
  };

  const baseBytes = Buffer.byteLength(JSON.stringify(payload));
  const fillerBytes = Math.max(0, targetBytes - baseBytes);
  if (fillerBytes > 0) {
    payload.blob = "x".repeat(fillerBytes);
  }

  return {
    payload,
    sizeClass,
    bytes: Buffer.byteLength(JSON.stringify(payload)),
  };
}

async function postEvent(
  routerUrl: string,
  deploymentId: string,
  runId: string,
  sequence: number,
): Promise<EventResult> {
  const { payload, sizeClass, bytes } = buildPayload(runId, sequence);
  const startedAt = performance.now();

  try {
    const response = await fetch(`${routerUrl}/ingress`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({
        deploymentId,
        messageId: `${runId}-${sequence}`,
        traceId: `${runId}-${sequence}`,
        partitionKey: `tenant-${sequence % 32}`,
        payload,
      }),
    });
    const latencyMs = performance.now() - startedAt;

    return {
      ok: response.ok,
      status: response.status,
      latencyMs,
      sizeClass,
      bytes,
      error: response.ok ? undefined : await response.text().catch(() => ""),
    };
  } catch (error) {
    return {
      ok: false,
      status: 0,
      latencyMs: performance.now() - startedAt,
      sizeClass,
      bytes,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

async function sleepUntil(timestampMs: number): Promise<void> {
  const waitMs = timestampMs - performance.now();
  if (waitMs > 0) {
    await Bun.sleep(waitMs);
  }
}

async function main() {
  const deploymentId = process.env.LOAD_TEST_DEPLOYMENT_ID;
  if (!deploymentId) {
    throw new Error("LOAD_TEST_DEPLOYMENT_ID is required");
  }

  const routerUrl = process.env.LOAD_TEST_ROUTER_URL ?? "http://127.0.0.1:3002";
  const sinkFile = process.env.LOAD_TEST_SINK_FILE ?? "/tmp/rohrpost-http-sink.jsonl";
  const ratesPerSecond = parseRates(process.env.LOAD_TEST_RATES);
  const stepSeconds = parseInteger(process.env.LOAD_TEST_STEP_SECONDS, 5);
  const settleSeconds = parseInteger(process.env.LOAD_TEST_SETTLE_SECONDS, 10);
  const tickMs = parseInteger(process.env.LOAD_TEST_TICK_MS, 100);
  const maxInflight = parseInteger(process.env.LOAD_TEST_MAX_INFLIGHT, 400);
  const runId = process.env.LOAD_TEST_RUN_ID ?? `load-${Date.now().toString(36)}`;
  const startedAt = new Date().toISOString();

  const targetEvents = ratesPerSecond.reduce((sum, rate) => sum + (rate * stepSeconds), 0);
  const baselineDeliveries = countSinkDeliveries(sinkFile, runId);
  const active = new Set<Promise<EventResult>>();
  const results: EventResult[] = [];
  let sequence = 0;
  const sendStart = performance.now();
  let tickDeadline = sendStart;

  for (const rate of ratesPerSecond) {
    const perTickBase = Math.floor(rate / (1_000 / tickMs));
    const ticksPerSecond = Math.floor(1_000 / tickMs);
    const remainder = rate - (perTickBase * ticksPerSecond);

    for (let secondIndex = 0; secondIndex < stepSeconds; secondIndex += 1) {
      for (let tick = 0; tick < ticksPerSecond; tick += 1) {
        const requestCount = perTickBase + (tick < remainder ? 1 : 0);

        for (let count = 0; count < requestCount; count += 1) {
          while (active.size >= maxInflight) {
            await Promise.race(active);
          }

          sequence += 1;
          const task = postEvent(routerUrl, deploymentId, runId, sequence)
            .then((result) => {
              results.push(result);
              return result;
            })
            .finally(() => {
              active.delete(task);
            });

          active.add(task);
        }

        tickDeadline += tickMs;
        await sleepUntil(tickDeadline);
      }
    }
  }

  await Promise.all(active);
  const sendDurationMs = performance.now() - sendStart;

  await Bun.sleep(settleSeconds * 1_000);
  const routerSummary = await fetch(`${routerUrl}/status`).then((response) => response.json()).catch(() => null);
  const dlqCount = await fetch(`${routerUrl}/dlq`)
    .then((response) => response.json())
    .then((records) => Array.isArray(records) ? records.length : 0)
    .catch(() => 0);

  const delivered = countSinkDeliveries(sinkFile, runId) - baselineDeliveries;
  const finishedAt = new Date().toISOString();
  const totalDurationMs = performance.now() - sendStart;
  const latencies = results.map((result) => result.latencyMs).sort((left, right) => left - right);
  const accepted = results.filter((result) => result.ok).length;
  const failed = results.length - accepted;
  const bySize = {
    small: { attempted: 0, accepted: 0, failed: 0, avgLatencyMs: 0 },
    medium: { attempted: 0, accepted: 0, failed: 0, avgLatencyMs: 0 },
    large: { attempted: 0, accepted: 0, failed: 0, avgLatencyMs: 0 },
  } satisfies RunSummary["bySize"];

  for (const sizeClass of ["small", "medium", "large"] as const) {
    const subset = results.filter((result) => result.sizeClass === sizeClass);
    bySize[sizeClass] = {
      attempted: subset.length,
      accepted: subset.filter((result) => result.ok).length,
      failed: subset.filter((result) => !result.ok).length,
      avgLatencyMs: average(subset.map((result) => result.latencyMs)),
    };
  }

  const summary: RunSummary = {
    runId,
    deploymentId,
    routerUrl,
    sinkFile,
    ratesPerSecond,
    stepSeconds,
    targetEvents,
    attempted: results.length,
    accepted,
    failed,
    achievedIngressRps: results.length / (sendDurationMs / 1_000),
    sendDurationMs,
    totalDurationMs,
    ingressLatencyMs: {
      min: latencies[0] ?? 0,
      p50: percentile(latencies, 0.5),
      p95: percentile(latencies, 0.95),
      p99: percentile(latencies, 0.99),
      max: latencies[latencies.length - 1] ?? 0,
      avg: average(latencies),
    },
    bySize,
    sinkDelivered: delivered,
    sinkMissing: accepted - delivered,
    routerSummary,
    dlqCount,
    startedAt,
    finishedAt,
  };

  console.log(JSON.stringify(summary, null, 2));
}

await main();
