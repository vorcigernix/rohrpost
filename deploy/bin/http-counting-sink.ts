import { writeFileSync } from "node:fs";

interface RunCount {
  count: number;
  bytes: number;
  lastReceivedAt: string;
}

const host = process.env.COUNTING_SINK_HOST ?? "0.0.0.0";
const port = Number(process.env.COUNTING_SINK_PORT ?? "4011");
const stateFile = process.env.COUNTING_SINK_STATE_FILE ?? "/tmp/rohrpost-http-sink-stats.json";

const startedAt = new Date().toISOString();
let totalCount = 0;
let totalBytes = 0;
const byRunId = new Map<string, RunCount>();

function extractRunId(payload: unknown): string | undefined {
  if (!payload || typeof payload !== "object") {
    return undefined;
  }

  const record = payload as Record<string, unknown>;
  const direct = record.loadRunId;
  if (typeof direct === "string" && direct) {
    return direct;
  }

  if (record.payload && typeof record.payload === "object") {
    const nested = (record.payload as Record<string, unknown>).loadRunId;
    if (typeof nested === "string" && nested) {
      return nested;
    }
  }

  if (record.envelope && typeof record.envelope === "object") {
    const envelope = record.envelope as Record<string, unknown>;
    if (envelope.payload && typeof envelope.payload === "object") {
      const nested = (envelope.payload as Record<string, unknown>).loadRunId;
      if (typeof nested === "string" && nested) {
        return nested;
      }
    }
  }

  return undefined;
}

function snapshot() {
  return {
    ok: true,
    startedAt,
    totalCount,
    totalBytes,
    byRunId: Object.fromEntries(byRunId.entries()),
  };
}

function persistState() {
  writeFileSync(stateFile, JSON.stringify(snapshot(), null, 2));
}

const server = Bun.serve({
  hostname: host,
  port,
  async fetch(request) {
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/health") {
      return Response.json({ ok: true, startedAt });
    }

    if (request.method === "GET" && url.pathname === "/status") {
      return Response.json(snapshot());
    }

    if (request.method === "POST" && url.pathname === "/reset") {
      const body = await request.json().catch(() => ({})) as { runId?: string };
      const runId = typeof body.runId === "string" && body.runId ? body.runId : url.searchParams.get("runId");

      if (runId) {
        byRunId.delete(runId);
      } else {
        totalCount = 0;
        totalBytes = 0;
        byRunId.clear();
      }

      persistState();
      return Response.json({
        ok: true,
        reset: runId ? "run" : "all",
        runId,
      });
    }

    if (request.method === "POST") {
      const body = await request.text();
      const bytes = Buffer.byteLength(body);
      totalCount += 1;
      totalBytes += bytes;

      let runId = "unclassified";
      try {
        const parsed = JSON.parse(body) as unknown;
        runId = extractRunId(parsed) ?? runId;
      } catch {
        runId = "unparsed";
      }

      const current = byRunId.get(runId) ?? {
        count: 0,
        bytes: 0,
        lastReceivedAt: startedAt,
      };
      current.count += 1;
      current.bytes += bytes;
      current.lastReceivedAt = new Date().toISOString();
      byRunId.set(runId, current);

      if (totalCount % 100 === 0) {
        persistState();
      }

      return new Response(null, { status: 200 });
    }

    return Response.json({ error: "not found" }, { status: 404 });
  },
});

persistState();

console.log(`[counting-sink] listening on http://${server.hostname}:${server.port}`);
await new Promise(() => {});
