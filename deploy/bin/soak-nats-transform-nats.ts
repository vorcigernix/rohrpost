import { connect, StringCodec, type NatsConnection } from "../../apps/router-workers/node_modules/nats";

type SizeClass = "small" | "medium" | "large";
type MemoryMode = "local" | "kubernetes";
type MemoryMetric = "rssMb" | "workingSetMb" | "usageMb";
type MemorySource = "local_process" | "container_process" | "kubelet_summary" | "mixed" | "unavailable";

interface LocalServiceProbe {
  label: string;
  port: number;
}

interface KubernetesMemoryProbe {
  label: string;
  namespace: string;
  selector: string;
  container?: string;
}

type MemoryProbe = LocalServiceProbe | KubernetesMemoryProbe;

interface ServiceMemorySample {
  label: string;
  pid: number | null;
  rssMb: number | null;
  workingSetMb: number | null;
  usageMb: number | null;
  cpuPercent: number | null;
  memorySource: MemorySource;
  podName?: string;
  container?: string;
  command?: string;
}

interface MemorySample {
  at: string;
  elapsedSeconds: number;
  targetRps: number;
  accepted: number;
  sinkDelivered: number;
  services: ServiceMemorySample[];
}

interface SizeStats {
  attempted: number;
  accepted: number;
  failed: number;
  avgLatencyMs: number;
  avgBytes: number;
}

interface SoakSummary {
  runId: string;
  deploymentId: string;
  controlApiUrl: string;
  routerUrl: string;
  natsUrl: string;
  sourceSubject: string;
  sinkSubject: string;
  durationSeconds: number;
  rateFloorRps: number;
  rateCeilingRps: number;
  attempted: number;
  accepted: number;
  failed: number;
  achievedPublishRps: number;
  sendDurationMs: number;
  totalDurationMs: number;
  publishLatencyMs: {
    min: number;
    p50: number;
    p95: number;
    p99: number;
    max: number;
    avg: number;
  };
  endToEndLatencyMs: {
    min: number;
    p50: number;
    p95: number;
    p99: number;
    max: number;
    avg: number;
  };
  bySize: Record<SizeClass, SizeStats>;
  routerAccepted: number;
  routerProcessed: number;
  routerDelivered: number;
  sinkDelivered: number;
  sinkMissing: number;
  routerSummary: unknown;
  dlqCount: number;
  memoryProfile: MemorySample[];
  memoryTrend: Array<{
    label: string;
    metric: MemoryMetric;
    memorySource: MemorySource | null;
    startMb: number | null;
    endMb: number | null;
    peakMb: number | null;
    deltaMb: number | null;
    slopeMbPerHour: number | null;
    stabilizedStartMb: number | null;
    stabilizedEndMb: number | null;
    stabilizedDeltaMb: number | null;
    stabilizedSlopeMbPerHour: number | null;
  }>;
  startedAt: string;
  finishedAt: string;
}

interface RuntimeDeploymentRecord {
  deployment: {
    id: string;
  };
  revision: {
    spec: {
      sources?: Array<{
        kind?: string;
        connector?: {
          capabilityId?: string;
          connectorId?: string;
        };
      }>;
      sinks?: Array<{
        kind?: string;
        connector?: {
          capabilityId?: string;
          connectorId?: string;
        };
      }>;
    };
  };
  connectors: Record<string, {
    id: string;
    capabilityId: string;
    config: Record<string, unknown>;
  }>;
}

interface RuntimeDeploymentResponse {
  deployments: RuntimeDeploymentRecord[];
}

interface RouterStatusSnapshot {
  deploymentStats?: Array<{
    deploymentId?: string;
    acceptedCount?: number;
    processedCount?: number;
    deliveredCount?: number;
    dlqCount?: number;
    failedCount?: number;
    inflightCount?: number;
    backlogCount?: number;
    state?: string;
  }>;
}

class LatencyHistogram {
  private readonly bins: Uint32Array;
  private totalCount = 0;
  private sum = 0;
  private min = Number.POSITIVE_INFINITY;
  private max = 0;

  public constructor(private readonly ceilingMs = 5_000) {
    this.bins = new Uint32Array(ceilingMs + 1);
  }

  public record(valueMs: number): void {
    const bucket = Math.max(0, Math.min(this.ceilingMs, Math.round(valueMs)));
    this.bins[bucket] += 1;
    this.totalCount += 1;
    this.sum += valueMs;
    this.min = Math.min(this.min, valueMs);
    this.max = Math.max(this.max, valueMs);
  }

  public summary() {
    return {
      min: Number.isFinite(this.min) ? this.min : 0,
      p50: this.quantile(0.5),
      p95: this.quantile(0.95),
      p99: this.quantile(0.99),
      max: this.max,
      avg: this.totalCount > 0 ? this.sum / this.totalCount : 0,
    };
  }

  private quantile(ratio: number): number {
    if (this.totalCount === 0) {
      return 0;
    }

    const target = Math.ceil(this.totalCount * ratio);
    let cumulative = 0;
    for (let index = 0; index < this.bins.length; index += 1) {
      cumulative += this.bins[index] ?? 0;
      if (cumulative >= target) {
        return index;
      }
    }

    return this.ceilingMs;
  }
}

function parseInteger(value: string | undefined, fallback: number): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.floor(parsed) : fallback;
}

function parseBoolean(value: string | undefined, fallback = false): boolean {
  if (value == null || value === "") {
    return fallback;
  }

  return value === "1" || value.toLowerCase() === "true" || value.toLowerCase() === "yes";
}

function average(total: number, count: number): number {
  return count > 0 ? total / count : 0;
}

function sizeClassFor(sequence: number): SizeClass {
  if (sequence % 10 === 0) return "large";
  if (sequence % 3 === 0) return "medium";
  return "small";
}

function targetBytesFor(sizeClass: SizeClass): number {
  switch (sizeClass) {
    case "medium":
      return parseInteger(process.env.LOAD_TEST_MEDIUM_BYTES, 2_048);
    case "large":
      return parseInteger(process.env.LOAD_TEST_LARGE_BYTES, 8_192);
    default:
      return parseInteger(process.env.LOAD_TEST_SMALL_BYTES, 256);
  }
}

function buildPayload(runId: string, sequence: number) {
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
      scenario: "nats-transform-nats-soak",
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

function rateAtSecond(second: number, minRps: number, maxRps: number): number {
  const midpoint = (minRps + maxRps) / 2;
  const amplitude = (maxRps - minRps) / 2;
  const primaryPeriod = parseInteger(process.env.LOAD_TEST_PRIMARY_PERIOD_SECONDS, 900);
  const burstPeriod = parseInteger(process.env.LOAD_TEST_BURST_PERIOD_SECONDS, 73);
  const jitterPeriod = parseInteger(process.env.LOAD_TEST_JITTER_PERIOD_SECONDS, 17);

  const primary = Math.sin(((second / primaryPeriod) * Math.PI * 2) - (Math.PI / 2));
  const burst = Math.sin((second / burstPeriod) * Math.PI * 2);
  const jitter = Math.sin((second / jitterPeriod) * Math.PI * 2);
  const rate = midpoint + (amplitude * primary) + (amplitude * 0.18 * burst) + (amplitude * 0.06 * jitter);

  return Math.max(minRps, Math.min(maxRps, Math.round(rate)));
}

async function sleepUntil(timestampMs: number): Promise<void> {
  const waitMs = timestampMs - performance.now();
  if (waitMs > 0) {
    await Bun.sleep(waitMs);
  }
}

function runShell(command: string): string {
  const result = Bun.spawnSync({
    cmd: ["sh", "-lc", command],
    stdout: "pipe",
    stderr: "pipe",
  });

  return result.stdout.toString().trim();
}

function runCommand(args: string[]): { stdout: string; stderr: string; success: boolean } {
  const result = Bun.spawnSync({
    cmd: args,
    stdout: "pipe",
    stderr: "pipe",
  });

  return {
    stdout: result.stdout.toString().trim(),
    stderr: result.stderr.toString().trim(),
    success: result.exitCode === 0,
  };
}

function unavailableMemorySample(
  label: string,
  command: string,
  options: { container?: string; podName?: string } = {},
): ServiceMemorySample {
  return {
    label,
    pid: null,
    rssMb: null,
    workingSetMb: null,
    usageMb: null,
    cpuPercent: null,
    memorySource: "unavailable",
    podName: options.podName,
    container: options.container,
    command,
  };
}

function probeLocalService({ label, port }: LocalServiceProbe): ServiceMemorySample {
  const output = runShell(
    `pid=$(lsof -nP -iTCP:${port} -sTCP:LISTEN -t | head -n 1); if [ -n "$pid" ]; then ps -o pid=,rss=,%cpu=,command= -p "$pid"; fi`,
  );

  if (!output) {
    return unavailableMemorySample(label, `no listener on port ${port}`);
  }

  const match = output.match(/^\s*(\d+)\s+(\d+)\s+([0-9.]+)\s+(.+)$/);
  if (!match) {
    return unavailableMemorySample(label, output);
  }

  return {
    label,
    pid: Number(match[1]),
    rssMb: Number(match[2]) / 1024,
    workingSetMb: null,
    usageMb: null,
    cpuPercent: Number(match[3]),
    memorySource: "local_process",
    command: match[4],
  };
}

function probeKubernetesWorkload({ label, namespace, selector, container }: KubernetesMemoryProbe): ServiceMemorySample {
  const podResult = runCommand([
    "kubectl",
    "-n",
    namespace,
    "get",
    "pods",
    "-l",
    selector,
    "-o",
    "jsonpath={range .items[*]}{.metadata.name}{\"\\t\"}{.status.phase}{\"\\t\"}{.spec.nodeName}{\"\\n\"}{end}",
  ]);

  if (!podResult.success || !podResult.stdout) {
    return unavailableMemorySample(label, podResult.stderr || "pod not found", { container });
  }

  const podName = podResult.stdout
    .split("\n")
    .map((line) => line.trim())
    .find((line) => line.includes("\tRunning\t"))
    ?.split("\t")[0];
  const nodeName = podResult.stdout
    .split("\n")
    .map((line) => line.trim())
    .find((line) => line.includes("\tRunning\t"))
    ?.split("\t")[2];

  if (!podName || !nodeName) {
    return unavailableMemorySample(label, "no running pod", { container });
  }

  const summaryResult = runCommand([
    "kubectl",
    "get",
    "--raw",
    `/api/v1/nodes/${nodeName}/proxy/stats/summary`,
  ]);

  let workingSetMb: number | null = null;
  let usageMb: number | null = null;
  let measuredContainerName = container;

  if (summaryResult.success && summaryResult.stdout) {
    try {
      const summary = JSON.parse(summaryResult.stdout) as {
        pods?: Array<{
          podRef?: { name?: string; namespace?: string };
          containers?: Array<{
            name?: string;
            memory?: {
              workingSetBytes?: number;
              usageBytes?: number;
            };
          }>;
        }>;
      };

      const pod = summary.pods?.find((entry) =>
        entry.podRef?.namespace === namespace && entry.podRef?.name === podName);
      const measuredContainer = pod?.containers?.find((entry) =>
        container ? entry.name === container : true);
      const workingSetBytes = measuredContainer?.memory?.workingSetBytes;
      const usageBytes = measuredContainer?.memory?.usageBytes;

      if (measuredContainer && (Number.isFinite(workingSetBytes) || Number.isFinite(usageBytes))) {
        workingSetMb = Number.isFinite(workingSetBytes) ? Number(workingSetBytes) / 1048576 : null;
        usageMb = Number.isFinite(usageBytes) ? Number(usageBytes) / 1048576 : null;
        measuredContainerName = measuredContainer.name ?? container;
      }
    } catch {
      // fall back to exec-based probing
    }
  }

  const execArgs = [
    "kubectl",
    "-n",
    namespace,
    "exec",
    podName,
  ];

  if (container) {
    execArgs.push("-c", container);
  }

  execArgs.push(
    "--",
    "sh",
    "-lc",
    "rss=$(awk '/VmRSS/{print $2}' /proc/1/status 2>/dev/null); cmd=$(tr '\\000' ' ' </proc/1/cmdline 2>/dev/null); printf '%s|%s\\n' \"${rss:-}\" \"${cmd:-}\"",
  );

  const execResult = runCommand(execArgs);
  let rssMb: number | null = null;
  let command = workingSetMb != null || usageMb != null ? "kubelet stats summary" : undefined;

  if (execResult.success && execResult.stdout) {
    const [rssKb, commandLine] = execResult.stdout.split("|", 2);
    const rssValue = Number(rssKb);
    rssMb = Number.isFinite(rssValue) ? rssValue / 1024 : null;
    command = commandLine?.trim() || command || podName;
  }

  if (rssMb == null && workingSetMb == null && usageMb == null) {
    return unavailableMemorySample(label, execResult.stderr || "kubectl exec failed", { podName, container });
  }

  return {
    label,
    pid: null,
    rssMb,
    workingSetMb,
    usageMb,
    cpuPercent: null,
    memorySource: rssMb != null && (workingSetMb != null || usageMb != null)
      ? "mixed"
      : rssMb != null
        ? "container_process"
        : "kubelet_summary",
    podName,
    container: measuredContainerName,
    command: command ?? podName,
  };
}

function buildMemoryProbes(memoryMode: MemoryMode): MemoryProbe[] {
  if (memoryMode === "kubernetes") {
    const namespace = process.env.LOAD_TEST_K8S_NAMESPACE ?? "rohrpost";
    const raw = process.env.LOAD_TEST_K8S_PROBES_JSON;
    if (raw) {
      try {
        const parsed = JSON.parse(raw) as KubernetesMemoryProbe[];
        if (Array.isArray(parsed) && parsed.length > 0) {
          return parsed;
        }
      } catch {
        // fall through to defaults
      }
    }

    return [
      {
        label: "router-workers",
        namespace,
        selector: "app.kubernetes.io/name=router-workers",
      },
      {
        label: "control-api",
        namespace,
        selector: "app.kubernetes.io/name=control-api",
      },
      {
        label: "nats",
        namespace,
        selector: "app.kubernetes.io/name=nats",
      },
    ];
  }

  return [
    { label: "router-workers", port: 3002 },
    { label: "control-api", port: 3001 },
    { label: "nats", port: 4222 },
  ];
}

function probeMemoryTarget(probe: MemoryProbe): ServiceMemorySample {
  if ("selector" in probe) {
    return probeKubernetesWorkload(probe);
  }

  return probeLocalService(probe);
}

function linearSlopePerHour(samples: Array<{ xHours: number; yMb: number }>): number | null {
  if (samples.length < 2) {
    return null;
  }

  const meanX = samples.reduce((sum, sample) => sum + sample.xHours, 0) / samples.length;
  const meanY = samples.reduce((sum, sample) => sum + sample.yMb, 0) / samples.length;
  let numerator = 0;
  let denominator = 0;

  for (const sample of samples) {
    numerator += (sample.xHours - meanX) * (sample.yMb - meanY);
    denominator += (sample.xHours - meanX) ** 2;
  }

  if (denominator === 0) {
    return null;
  }

  return numerator / denominator;
}

function sourceForMetric(service: ServiceMemorySample | undefined, metric: MemoryMetric): MemorySource | null {
  if (!service || service[metric] == null) {
    return null;
  }

  if (service.memorySource !== "mixed") {
    return service.memorySource;
  }

  return metric === "rssMb" ? "container_process" : "kubelet_summary";
}

function buildMemoryTrend(memoryProfile: MemorySample[], probes: MemoryProbe[], warmupSeconds: number): SoakSummary["memoryTrend"] {
  const trends: SoakSummary["memoryTrend"] = [];
  const metrics: MemoryMetric[] = ["rssMb", "workingSetMb", "usageMb"];

  for (const probe of probes) {
    for (const metric of metrics) {
      const samples = memoryProfile
        .map((sample) => sample.services.find((service) => service.label === probe.label))
        .map((service, index) => ({
          xHours: memoryProfile[index].elapsedSeconds / 3600,
          yMb: service?.[metric] ?? NaN,
          memorySource: sourceForMetric(service, metric),
        }))
        .filter((sample) => Number.isFinite(sample.yMb));

      if (samples.length === 0) {
        continue;
      }

      const startMb = samples[0]?.yMb ?? null;
      const endMb = samples[samples.length - 1]?.yMb ?? null;
      const peakMb = samples.length > 0 ? Math.max(...samples.map((sample) => sample.yMb)) : null;
      const deltaMb = startMb != null && endMb != null ? endMb - startMb : null;
      const stabilizedSamples = samples.filter((sample) => sample.xHours >= (warmupSeconds / 3600));
      const stabilizedStartMb = stabilizedSamples[0]?.yMb ?? null;
      const stabilizedEndMb = stabilizedSamples[stabilizedSamples.length - 1]?.yMb ?? null;
      const stabilizedDeltaMb =
        stabilizedStartMb != null && stabilizedEndMb != null
          ? stabilizedEndMb - stabilizedStartMb
          : null;

      trends.push({
        label: probe.label,
        metric,
        memorySource: samples[0]?.memorySource ?? null,
        startMb,
        endMb,
        peakMb,
        deltaMb,
        slopeMbPerHour: linearSlopePerHour(samples),
        stabilizedStartMb,
        stabilizedEndMb,
        stabilizedDeltaMb,
        stabilizedSlopeMbPerHour: linearSlopePerHour(stabilizedSamples),
      });
    }
  }

  return trends;
}

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

function extractIssuedAt(payload: unknown): string | undefined {
  if (!payload || typeof payload !== "object") {
    return undefined;
  }

  const record = payload as Record<string, unknown>;
  if (typeof record.issuedAt === "string" && record.issuedAt) {
    return record.issuedAt;
  }

  if (record.payload && typeof record.payload === "object") {
    const nested = (record.payload as Record<string, unknown>).issuedAt;
    if (typeof nested === "string" && nested) {
      return nested;
    }
  }

  if (record.envelope && typeof record.envelope === "object") {
    const envelope = record.envelope as Record<string, unknown>;
    if (envelope.payload && typeof envelope.payload === "object") {
      const nested = (envelope.payload as Record<string, unknown>).issuedAt;
      if (typeof nested === "string" && nested) {
        return nested;
      }
    }
  }

  return undefined;
}

async function fetchActiveDeployments(
  controlApiUrl: string,
  token: string,
): Promise<RuntimeDeploymentRecord[]> {
  const response = await fetch(`${controlApiUrl.replace(/\/$/, "")}/api/runtime/deployments/active`, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch active deployments: HTTP ${response.status}`);
  }

  const payload = await response.json() as RuntimeDeploymentResponse;
  return Array.isArray(payload.deployments) ? payload.deployments : [];
}

async function fetchRouterStatusSnapshot(
  routerUrl: string,
): Promise<RouterStatusSnapshot | null> {
  return fetch(`${routerUrl.replace(/\/$/, "")}/status`)
    .then((response) => response.json())
    .catch(() => null) as Promise<RouterStatusSnapshot | null>;
}

function deploymentStatsFromSnapshot(
  snapshot: RouterStatusSnapshot | null,
  deploymentId: string,
): {
  acceptedCount: number;
  processedCount: number;
  deliveredCount: number;
} {
  const record = snapshot?.deploymentStats?.find((entry) => entry.deploymentId === deploymentId);
  return {
    acceptedCount: Number(record?.acceptedCount ?? 0),
    processedCount: Number(record?.processedCount ?? 0),
    deliveredCount: Number(record?.deliveredCount ?? 0),
  };
}

function isNatsSource(record: RuntimeDeploymentRecord): boolean {
  return (record.revision.spec.sources ?? []).some((source) =>
    source.kind === "nats" || source.connector?.capabilityId === "nats_in");
}

function isNatsSink(record: RuntimeDeploymentRecord): boolean {
  return (record.revision.spec.sinks ?? []).some((sink) =>
    sink.kind === "nats" || sink.connector?.capabilityId === "nats_out");
}

function resolveSubject(
  record: RuntimeDeploymentRecord,
  connectorId: string | undefined,
  capabilityId: "nats_in" | "nats_out",
): string | undefined {
  if (!connectorId) {
    return undefined;
  }

  const connector = record.connectors[connectorId];
  if (!connector || connector.capabilityId !== capabilityId) {
    return undefined;
  }

  return typeof connector.config.subject === "string" ? connector.config.subject : undefined;
}

async function resolveScenario(
  controlApiUrl: string,
  token: string,
  requestedDeploymentId?: string,
): Promise<{
  deploymentId: string;
  sourceSubject: string;
  sinkSubject: string;
}> {
  const deployments = await fetchActiveDeployments(controlApiUrl, token);
  const matching = deployments.filter((record) => isNatsSource(record) && isNatsSink(record));
  const selected = requestedDeploymentId
    ? matching.find((record) => record.deployment.id === requestedDeploymentId)
    : matching[0];

  if (!selected) {
    if (requestedDeploymentId) {
      throw new Error(`No active nats-transform-nats deployment found for ${requestedDeploymentId}`);
    }

    throw new Error("No active nats-transform-nats deployment found");
  }

  const sourceConnectorId = (selected.revision.spec.sources ?? []).find((source) =>
    source.kind === "nats" || source.connector?.capabilityId === "nats_in")?.connector?.connectorId;
  const sinkConnectorId = (selected.revision.spec.sinks ?? []).find((sink) =>
    sink.kind === "nats" || sink.connector?.capabilityId === "nats_out")?.connector?.connectorId;

  const sourceSubject = process.env.LOAD_TEST_SOURCE_SUBJECT
    ?? resolveSubject(selected, sourceConnectorId, "nats_in");
  const sinkSubject = process.env.LOAD_TEST_SINK_SUBJECT
    ?? resolveSubject(selected, sinkConnectorId, "nats_out");

  if (!sourceSubject || !sinkSubject) {
    throw new Error(`Deployment ${selected.deployment.id} is missing source or sink NATS subjects`);
  }

  return {
    deploymentId: selected.deployment.id,
    sourceSubject,
    sinkSubject,
  };
}

function deliveredCount(byRunId: Map<string, { count: number }>, runId: string): number {
  return byRunId.get(runId)?.count ?? 0;
}

async function publishEvent(
  nc: NatsConnection,
  codec: ReturnType<typeof StringCodec>,
  sourceSubject: string,
  runId: string,
  sequence: number,
): Promise<{
  ok: boolean;
  sizeClass: SizeClass;
  bytes: number;
  latencyMs: number;
}> {
  const { payload, sizeClass, bytes } = buildPayload(runId, sequence);
  const startedAt = performance.now();

  try {
    nc.publish(sourceSubject, codec.encode(JSON.stringify(payload)));
    return {
      ok: true,
      sizeClass,
      bytes,
      latencyMs: performance.now() - startedAt,
    };
  } catch {
    return {
      ok: false,
      sizeClass,
      bytes,
      latencyMs: performance.now() - startedAt,
    };
  }
}

async function runPreflight(
  nc: NatsConnection,
  codec: ReturnType<typeof StringCodec>,
  sourceSubject: string,
  byRunId: Map<string, { count: number }>,
  runId: string,
): Promise<void> {
  const timeoutMs = parseInteger(process.env.LOAD_TEST_PREFLIGHT_TIMEOUT_MS, 15_000);
  const preflightRunId = `${runId}-preflight`;
  const result = await publishEvent(nc, codec, sourceSubject, preflightRunId, 1);

  if (!result.ok) {
    throw new Error(`Preflight publish failed for ${sourceSubject}`);
  }

  await nc.flush();

  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (deliveredCount(byRunId, preflightRunId) > 0) {
      return;
    }
    await Bun.sleep(500);
  }

  throw new Error(`Preflight delivery did not reach ${sourceSubject} -> sink within ${timeoutMs}ms`);
}

async function main() {
  const controlApiUrl = process.env.LOAD_TEST_CONTROL_API_URL ?? "http://127.0.0.1:3001";
  const controlApiToken = process.env.LOAD_TEST_CONTROL_API_TOKEN ?? "dev-admin-token";
  const routerUrl = process.env.LOAD_TEST_ROUTER_URL ?? "http://127.0.0.1:3002";
  const natsUrl = process.env.LOAD_TEST_NATS_URL ?? "nats://127.0.0.1:4222";
  const durationSeconds = parseInteger(process.env.LOAD_TEST_DURATION_SECONDS, 10_800);
  const minRps = parseInteger(process.env.LOAD_TEST_MIN_RPS, 200);
  const maxRps = parseInteger(process.env.LOAD_TEST_MAX_RPS, 1_200);
  const settleSeconds = parseInteger(process.env.LOAD_TEST_SETTLE_SECONDS, 30);
  const tickMs = parseInteger(process.env.LOAD_TEST_TICK_MS, 100);
  const progressIntervalSeconds = parseInteger(process.env.LOAD_TEST_PROGRESS_INTERVAL_SECONDS, 60);
  const sampleIntervalSeconds = parseInteger(process.env.LOAD_TEST_SAMPLE_INTERVAL_SECONDS, 60);
  const warmupSeconds = parseInteger(process.env.LOAD_TEST_WARMUP_SECONDS, 600);
  const requestedDeploymentId = process.env.LOAD_TEST_DEPLOYMENT_ID;
  const runId = process.env.LOAD_TEST_RUN_ID ?? `nats-soak-${Date.now().toString(36)}`;
  const startedAt = new Date().toISOString();
  const memoryMode = (process.env.LOAD_TEST_MEMORY_MODE === "kubernetes" ? "kubernetes" : "local") satisfies MemoryMode;
  const probes = buildMemoryProbes(memoryMode);
  const shouldPreflight = parseBoolean(process.env.LOAD_TEST_PREFLIGHT, false);

  const scenario = await resolveScenario(controlApiUrl, controlApiToken, requestedDeploymentId);
  const deploymentId = scenario.deploymentId;
  const sourceSubject = scenario.sourceSubject;
  const sinkSubject = scenario.sinkSubject;
  const baselineRouterStats = deploymentStatsFromSnapshot(
    await fetchRouterStatusSnapshot(routerUrl),
    deploymentId,
  );

  const publishHistogram = new LatencyHistogram();
  const endToEndHistogram = new LatencyHistogram(60_000);
  const bySize: Record<SizeClass, { attempted: number; accepted: number; failed: number; totalLatencyMs: number; totalBytes: number }> = {
    small: { attempted: 0, accepted: 0, failed: 0, totalLatencyMs: 0, totalBytes: 0 },
    medium: { attempted: 0, accepted: 0, failed: 0, totalLatencyMs: 0, totalBytes: 0 },
    large: { attempted: 0, accepted: 0, failed: 0, totalLatencyMs: 0, totalBytes: 0 },
  };
  const memoryProfile: MemorySample[] = [];
  const deliveredByRunId = new Map<string, { count: number; bytes: number; lastReceivedAt: string }>();
  let attempted = 0;
  let accepted = 0;
  let failed = 0;
  let sequence = 0;

  const nc = await connect({ servers: natsUrl, name: `load-test-${runId}` });
  const codec = StringCodec();
  const subscription = nc.subscribe(sinkSubject);
  const sinkLoop = (async () => {
    for await (const message of subscription) {
      let parsed: unknown;
      try {
        parsed = JSON.parse(codec.decode(message.data));
      } catch {
        parsed = undefined;
      }

      const currentRunId = extractRunId(parsed) ?? "unclassified";
      const current = deliveredByRunId.get(currentRunId) ?? {
        count: 0,
        bytes: 0,
        lastReceivedAt: startedAt,
      };
      current.count += 1;
      current.bytes += message.data.length;
      current.lastReceivedAt = new Date().toISOString();
      deliveredByRunId.set(currentRunId, current);

      const issuedAt = extractIssuedAt(parsed);
      if (issuedAt) {
        const latencyMs = Date.now() - Date.parse(issuedAt);
        if (Number.isFinite(latencyMs) && latencyMs >= 0) {
          endToEndHistogram.record(latencyMs);
        }
      }
    }
  })();

  await nc.flush();

  try {
    if (shouldPreflight) {
      await runPreflight(nc, codec, sourceSubject, deliveredByRunId, runId);
    }

    const sendStart = performance.now();
    const sendDeadline = sendStart + (durationSeconds * 1_000);
    let tickDeadline = sendStart;
    let nextProgressMs = sendStart + (progressIntervalSeconds * 1_000);
    let nextSampleMs = sendStart;

    while (performance.now() < sendDeadline) {
      const elapsedSeconds = (tickDeadline - sendStart) / 1_000;
      const second = Math.floor(elapsedSeconds);
      const targetRps = rateAtSecond(second, minRps, maxRps);
      const ticksPerSecond = Math.floor(1_000 / tickMs);
      const tickInSecond = Math.floor((elapsedSeconds * 1_000) / tickMs) % ticksPerSecond;
      const perTickBase = Math.floor(targetRps / ticksPerSecond);
      const remainder = targetRps - (perTickBase * ticksPerSecond);
      const requestCount = perTickBase + (tickInSecond < remainder ? 1 : 0);

      for (let count = 0; count < requestCount; count += 1) {
        sequence += 1;
        attempted += 1;

        const result = await publishEvent(nc, codec, sourceSubject, runId, sequence);
        bySize[result.sizeClass].attempted += 1;
        bySize[result.sizeClass].totalLatencyMs += result.latencyMs;
        bySize[result.sizeClass].totalBytes += result.bytes;
        publishHistogram.record(result.latencyMs);

        if (result.ok) {
          accepted += 1;
          bySize[result.sizeClass].accepted += 1;
        } else {
          failed += 1;
          bySize[result.sizeClass].failed += 1;
        }
      }

      await nc.flush();

      const now = performance.now();
      if (now >= nextProgressMs) {
        const routerProgress = deploymentStatsFromSnapshot(
          await fetchRouterStatusSnapshot(routerUrl),
          deploymentId,
        );
        const delivered = deliveredCount(deliveredByRunId, runId);
        const elapsed = Math.round((now - sendStart) / 1_000);
        console.error(
          `[progress] elapsed=${elapsed}s targetRps=${targetRps} attempted=${attempted} accepted=${accepted} routerDelivered=${Math.max(0, routerProgress.deliveredCount - baselineRouterStats.deliveredCount)} sinkDelivered=${delivered}`,
        );
        nextProgressMs += progressIntervalSeconds * 1_000;
      }

      if (now >= nextSampleMs) {
        const delivered = deliveredCount(deliveredByRunId, runId);
        memoryProfile.push({
          at: new Date().toISOString(),
          elapsedSeconds: Math.round((now - sendStart) / 1_000),
          targetRps,
          accepted,
          sinkDelivered: delivered,
          services: probes.map(probeMemoryTarget),
        });
        nextSampleMs += sampleIntervalSeconds * 1_000;
      }

      tickDeadline += tickMs;
      await sleepUntil(tickDeadline);
    }

    await nc.flush();
    const sendDurationMs = performance.now() - sendStart;
    await Bun.sleep(settleSeconds * 1_000);

    const delivered = deliveredCount(deliveredByRunId, runId);
    const routerSummary = await fetchRouterStatusSnapshot(routerUrl);
    const routerFinalStats = deploymentStatsFromSnapshot(routerSummary, deploymentId);
    const routerAccepted = Math.max(0, routerFinalStats.acceptedCount - baselineRouterStats.acceptedCount);
    const routerProcessed = Math.max(0, routerFinalStats.processedCount - baselineRouterStats.processedCount);
    const routerDelivered = Math.max(0, routerFinalStats.deliveredCount - baselineRouterStats.deliveredCount);
    const dlqCount = await fetch(`${routerUrl}/dlq`)
      .then((response) => response.json())
      .then((records) => Array.isArray(records) ? records.length : 0)
      .catch(() => 0);

    memoryProfile.push({
      at: new Date().toISOString(),
      elapsedSeconds: Math.round((performance.now() - sendStart) / 1_000),
      targetRps: rateAtSecond(durationSeconds, minRps, maxRps),
      accepted,
      sinkDelivered: delivered,
      services: probes.map(probeMemoryTarget),
    });

    const memoryTrend = buildMemoryTrend(memoryProfile, probes, warmupSeconds);

    const summary: SoakSummary = {
      runId,
      deploymentId,
      controlApiUrl,
      routerUrl,
      natsUrl,
      sourceSubject,
      sinkSubject,
      durationSeconds,
      rateFloorRps: minRps,
      rateCeilingRps: maxRps,
      attempted,
      accepted,
      failed,
      achievedPublishRps: attempted / (sendDurationMs / 1_000),
      sendDurationMs,
      totalDurationMs: performance.now() - sendStart,
      publishLatencyMs: publishHistogram.summary(),
      endToEndLatencyMs: endToEndHistogram.summary(),
      bySize: {
        small: {
          attempted: bySize.small.attempted,
          accepted: bySize.small.accepted,
          failed: bySize.small.failed,
          avgLatencyMs: average(bySize.small.totalLatencyMs, bySize.small.attempted),
          avgBytes: average(bySize.small.totalBytes, bySize.small.attempted),
        },
        medium: {
          attempted: bySize.medium.attempted,
          accepted: bySize.medium.accepted,
          failed: bySize.medium.failed,
          avgLatencyMs: average(bySize.medium.totalLatencyMs, bySize.medium.attempted),
          avgBytes: average(bySize.medium.totalBytes, bySize.medium.attempted),
        },
        large: {
          attempted: bySize.large.attempted,
          accepted: bySize.large.accepted,
          failed: bySize.large.failed,
          avgLatencyMs: average(bySize.large.totalLatencyMs, bySize.large.attempted),
          avgBytes: average(bySize.large.totalBytes, bySize.large.attempted),
        },
      },
      routerAccepted,
      routerProcessed,
      routerDelivered,
      sinkDelivered: delivered,
      sinkMissing: Math.max(0, routerDelivered - delivered),
      routerSummary,
      dlqCount,
      memoryProfile,
      memoryTrend,
      startedAt,
      finishedAt: new Date().toISOString(),
    };

    console.log(JSON.stringify(summary, null, 2));
  } finally {
    subscription.unsubscribe();
    await sinkLoop.catch(() => {});
    await nc.drain().catch(() => {});
    await nc.close().catch(() => {});
  }
}

await main();
