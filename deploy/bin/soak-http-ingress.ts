export {};

type SizeClass = "small" | "medium" | "large";

type MemoryMode = "local" | "kubernetes";
type MemoryMetric = "rssMb" | "workingSetMb" | "usageMb";
type MemorySource = "local_process" | "container_process" | "kubelet_summary" | "mixed" | "unavailable";
type PayloadProfile = "generic" | "customer_record" | "demo_store";
type DeliverySource = "sink" | "router";

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
  routerUrl: string;
  sinkStatusUrl: string;
  httpPath: string | null;
  payloadProfile: PayloadProfile;
  deliverySource: DeliverySource;
  durationSeconds: number;
  rateFloorRps: number;
  rateCeilingRps: number;
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
  bySize: Record<SizeClass, SizeStats>;
  routerDelivered: number;
  sinkDelivered: number;
  missingDeliveries: number;
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

function deltaFromBaseline(value: number, baseline: number): number {
  return Math.max(0, value - baseline);
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

function pick<T>(items: T[], sequence: number): T {
  return items[sequence % items.length] as T;
}

interface DemoProduct {
  id: string;
  sku: string;
  name: string;
  category: string;
  price: number;
}

interface DemoCartLine {
  product: DemoProduct;
  quantity: number;
}

const demoProducts: DemoProduct[] = [
  {
    id: "prod_olive_oil",
    sku: "PANTRY-OIL-001",
    name: "Cold Press Olive Oil",
    category: "Pantry",
    price: 18,
  },
  {
    id: "prod_granola",
    sku: "PANTRY-GRA-002",
    name: "Hazelnut Granola",
    category: "Breakfast",
    price: 9,
  },
  {
    id: "prod_coffee",
    sku: "PANTRY-COF-003",
    name: "Morning Filter Coffee",
    category: "Drinks",
    price: 15,
  },
  {
    id: "prod_citrus",
    sku: "PANTRY-CIT-004",
    name: "Seasonal Citrus Box",
    category: "Fresh",
    price: 24,
  },
  {
    id: "prod_chocolate",
    sku: "PANTRY-CHO-005",
    name: "Dark Chocolate Bar",
    category: "Snacks",
    price: 7,
  },
  {
    id: "prod_tea",
    sku: "PANTRY-TEA-006",
    name: "Jasmine Green Tea",
    category: "Drinks",
    price: 12,
  },
];

const demoEventNames = [
  "page_view",
  "view_item_list",
  "search",
  "view_item",
  "add_to_cart",
  "view_cart",
  "begin_checkout",
  "add_shipping_info",
  "add_payment_info",
  "purchase",
] as const;

function demoItem(product: DemoProduct, quantity: number) {
  return {
    item_id: product.id,
    sku: product.sku,
    item_name: product.name,
    item_category: product.category,
    price: product.price,
    quantity,
  };
}

function demoCartFor(sequence: number): DemoCartLine[] {
  const itemCount = (sequence % 3) + 1;
  return Array.from({ length: itemCount }, (_, index) => {
    const product = pick(demoProducts, sequence + index);
    return {
      product,
      quantity: ((sequence + index) % 2) + 1,
    };
  });
}

function demoCartValue(cart: DemoCartLine[]): number {
  return Math.round(cart.reduce((sum, line) => sum + line.product.price * line.quantity, 0) * 100) / 100;
}

function buildDemoStorePayload(runId: string, sourceRef: string | null, sequence: number) {
  const eventName = pick(demoEventNames, sequence);
  const cart = demoCartFor(sequence);
  const product = cart[0]?.product ?? pick(demoProducts, sequence);
  const value = demoCartValue(cart);
  const eventId = `${runId}-${String(sequence).padStart(8, "0")}`;
  const payload: Record<string, unknown> = {
    event_name: eventName,
    event_id: eventId,
    occurred_at: new Date().toISOString(),
    anonymous_id: `anon_load_${String(sequence % 25_000).padStart(5, "0")}`,
    session_id: `session_load_${String(Math.floor(sequence / 8) % 10_000).padStart(5, "0")}`,
    source: {
      type: "demo_store",
      name: "The Pantry",
      surface: "console_demo",
    },
    page: {
      path: "/demo",
      title: "The Pantry",
      referrer: sourceRef ?? "rohrpost-console",
    },
    context: {
      locale: "en-US",
      currency: "USD",
      user_agent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
    },
    ecommerce: {
      currency: "USD",
      value,
      items: cart.map((line) => demoItem(line.product, line.quantity)),
    },
    product: {
      item_id: product.id,
      sku: product.sku,
      item_name: product.name,
      item_category: product.category,
      price: product.price,
    },
    load_test: {
      run_id: runId,
      sequence,
      profile: "demo_store",
    },
  };

  if (eventName === "search") {
    payload.search = {
      query: pick(["olive", "coffee", "tea", "snacks"], sequence),
      result_count: (sequence % demoProducts.length) + 1,
    };
  }

  if (eventName === "add_shipping_info" || eventName === "add_payment_info") {
    payload.checkout = {
      step: eventName === "add_shipping_info" ? "shipping" : "payment",
      shipping_tier: eventName === "add_shipping_info" ? "standard" : undefined,
      payment_type: eventName === "add_payment_info" ? "card" : undefined,
    };
  }

  if (eventName === "purchase") {
    payload.order = {
      order_id: `order_${eventId}`,
      revenue: value,
      tax: Math.round(value * 0.08 * 100) / 100,
      shipping: value > 50 ? 0 : 6,
    };
  }

  return {
    payload,
    sizeClass: "medium" as const,
    bytes: Buffer.byteLength(JSON.stringify(payload)),
  };
}

function buildCustomerPayload(sourceRef: string | null, sequence: number) {
  const names = ["Jana", "Alicja", "Marta", "Petr", "Anna", "Marek", "Eva", "Karel"];
  const surnames = ["Novak", "Kowalska", "Svoboda", "Nowak", "Dvorak", "Wojcik", "Horak", "Mazur"];
  const countries = ["CZ", "PL", "DE", "SK", "AT", "HU"];
  const plans = ["free", "starter", "pro", "business", "enterprise"];
  const name = pick(names, sequence);
  const surname = pick(surnames, sequence * 3);
  const country = pick(countries, sequence * 7);
  const plan = pick(plans, sequence * 5);
  const emailLocal = `${String(name).toLowerCase()}.${String(surname).toLowerCase()}.${sequence}`;
  const payload = {
    customerId: `cust-http-${String(sequence).padStart(7, "0")}`,
    name,
    surname,
    email: `${emailLocal}@example.com`,
    country,
    plan,
    sourceRef: sourceRef ?? "/ingest/http",
  };

  return {
    payload,
    sizeClass: "small" as const,
    bytes: Buffer.byteLength(JSON.stringify(payload)),
  };
}

function buildPayload(runId: string, sequence: number, sourceRef: string | null, payloadProfile: PayloadProfile) {
  if (payloadProfile === "customer_record") {
    return buildCustomerPayload(sourceRef, sequence);
  }
  if (payloadProfile === "demo_store") {
    return buildDemoStorePayload(runId, sourceRef, sequence);
  }

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
      scenario: "three-hour-variable-soak",
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

async function postEvent(
  routerUrl: string,
  deploymentId: string,
  runId: string,
  sequence: number,
  httpPath: string | null,
  payloadProfile: PayloadProfile,
): Promise<{
  ok: boolean;
  sizeClass: SizeClass;
  bytes: number;
  latencyMs: number;
}> {
  const { payload, sizeClass, bytes } = buildPayload(runId, sequence, httpPath, payloadProfile);
  const startedAt = performance.now();

  const response = httpPath
    ? await fetch(`${routerUrl.replace(/\/$/, "")}${httpPath}`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify(payload),
      })
    : await fetch(`${routerUrl}/ingress`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({
          deploymentId,
          messageId: `${runId}-${sequence}`,
          traceId: `${runId}-${sequence}`,
          partitionKey: `tenant-${sequence % 64}`,
          payload,
        }),
      });

  return {
    ok: response.ok,
    sizeClass,
    bytes,
    latencyMs: performance.now() - startedAt,
  };
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
        label: "http-sink",
        namespace,
        selector: "app.kubernetes.io/name=http-counting-sink",
      },
    ];
  }

  return [
    { label: "router-workers", port: 3002 },
    { label: "control-api", port: 3001 },
    { label: "http-sink", port: 4011 },
  ];
}

function probeMemoryTarget(probe: MemoryProbe): ServiceMemorySample {
  if ("selector" in probe) {
    return probeKubernetesWorkload(probe);
  }

  return probeLocalService(probe);
}

async function fetchSinkDelivered(sinkStatusUrl: string, runId: string): Promise<number> {
  const payload = await fetch(sinkStatusUrl).then((response) => response.json()).catch(() => null) as {
    byRunId?: Record<string, { count?: number }>;
  } | null;

  return payload?.byRunId?.[runId]?.count ?? 0;
}

async function fetchRouterDelivered(routerUrl: string, deploymentId: string): Promise<number> {
  const payload = await fetch(`${routerUrl.replace(/\/$/, "")}/status`)
    .then((response) => response.json())
    .catch(() => null) as {
    deploymentStats?: Array<{
      deploymentId?: string;
      deliveredCount?: number;
    }>;
  } | null;

  const stats = payload?.deploymentStats?.find((entry) => entry.deploymentId === deploymentId);
  return Number(stats?.deliveredCount ?? 0);
}

async function fetchDeliveredCount(
  deliverySource: DeliverySource,
  routerUrl: string,
  sinkStatusUrl: string,
  deploymentId: string,
  runId: string,
): Promise<number> {
  if (deliverySource === "router") {
    return fetchRouterDelivered(routerUrl, deploymentId);
  }

  return fetchSinkDelivered(sinkStatusUrl, runId);
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

async function runPreflight(
  routerUrl: string,
  sinkStatusUrl: string,
  deploymentId: string,
  runId: string,
  httpPath: string | null,
  payloadProfile: PayloadProfile,
  deliverySource: DeliverySource,
): Promise<void> {
  const timeoutMs = parseInteger(process.env.LOAD_TEST_PREFLIGHT_TIMEOUT_MS, 15_000);
  const preflightRunId = `${runId}-preflight`;
  const baselineDelivered = await fetchDeliveredCount(
    deliverySource,
    routerUrl,
    sinkStatusUrl,
    deploymentId,
    preflightRunId,
  );
  const result = await postEvent(routerUrl, deploymentId, preflightRunId, 1, httpPath, payloadProfile);

  if (!result.ok) {
    throw new Error(`Preflight ingress failed for ${deploymentId}`);
  }

  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const delivered = await fetchDeliveredCount(
      deliverySource,
      routerUrl,
      sinkStatusUrl,
      deploymentId,
      preflightRunId,
    );
    if (delivered > baselineDelivered) {
      return;
    }
    await Bun.sleep(500);
  }

  throw new Error(`Preflight delivery did not reach the sink within ${timeoutMs}ms`);
}

async function main() {
  const deploymentId = process.env.LOAD_TEST_DEPLOYMENT_ID;
  if (!deploymentId) {
    throw new Error("LOAD_TEST_DEPLOYMENT_ID is required");
  }

  const routerUrl = process.env.LOAD_TEST_ROUTER_URL ?? "http://127.0.0.1:3002";
  const sinkStatusUrl = process.env.LOAD_TEST_SINK_STATUS_URL ?? "http://127.0.0.1:4011/status";
  const durationSeconds = parseInteger(process.env.LOAD_TEST_DURATION_SECONDS, 10_800);
  const minRps = parseInteger(process.env.LOAD_TEST_MIN_RPS, 200);
  const maxRps = parseInteger(process.env.LOAD_TEST_MAX_RPS, 1_200);
  const settleSeconds = parseInteger(process.env.LOAD_TEST_SETTLE_SECONDS, 30);
  const tickMs = parseInteger(process.env.LOAD_TEST_TICK_MS, 100);
  const maxInflight = parseInteger(process.env.LOAD_TEST_MAX_INFLIGHT, 512);
  const progressIntervalSeconds = parseInteger(process.env.LOAD_TEST_PROGRESS_INTERVAL_SECONDS, 60);
  const sampleIntervalSeconds = parseInteger(process.env.LOAD_TEST_SAMPLE_INTERVAL_SECONDS, 60);
  const warmupSeconds = parseInteger(process.env.LOAD_TEST_WARMUP_SECONDS, 600);
  const runId = process.env.LOAD_TEST_RUN_ID ?? `soak-${Date.now().toString(36)}`;
  const startedAt = new Date().toISOString();
  const memoryMode = (process.env.LOAD_TEST_MEMORY_MODE === "kubernetes" ? "kubernetes" : "local") satisfies MemoryMode;
  const httpPath = process.env.LOAD_TEST_HTTP_PATH ?? null;
  const payloadProfile = (process.env.LOAD_TEST_PAYLOAD_PROFILE === "customer_record" ||
    process.env.LOAD_TEST_PAYLOAD_PROFILE === "demo_store"
    ? process.env.LOAD_TEST_PAYLOAD_PROFILE
    : "generic") satisfies PayloadProfile;
  const deliverySource = (process.env.LOAD_TEST_DELIVERY_SOURCE === "router"
    ? "router"
    : "sink") satisfies DeliverySource;
  const probes = buildMemoryProbes(memoryMode);
  const shouldPreflight = parseBoolean(process.env.LOAD_TEST_PREFLIGHT, false);

  const histogram = new LatencyHistogram();
  const bySize: Record<SizeClass, { attempted: number; accepted: number; failed: number; totalLatencyMs: number; totalBytes: number }> = {
    small: { attempted: 0, accepted: 0, failed: 0, totalLatencyMs: 0, totalBytes: 0 },
    medium: { attempted: 0, accepted: 0, failed: 0, totalLatencyMs: 0, totalBytes: 0 },
    large: { attempted: 0, accepted: 0, failed: 0, totalLatencyMs: 0, totalBytes: 0 },
  };

  const memoryProfile: MemorySample[] = [];
  let attempted = 0;
  let accepted = 0;
  let failed = 0;
  let sequence = 0;
  const active = new Set<Promise<void>>();
  const sendStart = performance.now();
  const sendDeadline = sendStart + (durationSeconds * 1_000);
  let tickDeadline = sendStart;
  let nextProgressMs = sendStart + (progressIntervalSeconds * 1_000);
  let nextSampleMs = sendStart;

  if (shouldPreflight) {
    await runPreflight(routerUrl, sinkStatusUrl, deploymentId, runId, httpPath, payloadProfile, deliverySource);
  }

  const deliveryBaseline = await fetchDeliveredCount(
    deliverySource,
    routerUrl,
    sinkStatusUrl,
    deploymentId,
    runId,
  );

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
      while (active.size >= maxInflight) {
        await Promise.race(active);
      }

      sequence += 1;
      attempted += 1;
      const task = postEvent(routerUrl, deploymentId, runId, sequence, httpPath, payloadProfile)
        .then((result) => {
          bySize[result.sizeClass].attempted += 1;
          bySize[result.sizeClass].totalLatencyMs += result.latencyMs;
          bySize[result.sizeClass].totalBytes += result.bytes;
          histogram.record(result.latencyMs);
          if (result.ok) {
            accepted += 1;
            bySize[result.sizeClass].accepted += 1;
          } else {
            failed += 1;
            bySize[result.sizeClass].failed += 1;
          }
        })
        .catch(() => {
          failed += 1;
        })
        .finally(() => {
          active.delete(task);
        });

      active.add(task);
    }

    const now = performance.now();
    if (now >= nextProgressMs) {
      const delivered = deltaFromBaseline(
        await fetchDeliveredCount(deliverySource, routerUrl, sinkStatusUrl, deploymentId, runId),
        deliveryBaseline,
      );
      const elapsed = Math.round((now - sendStart) / 1_000);
      console.error(
        `[progress] elapsed=${elapsed}s targetRps=${targetRps} attempted=${attempted} accepted=${accepted} ${deliverySource}Delivered=${delivered} inflight=${active.size}`,
      );
      nextProgressMs += progressIntervalSeconds * 1_000;
    }

    if (now >= nextSampleMs) {
      const delivered = deltaFromBaseline(
        await fetchDeliveredCount(deliverySource, routerUrl, sinkStatusUrl, deploymentId, runId),
        deliveryBaseline,
      );
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

  await Promise.all(active);
  const sendDurationMs = performance.now() - sendStart;
  await Bun.sleep(settleSeconds * 1_000);

  const delivered = deltaFromBaseline(
    await fetchDeliveredCount(deliverySource, routerUrl, sinkStatusUrl, deploymentId, runId),
    deliveryBaseline,
  );
  const routerDelivered = deltaFromBaseline(
    await fetchRouterDelivered(routerUrl, deploymentId),
    deliveryBaseline,
  );
  const routerSummary = await fetch(`${routerUrl}/status`).then((response) => response.json()).catch(() => null);
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
    routerUrl,
    sinkStatusUrl,
    httpPath,
    payloadProfile,
    deliverySource,
    durationSeconds,
    rateFloorRps: minRps,
    rateCeilingRps: maxRps,
    attempted,
    accepted,
    failed,
    achievedIngressRps: attempted / (sendDurationMs / 1_000),
    sendDurationMs,
    totalDurationMs: performance.now() - sendStart,
    ingressLatencyMs: histogram.summary(),
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
    routerDelivered,
    sinkDelivered: deliverySource === "sink" ? delivered : 0,
    missingDeliveries: accepted - delivered,
    routerSummary,
    dlqCount,
    memoryProfile,
    memoryTrend,
    startedAt,
    finishedAt: new Date().toISOString(),
  };

  console.log(JSON.stringify(summary, null, 2));
}

await main();
