import { createHash, createSign } from "node:crypto";
import { appendFileSync, existsSync, mkdirSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { connect, consumerOpts, createInbox, StringCodec, type JsMsg, type NatsConnection } from "nats";
import { buildAdapterWorkSubjectPattern, type AdapterWorkItem } from "@rohrpost/control-api-contracts";
import type { AdapterRedpandaConfig } from "./config";

const stringCodec = StringCodec();

export interface AdapterDeliveryRecord {
  id: string;
  workId: string;
  connectorId: string;
  capabilityId: string;
  flowId: string;
  revisionId: string;
  deploymentId: string;
  messageId: string;
  traceId: string;
  status: "delivered" | "failed";
  mirrorSubject?: string;
  topic?: string;
  targetRef?: string;
  artifactPath?: string;
  objectKey?: string;
  deduplicated?: boolean;
  error?: string;
  receivedAt: string;
  completedAt: string;
}

export interface AdapterRuntimeSummary {
  connected: boolean;
  deliveries: number;
  successes: number;
  failures: number;
  mirroredPublishes: number;
  lastDeliveryAt?: string;
  workSubject: string;
  artifactRoot: string;
}

type JetStreamConsumerLike = {
  name?: string;
  config?: {
    durable_name?: string;
    filter_subject?: string;
    ack_wait?: number;
    max_ack_pending?: number;
    max_deliver?: number;
  };
};

type ServiceAccountCredentials = {
  clientEmail: string;
  privateKey: string;
  tokenUri: string;
};

type AccessToken = {
  token: string;
  expiresAtMs: number;
};

type BigQueryDeliveryMetadata = Pick<
  AdapterDeliveryRecord,
  "targetRef" | "artifactPath" | "deduplicated"
>;

type BigQueryWriteMethod = "storage_write_api" | "insert_all";

type BigQueryTarget = {
  project: string;
  jobProject: string;
  dataset: string;
  table: string;
  credentials: ServiceAccountCredentials;
  batchCount: number;
  batchPeriodMs: number;
  maxInFlightBatches: number;
};

type BigQueryBatchEntry = {
  item: AdapterWorkItem;
  materialized: BigQueryDeliveryMetadata;
  resolve: (metadata: BigQueryDeliveryMetadata) => void;
  reject: (error: unknown) => void;
};

type BigQueryStorageWriterLike = {
  appendRows(rows: Array<Record<string, unknown>>): Promise<void>;
  close(): void;
};

type BigQueryStorageWriterFactory = (target: BigQueryTarget) => BigQueryStorageWriterLike;
type BigQueryStorageModule = typeof import("@google-cloud/bigquery-storage");

const BIGQUERY_SCOPE = "https://www.googleapis.com/auth/bigquery";
const ADAPTER_ACK_WAIT_MS = 60_000;
const ADAPTER_ACK_WAIT_NS = ADAPTER_ACK_WAIT_MS * 1_000_000;
const ADAPTER_MAX_ACK_PENDING = readPositiveIntegerEnv("ADAPTER_MAX_ACK_PENDING", 1_024, 1, 10_000);
const ADAPTER_MAX_DELIVER = readPositiveIntegerEnv("ADAPTER_MAX_DELIVER", 5, 1, 100);
const ADAPTER_NAK_BASE_DELAY_MS = 5_000;
const accessTokenCache = new Map<string, AccessToken>();
let bigQueryStorageModulePromise: Promise<BigQueryStorageModule> | undefined;

function readPositiveIntegerEnv(key: string, fallback: number, min: number, max: number): number {
  const raw = process.env[key];
  if (!raw) return fallback;
  const value = Number.parseInt(raw, 10);
  if (!Number.isFinite(value)) return fallback;
  return Math.min(max, Math.max(min, value));
}

function isoNow(): string {
  return new Date().toISOString();
}

function sanitizeSubjectSegment(value: string): string {
  return value.replace(/[^a-zA-Z0-9_-]+/g, "_");
}

function fileToken(value: string): string {
  const sanitized = value
    .replace(/[^a-zA-Z0-9._-]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 48);
  const digest = createHash("sha1").update(value).digest("hex").slice(0, 10);
  return `${sanitized || "item"}-${digest}`;
}

function readStringConfig(
  config: Record<string, unknown> | undefined,
  key: string,
  fallback: string,
): string {
  const value = config?.[key];
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : fallback;
}

function readBooleanConfig(
  config: Record<string, unknown> | undefined,
  key: string,
  fallback: boolean,
): boolean {
  const value = config?.[key];
  if (typeof value === "boolean") return value;
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (["true", "1", "yes"].includes(normalized)) return true;
    if (["false", "0", "no"].includes(normalized)) return false;
  }
  return fallback;
}

function readPositiveIntegerConfig(
  config: Record<string, unknown> | undefined,
  key: string,
  fallback: number,
  min = 1,
  max = Number.MAX_SAFE_INTEGER,
): number {
  const raw = config?.[key];
  const value = typeof raw === "number"
    ? raw
    : typeof raw === "string"
      ? Number.parseInt(raw.trim(), 10)
      : Number.NaN;
  if (!Number.isFinite(value)) return fallback;
  return Math.min(max, Math.max(min, Math.floor(value)));
}

function readDurationConfigMs(
  config: Record<string, unknown> | undefined,
  key: string,
  fallback: number,
  min = 50,
  max = 60_000,
): number {
  const raw = config?.[key];
  if (typeof raw === "number" && Number.isFinite(raw)) {
    return Math.min(max, Math.max(min, Math.floor(raw)));
  }
  if (typeof raw !== "string") {
    return fallback;
  }

  const trimmed = raw.trim().toLowerCase();
  const match = trimmed.match(/^(\d+(?:\.\d+)?)(ms|s|m)?$/);
  if (!match) {
    return fallback;
  }

  const amount = Number.parseFloat(match[1] ?? "");
  if (!Number.isFinite(amount)) {
    return fallback;
  }

  const unit = match[2] ?? "ms";
  const value = unit === "m"
    ? amount * 60_000
    : unit === "s"
      ? amount * 1_000
      : amount;
  return Math.min(max, Math.max(min, Math.floor(value)));
}

function readBigQueryWriteMethod(config: Record<string, unknown> | undefined): BigQueryWriteMethod {
  const raw = readStringConfig(config, "writeMethod", "storage_write_api")
    .toLowerCase()
    .replaceAll("-", "_")
    .replaceAll(" ", "_");

  if (raw === "insertall" || raw === "insert_all" || raw === "legacy_insert_all") {
    return "insert_all";
  }

  return "storage_write_api";
}

function base64Url(value: string | Buffer): string {
  const buffer = typeof value === "string" ? Buffer.from(value) : value;
  return buffer
    .toString("base64")
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replace(/=+$/g, "");
}

function parseServiceAccountCredentials(config: Record<string, unknown> | undefined): ServiceAccountCredentials | null {
  const raw = config?.credentialsJson;
  if (!raw) return null;

  const parsed = typeof raw === "string" ? JSON.parse(raw) as unknown : raw;
  if (!parsed || typeof parsed !== "object") {
    throw new Error("BigQuery credentialsJson must be a service account JSON object.");
  }

  const value = parsed as Record<string, unknown>;
  const clientEmail = value.client_email;
  const privateKey = value.private_key;
  const tokenUri = value.token_uri;

  if (typeof clientEmail !== "string" || !clientEmail.trim()) {
    throw new Error("BigQuery credentialsJson is missing client_email.");
  }
  if (typeof privateKey !== "string" || !privateKey.includes("BEGIN PRIVATE KEY")) {
    throw new Error("BigQuery credentialsJson is missing private_key.");
  }

  return {
    clientEmail: clientEmail.trim(),
    privateKey,
    tokenUri: typeof tokenUri === "string" && tokenUri.trim()
      ? tokenUri.trim()
      : "https://oauth2.googleapis.com/token",
  };
}

function buildServiceAccountJwt(credentials: ServiceAccountCredentials, nowMs = Date.now()): string {
  const issuedAt = Math.floor(nowMs / 1000);
  const expiresAt = issuedAt + 3600;
  const header = base64Url(JSON.stringify({ alg: "RS256", typ: "JWT" }));
  const claims = base64Url(JSON.stringify({
    iss: credentials.clientEmail,
    scope: BIGQUERY_SCOPE,
    aud: credentials.tokenUri,
    iat: issuedAt,
    exp: expiresAt,
  }));
  const signingInput = `${header}.${claims}`;
  const signature = createSign("RSA-SHA256")
    .update(signingInput)
    .sign(credentials.privateKey);
  return `${signingInput}.${base64Url(signature)}`;
}

async function getServiceAccountAccessToken(credentials: ServiceAccountCredentials): Promise<string> {
  const cached = accessTokenCache.get(credentials.clientEmail);
  if (cached && cached.expiresAtMs - Date.now() > 60_000) {
    return cached.token;
  }

  const assertion = buildServiceAccountJwt(credentials);
  const response = await fetch(credentials.tokenUri, {
    method: "POST",
    headers: {
      "content-type": "application/x-www-form-urlencoded",
    },
    body: new URLSearchParams({
      grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
      assertion,
    }).toString(),
  });

  const body = await response.text();
  if (!response.ok) {
    throw new Error(`BigQuery service account token request failed with ${response.status}: ${body.slice(0, 500)}`);
  }

  const parsed = JSON.parse(body) as { access_token?: unknown; expires_in?: unknown };
  if (typeof parsed.access_token !== "string" || !parsed.access_token) {
    throw new Error("BigQuery service account token response did not include access_token.");
  }

  const expiresInSeconds = typeof parsed.expires_in === "number" ? parsed.expires_in : 3600;
  accessTokenCache.set(credentials.clientEmail, {
    token: parsed.access_token,
    expiresAtMs: Date.now() + expiresInSeconds * 1000,
  });

  return parsed.access_token;
}

function redactSensitiveValue(key: string, value: unknown): unknown {
  if (/credential|private[_-]?key|token|secret|password/i.test(key)) {
    return "[redacted]";
  }
  if (Array.isArray(value)) {
    return value.map((item) => typeof item === "object" && item !== null ? redactSensitiveObject(item as Record<string, unknown>) : item);
  }
  if (value && typeof value === "object") {
    return redactSensitiveObject(value as Record<string, unknown>);
  }
  return value;
}

function redactSensitiveObject(value: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(value).map(([key, entry]) => [key, redactSensitiveValue(key, entry)]),
  );
}

function redactWorkItem(item: AdapterWorkItem): AdapterWorkItem {
  return {
    ...item,
    connectorConfig: item.connectorConfig
      ? redactSensitiveObject(item.connectorConfig)
      : undefined,
  };
}

function buildWarehouseRow(item: AdapterWorkItem): Record<string, unknown> {
  return {
    workId: item.workId,
    connectorId: item.connectorId,
    capabilityId: item.capabilityId,
    deploymentId: item.deploymentId,
    flowId: item.flowId,
    revisionId: item.revisionId,
    sinkId: item.sinkId,
    tenantId: item.tenantId,
    attempt: item.attempt,
    sourceRef: item.sourceRef,
    traceId: item.traceId,
    messageId: item.messageId,
    enqueuedAt: item.enqueuedAt,
    envelope: item.envelope,
    payload: item.payload,
  };
}

function bigQueryPayloadRow(payload: unknown): Record<string, unknown> {
  if (payload && typeof payload === "object" && !Array.isArray(payload)) {
    return payload as Record<string, unknown>;
  }
  return { value: payload };
}

function readBigQueryTarget(
  config: Record<string, unknown> | undefined,
  credentials: ServiceAccountCredentials,
): BigQueryTarget {
  const project = readStringConfig(config, "project", "local-project");
  return {
    project,
    jobProject: readStringConfig(config, "jobProject", project),
    dataset: readStringConfig(config, "dataset", "event_router"),
    table: readStringConfig(config, "table", "ingest_events"),
    credentials,
    batchCount: readPositiveIntegerConfig(config, "batchCount", 500, 1, 10_000),
    batchPeriodMs: readDurationConfigMs(config, "batchPeriod", 5_000, 50, 60_000),
    maxInFlightBatches: readPositiveIntegerConfig(config, "maxInFlight", 4, 1, 32),
  };
}

function bigQueryTargetKey(target: BigQueryTarget): string {
  return JSON.stringify({
    project: target.project,
    jobProject: target.jobProject,
    dataset: target.dataset,
    table: target.table,
    clientEmail: target.credentials.clientEmail,
    batchCount: target.batchCount,
    batchPeriodMs: target.batchPeriodMs,
    maxInFlightBatches: target.maxInFlightBatches,
  });
}

function loadBigQueryStorageModule(): Promise<BigQueryStorageModule> {
  bigQueryStorageModulePromise ??= import("@google-cloud/bigquery-storage");
  return bigQueryStorageModulePromise;
}

class ManagedBigQueryStorageWriter implements BigQueryStorageWriterLike {
  private resources:
    | {
      writeClient: { close(): void };
      writer: {
        appendRows(rows: Array<Record<string, unknown>>): { getResult(): Promise<unknown> };
        close(): void;
      };
    }
    | undefined;
  private resourcesPromise:
    | Promise<NonNullable<ManagedBigQueryStorageWriter["resources"]>>
    | undefined;

  public constructor(private readonly target: BigQueryTarget) {}

  public async appendRows(rows: Array<Record<string, unknown>>): Promise<void> {
    const { writer } = await this.getResources();
    const pendingWrite = writer.appendRows(rows);
    await pendingWrite.getResult();
  }

  public close(): void {
    this.resources?.writer.close();
    this.resources?.writeClient.close();
    this.resources = undefined;
    this.resourcesPromise = undefined;
  }

  private async getResources(): Promise<NonNullable<ManagedBigQueryStorageWriter["resources"]>> {
    if (this.resources) {
      return this.resources;
    }

    this.resourcesPromise ??= this.createResources();
    try {
      this.resources = await this.resourcesPromise;
      return this.resources;
    } catch (error) {
      this.resourcesPromise = undefined;
      throw error;
    }
  }

  private async createResources(): Promise<NonNullable<ManagedBigQueryStorageWriter["resources"]>> {
    const { adapt, managedwriter } = await loadBigQueryStorageModule();
    const destinationTable = `projects/${this.target.project}/datasets/${this.target.dataset}/tables/${this.target.table}`;
    const writeClient = new managedwriter.WriterClient({
      projectId: this.target.jobProject,
      credentials: {
        client_email: this.target.credentials.clientEmail,
        private_key: this.target.credentials.privateKey,
      },
    });
    writeClient.enableWriteRetries(true);
    writeClient.setMaxRetryAttempts(3);

    const writeStream = await writeClient.getWriteStream({
      streamId: `${destinationTable}/streams/_default`,
      view: "FULL" as never,
    });
    if (!writeStream.tableSchema) {
      throw new Error("BigQuery Storage Write API did not return the destination table schema.");
    }

    const protoDescriptor = adapt.convertStorageSchemaToProto2Descriptor(
      writeStream.tableSchema,
      "root",
    );
    const connection = await writeClient.createStreamConnection({
      streamId: managedwriter.DefaultStream,
      destinationTable,
    });
    const writer = new managedwriter.JSONWriter({
      connection,
      protoDescriptor,
    });

    return { writeClient, writer };
  }
}

let bigQueryStorageWriterFactory: BigQueryStorageWriterFactory = (target) =>
  new ManagedBigQueryStorageWriter(target);

export function setBigQueryStorageWriterFactoryForTest(factory?: BigQueryStorageWriterFactory): void {
  bigQueryStorageWriterFactory = factory ?? ((target) => new ManagedBigQueryStorageWriter(target));
}

class BigQueryBatcher {
  private readonly entries: BigQueryBatchEntry[] = [];
  private readonly idleResolvers: Array<() => void> = [];
  private timer: ReturnType<typeof setTimeout> | undefined;
  private inFlightBatches = 0;
  private writer: BigQueryStorageWriterLike | undefined;

  public constructor(
    private readonly target: BigQueryTarget,
    private readonly writerFactory: BigQueryStorageWriterFactory,
  ) {}

  public enqueue(item: AdapterWorkItem, materialized: BigQueryDeliveryMetadata): Promise<BigQueryDeliveryMetadata> {
    return new Promise((resolve, reject) => {
      this.entries.push({ item, materialized, resolve, reject });
      this.pump(false);
    });
  }

  public flushNow(): void {
    this.pump(true);
  }

  public async close(): Promise<void> {
    this.flushNow();
    if (this.entries.length > 0 || this.inFlightBatches > 0) {
      await new Promise<void>((resolve) => {
        this.idleResolvers.push(resolve);
      });
    }
    this.writer?.close();
    this.writer = undefined;
  }

  private pump(force: boolean): void {
    if (force && this.timer) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }

    while (
      this.inFlightBatches < this.target.maxInFlightBatches
      && this.entries.length > 0
      && (force || this.entries.length >= this.target.batchCount)
    ) {
      const batch = this.entries.splice(0, this.target.batchCount);
      this.inFlightBatches += 1;
      void this.writeBatch(batch).finally(() => {
        this.inFlightBatches -= 1;
        if (this.entries.length >= this.target.batchCount) {
          this.pump(false);
        } else {
          this.armTimer();
        }
        this.resolveIdleIfDone();
      });
    }

    this.armTimer();
    this.resolveIdleIfDone();
  }

  private armTimer(): void {
    if (this.entries.length === 0 || this.entries.length >= this.target.batchCount || this.timer) {
      return;
    }

    this.timer = setTimeout(() => {
      this.timer = undefined;
      this.pump(true);
    }, this.target.batchPeriodMs);
  }

  private async writeBatch(batch: BigQueryBatchEntry[]): Promise<void> {
    try {
      this.writer ??= this.writerFactory(this.target);
      await this.writer.appendRows(batch.map((entry) => bigQueryPayloadRow(entry.item.payload)));
      for (const entry of batch) {
        entry.resolve(entry.materialized);
      }
    } catch (error) {
      for (const entry of batch) {
        entry.reject(error);
      }
    }
  }

  private resolveIdleIfDone(): void {
    if (this.entries.length > 0 || this.inFlightBatches > 0) {
      return;
    }

    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }

    while (this.idleResolvers.length > 0) {
      this.idleResolvers.pop()?.();
    }
  }
}

function consumerName(consumer: JetStreamConsumerLike): string | undefined {
  return consumer.name ?? consumer.config?.durable_name;
}

export function shouldTerminateWorkMessage(
  redeliveryCount: number,
  maxDeliver: number = ADAPTER_MAX_DELIVER,
): boolean {
  return redeliveryCount >= maxDeliver;
}

export function workNakDelayMs(redeliveryCount: number): number {
  return ADAPTER_NAK_BASE_DELAY_MS * Math.max(1, redeliveryCount);
}

export function selectAdapterRuntimeConsumersToDelete(
  consumers: JetStreamConsumerLike[],
  expectedConsumerName: string,
  expectedSubject: string,
): string[] {
  return consumers.flatMap((consumer) => {
    const name = consumerName(consumer);
    if (!name) {
      return [];
    }

    if (name === "adapter_redpanda_work") {
      return [name];
    }

    if (
      name === expectedConsumerName
      && (
        consumer.config?.filter_subject !== expectedSubject
        || consumer.config?.ack_wait !== ADAPTER_ACK_WAIT_NS
        || consumer.config?.max_ack_pending !== ADAPTER_MAX_ACK_PENDING
        || consumer.config?.max_deliver !== ADAPTER_MAX_DELIVER
      )
    ) {
      return [name];
    }

    return [];
  });
}

function readNatsApiError(error: unknown): { code?: number; errCode?: number } | undefined {
  if (!error || typeof error !== "object" || !("api_error" in error)) {
    return undefined;
  }

  const apiError = (error as { api_error?: { code?: unknown; err_code?: unknown } }).api_error;
  if (!apiError || typeof apiError !== "object") {
    return undefined;
  }

  return {
    code: typeof apiError.code === "number" ? apiError.code : undefined,
    errCode: typeof apiError.err_code === "number" ? apiError.err_code : undefined,
  };
}

function isFilteredConsumerConflict(error: unknown): boolean {
  const apiError = readNatsApiError(error);
  return apiError?.code === 400 && apiError.errCode === 10100;
}

async function postAudit(
  config: AdapterRedpandaConfig,
  item: AdapterWorkItem,
  record: AdapterDeliveryRecord,
): Promise<void> {
  if (!config.controlApiUrl || !config.controlApiToken) {
    return;
  }

  try {
    await fetch(`${config.controlApiUrl.replace(/\/$/, "")}/api/runtime/audit`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${config.controlApiToken}`,
      },
      body: JSON.stringify({
        tenantId: item.tenantId,
        actor: config.serviceName,
        action:
          record.status === "delivered"
            ? "adapter.delivery.succeeded"
            : "adapter.delivery.failed",
        subjectType: "adapter-work",
        subjectId: item.workId,
        details: {
          connectorId: item.connectorId,
          capabilityId: item.capabilityId,
          flowId: item.flowId,
          revisionId: item.revisionId,
          deploymentId: item.deploymentId,
          messageId: item.messageId,
          traceId: item.traceId,
          mirrorSubject: record.mirrorSubject,
          topic: record.topic,
          targetRef: record.targetRef,
          artifactPath: record.artifactPath,
          objectKey: record.objectKey,
          deduplicated: record.deduplicated,
          error: record.error,
        },
      }),
    });
  } catch {
    // Adapter-side audit fanout must not break delivery processing.
  }
}

async function postRunResult(
  config: AdapterRedpandaConfig,
  item: AdapterWorkItem,
  record: AdapterDeliveryRecord,
): Promise<void> {
  if (!config.controlApiUrl || !config.controlApiToken || !item.runId) {
    return;
  }

  try {
    await fetch(`${config.controlApiUrl.replace(/\/$/, "")}/api/runtime/adapter-results`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${config.controlApiToken}`,
      },
      body: JSON.stringify({
        runId: item.runId,
        sinkId: item.sinkId,
        connectorId: item.connectorId,
        capabilityId: item.capabilityId,
        status: record.status === "delivered" ? "succeeded" : "failed",
        targetRef: record.targetRef ?? null,
        artifactPath: record.artifactPath ?? null,
        objectKey: record.objectKey ?? null,
        error: record.error ?? null,
        startedAt: record.receivedAt,
        finishedAt: record.completedAt,
      }),
    });
  } catch {
    // Run-result reporting must not break adapter delivery.
  }
}

export class AdapterRedpandaRuntime {
  private readonly deliveries: AdapterDeliveryRecord[] = [];
  private nc: NatsConnection | undefined;
  private subscription: { unsubscribe(): Promise<void> } | undefined;
  private successes = 0;
  private failures = 0;
  private mirroredPublishes = 0;
  private lastDeliveryAt: string | undefined;
  private readonly workSubject = buildAdapterWorkSubjectPattern("inline");
  private readonly consumerName = "adapter_redpanda_work_v2";
  private readonly queueGroup = "adapter_redpanda";
  private readonly activeDeliveries = new Set<Promise<void>>();
  private readonly bigQueryBatchers = new Map<string, BigQueryBatcher>();
  private processingLoop: Promise<void> | undefined;

  public constructor(private readonly config: AdapterRedpandaConfig) {}

  public async start(): Promise<void> {
    if (!this.config.natsUrl) {
      return;
    }

    this.nc = await connect({
      servers: this.config.natsUrl,
      name: this.config.serviceName,
    });

    const js = this.nc.jetstream();
    const jsm = await this.nc.jetstreamManager();

    await this.deleteStaleWorkConsumers(jsm);

    const opts = consumerOpts();
    opts.deliverTo(createInbox());
    opts.durable(this.consumerName);
    opts.queue(this.queueGroup);
    opts.manualAck();
    opts.deliverAll();
    opts.ackExplicit();
    opts.ackWait(ADAPTER_ACK_WAIT_MS);
    opts.maxAckPending(ADAPTER_MAX_ACK_PENDING);
    opts.maxDeliver(ADAPTER_MAX_DELIVER);

    let subscription: Awaited<ReturnType<typeof js.subscribe>>;
    try {
      subscription = await js.subscribe(this.workSubject, opts);
    } catch (error) {
      if (!isFilteredConsumerConflict(error)) {
        throw error;
      }

      await this.deleteStaleWorkConsumers(jsm);
      subscription = await js.subscribe(this.workSubject, opts);
    }
    this.subscription = {
      async unsubscribe() {
        subscription.unsubscribe();
      },
    };

    const loop = (async () => {
      for await (const message of subscription) {
        while (this.activeDeliveries.size >= ADAPTER_MAX_ACK_PENDING) {
          await Promise.race(this.activeDeliveries);
        }

        const jsMessage = message as JsMsg;
        const task = this.handleWorkMessage(jsMessage);
        this.activeDeliveries.add(task);
        void task.finally(() => {
          this.activeDeliveries.delete(task);
        });
      }
    })();

    this.processingLoop = loop.catch(() => undefined);
  }

  private async handleWorkMessage(jsMessage: JsMsg): Promise<void> {
    try {
      const item = JSON.parse(
        new TextDecoder().decode(jsMessage.data),
      ) as AdapterWorkItem;
      const record = await this.processWorkItem(item);
      if (record.status === "delivered") {
        try {
          jsMessage.ack();
        } catch {
          // The server may already have closed this delivery during shutdown.
        }
      } else {
        this.nakOrTerminate(jsMessage);
      }
    } catch (error) {
      console.error(
        `[${this.config.serviceName}] failed to process work message (stream seq ${jsMessage.info.streamSequence}, delivery ${jsMessage.info.redeliveryCount}): ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      this.nakOrTerminate(jsMessage);
    }
  }

  private nakOrTerminate(jsMessage: JsMsg): void {
    try {
      const redeliveryCount = jsMessage.info.redeliveryCount;
      if (shouldTerminateWorkMessage(redeliveryCount)) {
        // Poison pill: stop occupying the work stream. Terminations must be
        // observable — for parsed items the failed delivery record was already
        // reported by processWorkItem; this line covers every branch.
        console.error(
          `[${this.config.serviceName}] terminating work message after ${redeliveryCount} deliveries (stream seq ${jsMessage.info.streamSequence})`,
        );
        jsMessage.term();
      } else {
        jsMessage.nak(workNakDelayMs(redeliveryCount));
      }
    } catch {
      // The server may already have closed this delivery during shutdown.
    }
  }

  private async deleteStaleWorkConsumers(
    jsm: Awaited<ReturnType<NatsConnection["jetstreamManager"]>>,
  ): Promise<void> {
    const lister = jsm.consumers.list("work");
    const consumers: JetStreamConsumerLike[] = [];

    for await (const consumer of lister) {
      consumers.push(consumer as JetStreamConsumerLike);
    }

    for (const consumerToDelete of selectAdapterRuntimeConsumersToDelete(
      consumers,
      this.consumerName,
      this.workSubject,
    )) {
      try {
        await jsm.consumers.delete("work", consumerToDelete);
      } catch {
        // Ignore consumers deleted by another rolling instance.
      }
    }
  }

  public async stop(): Promise<void> {
    if (this.subscription) {
      await this.subscription.unsubscribe();
      this.subscription = undefined;
    }
    if (this.processingLoop) {
      await this.processingLoop;
      this.processingLoop = undefined;
    }
    for (const batcher of this.bigQueryBatchers.values()) {
      batcher.flushNow();
    }
    if (this.activeDeliveries.size > 0) {
      await Promise.allSettled([...this.activeDeliveries]);
    }
    for (const batcher of this.bigQueryBatchers.values()) {
      await batcher.close();
    }
    this.bigQueryBatchers.clear();

    if (this.nc) {
      await this.nc.drain();
      await this.nc.close();
      this.nc = undefined;
    }
  }

  public getDeliveries(): AdapterDeliveryRecord[] {
    return [...this.deliveries];
  }

  public getSummary(): AdapterRuntimeSummary {
    return {
      connected: Boolean(this.nc),
      deliveries: this.deliveries.length,
      successes: this.successes,
      failures: this.failures,
      mirroredPublishes: this.mirroredPublishes,
      lastDeliveryAt: this.lastDeliveryAt,
      workSubject: this.workSubject,
      artifactRoot: resolve(this.config.artifactRoot),
    };
  }

  private artifactDir(...segments: string[]): string {
    return resolve(this.config.artifactRoot, ...segments.map((segment) => fileToken(segment)));
  }

  private artifactFileName(label: string, extension: string): string {
    return `${fileToken(label)}${extension}`;
  }

  private ensureParent(path: string): void {
    mkdirSync(dirname(path), { recursive: true });
  }

  private materializeSnowflake(item: AdapterWorkItem): Pick<
    AdapterDeliveryRecord,
    "targetRef" | "artifactPath" | "deduplicated"
  > {
    const account = readStringConfig(item.connectorConfig, "account", "local-account");
    const database = readStringConfig(item.connectorConfig, "database", "router");
    const schema = readStringConfig(item.connectorConfig, "schema", "public");
    const table = readStringConfig(item.connectorConfig, "table", "events_router_ingest");
    const targetRef = `snowflake://${account}/${database}/${schema}/${table}`;
    const artifactPath = resolve(
      this.artifactDir("snowflake", account, database, schema),
      this.artifactFileName(table, ".ndjson"),
    );
    const dedupePath = resolve(
      this.artifactDir(
        "snowflake",
        ".dedupe",
        account,
        database,
        schema,
        table,
        item.connectorId,
      ),
      this.artifactFileName(item.messageId, ".marker"),
    );

    if (existsSync(dedupePath)) {
      return {
        targetRef,
        artifactPath,
        deduplicated: true,
      };
    }

    this.ensureParent(artifactPath);
    appendFileSync(artifactPath, `${JSON.stringify(buildWarehouseRow(item))}\n`);
    this.ensureParent(dedupePath);
    writeFileSync(dedupePath, `${item.workId}\n`);

    return {
      targetRef,
      artifactPath,
      deduplicated: false,
    };
  }

  private bigQueryMetadata(item: AdapterWorkItem): BigQueryDeliveryMetadata {
    const project = readStringConfig(item.connectorConfig, "project", "local-project");
    const dataset = readStringConfig(item.connectorConfig, "dataset", "event_router");
    const table = readStringConfig(item.connectorConfig, "table", "ingest_events");
    const targetRef = `bigquery://${project}/${dataset}.${table}`;

    return {
      targetRef,
      deduplicated: false,
    };
  }

  private materializeBigQuery(item: AdapterWorkItem): BigQueryDeliveryMetadata {
    const project = readStringConfig(item.connectorConfig, "project", "local-project");
    const dataset = readStringConfig(item.connectorConfig, "dataset", "event_router");
    const table = readStringConfig(item.connectorConfig, "table", "ingest_events");
    const metadata = this.bigQueryMetadata(item);
    const artifactPath = resolve(
      this.artifactDir("bigquery", project, dataset),
      this.artifactFileName(table, ".ndjson"),
    );

    this.ensureParent(artifactPath);
    appendFileSync(artifactPath, `${JSON.stringify(buildWarehouseRow(item))}\n`);

    return {
      targetRef: metadata.targetRef,
      artifactPath,
      deduplicated: false,
    };
  }

  private getBigQueryBatcher(target: BigQueryTarget): BigQueryBatcher {
    const key = bigQueryTargetKey(target);
    let batcher = this.bigQueryBatchers.get(key);
    if (!batcher) {
      batcher = new BigQueryBatcher(target, bigQueryStorageWriterFactory);
      this.bigQueryBatchers.set(key, batcher);
    }
    return batcher;
  }

  private async deliverBigQueryInsertAll(
    item: AdapterWorkItem,
    credentials: ServiceAccountCredentials,
  ): Promise<void> {
    const project = readStringConfig(item.connectorConfig, "project", "local-project");
    const jobProject = readStringConfig(item.connectorConfig, "jobProject", project);
    const dataset = readStringConfig(item.connectorConfig, "dataset", "event_router");
    const table = readStringConfig(item.connectorConfig, "table", "ingest_events");
    const ignoreUnknownValues = readBooleanConfig(item.connectorConfig, "ignoreUnknownValues", true);
    const skipInvalidRows = readBooleanConfig(item.connectorConfig, "skipInvalidRows", false);
    const token = await getServiceAccountAccessToken(credentials);
    const url = new URL(
      `https://bigquery.googleapis.com/bigquery/v2/projects/${encodeURIComponent(jobProject)}/datasets/${encodeURIComponent(dataset)}/tables/${encodeURIComponent(table)}/insertAll`,
    );

    const response = await fetch(url, {
      method: "POST",
      headers: {
        authorization: `Bearer ${token}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({
        kind: "bigquery#tableDataInsertAllRequest",
        ignoreUnknownValues,
        skipInvalidRows,
        rows: [
          {
            insertId: item.messageId,
            json: bigQueryPayloadRow(item.payload),
          },
        ],
      }),
    });

    const body = await response.text();
    if (!response.ok) {
      throw new Error(`BigQuery insertAll failed with ${response.status}: ${body.slice(0, 1_000)}`);
    }

    const parsed = body.trim() ? JSON.parse(body) as { insertErrors?: unknown } : {};
    if (Array.isArray(parsed.insertErrors) && parsed.insertErrors.length > 0) {
      throw new Error(`BigQuery insertAll returned row errors: ${JSON.stringify(parsed.insertErrors).slice(0, 1_000)}`);
    }
  }

  private async deliverBigQuery(item: AdapterWorkItem): Promise<BigQueryDeliveryMetadata> {
    const credentials = parseServiceAccountCredentials(item.connectorConfig);
    if (!credentials) {
      return this.materializeBigQuery(item);
    }

    const metadata = this.bigQueryMetadata(item);
    if (readBigQueryWriteMethod(item.connectorConfig) === "insert_all") {
      await this.deliverBigQueryInsertAll(item, credentials);
      return metadata;
    }

    const target = readBigQueryTarget(item.connectorConfig, credentials);
    return this.getBigQueryBatcher(target).enqueue(item, metadata);
  }

  private materializeS3(item: AdapterWorkItem): Pick<
    AdapterDeliveryRecord,
    "targetRef" | "artifactPath" | "objectKey" | "deduplicated"
  > {
    const bucket = readStringConfig(item.connectorConfig, "bucket", "event-router-v1");
    const prefix = readStringConfig(item.connectorConfig, "prefix", "events/");
    const observedAt = item.envelope.receivedAt.slice(0, 10);
    const datePath = /^\d{4}-\d{2}-\d{2}$/.test(observedAt) ? observedAt.replace(/-/g, "/") : "unknown-date";
    const prefixSegments = prefix
      .split("/")
      .map((segment) => segment.trim())
      .filter((segment) => segment.length > 0);
    const fileName = `${fileToken(item.messageId)}-${fileToken(item.workId)}.json`;
    const objectKey = [...prefixSegments.map((segment) => fileToken(segment)), ...datePath.split("/"), fileName].join("/");
    const artifactPath = resolve(
      this.artifactDir("s3", bucket, ...prefixSegments, ...datePath.split("/")),
      fileName,
    );

    this.ensureParent(artifactPath);
    writeFileSync(artifactPath, `${JSON.stringify(buildWarehouseRow(item), null, 2)}\n`);

    return {
      targetRef: `s3://${bucket}/${objectKey}`,
      artifactPath,
      objectKey,
      deduplicated: false,
    };
  }

  public async processWorkItem(item: AdapterWorkItem): Promise<AdapterDeliveryRecord> {
    const startedAt = isoNow();
    const topic =
      typeof item.connectorConfig?.topic === "string"
        ? item.connectorConfig.topic
        : undefined;

    let record: AdapterDeliveryRecord;
    try {
      let mirrorSubject: string | undefined;
      let targetRef: string | undefined;
      let artifactPath: string | undefined;
      let objectKey: string | undefined;
      let deduplicated: boolean | undefined;

      switch (item.capabilityId) {
        case "kafka_out": {
          if (!topic) {
            throw new Error("adapter kafka_out work item is missing connectorConfig.topic");
          }

          mirrorSubject = `adapter.kafka.${sanitizeSubjectSegment(topic)}`;
          targetRef = `kafka://${topic}`;
          if (this.nc) {
            this.nc.publish(
              mirrorSubject,
              stringCodec.encode(
                JSON.stringify({
                  workId: item.workId,
                  connectorId: item.connectorId,
                  capabilityId: item.capabilityId,
                  deploymentId: item.deploymentId,
                  flowId: item.flowId,
                  revisionId: item.revisionId,
                  traceId: item.traceId,
                  messageId: item.messageId,
                  payload: item.payload,
                }),
              ),
            );
            await this.nc.flush();
          }

          this.mirroredPublishes += 1;
          break;
        }
        case "snowflake_sink": {
          ({ targetRef, artifactPath, deduplicated } = this.materializeSnowflake(item));
          break;
        }
        case "bigquery_sink": {
          ({ targetRef, artifactPath, deduplicated } = await this.deliverBigQuery(item));
          break;
        }
        case "s3_sink": {
          ({ targetRef, artifactPath, objectKey, deduplicated } = this.materializeS3(item));
          break;
        }
        default:
          throw new Error(`unsupported adapter capability: ${item.capabilityId}`);
      }

      record = {
        id: `adapter-delivery-${crypto.randomUUID()}`,
        workId: item.workId,
        connectorId: item.connectorId,
        capabilityId: item.capabilityId,
        flowId: item.flowId,
        revisionId: item.revisionId,
        deploymentId: item.deploymentId,
        messageId: item.messageId,
        traceId: item.traceId,
        status: "delivered",
        mirrorSubject,
        topic,
        targetRef,
        artifactPath,
        objectKey,
        deduplicated,
        receivedAt: startedAt,
        completedAt: isoNow(),
      };

      this.successes += 1;
    } catch (error) {
      record = {
        id: `adapter-delivery-${crypto.randomUUID()}`,
        workId: item.workId,
        connectorId: item.connectorId,
        capabilityId: item.capabilityId,
        flowId: item.flowId,
        revisionId: item.revisionId,
        deploymentId: item.deploymentId,
        messageId: item.messageId,
        traceId: item.traceId,
        status: "failed",
        topic,
        error: error instanceof Error ? error.message : String(error),
        receivedAt: startedAt,
        completedAt: isoNow(),
      };

      this.failures += 1;
    }

    this.lastDeliveryAt = record.completedAt;
    this.deliveries.unshift(record);
    this.deliveries.splice(200);

    if (this.config.deliveryLogEnabled) {
      const logPath = resolve(this.config.deliveryLogPath);
      mkdirSync(dirname(logPath), { recursive: true });
      appendFileSync(logPath, `${JSON.stringify({ record, item: redactWorkItem(item) })}\n`);
    }

    await postRunResult(this.config, item, record);
    await postAudit(this.config, item, record);
    return record;
  }
}
