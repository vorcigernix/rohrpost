import type { Database } from "bun:sqlite";
import type { ControlApiConfig } from "../config";
import type {
  AdapterWorkloadStatusRecord,
  AiProviderSettingsRecord,
  DeploymentRecord,
  FlowRevisionRecord,
  ReplayRequestRecord,
  ResolvedConnectorRecord,
} from "./types";
import type { CompiledFlowSummary, FlowSpec, SimulationReport } from "@rohrpost/shared-flow-spec";

export function isoNow(): string {
  return new Date().toISOString();
}

export function id(prefix: string): string {
  return `${prefix}_${crypto.randomUUID().replaceAll("-", "")}`;
}

export function parseJson<T>(value: unknown): T {
  if (typeof value !== "string") {
    throw new Error("Expected JSON string");
  }

  return JSON.parse(value) as T;
}

export function maxIsoString(...values: Array<string | null | undefined>): string | null {
  const filtered = values.filter((value): value is string => typeof value === "string" && value.length > 0);
  if (filtered.length === 0) {
    return null;
  }

  filtered.sort((left, right) => Date.parse(left) - Date.parse(right));
  return filtered[filtered.length - 1] ?? null;
}

export function parseStringArray(value: unknown): string[] {
  if (typeof value !== "string") {
    return [];
  }

  try {
    const parsed = JSON.parse(value) as unknown;
    return Array.isArray(parsed)
      ? parsed.filter((entry): entry is string => typeof entry === "string")
      : [];
  } catch {
    return [];
  }
}

export function mapDeploymentRow(row: Record<string, unknown>): DeploymentRecord {
  return {
    id: String(row.id),
    flowId: String(row.flow_id),
    revisionId: String(row.revision_id),
    status: String(row.status),
    rolloutStatus: String(row.rollout_status),
    createdAt: String(row.created_at),
    rolledBackFrom: row.rolled_back_from ? String(row.rolled_back_from) : null,
  };
}

export function mapRevisionRow(row: Record<string, unknown>): FlowRevisionRecord {
  return {
    id: String(row.id),
    flowId: String(row.flow_id),
    revisionNumber: Number(row.revision_number),
    spec: parseJson<FlowSpec>(row.spec_json),
    compiler: parseJson<CompiledFlowSummary>(row.compiler_json),
    simulation: parseJson<SimulationReport>(row.simulation_json),
    publishedAt: row.published_at ? String(row.published_at) : null,
    createdAt: String(row.created_at),
  };
}

export function mapReplayRow(row: Record<string, unknown>): ReplayRequestRecord {
  return {
    id: String(row.id),
    flowId: String(row.flow_id),
    revisionId: String(row.revision_id),
    reason: String(row.reason),
    sourceStream: String(row.source_stream),
    status: String(row.status),
    createdAt: String(row.created_at),
  };
}

export function mapAdapterWorkloadStatusRow(row: Record<string, unknown>): AdapterWorkloadStatusRecord {
  return {
    reporterId: String(row.reporter_id),
    reportedAt: String(row.reported_at),
    key: String(row.workload_key),
    connectorId: String(row.connector_id),
    capabilityId: String(row.capability_id),
    manifestId: String(row.manifest_id),
    deploymentIds: parseStringArray(row.deployment_ids_json),
    flowIds: parseStringArray(row.flow_ids_json),
    revisionIds: parseStringArray(row.revision_ids_json),
    runtimeRole: String(row.runtime_role) as AdapterWorkloadStatusRecord["runtimeRole"],
    inputRef: String(row.input_ref),
    outputRef: String(row.output_ref),
    status: String(row.status) as AdapterWorkloadStatusRecord["status"],
    backend: String(row.backend) as AdapterWorkloadStatusRecord["backend"],
    consumerRef: row.consumer_ref ? String(row.consumer_ref) : undefined,
    targetKind: String(row.target_kind) as AdapterWorkloadStatusRecord["targetKind"],
    artifactPath: row.artifact_path ? String(row.artifact_path) : undefined,
    configPath: String(row.config_path),
    containerName: row.container_name ? String(row.container_name) : undefined,
    startedAt: row.started_at ? String(row.started_at) : undefined,
    stoppedAt: row.stopped_at ? String(row.stopped_at) : undefined,
    lastError: row.last_error ? String(row.last_error) : undefined,
    restartCount: Number(row.restart_count),
    recentLogs: parseStringArray(row.recent_logs_json),
  };
}

export function mapConnectorRow(row: Record<string, unknown>): ResolvedConnectorRecord {
  return {
    id: String(row.id),
    tenantId: String(row.tenant_id),
    name: String(row.name),
    capabilityId: String(row.capability_id),
    executionMode: String(row.execution_mode) as "native" | "adapter",
    config: parseJson<Record<string, unknown>>(row.config_json),
    createdAt: String(row.created_at),
  };
}

export function mapAiProviderSettingsRow(row: Record<string, unknown>): AiProviderSettingsRecord {
  return {
    provider: "gemini",
    enabled: Number(row.enabled) === 1,
    apiKey: typeof row.api_key === "string" && row.api_key.length > 0 ? row.api_key : null,
    model: typeof row.model === "string" && row.model.length > 0 ? row.model : "gemini-2.5-flash",
    apiBaseUrl:
      typeof row.api_base_url === "string" && row.api_base_url.length > 0
        ? row.api_base_url
        : "https://generativelanguage.googleapis.com/v1beta",
    source: "database",
    updatedAt: typeof row.updated_at === "string" ? row.updated_at : null,
  };
}

export function defaultAiProviderSettings(config: ControlApiConfig): AiProviderSettingsRecord {
  const apiKey = config.geminiApiKey?.trim() || null;
  return {
    provider: "gemini",
    enabled: Boolean(apiKey),
    apiKey,
    model: config.geminiModel ?? "gemini-2.5-flash",
    apiBaseUrl: config.geminiApiBaseUrl ?? "https://generativelanguage.googleapis.com/v1beta",
    source: apiKey ? "environment" : "unset",
    updatedAt: null,
  };
}

export function getLatestRevision(db: Database, flowId: string): FlowRevisionRecord | null {
  const row = db
    .query("SELECT * FROM flow_revisions WHERE flow_id = ? ORDER BY revision_number DESC LIMIT 1")
    .get(flowId) as Record<string, unknown> | null;

  return row ? mapRevisionRow(row) : null;
}

export function getRevision(db: Database, flowId: string, revisionId?: string | null): FlowRevisionRecord | null {
  if (!revisionId) {
    return getLatestRevision(db, flowId);
  }

  const row = db
    .query("SELECT * FROM flow_revisions WHERE flow_id = ? AND id = ? LIMIT 1")
    .get(flowId, revisionId) as Record<string, unknown> | null;

  return row ? mapRevisionRow(row) : null;
}

export function getReplayRequest(db: Database, replayId: string): ReplayRequestRecord | null {
  const row = db
    .query("SELECT * FROM replay_requests WHERE id = ? LIMIT 1")
    .get(replayId) as Record<string, unknown> | null;

  return row ? mapReplayRow(row) : null;
}

export function resolveConnectorIds(spec: FlowSpec): string[] {
  const ids = new Set<string>();

  for (const source of spec.sources) {
    ids.add(source.connector.connectorId);
  }

  for (const processor of spec.processors) {
    if (processor.connector) {
      ids.add(processor.connector.connectorId);
    }
  }

  for (const sink of spec.sinks) {
    ids.add(sink.connector.connectorId);
  }

  return [...ids];
}

export function resolveConnectors(db: Database, connectorIds: string[]): Record<string, ResolvedConnectorRecord> {
  const connectors: Record<string, ResolvedConnectorRecord> = {};

  for (const connectorId of connectorIds) {
    const row = db
      .query("SELECT * FROM connectors WHERE id = ? LIMIT 1")
      .get(connectorId) as Record<string, unknown> | null;

    if (!row) {
      continue;
    }

    connectors[connectorId] = mapConnectorRow(row);
  }

  return connectors;
}

export function addAudit(
  db: Database,
  tenantId: string,
  actor: string,
  action: string,
  subjectType: string,
  subjectId: string,
  details: unknown,
): void {
  db.query(
    "INSERT INTO audit_records (id, tenant_id, actor, action, subject_type, subject_id, details_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
  ).run(id("audit"), tenantId, actor, action, subjectType, subjectId, JSON.stringify(details), isoNow());
}
