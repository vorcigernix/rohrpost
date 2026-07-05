import type { Database } from "bun:sqlite";
import { CONNECTOR_CAPABILITIES } from "@rohrpost/domain-connectors";
import type { ControlApiConfig } from "../config";
import { id, isoNow } from "./shared";

function ensureColumnExists(db: Database, tableName: string, columnName: string, definition: string): void {
  const columns = db
    .query(`PRAGMA table_info(${tableName})`)
    .all() as Array<{ name?: string }>;

  if (columns.some((column) => column.name === columnName)) {
    return;
  }

  db.exec(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${definition};`);
}

function createSchema(db: Database): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS users (
      id TEXT PRIMARY KEY,
      email TEXT NOT NULL UNIQUE,
      role TEXT NOT NULL,
      created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS api_tokens (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      label TEXT NOT NULL,
      token TEXT NOT NULL UNIQUE,
      created_at TEXT NOT NULL,
      last_used_at TEXT,
      FOREIGN KEY (user_id) REFERENCES users(id)
    );

    CREATE TABLE IF NOT EXISTS tenants (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS connectors (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      name TEXT NOT NULL,
      capability_id TEXT NOT NULL,
      execution_mode TEXT NOT NULL,
      config_json TEXT NOT NULL,
      created_at TEXT NOT NULL,
      FOREIGN KEY (tenant_id) REFERENCES tenants(id)
    );

    CREATE TABLE IF NOT EXISTS secret_references (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      name TEXT NOT NULL,
      provider TEXT NOT NULL,
      reference TEXT NOT NULL,
      created_at TEXT NOT NULL,
      FOREIGN KEY (tenant_id) REFERENCES tenants(id)
    );

    CREATE TABLE IF NOT EXISTS ai_provider_settings (
      id TEXT PRIMARY KEY,
      provider TEXT NOT NULL,
      enabled INTEGER NOT NULL,
      api_key TEXT,
      model TEXT NOT NULL,
      api_base_url TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS flow_definitions (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      name TEXT NOT NULL,
      status TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      FOREIGN KEY (tenant_id) REFERENCES tenants(id)
    );

    CREATE TABLE IF NOT EXISTS flow_revisions (
      id TEXT PRIMARY KEY,
      flow_id TEXT NOT NULL,
      revision_number INTEGER NOT NULL,
      spec_json TEXT NOT NULL,
      compiler_json TEXT NOT NULL,
      simulation_json TEXT NOT NULL,
      created_at TEXT NOT NULL,
      published_at TEXT,
      FOREIGN KEY (flow_id) REFERENCES flow_definitions(id)
    );

    CREATE TABLE IF NOT EXISTS deployments (
      id TEXT PRIMARY KEY,
      flow_id TEXT NOT NULL,
      revision_id TEXT NOT NULL,
      status TEXT NOT NULL,
      rollout_status TEXT NOT NULL,
      created_at TEXT NOT NULL,
      rolled_back_from TEXT,
      FOREIGN KEY (flow_id) REFERENCES flow_definitions(id),
      FOREIGN KEY (revision_id) REFERENCES flow_revisions(id)
    );

    CREATE TABLE IF NOT EXISTS run_summaries (
      id TEXT PRIMARY KEY,
      flow_id TEXT NOT NULL,
      revision_id TEXT NOT NULL,
      deployment_id TEXT,
      message_id TEXT,
      status TEXT NOT NULL,
      source_ref TEXT NOT NULL,
      trace_id TEXT NOT NULL,
      processed_count INTEGER NOT NULL,
      error_count INTEGER NOT NULL,
      started_at TEXT NOT NULL,
      finished_at TEXT NOT NULL,
      last_error TEXT,
      target_sink_ids_json TEXT NOT NULL DEFAULT '[]',
      awaited_sink_ids_json TEXT NOT NULL DEFAULT '[]',
      FOREIGN KEY (flow_id) REFERENCES flow_definitions(id),
      FOREIGN KEY (revision_id) REFERENCES flow_revisions(id)
    );

    CREATE TABLE IF NOT EXISTS adapter_run_results (
      run_id TEXT NOT NULL,
      sink_id TEXT NOT NULL,
      connector_id TEXT NOT NULL,
      capability_id TEXT NOT NULL,
      status TEXT NOT NULL,
      target_ref TEXT,
      artifact_path TEXT,
      object_key TEXT,
      error TEXT,
      started_at TEXT NOT NULL,
      finished_at TEXT NOT NULL,
      PRIMARY KEY (run_id, sink_id),
      FOREIGN KEY (run_id) REFERENCES run_summaries(id)
    );

    CREATE TABLE IF NOT EXISTS runtime_deployment_stats (
      deployment_id TEXT NOT NULL,
      reporter_id TEXT NOT NULL,
      flow_id TEXT NOT NULL,
      revision_id TEXT NOT NULL,
      accepted_count INTEGER NOT NULL,
      processed_count INTEGER NOT NULL,
      delivered_count INTEGER NOT NULL,
      retrying_count INTEGER NOT NULL,
      dlq_count INTEGER NOT NULL,
      failed_count INTEGER NOT NULL,
      filtered_count INTEGER NOT NULL,
      deduped_count INTEGER NOT NULL,
      sink_attempt_count INTEGER NOT NULL,
      sink_success_count INTEGER NOT NULL,
      sink_failure_count INTEGER NOT NULL,
      inflight_count INTEGER NOT NULL,
      backlog_count INTEGER NOT NULL,
      last_accepted_at TEXT,
      last_processed_at TEXT,
      last_error TEXT,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (deployment_id, reporter_id),
      FOREIGN KEY (deployment_id) REFERENCES deployments(id),
      FOREIGN KEY (flow_id) REFERENCES flow_definitions(id),
      FOREIGN KEY (revision_id) REFERENCES flow_revisions(id)
    );

    CREATE TABLE IF NOT EXISTS runtime_samples (
      deployment_id TEXT NOT NULL,
      flow_id TEXT NOT NULL,
      revision_id TEXT NOT NULL,
      source_kind TEXT NOT NULL,
      source_ref TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      observed_at TEXT NOT NULL,
      PRIMARY KEY (deployment_id, source_kind, source_ref),
      FOREIGN KEY (deployment_id) REFERENCES deployments(id),
      FOREIGN KEY (flow_id) REFERENCES flow_definitions(id),
      FOREIGN KEY (revision_id) REFERENCES flow_revisions(id)
    );

    CREATE TABLE IF NOT EXISTS adapter_workload_statuses (
      reporter_id TEXT NOT NULL,
      workload_key TEXT NOT NULL,
      connector_id TEXT NOT NULL,
      capability_id TEXT NOT NULL,
      manifest_id TEXT NOT NULL,
      deployment_ids_json TEXT NOT NULL DEFAULT '[]',
      flow_ids_json TEXT NOT NULL DEFAULT '[]',
      revision_ids_json TEXT NOT NULL DEFAULT '[]',
      runtime_role TEXT NOT NULL,
      input_ref TEXT NOT NULL,
      output_ref TEXT NOT NULL,
      status TEXT NOT NULL,
      backend TEXT NOT NULL,
      consumer_ref TEXT,
      target_kind TEXT NOT NULL,
      artifact_path TEXT,
      config_path TEXT NOT NULL,
      container_name TEXT,
      started_at TEXT,
      stopped_at TEXT,
      last_error TEXT,
      restart_count INTEGER NOT NULL,
      recent_logs_json TEXT NOT NULL,
      reported_at TEXT NOT NULL,
      PRIMARY KEY (reporter_id, workload_key)
    );

    CREATE TABLE IF NOT EXISTS replay_requests (
      id TEXT PRIMARY KEY,
      flow_id TEXT NOT NULL,
      revision_id TEXT NOT NULL,
      reason TEXT NOT NULL,
      source_stream TEXT NOT NULL,
      status TEXT NOT NULL,
      created_at TEXT NOT NULL,
      FOREIGN KEY (flow_id) REFERENCES flow_definitions(id),
      FOREIGN KEY (revision_id) REFERENCES flow_revisions(id)
    );

    CREATE TABLE IF NOT EXISTS audit_records (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      actor TEXT NOT NULL,
      action TEXT NOT NULL,
      subject_type TEXT NOT NULL,
      subject_id TEXT NOT NULL,
      details_json TEXT NOT NULL,
      created_at TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_run_summaries_started_at
    ON run_summaries (started_at DESC);

    CREATE INDEX IF NOT EXISTS idx_run_summaries_status_started_at
    ON run_summaries (status, started_at DESC);

    CREATE INDEX IF NOT EXISTS idx_runtime_samples_observed_at
    ON runtime_samples (observed_at DESC);

    CREATE INDEX IF NOT EXISTS idx_adapter_workload_statuses_reported_at
    ON adapter_workload_statuses (reported_at DESC);
  `);

  ensureColumnExists(db, "run_summaries", "message_id", "TEXT");
  ensureColumnExists(db, "run_summaries", "target_sink_ids_json", "TEXT NOT NULL DEFAULT '[]'");
  ensureColumnExists(db, "run_summaries", "awaited_sink_ids_json", "TEXT NOT NULL DEFAULT '[]'");
  ensureColumnExists(db, "adapter_workload_statuses", "deployment_ids_json", "TEXT NOT NULL DEFAULT '[]'");
  ensureColumnExists(db, "adapter_workload_statuses", "flow_ids_json", "TEXT NOT NULL DEFAULT '[]'");
  ensureColumnExists(db, "adapter_workload_statuses", "revision_ids_json", "TEXT NOT NULL DEFAULT '[]'");
}

function defaultConnectorConfig(capabilityId: string): Record<string, unknown> {
  switch (capabilityId) {
    case "http_in":
      return {
        path: "/ingest/http",
        method: "POST",
      };
    case "http_out":
      return {
        url: "http://127.0.0.1:4011/http-sink",
        method: "POST",
        timeoutMs: 5_000,
      };
    case "nats_in":
      return {
        subject: "events.source.nats",
      };
    case "nats_out":
      return {
        subject: "events.sink.nats",
      };
    case "snowflake_sink":
      return {
        table: "events_router_ingest",
      };
    case "bigquery_sink":
      return {
        dataset: "event_router",
        table: "ingest_events",
      };
    case "s3_sink":
      return {
        bucket: "event-router-v1",
        prefix: "events/",
      };
    case "kafka_in":
      return {
        topic: "router.ingress.kafka",
        brokers: ["host.docker.internal:9092"],
      };
    case "kafka_out":
      return {
        topic: "router.egress.kafka",
      };
    default:
      return {};
  }
}

function upsertSeedConnectors(db: Database, config: ControlApiConfig, now: string): void {
  for (const capability of CONNECTOR_CAPABILITIES) {
    const connectorId = `${capability.id}_default`;
    const existing = db
      .query("SELECT id FROM connectors WHERE id = ? LIMIT 1")
      .get(connectorId) as { id?: string } | null;

    if (existing?.id) {
      db.query(
        "UPDATE connectors SET tenant_id = ?, name = ?, capability_id = ?, execution_mode = ?, config_json = ? WHERE id = ?",
      ).run(
        config.defaultTenantId,
        `${capability.name} Default`,
        capability.id,
        capability.executionMode,
        JSON.stringify(defaultConnectorConfig(capability.id)),
        connectorId,
      );
      continue;
    }

    db.query(
      "INSERT INTO connectors (id, tenant_id, name, capability_id, execution_mode, config_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
    ).run(
      connectorId,
      config.defaultTenantId,
      `${capability.name} Default`,
      capability.id,
      capability.executionMode,
      JSON.stringify(defaultConnectorConfig(capability.id)),
      now,
    );
  }
}

function seedDatabase(db: Database, config: ControlApiConfig): void {
  const tenantExists = db
    .query("SELECT id FROM tenants WHERE id = ?")
    .get(config.defaultTenantId) as { id?: string } | null;

  const now = isoNow();

  if (tenantExists?.id) {
    upsertSeedConnectors(db, config, now);
    return;
  }

  const userId = id("user");

  db.query("INSERT INTO users (id, email, role, created_at) VALUES (?, ?, ?, ?)")
    .run(userId, config.bootstrapAdminEmail, "admin", now);

  db.query("INSERT INTO api_tokens (id, user_id, label, token, created_at) VALUES (?, ?, ?, ?, ?)")
    .run(id("token"), userId, "bootstrap-admin", config.bootstrapApiToken, now);

  db.query("INSERT INTO tenants (id, name, created_at) VALUES (?, ?, ?)")
    .run(config.defaultTenantId, config.defaultTenantName, now);

  upsertSeedConnectors(db, config, now);

  db.query(
    "INSERT INTO audit_records (id, tenant_id, actor, action, subject_type, subject_id, details_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
  ).run(
    id("audit"),
    config.defaultTenantId,
    config.bootstrapAdminEmail,
    "seed.completed",
    "tenant",
    config.defaultTenantId,
    JSON.stringify({ capabilities: CONNECTOR_CAPABILITIES.length }),
    now,
  );
}

export function initializeRepositoryDatabase(db: Database, config: ControlApiConfig): void {
  createSchema(db);
  seedDatabase(db, config);
}
