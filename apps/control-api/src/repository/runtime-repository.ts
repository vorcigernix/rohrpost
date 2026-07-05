import type { Database } from "bun:sqlite";
import type { ControlApiRuntimeStatsResponse } from "@rohrpost/control-api-contracts";
import type { ControlApiConfig } from "../config";
import {
  getRevision,
  id,
  isoNow,
  mapAdapterWorkloadStatusRow,
  mapDeploymentRow,
  maxIsoString,
  parseJson,
  parseStringArray,
  resolveConnectorIds,
  resolveConnectors,
} from "./shared";
import type {
  AdapterWorkloadStatusInput,
  Repository,
  RuntimeDeploymentStatsRecord,
  RuntimeSampleRecord,
  RuntimeRunInput,
} from "./types";

function pruneRunSummaries(db: Database, retentionLimit: number): void {
  if (retentionLimit <= 0) {
    db.query("DELETE FROM run_summaries").run();
    return;
  }

  db.query(
    `
      DELETE FROM run_summaries
      WHERE id IN (
        SELECT id
        FROM run_summaries
        ORDER BY started_at DESC
        LIMIT -1 OFFSET ?
      )
    `,
  ).run(retentionLimit);
}

function listRuntimeDeploymentStats(db: Database): RuntimeDeploymentStatsRecord[] {
  const rows = db
    .query(
      `
        SELECT
          stats.deployment_id,
          stats.reporter_id,
          stats.flow_id,
          stats.revision_id,
          stats.accepted_count,
          stats.processed_count,
          stats.delivered_count,
          stats.retrying_count,
          stats.dlq_count,
          stats.failed_count,
          stats.filtered_count,
          stats.deduped_count,
          stats.sink_attempt_count,
          stats.sink_success_count,
          stats.sink_failure_count,
          stats.inflight_count,
          stats.backlog_count,
          stats.last_accepted_at,
          stats.last_processed_at,
          stats.last_error,
          stats.updated_at,
          deployments.rollout_status,
          flow_definitions.name AS flow_name
        FROM runtime_deployment_stats AS stats
        LEFT JOIN deployments ON deployments.id = stats.deployment_id
        LEFT JOIN flow_definitions ON flow_definitions.id = stats.flow_id
        ORDER BY stats.updated_at DESC
      `,
    )
    .all() as Array<Record<string, unknown>>;

  const aggregate = new Map<string, RuntimeDeploymentStatsRecord>();

  for (const row of rows) {
    const deploymentId = String(row.deployment_id);
    const reporterId = String(row.reporter_id);
    const current = aggregate.get(deploymentId);
    const nextCounts = {
      acceptedCount: Number(row.accepted_count),
      processedCount: Number(row.processed_count),
      deliveredCount: Number(row.delivered_count),
      retryingCount: Number(row.retrying_count),
      dlqCount: Number(row.dlq_count),
      failedCount: Number(row.failed_count),
      filteredCount: Number(row.filtered_count),
      dedupedCount: Number(row.deduped_count),
      sinkAttemptCount: Number(row.sink_attempt_count),
      sinkSuccessCount: Number(row.sink_success_count),
      sinkFailureCount: Number(row.sink_failure_count),
      inflightCount: Number(row.inflight_count),
      backlogCount: Number(row.backlog_count),
    };

    if (!current) {
      aggregate.set(deploymentId, {
        deploymentId,
        flowId: String(row.flow_id),
        flowName: typeof row.flow_name === "string" ? row.flow_name : String(row.flow_id),
        revisionId: String(row.revision_id),
        rolloutStatus: typeof row.rollout_status === "string" ? row.rollout_status : "pending_activation",
        reporterIds: [reporterId],
        ...nextCounts,
        lastAcceptedAt: row.last_accepted_at ? String(row.last_accepted_at) : null,
        lastProcessedAt: row.last_processed_at ? String(row.last_processed_at) : null,
        updatedAt: row.updated_at ? String(row.updated_at) : null,
        lastError: row.last_error ? String(row.last_error) : null,
        state: "idle",
      });
      continue;
    }

    current.reporterIds.push(reporterId);
    current.acceptedCount += nextCounts.acceptedCount;
    current.processedCount += nextCounts.processedCount;
    current.deliveredCount += nextCounts.deliveredCount;
    current.retryingCount += nextCounts.retryingCount;
    current.dlqCount += nextCounts.dlqCount;
    current.failedCount += nextCounts.failedCount;
    current.filteredCount += nextCounts.filteredCount;
    current.dedupedCount += nextCounts.dedupedCount;
    current.sinkAttemptCount += nextCounts.sinkAttemptCount;
    current.sinkSuccessCount += nextCounts.sinkSuccessCount;
    current.sinkFailureCount += nextCounts.sinkFailureCount;
    current.inflightCount += nextCounts.inflightCount;
    current.backlogCount += nextCounts.backlogCount;
    current.lastAcceptedAt = maxIsoString(current.lastAcceptedAt, row.last_accepted_at ? String(row.last_accepted_at) : null);
    current.lastProcessedAt = maxIsoString(current.lastProcessedAt, row.last_processed_at ? String(row.last_processed_at) : null);
    current.updatedAt = maxIsoString(current.updatedAt, row.updated_at ? String(row.updated_at) : null);
    if (row.last_error && current.updatedAt === row.updated_at) {
      current.lastError = String(row.last_error);
    }
  }

  const stats = [...aggregate.values()].map((record) => {
    let state: RuntimeDeploymentStatsRecord["state"] = "healthy";
    if (record.rolloutStatus === "degraded") {
      state = "degraded";
    } else if (record.acceptedCount === 0 && record.processedCount === 0) {
      state = "idle";
    } else if (record.backlogCount > 0 || record.inflightCount > 0 || record.retryingCount > 0) {
      state = "backlogged";
    }

    return {
      ...record,
      reporterIds: [...new Set(record.reporterIds)].sort(),
      state,
    };
  });

  stats.sort(
    (left, right) =>
      Date.parse(right.updatedAt ?? "1970-01-01T00:00:00.000Z") -
      Date.parse(left.updatedAt ?? "1970-01-01T00:00:00.000Z"),
  );
  return stats;
}

export function createRuntimeRepository(
  db: Database,
  config: ControlApiConfig,
): Pick<
  Repository,
  | "listRuns"
  | "listDlq"
  | "getRuntimeStats"
  | "listActiveRuntimeDeployments"
  | "appendRunSummary"
  | "appendRunSummaries"
  | "recordAdapterRunResult"
  | "replaceRuntimeDeploymentStats"
  | "replaceRuntimeSamples"
  | "listRuntimeSamples"
  | "replaceAdapterWorkloadStatuses"
  | "listAdapterWorkloadStatuses"
  | "appendAuditRecord"
  | "updateDeploymentRuntimeStatus"
> {
  const insertRunSummaryStatement = db.query(
    "INSERT INTO run_summaries (id, flow_id, revision_id, deployment_id, message_id, status, source_ref, trace_id, processed_count, error_count, started_at, finished_at, last_error, target_sink_ids_json, awaited_sink_ids_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
  );
  const appendRunSummariesTxn = db.transaction((inputs: RuntimeRunInput[]) => {
    for (const input of inputs) {
      insertRunSummaryStatement.run(
        input.id ?? id("run"),
        input.flowId,
        input.revisionId,
        input.deploymentId ?? null,
        input.messageId ?? null,
        input.status,
        input.sourceRef,
        input.traceId,
        input.processedCount,
        input.errorCount,
        input.startedAt,
        input.finishedAt,
        input.lastError ?? null,
        JSON.stringify(input.targetSinkIds ?? []),
        JSON.stringify(input.awaitedSinkIds ?? []),
      );
    }

    pruneRunSummaries(db, config.runSummaryRetentionLimit);
  });
  const upsertAdapterWorkloadStatusStatement = db.query(
    `
      INSERT INTO adapter_workload_statuses (
        reporter_id,
        workload_key,
        connector_id,
        capability_id,
        manifest_id,
        deployment_ids_json,
        flow_ids_json,
        revision_ids_json,
        runtime_role,
        input_ref,
        output_ref,
        status,
        backend,
        consumer_ref,
        target_kind,
        artifact_path,
        config_path,
        container_name,
        started_at,
        stopped_at,
        last_error,
        restart_count,
        recent_logs_json,
        reported_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(reporter_id, workload_key) DO UPDATE SET
        connector_id = excluded.connector_id,
        capability_id = excluded.capability_id,
        manifest_id = excluded.manifest_id,
        deployment_ids_json = excluded.deployment_ids_json,
        flow_ids_json = excluded.flow_ids_json,
        revision_ids_json = excluded.revision_ids_json,
        runtime_role = excluded.runtime_role,
        input_ref = excluded.input_ref,
        output_ref = excluded.output_ref,
        status = excluded.status,
        backend = excluded.backend,
        consumer_ref = excluded.consumer_ref,
        target_kind = excluded.target_kind,
        artifact_path = excluded.artifact_path,
        config_path = excluded.config_path,
        container_name = excluded.container_name,
        started_at = excluded.started_at,
        stopped_at = excluded.stopped_at,
        last_error = excluded.last_error,
        restart_count = excluded.restart_count,
        recent_logs_json = excluded.recent_logs_json,
        reported_at = excluded.reported_at
    `,
  );
  const replaceAdapterWorkloadStatusesTxn = db.transaction((input: {
    reporterId: string;
    reportedAt?: string;
    workloads: AdapterWorkloadStatusInput[];
  }) => {
    const reportedAt = input.reportedAt ?? isoNow();
    const nextKeys = new Set(input.workloads.map((workload) => workload.key));
    const existingRows = db
      .query("SELECT workload_key FROM adapter_workload_statuses WHERE reporter_id = ?")
      .all(input.reporterId) as Array<{ workload_key?: string }>;
    let deleted = 0;

    for (const row of existingRows) {
      const key = typeof row.workload_key === "string" ? row.workload_key : "";
      if (!nextKeys.has(key)) {
        db.query("DELETE FROM adapter_workload_statuses WHERE reporter_id = ? AND workload_key = ?").run(
          input.reporterId,
          key,
        );
        deleted += 1;
      }
    }

    for (const workload of input.workloads) {
      upsertAdapterWorkloadStatusStatement.run(
        input.reporterId,
        workload.key,
        workload.connectorId,
        workload.capabilityId,
        workload.manifestId,
        JSON.stringify(workload.deploymentIds),
        JSON.stringify(workload.flowIds),
        JSON.stringify(workload.revisionIds),
        workload.runtimeRole,
        workload.inputRef,
        workload.outputRef,
        workload.status,
        workload.backend,
        workload.consumerRef ?? null,
        workload.targetKind,
        workload.artifactPath ?? null,
        workload.configPath,
        workload.containerName ?? null,
        workload.startedAt ?? null,
        workload.stoppedAt ?? null,
        workload.lastError ?? null,
        workload.restartCount,
        JSON.stringify(workload.recentLogs),
        reportedAt,
      );
    }

    return {
      updated: input.workloads.length,
      deleted,
      reportedAt,
    };
  });

  return {
    listRuns() {
      return (
        db
          .query("SELECT * FROM run_summaries ORDER BY started_at DESC LIMIT 100")
          .all() as Array<Record<string, unknown>>
      ).map((row) => {
        const flow = db
          .query("SELECT name FROM flow_definitions WHERE id = ? LIMIT 1")
          .get(String(row.flow_id)) as { name?: string } | null;
        const startedAt = String(row.started_at);
        const finishedAt = String(row.finished_at);
        const awaitedSinkIds = parseStringArray(row.awaited_sink_ids_json);
        const detail =
          row.status === "enqueued"
            ? `Awaiting adapter completion for ${Math.max(awaitedSinkIds.length, 1)} sink${Math.max(awaitedSinkIds.length, 1) === 1 ? "" : "s"}.`
            : typeof row.last_error === "string" && row.last_error.length > 0
              ? row.last_error
              : "Delivery completed without sink errors.";

        return {
          id: row.id,
          flowId: row.flow_id,
          flowName: flow?.name ?? row.flow_id,
          revisionId: row.revision_id,
          deploymentId: row.deployment_id,
          status: row.status,
          sourceRef: row.source_ref,
          traceId: row.trace_id,
          processedCount: row.processed_count,
          errorCount: row.error_count,
          startedAt,
          finishedAt,
          lastError: row.last_error,
          attempt: Number(row.error_count) > 0 ? 2 : 1,
          messageId: row.message_id ?? `${row.id}-msg`,
          partitionKey: row.source_ref,
          latencyMs: Math.max(1, Date.parse(finishedAt) - Date.parse(startedAt)),
          detail,
        };
      });
    },
    listDlq() {
      return (
        db
          .query("SELECT * FROM run_summaries WHERE status = 'dlq' ORDER BY started_at DESC")
          .all() as Array<Record<string, unknown>>
      ).map((row) => ({
        id: row.id,
        flowId: row.flow_id,
        revisionId: row.revision_id,
        deploymentId: row.deployment_id,
        status: row.status,
        sourceRef: row.source_ref,
        traceId: row.trace_id,
        processedCount: row.processed_count,
        errorCount: row.error_count,
        startedAt: row.started_at,
        finishedAt: row.finished_at,
        lastError: row.last_error,
      }));
    },
    getRuntimeStats() {
      const deployments = listRuntimeDeploymentStats(db);
      const summary = deployments.reduce(
        (totals, record) => {
          totals.acceptedCount += record.acceptedCount;
          totals.processedCount += record.processedCount;
          totals.deliveredCount += record.deliveredCount;
          totals.backlogCount += record.backlogCount;
          totals.inflightCount += record.inflightCount;
          if (record.state === "degraded") {
            totals.degradedDeployments += 1;
          } else if (record.rolloutStatus === "activated") {
            totals.healthyDeployments += 1;
          }
          totals.lastProcessedAt = maxIsoString(totals.lastProcessedAt, record.lastProcessedAt);
          return totals;
        },
        {
          acceptedCount: 0,
          processedCount: 0,
          deliveredCount: 0,
          backlogCount: 0,
          inflightCount: 0,
          healthyDeployments: 0,
          degradedDeployments: 0,
          lastProcessedAt: null as string | null,
        },
      );

      const response: ControlApiRuntimeStatsResponse = {
        observability: {
          mode: "otel-primary",
          consoleRole: "health-and-flow-mapping",
        },
        summary,
        deployments,
      };

      return response;
    },
    listActiveRuntimeDeployments() {
      const rows = db
        .query("SELECT * FROM deployments WHERE status = 'active' ORDER BY created_at DESC")
        .all() as Record<string, unknown>[];

      return rows.flatMap((row) => {
        const deployment = mapDeploymentRow(row);
        const revision = getRevision(db, deployment.flowId, deployment.revisionId);

        if (!revision) {
          return [];
        }

        return [
          {
            deployment,
            revision,
            connectors: resolveConnectors(db, resolveConnectorIds(revision.spec)),
          },
        ];
      });
    },
    appendRunSummary(input) {
      const runId = input.id ?? id("run");
      appendRunSummariesTxn([
        {
          ...input,
          id: runId,
        },
      ]);

      return { id: runId };
    },
    appendRunSummaries(inputs) {
      if (inputs.length === 0) {
        return { inserted: 0 };
      }

      appendRunSummariesTxn(inputs);
      return { inserted: inputs.length };
    },
    recordAdapterRunResult(input) {
      db.query(
        `
          INSERT INTO adapter_run_results (
            run_id,
            sink_id,
            connector_id,
            capability_id,
            status,
            target_ref,
            artifact_path,
            object_key,
            error,
            started_at,
            finished_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(run_id, sink_id) DO UPDATE SET
            connector_id = excluded.connector_id,
            capability_id = excluded.capability_id,
            status = excluded.status,
            target_ref = excluded.target_ref,
            artifact_path = excluded.artifact_path,
            object_key = excluded.object_key,
            error = excluded.error,
            started_at = excluded.started_at,
            finished_at = excluded.finished_at
        `,
      ).run(
        input.runId,
        input.sinkId,
        input.connectorId,
        input.capabilityId,
        input.status,
        input.targetRef ?? null,
        input.artifactPath ?? null,
        input.objectKey ?? null,
        input.error ?? null,
        input.startedAt,
        input.finishedAt,
      );

      const run = db
        .query("SELECT id, status, awaited_sink_ids_json, finished_at FROM run_summaries WHERE id = ? LIMIT 1")
        .get(input.runId) as Record<string, unknown> | null;

      if (!run) {
        return { updated: false, status: null };
      }

      const awaitedSinkIds = parseStringArray(run.awaited_sink_ids_json);
      if (awaitedSinkIds.length === 0 || !awaitedSinkIds.includes(input.sinkId)) {
        return { updated: false, status: typeof run.status === "string" ? run.status : null };
      }

      const resultRows = db
        .query(
          "SELECT sink_id, status, error, finished_at FROM adapter_run_results WHERE run_id = ? ORDER BY finished_at ASC",
        )
        .all(input.runId) as Array<Record<string, unknown>>;
      const awaitedResults = resultRows.filter((row) =>
        awaitedSinkIds.includes(String(row.sink_id)),
      );
      const failedResult = awaitedResults.find((row) => row.status === "failed");
      const completedAwaitedSinkIds = new Set(awaitedResults.map((row) => String(row.sink_id)));
      const nextFinishedAt =
        maxIsoString(
          typeof run.finished_at === "string" ? run.finished_at : null,
          ...awaitedResults.map((row) => (typeof row.finished_at === "string" ? row.finished_at : null)),
        ) ?? isoNow();

      if (failedResult) {
        db.query(
          "UPDATE run_summaries SET status = ?, error_count = ?, finished_at = ?, last_error = ? WHERE id = ?",
        ).run(
          "failed",
          1,
          nextFinishedAt,
          typeof failedResult.error === "string" ? failedResult.error : input.error ?? "adapter delivery failed",
          input.runId,
        );
        return { updated: true, status: "failed" };
      }

      if (completedAwaitedSinkIds.size >= awaitedSinkIds.length) {
        db.query(
          "UPDATE run_summaries SET status = ?, error_count = ?, finished_at = ?, last_error = NULL WHERE id = ?",
        ).run(
          "succeeded",
          0,
          nextFinishedAt,
          input.runId,
        );
        return { updated: true, status: "succeeded" };
      }

      return { updated: false, status: typeof run.status === "string" ? run.status : null };
    },
    replaceRuntimeDeploymentStats(inputs) {
      const statement = db.query(
        `
          INSERT INTO runtime_deployment_stats (
            deployment_id,
            reporter_id,
            flow_id,
            revision_id,
            accepted_count,
            processed_count,
            delivered_count,
            retrying_count,
            dlq_count,
            failed_count,
            filtered_count,
            deduped_count,
            sink_attempt_count,
            sink_success_count,
            sink_failure_count,
            inflight_count,
            backlog_count,
            last_accepted_at,
            last_processed_at,
            last_error,
            updated_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(deployment_id, reporter_id) DO UPDATE SET
            flow_id = excluded.flow_id,
            revision_id = excluded.revision_id,
            accepted_count = excluded.accepted_count,
            processed_count = excluded.processed_count,
            delivered_count = excluded.delivered_count,
            retrying_count = excluded.retrying_count,
            dlq_count = excluded.dlq_count,
            failed_count = excluded.failed_count,
            filtered_count = excluded.filtered_count,
            deduped_count = excluded.deduped_count,
            sink_attempt_count = excluded.sink_attempt_count,
            sink_success_count = excluded.sink_success_count,
            sink_failure_count = excluded.sink_failure_count,
            inflight_count = excluded.inflight_count,
            backlog_count = excluded.backlog_count,
            last_accepted_at = excluded.last_accepted_at,
            last_processed_at = excluded.last_processed_at,
            last_error = excluded.last_error,
            updated_at = excluded.updated_at
        `,
      );

      for (const input of inputs) {
        statement.run(
          input.deploymentId,
          input.reporterId,
          input.flowId,
          input.revisionId,
          input.acceptedCount,
          input.processedCount,
          input.deliveredCount,
          input.retryingCount,
          input.dlqCount,
          input.failedCount,
          input.filteredCount,
          input.dedupedCount,
          input.sinkAttemptCount,
          input.sinkSuccessCount,
          input.sinkFailureCount,
          input.inflightCount,
          input.backlogCount,
          input.lastAcceptedAt ?? null,
          input.lastProcessedAt ?? null,
          input.lastError ?? null,
          input.updatedAt ?? isoNow(),
        );
      }

      return { updated: inputs.length };
    },
    replaceRuntimeSamples(inputs) {
      if (inputs.length === 0) {
        return { updated: 0 };
      }

      const statement = db.query(
        `
          INSERT INTO runtime_samples (
            deployment_id,
            flow_id,
            revision_id,
            source_kind,
            source_ref,
            payload_json,
            observed_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(deployment_id, source_kind, source_ref) DO UPDATE SET
            flow_id = excluded.flow_id,
            revision_id = excluded.revision_id,
            payload_json = excluded.payload_json,
            observed_at = excluded.observed_at
        `,
      );

      for (const input of inputs) {
        statement.run(
          input.deploymentId,
          input.flowId,
          input.revisionId,
          input.sourceKind,
          input.sourceRef,
          JSON.stringify(input.payload),
          input.observedAt ?? isoNow(),
        );
      }

      return { updated: inputs.length };
    },
    listRuntimeSamples(options) {
      const limit = options?.limit ?? 12;
      const rows = (
        options?.sourceKind
          ? db
              .query(
                `
                  SELECT
                    rs.deployment_id,
                    rs.flow_id,
                    rs.revision_id,
                    rs.source_kind,
                    rs.source_ref,
                    rs.payload_json,
                    rs.observed_at,
                    fd.name AS flow_name
                  FROM runtime_samples rs
                  LEFT JOIN flow_definitions fd ON fd.id = rs.flow_id
                  WHERE rs.source_kind = ?
                  ORDER BY rs.observed_at DESC
                  LIMIT ?
                `,
              )
              .all(options.sourceKind, limit)
          : db
              .query(
                `
                  SELECT
                    rs.deployment_id,
                    rs.flow_id,
                    rs.revision_id,
                    rs.source_kind,
                    rs.source_ref,
                    rs.payload_json,
                    rs.observed_at,
                    fd.name AS flow_name
                  FROM runtime_samples rs
                  LEFT JOIN flow_definitions fd ON fd.id = rs.flow_id
                  ORDER BY rs.observed_at DESC
                  LIMIT ?
                `,
              )
              .all(limit)
      ) as Record<string, unknown>[];

      return rows.map((row) => ({
        deploymentId: String(row.deployment_id),
        flowId: String(row.flow_id),
        flowName: typeof row.flow_name === "string" ? row.flow_name : String(row.flow_id),
        revisionId: String(row.revision_id),
        sourceKind: String(row.source_kind) as RuntimeSampleRecord["sourceKind"],
        sourceRef: String(row.source_ref),
        payload: parseJson(row.payload_json),
        observedAt: String(row.observed_at),
      }));
    },
    replaceAdapterWorkloadStatuses(input) {
      return replaceAdapterWorkloadStatusesTxn(input);
    },
    listAdapterWorkloadStatuses() {
      return (
        db
          .query("SELECT * FROM adapter_workload_statuses ORDER BY reported_at DESC, connector_id ASC")
          .all() as Record<string, unknown>[]
      ).map(mapAdapterWorkloadStatusRow);
    },
    appendAuditRecord(input) {
      const auditId = id("audit");

      db.query(
        "INSERT INTO audit_records (id, tenant_id, actor, action, subject_type, subject_id, details_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      ).run(
        auditId,
        input.tenantId,
        input.actor,
        input.action,
        input.subjectType,
        input.subjectId,
        JSON.stringify(input.details),
        isoNow(),
      );

      return { id: auditId };
    },
    updateDeploymentRuntimeStatus(deploymentId, input) {
      const current = db
        .query("SELECT * FROM deployments WHERE id = ? LIMIT 1")
        .get(deploymentId) as Record<string, unknown> | null;

      if (!current) {
        return null;
      }

      const currentDeployment = mapDeploymentRow(current);
      db.query("UPDATE deployments SET status = ?, rollout_status = ? WHERE id = ?").run(
        input.status ?? currentDeployment.status,
        input.rolloutStatus,
        deploymentId,
      );

      const next = db
        .query("SELECT * FROM deployments WHERE id = ? LIMIT 1")
        .get(deploymentId) as Record<string, unknown> | null;

      return next ? mapDeploymentRow(next) : null;
    },
  };
}
