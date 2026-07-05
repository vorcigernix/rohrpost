import type { Database } from "bun:sqlite";
import { compileFlowSpec, type FlowSpec } from "@rohrpost/shared-flow-spec";
import {
  addAudit,
  getLatestRevision,
  getRevision,
  id,
  isoNow,
  mapDeploymentRow,
  mapRevisionRow,
} from "./shared";
import type { FlowListItem, Repository } from "./types";

export function createFlowRepository(
  db: Database,
): Pick<Repository, "listFlows" | "listRevisions" | "createOrUpdateFlow" | "deleteFlow" | "publishFlow" | "rollbackDeployment"> {
  const deleteFlowTxn = db.transaction((flowId: string, tenantId: string) => {
    db.query("DELETE FROM adapter_run_results WHERE run_id IN (SELECT id FROM run_summaries WHERE flow_id = ?)").run(
      flowId,
    );
    db.query("DELETE FROM runtime_deployment_stats WHERE flow_id = ?").run(flowId);
    db.query("DELETE FROM runtime_samples WHERE flow_id = ?").run(flowId);
    db.query("DELETE FROM replay_requests WHERE flow_id = ?").run(flowId);
    db.query("DELETE FROM run_summaries WHERE flow_id = ?").run(flowId);
    db.query("DELETE FROM deployments WHERE flow_id = ?").run(flowId);
    db.query("DELETE FROM flow_revisions WHERE flow_id = ?").run(flowId);
    db.query("DELETE FROM flow_definitions WHERE id = ?").run(flowId);

    addAudit(db, tenantId, "control-api", "flow.deleted", "flow", flowId, {});
  });

  return {
    listFlows() {
      const flows = db
        .query(`
          SELECT
            f.id,
            f.tenant_id AS tenantId,
            f.name,
            f.status,
            f.created_at AS createdAt,
            f.updated_at AS updatedAt,
            (
              SELECT fr.id
              FROM flow_revisions fr
              WHERE fr.flow_id = f.id
              ORDER BY fr.revision_number DESC
              LIMIT 1
            ) AS latestRevisionId,
            (
              SELECT d.revision_id
              FROM deployments d
              WHERE d.flow_id = f.id AND d.status = 'active'
              ORDER BY d.created_at DESC
              LIMIT 1
            ) AS activeRevisionId
          FROM flow_definitions f
          ORDER BY f.updated_at DESC
        `)
        .all() as FlowListItem[];

      return flows.map((flow) => {
        const revisionId = flow.activeRevisionId ?? flow.latestRevisionId;
        const revision = revisionId ? getRevision(db, flow.id, revisionId) : null;

        return {
          ...flow,
          revisionId,
          spec: revision?.spec,
          compiler: revision?.compiler,
        };
      });
    },
    listRevisions(flowId) {
      return (
        db
          .query("SELECT * FROM flow_revisions WHERE flow_id = ? ORDER BY revision_number DESC")
          .all(flowId) as Record<string, unknown>[]
      ).map(mapRevisionRow);
    },
    createOrUpdateFlow(input) {
      const now = isoNow();
      const existing = db
        .query("SELECT id FROM flow_definitions WHERE id = ? LIMIT 1")
        .get(input.spec.metadata.flowId) as { id?: string } | null;
      const latestRevision = getLatestRevision(db, input.spec.metadata.flowId);
      const revisionNumber = (latestRevision?.revisionNumber ?? 0) + 1;
      const revisionId =
        revisionNumber === 1
          ? input.spec.metadata.revisionId
          : `${input.spec.metadata.flowId}_v${revisionNumber}`;
      const spec: FlowSpec = {
        ...input.spec,
        metadata: {
          ...input.spec.metadata,
          revisionId,
          flowId: input.spec.metadata.flowId,
          tenantId: input.tenantId,
          name: input.name,
        },
      };
      const compiler = compileFlowSpec(spec);

      if (!existing?.id) {
        db.query(
          "INSERT INTO flow_definitions (id, tenant_id, name, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
        ).run(spec.metadata.flowId, input.tenantId, input.name, "draft", now, now);
      } else {
        db.query("UPDATE flow_definitions SET name = ?, updated_at = ? WHERE id = ?").run(
          input.name,
          now,
          spec.metadata.flowId,
        );
      }

      db.query(
        "INSERT INTO flow_revisions (id, flow_id, revision_number, spec_json, compiler_json, simulation_json, created_at, published_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      ).run(
        revisionId,
        spec.metadata.flowId,
        revisionNumber,
        JSON.stringify(spec),
        JSON.stringify(compiler),
        JSON.stringify(input.simulation),
        now,
        null,
      );

      addAudit(db, input.tenantId, "control-api", "flow.saved", "flow", spec.metadata.flowId, {
        revisionId,
        valid: input.validation.valid,
      });

      return {
        id: revisionId,
        flowId: spec.metadata.flowId,
        revisionNumber,
        spec,
        compiler,
        simulation: input.simulation,
        publishedAt: null,
        createdAt: now,
      };
    },
    deleteFlow(flowId) {
      const flow = db
        .query("SELECT tenant_id AS tenantId FROM flow_definitions WHERE id = ? LIMIT 1")
        .get(flowId) as { tenantId?: string } | null;

      if (!flow?.tenantId) {
        return null;
      }

      deleteFlowTxn(flowId, flow.tenantId);
      return { flowId, deleted: true };
    },
    publishFlow(flowId, revisionId) {
      const revision = getRevision(db, flowId, revisionId);
      if (!revision) {
        throw new Error("Flow revision not found");
      }

      db.query("UPDATE deployments SET status = 'inactive' WHERE flow_id = ? AND status = 'active'").run(flowId);

      const createdAt = isoNow();
      const deployment = {
        id: id("deploy"),
        flowId,
        revisionId: revision.id,
        status: "active",
        rolloutStatus: "pending_activation",
        createdAt,
        rolledBackFrom: null,
      };

      db.query(
        "INSERT INTO deployments (id, flow_id, revision_id, status, rollout_status, created_at, rolled_back_from) VALUES (?, ?, ?, ?, ?, ?, ?)",
      ).run(
        deployment.id,
        deployment.flowId,
        deployment.revisionId,
        deployment.status,
        deployment.rolloutStatus,
        deployment.createdAt,
        deployment.rolledBackFrom,
      );

      db.query("UPDATE flow_definitions SET status = 'active', updated_at = ? WHERE id = ?").run(
        deployment.createdAt,
        flowId,
      );
      db.query("UPDATE flow_revisions SET published_at = ? WHERE id = ?").run(deployment.createdAt, revision.id);

      addAudit(db, revision.spec.metadata.tenantId, "control-api", "flow.published", "deployment", deployment.id, {
        flowId,
        revisionId: revision.id,
      });

      return { deployment, revision: { ...revision, publishedAt: deployment.createdAt } };
    },
    rollbackDeployment(deploymentId, targetRevisionId) {
      const current = db
        .query("SELECT * FROM deployments WHERE id = ? LIMIT 1")
        .get(deploymentId) as Record<string, unknown> | null;

      if (!current) {
        throw new Error("Deployment not found");
      }

      const currentDeployment = mapDeploymentRow(current);

      const previousRevisionId =
        targetRevisionId ??
        (
          db
            .query(
              "SELECT revision_id FROM deployments WHERE flow_id = ? AND id != ? ORDER BY created_at DESC LIMIT 1",
            )
            .get(currentDeployment.flowId, currentDeployment.id) as { revision_id?: string } | null
        )?.revision_id;

      const revision = getRevision(db, currentDeployment.flowId, previousRevisionId ?? null);
      if (!revision) {
        throw new Error("Rollback target revision not found");
      }

      db.query("UPDATE deployments SET status = 'inactive' WHERE flow_id = ? AND status = 'active'").run(
        currentDeployment.flowId,
      );

      const deployment = {
        id: id("deploy"),
        flowId: currentDeployment.flowId,
        revisionId: revision.id,
        status: "active",
        rolloutStatus: "pending_activation",
        createdAt: isoNow(),
        rolledBackFrom: currentDeployment.id,
      };

      db.query(
        "INSERT INTO deployments (id, flow_id, revision_id, status, rollout_status, created_at, rolled_back_from) VALUES (?, ?, ?, ?, ?, ?, ?)",
      ).run(
        deployment.id,
        deployment.flowId,
        deployment.revisionId,
        deployment.status,
        deployment.rolloutStatus,
        deployment.createdAt,
        deployment.rolledBackFrom,
      );

      addAudit(db, revision.spec.metadata.tenantId, "control-api", "deployment.rollback", "deployment", deployment.id, {
        fromDeploymentId: currentDeployment.id,
        revisionId: revision.id,
      });

      return { deployment, revision };
    },
  };
}
