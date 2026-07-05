import { mkdirSync } from "node:fs";
import { Database } from "bun:sqlite";
import type { ControlApiOverview } from "@rohrpost/control-api-contracts";
import { CONNECTOR_CAPABILITIES } from "@rohrpost/domain-connectors";
import type { ControlApiConfig } from "./config";
import { databaseDirectory } from "./config";
import { initializeRepositoryDatabase } from "./repository/bootstrap";
import { createCatalogRepository } from "./repository/catalog-repository";
import { createFlowRepository } from "./repository/flow-repository";
import { createReplayRepository } from "./repository/replay-repository";
import { createRuntimeRepository } from "./repository/runtime-repository";
import type { Repository } from "./repository/types";

export * from "./repository/types";

export function createRepository(config: ControlApiConfig): Repository {
  mkdirSync(databaseDirectory(config), { recursive: true });

  const db = new Database(config.databasePath, { create: true, strict: true });
  db.exec(`
    PRAGMA journal_mode = WAL;
    PRAGMA synchronous = NORMAL;
    PRAGMA busy_timeout = 5000;
  `);
  initializeRepositoryDatabase(db, config);

  const catalogRepository = createCatalogRepository(db, config);
  const flowRepository = createFlowRepository(db);
  const runtimeRepository = createRuntimeRepository(db, config);
  const replayRepository = createReplayRepository(db, config);

  return {
    db,
    ...catalogRepository,
    ...flowRepository,
    ...runtimeRepository,
    ...replayRepository,
    getOverview() {
      const [flows, activeDeployments, pendingReplays] = [
        db.query("SELECT COUNT(*) AS count FROM flow_definitions").get() as { count: number },
        db.query("SELECT COUNT(*) AS count FROM deployments WHERE status = 'active'").get() as { count: number },
        db.query("SELECT COUNT(*) AS count FROM replay_requests WHERE status = 'pending'").get() as { count: number },
      ];
      const runtime = runtimeRepository.getRuntimeStats();

      const response: ControlApiOverview = {
        flows: flows.count,
        activeDeployments: activeDeployments.count,
        runs: runtime.summary.processedCount,
        pendingReplays: pendingReplays.count,
        capabilities: CONNECTOR_CAPABILITIES.length,
        runtime: runtime.summary,
        observability: runtime.observability,
        guarantees: {
          mode: "at-least-once",
          ordering: "per partition key",
          duplicatesPossible: true,
        },
      };

      return response;
    },
  };
}
