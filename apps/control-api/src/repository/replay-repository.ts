import type { Database } from "bun:sqlite";
import type { ControlApiConfig } from "../config";
import { addAudit, getReplayRequest, id, isoNow, mapReplayRow } from "./shared";
import type { Repository } from "./types";

export function createReplayRepository(
  db: Database,
  config: ControlApiConfig,
): Pick<Repository, "listPendingReplayRequests" | "claimReplayRequest" | "completeReplayRequest" | "createReplayRequest"> {
  return {
    listPendingReplayRequests(limit = 25) {
      return (
        db
          .query(
            "SELECT * FROM replay_requests WHERE status = 'pending' ORDER BY created_at ASC LIMIT ?",
          )
          .all(limit) as Record<string, unknown>[]
      ).map(mapReplayRow);
    },
    claimReplayRequest(replayId) {
      const existing = getReplayRequest(db, replayId);
      if (!existing || existing.status !== "pending") {
        return null;
      }

      db.query("UPDATE replay_requests SET status = 'processing' WHERE id = ?").run(replayId);

      return getReplayRequest(db, replayId);
    },
    completeReplayRequest(replayId, status) {
      const existing = getReplayRequest(db, replayId);
      if (!existing) {
        return null;
      }

      db.query("UPDATE replay_requests SET status = ? WHERE id = ?").run(status, replayId);

      return getReplayRequest(db, replayId);
    },
    createReplayRequest(input) {
      const request = {
        id: id("replay"),
        flowId: input.flowId,
        revisionId: input.revisionId,
        reason: input.reason,
        sourceStream: input.sourceStream,
        status: "pending" as const,
        createdAt: isoNow(),
      };

      db.query(
        "INSERT INTO replay_requests (id, flow_id, revision_id, reason, source_stream, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
      ).run(
        request.id,
        request.flowId,
        request.revisionId,
        request.reason,
        request.sourceStream,
        request.status,
        request.createdAt,
      );

      addAudit(db, config.defaultTenantId, "control-api", "replay.requested", "replay", request.id, request);

      return request;
    },
  };
}
