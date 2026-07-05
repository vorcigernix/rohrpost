import { Elysia, t } from "elysia";
import type {
  ControlApiAdapterWorkloadsResponse,
  ControlApiCountResponse,
  ControlApiDeploymentRecord,
  ControlApiIdResponse,
  ControlApiPendingReplayRequestsResponse,
  ControlApiRuntimeDeploymentsResponse,
  ControlApiRuntimeStatsResponse,
  ReplayCompletionStatus,
  UpdateDeploymentStatusInput,
} from "@rohrpost/control-api-contracts";
import type { ControlApiRouteDeps } from "./route-context";

export function createRuntimeRoutes(deps: ControlApiRouteDeps) {
  const runtimeRunSummarySchema = t.Object({
    id: t.Optional(t.String()),
    flowId: t.String(),
    revisionId: t.String(),
    deploymentId: t.Optional(t.Nullable(t.String())),
    messageId: t.Optional(t.String()),
    status: t.String(),
    sourceRef: t.String(),
    traceId: t.String(),
    processedCount: t.Numeric(),
    errorCount: t.Numeric(),
    startedAt: t.String(),
    finishedAt: t.String(),
    lastError: t.Optional(t.Nullable(t.String())),
    targetSinkIds: t.Optional(t.Array(t.String())),
    awaitedSinkIds: t.Optional(t.Array(t.String())),
  });

  const adapterRunResultSchema = t.Object({
    runId: t.String(),
    sinkId: t.String(),
    connectorId: t.String(),
    capabilityId: t.String(),
    status: t.Union([t.Literal("succeeded"), t.Literal("failed")]),
    targetRef: t.Optional(t.Nullable(t.String())),
    artifactPath: t.Optional(t.Nullable(t.String())),
    objectKey: t.Optional(t.Nullable(t.String())),
    error: t.Optional(t.Nullable(t.String())),
    startedAt: t.String(),
    finishedAt: t.String(),
  });

  const adapterWorkloadStatusSchema = t.Object({
    key: t.String(),
    connectorId: t.String(),
    capabilityId: t.String(),
    manifestId: t.String(),
    deploymentIds: t.Array(t.String()),
    flowIds: t.Array(t.String()),
    revisionIds: t.Array(t.String()),
    runtimeRole: t.Union([t.Literal("source"), t.Literal("sink")]),
    inputRef: t.String(),
    outputRef: t.String(),
    status: t.Union([
      t.Literal("starting"),
      t.Literal("running"),
      t.Literal("stopped"),
      t.Literal("degraded"),
    ]),
    backend: t.Union([
      t.Literal("docker"),
      t.Literal("kubernetes"),
      t.Literal("disabled"),
    ]),
    consumerRef: t.Optional(t.Nullable(t.String())),
    targetKind: t.Union([
      t.Literal("aws_s3"),
      t.Literal("file"),
      t.Literal("nats_jetstream"),
    ]),
    artifactPath: t.Optional(t.Nullable(t.String())),
    configPath: t.String(),
    containerName: t.Optional(t.Nullable(t.String())),
    startedAt: t.Optional(t.Nullable(t.String())),
    stoppedAt: t.Optional(t.Nullable(t.String())),
    lastError: t.Optional(t.Nullable(t.String())),
    restartCount: t.Numeric(),
    recentLogs: t.Array(t.String()),
  });

  return new Elysia()
    .get("/api/runtime/deployments/active", ({ request, status }) => {
      const auth = deps.requireAuth(request);
      if (!auth) return status(401, { error: "Unauthorized" });

      const response = {
        generatedAt: new Date().toISOString(),
        deployments: deps.repository.listActiveRuntimeDeployments(),
      } satisfies ControlApiRuntimeDeploymentsResponse;

      return response;
    })
    .get("/api/runtime/replays/pending", ({ request, query, status }) => {
      const auth = deps.requireAuth(request);
      if (!auth) return status(401, { error: "Unauthorized" });

      const response = {
        requests: deps.repository.listPendingReplayRequests(query.limit),
      } satisfies ControlApiPendingReplayRequestsResponse;

      return response;
    }, {
      query: t.Object({
        limit: t.Optional(t.Numeric()),
      }),
    })
    .get("/api/runs", ({ request, status }) => {
      const auth = deps.requireAuth(request);
      if (!auth) return status(401, { error: "Unauthorized" });
      return deps.repository.listRuns();
    })
    .get("/api/runtime/stats", ({ request, status }) => {
      const auth = deps.requireAuth(request);
      if (!auth) return status(401, { error: "Unauthorized" });
      const response = deps.repository.getRuntimeStats() satisfies ControlApiRuntimeStatsResponse;
      return response;
    })
    .get("/api/runtime/adapter-workloads", ({ request, status }) => {
      const auth = deps.requireAuth(request);
      if (!auth) return status(401, { error: "Unauthorized" });

      const response = {
        generatedAt: new Date().toISOString(),
        workloads: deps.repository.listAdapterWorkloadStatuses(),
      } satisfies ControlApiAdapterWorkloadsResponse;

      return response;
    })
    .get("/api/runtime/samples/recent", ({ request, query, status }) => {
      const auth = deps.requireAuth(request);
      if (!auth) return status(401, { error: "Unauthorized" });

      return {
        samples: deps.repository.listRuntimeSamples({
          sourceKind: query.sourceKind,
          limit: query.limit,
        }),
      };
    }, {
      query: t.Object({
        sourceKind: t.Optional(
          t.Union([t.Literal("http"), t.Literal("nats"), t.Literal("kafka")]),
        ),
        limit: t.Optional(t.Numeric()),
      }),
    })
    .get("/api/events/stream", ({ request, status }) => {
      const auth = deps.requireStreamAuth(request);
      if (!auth) return status(401, { error: "Unauthorized" });

      const origin = request.headers.get("origin") ?? "*";
      const encoder = new TextEncoder();
      let teardown: (() => void) | undefined;

      const stream = new ReadableStream<Uint8Array>({
        cancel: () => {
          teardown?.();
        },
        start: (controller) => {
          let closed = false;
          let heartbeat: ReturnType<typeof setInterval> | undefined;

          const send = (event: Parameters<ControlApiRouteDeps["onConsoleEvent"]>[0] extends (event: infer T) => void ? T : never, eventName = "message") => {
            try {
              controller.enqueue(
                encoder.encode(
                  `id: ${event.id}\nevent: ${eventName}\ndata: ${JSON.stringify(event)}\n\n`,
                ),
              );
            } catch {
              teardown?.();
            }
          };

          heartbeat = setInterval(() => {
            if (!closed) {
              try {
                controller.enqueue(encoder.encode("event: ping\ndata: {}\n\n"));
              } catch {
                teardown?.();
              }
            }
          }, 15_000);

          const cleanup = () => {
            if (closed) {
              return;
            }

            closed = true;
            if (heartbeat) {
              clearInterval(heartbeat);
            }
            unsubscribe();
            try {
              controller.close();
            } catch {
              // no-op
            }
          };
          teardown = cleanup;

          const unsubscribe = deps.onConsoleEvent((event) => {
            if (!closed) {
              send(event);
            }
          });
          send(
            {
              id: String(deps.getConsoleEventVersion()),
              kind: "runtime",
              at: new Date().toISOString(),
            },
            "ready",
          );
          request.signal.addEventListener("abort", cleanup, { once: true });
        },
      });

      return new Response(stream, {
        headers: {
          "content-type": "text/event-stream",
          "cache-control": "no-cache, no-transform",
          connection: "keep-alive",
          "x-accel-buffering": "no",
          "access-control-allow-origin": origin,
          vary: "Origin",
        },
      });
    })
    .get("/api/dlq", ({ request, status }) => {
      const auth = deps.requireAuth(request);
      if (!auth) return status(401, { error: "Unauthorized" });
      return deps.repository.listDlq();
    })
    .post(
      "/api/runtime/stats",
      ({ request, body, status }) => {
        const auth = deps.requireAuth(request);
        if (!auth) return status(401, { error: "Unauthorized" });

        const result = deps.repository.replaceRuntimeDeploymentStats(body);
        deps.publishConsoleEvents("runtime");
        const response = result satisfies ControlApiCountResponse;
        return status(202, response);
      },
      {
        body: t.Array(
          t.Object({
            deploymentId: t.String(),
            reporterId: t.String(),
            flowId: t.String(),
            revisionId: t.String(),
            acceptedCount: t.Numeric(),
            processedCount: t.Numeric(),
            deliveredCount: t.Numeric(),
            retryingCount: t.Numeric(),
            dlqCount: t.Numeric(),
            failedCount: t.Numeric(),
            filteredCount: t.Numeric(),
            dedupedCount: t.Numeric(),
            sinkAttemptCount: t.Numeric(),
            sinkSuccessCount: t.Numeric(),
            sinkFailureCount: t.Numeric(),
            inflightCount: t.Numeric(),
            backlogCount: t.Numeric(),
            lastAcceptedAt: t.Optional(t.Nullable(t.String())),
            lastProcessedAt: t.Optional(t.Nullable(t.String())),
            lastError: t.Optional(t.Nullable(t.String())),
            updatedAt: t.Optional(t.String()),
          }),
        ),
      },
    )
    .post(
      "/api/runtime/adapter-workloads",
      ({ request, body, status }) => {
        const auth = deps.requireAuth(request);
        if (!auth) return status(401, { error: "Unauthorized" });

        const result = deps.repository.replaceAdapterWorkloadStatuses(body);
        deps.publishConsoleEvents("runtime");
        const response = result satisfies ControlApiCountResponse;
        return status(202, response);
      },
      {
        body: t.Object({
          reporterId: t.String(),
          reportedAt: t.Optional(t.String()),
          workloads: t.Array(adapterWorkloadStatusSchema),
        }),
      },
    )
    .post(
      "/api/runtime/runs",
      ({ request, body, status }) => {
        const auth = deps.requireAuth(request);
        if (!auth) return status(401, { error: "Unauthorized" });

        const result = deps.repository.appendRunSummary(body);
        deps.publishConsoleEvents("runtime");
        return status(201, result);
      },
      {
        body: runtimeRunSummarySchema,
      },
    )
    .post(
      "/api/runtime/runs/batch",
      ({ request, body, status }) => {
        const auth = deps.requireAuth(request);
        if (!auth) return status(401, { error: "Unauthorized" });

        const result = deps.repository.appendRunSummaries(body);
        deps.publishConsoleEvents("runtime");
        return status(202, result satisfies { inserted: number });
      },
      {
        body: t.Array(runtimeRunSummarySchema),
      },
    )
    .post(
      "/api/runtime/adapter-results",
      ({ request, body, status }) => {
        const auth = deps.requireAuth(request);
        if (!auth) return status(401, { error: "Unauthorized" });

        const result = deps.repository.recordAdapterRunResult(body);
        if (result.updated) {
          deps.publishConsoleEvents("runtime");
        }
        return status(202, result);
      },
      {
        body: adapterRunResultSchema,
      },
    )
    .post(
      "/api/runtime/audit",
      ({ request, body, status }) => {
        const auth = deps.requireAuth(request);
        if (!auth) return status(401, { error: "Unauthorized" });

        const response = deps.repository.appendAuditRecord(body) satisfies ControlApiIdResponse;
        return status(201, response);
      },
      {
        body: t.Object({
          tenantId: t.String(),
          actor: t.String(),
          action: t.String(),
          subjectType: t.String(),
          subjectId: t.String(),
          details: t.Any(),
        }),
      },
    )
    .post(
      "/api/runtime/replays/:id/claim",
      ({ request, params, status }) => {
        const auth = deps.requireAuth(request);
        if (!auth) return status(401, { error: "Unauthorized" });

        const replay = deps.repository.claimReplayRequest(params.id);
        if (!replay) {
          return status(404, { error: "Replay request is not pending" });
        }

        return replay;
      },
    )
    .post(
      "/api/runtime/replays/:id/complete",
      ({ request, params, body, status }) => {
        const auth = deps.requireAuth(request);
        if (!auth) return status(401, { error: "Unauthorized" });

        const nextStatus: ReplayCompletionStatus = body.status;
        const replay = deps.repository.completeReplayRequest(params.id, nextStatus);
        if (!replay) {
          return status(404, { error: "Replay request not found" });
        }

        return replay;
      },
      {
        body: t.Object({
          status: t.Union([t.Literal("completed"), t.Literal("failed")]),
        }),
      },
    )
    .post(
      "/api/runtime/deployments/:id/status",
      ({ request, params, body, status }) => {
        const auth = deps.requireAuth(request);
        if (!auth) return status(401, { error: "Unauthorized" });

        const input: UpdateDeploymentStatusInput = body;
        const deployment = deps.repository.updateDeploymentRuntimeStatus(params.id, input);
        if (!deployment) {
          return status(404, { error: "Deployment not found" });
        }

        deps.publishConsoleEvents("runtime");
        return deployment satisfies ControlApiDeploymentRecord;
      },
      {
        body: t.Object({
          status: t.Optional(t.String()),
          rolloutStatus: t.String(),
        }),
      },
    )
    .post(
      "/api/runtime/samples",
      ({ request, body, status }) => {
        const auth = deps.requireAuth(request);
        if (!auth) return status(401, { error: "Unauthorized" });

        const result = deps.repository.replaceRuntimeSamples(body);
        deps.publishConsoleEvents("runtime");
        const response = result satisfies ControlApiCountResponse;
        return status(202, response);
      },
      {
        body: t.Array(
          t.Object({
            deploymentId: t.String(),
            flowId: t.String(),
            revisionId: t.String(),
            sourceKind: t.Union([t.Literal("http"), t.Literal("nats"), t.Literal("kafka")]),
            sourceRef: t.String(),
            payload: t.Any(),
            observedAt: t.Optional(t.String()),
          }),
        ),
      },
    );
}
