# Streaming Correctness & Throughput Implementation Plan

> **STATUS: EXECUTED AND SUPERSEDED (2026-07-03, commits 3f7bdc6..bb73b5b on main).**
> Do NOT re-execute this plan. All 8 tasks landed with review-driven fix rounds that
> deliberately diverge from the code below: shared-flow-spec WAS changed (re-execution-safe
> processor state, prototype-pollution guards in setPath/deletePath, batchingPolicy
> validation); ROUTER_MAX_ACK_PENDING defaults to 64, not 256; consumer drift recreation
> is an eager consumers.info check (the subscribe-error trigger below never fires with
> nats v2); the consume loop lives in an exported consumeJetStreamMessages; sink batchers
> are revision-scoped. Git history from 3f7bdc6 through bb73b5b is the source of truth.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the five high-severity streaming findings (lost retries on crash, no-op rate-limit/dedupe processors, redact simulator/runtime divergence, DLQ discard policy, adapter poison pills) and lift the router's throughput ceiling (explicit consumer limits, bounded parallelism with per-partition-key ordering, HTTP sink batching).

**Architecture:** All hot-path changes live in `apps/router-workers` (JetStream consumer in `nats.ts`, execution engine in `phase2-worker.ts`). The adapter poison-pill fix lives in `apps/adapter-redpanda/src/runtime.ts`. Stream config fixes are shell/YAML defaults in `deploy/`. No changes to `packages/shared-flow-spec` — the simulator's redact semantics are the intended contract; the runtime is aligned to it.

**Tech Stack:** Bun (runtime + test runner `bun test`), TypeScript, NATS JetStream via the `nats` npm client (v2 API: `consumerOpts()`, push consumers with `deliverTo`), Elysia.

**Verification commands** (used throughout):
- Router tests: `cd apps/router-workers && bun test`
- Router typecheck: `cd apps/router-workers && bunx tsc --noEmit -p tsconfig.json`
- Adapter tests: `cd apps/adapter-redpanda && bun test`
- Adapter typecheck: `cd apps/adapter-redpanda && bunx tsc -p tsconfig.json --noEmit`

**Key background for someone new to this codebase:**
- A `CanonicalEnvelope` (defined in `packages/shared-flow-spec/src/types.ts`) is the unit of work. It carries `headers: Record<string, string>`; the router tracks the delivery attempt in header `x-router-attempt` (see `apps/router-workers/src/phase2-worker.ts:45`, helpers `currentAttempt`/`nextAttemptEnvelope` at lines 132–145).
- `RouterWorkerRuntime.processDeploymentMessage` (phase2-worker.ts:1601) executes a FlowSpec against an envelope and delivers to sinks. It is invoked (a) directly by HTTP ingress in local mode, and (b) from JetStream subscriptions created in `ensureSubscriptions` (phase2-worker.ts:1433), which cover the deployment's ingress, retry, and replay subject patterns.
- `NatsMessageBus.subscribeToJetStream` (nats.ts:247) acks a message after the handler resolves and naks on throw. The `for await` loop is currently strictly serial per subscription.
- Tests use plain `bun:test`, a `createBusSpy()` fake bus, and `StaticDeploymentSource` (see `apps/router-workers/src/__tests__/phase2-worker.test.ts:278-360`).

---

### Task 1: Publish retries durably before ack (data-loss fix)

**Problem:** `scheduleRetry` (phase2-worker.ts:1522-1557) defers the durable publish to the retry stream behind an in-memory `setTimeout`. The consumer loop acks the original message as soon as the handler returns (nats.ts:277). A crash between the ack and the timer firing silently drops the event.

**Fix:** In NATS mode, publish the retry envelope to JetStream *synchronously inside the handler* (so it is durable before the ack), carrying the intended delay in a new `x-router-not-before` header. The retry consumer honors the header by sleeping the remaining time before processing. Local (busless) mode keeps the existing `setTimeout` behavior — it has no durable stream to lean on and existing tests depend on it.

**Files:**
- Modify: `apps/router-workers/src/phase2-worker.ts` (lines 45, 24-25 imports, 1442-1449, 1522-1557)
- Test: `apps/router-workers/src/__tests__/phase2-worker.test.ts`

- [ ] **Step 1: Write the failing test**

Add to `apps/router-workers/src/__tests__/phase2-worker.test.ts` (inside `describe("phase2 worker runtime", ...)`):

```typescript
test("publishes retries durably to the retry stream before returning", async () => {
  const spec = buildHttpFlow();
  const deployment = buildDeployment(spec, {
    sinks: {
      http_out_default: {
        kind: "http",
        connectorId: "http_out_default",
        url: "https://example.test/webhook",
      },
    },
  });

  const spy = createBusSpy();
  const runtime = new RouterWorkerRuntime(buildRuntimeConfig(), new StaticDeploymentSource([deployment]), {
    messageBus: spy.bus,
    fetchImpl: (async () => new Response("boom", { status: 500 })) as unknown as typeof fetch,
  });

  await runtime.syncDeployments();
  const result = await runtime.ingestEnvelope({
    deploymentId: deployment.id,
    tenantId: spec.metadata.tenantId,
    flowId: spec.metadata.flowId,
    revisionId: spec.metadata.revisionId,
    messageId: "message-durable-retry",
    sourceRef: "http://localhost/events",
    partitionKey: spec.metadata.tenantId,
    payload: { kind: "order", amount: 42 },
    headers: {},
  });

  expect(result.run.status).toBe("retrying");

  // The retry must already be durable — no setTimeout window.
  const retryPublish = spy.jetstreamPublishes.find((entry) => entry.subject.startsWith("router.retry."));
  expect(retryPublish).toBeDefined();
  const retryEnvelope = JSON.parse(retryPublish!.data) as {
    headers: Record<string, string>;
  };
  expect(retryEnvelope.headers["x-router-attempt"]).toBe("2");
  expect(retryEnvelope.headers["x-router-not-before"]).toBeDefined();
  await runtime.stop();
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd apps/router-workers && bun test src/__tests__/phase2-worker.test.ts -t "publishes retries durably"`
Expected: FAIL — `retryPublish` is `undefined` because the publish is still behind `setTimeout`.

- [ ] **Step 3: Implement the durable retry publish**

In `apps/router-workers/src/phase2-worker.ts`:

Next to `ATTEMPT_HEADER` (line 45), add:

```typescript
const NOT_BEFORE_HEADER = "x-router-not-before";
// Cap how long a consumer will hold a not-yet-due retry. Must stay well
// below the consumer ack wait so held messages are not redelivered mid-hold.
const MAX_RETRY_HOLD_MS = 10_000;
```

Extend the `./delivery` import (line 24) to include `sleep`:

```typescript
import { deliverHttpSink, deliverNatsSink, sleep, toDeliveryAttempt } from "./delivery";
```

Replace the body of `scheduleRetry` (lines 1522-1557) with:

```typescript
  private async scheduleRetry(
    deployment: RouterDeployment,
    envelope: CanonicalEnvelope,
  ): Promise<void> {
    const nextEnvelope = nextAttemptEnvelope(envelope);
    const delayMs = this.config.retryBaseDelayMs * currentAttempt(envelope);

    if (this.messageBus) {
      // Publish before the current message is acked so a crash between the
      // ack and the retry cannot drop the event. The delay is carried in the
      // envelope and honored by the retry consumer.
      nextEnvelope.headers[NOT_BEFORE_HEADER] = new Date(Date.now() + delayMs).toISOString();
      await this.messageBus.publishToJetStream(
        buildRetrySubject(
          nextEnvelope.tenantId,
          nextEnvelope.flowId,
          nextEnvelope.revisionId,
          nextEnvelope.messageId,
        ),
        encodeJsonMessage(nextEnvelope),
      );
      return;
    }

    const processLocally = async () => {
      const currentDeployment = this.deployments.get(deployment.id);
      if (currentDeployment) {
        await this.processDeploymentMessage(currentDeployment, nextEnvelope, "scheduled retry");
      }
    };

    if (delayMs <= 0) {
      await processLocally();
      return;
    }

    setTimeout(() => {
      void processLocally().catch(() => undefined);
    }, delayMs);
  }
```

In `ensureSubscriptions`, update the JetStream handler (lines 1442-1449) to honor the header:

```typescript
        async (data) => {
          const envelope = decodeJsonMessage<CanonicalEnvelope>(data);
          const notBefore = envelope.headers[NOT_BEFORE_HEADER];
          if (notBefore) {
            const waitMs = Date.parse(notBefore) - Date.now();
            if (Number.isFinite(waitMs) && waitMs > 0) {
              await sleep(Math.min(waitMs, MAX_RETRY_HOLD_MS));
            }
          }
          const currentDeployment = findDeploymentBySourceSubject(this.deployments.values(), subject, "jetstream");
          if (!currentDeployment) {
            return;
          }

          await this.processDeploymentMessage(currentDeployment, envelope);
        },
```

- [ ] **Step 4: Run the full router suite**

Run: `cd apps/router-workers && bun test && bunx tsc --noEmit -p tsconfig.json`
Expected: All tests PASS, including the existing local-mode test `"retries and sends to DLQ when HTTP delivery keeps failing"` (it runs without a bus, so the `setTimeout` path it depends on is preserved). Typecheck clean.

- [ ] **Step 5: Commit**

```bash
git add apps/router-workers/src/phase2-worker.ts apps/router-workers/src/__tests__/phase2-worker.test.ts
git commit -m "Publish retries durably before ack to close crash data-loss window

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: Deployment-scoped rate-limit and dedupe state

**Problem:** `processDeploymentMessage` creates fresh `dedupeWindow`/`rateLimit` maps per message (phase2-worker.ts:1607-1616), so `rate_limit` can never trigger and `dedupe_window` never dedupes across messages. Also, `rate_limit` buckets on wall-clock `Date.now()` (line 539) while `dedupe_window` uses `envelope.receivedAt` — replays behave differently from the original run.

**Fix:** Hold processor state per deployment on the runtime instance, and bucket rate limits on `envelope.receivedAt` (matching the simulator, `packages/shared-flow-spec/src/simulator.ts:245-255`). Bound dedupe memory by pruning expired entries when the map grows large.

**Files:**
- Modify: `apps/router-workers/src/phase2-worker.ts` (lines 538-548, 550-560, ~800 class fields, 1607-1616, 1210-1211)
- Test: `apps/router-workers/src/__tests__/phase2-worker.test.ts`

- [ ] **Step 1: Write the failing tests**

Add to `apps/router-workers/src/__tests__/phase2-worker.test.ts`. First add `CanonicalEnvelope` to the existing shared-flow-spec type import at the top of the file:

```typescript
import { compileFlowSpec, type CanonicalEnvelope, type FlowSpec } from "@rohrpost/shared-flow-spec";
```

Then add the tests:

```typescript
function buildStatefulProcessorFlow(processor: FlowSpec["processors"][number]): FlowSpec {
  const spec = buildHttpFlow();
  spec.sources[0].nextNodeIds = [processor.id];
  spec.processors = [processor];
  spec.routes[0].fromNodeId = processor.id;
  return spec;
}

function buildTestEnvelope(spec: FlowSpec, messageId: string, payload: unknown): CanonicalEnvelope {
  return {
    tenantId: spec.metadata.tenantId,
    flowId: spec.metadata.flowId,
    revisionId: spec.metadata.revisionId,
    messageId,
    sourceRef: "test-source",
    partitionKey: spec.metadata.tenantId,
    headers: {},
    payload,
    receivedAt: "2026-07-03T10:00:00.500Z",
    traceId: messageId,
  };
}

test("rate limits across messages received in the same second", async () => {
  const spec = buildStatefulProcessorFlow({
    id: "processor-rate",
    kind: "rate_limit",
    perSecond: 1,
    nextNodeIds: ["route-http"],
  });
  const deployment = buildDeployment(spec, {
    sinks: {
      http_out_default: { kind: "http", connectorId: "http_out_default", url: "https://example.test/webhook" },
    },
  });
  const runtime = new RouterWorkerRuntime(buildRuntimeConfig(), new StaticDeploymentSource([deployment]), {
    fetchImpl: (async () => new Response("ok", { status: 200 })) as unknown as typeof fetch,
  });
  await runtime.syncDeployments();

  const first = await runtime.replay({
    deploymentId: deployment.id,
    envelope: buildTestEnvelope(spec, "rate-1", { kind: "order" }),
    reason: "test",
    requestedAt: "2026-07-03T10:00:01.000Z",
  });
  const second = await runtime.replay({
    deploymentId: deployment.id,
    envelope: buildTestEnvelope(spec, "rate-2", { kind: "order" }),
    reason: "test",
    requestedAt: "2026-07-03T10:00:01.000Z",
  });

  expect(first.run.status).toBe("delivered");
  expect(second.run.status).toBe("failed");
  expect(second.run.reason).toContain("rate limited by processor-rate");
  await runtime.stop();
});

test("dedupes across messages within the dedupe window", async () => {
  const spec = buildStatefulProcessorFlow({
    id: "processor-dedupe",
    kind: "dedupe_window",
    keyPath: "orderId",
    windowMs: 60_000,
    nextNodeIds: ["route-http"],
  });
  const deployment = buildDeployment(spec, {
    sinks: {
      http_out_default: { kind: "http", connectorId: "http_out_default", url: "https://example.test/webhook" },
    },
  });
  const runtime = new RouterWorkerRuntime(buildRuntimeConfig(), new StaticDeploymentSource([deployment]), {
    fetchImpl: (async () => new Response("ok", { status: 200 })) as unknown as typeof fetch,
  });
  await runtime.syncDeployments();

  const first = await runtime.replay({
    deploymentId: deployment.id,
    envelope: buildTestEnvelope(spec, "dedupe-1", { orderId: "o-1" }),
    reason: "test",
    requestedAt: "2026-07-03T10:00:01.000Z",
  });
  const second = await runtime.replay({
    deploymentId: deployment.id,
    envelope: buildTestEnvelope(spec, "dedupe-2", { orderId: "o-1" }),
    reason: "test",
    requestedAt: "2026-07-03T10:00:01.000Z",
  });

  expect(first.run.status).toBe("delivered");
  expect(second.run.status).toBe("deduped");
  await runtime.stop();
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd apps/router-workers && bun test src/__tests__/phase2-worker.test.ts -t "across messages"`
Expected: BOTH FAIL — second message is `delivered` in each case because state resets per message.

- [ ] **Step 3: Implement deployment-scoped state**

In `apps/router-workers/src/phase2-worker.ts`:

Add a module constant near `MAX_RETRY_HOLD_MS`:

```typescript
// Bound for the per-deployment dedupe map before expired entries are pruned.
const DEDUPE_STATE_MAX_ENTRIES = 50_000;
```

Add a class field next to `deploymentStats` (around line 792):

```typescript
  private readonly processorState = new Map<
    string,
    {
      dedupeWindow: Map<string, number>;
      rateLimit: Map<string, { bucketAt: number; count: number }>;
    }
  >();
```

Add a private method near `processDeploymentMessage`:

```typescript
  private processorStateFor(deploymentId: string): {
    dedupeWindow: Map<string, number>;
    rateLimit: Map<string, { bucketAt: number; count: number }>;
  } {
    let state = this.processorState.get(deploymentId);
    if (!state) {
      state = { dedupeWindow: new Map(), rateLimit: new Map() };
      this.processorState.set(deploymentId, state);
    }
    return state;
  }
```

In `processDeploymentMessage` (lines 1607-1616), replace the fresh maps:

```typescript
    const context: ExecutionContext = {
      deployment,
      trace: [],
      sinkDeliveries: [],
      attemptedSinkIds: new Set<string>(),
      state: this.processorStateFor(deployment.id),
    };
```

In the `rate_limit` case (line 539), bucket on the envelope timestamp instead of wall clock:

```typescript
      case "rate_limit": {
        const windowBucket = Math.floor(Date.parse(envelope.receivedAt) / 1000);
        const current = context.state.rateLimit.get(processor.id);
        if (!current || current.bucketAt !== windowBucket) {
          context.state.rateLimit.set(processor.id, { bucketAt: windowBucket, count: 1 });
        } else if (current.count >= processor.perSecond) {
          return { payload: nextPayload, status: "failed", reason: `rate limited by ${processor.id}` };
        } else {
          current.count += 1;
        }
        break;
      }
```

In the `dedupe_window` case, after `context.state.dedupeWindow.set(dedupeKey, currentTime);` (line 558), add pruning:

```typescript
        if (context.state.dedupeWindow.size > DEDUPE_STATE_MAX_ENTRIES) {
          for (const [key, seenAt] of context.state.dedupeWindow) {
            if (currentTime - seenAt >= processor.windowMs) {
              context.state.dedupeWindow.delete(key);
            }
          }
        }
```

At the deployment-removal site (lines 1210-1211), add cleanup:

```typescript
        this.deployments.delete(deploymentId);
        this.deploymentStats.delete(deploymentId);
        this.processorState.delete(deploymentId);
```

- [ ] **Step 4: Run the full router suite**

Run: `cd apps/router-workers && bun test && bunx tsc --noEmit -p tsconfig.json`
Expected: All PASS. (No existing test relies on per-message state — the switch cases previously could never trigger across messages.)

- [ ] **Step 5: Commit**

```bash
git add apps/router-workers/src/phase2-worker.ts apps/router-workers/src/__tests__/phase2-worker.test.ts
git commit -m "Scope rate-limit and dedupe state to the deployment, bucket on receivedAt

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Align runtime redact with the simulator (mask, don't delete)

**Problem:** The runtime deletes redacted paths (`deletePath`, phase2-worker.ts:382-388) while the simulator masks them with `processor.mask ?? "[redacted]"` (`packages/shared-flow-spec/src/simulator.ts:69-79`, invoked at line 214). Users validate flows via simulation, then production behaves differently. `RedactProcessorNode` already declares `mask?: string` (types.ts:116-120), so masking is the intended contract.

**Files:**
- Modify: `apps/router-workers/src/phase2-worker.ts` (lines 1-11 imports, 382-388)
- Test: `apps/router-workers/src/__tests__/phase2-worker.test.ts`

- [ ] **Step 1: Write the failing test**

Add to `apps/router-workers/src/__tests__/phase2-worker.test.ts` (reuses `buildStatefulProcessorFlow` from Task 2):

```typescript
test("masks redacted paths instead of deleting them, matching the simulator", async () => {
  const spec = buildStatefulProcessorFlow({
    id: "processor-redact",
    kind: "redact",
    paths: ["ssn"],
    nextNodeIds: ["route-http"],
  });
  const deployment = buildDeployment(spec, {
    sinks: {
      http_out_default: { kind: "http", connectorId: "http_out_default", url: "https://example.test/webhook" },
    },
  });

  const bodies: Array<{ payload: Record<string, unknown> }> = [];
  const runtime = new RouterWorkerRuntime(buildRuntimeConfig(), new StaticDeploymentSource([deployment]), {
    fetchImpl: (async (_input: RequestInfo | URL, init?: RequestInit) => {
      bodies.push(JSON.parse(String(init?.body)) as { payload: Record<string, unknown> });
      return new Response("ok", { status: 200 });
    }) as unknown as typeof fetch,
  });
  await runtime.syncDeployments();

  const result = await runtime.ingestEnvelope({
    deploymentId: deployment.id,
    tenantId: spec.metadata.tenantId,
    flowId: spec.metadata.flowId,
    revisionId: spec.metadata.revisionId,
    messageId: "message-redact",
    sourceRef: "http://localhost/events",
    partitionKey: spec.metadata.tenantId,
    payload: { name: "Ada", ssn: "123-45-6789" },
    headers: {},
  });

  expect(result.run.status).toBe("delivered");
  expect(bodies[0]?.payload.ssn).toBe("[redacted]");
  expect(bodies[0]?.payload.name).toBe("Ada");
  await runtime.stop();
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd apps/router-workers && bun test src/__tests__/phase2-worker.test.ts -t "masks redacted"`
Expected: FAIL — `payload.ssn` is `undefined` (field deleted).

- [ ] **Step 3: Implement masking**

In `apps/router-workers/src/phase2-worker.ts`, replace `applyRedact` (lines 382-388):

```typescript
function applyRedact(payload: unknown, processor: Extract<ProcessorNode, { kind: "redact" }>): unknown {
  let nextPayload = payload;
  for (const path of processor.paths) {
    nextPayload = setPath(nextPayload, path, processor.mask ?? "[redacted]");
  }
  return nextPayload;
}
```

`deletePath` is now unused in this file — remove it from the `@rohrpost/shared-flow-spec` import block (lines 1-11):

```typescript
import {
  compileFlowSpec,
  evaluatePredicate,
  getPath,
  setPath,
  type CanonicalEnvelope,
  type FlowSpec,
  type ProcessorNode,
  type SinkNode,
} from "@rohrpost/shared-flow-spec";
```

- [ ] **Step 4: Run the full router suite**

Run: `cd apps/router-workers && bun test && bunx tsc --noEmit -p tsconfig.json`
Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add apps/router-workers/src/phase2-worker.ts apps/router-workers/src/__tests__/phase2-worker.test.ts
git commit -m "Mask redacted paths in the runtime to match simulator semantics

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: DLQ stream keeps the newest failures (discard old)

**Problem:** The DLQ stream defaults to `--discard new` (`deploy/nats/bootstrap.sh:16`, `deploy/k8s/nats-bootstrap-job.yaml:43`). When the 2GB cap is hit, the *most recent* failures — the ones an operator needs — are silently dropped.

**Files:**
- Modify: `deploy/nats/bootstrap.sh:16`
- Modify: `deploy/k8s/nats-bootstrap-job.yaml:43`

- [ ] **Step 1: Change the defaults**

In `deploy/nats/bootstrap.sh` line 16, change:

```bash
NATS_STREAM_DLQ_DISCARD="${NATS_STREAM_DLQ_DISCARD:-new}"
```

to:

```bash
NATS_STREAM_DLQ_DISCARD="${NATS_STREAM_DLQ_DISCARD:-old}"
```

In `deploy/k8s/nats-bootstrap-job.yaml` line 43, change the embedded default:

```yaml
              NATS_STREAM_DLQ_DISCARD="${NATS_STREAM_DLQ_DISCARD:-new}"
```

to:

```yaml
              NATS_STREAM_DLQ_DISCARD="${NATS_STREAM_DLQ_DISCARD:-old}"
```

Leave the other streams alone: `ingress`/`work`/`retry`/`replay` are WorkQueue streams where `discard new` is correct backpressure (reject producers rather than drop queued work — the router maps this to HTTP 503 in `classifyNatsPublishError`, nats.ts:90-133), and `audit` already uses `old`.

- [ ] **Step 2: Verify**

Run: `bash -n deploy/nats/bootstrap.sh && grep -n "DLQ_DISCARD" deploy/nats/bootstrap.sh deploy/k8s/nats-bootstrap-job.yaml`
Expected: no syntax error; both defaults show `:-old}`.

Note for rollout: `bootstrap.sh` uses `edit_stream` when the stream exists, so re-running the bootstrap job reconfigures live DLQ streams in place.

- [ ] **Step 3: Commit**

```bash
git add deploy/nats/bootstrap.sh deploy/k8s/nats-bootstrap-job.yaml
git commit -m "Default DLQ stream to discard old so newest failures are retained

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Adapter poison-pill bound (maxDeliver + terminate)

**Problem:** `handleWorkMessage` (`apps/adapter-redpanda/src/runtime.ts:809-835`) naks failures with a 5s delay and no delivery cap — a permanently failing work item (bad credentials, unresolvable target) redelivers forever and occupies the work stream.

**Fix:** Set `maxDeliver` on the consumer, add it to the consumer-drift check so existing consumers get recreated, and explicitly `term()` a message once its redelivery count reaches the cap. The failed delivery record is already reported to control-api by `processWorkItem`, so terminating is observable. (Publishing to `router.dlq.>` from the adapter is deliberately out of scope — the DLQ subject builders live in router-workers and this pass avoids a new cross-package contract.)

**Files:**
- Modify: `apps/adapter-redpanda/src/runtime.ts` (lines ~100, ~598-625 drift check + `JetStreamConsumerLike` type, ~772, 809-835)
- Test: `apps/adapter-redpanda/test/runtime.test.ts`

- [ ] **Step 1: Write the failing tests**

Add to `apps/adapter-redpanda/test/runtime.test.ts`. Import the two functions under test alongside the file's existing imports from `../src/runtime`:

```typescript
import { selectAdapterRuntimeConsumersToDelete, shouldTerminateWorkMessage } from "../src/runtime";
```

```typescript
describe("shouldTerminateWorkMessage", () => {
  test("terminates once redelivery count reaches the cap", () => {
    expect(shouldTerminateWorkMessage(5, 5)).toBe(true);
    expect(shouldTerminateWorkMessage(6, 5)).toBe(true);
  });

  test("naks below the cap", () => {
    expect(shouldTerminateWorkMessage(1, 5)).toBe(false);
    expect(shouldTerminateWorkMessage(4, 5)).toBe(false);
  });
});

describe("adapter consumer max_deliver drift", () => {
  test("recreates the work consumer when max_deliver drifts", () => {
    const consumers = [
      {
        name: "adapter_redpanda_work_v2",
        config: {
          filter_subject: "router.work.>",
          ack_wait: 60_000 * 1_000_000,
          max_ack_pending: 1_024,
          max_deliver: 3,
        },
      },
    ];

    expect(
      selectAdapterRuntimeConsumersToDelete(consumers, "adapter_redpanda_work_v2", "router.work.>"),
    ).toEqual(["adapter_redpanda_work_v2"]);
  });
});
```

Note: the existing drift test in this file shows the exact shape `selectAdapterRuntimeConsumersToDelete` expects — if `max_ack_pending` was overridden via `ADAPTER_MAX_ACK_PENDING` in the test environment, mirror whatever value the existing drift tests use.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd apps/adapter-redpanda && bun test test/runtime.test.ts -t "max_deliver"`
Expected: FAIL — `shouldTerminateWorkMessage` does not exist; drift selection returns `[]` because `max_deliver` is not checked.

- [ ] **Step 3: Implement**

In `apps/adapter-redpanda/src/runtime.ts`:

Next to `ADAPTER_MAX_ACK_PENDING` (line 100), add:

```typescript
const ADAPTER_MAX_DELIVER = readPositiveIntegerEnv("ADAPTER_MAX_DELIVER", 5, 1, 100);
```

Add an exported helper near `selectAdapterRuntimeConsumersToDelete` (line 598):

```typescript
export function shouldTerminateWorkMessage(
  redeliveryCount: number,
  maxDeliver: number = ADAPTER_MAX_DELIVER,
): boolean {
  return redeliveryCount >= maxDeliver;
}
```

Add `max_deliver?: number` to the `JetStreamConsumerLike` `config` type in this file (it already declares `ack_wait` and `max_ack_pending`), and extend the drift condition inside `selectAdapterRuntimeConsumersToDelete` (around line 615):

```typescript
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
```

In the consumer options block (after `opts.maxAckPending(...)` at line 772), add:

```typescript
    opts.maxDeliver(ADAPTER_MAX_DELIVER);
```

Replace the two failure branches of `handleWorkMessage` (lines 821-834) with a shared helper:

```typescript
      } else {
        this.nakOrTerminate(jsMessage);
      }
    } catch {
      this.nakOrTerminate(jsMessage);
    }
  }

  private nakOrTerminate(jsMessage: JsMsg): void {
    try {
      if (shouldTerminateWorkMessage(jsMessage.info.redeliveryCount)) {
        // Poison pill: the failed delivery record has already been reported
        // to control-api; stop occupying the work stream.
        jsMessage.term();
      } else {
        jsMessage.nak(5_000);
      }
    } catch {
      // The server may already have closed this delivery during shutdown.
    }
  }
```

- [ ] **Step 4: Run the adapter suite**

Run: `cd apps/adapter-redpanda && bun test && bunx tsc -p tsconfig.json --noEmit`
Expected: All PASS. If an existing drift test constructs a consumer config without `max_deliver`, it will now (correctly) be selected for recreation — update that test's expected consumer config to include `max_deliver: 5`.

- [ ] **Step 5: Commit**

```bash
git add apps/adapter-redpanda/src/runtime.ts apps/adapter-redpanda/test/runtime.test.ts
git commit -m "Bound adapter work redelivery with maxDeliver and terminal term()

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: Explicit router consumer limits (ackWait, maxAckPending) with drift recreation

**Problem:** `createJetStreamConsumerOptions` (nats.ts:168-179) sets neither `ackWait` nor `maxAckPending`, silently relying on broker defaults. Task 7 introduces concurrent processing, which requires these to be explicit and tuned together.

**Fix:** Mirror the adapter's pattern: env-configurable constants, applied in the consumer options, with config-drift detection so changed values recreate the durable consumer on the next subscribe conflict.

**Files:**
- Modify: `apps/router-workers/src/nats.ts` (lines 135-179 helpers/options, 144-166 drift selection, 252-267 subscribe catch)
- Test: `apps/router-workers/src/__tests__/nats.test.ts`

- [ ] **Step 1: Write the failing tests**

In `apps/router-workers/src/__tests__/nats.test.ts`, extend the existing `createJetStreamConsumerOptions` test's config type and assertions:

```typescript
    const config = (options as unknown as {
      config: {
        deliver_policy?: string;
        ack_policy?: string;
        durable_name?: string;
        deliver_subject?: string;
        ack_wait?: number;
        max_ack_pending?: number;
      };
    }).config;

    expect(config.deliver_policy).toBe("all");
    expect(config.ack_policy).toBe("explicit");
    expect(config.durable_name).toBe("router_ingress_tenant-a_flow-http_rev-http-v1__");
    expect(config.deliver_subject).toStartWith("_INBOX.");
    expect(config.ack_wait).toBe(30_000 * 1_000_000);
    expect(config.max_ack_pending).toBe(256);
```

Add a drift test to the `selectStaleJetStreamConsumers` describe block:

```typescript
  test("selects the same-name consumer when its ack config drifted", () => {
    const subject = "router.ingress.tenant_demo.flow_demo_orders.rev_demo_orders_v1.>";
    const durable = "router_ingress_tenant_demo_flow_demo_orders_rev_demo_orders_v1__";

    const drifted = selectStaleJetStreamConsumers(
      [{ name: durable, config: { filter_subject: subject, ack_wait: 5_000 * 1_000_000, max_ack_pending: 10 } }],
      subject,
      subject,
    );
    expect(drifted).toEqual([durable]);

    const matching = selectStaleJetStreamConsumers(
      [{ name: durable, config: { filter_subject: subject, ack_wait: 30_000 * 1_000_000, max_ack_pending: 256 } }],
      subject,
      subject,
    );
    expect(matching).toEqual([]);
  });
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd apps/router-workers && bun test src/__tests__/nats.test.ts`
Expected: FAIL — `ack_wait`/`max_ack_pending` are undefined; drift selection returns `[]` for same-name consumers.

- [ ] **Step 3: Implement**

In `apps/router-workers/src/nats.ts`:

Below `sanitizeConsumerName` (line 137), add:

```typescript
function readPositiveIntegerEnv(name: string, fallback: number, min: number, max: number): number {
  const parsed = Number(process.env[name]);
  if (!Number.isInteger(parsed)) {
    return fallback;
  }
  return Math.min(Math.max(parsed, min), max);
}

export const ROUTER_ACK_WAIT_MS = readPositiveIntegerEnv("ROUTER_CONSUMER_ACK_WAIT_MS", 30_000, 1_000, 300_000);
export const ROUTER_MAX_ACK_PENDING = readPositiveIntegerEnv("ROUTER_CONSUMER_MAX_ACK_PENDING", 256, 1, 10_000);
const ROUTER_ACK_WAIT_NS = ROUTER_ACK_WAIT_MS * 1_000_000;
```

Extend `JetStreamConsumerLike` (lines 40-46):

```typescript
type JetStreamConsumerLike = {
  name?: string;
  push_bound?: boolean;
  config?: {
    filter_subject?: string;
    ack_wait?: number;
    max_ack_pending?: number;
  };
};
```

In `createJetStreamConsumerOptions`, before `return opts;`:

```typescript
  opts.ackWait(ROUTER_ACK_WAIT_MS);
  opts.maxAckPending(ROUTER_MAX_ACK_PENDING);
```

Replace the body of `selectStaleJetStreamConsumers` (lines 144-166):

```typescript
export function selectStaleJetStreamConsumers(
  consumers: JetStreamConsumerLike[],
  subject: string,
  durableName: string,
): string[] {
  const expectedDurableName = sanitizeConsumerName(durableName);

  return consumers.flatMap((consumer) => {
    if (!consumer?.name || consumer.push_bound) {
      return [];
    }

    if (consumer.config?.filter_subject !== subject) {
      return [];
    }

    if (consumer.name !== expectedDurableName) {
      return [consumer.name];
    }

    const drifted =
      consumer.config?.ack_wait !== ROUTER_ACK_WAIT_NS
      || consumer.config?.max_ack_pending !== ROUTER_MAX_ACK_PENDING;
    return drifted ? [consumer.name] : [];
  });
}
```

In `subscribeToJetStream`, broaden the retry-once cleanup (lines 254-267) so config-mismatch errors also trigger recreation — attempt cleanup on any subscribe error and rethrow the original error if nothing was removed:

```typescript
    let subscription: Awaited<ReturnType<typeof this.js.subscribe>>;
    try {
      subscription = await this.js.subscribe(subject, opts);
    } catch (error) {
      const removed = await this.deleteStaleJetStreamConsumers(subject, options.durableName);
      if (!removed) {
        throw error;
      }

      subscription = await this.js.subscribe(subject, opts);
    }
```

`isFilteredConsumerConflict` (line 139) becomes unused — delete it.

- [ ] **Step 4: Run the full router suite**

Run: `cd apps/router-workers && bun test && bunx tsc --noEmit -p tsconfig.json`
Expected: All PASS, including the pre-existing `selectStaleJetStreamConsumers` test (its same-name entry is `push_bound`, so it is still skipped).

- [ ] **Step 5: Commit**

```bash
git add apps/router-workers/src/nats.ts apps/router-workers/src/__tests__/nats.test.ts
git commit -m "Set explicit ackWait and maxAckPending on router consumers with drift recreation

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: Bounded parallel consumption with per-partition-key ordering

**Problem:** The consumer loop (nats.ts:269-283) awaits each handler — FlowSpec execution *plus sink HTTP latency* — before pulling the next message. One slow sink caps a deployment below 1 event/sec. It also `throw`s after `nak()`, which kills the loop on the first handler error (the subscription silently stops consuming). Naive parallelism would break the documented "ordering per partition key" contract, which today only holds as a side effect of the serial loop.

**Fix:** A `KeyedConcurrencyLimiter` — tasks with the same key run serially in submission order; total in-flight tasks are bounded. The bus loop dispatches through it (key = envelope `partitionKey`, extracted by a caller-supplied function), acks/naks per message on task settlement, keeps consuming after failures, and `unsubscribe()` now drains in-flight work (closing the shutdown gap).

**Files:**
- Create: `apps/router-workers/src/keyed-concurrency.ts`
- Create: `apps/router-workers/src/__tests__/keyed-concurrency.test.ts`
- Modify: `apps/router-workers/src/nats.ts` (subscribeToJetStream, lines 247-292)
- Modify: `apps/router-workers/src/phase2-types.ts` (MessageBus interface, lines 240-244; RouterWorkerRuntimeConfig)
- Modify: `apps/router-workers/src/phase2-worker.ts` (ensureSubscriptions, lines 1440-1452)
- Modify: `apps/router-workers/src/config.ts` (line ~64)
- Modify: `apps/router-workers/src/__tests__/phase2-worker.test.ts` (buildRuntimeConfig)

- [ ] **Step 1: Write the failing limiter tests**

Create `apps/router-workers/src/__tests__/keyed-concurrency.test.ts`:

```typescript
import { describe, expect, test } from "bun:test";
import { KeyedConcurrencyLimiter } from "../keyed-concurrency";

function deferred(): { promise: Promise<void>; resolve: () => void; reject: (error: Error) => void } {
  let resolve!: () => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<void>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

async function settleMicrotasks(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 0));
}

describe("KeyedConcurrencyLimiter", () => {
  test("runs same-key tasks serially in submission order", async () => {
    const limiter = new KeyedConcurrencyLimiter(4);
    const events: string[] = [];
    const gate = deferred();

    const first = limiter.run("key-a", async () => {
      events.push("first:start");
      await gate.promise;
      events.push("first:end");
    });
    const second = limiter.run("key-a", async () => {
      events.push("second:start");
    });

    await settleMicrotasks();
    expect(events).toEqual(["first:start"]);

    gate.resolve();
    await Promise.all([first, second]);
    expect(events).toEqual(["first:start", "first:end", "second:start"]);
  });

  test("bounds total in-flight tasks across keys", async () => {
    const limiter = new KeyedConcurrencyLimiter(2);
    const events: string[] = [];
    const gateA = deferred();
    const gateB = deferred();

    const taskA = limiter.run("a", async () => {
      events.push("a:start");
      await gateA.promise;
    });
    const taskB = limiter.run("b", async () => {
      events.push("b:start");
      await gateB.promise;
    });
    const taskC = limiter.run("c", async () => {
      events.push("c:start");
    });

    await settleMicrotasks();
    expect(events).toEqual(["a:start", "b:start"]);

    gateA.resolve();
    await taskA;
    await settleMicrotasks();
    expect(events).toEqual(["a:start", "b:start", "c:start"]);

    gateB.resolve();
    await Promise.all([taskB, taskC]);
  });

  test("propagates rejections to the caller without breaking the key chain", async () => {
    const limiter = new KeyedConcurrencyLimiter(2);

    const failing = limiter.run("key-a", async () => {
      throw new Error("boom");
    });
    const following = limiter.run("key-a", async () => "ok");

    await expect(failing).rejects.toThrow("boom");
    await expect(following).resolves.toBe("ok");
  });
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd apps/router-workers && bun test src/__tests__/keyed-concurrency.test.ts`
Expected: FAIL — module `../keyed-concurrency` does not exist.

- [ ] **Step 3: Implement the limiter**

Create `apps/router-workers/src/keyed-concurrency.ts`:

```typescript
// Runs tasks with bounded total concurrency while guaranteeing that tasks
// sharing a key execute serially in submission order. Used by the JetStream
// consumer loop to parallelize message processing without violating the
// per-partition-key ordering contract.
export class KeyedConcurrencyLimiter {
  private readonly chains = new Map<string, Promise<void>>();
  private readonly waiters: Array<() => void> = [];
  private inflight = 0;

  public constructor(private readonly maxInflight: number) {}

  public run<T>(key: string, task: () => Promise<T>): Promise<T> {
    const previous = this.chains.get(key) ?? Promise.resolve();
    const result = previous.then(async () => {
      await this.acquire();
      try {
        return await task();
      } finally {
        this.release();
      }
    });

    const tail = result.then(
      () => undefined,
      () => undefined,
    );
    this.chains.set(key, tail);
    void tail.then(() => {
      if (this.chains.get(key) === tail) {
        this.chains.delete(key);
      }
    });

    return result;
  }

  private async acquire(): Promise<void> {
    if (this.inflight < this.maxInflight) {
      this.inflight += 1;
      return;
    }

    await new Promise<void>((resolve) => {
      this.waiters.push(resolve);
    });
    this.inflight += 1;
  }

  private release(): void {
    this.inflight -= 1;
    this.waiters.shift()?.();
  }
}
```

- [ ] **Step 4: Run the limiter tests**

Run: `cd apps/router-workers && bun test src/__tests__/keyed-concurrency.test.ts`
Expected: PASS (3 tests).

- [ ] **Step 5: Wire the limiter into the bus and runtime**

In `apps/router-workers/src/phase2-types.ts`, update `subscribeToJetStream` in the `MessageBus` interface (lines 240-244):

```typescript
  subscribeToJetStream(
    subject: string,
    handler: (data: Uint8Array, metadata: { subject: string; sequence?: number }) => Promise<void>,
    options: {
      durableName: string;
      concurrency?: number;
      partitionKey?: (data: Uint8Array) => string;
    },
  ): Promise<{ unsubscribe(): Promise<void> }>;
```

Add to `RouterWorkerRuntimeConfig` in the same file (alongside `pollIntervalMs` etc.):

```typescript
  subscriptionConcurrency: number;
```

In `apps/router-workers/src/config.ts`, add to the returned object in `loadRouterWorkerConfig` (after `pollIntervalMs`):

```typescript
    subscriptionConcurrency: parseNumber(env.ROUTER_SUBSCRIPTION_CONCURRENCY, 16),
```

In `apps/router-workers/src/nats.ts`, import the limiter and replace the loop in `subscribeToJetStream` (lines 247-292):

```typescript
import { KeyedConcurrencyLimiter } from "./keyed-concurrency";
```

```typescript
  public async subscribeToJetStream(
    subject: string,
    handler: (data: Uint8Array, metadata: { subject: string; sequence?: number }) => Promise<void>,
    options: {
      durableName: string;
      concurrency?: number;
      partitionKey?: (data: Uint8Array) => string;
    },
  ): Promise<{ unsubscribe(): Promise<void> }> {
    const opts = createJetStreamConsumerOptions(options.durableName);
    let subscription: Awaited<ReturnType<typeof this.js.subscribe>>;
    try {
      subscription = await this.js.subscribe(subject, opts);
    } catch (error) {
      const removed = await this.deleteStaleJetStreamConsumers(subject, options.durableName);
      if (!removed) {
        throw error;
      }

      subscription = await this.js.subscribe(subject, opts);
    }

    const concurrency = Math.max(1, options.concurrency ?? 1);
    const limiter = new KeyedConcurrencyLimiter(concurrency);
    const inflightTasks = new Set<Promise<void>>();

    const loop = (async () => {
      for await (const message of subscription) {
        const jsMessage = message as JsMsg;

        // Bound local queue growth: never hold more than 2x concurrency
        // undispatched-or-running tasks in memory.
        while (inflightTasks.size >= concurrency * 2) {
          await Promise.race(inflightTasks);
        }

        let key = jsMessage.subject;
        if (options.partitionKey) {
          try {
            key = options.partitionKey(jsMessage.data);
          } catch {
            // Fall back to subject-scoped ordering when no key can be derived.
          }
        }

        const task = limiter
          .run(key, () =>
            handler(jsMessage.data, {
              subject: jsMessage.subject,
              sequence: jsMessage.seq,
            }),
          )
          .then(() => {
            jsMessage.ack();
          })
          .catch(() => {
            jsMessage.nak();
          });

        inflightTasks.add(task);
        void task.finally(() => {
          inflightTasks.delete(task);
        });
      }
    })();

    void loop.catch(() => undefined);

    return {
      async unsubscribe() {
        subscription.unsubscribe();
        await Promise.allSettled([...inflightTasks]);
      },
    };
  }
```

(Behavior note: the old loop rethrew after `nak()`, which terminated the subscription on the first handler error. The new loop naks the failed message and keeps consuming — that is the correct behavior and is intentional.)

In `apps/router-workers/src/phase2-worker.ts`, pass concurrency and the key extractor in `ensureSubscriptions` (lines 1440-1452):

```typescript
      const handle = await this.messageBus.subscribeToJetStream(
        subject,
        async (data) => {
          const envelope = decodeJsonMessage<CanonicalEnvelope>(data);
          const notBefore = envelope.headers[NOT_BEFORE_HEADER];
          if (notBefore) {
            const waitMs = Date.parse(notBefore) - Date.now();
            if (Number.isFinite(waitMs) && waitMs > 0) {
              await sleep(Math.min(waitMs, MAX_RETRY_HOLD_MS));
            }
          }
          const currentDeployment = findDeploymentBySourceSubject(this.deployments.values(), subject, "jetstream");
          if (!currentDeployment) {
            return;
          }

          await this.processDeploymentMessage(currentDeployment, envelope);
        },
        {
          durableName: subject,
          concurrency: this.config.subscriptionConcurrency,
          partitionKey: (data) => {
            try {
              return decodeJsonMessage<CanonicalEnvelope>(data).partitionKey ?? "default";
            } catch {
              return "default";
            }
          },
        },
      );
```

In `apps/router-workers/src/__tests__/phase2-worker.test.ts`, add to `buildRuntimeConfig()`:

```typescript
    subscriptionConcurrency: 4,
```

- [ ] **Step 6: Run the full router suite**

Run: `cd apps/router-workers && bun test && bunx tsc --noEmit -p tsconfig.json`
Expected: All PASS. The `createBusSpy` fake in phase2-worker.test.ts types its `subscribeToJetStream` against the `MessageBus` interface, so the widened options type compiles without edits.

Operational note (include in the commit message body): the key extractor costs one extra `JSON.parse` per message; `ROUTER_CONSUMER_MAX_ACK_PENDING` (256) must stay ≥ 2× `ROUTER_SUBSCRIPTION_CONCURRENCY` (16), which it does by default.

- [ ] **Step 7: Commit**

```bash
git add apps/router-workers/src/keyed-concurrency.ts apps/router-workers/src/__tests__/keyed-concurrency.test.ts apps/router-workers/src/nats.ts apps/router-workers/src/phase2-types.ts apps/router-workers/src/phase2-worker.ts apps/router-workers/src/config.ts apps/router-workers/src/__tests__/phase2-worker.test.ts
git commit -m "Process JetStream messages in parallel with per-partition-key ordering

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 8: HTTP sink batching honoring batchingPolicy

**Problem:** Every event is a separate HTTP request to the sink (`delivery.ts:13-47`); the FlowSpec `batchingPolicy` (`packages/shared-flow-spec/src/types.ts:209-214` — `{ enabled, batchSize, flushIntervalMs?, keyPath? }`) is declared but never used. For high-volume flows, per-event requests are the dominant cost.

**Fix:** A per-(deployment, sink) `HttpSinkBatcher` that accumulates `DeliveryPayload`s and flushes at `batchSize` or after `flushIntervalMs` (default 250ms), POSTing `{ "messages": [...] }` to the sink. Each message's `enqueue()` promise resolves with the batch outcome, so ack-after-delivery and the existing per-message retry/DLQ paths are preserved: a failed batch fails every member, and each retries independently. Opt-in only — flows with `enabled: false` (all existing flows) are untouched. Batching only pays off with Task 7's concurrency > 1 (a serial consumer can never fill a batch; the interval flush keeps it correct regardless), and same-partition-key messages still serialize — different keys batch together.

**Files:**
- Create: `apps/router-workers/src/http-sink-batcher.ts`
- Create: `apps/router-workers/src/__tests__/http-sink-batcher.test.ts`
- Modify: `apps/router-workers/src/phase2-worker.ts` (deliverSinkOnce signature + http branch, delivery loop ~1645, stop(), deployment removal ~1210)

- [ ] **Step 1: Write the failing batcher tests**

Create `apps/router-workers/src/__tests__/http-sink-batcher.test.ts`:

```typescript
import { describe, expect, test } from "bun:test";
import { HttpSinkBatcher } from "../http-sink-batcher";
import type { DeliveryPayload, HttpSinkTarget } from "../phase2-types";

const target: HttpSinkTarget = {
  kind: "http",
  connectorId: "http_out_default",
  url: "https://example.test/webhook",
};

function buildPayload(messageId: string): DeliveryPayload {
  return {
    envelope: {
      tenantId: "tenant-a",
      flowId: "flow-http",
      revisionId: "rev-http-v1",
      messageId,
      sourceRef: "test",
      partitionKey: "tenant-a",
      headers: {},
      payload: { messageId },
      receivedAt: "2026-07-03T10:00:00.000Z",
      traceId: messageId,
    },
    payload: { messageId },
    deploymentId: "deployment-1",
    sinkId: "sink-http",
    flowId: "flow-http",
    revisionId: "rev-http-v1",
  };
}

describe("HttpSinkBatcher", () => {
  test("flushes one request once batchSize is reached", async () => {
    const requests: Array<{ messages: unknown[] }> = [];
    const batcher = new HttpSinkBatcher(target, 2, 10_000, (async (_input: RequestInfo | URL, init?: RequestInit) => {
      requests.push(JSON.parse(String(init?.body)) as { messages: unknown[] });
      return new Response("ok", { status: 200 });
    }) as unknown as typeof fetch);

    const [first, second] = await Promise.all([
      batcher.enqueue(buildPayload("m-1")),
      batcher.enqueue(buildPayload("m-2")),
    ]);

    expect(requests.length).toBe(1);
    expect(requests[0].messages.length).toBe(2);
    expect(first.ok).toBe(true);
    expect(second.ok).toBe(true);
  });

  test("flushes a partial batch when the interval elapses", async () => {
    const requests: Array<{ messages: unknown[] }> = [];
    const batcher = new HttpSinkBatcher(target, 10, 20, (async (_input: RequestInfo | URL, init?: RequestInit) => {
      requests.push(JSON.parse(String(init?.body)) as { messages: unknown[] });
      return new Response("ok", { status: 200 });
    }) as unknown as typeof fetch);

    const response = await batcher.enqueue(buildPayload("m-solo"));

    expect(requests.length).toBe(1);
    expect(requests[0].messages.length).toBe(1);
    expect(response.ok).toBe(true);
  });

  test("resolves every member with the failure when the batch request fails", async () => {
    const batcher = new HttpSinkBatcher(target, 2, 10_000, (async () =>
      new Response("boom", { status: 502 })) as unknown as typeof fetch);

    const [first, second] = await Promise.all([
      batcher.enqueue(buildPayload("m-1")),
      batcher.enqueue(buildPayload("m-2")),
    ]);

    expect(first.ok).toBe(false);
    expect(first.error).toBe("HTTP 502");
    expect(second.ok).toBe(false);
  });
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd apps/router-workers && bun test src/__tests__/http-sink-batcher.test.ts`
Expected: FAIL — module `../http-sink-batcher` does not exist.

- [ ] **Step 3: Implement the batcher**

Create `apps/router-workers/src/http-sink-batcher.ts`:

```typescript
import type { DeliveryPayload, DeliveryResponse, HttpSinkTarget } from "./phase2-types";

interface PendingEntry {
  message: DeliveryPayload;
  resolve: (response: DeliveryResponse) => void;
}

// Accumulates HTTP sink deliveries and flushes them as a single
// `{ "messages": [...] }` POST when the batch fills or the interval elapses.
// Every member of a batch resolves with the batch outcome, so per-message
// ack/retry semantics are unchanged: a failed batch fails all members and
// each takes its own retry/DLQ path.
export class HttpSinkBatcher {
  private pending: PendingEntry[] = [];
  private flushTimer: ReturnType<typeof setTimeout> | undefined;

  public constructor(
    private readonly target: HttpSinkTarget,
    private readonly batchSize: number,
    private readonly flushIntervalMs: number,
    private readonly fetchImpl: typeof fetch,
  ) {}

  public enqueue(message: DeliveryPayload): Promise<DeliveryResponse> {
    return new Promise<DeliveryResponse>((resolve) => {
      this.pending.push({ message, resolve });

      if (this.pending.length >= this.batchSize) {
        void this.flush();
        return;
      }

      if (!this.flushTimer) {
        this.flushTimer = setTimeout(() => {
          void this.flush();
        }, this.flushIntervalMs);
      }
    });
  }

  public async flush(): Promise<void> {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = undefined;
    }

    const batch = this.pending;
    this.pending = [];
    if (batch.length === 0) {
      return;
    }

    const response = await this.postBatch(batch.map((entry) => entry.message));
    for (const entry of batch) {
      entry.resolve(response);
    }
  }

  private async postBatch(messages: DeliveryPayload[]): Promise<DeliveryResponse> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.target.timeoutMs ?? 2_500);

    try {
      const response = await this.fetchImpl(this.target.url, {
        method: this.target.method ?? "POST",
        headers: {
          "content-type": "application/json",
          ...this.target.headers,
        },
        body: JSON.stringify({ messages }),
        signal: controller.signal,
      });

      const body = await response.text();
      return {
        ok: response.ok,
        statusCode: response.status,
        body,
        error: response.ok ? undefined : `HTTP ${response.status}`,
      };
    } catch (error) {
      return {
        ok: false,
        error: error instanceof Error ? error.message : String(error),
      };
    } finally {
      clearTimeout(timeout);
    }
  }
}
```

- [ ] **Step 4: Run the batcher tests**

Run: `cd apps/router-workers && bun test src/__tests__/http-sink-batcher.test.ts`
Expected: PASS (3 tests).

- [ ] **Step 5: Write the failing integration test**

Add to `apps/router-workers/src/__tests__/phase2-worker.test.ts`:

```typescript
test("batches HTTP sink deliveries when batchingPolicy is enabled", async () => {
  const spec = buildHttpFlow();
  // Long interval on purpose: only the batch-size trigger may flush. If the
  // two ingests accidentally serialize, this test times out instead of
  // passing flakily with two single-message requests.
  spec.batchingPolicy = { enabled: true, batchSize: 2, flushIntervalMs: 10_000 };
  const deployment = buildDeployment(spec, {
    sinks: {
      http_out_default: { kind: "http", connectorId: "http_out_default", url: "https://example.test/webhook" },
    },
  });

  const requests: Array<{ messages: unknown[] }> = [];
  const runtime = new RouterWorkerRuntime(buildRuntimeConfig(), new StaticDeploymentSource([deployment]), {
    fetchImpl: (async (_input: RequestInfo | URL, init?: RequestInit) => {
      requests.push(JSON.parse(String(init?.body)) as { messages: unknown[] });
      return new Response("ok", { status: 200 });
    }) as unknown as typeof fetch,
  });
  await runtime.syncDeployments();

  const ingest = (messageId: string) =>
    runtime.ingestEnvelope({
      deploymentId: deployment.id,
      tenantId: spec.metadata.tenantId,
      flowId: spec.metadata.flowId,
      revisionId: spec.metadata.revisionId,
      messageId,
      sourceRef: "http://localhost/events",
      partitionKey: spec.metadata.tenantId,
      payload: { messageId },
      headers: {},
    });

  const [first, second] = await Promise.all([ingest("batch-1"), ingest("batch-2")]);

  expect(first.run.status).toBe("delivered");
  expect(second.run.status).toBe("delivered");
  expect(requests.length).toBe(1);
  expect(requests[0].messages.length).toBe(2);
  await runtime.stop();
});
```

Run: `cd apps/router-workers && bun test src/__tests__/phase2-worker.test.ts -t "batches HTTP sink"`
Expected: FAIL — two separate requests, and each body has no `messages` array.

- [ ] **Step 6: Wire batching into the runtime**

In `apps/router-workers/src/phase2-worker.ts`:

Import the batcher:

```typescript
import { HttpSinkBatcher } from "./http-sink-batcher";
```

Add a class field next to `processorState` (from Task 2):

```typescript
  private readonly httpSinkBatchers = new Map<string, HttpSinkBatcher>();
```

Add a private method near `processorStateFor`:

```typescript
  private httpBatcherFor(deployment: RouterDeployment, sink: SinkNode): HttpSinkBatcher | undefined {
    const policy = deployment.spec.batchingPolicy;
    if (!policy?.enabled || policy.batchSize <= 1) {
      return undefined;
    }
    if (sink.connector.executionMode !== "native") {
      return undefined;
    }

    const target = sinkTargetFor(deployment, sink);
    if (!target || target.kind !== "http") {
      return undefined;
    }

    const key = `${deployment.id}:${sink.id}`;
    let batcher = this.httpSinkBatchers.get(key);
    if (!batcher) {
      batcher = new HttpSinkBatcher(target, policy.batchSize, policy.flushIntervalMs ?? 250, this.fetchImpl);
      this.httpSinkBatchers.set(key, batcher);
    }
    return batcher;
  }
```

Extend `deliverSinkOnce` (line 601) with a trailing optional parameter:

```typescript
async function deliverSinkOnce(
  runId: string,
  deployment: RouterDeployment,
  sink: SinkNode,
  payload: unknown,
  envelope: CanonicalEnvelope,
  bus: MessageBus | undefined,
  fetchImpl: typeof fetch,
  attemptNumber: number,
  httpBatcher?: HttpSinkBatcher,
): Promise<DeliveryOutcome> {
```

and route the http branch through it (the ternary at lines 696-709 becomes):

```typescript
  const response =
    target.kind === "http"
      ? httpBatcher
        ? await httpBatcher.enqueue({
            envelope,
            payload,
            deploymentId: deployment.id,
            sinkId: sink.id,
            flowId: deployment.flowId,
            revisionId: deployment.revisionId,
          })
        : await deliverHttpSink(
            target,
            {
              envelope,
              payload,
              deploymentId: deployment.id,
              sinkId: sink.id,
              flowId: deployment.flowId,
              revisionId: deployment.revisionId,
            },
            fetchImpl,
          )
      : target.kind === "nats"
```

At the call site in the delivery loop (line 1645), pass the batcher:

```typescript
        const outcome = await deliverSinkOnce(
          runId,
          deployment,
          sink,
          delivery.payload,
          envelope,
          this.messageBus,
          this.fetchImpl,
          currentAttempt(envelope),
          this.httpBatcherFor(deployment, sink),
        );
```

In `stop()` (lines 1245-1271), flush and clear batchers before closing the bus:

```typescript
    for (const batcher of this.httpSinkBatchers.values()) {
      await batcher.flush().catch(() => undefined);
    }
    this.httpSinkBatchers.clear();
```

At the deployment-removal site (lines 1210-1212, extended in Task 2), also evict batchers:

```typescript
        for (const key of [...this.httpSinkBatchers.keys()]) {
          if (key.startsWith(`${deploymentId}:`)) {
            const batcher = this.httpSinkBatchers.get(key);
            this.httpSinkBatchers.delete(key);
            void batcher?.flush().catch(() => undefined);
          }
        }
```

- [ ] **Step 7: Run the full router suite**

Run: `cd apps/router-workers && bun test && bunx tsc --noEmit -p tsconfig.json`
Expected: All PASS. Existing flows have `batchingPolicy.enabled: false`, so every existing delivery test takes the unbatched path unchanged.

- [ ] **Step 8: Document the batched sink contract**

Add to `apps/router-workers/README.md`, at the end:

```markdown
## Batched HTTP sinks

When a flow sets `batchingPolicy: { enabled: true, batchSize: N }`, native HTTP
sinks receive `POST { "messages": [<delivery payload>, ...] }` instead of one
delivery payload per request. Batches flush when `batchSize` is reached or
after `flushIntervalMs` (default 250ms). A failed batch fails every member;
each message then follows its normal retry/DLQ path, so batched sinks must be
idempotent (which the retry policy already requires). Batching only takes
effect with `ROUTER_SUBSCRIPTION_CONCURRENCY > 1`; messages that share a
partition key are processed serially and will not batch together.
```

- [ ] **Step 9: Commit**

```bash
git add apps/router-workers/src/http-sink-batcher.ts apps/router-workers/src/__tests__/http-sink-batcher.test.ts apps/router-workers/src/phase2-worker.ts apps/router-workers/src/__tests__/phase2-worker.test.ts apps/router-workers/README.md
git commit -m "Honor batchingPolicy for native HTTP sinks with batched POST delivery

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Final verification

- [ ] Run everything:

```bash
cd apps/router-workers && bun test && bunx tsc --noEmit -p tsconfig.json
cd ../adapter-redpanda && bun test && bunx tsc -p tsconfig.json --noEmit
cd ../../packages/shared-flow-spec && bun test
bash -n deploy/nats/bootstrap.sh
```

Expected: all suites green, script parses.

## Out of scope (deliberately)

- Exponential backoff + jitter for retries (medium finding; the linear `retryBaseDelayMs * attempt` formula is kept — change is trivial later but touches retry-timing tests).
- Publishing adapter poison pills to `router.dlq.>` (needs a shared subject-builder contract; failures are already reported to control-api).
- Consumer-lag metrics via `jsm.consumers.info()`, replay rate limiting, migration off the deprecated `nats` v2 client — separate efforts.
