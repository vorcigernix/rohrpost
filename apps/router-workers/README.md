# router-workers

Native Bun/TypeScript runtime for the current Phase 2 execution path.

## Scripts

- `bun run dev` - run the local simulation CLI
- `bun test` - run worker tests
- `bun run check` - type-check the package

## Scope

This package now owns the worker-side execution path for native routing:

- JetStream subject handling for `ingress`, `work`, `retry`, `dlq`, `audit`, and `replay`
- deterministic FlowSpec compilation
- fixed processor execution
- native HTTP and NATS sink delivery
- adapter sink handoff into `router.work.>`
- run and audit summaries

The runtime still favors pragmatic local self-hosted execution over full production orchestration, but it is no longer just a simulation scaffold.

## Batched HTTP sinks

When a flow sets `batchingPolicy: { enabled: true, batchSize: N }`, native HTTP
sinks receive `POST { "messages": [<delivery payload>, ...] }` instead of one
delivery payload per request. Batches flush when `batchSize` is reached or
after `flushIntervalMs` (default 250ms). A failed batch fails every member;
each message then follows its normal retry/DLQ path, so batched sinks must be
idempotent (which the retry policy already requires). Batching engages
whenever deliveries overlap in time — via concurrent HTTP ingress or
`ROUTER_SUBSCRIPTION_CONCURRENCY > 1` on the consume path; messages that
share a partition key are processed serially and will not batch together, so
the effective batch size is bounded by the number of concurrent deliveries.
Two caveats: a batch fails or succeeds as a unit, so one permanently failing
message can drag up to `batchSize - 1` deliverable messages into the DLQ with
it after retries are exhausted, and the sink's `timeoutMs` applies to the
whole batch, so size it for a full batch body. `batchingPolicy.keyPath` is
declared in the FlowSpec type but not yet supported and is ignored.

## Consumer tuning

JetStream consumption is governed by three env vars that must be tuned
together: `ROUTER_CONSUMER_ACK_WAIT_MS` (default 30000),
`ROUTER_CONSUMER_MAX_ACK_PENDING` (default 64), and
`ROUTER_SUBSCRIPTION_CONCURRENCY` (default 16; per-partition-key ordering is
preserved at any concurrency). Safe operation requires
`ackWait >= (maxAckPending / concurrency) x worst-case per-message latency`,
where latency includes sink timeouts, retry not-before holds, and any batching
flush interval. Changing the ack settings recreates the durable consumers on
the next worker start (config drift is detected before subscribing).
