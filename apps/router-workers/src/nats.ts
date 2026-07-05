import {
  connect,
  createInbox,
  StringCodec,
  consumerOpts,
  type NatsConnection,
  type Subscription,
} from "nats";
import type { MessageBus, NatsRuntime } from "./phase2-types";
import { resolveJetStreamStream } from "./jetstream";
import { KeyedConcurrencyLimiter } from "./keyed-concurrency";

export interface JetStreamLoopMessage {
  data: Uint8Array;
  subject: string;
  seq: number;
  ack(): void;
  nak(): void;
}

export function consumeJetStreamMessages(
  messages: AsyncIterable<JetStreamLoopMessage>,
  handler: (data: Uint8Array, metadata: { subject: string; sequence?: number }) => Promise<void>,
  options: { concurrency?: number; partitionKey?: (data: Uint8Array) => string },
): { done: Promise<void>; drain(): Promise<void> } {
  const concurrency = Math.max(1, options.concurrency ?? 1);
  const limiter = new KeyedConcurrencyLimiter(concurrency);
  const inflightTasks = new Set<Promise<void>>();

  const done = (async () => {
    for await (const message of messages) {
      // Bound local queue growth: never hold more than 2x concurrency
      // undispatched-or-running tasks in memory.
      while (inflightTasks.size >= concurrency * 2) {
        await Promise.race(inflightTasks);
      }

      let key = message.subject;
      if (options.partitionKey) {
        try {
          key = options.partitionKey(message.data);
        } catch {
          // Fall back to subject-scoped ordering when no key can be derived.
        }
      }

      const task = limiter
        .run(key, () =>
          handler(message.data, {
            subject: message.subject,
            sequence: message.seq,
          }),
        )
        .then(() => {
          try {
            message.ack();
          } catch {
            // The server may already have closed this delivery during shutdown.
          }
        })
        .catch(() => {
          // Nak so the message redelivers, but keep consuming — a single
          // handler failure must not wedge the durable subscription.
          try {
            message.nak();
          } catch {
            // The server may already have closed this delivery during shutdown.
          }
        });

      inflightTasks.add(task);
      void task.finally(() => {
        inflightTasks.delete(task);
      });
    }
  })();

  void done.catch(() => undefined);

  return {
    done,
    async drain() {
      await Promise.allSettled([...inflightTasks]);
    },
  };
}

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();
const stringCodec = StringCodec();

type NatsPublishErrorResponse = {
  status: number;
  body: {
    error: string;
    message: string;
    code?: string;
    apiError?: {
      code?: number;
      errCode?: number;
      description?: string;
    };
  };
};

type NatsLikeError = Error & {
  code?: string;
  api_error?: {
    code?: unknown;
    err_code?: unknown;
    description?: unknown;
  };
};

type JetStreamConsumerLike = {
  name?: string;
  push_bound?: boolean;
  config?: {
    filter_subject?: string;
    ack_wait?: number;
    max_ack_pending?: number;
  };
};

function readApiError(error: unknown):
  | {
      code?: number;
      errCode?: number;
      description?: string;
    }
  | undefined {
  if (!error || typeof error !== "object" || !("api_error" in error)) {
    return undefined;
  }

  const apiError = (error as { api_error?: { code?: unknown; err_code?: unknown; description?: unknown } }).api_error;
  if (!apiError || typeof apiError !== "object") {
    return undefined;
  }

  return {
    code: typeof apiError.code === "number" ? apiError.code : undefined,
    errCode: typeof apiError.err_code === "number" ? apiError.err_code : undefined,
    description: typeof apiError.description === "string" ? apiError.description : undefined,
  };
}

function isJetStreamCapacityError(apiError: ReturnType<typeof readApiError>): boolean {
  if (!apiError) {
    return false;
  }

  if (apiError.code === 503 && apiError.errCode === 10023) {
    return true;
  }

  const description = apiError.description?.toLowerCase() ?? "";
  return apiError.code === 503 && (
    description.includes("insufficient resources")
    || description.includes("maximum bytes")
    || description.includes("maximum messages")
    || description.includes("maximum msgs")
    || description.includes("stream limit")
  );
}

export function classifyNatsPublishError(error: unknown): NatsPublishErrorResponse | null {
  if (!(error instanceof Error)) {
    return null;
  }

  const natsError = error as NatsLikeError;
  if (typeof natsError.code !== "string") {
    return null;
  }

  const apiError = readApiError(natsError);
  if (isJetStreamCapacityError(apiError)) {
    return {
      status: 503,
      body: {
        error: "jetstream_backpressure",
        message: "JetStream rejected the publish because durable stream capacity is exhausted.",
        code: natsError.code,
        apiError,
      },
    };
  }

  if (apiError) {
    return {
      status: 502,
      body: {
        error: "jetstream_publish_failed",
        message: apiError.description ?? natsError.message,
        code: natsError.code,
        apiError,
      },
    };
  }

  return {
    status: 502,
    body: {
      error: "nats_publish_failed",
      message: natsError.message,
      code: natsError.code,
    },
  };
}

function sanitizeConsumerName(name: string): string {
  return name.replace(/[^a-zA-Z0-9_-]/g, "_");
}

function readPositiveIntegerEnv(name: string, fallback: number, min: number, max: number): number {
  const parsed = Number(process.env[name]);
  if (!Number.isInteger(parsed)) {
    return fallback;
  }
  return Math.min(Math.max(parsed, min), max);
}

export const ROUTER_ACK_WAIT_MS = readPositiveIntegerEnv("ROUTER_CONSUMER_ACK_WAIT_MS", 30_000, 1_000, 300_000);
export const ROUTER_MAX_ACK_PENDING = readPositiveIntegerEnv("ROUTER_CONSUMER_MAX_ACK_PENDING", 64, 1, 10_000);
const ROUTER_ACK_WAIT_NS = ROUTER_ACK_WAIT_MS * 1_000_000;

// Safe operation requires ackWait >= (maxAckPending / concurrency) x worst-case
// per-message latency, where latency includes sink timeouts, retry not-before
// holds, and any batching flush interval; retune these together with
// ROUTER_SUBSCRIPTION_CONCURRENCY, not independently. The budget also assumes
// roughly uniform partition keys — a hot key serializes its messages and can
// exceed ackWait under slow sinks, producing (contract-legal) redeliveries.
export function isConsumerConfigDrifted(config?: { ack_wait?: number; max_ack_pending?: number }): boolean {
  return config?.ack_wait !== ROUTER_ACK_WAIT_NS || config?.max_ack_pending !== ROUTER_MAX_ACK_PENDING;
}

export function isConsumerConflictError(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false;
  }
  const apiError = readApiError(error);
  if (apiError?.errCode === 10100) {
    return true;
  }
  return /duplicate subscription|consumer already exists|already bound/i.test(error.message);
}

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

    return isConsumerConfigDrifted(consumer.config) ? [consumer.name] : [];
  });
}

export function createJetStreamConsumerOptions(durableName: string) {
  const opts = consumerOpts();
  opts.deliverTo(createInbox());
  opts.durable(sanitizeConsumerName(durableName));
  opts.manualAck();
  // Router-managed JetStream streams use WorkQueue retention, so new
  // consumers must attach with deliverAll and the broker drops messages
  // once they are acknowledged.
  opts.deliverAll();
  opts.ackExplicit();
  opts.ackWait(ROUTER_ACK_WAIT_MS);
  opts.maxAckPending(ROUTER_MAX_ACK_PENDING);
  return opts;
}

class NatsMessageBus implements MessageBus {
  public constructor(
    private readonly nc: NatsConnection,
    private readonly js: ReturnType<NatsConnection["jetstream"]>,
    private readonly jsm: Awaited<ReturnType<NatsConnection["jetstreamManager"]>>,
  ) {}

  private async listJetStreamConsumers(stream: string): Promise<JetStreamConsumerLike[]> {
    const lister = this.jsm.consumers.list(stream);
    const consumers: JetStreamConsumerLike[] = [];

    for await (const consumer of lister) {
      consumers.push(consumer as JetStreamConsumerLike);
    }

    return consumers;
  }

  private async deleteStaleJetStreamConsumers(subject: string, durableName: string): Promise<boolean> {
    const stream = resolveJetStreamStream(subject);
    if (!stream) {
      return false;
    }

    const consumers = await this.listJetStreamConsumers(stream);
    const staleConsumers = selectStaleJetStreamConsumers(consumers, subject, durableName);
    if (staleConsumers.length === 0) {
      return false;
    }

    for (const consumerName of staleConsumers) {
      await this.jsm.consumers.delete(stream, consumerName);
    }

    return true;
  }

  private async recreateDriftedConsumer(subject: string, durableName: string): Promise<void> {
    const stream = resolveJetStreamStream(subject);
    if (!stream) {
      return;
    }

    let info: JetStreamConsumerLike | undefined;
    try {
      info = (await this.jsm.consumers.info(stream, sanitizeConsumerName(durableName))) as JetStreamConsumerLike;
    } catch {
      // Missing consumer (first deploy) or transient lookup failure — either
      // way js.subscribe will create or attach next; nothing to recreate.
      return;
    }

    if (info.push_bound || !isConsumerConfigDrifted(info.config)) {
      return;
    }

    try {
      await this.jsm.consumers.delete(stream, sanitizeConsumerName(durableName));
    } catch {
      // A peer instance may have deleted or rebound it concurrently.
    }
  }

  public async publish(subject: string, data: Uint8Array): Promise<void> {
    this.nc.publish(subject, data);
    await this.nc.flush();
  }

  public async publishToJetStream(subject: string, data: Uint8Array): Promise<void> {
    await this.js.publish(subject, data);
  }

  public async subscribe(
    subject: string,
    handler: (data: Uint8Array, metadata: { subject: string }) => Promise<void>,
  ): Promise<{ unsubscribe(): Promise<void> }> {
    const subscription: Subscription = this.nc.subscribe(subject);
    const loop = (async () => {
      for await (const message of subscription) {
        await handler(message.data, { subject: message.subject });
      }
    })();

    void loop.catch(() => undefined);

    return {
      async unsubscribe() {
        subscription.unsubscribe();
      },
    };
  }

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

    // js.subscribe silently attaches to an existing same-name durable and
    // adopts its old config, so drift must be handled before subscribing.
    await this.recreateDriftedConsumer(subject, options.durableName);

    let subscription: Awaited<ReturnType<typeof this.js.subscribe>>;
    try {
      subscription = await this.js.subscribe(subject, opts);
    } catch (error) {
      if (!isConsumerConflictError(error)) {
        throw error;
      }

      let removed = false;
      try {
        removed = await this.deleteStaleJetStreamConsumers(subject, options.durableName);
      } catch {
        // Surface the original subscribe failure, not the cleanup failure.
      }
      if (!removed) {
        throw error;
      }

      subscription = await this.js.subscribe(subject, opts);
    }

    const consumer = consumeJetStreamMessages(
      subscription as unknown as AsyncIterable<JetStreamLoopMessage>,
      handler,
      options,
    );

    return {
      async unsubscribe() {
        subscription.unsubscribe();
        await consumer.drain();
      },
    };
  }

  public async close(): Promise<void> {
    await this.nc.drain();
    await this.nc.close();
  }
}

export async function createNatsRuntime(url: string, serviceName: string): Promise<NatsRuntime> {
  const nc = await connect({ servers: url, name: serviceName });
  const js = nc.jetstream();
  const jsm = await nc.jetstreamManager();

  return {
    bus: new NatsMessageBus(nc, js, jsm),
    ready: Promise.resolve(),
  };
}

export function encodeBusMessage(value: unknown): Uint8Array {
  if (typeof value === "string") {
    return stringCodec.encode(value);
  }

  return textEncoder.encode(JSON.stringify(value));
}

export function decodeBusMessage(data: Uint8Array): string {
  return textDecoder.decode(data);
}

export function encodeJsonMessage(value: unknown): Uint8Array {
  return textEncoder.encode(JSON.stringify(value));
}

export function decodeJsonMessage<T>(data: Uint8Array): T {
  return JSON.parse(textDecoder.decode(data)) as T;
}
