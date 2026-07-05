import { describe, expect, test } from "bun:test";
import { consumeJetStreamMessages, type JetStreamLoopMessage } from "../nats";

const textEncoder = new TextEncoder();

function buildMessage(
  subject: string,
  seq: number,
  payload: unknown,
  log: string[],
): JetStreamLoopMessage {
  return {
    data: textEncoder.encode(JSON.stringify(payload)),
    subject,
    seq,
    ack() {
      log.push(`ack:${seq}`);
    },
    nak() {
      log.push(`nak:${seq}`);
    },
  };
}

async function* feed(messages: JetStreamLoopMessage[]): AsyncGenerator<JetStreamLoopMessage> {
  for (const message of messages) {
    yield message;
  }
}

describe("consumeJetStreamMessages", () => {
  test("acks successes, naks failures, and keeps consuming after a failure", async () => {
    const log: string[] = [];
    const messages = [1, 2, 3].map((seq) => buildMessage("s", seq, { seq }, log));

    const consumer = consumeJetStreamMessages(
      feed(messages),
      async (data) => {
        const { seq } = JSON.parse(new TextDecoder().decode(data)) as { seq: number };
        if (seq === 2) {
          throw new Error("boom");
        }
      },
      { concurrency: 1 },
    );

    await consumer.done;
    await consumer.drain();
    expect(log.sort()).toEqual(["ack:1", "ack:3", "nak:2"]);
  });

  test("serializes same-partition-key messages in order at high concurrency", async () => {
    const log: string[] = [];
    const order: number[] = [];
    const messages = [1, 2, 3, 4].map((seq) =>
      buildMessage("s", seq, { seq, key: seq % 2 === 0 ? "even" : "odd" }, log),
    );

    const consumer = consumeJetStreamMessages(
      feed(messages),
      async (data) => {
        const { seq } = JSON.parse(new TextDecoder().decode(data)) as { seq: number };
        // Reverse-biased delay: without keying, later messages would finish first.
        await new Promise((resolve) => setTimeout(resolve, seq === 1 || seq === 2 ? 10 : 1));
        order.push(seq);
      },
      {
        concurrency: 8,
        partitionKey: (data) => (JSON.parse(new TextDecoder().decode(data)) as { key: string }).key,
      },
    );

    await consumer.done;
    await consumer.drain();
    expect(order.indexOf(1)).toBeLessThan(order.indexOf(3));
    expect(order.indexOf(2)).toBeLessThan(order.indexOf(4));
    expect(log.filter((entry) => entry.startsWith("ack")).length).toBe(4);
  });

  test("falls back to subject ordering when the key extractor throws", async () => {
    const log: string[] = [];
    const messages = [1, 2].map((seq) => buildMessage("same-subject", seq, { seq }, log));
    const order: number[] = [];

    const consumer = consumeJetStreamMessages(
      feed(messages),
      async (data) => {
        const { seq } = JSON.parse(new TextDecoder().decode(data)) as { seq: number };
        await new Promise((resolve) => setTimeout(resolve, seq === 1 ? 10 : 1));
        order.push(seq);
      },
      {
        concurrency: 8,
        partitionKey: () => {
          throw new Error("no key");
        },
      },
    );

    await consumer.done;
    await consumer.drain();
    expect(order).toEqual([1, 2]);
    expect(log.length).toBe(2);
  });

  test("drain waits for in-flight handlers before resolving", async () => {
    const log: string[] = [];
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const messages = [1, 2].map((seq) => buildMessage("s", seq, { seq }, log));

    const consumer = consumeJetStreamMessages(
      feed(messages),
      async () => {
        await gate;
      },
      { concurrency: 2 },
    );

    await consumer.done;
    expect(log).toEqual([]);

    const drained = consumer.drain();
    release();
    await drained;
    expect(log.sort()).toEqual(["ack:1", "ack:2"]);
  });
});
