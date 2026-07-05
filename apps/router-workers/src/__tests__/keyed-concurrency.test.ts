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

  test("maxInflight=1 is strictly serial even under settle/submit races", async () => {
    const limiter = new KeyedConcurrencyLimiter(1);
    let running = 0;
    let maxRunning = 0;
    const work = async () => {
      running += 1;
      maxRunning = Math.max(maxRunning, running);
      await new Promise((resolve) => setTimeout(resolve, 1));
      running -= 1;
    };

    const tasks: Array<Promise<void>> = [];
    for (let index = 0; index < 12; index += 1) {
      tasks.push(limiter.run(`key-${index}`, work));
      if (index % 3 === 0) {
        await Promise.resolve();
      }
    }
    await Promise.all(tasks);
    expect(maxRunning).toBe(1);
  });

  test("global bound holds under mixed keys and interleaved settlement", async () => {
    const limiter = new KeyedConcurrencyLimiter(2);
    let running = 0;
    let maxRunning = 0;
    const work = async () => {
      running += 1;
      maxRunning = Math.max(maxRunning, running);
      await new Promise((resolve) => setTimeout(resolve, Math.random() < 0.5 ? 0 : 1));
      running -= 1;
    };

    const tasks: Array<Promise<void>> = [];
    for (let index = 0; index < 40; index += 1) {
      tasks.push(limiter.run(`key-${index % 7}`, work));
      if (index % 5 === 0) {
        await Promise.resolve();
      }
    }
    await Promise.all(tasks);
    expect(maxRunning).toBeLessThanOrEqual(2);
  });

  test("release/wake window does not admit a fresh acquire past the bound", async () => {
    const limiter = new KeyedConcurrencyLimiter(1);
    const gateA = deferred();
    const gateRest = deferred();
    let running = 0;
    let maxRunning = 0;
    const work = (gate: Promise<void>) => async () => {
      running += 1;
      maxRunning = Math.max(maxRunning, running);
      await gate;
      running -= 1;
    };

    const taskA = limiter.run("a", work(gateA.promise));
    const taskB = limiter.run("b", work(gateRest.promise));
    await settleMicrotasks();

    gateA.resolve();
    // One microtask hop lands run("c")'s acquire in the window between
    // release() decrementing inflight and waiter b incrementing it again.
    await Promise.resolve();
    const taskC = limiter.run("c", work(gateRest.promise));

    await settleMicrotasks();
    expect(maxRunning).toBe(1);

    gateRest.resolve();
    await Promise.all([taskA, taskB, taskC]);
    expect(maxRunning).toBe(1);
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
