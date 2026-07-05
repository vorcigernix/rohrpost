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

    // release() hands the slot to us directly — inflight is not decremented
    // on handoff, so a concurrent fresh acquire cannot slip into the window.
    await new Promise<void>((resolve) => {
      this.waiters.push(resolve);
    });
  }

  private release(): void {
    const next = this.waiters.shift();
    if (next) {
      next();
      return;
    }
    this.inflight -= 1;
  }
}
