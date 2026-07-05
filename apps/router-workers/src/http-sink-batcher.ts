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
