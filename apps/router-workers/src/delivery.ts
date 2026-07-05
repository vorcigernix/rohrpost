import type { DeliveryAttempt, DeliveryPayload, DeliveryResponse, NatsSinkTarget, HttpSinkTarget, MessageBus } from "./phase2-types";
import { encodeJsonMessage } from "./nats";

function nowIso(): string {
  return new Date().toISOString();
}

export async function sleep(ms: number): Promise<void> {
  if (ms <= 0) return;
  await new Promise((resolve) => setTimeout(resolve, ms));
}

export async function deliverHttpSink(
  target: HttpSinkTarget,
  payload: DeliveryPayload,
  fetchImpl: typeof fetch = fetch,
): Promise<DeliveryResponse> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), target.timeoutMs ?? 2_500);

  try {
    const response = await fetchImpl(target.url, {
      method: target.method ?? "POST",
      headers: {
        "content-type": "application/json",
        ...target.headers,
      },
      body: JSON.stringify(payload),
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

export async function deliverNatsSink(
  bus: MessageBus,
  target: NatsSinkTarget,
  payload: DeliveryPayload,
): Promise<DeliveryResponse> {
  try {
    await bus.publish(target.subject, encodeJsonMessage(payload));
    return { ok: true };
  } catch (error) {
    return {
      ok: false,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

export async function retryWithBackoff<T>(
  attempt: (attemptNumber: number) => Promise<T>,
  options: {
    maxAttempts: number;
    baseDelayMs: number;
    onAttempt?: (attemptNumber: number, startedAt: string) => Promise<void> | void;
    onFailure?: (attemptNumber: number, error: string) => Promise<void> | void;
  },
): Promise<{ value?: T; attempts: number; error?: string }> {
  let lastError: string | undefined;

  for (let attemptNumber = 1; attemptNumber <= options.maxAttempts; attemptNumber += 1) {
    const startedAt = nowIso();
    await options.onAttempt?.(attemptNumber, startedAt);

    try {
      const value = await attempt(attemptNumber);
      return { value, attempts: attemptNumber };
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
      await options.onFailure?.(attemptNumber, lastError);
      if (attemptNumber < options.maxAttempts) {
        await sleep(options.baseDelayMs * attemptNumber);
      }
    }
  }

  return {
    attempts: options.maxAttempts,
    error: lastError ?? "unknown error",
  };
}

export function toDeliveryAttempt(
  sinkId: string,
  connectorId: string,
  executionMode: "native" | "adapter",
  attempt: number,
  startedAt: string,
  finishedAt: string,
  response: DeliveryResponse,
): DeliveryAttempt {
  return {
    sinkId,
    connectorId,
    executionMode,
    attempt,
    succeeded: response.ok,
    startedAt,
    finishedAt,
    statusCode: response.statusCode,
    error: response.error,
  };
}
