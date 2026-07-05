interface AdapterConnectorTestResponse {
  ok: boolean;
  service: string;
  connectorId?: string;
  executionMode: "adapter";
  requestedDirection: "source" | "sink" | "bidirectional";
  message: string;
  runtime?: Record<string, unknown>;
  resolvedManifest?: Record<string, unknown> | null;
}

export class AdapterRedpandaClient {
  public constructor(
    private readonly baseUrl: string,
    private readonly fetchImpl: typeof fetch = fetch,
  ) {}

  private async requestJson<T>(path: string, init?: RequestInit): Promise<T> {
    const response = await this.fetchImpl(`${this.baseUrl.replace(/\/$/, "")}${path}`, {
      ...init,
      headers: {
        Accept: "application/json",
        ...(init?.body ? { "Content-Type": "application/json" } : {}),
        ...(init?.headers ?? {}),
      },
    });

    if (!response.ok) {
      throw new Error(`adapter-redpanda ${path} failed with status ${response.status}`);
    }

    return (await response.json()) as T;
  }

  public async testConnector(input: {
    capabilityId: string;
    direction: "source" | "sink" | "bidirectional";
  }): Promise<AdapterConnectorTestResponse> {
    return this.requestJson<AdapterConnectorTestResponse>("/connectors/test", {
      method: "POST",
      body: JSON.stringify(input),
    });
  }
}
