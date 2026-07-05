import type { FlowAuthoringService } from "../flows/service";
import type { AdapterRedpandaClient } from "../adapter-client";
import type { ControlApiConfig } from "../config";
import type { Repository } from "../repository";

export type ConsoleEventKind = "runtime" | "flows" | "connectors";

export interface ConsoleEventEnvelope {
  id: string;
  kind: ConsoleEventKind;
  at: string;
}

export interface ControlApiRouteDeps {
  config: ControlApiConfig;
  repository: Repository;
  flowAuthoring: FlowAuthoringService;
  adapterClient: AdapterRedpandaClient | null;
  requireAuth(request: Request): ReturnType<Repository["authenticate"]>;
  requireStreamAuth(request: Request): ReturnType<Repository["authenticate"]>;
  publishConsoleEvents(...kinds: ConsoleEventKind[]): void;
  onConsoleEvent(listener: (event: ConsoleEventEnvelope) => void): () => void;
  getConsoleEventVersion(): number;
  serializeAiSettings(
    settings: ReturnType<Repository["getAiProviderSettings"]>,
  ): Record<string, unknown>;
}
