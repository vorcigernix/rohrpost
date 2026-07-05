import {
  applySourceBindingToSpec,
  buildAutoSourceBinding,
  isDefaultSourceConnectorId,
  type JsonSourceBinding,
  type JsonSourceKind,
} from "@rohrpost/domain-connectors";
import {
  simulateFlowSpec,
  validateFlowSpec,
  type FlowSpec,
  type FlowSpecValidationResult,
} from "@rohrpost/shared-flow-spec";
import type { ControlApiConfig } from "../config";
import { buildDraftFromPrompt, type DraftResponse } from "./drafts";
import type {
  DeploymentRecord,
  FlowListItem,
  FlowRevisionRecord,
  ReplayRequestRecord,
  Repository,
} from "../repository";
import {
  buildJsonTransformAssistantResult,
  type JsonTransformDraftResponse,
} from "./transform-assistant";

export interface ComposeJsonTransformInput {
  prompt: string;
  samplePayload: unknown;
  tenantId?: string;
  name?: string;
  sourceKind?: JsonSourceKind;
  sinkCapabilityId?:
    | "http_out"
    | "nats_out"
    | "snowflake_sink"
    | "bigquery_sink"
    | "s3_sink"
    | "kafka_out";
  sinkConnectorId?: string;
}

export interface ValidateFlowInput {
  spec: FlowSpec;
  samplePayload?: unknown;
}

export interface SaveFlowInput {
  tenantId?: string;
  name?: string;
  samplePayload?: unknown;
  sourceBinding?: JsonSourceBinding;
  spec: FlowSpec;
}

export type SaveFlowResult =
  | {
      ok: true;
      revision: FlowRevisionRecord;
    }
  | {
      ok: false;
      issues: FlowSpecValidationResult["issues"];
    };

export interface FlowAuthoringService {
  listFlows(): FlowListItem[];
  composeJsonTransform(
    input: ComposeJsonTransformInput,
  ): Promise<JsonTransformDraftResponse & { sourceBinding: JsonSourceBinding }>;
  createDraftFromPrompt(input: {
    prompt: string;
    samplePayload?: unknown;
    tenantId?: string;
    name?: string;
  }): DraftResponse;
  validateFlow(input: ValidateFlowInput): {
    validation: FlowSpecValidationResult;
    simulation: ReturnType<typeof simulateFlowSpec>;
  };
  saveFlow(input: SaveFlowInput): SaveFlowResult;
  deleteFlow(flowId: string): { flowId: string; deleted: true } | null;
  publishFlow(
    flowId: string,
    revisionId?: string | null,
  ): { deployment: DeploymentRecord; revision: FlowRevisionRecord };
  rollbackDeployment(
    deploymentId: string,
    targetRevisionId?: string | null,
  ): { deployment: DeploymentRecord; revision: FlowRevisionRecord };
  createReplayRequest(input: {
    flowId: string;
    revisionId: string;
    reason: string;
    sourceStream: string;
  }): ReplayRequestRecord;
}

export interface FlowAuthoringDeps {
  config: ControlApiConfig;
  repository: Repository;
  fetchImpl?: typeof fetch;
}

export function createFlowAuthoringService(deps: FlowAuthoringDeps): FlowAuthoringService {
  function resolveDraftSourceKind(spec: FlowSpec): JsonSourceKind | null {
    const sourceKind = spec.sources[0]?.kind;
    return sourceKind === "http" || sourceKind === "nats" || sourceKind === "kafka"
      ? sourceKind
      : null;
  }

  function resolveSourceBinding(input: {
    tenantId: string;
    flowName: string;
    sourceKind: JsonSourceKind;
    binding?: JsonSourceBinding;
  }): JsonSourceBinding {
    if (input.binding) {
      return input.binding;
    }

    return buildAutoSourceBinding({
      sourceKind: input.sourceKind,
      flowName: input.flowName,
      existingConnectors: deps.repository.listConnectors({
        capabilityId:
          input.sourceKind === "http"
            ? "http_in"
            : input.sourceKind === "nats"
              ? "nats_in"
              : "kafka_in",
        tenantId: input.tenantId,
      }),
    });
  }

  function resolveAssistantConfig(): ControlApiConfig {
    const aiSettings = deps.repository.getAiProviderSettings();
    return {
      ...deps.config,
      geminiApiKey: aiSettings.enabled ? aiSettings.apiKey ?? undefined : undefined,
      geminiModel: aiSettings.model,
      geminiApiBaseUrl: aiSettings.apiBaseUrl,
    };
  }

  return {
    listFlows() {
      return deps.repository.listFlows();
    },
    async composeJsonTransform(input) {
      const tenantId = input.tenantId ?? deps.config.defaultTenantId;
      const result = await buildJsonTransformAssistantResult({
        prompt: input.prompt,
        samplePayload: input.samplePayload,
        sourceKind: input.sourceKind,
        sinkCapabilityId: input.sinkCapabilityId,
        sinkConnectorId: input.sinkConnectorId,
        tenantId,
        name: input.name,
        config: resolveAssistantConfig(),
        fetchImpl: deps.fetchImpl,
      });

      const sourceKind = input.sourceKind ?? "http";
      const sourceBinding = resolveSourceBinding({
        tenantId,
        flowName: input.name?.trim() || result.plan.suggestedName,
        sourceKind,
      });

      if (result.draft) {
        result.draft = applySourceBindingToSpec({ ...result.draft }, sourceBinding);
      }

      return {
        ...result,
        sourceBinding,
      };
    },
    createDraftFromPrompt(input) {
      return buildDraftFromPrompt({
        prompt: input.prompt,
        samplePayload: input.samplePayload,
        tenantId: input.tenantId ?? deps.config.defaultTenantId,
        name: input.name,
      });
    },
    validateFlow(input) {
      const validation = validateFlowSpec(input.spec);
      const simulation = simulateFlowSpec(input.spec, [
        {
          envelope: {},
          payload: input.samplePayload ?? { amount: 1, orderId: "sample-1" },
          sourceId: input.spec.sources[0]?.id,
        },
      ]);

      return { validation, simulation };
    },
    saveFlow(input) {
      let spec = structuredClone(input.spec);
      const tenantId = input.tenantId ?? spec.metadata.tenantId;
      const flowName = input.name ?? spec.metadata.name;
      const sourceKind = resolveDraftSourceKind(spec);
      const sourceConnectorId = spec.sources[0]?.connector.connectorId;
      const sourceCapabilityId = spec.sources[0]?.connector.capabilityId;
      const sourceConnectorExists = Boolean(
        sourceConnectorId
          && sourceCapabilityId
          && deps.repository.listConnectors({
            capabilityId: sourceCapabilityId,
            tenantId,
          }).some((connector) => connector.id === sourceConnectorId),
      );
      const shouldAutoBindSource = Boolean(
        sourceKind && (!sourceConnectorExists || isDefaultSourceConnectorId(sourceConnectorId)),
      );
      const resolvedSourceBinding =
        input.sourceBinding || (shouldAutoBindSource && sourceKind
          ? resolveSourceBinding({
              tenantId,
              flowName,
              sourceKind,
            })
          : null);

      if (resolvedSourceBinding) {
        deps.repository.saveConnector({
          id: resolvedSourceBinding.connectorId,
          tenantId,
          name: resolvedSourceBinding.connectorName,
          capabilityId: resolvedSourceBinding.capabilityId,
          executionMode: resolvedSourceBinding.executionMode,
          config: resolvedSourceBinding.config,
        });
        spec = applySourceBindingToSpec(spec, resolvedSourceBinding);
      }

      const validation = validateFlowSpec(spec);
      if (!validation.valid) {
        return {
          ok: false,
          issues: validation.issues,
        };
      }

      const simulation = simulateFlowSpec(spec, [
        {
          envelope: {},
          payload: input.samplePayload ?? { orderId: "created-flow-sample", amount: 250 },
          sourceId: spec.sources[0]?.id,
        },
      ]);

      return {
        ok: true,
        revision: deps.repository.createOrUpdateFlow({
          tenantId,
          name: flowName,
          spec,
          simulation,
          validation,
        }),
      };
    },
    deleteFlow(flowId) {
      return deps.repository.deleteFlow(flowId);
    },
    publishFlow(flowId, revisionId) {
      return deps.repository.publishFlow(flowId, revisionId);
    },
    rollbackDeployment(deploymentId, targetRevisionId) {
      return deps.repository.rollbackDeployment(deploymentId, targetRevisionId);
    },
    createReplayRequest(input) {
      return deps.repository.createReplayRequest(input);
    },
  };
}
