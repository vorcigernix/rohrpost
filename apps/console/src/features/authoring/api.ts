import {
  controlApiPaths,
  type ControlApiDraftFlowResponse,
  type ControlApiFlowPublishResponse,
  type ControlApiFlowRevisionRecord,
  type ControlApiTransformComposerResponse,
} from '@rohrpost/control-api-contracts';
import { validateFlowSpec, type FlowSpec, type FlowSpecValidationResult } from '@rohrpost/shared-flow-spec';
import {
  type DraftFlowResponse,
  type TransformComposerResponse,
} from '../../lib/api-types';
import { requestJson } from '../../lib/api-base';
import { mapCapability } from '../capabilities/api';
import { mapBackendDraft, type BackendFlowSpec } from '../flows/api';

export async function composeJsonTransform(input: {
  prompt: string;
  samplePayload: unknown;
  sourceKind: 'http' | 'nats' | 'kafka';
  sinkCapabilityId?: string;
  sinkConnectorId?: string;
  name?: string;
  tenantId?: string;
}): Promise<TransformComposerResponse> {
  const data = await requestJson<ControlApiTransformComposerResponse>(controlApiPaths.flowComposeJsonTransform(), {
    method: 'POST',
    body: JSON.stringify({
      prompt: input.prompt,
      samplePayload: input.samplePayload,
      sourceKind: input.sourceKind,
      sinkCapabilityId: input.sinkCapabilityId,
      sinkConnectorId: input.sinkConnectorId,
      name: input.name,
      tenantId: input.tenantId,
    }),
  });

  return {
    assistant: data.assistant,
    plan: {
      suggestedName: data.plan.suggestedName,
      summary: data.plan.summary,
      fieldMappings: data.plan.fieldMappings,
      filterSummary: data.plan.filterSummary,
      explanation: data.plan.explanation,
      recommendedExportIds: data.plan.recommendedSinkCapabilityIds,
    },
    preview: data.preview,
    exportOptions: data.exportOptions.map(mapCapability),
    sourceBinding: data.sourceBinding,
    draft: data.draft ? mapBackendDraft(data.draft, input.samplePayload) : undefined,
  };
}

export async function draftFlowFromPrompt(
  prompt: string,
  options?: { name?: string; samplePayload?: unknown; tenantId?: string },
): Promise<DraftFlowResponse> {
  const data = await requestJson<ControlApiDraftFlowResponse>(controlApiPaths.flowDraftFromPrompt(), {
    method: 'POST',
    body: JSON.stringify({
      prompt,
      name: options?.name,
      samplePayload: options?.samplePayload,
      tenantId: options?.tenantId,
    }),
  });
  return mapBackendDraft(data.draft, options?.samplePayload ?? { prompt });
}

export async function validateConsoleFlowSpec(spec: FlowSpec): Promise<FlowSpecValidationResult> {
  return validateFlowSpec(spec);
}

export async function publishDraft(input: {
  draft: DraftFlowResponse;
  name?: string;
  tenantId?: string;
  samplePayload?: unknown;
  sourceBinding?: TransformComposerResponse['sourceBinding'];
}): Promise<{ flowId: string; revisionId: string; deploymentId: string }> {
  const backendSpec = input.draft.backendSpec as BackendFlowSpec | undefined;
  if (!backendSpec?.metadata?.flowId) {
    throw new Error('The live API draft is missing backend metadata and cannot be published.');
  }

  const createResponse = await requestJson<ControlApiFlowRevisionRecord>(controlApiPaths.flows(), {
    method: 'POST',
    body: JSON.stringify({
      tenantId: input.tenantId ?? backendSpec.metadata.tenantId,
      name: input.name ?? backendSpec.metadata.name,
      samplePayload: input.samplePayload,
      sourceBinding: input.sourceBinding,
      spec: backendSpec,
    }),
  });

  const publishResponse = await requestJson<ControlApiFlowPublishResponse>(
    controlApiPaths.flowPublish(backendSpec.metadata.flowId),
    {
      method: 'POST',
      body: JSON.stringify({
        revisionId: createResponse.id,
      }),
    },
  );

  return {
    flowId: backendSpec.metadata.flowId,
    revisionId: createResponse.id,
    deploymentId: publishResponse.deployment.id,
  };
}
