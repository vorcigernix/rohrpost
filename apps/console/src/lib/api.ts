import {
  composeJsonTransform,
  draftFlowFromPrompt,
  publishDraft,
  validateConsoleFlowSpec,
} from '../features/authoring/api';
import { fetchCapabilities } from '../features/capabilities/api';
import { fetchConnectors, saveConnector } from '../features/catalog/api';
import { deleteFlow, fetchFlows, publishBackendFlowSpec } from '../features/flows/api';
import { fetchOverview } from '../features/overview/api';
import {
  fetchAdapterWorkloads,
  fetchRuns,
  fetchRuntimeSamples,
  fetchRuntimeStats,
  subscribeToConsoleEvents,
} from '../features/runtime/api';
import { fetchAiSettings, fetchOidcSettings, saveAiSettings, signOutOidc } from '../features/setup/api';

export type {
  AdapterWorkloadRecord,
  AiProviderSettings,
  ConnectorRecord,
  TransformComposerResponse,
} from './api-types';
export { consoleApiConfig } from './api-base';
export type { ConsoleEventMessage } from '../features/runtime/api';

export const api = {
  fetchOverview,
  fetchFlows,
  deleteFlow,
  publishBackendFlowSpec,
  fetchRuntimeStats,
  fetchAdapterWorkloads,
  fetchRuntimeSamples,
  fetchRuns,
  fetchCapabilities,
  fetchAiSettings,
  fetchOidcSettings,
  saveAiSettings,
  signOutOidc,
  fetchConnectors,
  saveConnector,
  composeJsonTransform,
  draftFlowFromPrompt,
  validateFlowSpec: validateConsoleFlowSpec,
  publishDraft,
  subscribeToConsoleEvents,
};
