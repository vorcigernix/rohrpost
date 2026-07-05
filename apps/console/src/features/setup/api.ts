import {
  controlApiPaths,
  type ControlApiAiProviderSettings,
  type ControlApiOidcConfig,
} from '@rohrpost/control-api-contracts';
import type { AiProviderSettings } from '../../lib/api-types';
import { requestJson } from '../../lib/api-base';
import { fetchOidcConfig as fetchPublicOidcConfig, logoutOidc } from '../../lib/auth';

export function fetchAiSettings(): Promise<AiProviderSettings> {
  return requestJson<ControlApiAiProviderSettings>(controlApiPaths.aiSettings());
}

export function fetchOidcSettings(): Promise<ControlApiOidcConfig> {
  return fetchPublicOidcConfig();
}

export function signOutOidc(): Promise<void> {
  return logoutOidc();
}

export function saveAiSettings(input: {
  enabled: boolean;
  apiKey?: string;
  clearApiKey?: boolean;
  model: string;
  apiBaseUrl: string;
}): Promise<AiProviderSettings> {
  return requestJson<ControlApiAiProviderSettings>(controlApiPaths.aiSettings(), {
    method: 'POST',
    body: JSON.stringify({
      provider: 'gemini',
      enabled: input.enabled,
      apiKey: input.apiKey,
      clearApiKey: input.clearApiKey,
      model: input.model,
      apiBaseUrl: input.apiBaseUrl,
    }),
  });
}
