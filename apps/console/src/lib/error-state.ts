import { consoleApiConfig } from './api';

export function describeConsoleError(error: unknown): {
  message: string;
  hint: string;
} {
  const message =
    error instanceof Error ? error.message : 'The console client failed to load data.';

  return {
    message,
    hint: `The console is using ${consoleApiConfig.apiBaseUrl || 'the configured API'}. Check \`VITE_API_BASE_URL\`, \`VITE_API_TOKEN\`, and the backend status endpoints.`,
  };
}
