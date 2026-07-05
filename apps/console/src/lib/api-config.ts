const viteEnv = (import.meta as ImportMeta & {
  env?: Record<string, string> & { DEV?: boolean };
}).env;

export const apiToken = viteEnv?.VITE_API_TOKEN ?? 'dev-admin-token';

function resolveDefaultApiBaseUrl(): string {
  const configured = viteEnv?.VITE_API_BASE_URL;
  if (configured) {
    return configured;
  }

  if (typeof window === 'undefined') {
    return '';
  }

  const { protocol, hostname } = window.location;
  if ((hostname === '127.0.0.1' || hostname === 'localhost') && viteEnv?.DEV) {
    return `${protocol}//${hostname}:3001`;
  }

  return '';
}

export const apiBaseUrl = resolveDefaultApiBaseUrl();
