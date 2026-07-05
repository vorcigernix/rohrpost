import { apiBaseUrl, apiToken } from './api-config';
import { isOidcAuthEnabled } from './auth-state';

export const consoleApiConfig = {
  apiBaseUrl,
};

export async function requestJson<T>(path: string, init?: RequestInit): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`${apiBaseUrl}${path}`, {
      ...init,
      credentials: 'include',
      headers: {
        Accept: 'application/json',
        ...(!isOidcAuthEnabled() && apiToken ? { Authorization: `Bearer ${apiToken}` } : {}),
        'Content-Type': 'application/json',
        ...(init?.headers ?? {}),
      },
    });
  } catch (error) {
    const detail = error instanceof Error ? error.message : 'Network request failed.';
    throw new Error(`Request to ${path} could not reach ${apiBaseUrl || 'the configured API'}: ${detail}`);
  }

  if (!response.ok) {
    const detail = await response.text().catch(() => '');
    throw new Error(
      `Request to ${path} failed with status ${response.status}${detail ? `: ${detail}` : ''}`,
    );
  }

  return (await response.json()) as T;
}

export function buildEventStreamUrl(path: string): string {
  if (typeof window === 'undefined') {
    return path;
  }

  const url = new URL(path, apiBaseUrl || window.location.origin);
  if (!isOidcAuthEnabled() && apiToken) {
    url.searchParams.set('access_token', apiToken);
  }
  return url.toString();
}
