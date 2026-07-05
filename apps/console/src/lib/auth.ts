import {
  controlApiPaths,
  type ControlApiAuthSession,
  type ControlApiOidcConfig,
} from '@rohrpost/control-api-contracts';
import { apiBaseUrl } from './api-config';
import { setAuthMode } from './auth-state';

const verifierKey = 'rohrpost_oidc_verifier';
const stateKey = 'rohrpost_oidc_state';
const returnToKey = 'rohrpost_oidc_return_to';

function authUrl(path: string): string {
  return `${apiBaseUrl}${path}`;
}

function randomString(): string {
  const bytes = new Uint8Array(32);
  crypto.getRandomValues(bytes);
  return btoa(String.fromCharCode(...bytes)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

async function codeChallenge(verifier: string): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(verifier));
  return btoa(String.fromCharCode(...new Uint8Array(digest))).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

async function publicJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(authUrl(path), {
    ...init,
    credentials: 'include',
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json',
      ...(init?.headers ?? {}),
    },
  });
  if (!response.ok) {
    throw new Error(await response.text().catch(() => 'Authentication request failed.'));
  }
  return (await response.json()) as T;
}

export async function fetchOidcConfig(): Promise<ControlApiOidcConfig> {
  const config = await publicJson<ControlApiOidcConfig>(controlApiPaths.oidcConfig());
  setAuthMode(config.enabled ? 'oidc' : 'api-token');
  return config;
}

export async function fetchAuthSession(): Promise<ControlApiAuthSession> {
  return publicJson<ControlApiAuthSession>(controlApiPaths.authSession());
}

export async function startOidcLogin(config: ControlApiOidcConfig): Promise<void> {
  if (!config.authorizationEndpoint || !config.clientId) {
    throw new Error('OIDC provider is missing an authorization endpoint or client ID.');
  }
  const verifier = randomString();
  const state = randomString();
  const returnTo = `${window.location.pathname}${window.location.search}${window.location.hash}`;
  sessionStorage.setItem(verifierKey, verifier);
  sessionStorage.setItem(stateKey, state);
  sessionStorage.setItem(returnToKey, returnTo === '/auth/callback' ? '/' : returnTo);

  const url = new URL(config.authorizationEndpoint);
  url.searchParams.set('response_type', 'code');
  url.searchParams.set('client_id', config.clientId);
  url.searchParams.set('redirect_uri', `${window.location.origin}/auth/callback`);
  url.searchParams.set('scope', config.scope ?? 'openid profile email');
  url.searchParams.set('state', state);
  url.searchParams.set('code_challenge_method', 'S256');
  url.searchParams.set('code_challenge', await codeChallenge(verifier));
  window.location.assign(url.toString());
}

export async function completeOidcLogin(): Promise<string> {
  const params = new URLSearchParams(window.location.search);
  const code = params.get('code');
  const state = params.get('state');
  const expectedState = sessionStorage.getItem(stateKey);
  const verifier = sessionStorage.getItem(verifierKey);
  if (!code || !state || !expectedState || !verifier || state !== expectedState) {
    throw new Error('OIDC login response could not be verified.');
  }

  await publicJson(controlApiPaths.oidcToken(), {
    method: 'POST',
    body: JSON.stringify({
      code,
      codeVerifier: verifier,
      redirectUri: `${window.location.origin}/auth/callback`,
    }),
  });

  sessionStorage.removeItem(verifierKey);
  sessionStorage.removeItem(stateKey);
  const returnTo = sessionStorage.getItem(returnToKey) ?? '/';
  sessionStorage.removeItem(returnToKey);
  return returnTo;
}

export async function logoutOidc(): Promise<void> {
  await publicJson(controlApiPaths.authLogout(), { method: 'POST' });
}
