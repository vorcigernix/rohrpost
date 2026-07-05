let authMode: 'api-token' | 'oidc' = 'api-token';

export function setAuthMode(mode: 'api-token' | 'oidc') {
  authMode = mode;
}

export function isOidcAuthEnabled(): boolean {
  return authMode === 'oidc';
}
