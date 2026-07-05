import { createHmac, timingSafeEqual } from "node:crypto";
import { createRemoteJWKSet, jwtVerify, type JWTPayload } from "jose";
import type { ControlApiConfig } from "./config";
import type { ApiTokenRecord } from "./repository/types";

export interface OidcDiscovery {
  issuer: string;
  authorization_endpoint: string;
  token_endpoint: string;
  jwks_uri: string;
}

export interface OidcSession {
  sub: string;
  email?: string;
  name?: string;
  exp: number;
}

export interface OidcTokenInput {
  code: string;
  codeVerifier: string;
  redirectUri: string;
}

const encoder = new TextEncoder();

function base64Url(input: string | Uint8Array): string {
  const bytes = typeof input === "string" ? encoder.encode(input) : input;
  return Buffer.from(bytes).toString("base64url");
}

function sign(value: string, secret: string): string {
  return createHmac("sha256", secret).update(value).digest("base64url");
}

function cookieValue(request: Request, name: string): string | null {
  const header = request.headers.get("cookie");
  if (!header) return null;
  for (const part of header.split(";")) {
    const [key, ...value] = part.trim().split("=");
    if (key === name) return value.join("=");
  }
  return null;
}

function authRecord(session: OidcSession): ApiTokenRecord {
  return {
    userId: `oidc:${session.sub}`,
    label: session.email ?? session.name ?? session.sub,
  };
}

export function createOidcAuth(config: ControlApiConfig, fetchImpl: typeof fetch = fetch) {
  const oidc = config.oidc;
  let discoveryPromise: Promise<OidcDiscovery> | null = null;
  let jwks: ReturnType<typeof createRemoteJWKSet> | null = null;

  async function discovery(): Promise<OidcDiscovery> {
    if (!oidc) throw new Error("OIDC is not configured.");
    discoveryPromise ??= fetchImpl(`${oidc.issuerUrl}/.well-known/openid-configuration`)
      .then(async (response) => {
        if (!response.ok) throw new Error(`OIDC discovery failed with ${response.status}.`);
        const body = await response.json() as Partial<OidcDiscovery>;
        if (!body.authorization_endpoint || !body.token_endpoint || !body.jwks_uri) {
          throw new Error("OIDC discovery response is missing required endpoints.");
        }
        return {
          issuer: body.issuer ?? oidc.issuerUrl,
          authorization_endpoint: body.authorization_endpoint,
          token_endpoint: body.token_endpoint,
          jwks_uri: body.jwks_uri,
        };
      });
    return discoveryPromise;
  }

  async function verifyIdToken(idToken: string): Promise<JWTPayload> {
    if (!oidc) throw new Error("OIDC is not configured.");
    const metadata = await discovery();
    jwks ??= createRemoteJWKSet(new URL(metadata.jwks_uri));
    const result = await jwtVerify(idToken, jwks, {
      issuer: metadata.issuer,
      audience: oidc.clientId,
    });
    return result.payload;
  }

  function setCookie(session: OidcSession, request: Request): string {
    if (!oidc) throw new Error("OIDC is not configured.");
    const payload = base64Url(JSON.stringify(session));
    const signature = sign(payload, oidc.sessionSecret);
    const maxAge = Math.max(0, session.exp - Math.floor(Date.now() / 1000));
    const secure = new URL(request.url).protocol === "https:" ? "; Secure" : "";
    return `${oidc.sessionCookieName}=${payload}.${signature}; HttpOnly; Path=/; SameSite=Lax; Max-Age=${maxAge}${secure}`;
  }

  return {
    isEnabled() {
      return Boolean(oidc);
    },

    async publicConfig() {
      if (!oidc) return { enabled: false, loginRequired: false as const };
      const metadata = await discovery();
      return {
        enabled: true,
        loginRequired: true as const,
        issuerUrl: oidc.issuerUrl,
        clientId: oidc.clientId,
        authorizationEndpoint: metadata.authorization_endpoint,
        scope: oidc.scope,
      };
    },

    sessionFromRequest(request: Request): OidcSession | null {
      if (!oidc) return null;
      const value = cookieValue(request, oidc.sessionCookieName);
      if (!value) return null;
      const [payload, signature] = value.split(".");
      if (!payload || !signature) return null;
      const expected = sign(payload, oidc.sessionSecret);
      const receivedBytes = Buffer.from(signature);
      const expectedBytes = Buffer.from(expected);
      if (receivedBytes.length !== expectedBytes.length || !timingSafeEqual(receivedBytes, expectedBytes)) {
        return null;
      }
      try {
        const session = JSON.parse(Buffer.from(payload, "base64url").toString("utf8")) as OidcSession;
        return session.exp > Math.floor(Date.now() / 1000) ? session : null;
      } catch {
        return null;
      }
    },

    authenticate(request: Request): ApiTokenRecord | null {
      const session = this.sessionFromRequest(request);
      return session ? authRecord(session) : null;
    },

    sessionResponse(request: Request) {
      if (!oidc) return { enabled: false, authenticated: true, mode: "api-token" as const };
      const session = this.sessionFromRequest(request);
      return {
        enabled: true,
        authenticated: Boolean(session),
        mode: "oidc" as const,
        user: session ? { sub: session.sub, email: session.email, name: session.name } : undefined,
      };
    },

    async exchangeCode(input: OidcTokenInput, request: Request) {
      if (!oidc) throw new Error("OIDC is not configured.");
      const metadata = await discovery();
      const body = new URLSearchParams({
        grant_type: "authorization_code",
        code: input.code,
        redirect_uri: input.redirectUri,
        code_verifier: input.codeVerifier,
      });
      const headers: Record<string, string> = {
        "content-type": "application/x-www-form-urlencoded",
      };
      if (oidc.clientSecret) {
        headers.authorization = `Basic ${Buffer.from(`${oidc.clientId}:${oidc.clientSecret}`).toString("base64")}`;
      } else {
        body.set("client_id", oidc.clientId);
      }

      const response = await fetchImpl(metadata.token_endpoint, {
        method: "POST",
        headers,
        body,
      });
      const tokenResponse = await response.json().catch(() => ({})) as { id_token?: string; error?: string };
      if (!response.ok || !tokenResponse.id_token) {
        throw new Error(tokenResponse.error ?? `OIDC token exchange failed with ${response.status}.`);
      }

      const payload = await verifyIdToken(tokenResponse.id_token);
      if (!payload.sub || !payload.exp) throw new Error("OIDC ID token is missing required claims.");
      const session: OidcSession = {
        sub: payload.sub,
        email: typeof payload.email === "string" ? payload.email : undefined,
        name: typeof payload.name === "string" ? payload.name : undefined,
        exp: payload.exp,
      };
      return {
        cookie: setCookie(session, request),
        body: {
          authenticated: true,
          user: { sub: session.sub, email: session.email, name: session.name },
          expiresAt: new Date(session.exp * 1000).toISOString(),
        },
      };
    },

    clearCookie(request: Request): string {
      const name = oidc?.sessionCookieName ?? "rohrpost_oidc";
      const secure = new URL(request.url).protocol === "https:" ? "; Secure" : "";
      return `${name}=; HttpOnly; Path=/; SameSite=Lax; Max-Age=0${secure}`;
    },
  };
}
