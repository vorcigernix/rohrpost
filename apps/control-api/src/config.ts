import { dirname, resolve } from "node:path";

export interface ControlApiConfig {
  host: string;
  port: number;
  databasePath: string;
  runSummaryRetentionLimit: number;
  bootstrapAdminEmail: string;
  bootstrapApiToken: string;
  defaultTenantId: string;
  defaultTenantName: string;
  adapterRedpandaUrl?: string;
  geminiApiKey?: string;
  geminiModel?: string;
  geminiApiBaseUrl?: string;
  oidc?: {
    issuerUrl: string;
    clientId: string;
    clientSecret?: string;
    scope: string;
    sessionCookieName: string;
    sessionSecret: string;
  };
}

function parsePort(value: string | undefined, fallback: number): number {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function parseNumber(value: string | undefined, fallback: number): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? Math.floor(parsed) : fallback;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): ControlApiConfig {
  const databasePath = resolve(env.CONTROL_API_DB_PATH ?? "data/control-plane.db");
  const oidcIssuerUrl = env.CONTROL_API_OIDC_ISSUER_URL ?? env.OIDC_ISSUER_URL;
  const oidcClientId = env.CONTROL_API_OIDC_CLIENT_ID ?? env.OIDC_CLIENT_ID;

  return {
    host: env.CONTROL_API_HOST ?? "0.0.0.0",
    port: parsePort(env.CONTROL_API_PORT, 3001),
    databasePath,
    runSummaryRetentionLimit: parseNumber(env.CONTROL_API_RUN_SUMMARY_RETENTION_LIMIT, 500),
    bootstrapAdminEmail: env.BOOTSTRAP_ADMIN_EMAIL ?? "admin@local.rohrpost",
    bootstrapApiToken: env.BOOTSTRAP_API_TOKEN ?? "dev-admin-token",
    defaultTenantId: env.DEFAULT_TENANT_ID ?? "tenant_demo",
    defaultTenantName: env.DEFAULT_TENANT_NAME ?? "Demo Tenant",
    adapterRedpandaUrl: env.ADAPTER_REDPANDA_URL,
    geminiApiKey: env.GEMINI_API_KEY ?? env.GOOGLE_API_KEY,
    geminiModel: env.GEMINI_MODEL ?? "gemini-2.5-flash",
    geminiApiBaseUrl: env.GEMINI_API_BASE_URL ?? "https://generativelanguage.googleapis.com/v1beta",
    oidc: oidcIssuerUrl && oidcClientId ? {
      issuerUrl: oidcIssuerUrl.replace(/\/$/, ""),
      clientId: oidcClientId,
      clientSecret: env.CONTROL_API_OIDC_CLIENT_SECRET ?? env.OIDC_CLIENT_SECRET,
      scope: env.CONTROL_API_OIDC_SCOPE ?? env.OIDC_SCOPE ?? "openid profile email",
      sessionCookieName: env.CONTROL_API_OIDC_SESSION_COOKIE ?? "rohrpost_oidc",
      sessionSecret: env.CONTROL_API_OIDC_SESSION_SECRET ?? env.BOOTSTRAP_API_TOKEN ?? "dev-admin-token",
    } : undefined,
  };
}

export function databaseDirectory(config: ControlApiConfig): string {
  return dirname(config.databasePath);
}
