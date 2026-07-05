import type { Database } from "bun:sqlite";
import { CONNECTOR_CAPABILITIES } from "@rohrpost/domain-connectors";
import type { ControlApiConfig } from "../config";
import {
  defaultAiProviderSettings,
  id,
  isoNow,
  mapAiProviderSettingsRow,
  mapConnectorRow,
} from "./shared";
import type { Repository } from "./types";

export function createCatalogRepository(
  db: Database,
  config: ControlApiConfig,
): Pick<
  Repository,
  "authenticate" | "getAiProviderSettings" | "saveAiProviderSettings" | "listCapabilities" | "listConnectors" | "saveConnector"
> {
  const getAiProviderSettings: Repository["getAiProviderSettings"] = () => {
    const row = db
      .query("SELECT * FROM ai_provider_settings WHERE id = 'default' LIMIT 1")
      .get() as Record<string, unknown> | null;

    return row ? mapAiProviderSettingsRow(row) : defaultAiProviderSettings(config);
  };

  return {
    authenticate(token) {
      if (!token) return null;

      const row = db
        .query("SELECT user_id, label FROM api_tokens WHERE token = ? LIMIT 1")
        .get(token) as { user_id?: string; label?: string } | null;

      if (!row?.user_id || !row.label) {
        return null;
      }

      db.query("UPDATE api_tokens SET last_used_at = ? WHERE token = ?").run(isoNow(), token);

      return {
        userId: row.user_id,
        label: row.label,
      };
    },
    getAiProviderSettings,
    saveAiProviderSettings(input) {
      const current = getAiProviderSettings();
      const now = isoNow();
      const apiKey =
        input.clearApiKey
          ? null
          : typeof input.apiKey === "string" && input.apiKey.trim().length > 0
            ? input.apiKey.trim()
            : current.source === "database"
              ? current.apiKey
              : null;
      const model = input.model?.trim() || current.model || "gemini-2.5-flash";
      const apiBaseUrl =
        input.apiBaseUrl?.trim() ||
        current.apiBaseUrl ||
        "https://generativelanguage.googleapis.com/v1beta";

      db.query(
        `
          INSERT INTO ai_provider_settings (id, provider, enabled, api_key, model, api_base_url, updated_at)
          VALUES ('default', ?, ?, ?, ?, ?, ?)
          ON CONFLICT(id) DO UPDATE SET
            provider = excluded.provider,
            enabled = excluded.enabled,
            api_key = excluded.api_key,
            model = excluded.model,
            api_base_url = excluded.api_base_url,
            updated_at = excluded.updated_at
        `,
      ).run(input.provider, input.enabled ? 1 : 0, apiKey, model, apiBaseUrl, now);

      return {
        provider: input.provider,
        enabled: input.enabled,
        apiKey,
        model,
        apiBaseUrl,
        source: "database",
        updatedAt: now,
      };
    },
    listCapabilities() {
      return CONNECTOR_CAPABILITIES;
    },
    listConnectors(options) {
      const tenantId = options?.tenantId ?? config.defaultTenantId;
      const capabilityId = options?.capabilityId;

      const rows = capabilityId
        ? (
            db
              .query(
                "SELECT * FROM connectors WHERE tenant_id = ? AND capability_id = ? ORDER BY name ASC, created_at ASC",
              )
              .all(tenantId, capabilityId) as Record<string, unknown>[]
          )
        : (
            db
              .query("SELECT * FROM connectors WHERE tenant_id = ? ORDER BY name ASC, created_at ASC")
              .all(tenantId) as Record<string, unknown>[]
          );

      return rows.map(mapConnectorRow);
    },
    saveConnector(input) {
      const connectorId = input.id?.trim() || id("connector");
      const now = isoNow();
      const existing = db
        .query("SELECT id, created_at FROM connectors WHERE id = ? LIMIT 1")
        .get(connectorId) as { id?: string; created_at?: string } | null;

      if (existing?.id) {
        db.query(
          "UPDATE connectors SET tenant_id = ?, name = ?, capability_id = ?, execution_mode = ?, config_json = ? WHERE id = ?",
        ).run(
          input.tenantId,
          input.name,
          input.capabilityId,
          input.executionMode,
          JSON.stringify(input.config),
          connectorId,
        );
      } else {
        db.query(
          "INSERT INTO connectors (id, tenant_id, name, capability_id, execution_mode, config_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ).run(
          connectorId,
          input.tenantId,
          input.name,
          input.capabilityId,
          input.executionMode,
          JSON.stringify(input.config),
          now,
        );
      }

      return {
        id: connectorId,
        tenantId: input.tenantId,
        name: input.name,
        capabilityId: input.capabilityId,
        executionMode: input.executionMode,
        config: input.config,
        createdAt: existing?.created_at ? String(existing.created_at) : now,
      };
    },
  };
}
