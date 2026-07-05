import { controlApiPaths, type ControlApiConnectorRecord } from '@rohrpost/control-api-contracts';
import type { ConnectorRecord } from '../../lib/api-types';
import { requestJson } from '../../lib/api-base';

function mapConnector(record: ControlApiConnectorRecord): ConnectorRecord {
  return {
    id: record.id,
    tenantId: record.tenantId ?? 'tenant_demo',
    name: record.name ?? record.id,
    capabilityId: record.capabilityId,
    executionMode: record.executionMode,
    config: record.config,
    createdAt: record.createdAt ?? '',
  };
}

export async function fetchConnectors(input?: {
  capabilityId?: string;
  tenantId?: string;
}): Promise<ConnectorRecord[]> {
  return (await requestJson<ControlApiConnectorRecord[]>(controlApiPaths.connectors(input))).map(mapConnector);
}

export async function saveConnector(input: {
  id?: string;
  name: string;
  capabilityId: string;
  config: Record<string, unknown>;
  tenantId?: string;
}): Promise<ConnectorRecord> {
  return mapConnector(
    await requestJson<ControlApiConnectorRecord>(controlApiPaths.connectors(), {
      method: 'POST',
      body: JSON.stringify(input),
    }),
  );
}
